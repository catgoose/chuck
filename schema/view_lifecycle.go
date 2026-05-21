package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/catgoose/chuck"
)

// ErrViewMissing is returned by ValidateView / ValidateViews when a declared
// view does not exist in the live database. Callers can errors.Is-detect this
// to distinguish "not bootstrapped yet" from "definition drift."
var ErrViewMissing = errors.New("schema: declared view does not exist in live database")

// ErrViewBodyDrift is returned by ValidateView / ValidateViews when a declared
// view exists in the live database but its normalized body does not match the
// declared body. Callers can errors.Is-detect this to distinguish drift from
// missing-object and from infrastructure errors.
var ErrViewBodyDrift = errors.New("schema: declared view body differs from live database body")

// ErrViewReplacementStillExists is returned by ValidateView / ValidateViews
// when a name declared via ViewDef.WithReplaces still exists in the live
// database. Callers can errors.Is-detect this to distinguish "old name not
// yet retired" from current-object drift.
var ErrViewReplacementStillExists = errors.New("schema: declared replaced view still exists in live database")

// ErrViewBodyComparisonUnsupported is returned by ValidateView / ValidateViews
// when the live engine canonicalizes view definitions enough that body-text
// drift cannot be honestly compared (today: Postgres `pg_get_viewdef` expands
// `SELECT *`, fully qualifies references, and inserts casts). Validate fails
// loud with this sentinel rather than silently returning success so callers
// asking for "validate" cannot mistake existence-only confirmation for a clean
// body match. Callers that want to opt into existence-only semantics on these
// engines can detect the sentinel via `errors.Is` and treat it as success;
// callers that need stricter assertions can fetch `LiveViewBody` and run their
// own comparison.
var ErrViewBodyComparisonUnsupported = errors.New("schema: view body comparison unsupported on this engine; existence confirmed")

// ViewDrift describes one declared view whose state in the live database does
// not match the declared definition. Either Missing is true, or BodyMismatch
// is true (with DeclaredBody / LiveBody populated for diagnosis).
//
// BodyComparisonSkipped is true when the live engine canonicalizes view
// definitions enough that body-text drift cannot be honestly reported (today:
// Postgres pg_get_viewdef rewrites identifiers, expands stars, fully-qualifies
// references). In that case Validate only confirms existence and Reason
// explains why body comparison was skipped.
type ViewDrift struct {
	Object                chuck.ObjectName
	Missing               bool
	BodyMismatch          bool
	BodyComparisonSkipped bool
	// ReplacementStale is true when Object names a prior view declared via
	// ViewDef.WithReplaces that still exists in the live database. The
	// current view (Object's caller) may still match; the drift flags a
	// rename rollout that has not finished cleaning up the prior name.
	ReplacementStale bool
	DeclaredBody     string
	LiveBody         string
	Reason           string
}

// ViewDriftError is returned by ValidateView / ValidateViews when one or more
// declared views are missing, drifted, or could not be honestly compared on
// the live engine. Drifts holds one entry per offending view, in
// caller-supplied order. The error wraps ErrViewMissing when every drift is a
// missing object, ErrViewBodyDrift when every drift is a body mismatch, and
// ErrViewBodyComparisonUnsupported when every drift is a body-comparison-skip
// (engine canonicalization made textual compare unreliable). Single-cause
// results can be branched cleanly via errors.Is. Mixed-cause errors do not
// wrap any sentinel.
type ViewDriftError struct {
	Drifts []ViewDrift
}

func (e *ViewDriftError) Error() string {
	parts := make([]string, 0, len(e.Drifts))
	for _, d := range e.Drifts {
		obj := objectKey(d.Object)
		switch {
		case d.Missing:
			parts = append(parts, fmt.Sprintf("view %q: missing", obj))
		case d.ReplacementStale:
			parts = append(parts, fmt.Sprintf("view %q: replacement still exists", obj))
		case d.BodyComparisonSkipped:
			parts = append(parts, fmt.Sprintf("view %q: %s", obj, d.Reason))
		case d.BodyMismatch:
			parts = append(parts, fmt.Sprintf("view %q: body drift", obj))
		default:
			parts = append(parts, fmt.Sprintf("view %q: %s", obj, d.Reason))
		}
	}
	return "schema: view drift detected: " + strings.Join(parts, ", ")
}

func (e *ViewDriftError) Unwrap() error {
	if len(e.Drifts) == 0 {
		return nil
	}
	onlyMissing := true
	onlyBody := true
	onlySkipped := true
	onlyReplStale := true
	for _, d := range e.Drifts {
		if !d.Missing {
			onlyMissing = false
		}
		if !d.BodyMismatch {
			onlyBody = false
		}
		if !d.BodyComparisonSkipped {
			onlySkipped = false
		}
		if !d.ReplacementStale {
			onlyReplStale = false
		}
	}
	switch {
	case onlyMissing:
		return ErrViewMissing
	case onlyBody:
		return ErrViewBodyDrift
	case onlySkipped:
		return ErrViewBodyComparisonUnsupported
	case onlyReplStale:
		return ErrViewReplacementStillExists
	default:
		return nil
	}
}

// LiveViewBody queries the live database for the declared body of an owned
// view, returning the body text (the portion that follows `AS` for engines
// that store the full CREATE statement). Returns (body, true, nil) on
// success, ("", false, nil) when the view does not exist, and a non-nil
// error only on infrastructure failure.
//
// On Postgres the returned body is whatever pg_get_viewdef(..., true) emits:
// a heavily canonicalized SELECT with expanded stars, fully-qualified
// references, and inserted casts. That text is exposed verbatim for callers
// that want to do their own comparison; the package's own ValidateView refuses
// to compare it against a declared body because the rewrites produce too much
// false drift to be honest about, and instead returns
// ErrViewBodyComparisonUnsupported so callers cannot mistake silent success
// for a clean body match.
func LiveViewBody(ctx context.Context, db *sql.DB, d chuck.Dialect, v *ViewDef) (body string, exists bool, err error) {
	switch d.Engine() {
	case chuck.SQLite:
		return liveViewBodySQLite(ctx, db, v)
	case chuck.MSSQL:
		return liveViewBodyMSSQL(ctx, db, d, v)
	case chuck.Postgres:
		return liveViewBodyPostgres(ctx, db, d, v)
	default:
		return "", false, fmt.Errorf("schema: unsupported engine for view introspection: %s", d.Engine())
	}
}

func liveViewBodySQLite(ctx context.Context, db *sql.DB, v *ViewDef) (body string, exists bool, err error) {
	const q = `SELECT sql FROM sqlite_master WHERE type='view' AND name=?`
	var raw sql.NullString
	switch err := db.QueryRowContext(ctx, q, v.Name).Scan(&raw); err {
	case nil:
		if !raw.Valid {
			return "", true, nil
		}
		return stripCreateViewPreamble(raw.String), true, nil
	case sql.ErrNoRows:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("schema: query sqlite_master for view %q: %w", v.Name, err)
	}
}

func liveViewBodyMSSQL(ctx context.Context, db *sql.DB, d chuck.Dialect, v *ViewDef) (body string, exists bool, err error) {
	schema, name := normalizedObject(d, v.Object())
	objArg := name
	if schema != "" {
		objArg = schema + "." + name
	}
	const q = `SELECT sm.definition
FROM sys.sql_modules sm
JOIN sys.views vw ON vw.object_id = sm.object_id
WHERE sm.object_id = OBJECT_ID(@p1)`
	var raw sql.NullString
	switch err := db.QueryRowContext(ctx, q, objArg).Scan(&raw); err {
	case nil:
		if !raw.Valid {
			return "", true, nil
		}
		return stripCreateViewPreamble(raw.String), true, nil
	case sql.ErrNoRows:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("schema: query sys.sql_modules for view %q: %w", objArg, err)
	}
}

func liveViewBodyPostgres(ctx context.Context, db *sql.DB, d chuck.Dialect, v *ViewDef) (body string, exists bool, err error) {
	schema := resolveSchemaForInspection(d, v.Object())
	_, name := normalizedObject(d, v.Object())
	const q = `SELECT pg_get_viewdef(c.oid, true)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v' AND n.nspname = $1 AND c.relname = $2`
	var raw sql.NullString
	switch err := db.QueryRowContext(ctx, q, schema, name).Scan(&raw); err {
	case nil:
		if !raw.Valid {
			return "", true, nil
		}
		return raw.String, true, nil
	case sql.ErrNoRows:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("schema: query pg_get_viewdef for view %q.%q: %w", schema, name, err)
	}
}

// viewBodyPreambleRe matches a leading `CREATE [OR REPLACE | OR ALTER] VIEW
// <identifier>(.<identifier>)? AS` prefix on a stored view definition. It is
// anchored to the start of the string and runs case-insensitively, so the
// matching tail is everything after the AS keyword and any trailing
// whitespace. The identifier subpattern accepts bracket-quoted, double-quoted,
// or bare names with an optional `schema.` prefix so SQLite (bare) and MSSQL
// (bracketed, optionally schema-qualified) live definitions both collapse to
// just the body.
var viewBodyPreambleRe = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?VIEW\s+(?:\[[^\]]+\]|"[^"]+"|[A-Za-z_][\w$]*)(?:\.(?:\[[^\]]+\]|"[^"]+"|[A-Za-z_][\w$]*))?\s+AS\s+`)

// stripCreateViewPreamble removes the leading `CREATE [OR ...] VIEW <name> AS`
// preamble from a stored view definition, returning just the body text. If
// the preamble is not found the input is returned unchanged so callers
// comparing already-bare bodies (e.g. Postgres pg_get_viewdef output) still
// get a usable string.
func stripCreateViewPreamble(s string) string {
	loc := viewBodyPreambleRe.FindStringIndex(s)
	if loc == nil {
		return s
	}
	return s[loc[1]:]
}

// stripLeadingBlockComments removes any number of leading SQL block comments
// plus surrounding leading whitespace. Used by validation so stacked doc /
// ownership annotations do not participate in drift comparison; callers want
// the actual executable statement body/definition checked, not front-matter
// comments.
func stripLeadingBlockComments(s string) string {
	out := strings.TrimLeftFunc(s, unicode.IsSpace)
	for strings.HasPrefix(out, "/*") {
		end := strings.Index(out, "*/")
		if end < 0 {
			return out
		}
		out = strings.TrimLeftFunc(out[end+2:], unicode.IsSpace)
	}
	return out
}

// canonicalizeStatement normalizes a SQL statement body for drift comparison.
// Used for both view bodies and procedure definitions. It trims surrounding
// whitespace, strips trailing semicolons, and collapses runs of internal
// whitespace (including newlines/tabs) to a single space. The result is the
// comparable form — not a roundtrippable SQL string.
func canonicalizeStatement(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, ";")
	s = strings.TrimSpace(s)
	var b strings.Builder
	b.Grow(len(s))
	inWS := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !inWS {
				b.WriteByte(' ')
				inWS = true
			}
			continue
		}
		b.WriteRune(r)
		inWS = false
	}
	return b.String()
}

// ValidateView checks that a declared view exists in the live database and
// that its body matches the declaration after canonical normalization
// (whitespace collapse, trailing-semicolon strip, CREATE-preamble strip on
// engines that store the full statement).
//
// Returns nil on match. Returns a `*ViewDriftError` whose Drifts slice has
// exactly one entry when the view is missing or its body differs.
// Infrastructure failures (query errors, unsupported engine) are returned
// directly.
//
// On Postgres the live body is whatever pg_get_viewdef emits — a
// canonicalized SELECT with expanded stars, fully-qualified references, and
// inserted casts. That canonicalization is too aggressive to support
// faithful body-drift detection against a hand-written declared body, so on
// Postgres ValidateView fails loud by returning a `*ViewDriftError` whose
// single entry has `BodyComparisonSkipped=true` and unwraps to
// `ErrViewBodyComparisonUnsupported`. Existence is still confirmed: if the
// view is missing the error unwraps to `ErrViewMissing` instead. Callers that
// want existence-only semantics on Postgres can branch on
// `errors.Is(err, ErrViewBodyComparisonUnsupported)` and treat it as success;
// callers that need stricter body assertions should fetch `LiveViewBody` and
// run their own comparison.
//
// ValidateView is a thin wrapper around ValidateViewWithOptions that passes
// the zero CodeObjectOptions, which neither injects nor tolerates a notice.
func ValidateView(ctx context.Context, db *sql.DB, d chuck.Dialect, v *ViewDef) error {
	return ValidateViewWithOptions(ctx, db, d, CodeObjectOptions{}, v)
}

// ValidateViewWithOptions is the option-aware counterpart to ValidateView.
// Leading block comments are treated as documentation, not executable view
// semantics: validation strips any leading block-comment stack from both the
// declared body and the live body before canonical comparison. This means
// ownership notices, doc preambles, per-object annotations, and other leading
// `/* ... */` front matter do not cause drift on their own.
//
// When the view declares prior names via WithReplaces, any of those names
// still present in the live database surface as additional drift entries
// (ReplacementStale=true), so a rename rollout that has not finished
// cleaning up old names fails validation explicitly.
func ValidateViewWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, v *ViewDef) error {
	drifts, err := validateViewWithOptionsInternal(ctx, db, d, opts, v, nil)
	if err != nil {
		return err
	}
	if len(drifts) == 0 {
		return nil
	}
	return &ViewDriftError{Drifts: drifts}
}

// validateViewWithOptionsInternal performs the per-view validate work and
// returns drifts in caller-supplied order. When checked != nil, replacement
// names are recorded as they are queried so a batch validator can dedupe
// repeated names across multiple defs.
func validateViewWithOptionsInternal(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, v *ViewDef, checked map[string]struct{}) ([]ViewDrift, error) {
	var drifts []ViewDrift
	live, exists, err := LiveViewBody(ctx, db, d, v)
	if err != nil {
		return nil, err
	}
	switch {
	case !exists:
		drifts = append(drifts, ViewDrift{
			Object:  v.Object(),
			Missing: true,
			Reason:  "view does not exist",
		})
	case d.Engine() == chuck.Postgres:
		drifts = append(drifts, ViewDrift{
			Object:                v.Object(),
			BodyComparisonSkipped: true,
			DeclaredBody:          v.Body(),
			LiveBody:              live,
			Reason:                "pg_get_viewdef canonicalization (star expansion, fully-qualified identifiers, inserted casts) makes textual body comparison unreliable; existence confirmed",
		})
	default:
		effOpts := effectiveOptionsForRender(d, opts)
		liveStripped := stripLeadingBlockComments(stripConfiguredApplyPrefix(live, effOpts, v.DocAnnotation()))
		declaredCanon := canonicalizeStatement(stripLeadingBlockComments(v.Body()))
		liveCanon := canonicalizeStatement(liveStripped)
		if declaredCanon != liveCanon {
			drifts = append(drifts, ViewDrift{
				Object:       v.Object(),
				BodyMismatch: true,
				DeclaredBody: declaredCanon,
				LiveBody:     liveCanon,
				Reason:       "view body differs after canonical normalization",
			})
		}
	}
	for _, repl := range v.Replaces() {
		key := objectKey(repl)
		if checked != nil {
			if _, ok := checked[key]; ok {
				continue
			}
			checked[key] = struct{}{}
		}
		tmp := &ViewDef{Name: repl.Name, schema: repl.Schema}
		liveBody, replExists, err := LiveViewBody(ctx, db, d, tmp)
		if err != nil {
			return nil, err
		}
		if replExists {
			drifts = append(drifts, ViewDrift{
				Object:           repl,
				ReplacementStale: true,
				LiveBody:         liveBody,
				Reason:           "view declared as replaced but still exists in live database",
			})
		}
	}
	return drifts, nil
}

// ValidateViews validates each given view in order, aggregating any drift
// into a single `*ViewDriftError`. Infrastructure errors short-circuit and
// are returned directly. Returns nil if every view matches.
func ValidateViews(ctx context.Context, db *sql.DB, d chuck.Dialect, views ...*ViewDef) error {
	return ValidateViewsWithOptions(ctx, db, d, CodeObjectOptions{}, views...)
}

// ValidateViewsWithOptions is the option-aware counterpart to ValidateViews.
// See ValidateViewWithOptions for the per-view semantics; aggregation behavior
// is identical to ValidateViews. Replacement-name checks are deduped across
// the batch so the same prior name only surfaces once even if it is declared
// via WithReplaces on multiple defs.
func ValidateViewsWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, views ...*ViewDef) error {
	checked := map[string]struct{}{}
	var drifts []ViewDrift
	for _, v := range views {
		ds, err := validateViewWithOptionsInternal(ctx, db, d, opts, v, checked)
		if err != nil {
			return err
		}
		drifts = append(drifts, ds...)
	}
	if len(drifts) == 0 {
		return nil
	}
	return &ViewDriftError{Drifts: drifts}
}

// ApplyView writes the declared view to the live database via its
// CreateOrReplaceSQL statement chain, executing each statement in order.
// This is idempotent on Postgres/MSSQL (CREATE OR REPLACE / CREATE OR ALTER)
// and effectively idempotent on SQLite (DROP IF EXISTS + CREATE).
//
// ApplyView is one-way: declared body becomes live body. It does no
// pre-flight drift check; callers that want validate-then-apply semantics
// should call ValidateView themselves first and only apply when drift is
// detected.
//
// ApplyView is a thin wrapper around ApplyViewWithOptions that passes the
// zero CodeObjectOptions; use ApplyViewWithOptions to opt into an
// ownership-notice prefix.
func ApplyView(ctx context.Context, db *sql.DB, d chuck.Dialect, v *ViewDef) error {
	return ApplyViewWithOptions(ctx, db, d, CodeObjectOptions{}, v)
}

// ApplyViewWithOptions is the option-aware counterpart to ApplyView. When
// opts.OwnershipNotice is set, the body rendered into the live database is
// prefixed with the corresponding SQL block comment so DB-side readers can
// see the chuck-owned marker. Callers that use this path should pair it with
// ValidateViewWithOptions (same opts) to keep apply and validate coherent.
//
// When the view declares prior names via WithReplaces, each listed name is
// dropped (same dialect-aware DROP IF EXISTS pattern as a regular view drop)
// before the current view is created. Cross-type drops are not attempted.
func ApplyViewWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, v *ViewDef) error {
	return applyViewWithOptionsInternal(ctx, db, d, opts, v, nil)
}

// applyViewWithOptionsInternal performs the per-view apply work. When
// dropped != nil, replacement names already dropped earlier in the batch are
// skipped so the same prior name is only dropped once across a batch.
func applyViewWithOptionsInternal(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, v *ViewDef, dropped map[string]struct{}) error {
	for _, repl := range v.Replaces() {
		key := objectKey(repl)
		if dropped != nil {
			if _, ok := dropped[key]; ok {
				continue
			}
			dropped[key] = struct{}{}
		}
		tmp := &ViewDef{Name: repl.Name, schema: repl.Schema}
		if _, err := db.ExecContext(ctx, tmp.DropSQL(d)); err != nil {
			return fmt.Errorf("schema: drop replaced view %q: %w", key, err)
		}
	}
	effOpts := effectiveOptionsForRender(d, opts)
	body := applyOwnershipNoticePrefix(v.Body(), effOpts, v.DocAnnotation())
	for _, stmt := range v.createOrReplaceWithBody(d, body) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: apply view %q: %w", objectKey(v.Object()), err)
		}
	}
	if opts.Metadata != nil {
		hash := hashCodeObjectDefinition(body)
		if err := recordCodeObjectMetadata(ctx, db, d, *opts.Metadata, MetadataObjectTypeView, v.Object(), hash); err != nil {
			return fmt.Errorf("schema: record metadata for view %q: %w", objectKey(v.Object()), err)
		}
	}
	return nil
}

// ApplyViews applies each declared view in caller-supplied order. Returns
// the first error encountered; partial application up to that point is left
// in place (matching the rest of chuck's no-implicit-rollback stance).
func ApplyViews(ctx context.Context, db *sql.DB, d chuck.Dialect, views ...*ViewDef) error {
	return ApplyViewsWithOptions(ctx, db, d, CodeObjectOptions{}, views...)
}

// ApplyViewsWithOptions is the option-aware counterpart to ApplyViews.
// Replacement-name drops are deduped across the batch so the same prior name
// is only issued one DROP statement even if it is declared via WithReplaces
// on multiple defs.
func ApplyViewsWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, views ...*ViewDef) error {
	dropped := map[string]struct{}{}
	for _, v := range views {
		if err := applyViewWithOptionsInternal(ctx, db, d, opts, v, dropped); err != nil {
			return err
		}
	}
	return nil
}
