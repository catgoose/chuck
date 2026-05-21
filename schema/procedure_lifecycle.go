package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/catgoose/chuck"
)

// ErrProcedureMissing is returned by ValidateProcedure / ValidateProcedures
// when a declared procedure does not exist in the live database.
var ErrProcedureMissing = errors.New("schema: declared procedure does not exist in live database")

// ErrProcedureDefinitionDrift is returned by ValidateProcedure /
// ValidateProcedures when a declared procedure exists but its normalized
// definition differs from the declaration.
var ErrProcedureDefinitionDrift = errors.New("schema: declared procedure definition differs from live database definition")

// ErrProcedureReplacementStillExists is returned by ValidateProcedure /
// ValidateProcedures when a name declared via ProcedureDef.WithReplaces still
// exists in the live database. Callers can errors.Is-detect this to
// distinguish "old name not yet retired" from current-object drift.
var ErrProcedureReplacementStillExists = errors.New("schema: declared replaced procedure still exists in live database")

// ProcedureDrift describes one declared procedure whose state in the live
// database does not match the declared definition. Either Missing is true,
// or DefinitionMismatch is true with DeclaredDefinition / LiveDefinition
// populated for diagnosis.
type ProcedureDrift struct {
	Object             chuck.ObjectName
	Missing            bool
	DefinitionMismatch bool
	// ReplacementStale is true when Object names a prior procedure declared
	// via ProcedureDef.WithReplaces that still exists in the live database.
	// The current procedure may still match; the drift flags a rename
	// rollout that has not finished cleaning up the prior name.
	ReplacementStale   bool
	DeclaredDefinition string
	LiveDefinition     string
	Reason             string
}

// ProcedureDriftError is returned by ValidateProcedure / ValidateProcedures
// when one or more declared procedures are missing or drifted. Drifts holds
// one entry per offending procedure. The error wraps ErrProcedureMissing
// when every drift is a missing object and ErrProcedureDefinitionDrift when
// every drift is a definition mismatch.
type ProcedureDriftError struct {
	Drifts []ProcedureDrift
}

func (e *ProcedureDriftError) Error() string {
	parts := make([]string, 0, len(e.Drifts))
	for _, d := range e.Drifts {
		obj := objectKey(d.Object)
		switch {
		case d.Missing:
			parts = append(parts, fmt.Sprintf("procedure %q: missing", obj))
		case d.ReplacementStale:
			parts = append(parts, fmt.Sprintf("procedure %q: replacement still exists", obj))
		case d.DefinitionMismatch:
			parts = append(parts, fmt.Sprintf("procedure %q: definition drift", obj))
		default:
			parts = append(parts, fmt.Sprintf("procedure %q: %s", obj, d.Reason))
		}
	}
	return "schema: procedure drift detected: " + strings.Join(parts, ", ")
}

func (e *ProcedureDriftError) Unwrap() error {
	if len(e.Drifts) == 0 {
		return nil
	}
	onlyMissing := true
	onlyDefn := true
	onlyReplStale := true
	for _, d := range e.Drifts {
		if !d.Missing {
			onlyMissing = false
		}
		if !d.DefinitionMismatch {
			onlyDefn = false
		}
		if !d.ReplacementStale {
			onlyReplStale = false
		}
	}
	switch {
	case onlyMissing:
		return ErrProcedureMissing
	case onlyDefn:
		return ErrProcedureDefinitionDrift
	case onlyReplStale:
		return ErrProcedureReplacementStillExists
	default:
		return nil
	}
}

// LiveProcedureDefinition queries the live MSSQL database for the declared
// definition of an owned procedure, returning the text that follows
// `CREATE OR ALTER PROCEDURE <qualified-name>` — i.e. the same shape the
// caller passes to NewProcedure / NewQualifiedProcedure as the definition
// payload. Returns ("", false, nil) when the procedure does not exist and a
// non-nil error only on infrastructure failure or unsupported dialect.
//
// Returns ErrProcedureDialectUnsupported on non-MSSQL dialects so callers
// running validate/apply against the wrong engine fail loud instead of
// silently no-op'ing.
func LiveProcedureDefinition(ctx context.Context, db *sql.DB, d chuck.Dialect, p *ProcedureDef) (definition string, exists bool, err error) {
	if d.Engine() != chuck.MSSQL {
		return "", false, fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	schema, name := normalizedObject(d, p.Object())
	objArg := name
	if schema != "" {
		objArg = schema + "." + name
	}
	const q = `SELECT sm.definition
FROM sys.sql_modules sm
JOIN sys.procedures pr ON pr.object_id = sm.object_id
WHERE sm.object_id = OBJECT_ID(@p1)`
	var raw sql.NullString
	switch err := db.QueryRowContext(ctx, q, objArg).Scan(&raw); err {
	case nil:
		if !raw.Valid {
			return "", true, nil
		}
		return stripCreateProcedurePreamble(raw.String), true, nil
	case sql.ErrNoRows:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("schema: query sys.sql_modules for procedure %q: %w", objArg, err)
	}
}

// procedurePreambleRe matches a leading `CREATE [OR ALTER] PROCEDURE <name>`
// prefix on a stored procedure definition. The identifier subpattern accepts
// bracketed, double-quoted, or bare names with an optional `schema.` prefix
// so MSSQL `[dbo].[name]` and bare-name definitions both strip cleanly.
var procedurePreambleRe = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+(?:\[[^\]]+\]|"[^"]+"|[A-Za-z_][\w$]*)(?:\.(?:\[[^\]]+\]|"[^"]+"|[A-Za-z_][\w$]*))?\s*`)

// stripCreateProcedurePreamble removes the leading
// `CREATE [OR ALTER] PROCEDURE <name>` preamble from a stored procedure
// definition, returning just the definition payload. If the preamble is not
// found the input is returned unchanged.
func stripCreateProcedurePreamble(s string) string {
	loc := procedurePreambleRe.FindStringIndex(s)
	if loc == nil {
		return s
	}
	return s[loc[1]:]
}

// ValidateProcedure checks that a declared MSSQL procedure exists in the
// live database and that its definition matches the declaration after
// canonical normalization (whitespace collapse, trailing-semicolon strip,
// CREATE-preamble strip). MSSQL stores `sys.sql_modules.definition`
// verbatim, so body comparison is honest on this engine.
//
// Returns nil on match. Returns a `*ProcedureDriftError` whose Drifts slice
// has exactly one entry when the procedure is missing or its definition
// differs. Returns ErrProcedureDialectUnsupported on non-MSSQL dialects.
//
// ValidateProcedure is a thin wrapper around ValidateProcedureWithOptions
// that passes the zero CodeObjectOptions, which neither injects nor
// tolerates a notice.
func ValidateProcedure(ctx context.Context, db *sql.DB, d chuck.Dialect, p *ProcedureDef) error {
	return ValidateProcedureWithOptions(ctx, db, d, CodeObjectOptions{}, p)
}

// ValidateProcedureWithOptions is the option-aware counterpart to
// ValidateProcedure. Leading block comments are treated as documentation, not
// executable procedure semantics: validation strips any leading block-comment
// stack from both the declared definition and the live definition before
// canonical comparison. This means ownership notices, doc preambles,
// per-object annotations, and other leading `/* ... */` front matter do not
// cause drift on their own.
//
// When the procedure declares prior names via WithReplaces, any of those
// names still present in the live database surface as additional drift
// entries (ReplacementStale=true), so a rename rollout that has not
// finished cleaning up old names fails validation explicitly.
func ValidateProcedureWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, p *ProcedureDef) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	drifts, err := validateProcedureWithOptionsInternal(ctx, db, d, opts, p, nil)
	if err != nil {
		return err
	}
	if len(drifts) == 0 {
		return nil
	}
	return &ProcedureDriftError{Drifts: drifts}
}

// validateProcedureWithOptionsInternal performs the per-procedure validate
// work and returns drifts in caller-supplied order. When checked != nil,
// replacement names are recorded as they are queried so a batch validator
// can dedupe repeated names across multiple defs.
func validateProcedureWithOptionsInternal(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, p *ProcedureDef, checked map[string]struct{}) ([]ProcedureDrift, error) {
	var drifts []ProcedureDrift
	live, exists, err := LiveProcedureDefinition(ctx, db, d, p)
	if err != nil {
		return nil, err
	}
	if !exists {
		drifts = append(drifts, ProcedureDrift{
			Object:  p.Object(),
			Missing: true,
			Reason:  "procedure does not exist",
		})
	} else {
		effOpts := effectiveOptionsForRender(d, opts)
		liveStripped := stripLeadingBlockComments(stripConfiguredApplyPrefix(live, effOpts, p.DocAnnotation()))
		declaredCanon := canonicalizeStatement(stripLeadingBlockComments(p.Definition()))
		liveCanon := canonicalizeStatement(liveStripped)
		if declaredCanon != liveCanon {
			drifts = append(drifts, ProcedureDrift{
				Object:             p.Object(),
				DefinitionMismatch: true,
				DeclaredDefinition: declaredCanon,
				LiveDefinition:     liveCanon,
				Reason:             "procedure definition differs after canonical normalization",
			})
		}
	}
	for _, repl := range p.Replaces() {
		key := objectKey(repl)
		if checked != nil {
			if _, ok := checked[key]; ok {
				continue
			}
			checked[key] = struct{}{}
		}
		tmp := &ProcedureDef{Name: repl.Name, schema: repl.Schema}
		liveDef, replExists, err := LiveProcedureDefinition(ctx, db, d, tmp)
		if err != nil {
			return nil, err
		}
		if replExists {
			drifts = append(drifts, ProcedureDrift{
				Object:           repl,
				ReplacementStale: true,
				LiveDefinition:   liveDef,
				Reason:           "procedure declared as replaced but still exists in live database",
			})
		}
	}
	return drifts, nil
}

// ValidateProcedures validates each declared procedure in order, aggregating
// any drift into a single `*ProcedureDriftError`. Infrastructure errors and
// unsupported-dialect errors short-circuit and are returned directly.
func ValidateProcedures(ctx context.Context, db *sql.DB, d chuck.Dialect, procs ...*ProcedureDef) error {
	return ValidateProceduresWithOptions(ctx, db, d, CodeObjectOptions{}, procs...)
}

// ValidateProceduresWithOptions is the option-aware counterpart to
// ValidateProcedures. See ValidateProcedureWithOptions for the per-procedure
// semantics; aggregation behavior is identical to ValidateProcedures.
// Replacement-name checks are deduped across the batch so the same prior
// name only surfaces once even if it is declared via WithReplaces on
// multiple defs.
func ValidateProceduresWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, procs ...*ProcedureDef) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	checked := map[string]struct{}{}
	var drifts []ProcedureDrift
	for _, p := range procs {
		ds, err := validateProcedureWithOptionsInternal(ctx, db, d, opts, p, checked)
		if err != nil {
			return err
		}
		drifts = append(drifts, ds...)
	}
	if len(drifts) == 0 {
		return nil
	}
	return &ProcedureDriftError{Drifts: drifts}
}

// ApplyProcedure writes the declared procedure to the live MSSQL database
// via its CreateOrAlterSQL statement, executing it as a single batch
// (matching chuck's one-statement-per-Exec MSSQL model). Idempotent: the
// MSSQL CREATE OR ALTER PROCEDURE batch replaces any prior definition with
// the same identity.
//
// Returns ErrProcedureDialectUnsupported on non-MSSQL dialects. ApplyProcedure
// is one-way: declared definition becomes live definition. It does no
// pre-flight drift check.
//
// ApplyProcedure is a thin wrapper around ApplyProcedureWithOptions that
// passes the zero CodeObjectOptions; use ApplyProcedureWithOptions to opt
// into an ownership-notice prefix.
func ApplyProcedure(ctx context.Context, db *sql.DB, d chuck.Dialect, p *ProcedureDef) error {
	return ApplyProcedureWithOptions(ctx, db, d, CodeObjectOptions{}, p)
}

// ApplyProcedureWithOptions is the option-aware counterpart to
// ApplyProcedure. When opts.OwnershipNotice is set, the definition rendered
// into the live MSSQL database is prefixed with the corresponding T-SQL
// block comment, sitting between the qualified procedure name and the first
// token of the caller's definition payload. Callers that use this path
// should pair it with ValidateProcedureWithOptions (same opts) to keep apply
// and validate coherent.
//
// When the procedure declares prior names via WithReplaces, each listed
// name is dropped (same guarded sys.procedures DROP IF EXISTS pattern as a
// regular procedure drop) before the current procedure is created.
// Cross-type drops are not attempted.
func ApplyProcedureWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, p *ProcedureDef) error {
	return applyProcedureWithOptionsInternal(ctx, db, d, opts, p, nil)
}

// applyProcedureWithOptionsInternal performs the per-procedure apply work.
// When dropped != nil, replacement names already dropped earlier in the
// batch are skipped so the same prior name is only dropped once across a
// batch.
func applyProcedureWithOptionsInternal(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, p *ProcedureDef, dropped map[string]struct{}) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	for _, repl := range p.Replaces() {
		key := objectKey(repl)
		if dropped != nil {
			if _, ok := dropped[key]; ok {
				continue
			}
			dropped[key] = struct{}{}
		}
		tmp := &ProcedureDef{Name: repl.Name, schema: repl.Schema}
		dropStmt, err := tmp.DropSQL(d)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, dropStmt); err != nil {
			return fmt.Errorf("schema: drop replaced procedure %q: %w", key, err)
		}
	}
	effOpts := effectiveOptionsForRender(d, opts)
	definition := applyOwnershipNoticePrefix(p.Definition(), effOpts, p.DocAnnotation())
	stmt, err := p.createOrAlterWithDefinition(d, definition)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("schema: apply procedure %q: %w", objectKey(p.Object()), err)
	}
	if opts.Metadata != nil {
		hash := hashCodeObjectDefinition(definition)
		if err := recordCodeObjectMetadata(ctx, db, d, *opts.Metadata, MetadataObjectTypeProcedure, p.Object(), hash); err != nil {
			return fmt.Errorf("schema: record metadata for procedure %q: %w", objectKey(p.Object()), err)
		}
	}
	return nil
}

// ApplyProcedures applies each declared procedure in caller-supplied order.
// Returns the first error encountered.
func ApplyProcedures(ctx context.Context, db *sql.DB, d chuck.Dialect, procs ...*ProcedureDef) error {
	return ApplyProceduresWithOptions(ctx, db, d, CodeObjectOptions{}, procs...)
}

// ApplyProceduresWithOptions is the option-aware counterpart to
// ApplyProcedures. Replacement-name drops are deduped across the batch so
// the same prior name is only issued one DROP statement even if it is
// declared via WithReplaces on multiple defs.
func ApplyProceduresWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, procs ...*ProcedureDef) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	dropped := map[string]struct{}{}
	for _, p := range procs {
		if err := applyProcedureWithOptionsInternal(ctx, db, d, opts, p, dropped); err != nil {
			return err
		}
	}
	return nil
}
