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

// ProcedureDrift describes one declared procedure whose state in the live
// database does not match the declared definition. Either Missing is true,
// or DefinitionMismatch is true with DeclaredDefinition / LiveDefinition
// populated for diagnosis.
type ProcedureDrift struct {
	Object             chuck.ObjectName
	Missing            bool
	DefinitionMismatch bool
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
	for _, d := range e.Drifts {
		if !d.Missing {
			onlyMissing = false
		}
		if !d.DefinitionMismatch {
			onlyDefn = false
		}
	}
	switch {
	case onlyMissing:
		return ErrProcedureMissing
	case onlyDefn:
		return ErrProcedureDefinitionDrift
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
// ValidateProcedure. Ownership decoration is apply-owned, not
// declaration-owned: when opts.OwnershipNotice or opts.DocPreamble are
// configured, the exact rendered prefix is stripped from the live
// definition before canonical comparison. Live definitions that do not
// carry the configured markers still validate cleanly against the same
// declared definition; live definitions that carry a different leading
// comment (including a stale notice) still report drift.
func ValidateProcedureWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, p *ProcedureDef) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	live, exists, err := LiveProcedureDefinition(ctx, db, d, p)
	if err != nil {
		return err
	}
	if !exists {
		return &ProcedureDriftError{Drifts: []ProcedureDrift{{
			Object:  p.Object(),
			Missing: true,
			Reason:  "procedure does not exist",
		}}}
	}
	liveStripped := stripConfiguredApplyPrefix(live, opts, p.DocAnnotation())
	declaredCanon := canonicalizeStatement(declaredDefinitionWithAnnotation(p))
	liveCanon := canonicalizeStatement(liveStripped)
	if declaredCanon == liveCanon {
		return nil
	}
	return &ProcedureDriftError{Drifts: []ProcedureDrift{{
		Object:             p.Object(),
		DefinitionMismatch: true,
		DeclaredDefinition: declaredCanon,
		LiveDefinition:     liveCanon,
		Reason:             "procedure definition differs after canonical normalization",
	}}}
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
func ValidateProceduresWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, procs ...*ProcedureDef) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	var drifts []ProcedureDrift
	for _, p := range procs {
		err := ValidateProcedureWithOptions(ctx, db, d, opts, p)
		if err == nil {
			continue
		}
		var drift *ProcedureDriftError
		if errors.As(err, &drift) {
			drifts = append(drifts, drift.Drifts...)
			continue
		}
		return err
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
func ApplyProcedureWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, p *ProcedureDef) error {
	definition := applyOwnershipNoticePrefix(p.Definition(), opts, p.DocAnnotation())
	stmt, err := p.createOrAlterWithDefinition(d, definition)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("schema: apply procedure %q: %w", objectKey(p.Object()), err)
	}
	return nil
}

// ApplyProcedures applies each declared procedure in caller-supplied order.
// Returns the first error encountered.
func ApplyProcedures(ctx context.Context, db *sql.DB, d chuck.Dialect, procs ...*ProcedureDef) error {
	return ApplyProceduresWithOptions(ctx, db, d, CodeObjectOptions{}, procs...)
}

// ApplyProceduresWithOptions is the option-aware counterpart to
// ApplyProcedures.
func ApplyProceduresWithOptions(ctx context.Context, db *sql.DB, d chuck.Dialect, opts CodeObjectOptions, procs ...*ProcedureDef) error {
	if d.Engine() != chuck.MSSQL {
		return fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	for _, p := range procs {
		if err := ApplyProcedureWithOptions(ctx, db, d, opts, p); err != nil {
			return err
		}
	}
	return nil
}
