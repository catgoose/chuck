package schema

import (
	"errors"
	"fmt"

	"github.com/catgoose/chuck"
)

// ErrProcedureDialectUnsupported is returned when a ProcedureDef lifecycle
// method is invoked against a dialect that has no first-class procedure
// support in this package. The first pass is MSSQL-only; callers running on
// Postgres or SQLite get an explicit error rather than a silent no-op so that
// owned-procedure declarations cannot quietly disappear from a bootstrap.
var ErrProcedureDialectUnsupported = errors.New("schema: stored procedure ownership is MSSQL-only in this release")

// ProcedureDef declares an owned MSSQL stored procedure as a first-class
// object alongside TableDef and ViewDef. Identity mirrors ViewDef: an optional
// schema namespace plus a name, rendered through the same structured
// ObjectName path so qualified procedures emit [schema].[name] consistently
// with the rest of the package.
//
// The definition payload is the full T-SQL text that follows the qualified
// procedure name. T-SQL grammar places parameter declarations and procedure
// options (e.g. WITH RECOMPILE, WITH ENCRYPTION) between the procedure name
// and the AS keyword, so the package cannot inject AS for the caller without
// closing off those slots. Callers therefore own everything from optional
// parameters through AS through the body itself, and the package wraps that
// text with the dialect-correct CREATE OR ALTER PROCEDURE preamble plus the
// qualified identifier.
//
// Non-MSSQL dialects are explicitly unsupported in this first pass; the
// lifecycle methods return ErrProcedureDialectUnsupported rather than
// silently no-op'ing so callers cannot accidentally lose ownership coverage
// on an engine that does not know how to apply it.
//
// Ordering between owned procedures and the tables/views they depend on is
// caller-owned. Procedures typically form a short list of refresh / migration
// entrypoints layered on top of the table graph; forcing them into a
// generalized scheduler would add weight without buying clarity.
type ProcedureDef struct {
	Name          string
	schema        string
	definition    string
	docAnnotation string
	replaces      []chuck.ObjectName
}

// NewProcedure creates an unqualified owned procedure with the given name and
// raw definition. The definition is the full T-SQL text that follows the
// qualified procedure name: optional parameter declarations, optional WITH
// options, the required AS keyword, and the procedure body. Example shape:
//
//	"@AgentID INT, @AsOf DATETIME2 = NULL\nWITH RECOMPILE\nAS\nBEGIN ... END"
//
// Callers own all inner identifier quoting and the AS keyword itself; the
// package contributes only the CREATE OR ALTER PROCEDURE preamble and the
// qualified identifier.
func NewProcedure(name, definition string) *ProcedureDef {
	return &ProcedureDef{Name: name, definition: definition}
}

// NewQualifiedProcedure creates an owned procedure with an explicit schema
// namespace. Equivalent to NewProcedure(name, definition).WithSchema(schema).
func NewQualifiedProcedure(schema, name, definition string) *ProcedureDef {
	return &ProcedureDef{Name: name, schema: schema, definition: definition}
}

// WithSchema sets the schema namespace for the procedure. Procedure ownership
// is MSSQL-only in this release, so the namespace is always rendered when
// present.
func (p *ProcedureDef) WithSchema(schema string) *ProcedureDef {
	p.schema = schema
	return p
}

// Schema returns the declared schema namespace for the procedure, or "" if
// none.
func (p *ProcedureDef) Schema() string {
	return p.schema
}

// WithDocAnnotation attaches a per-procedure doc comment to the declaration.
// When set, the annotation renders as a SQL block comment as part of the
// procedure's live SQL output, last among the leading comments (after any
// caller-level OwnershipNotice and DocPreamble), closest to the procedure
// definition. Validation intentionally ignores leading block-comment front
// matter, so changing this annotation does not by itself produce drift. This
// is the per-object counterpart to CodeObjectOptions.DocPreamble.
func (p *ProcedureDef) WithDocAnnotation(text string) *ProcedureDef {
	p.docAnnotation = text
	return p
}

// DocAnnotation returns the declared per-procedure doc annotation, or "" if
// none.
func (p *ProcedureDef) DocAnnotation() string {
	return p.docAnnotation
}

// WithReplaces declares prior names this procedure supersedes — used to
// retire renamed or replaced procedures explicitly without schema-wide
// pruning. Apply*WithOptions drops each listed name (exact schema + name
// match, same object type) before creating the current procedure;
// Validate*WithOptions reports each listed name as stale if it still exists
// in the live database. Cross-type replacement is intentionally not supported.
//
// Multiple calls accumulate; batch apply / validate helpers dedupe duplicate
// names across the batch so the same prior name is only dropped or checked
// once. Names are matched by structured ObjectName: schema and name must
// match exactly, with no schema-wide globbing.
func (p *ProcedureDef) WithReplaces(names ...chuck.ObjectName) *ProcedureDef {
	p.replaces = append(p.replaces, names...)
	return p
}

// Replaces returns the declared prior names this procedure supersedes, in
// caller-supplied order.
func (p *ProcedureDef) Replaces() []chuck.ObjectName {
	return p.replaces
}

// Definition returns the raw T-SQL definition declared for the procedure —
// everything from optional parameter declarations through AS through the
// procedure body.
func (p *ProcedureDef) Definition() string {
	return p.definition
}

// Object returns the structured ObjectName for the procedure.
func (p *ProcedureDef) Object() chuck.ObjectName {
	return chuck.ObjectName{Schema: p.schema, Name: p.Name}
}

// QualifiedNameFor returns the dialect-rendered, quoted, schema-qualified
// procedure identifier (e.g. [sg].[usp_RefreshDashboard] on MSSQL). It
// shares the same identifier-rendering path as TableDef and ViewDef so
// procedure references stay consistent with the rest of the package's DDL.
// Non-MSSQL dialects still render the identifier so callers building error
// messages or logs can quote the procedure name; only the lifecycle methods
// return ErrProcedureDialectUnsupported.
func (p *ProcedureDef) QualifiedNameFor(d chuck.Dialect) string {
	return qualifyTable(d, p.Object())
}

// CreateOrAlterSQL returns the dialect-idiomatic statement that creates the
// procedure, replacing any prior definition with the same identity. On MSSQL
// this is a single `CREATE OR ALTER PROCEDURE <qualified-name> <definition>`
// statement; MSSQL requires this to be the only statement in its batch, which
// matches how the rest of chuck emits MSSQL DDL (one statement per Exec).
//
// The package contributes the CREATE OR ALTER PROCEDURE preamble and the
// qualified identifier. The caller's definition supplies any parameter
// declarations, procedure options, the required AS keyword, and the body, in
// the order T-SQL demands.
//
// Returns ErrProcedureDialectUnsupported on non-MSSQL dialects.
func (p *ProcedureDef) CreateOrAlterSQL(d chuck.Dialect) (string, error) {
	return p.createOrAlterWithDefinition(d, p.definition)
}

// createOrAlterWithDefinition renders the same dialect-idiomatic
// CREATE OR ALTER PROCEDURE statement as CreateOrAlterSQL, but with a
// caller-supplied definition override. Used by the option-aware Apply /
// Validate helpers to inject an ownership notice into the definition payload
// without mutating the underlying ProcedureDef. The injected notice sits in a
// grammar-safe position (between the qualified name and the first token of
// the caller-supplied definition) because T-SQL accepts `/* ... */` block
// comments wherever whitespace is legal — including before parameter
// declarations and the AS keyword.
func (p *ProcedureDef) createOrAlterWithDefinition(d chuck.Dialect, definition string) (string, error) {
	if d.Engine() != chuck.MSSQL {
		return "", fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	return joinSQLHeadPayload(fmt.Sprintf("CREATE OR ALTER PROCEDURE %s", p.QualifiedNameFor(d)), definition), nil
}

// DropSQL returns a single safe `DROP PROCEDURE` statement that probes
// sys.procedures first, so callers can run it unconditionally during
// teardown. The probe uses the schema-qualified object literal so the right
// procedure is matched even when the same bare name exists under multiple
// schemas.
//
// Returns ErrProcedureDialectUnsupported on non-MSSQL dialects.
func (p *ProcedureDef) DropSQL(d chuck.Dialect) (string, error) {
	if d.Engine() != chuck.MSSQL {
		return "", fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	qt := p.QualifiedNameFor(d)
	schema, name := normalizedObject(d, p.Object())
	objArg := fmt.Sprintf("[%s]", name)
	if schema != "" {
		objArg = fmt.Sprintf("[%s].[%s]", schema, name)
	}
	return fmt.Sprintf(
		"IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'%s')) BEGIN DROP PROCEDURE %s; END",
		escapeQuote(objArg), qt,
	), nil
}
