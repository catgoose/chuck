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
// The body is taken verbatim. Callers own all inner identifier quoting,
// parameter declarations, and any RETURN / SET / control-flow inside the
// procedure body. The package wraps that body with the dialect-correct
// CREATE OR ALTER PROCEDURE preamble and emits a safe DROP path.
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
	Name   string
	schema string
	body   string
}

// NewProcedure creates an unqualified owned procedure with the given name and
// raw body. The body is the text that follows `CREATE OR ALTER PROCEDURE
// <name> AS` — typically parameter declarations followed by `BEGIN ... END`,
// without a trailing semicolon.
func NewProcedure(name, body string) *ProcedureDef {
	return &ProcedureDef{Name: name, body: body}
}

// NewQualifiedProcedure creates an owned procedure with an explicit schema
// namespace. Equivalent to NewProcedure(name, body).WithSchema(schema).
func NewQualifiedProcedure(schema, name, body string) *ProcedureDef {
	return &ProcedureDef{Name: name, schema: schema, body: body}
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

// Body returns the raw body declared for the procedure.
func (p *ProcedureDef) Body() string {
	return p.body
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
// this is a single `CREATE OR ALTER PROCEDURE <qualified-name> AS <body>`
// statement; MSSQL requires this to be the only statement in its batch, which
// matches how the rest of chuck emits MSSQL DDL (one statement per Exec).
//
// Returns ErrProcedureDialectUnsupported on non-MSSQL dialects.
func (p *ProcedureDef) CreateOrAlterSQL(d chuck.Dialect) (string, error) {
	if d.Engine() != chuck.MSSQL {
		return "", fmt.Errorf("%w: %s", ErrProcedureDialectUnsupported, d.Engine())
	}
	return fmt.Sprintf("CREATE OR ALTER PROCEDURE %s AS %s", p.QualifiedNameFor(d), p.body), nil
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
