package schema

import (
	"fmt"

	"github.com/catgoose/chuck"
)

// ViewDef declares an owned database view as a first-class object alongside
// TableDef. A view carries an optional schema namespace and a SELECT body; the
// package emits dialect-aware lifecycle SQL (create / create-or-replace / drop)
// so callers no longer need raw CREATE VIEW string constants next to their
// declarative table ownership.
//
// View identity follows the same structured ObjectName model as TableDef so
// schema-qualified views render correctly on MSSQL and Postgres and collapse
// to bare names on SQLite. The SELECT body is taken verbatim — callers are
// responsible for any inner identifier quoting, which keeps the API thin and
// matches the "explicit SQL, composable helpers" stance of the rest of chuck.
//
// Dependency ordering between views and the tables they read is intentionally
// left to the caller. Views typically form a small linear chain on top of
// owned tables; forcing a generalized scheduler abstraction for that case
// would add weight without buying clarity. Callers wanting explicit ordering
// can simply create views after their tables and drop them before.
type ViewDef struct {
	Name          string
	schema        string
	body          string
	docAnnotation string
}

// NewView creates an unqualified owned view with the given name and SELECT
// body. The body is the text that follows `CREATE VIEW <name> AS` — typically
// a SELECT statement — without a trailing semicolon.
func NewView(name, body string) *ViewDef {
	return &ViewDef{Name: name, body: body}
}

// NewQualifiedView creates an owned view with an explicit schema namespace.
// Equivalent to NewView(name, body).WithSchema(schema).
func NewQualifiedView(schema, name, body string) *ViewDef {
	return &ViewDef{Name: name, schema: schema, body: body}
}

// WithSchema sets the schema namespace for the view. SQLite ignores this
// because it has no schema namespace; other dialects render qualified
// identifiers like "schema"."view" or [schema].[view].
func (v *ViewDef) WithSchema(schema string) *ViewDef {
	v.schema = schema
	return v
}

// Schema returns the declared schema namespace for the view, or "" if none.
func (v *ViewDef) Schema() string {
	return v.schema
}

// WithDocAnnotation attaches a declaration-owned doc comment to the view. When
// set, the annotation renders as a SQL block comment as part of the view's
// own SQL output (between any caller-level DocPreamble and any caller-level
// OwnershipNotice) and participates in body-drift comparison: changing the
// annotation in code produces validation drift against a live view rendered
// from the prior annotation. This is the per-object counterpart to
// CodeObjectOptions.DocPreamble, which remains a caller-level apply-owned
// preamble shared across many objects.
func (v *ViewDef) WithDocAnnotation(text string) *ViewDef {
	v.docAnnotation = text
	return v
}

// DocAnnotation returns the declared declaration-owned doc annotation for the
// view, or "" if none.
func (v *ViewDef) DocAnnotation() string {
	return v.docAnnotation
}

// Body returns the raw SELECT body declared for the view.
func (v *ViewDef) Body() string {
	return v.body
}

// Object returns the structured ObjectName for the view.
func (v *ViewDef) Object() chuck.ObjectName {
	return chuck.ObjectName{Schema: v.schema, Name: v.Name}
}

// QualifiedNameFor returns the dialect-rendered, quoted, schema-qualified
// view identifier (e.g. [sg].[v_PTOUsage] on MSSQL, "sg"."v_pto_usage" on
// Postgres). On SQLite the schema component is dropped.
func (v *ViewDef) QualifiedNameFor(d chuck.Dialect) string {
	return qualifyTable(d, v.Object())
}

// CreateSQL returns a plain `CREATE VIEW <qualified-name> AS <body>` statement.
// This is the minimal, non-idempotent lifecycle primitive — use
// CreateOrReplaceSQL when re-running bootstrap should refresh an existing view.
func (v *ViewDef) CreateSQL(d chuck.Dialect) string {
	return fmt.Sprintf("CREATE VIEW %s AS %s", v.QualifiedNameFor(d), v.body)
}

// CreateOrReplaceSQL returns the dialect-idiomatic statements that create the
// view, replacing any prior definition with the same identity. The return
// type is a slice because SQLite has no CREATE OR REPLACE / CREATE OR ALTER
// equivalent for views and must emit a DROP-then-CREATE pair; callers should
// execute the slice in order:
//
//   - Postgres: `CREATE OR REPLACE VIEW ... AS ...` (single statement)
//   - MSSQL:    `CREATE OR ALTER VIEW ... AS ...` (single statement; MSSQL 2016+)
//   - SQLite:   `DROP VIEW IF EXISTS ...` followed by `CREATE VIEW ... AS ...`
//
// MSSQL `CREATE OR ALTER VIEW` requires the entire batch to be a standalone
// statement, which matches how the rest of chuck emits MSSQL DDL (one
// statement per Exec).
func (v *ViewDef) CreateOrReplaceSQL(d chuck.Dialect) []string {
	return v.createOrReplaceWithBody(d, v.body)
}

// createOrReplaceWithBody renders the same dialect-idiomatic create-or-replace
// statements as CreateOrReplaceSQL, but with a caller-supplied body override.
// Used by the option-aware Apply / Validate helpers to inject an ownership
// notice into the body payload without mutating the underlying ViewDef.
func (v *ViewDef) createOrReplaceWithBody(d chuck.Dialect, body string) []string {
	qt := v.QualifiedNameFor(d)
	switch d.Engine() {
	case chuck.Postgres:
		return []string{fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", qt, body)}
	case chuck.MSSQL:
		return []string{fmt.Sprintf("CREATE OR ALTER VIEW %s AS %s", qt, body)}
	case chuck.SQLite:
		return []string{
			fmt.Sprintf("DROP VIEW IF EXISTS %s", qt),
			fmt.Sprintf("CREATE VIEW %s AS %s", qt, body),
		}
	default:
		return []string{
			fmt.Sprintf("DROP VIEW IF EXISTS %s", qt),
			fmt.Sprintf("CREATE VIEW %s AS %s", qt, body),
		}
	}
}

// DropSQL returns a single `DROP VIEW IF EXISTS` statement rendered for the
// dialect. MSSQL wraps the drop in a sys.views existence probe (matching the
// table-drop pattern) so callers can run it unconditionally during teardown.
func (v *ViewDef) DropSQL(d chuck.Dialect) string {
	qt := v.QualifiedNameFor(d)
	switch d.Engine() {
	case chuck.MSSQL:
		schema, name := normalizedObject(d, v.Object())
		var objArg string
		if schema == "" {
			objArg = fmt.Sprintf("[%s]", name)
		} else {
			objArg = fmt.Sprintf("[%s].[%s]", schema, name)
		}
		return fmt.Sprintf(
			"IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'%s')) BEGIN DROP VIEW %s; END",
			escapeQuote(objArg), qt,
		)
	default:
		return fmt.Sprintf("DROP VIEW IF EXISTS %s", qt)
	}
}
