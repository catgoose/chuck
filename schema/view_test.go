package schema

import (
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
)

const viewBody = "SELECT [ID], [Title] FROM [Tasks] WHERE [DeletedAt] IS NULL"

func TestNewView_UnqualifiedIdentity(t *testing.T) {
	v := NewView("v_active_tasks", viewBody)
	assert.Equal(t, "v_active_tasks", v.Name)
	assert.Equal(t, "", v.Schema())
	assert.Equal(t, viewBody, v.Body())
	assert.Equal(t, chuck.ObjectName{Name: "v_active_tasks"}, v.Object())
}

func TestNewQualifiedView_PreservesSchema(t *testing.T) {
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	assert.Equal(t, "sg", v.Schema())
	assert.Equal(t, chuck.ObjectName{Schema: "sg", Name: "v_pto_usage"}, v.Object())
}

func TestNewView_WithSchema_Equivalence(t *testing.T) {
	// WithSchema must produce the same identity as NewQualifiedView so callers
	// can mix declaration styles in the same owned-schema bundle.
	a := NewQualifiedView("sg", "v_pto_usage", viewBody)
	b := NewView("v_pto_usage", viewBody).WithSchema("sg")
	assert.Equal(t, a.Object(), b.Object())
	assert.Equal(t, a.Body(), b.Body())
}

func TestViewDef_QualifiedNameFor_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	assert.Equal(t, "[sg].[v_pto_usage]", v.QualifiedNameFor(d))
}

func TestViewDef_QualifiedNameFor_Postgres_NormalizesIdentifiers(t *testing.T) {
	d := chuck.PostgresDialect{}
	v := NewQualifiedView("App", "PTOUsageView", viewBody)
	// Postgres normalizes camelCase to snake_case.
	assert.Equal(t, `"app"."pto_usage_view"`, v.QualifiedNameFor(d))
}

func TestViewDef_QualifiedNameFor_SQLite_DropsSchema(t *testing.T) {
	d := chuck.SQLiteDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	// SQLite has no schema namespace; the schema component must be dropped.
	assert.Equal(t, `"v_pto_usage"`, v.QualifiedNameFor(d))
}

func TestViewDef_CreateSQL_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	got := v.CreateSQL(d)
	assert.Equal(t, "CREATE VIEW [sg].[v_pto_usage] AS "+viewBody, got)
}

func TestViewDef_CreateSQL_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	got := v.CreateSQL(d)
	assert.Equal(t, `CREATE VIEW "sg"."v_pto_usage" AS `+viewBody, got)
}

func TestViewDef_CreateSQL_SQLite(t *testing.T) {
	d := chuck.SQLiteDialect{}
	v := NewView("v_active_tasks", viewBody)
	got := v.CreateSQL(d)
	assert.Equal(t, `CREATE VIEW "v_active_tasks" AS `+viewBody, got)
}

func TestViewDef_CreateOrReplaceSQL_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	stmts := v.CreateOrReplaceSQL(d)
	assert.Equal(t,
		[]string{`CREATE OR REPLACE VIEW "sg"."v_pto_usage" AS ` + viewBody},
		stmts,
		"Postgres has a native CREATE OR REPLACE VIEW; emit a single statement",
	)
}

func TestViewDef_CreateOrReplaceSQL_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	stmts := v.CreateOrReplaceSQL(d)
	assert.Equal(t,
		[]string{"CREATE OR ALTER VIEW [sg].[v_pto_usage] AS " + viewBody},
		stmts,
		"MSSQL 2016+ supports CREATE OR ALTER VIEW as a single batch",
	)
}

func TestViewDef_CreateOrReplaceSQL_SQLite_EmitsDropThenCreate(t *testing.T) {
	d := chuck.SQLiteDialect{}
	v := NewView("v_active_tasks", viewBody)
	stmts := v.CreateOrReplaceSQL(d)
	// SQLite has no CREATE OR REPLACE / CREATE OR ALTER for views, so the
	// helper must emit DROP-then-CREATE in order.
	assert.Equal(t, []string{
		`DROP VIEW IF EXISTS "v_active_tasks"`,
		`CREATE VIEW "v_active_tasks" AS ` + viewBody,
	}, stmts)
}

func TestViewDef_DropSQL_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	assert.Equal(t, `DROP VIEW IF EXISTS "sg"."v_pto_usage"`, v.DropSQL(d))
}

func TestViewDef_DropSQL_SQLite(t *testing.T) {
	d := chuck.SQLiteDialect{}
	v := NewView("v_active_tasks", viewBody)
	assert.Equal(t, `DROP VIEW IF EXISTS "v_active_tasks"`, v.DropSQL(d))
}

func TestViewDef_DropSQL_MSSQL_QualifiedExistenceProbe(t *testing.T) {
	d := chuck.MSSQLDialect{}
	v := NewQualifiedView("sg", "v_pto_usage", viewBody)
	got := v.DropSQL(d)
	assert.Contains(t, got, "sys.views",
		"MSSQL drop must probe sys.views, not sys.objects, so it matches the view-only object class")
	assert.Contains(t, got, "OBJECT_ID(N'[sg].[v_pto_usage]')",
		"MSSQL existence probe should use the schema-qualified object literal")
	assert.Contains(t, got, "DROP VIEW [sg].[v_pto_usage]")
}

func TestViewDef_DropSQL_MSSQL_UnqualifiedExistenceProbe(t *testing.T) {
	d := chuck.MSSQLDialect{}
	v := NewView("v_active_tasks", viewBody)
	got := v.DropSQL(d)
	// Without a schema namespace, the existence probe must still produce a
	// valid OBJECT_ID literal — bare name in brackets.
	assert.Contains(t, got, "OBJECT_ID(N'[v_active_tasks]')")
	assert.Contains(t, got, "DROP VIEW [v_active_tasks]")
}
