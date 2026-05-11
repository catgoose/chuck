package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableDef_WithSchema_Object(t *testing.T) {
	td := NewTable("SalesAgents").WithSchema("sg")
	assert.Equal(t, "sg", td.Schema())
	assert.Equal(t, chuck.ObjectName{Schema: "sg", Name: "SalesAgents"}, td.Object())
}

func TestNewQualifiedTable_Equivalence(t *testing.T) {
	a := NewQualifiedTable("sg", "SalesAgents")
	b := NewTable("SalesAgents").WithSchema("sg")
	assert.Equal(t, a.Object(), b.Object())
}

func TestQualifiedNameFor_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents")
	assert.Equal(t, "[sg].[SalesAgents]", td.QualifiedNameFor(d))
}

func TestQualifiedNameFor_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	td := NewQualifiedTable("App", "SalesAgents")
	// Postgres normalizes camelCase to snake_case.
	assert.Equal(t, `"app"."sales_agents"`, td.QualifiedNameFor(d))
}

func TestQualifiedNameFor_SQLite_DropsSchema(t *testing.T) {
	d := chuck.SQLiteDialect{}
	td := NewQualifiedTable("sg", "SalesAgents")
	// SQLite has no schema namespace; the schema component must be dropped.
	assert.Equal(t, `"SalesAgents"`, td.QualifiedNameFor(d))
}

func TestQualifiedNameFor_Unqualified_BackCompat(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewTable("Users")
	assert.Equal(t, "[Users]", td.QualifiedNameFor(d))
}

func TestCreateSQL_QualifiedMSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	stmts := td.CreateSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], "CREATE TABLE [sg].[SalesAgents]")
}

func TestCreateIfNotExistsSQL_QualifiedMSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	stmts := td.CreateIfNotExistsSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], "OBJECT_ID(N'[sg].[SalesAgents]')",
		"MSSQL existence probe should use the schema-qualified object literal")
	assert.Contains(t, stmts[0], "CREATE TABLE [sg].[SalesAgents]")
}

func TestCreateIfNotExistsSQL_QualifiedPostgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	stmts := td.CreateIfNotExistsSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], `CREATE TABLE IF NOT EXISTS "sg"."sales_agents"`)
}

func TestDropSQL_QualifiedMSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents")
	got := td.DropSQL(d)
	assert.Contains(t, got, "OBJECT_ID(N'[sg].[SalesAgents]')")
	assert.Contains(t, got, "DROP TABLE [sg].[SalesAgents]")
}

func TestDropSQL_QualifiedPostgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	td := NewQualifiedTable("sg", "SalesAgents")
	assert.Equal(t, `DROP TABLE IF EXISTS "sg"."sales_agents"`, td.DropSQL(d))
}

func TestSeedSQL_QualifiedMSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "Lookup").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
		).
		WithSeedRows(SeedRow{"Name": "'alpha'"})
	stmts := td.SeedSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], "INSERT INTO [sg].[Lookup]")
}

func TestSeedSQL_QualifiedPostgres_OnConflictDoNothing(t *testing.T) {
	d := chuck.PostgresDialect{}
	td := NewQualifiedTable("sg", "Lookup").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
		).
		WithSeedRows(SeedRow{"Name": "'alpha'"})
	stmts := td.SeedSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], `INSERT INTO "sg"."lookup"`)
	assert.Contains(t, stmts[0], "ON CONFLICT DO NOTHING")
}

func TestForeignKey_QualifiedReference_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "Goals").
		Columns(
			AutoIncrCol("ID"),
			Col("AgentID", TypeInt()).NotNull().
				ReferencesQualified("sg", "SalesAgents", "ID"),
		)
	stmts := td.CreateSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], "REFERENCES [sg].[SalesAgents]([ID])")
}

func TestForeignKey_QualifiedReference_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	td := NewQualifiedTable("sg", "Goals").
		Columns(
			AutoIncrCol("ID"),
			Col("AgentID", TypeInt()).NotNull().
				ReferencesQualified("sg", "SalesAgents", "ID"),
		)
	stmts := td.CreateSQL(d)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], `REFERENCES "sg"."sales_agents"("id")`)
}

func TestForeignKey_UnqualifiedBackCompat(t *testing.T) {
	d := chuck.PostgresDialect{}
	td := NewTable("Goals").Columns(
		AutoIncrCol("ID"),
		Col("AgentID", TypeInt()).References("SalesAgents", "ID"),
	)
	stmts := td.CreateSQL(d)
	require.NotEmpty(t, stmts)
	// No schema prefix when target is unqualified.
	assert.Contains(t, stmts[0], `REFERENCES "sales_agents"("id")`)
	assert.NotContains(t, stmts[0], `"public"."sales_agents"`)
}

func TestSnapshot_PreservesSchema(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").
		Columns(AutoIncrCol("ID"))
	snap := td.Snapshot(d)
	assert.Equal(t, "sg", snap.Schema)
	assert.Equal(t, "SalesAgents", snap.Name)
	assert.Equal(t, chuck.ObjectName{Schema: "sg", Name: "SalesAgents"}, snap.Object())
}

func TestSnapshot_FK_StructuredTarget(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "Goals").
		Columns(
			AutoIncrCol("ID"),
			Col("AgentID", TypeInt()).ReferencesQualified("sg", "SalesAgents", "ID"),
		)
	snap := td.Snapshot(d)
	require.Len(t, snap.Columns, 2)
	fkCol := snap.Columns[1]
	assert.Equal(t, "sg", fkCol.RefSchema)
	assert.Equal(t, "SalesAgents", fkCol.RefTable)
	assert.Equal(t, "ID", fkCol.RefColumn)
}

func TestSnapshot_SQLite_FlattensSchema(t *testing.T) {
	d := chuck.SQLiteDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	snap := td.Snapshot(d)
	assert.Empty(t, snap.Schema, "SQLite snapshot must not carry a schema component")
	assert.Equal(t, "SalesAgents", snap.Name)
}

func TestSnapshotString_QualifiedHeader(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	s := td.SnapshotString(d)
	assert.True(t, strings.HasPrefix(s, "TABLE sg.SalesAgents\n"),
		"SnapshotString should render the qualified TABLE header; got %q", s)
}

func TestCreationOrder_QualifiedDuplicateBareNames(t *testing.T) {
	// sg.SalesAgents and cl.SalesAgents must be treated as distinct nodes.
	sgAgents := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	clAgents := NewQualifiedTable("cl", "SalesAgents").Columns(AutoIncrCol("ID"))
	sgGoals := NewQualifiedTable("sg", "Goals").Columns(
		AutoIncrCol("ID"),
		Col("AgentID", TypeInt()).ReferencesQualified("sg", "SalesAgents", "ID"),
	)
	clGoals := NewQualifiedTable("cl", "Goals").Columns(
		AutoIncrCol("ID"),
		Col("AgentID", TypeInt()).ReferencesQualified("cl", "SalesAgents", "ID"),
	)
	sorted, err := CreationOrder(clGoals, sgGoals, clAgents, sgAgents)
	require.NoError(t, err)
	require.Len(t, sorted, 4)

	pos := make(map[string]int, 4)
	for i, t := range sorted {
		pos[t.Object().String()] = i
	}
	assert.Less(t, pos["sg.SalesAgents"], pos["sg.Goals"])
	assert.Less(t, pos["cl.SalesAgents"], pos["cl.Goals"])
}

func TestCreationOrder_QualifiedFKDoesNotCrossSchemas(t *testing.T) {
	// sg.Goals references sg.SalesAgents and must NOT pick cl.SalesAgents
	// even though the bare name matches.
	sgAgents := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	clAgents := NewQualifiedTable("cl", "SalesAgents").Columns(AutoIncrCol("ID"))
	sgGoals := NewQualifiedTable("sg", "Goals").Columns(
		AutoIncrCol("ID"),
		Col("AgentID", TypeInt()).ReferencesQualified("sg", "SalesAgents", "ID"),
	)
	sorted, err := CreationOrder(sgGoals, clAgents, sgAgents)
	require.NoError(t, err)
	require.Len(t, sorted, 3)
	pos := make(map[string]int, 3)
	for i, t := range sorted {
		pos[t.Object().String()] = i
	}
	assert.Less(t, pos["sg.SalesAgents"], pos["sg.Goals"])
}

func TestCheckSchemaCompatibility_SQLiteCollision(t *testing.T) {
	d := chuck.SQLiteDialect{}
	a := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	b := NewQualifiedTable("cl", "SalesAgents").Columns(AutoIncrCol("ID"))
	err := CheckSchemaCompatibility(d, []*TableDef{a, b})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSQLiteSchemaCollision),
		"expected ErrSQLiteSchemaCollision; got %v", err)
	assert.Contains(t, err.Error(), "SalesAgents")
}

func TestCheckSchemaCompatibility_SQLite_NoCollision(t *testing.T) {
	d := chuck.SQLiteDialect{}
	a := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	b := NewQualifiedTable("cl", "SalesGoals").Columns(AutoIncrCol("ID"))
	c := NewTable("Users").Columns(AutoIncrCol("ID"))
	assert.NoError(t, CheckSchemaCompatibility(d, []*TableDef{a, b, c}))
}

func TestCheckSchemaCompatibility_NonSQLite_NoCollisionCheck(t *testing.T) {
	d := chuck.MSSQLDialect{}
	a := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	b := NewQualifiedTable("cl", "SalesAgents").Columns(AutoIncrCol("ID"))
	// MSSQL distinguishes them by schema, so the collision check must not fire.
	assert.NoError(t, CheckSchemaCompatibility(d, []*TableDef{a, b}))
}

func TestQualifiedIndex_MSSQL_UsesQualifiedObjectID(t *testing.T) {
	d := chuck.MSSQLDialect{}
	td := NewQualifiedTable("sg", "SalesAgents").
		Columns(AutoIncrCol("ID"), Col("Name", TypeVarchar(50))).
		Indexes(Index("idx_sg_agents_name", "Name"))
	stmts := td.CreateIfNotExistsSQL(d)
	require.Len(t, stmts, 2)
	idxStmt := stmts[1]
	assert.Contains(t, idxStmt, "OBJECT_ID(N'sg.SalesAgents')",
		"MSSQL index probe should target schema-qualified OBJECT_ID")
	assert.Contains(t, idxStmt, "ON [sg].[SalesAgents]")
}
