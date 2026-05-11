package dbrepo

import (
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
)

func TestParseObjectName_Helper(t *testing.T) {
	cases := []struct {
		in   string
		want chuck.ObjectName
	}{
		{"Users", chuck.ObjectName{Name: "Users"}},
		{"sg.SalesAgents", chuck.ObjectName{Schema: "sg", Name: "SalesAgents"}},
		{" sg.agents ", chuck.ObjectName{Schema: "sg", Name: "agents"}},
		{"", chuck.ObjectName{}},
		{".trailing", chuck.ObjectName{Name: ".trailing"}},
		{"leading.", chuck.ObjectName{Name: "leading."}},
		{"(SELECT 1) sub", chuck.ObjectName{Name: "(SELECT 1) sub"}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, chuck.ParseObjectName(tc.in))
		})
	}
}

func TestSelectBuilder_QualifiedFrom_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	sql, _ := NewSelect("sg.SalesAgents", "ID", "Name").
		WithDialect(d).
		Build()
	assert.Equal(t, "SELECT ID, Name FROM [sg].[SalesAgents]", sql)
}

func TestSelectBuilder_QualifiedFrom_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	sql, _ := NewSelect("sg.SalesAgents", "ID", "Name").
		WithDialect(d).
		Build()
	assert.Equal(t, `SELECT ID, Name FROM "sg"."sales_agents"`, sql)
}

func TestSelectBuilder_QualifiedFrom_SQLite_DropsSchema(t *testing.T) {
	d := chuck.SQLiteDialect{}
	sql, _ := NewSelect("sg.SalesAgents", "ID").
		WithDialect(d).
		Build()
	assert.Equal(t, `SELECT ID FROM "SalesAgents"`, sql,
		"SQLite has no schema namespace; the schema prefix must be dropped")
}

func TestSelectBuilder_QualifiedJoin_MSSQL_NoAlias(t *testing.T) {
	d := chuck.MSSQLDialect{}
	sql, _ := NewSelect("Tasks", "Tasks.ID").
		Join("sg.SalesAgents", "Tasks.AgentID = sg.SalesAgents.ID").
		WithDialect(d).
		Build()
	assert.Equal(t,
		`SELECT [Tasks].[ID] FROM [Tasks] JOIN [sg].[SalesAgents] ON Tasks.AgentID = sg.SalesAgents.ID`,
		sql,
	)
}

func TestSelectBuilder_QualifiedJoin_MSSQL_WithAlias(t *testing.T) {
	d := chuck.MSSQLDialect{}
	sql, _ := NewSelect("Tasks", "Tasks.ID", "sa.Name").
		Join("sg.SalesAgents sa", "sa.ID = Tasks.AgentID").
		WithDialect(d).
		Build()
	assert.Equal(t,
		`SELECT [Tasks].[ID], [sa].[Name] FROM [Tasks] JOIN [sg].[SalesAgents] sa ON sa.ID = Tasks.AgentID`,
		sql,
	)
}

func TestSelectBuilder_QualifiedJoin_MSSQL_AsAlias(t *testing.T) {
	d := chuck.MSSQLDialect{}
	sql, _ := NewSelect("Tasks", "Tasks.ID").
		Join("sg.SalesAgents AS sa", "sa.ID = Tasks.AgentID").
		WithDialect(d).
		Build()
	assert.Equal(t,
		`SELECT [Tasks].[ID] FROM [Tasks] JOIN [sg].[SalesAgents] AS sa ON sa.ID = Tasks.AgentID`,
		sql,
	)
}

func TestSelectBuilder_QualifiedJoin_Postgres_AsAlias(t *testing.T) {
	d := chuck.PostgresDialect{}
	sql, _ := NewSelect("Tasks", "Tasks.ID").
		Join("sg.SalesAgents AS sa", "sa.ID = Tasks.AgentID").
		WithDialect(d).
		Build()
	assert.Equal(t,
		`SELECT "tasks"."id" FROM "tasks" JOIN "sg"."sales_agents" AS sa ON sa.ID = Tasks.AgentID`,
		sql,
	)
}

func TestSelectBuilder_ThreePartColumnRef_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	sql, _ := NewSelect("sg.SalesAgents", "sg.SalesAgents.ID", "sg.SalesAgents.Name").
		WithDialect(d).
		Build()
	assert.Equal(t,
		"SELECT [sg].[SalesAgents].[ID], [sg].[SalesAgents].[Name] FROM [sg].[SalesAgents]",
		sql,
	)
}

func TestSelectBuilder_ThreePartColumnRef_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	sql, _ := NewSelect("sg.SalesAgents", "sg.SalesAgents.ID").
		WithDialect(d).
		Build()
	assert.Equal(t,
		`SELECT "sg"."sales_agents"."id" FROM "sg"."sales_agents"`,
		sql,
	)
}

func TestSelectBuilder_CountQuery_QualifiedFrom(t *testing.T) {
	d := chuck.MSSQLDialect{}
	sql, _ := NewSelect("sg.SalesAgents", "ID").
		WithDialect(d).
		CountQuery()
	assert.Equal(t, "SELECT COUNT(*) FROM [sg].[SalesAgents]", sql)
}

func TestSelectBuilder_CountQuery_QualifiedJoin(t *testing.T) {
	d := chuck.PostgresDialect{}
	sql, _ := NewSelect("Tasks", "Tasks.ID").
		Join("sg.SalesAgents sa", "sa.ID = Tasks.AgentID").
		WithDialect(d).
		CountQuery()
	assert.Equal(t,
		`SELECT COUNT(*) FROM "tasks" JOIN "sg"."sales_agents" sa ON sa.ID = Tasks.AgentID`,
		sql,
	)
}

func TestUpdateBuilder_QualifiedTable_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	w := NewWhere().And("ID = @ID")
	sql, _ := NewUpdate("sg.SalesAgents", "Name").
		Where(w).
		WithDialect(d).
		Build()
	assert.Equal(t,
		"UPDATE [sg].[SalesAgents] SET [Name] = @Name WHERE ID = @ID",
		sql,
	)
}

func TestUpdateBuilder_QualifiedTable_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	w := NewWhere().And("ID = @ID")
	sql, _ := NewUpdate("sg.SalesAgents", "Name").
		Where(w).
		WithDialect(d).
		Build()
	assert.Equal(t,
		`UPDATE "sg"."sales_agents" SET "name" = @Name WHERE ID = @ID`,
		sql,
	)
}

func TestDeleteBuilder_QualifiedTable_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	w := NewWhere().And("ID = @ID")
	sql, _ := NewDelete("sg.SalesAgents").
		Where(w).
		WithDialect(d).
		Build()
	assert.Equal(t, "DELETE FROM [sg].[SalesAgents] WHERE ID = @ID", sql)
}

func TestInsertIntoQ_QualifiedTable_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	got := InsertIntoQ(d, "sg.SalesAgents", "Name", "Email")
	assert.Equal(t,
		"INSERT INTO [sg].[SalesAgents] ([Name], [Email]) VALUES (@Name, @Email)",
		got,
	)
}

func TestInsertIntoQ_QualifiedTable_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	got := InsertIntoQ(d, "sg.SalesAgents", "Name", "Email")
	assert.Equal(t,
		`INSERT INTO "sg"."sales_agents" ("name", "email") VALUES (@Name, @Email)`,
		got,
	)
}

func TestInsertIntoQ_QualifiedTable_SQLite_DropsSchema(t *testing.T) {
	d := chuck.SQLiteDialect{}
	got := InsertIntoQ(d, "sg.SalesAgents", "Name")
	assert.Equal(t,
		`INSERT INTO "SalesAgents" ("Name") VALUES (@Name)`,
		got,
	)
}

func TestBulkInsertInto_QualifiedTable_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	got := BulkInsertInto(d, "sg.SalesAgents", []string{"Name", "Email"}, 2)
	assert.Equal(t,
		`INSERT INTO "sg"."sales_agents" ("name", "email") VALUES ($1, $2), ($3, $4)`,
		got,
	)
}

func TestUpsertIntoQ_QualifiedTable_Postgres(t *testing.T) {
	d := chuck.PostgresDialect{}
	got := UpsertIntoQ(d, "sg.SalesAgents", []string{"Email"}, "Email", "Name")
	assert.Equal(t,
		`INSERT INTO "sg"."sales_agents" ("email", "name") VALUES (@Email, @Name) ON CONFLICT ("email") DO UPDATE SET "name" = EXCLUDED."name"`,
		got,
	)
}

func TestUpsertIntoQ_QualifiedTable_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	got := UpsertIntoQ(d, "sg.SalesAgents", []string{"Email"}, "Email", "Name")
	assert.Contains(t, got, "MERGE [sg].[SalesAgents] AS Target")
	assert.Contains(t, got, "Target.[Email] = Source.[Email]")
}

func TestUpsertIntoQ_QualifiedTable_SQLite_DropsSchema(t *testing.T) {
	d := chuck.SQLiteDialect{}
	got := UpsertIntoQ(d, "sg.SalesAgents", []string{"Email"}, "Email", "Name")
	assert.Contains(t, got, `INSERT INTO "SalesAgents"`)
	assert.NotContains(t, got, "sg.")
}

func TestSelectBuilder_DerivedTablePassthrough(t *testing.T) {
	d := chuck.PostgresDialect{}
	// Derived-table targets contain parentheses and must be passed through
	// unchanged; the builder does not attempt to parse subqueries.
	sql, _ := NewSelect("(SELECT 1) sub", "ID").
		WithDialect(d).
		Build()
	assert.Equal(t, `SELECT ID FROM (SELECT 1) sub`, sql)
}

func TestSelectBuilder_RawJoinPassthrough(t *testing.T) {
	d := chuck.PostgresDialect{}
	// Raw subquery join targets must be passed through unchanged.
	sql, _ := NewSelect("Tasks", "Tasks.ID").
		Join("(SELECT id FROM users) u", "u.id = Tasks.UserID").
		WithDialect(d).
		Build()
	assert.Equal(t,
		`SELECT "tasks"."id" FROM "tasks" JOIN (SELECT id FROM users) u ON u.id = Tasks.UserID`,
		sql,
	)
}

func TestQuoteQualifiedColumn_Passthrough(t *testing.T) {
	d := chuck.PostgresDialect{}
	// Bare column names stay unquoted to preserve back-compat with callers
	// that pass raw expressions.
	assert.Equal(t, "Name", quoteQualifiedColumn(d, "Name"))
	// Expression-like inputs are returned verbatim.
	assert.Equal(t, "COUNT(*)", quoteQualifiedColumn(d, "COUNT(*)"))
	assert.Equal(t, "a + b", quoteQualifiedColumn(d, "a + b"))
}
