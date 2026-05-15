package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDropForeignKeySQL_MSSQL_QualifiedParent(t *testing.T) {
	d := chuck.MSSQLDialect{}
	stmt := DropForeignKeySQL(d, ForeignKeyRef{
		Name:             "FK__Goals__AgentID__1234ABCD",
		ParentSchema:     "sg",
		ParentTable:      "Goals",
		ReferencedSchema: "sg",
		ReferencedTable:  "SalesAgents",
	})
	assert.Equal(t,
		"ALTER TABLE [sg].[Goals] DROP CONSTRAINT [FK__Goals__AgentID__1234ABCD]",
		stmt,
	)
}

func TestDropForeignKeySQL_MSSQL_DboDefault(t *testing.T) {
	d := chuck.MSSQLDialect{}
	stmt := DropForeignKeySQL(d, ForeignKeyRef{
		Name:             "FK__Posts__UserID",
		ParentSchema:     "dbo",
		ParentTable:      "Posts",
		ReferencedSchema: "dbo",
		ReferencedTable:  "Users",
	})
	assert.Equal(t,
		"ALTER TABLE [dbo].[Posts] DROP CONSTRAINT [FK__Posts__UserID]",
		stmt,
	)
}

func TestOwnedTableKeySet_DefaultsUnqualifiedToDefaultSchema(t *testing.T) {
	d := chuck.MSSQLDialect{}
	tables := []*TableDef{
		NewTable("Users").Columns(AutoIncrCol("ID")),
		NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID")),
	}
	owned := ownedTableKeySet(d, tables, "dbo")
	assert.Contains(t, owned, "dbo.Users",
		"unqualified declarations must default to engine default schema")
	assert.Contains(t, owned, "sg.SalesAgents",
		"qualified declarations must preserve their schema")
	assert.Len(t, owned, 2)
}

func TestOwnedFK_MatchesParentSide(t *testing.T) {
	owned := map[string]struct{}{"sg.Goals": {}}
	fk := ForeignKeyRef{
		ParentSchema:     "sg",
		ParentTable:      "Goals",
		ReferencedSchema: "ext",
		ReferencedTable:  "Catalog",
	}
	assert.True(t, ownedFK(owned, fk),
		"owned FK detection must trigger when parent table is owned")
}

func TestOwnedFK_MatchesReferencedSide(t *testing.T) {
	owned := map[string]struct{}{"sg.SalesAgents": {}}
	fk := ForeignKeyRef{
		ParentSchema:     "ext",
		ParentTable:      "AuditLog",
		ReferencedSchema: "sg",
		ReferencedTable:  "SalesAgents",
	}
	assert.True(t, ownedFK(owned, fk),
		"owned FK detection must trigger when referenced table is owned so outside tables can be detached")
}

func TestOwnedFK_NoMatchWhenBothSidesExternal(t *testing.T) {
	owned := map[string]struct{}{"sg.SalesAgents": {}}
	fk := ForeignKeyRef{
		ParentSchema:     "ext",
		ParentTable:      "AuditLog",
		ReferencedSchema: "ext",
		ReferencedTable:  "Catalog",
	}
	assert.False(t, ownedFK(owned, fk),
		"fully external FKs must be ignored to keep the destructive bootstrap scope tight")
}

func TestInboundForeignKeys_NonMSSQL_ReturnsNil(t *testing.T) {
	// Helper must be safely callable from dialect-agnostic bootstrap code.
	// On engines other than MSSQL it should report no work without touching db.
	td := NewQualifiedTable("sg", "SalesAgents").Columns(AutoIncrCol("ID"))
	ctx := context.Background()

	for _, d := range []chuck.Dialect{chuck.PostgresDialect{}, chuck.SQLiteDialect{}} {
		t.Run(string(d.Engine()), func(t *testing.T) {
			fks, err := InboundForeignKeys(ctx, (*sql.DB)(nil), d, td)
			require.NoError(t, err)
			assert.Nil(t, fks, "non-MSSQL engines must skip the helper without query")
		})
	}
}

func TestInboundForeignKeys_EmptyTableSet_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	fks, err := InboundForeignKeys(ctx, (*sql.DB)(nil), chuck.MSSQLDialect{})
	require.NoError(t, err)
	assert.Nil(t, fks, "empty owned set must short-circuit without touching the database")
}
