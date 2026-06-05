package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
)

func TestRetiredTable_Constructors(t *testing.T) {
	bare := RetiredTable("GroupMembershipSources")
	assert.Equal(t, "GroupMembershipSources", bare.Name())
	assert.Equal(t, "", bare.Schema())
	assert.Equal(t, chuck.ObjectName{Name: "GroupMembershipSources"}, bare.Object())

	qual := RetiredQualifiedTable("sg", "RetiredAgents")
	assert.Equal(t, "RetiredAgents", qual.Name())
	assert.Equal(t, "sg", qual.Schema())
	assert.Equal(t, chuck.ObjectName{Schema: "sg", Name: "RetiredAgents"}, qual.Object())
}

func TestRetiredTable_QualifiedNameFor_PerDialect(t *testing.T) {
	r := RetiredQualifiedTable("sg", "RetiredAgents")
	assert.Equal(t, "[sg].[RetiredAgents]", r.QualifiedNameFor(chuck.MSSQLDialect{}))
	assert.Equal(t, `"sg"."retired_agents"`, r.QualifiedNameFor(chuck.PostgresDialect{}))
	assert.Equal(t, `"RetiredAgents"`, r.QualifiedNameFor(chuck.SQLiteDialect{}),
		"SQLite must drop the schema component because it has no namespace")
}

func TestRetiredTable_DropSQL_PerDialect(t *testing.T) {
	r := RetiredQualifiedTable("dbo", "GroupMembershipSources")

	mssql := r.DropSQL(chuck.MSSQLDialect{})
	assert.Contains(t, mssql, "OBJECT_ID(N'[dbo].[GroupMembershipSources]')",
		"MSSQL drop must wrap in sys.objects existence probe with schema-qualified arg")
	assert.Contains(t, mssql, "DROP TABLE [dbo].[GroupMembershipSources]")

	pg := r.DropSQL(chuck.PostgresDialect{})
	assert.Equal(t, `DROP TABLE IF EXISTS "dbo"."group_membership_sources"`, pg)

	lite := r.DropSQL(chuck.SQLiteDialect{})
	assert.Equal(t, `DROP TABLE IF EXISTS "GroupMembershipSources"`, lite,
		"SQLite must drop the schema component and use the simple IF EXISTS form")
}

func TestDropRetiredTables_NilSlice_Noop(t *testing.T) {
	require.NoError(t, DropRetiredTables(context.Background(), nil, chuck.SQLiteDialect{}))
}

func TestDropRetiredTables_EmptyName_FailsLoud(t *testing.T) {
	err := DropRetiredTables(context.Background(), nil, chuck.SQLiteDialect{}, RetiredTable(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty name")
}

func TestDropRetiredTables_NilEntry_FailsLoud(t *testing.T) {
	err := DropRetiredTables(context.Background(), nil, chuck.SQLiteDialect{}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestDedupeRetired_PreservesOrderAndDedupesByIdentity(t *testing.T) {
	in := []*RetiredTableDef{
		RetiredQualifiedTable("dbo", "Old"),
		RetiredTable("Bare"),
		RetiredQualifiedTable("dbo", "Old"),    // duplicate
		RetiredQualifiedTable("sg", "Old"),     // distinct schema → distinct identity
		RetiredTable("Bare"),                   // duplicate
		RetiredQualifiedTable("dbo", "Another"),
	}
	out, err := dedupeRetired(in)
	require.NoError(t, err)
	require.Len(t, out, 4)
	assert.Equal(t, "dbo", out[0].Schema())
	assert.Equal(t, "Old", out[0].Name())
	assert.Equal(t, "", out[1].Schema())
	assert.Equal(t, "Bare", out[1].Name())
	assert.Equal(t, "sg", out[2].Schema())
	assert.Equal(t, "Old", out[2].Name())
	assert.Equal(t, "dbo", out[3].Schema())
	assert.Equal(t, "Another", out[3].Name())
}

func TestRetiredTableKeySet_DefaultsUnqualifiedToDefaultSchema(t *testing.T) {
	d := chuck.MSSQLDialect{}
	retired := []*RetiredTableDef{
		RetiredTable("GroupMembershipSources"),
		RetiredQualifiedTable("sg", "RetiredAgents"),
	}
	keys := retiredTableKeySet(d, retired, "dbo")
	assert.Contains(t, keys, "dbo.GroupMembershipSources",
		"unqualified retired declarations must default to the engine default schema")
	assert.Contains(t, keys, "sg.RetiredAgents",
		"qualified retired declarations must keep their schema")
	assert.Len(t, keys, 2)
}

// TestDropRetiredTables_SQLite_LiveDrop covers the SQLite live-drop path:
// declared retired tombstones for tables that exist must be dropped; tombstones
// for already-absent tables must succeed as no-ops via the IF EXISTS form.
func TestDropRetiredTables_SQLite_LiveDrop(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	// Two existing tables we will retire, plus a tombstone that names a table
	// that does not exist; the IF EXISTS form must keep the call clean.
	_, err = db.ExecContext(ctx, `CREATE TABLE retired_alpha (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE retired_beta (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)

	require.NoError(t, DropRetiredTables(ctx, db, d,
		RetiredTable("retired_alpha"),
		RetiredTable("retired_beta"),
		RetiredTable("retired_alpha"), // duplicate must not double-drop or error
		RetiredTable("never_existed"), // tombstone for absent table is IF EXISTS clean
	))

	for _, name := range []string{"retired_alpha", "retired_beta", "never_existed"} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n))
		assert.Equalf(t, 0, n, "retired table %q must be absent after DropRetiredTables", name)
	}
}

// TestDropRetiredTables_SQLite_ErrorContext asserts that a non-IF-EXISTS-style
// failure surfaces the qualified identity of the failing tombstone. SQLite's
// IF EXISTS form makes a genuine DROP failure hard to engineer in unit tests,
// so we close the connection first and confirm the resulting error wraps the
// retired table's display name.
func TestDropRetiredTables_SQLite_ErrorContext(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	err = DropRetiredTables(context.Background(), db, chuck.SQLiteDialect{},
		RetiredTable("retired_alpha"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drop retired table")
	assert.Contains(t, err.Error(), "retired_alpha")
}
