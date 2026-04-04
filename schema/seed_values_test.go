package schema

import (
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoValueToSQL(t *testing.T) {
	pg := chuck.PostgresDialect{}
	sq := chuck.SQLiteDialect{}
	ms := chuck.MSSQLDialect{}

	t.Run("nil", func(t *testing.T) {
		assert.Equal(t, "NULL", goValueToSQL(pg, nil))
		assert.Equal(t, "NULL", goValueToSQL(sq, nil))
		assert.Equal(t, "NULL", goValueToSQL(ms, nil))
	})

	t.Run("string", func(t *testing.T) {
		assert.Equal(t, "'hello'", goValueToSQL(pg, "hello"))
		assert.Equal(t, "'hello'", goValueToSQL(sq, "hello"))
		assert.Equal(t, "'hello'", goValueToSQL(ms, "hello"))
	})

	t.Run("string_with_single_quote", func(t *testing.T) {
		assert.Equal(t, "'it''s'", goValueToSQL(pg, "it's"))
		assert.Equal(t, "'it''s'", goValueToSQL(sq, "it's"))
		assert.Equal(t, "'it''s'", goValueToSQL(ms, "it's"))
	})

	t.Run("string_empty", func(t *testing.T) {
		assert.Equal(t, "''", goValueToSQL(pg, ""))
	})

	t.Run("int", func(t *testing.T) {
		assert.Equal(t, "42", goValueToSQL(pg, 42))
		assert.Equal(t, "0", goValueToSQL(sq, 0))
		assert.Equal(t, "-1", goValueToSQL(ms, -1))
	})

	t.Run("int64", func(t *testing.T) {
		assert.Equal(t, "9999999999", goValueToSQL(pg, int64(9999999999)))
	})

	t.Run("float64", func(t *testing.T) {
		assert.Equal(t, "3.14", goValueToSQL(pg, 3.14))
		assert.Equal(t, "0", goValueToSQL(sq, 0.0))
	})

	t.Run("bool_postgres", func(t *testing.T) {
		assert.Equal(t, "TRUE", goValueToSQL(pg, true))
		assert.Equal(t, "FALSE", goValueToSQL(pg, false))
	})

	t.Run("bool_sqlite", func(t *testing.T) {
		assert.Equal(t, "1", goValueToSQL(sq, true))
		assert.Equal(t, "0", goValueToSQL(sq, false))
	})

	t.Run("bool_mssql", func(t *testing.T) {
		assert.Equal(t, "1", goValueToSQL(ms, true))
		assert.Equal(t, "0", goValueToSQL(ms, false))
	})

	t.Run("sql_expr", func(t *testing.T) {
		expr := SQLExpr("CURRENT_TIMESTAMP")
		assert.Equal(t, "CURRENT_TIMESTAMP", goValueToSQL(pg, expr))
		assert.Equal(t, "CURRENT_TIMESTAMP", goValueToSQL(sq, expr))
		assert.Equal(t, "CURRENT_TIMESTAMP", goValueToSQL(ms, expr))
	})
}

func TestSeedValuesSQL(t *testing.T) {
	table := NewTable("Statuses").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
			Col("Label", TypeVarchar(100)).NotNull(),
			Col("Active", TypeBool()).NotNull(),
		).
		WithSeedValues(
			SeedValues{"Name": "active", "Label": "Active", "Active": true},
			SeedValues{"Name": "draft", "Label": "Draft", "Active": false},
		)

	assert.True(t, table.HasSeedData())

	t.Run("sqlite", func(t *testing.T) {
		stmts := table.SeedSQL(chuck.SQLiteDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], `INSERT OR IGNORE INTO "Statuses"`)
		assert.Contains(t, stmts[0], "'active'")
		assert.Contains(t, stmts[0], "'Active'")
		assert.Contains(t, stmts[0], "1") // bool true -> 1
		assert.Contains(t, stmts[1], "'draft'")
		assert.Contains(t, stmts[1], "0") // bool false -> 0
	})

	t.Run("postgres", func(t *testing.T) {
		stmts := table.SeedSQL(chuck.PostgresDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], `INSERT INTO "statuses"`)
		assert.Contains(t, stmts[0], "ON CONFLICT DO NOTHING")
		assert.Contains(t, stmts[0], "'active'")
		assert.Contains(t, stmts[0], "TRUE")
		assert.Contains(t, stmts[1], "FALSE")
	})

	t.Run("mssql", func(t *testing.T) {
		stmts := table.SeedSQL(chuck.MSSQLDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], "INSERT INTO [Statuses]")
		assert.Contains(t, stmts[0], "BEGIN TRY")
		assert.Contains(t, stmts[0], "'active'")
		assert.Contains(t, stmts[0], "1") // bool true -> 1
	})
}

func TestSeedValuesWithNil(t *testing.T) {
	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
			Col("Description", TypeText()),
		).
		WithSeedValues(
			SeedValues{"Name": "test", "Description": nil},
		)

	stmts := table.SeedSQL(chuck.SQLiteDialect{})
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "'test'")
	assert.Contains(t, stmts[0], "NULL")
}

func TestSeedValuesWithSQLExpr(t *testing.T) {
	table := NewTable("Events").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
			Col("CreatedAt", TypeTimestamp()).NotNull(),
		).
		WithSeedValues(
			SeedValues{"Name": "boot", "CreatedAt": SQLExpr("CURRENT_TIMESTAMP")},
		)

	stmts := table.SeedSQL(chuck.SQLiteDialect{})
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "'boot'")
	assert.Contains(t, stmts[0], "CURRENT_TIMESTAMP")
}

func TestSeedValuesNumericTypes(t *testing.T) {
	table := NewTable("Metrics").
		Columns(
			AutoIncrCol("ID"),
			Col("Count", TypeInt()).NotNull(),
			Col("BigCount", TypeBigInt()),
			Col("Rate", TypeFloat()),
		).
		WithSeedValues(
			SeedValues{"Count": 42, "BigCount": int64(9999999999), "Rate": 3.14},
		)

	stmts := table.SeedSQL(chuck.PostgresDialect{})
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "42")
	assert.Contains(t, stmts[0], "9999999999")
	assert.Contains(t, stmts[0], "3.14")
}

func TestSeedValuesStringEscaping(t *testing.T) {
	table := NewTable("Notes").
		Columns(
			AutoIncrCol("ID"),
			Col("Body", TypeText()).NotNull(),
		).
		WithSeedValues(
			SeedValues{"Body": "it's a \"test\""},
		)

	stmts := table.SeedSQL(chuck.SQLiteDialect{})
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "it''s a \"test\"")
}

func TestMixedSeedRowsAndSeedValues(t *testing.T) {
	table := NewTable("Statuses").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
			Col("Active", TypeBool()).NotNull(),
		).
		WithSeedRows(
			SeedRow{"Name": "'legacy'", "Active": "1"},
		).
		WithSeedValues(
			SeedValues{"Name": "modern", "Active": true},
		)

	assert.True(t, table.HasSeedData())

	stmts := table.SeedSQL(chuck.SQLiteDialect{})
	require.Len(t, stmts, 2)
	// First statement from SeedRow (raw SQL literals)
	assert.Contains(t, stmts[0], "'legacy'")
	// Second statement from SeedValues (Go values converted)
	assert.Contains(t, stmts[1], "'modern'")
	assert.Contains(t, stmts[1], "1")
}

func TestSeedValuesEmpty(t *testing.T) {
	table := NewTable("Empty").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)),
		)

	assert.False(t, table.HasSeedData())
	assert.Nil(t, table.SeedSQL(chuck.SQLiteDialect{}))
}
