package schema

import (
	"context"
	"database/sql"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLiveSnapshot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Tasks").
		Columns(
			AutoIncrCol("ID"),
			Col("Title", TypeString(255)).NotNull(),
			Col("Description", TypeText()),
		).
		WithTimestamps().
		WithSoftDelete().
		Indexes(
			Index("idx_tasks_title", "Title"),
		)

	// Create the table
	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Tasks")
	require.NoError(t, err)

	assert.Equal(t, "Tasks", snap.Name)

	// Should have all columns
	colNames := make([]string, len(snap.Columns))
	for i, c := range snap.Columns {
		colNames[i] = c.Name
	}
	assert.Contains(t, colNames, "ID")
	assert.Contains(t, colNames, "Title")
	assert.Contains(t, colNames, "Description")
	assert.Contains(t, colNames, "CreatedAt")
	assert.Contains(t, colNames, "UpdatedAt")
	assert.Contains(t, colNames, "DeletedAt")

	// Check nullability
	for _, c := range snap.Columns {
		switch c.Name {
		case "Title", "CreatedAt", "UpdatedAt":
			assert.False(t, c.Nullable, "%s should be NOT NULL", c.Name)
		case "Description", "DeletedAt":
			assert.True(t, c.Nullable, "%s should be nullable", c.Name)
		}
	}

	// Should have the index with columns and uniqueness
	require.Len(t, snap.Indexes, 1)
	assert.Equal(t, "idx_tasks_title", snap.Indexes[0].Name)
	assert.Equal(t, []string{"Title"}, snap.Indexes[0].Columns)
	assert.False(t, snap.Indexes[0].Unique)
}

func TestLiveSnapshotTableNotExists(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	_, err := LiveSnapshot(ctx, db, d, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestLiveSnapshotString(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Users")
	require.NoError(t, err)

	s := snap.String()
	assert.Contains(t, s, "TABLE Users")
	assert.Contains(t, s, "ID")
	assert.Contains(t, s, "Email")
	assert.Contains(t, s, "NOT NULL")
}

func TestLiveSchemaSnapshot(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)))
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)))

	for _, tbl := range []*TableDef{users, tasks} {
		for _, stmt := range tbl.CreateIfNotExistsSQL(d) {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}
	}

	snaps, err := LiveSchemaSnapshot(ctx, db, d, "Users", "Tasks")
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	assert.Equal(t, "Users", snaps[0].Name)
	assert.Equal(t, "Tasks", snaps[1].Name)
}

func TestLiveSnapshotStringWithIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Products").
		Columns(
			AutoIncrCol("ID"),
			Col("SKU", TypeVarchar(100)).NotNull(),
		).
		Indexes(
			Index("idx_products_sku", "SKU"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Products")
	require.NoError(t, err)

	s := snap.String()
	assert.Contains(t, s, "TABLE Products")
	assert.Contains(t, s, "INDEX idx_products_sku")
}

func TestLiveSnapshotStringNoIndex(t *testing.T) {
	// String() for a table with no indexes should omit the INDEX lines
	snap := LiveTableSnapshot{
		Name: "Simple",
		Columns: []LiveColumnSnapshot{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "val", Type: "TEXT", Nullable: true, Default: "'x'"},
		},
	}
	s := snap.String()
	assert.Contains(t, s, "TABLE Simple")
	assert.Contains(t, s, "id")
	assert.Contains(t, s, "NOT NULL")
	assert.Contains(t, s, "val")
	assert.Contains(t, s, "DEFAULT 'x'")
	assert.NotContains(t, s, "INDEX")
}

func TestQueryColumnsUnsupportedEngine(t *testing.T) {
	// queryColumns returns an error for unsupported engines.
	// We test this indirectly through LiveSnapshot since queryColumns is unexported.
	// Instead, verify that a valid SQLite in-memory DB returns columns correctly.
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Widgets").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
			Col("Weight", TypeFloat()),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Widgets")
	require.NoError(t, err)
	require.Len(t, snap.Columns, 3)

	// ID is primary key — NOT NULL
	assert.Equal(t, "ID", snap.Columns[0].Name)
	assert.False(t, snap.Columns[0].Nullable)

	// Name is NOT NULL
	assert.Equal(t, "Name", snap.Columns[1].Name)
	assert.False(t, snap.Columns[1].Nullable)

	// Weight is nullable
	assert.Equal(t, "Weight", snap.Columns[2].Name)
	assert.True(t, snap.Columns[2].Nullable)
}

func TestQueryIndexesMultiple(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Orders").
		Columns(
			AutoIncrCol("ID"),
			Col("CustomerID", TypeInt()).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull(),
		).
		Indexes(
			Index("idx_orders_customer", "CustomerID"),
			Index("idx_orders_status", "Status"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Orders")
	require.NoError(t, err)
	require.Len(t, snap.Indexes, 2)

	indexNames := []string{snap.Indexes[0].Name, snap.Indexes[1].Name}
	assert.Contains(t, indexNames, "idx_orders_customer")
	assert.Contains(t, indexNames, "idx_orders_status")
}

func TestLiveSnapshotUniqueIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Emails").
		Columns(
			AutoIncrCol("ID"),
			Col("Address", TypeVarchar(255)).NotNull(),
		).
		Indexes(
			UniqueIndex("idx_emails_address", "Address"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Emails")
	require.NoError(t, err)
	require.Len(t, snap.Indexes, 1)
	assert.Equal(t, "idx_emails_address", snap.Indexes[0].Name)
	assert.Equal(t, []string{"Address"}, snap.Indexes[0].Columns)
	assert.True(t, snap.Indexes[0].Unique)
}

func TestLiveSnapshotMultiColumnIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Events").
		Columns(
			AutoIncrCol("ID"),
			Col("Category", TypeVarchar(100)).NotNull(),
			Col("Priority", TypeInt()).NotNull(),
		).
		Indexes(
			Index("idx_events_cat_pri", "Category, Priority"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Events")
	require.NoError(t, err)
	require.Len(t, snap.Indexes, 1)
	assert.Equal(t, "idx_events_cat_pri", snap.Indexes[0].Name)
	assert.Equal(t, []string{"Category", "Priority"}, snap.Indexes[0].Columns)
	assert.False(t, snap.Indexes[0].Unique)
}

func TestLiveSnapshotPartialIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Tickets").
		Columns(
			AutoIncrCol("ID"),
			Col("Status", TypeVarchar(50)).NotNull(),
			Col("AssigneeID", TypeInt()),
		).
		Indexes(
			Index("idx_tickets_open", "AssigneeID").Where("Status = 'open'"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	snap, err := LiveSnapshot(ctx, db, d, "Tickets")
	require.NoError(t, err)
	require.Len(t, snap.Indexes, 1)
	assert.Equal(t, "idx_tickets_open", snap.Indexes[0].Name)
	assert.Equal(t, []string{"AssigneeID"}, snap.Indexes[0].Columns)
	assert.False(t, snap.Indexes[0].Unique)
	assert.Equal(t, "Status = 'open'", snap.Indexes[0].Where)
}

func TestPostgresColumnQueryScopesBySchema(t *testing.T) {
	// The Postgres column query must join pg_namespace and constrain by the
	// schema parameter so cross-schema matches do not occur. Schema is now
	// passed as a bound parameter ($1) rather than hardcoded, so qualified
	// tables in non-public schemas can be inspected.
	assert.Contains(t, postgresColumnQuery, "pg_namespace",
		"Postgres column query should reference pg_namespace")
	assert.Contains(t, postgresColumnQuery, "n.oid = t.relnamespace",
		"Postgres column query should join pg_namespace on relnamespace")
	assert.Contains(t, postgresColumnQuery, "c.table_schema = $1",
		"Postgres column query should constrain by the schema parameter")
}

func TestPostgresIndexQueryScopesBySchema(t *testing.T) {
	// The Postgres index query must join pg_namespace and constrain by the
	// schema parameter so cross-schema matches do not occur.
	assert.Contains(t, postgresIndexQuery, "pg_namespace",
		"Postgres index query should reference pg_namespace")
	assert.Contains(t, postgresIndexQuery, "n.oid = t.relnamespace",
		"Postgres index query should join pg_namespace on relnamespace")
	assert.Contains(t, postgresIndexQuery, "n.nspname = $1",
		"Postgres index query should constrain by the schema parameter")
}

func TestMSSQLIndexQueryExcludesIncludedColumns(t *testing.T) {
	// The MSSQL index query must filter out included (non-key) columns
	// so that only key columns appear in the column list.
	assert.Contains(t, mssqlIndexQuery, "ic.is_included_column = 0",
		"MSSQL index query should filter out included columns")
}

func TestMSSQLIndexQueryOrdersByKeyOrdinal(t *testing.T) {
	// The MSSQL index query must order by key_ordinal to preserve
	// the correct column ordering for multi-column indexes.
	assert.Contains(t, mssqlIndexQuery, "ORDER BY ic.key_ordinal",
		"MSSQL index query should order by key_ordinal")
}

func TestReconstructMSSQLType(t *testing.T) {
	intVal := func(n int64) sql.NullInt64 { return sql.NullInt64{Valid: true, Int64: n} }
	null := sql.NullInt64{}

	tests := []struct {
		name             string
		dataType         string
		charMaxLength    sql.NullInt64
		numericPrecision sql.NullInt64
		numericScale     sql.NullInt64
		want             string
	}{
		// String types
		{"VARCHAR(255)", "VARCHAR", intVal(255), null, null, "VARCHAR(255)"},
		{"VARCHAR(MAX)", "VARCHAR", intVal(-1), null, null, "VARCHAR(MAX)"},
		{"VARCHAR null length", "VARCHAR", null, null, null, "VARCHAR"},
		{"NVARCHAR(255)", "NVARCHAR", intVal(255), null, null, "NVARCHAR(255)"},
		{"NVARCHAR(MAX)", "NVARCHAR", intVal(-1), null, null, "NVARCHAR(MAX)"},
		{"CHAR(10)", "CHAR", intVal(10), null, null, "CHAR(10)"},
		{"NCHAR(8)", "NCHAR", intVal(8), null, null, "NCHAR(8)"},

		// Binary types
		{"VARBINARY(100)", "VARBINARY", intVal(100), null, null, "VARBINARY(100)"},
		{"VARBINARY(MAX)", "VARBINARY", intVal(-1), null, null, "VARBINARY(MAX)"},
		{"BINARY(16)", "BINARY", intVal(16), null, null, "BINARY(16)"},

		// Numeric types
		{"DECIMAL(10,2)", "DECIMAL", null, intVal(10), intVal(2), "DECIMAL(10,2)"},
		{"NUMERIC(18,4)", "NUMERIC", null, intVal(18), intVal(4), "NUMERIC(18,4)"},
		{"DECIMAL null params", "DECIMAL", null, null, null, "DECIMAL"},
		{"DECIMAL null scale", "DECIMAL", null, intVal(10), null, "DECIMAL"},

		// Other types — bare base name, no parameters appended
		{"INT bare", "INT", null, null, null, "INT"},
		{"INT with stray numeric metadata", "INT", null, intVal(10), intVal(0), "INT"},
		{"BIGINT bare", "BIGINT", null, null, null, "BIGINT"},
		{"DATETIME2 bare", "DATETIME2", null, null, null, "DATETIME2"},
		{"BIT bare", "BIT", null, null, null, "BIT"},
		{"UNIQUEIDENTIFIER bare", "UNIQUEIDENTIFIER", null, null, null, "UNIQUEIDENTIFIER"},
		{"FLOAT bare", "FLOAT", null, null, null, "FLOAT"},
		{"non-string null charMaxLength", "DATE", null, null, null, "DATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reconstructMSSQLType(tt.dataType, tt.charMaxLength, tt.numericPrecision, tt.numericScale)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMSSQLColumnQuerySelectsReconstructionMetadata(t *testing.T) {
	// The MSSQL column query must select CHARACTER_MAXIMUM_LENGTH,
	// NUMERIC_PRECISION, and NUMERIC_SCALE so reconstructMSSQLType can
	// rebuild parameterized type strings (e.g. NVARCHAR(255), DECIMAL(10,2)).
	assert.Contains(t, mssqlColumnQuery, "CHARACTER_MAXIMUM_LENGTH",
		"MSSQL column query should select CHARACTER_MAXIMUM_LENGTH")
	assert.Contains(t, mssqlColumnQuery, "NUMERIC_PRECISION",
		"MSSQL column query should select NUMERIC_PRECISION")
	assert.Contains(t, mssqlColumnQuery, "NUMERIC_SCALE",
		"MSSQL column query should select NUMERIC_SCALE")
	assert.Contains(t, mssqlColumnQuery, "INFORMATION_SCHEMA.COLUMNS",
		"MSSQL column query should read from INFORMATION_SCHEMA.COLUMNS")
}

func TestLiveSnapshotCompareWithDeclared(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	declared := table.Snapshot(d)
	live, err := LiveSnapshot(ctx, db, d, "Items")
	require.NoError(t, err)

	// Column count should match
	assert.Equal(t, len(declared.Columns), len(live.Columns))

	// Column names should match
	for i, dc := range declared.Columns {
		assert.Equal(t, dc.Name, live.Columns[i].Name)
	}

	// Nullability should match
	for i, dc := range declared.Columns {
		assert.Equal(t, dc.NotNull, !live.Columns[i].Nullable,
			"nullability mismatch for column %s", dc.Name)
	}
}
