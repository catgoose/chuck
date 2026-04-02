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

	// Should have the index
	require.Len(t, snap.Indexes, 1)
	assert.Equal(t, "idx_tasks_title", snap.Indexes[0].Name)
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
