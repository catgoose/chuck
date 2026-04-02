package chuck

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/mssql"
	_ "github.com/catgoose/chuck/driver/postgres"
	_ "github.com/catgoose/chuck/driver/sqlite"
)

// dialectIntegrationTest runs the same DDL/DML lifecycle against any dialect+db.
// It verifies that all dialect fragments produce valid SQL for the target engine.
func dialectIntegrationTest(t *testing.T, db *sql.DB, d Dialect) {
	t.Helper()
	ctx := context.Background()

	t.Run("CreateTable", func(t *testing.T) {
		boolDefault := "1"
		if d.Engine() == Postgres {
			boolDefault = "TRUE"
		}
		body := "id " + d.AutoIncrement() +
			", name " + d.VarcharType(255) + " NOT NULL" +
			", bio " + d.TextType() +
			", active " + d.BoolType() + " DEFAULT " + boolDefault +
			", score " + d.IntType() + " DEFAULT 0" +
			", created_at " + d.TimestampType()
		createSQL := d.CreateTableIfNotExists("chuck_test", body)
		_, err := db.ExecContext(ctx, createSQL)
		require.NoError(t, err)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		indexSQL := d.CreateIndexIfNotExists("idx_chuck_test_name", "chuck_test", "name")
		_, err := db.ExecContext(ctx, indexSQL)
		require.NoError(t, err)

		// Creating the same index again should not error (IF NOT EXISTS)
		_, err = db.ExecContext(ctx, indexSQL)
		require.NoError(t, err)
	})

	t.Run("TableExists", func(t *testing.T) {
		var result interface{}
		err := db.QueryRowContext(ctx, d.TableExistsQuery(), "chuck_test").Scan(&result)
		require.NoError(t, err, "table chuck_test should exist")
	})

	t.Run("TableColumns", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, d.TableColumnsQuery(), "chuck_test")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var columns []string
		for rows.Next() {
			var col string
			require.NoError(t, rows.Scan(&col))
			columns = append(columns, col)
		}
		require.NoError(t, rows.Err())
		assert.Contains(t, columns, "id")
		assert.Contains(t, columns, "name")
		assert.Contains(t, columns, "bio")
		assert.Contains(t, columns, "active")
		assert.Contains(t, columns, "score")
		assert.Contains(t, columns, "created_at")
	})

	t.Run("InsertAndQuery", func(t *testing.T) {
		insertSQL := "INSERT INTO chuck_test (name, bio, active, score) VALUES (" +
			d.Placeholder(1) + ", " + d.Placeholder(2) + ", " +
			d.Placeholder(3) + ", " + d.Placeholder(4) + ")"
		_, err := db.ExecContext(ctx, insertSQL, "Gobo", "A brave explorer", 1, 42)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, insertSQL, "Red", "An athletic explorer", 1, 99)
		require.NoError(t, err)

		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM chuck_test").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("Now", func(t *testing.T) {
		// Verify Now() is valid SQL for this engine
		var ts interface{}
		err := db.QueryRowContext(ctx, "SELECT "+d.Now()).Scan(&ts)
		require.NoError(t, err)
		assert.NotNil(t, ts)
	})

	t.Run("CreateTableIdempotent", func(t *testing.T) {
		// Running CreateTableIfNotExists again should not error
		body := "id " + d.AutoIncrement() + ", name " + d.VarcharType(255)
		createSQL := d.CreateTableIfNotExists("chuck_test", body)
		_, err := db.ExecContext(ctx, createSQL)
		require.NoError(t, err)
	})

	t.Run("ForeignKeyInlineReferences", func(t *testing.T) {
		// Create a parent table with IDENTITY/auto-increment PK
		parentBody := "id " + d.AutoIncrement() + ", label " + d.VarcharType(100) + " NOT NULL"
		parentSQL := d.CreateTableIfNotExists("chuck_fk_parent", parentBody)
		_, err := db.ExecContext(ctx, parentSQL)
		require.NoError(t, err, "create parent table with auto-increment PK")

		// Create a child table with inline REFERENCES to the parent's IDENTITY column
		childBody := "id " + d.AutoIncrement() +
			", parent_id " + d.IntType() + " NOT NULL REFERENCES " +
			d.QuoteIdentifier("chuck_fk_parent") + "(" + d.QuoteIdentifier("id") + ")" +
			", value " + d.VarcharType(100)
		childSQL := d.CreateTableIfNotExists("chuck_fk_child", childBody)
		_, err = db.ExecContext(ctx, childSQL)
		require.NoError(t, err, "create child table with inline REFERENCES on IDENTITY column")

		// Insert a parent row
		insertParent := "INSERT INTO " + d.QuoteIdentifier("chuck_fk_parent") +
			" (label) VALUES (" + d.Placeholder(1) + ")"
		_, err = db.ExecContext(ctx, insertParent, "parent1")
		require.NoError(t, err)

		// Insert a child row referencing the parent
		insertChild := "INSERT INTO " + d.QuoteIdentifier("chuck_fk_child") +
			" (parent_id, value) VALUES (" + d.Placeholder(1) + ", " + d.Placeholder(2) + ")"
		_, err = db.ExecContext(ctx, insertChild, 1, "child1")
		require.NoError(t, err)

		// Verify the relationship
		var count int
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+d.QuoteIdentifier("chuck_fk_child")+" WHERE parent_id = "+d.Placeholder(1), 1).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		// Clean up (child first due to FK constraint)
		_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_fk_child"))
		_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_fk_parent"))
	})

	t.Run("DropTable", func(t *testing.T) {
		dropSQL := d.DropTableIfExists("chuck_test")
		_, err := db.ExecContext(ctx, dropSQL)
		require.NoError(t, err)

		// Table should be gone
		var result interface{}
		err = db.QueryRowContext(ctx, d.TableExistsQuery(), "chuck_test").Scan(&result)
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("DropTableIdempotent", func(t *testing.T) {
		// Dropping again should not error
		dropSQL := d.DropTableIfExists("chuck_test")
		_, err := db.ExecContext(ctx, dropSQL)
		require.NoError(t, err)
	})
}

func TestIntegrationSQLite(t *testing.T) {
	ctx := context.Background()
	db, d, err := OpenSQLite(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dialectIntegrationTest(t, db, d)
}

func TestIntegrationPostgres(t *testing.T) {
	dsn := os.Getenv("CHUCK_POSTGRES_URL")
	if dsn == "" {
		t.Skip("CHUCK_POSTGRES_URL not set")
	}

	ctx := context.Background()
	db, d, err := OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Clean up in case a previous run failed mid-test
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_fk_child"))
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_fk_parent"))
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_test"))

	dialectIntegrationTest(t, db, d)
}

func TestIntegrationMSSQL(t *testing.T) {
	dsn := os.Getenv("CHUCK_MSSQL_URL")
	if dsn == "" {
		t.Skip("CHUCK_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Clean up in case a previous run failed mid-test
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_fk_child"))
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_fk_parent"))
	_, _ = db.ExecContext(ctx, d.DropTableIfExists("chuck_test"))

	dialectIntegrationTest(t, db, d)
}
