package schema_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/catgoose/chuck/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/mssql"
	_ "github.com/catgoose/chuck/driver/postgres"
	_ "github.com/catgoose/chuck/driver/sqlite"
)

// testTable defines a representative table using most schema features.
var testTable = schema.NewTable("chuck_schema_test").
	Columns(
		schema.AutoIncrCol("ID"),
		schema.Col("Name", schema.TypeString(255)).NotNull(),
		schema.Col("Email", schema.TypeVarchar(255)).NotNull().Unique(),
		schema.Col("Bio", schema.TypeText()),
		schema.Col("Score", schema.TypeInt()).NotNull().Default("0"),
		schema.Col("Active", schema.TypeBool()).NotNull().DefaultFn(func(d chuck.Dialect) string {
			if d.Engine() == chuck.Postgres {
				return "TRUE"
			}
			return "1"
		}),
	).
	WithTimestamps().
	WithSoftDelete().
	WithVersion().
	Indexes(
		schema.Index("idx_chuck_schema_test_name", "Name"),
	)

// schemaDriftTest creates a table from the declared schema, then validates it
// using ValidateSchema to verify column names, count, nullability, and indexes match.
func schemaDriftTest(t *testing.T, db *sql.DB, d chuck.Dialect) {
	t.Helper()
	ctx := context.Background()

	tableName := testTable.TableNameFor(d)

	// Clean up from any previous run
	_, _ = db.ExecContext(ctx, d.DropTableIfExists(tableName))

	// Create from declared schema
	for _, stmt := range testTable.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "create table: %s", stmt)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, d.DropTableIfExists(tableName))
	}()

	t.Run("ValidateSchema", func(t *testing.T) {
		for _, e := range schema.ValidateSchema(ctx, db, d, testTable) {
			t.Errorf("schema validation error: %s", e.Error())
		}
	})
}

func TestSchemaDriftSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	schemaDriftTest(t, db, chuck.SQLiteDialect{})
}

func TestSchemaDriftPostgres(t *testing.T) {
	dsn := os.Getenv("CHUCK_POSTGRES_URL")
	if dsn == "" {
		t.Skip("CHUCK_POSTGRES_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	schemaDriftTest(t, db, d)
}

func TestSchemaDriftMSSQL(t *testing.T) {
	dsn := os.Getenv("CHUCK_MSSQL_URL")
	if dsn == "" {
		t.Skip("CHUCK_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	schemaDriftTest(t, db, d)
}

// TestDropInboundForeignKeysMSSQL covers the destructive-bootstrap path for
// owned schemas on MSSQL (issue #77). Inline FKs declared in chuck/schema
// emit auto-generated constraint names, so callers cannot drop them by name
// without first discovering them at runtime. This test:
//
//  1. creates a child→parent table pair with an inline FK declared via the
//     schema DSL, mirroring what owned-schema bootstrap produces;
//  2. confirms a naive DropOrder-only teardown fails because the inbound FK
//     still pins the parent table (the regression #77 documents);
//  3. uses InboundForeignKeys + DropInboundForeignKeys to detach the FK,
//     deriving the owned set entirely from the declared *TableDef inputs;
//  4. verifies DropOrder then succeeds without referencing handwritten
//     constraint names.
func TestDropInboundForeignKeysMSSQL(t *testing.T) {
	dsn := os.Getenv("CHUCK_MSSQL_URL")
	if dsn == "" {
		t.Skip("CHUCK_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	parent := schema.NewTable("chuck_fk77_parent").Columns(
		schema.AutoIncrCol("ID"),
		schema.Col("Name", schema.TypeVarchar(50)).NotNull(),
	)
	child := schema.NewTable("chuck_fk77_child").Columns(
		schema.AutoIncrCol("ID"),
		schema.Col("ParentID", schema.TypeInt()).NotNull().
			References("chuck_fk77_parent", "ID"),
	)
	tables := []*schema.TableDef{parent, child}

	// Best-effort cleanup from any prior failed run before we begin.
	if fks, ferr := schema.InboundForeignKeys(ctx, db, d, tables...); ferr == nil {
		for _, fk := range fks {
			_, _ = db.ExecContext(ctx, schema.DropForeignKeySQL(d, fk))
		}
	}
	if dropOrder, oerr := schema.DropOrder(tables...); oerr == nil {
		for _, td := range dropOrder {
			_, _ = db.ExecContext(ctx, td.DropSQL(d))
		}
	}

	createOrder, err := schema.CreationOrder(tables...)
	require.NoError(t, err)
	for _, td := range createOrder {
		for _, stmt := range td.CreateIfNotExistsSQL(d) {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, "create table: %s", stmt)
		}
	}

	// Guarantee cleanup even if an assertion fails mid-test.
	defer func() {
		if fks, ferr := schema.InboundForeignKeys(ctx, db, d, tables...); ferr == nil {
			for _, fk := range fks {
				_, _ = db.ExecContext(ctx, schema.DropForeignKeySQL(d, fk))
			}
		}
		if dropOrder, oerr := schema.DropOrder(tables...); oerr == nil {
			for _, td := range dropOrder {
				_, _ = db.ExecContext(ctx, td.DropSQL(d))
			}
		}
	}()

	t.Run("inbound FK is discoverable from declared set", func(t *testing.T) {
		fks, err := schema.InboundForeignKeys(ctx, db, d, tables...)
		require.NoError(t, err)
		require.Len(t, fks, 1,
			"exactly one inbound FK should be reported for child→parent declarations")
		fk := fks[0]
		assert.Equal(t, "chuck_fk77_child", fk.ParentTable,
			"parent side of the FK is the child table that owns the column")
		assert.Equal(t, "chuck_fk77_parent", fk.ReferencedTable,
			"referenced side is the parent table the FK points at")
		assert.NotEmpty(t, fk.Name, "MSSQL must surface a concrete (auto-generated) constraint name")
	})

	t.Run("DropOrder alone fails while FK is in place", func(t *testing.T) {
		dropOrder, err := schema.DropOrder(tables...)
		require.NoError(t, err)
		// Drop the child first; that succeeds and removes the FK with it.
		// The parent drop must then succeed too — so to prove DropOrder is
		// insufficient we instead try to drop the parent directly while the
		// child still references it. That is the failure mode #77 documents.
		_, err = db.ExecContext(ctx, dropOrder[len(dropOrder)-1].DropSQL(d))
		require.Error(t, err,
			"DROP TABLE parent must fail while an inbound FK from the child still pins it")
	})

	t.Run("DropInboundForeignKeys detaches FK then DropOrder succeeds", func(t *testing.T) {
		dropped, err := schema.DropInboundForeignKeys(ctx, db, d, tables...)
		require.NoError(t, err)
		require.Len(t, dropped, 1, "expected the single declared FK to be dropped")

		// Second call must be a no-op now that the constraint is gone.
		remaining, err := schema.InboundForeignKeys(ctx, db, d, tables...)
		require.NoError(t, err)
		assert.Empty(t, remaining, "InboundForeignKeys must report no work once FKs are detached")

		dropOrder, err := schema.DropOrder(tables...)
		require.NoError(t, err)
		for _, td := range dropOrder {
			_, err := db.ExecContext(ctx, td.DropSQL(d))
			require.NoError(t, err, "DROP TABLE must succeed after FK teardown: %s", td.Name)
		}
	})
}
