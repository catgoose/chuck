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

// TestViewLifecycleSQLite exercises the ViewDef create / create-or-replace /
// drop lifecycle end-to-end against in-memory SQLite. Running this against a
// real engine catches surface-level breakage (escaping, missing whitespace,
// dialect-emitted statements that the engine refuses) that pure string tests
// would miss. SQLite is used because it needs no env-gated DSN.
func TestViewLifecycleSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	tbl := schema.NewTable("view_lifecycle_tasks").Columns(
		schema.AutoIncrCol("ID"),
		schema.Col("Title", schema.TypeVarchar(255)).NotNull(),
	)
	for _, stmt := range tbl.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "create base table: %s", stmt)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, d.DropTableIfExists(tbl.TableNameFor(d)))
	}()

	view := schema.NewView("v_view_lifecycle_active",
		`SELECT "id", "title" FROM "view_lifecycle_tasks"`)

	// Plain create succeeds against an empty schema.
	_, err = db.ExecContext(ctx, view.CreateSQL(d))
	require.NoError(t, err, "create view: %s", view.CreateSQL(d))

	// CreateOrReplaceSQL on SQLite emits DROP-then-CREATE; both must execute
	// cleanly so a re-bootstrap can refresh the view body.
	for _, stmt := range view.CreateOrReplaceSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "create-or-replace view: %s", stmt)
	}

	// Drop succeeds once, and runs cleanly a second time thanks to IF EXISTS.
	for i := range 2 {
		_, err := db.ExecContext(ctx, view.DropSQL(d))
		require.NoError(t, err, "drop view iteration %d: %s", i, view.DropSQL(d))
	}
}

// TestProcedureLifecycleMSSQL exercises the ProcedureDef create-or-alter /
// drop lifecycle end-to-end against a live MSSQL instance. Procedure
// ownership is MSSQL-only in this release, so SQLite/Postgres cannot cover
// the apply path. The definition is parameterized on purpose: it proves the
// pre-`AS` parameter slot survives the render path, in addition to the basic
// CREATE OR ALTER / DROP probe idempotency contract.
func TestProcedureLifecycleMSSQL(t *testing.T) {
	dsn := os.Getenv("CHUCK_MSSQL_URL")
	if dsn == "" {
		t.Skip("CHUCK_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	proc := schema.NewProcedure("usp_chuck_proc_lifecycle",
		"@Probe INT = 1 AS BEGIN SET NOCOUNT ON; SELECT @Probe AS Probe; END")

	// Best-effort cleanup from any prior failed run.
	if stmt, derr := proc.DropSQL(d); derr == nil {
		_, _ = db.ExecContext(ctx, stmt)
	}

	defer func() {
		if stmt, derr := proc.DropSQL(d); derr == nil {
			_, _ = db.ExecContext(ctx, stmt)
		}
	}()

	// CreateOrAlterSQL must succeed on a fresh schema and again on the
	// already-created proc; that is the contract MSSQL CREATE OR ALTER buys
	// us versus a plain CREATE PROCEDURE.
	for i := range 2 {
		stmt, err := proc.CreateOrAlterSQL(d)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err, "create-or-alter procedure iteration %d: %s", i, stmt)
	}

	// Existence probe drop is idempotent — second run must be a clean no-op.
	for i := range 2 {
		stmt, err := proc.DropSQL(d)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err, "drop procedure iteration %d: %s", i, stmt)
	}
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

// TestViewValidateApply_SQLite exercises the validate/apply lane against
// in-memory SQLite end-to-end: apply a declared view, validate it matches,
// drift the live body out-of-band, validate the drift is detected, then
// re-apply and validate it is gone. Uses SQLite because the lane works on
// all dialects but SQLite needs no env DSN.
func TestViewValidateApply_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	_, err = db.ExecContext(ctx, `CREATE TABLE vva_tasks (id INTEGER PRIMARY KEY, done INTEGER)`)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS vva_tasks`) }()

	v := schema.NewView("v_vva_open", "SELECT id FROM vva_tasks WHERE done = 0")
	defer func() { _, _ = db.ExecContext(ctx, v.DropSQL(d)) }()

	require.NoError(t, schema.ApplyView(ctx, db, d, v))
	require.NoError(t, schema.ValidateView(ctx, db, d, v),
		"ValidateView must report a clean match immediately after ApplyView")

	// Drift the live view out-of-band.
	_, err = db.ExecContext(ctx, `DROP VIEW v_vva_open`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE VIEW v_vva_open AS SELECT id FROM vva_tasks WHERE done = 1`)
	require.NoError(t, err)

	err = schema.ValidateView(ctx, db, d, v)
	require.Error(t, err, "out-of-band body change must be detected")
	assert.ErrorIs(t, err, schema.ErrViewBodyDrift)

	// ApplyView should restore the declared body.
	require.NoError(t, schema.ApplyView(ctx, db, d, v))
	require.NoError(t, schema.ValidateView(ctx, db, d, v),
		"validate must pass again after re-apply")
}

// TestProcedureValidateApply_MSSQL exercises the validate/apply lane against
// a live MSSQL instance. Procedure ownership is MSSQL-only, so SQLite /
// Postgres cannot stand in for this test. Gated by CHUCK_MSSQL_URL.
func TestProcedureValidateApply_MSSQL(t *testing.T) {
	dsn := os.Getenv("CHUCK_MSSQL_URL")
	if dsn == "" {
		t.Skip("CHUCK_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	proc := schema.NewProcedure("usp_chuck_vva_proc",
		"@Probe INT = 1 AS BEGIN SET NOCOUNT ON; SELECT @Probe AS Probe; END")

	// Best-effort cleanup from any prior failed run.
	if stmt, derr := proc.DropSQL(d); derr == nil {
		_, _ = db.ExecContext(ctx, stmt)
	}
	defer func() {
		if stmt, derr := proc.DropSQL(d); derr == nil {
			_, _ = db.ExecContext(ctx, stmt)
		}
	}()

	// Missing → ValidateProcedure reports drift wrapping ErrProcedureMissing.
	err = schema.ValidateProcedure(ctx, db, d, proc)
	require.Error(t, err)
	assert.ErrorIs(t, err, schema.ErrProcedureMissing)

	// Apply then validate matches.
	require.NoError(t, schema.ApplyProcedure(ctx, db, d, proc))
	require.NoError(t, schema.ValidateProcedure(ctx, db, d, proc),
		"validate must pass immediately after apply")

	// Drift definition out-of-band via a different definition body.
	drifted := schema.NewProcedure("usp_chuck_vva_proc",
		"@Probe INT = 2 AS BEGIN SET NOCOUNT ON; SELECT @Probe + 100 AS Probe; END")
	stmt, err := drifted.CreateOrAlterSQL(d)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, stmt)
	require.NoError(t, err)

	err = schema.ValidateProcedure(ctx, db, d, proc)
	require.Error(t, err, "out-of-band definition change must be detected")
	assert.ErrorIs(t, err, schema.ErrProcedureDefinitionDrift)

	// Re-apply the original and validate clean.
	require.NoError(t, schema.ApplyProcedure(ctx, db, d, proc))
	require.NoError(t, schema.ValidateProcedure(ctx, db, d, proc))
}
