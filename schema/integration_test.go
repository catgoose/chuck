package schema_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

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

// TestValidateView_Postgres_FailsLoud asserts the Postgres ValidateView
// contract: existence is confirmed, but body comparison is NOT silently
// green-lit. After apply, validate must return a *ViewDriftError whose entry
// has BodyComparisonSkipped=true and unwraps to
// ErrViewBodyComparisonUnsupported. Gated by CHUCK_POSTGRES_URL.
func TestValidateView_Postgres_FailsLoud(t *testing.T) {
	dsn := os.Getenv("CHUCK_POSTGRES_URL")
	if dsn == "" {
		t.Skip("CHUCK_POSTGRES_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS vva_pg_tasks (id SERIAL PRIMARY KEY, done BOOLEAN)`)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS vva_pg_tasks`) }()

	v := schema.NewView("v_vva_pg_open", "SELECT id FROM vva_pg_tasks WHERE done = FALSE")
	defer func() { _, _ = db.ExecContext(ctx, v.DropSQL(d)) }()

	// Missing → ValidateView reports drift wrapping ErrViewMissing.
	err = schema.ValidateView(ctx, db, d, v)
	require.Error(t, err)
	assert.ErrorIs(t, err, schema.ErrViewMissing,
		"missing view on Postgres must still fail loud with ErrViewMissing")

	require.NoError(t, schema.ApplyView(ctx, db, d, v))

	// Exists but body comparison is unavailable on Postgres → must fail loud
	// with ErrViewBodyComparisonUnsupported instead of returning nil.
	err = schema.ValidateView(ctx, db, d, v)
	require.Error(t, err, "Postgres ValidateView must NOT silently green-light when body compare is unavailable")
	assert.ErrorIs(t, err, schema.ErrViewBodyComparisonUnsupported)

	var drift *schema.ViewDriftError
	require.ErrorAs(t, err, &drift)
	require.Len(t, drift.Drifts, 1)
	assert.True(t, drift.Drifts[0].BodyComparisonSkipped,
		"Postgres drift entry must set BodyComparisonSkipped so callers can see why compare was skipped")
	assert.False(t, drift.Drifts[0].Missing)
	assert.False(t, drift.Drifts[0].BodyMismatch)
	assert.NotEmpty(t, drift.Drifts[0].Reason)
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

// TestViewValidateApplyWithOptions_SQLite_OwnershipNotice asserts the
// apply-owned ownership-notice contract on SQLite, which stores view text
// verbatim in sqlite_master.sql and therefore lets us observe the comment
// landed in the live object.
//
// Contract covered:
//
//   - apply-with-options injects the notice and the doc preamble into live
//     SQL in notice-then-preamble order
//   - validate-with-same-options strips the configured prefix and reports clean
//   - bare ValidateView (no opts) still validates because leading
//     comment-only front matter is ignored
//   - bare ApplyView strips the markers; validate-with-options against the
//     bare live body is clean (apply-owned tolerance — markers are not
//     required to exist live)
//   - live body with a different leading comment still validates cleanly
//     because comment-only front matter is ignored
func TestViewValidateApplyWithOptions_SQLite_OwnershipNotice(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	_, err = db.ExecContext(ctx, `CREATE TABLE vva_on_tasks (id INTEGER PRIMARY KEY, done INTEGER)`)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS vva_on_tasks`) }()

	v := schema.NewView("v_vva_on_open", "SELECT id FROM vva_on_tasks WHERE done = 0")
	defer func() { _, _ = db.ExecContext(ctx, v.DropSQL(d)) }()

	opts := schema.CodeObjectOptions{
		OwnershipNotice: schema.DefaultOwnershipNotice,
		DocPreamble:     "v_vva_on_open: open tasks fan-out for handler X",
	}

	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, opts, v))
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, opts, v),
		"apply-with-options must pair cleanly with validate-with-same-options")

	// Live body in sqlite_master must contain both the rendered doc preamble
	// and the ownership notice so DB-side readers see chuck-owned marker and
	// the caller commentary.
	var liveSQL string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='view' AND name=?`, "v_vva_on_open").
		Scan(&liveSQL))
	assert.Contains(t, liveSQL, "Owned by https://github.com/catgoose/chuck")
	assert.Contains(t, liveSQL, "may fail validation or be overwritten")
	assert.Contains(t, liveSQL, "open tasks fan-out for handler X")
	// Render order: ownership notice appears before the doc preamble.
	docIdx := strings.Index(liveSQL, "open tasks fan-out")
	notIdx := strings.Index(liveSQL, "Owned by https://github.com/catgoose/chuck")
	require.True(t, docIdx >= 0 && notIdx >= 0)
	assert.Less(t, notIdx, docIdx, "OwnershipNotice must render before DocPreamble")

	// Bare ValidateView (no opts) also passes: leading comment-only front
	// matter is ignored during body comparison.
	require.NoError(t, schema.ValidateView(ctx, db, d, v))

	// Bare ApplyView strips the markers; validate-with-options against the
	// bare live body must be clean — apply-owned tolerance lets validate
	// pass options without forcing markers to exist live.
	require.NoError(t, schema.ApplyView(ctx, db, d, v))
	require.NoError(t, schema.ValidateView(ctx, db, d, v),
		"bare apply + bare validate must still be coherent")
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, opts, v),
		"apply-owned: validate-with-options must tolerate live body without markers")

	// Different leading comment still validates: comment-only front matter
	// does not participate in body drift.
	_, err = db.ExecContext(ctx,
		`DROP VIEW IF EXISTS v_vva_on_open; `+
			`CREATE VIEW v_vva_on_open AS /* unexpected stale notice */ SELECT id FROM vva_on_tasks WHERE done = 0`)
	require.NoError(t, err)
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, opts, v),
		"validate-with-options must ignore comment-only front matter drift")
}

// TestViewValidateApplyWithOptions_SQLite_DocAnnotation asserts the per-object
// doc annotation render contract on SQLite.
//
// Contract covered:
//
//   - apply + validate with the same declared annotation is coherent
//   - the rendered annotation lands in sqlite_master.sql last among the
//     leading comments, after the caller-level OwnershipNotice and the
//     caller-level DocPreamble, closest to the SELECT body
//   - changing the declared annotation in code does not produce validation
//     drift because comment-only front matter is ignored
//   - the same ignore-comments rule holds even without any caller-level
//     decoration
func TestViewValidateApplyWithOptions_SQLite_DocAnnotation(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	_, err = db.ExecContext(ctx, `CREATE TABLE vva_da_tasks (id INTEGER PRIMARY KEY, done INTEGER)`)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS vva_da_tasks`) }()

	v := schema.NewView("v_vva_da_open", "SELECT id FROM vva_da_tasks WHERE done = 0").
		WithDocAnnotation("v_vva_da_open v1: returns currently-open task ids")
	defer func() { _, _ = db.ExecContext(ctx, v.DropSQL(d)) }()

	opts := schema.CodeObjectOptions{
		OwnershipNotice: schema.DefaultOwnershipNotice,
		DocPreamble:     "vva_da: declaration-owned annotation contract probe",
	}

	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, opts, v))
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, opts, v),
		"apply + validate with same opts and same annotation must be coherent")

	var liveSQL string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='view' AND name=?`, "v_vva_da_open").
		Scan(&liveSQL))

	docIdx := strings.Index(liveSQL, "declaration-owned annotation contract probe")
	annIdx := strings.Index(liveSQL, "v_vva_da_open v1: returns currently-open task ids")
	notIdx := strings.Index(liveSQL, "Owned by https://github.com/catgoose/chuck")
	require.True(t, docIdx >= 0 && annIdx >= 0 && notIdx >= 0,
		"all three comment segments must be present in the live SQL")
	assert.Less(t, notIdx, docIdx, "OwnershipNotice must render before DocPreamble")
	assert.Less(t, docIdx, annIdx, "DocPreamble must render before doc annotation")

	// Change annotation text in code. Validation still passes because
	// leading comments are documentation, not executable view semantics.
	vDrifted := schema.NewView("v_vva_da_open", "SELECT id FROM vva_da_tasks WHERE done = 0").
		WithDocAnnotation("v_vva_da_open v2: SEMANTIC CHANGE — please review caller")
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, opts, vDrifted),
		"declared annotation change must not surface as body drift")

	// Same for annotation-only rendering with no caller-level options.
	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, schema.CodeObjectOptions{}, v))
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, schema.CodeObjectOptions{}, vDrifted),
		"annotation-only declaration must not drive drift on its own")

	// Same declaration as live still validates clean under annotation-only.
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, schema.CodeObjectOptions{}, v))
}

// TestViewRenameRollout_SQLite_WithReplaces asserts the rename/tombstone
// contract end-to-end on SQLite, which stores view definitions verbatim and
// lets us observe both apply-side drops and validate-side stale-replacement
// drift directly via sqlite_master.
//
// Contract covered:
//
//   - apply drops each WithReplaces name before creating the current view
//   - validate is clean after apply when no prior name remains live
//   - re-creating a prior name out of band surfaces as stale-replacement
//     drift that unwraps to ErrViewReplacementStillExists
//   - batch apply / validate dedupe duplicate replacement names across defs
func TestViewRenameRollout_SQLite_WithReplaces(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	_, err = db.ExecContext(ctx, `CREATE TABLE rn_tasks (id INTEGER PRIMARY KEY, done INTEGER)`)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS rn_tasks`) }()

	// Seed the prior name as if a previous rollout had created it.
	_, err = db.ExecContext(ctx,
		`CREATE VIEW v_rn_open_v1 AS SELECT id FROM rn_tasks WHERE done = 0`)
	require.NoError(t, err)

	v := schema.NewView("v_rn_open", "SELECT id FROM rn_tasks WHERE done = 0").
		WithReplaces(chuck.ObjectName{Name: "v_rn_open_v1"})
	defer func() { _, _ = db.ExecContext(ctx, v.DropSQL(d)) }()

	require.NoError(t, schema.ApplyView(ctx, db, d, v))

	var oldCount int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name=?`,
		"v_rn_open_v1").Scan(&oldCount))
	assert.Equal(t, 0, oldCount, "prior name must be dropped after rollout apply")

	require.NoError(t, schema.ValidateView(ctx, db, d, v),
		"validate must be clean once the prior name is retired")

	// Simulate an out-of-band resurrection of the prior name (e.g. a
	// rollback patch or hand-edit). Validate must now flag stale
	// replacement and unwrap to ErrViewReplacementStillExists.
	_, err = db.ExecContext(ctx,
		`CREATE VIEW v_rn_open_v1 AS SELECT id FROM rn_tasks WHERE done = 0`)
	require.NoError(t, err)

	err = schema.ValidateView(ctx, db, d, v)
	require.Error(t, err)
	assert.ErrorIs(t, err, schema.ErrViewReplacementStillExists)

	// Batch dedup: two defs naming the same prior name must report it once.
	a := schema.NewView("v_rn_a", "SELECT id FROM rn_tasks").
		WithReplaces(chuck.ObjectName{Name: "v_rn_open_v1"})
	b := schema.NewView("v_rn_b", "SELECT id FROM rn_tasks").
		WithReplaces(chuck.ObjectName{Name: "v_rn_open_v1"})
	defer func() {
		_, _ = db.ExecContext(ctx, a.DropSQL(d))
		_, _ = db.ExecContext(ctx, b.DropSQL(d))
	}()
	require.NoError(t, schema.ApplyViews(ctx, db, d, a, b))

	// Re-seed the prior name to test validate dedup specifically.
	_, err = db.ExecContext(ctx,
		`CREATE VIEW v_rn_open_v1 AS SELECT id FROM rn_tasks WHERE done = 0`)
	require.NoError(t, err)

	err = schema.ValidateViews(ctx, db, d, a, b)
	require.Error(t, err)
	var drift *schema.ViewDriftError
	require.ErrorAs(t, err, &drift)
	// One drift entry for v_rn_open_v1, deduped across the two defs.
	staleCount := 0
	for _, dr := range drift.Drifts {
		if dr.ReplacementStale && dr.Object.Name == "v_rn_open_v1" {
			staleCount++
		}
	}
	assert.Equal(t, 1, staleCount,
		"v_rn_open_v1 must be deduped to one drift entry across the batch")
}

// TestProcedureValidateApplyWithOptions_MSSQL gates on a live MSSQL instance
// because MSSQL is the only engine with first-class procedure ownership in
// this release. Proves apply-with-options + validate-with-same-options is
// coherent and that the rendered comment lands in sys.sql_modules.definition.
func TestProcedureValidateApplyWithOptions_MSSQL(t *testing.T) {
	dsn := os.Getenv("CHUCK_MSSQL_URL")
	if dsn == "" {
		t.Skip("CHUCK_MSSQL_URL not set")
	}

	ctx := context.Background()
	db, d, err := chuck.OpenURL(ctx, dsn)
	require.NoError(t, err)
	defer db.Close()

	proc := schema.NewProcedure("usp_chuck_vva_proc_notice",
		"@Probe INT = 1 AS BEGIN SET NOCOUNT ON; SELECT @Probe AS Probe; END")

	if stmt, derr := proc.DropSQL(d); derr == nil {
		_, _ = db.ExecContext(ctx, stmt)
	}
	defer func() {
		if stmt, derr := proc.DropSQL(d); derr == nil {
			_, _ = db.ExecContext(ctx, stmt)
		}
	}()

	opts := schema.CodeObjectOptions{
		OwnershipNotice: schema.DefaultOwnershipNotice,
		DocPreamble:     "usp_chuck_vva_proc_notice: probe procedure for VVA notice contract",
	}

	require.NoError(t, schema.ApplyProcedureWithOptions(ctx, db, d, opts, proc))
	require.NoError(t, schema.ValidateProcedureWithOptions(ctx, db, d, opts, proc),
		"apply-with-options must pair cleanly with validate-with-same-options")

	// Confirm the live definition carries both markers so operators reading
	// sys.sql_modules see the doc preamble and the chuck-owned marker.
	live, exists, err := schema.LiveProcedureDefinition(ctx, db, d, proc)
	require.NoError(t, err)
	require.True(t, exists)
	require.Contains(t, live, "Owned by https://github.com/catgoose/chuck")
	require.Contains(t, live, "may fail validation or be overwritten")
	require.Contains(t, live, "probe procedure for VVA notice contract")

	// Bare ValidateProcedure (no opts) also passes: leading comments do not
	// participate in executable-definition drift.
	require.NoError(t, schema.ValidateProcedure(ctx, db, d, proc))

	// Bare ApplyProcedure strips the markers; validate-with-options against
	// the bare live definition must be clean — apply-owned tolerance.
	require.NoError(t, schema.ApplyProcedure(ctx, db, d, proc))
	require.NoError(t, schema.ValidateProcedure(ctx, db, d, proc))
	require.NoError(t, schema.ValidateProcedureWithOptions(ctx, db, d, opts, proc),
		"apply-owned: validate-with-options must tolerate live definition without markers")
}

// integrationFakeClock is the integration-test version of metadata_test.go's
// fakeClock. metadata_test.go lives in the schema package (white-box) and its
// type is not exported, so this file (schema_test, black-box) keeps its own
// minimal clock for end-to-end metadata assertions.
type integrationFakeClock struct {
	cur time.Time
}

func (c *integrationFakeClock) Now() time.Time { return c.cur }

func (c *integrationFakeClock) advance(by time.Duration) { c.cur = c.cur.Add(by) }

// TestViewApplyWithOptions_SQLite_MetadataLedger asserts the opt-in snapshot
// metadata contract end-to-end on SQLite: ApplyViewWithOptions writes a row
// when opts.Metadata is set, snapshot timestamps advance correctly across
// repeat applies, and validate-with-options is unaffected (snapshot is
// apply-side only in this first pass).
//
// Contract covered:
//
//   - bare apply (no Metadata) does not touch the ledger
//   - apply-with-Metadata creates the row on first apply; first_applied =
//     last_applied = last_changed
//   - re-applying the same body advances last_applied only; last_changed and
//     first_applied stay frozen
//   - applying a different body advances last_applied AND last_changed;
//     first_applied stays frozen; definition_hash is replaced
//   - chuck_database_metadata.first_applied stays frozen across repeat
//     applies for the same owner
//   - Validate*WithOptions remains clean regardless of ledger state — the
//     ledger is independent of drift comparison
func TestViewApplyWithOptions_SQLite_MetadataLedger(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	_, err = db.ExecContext(ctx, `CREATE TABLE vva_md_tasks (id INTEGER PRIMARY KEY, done INTEGER)`)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS vva_md_tasks`) }()

	clock := &integrationFakeClock{cur: time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)}
	cfg := schema.MetadataConfig{
		Owner:       "bootstrap",
		SourceRepo:  "https://github.com/catgoose/chuck",
		SourceRev:   "rev-1",
		ToolVersion: "v0.0.1-test",
		Now:         clock.Now,
	}
	require.NoError(t, schema.EnsureMetadataTables(ctx, db, d, cfg))

	v := schema.NewView("v_vva_md_open", "SELECT id FROM vva_md_tasks WHERE done = 0")
	defer func() { _, _ = db.ExecContext(ctx, v.DropSQL(d)) }()

	// Bare apply (no metadata) must leave the ledger empty.
	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, schema.CodeObjectOptions{}, v))
	assert.Equal(t, 0, countObjectMetadataRows(t, ctx, db, "bootstrap", "v_vva_md_open"),
		"bare apply must not write to the ledger")

	// First metadata-aware apply: row created at t0.
	t0 := clock.cur
	opts := schema.CodeObjectOptions{Metadata: &cfg}
	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, opts, v))
	row := readObjectMetadataRow(t, ctx, db, "bootstrap", "v_vva_md_open")
	assert.True(t, row.firstApplied.Equal(t0))
	assert.True(t, row.lastApplied.Equal(t0))
	assert.True(t, row.lastChanged.Equal(t0))
	firstHash := row.definitionHash
	require.NotEmpty(t, firstHash, "definition_hash must be written")

	// Second apply, same body: last_applied advances, last_changed unchanged.
	clock.advance(time.Hour)
	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, opts, v))
	row = readObjectMetadataRow(t, ctx, db, "bootstrap", "v_vva_md_open")
	assert.True(t, row.firstApplied.Equal(t0), "first_applied must stay frozen on same-hash reapply")
	assert.True(t, row.lastApplied.Equal(t0.Add(time.Hour)))
	assert.True(t, row.lastChanged.Equal(t0), "last_changed must not move when hash matches")
	assert.Equal(t, firstHash, row.definitionHash)

	// Third apply, executable text changed: last_applied AND last_changed
	// advance, first_applied still frozen, hash replaced.
	clock.advance(time.Hour)
	v2 := schema.NewView("v_vva_md_open", "SELECT id FROM vva_md_tasks WHERE done = 1")
	defer func() { _, _ = db.ExecContext(ctx, v2.DropSQL(d)) }()
	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, opts, v2))
	row = readObjectMetadataRow(t, ctx, db, "bootstrap", "v_vva_md_open")
	assert.True(t, row.firstApplied.Equal(t0), "first_applied stays frozen on hash change")
	assert.True(t, row.lastApplied.Equal(t0.Add(2*time.Hour)))
	assert.True(t, row.lastChanged.Equal(t0.Add(2*time.Hour)), "last_changed must move on hash change")
	assert.NotEqual(t, firstHash, row.definitionHash, "definition_hash must update on executable text change")

	// Comment-only change (different DocPreamble) must not move last_changed.
	clock.advance(time.Hour)
	optsWithDoc := schema.CodeObjectOptions{
		DocPreamble: "different preamble text",
		Metadata:    &cfg,
	}
	require.NoError(t, schema.ApplyViewWithOptions(ctx, db, d, optsWithDoc, v2))
	row = readObjectMetadataRow(t, ctx, db, "bootstrap", "v_vva_md_open")
	assert.True(t, row.lastApplied.Equal(t0.Add(3*time.Hour)))
	assert.True(t, row.lastChanged.Equal(t0.Add(2*time.Hour)),
		"comment-only change must not advance last_changed because hash ignores leading comments")

	// Per-owner database row tracks first/last apply.
	dbFirst, dbLast := readDatabaseMetadataRow(t, ctx, db, "bootstrap")
	assert.True(t, dbFirst.Equal(t0))
	assert.True(t, dbLast.Equal(t0.Add(3*time.Hour)))

	// Validate path must remain agnostic of the ledger.
	require.NoError(t, schema.ValidateViewWithOptions(ctx, db, d, optsWithDoc, v2),
		"validate must remain clean regardless of ledger state")
}

func countObjectMetadataRows(t *testing.T, ctx context.Context, db *sql.DB, owner, name string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM chuck_object_metadata WHERE owner = ? AND object_name = ?`,
		owner, name).Scan(&n))
	return n
}

type liveObjectMetadataRow struct {
	firstApplied   time.Time
	lastApplied    time.Time
	lastChanged    time.Time
	definitionHash string
}

func readObjectMetadataRow(t *testing.T, ctx context.Context, db *sql.DB, owner, name string) liveObjectMetadataRow {
	t.Helper()
	var row liveObjectMetadataRow
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT first_applied_at_utc, last_applied_at_utc, last_changed_at_utc, definition_hash
		   FROM chuck_object_metadata
		  WHERE owner = ? AND object_name = ?`,
		owner, name).Scan(&row.firstApplied, &row.lastApplied, &row.lastChanged, &row.definitionHash))
	return row
}

func readDatabaseMetadataRow(t *testing.T, ctx context.Context, db *sql.DB, owner string) (firstApplied, lastApplied time.Time) {
	t.Helper()
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT first_applied_at_utc, last_applied_at_utc
		   FROM chuck_database_metadata
		  WHERE owner = ?`, owner).Scan(&firstApplied, &lastApplied))
	return firstApplied, lastApplied
}
