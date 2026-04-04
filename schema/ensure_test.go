package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/sqlite"
)

func TestEnsureModeString(t *testing.T) {
	assert.Equal(t, "strict", ModeStrict.String())
	assert.Equal(t, "dev", ModeDev.String())
	assert.Equal(t, "dryrun", ModeDryRun.String())
	assert.Equal(t, "Mode(99)", Mode(99).String())
}

func TestEnsureStrictValidSchema(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	result, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeStrict))
	require.NoError(t, err)
	assert.Empty(t, result.Diffs)
	assert.Empty(t, result.TablesCreated)
	assert.Empty(t, result.TablesSeeded)
}

func TestEnsureStrictWithDrift(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	// Create table with 2 columns
	actual := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		)
	for _, stmt := range actual.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Declare table with 3 columns (drift)
	declared := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull(),
		)

	result, err := Ensure(ctx, db, d, []*TableDef{declared}, WithMode(ModeStrict))
	require.Error(t, err)

	var ensureErr *EnsureError
	require.ErrorAs(t, err, &ensureErr)
	require.Len(t, ensureErr.Diffs, 1)
	assert.True(t, ensureErr.Diffs[0].HasDrift)
	assert.Contains(t, err.Error(), "schema drift detected")

	// Result should also contain the diffs
	require.Len(t, result.Diffs, 1)
}

func TestEnsureStrictMissingTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Missing").
		Columns(AutoIncrCol("ID"))

	result, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeStrict))
	require.Error(t, err)

	var ensureErr *EnsureError
	require.ErrorAs(t, err, &ensureErr)
	require.Len(t, ensureErr.Diffs, 1)
	assert.True(t, ensureErr.Diffs[0].TableMissing)
	assert.NotNil(t, result)
}

func TestEnsureDevMissingTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		).
		WithSeedRows(
			SeedRow{"Name": "'default'"},
		)

	result, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeDev))
	require.NoError(t, err)
	assert.Contains(t, result.TablesCreated, "Items")
	assert.Contains(t, result.TablesSeeded, "Items")

	// Verify the table was actually created
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM Items").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // seed row
}

func TestEnsureDevExistingTableWithDrift(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	// Create table with 2 columns
	actual := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		)
	for _, stmt := range actual.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Declare table with 3 columns (drift)
	declared := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull(),
		)

	_, err := Ensure(ctx, db, d, []*TableDef{declared}, WithMode(ModeDev))
	require.Error(t, err)

	var ensureErr *EnsureError
	require.ErrorAs(t, err, &ensureErr)
	assert.Len(t, ensureErr.Diffs, 1)
}

func TestEnsureDevValidExistingTable(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	result, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeDev))
	require.NoError(t, err)
	assert.Empty(t, result.TablesCreated)
	assert.Empty(t, result.TablesSeeded)
	assert.Empty(t, result.Diffs)
}

func TestEnsureDevFKOrder(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	users := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		)

	tasks := NewTable("Tasks").
		Columns(
			AutoIncrCol("ID"),
			Col("Title", TypeString(255)).NotNull(),
			Col("UserID", TypeInt()).References("Users", "ID"),
		)

	// Pass tasks first — Ensure should still create users before tasks
	result, err := Ensure(ctx, db, d, []*TableDef{tasks, users}, WithMode(ModeDev))
	require.NoError(t, err)
	require.Len(t, result.TablesCreated, 2)
	// Users should be created before Tasks
	assert.Equal(t, "Users", result.TablesCreated[0])
	assert.Equal(t, "Tasks", result.TablesCreated[1])
}

func TestEnsureDryRun(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	existing := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		)
	for _, stmt := range existing.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Declare Items with extra column + a missing table
	declared := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)),
		)
	missing := NewTable("Other").
		Columns(AutoIncrCol("ID"))

	result, err := Ensure(ctx, db, d, []*TableDef{declared, missing}, WithMode(ModeDryRun))
	require.NoError(t, err) // DryRun never errors

	// Should have diffs for both tables
	require.Len(t, result.Diffs, 2)
	assert.True(t, result.Diffs[0].HasDrift) // Items has drift
	assert.True(t, result.Diffs[1].HasDrift) // Other is missing
	assert.True(t, result.Diffs[1].TableMissing)

	// Verify nothing was created
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Other'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestEnsureWithDiffOutput(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Missing").
		Columns(AutoIncrCol("ID"))

	var buf bytes.Buffer
	_, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeStrict), WithDiffOutput(&buf))
	require.Error(t, err)

	// Verify JSON was written
	assert.Greater(t, buf.Len(), 0)
	var diffs []*SchemaDiff
	err = json.Unmarshal(buf.Bytes(), &diffs)
	require.NoError(t, err)
	require.Len(t, diffs, 1)
	assert.True(t, diffs[0].TableMissing)
}

func TestEnsureWithDiffFile(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Missing").
		Columns(AutoIncrCol("ID"))

	path := filepath.Join(t.TempDir(), "diffs.json")
	_, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeStrict), WithDiffFile(path))
	require.Error(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var diffs []*SchemaDiff
	err = json.Unmarshal(data, &diffs)
	require.NoError(t, err)
	require.Len(t, diffs, 1)
	assert.True(t, diffs[0].TableMissing)
}

func TestEnsureDryRunWithDiffOutput(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Missing").
		Columns(AutoIncrCol("ID"))

	var buf bytes.Buffer
	result, err := Ensure(ctx, db, d, []*TableDef{table}, WithMode(ModeDryRun), WithDiffOutput(&buf))
	require.NoError(t, err)
	require.Len(t, result.Diffs, 1)

	// DryRun should still write diffs
	assert.Greater(t, buf.Len(), 0)
}

func TestEnsureDefaultIsStrict(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Missing").
		Columns(AutoIncrCol("ID"))

	// No WithMode — should default to ModeStrict
	_, err := Ensure(ctx, db, d, []*TableDef{table})
	require.Error(t, err)

	var ensureErr *EnsureError
	require.ErrorAs(t, err, &ensureErr)
}
