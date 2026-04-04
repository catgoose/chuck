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

func TestDiffSchema(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
		).
		Indexes(
			Index("idx_items_name", "Name"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	t.Run("no_drift", func(t *testing.T) {
		diff, err := DiffSchema(ctx, db, d, table)
		require.NoError(t, err)
		assert.False(t, diff.HasDrift)
		assert.False(t, diff.TableMissing)
		assert.Empty(t, diff.AddedColumns)
		assert.Empty(t, diff.RemovedColumns)
		assert.Empty(t, diff.ChangedColumns)
		assert.Empty(t, diff.MissingIndexes)
		assert.Empty(t, diff.ExtraIndexes)
	})

	t.Run("missing_column", func(t *testing.T) {
		// Declare a table with an extra column not in DB
		extra := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
				Col("Priority", TypeInt()),
			)

		diff, err := DiffSchema(ctx, db, d, extra)
		require.NoError(t, err)
		assert.True(t, diff.HasDrift)
		require.Len(t, diff.AddedColumns, 1)
		assert.Equal(t, "Priority", diff.AddedColumns[0].Name)
	})

	t.Run("extra_column", func(t *testing.T) {
		// Declare fewer columns than exist in DB
		fewer := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
			)

		diff, err := DiffSchema(ctx, db, d, fewer)
		require.NoError(t, err)
		assert.True(t, diff.HasDrift)
		require.Len(t, diff.RemovedColumns, 1)
		assert.Equal(t, "Status", diff.RemovedColumns[0])
	})

	t.Run("nullability_mismatch", func(t *testing.T) {
		// Declare Status as nullable, but it's NOT NULL in the DB
		mismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)),
			)

		diff, err := DiffSchema(ctx, db, d, mismatch)
		require.NoError(t, err)
		assert.True(t, diff.HasDrift)
		require.Len(t, diff.ChangedColumns, 1)
		assert.Equal(t, "Status", diff.ChangedColumns[0].Name)
		assert.False(t, diff.ChangedColumns[0].DeclaredNotNull)
		assert.False(t, diff.ChangedColumns[0].LiveNullable) // NOT NULL in DB = not nullable
	})

	t.Run("missing_table", func(t *testing.T) {
		missing := NewTable("Nonexistent").
			Columns(AutoIncrCol("ID"))

		diff, err := DiffSchema(ctx, db, d, missing)
		require.NoError(t, err)
		assert.True(t, diff.HasDrift)
		assert.True(t, diff.TableMissing)
	})

	t.Run("missing_index", func(t *testing.T) {
		withExtraIndex := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
			).
			Indexes(
				Index("idx_items_name", "Name"),
				Index("idx_items_status", "Status"),
			)

		diff, err := DiffSchema(ctx, db, d, withExtraIndex)
		require.NoError(t, err)
		assert.True(t, diff.HasDrift)
		require.Len(t, diff.MissingIndexes, 1)
		assert.Equal(t, "idx_items_status", diff.MissingIndexes[0].Name)
	})

	t.Run("extra_index", func(t *testing.T) {
		// Declare no indexes, but the DB has one
		noIndexes := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
			)

		diff, err := DiffSchema(ctx, db, d, noIndexes)
		require.NoError(t, err)
		assert.True(t, diff.HasDrift)
		require.Len(t, diff.ExtraIndexes, 1)
		assert.Equal(t, "idx_items_name", diff.ExtraIndexes[0])
	})
}

func TestDiffSchemaWriteTo(t *testing.T) {
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

	diff, err := DiffSchema(ctx, db, d, table)
	require.NoError(t, err)

	var buf bytes.Buffer
	n, err := diff.WriteTo(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	// Verify it's valid JSON
	var parsed SchemaDiff
	err = json.Unmarshal(buf.Bytes(), &parsed)
	require.NoError(t, err)
	assert.Equal(t, "Items", parsed.Table)
	assert.False(t, parsed.HasDrift)
}

func TestDiffSchemaWriteJSON(t *testing.T) {
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

	diff, err := DiffSchema(ctx, db, d, table)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "diff.json")
	err = diff.WriteJSON(path)
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var parsed SchemaDiff
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "Items", parsed.Table)
}

func TestDiffAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)).NotNull())
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)).NotNull())

	// Only create Users, not Tasks
	for _, stmt := range users.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	diffs, err := DiffAll(ctx, db, d, users, tasks)
	require.NoError(t, err)
	require.Len(t, diffs, 2)

	// Users should have no drift
	assert.False(t, diffs[0].HasDrift)

	// Tasks should be missing
	assert.True(t, diffs[1].HasDrift)
	assert.True(t, diffs[1].TableMissing)
}

func TestWriteDiffsTo(t *testing.T) {
	diffs := []*SchemaDiff{
		{Table: "Users", HasDrift: false},
		{Table: "Tasks", TableMissing: true, HasDrift: true},
	}

	var buf bytes.Buffer
	n, err := WriteDiffsTo(diffs, &buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	var parsed []*SchemaDiff
	err = json.Unmarshal(buf.Bytes(), &parsed)
	require.NoError(t, err)
	require.Len(t, parsed, 2)
	assert.Equal(t, "Users", parsed[0].Table)
	assert.True(t, parsed[1].TableMissing)
}

func TestWriteDiffsJSON(t *testing.T) {
	diffs := []*SchemaDiff{
		{Table: "Users", HasDrift: false},
	}

	path := filepath.Join(t.TempDir(), "diffs.json")
	err := WriteDiffsJSON(diffs, path)
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var parsed []*SchemaDiff
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	require.Len(t, parsed, 1)
}
