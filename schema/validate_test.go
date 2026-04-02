package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/sqlite"
)

func TestValidateSchema(t *testing.T) {
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

	t.Run("valid_schema", func(t *testing.T) {
		errs := ValidateSchema(ctx, db, d, table)
		assert.Nil(t, errs)
	})

	t.Run("missing_column", func(t *testing.T) {
		// Declare a table with an extra column that doesn't exist in DB
		extra := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
				Col("Priority", TypeInt()),
			)

		errs := ValidateSchema(ctx, db, d, extra)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, "Items.Priority: column missing")
	})

	t.Run("table_not_exists", func(t *testing.T) {
		missing := NewTable("Nonexistent").
			Columns(AutoIncrCol("ID"))

		errs := ValidateSchema(ctx, db, d, missing)
		require.NotNil(t, errs)
		assert.Contains(t, errs[0].Error(), "does not exist")
	})

	t.Run("nullability_mismatch", func(t *testing.T) {
		// Declare Status as nullable, but it's NOT NULL in the DB
		mismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)),
			)

		errs := ValidateSchema(ctx, db, d, mismatch)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Status" && strings.Contains(e.Message, "nullability") {
				found = true
			}
		}
		assert.True(t, found, "expected nullability mismatch for Status, got: %v", errs)
	})

	t.Run("extra_live_column", func(t *testing.T) {
		// Declare fewer columns than exist in DB
		fewer := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
			)

		errs := ValidateSchema(ctx, db, d, fewer)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, "Items.Status: unexpected column (exists in database but not in declaration)")
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

		errs := ValidateSchema(ctx, db, d, withExtraIndex)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, `Items: index "idx_items_status" missing`)
	})
}

func TestValidateSchemaPostgresNormalization(t *testing.T) {
	// This test verifies that ValidateSchema normalizes column names
	// for the Postgres dialect, matching CamelCase declarations against
	// the snake_case columns that DDL creates.
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{} // SQLite doesn't normalize, used as baseline

	table := NewTable("Accounts").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
			Col("PasswordHash", TypeText()).NotNull(),
		)

	// Create with SQLite (no normalization)
	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Validate should pass — SQLite preserves CamelCase
	errs := ValidateSchema(ctx, db, d, table)
	assert.Nil(t, errs)

	// Verify Postgres normalization produces snake_case names in Snapshot
	pg := chuck.PostgresDialect{}
	snap := table.Snapshot(pg)
	assert.Equal(t, "accounts", snap.Name)
	assert.Equal(t, "email", snap.Columns[1].Name)
	assert.Equal(t, "password_hash", snap.Columns[2].Name)

	// TableNameFor should also normalize
	assert.Equal(t, "accounts", table.TableNameFor(pg))
	assert.Equal(t, "Accounts", table.TableNameFor(d))
}

func TestValidateSchemaIssue11Repro(t *testing.T) {
	// Issue #11: ValidateSchema should normalize CamelCase column names through
	// the dialect before comparing against the live database.
	// On Postgres, DDL creates snake_case columns, so Snapshot must also
	// produce snake_case names for the comparison to succeed.

	pg := chuck.PostgresDialect{}

	// Define table with PascalCase names (how users define schemas)
	table := NewTable("Accounts").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
			Col("PasswordHash", TypeText()).NotNull(),
		).
		WithTimestamps()

	// Snapshot with Postgres dialect must normalize all names
	snap := table.Snapshot(pg)
	assert.Equal(t, "accounts", snap.Name)
	assert.Equal(t, "id", snap.Columns[0].Name)
	assert.Equal(t, "email", snap.Columns[1].Name)
	assert.Equal(t, "password_hash", snap.Columns[2].Name)
	assert.Equal(t, "created_at", snap.Columns[3].Name)
	assert.Equal(t, "updated_at", snap.Columns[4].Name)

	// SelectColumnsFor must also normalize
	pgCols := table.SelectColumnsFor(pg)
	assert.Equal(t, []string{"id", "email", "password_hash", "created_at", "updated_at"}, pgCols)

	// InsertColumnsFor must normalize (excludes auto-increment)
	pgInsert := table.InsertColumnsFor(pg)
	assert.Equal(t, []string{"email", "password_hash", "created_at", "updated_at"}, pgInsert)

	// UpdateColumnsFor must normalize (only mutable)
	pgUpdate := table.UpdateColumnsFor(pg)
	assert.Contains(t, pgUpdate, "email")
	assert.Contains(t, pgUpdate, "password_hash")
	assert.Contains(t, pgUpdate, "updated_at")
	assert.NotContains(t, pgUpdate, "id")
	assert.NotContains(t, pgUpdate, "created_at")

	// End-to-end: validate with SQLite (which doesn't normalize) still works
	// when table and columns match exactly
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	errs := ValidateSchema(ctx, db, d, table)
	assert.Nil(t, errs, "expected no errors, got: %v", errs)
}

func TestValidateAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)).NotNull())
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)).NotNull())

	for _, tbl := range []*TableDef{users, tasks} {
		for _, stmt := range tbl.CreateIfNotExistsSQL(d) {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}
	}

	t.Run("all_valid", func(t *testing.T) {
		errs := ValidateAll(ctx, db, d, users, tasks)
		assert.Nil(t, errs)
	})

	t.Run("one_invalid", func(t *testing.T) {
		bad := NewTable("Tasks").
			Columns(
				AutoIncrCol("ID"),
				Col("Title", TypeString(255)).NotNull(),
				Col("Missing", TypeText()),
			)

		errs := ValidateAll(ctx, db, d, users, bad)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Missing" {
				found = true
			}
		}
		assert.True(t, found, "expected error for missing column 'Missing'")
	})
}
