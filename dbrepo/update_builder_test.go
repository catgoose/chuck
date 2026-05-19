package dbrepo

import (
	"database/sql"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilder(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		q, args := NewUpdate("Users", "Name", "Email").Build()
		assert.Equal(t, "UPDATE Users SET Name = @Name, Email = @Email", q)
		assert.Empty(t, args)
	})

	t.Run("with_where", func(t *testing.T) {
		w := NewWhere().And("ID = @ID", sql.Named("ID", 42))
		q, args := NewUpdate("Users", "Name", "Email").
			Where(w).
			Build()
		assert.Equal(t, "UPDATE Users SET Name = @Name, Email = @Email WHERE ID = @ID", q)
		assert.Len(t, args, 1)
	})

	t.Run("with_where_semantic_filters", func(t *testing.T) {
		w := NewWhere().NotDeleted().HasStatus("active")
		q, args := NewUpdate("Tasks", "Title").
			Where(w).
			Build()
		assert.Contains(t, q, "UPDATE Tasks SET Title = @Title")
		assert.Contains(t, q, "WHERE DeletedAt IS NULL AND Status = @Status")
		assert.Len(t, args, 1)
	})

	t.Run("with_dialect_postgres", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		q, args := NewUpdate("Users", "Name", "Email").
			WithDialect(d).
			Build()
		assert.Equal(t, `UPDATE "users" SET "name" = @Name, "email" = @Email`, q)
		assert.Empty(t, args)
	})

	t.Run("with_dialect_mssql", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		q, args := NewUpdate("Users", "Name").
			WithDialect(d).
			Build()
		assert.Equal(t, "UPDATE [Users] SET [Name] = @Name", q)
		assert.Empty(t, args)
	})

	t.Run("with_dialect_sqlite", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		q, args := NewUpdate("Users", "Name").
			WithDialect(d).
			Build()
		assert.Equal(t, `UPDATE "Users" SET "Name" = @Name`, q)
		assert.Empty(t, args)
	})

	t.Run("returning_postgres", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		q, _ := NewUpdate("Users", "Name").
			WithDialect(d).
			Returning("ID", "Name").
			Build()
		assert.Equal(t, `UPDATE "users" SET "name" = @Name RETURNING ID, Name`, q)
	})

	t.Run("returning_sqlite", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		q, _ := NewUpdate("Users", "Name").
			WithDialect(d).
			Returning("ID").
			Build()
		assert.Equal(t, `UPDATE "Users" SET "Name" = @Name RETURNING ID`, q)
	})

	t.Run("returning_mssql_noop", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		q, _ := NewUpdate("Users", "Name").
			WithDialect(d).
			Returning("ID").
			Build()
		// MSSQL does not support RETURNING, so it should not appear
		assert.Equal(t, "UPDATE [Users] SET [Name] = @Name", q)
	})

	t.Run("full_query_with_where_and_returning", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		w := NewWhere().And("ID = @ID", sql.Named("ID", 1))
		q, args := NewUpdate("Users", "Name", "Email").
			WithDialect(d).
			Where(w).
			Returning("ID", "Name", "Email").
			Build()
		assert.Equal(t, `UPDATE "users" SET "name" = @Name, "email" = @Email WHERE ID = @ID RETURNING ID, Name, Email`, q)
		assert.Len(t, args, 1)
	})
}

func TestUpdateBuilder_SetValues_Positional(t *testing.T) {
	t.Run("no_dialect_emits_question_placeholders", func(t *testing.T) {
		q, args := NewUpdate("Users", "Name", "Email").
			SetValues("Alice", "alice@chuck.rock").
			Build()
		assert.Equal(t, "UPDATE Users SET Name = ?, Email = ?", q)
		assert.Equal(t, []any{"Alice", "alice@chuck.rock"}, args)
	})

	t.Run("with_where_set_args_before_where_args", func(t *testing.T) {
		// Issue #71 scenario: opt-out lets callers feed UpdateBuilder output
		// through sqlx.Rebind. SET values must precede WHERE args in args
		// slice so positional rewriting binds correctly.
		w := NewWhere().And("id = ?", 42)
		q, args := NewUpdate("accounts", "last_digest_sent_at").
			SetValues("2026-05-18T00:00:00Z").
			Where(w).
			Build()
		assert.Equal(t, "UPDATE accounts SET last_digest_sent_at = ? WHERE id = ?", q)
		assert.Equal(t, []any{"2026-05-18T00:00:00Z", 42}, args)
	})

	t.Run("postgres_dialect_preserves_quoting", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		q, args := NewUpdate("Users", "Name", "Email").
			WithDialect(d).
			SetValues("Alice", "alice@chuck.rock").
			Build()
		assert.Equal(t, `UPDATE "users" SET "name" = ?, "email" = ?`, q)
		assert.Equal(t, []any{"Alice", "alice@chuck.rock"}, args)
	})

	t.Run("mssql_dialect_preserves_quoting", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		q, _ := NewUpdate("Users", "Name").
			WithDialect(d).
			SetValues("Alice").
			Build()
		assert.Equal(t, "UPDATE [Users] SET [Name] = ?", q)
	})

	t.Run("sqlite_dialect_preserves_quoting", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		q, _ := NewUpdate("Users", "Name").
			WithDialect(d).
			SetValues("Alice").
			Build()
		assert.Equal(t, `UPDATE "Users" SET "Name" = ?`, q)
	})

	t.Run("returning_still_appended_in_positional_mode", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		q, args := NewUpdate("Users", "Name").
			WithDialect(d).
			SetValues("Alice").
			Where(NewWhere().And("id = ?", 7)).
			Returning("ID", "Name").
			Build()
		assert.Equal(t, `UPDATE "users" SET "name" = ? WHERE id = ? RETURNING ID, Name`, q)
		assert.Equal(t, []any{"Alice", 7}, args)
	})

	t.Run("schema_qualified_table_still_quoted", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		q, args := NewUpdate("sg.SalesAgents", "Name").
			WithDialect(d).
			SetValues("Gobo").
			Where(NewWhere().And("ID = ?", 1)).
			Build()
		assert.Equal(t, "UPDATE [sg].[SalesAgents] SET [Name] = ? WHERE ID = ?", q)
		assert.Equal(t, []any{"Gobo", 1}, args)
	})

	t.Run("zero_set_values_with_zero_cols_is_legal", func(t *testing.T) {
		// Degenerate edge case: caller wires SetValues path with empty column
		// list. Should not panic and should emit an empty SET clause; this
		// proves the length check is symmetric, not min-arity-1.
		q, args := NewUpdate("Users").SetValues().Build()
		assert.Equal(t, "UPDATE Users SET ", q)
		assert.Empty(t, args)
	})

	t.Run("mismatched_set_values_panics", func(t *testing.T) {
		assert.PanicsWithValue(t,
			"dbrepo: UpdateBuilder.SetValues count (1) does not match column count (2)",
			func() {
				_, _ = NewUpdate("Users", "Name", "Email").
					SetValues("Alice").
					Build()
			})
	})

	t.Run("default_named_path_unchanged", func(t *testing.T) {
		// Regression guard: confirm that not calling SetValues leaves the
		// existing @Name contract intact.
		q, args := NewUpdate("Users", "Name", "Email").Build()
		assert.Equal(t, "UPDATE Users SET Name = @Name, Email = @Email", q)
		assert.Empty(t, args)
	})
}
