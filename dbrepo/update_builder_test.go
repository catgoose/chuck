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
		assert.Equal(t, `UPDATE "Users" SET "Name" = @Name, "Email" = @Email`, q)
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
		assert.Equal(t, `UPDATE "Users" SET "Name" = @Name RETURNING ID, Name`, q)
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
		assert.Equal(t, `UPDATE "Users" SET "Name" = @Name, "Email" = @Email WHERE ID = @ID RETURNING ID, Name, Email`, q)
		assert.Len(t, args, 1)
	})
}
