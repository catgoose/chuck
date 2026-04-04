package dbrepo

import (
	"database/sql"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilder(t *testing.T) {
	t.Run("basic_no_where", func(t *testing.T) {
		q, args := NewDelete("Users").Build()
		assert.Equal(t, "DELETE FROM Users", q)
		assert.Empty(t, args)
	})

	t.Run("with_where", func(t *testing.T) {
		w := NewWhere().And("ID = @ID", sql.Named("ID", 42))
		q, args := NewDelete("Users").
			Where(w).
			Build()
		assert.Equal(t, "DELETE FROM Users WHERE ID = @ID", q)
		assert.Len(t, args, 1)
	})

	t.Run("with_where_semantic_filters", func(t *testing.T) {
		w := NewWhere().NotDeleted().HasStatus("active")
		q, args := NewDelete("Tasks").
			Where(w).
			Build()
		assert.Contains(t, q, "DELETE FROM Tasks")
		assert.Contains(t, q, "WHERE DeletedAt IS NULL AND Status = @Status")
		assert.Len(t, args, 1)
	})

	t.Run("with_dialect_postgres", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		q, _ := NewDelete("Users").
			WithDialect(d).
			Build()
		assert.Equal(t, `DELETE FROM "Users"`, q)
	})

	t.Run("with_dialect_mssql", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		q, _ := NewDelete("Users").
			WithDialect(d).
			Build()
		assert.Equal(t, "DELETE FROM [Users]", q)
	})

	t.Run("with_dialect_sqlite", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		q, _ := NewDelete("Users").
			WithDialect(d).
			Build()
		assert.Equal(t, `DELETE FROM "Users"`, q)
	})

	t.Run("returning_postgres", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		q, _ := NewDelete("Users").
			WithDialect(d).
			Returning("ID").
			Build()
		assert.Equal(t, `DELETE FROM "Users" RETURNING ID`, q)
	})

	t.Run("returning_sqlite", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		q, _ := NewDelete("Users").
			WithDialect(d).
			Returning("ID", "Name").
			Build()
		assert.Equal(t, `DELETE FROM "Users" RETURNING ID, Name`, q)
	})

	t.Run("returning_mssql_noop", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		q, _ := NewDelete("Users").
			WithDialect(d).
			Returning("ID").
			Build()
		assert.Equal(t, "DELETE FROM [Users]", q)
	})

	t.Run("full_query_with_where_and_returning", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		w := NewWhere().And("ID = @ID", sql.Named("ID", 1))
		q, args := NewDelete("Users").
			WithDialect(d).
			Where(w).
			Returning("ID").
			Build()
		assert.Equal(t, `DELETE FROM "Users" WHERE ID = @ID RETURNING ID`, q)
		assert.Len(t, args, 1)
	})
}
