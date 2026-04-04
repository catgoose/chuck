package dbrepo

import (
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
)

func TestSelectBuilder(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		sql, args := NewSelect("Users", "ID", "Name").Build()
		assert.Equal(t, "SELECT ID, Name FROM Users", sql)
		assert.Empty(t, args)
	})

	t.Run("with_where", func(t *testing.T) {
		w := NewWhere().NotDeleted().HasStatus("active")
		sql, args := NewSelect("Tasks", "ID", "Title").
			Where(w).
			Build()
		assert.Contains(t, sql, "SELECT ID, Title FROM Tasks")
		assert.Contains(t, sql, "WHERE DeletedAt IS NULL AND Status = @Status")
		assert.Len(t, args, 1)
	})

	t.Run("with_order_by", func(t *testing.T) {
		sql, _ := NewSelect("Tasks", "ID").
			OrderBy("CreatedAt DESC").
			Build()
		assert.Contains(t, sql, "ORDER BY CreatedAt DESC")
	})

	t.Run("with_pagination", func(t *testing.T) {
		sql, args := NewSelect("Tasks", "ID").
			OrderBy("ID ASC").
			Paginate(20, 40).
			Build()
		assert.Contains(t, sql, "LIMIT @Limit OFFSET @Offset")
		assert.Len(t, args, 2)
	})

	t.Run("with_dialect_pagination", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		sql, args := NewSelect("Tasks", "ID").
			OrderBy("ID ASC").
			Paginate(20, 40).
			WithDialect(d).
			Build()
		assert.Contains(t, sql, "OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY")
		assert.Len(t, args, 2)
	})

	t.Run("where_with_pagination_merges_args", func(t *testing.T) {
		w := NewWhere().HasStatus("active")
		sql, args := NewSelect("Tasks", "ID").
			Where(w).
			OrderBy("ID ASC").
			Paginate(25, 50).
			Build()
		assert.Contains(t, sql, "WHERE Status = @Status")
		assert.Contains(t, sql, "LIMIT @Limit OFFSET @Offset")
		// Must contain where args AND pagination args
		assert.Len(t, args, 3, "should have Status + Offset + Limit args")
	})

	t.Run("count_query", func(t *testing.T) {
		w := NewWhere().NotDeleted()
		sql, args := NewSelect("Tasks", "ID", "Title").
			Where(w).
			CountQuery()
		assert.Equal(t, "SELECT COUNT(*) FROM Tasks WHERE DeletedAt IS NULL", sql)
		assert.Empty(t, args)
	})

	t.Run("order_by_map", func(t *testing.T) {
		colMap := map[string]string{
			"name":  "Name",
			"date":  "CreatedAt",
		}
		sql, _ := NewSelect("Tasks", "ID").
			OrderByMap("name:asc,date:desc", colMap, "ID ASC").
			Build()
		assert.Contains(t, sql, "ORDER BY Name ASC, CreatedAt DESC")
	})

	t.Run("inner_join_postgres", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		sql, args := NewSelect("tasks", "tasks.id", "users.name").
			Join("users", "tasks.assignee_id = users.id").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT "tasks"."id", "users"."name" FROM "tasks" JOIN "users" ON tasks.assignee_id = users.id`, sql)
		assert.Empty(t, args)
	})

	t.Run("inner_join_sqlite", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		sql, args := NewSelect("Tasks", "Tasks.ID", "Users.Name").
			Join("Users", "Tasks.AssigneeID = Users.ID").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT "Tasks"."ID", "Users"."Name" FROM "Tasks" JOIN "Users" ON Tasks.AssigneeID = Users.ID`, sql)
		assert.Empty(t, args)
	})

	t.Run("inner_join_mssql", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		sql, args := NewSelect("Tasks", "Tasks.ID", "Users.Name").
			Join("Users", "Tasks.AssigneeID = Users.ID").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT [Tasks].[ID], [Users].[Name] FROM [Tasks] JOIN [Users] ON Tasks.AssigneeID = Users.ID`, sql)
		assert.Empty(t, args)
	})

	t.Run("left_join_postgres", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		sql, args := NewSelect("tasks", "tasks.id", "users.name").
			LeftJoin("users", "tasks.assignee_id = users.id").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT "tasks"."id", "users"."name" FROM "tasks" LEFT JOIN "users" ON tasks.assignee_id = users.id`, sql)
		assert.Empty(t, args)
	})

	t.Run("left_join_sqlite", func(t *testing.T) {
		d := chuck.SQLiteDialect{}
		sql, args := NewSelect("Tasks", "Tasks.ID", "Users.Name").
			LeftJoin("Users", "Tasks.AssigneeID = Users.ID").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT "Tasks"."ID", "Users"."Name" FROM "Tasks" LEFT JOIN "Users" ON Tasks.AssigneeID = Users.ID`, sql)
		assert.Empty(t, args)
	})

	t.Run("left_join_mssql", func(t *testing.T) {
		d := chuck.MSSQLDialect{}
		sql, args := NewSelect("Tasks", "Tasks.ID", "Users.Name").
			LeftJoin("Users", "Tasks.AssigneeID = Users.ID").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT [Tasks].[ID], [Users].[Name] FROM [Tasks] LEFT JOIN [Users] ON Tasks.AssigneeID = Users.ID`, sql)
		assert.Empty(t, args)
	})

	t.Run("multiple_joins", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		sql, args := NewSelect("tasks", "tasks.id", "users.name", "projects.title").
			Join("users", "tasks.assignee_id = users.id").
			LeftJoin("projects", "tasks.project_id = projects.id").
			WithDialect(d).
			Build()
		assert.Equal(t, `SELECT "tasks"."id", "users"."name", "projects"."title" FROM "tasks" JOIN "users" ON tasks.assignee_id = users.id LEFT JOIN "projects" ON tasks.project_id = projects.id`, sql)
		assert.Empty(t, args)
	})

	t.Run("join_with_where_orderby_pagination", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		w := NewWhere().NotDeleted().HasStatus("active")
		sql, args := NewSelect("tasks", "tasks.id", "users.name").
			Join("users", "tasks.assignee_id = users.id").
			Where(w).
			OrderBy("tasks.created_at DESC").
			Paginate(20, 40).
			WithDialect(d).
			Build()
		assert.Contains(t, sql, `SELECT "tasks"."id", "users"."name" FROM "tasks"`)
		assert.Contains(t, sql, `JOIN "users" ON tasks.assignee_id = users.id`)
		assert.Contains(t, sql, "WHERE DeletedAt IS NULL AND Status = @Status")
		assert.Contains(t, sql, "ORDER BY tasks.created_at DESC")
		assert.Contains(t, sql, "LIMIT @Limit OFFSET @Offset")
		assert.Len(t, args, 3, "should have Status + Offset + Limit args")
	})

	t.Run("count_query_with_join", func(t *testing.T) {
		d := chuck.PostgresDialect{}
		w := NewWhere().NotDeleted()
		sql, args := NewSelect("tasks", "tasks.id", "users.name").
			Join("users", "tasks.assignee_id = users.id").
			Where(w).
			WithDialect(d).
			CountQuery()
		assert.Equal(t, `SELECT COUNT(*) FROM "tasks" JOIN "users" ON tasks.assignee_id = users.id WHERE DeletedAt IS NULL`, sql)
		assert.Empty(t, args)
	})

	t.Run("dot_qualified_columns_without_dialect", func(t *testing.T) {
		sql, args := NewSelect("Tasks", "Tasks.ID", "Users.Name").
			Join("Users", "Tasks.AssigneeID = Users.ID").
			Build()
		// Without dialect, columns and tables are not quoted
		assert.Equal(t, "SELECT Tasks.ID, Users.Name FROM Tasks JOIN Users ON Tasks.AssigneeID = Users.ID", sql)
		assert.Empty(t, args)
	})
}
