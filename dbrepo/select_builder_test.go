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
}
