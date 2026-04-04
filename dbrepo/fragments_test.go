package dbrepo

import (
	"database/sql"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
)

func TestColumns(t *testing.T) {
	assert.Equal(t, "ID, Name, Email", Columns("ID", "Name", "Email"))
	assert.Equal(t, "ID", Columns("ID"))
}

func TestPlaceholders(t *testing.T) {
	assert.Equal(t, "@ID, @Name, @Email", Placeholders("ID", "Name", "Email"))
	assert.Equal(t, "@ID", Placeholders("ID"))
}

func TestSetClause(t *testing.T) {
	assert.Equal(t, "Name = @Name, Email = @Email", SetClause("Name", "Email"))
}

func TestInsertInto(t *testing.T) {
	result := InsertInto("Users", "Name", "Email")
	assert.Equal(t, "INSERT INTO Users (Name, Email) VALUES (@Name, @Email)", result)
}

func TestBulkInsertInto(t *testing.T) {
	tests := []struct {
		name     string
		dialect  chuck.Dialect
		table    string
		cols     []string
		rowCount int
		want     string
	}{
		{
			name:     "postgres 2 cols 3 rows",
			dialect:  chuck.PostgresDialect{},
			table:    "users",
			cols:     []string{"name", "email"},
			rowCount: 3,
			want:     `INSERT INTO "users" ("name", "email") VALUES ($1, $2), ($3, $4), ($5, $6)`,
		},
		{
			name:     "sqlite 2 cols 3 rows",
			dialect:  chuck.SQLiteDialect{},
			table:    "Users",
			cols:     []string{"Name", "Email"},
			rowCount: 3,
			want:     `INSERT INTO "Users" ("Name", "Email") VALUES (?, ?), (?, ?), (?, ?)`,
		},
		{
			name:     "mssql 2 cols 3 rows",
			dialect:  chuck.MSSQLDialect{},
			table:    "Users",
			cols:     []string{"Name", "Email"},
			rowCount: 3,
			want:     `INSERT INTO [Users] ([Name], [Email]) VALUES (@p1, @p2), (@p3, @p4), (@p5, @p6)`,
		},
		{
			name:     "postgres single row",
			dialect:  chuck.PostgresDialect{},
			table:    "events",
			cols:     []string{"type", "payload", "created_at"},
			rowCount: 1,
			want:     `INSERT INTO "events" ("type", "payload", "created_at") VALUES ($1, $2, $3)`,
		},
		{
			name:     "sqlite single column many rows",
			dialect:  chuck.SQLiteDialect{},
			table:    "tags",
			cols:     []string{"label"},
			rowCount: 5,
			want:     `INSERT INTO "tags" ("label") VALUES (?), (?), (?), (?), (?)`,
		},
		{
			name:     "mssql 4 cols 2 rows",
			dialect:  chuck.MSSQLDialect{},
			table:    "Orders",
			cols:     []string{"CustomerID", "Product", "Qty", "Price"},
			rowCount: 2,
			want:     `INSERT INTO [Orders] ([CustomerID], [Product], [Qty], [Price]) VALUES (@p1, @p2, @p3, @p4), (@p5, @p6, @p7, @p8)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BulkInsertInto(tt.dialect, tt.table, tt.cols, tt.rowCount)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNamedArgs(t *testing.T) {
	args := NamedArgs(map[string]any{
		"Name":  "Gobo",
		"Email": "gobo@chuck.rock",
	})
	assert.Len(t, args, 2)
	// Keys sorted: Email, Name
	assert.Equal(t, sql.Named("Email", "gobo@chuck.rock"), args[0])
	assert.Equal(t, sql.Named("Name", "Gobo"), args[1])
}
