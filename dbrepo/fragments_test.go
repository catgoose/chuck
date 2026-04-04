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

func TestUpsertInto(t *testing.T) {
	t.Run("Postgres", func(t *testing.T) {
		d, _ := chuck.New(chuck.Postgres)
		result := UpsertInto(d, "Users", []string{"Email"}, "Email", "Name", "Age")
		expected := `INSERT INTO "Users" (Email, Name, Age) VALUES (@Email, @Name, @Age) ON CONFLICT (Email) DO UPDATE SET Name = EXCLUDED.Name, Age = EXCLUDED.Age`
		assert.Equal(t, expected, result)
	})

	t.Run("SQLite", func(t *testing.T) {
		d, _ := chuck.New(chuck.SQLite)
		result := UpsertInto(d, "Users", []string{"Email"}, "Email", "Name", "Age")
		expected := `INSERT INTO "Users" (Email, Name, Age) VALUES (@Email, @Name, @Age) ON CONFLICT (Email) DO UPDATE SET Name = EXCLUDED.Name, Age = EXCLUDED.Age`
		assert.Equal(t, expected, result)
	})

	t.Run("MSSQL", func(t *testing.T) {
		d, _ := chuck.New(chuck.MSSQL)
		result := UpsertInto(d, "Users", []string{"Email"}, "Email", "Name", "Age")
		assert.Contains(t, result, "MERGE [Users] AS Target")
		assert.Contains(t, result, "USING (VALUES (@Email, @Name, @Age)) AS Source (Email, Name, Age)")
		assert.Contains(t, result, "ON Target.Email = Source.Email")
		assert.Contains(t, result, "WHEN MATCHED THEN UPDATE SET Name = Source.Name, Age = Source.Age")
		assert.Contains(t, result, "WHEN NOT MATCHED THEN INSERT (Email, Name, Age) VALUES (@Email, @Name, @Age)")
	})

	t.Run("multiple_conflict_columns", func(t *testing.T) {
		d, _ := chuck.New(chuck.Postgres)
		result := UpsertInto(d, "Events", []string{"UserID", "EventDate"}, "UserID", "EventDate", "Score", "Notes")
		expected := `INSERT INTO "Events" (UserID, EventDate, Score, Notes) VALUES (@UserID, @EventDate, @Score, @Notes) ON CONFLICT (UserID, EventDate) DO UPDATE SET Score = EXCLUDED.Score, Notes = EXCLUDED.Notes`
		assert.Equal(t, expected, result)
	})

	t.Run("single_update_column", func(t *testing.T) {
		d, _ := chuck.New(chuck.Postgres)
		result := UpsertInto(d, "Config", []string{"Key"}, "Key", "Value")
		expected := `INSERT INTO "Config" (Key, Value) VALUES (@Key, @Value) ON CONFLICT (Key) DO UPDATE SET Value = EXCLUDED.Value`
		assert.Equal(t, expected, result)
	})
}

func TestUpsertIntoQ(t *testing.T) {
	t.Run("Postgres", func(t *testing.T) {
		d, _ := chuck.New(chuck.Postgres)
		result := UpsertIntoQ(d, "Users", []string{"Email"}, "Email", "Name", "Age")
		expected := `INSERT INTO "Users" ("Email", "Name", "Age") VALUES (@Email, @Name, @Age) ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name", "Age" = EXCLUDED."Age"`
		assert.Equal(t, expected, result)
	})

	t.Run("SQLite", func(t *testing.T) {
		d, _ := chuck.New(chuck.SQLite)
		result := UpsertIntoQ(d, "Users", []string{"Email"}, "Email", "Name", "Age")
		expected := `INSERT INTO "Users" ("Email", "Name", "Age") VALUES (@Email, @Name, @Age) ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name", "Age" = EXCLUDED."Age"`
		assert.Equal(t, expected, result)
	})

	t.Run("MSSQL", func(t *testing.T) {
		d, _ := chuck.New(chuck.MSSQL)
		result := UpsertIntoQ(d, "Users", []string{"Email"}, "Email", "Name", "Age")
		assert.Contains(t, result, "MERGE [Users] AS Target")
		assert.Contains(t, result, "USING (VALUES (@Email, @Name, @Age)) AS Source ([Email], [Name], [Age])")
		assert.Contains(t, result, "ON Target.[Email] = Source.[Email]")
		assert.Contains(t, result, "WHEN MATCHED THEN UPDATE SET [Name] = Source.[Name], [Age] = Source.[Age]")
		assert.Contains(t, result, "WHEN NOT MATCHED THEN INSERT ([Email], [Name], [Age]) VALUES (@Email, @Name, @Age)")
	})

	t.Run("multiple_conflict_columns_MSSQL", func(t *testing.T) {
		d, _ := chuck.New(chuck.MSSQL)
		result := UpsertIntoQ(d, "Events", []string{"UserID", "EventDate"}, "UserID", "EventDate", "Score")
		assert.Contains(t, result, "ON Target.[UserID] = Source.[UserID] AND Target.[EventDate] = Source.[EventDate]")
		assert.Contains(t, result, "WHEN MATCHED THEN UPDATE SET [Score] = Source.[Score]")
	})
}
