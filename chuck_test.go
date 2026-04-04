package chuck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEngine(t *testing.T) {
	tests := []struct {
		input    string
		expected Engine
		wantErr  bool
	}{
		{"sqlserver", MSSQL, false},
		{"mssql", MSSQL, false},
		{"sqlite3", SQLite, false},
		{"sqlite", SQLite, false},
		{"postgres", Postgres, false},
		{"postgresql", Postgres, false},
		{"", "", true},
		{"mysql", "", true},
		{"oracle", "", true},
		{"POSTGRES", "", true},
		{"Sqlite", "", true},
	}
	for _, tt := range tests {
		name := tt.input
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			engine, err := ParseEngine(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unknown database engine")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, engine)
			}
		})
	}
}

func TestParseEngineRoundTrip(t *testing.T) {
	// Engine constant values should parse back to themselves
	engines := []Engine{MSSQL, SQLite, Postgres}
	for _, e := range engines {
		parsed, err := ParseEngine(string(e))
		require.NoError(t, err)
		assert.Equal(t, e, parsed)
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		engine  Engine
		wantErr bool
	}{
		{MSSQL, false},
		{SQLite, false},
		{Postgres, false},
		{Engine("unknown"), true},
		{Engine(""), true},
	}
	for _, tt := range tests {
		name := string(tt.engine)
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			d, err := New(tt.engine)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, d)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.engine, d.Engine())
			}
		})
	}
}

func TestNewReturnsCorrectTypes(t *testing.T) {
	d, _ := New(MSSQL)
	assert.IsType(t, MSSQLDialect{}, d)

	d, _ = New(SQLite)
	assert.IsType(t, SQLiteDialect{}, d)

	d, _ = New(Postgres)
	assert.IsType(t, PostgresDialect{}, d)
}

func TestMSSQLDialect(t *testing.T) {
	d := MSSQLDialect{}

	t.Run("Engine", func(t *testing.T) {
		assert.Equal(t, MSSQL, d.Engine())
	})

	t.Run("Pagination", func(t *testing.T) {
		assert.Equal(t, "OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY", d.Pagination())
	})

	t.Run("AutoIncrement", func(t *testing.T) {
		assert.Equal(t, "INT PRIMARY KEY IDENTITY(1,1)", d.AutoIncrement())
	})

	t.Run("Now", func(t *testing.T) {
		assert.Equal(t, "GETDATE()", d.Now())
	})

	t.Run("ColumnTypes", func(t *testing.T) {
		assert.Equal(t, "DATETIME", d.TimestampType())
		assert.Equal(t, "NVARCHAR(255)", d.StringType(255))
		assert.Equal(t, "NVARCHAR(50)", d.StringType(50))
		assert.Equal(t, "VARCHAR(255)", d.VarcharType(255))
		assert.Equal(t, "VARCHAR(100)", d.VarcharType(100))
		assert.Equal(t, "INT", d.IntType())
		assert.Equal(t, "BIGINT", d.BigIntType())
		assert.Equal(t, "FLOAT", d.FloatType())
		assert.Equal(t, "DECIMAL(10,2)", d.DecimalType(10, 2))
		assert.Equal(t, "NVARCHAR(MAX)", d.TextType())
		assert.Equal(t, "BIT", d.BoolType())
		assert.Equal(t, "UNIQUEIDENTIFIER", d.UUIDType())
		assert.Equal(t, "NVARCHAR(MAX)", d.JSONType())
	})

	t.Run("QuoteIdentifier", func(t *testing.T) {
		assert.Equal(t, "[Users]", d.QuoteIdentifier("Users"))
		assert.Equal(t, "[order]", d.QuoteIdentifier("order"))
		// Brackets in names are escaped by doubling
		assert.Equal(t, "[test]]table]", d.QuoteIdentifier("test]table"))
	})

	t.Run("LastInsertID", func(t *testing.T) {
		assert.Equal(t, "SELECT SCOPE_IDENTITY() AS ID", d.LastInsertIDQuery())
		assert.False(t, d.SupportsLastInsertID())
	})

	t.Run("Placeholder", func(t *testing.T) {
		assert.Equal(t, "@p1", d.Placeholder(1))
		assert.Equal(t, "@p2", d.Placeholder(2))
		assert.Equal(t, "@p10", d.Placeholder(10))
	})

	t.Run("ReturningClause", func(t *testing.T) {
		assert.Empty(t, d.ReturningClause("id"))
		assert.Empty(t, d.ReturningClause("id, name"))
	})

	t.Run("CreateTableIfNotExists", func(t *testing.T) {
		create := d.CreateTableIfNotExists("Users", "id INT PRIMARY KEY")
		assert.Contains(t, create, "IF NOT EXISTS")
		assert.Contains(t, create, "OBJECT_ID(N'[Users]')")
		assert.Contains(t, create, "CREATE TABLE [Users]")
		assert.Contains(t, create, "id INT PRIMARY KEY")
	})

	t.Run("DropTableIfExists", func(t *testing.T) {
		drop := d.DropTableIfExists("Users")
		assert.Contains(t, drop, "IF EXISTS")
		assert.Contains(t, drop, "OBJECT_ID(N'[Users]')")
		assert.Contains(t, drop, "DROP TABLE [Users]")
	})

	t.Run("CreateIndexIfNotExists", func(t *testing.T) {
		idx := d.CreateIndexIfNotExists("idx_users_mail", "Users", "Mail")
		assert.Contains(t, idx, "IF NOT EXISTS")
		assert.Contains(t, idx, "sys.indexes")
		assert.Contains(t, idx, "CREATE INDEX [idx_users_mail] ON [Users]([Mail])")
	})

	t.Run("SchemaQueries", func(t *testing.T) {
		assert.Contains(t, d.TableExistsQuery(), "sys.objects")
		assert.Contains(t, d.TableExistsQuery(), "OBJECT_ID(@p1)")
		assert.Contains(t, d.TableColumnsQuery(), "INFORMATION_SCHEMA.COLUMNS")
		assert.Contains(t, d.TableColumnsQuery(), "TABLE_NAME = @p1")
	})

	t.Run("InsertOrIgnore", func(t *testing.T) {
		stmt := d.InsertOrIgnore("Users", "Name, Email", "'Alice', 'alice@test.com'")
		assert.Contains(t, stmt, "BEGIN TRY")
		assert.Contains(t, stmt, "INSERT INTO [Users]")
		assert.Contains(t, stmt, "'Alice'")
		assert.Contains(t, stmt, "END CATCH")
	})

	t.Run("Upsert", func(t *testing.T) {
		stmt := d.Upsert("Users", "[Name], [Email]", "@Name, @Email", "[Email]", "[Name] = Source.[Name]")
		assert.Contains(t, stmt, "MERGE [Users] AS Target")
		assert.Contains(t, stmt, "USING (VALUES (@Name, @Email)) AS Source ([Name], [Email])")
		assert.Contains(t, stmt, "ON Target.[Email] = Source.[Email]")
		assert.Contains(t, stmt, "WHEN MATCHED THEN UPDATE SET [Name] = Source.[Name]")
		assert.Contains(t, stmt, "WHEN NOT MATCHED THEN INSERT ([Name], [Email]) VALUES (@Name, @Email)")
	})
}

func TestSQLiteDialect(t *testing.T) {
	d := SQLiteDialect{}

	t.Run("Engine", func(t *testing.T) {
		assert.Equal(t, SQLite, d.Engine())
	})

	t.Run("Pagination", func(t *testing.T) {
		assert.Equal(t, "LIMIT @Limit OFFSET @Offset", d.Pagination())
	})

	t.Run("AutoIncrement", func(t *testing.T) {
		assert.Equal(t, "INTEGER PRIMARY KEY AUTOINCREMENT", d.AutoIncrement())
	})

	t.Run("Now", func(t *testing.T) {
		assert.Equal(t, "CURRENT_TIMESTAMP", d.Now())
	})

	t.Run("ColumnTypes", func(t *testing.T) {
		assert.Equal(t, "TIMESTAMP", d.TimestampType())
		// SQLite ignores maxLen for string types
		assert.Equal(t, "TEXT", d.StringType(255))
		assert.Equal(t, "TEXT", d.StringType(50))
		assert.Equal(t, "TEXT", d.VarcharType(255))
		assert.Equal(t, "TEXT", d.VarcharType(100))
		assert.Equal(t, "INTEGER", d.IntType())
		assert.Equal(t, "INTEGER", d.BigIntType())
		assert.Equal(t, "REAL", d.FloatType())
		assert.Equal(t, "REAL", d.DecimalType(10, 2))
		assert.Equal(t, "TEXT", d.TextType())
		assert.Equal(t, "INTEGER", d.BoolType())
		assert.Equal(t, "TEXT", d.UUIDType())
		assert.Equal(t, "TEXT", d.JSONType())
	})

	t.Run("QuoteIdentifier", func(t *testing.T) {
		assert.Equal(t, `"Users"`, d.QuoteIdentifier("Users"))
		assert.Equal(t, `"order"`, d.QuoteIdentifier("order"))
	})

	t.Run("LastInsertID", func(t *testing.T) {
		assert.Empty(t, d.LastInsertIDQuery())
		assert.True(t, d.SupportsLastInsertID())
	})

	t.Run("Placeholder", func(t *testing.T) {
		// SQLite always uses ? regardless of position
		assert.Equal(t, "?", d.Placeholder(1))
		assert.Equal(t, "?", d.Placeholder(2))
		assert.Equal(t, "?", d.Placeholder(10))
	})

	t.Run("ReturningClause", func(t *testing.T) {
		assert.Equal(t, "RETURNING id", d.ReturningClause("id"))
		assert.Equal(t, "RETURNING id, created_at", d.ReturningClause("id, created_at"))
	})

	t.Run("CreateTableIfNotExists", func(t *testing.T) {
		create := d.CreateTableIfNotExists("Users", "id INTEGER PRIMARY KEY")
		assert.Equal(t, `CREATE TABLE IF NOT EXISTS "Users" (id INTEGER PRIMARY KEY)`, create)
	})

	t.Run("DropTableIfExists", func(t *testing.T) {
		assert.Equal(t, `DROP TABLE IF EXISTS "Users"`, d.DropTableIfExists("Users"))
	})

	t.Run("CreateIndexIfNotExists", func(t *testing.T) {
		assert.Equal(t, `CREATE INDEX IF NOT EXISTS "idx_users_mail" ON "Users"("Mail")`,
			d.CreateIndexIfNotExists("idx_users_mail", "Users", "Mail"))
	})

	t.Run("SchemaQueries", func(t *testing.T) {
		assert.Contains(t, d.TableExistsQuery(), "sqlite_master")
		assert.Contains(t, d.TableExistsQuery(), "name=?")
		assert.Contains(t, d.TableColumnsQuery(), "pragma_table_info(?)")
	})

	t.Run("InsertOrIgnore", func(t *testing.T) {
		stmt := d.InsertOrIgnore("Users", "Name, Email", "'Alice', 'alice@test.com'")
		assert.Equal(t, `INSERT OR IGNORE INTO "Users" (Name, Email) VALUES ('Alice', 'alice@test.com')`, stmt)
	})

	t.Run("Upsert", func(t *testing.T) {
		stmt := d.Upsert("Users", `"Name", "Email"`, "@Name, @Email", `"Email"`, `"Name" = EXCLUDED."Name"`)
		assert.Equal(t, `INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email) ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name"`, stmt)
	})
}

func TestPostgresDialect(t *testing.T) {
	d := PostgresDialect{}

	t.Run("Engine", func(t *testing.T) {
		assert.Equal(t, Postgres, d.Engine())
	})

	t.Run("Pagination", func(t *testing.T) {
		assert.Equal(t, "LIMIT @Limit OFFSET @Offset", d.Pagination())
	})

	t.Run("AutoIncrement", func(t *testing.T) {
		assert.Equal(t, "SERIAL PRIMARY KEY", d.AutoIncrement())
	})

	t.Run("Now", func(t *testing.T) {
		assert.Equal(t, "NOW()", d.Now())
	})

	t.Run("ColumnTypes", func(t *testing.T) {
		assert.Equal(t, "TIMESTAMPTZ", d.TimestampType())
		// Postgres StringType returns TEXT (not VARCHAR)
		assert.Equal(t, "TEXT", d.StringType(255))
		assert.Equal(t, "TEXT", d.StringType(50))
		assert.Equal(t, "VARCHAR(255)", d.VarcharType(255))
		assert.Equal(t, "VARCHAR(100)", d.VarcharType(100))
		assert.Equal(t, "INTEGER", d.IntType())
		assert.Equal(t, "BIGINT", d.BigIntType())
		assert.Equal(t, "DOUBLE PRECISION", d.FloatType())
		assert.Equal(t, "NUMERIC(10,2)", d.DecimalType(10, 2))
		assert.Equal(t, "TEXT", d.TextType())
		assert.Equal(t, "BOOLEAN", d.BoolType())
		assert.Equal(t, "UUID", d.UUIDType())
		assert.Equal(t, "JSONB", d.JSONType())
	})

	t.Run("NormalizeIdentifier", func(t *testing.T) {
		assert.Equal(t, "users", d.NormalizeIdentifier("Users"))
		assert.Equal(t, "created_at", d.NormalizeIdentifier("CreatedAt"))
		assert.Equal(t, "user_id", d.NormalizeIdentifier("UserID"))
		assert.Equal(t, "html_parser", d.NormalizeIdentifier("HTMLParser"))
		assert.Equal(t, "already_snake", d.NormalizeIdentifier("already_snake"))
	})

	t.Run("QuoteIdentifier", func(t *testing.T) {
		assert.Equal(t, `"Users"`, d.QuoteIdentifier("Users"))
		assert.Equal(t, `"order"`, d.QuoteIdentifier("order"))
	})

	t.Run("LastInsertID", func(t *testing.T) {
		assert.Empty(t, d.LastInsertIDQuery())
		assert.False(t, d.SupportsLastInsertID())
	})

	t.Run("Placeholder", func(t *testing.T) {
		assert.Equal(t, "$1", d.Placeholder(1))
		assert.Equal(t, "$2", d.Placeholder(2))
		assert.Equal(t, "$10", d.Placeholder(10))
	})

	t.Run("ReturningClause", func(t *testing.T) {
		assert.Equal(t, "RETURNING id", d.ReturningClause("id"))
		assert.Equal(t, "RETURNING id, created_at", d.ReturningClause("id, created_at"))
	})

	t.Run("CreateTableIfNotExists", func(t *testing.T) {
		create := d.CreateTableIfNotExists("Users", "id SERIAL PRIMARY KEY")
		assert.Equal(t, `CREATE TABLE IF NOT EXISTS "Users" (id SERIAL PRIMARY KEY)`, create)
	})

	t.Run("DropTableIfExists", func(t *testing.T) {
		assert.Equal(t, `DROP TABLE IF EXISTS "Users"`, d.DropTableIfExists("Users"))
	})

	t.Run("CreateIndexIfNotExists", func(t *testing.T) {
		assert.Equal(t, `CREATE INDEX IF NOT EXISTS "idx_users_mail" ON "Users"("mail")`,
			d.CreateIndexIfNotExists("idx_users_mail", "Users", "Mail"))
	})

	t.Run("SchemaQueries", func(t *testing.T) {
		eq := d.TableExistsQuery()
		assert.Contains(t, eq, "information_schema.tables")
		assert.Contains(t, eq, "table_schema = 'public'")
		assert.Contains(t, eq, "$1")

		cq := d.TableColumnsQuery()
		assert.Contains(t, cq, "information_schema.columns")
		assert.Contains(t, cq, "table_schema = 'public'")
		assert.Contains(t, cq, "$1")
		assert.Contains(t, cq, "ORDER BY ordinal_position")
	})

	t.Run("InsertOrIgnore", func(t *testing.T) {
		stmt := d.InsertOrIgnore("Users", "Name, Email", "'Alice', 'alice@test.com'")
		assert.Equal(t, `INSERT INTO "Users" (Name, Email) VALUES ('Alice', 'alice@test.com') ON CONFLICT DO NOTHING`, stmt)
	})

	t.Run("Upsert", func(t *testing.T) {
		stmt := d.Upsert("Users", `"Name", "Email"`, "@Name, @Email", `"Email"`, `"Name" = EXCLUDED."Name"`)
		assert.Equal(t, `INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email) ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name"`, stmt)
	})
}

// TestDialectConsistency verifies cross-dialect invariants that all
// implementations must satisfy.
func TestDialectConsistency(t *testing.T) {
	dialects := []Dialect{
		MSSQLDialect{},
		SQLiteDialect{},
		PostgresDialect{},
	}

	for _, d := range dialects {
		name := string(d.Engine())
		t.Run(name+"/non_empty_methods", func(t *testing.T) {
			assert.NotEmpty(t, d.Engine())
			assert.NotEmpty(t, d.Pagination())
			assert.NotEmpty(t, d.AutoIncrement())
			assert.NotEmpty(t, d.Now())
			assert.NotEmpty(t, d.TimestampType())
			assert.NotEmpty(t, d.StringType(255))
			assert.NotEmpty(t, d.VarcharType(255))
			assert.NotEmpty(t, d.IntType())
			assert.NotEmpty(t, d.BigIntType())
			assert.NotEmpty(t, d.FloatType())
			assert.NotEmpty(t, d.DecimalType(10, 2))
			assert.NotEmpty(t, d.TextType())
			assert.NotEmpty(t, d.BoolType())
			assert.NotEmpty(t, d.UUIDType())
			assert.NotEmpty(t, d.JSONType())
			assert.NotEmpty(t, d.Placeholder(1))
			assert.NotEmpty(t, d.QuoteIdentifier("test"))
			assert.NotEmpty(t, d.CreateTableIfNotExists("t", "id INT"))
			assert.NotEmpty(t, d.DropTableIfExists("t"))
			assert.NotEmpty(t, d.CreateIndexIfNotExists("idx", "t", "col"))
			assert.NotEmpty(t, d.TableExistsQuery())
			assert.NotEmpty(t, d.TableColumnsQuery())
		})

		t.Run(name+"/last_insert_id_consistency", func(t *testing.T) {
			// If the driver doesn't support LastInsertId(), there must be a query fallback
			if !d.SupportsLastInsertID() && d.LastInsertIDQuery() == "" {
				// Postgres and similar: uses RETURNING clause instead
				assert.NotEmpty(t, d.ReturningClause("id"),
					"dialect without SupportsLastInsertID or LastInsertIDQuery must support ReturningClause")
			}
		})

		t.Run(name+"/upsert_contains_table_name", func(t *testing.T) {
			table := "test_table"
			stmt := d.Upsert(table, "col1, col2", "@col1, @col2", "col1", "col2 = EXCLUDED.col2")
			assert.Contains(t, stmt, table)
		})

		t.Run(name+"/ddl_contains_table_name", func(t *testing.T) {
			table := "test_table"
			assert.Contains(t, d.CreateTableIfNotExists(table, "id INT"), table)
			assert.Contains(t, d.DropTableIfExists(table), table)
		})

		t.Run(name+"/index_contains_all_parts", func(t *testing.T) {
			idx := d.CreateIndexIfNotExists("idx_test", "my_table", "col1, col2")
			assert.Contains(t, idx, "idx_test")
			assert.Contains(t, idx, "my_table")
			assert.Contains(t, idx, "col1")
			assert.Contains(t, idx, "col2")
		})
	}
}

func TestQuoteColumns(t *testing.T) {
	dialects := []struct {
		name    string
		dialect Dialect
		// quote wraps the expected column name in the dialect's quoting style
		quote func(string) string
	}{
		{
			name:    "SQLite",
			dialect: SQLiteDialect{},
			quote:   func(s string) string { return `"` + s + `"` },
		},
		{
			name:    "Postgres",
			dialect: PostgresDialect{},
			quote:   func(s string) string { return `"` + s + `"` },
		},
		{
			name:    "MSSQL",
			dialect: MSSQLDialect{},
			quote:   func(s string) string { return "[" + s + "]" },
		},
	}

	for _, dd := range dialects {
		t.Run(dd.name, func(t *testing.T) {
			d := dd.dialect
			q := dd.quote

			t.Run("single column no suffix", func(t *testing.T) {
				assert.Equal(t, q("col1"), QuoteColumns(d, "col1"))
			})

			t.Run("multi column no suffix", func(t *testing.T) {
				expected := q("col1") + ", " + q("col2")
				assert.Equal(t, expected, QuoteColumns(d, "col1, col2"))
			})

			t.Run("DESC suffix preserved", func(t *testing.T) {
				expected := q("col1") + ", " + q("col2") + " DESC"
				assert.Equal(t, expected, QuoteColumns(d, "col1, col2 DESC"))
			})

			t.Run("ASC and DESC suffixes preserved", func(t *testing.T) {
				expected := q("col1") + " ASC, " + q("col2") + " DESC"
				assert.Equal(t, expected, QuoteColumns(d, "col1 ASC, col2 DESC"))
			})

			t.Run("case insensitive suffix normalized to uppercase", func(t *testing.T) {
				expected := q("col1") + ", " + q("col2") + " DESC"
				assert.Equal(t, expected, QuoteColumns(d, "col1, col2 desc"))
			})

			t.Run("ASC lowercase normalized", func(t *testing.T) {
				expected := q("col1") + " ASC"
				assert.Equal(t, expected, QuoteColumns(d, "col1 asc"))
			})
		})
	}
}

// TestDialectViaNew ensures New returns dialects that behave identically
// to directly constructed structs.
func TestDialectViaNew(t *testing.T) {
	tests := []struct {
		engine Engine
		want   Dialect
	}{
		{MSSQL, MSSQLDialect{}},
		{SQLite, SQLiteDialect{}},
		{Postgres, PostgresDialect{}},
	}
	for _, tt := range tests {
		t.Run(string(tt.engine), func(t *testing.T) {
			got, err := New(tt.engine)
			require.NoError(t, err)
			// Verify key methods return the same values
			assert.Equal(t, tt.want.Engine(), got.Engine())
			assert.Equal(t, tt.want.Pagination(), got.Pagination())
			assert.Equal(t, tt.want.AutoIncrement(), got.AutoIncrement())
			assert.Equal(t, tt.want.Now(), got.Now())
			assert.Equal(t, tt.want.TimestampType(), got.TimestampType())
			assert.Equal(t, tt.want.BoolType(), got.BoolType())
			assert.Equal(t, tt.want.Placeholder(1), got.Placeholder(1))
			assert.Equal(t, tt.want.ReturningClause("id"), got.ReturningClause("id"))
		})
	}
}
