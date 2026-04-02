package schema

import (
	"strings"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTable(t *testing.T) {
	table := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull().Unique(),
			Col("Name", TypeString(255)),
		).
		WithTimestamps().
		Indexes(
			Index("idx_users_email", "Email"),
		)

	assert.Equal(t, "Users", table.Name)

	t.Run("SelectColumns", func(t *testing.T) {
		cols := table.SelectColumns()
		assert.Contains(t, cols, "ID")
		assert.Contains(t, cols, "Email")
		assert.Contains(t, cols, "Name")
		assert.Contains(t, cols, "CreatedAt")
		assert.Contains(t, cols, "UpdatedAt")
	})

	t.Run("InsertColumns", func(t *testing.T) {
		cols := table.InsertColumns()
		assert.NotContains(t, cols, "ID") // auto-increment excluded
		assert.Contains(t, cols, "Email")
		assert.Contains(t, cols, "Name")
	})

	t.Run("UpdateColumns", func(t *testing.T) {
		cols := table.UpdateColumns()
		assert.NotContains(t, cols, "ID")        // immutable
		assert.NotContains(t, cols, "CreatedAt")  // immutable
		assert.Contains(t, cols, "Email")
		assert.Contains(t, cols, "Name")
		assert.Contains(t, cols, "UpdatedAt")
	})
}

func TestColumnsFor(t *testing.T) {
	table := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull().Unique(),
			Col("Name", TypeString(255)),
		).
		WithTimestamps()

	pg := chuck.PostgresDialect{}
	sq := chuck.SQLiteDialect{}

	t.Run("SelectColumnsFor_postgres", func(t *testing.T) {
		cols := table.SelectColumnsFor(pg)
		assert.Equal(t, []string{"id", "email", "name", "created_at", "updated_at"}, cols)
	})

	t.Run("SelectColumnsFor_sqlite", func(t *testing.T) {
		cols := table.SelectColumnsFor(sq)
		assert.Equal(t, []string{"ID", "Email", "Name", "CreatedAt", "UpdatedAt"}, cols)
	})

	t.Run("InsertColumnsFor_postgres", func(t *testing.T) {
		cols := table.InsertColumnsFor(pg)
		assert.NotContains(t, cols, "id")
		assert.Contains(t, cols, "email")
	})

	t.Run("UpdateColumnsFor_postgres", func(t *testing.T) {
		cols := table.UpdateColumnsFor(pg)
		assert.NotContains(t, cols, "id")
		assert.NotContains(t, cols, "created_at")
		assert.Contains(t, cols, "email")
		assert.Contains(t, cols, "updated_at")
	})
}

func TestTableTraits(t *testing.T) {
	table := NewTable("Tasks").
		Columns(
			AutoIncrCol("ID"),
			Col("Title", TypeString(255)).NotNull(),
		).
		WithUUID().
		WithStatus("draft").
		WithSortOrder().
		WithParent().
		WithNotes().
		WithExpiry().
		WithVersion().
		WithArchive().
		WithReplacement().
		WithTimestamps().
		WithSoftDelete().
		WithAuditTrail()

	cols := table.SelectColumns()
	assert.Contains(t, cols, "UUID")
	assert.Contains(t, cols, "Status")
	assert.Contains(t, cols, "SortOrder")
	assert.Contains(t, cols, "ParentID")
	assert.Contains(t, cols, "Notes")
	assert.Contains(t, cols, "ExpiresAt")
	assert.Contains(t, cols, "Version")
	assert.Contains(t, cols, "ArchivedAt")
	assert.Contains(t, cols, "ReplacedByID")
	assert.Contains(t, cols, "CreatedAt")
	assert.Contains(t, cols, "UpdatedAt")
	assert.Contains(t, cols, "DeletedAt")
	assert.Contains(t, cols, "CreatedBy")
	assert.Contains(t, cols, "UpdatedBy")
	assert.Contains(t, cols, "DeletedBy")

	assert.True(t, table.HasSoftDelete())
	assert.True(t, table.HasVersion())
	assert.True(t, table.HasExpiry())
	assert.True(t, table.HasArchive())

	// UUID and CreatedAt should be immutable
	updateCols := table.UpdateColumns()
	assert.NotContains(t, updateCols, "UUID")
	assert.NotContains(t, updateCols, "CreatedAt")
	assert.NotContains(t, updateCols, "CreatedBy")
}

func TestCreateIfNotExistsSQL(t *testing.T) {
	table := NewTable("Users").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
		).
		Indexes(
			Index("idx_users_name", "Name"),
		)

	dialects := []chuck.Dialect{
		chuck.PostgresDialect{},
		chuck.SQLiteDialect{},
		chuck.MSSQLDialect{},
	}

	for _, d := range dialects {
		t.Run(string(d.Engine()), func(t *testing.T) {
			stmts := table.CreateIfNotExistsSQL(d)
			require.GreaterOrEqual(t, len(stmts), 2) // CREATE TABLE + CREATE INDEX

			// CREATE TABLE statement — Postgres normalizes to snake_case
			if d.Engine() == chuck.Postgres {
				assert.Contains(t, stmts[0], "users")
				assert.Contains(t, stmts[0], "name")
			} else {
				assert.Contains(t, stmts[0], "Users")
				assert.Contains(t, stmts[0], "Name")
			}

			// CREATE INDEX statement
			assert.Contains(t, stmts[1], "idx_users_name")
		})
	}
}

func TestDropSQL(t *testing.T) {
	table := NewTable("Users")
	d := chuck.SQLiteDialect{}
	assert.Contains(t, table.DropSQL(d), "DROP TABLE")
	assert.Contains(t, table.DropSQL(d), "Users")
}

func TestUniqueColumns(t *testing.T) {
	table := NewTable("Mapping").
		Columns(
			Col("LeftID", TypeInt()).NotNull(),
			Col("RightID", TypeInt()).NotNull(),
		).
		UniqueColumns("LeftID", "RightID")

	d := chuck.SQLiteDialect{}
	stmts := table.CreateIfNotExistsSQL(d)
	assert.Contains(t, stmts[0], `UNIQUE ("LeftID", "RightID")`)
}

func TestSeedData(t *testing.T) {
	table := NewTable("Statuses").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeVarchar(50)).NotNull(),
			Col("Label", TypeVarchar(100)).NotNull(),
		).
		WithSeedRows(
			SeedRow{"Name": "'active'", "Label": "'Active'"},
			SeedRow{"Name": "'draft'", "Label": "'Draft'"},
		)

	assert.True(t, table.HasSeedData())
	assert.Len(t, table.SeedRows(), 2)

	t.Run("sqlite", func(t *testing.T) {
		stmts := table.SeedSQL(chuck.SQLiteDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], `INSERT OR IGNORE INTO "Statuses"`)
		assert.Contains(t, stmts[0], "'active'")
	})

	t.Run("postgres", func(t *testing.T) {
		stmts := table.SeedSQL(chuck.PostgresDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], `INSERT INTO "statuses"`)
		assert.Contains(t, stmts[0], "ON CONFLICT DO NOTHING")
		assert.Contains(t, stmts[0], "'active'")
	})

	t.Run("mssql", func(t *testing.T) {
		stmts := table.SeedSQL(chuck.MSSQLDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], "INSERT INTO [Statuses]")
		assert.Contains(t, stmts[0], "BEGIN TRY")
		assert.Contains(t, stmts[0], "'active'")
	})
}

func TestColumnDDL(t *testing.T) {
	d := chuck.PostgresDialect{}

	t.Run("basic", func(t *testing.T) {
		c := Col("Name", TypeVarchar(255))
		ddl := c.ddl(d)
		assert.Equal(t, `"name" VARCHAR(255)`, ddl)
	})

	t.Run("not_null_unique", func(t *testing.T) {
		c := Col("Email", TypeVarchar(255)).NotNull().Unique()
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "NOT NULL")
		assert.Contains(t, ddl, "UNIQUE")
	})

	t.Run("default_literal", func(t *testing.T) {
		c := Col("Status", TypeVarchar(50)).Default("'active'")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "DEFAULT 'active'")
	})

	t.Run("default_fn", func(t *testing.T) {
		c := Col("CreatedAt", TypeTimestamp()).DefaultFn(func(d chuck.Dialect) string { return d.Now() })
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "DEFAULT NOW()")
	})

	t.Run("references", func(t *testing.T) {
		c := Col("UserID", TypeInt()).References("Users", "ID")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, `REFERENCES "users"("id")`)
	})

	t.Run("references_on_delete", func(t *testing.T) {
		c := Col("TaskID", TypeInt()).NotNull().References("Tasks", "ID").OnDelete("CASCADE")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, `REFERENCES "tasks"("id") ON DELETE CASCADE`)
	})

	t.Run("references_on_update", func(t *testing.T) {
		c := Col("TaskID", TypeInt()).References("Tasks", "ID").OnUpdate("SET NULL")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, `REFERENCES "tasks"("id") ON UPDATE SET NULL`)
	})

	t.Run("references_on_delete_and_update", func(t *testing.T) {
		c := Col("TaskID", TypeInt()).References("Tasks", "ID").OnDelete("CASCADE").OnUpdate("SET NULL")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, `REFERENCES "tasks"("id") ON DELETE CASCADE ON UPDATE SET NULL`)
	})

	t.Run("auto_increment", func(t *testing.T) {
		c := AutoIncrCol("ID")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "id")
		assert.Contains(t, ddl, "SERIAL PRIMARY KEY")
	})

	t.Run("uuid_pk_postgres", func(t *testing.T) {
		c := UUIDPKCol("ID")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "UUID PRIMARY KEY DEFAULT gen_random_uuid()")
	})

	t.Run("uuid_pk_sqlite", func(t *testing.T) {
		c := UUIDPKCol("ID")
		sq := chuck.SQLiteDialect{}
		ddl := c.ddl(sq)
		assert.Contains(t, ddl, "TEXT PRIMARY KEY")
		assert.NotContains(t, ddl, "gen_random_uuid")
	})

	t.Run("uuid_pk_mssql", func(t *testing.T) {
		c := UUIDPKCol("ID")
		ms := chuck.MSSQLDialect{}
		ddl := c.ddl(ms)
		assert.Contains(t, ddl, "UNIQUEIDENTIFIER PRIMARY KEY")
		assert.NotContains(t, ddl, "gen_random_uuid")
	})

	t.Run("uuid_pk_immutable", func(t *testing.T) {
		c := UUIDPKCol("ID")
		assert.True(t, c.pk)
		assert.False(t, c.mutable)
	})

	t.Run("references_on_delete_sqlite", func(t *testing.T) {
		c := Col("TaskID", TypeInt()).NotNull().References("Tasks", "ID").OnDelete("CASCADE")
		sq := chuck.SQLiteDialect{}
		ddl := c.ddl(sq)
		assert.Contains(t, ddl, `REFERENCES "Tasks"("ID") ON DELETE CASCADE`)
	})

	t.Run("references_on_delete_mssql", func(t *testing.T) {
		c := Col("TaskID", TypeInt()).NotNull().References("Tasks", "ID").OnDelete("CASCADE").OnUpdate("SET NULL")
		ms := chuck.MSSQLDialect{}
		ddl := c.ddl(ms)
		assert.Contains(t, ddl, `REFERENCES [Tasks]([ID]) ON DELETE CASCADE ON UPDATE SET NULL`)
	})
}

func TestTableFactories(t *testing.T) {
	d := chuck.SQLiteDialect{}

	t.Run("MappingTable", func(t *testing.T) {
		table := NewMappingTable("UserRoles", "UserID", "RoleID")
		stmts := table.CreateIfNotExistsSQL(d)
		assert.Contains(t, stmts[0], `UNIQUE ("UserID", "RoleID")`)
		cols := table.SelectColumns()
		assert.Contains(t, cols, "UserID")
		assert.Contains(t, cols, "RoleID")
	})

	t.Run("ConfigTable", func(t *testing.T) {
		table := NewConfigTable("Settings", "Key", "Value")
		cols := table.SelectColumns()
		assert.Contains(t, cols, "ID")
		assert.Contains(t, cols, "Key")
		assert.Contains(t, cols, "Value")
	})

	t.Run("LookupTable", func(t *testing.T) {
		table := NewLookupTable("Options", "Category", "Label")
		cols := table.SelectColumns()
		assert.Contains(t, cols, "ID")
		assert.Contains(t, cols, "Category")
		assert.Contains(t, cols, "Label")
	})

	t.Run("LookupJoinTable", func(t *testing.T) {
		table := NewLookupJoinTable("TaskOptions")
		cols := table.SelectColumns()
		assert.Contains(t, cols, "OwnerID")
		assert.Contains(t, cols, "LookupID")
	})

	t.Run("EventTable", func(t *testing.T) {
		table := NewEventTable("AuditLog",
			Col("Action", TypeVarchar(50)).NotNull(),
			Col("Payload", TypeText()),
		)
		cols := table.SelectColumns()
		assert.Contains(t, cols, "ID")
		assert.Contains(t, cols, "Action")
		assert.Contains(t, cols, "Payload")
		assert.Contains(t, cols, "CreatedAt")
		// All columns should be immutable
		assert.Empty(t, table.UpdateColumns())
	})

	t.Run("QueueTable", func(t *testing.T) {
		table := NewQueueTable("Jobs", "Data")
		cols := table.SelectColumns()
		assert.Contains(t, cols, "ID")
		assert.Contains(t, cols, "Data")
		assert.Contains(t, cols, "Status")
		assert.Contains(t, cols, "RetryCount")
		assert.Contains(t, cols, "ScheduledAt")
		assert.Contains(t, cols, "ProcessedAt")
		assert.Contains(t, cols, "CreatedAt")
		stmts := table.CreateIfNotExistsSQL(d)
		// Should have CREATE TABLE + 3 indexes
		assert.Len(t, stmts, 4)
		joined := strings.Join(stmts, "\n")
		assert.Contains(t, joined, "idx_jobs_status")
		assert.Contains(t, joined, "idx_jobs_scheduledat")
	})
}

func TestColumnDefMethods(t *testing.T) {
	t.Run("PrimaryKey", func(t *testing.T) {
		c := Col("ID", TypeInt())
		assert.False(t, c.pk)
		c2 := c.PrimaryKey()
		assert.True(t, c2.pk)
		// original unchanged
		assert.False(t, c.pk)
	})

	t.Run("Mutable_toggle", func(t *testing.T) {
		// Col creates mutable by default
		c := Col("Name", TypeVarchar(255))
		assert.True(t, c.mutable)

		// Immutable turns it off
		c2 := c.Immutable()
		assert.False(t, c2.mutable)

		// Mutable() turns it back on
		c3 := c2.Mutable()
		assert.True(t, c3.mutable)

		// AutoIncrCol is immutable by default
		autoC := AutoIncrCol("ID")
		assert.False(t, autoC.mutable)

		// Mutable() makes it mutable
		autoC2 := autoC.Mutable()
		assert.True(t, autoC2.mutable)
	})

	t.Run("Name", func(t *testing.T) {
		c := Col("Email", TypeVarchar(255))
		assert.Equal(t, "Email", c.Name())

		c2 := AutoIncrCol("UserID")
		assert.Equal(t, "UserID", c2.Name())

		c3 := UUIDPKCol("RowID")
		assert.Equal(t, "RowID", c3.Name())
	})
}

func TestCreateSQL(t *testing.T) {
	t.Run("basic_sqlite", func(t *testing.T) {
		table := NewTable("Notes").
			Columns(
				AutoIncrCol("ID"),
				Col("Body", TypeText()).NotNull(),
			)
		d := chuck.SQLiteDialect{}
		stmts := table.CreateSQL(d)
		require.Len(t, stmts, 1) // no indexes
		assert.Contains(t, stmts[0], "CREATE TABLE")
		assert.Contains(t, stmts[0], `"Notes"`)
		assert.Contains(t, stmts[0], `"ID"`)
		assert.Contains(t, stmts[0], `"Body"`)
		assert.Contains(t, stmts[0], "NOT NULL")
	})

	t.Run("with_indexes", func(t *testing.T) {
		table := NewTable("Articles").
			Columns(
				AutoIncrCol("ID"),
				Col("Title", TypeVarchar(255)).NotNull(),
			).
			Indexes(
				Index("idx_articles_title", "Title"),
			)
		d := chuck.SQLiteDialect{}
		stmts := table.CreateSQL(d)
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], "CREATE TABLE")
		assert.Contains(t, stmts[1], "idx_articles_title")
	})

	t.Run("postgres_normalizes_names", func(t *testing.T) {
		table := NewTable("MyTable").
			Columns(
				AutoIncrCol("ID"),
				Col("MyColumn", TypeInt()).NotNull(),
			)
		d := chuck.PostgresDialect{}
		stmts := table.CreateSQL(d)
		require.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "my_table")
		assert.Contains(t, stmts[0], "my_column")
	})

	t.Run("mssql", func(t *testing.T) {
		table := NewTable("Docs").
			Columns(
				AutoIncrCol("ID"),
				Col("Content", TypeText()),
			)
		d := chuck.MSSQLDialect{}
		stmts := table.CreateSQL(d)
		require.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "CREATE TABLE")
		assert.Contains(t, stmts[0], "[Docs]")
	})
}

func TestTypeFuncs(t *testing.T) {
	d := chuck.PostgresDialect{}

	assert.Equal(t, "TEXT", TypeString(255)(d))
	assert.Equal(t, "VARCHAR(255)", TypeVarchar(255)(d))
	assert.Equal(t, "TIMESTAMPTZ", TypeTimestamp()(d))
	assert.Equal(t, "INTEGER", TypeInt()(d))
	assert.Equal(t, "BIGINT", TypeBigInt()(d))
	assert.Equal(t, "DOUBLE PRECISION", TypeFloat()(d))
	assert.Equal(t, "NUMERIC(10,2)", TypeDecimal(10, 2)(d))
	assert.Equal(t, "TEXT", TypeText()(d))
	assert.Equal(t, "BOOLEAN", TypeBool()(d))
	assert.Equal(t, "UUID", TypeUUID()(d))
	assert.Equal(t, "JSONB", TypeJSON()(d))
	assert.Equal(t, "CUSTOM_TYPE", TypeLiteral("CUSTOM_TYPE")(d))
}
