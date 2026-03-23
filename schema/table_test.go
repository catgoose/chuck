package schema

import (
	"strings"
	"testing"

	"github.com/catgoose/fraggle"
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

	dialects := []fraggle.Dialect{
		fraggle.PostgresDialect{},
		fraggle.SQLiteDialect{},
		fraggle.MSSQLDialect{},
	}

	for _, d := range dialects {
		t.Run(string(d.Engine()), func(t *testing.T) {
			stmts := table.CreateIfNotExistsSQL(d)
			require.GreaterOrEqual(t, len(stmts), 2) // CREATE TABLE + CREATE INDEX

			// CREATE TABLE statement
			assert.Contains(t, stmts[0], "Users")
			assert.Contains(t, stmts[0], "Name")

			// CREATE INDEX statement
			assert.Contains(t, stmts[1], "idx_users_name")
		})
	}
}

func TestDropSQL(t *testing.T) {
	table := NewTable("Users")
	d := fraggle.SQLiteDialect{}
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

	d := fraggle.SQLiteDialect{}
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
		stmts := table.SeedSQL(fraggle.SQLiteDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], `INSERT OR IGNORE INTO "Statuses"`)
		assert.Contains(t, stmts[0], "'active'")
	})

	t.Run("postgres", func(t *testing.T) {
		stmts := table.SeedSQL(fraggle.PostgresDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], `INSERT INTO "Statuses"`)
		assert.Contains(t, stmts[0], "ON CONFLICT DO NOTHING")
		assert.Contains(t, stmts[0], "'active'")
	})

	t.Run("mssql", func(t *testing.T) {
		stmts := table.SeedSQL(fraggle.MSSQLDialect{})
		require.Len(t, stmts, 2)
		assert.Contains(t, stmts[0], "INSERT INTO [Statuses]")
		assert.Contains(t, stmts[0], "BEGIN TRY")
		assert.Contains(t, stmts[0], "'active'")
	})
}

func TestColumnDDL(t *testing.T) {
	d := fraggle.PostgresDialect{}

	t.Run("basic", func(t *testing.T) {
		c := Col("Name", TypeVarchar(255))
		ddl := c.ddl(d)
		assert.Equal(t, `"Name" VARCHAR(255)`, ddl)
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
		c := Col("CreatedAt", TypeTimestamp()).DefaultFn(func(d fraggle.Dialect) string { return d.Now() })
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "DEFAULT NOW()")
	})

	t.Run("references", func(t *testing.T) {
		c := Col("UserID", TypeInt()).References("Users", "ID")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, `REFERENCES "Users"("ID")`)
	})

	t.Run("auto_increment", func(t *testing.T) {
		c := AutoIncrCol("ID")
		ddl := c.ddl(d)
		assert.Contains(t, ddl, "ID")
		assert.Contains(t, ddl, "SERIAL PRIMARY KEY")
	})
}

func TestTableFactories(t *testing.T) {
	d := fraggle.SQLiteDialect{}

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

func TestTypeFuncs(t *testing.T) {
	d := fraggle.PostgresDialect{}

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
