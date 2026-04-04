package schema

import (
	"encoding/json"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	table := NewTable("Tasks").
		Columns(
			AutoIncrCol("ID"),
			Col("Title", TypeString(255)).NotNull(),
			Col("UserID", TypeInt()).NotNull().References("Users", "ID"),
		).
		WithTimestamps().
		WithSoftDelete().
		WithVersion().
		Indexes(
			Index("idx_tasks_title", "Title"),
		)

	t.Run("struct_fields", func(t *testing.T) {
		snap := table.Snapshot(chuck.PostgresDialect{})

		assert.Equal(t, "tasks", snap.Name)
		assert.True(t, snap.HasSoftDelete)
		assert.True(t, snap.HasVersion)
		require.Len(t, snap.Columns, 7) // ID, Title, UserID, CreatedAt, UpdatedAt, DeletedAt, Version

		id := snap.Columns[0]
		assert.Equal(t, "id", id.Name)
		assert.True(t, id.PrimaryKey)
		assert.True(t, id.AutoIncr)
		assert.False(t, id.Mutable)

		userID := snap.Columns[2]
		assert.Equal(t, "user_id", userID.Name)
		assert.Equal(t, "users", userID.RefTable)
		assert.Equal(t, "id", userID.RefColumn)
		assert.True(t, userID.NotNull)

		createdAt := snap.Columns[3]
		assert.Equal(t, "created_at", createdAt.Name)
		assert.Equal(t, "NOW()", createdAt.Default)
		assert.False(t, createdAt.Mutable)

		require.Len(t, snap.Indexes, 1)
		assert.Equal(t, "idx_tasks_title", snap.Indexes[0].Name)
	})

	t.Run("json_serializable", func(t *testing.T) {
		snap := table.Snapshot(chuck.SQLiteDialect{})
		data, err := json.MarshalIndent(snap, "", "  ")
		require.NoError(t, err)
		assert.Contains(t, string(data), `"name": "Tasks"`)
		assert.Contains(t, string(data), `"has_soft_delete": true`)
	})

	t.Run("dialect_aware_types", func(t *testing.T) {
		pgSnap := table.Snapshot(chuck.PostgresDialect{})
		msSnap := table.Snapshot(chuck.MSSQLDialect{})

		// Title column type differs per dialect
		assert.Equal(t, "TEXT", pgSnap.Columns[1].Type)
		assert.Equal(t, "NVARCHAR(255)", msSnap.Columns[1].Type)

		// Timestamp default differs
		assert.Equal(t, "NOW()", pgSnap.Columns[3].Default)
		assert.Equal(t, "GETDATE()", msSnap.Columns[3].Default)
	})
}

func TestSnapshotString(t *testing.T) {
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

	s := table.SnapshotString(chuck.PostgresDialect{})

	assert.Contains(t, s, "TABLE users")
	assert.Contains(t, s, "id")
	assert.Contains(t, s, "PRIMARY KEY")
	assert.Contains(t, s, "email")
	assert.Contains(t, s, "NOT NULL")
	assert.Contains(t, s, "UNIQUE")
	assert.Contains(t, s, "INDEX idx_users_email ON (Email)")
	assert.Contains(t, s, "[immutable]")
}

func TestSnapshotStringMultiTable(t *testing.T) {
	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)))

	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)).NotNull())

	s := SchemaSnapshotString(chuck.SQLiteDialect{}, users, tasks)
	assert.Contains(t, s, "TABLE Users")
	assert.Contains(t, s, "TABLE Tasks")
}

func TestSnapshotUniqueConstraints(t *testing.T) {
	table := NewMappingTable("UserRoles", "UserID", "RoleID")
	snap := table.Snapshot(chuck.PostgresDialect{})

	require.Len(t, snap.UniqueConstraints, 1)
	assert.Equal(t, []string{"user_id", "role_id"}, snap.UniqueConstraints[0])
}

func TestSnapshotOnDeleteOnUpdate(t *testing.T) {
	table := NewTable("TaskReqs").
		Columns(
			AutoIncrCol("ID"),
			Col("TaskID", TypeInt()).NotNull().References("Tasks", "ID").OnDelete("CASCADE"),
			Col("ReqID", TypeInt()).NotNull().References("Requirements", "ID").OnDelete("SET NULL").OnUpdate("CASCADE"),
		)

	snap := table.Snapshot(chuck.PostgresDialect{})
	require.Len(t, snap.Columns, 3)

	taskID := snap.Columns[1]
	assert.Equal(t, "task_id", taskID.Name)
	assert.Equal(t, "CASCADE", taskID.OnDelete)
	assert.Empty(t, taskID.OnUpdate)

	reqID := snap.Columns[2]
	assert.Equal(t, "req_id", reqID.Name)
	assert.Equal(t, "SET NULL", reqID.OnDelete)
	assert.Equal(t, "CASCADE", reqID.OnUpdate)
}

func TestSnapshotCheck(t *testing.T) {
	table := NewTable("Products").
		Columns(
			AutoIncrCol("ID"),
			Col("Price", TypeDecimal(10, 2)).NotNull().Check("Price > 0"),
			Col("Status", TypeVarchar(20)).Check("Status IN ('active', 'draft')"),
		)

	t.Run("struct_fields", func(t *testing.T) {
		snap := table.Snapshot(chuck.PostgresDialect{})

		price := snap.Columns[1]
		assert.Equal(t, "price", price.Name)
		assert.Equal(t, "Price > 0", price.Check)

		status := snap.Columns[2]
		assert.Equal(t, "status", status.Name)
		assert.Equal(t, "Status IN ('active', 'draft')", status.Check)
	})

	t.Run("json_serializable", func(t *testing.T) {
		snap := table.Snapshot(chuck.SQLiteDialect{})
		data, err := json.MarshalIndent(snap, "", "  ")
		require.NoError(t, err)
		assert.Contains(t, string(data), `"check":`)
		// Verify the check field round-trips through JSON
		var decoded TableSnapshot
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, "Price > 0", decoded.Columns[1].Check)
	})

	t.Run("snapshot_string", func(t *testing.T) {
		s := table.SnapshotString(chuck.PostgresDialect{})
		assert.Contains(t, s, "CHECK (Price > 0)")
		assert.Contains(t, s, "CHECK (Status IN ('active', 'draft'))")
	})
}

func TestSchemaSnapshot(t *testing.T) {
	users := NewTable("Users").
		Columns(AutoIncrCol("ID"))
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"))

	snaps := SchemaSnapshot(chuck.PostgresDialect{}, users, tasks)
	require.Len(t, snaps, 2)
	assert.Equal(t, "users", snaps[0].Name)
	assert.Equal(t, "tasks", snaps[1].Name)
}
