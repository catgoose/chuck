package schema_test

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
	"github.com/catgoose/chuck/schema"
)

func ExampleNewTable() {
	tasks := schema.NewTable("Tasks").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Title", schema.TypeString(255)).NotNull(),
			schema.Col("Description", schema.TypeText()),
			schema.Col("AssigneeID", schema.TypeInt()).References("Users", "ID").OnDelete("SET NULL"),
		).
		Indexes(
			schema.Index("idx_tasks_title", "Title"),
		)

	// Snapshot shows the resolved schema for any dialect
	d, _ := chuck.New(chuck.SQLite)
	fmt.Print(tasks.SnapshotString(d))
	// Output:
	// TABLE Tasks
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   Title                TEXT NOT NULL
	//   Description          TEXT
	//   AssigneeID           INTEGER REFERENCES Users(ID) ON DELETE SET NULL
	//   INDEX idx_tasks_title ON (Title)
}

func ExampleNewTable_traits() {
	projects := schema.NewTable("Projects").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Name", schema.TypeString(255)).NotNull(),
		).
		WithStatus("draft").
		WithVersion().
		WithSoftDelete().
		WithTimestamps().
		WithAuditTrail()

	// The table knows which columns are mutable vs immutable
	fmt.Println("Select:", strings.Join(projects.SelectColumns(), ", "))
	fmt.Println("Update:", strings.Join(projects.UpdateColumns(), ", "))
	fmt.Println("HasSoftDelete:", projects.HasSoftDelete())
	fmt.Println("HasVersion:", projects.HasVersion())

	// Postgres normalizes column names to snake_case
	pg, _ := chuck.New(chuck.Postgres)
	fmt.Println("Postgres select:", strings.Join(projects.SelectColumnsFor(pg), ", "))
	// Output:
	// Select: ID, Name, Status, Version, DeletedAt, CreatedAt, UpdatedAt, CreatedBy, UpdatedBy, DeletedBy
	// Update: Name, Status, Version, DeletedAt, UpdatedAt, UpdatedBy, DeletedBy
	// HasSoftDelete: true
	// HasVersion: true
	// Postgres select: id, name, status, version, deleted_at, created_at, updated_at, created_by, updated_by, deleted_by
}

func ExampleNewTable_foreignKeys() {
	pg, _ := chuck.New(chuck.Postgres)

	comments := schema.NewTable("Comments").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("TaskID", schema.TypeInt()).NotNull().
				References("Tasks", "ID").OnDelete("CASCADE"),
			schema.Col("AuthorID", schema.TypeInt()).
				References("Users", "ID").OnDelete("SET NULL").OnUpdate("CASCADE"),
			schema.Col("Body", schema.TypeText()).NotNull(),
		)

	fmt.Print(comments.SnapshotString(pg))
	// Output:
	// TABLE comments
	//   id                   SERIAL PRIMARY KEY PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   task_id              INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE
	//   author_id            INTEGER REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE
	//   body                 TEXT NOT NULL
}

func ExampleNewTable_uuidPrimaryKey() {
	pg, _ := chuck.New(chuck.Postgres)

	tokens := schema.NewTable("Tokens").
		Columns(
			schema.UUIDPKCol("ID"),
			schema.Col("Scope", schema.TypeVarchar(100)).NotNull(),
		).
		WithExpiry().
		WithTimestamps()

	snap := tokens.Snapshot(pg)
	fmt.Println("Table:", snap.Name)
	fmt.Println("PK type:", snap.Columns[0].Type)
	fmt.Println("HasExpiry:", snap.HasExpiry)
	// Output:
	// Table: tokens
	// PK type: UUID PRIMARY KEY DEFAULT gen_random_uuid()
	// HasExpiry: true
}

func ExampleNewTable_allTraits() {
	// Compose every trait on a single table to show the full column set
	d, _ := chuck.New(chuck.SQLite)

	table := schema.NewTable("FullDemo").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Title", schema.TypeString(255)).NotNull(),
		).
		WithTimestamps().
		WithSoftDelete().
		WithAuditTrail().
		WithVersion().
		WithStatus("draft").
		WithSortOrder().
		WithNotes().
		WithUUID().
		WithParent().
		WithReplacement().
		WithArchive().
		WithExpiry()

	fmt.Print(d.Engine(), " columns:\n")
	for _, col := range table.SelectColumns() {
		fmt.Println(" ", col)
	}
	// Output:
	// sqlite3 columns:
	//   ID
	//   Title
	//   CreatedAt
	//   UpdatedAt
	//   DeletedAt
	//   CreatedBy
	//   UpdatedBy
	//   DeletedBy
	//   Version
	//   Status
	//   SortOrder
	//   Notes
	//   UUID
	//   ParentID
	//   ReplacedByID
	//   ArchivedAt
	//   ExpiresAt
}

func ExampleNewLookupTable() {
	d, _ := chuck.New(chuck.SQLite)

	options := schema.NewLookupTable("Options", "Category", "Label")

	fmt.Println("Columns:", strings.Join(options.SelectColumns(), ", "))
	fmt.Print(options.SnapshotString(d))
	// Output:
	// Columns: ID, Category, Label
	// TABLE Options
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   Category             TEXT NOT NULL
	//   Label                TEXT NOT NULL
	//   INDEX idx_options_category ON (Category)
	//   INDEX idx_options_category_label ON (Category, Label)
}

func ExampleNewMappingTable() {
	d, _ := chuck.New(chuck.SQLite)

	userRoles := schema.NewMappingTable("UserRoles", "UserID", "RoleID")

	fmt.Println("Columns:", strings.Join(userRoles.SelectColumns(), ", "))
	fmt.Println("Insert:", strings.Join(userRoles.InsertColumns(), ", "))
	fmt.Print(userRoles.SnapshotString(d))
	// Output:
	// Columns: UserID, RoleID
	// Insert: UserID, RoleID
	// TABLE UserRoles
	//   UserID               INTEGER NOT NULL [immutable]
	//   RoleID               INTEGER NOT NULL [immutable]
	//   INDEX idx_userroles_userid ON (UserID)
	//   INDEX idx_userroles_roleid ON (RoleID)
	//   UNIQUE (UserID, RoleID)
}

func ExampleNewConfigTable() {
	d, _ := chuck.New(chuck.SQLite)

	settings := schema.NewConfigTable("Settings", "Key", "Value")

	fmt.Println("Columns:", strings.Join(settings.SelectColumns(), ", "))
	fmt.Print(settings.SnapshotString(d))
	// Output:
	// Columns: ID, Key, Value
	// TABLE Settings
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   Key                  TEXT NOT NULL UNIQUE
	//   Value                TEXT
	//   INDEX idx_settings_key ON (Key)
}

func ExampleNewEventTable() {
	d, _ := chuck.New(chuck.SQLite)

	auditLog := schema.NewEventTable("AuditLog",
		schema.Col("EventType", schema.TypeVarchar(50)).NotNull(),
		schema.Col("ActorID", schema.TypeInt()).NotNull(),
		schema.Col("Payload", schema.TypeJSON()),
	)

	fmt.Println("Columns:", strings.Join(auditLog.SelectColumns(), ", "))
	// All columns are immutable in an event table
	fmt.Println("Mutable count:", len(auditLog.UpdateColumns()))
	fmt.Print(auditLog.SnapshotString(d))
	// Output:
	// Columns: ID, EventType, ActorID, Payload, CreatedAt
	// Mutable count: 0
	// TABLE AuditLog
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   EventType            TEXT NOT NULL [immutable]
	//   ActorID              INTEGER NOT NULL [immutable]
	//   Payload              TEXT [immutable]
	//   CreatedAt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP [immutable]
	//   INDEX idx_auditlog_createdat ON (CreatedAt)
}

func ExampleNewQueueTable() {
	d, _ := chuck.New(chuck.SQLite)

	jobs := schema.NewQueueTable("Jobs", "Payload")

	fmt.Println("Columns:", strings.Join(jobs.SelectColumns(), ", "))
	fmt.Print(jobs.SnapshotString(d))
	// Output:
	// Columns: ID, Payload, Status, RetryCount, ScheduledAt, ProcessedAt, CreatedAt
	// TABLE Jobs
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   Payload              TEXT NOT NULL
	//   Status               TEXT NOT NULL DEFAULT 'pending'
	//   RetryCount           INTEGER NOT NULL DEFAULT 0
	//   ScheduledAt          TIMESTAMP
	//   ProcessedAt          TIMESTAMP
	//   CreatedAt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP [immutable]
	//   INDEX idx_jobs_status ON (Status)
	//   INDEX idx_jobs_scheduledat ON (ScheduledAt)
	//   INDEX idx_jobs_status_scheduledat ON (Status, ScheduledAt)
}

func ExampleNewLookupJoinTable() {
	d, _ := chuck.New(chuck.SQLite)

	taskOptions := schema.NewLookupJoinTable("TaskOptions")

	fmt.Println("Columns:", strings.Join(taskOptions.SelectColumns(), ", "))
	fmt.Print(taskOptions.SnapshotString(d))
	// Output:
	// Columns: OwnerID, LookupID
	// TABLE TaskOptions
	//   OwnerID              INTEGER NOT NULL [immutable]
	//   LookupID             INTEGER NOT NULL [immutable]
	//   INDEX idx_taskoptions_ownerid ON (OwnerID)
	//   INDEX idx_taskoptions_lookupid ON (LookupID)
}

func ExampleTableDef_WithSeedRows() {
	d, _ := chuck.New(chuck.SQLite)

	statuses := schema.NewTable("Statuses").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Name", schema.TypeVarchar(50)).NotNull().Unique(),
		).
		WithSeedRows(
			schema.SeedRow{"Name": "'active'"},
			schema.SeedRow{"Name": "'archived'"},
			schema.SeedRow{"Name": "'deleted'"},
		)

	fmt.Println("HasSeedData:", statuses.HasSeedData())
	fmt.Println("SeedRows:", len(statuses.SeedRows()))
	for _, stmt := range statuses.SeedSQL(d) {
		fmt.Println(stmt)
	}
	// Output:
	// HasSeedData: true
	// SeedRows: 3
	// INSERT OR IGNORE INTO "Statuses" (Name) VALUES ('active')
	// INSERT OR IGNORE INTO "Statuses" (Name) VALUES ('archived')
	// INSERT OR IGNORE INTO "Statuses" (Name) VALUES ('deleted')
}

func ExampleTableDef_WithSeedRows_postgres() {
	d, _ := chuck.New(chuck.Postgres)

	roles := schema.NewTable("Roles").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Name", schema.TypeVarchar(50)).NotNull().Unique(),
			schema.Col("Description", schema.TypeText()),
		).
		WithSeedRows(
			schema.SeedRow{"Name": "'admin'", "Description": "'Full access'"},
			schema.SeedRow{"Name": "'viewer'", "Description": "'Read-only access'"},
		)

	for _, stmt := range roles.SeedSQL(d) {
		fmt.Println(stmt)
	}
	// Output:
	// INSERT INTO "roles" (name, description) VALUES ('admin', 'Full access') ON CONFLICT DO NOTHING
	// INSERT INTO "roles" (name, description) VALUES ('viewer', 'Read-only access') ON CONFLICT DO NOTHING
}

func ExampleTableDef_Snapshot() {
	d, _ := chuck.New(chuck.Postgres)

	tasks := schema.NewTable("Tasks").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Title", schema.TypeString(255)).NotNull(),
		).
		WithSoftDelete().
		WithTimestamps()

	fmt.Print(tasks.SnapshotString(d))
	// Output:
	// TABLE tasks
	//   id                   SERIAL PRIMARY KEY PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   title                TEXT NOT NULL
	//   deleted_at           TIMESTAMPTZ
	//   created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW() [immutable]
	//   updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
}

func ExampleTableDef_columnLists() {
	d, _ := chuck.New(chuck.Postgres)

	tasks := schema.NewTable("Tasks").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Title", schema.TypeString(255)).NotNull(),
			schema.Col("Done", schema.TypeBool()).NotNull().Default("false"),
		).
		WithTimestamps()

	fmt.Println("Select:", strings.Join(tasks.SelectColumnsFor(d), ", "))
	fmt.Println("Insert:", strings.Join(tasks.InsertColumnsFor(d), ", "))
	fmt.Println("Update:", strings.Join(tasks.UpdateColumnsFor(d), ", "))
	// Output:
	// Select: id, title, done, created_at, updated_at
	// Insert: title, done, created_at, updated_at
	// Update: title, done, updated_at
}

func ExampleSchemaSnapshotString() {
	d, _ := chuck.New(chuck.SQLite)

	users := schema.NewTable("Users").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Email", schema.TypeString(255)).NotNull().Unique(),
		)

	tasks := schema.NewTable("Tasks").
		Columns(
			schema.AutoIncrCol("ID"),
			schema.Col("Title", schema.TypeString(255)).NotNull(),
			schema.Col("UserID", schema.TypeInt()).References("Users", "ID"),
		)

	fmt.Print(schema.SchemaSnapshotString(d, users, tasks))
	// Output:
	// TABLE Users
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   Email                TEXT NOT NULL UNIQUE
	//
	// TABLE Tasks
	//   ID                   INTEGER PRIMARY KEY AUTOINCREMENT PRIMARY KEY AUTO INCREMENT NOT NULL [immutable]
	//   Title                TEXT NOT NULL
	//   UserID               INTEGER REFERENCES Users(ID)
}
