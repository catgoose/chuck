# Fraggle

<img src="https://raw.githubusercontent.com/catgoose/screenshots/main/fraggle/fraggle.jpeg" alt="Fraggle Rock" width="400">

*"Down at Fraggle Rock!"*

Fraggle is a multi-dialect SQL fragment system for Go. Like the Fraggles exploring different caves in the Rock, Fraggle lets your queries travel between SQLite, PostgreSQL, and MSSQL without getting lost.

No ORM, no query builder magic — just explicit SQL fragments, composable schema definitions, and domain patterns as primitives.

## Install

```bash
go get github.com/catgoose/fraggle
```

Import only the database drivers you need:

```go
import _ "github.com/catgoose/fraggle/driver/sqlite"
import _ "github.com/catgoose/fraggle/driver/postgres"
import _ "github.com/catgoose/fraggle/driver/mssql"
```

## Dialect Interface

Every engine implements the same interface. You get raw SQL strings that you compose into full queries — the generated SQL is predictable because it's just string concatenation with guard rails.

```go
d, _ := fraggle.New(fraggle.Postgres)

d.AutoIncrement()  // "SERIAL PRIMARY KEY"
d.TimestampType()  // "TIMESTAMPTZ"
d.Pagination()     // "LIMIT @Limit OFFSET @Offset"
d.Now()            // "NOW()"
d.Placeholder(1)   // "$1"
```

Each engine speaks its own dialect:

| Method | PostgreSQL | SQLite | MSSQL |
|--------|-----------|--------|-------|
| `AutoIncrement()` | `SERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` | `INT PRIMARY KEY IDENTITY(1,1)` |
| `TimestampType()` | `TIMESTAMPTZ` | `TIMESTAMP` | `DATETIME` |
| `Now()` | `NOW()` | `CURRENT_TIMESTAMP` | `GETDATE()` |
| `Placeholder(1)` | `$1` | `?` | `@p1` |
| `Pagination()` | `LIMIT @Limit OFFSET @Offset` | `LIMIT @Limit OFFSET @Offset` | `OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY` |
| `BoolType()` | `BOOLEAN` | `INTEGER` | `BIT` |
| `QuoteIdentifier("t")` | `"t"` | `"t"` | `[t]` |

### Column Type Methods

| Method | Purpose |
|--------|---------|
| `StringType(n)` | Engine's preferred string type — `NVARCHAR(n)` on MSSQL (Unicode), `TEXT` on Postgres/SQLite |
| `VarcharType(n)` | Exact `VARCHAR(n)` — use when you need explicit length or non-Unicode on MSSQL |
| `IntType()` | `INTEGER` / `INT` |
| `BigIntType()` | `BIGINT` / `INTEGER` (SQLite) |
| `FloatType()` | `DOUBLE PRECISION` / `REAL` / `FLOAT` |
| `DecimalType(p,s)` | `NUMERIC(p,s)` / `DECIMAL(p,s)` / `REAL` (SQLite) |
| `TextType()` | Unlimited text — `TEXT` / `NVARCHAR(MAX)` |
| `BoolType()` | `BOOLEAN` / `INTEGER` / `BIT` |
| `UUIDType()` | `UUID` / `TEXT` / `UNIQUEIDENTIFIER` |
| `JSONType()` | `JSONB` / `TEXT` / `NVARCHAR(MAX)` |

### DDL Methods

All DDL methods quote identifiers automatically using the engine's quoting style.

```go
d.CreateTableIfNotExists("users", body)
d.DropTableIfExists("users")
d.CreateIndexIfNotExists("idx_users_email", "users", "email")
d.InsertOrIgnore("users", "name, email", "'Alice', 'alice@test.com'")
```

`InsertOrIgnore` produces idempotent inserts: `INSERT OR IGNORE` (SQLite), `ON CONFLICT DO NOTHING` (Postgres), or `BEGIN TRY...END CATCH` (MSSQL).

## Opening Connections

```go
import _ "github.com/catgoose/fraggle/driver/postgres"

db, dialect, _ := fraggle.OpenURL(ctx, "postgres://user:pass@localhost:5432/myapp?sslmode=disable")
db, dialect, _ := fraggle.OpenURL(ctx, "sqlite://:memory:")
db, dialect, _ := fraggle.OpenURL(ctx, "sqlserver://user:pass@host:1433?database=erp")
```

`OpenURL` detects the engine from the URL scheme and returns a raw `*sql.DB` plus the matching `Dialect`. Supported schemes: `postgres://` (`postgresql://`), `sqlite://` (`sqlite3://`), `sqlserver://` (`mssql://`).

For SQLite, `OpenSQLite` opens a database with sensible defaults (WAL mode, 30s busy timeout, single-connection pool):

```go
import _ "github.com/catgoose/fraggle/driver/sqlite"

db, dialect, _ := fraggle.OpenSQLite(ctx, "path/to/app.db")
```

## Schema as Code

The `schema` package defines tables in Go. One declaration drives DDL generation, column lists, seed data, and schema snapshots.

```go
import "github.com/catgoose/fraggle/schema"

var TasksTable = schema.NewTable("Tasks").
    Columns(
        schema.AutoIncrCol("ID"),
        schema.Col("Title", schema.TypeString(255)).NotNull(),
        schema.Col("Description", schema.TypeText()),
        schema.Col("AssigneeID", schema.TypeInt()).References("Users", "ID"),
    ).
    WithStatus("draft").
    WithVersion().
    WithSoftDelete().
    WithTimestamps().
    Indexes(
        schema.Index("idx_tasks_title", "Title"),
    )

// Generate DDL for any dialect
stmts := TasksTable.CreateIfNotExistsSQL(dialect)
for _, stmt := range stmts {
    db.Exec(stmt)
}
```

### Traits

Traits add columns and behavior in one call. They're composable — use as many or as few as you need:

| Trait | Columns Added | Purpose |
|-------|--------------|---------|
| `WithTimestamps()` | `CreatedAt` (immutable), `UpdatedAt` | Creation and modification tracking |
| `WithSoftDelete()` | `DeletedAt` | Soft delete (nullable timestamp) |
| `WithAuditTrail()` | `CreatedBy`, `UpdatedBy`, `DeletedBy` | User attribution |
| `WithVersion()` | `Version` (default 1) | Optimistic concurrency control |
| `WithStatus(default)` | `Status` | Workflow state |
| `WithSortOrder()` | `SortOrder` | Manual ordering |
| `WithNotes()` | `Notes` | Nullable text field |
| `WithUUID()` | `UUID` (immutable, unique) | External identifier |
| `WithParent()` | `ParentID` | Tree/hierarchy structures |
| `WithReplacement()` | `ReplacedByID` | Entity lineage tracking |
| `WithArchive()` | `ArchivedAt` | Archival timestamp |
| `WithExpiry()` | `ExpiresAt` | Expiration timestamp |

Traits use PascalCase column names by default. For custom naming, use `Col()` directly:

```go
schema.Col("deleted_at", schema.TypeTimestamp()) // snake_case
```

### Table Factories

Common table patterns have factory functions:

```go
schema.NewMappingTable("UserRoles", "UserID", "RoleID")     // Many-to-many join table
schema.NewConfigTable("Settings", "Key", "Value")            // Key-value config
schema.NewLookupTable("Options", "Category", "Label")        // Lookup with grouping
schema.NewEventTable("AuditLog", cols...)                     // Append-only (all immutable)
schema.NewQueueTable("Jobs", "Payload")                       // Job queue with scheduling
```

### Column Lists

`TableDef` knows which columns to use in each context:

```go
TasksTable.SelectColumns()  // All columns
TasksTable.InsertColumns()  // Excludes auto-increment
TasksTable.UpdateColumns()  // Only mutable columns
```

### Seed Data

Declare initial rows as part of the schema. Seed is idempotent via the dialect's `InsertOrIgnore`:

```go
var StatusTable = schema.NewTable("Statuses").
    Columns(
        schema.AutoIncrCol("ID"),
        schema.Col("Name", schema.TypeVarchar(50)).NotNull().Unique(),
    ).
    WithSeedRows(
        schema.SeedRow{"Name": "'active'"},
        schema.SeedRow{"Name": "'archived'"},
    )

for _, stmt := range StatusTable.SeedSQL(dialect) {
    db.Exec(stmt)
}
```

### Schema Snapshots

Export the declared schema in structured or text format for diffing:

```go
// Structured (JSON-serializable) — for CI or programmatic comparison
snap := TasksTable.Snapshot(dialect)
data, _ := json.MarshalIndent(snap, "", "  ")

// Human-readable text — for side-by-side diffing
fmt.Println(TasksTable.SnapshotString(dialect))
// TABLE Tasks
//   ID                   SERIAL PRIMARY KEY AUTO INCREMENT [immutable]
//   Title                TEXT NOT NULL
//   Description          TEXT
//   ...

// Multi-table snapshot
fmt.Println(schema.SchemaSnapshotString(dialect, UsersTable, TasksTable, StatusTable))
```

### Live Schema Snapshots

Query a live database to get its actual schema, then compare against your declared schema:

```go
// Read what the database actually has
live, err := schema.LiveSnapshot(ctx, db, dialect, "Tasks")

// Read what your code declares
declared := TasksTable.Snapshot(dialect)

// Compare — column names, types, nullability
for i, dc := range declared.Columns {
    if dc.Name != live.Columns[i].Name {
        log.Printf("column mismatch at position %d: declared %s, live %s", i, dc.Name, live.Columns[i].Name)
    }
}

// Or compare the text representations side by side
fmt.Println("=== Declared ===")
fmt.Println(TasksTable.SnapshotString(dialect))
fmt.Println("=== Live ===")
fmt.Println(live.String())
```

`LiveSnapshot` returns column names, types, nullability, defaults, and indexes. Use it in CI to catch schema drift:

```go
func TestSchemaDrift(t *testing.T) {
    live, err := schema.LiveSnapshot(ctx, db, dialect, "Tasks")
    require.NoError(t, err)
    declared := TasksTable.Snapshot(dialect)

    require.Equal(t, len(declared.Columns), len(live.Columns), "column count mismatch")
    for i, dc := range declared.Columns {
        assert.Equal(t, dc.Name, live.Columns[i].Name, "column name mismatch at position %d", i)
        assert.Equal(t, dc.NotNull, !live.Columns[i].Nullable, "nullability mismatch for %s", dc.Name)
    }
}
```

Multi-table variant:

```go
snaps, err := schema.LiveSchemaSnapshot(ctx, db, dialect, "Users", "Tasks", "Statuses")
```

## Composable SQL Fragments (`dbrepo`)

The `dbrepo` package provides composable helpers that keep SQL visible. Functions use `@Name` placeholders with `sql.Named()` for dialect-agnostic parameter binding.

### Building Queries

```go
import "github.com/catgoose/fraggle/dbrepo"

dbrepo.Columns("ID", "Name", "Email")               // "ID, Name, Email"
dbrepo.Placeholders("ID", "Name", "Email")           // "@ID, @Name, @Email"
dbrepo.SetClause("Name", "Email")                    // "Name = @Name, Email = @Email"
dbrepo.InsertInto("Users", "Name", "Email")          // "INSERT INTO Users (Name, Email) VALUES (@Name, @Email)"
```

Dialect-aware variants quote identifiers:

```go
dbrepo.ColumnsQ(d, "ID", "Name")                     // `"ID", "Name"` (Postgres)
dbrepo.SetClauseQ(d, "Name", "Email")                // `"Name" = @Name, "Email" = @Email`
dbrepo.InsertIntoQ(d, "Users", "Name", "Email")      // `INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email)`
```

### WhereBuilder

Compose WHERE clauses with named parameters:

```go
w := dbrepo.NewWhere().
    And("DepartmentID = @DeptID", sql.Named("DeptID", 5)).
    AndIf(searchTerm != "", "Name LIKE @Pattern", sql.Named("Pattern", "%"+searchTerm+"%"))

query := "SELECT * FROM Users " + w.String()
// "SELECT * FROM Users WHERE DepartmentID = @DeptID AND Name LIKE @Pattern"
```

Semantic filter methods encode domain patterns. Each accepts an optional column name override for custom naming:

```go
w := dbrepo.NewWhere().
    NotDeleted().                  // DeletedAt IS NULL
    NotArchived().                 // ArchivedAt IS NULL
    NotExpired().                  // ExpiresAt IS NULL OR ExpiresAt > CURRENT_TIMESTAMP
    HasStatus("active").           // Status = @Status
    HasVersion(3).                 // Version = @Version
    IsRoot().                      // ParentID IS NULL
    NotReplaced().                 // ReplacedByID IS NULL
    Search("fraggle", "Name", "Bio")  // (Name LIKE @SearchPattern OR Bio LIKE @SearchPattern)

// Snake-case schemas: pass the column name
w := dbrepo.NewWhere().
    NotDeleted("deleted_at").      // deleted_at IS NULL
    HasStatus("active", "status"). // status = @Status
```

### SelectBuilder

```go
sb := dbrepo.NewSelect("Tasks", "ID", "Title", "Status").
    Where(w).
    OrderByMap("title:asc,created_at:desc", columnMap, "ID ASC").
    Paginate(20, 0).
    WithDialect(dialect)

query, args := sb.Build()
countQuery, countArgs := sb.CountQuery()
```

When a dialect is set, table names are automatically quoted.

### Audit Helpers

Domain patterns as plain functions — no base class, no embedded struct:

```go
// Creating a record
dbrepo.SetCreateTimestamps(&t.CreatedAt, &t.UpdatedAt)
dbrepo.InitVersion(&t.Version)
dbrepo.SetCreateAudit(&t.CreatedBy, &t.UpdatedBy, currentUser)

// Updating
dbrepo.SetUpdateTimestamp(&t.UpdatedAt)
dbrepo.IncrementVersion(&t.Version)

// Soft delete
dbrepo.SetSoftDelete(&t.DeletedAt)
dbrepo.SetDeleteAudit(&t.DeletedAt, &t.DeletedBy, currentUser)

// State management
dbrepo.SetStatus(&t.Status, "published")
dbrepo.SetArchive(&t.ArchivedAt)
dbrepo.ClearArchive(&t.ArchivedAt)     // sets sql.NullTime.Valid = false (SQL NULL)
dbrepo.SetExpiry(&t.ExpiresAt, future)
dbrepo.ClearExpiry(&t.ExpiresAt)       // sets sql.NullTime.Valid = false (SQL NULL)
dbrepo.SetReplacement(&t.ReplacedByID, newID)
dbrepo.ClearReplacement(&t.ReplacedByID)  // sets sql.NullInt64.Valid = false (SQL NULL)
```

For deterministic tests, override the clock:

```go
dbrepo.NowFunc = func() time.Time { return fixedTime }
```

## Engines

| Engine | Constant | Driver Package |
|--------|----------|----------------|
| PostgreSQL | `fraggle.Postgres` | `fraggle/driver/postgres` |
| SQLite | `fraggle.SQLite` | `fraggle/driver/sqlite` |
| MSSQL | `fraggle.MSSQL` | `fraggle/driver/mssql` |

## Testing

Tests run against all three engines. SQLite runs in-memory, Postgres and MSSQL run via service containers in CI.

```bash
# Unit tests (always work, no external deps)
go test ./...

# Integration tests against real databases
FRAGGLE_POSTGRES_URL="postgres://user:pass@localhost:5432/testdb?sslmode=disable" \
FRAGGLE_MSSQL_URL="sqlserver://SA:Password@localhost:1433?database=master" \
go test ./... -v
```

## Philosophy

Fraggle follows Go's values and the [dothog design philosophy](https://github.com/catgoose/dothog/blob/main/PHILOSOPHY.md):

- **Explicit SQL, composable helpers.** Write the SQL, but don't write it by hand every time. The generated SQL is predictable — you can read it, copy it into a query tool, and run it directly.
- **Schema as code.** Table definitions are the source of truth. One declaration drives DDL, column lists, seed data, and schema snapshots. No drift between migration files and application code.
- **Domain patterns as primitives.** Soft delete, optimistic locking, archival — these aren't framework features. They're small functions that set timestamps and check values. If you need soft delete, call `SetSoftDelete`. If you don't, don't.
- **A little copying is better than a little dependency.** The Go standard library is the dependency. Everything else earns its place.

*"A Fraggle is never lost. A Fraggle is just exploring."*

## License

MIT
