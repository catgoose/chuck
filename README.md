# chuck

[![Go Reference](https://pkg.go.dev/badge/github.com/catgoose/chuck.svg)](https://pkg.go.dev/github.com/catgoose/chuck)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

![chuck](https://raw.githubusercontent.com/catgoose/screenshots/main/chuck/chuck.jpg)

Chuck is a multi-dialect SQL fragment system for Go. One schema definition works across SQLite, PostgreSQL, and MSSQL.

No ORM, no query builder magic — just explicit SQL fragments, composable schema definitions, and domain patterns as primitives.

## Why

> Big Brain Developer say "it renders a table of users."
>
> -- Layman Grug

**Without chuck:**

```go
// One table. Three dialects. Three separate DDL strings.
const createTasksSQLite = `CREATE TABLE IF NOT EXISTS Tasks (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Title TEXT NOT NULL,
    Description TEXT,
    DeletedAt TIMESTAMP,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`
const createTasksPostgres = `CREATE TABLE IF NOT EXISTS "tasks" (
    "id" SERIAL PRIMARY KEY,
    "title" TEXT NOT NULL,
    "description" TEXT,
    "deleted_at" TIMESTAMPTZ,
    "created_at" TIMESTAMPTZ DEFAULT NOW(),
    "updated_at" TIMESTAMPTZ DEFAULT NOW()
)`
// Now maintain column lists for SELECT, INSERT, UPDATE -- per dialect.
// Add a column? Update six places.
```

**With chuck:**

```go
var TasksTable = schema.NewTable("Tasks").
    Columns(
        schema.AutoIncrCol("ID"),
        schema.Col("Title", schema.TypeString(255)).NotNull(),
        schema.Col("Description", schema.TypeText()),
    ).
    WithTimestamps().
    WithSoftDelete()

// Generate DDL for any dialect
for _, stmt := range TasksTable.CreateIfNotExistsSQL(dialect) {
    db.Exec(stmt)
}
// Column lists come free: TasksTable.InsertColumnsFor(dialect)
```

## Install

```bash
go get github.com/catgoose/chuck
```

Import only the database drivers you need:

```go
import _ "github.com/catgoose/chuck/driver/sqlite"
import _ "github.com/catgoose/chuck/driver/postgres"
import _ "github.com/catgoose/chuck/driver/mssql"
```

## Dialect Interface

The `Dialect` interface is composed from focused sub-interfaces. Each sub-interface captures a single responsibility, so functions can accept only the capability they need:

| Interface | Purpose |
|-----------|---------|
| `TypeMapper` | Maps Go types to SQL column type strings (`IntType`, `StringType`, `BoolType`, etc.) |
| `DDLWriter` | Generates DDL statements (`CreateTableIfNotExists`, `DropTableIfExists`, `InsertOrIgnore`, etc.) |
| `QueryWriter` | Generates query fragments (`Placeholder`, `Pagination`, `Now`, `LastInsertIDQuery`, etc.) |
| `Identifier` | Handles SQL identifier formatting (`NormalizeIdentifier`, `QuoteIdentifier`) |
| `Inspector` | Provides schema introspection queries (`TableExistsQuery`, `TableColumnsQuery`) |

`Dialect` composes all five, so passing a `Dialect` still works everywhere. But a function that only quotes identifiers can accept `Identifier` instead, making its dependency explicit and its tests simpler.

```go
d, _ := chuck.New(chuck.Postgres)

d.AutoIncrement()  // "SERIAL PRIMARY KEY"
d.TimestampType()  // "TIMESTAMPTZ"
d.Pagination()     // "LIMIT @Limit OFFSET @Offset"
d.Now()            // "NOW()"
d.Placeholder(1)   // "$1"
```

> grug not understand why other developer make thing so hard. grug supernatural power and marvelous activity: returning html and carrying single binary.
>
> -- Layman Grug

Each engine speaks its own dialect:

| Method | PostgreSQL | SQLite | MSSQL |
|--------|-----------|--------|-------|
| `AutoIncrement()` | `SERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` | `INT PRIMARY KEY IDENTITY(1,1)` |
| `TimestampType()` | `TIMESTAMPTZ` | `TIMESTAMP` | `DATETIME` |
| `Now()` | `NOW()` | `CURRENT_TIMESTAMP` | `GETDATE()` |
| `Placeholder(1)` | `$1` | `?` | `@p1` |
| `Pagination()` | `LIMIT @Limit OFFSET @Offset` | `LIMIT @Limit OFFSET @Offset` | `OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY` |
| `BoolType()` | `BOOLEAN` | `INTEGER` | `BIT` |
| `NormalizeIdentifier("CreatedAt")` | `created_at` | `CreatedAt` | `CreatedAt` |
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

### Identifier Normalization

`NormalizeIdentifier` transforms identifiers to the engine's idiomatic form. For Postgres, PascalCase names are converted to snake_case. Other engines return names unchanged.

```go
pg := chuck.PostgresDialect{}
pg.NormalizeIdentifier("CreatedAt")  // "created_at"
pg.NormalizeIdentifier("UserID")     // "user_id"
pg.NormalizeIdentifier("HTMLParser") // "html_parser"

sq := chuck.SQLiteDialect{}
sq.NormalizeIdentifier("CreatedAt")  // "CreatedAt" (unchanged)
```

All schema DDL methods apply normalization automatically — column names, table names, references, and index columns are all normalized for the target dialect.

### DDL Methods

All DDL methods quote and normalize identifiers automatically using the engine's style.

```go
d.CreateTableIfNotExists("users", body)
d.DropTableIfExists("users")
d.CreateIndexIfNotExists("idx_users_email", "users", "email")
d.InsertOrIgnore("users", "name, email", "'Alice', 'alice@test.com'")
```

`InsertOrIgnore` produces idempotent inserts: `INSERT OR IGNORE` (SQLite), `ON CONFLICT DO NOTHING` (Postgres), or `BEGIN TRY...END CATCH` (MSSQL).

`ReturningClause` generates a `RETURNING` clause for INSERT/UPDATE statements (supported by Postgres and SQLite 3.35+, empty on MSSQL):

```go
d.ReturningClause("id")              // Postgres: "RETURNING id"
d.ReturningClause("id, created_at")  // SQLite:   "RETURNING id, created_at"
```

`QuoteColumns` splits a comma-separated column list, normalizes and quotes each identifier, preserving sort direction suffixes:

```go
chuck.QuoteColumns(d, "CreatedAt, Title DESC")
// Postgres: "created_at", "title" DESC
```

### Accepting Sub-Interfaces

Functions that only need a subset of `Dialect` can accept a sub-interface directly. This makes dependencies explicit and simplifies testing -- you only need to implement the methods the function actually calls.

```go
// Only needs identifier quoting -- accepts Identifier, not full Dialect.
func quotedColumnList(d chuck.Identifier, cols []string) string {
    quoted := make([]string, len(cols))
    for i, c := range cols {
        quoted[i] = d.QuoteIdentifier(c)
    }
    return strings.Join(quoted, ", ")
}

// Needs type mapping and identifiers -- accepts Dialect (which embeds both).
func columnDDL(d chuck.Dialect, name string) string {
    return d.QuoteIdentifier(name) + " " + d.IntType()
}
```

All three implementations (`SQLiteDialect`, `PostgresDialect`, `MSSQLDialect`) satisfy every sub-interface, verified by compile-time checks.

## Opening Connections

```go
import _ "github.com/catgoose/chuck/driver/postgres"

db, dialect, _ := chuck.OpenURL(ctx, "postgres://user:pass@localhost:5432/myapp?sslmode=disable")
db, dialect, _ := chuck.OpenURL(ctx, "sqlite://:memory:")
db, dialect, _ := chuck.OpenURL(ctx, "sqlserver://user:pass@host:1433?database=erp")
```

`OpenURL` detects the engine from the URL scheme and returns a raw `*sql.DB` plus the matching `Dialect`. Supported schemes: `postgres://` (`postgresql://`), `sqlite://` (`sqlite3://`), `sqlserver://` (`mssql://`).

For SQLite, `OpenSQLite` opens a database with sensible defaults (WAL mode, 30s busy timeout, single-connection pool):

```go
import _ "github.com/catgoose/chuck/driver/sqlite"

db, dialect, _ := chuck.OpenSQLite(ctx, "path/to/app.db")
```

## Schema as Code

> THE FOOL asked: "What is a representation?" ... unlike a photograph, a representation carries CONTROLS. Instructions. Affordances.
>
> -- The Wisdom of the Uniform Interface

The `schema` package defines tables in Go. One declaration drives DDL generation, column lists, seed data, and schema snapshots.

```go
import "github.com/catgoose/chuck/schema"

var TasksTable = schema.NewTable("Tasks").
    Columns(
        schema.AutoIncrCol("ID"),
        schema.Col("Title", schema.TypeString(255)).NotNull(),
        schema.Col("Description", schema.TypeText()),
        schema.Col("AssigneeID", schema.TypeInt()).References("Users", "ID").OnDelete("SET NULL"),
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

### UUID Primary Keys

For Postgres apps using UUID primary keys:

```go
schema.UUIDPKCol("id")
// Postgres: id UUID PRIMARY KEY DEFAULT gen_random_uuid()
// SQLite:   id TEXT PRIMARY KEY
```

### Foreign Key References

`References` defines a foreign key. Chain `OnDelete` and `OnUpdate` for referential actions:

```go
schema.Col("TaskID", schema.TypeInt()).NotNull().
    References("Tasks", "ID").OnDelete("CASCADE")

schema.Col("AssigneeID", schema.TypeInt()).
    References("Users", "ID").OnDelete("SET NULL").OnUpdate("CASCADE")
```

Supported actions: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, `NO ACTION`.

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

Traits use PascalCase column names internally. For Postgres, these are automatically normalized to snake_case (`CreatedAt` becomes `created_at`). SQLite and MSSQL preserve the original casing.

### Table Factories

Common table patterns have factory functions:

```go
schema.NewMappingTable("UserRoles", "UserID", "RoleID")     // Many-to-many join table
schema.NewConfigTable("Settings", "Key", "Value")            // Key-value config
schema.NewLookupTable("Options", "Category", "Label")        // Lookup with grouping
schema.NewLookupJoinTable("TaskOptions")                      // Owner-to-lookup join table
schema.NewEventTable("AuditLog", cols...)                     // Append-only (all immutable)
schema.NewQueueTable("Jobs", "Payload")                       // Job queue with scheduling
```

### Column Lists

`TableDef` knows which columns to use in each context:

```go
TasksTable.SelectColumns()  // All columns (raw names)
TasksTable.InsertColumns()  // Excludes auto-increment
TasksTable.UpdateColumns()  // Only mutable columns
```

Dialect-aware variants normalize names for the target engine:

```go
TasksTable.SelectColumnsFor(dialect)  // Postgres: ["id", "title", "created_at", ...]
TasksTable.InsertColumnsFor(dialect)  // Postgres: ["title", "description", ...]
TasksTable.UpdateColumnsFor(dialect)  // Postgres: ["title", "description", "updated_at", ...]
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

`LiveSnapshot` returns column names, types, nullability, defaults, and indexes.

### Schema Validation

`ValidateSchema` compares a declared table definition against the live database. It normalizes column and table names for the dialect automatically — PascalCase declarations match the snake_case columns that Postgres DDL creates.

```go
// Validate a single table
errs := schema.ValidateSchema(ctx, db, dialect, TasksTable)
for _, e := range errs {
    log.Println(e) // "tasks.priority: column missing"
}

// Validate all tables at once
errs := schema.ValidateAll(ctx, db, dialect, UsersTable, TasksTable, StatusTable)
```

Use it in CI to catch schema drift:

```go
func TestSchemaDrift(t *testing.T) {
    errs := schema.ValidateSchema(ctx, db, dialect, TasksTable)
    if errs != nil {
        for _, e := range errs {
            t.Error(e)
        }
    }
}
```

For manual comparison, `LiveSnapshot` and `Snapshot` are still available:

```go
live, err := schema.LiveSnapshot(ctx, db, dialect, TasksTable.TableNameFor(dialect))
declared := TasksTable.Snapshot(dialect)
```

Multi-table variant:

```go
snaps, err := schema.LiveSchemaSnapshot(ctx, db, dialect, "users", "tasks", "statuses")
```

## Composable SQL Fragments (`dbrepo`)

The `dbrepo` package provides composable helpers that keep SQL visible. Functions use `@Name` placeholders with `sql.Named()` for dialect-agnostic parameter binding.

### Building Queries

```go
import "github.com/catgoose/chuck/dbrepo"

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

`Or` and `OrIf` add OR branches:

```go
w := dbrepo.NewWhere().
    And("Status = @Status", sql.Named("Status", "active")).
    Or("Status = @Status2", sql.Named("Status2", "pending"))
// WHERE Status = @Status OR Status = @Status2
```

Set a dialect for dialect-aware behavior (e.g. ILIKE on Postgres):

```go
w := dbrepo.NewWhere().WithDialect(dialect)
```

Semantic filter methods encode domain patterns. Each accepts an optional column name override for custom naming:

```go
w := dbrepo.NewWhere().WithDialect(dialect).
    NotDeleted().                  // DeletedAt IS NULL
    NotArchived().                 // ArchivedAt IS NULL (timestamp column)
    NotArchivedBool().             // NOT archived (boolean column)
    NotExpired().                  // ExpiresAt IS NULL OR ExpiresAt > CURRENT_TIMESTAMP
    HasStatus("active").           // Status = @Status
    HasVersion(3).                 // Version = @Version
    IsRoot().                      // ParentID IS NULL
    NotReplaced().                 // ReplacedByID IS NULL
    Search("chuck", "Name", "Bio")  // Postgres: ILIKE, others: LIKE

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

> Student ask Grug about complexity. Grug say: "complexity is apex predator." Student say: "how do I defeat the complexity?" Grug say: "no."
>
> -- Layman Grug

Chuck says "no" to the complexity of maintaining separate DDL strings per dialect. One schema definition. All dialects generated.

## Engines

| Engine | Constant | Driver Package |
|--------|----------|----------------|
| PostgreSQL | `chuck.Postgres` | `chuck/driver/postgres` |
| SQLite | `chuck.SQLite` | `chuck/driver/sqlite` |
| MSSQL | `chuck.MSSQL` | `chuck/driver/mssql` |

> A media type is a COVENANT. A sacred compact. A pinky promise between systems.
>
> -- The Wisdom of the Uniform Interface

A schema definition is the same kind of covenant — between your application and your database. Chuck makes that covenant explicit, testable, and diffable.

## Testing

Tests run against all three engines. SQLite runs in-memory, Postgres and MSSQL run via service containers in CI.

```bash
# Unit tests (always work, no external deps)
go test ./...

# Integration tests against real databases
CHUCK_POSTGRES_URL="postgres://user:pass@localhost:5432/testdb?sslmode=disable" \
CHUCK_MSSQL_URL="sqlserver://SA:Password@localhost:1433?database=master" \
go test ./... -v
```

## Philosophy

Chuck follows Go's values and the [dothog design philosophy](https://github.com/catgoose/dothog/blob/main/PHILOSOPHY.md):

- **Explicit SQL, composable helpers.** Write the SQL, but don't write it by hand every time. The generated SQL is predictable — you can read it, copy it into a query tool, and run it directly.
- **Schema as code.** Table definitions are the source of truth. One declaration drives DDL, column lists, seed data, and schema snapshots. No drift between migration files and application code.
- **Domain patterns as primitives.** Soft delete, optimistic locking, archival — these aren't framework features. They're small functions that set timestamps and check values. If you need soft delete, call `SetSoftDelete`. If you don't, don't.
- **A little copying is better than a little dependency.** The Go standard library is the dependency. Everything else earns its place.


## Architecture

```
  ┌─────────────────────────────────────────────────┐
  │                  your application                │
  │                                                 │
  │  schema.NewTable("Tasks")     dbrepo.NewWhere() │
  │        │                            │           │
  └────────┼────────────────────────────┼───────────┘
           │                            │
           v                            v
  ┌─────────────────────────────────────────────────┐
  │                    chuck                       │
  │                                                 │
  │  Dialect interface                              │
  │  ┌──────────┬──────────┬──────────┐             │
  │  │TypeMapper│DDLWriter │QueryWriter│             │
  │  │Identifier│Inspector │          │             │
  │  └──────────┴──────────┴──────────┘             │
  └────────┬───────────┬───────────┬────────────────┘
           │           │           │
      ┌────v────┐ ┌────v────┐ ┌───v─────┐
      │ SQLite  │ │Postgres │ │  MSSQL  │
      └─────────┘ └─────────┘ └─────────┘
```

One schema definition at the top, dialect-specific SQL at the bottom. Chuck
generates DDL, column lists, seed data, and query fragments for whichever
engine you're running.

## License

MIT
