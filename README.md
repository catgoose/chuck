# chuck

<!--toc:start-->

- [chuck](#chuck)
  - [Why](#why)
  - [Philosophy](#philosophy)
  - [Install](#install)
  - [Schema as Code](#schema-as-code)
    - [UUID Primary Keys](#uuid-primary-keys)
    - [Foreign Key References](#foreign-key-references)
    - [CHECK Constraints](#check-constraints)
    - [Traits](#traits)
    - [Table Factories](#table-factories)
    - [Indexes](#indexes)
    - [Column Lists](#column-lists)
    - [Seed Data](#seed-data)
    - [Table Dependency Ordering](#table-dependency-ordering)
    - [Schema Snapshots](#schema-snapshots)
    - [Live Schema Snapshots](#live-schema-snapshots)
    - [Schema Validation](#schema-validation)
    - [Schema Ensure](#schema-ensure)
      - [Structured Diffs](#structured-diffs)
  - [Dialect Interface](#dialect-interface)
    - [Column Type Methods](#column-type-methods)
    - [Identifier Normalization](#identifier-normalization)
    - [DDL Methods](#ddl-methods)
    - [Expression Helpers](#expression-helpers)
    - [Accepting Sub-Interfaces](#accepting-sub-interfaces)
  - [Opening Connections](#opening-connections)
  - [Composable SQL Fragments (`dbrepo`)](#composable-sql-fragments-dbrepo)
    - [Building Queries](#building-queries)
    - [Bulk INSERT](#bulk-insert)
    - [UPSERT](#upsert)
    - [WhereBuilder](#wherebuilder)
    - [SelectBuilder](#selectbuilder)
    - [UpdateBuilder](#updatebuilder)
    - [DeleteBuilder](#deletebuilder)
    - [Audit Helpers](#audit-helpers)
  - [Engines](#engines)
  - [Testing](#testing)
  - [Architecture](#architecture)
  - [License](#license)
  <!--toc:end-->

[![Go Reference](https://pkg.go.dev/badge/github.com/catgoose/chuck.svg)](https://pkg.go.dev/github.com/catgoose/chuck)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

![chuck](https://raw.githubusercontent.com/catgoose/screenshots/main/chuck/chuck.jpg)

Chuck is a multi-dialect SQL fragment system for Go. One schema definition works across SQLite, PostgreSQL, and MSSQL.

No ORM, no query builder magic -- just explicit SQL fragments, composable schema definitions, and domain patterns as primitives.

> A representation carries CONTROLS. Instructions. _Affordances._ The next available actions. Your JSON endpoint is a photograph of a mountain thrown at the client's face with a note that says "figure out the trails yourself."
>
> -- The Wisdom of the Uniform Interface

A schema definition isn't a photograph. It carries controls -- DDL generation, column lists, seed data, validation, snapshots, structured diffs. You define the table once and the representation tells every dialect what to do next. No separate document. No Confluence page for your DDL. The schema _is_ the instruction set.

## Why

> Big Brain Developer come to Grug and say "Grug, I have achieved enlightenment. I have built a micro-frontend architecture with seventeen independently deployable SPAs, each with its own state management solution, communicating through a custom event bus with schema validation."
>
> Grug say nothing for long time.
>
> Then Grug say "what it do"
>
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

Three dialects. Three DDL strings. Six column lists. Add a field? Update them all. Rename a column? Grep and pray. This is the Rube Goldberg machine of schema management. The ball rolls down the chute, hits the domino, rings the bell, and the hamster renders a table of users.

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

One definition. All dialects generated. Column lists derived. The schema tells each engine what to do -- in the response itself.

## Philosophy

> Student ask Grug about complexity.
>
> Grug say: "complexity is apex predator."
>
> Student say: "how do I defeat the complexity?"
>
> Grug say: "you do not defeat. you say the magic word."
>
> Student lean forward. "what is the magic word?"
>
> Grug say: "no."
>
> -- Layman Grug

Chuck says "no" to the complexity of maintaining separate DDL per dialect, hand-rolled column lists, and schema definitions that drift from the live database. One declaration. Everything derived.

Chuck follows Go's values and the [dothog manifesto](https://github.com/catgoose/dothog/blob/main/MANIFESTO.md):

- **Explicit SQL, composable helpers.** Write the SQL, but don't write it by hand every time. The generated SQL is predictable -- you can read it, copy it into a query tool, and run it directly. No magic. No DSL that compiles to something you can't debug. `hx-get` for your database.
- **Schema as code.** Table definitions are the source of truth. One declaration drives DDL, column lists, seed data, and schema snapshots. No drift between migration files and application code. The schema is a _covenant_ between your application and your database -- chuck makes that covenant explicit, testable, and diffable.
- **Domain patterns as primitives.** Soft delete, optimistic locking, archival -- these aren't framework features. They're small functions that set timestamps and check values. If you need soft delete, call `SetSoftDelete`. If you don't, don't. No base class. No embedded struct. No framework to buy into. Just functions.
- **A little copying is better than a little dependency.** The Go standard library is the dependency. Everything else earns its place. Your `node_modules` directory is zero megabytes of knowledge.

> grug not understand why other developer make thing so hard. grug supernatural power and marvelous activity: returning html and carrying single binary.
>
> -- Layman Grug

Chuck's supernatural power: returning DDL and carrying a single schema definition.

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

## Schema as Code

> THE FOOL asked: "What is a representation?" ... unlike a photograph, a representation carries CONTROLS. Instructions. Affordances.
>
> -- The Wisdom of the Uniform Interface

The `schema` package defines tables in Go. One declaration drives DDL generation, column lists, seed data, and schema snapshots. The representation carries the controls.

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

### CHECK Constraints

Add database-level value constraints to columns:

```go
schema.Col("Age", schema.TypeInt()).Check("Age >= 0")
schema.Col("Status", schema.TypeVarchar(20)).Check("Status IN ('draft','published','archived')")
schema.Col("Percentage", schema.TypeDecimal(5, 2)).Check("Percentage BETWEEN 0 AND 100")
```

CHECK constraints appear in DDL output for all three dialects and are included in schema snapshots.

### Traits

Traits add columns and behavior in one call. They're composable -- use as many or as few as you need:

| Trait                 | Columns Added                         | Purpose                            |
| --------------------- | ------------------------------------- | ---------------------------------- |
| `WithTimestamps()`    | `CreatedAt` (immutable), `UpdatedAt`  | Creation and modification tracking |
| `WithSoftDelete()`    | `DeletedAt`                           | Soft delete (nullable timestamp)   |
| `WithAuditTrail()`    | `CreatedBy`, `UpdatedBy`, `DeletedBy` | User attribution                   |
| `WithVersion()`       | `Version` (default 1)                 | Optimistic concurrency control     |
| `WithStatus(default)` | `Status`                              | Workflow state                     |
| `WithSortOrder()`     | `SortOrder`                           | Manual ordering                    |
| `WithNotes()`         | `Notes`                               | Nullable text field                |
| `WithUUID()`          | `UUID` (immutable, unique)            | External identifier                |
| `WithParent()`        | `ParentID`                            | Tree/hierarchy structures          |
| `WithReplacement()`   | `ReplacedByID`                        | Entity lineage tracking            |
| `WithArchive()`       | `ArchivedAt`                          | Archival timestamp                 |
| `WithExpiry()`        | `ExpiresAt`                           | Expiration timestamp               |

Traits use PascalCase column names internally. For Postgres, these are automatically normalized to snake_case (`CreatedAt` becomes `created_at`). SQLite and MSSQL preserve the original casing.

### Table Factories

Common table patterns have factory functions:

```go
schema.NewMappingTable("UserRoles", "UserID", "RoleID") // Many-to-many join table
schema.NewConfigTable("Settings", "Key", "Value")       // Key-value config
schema.NewLookupTable("Options", "Category", "Label")   // Lookup with grouping
schema.NewLookupJoinTable("TaskOptions")                // Owner-to-lookup join table
schema.NewEventTable("AuditLog", cols...)               // Append-only (all immutable)
schema.NewQueueTable("Jobs", "Payload")                 // Job queue with scheduling
```

### Indexes

Plain, unique, and partial (filtered) indexes:

```go
schema.Index("idx_tasks_title", "Title")                                   // plain
schema.UniqueIndex("idx_users_email", "Email")                             // unique
schema.PartialIndex("idx_active_users", "Email").Where("DeletedAt IS NULL") // partial (filtered)
schema.UniquePartialIndex("idx_active_email", "Email").Where("DeletedAt IS NULL") // both
```

Partial indexes pair naturally with `WithSoftDelete()` -- index only the rows that matter. Each dialect generates the correct syntax:

```
Postgres: CREATE UNIQUE INDEX IF NOT EXISTS "idx" ON "table" ("col") WHERE condition
SQLite:   CREATE UNIQUE INDEX IF NOT EXISTS "idx" ON "Table" ("Col") WHERE condition
MSSQL:    IF NOT EXISTS (...) CREATE UNIQUE INDEX [idx] ON [Table]([Col]) WHERE condition
```

### Column Lists

`TableDef` knows which columns to use in each context:

```go
TasksTable.SelectColumns() // All columns (raw names)
TasksTable.InsertColumns() // Excludes auto-increment
TasksTable.UpdateColumns() // Only mutable columns
```

Dialect-aware variants normalize names for the target engine:

```go
TasksTable.SelectColumnsFor(dialect) // Postgres: ["id", "title", "created_at", ...]
TasksTable.InsertColumnsFor(dialect) // Postgres: ["title", "description", ...]
TasksTable.UpdateColumnsFor(dialect) // Postgres: ["title", "description", "updated_at", ...]
```

### Seed Data

Declare initial rows as part of the schema. Seed is idempotent via the dialect's `InsertOrIgnore`:

```go
var StatusTable = schema.NewTable("Statuses").
	Columns(
		schema.AutoIncrCol("ID"),
		schema.Col("Name", schema.TypeVarchar(50)).NotNull().Unique(),
		schema.Col("Active", schema.TypeBool()).NotNull(),
	).
	WithSeedValues(
		schema.SeedValues{"Name": "active", "Active": true},
		schema.SeedValues{"Name": "archived", "Active": false},
	)

for _, stmt := range StatusTable.SeedSQL(dialect) {
	db.Exec(stmt)
}
```

`SeedValues` accepts Go values -- strings, ints, floats, bools, nil -- and handles SQL quoting per dialect. Booleans become `TRUE`/`FALSE` on Postgres and `1`/`0` on SQLite/MSSQL. For raw SQL expressions (functions, defaults), use `SQLExpr`:

```go
schema.SeedValues{"CreatedAt": schema.SQLExpr("CURRENT_TIMESTAMP")}
```

The older `SeedRow` API (raw SQL literals with manual quoting) still works:

```go
schema.SeedRow{"Name": "'active'"}  // you handle the quotes
```

### Table Dependency Ordering

When creating multiple tables with foreign key relationships, order matters. `CreationOrder` topologically sorts tables so parents are created before children:

```go
ordered, err := schema.CreationOrder(UsersTable, TasksTable, CommentsTable)
for _, t := range ordered {
    for _, stmt := range t.CreateIfNotExistsSQL(dialect) {
        db.Exec(stmt)
    }
}

// Reverse for teardown
dropOrder, _ := schema.DropOrder(UsersTable, TasksTable, CommentsTable)
for _, t := range dropOrder {
    db.Exec(t.DropSQL(dialect))
}
```

Self-referential foreign keys (`WithParent()`) are handled gracefully. Circular dependencies return `ErrCyclicDependency`.

### Schema Snapshots

Export the declared schema in structured or text format for diffing:

```go
// Structured (JSON-serializable) -- for CI or programmatic comparison
snap := TasksTable.Snapshot(dialect)
data, _ := json.MarshalIndent(snap, "", "  ")

// Human-readable text -- for side-by-side diffing
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

// Compare -- column names, types, nullability
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

`ValidateSchema` compares a declared table definition against the live database. It normalizes column and table names for the dialect automatically -- PascalCase declarations match the snake_case columns that Postgres DDL creates.

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

For structured, machine-readable drift detection, see `DiffSchema` in the [Schema Ensure](#schema-ensure) section below.

### Schema Ensure

> grug note: in development, grug want table to appear when grug declare it.
> in production, grug want loud alarm if table not match declaration. grug not
> want same behavior in both places. this seem like mass of mass of complexity
> but actually is one function with different mode. grug has mass enlightenment.

`Ensure` validates your schema against the live database -- and in development, bootstraps what's missing. Your schema definition is the covenant. Ensure enforces it.

| Mode         | Missing tables      | Drift in existing tables | Makes changes? |
| ------------ | ------------------- | ------------------------ | -------------- |
| `ModeStrict` | Error               | Error                    | Never          |
| `ModeDev`    | Auto-create + seed  | Error                    | Creates only   |
| `ModeDryRun` | Reports in diff     | Reports in diff          | Never          |

**ModeStrict** -- production. The covenant is law. If the schema doesn't match, you hear about it immediately:

```go
result, err := schema.Ensure(ctx, db, dialect, tables, schema.WithMode(schema.ModeStrict))
if err != nil {
    var ensureErr *schema.EnsureError
    if errors.As(err, &ensureErr) {
        for _, d := range ensureErr.Diffs {
            log.Printf("drift: %s", d.Table)
        }
    }
    log.Fatal(err)
}
```

**ModeDev** -- convenience. Tables appear when you declare them. Seed data flows in. But if an existing table has drifted, you still get an error -- the covenant holds for what already exists:

```go
result, err := schema.Ensure(ctx, db, dialect, tables, schema.WithMode(schema.ModeDev))
// result.TablesCreated: ["statuses", "tasks"]
// result.TablesSeeded:  ["statuses"]
```

Tables are created in foreign-key order automatically via `CreationOrder`. No manual ordering needed.

**ModeDryRun** -- measure twice, cut once. Reports everything, changes nothing. Run it against production before deploying to see exactly what's different:

```go
result, _ := schema.Ensure(ctx, db, dialect, tables,
    schema.WithMode(schema.ModeDryRun),
    schema.WithDiffOutput(os.Stdout),
)
for _, d := range result.Diffs {
    if d.HasDrift {
        log.Printf("%s has drift", d.Table)
    }
}
```

> past is already past -- don't debug it
>
> future not here yet -- don't optimize for it
>
> `Ensure(ModeStrict)` -- this present moment
>
> -- Layman Grug (adapted)

#### Structured Diffs

The diff is the representation that carries controls. It tells you -- or your CI pipeline, or an agent -- exactly what changed and what to do about it. No migration framework. No up/down files. The diff IS the instruction set.

```go
diff, err := schema.DiffSchema(ctx, db, dialect, TasksTable)
// diff.AddedColumns    -- in your code, not in the database
// diff.RemovedColumns  -- in the database, not in your code
// diff.ChangedColumns  -- nullability mismatches
// diff.MissingIndexes  -- declared indexes not found live
// diff.ExtraIndexes    -- live indexes not in your declaration
// diff.TableMissing    -- table doesn't exist at all
// diff.HasDrift        -- true if any of the above are non-empty
```

Diffs are JSON-serializable. Write them to a file for CI artifacts, pipe them to stdout for review, or feed them to whatever consumes structured data:

```go
// Write a single diff
diff.WriteTo(os.Stdout)
diff.WriteJSON("drift-report.json")

// Write all diffs
diffs, _ := schema.DiffAll(ctx, db, dialect, UsersTable, TasksTable)
schema.WriteDiffsTo(diffs, os.Stdout)
schema.WriteDiffsJSON(diffs, "all-diffs.json")

// Or capture during Ensure
var buf bytes.Buffer
schema.Ensure(ctx, db, dialect, tables,
    schema.WithMode(schema.ModeDryRun),
    schema.WithDiffOutput(&buf),
    schema.WithDiffFile("ensure-report.json"),
)
```

## Dialect Interface

> How many web browsers know the difference between a banking application and a wiki? _None of them._ NONE. And yet they operate ALL OF THEM.
>
> -- The Wisdom of the Uniform Interface

How many chuck schema definitions know the difference between Postgres and SQLite? None of them. And yet they generate DDL for all of them. One interface. Every engine. That is the uniform interface applied to your database layer.

The `Dialect` interface is composed from focused sub-interfaces. Each sub-interface captures a single responsibility, so functions can accept only the capability they need:

| Interface     | Purpose                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------ |
| `TypeMapper`  | Maps Go types to SQL column type strings (`IntType`, `StringType`, `BoolType`, etc.)             |
| `DDLWriter`   | Generates DDL statements (`CreateTableIfNotExists`, `DropTableIfExists`, `InsertOrIgnore`, etc.) |
| `QueryWriter` | Generates query fragments (`Placeholder`, `Pagination`, `Now`, `IsNull`, `Concat`, etc.)         |
| `Identifier`  | Handles SQL identifier formatting (`NormalizeIdentifier`, `QuoteIdentifier`)                     |
| `Inspector`   | Provides schema introspection queries (`TableExistsQuery`, `TableColumnsQuery`)                  |

`Dialect` composes all five, so passing a `Dialect` still works everywhere. But a function that only quotes identifiers can accept `Identifier` instead, making its dependency explicit and its tests simpler.

```go
d, _ := chuck.New(chuck.Postgres)

d.AutoIncrement() // "SERIAL PRIMARY KEY"
d.TimestampType() // "TIMESTAMPTZ"
d.Pagination()    // "LIMIT @Limit OFFSET @Offset"
d.Now()           // "NOW()"
d.Placeholder(1)  // "$1"
```

Each engine speaks its own dialect:

| Method                             | PostgreSQL                    | SQLite                              | MSSQL                                             |
| ---------------------------------- | ----------------------------- | ----------------------------------- | ------------------------------------------------- |
| `AutoIncrement()`                  | `SERIAL PRIMARY KEY`          | `INTEGER PRIMARY KEY AUTOINCREMENT` | `INT PRIMARY KEY IDENTITY(1,1)`                   |
| `TimestampType()`                  | `TIMESTAMPTZ`                 | `TIMESTAMP`                         | `DATETIME`                                        |
| `Now()`                            | `NOW()`                       | `CURRENT_TIMESTAMP`                 | `GETDATE()`                                       |
| `Placeholder(1)`                   | `$1`                          | `?`                                 | `@p1`                                             |
| `Pagination()`                     | `LIMIT @Limit OFFSET @Offset` | `LIMIT @Limit OFFSET @Offset`       | `OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY` |
| `BoolType()`                       | `BOOLEAN`                     | `INTEGER`                           | `BIT`                                             |
| `NormalizeIdentifier("CreatedAt")` | `created_at`                  | `CreatedAt`                         | `CreatedAt`                                       |
| `QuoteIdentifier("t")`             | `"t"`                         | `"t"`                               | `[t]`                                             |

### Column Type Methods

| Method             | Purpose                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------- |
| `StringType(n)`    | Engine's preferred string type -- `NVARCHAR(n)` on MSSQL (Unicode), `TEXT` on Postgres/SQLite |
| `VarcharType(n)`   | Exact `VARCHAR(n)` -- use when you need explicit length or non-Unicode on MSSQL               |
| `IntType()`        | `INTEGER` / `INT`                                                                            |
| `BigIntType()`     | `BIGINT` / `INTEGER` (SQLite)                                                                |
| `FloatType()`      | `DOUBLE PRECISION` / `REAL` / `FLOAT`                                                        |
| `DecimalType(p,s)` | `NUMERIC(p,s)` / `DECIMAL(p,s)` / `REAL` (SQLite)                                            |
| `TextType()`       | Unlimited text -- `TEXT` / `NVARCHAR(MAX)`                                                    |
| `BoolType()`       | `BOOLEAN` / `INTEGER` / `BIT`                                                                |
| `UUIDType()`       | `UUID` / `TEXT` / `UNIQUEIDENTIFIER`                                                         |
| `JSONType()`       | `JSONB` / `TEXT` / `NVARCHAR(MAX)`                                                           |

### Identifier Normalization

`NormalizeIdentifier` transforms identifiers to the engine's idiomatic form. For Postgres, PascalCase names are converted to snake_case. Other engines return names unchanged.

```go
pg := chuck.PostgresDialect{}
pg.NormalizeIdentifier("CreatedAt")  // "created_at"
pg.NormalizeIdentifier("UserID")     // "user_id"
pg.NormalizeIdentifier("HTMLParser") // "html_parser"

sq := chuck.SQLiteDialect{}
sq.NormalizeIdentifier("CreatedAt") // "CreatedAt" (unchanged)
```

All schema DDL methods apply normalization automatically -- column names, table names, references, and index columns are all normalized for the target dialect.

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
d.ReturningClause("id")             // Postgres: "RETURNING id"
d.ReturningClause("id, created_at") // SQLite:   "RETURNING id, created_at"
```

`QuoteColumns` splits a comma-separated column list, normalizes and quotes each identifier, preserving sort direction suffixes:

```go
chuck.QuoteColumns(d, "CreatedAt, Title DESC")
// Postgres: "created_at", "title" DESC
```

### Expression Helpers

Some SQL functions have genuinely different syntax across engines. Chuck provides dialect-aware helpers only where the divergence matters:

| Method                           | PostgreSQL                            | SQLite                               | MSSQL                                |
| -------------------------------- | ------------------------------------- | ------------------------------------ | ------------------------------------ |
| `IsNull("Email", "'none'")`      | `COALESCE("email", 'none')`          | `IFNULL("Email", 'none')`           | `ISNULL([Email], 'none')`           |
| `Concat("First", "' '", "Last")` | `"first_name" \|\| ' ' \|\| "last_name"` | `"First" \|\| ' ' \|\| "Last"` | `[First] + ' ' + [Last]`           |
| `Now()`                          | `NOW()`                               | `CURRENT_TIMESTAMP`                  | `GETDATE()`                          |

Column identifiers are normalized and quoted per dialect. String literals (single-quoted) pass through as-is.

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

## Composable SQL Fragments (`dbrepo`)

> Grug say: "before enlightenment: fetch JSON, parse JSON, validate JSON, transform JSON, store JSON in client state, derive view from client state, diff virtual DOM, reconcile DOM, hydrate DOM, subscribe to store, dispatch action, reduce state, re-derive view, re-diff virtual DOM."
>
> Student say: "and after enlightenment?"
>
> Grug say: "`hx-get`"
>
> -- Layman Grug

Before chuck: hand-roll WHERE clauses, duplicate column lists, maintain separate query strings per dialect, copy-paste audit logic into every repository.

After chuck: `dbrepo.NewWhere().NotDeleted().HasStatus("active")`.

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
dbrepo.ColumnsQ(d, "ID", "Name")                // `"ID", "Name"` (Postgres)
dbrepo.SetClauseQ(d, "Name", "Email")           // `"Name" = @Name, "Email" = @Email`
dbrepo.InsertIntoQ(d, "Users", "Name", "Email") // `INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email)`
```

### Bulk INSERT

Multi-row inserts with correct dialect-specific placeholders:

```go
dbrepo.BulkInsertInto(dialect, "Users", []string{"Name", "Email"}, 3)
// Postgres: INSERT INTO "users" ("name", "email") VALUES ($1, $2), ($3, $4), ($5, $6)
// SQLite:   INSERT INTO "Users" ("Name", "Email") VALUES (?, ?), (?, ?), (?, ?)
// MSSQL:    INSERT INTO [Users] ([Name], [Email]) VALUES (@p1, @p2), (@p3, @p4), (@p5, @p6)
```

The row count parameter controls how many value tuples are generated. Be aware of engine parameter limits (SQLite: 999, MSSQL: 2100) -- the function generates the SQL, chunking is your responsibility.

### UPSERT

> Master say: "but how do you handle conflicts?"
>
> Grug say: "ON CONFLICT DO UPDATE."
>
> Master say: "but MSSQL--"
>
> Grug say: "MERGE."
>
> Master say: "you are oversimplifying."
>
> Grug say: "yes. this is the point."

Insert-or-update with dialect-aware conflict resolution:

```go
dbrepo.UpsertIntoQ(dialect, "Settings", []string{"Key"}, "Key", "Value")
// Postgres: INSERT INTO "settings" ("key", "value") VALUES (@Key, @Value)
//           ON CONFLICT ("key") DO UPDATE SET "value" = EXCLUDED."value"
// SQLite:   INSERT INTO "Settings" ("Key", "Value") VALUES (@Key, @Value)
//           ON CONFLICT ("Key") DO UPDATE SET "Value" = excluded."Value"
// MSSQL:    MERGE [Settings] AS Target USING (VALUES (@Key, @Value)) AS Source ([Key], [Value])
//           ON Target.[Key] = Source.[Key]
//           WHEN MATCHED THEN UPDATE SET [Value] = Source.[Value]
//           WHEN NOT MATCHED THEN INSERT ([Key], [Value]) VALUES (Source.[Key], Source.[Value]);
```

The second argument specifies the conflict columns (uniqueness constraint). Non-conflict columns are automatically placed in the UPDATE SET clause.

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

When a dialect is set, table names and dot-qualified column names are automatically quoted.

**JOINs** -- INNER JOIN and LEFT JOIN composition:

```go
sb := dbrepo.NewSelect("Tasks", "Tasks.ID", "Tasks.Title", "Users.Name").
	Join("Users", "Tasks.AssigneeID = Users.ID").
	LeftJoin("Statuses", "Tasks.StatusID = Statuses.ID").
	Where(w).
	WithDialect(dialect)

query, args := sb.Build()
// SELECT "Tasks"."ID", "Tasks"."Title", "Users"."Name"
// FROM "Tasks"
// JOIN "Users" ON Tasks.AssigneeID = Users.ID
// LEFT JOIN "Statuses" ON Tasks.StatusID = Statuses.ID
// WHERE ...
```

Table names in JOIN clauses are dialect-quoted. ON conditions pass through as raw SQL. `CountQuery()` includes JOINs automatically. Dot-qualified column names (`Table.Column`) are quoted per-part.

### UpdateBuilder

> THE FOOL asked: "What is the uniform interface?"
>
> ... Your browser does not download a BankingApplicationSDK. Your browser speaks HTTP, understands media types, and follows links. Three things.
>
> -- The Wisdom of the Uniform Interface

UpdateBuilder speaks the dialect, understands WHERE clauses, and composes SET fragments. Three things:

```go
ub := dbrepo.NewUpdate("Tasks", "Title", "Status").
	Where(w).
	WithDialect(dialect)

query, args := ub.Build()
// UPDATE "tasks" SET "title" = @Title, "status" = @Status WHERE "deleted_at" IS NULL
```

Chain `.Returning("id")` for Postgres/SQLite RETURNING clause support (no-op on MSSQL).

WhereBuilder's semantic filters are most valuable here -- accidentally updating soft-deleted rows is a real bug that `.NotDeleted()` prevents.

### DeleteBuilder

```go
db := dbrepo.NewDelete("Tasks").
	Where(dbrepo.NewWhere().NotDeleted().HasStatus("archived")).
	WithDialect(dialect)

query, args := db.Build()
// DELETE FROM "tasks" WHERE "deleted_at" IS NULL AND "status" = @Status
```

Same pattern -- `.Where()`, `.WithDialect()`, `.Returning()`, `.Build()`.

### Audit Helpers

Domain patterns as plain functions -- no base class, no embedded struct:

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
dbrepo.ClearArchive(&t.ArchivedAt) // sets sql.NullTime.Valid = false (SQL NULL)
dbrepo.SetExpiry(&t.ExpiresAt, future)
dbrepo.ClearExpiry(&t.ExpiresAt) // sets sql.NullTime.Valid = false (SQL NULL)
dbrepo.SetReplacement(&t.ReplacedByID, newID)
dbrepo.ClearReplacement(&t.ReplacedByID) // sets sql.NullInt64.Valid = false (SQL NULL)
```

For deterministic tests, override the clock:

```go
dbrepo.NowFunc = func() time.Time { return fixedTime }
```

## Engines

| Engine     | Constant         | Driver Package          |
| ---------- | ---------------- | ----------------------- |
| PostgreSQL | `chuck.Postgres` | `chuck/driver/postgres` |
| SQLite     | `chuck.SQLite`   | `chuck/driver/sqlite`   |
| MSSQL      | `chuck.MSSQL`    | `chuck/driver/mssql`    |

> A media type is a COVENANT. A sacred compact. A pinky promise between systems.
>
> -- The Wisdom of the Uniform Interface

A schema definition is the same kind of covenant -- between your application and your database. It says: "if you receive this table definition, here is how you shall create it, query it, seed it, and validate it." The processing model is built in. The controls are embedded. No separate Swagger spec. No README that Kevin forgot to update.

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

## Architecture

```
  ┌──────────────────────────────────────────────────────────┐
  │                    your application                       │
  │                                                          │
  │  schema.NewTable("Tasks")   schema.Ensure()    dbrepo   │
  │        │                       │                  │      │
  └────────┼───────────────────────┼──────────────────┼──────┘
           │                       │                  │
           v                       v                  v
  ┌──────────────────────────────────────────────────────────┐
  │                        chuck                              │
  │                                                          │
  │  Dialect interface          Ensure pipeline               │
  │  ┌──────────┬──────────┐    ┌─────────────────────────┐  │
  │  │TypeMapper│DDLWriter │    │ DiffSchema → SchemaDiff │  │
  │  │Query     │Identifier│    │ Strict / Dev / DryRun   │  │
  │  │Writer    │Inspector │    │ JSON diff output        │  │
  │  └──────────┴──────────┘    └─────────────────────────┘  │
  └────────┬───────────┬───────────┬─────────────────────────┘
           │           │           │
      ┌────v────┐ ┌────v────┐ ┌───v─────┐
      │ SQLite  │ │Postgres │ │  MSSQL  │
      └─────────┘ └─────────┘ └─────────┘
```

<details>
<summary>Mermaid version</summary>

```mermaid
graph TB
    subgraph app ["Your Application"]
        NT["schema.NewTable()"]
        EN["schema.Ensure()"]
        DR["dbrepo"]
    end

    subgraph chuck ["chuck"]
        subgraph dialect ["Dialect Interface"]
            TM["TypeMapper"]
            DW["DDLWriter"]
            QW["QueryWriter"]
            ID["Identifier"]
            IN["Inspector"]
        end
        subgraph ensure ["Ensure Pipeline"]
            DIFF["DiffSchema → SchemaDiff"]
            MODES["Strict · Dev · DryRun"]
        end
    end

    NT --> dialect
    EN --> ensure
    DR --> dialect

    dialect --> SQLite[(SQLite)]
    dialect --> Postgres[(PostgreSQL)]
    dialect --> MSSQL[(MSSQL)]
    ensure --> SQLite
    ensure --> Postgres
    ensure --> MSSQL
```

</details>

One schema definition at the top, dialect-specific SQL at the bottom. Enter the application with a single table definition and a set of standardized interfaces. Follow the methods. Let the dialect drive the output. That is all.

> past is already past -- don't debug it
>
> future not here yet -- don't optimize for it
>
> server return html -- this present moment
>
> -- Layman Grug

Define the schema. Generate the DDL. Query the database. This present moment.

## License

MIT
