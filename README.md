# chuck

![image](https://github.com/catgoose/screenshots/blob/main/chuck/chuck.jpg)

<!--toc:start-->

- [chuck](#chuck)
  - [Why](#why)
  - [Philosophy](#philosophy)
  - [Install](#install)
  - [Schema as Code](#schema-as-code)
    - [Primary Keys](#primary-keys)
    - [Foreign Key References](#foreign-key-references)
    - [CHECK Constraints](#check-constraints)
    - [Traits](#traits)
    - [Table Factories](#table-factories)
    - [Indexes](#indexes)
    - [Column Lists](#column-lists)
    - [Seed Data](#seed-data)
    - [Table Dependency Ordering](#table-dependency-ordering)
    - [Owned Views](#owned-views)
    - [Owned Procedures (MSSQL)](#owned-procedures-mssql)
    - [Code-Object Validate and Apply](#code-object-validate-and-apply)
    - [Schema Snapshots](#schema-snapshots)
    - [Live Schema Snapshots](#live-schema-snapshots)
    - [Schema Validation](#schema-validation)
    - [Schema Ensure](#schema-ensure)
      - [Structured Diffs](#structured-diffs)
      - [Drift Detection Coverage](#drift-detection-coverage)
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

### Primary Keys

Auto-incrementing primary keys use a dedicated constructor:

```go
schema.AutoIncrCol("ID")
// Postgres: "id" SERIAL PRIMARY KEY
// SQLite:   "ID" INTEGER PRIMARY KEY AUTOINCREMENT
// MSSQL:    [ID] INT PRIMARY KEY IDENTITY(1,1)
```

UUID primary keys for Postgres apps:

```go
schema.UUIDPKCol("ID")
// Postgres: "id" UUID PRIMARY KEY DEFAULT gen_random_uuid()
// SQLite:   "ID" TEXT PRIMARY KEY
// MSSQL:    [ID] UNIQUEIDENTIFIER PRIMARY KEY
```

For arbitrary primary keys, chain `.PrimaryKey()` on any column. This affects both the snapshot metadata and the generated DDL:

```go
schema.Col("Code", schema.TypeVarchar(10)).PrimaryKey().NotNull()
// Postgres: "code" VARCHAR(10) PRIMARY KEY NOT NULL
```

### Foreign Key References

`References` defines a foreign key against an unqualified target. Chain `OnDelete` and `OnUpdate` for referential actions:

```go
schema.Col("TaskID", schema.TypeInt()).NotNull().
    References("Tasks", "ID").OnDelete("CASCADE")

schema.Col("AssigneeID", schema.TypeInt()).
    References("Users", "ID").OnDelete("SET NULL").OnUpdate("CASCADE")
```

For schema-qualified targets use `ReferencesQualified` (or `ReferencesObject` with a `chuck.ObjectName`):

```go
schema.Col("AgentID", schema.TypeInt()).NotNull().
    ReferencesQualified("sg", "SalesAgents", "ID")
```

Supported actions: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, `NO ACTION`.

### Schema-Qualified Tables

Tables can declare a schema namespace that is preserved through DDL generation, foreign-key metadata, snapshots, validation, diffing, ensure, and live introspection:

```go
schema.NewTable("SalesAgents").WithSchema("sg")
// or equivalently:
schema.NewQualifiedTable("sg", "SalesAgents")
```

On MSSQL this renders as `[sg].[SalesAgents]`; on Postgres as `"sg"."sales_agents"` (with the usual identifier normalization). Dependency ordering keys on the fully qualified `(schema, name)` pair, so `sg.SalesAgents` and `cl.SalesAgents` are treated as distinct nodes.

**SQLite fallback.** SQLite has no schema namespace, so the schema component is dropped from emitted SQL. When two declared schema-qualified tables would collapse to the same bare SQLite table name (e.g. `sg.SalesAgents` and `cl.SalesAgents`), `Ensure` and `CheckSchemaCompatibility` fail fast with `ErrSQLiteSchemaCollision` rather than silently merging them.

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

Each trait has a corresponding `ColumnDefs()` function (e.g., `TimestampColumnDefs()`, `SoftDeleteColumnDefs()`) if you need the column definitions without attaching them to a table.

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

#### MSSQL Destructive Bootstrap: Dropping Inbound Foreign Keys

On MSSQL, inline foreign keys declared via `chuck/schema` emit auto-generated constraint names (e.g. `FK__Goals__AgentID__1234ABCD`). `DropOrder` puts tables in the right teardown order, but `DROP TABLE` still fails while an inbound FK pins the table — the constraint must be dropped by name first.

`InboundForeignKeys` queries `sys.foreign_keys` for every FK whose parent or referenced table belongs to the owned set, deriving membership from your declared `*TableDef` slice rather than a handwritten parallel list. `DropInboundForeignKeys` executes the matching `ALTER TABLE ... DROP CONSTRAINT` statements so a destructive rebuild can proceed:

```go
tables := []*schema.TableDef{UsersTable, TasksTable, CommentsTable}

// Detach auto-named FKs first; on non-MSSQL engines this is a no-op.
if _, err := schema.DropInboundForeignKeys(ctx, db, dialect, tables...); err != nil {
    log.Fatal(err)
}

dropOrder, _ := schema.DropOrder(tables...)
for _, t := range dropOrder {
    db.ExecContext(ctx, t.DropSQL(dialect))
}
```

For dry-run logging, call `InboundForeignKeys` and feed each entry to `DropForeignKeySQL` to inspect the generated statements without executing them.

### Owned Views

Views live next to owned tables as first-class `ViewDef` values, so schema ownership stays in one package instead of splitting `TableDef` declarations from handwritten `CREATE VIEW` constants. A view carries an optional schema namespace and a SELECT body; the package emits dialect-aware lifecycle SQL through the same identifier-rendering path as `TableDef`.

```go
var ActiveTasksView = schema.NewView("v_active_tasks",
    `SELECT "id", "title" FROM "tasks" WHERE "deleted_at" IS NULL`)

// Or schema-qualified, mirroring NewQualifiedTable.
var PTOUsageView = schema.NewQualifiedView("sg", "v_pto_usage",
    `SELECT [AgentID], SUM([Hours]) AS [TotalHours] FROM [sg].[PTOEntries] GROUP BY [AgentID]`)

// Create / replace / drop -- rendered per dialect.
db.Exec(ActiveTasksView.CreateSQL(dialect))
for _, stmt := range ActiveTasksView.CreateOrReplaceSQL(dialect) {
    db.Exec(stmt)
}
db.Exec(ActiveTasksView.DropSQL(dialect))
```

Lifecycle SQL is rendered per dialect:

- **Postgres** uses native `CREATE OR REPLACE VIEW ... AS ...` and `DROP VIEW IF EXISTS ...`.
- **MSSQL** uses `CREATE OR ALTER VIEW ... AS ...` (MSSQL 2016+) and wraps the drop in a `sys.views` existence probe, matching the table-drop pattern.
- **SQLite** has no `CREATE OR REPLACE` / `CREATE OR ALTER` for views, so `CreateOrReplaceSQL` returns a `DROP VIEW IF EXISTS` followed by `CREATE VIEW` -- run them in order. The schema component is dropped on SQLite because it has no namespace.

The view body is taken verbatim; callers own its inner quoting and any references to owned tables. Ordering between owned views and the tables they read is caller-owned: create views after their tables, drop them before. This keeps the API thin and avoids forcing a generalized scheduler for what is usually a short linear chain on top of owned tables.

### Owned Procedures (MSSQL)

Stored procedures live next to owned tables and views as first-class `ProcedureDef` values, so MSSQL refresh / migration entrypoints stop being raw checked-in `.sql` files outside chuck ownership. Identity mirrors `ViewDef`: an optional schema namespace plus a name, rendered through the same `ObjectName` path so qualified procedures emit `[schema].[name]` consistently with `TableDef` and `ViewDef`.

The definition payload is the full T-SQL text that follows the qualified procedure name: optional parameter declarations, optional `WITH ...` options, the required `AS` keyword, and the body. T-SQL grammar places parameters and options between the name and `AS`, so chuck cannot inject `AS` on the caller's behalf without closing off those slots — the caller owns everything from parameters through `AS` through the body.

```go
var RefreshDashboardProc = schema.NewQualifiedProcedure("sg",
    "usp_RefreshDashboard",
    `@AgentID INT, @AsOf DATETIME2 = NULL
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM [sg].[Dashboard] WHERE [AgentID] = @AgentID;
    INSERT INTO [sg].[Dashboard] ([AgentID], [Total])
        SELECT [AgentID], SUM([Hours])
        FROM [sg].[PTOEntries]
        WHERE [AgentID] = @AgentID AND (@AsOf IS NULL OR [RecordedAt] <= @AsOf)
        GROUP BY [AgentID];
END`)

// Apply (idempotent on MSSQL 2016+):
stmt, err := RefreshDashboardProc.CreateOrAlterSQL(dialect)
if err != nil { return err }
if _, err := db.ExecContext(ctx, stmt); err != nil { return err }

// Tear down:
stmt, err = RefreshDashboardProc.DropSQL(dialect)
if err != nil { return err }
if _, err := db.ExecContext(ctx, stmt); err != nil { return err }
```

For a zero-parameter procedure the definition simply begins with `AS`:

```go
schema.NewQualifiedProcedure("sg", "usp_Ping",
    "AS BEGIN SET NOCOUNT ON; SELECT 1 AS Probe; END")
```

Lifecycle rules:

- **MSSQL** uses `CREATE OR ALTER PROCEDURE [schema].[name] <definition>` (MSSQL 2016+) for the apply path and wraps the drop in a `sys.procedures` existence probe so callers can run it unconditionally during teardown.
- **Postgres / SQLite** are explicitly unsupported in this first pass — the lifecycle methods return `schema.ErrProcedureDialectUnsupported` instead of silently no-op'ing. Use `errors.Is(err, schema.ErrProcedureDialectUnsupported)` to branch your bootstrap when the same code path runs against a non-MSSQL engine.

The definition is taken verbatim. Callers own all inner identifier quoting, parameter declarations, procedure options, the `AS` keyword itself, and the body; chuck contributes only the `CREATE OR ALTER PROCEDURE <qualified-name>` preamble. Ordering between procedures and the tables / views they read is caller-owned: create procedures after their dependencies, drop them before. SQL Agent jobs and `msdb`-level admin objects are intentionally out of scope for this primitive.

### Code-Object Validate and Apply

Tables stay on the strict `Ensure(...)` / `DiffSchema(...)` / `ValidateSchema(...)` covenant — relational data must never be auto-migrated, and drift in a table is always an error to escalate. Views and procedures are different: their bodies are code, callers usually want them to track the declaration the same way application code tracks `main.go`, and the safe answer is an idempotent re-apply. Chuck exposes explicit verb helpers so bootstrap code can pick the behavior intentionally rather than receiving it as a side effect of `Ensure`:

```go
// Views (all dialects):
schema.ApplyView(ctx, db, dialect, RefreshDashboardView)
schema.ApplyViews(ctx, db, dialect, v1, v2, v3) // caller-ordered, dependency-aware
schema.ValidateView(ctx, db, dialect, RefreshDashboardView)
schema.ValidateViews(ctx, db, dialect, v1, v2, v3)

// Procedures (MSSQL only):
schema.ApplyProcedure(ctx, db, dialect, RefreshDashboardProc)
schema.ApplyProcedures(ctx, db, dialect, p1, p2)
schema.ValidateProcedure(ctx, db, dialect, RefreshDashboardProc)
schema.ValidateProcedures(ctx, db, dialect, p1, p2)
```

`Apply*` is one-way: declared definition overwrites the live object. It is idempotent on Postgres / MSSQL (`CREATE OR REPLACE` / `CREATE OR ALTER`) and effectively idempotent on SQLite (`DROP IF EXISTS` + `CREATE`). `Apply*` does no pre-flight drift check — callers that want validate-then-apply semantics call `Validate*` first and apply only when drift is reported.

`Validate*` confirms the object exists and, where the engine stores definitions verbatim, also confirms the body / definition text matches the declaration after canonical normalization (whitespace collapse, trailing-semicolon strip, CREATE-preamble strip):

| Object     | Engine   | Existence | Body / definition comparison                                                                        |
| ---------- | -------- | --------- | --------------------------------------------------------------------------------------------------- |
| View       | SQLite   | yes       | yes (`sqlite_master.sql` stored verbatim)                                                           |
| View       | MSSQL    | yes       | yes (`sys.sql_modules.definition` stored verbatim)                                                  |
| View       | Postgres | yes       | **fails loud** — returns `ErrViewBodyComparisonUnsupported`; `pg_get_viewdef` canonicalizes too aggressively to support honest text comparison |
| Procedure  | MSSQL    | yes       | yes (`sys.sql_modules.definition` stored verbatim)                                                  |
| Procedure  | other    | n/a       | returns `schema.ErrProcedureDialectUnsupported`                                                     |

Postgres view-body comparison is intentionally not attempted: `pg_get_viewdef` expands `SELECT *` into explicit column lists, fully qualifies references, and inserts casts, so a textual compare against the declared body produces false drift on legitimate-looking declarations. Rather than silently returning success on existence, `ValidateView` on Postgres fails loud with a `*schema.ViewDriftError` whose single entry has `BodyComparisonSkipped=true` and unwraps to `schema.ErrViewBodyComparisonUnsupported`. Callers that want existence-only semantics on Postgres can opt in explicitly:

```go
if err := schema.ValidateView(ctx, db, dialect, viewDef); err != nil {
    if errors.Is(err, schema.ErrViewBodyComparisonUnsupported) {
        // Postgres: view exists, body compare unavailable — treat as success.
    } else {
        return err // ErrViewMissing or infra failure: still hard fail.
    }
}
```

Callers that need a stricter body assertion can fetch the live body via `schema.LiveViewBody(ctx, db, dialect, viewDef)` and run their own comparison against a canonicalized form they trust.

Drift surfaces as a structured error:

```go
err := schema.ValidateViews(ctx, db, dialect, v1, v2, v3)
if err != nil {
    var drift *schema.ViewDriftError
    if errors.As(err, &drift) {
        for _, d := range drift.Drifts {
            // d.Object, d.Missing, d.BodyMismatch, d.BodyComparisonSkipped,
            // d.DeclaredBody, d.LiveBody, d.Reason
        }
    }
    // For single-cause results, the error also unwraps to a sentinel:
    if errors.Is(err, schema.ErrViewMissing)  { /* none of the views exist */ }
    if errors.Is(err, schema.ErrViewBodyDrift) { /* every drifted view is a body mismatch */ }
    if errors.Is(err, schema.ErrViewBodyComparisonUnsupported) {
        // Every drift was a body-compare-skip (Postgres). Existence confirmed
        // for all — callers that want existence-only semantics treat this as
        // success.
    }
}
```

`schema.ProcedureDriftError` has the same shape with sentinels `schema.ErrProcedureMissing` and `schema.ErrProcedureDefinitionDrift`. Mixed-cause aggregate results (some missing + some drifted) intentionally do not unwrap to either sentinel, so callers can't branch on a single cause when the real failure mode is heterogeneous.

The intended ordering in bootstrap code stays caller-owned:

```go
// Tables strict — drift always fails, never auto-applies.
if _, err := schema.Ensure(ctx, db, dialect, tables); err != nil { return err }

// Views second — apply unconditionally, or validate-then-apply.
if err := schema.ApplyViews(ctx, db, dialect, views...); err != nil { return err }

// Procedures last — MSSQL only; non-MSSQL deployments skip this branch.
if dialect.Engine() == chuck.MSSQL {
    if err := schema.ApplyProcedures(ctx, db, dialect, procs...); err != nil { return err }
}
```

Chuck does not provide a generalized object-graph scheduler that hides this ordering, because the dependency chain on top of an owned table set is almost always short and linear and a scheduler abstraction would obscure more than it would help. Pick the order in code; the helpers preserve it.

#### Opt-in ownership notice (apply-owned decoration)

Owned views and procedures can carry an embedded "owned by https://github.com/catgoose/chuck" notice so DB-side readers (an operator inspecting `sqlite_master.sql` or `sys.sql_modules.definition`) can tell the object is code-owned and find the project directly. The notice is **opt-in** and **apply-owned**: configured once in a `schema.CodeObjectOptions` struct and injected into the live SQL only by the `Apply*WithOptions` helpers. The matching `Validate*WithOptions` helpers strip the configured prefix from the live text before comparison and tolerate its absence, so validate-only callers can pass options without forcing markers to exist in the database.

```go
opts := schema.CodeObjectOptions{
    OwnershipNotice: schema.DefaultOwnershipNotice,
    DocPreamble:     "Generated by bootstrap. See docs/schema.md for runbook.",
}

if err := schema.ApplyViewsWithOptions(ctx, db, dialect, opts, views...); err != nil {
    return err
}
if err := schema.ValidateViewsWithOptions(ctx, db, dialect, opts, views...); err != nil {
    return err
}

if dialect.Engine() == chuck.MSSQL {
    if err := schema.ApplyProceduresWithOptions(ctx, db, dialect, opts, procs...); err != nil {
        return err
    }
    if err := schema.ValidateProceduresWithOptions(ctx, db, dialect, opts, procs...); err != nil {
        return err
    }
}
```

`CodeObjectOptions` has two comment lanes, both apply-owned:

- **`OwnershipNotice`** — apply-owned marker. `schema.DefaultOwnershipNotice` is intentionally soft (it says out-of-band changes "may" fail validation or be overwritten) because the `Validate*` / `Apply*` lanes are explicit and Postgres view validation is existence-only. Callers can supply any string.
- **`DocPreamble`** — optional caller-controlled doc comment. Intended for purpose / contact / runbook links. **Not** treated as proof of ownership; it is metadata only.

A third lane lives on the object itself:

- **`ViewDef.WithDocAnnotation(text)` / `ProcedureDef.WithDocAnnotation(text)`** — per-object doc comment carried by the declaration. Unlike `DocPreamble` (which is shared across many objects and caller-controlled), the annotation hangs directly off the owned object and renders into the live SQL. Validation ignores leading comment-only front matter, so changing the text in code does **not** by itself produce drift.

```go
v := schema.NewView("v_open_tasks",
    "SELECT id FROM tasks WHERE done = 0").
    WithDocAnnotation("v_open_tasks v1: returns currently-open task ids")
```

When all three are set the rendered order is **`OwnershipNotice`, then `DocPreamble`, then per-object annotation**, then the payload. The chuck-owned marker sits at the top so DB readers see it first; the per-object annotation sits closest to the SQL body so it reads like a docstring on the object. Each non-empty segment becomes its own `/* ... */` SQL block comment, with a blank line between comment blocks, and the payload starts on the next line after the final block. `Validate*WithOptions` strips configured front matter and then ignores any remaining leading block comments on both sides before comparing the executable statement body/definition, so comment-only changes do not report drift.

Apply-owned tolerance, summarized:

| Live body shape | `ValidateView` (bare) | `ValidateViewWithOptions(opts)` |
| --- | --- | --- |
| Raw declared body (no comment) | pass | pass |
| Raw declared body + configured prefix (preamble + notice) | drift | pass |
| Raw declared body + a different leading comment | pass | pass |
| Raw declared body + stale configured prefix that no longer matches `opts` | drift | drift |

Per-object annotation behavior (`WithDocAnnotation`):

| Declared annotation vs. live annotation | `ValidateViewWithOptions(opts)` |
| --- | --- |
| Same text in code and live SQL | pass |
| Annotation changed in code (live still has prior text) | pass |
| Annotation added in code (live has no annotation) | pass |
| Annotation removed in code (live still carries old annotation) | pass |

Bare `ApplyView` / `ValidateView` / `ApplyProcedure` / `ValidateProcedure` are unchanged: they neither inject nor tolerate markers.

#### Renaming or retiring an owned view or procedure

Use **`WithReplaces(names ...chuck.ObjectName)`** on `ViewDef` or `ProcedureDef` to declare prior names that the apply path should drop and the validate path should flag as stale if they still exist. Same-type only: views replace views, procedures replace procedures. No schema-wide pruning, no automatic name inference.

```go
v := schema.NewView("v_open_tasks",
    "SELECT id FROM tasks WHERE done = 0").
    WithReplaces(
        chuck.ObjectName{Name: "v_open_tasks_v1"},
        chuck.ObjectName{Schema: "legacy", Name: "v_OpenTasks"},
    )

if err := schema.ApplyViewsWithOptions(ctx, db, dialect, opts, v); err != nil {
    return err
}
if err := schema.ValidateViewsWithOptions(ctx, db, dialect, opts, v); err != nil {
    return err // on SQLite/MSSQL, unwraps to schema.ErrViewReplacementStillExists when stale replacement is sole drift cause
}
```

Apply semantics: each listed name is dropped with the same dialect-aware `DROP IF EXISTS` pattern as a regular owned-object drop, then the current view (or procedure) is created. Validate semantics: each listed name is queried in the live database; any name still present surfaces as a `ViewDrift` / `ProcedureDrift` entry with `ReplacementStale=true` and the aggregated error unwraps to `schema.ErrViewReplacementStillExists` / `schema.ErrProcedureReplacementStillExists` when stale replacements are the only drift cause.

Batch helpers (`ApplyViewsWithOptions`, `ValidateViewsWithOptions`, and the procedure analogues) dedupe duplicate names across defs by structured `ObjectName` (schema + name match exactly), so the same prior name is only dropped or checked once even when multiple defs name it.

Engine notes:

- **SQLite** stores the view text verbatim in `sqlite_master.sql`, so both comments are fully visible there.
- **MSSQL** stores both view and procedure text verbatim in `sys.sql_modules.definition`.
- **Postgres** `pg_get_viewdef` reconstructs the SELECT from the parse tree and discards comments, so the notice will be present in the `CREATE OR REPLACE VIEW` statement chuck issues but will not be visible when an operator reads `pg_get_viewdef` output. This does not produce false drift because Postgres view validation is existence-only (`ErrViewBodyComparisonUnsupported`).

#### Opt-in snapshot operational metadata

For provenance — when an owned object was first applied, last applied, last
changed, what the current definition hashes to, and which source rev/tool
version applied it — chuck can record one row per owned object in a small
snapshot ledger alongside your data. The ledger is **opt-in** and **snapshot
only**: one current row per owned object, no append-only history table in
this first pass.

```go
cfg := schema.MetadataConfig{
    Owner:       "bootstrap",                         // required
    Schema:      "ops",                               // optional; ignored on SQLite
    SourceRepo:  "https://github.com/example/repo",   // optional provenance
    SourceRev:   commitSHA,                           // optional provenance
    ToolVersion: "v1.4.2",                            // optional provenance
}

// Create the ledger tables once, in your own bootstrap. Chuck does NOT
// implicitly CREATE SCHEMA for you when cfg.Schema is set.
if err := schema.EnsureMetadataTables(ctx, db, dialect, cfg); err != nil {
    return err
}

// Wire the ledger config into your existing CodeObjectOptions. Apply* writes
// a row after each successful apply when Metadata is non-nil; bare apply and
// the Validate* lane both ignore Metadata.
opts := schema.CodeObjectOptions{
    OwnershipNotice: schema.DefaultOwnershipNotice,
    DocPreamble:     "bootstrap: applied by example/repo",
    Metadata:        &cfg,
}
if err := schema.ApplyViewsWithOptions(ctx, db, dialect, opts, views...); err != nil {
    return err
}
```

Two tables are written:

- **`chuck_database_metadata`** — one row per `Owner`, tracking when that
  owner first applied anything and when it last applied anything.
- **`chuck_object_metadata`** — one row per `(owner, object_type,
  object_schema, object_name)` with `first_applied_at_utc`,
  `last_applied_at_utc`, `last_changed_at_utc`, `definition_hash`, and the
  optional `source_repo` / `source_rev` / `tool_version` provenance columns.

Snapshot semantics on each successful apply:

- **New row**: `first_applied = last_applied = last_changed = now`.
- **Same hash as last apply**: only `last_applied` advances. `last_changed`
  and `first_applied` stay frozen.
- **Different hash**: `last_applied` and `last_changed` advance,
  `definition_hash` is replaced, `first_applied` stays frozen.

The hash is computed over the canonical form of the rendered body or
definition — leading block-comment front matter (ownership notice, doc
preamble, per-object annotation) is stripped before hashing. Comment-only
changes therefore do not move `last_changed`, matching the comment-insensitive
validation contract.

The ledger is independent of drift detection: `Validate*WithOptions` does not
read the ledger and does not produce drift over missing or stale ledger
rows. Empty provenance fields are stored as SQL `NULL` so the ledger can
distinguish "not recorded" from "recorded as empty string".

When `OwnershipNotice` and `Metadata` are both set, the rendered chuck-owned
ownership comment carries an extra single-line pointer to the ledger so DB
readers inspecting the live view or procedure can jump straight to the row
that records its provenance:

```sql
CREATE VIEW "v_open_tasks" AS
/* Owned by https://github.com/catgoose/chuck. Do not edit in database.
Out-of-band changes may fail validation or be overwritten by apply/bootstrap.
Provenance recorded in "ops"."chuck_object_metadata". */
SELECT id FROM tasks WHERE done = 0
```

The pointer uses the dialect-quoted, schema-qualified form of
`chuck_object_metadata` (bare on SQLite; bracketed on MSSQL; double-quoted
on Postgres). It is only added when an ownership notice is already
rendering — `Metadata` alone never invents a fresh ownership block — and is
omitted when `Metadata` is `nil`, so the pointer text stays honest about
whether the ledger is actually enabled. Comment-only changes do not move
`last_changed` and do not produce drift.

### Schema Snapshots

Export the declared schema in structured or text format for diffing:

```go
// Structured (JSON-serializable) -- for CI or programmatic comparison
snap := TasksTable.Snapshot(dialect)
data, _ := json.MarshalIndent(snap, "", "  ")

// Human-readable text -- for side-by-side diffing
fmt.Println(TasksTable.SnapshotString(dialect))
// TABLE tasks
//   id                   SERIAL PRIMARY KEY NOT NULL [immutable]
//   title                TEXT NOT NULL
//   description          TEXT
//   ...

// Multi-table snapshot
fmt.Println(schema.SchemaSnapshotString(dialect, UsersTable, TasksTable, StatusTable))
```

### Live Schema Snapshots

Query a live database to get its actual schema, then compare against your declared schema:

```go
// Read what the database actually has -- single table
live, err := schema.LiveSnapshot(ctx, db, dialect, "Tasks")

// Or read multiple tables at once
lives, err := schema.LiveSchemaSnapshot(ctx, db, dialect, "Tasks", "Users", "Statuses")

// Compare declared vs live
declared := TasksTable.Snapshot(dialect)
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

`LiveSnapshot` returns column names, types, nullability, defaults, and indexes (including each index's columns, uniqueness, and WHERE clause). Postgres introspection uses `format_type()` for full type fidelity and scopes all queries to the `public` schema via `pg_namespace`.

### Schema Validation

> THE FOOL asked: "What is out-of-band information?" Out-of-band information is THE CONSPIRACY. It is the hidden knowledge. The secret handshake.
>
> -- The Wisdom of the Uniform Interface

Schema drift is out-of-band information. Your application thinks the table looks one way; the database knows otherwise. `ValidateSchema` eliminates the conspiracy.

```go
// Validate a single table
errs := schema.ValidateSchema(ctx, db, dialect, TasksTable)
for _, e := range errs {
    log.Println(e) // "tasks.priority: column missing"
}

// Validate all tables at once
errs := schema.ValidateAll(ctx, db, dialect, UsersTable, TasksTable, StatusTable)
```

Validation normalizes column and table names for the dialect automatically -- PascalCase declarations match the snake_case columns that Postgres DDL creates. It detects missing columns, extra columns, type mismatches, nullability mismatches, default mismatches, missing indexes, extra indexes, and index property changes.

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

> If your client must read your API docs to know which URL to POST to, that is out-of-band. If your client must know that `/api/v2/users/{id}` is the pattern for user resources, that is out-of-band.
>
> -- The Wisdom of the Uniform Interface

The diff is the representation that carries controls. It tells you -- or your CI pipeline, or an agent -- exactly what changed and what to do about it. No migration framework. No up/down files. The diff IS the instruction set. No out-of-band knowledge required.

```go
diff, err := schema.DiffSchema(ctx, db, dialect, TasksTable)
// diff.AddedColumns    -- in your code, not in the database
// diff.RemovedColumns  -- in the database, not in your code
// diff.ChangedColumns  -- type, nullability, or default mismatches
// diff.MissingIndexes  -- declared indexes not found live
// diff.ExtraIndexes    -- live indexes not in your declaration
// diff.ChangedIndexes  -- columns, uniqueness, or WHERE clause mismatches
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

#### Drift Detection Coverage

`DiffSchema` and `ValidateSchema` compare declared and live schemas across these dimensions:

| Dimension        | What's compared                                      |
| ---------------- | ---------------------------------------------------- |
| **Columns**      | Presence (missing/extra), type, nullability, default  |
| **Indexes**      | Presence (missing/extra), columns, uniqueness, WHERE  |

Type comparison normalizes across dialect aliases -- `VARCHAR(255)` matches `CHARACTER VARYING(255)`, `TIMESTAMPTZ` matches `TIMESTAMP WITH TIME ZONE`, `SERIAL` matches `INTEGER` for auto-increment columns. Default comparison normalizes syntax noise (whitespace, outer parentheses, SQL keyword casing) while preserving case-sensitive string literals -- `'Admin'` and `'admin'` are correctly detected as different.

Not currently covered by drift detection:

- Column-level unique constraints
- Primary key drift
- Foreign key constraints (ref table, ref column, ON DELETE, ON UPDATE)
- CHECK constraints

These are modeled in the snapshot structs but not yet introspected from live databases. Contributions for per-dialect constraint introspection are welcome as focused PRs.

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

For literal type strings that bypass dialect mapping, use `TypeLiteral("BYTEA")`.

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

All schema DDL methods and `dbrepo` builders apply normalization automatically when a dialect is set -- column names, table names, references, and index columns are all normalized for the target dialect.

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
db, dialect, _ := chuck.OpenURL(ctx, "sqlite://:memory:")   // or sqlite:///:memory:
db, dialect, _ := chuck.OpenURL(ctx, "sqlserver://user:pass@host:1433?database=erp")
```

`OpenURL` detects the engine from the URL scheme and returns a raw `*sql.DB` plus the matching `Dialect`. Supported schemes: `postgres://` (`postgresql://`), `sqlite://` (`sqlite3://`), `sqlserver://` (`mssql://`). For SQLite, both two-slash (`sqlite://:memory:`) and three-slash (`sqlite:///:memory:`, `sqlite:///path/to/db`) forms are accepted.

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

The `dbrepo` package provides composable helpers that keep SQL visible. By default, the named fragment helpers (`SetClause`, `Placeholders`, `InsertInto`, `UpsertInto`, etc.) and `UpdateBuilder` emit `@Name` placeholders paired with `sql.Named()`. This works directly against `database/sql` drivers that translate `sql.NamedArg` into native parameter syntax (e.g. `mattn/go-sqlite3`, `microsoft/go-mssqldb`, `jackc/pgx`), but it is **not** universally driver-agnostic: `lib/pq` and `sqlx.Rebind` do not understand `@Name` tokens, so callers on that stack must use the positional-bind escape hatches (`BulkInsertInto`, `UpdateBuilder.SetValues` -- see [UpdateBuilder](#updatebuilder)).

### Building Queries

```go
import "github.com/catgoose/chuck/dbrepo"

dbrepo.Columns("ID", "Name", "Email")               // "ID, Name, Email"
dbrepo.Placeholders("ID", "Name", "Email")           // "@ID, @Name, @Email"
dbrepo.SetClause("Name", "Email")                    // "Name = @Name, Email = @Email"
dbrepo.InsertInto("Users", "Name", "Email")          // "INSERT INTO Users (Name, Email) VALUES (@Name, @Email)"
```

Dialect-aware variants normalize and quote identifiers:

```go
dbrepo.ColumnsQ(d, "ID", "Name")                // `"id", "name"` (Postgres, normalized)
dbrepo.SetClauseQ(d, "Name", "Email")           // `"name" = @Name, "email" = @Email`
dbrepo.InsertIntoQ(d, "Users", "Name", "Email") // `INSERT INTO "users" ("name", "email") VALUES (@Name, @Email)`
```

Schema-qualified table names (`schema.table`) are rendered with each part quoted separately. SQLite drops the schema component because it has no schema namespace:

```go
dbrepo.InsertIntoQ(mssql, "sg.SalesAgents", "Name")
// INSERT INTO [sg].[SalesAgents] ([Name]) VALUES (@Name)

dbrepo.NewSelect("sg.SalesAgents", "ID", "Name").WithDialect(pg).Build()
// SELECT ID, Name FROM "sg"."sales_agents"

dbrepo.NewSelect("Tasks", "Tasks.ID").
    Join("sg.SalesAgents sa", "sa.ID = Tasks.AgentID").
    WithDialect(mssql).Build()
// SELECT [Tasks].[ID] FROM [Tasks] JOIN [sg].[SalesAgents] sa ON sa.ID = Tasks.AgentID
```

`SelectBuilder` column lists also accept three-part `schema.table.column` references; bare names and expressions (anything with whitespace or parentheses) pass through unquoted.

Convert a map to deterministic named args for use with `db.ExecContext`:

```go
args := dbrepo.NamedArgs(map[string]any{"Name": "Alice", "Email": "alice@test.com"})
// []any{sql.Named("Email", "alice@test.com"), sql.Named("Name", "Alice")} -- sorted by key
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

Semantic filter methods encode domain patterns directly. Each corresponds to a schema trait and accepts an optional column name override for snake_case schemas:

```go
w := dbrepo.NewWhere().WithDialect(dialect).
    NotDeleted().                  // DeletedAt IS NULL
    NotArchived().                 // ArchivedAt IS NULL (timestamp column)
    NotArchivedBool().             // NOT archived (boolean column)
    NotExpired().                  // ExpiresAt IS NULL OR ExpiresAt > CURRENT_TIMESTAMP
    HasStatus("active").           // Status = @Status
    HasVersion(3).                 // Version = @Version
    IsRoot().                      // ParentID IS NULL
    HasParent(42).                 // ParentID = @ParentID
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

When a dialect is set, table names and dot-qualified column names are automatically normalized and quoted.

**JOINs** -- INNER JOIN and LEFT JOIN composition:

```go
sb := dbrepo.NewSelect("Tasks", "Tasks.ID", "Tasks.Title", "Users.Name").
    Join("Users", "Tasks.AssigneeID = Users.ID").
    LeftJoin("Statuses", "Tasks.StatusID = Statuses.ID").
    Where(w).
    WithDialect(dialect)

query, args := sb.Build()
// Postgres (normalized to snake_case):
// SELECT "tasks"."id", "tasks"."title", "users"."name"
// FROM "tasks"
// JOIN "users" ON Tasks.AssigneeID = Users.ID
// LEFT JOIN "statuses" ON Tasks.StatusID = Statuses.ID
// WHERE ...
//
// SQLite (casing preserved):
// SELECT "Tasks"."ID", "Tasks"."Title", "Users"."Name"
// FROM "Tasks"
// JOIN "Users" ON Tasks.AssigneeID = Users.ID
// LEFT JOIN "Statuses" ON Tasks.StatusID = Statuses.ID
// WHERE ...
```

Table names and dot-qualified column names are normalized and quoted per dialect. ON conditions pass through as raw SQL. `CountQuery()` includes JOINs automatically.

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
// UPDATE "tasks" SET "title" = @Title, "status" = @Status WHERE DeletedAt IS NULL
```

Chain `.Returning("id")` for Postgres/SQLite RETURNING clause support (no-op on MSSQL).

WhereBuilder's semantic filters are most valuable here -- accidentally updating soft-deleted rows is a real bug that `.NotDeleted()` prevents.

#### Positional bind opt-out (`SetValues`)

The default `@Name` SET clause is opaque to `lib/pq` and to `sqlx.Rebind` (issue [#71](https://github.com/catgoose/chuck/issues/71)). Call `.SetValues(...)` to opt the SET clause out of `@Name` placeholders and into positional `?` placeholders that survive `sqlx.Rebind`:

```go
query, args := dbrepo.NewUpdate("accounts", "last_digest_sent_at").
    WithDialect(pgDialect).
    SetValues(time.Now()).
    Where(dbrepo.NewWhere().And("id = ?", accountID)).
    Build()
// query: UPDATE "accounts" SET "last_digest_sent_at" = ? WHERE id = ?
// args:  [time.Now(), accountID]

// Feed it through sqlx.Rebind for lib/pq:
result, err := db.ExecContext(ctx, sqlx.Rebind(sqlx.DOLLAR, query), args...)
```

Contract:

- SET values are supplied in column-declaration order (the order passed to `NewUpdate`); `Build` panics if the count does not match the column count.
- Returned args slice is `<SET values...> <WHERE args...>`. WHERE conditions are caller-supplied -- use `?` placeholders inside `NewWhere().And(...)` fragments so `sqlx.Rebind` rewrites SET and WHERE together.
- Dialect identifier quoting (`WithDialect`) still applies to column identifiers; only the placeholder shape changes.
- The default named-placeholder path is unchanged when `SetValues` is not called.

### DeleteBuilder

```go
db := dbrepo.NewDelete("Tasks").
    Where(dbrepo.NewWhere().NotDeleted().HasStatus("archived")).
    WithDialect(dialect)

query, args := db.Build()
// DELETE FROM "tasks" WHERE DeletedAt IS NULL AND Status = @Status
```

Same pattern -- `.Where()`, `.WithDialect()`, `.Returning()`, `.Build()`.

### Audit Helpers

> grug supernatural power and marvelous activity: drawing water and carrying firewood.
>
> (draw water in this metaphor is `SELECT` query. carry firewood is `template.Execute`. grug want be clear. metaphor sometimes confuse junior developer.)
>
> -- Layman Grug

Domain patterns as plain functions -- no base class, no embedded struct. Each function corresponds to a schema trait: `WithTimestamps()` declares the columns, these helpers set the values.

```go
// Creating a record (pairs with WithTimestamps, WithVersion, WithAuditTrail)
dbrepo.SetCreateTimestamps(&t.CreatedAt, &t.UpdatedAt)
dbrepo.InitVersion(&t.Version)
dbrepo.SetCreateAudit(&t.CreatedBy, &t.UpdatedBy, currentUser)

// Updating (pairs with WithTimestamps, WithVersion, WithAuditTrail)
dbrepo.SetUpdateTimestamp(&t.UpdatedAt)
dbrepo.IncrementVersion(&t.Version)
dbrepo.SetUpdateAudit(&t.UpdatedBy, currentUser)

// Soft delete (pairs with WithSoftDelete, WithAuditTrail)
dbrepo.SetSoftDelete(&t.DeletedAt)
dbrepo.SetDeleteAudit(&t.DeletedAt, &t.DeletedBy, currentUser)

// State management (pairs with WithStatus, WithArchive, WithExpiry, WithReplacement)
dbrepo.SetStatus(&t.Status, "published")
dbrepo.SetArchive(&t.ArchivedAt)
dbrepo.ClearArchive(&t.ArchivedAt) // SQL NULL
dbrepo.SetExpiry(&t.ExpiresAt, future)
dbrepo.ClearExpiry(&t.ExpiresAt) // SQL NULL
dbrepo.SetReplacement(&t.ReplacedByID, newID)
dbrepo.ClearReplacement(&t.ReplacedByID) // SQL NULL
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

Tests run against all three engines. SQLite runs in-memory by default. Postgres and MSSQL integration tests are gated behind environment variables:

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
