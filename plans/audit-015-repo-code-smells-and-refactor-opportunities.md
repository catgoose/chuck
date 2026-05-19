# Chuck Repo Audit — Code Smells, Refactor Opportunities, Coverage Gaps

**Date:** 2026-05-19
**Branch base:** `origin/main` @ `d68be3c` (post issue-#84 apply-owned decoration squash)
**Scope:** root package (`chuck`), `schema/`, `dbrepo/`, CI workflows, golangci config
**Method:** parallel read-survey across three packages, hand-verification of every claim quoted below

This artifact is diagnosis-only. No fixes were applied. Findings are graded by leverage; "issue-ready" items have enough evidence to file directly.

## Executive Summary

The codebase is in good shape overall: tests are dense, dialects are factored cleanly, and the recent apply-owned ownership-notice rework (PRs #85/#86) tightened the validate contract honestly. The audit surfaced **two concrete latent bugs** in identifier quoting, **one naming smell** that will mislead readers as the procedure surface grows, **a small cluster of coverage gaps** around regex preamble strippers, and **a handful of godoc drifts** left over from the past three PRs. Nothing is currently breaking production; the two quoting bugs are reachable only when a caller passes an identifier containing `"` or `'`.

## Prioritized Findings

### P0 — Latent Bugs (issue-ready)

#### F1. SQLite & Postgres `QuoteIdentifier` does not escape embedded double quotes
- **Severity:** bug-risk
- **Refs:** `sqlite.go:51-53`, `postgres.go:50-52`
- **Evidence:**
  ```go
  func (SQLiteDialect) QuoteIdentifier(name string) string  { return `"` + name + `"` }
  func (PostgresDialect) QuoteIdentifier(name string) string { return `"` + name + `"` }
  ```
  Compare to MSSQL (`mssql.go:55-58`), which correctly doubles `]`:
  ```go
  return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
  ```
  A caller passing `my"col` produces `"my"col"` — invalid SQL that closes the quoted identifier early. With user-influenced identifier names (rare in chuck's normal flow but possible in dbrepo Search-style helpers via `validIdentifier` regex which excludes `"`), this would surface as a parse error rather than injection. Still a real bug.
- **Fix shape:** mirror MSSQL — `strings.ReplaceAll(name, `"`, `""`)` for both engines.
- **Status:** issue-ready.

#### F2. MSSQL `CreateTableIfNotExists` and `DropTableIfExists` embed bracket-quoted name into single-quoted `N'…'` literal without escaping single quotes
- **Severity:** bug-risk
- **Refs:** `mssql.go:69-83`
- **Evidence:**
  ```go
  func (d MSSQLDialect) CreateTableIfNotExists(table, body string) string {
      q := d.QuoteIdentifier(table)
      return fmt.Sprintf(
          "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s') AND type in (N'U')) BEGIN CREATE TABLE %s (\n%s\n\t\t) END",
          q, q, body,
      )
  }
  ```
  `q` is the bracket-quoted name (e.g. `[foo]`). It is then interpolated into a `N'…'` Unicode string literal. If `table` contains a single quote (e.g. `tab'le`), `q = [tab'le]`, and the rendered SQL is `OBJECT_ID(N'[tab'le]')` — the literal terminates after the `'` and the trailing `le]')` becomes orphan tokens.
- Contrast with `CreateIndexIfNotExists` (`mssql.go:85-93`), which **does** escape: `strings.ReplaceAll(table, "'", "''")`. The schema package's `procedure.go:153-156` also escapes via the helper `escapeQuote`. The two table-DDL helpers are the outliers.
- **Fix shape:** wrap `q` with `strings.ReplaceAll(q, "'", "''")` before format, or hoist `escapeQuote` (currently in `schema/index.go:135`) into the root package.
- **Status:** issue-ready.

### P1 — Naming / Refactor

#### F3. `canonicalizeViewBody` is reused for procedure definitions
- **Severity:** refactor
- **Refs:** `schema/view_lifecycle.go:233`, `schema/procedure_lifecycle.go:181-182`
- **Evidence:** the function does generic SQL normalization (trim, drop trailing `;`, collapse internal whitespace). It is called from `ValidateProcedureWithOptions` to normalize procedure definitions. The name is misleading and the doc comment says "normalizes a view body", which is now wrong.
- **Fix shape:** rename to `canonicalizeStatement` or `canonicalizeSQLPayload`; update godoc to mention both view and procedure callers.
- **Status:** mechanical refactor; low risk; eligible for one-line cleanup PR.

#### F4. `escapeQuote` helper lives in `schema/index.go` but is needed by root package
- **Severity:** refactor
- **Refs:** `schema/index.go:135-145`, used at `schema/procedure.go:155`, `schema/index.go:130`. F2 above shows the root package re-implementing the same logic inline (without the helper).
- **Fix shape:** promote `escapeQuote` to the root package (export as `EscapeSQLStringLiteral` or keep unexported but in `chuck.go`); have `mssql.go`, `schema/index.go`, `schema/procedure.go` call the same helper.
- **Status:** roll into F2's fix PR.

### P2 — Coverage Gaps

#### F5. Preamble-stripper regexes are untested on quoted identifiers with special characters
- **Severity:** coverage-gap
- **Refs:** `schema/view_lifecycle.go:214` (`viewBodyPreambleRe`), `schema/procedure_lifecycle.go:126` (`procedurePreambleRe`)
- **Evidence:** both regexes claim to handle `[bracket]`, `"double-quote"`, and bare identifier forms. Coverage in `view_lifecycle_test.go` exercises bare and bracket forms; double-quoted forms (Postgres-style) are not tested. A Postgres view with a name containing a space (`"my view"`) or a backslash would today escape the regex's `[^"]+` body silently, but no test confirms it.
- **Fix shape:** add table-driven tests covering Postgres quoted identifiers and schema-qualified mixed-quoting (`[sg]."weird name"`).
- **Status:** test-only, can land alongside F1 since both touch quoting edge cases.

#### F6. `ApplyProcedureWithOptions` lacks an explicit non-MSSQL-dialect rejection test
- **Severity:** coverage-gap
- **Refs:** `schema/procedure_lifecycle.go:250-260`
- **Evidence:** `code_object_options_test.go:182-189` covers `ApplyProcedureWithOptions` rejection on Postgres. `ApplyProceduresWithOptions` (plural) at `:270` also has a guard but is not directly asserted in any test for non-MSSQL refusal. `ValidateProcedure` / `ValidateProceduresWithOptions` are both covered.
- **Fix shape:** add a parallel test for `ApplyProceduresWithOptions` on SQLite dialect, asserting `ErrProcedureDialectUnsupported`.
- **Status:** test-only.

#### F7. Table column-default normalization differs from view/proc body normalization
- **Severity:** refactor or docs-drift (pick one)
- **Refs:** `schema/diff.go:339-363` (`normalizeDefault`) vs `schema/view_lifecycle.go:233` (`canonicalizeViewBody`)
- **Evidence:** `normalizeDefault` trims, strips parens, lowercases non-quoted segments, collapses `nextval(...)` → ""; it does **not** collapse internal whitespace. View/proc canonicalization collapses internal whitespace but does not lowercase. A declared default `"now( )"` would not match live `"now()"` — drift would be reported even though semantics are identical. The asymmetry is intentional (column defaults rarely contain whitespace inside parens) but is undocumented.
- **Fix shape:** either (a) add `strings.Fields(s)`-style whitespace collapse to `normalizeDefault`, or (b) add a doc comment explaining why the rules diverge.
- **Status:** discretionary; not currently biting any caller.

### P3 — Docs / Godoc Drift

#### F8. `dbrepo/fragments.go` package doc still claims "dialect-agnostic" before listing dialect-aware helpers
- **Severity:** docs-drift
- **Refs:** `dbrepo/fragments.go` package-level comment
- **Evidence:** the doc was updated for PR #83's `SetValues` path but still uses "dialect-agnostic" framing for the `@Name`-placeholder helpers, which is true for the placeholder lane only. The Q-suffixed helpers and `BulkInsertInto`/`UpsertIntoQ` are dialect-aware, and the package now strongly couples to `chuck.Dialect` for quoting. A reader skimming the package doc will miss the distinction.
- **Fix shape:** one-paragraph rewrite: "two lanes — `@Name` placeholders for drivers that translate `sql.NamedArg`, plus Q-suffixed helpers that take a `chuck.Dialect` for identifier quoting and normalization. See `UpdateBuilder.SetValues` for the positional-bind escape hatch."
- **Status:** doc-only.

#### F9. `Dialect.SupportsLastInsertID` semantic is undocumented at the interface
- **Severity:** docs-drift
- **Refs:** `sqlite.go:93` (`true`), `mssql.go:112` (`false`), `postgres.go:93` (`false`), interface declaration in `chuck.go`
- **Evidence:** SQLite returns `true` (driver's native `Result.LastInsertId()` works); MSSQL returns `false` despite also exposing `LastInsertIDQuery() = "SELECT SCOPE_IDENTITY()…"`; Postgres returns `false` and relies on `RETURNING`. The intended contract is "does the driver support `Result.LastInsertId()` directly?" — but the interface godoc does not say so. Callers might reasonably assume `SupportsLastInsertID == false` means "the engine has no concept of last-insert-id at all," which is wrong for MSSQL.
- **Fix shape:** one-sentence godoc on the interface method explaining what `true` and `false` mean and pointing at `LastInsertIDQuery` / `ReturningClause` as the per-engine recipes.
- **Status:** doc-only.

#### F10. `WhereBuilder.Search` registers a `Search` named arg that no SQL fragment references
- **Severity:** refactor
- **Refs:** `dbrepo/where.go:96-99`
- **Evidence:**
  ```go
  w.And("("+strings.Join(conditions, " OR ")+")",
      sql.Named("Search", search),
      sql.Named("SearchPattern", pattern),
  )
  ```
  Only `@SearchPattern` is referenced in the assembled SQL fragment. The `Search` arg is dead — drivers will warn or ignore it depending on the driver. More worrying: two calls to `WhereBuilder.Search` will register `SearchPattern` twice with different values, and behavior is driver-dependent (lib/pq tolerates, MSSQL driver errors out).
- **Fix shape:** remove the dead `Search` named arg; for the duplicate-`Search` case, generate a unique key per call (`SearchPattern_1`, `_2`, …) or document that `Search` is single-call only.
- **Status:** one-line drop for the dead arg; the duplicate-call issue needs a design choice.

### P4 — Accepted Tradeoffs / Skim Notes

- **`ObjectName.Equal` is byte-comparison.** Agent flagged as case-insensitivity bug; on read this is the intended contract — callers must normalize both sides via `Dialect.NormalizeIdentifier` before calling `Equal`. Godoc could mention this but the behavior is correct.
- **Procedure surface is MSSQL-only.** All public procedure helpers gate on `chuck.MSSQL` and return `ErrProcedureDialectUnsupported` elsewhere. This is documented and intentional.
- **`ParseObjectName` passthrough for strings containing parens/whitespace.** Documented as intentional (subqueries, derived tables). No issue.
- **CI integration tests skip silently when `CHUCK_POSTGRES_URL` / `CHUCK_MSSQL_URL` is unset.** Workflows do provide the services and env vars, so silent skips would only fire if the workflow file is mis-edited. Acceptable, but adding `t.Fatal` when the env var is required in CI (vs. local skip) would tighten regressions.
- **Test false-greens after `ApplyView`.** Several `view_lifecycle_test.go` tests `require.NoError` on apply without re-reading live DDL. This is acceptable because the unit tests run against in-memory SQLite where a malformed `CREATE VIEW` would fail the `Exec`. Integration tests do the round-trip check.

## Refactor Opportunities (Debt Buckets)

1. **Identifier-escape unification.** Promote `schema/index.go:escapeQuote` to the root package; fix F2; mirror the escape policy across `mssql.go`, `sqlite.go`, `postgres.go` `QuoteIdentifier`. One PR.

2. **Canonicalization helper renaming + consolidation.** Rename `canonicalizeViewBody` → `canonicalizeStatement`; consider whether `normalizeDefault` should share a whitespace-collapse step. One PR.

3. **`Dialect` interface godoc pass.** `SupportsLastInsertID`, `LastInsertIDQuery`, `ReturningClause` need a shared "how to get the new row's ID per engine" doc block. One PR, doc-only.

4. **`WhereBuilder.Search` cleanup.** Drop the dead `Search` named arg; decide design for multi-`Search` chains. One PR.

## Test / Validation Blind Spots

- Postgres double-quoted identifier handling in preamble regexes (F5).
- `ApplyProceduresWithOptions` non-MSSQL rejection (F6).
- `QuoteIdentifier` round-trip with embedded `"` / `]` / `'` (F1, F2).
- Column-default whitespace edge cases (F7) — low priority.

## Issue Candidates (highest-signal, ready to file)

1. **"SQLite and Postgres `QuoteIdentifier` do not escape embedded double quotes"** — body: F1 above with reproducer.
2. **"MSSQL `CreateTableIfNotExists` / `DropTableIfExists` interpolate bracket-quoted name into `N'…'` literal without escaping single quotes"** — body: F2 above with reproducer.
3. **"Rename `canonicalizeViewBody` → `canonicalizeStatement` to reflect dual-use across views and procedures"** — body: F3 above, mechanical refactor.

The remaining findings (F5–F10) are real but smaller-leverage; recommend bundling into one "code-audit cleanup" PR rather than separate issues. If a follow-up roadmap is wanted, group as:

- Lane A: latent-bug fixes (F1, F2, F4 helper hoist).
- Lane B: canonicalization rename + table-default whitespace decision (F3, F7).
- Lane C: doc passes (F8, F9) + Search cleanup (F10) + coverage backfill (F5, F6).

## Validation Performed for This Audit

- `go test ./...` — passed before audit started (no churn).
- `golangci-lint run --timeout=5m ./...` — 0 issues.
- Hand-verified the four agent claims that looked too strong:
  - **`escapeQuote` "undefined"** — false, defined at `schema/index.go:135`.
  - **MSSQL `CreateIndexIfNotExists` injection** — false; it does escape. The real injection vectors are `CreateTableIfNotExists` and `DropTableIfExists` (F2 above).
  - **SQLite/Postgres `QuoteIdentifier` not escaping `"`** — confirmed (F1).
  - **`canonicalizeViewBody` used for procedures** — confirmed (F3).
- Did **not** re-run any integration test paths; `CHUCK_POSTGRES_URL` and `CHUCK_MSSQL_URL` were not set locally.

## Areas Skimmed but Not Deep-Read

- `schema/event.go`, `schema/queue.go`, `schema/lookup.go`, `schema/mapping.go`, `schema/seed_values.go`, `schema/order.go`, `schema/traits.go`, `schema/config.go` — surface-skimmed for naming consistency; no findings surfaced; deep-read deferred.
- `dbrepo/audit.go` — surface-skimmed; pass-through timestamp helpers.
- Driver shims in `driver/{mssql,postgres,sqlite}` — surface-skimmed; thin registration wrappers; no findings.
- README narrative outside the recently-edited "Code-Object Validate and Apply" subsection — not re-audited.
