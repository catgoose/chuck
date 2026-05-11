# Plan: #65 Item 3 — Postgres Sequence Default Normalization

## Objective

Stop reporting Postgres sequence-backed column defaults as drift when the declared side has no explicit default.

This plan covers only item 3 from issue `#65` and is the last remaining piece of #65.

## Problem

`schema/validate.go:101` compares declared and live column defaults via `normalizeDefault` (defined in `schema/diff.go:327`). The current normalization handles whitespace, outer parentheses, and case folding outside quotes — but it does not recognize Postgres sequence-backed defaults.

CI failure shape:

```
chuck_schema_test.id: default mismatch: declared "", live "nextval('chuck_schema_test_id_seq'::regclass)"
```

The declared `id` column has no explicit `DEFAULT` clause because the schema DSL treats integer primary keys / SERIAL-style columns as having an implicit sequence default that the dialect handles in DDL generation. Postgres stores the actual `nextval(...)` expression in `information_schema.columns.column_default`, which is what the live snapshot returns. Comparison sees `""` vs `"nextval(...)"` and reports drift.

## Decision

Extend `normalizeDefault` to recognize sequence-backed defaults and canonicalize them to the empty string.

A default value is considered sequence-backed when, after the existing trim / paren-strip / lowercasing, it matches the shape `nextval(...)`. The argument to `nextval` is irrelevant — any call to `nextval` is by definition sequence-backed.

Rationale:

- This is a comparator-local fix consistent with items 2 and 4. No ColumnDef changes, no DSL changes, no dialect changes.
- The empty-string canonicalization makes a declared `""` match a live `nextval('..._seq'::regclass)` without needing to know whether the user wrote `INTEGER PRIMARY KEY`, `BIGSERIAL`, or anything else.
- It is semantically correct: a sequence-backed default is the schema package's "no explicit default" state for an autoincrement column.
- Pattern recognition is engine-agnostic in implementation but Postgres-specific in practice — `nextval` is Postgres syntax, so SQLite and MSSQL live snapshots will never trigger the normalization. No risk to those engines.
- Identifying sequence-backed defaults purely by syntax (`nextval(...)`) avoids the need to thread engine context through `normalizeDefault`, keeping the change minimal.

## Scope

Modify only `normalizeDefault` in `schema/diff.go` and add tests. Do not touch:

- `ValidateSchema` / `validateAgainstLiveSnapshot` comparator logic
- `ColumnDef` or any DSL surface
- Live snapshot queries
- DDL generation
- Other normalization helpers (`normalizeType`, `splitTypeParams`)

## Files To Inspect

- `schema/diff.go` (the helper)
- `schema/diff_test.go` (existing normalizeDefault tests, if any)
- `schema/validate.go` (the call site, read-only — no edits)

## Matching Rule

After the existing trim / paren-strip / case-folding pass, if the result starts with `nextval(` and ends with `)`, return the empty string.

Apply the check after the existing transforms because:

- The existing case-folding lowercases keywords, so the check should look for `nextval` (lowercase), which is what the existing transform produces.
- The existing paren-strip handles wrappers like `(nextval(...))` that some Postgres versions emit.
- Putting the recognition at the end keeps existing behavior unchanged for non-sequence defaults.

## Implementation Plan

1. Read `normalizeDefault` in `schema/diff.go` and confirm the lowercasing transform leaves `nextval` lowercase regardless of the input casing.
2. Append the sequence-detection branch:

   ```go
   // Postgres sequence-backed defaults canonicalize to the empty
   // string so a declared column with no explicit default matches a
   // live column whose default is the implicit sequence created by
   // SERIAL / BIGSERIAL / INTEGER PRIMARY KEY.
   if strings.HasPrefix(s, "nextval(") && strings.HasSuffix(s, ")") {
       return ""
   }
   ```

   Place this after the lowercasing loop and before the final `return`.
3. Do not change the helper signature.
4. Do not change call sites.

## Edge Cases

Handle these explicitly:

- bare `nextval('seq'::regclass)` → empty (the failing case)
- wrapped `(nextval('seq'::regclass))` → empty (paren-stripped first)
- mixed-case `NEXTVAL('seq'::regclass)` → empty (lowercased first)
- trailing whitespace → empty (trimmed first)
- a literal default `'nextval'` (string default that happens to spell `nextval`) → unchanged because the surrounding single quotes are not stripped by the normalizer
- a non-sequence default like `42` or `CURRENT_TIMESTAMP` → unchanged
- empty string declared default → unchanged (still empty, still matches a sequence-normalized live default)

## Test Plan

Add to `schema/diff_test.go`. Look for the existing `normalizeDefault` test function — if one exists, extend it; otherwise create `TestNormalizeDefaultSequences`.

Cases:

1. `nextval('chuck_schema_test_id_seq'::regclass)` → `""`
2. `nextval('public.users_id_seq'::regclass)` → `""`
3. `(nextval('seq'::regclass))` → `""` (paren-wrapped form)
4. `NEXTVAL('seq'::regclass)` → `""` (case insensitivity via existing fold)
5. `  nextval('seq'::regclass)  ` → `""` (whitespace via existing trim)
6. `nextval('seq')` → `""` (no `::regclass` cast)
7. `'nextval'` (literal string default) → `'nextval'` (unchanged — quoted)
8. `42` → `"42"` (unchanged)
9. `CURRENT_TIMESTAMP` → `current_timestamp` (unchanged from existing case folding)
10. `""` → `""` (unchanged)

Then add an end-to-end test to `schema/validate_test.go` mirroring the style of `TestValidateImplicitUniqueIndexes` and `TestValidateIndexColumnNormalization`:

11. Postgres dialect, declared column with `Default: ""`, live column with `Default: "nextval('table_id_seq'::regclass)"` → no drift reported
12. Postgres dialect, declared column with `Default: "0"`, live column with `Default: "nextval(...)"` → drift IS reported (the user explicitly wanted `0`, not a sequence)
13. SQLite dialect, declared column with `Default: ""`, live column with `Default: ""` → no drift (regression guard, sequence path not engaged)

Use `validateAgainstLiveSnapshot` directly with crafted fixtures for tests 11–13.

## Acceptance Criteria

- `TestSchemaDriftPostgres/ValidateSchema` passes — no drift errors on the `id` column.
- `TestSchemaDriftMSSQL/ValidateSchema` continues to pass.
- The full schema integration test suite is fully green in CI.
- `normalizeDefault` continues to behave identically for all non-sequence defaults.
- Item 3 of #65 is closed by this PR. Issue #65 itself can be closed once CI confirms green.

## Risks

- Over-broad pattern recognition could mask real drift. Mitigation: the pattern requires `nextval(` literally as the leading token, which is a Postgres-specific built-in function name. No realistic non-sequence default should match this pattern.
- A user who explicitly declares `DEFAULT nextval('custom_seq')` and expects strict comparison against a different sequence name would lose that strictness. This is a deliberate trade-off — both sides normalize to empty, so they match. The plan accepts this because the schema DSL does not currently expose a way to declare a specific sequence name as a default, so this scenario is not reachable through the supported API.

## Execution Order

1. Inspect `normalizeDefault` and verify the lowercasing transform output for `nextval`.
2. Implement the pattern check.
3. Add unit tests for `normalizeDefault`.
4. Add end-to-end tests via `validateAgainstLiveSnapshot`.
5. Run `go test ./schema/...` locally.
6. Push and verify the full integration test suite goes green in CI.
7. Close #65 once CI is green (parent will handle).
