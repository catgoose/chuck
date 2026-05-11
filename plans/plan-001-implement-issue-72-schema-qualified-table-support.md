# Implement issue 72 schema-qualified table support

## Source

- GitHub issue `#72`: schema-qualified table support, updated on 2026-05-11 to narrow scope to the schema/DDL/introspection layer.
- GitHub issue `#73`: `dbrepo` support for schema-qualified table references is explicitly out of scope for this plan.
- Relevant code paths today:
  - `schema/table.go`
  - `schema/column.go`
  - `schema/index.go`
  - `schema/snapshot.go`
  - `schema/live_snapshot.go`
  - `schema/ensure.go`
  - `schema/validate.go`
  - `schema/diff.go`
  - `schema/order.go`
  - `postgres.go`
  - `mssql.go`
  - `sqlite.go`
  - schema and dialect tests under `schema/*_test.go`, `chuck_test.go`, and integration coverage where available

## Objective

- Implement first-class schema-qualified table identity for the `schema` package and dialect inspection layer without breaking existing unqualified callers.
- Keep schema and table as structured metadata through DDL generation, foreign-key metadata, snapshots, validation, diffing, ensure, and live introspection.
- Match the updated issue direction: do not implement this as a dotted-string shortcut, and do not pull `dbrepo` into this plan.

## Scope

- Add a structured object-name representation used internally for declared tables and foreign-key targets.
- Extend `TableDef` with schema-aware identity and provide an additive public API such as `WithSchema`, plus any thin constructor/helper that is clearly useful and does not muddy the model.
- Replace raw string-based table rendering paths with one shared helper for normalized and quoted table references.
- Update DDL generation for create, drop, indexes, seed SQL, and foreign keys to render qualified names correctly for Postgres and MSSQL.
- Update snapshot and live-snapshot representations so schema-qualified identity is preserved as structured data rather than only as one combined name string.
- Update schema ensure/validate/diff/live introspection to inspect the correct object by schema and table, not by table name alone.
- Update dependency ordering to key relationships by fully qualified identity so duplicate table names in different schemas work correctly.
- Implement SQLite fallback behavior from the issue: ignore schema in emitted SQL but fail fast if multiple declared tables collapse to the same bare SQLite table name.
- Add or revise unit/integration tests and minimal README/package examples needed to document the supported API and SQLite limitation.

## Non-Goals

- Do not implement `dbrepo` `FROM`/`JOIN`/alias/column-reference support here; that belongs to `#73`.
- Do not accept or normalize dotted-string table names as the primary implementation model.
- Do not redesign the library into a migration framework or broaden scope beyond schema declarations and dialect inspection.
- Do not make avoidable breaking API changes for existing unqualified table definitions.

## Constraints

- Preserve backward compatibility for existing callers that only use `TableDef.Name`-style unqualified declarations.
- Prefer additive API and helper changes over a breaking `Dialect` interface redesign; if dialect inspection needs a new shape, keep it narrowly scoped and defensible.
- Use one internal qualified-name helper path consistently so table rendering logic does not fork across DDL, seed SQL, snapshots, and inspectors.
- Foreign-key metadata must point at structured object identity, not a raw referenced-table string.
- Dependency ordering must compare self-references and parent/child edges using fully qualified identity.
- SQLite behavior must be explicit and safe. Ignoring schema is acceptable; silent name collisions are not.
- Keep snapshot/live-snapshot output coherent for consumers: schema/table split should remain inspectable rather than being recombined into an opaque string.
- Philosophy source is absent in this repo status output, so skip philosophy-specific steps unless a source appears during execution.

## Validation

Respect repo-local `.agents/tmux-skills*.json` for tests/commits.

- `workflow.tests` is `optional`, so run the targeted tests that directly cover touched packages and note any broader suites intentionally skipped.
- Minimum validation target:
  - `go test . ./schema`
- If shared root helpers or cross-package behavior are changed more broadly than expected, expand to:
  - `go test ./...`
- If external-engine env vars are available during execution, run the relevant Postgres and MSSQL schema/integration tests that cover qualified-name introspection. If not available, report that engine validation was skipped.
- `workflow.commit_policy` is `ask`, so do not commit unless the human explicitly requests it after review.
- `workflow.archaeology_check` is `required`: begin implementation with a quick code-path audit of every single-string table identity use before editing.

## Stages

- [ ] Stage 001: Re-read `#72` and confirm the narrowed boundary against `#73`, then audit all current single-string table identity flows and settle the internal structured-name model plus additive public API surface.
- [ ] Stage 002: Implement structured schema-aware table and foreign-key identity plus shared qualified-name rendering helpers across `schema` DDL, seeds, indexes, and declared snapshots.
- [ ] Stage 003: Update dialect inspection, live snapshot, ensure, validate, diff, and dependency ordering to operate on schema-qualified identity, including SQLite collision detection.
- [ ] Stage 004: Add or revise tests and docs, run the chosen validation set, and report any remaining API tradeoffs or follow-up work that still belongs in `#73`.

## Completion

Respect repo-local `workflow.commit_policy`:

- `feature_branch`: work on a feature branch, commit explicit files, then wake
  Codex with the commit SHA.
- `ask`: do not commit unless explicitly asked; wake Codex with a short label
  and include changed files, validation, and risks in your reply.
- `never`: do not commit.

## Escalate If

- A correct implementation appears to require a breaking public API change rather than additive fields/helpers.
- Dialect inspection cannot distinguish schema-qualified objects cleanly without a larger interface redesign than this issue warrants.
- Snapshot or JSON compatibility expectations are unclear enough that preserving both backward compatibility and structured schema metadata is not obvious.
- SQLite fallback semantics become ambiguous for existing tests or examples.
- Live Postgres/MSSQL behavior contradicts the issue assumptions during validation and the mismatch cannot be resolved with a localized fix.

## Completion Signal

Wake Codex with `tmux-skills-compact-and-review codex` after reporting changed
files, validation performed or skipped, commit SHA when applicable, and any
unresolved risks.
