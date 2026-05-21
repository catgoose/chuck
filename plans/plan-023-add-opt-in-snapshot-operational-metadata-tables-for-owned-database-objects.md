# add opt-in snapshot operational metadata tables for owned database objects

## Source

- User request on 2026-05-21 to plan operational metadata tables for chuck:
  snapshot-style provenance, not column/table annotation metadata and not
  timestamps embedded into generated SQL comments.
- Prior conversation direction:
  - keep generated proc/view comments stable
  - prefer structured metadata over runtime timestamp churn in definitions
  - first pass should favor snapshot tables over append-only history
- Current owned-object apply seams:
  - `/home/jtye/git/chuck/schema/view_lifecycle.go`
  - `/home/jtye/git/chuck/schema/procedure_lifecycle.go`
  - `/home/jtye/git/chuck/schema/code_object_options.go`
  - `/home/jtye/git/chuck/README.md`

## Objective

Design and implement an **opt-in snapshot operational metadata ledger** for
owned database objects so callers can record provenance such as:

- first applied time
- last applied time
- last changed time
- definition hash
- source revision / source repo

without polluting view/procedure definitions and without requiring an
append-only history system in first pass.

## Scope

- Create or switch to a fresh feature branch from `origin/main`
- Add first-pass metadata schema owned by chuck:
  - `chuck_database_metadata`
  - `chuck_object_metadata`
- Keep first pass **snapshot only**
  - one current row per owner in `chuck_database_metadata`
  - one current row per owned object in `chuck_object_metadata`
  - no append-only event/history table yet
- Make feature **opt-in**, not always-on
- Scope object coverage to owned code objects already on explicit apply paths:
  - `ViewDef` apply helpers
  - `ProcedureDef` apply helpers
- Include database-level row updates during successful opt-in apply
- Include object-level upsert/update during successful opt-in apply
- Record at least:
  - `owner`
  - object type
  - schema name
  - object name
  - `first_applied_at_utc`
  - `last_applied_at_utc`
  - `last_changed_at_utc`
  - `definition_hash`
  - optional `source_repo`
  - optional `source_rev`
  - optional `tool_version`
- Keep metadata updates tied to successful apply only
- Add focused unit/integration coverage and README docs
- Run required validation
- Commit explicit files, push branch, open PR targeting `main`, and wake
  Codex with review SHA

## Non-Goals

- No append-only `chuck_apply_history` table in this pass
- No table/column annotation feature
- No table `Ensure(...)` / drift-engine integration in this pass
- No automatic backfill for objects not applied through chuck
- No metadata timestamps embedded into proc/view definition comments
- No cross-process locking/distributed migration system
- No vendor-specific extended-property implementation as primary feature

## Constraints

- Feature must be opt-in and explicit
- Preserve existing bare apply/validate behavior when metadata option not used
- Prefer structured typed columns over EAV junk drawer
- Keep first pass small enough to fit one implementation plan
- Respect repo config: `feature_branch`, tests required, philosophy if present
- Respect dirty worktree; do not disturb unrelated `plans/` artifacts
- Touch only files needed for API, metadata apply logic, tests, and docs
- If metadata table creation needs bootstrap ordering, keep it caller-honest
  and explicit rather than hidden magic where possible

## Validation

Respect repo-local `.agents/tmux-skills*.json` for tests/commits.

- `go test ./...`
- `golangci-lint run --timeout=5m ./...`

## Completion

Respect repo-local `workflow.commit_policy`:

- `feature_branch`: work on a feature branch, commit explicit files, then wake
  Codex with the commit SHA.
- `ask`: do not commit unless explicitly asked; wake Codex with a short label
  and include changed files, validation, and risks in your reply.
- `never`: do not commit.

For this repo:

- branch from `origin/main`
- use focused feature branch for metadata snapshot work
- commit explicit API/logic/test/doc files
- push branch
- open PR targeting `main`
- do not self-merge from Claude side; Codex will review and merge after CI
- wake Codex with: `$tmux-flow review <commit-sha>`

## Escalate If

- Best API surface for opt-in metadata is ambiguous enough to need human
  choice. Examples:
  - new metadata options object layered onto existing apply helpers
  - dedicated wrapper helpers like `ApplyViewsWithMetadata(...)`
  - metadata manager object separate from `CodeObjectOptions`
- Supporting tables in first pass proves materially cleaner than code-objects
  only and needs scope decision
- Cross-dialect table DDL for metadata ledger becomes awkward enough to need
  phased delivery
- Validation fails for unrelated reasons
- Branch state cannot be cleanly based on `origin/main`

## Completion Signal

Wake Codex with `tmux-skills-compact-and-review codex` after reporting changed
files, validation performed or skipped, commit SHA when applicable, and any
unresolved risks.
