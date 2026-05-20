# reorder code object comment front matter so ownership notice renders first

## Source

- User request on 2026-05-20 to reorder generated owned view/procedure
  comment front matter so the chuck ownership warning renders first, the
  caller-level app doc preamble renders second, and the per-object
  `WithDocAnnotation(...)` comment renders third, closest to the SQL body.
- Current render path:
  - `/home/jtye/git/chuck/schema/code_object_options.go`
  - `/home/jtye/git/chuck/schema/view.go`
  - `/home/jtye/git/chuck/schema/procedure.go`
  - `/home/jtye/git/chuck/schema/code_object_options_test.go`
  - `/home/jtye/git/chuck/schema/integration_test.go`
  - `/home/jtye/git/chuck/README.md`
- Current behavior after PR `#98`:
  - stacked comments render with blank lines between blocks
  - validation ignores leading block-comment front matter

## Objective

Change owned code-object rendering order to:

1. `OwnershipNotice`
2. `DocPreamble`
3. per-object `DocAnnotation`
4. SQL payload

while preserving the new readable multi-line spacing and preserving
comment-insensitive validation semantics.

## Scope

- Create or switch to a fresh feature branch from `origin/main`
- Update render helper ordering in `schema/code_object_options.go`
- Update any affected helper comments / godoc so docs match new order
- Update focused unit tests for render order / exact SQL strings
- Update README examples/docs to show ownership notice first
- Run required validation
- Commit explicit files, push branch, open PR targeting `main`, and wake
  Codex with review SHA

## Non-Goals

- No change to comment-insensitive validation semantics landed in PR `#98`
- No rename of `DocPreamble` / `OwnershipNotice` / `DocAnnotation` APIs
- No table-related behavior
- No new metadata/comment feature surface
- No change to replacement/tombstone behavior

## Constraints

- Keep change presentation-only except for exact comment ordering
- Preserve blank-line-separated stacked comment formatting
- Preserve existing behavior when one or more comment fields are empty
- Preserve zero-options / bare helper output
- Respect dirty worktree; do not disturb unrelated `plans/` artifacts
- Touch only files needed for render order, tests, and docs

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
- use focused feature branch for this comment-order change
- commit explicit render/test/doc files
- push branch
- open PR targeting `main`
- do not self-merge from Claude side; Codex will review and merge after CI
- wake Codex with: `$tmux-flow review <commit-sha>`

## Escalate If

- Reordering reveals hidden assumptions in validation stripping or SQL
  generation outside views/procedures
- More than one reasonable public semantic for field order emerges and needs
  human decision
- Branch state cannot be cleanly based on `origin/main`
- Validation fails for unrelated reasons

## Completion Signal

Wake Codex with `tmux-skills-compact-and-review codex` after reporting changed
files, validation performed or skipped, commit SHA when applicable, and any
unresolved risks.
