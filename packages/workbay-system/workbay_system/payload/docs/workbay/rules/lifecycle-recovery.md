# Lifecycle Recovery

Operator-facing recovery reference for the workflow-block-friction (internal)
epic. Each lifecycle guard exists to protect an invariant; this page is
the single place that names, for every guard, **what it protects** and
**the one-line escape hatch** when it fires on a legitimate operation.

A guard without a documented escape hatch is a guard that surprises.
When a lifecycle gate blocks you and the block looks wrong, find the
surface below, confirm the invariant still applies, and take the listed
escape hatch instead of disabling the guard.

Cross-references:

- [development-workflow.md](development-workflow.md) — the canonical
  workflow loop and Dirty-Main Policy these guards enforce.
- [planning-artifact-home.md](planning-artifact-home.md) — where
  planning artifacts live (relevant to Surface 3).

---

## SessionStart: cold-start orientation recipe

Any harness session-start hook can wire the same cold-start path without
vendor-specific steps:

1. Run `make context` (or call `load_session` directly) at session start.
2. The compound load returns handoff state + open findings + touched files
   + newest compaction advisory + **newest continuation packet** when one
   exists (`data.continuation`: `packet_id`, `saved_at`, `lane_id`,
   `sections`).
3. `make context` prints a `CONTINUATION` section with the four section
   bodies (`done_do_not_redo`, `next_actions`, `verified_anchors`,
   `gotchas`) when a packet is present for the resolved task.
4. Agents author packets via `continuation(operation='save', ...)` before
   ending a session; the next cold start injects the newest packet
   automatically. Pass `last_injected_continuation_id` on later
   `load_session` calls in the same live session to avoid re-injecting
   the full body.

---

## Surface 1: Linked-worktree resolve-gate

- **Guard**: `_resolve_primary_worktree_root`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/config.py`) and
  `_workspace_git_context`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/shared_write_context.py`).
- **Invariant**: an MCP write from inside a linked worktree attributes
  to the task that owns that branch/worktree, not to `main`.
  `RuntimeConfig.for_repo` collapses every linked worktree to the
  primary root, so workspace-root alone cannot tell the rows apart.
- **Escape hatch**: internal split the git-context probe from
  workspace-root resolution; pass `actor={branch, commit_sha}` (or
  `--actor-commit-sha`, internal) on the write so provenance lands on the
  correct row even when cwd resolves to the primary root.

## Surface 2: handoff-close-check view drift

- **Guard**: `handoff_close_check`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/decisions.py`),
  which materializes `CURRENT_TASK.json` from the live workspace summary
  before its in-sync comparison.
- **Invariant**: close-check evaluates the live state of the named task,
  not a stale rendered view carried over from a different worktree.
  Same root cause as Surface 1 (internal).
- **Escape hatch**: the internal git-context split removed the drift; pass
  `task_ref` explicitly so the check binds to the row you intend to gate
  instead of inferring it from cwd.

## Surface 3: check-main-clean planning-artifact over-fire

- **Guard**: `check_main_clean.py`
  (`packages/workbay-system/scripts/hooks/check_main_clean.py`).
- **Invariant**: code mutations on root `main` are blocked, but
  planning artifacts authored on `main` (task plans, `docs/plans/…`)
  must not trip the dirty-main block meant for source changes.
- **Escape hatch**: internal split the state-dirty surfaces so
  planning-artifact paths are warn-only; commit the plan on `main`, or
  move code edits to the feature branch the block is steering you to.

## Surface 4: PreToolUse root-on-main Bash guard

- **Guard**: `guard-bash-main-branch.py`
  (`packages/workbay-system/scripts/hooks/guard-bash-main-branch.py`).
- **Invariant**: code-mutating Bash run from the root worktree while on
  `main` is blocked so work cannot accumulate off-branch; lightweight,
  read-only, or branchless operations are not blocked.
- **Escape hatch**: internal added a lightweight-branch carve-out; start
  or switch to the task's feature branch (`make task-start` /
  `switch_task`) before the mutating command.

## Surface 5: Ambiguous active-task raise

- **Guard**: `_raise_ambiguous` / `AmbiguousWorkspaceContextError`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/shared_primitives.py`)
  and the no-`task_ref` branch of `handoff_close_check`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/decisions.py`).
- **Invariant**: when no `task_ref` is named and multiple live rows
  share the workspace, the system refuses loudly with the candidate list
  rather than silently misrouting the write or gate. An explicit
  `task_ref` is the operator's disambiguation and always wins (honor
  before raise — internal).
- **Escape hatch**: pass `task_ref=<one of the listed candidates>` (or
  `TASK=` on the make target) to bind the operation explicitly.

## Surface 6: CLI cwd workspace-root fallback

- **Guard**: `RuntimeConfig.from_args`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/config.py`).
- **Invariant**: when the workspace root is not declared explicitly, the
  CLI derives it from cwd, so a command run inside a linked worktree
  still binds to the correct `.task-state` instead of an empty or
  primary-root database.
- **Escape hatch**: internal wired the cwd fallback; if cwd is outside any
  registered worktree, pass an explicit workspace/state-dir argument so
  resolution is unambiguous.

## Extra case 1: plan-accept no-op, worktree claim, and context projection

- **Guard**: `plan_accept.py` and `task_start.py`
  (`packages/workbay-system/scripts/workbay/lifecycle/handlers/plan_accept.py`,
  `…/task_start.py`).
- **Invariant**: re-accepting an already-accepted plan is an idempotent
  no-op (not an error); a reviewed untracked plan on root `main` is accepted
  through `plan-accept --local --plan <path> --source-branch main`; an unowned
  existing worktree on the requested branch is claimable rather than a hard
  block; and a `task-start` that creates the branch/worktree must materialize
  the `handoff_state` row.
- **Escape hatch**: re-run `make plan-accept` (no-op is safe); use
  `MODE=claim` to adopt an unowned existing worktree; if the MCP adapter is
  unavailable and `task-start` reports `projection: pending` or `spooled`,
  create the row directly with
  `set_handoff_state(task_ref=…, status="in_progress", …)`.

## Extra case 2: deleted-worktree task-finish / archive

- **Guard**: `task_finish.py`
  (`packages/workbay-system/scripts/workbay/lifecycle/handlers/task_finish.py`)
  and the MCP `archive` operation.
- **Invariant**: finishing or archiving a task whose linked worktree is
  already gone must still set the row to `done`, archive it, and delete
  the merged branch without crashing on the stale worktree pointer.
  Review/audit writes against a torn-down task's `task_ref` are rejected
  by the write-side worktree guard.
- **Escape hatch**: route post-merge audits and review writes through a
  `MAINT-*` task on `main`; run the documented close sequence
  (`set_handoff_state status=done` → archive → dashboard) even when the
  worktree pointer is already stale.

## Restore: owner repo wrongly bare (`core.bare=true` leak)

- **Doctor finding**: `owner_repo_bare`
  (`packages/workbay-bootstrap/src/workbay_bootstrap/coherence.py`).
- **Invariant**: the owner/primary git repository for a bootstrap overlay must
  not carry `core.bare=true` in the shared config when the overlay was
  installed against a working tree — bootstrap git writes must not brick every
  worktree with "this operation must be run in a work tree".
- **Escape hatch** (validated live, implementation note §0): on the **primary** repo root,
  run `git config core.bare false`; verify
  `git rev-parse --is-bare-repository` prints `false` and
  `git worktree list` shows no `(bare)` tag on the main worktree line.
  Re-run `workbay-bootstrap doctor --target <overlay-root>` — the
  `owner_repo_bare` row should clear once the owner is no longer bare.
  A bare primary whose linked worktrees are the only checkouts is a legitimate
  git layout; doctor reports that case as a **warning** (not an error). It
  escalates to an **error** only when the bootstrap manifest at the owner root
  contradicts the bareness by recording any working-tree install kind —
  `source_kind` in `worktree`, `git_overlay`, or `package`
  (`_WORKING_TREE_INSTALL_KINDS` in `coherence.py`), matching the doctor detail
  string.

## Extra case 3: pyenv pytest shim wins over worktree-local `.venv`

- **Guard**: `task_start.py` / `slice_start.py` root-venv provisioning and
  the shared `uv_provisioning.py`
  (`packages/workbay-system/scripts/workbay/lifecycle/handlers/task_start.py`,
  `…/slice_start.py`,
  `…/uv_provisioning.py`); the orchestrator `provision-env` entry point
  (`packages/workbay-system/scripts/workbay/lifecycle/handlers/provision_env.py`).
- **Invariant**: a task worktree with discovered Python package targets carries
  a root `.venv` so bare `pytest` resolves to `<worktree>/.venv/bin/pytest`, not
  a pyenv shim that loads the primary repo's `src` via a `.pth`. Lifecycle-run
  tests prepend `.venv/bin`; a package-less repo no-ops the step rather than
  failing.
- **Escape hatch**: if `command -v pytest` points at pyenv inside a task
  worktree, `source .venv/bin/activate` before direct pytest, or rerun lifecycle
  provisioning (`make slice-start`, or the orchestrator
  `provision-env --worktree <path>` entry point) to rebuild the missing/stale
  root `.venv`.
