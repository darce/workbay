# Development Workflow

Operator-facing reference for the workflow rules enforced by the
workbay-system git hooks. Hook scripts cite this file by path and anchor
when they block a commit, push, or PreToolUse so operators have a
single place to find the protocol that fired.

## Canonical Workflow Loop

The default workflow for both operators and agents is a six-step loop.
Each step has exactly one Make entry point; the JSON form of every loop
target is `LIFECYCLE_ARGS=--json` (raw `make <target> --json` is parsed
by Make as a target list, not a flag, and silently does the wrong
thing):

```bash
make status LIFECYCLE_ARGS=--json
make tasks LIFECYCLE_ARGS=--json
make plan-accept TASK=<task-ref>
make task-start TASK=<task-ref> OBJECTIVE="..."
make review-ready LIFECYCLE_ARGS=--json
make close-check LIFECYCLE_ARGS=--json
```

**Loop discipline** (rationale in [engineering-heuristics.md](engineering-heuristics.md)):

- **Compile-test-commit cadence** — tiny verified steps; on surprise failure revert to last green and retry smaller.
- **Self-testing code prerequisite** — no safe refactoring/slicing without an automatic self-checking suite before entering the loop.
- **CI over long-lived branches** — integrate ≥ daily; long branches defer conflict → merge hell (red-flag).
- **Deployable ≠ releasable** — ship deployable units continuously with unfinished features hidden behind flags.
- **Near-green / revert-on-surprise** — when a step fails unexpectedly, revert to last green rather than debugging forward in a dirty tree.

For repos that adopt internal's eval surface, the root operator checks are:

```bash
make evals-list LIFECYCLE_ARGS=--json
make evals-run SUITE=lifecycle-smoke [TASK=<task-ref>] LIFECYCLE_ARGS=--json
```

Suites that opt into handoff mirroring only write review findings when the runner is given a concrete `TASK=<task-ref>`; repo-wide dry runs may still update the eval ledger and dashboard without creating task-scoped findings.

1. **status** — orient on the current checkout. From root `main` the
   receipt classifies the checkout as the control plane and points at
   `make tasks` or `make task-start`. From a task worktree it points at
   the implementation or review gates.
2. **tasks** — list active tasks; the natural follow-up from `status`
   when more than one task may be in flight.
3. **plan-accept** — for plan-backed tasks, land the reviewed plan as
   the accepted plan baseline on root `main` before opening or resuming
   implementation. `task-start`, `review-ready`, and `close-check`
   report `plan_baseline_missing` when a plan-backed task has no accepted
   baseline; the recovery command is `make plan-accept TASK=<task-ref>`.
   When the reviewed plan is a brand-new untracked draft already on root
   `main`, use the receipt's explicit `LIFECYCLE_ARGS="--json --local --plan
   <path> --source-branch main"` form so `plan-accept` commits only that plan
   file before `task-start` creates the implementation worktree.
4. **task-start** — create or reuse a task feature branch + worktree,
   then continue implementation in that worktree, not in root `main`.
5. **review-ready** — block on missing tests, open findings, missing
   accepted plan baseline, or contract drift before requesting review.
6. **close-check** — final merge-readiness gate; failures are reported
   by owner (feature branch / handoff evidence / mergeability /
   root-main hygiene).

Routine implementation does not require `cat DASHBOARD.txt`,
`sqlite3 .task-state/handoff.db`, or raw MCP calls before this loop.

## Root `main` is a Control Plane

The root checkout on `main` (or `master`) is the **control plane**: a
read-mostly orientation surface used to plan, list tasks, and decide
what to enter next. It is **not** a shared implementation workspace.

- Code-bearing edits belong in a task worktree created or discovered
  through `make status` / `make task-start`. The PreToolUse and
  pre-commit gates block edits to protected paths on `main` outright;
  this rule documents the workflow intent behind those guards.
- Operator artifacts (planning documents, generated dashboards, derived
  receipts) may live on root `main` when they are explicitly part of
  the control plane.
- Independent harnesses sharing the same root checkout should expect to
  see one another's drift surfaced as warnings, not as hard blocks
  during routine work. Hard dirty-main blocks fire at publish/close
  boundaries (see Dirty-Main Policy below) where an integration
  decision is already being made.

## Implementation Plane (task worktrees)

Each task feature branch lives in its own worktree under
`<repo-parent>/workbay-<task-slug>/`. The
implementation plane is where:

- code-bearing edits happen,
- TDD slices run (`make slice-start` / `make slice-commit`),
- `make review-ready` and `make close-check` evaluate readiness.

`make task-start` provisions a worktree-root `.venv` (pytest plus the
discovered packages installed editably) so the worktree is self-contained:
after `cd <target_worktree_path>`, bare `pytest` resolves to
`<worktree>/.venv/bin/pytest` rather than a pyenv shim. Lifecycle commands
like `make slice-start` prepend `.venv/bin` automatically; for direct pytest
runs, `source .venv/bin/activate` first. If `command -v pytest` still points at
pyenv inside a task worktree, the venv is missing or stale — rerun lifecycle
provisioning (`make slice-start`, or the orchestrator `provision-env --worktree
<path>` entry point) to rebuild it. In a package-less repo the provisioning step
no-ops, so this only matters where Python package targets exist.

### Post-provision worktree bootstrap (`LIFECYCLE_WORKTREE_BOOTSTRAP`)

After Python provisioning and the best-effort bootstrap overlay adopt,
`make task-start` can run a consumer-authored shell command for
*non-Python* per-worktree setup (for example `npm install` for a plugin
app). This is **runner-invoked and automatic** on the `make task-start`
path — analogous to `WORKBAY_ADOPT_CMD`, not to `LIFECYCLE_FORMATTER`
(which is agent-invoked via `make format`).

- **Consumer surface (Make):** set `LIFECYCLE_WORKTREE_BOOTSTRAP` in your
  root `Makefile` (empty default = feature off). Example:
  `LIFECYCLE_WORKTREE_BOOTSTRAP = cd apps/my-app && npm install`.
- **Runner surface (env):** the `task-start` recipe forwards the Make var as
  `WORKBAY_WORKTREE_BOOTSTRAP_CMD`. The runner shells out with `sh -c`
  and **cwd = worktree root**, so relative `cd …` paths in the command
  resolve correctly.
- **Semantics:** best-effort, never fatal — a failed bootstrap does not roll
  back the worktree, branch, or handoff row; `task-start` still returns
  `ok: true` with `worktree_bootstrap.ok=false` in the receipt. Commands
  must be idempotent (reused/claimed worktrees re-run the hook). Default
  timeout is 600s; override with `WORKBAY_WORKTREE_BOOTSTRAP_TIMEOUT`
  (seconds). Trigger coverage: `MODE=worktree` and `MODE=claim` only;
  `MODE=here` is excluded. MCP/orchestrator worktree creation does not
  fire this hook in v1.

Switching between tasks means switching worktrees, not carrying
uncommitted changes across tasks. If the wrong task is active, use
`switch_task` (or close one task before starting another) instead of
crossing branch boundaries on a single shell.

## Pre-Worktree MCP Writes

An implementation task row is not write-ready until its `target_branch`
has a real linked worktree. Before that point, MCP writes that need a
write actor grounded to the implementation worktree can fail with
`WorktreeNotFoundError`.

Do not use `set_handoff_state`, `record_event`, `close_slice`,
`review_findings`, or `review_runs` against a pre-worktree implementation
row. Use one of these safe choices instead:

1. Run `make task-start TASK=<task-ref> OBJECTIVE="..."` once the accepted
  plan baseline exists.
2. Use a `target_branch=main` `MAINT-*` task for planning/review work that
  must be recorded before implementation starts.
3. Wait until the feature worktree exists and is the cwd, then write MCP
  state against the implementation task.

## Cross-Harness Compaction Ownership

Cross-harness compaction is split across three surfaces on purpose:

- `docs/workbay/contracts/harness-protocol.yaml` owns the portable data contract: advisory field name, thresholds, transcript discovery rules, and unknown-harness behavior.
- `skills/handoff-lifecycle/body.md` owns the agent-facing response guidance for that contract, including when to call `compaction(operation="record", ...)` after an MCP response advertises `compaction_recommended: true`.
- Generated per-harness adapters remain artifacts, not sources. Regenerate them with `make generate-agent-workflows`; verify drift with `make check-agent-workflows`. Both targets must be invoked from the monorepo root and require a Python interpreter with PyYAML installed; `Makefile.d/workflows.mk` auto-selects the pyenv-managed project interpreter when present, and the `WORKFLOWS_PYTHON=...` override exists for environments without pyenv.

Do not move compaction thresholds or transcript discovery into harness-specific settings files. The contract stays canonical in `harness-protocol.yaml`, the prose stays canonical in `skills/handoff-lifecycle/body.md`, and emitted harness surfaces consume those sources downstream.

## Workspace Cardinality and Task Kinds (internal)

The handoff DB (`handoff_state` rows) is multi-task by design: more than
one task can be live concurrently. The per-workspace projection at
`CURRENT_TASK.json` honors that cardinality with three explicit shapes
— `single`, `workspace_ambiguous`, `none` — but the workflow policy
still segregates *kinds* of work by where they run.

Pinned discriminator (CTP-internal): a task is treated as
*planning/maintenance* iff its `target_branch == "main"`; otherwise it
is *implementation*. The two kinds map onto two cardinality rules:

- **Implementation tasks → per-worktree singleton (internal
  refinement).** Each *linked worktree* owns at most one live
  implementation task. Multiple implementation tasks may coexist in the
  same workspace as long as each lives in its own linked worktree —
  the singleton is scoped to a worktree, not to the workspace as a
  whole. `task-start` refuses only on *real* conflicts (see
  *Task-Start Identity Resolution* below); the ambiguity guard projects
  an `ambiguity_resolved` decision with a structured `conflict_kind`
  and exits non-zero before any git mutation.
- **Planning / maintenance tasks → multi-on-main.** Any number of live
  planning/maintenance tasks may share the root `main` workspace. They
  do not contend for a feature-branch checkout, so the workspace
  summary surfaces them as `workspace_ambiguous` rather than collapsing
  to one.
- **Always pass `task_ref` for planning/maintenance.** Readers and
  writers operating on `main` must name the task explicitly (CLI
  `--task`, MCP `task_ref`, or `AGENTIC_LANE_ID` env binding). The
  workspace summary cannot pick "the" task for the operator when
  several are concurrent — the four-step Resolution Rule fails closed
  with the structured ambiguity list rather than guessing.

Cross-kind concurrency on the same workspace path (e.g. an
implementation task starting while planning rows are live on `main`)
is allowed: the new feature-branch worktree is a *sibling* path and
does not displace the on-main rows. The ambiguity guard accordingly
falls through OQ2 case 4 (`single` + active planning/maintenance + new
implementation request → allow).

### Task-Start Identity Resolution (internal)

internal supersedes the pre-existing `workspace_ambiguous` veto with a
claim-aware identity resolver. `task-start` now follows a five-step
**Resolution Order** to determine which task this invocation refers to,
then refuses only when a *real* conflict is found:

1. **Explicit `TASK=<ref>` / `--task <ref>`.** When the caller passes a
   ref, that ref is authoritative for *identity*. Steps 2–4 inform
   conflict checks, not identity choice.
2. **Live row whose `target_worktree_path` matches the current
   worktree.** Used when no explicit ref is supplied; identifies "the
   task this shell is attached to."
3. **Current branch resolved via registered live refs.** Uses
   `select_task_ref_candidate(branch, known_task_refs=live_refs)` when
   importable from `workbay_protocol.branch_naming` (internal); falls
   back to shortest-prefix derivation otherwise.
4. **Unique workspace summary.** When the summary resolves to
   `shape="single"`, that single task is the identity.
5. **Fail with structured choices.** When none of 1–4 produces an
   unambiguous identity, refuse with a list of candidate task refs so
   the operator can re-issue with `TASK=`.

`workspace_ambiguous` is now an **index, not a veto**: its role is to
enumerate currently-live tasks so the guard can detect real conflicts,
not to refuse on shape alone.

#### Real-conflict taxonomy

The guard refuses on exactly four kinds of conflict, partitioned by
**category**:

- **Resource collisions** — name clashes detectable from git /
  filesystem state alone. Operator remediation is rename / choose a
  different path or branch.
  - `branch_collision` — requested `target_branch` is already attached
    to a different task's linked worktree, or already exists as a
    local branch.
  - `worktree_path_collision` — derived (sibling-of-primary)
    `target_worktree_path` already exists / is owned by a different
    task.
- **Policy conflicts** — no name clash, but a worktree-singleton-class
  rule forbids the start. Operator remediation is switch task or
  abandon the start.
  - `same_task_elsewhere` — the requested `task_ref` is already live in
    a different worktree.
  - `mode_here_implementation_conflict` — `MODE=here` against a primary
    checkout currently attached to a different implementation
    (non-`main`-target) task.

On refusal, `task-start` emits `error == "task_ref_ambiguous"` (the
existing string is preserved so hooks and substring matchers continue
to fire) plus additive `conflict_kind` and `conflict_category` fields.
The additive fields are populated **only** when the refusal comes from
the internal `_detect_real_conflict` path (i.e.
`pre_view.shape == "workspace_ambiguous"`); they identify the exact
conflict kind and category and the audit decision row embeds the same
fields plus the specific conflicting ref / branch / path. The
single-shape worktree-singleton refusal (`pre_view.shape == "single"`,
active implementation task differs from request) does not run
`_detect_real_conflict` and therefore emits both fields as `None` — the
receipt still carries them for shape stability, but the discriminator
on that path is the `error` string alone.

#### MODE=here decision table

For `MODE=here`, the guard's behavior depends on what the primary
checkout's HEAD is currently attached to (`main` is the
planning/maintenance discriminator, per CTP-internal):

| `pre_view.shape` | primary HEAD attached to …                              | outcome  | refusal kind                          |
| ---------------- | ------------------------------------------------------- | -------- | ------------------------------------- |
| `single`         | implementation task (different from requested)          | refuse   | `mode_here_implementation_conflict`   |
| `single`         | planning/maintenance task (`target_branch == "main"`)   | allow    | —                                     |
| `single`         | requested task itself                                   | allow    | — (idempotent re-checkout)            |
| `workspace_ambiguous` | implementation task (different from requested)     | refuse   | `mode_here_implementation_conflict`   |
| `workspace_ambiguous` | planning/maintenance task (`target_branch=="main"`)| allow    | —                                     |
| `workspace_ambiguous` | requested task itself                              | allow    | — (idempotent)                        |
| `none`           | any                                                     | allow    | — (no live row to displace)           |

`MODE=worktree` ignores primary-HEAD attachment entirely — its
conflicts are about the *target* worktree path and branch claim, not
the primary checkout.

### `current_task_auto_regen` flag re-scope

The `current_task_auto_regen` flag in
`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/config.py` is
**re-scoped** by internal (CTP-internal + CTP-PR-T3-01):

- Per-task projection writes to `.task-state/current/<task_ref>.json`
  are **always-on** on every task-affecting DB write
  (`set_handoff_state`, `close_slice`, `archive`)
  regardless of the flag — derive-on-read needs them as the source of
  truth.
- The flag now only controls whether the workspace-summary
  `CURRENT_TASK.json` is **also** eagerly re-derived-and-written
  alongside each per-task write. Default `False` keeps
  `CURRENT_TASK.json` derive-on-read only; `True` opts cold-start
  readers (e.g. raw `cat CURRENT_TASK.json`) into eager refresh.
- DASHBOARD.txt is unaffected: it remains the always-current operator
  surface (CTP-PR-T2-03).

### Per-task projection lifecycle

Each `.task-state/current/<task_ref>.json` file is reaped on
`archive` of the task it projects (the writer matches the existing
single-active reaper behavior for the legacy snapshot). Absence of
the file is the canonical "not live" signal that
`snapshot_is_live_for_task` and the workspace-summary derive both
consume.

### Canonical "what's active here" surface

`make tasks` is the canonical way to ask "which tasks are currently
live in this workspace?" — it queries the live `handoff_state` rows
DB-direct, never the file projection, and is therefore correct under
all three projection shapes (including `workspace_ambiguous`, where
`CURRENT_TASK.json` deliberately refuses to collapse to a single
active block). Use `make tasks` instead of reading
`CURRENT_TASK.json` for that question; readers that still consume the
file go through `_common.load_workspace_summary` and branch on
`view.shape`.

## Dirty-Main Policy

A "dirty" root `main` is a working tree where one of the **protected
paths** declared in `harness-protocol.yaml` (`branch_isolation` →
`code_roots`, `protected_extensions`, `protected_main_surfaces`,
`root_protected_files`) has uncommitted modifications, untracked content,
or staged changes. The set of protected paths is the same set the
PreToolUse, pre-commit, and pre-push gates already enforce; the
dirty-main policy is the routine-vs-publish severity matrix layered on
top of that detection.

Severity follows a single mode axis (`warn`, `doctor`, `block`) so
`make doctor`, the git hooks, and `make close-check` can route on the
same value:

| Boundary                            | Mode      | Effect                                                                                    |
| ----------------------------------- | --------- | ----------------------------------------------------------------------------------------- |
| Routine work on root `main`         | `warn`    | `make doctor` surfaces dirty protected paths under `dirty_main`; no command is blocked.   |
| Operator triage requested           | `doctor`  | `check_main_clean.py --mode doctor` returns the same finding shape for scripted recovery. |
| Publish boundary (`pre-push`)       | `block`   | Push refuses to leave the local repo while protected paths on `main` are dirty.           |
| Close boundary (`make close-check`) | `block`   | Close gate refuses ready-to-merge while protected paths on root `main` are dirty.         |

Routine merges and post-checkout transitions stay warn-only on purpose:
`post-merge` was retuned to `--mode doctor` in internal so a
shared root checkout used by independent harnesses no longer fails
operations during normal integration. The hard blocks fire only at the
two boundaries where an integration decision is already being made
(`pre-push`, `close-check`).

The remediation command is `make doctor LIFECYCLE_ARGS=--json`. The
`dirty_main` facet on the doctor receipt names every dirty protected
path, the recommended mode, and a concrete `remediation` action list
(stash, drop, commit on a feature branch, or `switch_task` into the
worktree that owns the change). Operators should not edit protected
paths on root `main` to "clear" the gate; the policy assumes the dirty
content belongs in a task worktree and the doctor remediation is what
moves it there.

## Branch Isolation Protocol (mandatory)

All implementation work happens on a task-scoped feature branch. The
hooks classify the current branch into one of three buckets and behave
differently for each.

### Protected branches

The following branches are **protected**: code-bearing edits are
blocked outright (no override). Branch-naming gates do not fire on
these because they are not "non-conforming" — they are off the
implementation grammar entirely.

- `main`, `master`
- Any branch under `release/*` or `hotfix/*`

To work on protected-branch hotfix/release work, create a dedicated
release/hotfix branch and follow the project's release procedure.

### Conforming branches

A **conforming** feature branch matches the canonical regex exported
by `workbay_protocol.branch_naming.TASK_REF_RE`:

```
feature/<task-ref>-<slug>
```

with the rules:

- Lowercase only.
- The `<task-ref>` segment must contain at least one digit
  (e.g. `internal-37`, `internal-3`, `epic-12-3`).
- The `<slug>` is `[a-z0-9-]+`.

Examples that pass: `feature/internal-37-pre-push-mirror`,
`feature/internal-3-tool-surface`. Examples that reject:
`feature/bad-name` (no digit), `Feature/internal-x` (uppercase).

The four-layer naming gate (post-checkout warn, PreToolUse block,
pre-commit block, pre-push block) all import this regex by reference
from `workbay_protocol.branch_naming`. A grammar tweak there
propagates to every gate with no other code change required.

### Non-conforming branches

A branch that is neither protected nor conforming. The four gates
behave as follows:

| Gate           | Behavior on non-conforming                                  |
| -------------- | ----------------------------------------------------------- |
| post-checkout  | Warn-only. Includes a "did you mean…" suggestion.           |
| PreToolUse     | Block code-bearing tool calls.                              |
| pre-commit     | Block (override env: `AGENTIC_ALLOW_NONCONFORMING_BRANCH`). |
| pre-push       | Block (override env: `AGENTIC_ALLOW_NONCONFORMING_BRANCH_PUSH`). |

The commit-side and push-side override env vars are intentionally
distinct so that commit-side leniency does not silently leak across
the publish boundary. To push a non-conforming branch, the operator
must re-assert the decision at push time.

#### Override env vars (audited)

Both overrides are audited via a recorded MCP decision event. Set the
override variable to `1` to permit the operation; optionally provide a
human-readable reason in the matching `_REASON` variable so the audit
record explains why.

| Variable                                       | Effect                                                                        |
| ---------------------------------------------- | ----------------------------------------------------------------------------- |
| `AGENTIC_ALLOW_NONCONFORMING_BRANCH=1`         | Allow a single non-conforming **commit**. Override audited.                    |
| `AGENTIC_ALLOW_NONCONFORMING_BRANCH_REASON`    | Free-text reason recorded with the commit-side override decision.             |
| `AGENTIC_ALLOW_NONCONFORMING_BRANCH_PUSH=1`    | Allow a single non-conforming **push**. Override audited.                      |
| `AGENTIC_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON` | Free-text reason recorded with the push-side override decision.             |

The override path attempts to record the decision through the running
MCP handoff server. On timeout (>2s), DB contention, or any error, the
override never blocks the operator: the decision is appended to a
JSONL fallback log so the audit trail is preserved out-of-band.

#### Audit log paths

Override decisions are recorded as MCP handoff events when the server
is reachable. When the live recording fails or times out, the override
falls back to disk:

- Commit-side fallback: `.task-state/branch_naming_overrides.log`
- Push-side fallback:   `.task-state/branch_naming_push_overrides.log`

Each line in those files is a JSON record with the branch name, the
operator-supplied reason, and a timestamp. Operators investigating an
override-related incident should look in both the MCP decision log and
these JSONL files; the union is the full audit trail.

#### Fixture-only env vars (un-audited)

A small number of env vars exist purely to let synthetic fixture repos
exercise the hook without a live MCP. They are **not** audited — no MCP
decision is recorded — because they only suppress warnings, never an
enforcement gate. Do not export them outside of test fixtures.

| Variable                              | Effect                                                                                                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKBAY_SKIP_ACTIVE_TASK_PROBE=1`  | Skip the maintenance-task warning probe in `scripts/hooks/guard-main-branch.sh`. Consumed by `scripts/check_harness_sync.py::_fixture_env` so its synthetic monorepo fixture does not need a live `mcp-workbay-handoff` CLI to pass. Warning-only suppression; the protected-paths block still fires. |

## Session Compaction

The workbay-system Stop hook
(`packages/workbay-system/scripts/hooks/compact-session.py`) writes one
`session_compactions` row per turn-end so the next process resuming
the task starts from a structured cold-start summary instead of an
opaque transcript. The hook is wired into the harness via
`workbay-bootstrap install`, which walks the v2
`config/agent-workflows/portable_commands.json` manifest and applies
each `compact-session` adapter row whose opt-in flag was passed on the
command line. Claude Stop adapters are not written by default — pass
`--install-claude-stop-hook-local` to write the user-owned (gitignored)
`<target>/.claude/settings.local.json` adapter. Under wholesale Project
ownership, `.claude/settings.json` is contract-managed; opt-in Stop hooks
must target Local so they never conflict with the managed file. Each merge
is idempotent (sentinel-tagged `_managed_by: workbay-bootstrap`).

The PostToolUse(Bash) `capture-agent-errors` hook is **managed by default**
in the wholesale `.claude/settings.json` contract — no install flag is
required. Example dogfood install when opting into the Stop recorder:

```bash
make dogfood DOGFOOD_INSTALL_FLAGS="--install-claude-stop-hook-local"
```

Harness attribution for captured errors comes from launcher-level
`WORKBAY_HANDOFF_HARNESS` (defaults to `claude-code` when unset) — the
installed settings entry does not bake an inline harness export.

Three env vars throttle the hook without requiring a code change.
Defaults preserve pre-internal behavior: every turn-end with at least
one new turn writes a row. Canonical names share the package's
dominant `AGENT_HANDOFF_*` prefix; the legacy `WORKBAY_COMPACTION_*`
spellings are accepted as **deprecated** one-release aliases and
emit a stderr warning naming the canonical replacement when they
are the resolved source. See
`docs/workbay/environment-variables.md` for the cross-package
registry.

| Variable                                   | Legacy alias (deprecated)         | Default | Effect when set                                                                 |
| ------------------------------------------ | --------------------------------- | ------- | ------------------------------------------------------------------------------- |
| `WORKBAY_HANDOFF_COMPACTION_DISABLED`        | `AGENT_HANDOFF_COMPACTION_DISABLED`, `WORKBAY_COMPACTION_DISABLED`       | unset   | Truthy value silences both surfaces: Stop hook short-circuits with `compaction skipped: disabled (source=env)` and `compute_compaction_advisory` returns `disabled=true,disabled_source="env"`. internal also exposes a writable DB equivalent via `make compaction-disable\|compaction-enable\|compaction-status [TASK=<ref>]` (workspace-default if `TASK` omitted). Resolver precedence: env → task row → workspace row → enabled. |
| `WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TURNS`   | `AGENT_HANDOFF_COMPACTION_MIN_NEW_TURNS`, `WORKBAY_COMPACTION_MIN_NEW_TURNS`  | _(retired)_ | **Retired** as a trigger (implementation note / D1) — no longer read from env; field kept on `CompactionSettings` (default `0`) for backward compat only, no live gating. |
| `WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS`  | `AGENT_HANDOFF_COMPACTION_MIN_NEW_TOKENS`, `WORKBAY_COMPACTION_MIN_NEW_TOKENS` | contract `threshold_tokens` | Sole trigger gate: write a compaction once the new-turn slice (cl100k_base) reaches the threshold. When unset, default resolves env → overlay `compaction.thresholds.tokens` → `harness-protocol.yaml` `compaction.threshold_tokens` → code constant. `0` = never fire the token gate (skip every turn); `1` = documented no-gate idiom; use `…_DISABLED` to silence entirely. |
| `WORKBAY_HANDOFF_COMPACTION_THRESHOLD_TOKENS` | _(none)_ | contract `threshold_tokens` | Advisory-only token-threshold override (env > overlay > contract). Stop-hook gate uses `MIN_NEW_TOKENS` when set; otherwise overlay `compaction.thresholds.tokens`, then contract `threshold_tokens`. |
| `WORKBAY_HANDOFF_COMPACTION_THRESHOLD_CHARS` | _(none)_ | contract `threshold_chars` | Advisory-only override for the char threshold (env > overlay > contract). |
| `WORKBAY_HANDOFF_ACTIVE_TASK` | _(none)_ | unset | Pin active `task_ref` when multiple live rows share the workspace (hooks only; MCP writes still raise on ambiguity unless `task_ref` is explicit). |
| `WORKBAY_HANDOFF_COMPACTION_NOTIFY`      | _(none)_                             | `1` (on) | Emit a user-visible one-line notification when a compaction fires (Stop hook) and on session-start reinjection. `claude-code` gets a `systemMessage` JSON envelope; other harnesses keep the stderr line / raw block. Falsy suppresses only the notification. |

**Caution:** the legacy aliases are documentation-only — the current resolver
reads ONLY the `WORKBAY_HANDOFF_COMPACTION_*` names (no fallback). An alias
left set in the environment is silently ignored; rename it to the canonical
name.

Invalid integer values for the threshold knobs no longer fall back
to defaults silently — `workbay_handoff_mcp.CompactionSettings.from_env()`
raises a Pydantic `ValidationError` at the hook boundary, which the
Stop hook surfaces as a one-line `compaction failed: invalid
compaction settings: ...` on stderr (still exit 0, never blocks the
turn).

When the hook is disabled or skipped and an operator wants to force a
compaction anyway — for example after a long idle window or when
debugging the cold-start render — use the manual launcher:

```
make compact-now TASK=<task-ref> [TRANSCRIPT=<path>] [HARNESS=manual]
```

`compact-now` shells through
`uvx --from mcp-workbay-handoff python -m workbay_handoff_mcp.compaction_cli`,
writes one `session_compactions` row, and prints
`compaction_id=C-<task-ref>-NNNN` to stdout. Omitting `TRANSCRIPT`
falls back to the most recent file under
`~/.claude/projects/<workspace-slug>/`.

### Enabled vs wired: compaction status is not recorder wiring

`compaction(operation="status")` reporting `disabled=false` means the internal
compaction **surface** is active: `compute_compaction_advisory` evaluates the
thresholds and manual recording (`compaction(operation="record")` /
`make compact-now`) works. It does **not** mean an automatic recorder is wired.
Automatic stop-time recording only runs when the relevant harness
`compact-session` Stop adapter is installed (see the opt-in flags above), so an
operator can read a clean compaction status while the automatic path is inert.
Enabling compaction and installing an automatic recorder are two independent
facts — the env vars and DB enable/disable rows above never install a Stop
adapter.

### Enabled vs wired vs reinjected: context re-injection is a third fact

The `reinject-context` SessionStart hook is also opt-in and independent from
both compaction enablement and Stop-hook recorder wiring. A task can have
compaction enabled, no automatic Stop recorder installed, and no SessionStart
re-injection installed; each state is valid and diagnosed separately.

`workbay-bootstrap install --install-claude-reinject-hook-local` writes the
user-owned `.claude/settings.local.json` SessionStart adapter (the only
supported opt-in path under wholesale Project ownership). The hook is
fail-open: disabled
compaction, missing state, malformed SessionStart input, or unavailable MCP
runtime produce a single `reinject skipped: <reason>` stderr line and no stdout
block.

The hook emits a fenced `workbay-reinject` block only for enabled
SessionStart sources. By default that means `compact,resume`; operators may set
`WORKBAY_REINJECT_SOURCES` to a comma-separated allow-list, and
`WORKBAY_REINJECT_BUDGET_CHARS` to cap total stdout chars (default `1500`).
`get_handoff_state` also exposes the additive `latest_compaction_id` field so
long-running consumers can cheaply dedupe the latest compaction packet.

`make doctor LIFECYCLE_ARGS=--json` resolves the gap. Its `hooks` facet reports,
per harness, whether each `compact-session` Stop adapter is **installed**,
**drifted** (`stop_adapters_drifted`), or **optional-not-installed** — a known
adapter the operator never opted into, which is visible but is not an error and
never turns the doctor run red. Drift comes from the bootstrap doctor finding
`hook_adapter_drift`, which is emitted only when `workbay-bootstrap` previously
installed the managed adapter and the file is missing or no longer matches the
manifest-declared entry; `workbay-bootstrap repair` restores it. First-time
installation stays behind the opt-in flags — doctor diagnoses, it never installs.

The same `hooks` facet reports repo git-hook **hoist readiness**: whether the
hook scripts reached through `core.hooksPath` are materialized in the inspected
checkout. Creating a linked worktree can succeed while inherited git hooks print
warnings about missing hoisted hook scripts in the new checkout. That warning is
a **non-fatal** hook-readiness diagnostic, not a task-start failure: the branch
and worktree were created. Run `make doctor` in the new worktree to see the
missing-hoist state and the remediation command rather than treating the
warning as a broken start.

## Decision IDs

The MCP handoff server records discrete decisions throughout the task
lifecycle (slice completions, override grants, review verdicts, etc.).
Decision IDs follow a stable grammar so downstream gates and dashboards
can pattern-match without parsing free text.

### Grammar

```
<actor_tag>_<decision_kind>_<task_ref>_<slug>
```

- `<actor_tag>` — short identifier for the recording actor
  (`claude`, `codex`, `human`, `hook`, etc.). Lowercase, `[a-z0-9_]+`.
- `<decision_kind>` — what kind of decision this is. The closed set
  the gates currently look for:
  - `slice_complete` — emitted at the end of every TDD slice.
  - `branch_review_<verdict>` — `pass`, `pass_with_findings`,
    `conditional_pass`, `fail`.
  - `planning_review_<verdict>` — same verdict set.
  - `nonconforming_branch_override` — emitted by the branch-naming
    gates when an audited override is granted.
- `<task_ref>` — the canonical task ref (lowercased, dashes preserved
  where the original used dashes; underscores otherwise).
- `<slug>` — short hyphen-or-underscore separated descriptor of the
  decision content (e.g. `internal_37_pre_push_mirror`).

### Examples

```
claude_slice_complete_internal_pre_push_mirror
hook_nonconforming_branch_override_internal_late_commit
codex_branch_review_internal_pass_with_findings
```

### Why the format matters

The post-loop close gate (`integrity_check(payload={"kind":"close"})`) and the
`current_commit_handoff` consistency check both walk the decision log
looking for slice-complete records associated with the current HEAD.
Without a parseable `slice_complete` decision tied to the right
commit_sha, these gates fail and the task cannot close cleanly. The
`auto-fix` skill's slice-complete recording step uses this grammar
explicitly for that reason.

## Checklist Sync

Task plans accumulate `- [ ]` checkboxes across `## Context and
Ownership`, `### Checklist for Slice N: ...`, `## Review Readiness`,
`## Stretch Goals`, and `## Success Criteria`. The
`sync-task-plan-checklist` handler (internal) keeps those boxes
honest by projecting handoff DB state onto the plan markdown:

- **Granular.** Every flipped box traces to a specific DB record —
  a `close_slice` decision's `changed_files`, a
  `record_event(event_kind="test_result")` `command`, or an explicit
  decision id reference. No "all done when status=done" sweep.
- **Stretch never auto-ticks.** Items under `## Stretch Goals` are
  filtered before resolution; they stay `- [ ]` until a human opts in.
- **One-way ratchet.** Boxes only go `- [ ]` -> `- [x]`. A box that
  should never have been ticked stays ticked; manual fix only.
- **Idempotent + dry-run-by-default.** Running twice on a plan that
  did not change produces zero diff. The bare invocation prints a
  diff and exits 0; `APPLY=1` mutates.

Invocation surfaces:

- `make slice-commit` and `make task-finish` invoke the sync as a
  recipe-level post-step (per-slice and full-plan sweeps respectively).
- `make sync-task-plan-checklist TASK=<ref> [APPLY=1] [PLAN=<path>]`
  for manual / debug runs. `PLAN=` defaults to the task's stored
  `handoff_state.task_plan_path`.

If a box did not auto-tick, the evidence anchor in the item body did
not resolve to a DB record — verify the item references a
backtick-quoted file path, a `make <target>`, a `Slice N` reference,
or a canonical decision id, or tick manually.

### Audit and Historical Backfill (internal)

When the post-slice sync misses items — stale anchors, plans rewritten
mid-task, or pre-internal plans that were never reconciled — two
read-only / opt-in commands recover the state without violating the
one-way ratchet:

- `make task-plan-checklist-audit TASK=<ref> LIFECYCLE_ARGS=--json`
  walks the plan, classifies every item as `already_ticked`,
  `tick_candidate`, `kept_unresolved`, or `stretch_skipped`, and prints
  the per-item reason. It never writes. Use `TASKS="A B C"` to fan out
  across multiple refs.
- `make task-plan-checklist-backfill TASKS="A B C" [APPLY=1]` runs the
  same resolver against the canonical workspace's plans and, with
  `APPLY=1`, projects evidence-backed ticks. Dry-run by default.

Both commands resolve plan paths from `handoff_state.task_plan_path`
first, then fall back to `packages/*/docs/tasks/<TASK>-*-task-plan.md`
globs. The audit's `kept_unresolved` row carries a `reason` string —
the most common fixes are:

- `anchor_missing_from_evidence` — the item body lacks a backtick file
  path, a `make` target, a `Slice N` reference, or a canonical
  decision id. Edit the item body to include one.
- `path_not_in_changed_files` — the anchor points at a file no slice
  touched. Either the slice never ran or the path is wrong; tick
  manually after verification.
- `slice_not_closed` — bare `Slice N` reference but no
  `_slice_complete_<work_ref>_slice_<N>` decision exists for that ref.
  Close the slice first.

The backfill enforces the same collision invariant as the audit: when
two plans share a `task_ref` (cross-package collisions like the
historical internal case), bare `Slice N` resolution is suppressed for
that ref and only path/command/decision-id anchors are honored — both
plans still appear in the receipt with `slice_ref_suppressed: true` so
the operator can disambiguate by hand.

### Merge Gate Guardrail

`make review-ready` warns when the active task's plan still has
evidence-backed `- [ ]` items (reason `checklist_sync_pending`,
warn-only because slices may still be in flight). `make close-check`
upgrades the same probe to a blocker — the branch cannot land while
the resolver believes the plan is out of sync with recorded evidence.
The next-command for both is
`make sync-task-plan-checklist TASK=<task-ref> APPLY=1`.

## See also

- [`branch-review-guide.md`](branch-review-guide.md) — review checklist that
  cites this protocol.
- [`planning-artifact-home.md`](planning-artifact-home.md) — where scopes,
  plans, ADRs, and task plans live before a task branch claims them; warn-only
  lifecycle check fires on untracked planning files on `main`.
- `packages/workbay-protocol/src/workbay_protocol/branch_naming.py` —
  the canonical regex and helper functions.
- `packages/workbay-system/scripts/hooks/check_branch_naming.py` —
  the four-layer naming gate implementation.
