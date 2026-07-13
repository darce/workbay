# Auto-Fix

## Overview

Use this skill to drive a single failing test to green through a bounded, cache-aware loop. Each iteration reads only bounded identity state, proposes the smallest candidate edit, commits it on the feature branch, runs the test, and records a `test_result`. The loop exits on the first `passed=true` row tied to HEAD. `integrity_check(payload={"kind":"close","enforce":true,"require_fresh_tests":true})` is reserved as the single post-loop pre-merge gate — never a per-iteration exit condition.

## Trigger

Use this skill when:

- a known failing test exists and the operator wants it driven to green autonomously
- the active task already has a feature `target_branch`; code edits belong on that branch
- iterations should be bounded by a hard iteration cap so token spend is predictable
- the work is a targeted fix, not a multi-path feature slice (use `incremental-implementation` for feature slices)

Do not use it on `main`/`master`, without an active task, for planning or review work, or as a substitute for a structured feature slice. The skill will refuse to start in the first three cases.

## Goal

Convert a `failing_test_cmd` from red to green inside a bounded loop with predictable token cost, then close the slice cleanly so the pre-merge gate can verify the fix against the current HEAD.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../packages/mcp-workbay-handoff/docs/guides/token-efficient-usage.md](../../../packages/mcp-workbay-handoff/docs/guides/token-efficient-usage.md)
- [../../../docs/tasks/17.0/internal-parallel-reviews-and-autonomous-bug-fix-loop-task-plan.md](../../../docs/tasks/17.0/internal-parallel-reviews-and-autonomous-bug-fix-loop-task-plan.md)
- Cite stable rule IDs from [engineering-heuristics.md](../../docs/workbay/rules/engineering-heuristics.md) at use time; never pin a canon version or date.

This skill owns the bounded-loop protocol and the post-loop finalization contract. The `tdd` skill owns the RED→GREEN gate inside a structured slice; `branch-review`/`review-parallel` own post-implementation review.

Allocation policy (owned by the `offload` skill, *Allocation and gate policy*): re-offload a fix as a lane slice only when it has a deterministic red test, localized scope, no design decision, and batches ≥2 findings or exceeds the lane fixed overhead — otherwise fix inline. Triage and `resolve` writes stay with the orchestrator; mid-loop handoff reads are identity-only.

## Harness Scope

The MCP state the loop produces is identical across Claude Code, Codex, and Copilot. Only the inter-iteration wait primitive differs:

| Harness | Inter-iteration wait |
| --- | --- |
| Claude Code | `ScheduleWakeup(delaySeconds<270)` for warm-cache polling; `delaySeconds>=1200` for idle waits; **300 is forbidden** (cache miss without amortization). |
| Codex | No `ScheduleWakeup` primitive — iterate inline unconditionally. |
| Copilot / VS Code | No `ScheduleWakeup` primitive — iterate inline unconditionally. |

The cadence block in Core Process applies only where `ScheduleWakeup` exists.

## Precondition

Run before any iteration starts. Zero MCP writes if the precondition fails.

1. `get_handoff_state(sections="identity")`. If `active.task_ref` is absent, abort with: "auto-fix requires an active task; run `make task-start TASK=<id>` first."
2. Read `active.target_branch`. If it is `main`, `master`, or `None`/unset, abort with: "auto-fix refuses to run on `main`/`master`/unset target_branch; start a feature branch via `make task-start TASK=<id>` first." The branch-isolation hook ([scripts/hooks/guard-main-branch.sh](../../../scripts/hooks/guard-main-branch.sh), [.github/hooks/guard-main-branch.py](../../../.github/hooks/guard-main-branch.py)) would reject the first code edit regardless, but the precondition produces a clearer error.
3. Record the investigation opening as a **decision** (not a finding): `record_event(event={"event_kind":"decision","decision":"<tag>_auto_fix_open_<task>_<slug>","rationale":"<why this failing test, what the scope_hint bounds>"})`. This mirrors the `investigate` skill's provenance goal without an invalid `review_mode`.

## Core Process

### Per iteration (bounded, cache-aware)

1. `get_handoff_state(sections="identity")`. **Never** `detail="full"` mid-loop — the response grows with task history and turns each iteration into a 5–30K-token tax.
2. Read only the files implicated by `scope_hint` plus whatever the failing test output names. Do not pre-read adjacent modules "in case."
3. Propose the smallest candidate fix — one localized edit, not a refactor.
4. **Commit the candidate fix on the feature branch** before running the test: `git commit -am "wip(auto-fix): iter <N>"`. This advances HEAD so the next step's `test_result` provenance ties the row to the actual fix. Uncommitted-workspace iterations are an anti-pattern: `verified_tests` would stay on the pre-fix SHA and the post-loop gate would see stale tests.
5. Run `failing_test_cmd`; capture stdout/stderr.
6. Record the outcome: `record_event(event={"event_kind":"test_result","command":failing_test_cmd,"passed":<bool>,"actor":{"commit_sha":<HEAD>}})`.
7. **Per-iteration exit signal**: `passed=true` on step 6. If true, break out of the iteration loop and proceed to Finalization. If false and iteration count `< max_iterations`, continue to the next iteration. The exit signal is the fresh `test_result` / `verified_tests` row, **not** `integrity_check(kind="close").ok` — invoking the gate per iteration would fail every time until the slice-complete decision exists, which wastes tokens and muddies the audit trail.
8. At `iteration == max_iterations` without a passing run: `record_event(event={"event_kind":"blocker","operation":"add","description":"auto-fix exhausted <N> iterations without convergence"})`, leave the WIP commits for human triage (or squash them into one WIP commit), and exit non-zero. Do not silently exit.

### Finalization (runs once, only after a passing iteration)

1. Optionally squash the WIP iteration commits into a single clean commit (`git rebase -i` or `git reset --soft` + `git commit`). HEAD after this step is what the gate will verify.
2. Record the canonical slice-complete decision via `close_slice(task_ref=<active>, author_tag=<tag>, work_ref=<task>, slug="autofix", rationale=<structured summary with the four required ## sections>, session=<session>, expected_revision=<rev>, actor={"commit_sha":<HEAD>})`. `close_slice` is the canonical write path because it enforces the four required rationale sections (`## Changes`, `## Verification`, `## Schema / Contract Changes`, `## Open Threads`); plain `record_event(event_kind="decision", decision="<tag>_slice_complete_*")` passes the id regex but skips that structural gate and the post-loop close check then fails the `current_commit_handoff` audit on a malformed rationale.
3. If the active task status is not `done`, call `set_handoff_state(task_ref=<active>, status="done", status_only=True)`.
4. Call `integrity_check(payload={"kind":"close","enforce":true,"require_fresh_tests":true,"current_commit_sha":<HEAD>})` **exactly once**. Expect `ok=true`. On `ok=false`, surface the failure list to the operator; do not re-enter the loop to "fix" the gate.
5. Any defects observed mid-loop that deserve review tracking are recorded via `review_findings(review={"operation":"record","review_mode":"branch", ...})`. Those must be closed or deferred before the post-loop gate; they count as open findings on the active task.

### Cadence (harness-scoped)

- **Claude Code**: between iterations, use `ScheduleWakeup(delaySeconds<270)` only when waiting on an external signal (e.g. CI). Otherwise iterate inline. Never `delaySeconds=300` — it pays the cache-TTL miss without amortizing it.
- **Codex / Copilot**: no `ScheduleWakeup` primitive. Iterate inline unconditionally. Detect the harness using the same routing table the `/review-parallel` skill documents.
- Cross-harness MCP state is identical; only the wall-clock cadence differs.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I'll run the candidate fix without committing first — committing per iteration is noisy." | `verified_tests` rows record the caller's `commit_sha`. Running the test against an uncommitted workspace records the row under the pre-fix SHA, so the post-loop gate sees stale tests and `integrity_check(kind="close")` fails with a confusing "tests not fresh against HEAD" error. | Commit the candidate fix before running the test every iteration. Squash at Finalization if the operator wants a clean history. |
| "`integrity_check(kind=\"close\").ok=true` is the real convergence signal; I'll call it every iteration." | `ok=true` requires task `status=done` + a canonical `slice_complete_*` decision + fresh tests on HEAD **simultaneously**. Mid-loop none of those hold. The gate is a pre-merge check, not a loop-internal exit condition. | Use the `passed=true` test_result row as the per-iteration exit signal. Call `integrity_check(kind="close")` exactly once in Finalization. |
| "Calling `get_handoff_state` with `detail='full'` is more thorough — I'll use it between iterations." | The full response grows with task history and each iteration burns 5–30K tokens on data the loop does not consume. Compounded across tens of iterations that is a task-plan's worth of tokens. | Always `get_handoff_state(read_profile="identity")` (or `sections="identity"`) between iterations. |
| "I'll just sleep 300s between iterations, that's simple." | 300s straddles the 5-minute prompt-cache TTL boundary on Claude Code: you pay the cache-miss cost without amortizing it over a longer wait. | Use `<270s` for warm-cache polling or `>=1200s` for idle waits. Never 300. |
| "The failing test is on `main` but the fix is tiny — I'll just edit and commit on `main`." | The branch-isolation hooks reject code edits on `main`/`master`. Running `/auto-fix` without a feature branch burns the precondition error every iteration. | Create the feature branch first via `make task-start TASK=<id>`, then re-invoke `/auto-fix`. |

## Red Flags

Each flag is a re-entry trigger. Stop and re-enter at the step shown.

| Flag | Re-entry point |
|---|---|
| Loop called with no active task or `target_branch in {main, master, None}` | Precondition: abort with zero MCP writes; fix the branch state and re-invoke. |
| `get_handoff_state` called with `detail="full"` mid-loop | Core Process step 1: switch to `sections="identity"` for every iteration. |
| Test run before the candidate fix was committed | Core Process step 4: commit first, then run the test. |
| `integrity_check(kind="close")` invoked mid-loop | Core Process step 7: use the `passed=true` test_result as the exit signal; defer the gate to Finalization. |
| Finalization skipped `slice_complete` decision | Finalization step 2: record the canonical decision before calling the gate. |
| Iteration cap reached without a blocker event | Core Process step 8: record the blocker and exit non-zero. |
| Wait `delaySeconds=300` used on Claude Code | Cadence: pick `<270` or `>=1200`. |

## Recovery

- If the precondition fails, do not record any MCP rows. Surface the error, have the operator run `make task-start`, and re-invoke.
- If a candidate fix breaks an adjacent test the loop is not tracking, stop, record the regression as a `review_findings(review_mode="branch", ...)` row, and exit non-zero. Do not auto-"fix" findings outside the `failing_test_cmd` scope.
- If `integrity_check(kind="close").ok=false` at Finalization, read the returned failure list, resolve the named conditions (open findings, missing `slice_complete` decision, stale tests), and re-run the single gate call. Do not re-enter the iteration loop.
- If the operator cancels mid-loop, the WIP commits remain on the feature branch for triage; no finalization decision is recorded.

## Convergence Criteria

- Precondition passed (active task + feature `target_branch`).
- Every iteration called `get_handoff_state(sections="identity")` — no `detail="full"` mid-loop reads.
- Every iteration committed its candidate fix before running `failing_test_cmd`.
- Exactly one `test_result` row with `passed=true` and `commit_sha == HEAD` terminated the loop (or a blocker row was recorded at `max_iterations`).
- Finalization recorded a canonical `<tag>_slice_complete_<task>_autofix` decision, set `set_handoff_state(status="done", status_only=True)`, and called `integrity_check(payload={"kind":"close","enforce":true,"require_fresh_tests":true,"current_commit_sha":<HEAD>})` exactly once with `ok=true`.
- No 300-second waits on Claude Code; Codex / Copilot ran inline.

## See Also

- [../tdd/SKILL.md](../tdd/SKILL.md)
- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../investigate/SKILL.md](../investigate/SKILL.md)
- [../review-parallel/SKILL.md](../review-parallel/SKILL.md)
