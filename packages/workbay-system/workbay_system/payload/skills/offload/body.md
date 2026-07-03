# Offload

## Overview

Use this skill to offload one self-contained implementation slice to a grok Composer-2.5
lane with a caller-enforced token governor, grok-pinned self-review, and a human/orchestrator
review gate before merge.

## Trigger

Use this skill when:

- the operator invokes `/offload "<slice objective>"`
- a bounded junior-subagent slice should run out-of-band (`single_pass=true`)
- token spend must be capped by `token_budget` (cross-cycle circuit breaker) plus
  single-cycle `max_turns`/`timeout` derived at pre-flight

Do not use it for codex/claude offload (fast-follow), multi-lane fan-out, synchronous
inline wait, or work on `main` without a feature task branch.

## Goal

Run Fail-Fast pre-flight, materialize a grok lane with reviewer-backend pin, dispatch
and start a one-shot worker with `token_budget`, poll to completion, and present the
handoff diff for review — without the harness auto-launch prompt.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- Task plan: `docs/tasks/internal-junior-subagent-offload-governor-task-plan.md`

## Core Process

1. Confirm an active feature-branch task (`get_handoff_state(sections="identity")`).
   Abort on `main`/`master`/unset `target_branch`.
2. Parse the slice objective and `token_budget` (required). Default model is
   `grok-composer-2.5-fast`; backend must be explicit `grok-cli` on every dispatch/start.
3. **Fail-Fast pre-flight** via orchestrator `offload_preflight`:
   - `probe_availability("grok-cli")` must be available
   - model pin == `grok-composer-2.5-fast`
   - worktree clean
   - `token_budget` set
   - derive single-cycle `max_turns`/`timeout` from budget (Release It! §5.1 limit-at-caller)
4. `manage_worktree_lane(operation="upsert", backend="grok-cli", ...)` for the lane row.
5. **Materialize lane manifest**: call
   `materialize_offload_lane_manifest(task_ref, lane_id, worktree_path, branch)` — it pins
   `preferred_backend=grok-cli` / `preferred_model=grok-composer-2.5-fast` in the manifest as
   defense-in-depth for any review path that would otherwise default to codex-cli
   (`review_runner.py:471`). The offload worker's own review is already routed to grok by the
   explicit `backend="grok-cli"` on the lane upsert + worker start (steps 4/7), which wins over
   the manifest (`CLI > Manifest > Default` priority).
6. `dispatch_lane_work(backend="grok-cli", model, reasoning_effort, start_worker=false)`.
7. `manage_worker(action="start", backend="grok-cli", single_pass=true, token_budget=N)`.
8. Poll `worker_reports` / `get_lane_activity` until handoff-ready or `token_budget_exceeded`
   blocker surfaces.
9. Present the lane handoff diff to the orchestrator for **review gate** (branch-review);
   do not auto-merge.

## Governor discipline

- `token_budget` is a **cross-cycle** circuit breaker: non-converging multi-cycle lanes stop
  at the next cycle boundary after cumulative spend crosses the cap.
- A single converged `single_pass` cycle is bounded by derived `max_turns`/`timeout`, not
  `token_budget` (cycle-0 pre-check sees `cumulative_tokens=0`).
- Open-circuit keeps the worktree diff and records a distinct `token_budget_exceeded` blocker.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "The manifest `preferred_backend` is what pins the offload worker's review." | The worker review runs on `ctx.backend` (the explicit `backend=grok-cli` from lane upsert + worker start), which wins over the manifest (`CLI > Manifest > Default`). The manifest pin only redirects review paths that would default to codex-cli. | Pass `backend="grok-cli"` explicitly on every dispatch/start; materialize the manifest as defense-in-depth. |
| "I'll omit `backend` on dispatch — manage_worker defaults are fine." | Defaults route to codex-subagent, not grok. | Pass `backend="grok-cli"` explicitly on every call. |
| "token_budget bounds a single converged offload." | Governor pre-check is post-cycle; single-pass happy path exits before a second boundary. | Rely on derived `max_turns`/`timeout` for single-cycle bound. |

## Red Flags

| Flag | Re-entry |
|---|---|
| `offload_preflight` returned an error | Do not dispatch; surface the Fail-Fast reason and stop (zero spend). |
| Dispatch/start omitted `backend="grok-cli"` | Re-issue with explicit backend; codex is the default and would offload to the wrong model. |
| Lane wedged with repeated `token_budget_exceeded` blockers | Open-circuit tripped; stop the lane, inspect the kept worktree diff, do not re-dispatch. |

## Recovery

- Pre-flight failure: fix the named precondition (grok on PATH, model pin, clean worktree, positive `token_budget`) and re-run; nothing was dispatched.
- Backend outage mid-run: the lane fails fast with a blocker; no fallback-to-Claude, no retry storm — report and stop.
- Budget exceeded: the worktree diff is preserved with a `token_budget_exceeded` blocker; review the partial diff at the gate, then continue or abandon.

## Convergence Criteria

- Pre-flight passed with zero dispatch on failure.
- Manifest carries `preferred_backend=grok-cli`; review phase observable on grok-cli.
- Worker started with `single_pass=true` and `token_budget`.
- Handoff diff presented for orchestrator review gate.

## See Also

- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../auto-fix/SKILL.md](../auto-fix/SKILL.md)