# Offload

## Overview

Use this skill to offload one self-contained implementation slice to a junior lane on an
explicit backend (`grok-cli` or `codex-subagent`) at an explicit reasoning effort, with a
caller-enforced token governor, a manifest-pinned reviewer backend, and a human/orchestrator
review gate before merge. Fail-fast, **no fallback** between backends.

## Trigger

Use this skill when:

- the operator invokes `/offload --agent <grok-cli|codex-subagent> --effort <level> --token-budget <N> "<slice objective>"`
- a bounded junior-subagent slice should run out-of-band (`single_pass=true`)
- token spend must be capped by `token_budget` (cross-cycle circuit breaker)

Do not use it for `codex-cli` offload (deferred until it carries a single-cycle wall-clock
bound), multi-lane fan-out, synchronous inline wait, or work on `main` without a feature task
branch.

## Goal

Select/confirm an explicit backend + effort, run Fail-Fast pre-flight, materialize the lane
with a reviewer-backend pin, dispatch and start a one-shot worker with `token_budget`, poll to
completion, and present the handoff diff for review.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- Task plan: `docs/tasks/internal-cross-harness-token-aware-offload-task-plan.md`

## Explicit agent + effort

`/offload` requires `--agent` and `--effort`; there is no `--agent auto` and no implicit
backend default.

- **agent**: `grok-cli` or `codex-subagent`. Normalized through
  `backend_registry.validate_backend`; anything else (including `codex-cli`) fails fast.
- **effort**: one of `low|medium|high|xhigh|auto|inherit` (the canonical
  `_env.WORKER_REASONING_EFFORT_CHOICES` set). Concrete efforts are pinned into the lane
  manifest; `auto|inherit` are resolved by `_env.resolve_auto_reasoning_effort` at execution
  and are **not** pinned.
- **single-cycle bound**: `grok-cli` derives `max_turns`/`timeout` from `token_budget`;
  `codex-subagent` is guarded by the Codex app-server bridge timeout.

## Token-aware advisory selection (optional)

When the operator asks the orchestrator to choose the agent, run this deterministic selector
and then pass the chosen values explicitly — the selection is a preparation step, not a hidden
default:

1. Reject a missing or non-positive `token_budget` before scoring.
2. Start from the supported offload profiles (`grok-cli`, `codex-subagent`); drop any that
   `list_available_backends(probe=true)` reports unavailable, or whose model pin / effort the
   profile cannot honor.
3. Pick effort from task difficulty unless the operator supplied one: `low` for docs/tests-only,
   `medium` for bounded implementation, `high` for cross-boundary/ambiguous slices, `xhigh` only
   when explicitly requested. `auto|inherit` pass through only when explicitly requested.
4. Read `turn_metrics(operation="summary", task_ref=<coordinator task_ref>)` and score each
   candidate by recent burn at `by_backend_model_total_tokens[<backend>::<model-or-default>]`.
   Missing metrics count as zero recent burn, not a failure.
5. Choose the lowest recent-burn candidate; break ties by profile declaration order. Emit the
   selected agent/model/effort + rationale before dispatch.
6. If no candidate remains, fail fast with the preflight reason. **Do not fall back** to another
   backend after a selected backend fails.

## Core Process

1. Confirm an active feature-branch task (`get_handoff_state(sections="identity")`).
   Abort on `main`/`master`/unset `target_branch`.
2. Resolve explicit `agent`, `effort`, and `token_budget` (all required). If the orchestrator is
   choosing, run the advisory selector above and materialize the choice into explicit values.
3. **Fail-Fast pre-flight** via orchestrator
   `offload_preflight(agent=..., reasoning_effort=..., model=..., token_budget=..., worktree_path=...)`:
   - `probe_availability(agent)` must be available
   - effort valid for the profile; model pin honored (grok pins `grok-composer-2.5-fast`;
     codex-subagent model is optional)
   - worktree clean; positive `token_budget`
   - returns selected backend/model/effort, `pinned_reasoning_effort`, and (grok) derived
     `single_cycle_bounds`
   On any error, stop with the Fail-Fast reason (zero dispatch spend).
4. `manage_worktree_lane(operation="upsert", backend=<agent>, ...)` for the lane row.
5. **Materialize lane manifest**: call
   `materialize_offload_lane_manifest(task_ref, lane_id, worktree_path, branch,
   preferred_backend=<agent>, preferred_model=<selected model or None>,
   preferred_reasoning_effort=<pinned effort or None>)` — it always pins `preferred_backend`
   (and pins effort only when concrete) as defense-in-depth for any review path that would
   otherwise default to codex-cli (`review_runner.py:run_review`).
6. `dispatch_lane_work(backend=<agent>, model, reasoning_effort, start_worker=false)`.
7. `manage_worker(action="start", backend=<agent>, single_pass=true, token_budget=N)`.
8. Poll `worker_reports` / `get_lane_activity` until handoff-ready or `token_budget_exceeded`
   blocker surfaces.
9. Present the lane handoff diff to the orchestrator for the **review gate** (branch-review);
   do not auto-merge.

## Governor discipline

- `token_budget` is a **cross-cycle** circuit breaker: non-converging multi-cycle lanes stop
  at the next cycle boundary after cumulative spend crosses the cap.
- A single converged `single_pass` cycle is bounded by the profile's single-cycle bound
  (grok derived `max_turns`/`timeout`; codex-subagent bridge timeout), not `token_budget`.
- Open-circuit keeps the worktree diff and records a distinct `token_budget_exceeded` blocker.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "A selected backend outage means I should try the other backend." | There is **no fallback**: an outage after selection hides failure and can amplify cost (Release It!). | Fail fast, surface the blocker, stop. |
| "I'll omit `--agent`/`--effort` and let a default apply." | Explicitness is the safety contract; there is no implicit backend default and no `--agent auto`. | Require explicit `--agent` and `--effort` on every invocation. |
| "I'll pin `auto`/`inherit` effort into the manifest." | Lane-manifest validation rejects non-concrete efforts; they are resolved at execution by `_env.resolve_auto_reasoning_effort`. | Pin only concrete efforts; leave `auto`/`inherit` unpinned. |
| "I'll omit `backend` on dispatch/start — defaults are fine." | Defaults route to codex-subagent, not the selected agent. | Pass `backend=<agent>` explicitly on every dispatch/start. |

## Red Flags

| Flag | Re-entry |
|---|---|
| `offload_preflight` returned an error | Do not dispatch; surface the Fail-Fast reason and stop (zero spend). |
| Selected backend unavailable | Fail fast; do not fall back to another backend. |
| Dispatch/start omitted explicit `backend=<agent>` | Re-issue with the explicit agent; the default would offload to the wrong backend. |
| Lane wedged with repeated `token_budget_exceeded` blockers | Open-circuit tripped; stop the lane, inspect the kept worktree diff, do not re-dispatch. |

## Recovery

- Pre-flight failure: fix the named precondition (agent available, valid effort, model pin, clean worktree, positive `token_budget`) and re-run; nothing was dispatched.
- Backend outage mid-run: the lane fails fast with a blocker; no fallback to another backend, no retry storm — report and stop.
- Budget exceeded: the worktree diff is preserved with a `token_budget_exceeded` blocker; review the partial diff at the gate, then continue or abandon.

## Convergence Criteria

- Explicit `--agent` and `--effort` resolved; pre-flight passed with zero dispatch on failure.
- Manifest carries `preferred_backend=<agent>` (and concrete effort); review phase observable on the selected backend.
- Worker started with `single_pass=true` and `token_budget`.
- Handoff diff presented for orchestrator review gate; no auto-merge, no fallback.

## See Also

- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../auto-fix/SKILL.md](../auto-fix/SKILL.md)
