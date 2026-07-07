# Offload

## Overview

Use this skill to offload one self-contained implementation slice to a junior lane on an
explicit backend (`grok-cli` or `codex-subagent`) at an explicit reasoning effort, with a
caller-enforced token governor, a manifest-pinned reviewer backend, and a human/orchestrator
review gate before merge. Fail-fast, **no fallback** between backends.

## Trigger

Use this skill when:

- the operator invokes `/offload --agent <grok-cli|codex-subagent> --effort <level> --token-budget <N> "<slice objective>"`
- a bounded junior-subagent slice should run as one synchronous offload pass (`single_pass` semantics)
- token spend must be capped by `token_budget` (cross-cycle circuit breaker)

Do not use it for `codex-cli` offload (deferred until it carries a single-cycle wall-clock
bound), multi-lane fan-out, synchronous inline wait, or work on `main` without a feature task
branch.

## Goal

Select/confirm an explicit backend + effort, run Fail-Fast pre-flight, materialize the lane
with a reviewer-backend pin, atomically dispatch the brief, run one synchronous
`run_offload_pass` bounded by `token_budget` + `timeout_seconds`, branch on its typed
outcome, and present the handoff diff for review. No polling, no background monitors.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- Task plan: `docs/tasks/internal-synchronous-offload-dispatch-task-plan.md`
  (synchronous atomic dispatch; supersedes the earlier cross-harness plan
  `docs/tasks/internal-cross-harness-token-aware-offload-task-plan.md`,
  whose token-aware profile guardrails still apply)

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
6. `dispatch_lane_work(brief=<brief per the Brief contract below>, dispatch_id=<idempotency key>,
   backend=<agent>, model, reasoning_effort, start_worker=false)`. Dispatch records the brief
   atomically with the lane params and returns `outcome` + `actionable`; a call without `brief`
   returns `params_only` and the lane stays non-actionable. Re-dispatch with the same
   `dispatch_id` is a no-op (`duplicate_dispatch`) — restart-after-fix never double-enqueues.
7. `run_offload_pass(lane_id, backend=<agent>, model, reasoning_effort, token_budget=N,
   timeout_seconds=T, turn_timeout_seconds<=T)`. One synchronous call executes the bounded
   execute→review→fix pass and returns a **typed outcome**:
   `handoff_ready | needs_guidance | no_actionable_work | uncommitted_work | token_budget_exceeded | timeout | error | lane_not_found`.
   `needs_guidance` means the worker submitted a blocked or verification-failed handoff —
   the submission landed but the work is **not** merge-ready; treat it like a blocker, not a
   pass. Branch on the outcome enum only — never on bare ok/exit codes, and never on log tails.
   Note: input-validation refusals (missing/invalid `token_budget`, missing `timeout_seconds`,
   a `turn_timeout_seconds` a backend cannot enforce) are **not** enum outcomes — they return
   `ok:false` with an `error` string before any spend; check `ok` first, then the outcome enum.
8. On `still_running` (or a client timeout/disconnect mid-pass), recover with bounded
   `await_offload_pass(pass_id, wait_seconds)` calls — one call per wait window, a coarse
   continuation, **not** a poll loop. The `pass_id` is the one `run_offload_pass` returns on
   its result (or a caller-supplied `pass_id` passed into `run_offload_pass` up front so it is
   known before the call blocks); persist it so a disconnect can reconnect to the same pass.
9. Present the lane handoff diff to the orchestrator for the **review gate** (branch-review);
   do not auto-merge.

**Prohibited**: no poll loops (`worker_reports`/`get_lane_activity` polling), no
`manage_worker` start step, no background monitors, no detached daemons anywhere in the
flow. An idle lane and uncommitted execute output surface as **typed outcome** values
(`no_actionable_work` / `uncommitted_work`); a missing/invalid budget or timeout surfaces as
an `ok:false` **error string** before any spend. Either way, do not retry inside the flow;
recovery is a new explicit dispatch (idempotent on `dispatch_id`).

## Offload decision rule and slice sizing

- Offload only when the expected turn exceeds the **fixed overhead** (~5–10 min:
  preflight + lane + brief + turn spin-up + review). Otherwise implement inline, or batch
  related small slices into one brief.
- Size each brief to fit one backend turn within `turn_timeout_seconds`
  (`turn_timeout_seconds` ≤ pass `timeout_seconds`; a slice that outlives the turn bound
  pays salvage/redispatch). `max_review_cycles` defaults to 2: a clean review costs one
  inner pass; findings get one in-turn self-fix round; more cycles only by explicit request.
- Coordinator reads handoff state with `read_profile`/`response_budget_bytes` and branches
  on **typed outcome** payloads (backend, model, tokens, checkpoint refs, slice_closure) —
  no log-tail archaeology.

## Brief contract

Every brief is the worker's complete assignment; it must state the mandatory worker
**end-state** and the verification inputs:

- **End-state (PR-09/PR-10)**: a git commit on the lane branch plus a worker report, with
  bounded auto-fix of the worker's own inner review findings (the execute→review→fix loop, ≤
  `max_review_cycles`, within budget). When the worker cannot comply it records a
  **typed blocker** — never a silent idle exit. Slice closure is recorded by the engine from the
  verified commit + report with the backend's actor identity; the coordinator never implements,
  fixes, or commits lane work.
- **Scoped `TEST_CMD`**: the exact bounded verification command for the slice (never the full
  suite), so the worker does not re-run unrelated suites.
- **Known-red baseline ref**: the recorded `test_result` row capturing pre-existing failures,
  so the worker neither re-diagnoses them nor mistakes them for its own regression.

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
| "I'll omit `backend` on dispatch/`run_offload_pass` — defaults are fine." | Defaults route to codex-subagent, not the selected agent. | Pass `backend=<agent>` explicitly on every `dispatch_lane_work`/`run_offload_pass`. |

## Red Flags

| Flag | Re-entry |
|---|---|
| `offload_preflight` returned an error | Do not dispatch; surface the Fail-Fast reason and stop (zero spend). |
| Selected backend unavailable | Fail fast; do not fall back to another backend. |
| `dispatch_lane_work`/`run_offload_pass` omitted explicit `backend=<agent>` | Re-issue with the explicit agent; the default would offload to the wrong backend. |
| Lane wedged with repeated `token_budget_exceeded` blockers | Open-circuit tripped; stop the lane, inspect the kept worktree diff, do not re-dispatch. |

## Recovery

- Pre-flight failure: fix the named precondition (agent available, valid effort, model pin, clean worktree, positive `token_budget`) and re-run; nothing was dispatched.
- Backend outage mid-run: the lane fails fast with a blocker; no fallback to another backend, no retry storm — report and stop.
- Budget exceeded: the worktree diff is preserved with a `token_budget_exceeded` blocker; review the partial diff at the gate, then continue or abandon.

## Convergence Criteria

- Explicit `--agent` and `--effort` resolved; pre-flight passed with zero dispatch on failure.
- Manifest carries `preferred_backend=<agent>` (and concrete effort); review phase observable on the selected backend.
- Pass executed via `run_offload_pass` with mandatory `token_budget` + `timeout_seconds`; typed outcome handled (`await_offload_pass` only on `still_running`/disconnect).
- Handoff diff presented for orchestrator review gate; no auto-merge, no fallback.

## See Also

- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../auto-fix/SKILL.md](../auto-fix/SKILL.md)
