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
- Cite stable rule IDs from [engineering-heuristics.md](../../docs/workbay/rules/engineering-heuristics.md) at use time; never pin a canon version or date. Every dispatched brief must carry this **link** (not pasted heuristics content, not a version label).
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
   - effort valid for the profile; model pin honored (grok defaults to `grok-4.5`;
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
   `handoff_ready | needs_guidance | no_actionable_work | uncommitted_work | token_budget_exceeded | timeout | error | lane_not_found | self_verify_failed | composer_violation_quarantined | checkpoint`.
   Every outcome payload also carries **`commit_landed: bool`** and **`failed_stage`**
   (`execute | self_verify | review | handoff | attestation | null`) so the gate branches
   without git archaeology (implementation note). Green self-verify under the configured pin
   (`DEFAULT_GROK_MODEL` / `WORKBAY_GROK_MODEL`, default grok-4.5) returns plain
   `handoff_ready` — Composer attestation is retired (implementation note S2); there is no
   `handoff_ready_unattested` / `attestation_missing` branch. `needs_guidance` means the
   worker submitted a blocked or verification-failed handoff — the submission landed but
   the work is **not** merge-ready; treat it like a blocker, not a pass. `self_verify_failed`
   means the worker's `TEST_CMD` gate failed before commit — no green commit exists; a typed
   blocker carries the output tail. `composer_violation_quarantined` (grok-cli only) means
   **grok-build contamination** fired *after* a self-verified checkpoint — the commit is
   preserved with evidence and routed to this review gate; never auto-merge it, never
   silent-discard it (Composer pin attestation is gone; this outcome is contamination only).
   `checkpoint` means execute stopped on max turns with a self-verified checkpoint preserved
   and a `continuation_dispatch_id` returned — resumable, **not** terminal: re-dispatch with
   that same `dispatch_id` to continue (never re-enqueues). Branch on the outcome enum only —
   never on bare ok/exit codes, and never on log tails.
   Note: input-validation refusals (missing/invalid `token_budget`, missing `timeout_seconds`,
   a `turn_timeout_seconds` a backend cannot enforce) are **not** enum outcomes — they return
   `ok:false` with an `error` string before any spend; check `ok` first, then the outcome enum.
8. On `still_running` (or a client timeout/disconnect mid-pass), recover with bounded
   `await_offload_pass(pass_id, wait_seconds)` calls — one call per wait window, a coarse
   continuation, **not** a poll loop. The `pass_id` is the one `run_offload_pass` returns on
   its result (or a caller-supplied `pass_id` passed into `run_offload_pass` up front so it is
   known before the call blocks); persist it so a disconnect can reconnect to the same pass.
9. Present the lane handoff diff to the orchestrator for the **review gate** (`/review-parallel`); do not auto-merge.

   Distinct from the in-lane `/branch-review` smoke test — `/review-parallel` is the
   branch-complete merge gate.
10. **Close the lane after the gate merges.** Once the review gate has merged the lane's
    work, close the lane explicitly:
    `manage_worktree_lane(operation="close", lane_id=<lane>, task_ref=<task>, status="merged")`
    — or follow the pass result's `next_lane_action` when it names the close call. This is
    an orchestrator-owned MCP write; no `wb` one-shot performs it (`wb ship` only reaps
    still-open lanes as a task-finish safety net, which under-records merged lanes).
    Skipping this step leaves orphan open lanes that block later close-checks.

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

## Allocation and gate policy

Who runs what, on which model, with which read budget — so orchestration cost does not
scale with slice count:

- **Branch-complete gate**: `/review-parallel` followed by `/auto-fix` is the **default
  next step the orchestrator must run** when all slices are closed — the gate is invoked
  explicitly (nothing fires it for you; review-parallel's own Trigger and branch-lifecycle
  step 6 are explicit invocations). The gate is **orchestrator-owned** (it needs the
  subagent fan-out primitive, merge authority, and disposition judgment). Skipping it
  requires a recorded decision, never a silent skip.
- **Auto-fix offload routing**: re-offload a fix as a lane slice only when (a) a
  deterministic red test exists, (b) the scope is localized, (c) no design decision is
  required, and (d) the batch is ≥2 findings or the estimate exceeds the lane fixed
  overhead (~5–10 min); otherwise fix inline. Triage and `resolve` writes stay with the
  orchestrator.
- **No-LLM mechanics**: deterministic lifecycle steps (worktree create/teardown,
  close-check, plan-checklist ticks) run as make targets / `wb <verb>` one-shots —
  never through a model, orchestrator or lane (see the lifecycle runbook,
  `../../docs/workbay/wb-lifecycle-runbook.md`). Lane close is the exception: no `wb`
  one-shot closes lanes (`wb ship` only *reaps* leftovers via the task-finish safety
  net) — the orchestrator closes each lane explicitly with
  `manage_worktree_lane(operation="close", ...)` after the review gate merges (step 10).
- **Bounded reads by default**: every skill-mandated handoff read names a `read_profile`
  (plus `response_budget_bytes` where responses can grow); mid-loop reads are
  identity-only. The `include_write_schemas` block stays opt-in.
- **Subagent model tiering**: mechanical passes (verification forensics, single-slice
  reviewers, grep sweeps) request a cheaper model via the fan-out primitive's model
  parameter; the frontier model is reserved for design judgment, harmonization, and
  verdicts. Prefer forks/Explore agents for forensics so raw diffs and logs stay out of
  the coordinator context.
- **Context hygiene**: the coordinator compacts between phases (implementation → review
  gate → finalize); a session running past ~8h must be a deliberate loop, not a leftover.

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
- **Heuristics link (T17)**: every dispatched brief **must** include the versionless relative
  link to [engineering-heuristics.md](../../docs/workbay/rules/engineering-heuristics.md)
  plus "cite stable rule IDs at use time". Do **not** paste heuristics body text and do **not**
  pin a canon version or date.
- **No subagent steps for grok lanes (T6)**: when `backend=grok-cli`, the brief must **not**
  request subagent-requiring steps (`/review-parallel`, subagent fan-out reviews). Use
  in-lane `/branch-review` only; reserve `/review-parallel` for the orchestrator merge gate.
  `dispatch_lane_work` emits a named warning (`grok_brief_subagent_steps`) when a grok brief
  still mentions those steps — warn only, never block.
- **Grounding accuracy (T20)**: symbol/key anchors in briefs come from exact tools
  (`search_graph` / `get_code_snippet` or direct file reads) — **never** raw grep for symbol
  identity. Briefs carry only verified literals; do not over-specify unverified names.

### Per-slice review (single-reviewer)

After each slice the worker runs `/branch-review` (single-reviewer, **no** subagent fan-out)
followed by `/auto-fix` on its own findings. This in-lane pass is an explicitly
**non-authoritative smoke test** — it catches obvious regressions before the next slice but
does not gate merge. The orchestrator's `/review-parallel` at branch-complete is the
**sole merge gate**.

### Implementation discipline (worker mandates)

The worker writing offloaded code must follow these five mandates:

- **(a) Real-shape tests** — before mocking any function's output, read its actual producer
  and mirror its exact return shape; never fabricate a simpler shape.
- **(b) Degrade-path coverage** — for every optional-dependency import or I/O call, add a test
  exercising the failure branch, not just the happy path.
- **(c) Grounded branch conditions** — verify any value you branch on (cycle start index,
  status-dict key presence, etc.) against the real producer before relying on it.
- **(d) Valid handoff JSON** — emit exactly one schema-valid JSON object as the final turn
  output; a malformed handoff false-negatives finished work to `needs_guidance`.
- **(e) Anchor override authority** — the worker is authorized to override demonstrably-wrong
  brief anchors (wrong symbol/key/path after verification) and **must** record the override
  in its worker report.

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
| "The grok lane will run `/review-parallel` per slice." | The grok adapter hardcodes `--no-subagents`, which disables the subagent fan-out primitive `/review-parallel` requires. | Run `/branch-review` in-lane; reserve `/review-parallel` for the orchestrator gate. |
| "I'll mock the return value from memory — the shape is obvious." | Fabricated mock shapes hide integration bugs; real-shape tests pass while production code mis-reads real producer output. | Read the actual producer and mirror its exact return shape before mocking (mandate **(a)**). |
| "Optional imports only need happy-path tests." | Degrade paths fail silently in production when a dependency is missing or I/O errors. | Add a test exercising the failure branch for every optional-dependency import or I/O call (mandate **(b)**). |
| "The status dict surely has that key — I'll branch on it." | Ungrounded branch conditions cause silent wrong-path execution when assumed keys or indices are absent. | Verify grounded branch conditions against the real producer before relying on them (mandate **(c)**). |
| "The handoff JSON is close enough — the orchestrator will parse it." | Malformed final-turn JSON false-negatives finished work to `needs_guidance`, wasting a full offload cycle. | Emit exactly one schema-valid JSON object as the final turn output (mandate **(d)**). |
| "The brief anchor must be right — I'll follow it even if lookup fails." | Grep-grounded briefs can carry wrong symbols/keys; silent obedience wastes the whole turn. | Override demonstrably-wrong anchors after exact-tool verification and record the override (mandate **(e)**). |

## Red Flags

| Flag | Re-entry |
|---|---|
| `offload_preflight` returned an error | Do not dispatch; surface the Fail-Fast reason and stop (zero spend). |
| Selected backend unavailable | Fail fast; do not fall back to another backend. |
| `dispatch_lane_work`/`run_offload_pass` omitted explicit `backend=<agent>` | Re-issue with the explicit agent; the default would offload to the wrong backend. |
| Lane wedged with repeated `token_budget_exceeded` blockers | Open-circuit tripped; stop the lane, inspect the kept worktree diff, do not re-dispatch. |
| Brief tells the lane to run `/review-parallel` per slice | Replace with `/branch-review` single-reviewer; route `/review-parallel` to the orchestrator gate. |

## Recovery

- Pre-flight failure: fix the named precondition (agent available, valid effort, model pin, clean worktree, positive `token_budget`) and re-run; nothing was dispatched.
- Backend outage mid-run: the lane fails fast with a blocker; no fallback to another backend, no retry storm — report and stop.
- Budget exceeded: the worktree diff is preserved with a `token_budget_exceeded` blocker; review the partial diff at the gate, then continue or abandon.

## Convergence Criteria

- Explicit `--agent` and `--effort` resolved; pre-flight passed with zero dispatch on failure.
- Manifest carries `preferred_backend=<agent>` (and concrete effort); review phase observable on the selected backend.
- Pass executed via `run_offload_pass` with mandatory `token_budget` + `timeout_seconds`; typed outcome handled (`await_offload_pass` only on `still_running`/disconnect).
- Handoff diff presented for orchestrator `/review-parallel` review gate (branch-complete
  merge gate); no auto-merge, no fallback.

## See Also

- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../auto-fix/SKILL.md](../auto-fix/SKILL.md)
