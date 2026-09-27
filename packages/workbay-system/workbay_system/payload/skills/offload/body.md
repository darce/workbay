# Offload

## Overview

Use this skill to offload one self-contained implementation slice to a junior lane on an
explicit backend supported by the installed backend registry (`grok-remote`, `grok-cli`,
`codex-subagent`, `codex-remote`, `cursor-cli`, `cursor-remote`, or `openrouter-remote`)
at an explicit reasoning effort and token budget. The registry's capabilities and the
install's execution mode determine which choices are available. `codex-remote` is a valid
operator-selected remote backend; the current `remote_only` compatibility default remains
`grok-remote` at high effort. Once selected, a failed backend never silently falls back.

## Trigger

Use this skill when:

- the operator invokes `/wb-offload --agent <grok-remote|grok-cli|codex-subagent|codex-remote|cursor-cli|cursor-remote|openrouter-remote> --effort <level> --token-budget <N> "<slice objective>"`
- a bounded junior-subagent slice should run as one synchronous offload pass (`single_pass` semantics)
- token spend must be capped by `token_budget` (cross-cycle circuit breaker)

Do not use it for `codex-cli` offload (deferred until it carries a single-cycle wall-clock
bound), multi-lane fan-out, or work on `main` without a feature task
branch.

## Goal

Select/confirm an available backend + effort, run Fail-Fast pre-flight, materialize the lane
with a reviewer-backend pin, atomically dispatch the brief, run one synchronous
`run_offload_pass` bounded by `token_budget` + `timeout_seconds`, and branch on its typed
outcome. A durable self-verified exact tip can be integrated into its feature branch before
the branch-complete review finishes; that receipt does not authorize a merge to `main`.
No polling and no background monitors.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- Cite stable rule IDs from [heuristics canon](https://github.com/darce/heuristics-canon) at use time; never pin a canon version or date. Every dispatched brief must carry this **link** (not pasted heuristics content, not a version label).
- Task plan: `docs/tasks/internal-synchronous-offload-dispatch-task-plan.md`
  (synchronous atomic dispatch; supersedes the earlier cross-harness plan
  `docs/tasks/internal-cross-harness-token-aware-offload-task-plan.md`,
  whose token-aware profile guardrails still apply)

## Explicit agent + effort

`/wb-offload` requires `--agent` and `--effort`; there is no `--agent auto` and no implicit
backend default.

- **agent**: `grok-remote`, `grok-cli`, `codex-subagent`, `codex-remote`, `cursor-cli`,
  `cursor-remote`, or `openrouter-remote`. Normalized through
  `backend_registry.validate_backend`; anything else (including `codex-cli`) fails fast.
  Under a `remote_only` ledger the explicit-agent requirement is satisfied by the
  engine-resolved default (`grok-remote`) — see Remote preference below.
  `codex-remote`, `cursor-remote`, and `openrouter-remote` are the off-box members (with
  `grok-remote`) of `REMOTE_OFFLOAD_BACKENDS`, derived by `derive_remote_offload_backends`
  from the declared `dispatchable_off_box` capability on each backend-registry row, and
  are explicitly selectable `/wb-offload --agent` values — including under `remote_only`.
- **effort**: one of `low|medium|high|xhigh|auto|inherit` (the canonical
  `_env.WORKER_REASONING_EFFORT_CHOICES` set). Concrete efforts are pinned into the lane
  manifest; `auto|inherit` are resolved by `_env.resolve_auto_reasoning_effort` at execution
  and are **not** pinned.
- **single-cycle bound**: from each profile's `timeout_cap`, read via
  `resolve_adapter_timeout_cap` — `grok-cli` and `grok-remote` derive `max_turns`/`timeout`
  from `token_budget`; `codex-subagent` is guarded by the Codex app-server bridge timeout;
  `cursor-cli` is bounded by a 900-second wall-clock timeout cap (cursor-agent has no
  max-turns flag); `cursor-remote` is bounded by a 900-second wall-clock timeout cap;
  `codex-remote` is bounded by a 3600-second wall-clock timeout cap; `openrouter-remote`
  is bounded by a 1500-second wall-clock timeout cap.

## Remote preference (`remote_only` installs)

When the consumer was installed with `--with-remote`, the bootstrap ledger records
`execution_mode: remote_only` and the preference is **native, not advisory**
(canonical playbook: [offload-remote-playbook](../../docs/workbay/rules/offload-remote-playbook.md)):

- Default delegation is `--agent grok-remote --effort high` (model pin
  `DEFAULT_GROK_MODEL`); the engine resolves omitted backends to `grok-remote` and
  refuses an explicit local backend with the typed `remote_required` outcome.
- **Flag, never substitute**: on `remote_required` (or a failed remote probe),
  surface the refusal — record it as a blocker-style decision and stop; do not
  silently run `grok-cli`/`codex-subagent`. Dropping the policy is
  `repair --no-remote`, an operator act.
- Under `local_ok` (or no ledger) nothing changes: explicit `--agent`/`--effort`
  required as below, with `grok-remote` available as an explicit choice when the
  probe reports it available.

This mode selects the execution location. It does not make grok the only supported remote
backend: explicitly selected `codex-remote`, `cursor-remote`, and `openrouter-remote` remain
available when their registry capability and pre-flight probe permit them. Preserve the
existing grok default as compatibility behavior; do not describe it as a changed runtime
default.

## Token-aware advisory selection (optional)

When the operator asks the orchestrator to choose the agent, run this deterministic selector
and then pass the chosen values explicitly — the selection is a preparation step, not a hidden
default:

1. Reject a missing or non-positive `token_budget` before scoring.
2. Start from the supported offload profiles (`grok-remote`, `grok-cli`, `codex-subagent`,
   `codex-remote`, `cursor-cli`, `cursor-remote`, `openrouter-remote` — under `remote_only`,
   `grok-remote` is the default when no backend is given and every `REMOTE_OFFLOAD_BACKENDS`
   member (`codex-remote`, `cursor-remote`, `grok-remote`, `openrouter-remote`) is
   explicitly selectable); drop any that
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
   - effort valid for the profile; model pin honored (grok defaults to `grok-4.6`;
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
   backend=<agent>, model, reasoning_effort, start_worker=false,
   include_context_packet=true, context_targets=[<the slice's files>])`. Dispatch records the
   brief atomically with the lane params and returns `outcome` + `actionable`; a call without
   `brief` returns `params_only` and the lane stays non-actionable. Re-dispatch with the same
   `dispatch_id` is a no-op (`duplicate_dispatch`) — restart-after-fix never double-enqueues.
   **Default to `include_context_packet=true` with `context_targets` = the slice's files**: a
   deterministic codemap lane-context packet (blast-radius map for those targets) is appended
   to the brief so the worker cold-starts oriented instead of re-deriving structure from
   scratch (implementation note S12); it degrades typed without failing dispatch when the codemap is
   absent. Omit only for a trivial single-file slice. **Also call
   `semantic_reinjection_packet(task_ref=<lane task_ref>,
   anchor_texts=<slice files + brief rationale excerpt>)`** and fold non-empty
   `relevant_lines` into the brief; when `status=skipped` or `relevant_lines` is empty, omit
   the semantic block entirely (clean degrade — same pattern as review-parallel step 4).
   The current in-process ONNX load path is removed; if the semantic service reports
   `offload_semantic_service_unconfigured`, keep that typed marker and continue without
   semantic context. Do not copy or download model assets into a remote sandbox. A configured
   shared service should reuse its installed assets rather than load one model per lane.
   Cold-start workers are the textbook case for reinjection. Dispatch also warns (never
   blocks) when `test_cmd` looks like a whole-package pytest with no `-k`/`::`/file selector,
   a whole-suite `vitest run` with no `--changed` merge-base (including `npx vitest run <dir>`),
   or `npm test` / `npm run test` (`brief_test_cmd_full_suite`); when the command wraps the
   runner in `|| true` / `; true` (`brief_test_cmd_swallows_failure` — a swallowed failure
   reads as green); or when the brief text instructs a full re-baseline
   (`brief_requests_full_rebaseline`) — those time out a grok pass or hide a red suite. Merge-gate
   and lane self-verify use the delta form (`npx vitest run --changed $(git merge-base HEAD
   <integration>)`, or pytest spec paths reachable from the changed modules); the full suite
   belongs to the release cut only. Prefer a scoped `build_lane_test_cmd(pkg, selector)`
   hermetic worktree-venv form for pytest (implementation note).
   On **recurring `self_verify_failed`**, re-run `offload_preflight` and, before the next
   re-dispatch, `uv sync --group dev && make dev-install` in the lane worktree so a stale/missing venv is not
   misread as a code failure (pairs with the step-7 circuit-breaker).
7. `run_offload_pass(lane_id, backend=<agent>, model, reasoning_effort, token_budget=N,
   timeout_seconds=T, turn_timeout_seconds<=T)`. One synchronous call executes the bounded
   execute→review→fix pass and returns a **typed outcome**:
   `handoff_ready | review_complete | escalated | needs_guidance | rate_limited | transport_failure | completed_unreviewed | no_actionable_work | uncommitted_work | token_budget_exceeded | timeout | error | lane_not_found | self_verify_failed | self_verify_inconclusive | composer_violation_quarantined | checkpoint | server_stale_restart_required | admission_deferred | admission_refused | dispatch_refused | remote_required | ceremony_failed | worktree_claim_held | worktree_unrecoverable`.
   `server_stale_restart_required` (implementation note) means the pass engine's own on-disk
   source vanished since import (a concurrent env flip deleted the installed
   package); it is a pre-work refusal — restart the MCP orchestrator server, then
   re-dispatch. Never a code/worker fault.
   Every outcome payload also carries **`commit_landed: bool`** and **`failed_stage`**
   (`execute | self_verify | review | handoff | attestation | null`) so the gate branches
   without git archaeology (implementation note). Green self-verify returns plain `handoff_ready`.
   `needs_guidance` means the
   worker submitted a blocked or verification-failed handoff — the submission landed but
   the work is **not** merge-ready; treat it like a blocker, not a pass. `review_complete`
   (implementation note) is a completed **review lane** (`lane_kind='review'`) — clean tree, unchanged
   HEAD, handoff submitted, and a parseable findings block harvested (`findings_harvest` +
   `findings` surface them); a success with `failed_stage=null`, NOT a wedged needs_guidance —
   inspect the findings, it is not a merge-ready implement handoff. `self_verify_failed`
   means the worker's `TEST_CMD` gate failed before commit — no green commit exists; a typed
   blocker carries the output tail. `composer_violation_quarantined` (grok-cli only) means
   **grok-build contamination** fired *after* a self-verified checkpoint — the commit is
   preserved with evidence and routed to this review gate; never auto-merge it, never
   silent-discard it.
   `checkpoint` means execute stopped on max turns with a self-verified checkpoint preserved
   and a `continuation_dispatch_id` returned — resumable, **not** terminal: re-dispatch with
   that same `dispatch_id` to continue (never re-enqueues). Branch on the outcome enum only —
   never on bare ok/exit codes, and never on log tails.
   **Green-commit-despite-block → verify at the gate, never re-dispatch.** When
   `commit_landed: true` co-occurs with `needs_guidance` / `timeout` / `error` **and**
   `failed_stage ∈ {review, handoff, attestation, null}` (i.e. NOT `execute`/`self_verify`),
   the worker's self-verified commit **already landed** on the lane branch — the block is a
   post-commit stage (review/handoff/attestation) or a wall-clock cutoff, not a failed build.
   Do **not** re-dispatch: a re-dispatch re-runs the pass against an already-green tree and
   deterministically re-hits the same post-commit block → a livelock that burns budget without
   progress. Instead inspect the landed commit (`git log`/diff on the lane branch) and route it
   straight to the review gate. Re-dispatch is correct **only** when `commit_landed: false` —
   no green commit exists (`self_verify_failed`, or `failed_stage ∈ {execute, self_verify}`).
   **Circuit-breaker on repeated `self_verify_failed` (RES-06 / AGT-12).** Re-dispatch after
   `commit_landed: false` is **bounded**: the same `self_verify_failed` on the same `lane_id`
   after **2 consecutive re-dispatches** → stop. Do not open a third re-dispatch. Inspect
   worktree env drift first (`offload_preflight`, then `uv sync --group dev && make dev-install` in the lane
   worktree), and escalate to rescue-lane or a typed blocker if the failure is not a real
   code/test defect. Unbounded re-dispatch of near-identical self-verify blockers is a
   livelock (same signature as a stale/missing worktree venv).
   Note: input-validation refusals (missing/invalid `token_budget`, missing `timeout_seconds`,
   a `turn_timeout_seconds` a backend cannot enforce) are **not** enum outcomes — they return
   `ok:false` with an `error` string before any spend; check `ok` first, then the outcome enum.
   **Host-memory admission (internal).** `admission_deferred` means the host was under
   memory pressure — at pass start (spawn never happened) or mid-pass (the pass parked; any
   dirty work is checkpoint-preserved). It is **retryable**: wait for pressure to drop (watch
   the `make doctor` `host_memory` facet), then re-dispatch fresh. `admission_refused` means a
   resource floor is breached (critical pressure / swap floor / width 0) — do not retry until
   the host recovers. **Override etiquette:** every gated surface takes
   `admission_override=true`; it is an operator escape hatch for false positives only — it is
   recorded as a decision event, it also resets the post-crash breaker, and using it to push
   through genuine pressure defeats the gate that exists to prevent OOM-killing co-resident
   services. Never wrap a dispatch loop in blanket overrides.
   **Remote VM admission.** Before an off-box (`COST_REMOTE`) lane starts,
   `scripts/remote_agent.sh` checks the VM and defers with exit 75 when MemAvailable is
   below `WORKBAY_REMOTE_GATE_MEM_FLOOR_MB` (default 2048; `defer_reason:
   vm_memory_pressure`), when active `grok-lane-*` scopes reach
   `WORKBAY_REMOTE_AGENT_MAX_LANES` (default 20; `vm_lane_cap`), or when free user-slice
   pids fall below `WORKBAY_REMOTE_GATE_PID_FLOOR` (default 64; reported as
   `vm_measured_saturation` with `binding_gauge: vm_pids`). An unreadable value admits
   (the probe fails open). Off-box admission payloads report `derived_width: null`,
   `width_source: off_box_vm_measured`; `max_width` bounds local heavy slots only. Report
   the defer reason and its numbers, not a local slot count.
8. On `still_running` (or a client timeout/disconnect mid-pass), recover with bounded
   `await_offload_pass(pass_id, wait_seconds)` calls — one call per wait window, a coarse
   continuation, **not** a poll loop. The `pass_id` is the one `run_offload_pass` returns on
   its result (or a caller-supplied `pass_id` passed into `run_offload_pass` up front so it is
   known before the call blocks); persist it so a disconnect can reconnect to the same pass.
   **Coordinator session durability (AGT-07).** Before a long-running `run_offload_pass` or
   before ending the coordinator session while a lane is in flight, save a continuation
   packet: `continuation(operation="save", lane_id=..., next_actions="await pass_id
   <X> / re-dispatch dispatch_id <Y>")` (durable `packet_id`; `load_session` auto-injects
   on resume). Pass-id / dispatch-id persistence alone does not cover coordinator-session
   end mid-dispatch.
9. Land each durable, exact-tip self-verified lane to its feature ref before new admission; `/wb-review-slice` runs once off this path.
   Use the canonical workspace root and explicit refs; for example:
   `make lane-land LAND_ARGS="--task <task-ref> --lane <lane-id> --expected-tip <40hex> --integration-ref <feature-ref>"`.
   Use the worker's verified tip, not a later branch lookup. A `moved`, incomplete, or
   indeterminate receipt is a typed refusal/retry state. Lane landing is not a main merge
   and does not wait for review.
10. Run one branch-complete review off the landing critical path. Allow at most one grouped
    fix wave and validate it with focused tests; do not schedule a second review of the fix
    wave. Unresolved HIGH findings block the main gate. A MEDIUM finding gets a one-file fix
    or joins one named wave; LOW and lint-only findings default to defer. Finding status or
    classifier text is not itself a mechanical reap precondition.
11. Before the final main gate, finish any candidate-changing work and run
    `make finalize-plan TASK=<task-ref>` on the feature branch. Then use
    `make wb VERB=ship TASK=<task-ref>`: the deterministic ship receipt composes the
    exact-tip close-check, pinned-SHA merge, and canonical task cleanup without requiring an
    extra operator approval between phases. Treat `cleanup_pending` as incomplete and
    retryable, even if an older child receipt has `ok: true`.

The target lifecycle contract uses exact landing receipts and typed retries, but the active
land/drain/daemon join is not complete on every path. Do not claim background automation has
completed a landing or cleanup; branch on the receipt and let the canonical lifecycle/reap
authority preserve the worktree, sandbox, and ancestry evidence. Resource leases remain held
until their worker exits.

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
- **Inline-only (never offload) — judgment work, not mechanical slices:** golden/snapshot
  recapture, output-normalizer changes, test-isolation fixes, and hermeticity / env-var work.
  A blind grok recapture bakes whatever the current (possibly non-deterministic or flaky)
  output is into the new golden (proven in implementation note S4), and isolation/normalizer/hermeticity
  fixes turn on a human-verified expected value the backend cannot infer from the brief.
  Offload is for **mechanical multi-file slices** (extract-method, gateway migration, line
  ratchets) where a deterministic red test already pins the target.
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

- **Safe parallel width**: schedule only the ready frontier from the lane dependency DAG and
  keep concurrent lane write sets disjoint. Set width from measured runtime capacity and
  headroom (including cgroup memory and pid limits), then apply backend-specific caps and
  active resource leases. There is no repository-wide fixed four-lane ceiling. Remote VM
  caps remain backend-specific compatibility controls. Prioritize finished work and
  `cleanup_pending` retries during admission/drain; do not release worker leases before the
  worker process exits. These joins can return typed incomplete/deferred outcomes and must
  not be reported complete until their receipts prove it.
- **Branch-complete review**: run one `/wb-review-slice` after safe feature-branch landings,
  off the lane admission critical path. Allow at most one named fix wave; tests and the final
  exact-tip HIGH gate validate that wave without a second review pass. This review disposition
  controls main release policy, not whether a self-verified lane can land on its feature ref.
- **Auto-fix offload routing**: re-offload a fix as a lane slice only when (a) a
  deterministic red test exists, (b) the scope is localized, (c) no design decision is
  required, and (d) the batch is ≥2 findings or the estimate exceeds the lane fixed
  overhead (~5–10 min); otherwise fix inline. Triage and `resolve` writes stay with the
  orchestrator.
- **No-LLM mechanics**: deterministic lifecycle steps (worktree create/teardown,
  lane land/reap, close-check, plan-checklist ticks) run as make targets / `wb <verb>`
  one-shots — never through a model (see the lifecycle runbook,
  `../../docs/workbay/wb-lifecycle-runbook.md`). Keep the canonical teardown authority in
  control; do not force-delete an active worker's tree or archive ahead of integration and
  preservation checks. If lifecycle joining is incomplete, return the typed pending state
  for an explicit retry.
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
  suite — that belongs to the release cut only). Merge-gate and lane self-verify use the
  delta form: `npx vitest run --changed $(git merge-base HEAD <integration>)` (the merge-base
  sha, never bare `--changed`), or pytest spec paths reachable from the changed modules.
- **Non-Python / no-root-pyproject consumers**: the secure sandbox is a shallow clone that
  excludes gitignored artifacts for every ecosystem (`node_modules`, `vendor`, `.venv`).
  Sandbox provisioning runs root-level `uv sync` only when a root `pyproject.toml` is
  present; otherwise it records `provision_skipped: no_python_project` and continues
  (sanctioned skip — do not set `WORKBAY_GROK_SANDBOX_PROVISION=0` for this). Put the
  ecosystem's dep install at the head of `TEST_CMD` so verification installs inside the
  sandbox (e.g. `npm ci && npx vitest run --changed $(git merge-base HEAD <integration>)`,
  or `composer install && …`). The same pattern applies to a Python monorepo with
  per-package pyprojects but no root `pyproject.toml`: root `uv sync` cannot succeed
  there either, so consumers own env setup via `TEST_CMD`.
- **Known-red baseline ref**: the recorded `test_result` row capturing pre-existing failures,
  so the worker neither re-diagnoses them nor mistakes them for its own regression.
- **Heuristics link (T17)**: every dispatched brief **must** include the versionless relative
  link to [heuristics canon](https://github.com/darce/heuristics-canon)
  plus "cite stable rule IDs at use time". Do **not** paste heuristics body text and do **not**
  pin a canon version or date.
- **No subagent steps for grok lanes (T6)**: when `backend=grok-cli`, the brief must **not**
  request subagent-requiring steps (`/wb-review-slice`, subagent fan-out reviews). Use
  in-lane `/wb-review-code` only; reserve `/wb-review-slice` for the orchestrator merge gate.
  `dispatch_lane_work` emits a named warning (`grok_brief_subagent_steps`) when a grok brief
  still mentions those steps — warn only, never block.
- **Grounding accuracy**: take symbol/key anchors in briefs from `search_graph` /
  `get_code_snippet` or a direct file read; a grep hit does not establish symbol identity.
  Briefs carry only verified literals.

### Per-slice review (single-reviewer)

After each slice the worker runs `/wb-review-code` (single-reviewer, **no** subagent fan-out)
followed by `/wb-auto-fix` on its own findings. This in-lane pass is an explicitly
**non-authoritative smoke test** — it catches obvious regressions before the next slice but
does not gate feature-branch landing. The orchestrator runs one `/wb-review-slice` after
feature-branch landing. The exact-tip close-check is the **sole merge gate on the main path**
and blocks unresolved HIGH findings.

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
  (`grok-cli`/`grok-remote` derived `max_turns`/`timeout`; codex-subagent bridge timeout;
  `cursor-cli` 900s and `cursor-remote` 900s wall-clock caps; `codex-remote` 3600s
  wall-clock cap; `openrouter-remote` 1500s wall-clock cap), not `token_budget`.
- Open-circuit keeps the worktree diff and records a distinct `token_budget_exceeded` blocker.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "A selected backend outage means I should try the other backend." | There is **no fallback**: an outage after selection hides failure and can amplify cost (Release It!). | Fail fast, surface the blocker, stop. |
| "I'll omit `--agent`/`--effort` and let a default apply." | Explicitness is the safety contract; there is no implicit backend default and no `--agent auto`. | Require explicit `--agent` and `--effort` on every invocation. |
| "I'll pin `auto`/`inherit` effort into the manifest." | Lane-manifest validation rejects non-concrete efforts; they are resolved at execution by `_env.resolve_auto_reasoning_effort`. | Pin only concrete efforts; leave `auto`/`inherit` unpinned. |
| "I'll omit `backend` on dispatch/`run_offload_pass` — defaults are fine." | Offload resolution (`resolve_offload_backend_for_execution_mode`) defaults to `grok-cli` under `local_ok` / `grok-remote` under `remote_only` — not the selected agent. Some older lane-tool keyword defaults still say `codex-subagent` (`api.py` worker tools, `generate_agent_config`); those are a different surface from `/wb-offload` pass resolution. | Pass `backend=<agent>` explicitly on every `dispatch_lane_work`/`run_offload_pass`. |
| "The grok lane will run `/wb-review-slice` per slice." | The grok adapter hardcodes `--no-subagents`, which disables the subagent fan-out primitive `/wb-review-slice` requires. | Run `/wb-review-code` in-lane; reserve `/wb-review-slice` for the orchestrator gate. |
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
| Lane wedged with repeated `token_budget_exceeded` blockers | Open-circuit tripped; stop the lane, inspect the kept worktree diff, do not re-dispatch. Escalate via rescue-lane when the lane is unrecoverable in place. |
| Same `self_verify_failed` after 2 consecutive re-dispatches on one lane | Circuit-breaker (step 7): stop, `offload_preflight` + `uv sync --group dev && make dev-install` in the worktree, or escalate (rescue-lane) — never open a third identical re-dispatch. |
| Near-duplicate blockers spam the same `task_ref` within a short window | Dedup/rate-limit: collapse identical blocker text for the same task into one row with an occurrence count (occurrence_count pattern); do not record a new full blocker per re-dispatch tick. |
| Brief tells the lane to run `/wb-review-slice` per slice | Replace with `/wb-review-code` single-reviewer; route `/wb-review-slice` to the orchestrator gate. |

## Recovery

- Pre-flight failure: fix the named precondition (agent available, valid effort, model pin, clean worktree, positive `token_budget`) and re-run; nothing was dispatched.
- Backend outage mid-run: the lane fails fast with a blocker; no fallback to another backend, no retry storm — report and stop.
- Budget exceeded: the worktree diff is preserved with a `token_budget_exceeded` blocker; review the partial diff at the gate, then continue or abandon.
- Recurring `self_verify_failed` / env drift: after the second consecutive failure, stop re-dispatch; run `offload_preflight` and `uv sync --group dev && make dev-install` in the lane worktree before any further attempt; escalate to rescue-lane if still red.
- Coordinator session about to end mid-pass: save `continuation(operation=save, ...)` with `pass_id` / `dispatch_id` next-actions before exit; resume via `load_session` rather than inventing a new dispatch.
- Repeated identical blockers on re-dispatch: when recording a blocker, warn and collapse near-duplicates for the same `task_ref` within a short window (adopt the `occurrence_count` pattern) instead of flooding the inbox.

## Convergence Criteria

- Explicit `--agent` and `--effort` resolved; pre-flight passed with zero dispatch on failure.
- Manifest carries `preferred_backend=<agent>` (and concrete effort); review phase observable on the selected backend.
- Pass executed via `run_offload_pass` with mandatory `token_budget` + `timeout_seconds`; typed outcome handled (`await_offload_pass` only on `still_running`/disconnect).
- The exact self-verified tip is preserved and, when accepted, landed only to the named
  feature ref before new admission; this is not a main merge.
- One `/wb-review-slice` branch-complete review and at most one fix wave are recorded;
  unresolved HIGH findings block main integration while lower severities follow their
  default disposition.
- The final main gate uses the finalized exact candidate; canonical cleanup is complete
  before `wb ship` reports success, and `cleanup_pending` remains retryable.

## See Also

- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../auto-fix/SKILL.md](../auto-fix/SKILL.md)
- [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md) (wedged lane, repeated `token_budget_exceeded` / `self_verify_failed` circuit-breaker)
