# Review Parallel

## Overview

Use this skill when one pass of `branch-review` is not enough. The coordinator stays in-process, fans out N ephemeral reviewers via the cheapest subagent primitive its harness exposes, scopes each reviewer to its own `task_ref`, then merges every reviewer's findings under the coordinator's `task_ref` via `review_findings(operation="merge")`. All supported harnesses (Claude Code, Codex, Copilot, Grok) produce the same MCP state — only the fan-out primitive differs.

## Trigger

Use this skill when:

- a branch review's coverage target is 2 or more reviewers
- a diff is large or cross-cutting enough that independent passes de-risk bias
- the user explicitly invokes `/review-parallel`

Do not use it for single-reviewer passes (stay with `branch-review`), for planning artifacts (use `planning-review`), or for worker-daemon-backed continuous-polling orchestration (`dispatch_lane_work(start_worker=True)` is the right tool for multi-hour operator pipelines, not bounded reviews).

## Goal

Produce one merged finding set under the coordinator `task_ref` whose `merged_from` provenance names every source reviewer, with reviewer source rows retired to terminal status after merge (content preserved for audit) and a single branch-mode review run recorded for the combined pass.

## Canonical Policy

- [../../../docs/workbay/rules/branch-review-guide.md](../../../docs/workbay/rules/branch-review-guide.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/tasks/17.0/internal-parallel-reviews-and-autonomous-bug-fix-loop-task-plan.md](../../../docs/tasks/17.0/internal-parallel-reviews-and-autonomous-bug-fix-loop-task-plan.md)
- Use [../../../docs/workbay/rules/reasoning-discipline.md](../../../docs/workbay/rules/reasoning-discipline.md) for the reasoning posture underneath this skill (diagnose-before-execute, position-with-counter-case, escalation tiers).

This skill owns the coordinator protocol and the per-harness routing decision. The `branch-review` skill owns the checklist each reviewer runs.

## Harness Routing

The coordinator must call the **in-process** subagent primitive the current harness exposes. Shelling out to an external CLI from inside an active interactive session inverts the cost premise this skill is built on (the motivating assessment showed 1–2 orders of magnitude savings vs worker-daemon orchestration for bounded reviews).

<!-- BEGIN GENERATED: harness-routing-table -->
| Harness detected | Primitive the coordinator uses | Notes |
| --- | --- | --- |
| _routing table rendered from harness-protocol.yaml at plugin emission time_ | | |
<!-- END GENERATED: harness-routing-table -->

### Capability detection & deterministic degradation contract

Do not assume the routing-table primitive is wired in the current runtime. **Probe first:** call `list_available_backends` on `workbay-orchestrator-mcp`; if the preferred backend reports `is_available=false`, `availability_state="declared_not_installed"`, or lacks `supports_sync_turn`, run one bounded probe of `run_structured_turn` before giving up. On any negative signal, take the **single deterministic fallback** — two in-process sequential read-only reviewers under scoped `<coordinator>-REV-<round>-<letter>` task_refs, merged as usual — rather than improvising a different degradation per run. Record the downgrade so it is auditable: a decision whose id carries the `routing_downgrade` convention (`record_event(decision="<tag>_routing_downgrade_<coordinator>", rationale="preferred backend <name> unavailable; used in-process reviewers")`), and reference that decision id from the combined `review_runs` record via its `verdict_decision` field (the only free-text slot the record exposes — there is no separate `reason` field). This makes Claude Code, Codex, Copilot, and Grok converge on the same recorded shape whether or not the preferred backend exists.

An **unreachable orchestrator MCP is itself a negative signal**, not a separate failure mode: if `list_available_backends` errors because `workbay-orchestrator-mcp` never launched (e.g. the pinned `uvx mcp-workbay-orchestrator[bridge]@<ver>` environment cannot be built on an offline host or a mirror without the bridge wheel), take the same deterministic fallback. The fallback is reachable by construction because it runs in the coordinator's own process — the coordinator performs the two sequential read-only review passes itself (Claude Code may also use its in-process `Agent` tool) and merges as usual; it must not route through `run_structured_turn` or any other tool on the orchestrator server. Record the same `routing_downgrade` decision, with the rationale naming the unreachable server. Only the orchestrator-recorded audit extras (lane activity, turn metrics) are lost in this mode; the handoff MCP write path (`review_findings`, `review_runs`, `record_event`) is a different server and stays available.


## Core Process

0. **Ensure coordinator task scope before any cwd-resolving MCP read.** If `get_handoff_state(sections="identity")` returns `Ambiguous active task`, archive stale MAINT-* rows in one shot with `make maint-archive-stale` (or `MAINT_ARCHIVE_ARGS="--yes"`) and re-run `make context` (exits `2` on this ambiguity). If no task is active and this is ad-hoc work on main, register a coordinator MAINT task first — `set_handoff_state(task_ref="MAINT-<slug>-<YYYYMMDD>", objective="...", status="in_progress", target_branch="main")` — before step 1.

1. Confirm the coordinator has an active `task_ref`. `get_handoff_state(sections="identity")` is the bounded read. If there is no active task, abort with a clear precondition error; do not write anything.
2. Pick one round token for this review run (for example a short date stamp or commit prefix) and `N = reviewers_count` (default 2). Assign each reviewer a scoped `task_ref` following `<coordinator-task-ref>-REV-<round>-<letter>`, using letters A, B, C, … in order. Record the round token and coordinator/reviewer mapping in the opening decision: `record_event(event={"event_kind":"decision","decision":"<tag>_review_parallel_open_<coordinator>","rationale":"round=<round>; mapping=<REV-A>, <REV-B>, ..."})`. **Preflight each planned scratch ref** with `review_findings(review={"operation":"list","task_ref":"<scratch-ref>","limit":1})` and require `total_matching == 0`; if any ref is non-empty, pick a new round token, record why, and re-preflight before fan-out. Prefer a token embedding the reviewed commit prefix (naturally unique per diff); the preflight is best-effort against earlier runs, not a concurrency lock against a simultaneous review of the same coordinator.
3. For each reviewer index, invoke the subagent primitive from the routing table above. The reviewer receives:
   - the reviewer prompt (default or the value of `reviewer_prompt_template` resolved under `config/agent-workflows/prompts/review-parallel/`),
   - the scoped `task_ref` it must write under,
   - the diff or slice packet under review.
4. Each reviewer records its findings via `review_findings(review={"operation":"batch_record", "task_ref":"<REV-X>", ...})` and returns a short summary to the coordinator. A reviewer must never write directly under the coordinator `task_ref` while another reviewer is still running.
5. After every reviewer returns, the coordinator merges with source retirement: `review_findings(review={"operation":"merge","source_task_refs":[<REV-A>, <REV-B>, …],"target_task_ref":"<coordinator>","retire_sources":true})`. This re-records each reviewer's findings under the coordinator `task_ref` with `merged_from` provenance pointing back at the source, then retires merged source rows to `superseded` in the same transaction. Reviewer source content remains available for audit; rows become terminal and must not stay `open`. Use `retire_sources=false` only when a legacy caller explicitly needs the old non-retiring merge behavior. **Server-version caveat:** `retire_sources` and the `superseded` status ship in a later handoff slice; a server predating them silently ignores the unknown `retire_sources` key — the merge still copies findings but leaves source rows `open`. Until that slice is available, `retire_sources=true` is the preferred form but is a no-op there, so after merging list each merged `*-REV-*` ref's rows and mark them `wontfix` as the interim terminal teardown, then confirm their `status='open'` count is zero.
6. Record the combined review run: `review_runs(operation="record", review_mode="branch", ...)` with `subject_path` pointing at the branch or slice artifact reviewed. Reviewer sub-runs remain under their own `task_ref`s as the audit trail.
7. Decide the combined verdict (`pass`, `pass_with_findings`, `conditional_pass`, `fail`) and record it as a decision tied to the coordinator `task_ref`.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I'll just have each reviewer write directly under the coordinator task_ref; simpler than merging." | Two reviewers racing on the same `task_ref` produces interleaved rows with no provenance; downstream reviewers cannot tell which finding came from which pass, and deduplication becomes guesswork. | Keep each reviewer's writes scoped to its own `task_ref`; merge at the end. |
| "The coordinator is already a Claude Code session, so I'll call the Claude CLI adapter for the reviewers." | Shelling out to `claude` from inside a running Claude Code session spins a full new harness per reviewer. That erases the in-process savings this skill is built on. | Use the in-process `Agent` tool from Claude Code. Reserve `ClaudeCodeAdapter` for external orchestration. |
| "We only need one reviewer this time; I'll still run `/review-parallel` with N=1." | A single reviewer does not benefit from the merge step and the extra coordinator overhead is pure waste. | Use `branch-review` directly when `reviewers_count <= 1`. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| Two reviewers writing under the same `task_ref` | Step 2: assign scoped `<task>-REV-<round>-<letter>` task_refs before fan-out. |
| Coordinator shelling out to a CLI inside an interactive harness | Step 3: use the in-process primitive from the routing table. |
| Coordinator records findings before merge completes | Step 5: merge first, then record verdict/decision under the coordinator `task_ref`. |
| No combined `review_runs` row after fan-out | Step 6: record the branch-mode review run for the combined pass. |

## Recovery

- If a reviewer crashes mid-run, record a blocker under its scoped `task_ref` and exclude that reviewer from the merge. List that ref's open findings and mark each `wontfix` with notes that the reviewer was excluded and not merged. The remaining reviewers still produce a valid combined pass.
- If merge fails on an empty source list (validated error), at least one reviewer produced zero findings. Decide whether to retry that reviewer or proceed with the remaining source task_refs.
- If legacy scratch refs still have `open` rows after merge (pre-retirement drift), list each `*-REV-*` ref under the coordinator, run `reconcile_review_findings(task_ref=<coordinator>, apply=false)` and inspect `checks.reviewer_scratch_drift`. Rows with `eligible_for_retirement=true` already have a coordinator terminal copy — re-run with `apply=true` to retire them via the merge-managed `superseded` path. Rows without a coordinator copy need explicit merge or `wontfix`; verify every scratch ref has `status=\"open\"` count zero before closing the review round.
- If the coordinator cannot route the fan-out (harness primitive missing), apply the single **deterministic degradation contract** under Harness Routing above — capability-probe, then the in-process two-reviewer fallback under scoped `<coordinator>-REV-<round>-<letter>` task_refs, recording the `routing_downgrade`. Do not improvise a different per-run fallback (e.g. a one-off sequential `branch-review`); the contract is the one canonical path.

## Convergence Criteria

- Every reviewer finding is recorded under its scoped `task_ref` before merge.
- `review_findings(operation="merge")` has produced N × K rows under the coordinator `task_ref` with `merged_from` provenance on every row (N = reviewer count, K = per-reviewer finding count).
- Reviewer source rows are retired (`superseded` after merge, or `wontfix` for excluded refs); audit content is preserved but no merged reviewer ref retains `open` rows.
- One branch-mode `review_runs` row records the combined pass under the coordinator `task_ref`.
- A verdict decision on the coordinator `task_ref` ties the review to its commit SHA.

## External Orchestration Contract

The canonical surface for *external* orchestrators (no interactive harness) is the `BackendAdapter` protocol at [backend_adapter.py](../../../packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_adapter.py) resolved via [backend_registry.py](../../../packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_registry.py) (`get_adapter(<kind>)`). Registered kinds today: `structured-turn` (always-available in-repo anchor; see [structured_turn.py](../../../packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/structured_turn.py)), `codex-cli`, `claude-code`, `codex-subagent`, `copilot-host`, `local-model-openai`. Cross-vendor equivalence is proven by [test_cross_vendor_subagent_equivalence.py](../../../packages/mcp-workbay-orchestrator/tests/test_cross_vendor_subagent_equivalence.py) on the `(count, severity_distribution, verified_commit_sha)` tuple. Inside an active harness session the Harness Routing table above is the contract; the `BackendAdapter` surface is reserved for external orchestration.

## See Also

- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../../../docs/workbay/rules/branch-review-guide.md](../../../docs/workbay/rules/branch-review-guide.md)
- [../../../packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_adapter.py](../../../packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_adapter.py)
