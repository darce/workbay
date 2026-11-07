# Branch Review

## Overview

Use this skill for code and workflow diffs on feature branches. It runs the branch-review checklist, records findings before chat output, and closes with a durable review verdict.

## Trigger

Use this skill when:

- reviewing a feature branch diff before merge
- running `make review-run` for lane/local working-tree review
- auditing implementation changes under `apps/`, `packages/`, `scripts/`, or `mk/`

Do not use it for task plans, epics, ADRs, or other planning artifacts.

## Goal

Produce a branch-review verdict with MCP-recorded findings, a recorded review run, and clear evidence about whether the branch is genuinely merge-ready.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/workbay/rules/branch-review-guide.md](../../../docs/workbay/rules/branch-review-guide.md)
- Heuristics: follow the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md) (one pinned, digest-verified release). For this skill's artifact, route the diff by what it changes, keep only the rows whose trigger fires, apply the Verification of every card that lists a kept ID, and cite `[FAM-NN]` with the rule's bold name and Source cell. `B` blocks, `S` needs a named exemption, and a `J` rule you lack context for is escalated, not skipped.

This skill owns branch-review execution order. The guide owns the detailed checklist.

## Review contract and parallel frontiers

Every reviewer receives the same exact base and candidate SHAs, diff/slice scope, touched contracts, test commands and receipts, prior findings and review verdicts, codemap anchors with index freshness, semantic prior-work context with retrieval status, and applicable canon rows with their IDs and sources. Pass this packet to slice, whole-branch, and integration reviewers alike. Missing graph or semantic capability is an explicit context limitation; similarity and unavailable retrieval are not correctness evidence. Use actual source to confirm or refute each claim, and reject unsupported conclusions rather than promoting uncertainty to a finding. Preserve the complete branch-mode checklist, verdict, and recording authority even when work is partitioned.

Use [GRPH-01] **Model the work as a DAG, then topological-sort** (Source: The Algorithm Design Manual, ch. 7) and [GRPH-33] **A node without a contract cannot be parallelised** (Source: practice label bootstrap): each review lane names its input commit, scope, output schema, and provenance. Independent read-only review frontiers may run concurrently with isolated finding writes. Order shared target/ref mutations and git integration explicitly; unrelated branches need no ordering edge. Merge and adjudicate results deterministically, preserving reviewer lineage under [GRPH-14] **Record provenance as a walkable lineage graph** (Source: Building Knowledge Graphs, ch. 8 + Mental Models, ch. 8). Runtime canon guidance: [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md).

Remote execution and payload authorization come from the selected installed capability/preflight receipts and the shared native adapter (`scripts/remote_agent.sh` and its payload twin). Preserve resolved invocation values from those receipts; do not invent backend/model defaults or substitute another backend or local execution after selection. Surface a typed refusal when the selected execution cannot proceed.

## Core Process

0. **Ensure task scope before any cwd-resolving MCP read.** Every other step calls `load_session`, `search_handoff`, `review_findings`, etc. — these resolve the active task from cwd unless `task_ref` is passed explicitly. On cold start:
   - Run `make status LIFECYCLE_ARGS=--json` from the target worktree first so branch/worktree alignment and the active task surface are confirmed through the public facade.
   - If more than one task may be active, run `make tasks LIFECYCLE_ARGS=--json` before any MCP read so you can pick the correct `task_ref` without spelunking handoff state directly.
   - **Resumption:** work from the existing task's `target_worktree_path`; pass `task_ref` explicitly to all subsequent MCP calls.
   - **Ad-hoc on main (e.g. reviewing a merged commit):** register a maintenance task first — `set_handoff_state(task_ref="MAINT-<slug>-<YYYYMMDD>", objective="...", status="in_progress", target_branch="main")` — and pass that `task_ref` forward.
   - **`Ambiguous active task` error:** use the canonical escape hatch in [lifecycle-recovery.md § Surface 5](../../../docs/workbay/rules/lifecycle-recovery.md#surface-5-ambiguous-active-task-raise) (`task_ref=` / `TASK=`, or `make task-reap` then `make context`; exit `2` on ambiguity).

1. Start with a real review scope and pin the base and candidate to exact 40-hex SHAs. `make review-run` is for lane/local working-tree review because `review_runner.py` only inspects local changed files; its verdict does not by itself cover the committed branch. For committed feature-branch diff review, load the latest slice review packet when available or review `git diff <base>...<candidate>` scope directly. If `workbay-orchestrator-mcp` is unavailable, use the handoff-only context path from `branch-review-guide.md`: `load_session(task_ref=<task-ref>, read_profile="review_packet", response_budget_bytes=16000)` -> `search_handoff(queries=["slice_complete"], record_types=["decision"], limit=1)` -> `get_verified_tests` -> `review_findings(list)` and review against branch-diff scope. This context fallback does not change a selected execution backend. Assemble the review contract above before any reviewer starts.
2. Pre-triage with `get_review_findings_summary` and `reconcile_review_findings` so old open findings are understood before new detection passes begin. When orchestrator is unavailable, state that the pass is using `branch_diff` fallback scope instead of `slice_packet`.
3. Check prior whole-branch review history with `review_runs(review={"operation": "list", "task_ref": "<owner-task-ref>", "subject_path": ".", "review_mode": "branch", ...})`. Keep this stable workspace-relative subject across candidate changes so inventory and prior rounds share one history.
4. Run the complete branch-review checklist against the actual diff, touched contracts, and fresh verification evidence for the pinned candidate. When a codebase-graph MCP is connected, use the [Codebase-graph capability](#codebase-graph-capability-optional) block — especially `query_graph` over TESTS edges to investigate coverage gaps. A missing graph edge is a lead to verify against source/tests, not proof of a defect or a requirement to edit a test file.
5. Record every accepted finding with `review_findings`. Use `batch_record` for multi-finding passes. Include scope, commit, source anchors, mechanism, and verification evidence; reject unsupported hypotheses with reasons in the verdict decision, or record a named evidence blocker when adjudication remains incomplete. Prior similarity, uncertainty, or nonreproduction alone cannot justify a confirmed finding, fixed status, or `wontfix`.
6. Decide the verdict: `pass`, `pass_with_findings`, `conditional_pass`, or `fail`.
   Only blocking findings decide it. Fail, or hold the merge, only on a `[blocking]` finding tagged `[listed-open]` or `[introduced]`, or on a high-severity `[blocking]` `[pre-existing]` finding ([review-blocking-scope.md](../../../docs/workbay/rules/review-blocking-scope.md)). `[follow-up]` findings never fail the verdict: record each one as a tracked open finding and report it in full.
7. Record the verdict decision with `record_event(event={"event_kind": "decision", ...})`, explicitly bound to the owner `task_ref`. Its rationale records the exact 40-hex base and candidate SHAs, candidate tree, diff/slice scope, and verification evidence. Use the supported `actor.branch` and `actor.commit_sha` fields for the reviewed branch and candidate SHA; retain base/tree provenance in the decision rationale.
8. Record the whole-branch review run with `review_runs(review={"operation": "record", "review_run_id": "<unique-run-id>", "session": "<session-id>", "task_ref": "<owner-task-ref>", "review_mode": "branch", "subject_kind": "branch", "subject_path": ".", "verdict": "<verdict>", "verdict_decision": "<recorded-verdict-decision-id>", "actor": {"branch": "<reviewed-branch>", "commit_sha": "<candidate-40hex>"}, "max_review_rounds": 5})`. Use the same stable `subject_path` for every whole-branch record and history query; the linked verdict decision carries the exact base/candidate/tree provenance. `branch_diff` is only the honest review-scope label; it is not a valid persisted `subject_kind`. A new tip or run ID does not create a new review-round budget.
9. Re-check whether open findings remain. If none remain and the branch claims readiness, run `make handoff-close-check TASK=<task-ref>` as the **local pre-ship merge-readiness gate** on the exact verified active candidate. Require an actual ready receipt with evaluated, unskipped gates; preserve the unresolved HIGH/main integration gate and the landing policy for other findings. Any candidate change requires fresh exact-SHA evidence and a renewed gate.
   - Follow [branch-lifecycle steps 9–11](../branch-lifecycle/body.md#core-process) for canonical ship, integration, and owner cleanup. Lifecycle owns the live owner transition to `done`; review and the local gate alone do not complete it. Only after successful ship proves integration and cleanup complete, run the separate **raw strict post-ship gate** from the primary checkout: `integrity_check(payload={"kind":"close","task_ref":"<owner-task-ref>","enforce":true,"require_fresh_tests":true,"current_commit_sha":"<receipt.integration_sha>"})`.
   - Require the live owner row to be `done`, fresh tests and structured slice-complete evidence at that exact integration SHA, `ok: true`, `data.ready_to_close: true`, and evaluated, unskipped gates. If integration changes the SHA, verify and record evidence at that new exact SHA; ancestry or tree equivalence is insufficient. Keep cleanup-pending or raw-close-refused outcomes incomplete and report the typed blocker through the canonical lifecycle path; do not manually set `done`, recreate state, or change finding dispositions to force a pass.
10. **Closing findings correctly (author side).** Close only explicit `review.finding_ids` whose fixes were individually verified on the exact current commit. Use the existing single-`review` signature, preferably one ID per call so evidence stays specific: `review_findings(review={"operation":"resolve", "task_ref":"<owner-task-ref>", "finding_ids":["<verified-finding-id>"], "resolution_notes":"<scope, fix, and provenance>", "verification_evidence":"<finding-specific source/diff proof and verification command/result at this SHA>"})`. Keep notes ≤ 500 chars and evidence ≤ 2000 chars. Confirm the task/lane scope, original finding anchor, clean committed fix, and exact-SHA verification before writing; commit ancestry or automatic test evidence alone does not prove every finding fixed. `resolve` auto-fills `verified_commit_sha` from the clean resolution context; confirm it matches the exact 40-hex SHA you verified. Preserve descendant, batch-close, and reopen guards and inspect each receipt. Leave pending, context-blocked, missing, or unverified IDs open; do not widen selection or bypass a refusal with manual `update(status="fixed")`. Any amend, squash, rebase, or new commit invalidates the old candidate proof: verify the new SHA before closure or readiness claims.
11. **Close the on-main MAINT row** when the audit pass is complete: `make plan-done TASK=<maint-ref>` (not `make task-finish`).

When MCP tools are unavailable but the CLI wrapper is available, use the CLI wrapper as the fallback MCP surface (`mcp-workbay-handoff --workspace-root <repo> review-findings ...`, `review-runs ...`, `event ...`). Do not write directly to `.task-state/handoff.db`; raw SQL bypasses validation, dashboard/event hooks, and future server-owned side effects. An explicitly delegated remote lane without MCP returns its accepted/rejected hypotheses, exact scope/SHA, evidence, and unresolved dependencies in the assigned report schema for the coordinator to record. Label these as pending coordinator recording, without claiming MCP receipts or branch readiness. Otherwise, if neither surface is available, report the recording blocker.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "The diff is small, so a quick skim is enough." | Small diffs still break contracts, test freshness, and branch isolation. | Run the full checklist. |
| "I'll mention the issue first and record it later." | Unrecorded findings do not exist to the gate or the next reviewer. | Record findings before reporting them. |
| "Prior open findings are probably stale anyway." | Re-reviewing without reconciliation creates duplicates or misses still-open regressions. | Pre-triage existing findings first. |
| "Recording the fix as `update(status="fixed")` is more explicit." | Manual status writes do not substitute for finding-specific proof or close guards. | Use step 10's explicit `finding_ids`, exact `verified_commit_sha`, scope/provenance notes, and guard evidence; inspect every receipt. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| Review starts without a clear diff or slice packet | Step 1: fix scope before continuing. |
| Existing open findings were ignored | Step 2: reconcile before new detection passes. |
| Verdict is about to be reported without a review run | Step 8: record the run first. |
| Findings are being closed without individual exact-commit proof | Step 10: use explicit `finding_ids` and finding-specific evidence; leave the rest open. |
| Diff growing a second responsibility | [../refactor/SKILL.md](../refactor/SKILL.md): run `refactor` on the touched area before approving scope creep. |

## Recovery

- If the slice packet is missing, state that the review is branch-diff fallback scope.
- If `workbay-orchestrator-mcp` is not loaded, switch to the documented handoff-only fallback path instead of stopping at the missing slice-packet call.
- If MCP is unavailable, use the CLI wrapper or the explicitly assigned remote reporting contract; coordinator recording remains required before a branch verdict is complete.
- If findings recur from a prior pass, update or reopen them rather than creating duplicates.

## Codebase-graph capability (optional)

If a codebase-graph MCP is connected (exposes `get_architecture`, `trace_path`, `detect_changes`, `query_graph`), use it to map the branch diff to symbols, flag un-updated call sites, and scope blast radius. Prefer `query_graph` over TESTS edges to flag changed code with no covering test. Skip when absent.

## Convergence Criteria

- Findings mentioned to the user are already recorded in MCP.
- A branch-mode review run exists for the pass.
- A verdict decision exists for the review.
- The final response prints the MCP write receipt with row ids, e.g. `MCP writes: 4 findings batch-recorded; verdict decision id 2538 (branch_review_...); review_run id 414. DASHBOARD.txt refreshed. Handoff updated: decision branch_review_... recorded.`
- The review scope is identified honestly as `slice_packet` when packet-backed or `branch_diff` when running fallback scope.
- Merge-readiness claims are backed by fresh evidence, not assumption.
- Every closed finding has explicit selection, scope/provenance notes, and individual proof at its verified commit; unresolved IDs remain visible.
- Review/fix passes have named scope, evidence targets, and budget. Continue targeted passes when fresh evidence demonstrates defects; if progress or budget is exhausted, report the unresolved IDs and blocker rather than assuming convergence. Any candidate change requires affected-contract review and exact final-candidate verification, with a full branch pass when scope or integration contracts changed.

## See Also

- [../../../docs/workbay/rules/branch-review-guide.md](../../../docs/workbay/rules/branch-review-guide.md)
- [../../../docs/workbay/rules/lifecycle-recovery.md](../../../docs/workbay/rules/lifecycle-recovery.md) — Ambiguous active-task escape hatch (Surface 5)
- [../planning-review/body.md](../planning-review/body.md)
- [../refactor/SKILL.md](../refactor/SKILL.md)
