# Planning Review

## Overview

Use this skill for durable review of planning artifacts. It applies the planning-review checklist, records findings in MCP, refreshes `DASHBOARD.txt`, and closes with a planning-mode review run plus verdict decision.

## Trigger

Use this skill when:

- reviewing a task plan, epic, ADR, roadmap, or assessment
- running `make plan-review DOC=<path>`
- validating that a planning artifact is ready for implementation

Do not use it for implementation diffs or pre-review triage of plan quality.

## Goal

Produce a planning-review verdict backed by recorded findings, printed handoff gap ids (`finding_id`), a refreshed `DASHBOARD.txt`, and a planning-mode review run, with the artifact either cleared for the next stage or blocked on concrete gaps.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/workbay/rules/planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md)
- [docs/workbay/templates/TASK_PLAN.template.md](docs/workbay/templates/TASK_PLAN.template.md) for task plans under `docs/tasks/`
- Heuristics: follow the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md) (one pinned, digest-verified release). Route this skill's artifact as **architecture proposal or adr** (plus **estimate or forecast in a plan** when the plan carries estimates), keep only the rows whose trigger fires, apply the Verification of every card that lists a kept ID, and cite `[FAM-NN]` with the rule's bold name and Source cell. `B` blocks, `S` needs a named exemption, and a `J` rule you lack context for is escalated, not skipped.

This skill owns planning-review execution order. The guide owns the detailed checklist and severity model.

## Core Process

0. **Ensure task scope before any cwd-resolving MCP read.** `search_handoff`, `review_runs`, `review_findings` without `task_ref` all resolve from cwd. On cold start:
   - Run `make status LIFECYCLE_ARGS=--json` from the target worktree first so task/branch alignment comes from the public facade rather than a raw MCP read.
   - If the planning pass might touch more than one active task, run `make tasks LIFECYCLE_ARGS=--json` before choosing a `task_ref`.
   - **Resumption:** work from the existing task's `target_worktree_path`; pass `task_ref` explicitly.
   - **Ad-hoc planning review on main:** register a maintenance task first — `set_handoff_state(task_ref="MAINT-<slug>-<YYYYMMDD>", objective="...", status="in_progress", target_branch="main")`.
   - **Pre-worktree implementation row:** if the implementation task's `target_branch` has no linked worktree yet, do not write MCP review state against that implementation row. Those writes can fail with `WorktreeNotFoundError`. Use a `target_branch=main` `MAINT-*` row for the planning pass, run `make task-start` once the accepted baseline exists, or wait until the feature worktree exists.
   - **`Ambiguous active task` error:** pass `task_ref` explicitly; if stale rows cause the ambiguity, run `make task-reap` (dry-run) to see closeable vs active rows; `make task-reap REAP_ARGS=--apply` closes only closeable rows, then re-run `make context`. `make context` exits `2` on this ambiguity.

1. **Execute one bounded remote round through the normal gateway.** From the target worktree run:

   ```sh
   make plan-review DOC=<path> LIFECYCLE_ARGS="--task-ref <task-ref> --base-sha <full-commit> --round 1 --scope-decision <decision-id> --review-run-id <stable-id> --session <session> --json"
   ```

   The installed `workbay_orchestrator_mcp.orchestration.plan_review_driver` owns execution. Root and payload gateways use that same package API. Defaults are `codex-remote`, `gpt-6.1-sol`, `high`, `standard`; explicit `--model` and `--effort` are validated through the remote catalogue. All reviewers execute remotely; unavailable routes fail without local fallback. One invocation is one round, with no automatic rereview. Round 2 requires `--prior-adjudication <json-path>`; rounds above 2 also require `--round-justification <reason>`. Prior JSON names `task_ref`, the preceding `round`, and its `review_run_id`; its exact byte digest travels with the review. `--timeout` bounds native transport to 1..1800 seconds (default 900).

   - The gateway freezes once through NWI02's authoritative pending DTO, then runs NWI01 lint against the snapshot and immutable source tree before intent or model input. `invalid` exits 1; `unknown` exits 2. Unsupported evidence is unknown. No mutable plan path substitutes for the frozen bytes.
   - `--preflight-only` produces the pending DTO without execution. To continue it, pass its **same** `--review-run-id` and base to the normal gateway. Completed retries read the canonical row and return the same review identity without resubmission. Changed immutable parameters conflict; old unbound rows cannot become new proof.
   - The driver packages the exact base64 snapshot, digest, immutable source, round, prior adjudication and scope, builds a read-only AgentSpec, and executes existing native submit/status/collect. `COPYFILE_DISABLE=1` and `COPYINGFILE_DISABLE=1` are set for archive transport. A packet, intent, or spool is not a completed review.
   - Every real defect must be in `findings[]`, including confirmed out-of-scope defects as `follow_up` with origin and rationale. Notes are bounded non-defect observations; notes-only output is not qualified authority. Schema validation cannot prove semantic completeness. Independent qualification remains required and must check for defects hidden in prose.
   - Canonical finding batches are capped at 100 per write, retain committed event receipts, reconcile linked prior findings, and never replay a terminal upsert. Partial recording exits nonzero without completion and resumes through canonical receipts. A stable verdict event precedes the explicit digest CAS of the same pending row.
   - The machine receipt names the job/run, source/tree, subject and packet digests, round, prior digest, scope, requested route, all canonical finding IDs, verdict and CAS. `review_completed` and `plan_accepted` are distinct; this driver never accepts or publishes a plan.
   - Failure codes: 20 parameters, 21 identity conflict, 22 route/catalogue, 23 native transport/provenance, 24 result/schema, 25 incomplete recording, 26 completion CAS, 27 dashboard outcome unknown, 28 busy run, 29 subject unavailable. Native return code and partial progress remain in the receipt. Exit 0 means all recording and one dashboard refresh completed, not that the verdict was a pass.
   - Delivery artifacts and the existing native-job ledger are resumable transport state, not another review database. A dashboard attempt whose acknowledgment was lost reports unknown and requires reconciliation; retry never silently renders twice.

   Steps 2–10 below describe the checks and canonical effects owned by this driver. Inspect its receipt; do not launch another reviewer or repeat its writes in-session. Resume a failed operation with the same immutable parameters and stable run ID. Steps 11–12 retain the existing separate operator workflow.

2. Check prior planning review history with `review_runs(review={"operation": "list", "review_mode": "planning", ...})`.
3. Execute the planning-review checklist: current-state accuracy, internal consistency, architecture ownership, contract realism, interface realism, **junior-agent implementability**, naming compliance, pipeline readiness, and testability. The *Junior-Agent Implementability* subsection of `planning-review-guide.md` is mandatory: verify the plan names files **and** functions (`path:symbol`), grounds every code claim against the actual codebase (no invented APIs/fields/files), and is implementable from the text alone — spot-check cited anchors exist.
4. If the artifact is a task plan under `docs/tasks/`, verify it still conforms to `docs/workbay/templates/TASK_PLAN.template.md`, including the `## Consolidated Checklist` structure, the required supporting sections, and the *Implementation Readiness — Junior-Agent Standard*.
5. Record every finding in MCP with `review_findings`.
6. Decide the planning verdict.
7. Record the verdict decision with `record_event(event={"event_kind": "decision", ...})`.
8. Complete the **same authoritative pending review** through the freeze producer's bound completion protocol, preserving `review_run_id` and the exact reviewed digest. Do not insert a new unbound verdict or capture a digest after judging. Completion and plan acceptance must recheck the reviewed bytes at effect; if the NWI02 completion/acceptance integration is unavailable, report that dependency and stop without declaring acceptance.
9. Refresh `DASHBOARD.txt` with `render_handoff(kind='dashboard')` after the state-changing writes land.
10. Confirm whether open findings remain before declaring the artifact ready, and cite the stable handoff gap ids (`finding_id`) when reporting them.
11. **Post-`pass`**: when the verdict is exactly `pass` and zero open planning findings remain for the subject, instruct the operator to run `make plan-accept TASK=<ref>` from the root `main` checkout before `make task-start`. This lands the plan on `main` as a docs-only commit so the accepted baseline becomes durable and coordinators can read it locally. If the reviewed plan is a brand-new untracked draft on root `main`, use the receipt's explicit `LIFECYCLE_ARGS="--json --local --plan <path> --source-branch main"` form so `plan-accept` commits only that plan file. `task-start`, `review-ready`, and `handoff-close-check` report `plan_baseline_missing` until that baseline exists. `pass_with_findings`, `conditional_pass`, `fail`, and absent runs do **not** unlock acceptance.
12. **Close the on-main MAINT row** when the pass is complete: run `make plan-done TASK=<maint-ref>` from any checkout (sets `status=done` and runs the on-main MAINT GC sweep). Do not use `make task-finish` — that path assumes a feature branch/worktree teardown.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "It's only a plan, so small inconsistencies can wait for implementation." | Planning drift becomes implementation churn. The cheapest fix is before code starts. | Record and resolve the inconsistency now. |
| "The artifact mostly looks right, so I don't need code anchors." | Plans fail on stale assumptions about the current repo state. | Check the actual code or contract surface. |
| "The analysis pass already looked at this." | `plan-draft` (`/wb-draft`) is triage, not the required planning review gate. | Run the full planning review anyway. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| Planning review starts without current code anchors | Step 1: load the missing anchor. |
| Findings are about to be reported without MCP ids | Step 5: record them first. |
| Artifact is being approved while open findings still exist | Step 10: block approval until statuses are resolved. |

## Recovery

- Use the same normal gateway to resume a pending run. Direct model reasoning outside this gateway cannot claim its freeze, native execution, or completion proof. A protocol fixture is author support only; independent actual remote qualification owns production capability claims.
- If the planning packet is incomplete, record the missing dependency as a finding instead of guessing.
- If MCP is unavailable, stop and record a blocker when access returns.

## Codebase-graph capability (optional)

If a codebase-graph MCP is connected (exposes `get_architecture`, `trace_path`, `detect_changes`, or search surfaces such as `search_graph` / `search_code`), use it during step 3's junior-agent implementability pass to confirm cited `path:symbol` anchors exist before recording invented-API findings. Skip this block when the capability is absent — the skill remains valid without it.

## Convergence Criteria

- Planning findings are recorded in MCP before they are reported.
- Reported findings cite their stable handoff gap ids (`finding_id`).
- A planning-mode review run exists for the pass.
- A verdict decision exists for the artifact.
- `DASHBOARD.txt` is refreshed after the planning-review writes.
- The artifact is either cleared with zero open findings or blocked on explicit unresolved gaps.

## See Also

- [../../../docs/workbay/rules/planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md)
- [../plan-draft/SKILL.md](../plan-draft/SKILL.md)

## Contract basis

Pinned canon 0.26.2 (`meta/release-manifest.json`): OPS-76, `lexicons/planning.md:109`
(Site Reliability Engineering ch. 5), removes repeated round ceremony without
claiming measured savings. AGT-07, `lexicons/engineering.md:81` (The Programmer's
Brain ch. 11; CARD-05/P7), keeps decisions durable. GRPH-33,
`lexicons/graph-theory.md:190` (practice label), requires explicit packet/result
contracts. AGT-21, `lexicons/engineering.md:94` (The UNIX Programming Environment
ch. 5; CARD-01/P18), requires named nonzero outcomes. DATA-14 (`lexicons/engineering.md:232`, Designing Data-Intensive Applications ch. 11; CARD-02/P12) keeps
canonical byte authority. OBS-09 (`lexicons/engineering.md:615`, Thinking in Systems ch. 6; CARD-11/P15) retains independent qualification.
CON-11 (`lexicons/engineering.md:185`, Operating Systems: Three Easy Pieces ch. 32) leaves completion enforcement at the existing CAS effect.
