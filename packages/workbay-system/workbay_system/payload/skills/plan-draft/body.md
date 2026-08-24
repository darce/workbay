# Plan Draft

## Overview

Use this skill as a pre-review planning triage step. It runs a focused analysis pass on one planning artifact and records planning-mode findings in MCP. It does **not** implement a mechanical precheck gate for `make plan-review` — that target is a pure skill-broadcaster. Do **not** record under a `plan-analyze-*` session prefix: that prefix was retired because it invited a phantom `make plan-review` precheck. Distinguish triage from formal review by `review_mode="planning"` and the run rationale, not by a reserved session prefix.

## Trigger

Use this skill when:

- running `/wb-draft` or `make plan-analyze DOC=<path>`
- triaging a task plan or epic before formal planning review
- checking a planning artifact for ambiguity, duplication, coverage gaps, or terminology drift

Do not use it as a substitute for `planning-review`, and do not use it for branch diffs.

## Goal

Surface likely planning problems early, record them as planning findings, and hand the artifact off to `planning-review` only after the cheap gaps are understood.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md) (canonical policy — single source of truth in this checkout)
- [../../../docs/workbay/rules/planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md) (MCP-unavailable fallback and shared workflow policy)
- [docs/workbay/templates/TASK_PLAN.template.md](docs/workbay/templates/TASK_PLAN.template.md) for task plans under `docs/tasks/`
- Use [heuristics canon](https://github.com/darce/heuristics-canon) for the reasoning posture underneath this skill (diagnose-before-execute, position-with-counter-case, escalation tiers). Codebase-graph optional tools follow the shared optional-capability pattern used by sibling review skills — confirm anchors with the tools that can resolve `path:symbol` (see Core Process step 3 and the capability block below).

This skill owns triage only. It records planning-mode findings and a `review_runs(review={"operation": "record", "review_mode": "planning", ...})` entry so the triage pass is durable in MCP. It does not replace the formal planning-review run, and no make target prechecks a reserved session prefix.

## Core Process

0. Before any MCP write, confirm whether the target implementation task already has a linked worktree. If its `target_branch` has no worktree yet, do not record plan-draft output against that implementation row: write-side attribution can fail with `WorktreeNotFoundError`. Use a `target_branch=main` `MAINT-*` row for ad-hoc planning work, run `make task-start` once the accepted baseline exists, or wait until the feature worktree exists.
1. Load the planning artifact, the constitution, and only the minimum adjacent code or contract anchors needed to test the artifact's claims.
2. If the artifact is a task plan under `docs/tasks/`, validate its structure against `docs/workbay/templates/TASK_PLAN.template.md`, including the `## Consolidated Checklist` section and its supporting `Context and Ownership`, per-slice checklist, `Review Readiness`, and `Success Criteria` blocks, plus the *Implementation Readiness — Junior-Agent Standard* requirements (files **and** functions named, grounded anchors, self-contained checklist).
3. Run eight analysis passes: duplication, ambiguity, underspecification, constitution alignment, coverage gaps, terminology drift, **implementation-grounding** — spot-check that the plan's cited `path:symbol` anchors actually exist in the codebase (flag invented/assumed APIs, files, or fields), that change sites name functions not just files, and that the plan is implementable by a junior agent from the text alone — and **design-quality & failure-mode grounding** — spot-check the plan against [planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md) design/failure-mode sections (coupling-type triage, ports&adapters, steady-state reclaimer, consistency model, unbounded-result, complexity budget) plus [heuristics canon](https://github.com/darce/heuristics-canon) rule IDs; record gaps as findings `review_mode="planning"`. Cue+link only — do not restate the checklist rows. **Anchor verification (AGT-02):** use `search_graph` / `search_code` (or direct file reads / `get_code_snippet`) to confirm each cited `path:symbol` exists before flagging it as invented; architecture-only tools (`get_architecture` / `trace_path` / `detect_changes`) do not substitute for symbol lookup.
4. Turn concrete problems into MCP findings with `review_findings(..., review_mode="planning")`.
5. Record a planning-mode review run for this triage pass. Do **not** use a `plan-analyze-*` session prefix.
6. Summarize whether the artifact should proceed directly to `planning-review` or be revised first.
7. When triage is complete and the on-main `MAINT-*` row is no longer needed, close it with `make plan-done TASK=<maint-ref>` (not `make task-finish`).

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "Analysis already found issues, so the formal review can be skipped." | Analysis is triage, not the planning gate. It does not produce the required formal planning-review run. | Still run `planning-review`. |
| "I can just leave the issues in chat because this is only advisory." | Advisory findings still need durable ids so planners can fix or defer them. | Record them in MCP with `review_mode="planning"`. |
| "The document is short, so detailed passes are unnecessary." | Short plans can still hide stale assumptions or missing rollout details. | Run every analysis pass anyway. |
| "I recorded a reserved triage session prefix, so `make plan-review` is gated / safe to skip." | No consumer implements a plan-draft session precheck; reserved prefixes were retired for that reason. | Run formal `planning-review` when the artifact needs the planning gate. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| Analysis is about to approve a document without touching the constitution or code anchors | Step 1: load the missing anchor. |
| Findings are being recorded without `review_mode="planning"` | Step 3: correct the write mode before continuing. |
| The review run session starts with `plan-analyze-` | Step 5: drop the retired prefix; record the triage run without it. |
| Cited `path:symbol` anchors were not looked up with search tools | Step 3: confirm each anchor via `search_graph` / `search_code` (or file read) before inventing-API findings. |

## Recovery

- If the artifact is too broad, narrow the pass to the next planning slice instead of loading half the repo.
- If adjacent contracts are missing, record that gap as a finding.
- If MCP is unavailable, follow the shared MCP-unavailable policy in [development-workflow.md](../../../docs/workbay/rules/development-workflow.md); for this skill, durable finding recording is a **blocker** before recommending implementation (do not draft a provisional "approved" path that assumes findings landed).

## Codebase-graph capability (optional)

If a codebase-graph MCP is connected, use it during the implementation-grounding pass. Prefer tools that resolve symbols: `search_graph`, `search_code` (and `get_code_snippet` when available) to confirm each cited `path:symbol` exists before recording invented-API findings. Architecture helpers (`get_architecture`, `trace_path`, `detect_changes`) may complement blast-radius judgment; they do not replace symbol verification. Skip this block when the capability is absent — the skill remains valid without it (degrade: use direct file reads).

## Convergence Criteria

- Analysis findings are recorded in MCP with `review_mode="planning"`.
- A planning-mode review run exists for the triage pass and does not use a `plan-analyze-*` session prefix.
- The recommendation clearly says either "revise first" or "proceed to planning-review."

## See Also

- [../planning-review/SKILL.md](../planning-review/SKILL.md)
- [../../../docs/workbay/rules/planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md)
