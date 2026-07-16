# Task Plan Template

> **Metadata** — fill in when creating a new doc from this template:
>
> - **Date**: [YYYY-MM-DD HH:MM EST]
> - **Author**: {{MODEL_IDENTITY}}
> - **Owning Epic**: [path/to/epic.md] _(use for epic-owned task plans)_
> - **Epic Short ID**: [must match the owning epic's declared `Epic Short ID`] _(use for epic-owned task plans)_
> - **Project**: [package/project name] _(use for package-local or standalone task plans)_
> - **Task ID**: [documented task id such as `internal`] _(use for package-local or standalone task plans)_
> - **Task Plan Status**: `proposed` _(set to `active`, `closed`, or `archived` as the plan's durable state changes)_
> - **Target Branch**: `feature/<task-ref-lowercase>` _(must match the branch `make task-start` derives: literally `feature/<task-ref>` with the ref lowercased and no descriptive suffix. The lifecycle handler always derives this short form (`packages/workbay-system/scripts/workbay/lifecycle/handlers/task_start.py:505`), so any longer name drifts from the actual branch. Historical plans with long-form names are not the convention.)_
> - **Review Coverage Target**: 2 _(optional — min number of review passes before this plan is considered implementation-ready; omit for spike/research tasks)_
>
> **Review coverage note:** Do not embed mutable review counts, finding totals, or run histories in this file.
> Those are volatile; their canonical home is the handoff DB.
> Use `get_review_coverage(task_ref=...)` or `list_review_runs(task_ref=...)` to query live coverage state.
> The `Review Coverage Target` field above carries only your intent (minimum passes); actual coverage is always DB-generated.
>
> **Task plan status note:** missing `Task Plan Status` is acceptable only for grandfathered historical closed plans. New plans and any active/proposed plan should declare it explicitly so docs-hygiene checks can distinguish live drift from archival debt.
>
> **No revision-history blocks.** Task plans under `docs/tasks/**` (and `packages/*/docs/tasks/**`) must not carry a `Revision history:` section, an `## Revision history` heading, or any equivalent execution diary. Mutable execution history — status changes, slice-close rationale, review-pass results, run history, and similar prose — lives in the handoff DB. Record it through `set_handoff_state`, `record_event`, `close_slice`, `review_findings`, and `render_handoff`. Stable planning prose (objective, constraints, current-state analysis, slices, verification intent) stays in the markdown plan; mutable history does not. Legacy numbered plans under `docs/plans/**` are historical artifacts and may still contain revision-history sections; that does not make the pattern acceptable for new task plans.
>
> Use this template for all implementation plans under `docs/tasks/`.
> Task plans describe executable work for one bounded objective.
> Task plans use **slices**, not phases:
>
> - **Phases** belong to epics and describe coarse-grained temporal delivery across multiple task plans.
> - **Slices** are reviewable implementation increments that can be completed, verified, and logged independently.
>
> Favor slices that each produce behavior plus proof. Avoid scaffold-only slices that add placeholders, skipped tests, or empty abstractions without executable value.
> See `docs/workbay/instructions.md` and `docs/workbay/rules/planning-review-guide.md` for repo-wide planning rules.
> Use one identity model consistently:
>
> - **Epic-owned task plan**: title and references derive from the owning epic's declared `Epic Short ID`
> - **Package-local / standalone task plan**: title and references use the declared `Task ID`
>
> Do not invent ad hoc prefixes or copy example ids from unrelated epics or packages.

> **Implementation Readiness — Junior-Agent Standard (mandatory).**
> Author every plan so a *junior agent with no prior context* can implement it without re-discovering the codebase or guessing. That means:
>
> - **Grounded, not assumed.** Every code claim cites a real anchor as `path:symbol` (function/class/constant name), verified against the current codebase — never invented APIs, fields, or files. State the commit/date the anchors were verified at. If something must be *created*, mark it explicitly as new.
> - **Files _and_ functions to touch.** The `## Files and Surfaces to Change` table names the exact symbol(s) in each file, not just the path. Prefer symbol names over line numbers (lines drift); include lines only as a hint.
> - **Reuse named, not reinvented.** Point at the existing helper/abstraction to reuse (`path:symbol`) so the implementer extends rather than duplicates.
> - **Rationale travels with the decision.** Each non-obvious choice carries a one-line *why*, tied to evidence in the codebase or a finding/decision id — not preference.
> - **Self-contained, consolidated task list.** The `## Consolidated Checklist` is executable end-to-end from the plan alone: every slice's steps, the files/functions each touches, and the proof that closes it. No "see chat" / "obvious" gaps.
> - **Proof is pre-defined.** Each slice names the exact command/fixture/observable that makes it honestly complete (see `## Verification Strategy`).
>
> A plan that cannot be handed to a junior agent and implemented from the text alone is not implementation-ready. `plan-analyze` and `planning-review` enforce this standard.

---

## [WORK_REF]. [TASK_TITLE]

## Objective

[What changes when this task is complete. 2-3 sentences max.]

## Intake (optional — new features and epics only)

> Include this section when a P0 scope intake pass was conducted before assessment. Omit for bug fixes, tech debt tasks, and spec-derived tasks.

- **Scope one-pager**: `docs/scopes/[slug].md` _(link if created)_
- **Key Q&A decisions**: [MCP decision IDs where answers were recorded, e.g. `decision #NNN`]
- **Not-Doing**: [Explicit out-of-scope items confirmed during intake]

## Problem Statement

[What behavior, contract, or workflow needs to change, and why the current state is insufficient.]

## Constraints

- [Architectural, policy, or runtime constraint]
- [Cross-boundary or ownership constraint]
- [Testing, rollout, or safety constraint]

## Workflow Principles

- [Behavioral or policy rule that guides implementation decisions]
- [Another principle, such as single contract owner or no compatibility shim in greenfield paths]

## Terminology

- **[Term]**: [Definition as used in this task]

## Current State Analysis

- [What currently works]
- [What is broken or drifting]
- [What assumptions/tests/docs are currently misleading]

## Target Outcome

[Narrative description of the intended behavior and the preferred end-state design.]

## Context Loading

> List the minimum authoritative context an agent should load before implementation.
> Prefer small, role-specific surfaces over broad repo ingestion.

- Rules: `[path/to/rule.md]`
- Contracts: `[path/to/contract.md]`
- Handoff/MCP state: [task ref, findings, or decision surfaces to inspect]
- External docs via `ctx7` only if: [exact dependency/runtime reason]

## Contract and Boundary Impact

> Required for any task that touches a cross-service, cross-language, or tool/client boundary.
> Omit only when the task is strictly local and cannot affect a boundary contract.

| Boundary          | Owner                              | Current Contract        | Expected Change    | Compatibility Needed? | Verification          |
| ----------------- | ---------------------------------- | ----------------------- | ------------------ | --------------------- | --------------------- |
| `[boundary-name]` | [backend / proxy / frontend / MCP] | `[path/to/contract.md]` | [Change or `none`] | [yes/no + why]        | [fixture/schema/test] |

> **Non-functional author prompts** (when boundary touches replica/cache/external store):
> - State the **consistency model** and acceptable staleness for cached/replicated reads — see planning-review-guide § Non-Functional & Failure-Mode Realism.
> - If schema/wire format changes: confirm **backward + forward compat** during rollout and expand→migrate→contract path.
> - If integrating across services: confirm no **integration DB** access and ports-and-adapters at external seams.

## Proposed Solution

[Concise description of the approach. Keep this high-level. Put implementation sequencing in slices below.]

## Files and Surfaces to Change

> Name the exact symbol(s) per file — the implementer should not have to grep for the edit site. Mark new symbols `(new)`. Prefer symbol names over line numbers.

| Surface                               | File           | Symbol / Function                  | Change            |
| ------------------------------------- | -------------- | ---------------------------------- | ----------------- |
| [backend/frontend/docs/tests/tooling] | `path/to/file` | `function_or_class` / `CONST` / `(new)` | [Specific change] |

## Related Files

| File           | Note                                         |
| -------------- | -------------------------------------------- |
| `path/to/file` | [Relevant context or likely adjacent impact] |

## Verification Strategy

> Define the evidence bundle before implementation.
> Include deterministic tests first; add runtime-parity or manual verification when they are genuinely required.

- Deterministic tests:
  - `[command]`
- Runtime-parity / environment checks:
  - `[command or workflow]`
- Contract/fixture verification:
  - `[command or assertion]`
- **Failure-mode / SLA prompts** (when user-facing or dependency-bound):
  - State p99 latency target (not average), degraded behavior when dependency X is down/slow, and spike/peak load assumptions — see planning-review-guide § Non-Functional & Failure-Mode Realism.
- Manual verification:
  - `[UI path or operator action]`

## Slice Delivery

> Each slice must name the **files and functions** it touches (`path:symbol`) and the **proof** that closes it — enough for a junior agent to implement the slice from this block alone.

### implementation note: [Title]

**Goal**: [One sentence.]

Files/functions:

- `path/to/file:symbol` — [what changes here] (mark `(new)` if created)

Changes:

- [Behavioral change]
- [Docs/contract/test change in same slice]

Proof:

- [Command, fixture, or observable outcome]

### implementation note: [Title]

**Goal**: [One sentence.]

Changes:

- [Behavioral change]
- [Docs/contract/test change in same slice]

Proof:

- [Command, fixture, or observable outcome]

## Lane Decomposition (Multi-Agent)

> Include this section only when the task naturally splits into independent lanes.
> Omit for single-lane work that one agent can complete in a bounded session.

### Lanes

| Lane ID   | Owned Paths | Upstream Dependencies     | Required Tests |
| --------- | ----------- | ------------------------- | -------------- |
| `lane-id` | `path/**`   | [None or lane dependency] | `[command]`    |

### Merge Order

[List lanes in dependency order.]

### Manifest

```bash
make lane-manifest-init TASK=<task-ref> LANE_IDS='<lane-a lane-b>' TASK_PLAN=docs/tasks/<version>/<this-file>.md
```

### Orchestration Mode

- **Codex subagent (preferred when available)**: Use MCP worker lifecycle tools with the declared lane ownership and verification boundaries.
- **Shell fallback**: Use repo lane helpers or manual worktrees while preserving the same ownership and evidence requirements.

---

## Consolidated Checklist

> **Checklist scope rule:** Describe work being delivered, not finding status. Do not add rows like `(BR-04 closed)`, `(fixed in MCP)`, or "resolve DEMO-7-BR-02"; finding status is queried from the handoff DB via `review_findings(review={"operation":"list","status":"open","task_ref":"<task>"})` or read from `DASHBOARD.txt`. See [`branch-review-guide.md` § Review Findings Placement](../rules/branch-review-guide.md#review-findings-placement-mandatory).

## Context and Ownership

- [ ] Loaded the minimum authoritative rules, contracts, and handoff state before editing.
- [ ] Confirmed whether external dependency context requires `ctx7`.
- [ ] Recorded boundary ownership and compatibility expectations if any contract is touched.

### Checklist for implementation note: [Title]

- [ ] [Implementation step]
- [ ] [Contract/docs/tests updated in same slice]
- [ ] [Verification evidence captured]

### Checklist for implementation note: [Title]

- [ ] [Implementation step]
- [ ] [Contract/docs/tests updated in same slice]
- [ ] [Verification evidence captured]

## Review Readiness

- [ ] No boundary-touching implementation is left without matching contract/doc/fixture evidence.
- [ ] Runtime-parity checks are included where tests can mask real behavior.
- [ ] Handoff decision records the change, verification, and any contract implications.

## Stretch Goals

- [ ] [Nice-to-have that will not block completion]

## Success Criteria

- [ ] [Observable outcome that proves the task is done]
- [ ] [Another observable outcome]
