# Epic Template

> **Metadata** — fill in when creating a new doc from this template:
>
> - **Date**: [YYYY-MM-DD HH:MM EST]
> - **Author**: {{MODEL_IDENTITY}}
> - **Epic Short ID**: [2-5 uppercase letters; this becomes the task-plan prefix source]
>
> Use this template for multi-phase epics under `docs/epics/`.
> Epics define a bounded capability or process change delivered across multiple task plans.
> Epics describe the end-state architecture, evidence model, and delivery phases.
> Task plans describe executable implementation slices and should use `TASK_PLAN.template.md`.
>
> Key differences from task plans:
>
> - Epics define destination and sequencing, not file-by-file implementation steps.
> - Epics use **phases** for coarse-grained temporal progression across multiple task plans.
> - Epics should identify durable process/data model changes, evidence gates, and cross-team dependencies.
> - Epics should not duplicate live review findings from MCP handoff; they should describe policy, scope, and delivery shape.

---

# E[GLOBAL_EPIC_INDEX]. [EPIC_TITLE] (v[VERSION])

> **Epic Short ID**: [SHORT_ID]

## Objective

[What capability, workflow, or product surface the system gains when this epic is complete. 2-3 sentences max.]

## Problem Statement

[What is broken, missing, or drifting today. Why the current architecture or workflow fails.]

## UX Vision

[What the intended user or operator experience looks like when this epic is complete.]

## Constraints

- [Architectural, policy, or repo-level constraint]
- [Dependency, staffing, or runtime constraint]
- [Technology, safety, or timeline constraint]

## Terminology

- **[Term]**: [Definition as used in this epic]

## Current State

- [Current behavior that works but is insufficient]
- [Broken or missing behavior]
- [Known drift, risk, or process gap]

## Applied Concepts from Sources

> Optional but recommended when the epic is informed by literature, prior audits, or external process references.
> Prefer concrete source-to-application mappings over broad summaries.

| Source             | Concept           | Epic application                                |
| ------------------ | ----------------- | ----------------------------------------------- |
| `[path/to/source]` | [Idea or pattern] | [How it changes this epic's design or delivery] |

## Git Workflow Assessment

> Optional when the epic changes workflow, orchestration, or review behavior.
> Use this section to clarify what should be borrowed, adapted, or explicitly rejected from external workflows.

[Short assessment of how git/worktree/review flow should support this epic.]

## Target Architecture

[Narrative description of the end-state architecture or process. Explain how context, contracts, runtime behavior, evidence, and review fit together.]

### Design Decisions

| Decision   | Rationale                             |
| ---------- | ------------------------------------- |
| [Decision] | [Why it was chosen over alternatives] |

### Data Model

[Describe the important information flow. For process epics, this can be a process/evidence model instead of product schema.]

- [Canonical source of truth]
- [Evidence or artifact flow]
- [State model or cross-boundary data flow]

## Phased Delivery

### Phase N: [Title] -- [STATUS]

> **Status**: completed | in-progress | not-started
> **Task plans**: [link to task plan(s)] or `not yet scoped`

**Goal**: [One sentence.]

Deliverables:

- [Deliverable]
- [Deliverable]

Exit criteria:

- [Observable outcome that proves the phase is complete]
- [Another observable outcome]

## External Dependencies

| Dependency   | Owner   | Status                             | Blocks                           |
| ------------ | ------- | ---------------------------------- | -------------------------------- |
| [Dependency] | [Owner] | [Not started / In progress / Done] | [Phase or exit criteria blocked] |

## Code Anchors

| Layer   | File           | Note                                  |
| ------- | -------------- | ------------------------------------- |
| [Layer] | `path/to/file` | [Why this anchor matters to the epic] |

---

# Consolidated Checklist

## Phase N: [Title] -- [STATUS]

- [ ] [Deliverable or sub-task]

## Deferred (Post-[VERSION])

- [ ] [Explicitly deferred work with rationale]
