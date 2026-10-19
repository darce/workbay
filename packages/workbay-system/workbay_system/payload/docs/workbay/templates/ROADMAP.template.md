# Roadmap Template

> **Metadata** — fill in when creating a new doc from this template:
>
> - **Date**: [YYYY-MM-DD HH:MM EST]
> - **Author**: {{MODEL_IDENTITY}}
>
> Use this template for multi-phase architectural roadmaps under `docs/roadmaps/`.
> Roadmaps describe **what** and **why** across multiple delivery phases.
> Individual phases spawn task plans (`TASK_PLAN.template.md`) for **how**.
>
> Key differences from task plans:
>
> - No "Functions to Change" or code patterns (too granular for a roadmap)
> - Phases have exit criteria, not just checklists
> - External dependencies and parallel workstreams are first-class
> - Success metrics are measurable outcomes, not implementation checkboxes

---

# [ROADMAP_TITLE] (v[VERSION])

## Objective

[What capability the system gains when this roadmap is complete. 2-3 sentences max.]

## Problem Statement

[What user-visible behavior is broken or missing. Why the current architecture fails.]

## Constraints

- [Architectural or policy constraint that shapes all phases]
- [External dependency constraint]
- [Technology or timeline constraint]

## Terminology

- **[Term]**: [Definition as used in this roadmap]

## Current State

[What works, what's broken, what's missing. Bullet points.]

- [Component] does X but should do Y.
- [Capability] does not exist yet.

## Target Architecture

[Narrative description of the end-state architecture. Include data model, integration pattern, and key design decisions.]

### Design Decisions

| Decision                         | Rationale                                  |
| -------------------------------- | ------------------------------------------ |
| [e.g., No taxonomy for clusters] | [Why this was chosen over the alternative] |

### Data Model

[Table schemas, entity relationships, or data flow description.]

## Phased Delivery

### Phase 1: [Title]

**Goal**: [One sentence.]

Deliverables:

- [Deliverable]
- [Deliverable]

Exit criteria:

- [Observable outcome that proves the phase is done]

### Phase 2: [Title]

**Goal**: [One sentence.]

Deliverables:

- [Deliverable]

Exit criteria:

- [Observable outcome]

## External Dependencies

| Dependency                | Owner                | Status                             | Blocks                  |
| ------------------------- | -------------------- | ---------------------------------- | ----------------------- |
| [e.g., Snapshot endpoint] | [e.g., Backend team] | [Not started / In progress / Done] | [Phase N exit criteria] |

## Code Anchors

| Layer    | File               | Note                            |
| -------- | ------------------ | ------------------------------- |
| Plugin   | `path/to/file.php` | [Current role and what changes] |
| Backend  | `path/to/file.py`  | [Current role]                  |
| Frontend | `path/to/file.ts`  | [Current role]                  |

## Risks and Mitigations

- **Risk**: [What could go wrong]
  Mitigation: [How to handle it]

## Success Metrics

- [Measurable outcome, e.g., "Cluster UI renders from local store during backend outage"]
- [Performance target, e.g., "Cluster list latency < Xms from local DB"]

---

# Consolidated Checklist

## Phase 1: [Title]

- [ ] [Deliverable or sub-task]
- [ ] [Deliverable or sub-task]

## Phase 2: [Title]

- [ ] [Deliverable or sub-task]

## Deferred (Post-[VERSION])

- [ ] [Explicitly deferred work with rationale]

## Success Criteria

- [ ] [Observable outcome that proves the roadmap is complete]
