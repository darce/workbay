# Spec

## Overview

Use this skill to turn reviewed intent and code-verified assessment findings into a spec that is ready for ADR and task-plan work. The skill owns requirement shape, traceability, and deferral boundaries; it does not approve implementation.

## Trigger

Use this skill when:

- drafting or revising a repository spec under `docs/specs/`
- closing planning-review findings that require spec coverage
- converting assessment findings into RCL-style requirements
- deciding whether a concern belongs in a spec, ADR, or task plan

Do not use it to approve implementation or to skip ADR/task-plan/review gates.

## Goal

Produce a reviewed-intent spec with stable requirements, explicit traceability, ADR gates where needed, deferral boundaries, validation commands, and downstream guidance for ADR or task-plan ownership.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance](../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance)
- [../../../docs/workbay/rules/planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md)
- [../../../docs/workbay/templates/SPEC.template.md](../../../docs/workbay/templates/SPEC.template.md)
- [../../../docs/workbay/templates/ADR.template.md](../../../docs/workbay/templates/ADR.template.md)
- [../../../docs/workbay/templates/TASK_PLAN.template.md](../../../docs/workbay/templates/TASK_PLAN.template.md)

This skill owns spec shape and traceability. It does not approve implementation or replace ADR/task-plan review gates.

## Core Process

1. Load the source artifact first: scope, assessment, prior spec, ADR finding, or planning-review finding.
2. Load only the code anchors needed to verify each requirement. Every major requirement should trace to current code, an assessment finding, or an explicit user decision.
3. Choose the spec path under `docs/specs/` and use existing local spec style before inventing a new format.
4. Write requirements as stable IDs with trace, priority, ADR gate, rationale, and done-when criteria.
5. Separate implementation tiers from implementation permission. Specs may say which requirements are lower-risk, but code still waits for the required ADR/task-plan/review gates.
6. Add an explicit Deferred or Rejected section for tempting alternatives that should not quietly re-enter implementation.
7. End with validation commands and downstream artifact guidance: whether an ADR is required, which task plan should own implementation, and which findings the spec closes.

## Spec Boundaries

- Put user-visible behavior, contracts, constraints, done-when criteria, and deferrals in the spec.
- Put design decisions with alternatives in an ADR when authority, ownership, event semantics, or cross-boundary data models are uncertain.
- Put slice order, branch, lane ownership, and command evidence in a task plan.
- Put observations and code-verified problems in an assessment before writing a broad new spec.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "The task plan already explains the behavior, so no spec is needed." | Task plans own execution order, not durable requirements or cross-boundary contracts. | Extract stable requirements into a spec, then let the task plan reference them. |
| "This can go straight to ADR." | ADRs decide among alternatives; they do not replace testable user-facing requirements. | Write or update the spec first unless the only missing artifact is an explicit design decision. |
| "Implementation tiers mean low-risk items can start now." | Tiers describe risk; they do not grant permission to skip planning gates. | Keep implementation blocked until ADR/task-plan/review gates are satisfied. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| A requirement has no code, assessment, finding, or user-decision trace | Step 1: load or record the missing source before drafting. |
| The spec chooses an owner for uncertain event semantics or cross-boundary data | Spec Boundaries: move that decision into an ADR gate. |
| Deferred alternatives are only implied by omission | Step 6: add a Deferred or Rejected section explicitly. |

## Recovery

- If the source artifact is missing or too vague, stop and use `scope` or an assessment before drafting the spec.
- If a requirement cannot be verified against current code, mark the trace as pending and keep the spec out of planning review.
- If ADR ownership is unclear, write the spec requirement with `ADR gate: required` and name the blocked decision.
- If MCP is unavailable, draft only provisional text and treat durable finding/decision closure as blocked.

## Convergence Criteria

- Every major requirement has a stable ID, trace, priority, ADR gate, rationale, and done-when criteria.
- Every source finding or assessment item is mapped to a requirement or explicitly deferred/rejected.
- Contract-changing requirements name owner and consumer surfaces.
- Validation commands and downstream ADR/task-plan guidance are present.
- The spec is ready for planning review, not implementation.

## Review Readiness

Before declaring a spec ready for planning review, check:

- Every finding or assessment item is either mapped to a requirement or explicitly deferred.
- Requirement priorities do not conflict with stated implementation gates.
- Contract-changing items identify the owner and consumer surfaces.
- Fixture strategy is named for regression-critical examples.
- UI requirements define navigation and empty/error states, not just labels.
- Validation commands are deterministic enough for the next agent to run.

## See Also

- [../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance](../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance)
- [../../../docs/workbay/rules/planning-review-guide.md](../../../docs/workbay/rules/planning-review-guide.md)
- [../../../docs/workbay/templates/ADR.template.md](../../../docs/workbay/templates/ADR.template.md)
- [../../../docs/workbay/templates/TASK_PLAN.template.md](../../../docs/workbay/templates/TASK_PLAN.template.md)
