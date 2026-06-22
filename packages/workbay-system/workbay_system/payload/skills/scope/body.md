# Scope

## Overview

Use this skill before writing an assessment, spec, or task plan for a new feature. It enforces the question-first intake pattern so planning starts from recorded answers instead of agent guesswork.

## Trigger

Use this skill when:

- a new feature, capability, or epic is being proposed
- the request is not yet traced to concrete code
- an agent is about to draft planning artifacts from a sparse prompt

Do not use it for bug fixes, tech-debt tasks, or work already derived from an approved spec or task plan.

## Goal

Collect enough user intent to define an MVP scope, success criteria, edge cases, and explicit not-doing boundaries before any planning artifact is produced.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance](../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)

This skill owns the intake question pass and MCP decision logging for new feature scope. Downstream planning artifacts own solution design.

## Core Process

1. Determine whether the request is a true new-feature intake. If the problem is already traced to code or an approved spec, skip this skill and move to the appropriate planning or implementation flow.
2. Ask 3-5 targeted questions via `AskUserQuestion` before producing any plan. Cover scope, completion signals, edge cases, non-functional constraints, and explicit not-doing boundaries.
3. Record the resulting answers as MCP decisions with `record_event(event_kind="decision", ...)` so later planning sessions can reuse the same intake context.
4. Summarize the intake into one bounded scope note, preferably in `docs/scopes/<slug>.md` when a durable artifact is needed.
5. End with a compact framing: MVP scope, assumptions, success criteria, and Not-Doing list.
6. When intake is complete and the on-main `MAINT-*` row is no longer needed, close it with `make plan-done TASK=<maint-ref>` (not `make task-finish`).

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "The prompt is clear enough; I don't need questions." | Sparse prompts hide assumptions. The missing constraint is usually what later blows up the plan. | Ask the missing questions first. |
| "I'll ask follow-up questions only if I get stuck." | By then the draft has already anchored on assumptions the user may not share. | Front-load the questions before generating scope output. |
| "Not-doing can wait until the task plan." | Without an early boundary, optional ideas quietly become implied requirements. | Capture a Not-Doing list during intake. |

## Red Flags

| Flag | Re-entry point |
|---|---|
| Planning artifact is about to be drafted from a one-line feature ask | Step 2: ask intake questions first. |
| The request mixes MVP needs with stretch ideas | Step 2: separate must-have scope from optional work. |
| Success is described only as "ship it" or "make it work" | Step 2: ask for concrete completion signals. |

## Recovery

- If the user cannot answer everything, document assumptions explicitly instead of filling gaps silently.
- If the ask collapses into a bug fix or spec-derived task, exit this skill and use the normal planning or implementation flow.
- If MCP is unavailable, gather answers but treat durable recording as a blocker before formal planning begins.

## Convergence Criteria

- At least 3 targeted questions were asked and answered.
- The answers were recorded as MCP decisions.
- The resulting scope framing includes explicit success criteria and a Not-Doing list.
- The work is now ready for assessment/spec/task-plan drafting without hidden assumptions.

## See Also

- [../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance](../../../docs/workbay/rules/planning-review-guide.md#planning-pipeline-and-lifecycle-compliance)
- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
