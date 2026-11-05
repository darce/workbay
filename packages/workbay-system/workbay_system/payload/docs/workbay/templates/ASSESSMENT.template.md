# Assessment Template

> **Metadata** — fill in when creating a new assessment:
>
> - **Date**: [YYYY-MM-DD]
> - **Author**: {{MODEL_IDENTITY}}
> - **Scope**: [package or surface being assessed, e.g. `packages/mcp-workbay-handoff`]
> - **Status**: [Draft / Reviewed / Superseded]
>
> **Purpose:** Assessments surface problems and verify them against code.
> They are the entry point for the spec definition pipeline.
> An assessment inventories what is wrong, traces findings to code, and recommends
> directions — without prescribing solutions or committing to implementation.
>
> Use this template for reports, investigations, and audits that precede spec work.
> Name the artifact by scope: `*-report.md`, `*-investigation.md`, or `*-audit.md`.
>
> **Pipeline position:** **Assessment** → Spec → [ADR] → Task Plan → Implementation.
>
> **Exit gate:** Every finding must cite `file:line` in current code. Recommendations
> are prioritized. Deferred items are named explicitly. Owner review resolves
> disagreements before a spec is started.

---

# [Scope] [Topic] Report

> [One-paragraph summary: what the assessment found and why it matters.
> A reader should know after this paragraph whether the report is relevant
> to their concern.]

**Related docs:**
- [links to contracts, prior reports, or upstream docs that motivated this assessment]

## Executive Summary

[2-4 paragraphs. State the core problem, the main contributing causes, and the
recommended direction. Do not propose detailed solutions — that is spec work.
End with a clear statement of what should happen next.]

## Findings

> Each finding gets a stable ID (F1, F2, ...) for traceability into specs.
> Findings must cite concrete file paths and line numbers from the current codebase.
> A finding without a code reference is an opinion, not an assessment.

### F1. [Finding title]

[Description of the problem. What is happening, where in the code, and why it matters.]

Current examples:

- `file_path:line` — [what the code does]
- `file_path:line` — [what the code does]

**Impact:** [How this affects consumers, agents, performance, or correctness.]

### F2. [Finding title]

[Same structure as F1.]

## Recommendations

> Recommendations are directions, not specs. They describe what should change
> and why, but leave the exact implementation to the spec stage.
> Each recommendation should trace to one or more findings.

### 1. [Recommendation title]

**Traces:** F1, F2
**Priority:** P0 / P1 / P2 / P3

[What should change and why. Include enough detail for a spec author to
understand the intent without re-reading the entire assessment.]

### 2. [Recommendation title]

[Same structure.]

## Code-Verified Critique

> Added after the initial assessment is drafted and the code has been
> re-examined with a skeptical lens. Catches overstatements, missing
> context, and gaps before the assessment feeds into spec work.

### What the assessment gets right

[Which findings are strongly supported by code evidence.]

### Where the assessment overstates the problem

[Which findings are partially true but need qualification, with code evidence
for the narrower claim.]

### Recommendations the assessment is missing

[Problems or directions that emerged during the critique but were not in the
original findings. Use R-MISS-N identifiers for traceability into specs.]

## Priority Ordering

| Priority | Change | Impact | Effort | Trace |
|----------|--------|--------|--------|-------|
| **P0** | [change] | [impact] | [effort estimate] | F1 |
| **P1** | [change] | [impact] | [effort estimate] | F2 |

## Deferred or Rejected Directions

- [Direction not recommended yet, with reason]

## Suggested Spec Direction

[Brief guidance for whoever writes the spec from this assessment.
What should be in scope, what should be deferred, where the spec
should live (package-local vs monorepo), and what structure it should follow.]

## Next Step

- [ ] Write a spec (path: `[target spec path]`) — flag ADR-gated items within the spec for design-uncertain findings
- [ ] No spec needed; handle as direct implementation or doc sync

## References

- [path/to/adjacent-doc.md]
