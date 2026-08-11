# ADR Template

> **Metadata** — fill in when creating a new ADR under `docs/adrs/`:
>
> - **Date**: [YYYY-MM-DD]
> - **Author**: {{MODEL_IDENTITY}}
> - **Status**: [Proposed / Accepted / Superseded by ADR-NNN / Rejected]
>
> **Purpose:** ADRs record design decisions with rationale, rejected alternatives,
> and guardrails for follow-on implementation. They resolve design uncertainty
> that a spec explicitly marks as blocked.
>
> ADRs are decision artifacts, not task plans. The ADR is reviewed first;
> any implementation task plan is then derived from the approved ADR.
> If the ADR itself needs structured planning (inventory, evaluation slices),
> that planning lives in the spec or assessment — not in a wrapper task plan.
>
> **Pipeline position:** Assessment → Spec → **ADR** → Task Plan → Implementation.
> ADRs are created only when a spec item is explicitly design-uncertain.
> The ADR resolves the design question; the task plan that follows implements it.
>
> **Exit gate:** The ADR must be reviewed (planning review with findings recorded
> in MCP) before an implementation task is created from it.
>
> **Naming:** `ADR-NNN-[kebab-case-topic].md`. Number sequentially.
>
> **Minimum required sections:** Status, Date, Context, Decision, Consequences.
> All other sections are recommended when they improve review quality.

---

# ADR-NNN: [Decision Title]

## Status

[Proposed / Accepted / Superseded by ADR-NNN / Rejected]

## Date

[YYYY-MM-DD]

## Context

[What problem or design question prompted this ADR. Reference the spec item
that is blocked on this decision, and the assessment findings that motivated it.
Include enough background that a reader unfamiliar with the spec can understand
the decision space.]

### Constraints from prior review

> Hard constraints established during spec review or assessment that the chosen
> approach must satisfy. These are non-negotiable.

- [Constraint 1, e.g. "typed MCP schemas per entity family — no opaque payload: dict"]
- [Constraint 2]

## Current State Inventory

> Ground the decision in the live codebase, not memory. Enumerate the surfaces
> that will be affected. This prevents the decision from being made against an
> imagined state.

[Inventory of current tools, schemas, surfaces, or patterns relevant to the decision.
Cite file paths and test assertions.]

### Downstream surfaces that must migrate together

[Every doc, test, adapter, or runtime surface that assumes the current state.
This scoping is why the decision must be taken at ADR level.]

- `file_path` — [what it enumerates or assumes]

## Decision

[State the chosen approach clearly. Lead with the decision, then the design rules.]

### Chosen design rules

1. **[Rule title.]** [What this rule means and which surfaces it affects.]
2. **[Rule title.]** [Same structure.]

### Target outcome

[Quantitative or qualitative target: tool count range, token budget, schema property,
or behavioral guarantee.]

## Why This Decision

> Explain why this approach was chosen over the alternatives.
> Each subsection names a concrete benefit the alternatives lack.

### [Benefit 1]

[Why this matters, referencing constraints or current state.]

### [Benefit 2]

[Same structure.]

## Alternatives Considered

> Each alternative gets a heading, a rejection verdict, and a concrete reason.
> "Rejected" alone is not enough — explain what would go wrong.

### 1. [Alternative title]

Rejected.

[Why this approach was not chosen. What it would cost or break.]

### 2. [Alternative title]

Rejected.

[Same structure.]

## Consequences

### Positive

- [Benefit that follows from the decision]

### Negative

- [Cost or risk that follows from the decision]

### Guardrails for the follow-on implementation task

> Hard rules the implementation must follow. Prevent drift toward rejected alternatives.

- [Guardrail 1]
- [Guardrail 2]

## References

- Spec: [link to the spec this ADR unblocks]
- Assessment: [link to the assessment report, if applicable]
- Implementation task plan: [link to the task plan derived from this ADR, once created]
- Related: [links to related ADRs, task plans, or prior art]
