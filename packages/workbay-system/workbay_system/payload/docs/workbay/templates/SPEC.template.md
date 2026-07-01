# Specification Template

> **Metadata** — fill in when creating a new specification:
>
> - **Date**: [YYYY-MM-DD]
> - **Author**: {{MODEL_IDENTITY}}
> - **Status**: [Draft / Reviewed / Approved]
> - **Assessment**: [path to the assessment report, or `n/a` if none]
> - **Package version target**: [target version, or `n/a`]
>
> **Purpose:** Specs define concrete, testable changes derived from an assessment
> or a direct problem statement. Each spec item has a stable identifier,
> traceability to findings, before/after code, and a done-when definition
> that is machine-verifiable.
>
> Specs are binding design documents. They sit between assessment (problem surfacing)
> and task plans (implementation slices). A spec defines *what* changes and *how to verify it*.
> A task plan defines *how to build it* and *in what order*.
>
> **Pipeline position:** Assessment → **Spec** → [ADR] → Task Plan → Implementation.
>
> **Exit gate:** At least one planning review pass with findings recorded in MCP.
> All findings resolved. Validation snippets verified against the current package.
> No implementation tasks may be created until this gate is passed.

---

# [Topic] Specification

> [One-paragraph motivation: what problem this spec solves and why it matters now.]

**Constraints:** [Key constraints that bound the design space, e.g. "Greenfield.
Breaking changes are free." or "Must preserve backward compatibility with v1 consumers."]

---

## Spec Items

> Each item gets a stable identifier (e.g. OC-001) for traceability into task plans
> and review findings. Items include Trace (back to assessment finding), Priority,
> and Done When (testable, not subjective).
>
> For items that change code: include Before/After with stable function or target
> names as anchors (`file_path::function_name`). Line numbers may be added as
> temporary verification notes but are not durable — they shift with unrelated edits.
> Do not guess at code shapes; verify against the live codebase.

### [PREFIX]-001: [Item title]

**Trace:** [Assessment finding ID or direct problem description]
**Priority:** P0 / P1 / P2

[Description of the change. What is wrong now, what should change, and why.]

**Before** (`file_path::function_name`):
```python
# current code — verified against live codebase
```

**After:**
```python
# target code after the change
```

**Done when:**
- [Testable criterion 1]
- [Testable criterion 2]

---

### [PREFIX]-002: [Item title]

[Same structure as above.]

---

## Entity / Payload Schemas

> Include when the spec defines structured payloads, request/response shapes,
> or entity models. Omit when the spec is purely behavioral.

### [Entity name]
```json
{
  "field": "type (required/optional)",
  "another_field": "type | null"
}
```

---

## Implementation Tiers

> Group spec items by readiness and dependency order.
> Tier 1: high-certainty, code-verified, independent — can become task plans immediately.
> Tier 2: mechanical but depends on Tier 1 landing first.
> Tier 3: blocked on a design decision (ADR) — create a design task, not an implementation task.

### Tier 1 — Ready to implement

Task plan: `[path or "to be created"]`

```
[PREFIX]-001  [short description]     [effort/scope note]
[PREFIX]-002  [short description]     [effort/scope note]
```

[Dependency notes: which items can be parallelized, which must be sequential.]

### Tier 2 — Ready after Tier 1

Task plan: `[path or "to be created"]`

```
[PREFIX]-003  [short description]     [depends-on note]
```

[Why this tier depends on Tier 1.]

### Tier 3 — Blocked on ADR

Design task: `[path or "to be created"]`
ADR: `[path or "to be created"]`

```
[PREFIX]-004  [short description]     [blocked-on note]
```

[What the ADR must resolve. Do not create implementation tasks for this tier
until the ADR is reviewed and the spec is updated with the ADR-backed direction.]

---

## Spec-Review Gate

No implementation tasks may be created from this spec until:

1. The spec has been reviewed with findings recorded in MCP
2. All review findings are resolved (fixed, deferred with rationale, or wontfix)
3. Validation snippets have been verified against the current package (not guessed)

For Tier 3 / architectural items, the review gate applies to the ADR as well —
the ADR must be reviewed before implementation tasks are created from it.

---

## Validation

> Split validation by tier. Each snippet runs against the package state after
> its tier is implemented. Verify snippets against the current package before
> marking the spec as reviewed — do not guess at import paths, function names,
> or response shapes.

### Tier 1 validation

```bash
# --- [PREFIX]-001: [short description] ---
[runnable validation command verified against current codebase]
```

### Tier 2 validation

```bash
# --- [PREFIX]-003: [short description] ---
[runnable validation command]
```
