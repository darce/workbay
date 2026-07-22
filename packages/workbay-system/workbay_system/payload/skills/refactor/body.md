# Refactor

## Overview

Use this skill to produce a structured refactoring evaluation of a codebase area. The evaluation identifies design smells, clarifies the behavior-preserving refactorings that fit the evidence, and sequences the work into small, testable steps.

This skill is language-agnostic. Apply it to the target repository's actual source language, framework, data model, and testing style instead of assuming this repository's package layout or stack.

## Trigger

Use this skill when the request matches any of:

- "refactoring evaluation", "code smell audit", "refactor assessment"
- "evaluate code health" or "tech debt assessment"
- "identify refactoring opportunities" for a module, package, service, app, script, library, or workflow
- "design-system audit" or "UI token review" when the target scope is explicitly visual or frontend-facing

Do not use this skill for:

- Fixing a specific known bug; use `investigate`.
- Reviewing a branch diff for merge readiness; use `review` or `branch-review`.
- Security-focused auditing; use `security-audit`.
- Broad architecture planning without inspected code evidence; use `scope`, `spec`, or planning review first.

## Goal

Produce an evidence-backed refactoring evaluation with:

- findings classified by impact,
- each finding tied to a named smell or design pressure,
- recommended behavior-preserving refactorings,
- a dependency-aware remediation sequence,
- MCP records when the work needs to survive handoff.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for slice and testing conventions when this repo provides those docs.
- Use this skill for refactoring evaluation methodology and output shape only; local repository instructions, contribution docs, architecture decision records, and test policy take precedence for implementation details.

## Reference Method

Primary baseline:

- Martin Fowler with Kent Beck, `Refactoring: Improving the Design of Existing Code`. If a local extracted copy is available, set its path via the `REFACTORING_TEXT` environment variable.

Use the reference text (when available) as a source for smell names, refactoring names, and the behavior-preserving discipline. Do not copy long passages from it. Paraphrase, cite short method names, and tie recommendations to inspected code evidence.

Optional overlays:

- Language-specific refactoring books or style guides when the target repository actually uses that language.
- Frontend design-system guidance when the target scope includes UI components, CSS, design tokens, or visual hierarchy.
- Persistence, API, or workflow-specific rules from the consumer repository when those rules are explicit in local docs or tests.

The Fowler/Beck vocabulary is the shared baseline. Stack-specific overlays refine it; they do not replace it.

## Domain Detection

Infer the evaluation dimensions from file paths and code content. Do not ask when the target is clear.

### Code Structure - All Source Languages

Apply to any source file that implements behavior: application code, libraries, services, CLIs, scripts, tests with substantial helper logic, generated-code wrappers when locally maintained, and workflow automation.

Use language-appropriate thresholds. A 40-line declarative schema, a 40-line parser, and a 40-line UI render function carry different risk. Count structure, nesting, parameter shape, mutation, duplication, dependency direction, and change coupling rather than line count alone.

Core smell families to scan:

| Smell Family | Evidence To Look For | Common Refactoring Directions |
|---|---|---|
| Unclear names | Names that hide domain meaning or force readers to inspect implementation | Rename Variable, Rename Field, Change Function Declaration |
| Duplication | Repeated logic, repeated condition shape, copy-pasted validation, parallel tests | Extract Function, Pull Up Method, Form Template Method, Extract Class |
| Long routine | Many responsibilities, mixed abstraction levels, nested setup plus behavior | Extract Function, Replace Temp with Query, Split Phase |
| Wide inputs | Long parameter lists, recurring parameter groups, flag arguments | Introduce Parameter Object, Preserve Whole Object, Remove Flag Argument |
| Mutable or global state | Shared writable state, temporal coupling, unexpected in-place changes | Encapsulate Variable, Split Variable, Change Reference to Value |
| Change scatter | One concept changed across many files or one file changed for many reasons | Move Function, Move Field, Split Phase, Extract Class |
| Data clumps and primitives | Repeated field groups, raw strings/ints/bools standing in for domain ideas | Introduce Parameter Object, Extract Class, Replace Primitive with Object |
| Repeated conditional dispatch | Same switch/if ladder repeated, type-code branching, role branching | Replace Conditional with Polymorphism, Introduce Special Case |
| Poor object/module boundaries | Feature envy, insider trading, message chains, middle-man wrappers | Move Function, Hide Delegate, Remove Middle Man, Extract Function |
| Oversized or hollow types | Large class/module, data class, lazy element, speculative abstraction | Extract Class, Inline Class, Collapse Hierarchy, Remove Dead Code |
| Comment deodorant | Comments explaining unclear code rather than intent, warnings around fragile flow | Extract Function, Rename, Change Function Declaration |

### Optional Stack Overlays

Apply overlays only when evidence supports them:

- Type systems: null handling, type-code drift, boolean flags, overloaded signatures, unvalidated boundary data, values that should be domain types.
- Functional or dataflow code: pipeline clarity, transformation boundaries, excessive intermediate state, mutation inside transformations.
- Object-oriented code: misplaced methods, inheritance that is not substitutable, anemic data holders, delegation that hides nothing.
- UI/design-system code: ad-hoc color/spacing/type values, weak visual hierarchy, inconsistent component variants, state styles that are not tokenized.
- Persistence/API code: duplicated query construction, transaction boundaries, DTO/domain leakage, schema validation drift, migration or compatibility rules defined by the repo.
- Test code: fragile fixtures, unclear arrange/act/assert boundaries, over-mocked behavior, helper APIs that hide the behavior under test.
- **Deceptive booleans → outcome enum** — caller null-checks pile up on bool/`X|null` return. See [engineering-heuristics.md § Refactoring & Design](../../rules/engineering-heuristics.md#refactoring-design).
- **Strategy-map over switch** — fat if/else-if grows with each new case. See [engineering-heuristics.md](../../rules/engineering-heuristics.md#refactoring-design).
- **Gate class** — repeated async permission checks across handlers. See [engineering-heuristics.md](../../rules/engineering-heuristics.md#refactoring-design).
- **Pipe / condition-object** — 4+ booleans combined and reused. See [engineering-heuristics.md](../../rules/engineering-heuristics.md#refactoring-design).
- **Backpressure / bounded queue** — producer-consumer with no queue bound. See [engineering-heuristics.md § Resilience](../../rules/engineering-heuristics.md#resilience-failure-modes).
- **Amdahl ceiling** — parallelism proposed without serial-fraction math. See [engineering-heuristics.md § Performance](../../rules/engineering-heuristics.md#performance-tail-latency).
- **Algorithmic complexity at scale** — nested iteration on hot path. See [engineering-heuristics.md](../../rules/engineering-heuristics.md#performance-tail-latency).

When a stack overlay and the Fowler/Beck baseline point at the same code, record one canonical finding and note the overlap. Do not create duplicate remediation work.

## Repo-Specific Rules

Before recommending changes, load local instructions and nearby docs when they exist:

- repository agent instructions, `AGENTS.md`, `CLAUDE.md`, `.instructions.md`, or equivalent,
- contribution and testing docs,
- architecture decisions, contracts, schemas, or package READMEs,
- prior refactoring or tech-debt evaluations.

Cite only rules that exist in the target repo. Do not assume this monorepo's domain names, MCP package layout, design-token prefix, greenfield policy, or workflow conventions apply to a consumer repository.

Common local policy families to check when present:

- compatibility and migration policy,
- validation versus assertion boundaries,
- shared transaction or retry wrappers,
- enum/value-object conventions for status values,
- design-token and component-system conventions,
- generated-file and code ownership rules,
- task, handoff, or review recording requirements.

## Core Process

1. Determine the target scope and primary evaluation dimensions.
2. Load local instructions, nearby tests, and prior evaluations for that scope.
3. Inspect the target files and the callers/callees needed to understand behavior.
4. Run the structured evaluation phases below.
5. Record durable findings when the evaluation needs to survive handoff.
6. End with a dependency-aware remediation sequence, not a flat smell list.

## Phase 1 - Scope And Context

1. If MCP is available, call `get_handoff_state` with a bounded read to identify the active task. Pass `task_ref` explicitly after scope is known.
2. Identify the target files, owner boundaries, tests, and public contracts.
3. Check recent history for churn and prior refactor attempts when it helps explain design pressure.
4. Read only enough adjacent code to understand behavior and dependencies. Include callers and tests before prescribing moves.
5. Search for existing evaluations or tracked findings to avoid duplicate work.

## Phase 2 - Systematic Evaluation

Walk the applicable smell families against the target code.

For each potential finding:

1. Measure the evidence: size, nesting, parameters, mutation, duplication, coupling, fan-in/fan-out, test burden, and change history when available.
2. Identify the smell family and the concrete code behavior that creates maintenance risk.
3. Cite file paths and line ranges in the evaluation document.
4. Classify severity:

| Severity | Code Structure | UI/Design System |
|---|---|---|
| High | Broad change scatter, central object doing many jobs, pervasive primitive/domain leakage, unsafe shared state | Token system absent where UI scope depends on it, ad-hoc styling across many components |
| Medium | Long routine, duplicated conditional logic, data clumps, misplaced behavior, unclear boundaries | Partial token coverage, inconsistent component variants, repeated ad-hoc values |
| Low | Naming issues, lazy wrappers, speculative abstraction, comment deodorant, localized cleanup | Minor missing variant, isolated inconsistency |

5. Prescribe a refactoring direction using named Fowler/Beck techniques where possible.
6. State the smallest behavior-preserving verification step for the fix.

## Phase 3 - Architectural Observations

After individual findings, summarize structural forces:

1. What the code already does well and should preserve.
2. Where the architecture creates smell pressure across multiple findings.
3. Which boundaries are stable enough to refactor now and which need tests first.
4. Which findings overlap so one remediation phase can address them together.

Use an overlap table when multiple dimensions apply:

```markdown
| Finding | Related Finding Or Dimension | Overlap | Canonical Action |
|---|---|---|---|
```

## Phase 4 - Remediation Sequence

Order recommendations by dependency and risk. Prefer small, behavior-preserving steps with tests green between steps.

Structure phases like this:

```markdown
### Phase N: <Title> (addresses <finding-ids>)

**Effort:** Small | Small-Medium | Medium | Medium-Large | Large
**Prerequisites:** <none or earlier phase, with reason>
**Verification:** <specific test or inspection gate>

1. <Specific step>
2. <Specific step>
```

Sequence guidance:

- Add or stabilize characterization tests before moving behavior with weak coverage.
- Rename and extract before moving code across ownership boundaries.
- Encapsulate mutable data before changing how it is represented.
- Split phases before introducing polymorphism or new domain objects.
- Remove speculative abstractions only after confirming there is no active caller contract.
- Keep UI/design-token changes separate from behavior refactors unless one blocks the other.

End with a checklist:

```markdown
## Checklist

- [ ] Phase 1: <item>
- [ ] Phase 2: <item>
```

## Phase 5 - Record And Output

### MCP Recording

Record each durable finding in MCP before treating the evaluation as complete:

```text
review_findings(review={
  "operation": "record",
  "session": "<session-id>",
  "finding_id": "REFACTOR-<severity-prefix><n>",
  "severity": "high|medium|low",
  "file_path": "<repo-relative-path>",
  "description": "<smell>: <evidence and recommended refactoring>",
  "task_ref": "__repo__",
  "review_mode": "branch",
  "details": {
    "line_start": N,
    "line_end": N,
    "fix": "<recommended refactoring direction>"
  }
})
```

Finding ID format: `REFACTOR-H<n>`, `REFACTOR-M<n>`, `REFACTOR-L<n>`. Use `batch_record` when recording three or more findings.

Use `task_ref="__repo__"` for repo-scoped evaluations not tied to a specific task. If the evaluation belongs to an active task, use that task ref.

### Document Output

Write the evaluation to the repository's established tech-debt or planning location. If no convention exists, use a tracked path such as `docs/tech-debt/refactoring-<scope>-evaluation.md` or the closest repo-local equivalent.

Recommended document shape:

```markdown
# Refactoring Evaluation: <Scope Title>

**Date:** <YYYY-MM-DD>
**Scope:** <description>
**Method:** Fowler/Beck smell and refactoring catalog, plus <optional overlays>
**Evidence:** <files/tests/docs inspected>

---

## Executive Summary
## Methodology
## What The Codebase Does Well
## High-Impact Findings
## Medium-Impact Findings
## Low-Impact Findings
## Architectural Observations
## Recommended Refactoring Sequence
## Overlap Index (if needed)
## Checklist
```

### Decision Recording

Record the evaluation completion:

```text
record_event(event={
  "event_kind": "decision",
  "decision": "refactoring_evaluation_<scope>_<date>",
  "rationale": "Evaluation document: <path>; findings recorded: H:<n> M:<n> L:<n>; verification: <coverage>; open threads: <next phases>"
})
```

Refresh the operator view with `render_handoff(kind='dashboard')` after state-changing writes when the write path did not already do so. Generate `CURRENT_TASK.json` only for an explicit legacy export request.

## Recovery

- If MCP is unavailable, write the evaluation document and record findings when MCP returns. Do not claim durable recording happened until it has.
- If the scope is too broad, split by boundary or dimension and produce one evaluation at a time.
- If prior evaluation findings still apply, update their status or reference them instead of duplicating them.
- If a refactor recommendation requires ownership beyond the requested scope, record it as a dependency or follow-up, not as hidden work.

## Common Rationalizations

- "I can just list smells." A useful refactor plan needs sequencing and verification gates.
- "This is language X, so Fowler/Beck does not apply." The mechanics need adapting, but the smell vocabulary and behavior-preserving discipline still apply.
- "The UI and code-structure issues are basically the same." They may overlap, but they need one canonical finding and a clear action, not duplicate work.
- "This is only advisory, so MCP recording does not matter." Long-running refactors often span sessions; durable findings keep the plan recoverable.

## Red Flags

- The target scope spans multiple domains with no primary boundary.
- Findings cite code you have not inspected.
- Recommended refactors change behavior without a test or characterization step.
- Stack-specific guidance is being applied to a repo that does not use that stack.
- The evaluation drifts into bug fixing, security review, or merge-readiness review.

## Convergence Criteria

- The target scope and local rules are explicit.
- Every finding has inspected evidence, a named smell, severity, and refactoring direction.
- High-impact findings cite concrete files and lines in the evaluation document.
- The remediation sequence is dependency-aware and includes verification gates.
- Durable findings and a completion decision are recorded in MCP when MCP is available.
- The final response includes whether handoff was updated.

## See Also

- [../investigate/SKILL.md](../investigate/SKILL.md)
- [../review/SKILL.md](../review/SKILL.md)
- [../../rules/development-workflow.md](../../rules/development-workflow.md)