# Planning Review Guide

> **Reference Appendix.** The skill at [`.claude/skills/planning-review/SKILL.md`](../../../.claude/skills/planning-review/SKILL.md) is the primary entry point for planning review. This guide is consulted from the skill, not loaded directly as the execution surface.

> **Purpose:** Structured review checklist for planning documents (assessments, specs, task plans, epics, roadmaps, ADRs, implementation plans, scope/dependency docs) before implementation or approval.
> Planning reviews are document-and-codebase reviews, not branch-diff reviews. For code diffs, use [branch-review-guide.md](branch-review-guide.md).

---

## How to Use This Guide

### Scope

Review the planning document against: (1) the current codebase, (2) adjacent planning docs and contracts, (3) already-completed prerequisite phases, (4) stated success criteria.

**Greenfield constraints** (apply unless task documents an exception):

- Prefer clean rewrites over backward-compatibility shims.
- Schema changes go in the baseline migration, not follow-on migrations.
- Storage migration/preservation work is suspect -- no production data to preserve.

### Agent Procedure

1. Read the planning document.
2. Check referenced code paths and adjacent plans/contracts.
3. Record each finding in MCP handoff before mentioning it in chat.
4. Cite concrete file and line references for both the plan and current implementation.
5. Prefer correctness/scope/architecture findings over stylistic feedback.

Hard rules:

- Do not present a finding in chat unless it has a stable `finding_id` in MCP.
- **Never paste a finding list into the planning document.** Findings live in `workbay-handoff-mcp` (`review_findings(review={"operation":"record"|"batch_record", ...})`). The `scripts/hooks/guard-task-plan-findings.py` hook rejects inlined finding lists. See [branch-review-guide.md § Review Findings Placement](branch-review-guide.md#review-findings-placement-mandatory).

---

## Planning Intake

Load only the minimum planning packet before walking the checklist:

| Item | Detail |
|------|--------|
| Planning document | Path to the doc under review |
| Prerequisites | Assessment/spec/ADR/contracts it depends on |
| Implementation anchors | Current code surfaces the plan claims to change |
| Dependency state | Completed slices or adjacent plans constraining sequencing |
| Post-implementation review mode | Ordinary branch review, specialized module review, or release-style audit |
| Scope source | `slice_packet` for latest completed planning slice; otherwise direct doc/codebase review |

Avoid speculative review against broad repo context. If the plan cannot be evaluated from these surfaces, name the missing dependency as the finding.

### Latest Planning Slice Review

When reviewing the latest completed planning slice, prefer the MCP-backed slice packet:

1. Request the latest slice packet with `review_kind="planning"`.
2. Use packet `changed_files` as review scope when `scope_source="slice_packet"`.
3. Confirm the packet is docs-only; mixed doc-plus-code slices fall back to branch review.
4. If no valid packet exists, state the review uses fallback scope.

### Handoff-only Fallback

When `workbay-orchestrator-mcp` is not loaded:

1. `load_session`
2. `search_handoff(queries=["slice_complete"], record_types=["decision"], limit=1)`
3. `get_verified_tests(task_ref=..., commit_sha=...)`
4. `review_findings(review={"operation":"list","status":"open"})`

State explicitly that this is fallback scope. Prefer `get_latest_slice_review_packet(review_kind="planning")` when orchestrator is available.

---

## Planning Review Checklist

### Current-State Accuracy

- [ ] Current-state claims match the actual codebase.
- [ ] "Already implemented" items are actually implemented.
- [ ] "Missing" items are still actually missing.
- [ ] Existing contracts, schemas, and service ownership are described accurately.

### Internal Consistency

- [ ] Problem statement, current-state section, checklist, and success criteria do not contradict each other.
- [ ] Deferred/stretch items do not conflict with "done" or success-criteria language.
- [ ] Slice ordering matches stated prerequisites and dependencies.
- [ ] Terminology is consistent with current ADRs/contracts.
- [ ] Review findings and handoff action items are tracked exclusively in MCP handoff state, not duplicated into the task plan.

### Architecture and Ownership

- [ ] Proposed changes belong to the named service/layer and do not duplicate existing ownership.
- [ ] New handlers/endpoints are added to the correct boundary.
- [ ] The plan does not re-implement behavior that already exists in another service or adapter.
- [ ] Compound operations have an explicit contract for atomicity, idempotency, and conflict ownership.
- [ ] Boundary-touching slices identify the owning contract and the canonical boundary owner explicitly.
- [ ] Plans state whether compatibility is required; greenfield default is no shim unless an exception is documented.

### Contract and Data Model Realism

- [ ] Proposed request/response fields exist or are explicitly added in the same scope.
- [ ] Proposed conflict/version semantics match the current storage model.
- [ ] Multi-entity operations define which entity/version drives conflict detection.
- [ ] Schema changes are sufficient for the reporting/metrics the plan promises.
- [ ] Migration strategy matches greenfield policy: baseline schema edits, no preservation-only migrations, no backward-compatibility shims unless explicitly justified.
- [ ] If a slice changes a boundary field or payload shape, the plan updates the shared schema/fixture and owning contract in the same slice.
- [ ] If a remediation plan cites a `finding_id`, that id resolves to a real MCP finding or a concrete code site before implementation begins.

### Interface and API Realism

- [ ] Pseudocode functions and helper references map to actual existing APIs/imports or are explicitly marked as new code to create.
- [ ] Enum values, status strings, and filter parameters in the plan exist in the actual API/schema.
- [ ] API capabilities assumed by the plan actually exist; client-side workarounds are noted if not.
- [ ] Files listed for modification actually require code changes; verification-only files are flagged as such.
- [ ] Code location references use function/target names, not brittle line numbers.

### Junior-Agent Implementability

> The plan must be implementable by a junior agent with no prior context, from the text alone. Verify against the codebase, not the plan's self-description. See `TASK_PLAN.template.md` → *Implementation Readiness — Junior-Agent Standard*.

- [ ] **Files _and_ functions named.** Every change site cites a concrete `path:symbol` (function/class/constant), not just a file path. Spot-check that the named symbols actually exist (or are explicitly marked `(new)`).
- [ ] **Grounded, not assumed.** Code claims are verified against the current codebase; no invented APIs, fields, files, or behaviors. The plan states what it verified against (commit/date) when it makes current-state claims.
- [ ] **Reuse named.** Where existing helpers/abstractions should be extended, the plan points at the specific `path:symbol` to reuse rather than implying a reimplementation.
- [ ] **Rationale present.** Non-obvious decisions carry a one-line *why* tied to codebase evidence or a finding/decision id, not bare preference.
- [ ] **Consolidated checklist is self-contained.** The checklist (plus per-slice steps) can be executed end-to-end from the plan alone — no "see chat", "obvious", or undocumented prerequisite steps; each slice's files/functions and closing proof are present.
- [ ] **Proof pre-defined per slice.** Each slice names the exact command/fixture/observable that makes it honestly complete.

### Naming and Reference Compliance

- [ ] Epic title uses the `E<number>. <Title>` format with the correct global sequential index.
- [ ] Epic declares an `Epic Short ID` near the top of the document.
- [ ] Task plan title uses the `<EpicShortID>-<N>. <Title>` format matching the owning epic's short id, or a documented package/project-local task id when the plan is not epic-owned.
- [ ] Decision ids referenced in the plan follow the `<author_tag>_<kind>_<work_ref>_<slug>` grammar.
- [ ] Historical docs and decisions are treated as grandfathered; the plan does not mandate retroactive renames unless a concrete artifact blocks tooling or review.

### Planning Pipeline and Lifecycle Compliance

This section is the canonical planning-pipeline compliance reference. Epic lifecycle reference: [development-workflow.md](development-workflow.md).

- [ ] **Intake Q&A present for new features.** For new features/epics where the problem was not pre-traced to code: a scope intake pass (3–5 questions using the `scope` skill) was conducted before assessment; answers are recorded as MCP decisions on the task ref; scope one-pager exists in `docs/scopes/` or the planning doc's objective section includes explicit Not-Doing language. Bug fixes, tech debt tasks, and spec-derived tasks are exempt.
- [ ] **Pipeline stage appropriate.** Assessments surface problems, specs define testable changes, ADRs resolve design uncertainty, task plans define executable slices. Mixed-stage artifacts should be split.
- [ ] **Upstream traceability present.** Specs trace to assessment findings, task plans to spec items or epic deliverables, ADRs to blocked spec items. Skipped stages require stated justification.
- [ ] **Exit gates satisfied for upstream stages.** Task plans derived from specs/ADRs require passed review gates on those upstream artifacts.
- [ ] **Epic-to-task decomposition sound.** Each epic phase maps to task plans. Task plans do not span phases unless justified. Phase ordering matches dependency ordering.
- [ ] **Target branch declared.** Task plans declare a `Target Branch` in metadata. Plans without one should be flagged.
- [ ] **Version directory consistent.** Epics filed under `docs/epics/v<version>/` matching their milestone. Task plans reference the correct epic path. Carry-forward notes present when work migrated from an older epic.

### Rollout and Testability

- [ ] Incremental implementation without impossible intermediate states.
- [ ] Cross-boundary work cites the governing spec/ADR.
- [ ] Each slice names files, contracts, and tests it touches.
- [ ] Each slice states what proof makes it honestly complete.
- [ ] Scaffold-only slices rejected unless they deliver executable value in the same slice.
- [ ] Plan declares expected review path: ordinary, specialized module, or release-style audit.
- [ ] Tests validate real behavior, not placeholder scaffolding.
- [ ] Manual/E2E-only steps do not hide core correctness gaps.
- [ ] Success criteria are objectively testable from code and tests.

### Complexity Control

- [ ] Reuses existing abstractions where appropriate.
- [ ] New abstractions justified by real seams, not hypothetical flexibility.
- [ ] Scope minimal for the stated phase goal.
- [ ] Stretch work truly optional and not required for honest phase completion.

### Tech Debt Awareness

Ref: `docs/tasks/tech-debt/refactoring-*.md`.

- [ ] **God-object growth budgeted.** Adding logic to a class/component >~400 lines must include extraction work or explicitly note the debt increase with a follow-up reference.
- [ ] **New domain concepts typed, not stringly.** New status values, operation types, or domain identifiers must use enums / value objects / `as const`, not raw strings.
- [ ] **Transaction/boilerplate duplication avoided.** New PHP mutation endpoints must use the shared `run_transactional()` wrapper.
- [ ] **Hook/component decomposition considered.** New UI logic targeting an already-flagged god component must scope into a focused sub-hook or sub-component.
- [ ] **Design token surfaces used.** New visual properties must reference the repo's design tokens, not raw values. New tokens go in the shared surface.

---

## Finding Categories

Same as branch review:

| Category | Meaning |
|----------|---------|
| **ANTIPATTERN** | Works conceptually but pushes the system toward the wrong structure |
| **DEAD_CODE** | Obsolete doc paths, stale assumptions, or plans for nonexistent code paths |
| **COMPLEXITY** | Unnecessary abstraction or over-scoped implementation |
| **GAP** | Missing contract, test, migration, dependency, or rollout detail |

---

## Severity Guidance

- **HIGH**: the plan cannot be implemented correctly as written, or it assigns work to the wrong architectural owner
- **MEDIUM**: the plan is implementable but likely to cause regressions, churn, or contradictory completion status
- **LOW**: cleanup, wording drift, or minor sequencing/documentation gaps

---

## MCP Handoff Integration (MANDATORY for Agents)

For every finding, call `record_review_finding` / `review-record` with:

| Parameter     | Value                                                                                                                               |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `finding_id`  | Short ID matching the report (e.g., `internal`)                                                                                  |
| `severity`    | `high`, `medium`, or `low`                                                                                                          |
| `file_path`   | Planning doc path (monorepo-relative)                                                                                               |
| `description` | One-paragraph description with evidence. Include `[sr-NNN]`/`[rg-NNN]` rule IDs when a finding confirms or contradicts an ACE rule. |
| `session`     | Current session identifier                                                                                                          |
| `task_ref`    | Task ref owning the plan (may differ from active task; pass explicitly)                                                             |
| `details`     | Nested object: `{ "line_start"?: int, "line_end"?: int, "fix"?: str }` -- **must be nested, NOT top-level**                         |
| `actor`       | Nested object: `{ "agent"?: str, "model"?: str, "model_label"?: str, "reasoning_level"?: str, "branch"?: str, "commit_sha"?: str }` |

> **Schema contract**: `line_start`, `line_end`, and `fix` MUST be inside `details`. Top-level placement fails with schema validation error.

After the review:

1. Confirm findings with `list_review_findings` or `get_review_findings_summary`.
2. Record a verdict decision citing the decision number of the artifact under review (e.g., "review of decision #966") for bidirectional linking.
3. Record the planning-mode review run with `review_runs(review={"operation":"record", "review_mode":"planning", ...})`. If `review_runs` is unavailable in the current harness, use the repo-local fallback: `make handoff-review-run TASK_REF=<task-ref> MODE=planning SUBJECT=<doc-path> SUBJECT_KIND=<task_plan|epic|adr|roadmap|other> VERDICT=<verdict> DECISION=<decision-id> SESSION=<session> RUN_ID=<run-id>`.
4. If requested, patch the plan to resolve findings.
5. Regenerate `DASHBOARD.txt` for the operator view. Use `render_handoff(kind='current_task', task_ref=<active-task-ref>)` only when a legacy consumer explicitly needs the task-scoped export, and always use the **active** task's ref.
6. Include `Handoff updated: yes` in the final response.

---

## Output Expectations

Finding priority order: (1) obsolete assumptions, (2) architecture/ownership mistakes, (3) greenfield-policy violations, (4) contradictory scope/checklist logic, (5) contract gaps, (6) unnecessary complexity.

Do not spend review time on prose polish unless it affects implementation correctness.
