# Branch Review Guide

> **Reference Appendix.** The skill at [`.claude/skills/branch-review/SKILL.md`](../../../.claude/skills/branch-review/SKILL.md) is the primary entry point for branch review. This guide is consulted from the skill, not loaded directly as the execution surface.

> Structured checklist for reviewing feature branches before merge. Checklist items reference [`instructions.md`](../instructions.md).

## Quick Navigation

For planning documents (task plans, epics, ADRs), use [planning-review-guide.md](planning-review-guide.md) instead.

**Stack guides** (load only those relevant to the branch diff):

- Python / FastAPI / SQLAlchemy → [branch-review-python.md](branch-review-python.md)
- TypeScript / React / Vitest → [branch-review-typescript.md](branch-review-typescript.md)
- PHP / Composer / PHPUnit → [branch-review-php.md](branch-review-php.md)

---

## How to Use This Guide

### When

Run on every feature branch **before merge to `main`**. For multi-worktree orchestration, review from the orchestrator root and log findings into MCP before routing to worker lanes.

### Who

The reviewer can be human or agentic. Agent reviewers must:

1. Walk each checklist section, citing specific files and line numbers
2. Classify findings by severity and category (below)
3. Record each finding into MCP handoff (see MCP Handoff Integration)
4. Only produce a report file if the user explicitly requests one
5. For worktree-split tasks, run `make handoff-dispatch TASK=<task-ref>` after logging findings

**Hard rules for agent responses:**

- Never present a finding in chat unless it has a stable `finding_id` in MCP.
- If discussed before recording, immediately record it and reference the `finding_id`.
- Never write review findings into task plans, ADRs, or other planning docs. Use MCP handoff; generate `CURRENT_TASK.json` only as an explicit compatibility export.

### Review Findings Placement (MANDATORY)

Review findings live in `workbay-handoff-mcp`. Record with `review_findings(review={"operation":"record"|"batch_record", ...})`, read with `review_findings(review={"operation":"list"|"get"})`. **Pasting a finding list into a task plan, epic, ADR, or any other markdown document is forbidden** -- it duplicates the source of truth and escapes the pre-merge gate.

Reference findings by ID (`see internal in handoff`), never by duplicating their bodies. `DASHBOARD.txt` is the operator-facing projection; `CURRENT_TASK.json` is an on-demand export for legacy consumers, not the review source of truth.

**Finding status is DB-only.** Do not mirror status into task-plan checklists or success criteria — no `(BR-08 closed)` / `(BR-09 fixed)` / `(deferred)` trailers, and no checklist items whose completion is keyed to a finding ID. A Slice checklist describes the work being delivered; the live status of any finding for that Slice is queried via `review_findings(review={"operation":"list","status":"open","task_ref":"<task>"})` or read from `DASHBOARD.txt`. Mirroring status in markdown invariably drifts: a finding is reopened, re-classified, or fixed on another branch, and the checkbox silently lies. The pre-merge gate (`handoff_close_check`) also only audits MCP-stored findings, so a "closed in the plan, still open in the DB" row blocks merge with no obvious cause.

When implementation fixes an existing finding, prefer the commit-backed resolution path instead of writing a substitute blocker or no-op test row. Use `mcp-workbay-handoff review-findings --operation resolve --task-ref <task-ref> --resolve-finding-id <finding-id>` or the MCP equivalent `review_findings(review={"operation":"resolve", ...})`. If the receipt reports `pending_uncommitted` or `blocked_by_context`, leave the finding open, surface that receipt, and do not record dummy evidence as a stand-in for disposition. Every new guard message that rejects a finding closure must name the sanctioned call shape in the invocable single-`review`-param form, usually `review_findings(review={"operation":"resolve", "all_open":true, "verification_evidence":...})`; orphan or done-task rows use `review_findings(review={"operation":"disposition", ...})`.

**Enforcement:** `PreToolUse` hook (`scripts/hooks/guard-task-plan-findings.py`) rejects 3+ consecutive finding-style bullets in task plan files, plus any finding bullet under a `Findings` heading. Also wired into `make check-all` via `make lint-task-plans`. Exposes `--scan-staged` for opt-in pre-commit integration. The hook does **not yet** catch single-bullet status-tracking trailers (e.g. `- [x] ... (BR-09 closed)`); track proposed follow-ups in handoff rather than package-local docs.

**No revision-history blocks in task plans.** The same hook rejects `Revision history:` sections and `## Revision history` headings inside task-plan paths (`docs/tasks/**`, `packages/*/docs/tasks/**`). Mutable execution history — status changes, slice-close rationale, review-pass results, run history, and similar prose — belongs in the handoff DB. Record it through `set_handoff_state`, `record_event`, `close_slice`, `review_findings`, and `render_handoff`. Stable planning prose (objective, constraints, current-state analysis, slices, verification intent) stays in the markdown plan; the mutable diary does not. Legacy numbered plans under `docs/plans/**` are historical artifacts and remain out of scope for this guard, even when they carry revision-history sections.

### Scope

- Local exploratory review: inspect **uncommitted working-directory changes** (`git status` / `git diff --name-only`) before commit when you need quick feedback.
- Recorded branch review: use a committed scope only. Review `git diff --name-only main...HEAD`, or prefer the MCP-backed latest-slice packet when available.
- `review_runner.py run --record-findings` must not sign off on dirty `branch_diff` scope. Commit or stash first, or rerun with latest-slice scope so findings attach to committed state.

### Automated Review

This guide is also consumed as prompt input by `review_runner.py` (`make review-run TASK=<task> LANE=<lane>`). Structural changes here affect automated review fidelity. The orchestrator daemon dispatches automated-review findings to worker lanes via `make handoff-dispatch`.

### Dev Tooling (Lightweight Review)

Files under `scripts/mcp/`, MCP test files, and other dev tooling get a lightweight review:

- Correctness and obvious bugs only (skip metric thresholds, architecture boundary checks, Protocol typing)
- Still record findings via MCP handoff; never HIGH unless they corrupt production data or state

---

## Review Intake

Load only the minimum review packet before walking the checklist:

1. Intended change (task plan, scoped request, or branch objective)
2. Actual diff or working-directory change set
3. Touched boundary contracts, ADRs, and repo rules
4. Proof artifacts already produced (tests, type checks, static analysis)

Required intake details: branch/commit range, intended scope reference, relevant contracts/ADRs/rules, verification commands already run, review mode (normal or `release_audit`), scope source (`slice_packet` or `branch_diff`).

Do not bulk-load unrelated docs, lane chatter, or historical artifacts unless required for correct review.

### Latest-Slice Review Preference

For cross-agent post-implementation review, query the latest-slice packet before reading git diff:

1. Request the latest completed slice packet for the active task (optionally scoped by lane).
2. Use packet `changed_files` as the authoritative file set when `scope_source="slice_packet"`.
3. Confirm the packet carries verification evidence and contract/doc touches.
4. Fall back to branch diff only when no valid packet exists; call that out explicitly in review output.

Never reconstruct the "latest slice" from chat memory or recent commits when MCP packet state is available.

### Handoff-only Fallback

When `workbay-orchestrator-mcp` is not loaded, use this degraded path:

1. `load_session`
2. `search_handoff(queries=["slice_complete"], record_types=["decision"], limit=1)`
3. `get_verified_tests(task_ref=..., commit_sha=...)`
4. `review_findings(review={"operation":"list","status":"open"})`

Call this out as fallback scope in review output. Prefer `get_latest_slice_review_packet` when orchestrator is available.

## Fresh Verification Evidence

Treat any claim of "done", "fixed", "passing", or "ready" as unproven without fresh verification evidence for the current branch state. Historical test rows are context, not proof.

Reviewer prompts:

- What command would prove this claim? Was it run on the current branch state?
- Does the output support the full claim, or only part of it?
- Is runtime-sensitive behavior justified only by unit tests?

If no: `GAP` finding. `HIGH` when the missing proof affects merge or release readiness.

## Contract-Change Gate

Apply when the diff changes a documented boundary payload, status/error contract, shared enum vocabulary, or adapter-owned envelope shape.

- **Review-ready** requires: owning contract updated in the same slice (or explicit no-change rationale), plus matching fixture/schema/contract-test evidence.
- **Merge-ready** requires: handoff records boundary owner, verification path, and current contract reference.
- Canonical review aid: [contract-change-checklist.md](contract-change-checklist.md).

## Common Checklist

Language-agnostic items. Stack-specific items are in the language guides linked above.

### Branch Isolation

Reference: [development-workflow.md](development-workflow.md#branch-isolation-protocol-mandatory).

- [ ] **Code changes are not on `main`** — code-file edits under `apps/` or `packages/` on `main` are HIGH.
- [ ] **Branch name matches task plan** — matches `workbay_protocol.branch_naming.TASK_REF_RE` (lowercase `feature/<task-ref>-<slug>` with at least one digit; e.g. `feature/internal-37-pre-push-mirror` accepts, `feature/bad-name` rejects) and matches the task plan's `Target Branch`. implementation note wires this rule into post-checkout (warn), PreToolUse (block), pre-commit (block + audited override `AGENTIC_ALLOW_NONCONFORMING_BRANCH=1`), and pre-push (block + distinct override `AGENTIC_ALLOW_NONCONFORMING_BRANCH_PUSH=1`).
- [ ] **Plan-backed branch carries its `-plan<NNNN>` segment** (implementation note) — a task whose live handoff row has a `task_plan_path` (or an accepted numbered-plan baseline) MUST run on a `feature/<task-ref>-plan<NNNN>` branch. This is now an enforced lifecycle invariant, not just a convention: `review-ready`/`handoff-close-check` emit `plan_id_missing_from_branch` (owner bucket `planning_baseline`) and refuse to pass a segment-less plan-backed branch, and `task-start` refuses with `plan_id_unresolved_for_plan_backed_task` rather than mint one when it can already resolve the task as numbered-plan-backed. `TASK_REF_RE` is unchanged (the segment stays grammar-optional so non-plan and `MAINT-*` planless tasks keep their bare `feature/<task-ref>`); enforcement lives only in the lifecycle gates, which hold the task context the pure-grammar git-hook validator lacks. Remedy for a stranded segment-less branch: rename to `feature/<task-ref>-plan<NNNN>`, or re-tear via `make task-finish` then `make task-start TASK=<task-ref> PLAN=docs/plans/<NNNN>-*.md`.
- [ ] **No stale cross-branch bleed** — diff does not include unrelated changes from a dirty `main` working tree.

### Correctness

- [ ] **Migration ↔ Model parity** — constraints match between migration and ORM model.
- [ ] **Schema-column parity** — SQL `WHERE`/`JOIN` keys match actual schema columns (no stale key names).
- [ ] **No unreachable code** — dead branches inside conditionals.
- [ ] **No duplicate field declarations** — Pydantic models, dataclasses.
- [ ] **API contract alignment** — response schemas match `docs/workbay/contracts/`. New fields have tests.
- [ ] **Boundary metadata preservation** — adapters do not invent envelope metadata; every field traces to request, upstream payload, or documented fallback.
- [ ] **Assertion intent matches layer** — assertions for internal invariants only, never boundary validation.
- [ ] **Runtime dependency integrity** — no type-only shims masking missing runtime packages; verify with real build/test.
- [ ] **PHP runtime autoload parity** — new classes resolve under the real framework/Composer load path, not only PHPUnit bootstrap.
- [ ] **Atomic mutation path preserved** — no splitting atomic backend writes into multiple client mutations without sign-off.
- [ ] **Primary control reachability** — primary actions reachable from initial zero-state UI.
- [ ] **Stale/offline path remains user-recoverable** — automation cannot remove explicit manual recovery actions.
- [ ] **Interactive timeout parity** — related remote calls use shared timeout helper and consistent budgets.
- [ ] **Import/restore payload validation** — malformed snapshots fail fast with explicit errors.
- [ ] **Provenance preservation** — status/update operations do not overwrite original creator metadata.
- [ ] **Single contract owner per boundary** — only one layer owns shape adaptation; downstream consumes the canonical shape.
- [ ] **Contract-change gate satisfied** — boundary changes have owning-contract updates or no-change rationale, plus fixture/schema/test evidence.

### Code Duplication

- [ ] **Shared test stubs** — Protocol stubs used in 3+ files extracted to shared location.
- [ ] **One canonical fake per protocol** — no divergent fakes across test files.
- [ ] **No duplicate methods** — interfaces have no aliased methods.

### Tests

- [ ] **No permanently skipped tests** — every skip has an issue reference.
- [ ] **No empty test bodies** — `pass`/`...` tests completed or deleted.
- [ ] **No false-positive fakes** — test fakes are configurable, not hardcoded to return `None`/empty.
- [ ] **Single injection strategy** — don't mix `dependency_overrides` and `monkeypatch.setattr`.
- [ ] **Adequate coverage for new components** — render, loading, error, and primary interaction.

### Test Quality (Farley 8 Properties)

- [ ] **Understandable** — test name and body state intent without spelunking; a reader knows what broke from the failure alone, not from reverse-engineering setup.
- [ ] **Maintainable** — no leaked implementation detail (e.g. selectors, internal call order); drives behavior through a stable seam, so a refactor doesn't churn the test.
- [ ] **Repeatable** — deterministic and isolated; no shared mutable state, clock/network/order dependence, or cross-test bleed.
- [ ] **Atomic** — one behavior per test; a failure points at a single cause, not a bundle of unrelated assertions.
- [ ] **Necessary** — asserts behavior the spec requires; not a tautology, not re-testing the framework, not duplicating an existing case.
- [ ] **Granular** — exercises one unit/seam at the right level; not incidental end-to-end sprawl when a focused test suffices.
- [ ] **Fast** — no gratuitous sleeps, real network, or heavyweight fixtures where a fake or seeded state would do.
- [ ] **First / TDD-shaped** — test specifies behavior rather than mirroring the implementation after the fact.

_Advisory: these mirror the external `farley_score_plugin` (which weights Understandable/Maintainable highest). After installing it (`/plugin marketplace add mse-online/farley_score_plugin`), `/msec:farley-score` scores pytest/unittest/Jest/Vitest suites — opt-in, never a close-check gate, no PHP/PHPUnit coverage._

### Documentation & Cleanup

- [ ] **Format applied** — `make format-all` (or per-component equivalent) run on the branch; `make lint-all` passes with zero format violations.
- [ ] **No stale comments** — "TODO", "assuming this", "will verify" resolved or removed.
- [ ] **Planning-doc consistency** — "What works", "What's missing", checklist state, and success criteria do not contradict.
- [ ] **ADR terminology alignment** — accepted domain terms are used consistently (no regressions to retired naming).
- [ ] **Command invocability** — documented commands run as written (no broken copy-paste syntax).
- [ ] **No debug/large artifact commits** — logs and generated output files are ignored and excluded from Git.
- [ ] **No duplicate imports** — each symbol imported once per file.
- [ ] **Docstrings complete** — no empty `Raises:` or `Returns:` sections.
- [ ] **Function-level imports justified** — standard deps at module scope unless genuine cold-start reason.

### Code Health (Tech Debt Prevention)

**Size and responsibility boundaries:**

- [ ] **No god classes/components** -- single class/component file <= ~400 lines. Budget extraction in same PR or file follow-up.
- [ ] **No god components mixing concerns** -- React components must not combine API fetching, domain logic, and rendering. Extract hooks and sub-components.
- [ ] **Hook/function parameter count** -- >8 destructured params must be grouped into typed option objects.

**Primitive obsession and enum discipline:**

- [ ] **Domain concepts as types, not raw strings** -- recurring domain values use enums / `as const` / PHP backed enums, not inline string literals.
- [ ] **No duplicate status definitions** -- single canonical type definition per domain concept.

**Structural duplication:**

- [ ] **Transaction boilerplate not inlined** -- use `run_transactional(callable)` or equivalent; no inline START TRANSACTION / COMMIT / ROLLBACK.
- [ ] **Transform loops not duplicated** -- extract shared pipeline when functions differ only in the loop transform.

**Conditional complexity:**

- [ ] **Nesting depth <= 3** -- refactor to guard clauses.
- [ ] **Wordy conditionals named** -- 4+ part boolean expressions extracted into named variables.
- [ ] **No null sentinels for flow control** -- use explicit booleans or Null Object pattern.

**Design token discipline (CSS/SCSS):**

- [ ] **No ad-hoc font-size literals** -- use the repo's typography tokens.
- [ ] **No hardcoded hex gray values** -- use the repo's gray scale or semantic tokens.
- [ ] **No ad-hoc box-shadow** -- use the repo's elevation tokens.
- [ ] **No hardcoded border-radius** -- use the repo's radius tokens.
- [ ] **Focus rings use shared tokens** -- use the repo's focus-ring and focus-offset tokens.
- [ ] **Status indicators pair color with icon** -- every status color must have a distinguishing icon.

### Bug-Finding Heuristics (Universal)

**Variable identity after normalization:** Verify every subsequent reference uses the normalized variable, never the original.

**Cross-method contract bugs:** When method A transforms data and passes it to method B:

- [ ] Transformation output matches method B's expected input type and format.
- [ ] Error/null returns from B are checked by A.
- [ ] Derivatives computed from the filtered set, not the original unfiltered input.

**Envelope-wrapper sanity check:** When a boundary layer wraps a list payload into an envelope:

- [ ] `limit`/`offset` from request or upstream paging contract, never from list length or hardcoded defaults.
- [ ] `total` from upstream contract; if falling back to page length, that fallback is documented and correct.
- [ ] Provenance fields copied from real upstream value or documented local authority, not guessed.
- [ ] Unexpected upstream shape (e.g. envelope where only list is valid) fails explicitly.

**Boundary value sweep:** Mentally substitute empty, single-element, and large (10k+) inputs for each numeric/collection parameter.

**Retry lifecycle traps:**

- [ ] Per-cycle guards prevent duplicate concurrent retry attempts.
- [ ] Guard reset only on intentional state transitions, preventing infinite or permanently disabled retries.

**Import strictness check:** Feed one malformed payload variant and verify `ok: false` with explicit error.

**Mutable-record provenance:** Confirm updater context does not erase original creator metadata (`agent`, `branch`, `commit_sha`).

**IDE stale-file guard:** Editor file-reading tools may return cached content after git operations. Use native IDE search (`grep_search`) to confirm missing functions; terminal `grep` as fallback for terminal-only environments. File-length mismatches signal stale cache.

**Runtime Red-Flags** (inline cue + lexicon rationale — see [engineering-heuristics.md](engineering-heuristics.md)):

- Retried network/DB/queue write — is this write safe to retry? → [retry-without-idempotency](engineering-heuristics.md#resilience-failure-modes)
- Blocking call with no timeout — what bounds this wait? → [timeout on every blocking call](engineering-heuristics.md#resilience-failure-modes)
- Fetch-all / no LIMIT — what if this returns 10M rows? → [unbounded result set](engineering-heuristics.md#resilience-failure-modes)
- Blocking I/O inside `async def` — is this async code blocking the loop? → [blocking the event loop](engineering-heuristics.md#concurrency-async)
- Shared state touched across `await` — can another task mutate this between read and write? → [shared mutable state across await](engineering-heuristics.md#concurrency-async)
- Read→compute→write without atomicity — do concurrent writers need CAS/serializable? → [lost update / write skew](engineering-heuristics.md#concurrency-async)
- Value-returning fn also mutates — is this query secretly mutating? → [command-query separation](engineering-heuristics.md#refactoring-design)
- Getter returns backing collection — does this getter expose mutable internals? → [encapsulate-collection leak](engineering-heuristics.md#refactoring-design)
- `create_task()` result dropped — is this background task tracked, or orphaned? → [fire-and-forget / orphan task](engineering-heuristics.md#concurrency-async)
- `except CancelledError` without re-raise — does this coroutine honor cancellation? → [swallowed CancelledError](engineering-heuristics.md#concurrency-async) [CON-03]
- Diff changes untested code with no characterization test — what does this code do *today*? → [TEST-03](engineering-heuristics.md#testing-strategy)
- Test hits DB/network/filesystem or mutates a shared fixture — fast-suite discipline? → [TEST-07]/[TEST-09](engineering-heuristics.md#testing-strategy)
- Fix claimed without re-running the original failing sequence — actually fixed? → [DBG-06](engineering-heuristics.md#debugging-procedure) / [AGT-03](engineering-heuristics.md#agent-craft)
- Multi-change "see if it helps" diff — which single variable does this test? → [DBG-03](engineering-heuristics.md#debugging-procedure)
- LLM output reaching SQL/shell/eval/HTML, or new tool scope for a model — trust boundary held? → [SEC-02]/[SEC-04](security-heuristics.md#1-llm-agent-security)
- Published-API element removed/renamed/retyped — could any caller observe this? → [API-09](engineering-heuristics.md#api-design)
- New endpoint/queue/external call with no telemetry — invisible in prod? → [OBS-01](engineering-heuristics.md#observability)
- Completion claim without command + decisive output line — evidence verbatim? → [AGT-04](engineering-heuristics.md#agent-craft)

Cite lexicon rules by ID (e.g. `[RES-01]`) in findings — IDs are stable citation keys; the full rule lives one anchor away. Review-phase rules carry the `·r` tag in the lexicon's `T·P` column — filter on the tag rather than copying rows into this guide.

**False-fix detection (review finding closures):** When findings are marked `fixed`, verify each closure:

- [ ] Claimed code change exists in current working tree (grep for function/variable by name).
- [ ] Resolution notes reference real file paths and function names verifiable by string search.
- [ ] Rapid closures (3+ in <60s) are suspect -- each needs independent fix evidence.
- [ ] Reopened findings (reopen_count >= 2) require `verification_evidence` with concrete proof.
- [ ] Plan documents updated alongside bulk closures are cross-checked against actual code.

The `update_review_finding` tool enforces two guards:

1. **Reopen escalation:** Findings reopened >= 2 times require `verification_evidence` to mark `fixed`.
2. **Batch-close detection:** 2+ findings fixed in last 60 seconds require `verification_evidence` for subsequent closures.

Satisfy guards with string-search output, diff excerpts, or code snippets proving the fix.

---

## Multi-Lens Audit Workflow

Escalate to a multi-lens audit when the branch touches: security/compliance boundaries, release/deploy paths, major architecture transitions, multi-service state machines, high-risk persistence/migration, or broad UI/UX surfaces.

Record all findings via MCP with `review_mode=release_audit`. Use finding-ID prefixes: `ARCH-<id>`, `QA-<id>`, `CONTRACT-<id>`.

### Lens Definitions

| Lens | Focus | Bias toward |
|------|-------|-------------|
| **Architecture / Reliability** | Ownership boundaries, adapter seams, single-writer discipline, degraded/error behavior, retry/timeout/recovery, cross-service race conditions | Hidden coupling, state drift, unsafe fallbacks, replay hazards |
| **QA / State-Matrix** | State coverage (loading/empty/degraded/error/stale/recovery), runtime-parity proof, regression traps in polling/retries/import flows | Missing tests, stale verification, unproven fixes, uncovered state combinations |
| **Contract / Compliance** | Contract ownership, same-slice updates, schema/fixture parity, boundary metadata provenance, release claims | Undocumented shape changes, backward-compat drift, missing contract co-change |
| **Resilience/Stability** | Timeouts, circuit-breaker behavior, pool exhaustion, unbounded result sets, retry/backoff, steady-state reclaimers — see [engineering-heuristics.md § Resilience & Failure Modes](engineering-heuristics.md#resilience-failure-modes) | Missing timeout, retry storm, resource leak, cache without bound |
| **Concurrency/Async** | Event-loop blocking, shared state across `await`, cancellation hygiene, orphan tasks, lost-update/write-skew — see [engineering-heuristics.md § Concurrency & Async](engineering-heuristics.md#concurrency-async) | Blocking async path, race across await, swallowed `CancelledError` |
| **Data/Consistency** | Consistency model, replication lag, schema compat, fencing tokens, partition skew — see [engineering-heuristics.md § Data & Consistency](engineering-heuristics.md#data-consistency) | Unstated consistency, lost update, integration DB access |
| **Performance/Tail-Latency** | p99 targets, fan-out amplification, coordinated omission, hot-path complexity — see [engineering-heuristics.md § Performance & Tail-Latency](engineering-heuristics.md#performance-tail-latency) | Average-only claims, scatter/gather tail risk, O(n²) on hot path |

### Execution Flow

1. Confirm escalation is warranted from triggers above.
2. Load minimum audit packet: intended change, actual diff, touched contracts/ADRs/rules, fresh verification evidence.
3. Run lenses sequentially: architecture/reliability, QA/state-matrix, contract/compliance.
4. Record findings after each lens before moving to the next.
5. Use `review_mode=release_audit` and lens-specific finding-ID prefixes for all findings.
6. If a later lens depends on an unresolved HIGH from an earlier lens, record that dependency.

### Completion Criteria

- All applicable lenses applied (baseline three + engineering lenses when diff touches network/DB/async/shared-state; even if one found no issues)
- All HIGH findings fixed or deferred with rationale
- Remaining MEDIUM/LOW findings triaged
- Summary decision recorded in MCP with audit scope, outcome, and deferred risk

### Reporting Rules

- Only supported `review_mode` value is `release_audit`. Do not invent new categories.
- Per-lens classification goes in the `finding_id` prefix, not in `review_mode`.
- If triggers are not met, stay in normal branch-review mode.

## Finding Categories

| Category        | Icon          | Description                                                             |
| --------------- | ------------- | ----------------------------------------------------------------------- |
| **ANTIPATTERN** | :warning:     | Works but violates patterns, creating maintenance risk                  |
| **DEAD_CODE**   | :wastebasket: | Unreachable code, unused params, duplicate declarations, skipped tests  |
| **COMPLEXITY**  | :tangled:     | Unnecessary duplication, overly complex functions, missing abstractions |
| **GAP**         | :hole:        | Missing functionality, incomplete contracts, missing tests              |

---

## Severity Classification

| Severity   | Criteria                                                                                      | Action                                    |
| ---------- | --------------------------------------------------------------------------------------------- | ----------------------------------------- |
| **HIGH**   | Incorrect behavior, data corruption, type unsafety, or >200 lines unnecessary duplication     | Must fix before merge                     |
| **MEDIUM** | Architecture violations, maintenance burden, defeated type checking, reduced test reliability | Should fix; defer only with justification |
| **LOW**    | Style, minor cleanup, small duplications                                                      | Fix if easy; otherwise next pass          |

---

## Resolving Findings

Each finding needs an explicit lifecycle transition:

| Outcome | Call |
|---------|------|
| Fixed | `review_findings(operation="resolve", all_open=true)` — commit-backed, auto-fills `verified_commit_sha` on a clean tree (see Closing Findings). Manual `update_review_finding(status="fixed", ...)` only for a single same-commit close. |
| Deferred | `update_review_finding(status="deferred", resolution_notes=...)` |
| Wontfix | `update_review_finding(status="wontfix", resolution_notes=...)` |
| Regressed | `update_review_finding(status="open", reopen_reason="...")` |

`record_decision(...)` may add rationale but does not replace the finding status update.

Before declaring review complete: (1) verify findings written, (2) verify status transitions reflected in MCP, (3) run `get_review_findings_summary` to confirm open/deferred state matches the verdict.

## MCP Handoff Integration (MANDATORY for Agents)

Every finding **must** be recorded into MCP handoff for cross-agent visibility.

### Required Execution Surface

Agents log findings through the handoff MCP server from the orchestrator root. Task plans may update checklist state, but review findings stay in MCP handoff and the dashboard projection.

Pattern:

1. Review from orchestrator root worktree.
2. Use repo-local MCP runtime with `--workspace-root`, `--state-dir`, `--current-task-path`, `--exports-dir`.
3. Record each finding with `review-record` before mentioning in chat.
4. Verify with `review-list` or `review-summary`.
5. Record final review decision with `decision`.
6. Record the branch-mode review run with `review_runs(review={"operation":"record", "review_mode":"branch", ...})`. If `review_runs` is unavailable in the current harness, use the repo-local fallback: `make handoff-review-run TASK_REF=<task-ref> MODE=branch SUBJECT=<branch-or-artifact-path> SUBJECT_KIND=branch VERDICT=<verdict> DECISION=<decision-id> SESSION=<session> RUN_ID=<run-id>`.
7. Dispatch lane work with `make handoff-dispatch TASK=<task-ref>` from root.

Example:

```bash
mcp-workbay-handoff \
  --workspace-root /abs/path/to/repo \
  --state-dir /abs/path/to/repo/.task-state \
  --current-task-path /abs/path/to/repo/CURRENT_TASK.json \
  --exports-dir /abs/path/to/repo/.task-state/exports \
  review-record \
  --session <review-session> \
  --finding-id <finding-id> \
  --severity medium \
  --file-path path/to/file.py \
  --line-start 10 \
  --line-end 20 \
  --description "Concrete bug description." \
  --fix "Suggested remediation."
```

### Recording Findings: Single vs Batch

For 3+ findings in a single pass, use `batch_record_review_findings` (one SQLite transaction, one MCP write batch).

```python
# Preferred for 3+ findings
batch_record_review_findings(
    session="...",
    task_ref="...",
    findings=[
        {"finding_id": "H-1", "severity": "high", "file_path": "...", "description": "..."},
        {"finding_id": "M-2", "severity": "medium", "file_path": "...", "description": "...", "details": {...}},
    ],
)
```

Use single `record_review_finding` for 1–2 findings or when findings are discovered incrementally across separate tool calls.

### After Each Finding (Single-Item Path)

Call `review-record` / `record_review_finding` with:

| Parameter     | Value                                                                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `session`     | Current session identifier (e.g., `2026-02-20-copilot-review`)                                                                        |
| `finding_id`  | Short ID matching the report (e.g., `H-1`, `M-2`, `L-3`)                                                                              |
| `severity`    | `high`, `medium`, or `low`                                                                                                            |
| `file_path`   | Relative path from monorepo root                                                                                                      |
| `description` | One-paragraph description with code references. **ACE rule citation:** if the finding confirms or contradicts a documented `[sr-NNN]` or `[rg-NNN]` rule from the declared ACE playbook (`WORKBAY_ACE_PLAYBOOK_FILES`), include the rule ID in the description text (e.g., "violates [rg-005] schema/contract parity"). This enables automated ACE counter updates. |
| `details`     | Optional object: `{ "line_start"?: int, "line_end"?: int, "fix"?: str }`                                                              |
| `actor`       | Optional object: `{ "agent"?: str, "model"?: str, "model_label"?: str, "reasoning_level"?: str, "branch"?: str, "commit_sha"?: str }` |

### After All Findings Recorded

1. Call `review-summary` / `get_review_findings_summary` to confirm severity/status counts for the task.
2. Use `review-list --status all` / `list_review_findings(status="all")` if you need full finding-by-finding verification.
3. Call `decision` / `record_decision` summarizing the review (finding count by severity, session ID). **The verdict decision must cite the decision number of the artifact under review** (e.g., "review of decision #966") so the reviewed artifact and its review are bidirectionally linked in handoff search.
4. Regenerate `DASHBOARD.txt` if your workflow needs a refreshed operator projection; render `CURRENT_TASK.json` only when a legacy consumer explicitly depends on that export.
5. If this review creates actionable lane work, run `make handoff-dispatch TASK=<task-ref>` from the orchestrator root after logging findings.
6. If this review concludes the task, run `handoff_close_check(enforce=True)` before final handoff.
7. Include `Handoff updated: yes` in the response.

Do not use direct `sqlite3` shell queries for MCP handoff verification when these tools are available.
Do not mention a finding in chat before it exists in MCP handoff with a stable `finding_id`.

### Closing Findings (verification_evidence)

`update_review_finding` accepts optional `verification_evidence` (string, max 2000 chars). **Required** when:

- Finding reopened 2+ times (reopen escalation guard)
- 2+ findings for same task fixed in last 60 seconds (batch-close guard)

Good evidence: string-search output, `git diff` excerpts, code snippets proving the fix. Bad evidence: restating resolution notes or referencing commit SHA alone.

Rejected closures include a `false_fix_guard` object identifying which guard fired.

### Severity Mapping

`HIGH` -> `high` (must fix before merge) | `MEDIUM` -> `medium` (should fix; defer with justification) | `LOW` -> `low` (fix if easy; otherwise next pass)

---

## ACE Reflection

Note `[sr-NNN]` or `[rg-NNN]` rule IDs referenced or contradicted by findings.

- **Confirms** a rule -> increments `helpful`. **Contradicts** -> increments `harmful`.
- Rules with `helpful=0 harmful>=2` become pruning candidates.

### Automated Detection (PostToolUse hook)

The `ace-detect` PostToolUse hook scans `review_findings` `record` and
`batch_record` payloads for ACE rule references and appends durable records to
`.task-state/ace_reflect_log.jsonl`:

1. Records use the backward-compatible shape: `finding_id`, `rule_id`,
   `contradicts`, and `timestamp` (no daemon `cycle` field).
2. Capture runs at tool time when the host supports PostToolUse; the worker
   daemon does not perform a second detection pass.
3. Counter updates remain deferred until the operator runs `make ace-reflect`.

Operational health states: `defined` (no log yet), `detecting` (pending
entries), `applied` (all processed), `backfill needed` (historical findings
reference rules but no log exists).

### Applying Counter Updates

```bash
make ace-reflect WORKBAY_ACE_PLAYBOOK_FILES=docs/workbay/constitution.md
make ace-reflect WORKBAY_ACE_PLAYBOOK_FILES=docs/workbay/constitution.md ACE_ARGS=--dry-run
```

Reads pending entries from `.task-state/ace_reflect_log.jsonl`, increments
`helpful`/`harmful` counters in the declared playbook file(s), and deduplicates
via the orchestrator journal. All local, no model calls.

For programmatic use inside orchestrator tooling:

```python
from workbay_orchestrator_mcp.orchestration.ace_reflect import ace_reflect_on_findings
ace_reflect_on_findings(findings, playbook_files, state_dir=Path(".task-state"))
```

### Curation Report

```bash
make ace-curation-report WORKBAY_ACE_PLAYBOOK_FILES=docs/workbay/constitution.md
```

Prints all bullets where `helpful=0 and harmful>=2`. Delete or revise in a focused PR. Curation is advisory; neither the report nor optional model-backed curation rewrites rules automatically.

---

## Review Report Template

> Only produce when the user explicitly requests a written report. MCP findings are the canonical store.

Save to `docs/tasks/<version>/<branch-name>-branch-audit-findings.md`.

```markdown
# Branch Audit — `feature/<branch-name>`

> **Date:** YYYY-MM-DD
> **Scope:** N files changed, +X / −Y lines vs `main`
> **Categories:** ANTIPATTERN · DEAD_CODE · COMPLEXITY · GAP

---

## Summary

| Severity   | Count |
| ---------- | ----- |
| **HIGH**   | N     |
| **MEDIUM** | N     |
| **LOW**    | N     |
| **Total**  | **N** |

---

## Automated Check Results (reported by submitter)

| Check                                   | Result                                |
| --------------------------------------- | ------------------------------------- |
| `make format-all` (all components)      | :white_check_mark: / :x:              |
| `make check` (ruff + mypy + pytest)     | :white_check_mark: / :x:              |
| `npm run typecheck`                     | :white_check_mark: / :x:              |
| `npm run test -- --run`                 | :white_check_mark: / :x:              |
| `npm run lint`                          | :white_check_mark: / :x:              |
| `check-architecture-compliance.js`      | :white_check_mark: / :x: (N warnings) |
| `composer phpstan`                      | :white_check_mark: / :x:              |
| Cyclomatic complexity (radon, grade C+) | N functions flagged                   |

---

## HIGH Severity

### H-1 · <Title>

|              |                                            |
| ------------ | ------------------------------------------ |
| **Files**    | `path/to/file.py` LN                       |
| **Category** | ANTIPATTERN / DEAD_CODE / COMPLEXITY / GAP |

<Description with specific code references.>

## MEDIUM Severity

<!-- Same structure -->

## LOW Severity

<!-- Same structure -->

---

## Recommended Fix Order

### Phase 1 — Correctness (before merge)

1. **H-1** — <one-line summary>

### Phase 2 — Robustness (soon after merge)

2. **M-1** — <one-line summary>

### Phase 3 — Maintainability (tech debt backlog)

3. **L-1** — <one-line summary>

---

# Consolidated Checklist

## Phase 1 — Correctness (before merge)

- [ ] **H-1** — <action item>

## Phase 2 — Robustness

- [ ] **M-1** — <action item>

## Phase 3 — Maintainability

- [ ] **L-1** — <action item>

## Success Criteria

- [ ] Zero HIGH findings remaining
- [ ] `make check` passes
- [ ] `npm run typecheck` passes with zero new errors
- [ ] All existing tests continue to pass
- [ ] Branch audit re-run shows no regressions
```
