# Incremental Implementation

## Overview

Use this skill when turning a task plan into code or docs slices. It enforces vertical, test-backed increments so each slice closes one user-visible path instead of spreading unfinished work across layers.

## Trigger

Use this skill when:

- starting feature implementation from a reviewed task plan
- choosing the next slice under an active task
- deciding whether a proposed diff is one slice or several

Do not use it for planning review, branch review, or pure handoff bookkeeping.

## Goal

Advance one bounded plan item with a complete end-to-end slice that starts with a failing test, stays scoped to one behavior path, and ends ready for `make slice-commit`.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
- [../../../docs/tasks/17.0/internal-tdd-and-incremental-implementation-skills-task-plan.md](../../../docs/tasks/17.0/internal-tdd-and-incremental-implementation-skills-task-plan.md)

This skill owns slice sizing, vertical-path discipline, and plan-item advancement. The `tdd` skill owns the RED -> GREEN gate inside the slice.

## Core Process

1. Load the active task plan and identify the next slice worth shipping. If prior slices are unclear, use `search_handoff` to find the latest slice summaries first.
2. Advance the slice cursor with `plan_cursor(operation="upsert", task_ref=..., plan_item_id=..., require_clean_slice=true)`. The `require_clean_slice` guard means the next slice does not start while open findings still exist on the prior one.
3. Define the smallest end-to-end path that delivers user value. Prefer one behavior path such as one field, one endpoint, one UI state, or one docs workflow step. When writing the slice's checklist items, include explicit evidence anchors in each item's body — a backtick-quoted file path, a `make <target>` reference, a `Slice N` reference, or a recorded decision id — so the post-slice `sync-task-plan-checklist` recipe can resolve the item to its proof.
4. Open the slice with the `tdd` skill: write the failing test first, run `make slice-start TASK=<task-ref> TEST_CMD="<command>"`, then make the smallest change that turns the check green.
5. Scaffold only the signatures the test needs. Do not pre-build adjacent layers "for later."
6. Implement the minimum to make the path work end to end. Keep each layer thin enough that the test still explains the whole diff.
7. Re-run the targeted checks and record passing evidence with `record_event(event={"event_kind": "test_result", "passed": true, ...})`.
8. Check the diff boundary and the **Produce-first shape gate** above. If the change now spans multiple independent user paths or grows a second responsibility, split it before commit.
9. Close the slice with `make slice-commit TASK=<task-ref> MSG="..."`, then refresh task context if more slices remain.

**Refactoring cadence & scale cues** (rationale in [engineering-heuristics.md](../../../docs/workbay/rules/engineering-heuristics.md)):

- **Preparatory refactoring first** — make the change easy before making the easy change. Trigger: feature awkward in current structure.
- **Branch by abstraction** — swap a lib/subsystem incrementally behind an abstraction, never big-bang. Trigger: slice replaces a dependency.
- **Parallel change (expand/contract)** — support old+new, migrate callers, drop old. Trigger: rename/restructure a load-bearing field.
- **Smaller batches are reversible** — small steps keep options open. Trigger: sizing a slice.
- **Test at production scale** — index/result-set/N+1 problems surface only on prod-volume data. Trigger: slice touches query/index/collection traversal.
- **Out-of-spec failure harness** — mocks only replay in-spec errors; inject raw network failures (RST, half-open, slow-byte) separately. Trigger: slice integrates a remote dependency.
- **Longevity / leak testing** — leaks and pool/firewall-timeout bugs appear only under sustained multi-day load. Trigger: slice adds long-lived state, pooled connections, or caches.

**Produce-first shape gate** (apply while writing the slice — complements step 8 diff-boundary check; rationale in [engineering-heuristics.md](../../../docs/workbay/rules/engineering-heuristics.md#refactoring-design)):

- **Cohesion / "and"-test** — a slice objective with "and" usually hides two responsibilities; split before coding. See [#refactoring-design](../../../docs/workbay/rules/engineering-heuristics.md#refactoring-design).
- **Size / nesting budget** — prefer ≤~400 lines and ≤3 nesting levels per unit; Extract when exceeded. See [#refactoring-design](../../../docs/workbay/rules/engineering-heuristics.md#refactoring-design).
- **Extract on second responsibility** — if the diff grows a second concern, stop and Extract before expanding. See [#refactoring-design](../../../docs/workbay/rules/engineering-heuristics.md#refactoring-design).
- **Strategy-map / outcome enum** — prefer enum-keyed handler maps over fat switches and outcome enums over deceptive booleans. See [#refactoring-design](../../../docs/workbay/rules/engineering-heuristics.md#refactoring-design).

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I'll finish the backend first and wire the rest in later." | A backend-first pass creates half-wired layers that cannot be verified. Each layer added before the path is closed introduces assumptions that are only caught at integration. | Pick the smallest complete end-to-end path. One endpoint with one test, all the way through. |
| "These are only types or scaffolds, so a test-backed slice can wait." | Scaffolds that land without tests establish interfaces no one has verified. They frequently drift from what the implementation actually needs when the "real" slice arrives. | If the scaffold cannot be tested, it is not yet small enough. Shrink it until one test can describe it. |
| "The slice is almost done; I'll split it after the big diff lands." | Splitting after the fact means the tests for the second path must be written retroactively — they never had a red gate. And "almost done" is always further away than it looks from inside the diff. | Stop adding files. Ship the current path, commit, then start a new slice for the rest. |

## Red Flags

Each flag is a re-entry trigger. Stop and re-enter at the step shown.

| Flag | Re-entry point |
|---|---|
| Diff touches multiple unrelated user paths | Step 3: redefine the slice as one path. Defer the rest to the next slice. |
| `plan_cursor(require_clean_slice=true)` rejected — prior slice has open findings | Step 2: resolve or explicitly defer/wontfix those findings before advancing the cursor. |
| Implementation edits before failing test recorded | Step 4 (tdd skill, Step 2): write the failing test first. |
| Slice ends with half-wired layers that cannot be verified together | Step 6: ship only what the test covers. Leave the next layer for the next slice. |
| Diff growing a second responsibility | Step 8 + [../refactor/SKILL.md](../refactor/SKILL.md): run `refactor` before expanding scope. |

## Recovery

- If the next slice is still fuzzy, shrink it until one test can describe the full path.
- If a previous slice still has open findings, resolve or defer them before advancing the cursor.
- If the diff has already sprawled horizontally, stop adding files, separate the current user path, and defer the rest into the next slice.
- If `make slice-commit` fails after the commit lands, record a valid `slice_complete_*` decision for that commit before moving on.

## Codebase-graph capability (optional)

If a codebase-graph MCP is connected (exposes `get_architecture`, `trace_path`, `detect_changes`), use it to enumerate call sites and trace error paths while sizing a slice. Skip when absent.

## Convergence Criteria

- The current slice maps to one plan item and one behavior path.
- The failing test was recorded before implementation edits.
- Passing test evidence exists for the slice outcome.
- The diff is small enough to review as one vertical slice.
- The slice is committed and reflected in task context before the next slice begins.

## See Also

- [../tdd/SKILL.md](../tdd/SKILL.md)
- [../refactor/SKILL.md](../refactor/SKILL.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
