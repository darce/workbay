# TDD

## Overview

Own the RED -> GREEN -> REFACTOR loop for each affected behavior. [incremental-implementation](../incremental-implementation/body.md) owns slices and scheduling; [handoff-lifecycle](../handoff-lifecycle/body.md) owns state and evidence durability. Backend/model configuration remains with the live registry and resolved AgentSpec.

Remote preference: on a `remote_only` install (`--with-remote`), delegation-eligible mechanical slices go through `/wb-offload` with an **explicit** remote backend that its preflight probe reports available; `grok-remote` is discontinued and never a default for dispatch; a `remote_required` refusal (or a failed remote probe) is surfaced as a recorded blocker, never silently downgraded to a local backend ([offload-remote-playbook](../../docs/workbay/rules/offload-remote-playbook.md)).

## Trigger

Use for new or changed production behavior and when `make slice-start` is the required lifecycle step. Planning, review, and handoff bookkeeping have separate owners. A documentation protocol correction can be verified with existing materialization/contracts without inventing a failing prose-mirroring test.

## Canonical Policy

- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)

## Prerequisites and Test Selection

Start from a bounded behavior contract, owned paths, aligned task/worktree, and a runnable relevant test command. Inspect existing producers, contracts, and representative inputs before choosing mocks. Preserve optional-dependency failure/degrade behavior; a mock must not hide an import or provider failure the real path exposes.

Choose the cheapest check that distinguishes the accepted behavior from the defect. Reuse an existing regression or reproducible test sequence when it already fails for the intended reason; do not require a new test file or a separate test-writing dispatch. Broaden to adjacent contracts when the footprint warrants it, and stop after required checks pass unless new changes or unresolved risk justify more runs. A reversible low-impact edit need not acquire a test that merely copies its implementation.

## Core Process

1. Identify the behavior and prediction. Use `search_handoff` for known tokens/IDs; for paraphrase prior art do not invent FTS synonyms — call `find_related_prior_work` and apply handoff-lifecycle's semantic response check, then verify the candidate source.
2. Before production edits for this behavior, write/update a targeted check or reuse existing reproducible failing evidence. Run it against the relevant baseline and confirm the predicted failure, not syntax, import, or environment noise: [DBG-01] **Reproduce before changing** (Debugging: The 9 Indispensable Rules).
3. Record the command, baseline commit, observed failure, and affected behavior. Run `make slice-start TASK=<task-ref> TEST_CMD="<test command>"` where required; it records the failing test result. This dependency is local to the behavior/slice: independent groups may run RED concurrently and implement as soon as their own RED evidence is accepted. One lane may own both phases; a second dispatch or global serial chain is not required.
4. Make the smallest production change that satisfies the check, within owned paths. Do not implement unrelated behavior under this slice's RED evidence.
5. Re-run the targeted check and justified adjacent contracts at the exact tested HEAD. Capture stdout/stderr AND the actual executed test process exit status; retain the status immediately before later shell commands or wrappers overwrite it (save `$?` for a direct shell command; for a pipeline/runner retain the test process's own status). Never fabricate/default zero or infer exit status from log text. Record actual outcomes with `record_event(event={"event_kind": "test_result", "session": "<session>", "task_ref": "<task-ref>", "command": "<executed test command>", "passed": <observed outcome bool>, "exit_code": <observed process exit code>, "result": "<observed stdout/stderr evidence>", "actor": {"commit_sha": "<exact tested HEAD>", "lane_id": "<lane>"}})`, retaining exact commit provenance through handoff-lifecycle. Replace placeholders with actual values; claim durability only after a successful write receipt. GREEN requires BOTH the observed passing outcome and `exit_code==0`; missing status, nonzero with `passed=true`, or zero with a failing outcome cannot establish GREEN or closure.
6. Refactor while the checks stay green. A new behavior change needs its own RED evidence. Leave the bounded slice ready for `make slice-commit`; any commit/squash/amend/rebase that changes HEAD requires a rerun and fresh step 5 evidence with the actual rerun exit code and exact new tested HEAD before closure. Confirm HEAD still matches that evidence.

**TDD discipline cues** (follow the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md); cite a row only when its trigger fires in the slice at hand):

- **Test-first = experiment** — predict the exact failure, run red to confirm the test exercises something, then green. Trigger: starting new behavior.
  - When the slice starts from a debugging exchange that has listed the facts and proposes a fix without a prediction and an experiment to test it, write the prediction (the exact failure the red run must show) before the first run: [DIAG-03] **Falsifiable hypothesis** (Why Programs Fail, ch. 6).
  - When evaluating a passing test's ability to catch its claimed regression, introduce that bug as a temporary, uncommitted mutation in the working tree, confirm the test fails, and revert the mutation afterwards: [TEST-15] **Prove the green can go red** (The Pragmatic Programmer, ch. 9).
- **Test-after ≠ TDD** — tests written post-code breed implementation coupling (red-flag). workbay practice (no canon row).
- **Hard-to-write test = redesign signal** — change the design, don't fight the test. Fires when a test is hard to write, needs threads, or needs a real DB/file/network: [TEST-09] **Hard test = design defect** (Modern Software Engineering, ch. 14).
- **Only enough code to pass** — over-writing past the spec reduces cohesion. workbay practice (no canon row).

## Common Rationalizations

| Rationalization | Required action |
|---|---|
| "I'll write tests after production code." | Establish RED evidence for the affected behavior first; a later passing check does not prove RED existed. |
| "Config and glue do not need verification." | Select the relevant behavior/contract check; changing their behavior still needs the applicable gate. |
| "We need a new test even though the regression already fails." | Reuse the observed regression and preserve its command, baseline, and result. |

## Red Flags and Recovery

- Unrelated syntax/import/environment noise: fix the prerequisite or report the blocker; do not count it as behavioral RED.
- A check passes on first run: inspect its scope. Existing passing contract checks can verify a documentation correction; changed production behavior still needs reproducible RED evidence.
- `make slice-start` or shared MCP is unavailable: follow handoff-lifecycle's durable-report fallback, report the unavailable gate, and never fabricate a receipt. Continue only within existing authorization and required gate policy.
- The diff grows beyond the behavior covered by RED: split a new slice before adding that production change.

## Convergence Criteria

- Reproducible RED evidence precedes production edits for each affected behavior, or an applicable documented contract-verification path is used.
- GREEN/closure evidence identifies the actual command, stdout/stderr, retained observed process exit code and exact tested HEAD; BOTH the observed passing outcome and exit zero are required. Rerun and record fresh evidence after HEAD changes, including squash/amend/rebase.
- The diff is bounded and ready for slice completion or an explicit follow-on slice.

## See Also

- [../incremental-implementation/body.md](../incremental-implementation/body.md)
- [../handoff-lifecycle/body.md](../handoff-lifecycle/body.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
