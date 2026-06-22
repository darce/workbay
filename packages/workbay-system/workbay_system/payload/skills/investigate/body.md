# Investigate

## Overview

Use this skill for systematic root-cause debugging when a test failure, runtime error, or unexpected behavior needs diagnosis before a fix is attempted. This is an execution skill: the investigation is not complete until the root cause and outcome are recorded durably.

## Trigger

Use this skill when the request matches any of:

- "investigate", "debug", "diagnose", or "root cause"
- "why is this failing", "what's causing", or "trace this"
- a presented test failure or error whose cause is not immediately obvious
- a review finding that has reopened two or more times

Do not use this skill for:

- known bugs with obvious fixes
- review or audit work that belongs to the `review` skill

## Goal

Identify and verify the root cause of a defect before applying a fix, then preserve the investigation trail in MCP so the diagnosis survives handoff.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for slice checklist and regression sweep conventions.
- Use [../../rules/branch-review-guide.md](../../rules/branch-review-guide.md) for bug-finding heuristics such as variable identity, cross-method contracts, boundary values, retry lifecycle, and stale-file verification.
- Use this skill for investigation phasing and MCP recording discipline only; broader process policy lives in the linked docs.

## Core Process

0. **Ensure task scope before any cwd-resolving MCP read.** `search_handoff`, `review_findings`, `get_handoff_state` without `task_ref` all resolve from cwd. On cold start:
   - Run `make status LIFECYCLE_ARGS=--json` from the target worktree first so the initial orientation path stays on the public facade.
   - If more than one active task may explain the symptom, run `make tasks LIFECYCLE_ARGS=--json` before any raw MCP read.
   - **Resumption:** work from the existing task's `target_worktree_path`; pass `task_ref` explicitly.
   - **Ad-hoc investigation on main:** register a maintenance task first — `set_handoff_state(task_ref="MAINT-<slug>-<YYYYMMDD>", objective="...", status="in_progress", target_branch="main")`.
   - **`Ambiguous active task` error:** run `make maint-archive-stale` (or `MAINT_ARCHIVE_ARGS="--yes"`), then re-run `make context`. `make context` exits `2` on this ambiguity.

1. Collect symptoms before forming hypotheses:
   - exact error output
   - reproduction steps
   - recency checks with recent git history
   - related MCP findings via `search_handoff(...)` and `review_findings(review={"operation":"list", ...})`
   - active task context via `get_handoff_state(read_profile="hot_summary")`
2. Record the investigation opening as a finding with `review_findings(review={"operation":"record", ...})`.
3. Analyze the symptom against common defect patterns:
   - race condition
   - stale state
   - contract mismatch
   - boundary value
   - integration seam
   - config drift
   - regression
4. Form one specific, testable hypothesis at a time and inspect the suspected boundary.
5. After each hypothesis, decide whether the evidence confirms or refutes it.
6. If three hypotheses fail in sequence:
   - record the escalation with `record_event(event={"event_kind":"decision", ...})`
   - record a blocker with `record_event(event={"event_kind":"blocker", ...})`
   - stop rather than guessing
7. Only after confirming the root cause, implement the smallest fix, add a regression test, and run the relevant suite.
8. Close or update the investigation finding with `review_findings(review={"operation":"update", ...})`.
9. Record the investigation outcome with `record_event(event={"event_kind":"decision", ...})`.
10. Refresh the operator view with `render_handoff(kind='dashboard')` if needed; render `CURRENT_TASK.json` only for an explicit legacy export request.

### Useful commands

```bash
git log --oneline -20
git diff --stat HEAD~5..HEAD
```

Run the smallest relevant test command for the affected surface before widening the scope.

## Common Rationalizations

- "I already know the fix, so I don't need to prove the cause."
- "A quick workaround is fine for now."
- "Three failed hypotheses just mean I should keep poking around."
- "I can skip the MCP trail because this is just debugging."

## Red Flags

- A proposed fix does not explain how the symptom was produced.
- The investigation is touching more than five files for what should be a localized defect.
- Cascading failures suggest the wrong architectural layer is being debugged.
- The same issue has been reopened multiple times, but prior findings are not being consulted.
- A fix is about to land before the root cause is recorded.

## Recovery

- If MCP is unavailable, capture investigation notes locally and transfer them to MCP as soon as access returns. Do not pretend the investigation is complete without that transfer.
- If the symptom cannot be reproduced, record that fact and close the finding as `wontfix` with the reproduction attempts.
- If the fix requires changes outside the current lane or owned paths, record a cross-lane blocker and escalate instead of crossing boundaries silently.
- If a prior investigation finding exists for the same symptom, reopen it with `review_findings(review={"operation":"update", ...})` rather than creating a duplicate.

## Convergence Criteria

- The root cause is identified and recorded before any fix is treated as complete.
- The resulting fix is minimal and paired with a regression test when a fix is applied.
- The investigation finding is updated with outcome and verification evidence.
- The investigation outcome is recorded with `record_event(event={"event_kind":"decision", ...})`.
- `render_handoff(kind='dashboard')` has been run after state-changing writes that did not already refresh the operator view.

## See Also

- [review](../review/SKILL.md)
- [branch-review-guide.md](../../rules/branch-review-guide.md)
- [development-workflow.md](../../rules/development-workflow.md)
