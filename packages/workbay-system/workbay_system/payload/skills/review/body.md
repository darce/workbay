# Review

## Overview

Use this skill when the user asks to review code, a branch, a PR diff, a task plan, an epic, a roadmap, or an ADR. This is an execution skill: findings must be recorded in MCP before they are presented, and the review is not complete until a verdict plus review-run record exist.

## Trigger

Use this skill when the request matches any of:

- "review" + implementation, code, changes, branch, PR, or diff
- "audit" + branch or code
- "propose improvements" or "flag gaps/bugs"
- "review" + task plan, epic, roadmap, ADR, or planning document
- "audit" + plan, epic, or roadmap

If the target is source under `apps/`, `packages/`, `scripts/`, or `mk/`, treat it as a branch review.

If the target is under `docs/tasks/`, `docs/epics/`, `docs/roadmaps/`, or `docs/workbay/contracts/`, treat it as a planning review.

If the diff contains both code and planning docs, run separate passes for each review mode.

## Goal

Produce a structured, MCP-recorded set of review findings with a durable verdict and review-run record, using the correct review guide for the target under review.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for slice and review-readiness rules.
- Use [../../rules/branch-review-guide.md](../../rules/branch-review-guide.md) for branch reviews.
- Use [../../rules/planning-review-guide.md](../../rules/planning-review-guide.md) for planning reviews.
- Use `make review-dispatch` as the current Makefile entry point when invoking the review runner through the repo workflow.
- Use this skill for review-mode detection, execution order, and MCP recording discipline. The detailed review checklists live in the linked guides.

## Core Process

1. Determine the review mode from the request and target paths. Do not ask the user if the mode is inferable from context.
2. Load the active task and prior review state:
   - `get_handoff_state(read_profile="review_packet")`
   - `review_runs(review={"operation":"list", ...})`
   - `review_findings(review={"operation":"list", ...})`
3. Load the correct guide and scope:
   - Branch review: inspect the diff and review packet.
   - Planning review: inspect the named planning artifact plus prerequisite contracts or specs.
4. Execute the full checklist from the selected guide. Do not skip sections because the diff looks small.
5. Record every finding before mentioning it in chat:
   - Single finding: `review_findings(review={"operation":"record", ...})`
   - Three or more findings in one pass: `review_findings(review={"operation":"batch_record", ...})`
6. Determine the verdict:
   - `pass`
   - `pass_with_findings`
   - `conditional_pass`
   - `fail`
7. Record the verdict decision with `record_event(event={"event_kind":"decision", ...})`.
8. Record the review run with `review_runs(review={"operation":"record", ...})`. If `review_runs` is unavailable in the current harness, use `make handoff-review-run TASK_REF=<task-ref> MODE=<branch|planning|release_audit> SUBJECT=<path-or-.> SUBJECT_KIND=<task_plan|epic|branch|adr|roadmap|other> VERDICT=<verdict> DECISION=<decision-id> SESSION=<session> RUN_ID=<run-id>`.
9. Regenerate the operator view with `render_handoff(kind='dashboard')`.
10. Respond with findings grouped by severity, then the verdict summary and an MCP write receipt. Use this shape: `MCP writes: <n> findings batch-recorded; verdict decision id <id> (<decision_key>); review_run id <id>. DASHBOARD.txt refreshed. Handoff updated: decision <decision_key> recorded.`

### Branch review scope commands

```bash
git diff --name-only <base>...HEAD
git diff --stat <base>...HEAD
git log --oneline <base>...HEAD
```

### Branch review priorities

1. Correctness and type safety
2. Architecture violations and regression guards
3. Dead code and complexity
4. Test coverage gaps
5. Contract and schema parity

### Planning review priorities

1. Obsolete assumptions
2. Architecture and ownership mistakes
3. Greenfield-policy violations
4. Contradictory scope or checklist logic
5. Contract gaps
6. Unnecessary complexity

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I can mention the issue now and record it later." | Findings without a stable `finding_id` cannot be tracked, deferred, or verified by the pre-merge gate. The gate audits MCP state, not chat history. | Record the finding before mentioning it. If MCP is unavailable, record a blocker instead of continuing. |
| "This looks minor, so I don't need the full checklist." | Severity cannot be assessed by inspection before the checklist runs. Prior reviews have found contract drift, missing test coverage, and architecture violations on "minor" diffs. | Run the full checklist. Mark findings LOW where appropriate, but still record them. |
| "The user only asked for a quick pass, so MCP bookkeeping can wait." | "Quick" describes the desired pace, not permission to skip recording discipline. An unrecorded pass leaves no evidence at the pre-merge gate. | Full checklist, expedited where straightforward. Recording takes two tool calls; omitting it voids the review. |
| "A planning document doesn't need the same rigor as a code review." | Planning docs drive architecture decisions. A missed assumption in a task plan becomes a regression in code. The planning-review checklist exists for exactly this reason. | Use the planning-review guide. Record findings with the same MCP discipline as branch reviews. |

## Red Flags

- MCP is unavailable, but findings are about to be reported anyway.
- The review mixes code and planning artifacts without separate modes.
- Open findings from a prior pass are being duplicated instead of reconciled.
- The diff or document scope is unclear, but review has already started.
- A verdict is about to be reported without a recorded decision and review run.

## Recovery

- If MCP is unavailable, stop the review and record a blocker instead of producing untracked findings.
- If the diff is empty or the planning document is missing, report the scope issue and exit without findings.
- If open findings from a prior review run still apply, acknowledge them and avoid duplication. Reopen with `review_findings(review={"operation":"update", ...})` if a prior finding recurs.
- If the review target crosses audit triggers such as architecture transitions, multi-service state, persistence changes, or broad UI state surfaces, escalate to the multi-lens audit workflow from the branch review guide.

## Convergence Criteria

- Every finding mentioned to the user is already recorded in MCP with a stable ID.
- A verdict decision exists via `record_event(event={"event_kind":"decision", ...})`.
- A review-run record exists via `review_runs(review={"operation":"record", ...})`.
- `render_handoff(kind='dashboard')` has been run after the state-changing writes.
- The final response includes the verdict and an MCP write receipt with finding count, verdict decision row id, review-run row id, and dashboard refresh status.

### Verification Evidence

Do not report a verdict until each claim below is backed by a fresh command run with quoted output. Stale results and assumed states do not count.

| Claim | Command | Required output shape |
|---|---|---|
| Diff scope is understood | `git diff --stat <base>...HEAD` | File list with actual line counts quoted in response |
| Tests pass at HEAD | `<stack test command>` | `N passed` with N > 0, exit 0 — quote the count |
| No open findings on task | `review_findings(operation="list", status="open")` | `items: []` or explicit list of open items to address |
| Verdict recorded | `review_runs(operation="list", task_ref=...)` | Entry with matching `review_run_id` and `verdict` |

## See Also

- [branch-review-guide.md](../../rules/branch-review-guide.md)
- [planning-review-guide.md](../../rules/planning-review-guide.md)
- [development-workflow.md](../../rules/development-workflow.md)
