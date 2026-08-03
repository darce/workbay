# Worktree Worker

## Overview

Use this skill when you are the worker agent assigned to a bounded worktree lane.

## Trigger

Use this skill when you are implementing a delegated lane slice with a defined owned-path boundary and you need to stay inside that scope until the work is ready for orchestrator review.

## Goal

- reading lane scope from shared MCP state
- staying inside owned paths
- implementing only the delegated slice
- running lane-local verification
- handing the slice back cleanly for orchestrator review

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary, slice, and review-readiness rules.
- Use [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md) for the canonical lane lifecycle procedure (worker states, scope enforcement, handoff contract, health model).
- This skill is an execution wrapper for this specific runtime. Shared process policy remains in the linked canonical docs above.

## Core Process

1. Confirm the lane scope, inbox, and owned paths before touching files.
2. Implement only the delegated slice inside lane ownership boundaries.
3. Run lane-local verification and collect the evidence needed for handback.
4. Return the work through the lane handoff path instead of editing shared orchestrator state directly.

## Start-up checklist

1. Read [instructions.md](../../instructions.md), especially the multi-agent worktree section.
2. Poll the lane inbox before changing code:

```bash
make lane-inbox
```

If you are operating outside the Makefile wrapper, query the shared lane state directly:

```bash
scripts/worktree-lane status \
  --orchestrator-root /abs/path/to/orchestrator \
  --task-ref <task-ref> \
  --lane-id <lane-id> \
  --worktree-path /abs/path/to/current-worktree
```

3. Confirm your changed-file budget matches the lane's owned paths.
4. If lane ownership is unclear or missing, stop and ask the orchestrator to create or update the lane instead of guessing.

Treat lane-stamped open review findings, blockers, and pending next actions shown in lane activity as part of your actionable inbox. The orchestrator may have routed them from root with `make handoff-dispatch`.

## Implementation rules

- Edit only files inside your lane's owned paths.
- If another domain must change, record a blocker or lane message for the orchestrator.
- Do not update shared plans, checklists, or sibling-lane files unless the brief explicitly says so.
- Use targeted tests for your lane only.

## Before handoff

1. Check your diff stays in scope:

```bash
git diff --name-only
```

2. Commit on the worktree branch itself.
3. Preferred merge-ready handback:

```bash
make lane-handoff
```

This verifies lane scope, commits lane-owned changes if needed, records the lane report in shared MCP state, and opens the worker-to-orchestrator handoff message automatically.

If you are using the non-interactive automation path, prefer:

```bash
make lane-run
```

That command renders the worker prompt from MCP state, runs `codex exec`, and auto-submits the final structured handoff for you.

Keep the low-level helper only as a fallback when you need direct control:

```bash
scripts/worktree-lane report \
  --orchestrator-root /abs/path/to/orchestrator \
  --task-ref <task-ref> \
  --lane-id <lane-id> \
  --session <session-name> \
  --summary "Slice implemented and ready for orchestrator review." \
  --test-command "cd ... && pytest ..." \
  --merge-ready
```

Merge-ready and blocked reports auto-open a worker-to-orchestrator handoff message even if you do not pass `--message`. Use that path whenever the lane is done or needs more guidance from root.

## Evidence Collection

- At implementation start, confirm the contracts touching your owned paths are loaded. If a required contract is missing, stop and record a blocker naming the missing contract surface.
- When modifying a boundary call, shared type, schema, REST route, or MCP API surface, follow [../../rules/development-workflow.md#cross-boundary-change-protocol](../../rules/development-workflow.md#cross-boundary-change-protocol).
- At handoff, use the appropriate decision template when your slice changes a contract or creates a cross-lane dependency:
  - [../../templates/DECISION_CONTRACT_CHANGE.template.md](../../templates/DECISION_CONTRACT_CHANGE.template.md)
  - [../../templates/DECISION_BREAKING_CHANGE.template.md](../../templates/DECISION_BREAKING_CHANGE.template.md)
  - [../../templates/DECISION_CROSS_LANE.template.md](../../templates/DECISION_CROSS_LANE.template.md)
- Do not hand off a changed boundary without citing at least one verification command result for that boundary in the lane report or decision entry.

## Common Rationalizations

- "This shared file change is tiny, so I will just include it." Small scope breaks still create orchestrator merge pain.
- "I already know my lane scope." Polling the lane state first is cheaper than fixing ownership drift later.
- "I can hand back without lane-local tests because the orchestrator will catch it." The worker owns first-pass verification.

## Red Flags

- The diff is crossing lane-owned path boundaries.
- The inbox or lane activity contains unresolved blockers or findings you are about to ignore.
- The worker is about to modify shared plans or sibling-lane files without explicit delegation.

## Safety Constraints

- Do not mark the whole task complete.
- Use lane status progression:
  - `active` while coding
  - `review` when the slice is committed and ready
  - `merged` only after the orchestrator has taken it
  - `closed` when the lane is fully done
- Keep blockers factual and lane-local.

## Recovery

- If lane ownership is missing or ambiguous, stop and ask the orchestrator to repair the lane definition before editing.
- If your fix requires another domain’s files, record a blocker or lane message instead of crossing the boundary.
- If the slice changes a contract surface, use the cross-boundary protocol and the appropriate decision template before handoff.
- If lane-local tests fail unexpectedly, keep the failure scoped and report the concrete command plus result instead of summarizing loosely.

## Convergence Criteria

- All changed files stay inside the lane’s owned paths.
- Lane-local verification has been run and the results are ready to cite in the handoff.
- The lane is handed back through the supported report/handoff path with a clear merge-ready or blocked status.

## See Also

- [../worktree-orchestrator/SKILL.md](../worktree-orchestrator/SKILL.md)
- [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md)
- [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md)
