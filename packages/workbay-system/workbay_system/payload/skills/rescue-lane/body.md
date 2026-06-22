# Rescue Lane

## Overview

Use this skill when a worker lane cannot be taken through the normal `lane-check` and intake path.

## Trigger

Use this skill when a lane has one of these failure modes:

- merge-ready commits exist but lane intake or verification regressed
- the lane branch is broken by a bad cherry-pick or merge conflict
- a lane needs a targeted rescue branch to preserve good work without force-pushing or rewriting the broken lane in place

## Goal

Recover the lane through a bounded rescue branch and a documented MCP trail instead of improvising fixes directly on the broken branch.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary change rules and review-readiness expectations.
- Treat this skill as the rescue execution recipe; project-wide policy stays in the linked canonical docs.

## Core Process

1. Confirm the failing lane state:
   - inspect lane status with `scripts/worktree-lane status`
   - inspect open findings, blockers, and worker reports in MCP
2. Identify the last known-good commit on the lane branch.
3. Create a rescue branch from that known-good point:
   - `codex/rescue-<lane>-<timestamp>`
4. Cherry-pick only the fix commits needed for recovery onto the rescue branch.
5. Diff contract surfaces between the broken lane and the rescue branch:
   - contract docs
   - schema/type surfaces
   - lane-owned code paths
6. Run the lane-local verification pack plus any targeted regression test needed for the rescue.
7. Record the rescue outcome in MCP with a decision entry and any blocker or next-action updates required.
8. Refresh `DASHBOARD.txt` after the decision write so the operator surface reflects the rescue state; render `CURRENT_TASK.json` only for an explicit legacy export request.

## Safety Constraints

- Never force-push the broken lane as part of rescue.
- Always create a new rescue branch instead of mutating the broken branch in place.
- Do not declare the rescue complete until the rescue branch passes the lane-local verification pack.
- Do not skip contract-diff checks when the rescued change touches a boundary.

## Common Rationalizations

- "I can fix the broken lane in place faster." Rescue work is safer on a fresh branch than on a corrupted history.
- "The last good commit is probably obvious." Rescue branches need an explicit known-good base, not a guess.
- "I will skip the contract diff because this is only cleanup." Rescues often cross exactly the boundaries most likely to regress.

## Red Flags

- The rescue is about to rewrite or force-push the original broken lane.
- The known-good base was not verified.
- Verification is failing and the workflow is still trying to declare the rescue complete.

## Recovery

- If cherry-pick conflicts cannot be resolved cleanly, stop and record a blocker instead of guessing.
- If the rescue branch exposes a cross-lane dependency, record that explicitly in the MCP decision or blocker flow before asking for intake.
- If the rescue verification fails, keep the rescue branch as evidence and report the failing test/contract path instead of deleting it.

## Convergence Criteria

- The rescue branch contains only the minimum recovery commits needed for the lane.
- Verification for the rescued slice is recorded and passing, or an explicit blocker documents why rescue could not complete.
- MCP state contains a clear decision trail that explains the rescue branch, verification, and remaining follow-up.

## See Also

- [../worktree-orchestrator/SKILL.md](../worktree-orchestrator/SKILL.md)
- [../worktree-worker/SKILL.md](../worktree-worker/SKILL.md)
- [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md)
