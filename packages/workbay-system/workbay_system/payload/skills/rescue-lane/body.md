# Rescue Lane

## Overview

Use this skill when a worker lane cannot be taken through the normal intake path and a targeted rescue branch is required to preserve good work.

## Trigger

Use this skill when a lane has one of these failure modes:

- merge-ready commits exist but lane intake or verification regressed
- the lane branch is broken by a bad cherry-pick or merge conflict
- a lane needs a targeted rescue branch to preserve good work without force-pushing or rewriting the broken lane in place
- repeated `self_verify_failed` outcomes or a corrupt worktree leave no clean re-dispatch path

Do **not** use this skill when a plain re-dispatch would resume correctly (see Decision rule below).

## Goal

Recover the lane through a bounded rescue branch and a documented MCP trail instead of improvising fixes directly on the broken branch.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary change rules and review-readiness expectations.
- Treat this skill as the rescue execution recipe; project-wide policy stays in the linked canonical docs.
- Use [reasoning-discipline](https://github.com/darce/heuristics-canon) for escalation tiers when recovery stalls (Tier 4: stop-and-record rather than guess).

## Decision rule: rescue vs re-dispatch

Prefer the lighter path first. Cross-check [../offload/SKILL.md](../offload/SKILL.md) before opening a rescue branch:

| Situation | Action |
|---|---|
| Pass outcome is `checkpoint` with a `continuation_dispatch_id` | Re-dispatch with that same `dispatch_id` (idempotent resume). Do not rescue. |
| Outcome is `admission_deferred` / `admission_refused` | Wait for host pressure to drop (or operator `admission_override` for false positives only), then re-dispatch. Do not rescue. |
| Outcome is `self_verify_failed` once, tree still coherent | Fix the failing TEST_CMD evidence and re-dispatch; rescue only after repeated self-verify failures leave the branch unusable. |
| Bad cherry-pick / merge conflict / corrupt worktree / history rewrite risk | **Rescue**: new branch from last known-good commit, cherry-pick only clean fix commits. |
| Merge-ready commits exist but intake/verification regressed without history corruption | Prefer MCP handback repair (`worker_reports` / `lane_communication`) over rescue; rescue only if the branch itself is damaged. |

Hand-cherry-picking commits that a preserved `dispatch_id` would resume correctly is an anti-pattern — it forks history the offload pass already owns.

## Core Process

1. Confirm the failing lane state via registered MCP tools (not ad-hoc CLI):
   - `get_lane_activity(lane_id=..., task_ref=..., format="full")` for decisions, tests, blockers, findings, reports, and messages
   - `worker_reports(operation="list", lane_id=..., task_ref=...)` for recent handbacks
   - `lane_communication(kind="message", operation="list", lane_id=..., task_ref=...)` for open orchestrator/worker messages
2. Apply the **Decision rule** above. If re-dispatch applies, stop and hand back to offload/orchestrator with the typed outcome — do not open a rescue branch.
3. Identify the last known-good commit on the lane branch.
4. Create a rescue branch from that known-good point:
   - `codex/rescue-<lane>-<timestamp>` (or the host's equivalent naming convention)
5. Cherry-pick only the fix commits needed for recovery onto the rescue branch.
6. Diff contract surfaces between the broken lane and the rescue branch:
   - contract docs
   - schema/type surfaces
   - lane-owned code paths
7. Run the lane-local verification pack plus any targeted regression test needed for the rescue.
8. Record the rescue outcome in MCP:
   - `record_event(event={"event_kind": "decision", ...})` with the rescue branch name, known-good base, verification command, and remaining follow-up
   - open a blocker via `record_event(event={"event_kind": "blocker", ...})` when rescue cannot complete cleanly
   - notify the orchestrator with `lane_communication(kind="message", operation="record", direction="worker_to_orchestrator", ...)` when intake should resume on the rescue branch
9. Refresh the operator view per [../handoff-lifecycle/SKILL.md](../handoff-lifecycle/SKILL.md) step 5 after state-changing writes.

## Safety Constraints

- Never force-push the broken lane as part of rescue.
- Always create a new rescue branch instead of mutating the broken branch in place.
- Do not declare the rescue complete until the rescue branch passes the lane-local verification pack.
- Do not skip contract-diff checks when the rescued change touches a boundary.
- Do not hand-cherry-pick a commit that re-dispatch with a preserved `dispatch_id` would resume correctly.

## Common Rationalizations

- "I can fix the broken lane in place faster." Rescue work is safer on a fresh branch than on a corrupted history.
- "The last good commit is probably obvious." Rescue branches need an explicit known-good base, not a guess.
- "I will skip the contract diff because this is only cleanup." Rescues often cross exactly the boundaries most likely to regress.
- "I'll just cherry-pick what the worker almost finished." If the outcome was `checkpoint`, re-dispatch with `continuation_dispatch_id` instead.

## Red Flags

- The rescue is about to rewrite or force-push the original broken lane.
- The known-good base was not verified.
- Verification is failing and the workflow is still trying to declare the rescue complete.
- A resumable offload outcome is being treated as a rescue candidate.

## Recovery

- If cherry-pick conflicts cannot be resolved cleanly, stop and record a blocker instead of guessing ([AGT-06] / reasoning-discipline Tier 4: escalate and record, do not invent a recovery path).
- If the rescue branch exposes a cross-lane dependency, record that explicitly in the MCP decision or blocker flow before asking for intake.
- If the rescue verification fails, keep the rescue branch as evidence and report the failing test/contract path instead of deleting it.
- If three recovery attempts fail in sequence, stop, record the blocker with the attempts tried, and return control to the orchestrator rather than widening the rescue scope.

## Convergence Criteria

- The rescue path was chosen only after the Decision rule ruled out re-dispatch.
- The rescue branch contains only the minimum recovery commits needed for the lane.
- Verification for the rescued slice is recorded and passing, or an explicit blocker documents why rescue could not complete.
- MCP state contains a clear decision trail that explains the rescue branch, verification, and remaining follow-up.

## See Also

- [../offload/SKILL.md](../offload/SKILL.md)
- [../worktree-orchestrator/SKILL.md](../worktree-orchestrator/SKILL.md)
- [../worktree-worker/SKILL.md](../worktree-worker/SKILL.md)
