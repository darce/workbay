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
- respecting bounded-turn / timeout / admission discipline on long daemon passes

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary, slice, and review-readiness rules.
- Use [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md) for the canonical lane lifecycle procedure (worker states, scope enforcement, handoff contract, health model). Shared preamble and decision-template inventory live there once — this skill is the worker execution wrapper only.
- Decision templates when your slice changes a contract or creates a cross-lane dependency: `DECISION_CONTRACT_CHANGE`, `DECISION_BREAKING_CHANGE`, `DECISION_CROSS_LANE` under the playbook/templates tree.

## Registered MCP surface

Prefer these tools (declared on this skill) over ad-hoc make targets or scripts:

| Tool | Role |
|---|---|
| `get_lane_activity` | Scope, inbox findings, blockers, messages, and prior reports for this lane |
| `lane_communication` | Read orchestrator briefs/messages; send `worker_to_orchestrator` when blocked or merge-ready |
| `worker_reports` | `operation="record"` structured handback (`merge_ready`, summary, tests, blockers) |
| `review_findings` | Lane-stamped open findings that are part of the actionable inbox |
| `record_event` | Decisions and blockers that must survive handoff |

## Core Process

1. Confirm the lane scope, inbox, and owned paths before touching files (`get_lane_activity` + open `review_findings`).
2. Implement only the delegated slice inside lane ownership boundaries.
3. Run lane-local verification and collect the evidence needed for handback.
4. Return the work through `worker_reports` / `lane_communication` instead of editing shared orchestrator state directly.

## Start-up checklist

1. Read [instructions.md](../../instructions.md), especially the multi-agent worktree section.
2. Load lane scope from MCP before changing code:

```text
get_lane_activity(lane_id=<lane-id>, task_ref=<task-ref>, format="full")
lane_communication(kind="brief", operation="list", lane_id=<lane-id>, task_ref=<task-ref>)
lane_communication(kind="message", operation="list", lane_id=<lane-id>, task_ref=<task-ref>)
review_findings(review={"operation": "list", "status": "open", ...})
```

Treat lane-stamped open review findings, blockers, and pending next actions in lane activity as part of your actionable inbox. The orchestrator may have routed them via lane messages from root.

3. Confirm your changed-file budget matches the lane's owned paths.
4. If lane ownership is unclear or missing, stop and ask the orchestrator to create or update the lane (`lane_communication` / blocker) instead of guessing.

## Implementation rules

- Edit only files inside your lane's owned paths.
- If another domain must change, record a blocker or lane message for the orchestrator.
- Do not update shared plans, checklists, or sibling-lane files unless the brief explicitly says so.
- Use targeted tests for your lane only.

## Bounded-turn and timeout discipline

Long-running daemon / offload-backed worker passes are high wedge and OOM risk. Align with [../offload/SKILL.md](../offload/SKILL.md):

- Honor the pass `timeout_seconds` and `turn_timeout_seconds` (turn bound ≤ pass bound). Size the slice so one turn finishes inside the turn cap; do not start open-ended exploration inside a timed pass.
- On `checkpoint` with `continuation_dispatch_id`, stop cleanly and let the orchestrator re-dispatch — do not invent a second unbounded session.
- On `admission_deferred` / `admission_refused` (internal), do not retry in a tight loop; surface the typed outcome and wait for host recovery or orchestrator guidance.
- Prefer a scoped lane `test_cmd` over whole-package suites so self-verify fits the turn budget.

## Before handoff

1. Check your diff stays in scope:

```bash
git diff --name-only
```

2. Commit on the worktree branch itself.
3. Preferred merge-ready handback:

```text
worker_reports(
  operation="record",
  lane_id=<lane-id>,
  task_ref=<task-ref>,
  session=<session>,
  summary="...",
  changed_files=[...],
  test_commands=[...],
  blockers=[],
  merge_ready=true,
  outcome="handoff_ready"  # or needs_guidance / blocked equivalent
)
lane_communication(
  kind="message",
  operation="record",
  lane_id=<lane-id>,
  task_ref=<task-ref>,
  direction="worker_to_orchestrator",
  subject="merge-ready|blocked",
  message="..."
)
```

When blocked, set `merge_ready=false`, populate `blockers`, and keep the message factual and lane-local. Do not mark the whole task complete.

## Evidence Collection

- At implementation start, confirm the contracts touching your owned paths are loaded. If a required contract is missing, stop and record a blocker naming the missing contract surface.
- When modifying a boundary call, shared type, schema, REST route, or MCP API surface, follow [../../rules/development-workflow.md#cross-boundary-change-protocol](../../rules/development-workflow.md#cross-boundary-change-protocol).
- At handoff, use the appropriate decision template when your slice changes a contract or creates a cross-lane dependency (see Canonical Policy).
- Do not hand off a changed boundary without citing at least one verification command result for that boundary in the lane report or decision entry.

## Common Rationalizations

- "This shared file change is tiny, so I will just include it." Small scope breaks still create orchestrator merge pain.
- "I already know my lane scope." Polling the lane state first is cheaper than fixing ownership drift later.
- "I can hand back without lane-local tests because the orchestrator will catch it." The worker owns first-pass verification.
- "I'll keep going past the turn timeout; I'm almost done." Timed passes checkpoint or kill; overruns burn budget without a durable handback.

## Red Flags

- The diff is crossing lane-owned path boundaries.
- The inbox or lane activity contains unresolved blockers or findings you are about to ignore.
- The worker is about to modify shared plans or sibling-lane files without explicit delegation.
- The pass is approaching the turn or wall-clock bound without a handback plan.

## Safety Constraints

- Do not mark the whole task complete.
- Use lane status progression (orchestrator owns row updates; worker signals via reports):
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
- If the lane branch is corrupt or a bad cherry-pick cannot be unwound, stop and escalate to [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md) rather than force-pushing.
- If the host admits pressure mid-pass, surface `admission_deferred` / checkpoint state; do not thrash retries.

## Convergence Criteria

- All changed files stay inside the lane’s owned paths.
- Lane-local verification has been run and the results are ready to cite in the handoff.
- The lane is handed back through `worker_reports` / `lane_communication` with a clear merge-ready or blocked status.
- Turn/timeout and admission constraints were respected on daemon-backed passes.

## See Also

- [../worktree-orchestrator/SKILL.md](../worktree-orchestrator/SKILL.md)
- [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md)
- [../offload/SKILL.md](../offload/SKILL.md)
- [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md)
