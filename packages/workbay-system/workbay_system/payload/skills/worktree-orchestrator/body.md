# Worktree Orchestrator

## Overview

Use this skill when one main agent needs to coordinate worker agents in sibling Git worktrees.

## Trigger

Use this skill when a task should be split across stable seams such as owned paths, contract boundaries, or test packs, and one orchestrator agent needs to coordinate multiple worker lanes without losing shared task truth.

## Goal

- deciding whether a task should be split into lanes
- creating worker worktrees on `codex/*` branches
- registering/updating shared MCP lanes
- rendering bounded worker briefs
- deciding merge order and integration checks
- keeping shared checklist and task truth centralized

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary, slice, and review-readiness rules.
- Use [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md) for the canonical lane lifecycle procedure (task manifests, make commands, scope enforcement, health model, recipes).
- This skill is an execution wrapper for this specific runtime. Shared process policy remains in the linked canonical docs above.

## Preflight

1. Read [instructions.md](../../instructions.md), especially the multi-agent worktree section.
2. Confirm the active handoff task already exists in shared MCP state.
3. Split work only along stable seams:
   - path ownership
   - API contract ownership
   - test ownership
4. Keep shared plans, checklists, and cross-lane conclusions in the orchestrator lane unless explicitly delegated.

## Lane Split Guidance

Do not rely on packaged default lanes. Derive lane IDs, owned paths,
required docs, and verification commands from the local task plan,
lane manifest, or repository overlay. Common portable splits include:

- `api`: service API routes, schemas, and contract tests
- `domain`: domain models, persistence, and migration-owned tests
- `ui`: frontend components, state hooks, and browser/unit tests
- `docs`: operator docs, task plans, and generated instruction surfaces

## Core Process

### 1. Create the lane

Use the helper from the orchestrator root:

```bash
scripts/worktree-lane create \
  --orchestrator-root /abs/path/to/orchestrator \
  --lane-id api \
  --branch codex/<task>-api \
  --title "API" \
  --objective "Implement the API schema slice."
```

This creates the sibling worktree and registers the lane in shared MCP state.

### 2. Render the worker brief

Use the brief template renderer:

```bash
scripts/worktree-lane brief \
  --orchestrator-root /abs/path/to/orchestrator \
  --task-ref <task-ref> \
  --lane-id api \
  --branch codex/<task>-api \
  --worktree-path /abs/path/to/worktree \
  --objective "Implement the API schema slice." \
  --owned-path services/api/** \
  --required-doc docs/workbay/instructions.md \
  --required-doc docs/workbay/contracts/workbay-handoff-mcp.md \
  --test-command "cd services/api && pytest tests/api/test_contract.py" \
  --definition "Ready for orchestrator branch review with targeted tests passing."
```

Paste that brief into the worker session.

### 3. Dispatch and monitor the lane

Use shared MCP state, not chat memory:

```bash
make lane-dispatch TASK=<task-ref> LANE=api MESSAGE="Implement the API slice and verify the lane-local checks."
make handoff-inbox TASK=<task-ref>
```

Then monitor the lane with:

```bash
scripts/worktree-lane status \
  --orchestrator-root /abs/path/to/orchestrator \
  --task-ref <task-ref> \
  --lane-id api \
  --worktree-path /abs/path/to/worktree
```

If the worker gets blocked on another domain, keep the lane in scope and reassign the blocker instead of letting the worker edit outside lane ownership. The root-side `make handoff-inbox` poller is where those worker handoff/guidance messages surface.

When the orchestrator records review findings, blockers, or next actions in MCP from root, fan them back out with:

```bash
make handoff-dispatch TASK=<task-ref>
```

That stamps routeable unassigned open handoff items onto the correct lane and creates or reuses lane messages so workers pick them up in `make lane-inbox`.

Workers should also emit `worker_to_orchestrator` messages when they are merge-ready or blocked. Poll those from root with:

```bash
make handoff-inbox TASK=<task-ref>
```

### 4. Intake the lane

Workers should finish by setting the lane to `review`, submitting a merge-ready lane report, and optionally sending a lane message.

Review the worker branch before merge. Prefer:

```bash
git cherry-pick <worker-commit-sha>
```

Use selective file intake only when intentionally trimming scope:

```bash
git checkout codex/<task>-<lane> -- path/to/file
```

## Handoff Evidence Checklist

- Before dispatching work to a lane, confirm the worker's contract surface exists and is current. Cite the contract path in the lane brief or dispatch message.
- Before accepting a lane handoff, verify the worker summary names changed contracts, test counts, and schema or runtime implications.
- When routing findings or blockers to a lane, include the contract path and at least one verification command so the worker does not have to rediscover the boundary from scratch.
- When a slice changes a boundary or creates a downstream dependency, require the worker to use one of:
  - [../../templates/DECISION_CONTRACT_CHANGE.template.md](../../templates/DECISION_CONTRACT_CHANGE.template.md)
  - [../../templates/DECISION_BREAKING_CHANGE.template.md](../../templates/DECISION_BREAKING_CHANGE.template.md)
  - [../../templates/DECISION_CROSS_LANE.template.md](../../templates/DECISION_CROSS_LANE.template.md)
- Keep policy details in the canonical sources: [../../instructions.md](../../instructions.md) for startup and loading rules, and [../../rules/development-workflow.md#cross-boundary-change-protocol](../../rules/development-workflow.md#cross-boundary-change-protocol) for boundary validation.

## Common Rationalizations

- "I can split the task later if it gets messy." Late lane creation usually means ownership is already blurred.
- "The worker can touch shared plans just this once." Shared planning state should stay centralized unless explicitly delegated.
- "Merge order does not matter if each lane passes tests." Cross-lane integration still needs an orchestrated order and intake check.

## Red Flags

- Owned-path boundaries are unclear or overlapping.
- The orchestrator is about to delegate work without a lane brief or test command.
- Shared docs or contracts are drifting into worker-owned changes without an explicit delegation.

## Safety Constraints

- Do not assign two workers to the same owned path set.
- Do not let workers rewrite shared plan truth unless explicitly assigned.
- Reject out-of-scope files during intake.
- Keep the overall task `in_progress` while individual lanes move to `review`, `merged`, or `closed`.
- The orchestrator owns final MCP task updates and dashboard generation; `CURRENT_TASK.json` is only an on-demand compatibility export.

## Recovery

- If lane ownership is ambiguous, stop and split the work again before dispatching.
- If a worker reports out-of-scope changes, reject intake and reroute the work rather than silently absorbing it.
- If a lane gets blocked on another domain, record the blocker and dispatch the dependent work instead of letting the worker cross boundaries.
- If orchestration state drifts from the shared MCP state, regenerate the shared human-readable surfaces only after the MCP write path is corrected.

## Convergence Criteria

- Each active lane has a bounded owned path set, a worker brief, and a clear verification target.
- Worker handoffs route back through MCP with merge-ready or blocker status instead of ad hoc chat-only summaries.
- Final task truth, shared checklist state, and `DASHBOARD.txt` are consistent with the orchestrator's MCP updates.

## See Also

- [../worktree-worker/SKILL.md](../worktree-worker/SKILL.md)
- [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md)
- [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md)
