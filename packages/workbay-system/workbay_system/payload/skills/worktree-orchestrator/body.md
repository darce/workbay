# Worktree Orchestrator

## Overview

Use this skill when one main agent needs to coordinate worker agents in sibling Git worktrees.

## Trigger

Use this skill when a task should be split across stable seams such as owned paths, contract boundaries, or test packs, and one orchestrator agent needs to coordinate multiple worker lanes without losing shared task truth.

## Goal

- deciding whether a task should be split into lanes
- creating or registering worker worktrees / lane rows
- rendering bounded worker briefs
- deciding merge order and integration checks
- keeping shared checklist and task truth centralized
- applying host-memory admission and lane-hygiene discipline before wide fanout

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary, slice, and review-readiness rules.
- Use [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md) for the canonical lane lifecycle procedure (task manifests, scope enforcement, health model, recipes). Shared preamble, lane states, and decision-template inventory live there once — this skill is the orchestrator execution wrapper only.
- Decision templates when a slice changes a boundary or creates a cross-lane dependency: `DECISION_CONTRACT_CHANGE`, `DECISION_BREAKING_CHANGE`, `DECISION_CROSS_LANE` under the playbook/templates tree (require the worker to use one; do not restate the full list elsewhere).

## Registered MCP surface

Prefer these tools (declared on this skill) over ad-hoc make targets or scripts:

| Tool | Role |
|---|---|
| `manage_worktree_lane` | `operation="upsert"` create/update lane rows; `list` inventory; `close` finish/merged/closed_stale |
| `lane_communication` | `kind="brief"` / `kind="message"` with `operation="record"|"list"|"update"` for briefs and orchestrator↔worker messages |
| `get_lane_activity` | Lane-scoped decisions, tests, blockers, findings, reports, messages (`format="full"` or `"archival"`) |
| `worker_reports` | `operation="list"|"acknowledge"|"record"` for worker handbacks |
| `plan_cursor` | Durable plan-item cursor for multi-slice lane planning (`get` / `list` / `upsert`, optional `require_clean_slice=true`) |

Related orchestrator tools used from this skill's flow but owned by offload/daemon surfaces: `dispatch_lane_work`, `run_offload_pass`, host-memory admission on pass start. See [../offload/SKILL.md](../offload/SKILL.md).

## Preflight

1. Read [instructions.md](../../instructions.md), especially the multi-agent worktree section.
2. Confirm the active handoff task already exists in shared MCP state.
3. Split work only along stable seams:
   - path ownership
   - API contract ownership
   - test ownership
4. Keep shared plans, checklists, and cross-lane conclusions in the orchestrator lane unless explicitly delegated.
5. **Host-memory admission (internal).** Before multi-lane fanout or concurrent daemon width, check the `make doctor` `host_memory` facet (or the admission probe used by offload). Under pressure, reduce concurrent width or wait; on `admission_deferred` retry after pressure drops; on `admission_refused` do not force width until the host recovers. Do not blanket `admission_override=true` to push through genuine pressure.

## Lane Split Guidance

Do not rely on packaged default lanes. Derive lane IDs, owned paths,
required docs, and verification commands from the local task plan,
lane manifest, or repository overlay. Common portable splits include:

- `api`: service API routes, schemas, and contract tests
- `domain`: domain models, persistence, and migration-owned tests
- `ui`: frontend components, state hooks, and browser/unit tests
- `docs`: operator docs, task plans, and generated instruction surfaces

## Core Process

### 1. Create or upsert the lane

From the orchestrator root, register the lane in shared MCP state:

```text
manage_worktree_lane(
  operation="upsert",
  lane_id="<lane>",
  worktree_path="/abs/path/to/worktree",
  branch="<task-branch-or-codex/*>",
  title="...",
  objective="...",
  test_cmd="...",
  task_ref="<task-ref>",
  status="planned"  # or "active" when the worker is about to start
)
```

Create the sibling worktree with the host's normal git worktree flow when one does not already exist; the MCP row is the authority for inflight status ([inflight-worktree-lifecycle](../../../docs/workbay/rules/inflight-worktree-lifecycle.md)).

### 2. Render and record the worker brief

Record a structured brief (owned paths, required docs, test command, definition of ready):

```text
lane_communication(
  kind="brief",
  operation="record",
  lane_id="<lane>",
  task_ref="<task-ref>",
  summary="...",
  required_actions=[...],
  artifacts=[...]
)
```

Paste that brief into the worker session, or pass it through `dispatch_lane_work(..., brief=..., dispatch_id=...)` when using the offload/pass path.

### 3. Dispatch and monitor the lane

Use shared MCP state, not chat memory:

- Dispatch work: `dispatch_lane_work` / offload pass tools when running bounded backends; otherwise open an orchestrator→worker message with `lane_communication(kind="message", operation="record", direction="orchestrator_to_worker", ...)`.
- Monitor: `get_lane_activity(lane_id=..., task_ref=..., format="full")` and `worker_reports(operation="list", lane_id=..., task_ref=...)`.
- Prefer bounded reads over poll loops; for offload, branch on typed pass outcomes rather than spinning on reports ([../offload/SKILL.md](../offload/SKILL.md)).

If the worker gets blocked on another domain, keep the lane in scope and reassign the blocker instead of letting the worker edit outside lane ownership.

When the orchestrator records review findings, blockers, or next actions in MCP from root, stamp them onto the correct lane via lane messages / activity so workers pick them up in `get_lane_activity` / open findings — do not invent make targets for that fanout.

### 4. plan_cursor for multi-slice lane planning

When a task plan is sliced across lanes or sequential slices, advance durable cursors with:

```text
plan_cursor(
  operation="upsert",
  plan_item_id="<item>",
  state="in_progress|done|...",
  lane_id="<lane>",
  task_ref="<task-ref>",
  require_clean_slice=true,  # reject when prior slice still has open HIGH findings
  summary="..."
)
```

`require_clean_slice=true` is the same clean-slice gate documented in [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md): do not start the next plan item while open findings remain on the prior one. Use `operation="get"|"list"` to inspect cursors before dispatch.

### 5. Intake the lane

Workers finish by submitting a merge-ready `worker_reports(operation="record", merge_ready=true, ...)` and optionally a `worker_to_orchestrator` message.

Review the worker branch before merge. Prefer cherry-pick or the host's normal intake path for the worker commit; use selective file intake only when intentionally trimming scope. Acknowledge the report with `worker_reports(operation="acknowledge", ...)`.

After the review gate merges the lane's work, close the lane:

```text
manage_worktree_lane(operation="close", lane_id=<lane>, task_ref=<task>, status="merged")
```

### 6. Lane hygiene

At session start and before task-finish:

1. `manage_worktree_lane(operation="list", task_ref=..., status="all")` (or task-wide list) and flag lanes stuck in `planned` / `blocked` (or idle `active` / `review`) longer than ~48h.
2. For conclusive-dead blocked lanes, close via `manage_worktree_lane(operation="close", ..., status="closed")` or rely on the blocked-lane reaper path that CAS-closes to `closed_stale` (implementation note / orchestrator reaper) — do not leave orphan rows that block later close-checks.
3. Prefer fixing the owning task with `make task-finish` teardown after merge rather than letting debris accumulate ([inflight-worktree-lifecycle](../../../docs/workbay/rules/inflight-worktree-lifecycle.md)).

## Handoff Evidence Checklist

- Before dispatching work to a lane, confirm the worker's contract surface exists and is current. Cite the contract path in the lane brief or dispatch message.
- Before accepting a lane handoff, verify the worker summary names changed contracts, test counts, and schema or runtime implications.
- When routing findings or blockers to a lane, include the contract path and at least one verification command so the worker does not have to rediscover the boundary from scratch.
- When a slice changes a boundary or creates a downstream dependency, require the worker to use the appropriate decision template (see Canonical Policy).
- Keep policy details in the canonical sources: [../../instructions.md](../../instructions.md) for startup and loading rules, and [../../rules/development-workflow.md#cross-boundary-change-protocol](../../rules/development-workflow.md#cross-boundary-change-protocol) for boundary validation.

## Common Rationalizations

- "I can split the task later if it gets messy." Late lane creation usually means ownership is already blurred.
- "The worker can touch shared plans just this once." Shared planning state should stay centralized unless explicitly delegated.
- "Merge order does not matter if each lane passes tests." Cross-lane integration still needs an orchestrated order and intake check.
- "I'll fan out all daemons regardless of host memory." Wide fanout under pressure produces `admission_deferred` / OOM; check admission first.
- "Stale planned/blocked lanes are harmless." Orphan rows block close-checks and obscure real inflight work.

## Red Flags

- Owned-path boundaries are unclear or overlapping.
- The orchestrator is about to delegate work without a lane brief or test command.
- Shared docs or contracts are drifting into worker-owned changes without an explicit delegation.
- Multi-lane fanout is starting while `host_memory` doctor facet shows pressure.
- Lanes older than ~48h remain `planned`/`blocked` with no hygiene action.

## Safety Constraints

- Do not assign two workers to the same owned path set.
- Do not let workers rewrite shared plan truth unless explicitly assigned.
- Reject out-of-scope files during intake.
- Keep the overall task `in_progress` while individual lanes move to `review`, `merged`, or `closed`.
- The orchestrator owns final MCP task updates and dashboard generation; task-scoped machine snapshots are only an on-demand compatibility export (see [../handoff-lifecycle/SKILL.md](../handoff-lifecycle/SKILL.md) step 5).

## Recovery

- If lane ownership is ambiguous, stop and split the work again before dispatching.
- If a worker reports out-of-scope changes, reject intake and reroute the work rather than silently absorbing it.
- If a lane gets blocked on another domain, record the blocker and dispatch the dependent work instead of letting the worker cross boundaries.
- If orchestration state drifts from the shared MCP state, regenerate the shared human-readable surfaces only after the MCP write path is corrected.
- If host admission refuses fanout, shrink width or wait; do not loop with overrides.
- Broken lane history / corrupt worktree → [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md); resumable `checkpoint` / `admission_deferred` → re-dispatch via [../offload/SKILL.md](../offload/SKILL.md).

## Convergence Criteria

- Each active lane has a bounded owned path set, a worker brief, and a clear verification target.
- Worker handoffs route back through MCP (`worker_reports` / `lane_communication`) with merge-ready or blocker status instead of ad hoc chat-only summaries.
- Final task truth, shared checklist state, and `DASHBOARD.txt` are consistent with the orchestrator's MCP updates.
- Stale planned/blocked lanes have been flagged or closed; admission was considered before wide fanout.

## See Also

- [../worktree-worker/SKILL.md](../worktree-worker/SKILL.md)
- [../rescue-lane/SKILL.md](../rescue-lane/SKILL.md)
- [../offload/SKILL.md](../offload/SKILL.md)
- [../incremental-implementation/SKILL.md](../incremental-implementation/SKILL.md)
- [../../playbooks/worktree-orchestration-playbook.md](../../playbooks/worktree-orchestration-playbook.md)
