# Daemon Lifecycle

## Overview

Use this skill when you need to operate on the orchestrator daemon or a lane-scoped worker daemon safely.

## Trigger

Use this skill when a task requires any of the following:

- start or restart an orchestrator or worker daemon
- stop or force-stop a daemon
- pause or resume orchestration
- inspect daemon status, logs, or stale locks
- recover from a daemon that appears wedged, stopped, or detached from its lock file

## Goal

Operate the daemon through the supported control paths so the process state, lock state, and status files stay consistent.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and `ctx7` policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for cross-boundary and review-readiness rules.
- Treat this skill as the execution recipe for daemon operations, not as the canonical source of project-wide policy.

## Core Process

Preferred entrypoints:

- MCP-capable hosts: use MCP tools such as `orchestrator_status`, `orchestrator_pause`, `orchestrator_resume`, `worker_status`, `worker_start`, `worker_stop`, and `worker_resume`
- Shell operators in this repo: prefer the Make targets from `mk/handoff.mk`, including `make daemon-status`, `make daemon-pause`, `make daemon-resume`, `make worker-daemon-status`, `make worker-daemon-stop`, and `make worker-daemon-resume`
- Direct CLI use: prefer the installed `mcp-workbay-handoff --workspace-root <repo> ...` form over `python3 -m ...`

### Orchestrator daemon

1. Inspect daemon state before changing anything:
   - `make daemon-status`
   - `orchestrator_status`
   - confirm lock and process state through `daemon_status()` in `orchestrator_daemon.py`
2. For pause and resume, use the supported pause sentinel flow:
   - `make daemon-pause`
   - `make daemon-resume`
   - `orchestrator_pause`
   - `orchestrator_resume`
   - these map to `daemon_pause()` and `daemon_resume()`
3. For health evaluation, use the daemon status plus lane-health signals:
   - `_check_lane_health()`
   - recent log events
   - worker lane status and open blockers
4. For restart or recovery, stop cleanly first, then start again through the orchestrator controls. Treat `OrchestratorLock` as the source of truth for stale-lock checks.

### Worker daemon

1. Inspect worker status first:
   - `make worker-daemon-status TASK=<task-ref> LANE=<lane-id>`
   - `worker_status`
   - or use `daemon_status()` in `worker_daemon_ctl.py`
2. Start through the supported control path:
   - `worker_start`
   - `daemon_start()`
3. Stop cleanly first:
   - `make worker-daemon-stop TASK=<task-ref> LANE=<lane-id>`
   - `worker_stop`
   - `daemon_stop()`
4. Resume only when a worker was intentionally paused:
   - `make worker-daemon-resume TASK=<task-ref> LANE=<lane-id>`
   - `worker_resume`
   - `daemon_resume()`
5. When a worker appears detached from its lock file, inspect:
   - `WorkerLock`
   - lock payload contents
   - process scan results from `_find_worker_process()`
   - status file from `_read_status_file()`

## Safety Constraints

- Never remove a lock file until you have confirmed the PID is stale or the process is gone.
- Never jump straight to force-kill. Attempt the supported graceful stop path first.
- Do not auto-restart unhealthy lanes just because a daemon is down; inspect `_check_lane_health()` signals, blockers, and recent worker reports first.
- Do not treat a paused daemon as dead. Check pause/resume state before stale-lock recovery.
- Keep daemon operations scoped to the current workspace/task state; do not point a worker control command at a different lane by guesswork.

## Common Rationalizations

- "I can just kill the process and sort out state later." Lock files and status files then drift out of sync with reality.
- "Status looks stale, so I will restart blindly." You need an inspection pass first to distinguish stale locks from a healthy but paused daemon.
- "Worker and orchestrator controls are interchangeable." They use different lifecycles and should be driven through their own supported paths.

## Red Flags

- Lock state, PID state, and status-file state disagree.
- Recovery is about to mutate a daemon without a fresh inspection step.
- The requested action targets the wrong lane, wrong worktree, or wrong daemon type.

## Recovery

- If a daemon fails to start, inspect lock state, last log events, and status-file payload before retrying.
- If a stop request hangs, retry through the supported force option only after the graceful path times out.
- If status says stopped but a lock file still exists, verify the PID with process inspection before removing the lock.
- If a worker is blocked because the lane is unhealthy or out of scope, do not restart repeatedly; record or route the blocker instead.

## Convergence Criteria

- The target daemon is running with a valid lock/status state, or it is cleanly stopped with stale artifacts removed.
- Pause/resume state is reflected correctly in daemon status.
- Any recovery action leaves a clear trail in logs or MCP state rather than an ambiguous partially-running process.

## See Also

- [../worktree-orchestrator/SKILL.md](../worktree-orchestrator/SKILL.md)
- [../worktree-worker/SKILL.md](../worktree-worker/SKILL.md)
- [../../rules/development-workflow.md](../../rules/development-workflow.md)
