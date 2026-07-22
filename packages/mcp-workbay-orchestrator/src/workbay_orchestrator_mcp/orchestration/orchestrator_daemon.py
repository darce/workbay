#!/usr/bin/env python3
"""Orchestrator daemon: dispatch open issues, intake merge-ready lanes, refresh dependents.

Usage:
    python3 scripts/mcp/orchestrator_daemon.py \
        --orchestrator-root . --task-ref <task> \
        [--single-pass] [--poll-interval 60] [--dry-run]
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from dataclasses import field as _dc_field
from importlib import import_module
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Graceful shutdown flag (set by SIGTERM handler)
# ---------------------------------------------------------------------------

_shutdown_requested: bool = False


def _handle_sigterm(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True


# ---------------------------------------------------------------------------
# Re-export submodule symbols for backward compatibility (tests load this
# module via importlib and access everything through ``mod.X``).
# ---------------------------------------------------------------------------
from orchestrator_guidance import (  # noqa: F401
    GUIDANCE_STALL_THRESHOLD,
    GuidanceResolution,
    GuidanceResolutionKind,
    _apply_guidance_resolution,
    _classify_guidance,
    _dedupe_worker_guidance_messages,
    _lane_activity,
    _lane_row,
    _latest_lane_report,
    _list_open_dispatch_messages,
    _list_open_worker_guidance,
    _pending_lane_actions,
    _resolve_guidance_cycle,
    _resolve_next_assignment,
)
from orchestrator_helpers import (  # noqa: F401
    _combined_text,
    _json_list_text,
    _log,
    _message_timestamp,
    _normalize_text,
    _report_timestamp,
    _require_dict_payload,
)

_handoff_read_shapes = import_module(f"{__package__}.handoff_read_shapes" if __package__ else "handoff_read_shapes")
from orchestrator_lanes import (  # noqa: F401
    REASON_DEPENDENCY_CHECK_FAILED,
    _complete_lane_plan_cursor,
    _count_dependency_refusal,
    _git_is_ancestor,
    _git_stdout,
    _intake_lane,
    _is_full_commit_sha,
    _lane_branch_contained_in,
    _lane_has_capacity,
    _lane_has_unmerged_commits,
    _latest_worker_report_outcome,
    _provision_fresh_worktree,
    _refresh_downstream,
    _resolve_lane_branch,
    _resolve_lane_worktree,
    _run_handoff_dispatch,
    _sort_by_manifest_merge_order,
    _task_branch_landing,
    allow_empty_dependency_graph,
    collect_unsatisfied_dependencies,
    depends_on_ancestors,
    depends_on_scheduling_active,
    lane_dependency_satisfied,
    load_manifest_scheduling_state,
    log_dependency_refusal_summary,
    record_lane_landing,
)

# ---------------------------------------------------------------------------
# Thresholds for stall detection
# ---------------------------------------------------------------------------

PLAN_STALL_THRESHOLD = 3
ATTENTION_STALL_THRESHOLD = 3


# ---------------------------------------------------------------------------
# Orchestration-level lane queries (stay here so tests can patch siblings)
# ---------------------------------------------------------------------------


def _poll_merge_ready_lanes(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
) -> list[str]:
    """Return lane IDs that have a merge-ready worker report and unmerged commits.

    Lanes reporting ``merge_ready`` without unmerged commits are intentionally
    excluded here; ``_complete_already_satisfied_merge_ready_lanes`` advances
    their plan cursor instead of thrashing intake/merge.
    """
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    from workbay_orchestrator_mcp.lanes import worker_reports

    ready: list[str] = []
    for lane_id in lane_ids:
        payload = _require_dict_payload(
            worker_reports(
                operation="list",
                task_ref=task_ref,
                lane_id=lane_id,
                limit=1,
                fields="merge_ready",
            ),
            source=f"worker_reports(list merge-ready:{lane_id})",
        )
        if payload.get("ok") is not True:
            continue
        reports = payload.get("reports", [])
        if reports and isinstance(reports[0], dict) and reports[0].get("merge_ready"):
            if _lane_has_unmerged_commits(orchestrator_root, task_ref, lane_id):
                ready.append(lane_id)
    return ready


def _complete_already_satisfied_merge_ready_lanes(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> list[str]:
    """Advance plan cursors for merge-ready lanes that produced no new commits."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    from workbay_orchestrator_mcp.lanes import consume_lane_worker_reports, plan_cursor, worker_reports

    completed: list[str] = []
    for lane_id in lane_ids:
        payload = _require_dict_payload(
            worker_reports(
                operation="list",
                task_ref=task_ref,
                lane_id=lane_id,
                limit=1,
                fields="id,merge_ready,created_at,status",
            ),
            source=f"worker_reports(list noop-merge-ready:{lane_id})",
        )
        if payload.get("ok") is not True:
            continue
        reports = payload.get("reports", [])
        if not reports or not isinstance(reports[0], dict) or not reports[0].get("merge_ready"):
            continue
        if _lane_has_unmerged_commits(orchestrator_root, task_ref, lane_id):
            continue
        report = reports[0]
        report_created_at = report.get("created_at")
        cursor_payload = _require_dict_payload(
            plan_cursor(
                operation="list",
                task_ref=task_ref,
                state="dispatched",
                lane_id=lane_id,
                limit=1,
                fields="dispatched_at,state",
            ),
            source=f"plan_cursor(list noop-freshness:{lane_id})",
        )
        if cursor_payload.get("ok") is not True:
            continue
        cursors = cursor_payload.get("cursors", [])
        if not cursors or not isinstance(cursors[0], dict):
            continue
        dispatched_at = cursors[0].get("dispatched_at")
        if not report_created_at or not dispatched_at or report_created_at <= dispatched_at:
            continue
        if dry_run:
            completed.append(lane_id)
            continue
        cursor = _complete_lane_plan_cursor(task_ref, lane_id)
        if cursor is not None:
            report_id = report.get("id")
            consume_lane_worker_reports(
                lane_id,
                report_id=int(report_id) if report_id is not None else None,
                task_ref=task_ref,
            )
            completed.append(lane_id)
            if callable(log):
                log(
                    "INFO",
                    "plan_cursor_noop_completed",
                    lane=lane_id,
                    plan_item_id=cursor.get("plan_item_id"),
                )
    return completed


def _run_cross_lane_verify(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Run ``make lane-check`` from the lane worktree for the intaken lane."""
    if dry_run:
        return True
    lane_worktree = _resolve_lane_worktree(orchestrator_root, task_ref, lane_id)
    if lane_worktree is None or not lane_worktree.is_dir():
        return False
    cmd = [
        "make",
        "lane-check",
        f"TASK={task_ref}",
        f"LANE={lane_id}",
    ]
    result = subprocess.run(
        cmd,
        cwd=lane_worktree,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _has_open_plan_action(task_ref: str, plan_item_id: str) -> bool:
    from workbay_handoff_mcp import list_next_actions
    from workbay_handoff_mcp.enums import ActionStatus  # noqa: PLC0415

    marker = f"[plan:{plan_item_id}]"
    payload = _require_dict_payload(
        list_next_actions(task_ref=task_ref, status=ActionStatus.PENDING, limit=200),
        source=f"list_next_actions({task_ref})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list next actions for {task_ref}.")
    for row in payload.get("actions", []):
        if isinstance(row, dict) and marker in str(row.get("action") or ""):
            return True
    return False


def _has_open_plan_message(task_ref: str, plan_item_id: str) -> bool:
    from workbay_handoff_mcp.enums import MessageStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication

    marker = f"[plan:{plan_item_id}]"
    payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            status=MessageStatus.OPEN,
            limit=200,
            fields="subject,message",
        ),
        source=f"lane_communication(list plan messages:{task_ref})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lane messages for {task_ref}.")
    for row in payload.get("messages", []):
        if not isinstance(row, dict):
            continue
        haystack = f"{row.get('subject') or ''} {row.get('message') or ''}"
        if marker in haystack:
            return True
    return False


def _escalate_plan_item(
    task_ref: str,
    *,
    plan_item_id: str,
    summary: str,
    heading: str,
    dry_run: bool = False,
    log: Any | None = None,
) -> None:
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import plan_cursor  # noqa: PLC0415

    if dry_run:
        return
    _require_dict_payload(
        plan_cursor(
            operation="upsert",
            task_ref=task_ref,
            plan_item_id=plan_item_id,
            state=PlanCursorState.ESCALATED,
            summary=summary,
            source_heading=heading or None,
        ),
        source=f"plan_cursor(upsert escalate:{plan_item_id})",
    )
    record_decision(
        session=f"{task_ref}-orchestrator-daemon",
        decision=f"Escalated plan item {plan_item_id} for human review.",
        rationale="Task plan item could not be mapped to a single lane from explicit annotations or manifest routing metadata.",
    )
    if callable(log):
        log("WARN", "task_plan_item_escalated", plan_item_id=plan_item_id, heading=heading)


def _dispatch_plan_item(
    task_ref: str,
    *,
    lane_id: str,
    plan_item_id: str,
    summary: str,
    heading: str,
    resolved_plan: Path,
    owned_paths_override: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    from workbay_handoff_mcp import record_decision, update_next_actions  # noqa: PLC0415
    from workbay_handoff_mcp.api import WriteActorInput  # noqa: PLC0415
    from workbay_handoff_mcp.enums import MessageStatus, PlanCursorState  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication, plan_cursor  # noqa: PLC0415

    marker = f"[plan:{plan_item_id}]"
    lane_actor = WriteActorInput(lane_id=lane_id)
    result = {
        "plan_item_id": plan_item_id,
        "lane_id": lane_id,
        "summary": summary,
        "heading": heading,
    }
    if dry_run:
        return result

    action_payload = _require_dict_payload(
        update_next_actions(
            operation="add",
            action=f"{marker} {summary}",
            priority=100,
            actor=lane_actor,
        ),
        source=f"update_next_actions(add:{plan_item_id})",
    )
    if action_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to create next action for {plan_item_id}.")
    action = action_payload.get("action", {})
    action_id_raw = action.get("id") if isinstance(action, dict) else None
    action_id = int(action_id_raw) if action_id_raw is not None else None

    message_payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="record",
            lane_id=lane_id,
            session=f"{task_ref}-orchestrator-plan",
            direction="orchestrator_to_worker",
            subject=f"{lane_id} plan assignment",
            message=f"{marker} {summary}",
            status=MessageStatus.OPEN,
            payload={"owned_paths_override": owned_paths_override} if owned_paths_override else None,
        ),
        source=f"lane_communication(record:{plan_item_id})",
    )
    if message_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to create lane message for {plan_item_id}.")

    cursor_update = _require_dict_payload(
        plan_cursor(
            operation="upsert",
            task_ref=task_ref,
            plan_item_id=plan_item_id,
            state=PlanCursorState.DISPATCHED,
            lane_id=lane_id,
            mcp_action_id=action_id,
            summary=summary,
            source_heading=heading or None,
        ),
        source=f"plan_cursor(upsert dispatch:{plan_item_id})",
    )
    if cursor_update.get("ok") is not True:
        raise RuntimeError(f"Failed to persist plan cursor for {plan_item_id}.")

    record_decision(
        session=f"{task_ref}-orchestrator-daemon",
        decision=f"Dispatched plan item {plan_item_id} to {lane_id}.",
        rationale=f"Selected the next unchecked task-plan item from {resolved_plan.name} and routed it via manifest-owned lane metadata.",
        actor=lane_actor,
    )
    return result


def _dispatch_from_task_plan(
    orchestrator_root: Path,
    task_ref: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> dict[str, Any] | None:
    # Per-slice offload dispatch reuses the existing plan_cursor machinery as-is
    # (no schema change): each unchecked plan item maps to one bounded single_pass
    # cycle via DISPATCHED → COMPLETED cursor advancement.
    from lane_manifest import load_manifest, task_plan_path
    from task_plan_parser import map_plan_item_to_lane, normalize_plan_item, parse_task_plan
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import plan_cursor  # noqa: PLC0415

    plan_path = task_plan_path(task_ref, orchestrator_root=str(orchestrator_root))
    if not isinstance(plan_path, str) or not plan_path.strip():
        return None
    resolved_plan = Path(plan_path)
    if not resolved_plan.exists():
        raise RuntimeError(f"Task plan path does not exist for {task_ref}: {resolved_plan}")

    manifest = load_manifest(task_ref)
    if not isinstance(manifest, dict):
        return None
    items = parse_task_plan(resolved_plan)
    m_order = manifest.get("merge_order", [])

    def _sort_key(item) -> int:
        n = normalize_plan_item(item)
        lane_id = map_plan_item_to_lane(n, manifest=manifest)
        if lane_id and lane_id in m_order:
            return m_order.index(lane_id)
        return len(m_order)

    items.sort(key=_sort_key)

    for item in items:
        if item.checked:
            continue
        normalized = normalize_plan_item(item)
        cursor_payload = _require_dict_payload(
            plan_cursor(operation="get", task_ref=task_ref, plan_item_id=normalized.plan_item_id),
            source=f"plan_cursor(get:{normalized.plan_item_id})",
        )
        if cursor_payload.get("ok") is not True:
            raise RuntimeError(f"Failed to read plan cursor for {normalized.plan_item_id}.")
        cursor = cursor_payload.get("cursor")
        if isinstance(cursor, dict) and str(cursor.get("state") or "") in {
            PlanCursorState.DISPATCHED,
            PlanCursorState.COMPLETED,
            PlanCursorState.SKIPPED,
            PlanCursorState.ESCALATED,
        }:
            continue

        lane_id = map_plan_item_to_lane(normalized, manifest=manifest)
        if lane_id is None:
            _escalate_plan_item(
                task_ref,
                plan_item_id=normalized.plan_item_id,
                summary=normalized.summary,
                heading=normalized.heading,
                dry_run=dry_run,
                log=log,
            )
            continue

        if _has_open_plan_action(task_ref, normalized.plan_item_id) or _has_open_plan_message(
            task_ref, normalized.plan_item_id
        ):
            continue
        if not _lane_has_capacity(task_ref, lane_id):
            continue

        result = _dispatch_plan_item(
            task_ref,
            lane_id=lane_id,
            plan_item_id=normalized.plan_item_id,
            summary=normalized.summary,
            heading=normalized.heading,
            resolved_plan=resolved_plan,
            dry_run=dry_run,
        )
        result["line_start"] = normalized.line_start
        return result
    return None


# ---------------------------------------------------------------------------
# salvage_and_close_lane: freeze a failed lane and classify its changed files
# ---------------------------------------------------------------------------


def salvage_and_close_lane(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> dict[str, Any]:
    """Freeze a failed lane, classify its changed files by ownership, and close it.

    Returns a dict with keys:
    - ``lane_id``: the lane that was closed
    - ``this_lane``: files in the lane's own owned_paths
    - ``other_lanes``: dict mapping lane IDs to files belonging to those lanes
    - ``unclassified``: files that don't match any lane's owned_paths
    - ``worktree_preserved``: str path to the preserved worktree
    - ``dry_run``: whether mutation was skipped
    """
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    from lane_exec import _matches_any_owned_path
    from lane_manifest import load_manifest

    worktree = _resolve_lane_worktree(orchestrator_root, task_ref, lane_id)

    # Collect all changed + untracked files in the lane's worktree
    changed: list[str] = []
    if worktree is not None and worktree.is_dir():
        for git_args in (
            ["git", "-C", str(worktree), "diff", "--name-only", "HEAD"],
            ["git", "-C", str(worktree), "ls-files", "--others", "--exclude-standard"],
        ):
            try:
                result = subprocess.run(git_args, capture_output=True, text=True, check=False, timeout=15)
                for line in (result.stdout or "").splitlines():
                    f = line.strip()
                    if f:
                        changed.append(f)
            except (subprocess.TimeoutExpired, OSError):
                pass

    # Load manifest to retrieve owned_paths for every lane
    manifest = load_manifest(task_ref)
    all_lanes: dict[str, Any] = manifest.get("lanes", {}) if isinstance(manifest, dict) else {}

    this_owned: list[str] = []
    if isinstance(all_lanes.get(lane_id), dict):
        this_owned = list(all_lanes[lane_id].get("owned_paths") or [])

    # Classify each changed file
    this_lane_files: list[str] = []
    other_lane_files: dict[str, list[str]] = {}
    unclassified: list[str] = []

    for f in sorted(set(changed)):
        if _matches_any_owned_path(f, this_owned):
            this_lane_files.append(f)
            continue
        matched_lanes = [
            lid
            for lid, cfg in all_lanes.items()
            if lid != lane_id
            and isinstance(cfg, dict)
            and _matches_any_owned_path(f, list(cfg.get("owned_paths") or []))
        ]
        if matched_lanes:
            for m in matched_lanes:
                other_lane_files.setdefault(m, []).append(f)
        else:
            unclassified.append(f)

    salvage: dict[str, Any] = {
        "lane_id": lane_id,
        "this_lane": this_lane_files,
        "other_lanes": other_lane_files,
        "unclassified": unclassified,
        "worktree_preserved": str(worktree) if worktree else None,
        "dry_run": dry_run,
    }

    if not dry_run:
        from workbay_handoff_mcp import record_decision  # noqa: PLC0415
        from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        # Resolve branch name from manifest
        lane_cfg = all_lanes.get(lane_id)
        branch = (lane_cfg.get("branch") or "") if isinstance(lane_cfg, dict) else ""

        _require_dict_payload(
            manage_worktree_lane(
                operation="upsert",
                lane_id=lane_id,
                worktree_path=str(worktree) if worktree else "",
                branch=branch,
                status=LaneStatus.CLOSED,
                task_ref=task_ref,
                notes=(
                    f"salvage_and_close: {len(this_lane_files)} owned files preserved; "
                    f"{len(unclassified)} unclassified."
                ),
            ),
            source=f"manage_worktree_lane(upsert salvage:{lane_id})",
        )
        record_decision(
            session=f"{task_ref}-orchestrator-daemon",
            decision=f"salvage_and_close: lane {lane_id} closed. Worktree preserved at {worktree}.",
            rationale=json.dumps(salvage, indent=2, default=str),
        )

    if callable(log):
        log(
            "INFO",
            "salvage_and_close_complete",
            lane_id=lane_id,
            this_lane_count=len(this_lane_files),
            other_lanes_count=sum(len(v) for v in other_lane_files.values()),
            unclassified_count=len(unclassified),
            dry_run=dry_run,
        )

    return salvage


# ---------------------------------------------------------------------------
# Exclusive orchestrator lock
# ---------------------------------------------------------------------------


class OrchestratorLock:
    """flock-based exclusive lock so only one orchestrator daemon runs at a time."""

    def __init__(self, state_dir: Path) -> None:
        self._lock_path = state_dir / "orchestrator.lock"
        self._fh: Any = None

    def acquire(self) -> bool:
        """Try to acquire the lock.  Returns True on success."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._lock_path.open("w")
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fh.write(json.dumps({"pid": os.getpid()}))
            self._fh.flush()
            return True
        except OSError:
            self._fh.close()
            self._fh = None
            return False

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
            self._fh.close()
        except Exception:
            pass
        self._fh = None
        try:
            self._lock_path.unlink(missing_ok=True)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Pause/resume surface
# ---------------------------------------------------------------------------


def _pause_path(state_dir: Path) -> Path:
    return state_dir / "daemon-paused"


def _is_paused(state_dir: Path) -> bool:
    return _pause_path(state_dir).exists()


def daemon_pause(state_dir: Path) -> None:
    """Create the pause sentinel."""
    import datetime

    state_dir.mkdir(parents=True, exist_ok=True)
    _pause_path(state_dir).write_text(
        json.dumps({"paused_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    )


def daemon_resume(state_dir: Path) -> None:
    """Remove the pause sentinel."""
    p = _pause_path(state_dir)
    if p.exists():
        p.unlink()


# ---------------------------------------------------------------------------
# Status query
# ---------------------------------------------------------------------------


def daemon_status(state_dir: Path, log_dir: Path) -> dict[str, Any]:
    """Return a status dict for the daemon-status target."""
    lock_path = state_dir / "orchestrator.lock"
    lock_info: dict[str, Any] = {"held": False}
    if lock_path.exists():
        try:
            lock_info = {**json.loads(lock_path.read_text()), "held": True}
        except (json.JSONDecodeError, OSError):
            lock_info = {"held": True, "pid": "unknown"}

    paused = _is_paused(state_dir)

    log_path = log_dir / "orchestrator.jsonl"
    last_cycle: dict[str, Any] | None = None
    last_verify: dict[str, Any] | None = None
    if log_path.exists():
        for line in reversed(log_path.read_text().splitlines()):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if last_cycle is None and entry.get("event") == "cycle_end":
                last_cycle = entry
            if last_verify is None and entry.get("event") == "verify_complete":
                last_verify = entry
            if last_cycle and last_verify:
                break

    return {
        "mode": "singleton",
        "state_dir": str(state_dir),
        "log_dir": str(log_dir),
        "lock": lock_info,
        "paused": paused,
        "last_cycle": last_cycle,
        "last_verify": last_verify,
    }


# ---------------------------------------------------------------------------
# Orchestration queries (restored for backward compatibility and logic)
# ---------------------------------------------------------------------------


def _resolve_task_ref(orchestrator_root: Path, task_ref: str | None) -> str:
    """Infer the task reference from active state or manifests if not provided."""
    if task_ref:
        return task_ref
    # 1. Try active task from MCP. read_handoff_state validates the
    # envelope through workbay_protocol.ActiveTask before returning.
    try:
        envelope = _handoff_read_shapes.read_handoff_state(**_handoff_read_shapes.active_task_identity_kwargs())
        state = _require_dict_payload(envelope, source="get_handoff_state(identity)")
        if state.get("ok") and state.get("task_ref"):
            return state["task_ref"]
    except Exception:
        pass
    # 2. Try sole manifest in docs/tasks/
    from lane_manifest import list_manifest_tasks

    tasks = list_manifest_tasks(orchestrator_root=str(orchestrator_root))
    if len(tasks) == 1:
        return tasks[0]
    if not tasks:
        raise RuntimeError("No task manifests found in docs/tasks/.")
    raise RuntimeError(f"Task reference is ambiguous. Available manifests: {', '.join(tasks)}.")


def _lane_work_in_flight(rows: list[dict[str, Any]], *, stale_attention_lanes: set[str] | None = None) -> bool:
    """True if any lane is running or needs attention (and is not stale)."""
    for row in rows:
        if row.get("running"):
            return True
        if row.get("action") == "skip" and row.get("reason") == "attention_required":
            lane_id = _normalize_text(row.get("lane_id"))
            if stale_attention_lanes and lane_id in stale_attention_lanes:
                continue
            return True
    return False


def _remaining_plan_work(task_ref: str) -> list[dict[str, Any]]:
    """Return a list of plan items that are not yet dispatched or completed."""
    # This is primarily for stall detection. In this implementation, we rely on
    # _dispatch_from_task_plan returning None to detect when the plan is empty
    # or stalled. Tests mock this to return non-empty when they want to simulate
    # a stall.
    return []


def _check_lane_health(status: dict[str, Any]) -> tuple[str, str | None]:
    """Compute health from a worker status dict.

    Returns ``(health, recommended_action)`` where *health* is one of
    ``"healthy"``, ``"degraded"``, or ``"unhealthy"`` and *recommended_action*
    is an operator hint string or ``None``.
    """
    attention = bool(status.get("attention_required"))
    worker_state = status.get("worker_state")

    status_record = status.get("status_record") or {}
    streak_info = status_record.get("exhaustion_streak")
    streak = int(streak_info.get("count") or 0) if isinstance(streak_info, dict) else 0

    obs = status.get("observability") or {}
    history = obs.get("history") or []
    scope_violations = sum(1 for e in history if isinstance(e, dict) and e.get("phase") == "scope_check")

    latest_obs = obs.get("latest") or {}
    ctx = status.get("context_utilization_latest") or latest_obs.get("context_utilization") or {}
    pressure = str(ctx.get("pressure") or "normal")

    if worker_state == "unhealthy" or streak >= 2 or attention:
        action: str | None = "promote_model" if streak >= 2 else "close_lane"
        return "unhealthy", action

    if scope_violations > 0:
        return "degraded", "fresh_worktree"

    if pressure == "high":
        return "degraded", "split_lane"

    if pressure == "elevated":
        return "degraded", None

    return "healthy", None


def _ensure_lane_workers(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    *,
    backend: str = "codex-cli",
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
    dry_run: bool = False,
    log: Any = None,
    prev_health: "dict[str, str] | None" = None,
) -> list[dict[str, Any]]:
    """Status all lanes and optionally start missing workers via MCP.

    When the manifest's total ``depends_on`` edge set is non-empty
    (or ``WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1``), dispatch is gated by the
    completion predicate over transitive ancestors. With total edges == 0
    and no override, only ``_check_lane_health`` gates starts (legacy).
    Manifest missing/unparseable degrades to legacy (never raises).
    """
    from workbay_orchestrator_mcp.api import manage_worker  # noqa: PLC0415

    # Scheduling edge source — load once per call (manifest unreadable → legacy).
    # Returns (depends_on, total_edges, scheduling_active) in that order.
    try:
        depends_on, total_edges, scheduling_active = load_manifest_scheduling_state(
            task_ref,
            orchestrator_root=orchestrator_root,
        )
    except Exception:  # noqa: BLE001 — mirror salvage_skipped_no_manifest shape
        depends_on, total_edges, scheduling_active = {}, 0, False
        if log is not None:
            log(
                "WARNING",
                "dependency_gate_skipped_no_manifest",
                task_ref=task_ref,
            )

    # Fan-out order when the graph has real edges (implementation note S2). Build a NEW
    # local list — never mutate lane_ids / ctx.m_order (re-read after this call).
    if scheduling_active and total_edges > 0:
        from lane_ready_set import dispatch_order  # noqa: PLC0415

        ordered = dispatch_order(list(lane_ids), depends_on if isinstance(depends_on, dict) else {})
    else:
        ordered = list(lane_ids)

    # Hoist one worktree-lane list for per-lane backend fallback (avoid O(n)
    # manage_worktree_lane list calls via _lane_row inside the spawn loop).
    lane_rows_by_id: dict[str, dict[str, Any]] = {}
    try:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        listed_raw = manage_worktree_lane(
            operation="list",
            task_ref=task_ref,
            status="all",
            limit=500,
        )
        if isinstance(listed_raw, str):
            listed_raw = json.loads(listed_raw)
        if isinstance(listed_raw, dict) and listed_raw.get("ok") is True:
            for row in listed_raw.get("lanes") or []:
                if isinstance(row, dict) and row.get("lane_id") is not None:
                    lane_rows_by_id[str(row["lane_id"])] = row
    except Exception:  # noqa: BLE001 — backend fallback degrades; never abort spawn
        lane_rows_by_id = {}

    rows: list[dict[str, Any]] = []
    for lane_id in ordered:
        status_payload = _require_dict_payload(
            manage_worker(task_ref=task_ref, lane_id=lane_id, action="status"),
            source=f"manage_worker(status:{lane_id})",
        )
        if status_payload.get("ok") is not True:
            continue

        # Merge with lane identity
        status_payload["lane_id"] = lane_id

        if status_payload.get("running"):
            rows.append(status_payload)
            if prev_health is not None:
                prev_health[lane_id] = "healthy"
            continue

        # Gate: skip lanes that are unhealthy
        health, recommended_action = _check_lane_health(status_payload)

        # Emit lane_health_changed when health transitions between cycles.
        if prev_health is not None:
            previous = prev_health.get(lane_id)
            if previous is not None and previous != health and log is not None:
                log(
                    "INFO",
                    "lane_health_changed",
                    lane_id=lane_id,
                    previous=previous,
                    current=health,
                    recommended_action=recommended_action,
                )
            prev_health[lane_id] = health

        if health == "unhealthy":
            status_payload["worker_state"] = "unhealthy"
            status_payload["reason"] = "attention_required"
            if log is not None:
                status_record = status_payload.get("status_record") or {}
                streak_info = status_record.get("exhaustion_streak")
                streak = int(streak_info.get("count") or 0) if isinstance(streak_info, dict) else 0
                log(
                    "WARNING",
                    "lane_unhealthy",
                    lane_id=lane_id,
                    exhaustion_streak=streak,
                    attention_required=bool(status_payload.get("attention_required")),
                    recommended_action=recommended_action,
                )
            if recommended_action == "close_lane":
                try:
                    salvage_and_close_lane(
                        orchestrator_root,
                        task_ref,
                        lane_id,
                        dry_run=dry_run,
                        log=log,
                    )
                except FileNotFoundError:
                    if log is not None:
                        log(
                            "WARNING",
                            "salvage_skipped_no_manifest",
                            lane_id=lane_id,
                            task_ref=task_ref,
                        )
            rows.append(status_payload)
            continue

        # Provision a fresh worktree when health is degraded by scope violations
        if recommended_action == "fresh_worktree":
            from lane_manifest import get_lane_config

            lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
            if isinstance(lane_cfg, dict) and lane_cfg.get("redispatch_mode") == "fresh_worktree":
                fresh_path = _provision_fresh_worktree(orchestrator_root, task_ref, lane_id, dry_run=dry_run)
                if fresh_path is not None and log is not None:
                    log("INFO", "fresh_worktree_provisioned", lane_id=lane_id, worktree_path=str(fresh_path))
                elif log is not None:
                    log("WARNING", "fresh_worktree_provision_failed", lane_id=lane_id)

        # depends_on completion gate (internal). Legacy path: health only.
        # Per-lane try/except: a predicate fault refuses this lane only (fail-closed),
        # never aborts the poll cycle or the remaining lanes (mirror slice-2 shape).
        if scheduling_active:
            try:
                blocked_by, dep_reason = collect_unsatisfied_dependencies(
                    orchestrator_root,
                    task_ref,
                    lane_id,
                    depends_on if isinstance(depends_on, dict) else {},
                    log=log,
                )
            except Exception as exc:  # noqa: BLE001 — fail closed for this lane
                if log is not None:
                    log(
                        "WARNING",
                        "lane_dependency_check_failed",
                        lane_id=lane_id,
                        task_ref=task_ref,
                        error=str(exc),
                    )
                try:
                    _count_dependency_refusal(REASON_DEPENDENCY_CHECK_FAILED)
                except Exception:  # noqa: BLE001 — observability must not abort cycle
                    pass
                status_payload["started"] = False
                status_payload["skipped"] = True
                status_payload["reason"] = REASON_DEPENDENCY_CHECK_FAILED
                status_payload["blocked_by"] = []
                status_payload["worker_state"] = "blocked_upstream"
                rows.append(status_payload)
                continue
            if blocked_by:
                status_payload["started"] = False
                status_payload["skipped"] = True
                status_payload["reason"] = dep_reason or "unresolved_upstream_dependencies"
                status_payload["blocked_by"] = blocked_by
                status_payload["worker_state"] = "blocked_upstream"
                rows.append(status_payload)
                continue

        # Decide if we should start it
        if worker_start_mode == "mcp" and not dry_run:
            # Per-lane backend (implementation note S2): preferred_backend pin, else lane row.
            # Do NOT use the function-level/ctx.backend for twins that carry their own.
            lane_backend: str | None = None
            try:
                from lane_manifest import get_lane_config  # noqa: PLC0415

                pin = get_lane_config(
                    task_ref,
                    lane_id,
                    orchestrator_root=str(orchestrator_root),
                )
                if isinstance(pin, dict):
                    preferred = pin.get("preferred_backend")
                    if isinstance(preferred, str) and preferred.strip():
                        lane_backend = preferred.strip()
            except Exception:  # noqa: BLE001 — fall through to lane-row backend
                pass
            if not lane_backend:
                lane_row = lane_rows_by_id.get(lane_id)
                if lane_row is None:
                    try:
                        lane_row = _lane_row(task_ref, lane_id)
                    except Exception:  # noqa: BLE001 — _lane_row raises on miss
                        lane_row = None
                row_backend = (lane_row or {}).get("backend")
                if isinstance(row_backend, str) and row_backend.strip():
                    lane_backend = row_backend.strip()
            if not lane_backend:
                lane_backend = backend

            start_payload = _require_dict_payload(
                manage_worker(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    action="start",
                    backend=lane_backend,
                    reasoning_effort=worker_reasoning_effort,
                    model=model,
                ),
                source=f"manage_worker(start:{lane_id})",
            )
            if start_payload.get("ok"):
                status_payload["running"] = True
                status_payload["worker_state"] = "spawned"
                status_payload["pid"] = start_payload.get("pid")

        rows.append(status_payload)
    return rows


# ---------------------------------------------------------------------------
# Main orchestrator loop
# ---------------------------------------------------------------------------


@dataclass
class OrchestratorContext:
    """Shared state object threaded through each phase of the orchestrator loop."""

    __module__ = "builtins"
    # Immutable configuration
    orchestrator_root: Path
    task_ref: str
    state_dir: Path
    log_dir: Path
    m_order: list[str]
    poll_interval: int
    single_pass: bool
    dry_run: bool
    backend: str
    worker_start_mode: str
    worker_reasoning_effort: str
    model: "str | None"
    log: Any
    # Mutable counters / per-loop state
    dispatch_failure_count: int = 0
    runtime_failure_count: int = 0
    plan_stall_count: int = 0
    guidance_stalls: "dict[str, tuple[int, int]]" = _dc_field(default_factory=dict)
    attention_stalls: "dict[str, int]" = _dc_field(default_factory=dict)
    lane_health_prev: "dict[str, str]" = _dc_field(default_factory=dict)
    # Per-cycle outputs written by phases and consumed by the main loop
    guidance_results: list = _dc_field(default_factory=list)
    plan_dispatch: "dict[str, Any] | None" = None
    autostart_results: list = _dc_field(default_factory=list)
    ordered_ready: list = _dc_field(default_factory=list)
    has_in_flight: bool = False
    ready_to_close: bool = False


def _dispatch_phase(ctx: OrchestratorContext) -> None:
    """Step 1: Dispatch open issues to lanes and emit ACE advisory if needed."""
    try:
        dispatch_result = _run_handoff_dispatch(
            ctx.orchestrator_root,
            ctx.task_ref,
            dry_run=ctx.dry_run,
        )
        ctx.dispatch_failure_count = 0
        ctx.log("INFO", "dispatch_complete", result=dispatch_result)

        # ACE advisory was removed — ACE is now project-local (scripts/ace/).
        # The PostToolUse hook (scripts/hooks/ace-detect.py) handles detection;
        # operators run 'make ace-reflect' to apply counter updates.
    except Exception as exc:
        ctx.dispatch_failure_count += 1
        ctx.log("ERROR", "dispatch_failed", error=str(exc))
        if ctx.single_pass or ctx.dispatch_failure_count >= 3:
            raise


def _guidance_phase(ctx: OrchestratorContext) -> None:
    """Step 2: Resolve worker guidance handoffs and update guidance stall counters."""
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    ctx.guidance_results = _resolve_guidance_cycle(
        ctx.orchestrator_root,
        ctx.task_ref,
        dry_run=ctx.dry_run,
        log=ctx.log,
    )
    for resolution in ctx.guidance_results:
        if resolution.kind == GuidanceResolutionKind.FATAL_ERROR:
            previous = ctx.guidance_stalls.get(resolution.lane_id)
            if previous and previous[0] == resolution.worker_message_id:
                ctx.guidance_stalls[resolution.lane_id] = (resolution.worker_message_id, previous[1] + 1)
            else:
                ctx.guidance_stalls[resolution.lane_id] = (resolution.worker_message_id, 1)
            stall_count = ctx.guidance_stalls[resolution.lane_id][1]
            ctx.log(
                "ERROR",
                "guidance_failed",
                lane=resolution.lane_id,
                error=resolution.error,
                stall_count=stall_count,
            )
            if stall_count >= GUIDANCE_STALL_THRESHOLD:
                ctx.log("ERROR", "terminal_error", lane=resolution.lane_id, reason="guidance_stall")
                raise RuntimeError(f"guidance_stall: lane={resolution.lane_id}")
            if ctx.single_pass:
                # Continue cycle to intake other lanes, but mark for exit
                ctx.dispatch_failure_count = 999
            continue
        ctx.guidance_stalls.pop(resolution.lane_id, None)
        event_name = "guidance_resolved"
        if resolution.kind == GuidanceResolutionKind.REDISPATCH:
            event_name = "guidance_redispatched"
        elif resolution.lane_status == LaneStatus.BLOCKED:
            event_name = "guidance_escalated"
        ctx.log(
            "INFO",
            event_name,
            lane=resolution.lane_id,
            kind=resolution.kind,
            latest_report_id=resolution.latest_report_id,
        )


def _plan_dispatch_phase(ctx: OrchestratorContext) -> None:
    """Step 3: Derive new work from the task plan when backlog is otherwise empty."""
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    ctx.plan_dispatch = _dispatch_from_task_plan(
        ctx.orchestrator_root,
        ctx.task_ref,
        dry_run=ctx.dry_run,
        log=ctx.log,
    )
    if ctx.plan_dispatch is not None:
        if not ctx.dry_run:
            record_decision(
                session=f"{ctx.task_ref}-orchestrator-daemon",
                decision="Per-slice offload dispatch reuses plan_cursor without schema changes.",
                rationale=(
                    "Each unchecked task-plan item maps to one bounded single_pass cycle; "
                    "plan_cursor state DISPATCHED tracks the active slice and advances to COMPLETED on intake."
                ),
            )
        ctx.log("INFO", "task_plan_dispatch", **ctx.plan_dispatch)


def _reap_blocked_lanes_maintenance(ctx: OrchestratorContext) -> None:
    """Per-cycle conclusive-close pass for aged blocked lanes ([RES-07] Slice-3).

    The blocked-lane AGING report is surfaced read-only on the dashboard every
    cycle; without a wired write side, ``blocked`` lanes whose worktree is gone
    AND branch is merged/deleted never transition to ``closed_stale`` and
    accumulate unbounded. This runs the conservative reaper on the same lane
    heartbeat that already polls merge-ready lanes.

    ``apply`` is gated on ``dry_run`` (a dry-run daemon reports would-close only
    and writes nothing). The reaper never raises, but the call is defensively
    wrapped so a maintenance hiccup can never take down a cycle.
    """
    try:
        from workbay_orchestrator_mcp.lanes import reap_blocked_lanes  # noqa: PLC0415

        result = reap_blocked_lanes(apply=not ctx.dry_run)
    except Exception as exc:  # noqa: BLE001 — maintenance must never break the cycle
        ctx.log("WARN", "blocked_lane_reap_failed", error=str(exc))
        return
    if not isinstance(result, dict):
        return
    closed = result.get("closed") or []
    would_close = result.get("would_close") or []
    if closed or would_close:
        ctx.log(
            "INFO",
            "blocked_lane_reap",
            applied=bool(result.get("applied")),
            closed=len(closed),
            would_close=len(would_close),
        )

    # 0112 Bug 2: the periodic catch-all for lanes orphaned by task archival that
    # never entered ``blocked`` (invisible to reap_blocked_lanes). Daemon-less flows
    # self-heal at task-finish; this is the daemon backstop for the rest. Same
    # dry-run gate; defensively wrapped so it can never take down a cycle.
    try:
        from workbay_orchestrator_mcp.lanes import reap_task_archived_orphan_lanes  # noqa: PLC0415

        orphan_result = reap_task_archived_orphan_lanes(apply=not ctx.dry_run)
    except Exception as exc:  # noqa: BLE001 — maintenance must never break the cycle
        ctx.log("WARN", "archived_orphan_lane_reap_failed", error=str(exc))
        return
    if isinstance(orphan_result, dict):
        o_closed = orphan_result.get("closed") or []
        o_would = orphan_result.get("would_close") or []
        if o_closed or o_would:
            ctx.log(
                "INFO",
                "archived_orphan_lane_reap",
                applied=bool(orphan_result.get("applied")),
                closed=len(o_closed),
                would_close=len(o_would),
            )
        # PMH-F12: a truncated sweep (batch cap hit) leaves a backlog; surface it
        # so a partial reap is not mistaken for a clean one. The next cycle
        # continues, but if the daemon stops first the remainder persists.
        if orphan_result.get("truncated"):
            ctx.log(
                "WARN",
                "archived_orphan_lane_reap_truncated",
                max_batch=orphan_result.get("max_batch"),
                note="batch cap hit; more archived-orphan lanes may remain — next cycle continues",
            )


def _recover_stranded_landing_lanes(ctx: OrchestratorContext) -> list[str]:
    """Recover non-MERGED lanes stranded after a real merge ([DOM-06]).

    Two sub-arms (both unreachable via re-intake once the lane tip is in the
    task tip — ``_poll_merge_ready_lanes`` requires unmerged commits):

    1. **Valid landing record present, status not MERGED** — crash between
       ``record_lane_landing`` and the MERGED status write. Re-close MERGED
       when the landing SHA is an ancestor of the task-branch tip **and**
       fully contains the lane branch. A present-but-invalid landing row
       (missing/non-full ``commit_sha``) is treated as absent and falls
       through to sub-arm 2.
    2. **No valid landing record, status not MERGED** — intake withheld
       MERGED after a ledger write failure. Heal only when an **unconsumed**
       ``merge_ready`` worker report remains (consume-after-record leaves it
       submitted on record/close failure) **and** the lane branch is
       contained in the task tip; then re-attempt ``record_lane_landing``
       (fresh tip capture), MERGED, consume the report (mirroring intake),
       and refresh. Vacuous success lanes (``no_work`` / never merge-ready /
       already-consumed report) must not be terminalized.

    Reopened lanes (old landing still ancestral, but new commits on the lane
    branch) are skipped so recovery cannot re-terminalize work in progress.
    Close envelope failures and per-lane exceptions never count as progress and
    never abort the recovery loop for remaining lanes.
    """
    if ctx.dry_run:
        return []

    try:
        from workbay_handoff_mcp import latest_lane_landing  # noqa: PLC0415
    except ImportError:
        # Reader ships concurrently in the handoff package (internal).
        # Until it merges, recovery is a no-op rather than crashing the daemon.
        return []

    from lane_manifest import downstream_lanes  # noqa: PLC0415
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
        consume_lane_worker_reports,
        manage_worktree_lane,
        worker_reports,
    )

    tip = _git_stdout(ctx.orchestrator_root, "rev-parse", "HEAD")
    if not tip or not _is_full_commit_sha(tip):
        return []

    try:
        listed_raw = manage_worktree_lane(
            operation="list",
            task_ref=ctx.task_ref,
            status="all",
            limit=500,
        )
        if isinstance(listed_raw, str):
            listed_raw = json.loads(listed_raw)
        listed = listed_raw if isinstance(listed_raw, dict) else {}
    except Exception as exc:  # noqa: BLE001 — recovery must never kill the cycle
        ctx.log("ERROR", "landing_recovery_list_failed", error=str(exc))
        return []
    if listed.get("ok") is not True:
        return []

    status_by_lane: dict[str, str] = {}
    branch_by_lane: dict[str, str] = {}
    for row in listed.get("lanes") or []:
        if isinstance(row, dict) and row.get("lane_id") is not None:
            lid = str(row["lane_id"])
            status_by_lane[lid] = str(row.get("status") or "").strip().lower()
            branch_raw = row.get("branch")
            if isinstance(branch_raw, str) and branch_raw.strip():
                branch_by_lane[lid] = branch_raw.strip()

    merged_token = str(getattr(LaneStatus, "MERGED", "merged")).strip().lower()
    if merged_token.startswith("lanestatus."):
        merged_token = "merged"

    def _close_merged(lane_id: str, *, notes: str, sha: str) -> bool:
        close_raw = manage_worktree_lane(
            operation="close",
            lane_id=lane_id,
            status=LaneStatus.MERGED,
            notes=notes,
            task_ref=ctx.task_ref,
        )
        if isinstance(close_raw, str):
            close_raw = json.loads(close_raw)
        close_payload = close_raw if isinstance(close_raw, dict) else {}
        # H4: only count actual successful transitions as recovery progress.
        if close_payload.get("ok") is not True:
            ctx.log(
                "ERROR",
                "landing_recovery_close_failed",
                lane=lane_id,
                sha=sha,
                payload=close_payload,
            )
            return False
        return True

    def _refresh_after_recovery(lane_id: str) -> None:
        deps = downstream_lanes(ctx.task_ref, lane_id)
        if deps:
            ctx.log("INFO", "refresh_start", lane=lane_id, downstream=deps, reason="landing_recovery")
            refresh_results = _refresh_downstream(
                ctx.orchestrator_root,
                ctx.task_ref,
                lane_id,
                deps,
                dry_run=ctx.dry_run,
            )
            ctx.log("INFO", "refresh_complete", lane=lane_id, results=refresh_results)

    def _unconsumed_merge_ready_report(lane_id: str) -> dict[str, Any] | None:
        """Latest unconsumed merge-ready report, or None (fail-closed)."""
        try:
            payload = worker_reports(
                operation="list",
                task_ref=ctx.task_ref,
                lane_id=lane_id,
                limit=1,
                fields="id,merge_ready,status,outcome",
            )
        except Exception:  # noqa: BLE001 — heal eligibility fails closed
            return None
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                return None
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            return None
        reports = payload.get("reports")
        if not isinstance(reports, list) or not reports:
            return None
        report = reports[0]
        if not isinstance(report, dict) or not report.get("merge_ready"):
            return None
        # Consume ACKs (status out of submitted); rows survive with merge_ready set.
        if str(report.get("status") or "").strip().lower() != "submitted":
            return None
        return report

    recovered: list[str] = []
    for lane_id in ctx.m_order:
        try:
            status = status_by_lane.get(lane_id, "")
            if status == merged_token or status == "merged":
                continue

            try:
                raw = latest_lane_landing(lane_id=lane_id, task_ref=ctx.task_ref)
                if isinstance(raw, str):
                    raw = json.loads(raw)
                env = raw if isinstance(raw, dict) else {}
            except Exception as exc:  # noqa: BLE001 — isolation / mock safety
                ctx.log("ERROR", "landing_recovery_read_failed", lane=lane_id, error=str(exc))
                continue

            data = env.get("data") if isinstance(env.get("data"), dict) else env
            if not isinstance(data, dict):
                continue
            landing = data.get("landing")

            # --- Sub-arm 1: valid record present, status not MERGED ---
            if isinstance(landing, dict):
                sha = str(landing.get("commit_sha") or "").strip()
                if sha and _is_full_commit_sha(sha):
                    if not _git_is_ancestor(ctx.orchestrator_root, sha, tip):
                        ctx.log(
                            "INFO",
                            "landing_recovery_sha_not_ancestor",
                            lane=lane_id,
                            sha=sha,
                            tip=tip,
                        )
                        continue

                    # H5: skip reopened lanes — old landing may still be ancestral to tip,
                    # but new commits on the lane branch mean work is in progress again.
                    lane_branch = _resolve_lane_branch(
                        ctx.orchestrator_root,
                        ctx.task_ref,
                        lane_id,
                        branch_hint=branch_by_lane.get(lane_id),
                    )
                    if not lane_branch:
                        ctx.log(
                            "INFO",
                            "landing_recovery_branch_unresolved",
                            lane=lane_id,
                            sha=sha,
                        )
                        continue
                    contained = _lane_branch_contained_in(ctx.orchestrator_root, sha, lane_branch)
                    if contained is not True:
                        ctx.log(
                            "INFO",
                            "landing_recovery_lane_not_contained",
                            lane=lane_id,
                            sha=sha,
                            branch=lane_branch,
                            contained=contained,
                        )
                        continue

                    if not _close_merged(
                        lane_id,
                        notes="Recovered by orchestrator daemon: landing record present, status not MERGED.",
                        sha=sha,
                    ):
                        continue

                    ctx.log("INFO", "lane_landing_recovered", lane=lane_id, sha=sha)
                    _refresh_after_recovery(lane_id)
                    recovered.append(lane_id)
                    continue

                # Present-but-invalid landing (missing/non-full SHA): treat as
                # absent so the no-record heal path can re-record. continue would
                # permanently strand the lane once tip has no unmerged commits.
                ctx.log(
                    "WARN",
                    "landing_recovery_invalid_landing",
                    lane=lane_id,
                    commit_sha=sha or None,
                )

            # --- Sub-arm 2: no valid landing record, status not MERGED (H3) ---
            # Re-entry via poll requires unmerged commits; post-merge those are
            # gone. Unconsumed merge_ready is the discriminator for a real
            # landed-but-unrecorded intake (consume-after-record leaves it).
            ready_report = _unconsumed_merge_ready_report(lane_id)
            if ready_report is None:
                continue

            lane_branch = _resolve_lane_branch(
                ctx.orchestrator_root,
                ctx.task_ref,
                lane_id,
                branch_hint=branch_by_lane.get(lane_id),
            )
            if not lane_branch:
                ctx.log(
                    "INFO",
                    "landing_recovery_branch_unresolved",
                    lane=lane_id,
                    sha=tip,
                )
                continue
            contained = _lane_branch_contained_in(ctx.orchestrator_root, tip, lane_branch)
            if contained is not True:
                ctx.log(
                    "INFO",
                    "landing_recovery_lane_not_contained",
                    lane=lane_id,
                    sha=tip,
                    branch=lane_branch,
                    contained=contained,
                )
                continue

            landed_sha, task_branch = _task_branch_landing(ctx.orchestrator_root)
            if landed_sha is None or not _is_full_commit_sha(landed_sha):
                ctx.log(
                    "ERROR",
                    "landing_recovery_sha_unresolved",
                    lane=lane_id,
                )
                continue

            if not record_lane_landing(
                ctx.task_ref,
                lane_id,
                landed_sha,
                task_branch or "",
                log=ctx.log,
            ):
                ctx.log(
                    "ERROR",
                    "landing_recovery_record_failed",
                    lane=lane_id,
                    sha=landed_sha,
                )
                continue

            if not _close_merged(
                lane_id,
                notes="Recovered by orchestrator daemon: re-recorded landing after ledger write failure.",
                sha=landed_sha,
            ):
                continue

            # Close-cycle ack only after record + MERGED succeeded (mirror intake).
            report_id = ready_report.get("id")
            consume_lane_worker_reports(
                lane_id,
                report_id=int(report_id) if report_id is not None else None,
                task_ref=ctx.task_ref,
            )

            ctx.log(
                "INFO",
                "lane_landing_recovered",
                lane=lane_id,
                sha=landed_sha,
                reason="no_record_unconsumed_merge_ready",
            )
            _refresh_after_recovery(lane_id)
            recovered.append(lane_id)
        except Exception as exc:  # noqa: BLE001 — H6: one lane must not abort the cycle
            ctx.log("ERROR", "landing_recovery_lane_failed", lane=lane_id, error=str(exc))
            continue
    return recovered


def _resolve_verify_twin_blockers(ctx: OrchestratorContext) -> None:
    """Resolve admit-time blockers for finished verify-twin lanes (implementation note S2).

    Observer only: never aborts the cycle. Strict outcome gate — only
    ``finished`` resolves (not the broader ``_SUCCESS_WORKER_REPORT_OUTCOMES``
    set used by the propagation predicate). Reads open blockers via the
    unlimited ``handoff_close_check`` items list (not the capped
    ``get_handoff_state.blockers_open``).
    """
    if ctx.dry_run:
        return

    twin_ids: list[str] = [
        lid for lid in (ctx.m_order or []) if isinstance(lid, str) and "__verify__" in lid
    ]
    if not twin_ids:
        try:
            from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

            listed_raw = manage_worktree_lane(
                operation="list",
                task_ref=ctx.task_ref,
                status="all",
                limit=500,
            )
            if isinstance(listed_raw, str):
                listed_raw = json.loads(listed_raw)
            listed = listed_raw if isinstance(listed_raw, dict) else {}
            if listed.get("ok") is True:
                for row in listed.get("lanes") or []:
                    if not isinstance(row, dict):
                        continue
                    lid = row.get("lane_id")
                    if isinstance(lid, str) and "__verify__" in lid:
                        twin_ids.append(lid)
        except Exception as exc:  # noqa: BLE001 — fail-closed; never abort the cycle
            ctx.log("ERROR", "verify_twin_list_failed", error=str(exc))
            return

    if not twin_ids:
        return

    from workbay_handoff_mcp import handoff_close_check, report_blocker  # noqa: PLC0415

    for twin_id in twin_ids:
        try:
            outcome = _latest_worker_report_outcome(ctx.task_ref, twin_id)
            # STRICT: only "finished" (plan lines 282-285). no_work / no_actionable_work
            # must NOT resolve the task-level admit-time blocker.
            if outcome != "finished":
                continue

            close_raw = handoff_close_check(task_ref=ctx.task_ref)
            if isinstance(close_raw, str):
                close_raw = json.loads(close_raw)
            if not isinstance(close_raw, dict):
                continue
            data = close_raw.get("data") if isinstance(close_raw.get("data"), dict) else close_raw
            checks = data.get("checks") if isinstance(data, dict) else None
            open_blockers = (
                checks.get("open_blockers") if isinstance(checks, dict) else None
            )
            items = (
                open_blockers.get("items") if isinstance(open_blockers, dict) else None
            )
            if not isinstance(items, list):
                continue

            blocker_id: int | None = None
            for row in items:
                if not isinstance(row, dict):
                    continue
                if row.get("lane_id") != twin_id:
                    continue
                status = str(row.get("status") or "").strip().lower()
                if status and status != "open":
                    continue
                raw_id = row.get("id")
                if raw_id is None:
                    continue
                try:
                    blocker_id = int(raw_id)
                except (TypeError, ValueError):
                    continue
                break

            if blocker_id is None:
                continue

            resolve_raw = report_blocker(
                operation="resolve",
                blocker_id=blocker_id,
                task_ref=ctx.task_ref,
                actor={"lane_id": twin_id},
            )
            if isinstance(resolve_raw, str):
                resolve_raw = json.loads(resolve_raw)
            resolve_result = resolve_raw if isinstance(resolve_raw, dict) else {}
            # report_blocker can return {ok:False} without raising (id race,
            # missing blocker). Log success only when the envelope is ok so a
            # failed resolve is not hidden ([AGT-10] degrade loudly).
            if resolve_result.get("ok") is True:
                ctx.log(
                    "INFO",
                    "verify_twin_blocker_resolved",
                    lane_id=twin_id,
                    blocker_id=blocker_id,
                )
            else:
                err = (
                    (resolve_result.get("data") or {}).get("error")
                    if isinstance(resolve_result.get("data"), dict)
                    else None
                ) or resolve_result.get("error") or "resolve returned not-ok"
                ctx.log(
                    "WARNING",
                    "verify_twin_blocker_resolve_failed",
                    lane_id=twin_id,
                    blocker_id=blocker_id,
                    error=str(err),
                )
        except Exception as exc:  # noqa: BLE001 — one twin must not abort the cycle
            ctx.log(
                "ERROR",
                "verify_twin_blocker_resolve_failed",
                lane_id=twin_id,
                error=str(exc),
            )
            continue


def _worker_management_phase(ctx: OrchestratorContext) -> None:
    """Step 4: Check worker status / health, start missing workers, poll merge-ready lanes.

    Per-cycle worker bounds (grok ``max_turns`` / wall-clock timeout) and cross-cycle
    ``token_budget`` are unchanged. Junior grok lanes retain ``--no-subagents`` for
    spend containment (see ``grok_cli.py``).
    """
    ctx.autostart_results = _ensure_lane_workers(
        orchestrator_root=ctx.orchestrator_root,
        task_ref=ctx.task_ref,
        lane_ids=ctx.m_order,
        backend=ctx.backend,
        worker_start_mode=ctx.worker_start_mode,
        worker_reasoning_effort=ctx.worker_reasoning_effort,
        model=ctx.model,
        dry_run=ctx.dry_run,
        log=ctx.log,
        prev_health=ctx.lane_health_prev,
    )
    for row in ctx.autostart_results:
        if isinstance(row, dict) and row.get("reason") == "attention_required":
            ctx.attention_stalls[row["lane_id"]] = ctx.attention_stalls.get(row["lane_id"], 0) + 1
        elif isinstance(row, dict) and row.get("lane_id") in ctx.attention_stalls:
            del ctx.attention_stalls[row["lane_id"]]

    stale_attention = {lane for lane, count in ctx.attention_stalls.items() if count >= 3}

    ctx.has_in_flight = _lane_work_in_flight(ctx.autostart_results, stale_attention_lanes=stale_attention)
    if ctx.has_in_flight:
        ctx.log("INFO", "worker_pool_checked", results=ctx.autostart_results)

    ready_lanes = _poll_merge_ready_lanes(ctx.orchestrator_root, ctx.task_ref, ctx.m_order)

    # Explicit recovery arm ([DOM-06] / internal): record-without-MERGED
    # cannot self-heal via re-intake (clean lanes are excluded from ordered_ready).
    # Recovery MUST run before the noop-completer: the no-record heal sub-arm
    # keys on an unconsumed merge-ready report, and the completer consumes that
    # report for clean lanes — completer-first would eat the heal evidence and
    # re-strand a record-failure lane.
    recovered_landings = _recover_stranded_landing_lanes(ctx)
    if recovered_landings:
        ctx.log("INFO", "landing_recovery_completed", lanes=recovered_landings)

    noop_completed = _complete_already_satisfied_merge_ready_lanes(
        ctx.orchestrator_root,
        ctx.task_ref,
        ctx.m_order,
        dry_run=ctx.dry_run,
        log=ctx.log,
    )
    if noop_completed:
        ctx.log("INFO", "merge_ready_noop_completed", lanes=noop_completed)

    ctx.ordered_ready = _sort_by_manifest_merge_order(ready_lanes, ctx.m_order)
    ctx.log("INFO", "poll_complete", ready_lanes=ctx.ordered_ready)

    # Recovery and noop both advance terminal progress; reset the stall tripwire
    # so a recovery-only cycle does not count toward plan_stall_threshold_reached.
    if noop_completed or recovered_landings:
        ctx.plan_stall_count = 0
    elif not ready_lanes and not ctx.guidance_results and not ctx.plan_dispatch and not ctx.has_in_flight:
        ctx.plan_stall_count += 1
        if ctx.plan_stall_count >= 3:
            ctx.log("ERROR", "plan_stall_threshold_reached")
            raise RuntimeError("plan_stall_threshold_reached")
    else:
        ctx.plan_stall_count = 0

    # Write side of the blocked-lane heartbeat: conclusively close aged, dead
    # blocked lanes so they cannot accumulate unbounded ([RES-07] Slice-3).
    _reap_blocked_lanes_maintenance(ctx)


def _lane_intake_phase(ctx: OrchestratorContext) -> None:
    """Step 5: Intake merge-ready lanes, refresh downstream, verify, and check close readiness."""
    from lane_manifest import downstream_lanes  # noqa: PLC0415
    from workbay_handoff_mcp import (  # noqa: PLC0415
        handoff_close_check,
        record_decision,
        record_test_result,
    )

    # Resolve finished verify-twin blockers BEFORE the cycle's close check so
    # the same intake handoff_close_check sees ready_to_close flip (implementation note S2).
    _resolve_verify_twin_blockers(ctx)

    for lane_id in ctx.ordered_ready:
        ctx.log("INFO", "intake_start", lane=lane_id)
        intake_ok = _intake_lane(
            ctx.orchestrator_root,
            ctx.task_ref,
            lane_id,
            dry_run=ctx.dry_run,
        )
        decision_text = (
            f"Orchestrator daemon intaked lane {lane_id} successfully."
            if intake_ok
            else f"Orchestrator daemon failed to intake lane {lane_id}."
        )
        if not ctx.dry_run:
            record_decision(
                session=f"{ctx.task_ref}-orchestrator-daemon",
                decision=decision_text,
                rationale=f"Automated intake cycle for merge-ready lane {lane_id}.",
            )
        ctx.log("INFO", "intake_complete", lane=lane_id, success=intake_ok)

        if not intake_ok:
            continue

        # Propagate refresh/verify only when the lane actually terminalized
        # (MERGED written) or dry_run. Containment / record failures leave
        # write_merged False so dependents are not refreshed from a non-landed tip.
        write_merged = False
        if not ctx.dry_run:
            cursor = _complete_lane_plan_cursor(ctx.task_ref, lane_id)
            if cursor is not None:
                ctx.log("INFO", "plan_cursor_completed", lane=lane_id, plan_item_id=cursor.get("plan_item_id"))
            from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

            from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
                consume_lane_worker_reports,
                manage_worktree_lane,
            )

            # Record-first ([GRPH-14] / [DOM-06]): capture the task-branch tip the
            # lane just landed on BEFORE the terminal MERGED write, so the
            # moment-1 predicate always has evidence for a lane that reached MERGED.
            #
            # Consume is deferred until record + MERGED succeed so a withhold
            # keeps the merge_ready report for poll re-entry when unmerged work
            # remains. Post-merge ledger failure (no unmerged commits) is healed
            # by ``_recover_stranded_landing_lanes`` no-record sub-arm.
            #
            # Failure arms (H3):
            # - No usable SHA (git failed / invalid shape): skip the record and
            #   still write MERGED — post-merge the lane has no unmerged commits
            #   and can never re-enter ordered_ready; withholding MERGED wedges it.
            # - SHA in hand but containment fails: do NOT write MERGED; leave the
            #   merge_ready report unacked for poll re-entry (unmerged commits).
            # - Ledger write fails with a trusted SHA: do NOT write MERGED; the
            #   recovery no-record sub-arm re-attempts record then MERGED when an
            #   unconsumed merge_ready report remains and containment still holds.
            landed_sha, task_branch = _task_branch_landing(ctx.orchestrator_root)
            write_merged = True
            if landed_sha is None:
                ctx.log("ERROR", "landing_sha_unresolved", lane=lane_id)
            else:
                lane_branch = _resolve_lane_branch(ctx.orchestrator_root, ctx.task_ref, lane_id)
                contained = (
                    _lane_branch_contained_in(ctx.orchestrator_root, landed_sha, lane_branch)
                    if lane_branch
                    else None
                )
                if contained is not True:
                    # H1/H2: recipe exit-0 without merge, or unresolvable branch —
                    # record NOTHING. Absence is safe; a false landing is not.
                    ctx.log(
                        "ERROR",
                        "landing_not_contained",
                        lane=lane_id,
                        sha=landed_sha,
                        branch=lane_branch,
                        contained=contained,
                    )
                    write_merged = False
                elif record_lane_landing(
                    ctx.task_ref,
                    lane_id,
                    landed_sha,
                    task_branch,
                    log=ctx.log,
                ):
                    ctx.log(
                        "INFO",
                        "lane_landing_recorded",
                        lane=lane_id,
                        sha=landed_sha,
                        branch=task_branch,
                    )
                else:
                    # H3: valid SHA but ledger write failed — recovery re-attempts.
                    write_merged = False

            if write_merged:
                close_raw = manage_worktree_lane(
                    operation="close",
                    lane_id=lane_id,
                    status=LaneStatus.MERGED,
                    notes="Auto-closed by orchestrator daemon post-intake.",
                    task_ref=ctx.task_ref,
                )
                if isinstance(close_raw, str):
                    close_raw = json.loads(close_raw)
                close_payload = close_raw if isinstance(close_raw, dict) else {}
                # Mirror recovery: only claim terminal success when close ok.
                if close_payload.get("ok") is True:
                    ctx.log("INFO", "lane_auto_merged", lane=lane_id)
                    # Close-cycle ack only after record + MERGED succeeded.
                    consume_lane_worker_reports(lane_id, task_ref=ctx.task_ref)
                else:
                    ctx.log(
                        "ERROR",
                        "lane_auto_merge_failed",
                        lane=lane_id,
                        payload=close_payload,
                    )
                    # Status may still be non-MERGED; recovery heals next cycle.
                    # Do not refresh dependents from a non-terminal close.
                    write_merged = False

        # Gate merge-propagation on the same condition as MERGED ([DOM-06]).
        if not (ctx.dry_run or write_merged):
            continue

        deps = downstream_lanes(ctx.task_ref, lane_id)
        if deps:
            ctx.log("INFO", "refresh_start", lane=lane_id, downstream=deps)
            refresh_results = _refresh_downstream(
                ctx.orchestrator_root,
                ctx.task_ref,
                lane_id,
                deps,
                dry_run=ctx.dry_run,
            )
            ctx.log("INFO", "refresh_complete", lane=lane_id, results=refresh_results)

        ctx.log("INFO", "verify_start", lane=lane_id)
        verify_ok = _run_cross_lane_verify(
            ctx.orchestrator_root,
            ctx.task_ref,
            lane_id,
            dry_run=ctx.dry_run,
        )
        if not ctx.dry_run:
            record_test_result(
                session=f"{ctx.task_ref}-orchestrator-daemon",
                command=f"make lane-check TASK={ctx.task_ref} LANE={lane_id}",
                passed=verify_ok,
                result="Cross-lane verification passed." if verify_ok else "Cross-lane verification failed.",
            )
        ctx.log("INFO", "verify_complete", lane=lane_id, passed=verify_ok)

    close_check = _require_dict_payload(
        handoff_close_check(task_ref=ctx.task_ref),
        source=f"handoff_close_check({ctx.task_ref})",
    )
    # v2 envelopes put ready_to_close under data only (no top-level mirror).
    # Fall back to the raw dict for callers that already pass an unwrapped payload.
    ctx.ready_to_close = bool((close_check.get("data") or close_check).get("ready_to_close"))
    ctx.runtime_failure_count = 0
    ctx.log("INFO", "close_check_complete", ready_to_close=ctx.ready_to_close)
    ctx.log("INFO", "cycle_end", intaked=ctx.ordered_ready, guidance=len(ctx.guidance_results))


def _build_orchestrator_context(
    orchestrator_root: Path,
    task_ref: str,
    poll_interval: int,
    single_pass: bool,
    dry_run: bool,
    backend: str,
    worker_start_mode: str,
    worker_reasoning_effort: str,
    model: str | None,
    state_dir: Path | None = None,
) -> OrchestratorContext:
    """Configure MCP runtime and build an OrchestratorContext ready for the loop."""
    from lane_manifest import merge_order as manifest_merge_order  # noqa: PLC0415
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime  # noqa: PLC0415

    state_dir = state_dir or orchestrator_root / ".task-state"
    log_dir = orchestrator_root / "logs" / "daemon"
    run_id = str(uuid.uuid4())

    def log(level: str, event: str, **kw: object) -> None:
        _log(log_dir, level, event, run_id=run_id, **kw)

    runtime = RuntimeConfig.for_repo(
        orchestrator_root,
        state_dir=state_dir,
        current_task_path=orchestrator_root / "CURRENT_TASK.json",
        exports_dir=state_dir / "exports",
    )
    configure_runtime(runtime)

    log(
        "INFO",
        "daemon_start",
        task_ref=task_ref,
        single_pass=single_pass,
        backend=backend,
        worker_start_mode=worker_start_mode,
        worker_reasoning_effort=worker_reasoning_effort,
        model=model,
    )

    m_order = manifest_merge_order(task_ref)
    log("INFO", "manifest_loaded", merge_order=m_order)

    return OrchestratorContext(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        state_dir=state_dir,
        log_dir=log_dir,
        m_order=m_order,
        poll_interval=poll_interval,
        single_pass=single_pass,
        dry_run=dry_run,
        backend=backend,
        worker_start_mode=worker_start_mode,
        worker_reasoning_effort=worker_reasoning_effort,
        model=model,
        log=log,
    )


def _run_orchestrator_cycle(ctx: OrchestratorContext) -> int:
    """Run one orchestrator cycle. Returns an exit code or ``-1`` to continue."""
    log = ctx.log
    poll_interval = ctx.poll_interval
    task_ref = ctx.task_ref
    single_pass = ctx.single_pass

    # Cycle-scoped dependency-refusal observability: surface counts
    # accumulated since the previous cycle, then reset so each summary
    # covers exactly one cycle regardless of which exit path it takes.
    log_dependency_refusal_summary(log, reset=True, task_ref=task_ref)

    if _shutdown_requested:
        log("INFO", "daemon_stop", reason="sigterm")
        return 0
    if _is_paused(ctx.state_dir):
        log("INFO", "daemon_paused")
        if single_pass:
            return 0
        # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
        time.sleep(poll_interval)
        return -1  # continue

    log("INFO", "cycle_start")

    try:
        _dispatch_phase(ctx)
    except Exception:
        if single_pass or ctx.dispatch_failure_count >= 3:
            return 1

    try:
        _guidance_phase(ctx)
        _plan_dispatch_phase(ctx)
        _worker_management_phase(ctx)
        _lane_intake_phase(ctx)
    except Exception as exc:
        import traceback

        traceback.print_exc()
        ctx.runtime_failure_count += 1
        log("ERROR", "runtime_phase_failed", error=str(exc), failure_count=ctx.runtime_failure_count)
        if single_pass or ctx.runtime_failure_count >= 3:
            log("ERROR", "terminal_error", reason="runtime_failure")
            return 1
        log("INFO", "poll_sleep", interval=poll_interval)
        # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
        time.sleep(poll_interval)
        return -1  # continue

    if ctx.ready_to_close:
        remaining_plan_items = _remaining_plan_work(task_ref)
        if not remaining_plan_items:
            log("INFO", "task_complete", task_ref=task_ref)
            return 0
        log("INFO", "task_close_blocked_by_plan", remaining=len(remaining_plan_items))
        if single_pass:
            return 1

    if single_pass:
        return 0

    log("INFO", "poll_sleep", interval=poll_interval)
    # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
    time.sleep(poll_interval)
    return -1  # continue


def orchestrator_loop(
    *,
    orchestrator_root: Path,
    task_ref: str,
    poll_interval: int = 60,
    single_pass: bool = False,
    dry_run: bool = False,
    backend: str = "codex-cli",
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
    state_dir: Path | None = None,
) -> int:
    """Main daemon loop.  Returns 0 on clean exit, 1 on failure."""
    ctx = _build_orchestrator_context(
        orchestrator_root,
        task_ref,
        poll_interval,
        single_pass,
        dry_run,
        backend,
        worker_start_mode,
        worker_reasoning_effort,
        model,
        state_dir=state_dir,
    )
    from workbay_orchestrator_mcp.orchestration.daemon_startup import (  # noqa: PLC0415
        emit_daemon_startup_warning,
    )

    emit_daemon_startup_warning("orchestrator", poll_interval=poll_interval)
    # Objective 5: empty-graph override is read once at process start; state it.
    if allow_empty_dependency_graph():
        ctx.log(
            "INFO",
            "WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1 activates depends_on scheduling "
            "over an empty graph = unconstrained dispatch",
        )
    while True:
        result = _run_orchestrator_cycle(ctx)
        if result != -1:
            return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orchestrator daemon: dispatch, intake, refresh, verify.")
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Run the orchestrator loop.")
    run_parser.add_argument("--orchestrator-root", required=True, help="Absolute path to the monorepo root.")
    run_parser.add_argument(
        "--task-ref", required=False, help="MCP task reference. If omitted, infers from active task or manifests."
    )
    run_parser.add_argument("--poll-interval", type=int, default=60, help="Seconds between poll cycles (default: 60).")
    run_parser.add_argument("--single-pass", action="store_true", help="Run one cycle and exit.")
    run_parser.add_argument("--dry-run", action="store_true", help="Skip mutating operations.")
    run_parser.add_argument(
        "--backend", default="codex-cli", help="Execution backend for worker spawning (default: codex-cli)."
    )
    run_parser.add_argument("--worker-start-mode", default="mcp", help="Worker session startup mode (default: mcp).")
    run_parser.add_argument(
        "--worker-reasoning-effort", default="auto", help="Reasoning effort for spawned workers (default: auto)."
    )
    run_parser.add_argument("--model", help="Execution model to use for worker spawning.")
    run_parser.add_argument(
        "--state-dir", default=None, help="State directory. Defaults to <orchestrator-root>/.task-state."
    )

    pause_parser = sub.add_parser("pause", help="Pause the daemon.")
    pause_parser.add_argument("--state-dir", required=True)

    resume_parser = sub.add_parser("resume", help="Resume the daemon.")
    resume_parser.add_argument("--state-dir", required=True)

    status_parser = sub.add_parser("status", help="Show daemon status.")
    status_parser.add_argument("--state-dir", required=True)
    status_parser.add_argument("--log-dir", default=None, help="Log directory. Defaults to <state-dir>/../logs/daemon.")

    salvage_parser = sub.add_parser(
        "salvage-and-close",
        help="Freeze a failed lane, classify its changed files, and close it.",
    )
    salvage_parser.add_argument("--orchestrator-root", required=True, help="Absolute path to the monorepo root.")
    salvage_parser.add_argument("--task-ref", required=True, help="MCP task reference.")
    salvage_parser.add_argument("--lane-id", required=True, help="Lane to salvage and close.")
    salvage_parser.add_argument(
        "--dry-run", action="store_true", help="Print salvage groups without mutating MCP state."
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.command == "pause":
        state_dir = Path(args.state_dir).expanduser().resolve()
        daemon_pause(state_dir)
        print("Daemon paused.")
        return 0

    if args.command == "resume":
        state_dir = Path(args.state_dir).expanduser().resolve()
        daemon_resume(state_dir)
        print("Daemon resumed.")
        return 0

    if args.command == "status":
        state_dir = Path(args.state_dir).expanduser().resolve()
        log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else state_dir.parent / "logs" / "daemon"
        status = daemon_status(state_dir, log_dir)
        print(json.dumps(status, indent=2, default=str))
        return 0

    if args.command == "salvage-and-close":
        orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
        state_dir = orchestrator_root / ".task-state"
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime

        runtime = RuntimeConfig.for_repo(
            orchestrator_root,
            state_dir=state_dir,
            current_task_path=orchestrator_root / "CURRENT_TASK.json",
            exports_dir=state_dir / "exports",
        )
        configure_runtime(runtime)
        result = salvage_and_close_lane(
            orchestrator_root,
            args.task_ref,
            args.lane_id,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "run":
        orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
        state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else orchestrator_root / ".task-state"

        lock = OrchestratorLock(state_dir)
        if not lock.acquire():
            print("Another orchestrator daemon is already running.", file=sys.stderr)
            return 1

        signal.signal(signal.SIGTERM, _handle_sigterm)
        try:
            resolved_task = _resolve_task_ref(orchestrator_root, args.task_ref)
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            return 1

        try:
            return orchestrator_loop(
                orchestrator_root=orchestrator_root,
                task_ref=resolved_task,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                dry_run=args.dry_run,
                backend=args.backend,
                worker_start_mode=args.worker_start_mode,
                worker_reasoning_effort=args.worker_reasoning_effort,
                model=args.model,
                state_dir=state_dir,
            )
        finally:
            lock.release()

    # No subcommand -- print help
    _parse_args()
    return 1


if __name__ == "__main__":
    sys.exit(main())
