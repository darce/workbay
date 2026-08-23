"""Worker-guidance resolution: classify, apply, and cycle through worker guidance messages."""

from __future__ import annotations

import sys
from enum import StrEnum
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from orchestrator_helpers import (
    _combined_text,
    _json_list_text,
    _message_timestamp,
    _normalize_text,
    _require_dict_payload,
)

# ---------------------------------------------------------------------------
# Guidance markers
# ---------------------------------------------------------------------------

_RESOLVED_MARKERS = (
    "already resolved",
    "already covered",
    "already present",
    "already correct",
    "already appears",
    "already wired",
    "no code changes were warranted",
    "no lane-owned code changes were warranted",
    "no stale fallback wiring was found",
    "appears already resolved",
    "work appears present already",
    "existing coverage",
    "substantially covered",
    "no code changes were needed",
    "already fixed",
    "work already done",
    "verification passed",
)

_REMAINING_WORK_MARKERS = (
    "remaining domain implementation target",
    "highest-priority open lane-owned gap",
    "open domain work still appears",
    "remaining open frontend slice",
    "remaining slice",
    "next slice",
    "still appears to be",
)

_ENV_BLOCKER_MARKERS = (
    "read-only",
    "sandbox",
    "writable temp directory",
    "no usable temporary directory",
    "mypy is not available",
    "mypy was unavailable",
    "postgresql is not running",
    "permissionerror",
    "vendor is a symlink",
    "vendor/bin/phpunit",
    "vendor/bin/phpstan",
    "composer install",
    "npm install",
    "node_modules",
    "command not found",
    "exit with code 127",
)

GUIDANCE_STALL_THRESHOLD = 3


# ---------------------------------------------------------------------------
# GuidanceResolution
# ---------------------------------------------------------------------------


class GuidanceResolutionKind(StrEnum):
    """Closed set of resolution kinds emitted by the guidance classifier.

    internal: replaces ad-hoc magic-string comparisons on
    ``GuidanceResolution.kind`` so new kinds fail fast at construction and at
    the (exhaustive) comparison sites.
    """

    MESSAGE = "message"
    REVIEW = "review"
    REDISPATCH = "redispatch"
    BLOCKED = "blocked"
    FATAL_ERROR = "fatal_error"


class GuidanceResolution:
    def __init__(
        self,
        *,
        kind: GuidanceResolutionKind | str,
        lane_id: str,
        worker_message_id: int,
        latest_report_id: int | None = None,
        decision: str = "",
        rationale: str | None = None,
        lane_status: str = "review",
        lane_notes: str | None = None,
        dispatch_subject: str | None = None,
        dispatch_message: str | None = None,
        close_dispatch_ids: tuple[int, ...] = (),
        error: str | None = None,
    ) -> None:
        # Coerce plain strings so downstream callers always see the StrEnum.
        self.kind: GuidanceResolutionKind = GuidanceResolutionKind(kind)
        self.lane_id = lane_id
        self.worker_message_id = worker_message_id
        self.latest_report_id = latest_report_id
        self.decision = decision
        self.rationale = rationale
        self.lane_status = lane_status
        self.lane_notes = lane_notes
        self.dispatch_subject = dispatch_subject
        self.dispatch_message = dispatch_message
        self.close_dispatch_ids = close_dispatch_ids
        self.error = error


# ---------------------------------------------------------------------------
# MCP query helpers
# ---------------------------------------------------------------------------


def _list_open_worker_guidance(task_ref: str) -> list[dict[str, Any]]:
    from workbay_orchestrator_mcp.lanes import lane_communication  # noqa: PLC0415

    payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            status="open",
            limit=200,
            fields="id,lane_id,session,direction,subject,message,status,created_at,updated_at",
        ),
        source="lane_communication(list worker guidance)",
    )
    if payload.get("ok") is not True:
        raise RuntimeError("Failed to list lane messages.")
    rows = payload.get("messages", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict) and row.get("direction") == "worker_to_orchestrator"]


def _dedupe_worker_guidance_messages(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the newest open worker guidance message per lane."""
    latest_by_lane: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        lane_id = _normalize_text(row.get("lane_id"))
        if not lane_id:
            continue
        current = latest_by_lane.get(lane_id)
        candidate_key = (_message_timestamp(row), int(row.get("id") or 0))
        current_key = (
            (_message_timestamp(current), int(current.get("id") or 0)) if isinstance(current, dict) else ("", 0)
        )
        if current is None or candidate_key >= current_key:
            latest_by_lane[lane_id] = row
    return list(latest_by_lane.values())


def _list_open_dispatch_messages(task_ref: str, lane_id: str) -> list[dict[str, Any]]:
    from workbay_orchestrator_mcp.lanes import lane_communication  # noqa: PLC0415

    payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            status="open",
            limit=200,
            fields="id,direction",
        ),
        source=f"lane_communication(list dispatch messages:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lane messages for {lane_id}.")
    rows = payload.get("messages", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict) and row.get("direction") == "orchestrator_to_worker"]


def _latest_lane_report(task_ref: str, lane_id: str, *, session: str | None = None) -> dict[str, Any] | None:
    from workbay_orchestrator_mcp.lanes import worker_reports  # noqa: PLC0415

    payload = _require_dict_payload(
        worker_reports(
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            limit=20,
            fields="id,session,summary,blockers_json",
        ),
        source=f"worker_reports(list:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list worker reports for {lane_id}.")
    reports = payload.get("reports", [])
    if not isinstance(reports, list):
        return None
    if session:
        for report in reports:
            if isinstance(report, dict) and report.get("session") == session:
                return report
    for report in reports:
        if isinstance(report, dict):
            return report
    return None


def _lane_row(task_ref: str, lane_id: str) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    payload = _require_dict_payload(
        manage_worktree_lane(operation="list", task_ref=task_ref, status="all", limit=200),
        source=f"manage_worktree_lane(list:{task_ref})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lanes for {task_ref}.")
    for lane in payload.get("lanes", []):
        if isinstance(lane, dict) and lane.get("lane_id") == lane_id:
            return lane
    raise RuntimeError(f"Lane {lane_id} not found for task {task_ref}.")


def _lane_activity(task_ref: str, lane_id: str) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import get_lane_activity  # noqa: PLC0415

    payload = _require_dict_payload(
        get_lane_activity(lane_id=lane_id, task_ref=task_ref, limit_actions=50),
        source=f"get_lane_activity({lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to fetch lane activity for {lane_id}.")
    return payload


def _pending_lane_actions(activity: dict[str, Any]) -> list[dict[str, Any]]:
    rows = activity.get("actions", [])
    if not isinstance(rows, list):
        return []
    pending = [row for row in rows if isinstance(row, dict) and row.get("status") == "pending"]
    return sorted(pending, key=lambda row: (int(row.get("priority", 100)), int(row.get("id", 0))))


# ---------------------------------------------------------------------------
# Guidance classification and resolution
# ---------------------------------------------------------------------------


def _resolve_next_assignment(
    task_ref: str, lane_id: str, activity: dict[str, Any], text: str
) -> tuple[str, str] | None:
    pending_actions = _pending_lane_actions(activity)
    if pending_actions:
        action = pending_actions[0]
        return (
            f"{lane_id} next assignment",
            _normalize_text(action.get("action")),
        )

    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from orchestrator_guidance_policy import resolve_assignment as resolve_policy_assignment

    policy_assignment = resolve_policy_assignment(task_ref, lane_id, text, activity)
    if policy_assignment is not None:
        return policy_assignment

    lane = activity.get("lane", {})
    if any(marker in text for marker in _REMAINING_WORK_MARKERS):
        objective = _normalize_text(lane.get("objective"))
        if objective:
            return (f"{lane_id} next assignment", objective)
    return None


def _classify_guidance(
    *,
    task_ref: str,
    worker_message: dict[str, Any],
    latest_report: dict[str, Any] | None,
    activity: dict[str, Any],
    open_dispatches: list[dict[str, Any]],
) -> GuidanceResolution:
    lane_id = _normalize_text(worker_message.get("lane_id"))
    worker_message_id_raw = worker_message.get("id")
    if worker_message_id_raw is None:
        raise RuntimeError("Worker guidance message is missing an id.")
    worker_message_id = int(worker_message_id_raw)
    latest_report_id = (
        int(latest_report["id"]) if isinstance(latest_report, dict) and latest_report.get("id") is not None else None
    )
    combined = _combined_text(
        worker_message.get("subject"),
        worker_message.get("message"),
        latest_report.get("summary") if isinstance(latest_report, dict) else "",
        _json_list_text(latest_report.get("blockers_json")) if isinstance(latest_report, dict) else "",
    )
    close_dispatch_ids = tuple(
        int(row["id"]) for row in open_dispatches if isinstance(row, dict) and row.get("id") is not None
    )

    pending_actions = _pending_lane_actions(activity)
    has_resolved_marker = any(marker in combined for marker in _RESOLVED_MARKERS)
    has_env_blocker = any(marker in combined for marker in _ENV_BLOCKER_MARKERS)

    if has_resolved_marker and not pending_actions:
        return GuidanceResolution(
            kind=GuidanceResolutionKind.REVIEW,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Resolved worker guidance for {lane_id} by closing stale work and marking the lane ready for review.",
            rationale="Worker report indicates the assigned lane slice is already satisfied in the current branch state.",
            lane_status="review",
            lane_notes="Orchestrator confirmed the worker guidance reflected already-satisfied lane work.",
            close_dispatch_ids=close_dispatch_ids,
        )

    next_assignment = _resolve_next_assignment(task_ref, lane_id, activity, combined)

    if next_assignment is not None:
        subject, message = next_assignment
        return GuidanceResolution(
            kind=GuidanceResolutionKind.REDISPATCH,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Resolved worker guidance for {lane_id} by dispatching the next lane assignment.",
            rationale="Worker reported the prior slice as satisfied or blocked and identified a concrete remaining lane-owned target.",
            lane_status="active",
            lane_notes="Orchestrator resolved worker guidance and dispatched the next lane-owned slice.",
            dispatch_subject=subject,
            dispatch_message=message,
            close_dispatch_ids=close_dispatch_ids,
        )

    if has_env_blocker:
        return GuidanceResolution(
            kind=GuidanceResolutionKind.BLOCKED,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Resolved worker guidance for {lane_id} by marking the lane blocked for operator/environment follow-up.",
            rationale="Worker report indicates an environment or sandbox blocker without a safe automatic redispatch target.",
            lane_status="blocked",
            lane_notes="Worker needs a writable or better-provisioned environment before the next lane step can continue.",
            close_dispatch_ids=close_dispatch_ids,
        )

    if combined.strip():
        return GuidanceResolution(
            kind=GuidanceResolutionKind.BLOCKED,
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            latest_report_id=latest_report_id,
            decision=f"Classified unrecognized worker guidance for {lane_id} as blocked (fallback).",
            rationale="Guidance text present but did not match known resolved, redispatch, or environment-blocked patterns.",
            lane_status="blocked",
            lane_notes="Unclassifiable guidance; marked blocked for operator review.",
            dispatch_subject=None,
            dispatch_message=None,
            close_dispatch_ids=close_dispatch_ids,
            error=None,
        )

    return GuidanceResolution(
        kind=GuidanceResolutionKind.FATAL_ERROR,
        lane_id=lane_id,
        worker_message_id=worker_message_id,
        latest_report_id=latest_report_id,
        error=f"Unable to classify worker guidance for lane {lane_id}.",
        decision=f"Failed to resolve worker guidance for {lane_id}.",
        rationale="Guidance message did not match a known resolved, redispatchable, or environment-blocked pattern.",
        close_dispatch_ids=close_dispatch_ids,
    )


def _apply_guidance_resolution(
    *,
    task_ref: str,
    orchestrator_root: Path,
    resolution: GuidanceResolution,
    dry_run: bool = False,
) -> GuidanceResolution:
    from workbay_handoff_mcp import record_decision, update_next_actions  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication, manage_worktree_lane  # noqa: PLC0415

    lane = _lane_row(task_ref, resolution.lane_id)
    if dry_run:
        return resolution

    lane_communication(
        kind="message",
        operation="update",
        message_id=resolution.worker_message_id,
        status="closed",
        task_ref=task_ref,
    )
    for message_id in resolution.close_dispatch_ids:
        lane_communication(
            kind="message",
            operation="update",
            message_id=message_id,
            status="closed",
            task_ref=task_ref,
        )

    from lane_manifest import get_lane_config

    lane_cfg = get_lane_config(task_ref, resolution.lane_id) or {}

    # Derive owner_agent from backend if not already set in DB
    existing_owner = _normalize_text(lane.get("owner_agent"))
    backend = _normalize_text(lane_cfg.get("preferred_backend"))

    owner_agent = existing_owner
    if not owner_agent:
        if backend and "claude" in backend.lower():
            owner_agent = "claude"
        elif backend and "codex" in backend.lower():
            owner_agent = "codex"
        else:
            owner_agent = backend or "codex-subagent"

    manage_worktree_lane(
        operation="upsert",
        task_ref=task_ref,
        lane_id=resolution.lane_id,
        worktree_path=str(lane.get("worktree_path") or ""),
        branch=str(lane.get("branch") or ""),
        title=_normalize_text(lane.get("title")) or None,
        objective=_normalize_text(lane.get("objective")) or None,
        owner_agent=owner_agent,
        status=resolution.lane_status,
        notes=resolution.lane_notes,
    )

    if resolution.kind == GuidanceResolutionKind.REVIEW:
        for action in _pending_lane_actions(_lane_activity(task_ref, resolution.lane_id)):
            action_id = action.get("id")
            if action_id is None:
                continue
            update_next_actions(operation="update", action_id=int(action_id), status="done")

    if resolution.kind == GuidanceResolutionKind.REDISPATCH and resolution.dispatch_message:
        _dispatch_payload: dict | None = None
        try:
            from workbay_handoff_mcp import artifact_index as _art_idx
            from workbay_handoff_mcp.config import RuntimeConfig as _ArtCfg

            _art_config = _ArtCfg.for_repo(orchestrator_root)
            _art_ref = _art_idx.maybe_record_artifact(
                task_ref=task_ref,
                lane_id=resolution.lane_id,
                app_root=None,
                source_kind="guidance-redispatch",
                source_label=f"{resolution.lane_id}-guidance",
                content_type="text/plain",
                summary=str(resolution.dispatch_subject or f"{resolution.lane_id} next assignment"),
                content=resolution.dispatch_message,
                artifact_db_path=_art_config.artifact_db_path,
                min_bytes=_art_config.artifact_index_min_bytes,
                min_lines=_art_config.artifact_index_min_lines,
            )
            if _art_ref is not None:
                _dispatch_payload = {"artifacts": [str(_art_ref["source_id"])]}
        except Exception:  # noqa: BLE001
            pass
        lane_communication(
            kind="message",
            operation="record",
            task_ref=task_ref,
            lane_id=resolution.lane_id,
            session=f"{task_ref}-orchestrator-guidance",
            direction="orchestrator_to_worker",
            subject=resolution.dispatch_subject,
            message=resolution.dispatch_message,
            status="open",
            payload=_dispatch_payload,
        )

    record_decision(
        session=f"{task_ref}-orchestrator-daemon",
        decision=resolution.decision,
        rationale=resolution.rationale,
    )
    return resolution


def _resolve_guidance_cycle(
    orchestrator_root: Path,
    task_ref: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> list[GuidanceResolution]:
    from workbay_handoff_mcp import record_decision

    results: list[GuidanceResolution] = []
    for worker_message in _dedupe_worker_guidance_messages(_list_open_worker_guidance(task_ref)):
        lane_id = _normalize_text(worker_message.get("lane_id"))
        if not lane_id:
            continue
        if callable(log):
            log(
                "INFO",
                "guidance_detected",
                lane=lane_id,
                worker_message_id=int(worker_message.get("id") or 0),
            )
        latest_report = _latest_lane_report(
            task_ref,
            lane_id,
            session=_normalize_text(worker_message.get("session")) or None,
        )
        activity = _lane_activity(task_ref, lane_id)
        open_dispatches = _list_open_dispatch_messages(task_ref, lane_id)
        resolution = _classify_guidance(
            task_ref=task_ref,
            worker_message=worker_message,
            latest_report=latest_report,
            activity=activity,
            open_dispatches=open_dispatches,
        )
        if resolution.kind == GuidanceResolutionKind.FATAL_ERROR:
            if not dry_run:
                record_decision(
                    session=f"{task_ref}-orchestrator-daemon",
                    decision=resolution.decision,
                    rationale=resolution.rationale,
                )
            results.append(resolution)
            continue
        results.append(
            _apply_guidance_resolution(
                task_ref=task_ref,
                orchestrator_root=orchestrator_root,
                resolution=resolution,
                dry_run=dry_run,
            )
        )
    return results
