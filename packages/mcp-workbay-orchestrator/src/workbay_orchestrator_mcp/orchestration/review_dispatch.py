#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from importlib import import_module
from pathlib import Path
from typing import Any, Literal

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lane_manifest import lane_route_hints, list_task_refs, route_patterns

_handoff_read_shapes = import_module(f"{__package__}.handoff_read_shapes" if __package__ else "handoff_read_shapes")

from workbay_orchestrator_mcp.lanes import lane_communication
from workbay_orchestrator_mcp.orchestration.orchestrator_helpers import _require_dict_payload

ISSUE_KIND_LABELS: dict[str, dict[str, str]] = {
    "review_findings": {
        "subject": "open review findings",
        "verb": "Resolve",
        "noun": "review findings",
    },
    "blockers": {
        "subject": "open blockers",
        "verb": "Investigate",
        "noun": "blockers",
    },
    "actions": {
        "subject": "pending next actions",
        "verb": "Pick up",
        "noun": "next actions",
    },
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch open handoff issues to the correct worker lanes.")
    parser.add_argument("--orchestrator-root", required=True)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _route_lane(task_ref: str, file_path: str) -> str | None:
    patterns = route_patterns(task_ref)
    normalized_path = file_path.strip()
    for pattern, lane_id in patterns:
        if normalized_path == pattern or normalized_path.startswith(pattern):
            return str(lane_id)
    return None


def _collect_text_lane_candidates(task_ref: str, text: str) -> set[str]:
    normalized_text = " ".join(text.lower().split())
    candidates: set[str] = set()
    for lane_id, hints in lane_route_hints(task_ref).items():
        for hint in hints:
            escaped = re.escape(hint.lower())
            if re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", normalized_text):
                candidates.add(lane_id)
                break
    for pattern, lane_id in route_patterns(task_ref):
        if pattern in text:
            candidates.add(lane_id)
    return candidates


def _route_issue(task_ref: str, issue_kind: str, issue: dict[str, Any]) -> str | None:
    if issue_kind == "review_findings":
        return _route_lane(task_ref, str(issue.get("file_path", "")))

    text = str(issue.get("description") or issue.get("action") or "")
    if not text.strip():
        return None
    candidates = _collect_text_lane_candidates(task_ref, text)
    if len(candidates) == 1:
        return next(iter(candidates))
    if len(candidates) > 1:
        issue_id = issue.get("finding_id") or issue.get("id") or "unknown"
        print(
            f"review-dispatch: ambiguous routing for {issue_kind} {issue_id}: matched {sorted(candidates)}, skipping",
            file=sys.stderr,
        )
    return None


def _format_location(finding: dict[str, Any]) -> str:
    file_path = str(finding.get("file_path", ""))
    line_start = finding.get("line_start")
    if isinstance(line_start, int):
        return f"{file_path}:{line_start}"
    return file_path


def _format_issue_ref(issue_kind: str, issue: dict[str, Any]) -> str:
    if issue_kind == "review_findings":
        return str(issue["finding_id"])
    return f"#{issue['id']}"


def _format_issue_detail(issue_kind: str, issue: dict[str, Any]) -> str:
    if issue_kind == "review_findings":
        return f"{issue['finding_id']} [{issue['severity']}] {_format_location(issue)}"
    if issue_kind == "blockers":
        return f"#{issue['id']} {str(issue.get('description', '')).strip()}"
    priority = issue.get("priority")
    priority_label = f" [P{priority}]" if isinstance(priority, int) else ""
    return f"#{issue['id']}{priority_label} {str(issue.get('action', '')).strip()}"


def _format_message(lane_id: str, issue_kind: str, items: list[dict[str, Any]]) -> tuple[str, str]:
    labels = ISSUE_KIND_LABELS[issue_kind]
    item_refs = ", ".join(_format_issue_ref(issue_kind, item) for item in items)
    details = "; ".join(_format_issue_detail(issue_kind, item) for item in items)
    subject = f"{lane_id} {labels['subject']}"
    message = (
        f"{labels['verb']} open handoff {labels['noun']} assigned from orchestrator root: {item_refs}. "
        f"Stamped to this lane in MCP so they appear in lane activity. Details: {details}."
    )
    return subject, message


def _run_make_dispatch(
    orchestrator_root: Path, task_ref: str, lane_id: str, subject: str, message: str, dry_run: bool
) -> None:
    cmd = ["make", "lane-dispatch", f"TASK={task_ref}", f"LANE={lane_id}"]
    env = os.environ.copy()
    env["SUBJECT"] = subject
    env["MESSAGE"] = message
    if dry_run:
        env["DRY_RUN"] = "1"
    subprocess.run(cmd, cwd=orchestrator_root, env=env, check=True)


def _has_open_dispatch(task_ref: str, lane_id: str, subject: str) -> bool:
    from workbay_handoff_mcp.enums import LaneMessageDirection, MessageStatus  # noqa: PLC0415

    payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            status=MessageStatus.OPEN,
            limit=100,
        ),
        source=f"lane_communication(list dispatch:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Unable to list open lane messages for {lane_id}: {payload}")
    messages = payload.get("messages", [])
    if not isinstance(messages, list):
        return False
    for message in messages:
        if not isinstance(message, dict):
            continue
        if (
            message.get("direction") == LaneMessageDirection.ORCHESTRATOR_TO_WORKER
            and message.get("subject") == subject
        ):
            return True
    return False


def _load_open_handoff_items(task_ref: str) -> dict[str, list[dict[str, Any]]]:
    payload = _require_dict_payload(
        _handoff_read_shapes.read_handoff_state(**_handoff_read_shapes.open_handoff_items_kwargs(task_ref)),
        source=f"get_handoff_state(open items:{task_ref})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Unable to load open handoff state: {payload}")

    issue_sets: dict[str, list[dict[str, Any]]] = {}
    for issue_kind, key in (
        ("review_findings", "findings_open"),
        ("blockers", "blockers_open"),
        ("actions", "actions_pending"),
    ):
        rows = payload.get(key, [])
        if not isinstance(rows, list):
            raise RuntimeError(f"handoff state returned non-list payload for {key}")
        issue_sets[issue_kind] = [row for row in rows if isinstance(row, dict)]
    return issue_sets


def _normalize_action_status(value: object) -> Literal["pending", "done", "skipped"]:
    from workbay_handoff_mcp.enums import ActionStatus  # noqa: PLC0415

    if value == ActionStatus.DONE:
        return "done"
    if value == ActionStatus.SKIPPED:
        return "skipped"
    return "pending"


def _stamp_issue_to_lane(issue_kind: str, issue: dict[str, Any], lane_id: str, dispatch_session: str) -> None:
    from workbay_handoff_mcp import (  # noqa: PLC0415
        report_blocker,
        update_next_actions,
        update_review_finding,
    )
    from workbay_handoff_mcp.api import WriteActorInput  # noqa: PLC0415

    lane_actor = WriteActorInput(lane_id=lane_id)
    if issue_kind == "review_findings":
        result = _require_dict_payload(
            update_review_finding(
                status="open",
                finding_id=str(issue["finding_id"]),
                session=dispatch_session,
                actor=lane_actor,
            ),
            source=f"update_review_finding({issue['finding_id']})",
        )
        if result.get("ok") is not True:
            raise RuntimeError(f"Unable to stamp finding {issue['finding_id']} to lane {lane_id}: {result}")
        return

    if issue_kind == "blockers":
        result = _require_dict_payload(
            report_blocker(
                operation="reopen",
                blocker_id=int(issue["id"]),
                actor=lane_actor,
            ),
            source=f"report_blocker(reopen:{issue['id']})",
        )
        if result.get("ok") is not True:
            raise RuntimeError(f"Unable to stamp blocker #{issue['id']} to lane {lane_id}: {result}")
        return

    result = _require_dict_payload(
        update_next_actions(
            operation="update",
            action_id=int(issue["id"]),
            status=_normalize_action_status(issue.get("status")),
            actor=lane_actor,
        ),
        source=f"update_next_actions({issue['id']})",
    )
    if result.get("ok") is not True:
        raise RuntimeError(f"Unable to stamp action #{issue['id']} to lane {lane_id}: {result}")


def main() -> int:
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime, record_decision  # noqa: PLC0415

    args = _parse_args()
    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    runtime = RuntimeConfig.for_repo(orchestrator_root)
    configure_runtime(runtime)

    if args.task_ref not in list_task_refs():
        print(
            json.dumps(
                {
                    "ok": False,
                    "message": f"No lane manifest found for task '{args.task_ref}'. "
                    "Add config/lane-orchestration/<task-ref>.json before dispatching handoff items.",
                },
                indent=2,
            )
        )
        return 1

    open_items = _load_open_handoff_items(args.task_ref)
    pending_by_kind_and_lane: dict[str, dict[str, list[dict[str, Any]]]] = {
        issue_kind: defaultdict(list) for issue_kind in ISSUE_KIND_LABELS
    }
    already_assigned: dict[str, int] = {issue_kind: 0 for issue_kind in ISSUE_KIND_LABELS}
    unmatched: list[dict[str, Any]] = []

    for issue_kind, items in open_items.items():
        for item in items:
            if item.get("lane_id"):
                already_assigned[issue_kind] += 1
                continue
            lane_id = _route_issue(args.task_ref, issue_kind, item)
            if lane_id is None:
                unmatched.append(
                    {
                        "issue_kind": issue_kind,
                        "id": item.get("finding_id") or item.get("id"),
                        "preview": str(item.get("description") or item.get("action") or item.get("file_path") or ""),
                    }
                )
                continue
            pending_by_kind_and_lane[issue_kind][lane_id].append(item)

    summary = {
        "task_ref": args.task_ref,
        "dry_run": args.dry_run,
        "already_assigned": already_assigned,
        "open_items": {issue_kind: len(items) for issue_kind, items in open_items.items()},
        "pending_dispatch": {
            issue_kind: {lane_id: len(items) for lane_id, items in sorted(pending_by_kind_and_lane[issue_kind].items())}
            for issue_kind in ISSUE_KIND_LABELS
        },
        "reused_open_messages": [],
        "unmatched": unmatched,
    }

    if not any(pending_by_kind_and_lane[issue_kind] for issue_kind in ISSUE_KIND_LABELS):
        message = "No unassigned open handoff items to dispatch."
        if unmatched:
            message = "No routeable unassigned open handoff items to dispatch."
        print(json.dumps({"ok": True, "message": message, **summary}, indent=2))
        return 0

    dispatch_session = f"{args.task_ref}-handoff-dispatch"
    dispatched: dict[str, dict[str, list[str]]] = {issue_kind: {} for issue_kind in ISSUE_KIND_LABELS}
    for issue_kind in ISSUE_KIND_LABELS:
        for lane_id, items in sorted(pending_by_kind_and_lane[issue_kind].items()):
            subject, message = _format_message(lane_id, issue_kind, items)
            has_open_dispatch = _has_open_dispatch(args.task_ref, lane_id, subject)
            if has_open_dispatch:
                summary["reused_open_messages"].append(f"{issue_kind}:{lane_id}")
            elif not args.dry_run:
                _run_make_dispatch(orchestrator_root, args.task_ref, lane_id, subject, message, args.dry_run)
            dispatched[issue_kind][lane_id] = [_format_issue_ref(issue_kind, item) for item in items]
            if args.dry_run:
                continue
            for item in items:
                _stamp_issue_to_lane(issue_kind, item, lane_id, dispatch_session)

    if not args.dry_run:
        decision = _require_dict_payload(
            record_decision(
                session=dispatch_session,
                decision="Dispatched open handoff items from orchestrator root to worker lanes.",
                rationale="; ".join(
                    f"{issue_kind} {lane_id}: {', '.join(item_refs)}"
                    for issue_kind in ISSUE_KIND_LABELS
                    for lane_id, item_refs in sorted(dispatched[issue_kind].items())
                ),
            ),
            source="record_decision(review_dispatch)",
        )
        if decision.get("ok") is not True:
            raise RuntimeError(f"Unable to record handoff dispatch decision: {decision}")

    payload: dict[str, Any] = {"ok": True, **summary, "dispatched": dispatched}
    if unmatched:
        payload["message"] = "Dispatched routeable open handoff items. Some items still need manual routing."
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
