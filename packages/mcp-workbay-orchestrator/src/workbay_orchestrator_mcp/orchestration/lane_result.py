#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
_SUBPROCESS_TIMEOUT_SECONDS = 300


def _lane_message_available() -> bool:
    return importlib.util.find_spec("workbay_handoff_mcp") is not None


def _record_artifact_lane_message(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    details: str,
    artifact_ref: Any,
) -> None:
    from workbay_handoff_mcp.api import configure_runtime  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication  # noqa: PLC0415

    configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
    lane_communication(
        kind="message",
        operation="record",
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        direction="worker_to_orchestrator",
        message=details,
        subject=f"{lane_id} handoff",
        status="open",
        payload={"artifacts": [str(artifact_ref)]},
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Handle structured lane run results.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("schema", help="Print the JSON schema for codex exec lane results.")

    handoff = subparsers.add_parser("handoff", help="Turn a structured codex result into a lane handoff.")
    handoff.add_argument("--orchestrator-root", required=True)
    handoff.add_argument("--task-ref", required=True)
    handoff.add_argument("--lane-id", required=True)
    handoff.add_argument("--session", required=True)
    handoff.add_argument("--worktree-path", required=True)
    handoff.add_argument("--result-file", required=True)
    handoff.add_argument("--outcome", default=None)
    handoff.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def _schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["handoff_action", "summary", "details", "tests_run", "blockers"],
        "properties": {
            "handoff_action": {
                "type": "string",
                "enum": ["merge_ready", "needs_guidance"],
                "description": "Use merge_ready only when lane-owned code changes were made and are ready for orchestrator review. Use needs_guidance for sandbox failures, verification blockers, already-resolved findings needing orchestrator review, or any case with no merge-ready commit.",
            },
            "summary": {
                "type": "string",
                "minLength": 1,
                "description": "One short sentence the orchestrator can scan quickly.",
            },
            "details": {
                "type": "string",
                "minLength": 1,
                "description": "Concise explanation of what changed or what was verified, plus why the lane is ready or blocked.",
            },
            "tests_run": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Only the commands actually run in this session.",
            },
            "blockers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Concrete blockers or asks for the orchestrator. Use an empty array when none.",
            },
        },
    }


def _load_result(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("Expected JSON object in result file.")
    return payload


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _normalize_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_normalize_text(item) for item in value if _normalize_text(item)]


def _build_report_command(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    result: dict[str, Any],
    outcome: str | None = None,
) -> list[str]:
    action = _normalize_text(result.get("handoff_action"))
    summary = _normalize_text(result.get("summary"))
    details = _normalize_text(result.get("details"))
    tests_run = _normalize_list(result.get("tests_run"))
    blockers = _normalize_list(result.get("blockers"))

    if not action:
        raise RuntimeError("Missing handoff_action in result payload.")
    if not summary:
        raise RuntimeError("Missing summary in result payload.")
    if not details:
        raise RuntimeError("Missing details in result payload.")

    from workbay_orchestrator_mcp._assets import bundled_script_path  # noqa: PLC0415

    report_cmd = [
        str(bundled_script_path("worktree-lane")),
        "report",
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
        "--lane-id",
        lane_id,
        "--session",
        session,
        "--summary",
        summary,
        "--worktree-path",
        str(worktree_path),
    ]
    normalized_outcome = _normalize_text(outcome)
    if normalized_outcome:
        report_cmd.extend(["--outcome", normalized_outcome])
    for test_command in tests_run:
        report_cmd.extend(["--test-command", test_command])
    if details:
        report_cmd.extend(["--message", details])
    if action == "merge_ready":
        report_cmd.append("--merge-ready")
        return report_cmd
    if action == "needs_guidance":
        report_cmd.append("--guidance-request")
        # Guidance requests may describe already-present lane-local state or
        # verification on top of uncommitted files, so they must not hard-fail
        # on a dirty worktree before the orchestrator can intake the report.
        report_cmd.append("--allow-dirty")
        for blocker in blockers:
            report_cmd.extend(["--blocker", blocker])
        return report_cmd
    raise RuntimeError(f"Unsupported handoff_action: {action}")


def _build_command_plan(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    result: dict[str, Any],
    outcome: str | None = None,
) -> list[tuple[list[str], bool]]:
    """Return a list of (command, critical) tuples."""
    action = _normalize_text(result.get("handoff_action"))
    base_make = [
        "make",
        "-f",
        str(orchestrator_root / "Makefile"),
        "-C",
        str(worktree_path),
    ]
    report_cmd = _build_report_command(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        worktree_path=worktree_path,
        result=result,
        outcome=outcome,
    )
    if action == "merge_ready":
        summary = _normalize_text(result.get("summary"))
        return [
            (base_make + ["lane-commit", f"TASK={task_ref}", f"LANE={lane_id}", f"COMMIT_MSG={summary}"], True),
            (report_cmd, True),
            (base_make + ["lane-status", f"TASK={task_ref}", f"LANE={lane_id}"], False),
        ]
    if action == "needs_guidance":
        return [(report_cmd, True)]
    raise RuntimeError(f"Unsupported handoff_action: {action}")


def main() -> int:
    args = _parse_args()
    if args.command == "schema":
        print(json.dumps(_schema(), indent=2))
        return 0

    result_path = Path(args.result_file).expanduser().resolve()
    result = _load_result(result_path)
    commands = _build_command_plan(
        orchestrator_root=Path(args.orchestrator_root).expanduser().resolve(),
        task_ref=args.task_ref,
        lane_id=args.lane_id,
        session=args.session,
        worktree_path=Path(args.worktree_path).expanduser().resolve(),
        result=result,
        outcome=args.outcome,
    )

    if args.dry_run:
        print(json.dumps({"commands": [cmd for cmd, _ in commands]}, indent=2))
        return 0

    for command, critical in commands:
        try:
            completed = subprocess.run(command, check=False, timeout=_SUBPROCESS_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            logger.warning(
                "lane-result: command timed out after %ss: %s",
                _SUBPROCESS_TIMEOUT_SECONDS,
                command,
            )
            if critical:
                return 124
            continue
        if completed.returncode != 0:
            if critical:
                return completed.returncode
            logger.warning("lane-result: non-critical step failed (exit %s), continuing", completed.returncode)

    artifact_ref = result.get("details_artifact_ref")
    if artifact_ref is not None and _lane_message_available():
        details = _normalize_text(result.get("details"))
        try:
            _record_artifact_lane_message(
                orchestrator_root=Path(args.orchestrator_root).expanduser().resolve(),
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                session=args.session,
                details=details,
                artifact_ref=artifact_ref,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("lane-result: artifact-carrying lane message failed: %s", exc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
