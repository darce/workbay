#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lane_manifest import (
    get_lane_config,
    infer_lane_from_branch,
    infer_task_from_branch_or_worktree,
    list_lanes,
    list_task_refs,
    load_manifest,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit lane/task configuration for Makefile helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-tasks")

    list_lanes_parser = subparsers.add_parser("list-lanes")
    list_lanes_parser.add_argument("--task-ref", required=True)

    infer_parser = subparsers.add_parser("infer-lane")
    infer_parser.add_argument("--branch", required=True)
    infer_parser.add_argument("--task-ref")
    infer_parser.add_argument("--worktree-path")
    infer_parser.add_argument("--orchestrator-root")

    infer_task_parser = subparsers.add_parser("infer-task")
    infer_task_parser.add_argument("--branch", required=True)
    infer_task_parser.add_argument("--worktree-path")
    infer_task_parser.add_argument("--orchestrator-root")

    resolve_task_parser = subparsers.add_parser("resolve-task")
    resolve_task_parser.add_argument("--explicit-task")
    resolve_task_parser.add_argument("--active-task")
    resolve_task_parser.add_argument("--sole-task")
    resolve_task_parser.add_argument("--lane-id")
    resolve_task_parser.add_argument("--branch", required=True)
    resolve_task_parser.add_argument("--worktree-path")
    resolve_task_parser.add_argument("--orchestrator-root")
    resolve_task_parser.add_argument("--in-orchestrator-root", action="store_true")

    choose_task_parser = subparsers.add_parser("choose-task")
    choose_task_parser.add_argument("--explicit-task")
    choose_task_parser.add_argument("--active-task")
    choose_task_parser.add_argument("--sole-task")
    choose_task_parser.add_argument("--lane-id")
    choose_task_parser.add_argument("--branch", required=True)
    choose_task_parser.add_argument("--worktree-path")
    choose_task_parser.add_argument("--orchestrator-root")
    choose_task_parser.add_argument("--in-orchestrator-root", action="store_true")

    field_parser = subparsers.add_parser("field")
    field_parser.add_argument("--task-ref", required=True)
    field_parser.add_argument("--lane-id", required=True)
    field_parser.add_argument("--field", required=True)
    field_parser.add_argument("--orchestrator-root")

    return parser.parse_args()


def _quote_args(flag: str, values: list[str]) -> str:
    if not values:
        return ""
    return " ".join(f"{flag} {shlex.quote(value)}" for value in values)


def _escape_make(value: str) -> str:
    return value.replace("$", "$$")


def _print_make_var(name: str, value: str) -> None:
    print(f"{name} := {_escape_make(value)}")


def _field_value(task_ref: str, lane_id: str, field: str, orchestrator_root: str | None) -> str:
    manifest = load_manifest(task_ref)
    lane = get_lane_config(task_ref, lane_id, orchestrator_root=orchestrator_root)
    if lane is None:
        return ""

    test_commands = [str(item) for item in lane.get("test_commands", []) if str(item).strip()]
    owned_paths = [str(item) for item in lane.get("owned_paths", []) if str(item).strip()]
    required_docs = [str(item) for item in lane.get("required_docs", []) if str(item).strip()]
    non_goals = [str(item) for item in lane.get("non_goals", []) if str(item).strip()]
    commit_paths = [str(item) for item in lane.get("commit_paths", []) if str(item).strip()]
    tooling_paths = [str(item) for item in lane.get("tooling_paths", []) if str(item).strip()]
    default_done = str(manifest.get("default_done_definition", ""))
    values = {
        "branch": str(lane.get("branch", "")),
        "worktree_path": str(lane.get("worktree_path", "")),
        "title": str(lane.get("title", "")),
        "objective": str(lane.get("objective", "")),
        "owned_args": _quote_args("--owned-path", owned_paths),
        "doc_args": _quote_args("--required-doc", required_docs),
        "test_args": _quote_args("--test-command", test_commands),
        "test_command_1": test_commands[0] if len(test_commands) >= 1 else "",
        "test_command_2": test_commands[1] if len(test_commands) >= 2 else "",
        "non_goal_args": _quote_args("--non-goal", non_goals),
        "commit_paths": " ".join(commit_paths),
        "commit_subject": str(lane.get("commit_subject", "")),
        "done_definition": str(lane.get("done_definition", lane.get("definition", default_done))),
        "tooling_paths": " ".join(tooling_paths),
    }
    return values.get(field, "")


def _task_candidates_for_lane(lane_id: str | None) -> list[str]:
    tasks: list[str] = list(list_task_refs())
    if not lane_id:
        return tasks
    return [task_ref for task_ref in tasks if lane_id in list_lanes(task_ref)]


def _task_activity_query_sql() -> str:
    return """
    WITH activity AS (
      SELECT task_ref, MAX(updated_at) AS last_activity FROM (
        SELECT task_ref, updated_at FROM handoff_state
        UNION ALL SELECT task_ref, created_at AS updated_at FROM decisions
        UNION ALL SELECT task_ref, created_at AS updated_at FROM blockers
        UNION ALL SELECT task_ref, updated_at FROM next_actions
        UNION ALL SELECT task_ref, verified_at AS updated_at FROM verified_tests
        UNION ALL SELECT task_ref, COALESCE(updated_at, resolved_at, created_at) AS updated_at FROM review_findings
        UNION ALL SELECT task_ref, updated_at FROM worktree_lanes
        UNION ALL SELECT task_ref, created_at AS updated_at FROM worker_reports
        UNION ALL SELECT task_ref, updated_at FROM lane_messages
      )
      GROUP BY task_ref
    )
    SELECT task_ref, last_activity FROM activity
    """


def _task_activity_labels(orchestrator_root: str | None, candidates: list[str]) -> dict[str, str]:
    if not orchestrator_root or not candidates:
        return {}
    db_path = Path(orchestrator_root).expanduser().resolve() / ".task-state" / "handoff.db"
    if not db_path.exists():
        return {}

    labels: dict[str, str] = {}
    try:
        with sqlite3.connect(db_path) as conn:
            for task_ref, last_activity in conn.execute(_task_activity_query_sql()).fetchall():
                task_text = str(task_ref or "").strip()
                if task_text in candidates and last_activity:
                    labels[task_text] = str(last_activity)
    except sqlite3.Error:
        return {}
    return labels


def _recent_manifest_task_refs(orchestrator_root: str | None, lane_id: str | None) -> list[str]:
    candidates = _task_candidates_for_lane(lane_id)
    if not candidates:
        return []
    if not orchestrator_root:
        return candidates

    labels = _task_activity_labels(orchestrator_root, candidates)
    ordered: list[str] = []
    for task_ref in sorted(labels, key=lambda task: labels[task], reverse=True):
        if task_ref in candidates and task_ref not in ordered:
            ordered.append(task_ref)

    for task_ref in candidates:
        if task_ref not in ordered:
            ordered.append(task_ref)
    return ordered


def resolve_task_choice(
    *,
    explicit_task: str | None,
    active_task: str | None,
    sole_task: str | None,
    lane_id: str | None,
    branch: str,
    worktree_path: str | None,
    orchestrator_root: str | None,
    in_orchestrator_root: bool = False,
) -> str:
    supported = set(list_task_refs())

    explicit = str(explicit_task or "").strip()
    if explicit in supported:
        return explicit

    if not in_orchestrator_root:
        inferred = infer_task_from_branch_or_worktree(
            branch,
            worktree_path=worktree_path,
            orchestrator_root=orchestrator_root,
        )
        if inferred:
            return str(inferred)

    lane_matches = _task_candidates_for_lane(lane_id)
    if len(lane_matches) == 1:
        return lane_matches[0]

    active = str(active_task or "").strip()
    if active in supported:
        return active

    sole = str(sole_task or "").strip()
    if sole in supported:
        return sole

    recent = _recent_manifest_task_refs(orchestrator_root, lane_id)
    if len(recent) == 1:
        return recent[0]

    return ""


def choose_task_interactively(
    *,
    explicit_task: str | None,
    active_task: str | None,
    sole_task: str | None,
    lane_id: str | None,
    branch: str,
    worktree_path: str | None,
    orchestrator_root: str | None,
    in_orchestrator_root: bool = False,
) -> str:
    resolved = resolve_task_choice(
        explicit_task=explicit_task,
        active_task=active_task,
        sole_task=sole_task,
        lane_id=lane_id,
        branch=branch,
        worktree_path=worktree_path,
        orchestrator_root=orchestrator_root,
        in_orchestrator_root=in_orchestrator_root,
    )
    if resolved:
        return resolved

    candidates = _recent_manifest_task_refs(orchestrator_root, lane_id)
    if not candidates or not sys.stdin.isatty():
        return ""
    if len(candidates) == 1:
        return candidates[0]

    labels = _task_activity_labels(orchestrator_root, candidates)
    target = lane_id or "lane work"
    print(f"Select task manifest for {target}:", file=sys.stderr)
    for index, task_ref in enumerate(candidates, start=1):
        suffix = f" (last activity: {labels[task_ref]})" if task_ref in labels else ""
        print(f"  {index}. {task_ref}{suffix}", file=sys.stderr)

    while True:
        response = input("Task number (blank to cancel): ").strip()
        if not response:
            return ""
        if response.isdigit():
            index = int(response)
            if 1 <= index <= len(candidates):
                return candidates[index - 1]
        print("Invalid selection.", file=sys.stderr)


def main() -> int:
    args = _parse_args()
    if args.command == "list-tasks":
        print(" ".join(list_task_refs()))
        return 0
    if args.command == "list-lanes":
        print(" ".join(list_lanes(args.task_ref)))
        return 0
    if args.command == "infer-lane":
        print(infer_lane_from_branch(args.branch, args.task_ref))
        return 0
    if args.command == "infer-task":
        print(
            infer_task_from_branch_or_worktree(
                args.branch,
                worktree_path=args.worktree_path,
                orchestrator_root=args.orchestrator_root,
            )
        )
        return 0
    if args.command == "resolve-task":
        print(
            resolve_task_choice(
                explicit_task=args.explicit_task,
                active_task=args.active_task,
                sole_task=args.sole_task,
                lane_id=args.lane_id,
                branch=args.branch,
                worktree_path=args.worktree_path,
                orchestrator_root=args.orchestrator_root,
                in_orchestrator_root=args.in_orchestrator_root,
            )
        )
        return 0
    if args.command == "choose-task":
        print(
            choose_task_interactively(
                explicit_task=args.explicit_task,
                active_task=args.active_task,
                sole_task=args.sole_task,
                lane_id=args.lane_id,
                branch=args.branch,
                worktree_path=args.worktree_path,
                orchestrator_root=args.orchestrator_root,
                in_orchestrator_root=args.in_orchestrator_root,
            )
        )
        return 0
    if args.command == "field":
        print(_field_value(args.task_ref, args.lane_id, args.field, args.orchestrator_root))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
