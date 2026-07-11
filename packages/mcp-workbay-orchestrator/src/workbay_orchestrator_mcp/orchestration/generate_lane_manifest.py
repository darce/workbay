#!/usr/bin/env python3
"""Scaffold a task-aware lane orchestration manifest.

This intentionally generates a generic starting point that can be reused for
any task plan. It does not hardcode Phase 5 semantics.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from workbay_protocol import INSTRUCTIONS_RELPATH

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DONE = "Ready for orchestrator branch review with lane-local verification complete."


def _humanize_lane(lane_id: str) -> str:
    parts = [part for part in lane_id.replace("_", "-").split("-") if part]
    if not parts:
        return lane_id
    return " ".join(part.upper() if len(part) <= 3 else part.capitalize() for part in parts)


def _default_branch(task_ref: str, lane_id: str) -> str:
    return f"codex/{task_ref}-{lane_id}"


def _default_worktree(task_ref: str, lane_id: str) -> str:
    return f"{{orchestrator_root}}-{task_ref}-{lane_id}"


def _default_grants() -> dict[str, Any]:
    """Default per-lane permission surface (adoption A).

    Derived from the lane worktree (read_write) and the primary repo (read_only);
    ``owned_paths`` live inside the worktree, so no extra write paths are granted
    by default.
    """
    return {
        "worktree": "read_write",
        "primary_repo": "read_only",
        "extra_write_paths": [],
    }


def _route_hints(lane_id: str, title: str) -> list[str]:
    hints = [lane_id, lane_id.replace("-", " ")]
    if title.strip():
        hints.extend([title, title.lower()])
    return list(dict.fromkeys(hint for hint in hints if hint.strip()))


def build_manifest(
    *,
    task_ref: str,
    lane_ids: list[str],
    task_plan: str | None = None,
    prefix: str | None = None,
    lane_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    required_docs = [str(INSTRUCTIONS_RELPATH)]
    if task_plan:
        required_docs.append(task_plan)

    name_prefix = prefix.strip() if prefix else task_ref
    lanes: dict[str, Any] = {}
    for lane_id in lane_ids:
        title = _humanize_lane(lane_id)
        lanes[lane_id] = {
            "branch": _default_branch(name_prefix, lane_id),
            "worktree_path": _default_worktree(name_prefix, lane_id),
            "title": title,
            "objective": f"{title} slice for task {task_ref}.",
            "owned_paths": [],
            "required_docs": required_docs,
            "test_commands": [],
            "capability_tags": [],
            "preflight_commands": [],
            "non_goals": [],
            "commit_paths": [],
            "commit_subject": f"update {lane_id}",
            "route_hints": _route_hints(lane_id, title),
            "guidance_fallbacks": [],
            "tooling_paths": [],
            "grants": _default_grants(),
        }
        overrides = (lane_overrides or {}).get(lane_id)
        if isinstance(overrides, dict):
            lanes[lane_id].update(overrides)

    downstream: dict[str, list[str]] = {}
    for idx, lane_id in enumerate(lane_ids):
        downstream[lane_id] = lane_ids[idx + 1 :]

    return {
        "task_ref": task_ref,
        "default_done_definition": DEFAULT_DONE,
        "merge_order": lane_ids,
        "routing": [],
        "lanes": lanes,
        "downstream": downstream,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a reusable lane manifest scaffold for a task.")
    parser.add_argument(
        "--task-ref", required=True, help="Task ref, used for filename and default branch/worktree names."
    )
    parser.add_argument(
        "--lane", dest="lanes", action="append", required=True, help="Lane id to include. Repeat for each lane."
    )
    parser.add_argument(
        "--prefix", help="Optional short prefix used for default branch/worktree names instead of the full task ref."
    )
    parser.add_argument("--task-plan", help="Optional task plan path to include in required_docs.")
    parser.add_argument(
        "--orchestrator-root",
        default=".",
        help="Workspace root used to resolve the default config/lane-orchestration output directory.",
    )
    parser.add_argument("--output", help="Optional output path. Defaults to config/lane-orchestration/<task-ref>.json.")
    parser.add_argument("--stdout", action="store_true", help="Print the generated manifest instead of writing it.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output file.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    lane_ids = [lane.strip() for lane in args.lanes if lane and lane.strip()]
    if not lane_ids:
        raise SystemExit("At least one --lane is required.")

    manifest = build_manifest(
        task_ref=args.task_ref,
        lane_ids=lane_ids,
        task_plan=args.task_plan,
        prefix=args.prefix,
    )
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import validate_manifest

    validate_manifest(manifest, Path("<generated-manifest>"))
    rendered = json.dumps(manifest, indent=2) + "\n"
    if args.stdout:
        print(rendered, end="")
        return 0

    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    default_output = orchestrator_root / "config" / "lane-orchestration" / f"{args.task_ref}.json"
    output = Path(args.output).expanduser() if args.output else default_output
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not args.force:
        raise SystemExit(f"Refusing to overwrite existing manifest without --force: {output}")
    output.write_text(rendered)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
