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
from workbay_protocol.branch_naming import extract_plan_id, format_lane_id_with_plan

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
    plan_id: str | None = None,
) -> dict[str, Any]:
    required_docs = [str(INSTRUCTIONS_RELPATH)]
    if task_plan:
        required_docs.append(task_plan)

    # Mint plan-prefixed lane ids once; every derived surface (lanes keys,
    # merge_order, downstream, branch/worktree) inherits via lane_id alone.
    # Absent / non-extractable plan id is a pure no-op (DATA-04).
    # Callers that already derived plan_id (main) pass it so depends_on minting
    # cannot diverge from the lane-id mint used here.
    if plan_id is None:
        plan_id = extract_plan_id(task_plan)
    minted_lane_ids = [format_lane_id_with_plan(lane_id, plan_id) for lane_id in lane_ids]

    name_prefix = prefix.strip() if prefix else task_ref
    lanes: dict[str, Any] = {}
    for raw_lane_id, lane_id in zip(lane_ids, minted_lane_ids, strict=True):
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
        # Accept overrides keyed by either the operator input or the minted id
        # so callers that still pass bare ids (e.g. offload_preflight without a
        # task_plan) keep working, and plan-aware callers can use either form.
        overrides = (lane_overrides or {}).get(lane_id)
        if overrides is None:
            overrides = (lane_overrides or {}).get(raw_lane_id)
        if isinstance(overrides, dict):
            lanes[lane_id].update(overrides)

    downstream: dict[str, list[str]] = {}
    for idx, lane_id in enumerate(minted_lane_ids):
        downstream[lane_id] = minted_lane_ids[idx + 1 :]

    # Scheduling relation (independent of downstream merge-propagation). Default
    # empty; author via --depends-on LANE=DEP[,DEP...] on the CLI.
    depends_on: dict[str, list[str]] = {}

    return {
        "task_ref": task_ref,
        "default_done_definition": DEFAULT_DONE,
        "merge_order": minted_lane_ids,
        "routing": [],
        "lanes": lanes,
        "downstream": downstream,
        "depends_on": depends_on,
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
    parser.add_argument(
        "--depends-on",
        dest="depends_on_specs",
        action="append",
        default=None,
        metavar="LANE=DEP[,DEP...]",
        help="Declare scheduling prerequisites for a lane (repeatable). Shape: LANE=DEP[,DEP...].",
    )
    return parser.parse_args()


def _parse_depends_on_specs(specs: list[str]) -> dict[str, list[str]]:
    """Parse repeatable --depends-on LANE=DEP[,DEP...] into a depends_on map."""
    depends_on: dict[str, list[str]] = {}
    for raw in specs:
        spec = str(raw).strip()
        if not spec:
            continue
        if "=" not in spec:
            raise SystemExit(f"--depends-on expects LANE=DEP[,DEP...], got: {raw!r}")
        lane_part, deps_part = spec.split("=", 1)
        lane_id = lane_part.strip()
        if not lane_id:
            raise SystemExit(f"--depends-on expects LANE=DEP[,DEP...], got: {raw!r}")
        prereqs = [dep.strip() for dep in deps_part.split(",") if dep.strip()]
        bucket = depends_on.setdefault(lane_id, [])
        for prereq in prereqs:
            if prereq not in bucket:
                bucket.append(prereq)
    return depends_on


def main() -> int:
    args = _parse_args()
    lane_ids = [lane.strip() for lane in args.lanes if lane and lane.strip()]
    if not lane_ids:
        raise SystemExit("At least one --lane is required.")

    # Derive plan_id once; share with build_manifest and depends_on minting.
    plan_id = extract_plan_id(args.task_plan)
    manifest = build_manifest(
        task_ref=args.task_ref,
        lane_ids=lane_ids,
        task_plan=args.task_plan,
        prefix=args.prefix,
        plan_id=plan_id,
    )
    depends_on_specs = args.depends_on_specs if args.depends_on_specs is not None else []
    depends_on = _parse_depends_on_specs(list(depends_on_specs))
    if depends_on:
        # Match lane_overrides acceptance: raw operator id or already-minted id.
        # format_lane_id_with_plan is idempotent, so either form is safe.
        manifest["depends_on"] = {
            format_lane_id_with_plan(lane_id, plan_id): [
                format_lane_id_with_plan(dep, plan_id) for dep in deps
            ]
            for lane_id, deps in depends_on.items()
        }
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
