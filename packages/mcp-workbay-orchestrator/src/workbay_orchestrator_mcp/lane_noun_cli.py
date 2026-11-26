"""The ``workbay lane`` front-door noun plugin for ``mcp-workbay-orchestrator``.

Verbs:
  status        -> ``api.lane_status(task_ref, limit, byte_cap)``
  list          -> ``manage_worktree_lane(operation="list", ...)``
  close         -> ``manage_worktree_lane(operation="close", ...)``
  reclaim-scan  -> ``lane_reclaim_scan.run_lane_reclaim_scan(...)``

All verbs emit one compact JSON object on stdout and exit with
:class:`workbay_protocol.cli_contract.ExitCode`.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_protocol.cli_contract import (
    ExitCode,
    JsonArgumentParser,
    run_json_command,
)

from . import api
from .lane_reclaim_scan import run_lane_reclaim_scan
from .lanes import manage_worktree_lane

SurfaceClass = Literal["query", "action"]


@dataclass(frozen=True, slots=True)
class LaneVerbSpec:
    """Permission-generator metadata for a ``workbay lane`` verb."""

    noun: str
    verb: str
    surface_class: SurfaceClass
    action_flags: tuple[str, ...] = ()
    description: str = ""


# Exported table for the Wave 2 permission generator ([SECD-01], [ARCH-13]).
# ``reclaim-scan`` is read-only (query) without ``--lane-id`` nudge; passing
# ``--lane-id`` triggers a nudge (action). ``close`` is always an action.
LANE_VERB_SPECS: Mapping[str, LaneVerbSpec] = MappingProxyType(
    {
        "status": LaneVerbSpec(
            noun="lane",
            verb="status",
            surface_class="query",
            description="Read-only bounded census snapshot for active lanes.",
        ),
        "list": LaneVerbSpec(
            noun="lane",
            verb="list",
            surface_class="query",
            description="List worktree lanes.",
        ),
        "close": LaneVerbSpec(
            noun="lane",
            verb="close",
            surface_class="action",
            description="Close a worktree lane (terminal status transition).",
        ),
        "reclaim-scan": LaneVerbSpec(
            noun="lane",
            verb="reclaim-scan",
            surface_class="query",
            action_flags=("--lane-id",),
            description="Scan terminal on-disk lanes for reclaim candidates (read-only without --lane-id nudge).",
        ),
    }
)

LANE_VERB_SURFACE_CLASS: Mapping[str, SurfaceClass] = MappingProxyType(
    {verb: spec.surface_class for verb, spec in LANE_VERB_SPECS.items()}
)


def verb_surface_class(verb: str, *, nudge: bool = False) -> SurfaceClass:
    """Return the effective surface class for a ``workbay lane`` verb."""
    spec = LANE_VERB_SPECS[verb]
    if verb == "reclaim-scan" and nudge:
        return "action"
    return spec.surface_class


def _positive_int_arg(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def _nonnegative_int_arg(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return value


def _add_common_flags(parser: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default_root: Any = argparse.SUPPRESS if suppress_defaults else Path.cwd()
    default_none: Any = argparse.SUPPRESS if suppress_defaults else None
    default_json: Any = argparse.SUPPRESS if suppress_defaults else False
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=default_root,
        help="Workspace root directory (default: cwd).",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=default_none,
        help="State directory (default: <workspace-root>/.task-state).",
    )
    parser.add_argument(
        "--current-task-path",
        type=Path,
        default=default_none,
        help="CURRENT_TASK.json path (default: <workspace-root>/CURRENT_TASK.json).",
    )
    parser.add_argument(
        "--exports-dir",
        type=Path,
        default=default_none,
        help="Exports directory (default: <state-dir>/exports).",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=default_json,
        help="Emit compact JSON envelope (always on for workbay lane).",
    )


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(
        prog="workbay lane",
        description="Manage and inspect WorkBay orchestrator worktree lanes.",
    )
    _add_common_flags(parser, suppress_defaults=False)
    subparsers = parser.add_subparsers(dest="verb", required=True)

    # --- status ---
    status_p = subparsers.add_parser(
        "status",
        help=LANE_VERB_SPECS["status"].description,
    )
    _add_common_flags(status_p, suppress_defaults=True)
    status_p.add_argument("--task-ref", default=None, help="Optional task scope.")
    status_p.add_argument("--limit", type=_positive_int_arg, default=20, help="Max lane rows (default: 20).")
    status_p.add_argument(
        "--byte-cap",
        type=_positive_int_arg,
        default=24_576,
        help="Maximum response bytes (default: 24576).",
    )

    # --- list ---
    list_p = subparsers.add_parser(
        "list",
        help=LANE_VERB_SPECS["list"].description,
    )
    _add_common_flags(list_p, suppress_defaults=True)
    list_p.add_argument("--task-ref", default=None, help="Optional task scope.")
    list_p.add_argument("--status", default="all", help="Lane status filter (default: all).")
    list_p.add_argument("--limit", type=int, default=100, help="Max lanes to return (default: 100).")
    list_p.add_argument("--offset", type=int, default=0, help="Pagination offset (default: 0).")

    # --- close ---
    close_p = subparsers.add_parser(
        "close",
        help=LANE_VERB_SPECS["close"].description,
    )
    _add_common_flags(close_p, suppress_defaults=True)
    close_p.add_argument("--lane-id", required=True, help="Lane identifier to close.")
    close_p.add_argument("--status", default="closed", help="Terminal close status (default: closed).")
    close_p.add_argument("--notes", default=None, help="Optional close notes.")
    close_p.add_argument("--task-ref", default=None, help="Optional task scope.")
    close_p.add_argument("--landing-commit-sha", default=None, help="Optional landing commit SHA.")
    close_p.add_argument("--branch-tip-sha", default=None, help="Optional branch tip SHA.")
    close_p.add_argument("--event-id", default=None, help="Optional idempotency event ID.")

    # --- reclaim-scan ---
    reclaim_p = subparsers.add_parser(
        "reclaim-scan",
        help=LANE_VERB_SPECS["reclaim-scan"].description,
    )
    _add_common_flags(reclaim_p, suppress_defaults=True)
    scope = reclaim_p.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--task-ref",
        default=None,
        help="Task scope for the reclaim scan (blank/whitespace refused).",
    )
    scope.add_argument(
        "--all-tasks",
        action="store_true",
        default=False,
        help="Explicitly scan terminal lane rows across every task.",
    )
    reclaim_p.add_argument(
        "--lane-id",
        default=None,
        help="When set, also run nudge_reclaim_candidate for this lane after the scan.",
    )
    reclaim_p.add_argument(
        "--limit",
        type=_positive_int_arg,
        default=None,
        help="Evaluate at most N registry rows in this invocation.",
    )
    reclaim_p.add_argument(
        "--offset",
        type=_nonnegative_int_arg,
        default=0,
        help="Skip N registry rows before evaluating this page (default: 0).",
    )

    return parser


def _configure_runtime(args: argparse.Namespace) -> RuntimeConfig:
    config = RuntimeConfig.for_repo(
        args.workspace_root,
        state_dir=args.state_dir,
        current_task_path=args.current_task_path,
        exports_dir=args.exports_dir,
    )
    api.configure_runtime(config)
    return config


def _dispatch_verb(args: argparse.Namespace) -> Mapping[str, Any]:
    verb = args.verb
    if verb == "reclaim-scan":
        # Validate blank/conflicting flags before configuring runtime or calling symbols.
        if (
            (args.task_ref is not None and args.task_ref.strip() == "")
            or (args.all_tasks and args.lane_id is not None)
            or (args.lane_id is not None and args.lane_id.strip() == "")
        ):
            return run_lane_reclaim_scan(
                orchestrator_root=args.workspace_root,
                task_ref=args.task_ref,
                all_tasks=bool(args.all_tasks),
                lane_id=args.lane_id,
                limit=args.limit,
                offset=args.offset,
            ).payload
        config = _configure_runtime(args)
        return run_lane_reclaim_scan(
            orchestrator_root=config.workspace_root,
            task_ref=args.task_ref,
            all_tasks=bool(args.all_tasks),
            lane_id=args.lane_id,
            limit=args.limit,
            offset=args.offset,
        ).payload

    _configure_runtime(args)

    if verb == "status":
        return api.lane_status(
            task_ref=args.task_ref,
            limit=args.limit,
            byte_cap=args.byte_cap,
        )

    if verb == "list":
        return manage_worktree_lane(
            operation="list",
            task_ref=args.task_ref,
            status=args.status,
            limit=args.limit,
            offset=args.offset,
        )

    if verb == "close":
        close_kwargs: dict[str, Any] = {
            "operation": "close",
            "lane_id": args.lane_id,
            "status": args.status,
            "notes": args.notes,
            "task_ref": args.task_ref,
            "landing_commit_sha": args.landing_commit_sha,
            "branch_tip_sha": args.branch_tip_sha,
        }
        if args.event_id is not None:
            close_kwargs["event_id"] = args.event_id
        return manage_worktree_lane(**close_kwargs)

    return {
        "ok": False,
        "source": "workbay lane",
        "error": {"kind": "usage", "message": f"Unknown verb: {verb}"},
    }


def workbay_front_door(argv: Sequence[str] | None = None) -> int:
    """Entry point registered under ``[project.entry-points."workbay.commands"]`` as ``lane``."""
    args_list = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    try:
        args = parser.parse_args(args_list)
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return int(ExitCode.OK)
        if isinstance(code, int):
            return code
        return int(ExitCode.USAGE)

    source = f"workbay lane {args.verb}"
    return run_json_command(lambda: _dispatch_verb(args), source=source)


__all__ = [
    "LANE_VERB_SPECS",
    "LANE_VERB_SURFACE_CLASS",
    "LaneVerbSpec",
    "SurfaceClass",
    "verb_surface_class",
    "workbay_front_door",
]
