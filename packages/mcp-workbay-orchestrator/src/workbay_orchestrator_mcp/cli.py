"""CLI entry point for the WorkBay Orchestrator MCP server.

Subcommands:
  serve / serve-stdio      Start the MCP server over stdio.
  doctor                   Print server diagnostics.
  tools-snapshot           Capture a normalized tools/list snapshot.
  orchestrator-start       Start the orchestrator daemon for a task.
  orchestrator-status      Print orchestrator daemon status.
  orchestrator-pause       Pause the orchestrator daemon.
  orchestrator-resume      Resume the orchestrator daemon.
  orchestrator-stop        Stop the orchestrator daemon.
  orchestrator-cycle       Run one orchestrator cycle synchronously.
  worker-start             Start a worker daemon for a specific lane.
  worker-status            Print worker daemon status for a lane.
  worker-stop              Stop a worker daemon for a lane.
  worker-resume            Resume a worker daemon for a lane.
  worker-start-all         Start worker daemons for all lanes in a task.
  worker-events            Print worker event history for a lane.
  dispatch                 Dispatch (upsert) work for a lane.
  lane-upsert              Upsert worktree lane metadata.
  lane-close               Close a worktree lane (terminal status transition).
  lane-list                List worktree lanes for a task.
  lane-activity            Read lane activity summary.
  lane-message             Record a lane message.
  lane-message-list        List lane messages.
  lane-message-update      Update lane message status.
  lane-report              Record a worker lane report.
  lane-report-list         List worker lane reports.
  lane-report-ack-backfill Backfill stranded submitted worker reports to acknowledged.
  lane-reap                Reap conclusive-dead non-terminal lane rows (dry-run by default).
  lane-census              Census non-terminal remote lanes and plan typed repairs (dry-run by default).
  lane-reclaim-scan        Scan terminal on-disk lanes for reclaim candidates (optional nudge).
  list-backends            List available AI backends.
  metrics                  Print ACE metrics summary.
  ace-reflect              Apply pending ACE counter updates.
  ace-curation-report      Print ACE curation report.
  ace-metrics              Build full ACE metrics snapshot.
  ace-trends               Print ACE metrics sparklines.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_protocol import BRAND_NAME

from .api import (
    build_orchestrator_mcp,
    configure_runtime,
    dispatch_lane_work,
    get_metrics_summary,
    list_available_backends,
    manage_orchestrator,
    manage_worker,
    run_doctor,
    run_tools_snapshot,
)
from .lanes import (
    DEFAULT_BLOCKED_LANE_REAP_BATCH,
    backfill_worker_report_acks,
    get_lane_activity,
    lane_communication,
    manage_worktree_lane,
    reap_blocked_lanes,
    worker_reports,
)
from .orchestration.lane_reclaim import (
    NudgeFailure,
    nudge_reclaim_candidate,
    scan_terminal_lanes,
)

# OBS-08 / REVHARD-EXIT-CODE-NOT-PINNED-01: incomplete scan must exit this exact
# nonzero code (distinct from clean-empty exit 0 and from generic handler error 1).
_LANE_RECLAIM_SCAN_DEGRADED_EXIT = 2


def _build_config(
    workspace_root: Path,
    state_dir: Path | None = None,
    current_task_path: Path | None = None,
    exports_dir: Path | None = None,
) -> RuntimeConfig:
    return RuntimeConfig.for_repo(
        workspace_root,
        state_dir=state_dir,
        current_task_path=current_task_path,
        exports_dir=exports_dir,
    )


def _print_json(payload: Any) -> None:
    if isinstance(payload, dict):
        print(json.dumps(payload, indent=2))
        return
    print(payload)


def _emit_lane_payload(payload: Any) -> None:
    """Print lane-data JSON and propagate business-level failure to the shell."""
    _print_json(payload)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise SystemExit(1)


def _handle_lane_reclaim_scan(
    *,
    orchestrator_root: Path,
    task_ref: str | None,
    all_tasks: bool,
    lane_id: str | None,
) -> None:
    """Run scan_terminal_lanes and optional nudge_reclaim_candidate.

    Never-raise wrapper (RES-07/AGT-10): the whole post-validation body is under
    one try/except so non-conforming returns and symbol faults become an
    ``ok: false`` JSON report and exit 1 (no traceback). Incomplete scans
    (``ScanResult.complete`` is False) exit with the pinned degraded code 2
    (OBS-08), distinct from generic error exit 1. Clean complete scans exit 0.
    """
    # Blank/whitespace --task-ref refused before any symbol call (mirror lane-reap).
    if task_ref is not None and task_ref.strip() == "":
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "blank_task_ref: --task-ref was provided but empty/whitespace; "
                    "an empty scope is not a valid reclaim scan. "
                    "Pass a real task_ref."
                ),
            }
        )
        return

    if all_tasks and lane_id is not None:
        _emit_lane_payload(
            {"ok": False, "error": "all_tasks_lane_id: --lane-id requires a scoped --task-ref scan."}
        )
        return

    # Blank/whitespace --lane-id refused the same way when the flag is present.
    if lane_id is not None and lane_id.strip() == "":
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "blank_lane_id: --lane-id was provided but empty/whitespace; "
                    "an empty lane_id is not a valid reclaim nudge. "
                    "Pass a real lane_id or omit the flag."
                ),
            }
        )
        return

    # Whole post-validation body under never-raise (REAPCONV-RECLAIMWIRE-NEVERRAISE-WRAP-INCOMPLETE-01):
    # scan, complete/candidate access, iteration, and nudge result rendering.
    # SystemExit (degraded=2 or _emit_lane_payload=1) is BaseException — not caught here.
    try:
        scan = scan_terminal_lanes(
            orchestrator_root=orchestrator_root,
            **({"all_tasks": True} if all_tasks else {"task_ref": task_ref}),
        )

        if not scan.complete:
            reason = scan.refusal_reason if scan.refusal_reason is not None else "scan_incomplete"
            print(f"refusal_reason={reason}")
            raise SystemExit(_LANE_RECLAIM_SCAN_DEGRADED_EXIT)

        print(f"candidates={len(scan)}")
        # Per-lane faults do not flip complete (OBS-08 / PARTIAL-SCAN-FIELDS): surface counts.
        failed_n = len(scan.failed_lanes)
        unrecorded_n = len(scan.unrecorded_lanes)
        if failed_n or unrecorded_n:
            print(f"failed_lanes={failed_n} unrecorded_lanes={unrecorded_n}")

        for cand in scan:
            verdict = cand.verdict
            print(
                f"candidate lane_id={cand.lane_id} "
                f"reclaimable={verdict.reclaimable} "
                f"reason={verdict.reason} "
                f"recorded={cand.recorded}"
            )
            branch_verdict = cand.branch_verdict
            if branch_verdict is not None:
                print(
                    f"branch_candidate task_ref={cand.task_ref} lane_id={cand.lane_id} "
                    f"reclaimable={branch_verdict.reclaimable} "
                    f"reason={branch_verdict.reason} "
                    f"recorded={cand.branch_recorded}"
                )

        if lane_id is None:
            return

        nudged = nudge_reclaim_candidate(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
        )

        if nudged is None:
            print(f"nudge lane_id={lane_id} not-a-candidate")
        elif isinstance(nudged, NudgeFailure):
            print(f"nudge lane_id={nudged.lane_id} failure reason={nudged.reason}")
        else:
            verdict = nudged.verdict
            print(
                f"nudge lane_id={nudged.lane_id} "
                f"reclaimable={verdict.reclaimable} "
                f"reason={verdict.reason} "
                f"recorded={nudged.recorded}"
            )
    except Exception as exc:  # noqa: BLE001 — CLI never-raise surface
        # Unify error surface with blank-flag refusal: JSON ok:false, exit 1.
        _emit_lane_payload(
            {
                "ok": False,
                "error": f"lane-reclaim-scan error: {exc}",
            }
        )


def _resolve_playbook_paths(paths: list[str] | None) -> list[str]:
    """Resolve ACE playbook declarations: explicit --playbook-file wins, else fall
    back to the canonical WORKBAY_ACE_PLAYBOOK_FILES env var.

    The env var is read through ``workbay_protocol.resolve_env_alias`` so it is
    the single canonical resolution seam (blank == unset) and renames in lockstep
    with every other resolve_env_alias call site under the WorkBay rebrand
    (implementation note). ``Makefile.d/ace.mk`` still expands the same declaration into
    explicit --playbook-file flags for the operator surface.
    """
    if paths:
        return paths
    from workbay_protocol import resolve_env_alias  # noqa: PLC0415

    declared = resolve_env_alias("WORKBAY_ACE_PLAYBOOK_FILES", default="")
    return [token for token in declared.split() if token]


def _coerce_playbook_paths(raw_paths: list[str], workspace_root: Path) -> list[Path]:
    resolved: list[Path] = []
    for path in raw_paths:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = workspace_root / candidate
        resolved.append(candidate)
    return resolved


def _validated_playbook_files(command: str, paths: list[str] | None, workspace_root: Path) -> list[Path]:
    from workbay_orchestrator_mcp.orchestration.ace_reflect import (  # noqa: PLC0415
        PlaybookValidationError,
        validate_playbook_files,
    )

    playbook_files = _coerce_playbook_paths(_resolve_playbook_paths(paths), workspace_root)
    try:
        validate_playbook_files(playbook_files)
    except PlaybookValidationError as exc:
        print(f"{command}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    return playbook_files


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-workbay-orchestrator",
        description=f"{BRAND_NAME} Orchestrator MCP server.",
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path.cwd(),
        help="Workspace root directory (default: cwd).",
    )
    parser.add_argument(
        "--state-dir", type=Path, default=None, help="State directory (default: <workspace-root>/.task-state)."
    )
    parser.add_argument(
        "--current-task-path",
        type=Path,
        default=None,
        help="CURRENT_TASK.json path (default: <workspace-root>/CURRENT_TASK.json).",
    )
    parser.add_argument(
        "--exports-dir", type=Path, default=None, help="Exports directory (default: <state-dir>/exports)."
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- serve ---
    subparsers.add_parser("serve", help="Start the MCP server (default).")
    subparsers.add_parser("serve-stdio", help="Start the MCP server over stdio (alias for serve).")

    # --- doctor ---
    doctor_p = subparsers.add_parser("doctor", help="Print server diagnostics.")
    doctor_p.add_argument("--json", dest="json_output", action="store_true")

    # --- tools snapshot ---
    snapshot_p = subparsers.add_parser("tools-snapshot", help="Capture a normalized tools/list snapshot.")
    snapshot_p.add_argument("--output", type=Path, default=None)
    snapshot_p.add_argument("--json", dest="json_output", action="store_true")

    # --- orchestrator daemon ---
    ostart = subparsers.add_parser("orchestrator-start", help="Start the orchestrator daemon.")
    ostart.add_argument("--task-ref", required=True)
    ostart.add_argument("--backend", default="codex-cli")
    ostart.add_argument("--poll-interval", type=int, default=60)
    ostart.add_argument("--single-pass", action="store_true", default=False)
    ostart.add_argument("--worker-start-mode", default="mcp")
    ostart.add_argument("--worker-reasoning-effort", default="auto")
    ostart.add_argument("--model", default=None)

    subparsers.add_parser("orchestrator-status", help="Print orchestrator daemon status.")
    subparsers.add_parser("orchestrator-pause", help="Pause the orchestrator daemon.")
    subparsers.add_parser("orchestrator-resume", help="Resume the orchestrator daemon.")

    ostop = subparsers.add_parser("orchestrator-stop", help="Stop the orchestrator daemon.")
    ostop.add_argument("--force", action="store_true", default=False)
    ostop.add_argument("--wait", type=float, default=5.0, dest="wait_seconds")

    ocycle = subparsers.add_parser("orchestrator-cycle", help="Run one orchestrator cycle synchronously.")
    ocycle.add_argument("--task-ref", required=True)
    ocycle.add_argument("--backend", default="codex-cli")
    ocycle.add_argument("--dry-run", action="store_true", default=False)
    ocycle.add_argument("--timeout", type=float, default=300.0, dest="timeout_seconds")
    ocycle.add_argument("--worker-start-mode", default="mcp")
    ocycle.add_argument("--worker-reasoning-effort", default="auto")
    ocycle.add_argument("--model", default=None)

    # --- worker daemon ---
    wstart = subparsers.add_parser("worker-start", help="Start a worker daemon for a lane.")
    wstart.add_argument("--task-ref", required=True)
    wstart.add_argument("--lane-id", required=True)
    wstart.add_argument("--backend", default="codex-subagent")
    wstart.add_argument("--poll-interval", type=int, default=30)
    wstart.add_argument("--single-pass", action="store_true", default=False)
    wstart.add_argument("--session", default=None)
    wstart.add_argument("--session-mode", default="fresh_turn")
    wstart.add_argument("--reasoning-effort", default="inherit")
    wstart.add_argument("--model", default=None)

    wstatus = subparsers.add_parser("worker-status", help="Print worker daemon status for a lane.")
    wstatus.add_argument("--task-ref", required=True)
    wstatus.add_argument("--lane-id", required=True)

    wstop = subparsers.add_parser("worker-stop", help="Stop a worker daemon for a lane.")
    wstop.add_argument("--task-ref", required=True)
    wstop.add_argument("--lane-id", required=True)
    wstop.add_argument("--force", action="store_true", default=False)

    wresume = subparsers.add_parser("worker-resume", help="Resume a worker daemon for a lane.")
    wresume.add_argument("--task-ref", required=True)
    wresume.add_argument("--lane-id", required=True)

    wall = subparsers.add_parser("worker-start-all", help="Start worker daemons for all lanes in a task.")
    wall.add_argument("--task-ref", required=True)
    wall.add_argument("--backend", default="codex-subagent")
    wall.add_argument("--poll-interval", type=int, default=30)
    wall.add_argument("--single-pass", action="store_true", default=False)
    wall.add_argument("--session-mode", default="fresh_turn")
    wall.add_argument("--reasoning-effort", default="inherit")
    wall.add_argument("--model", default=None)

    wevents = subparsers.add_parser("worker-events", help="Print worker event history for a lane.")
    wevents.add_argument("--task-ref", required=True)
    wevents.add_argument("--lane-id", required=True)
    wevents.add_argument("--limit", type=int, default=50)
    wevents.add_argument("--event-name", default=None)

    # --- dispatch ---
    dispatch_p = subparsers.add_parser("dispatch", help="Dispatch (upsert) work for a lane.")
    dispatch_p.add_argument("--lane-id", required=True)
    dispatch_p.add_argument("--task-ref", default=None)
    dispatch_p.add_argument("--model", default=None)
    dispatch_p.add_argument("--backend", default=None)
    dispatch_p.add_argument("--reasoning-effort", default=None)
    dispatch_p.add_argument("--start-worker", action="store_true", default=False)

    # --- lane data (bash-callable adapters over lanes.py) ---
    lane_upsert_p = subparsers.add_parser("lane-upsert", help="Upsert worktree lane metadata.")
    lane_upsert_p.add_argument("--lane-id", required=True)
    lane_upsert_p.add_argument("--worktree-path", required=True)
    lane_upsert_p.add_argument("--branch", required=True)
    lane_upsert_p.add_argument("--owner-agent", default=None)
    lane_upsert_p.add_argument("--status", default="planned")
    lane_upsert_p.add_argument("--title", default=None)
    lane_upsert_p.add_argument("--objective", default=None)
    lane_upsert_p.add_argument("--notes", default=None)
    lane_upsert_p.add_argument("--task-ref", default=None)

    lane_close_p = subparsers.add_parser(
        "lane-close",
        help="Close a worktree lane (terminal status via close operation).",
    )
    lane_close_p.add_argument("--lane-id", required=True)
    lane_close_p.add_argument("--status", default="closed")
    lane_close_p.add_argument("--notes", default=None)
    lane_close_p.add_argument("--task-ref", default=None)

    lane_list_p = subparsers.add_parser("lane-list", help="List worktree lanes.")
    lane_list_p.add_argument("--task-ref", default=None)
    lane_list_p.add_argument("--status", default="all")
    lane_list_p.add_argument("--limit", type=int, default=100)
    lane_list_p.add_argument("--offset", type=int, default=0)

    lane_activity_p = subparsers.add_parser("lane-activity", help="Read lane activity summary.")
    lane_activity_p.add_argument("--lane-id", required=True)
    lane_activity_p.add_argument("--task-ref", default=None)

    lane_message_p = subparsers.add_parser("lane-message", help="Record a lane message.")
    lane_message_p.add_argument("--task-ref", default=None)
    lane_message_p.add_argument("--lane-id", required=True)
    lane_message_p.add_argument("--session", required=True)
    lane_message_p.add_argument("--direction", required=True)
    lane_message_p.add_argument("--message", required=True)
    lane_message_p.add_argument("--status", default="open")
    lane_message_p.add_argument("--subject", default=None)

    lane_message_list_p = subparsers.add_parser("lane-message-list", help="List lane messages.")
    lane_message_list_p.add_argument("--task-ref", default=None)
    lane_message_list_p.add_argument("--lane-id", default=None)
    lane_message_list_p.add_argument("--status", default="all")
    lane_message_list_p.add_argument("--limit", type=int, default=20)
    lane_message_list_p.add_argument("--offset", type=int, default=0)

    lane_message_update_p = subparsers.add_parser("lane-message-update", help="Update lane message status.")
    lane_message_update_p.add_argument("--task-ref", default=None)
    lane_message_update_p.add_argument("--message-id", type=int, required=True)
    lane_message_update_p.add_argument("--status", required=True)

    lane_report_p = subparsers.add_parser("lane-report", help="Record a worker lane report.")
    lane_report_p.add_argument("--task-ref", default=None)
    lane_report_p.add_argument("--lane-id", required=True)
    lane_report_p.add_argument("--session", required=True)
    lane_report_p.add_argument(
        "--delivery-id",
        default=None,
        help=(
            "Stable delivery identifier; it must be unique across all lanes and tasks. "
            "Re-submitting the same identifier is an idempotent replay of the original report."
        ),
    )
    lane_report_p.add_argument("--summary", required=True)
    lane_report_p.add_argument("--status", default="submitted")
    lane_report_p.add_argument("--outcome", default=None)
    lane_report_p.add_argument("--merge-ready", action="store_true", default=False)
    lane_report_p.add_argument("--changed-file", action="append", default=None)
    lane_report_p.add_argument("--test-command", action="append", default=None)
    lane_report_p.add_argument("--blocker", action="append", default=None)

    lane_report_list_p = subparsers.add_parser("lane-report-list", help="List worker lane reports.")
    lane_report_list_p.add_argument("--task-ref", default=None)
    lane_report_list_p.add_argument("--lane-id", default=None)
    lane_report_list_p.add_argument("--limit", type=int, default=20)
    lane_report_list_p.add_argument("--offset", type=int, default=0)

    lane_report_ack_backfill_p = subparsers.add_parser(
        "lane-report-ack-backfill",
        help="Backfill stranded status=submitted worker reports to acknowledged (CAS, idempotent).",
    )
    lane_report_ack_backfill_p.add_argument("--task-ref", default=None)
    lane_report_ack_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Count stranded rows without updating.",
    )
    lane_report_ack_backfill_p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max rows to process (default: all).",
    )

    lane_reap_p = subparsers.add_parser(
        "lane-reap",
        help="Reap conclusive-dead non-terminal lane rows (dry-run by default).",
    )
    lane_reap_p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="CAS-close conclusive-dead rows (default is dry-run report only).",
    )
    lane_reap_p.add_argument(
        "--task-ref",
        default=None,
        help="Scope candidates and closes to one task_ref (default: repo-wide).",
    )
    lane_reap_p.add_argument(
        "--max-batch",
        type=int,
        default=DEFAULT_BLOCKED_LANE_REAP_BATCH,
        help=f"Max candidate rows per invocation (default: {DEFAULT_BLOCKED_LANE_REAP_BATCH}).",
    )
    lane_reap_p.add_argument(
        "--min-age-hours",
        type=float,
        default=24.0,
        help="Grace floor for non-blocked candidates in hours (default: 24.0).",
    )
    lane_reap_p.add_argument(
        "--reclaim-worktrees",
        action="store_true",
        default=False,
        help=(
            "Operator opt-in: attempt guarded git worktree remove for the "
            "merged-branch/live-worktree deadlock (default off; daemon never "
            "sets this)."
        ),
    )

    lane_census_p = subparsers.add_parser(
        "lane-census",
        help="Census non-terminal remote lanes and plan typed repairs (dry-run by default).",
    )
    lane_census_p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Apply typed repairs and reap harvested remote sandboxes (default is dry-run report only).",
    )
    lane_census_p.add_argument(
        "--task-ref",
        required=True,
        help="Task to census (required).",
    )
    lane_census_p.add_argument(
        "--max-batch",
        type=int,
        default=50,
        help="Max repair-eligible rows per invocation (default: 50). Observe-only sinks fill spare slots.",
    )
    lane_census_p.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=False,
        help="Emit the census report as JSON (always on for this command; accepted for parser parity).",
    )

    lane_reclaim_scan_p = subparsers.add_parser(
        "lane-reclaim-scan",
        help="Scan terminal on-disk lanes for reclaim candidates; optional single-lane nudge.",
    )
    lane_reclaim_scope = lane_reclaim_scan_p.add_mutually_exclusive_group(required=True)
    lane_reclaim_scope.add_argument(
        "--task-ref",
        help="Task scope for the reclaim scan (required; blank/whitespace refused).",
    )
    lane_reclaim_scope.add_argument(
        "--all-tasks",
        action="store_true",
        help="Explicitly scan terminal lane rows across every task.",
    )
    lane_reclaim_scan_p.add_argument(
        "--lane-id",
        default=None,
        help="When set, also run nudge_reclaim_candidate for this lane after the scan.",
    )

    # --- list-backends ---
    list_backends_p = subparsers.add_parser("list-backends", help="List available AI backends.")
    list_backends_p.add_argument(
        "--probe",
        action="store_true",
        default=False,
        help="Probe live availability per backend (may shell out to codex/claude and import optional bridges).",
    )

    # --- metrics ---
    metrics_p = subparsers.add_parser("metrics", help="Print ACE metrics summary.")
    metrics_p.add_argument("--task-ref", default=None)
    metrics_p.add_argument("--format", dest="output_format", default="markdown", choices=["markdown", "json"])

    ace_playbook: dict[str, Any] = {
        "action": "append",
        "dest": "playbook_files",
        "help": (
            "Playbook file with ACE strategy bullets (repeatable). Falls back to "
            "the WORKBAY_ACE_PLAYBOOK_FILES env var when omitted."
        ),
    }

    ace_reflect_p = subparsers.add_parser("ace-reflect", help="Apply pending ACE counter updates.")
    ace_reflect_p.add_argument("--playbook-file", **ace_playbook)
    ace_reflect_p.add_argument("--dry-run", action="store_true")
    ace_reflect_p.add_argument("--model-curation-backend", default=None)
    ace_reflect_p.add_argument("--model-curation-model", default=None)
    ace_reflect_p.add_argument("--model-curation-reasoning-effort", default=None)
    ace_reflect_p.add_argument("--model-curation-threshold", type=int, default=5)
    ace_reflect_p.add_argument("--model-curation-budget-tokens", type=int, default=20000)

    ace_report_p = subparsers.add_parser("ace-curation-report", help="Print ACE curation report.")
    ace_report_p.add_argument("--playbook-file", **ace_playbook)

    ace_metrics_p = subparsers.add_parser("ace-metrics", help="Build full ACE metrics snapshot.")
    ace_metrics_p.add_argument("--task-ref", required=True)
    ace_metrics_p.add_argument("--playbook-file", **ace_playbook)
    ace_metrics_p.add_argument("--logs-dir", default="logs")
    ace_metrics_p.add_argument("--format", dest="output_format", default="markdown", choices=["markdown", "json"])

    ace_trends_p = subparsers.add_parser("ace-trends", help="Print ACE metrics sparklines.")
    ace_trends_p.add_argument("--task-ref", required=True)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config = _build_config(
        args.workspace_root,
        state_dir=args.state_dir,
        current_task_path=args.current_task_path,
        exports_dir=args.exports_dir,
    )
    configure_runtime(config)

    cmd = args.command

    # --- serve ---
    if cmd in (None, "serve", "serve-stdio"):
        mcp = build_orchestrator_mcp(config)
        mcp.run()
        return

    # --- doctor ---
    if cmd == "doctor":
        result = run_doctor(config)
        if getattr(args, "json_output", False):
            print(json.dumps(result, indent=2))
        else:
            print(f"server: {result.get('server', 'mcp-workbay-orchestrator')}")
            print(f"tool_count: {result.get('tool_count', '?')}")
            for name in sorted(result.get("tools", [])):
                print(f"  - {name}")
        return

    if cmd == "tools-snapshot":
        output_path = args.output
        if output_path is None:
            output_path = config.state_dir / "tools-list-snapshot.json"
        result = run_tools_snapshot(config, output_path=output_path)
        if getattr(args, "json_output", False):
            print(json.dumps(result, indent=2))
        else:
            print(f"server: {result['server']}")
            print(f"tool_count: {result['tool_count']}")
            print(
                "estimated_tools_list_tokens: "
                f"{result['estimated_tools_list_tokens']} ({result['token_estimation_method']})"
            )
            print(f"tools_list_bytes: {result['tools_list_bytes']}")
            print(f"output_path: {result['output_path']}")
        return

    # --- orchestrator daemon ---
    if cmd == "orchestrator-start":
        _print_json(
            manage_orchestrator(
                operation="start",
                task_ref=args.task_ref,
                backend=args.backend,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                worker_start_mode=args.worker_start_mode,
                worker_reasoning_effort=args.worker_reasoning_effort,
                model=args.model,
            )
        )
        return

    if cmd == "orchestrator-status":
        _print_json(manage_orchestrator(operation="status"))
        return

    if cmd == "orchestrator-pause":
        _print_json(manage_orchestrator(operation="pause"))
        return

    if cmd == "orchestrator-resume":
        _print_json(manage_orchestrator(operation="resume"))
        return

    if cmd == "orchestrator-stop":
        _print_json(manage_orchestrator(operation="stop", force=args.force, wait_seconds=args.wait_seconds))
        return

    if cmd == "orchestrator-cycle":
        _print_json(
            manage_orchestrator(
                operation="single_cycle",
                task_ref=args.task_ref,
                backend=args.backend,
                dry_run=args.dry_run,
                timeout_seconds=args.timeout_seconds,
                worker_start_mode=args.worker_start_mode,
                worker_reasoning_effort=args.worker_reasoning_effort,
                model=args.model,
            )
        )
        return

    # --- worker daemon ---
    if cmd == "worker-start":
        _print_json(
            manage_worker(
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                action="start",
                backend=args.backend,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                session=args.session,
                session_mode=args.session_mode,
                reasoning_effort=args.reasoning_effort,
                model=args.model,
            )
        )
        return

    if cmd == "worker-status":
        _print_json(manage_worker(task_ref=args.task_ref, lane_id=args.lane_id, action="status"))
        return

    if cmd == "worker-stop":
        _print_json(manage_worker(task_ref=args.task_ref, lane_id=args.lane_id, action="stop", force=args.force))
        return

    if cmd == "worker-resume":
        _print_json(manage_worker(task_ref=args.task_ref, lane_id=args.lane_id, action="resume"))
        return

    if cmd == "worker-start-all":
        _print_json(
            manage_worker(
                task_ref=args.task_ref,
                action="start_all",
                backend=args.backend,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                session_mode=args.session_mode,
                reasoning_effort=args.reasoning_effort,
                model=args.model,
            )
        )
        return

    if cmd == "worker-events":
        _print_json(
            manage_worker(
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                action="event_history",
                limit=args.limit,
                event_name=args.event_name,
            )
        )
        return

    # --- dispatch ---
    if cmd == "dispatch":
        _print_json(
            dispatch_lane_work(
                lane_id=args.lane_id,
                model=args.model,
                backend=args.backend,
                reasoning_effort=args.reasoning_effort,
                task_ref=args.task_ref,
                start_worker=args.start_worker,
            )
        )
        return

    # --- lane data ---
    if cmd == "lane-upsert":
        _emit_lane_payload(
            manage_worktree_lane(
                operation="upsert",
                lane_id=args.lane_id,
                worktree_path=args.worktree_path,
                branch=args.branch,
                owner_agent=args.owner_agent,
                status=args.status,
                title=args.title,
                objective=args.objective,
                notes=args.notes,
                task_ref=args.task_ref,
            )
        )
        return

    if cmd == "lane-close":
        _emit_lane_payload(
            manage_worktree_lane(
                operation="close",
                lane_id=args.lane_id,
                status=args.status,
                notes=args.notes,
                task_ref=args.task_ref,
            )
        )
        return

    if cmd == "lane-list":
        _emit_lane_payload(
            manage_worktree_lane(
                operation="list",
                task_ref=args.task_ref,
                status=args.status,
                limit=args.limit,
                offset=args.offset,
            )
        )
        return

    if cmd == "lane-activity":
        _emit_lane_payload(get_lane_activity(lane_id=args.lane_id, task_ref=args.task_ref))
        return

    if cmd == "lane-message":
        _emit_lane_payload(
            lane_communication(
                kind="message",
                operation="record",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                session=args.session,
                direction=args.direction,
                message=args.message,
                status=args.status,
                subject=args.subject,
            )
        )
        return

    if cmd == "lane-message-list":
        _emit_lane_payload(
            lane_communication(
                kind="message",
                operation="list",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                status=args.status,
                limit=args.limit,
                offset=args.offset,
            )
        )
        return

    if cmd == "lane-message-update":
        _emit_lane_payload(
            lane_communication(
                kind="message",
                operation="update",
                task_ref=args.task_ref,
                message_id=args.message_id,
                status=args.status,
            )
        )
        return

    if cmd == "lane-report":
        _emit_lane_payload(
            worker_reports(
                operation="record",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                session=args.session,
                delivery_id=args.delivery_id,
                summary=args.summary,
                status=args.status,
                outcome=args.outcome,
                merge_ready=args.merge_ready,
                changed_files=args.changed_file,
                test_commands=args.test_command,
                blockers=args.blocker,
            )
        )
        return

    if cmd == "lane-report-list":
        _emit_lane_payload(
            worker_reports(
                operation="list",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                limit=args.limit,
                offset=args.offset,
            )
        )
        return

    if cmd == "lane-report-ack-backfill":
        _emit_lane_payload(
            backfill_worker_report_acks(
                task_ref=args.task_ref,
                dry_run=args.dry_run,
                limit=args.limit,
            )
        )
        return

    if cmd == "lane-reap":
        # AGT-14: no confirmation prompt — safety is dry-run default + --task-ref.
        # Blank/whitespace --task-ref would silently widen to repo-wide via reaper
        # normalization; refuse at the CLI shell instead (do not call reaper).
        if args.task_ref is not None and args.task_ref.strip() == "":
            _emit_lane_payload(
                {
                    "ok": False,
                    "error": (
                        "blank_task_ref: --task-ref was provided but empty/whitespace; "
                        "an empty scope would silently widen the reap to repo-wide. "
                        "Omit the flag for an explicit repo-wide dry run, or pass a real task_ref."
                    ),
                }
            )
            return
        payload = reap_blocked_lanes(
            apply=args.apply,
            task_ref=args.task_ref,
            max_batch=args.max_batch,
            min_age_hours=args.min_age_hours,
            reclaim_worktrees=args.reclaim_worktrees,
        )
        # OBS-08: reap_blocked_lanes is never-raise (RES-07) and may return ok:true
        # with an error key or non-empty failed list. Print payload verbatim, exit
        # nonzero on degraded sweeps so make/automation cannot treat failure as success.
        if isinstance(payload, dict):
            failed = payload.get("failed")
            if payload.get("error") or (isinstance(failed, list) and failed):
                _print_json(payload)
                raise SystemExit(1)
        _emit_lane_payload(payload)
        return

    if cmd == "lane-census":
        # AGT-14: no confirmation prompt — safety is dry-run default + required --task-ref.
        if args.task_ref is not None and args.task_ref.strip() == "":
            _emit_lane_payload(
                {
                    "ok": False,
                    "error": (
                        "blank_task_ref: --task-ref was provided but empty/whitespace; "
                        "pass a real task_ref."
                    ),
                }
            )
            return
        from workbay_orchestrator_mcp.orchestration.lane_census import (  # noqa: PLC0415
            census_lanes,
            report_payload,
        )

        try:
            report = census_lanes(
                args.task_ref,
                root=config.workspace_root,
                apply=args.apply,
                max_batch=args.max_batch,
            )  # apply=True also runs the acting remote-sandbox reaper
        except Exception as exc:  # noqa: BLE001 — CLI never-raise
            _emit_lane_payload({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
            return
        _emit_lane_payload(report_payload(report, task_ref=args.task_ref, apply=args.apply))
        return

    if cmd == "lane-reclaim-scan":
        _handle_lane_reclaim_scan(
            orchestrator_root=config.workspace_root,
            task_ref=args.task_ref,
            all_tasks=args.all_tasks,
            lane_id=args.lane_id,
        )
        return

    # --- list-backends ---
    if cmd == "list-backends":
        _print_json(list_available_backends(probe=args.probe))
        return

    # --- metrics ---
    if cmd == "metrics":
        print(get_metrics_summary(task_ref=args.task_ref, output_format=args.output_format))
        return

    if cmd == "ace-reflect":
        from workbay_orchestrator_mcp.orchestration.ace_reflect import run_ace_reflect  # noqa: PLC0415

        raise SystemExit(
            run_ace_reflect(
                state_dir=config.state_dir,
                playbook_files=_validated_playbook_files("ace-reflect", args.playbook_files, config.workspace_root),
                dry_run=args.dry_run,
                model_curation_backend=args.model_curation_backend,
                model_curation_model=args.model_curation_model,
                model_curation_reasoning_effort=args.model_curation_reasoning_effort,
                model_curation_threshold=args.model_curation_threshold,
                model_curation_budget_tokens=args.model_curation_budget_tokens,
            )
        )

    if cmd == "ace-curation-report":
        from workbay_orchestrator_mcp.orchestration.ace_reflect import run_curation_report  # noqa: PLC0415

        raise SystemExit(
            run_curation_report(
                playbook_files=_validated_playbook_files(
                    "ace-curation-report", args.playbook_files, config.workspace_root
                )
            )
        )

    if cmd == "ace-metrics":
        from workbay_orchestrator_mcp.orchestration.ace_metrics import (  # noqa: PLC0415
            _append_snapshot,
            build_snapshot,
            render_markdown,
        )

        playbook_files = _validated_playbook_files("ace-metrics", args.playbook_files, config.workspace_root)
        snapshot = build_snapshot(
            task_ref=args.task_ref,
            state_dir=config.state_dir,
            logs_dir=config.workspace_root / args.logs_dir,
            instruction_files=playbook_files,
        )
        _append_snapshot(config.state_dir, snapshot)
        if args.output_format == "json":
            print(json.dumps(snapshot, indent=2))
        else:
            print(render_markdown(snapshot))
        return

    if cmd == "ace-trends":
        from workbay_orchestrator_mcp.orchestration.ace_metrics import render_sparklines  # noqa: PLC0415

        print(render_sparklines(config.state_dir, args.task_ref))
        return

    parser.error(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
