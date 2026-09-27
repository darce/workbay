#!/usr/bin/env python3
"""Polling live dashboard for lane worker health.

Usage:
    python3 scripts/mcp/dashboard_live.py \\
        --orchestrator-root . \\
        --task-ref <task-ref> \\
        [--lanes ui domain] \\
        [--interval 10] \\
        [--once]

Prints a compact status table for each lane on each poll cycle.
Press Ctrl-C or pass --once to exit after the first render.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from workbay_protocol import INSTRUCTIONS_RELPATH

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


# ---------------------------------------------------------------------------
# MCP helpers
# ---------------------------------------------------------------------------


def _mcp_worker_status(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    state_dir: Path | None = None,
) -> dict[str, Any]:
    """Return daemon_status dict for a lane by calling worker_daemon_ctl.py."""
    state_dir = state_dir if state_dir is not None else orchestrator_root / ".task-state"
    log_dir = orchestrator_root / "logs" / "worker-daemon"
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "worker_daemon_ctl.py"),
        "status",
        "--state-dir",
        str(state_dir),
        "--log-dir",
        str(log_dir),
        "--lane-id",
        lane_id,
        "--task-ref",
        task_ref,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            data: dict[str, Any] = json.loads(result.stdout)
            return data
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
        pass
    return {"lane_id": lane_id, "worker_state": "unknown", "state_summary": "Could not retrieve status."}


def _get_artifact_count(state_dir: Path, task_ref: str, lane_id: str) -> int | None:
    """Return number of indexed artifact sources for this lane, or None if unavailable."""
    artifact_db_path = state_dir / "mcp-artifacts.db"
    if not artifact_db_path.exists():
        return 0
    try:
        import sqlite3 as _sqlite3

        conn = _sqlite3.connect(str(artifact_db_path))
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM artifact_sources WHERE task_ref = ? AND lane_id = ?",
                [task_ref, lane_id],
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Lane discovery
# ---------------------------------------------------------------------------


def _resolve_lane_ids(
    orchestrator_root: Path,
    task_ref: str,
    requested: list[str] | None,
) -> list[str]:
    """Return the lane IDs to display: explicit list, or all from manifest."""
    if requested:
        return list(requested)
    try:
        from lane_manifest import load_manifest

        manifest = load_manifest(task_ref)
        lanes = manifest.get("lanes", {})
        if isinstance(lanes, dict):
            return sorted(lanes.keys())
    except Exception:  # noqa: BLE001
        pass
    return []


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

_STATE_SYMBOLS: dict[str, str] = {
    "executing": "[EXEC]",
    "reviewing": "[REVW]",
    "verifying": "[VRFY]",
    "handoff": "[HNDOFF]",
    "waiting_for_orchestrator": "[WAIT]",
    "idle": "[IDLE]",
    "starting": "[START]",
    "stopped": "[STOP]",
    "handoff_failed": "[FAIL]",
    "paused": "[PAUSE]",
    "unknown": "[???]",
}


def _summarize(status: dict[str, Any]) -> dict[str, Any]:
    """Extract the display-relevant fields from a daemon_status dict."""
    state = str(status.get("worker_state") or "unknown")
    attention = bool(status.get("attention_required"))
    summary = str(status.get("state_summary") or "")[:80]
    process = status.get("process")
    pid = process.get("pid") if isinstance(process, dict) else None
    obs = status.get("observability")
    history = (obs.get("history") or []) if isinstance(obs, dict) else []
    cumulative_tokens = sum(
        int((e.get("token_usage_totals") or {}).get("total_tokens") or 0) for e in history if isinstance(e, dict)
    )
    # Model and effort from latest observability entry
    latest_obs = (obs.get("latest") or {}) if isinstance(obs, dict) else {}
    model = str(latest_obs.get("model") or "") or "-"
    effort = str(latest_obs.get("effective_reasoning_effort") or "") or "-"
    last_event = status.get("last_event")
    last_ts = str(last_event.get("ts") or "")[:19].replace("T", " ") if isinstance(last_event, dict) else ""
    streak_info = None
    status_record = status.get("status_record")
    if isinstance(status_record, dict):
        streak_info = status_record.get("exhaustion_streak")
    streak = int(streak_info.get("count") or 0) if isinstance(streak_info, dict) else 0
    # Context pressure from context_utilization_latest or latest observability
    ctx_util = status.get("context_utilization_latest")
    if not isinstance(ctx_util, dict):
        ctx_util = latest_obs.get("context_utilization")
    pressure = str((ctx_util or {}).get("pressure") or "normal")
    # Stale-lock: lock file exists but process is not running
    lock_path = status.get("lock_path")
    stale_lock = False
    if lock_path and not status.get("running"):
        try:
            stale_lock = Path(lock_path).exists()
        except Exception:  # noqa: BLE001
            pass
    # Composite health: unhealthy > attention > pressure > normal
    if state == "unhealthy" or stale_lock:
        health = "UNHEALTHY"
    elif attention or streak >= 2:
        health = "ATTENTION"
    elif pressure in ("elevated", "high"):
        health = "DEGRADED"
    else:
        health = "ok"
    return {
        "state": state,
        "symbol": _STATE_SYMBOLS.get(state, f"[{state[:5].upper()}]"),
        "attention": attention,
        "pid": pid,
        "summary": summary,
        "cumulative_tokens": cumulative_tokens,
        "last_ts": last_ts,
        "exhaustion_streak": streak,
        "model": model,
        "effort": effort,
        "pressure": pressure,
        "stale_lock": stale_lock,
        "health": health,
        "cycle": int(status_record.get("cycle") or 0) if isinstance(status_record, dict) else 0,
    }


def _format_table(
    task_ref: str,
    rows: list[tuple[str, dict[str, Any]]],
    ts: str,
) -> str:
    """Format lanes as a compact status table."""
    col_lane = max((len(lane_id) for lane_id, _ in rows), default=8)
    col_state = 9
    col_health = 9
    col_model = max((len(str(info.get("model") or "-")) for _, info in rows), default=5)
    col_model = max(col_model, 5)
    # header
    lines: list[str] = [
        f"--- dashboard-live  task={task_ref}  {ts} ---",
        f"{'LANE':<{col_lane}}  {'STATE':<{col_state}}  {'HEALTH':<{col_health}}  "
        f"{'PID':>6}  {'TOKENS':>8}  {'STK':>3}  {'CYC':>3}  {'PRES':<8}  "
        f"{'ARTF':>5}  {'EFFORT':<8}  {'MODEL':<{col_model}}  SUMMARY",
        "-" * (col_lane + col_state + col_health + col_model + 70),
    ]
    for lane_id, info in rows:
        attn = "!" if info["attention"] else " "
        pid_str = str(info["pid"]) if info["pid"] else "-"
        tok_str = f"{info['cumulative_tokens']:,}" if info["cumulative_tokens"] else "-"
        streak_str = str(info["exhaustion_streak"]) if info["exhaustion_streak"] > 0 else "-"
        cycle_str = str(info.get("cycle") or 0)
        stale_mark = "[STALE-LOCK] " if info.get("stale_lock") else ""
        pressure_display = str(info.get("pressure") or "normal")
        effort_display = str(info.get("effort") or "-")[:8]
        health_display = str(info.get("health") or "ok")
        model_display = str(info.get("model") or "-")[:col_model]
        artf_count = info.get("artifact_count")
        artf_str = str(artf_count) if artf_count is not None else "-"
        lines.append(
            f"{lane_id:<{col_lane}}{attn} {info['symbol']:<{col_state}}  "
            f"{health_display:<{col_health}}  "
            f"{pid_str:>6}  {tok_str:>8}  {streak_str:>3}  {cycle_str:>3}  "
            f"{pressure_display:<8}  {artf_str:>5}  {effort_display:<8}  "
            f"{model_display:<{col_model}}  "
            f"{stale_mark}{info['summary']}"
        )
    if not rows:
        lines.append("  (no lanes found)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------


def _metrics_summary_line(task_ref: str, state_dir: Path, orchestrator_root: Path) -> str:
    """Return a one-line metrics summary string for the dashboard footer."""
    try:
        from workbay_orchestrator_mcp.orchestration.ace_metrics import build_snapshot  # noqa: PLC0415
    except ImportError:
        return "  metrics: unavailable (ace_metrics not importable)"

    try:
        snap = build_snapshot(
            task_ref=task_ref,
            state_dir=state_dir,
            logs_dir=orchestrator_root / "logs",
            instruction_files=[
                orchestrator_root / INSTRUCTIONS_RELPATH,
            ],
        )
        tb = snap["token_burn"]
        cp = snap["context_pressure"]
        ace = snap["ace_documentation"]
        tokens = f"{tb['total_tokens']:,}" if tb["data_available"] else "n/a"
        pressure = cp["latest_pressure"] if cp["data_available"] else "n/a"
        bullets = str(ace["total_strategy_bullets"]) if ace["data_available"] else "n/a"
        pruning = str(ace["pruning_candidates"]) if ace["data_available"] else "n/a"
        return f"  metrics: tokens={tokens}  pressure={pressure}  bullets={bullets}  pruning_candidates={pruning}"
    except Exception as exc:  # noqa: BLE001
        return f"  metrics: error ({exc})"


def poll_lane_status(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    interval: int,
    once: bool,
    state_dir: Path | None = None,
    show_metrics: bool = False,
) -> None:
    """Continuously poll and display lane worker status."""
    resolved_state_dir = state_dir if state_dir is not None else orchestrator_root / ".task-state"
    while True:
        import datetime

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows: list[tuple[str, dict[str, Any]]] = []
        for lane_id in lane_ids:
            status = _mcp_worker_status(orchestrator_root, task_ref, lane_id, state_dir=resolved_state_dir)
            info = _summarize(status)
            info["artifact_count"] = _get_artifact_count(resolved_state_dir, task_ref, lane_id)
            rows.append((lane_id, info))
        table = _format_table(task_ref, rows, now)
        # Clear screen (ANSI) then print
        output = "\033[2J\033[H" + table
        if show_metrics:
            output += "\n" + _metrics_summary_line(task_ref, resolved_state_dir, orchestrator_root)
        print(output, flush=True)
        if once:
            break
        time.sleep(interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live polling dashboard for lane worker health.")
    parser.add_argument(
        "--orchestrator-root",
        default=".",
        help="Path to the monorepo root (default: current directory).",
    )
    parser.add_argument("--task-ref", required=True, help="MCP task reference.")
    parser.add_argument("--lanes", nargs="*", help="Lane IDs to display (default: all manifest lanes).")
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Poll interval in seconds (default: 10).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print once and exit instead of polling continuously.",
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help="Path to the task-state directory (default: <orchestrator-root>/.task-state).",
    )
    parser.add_argument(
        "--show-metrics",
        action="store_true",
        help="Append a one-line ACE metrics summary (tokens, pressure, bullet health) after the lane table.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()

    task_ref = str(args.task_ref or (orchestrator_root.parent.name if not args.task_ref else "")).strip()
    if not task_ref:
        print("ERROR: --task-ref is required.", file=sys.stderr)
        return 1

    lane_ids = _resolve_lane_ids(orchestrator_root, task_ref, args.lanes or [])
    if not lane_ids:
        print(
            f"WARNING: No lanes found for task '{task_ref}'. Pass --lanes to specify explicitly.",
            file=sys.stderr,
        )
        # Carry on; the table will show "(no lanes found)"

    interval = max(1, int(args.interval or 10))

    state_dir: Path | None = None
    if getattr(args, "state_dir", None):
        state_dir = Path(args.state_dir).expanduser().resolve()

    try:
        poll_lane_status(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_ids=lane_ids,
            interval=interval,
            once=args.once,
            state_dir=state_dir,
            show_metrics=getattr(args, "show_metrics", False),
        )
    except KeyboardInterrupt:
        print("\ndashboard-live interrupted.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
