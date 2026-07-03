#!/usr/bin/env python3
"""Interactive TUI dashboard for lane worker health.

Uses the Textual framework when available; falls back to a rich.live.Live
panel when textual is not installed.

Usage:
    python3 scripts/mcp/dashboard_tui.py \\
        --orchestrator-root . \\
        --task-ref <task-ref> \\
        [--lanes ui api] \\
        [--interval 10] \\
        [--once]

Install dashboard extras for the full interactive TUI::

    pip install -e ".[dashboard]"   # from the package that provides the dashboard extra

Without textual the dashboard runs as a rich.live auto-refreshing panel.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Re-use the data layer and formatting helpers from dashboard_live.
from dashboard_live import (
    _mcp_worker_status,
    _resolve_lane_ids,
    _summarize,
)

# ---------------------------------------------------------------------------
# Textual TUI (only loaded when textual is available)
# ---------------------------------------------------------------------------

_TEXTUAL_AVAILABLE: bool = False
try:
    import textual  # noqa: F401

    _TEXTUAL_AVAILABLE = True
except ImportError:
    pass


def _build_textual_app(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    interval: int,
) -> Any:
    """Construct and return a Textual App instance for the dashboard."""
    from textual.app import App, ComposeResult
    from textual.reactive import reactive
    from textual.widgets import DataTable, Footer, Header, Label

    class DashboardApp(App[None]):
        """Lane worker health TUI dashboard."""

        CSS = """
        DataTable { height: 1fr; }
        #status-bar { height: 1; dock: bottom; background: $surface; }
        """

        BINDINGS = [("q", "quit", "Quit"), ("r", "refresh", "Refresh now")]

        refresh_count: reactive[int] = reactive(0)

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield DataTable(id="lane-table")
            yield Label("", id="status-bar")
            yield Footer()

        def on_mount(self) -> None:
            table = self.query_one("#lane-table", DataTable)
            table.add_columns(
                "LANE",
                "STATE",
                "HEALTH",
                "PID",
                "TOKENS",
                "STK",
                "CYC",
                "PRES",
                "EFFORT",
                "MODEL",
                "SUMMARY",
            )
            self._populate_table()
            self.set_interval(interval, self._refresh_tick)

        def _refresh_tick(self) -> None:
            self.refresh_count += 1
            self._populate_table()

        def action_refresh(self) -> None:
            self.refresh_count += 1
            self._populate_table()

        def _populate_table(self) -> None:
            import datetime

            table = self.query_one("#lane-table", DataTable)
            table.clear()
            status_bar = self.query_one("#status-bar", Label)
            now = datetime.datetime.now().strftime("%H:%M:%S")
            for lane_id in lane_ids:
                raw = _mcp_worker_status(orchestrator_root, task_ref, lane_id)
                info = _summarize(raw)
                attn = "!" if info["attention"] else ""
                pid_str = str(info["pid"]) if info["pid"] else "-"
                tok_str = f"{info['cumulative_tokens']:,}" if info["cumulative_tokens"] else "-"
                streak_str = str(info["exhaustion_streak"]) if info["exhaustion_streak"] > 0 else "-"
                cycle_str = str(info.get("cycle") or 0)
                health = info.get("health") or "ok"
                table.add_row(
                    f"{lane_id}{attn}",
                    info["symbol"],
                    health,
                    pid_str,
                    tok_str,
                    streak_str,
                    cycle_str,
                    str(info.get("pressure") or "normal")[:8],
                    str(info.get("effort") or "-")[:8],
                    str(info.get("model") or "-"),
                    info["summary"],
                    key=lane_id,
                )
            status_bar.update(f"task={task_ref}  lanes={len(lane_ids)}  updated={now}  refresh #{self.refresh_count}")

    return DashboardApp()


# ---------------------------------------------------------------------------
# Rich-based live panel (fallback when textual is not available)
# ---------------------------------------------------------------------------


def _run_rich_live(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    interval: int,
    once: bool,
) -> None:
    """Rich.live.Live auto-refreshing dashboard (textual not installed)."""
    import datetime

    try:
        from rich.console import Console
        from rich.live import Live
        from rich.table import Table
    except ImportError:
        # rich not available either -- fall back to plain-text polling
        _run_plain_text(orchestrator_root, task_ref, lane_ids, interval, once)
        return

    _HEALTH_STYLES: dict[str, str] = {
        "ok": "green",
        "DEGRADED": "yellow",
        "ATTENTION": "bold yellow",
        "UNHEALTHY": "bold red",
    }
    _STATE_STYLES: dict[str, str] = {
        "executing": "cyan",
        "reviewing": "magenta",
        "verifying": "blue",
        "handoff": "green",
        "waiting_for_orchestrator": "dim",
        "idle": "dim",
        "stopped": "dim",
        "unhealthy": "bold red",
        "unknown": "dim",
    }

    def _build_table() -> Table:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table = Table(
            title=f"[bold]Lane Dashboard[/bold]  task=[cyan]{task_ref}[/cyan]  {now}",
            show_lines=False,
            expand=True,
        )
        table.add_column("LANE", style="bold", no_wrap=True)
        table.add_column("STATE", no_wrap=True)
        table.add_column("HEALTH", no_wrap=True)
        table.add_column("PID", justify="right", no_wrap=True)
        table.add_column("TOKENS", justify="right", no_wrap=True)
        table.add_column("STK", justify="right", no_wrap=True)
        table.add_column("CYC", justify="right", no_wrap=True)
        table.add_column("PRES", no_wrap=True)
        table.add_column("EFFORT", no_wrap=True)
        table.add_column("MODEL", no_wrap=True)
        table.add_column("SUMMARY")

        for lane_id in lane_ids:
            raw = _mcp_worker_status(orchestrator_root, task_ref, lane_id)
            info = _summarize(raw)
            attn_mark = "[bold red]![/bold red]" if info["attention"] else ""
            health = info.get("health") or "ok"
            health_style = _HEALTH_STYLES.get(health, "white")
            state = str(info.get("state") or "unknown")
            state_style = _STATE_STYLES.get(state, "white")
            pid_str = str(info["pid"]) if info["pid"] else "-"
            tok_str = f"{info['cumulative_tokens']:,}" if info["cumulative_tokens"] else "-"
            streak_str = (
                f"[bold red]{info['exhaustion_streak']}[/bold red]"
                if info["exhaustion_streak"] >= 2
                else (str(info["exhaustion_streak"]) if info["exhaustion_streak"] > 0 else "-")
            )
            table.add_row(
                f"{lane_id}{attn_mark}",
                f"[{state_style}]{info['symbol']}[/{state_style}]",
                f"[{health_style}]{health}[/{health_style}]",
                pid_str,
                tok_str,
                streak_str,
                str(info.get("cycle") or 0),
                str(info.get("pressure") or "normal")[:8],
                str(info.get("effort") or "-")[:8],
                str(info.get("model") or "-"),
                info["summary"],
            )

        if not lane_ids:
            table.add_row(*["(no lanes)" for _ in range(11)])
        return table

    console = Console()
    if once:
        console.print(_build_table())
        return

    with Live(
        _build_table(),
        refresh_per_second=0.5,
        console=console,
        screen=True,
    ) as live:
        try:
            while True:
                time.sleep(interval)
                live.update(_build_table())
        except KeyboardInterrupt:
            pass


# ---------------------------------------------------------------------------
# Plain text fallback (neither textual nor rich available)
# ---------------------------------------------------------------------------


def _run_plain_text(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    interval: int,
    once: bool,
) -> None:
    """Minimal plain-text fallback when neither textual nor rich is available."""
    from dashboard_live import poll_lane_status as _live_poll

    # Re-use dashboard_live's poll loop unchanged
    _live_poll(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_ids=lane_ids,
        interval=interval,
        once=once,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Interactive TUI dashboard for lane worker health.  Uses Textual when installed; falls back to rich.live."
        )
    )
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
        help="Refresh interval in seconds (default: 10).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Render once and exit (rich mode only; textual ignores this flag).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    task_ref = str(args.task_ref or "").strip()
    if not task_ref:
        print("ERROR: --task-ref is required.", file=sys.stderr)
        return 1

    lane_ids = _resolve_lane_ids(orchestrator_root, task_ref, args.lanes or [])
    if not lane_ids:
        print(
            f"WARNING: No lanes found for task '{task_ref}'. Pass --lanes to specify explicitly.",
            file=sys.stderr,
        )

    interval = max(1, int(args.interval or 10))

    if _TEXTUAL_AVAILABLE and not args.once:
        app = _build_textual_app(orchestrator_root, task_ref, lane_ids, interval)
        app.run()
    else:
        try:
            _run_rich_live(
                orchestrator_root=orchestrator_root,
                task_ref=task_ref,
                lane_ids=lane_ids,
                interval=interval,
                once=args.once,
            )
        except KeyboardInterrupt:
            print("\ndashboard-tui interrupted.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
