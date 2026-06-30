"""Daemon startup helpers (internal follow-up).

Emits a one-shot ``logging.WARNING`` per process the first time each daemon
kind starts. The warning names the poll interval, an approximate per-cycle
MCP-query count, a qualitative token-cost note (no invented tokens/hour
figure), and the path to the event-driven rework design note.
"""

from __future__ import annotations

import logging

_LOGGER = logging.getLogger("workbay_orchestrator_mcp.daemon_startup")

_DESIGN_NOTE_PATH = "packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md"

_emitted: set[str] = set()


def _reset_emitted_for_tests() -> None:
    """Test hook: clear the per-process emit memo."""
    _emitted.clear()


def emit_daemon_startup_warning(daemon_kind: str, *, poll_interval: int) -> None:
    """Emit the one-shot daemon-startup WARNING for ``daemon_kind``.

    ``daemon_kind`` is ``"orchestrator"`` or ``"worker"``. Repeated calls in
    the same process for the same kind are suppressed so a single startup
    produces a single line in the log.
    """
    if daemon_kind in _emitted:
        return
    _emitted.add(daemon_kind)

    if daemon_kind == "orchestrator":
        queries_phrase = "~10-15 MCP queries/cycle"
        extra = ""
    elif daemon_kind == "worker":
        queries_phrase = "~3-5 MCP queries/cycle"
        extra = " Worker daemons also spawn lane_prompt.py --check subprocesses per poll."
    else:
        queries_phrase = "MCP queries/cycle"
        extra = ""

    _LOGGER.warning(
        "workbay-orchestrator-mcp: %s daemon enabled (poll_interval=%ds, %s).%s "
        "This may consume significant agent tokens over long runs. "
        "Rework candidate: see %s",
        daemon_kind,
        poll_interval,
        queries_phrase,
        extra,
        _DESIGN_NOTE_PATH,
    )
