from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import TypedDict

_WRITE_QUERIES = (
    (
        "decisions",
        "SELECT task_ref, session, 'decision' AS write_kind, created_at AS created_at FROM decisions",
    ),
    (
        "verified_tests",
        "SELECT task_ref, session, 'verified_test' AS write_kind, verified_at AS created_at FROM verified_tests",
    ),
    (
        "review_findings",
        "SELECT task_ref, session, 'review_finding' AS write_kind, created_at AS created_at FROM review_findings",
    ),
)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    return row is not None


def _rows_from_db(path: Path) -> dict[str, list[dict]]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "orientation_reads"):
            raise ValueError(f"{path}: no orientation_reads table (schema predates v21)")
        reads = [dict(row) for row in conn.execute("SELECT * FROM orientation_reads ORDER BY created_at ASC, id ASC")]
        reinjections = (
            [dict(row) for row in conn.execute("SELECT * FROM session_reinjections ORDER BY created_at ASC")]
            if _table_exists(conn, "session_reinjections")
            else []
        )
        writes: list[dict] = []
        for table, query in _WRITE_QUERIES:
            if _table_exists(conn, table):
                writes.extend(dict(row) for row in conn.execute(query))
        writes.sort(key=lambda row: (str(row.get("created_at") or ""), str(row.get("task_ref") or "")))
        return {"reads": reads, "writes": writes, "reinjections": reinjections}
    finally:
        conn.close()


def collect_orientation_rows(sources: list[Path], *, since: str | None = None) -> dict[str, object]:
    reads: list[dict] = []
    writes: list[dict] = []
    reinjections: list[dict] = []
    source_receipts: list[dict] = []
    for source in sources:
        packet = _rows_from_db(Path(source))
        source_reads = [row for row in packet["reads"] if since is None or str(row.get("created_at") or "") >= since]
        source_writes = [row for row in packet["writes"] if since is None or str(row.get("created_at") or "") >= since]
        source_reinjections = [
            row for row in packet["reinjections"] if since is None or str(row.get("created_at") or "") >= since
        ]
        reads.extend(source_reads)
        writes.extend(source_writes)
        reinjections.extend(source_reinjections)
        source_receipts.append(
            {
                "path": str(source),
                "reads": len(source_reads),
                "writes": len(source_writes),
                "reinjections": len(source_reinjections),
            }
        )
    return {"reads": reads, "writes": writes, "reinjections": reinjections, "sources": source_receipts}


# Session-window segmentation gap. Reads carry no session (only the reinject
# hook populates one) and writes carry the actor's own session, so the two
# never share a session key; windows must be segmented on ``task_ref`` +
# a time gap, session-agnostic, associating reads to writes by time proximity.
# The readout doc reports sensitivity to this size (3h/6h/12h).
_WINDOW_GAP_SECONDS = 6 * 3600

# Event kinds ordered so that reads/reinjections at the same instant as a write
# sort *before* it and therefore count as prior orientation for that window.
_READ = "read"
_REINJECTION = "reinjection"
_WRITE = "write"
_KIND_ORDER = {_READ: 0, _REINJECTION: 0, _WRITE: 1}


class _Event(TypedDict):
    ts: datetime
    kind: str
    row: dict[str, object]


def _arm_summary(values: list[float]) -> dict[str, object]:
    return {
        "window_count": len(values),
        "median_m2_seconds": float(median(values)) if values else None,
    }


def _events_by_task(packet: dict[str, object]) -> dict[str, list[_Event]]:
    by_task: dict[str, list[_Event]] = {}

    def _add(rows_key: str, kind: str) -> None:
        rows = packet.get(rows_key, [])
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw_ts = row.get("created_at")
            task_ref = str(row.get("task_ref") or "")
            if not raw_ts or not task_ref:
                continue
            try:
                ts = _parse_ts(str(raw_ts))
            except (ValueError, TypeError):
                continue
            by_task.setdefault(task_ref, []).append({"ts": ts, "kind": kind, "row": row})

    _add("reads", _READ)
    _add("reinjections", _REINJECTION)
    _add("writes", _WRITE)
    return by_task


def _segment_windows(events: list[_Event]) -> list[list[_Event]]:
    """Split time-ordered events into windows separated by > _WINDOW_GAP_SECONDS."""
    ordered = sorted(events, key=lambda e: (e["ts"], _KIND_ORDER.get(str(e["kind"]), 9)))
    windows: list[list[_Event]] = []
    current: list[_Event] = []
    prev_ts = None
    for event in ordered:
        ts = event["ts"]
        if prev_ts is not None and (ts - prev_ts).total_seconds() > _WINDOW_GAP_SECONDS:
            windows.append(current)
            current = []
        current.append(event)
        prev_ts = ts
    if current:
        windows.append(current)
    return windows


def build_orientation_report(packet: dict[str, object]) -> dict[str, object]:
    oriented: list[float] = []
    non_oriented: list[float] = []
    reinjected: list[float] = []
    not_reinjected: list[float] = []
    windows: list[dict[str, object]] = []
    cold_window_count = 0

    for task_ref, events in sorted(_events_by_task(packet).items()):
        for window_events in _segment_windows(events):
            writes = [e for e in window_events if e["kind"] == _WRITE]
            if not writes:
                # A window with orientation activity but no ledger write has no
                # measurable time-to-first-write; skip it (still nothing to gate on).
                continue
            first_write = min(writes, key=lambda e: e["ts"])
            first_write_at = first_write["ts"]
            prior_reads = [e for e in window_events if e["kind"] == _READ and e["ts"] <= first_write_at]
            prior_reinjections = [e for e in window_events if e["kind"] == _REINJECTION and e["ts"] <= first_write_at]
            is_oriented = bool(prior_reads)
            is_reinjected = bool(prior_reinjections)
            is_cold = not is_oriented and not is_reinjected
            if is_oriented:
                start_at = min(e["ts"] for e in prior_reads)
            elif is_reinjected:
                start_at = min(e["ts"] for e in prior_reinjections)
            else:
                # Cold window: no orientation read and no reinjection preceded
                # the first write, so the window opens at the write itself and
                # m2' is 0. These stay in the non-oriented arm (they represent
                # "wrote without orienting") rather than being dropped — dropping
                # them would starve the arm and force extend_window forever.
                start_at = first_write_at
                cold_window_count += 1
            m2_seconds = (first_write_at - start_at).total_seconds()
            (oriented if is_oriented else non_oriented).append(m2_seconds)
            (reinjected if is_reinjected else not_reinjected).append(m2_seconds)
            first_write_row = first_write["row"] if isinstance(first_write["row"], dict) else {}
            windows.append(
                {
                    "task_ref": task_ref,
                    "window_start_at": start_at.isoformat(),
                    "first_write_at": first_write_at.isoformat(),
                    "oriented": is_oriented,
                    "reinjected": is_reinjected,
                    "cold": is_cold,
                    "m2_seconds": m2_seconds,
                    "first_write_kind": first_write_row.get("write_kind"),
                }
            )

    oriented_median = _arm_summary(oriented)["median_m2_seconds"]
    non_oriented_median = _arm_summary(non_oriented)["median_m2_seconds"]
    improvement_ratio = None
    if isinstance(oriented_median, float) and isinstance(non_oriented_median, float) and non_oriented_median > 0:
        improvement_ratio = round((non_oriented_median - oriented_median) / non_oriented_median, 4)
    return {
        "total_windows": len(windows),
        "cold_window_count": cold_window_count,
        "oriented": _arm_summary(oriented),
        "non_oriented": _arm_summary(non_oriented),
        "reinjected": _arm_summary(reinjected),
        "not_reinjected": _arm_summary(not_reinjected),
        "improvement_ratio": improvement_ratio,
        "windows": windows,
    }


def apply_orientation_gate(report: dict[str, object]) -> dict[str, object]:
    oriented = report["oriented"]
    non_oriented = report["non_oriented"]
    assert isinstance(oriented, dict)
    assert isinstance(non_oriented, dict)
    oriented_count = int(oriented.get("window_count") or 0)
    non_oriented_count = int(non_oriented.get("window_count") or 0)
    oriented_median = oriented.get("median_m2_seconds")
    non_oriented_median = non_oriented.get("median_m2_seconds")
    powered = oriented_count >= 10 and non_oriented_count >= 10
    if not powered or oriented_median is None or non_oriented_median is None:
        # Underpowered or an empty arm: not enough signal to decide.
        recommendation = "extend_window"
    elif float(non_oriented_median) == 0.0:
        # Non-oriented sessions write with no measurable pre-write latency
        # (cold windows: they just write). An orientation read can only add
        # pre-write latency, so it cannot be >=20% *below* a zero baseline —
        # the readout shows orientation is not reducing time-to-first-write.
        # This is a real signal, not a degenerate one, so freeze rather than
        # extend the collection window forever.
        recommendation = "freeze"
    else:
        improvement = (float(non_oriented_median) - float(oriented_median)) / float(non_oriented_median)
        recommendation = "invest" if improvement >= 0.20 else "freeze"
    return {
        "recommendation": recommendation,
        "rule": "invest iff oriented median m2 is >=20% lower than non-oriented median and both arms have >=10 windows; underpowered extends collection",
        "min_windows_per_arm": 10,
        "required_improvement_ratio": 0.20,
        "oriented_window_count": oriented_count,
        "non_oriented_window_count": non_oriented_count,
        "oriented_median_m2_seconds": oriented_median,
        "non_oriented_median_m2_seconds": non_oriented_median,
    }
