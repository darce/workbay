"""Sanctioned-write rejection-rate dashboard metric (internal).

Renders ``mcp_write_rejected`` occurrences/day over a rolling 7-day window
from harvested ``agent_errors``, annotated with target ``<2/day``.

**[OBS-08] silence is not success.** A drop to zero only reads as health
when capture-liveness is proven. The metric pairs the rate with a harvest
freshness gate (newest ``agent_errors`` heartbeat age). Empty or stale
tables fire the gate loudly so a broken harvest never masquerades as
``<2/day`` success — the audit trap where post-06-20 "improvement" was
logging stoppage, not health.

Populating ``agent_errors`` (harvest itself) is implementation note's job; this
module only *renders* the rate + freshness signal and degrades to a
fired gate when the table is empty/stale.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Literal, Required, TypedDict

REJECTION_ERROR_CLASS = "mcp_write_rejected"
WINDOW_DAYS = 7
TARGET_PER_DAY = 2.0
# Heartbeat: newest agent_errors row (any class) older than this → STALE.
# Deliberately independent of (and far tighter than) the metric window: a
# multi-day-stale harvest must FIRE the gate rather than read fresh/[OK], so a
# broken harvester cannot green-wash the rate for most of the 7-day window.
FRESHNESS_MAX_AGE_HOURS = 48.0

FreshnessStatus = Literal["fresh", "stale", "empty"]


class RejectionRateMetric(TypedDict):
    """Structured rejection-rate + freshness payload for dashboard/tests."""

    window_days: Required[int]
    target_per_day: Required[float]
    rejection_count: Required[int]
    rejections_per_day: Required[float]
    freshness_status: Required[FreshnessStatus]
    last_harvest_at: Required[str | None]
    harvest_age_hours: Required[float | None]
    freshness_ok: Required[bool]
    freshness_gate_fired: Required[bool]
    meets_target: Required[bool]


def _parse_sqlite_ts(value: str) -> datetime:
    """Parse SQLite ``datetime('now')`` style timestamps as UTC-naive→aware."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "T" not in text and " " in text:
        text = text.replace(" ", "T", 1)
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _fmt_sqlite_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def compute_rejection_rate_metric(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    window_days: int = WINDOW_DAYS,
    target_per_day: float = TARGET_PER_DAY,
    freshness_max_age_hours: float = FRESHNESS_MAX_AGE_HOURS,
) -> RejectionRateMetric:
    """Compute rejections/day + [OBS-08] freshness gate from ``agent_errors``.

    Rate = ``SUM(occurrence_count)`` for ``error_class = mcp_write_rejected``
    rows with ``last_seen_at`` in the window, divided by ``window_days``.

    Freshness uses the newest ``last_seen_at`` across *all* error classes
    (event-stream heartbeat). Empty table or age > threshold fires the gate;
    ``meets_target`` is never true when the gate has fired.
    """
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if freshness_max_age_hours < 0:
        raise ValueError("freshness_max_age_hours must be non-negative")

    now_dt = now.astimezone(UTC) if now is not None else datetime.now(UTC)
    window_start = _fmt_sqlite_ts(now_dt - timedelta(days=window_days))

    if not _table_exists(conn, "agent_errors"):
        return RejectionRateMetric(
            window_days=window_days,
            target_per_day=target_per_day,
            rejection_count=0,
            rejections_per_day=0.0,
            freshness_status="empty",
            last_harvest_at=None,
            harvest_age_hours=None,
            freshness_ok=False,
            freshness_gate_fired=True,
            meets_target=False,
        )

    count_row = conn.execute(
        """
        SELECT COALESCE(SUM(occurrence_count), 0) AS n
        FROM agent_errors
        WHERE error_class = ?
          AND last_seen_at >= ?
        """,
        (REJECTION_ERROR_CLASS, window_start),
    ).fetchone()
    rejection_count = int(count_row[0] if count_row is not None else 0)
    rejections_per_day = rejection_count / float(window_days)

    hb_row = conn.execute("SELECT MAX(last_seen_at) FROM agent_errors").fetchone()
    last_harvest_at = str(hb_row[0]) if hb_row is not None and hb_row[0] is not None else None

    if last_harvest_at is None:
        freshness_status: FreshnessStatus = "empty"
        harvest_age_hours: float | None = None
        freshness_ok = False
    else:
        age = now_dt - _parse_sqlite_ts(last_harvest_at)
        harvest_age_hours = max(0.0, age.total_seconds() / 3600.0)
        if harvest_age_hours > freshness_max_age_hours:
            freshness_status = "stale"
            freshness_ok = False
        else:
            freshness_status = "fresh"
            freshness_ok = True

    freshness_gate_fired = not freshness_ok
    # Target is strict <2/day; never green when harvest liveness is unproven.
    meets_target = freshness_ok and rejections_per_day < target_per_day

    return RejectionRateMetric(
        window_days=window_days,
        target_per_day=target_per_day,
        rejection_count=rejection_count,
        rejections_per_day=rejections_per_day,
        freshness_status=freshness_status,
        last_harvest_at=last_harvest_at,
        harvest_age_hours=harvest_age_hours,
        freshness_ok=freshness_ok,
        freshness_gate_fired=freshness_gate_fired,
        meets_target=meets_target,
    )


def render_rejection_rate_section(metric: RejectionRateMetric) -> list[str]:
    """Render the REJECTION RATE dashboard section (Setext heading)."""
    rate = metric["rejections_per_day"]
    count = metric["rejection_count"]
    window = metric["window_days"]
    target = metric["target_per_day"]
    lines: list[str] = ["", "REJECTION RATE", "-" * 14]

    rate_line = f"  mcp_write_rejected: {rate:.2f}/day over {window}d ({count} rejections)  target <{target:g}/day"
    if metric["freshness_gate_fired"]:
        rate_line += "  [FRESHNESS GATE — NOT SUCCESS]"
    elif metric["meets_target"]:
        rate_line += "  [OK]"
    else:
        rate_line += "  [OVER TARGET]"
    lines.append(rate_line)

    if metric["freshness_status"] == "empty":
        lines.append("  harvest freshness: no agent_errors rows  [STALE/EMPTY]  silence is not success [OBS-08]")
    elif metric["freshness_status"] == "stale":
        age = metric["harvest_age_hours"]
        age_s = f"{age:.0f}h" if age is not None else "?"
        lines.append(
            f"  harvest freshness: last_seen {metric['last_harvest_at']} "
            f"(age {age_s})  [STALE]  silence is not success [OBS-08]"
        )
    else:
        age = metric["harvest_age_hours"]
        age_s = f"{age:.0f}h" if age is not None else "?"
        lines.append(f"  harvest freshness: last_seen {metric['last_harvest_at']} (age {age_s})  [OK]")
    return lines
