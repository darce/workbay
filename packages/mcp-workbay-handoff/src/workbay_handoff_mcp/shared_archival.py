"""Archival summary helpers for workbay_handoff_mcp.

Extracted from _shared.py (implementation note of internal). Contains:
  - _count_by_value: group-by count helper scoped to archival queries
  - ArchivalSummaryBuilder: composes archival activity summaries for a (task_ref, lane_id) pair
  - _build_archival_decision_summary: shorthand for ArchivalSummaryBuilder.decision_summary()
  - _build_archival_report_summary: shorthand for ArchivalSummaryBuilder.report_summary()
  - _build_archival_test_summary: shorthand for ArchivalSummaryBuilder.test_summary()
  - _build_archival_message_summary: shorthand for ArchivalSummaryBuilder.message_summary()
  - _build_archival_lane_activity_summary: shorthand for ArchivalSummaryBuilder.lane_activity_summary()

All symbols are re-exported from _shared.py for backward compatibility.

Imports from _shared are done at function level (late imports) to avoid a circular
module dependency: _shared.py re-exports from this module at its top level, so
module-level imports here would create a deadlock when this module is loaded first.
"""

from __future__ import annotations

import sqlite3

from .enums import FindingStatus, LaneMessageDirection, MessageStatus
from .shared_primitives import _excerpt_text, _normalize_optional_text

# Re-compute frozenset constants locally from enums (same values as in _shared.py)
_REVIEW_FINDING_STATUSES: frozenset[str] = frozenset(s.value for s in FindingStatus)
_MESSAGE_STATUSES: frozenset[str] = frozenset(s.value for s in MessageStatus)
_LANE_MESSAGE_DIRECTIONS: frozenset[str] = frozenset(d.value for d in LaneMessageDirection)


def _count_by_value(
    conn: sqlite3.Connection,
    *,
    table: str,
    field: str,
    task_ref: str,
    lane_id: str,
    allowed_values: frozenset[str],
) -> dict[str, int]:
    # This helper intentionally supports only the fixed archival-summary queries below.
    allowed_identifiers = {
        ("review_findings", "status"),
        ("lane_messages", "direction"),
        ("lane_messages", "status"),
    }
    if (table, field) not in allowed_identifiers:
        raise ValueError(f"Unsupported count identifiers: {table}.{field}")
    counts = {value: 0 for value in sorted(allowed_values)}
    rows = conn.execute(
        f"SELECT {field} AS value, COUNT(*) AS count FROM {table} WHERE task_ref = ? AND lane_id = ? GROUP BY {field}",
        (task_ref, lane_id),
    ).fetchall()
    for row in rows:
        value = _normalize_optional_text(row["value"])
        if value is not None and value in counts:
            counts[value] = int(row["count"])
    return counts


class ArchivalSummaryBuilder:
    """Composes archival activity summaries for a single (task_ref, lane_id) pair.

    Combines the five ``_build_archival_*`` helpers into a single object so the
    shared ``(conn, task_ref, lane_id)`` context is passed once instead of being
    threaded through every call.
    """

    def __init__(self, conn: sqlite3.Connection, *, task_ref: str, lane_id: str) -> None:
        self._conn = conn
        self._task_ref = task_ref
        self._lane_id = lane_id

    def decision_summary(self) -> dict[str, object]:
        decisions_total_row = self._conn.execute(
            "SELECT COUNT(*) AS count FROM decisions WHERE task_ref = ? AND lane_id = ?",
            (self._task_ref, self._lane_id),
        ).fetchone()
        latest_decision_row = self._conn.execute(
            """
            SELECT rationale
            FROM decisions
            WHERE task_ref = ? AND lane_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (self._task_ref, self._lane_id),
        ).fetchone()
        return {
            "count": int(decisions_total_row["count"]) if decisions_total_row else 0,
            "latest_rationale_excerpt": _excerpt_text(
                str(latest_decision_row["rationale"])
                if latest_decision_row and latest_decision_row["rationale"] is not None
                else None
            ),
        }

    def report_summary(self) -> dict[str, object]:
        reports_total_row = self._conn.execute(
            "SELECT COUNT(*) AS count FROM worker_reports WHERE task_ref = ? AND lane_id = ?",
            (self._task_ref, self._lane_id),
        ).fetchone()
        latest_report_row = self._conn.execute(
            """
            SELECT merge_ready
            FROM worker_reports
            WHERE task_ref = ? AND lane_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (self._task_ref, self._lane_id),
        ).fetchone()
        return {
            "count": int(reports_total_row["count"]) if reports_total_row else 0,
            "latest_merge_ready": (
                bool(latest_report_row["merge_ready"])
                if latest_report_row is not None and latest_report_row["merge_ready"] is not None
                else None
            ),
        }

    def test_summary(self) -> dict[str, object]:
        tests_summary_row = self._conn.execute(
            """
            SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed
            FROM verified_tests
            WHERE task_ref = ? AND lane_id = ?
            """,
            (self._task_ref, self._lane_id),
        ).fetchone()
        tests_total = int(tests_summary_row["total"]) if tests_summary_row else 0
        tests_passed = int(tests_summary_row["passed"]) if tests_summary_row else 0
        return {
            "total": tests_total,
            "passed": tests_passed,
            "pass_rate": round(tests_passed / tests_total, 3) if tests_total else None,
        }

    def message_summary(self) -> dict[str, object]:
        return {
            "counts_by_direction": _count_by_value(
                self._conn,
                table="lane_messages",
                field="direction",
                task_ref=self._task_ref,
                lane_id=self._lane_id,
                allowed_values=_LANE_MESSAGE_DIRECTIONS,
            ),
            "counts_by_status": _count_by_value(
                self._conn,
                table="lane_messages",
                field="status",
                task_ref=self._task_ref,
                lane_id=self._lane_id,
                allowed_values=_MESSAGE_STATUSES,
            ),
        }

    def lane_activity_summary(self) -> dict[str, object]:
        return {
            "decisions": self.decision_summary(),
            "findings": {
                "counts_by_status": _count_by_value(
                    self._conn,
                    table="review_findings",
                    field="status",
                    task_ref=self._task_ref,
                    lane_id=self._lane_id,
                    allowed_values=_REVIEW_FINDING_STATUSES,
                ),
            },
            "reports": self.report_summary(),
            "messages": self.message_summary(),
            "tests": self.test_summary(),
        }


def _build_archival_decision_summary(conn: sqlite3.Connection, *, task_ref: str, lane_id: str) -> dict[str, object]:
    return ArchivalSummaryBuilder(conn, task_ref=task_ref, lane_id=lane_id).decision_summary()


def _build_archival_report_summary(conn: sqlite3.Connection, *, task_ref: str, lane_id: str) -> dict[str, object]:
    return ArchivalSummaryBuilder(conn, task_ref=task_ref, lane_id=lane_id).report_summary()


def _build_archival_test_summary(conn: sqlite3.Connection, *, task_ref: str, lane_id: str) -> dict[str, object]:
    return ArchivalSummaryBuilder(conn, task_ref=task_ref, lane_id=lane_id).test_summary()


def _build_archival_message_summary(conn: sqlite3.Connection, *, task_ref: str, lane_id: str) -> dict[str, object]:
    return ArchivalSummaryBuilder(conn, task_ref=task_ref, lane_id=lane_id).message_summary()


def _build_archival_lane_activity_summary(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
) -> dict[str, object]:
    return ArchivalSummaryBuilder(conn, task_ref=task_ref, lane_id=lane_id).lane_activity_summary()
