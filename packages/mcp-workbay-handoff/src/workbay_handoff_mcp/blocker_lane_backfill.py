"""Explicit recovery for historical blocker lane attribution."""

from __future__ import annotations

import re

from .shared_schema import _get_db_connection

_LANE_DESCRIPTION_RE = re.compile(r"for lane '([^']+)'")


def backfill_blocker_lane_ids(*, apply: bool = False) -> dict[str, object]:
    """Recover missing blocker lane ids from literal ``for lane '<id>'`` text.

    The default is a read-only preview. Only rows whose lane attribution is
    currently NULL or empty and whose description contains the literal pattern
    are candidates. The guarded update makes repeated applied runs no-ops and
    never overwrites an attribution supplied by another writer.
    """
    candidates: list[dict[str, object]] = []
    with _get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, task_ref, description
            FROM blockers
            WHERE lane_id IS NULL OR lane_id = ''
            ORDER BY id
            """
        ).fetchall()
        for row in rows:
            match = _LANE_DESCRIPTION_RE.search(str(row["description"]))
            if match is None:
                continue
            candidates.append(
                {
                    "id": int(row["id"]),
                    "task_ref": str(row["task_ref"]),
                    "lane_id": match.group(1),
                }
            )

        updated = 0
        if apply:
            for candidate in candidates:
                cursor = conn.execute(
                    """
                    UPDATE blockers
                    SET lane_id = ?
                    WHERE id = ? AND (lane_id IS NULL OR lane_id = '')
                    """,
                    (candidate["lane_id"], candidate["id"]),
                )
                updated += cursor.rowcount

    return {
        "ok": True,
        "dry_run": not apply,
        "would_update": len(candidates),
        "updated": updated,
        "noop": len(candidates) == 0 if not apply else updated == 0,
        "candidates": candidates,
    }
