"""Bounded MOVE of terminal review_findings from the hot table into the archive.

Archive, never delete. Cold rows live in ``review_findings_archive`` and are
SQL-only: MCP ``list_review_findings`` / ``get_review_findings_summary`` /
``search_handoff`` / snapshot export/import continue to query
``review_findings`` only. Restore a culled row with SQL against the archive
table. ``replace_task`` cleanup also leaves the cold table untouched.

Triggered from ``reap_tasks`` / ``archive_task_state``, never from bootstrap
or ``_prepare_handoff_connection``.

File-size reclaim is a separate operator step (exclusive lock), never this
pass and never first-open bootstrap::

    sqlite3 .task-state/handoff.db 'VACUUM;'

FTS: deleting from ``review_findings`` fires ``findings_fts_delete``. This
pass then issues FTS5 ``optimize`` so deleted segments merge. That does not
shrink the SQLite file; VACUUM does.
"""

from __future__ import annotations

import logging
import sqlite3

from .enums import FindingStatus
from .shared_primitives import LIVE_ACTIVE_STATUSES, TERMINAL_REVIEW_FINDING_STATUSES

_log = logging.getLogger("workbay_handoff_mcp")

DEFAULT_REVIEW_FINDINGS_ARCHIVE_RETENTION_DAYS = 90
REVIEW_FINDINGS_ARCHIVE_BATCH_LIMIT = 500
REVIEW_FINDINGS_ARCHIVE_TABLE = "review_findings_archive"
# Operator file-size reclaim. Exclusive lock. Never invoked from bootstrap,
# ``_prepare_handoff_connection``, or ``archive_terminal_review_findings``.
REVIEW_FINDINGS_ARCHIVE_OPERATOR_VACUUM_SQL = "VACUUM;"

# Re-export so SQL placeholders and the enum cannot drift: the set is derived
# in shared_primitives as FindingStatus minus OPEN and FIXED.
assert TERMINAL_REVIEW_FINDING_STATUSES == frozenset(
    status.value for status in FindingStatus if status not in {FindingStatus.OPEN, FindingStatus.FIXED}
)

_MOVE_SAVEPOINT = "rf_archive_move"


def _normalize_archive_bounds(
    retention_days: int | None,
    batch_limit: int | None,
) -> tuple[int, int]:
    if retention_days is None:
        retention_days = DEFAULT_REVIEW_FINDINGS_ARCHIVE_RETENTION_DAYS
    if batch_limit is None:
        batch_limit = REVIEW_FINDINGS_ARCHIVE_BATCH_LIMIT
    if retention_days < 0:
        raise ValueError("retention_days must be >= 0")
    if batch_limit <= 0:
        raise ValueError("batch_limit must be > 0")
    return retention_days, batch_limit


def _pragma_column_names(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names: list[str] = []
    for row in rows:
        try:
            names.append(str(row["name"]))
        except (KeyError, IndexError, TypeError):
            names.append(str(row[1]))
    return names


def _mirrored_archive_copy_columns(conn: sqlite3.Connection) -> list[str]:
    """Intersection of hot and cold columns, preserving hot-table order.

    Never dump an unbounded hot-table PRAGMA list: a later additive column on
    ``review_findings`` that has not yet been ALTERed onto the archive twin
    would make INSERT fail with ``no such column``. ``archived_at`` is
    archive-only and is supplied by the INSERT SELECT.
    """
    hot = _pragma_column_names(conn, "review_findings")
    cold = set(_pragma_column_names(conn, REVIEW_FINDINGS_ARCHIVE_TABLE))
    columns = [name for name in hot if name in cold and name != "archived_at"]
    if not columns:
        raise sqlite3.OperationalError("no shared columns between review_findings and review_findings_archive")
    return columns


def _qualify_terminal_review_finding_ids(
    conn: sqlite3.Connection,
    *,
    retention_days: int,
    batch_limit: int,
) -> list[int]:
    statuses = tuple(sorted(TERMINAL_REVIEW_FINDING_STATUSES))
    status_placeholders = ",".join("?" for _ in statuses)
    live_statuses = tuple(LIVE_ACTIVE_STATUSES)
    live_placeholders = ",".join("?" for _ in live_statuses)
    rows = conn.execute(
        f"""
        SELECT rf.id
        FROM review_findings AS rf
        WHERE rf.status IN ({status_placeholders})
          AND rf.updated_at <= datetime('now', ?)
          AND NOT EXISTS (
            SELECT 1 FROM handoff_state AS hs
            WHERE hs.task_ref = rf.task_ref
              AND hs.status IN ({live_placeholders})
          )
        ORDER BY rf.id
        LIMIT ?
        """,
        (*statuses, f"-{int(retention_days)} days", *live_statuses, int(batch_limit)),
    ).fetchall()
    return [int(row[0] if not hasattr(row, "keys") else row["id"]) for row in rows]


def _copy_terminal_review_findings_batch(conn: sqlite3.Connection, ids: list[int]) -> int:
    """INSERT selected hot rows into the archive. Abort the batch on conflict.

    Plain INSERT (SQLite default ABORT): a PRIMARY KEY collision raises rather
    than swallowing the row. ``changes()`` must equal ``len(ids)`` before the
    caller may DELETE. Returning the qualify-list length after a conflict-skip
    insert would let a hot row that was not copied get removed. Column list is
    the hot/cold intersection so a drifted archive twin cannot abort the MOVE.
    """
    columns = _mirrored_archive_copy_columns(conn)
    col_csv = ", ".join(columns)
    id_placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"INSERT INTO {REVIEW_FINDINGS_ARCHIVE_TABLE} ({col_csv}, archived_at) "
        f"SELECT {col_csv}, datetime('now') FROM review_findings WHERE id IN ({id_placeholders})",
        ids,
    )
    inserted = int(conn.execute("SELECT changes()").fetchone()[0])
    if inserted != len(ids):
        raise sqlite3.IntegrityError(f"archive INSERT copied {inserted} of {len(ids)} rows; refusing DELETE")
    return inserted


def _delete_hot_review_findings(conn: sqlite3.Connection, ids: list[int]) -> None:
    id_placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"DELETE FROM review_findings WHERE id IN ({id_placeholders})",
        ids,
    )


def _findings_fts_present(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'findings_fts' LIMIT 1").fetchone()
    return row is not None


def _optimize_findings_fts(conn: sqlite3.Connection) -> None:
    """Merge FTS5 deleted-row segments after archival DELETEs.

    ``findings_fts_delete`` already removes matching FTS rows when a hot
    ``review_findings`` row is deleted, so search stays consistent without
    extra DML. FTS5 DELETE only marks rows deleted; it does not reclaim
    pages. ``INSERT INTO findings_fts(findings_fts) VALUES('optimize')``
    merges those segments. It still does not shrink the SQLite file;
    reclaiming bytes from the 284 MB operator DB needs a separate VACUUM
    (exclusive lock) as an operator step, not this pass.
    """
    if not _findings_fts_present(conn):
        return
    try:
        conn.execute("INSERT INTO findings_fts(findings_fts) VALUES('optimize')")
    except sqlite3.Error:
        _log.exception("findings_fts optimize after archival delete failed (hot/cold move already committed)")


class _ArchiveMoveTxn:
    """Own BEGIN IMMEDIATE, or SAVEPOINT when the caller already holds a txn.

    Must not assign ``isolation_level`` while ``in_transaction`` is true:
    Python 3.12 commits the open transaction on that assignment and then
    leaves the connection in autocommit, so a later INSERT cannot be undone
    by ROLLBACK.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.saved_isolation = conn.isolation_level
        self.owns_txn = False
        self.savepoint: str | None = None
        self.isolation_mutated = False

    def begin(self) -> None:
        if self.conn.in_transaction:
            self.savepoint = _MOVE_SAVEPOINT
            self.conn.execute(f"SAVEPOINT {self.savepoint}")
            return
        if self.conn.isolation_level is not None:
            self.conn.isolation_level = None
            self.isolation_mutated = True
        self.conn.execute("BEGIN IMMEDIATE")
        self.owns_txn = True

    def commit(self) -> None:
        if self.savepoint is not None:
            self.conn.execute(f"RELEASE SAVEPOINT {self.savepoint}")
            self.savepoint = None
            return
        if self.owns_txn:
            self.conn.execute("COMMIT")
            self.owns_txn = False

    def rollback(self) -> None:
        if self.savepoint is not None:
            try:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {self.savepoint}")
                self.conn.execute(f"RELEASE SAVEPOINT {self.savepoint}")
            except sqlite3.Error:
                pass
            self.savepoint = None
            return
        if self.owns_txn:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self.owns_txn = False

    def restore_isolation(self) -> None:
        if self.isolation_mutated and not self.conn.in_transaction:
            self.conn.isolation_level = self.saved_isolation
            self.isolation_mutated = False


def archive_terminal_review_findings(
    conn: sqlite3.Connection,
    *,
    retention_days: int | None = None,
    batch_limit: int | None = None,
) -> int:
    """Move one bounded batch of qualifying terminal findings hot → archive.

    A row qualifies when ALL of:
    - ``status`` is in ``TERMINAL_REVIEW_FINDING_STATUSES``
    - its ``task_ref`` is **not** live (``handoff_state.status`` is missing,
      ``done``, or otherwise outside ``LIVE_ACTIVE_STATUSES``). Live
      ``in_progress`` / ``review`` / ``blocked`` tasks are never culled.
      After ``archive_task_state`` deletes the live row and
      ``archives_retention_gc`` drops the matching ``task_archives`` row,
      leftover terminal hot rows still qualify.
    - ``updated_at`` is older than ``retention_days`` (default 90)

    One invocation moves at most ``REVIEW_FINDINGS_ARCHIVE_BATCH_LIMIT`` rows
    inside a single ``BEGIN IMMEDIATE`` (or a SAVEPOINT when joining an
    existing transaction). Remaining rows drain on later reap/archive
    cadences. Never invoked from bootstrap / prepare.

    FTS: deleting from ``review_findings`` fires ``findings_fts_delete``.
    After the MOVE is durable this pass issues FTS5 ``optimize`` so deleted
    segments merge; file-size reclaim still requires a separate operator
    ``VACUUM`` (see ``REVIEW_FINDINGS_ARCHIVE_OPERATOR_VACUUM_SQL``).
    """
    retention_days, batch_limit = _normalize_archive_bounds(retention_days, batch_limit)
    txn = _ArchiveMoveTxn(conn)
    moved = 0
    try:
        txn.begin()
        ids = _qualify_terminal_review_finding_ids(conn, retention_days=retention_days, batch_limit=batch_limit)
        if ids:
            inserted = _copy_terminal_review_findings_batch(conn, ids)
            _delete_hot_review_findings(conn, ids)
            moved = inserted
        txn.commit()
    except Exception:
        txn.rollback()
        raise
    finally:
        txn.restore_isolation()
    if moved:
        _optimize_findings_fts(conn)
    return moved
