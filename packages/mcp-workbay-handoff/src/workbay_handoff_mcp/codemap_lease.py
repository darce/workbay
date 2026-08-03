"""Singleton codemap reindex lease (implementation note S1) — flock mutual exclusion.

Coordinates exactly one reindex in flight per repo via an OS advisory lock
(``fcntl.flock`` on a per-``repo_instance_id`` lock file) plus a durable queue
row in ``codemap_reindex_lease``. Generation fencing ([RES-10]) ensures a
paused-then-resumed holder cannot release or complete a lease that a newer
holder owns — an advisory lock does not stop that write race on its own.

Mutual exclusion is single-node only ([DATA-06]): all participants share one
local filesystem. The kernel releases the flock when the holder process dies,
so there is no dead-holder detection, no pid liveness probe, no TTL-based
reclaim for the crash case, and no SIGTERM path. Returning ``None`` from
``acquire_reindex_lease`` means "someone else is indexing" — it is not an error.

On matching release, ``consumed_shas`` is subtracted from ``requested_shas``;
an empty remainder deletes the row, otherwise the row is rewritten to a
holder_pid=0 placeholder so mid-run queue entries survive.

``LEASE_TTL_SECONDS`` is retained only as a staleness *annotation* for
observability (``expires_at``), never as a correctness mechanism.

``repo_instance_id`` is resolved by callers via
``resolve_repo_instance_id(db_path, repo_path=...)``, which keys on the git
common dir of the explicit ``repo_path``. Resolution fails closed when git
cannot provide a common dir — a workspace-path fallback would split the
singleton across worktrees under load.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

LEASE_TTL_SECONDS = 900
RUNNER_TIMEOUT_SECONDS = 600

_GIT_TIMEOUT_SECONDS = 5
_CORRUPT_SHA_LOG_LIMIT = 200
_MAX_REQUESTED_SHAS = 128
_LOCK_DIR_NAME = ".codemap_reindex_locks"
_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9._-]+")

# Sidecar watermark: generation must stay monotonic after a matching release
# DELETEs the lease row, otherwise a delayed release(gen=N) could clear a later
# holder that re-acquired at gen=N ([RES-10]). One int per repo; does not grow.
_GEN_WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS codemap_reindex_generation (
    repo_instance_id TEXT PRIMARY KEY,
    last_generation INTEGER NOT NULL
)
"""


class LeaseUnavailable(Exception):
    """Raised when a lease-related operation cannot proceed safely.

    Contention ("someone else is indexing") is *not* this error — acquire
    returns ``None`` for that case. Use this for fail-closed conditions such as
    unresolved git common dir or unusable lock path.
    """


@dataclass(frozen=True)
class ReindexLease:
    """Handle returned by a successful ``acquire_reindex_lease``."""

    generation: int  # RES-10 fencing token; monotonic, never reused
    repo_instance_id: str
    lock_fd: int  # OS-owned; released by the kernel on process death


def _connect(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_gen_watermark(conn: sqlite3.Connection) -> None:
    conn.execute(_GEN_WATERMARK_DDL)


def _next_generation(conn: sqlite3.Connection, repo_instance_id: str) -> int:
    """Allocate the next monotonic generation for ``repo_instance_id`` ([RES-10])."""
    _ensure_gen_watermark(conn)
    row = conn.execute(
        "SELECT last_generation FROM codemap_reindex_generation WHERE repo_instance_id = ?",
        (repo_instance_id,),
    ).fetchone()
    nxt = (int(row["last_generation"]) if row is not None else 0) + 1
    conn.execute(
        """
        INSERT INTO codemap_reindex_generation (repo_instance_id, last_generation)
        VALUES (?, ?)
        ON CONFLICT(repo_instance_id) DO UPDATE SET last_generation = excluded.last_generation
        """,
        (repo_instance_id, nxt),
    )
    return nxt


def _warn_corrupt_requested_shas(repo_instance_id: str, raw: str) -> None:
    truncated = raw if len(raw) <= _CORRUPT_SHA_LOG_LIMIT else raw[:_CORRUPT_SHA_LOG_LIMIT] + "..."
    print(
        "workbay_handoff_mcp.codemap_lease: corrupt requested_shas for "
        f"repo_instance_id={repo_instance_id!r} raw={truncated!r}",
        file=sys.stderr,
    )


def _parse_requested_shas(
    raw: str | None,
    *,
    repo_instance_id: str = "?",
    fail_closed: bool = False,
) -> list[str] | None:
    """Parse the queue JSON.

    When ``fail_closed`` is False (read path), corrupt input yields ``[]`` after
    a stderr warning. When True (release settlement), corrupt input yields
    ``None`` so the caller leaves the row untouched ([AGT-10], ra_04).
    """
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        _warn_corrupt_requested_shas(repo_instance_id, raw)
        return None if fail_closed else []
    if not isinstance(data, list):
        _warn_corrupt_requested_shas(repo_instance_id, raw)
        return None if fail_closed else []
    return [str(item) for item in data]


def _placeholder_update_sql() -> str:
    return """
        UPDATE codemap_reindex_lease
        SET holder_pid = 0,
            generation = 0,
            acquired_at = '0',
            expires_at = '0',
            target_sha = '',
            requested_shas = ?
        WHERE repo_instance_id = ? AND generation = ?
        """


def _default_lock_dir(db_path: Path | str) -> Path:
    return Path(db_path).resolve().parent / _LOCK_DIR_NAME


def _lock_file_path(
    db_path: Path | str,
    *,
    repo_instance_id: str,
    lock_dir: Path | None,
) -> Path:
    base = Path(lock_dir) if lock_dir is not None else _default_lock_dir(db_path)
    safe = _SAFE_ID_RE.sub("_", repo_instance_id).strip("._") or "repo"
    # Cap length so path stays well under common FS limits.
    if len(safe) > 120:
        safe = safe[:120]
    return base / f"{safe}.lock"


def _try_acquire_flock(lock_path: Path) -> int | None:
    """Open ``lock_path`` and take LOCK_EX|LOCK_NB. Return fd, or None if held.

    Raises ``LeaseUnavailable`` when the lock file cannot be created/opened.
    """
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    except OSError as exc:
        raise LeaseUnavailable(f"cannot open codemap reindex lock file {lock_path}: {exc}") from exc
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    except OSError as exc:
        os.close(fd)
        raise LeaseUnavailable(f"fcntl.flock failed on {lock_path}: {exc}") from exc
    return fd


def _unlock_and_close(lock_fd: int | None) -> None:
    if lock_fd is None:
        return
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(lock_fd)
    except OSError:
        pass


def _grant_lease_row(
    conn: sqlite3.Connection,
    *,
    repo_instance_id: str,
    now_ts: float,
    preserved_requested: str,
) -> int:
    """Write the durable grant row; return the new generation."""
    generation = _next_generation(conn, repo_instance_id)
    acquired_at = str(now_ts)
    # expires_at is a staleness annotation only — never used for mutual exclusion.
    expires_at = str(now_ts + LEASE_TTL_SECONDS)
    holder_pid = os.getpid()  # diagnostic only; not a liveness input
    conn.execute(
        """
        INSERT INTO codemap_reindex_lease (
            repo_instance_id, holder_pid, generation,
            acquired_at, expires_at, target_sha, requested_shas
        ) VALUES (?, ?, ?, ?, ?, '', ?)
        ON CONFLICT(repo_instance_id) DO UPDATE SET
            holder_pid = excluded.holder_pid,
            generation = excluded.generation,
            acquired_at = excluded.acquired_at,
            expires_at = excluded.expires_at,
            target_sha = excluded.target_sha,
            requested_shas = excluded.requested_shas
        """,
        (
            repo_instance_id,
            holder_pid,
            generation,
            acquired_at,
            expires_at,
            preserved_requested,
        ),
    )
    return generation


def acquire_reindex_lease(
    db_path: Path | str,
    *,
    repo_instance_id: str,
    lock_dir: Path | None = None,
    now_ts: float | None = None,
) -> ReindexLease | None:
    """Return a lease, or None when another live process holds it.

    Mutual exclusion is ``fcntl.flock(fd, LOCK_EX | LOCK_NB)`` on a lock file.
    NOT the flock(1) CLI — that is absent on macOS; ``fcntl.flock`` is stdlib and
    works on both macOS and Linux. The kernel releases the lock when the holder
    process dies, so there is NO dead-holder detection, NO pid liveness probe,
    NO TTL-based reclaim for the crash case, and NO SIGTERM path.
    Returning None means 'someone else is indexing' — it is NOT an error.
    """
    ts = float(time.time() if now_ts is None else now_ts)
    lock_path = _lock_file_path(db_path, repo_instance_id=repo_instance_id, lock_dir=lock_dir)
    lock_fd = _try_acquire_flock(lock_path)
    if lock_fd is None:
        return None

    # Release the flock if connect fails; otherwise the exclusive lock is
    # stranded for the life of this process and later acquires return None.
    try:
        conn = _connect(db_path)
    except BaseException:
        _unlock_and_close(lock_fd)
        raise

    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT requested_shas FROM codemap_reindex_lease WHERE repo_instance_id = ?",
            (repo_instance_id,),
        ).fetchone()
        preserved = str(row["requested_shas"] or "[]") if row is not None else "[]"
        generation = _grant_lease_row(
            conn,
            repo_instance_id=repo_instance_id,
            now_ts=ts,
            preserved_requested=preserved,
        )
        conn.execute("COMMIT")
        return ReindexLease(
            generation=generation,
            repo_instance_id=repo_instance_id,
            lock_fd=lock_fd,
        )
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        _unlock_and_close(lock_fd)
        raise
    finally:
        conn.close()


def release_reindex_lease(
    db_path: Path | str,
    *,
    repo_instance_id: str,
    generation: int,
    consumed_shas: Sequence[str] | None = None,
    lock_fd: int | None = None,
) -> bool:
    """Release the lease when ``generation`` matches; unlock ``lock_fd``.

    Unchanged queue semantics: subtract ``consumed_shas`` from the pending queue.
    Empty remainder deletes the row; otherwise rewrite to the holder_pid=0
    placeholder so mid-run requests survive. ``consumed_shas=None`` means
    'consumed nothing'.

    Returns False when fenced (generation mismatch) — caller must then leave
    the queue alone. Also returns False (and leaves the row untouched) when
    ``requested_shas`` is corrupt so settlement cannot silently drop the queue
    ([AGT-10]). Always closes/unlocks ``lock_fd`` when provided.
    """
    try:
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT generation, requested_shas FROM codemap_reindex_lease WHERE repo_instance_id = ?",
                (repo_instance_id,),
            ).fetchone()
            if row is None or int(row["generation"]) != int(generation):
                conn.execute("COMMIT")
                return False
            requested = _parse_requested_shas(
                row["requested_shas"],
                repo_instance_id=repo_instance_id,
                fail_closed=True,
            )
            if requested is None:
                # Corrupt queue — leave the row untouched; unlock still happens
                # in finally so a stuck holder process does not wedge forever.
                conn.execute("COMMIT")
                return False
            consumed = set(consumed_shas or [])
            remaining = [s for s in requested if s not in consumed]
            if not remaining:
                conn.execute(
                    "DELETE FROM codemap_reindex_lease WHERE repo_instance_id = ? AND generation = ?",
                    (repo_instance_id, generation),
                )
            else:
                conn.execute(
                    _placeholder_update_sql(),
                    (json.dumps(remaining), repo_instance_id, generation),
                )
            conn.execute("COMMIT")
            return True
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
    finally:
        _unlock_and_close(lock_fd)


def request_reindex(
    db_path: Path | str,
    *,
    repo_instance_id: str,
    sha: str,
) -> None:
    """Append-and-coalesce ``sha`` into ``requested_shas`` without acquiring.

    Works while another process holds the lease. Does not create a live holder
    row that would block a subsequent acquire. Uses ``BEGIN IMMEDIATE``.
    """
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM codemap_reindex_lease WHERE repo_instance_id = ?",
            (repo_instance_id,),
        ).fetchone()
        if row is None:
            pending = [sha]
            # Placeholder row: holder_pid=0 is never a live holder; the flock is
            # the mutual-exclusion gate. generation=0 is not a real fencing token.
            conn.execute(
                """
                INSERT INTO codemap_reindex_lease (
                    repo_instance_id, holder_pid, generation,
                    acquired_at, expires_at, target_sha, requested_shas
                ) VALUES (?, 0, 0, '0', '0', '', ?)
                """,
                (repo_instance_id, json.dumps(pending)),
            )
        else:
            pending = _parse_requested_shas(row["requested_shas"], repo_instance_id=repo_instance_id) or []
            if sha not in pending:
                pending.append(sha)
            # Bound the queue: a hot repo can enqueue faster than the singleton
            # drains. Consumption is newest-wins, so keep the newest and evict
            # the oldest once the cap is exceeded.
            if len(pending) > _MAX_REQUESTED_SHAS:
                pending = pending[-_MAX_REQUESTED_SHAS:]
            conn.execute(
                """
                UPDATE codemap_reindex_lease
                SET requested_shas = ?
                WHERE repo_instance_id = ?
                """,
                (json.dumps(pending), repo_instance_id),
            )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def read_requested_shas(db_path: Path | str, *, repo_instance_id: str) -> list[str]:
    """Return the pending SHA queue, oldest first. [] when no row / empty / corrupt."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT requested_shas FROM codemap_reindex_lease WHERE repo_instance_id = ?",
            (repo_instance_id,),
        ).fetchone()
        if row is None:
            return []
        parsed = _parse_requested_shas(row["requested_shas"], repo_instance_id=repo_instance_id)
        return parsed if parsed is not None else []
    finally:
        conn.close()


def _resolve_git_common_dir(repo_path: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--git-common-dir"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    raw = proc.stdout.strip()
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = (repo_path / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return str(candidate)


def resolve_repo_instance_id(db_path: Path | str, *, repo_path: Path | str) -> str:
    """Resolve (or create) a ``repo_instances`` id for the given ``repo_path``.

    Derives the git common dir from ``repo_path`` via
    ``git rev-parse --git-common-dir``. Fails closed with ``LeaseUnavailable``
    when that resolution fails — never invents a workspace-path instance id
    that would split the singleton across worktrees (ra_02 / [RES-13]).

    SELECTs an existing row for that common dir and INSERTs only if absent,
    all inside a single ``BEGIN IMMEDIATE`` so concurrent first-seen resolvers
    cannot create two ids for one repo.
    """
    root = Path(repo_path).resolve()
    git_common_dir = _resolve_git_common_dir(root)
    if not git_common_dir:
        raise LeaseUnavailable(
            "cannot resolve git common dir for repo_path="
            f"{root}; refusing to invent a workspace-path repo_instance_id "
            "(would split the codemap singleton across worktrees)"
        )
    workspace_root = str(root)
    seen_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT repo_instance_id FROM repo_instances WHERE git_common_dir = ? "
            "ORDER BY created_at ASC, repo_instance_id ASC LIMIT 1",
            (git_common_dir,),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT repo_instance_id FROM repo_instances WHERE workspace_root = ? "
                "ORDER BY created_at ASC, repo_instance_id ASC LIMIT 1",
                (workspace_root,),
            ).fetchone()
        if row is not None:
            repo_instance_id = str(row["repo_instance_id"])
            conn.execute(
                """
                UPDATE repo_instances
                SET workspace_root = ?, git_common_dir = ?, last_seen_at = ?
                WHERE repo_instance_id = ?
                """,
                (workspace_root, git_common_dir, seen_at, repo_instance_id),
            )
            conn.execute("COMMIT")
            return repo_instance_id

        repo_instance_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO repo_instances (
                repo_instance_id, workspace_root, git_common_dir, created_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (repo_instance_id, workspace_root, git_common_dir, seen_at, seen_at),
        )
        conn.execute("COMMIT")
        return repo_instance_id
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


__all__ = [
    "LEASE_TTL_SECONDS",
    "RUNNER_TIMEOUT_SECONDS",
    "ReindexLease",
    "LeaseUnavailable",
    "acquire_reindex_lease",
    "release_reindex_lease",
    "request_reindex",
    "read_requested_shas",
    "resolve_repo_instance_id",
]
