"""Blocked-lane aging and reaping operations."""

from __future__ import annotations

import contextvars
import errno
import fcntl
import hashlib
import json
import logging
import math
import os
import random
import re
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, closing, contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TypeVar, cast

from .lanes_support import (
    _BRANCH_PROBE_TIMEOUT_S,
    _LANE_STATUS_BLOCKED,
    _LANE_STATUS_CLOSED_STALE,
    CLOSEABLE_LANE_STATUSES,
    DEFAULT_BLOCKED_LANE_REAP_BATCH,
    _json_response,
    _normalize_optional_text,
    _workspace_root,
)
from .lanes_support import (
    _get_db_connection as _unbounded_db_connection,
)
from .orchestration.adapters._proc import terminate_process_group
from .orchestration.audit_preservation import preserve_audit_trails
from .orchestration.lane_reclaim import ReclaimVerdict, _scan_read_pass, lane_branch_reclaimable
from .orchestration.lane_terminal_dispose import BranchDisposition, classify_terminal_lane, dispose_terminal_lane
from .orchestration.worker_lock import lock_is_held, release_worker_lock, try_acquire_worker_lock

_logger = logging.getLogger(__name__)


_REGISTRY_REAP_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "registry_reap_deadline", default=None
)


def _registry_reap_remaining() -> float | None:
    deadline = _REGISTRY_REAP_DEADLINE.get()
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("registry reap deadline exceeded")
    return remaining


@contextmanager
def _get_db_connection(*, begin_immediate: bool = False, deadline: float | None = None) -> Iterator[sqlite3.Connection]:
    """Carry the sweep's absolute budget through setup, busy waits and queries."""
    sweep_deadline = _REGISTRY_REAP_DEADLINE.get()
    if sweep_deadline is not None:
        _registry_reap_remaining()
        deadline = sweep_deadline if deadline is None else min(deadline, sweep_deadline)
    kwargs: dict[str, object] = {"begin_immediate": True} if begin_immediate else {}
    if deadline is not None:
        kwargs["deadline"] = deadline
    with _unbounded_db_connection(**kwargs) as conn:
        if deadline is not None:
            conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        try:
            yield conn
        finally:
            if deadline is not None:
                conn.set_progress_handler(None, 0)


def _parse_sqlite_utc(ts: object) -> datetime | None:
    """Parse SQLite ``datetime('now')`` style timestamps as UTC-naive or aware."""
    if not isinstance(ts, str) or not ts.strip():
        return None
    raw = ts.strip().replace("T", " ")
    # Drop fractional seconds / trailing Z for fromisoformat friendliness.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if "." in raw and "+" not in raw[10:] and raw.count(":") >= 2:
        # "YYYY-MM-DD HH:MM:SS.ffffff" — keep whole string for fromisoformat
        pass
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw[:19] if len(raw) >= 19 else raw, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_lane_age_label(
    updated_at: object = None,
    created_at: object = None,
    *,
    now: datetime | None = None,
) -> str:
    """Human age label for a blocked lane (``5d``, ``12h``, ``unknown``)."""
    stamp = _parse_sqlite_utc(updated_at) or _parse_sqlite_utc(created_at)
    if stamp is None:
        return "unknown"
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    delta = current - stamp
    seconds = max(0, int(delta.total_seconds()))
    days = seconds // 86400
    if days >= 1:
        return f"{days}d"
    hours = max(1, seconds // 3600) if seconds >= 3600 else 0
    if hours >= 1:
        return f"{hours}h"
    minutes = max(1, seconds // 60) if seconds >= 60 else 0
    if minutes >= 1:
        return f"{minutes}m"
    return "0m"


def _blocker_text_from_reports(
    *,
    lane_id: str | None,
    task_ref: str | None,
    reports: Sequence[Mapping[str, object]] | None,
) -> str | None:
    if not reports:
        return None
    for report in reports:
        r_lane = _normalize_optional_text(report.get("lane_id"))
        r_task = _normalize_optional_text(report.get("task_ref"))
        if lane_id is not None and r_lane != lane_id:
            continue
        if task_ref is not None and r_task != task_ref:
            continue
        raw = report.get("blockers_json")
        if raw is None:
            raw = report.get("blockers")
        items: list[object]
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = None
            items = parsed if isinstance(parsed, list) else []
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        for item in items:
            if isinstance(item, str) and item.strip():
                return item.strip()
            if isinstance(item, Mapping):
                for key in ("description", "text", "summary", "blocker"):
                    text = _normalize_optional_text(item.get(key))
                    if text is not None:
                        return text
    return None


def _last_blocker_text_for_lane(
    conn: sqlite3.Connection | None,
    *,
    task_ref: str,
    lane_id: str,
    notes: object = None,
    reports: Sequence[Mapping[str, object]] | None = None,
) -> str:
    """Best-effort last open blocker text; degrades to notes / report / placeholder."""
    if conn is not None:
        try:
            row = conn.execute(
                """
                SELECT description FROM blockers
                WHERE status = 'open'
                  AND task_ref = ?
                  AND lane_id = ?
                ORDER BY datetime(created_at) DESC,
                  id DESC
                LIMIT 1
                """,
                (task_ref, lane_id),
            ).fetchone()
            if row is not None:
                text = _normalize_optional_text(row["description"] if isinstance(row, sqlite3.Row) else row[0])
                if text is not None:
                    return text
        except sqlite3.Error:
            pass
    from_reports = _blocker_text_from_reports(lane_id=lane_id, task_ref=task_ref, reports=reports)
    if from_reports is not None:
        return from_reports
    note = _normalize_optional_text(notes)
    if note is not None:
        return note
    return "(no blocker text)"


def _is_token_budget_blocker_shape(description: object) -> bool:
    """True for the unkeyed token-budget defect class (any in-tree variant)."""
    text = _normalize_optional_text(description)
    if text is None:
        return False
    return "token_budget" in text.casefold()


def _unique_live_lane_id(conn: sqlite3.Connection, task_ref: str) -> str | None:
    """Return the sole non-terminal lane_id for ``task_ref``, else None."""
    terminals = tuple(sorted(CLOSEABLE_LANE_STATUSES | {_LANE_STATUS_CLOSED_STALE, "archived"}))
    placeholders = ", ".join("?" for _ in terminals)
    rows = conn.execute(
        f"""
        SELECT DISTINCT lane_id FROM worktree_lanes
        WHERE task_ref = ?
          AND lane_id IS NOT NULL
          AND TRIM(lane_id) != ''
          AND COALESCE(status, '') NOT IN ({placeholders})
        """,
        (task_ref, *terminals),
    ).fetchall()
    live: list[str] = []
    for row in rows:
        raw = row["lane_id"] if isinstance(row, sqlite3.Row) else row[0]
        keyed = _normalize_optional_text(raw)
        if keyed is not None:
            live.append(keyed)
    if len(live) == 1:
        return live[0]
    return None


def reap_unkeyed_token_budget_blockers(*, apply: bool = False) -> dict[str, object]:
    """One-shot drain for legacy unkeyed token-budget blocker rows.

    Tightening ``_last_blocker_text_for_lane`` to ``lane_id = ?`` stopped
    attributing NULL/blank rows to every lane, but those rows still block
    review-ready / close-check with no purge path. This arm keys a row onto
    the task's unique live lane, or resolves it when the lane is ambiguous.

    Dry-run by default (``apply=False``). Never raises. Not invoked from the
    periodic reaper — callers must opt in.
    """
    scanned = 0
    keyed: list[dict[str, object]] = []
    closed: list[dict[str, object]] = []
    would_key: list[dict[str, object]] = []
    would_close: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    try:
        with _get_db_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, task_ref, lane_id, description, status
                FROM blockers
                WHERE status = 'open'
                  AND (lane_id IS NULL OR TRIM(COALESCE(lane_id, '')) = '')
                """
            ).fetchall()
            for row in rows:
                description = row["description"] if isinstance(row, sqlite3.Row) else row[3]
                if not _is_token_budget_blocker_shape(description):
                    continue
                scanned += 1
                blocker_id = int(row["id"] if isinstance(row, sqlite3.Row) else row[0])
                task_ref = str(row["task_ref"] if isinstance(row, sqlite3.Row) else row[1] or "")
                target_lane = _unique_live_lane_id(conn, task_ref) if task_ref else None
                entry: dict[str, object] = {
                    "id": blocker_id,
                    "task_ref": task_ref,
                    "description": description,
                    "target_lane_id": target_lane,
                }
                if target_lane is not None:
                    would_key.append(entry)
                    if not apply:
                        continue
                    try:
                        conn.execute(
                            "UPDATE blockers SET lane_id = ? WHERE id = ? AND status = 'open'",
                            (target_lane, blocker_id),
                        )
                    except sqlite3.Error as exc:
                        failed.append({**entry, "error": str(exc)})
                        continue
                    keyed.append(entry)
                    continue
                would_close.append(entry)
                if not apply:
                    continue
                try:
                    conn.execute(
                        """
                        UPDATE blockers
                        SET status = 'resolved', resolved_at = datetime('now')
                        WHERE id = ? AND status = 'open'
                        """,
                        (blocker_id,),
                    )
                except sqlite3.Error as exc:
                    failed.append({**entry, "error": str(exc)})
                    continue
                closed.append(entry)
    except Exception as exc:  # noqa: BLE001 — never-raise reaper
        return _json_response(
            {
                "ok": True,
                "applied": apply,
                "scanned": scanned,
                "keyed": keyed,
                "closed": closed,
                "would_key": would_key,
                "would_close": would_close,
                "failed": failed,
                "error": str(exc),
            }
        )
    return _json_response(
        {
            "ok": True,
            "applied": apply,
            "scanned": scanned,
            "keyed": keyed,
            "closed": closed,
            "would_key": would_key,
            "would_close": would_close,
            "failed": failed,
        }
    )


def format_blocked_lane_aging_line(entry: Mapping[str, object]) -> str:
    """Single DASHBOARD report line: age + task_ref + last blocker."""
    lane_id = entry.get("lane_id") or entry.get("id") or "?"
    task_ref = entry.get("task_ref") or "?"
    age = entry.get("age") or "unknown"
    blocker = entry.get("blocker") or entry.get("last_blocker") or "(no blocker text)"
    # Keep single-line for dashboard; trim long blocker text.
    blocker_text = str(blocker).replace("\n", " ").strip()
    if len(blocker_text) > 120:
        blocker_text = blocker_text[:117] + "..."
    return f"  ⚠ {lane_id}  task={task_ref}  age={age}  blocker: {blocker_text}"


def collect_blocked_lane_aging_entries(
    lanes: Sequence[Mapping[str, object]],
    *,
    reports: Sequence[Mapping[str, object]] | None = None,
    now: datetime | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, object]]:
    """Build aging report entries for blocked lanes (dashboard + reaper share shape)."""
    entries: list[dict[str, object]] = []
    for lane in lanes:
        if str(lane.get("status") or "") != _LANE_STATUS_BLOCKED:
            continue
        lane_id = _normalize_optional_text(lane.get("lane_id")) or str(lane.get("id") or "?")
        task_ref = _normalize_optional_text(lane.get("task_ref")) or "?"
        age = format_lane_age_label(lane.get("updated_at"), lane.get("created_at"), now=now)
        blocker = _last_blocker_text_for_lane(
            conn,
            task_ref=task_ref,
            lane_id=lane_id,
            notes=lane.get("notes"),
            reports=reports,
        )
        entries.append(
            {
                "id": lane.get("id"),
                "task_ref": task_ref,
                "lane_id": lane_id,
                "status": _LANE_STATUS_BLOCKED,
                "worktree_path": lane.get("worktree_path"),
                "branch": lane.get("branch"),
                "updated_at": lane.get("updated_at"),
                "created_at": lane.get("created_at"),
                "age": age,
                "blocker": blocker,
                "notes": lane.get("notes"),
            }
        )
    return entries


def _probe_worktree_gone(worktree_path: object) -> bool | None:
    """Return True if path is gone, False if present, None if probe unavailable."""
    path = _normalize_optional_text(worktree_path)
    if path is None:
        # Empty worktree path: treat as gone (nothing on disk to recover).
        return True
    try:
        return not Path(path).exists()
    except OSError:
        return None


#: Identities for the five git forks ``_probe_branch_dead`` routes through
#: ``_run_reclaim_command``. The shared tri-state mapper takes one of these
#: so a mutant cannot collapse one site's None-to-unknown without naming it.
_PROBE_SITE_REMOTE_PREFIX = "remote_prefix"
_PROBE_SITE_MERGE_BASE = "merge_base"
_PROBE_SITE_LOCAL_SHOW_REF = "local_show_ref"
_PROBE_SITE_REMOTES_LIST = "remotes_list"
_PROBE_SITE_REMOTE_SHOW_REF = "remote_show_ref"
_PROBE_SITES = frozenset(
    {
        _PROBE_SITE_REMOTE_PREFIX,
        _PROBE_SITE_MERGE_BASE,
        _PROBE_SITE_LOCAL_SHOW_REF,
        _PROBE_SITE_REMOTES_LIST,
        _PROBE_SITE_REMOTE_SHOW_REF,
    }
)
#: Git's defined negative (exit 1) is only a site-specific answer at
#: ``merge-base --is-ancestor`` and ``show-ref --verify``. ``git remote``
#: listing has no defined-negative; nonzero there is unknown.
_PROBE_SITES_DEFINED_NEGATIVE = frozenset(
    {
        _PROBE_SITE_MERGE_BASE,
        _PROBE_SITE_LOCAL_SHOW_REF,
        _PROBE_SITE_REMOTE_SHOW_REF,
    }
)


def _probe_command_tristate(
    site: str,
    result: subprocess.CompletedProcess[str] | None,
) -> bool | None:
    """Map a reclaim-command result onto unknown / the site's own answer.

    ``None`` from ``_run_reclaim_command`` means the command could not run
    at all — that is unknown, never live or dead. Returncode 0 is the
    site's positive. Returncode 1 is git's defined negative only at the
    merge-base and show-ref sites. Any other exit, an unknown site name,
    or a missing result is unknown. Neither collapse is allowed: a helper
    ``None`` must not become a confident True/False, and a defined 0/1
    must not become unknown [OBS-08].
    """
    if site not in _PROBE_SITES:
        return None
    if result is None:
        return None
    if result.returncode == 0:
        return True
    if result.returncode == 1 and site in _PROBE_SITES_DEFINED_NEGATIVE:
        return False
    return None


def _probe_command_stdout(result: subprocess.CompletedProcess[str] | None) -> str:
    """Stdout of a command that already mapped to the site's positive."""
    if result is None:
        return ""
    return result.stdout or ""


def _probe_branch_dead(
    branch: object,
    *,
    repo_root: Path | None = None,
    integration_ref: str = "HEAD",
) -> bool | None:
    """Return True if branch is deleted or merged into *integration_ref*.

    ``HEAD`` remains the default for the non-terminal close-path liveness
    check. Terminal worktree reclaim passes the task's recorded target branch
    so a lane landed on a feature integration ref is not mistaken for live.

    Resolution order:
    1. Normalize the name (strip ``refs/heads/``; strip a leading ``<remote>/``
       only when that segment names a configured remote — never a blind first
       path-segment strip).
    2. If ``refs/heads/<name>`` exists, judge by merge into *integration_ref*.
    3. Else consult every ``refs/remotes/<remote>/<name>``: ANY unmerged remote
       tip forces live (False). Only when every present remote tip is merged
       (or no remote carries the name) is the branch dead. Remote sort order
       is irrelevant to the verdict.
    4. Else the branch is genuinely gone → True.
    Unexpected git exit codes and unrunnable commands degrade to None.
    Probe failure never raises. The reaper write-lock barrier still fails
    closed if this runs while SQLite RESERVED is held.
    """
    name = _normalize_optional_text(branch)
    if name is None:
        return None
    compare_ref = _normalize_optional_text(integration_ref)
    if compare_ref is None:
        return None
    cwd = repo_root
    if cwd is None:
        try:
            cwd = _workspace_root()
        except Exception:  # noqa: BLE001 — probe degrade
            return None
    try:
        cwd_str = str(cwd)
        ref = name
        if ref.startswith("refs/heads/"):
            ref = ref[len("refs/heads/") :]
        # Strip a leading configured-remote prefix only (e.g. origin/feature/x →
        # feature/x). Do not strip the first path segment of feature/x → x.
        slash = ref.find("/")
        if slash > 0:
            maybe_remote = ref[:slash]
            remotes_list = _run_reclaim_command(
                ["git", "-C", cwd_str, "remote"],
                timeout=_BRANCH_PROBE_TIMEOUT_S,
            )
            # Site 1: listing failed or could not run → unknown, do not guess
            # whether the first path segment is a configured remote.
            if _probe_command_tristate(_PROBE_SITE_REMOTE_PREFIX, remotes_list) is not True:
                return None
            configured = {line.strip() for line in _probe_command_stdout(remotes_list).splitlines() if line.strip()}
            if maybe_remote in configured:
                ref = ref[slash + 1 :]

        def _merged_into_integration(git_ref: str) -> bool | None:
            merged = _run_reclaim_command(
                ["git", "-C", cwd_str, "merge-base", "--is-ancestor", git_ref, compare_ref],
                timeout=_BRANCH_PROBE_TIMEOUT_S,
            )
            return _probe_command_tristate(_PROBE_SITE_MERGE_BASE, merged)

        # Local branch ref.
        show = _run_reclaim_command(
            ["git", "-C", cwd_str, "show-ref", "--verify", "--quiet", f"refs/heads/{ref}"],
            timeout=_BRANCH_PROBE_TIMEOUT_S,
        )
        local_present = _probe_command_tristate(_PROBE_SITE_LOCAL_SHOW_REF, show)
        if local_present is None:
            return None
        if local_present is True:
            return _merged_into_integration(f"refs/heads/{ref}")

        # Local missing: consult remote-tracking refs before concluding dead.
        remotes_list = _run_reclaim_command(
            ["git", "-C", cwd_str, "remote"],
            timeout=_BRANCH_PROBE_TIMEOUT_S,
        )
        if _probe_command_tristate(_PROBE_SITE_REMOTES_LIST, remotes_list) is not True:
            return None
        remotes = [line.strip() for line in _probe_command_stdout(remotes_list).splitlines() if line.strip()]
        saw_remote = False
        any_unmerged = False
        any_unknown = False
        for remote in remotes:
            remote_ref = f"refs/remotes/{remote}/{ref}"
            rshow = _run_reclaim_command(
                ["git", "-C", cwd_str, "show-ref", "--verify", "--quiet", remote_ref],
                timeout=_BRANCH_PROBE_TIMEOUT_S,
            )
            remote_present = _probe_command_tristate(_PROBE_SITE_REMOTE_SHOW_REF, rshow)
            if remote_present is None:
                return None
            if remote_present is True:
                saw_remote = True
                verdict = _merged_into_integration(remote_ref)
                if verdict is False:
                    any_unmerged = True
                elif verdict is None:
                    any_unknown = True
        if not saw_remote:
            # Neither local nor any remote carries the name → genuinely gone.
            return True
        # X2: any unmerged remote copy forces live; sort order does not matter.
        if any_unmerged:
            return False
        if any_unknown:
            return None
        return True
    except (OSError, subprocess.TimeoutExpired, subprocess.SubprocessError):
        return None


def _reclaim_integration_ref(
    *,
    task_ref: object,
    target_branch: object,
    repo_root: Path | str | None,
) -> tuple[str, str | None]:
    """Resolve a lane's landing ref, retaining malformed manifest evidence.

    The task manifest's integration branch is the row's lane integration ref.
    A missing field falls back to the task target, then ``main``. A manifest
    that is present but cannot be read is returned as an error so callers keep
    the worktree and report the uncertainty instead of silently trying a
    weaker ref.
    """
    fallback = _normalize_optional_text(target_branch) or _RECLAIM_INTEGRATION_REF
    task = _normalize_optional_text(task_ref)
    if task is None:
        return fallback, None
    try:
        from .orchestration.lane_postmerge import _read_manifest_integration_branch  # noqa: PLC0415

        root = Path(repo_root) if repo_root is not None else _workspace_root()
        manifest_ref = _read_manifest_integration_branch(task, repo=root, orchestrator_root=None)
    except Exception as exc:  # noqa: BLE001 — invalid present manifest must fail closed
        return fallback, f"task_manifest_integration_branch_unresolved: {exc}"
    return manifest_ref or fallback, None


def _classify_blocked_lane_liveness(
    *,
    worktree_gone: bool | None,
    branch_dead: bool | None,
) -> tuple[str, str]:
    """Conclusive-dead only when BOTH probes prove dead; else report-only classes."""
    if worktree_gone is True and branch_dead is True:
        return "dead", "worktree gone and branch merged/deleted"
    if worktree_gone is False and branch_dead is False:
        return "alive", "worktree present and branch still live"
    if worktree_gone is None or branch_dead is None:
        return "ambiguous", "probe unavailable or inconclusive"
    # Exactly one condition proven dead — ambiguous (do NOT close).
    if worktree_gone is True and branch_dead is not True:
        return "ambiguous", "worktree gone but branch not proven merged/deleted"
    if branch_dead is True and worktree_gone is not True:
        return "ambiguous", "branch merged/deleted but worktree still present"
    return "ambiguous", "inconclusive liveness"


# ---------------------------------------------------------------------------
# Guarded worktree reclamation
#
# ``_classify_blocked_lane_liveness`` above is and stays a PURE function: it
# only reads two booleans. A lane whose branch merged while its worktree stayed
# on disk therefore classifies "ambiguous" forever unless something removes the
# worktree — and nothing did, so the row waited for a removal nobody performed
# while the worktree waited for a reap the row never reached. The reclamation
# below breaks that deadlock in the CALLER: remove under guards, re-probe, then
# re-classify with the same pure classifier.
#
# Every guard fails closed [SECD-05]: an unreadable probe refuses the removal.
# ---------------------------------------------------------------------------

#: A lane branch must be fully merged into this ref before its worktree may go.
_RECLAIM_INTEGRATION_REF = "main"
#: Registration grace for the registry sweep, mirroring ``reap_blocked_lanes``'
#: ``min_age_hours``. The row-driven arm already refuses to garbage-collect a
#: freshly upserted row; the registry arm is the one that actually deletes
#: files, and it had no floor at all. Aged off the worktree's own admin
#: ``gitdir`` stamp because the sweep runs from git, not from rows -- during a
#: projection outage there is no row to age (internal).
_REGISTRY_SWEEP_MIN_AGE_HOURS = 24.0

# Rollback-bundle retention is per-branch: one churning lane must never evict
# another lane's only restore point [RLSE-08].
_DEFAULT_BUNDLE_RETENTION_COUNT = 5
_BUNDLE_DIR_NAME = "branch-reclaim-bundles"
# Per-branch retention alone is unbounded in *branch count*: each bundle carries
# the branch's full history (~36 MB against this repo's main), and a repo that
# reaps hundreds of lanes converges on tens of GB with nothing evicting across
# branches. Anything that grows needs a same-rate purge shipped with it, so the
# directory carries an absolute ceiling too. Each branch's newest bundle is
# exempt: the global cap must never be the thing that destroys a lane's only
# restore point [RES-07][RLSE-08].
_DEFAULT_BUNDLE_DIR_MAX_BYTES = 2 * 1024**3
#: One ps snapshot of every process. STAT is requested so zombies can be
#: excluded: ``ps -p <pid>`` exits 0 for a zombie, so a dead worker otherwise
#: reads as a live owner and no worktree is ever reclaimable.
_PS_SNAPSHOT_CMD: tuple[str, ...] = ("ps", "-Ao", "pid=,stat=,command=")
#: One lsof snapshot of every process cwd (``-Fpn`` → ``p<pid>`` / ``n<path>``).
#: Deliberately NOT ``+D <worktree>``: that stats the whole tree per lane, and
#: this runs twice per candidate inside a batch sweep.
_LSOF_CWD_SNAPSHOT_CMD: tuple[str, ...] = ("lsof", "-w", "-d", "cwd", "-Fpn")
_RECLAIM_PROBE_TIMEOUT_S = 20.0
_BUNDLE_VERIFY_TIMEOUT_S = 300.0
_BUNDLE_VERIFY_SCRATCH_PREFIX = "workbay-bundle-verify-"
_BUNDLE_VERIFY_SCRATCH_MAX_AGE_S = 30 * 60.0
# The reachability walk enumerates every object on every ref, so it is the one
# reclaim probe whose cost scales with repository history rather than with the
# worktree. Given its own budget so a slow walk degrades to "unproven" -- the
# at-risk cell -- instead of borrowing time from the cheap guards.
_RECLAIM_REACHABILITY_TIMEOUT_S = 120.0
# repo_root -> (roots digest, monotonic stamp, reachable blob shas). Memoised so
# a sweep over many dirty lanes walks the object graph once per distinct ref
# state. The TTL is a second, independent bound on what the digest cannot see.
# It does *not* exist for per-worktree ref namespaces: measured on git 2.39.5, a
# commit held only by ``refs/worktree/*`` or ``refs/bisect/*`` in a linked
# worktree is NOT reached by ``rev-list --all`` from the main worktree, so those
# namespaces are outside the pool on both sides and cannot make the memo stale.
# What is left over is real but narrower -- ``info/grafts`` and replace-object
# rewrites that change reachability without moving a ref, and a ``gc``/``prune``
# landing between the key and its use -- and the orchestrator is a long-lived
# daemon, so without a TTL that residual window would be measured in process
# uptime rather than in seconds [REF-09].
_REACHABLE_BLOBS_CACHE: dict[str, tuple[str, float, set[str]]] = {}
_REACHABLE_BLOBS_TTL_S = 120.0
# One entry is ~84k shas / ~10MB for this repo, and the TTL gates *serving*, not
# retention -- an expired entry is never returned but was never dropped either.
# A daemon that sweeps several checkouts would accumulate one of those per root
# forever, so the map is bounded and evicted oldest-first.
_REACHABLE_BLOBS_CACHE_MAX_ROOTS = 4
_RECLAIM_TIMEOUT_RETURN_CODE = 124
_RECLAIM_DETAIL_CAP = 4096
_RECLAIM_MUTATION_GRACE_ENV = "WORKBAY_RECLAIM_TERMINATION_GRACE_S"
_RECLAIM_MUTATION_GRACE_DEFAULT_S = 5.0
_RECLAIM_MUTATION_GRACE_MAX_S = 30.0
_STALE_REF_LOCK_AGE_ENV = "WORKBAY_STALE_REF_LOCK_S"
_STALE_REF_LOCK_AGE_DEFAULT_S = 900.0
_STALE_REF_LOCK_OBSERVATION_GAP_S = 2.0
_stale_ref_lock_observation_sleep = time.sleep
_STALE_REF_LOCK_HOLDER_TIMEOUT_S = 5.0
_STALE_REF_RECOVERY_LOCK_TIMEOUT_S = 3.0
_STALE_REF_RECOVERY_LOCK_RETRY_MIN_S = 0.01
_STALE_REF_RECOVERY_LOCK_RETRY_MAX_S = 0.05
_recovery_lock_sleep = time.sleep
_STALE_REF_LOCK_DETAIL_CAP = 300
_UNABLE_TO_CREATE_LOCK_RE = re.compile(
    r"Unable to create ['\"](?P<path>[^'\"]+\.lock)['\"]:\s*File exists", re.IGNORECASE
)
_UNABLE_TO_CREATE_TEMPFILE_RE = re.compile(
    r"unable to create file\s+['\"]?(?P<path>[^'\"\r\n]+?\.new)['\"]?:\s*File exists",
    re.IGNORECASE,
)
_CANNOT_LOCK_REF_RE = re.compile(r"cannot lock ref ['\"](?P<ref>[^'\"]+)['\"]", re.IGNORECASE)
#: Lifecycle roots that are recreated by task-start/bootstrap.  The tracked
#: bootstrap ledger extends this set per worktree; these roots are the fallback
#: for older worktrees and small repositories without a ledger.
#: ``_root_rule`` grants a whole subtree with no leaf grammar, so a directory
#: listed here authorizes deleting *anything* under it.  Only roots whose entire
#: contents are machine-derived may appear.  ``docs/workbay/rules``,
#: ``docs/workbay/templates`` and ``.github/prompts`` were removed after a review
#: empirically deleted a hand-authored ``.github/prompts/*.prompt.md`` through
#: the prefix grant: all three are directories humans author in directly, and
#: ``/.github/prompts/`` is the standard VS Code / Copilot custom-prompt-file
#: location.  A ledger surface still reaches those paths per worktree, but only
#: for the specific files the ledger names -- that is provenance, not a subtree.
_REGENERABLE_IGNORED_ROOTS: frozenset[str] = frozenset(
    {
        ".venv",
        ".cursor/hooks.json",
    }
)
_REGENERABLE_CACHE_DIR_NAMES: frozenset[str] = frozenset({"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"})
#: Roots holding per-worktree runtime state: liveness stamps, locks, breaker
#: state and daemon logs that a later run re-derives and that nothing outside
#: this worktree ever reads.  Unlike the content-identity rules, this one makes
#: no claim that a surviving copy exists somewhere -- it claims the bytes are
#: worth nothing once the worktree is gone.  That is a strictly stronger
#: assertion, so the list is enumerated per file wherever a directory grant
#: would also sweep up something else.  Deliberately excluded:
#:
#: * ``.task-state`` itself -- it holds the audit trails below, and in the
#:   *primary* checkout the canonical ``handoff.db``.  A linked worktree never
#:   holds that DB (``RuntimeConfig.for_repo`` resolves every linked worktree to
#:   the primary via ``git rev-parse --git-common-dir``), but the directory
#:   still is not uniformly regenerable.
#: * ``.task-state/branch_naming_overrides.log``,
#:   ``.task-state/branch_naming_push_overrides.log`` and
#:   ``.task-state/lifecycle_guard_bypass.jsonl`` -- sole-copy operator audit
#:   trails of commit/publish-time leniency and of lifecycle-guard bypasses.  Nothing
#:   re-derives an audit record; outliving the run is its whole point.
#: * ``.task-state/review-adjudication/`` -- the receipt recording a merge
#:   verdict against a named subject tip.  Re-deriving it means re-reviewing.
#: * bare ``logs`` and bare ``.workbay-cache`` -- only the machine-written
#:   subtrees below are audited, and a root grants its whole subtree.
_RUNTIME_STATE_REGENERABLE_ROOTS: frozenset[str] = frozenset(
    {
        # ``lane_census`` remote-sandbox reap scheduling: a lock, its state
        # lock, and a "last run at / last outcome" stamp the next sweep
        # overwrites wholesale.
        ".task-state/.remote-sandbox-reap.lock",
        ".task-state/remote-sandbox-reap.json",
        ".task-state/remote-sandbox-reap-state.lock",
        # ``projection_queue`` breaker state and the replay lock; both are
        # re-derived from the queue on the next projection attempt.
        ".task-state/projection-breaker.json",
        ".task-state/projection-replay.lock",
        # Scratch directory minted under state by the orchestrator's own runs.
        ".task-state/tmp",
        # Worker-daemon status for the literal fixture lane id the suite uses.
        # A real lane id is not covered: only this one is provably a test
        # artifact rather than a record of work.
        ".task-state/worker-test-lane.status.json",
        # Daemon-written logs only.  ``worker_daemon_ctl`` places its per-lane
        # JSONL here and the orchestrator daemon its reap heartbeat; both
        # describe a process inside the worktree that is being removed.
        "logs/daemon",
        "logs/worker-daemon",
        # Bootstrap seeds ``.workbay-cache`` as "the check-all stamp-skip cache
        # root ... a runtime-generated local dir" (``install.py``); a stamp only
        # records that a gate already passed at some tree state.
        ".workbay-cache/checkall-stamps",
    }
)
#: The allowlist is this rule's entire authority, so an oversized one is not a
#: list to truncate -- truncating would authorize an arbitrary subset chosen by
#: iteration order.  Mirrors ``_MAX_PAYLOAD_PROVENANCE_ROOTS``: over the bound,
#: no roots at all.  The audited list is well under half this.
_MAX_RUNTIME_STATE_ROOTS = 24
#: A ledger is useful provenance, not unlimited authority to erase a subtree.
#: The current bootstrap profile has fewer than half this many surfaces.  An
#: oversized ledger contributes no roots so corruption cannot change which
#: arbitrary ignored files are eligible for deletion.
_MAX_REGENERABLE_LEDGER_ROOTS = 64
_SUPPORTED_BOOTSTRAP_LEDGER_SCHEMA_VERSION = 2
# ``shared`` is rematerialized from the pinned payload and ``lifecycle`` from
# bootstrap's lifecycle payload. ``local`` is explicitly operator-owned (and a
# retired surface may be its last copy), so neither can authorize deletion.
_REGENERABLE_SURFACE_SOURCES: frozenset[str] = frozenset({"generated", "lifecycle", "shared"})
# ``action`` is point-in-time install provenance, never standing authority to
# delete.  ``created`` records only that the file did not exist when bootstrap
# ran in the *primary repo*; it cannot say whether the user has since edited it,
# and the entries carrying it are exactly the deep-merge targets users extend
# with their own servers and credentials (``.claude/settings.json``,
# ``.cursor/mcp.json``, ``.vscode/mcp.json``).  No action value re-derives that
# fact per lane worktree, so none authorizes deletion.  The live tracked ledger
# carries zero ``created`` entries, so this costs no real reclaim capability.
# Re-populating this set requires an install-time content hash that still
# matches at reclaim time, not a verb.
_REGENERABLE_CONFIG_ACTIONS: frozenset[str] = frozenset()
#: Ledger ``provenance_key`` values of this form name the tracked directory a
#: surface was hoisted from (``link:packages/.../payload``).
_PROVENANCE_LINK_PREFIX = "link:"
#: Content identity is bounded by the roots the ledger itself declares.  A
#: corrupt or oversized ledger must not turn classification into an unbounded
#: hunt for any file anywhere that happens to match.
_MAX_PAYLOAD_PROVENANCE_ROOTS = 8
#: Above this size a candidate is not compared and contributes no rule.  The
#: bound keeps a reclaim probe from streaming an arbitrarily large ignored file.
_MAX_IDENTITY_COMPARE_BYTES = 8 * 1024 * 1024
_IDENTITY_COMPARE_CHUNK = 65536
_HEARTBEAT_FINAL_RE = re.compile(r"[A-Za-z0-9_.@+\-]{1,120}__[0-9a-f]{16}\.json\Z")
_HEARTBEAT_TEMP_RE = re.compile(r"(?P<final>.+\.json)\.\d+\.\d+\.tmp\Z")
#: Durable claim kind written into the worker lock while a reaper holds it
#: from the authorizing liveness sample through CAS close. Dispatch
#: rematerialize takes the same flock and must refuse while this claim is live.
_REAP_CLAIM_KIND = "reaping"
#: Env-gated rendezvous after acquire, before probe-authorized close.
_REAP_CLAIM_BARRIER_ENV = "WORKBAY_TEST_REAP_CLAIM_BARRIER"
#: Env-gated rendezvous after the recency re-probe and before CAS close.
#: Tests interleave rematerialize in the window the spanning claim exists to seal.
_REAP_CLOSE_WINDOW_BARRIER_ENV = "WORKBAY_TEST_REAP_CLOSE_WINDOW_BARRIER"


def _assert_no_reaper_write_lock(*, reason: str) -> None:
    """Fail closed if this thread holds SQLite RESERVED (CON-18 / CON-21).

    Reclaim probes, flock, and ``git worktree remove`` must not run while a
    write transaction is open. The handoff barrier is the same chokepoint
    used by ``run_subprocess`` / ``acquire_flock``.
    """
    from workbay_handoff_mcp.shared_write_context import (  # noqa: PLC0415
        assert_no_write_lock_held,
    )

    assert_no_write_lock_held(reason)


def _run_reclaim_command(
    argv: Sequence[str],
    *,
    timeout: float = _RECLAIM_PROBE_TIMEOUT_S,
    stdin_text: str | None = None,
    mutating: bool | None = None,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str] | None:
    """Run a probe/removal command. ``None`` means the command could not run.

    ``stdin_text`` exists for ``git hash-object --stdin``, which is the only way
    to hash bytes git would store that are not the bytes on disk -- a symlink,
    whose blob is its target path.
    """
    remaining = _registry_reap_remaining()
    if remaining is not None:
        timeout = min(timeout, remaining)
    _assert_no_reaper_write_lock(reason="subprocess")
    command = list(argv)
    if mutating is None:
        mutating = _is_reclaim_git_mutation(command)
    if mutating:
        return _run_reclaim_mutation(command, timeout=timeout, stdin_text=stdin_text, env=env)
    try:
        return subprocess.run(  # noqa: S603 — fixed argv, no shell
            command,
            input=stdin_text,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            timeout=timeout,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            list(argv),
            _RECLAIM_TIMEOUT_RETURN_CODE,
            stdout=exc.stdout if isinstance(exc.stdout, str) else "",
            stderr=f"reclaim_probe_timeout:{timeout:g}s",
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return None


def _is_reclaim_git_mutation(argv: Sequence[str]) -> bool:
    """Recognize ref-writing and worktree-removal commands in the reclaim path."""
    if not argv or Path(str(argv[0])).name != "git":
        return False
    tokens = [str(token) for token in argv[1:]]
    if "update-ref" in tokens:
        return True
    for index, token in enumerate(tokens[:-1]):
        if token == "worktree" and tokens[index + 1] == "remove":
            return True
        if token == "bundle" and tokens[index + 1] == "create":
            return True
        if token == "branch" and tokens[index + 1].casefold() in {"-d", "--delete"}:
            return True
    return False


def _reclaim_mutation_grace_s() -> float:
    raw = os.environ.get(_RECLAIM_MUTATION_GRACE_ENV)
    if raw is None:
        return _RECLAIM_MUTATION_GRACE_DEFAULT_S
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _RECLAIM_MUTATION_GRACE_DEFAULT_S
    if not math.isfinite(value):
        return _RECLAIM_MUTATION_GRACE_DEFAULT_S
    return min(max(value, 0.0), _RECLAIM_MUTATION_GRACE_MAX_S)


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _signal_process_group(process_group_id: int, sig: int) -> bool:
    try:
        os.killpg(process_group_id, sig)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _timeout_output_text(raw: object) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "surrogateescape")
    return raw if isinstance(raw, str) else ""


#: The only mutations a reclaim fence grants its lock to: ``git`` builtins whose own processes keep
#: inherited descriptors open until they exit. Each entry is the subcommand and, where the
#: subcommand alone is not a mutation, the allowed second tokens.
_FENCED_GIT_MUTATIONS: dict[str, frozenset[str] | None] = {
    "worktree": frozenset({"remove", "move"}),
    "branch": frozenset({"-d", "-D", "--delete"}),
    "bundle": frozenset({"create"}),
    "update-ref": None,
}
#: Fenced subcommands that write refs and so run the reference-transaction hook.
_FENCED_REF_MUTATIONS = frozenset({"branch", "update-ref"})
#: Environment that would let git run code from somewhere this contract has not audited.
_FENCED_MUTATION_REFUSED_ENV = ("GIT_EXEC_PATH",)
#: Pinned under every fenced mutation, after the caller's options so they win: no hook outside a
#: verified guard directory, no fsmonitor hook or daemon, no automatic (detached) gc or maintenance.
_FENCED_MUTATION_PINNED_CONFIG = ("core.fsmonitor=false", "gc.auto=0", "maintenance.auto=false")
_RECLAIM_GUARD_HOOK_NAME = "reference-transaction"
_reclaim_guard_hook_bytes: bytes | None = None


def _expected_reclaim_guard_hook() -> bytes:
    """The bytes ``branch_reclaim_delete._write_sha_guard_hook`` installs, rendered once."""
    global _reclaim_guard_hook_bytes  # noqa: PLW0603 — immutable after the first render
    if _reclaim_guard_hook_bytes is None:
        from .orchestration.branch_reclaim_delete import _write_sha_guard_hook  # noqa: PLC0415

        with tempfile.TemporaryDirectory(prefix="workbay-reclaim-hook-render-") as rendered:
            _write_sha_guard_hook(Path(rendered))
            _reclaim_guard_hook_bytes = (Path(rendered) / _RECLAIM_GUARD_HOOK_NAME).read_bytes()
    return _reclaim_guard_hook_bytes


def _is_reclaim_guard_hook_dir(directory: str) -> bool:
    """Whether ``directory`` holds exactly the SHA guard hook and nothing else, none of it a symlink."""
    try:
        path = Path(directory)
        if not path.is_absolute() or not stat.S_ISDIR(os.lstat(path).st_mode):
            return False
        with os.scandir(path) as scan:
            entries = [entry.name for _, entry in zip(range(2), scan)]
        hook = path / _RECLAIM_GUARD_HOOK_NAME
        return (
            entries == [_RECLAIM_GUARD_HOOK_NAME]
            and stat.S_ISREG(os.lstat(hook).st_mode)
            and hook.read_bytes() == _expected_reclaim_guard_hook()
        )
    except (OSError, ImportError, ValueError):
        return False


def _fenced_mutation_custody(argv: Sequence[str], env: Mapping[str, str] | None) -> tuple[str, list[str]]:
    """Bound a mutation spawned under a reclaim fence to the supported custody contract.

    Returns ``(refusal, command)``. ``refusal`` is ``""`` and ``command`` is what runs when ``argv``
    is a supported consumer: ``git`` with only ``-C <dir>`` and a ``core.hooksPath`` naming a verified
    SHA guard directory as global options, then a subcommand from ``_FENCED_GIT_MUTATIONS``. Every
    other consumer (another program, a shell, a script, an unknown git option) is refused before it
    runs, because it could close the inherited lock descriptor and keep mutating after the fence is
    released [SECD-01].

    Git itself keeps inherited descriptors open in every process it starts, so the remaining
    handoffs are the ones its configuration can add; ``command`` pins them shut after the caller's
    options: hooks run only from the verified guard directory (otherwise from nowhere), and neither
    fsmonitor nor automatic gc or maintenance, which may detach, can start. A ref mutation in a
    repository with its own reference-transaction hook is refused rather than run with that hook
    bypassed, as ``_delete_merged_branch`` does. This bounds the authority of a fenced mutation; it
    is not a proof about arbitrary processes.
    """
    if not argv or argv[0] != "git":
        return ("unsupported_consumer", [])
    effective_env = os.environ if env is None else env
    if any(name in effective_env for name in _FENCED_MUTATION_REFUSED_ENV):
        return ("unsupported_environment", [])
    tokens = [str(token) for token in argv[1:]]
    options: list[str] = []
    root: str | None = None
    hooks_dir: str | None = None
    index = 0
    while index < len(tokens) and tokens[index].startswith("-"):
        if index + 1 >= len(tokens):
            return ("unsupported_git_option", [])
        option, value = tokens[index], tokens[index + 1]
        if option == "-C" and root is None:
            root = value
            options += [option, value]
        elif option == "-c" and hooks_dir is None and value.startswith("core.hooksPath="):
            hooks_dir = value.partition("=")[2]
            if not _is_reclaim_guard_hook_dir(hooks_dir):
                return ("unsupported_hook", [])
        else:
            return ("unsupported_git_option", [])
        index += 2
    subcommand = tokens[index:]
    if not subcommand or subcommand[0] not in _FENCED_GIT_MUTATIONS:
        return ("unsupported_git_mutation", [])
    allowed = _FENCED_GIT_MUTATIONS[subcommand[0]]
    if allowed is not None and (len(subcommand) < 2 or subcommand[1] not in allowed):
        return ("unsupported_git_mutation", [])
    if hooks_dir is None and subcommand[0] in _FENCED_REF_MUTATIONS:
        hook_state, _detail = _reference_transaction_hook_state(Path(root or "."))
        if hook_state != "absent":
            return (f"reference_transaction_hook_{hook_state}", [])
    pinned = [token for item in _FENCED_MUTATION_PINNED_CONFIG for token in ("-c", item)]
    pinned += ["-c", f"core.hooksPath={hooks_dir if hooks_dir is not None else os.devnull}"]
    return ("", ["git", *options, *pinned, *subcommand])


#: Every asynchronous signal whose handler raises in the post-merge hook: the rowless watchdog
#: (SIGALRM), the hook's SIGTERM -> SystemExit, and SIGINT, which the interpreter's default handler
#: turns into KeyboardInterrupt. ``_run_reclaim_mutation`` holds exactly this set while no scope owns
#: its child (spawn to handle assignment) and while it kills and reaps, and restores exactly this set.
#: The hold defers handlers rather than masking across spawn: a child keeps the mask it is spawned
#: under, and git with SIGTERM blocked could not remove its lock files on a graceful termination.
_MUTATION_OWNERSHIP_SIGNALS = frozenset({signal.SIGALRM, signal.SIGINT, signal.SIGTERM})


def _run_reclaim_mutation(
    argv: list[str], *, timeout: float, stdin_text: str | None, env: Mapping[str, str] | None
) -> subprocess.CompletedProcess[str] | None:
    """Run a mutation in its own group and give its cleanup handler a grace period.

    Under a reclaim fence the mutation inherits every fence lock descriptor this context holds, so
    each lock stays held until the mutation and every process it spawns have exited or closed it
    (``_run_under_reclaim_fence`` protection model, item 4). That holds on every exit from here: a
    timeout or an error signals the group but proves nothing, and a process that survives the
    signals keeps the lock.

    Under a fence only a supported consumer runs (``_fenced_mutation_custody``), and only on the
    thread that holds the fence: a context copied into another thread would hand the lock to a
    mutation the fence does not wait for. Anything else is refused before it runs and returns
    ``None``, the same as a command that could not run. Without a fence nothing changes.
    """
    remaining = _registry_reap_remaining()
    if remaining is not None:
        timeout = min(timeout, remaining)
    fence_descriptors = _ACTIVE_RECLAIM_FENCE_LOCK_FDS.get()
    if fence_descriptors:
        command: list[str] = []
        if _ACTIVE_RECLAIM_FENCE_THREAD.get() != threading.get_ident():
            refusal = "fence_thread_handoff"
        else:
            refusal, command = _fenced_mutation_custody(argv, env)
        if refusal:
            _logger.warning("reclaim_mutation_refused:%s: %s", refusal, " ".join(str(token) for token in argv)[:300])
            return None
        argv = command
    # Python runs signal handlers only on the main thread, so only there can a handler raise
    # between spawn and the scope that owns the child. Hold every signal in
    # _MUTATION_OWNERSHIP_SIGNALS from spawn until ``process`` is assigned and again from the
    # finally until the group is killed and reaped: a wrapper defers its handler, whichever thread
    # the kernel delivered it to. Held signals are delivered only where the finally below reaps.
    # Signal state is touched only on the main thread; workers keep their handlers and mask.
    process: subprocess.Popen[str] | None = None
    previous_handlers: dict[int, Any] = {}
    previous_sigmask: set[signal.Signals] | None = None
    deferred_signals: set[int] = set()
    holding_signals = True
    exited = False

    def defer_held_signal(signum: int, frame: Any) -> None:
        # A native thread can receive a process-wide signal and queue its Python handler on the
        # main thread even while the main thread masks it. Honor the hold and the mask here too.
        if holding_signals or signum in signal.pthread_sigmask(signal.SIG_BLOCK, set()):
            deferred_signals.add(signum)
        elif callable(handler := previous_handlers.get(signum)):
            handler(signum, frame)

    def deliver_held_signals() -> None:
        if previous_sigmask is None:
            return
        # Queue each held signal on this thread while it is blocked, then restore the caller's
        # mask: every one is delivered, even if the first handler raises.
        signal.pthread_sigmask(signal.SIG_BLOCK, _MUTATION_OWNERSHIP_SIGNALS)
        try:
            while deferred_signals:
                signal.raise_signal(deferred_signals.pop())
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_sigmask)

    try:
        if threading.current_thread() is threading.main_thread():
            previous_sigmask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            for signum in _MUTATION_OWNERSHIP_SIGNALS:
                handler = signal.getsignal(signum)
                if callable(handler):
                    previous_handlers[signum] = handler
                    signal.signal(signum, defer_held_signal)
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE if stdin_text is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="surrogateescape",
                start_new_session=True,
                env=env,
                pass_fds=fence_descriptors,
            )
        except (OSError, subprocess.SubprocessError, UnicodeError):
            return None
        # Ownership transferred: a held signal's handler may raise from here, inside this scope.
        holding_signals = False
        deliver_held_signals()

        # One scope owns the group from spawn to reap, including delivery of a held signal above or
        # a signal in a handler below. A normal exit keeps descendant custody.
        try:
            stdout, stderr = process.communicate(input=stdin_text, timeout=timeout)
            exited = True
            return subprocess.CompletedProcess(argv, process.returncode, stdout or "", stderr or "")
        except subprocess.TimeoutExpired as timeout_error:
            grace_s = _reclaim_mutation_grace_s()
            term_started = time.monotonic()
            _signal_process_group(process.pid, signal.SIGTERM)
            stdout = _timeout_output_text(timeout_error.output)
            stderr = _timeout_output_text(timeout_error.stderr)
            killed = False
            try:
                term_stdout, term_stderr = process.communicate(timeout=grace_s)
                stdout = term_stdout or stdout
                stderr = term_stderr or stderr
            except subprocess.TimeoutExpired as term_error:
                stdout = _timeout_output_text(term_error.output) or stdout
                stderr = _timeout_output_text(term_error.stderr) or stderr

            deadline = term_started + grace_s
            while _process_group_exists(process.pid) and time.monotonic() < deadline:
                time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
            if _process_group_exists(process.pid):
                killed = True
                _signal_process_group(process.pid, signal.SIGKILL)
                try:
                    kill_stdout, kill_stderr = process.communicate(timeout=max(1.0, min(5.0, grace_s or 1.0)))
                    stdout = kill_stdout or stdout
                    stderr = kill_stderr or stderr
                except subprocess.TimeoutExpired as kill_error:
                    stdout = _timeout_output_text(kill_error.output) or stdout
                    stderr = _timeout_output_text(kill_error.stderr) or stderr

            outcome = "killed" if killed else "terminated_gracefully"
            marker = f"reclaim_mutation_timeout:{timeout:g}s; {outcome}"
            stderr = f"{stderr.rstrip()}\n{marker}" if stderr.strip() else marker
            return subprocess.CompletedProcess(argv, _RECLAIM_TIMEOUT_RETURN_CODE, stdout, stderr)
        except (OSError, subprocess.SubprocessError, UnicodeError):
            _signal_process_group(process.pid, signal.SIGTERM)
            try:
                process.communicate(timeout=_reclaim_mutation_grace_s())
            except (OSError, subprocess.SubprocessError, UnicodeError):
                pass
            if _process_group_exists(process.pid):
                _signal_process_group(process.pid, signal.SIGKILL)
                try:
                    process.communicate(timeout=5.0)
                except (OSError, subprocess.SubprocessError, UnicodeError):
                    pass
            return None
    finally:
        # First statement, before any call: a handler queued from here on is deferred, not run.
        holding_signals = True
        try:
            if process is not None and not exited:
                terminate_process_group(process)
                # Once a handler has reaped the leader the helper cannot look its group up; under
                # start_new_session the group id is the leader's pid, so a surviving member is still hit.
                _signal_process_group(process.pid, signal.SIGKILL)
                with suppress(OSError, subprocess.SubprocessError):
                    process.wait(timeout=5.0)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        with suppress(OSError):
                            stream.close()
        finally:
            # Restore the caller's exact handlers and mask on all exits, each step even if an earlier
            # one raises; held signals are delivered last, once the child is reaped.
            with ExitStack() as restore:
                restore.callback(deliver_held_signals)
                for signum, handler in previous_handlers.items():
                    restore.callback(signal.signal, signum, handler)


def _stale_ref_lock_age_threshold_s() -> float:
    raw = os.environ.get(_STALE_REF_LOCK_AGE_ENV)
    if raw is None:
        return _STALE_REF_LOCK_AGE_DEFAULT_S
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _STALE_REF_LOCK_AGE_DEFAULT_S
    if not math.isfinite(value):
        return _STALE_REF_LOCK_AGE_DEFAULT_S
    return max(0.0, value)


def _git_common_dir(repo_root: Path | str) -> Path | None:
    root = Path(repo_root)
    proc = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--git-common-dir"])
    raw = (proc.stdout or "").strip() if proc is not None and proc.returncode == 0 else ""
    if not raw:
        return None
    common = Path(raw)
    if not common.is_absolute():
        common = root / common
    try:
        return common.resolve(strict=True)
    except OSError:
        return None


def _ref_lock_identity(path: Path) -> tuple[int, int, int] | None:
    try:
        info = path.lstat()
    except OSError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    return (info.st_ino, info.st_size, info.st_mtime_ns)


def _ref_lock_stat_details(path: Path, threshold_s: float) -> dict[str, object]:
    try:
        info = path.lstat()
    except OSError:
        return {"age_s": None, "size": None, "stale_after_s": threshold_s}
    age_s = max(0.0, time.time() - info.st_mtime)
    return {
        "age_s": round(age_s, 3),
        "size": info.st_size,
        "stale_after_s": round(max(0.0, threshold_s - age_s), 3),
    }


def _has_lock_contention_diagnostic(stderr: str, match: re.Match[str] | None) -> bool:
    if match is None:
        return False
    tail = stderr[match.end() :].casefold()
    return any(
        marker in tail
        for marker in ("unable to create", "file exists", "lock file", "another git process", "already locked")
    )


def _ref_lock_busy_info(repo_root: Path | str, stderr: str) -> dict[str, object] | None:
    """Classify Git's lock-busy stderr and attach the concrete lock metadata."""
    if not stderr:
        return None
    lock_match = _UNABLE_TO_CREATE_LOCK_RE.search(stderr)
    tempfile_match = _UNABLE_TO_CREATE_TEMPFILE_RE.search(stderr)
    ref_match = _CANNOT_LOCK_REF_RE.search(stderr)
    if lock_match is None and tempfile_match is None and ref_match is None:
        return None

    root = Path(repo_root)
    temp_path = tempfile_match is not None
    path_match = tempfile_match if temp_path else lock_match
    raw_path = path_match.group("path") if path_match is not None else ""
    if raw_path:
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = root / candidate
        if temp_path:
            common = _git_common_dir(root)
            if common is None:
                return None
            candidate = Path(os.path.abspath(candidate))
            if candidate != common / "packed-refs.new":
                return None
    else:
        ref = ref_match.group("ref") if ref_match is not None else ""
        common = _git_common_dir(root)
        if common is None or not ref.startswith("refs/"):
            if not _has_lock_contention_diagnostic(stderr, ref_match):
                return None
            return {
                "lock_path": None,
                "age_s": None,
                "size": None,
                "stale_after_s": _stale_ref_lock_age_threshold_s(),
                "lock_kind": "ref_lock",
                "stderr": stderr[:_STALE_REF_LOCK_DETAIL_CAP],
            }
        candidate = common / f"{ref}.lock"
        if not _has_lock_contention_diagnostic(stderr, ref_match) and not candidate.exists():
            packed_refs_lock = common / "packed-refs.lock"
            if not packed_refs_lock.exists():
                return None
            candidate = packed_refs_lock
    candidate = Path(os.path.abspath(candidate))
    try:
        lock_path = candidate.parent.resolve(strict=False) / candidate.name
    except OSError:
        lock_path = candidate
    info = _ref_lock_stat_details(lock_path, _stale_ref_lock_age_threshold_s())
    return {
        "lock_path": str(lock_path),
        "lock_kind": "packed_refs_tempfile"
        if temp_path
        else ("packed_refs_lock" if lock_path.name == "packed-refs.lock" else "ref_lock"),
        "stderr": stderr[:_STALE_REF_LOCK_DETAIL_CAP],
        **info,
    }


def _probe_ref_lock_holders(lock_path: Path) -> tuple[str, str]:
    """Return (held|free|unavailable|error, detail) using lsof as the holder proof."""
    lsof = shutil.which("lsof")
    if not lsof:
        return ("unavailable", "lsof_unavailable")
    _assert_no_reaper_write_lock(reason="ref_lock_holder_probe")
    try:
        proc = subprocess.run(
            [lsof, "-t", "--", str(lock_path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_STALE_REF_LOCK_HOLDER_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        return ("error", f"lsof_failed:{type(exc).__name__}")
    if proc.returncode == 0 and (proc.stdout or "").strip():
        return ("held", (proc.stdout or "").strip()[:_STALE_REF_LOCK_DETAIL_CAP])
    if proc.returncode == 1 and not (proc.stdout or "").strip() and not (proc.stderr or "").strip():
        return ("free", "")
    return ("error", (proc.stderr or proc.stdout or f"lsof_rc_{proc.returncode}").strip()[:_STALE_REF_LOCK_DETAIL_CAP])


def _stale_ref_lock_kind(candidate: Path) -> str:
    if candidate.name == "packed-refs.new":
        return "packed_refs_tempfile"
    return "packed_refs_lock" if candidate.name == "packed-refs.lock" else "ref_lock"


def _ref_recovery_result(
    candidate: Path,
    status: str,
    details: dict[str, object] | None = None,
    **extra: object,
) -> dict[str, object]:
    return {
        "recovered": status in {"recovered", "recovered_receipt_failed"},
        "status": status,
        "lock_path": str(candidate),
        "lock_kind": _stale_ref_lock_kind(candidate),
        **(details or {}),
        **extra,
    }


def _validate_stale_ref_lock_path(root: Path, common: Path, lock_path: Path | str) -> tuple[Path | None, str]:
    candidate = Path(lock_path)
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = Path(os.path.abspath(candidate))
    try:
        raw_info = candidate.lstat()
    except OSError:
        raw_info = None
    if raw_info is not None and stat.S_ISLNK(raw_info.st_mode):
        return None, "lock_path_is_symlink"
    try:
        resolved = candidate.resolve(strict=False)
    except OSError:
        resolved = candidate.absolute()
    if resolved != candidate:
        return None, "lock_path_uses_symlink"
    try:
        relative = resolved.relative_to(common)
    except ValueError:
        return None, "lock_path_outside_common_git_dir"
    allowed = relative in {Path("packed-refs.lock"), Path("packed-refs.new")} or (
        relative.parts and relative.parts[0] == "refs" and relative.name.endswith(".lock")
    )
    return (resolved, "") if allowed else (None, "lock_path_not_ref_lock")


def _prepare_stale_ref_lock_evidence(common: Path) -> tuple[Path | None, str]:
    state_dir = common.parent / ".task-state"
    evidence_dir = state_dir / "stale-ref-locks"
    try:
        if state_dir.is_symlink() or evidence_dir.is_symlink():
            raise OSError("evidence_directory_symlink")
        evidence_dir.mkdir(parents=True, exist_ok=True)
        resolved = evidence_dir.resolve(strict=True)
        resolved.relative_to(common.parent.resolve(strict=True))
        return resolved, ""
    except (OSError, ValueError) as exc:
        return None, f"{type(exc).__name__}: {exc}"[:_STALE_REF_LOCK_DETAIL_CAP]


def _path_is_absent(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False


def _stale_ref_lock_identity_fence(
    candidate: Path,
    threshold_s: float,
) -> tuple[tuple[int, int, int] | None, dict[str, object], str]:
    packed_lock = candidate.parent / "packed-refs.lock" if candidate.name == "packed-refs.new" else None
    if packed_lock is not None and not _path_is_absent(packed_lock):
        return None, _ref_lock_stat_details(candidate, threshold_s), "packed_refs_lock_present"
    identity = _ref_lock_identity(candidate)
    details = _ref_lock_stat_details(candidate, threshold_s)
    if identity is None:
        return None, details, "lock_missing_or_not_regular"
    age_s = details.get("age_s")
    if not isinstance(age_s, (int, float)) or age_s < threshold_s:
        return None, details, "not_stale"
    _stale_ref_lock_observation_sleep(_STALE_REF_LOCK_OBSERVATION_GAP_S)
    if packed_lock is not None and not _path_is_absent(packed_lock):
        return None, _ref_lock_stat_details(candidate, threshold_s), "packed_refs_lock_present"
    second_identity = _ref_lock_identity(candidate)
    second_details = _ref_lock_stat_details(candidate, threshold_s)
    return (identity, second_details, "") if second_identity == identity else (None, second_details, "identity_changed")


def _write_stale_ref_lock_receipt(receipt_path: Path, receipt: dict[str, object]) -> str:
    try:
        with receipt_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(receipt, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"[:_STALE_REF_LOCK_DETAIL_CAP]
    return ""


def _archive_stale_ref_lock(
    candidate: Path,
    evidence_dir: Path,
    identity: tuple[int, int, int],
    details: dict[str, object],
) -> dict[str, object]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    evidence_path = evidence_dir / f"{stamp}-{candidate.name}-{os.getpid()}-{time.time_ns()}"
    try:
        os.rename(candidate, evidence_path)
    except OSError as exc:
        return _ref_recovery_result(
            candidate,
            "rename_failed",
            details,
            detail=f"{type(exc).__name__}: {exc}"[:_STALE_REF_LOCK_DETAIL_CAP],
        )
    receipt_path = evidence_dir / "receipts.jsonl"
    receipt = {
        "recovered_at": datetime.now(timezone.utc).isoformat(),
        "lock_path": str(candidate),
        "evidence_path": str(evidence_path),
        "identity": {"inode": identity[0], "size": identity[1], "mtime_ns": identity[2]},
        "age_s": details.get("age_s"),
        "holder_probe": "free",
    }
    receipt_error = _write_stale_ref_lock_receipt(receipt_path, receipt)
    status = "recovered" if not receipt_error else "recovered_receipt_failed"
    return _ref_recovery_result(
        candidate,
        status,
        details,
        evidence_path=str(evidence_path),
        receipt_path=str(receipt_path),
        receipt_error=receipt_error or None,
    )


def _recover_stale_ref_lock_under_guard(
    candidate: Path,
    evidence_dir: Path,
    threshold_s: float,
) -> dict[str, object]:
    identity, details, fence_status = _stale_ref_lock_identity_fence(candidate, threshold_s)
    if fence_status:
        return _ref_recovery_result(candidate, fence_status, details)
    holder_state, holder_detail = _probe_ref_lock_holders(candidate)
    if holder_state != "free":
        status = "holder_present" if holder_state == "held" else f"holder_probe_{holder_state}"
        return _ref_recovery_result(candidate, status, details, detail=holder_detail)
    if _ref_lock_identity(candidate) != identity:
        return _ref_recovery_result(
            candidate,
            "recovery_candidate_changed",
            _ref_lock_stat_details(candidate, threshold_s),
        )
    if candidate.name == "packed-refs.new" and not _path_is_absent(candidate.parent / "packed-refs.lock"):
        return _ref_recovery_result(
            candidate, "packed_refs_lock_present", _ref_lock_stat_details(candidate, threshold_s)
        )
    return _archive_stale_ref_lock(candidate, evidence_dir, cast(tuple[int, int, int], identity), details)


def _recover_stale_ref_lock_serialized(
    candidate: Path,
    evidence_dir: Path,
    threshold_s: float,
) -> dict[str, object]:
    lock_path = evidence_dir / ".recover.lock"
    descriptor = -1
    acquired = False
    try:
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(lock_path, flags, 0o600)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return _ref_recovery_result(candidate, "recovery_lock_unavailable", detail="lock_path_not_regular")

        wait_started_s = time.monotonic()
        deadline = wait_started_s + max(0.0, _STALE_REF_RECOVERY_LOCK_TIMEOUT_S)
        attempted = False

        def lock_busy_result() -> dict[str, object]:
            return _ref_recovery_result(
                candidate,
                "recovery_lock_busy",
                waited_s=round(max(0.0, time.monotonic() - wait_started_s), 3),
            )

        while True:
            if attempted and time.monotonic() >= deadline:
                return lock_busy_result()
            attempted = True
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                    raise
                remaining_s = deadline - time.monotonic()
                if remaining_s <= 0:
                    return lock_busy_result()
                sleep_s = random.uniform(
                    _STALE_REF_RECOVERY_LOCK_RETRY_MIN_S,
                    _STALE_REF_RECOVERY_LOCK_RETRY_MAX_S,
                )
                _recovery_lock_sleep(min(sleep_s, remaining_s))

        try:
            return _recover_stale_ref_lock_under_guard(candidate, evidence_dir, threshold_s)
        finally:
            if acquired:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                acquired = False
    except OSError as exc:
        return _ref_recovery_result(
            candidate,
            "recovery_lock_unavailable",
            detail=f"{type(exc).__name__}: {exc}"[:_STALE_REF_LOCK_DETAIL_CAP],
        )
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def recover_stale_ref_lock(repo_root: Path | str, lock_path: Path | str) -> dict[str, object]:
    """Move a provably stale Git ref lock to reversible evidence and receipt it."""
    root = Path(repo_root)
    try:
        root = root.resolve(strict=True)
    except OSError:
        return {"recovered": False, "status": "repo_root_unavailable", "lock_path": str(lock_path)}
    common = _git_common_dir(root)
    if common is None:
        return {"recovered": False, "status": "common_git_dir_unavailable", "lock_path": str(lock_path)}
    candidate, path_error = _validate_stale_ref_lock_path(root, common, lock_path)
    if candidate is None:
        return {"recovered": False, "status": path_error, "lock_path": str(lock_path)}
    evidence_dir, evidence_error = _prepare_stale_ref_lock_evidence(common)
    if evidence_dir is None:
        return _ref_recovery_result(candidate, "evidence_directory_unavailable", detail=evidence_error)
    return _recover_stale_ref_lock_serialized(candidate, evidence_dir, _stale_ref_lock_age_threshold_s())


def _resolved_path_text(path: object) -> str:
    text = _normalize_optional_text(path) or (str(path) if path is not None else "")
    if not text:
        return ""
    try:
        return str(Path(text).resolve())
    except OSError:
        return text


def _worktree_path_spellings(worktree_path: object) -> list[str]:
    """Raw and resolved spellings of a worktree path, deduped, empties dropped."""
    raw = _normalize_optional_text(worktree_path)
    if raw is None:
        return []
    spellings = [raw.rstrip("/") or raw]
    resolved = _resolved_path_text(raw).rstrip("/")
    if resolved and resolved not in spellings:
        spellings.append(resolved)
    return spellings


def _ps_caller_ancestry(ps_output: str, *, pid: int) -> set[int]:
    """Exclude nothing unless the complete caller chain reaches parent zero."""
    parents: dict[int, int] = {}
    for line in ps_output.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and parts[0].isdigit() and parts[1].isdigit():
            child, parent = int(parts[0]), int(parts[1])
            if child in parents:
                return set()
            parents[child] = parent
    ancestry: set[int] = set()
    while pid:
        if pid in ancestry or pid not in parents or len(ancestry) >= 64:
            return set()
        ancestry.add(pid)
        pid = parents[pid]
    return ancestry


def _ps_argv_worktree_owners(
    ps_output: str, *, worktree_paths: Sequence[str], excluded_pids: set[int] | None = None
) -> list[str]:
    """PURE: ``pid ppid stat command`` records whose argv names one of the paths.

    A path is named when followed by end of string or a character outside the
    POSIX portable filename set ``[A-Za-z0-9._-]``. This fails closed for unusual
    separators while rejecting sibling paths that continue the filename.

    Zombie filter: a record whose STAT starts with ``Z`` is excluded. A zombie
    holds no working directory and cannot be using the worktree, but it is
    still visible to ``ps`` and to ``ps -p``, so counting it as an owner would
    let one dead worker pin a worktree forever.
    """
    needles = [p for p in worktree_paths if p]
    owners: list[str] = []
    if not needles:
        return owners
    for line in ps_output.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        # Accept older injected snapshots conservatively, without ancestry.
        if parts[1].isdigit():
            parts = line.strip().split(None, 3)
            if len(parts) < 4:
                continue
            pid, _ppid, stat, command = parts
        else:
            pid, stat, command = parts
        if not pid.isdigit() or stat.startswith("Z") or int(pid) in (excluded_pids or set()):
            continue
        if any(re.search(re.escape(needle) + r"(?![A-Za-z0-9._-])", command) for needle in needles):
            owners.append(f"{pid}:{command[:160]}")
    return owners


def _lsof_cwd_worktree_owners(lsof_output: str, *, worktree_paths: Sequence[str]) -> list[str]:
    """PURE: ``lsof -Fpn`` pids whose cwd is AT or UNDER one of the paths.

    Prefix matching is boundary-aware: ``/x/wt-a`` must not match a process
    sitting in the sibling tree ``/x/wt-a-other``.
    """
    prefixes = [p.rstrip("/") for p in worktree_paths if p]
    owners: list[str] = []
    pid = "?"
    for line in lsof_output.splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:].strip()
        if tag == "p":
            pid = value or "?"
            continue
        if tag != "n" or not value:
            continue
        cwd = value.rstrip("/") or value
        if any(cwd == prefix or cwd.startswith(prefix + "/") for prefix in prefixes):
            owners.append(f"{pid}:{cwd}")
    return owners


def _lsof_table_was_sampled(lsof_output: str) -> bool:
    """True when lsof emitted at least one parseable pid or cwd record.

    Exit status 1 is overloaded: empty stdout means the process table was
    not read; a sampled ``p``/``n`` row means the table was read even if no
    row owns this worktree.
    """
    for line in lsof_output.splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:].strip()
        if tag in {"p", "n"} and value:
            return True
    return False


def _probe_worktree_process_owner(
    worktree_path: object,
    *,
    ps_output: str | None = None,
    lsof_output: str | None = None,
) -> tuple[str, str]:
    """Return ``("owned" | "free" | "unknown", detail)`` for ``worktree_path``.

    ``owned`` when a live (non-zombie) process references the path in its argv
    or has a cwd inside it. ``unknown`` when either snapshot could not be read —
    callers must treat ``unknown`` exactly like ``owned``. ``ps_output`` and
    ``lsof_output`` are test seams; production passes neither and samples both
    snapshots live at the moment of the call.
    """
    paths = _worktree_path_spellings(worktree_path)
    if not paths:
        return ("unknown", "worktree_path_unset")
    if ps_output is None:
        proc = _run_reclaim_command(("ps", "-Ao", "pid=,ppid=,stat=,command="))
        if proc is None or proc.returncode != 0:
            return ("unknown", "ps_probe_failed")
        ps_output = proc.stdout or ""
    argv_owners = _ps_argv_worktree_owners(
        ps_output, worktree_paths=paths, excluded_pids=_ps_caller_ancestry(ps_output, pid=os.getpid())
    )
    if argv_owners:
        return ("owned", f"argv:{argv_owners[0]}"[:_RECLAIM_DETAIL_CAP])
    lsof_returncode = 0
    if lsof_output is None:
        proc = _run_reclaim_command(_LSOF_CWD_SNAPSHOT_CMD)
        # lsof exits 1 for "found nothing" AND for a partially or wholly
        # unreadable process table. Exit 0, or exit 1 with sampled table
        # rows, is a read. Exit 1 with no parseable rows is an unreadable
        # instrument and must not fall through to free.
        if proc is None or proc.returncode > 1:
            return ("unknown", "lsof_probe_failed")
        lsof_output = proc.stdout or ""
        lsof_returncode = proc.returncode
    cwd_owners = _lsof_cwd_worktree_owners(lsof_output, worktree_paths=paths)
    if cwd_owners:
        return ("owned", f"cwd:{cwd_owners[0]}"[:_RECLAIM_DETAIL_CAP])
    if lsof_returncode == 1 and not _lsof_table_was_sampled(lsof_output):
        return ("unknown", "lsof_probe_failed")
    return ("free", "")


def _linked_worktree_paths(repo_root: Path) -> tuple[set[str] | None, str]:
    """Resolved paths of this repo's LINKED worktrees — the primary EXCLUDED.

    ``git worktree list --porcelain`` always reports the main worktree first;
    dropping that first record is what keeps the operator's primary checkout
    off-limits. Registration is also the only proof that a path on disk is a
    worktree of THIS repo rather than an unrelated directory a stale lane row
    happens to name.
    """
    proc = _run_reclaim_command(["git", "-C", str(repo_root), "worktree", "list", "--porcelain"])
    if proc is None or proc.returncode != 0:
        return (None, "worktree_list_failed")
    entries = [
        line[len("worktree ") :].strip() for line in (proc.stdout or "").splitlines() if line.startswith("worktree ")
    ]
    if not entries:
        return (None, "worktree_list_empty")
    linked = {_resolved_path_text(raw).rstrip("/") for raw in entries[1:]}
    linked.discard("")
    return (linked, "")


def _probe_worktree_merged(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    integration_ref: str = _RECLAIM_INTEGRATION_REF,
) -> tuple[bool | None, str]:
    """Is every commit reachable from this worktree already on ``integration_ref``?

    Two conjunctive proofs, both required:

    1. The worktree's own ``HEAD`` commit is an ancestor of ``integration_ref``.
       This is the load-bearing one — it covers a detached HEAD and a branch ref
       that was deleted after the merge, where a branch-name check cannot answer.
    2. When ``refs/heads/<branch>`` still exists, that ref is an ancestor of
       ``integration_ref`` too, so a branch left behind the tree still refuses.

    Returns ``(True, reason)`` only when both hold; ``(False, reason)`` for a
    proven-unmerged tree; ``(None, reason)`` when a probe could not answer.
    """
    integration = _run_reclaim_command(
        ["git", "-C", str(repo_root), "rev-parse", "--verify", "--quiet", f"{integration_ref}^{{commit}}"]
    )
    if integration is None or integration.returncode != 0:
        return (None, f"integration_ref_unresolved:{integration_ref}")

    head = _run_reclaim_command(["git", "-C", worktree_path, "rev-parse", "HEAD"])
    if head is None or head.returncode != 0:
        return (None, "worktree_head_unreadable")
    head_sha = (head.stdout or "").strip()
    if not head_sha:
        return (None, "worktree_head_unreadable")

    def _is_ancestor(rev: str) -> bool | None:
        proc = _run_reclaim_command(["git", "-C", str(repo_root), "merge-base", "--is-ancestor", rev, integration_ref])
        if proc is None:
            return None
        if proc.returncode == 0:
            return True
        if proc.returncode == 1:
            return False
        return None

    head_merged = _is_ancestor(head_sha)
    if head_merged is None:
        return (None, "head_ancestry_probe_failed")
    if head_merged is False:
        return (False, "worktree_head_not_merged")

    name = _normalize_optional_text(branch)
    if name is None:
        # No branch recorded, but the tree's HEAD is provably on the
        # integration ref, so nothing on disk is unpreserved.
        return (True, "worktree_head_merged_no_branch")
    ref = name[len("refs/heads/") :] if name.startswith("refs/heads/") else name
    show = _run_reclaim_command(["git", "-C", str(repo_root), "show-ref", "--verify", "--quiet", f"refs/heads/{ref}"])
    if show is None:
        return (None, "branch_ref_probe_failed")
    if show.returncode == 1:
        return (True, "branch_ref_absent_worktree_head_merged")
    if show.returncode != 0:
        return (None, "branch_ref_probe_failed")
    branch_merged = _is_ancestor(f"refs/heads/{ref}")
    if branch_merged is None:
        return (None, "branch_ancestry_probe_failed")
    if branch_merged is False:
        return (False, "branch_not_merged")
    return (True, "branch_and_worktree_head_merged")


def _probe_worktree_clean(worktree_path: str) -> tuple[bool | None, str]:
    """``git status --porcelain`` empty? ``None`` when the status could not run."""
    proc = _run_reclaim_command(["git", "-C", worktree_path, "status", "--porcelain"])
    if proc is None or proc.returncode != 0:
        return (None, "status_probe_failed")
    porcelain = (proc.stdout or "").strip()
    if not porcelain:
        return (True, "")
    first = porcelain.splitlines()[0].strip()
    return (False, f"dirty:{first}"[:_RECLAIM_DETAIL_CAP])


def _is_interrupted_worktree_removal(repo_root: Path, worktree_path: str, branch: object) -> bool:
    """Recognize only a worktree whose complete tracked tree was unlinked.

    This state can be resumed because its branch still holds every tracked
    blob. Any staged change, modified or untracked path, incomplete deletion,
    missing branch, or moved HEAD remains an ordinary dirty refusal. Ignored
    content is checked separately by :func:`_probe_worktree_ignored`.
    """
    status = _run_reclaim_command(["git", "-C", worktree_path, "status", "--porcelain=v1", "-z", "-uall"])
    if status is None or status.returncode != 0:
        return False
    entries = [entry for entry in (status.stdout or "").split("\0") if entry]
    if not entries or any(len(entry) < 4 or entry[:2] != " D" for entry in entries):
        return False
    deleted_paths = [entry[3:] for entry in entries]
    if any(not path for path in deleted_paths):
        return False

    tracked = _run_reclaim_command(["git", "-C", worktree_path, "ls-files", "-z"])
    if tracked is None or tracked.returncode != 0:
        return False
    tracked_paths = [path for path in (tracked.stdout or "").split("\0") if path]
    if not tracked_paths or len(deleted_paths) != len(tracked_paths) or set(deleted_paths) != set(tracked_paths):
        return False

    # A deleted gitlink can hide independent work inside the submodule, which
    # the parent worktree's status cannot classify. Keep that shape dirty.
    staged = _run_reclaim_command(["git", "-C", worktree_path, "ls-files", "--stage", "-z"])
    if staged is None or staged.returncode != 0:
        return False
    for record in (staged.stdout or "").split("\0"):
        if not record:
            continue
        meta, separator, _path = record.partition("\t")
        fields = meta.split(maxsplit=1)
        if not separator or not fields or fields[0] == "160000":
            return False

    branch_name = _recorded_branch_short_name(branch)
    if branch_name is None or _worktree_head_is_recorded_branch(worktree_path, branch) is not True:
        return False
    head = _reclaim_rev_parse(Path(worktree_path), "HEAD")
    branch_tip = _reclaim_rev_parse(repo_root, f"refs/heads/{branch_name}")
    return head is not None and branch_tip is not None and head == branch_tip


# A conflict marker is exactly ``marker-size`` (default 7) repeats of one
# character, optionally followed by a space and a label. Anchored and exact --
# a hand-typed ``=========================`` underline is 25 characters, does
# not match, and therefore stays a content line.
_CONFLICT_MARKER_RE = re.compile(r"^(<{7}|\|{7}|={7}|>{7})(?: .*)?$")

#: ``git merge-file`` conflict styles, tried in order. The repository's
#: ``merge.conflictStyle`` decides which one produced the file on disk and this
#: probe must not depend on reading that config correctly, so all three are
#: candidates and any match is accepted. An unsupported flag on an older git
#: exits nonzero with empty stdout and drops out on its own.
_CONFLICT_STYLE_FLAGS: tuple[tuple[str, ...], ...] = ((), ("--diff3",), ("--zdiff3",))


#: Markers git writes a label after: the ref name on the ``ours`` opener, the
#: merged-in ref on the ``theirs`` closer, the merge base on the ``|||||||``
#: divider. ``git merge`` labels them with the ref, ``git checkout --merge``
#: regenerates them as ``ours``/``theirs``, so the label carries no information
#: about whether the *content* is work. ``=======`` is deliberately absent: git
#: always writes it bare, so it is compared verbatim and text appended to it
#: stays visible as the hand edit it is.
_LABELLED_MARKERS = frozenset({"<<<<<<<", "|||||||", ">>>>>>>"})


def _normalize_conflict_lines(text: str) -> list[str]:
    """Structural marker labels dropped; every other line kept verbatim.

    A marker is only structural where the conflict grammar admits one: an
    opener outside a block, a base divider inside the ours section, a
    ``=======`` in ours or base, a closer in theirs. Shape alone is not enough.
    A *content* line of seven ``>`` and a label -- a quoted email, a diff pasted
    into prose -- sits outside any block, and erasing its tail there would hide
    a hand edit to real text. The state is the discriminator, so the walk
    carries one [GRPH-27].

    Only the labelled markers lose their tail. Everything else, ``=======``
    included, still has to match exactly, so line order and line content are
    otherwise untouched by this pass.
    """
    outside, ours, base, theirs = 0, 1, 2, 3
    out: list[str] = []
    state = outside
    for line in text.splitlines():
        match = _CONFLICT_MARKER_RE.match(line)
        token = match.group(1) if match else None
        structural = True
        if token == "<<<<<<<" and state == outside:
            state = ours
        elif token == "|||||||" and state == ours:
            state = base
        elif token == "=======" and state in (ours, base):
            state = theirs
        elif token == ">>>>>>>" and state == theirs:
            state = outside
        else:
            structural = False
        out.append(token if structural and token in _LABELLED_MARKERS else line)
    return out


def _is_derived_conflict_artifact(path: Path, stage_texts: dict[str, str]) -> bool:
    """True only when git can regenerate this file byte for byte from the stages.

    A working-tree file at an unmerged path is normally written *by git* from
    the stages, so it is regenerable and is not work. The same structural state
    after a hand resolution means the opposite: those bytes exist nowhere else.
    Keyed on the full tuple rather than left to fall into whichever neighbour
    the control flow reaches first [GRPH-27].

    The rule is re-derivation, not inspection. An earlier version asked whether
    every non-marker line appeared in some stage, which catches only *added*
    lines: deleting the unwanted half of a conflict, or reordering the two
    halves, is equally a resolution and equally exists in no commit, and both
    scored as derived. Deleting one side is the *common* half-resolution.
    ``git checkout --merge -- <path>`` is deterministic, so the sound form of
    the question is "is this exactly what git would write", and every other
    edit -- addition, deletion, reordering -- falls to the at-risk side by
    construction. (Not *every* other edit: ``str.splitlines`` collapses the
    line terminator, so a pure CRLF/LF conversion or a stripped final newline
    still reads as derived. That is the right answer for the wrong reason --
    git checks the file out through the eol filter while ``merge-file`` sees
    raw blobs -- but it is a known blind spot rather than a guarantee.)

    Re-derivation is necessary and not sufficient: the run must also have
    *conflicted*. ``git merge-file`` exits 0 and prints a clean, marker-free
    merge whenever the two sides do not overlap, and ``git read-tree -m``,
    ``git rerere`` and ``update-index --index-info`` all leave stages 1/2/3 at
    such a path. Accepting that output would score the merge product -- bytes
    that are in no commit and are recomputable only from the index this reap is
    about to destroy -- as safely reachable. So a candidate output is required
    to carry a conflict, by both of the signals git offers: a positive exit
    status (the conflict count) and a structural marker in the rendered text.

    ``stage_texts`` maps stage number to blob text. Stages 2 and 3 are both
    required: without them there is nothing to re-derive, so the file is scored
    as ordinary content rather than guessed at.

    Text is carried with ``surrogateescape`` end to end -- the disk read, the
    stage write and ``_run_reclaim_command``'s own decode all use it -- so the
    comparison is byte-exact even when a stage is not valid UTF-8. ``replace``
    is not interchangeable here: it maps a bad byte to ``?`` on one side and to
    ``U+FFFD`` on the other, and the two never compare equal, which silently
    scores every non-UTF-8 conflict as at-risk.

    Deliberately duplicated from ``scripts/worktree_reachability.py`` rather
    than imported. ``scripts/`` is not a package and carries no ``__init__.py``,
    so an installed module cannot import it without ``sys.path`` surgery -- and
    doing that surgery from inside the orchestrator would bind a lane's reap
    decision to whichever checkout happened to be first on the path. The two
    copies are pinned against the same scenarios in both suites, and each pin
    is mutation-checked against the rule it replaces [TEST-15], so a drift
    fails a test rather than a reap.
    """
    if "2" not in stage_texts or "3" not in stage_texts:
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="surrogateescape")
    except OSError:
        return False
    want = _normalize_conflict_lines(text)
    with tempfile.TemporaryDirectory(prefix="wb-conflict-") as tmp:
        root = Path(tmp)
        names = {"1": root / "base", "2": root / "ours", "3": root / "theirs"}
        for stage, dest in names.items():
            dest.write_text(stage_texts.get(stage, ""), encoding="utf-8", errors="surrogateescape")
        for flags in _CONFLICT_STYLE_FLAGS:
            proc = _run_reclaim_command(
                [
                    "git",
                    "merge-file",
                    "-p",
                    *flags,
                    "-L",
                    "ours",
                    "-L",
                    "base",
                    "-L",
                    "theirs",
                    str(names["2"]),
                    str(names["1"]),
                    str(names["3"]),
                ]
            )
            if proc is None or not proc.stdout:
                continue
            # ``merge-file`` returns the conflict count, so 0 is a clean merge:
            # real output, real bytes, and in no commit. An error is negative
            # (surfacing as 255/128 here), which the marker check below rejects
            # on its own -- the two signals are kept independent on purpose.
            if proc.returncode <= 0:
                continue
            got = _normalize_conflict_lines(proc.stdout)
            if not any(_CONFLICT_MARKER_RE.match(line) for line in got):
                continue
            if got == want:
                return True
    return False


def _reachable_blob_set(repo_root: Path) -> tuple[set[str] | None, str]:
    """Every blob reachable from every ref, memoised on the current ref tips.

    The walk is the one reclaim probe whose cost scales with repository history
    rather than with the worktree: measured here at ~3.8s over ~101k objects and
    ~2,400 refs. A reap sweep inspects every dirty lane, so walking per lane
    turns a safety check into minutes of redundant work -- and a safety check
    too slow to leave switched on gets switched off.

    The memo key is a digest of the walk's *roots*, not a timestamp: any root
    that moves, appears or disappears changes the key, so a stale set cannot be
    served after someone lands a commit or deletes a branch mid-sweep.

    ``for-each-ref`` alone is not that key. ``git rev-list --all`` means every
    ref under ``refs/`` *along with HEAD*, and by default the HEAD of every
    linked worktree -- which ``for-each-ref`` does not list. A lane sitting at a
    detached HEAD is exactly the shape here, and moving it changed the reachable
    set while leaving the ref digest identical, so a stale set was served and
    at-risk content read as recoverable [REF-09]. ``git worktree list
    --porcelain`` carries a ``HEAD <sha>`` line per worktree and closes it.
    """
    tips = _run_reclaim_command(["git", "-C", str(repo_root), "for-each-ref", "--format=%(objectname)"])
    if tips is None or tips.returncode != 0:
        return (None, "reachability_refs_probe_failed")
    heads = _run_reclaim_command(["git", "-C", str(repo_root), "worktree", "list", "--porcelain"])
    if heads is None or heads.returncode != 0:
        return (None, "reachability_heads_probe_failed")
    digest = hashlib.sha256()
    digest.update((tips.stdout or "").encode("utf-8", "replace"))
    digest.update(b"\0")
    digest.update((heads.stdout or "").encode("utf-8", "replace"))
    key = digest.hexdigest()
    now = time.monotonic()
    cached = _REACHABLE_BLOBS_CACHE.get(str(repo_root))
    if cached is not None and cached[0] == key and (now - cached[1]) < _REACHABLE_BLOBS_TTL_S:
        return (cached[2], "")

    walk = _run_reclaim_command(
        ["git", "-C", str(repo_root), "rev-list", "--all", "--objects"],
        timeout=_RECLAIM_REACHABILITY_TIMEOUT_S,
    )
    if walk is None or walk.returncode != 0:
        return (None, "reachability_walk_failed")
    blobs = {line.split(" ", 1)[0] for line in (walk.stdout or "").splitlines() if " " in line}
    if not blobs:
        return (None, "reachability_walk_empty")
    _REACHABLE_BLOBS_CACHE[str(repo_root)] = (key, now, blobs)
    while len(_REACHABLE_BLOBS_CACHE) > _REACHABLE_BLOBS_CACHE_MAX_ROOTS:
        oldest = min(_REACHABLE_BLOBS_CACHE, key=lambda root: _REACHABLE_BLOBS_CACHE[root][1])
        del _REACHABLE_BLOBS_CACHE[oldest]
    return (blobs, "")


def _probe_dirty_content_reachable(repo_root: Path, worktree_path: str) -> tuple[bool | None, str]:
    """Is every dirty byte in this worktree already inside some commit?

    Guard 4 refuses on any ``status --porcelain`` output. That is the correct
    default and this probe does not relax it -- it only splits the refusal into
    the two cells it has always conflated, because they license opposite
    operator decisions:

    * dirty because the tree holds bytes no commit has -- removing it destroys
      the only copy, and the tree must be kept until someone lands the work;
    * dirty because the tree holds bytes that are byte-identical to blobs
      already reachable from a ref -- a scratch directory wearing a data-loss
      warning, held for tidiness rather than risk.

    A rescue-named checkout is the case that forced this: nine
    dirty files, an abandoned merge where ``git merge --abort`` refuses, held
    out of every reap for weeks. Eight of the nine paths were byte-identical to
    blobs on live branches. The outcome vocabulary had no way to say so, so the
    tree read as maximally dangerous to every operator who looked at it.

    That tree still reports ``refused_dirty`` -- and that is the right answer,
    not a shortfall. Source 2 below found a tenth thing nobody had counted: a
    staged, never-committed edit to ``Makefile.d/lane-gate.mk`` whose blob
    appears in zero ``rev-list --all --objects`` lines. The refusal is now
    attributable to one named path instead of standing for the whole tree,
    which is the actual product here: the answer did not change, the reason
    did.

    Three sources of content are scored, because any one of them alone reports
    a tree as safe that is not:

    1. the working tree, with ``-uall`` so an untracked DIRECTORY is expanded
       into its files rather than collapsing to one ``dir/`` entry that no
       file-scan can hash;
    2. the index at stage 0, for content staged and then either unlinked or
       overwritten on disk -- ``git worktree remove --force`` discards it and
       nothing else holds it;
    3. index stages 1/2/3, for an in-progress or abandoned merge.

    ``None`` means the probe could not run, which the caller treats as the
    at-risk case. The reachable set is a *pool*, and [EVAL-25]'s rule is that a
    pool is incomplete by design: an item outside it is unpooled, never known
    nonrelevant. So "this blob is in a commit" is proven, while "this blob is
    in no commit" is only ever "in none of the refs I enumerated" -- and a walk
    that did not finish enumerated nothing at all.
    """
    proc = _run_reclaim_command(["git", "-C", worktree_path, "status", "--porcelain=v1", "-z", "-uall"])
    if proc is None or proc.returncode != 0:
        return (None, "reachability_status_probe_failed")
    fields = [f for f in (proc.stdout or "").split("\0") if f]

    rels: list[str] = []
    i = 0
    while i < len(fields):
        entry = fields[i]
        rels.append(entry[3:])
        if "R" in entry[:2] or "C" in entry[:2]:
            i += 1  # a rename/copy record is followed by its source path
        i += 1
    if not rels:
        # Guard 4 has already proven this tree dirty. A status parse that finds
        # nothing therefore disagrees with the guard that called us, and
        # asserting "recoverable" from a check that saw nothing is exactly the
        # silence-as-success move [OBS-08].
        return (None, "reachability_status_empty_after_dirty")

    reachable, walk_detail = _reachable_blob_set(repo_root)
    if reachable is None:
        return (None, walk_detail)

    # Stage blobs first: they are both scored content AND the reference the
    # conflict-artifact rule below needs, so they have to be collected before
    # the working tree is walked.
    #
    # ``--stage`` alone emits every index entry: stage 0 for ordinary paths and
    # stages 1/2/3 for unmerged ones. Adding ``-u`` would RESTRICT output to the
    # unmerged entries and silently drop stage 0, which is the staged-then-
    # overwritten case. No pathspec is passed -- a lane can be dirty in
    # thousands of paths and an argv that long is an ``E2BIG`` away from a probe
    # failure; filtering the full listing costs one pass over the index.
    #
    # ``-z`` is load-bearing, not symmetry with the status call above. Without
    # it ``ls-files`` honours ``core.quotePath`` and prints a path with any
    # non-ASCII byte -- or a newline -- as a C-quoted string, while ``status -z``
    # printed it raw. The two spellings then fail to match, the index blob is
    # dropped from scoring, and a lane whose only copy of some work is a staged
    # ``café.txt`` reads as recoverable.
    dirty = set(rels)
    stages = _run_reclaim_command(["git", "-C", worktree_path, "ls-files", "--stage", "-z"])
    if stages is None or stages.returncode != 0:
        return (None, "reachability_stage_probe_failed")

    # A set, not a list. Every staged-and-on-disk path contributes an index
    # entry here and a working-tree hash below, so a list double-counts each one
    # and the disclosed pool depth stops being the number of distinct blobs it
    # claims to be.
    candidates: set[tuple[str, str]] = set()
    derived: list[str] = []
    unmerged_stages: dict[str, dict[str, str]] = {}
    for record in (stages.stdout or "").split("\0"):
        if not record:
            continue
        meta, _, rel = record.partition("\t")
        parts = meta.split()
        if len(parts) != 3 or rel not in dirty:
            continue
        candidates.add((parts[1], rel))
        if parts[2] != "0":
            unmerged_stages.setdefault(rel, {})[parts[2]] = parts[1]

    for rel in rels:
        target = Path(worktree_path) / rel
        if rel in unmerged_stages:
            # git wrote this file from the stages. Hashing it would score a
            # marker soup that exists in no commit and never could, reporting
            # every abandoned merge as at-risk. A hand resolution at the same
            # path is NOT derived and falls through to be scored as the work it
            # is -- which is why the test is "can git regenerate exactly this"
            # rather than "does the path have stages".
            texts: dict[str, str] = {}
            for stage, sha in sorted(unmerged_stages[rel].items()):
                blob = _run_reclaim_command(["git", "-C", worktree_path, "cat-file", "-p", sha])
                if blob is None or blob.returncode != 0:
                    return (None, f"reachability_stage_read_failed:{rel}"[:_RECLAIM_DETAIL_CAP])
                texts[stage] = blob.stdout or ""
            if _is_derived_conflict_artifact(target, texts):
                derived.append(rel)
                continue
        if target.is_symlink():
            # git stores the target path as the blob, not the pointee's bytes.
            # ``is_symlink`` then ``readlink`` is a TOCTOU on a live lane, and
            # an uncaught ``OSError`` here would cost the row its whole
            # classification for a race the caller cannot act on.
            try:
                link_target = os.readlink(target)
            except OSError:
                return (None, f"reachability_readlink_failed:{rel}"[:_RECLAIM_DETAIL_CAP])
            hashed = _run_reclaim_command(
                ["git", "-C", str(repo_root), "hash-object", "--stdin"],
                stdin_text=link_target,
            )
        elif target.is_file():
            hashed = _run_reclaim_command(["git", "-C", str(repo_root), "hash-object", "--", str(target)])
        else:
            continue  # a deletion cannot be lost content
        if hashed is None or hashed.returncode != 0:
            return (None, f"reachability_hash_failed:{rel}"[:_RECLAIM_DETAIL_CAP])
        candidates.add(((hashed.stdout or "").strip(), rel))

    for sha, rel in sorted(candidates):
        if sha and sha not in reachable:
            return (False, f"unreachable:{rel}"[:_RECLAIM_DETAIL_CAP])
    # The recoverable cell states its own pool depth. "Recoverable" with no
    # numbers is the same silence-as-success the at-risk cell was fixed for --
    # an operator has no way to tell a tree that was scored from one that was
    # scored shallowly [OBS-08], and disclosing what the pool contained is what
    # [EVAL-25] actually asks for.
    detail = f"reachable:{len(candidates)} blobs/{len(rels)} paths"
    if derived:
        # A derived path was never scored -- its working-tree bytes were
        # skipped, not found reachable. Folding it silently into the same
        # "reachable:N" as a scored path is the one thing this detail string
        # exists to prevent: the operator cannot tell measured from skipped
        # [OBS-08]. Name them.
        detail += "/derived:" + ",".join(sorted(derived))
    return (True, detail[:_RECLAIM_DETAIL_CAP])


def _dirty_refusal_outcome(repo_root: Path, worktree_path: str, clean_detail: str) -> tuple[str, str]:
    """Which of the two dirty cells this tree is in, and why.

    Both refuse. The tree survives either way and this helper cannot authorise
    a removal -- it is called only after guard 4 has already decided to refuse.
    What it changes is what the operator is told, which is the difference
    between "land this work before reaping" and "this tree is scratch".
    An unrunnable probe reports the at-risk name, because a check that did not
    complete has proven nothing [OBS-08].

    The probe's detail is appended rather than dropped. ``refused_dirty`` on its
    own tells an operator to go re-derive by hand exactly what the probe just
    computed -- either which path holds the unreachable bytes, or which probe
    could not run. A guard that knows why it held and does not say so is the
    [OBS-08] failure in its own right.
    """
    reachable, detail = _probe_dirty_content_reachable(repo_root, worktree_path)
    outcome = "refused_dirty_recoverable" if reachable is True else "refused_dirty"
    if not detail:
        return (outcome, clean_detail)
    joined = f"{clean_detail} {detail}" if clean_detail else detail
    return (outcome, joined[:_RECLAIM_DETAIL_CAP])


def _normalized_reclaim_relpath(value: object) -> str | None:
    """A safe repository-relative POSIX path, or ``None`` for bad input."""
    if not isinstance(value, str):
        return None
    path = value.rstrip("/")
    if not path or path.startswith("/"):
        return None
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return None
    return "/".join(parts)


def _bootstrap_config_relpath(worktree_path: str, value: object) -> str | None:
    """Return a config path only when it names a descendant of the worktree.

    ``configs[].path`` is polymorphic: alongside filesystem paths it records
    Git configuration keys such as ``core.hooksPath`` and tracked top-level
    files such as ``Makefile`` and ``.gitignore``.  Bootstrap-managed ignored
    content must therefore pass this positive filesystem-path predicate before
    the ledger can grant it any authority.
    """
    normalized = _normalized_reclaim_relpath(value)
    if normalized is None or "/" not in normalized:
        return None
    try:
        worktree = Path(worktree_path).resolve()
        candidate = (worktree / normalized).resolve()
        candidate.relative_to(worktree)
    except (OSError, ValueError):
        return None
    return normalized


def _bootstrap_surface_root(worktree_path: str, entry: object) -> str | None:
    """Return a root only for a bootstrap-generated surface."""
    if not isinstance(entry, Mapping) or entry.get("source") not in _REGENERABLE_SURFACE_SOURCES:
        return None
    normalized = _normalized_reclaim_relpath(entry.get("path"))
    if normalized is None or "/" not in normalized:
        return None
    if entry.get("source") == "shared":
        # Older receipts also used shared for real copies. Only a current
        # payload link grants root authority; copies need current identity.
        target = _symlink_target_relpath(Path(worktree_path) / normalized, worktree_path)
        roots = _payload_provenance_roots([entry])
        if target is None or not any(target == root or target.startswith(root + "/") for root in roots):
            return None
    return normalized


def _bootstrap_config_root(worktree_path: str, entry: object) -> str | None:
    """Return a root only for a newly-created, path-shaped config."""
    if not isinstance(entry, Mapping) or entry.get("action") not in _REGENERABLE_CONFIG_ACTIONS:
        return None
    return _bootstrap_config_relpath(worktree_path, entry.get("path"))


def _bootstrap_ledger_entries(payload: Mapping[object, object], key: str) -> list[object] | None:
    """Read one bounded ledger array without accepting polymorphic values."""
    entries = payload.get(key)
    return entries if isinstance(entries, list) else None


def _bootstrap_schema_supported(payload: Mapping[object, object]) -> bool:
    """Accept the audited integer schema version, not equality-compatible values."""
    schema_version = payload.get("schema_version")
    return type(schema_version) is int and schema_version == _SUPPORTED_BOOTSTRAP_LEDGER_SCHEMA_VERSION


def _load_bootstrap_ledger(worktree_path: str) -> tuple[list[object], list[object]] | None:
    """Load bounded arrays from the one audited bootstrap ledger schema."""
    ledger = _run_reclaim_command(["git", "-C", worktree_path, "show", "HEAD:.workbay-bootstrap.json"])
    if ledger is None or ledger.returncode != 0:
        return None
    try:
        payload = json.loads(ledger.stdout or "")
    except (json.JSONDecodeError, UnicodeError):
        return None
    if not isinstance(payload, Mapping) or not _bootstrap_schema_supported(payload):
        return None
    surfaces = _bootstrap_ledger_entries(payload, "surfaces")
    configs = _bootstrap_ledger_entries(payload, "configs")
    if surfaces is None or configs is None:
        return None
    if len(surfaces) + len(configs) > _MAX_REGENERABLE_LEDGER_ROOTS:
        return None
    return (surfaces, configs)


def _roots_from_ledger_entries(
    entries: Sequence[object], root_for_entry: Callable[[object], str | None]
) -> dict[str, str]:
    """Collect normalized roots accepted by one typed authority source."""
    roots = (root_for_entry(entry) for entry in entries)
    return {root: root for root in roots if root is not None}


def _bootstrap_regenerable_roots(worktree_path: str) -> dict[str, str]:
    """Managed roots recorded by this worktree's tracked bootstrap ledger.

    A clean worktree cannot alter the ledger without tripping the earlier dirty
    guard, making its ``surfaces`` and path-shaped ``configs`` entries the
    authoritative provenance for exclusively generated, reproducible content.
    A missing, unsupported, or malformed ledger contributes no allow-list
    entries; classification remains default-deny.
    """
    ledger_entries = _load_bootstrap_ledger(worktree_path)
    if ledger_entries is None:
        return {}
    surfaces, configs = ledger_entries
    roots = _roots_from_ledger_entries(surfaces, lambda entry: _bootstrap_surface_root(worktree_path, entry))
    roots.update(_roots_from_ledger_entries(configs, lambda entry: _bootstrap_config_root(worktree_path, entry)))
    return roots


def _cache_rule_for_parts(parts: list[str]) -> str | None:
    """Return the cache rule for a cache-shaped leaf, never arbitrary content."""
    leaf = parts[-1]
    directory_parts = parts[:-1]
    cache_names = [part for part in directory_parts if part in _REGENERABLE_CACHE_DIR_NAMES]
    if not cache_names:
        return None
    cache_name = cache_names[-1]
    cache_index = max(index for index, part in enumerate(directory_parts) if part == cache_name)
    relative_parts = parts[cache_index + 1 :]
    if cache_name == "__pycache__":
        eligible = leaf.endswith((".pyc", ".pyo")) or re.fullmatch(r".+\.(?:pyc|pyo)\.[0-9]+", leaf) is not None
    elif cache_name == ".pytest_cache":
        eligible = leaf in {".gitignore", "CACHEDIR.TAG", "nodeids", "lastfailed", "stepwise"} or (
            relative_parts == ["README.md"]
        )
    elif cache_name == ".ruff_cache":
        eligible = leaf in {".gitignore", "CACHEDIR.TAG", "cache"} or (
            len(leaf) >= 16 and all(char in "0123456789abcdefABCDEF" for char in leaf)
        )
    else:  # .mypy_cache
        eligible = leaf in {".gitignore", "CACHEDIR.TAG", "cache.db", "missing_stubs"} or leaf.endswith(
            (".json", ".data")
        )
    return f"cache:{cache_name}" if eligible else None


def _heartbeat_rule(parts: list[str]) -> str | None:
    """Recognize only final and atomic-temp lifecycle heartbeat leaves."""
    if parts == [".task-state", ".heartbeat"]:
        return "hardcoded_root:.task-state/.heartbeat"
    if len(parts) != 3 or parts[:2] != [".task-state", ".heartbeat"]:
        return None
    final_name = parts[2]
    temporary = _HEARTBEAT_TEMP_RE.fullmatch(final_name)
    if temporary is not None:
        final_name = temporary.group("final")
    if _HEARTBEAT_FINAL_RE.fullmatch(final_name):
        return "lifecycle_heartbeat"
    return None


def _root_rule(normalized: str, roots: Mapping[str, str], *, prefix: str) -> str | None:
    """Return attribution for the narrowest matching authorized root."""
    for root, label in sorted(roots.items(), key=lambda item: (-len(item[0]), item[0])):
        if normalized == root or normalized.startswith(root + "/"):
            return f"{prefix}:{label}"
    return None


def _cache_authority(parts: list[str]) -> tuple[bool, str | None]:
    """Classify a cache path and prevent fallthrough from malformed leaves."""
    rule = _cache_rule_for_parts(parts)
    within_cache = any(part in _REGENERABLE_CACHE_DIR_NAMES for part in parts[:-1])
    return (rule is not None or within_cache, rule)


@dataclass(frozen=True)
class _ContentIdentityContext:
    """Per-worktree evidence for judging an ignored file by content, not label.

    The bootstrap ledger records where a surface came from, but its ``source``
    label is install-time history: ``install._copy_surface_entry`` receipts any
    pre-existing destination ``local`` and never re-verifies it, so one ordering
    artifact permanently mislabels a payload-hoisted file.  A label cannot be
    trusted to say what content is; the content can.

    ``tracked_by_root`` is materialized once per worktree, so classification
    costs one ``git ls-files`` per declared root rather than one subprocess per
    ignored file.  Tracked-ness is load-bearing: identity against an *untracked*
    file proves nothing survives the delete.
    """

    worktree_path: str
    tracked_by_root: Mapping[str, frozenset[str]]
    #: Absolute path of the main working tree, which this sweep never reclaims,
    #: so its copy of a file is what survives a worktree's removal. ``None``
    #: when it could not be resolved or is this worktree itself.
    primary_root: str | None = None
    #: Config surfaces the ledger itself declares. Matching the primary is not
    #: authority on its own -- without this bound, any ignored scratch file that
    #: happened to duplicate one in the primary would authorize its own delete.
    primary_config_paths: frozenset[str] = frozenset()


def _payload_provenance_roots(surfaces: Sequence[object]) -> list[str]:
    """Normalized tracked roots named by ledger ``provenance_key`` links."""
    roots: list[str] = []
    for entry in surfaces:
        if not isinstance(entry, Mapping):
            continue
        key = entry.get("provenance_key")
        if not isinstance(key, str) or not key.startswith(_PROVENANCE_LINK_PREFIX):
            continue
        root = _normalized_reclaim_relpath(key[len(_PROVENANCE_LINK_PREFIX) :])
        if root is not None and root not in roots:
            roots.append(root)
    if len(roots) > _MAX_PAYLOAD_PROVENANCE_ROOTS:
        return []
    return roots


def _tracked_paths_under(worktree_path: str, root: str) -> frozenset[str] | None:
    """Relative paths tracked below ``root``, or ``None`` when unreadable."""
    proc = _run_reclaim_command(["git", "-C", worktree_path, "ls-files", "-z", "--", root])
    if proc is None or proc.returncode != 0:
        return None
    prefix = root + "/"
    return frozenset(entry[len(prefix) :] for entry in (proc.stdout or "").split("\0") if entry.startswith(prefix))


def _primary_worktree_root(worktree_path: str) -> str | None:
    """Absolute main working tree of this checkout, or ``None``.

    ``git worktree list --porcelain`` reports the main working tree first; that
    is the definition, not an inference from the ``.git`` layout. ``None`` when
    the lookup fails or resolves to this worktree, which can vouch for nothing.
    """
    proc = _run_reclaim_command(["git", "-C", worktree_path, "worktree", "list", "--porcelain"])
    if proc is None or proc.returncode != 0:
        return None
    first = next((line for line in (proc.stdout or "").splitlines() if line.startswith("worktree ")), None)
    if first is None:
        return None
    primary = _resolved_path_text(first[len("worktree ") :])
    if not primary or primary == _resolved_path_text(worktree_path):
        return None
    return primary


def _ledger_config_paths(configs: Sequence[object]) -> frozenset[str]:
    """Normalized config surfaces the ledger declares, bounding primary identity."""
    paths = set()
    for entry in configs:
        if not isinstance(entry, Mapping):
            continue
        normalized = _normalized_reclaim_relpath(entry.get("path"))
        if normalized is not None:
            paths.add(normalized)
    return frozenset(paths)


def _content_identity_context(worktree_path: str) -> _ContentIdentityContext | None:
    """Build the identity evidence for one worktree, or ``None`` if unavailable."""
    ledger_entries = _load_bootstrap_ledger(worktree_path)
    if ledger_entries is None:
        return None
    surfaces, configs = ledger_entries
    tracked_by_root: dict[str, frozenset[str]] = {}
    for root in _payload_provenance_roots(surfaces):
        tracked = _tracked_paths_under(worktree_path, root)
        if tracked:
            tracked_by_root[root] = tracked
    config_paths = _ledger_config_paths(configs)
    if not tracked_by_root and not config_paths:
        return None
    return _ContentIdentityContext(
        worktree_path=worktree_path,
        tracked_by_root=tracked_by_root,
        primary_root=_primary_worktree_root(worktree_path),
        primary_config_paths=config_paths,
    )


def _regular_file_size(path: Path) -> int | None:
    """Size of a real regular file; ``None`` for anything else or unreadable."""
    try:
        info = path.lstat()
    except OSError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    return info.st_size


def _files_have_identical_bytes(left: Path, right: Path) -> bool:
    """Compare regular-file bytes and producer-relevant executable bits."""
    try:
        if (left.lstat().st_mode & 0o111) != (right.lstat().st_mode & 0o111):
            return False
    except OSError:
        return False
    left_size = _regular_file_size(left)
    right_size = _regular_file_size(right)
    if left_size is None or right_size is None or left_size != right_size:
        return False
    if left_size > _MAX_IDENTITY_COMPARE_BYTES:
        return False
    try:
        with left.open("rb") as left_handle, right.open("rb") as right_handle:
            while True:
                left_chunk = left_handle.read(_IDENTITY_COMPARE_CHUNK)
                right_chunk = right_handle.read(_IDENTITY_COMPARE_CHUNK)
                if left_chunk != right_chunk:
                    return False
                if not left_chunk:
                    return True
    except OSError:
        return False


def _payload_ancestor_modes_match(worktree: Path, root: str, relative: str) -> bool:
    """Check real directory ancestors before comparing an ignored payload leaf.

    Git enumerates ignored leaves, so directory traversal customizations must
    be checked here. Walk from the top down to avoid following parent links.
    """
    payload = worktree
    try:
        for part in Path(root).parts:
            payload /= part
            if not stat.S_ISDIR(payload.lstat().st_mode):
                return False
        copied = worktree
        for part in Path(relative).parts[:-1]:
            copied /= part
            payload /= part
            copied_mode = copied.lstat().st_mode
            payload_mode = payload.lstat().st_mode
            if not stat.S_ISDIR(copied_mode) or not stat.S_ISDIR(payload_mode):
                return False
            if (copied_mode & 0o111) != (payload_mode & 0o111):
                return False
    except OSError:
        return False
    return True


def _symlink_target_relpath(link: Path, worktree_path: str) -> str | None:
    """Worktree-relative path a symlink resolves to, or ``None``.

    ``None`` covers every case that must not authorize a delete: the entry is
    not a symlink, the target does not exist, the chain leaves the worktree, or
    either path cannot be resolved at all.
    """
    try:
        if not link.is_symlink():
            return None
        resolved = link.resolve(strict=True)
        root = Path(worktree_path).resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    try:
        return _normalized_reclaim_relpath(str(resolved.relative_to(root)))
    except ValueError:
        return None


def _resolve_dangling_symlink_target(target: str, link_parent: Path) -> str | None:
    """Resolve existing target components and retain the missing remainder.

    Strict realpath on each existing component follows symlinks before a later
    ``..`` is handled. If an existing symlink points to a missing target,
    expand that link and resolve its target components the same way. Once a
    component is missing, later names remain a lexical tail; parent components
    can cancel that tail and resume resolution at the last existing directory.
    """
    try:
        parent = link_parent.resolve(strict=True)
        target_path = target if os.path.isabs(target) else os.path.join(str(parent), target)
        current = os.path.abspath(os.sep)
        missing: list[str] = []
        components = target_path.split(os.sep)
        component_index = 0
        expanded_links = 0
        while component_index < len(components):
            component = components[component_index]
            component_index += 1
            if not component or component == ".":
                continue
            if component == "..":
                if missing:
                    missing.pop()
                else:
                    current = os.path.realpath(os.path.join(current, ".."), strict=True)
                continue
            if missing:
                missing.append(component)
                continue
            candidate = os.path.join(current, component)
            try:
                current = os.path.realpath(candidate, strict=True)
            except FileNotFoundError:
                if os.path.islink(candidate):
                    expanded_links += 1
                    if expanded_links > 40:
                        return None
                    link_target = os.readlink(candidate)
                    if os.path.isabs(link_target):
                        current = os.path.abspath(os.sep)
                        missing.clear()
                    components = link_target.split(os.sep) + components[component_index:]
                    component_index = 0
                else:
                    missing.append(component)
        return os.path.join(current, *missing) if missing else current
    except (OSError, RuntimeError):
        return None


def _symlink_escapes_worktree(link: Path, worktree_path: str) -> str | None:
    """Absolute target of a symlink whose target is outside the worktree.

    ``_symlink_target_relpath`` answers ``None`` for such a link, and the
    content-identity rule then reads that ``None`` as "unique content" -- the
    opposite of the truth.  A symlink is a pointer, not a copy: deleting it
    never touches its target, and a target that is not under this worktree
    cannot be destroyed by removing this worktree.  That is a *weaker*
    precondition than the in-worktree case already authorized above, which must
    additionally prove the target is committed, so this rule is safe wherever
    that one is.

    This is why the reaper reclaimed nothing in practice: lane provisioning
    links ``Makefile.d`` and ``.venv`` at the primary checkout, so every lane
    worktree carried at least two permanently unclassifiable leaves and every
    candidate was refused on ignored content.

    For a dangling target, existing components are resolved through realpath
    before parent components are processed. ``None`` also covers non-symlinks,
    unreadable links, unresolvable roots, and symlink loops, all of which must
    fail closed.
    """
    try:
        root = Path(worktree_path).resolve(strict=True)
        if not link.is_symlink():
            return None
    except (OSError, RuntimeError):
        return None
    try:
        resolved = link.resolve(strict=True)
    except FileNotFoundError:
        try:
            target = os.readlink(link)
        except OSError:
            return None
        resolved_target = _resolve_dangling_symlink_target(target, link.parent)
        if resolved_target is None:
            return None
        try:
            Path(resolved_target).relative_to(root)
        except ValueError:
            return resolved_target
        return None
    except (OSError, RuntimeError):
        return None
    try:
        resolved.relative_to(root)
    except ValueError:
        return str(resolved)
    return None


def _symlink_outside_worktree_rule(normalized: str, worktree_path: str | None) -> str | None:
    """Rule name for an ignored symlink whose target lives outside the worktree."""
    if worktree_path is None:
        return None
    link = Path(worktree_path) / normalized
    if _symlink_escapes_worktree(link, worktree_path) is None:
        return None
    try:
        link.resolve(strict=True)
    except FileNotFoundError:
        return "symlink_outside_worktree_dangling"
    except (OSError, RuntimeError):
        return None
    return "symlink_outside_worktree"


def _tracked_under_root(tracked: frozenset[str], relative: str) -> bool:
    """Whether ``relative`` names a tracked file or a tracked directory prefix.

    Lane provisioning links whole directories (``docs/workbay/rules``), and
    ``git ls-files`` reports such a link as a single entry, so a file-only
    membership test would leave every directory surface blocking.
    """
    return relative in tracked or any(entry.startswith(relative + "/") for entry in tracked)


def _content_identity_rule(path: str, identity: _ContentIdentityContext | None) -> str | None:
    """Authorize one ignored file whose bytes already exist in tracked history.

    Deleting a byte-identical copy of a committed file loses nothing this
    worktree does not already carry, so the ledger's ``local`` label cannot
    outvote it.  Every comparison that cannot be completed -- unreadable,
    symlinked, non-regular, oversized, escaping the worktree -- contributes no
    rule and therefore still blocks (default-deny).
    """
    if identity is None:
        return None
    normalized = _normalized_reclaim_relpath(path)
    if normalized is None:
        return None
    candidate = Path(identity.worktree_path) / normalized
    target = _symlink_target_relpath(candidate, identity.worktree_path)
    if target is not None:
        # A link holds no content of its own. Once its target is committed
        # under a root the ledger declared, deleting the link loses nothing.
        for root, tracked in identity.tracked_by_root.items():
            prefix = root + "/"
            if target.startswith(prefix) and _tracked_under_root(tracked, target[len(prefix) :]):
                return f"symlink_to_tracked:{root}"
        return None
    if _regular_file_size(candidate) is None:
        return None
    for root, tracked in identity.tracked_by_root.items():
        if normalized not in tracked:
            continue
        if not _payload_ancestor_modes_match(Path(identity.worktree_path), root, normalized):
            continue
        if _files_have_identical_bytes(candidate, Path(identity.worktree_path) / root / normalized):
            return f"payload_identical:{root}"
    # A merged config surface has no tracked counterpart anywhere -- the
    # installer writes it into an ignored path in every checkout. The primary's
    # copy is the only thing that survives, and it does survive: the sweep
    # never reclaims the main working tree.
    if identity.primary_root is not None and normalized in identity.primary_config_paths:
        if _files_have_identical_bytes(candidate, Path(identity.primary_root) / normalized):
            return "identical_to_primary"
    return None


def _runtime_state_roots() -> list[str]:
    """Normalized runtime-state roots, or ``[]`` when the allowlist is oversized."""
    roots: list[str] = []
    for value in _RUNTIME_STATE_REGENERABLE_ROOTS:
        normalized = _normalized_reclaim_relpath(value)
        if normalized is not None and normalized not in roots:
            roots.append(normalized)
    if len(roots) > _MAX_RUNTIME_STATE_ROOTS:
        return []
    return roots


def _contained_worktree_path(worktree_path: str, normalized: str) -> Path | None:
    """Join ``normalized`` onto the worktree only if it resolves inside it.

    ``_normalized_reclaim_relpath`` already rejects ``..`` and absolute
    spellings, so the remaining way out is a symlinked *parent* -- a
    ``logs/worker-daemon`` pointing at some directory elsewhere.  ``lstat`` on
    the leaf follows those intermediate links and would report a perfectly
    ordinary regular file living outside the tree being reclaimed.  The
    returned path is the unresolved join, so a caller's leaf checks keep their
    ``lstat`` semantics.  ``None`` on any resolution or permission failure.
    """
    try:
        root = Path(worktree_path).resolve(strict=True)
        (root / normalized).resolve(strict=True).relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return None
    return root / normalized


def _runtime_state_regenerable_rule(path: str, worktree_path: str | None) -> str | None:
    """Authorize one per-worktree runtime-state file under an explicit root.

    Breaker state, locks, scheduling stamps and daemon logs are re-derived by
    the next run that needs them and are read by nothing outside the worktree,
    so removing the worktree destroys no information.  The authority is the
    ``_RUNTIME_STATE_REGENERABLE_ROOTS`` allowlist and nothing else: outside
    those prefixes a path keeps whatever classification the earlier rules gave
    it (default-deny).  Every case that cannot be checked -- no worktree to
    check against, an unresolvable or escaping path, a symlink, a directory, an
    unreadable entry -- contributes no rule and therefore still blocks.
    """
    if worktree_path is None:
        return None
    normalized = _normalized_reclaim_relpath(path)
    if normalized is None:
        return None
    rule = _root_rule(normalized, {root: root for root in _runtime_state_roots()}, prefix="runtime_state")
    # The embedding digest memo is one file, never a directory authority.
    if normalized == ".task-state/embeddings/verify-cache.json":
        rule = f"runtime_state:{normalized}"
    if rule is None:
        return None
    candidate = _contained_worktree_path(worktree_path, normalized)
    # A link is not the state: its target may be the only copy of something the
    # worktree does not own, so only a real regular file authorizes anything.
    if candidate is None or _regular_file_size(candidate) is None:
        return None
    return rule


def _package_virtualenv_rule(
    normalized: str,
    worktree_path: str | None,
    marker_cache: dict[str, bool],
) -> str | None:
    """Authorize ignored files under a structurally verified worktree venv."""
    parts = normalized.split("/")
    if len(parts) >= 2 and (parts[0] == ".venv" or parts[0].startswith(".venv-")):
        venv_dir = parts[0]
    elif len(parts) >= 4 and parts[0] == "packages" and (parts[2] == ".venv" or parts[2].startswith(".venv-")):
        venv_dir = "/".join(parts[:3])
    else:
        return None

    if worktree_path is None:
        return None
    if venv_dir not in marker_cache:
        marker = _contained_worktree_path(worktree_path, f"{venv_dir}/pyvenv.cfg")
        marker_cache[venv_dir] = marker is not None and _regular_file_size(marker) is not None
    if not marker_cache[venv_dir]:
        return None
    return f"package_virtualenv:{venv_dir}"


def _ignored_path_regenerable_rule(
    path: str,
    *,
    managed_roots: Mapping[str, str],
    identity: _ContentIdentityContext | None = None,
    worktree_path: str | None = None,
    package_virtualenv_cache: dict[str, bool] | None = None,
) -> str | None:
    """Name the positive classification rule for one ignored file.

    Deep-merged configs such as .grok/config.toml preserve operator content;
    their pathname alone is never proof that they are regenerable.
    """
    normalized = _normalized_reclaim_relpath(path)
    if normalized is None:
        return None
    parts = normalized.split("/")
    cache_matched, cache_rule = _cache_authority(parts)
    # Once a path enters a cache-named directory it must satisfy that cache's
    # leaf grammar.  Do not fall through to a broader .venv, hardcoded, or
    # ledger root: that would make ``.venv/x/__pycache__/AUTHORED.md`` safe.
    if cache_matched:
        return cache_rule
    heartbeat_rule = _heartbeat_rule(parts)
    if heartbeat_rule is not None:
        return heartbeat_rule
    if len(parts) == 2 and parts[0] == ".task-state" and parts[1].endswith(".stamp"):
        return "hardcoded_root:.task-state/*.stamp"
    hardcoded_roots = {root: root for root in _REGENERABLE_IGNORED_ROOTS}
    venv_marker_cache = package_virtualenv_cache if package_virtualenv_cache is not None else {}
    return (
        _root_rule(normalized, hardcoded_roots, prefix="hardcoded_root")
        or _package_virtualenv_rule(normalized, worktree_path, venv_marker_cache)
        or _root_rule(normalized, managed_roots, prefix="ledger_surface")
        # Consulted last: a ledger label that already authorizes the path needs
        # no filesystem comparison, and content identity only ever *adds* a
        # positive classification the labels alone could not reach.
        or _content_identity_rule(normalized, identity)
        # A link whose target resolves outside the worktree holds no content of
        # its own and its pointee survives removal -- ``Makefile.d`` and
        # ``.venv`` symlinked at the primary checkout.  Needs no ledger.
        or _symlink_outside_worktree_rule(normalized, worktree_path)
        # Consulted after content identity, and last overall.  It authorizes on
        # a weaker premise -- not "a copy survives" but "the bytes are worthless
        # once this worktree is gone" -- so it only ever converts a would-be
        # ``potentially_unique`` into an accounted path.  The cache short-circuit
        # above still wins: an earlier refusal is never re-opened here.
        or _runtime_state_regenerable_rule(normalized, worktree_path)
    )


def _ignored_path_is_regenerable(path: str, *, managed_roots: Mapping[str, str]) -> bool:
    """Positive classification for one ignored file; unknown stays unsafe."""
    return _ignored_path_regenerable_rule(path, managed_roots=managed_roots) is not None


def _ignored_path_is_nested_repository(worktree_path: str, path: str) -> bool | None:
    """Whether one enumerated ignored entry is itself a Git repository.

    Git deliberately treats a nested repository as one opaque entry.  Such an
    entry may contain committed work that the parent repository cannot
    enumerate, so it must never inherit the entry's regenerable classification.
    ``None`` represents an unreadable filesystem probe and fails closed.
    """
    normalized = _normalized_reclaim_relpath(path)
    if normalized is None:
        return None
    candidate = Path(worktree_path) / normalized
    try:
        candidate_mode = candidate.stat().st_mode
    except FileNotFoundError:
        return False
    except OSError:
        return None
    if not stat.S_ISDIR(candidate_mode):
        return False

    try:
        (candidate / ".git").lstat()
    except FileNotFoundError:
        pass
    except OSError:
        return None
    else:
        return True

    # A bare repository has no .git marker. False positives here only refuse
    # reclamation, which is the safe direction for an authored directory.
    for marker in ("HEAD", "config", "objects", "refs"):
        try:
            (candidate / marker).lstat()
        except FileNotFoundError:
            return False
        except OSError:
            return None
    return True


@dataclass(frozen=True)
class _IgnoredContentVerdict:
    """Structured result from one ignored-content authority stage."""

    safe: bool | None
    detail: str


def _enumerate_ignored_paths(worktree_path: str) -> tuple[list[str] | None, str]:
    """Enumerate every ignored leaf, retaining typed probe failures."""
    proc = _run_reclaim_command(
        ["git", "-C", worktree_path, "ls-files", "--others", "--ignored", "--exclude-standard", "-z"]
    )
    if proc is not None and proc.returncode == _RECLAIM_TIMEOUT_RETURN_CODE:
        return (None, "ignored_probe_timeout")
    if proc is None or proc.returncode != 0:
        return (None, "ignored_probe_failed")
    stderr = (proc.stderr or "").strip()
    if stderr:
        warning = " ".join(stderr.split())
        return (None, f"ignored_probe_incomplete:{warning}"[:_RECLAIM_DETAIL_CAP])
    return ([path for path in (proc.stdout or "").split("\0") if path], "")


def _nested_repository_verdict(worktree_path: str, ignored: Sequence[str]) -> _IgnoredContentVerdict | None:
    """Return a refusal/fault for nested repositories, else continue."""
    nested_repositories: list[str] = []
    for path in ignored:
        nested = _ignored_path_is_nested_repository(worktree_path, path)
        if nested is None:
            detail = f"ignored_nested_repository_probe_failed:{path}"[:_RECLAIM_DETAIL_CAP]
            return _IgnoredContentVerdict(None, detail)
        if nested:
            nested_repositories.append(path.rstrip("/"))
    if not nested_repositories:
        return None
    sample = ",".join(nested_repositories[:3])
    detail = f"ignored_content:{len(ignored)};nested_repositories:{len(nested_repositories)};first:{sample}"
    return _IgnoredContentVerdict(False, detail[:_RECLAIM_DETAIL_CAP])


def _classification_verdict(
    ignored: Sequence[str],
    managed_roots: Mapping[str, str],
    identity: _ContentIdentityContext | None = None,
    worktree_path: str | None = None,
    harvested: frozenset[str] = frozenset(),
) -> _IgnoredContentVerdict:
    """Combine per-path authority rules into one auditable verdict."""
    package_virtualenv_cache: dict[str, bool] = {}
    classified = [
        (
            path,
            "audit_trail_harvested"
            if _normalized_reclaim_relpath(path) in harvested
            else _ignored_path_regenerable_rule(
                path,
                managed_roots=managed_roots,
                identity=identity,
                worktree_path=worktree_path,
                package_virtualenv_cache=package_virtualenv_cache,
            ),
        )
        for path in ignored
    ]
    potentially_unique = [path for path, rule in classified if rule is None]
    if potentially_unique:
        sample = ",".join(potentially_unique[:3])
        detail = f"ignored_content:{len(ignored)};potentially_unique:{len(potentially_unique)};first:{sample}"
        return _IgnoredContentVerdict(False, detail[:_RECLAIM_DETAIL_CAP])
    rules = [rule for _path, rule in classified if rule is not None]
    histogram = Counter(rules)
    rule_detail = json.dumps(dict(sorted(histogram.items())), separators=(",", ":"))
    detail = f"ignored_content:{len(ignored)};regenerable:{len(ignored)};rules_json:{rule_detail}"
    return _IgnoredContentVerdict(True, detail)


#: Ignored paths that are sole-copy operator audit trails: nothing re-derives
#: them, so the classifier is right to refuse a worktree that still holds one.
#: The gap was that nothing ever *harvested* them, which left the refusal
#: permanent -- correct, and useless.  Harvesting copies the bytes somewhere
#: durable first; only then does the path stop blocking.
_SOLE_COPY_AUDIT_TRAILS: tuple[str, ...] = (
    ".task-state/branch_naming_overrides.log",
    ".task-state/branch_naming_push_overrides.log",
    ".task-state/lifecycle_guard_bypass.jsonl",
)


def _harvest_sole_copy_audit_trails(worktree_path: str, repo_root: str | Path) -> frozenset[str]:
    """Atomically preserve this worktree's sole-copy audit trails.

    Returns the worktree-relative paths that were successfully preserved, so
    the classifier can stop treating them as blocking unique content.  A copy
    that fails verification or publication is absent from the result and
    therefore keeps blocking -- preservation must be proven, never assumed.
    """
    return preserve_audit_trails(worktree_path, repo_root, _SOLE_COPY_AUDIT_TRAILS)


def _probe_worktree_ignored(worktree_path: str) -> tuple[bool | None, str]:
    """Refuse only potentially-unique ignored content, with default-deny.

    ``git ls-files`` is used instead of collapsed ``status --ignored`` output
    so every ignored file below a directory is classified.  Caches, the lane
    virtualenv, and bootstrap-managed payload surfaces are reconstructible.
    The task-start heartbeat and direct ``.task-state/*.stamp`` files are also
    reconstructible, as are the allowlisted runtime-state roots -- locks,
    breaker state, scheduling stamps and daemon logs the next run re-derives.
    Every other task-state or authored review file is potentially unique and
    therefore still blocks removal. ``None`` means the recursive probe could
    not answer and also fails closed.
    """
    ignored, enumeration_detail = _enumerate_ignored_paths(worktree_path)
    if ignored is None:
        return (None, enumeration_detail)
    if not ignored:
        return (True, "ignored_content:0")
    managed_roots = _bootstrap_regenerable_roots(worktree_path)
    nested_verdict = _nested_repository_verdict(worktree_path, ignored)
    identity = _content_identity_context(worktree_path)
    # Sole-copy audit trails are copied under the primary checkout before
    # classification; a worktree that cannot name its primary keeps blocking.
    primary_root = identity.primary_root if identity is not None else _primary_worktree_root(worktree_path)
    harvested = (
        _harvest_sole_copy_audit_trails(worktree_path, primary_root) if primary_root is not None else frozenset()
    )
    verdict = nested_verdict or _classification_verdict(
        ignored, managed_roots, identity, worktree_path, harvested=harvested
    )
    return (verdict.safe, verdict.detail)


def _ignored_rule_histogram_from_detail(detail: str) -> dict[str, int]:
    """Recover the structured allow attribution carried by a reclaim detail."""
    marker = ";rules_json:"
    marker_at = detail.rfind(marker)
    if marker_at < 0:
        return {}
    try:
        decoded, _end = json.JSONDecoder().raw_decode(detail[marker_at + len(marker) :])
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(decoded, Mapping):
        return {}
    return {
        rule: count
        for rule, count in decoded.items()
        if isinstance(rule, str) and isinstance(count, int) and not isinstance(count, bool) and count >= 0
    }


def _name_only_path_set(stdout: str | None) -> set[str]:
    return {line for line in (stdout or "").splitlines() if line}


#: Object ids listed in ``.git/shallow``. SHA-1 is 40 hex chars; SHA-256 is 64.
_SHALLOW_OBJECT_ID_RE = re.compile(r"\A(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


def _merged_by_tree_untrusted_graph_reason(
    cwd: Path | str,
    *,
    compared_branch: str,
    integration_ref: str,
) -> str | None:
    """Refuse when grafts or a *reachable* shallow boundary rewrite the graph.

    ``--no-replace-objects`` does not disable grafts, so any grafts file is
    still ``None`` / ``merged_by_tree_probe_failed``. A shallow file is not
    a global refuse: each listed SHA is tested with
    ``merge-base --is-ancestor <sha> <ref>`` against both the compared
    branch and the integration ref. Exit 0 (reachable) refuses; exit 1
    (unrelated) continues; any other exit, a malformed SHA, or an
    unreadable common-dir / shallow file fails closed. Never land.
    """
    proc = _run_reclaim_command(["git", "-C", str(cwd), "rev-parse", "--git-common-dir"])
    if proc is None or proc.returncode != 0:
        return "merged_by_tree_probe_failed"
    raw = (proc.stdout or "").strip()
    if not raw:
        return "merged_by_tree_probe_failed"
    common = Path(raw)
    if not common.is_absolute():
        common = Path(cwd) / common
    try:
        if (common / "info" / "grafts").is_file():
            return "merged_by_tree_probe_failed"
        shallow_path = common / "shallow"
        if not shallow_path.is_file():
            return None
        text = shallow_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return "merged_by_tree_probe_failed"
    for raw_line in text.splitlines():
        sha = raw_line.strip()
        if not sha:
            continue
        if _SHALLOW_OBJECT_ID_RE.fullmatch(sha) is None:
            return "merged_by_tree_probe_failed"
        for ref in (compared_branch, integration_ref):
            ancestor_proc = _run_reclaim_command(
                [
                    "git",
                    "-C",
                    str(cwd),
                    "--no-replace-objects",
                    "merge-base",
                    "--is-ancestor",
                    sha,
                    ref,
                ]
            )
            answer = _probe_command_tristate(_PROBE_SITE_MERGE_BASE, ancestor_proc)
            if answer is False:
                continue
            return "merged_by_tree_probe_failed"
    return None


def _probe_name_only_paths(cwd: Path | str, left: str, right: str) -> set[str] | None:
    """``git diff --name-only`` path set, or ``None`` when the command failed.

    Pins so the path set is repository contents, not inherited config:
    ``core.quotepath=false`` keeps names as literal bytes; ``--no-renames``
    keeps both halves of a rename; ``diff.ignoreSubmodules=none`` and
    ``--ignore-submodules=none`` keep gitlinks (the flag also defeats a
    per-submodule ignore). No pathspec is passed — empty/large/spaced
    names are set members, not argv. A failed command is never treated
    as an empty set.
    """
    proc = _run_reclaim_command(
        [
            "git",
            "-C",
            str(cwd),
            "--no-replace-objects",
            "-c",
            "core.quotepath=false",
            "-c",
            "diff.ignoreSubmodules=none",
            "diff",
            "--no-renames",
            "--ignore-submodules=none",
            "--name-only",
            left,
            right,
        ]
    )
    if proc is None or proc.returncode != 0:
        return None
    return _name_only_path_set(proc.stdout)


def _probe_branch_merged_by_tree(
    branch: object,
    *,
    repo_root: Path | str | None = None,
    integration_ref: str = _RECLAIM_INTEGRATION_REF,
) -> tuple[bool | None, str]:
    """Path-set fallback when SHA ancestry is orphaned (``merged_by_tree``).

    ``BASES = merge-base --all(integration, branch)``. TOUCHED is the union of
    ``diff --name-only BASE BRANCH`` over every base; DIFFERING is
    ``diff --name-only INTEGRATION BRANCH``. Landed is the empty intersection:
    the branch changed nothing that still disagrees with integration. Unrelated
    integration commits do not pin a landed lane. An empty TOUCHED set is its
    own reason (``merged_by_tree_untouched``), not an empty-intersection
    coincidence. More than one base with a nonempty landed TOUCHED set uses
    ``merged_by_tree_multi_base``. A failed or empty base listing, or any
    failed per-base listing, is ``None`` / ``merged_by_tree_probe_failed``.
    Grafts rewrite history and refuse globally. A shallow file refuses
    only when a listed boundary SHA is an ancestor of the compared branch
    or the integration ref; an unrelated boundary does not.
    Every merge-base and name-only diff passes ``--no-replace-objects``.
    """
    name = _normalize_optional_text(branch)
    if name is None:
        return (None, "merged_by_tree_branch_unset")
    cwd: Path | str | None = repo_root
    if cwd is None:
        try:
            cwd = _workspace_root()
        except Exception as exc:  # noqa: BLE001 — probe degrade
            return (None, f"merged_by_tree_repo_unresolved:{exc}"[:_RECLAIM_DETAIL_CAP])
    ref = name[len("refs/heads/") :] if name.startswith("refs/heads/") else name
    untrusted = _merged_by_tree_untrusted_graph_reason(
        cwd,
        compared_branch=ref,
        integration_ref=integration_ref,
    )
    if untrusted is not None:
        return (None, untrusted)
    base_proc = _run_reclaim_command(
        ["git", "-C", str(cwd), "--no-replace-objects", "merge-base", "--all", integration_ref, ref]
    )
    if base_proc is None or base_proc.returncode != 0:
        return (None, "merged_by_tree_probe_failed")
    bases = [line.strip() for line in (base_proc.stdout or "").splitlines() if line.strip()]
    if not bases:
        return (None, "merged_by_tree_probe_failed")
    touched: set[str] = set()
    for base in bases:
        paths = _probe_name_only_paths(cwd, base, ref)
        if paths is None:
            return (None, "merged_by_tree_probe_failed")
        touched.update(paths)
    if not touched:
        return (True, "merged_by_tree_untouched")
    differing = _probe_name_only_paths(cwd, integration_ref, ref)
    if differing is None:
        return (None, "merged_by_tree_probe_failed")
    if touched.isdisjoint(differing):
        if len(bases) > 1:
            return (True, "merged_by_tree_multi_base")
        return (True, "merged_by_tree")
    return (False, "trees_differ")


def _session_heartbeat_blocks_reclaim(
    *,
    repo_root: Path | str,
    worktree_path: str,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> tuple[bool, str]:
    """Fail-closed extra gate: live or unknown session heartbeat refuses reclaim.

    Calls ``_session_live`` from the reclaim module. A parked peer session with
    a clean tree and cwd elsewhere must still refuse. ``None`` (unknown) is
    treated as live — only an exact ``False`` permits reclaim. Any predicate
    exception refuses.
    """
    del task_ref, lane_id  # reserved so callers can pass row identity
    try:
        from workbay_orchestrator_mcp.orchestration.lane_reclaim import (  # noqa: PLC0415
            _session_live,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (True, f"reclaimable_predicate_import_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    # Bind the heartbeat probe so a missing symbol fails closed at this gate
    # rather than silently dropping the heartbeat (decision 2496).
    if not callable(_session_live):
        return (True, "reclaimable_predicate_unusable")
    try:
        live = _session_live(orchestrator_root=repo_root, worktree=worktree_path)
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (True, f"session_heartbeat_probe_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    if live is not False:
        return (True, "session_heartbeat_live")
    return (False, "")


def _shared_path_identity(path: object, repo_root: Path | str | None = None) -> str:
    """Resolved-path identity: realpath, trailing slash, and relative spellings."""
    raw = _normalize_optional_text(path)
    if raw is None:
        return ""
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute() and repo_root is not None:
        candidate = Path(repo_root) / candidate
    try:
        return str(candidate.resolve()).rstrip("/")
    except OSError:
        return str(candidate).rstrip("/")


def _shared_path_blocks_reclaim(
    *,
    worktree_path: str,
    task_ref: str | None,
    lane_id: str | None,
    repo_root: Path | str | None = None,
) -> tuple[bool, str]:
    """Fail-closed extra gate: another live lane owns the same resolved path.

    Exact stored-string SQL matching cannot see symlink aliases or trailing-
    slash spellings. Enumerate the global non-terminal ledger once and compare
    resolved identity in Python (REF-26). ``list_lanes_by_worktree_path`` keeps
    its exact-match contract for other callers.
    """
    try:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            list_nonterminal_lanes_with_worktree_path,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (True, f"shared_path_import_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    try:
        env = list_nonterminal_lanes_with_worktree_path()
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (True, f"shared_path_probe_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    if not isinstance(env, dict):
        return (True, "shared_path_lookup_failed")
    if env.get("ok") is False:
        return (True, "shared_path_lookup_failed")
    data: Any = env.get("data") if isinstance(env.get("data"), dict) else env
    lanes = data.get("lanes") if isinstance(data, dict) else None
    if lanes is None and isinstance(env.get("lanes"), list):
        lanes = env.get("lanes")
    if not isinstance(lanes, list):
        return (True, "shared_path_lookup_malformed")
    target = _shared_path_identity(worktree_path, repo_root)
    if not target:
        return (True, "shared_path_target_unresolved")
    for owner in lanes:
        if not isinstance(owner, dict):
            return (True, "shared_path_malformed_owner")
        owner_id = owner.get("lane_id")
        owner_task = owner.get("task_ref")
        if owner_id == lane_id and owner_task == task_ref:
            continue
        owner_key = _shared_path_identity(owner.get("worktree_path"), repo_root)
        if not owner_key or owner_key != target:
            continue
        return (True, f"shared_with_lane:{owner_id}"[:_RECLAIM_DETAIL_CAP])
    return (False, "")


def _lane_worker_lock_path(lane_id: str) -> Path | None:
    try:
        from workbay_handoff_mcp import get_runtime_config  # noqa: PLC0415

        cfg = get_runtime_config()
        state_dir = Path(cfg.state_dir)
    except Exception:  # noqa: BLE001 — fail closed at caller
        return None
    return state_dir / f"worker-{lane_id}.lock"


def _write_lock_claim_payload(handle: Any, *, claim: str) -> None:
    """Stamp the held lock with a durable claim kind (flock remains the oracle)."""
    payload = {
        "pid": os.getpid(),
        "claim": claim,
        "heartbeat_ts": time.time(),
    }
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(payload))
        handle.flush()
        os.fsync(handle.fileno())
    except OSError:
        pass


def _wait_reaping_test_barrier(env_name: str) -> None:
    """Env-gated rendezvous so tests can interleave rematerialize under the claim."""
    raw = os.environ.get(env_name)
    if not raw or not str(raw).strip():
        return
    barrier = Path(str(raw).strip())
    try:
        barrier.mkdir(parents=True, exist_ok=True)
        (barrier / f"ready.{os.getpid()}").touch()
        go = barrier / "go"
        deadline = time.monotonic() + 30.0
        while not go.exists():
            if time.monotonic() >= deadline:
                break
            time.sleep(0.005)
    except OSError:
        return


def _reaping_claim_barrier() -> None:
    """Park after acquire so tests can observe the flock before recency/CAS."""
    _wait_reaping_test_barrier(_REAP_CLAIM_BARRIER_ENV)


class CloseWindowBarrierExpired(RuntimeError):
    """Bounded wait for the rematerialize peer expired.

    Distinct from a close-window invariant violation (both close and
    rematerialize succeeded, or neither did). A red of this type means
    the harness never observed the parent enter ``ensure_lane_worktree``,
    not that the exclusive-outcome contract broke [Release It! ch. 5].
    """


def _wait_for_peer_inside_ensure(barrier: Path, *, timeout: float) -> None:
    """Wait until the test parent marks that it is inside ``ensure_lane_worktree``.

    ``inside-ensure`` is the observation point (CON-22). ``go`` is accepted
    as a legacy alias so older close-window fixtures that still write ``go``
    after rematerialize keep working. Expiry raises
    :class:`CloseWindowBarrierExpired` — never an invariant assertion.
    """
    inside = barrier / "inside-ensure"
    go = barrier / "go"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if inside.exists() or go.exists():
            return
        time.sleep(0.005)
    raise CloseWindowBarrierExpired(f"close-window barrier expired waiting for ensure_lane_worktree entry at {inside}")


def _reaping_close_window_barrier() -> None:
    """Park until the test parent is inside ``ensure_lane_worktree``.

    Inverted vs the post-acquire claim barrier: this process does not
    require the parent to wait on a ``ready.*`` file it writes on the way
    out. Tests still see ``ready.*`` for fixtures that poll it, but the
    load-bearing wait is for the parent-inside-ensure mark. The reaper
    never raises; expiry is recorded as ``expired`` on the barrier so a
    test can distinguish harness timeout from an invariant miss.
    """
    raw = os.environ.get(_REAP_CLOSE_WINDOW_BARRIER_ENV)
    if not raw or not str(raw).strip():
        return
    barrier = Path(str(raw).strip())
    try:
        barrier.mkdir(parents=True, exist_ok=True)
        (barrier / f"ready.{os.getpid()}").touch()
        _wait_for_peer_inside_ensure(barrier, timeout=30.0)
    except CloseWindowBarrierExpired:
        try:
            (barrier / "expired").touch()
        except OSError:
            pass
    except OSError:
        return


def _acquire_lane_worker_lock(
    lane_id: str,
    *,
    claim: str = _REAP_CLAIM_KIND,
) -> tuple[Any | None, str]:
    """Exclusive non-blocking flock of the lane's existing worker lock."""
    _assert_no_reaper_write_lock(reason="fcntl.flock")
    path = _lane_worker_lock_path(lane_id)
    if path is None:
        return (None, "worker_lock_path_unresolved")
    handle = None
    try:
        handle = try_acquire_worker_lock(path)
        if handle is None:
            return (None, "worker_lock_held")
        _write_lock_claim_payload(handle, claim=claim)
        return (handle, "")
    except OSError as exc:
        if handle is not None:
            release_worker_lock(handle)
        return (None, f"worker_lock_unavailable:{exc}"[:_RECLAIM_DETAIL_CAP])
    except BaseException:
        if handle is not None:
            release_worker_lock(handle)
        raise


def _try_row_reaping_claim(lane_id: object) -> tuple[Any | None, str]:
    """Acquire the durable reaping claim and park at the test barrier on success."""
    lane_key = _normalize_optional_text(lane_id)
    if lane_key is None:
        return (None, "lane_id_missing")
    handle, detail = _acquire_lane_worker_lock(lane_key)
    if handle is not None:
        _reaping_claim_barrier()
    return (handle, detail)


def _release_pending_reaping_claims(
    pending_closes: Sequence[tuple[Any, Any, str, Any]],
) -> None:
    for _row, _entry, _note, handle in pending_closes:
        if handle is not None:
            _release_lane_worker_lock(handle)


def _release_lane_worker_lock(handle: Any) -> None:
    release_worker_lock(handle)


def _is_peer_held_reaping_claim(claim_detail: str) -> bool:
    """True when a live peer owns the flock — retryable contention, not an OS fault."""
    return claim_detail == "worker_lock_held"


def _persist_structural_reaping_blocker(
    *,
    task_ref: str,
    lane_id: str,
    claim_detail: str,
) -> str:
    """Persist a remediation-required blocker so the aging line can render it.

    ``format_blocked_lane_aging_line`` reads ``blocker`` / ``last_blocker``,
    which ``_last_blocker_text_for_lane`` loads from the ``blockers`` table.
    Peer-held contention must not call this — it is normal and would become
    alert noise. Do not touch ``worktree_lanes.updated_at``: that would race
    the CAS the claim exists to protect.
    """
    description = f"reaping_claim_unavailable: {claim_detail} — remediation required"
    try:
        with _get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO blockers (task_ref, lane_id, description, status)
                VALUES (?, ?, ?, 'open')
                """,
                (task_ref, lane_id, description),
            )
    except sqlite3.Error:
        pass
    return description


def _record_unclaimed_reaping_row(
    *,
    entry: dict[str, object],
    claim_detail: str,
    alive: list[dict[str, object]],
    failed: list[dict[str, object]],
    ambiguous: list[dict[str, object]] | None = None,
    triage: list[str],
    sweep_label: str,
    reclaim_only: bool = False,
) -> dict[str, object]:
    """Skip a row whose reaping claim could not be taken. Returns the updated entry.

    Peer-held → alive (retry next tick). Structural → persist a blocker, put
    the row on the failed/WARN path, and stamp ``blocker`` so this sweep's
    dashboard line renders the text the operator actually sees.
    """
    reason = f"reaping_claim_unavailable: {claim_detail}"
    if _is_peer_held_reaping_claim(claim_detail):
        updated_verdict = "ambiguous" if reclaim_only else "alive"
        updated = {**entry, "verdict": updated_verdict, "reason": reason}
        if reclaim_only and ambiguous is not None:
            ambiguous.append(updated)
            triage.append(f"{sweep_label} {updated.get('lane_id')} task={updated.get('task_ref')}: {reason}")
        else:
            alive.append(updated)
        return updated
    blocker = _persist_structural_reaping_blocker(
        task_ref=str(entry.get("task_ref") or ""),
        lane_id=str(entry.get("lane_id") or ""),
        claim_detail=claim_detail,
    )
    updated = {
        **entry,
        "verdict": "ambiguous",
        "reason": reason,
        "blocker": blocker,
    }
    failed.append({**updated, "stage": "reaping_claim", "error": reason})
    triage.append(f"{sweep_label} {updated.get('lane_id')} task={updated.get('task_ref')}: {reason}")
    return updated


#: Set by an applying registry sweep: its direct ``git worktree remove`` records a
#: durable intent first, so an interruption after ``rmdir(worktree)`` that strands
#: ``.git/worktrees/<id>`` is finished by the next sweep [RES-20].
_REGISTRY_REMOVAL_INTENTS: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "registry_removal_intents", default=False
)
#: Beside the tombstone retire intents and written with their fsynced publisher. Their
#: drain globs only the top level, so it never treats these records as retirements.
_REGISTRY_REMOVAL_INTENT_SUBDIR = "registry-removals"
_REGISTRY_REMOVAL_INTENT_KIND = "registry_worktree_remove"


@dataclass
class _RegistryBranchHandoff:
    """Queue identity the sweep hands a removed worktree's branch to.

    The removal intent records it, so one durable record covers the whole step: the
    tree, its admin entry and this branch's reclaim record. ``queued`` is set once the
    removal's own settle wrote that record [RES-20].
    """

    task_ref: str
    lane_id: str
    branch: str
    queued: bool = False


#: Set by the sweep around one removal whose branch it queues. ``None`` keeps the branch.
_REGISTRY_BRANCH_HANDOFF: contextvars.ContextVar[_RegistryBranchHandoff | None] = contextvars.ContextVar(
    "registry_branch_handoff", default=None
)


def _registry_removal_intent_dir(repo_root: Path | str) -> Path:
    from .orchestration import worktree_tombstone  # noqa: PLC0415

    return Path(repo_root).resolve() / worktree_tombstone._INTENT_RELATIVE / _REGISTRY_REMOVAL_INTENT_SUBDIR


def _registry_directory_incarnation(path: Path) -> dict[str, int]:
    observed = path.lstat()
    if not stat.S_ISDIR(observed.st_mode):
        raise OSError("removal target is not a directory")
    return {"st_dev": observed.st_dev, "st_ino": observed.st_ino}


def _registry_admin_incarnation(path: Path, descriptor: int) -> dict[str, object]:
    """Read through the held directory, then verify the name still denotes it."""
    opened = os.fstat(descriptor)
    identity: dict[str, object] = {"st_dev": opened.st_dev, "st_ino": opened.st_ino}
    for name in ("gitdir", "HEAD"):
        flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(name, flags, dir_fd=descriptor), "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise OSError("removal identity is not a regular file")
            # Exact bytes, including whitespace; JSON stores a lossless hex encoding.
            identity[name] = handle.read().hex()
    if _registry_directory_incarnation(path) != {"st_dev": opened.st_dev, "st_ino": opened.st_ino}:
        raise OSError("removal admin changed while being observed")
    return identity


def _registry_removal_incarnation_matches(record: Mapping[str, object], admin: Path) -> bool:
    """Absence is an allowed removal transition; a different present object is not."""
    incarnation = record.get("incarnation")
    if not isinstance(incarnation, dict) or not isinstance(incarnation.get("admin"), dict):
        return False
    if not isinstance(incarnation.get("worktree"), dict):
        return False
    for target in (incarnation["admin"], incarnation["worktree"]):
        if not all(type(target.get(key)) is int for key in ("st_dev", "st_ino")):
            return False
    if not all(isinstance(incarnation["admin"].get(key), str) for key in ("gitdir", "HEAD")):
        return False
    original_admin = Path(str(record["admin_git_dir"]))
    worktree = Path(str(record["original_worktree_path"]))
    try:
        if admin != original_admin and os.path.lexists(original_admin):
            return False
        if os.path.lexists(worktree):
            if _registry_directory_incarnation(worktree) != incarnation["worktree"]:
                return False
            if not os.path.lexists(admin):
                return False
        if os.path.lexists(admin):
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(admin, flags)
            try:
                if _registry_admin_incarnation(admin, descriptor) != incarnation["admin"]:
                    return False
            finally:
                os.close(descriptor)
    except OSError:
        return False
    return True


def _record_registry_removal_intent(
    repo_root: Path | str, worktree_path: str, *, lane_id: str | None
) -> tuple[Path | None, dict[str, object], str]:
    """Durably record path, branch, tip and admin id before ``git worktree remove``.

    Returns ``(intent_path, intent, "")``, or ``(None, {}, reason)`` when the identity
    cannot be read or the record cannot be published; the removal then does not run.
    """
    from .orchestration import worktree_tombstone  # noqa: PLC0415

    if not lane_id:
        return (None, {}, "removal_intent_identity_missing")
    try:
        original = Path(worktree_path).expanduser().resolve(strict=True)
    except OSError:
        return (None, {}, "removal_intent_worktree_unreadable")
    # A pending recovery owns this path until its incarnation can be resolved.
    # The sweep may encounter a clean replacement later in its ordinary registry
    # pass; it must not bypass the recovery refusal by starting another removal.
    for pending_path in _registry_removal_intent_dir(repo_root).glob("*.json"):
        try:
            pending = json.loads(pending_path.read_text(encoding="utf-8"))
            if not isinstance(pending, dict) or pending.get("original_worktree_path") == str(original):
                return (None, {}, "removal_intent_recovery_pending")
        except (OSError, ValueError):
            return (None, {}, "removal_intent_recovery_pending")
    probe = _run_reclaim_command(
        [
            "git",
            "-C",
            str(original),
            "rev-parse",
            "--path-format=absolute",
            "--git-dir",
            "--git-common-dir",
            "HEAD",
            "--symbolic-full-name",
            "HEAD",
        ]
    )
    if probe is None or getattr(probe, "returncode", None) != 0:
        return (None, {}, "removal_intent_identity_unavailable")
    lines = _probe_command_stdout(probe).splitlines()
    if len(lines) != 4 or not lines[3].startswith("refs/heads/"):
        return (None, {}, "removal_intent_identity_unavailable")
    admin = Path(lines[0]).resolve(strict=False)
    common = Path(lines[1]).resolve(strict=False)
    if admin.parent != (common / "worktrees").resolve(strict=False):
        return (None, {}, "removal_intent_identity_unavailable")
    branch = lines[3].removeprefix("refs/heads/")
    handoff = _REGISTRY_BRANCH_HANDOFF.get()
    if handoff is not None and handoff.branch != branch:
        return (None, {}, "removal_intent_branch_mismatch")
    try:
        worktree_incarnation = _registry_directory_incarnation(original)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(admin, flags)
        try:
            admin_incarnation = _registry_admin_incarnation(admin, descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        return (None, {}, "removal_intent_incarnation_unavailable")
    nonce = uuid.uuid4().hex
    lane_key = hashlib.sha256(lane_id.encode("utf-8")).hexdigest()[:16]
    intent_path = _registry_removal_intent_dir(repo_root) / f"{lane_key}-{nonce}.json"
    intent: dict[str, object] = {
        "schema": 1,
        "kind": _REGISTRY_REMOVAL_INTENT_KIND,
        "nonce": nonce,
        "lane_id": lane_id,
        "original_worktree_path": str(original),
        # Never created: the tombstone admin check accepts either path's ``.git``.
        "tombstone_path": str(original.with_name(f".{original.name}.workbay-retire-{nonce}")),
        "branch": branch,
        "head_sha": lines[2],
        "admin_git_dir": str(admin),
        "git_common_dir": str(common),
        "incarnation": {"admin": admin_incarnation, "worktree": worktree_incarnation},
        # The tip the guards just admitted is the only tip the reclaim record authorizes.
        "branch_handoff": (
            None
            if handoff is None
            else {"task_ref": handoff.task_ref, "lane_id": handoff.lane_id, "authorized_sha": lines[2]}
        ),
    }
    try:
        if not _registry_removal_incarnation_matches(intent, admin):
            return (None, {}, "removal_intent_incarnation_changed")
        worktree_tombstone._write_intent(intent_path, intent)
    except OSError as exc:
        return (None, {}, f"removal_intent_write_failed:{type(exc).__name__}")
    return (intent_path, intent, "")


def _hand_off_registry_removal_branch(intent: Mapping[str, object]) -> tuple[bool, str]:
    """Queue a removed worktree's branch for reclaim. Never touches a ref.

    One queue row per ``(lane, tip)``, so a redo after an interruption converges on the
    same record [RES-19]. ``branch_handoff: None`` is the sweep's recorded decision to
    keep the branch. A missing or malformed hand-off cannot prove the branch handled.
    """
    if "branch_handoff" not in intent:
        return (False, "removal_handoff_unrecorded")
    handoff = intent["branch_handoff"]
    if handoff is None:
        return (True, "")
    if not isinstance(handoff, Mapping) or not all(
        isinstance(handoff.get(key), str) and handoff.get(key) for key in ("task_ref", "lane_id", "authorized_sha")
    ):
        return (False, "removal_handoff_invalid")
    try:
        from .orchestration.branch_reclaim_queue import enqueue_branch_reclaim_candidate  # noqa: PLC0415

        _registry_reap_remaining()
        queued = enqueue_branch_reclaim_candidate(
            task_ref=str(handoff["task_ref"]),
            lane_id=str(handoff["lane_id"]),
            branch=str(intent["branch"]),
            authorized_sha=str(handoff["authorized_sha"]),
            deadline=_REGISTRY_REAP_DEADLINE.get(),
        )
        # A successful but late commit is recoverable. Keep the intent until a
        # later sweep can confirm the durable hand-off within its own budget.
        _registry_reap_remaining()
    except TimeoutError:
        return (False, "deadline_exceeded")
    except Exception as exc:  # noqa: BLE001 — the intent stays pending and is reported
        return (False, f"removal_handoff_enqueue_failed:{type(exc).__name__}")
    if queued is not True:
        return (False, "removal_handoff_enqueue_failed")
    return (True, "")


def _registry_recovery_paths_absent(record: Mapping[str, object]) -> tuple[bool, str]:
    """Observe only: a present or unreadable path requires operator reconciliation.

    External Git does not share our lock. Even matching inode and content identity
    cannot authorize replay of an irreversible filesystem effect [FLOW-16, CON-11].
    """
    observed: dict[str, object] = {}
    absent = True
    for key in ("original_worktree_path", "admin_git_dir"):
        try:
            info = Path(str(record[key])).lstat()
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                observed[key] = None
                continue
            observed[key] = {"error": type(exc).__name__, "errno": exc.errno}
        else:
            observed[key] = {
                "st_dev": info.st_dev,
                "st_ino": info.st_ino,
                "st_mode": info.st_mode,
                "st_size": info.st_size,
                "st_mtime_ns": info.st_mtime_ns,
                "st_ctime_ns": info.st_ctime_ns,
            }
        absent = False
    if absent:
        return (True, "")
    details = {
        "worktree_path": record["original_worktree_path"],
        "branch": record["branch"],
        "admin_git_dir": record["admin_git_dir"],
        "recorded_incarnation": record.get("incarnation"),
        "observed_lstat": observed,
    }
    return (False, "recovery_needs_operator:" + json.dumps(details))


def _commit_registry_removal_intent(intent_path: Path, record: Mapping[str, object]) -> tuple[str, str]:
    """Clear only after the metadata hand-off and a final absence/deadline check.

    The deadline is checked last so an observation that outlives the budget keeps
    the intent. Repetition converges on the same queue record [RES-19].
    """
    from .orchestration import worktree_tombstone  # noqa: PLC0415

    absent, detail = _registry_recovery_paths_absent(record)
    if not absent:
        return ("pending", detail)
    try:
        _registry_reap_remaining()
    except TimeoutError:
        return ("pending", "deadline_exceeded")
    worktree_tombstone._remove_intent(intent_path)
    return ("completed", "already_complete")


def _settle_registry_removal_intent(intent_path: Path, intent: Mapping[str, object]) -> tuple[str, str]:
    """Use the common-repository actuator lock from observation through settlement."""
    from .orchestration.orchestrator_lanes import _landing_mutation_lock  # noqa: PLC0415

    # registry-removals / worktree-tombstones / .task-state / repo
    root = intent_path.parents[3]
    with _landing_mutation_lock(root, operation="registry_removal_recovery", lane=str(intent["lane_id"])) as acquired:
        if acquired is not True:
            return ("pending", "recovery_repo_lock_unavailable")
        return _settle_registry_removal_intent_locked(intent_path, intent)


def _settle_registry_removal_intent_locked(intent_path: Path, intent: Mapping[str, object]) -> tuple[str, str]:
    """Reconcile metadata only; never mutate an existing worktree or admin path.

    Both paths must be absent by lstat (ENOENT) before branch hand-off. Otherwise
    keep the intent and report the recorded and observed identities on every sweep.
    Operators reconcile any surviving paths before retrying [SECD-05, RES-27].
    """
    try:
        absent, detail = _registry_recovery_paths_absent(intent)
        if not absent:
            return ("pending", detail)
        handed_off, handoff_detail = _hand_off_registry_removal_branch(intent)
        if not handed_off:
            return ("pending", handoff_detail)
        return _commit_registry_removal_intent(intent_path, intent)
    except OSError as exc:
        return ("pending", f"removal_intent_settle_failed:{type(exc).__name__}")


def _recover_registry_removal_intents(root: Path, *, apply: bool) -> list[dict[str, object]]:
    """Report, and when applying finish, removals an earlier sweep left interrupted.

    Each record is settled under its lane's worker lock, so an in-flight removal is
    never raced. A record that is unreadable or names another repository stays put.
    """
    directory = _registry_removal_intent_dir(root)
    if not directory.is_dir():
        return []
    paths = sorted(path for path in directory.glob("*.json") if path.is_file())
    if not paths:
        return []
    common_probe = _run_reclaim_command(
        ["git", "-C", str(root), "rev-parse", "--path-format=absolute", "--git-common-dir"]
    )
    if common_probe is None or getattr(common_probe, "returncode", None) != 0:
        return [{"intent": path.name, "state": "pending", "detail": "repo_common_dir_unknown"} for path in paths]
    common = Path(_probe_command_stdout(common_probe).strip()).resolve(strict=False)
    records: list[dict[str, object]] = []
    for path in paths:
        _registry_reap_remaining()
        record: dict[str, object] = {"intent": path.name}
        try:
            intent = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            records.append({**record, "state": "pending", "detail": "removal_intent_unreadable"})
            continue
        fields = ("lane_id", "nonce", "original_worktree_path", "tombstone_path", "branch", "admin_git_dir")
        if (
            not isinstance(intent, dict)
            or intent.get("kind") != _REGISTRY_REMOVAL_INTENT_KIND
            or not all(isinstance(intent.get(key), str) and intent.get(key) for key in fields)
            or path.name
            != f"{hashlib.sha256(intent['lane_id'].encode('utf-8')).hexdigest()[:16]}-{intent['nonce']}.json"
            or Path(str(intent.get("git_common_dir") or "")).resolve(strict=False) != common
            or Path(intent["admin_git_dir"]).parent.resolve(strict=False)
            != (common / "worktrees").resolve(strict=False)
        ):
            records.append({**record, "state": "pending", "detail": "removal_intent_foreign"})
            continue
        record = {**record, "worktree_path": intent["original_worktree_path"], "branch": intent["branch"]}
        if not apply:
            absent, detail = _registry_recovery_paths_absent(intent)
            if not absent:
                record["reason"] = "recovery_needs_operator"
            records.append({**record, "state": "pending", "detail": "dry_run" if absent else detail})
            continue
        handle, lock_detail = _acquire_lane_worker_lock(intent["lane_id"])
        if handle is None:
            records.append({**record, "state": "pending", "detail": lock_detail})
            continue
        try:
            state, detail = _settle_registry_removal_intent(path, intent)
        finally:
            _release_lane_worker_lock(handle)
        if detail.startswith("recovery_needs_operator:"):
            record["reason"] = "recovery_needs_operator"
        records.append({**record, "state": state, "detail": detail})
    return records


def _remove_lane_worktree(
    repo_root: Path | str,
    worktree_path: str,
    *,
    runner: Callable[[list[str]], object] | None = None,
    expected_tip: str | None = None,
    lane_id: str | None = None,
    task_ref: str | None = None,
    landing_lock_held: bool = False,
) -> tuple[bool, str]:
    """Remove a linked worktree with Git's SAFE dirty-tree check.

    Interrupted removals restore deleted tracked files and reject ignored
    content before reaching this helper. A writer can still race between that
    preflight and this command: Git refuses non-ignored dirty content, but
    cannot detect ignored content created in the gap. This residual window is
    shared by every safe-remove caller. Branch refs are never touched here —
    a worktree is removed, never a ref.

    A caller that passes row identity is bound to the row's close authority here,
    with or without an expected tip, so no entry path removes a moved closed
    worktree. A caller's expected tip that disagrees with the authority is refused.
    """
    if lane_id:
        row, authority, refusal = _durable_close_authority(task_ref=task_ref, lane_id=lane_id)
        if refusal:
            return (False, refusal)
        if row is not None and task_ref is None:
            # A task-less caller takes the task that owns this lane id. A lane id shared
            # across tasks is already refused as lane_identity_ambiguous above.
            task_ref = str(row["task_ref"])
        if authority is not None:
            if expected_tip is not None and expected_tip != authority["authorized_tip"]:
                return (False, "authority_mismatch")
            expected_tip = authority["authorized_tip"]
    if expected_tip is not None:
        from .orchestration.worktree_tombstone import retire_worktree  # noqa: PLC0415

        if not lane_id or not task_ref:
            return (False, "worktree_removal_identity_missing")
        # landing_lock_held is True only when the caller already holds the landing
        # lock in this process (lane_land); otherwise the drain takes it itself.
        return retire_worktree(
            repo_root,
            str(worktree_path),
            lane_id=lane_id,
            task_ref=task_ref,
            expected_tip=expected_tip,
            landing_lock_held=landing_lock_held,
            close_authority=authority,
        )
    argv = ["git", "-C", str(repo_root), "worktree", "remove", str(worktree_path)]
    run = runner if runner is not None else _run_reclaim_command
    intent_path: Path | None = None
    intent: dict[str, object] = {}
    if _REGISTRY_REMOVAL_INTENTS.get():
        intent_path, intent, intent_refusal = _record_registry_removal_intent(
            repo_root, str(worktree_path), lane_id=lane_id
        )
        if intent_path is None:
            return (False, intent_refusal)
    # An interruption, including the hook watchdog, leaves the intent for the next sweep.
    proc = run(argv)
    if intent_path is not None:
        settled, settle_detail = _settle_registry_removal_intent(intent_path, intent)
        handoff = _REGISTRY_BRANCH_HANDOFF.get()
        if handoff is not None and settled == "completed" and intent.get("branch_handoff") is not None:
            handoff.queued = True
        if settled == "pending":
            _logger.warning("registry removal intent left pending path=%s detail=%s", worktree_path, settle_detail)
    if proc is None:
        return (False, "git_worktree_remove_unavailable")
    # None / non-int returncode is FAILURE — `int(rc or 0)` would coerce None to success.
    rc = getattr(proc, "returncode", 1)
    if type(rc) is not int or rc != 0:
        detail = str(getattr(proc, "stderr", "") or getattr(proc, "stdout", "") or "").strip()
        return (False, (detail or "git_worktree_remove_failed")[:_RECLAIM_DETAIL_CAP])
    return (True, "")


def _remove_interrupted_lane_worktree(
    repo_root: Path | str,
    worktree_path: str,
    *,
    lane_id: str | None = None,
    task_ref: str | None = None,
    landing_lock_held: bool = False,
) -> tuple[bool, str]:
    """Restore an interrupted removal's absent tracked paths, then remove safely.

    Every command uses the bounded reclaim runner. ``checkout-index`` omits
    ``-f`` so a writer that recreates a path after validation keeps its bytes.
    The post-restore status check also covers ignored files, which Git's safe
    worktree removal does not protect. ``lane_id``/``task_ref`` bind the final
    removal to the row's close authority. ``landing_lock_held`` is forwarded
    unchanged to the final removal; see ``_remove_lane_worktree``.
    """

    def _failure_detail(code: str, proc: object | None = None) -> str:
        detail = str(getattr(proc, "stderr", "") or getattr(proc, "stdout", "") or "").strip()
        return f"{code}:{detail}"[:_RECLAIM_DETAIL_CAP] if detail else code

    deleted = _run_reclaim_command(["git", "-C", str(worktree_path), "ls-files", "--deleted", "-z"])
    deleted_rc = getattr(deleted, "returncode", 1)
    if deleted is None or type(deleted_rc) is not int or deleted_rc != 0:
        return (False, _failure_detail("interrupted_removal_restore_probe_failed", deleted))

    deleted_paths = getattr(deleted, "stdout", "") or ""
    if deleted_paths:
        restored = _run_reclaim_command(
            ["git", "-C", str(worktree_path), "checkout-index", "-q", "-z", "--stdin"],
            stdin_text=deleted_paths,
        )
        restored_rc = getattr(restored, "returncode", 1)
        if restored is None or type(restored_rc) is not int or restored_rc != 0:
            return (False, _failure_detail("interrupted_removal_restore_failed", restored))

    status = _run_reclaim_command(["git", "-C", str(worktree_path), "status", "--porcelain=v1", "-z", "--ignored"])
    status_rc = getattr(status, "returncode", 1)
    if status is None or type(status_rc) is not int or status_rc != 0:
        return (False, _failure_detail("interrupted_removal_status_failed", status))
    status_entries = [entry for entry in (getattr(status, "stdout", "") or "").split("\0") if entry]
    if any(not entry.startswith("!! ") for entry in status_entries):
        return (False, "interrupted_removal_dirty_after_restore")
    if status_entries:
        ignored_ok, ignored_detail = _probe_worktree_ignored(worktree_path)
        if ignored_ok is None:
            return (False, f"interrupted_removal_ignored_probe_failed:{ignored_detail}"[:_RECLAIM_DETAIL_CAP])
        if ignored_ok is False:
            return (False, f"interrupted_removal_ignored:{ignored_detail}"[:_RECLAIM_DETAIL_CAP])

    removed, remove_detail = _remove_lane_worktree(
        repo_root,
        worktree_path,
        expected_tip=None,
        lane_id=lane_id,
        task_ref=task_ref,
        **({"landing_lock_held": True} if landing_lock_held else {}),
    )
    if not removed:
        detail = f"interrupted_removal_remove_failed:{remove_detail}"[:_RECLAIM_DETAIL_CAP]
        return (False, _with_fence_release_detail(detail, remove_detail))
    return (True, remove_detail)


def _worktree_head_is_recorded_branch(worktree_path: str, branch: object) -> bool | None:
    """Is this worktree's HEAD attached to the recorded lane branch?

    ``None`` when HEAD or the recorded branch name cannot be read. Detached
    HEAD and HEAD attached to any other branch are ``False``. Tree equality
    is diagnostic only and never authorizes reclaim from this helper.
    """
    name = _normalize_optional_text(branch)
    if name is None:
        return None
    ref = name[len("refs/heads/") :] if name.startswith("refs/heads/") else name
    proc = _run_reclaim_command(["git", "-C", worktree_path, "symbolic-ref", "-q", "HEAD"])
    if proc is None:
        return None
    if proc.returncode == 1:
        return False
    if proc.returncode != 0:
        return None
    head_ref = (proc.stdout or "").strip()
    if not head_ref:
        return None
    return head_ref == f"refs/heads/{ref}"


def _landed_reclaim_verdict(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    integration_ref: str,
    base_sha: str | None = None,
    created_at: object | None = None,
) -> tuple[bool | None, str]:
    """SHA merge verdict for worktree reclaim. Tree equality is diagnostic only.

    ``_probe_branch_merged_by_tree`` may still report ``merged_by_tree`` or
    ``merged_by_tree_untouched``, but that result does not authorize reclaim
    (AGT-10). A recorded attached branch that adds then deletes a product
    path can have an unchanged tree and nonempty
    ``git rev-list integration..lane``; that history stays retained.
    Detached HEAD, a different attached branch, and an unreadable HEAD all
    refuse. Identity-bound squash/rebase receipts are a separate contract.

    Ancestry alone cannot authorize reclaim of a never-started cut: an empty
    branch sitting on (or ancestral to) the integration ref is ``never_started``,
    not merged [OBS-12]. A valid positive ``base..tip`` count is authoritative
    own-work proof and is not overridden by equal-tip or first-parent empty
    heuristics. Unknown never becomes preserved.
    """
    merged, merged_detail = _probe_worktree_merged(
        repo_root=repo_root,
        branch=branch,
        worktree_path=worktree_path,
        integration_ref=integration_ref,
    )
    if merged is None:
        return (None, merged_detail)
    if merged is False:
        # Keep the recorded-HEAD refusal explicit so detached / other-branch
        # work cannot inherit a landed-true from a diagnostic tree probe.
        if (
            merged_detail == "worktree_head_not_merged"
            and _worktree_head_is_recorded_branch(worktree_path, branch) is not True
        ):
            return (False, merged_detail)
        return (False, merged_detail)
    empty, empty_detail = _empty_lane_blocks_landed_reclaim(
        repo_root=repo_root,
        branch=branch,
        worktree_path=worktree_path,
        integration_ref=integration_ref,
        base_sha=base_sha,
        created_at=created_at,
    )
    if empty is True:
        return (False, empty_detail)
    if empty is None:
        return (None, empty_detail)
    return (True, merged_detail)


def _trusted_landing_reclaim_verdict(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    task_ref: str | None,
    lane_id: str | None,
    proof: Mapping[str, object],
) -> tuple[bool | None, str]:
    """Validate a landing receipt against the task's registered integration ref.

    This proof is only used by the landing actuator. It does not widen the
    public reaper's default main-branch policy.
    """
    expected_task = proof.get("task_ref")
    expected_lane = proof.get("lane_id")
    expected_tip = proof.get("expected_tip")
    integration_ref = proof.get("integration_ref")
    landing_commit = proof.get("landing_commit")
    run_id = proof.get("run_id")
    if (
        not isinstance(task_ref, str)
        or not isinstance(lane_id, str)
        or task_ref != expected_task
        or lane_id != expected_lane
        or not isinstance(expected_tip, str)
        or re.fullmatch(r"[0-9a-f]{40}", expected_tip) is None
        or not isinstance(integration_ref, str)
        or not integration_ref.strip()
        or not isinstance(landing_commit, str)
        or re.fullmatch(r"[0-9a-f]{40}", landing_commit) is None
        or not isinstance(run_id, str)
    ):
        return (False, "task_landing_proof_invalid")
    normalized_branch = _recorded_branch_short_name(branch)
    if normalized_branch is None:
        return (False, "lane_branch_unavailable")
    branch_ref = (
        branch if isinstance(branch, str) and branch.startswith("refs/heads/") else f"refs/heads/{normalized_branch}"
    )
    current_tip = _reclaim_rev_parse(repo_root, branch_ref)
    if current_tip is None:
        return (None, "lane_branch_probe_failed")
    if current_tip != expected_tip:
        return (False, "lane_branch_tip_changed")
    if _worktree_head_is_recorded_branch(worktree_path, branch) is not True:
        return (False, "worktree_head_not_recorded_branch")

    integration = integration_ref.strip()
    try:
        from .orchestration.lane_land import _registered_integration_ref  # noqa: PLC0415

        declared, registration_error = _registered_integration_ref(
            root=repo_root,
            task_ref=task_ref,
            requested_ref=integration,
            manifest_root=proof.get("manifest_root"),
        )
    except Exception as exc:  # noqa: BLE001 — present malformed task proof fails closed
        return (None, f"task_integration_ref_unreadable:{type(exc).__name__}")
    if registration_error is not None or declared is None:
        return (False, registration_error or "integration_ref_not_task_registered")

    if proof.get("contained") is True:
        if re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
            return (False, "contained_landing_run_id_invalid")
        try:
            with _get_db_connection() as conn:
                lane_row = conn.execute(
                    """
                    SELECT status, branch, worktree_path, branch_tip_sha, landing_commit_sha
                    FROM worktree_lanes
                    WHERE task_ref = ? AND lane_id = ?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (task_ref, lane_id),
                ).fetchone()
        except Exception as exc:  # noqa: BLE001 — unreadable ownership never authorizes removal
            return (None, f"contained_lane_row_probe_failed:{type(exc).__name__}")
        if lane_row is None:
            return (False, "contained_terminal_lane_row_missing")
        recorded_branch = _recorded_branch_short_name(lane_row["branch"])
        recorded_path = _normalize_optional_text(lane_row["worktree_path"])
        if (
            str(lane_row["status"] or "").strip().lower() not in _RECLAIM_ELIGIBLE_TERMINAL_STATUSES
            or recorded_branch != normalized_branch
            or recorded_path != _normalize_optional_text(worktree_path)
            or lane_row["branch_tip_sha"] not in {expected_tip, landing_commit}
            or lane_row["landing_commit_sha"] != landing_commit
        ):
            return (False, "contained_terminal_lane_identity_mismatch")
        try:
            from .orchestration import landing_log  # noqa: PLC0415

            carrier = landing_log.find_carrier(repo_root, expected_tip, main_ref=landing_commit)
        except Exception as exc:  # noqa: BLE001 — contained ancestry is fail-closed
            return (None, f"contained_ancestry_probe_failed:{type(exc).__name__}")
        if getattr(carrier, "error", None):
            return (None, f"contained_ancestry_probe_failed:{carrier.error}")
        if not getattr(carrier, "contained", False) or getattr(carrier, "main_tip", None) != landing_commit:
            return (False, "lane_tip_not_contained_by_recorded_landing")
        try:
            relation = _run_reclaim_command(
                ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", landing_commit, integration]
            )
        except Exception:
            return (None, "integration_ancestry_probe_failed")
        if relation is None or relation.returncode not in {0, 1}:
            return (None, "integration_ancestry_probe_failed")
        if relation.returncode != 0:
            return (False, "landing_commit_not_in_task_integration")
        return (True, "verified_recorded_contained_landing")

    try:
        from .orchestration import landing_log  # noqa: PLC0415

        verdict = landing_log.verify_receipt(repo_root, landing_commit, main_ref=integration)
    except Exception as exc:  # noqa: BLE001 — unverified receipts never authorize removal
        return (None, f"landing_receipt_probe_failed:{type(exc).__name__}")
    receipt = getattr(verdict, "receipt", None)
    if not getattr(verdict, "verified", False):
        failed = getattr(verdict, "failed_check", None) or "unknown"
        return (False, f"landing_receipt_unverified:{failed}")
    if (
        getattr(receipt, "task_ref", None) != task_ref
        or getattr(receipt, "lane_id", None) != lane_id
        or getattr(receipt, "tip", None) != expected_tip
        or getattr(receipt, "run_id", None) != run_id
    ):
        return (False, "landing_receipt_identity_mismatch")
    try:
        relation = _run_reclaim_command(
            ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", landing_commit, integration]
        )
    except Exception:
        return (None, "integration_ancestry_probe_failed")
    if relation is None or relation.returncode not in {0, 1}:
        return (None, "integration_ancestry_probe_failed")
    if relation.returncode != 0:
        return (False, "landing_commit_not_in_task_integration")
    return (True, "verified_task_landing_receipt")


def _recorded_branch_short_name(branch: object) -> str | None:
    text = _normalize_optional_text(branch)
    if text is None:
        return None
    if text.startswith("refs/heads/"):
        return text[len("refs/heads/") :]
    return text


def _branch_checkout_blocks_reclaim(
    *,
    repo_root: Path | str,
    branch: object,
    exclude_path: object = None,
) -> tuple[bool, str]:
    """Refuse when another registered linked worktree has ``branch`` checked out."""
    want = _recorded_branch_short_name(branch)
    if want is None:
        return (False, "")
    root = Path(repo_root)
    proc = _run_reclaim_command(["git", "-C", str(root), "worktree", "list", "--porcelain"])
    if proc is None or proc.returncode != 0:
        return (True, "worktree_list_failed")
    entries = _parse_worktree_registry(proc.stdout or "")
    exclude = _resolved_path_text(exclude_path).rstrip("/") if exclude_path is not None else ""
    for entry in entries:
        path = _resolved_path_text(entry.get("path")).rstrip("/")
        if exclude and path == exclude:
            continue
        if _recorded_branch_short_name(entry.get("branch")) == want:
            return (True, f"branch_checked_out:{path}"[:_RECLAIM_DETAIL_CAP])
    return (False, "")


_BRANCH_LEDGER_TERMINALS = frozenset({"closed", "merged", "closed_stale", "superseded"})
_BRANCH_OWNER_PAGE_LIMIT = 1000
_BRANCH_OWNER_PAGE_CAP = 64


def _branch_owner_page_lanes(
    env: object,
) -> tuple[str | None, list[Any], bool, object]:
    """Parse one ``list_lanes`` page. Fail closed on a malformed envelope."""
    if not isinstance(env, dict):
        return ("branch_owner_lookup_failed", [], False, None)
    if env.get("ok") is False:
        return ("branch_owner_lookup_failed", [], False, None)
    data: Any = env.get("data") if isinstance(env.get("data"), dict) else env
    if not isinstance(data, dict):
        return ("branch_owner_lookup_failed", [], False, None)
    lanes = data.get("lanes")
    if lanes is None and isinstance(env.get("lanes"), list):
        lanes = env.get("lanes")
    if not isinstance(lanes, list):
        return ("branch_owner_lookup_malformed", [], False, None)
    has_more_present = "has_more" in data or "has_more" in env
    has_more_raw = data.get("has_more") if "has_more" in data else env.get("has_more")
    if not has_more_present or type(has_more_raw) is not bool:
        return ("branch_owner_lookup_malformed", [], False, None)
    has_more = has_more_raw
    next_present = "next_after_id" in data or "next_after_id" in env
    next_after_id = data.get("next_after_id") if "next_after_id" in data else env.get("next_after_id")
    if has_more:
        if not next_present or next_after_id is None or type(next_after_id) is bool:
            return ("branch_owner_lookup_malformed", [], False, None)
        if type(next_after_id) is not int and not (isinstance(next_after_id, str) and str(next_after_id).strip()):
            return ("branch_owner_lookup_malformed", [], False, None)
    return (None, lanes, has_more, next_after_id)


def _branch_ledger_owner_blocks_reclaim(
    *,
    branch: object,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> tuple[bool, str]:
    """Refuse when a global non-terminal ledger row still owns ``branch``.

    Pages the existing ``list_lanes`` reader to exhaustion under the held
    claim (REAP07RV-003). A malformed, empty, repeating, or stalled
    continuation page fails closed. Deletion is authorized only after the
    complete non-terminal owner set has been examined.
    """
    want = _recorded_branch_short_name(branch)
    if want is None:
        return (False, "")
    try:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            list_lanes,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (True, f"branch_owner_import_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    after_id: object = None
    offset = 0
    use_keyset = True
    seen_ids: set[object] = set()
    for _ in range(_BRANCH_OWNER_PAGE_CAP):
        try:
            if use_keyset:
                env = list_lanes(
                    all_tasks=True,
                    status="all",
                    limit=_BRANCH_OWNER_PAGE_LIMIT,
                    after_id=after_id,
                )
            else:
                env = list_lanes(
                    all_tasks=True,
                    status="all",
                    limit=_BRANCH_OWNER_PAGE_LIMIT,
                    offset=offset,
                )
        except Exception as exc:  # noqa: BLE001 — fail closed
            return (True, f"branch_owner_probe_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
        err, lanes, has_more, next_after_id = _branch_owner_page_lanes(env)
        if err:
            return (True, err)
        continuation = after_id is not None or offset > 0
        if not lanes:
            if has_more or continuation:
                return (True, "branch_owner_page_empty")
            return (False, "")
        new_ids = 0
        for owner in lanes:
            if not isinstance(owner, dict):
                return (True, "branch_owner_malformed")
            row_id = owner.get("id")
            if row_id is not None:
                if row_id in seen_ids:
                    return (True, "branch_owner_page_repeat")
                seen_ids.add(row_id)
                new_ids += 1
            owner_lane = owner.get("lane_id")
            owner_task = owner.get("task_ref")
            if owner_lane == lane_id and owner_task == task_ref:
                continue
            status = str(owner.get("status") or "")
            if status in _BRANCH_LEDGER_TERMINALS:
                continue
            if _recorded_branch_short_name(owner.get("branch")) == want:
                return (True, f"branch_owned_by_lane:{owner_lane}"[:_RECLAIM_DETAIL_CAP])
        if not has_more:
            return (False, "")
        if use_keyset:
            if next_after_id is None:
                if after_id is None and new_ids > 0:
                    use_keyset = False
                    offset = len(lanes)
                    continue
                return (True, "branch_owner_lookup_malformed")
            if next_after_id == after_id:
                return (True, "branch_owner_lookup_malformed")
            after_id = next_after_id
            continue
        if new_ids == 0:
            return (True, "branch_owner_page_stalled")
        offset += len(lanes)
    return (True, "branch_owner_scan_truncated")


def _under_claim_worktree_reclaim_guards(
    *,
    worktree_path: object,
    branch: object,
    repo_root: Path | str,
    task_ref: str | None = None,
    lane_id: str | None = None,
    owner_probe: Callable[[object], tuple[str, str]] | None = None,
) -> tuple[str | None, str]:
    """Under-claim guards shared by every worktree reclaim actuator (REF-26).

    Caller must already hold the lane reaping claim. This helper re-checks
    linked-worktree identity, recorded HEAD, owner, heartbeat, dirty and
    ignored content, and the global shared-path ledger immediately before
    removal. With no surviving tree it still enumerates other checkouts of
    ``branch`` and global non-terminal ledger owners of that branch so a
    branch-only delete cannot drop a successor's live ref. It does not
    acquire the lock, sample merge, or remove the worktree.
    ``(None, detail)`` means the guards still authorize removal.
    """
    path = _normalize_optional_text(worktree_path)
    root = Path(repo_root)
    ignored_detail = ""
    if path is not None:
        linked, registry_detail = _linked_worktree_paths(root)
        if linked is None:
            return ("refused_registry_unavailable", registry_detail)
        target = _resolved_path_text(path).rstrip("/")
        if not target or target not in linked:
            return ("refused_not_linked_worktree", f"not a linked worktree of {root}")
        root_resolved = _resolved_path_text(root).rstrip("/")
        if target == root_resolved:
            return ("refused_repo_root", "target equals repo_root")

        head_ok = _worktree_head_is_recorded_branch(path, branch)
        if head_ok is None:
            return ("refused_head_unknown", "worktree_head_unreadable")
        if head_ok is not True:
            return ("refused_head_mismatch", "worktree_head_not_recorded_branch")

        # Porcelain can be clean while MERGE_AUTOSTASH owns uncommitted edits.
        # Check operation custody here, under the removal claim [SECD-03/05].
        gitdir_probe = _run_reclaim_command(["git", "-C", path, "rev-parse", "--absolute-git-dir"])
        if gitdir_probe is None or gitdir_probe.returncode != 0:
            return ("refused_operation_unknown", "worktree_gitdir_unreadable")
        gitdir_text = _probe_command_stdout(gitdir_probe).strip()
        if not gitdir_text:
            return ("refused_operation_unknown", "worktree_gitdir_unreadable")
        gitdir = Path(gitdir_text)
        try:
            for marker in (
                "MERGE_HEAD",
                "MERGE_AUTOSTASH",
                "CHERRY_PICK_HEAD",
                "REVERT_HEAD",
                "rebase-merge",
                "rebase-apply",
            ):
                # lstat also retains dangling links and reports permission errors.
                try:
                    (gitdir / marker).lstat()
                except FileNotFoundError:
                    continue
                return ("refused_operation_in_progress", f"pending_git_operation:{marker}")
        except OSError:
            return ("refused_operation_unknown", "worktree_operation_metadata_unreadable")

        probe = owner_probe if owner_probe is not None else _probe_worktree_process_owner
        state, owner_detail = probe(path)
        if state == "owned":
            return ("refused_owned", owner_detail)
        if state != "free":
            return ("refused_owner_unknown", owner_detail)

        heartbeat_blocks, heartbeat_detail = _session_heartbeat_blocks_reclaim(
            repo_root=root,
            worktree_path=path,
            task_ref=task_ref,
            lane_id=lane_id,
        )
        if heartbeat_blocks:
            return ("refused_session_live", heartbeat_detail)

        clean, clean_detail = _probe_worktree_clean(path)
        if clean is None:
            return ("refused_status_unknown", clean_detail)
        interrupted_removal = False
        if clean is False:
            if not _is_interrupted_worktree_removal(root, path, branch):
                return _dirty_refusal_outcome(root, path, clean_detail)
            interrupted_removal = True

        ignored_ok, ignored_detail = _probe_worktree_ignored(path)
        if ignored_ok is None:
            if ignored_detail == "ignored_probe_timeout":
                return ("refused_ignored_timeout", ignored_detail)
            return ("refused_ignored_unknown", ignored_detail)
        if ignored_ok is False:
            return ("refused_ignored", ignored_detail)

        shared_blocks, shared_detail = _shared_path_blocks_reclaim(
            worktree_path=path,
            task_ref=task_ref,
            lane_id=lane_id,
            repo_root=root,
        )
        if shared_blocks:
            return ("refused_shared_path", shared_detail)
        if interrupted_removal:
            ignored_detail = f"interrupted_removal_resumed;{ignored_detail}"
        return (None, ignored_detail)

    if _normalize_optional_text(branch) is None:
        return ("skipped_no_worktree_path", "")
    checkout_blocks, checkout_detail = _branch_checkout_blocks_reclaim(
        repo_root=root,
        branch=branch,
        exclude_path=None,
    )
    if checkout_blocks:
        return ("refused_owned", checkout_detail)
    ledger_blocks, ledger_detail = _branch_ledger_owner_blocks_reclaim(
        branch=branch,
        task_ref=task_ref,
        lane_id=lane_id,
    )
    if ledger_blocks:
        return ("refused_owned", ledger_detail)
    return (None, "branch_only")


def _no_work_lane_commit_count(
    *,
    row: Mapping[str, object],
    pass_record: Mapping[str, object],
    repo_root: Path | str,
) -> tuple[int | None, str]:
    """Count commits from a lane's recorded base to its current branch tip.

    A missing or malformed base is not inferred from the reflog: it is not
    proof that a lane produced no work. Pass state is accepted as recorded
    base evidence only when the row has no base field of its own.
    """
    task_ref = _normalize_optional_text(row.get("task_ref"))
    lane_id = _normalize_optional_text(row.get("lane_id"))
    branch = _recorded_branch_short_name(row.get("branch"))
    if task_ref is None or lane_id is None or branch is None:
        return (None, "refused_base_unknown")

    result = pass_record.get("result")
    pass_base: object = pass_record.get("base_sha")
    if not pass_base and isinstance(result, Mapping):
        pass_base = result.get("base_sha")
    base_row = dict(row)
    if not base_row.get("base_sha") and pass_base:
        base_row["base_sha"] = pass_base

    try:
        from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
            _try_authoritative_lane_base_sha,
        )

        base_sha, base_error = _try_authoritative_lane_base_sha(
            row=base_row,
            task_ref=task_ref,
            lane_id=lane_id,
            repo=Path(repo_root),
        )
    except Exception:  # noqa: BLE001 — unreadable base evidence fails closed
        return (None, "refused_base_unknown")
    binding = pass_record.get("lane_cycle_binding")
    if isinstance(binding, Mapping):
        source = binding.get("source_base_sha")
        if not isinstance(source, str) or re.fullmatch(r"[0-9a-fA-F]{40}", source) is None:
            return (None, "refused_source_mismatch")
        if base_error or not base_sha or base_sha != source:
            return (None, "refused_source_mismatch")
        base_sha = source
    if base_error or not base_sha:
        return (None, "refused_base_unknown")

    branch_tip = _reclaim_rev_parse(Path(repo_root), f"refs/heads/{branch}")
    if branch_tip is None:
        return (None, "refused_tip_unknown")
    count = _reclaim_rev_list_count(Path(repo_root), base_sha, branch_tip)
    if count is None or count < 0:
        return (None, "refused_commit_count_unknown")
    return (count, "zero_own_commits" if count == 0 else "own_commits_present")


def _no_work_pass_started_at_ns(pass_record: Mapping[str, object]) -> int | None:
    """Return durable pass-start evidence as epoch nanoseconds, if recorded."""
    value = pass_record.get("started_at")
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)) or value < 0:
            return None
        return int(float(value) * 1_000_000_000)
    started_at = _parse_sqlite_utc(value)
    if started_at is None:
        return None
    try:
        return int(started_at.timestamp() * 1_000_000_000)
    except (OverflowError, OSError, ValueError):
        return None


def _no_work_worktree_provisioned_at_ns(worktree_path: Path | str) -> int | None:
    """Use the linked worktree's stable ``.git`` pointer ctime as incarnation evidence."""
    git_pointer = Path(worktree_path) / ".git"
    try:
        pointer_stat = git_pointer.lstat()
    except OSError:
        return None
    if not stat.S_ISREG(pointer_stat.st_mode):
        return None
    created_ns = getattr(pointer_stat, "st_ctime_ns", None)
    if isinstance(created_ns, int) and created_ns > 0:
        return created_ns
    try:
        created_at = float(pointer_stat.st_ctime)
    except (AttributeError, OverflowError, TypeError, ValueError):
        return None
    if not math.isfinite(created_at) or created_at <= 0:
        return None
    return int(created_at * 1_000_000_000)


def _no_work_incarnation_fence(
    *, worktree_path: Path | str, pass_record: Mapping[str, object]
) -> tuple[str | None, str]:
    """Require the no-work pass to have started in this worktree incarnation."""
    pass_started_at_ns = _no_work_pass_started_at_ns(pass_record)
    if pass_started_at_ns is None:
        return ("refused_pass_start_unknown", "terminal pass has no valid started_at timestamp")
    provisioned_at_ns = _no_work_worktree_provisioned_at_ns(worktree_path)
    if provisioned_at_ns is None:
        return ("refused_provisioning_unknown", "worktree .git pointer ctime is unavailable")
    if pass_started_at_ns < provisioned_at_ns:
        return (
            "refused_reprovisioned_after_pass",
            "worktree .git pointer was provisioned after the no-work pass started",
        )
    return (None, "pass_started_in_worktree_incarnation")


def _fresh_no_work_pass_authority(*, state_dir: Path, task_ref: str, lane_id: str) -> tuple[dict[str, Any] | None, str]:
    """Read every durable pass under lane EX; a caller snapshot is never authority."""
    from .orchestration.offload_pass import (  # noqa: PLC0415
        _TERMINAL_NON_DONE_PASS_STATUSES,
        _latest_pass_payload,
    )
    from .orchestration.offload_pass_index import lane_pass_records  # noqa: PLC0415
    from .orchestration.pass_terminality import pass_outcome_is_terminal  # noqa: PLC0415

    try:
        indexed = lane_pass_records(state_dir, task_ref=task_ref, lane_id=lane_id, index_lock_timeout_seconds=1.0)
        if not indexed.sources_complete or not indexed.records:
            return (None, "refused_pass_authority_unknown")
        for record in indexed.records:
            payload = record.payload
            if (
                not isinstance(payload, dict)
                or payload.get("task_ref") != task_ref
                or payload.get("lane_id") != lane_id
            ):
                return (None, "refused_pass_authority_unknown")
            status = payload.get("status")
            result = payload.get("result")
            if status == "done":
                outcome = result.get("outcome") if isinstance(result, Mapping) else None
                if not isinstance(outcome, str) or not pass_outcome_is_terminal(outcome):
                    return (None, "refused_pass_not_terminal")
            elif not isinstance(status, str) or status not in _TERMINAL_NON_DONE_PASS_STATUSES:
                return (None, "refused_pass_not_terminal")
        latest = _latest_pass_payload(indexed.records)
        if latest is None:
            return (None, "refused_pass_authority_unknown")
        return (latest, "")
    except Exception:  # noqa: BLE001 — incomplete history cannot authorize Git
        return (None, "refused_pass_authority_unknown")


def _no_work_binding_refusal(row: Mapping[str, object], binding: object) -> str:
    """A logical cycle, row and physical checkout must all name the published cycle."""
    if not isinstance(binding, Mapping) or type(binding.get("v")) is not int or binding["v"] != 1:
        return "refused_cycle_binding_unknown"
    for field in ("row_id", "cycle_epoch"):
        if type(binding.get(field)) is not int or binding[field] <= 0:
            return "refused_cycle_binding_unknown"
    if type(row.get("cycle_epoch")) is not int or row["cycle_epoch"] <= 0:
        return "refused_cycle_binding_unknown"
    for field in ("task_ref", "lane_id", "worktree_path", "branch"):
        if not isinstance(binding.get(field), str) or not binding[field]:
            return "refused_cycle_binding_unknown"
    if binding["row_id"] != row.get("id") or binding["cycle_epoch"] != row.get("cycle_epoch"):
        return "refused_cycle_mismatch"
    if any(binding[field] != row.get(field) for field in ("task_ref", "lane_id", "worktree_path", "branch")):
        return "refused_cycle_mismatch"
    return ""


def _no_work_holder_matches(row: Mapping[str, object], fence: str, held: _ReclaimFenceLock) -> bool:
    if not _reclaim_fence_recoverable(fence, held=held):
        return False
    holder = json.loads(fence)
    return (
        holder.get("purpose") == "no_work_release"
        and type(holder.get("bound_cycle_epoch")) is int
        and holder["bound_cycle_epoch"] == row.get("cycle_epoch")
        and type(holder.get("row_id")) is int
        and holder["row_id"] == row.get("id")
        and holder.get("task_ref") == row.get("task_ref")
        and holder.get("lane_id") == row.get("lane_id")
    )


def _no_work_release_row_cas(
    row: Mapping[str, object], *, binding: Mapping[str, object], holder_json: str, held: _ReclaimFenceLock
) -> tuple[bool, str]:
    """Claim only this row and epoch in one short transaction; never change status here."""
    from workbay_handoff_mcp.shared_schema import (  # noqa: PLC0415
        unproven_cycle_epoch_triggers,
        unproven_reclaim_fence_guards,
    )

    try:
        with _get_db_connection(begin_immediate=True) as conn:
            if unproven_reclaim_fence_guards(conn) or unproven_cycle_epoch_triggers(conn):
                return (False, "reclaim_fence_unavailable")
            found = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=? ORDER BY id DESC LIMIT 1",
                (row["task_ref"], row["lane_id"]),
            ).fetchone()
            if found is None:
                return (False, "refused_cycle_mismatch")
            current = dict(found)
            refusal = _no_work_binding_refusal(current, binding)
            if refusal:
                return (False, refusal)
            if current.get("status") not in {"active", "blocked", "planned"}:
                return (False, "refused_lane_status")
            observed = current.get("reclaim_fence")
            if observed is not None and not _no_work_holder_matches(current, str(observed), held):
                return (False, "reclaim_fence_held")
            updated = conn.execute(
                "UPDATE worktree_lanes SET reclaim_fence=? "
                "WHERE id=? AND cycle_epoch=? AND worktree_path=? AND branch=? AND reclaim_fence IS ?",
                (
                    holder_json,
                    binding["row_id"],
                    binding["cycle_epoch"],
                    binding["worktree_path"],
                    binding["branch"],
                    observed,
                ),
            )
            return (True, "") if updated.rowcount == 1 else (False, "refused_cycle_mismatch")
    except Exception:  # noqa: BLE001 — no proved claim, no Git
        _logger.warning("no-work fence claim failed for lane %s", row.get("lane_id"), exc_info=True)
        return (False, "reclaim_fence_unavailable")


def _run_under_no_work_fence(
    *, row: Mapping[str, object], binding: Mapping[str, object], actuator: Callable[[], tuple[str, str]]
) -> tuple[str, str]:
    """Use retireid17's inode custody and release proof for a no-work cycle."""
    _assert_no_reaper_write_lock(reason="fcntl.flock")
    try:
        path = _reclaim_fence_lock_path(str(row["task_ref"]), str(row["lane_id"]))
        refusal, held = _acquire_reclaim_fence_lock(path)
    except Exception:  # noqa: BLE001
        return ("reclaim_fence_unavailable", "fence lock path unavailable")
    if held is None:
        return (refusal, "fence lock unavailable")
    fds_token = _ACTIVE_RECLAIM_FENCE_LOCK_FDS.set((*_ACTIVE_RECLAIM_FENCE_LOCK_FDS.get(), held.descriptor))
    thread_token = _ACTIVE_RECLAIM_FENCE_THREAD.set(threading.get_ident())
    holder_json = json.dumps(
        {
            "token": uuid.uuid4().hex,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "lane_id": row["lane_id"],
            "task_ref": row["task_ref"],
            "lock": held.path,
            "lock_dev": held.device,
            "lock_ino": held.inode,
            "lock_kind": _RECLAIM_FENCE_LOCK_KIND,
            "purpose": "no_work_release",
            "row_id": binding["row_id"],
            "bound_cycle_epoch": binding["cycle_epoch"],
        },
        sort_keys=True,
    )
    claimed = False
    result = ("no_work_release_failed", "actuator did not finish")
    error: BaseException | None = None
    try:
        claimed, refusal = _no_work_release_row_cas(row, binding=binding, holder_json=holder_json, held=held)
        if not claimed:
            return (refusal, "lane cycle changed or fence unavailable")
        try:
            result = actuator()
        except BaseException as exc:
            error = exc
            raise
    finally:
        _ACTIVE_RECLAIM_FENCE_THREAD.reset(thread_token)
        _ACTIVE_RECLAIM_FENCE_LOCK_FDS.reset(fds_token)
        _close_reclaim_fence_lock(held.descriptor)
        if claimed:
            release = _release_reclaim_fence(
                row_id=int(binding["row_id"]),
                holder_json=holder_json,
                lane_id=str(row["lane_id"]),
                held=held,
                no_work_epoch=int(binding["cycle_epoch"])
                if error is None and result[0] in {"released_no_work", "already_released"}
                else None,
            )
            if error is not None:
                _attach_reclaim_fence_release(error, release)
    return (result[0], f"{result[1]};{_REAP_FENCE_RELEASE_DETAIL}{release}")


def release_no_work_lane_worktree(
    *, row: Mapping[str, object], pass_record: Mapping[str, object], repo_root: Path | str, apply: bool
) -> tuple[str, str]:
    """Release one freshly published cycle under WorkerLock, lane EX and a durable row fence.

    ``pass_record`` is advisory only. No SQLite transaction or pass-index lock
    spans a Git probe or mutation. The branch ref is kept for future dispatch.
    """
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415
    from workbay_protocol.lifecycle_coordination import LifecycleLeaseTimeout, LifecycleOwnershipError  # noqa: PLC0415

    from .orchestration.lane_publication_fence import lane_publication_lease  # noqa: PLC0415

    path = _normalize_optional_text(row.get("worktree_path"))
    lane_id = _normalize_optional_text(row.get("lane_id"))
    task_ref = _normalize_optional_text(row.get("task_ref"))
    branch = _normalize_optional_text(row.get("branch"))
    root = Path(repo_root)
    if path is None or lane_id is None or task_ref is None or branch is None:
        return ("refused_lane_identity_unknown", "lane row identity is incomplete")
    if not Path(path).exists():
        # Step (0) has no destructive effect and needs no lifecycle permission.
        # Consult the durable row rather than a caller's stale planned snapshot.
        try:
            absent, ambiguous = _read_durable_lane_row(task_ref=task_ref, lane_id=lane_id)
        except Exception:  # noqa: BLE001 — no row authority for an absent checkout
            return ("refused_worktree_missing", "absent checkout row is unreadable")
        if (
            ambiguous
            or absent is None
            or any(absent.get(field) != row.get(field) for field in ("id", "worktree_path", "branch"))
        ):
            return ("refused_worktree_missing", "absent checkout row changed")
        if absent.get("reclaim_fence") is None:
            return (
                ("already_released", "worktree is already absent")
                if absent.get("status") == "planned"
                else ("refused_worktree_missing", "recorded worktree is absent")
            )
    claim_handle, claim_detail = _try_row_reaping_claim(lane_id)
    if claim_handle is None:
        return (
            "refused_lock_held" if _is_peer_held_reaping_claim(claim_detail) else "refused_lock_unavailable",
            claim_detail,
        )
    try:
        with ExitStack() as stack:
            try:
                state_dir = Path(get_runtime_config().state_dir)
                stack.enter_context(
                    lane_publication_lease(
                        state_dir, task_ref, lane_id, exclusive=True, deadline=time.monotonic() + 1.0
                    )
                )
            except (LifecycleLeaseTimeout, LifecycleOwnershipError, OSError) as exc:
                return ("refused_lifecycle_busy", str(exc))
            try:
                fresh, ambiguous = _read_durable_lane_row(task_ref=task_ref, lane_id=lane_id)
            except Exception:  # noqa: BLE001
                return ("refused_lane_row_unknown", "lane row unreadable")
            if ambiguous or fresh is None:
                return ("refused_lane_row_unknown", "lane row unavailable")
            if any(fresh.get(field) != row.get(field) for field in ("id", "worktree_path", "branch")):
                return ("refused_cycle_mismatch", "lane identity changed")
            if not Path(path).exists():
                if fresh.get("reclaim_fence") is None:
                    return (
                        ("already_released", "worktree is already absent")
                        if fresh.get("status") == "planned"
                        else ("refused_worktree_missing", "recorded worktree is absent")
                    )
                try:
                    holder = json.loads(str(fresh["reclaim_fence"]))
                    if (
                        holder.get("purpose") != "no_work_release"
                        or type(holder.get("bound_cycle_epoch")) is not int
                        or holder["bound_cycle_epoch"] != fresh.get("cycle_epoch")
                    ):
                        return ("refused_worktree_missing", "absent checkout has no matching no-work fence")
                    binding = {
                        "v": 1,
                        "row_id": fresh["id"],
                        "cycle_epoch": holder["bound_cycle_epoch"],
                        **{key: fresh[key] for key in ("task_ref", "lane_id", "worktree_path", "branch")},
                    }
                except (ValueError, TypeError, AttributeError, RecursionError):
                    return ("refused_worktree_missing", "absent checkout has malformed fence")
                if not apply:
                    return ("would_release_no_work", "recover fenced absent checkout")
                outcome, detail = _run_under_no_work_fence(
                    row=fresh, binding=binding, actuator=lambda: ("already_released", "recovered absent checkout")
                )
                return (outcome if outcome == "already_released" else "refused_worktree_missing", detail)

            authoritative, refusal = _fresh_no_work_pass_authority(
                state_dir=state_dir, task_ref=task_ref, lane_id=lane_id
            )
            if refusal or authoritative is None:
                return (refusal, "fresh pass history does not authorize release")
            binding = authoritative.get("lane_cycle_binding")
            refusal = _no_work_binding_refusal(fresh, binding)
            if refusal:
                return (refusal, "pass binding does not name this lane cycle")
            assert isinstance(binding, Mapping)

            def probe() -> tuple[str, str]:
                latest, refusal = _fresh_no_work_pass_authority(state_dir=state_dir, task_ref=task_ref, lane_id=lane_id)
                if refusal or latest is None:
                    return (refusal, "fresh pass history changed")
                refusal = _no_work_binding_refusal(fresh, latest.get("lane_cycle_binding"))
                if refusal:
                    return (refusal, "pass binding changed")
                incarnation, detail = _no_work_incarnation_fence(worktree_path=path, pass_record=latest)
                if incarnation:
                    return (incarnation, detail)
                count, detail = _no_work_lane_commit_count(row=fresh, pass_record=latest, repo_root=root)
                if count is None:
                    return (detail, "recorded source or branch tip is unknown")
                if count != 0:
                    return ("refused_own_commits", detail)
                guard, detail = _under_claim_worktree_reclaim_guards(
                    worktree_path=path, branch=branch, repo_root=root, task_ref=task_ref, lane_id=lane_id
                )
                return (guard or "", detail)

            refusal, detail = probe()
            if refusal:
                return (refusal, detail)
            if not apply:
                return ("would_release_no_work", "zero_own_commits;" + detail)

            def remove() -> tuple[str, str]:
                refusal, detail = probe()
                if refusal:
                    return (refusal, detail)
                if detail.startswith("interrupted_removal_resumed;"):
                    removed, removal = _remove_interrupted_lane_worktree(root, path, lane_id=lane_id, task_ref=task_ref)
                    if not removed:
                        return (removal.split(":", 1)[0], removal)
                else:
                    removed, removal = _remove_lane_worktree(
                        root, path, expected_tip=None, lane_id=lane_id, task_ref=task_ref
                    )
                    if not removed:
                        return ("remove_failed", removal)
                return ("released_no_work", _with_fence_release_detail("zero_own_commits;" + detail, removal))

            return _run_under_no_work_fence(row=fresh, binding=binding, actuator=remove)
    finally:
        _release_lane_worker_lock(claim_handle)


def _terminal_never_started_lane_row(
    *,
    task_ref: str | None,
    lane_id: str | None,
) -> tuple[bool, str]:
    """Authorize the never-started exception only from a fresh terminal row."""
    task = _normalize_optional_text(task_ref)
    lane = _normalize_optional_text(lane_id)
    if task is None or lane is None:
        return (False, "never_started")
    try:
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT status FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? ORDER BY id DESC LIMIT 1",
                (task, lane),
            ).fetchone()
    except Exception:  # noqa: BLE001 — unreadable rows never authorize deletion
        return (False, "never_started")
    if row is None:
        return (False, "never_started")
    status = str(row["status"] or "").strip().lower()
    if status not in _RECLAIM_ELIGIBLE_TERMINAL_STATUSES:
        return (False, "never_started")
    return (True, "never_started_terminal_row")


@dataclass
class _RowlessLaneRowIndex:
    """One rowless sweep's lane-row snapshot and lookup caches."""

    error: str | None
    row_path_branches: dict[int, tuple[object, object]]
    branch_rows: dict[str, tuple[int, sqlite3.Row]]
    path_rows: dict[str, tuple[int, sqlite3.Row]]
    basename_rows: dict[str, list[tuple[int, str, sqlite3.Row]]]
    resolved_path_rows: dict[str, tuple[int, sqlite3.Row]]
    resolved_path_row_ids: dict[str, list[int]]
    resolved_row_paths: dict[str, str]
    symlink_component_cache: dict[str, bool]
    resolved_worktree_paths: dict[str, str]
    max_row_id: int
    identity_seq_available: bool
    # Persistent counter value observed before reading the row snapshot.
    max_identity_seq: int


_ROWLESS_LANE_ROW_INDEX: contextvars.ContextVar[_RowlessLaneRowIndex | None] = contextvars.ContextVar(
    "rowless_lane_row_index",
    default=None,
)
_ROWLESS_LANE_ROW_FORCE_FRESH: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "rowless_lane_row_force_fresh",
    default=False,
)

# Fresh scans cap rows that are new or whose path/branch changed since the
# sweep snapshot. SQLite cannot compare realpath/symlink identity, so changed
# rows are checked in Python; overflow refuses authorization rather than
# silently omitting an unexamined possible alias.
_ROWLESS_FRESH_INSERT_FALLBACK_LIMIT = 256


def _ensure_rowless_fresh_query_indexes(conn: sqlite3.Connection) -> None:
    """Index normalized SQL predicates used by per-candidate fresh probes."""
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rowless_worktree_lanes_branch_trim ON worktree_lanes(trim(branch))")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rowless_worktree_lanes_path_norm "
        "ON worktree_lanes(rtrim(trim(worktree_path), '/'))"
    )


def _rowless_path_has_symlink_component(path_text: str, cache: dict[str, bool]) -> bool:
    """Identify recorded spellings that may resolve to a different path."""
    path = Path(path_text)
    try:
        if not os.path.lexists(path_text):
            return False
    except OSError:
        # An inaccessible path cannot safely be assumed to be a non-symlink.
        return True
    if not path.is_absolute() or os.path.normpath(path_text) != path_text.rstrip("/"):
        return True
    for component in (path, *path.parents):
        component_text = str(component)
        if component_text not in cache:
            try:
                cache[component_text] = component.is_symlink()
            except OSError:
                cache[component_text] = False
        if cache[component_text]:
            return True
    return False


def _load_rowless_lane_row_index() -> _RowlessLaneRowIndex:
    """Read all lane rows once and index only fields used by rowless checks."""
    try:
        with _get_db_connection() as conn:
            # Fresh probes use these expression indexes for their exact,
            # normalized branch/path predicates. Symlink identity is not a
            # SQL property and is handled through the bounded candidate sets
            # below instead of resolving every recorded path per worktree.
            _ensure_rowless_fresh_query_indexes(conn)
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(worktree_lanes)").fetchall()}
            identity_seq_column_available = "identity_seq" in columns
            identity_seq_select = ", identity_seq" if identity_seq_column_available else ""
            counter_table_available = identity_seq_column_available and (
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'worktree_lanes_identity_seq_counter'"
                ).fetchone()
                is not None
            )
            identity_seq_available = False
            max_identity_seq = 0
            if counter_table_available:
                # Read the persistent high-water mark with the rows in one
                # SQLite statement. Changes committed afterward receive a
                # larger value and are included by the fresh probe; an empty
                # lane table still returns the counter through its sentinel.
                rows = conn.execute(
                    "SELECT lanes.id, lanes.lane_id, lanes.task_ref, lanes.worktree_path, lanes.branch, "
                    "lanes.identity_seq, counter.value AS _rowless_identity_seq_high_water, "
                    "0 AS _rowless_empty FROM worktree_lanes AS lanes "
                    "LEFT JOIN worktree_lanes_identity_seq_counter AS counter ON counter.id = 1 "
                    "UNION ALL "
                    "SELECT NULL, NULL, NULL, NULL, NULL, NULL, counter.value, 1 "
                    "FROM worktree_lanes_identity_seq_counter AS counter "
                    "WHERE counter.id = 1 AND NOT EXISTS (SELECT 1 FROM worktree_lanes) "
                    "ORDER BY 8, 1"
                ).fetchall()
                if rows:
                    counter_value = rows[0]["_rowless_identity_seq_high_water"]
                    if counter_value is not None:
                        identity_seq_available = True
                        max_identity_seq = int(counter_value)
            else:
                rows = conn.execute(
                    "SELECT id, lane_id, task_ref, worktree_path, branch"
                    f"{identity_seq_select} FROM worktree_lanes ORDER BY id"
                ).fetchall()
    except Exception as exc:  # noqa: BLE001 — rowlessness is an authorization gate
        return _RowlessLaneRowIndex(
            error=f"rowless_row_probe_failed:{exc}"[:_RECLAIM_DETAIL_CAP],
            row_path_branches={},
            branch_rows={},
            path_rows={},
            basename_rows={},
            resolved_path_rows={},
            resolved_path_row_ids={},
            resolved_row_paths={},
            symlink_component_cache={},
            resolved_worktree_paths={},
            max_row_id=0,
            identity_seq_available=False,
            max_identity_seq=0,
        )

    row_tuple = tuple(row for row in rows if row["id"] is not None)
    branch_rows: dict[str, tuple[int, sqlite3.Row]] = {}
    path_rows: dict[str, tuple[int, sqlite3.Row]] = {}
    basename_rows: dict[str, list[tuple[int, str, sqlite3.Row]]] = {}
    resolved_path_rows: dict[str, tuple[int, sqlite3.Row]] = {}
    resolved_path_row_ids: dict[str, list[int]] = {}
    resolved_row_paths: dict[str, str] = {}
    symlink_component_cache: dict[str, bool] = {}
    for index, row in enumerate(row_tuple):
        row_branch = _recorded_branch_short_name(row["branch"])
        if row_branch is not None:
            branch_rows.setdefault(row_branch, (index, row))
        row_path = _normalize_optional_text(row["worktree_path"])
        if row_path is None:
            continue
        literal_row_path = row_path.rstrip("/")
        path_rows.setdefault(literal_row_path, (index, row))
        basename_rows.setdefault(Path(row_path.rstrip("/")).name, []).append((index, row_path, row))
        if _rowless_path_has_symlink_component(row_path, symlink_component_cache):
            resolved_row_path = resolved_row_paths.get(row_path)
            if resolved_row_path is None:
                resolved_row_path = _resolved_path_text(row_path).rstrip("/")
                resolved_row_paths[row_path] = resolved_row_path
            resolved_path_rows.setdefault(resolved_row_path, (index, row))
        else:
            # For an existing canonical path with no symlink component, its
            # literal spelling is also its resolved identity.
            resolved_row_path = row_path.rstrip("/")
            resolved_path_rows.setdefault(resolved_row_path, (index, row))
        resolved_path_row_ids.setdefault(resolved_row_path, []).append(int(row["id"]))
    return _RowlessLaneRowIndex(
        error=None,
        row_path_branches={int(row["id"]): (row["worktree_path"], row["branch"]) for row in row_tuple},
        branch_rows=branch_rows,
        path_rows=path_rows,
        basename_rows=basename_rows,
        resolved_path_rows=resolved_path_rows,
        resolved_path_row_ids=resolved_path_row_ids,
        resolved_row_paths=resolved_row_paths,
        symlink_component_cache=symlink_component_cache,
        resolved_worktree_paths={},
        max_row_id=max((int(row["id"]) for row in row_tuple), default=0),
        identity_seq_available=identity_seq_available,
        max_identity_seq=max_identity_seq,
    )


def _rowless_lane_row_verdict(worktree_path: str, branch: object) -> tuple[bool | None, str]:
    """Return whether no lane row of any status names this path or branch."""
    if _ROWLESS_LANE_ROW_FORCE_FRESH.get():
        return _rowless_lane_row_verdict_fresh(worktree_path, branch)
    index = _ROWLESS_LANE_ROW_INDEX.get() or _load_rowless_lane_row_index()
    if index.error is not None:
        return (None, index.error)

    wanted_branch = _recorded_branch_short_name(branch)
    wanted_path = index.resolved_worktree_paths.get(worktree_path)
    if wanted_path is None:
        wanted_path = _resolved_path_text(worktree_path).rstrip("/")
        index.resolved_worktree_paths[worktree_path] = wanted_path

    earliest_row: tuple[int, sqlite3.Row] | None = None
    if wanted_branch is not None:
        earliest_row = index.branch_rows.get(wanted_branch)
    literal_path_row = index.path_rows.get(worktree_path.rstrip("/"))
    if literal_path_row is not None and (earliest_row is None or literal_path_row[0] < earliest_row[0]):
        earliest_row = literal_path_row
    resolved_path_row = index.resolved_path_rows.get(wanted_path)
    if resolved_path_row is not None and (earliest_row is None or resolved_path_row[0] < earliest_row[0]):
        earliest_row = resolved_path_row

    candidate_rows = {
        (row_index, row_path): row
        for row_index, row_path, row in index.basename_rows.get(Path(worktree_path.rstrip("/")).name, ())
        if earliest_row is None or row_index < earliest_row[0]
    }
    for row_index, row_path in sorted(candidate_rows):
        row = candidate_rows[(row_index, row_path)]
        if (
            _rowless_lane_row_match_kind(
                row,
                wanted_branch=wanted_branch,
                literal_worktree_path=worktree_path,
                resolved_worktree_path=wanted_path,
                resolved_row_paths=index.resolved_row_paths,
            )
            is not None
        ):
            earliest_row = (row_index, row)
            break

    if earliest_row is not None:
        row = earliest_row[1]
        row_branch = _recorded_branch_short_name(row["branch"])
        if wanted_branch is not None and row_branch == wanted_branch:
            return (False, f"lane_row_branch:{row['lane_id']} task={row['task_ref']}")
        return (False, f"lane_row_path:{row['lane_id']} task={row['task_ref']}")
    return (True, "")


def _rowless_lane_row_match_kind(
    row: sqlite3.Row,
    *,
    wanted_branch: str | None,
    literal_worktree_path: str,
    resolved_worktree_path: str,
    resolved_row_paths: dict[str, str] | None = None,
    symlink_component_cache: dict[str, bool] | None = None,
) -> str | None:
    """Apply the rowless branch/path predicate to a prefiltered lane row."""
    row_branch = _recorded_branch_short_name(row["branch"])
    if wanted_branch is not None and row_branch == wanted_branch:
        return "branch"

    row_path = _normalize_optional_text(row["worktree_path"])
    if row_path is None:
        return None
    normalized_row_path = row_path.rstrip("/")
    normalized_worktree_path = literal_worktree_path.rstrip("/")
    if normalized_row_path in {normalized_worktree_path, resolved_worktree_path.rstrip("/")}:
        return "path"

    resolved_row_path = resolved_row_paths.get(row_path) if resolved_row_paths is not None else None
    if resolved_row_path is None:
        path_symlink_cache = symlink_component_cache if symlink_component_cache is not None else {}
        if not _rowless_path_has_symlink_component(row_path, path_symlink_cache):
            return None
        resolved_row_path = _resolved_path_text(row_path).rstrip("/")
        if resolved_row_paths is not None:
            resolved_row_paths[row_path] = resolved_row_path
    if resolved_row_path == resolved_worktree_path:
        return "path"
    return None


def _rowless_lane_row_verdict_fresh(worktree_path: str, branch: object) -> tuple[bool | None, str]:
    """Read plausible current lane rows before authorizing worktree removal."""
    try:
        wanted_branch = _recorded_branch_short_name(branch)
        literal_path = (_normalize_optional_text(worktree_path) or worktree_path).rstrip("/")
        resolved_path = _resolved_path_text(worktree_path).rstrip("/")
        basename = Path(literal_path).name
        index = _ROWLESS_LANE_ROW_INDEX.get()
        if index is not None and index.error is not None:
            return (None, index.error)

        candidate_ids: set[int] = set()
        if index is not None:
            if wanted_branch is not None:
                branch_row = index.branch_rows.get(wanted_branch)
                if branch_row is not None:
                    candidate_ids.add(int(branch_row[1]["id"]))
            path_row = index.path_rows.get(literal_path)
            if path_row is not None:
                candidate_ids.add(int(path_row[1]["id"]))
            resolved_row = index.resolved_path_rows.get(resolved_path)
            if resolved_row is not None:
                candidate_ids.add(int(resolved_row[1]["id"]))
            candidate_ids.update(index.resolved_path_row_ids.get(resolved_path, ()))
            candidate_ids.update(int(row["id"]) for _row_index, _row_path, row in index.basename_rows.get(basename, ()))

        path_expr = "rtrim(trim(worktree_path), '/')"
        predicates = [f"{path_expr} IN (?, ?)"]
        parameters: list[object] = [literal_path, resolved_path]
        if wanted_branch is not None:
            predicates.append("trim(branch) IN (?, ?)")
            parameters.extend((wanted_branch, f"refs/heads/{wanted_branch}"))

        with _get_db_connection() as conn:
            _ensure_rowless_fresh_query_indexes(conn)
            if index is not None and index.identity_seq_available:
                direct_and_changed_rows = conn.execute(
                    "SELECT id, lane_id, task_ref, worktree_path, branch, "
                    "1 AS _rowless_direct, 0 AS _rowless_changed FROM ("
                    "SELECT id, lane_id, task_ref, worktree_path, branch FROM worktree_lanes "
                    f"WHERE ({' OR '.join(predicates)}) ORDER BY id) "
                    "UNION ALL "
                    "SELECT id, lane_id, task_ref, worktree_path, branch, "
                    "0 AS _rowless_direct, 1 AS _rowless_changed FROM ("
                    "SELECT id, lane_id, task_ref, worktree_path, branch FROM worktree_lanes "
                    "WHERE identity_seq > ? ORDER BY identity_seq LIMIT ?)",
                    [
                        *parameters,
                        index.max_identity_seq,
                        _ROWLESS_FRESH_INSERT_FALLBACK_LIMIT + 1,
                    ],
                ).fetchall()
                rows = [row for row in direct_and_changed_rows if row["_rowless_direct"]]
                delta_rows = [row for row in direct_and_changed_rows if row["_rowless_changed"]]
            else:
                rows = conn.execute(
                    "SELECT id, lane_id, task_ref, worktree_path, branch FROM worktree_lanes "
                    f"WHERE ({' OR '.join(predicates)}) ORDER BY id",
                    parameters,
                ).fetchall()
                delta_rows = []
            candidate_rows: list[sqlite3.Row] = []
            candidate_id_values = sorted(candidate_ids)
            for chunk_start in range(0, len(candidate_id_values), 800):
                chunk = candidate_id_values[chunk_start : chunk_start + 800]
                placeholders = ", ".join("?" for _ in chunk)
                candidate_rows.extend(
                    conn.execute(
                        "SELECT id, lane_id, task_ref, worktree_path, branch FROM worktree_lanes "
                        f"WHERE id IN ({placeholders}) ORDER BY id",
                        chunk,
                    ).fetchall()
                )
            # Preserve the append-only fallback for rows whose ids exceed the
            # snapshot high-water mark. The sequence delta also catches new
            # identities whose ids were reused below it.
            recent_rows = conn.execute(
                "SELECT id, lane_id, task_ref, worktree_path, branch FROM worktree_lanes "
                "WHERE id > ? ORDER BY id LIMIT ?",
                (
                    index.max_row_id if index is not None else 0,
                    _ROWLESS_FRESH_INSERT_FALLBACK_LIMIT + 1,
                ),
            ).fetchall()
            if index is None:
                # With no snapshot every row is potentially changed. Keep the
                # bounded fallback and refuse if the complete set will not fit.
                delta_rows: list[sqlite3.Row] = []
            elif not index.identity_seq_available:
                # Older schemas have no database-owned identity cursor. Keep
                # the exact snapshot diff for those databases rather than
                # skipping a change check when readers and DB upgrades skew.
                conn.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS _rowless_lane_row_snapshot "
                    "(id INTEGER PRIMARY KEY, worktree_path, branch)"
                )
                conn.execute("DELETE FROM _rowless_lane_row_snapshot")
                conn.executemany(
                    "INSERT INTO _rowless_lane_row_snapshot (id, worktree_path, branch) VALUES (?, ?, ?)",
                    [(row_id, path, row_branch) for row_id, (path, row_branch) in index.row_path_branches.items()],
                )
                delta_rows = conn.execute(
                    "WITH changed AS ("
                    "SELECT current.id, current.lane_id, current.task_ref, "
                    "current.worktree_path, current.branch "
                    "FROM worktree_lanes AS current "
                    "LEFT JOIN _rowless_lane_row_snapshot AS snapshot ON snapshot.id = current.id "
                    "WHERE snapshot.id IS NULL "
                    "OR current.worktree_path IS NOT snapshot.worktree_path "
                    "OR current.branch IS NOT snapshot.branch) "
                    "SELECT id, lane_id, task_ref, worktree_path, branch FROM changed "
                    "ORDER BY id LIMIT ?",
                    (_ROWLESS_FRESH_INSERT_FALLBACK_LIMIT + 1,),
                ).fetchall()

        changed_rows_by_id = {int(row["id"]): row for row in (*recent_rows, *delta_rows)}
        changed_rows = [changed_rows_by_id[row_id] for row_id in sorted(changed_rows_by_id)]
        changed_rows_overflow = (
            len(recent_rows) > _ROWLESS_FRESH_INSERT_FALLBACK_LIMIT
            or len(delta_rows) > _ROWLESS_FRESH_INSERT_FALLBACK_LIMIT
            or len(changed_rows) > _ROWLESS_FRESH_INSERT_FALLBACK_LIMIT
        )
        rows_by_id = {
            int(row["id"]): row
            for row in (*rows, *candidate_rows, *changed_rows[:_ROWLESS_FRESH_INSERT_FALLBACK_LIMIT])
        }
        symlink_component_cache: dict[str, bool] = {}

        for row in (rows_by_id[row_id] for row_id in sorted(rows_by_id)):
            row_branch = _recorded_branch_short_name(row["branch"])
            if wanted_branch is not None and row_branch == wanted_branch:
                return (False, f"lane_row_branch:{row['lane_id']} task={row['task_ref']}")

            row_path = _normalize_optional_text(row["worktree_path"])
            if row_path is None:
                continue
            normalized_row_path = row_path.rstrip("/")
            if normalized_row_path in {literal_path, resolved_path}:
                return (False, f"lane_row_path:{row['lane_id']} task={row['task_ref']}")
            if not _rowless_path_has_symlink_component(row_path, symlink_component_cache):
                continue
            resolved_row_path = _resolved_path_text(row_path).rstrip("/")
            if resolved_row_path == resolved_path:
                return (False, f"lane_row_path:{row['lane_id']} task={row['task_ref']}")
        if changed_rows_overflow:
            if index is not None and any(int(row["id"]) in index.row_path_branches for row in changed_rows):
                return (None, "rowless_row_probe_failed:changed_row_fallback_exceeded")
            return (None, "rowless_row_probe_failed:recent_row_fallback_exceeded")
    except Exception as exc:  # noqa: BLE001 — rowlessness is an authorization gate
        return (None, f"rowless_row_probe_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    return (True, "")


def _removal_time_rowless_lane_row_verdict(worktree_path: str, branch: object) -> tuple[bool | None, str]:
    """Use the existing verdict seam while forcing a fresh authority read."""
    token = _ROWLESS_LANE_ROW_FORCE_FRESH.set(True)
    try:
        return _rowless_lane_row_verdict(worktree_path, branch)
    finally:
        _ROWLESS_LANE_ROW_FORCE_FRESH.reset(token)


def _rowless_worktree_landed_verdict(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    integration_ref: str,
) -> tuple[bool | None, str]:
    """Prove the attached worktree tip is wholly contained in integration."""
    name = _recorded_branch_short_name(branch)
    if name is None:
        return (False, "rowless_branch_unreadable")
    attached = _worktree_head_is_recorded_branch(worktree_path, name)
    if attached is not True:
        return (False, "rowless_worktree_head_mismatch")
    branch_tip = _reclaim_rev_parse(repo_root, f"refs/heads/{name}")
    worktree_tip = _reclaim_rev_parse(Path(worktree_path), "HEAD")
    integration_tip = _reclaim_rev_parse(repo_root, integration_ref)
    if not branch_tip or not worktree_tip or not integration_tip:
        return (None, "rowless_commit_probe_unknown")
    if branch_tip != worktree_tip:
        return (False, "rowless_branch_tip_changed")
    ancestor = _branch_merged_into_integration(repo_root, name, integration_ref)
    if ancestor is None:
        return (None, "rowless_ancestry_unknown")
    if not ancestor:
        return (False, "rowless_unique_commits")
    ahead = _reclaim_rev_list_count(repo_root, integration_tip, worktree_tip)
    if ahead is None:
        return (None, "rowless_commit_count_unknown")
    if ahead:
        return (False, "rowless_unique_commits")
    return (True, "rowless_work_complete")


def _rowless_reap_grace_seconds() -> float:
    raw = os.environ.get("WORKBAY_ROWLESS_REAP_GRACE_S", "3600").strip()
    try:
        value = float(raw)
    except ValueError:
        return 3600.0
    if value < 0 or value != value:
        return 3600.0
    return value


def _terminal_unmerged_disposition_allows_reclaim(
    *,
    repo_root: Path,
    branch: object,
    integration_ref: str,
) -> bool:
    """Use the canonical disposition table for terminal unmerged branches.

    The kind policy is ``terminal_dispose_authorized``, the same predicate the
    branch-delete gate applies, so a lane is never reclaimed here and refused there.
    """
    normalized_branch = _normalize_optional_text(branch)
    if normalized_branch is None:
        return False
    try:
        from .orchestration import lane_disposition  # noqa: PLC0415
        from .orchestration.branch_reclaim_delete import terminal_dispose_authorized  # noqa: PLC0415

        snapshot = lane_disposition.collect_snapshot(
            repo_root,
            normalized_branch,
            integration_ref=integration_ref,
        )
        # The attached worktree is the object being classified for retirement;
        # it must not trigger lane_disposition's LIVE rule.
        disposition = lane_disposition.decide(replace(snapshot, worktree_paths=()))
        authorized, _reason = terminal_dispose_authorized(disposition)
        return authorized
    except Exception:  # noqa: BLE001 — unknown evidence cannot authorize removal
        return False


_CLOSED_LANE_STATUSES = frozenset({"closed", "closed_stale"})
_CLOSE_AUTHORITY_IDENTITY_KEYS = ("id", "identity_seq", "branch", "worktree_path", "owner_agent")
_CLOSE_AUTHORITY_SNAPSHOT_KEYS = (
    "id",
    "status",
    "updated_at",
    "notes",
    "worktree_path",
    "branch",
    "lane_kind",
    "branch_tip_sha",
    "owner_agent",
)


def _parse_close_authority(notes: object) -> tuple[dict[str, Any] | None, bool]:
    """Return ``(authority, malformed)`` from a row's note envelope.

    ``(None, False)`` is a legacy row closed before close authority was
    recorded. ``malformed`` is True when an authority is present but is not
    the schema-1 shape this owner can verify; that is never read as legacy.
    """
    try:
        envelope = json.loads(notes or "null")
    except (TypeError, ValueError):
        return (None, '"close_authority"' in str(notes or ""))
    if not isinstance(envelope, dict) or "close_authority" not in envelope:
        return (None, False)
    authority = envelope["close_authority"]
    if not isinstance(authority, dict) or authority.get("schema") != 1:
        return (None, True)
    tip = authority.get("authorized_tip")
    identity = authority.get("closed_identity")
    snapshot = authority.get("closed_snapshot")
    if (
        not isinstance(tip, str)
        or re.fullmatch(r"[0-9a-f]{40}", tip) is None
        or not isinstance(identity, dict)
        or not isinstance(snapshot, dict)
        or any(key not in identity for key in _CLOSE_AUTHORITY_IDENTITY_KEYS)
        or type(identity.get("id")) is not int
        or any(key not in snapshot for key in _CLOSE_AUTHORITY_SNAPSHOT_KEYS)
    ):
        return (None, True)
    return (authority, False)


def _close_authority_row_refusal(row: Mapping[str, Any] | None, authority: Mapping[str, Any]) -> str:
    """``row_changed_after_close`` unless ``row`` is still the row closed under ``authority``.

    Identity is ``id`` plus ``identity_seq``: a same-second update and a
    delete-and-recreate under the same lane id both move it, while
    ``updated_at`` (seconds precision) cannot. A schema without
    ``identity_seq`` falls back to the exact persisted note bytes of the
    closing write.
    """
    if row is None:
        return "row_changed_after_close"
    identity = authority["closed_identity"]
    snapshot = authority["closed_snapshot"]
    changed = (
        any(row.get(key) != value for key, value in identity.items())
        or row.get("status") not in _CLOSED_LANE_STATUSES
        or row.get("status") != snapshot.get("status")
        or row.get("branch_tip_sha") != authority["authorized_tip"]
    )
    if not changed and identity.get("identity_seq") is None:
        try:
            expected_notes = json.dumps({**json.loads(snapshot["notes"]), "close_authority": authority}, sort_keys=True)
        except (KeyError, TypeError, ValueError):
            return "row_changed_after_close"
        changed = any(row.get(key) != (expected_notes if key == "notes" else value) for key, value in snapshot.items())
    elif not changed:
        changed = _parse_close_authority(row.get("notes"))[0] != authority
    return "row_changed_after_close" if changed else ""


def _read_durable_lane_row(*, task_ref: object, lane_id: str) -> tuple[dict[str, Any] | None, bool]:
    """Return ``(row, ambiguous)`` for the newest durable row of ``lane_id``.

    Without a task, the newest row may belong to another task that reused the
    lane id. ``ambiguous`` is True when the lane id names more than one task, so
    no single row can stand for the caller's lane.
    """
    task = _normalize_optional_text(task_ref)
    with _get_db_connection() as conn:
        if task is None:
            task_count = conn.execute(
                "SELECT COUNT(DISTINCT task_ref) FROM worktree_lanes WHERE lane_id = ?", (lane_id,)
            ).fetchone()[0]
            row = conn.execute(
                "SELECT * FROM worktree_lanes WHERE lane_id = ? ORDER BY id DESC LIMIT 1", (lane_id,)
            ).fetchone()
            ambiguous = task_count > 1
        else:
            row = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? ORDER BY id DESC LIMIT 1",
                (task, lane_id),
            ).fetchone()
            ambiguous = False
    return (None if row is None else dict(row), ambiguous)


def _durable_close_authority(
    *,
    task_ref: object,
    lane_id: object,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    """Read the durable row for ``lane_id`` as ``(row, authority, refusal)``.

    ``authority`` is None for a rowless lane and for a legacy row. A non-empty
    ``refusal`` is the typed outcome that must stop removal: the authority is
    malformed, the row no longer matches it, the row cannot be read, or a
    task-less lookup names a lane id shared by more than one task.
    """
    lane = _normalize_optional_text(lane_id)
    if lane is None:
        return (None, None, "")
    try:
        row, ambiguous = _read_durable_lane_row(task_ref=task_ref, lane_id=lane)
    except Exception:  # noqa: BLE001 — an unreadable row never authorizes removal
        return (None, None, "lane_row_read_failed")
    if ambiguous:
        return (None, None, "lane_identity_ambiguous")
    if row is None:
        return (None, None, "")
    authority, malformed = _parse_close_authority(row.get("notes"))
    if malformed:
        return (row, None, "authority_mismatch")
    if authority is None:
        return (row, None, "")
    return (row, authority, _close_authority_row_refusal(row, authority))


_T = TypeVar("_T")

#: Fence lock files live in this directory beside the handoff database.
_RECLAIM_FENCE_LOCK_DIR = "reclaim-fence-locks"
#: A holder record of this kind held ``LOCK_EX`` on the inode its ``lock_dev``/``lock_ino`` name, from
#: before its claim until after its release, through a descriptor every spawned mutation inherits.
_RECLAIM_FENCE_LOCK_KIND = "flock-v1"
_RECLAIM_FENCE_RELEASE_ATTEMPTS = 3
_RECLAIM_FENCE_RELEASE_BACKOFF_S = 0.25
#: One monotonic budget for the whole release, SQLite lock waits included.
_RECLAIM_FENCE_RELEASE_BUDGET_S = 10.0
#: Descriptors of every fence lock this context holds, innermost last. Each mutation spawned through
#: ``_run_reclaim_mutation`` inherits all of them. A contextvar does not cross into a new thread, so an
#: actuator must spawn its mutations on the thread that runs it.
_ACTIVE_RECLAIM_FENCE_LOCK_FDS: contextvars.ContextVar[tuple[int, ...]] = contextvars.ContextVar(
    "_ACTIVE_RECLAIM_FENCE_LOCK_FDS", default=()
)
#: The thread running the innermost fence's actuator; a fenced mutation spawns only on it.
_ACTIVE_RECLAIM_FENCE_THREAD: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_ACTIVE_RECLAIM_FENCE_THREAD", default=None
)


@dataclass(frozen=True)
class _ReclaimFenceLock:
    """``LOCK_EX`` held through ``descriptor`` on the inode ``(device, inode)`` that ``path`` names."""

    path: str
    device: int
    inode: int
    descriptor: int


def _reclaim_fence_lock_path(task_ref: str, lane_id: str) -> Path:
    """The row's fence lock file, in a directory beside the handoff database.

    The database is the one ``_get_db_connection`` opens, ``get_runtime_config().db_path``
    (``workbay_handoff_mcp.shared_schema._open_db_connection``). The path is resolved, so every process
    on the same database names the same file in its holder record.
    """
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

    digest = hashlib.sha256(f"{task_ref}\0{lane_id}".encode()).hexdigest()[:32]
    database_dir = Path(get_runtime_config().db_path).expanduser().resolve().parent
    return database_dir / _RECLAIM_FENCE_LOCK_DIR / f"{digest}.lock"


def _acquire_reclaim_fence_lock(path: Path) -> tuple[str, _ReclaimFenceLock | None]:
    """Take ``LOCK_EX`` on the fence lock file without blocking. Returns ``(refusal, lock)``.

    The file is opened as ``_recover_stale_ref_lock_serialized`` opens its lock: no symlink, mode 0600,
    a regular file, in a 0700 directory that is not a symlink. A busy lock is ``reclaim_fence_held``. Any
    other failure, or a path that no longer names the locked inode once the lock is taken, is
    ``reclaim_fence_unavailable``. The file is never unlinked: [RES-07] exemption, one empty file per
    (task, lane) pair that was ever fenced.
    """
    descriptor = -1
    try:
        path.parent.mkdir(mode=0o700, exist_ok=True)
        if not stat.S_ISDIR(os.lstat(path.parent).st_mode):
            _logger.warning("reclaim fence lock directory %s is not a directory", path.parent)
            return ("reclaim_fence_unavailable", None)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            _logger.warning("reclaim fence lock %s is not a regular file", path)
            return ("reclaim_fence_unavailable", None)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                return ("reclaim_fence_held", None)
            raise
        opened, named = os.fstat(descriptor), os.lstat(path)
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            _logger.warning("reclaim fence lock %s was replaced while it was being locked", path)
            return ("reclaim_fence_unavailable", None)
        locked = _ReclaimFenceLock(path=str(path), device=opened.st_dev, inode=opened.st_ino, descriptor=descriptor)
        descriptor = -1
        return ("", locked)
    except OSError:
        _logger.warning("reclaim fence lock %s is unavailable", path, exc_info=True)
        return ("reclaim_fence_unavailable", None)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _close_reclaim_fence_lock(descriptor: int) -> None:
    """Close our lock descriptor without ``LOCK_UN``.

    ``LOCK_UN`` would drop the lock for every process that shares the open file description, including
    a spawned process that is still running. Closing drops only our reference; the kernel releases the
    lock when the last process holding it exits or closes it.
    """
    try:
        os.close(descriptor)
    except OSError:
        _logger.warning("reclaim fence lock descriptor %d did not close", descriptor, exc_info=True)


def _reclaim_fence_recoverable(fence: str, *, held: _ReclaimFenceLock) -> bool:
    """Whether this claimer may take over ``fence``: only on proof that its holder and children are gone.

    The proof is the kernel lock. A ``flock-v1`` holder on this host whose lock is the very inode this
    claimer now holds ``LOCK_EX`` on held that lock, through a descriptor every process it spawned
    inherited, until it and they exited or closed it; holding it now proves none of them still does
    [RES-10]. Any other record (unparseable, legacy, another host, another path or another inode) is
    not recoverable and refuses the claim [SECD-05]. No pid, age, deadline or timeout is consulted.
    """
    try:
        holder: object = json.loads(fence)
    except (ValueError, RecursionError):
        return False
    return (
        isinstance(holder, dict)
        and holder.get("lock_kind") == _RECLAIM_FENCE_LOCK_KIND
        and holder.get("host") == socket.gethostname()
        and holder.get("lock") == held.path
        and type(holder.get("lock_dev")) is int
        and type(holder.get("lock_ino")) is int
        and (holder["lock_dev"], holder["lock_ino"]) == (held.device, held.inode)
    )


def _claim_reclaim_fence(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
    authority: Mapping[str, Any],
    holder_json: str,
    held: _ReclaimFenceLock,
) -> tuple[str, int | None]:
    """Set the row's fence if the row still matches ``authority``. Runs inside the caller's transaction.

    Returns ``(refusal, row_id)``; ``refusal`` is ``""`` once the fence is set.
    """
    # Prove both guard triggers, with their exact bodies, in this claim's own transaction [SECD-03].
    try:
        from workbay_handoff_mcp.shared_schema import unproven_reclaim_fence_guards  # noqa: PLC0415
    except ImportError:
        _logger.warning(
            "reclaim fence guards cannot be proven (no guard proof in the handoff schema); "
            "not removing lane %s (task %s)",
            lane_id,
            task_ref,
            exc_info=True,
        )
        return ("reclaim_fence_unavailable", None)
    unproven = unproven_reclaim_fence_guards(conn)
    if unproven:
        _logger.warning(
            "reclaim fence trigger %s missing or not as shipped; not removing lane %s (task %s)",
            ", ".join(unproven),
            lane_id,
            task_ref,
        )
        return ("reclaim_fence_unavailable", None)
    found = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? ORDER BY id DESC LIMIT 1",
        (task_ref, lane_id),
    ).fetchone()
    if found is None:
        return ("row_changed_after_close", None)
    row = dict(found)
    refusal = _close_authority_row_refusal(row, authority)
    if refusal:
        return (refusal, None)
    observed = row.get("reclaim_fence")
    if observed is not None:
        if not _reclaim_fence_recoverable(str(observed), held=held):
            return ("reclaim_fence_held", None)
        _logger.warning(
            "reclaim_fence_recovered: lane %s (task %s) fence lock %s is held by this claimer; previous holder: %s",
            lane_id,
            task_ref,
            held.path,
            observed,
        )
    cursor = conn.execute(
        "UPDATE worktree_lanes SET reclaim_fence = ? WHERE id = ? AND reclaim_fence IS ?",
        (holder_json, row["id"], observed),
    )
    if cursor.rowcount != 1:
        return ("reclaim_fence_held", None)
    return ("", int(row["id"]))


def _release_reclaim_fence(
    *, row_id: int, holder_json: str, lane_id: str, held: _ReclaimFenceLock, no_work_epoch: int | None = None
) -> str:
    """Clear our own fence once nothing spawned under it can still mutate. Returns ``released``,
    ``not_ours`` or ``release_failed``; never raises.

    For a successful no-work removal, ``no_work_epoch`` also sets status to
    planned and appends the cycle marker in that same fence-clearing UPDATE.
    Failed actuators pass None and only clear the fence.

    The caller has closed its own lock descriptor first. Every attempt then takes ``LOCK_EX`` afresh,
    through a new open file description of the inode ``held`` names, before its BEGIN IMMEDIATE
    [CON-13]. Getting that lock is the same proof a recovering claimer relies on: no process that
    inherited our descriptor still holds it. The actuator returning, raising or timing out is not.
    While the lock is still held, or the path no longer names that inode, the row is not touched.

    A failed attempt is retried after a fixed back-off, at most three attempts within one monotonic
    budget that bounds the SQLite lock waits too; no SQLite lock is held while waiting. ``not_ours``
    means our exact holder record was no longer in place. ``release_failed`` leaves the row fenced
    with our record, so the schema triggers keep refusing other writers, until the next claimer that
    holds the row's fence lock recovers it [RES-19].
    """
    deadline = time.monotonic() + _RECLAIM_FENCE_RELEASE_BUDGET_S
    for attempt in range(1, _RECLAIM_FENCE_RELEASE_ATTEMPTS + 1):
        if attempt > 1:
            if time.monotonic() + _RECLAIM_FENCE_RELEASE_BACKOFF_S >= deadline:
                break
            time.sleep(_RECLAIM_FENCE_RELEASE_BACKOFF_S)
        lock_refusal, proof = _acquire_reclaim_fence_lock(Path(held.path))
        if proof is None:
            _logger.warning(
                "reclaim fence release attempt %d/%d for lane %s: fence lock %s (%s); a process spawned "
                "under the fence may still hold it, so the row stays fenced",
                attempt,
                _RECLAIM_FENCE_RELEASE_ATTEMPTS,
                lane_id,
                held.path,
                lock_refusal,
            )
            continue
        try:
            if (proof.device, proof.inode) != (held.device, held.inode):
                _logger.warning(
                    "reclaim fence lock %s was replaced after lane %s was fenced; the row stays fenced",
                    held.path,
                    lane_id,
                )
                break
            try:
                with _get_db_connection(begin_immediate=True, deadline=deadline) as conn:
                    if no_work_epoch is None:
                        released = conn.execute(
                            "UPDATE worktree_lanes SET reclaim_fence = NULL WHERE id = ? AND reclaim_fence = ?",
                            (row_id, holder_json),
                        ).rowcount
                    else:
                        released = conn.execute(
                            "UPDATE worktree_lanes SET reclaim_fence=NULL, status='planned', "
                            "notes=COALESCE(notes,'') || ?, updated_at=datetime('now') "
                            "WHERE id=? AND reclaim_fence=?",
                            (f" [released_no_work cycle={no_work_epoch}]", row_id, holder_json),
                        ).rowcount
            except Exception:  # noqa: BLE001 — retried, then reported as release_failed
                _logger.warning(
                    "reclaim fence release attempt %d/%d failed for lane %s",
                    attempt,
                    _RECLAIM_FENCE_RELEASE_ATTEMPTS,
                    lane_id,
                    exc_info=True,
                )
                continue
        finally:
            _close_reclaim_fence_lock(proof.descriptor)
        if released != 1:
            _logger.warning("reclaim fence for lane %s was not ours at release; left to its holder", lane_id)
            return "not_ours"
        return "released"
    _logger.warning(
        "reclaim_fence_release_failed: lane %s stays fenced until a claimer holding its fence lock recovers it",
        lane_id,
    )
    return "release_failed"


def _run_under_reclaim_fence(
    *, task_ref: str, lane_id: str, authority: Mapping[str, Any], actuator: Callable[[], _T]
) -> tuple[str, _T | None, str]:
    """Run ``actuator`` while a kernel lock and the durable reclaim fence pin the closed row.

    Protection model [SECD-01]:

    1. The row's ``reclaim_fence`` column holds our holder record, and our token is in it.
    2. Both guard triggers are present with their exact bodies, proved inside the claim's transaction.
    3. This process holds ``LOCK_EX`` on the row's fence lock file from before the claim until after
       the release.
    4. Every process the actuator spawns through ``_run_reclaim_mutation`` inherits that lock, so the
       lock stays held until the holder and every spawned process have exited or closed it.
    5. Only a claimer that itself holds the lock may take over an existing fence, and only a record
       naming the inode it holds. Holding it is positive evidence that no earlier holder, and no
       process spawned under it, still holds it.
    6. The release state (``released``, ``not_ours`` or ``release_failed``) reaches every caller.

    7. The release clears the row only after this process has closed its own descriptor and taken
       the lock again through a new open file description; until then the row stays fenced and the
       schema triggers keep refusing every other writer. The actuator returning, raising or timing
       out proves nothing about what it spawned.
    8. Item 4 covers only a consumer that keeps the inherited descriptor while it can mutate. Under
       a fence ``_run_reclaim_mutation`` runs only the supported git mutations, with every
       configured handoff that could detach (hooks outside a verified guard directory, fsmonitor,
       automatic gc and maintenance) pinned off, and only on this actuator's thread; any other
       consumer is refused before it runs (``_fenced_mutation_custody``).

    The model's limit: an actuator that spawns a process by other means than
    ``_run_reclaim_mutation`` gives it no lock and no custody. Every actuator in this package
    mutates in process or through ``_run_reclaim_mutation``; a new one must do the same.

    Lock order [CON-13]: the fence lock file first, then the handoff database write transaction;
    never the reverse. The lock is taken with ``LOCK_NB`` and never waits, and no clock, pid or
    subprocess timeout decides whether a holder is gone [RES-10]. A stopped holder keeps its lock, and
    so does a spawned process that outlived its timeout or its holder.

    The claim is one short BEGIN IMMEDIATE transaction that proves the guards, re-reads the row,
    refuses unless it still matches ``authority``, and sets ``reclaim_fence``. It commits before any
    git command runs, so the handoff write lock is never held across the actuator (repo barrier
    ``assert_no_write_lock_held``; [CON-18] when a git hook would need the lock). While the fence is
    set, the schema triggers refuse every other write to the row [CON-11]. The release clears our
    exact holder record, also when the actuator raises, and our lock descriptor is closed either
    way.

    Returns ``(refusal, result, release)``. ``actuator`` runs only when ``refusal`` is ``""``, and
    ``result`` is then its return value unchanged. ``release`` is ``""`` when no fence was claimed.
    ``release_failed`` is never folded into ``refusal``: the actuator ran and the row stays fenced
    [AGT-10]. When the actuator raises, the exception propagates unchanged and carries the release
    state as ``reclaim_fence_release`` (``reclaim_fence_release_of``).
    """
    _assert_no_reaper_write_lock(reason="fcntl.flock")
    try:
        lock_path = _reclaim_fence_lock_path(task_ref, lane_id)
    except Exception:  # noqa: BLE001 — an unknown lock location never authorizes removal
        _logger.warning(
            "reclaim fence lock path unavailable; not removing lane %s (task %s)", lane_id, task_ref, exc_info=True
        )
        return ("reclaim_fence_unavailable", None, "")
    refusal, held = _acquire_reclaim_fence_lock(lock_path)
    if held is None:
        return (refusal or "reclaim_fence_unavailable", None, "")
    fds_token = _ACTIVE_RECLAIM_FENCE_LOCK_FDS.set((*_ACTIVE_RECLAIM_FENCE_LOCK_FDS.get(), held.descriptor))
    thread_token = _ACTIVE_RECLAIM_FENCE_THREAD.set(threading.get_ident())
    relinquished = False

    def relinquish() -> None:
        """Stop handing our descriptor to new mutations and close it, once."""
        nonlocal relinquished
        if not relinquished:
            relinquished = True
            _ACTIVE_RECLAIM_FENCE_THREAD.reset(thread_token)
            _ACTIVE_RECLAIM_FENCE_LOCK_FDS.reset(fds_token)
            _close_reclaim_fence_lock(held.descriptor)

    try:
        holder_json = json.dumps(
            {
                "token": uuid.uuid4().hex,
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "lane_id": lane_id,
                "task_ref": task_ref,
                "lock": held.path,
                "lock_dev": held.device,
                "lock_ino": held.inode,
                "lock_kind": _RECLAIM_FENCE_LOCK_KIND,
            },
            sort_keys=True,
        )
        try:
            with _get_db_connection(begin_immediate=True) as conn:
                refusal, row_id = _claim_reclaim_fence(
                    conn, task_ref=task_ref, lane_id=lane_id, authority=authority, holder_json=holder_json, held=held
                )
        except Exception:  # noqa: BLE001 — an unproven fence never authorizes removal
            _logger.warning(
                "reclaim fence claim failed; not removing lane %s (task %s)", lane_id, task_ref, exc_info=True
            )
            return ("reclaim_fence_unavailable", None, "")
        if refusal or row_id is None:
            return (refusal or "reclaim_fence_unavailable", None, "")
        try:
            result = actuator()
        except BaseException as exc:
            relinquish()
            release = _release_reclaim_fence(row_id=row_id, holder_json=holder_json, lane_id=lane_id, held=held)
            _attach_reclaim_fence_release(exc, release)
            raise
        relinquish()
        release = _release_reclaim_fence(row_id=row_id, holder_json=holder_json, lane_id=lane_id, held=held)
        return ("", result, release)
    finally:
        relinquish()


def _attach_reclaim_fence_release(exc: BaseException, release: str) -> None:
    """Record ``release`` on an exception that escaped a fenced actuator; the innermost fence wins."""
    if getattr(exc, "reclaim_fence_release", None) is None:
        try:
            exc.reclaim_fence_release = release  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            _logger.warning("reclaim fence release %s could not be attached to %r", release, exc)


def reclaim_fence_release_of(exc: BaseException) -> str:
    """The release state a fenced actuator's escaped exception carries, ``""`` if none."""
    release = getattr(exc, "reclaim_fence_release", None)
    return release if isinstance(release, str) else ""


def _branch_delete_under_authority(
    close_authority: Mapping[str, Any] | None,
    *,
    task_ref: str | None,
    lane_id: str | None,
    actuator: Callable[[], subprocess.CompletedProcess[str] | None],
) -> tuple[str, subprocess.CompletedProcess[str] | None, str]:
    """Run the guarded branch delete under the row's reclaim fence; unfenced without close authority.

    Returns ``(refusal, proc, release)`` as ``_run_under_reclaim_fence`` does; ``release`` is ``""`` unfenced.
    """
    if close_authority is None:
        return ("", actuator(), "")
    if not task_ref or not lane_id:
        return ("reclaim_fence_unavailable", None, "")
    return _run_under_reclaim_fence(task_ref=task_ref, lane_id=lane_id, authority=close_authority, actuator=actuator)


def _reap_fence_release(bundle_record: Mapping[str, object]) -> dict[str, object]:
    """The fenced branch delete's release state, carried onto its deleted-branch record [AGT-10]."""
    return {key: bundle_record[key] for key in ("reap_fence_release",) if key in bundle_record}


#: Prefix of the detail a fenced removal returns beside its success when its fence was not released.
_REAP_FENCE_RELEASE_DETAIL = "reap_fence_release:"


def _with_fence_release_detail(detail: str, remove_detail: str) -> str:
    """Retain the removal's receipt while translating its detail, keeping typed cause bytes [AGT-10]."""
    from .orchestration.branch_reclaim_delete import FenceReleaseDetail, fence_release_of  # noqa: PLC0415

    if isinstance(remove_detail, FenceReleaseDetail):
        return FenceReleaseDetail(detail, fence_release_of(remove_detail))
    if remove_detail.startswith(_REAP_FENCE_RELEASE_DETAIL):
        return f"{detail};{remove_detail}" if detail else remove_detail
    return detail


def _worktree_reclaim_release_fields(detail: str) -> dict[str, object]:
    """Extract a receipt before a public reaper record serializes its string-valued cause."""
    from .orchestration.branch_reclaim_delete import fence_release_of  # noqa: PLC0415

    release = fence_release_of(detail)
    if release in {"not_ours", "release_failed"}:
        return {
            "fence_release": [{"step": "worktree_reclaim", "release": release}],
            "cleanup_pending": "reclaim_fence_release",
        }
    return {}


def _branch_tip_refusal(*, repo_root: Path, branch: object, expected_tip: str) -> str:
    """``""`` while ``refs/heads/<branch>`` is still ``expected_tip``, else the refusal outcome."""
    tip_probe = _run_reclaim_command(
        [
            "git",
            "-C",
            str(repo_root),
            "rev-parse",
            "--verify",
            f"refs/heads/{str(branch).removeprefix('refs/heads/')}^{{commit}}",
        ]
    )
    if tip_probe is None or tip_probe.returncode != 0:
        return "branch_probe_failed"
    return "" if _probe_command_stdout(tip_probe).strip() == expected_tip else "tip_moved_after_close"


def _branch_ref_state(repo_root: Path | str, branch: object) -> tuple[str, str]:
    """``("present", sha)``, ``("absent", "")`` or ``("unknown", detail)`` for ``refs/heads/<branch>``.

    ``--quiet`` makes a missing ref exit 1 with no output, which is not a probe
    failure: it is a ref that is gone, so the caller must not read it as a tip.
    """
    name = _normalize_optional_text(branch)
    if name is None:
        return ("unknown", "branch_missing")
    ref = name[len("refs/heads/") :] if name.startswith("refs/heads/") else name
    proc = _run_reclaim_command(
        ["git", "-C", str(repo_root), "rev-parse", "--verify", "--quiet", f"refs/heads/{ref}^{{commit}}"]
    )
    if proc is None:
        return ("unknown", "branch_probe_failed")
    if proc.returncode == 1:
        return ("absent", "")
    sha = _probe_command_stdout(proc).strip()
    if proc.returncode != 0 or not sha:
        return ("unknown", "branch_probe_failed")
    return ("present", sha)


def _reopen_moved_closed_row(row: Mapping[str, Any]) -> str:
    """Reopen a closed row whose authorized tip moved to ``blocked``, with a ``moved_after_close`` event.

    Returns ``""`` when the row reopened and its event was recorded, else the
    detail naming what failed. A refused reopen (the status rules may forbid
    closed to blocked) leaves the row closed; the typed event is still recorded
    and every later retire refuses on the tip.
    """
    from .orchestration.lane_retire_sweep import _close_lane_cas  # noqa: PLC0415

    reopened = _close_lane_cas(dict(row), target_status="blocked", note="moved_after_close")
    if reopened.get("ok") is not True:
        return f"moved_after_close:reopen_failed:{reopened.get('reason')}"[:_RECLAIM_DETAIL_CAP]
    return str(reopened.get("blocker_event") or "")[:_RECLAIM_DETAIL_CAP]


def _reopen_tip_moved_at_removal(
    *,
    task_ref: str | None,
    lane_id: str | None,
    authority: Mapping[str, Any] | None,
    locked_row: Mapping[str, Any] | None,
) -> str:
    """Reopen the row for a tip move seen at removal time, and return the reopen detail.

    An authority first written after the locked read is visible only to a fresh
    read, so that read decides first. The locked sample is the fallback.
    """
    row, current, refusal = _durable_close_authority(task_ref=task_ref, lane_id=lane_id)
    if row is not None and current is not None and not refusal:
        return _reopen_moved_closed_row(row)
    if authority is not None and locked_row is not None:
        return _reopen_moved_closed_row(locked_row)
    return ""


def _reclaim_lane_worktree(
    *,
    worktree_path: object,
    branch: object,
    repo_root: Path | str | None,
    apply: bool,
    owner_probe: Callable[[object], tuple[str, str]] | None = None,
    integration_ref: str = _RECLAIM_INTEGRATION_REF,
    task_ref: str | None = None,
    lane_id: str | None = None,
    held_lock: Any | None = None,
    expected_tip: str | None = None,
    allow_terminal_never_started: bool = False,
    allow_terminal_unmerged_bundled: bool = False,
    allow_rowless_work_complete: bool = False,
    trusted_landing: Mapping[str, object] | None = None,
    landing_lock_held: bool = False,
    preserving_ref: str | None = None,
) -> tuple[str, str]:
    """Remove one lane worktree if — and only if — every guard proves it safe.

    Guard order (cheapest and most absolute first; each one fails closed):

    1. a worktree path is recorded and a repo root resolves;
    2. the path is a REGISTERED LINKED worktree of that repo (this is what
       excludes the primary checkout and any unrelated directory);
    2b. the resolved target is not the repo root itself (a linked worktree
        used as ``repo_root`` must not be a legal removal target of its own
        pass);
    3. the branch (and the tree's HEAD) is fully merged into ``integration_ref``,
       or, when ``preserving_ref`` names a live task integration branch, is
       preserved by that ref (:func:`_extra_ref_preservation_verdict`);
    4. ``git status --porcelain`` in the worktree is empty;
    4b. every ignored entry enumerated by ``git ls-files`` is proven
        regenerable, with incomplete and nested-repository probes refused;
    5. no live, non-zombie process owns the worktree;
    5b. ``_session_live`` heartbeat is not live (``None``/unknown fail-closed)
        and no cross-task peer owns the path;
    5c. the lane's worker flock is acquired exclusively non-blocking around
        the removal-time re-probe + remove (TOCTOU). When the caller already
        holds a durable reaping claim, pass it as ``held_lock`` so this helper
        neither re-acquires nor releases the spanning claim.
    5d. a row closed with close authority (``close_authority`` in its notes)
        is removed only while the row is still the one closed and its branch
        is still at the authorized tip. This binds every caller, whether or
        not it passes ``expected_tip``; a moved tip reopens the row. A
        task-less caller whose lane id names more than one task is refused as
        ``lane_identity_ambiguous``.

    ``landing_lock_held`` is True only when the caller already holds
    ``_landing_mutation_lock`` for this repo in this process; the removal then
    does not take that lock again, because the lock is not reentrant across
    file descriptions.

    Guard 3 (merge / landed), guard 5 (owner + heartbeat) and guards 4/4b
    (dirty + ignored) run TWICE on the apply path: once as a cheap early
    filter and again under the worker flock immediately before the removal.
    A liveness, merge, or content snapshot goes stale — a worker can claim
    the tree, a commit can land, or ignored files (``.env``) can appear,
    between the decision and the delete — so the verdict that authorises the
    removal must be sampled at removal time, not reused. The pre-lock merge
    sample is never the authority. ``git worktree remove`` deletes ignored
    files even on a porcelain-clean tree.

    Returns ``(outcome, detail)``. ``apply=False`` can only ever return
    ``would_reclaim``; it never runs a removal.
    """
    _assert_no_reaper_write_lock(reason="lane_worktree_reclaim")
    path = _normalize_optional_text(worktree_path)
    if path is None:
        return ("skipped_no_worktree_path", "")
    if repo_root is None:
        try:
            repo_root = _workspace_root()
        except Exception as exc:  # noqa: BLE001 — probe degrade
            return ("refused_repo_root_unresolved", str(exc)[:_RECLAIM_DETAIL_CAP])
    root = Path(repo_root)

    linked, registry_detail = _linked_worktree_paths(root)
    if linked is None:
        return ("refused_registry_unavailable", registry_detail)
    target = _resolved_path_text(path).rstrip("/")
    if not target or target not in linked:
        return ("refused_not_linked_worktree", f"not a linked worktree of {root}")
    root_resolved = _resolved_path_text(root).rstrip("/")
    if target == root_resolved:
        return ("refused_repo_root", "target equals repo_root")

    if allow_rowless_work_complete:
        no_row, row_detail = _removal_time_rowless_lane_row_verdict(path, branch)
        if no_row is None:
            return ("refused_rowless_row_unknown", row_detail)
        if not no_row:
            return ("refused_rowless_row_present", row_detail)

    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        _try_authoritative_lane_base_sha,
    )

    if preserving_ref is not None:
        merged, merged_detail = _extra_ref_reclaim_verdict(
            repo_root=root,
            branch=branch,
            worktree_path=path,
            ref=preserving_ref,
        )
    elif allow_rowless_work_complete:
        merged, merged_detail = _rowless_worktree_landed_verdict(
            repo_root=root,
            branch=branch,
            worktree_path=path,
            integration_ref=integration_ref,
        )
    elif trusted_landing is not None:
        merged, merged_detail = _trusted_landing_reclaim_verdict(
            repo_root=root,
            branch=branch,
            worktree_path=path,
            task_ref=task_ref,
            lane_id=lane_id,
            proof=trusted_landing,
        )
    else:
        base_sha, base_error = _try_authoritative_lane_base_sha(
            task_ref=task_ref,
            lane_id=lane_id,
            repo=root,
        )
        if base_error:
            return ("refused_merge_unknown", "empty_lane_base_sha_unresolvable")
        merged, merged_detail = _landed_reclaim_verdict(
            repo_root=root,
            branch=branch,
            worktree_path=path,
            integration_ref=integration_ref,
            base_sha=base_sha,
        )
    if merged is None:
        return ("refused_merge_unknown", merged_detail)
    never_started_candidate = merged is False and merged_detail == "never_started"
    terminal_unmerged_candidate = (
        merged is False
        and merged_detail == "worktree_head_not_merged"
        and _worktree_head_is_recorded_branch(path, branch) is True
    )
    terminal_disposition_allows_reclaim = (
        allow_terminal_unmerged_bundled
        and terminal_unmerged_candidate
        and _terminal_unmerged_disposition_allows_reclaim(
            repo_root=root,
            branch=branch,
            integration_ref=integration_ref,
        )
    )
    unmerged_candidate = (
        allow_terminal_never_started and never_started_candidate
    ) or terminal_disposition_allows_reclaim
    if merged is False and not unmerged_candidate:
        return ("refused_unmerged", merged_detail)
    authorizing = (merged, merged_detail)

    clean, clean_detail = _probe_worktree_clean(path)
    if clean is None:
        return ("refused_status_unknown", clean_detail)
    interrupted_removal_resumed = False
    if clean is False:
        if not _is_interrupted_worktree_removal(root, path, branch):
            return _dirty_refusal_outcome(root, path, clean_detail)
        interrupted_removal_resumed = True

    ignored_ok, ignored_detail = _probe_worktree_ignored(path)
    if ignored_ok is None:
        if ignored_detail == "ignored_probe_timeout":
            return ("refused_ignored_timeout", ignored_detail)
        return ("refused_ignored_unknown", ignored_detail)
    if ignored_ok is False:
        return ("refused_ignored", ignored_detail)

    probe = owner_probe if owner_probe is not None else _probe_worktree_process_owner
    state, owner_detail = probe(path)
    if state == "owned":
        return ("refused_owned", owner_detail)
    if state != "free":
        return ("refused_owner_unknown", owner_detail)

    heartbeat_blocks, heartbeat_detail = _session_heartbeat_blocks_reclaim(
        repo_root=root,
        worktree_path=path,
        task_ref=task_ref,
        lane_id=lane_id,
    )
    if heartbeat_blocks:
        return ("refused_session_live", heartbeat_detail)

    shared_blocks, shared_detail = _shared_path_blocks_reclaim(
        worktree_path=path,
        task_ref=task_ref,
        lane_id=lane_id,
        repo_root=root,
    )
    if shared_blocks:
        return ("refused_shared_path", shared_detail)

    # Close authority binds every caller, not only those that pass expected_tip:
    # the durable row's authorized tip replaces an absent expectation, and a
    # caller that passes a different tip is refused, never overridden. Sampled
    # after the guards above, then re-checked under the lock below.
    durable_row, authority, authority_refusal = _durable_close_authority(task_ref=task_ref, lane_id=lane_id)
    if authority_refusal:
        return (authority_refusal, f"close_authority:{authority_refusal}")
    if authority is not None:
        if expected_tip is not None and expected_tip != authority["authorized_tip"]:
            return ("authority_mismatch", "expected_tip disagrees with the recorded close authority")
        expected_tip = authority["authorized_tip"]
    # A legacy row (closed before close authority existed) keeps today's
    # behaviour. The one typed diagnostic is logged, not added to the detail,
    # because reclaim details are compared exactly by existing callers.
    if apply and durable_row is not None and authority is None:
        _logger.info("close_authority_absent lane_id=%s task_ref=%s", lane_id, task_ref)

    own_lock = False
    lock_handle = held_lock
    if lock_handle is None:
        lane_key = _normalize_optional_text(lane_id)
        if lane_key is None:
            return ("refused_lock_unavailable", "lane_id_missing")
        lock_handle, lock_detail = _acquire_lane_worker_lock(lane_key)
        if lock_handle is None:
            return ("refused_lock_held", lock_detail)
        own_lock = True

    try:
        # Re-read the authority under the lock, before any dry-run return, so a
        # dry run never reports would_reclaim for a row apply would refuse.
        locked_row, locked_authority, locked_refusal = _durable_close_authority(task_ref=task_ref, lane_id=lane_id)
        if locked_refusal or locked_authority != authority:
            return (locked_refusal or "row_changed_after_close", "close authority changed before reclaim")
        if expected_tip is not None:
            tip_refusal = _branch_tip_refusal(repo_root=root, branch=branch, expected_tip=expected_tip)
            if tip_refusal == "tip_moved_after_close":
                detail = "authorized branch tip changed before reclaim"
                if apply and authority is not None and locked_row is not None:
                    reopen_detail = _reopen_moved_closed_row(locked_row)
                    if reopen_detail:
                        detail = f"{detail};{reopen_detail}"
                return ("tip_moved_after_close", detail)
            if tip_refusal:
                return (tip_refusal, "authorized branch tip unavailable")
        if not apply:
            if never_started_candidate or terminal_unmerged_candidate:
                row_terminal, _terminal_detail = _terminal_never_started_lane_row(
                    task_ref=task_ref,
                    lane_id=lane_id,
                )
                if not row_terminal:
                    return ("refused_unmerged", merged_detail)
                merged_detail = "never_started_terminal_row" if never_started_candidate else "terminal_unmerged_bundled"
            detail = f"{merged_detail};{ignored_detail}"
            if interrupted_removal_resumed:
                detail += ";interrupted_removal_resumed"
            return ("would_reclaim", detail)

        # Re-derive identity, liveness, dirty/ignored, and shared-path at
        # removal time while holding the worker flock. One helper, both
        # callers (REF-26) — never a second weaker copy of these guards.
        guard_outcome, guard_detail = _under_claim_worktree_reclaim_guards(
            worktree_path=path,
            branch=branch,
            repo_root=root,
            task_ref=task_ref,
            lane_id=lane_id,
            owner_probe=probe,
        )
        if guard_outcome is not None:
            return (guard_outcome, guard_detail)
        ignored_detail = guard_detail
        interrupted_removal_resumed = guard_detail.startswith("interrupted_removal_resumed;")

        # Re-run SHA merge under the flock. A commit created after the
        # pre-lock sample is invisible to porcelain; the early True is not
        # permission to delete. Tree equality is diagnostic only.
        if preserving_ref is not None:
            locked_merged, locked_detail = _extra_ref_reclaim_verdict(
                repo_root=root,
                branch=branch,
                worktree_path=path,
                ref=preserving_ref,
            )
        elif allow_rowless_work_complete:
            locked_merged, locked_detail = _rowless_worktree_landed_verdict(
                repo_root=root,
                branch=branch,
                worktree_path=path,
                integration_ref=integration_ref,
            )
        elif trusted_landing is not None:
            locked_merged, locked_detail = _trusted_landing_reclaim_verdict(
                repo_root=root,
                branch=branch,
                worktree_path=path,
                task_ref=task_ref,
                lane_id=lane_id,
                proof=trusted_landing,
            )
        else:
            locked_base, locked_base_error = _try_authoritative_lane_base_sha(
                task_ref=task_ref,
                lane_id=lane_id,
                repo=root,
            )
            if locked_base_error:
                return ("refused_merge_unknown", "empty_lane_base_sha_unresolvable")
            locked_merged, locked_detail = _landed_reclaim_verdict(
                repo_root=root,
                branch=branch,
                worktree_path=path,
                integration_ref=integration_ref,
                base_sha=locked_base,
            )
        if locked_merged is None:
            return ("refused_merge_unknown", locked_detail)
        if (locked_merged, locked_detail) != authorizing:
            return ("refused_merge_changed", locked_detail)
        if locked_merged is False:
            if never_started_candidate and allow_terminal_never_started and locked_detail == "never_started":
                row_terminal, _terminal_detail = _terminal_never_started_lane_row(
                    task_ref=task_ref,
                    lane_id=lane_id,
                )
                if not row_terminal:
                    return ("refused_unmerged", "never_started")
                merged_detail = "never_started_terminal_row"
            elif (
                terminal_unmerged_candidate
                and allow_terminal_unmerged_bundled
                and _worktree_head_is_recorded_branch(path, branch) is True
            ):
                row_terminal, _terminal_detail = _terminal_never_started_lane_row(
                    task_ref=task_ref,
                    lane_id=lane_id,
                )
                if not row_terminal:
                    return ("refused_unmerged", "worktree_head_not_merged")
                if not _terminal_unmerged_disposition_allows_reclaim(
                    repo_root=root,
                    branch=branch,
                    integration_ref=integration_ref,
                ):
                    return ("refused_unmerged", "worktree_head_not_merged")
                merged_detail = "terminal_unmerged_bundled"
            else:
                return ("refused_unmerged", locked_detail)
        else:
            merged_detail = locked_detail

        if allow_rowless_work_complete:
            no_row, row_detail = _removal_time_rowless_lane_row_verdict(path, branch)
            if no_row is None:
                return ("refused_rowless_row_unknown", row_detail)
            if not no_row:
                return ("refused_rowless_row_present", row_detail)

        # The tip was checked under the lock above; the tombstone's drain re-checks
        # it against the intent, so a move after this point is still refused.
        if interrupted_removal_resumed and expected_tip is None:
            # Identity is passed so the removal re-derives close authority at removal
            # time: an authority written after the locked read still binds this removal.
            removed, remove_detail = _remove_interrupted_lane_worktree(
                root,
                path,
                lane_id=lane_id,
                task_ref=task_ref,
                **({"landing_lock_held": True} if landing_lock_held else {}),
            )
            if not removed:
                if remove_detail.endswith("tip_moved_after_intent"):
                    # Same reopen policy as the plain arm. The outcome and detail stay
                    # byte-identical, so a failed reopen is logged rather than returned.
                    reopen_detail = _reopen_tip_moved_at_removal(
                        task_ref=task_ref, lane_id=lane_id, authority=authority, locked_row=locked_row
                    )
                    if reopen_detail:
                        _logger.warning(
                            "tip_moved_after_close reopen degraded lane_id=%s detail=%s", lane_id, reopen_detail
                        )
                return (remove_detail.split(":", 1)[0], remove_detail)
        else:
            # Identity is passed with or without an expected tip, so a row that gained
            # close authority after the locked read is still re-derived at removal time.
            removed, remove_detail = _remove_lane_worktree(
                root,
                path,
                expected_tip=expected_tip,
                lane_id=lane_id,
                task_ref=task_ref,
                **({"landing_lock_held": True} if landing_lock_held else {}),
            )
            if not removed and remove_detail == "tip_moved_after_intent":
                # The tombstone's drain-time tip guard saw the move after the intent
                # was written. Same fact as the pre-removal check: same outcome, same
                # reopen policy for a row that carries close authority.
                detail = "authorized branch tip changed before tombstone drain"
                reopen_detail = _reopen_tip_moved_at_removal(
                    task_ref=task_ref, lane_id=lane_id, authority=authority, locked_row=locked_row
                )
                if reopen_detail:
                    detail = f"{detail};{reopen_detail}"
                return ("tip_moved_after_close", _with_fence_release_detail(detail, remove_detail))
            if not removed:
                return ("remove_failed", remove_detail)
        # A removal that ran under the row's fence but could not release it says so beside its success.
        return ("reclaimed", _with_fence_release_detail(f"{merged_detail};{ignored_detail}", remove_detail))
    finally:
        if own_lock:
            _release_lane_worker_lock(lock_handle)


def _close_blocked_lane_cas(
    conn: sqlite3.Connection,
    *,
    lane_pk: int,
    probed_updated_at: object,
    note: str,
    expected_status: str = _LANE_STATUS_BLOCKED,
) -> bool:
    """CAS: ``expected_status`` → ``closed_stale`` only if the row still has that
    status with the probed ``updated_at`` (default ``blocked`` for the blocked-lane
    reaper; the task-archived-orphan reaper passes the lane's actual status since
    an orphan can sit in ``planned``/``active``/``review``)."""
    existing_notes = conn.execute(
        "SELECT notes FROM worktree_lanes WHERE id = ?",
        (lane_pk,),
    ).fetchone()
    prior = ""
    if existing_notes is not None:
        prior_raw = existing_notes["notes"] if isinstance(existing_notes, sqlite3.Row) else existing_notes[0]
        prior = str(prior_raw or "").strip()
    new_notes = f"{prior} [{note}]".strip() if prior else note
    cur = conn.execute(
        """
        UPDATE worktree_lanes
        SET status = ?,
            notes = ?,
            updated_at = datetime('now')
        WHERE id = ?
          AND status = ?
          AND ((updated_at IS NULL AND ? IS NULL) OR updated_at = ?)
        """,
        (
            _LANE_STATUS_CLOSED_STALE,
            new_notes,
            lane_pk,
            expected_status,
            probed_updated_at,
            probed_updated_at,
        ),
    )
    return int(cur.rowcount or 0) == 1


def _cas_liveness_raced_reason(
    *,
    worktree_path: object,
    branch: object,
    branch_dead: object,
    merged_by_tree: object,
    path_probe: Callable[[object], bool | None],
    branch_probe: Callable[[object], bool | None],
    repo_root: Path | str | None,
) -> str | None:
    """Return a skip reason if the close-time re-probe is no longer dead.

    Recency check only — subprocesses stay outside the sqlite write
    transaction. ``None`` means still conclusive-dead; a string is the
    raced/skipped telemetry reason. Tree equality is diagnostic evidence,
    not close authority (AGT-10): a tree-only close must re-check SHA
    recency and skip when the branch is not independently dead.
    """
    try:
        gone = path_probe(worktree_path)
    except Exception:  # noqa: BLE001 — recency degrade is a race
        gone = None
    if gone is not True:
        return "liveness raced: worktree no longer gone"
    if merged_by_tree is True:
        try:
            tree_eq, _detail = _probe_branch_merged_by_tree(branch, repo_root=repo_root)
        except Exception:  # noqa: BLE001 — recency degrade is a race
            tree_eq = None
        if tree_eq is not True:
            return "liveness raced: branch no longer dead"
        # Tree equality corroborates; it cannot prove the branch dead.
        if branch_dead is not True:
            return "liveness raced: branch no longer dead"
    if branch_dead is True:
        try:
            dead = branch_probe(branch)
        except Exception:  # noqa: BLE001 — recency degrade is a race
            dead = None
        if dead is not True:
            return "liveness raced: branch no longer dead"
    return None


DEFAULT_BRANCH_RECLAIM_DRAIN_BATCH = 25
TERMINAL_BRANCH_PRODUCER_MAX_EVALUATIONS = 25
_TERMINAL_BRANCH_PRODUCER_SESSION = "terminal-branch-producer-v1"
_TERMINAL_BRANCH_PRODUCER_CURSOR_DECISION = "page-cursor"
_TERMINAL_BRANCH_PRODUCER_ALL_TASKS = "__workbay_terminal_branch_producer_all_tasks__"


def _terminal_branch_producer_scope(task_ref: str | None) -> str:
    return task_ref or _TERMINAL_BRANCH_PRODUCER_ALL_TASKS


class TerminalBranchCursorMalformed(ValueError):
    """Persisted continuation is unsafe; leave it intact for operator repair."""


def _load_terminal_branch_producer_rows(
    *,
    task_ref: str | None,
    max_rows: int,
) -> tuple[list[dict[str, object]], int, int]:
    """Read an indexed page plus bounded lookahead, rotating at the end.

    The second value is the number observed, not a full-table total. Remaining
    is exact up to max_rows; larger values are a lower bound. Index construction
    is a one-time schema cost, matching the existing rowless-probe indexes.
    """
    statuses = ", ".join(f"'{status}'" for status in sorted(_RECLAIM_ELIGIBLE_TERMINAL_STATUSES))
    scope = _terminal_branch_producer_scope(task_ref)
    scoped_sql = "AND task_ref = ? " if task_ref is not None else ""
    scoped_params: tuple[object, ...] = (task_ref,) if task_ref is not None else ()
    identity_filter = f"TRIM(COALESCE(branch, '')) != '' AND status IN ({statuses}) "
    time_expr = "datetime(COALESCE(created_at, updated_at))"
    lookahead = max_rows * 2 + 1

    with _get_db_connection() as conn:
        cursor_row = conn.execute(
            "SELECT rationale FROM decisions WHERE task_ref = ? AND session = ? AND decision = ?",
            (scope, _TERMINAL_BRANCH_PRODUCER_SESSION, _TERMINAL_BRANCH_PRODUCER_CURSOR_DECISION),
        ).fetchone()
        cursor: dict[str, object] = {}
        if cursor_row is not None:
            try:
                parsed = json.loads(str(cursor_row["rationale"] or ""))
            except (TypeError, ValueError) as exc:
                raise TerminalBranchCursorMalformed("cursor_malformed: invalid JSON") from exc
            if not isinstance(parsed, dict) or type(parsed.get("id")) is not int or parsed["id"] <= 0:
                raise TerminalBranchCursorMalformed("cursor_malformed: expected a positive integer id")
            stamp = parsed.get("created_at")
            if not isinstance(stamp, str) or not stamp.strip():
                raise TerminalBranchCursorMalformed("cursor_malformed: expected a nonblank created_at")
            valid_stamp = conn.execute("SELECT datetime(?) AS stamp", (stamp,)).fetchone()["stamp"]
            if valid_stamp is None:
                raise TerminalBranchCursorMalformed("cursor_malformed: invalid created_at timestamp")
            cursor = {"id": parsed["id"], "created_at": stamp}
            if "retry_id" in parsed:
                if type(parsed["retry_id"]) is not int or parsed["retry_id"] <= 0:
                    raise TerminalBranchCursorMalformed("cursor_malformed: expected a positive integer retry_id")
                cursor["retry_id"] = parsed["retry_id"]

        # Partial ordered indexes serve both scopes without scanning/sorting all
        # terminal rows. Predicates contain only the module's trusted status set.
        for suffix, prefix in (("global", ""), ("scoped", "task_ref, ")):
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_terminal_branch_producer_{suffix} "
                f"ON worktree_lanes({prefix}{time_expr}, id) WHERE {identity_filter}"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_terminal_branch_producer_report "
            "ON worker_reports(task_ref, lane_id, created_at DESC, id DESC)"
        )
        columns = (
            "SELECT id, task_ref, lane_id, branch, worktree_path, status, lane_kind, notes, created_at, updated_at "
            f"FROM worktree_lanes WHERE {identity_filter}{scoped_sql}"
        )
        ordering = f" ORDER BY {time_expr} ASC, id ASC LIMIT ?"
        rows: list[sqlite3.Row] = []
        seek_cursor = cursor
        retrying = False
        if "retry_id" in cursor:
            retry = conn.execute(f"{columns} AND id = ? LIMIT 1", (*scoped_params, cursor["retry_id"])).fetchone()
            if retry is not None:
                # Retry once from the failed row, filling the same bounded page.
                # The following call resumes progress even if this retry fails.
                seek_cursor = dict(retry)
                retrying = True
        if seek_cursor:
            stamp = str(seek_cursor.get("created_at") or seek_cursor.get("updated_at"))
            cursor_id = int(seek_cursor["id"])
            id_operator = ">=" if retrying else ">"
            # Separate seeks matter: SQLite scans an expression index for a
            # tuple comparison, but seeks on expression equality plus id range.
            rows = conn.execute(
                f"{columns} AND {time_expr} = datetime(?) AND id {id_operator} ? ORDER BY id ASC LIMIT ?",
                (*scoped_params, stamp, cursor_id, lookahead),
            ).fetchall()
            if len(rows) < lookahead:
                rows.extend(
                    conn.execute(
                        f"{columns} AND {time_expr} > datetime(?){ordering}",
                        (*scoped_params, stamp, lookahead - len(rows)),
                    ).fetchall()
                )
        if not rows:
            rows = conn.execute(f"{columns}{ordering}", (*scoped_params, lookahead)).fetchall()
        page = [dict(row) for row in rows[:max_rows]]
        for row in page:
            row["_producer_retrying"] = retrying
            # Crash and needs-guidance reports are persisted as failed/stopped,
            # independently of the coordinator's terminal registry status.
            report = conn.execute(
                "SELECT outcome FROM worker_reports WHERE task_ref = ? AND lane_id = ? "
                "ORDER BY created_at DESC, id DESC LIMIT 1",
                (row["task_ref"], row["lane_id"]),
            ).fetchone()
            row["worker_outcome"] = report["outcome"] if report is not None else None
        return page, len(rows), max(0, len(rows) - len(page))


def _terminal_branch_outcome_allows_enqueue(row: Mapping[str, object]) -> bool:
    """Keep the dispose helper's outcome guard effective for registry rows."""
    from .orchestration.lane_terminal_dispose import (  # noqa: PLC0415
        _outcome_is_crash_terminal,
        _outcome_token,
    )

    if row.get("worker_outcome") in {"failed", "stopped", "exhausted"}:
        return False
    try:
        notes = _terminal_dispose_notes(row.get("notes"), strict=True)
    except ValueError:
        # Nonempty, unreadable outcome evidence cannot authorize disposal.
        return False
    signals = [{"status": row.get("status"), **notes}, notes.get("terminal_outcome"), notes.get("handoff_action")]
    return all(
        _outcome_token(signal) not in {"error", "needs_guidance"}
        and not _outcome_is_crash_terminal(signal, notes.get("is_crash_terminal"))
        for signal in signals
        if signal is not None and signal != {}
    )


def _store_terminal_branch_producer_cursor(
    *, task_ref: str | None, row: Mapping[str, object], retry_id: int | None = None
) -> None:
    scope = _terminal_branch_producer_scope(task_ref)
    payload = json.dumps(
        {
            "id": int(row["id"]),
            "created_at": str(row.get("created_at") or row.get("updated_at") or ""),
            **({"retry_id": retry_id} if retry_id is not None else {}),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    with _get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO decisions (task_ref, session, decision, rationale, decision_origin)
            VALUES (?, ?, ?, ?, 'system')
            ON CONFLICT(task_ref, decision, session)
            DO UPDATE SET rationale = excluded.rationale
            """,
            (scope, _TERMINAL_BRANCH_PRODUCER_SESSION, _TERMINAL_BRANCH_PRODUCER_CURSOR_DECISION, payload),
        )


def _terminal_branch_producer_error(exc: BaseException) -> dict[str, object]:
    return {
        **({"reason": "cursor_malformed"} if isinstance(exc, TerminalBranchCursorMalformed) else {}),
        "ok": False,
        "evaluated": 0,
        "enqueued": [],
        "would_enqueue": [],
        "skipped": [],
        "truncated": False,
        "remaining": 0,
        "error": str(exc),
        "error_type": type(exc).__name__,
    }


def _produce_terminal_branch_reclaims(
    *,
    apply: bool,
    task_ref: str | None,
    orchestrator_root: Path | str | None,
) -> dict[str, object]:
    """Queue authorized terminal branch tips for the existing steady-state drain."""
    try:
        batch = max(1, int(TERMINAL_BRANCH_PRODUCER_MAX_EVALUATIONS))
        rows, _total, remaining = _load_terminal_branch_producer_rows(task_ref=task_ref, max_rows=batch)
    except Exception as exc:  # noqa: BLE001 — row/cursor failures are visible and fail closed
        return _terminal_branch_producer_error(exc)

    result: dict[str, object] = {
        "ok": True,
        "evaluated": 0,
        "enqueued": [],
        "would_enqueue": [],
        "skipped": [],
        "truncated": remaining > 0,
        "remaining": remaining,
        "remaining_is_lower_bound": remaining > batch,
    }
    skipped = cast(list[dict[str, object]], result["skipped"])
    enqueued = cast(list[dict[str, object]], result["enqueued"])
    would_enqueue = cast(list[dict[str, object]], result["would_enqueue"])
    if not rows:
        return result

    root = Path(orchestrator_root) if orchestrator_root is not None else _workspace_root()
    configured = os.environ.get("WORKBAY_TERMINAL_DISPOSE_ENABLED")
    disposal_enabled = configured is None or configured.strip().casefold() in {"1", "true", "yes", "on"}
    registered_branches: set[str] = set()
    try:
        from .orchestration.branch_reclaim_queue import list_branch_reclaim_queue  # noqa: PLC0415

        queued_items = list_branch_reclaim_queue(task_ref=task_ref)
    except Exception as exc:  # noqa: BLE001 — queue identity cannot be safely duplicated on a failed read
        result.update({"ok": False, "error": str(exc), "error_type": type(exc).__name__})
        return result
    queued_identities = {(item.task_ref, item.lane_id, item.authorized_sha) for item in queued_items}
    if disposal_enabled:
        worktrees = _run_reclaim_command(["git", "-C", str(root), "worktree", "list", "--porcelain"])
        if worktrees is None or worktrees.returncode != 0:
            error = RuntimeError("terminal branch producer could not read git worktree registry")
            result.update({"ok": False, "error": str(error), "error_type": type(error).__name__})
            return result
        registered_branches = {
            str(entry["branch"]) for entry in _parse_worktree_registry(worktrees.stdout or "") if entry.get("branch")
        }

    integration_ref = _RECLAIM_INTEGRATION_REF.removeprefix("refs/heads/")
    first_failed_index: int | None = None

    def row_failure(index: int, error: BaseException) -> None:
        nonlocal first_failed_index
        if first_failed_index is None:
            first_failed_index = index
        result.update({"ok": False, "error": str(error), "error_type": type(error).__name__})

    for index, row in enumerate(rows):
        result["evaluated"] = int(result["evaluated"]) + 1
        branch = _normalize_optional_text(row.get("branch")) or ""
        task = _normalize_optional_text(row.get("task_ref")) or ""
        lane = _normalize_optional_text(row.get("lane_id")) or ""
        status = _normalize_optional_text(row.get("status")) or ""
        lane_kind = _normalize_optional_text(row.get("lane_kind"))
        identity = {"task_ref": task, "lane_id": lane, "branch": branch}
        if not task or not lane or not branch:
            skipped.append({**identity, "reason": "lane_identity_incomplete"})
            continue
        if not disposal_enabled:
            skipped.append({**identity, "reason": "terminal_dispose_disabled"})
            continue
        if not _terminal_branch_outcome_allows_enqueue(row):
            skipped.append({**identity, "reason": "terminal_outcome_refused"})
            continue
        if branch == integration_ref:
            skipped.append({**identity, "reason": "integration_branch"})
            continue
        if branch in registered_branches:
            skipped.append({**identity, "reason": "checked_out"})
            continue
        worktree_path = _normalize_optional_text(row.get("worktree_path"))
        if worktree_path is not None and Path(worktree_path).exists():
            skipped.append({**identity, "reason": "worktree_path_exists"})
            continue

        proc = _run_reclaim_command(
            ["git", "-C", str(root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"]
        )
        confirmation_detail = ""
        if (
            proc is not None
            and proc.returncode == 128
            and (proc.stderr or "").strip() == "fatal: Needed a single revision"
        ):
            # This is rev-parse's defined missing-ref shape. Confirm the exact
            # ref is absent: an existing non-commit ref can have the same shape.
            absent = _run_reclaim_command(
                ["git", "-C", str(root), "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"]
            )
            if absent is not None and absent.returncode == 1 and not (absent.stderr or "").strip():
                skipped.append({**identity, "reason": "branch_missing"})
                continue
            confirmation_detail = (
                "; absence confirmation: no process"
                if absent is None
                else f"; absence confirmation rc={absent.returncode}: {(absent.stderr or '').strip()}"
            )
        tip = (proc.stdout or "").strip().lower() if proc is not None else ""
        if proc is None or proc.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", tip) is None:
            detail = (
                "no process"
                if proc is None
                else f"rc={proc.returncode}: {(proc.stderr or '').strip() or 'invalid commit tip'}"
            )
            error = RuntimeError(f"terminal branch tip probe failed for {branch}: {detail}{confirmation_detail}")
            skipped.append(
                {
                    **identity,
                    "reason": "branch_tip_probe_failed",
                    "error": str(error),
                    "error_type": type(error).__name__,
                }
            )
            row_failure(index, error)
            continue
        if (task, lane, tip) in queued_identities:
            skipped.append({**identity, "reason": "already_queued", "authorized_sha": tip})
            continue
        if not _terminal_unmerged_disposition_allows_reclaim(
            repo_root=root,
            branch=branch,
            integration_ref=_RECLAIM_INTEGRATION_REF,
        ):
            skipped.append({**identity, "reason": "disposition_refused"})
            continue

        candidate = {**identity, "authorized_sha": tip, "outcome": status, "lane_kind": lane_kind}
        if not apply:
            would_enqueue.append(candidate)
            continue
        try:
            accepted = dispose_terminal_lane(
                task,
                lane,
                branch,
                tip_sha=tip,
                outcome=status,
                lane_kind=lane_kind,
            )
        except Exception as exc:  # noqa: BLE001 — typed row failure; the drain still runs
            skipped.append({**identity, "reason": "queue_write_failed", "error": str(exc)})
            row_failure(index, exc)
            continue
        if not accepted:
            error = RuntimeError(f"terminal queue refused candidate for {branch}")
            skipped.append({**identity, "reason": "queue_write_refused"})
            row_failure(index, error)
            continue
        enqueued.append(candidate)

    # Advance past every evaluated row. Remember at most one immediate retry;
    # further failures remain reachable when the indexed scan wraps around.
    if apply:
        try:
            _store_terminal_branch_producer_cursor(
                task_ref=task_ref,
                row=rows[-1],
                retry_id=int(rows[first_failed_index]["id"])
                if first_failed_index is not None and not rows[0].get("_producer_retrying")
                else None,
            )
        except Exception as exc:  # noqa: BLE001 — continuation loss is an explicit producer failure
            result.update({"ok": False, "error": str(exc), "error_type": type(exc).__name__})
    return result


def _terminal_dispose_notes(value: object, *, strict: bool = False) -> dict[str, Any]:
    """Strict reads protect admission; optional review metadata stays lenient."""
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        if strict and value is not None and not isinstance(value, str):
            raise ValueError("terminal dispose notes must be an object")
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        if strict:
            raise
        return {}
    if not isinstance(parsed, dict):
        if strict:
            raise ValueError("terminal dispose notes must be an object")
        return {}
    return dict(parsed)


def _terminal_dispose_receipt_has_evidence(state_dir: Path | None, task_ref: str, lane_id: str) -> bool:
    """Return true only for a receipt recorded for this exact lane identity."""

    if state_dir is None or not state_dir.is_dir():
        return False
    for path in sorted(state_dir.rglob("review-receipt-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            continue
        if not isinstance(payload, Mapping):
            continue
        identity = payload.get("identity")
        kwargs = payload.get("kwargs")
        identity = identity if isinstance(identity, Mapping) else {}
        kwargs = kwargs if isinstance(kwargs, Mapping) else {}
        saved_task = identity.get("task_ref") or kwargs.get("task_ref")
        saved_lane = identity.get("lane_id") or kwargs.get("lane_id")
        if saved_task != task_ref or saved_lane != lane_id:
            continue
        if payload.get("status") == "recorded":
            return True
        harvest = payload.get("harvest")
        if not isinstance(harvest, Mapping):
            continue
        dropped = harvest.get("dropped", 0)
        if harvest.get("status") == "recorded" and type(dropped) is int and dropped == 0:
            return True
        if harvest.get("reason") == "no_valid_findings" and type(dropped) is int and dropped == 0:
            return True
    return False


def _terminal_dispose_ref_sha(root: Path, ref: str) -> str | None:
    result = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--verify", f"{ref}^{{commit}}"])
    if result is None or result.returncode != 0:
        return None
    value = (result.stdout or "").strip()
    return value or None


def _terminal_dispose_ancestor(root: Path, tip_sha: str, integration_ref: str) -> bool | None:
    result = _run_reclaim_command(["git", "-C", str(root), "merge-base", "--is-ancestor", tip_sha, integration_ref])
    return _probe_command_tristate(_PROBE_SITE_MERGE_BASE, result)


def _terminal_dispose_no_commits(root: Path, row: Mapping[str, Any], tip_sha: str) -> bool | None:
    subject = _terminal_dispose_notes(row.get("notes")).get("review_subject")
    subject = subject if isinstance(subject, Mapping) else {}
    base_ref = subject.get("base_ref") or row.get("review_base_ref")
    tip_ref = subject.get("tip_ref") or row.get("review_tip_ref")
    if not isinstance(base_ref, str) or not base_ref.strip():
        return None
    base_sha = _terminal_dispose_ref_sha(root, base_ref.strip())
    if base_sha is None:
        return None
    observed_tip = tip_sha
    if isinstance(tip_ref, str) and tip_ref.strip():
        observed_tip = _terminal_dispose_ref_sha(root, tip_ref.strip()) or observed_tip
    return base_sha == observed_tip


def _terminal_dispose_lane_row(item: Any) -> tuple[dict[str, Any] | None, int | None]:
    try:
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? ORDER BY id DESC LIMIT 1",
                (item.task_ref, item.lane_id),
            ).fetchone()
            try:
                finding_row = conn.execute(
                    "SELECT COUNT(*) AS count FROM review_findings WHERE task_ref = ? AND lane_id = ?",
                    (item.task_ref, item.lane_id),
                ).fetchone()
                finding_count = int(finding_row["count"] or 0) if finding_row is not None else None
            except sqlite3.Error:
                finding_count = None
    except Exception:  # noqa: BLE001 - missing runtime/schema is a typed refusal
        return None, None
    return (dict(row) if row is not None else None), finding_count


def _terminal_dispose_verdict(
    item: Any,
    *,
    root: Path,
    integration_ref: str,
    row: Mapping[str, Any] | None,
) -> ReclaimVerdict:
    if row is None:
        return ReclaimVerdict(
            reclaimable=False,
            reason="lane_row_missing",
            observed={
                "branch": item.branch,
                "production_paths": [],
                "survivor_refs": [],
                "unpreserved_paths": [],
            },
        )
    try:
        with _scan_read_pass():
            return lane_branch_reclaimable(
                orchestrator_root=root,
                task_ref=item.task_ref,
                lane_id=item.lane_id,
                integration_ref=integration_ref,
            )
    except Exception as exc:  # noqa: BLE001 - classifier failures are typed refusals
        return ReclaimVerdict(
            reclaimable=False,
            reason="evaluator_failed",
            observed={
                "branch": item.branch,
                "production_paths": [],
                "survivor_refs": [],
                "unpreserved_paths": [],
                "error": str(exc),
            },
        )


def _terminal_disposition_for_item(
    item: Any,
    *,
    root: Path,
    integration_ref: str,
) -> BranchDisposition | None:
    """Classify terminal queue facts once so dry-run and apply share the result.

    Reclaim items guarantee the branch identity fields used by the actuator;
    ``outcome`` is optional for compatibility with older duck-typed callers.
    """

    try:
        if getattr(item, "outcome", None) != "terminal_lane":
            return None
        task_ref = item.task_ref
        lane_id = item.lane_id
        branch = item.branch
        authorized_sha = item.authorized_sha
        row, finding_count = _terminal_dispose_lane_row(item)
        verdict = _terminal_dispose_verdict(
            item,
            root=root,
            integration_ref=integration_ref,
            row=row,
        )
        try:
            from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

            state_dir = Path(get_runtime_config().state_dir)
        except Exception:  # noqa: BLE001 - absent runtime/evidence is not proof
            state_dir = None
        evidence = _terminal_dispose_receipt_has_evidence(state_dir, task_ref, lane_id)
        is_ancestor = _terminal_dispose_ancestor(root, authorized_sha, integration_ref)
        no_commits = _terminal_dispose_no_commits(root, row, authorized_sha) if row is not None else None
    except Exception as exc:  # noqa: BLE001 - malformed queue items must not abort the drain
        _logger.warning("terminal disposition skipped for queue item: %s", type(exc).__name__)
        return None
    try:
        return classify_terminal_lane(
            verdict,
            row,
            is_ancestor=is_ancestor,
            no_commits=no_commits,
            review_evidence=evidence,
            review_findings_count=finding_count,
            integration_ref=integration_ref,
            branch=branch,
        )
    except Exception:  # noqa: BLE001 - classifier failures must not arm deletion
        return BranchDisposition.UNDECIDABLE


def _branch_reclaim_item_summary(
    item: Any, result: Any, *, acked: bool | None = None, bundle: dict[str, object] | None = None
) -> dict[str, object]:
    summary: dict[str, object] = {
        "task_ref": item.task_ref,
        "lane_id": item.lane_id,
        "branch": item.branch,
        "authorized_sha": item.authorized_sha,
        "reason": item.reason,
        "force_authorized": item.force_authorized,
        "delete_reason": result.reason,
    }
    delete_detail = getattr(result, "detail", None)
    if delete_detail:
        summary["delete_detail"] = delete_detail
    if acked is not None:
        summary["acked"] = acked
    if bundle is not None:
        summary.update({key: bundle[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle})
    disposition = getattr(result, "disposition", None)
    if disposition is not None:
        summary["disposition"] = str(disposition)
    summary.update(_branch_reclaim_fence_release_fields(getattr(result, "fence_release", "")))
    return summary


def _branch_reclaim_fence_release_fields(release: object) -> dict[str, object]:
    """Typed fields for a fenced branch delete whose fence was not released; ``{}`` when clean.

    ``release`` is the delete actuator's own ``fence_release`` (or the state an escaped
    exception carries). ``released`` and an unfenced ``""`` add nothing, so a clean item keeps
    its old shape; ``not_ours`` and ``release_failed`` are reported beside the item's own
    result, never folded into ``delete_reason`` or ``acked`` [AGT-10].
    """
    if not isinstance(release, str) or not release or release == "released":
        return {}
    return {
        "fence_release": [{"step": "branch_delete", "release": release}],
        "cleanup_pending": "reclaim_fence_release",
    }


def _note_raised_branch_reclaim_release(result: dict[str, object], item: Any, exc: BaseException) -> None:
    """Record an unreleased fence an item's raised delete carries, with its identity [AGT-10].

    The exception itself still propagates to the drain's never-raise handler; this only keeps
    the release state the fenced act attached to it from being lost with the item summary.
    """
    from workbay_orchestrator_mcp.orchestration.branch_reclaim_delete import (  # noqa: PLC0415
        exception_fence_release,
    )

    fields = _branch_reclaim_fence_release_fields(exception_fence_release(exc))
    if not fields:
        return
    records = result.setdefault("fence_release", [])
    for record in cast(list[dict[str, object]], fields["fence_release"]):
        cast(list[dict[str, object]], records).append(
            {
                "task_ref": item.task_ref,
                "lane_id": item.lane_id,
                "branch": item.branch,
                "authorized_sha": item.authorized_sha,
                **record,
                "raised": type(exc).__name__,
            }
        )
    result["cleanup_pending"] = fields["cleanup_pending"]


def _drain_one_branch_reclaim_item(
    item: Any,
    *,
    apply: bool,
    orchestrator_root: Path,
    integration_ref: str,
    bundle_retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
    drained: list[dict[str, object]],
    would_drain: list[dict[str, object]],
    skipped: list[dict[str, object]],
    probe_failed: list[dict[str, object]],
) -> Any | None:
    from workbay_orchestrator_mcp.orchestration.branch_reclaim_delete import (  # noqa: PLC0415
        delete_authorized_branch,
    )
    from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
        queue_item_is_drainable,
        record_branch_reclaim_failure,
    )

    if not queue_item_is_drainable(item):
        skipped.append({"task_ref": item.task_ref, "lane_id": item.lane_id, "reason": item.reason})
        return None
    delete_args = dict(
        orchestrator_root=orchestrator_root,
        lane_id=item.lane_id,
        branch=item.branch,
        authorized_sha=item.authorized_sha,
        task_ref=item.task_ref,
        integration_ref=integration_ref,
    )
    disposition = _terminal_disposition_for_item(
        item,
        root=orchestrator_root,
        integration_ref=integration_ref,
    )
    if disposition is not None:
        delete_args["disposition"] = disposition
    # Preserve the actuator's typed missing/invalid refusals before archiving.
    # A squash proof is intentionally unavailable in dry-run, so it may still
    # proceed to the apply-time live proof after the bundle guard [RLSE-08].
    result = delete_authorized_branch(**delete_args, apply=False)
    bundle: dict[str, object] | None = None
    if apply and result.reason in {"would_delete", "live_proof_failed"}:
        bundle = _bundle_before_reap(
            orchestrator_root,
            item.branch.strip().removeprefix("refs/heads/"),
            retention_count=bundle_retention_count,
        )
        if not bundle.get("ok") or bundle.get("tip_sha") != item.authorized_sha:
            detail = str(bundle.get("error") or "bundled_tip_differs_from_authorized_sha")
            record_branch_reclaim_failure(item=item, error=detail)
            skipped.append(
                {
                    **_branch_reclaim_item_summary(item, result, bundle=bundle),
                    "delete_reason": "verified_bundle_required",
                    "reap_guard": "verified_bundle_required",
                    "reap_refusal": "bundle_failed",
                    "reap_detail": detail,
                }
            )
            return None
        # Re-run all live proof and SHA guards after the potentially slow bundle.
        result = delete_authorized_branch(**delete_args, apply=True)
    if result.reason == "would_delete":
        would_drain.append(_branch_reclaim_item_summary(item, result))
        return None
    if apply and (result.deleted or result.reason == "branch_missing"):
        drained.append(_branch_reclaim_item_summary(item, result, acked=False, bundle=bundle))
        return item
    if apply and result.reason == "probe_failed":
        record_branch_reclaim_failure(item=item, error=result.detail or result.reason)
        probe_failed.append(_branch_reclaim_item_summary(item, result, acked=False, bundle=bundle))
        return None
    if apply and result.reason in {"live_proof_failed", "delete_failed"}:
        record_branch_reclaim_failure(item=item, error=result.detail or result.reason)
    skipped.append(_branch_reclaim_item_summary(item, result, bundle=bundle))
    return None


def _empty_branch_reclaim_drain(*, apply: bool, max_batch: int) -> dict[str, object]:
    return {
        "ok": True,
        "applied": apply,
        "max_batch": max_batch,
        "truncated": False,
        "drained": [],
        "would_drain": [],
        "skipped": [],
        "probe_failed": [],
        "skipped_counts": {},
    }


def _rotate_queue_items_after_cursor(items: list[Any], after_id: int) -> list[Any]:
    from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
        queue_item_cursor_id,
    )

    tail = [item for item in items if queue_item_cursor_id(item) > after_id]
    head = [item for item in items if queue_item_cursor_id(item) <= after_id]
    return tail + head


def _commit_branch_reclaim_acks_and_cursor(
    *,
    task_ref: str | None,
    pending_acks: list[Any],
    drained: list[dict[str, object]],
    last_examined: int,
    examined_any: bool,
    persist_cursor: bool,
) -> None:
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415
    from workbay_handoff_mcp.shared_schema import connect_handoff_db  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
        acknowledge_branch_reclaim_item,
        store_branch_reclaim_drain_cursor,
    )

    if not pending_acks and not (examined_any and persist_cursor):
        return
    db_path = get_runtime_config().db_path
    with closing(connect_handoff_db(db_path, read_only=False)) as conn:
        conn.execute("BEGIN")
        try:
            for item, summary in zip(pending_acks, drained, strict=True):
                summary["acked"] = acknowledge_branch_reclaim_item(
                    conn,
                    item=item,
                    authorized_sha=item.authorized_sha,
                )
            if examined_any and persist_cursor:
                store_branch_reclaim_drain_cursor(conn, task_ref=task_ref, last_id=last_examined)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def drain_branch_reclaim_queue(
    *,
    apply: bool = False,
    task_ref: str | None = None,
    orchestrator_root: Path | str | None = None,
    integration_ref: str = "main",
    max_batch: int = DEFAULT_BRANCH_RECLAIM_DRAIN_BATCH,
    bundle_retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
) -> dict[str, object]:
    """Drain queued branch-reclaim jobs through the live-proof actuator."""

    try:
        batch = max(1, int(max_batch))
    except (TypeError, ValueError):
        batch = DEFAULT_BRANCH_RECLAIM_DRAIN_BATCH
    result = _empty_branch_reclaim_drain(apply=apply, max_batch=batch)
    try:
        from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415
        from workbay_handoff_mcp.shared_schema import connect_handoff_db  # noqa: PLC0415

        from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
            list_branch_reclaim_queue_with_conn,
            load_branch_reclaim_drain_cursor,
            queue_item_cursor_id,
            queue_item_is_drainable,
        )

        root = Path(orchestrator_root) if orchestrator_root is not None else _workspace_root()
        # The decisions store only offers prefix GLOB, not a drainable-only
        # LIMIT query. Coalescing a tip also needs every producer row, so the
        # bounded window is applied in process after the coalesced list returns.
        db_path = get_runtime_config().db_path
        with closing(connect_handoff_db(db_path, read_only=True)) as conn:
            items = list_branch_reclaim_queue_with_conn(conn, task_ref=task_ref)
            after_id = load_branch_reclaim_drain_cursor(conn, task_ref=task_ref)
        ordered = _rotate_queue_items_after_cursor(items, after_id)
        drainable_total = sum(1 for item in items if queue_item_is_drainable(item))
        drainable_taken = 0
        last_examined = after_id
        examined_any = False
        pending_acks: list[Any] = []
        skipped_counts: dict[str, int] = {}
        for item in ordered:
            drainable = queue_item_is_drainable(item)
            if drainable and drainable_taken >= batch:
                break
            examined_any = True
            last_examined = queue_item_cursor_id(item)
            try:
                to_ack = _drain_one_branch_reclaim_item(
                    item,
                    apply=apply,
                    orchestrator_root=root,
                    integration_ref=integration_ref,
                    bundle_retention_count=bundle_retention_count,
                    drained=result["drained"],  # type: ignore[arg-type]
                    would_drain=result["would_drain"],  # type: ignore[arg-type]
                    skipped=result["skipped"],  # type: ignore[arg-type]
                    probe_failed=result["probe_failed"],  # type: ignore[arg-type]
                )
            except Exception as exc:
                _note_raised_branch_reclaim_release(result, item, exc)
                raise
            if drainable:
                drainable_taken += 1
            else:
                skipped_counts[item.reason] = skipped_counts.get(item.reason, 0) + 1
            if to_ack is not None:
                pending_acks.append(to_ack)
        result["truncated"] = drainable_taken < drainable_total
        result["skipped_counts"] = skipped_counts
        _commit_branch_reclaim_acks_and_cursor(
            task_ref=task_ref,
            pending_acks=pending_acks,
            drained=result["drained"],  # type: ignore[arg-type]
            last_examined=last_examined,
            examined_any=examined_any,
            persist_cursor=apply,
        )
        return _with_bundle_telemetry(result)
    except Exception as exc:  # noqa: BLE001 — never-raise reaper arm
        result["ok"] = False
        result["error"] = str(exc)
        return _with_bundle_telemetry(result)


_BUNDLE_TELEMETRY_FIELDS = ("bundle_mode", "bundle_bytes", "bundle_fallback_reason", "bundle_reused")
_BUNDLE_RECORD_FIELDS = (
    "bundle_path",
    "tip_sha",
    *_BUNDLE_TELEMETRY_FIELDS,
    "bundle_retention_count",
    "bundles_pruned",
)


def _with_bundle_telemetry(payload: dict[str, object]) -> dict[str, object]:
    """Total distinct rollback bundles, retaining missing telemetry as a signal [OBS-10].

    A bundle may appear in both reclaimed and deleted records, or in multiple
    reaper arms. Count its path once, including reused bundles and bundles whose
    subsequent deletion failed. This is artifact size, not bytes newly written.
    """

    def collect(aggregate: dict[str, object]) -> dict[str, int]:
        bundles: dict[str, int] = {}
        for key, value in list(aggregate.items()):
            if key in ("registry_sweep", "branch_reclaim") and isinstance(value, dict):
                bundles.update(collect(value))
                if value.get("telemetry_incomplete"):
                    aggregate["telemetry_incomplete"] = True
            elif isinstance(value, list):
                for record in value:
                    if not isinstance(record, dict):
                        continue
                    deleted = key == "deleted_branches" or (
                        key == "drained" and record.get("delete_reason") == "deleted"
                    )
                    if deleted or "bundle_path" in record:
                        missing = [field for field in _BUNDLE_TELEMETRY_FIELDS if record.get(field) is None]
                        if missing:
                            record["telemetry_incomplete"] = missing
                            aggregate["telemetry_incomplete"] = True
                    size = record.get("bundle_bytes")
                    path = record.get("bundle_path")
                    if isinstance(path, str) and path and type(size) is int and size >= 0:
                        bundles[path] = size
        # Empty/skipped nested arms have no bundle evidence to enrich [OBS-08].
        # Public aggregate builders still report their own zero total.
        if (
            aggregate is payload
            or bundles
            or aggregate.get("telemetry_incomplete")
            or "bundle_bytes_total" in aggregate
        ):
            aggregate["bundle_bytes_total"] = sum(bundles.values())
        return bundles

    collect(payload)
    return payload


def _with_branch_reclaim_drain(
    payload: dict[str, object],
    *,
    apply: bool,
    task_ref: str | None,
    orchestrator_root: Path | str | None,
    bundle_retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
) -> dict[str, object]:
    try:
        payload["terminal_branch_producer"] = _produce_terminal_branch_reclaims(
            apply=apply,
            task_ref=task_ref,
            orchestrator_root=orchestrator_root,
        )
    except Exception as exc:  # noqa: BLE001 — keep the existing drain available after producer faults
        payload["terminal_branch_producer"] = _terminal_branch_producer_error(exc)
    producer = cast(dict[str, object], payload["terminal_branch_producer"])
    if producer.get("ok") is False:
        payload["ok"] = False
        payload["degraded"] = True
        # Existing CLI and daemon telemetry consume these top-level fields.
        payload.setdefault("error", producer.get("error") or "terminal branch producer failed")
        payload.setdefault("error_type", producer.get("error_type") or "TerminalBranchProducerError")
    try:
        payload["branch_reclaim"] = drain_branch_reclaim_queue(
            apply=apply,
            task_ref=task_ref,
            orchestrator_root=orchestrator_root,
            bundle_retention_count=bundle_retention_count,
        )
    except Exception as exc:  # noqa: BLE001 — per-arm degrade [RES-07]
        payload["branch_reclaim"] = {"ok": False, "applied": apply, "error": str(exc)}
    refusals = [
        {"branch": record.get("branch"), "reason": record.get("reap_detail") or "verified_bundle_required"}
        for record in payload["branch_reclaim"].get("skipped", [])
        if isinstance(record, dict) and record.get("reap_guard") == "verified_bundle_required"
    ]
    if refusals:
        payload["bundle_refusals"] = refusals
        payload["degraded"] = True
        payload["ok"] = False
    return _with_bundle_telemetry(payload)


def _shape_reap_row_entry(
    conn: sqlite3.Connection | None,
    row: Mapping[str, object],
    *,
    clock: datetime,
    reports: Sequence[Mapping[str, object]] | None,
) -> dict[str, object]:
    """Shape one worktree_lanes row into the blocked-lane reaper entry dict."""
    lane_id = _normalize_optional_text(row["lane_id"]) or str(row["id"])
    row_task = _normalize_optional_text(row["task_ref"]) or "?"
    status = str(row["status"] or "")
    return {
        "id": row["id"],
        "task_ref": row_task,
        "lane_id": lane_id,
        "status": status,
        "worktree_path": row["worktree_path"],
        "branch": row["branch"],
        "updated_at": row["updated_at"],
        "created_at": row["created_at"],
        "age": format_lane_age_label(row["updated_at"], row["created_at"], now=clock),
        "blocker": _last_blocker_text_for_lane(
            conn,
            task_ref=row_task,
            lane_id=lane_id,
            notes=row["notes"],
            reports=reports,
        ),
        "notes": row["notes"],
    }


# A scan budget is independent of the mutation budget: ambiguous rows consume
# probe time too. Persist continuation so neither an ambiguous prefix nor an
# exhausted mutation budget can monopolize every daemon tick [CARD-09].
_BLOCKED_LANE_SCAN_LIMIT = 16
_BLOCKED_LANE_SCAN_SESSION = "blocked-lane-reap-scan-v1"


def _blocked_lane_scan_key(*, apply: bool, reclaim_worktrees: bool) -> str:
    # Preview and close-only scans must not move an apply/reclaim scan's cursor.
    return f"page-cursor:apply={int(apply)}:reclaim={int(reclaim_worktrees)}"


def _load_blocked_lane_scan_cursor(conn: sqlite3.Connection, *, scope: str, key: str) -> int:
    try:
        row = conn.execute(
            "SELECT rationale FROM decisions WHERE task_ref = ? AND session = ? AND decision = ?",
            (scope, _BLOCKED_LANE_SCAN_SESSION, key),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if str(exc) != "no such table: decisions":
            raise
        return 0
    try:
        return max(0, int(row["rationale"])) if row is not None else 0
    except (TypeError, ValueError):
        return 0


def _load_blocked_lane_deferred_ids(conn: sqlite3.Connection, *, scope: str, key: str) -> list[int]:
    try:
        row = conn.execute(
            "SELECT rationale FROM decisions WHERE task_ref = ? AND session = ? AND decision = ?",
            (scope, _BLOCKED_LANE_SCAN_SESSION, key + ":deferred"),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if str(exc) != "no such table: decisions":
            raise
        return []
    try:
        ids = json.loads(row["rationale"]) if row is not None else []
        if isinstance(ids, list) and all(type(value) is int and value > 0 for value in ids):
            return ids[:_BLOCKED_LANE_SCAN_LIMIT]
    except (TypeError, ValueError):
        pass
    return []


def _store_blocked_lane_scan_cursor(*, scope: str, key: str, last_id: int, deferred_ids: list[int]) -> bool:
    # A short transaction ends before any subprocess or filesystem probe.
    # Scheduling metadata uses the existing decision ledger, like the branch
    # reclaim queue cursor; no new schema or destructive authority is needed.
    try:
        with _get_db_connection() as conn:
            conn.executemany(
                """
                INSERT INTO decisions (task_ref, session, decision, rationale, decision_origin)
                VALUES (?, ?, ?, ?, 'system')
                ON CONFLICT(task_ref, decision, session)
                DO UPDATE SET rationale = excluded.rationale
                """,
                [
                    (scope, _BLOCKED_LANE_SCAN_SESSION, key, str(last_id)),
                    (scope, _BLOCKED_LANE_SCAN_SESSION, key + ":deferred", json.dumps(deferred_ids)),
                ],
            )
    except sqlite3.OperationalError as exc:
        if str(exc) != "no such table: decisions":
            raise
        return False
    return True


def reap_blocked_lanes(
    *,
    apply: bool = False,
    max_batch: int = DEFAULT_BLOCKED_LANE_REAP_BATCH,
    worktree_probe: Callable[[object], bool | None] | None = None,
    branch_probe: Callable[[object], bool | None] | None = None,
    now: datetime | None = None,
    min_age_hours: float = 24.0,
    task_ref: str | None = None,
    reclaim_worktrees: bool = False,
    reclaim_repo_root: Path | str | None = None,
    worktree_owner_probe: Callable[[object], tuple[str, str]] | None = None,
    bundle_retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
) -> dict:
    """Report non-terminal lane age/task/blocker; CAS-close conclusive-dead to ``closed_stale``.

    Naming note: historically this reaper only selected ``status='blocked'``
    (hence the name). The close candidate set is now every **non-terminal**
    status. When worktree reclaim is explicitly enabled, terminal rows with a
    recorded worktree are also scanned, but only by the guarded reclaim arm;
    they are never re-closed merely because their lane status is terminal.
    The rename blast radius across daemon/tests is deferred; the name
    understates the widened scope.

    Candidate grace: ``min_age_hours`` (default 24) requires
    ``COALESCE(updated_at, created_at)`` to be at least that many hours older
    than wall-clock now. **Blocked rows are exempt** — they remain admissible
    with no age floor so existing blocked-lane aging behaviour is unchanged.
    The grace exists so a freshly upserted non-blocked row (no worktree/branch
    yet) is not treated as conclusive-dead within seconds of creation.

    Conclusive-dead requires **both** worktree-gone **and** branch merged/deleted.
    Ambiguous (only one condition, or probe unavailable) → report only, never close.
    Dry-run by default (``apply=False``). Never raises.

    Worktree reclamation (``reclaim_worktrees``, default **off**) is
    operator-opt-in: the background daemon must not delete worktrees every
    tick. The CLI ``lane-reap --reclaim-worktrees`` flag (and make
    ``lane-reap`` via ``REAP_ARGS``) passes ``reclaim_worktrees=True``
    explicitly. When enabled it breaks the one ambiguity that could never
    resolve on its own: branch merged, worktree still on disk. Nothing else in
    the repo removes a lane worktree, so such a row sat ``ambiguous`` on every
    tick forever. For those rows only, the reaper attempts a guarded
    ``git worktree remove`` (see :func:`_reclaim_lane_worktree` for the
    guards), then RE-PROBES the path and re-runs the pure classifier — the
    verdict is re-derived from evidence, never assumed from the removal. Every
    other ambiguity class is untouched. ``apply=False`` only ever reports
    (``would_reclaim``). ``worktree_owner_probe`` and ``reclaim_repo_root`` are
    injection seams for tests; production samples ``ps``/``lsof`` live.
    Terminal rows pass through that same guard, including its SHA-landed
    survival proof: terminal status alone never authorizes deletion. Tree
    equality remains diagnostic and is not reclaim authority.

    ``max_batch`` caps rows that can make progress (a close or a guarded
    reclaim). A separate 16-row scan page bounds probe work per sweep. Pages
    rotate by durable row id with a persisted cursor per task and scan mode;
    blocked/oldest priority applies within each page. Permanently ambiguous
    rows therefore cannot starve later candidates. Dry-run persists only this
    scheduling metadata, never lifecycle or worktree mutations.

    Probes (git/ps/lsof/heartbeat) run with **no write transaction open**.
    Row updates use short per-row write transactions; the CAS guard already
    protects against concurrent movement.

    Apply-path closes take a durable leased reaping claim (exclusive flock on
    ``worker-<lane_id>.lock`` plus a ``claim=reaping`` payload) *before* the
    close-time liveness re-probe and hold it through CAS. Dispatch
    rematerialize takes the same flock, so a missing worktree cannot be
    rebuilt in the recency-probe → close window. A held worker lock on a
    conclusive-dead row is treated as live (skip close).

    Pass ``task_ref`` to scope candidates (and closes) to one task; omit for
    repo-wide sweep. Scoped filter applies to the SELECT that drives both
    reporting and closing — a scoped call cannot close foreign rows.
    The registry-driven git sweep is skipped when ``task_ref`` is set: it has
    no task bound and would otherwise garbage-collect merged refs repo-wide.
    """
    try:
        batch = max(1, int(max_batch))
    except (TypeError, ValueError):
        batch = DEFAULT_BLOCKED_LANE_REAP_BATCH
    try:
        age_h = float(min_age_hours)
    except (TypeError, ValueError):
        age_h = 24.0
    if age_h < 0:
        age_h = 0.0
    # SQLite datetime modifier, e.g. "-24.0 hours".
    age_mod = f"-{age_h} hours"
    scoped = _normalize_optional_text(task_ref)
    scan_scope = scoped or ""
    scan_key = _blocked_lane_scan_key(apply=apply, reclaim_worktrees=reclaim_worktrees)
    # A1: one clock sample drives both grace SQL and age labels so the two
    # cannot disagree across a SQLite ``datetime('now')`` vs Python ``now()``.
    clock = now if now is not None else datetime.now(timezone.utc)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    else:
        clock = clock.astimezone(timezone.utc)
    clock_sqlite = clock.strftime("%Y-%m-%d %H:%M:%S")

    reported: list[dict[str, object]] = []
    closed: list[dict[str, object]] = []
    would_close: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    alive: list[dict[str, object]] = []
    triage: list[str] = []
    failed: list[dict[str, object]] = []
    reclaimed: list[dict[str, object]] = []
    would_reclaim: list[dict[str, object]] = []
    deferred: list[dict[str, object]] = []
    budget_deferred_ids: list[int] = []
    progress_candidates = 0

    path_probe = worktree_probe or _probe_worktree_gone
    br_probe = branch_probe or _probe_branch_dead

    def _attach_registry_sweep(payload: dict) -> dict:
        # Always carry the reclaim arm so callers can distinguish disabled
        # work from a sweep that ran and found nothing [OBS-08].
        if not reclaim_worktrees:
            payload["registry_sweep"] = {
                "ok": True,
                "applied": apply,
                "skipped": "reclaim_worktrees_disabled",
            }
        elif scoped is not None:
            # The registry has no task_ref; scoping is a blast-radius cap.
            payload["registry_sweep"] = {"skipped": "scoped_call"}
        else:
            try:
                payload["registry_sweep"] = reap_merged_registry_worktrees(
                    apply=apply,
                    repo_root=reclaim_repo_root,
                    owner_probe=worktree_owner_probe,
                    # One grace, both arms. The row arm refuses to collect a
                    # freshly upserted row; the registry arm -- the one that
                    # deletes files -- must not be the looser of the two.
                    min_age_hours=age_h,
                    bundle_retention_count=bundle_retention_count,
                )
            except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]
                payload["registry_sweep"] = {"ok": False, "applied": apply, "error": str(exc)}
        # The CLI branches on the *top-level* ``failed``/``error`` keys, so a
        # sweep that refused every branch delete (unwritable state dir, full
        # disk, bundle verify failure) would print its refusals nested and still
        # exit 0. Surface a typed count the caller can branch on without parsing
        # message text [AGT-21][OBS-08].
        sweep = payload.get("registry_sweep")
        if isinstance(sweep, dict):
            # Row reclamation can remove the worktree before the registry arm
            # bundles its now-unchecked-out branch. Attach that rollback handle
            # to every row view while preserving the row's original verdict.
            rollback_by_branch = {
                record["branch"]: {key: record[key] for key in _BUNDLE_RECORD_FIELDS if key in record}
                for shape in ("reclaimed", "failed", "deleted_branches")
                for record in sweep.get(shape, [])
                if isinstance(record, dict) and record.get("branch") and record.get("bundle_path")
            }
            for shape in ("reported", "closed", "would_close", "ambiguous", "alive", "failed", "reclaimed"):
                for record in payload.get(shape, []):
                    record.update(rollback_by_branch.get(record.get("branch"), {}))
            sweep_failed = sweep.get("failed")
            sweep_failed = sweep_failed if isinstance(sweep_failed, list) else []
            payload["registry_sweep_failed_count"] = len(sweep_failed)
            # A hard top-level sweep failure is degraded even when no per-entry
            # rows were produced. Preserve the historical exit-0 contract only
            # for the typed no-repository result used by package-less fixtures.
            tolerated_no_repo = sweep.get("error") == "worktree_registry_unavailable"
            hard_sweep_failure = (sweep.get("ok") is False or bool(sweep.get("error"))) and not tolerated_no_repo
            if sweep_failed or hard_sweep_failure:
                payload["registry_sweep_degraded"] = True
        return payload

    scan_cursor_persisted = True

    def _finalize_blocked_lane_envelope(payload: dict[str, object]) -> dict[str, object]:
        """Attach both reclaim arms even when the close arm degraded."""
        if scan_cursor_persisted is False:
            # Scheduling metadata cannot authorize or veto reclamation.
            payload["scan_cursor"] = {"persisted": False, "reason": "decisions_table_missing"}
        if reclaim_worktrees:
            payload["reclaim_counts"] = {
                shape: len(payload.get(shape) or [])
                for shape in ("reclaimed", "failed", "ambiguous", "would_reclaim", "deferred")
            }
        return _json_response(
            _with_branch_reclaim_drain(
                _attach_registry_sweep(payload),
                apply=apply,
                task_ref=task_ref,
                orchestrator_root=reclaim_repo_root,
                bundle_retention_count=bundle_retention_count,
            )
        )

    materialized: list[tuple[dict[str, object], dict[str, object]]] = []
    pending_closes: list[tuple[dict[str, object], dict[str, object], str, Any]] = []

    try:
        empty_payload: dict[str, object] | None = None
        with _get_db_connection() as conn:
            # Terminal set shared with the archived-orphan reaper (single source).
            # Defined later in this module; looked up at call time after import.
            terminals = _ARCHIVED_ORPHAN_TERMINAL_STATUSES
            placeholders = ", ".join("?" for _ in terminals)
            params: list[object] = list(terminals)
            params.append(_LANE_STATUS_BLOCKED)
            params.append(clock_sqlite)
            params.append(age_mod)
            terminal_reclaim_sql = ""
            if reclaim_worktrees:
                reclaim_terminals = tuple(sorted(_RECLAIM_ELIGIBLE_TERMINAL_STATUSES))
                terminal_placeholders = ", ".join("?" for _ in reclaim_terminals)
                terminal_reclaim_sql = (
                    "OR (COALESCE(status, '') IN ("
                    + terminal_placeholders
                    + ") AND TRIM(COALESCE(worktree_path, '')) != '' "
                    "AND datetime(COALESCE(updated_at, created_at)) <= datetime(?, ?)) "
                )
                params.extend(reclaim_terminals)
                params.extend((clock_sqlite, age_mod))
            scope_sql = ""
            if scoped is not None:
                scope_sql = "AND task_ref = ? "
                params.append(scoped)
            after_id = _load_blocked_lane_scan_cursor(conn, scope=scan_scope, key=scan_key)
            deferred_ids = _load_blocked_lane_deferred_ids(conn, scope=scan_scope, key=scan_key)
            deferred_rank = {lane_pk: index for index, lane_pk in enumerate(deferred_ids)}
            deferred_sql = ""
            if deferred_ids:
                deferred_placeholders = ", ".join("?" for _ in deferred_ids)
                deferred_sql = f"CASE WHEN id IN ({deferred_placeholders}) THEN 0 ELSE 1 END ASC, "
                params.extend(deferred_ids)
            params.extend((after_id, _BLOCKED_LANE_SCAN_LIMIT + 1))
            # Circular id ordering tolerates deleted/ineligible cursor rows.
            # One lookahead row signals more work without probing that row.
            rows = conn.execute(
                f"""
                SELECT id, task_ref, lane_id, title, objective, worktree_path, branch,
                       owner_agent, status, notes, created_at, updated_at,
                       (SELECT target_branch FROM handoff_state
                        WHERE handoff_state.task_ref = worktree_lanes.task_ref) AS target_branch
                FROM worktree_lanes
                WHERE (
                    (COALESCE(status, '') NOT IN ({placeholders})
                     AND (
                       status = ?
                       OR datetime(COALESCE(updated_at, created_at))
                          <= datetime(?, ?)
                     ))
                    {terminal_reclaim_sql}
                  )
                  {scope_sql}
                ORDER BY {deferred_sql}CASE WHEN id > ? THEN 0 ELSE 1 END ASC, id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            if len(rows) > _BLOCKED_LANE_SCAN_LIMIT:
                deferred.append({**dict(rows[-1]), "reason": "scan limit reached; resume next sweep"})
                rows = rows[:_BLOCKED_LANE_SCAN_LIMIT]
            fresh_rows = [row for row in rows if row["id"] not in deferred_rank]
            last_scanned_id = int(fresh_rows[-1]["id"]) if fresh_rows else after_id
            # Retry budget-deferred actions in their prior order before fresh
            # priorities. Otherwise persistent previews or CAS misses reset
            # priority every sweep and starve the rest of this bounded page.
            # Only fresh rows advance the scan ring [CARD-09].
            rows = sorted(
                rows,
                key=lambda row: (
                    deferred_rank.get(row["id"], len(deferred_rank)),
                    row["status"] != _LANE_STATUS_BLOCKED,
                    str(row["updated_at"] or row["created_at"] or ""),
                    row["id"],
                ),
            )

            if not rows:
                # Build the empty payload here, but attach the registry sweep
                # only after this connection closes. Git/flock/remove/delete
                # must not run while the SELECT context is held.
                empty_payload = {
                    "ok": True,
                    "applied": apply,
                    "max_batch": batch,
                    "truncated": False,
                    "reported": [],
                    "closed": [],
                    "would_close": [],
                    "ambiguous": [],
                    "alive": [],
                    "triage": [],
                    "failed": [],
                    "reclaimed": [],
                    "would_reclaim": [],
                    "deferred": [],
                    "dashboard_lines": [],
                }
            else:
                lane_maps = [dict(row) for row in rows]
                # Pull recent reports once for blocker-text fallback.
                try:
                    report_rows = conn.execute(
                        """
                        SELECT task_ref, lane_id, blockers_json, created_at
                        FROM worker_reports
                        ORDER BY created_at DESC, id DESC
                        LIMIT 100
                        """
                    ).fetchall()
                    reports = [dict(r) for r in report_rows]
                except sqlite3.Error:
                    reports = []

                entries = (
                    collect_blocked_lane_aging_entries(
                        lane_maps,
                        reports=reports,
                        now=clock,
                        conn=conn,
                    )
                    if rows
                    else []
                )
                by_pk = {int(cast(int, e["id"])): e for e in entries if e.get("id") is not None}

                for row in rows:
                    entry = by_pk.get(int(row["id"]))
                    if entry is None:
                        # Non-blocked candidate admitted by the widened query;
                        # collect_blocked_lane_aging_entries only shapes blocked rows.
                        entry = _shape_reap_row_entry(conn, row, clock=clock, reports=reports)
                    materialized.append((dict(row), dict(entry)))

        if empty_payload is not None:
            return _finalize_blocked_lane_envelope(empty_payload)

        # Probe phase: no write transaction is open. Git/ps/lsof/heartbeat
        # subprocesses must not run while this process holds the WAL writer.
        for row, entry in materialized:
            reported.append(entry)
            terminal_row = str(row["status"] or "") in _ARCHIVED_ORPHAN_TERMINAL_STATUSES
            reclaim_only = (
                reclaim_worktrees
                and str(row["status"] or "") in _RECLAIM_ELIGIBLE_TERMINAL_STATUSES
                and lane_status_is_reclaim_eligible(row["status"])
            )
            integration_ref = _normalize_optional_text(row.get("target_branch")) or _RECLAIM_INTEGRATION_REF
            integration_ref_error: str | None = None
            if reclaim_worktrees:
                integration_ref, integration_ref_error = _reclaim_integration_ref(
                    task_ref=row.get("task_ref"),
                    target_branch=row.get("target_branch"),
                    repo_root=reclaim_repo_root,
                )
            row_progress_charged = False
            try:
                worktree_gone = path_probe(row["worktree_path"])
            except Exception as exc:  # noqa: BLE001 — per-row degrade
                worktree_gone = None
                triage.append(f"lane {entry['lane_id']}: worktree probe raised: {exc}")
            try:
                if reclaim_only and integration_ref_error is not None:
                    branch_dead = None
                elif reclaim_only:
                    branch_dead = _probe_branch_dead(
                        row["branch"],
                        repo_root=Path(reclaim_repo_root) if reclaim_repo_root is not None else None,
                        integration_ref=integration_ref,
                    )
                else:
                    branch_dead = br_probe(row["branch"])
            except Exception as exc:  # noqa: BLE001 — per-row degrade
                branch_dead = None
                triage.append(f"lane {entry['lane_id']}: branch probe raised: {exc}")

            verdict, reason = _classify_blocked_lane_liveness(
                worktree_gone=worktree_gone,
                branch_dead=branch_dead,
            )
            if verdict == "alive" and branch_dead is False:
                try:
                    tree_eq, tree_detail = _probe_branch_merged_by_tree(
                        row["branch"],
                        repo_root=reclaim_repo_root,
                    )
                except Exception as exc:  # noqa: BLE001 — per-row degrade
                    tree_eq, tree_detail = None, str(exc)
                    triage.append(f"lane {entry['lane_id']}: tree probe raised: {exc}")
                if tree_eq is True:
                    # Diagnostic only: do not treat tree equality as
                    # branch_dead or close/reclaim authority (AGT-10).
                    entry = {**entry, "merged_by_tree": True, "merged_by_tree_detail": tree_detail}
                # Probe error, trees differ, or tree-untouched unlanded
                # history: keep alive (fail-closed).
            entry = {
                **entry,
                "worktree_gone": worktree_gone,
                "branch_dead": branch_dead,
                "verdict": verdict,
                "reason": reason,
            }
            if reclaim_only:
                entry["reclaim_integration_ref"] = integration_ref
                if integration_ref_error is not None:
                    entry["reclaim_integration_ref_error"] = integration_ref_error
            # Refresh reported list item with probe fields.
            reported[-1] = entry

            if reclaim_only and worktree_gone is True:
                reason = "reclaim_candidate_worktree_already_gone"
                entry = {**entry, "verdict": "ambiguous", "reason": reason}
                reported[-1] = entry
                ambiguous.append(entry)
                continue

            if verdict == "alive" and not reclaim_only:
                alive.append(entry)
                continue

            claim_handle: Any | None = None
            claim_transferred = False
            try:
                will_reclaim = reclaim_worktrees and worktree_gone is False and (branch_dead is True or reclaim_only)
                if will_reclaim and progress_candidates >= batch:
                    deferred.append({**entry, "reason": "batch limit reached before guarded reclaim"})
                    budget_deferred_ids.append(int(row["id"]))
                    reported.pop()
                    continue
                if verdict == "dead" and not terminal_row and progress_candidates >= batch:
                    deferred.append({**entry, "reason": "batch limit reached before close"})
                    budget_deferred_ids.append(int(row["id"]))
                    reported.pop()
                    continue
                if apply and (will_reclaim or (verdict == "dead" and not terminal_row)):
                    claim_handle, claim_detail = _try_row_reaping_claim(entry.get("lane_id"))
                    if claim_handle is None:
                        # Exclusive claim failed on either arm: a live worker
                        # or in-flight rematerialize owns the lock, or we
                        # cannot resolve it. Do not reclaim or CAS-close a
                        # row we could not exclusively claim — the will_reclaim
                        # arm is the one that removes a worktree, and dropping
                        # the spanning flock there is the close-after-rebuild hole.
                        # Peer-held is retryable alive for close-path rows;
                        # reclaim-only rows report it as ambiguous. Structural
                        # failure is owned via a persisted blocker (OBS-08).
                        entry = _record_unclaimed_reaping_row(
                            entry=entry,
                            claim_detail=claim_detail,
                            alive=alive,
                            failed=failed,
                            ambiguous=ambiguous,
                            triage=triage,
                            sweep_label="blocked lane",
                            reclaim_only=reclaim_only,
                        )
                        reported[-1] = entry
                        continue

                # The one ambiguity that cannot resolve itself: the branch is
                # merged but the worktree is still on disk. Attempt a guarded
                # removal, then RE-PROBE and re-classify with the same pure
                # classifier — never assume the removal worked.
                # ``will_reclaim`` already names this arm; repeating the four
                # conjuncts here was an incidental +3 on the claim-protocol
                # delta (same decision, copied).
                reclaim_outcome = ""
                if will_reclaim:
                    if integration_ref_error is not None:
                        reclaim_outcome = "refused_merge_unknown"
                        reclaim_detail = integration_ref_error
                    else:
                        try:
                            reclaim_outcome, reclaim_detail = _reclaim_lane_worktree(
                                worktree_path=row["worktree_path"],
                                branch=row["branch"],
                                repo_root=reclaim_repo_root,
                                apply=apply,
                                owner_probe=worktree_owner_probe,
                                task_ref=_normalize_optional_text(entry.get("task_ref")),
                                lane_id=_normalize_optional_text(entry.get("lane_id")),
                                held_lock=claim_handle,
                                integration_ref=integration_ref,
                                allow_terminal_unmerged_bundled=reclaim_only,
                            )
                        except Exception as exc:  # noqa: BLE001 — per-row degrade
                            reclaim_outcome = "reclaim_probe_raised"
                            reclaim_detail = str(exc)
                    entry = {
                        **entry,
                        "worktree_reclaim": reclaim_outcome,
                        "worktree_reclaim_detail": reclaim_detail,
                        **_worktree_reclaim_release_fields(reclaim_detail),
                    }
                    rule_histogram = _ignored_rule_histogram_from_detail(reclaim_detail)
                    if rule_histogram:
                        entry["ignored_rule_histogram"] = rule_histogram
                    reported[-1] = entry
                    if reclaim_outcome == "reclaimed":
                        progress_candidates += 1
                        row_progress_charged = True
                        try:
                            worktree_gone = path_probe(row["worktree_path"])
                        except Exception as exc:  # noqa: BLE001 — per-row degrade
                            worktree_gone = None
                            triage.append(f"lane {entry['lane_id']}: worktree re-probe raised: {exc}")
                        verdict, reason = _classify_blocked_lane_liveness(
                            worktree_gone=worktree_gone,
                            branch_dead=branch_dead,
                        )
                        if entry.get("merged_by_tree") is True:
                            reason = f"merged_by_tree: {reason}"
                        entry = {
                            **entry,
                            "worktree_gone": worktree_gone,
                            "verdict": verdict,
                            "reason": reason,
                        }
                        reported[-1] = entry
                        reclaimed.append(entry)
                        if reclaim_only:
                            # The terminal candidate has exactly one outcome:
                            # the guarded removal succeeded. A failed outer
                            # liveness probe must not add a second ambiguous
                            # outcome after this stronger inner gate passed.
                            continue
                    elif reclaim_outcome != "would_reclaim":
                        triage.append(
                            f"blocked lane {entry['lane_id']} task={entry['task_ref']}: "
                            f"worktree reclaim {reclaim_outcome}: {reclaim_detail}"
                        )
                    if reclaim_only and reclaim_outcome not in {"reclaimed", "would_reclaim"}:
                        reclaim_reason = (
                            f"worktree reclaim {reclaim_outcome or 'failed'}: {reclaim_detail or 'no detail'}"
                        )
                        uncertain = (
                            "unknown" in reclaim_outcome
                            or "unavailable" in reclaim_outcome
                            or "timeout" in reclaim_outcome
                            or reclaim_outcome.endswith("_raised")
                            or reclaim_outcome
                            in {
                                "refused_unmerged",
                                "refused_merge_changed",
                                "refused_lock_held",
                                "refused_session_live",
                                "refused_not_linked_worktree",
                                "refused_repo_root",
                                "refused_repo_root_unresolved",
                                "skipped_no_worktree_path",
                                "refused_owned",
                            }
                        )
                        if uncertain:
                            entry = {**entry, "verdict": "ambiguous", "reason": reclaim_reason}
                            ambiguous.append(entry)
                        else:
                            entry = {**entry, "reason": reclaim_reason}
                            failed.append({**entry, "stage": "worktree_reclaim", "error": reclaim_reason})
                        reported[-1] = entry
                        continue

                if verdict != "dead":
                    if reclaim_outcome == "would_reclaim":
                        # Dry-run: record the preview outside the apply gate;
                        # otherwise this is structurally empty even when the
                        # exact same guard would authorize an apply run.
                        progress_candidates += 1
                        row_progress_charged = True
                        would_reclaim.append(entry)
                        if not terminal_row:
                            would_close.append(
                                {
                                    **entry,
                                    "note": (
                                        "closed_stale by blocked-lane reaper: would reclaim worktree then close_stale"
                                    ),
                                }
                            )
                        continue
                    if terminal_row and reclaim_outcome == "reclaimed":
                        # Its lifecycle row is already terminal. Reclaim was
                        # the only allowed transition for this sweep.
                        continue
                    ambiguous.append(entry)
                    triage.append(
                        f"blocked lane {entry['lane_id']} task={entry['task_ref']} age={entry['age']}: {reason}"
                    )
                    continue

                if terminal_row:
                    # A terminal row whose worktree is already gone needs no
                    # action. Terminal status alone never authorizes a second
                    # lifecycle transition.
                    continue

                note = f"closed_stale by blocked-lane reaper: {reason}"
                close_entry = {**entry, "note": note}
                would_close.append(close_entry)
                if not row_progress_charged:
                    progress_candidates += 1
                if apply:
                    pending_closes.append((row, close_entry, note, claim_handle))
                    claim_transferred = True
            finally:
                if claim_handle is not None and not claim_transferred:
                    _release_lane_worker_lock(claim_handle)

        # Persist continuation only after the probe phase, preserving the
        # no-write-before-reclaim ordering as well as the no-held-write-lock
        # invariant. Per-row probe failures still advance with the page.
        scan_cursor_persisted = _store_blocked_lane_scan_cursor(
            scope=scan_scope,
            key=scan_key,
            last_id=last_scanned_id,
            deferred_ids=budget_deferred_ids,
        )

        # Write phase: short per-row write transactions. The CAS predicate
        # already refuses if the row moved since the probe snapshot. The
        # reaping claim stays held across the recency re-probe and the CAS
        # so rematerialize cannot land in that window.
        if apply:
            for index, (row, close_entry, note, claim_handle) in enumerate(pending_closes):
                try:
                    row_status = str(row["status"] or _LANE_STATUS_BLOCKED)
                    # Recency re-probe BEFORE the write txn, while the leased
                    # reaping claim is still held. A worktree recreated (or
                    # branch revived) after the probe phase must not CAS-close.
                    raced = _cas_liveness_raced_reason(
                        worktree_path=row["worktree_path"],
                        branch=row["branch"],
                        branch_dead=close_entry.get("branch_dead"),
                        merged_by_tree=close_entry.get("merged_by_tree"),
                        path_probe=path_probe,
                        branch_probe=br_probe,
                        repo_root=reclaim_repo_root,
                    )
                    if raced is not None:
                        ambiguous.append({**close_entry, "reason": raced})
                        triage.append(
                            f"blocked lane {close_entry['lane_id']} task={close_entry['task_ref']}: {raced}; skipped"
                        )
                        continue
                    # Recency has already sampled disk. Hold the claim across
                    # this seam so rematerialize cannot rebuild before CAS.
                    _reaping_close_window_barrier()
                    try:
                        with _get_db_connection() as conn:
                            ok = _close_blocked_lane_cas(
                                conn,
                                lane_pk=int(cast(int, row["id"])),
                                probed_updated_at=row["updated_at"],
                                note=note,
                                expected_status=row_status,
                            )
                    except sqlite3.Error as exc:
                        failed.append({**close_entry, "stage": "close", "error": str(exc)})
                        continue
                    if ok:
                        closed.append({**close_entry, "status": _LANE_STATUS_CLOSED_STALE})
                    else:
                        ambiguous.append({**close_entry, "reason": "CAS miss: row changed since probe"})
                        triage.append(
                            f"blocked lane {close_entry['lane_id']} task={close_entry['task_ref']}: "
                            "CAS miss; re-probe next tick"
                        )
                finally:
                    if claim_handle is not None:
                        _release_lane_worker_lock(claim_handle)
                        pending_closes[index] = (row, close_entry, note, None)
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
        _release_pending_reaping_claims(pending_closes)
        triage.append(f"blocked-lane sweep failed: {exc}")
        return _finalize_blocked_lane_envelope(
            {
                "ok": True,
                "applied": apply,
                "max_batch": batch,
                "truncated": bool(deferred),
                "error": str(exc),
                "reported": reported,
                "closed": closed,
                "would_close": would_close,
                "ambiguous": ambiguous,
                "alive": alive,
                "triage": triage,
                "failed": failed,
                "reclaimed": reclaimed,
                "would_reclaim": would_reclaim,
                "deferred": deferred,
                "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
            }
        )

    payload = {
        "ok": True,
        "applied": apply,
        "max_batch": batch,
        # The lookup itself is deliberately unbounded. Truncation now means a
        # progress-capable row was reached but deferred by the mutation budget.
        "truncated": bool(deferred),
        "reported": reported,
        "closed": closed,
        "would_close": would_close,
        "ambiguous": ambiguous,
        "alive": alive,
        "triage": triage,
        "failed": failed,
        "reclaimed": reclaimed,
        "would_reclaim": would_reclaim,
        "deferred": deferred,
        "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
    }
    return _finalize_blocked_lane_envelope(payload)


# 0112 Bug 2: statuses a lane is already terminal in — never re-close these.
# Source of truth: CLOSEABLE_LANE_STATUSES (lanes.py:19) plus the reaper's
# closed_stale; 'archived' is a defensive extra (not a canonical LANE_STATUS).
_ARCHIVED_ORPHAN_TERMINAL_STATUSES: tuple[str, ...] = tuple(
    sorted(CLOSEABLE_LANE_STATUSES | {_LANE_STATUS_CLOSED_STALE, "archived"})
)

# Distinct from CLOSEABLE_LANE_STATUSES: leftover worktrees of successful
# terminals remain reclaim-eligible. Widening the closeable set would change
# closing semantics for callers that are correct today [OBS-08][RES-07].
_RECLAIM_ELIGIBLE_TERMINAL_STATUSES: frozenset[str] = frozenset({"merged", "closed", _LANE_STATUS_CLOSED_STALE})


def lane_status_is_reclaim_eligible(status: object) -> bool:
    """True when lane status membership does not forbid worktree reclaim.

    Closeability and reclaim eligibility are different questions. A row at
    ``merged`` is correctly closeable (and already terminal for closing) but
    must remain reclaim-eligible — otherwise success excludes the leftover
    worktree and reclaim counts read zero. Live statuses are also eligible;
    the liveness classifier is the remaining gate. ``archived`` is not.
    """
    text = _normalize_optional_text(status)
    if text is None:
        return False
    if text in _RECLAIM_ELIGIBLE_TERMINAL_STATUSES:
        return True
    return text not in _ARCHIVED_ORPHAN_TERMINAL_STATUSES


def _live_abandoned_lane_stale_eligibility(
    row: Mapping[str, object],
    *,
    repo_root: Path,
    now_epoch: float,
    reaper_claim_held: bool = False,
) -> tuple[bool, str]:
    """Probe every live-row condition before allowing a stale close."""
    from workbay_orchestrator_mcp.orchestration.lane_stale_close import (  # noqa: PLC0415
        absent_branch_stale_eligible,
    )

    worktree_value = row.get("worktree_path")
    worktree_text = _normalize_optional_text(worktree_value)
    worktree_exists = True
    absolute_worktree: Path | None = None
    if worktree_text is not None:
        absolute_worktree = Path(worktree_text)
        if not absolute_worktree.is_absolute():
            absolute_worktree = repo_root / absolute_worktree
        try:
            absolute_worktree.lstat()
        except FileNotFoundError:
            worktree_exists = False
        except OSError:
            worktree_exists = True

    branch_name = _recorded_branch_short_name(row.get("branch"))
    branch_absent: bool | None = None
    if branch_name:
        ref = f"refs/heads/{branch_name}"
        valid_ref = _run_reclaim_command(["git", "-C", str(repo_root), "check-ref-format", ref])
        if valid_ref is not None and valid_ref.returncode == 0:
            proc = _run_reclaim_command(
                ["git", "-C", str(repo_root), "for-each-ref", "--format=%(refname)", "refs/heads", "refs/remotes"]
            )
            if proc is not None and proc.returncode == 0:
                refs = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
                branch_absent = not any(
                    candidate == ref
                    or (candidate.startswith("refs/remotes/") and candidate.endswith(f"/{branch_name}"))
                    for candidate in refs
                )

    lane_id = _normalize_optional_text(row.get("lane_id"))
    driver_live: bool | None = None
    if lane_id:
        try:
            if reaper_claim_held:
                # The caller already owns this exact flock as its reaping
                # claim, so it must not block its own stale-close verdict.
                driver_live = False
            else:
                lock_path = _lane_worker_lock_path(lane_id)
                driver_live = True if lock_path is None else lock_is_held(lock_path)
        except Exception:  # noqa: BLE001 — unknown driver probe stays fail-closed
            driver_live = None

    session_live: bool | None = None
    task_ref = _normalize_optional_text(row.get("task_ref"))
    if absolute_worktree is not None and task_ref and lane_id:
        heartbeat_blocks, _heartbeat_detail = _session_heartbeat_blocks_reclaim(
            repo_root=repo_root,
            worktree_path=str(absolute_worktree),
            task_ref=task_ref,
            lane_id=lane_id,
        )
        session_live = heartbeat_blocks

    return absent_branch_stale_eligible(
        dict(row),
        now_epoch=now_epoch,
        worktree_exists=worktree_exists,
        driver_live=driver_live,
        branch_absent=branch_absent,
        session_live=session_live,
    )


_ARCHIVED_ORPHAN_SCAN_SESSION = "archived-orphan-lane-reap-scan-v1"
_ARCHIVED_ORPHAN_SCAN_PAGE_SIZE = 16
_ARCHIVED_ORPHAN_SCAN_LIMIT = 200
_ARCHIVED_ORPHAN_SCAN_TIME_S = 10.0


def _archived_orphan_scan_key(*, apply: bool) -> str:
    return f"ordering-cursor:apply={int(apply)}"


def _load_archived_orphan_scan_cursor(*, apply: bool) -> tuple[str, int] | None:
    try:
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT rationale FROM decisions WHERE task_ref = ? AND session = ? AND decision = ?",
                ("", _ARCHIVED_ORPHAN_SCAN_SESSION, _archived_orphan_scan_key(apply=apply)),
            ).fetchone()
    except sqlite3.OperationalError as exc:
        if str(exc) != "no such table: decisions":
            raise
        return None
    try:
        parsed = json.loads(row["rationale"]) if row is not None else None
        if (
            isinstance(parsed, list)
            and len(parsed) == 2
            and isinstance(parsed[0], str)
            and type(parsed[1]) is int
            and parsed[1] > 0
        ):
            return parsed[0], parsed[1]
    except (KeyError, TypeError, ValueError):
        pass
    return None


def _store_archived_orphan_scan_cursor(*, apply: bool, cursor: tuple[str, int] | None) -> bool:
    rationale = json.dumps(list(cursor)) if cursor is not None else "[]"
    try:
        with _get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO decisions (task_ref, session, decision, rationale, decision_origin)
                VALUES (?, ?, ?, ?, 'system')
                ON CONFLICT(task_ref, decision, session)
                DO UPDATE SET rationale = excluded.rationale
                """,
                ("", _ARCHIVED_ORPHAN_SCAN_SESSION, _archived_orphan_scan_key(apply=apply), rationale),
            )
    except sqlite3.OperationalError as exc:
        if str(exc) != "no such table: decisions":
            raise
        return False
    return True


def reap_task_archived_orphan_lanes(
    *,
    apply: bool = False,
    max_batch: int = DEFAULT_BLOCKED_LANE_REAP_BATCH,
    task_ref: str | None = None,
    now: datetime | None = None,
) -> dict:
    """CAS-close lanes whose task disappeared or whose live row is abandoned.

    Lanes with no corresponding live task row are orphans, whether the task
    was archived or its task row was removed. They are closed regardless of
    lane status, as in the original archive backstop. A still-live task row is
    only eligible when an active/planned lane is at least seven days old, its
    branch and worktree are both absent, and driver/session liveness probes are
    conclusively false. Pass ``task_ref`` to scope to one finishing task;
    omit it for an unscoped preview. Dry-run by default. Never raises. The CAS
    on ``updated_at`` catches a writer that already mutated the row; the lane
    claim protects against an in-flight rematerialize that has not written yet.
    Ambiguous live rows do not consume ``max_batch``. Candidate probes use a
    bounded keyset scan with a persisted ordering cursor, capped at 200 rows
    or ten seconds per sweep.
    """
    try:
        batch = max(1, int(max_batch))
    except (TypeError, ValueError):
        batch = DEFAULT_BLOCKED_LANE_REAP_BATCH

    clock = now or datetime.now(timezone.utc)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    from workbay_orchestrator_mcp.orchestration.lane_stale_close import (  # noqa: PLC0415
        ABSENT_BRANCH_LIVE_ABANDONED_MIN_AGE_S,
    )

    cutoff = (clock - timedelta(seconds=ABSENT_BRANCH_LIVE_ABANDONED_MIN_AGE_S)).strftime("%Y-%m-%d %H:%M:%S")
    now_epoch = clock.timestamp()

    reported: list[dict[str, object]] = []
    closed: list[dict[str, object]] = []
    would_close: list[dict[str, object]] = []
    # ``ambiguous`` contains old live rows that failed one of the required
    # abandoned-lane proofs; they stay visible without being closed.
    ambiguous: list[dict[str, object]] = []
    alive: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    triage: list[str] = []
    pending_closes: list[tuple[Any, dict[str, object], str, Any]] = []

    placeholders = ", ".join("?" for _ in _ARCHIVED_ORPHAN_TERMINAL_STATUSES)
    scoped = _normalize_optional_text(task_ref)
    order_key_sql = "COALESCE(datetime(COALESCE(wl.updated_at, wl.created_at)), '')"
    scan_cursor: tuple[str, int] | None = None
    cursor_to_store: tuple[str, int] | None = None
    scan_cursor_persisted = True
    scan_truncated = False

    def _fetch_candidate_page(
        *,
        after: tuple[str, int] | None,
        upper: tuple[str, int] | None,
        limit: int,
    ) -> list[Any]:
        params: list[object] = [*sorted(_ARCHIVED_ORPHAN_TERMINAL_STATUSES), "active", "planned", cutoff]
        scope_sql = ""
        if scoped is not None:
            scope_sql = "AND wl.task_ref = ? "
            params.append(scoped)
        keyset_sql = ""
        if after is not None:
            keyset_sql += f"AND ({order_key_sql} > ? OR ({order_key_sql} = ? AND wl.id > ?)) "
            params.extend((after[0], after[0], after[1]))
        if upper is not None:
            keyset_sql += f"AND ({order_key_sql} < ? OR ({order_key_sql} = ? AND wl.id <= ?)) "
            params.extend((upper[0], upper[0], upper[1]))
        params.append(limit)
        with _get_db_connection() as conn:
            return conn.execute(
                f"""
                SELECT id, task_ref, lane_id, worktree_path, branch, status, created_at, updated_at,
                       EXISTS (SELECT 1 FROM handoff_state hs WHERE hs.task_ref = wl.task_ref) AS owner_live,
                       EXISTS (SELECT 1 FROM task_archives ta WHERE ta.task_ref = wl.task_ref) AS owner_archived,
                       {order_key_sql} AS scan_order_key
                FROM worktree_lanes wl
                WHERE COALESCE(wl.status, '') NOT IN ({placeholders})
                  AND (
                    NOT EXISTS (SELECT 1 FROM handoff_state hs WHERE hs.task_ref = wl.task_ref)
                    OR (
                      LOWER(COALESCE(wl.status, '')) IN (?, ?)
                      AND datetime(COALESCE(NULLIF(wl.updated_at, ''), wl.created_at)) <= datetime(?)
                    )
                  )
                  {scope_sql}
                  {keyset_sql}
                ORDER BY {order_key_sql} ASC, wl.id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()

    try:
        initial_cursor = _load_archived_orphan_scan_cursor(apply=apply)
        scan_cursor = initial_cursor
        phase_after_cursor = initial_cursor is not None
        phase_cursor: tuple[str, int] | None = None
        visited_ids: set[int] = set()
        scanned_count = 0
        scan_started = time.monotonic()
        exhausted = False
        reached_batch = False
        while (
            scanned_count < _ARCHIVED_ORPHAN_SCAN_LIMIT
            and time.monotonic() - scan_started < _ARCHIVED_ORPHAN_SCAN_TIME_S
            and len(would_close) < batch
        ):
            page_limit = min(_ARCHIVED_ORPHAN_SCAN_PAGE_SIZE, _ARCHIVED_ORPHAN_SCAN_LIMIT - scanned_count)
            if phase_after_cursor:
                rows = _fetch_candidate_page(after=scan_cursor, upper=None, limit=page_limit)
                if not rows:
                    phase_after_cursor = False
                    phase_cursor = None
                    continue
            else:
                rows = _fetch_candidate_page(after=phase_cursor, upper=initial_cursor, limit=page_limit)
                if not rows:
                    exhausted = True
                    scan_cursor = None
                    break

            for candidate in rows:
                row = dict(candidate)
                row_id = int(row["id"])
                scanned_cursor = (str(row["scan_order_key"] or ""), row_id)
                scan_cursor = scanned_cursor
                if not phase_after_cursor:
                    phase_cursor = scanned_cursor
                scanned_count += 1
                if row_id in visited_ids:
                    continue
                visited_ids.add(row_id)

                lane_id = _normalize_optional_text(row["lane_id"]) or str(row["id"])
                ref = _normalize_optional_text(row["task_ref"]) or "?"
                status = str(row["status"] or "")
                entry: dict[str, object] = {
                    "id": row["id"],
                    "task_ref": ref,
                    "lane_id": lane_id,
                    "status": status,
                    "worktree_path": row["worktree_path"],
                    "branch": row["branch"],
                    "updated_at": row["updated_at"],
                }
                reported.append(entry)
                live_owner = bool(row.get("owner_live"))
                stale_reason: str | None = None
                if live_owner:
                    eligible, eligibility_reason = _live_abandoned_lane_stale_eligibility(
                        row,
                        repo_root=_workspace_root(),
                        now_epoch=now_epoch,
                    )
                    if not eligible:
                        ambiguous.append({**entry, "reason": eligibility_reason})
                        continue
                    stale_reason = "absent_branch_live_abandoned"
                    note = (
                        "closed_stale by absent-branch-live-abandoned reaper: "
                        f"branch/worktree absent for stale task {ref} (was {status})"
                    )
                elif bool(row.get("owner_archived")):
                    note = f"closed_stale by task-archived-orphan reaper: task {ref} archived (was {status})"
                else:
                    note = (
                        f"closed_stale by task-archived-orphan reaper: task {ref} missing from "
                        f"handoff_state and task_archives (was {status})"
                    )
                if not apply:
                    preview_entry = {**entry, "note": note}
                    if stale_reason is not None:
                        preview_entry["stale_reason"] = stale_reason
                    would_close.append(preview_entry)
                    if len(would_close) >= batch:
                        reached_batch = True
                        break
                    continue
                claim_handle, claim_detail = _try_row_reaping_claim(entry.get("lane_id"))
                if claim_handle is None:
                    # Same protocol as reap_blocked_lanes: do not CAS-close a row
                    # we could not exclusively claim. Rematerialize that has not
                    # written yet leaves updated_at unchanged; the flock is the
                    # barrier, not the CAS.
                    entry = _record_unclaimed_reaping_row(
                        entry=entry,
                        claim_detail=claim_detail,
                        alive=alive,
                        failed=failed,
                        triage=triage,
                        sweep_label="archived-orphan lane",
                    )
                    reported[-1] = entry
                    continue
                close_entry = {**entry, "note": note}
                if stale_reason is not None:
                    # Probe again while holding the per-lane claim so a liveness
                    # transition between preview and CAS cannot authorize a close.
                    eligible, eligibility_reason = _live_abandoned_lane_stale_eligibility(
                        row,
                        repo_root=_workspace_root(),
                        now_epoch=datetime.now(timezone.utc).timestamp(),
                        reaper_claim_held=True,
                    )
                    if not eligible:
                        ambiguous.append({**entry, "reason": eligibility_reason})
                        _release_lane_worker_lock(claim_handle)
                        continue
                    close_entry["stale_reason"] = stale_reason
                would_close.append(close_entry)
                pending_closes.append((row, close_entry, note, claim_handle))
                if len(would_close) >= batch:
                    reached_batch = True
                    break

        scan_budget_reached = scanned_count >= _ARCHIVED_ORPHAN_SCAN_LIMIT or (
            time.monotonic() - scan_started >= _ARCHIVED_ORPHAN_SCAN_TIME_S
        )
        if not exhausted and not reached_batch and scan_budget_reached:
            # One lookahead is enough to distinguish a full bounded scan from
            # exhaustion. This check does not run filesystem or process probes.
            if phase_after_cursor:
                more_rows = _fetch_candidate_page(after=scan_cursor, upper=None, limit=1)
                if not more_rows and initial_cursor is not None:
                    more_rows = _fetch_candidate_page(after=None, upper=initial_cursor, limit=1)
            else:
                more_rows = _fetch_candidate_page(after=phase_cursor, upper=initial_cursor, limit=1)
            if not more_rows:
                exhausted = True
                scan_cursor = None
        cursor_to_store = None if exhausted else scan_cursor
        scan_truncated = reached_batch or (scan_budget_reached and not exhausted)

        # Write phase: claim stays held across the CAS transaction, then
        # released — mirror reap_blocked_lanes 1584-1589.
        if apply:
            for index, (row, close_entry, note, claim_handle) in enumerate(pending_closes):
                try:
                    row_status = str(row["status"] or "")
                    try:
                        if close_entry.get("stale_reason") == "absent_branch_live_abandoned":
                            from workbay_orchestrator_mcp.orchestration.lane_stale_close import (  # noqa: PLC0415
                                StaleReason,
                                close_lane_stale,
                            )

                            close_result = close_lane_stale(
                                task_ref=str(close_entry["task_ref"]),
                                lane_id=str(close_entry["lane_id"]),
                                reason=StaleReason.ABSENT_BRANCH_LIVE_ABANDONED,
                                notes=note,
                                expected_status=row_status,
                                expected_updated_at=str(row["updated_at"] or ""),
                                expected_row=dict(row),
                            )
                            ok = close_result.get("ok") is True
                            close_error = close_result.get("error")
                        else:
                            with _get_db_connection() as conn:
                                ok = _close_blocked_lane_cas(
                                    conn,
                                    lane_pk=int(cast(int, row["id"])),
                                    probed_updated_at=row["updated_at"],
                                    note=note,
                                    expected_status=row_status,
                                )
                            close_error = None
                    except Exception as exc:  # noqa: BLE001 — retain the claim's best-effort CAS contract
                        failed.append({**close_entry, "error": str(exc)})
                        continue
                    if ok:
                        closed.append({**close_entry, "status": _LANE_STATUS_CLOSED_STALE})
                    elif close_error:
                        triage.append(
                            f"orphan lane {close_entry['lane_id']} task={close_entry['task_ref']}: {close_error}"
                        )
                    else:
                        triage.append(
                            f"orphan lane {close_entry['lane_id']} "
                            f"task={close_entry['task_ref']}: CAS miss; re-probe next tick"
                        )
                finally:
                    if claim_handle is not None:
                        _release_lane_worker_lock(claim_handle)
                        pending_closes[index] = (row, close_entry, note, None)
        scan_cursor_persisted = _store_archived_orphan_scan_cursor(apply=apply, cursor=cursor_to_store)
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
        _release_pending_reaping_claims(pending_closes)
        triage.append(f"task-archived-orphan sweep failed: {exc}")
        return _json_response(
            {
                "ok": True,
                "applied": apply,
                "max_batch": batch,
                # Computed here — do not omit for a helper to invent False.
                "truncated": scan_truncated,
                "error": str(exc),
                "reported": reported,
                "closed": closed,
                "would_close": would_close,
                "ambiguous": ambiguous,
                "ambiguous_skipped": len(ambiguous),
                "alive": alive,
                "failed": failed,
                "triage": triage,
                "scan_cursor": {"persisted": scan_cursor_persisted},
            }
        )

    return _json_response(
        {
            "ok": True,
            "applied": apply,
            "max_batch": batch,
            # A full eligibility batch or exhausted bounded scan can leave
            # candidates for a later sweep. Ambiguous probes do not consume the
            # mutation batch, and the keyset cursor rotates past them.
            "truncated": scan_truncated,
            "reported": reported,
            "closed": closed,
            "would_close": would_close,
            "ambiguous": ambiguous,
            "ambiguous_skipped": len(ambiguous),
            "alive": alive,
            "failed": failed,
            "triage": triage,
            "scan_cursor": {"persisted": scan_cursor_persisted},
        }
    )


# ---------------------------------------------------------------------------
# Registry-driven sweep: git is the candidate source, not lane rows.
#
# The row-driven reaper above can only see non-terminal lane rows, but the
# normal lifecycle closes a lane's row at merge time — so exactly the leaked
# resources (merged branch, surviving worktree, row already terminal) were
# never scanned and nothing on disk was ever reclaimed. The DB is a
# materialized view that loses its rows before the reaper looks; the worktree
# registry and refs/heads are the authority. Ordering matters: the worktree
# is removed first (which releases the checked-out branch), then the branch
# is deleted with ``git branch -d`` from the integration root — git's own
# unmerged refusal is the final fail-safe under every earlier guard.
# ---------------------------------------------------------------------------


def _parse_worktree_registry(porcelain: str) -> list[dict[str, object]]:
    """Parse ``git worktree list --porcelain`` into path/branch/detached rows."""
    entries: list[dict[str, object]] = []
    cur: dict[str, object] = {}
    for raw in porcelain.splitlines():
        line = raw.strip()
        if not line:
            if cur.get("path"):
                entries.append(cur)
            cur = {}
            continue
        if line.startswith("worktree "):
            cur = {"path": line[len("worktree ") :], "branch": None, "detached": False}
        elif line.startswith("branch "):
            ref = line[len("branch ") :]
            if ref.startswith("refs/heads/"):
                ref = ref[len("refs/heads/") :]
            cur["branch"] = ref
        elif line == "detached":
            cur["detached"] = True
    if cur.get("path"):
        entries.append(cur)
    return entries


def _branch_is_at_integration_tip(root: Path, branch: str, integration_ref: str) -> bool | None:
    """True when ``branch`` names the same commit as ``integration_ref``.

    Ancestry alone cannot tell a lane whose work landed from a lane that never
    started: a branch cut from ``main`` and not yet committed to is *trivially*
    an ancestor of ``main``, so the merged probe reports "done" about a worktree
    that is one minute old. The two cases differ in one observable -- real
    landed work leaves the integration ref ahead of (or merged past) the lane
    tip, while a never-started lane sits exactly *on* it. Refusing the
    degenerate equal-tip case costs only a fast-forwarded branch's reclamation,
    deferred to the next tick after the integration ref moves, and buys back the
    live worktree this sweep would otherwise delete [OBS-12].

    ``None`` means unknown, which the caller treats as a refusal [SECD-05].
    """
    tips = []
    for ref in (f"refs/heads/{branch}", integration_ref):
        proc = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--verify", f"{ref}^{{commit}}"])
        if proc is None or getattr(proc, "returncode", None) != 0:
            return None
        tip = _probe_command_stdout(proc).strip()
        if not tip:
            return None
        tips.append(tip)
    return tips[0] == tips[1]


def _reclaim_rev_parse(root: Path, ref: str) -> str | None:
    proc = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--verify", f"{ref}^{{commit}}"])
    if proc is None or getattr(proc, "returncode", None) != 0:
        return None
    sha = _probe_command_stdout(proc).strip().lower()
    return sha or None


def _reclaim_rev_list_count(root: Path, start: str, end: str) -> int | None:
    proc = _run_reclaim_command(["git", "-C", str(root), "rev-list", "--count", f"{start}..{end}"])
    if proc is None or getattr(proc, "returncode", None) != 0:
        return None
    raw = _probe_command_stdout(proc).strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _reclaim_first_parent_lineage(root: Path, tip_sha: str, integration_ref: str) -> bool | None:
    integ_sha = _reclaim_rev_parse(root, integration_ref)
    if not integ_sha:
        return None
    if tip_sha.lower() == integ_sha:
        return True
    exclusive = _reclaim_rev_list_count(root, tip_sha, integ_sha)
    if exclusive is None or exclusive < 0:
        return None
    proc = _run_reclaim_command(["git", "-C", str(root), "rev-list", "--first-parent", f"-n{exclusive + 1}", integ_sha])
    if proc is None or getattr(proc, "returncode", None) != 0:
        return None
    lineage = {line.strip().lower() for line in _probe_command_stdout(proc).splitlines() if line.strip()}
    return tip_sha.lower() in lineage


def _reclaim_committer_epoch(root: Path, sha: str) -> int | None:
    proc = _run_reclaim_command(["git", "-C", str(root), "log", "-1", "--format=%ct", sha])
    if proc is None or getattr(proc, "returncode", None) != 0:
        return None
    raw = _probe_command_stdout(proc).strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _worktree_registration_epoch(worktree_path: str) -> int | None:
    try:
        git_marker = Path(worktree_path) / ".git"
        if not git_marker.is_file():
            return None
        lines = git_marker.read_text(encoding="utf-8").splitlines()
        if not lines:
            return None
        first = lines[0].strip()
        prefix = "gitdir:"
        if not first.lower().startswith(prefix):
            return None
        pointer = first[len(prefix) :].strip()
        if not pointer:
            return None
        admin = Path(pointer)
        if not admin.is_absolute():
            admin = (git_marker.parent / admin).resolve()
        return int((admin / "gitdir").stat().st_mtime)
    except (OSError, UnicodeDecodeError, ValueError):
        return None


def _empty_lane_blocks_landed_reclaim(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    integration_ref: str,
    base_sha: str | None = None,
    created_at: object | None = None,
) -> tuple[bool | None, str]:
    """True when the lane never produced commits; None when the gate cannot answer.

    A positive ``base..tip`` count is own-work proof only when the tip is off
    the integration ref's first-parent lineage. On that lineage the count may
    include inherited commits, so the gate cannot answer. Count zero remains
    ``never_started``. An unresolvable base or lineage probe fails closed.
    """
    name = _recorded_branch_short_name(branch)
    if name is None:
        return (False, "")
    tip_sha = _reclaim_rev_parse(repo_root, f"refs/heads/{name}")
    if not tip_sha:
        return (None, "integration_tip_unknown")

    if base_sha is not None:
        from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
            _coerce_commit_sha,
            _worktree_reflog_proves_no_commit_after_base,
        )

        coerced = _coerce_commit_sha(base_sha) if isinstance(base_sha, str) else None
        if coerced is None:
            return (None, "empty_lane_base_sha_unresolvable")
        count = _reclaim_rev_list_count(repo_root, coerced, tip_sha)
        if count is None:
            return (None, "empty_lane_base_sha_unresolvable")
        if count == 0:
            return (True, "never_started")
        if count < 0:
            return (None, "empty_lane_base_sha_unresolvable")
        if _worktree_reflog_proves_no_commit_after_base(
            worktree_path,
            name,
            coerced,
            created_at=created_at,
        ):
            return (True, "never_started")
        on_lineage = _reclaim_first_parent_lineage(repo_root, tip_sha, integration_ref)
        if on_lineage is True:
            return (None, "empty_lane_base_sha_first_parent_lineage_ambiguous")
        if on_lineage is None:
            return (None, "empty_lane_first_parent_lineage_unresolvable")
        return (False, "")

    at_tip = _branch_is_at_integration_tip(repo_root, name, integration_ref)
    if at_tip is True:
        return (True, "never_started")
    if at_tip is None:
        return (None, "integration_tip_unknown")

    on_lineage = _reclaim_first_parent_lineage(repo_root, tip_sha, integration_ref)
    if on_lineage is True:
        return (True, "never_started")
    if on_lineage is None:
        return (None, "empty_lane_first_parent_lineage_unresolvable")

    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        _worktree_reflog_tip_evidence,
    )

    reflog_evidence = _worktree_reflog_tip_evidence(
        worktree_path,
        name,
        tip_sha,
        lane_created_at=created_at,
    )
    if reflog_evidence == "empty":
        return (True, "never_started")
    if reflog_evidence == "own":
        return (False, "")

    created_epoch = None
    parsed = _parse_sqlite_utc(created_at)
    if parsed is not None:
        created_epoch = int(parsed.timestamp())
    if created_epoch is None:
        created_epoch = _worktree_registration_epoch(worktree_path)
    if created_epoch is None:
        return (None, "empty_lane_created_at_unresolvable")
    tip_epoch = _reclaim_committer_epoch(repo_root, tip_sha)
    if tip_epoch is None:
        return (None, "empty_lane_branch_tip_committer_unresolvable")
    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        LANE_TIMESTAMP_SKEW_TOLERANCE_S,
    )

    registered = _worktree_registration_epoch(worktree_path)
    if registered is not None and tip_epoch > registered:
        return (False, "")
    if tip_epoch < created_epoch - LANE_TIMESTAMP_SKEW_TOLERANCE_S:
        return (True, "never_started")
    if tip_epoch > created_epoch + LANE_TIMESTAMP_SKEW_TOLERANCE_S:
        return (False, "")
    return (None, "empty_lane_timestamp_inconclusive")


def _worktree_registration_age_hours(worktree_path: str) -> float | None:
    """Hours since git registered this linked worktree. ``None`` when unknown.

    Read from the administrative ``gitdir`` stamp under ``.git/worktrees/<name>``,
    which ``git worktree add`` writes once at registration. A lane row is not
    usable here: the sweep is driven by the registry precisely so it can see
    worktrees whose rows are terminal or -- during a projection outage -- were
    never written at all.
    """
    try:
        git_marker = Path(worktree_path) / ".git"
        if not git_marker.is_file():
            return None
        lines = git_marker.read_text(encoding="utf-8").splitlines()
        if not lines:
            return None
        first = lines[0].strip()
        prefix = "gitdir:"
        if not first.lower().startswith(prefix):
            return None
        pointer = first[len(prefix) :].strip()
        if not pointer:
            return None
        admin = Path(pointer)
        if not admin.is_absolute():
            admin = (git_marker.parent / admin).resolve()
        age_s = time.time() - (admin / "gitdir").stat().st_mtime
    except (OSError, UnicodeDecodeError, ValueError):
        return None
    return max(0.0, age_s) / 3600.0


def _branch_merged_into_integration(root: Path, branch: str, integration_ref: str) -> bool | None:
    """Ancestry probe from the integration root. True/False/None(unknown)."""
    proc = _run_reclaim_command(
        [
            "git",
            "-C",
            str(root),
            "merge-base",
            "--is-ancestor",
            f"refs/heads/{branch}",
            integration_ref,
        ]
    )
    if proc is None:
        return None
    rc = getattr(proc, "returncode", None)
    if rc == 0:
        return True
    if rc == 1:
        return False
    return None


def _unlanded_patch_commit_count(root: Path, ref: str, tip: str) -> int | None:
    """:func:`lane_disposition.unlanded_patch_commit_count` run through the reclaim runner.

    The patch-equivalence probe is a disposition primitive, so it lives in the
    designated module; ``None`` still means the probe cannot answer.
    """
    from .orchestration import lane_disposition  # noqa: PLC0415

    return lane_disposition.unlanded_patch_commit_count(root, ref, tip, run=_run_reclaim_command)


def _extra_ref_preservation_verdict(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    ref: str,
) -> tuple[str, str]:
    """Is this row-less lane's branch preserved by a live task integration branch?

    Returns ``(state, detail)`` with state one of ``preserved``, ``never_started``,
    ``unmerged`` or ``unknown``. The branch must be attached in the worktree and
    the worktree's HEAD must be its tip, so an unattached or diverged tree is never
    preserved by proxy. A branch sitting exactly on ``ref`` is ``never_started``,
    mirroring :func:`_branch_is_at_integration_tip`. Otherwise the branch is
    preserved when it is an ancestor of ``ref`` or carries zero unlanded
    patch-equivalent commits with merge-touched paths equal on both tips.
    ``unknown`` never authorizes removal [SECD-05].
    """
    name = _recorded_branch_short_name(branch)
    if name is None:
        return ("unmerged", "lane_branch_unavailable")
    attached = _worktree_head_is_recorded_branch(worktree_path, name)
    if attached is None:
        return ("unknown", "worktree_head_unreadable")
    if attached is False:
        return ("unmerged", "worktree_head_not_recorded_branch")
    tip = _reclaim_rev_parse(repo_root, f"refs/heads/{name}")
    head = _reclaim_rev_parse(Path(worktree_path), "HEAD")
    preserving = _reclaim_rev_parse(repo_root, f"refs/heads/{ref}")
    if tip is None or head is None or preserving is None:
        return ("unknown", "preservation_probe_unknown")
    if head != tip:
        return ("unmerged", "worktree_head_not_branch_tip")
    if tip == preserving:
        return ("never_started", f"at_tip_of:{ref}")
    ancestor = _branch_merged_into_integration(repo_root, name, f"refs/heads/{ref}")
    if ancestor is None:
        return ("unknown", "preservation_ancestry_unknown")
    if ancestor:
        return ("preserved", f"preserved_by:{ref}:ancestor")
    unlanded = _unlanded_patch_commit_count(repo_root, ref, tip)
    if unlanded is None:
        return ("unknown", "patch_equivalence_unknown")
    if unlanded == 0:
        # Cherry omits merge commits, including unique conflict resolutions.
        merges = _run_reclaim_command(["git", "-C", str(repo_root), "rev-list", "--merges", f"{preserving}..{tip}"])
        if merges is None or merges.returncode != 0:
            return ("unknown", "merge_preservation_unknown")
        merge_paths: set[str] = set()
        for merge in _probe_command_stdout(merges).splitlines():
            paths = _run_reclaim_command(
                [
                    "git",
                    "-C",
                    str(repo_root),
                    "diff-tree",
                    "--no-commit-id",
                    "--name-only",
                    "--no-renames",
                    "-r",
                    "-m",
                    "-z",
                    merge,
                ]
            )
            if paths is None or paths.returncode != 0:
                return ("unknown", "merge_paths_unknown")
            merge_paths.update(path for path in _probe_command_stdout(paths).split("\0") if path)
        if merge_paths:
            # -m includes changes against every parent, so merge-only additions
            # and conflict resolutions cannot disappear from this path set. Tree
            # entries (mode, type, object id; gitlinks included) are compared, not
            # a diff, so no diff.ignoreSubmodules or other diff config can hide one.
            pathspecs = [f":(literal){path}" for path in sorted(merge_paths)]
            listings: list[str] = []
            for rev in (preserving, tip):
                entries = _run_reclaim_command(
                    ["git", "-C", str(repo_root), "ls-tree", "-r", "-z", "--full-tree", rev, "--", *pathspecs]
                )
                if entries is None or entries.returncode != 0:
                    return ("unknown", "merge_content_preservation_unknown")
                listings.append(_probe_command_stdout(entries))
            if listings[0] != listings[1]:
                return ("unmerged", f"merge_content_not_on:{ref}")
        return ("preserved", f"preserved_by:{ref}:patch_equivalent")
    return ("unmerged", f"unlanded_commits_not_on:{ref}")


def _extra_ref_reclaim_verdict(
    *, repo_root: Path, branch: object, worktree_path: str, ref: str
) -> tuple[bool | None, str]:
    """Map a preservation verdict onto the merged-verdict shape reclaim consumes."""
    state, detail = _extra_ref_preservation_verdict(
        repo_root=repo_root,
        branch=branch,
        worktree_path=worktree_path,
        ref=ref,
    )
    if state == "preserved":
        return (True, detail)
    if state == "unknown":
        return (None, detail)
    return (False, "never_started" if state == "never_started" else detail)


def _preserving_integration_ref(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    refs: Sequence[str],
) -> tuple[str | None, str, str]:
    """First live task integration ref that preserves this branch, in sorted order.

    Returns ``(ref, "", detail)`` on a preservation, otherwise ``(None, reason, detail)``
    with ``reason`` one of ``never_started`` (on some ref's tip, which keeps the tree
    outright), ``merge_unknown`` (no ref preserved it and at least one could not be
    read) or ``unmerged``. A ref that cannot answer never negates a ref that proves
    preservation.
    """
    own = _recorded_branch_short_name(branch)
    unknown_detail = ""
    for ref in refs:
        if ref == own:
            continue
        state, detail = _extra_ref_preservation_verdict(
            repo_root=repo_root,
            branch=branch,
            worktree_path=worktree_path,
            ref=ref,
        )
        if state == "preserved":
            return (ref, "", detail)
        if state == "never_started":
            return (None, "never_started", detail)
        if state == "unknown" and not unknown_detail:
            unknown_detail = f"{ref}:{detail}"
    if unknown_detail:
        return (None, "merge_unknown", unknown_detail)
    return (None, "unmerged", "")


_LIVE_TASK_TARGET_STATUSES = ("in_progress", "review")
_PRESERVED_BRANCH_KEPT = "preserved_by_non_main_ref"


def _read_live_task_target_branches() -> list[str]:
    """``target_branch`` of every non-archived handoff task in progress or in review."""
    with _get_db_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT hs.target_branch AS target_branch FROM handoff_state hs "
            "WHERE LOWER(COALESCE(hs.status, '')) IN (?, ?) "
            "AND NOT EXISTS (SELECT 1 FROM task_archives ta WHERE ta.task_ref = hs.task_ref)",
            _LIVE_TASK_TARGET_STATUSES,
        ).fetchall()
    return [str(row["target_branch"]) for row in rows if row["target_branch"]]


def _live_task_integration_refs(root: Path, integration_ref: str) -> tuple[tuple[str, ...], str | None]:
    """Local task integration branches, other than ``integration_ref``, that may preserve lanes.

    Returns ``(names, degraded)``. A failed read returns no names and a typed
    ``integration_refs_degraded`` note; the full sweep falls back to main only,
    while the hook arm stops. Deadline expiry propagates [SECD-05][AGT-10].
    """
    try:
        targets = _read_live_task_target_branches()
    except Exception as exc:  # noqa: BLE001 — full sweeps degrade; the hook arm stops
        if isinstance(exc, TimeoutError) and _REGISTRY_REAP_DEADLINE.get() is not None:
            raise
        return ((), f"integration_refs_degraded:{type(exc).__name__}"[:_RECLAIM_DETAIL_CAP])
    main_name = _recorded_branch_short_name(integration_ref)
    names: set[str] = set()
    for target in targets:
        name = _recorded_branch_short_name(target)
        if name is None or name == main_name:
            continue
        proc = _run_reclaim_command(["git", "-C", str(root), "show-ref", "--verify", "--quiet", f"refs/heads/{name}"])
        if proc is None or getattr(proc, "returncode", None) not in (0, 1):
            return ((), "integration_refs_degraded:ref_probe_failed")
        if proc.returncode == 0:
            names.add(name)
    return (tuple(sorted(names)), None)


def _open_lane_row_names_branch(branch: str) -> tuple[bool | None, str]:
    """True if a non-terminal lane row names this branch; None on probe failure."""
    try:
        placeholders = ",".join("?" for _ in _ARCHIVED_ORPHAN_TERMINAL_STATUSES)
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT lane_id, task_ref FROM worktree_lanes "
                f"WHERE branch = ? AND COALESCE(status, '') NOT IN ({placeholders}) "
                "LIMIT 1",
                (branch, *_ARCHIVED_ORPHAN_TERMINAL_STATUSES),
            ).fetchone()
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (None, f"open_row_probe_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    if row is not None:
        return (True, f"open_lane:{row['lane_id']} task={row['task_ref']}")
    return (False, "")


def _row_identity_for_worktree_path(path: str) -> tuple[str | None, str | None]:
    """Newest lane row naming this path (any status) → (task_ref, lane_id).

    Passing the historical identity through lets the shared-path gate
    recognise a worktree whose only row is already terminal as self-owned
    rather than refusing it as foreign forever. A failed read raises, so
    ``(None, None)`` always means that no row names the path.
    """
    resolved = _resolved_path_text(path).rstrip("/")
    with _get_db_connection() as conn:
        rows = conn.execute(
            "SELECT task_ref, lane_id, worktree_path FROM worktree_lanes "
            "WHERE worktree_path IS NOT NULL ORDER BY id DESC",
        ).fetchall()
    for row in rows:
        candidate = str(row["worktree_path"] or "")
        if candidate.rstrip("/") == path.rstrip("/") or (_resolved_path_text(candidate).rstrip("/") == resolved):
            return (row["task_ref"], row["lane_id"])
    return (None, None)


def _observe_branch_tip(root: Path, branch: str) -> str | None:
    """Full SHA of ``refs/heads/<branch>`` at this instant, or None if unknown."""
    proc = _run_reclaim_command(
        [
            "git",
            "-C",
            str(root),
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/heads/{branch}^{{commit}}",
        ]
    )
    if proc is None or getattr(proc, "returncode", None) != 0:
        return None
    sha = (_probe_command_stdout(proc) or "").strip()
    return sha or None


def _row_identity_for_branch(branch: str) -> tuple[str | None, str | None]:
    """Newest lane row naming this branch (any status) → (task_ref, lane_id).

    A failed read raises. Callers that authorize deletion must refuse, not read
    an unreadable row as a rowless branch.
    """
    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT task_ref, lane_id FROM worktree_lanes WHERE branch = ? ORDER BY id DESC LIMIT 1",
            (branch,),
        ).fetchone()
    if row is None:
        return (None, None)
    return (row["task_ref"], row["lane_id"])


def _registry_reclaim_queue_identity(task_ref: str | None, lane_id: str | None, branch: str) -> tuple[str, str]:
    """``(task_ref, lane_id)`` a registry-swept branch is queued under; rowless gets a stand-in."""
    return ((task_ref or "").strip() or "merged-registry-reclaim", (lane_id or "").strip() or f"registry-{branch}")


def _enqueue_merged_registry_reclaim_candidate(
    *,
    root: Path,
    branch: str,
    task_ref: str | None,
    lane_id: str | None,
    failed: list[dict[str, object]],
    worktree_path: object = None,
    authorized_sha: str | None = None,
) -> None:
    """Enqueue one authorized tip for a qualifying merged branch.

    ``authorized_sha`` is the close-authorized tip when the row has one; the
    observed tip is authority only for a rowless branch. Queue writes must never
    fail the sweep: a raise is recorded on ``failed`` and the caller continues
    [Release It!].
    """
    record: dict[str, object] = {"worktree_path": worktree_path, "branch": branch}
    try:
        from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
            enqueue_branch_reclaim_candidate,
        )

        sha = authorized_sha or _observe_branch_tip(root, branch)
        if sha is None:
            failed.append({**record, "stage": "branch_reclaim_enqueue", "detail": "tip_unresolvable"})
            return
        queued_task, queued_lane = _registry_reclaim_queue_identity(task_ref, lane_id, branch)
        accepted = enqueue_branch_reclaim_candidate(
            task_ref=queued_task,
            lane_id=queued_lane,
            branch=branch,
            authorized_sha=sha,
        )
        if not accepted:
            failed.append({**record, "stage": "branch_reclaim_enqueue", "detail": "queue_write_refused"})
    except Exception as exc:  # noqa: BLE001 — never fail the reap
        failed.append(
            {
                **record,
                "stage": "branch_reclaim_enqueue",
                "detail": str(exc)[:_RECLAIM_DETAIL_CAP],
            }
        )


def _enqueue_merged_registry_reclaim_skip(
    *,
    root: Path,
    branch: str,
    task_ref: str | None,
    lane_id: str | None,
    failed: list[dict[str, object]],
    reason: str,
) -> None:
    """Persist one typed refusal without creating a new candidate.

    The caller must have already admitted the branch through ancestry and the
    open-lane-row brake. Keeping this distinct from the positive candidate
    helper makes that authorization boundary explicit.
    """
    record: dict[str, object] = {"worktree_path": None, "branch": branch}
    try:
        from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
            enqueue_branch_reclaim_outcome,
        )

        sha = _observe_branch_tip(root, branch)
        if sha is None:
            failed.append({**record, "stage": "branch_reclaim_enqueue", "detail": "tip_unresolvable"})
            return
        queued_task, queued_lane = _registry_reclaim_queue_identity(task_ref, lane_id, branch)
        accepted = enqueue_branch_reclaim_outcome(
            task_ref=queued_task,
            lane_id=queued_lane,
            branch=branch,
            authorized_sha=sha,
            reason=reason,
        )
        if not accepted:
            failed.append({**record, "stage": "branch_reclaim_enqueue", "detail": "queue_write_refused"})
    except Exception as exc:  # noqa: BLE001 — never fail the reap
        failed.append(
            {
                **record,
                "stage": "branch_reclaim_enqueue",
                "detail": str(exc)[:_RECLAIM_DETAIL_CAP],
            }
        )


def _registry_skip_counts(skipped: list[dict[str, object]]) -> dict[str, int]:
    """Count typed registry refusals for operator-facing drain telemetry."""
    counts: dict[str, int] = {}
    for item in skipped:
        reason = item.get("reason")
        if isinstance(reason, str) and reason:
            counts[reason] = counts.get(reason, 0) + 1
    return counts


def _branch_bundle_key(branch: str) -> str:
    """Mirror ``bundle_lane_branch``'s filename key so unreap can find the bundle."""
    return hashlib.sha256(branch.encode("utf-8")).hexdigest()[:16]


def _bundle_dir(root: Path | str, explicit: Path | str | None = None) -> Path:
    """Resolve the archive, isolating shared storage by canonical Git common dir.

    Linked worktrees share an identity; separate repositories never share
    retention or implicit unreap selection, even with identical branch names.
    Legacy unnamespaced external archives require an explicit unreap path.
    """
    if explicit is not None and str(explicit).strip():
        archive = Path(explicit)
    elif configured := os.environ.get("WORKBAY_BUNDLE_DIR", "").strip():
        archive = Path(configured)
    else:
        return Path(root).resolve() / ".task-state" / _BUNDLE_DIR_NAME
    common = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--git-common-dir"])
    common_dir = _probe_command_stdout(common).strip()
    if common is None or common.returncode != 0 or not common_dir:
        raise OSError("bundle_repository_identity_unavailable")
    identity = Path(common_dir)
    if not identity.is_absolute():
        identity = Path(root) / identity
    key = hashlib.sha256(str(identity.resolve()).encode("utf-8")).hexdigest()
    return archive.resolve() / f"repo-{key}"


def _prune_branch_bundles(
    root: Path | str,
    branch: str,
    *,
    retention_count: int,
    keep: Path | None = None,
    bundle_dir: Path | str | None = None,
) -> int:
    """Keep the newest ``retention_count`` bundles for one branch; return the prune count.

    Retention is per-branch, not per-directory: one busy lane must not evict
    another lane's only rollback artifact. ``keep`` is forced to survive
    regardless of clock skew between file mtimes.
    """
    if retention_count <= 0:
        return 0
    try:
        entries = list(_bundle_dir(root, bundle_dir).glob(f"{_branch_bundle_key(branch)}-*.bundle"))
    except OSError:
        return 0
    if len(entries) <= retention_count:
        return 0

    def _sort_key(path: Path) -> tuple[float, str]:
        try:
            return (path.stat().st_mtime, path.name)
        except OSError:
            return (0.0, path.name)

    ordered = sorted(entries, key=_sort_key, reverse=True)
    if keep is not None:
        kept = str(keep)
        ordered = [item for item in ordered if str(item) == kept] + [item for item in ordered if str(item) != kept]
    pruned = 0
    for path in ordered[retention_count:]:
        try:
            path.unlink()
        except OSError:
            continue
        _remove_bundle_verification_receipt(path)
        pruned += 1
    return pruned


def _bundle_filename_tip_sha(path: Path | str) -> str:
    """Recover the bundled tip from the filename ``<branch-key>-<sha>.bundle``.

    The name is written by ``bundle_lane_branch`` and is the only in-band record
    of which commit a bundle holds, so unreap can assert what it restored.
    """
    stem = Path(path).name
    if not stem.endswith(".bundle"):
        return ""
    _, _, candidate = stem[: -len(".bundle")].partition("-")
    if len(candidate) != 40 or any(char not in "0123456789abcdef" for char in candidate):
        return ""
    return candidate


def _bundle_prerequisite_shas(path: Path | str) -> list[str]:
    """Read prerequisite object ids from a v2/v3 bundle header."""
    prerequisites: list[str] = []
    try:
        with Path(path).open("rb") as handle:
            for raw_line in handle:
                if raw_line in {b"\n", b"\r\n"}:
                    break
                if not raw_line.startswith(b"-"):
                    continue
                candidate = raw_line[1:].split(maxsplit=1)[0].decode("ascii", errors="ignore")
                if len(candidate) in {40, 64} and all(char in "0123456789abcdef" for char in candidate):
                    prerequisites.append(candidate)
    except OSError:
        return []
    return prerequisites


def _bundle_verification_identity(path: Path, tip_sha: str, prerequisites: list[str]) -> dict[str, object] | None:
    """Return stable content identity for a bundle, or ``None`` if it changed while hashing."""
    try:
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        after = path.stat()
    except OSError:
        return None
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        return None
    return {
        "version": 1,
        "bundle_size": after.st_size,
        "bundle_mtime_ns": after.st_mtime_ns,
        "bundle_sha256": digest.hexdigest(),
        "tip_sha": tip_sha,
        "prerequisite_shas": prerequisites,
    }


def _bundle_verification_receipt_path(path: Path) -> Path:
    return Path(f"{path}.verified.json")


def _remove_bundle_verification_receipt(path: Path) -> None:
    try:
        _bundle_verification_receipt_path(path).unlink(missing_ok=True)
    except OSError as exc:
        _logger.warning("could not remove bundle verification receipt for %s: %s", path, exc)


def _bundle_verification_receipt_matches(path: Path, identity: dict[str, object] | None) -> bool:
    if identity is None or path.suffix != ".bundle":
        return False
    try:
        with _bundle_verification_receipt_path(path).open("r", encoding="utf-8") as handle:
            receipt = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return isinstance(receipt, dict) and receipt == identity


def _write_bundle_verification_receipt(path: Path, identity: dict[str, object] | None) -> None:
    """Persist a verified bundle identity atomically; the bundle remains authoritative."""
    if identity is None or path.suffix != ".bundle":
        return
    receipt = _bundle_verification_receipt_path(path)
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{receipt.name}.", suffix=".tmp", dir=receipt.parent)
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(identity, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, receipt)
        directory_fd = os.open(receipt.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        _logger.warning("could not write bundle verification receipt for %s: %s", path, exc)
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _reap_stale_bundle_verification_scratch(parent: Path) -> int:
    """Remove only old scratch directories created by this verifier."""
    now = time.time()
    removed = 0
    try:
        entries = list(parent.iterdir())
    except OSError:
        return 0
    for entry in entries:
        if not entry.name.startswith(_BUNDLE_VERIFY_SCRATCH_PREFIX):
            continue
        try:
            info = entry.lstat()
        except OSError:
            continue
        if not stat.S_ISDIR(info.st_mode) or now - info.st_mtime <= _BUNDLE_VERIFY_SCRATCH_MAX_AGE_S:
            continue
        try:
            shutil.rmtree(entry)
        except OSError:
            continue
        removed += 1
    if removed:
        _logger.info("removed stale bundle verification scratch directories count=%d", removed)
    return removed


def _verified_bundle_contains(root: Path, path: Path, tip_sha: str, *, errors: list[str] | None = None) -> bool:
    def _failed(reason: str) -> bool:
        if errors is not None:
            errors.append(reason)
        return False

    listed = _run_reclaim_command(["git", "-C", str(root), "bundle", "list-heads", str(path)])
    if listed is None or listed.returncode != 0:
        return _failed(
            "bundle_list_heads_timeout"
            if listed is not None and listed.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
            else "bundle_list_heads_failed"
        )
    verified = _run_reclaim_command(
        ["git", "-C", str(root), "bundle", "verify", str(path)],
        timeout=_BUNDLE_VERIFY_TIMEOUT_S,
    )
    if verified is None or verified.returncode != 0:
        return _failed(
            "bundle_verify_timeout"
            if verified is not None and verified.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
            else "bundle_verify_failed"
        )
    listed_shas = {line.split(maxsplit=1)[0] for line in _probe_command_stdout(listed).splitlines() if line.strip()}
    if tip_sha not in listed_shas:
        return _failed("bundle_tip_not_listed")
    prerequisites = sorted(set(_bundle_prerequisite_shas(path)))
    identity = _bundle_verification_identity(path, tip_sha, prerequisites)
    if identity is None:
        return _failed("bundle_identity_unavailable")
    scratch_parent: Path | None = None
    try:
        scratch_parent = path.resolve().parent
    except OSError as exc:
        _logger.warning("could not resolve bundle verification scratch parent for %s: %s", path, exc)
    if scratch_parent is not None:
        try:
            _reap_stale_bundle_verification_scratch(scratch_parent)
        except OSError as exc:
            _logger.warning("could not reap stale bundle verification scratch in %s: %s", scratch_parent, exc)
    if _bundle_verification_receipt_matches(path, identity):
        return True
    # Source alternates would hide missing bundle objects. Copy only the closure
    # of the declared prerequisites' boundary commits and trees into isolated
    # storage before unpacking. Marking each boundary commit shallow means the
    # walk below can prove the bundle's delta without requiring main's history.
    try:
        # Keep prerequisite packs on the archive filesystem, whose capacity the
        # operator selected for rollback storage, and clean up on every exit.
        if scratch_parent is None:
            scratch_parent = path.resolve().parent
        with tempfile.TemporaryDirectory(prefix=_BUNDLE_VERIFY_SCRATCH_PREFIX, dir=scratch_parent) as scratch:
            initialized = _run_reclaim_command(
                [
                    "git",
                    "init",
                    "--bare",
                    "--quiet",
                    f"--object-format={'sha256' if len(tip_sha) == 64 else 'sha1'}",
                    scratch,
                ]
            )
            if initialized is None or initialized.returncode != 0:
                return _failed(
                    "bundle_verify_scratch_init_timeout"
                    if initialized is not None and initialized.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
                    else "bundle_verify_scratch_init_failed"
                )
            if prerequisites:
                prerequisite_objects = _run_reclaim_command(
                    [
                        "git",
                        "-C",
                        str(root),
                        "rev-list",
                        "--objects",
                        "--no-walk",
                        *prerequisites,
                    ],
                    timeout=_BUNDLE_VERIFY_TIMEOUT_S,
                )
                if prerequisite_objects is None or prerequisite_objects.returncode != 0:
                    return _failed(
                        "bundle_prerequisite_objects_timeout"
                        if prerequisite_objects is not None
                        and prerequisite_objects.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
                        else "bundle_prerequisite_objects_failed"
                    )
                object_ids = list(
                    dict.fromkeys(
                        line.split(maxsplit=1)[0]
                        for line in _probe_command_stdout(prerequisite_objects).splitlines()
                        if line.strip()
                    )
                )
                if not object_ids:
                    return _failed("bundle_prerequisite_objects_empty")
                packed = _run_reclaim_command(
                    [
                        "git",
                        "-C",
                        str(root),
                        "pack-objects",
                        str(Path(scratch) / "objects" / "pack" / "pack"),
                    ],
                    timeout=_BUNDLE_VERIFY_TIMEOUT_S,
                    stdin_text="\n".join(object_ids) + "\n",
                )
                if packed is None or packed.returncode != 0:
                    return _failed(
                        "bundle_prerequisite_pack_timeout"
                        if packed is not None and packed.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
                        else "bundle_prerequisite_pack_failed"
                    )
                try:
                    (Path(scratch) / "shallow").write_text("\n".join(prerequisites) + "\n", encoding="ascii")
                except OSError:
                    return _failed("bundle_prerequisite_shallow_write_failed")
            unpacked = _run_reclaim_command(
                ["git", "-C", scratch, "bundle", "unbundle", str(path.resolve())],
                timeout=_BUNDLE_VERIFY_TIMEOUT_S,
            )
            if unpacked is None or unpacked.returncode != 0:
                return _failed(
                    "bundle_unbundle_timeout"
                    if unpacked is not None and unpacked.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
                    else "bundle_unbundle_failed"
                )
            connected = _run_reclaim_command(
                [
                    "git",
                    "-C",
                    scratch,
                    "rev-list",
                    "--objects",
                    "--missing=error",
                    f"{tip_sha}^{{commit}}",
                    *(["--not", *prerequisites] if prerequisites else []),
                ],
                timeout=_BUNDLE_VERIFY_TIMEOUT_S,
            )
            if connected is None or connected.returncode != 0:
                return _failed(
                    "bundle_tip_reachability_timeout"
                    if connected is not None and connected.returncode == _RECLAIM_TIMEOUT_RETURN_CODE
                    else "bundle_tip_unreachable"
                )
        verified_identity = _bundle_verification_identity(path, tip_sha, prerequisites)
        if verified_identity != identity:
            return _failed("bundle_changed_during_verification")
        _write_bundle_verification_receipt(path, verified_identity)
        return True
    except (OSError, subprocess.SubprocessError):
        return _failed("bundle_verify_os_error")


def _prune_bundle_dir_to_cap(
    root: Path | str,
    *,
    max_bytes: int = _DEFAULT_BUNDLE_DIR_MAX_BYTES,
    keep: Path | None = None,
    bundle_dir: Path | str | None = None,
) -> int:
    """Evict oldest-first across branches until the directory fits ``max_bytes``.

    Each branch's newest bundle -- and ``keep`` -- are exempt, so the global
    ceiling can shed history depth but never a lane's last restore point
    [RES-07][RLSE-08].
    """
    if max_bytes <= 0:
        return 0
    try:
        entries = list(_bundle_dir(root, bundle_dir).glob("*.bundle"))
    except OSError:
        return 0

    def _stat(path: Path) -> tuple[float, int]:
        try:
            info = path.stat()
        except OSError:
            return (0.0, 0)
        return (info.st_mtime, info.st_size)

    sized = {path: _stat(path) for path in entries}
    total = sum(size for _, size in sized.values())
    if total <= max_bytes:
        return 0
    protected: set[Path] = {Path(keep)} if keep is not None else set()
    newest_per_key: dict[str, Path] = {}
    for path in entries:
        key = path.name.partition("-")[0]
        incumbent = newest_per_key.get(key)
        if incumbent is None or sized[path] > sized[incumbent]:
            newest_per_key[key] = path
    protected.update(newest_per_key.values())

    pruned = 0
    for path in sorted((p for p in entries if p not in protected), key=lambda item: (sized[item][0], item.name)):
        if total <= max_bytes:
            break
        try:
            path.unlink()
        except OSError:
            continue
        _remove_bundle_verification_receipt(path)
        total -= sized[path][1]
        pruned += 1
    return pruned


def _bundle_before_reap(
    root: Path | str,
    branch: str,
    *,
    retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
    bundle_dir: Path | str | None = None,
) -> dict[str, object]:
    """Write and verify the rollback bundle that must exist before a ref is deleted.

    Returns a typed envelope; a failure here is a *refusal to delete*, never a
    warning to step over [RLSE-08].
    """
    # Bundle creation is the longest subprocess in the delete path, so it owes
    # the same CON-18/CON-21 barrier as every other reclaim command.
    _assert_no_reaper_write_lock(reason="bundle_before_reap")
    repo = Path(root)
    ref = f"refs/heads/{branch}"
    resolved = _run_reclaim_command(["git", "-C", str(repo), "rev-parse", "--verify", f"{ref}^{{commit}}"])
    tip_sha = _probe_command_stdout(resolved).strip()
    if resolved is None or resolved.returncode != 0 or len(tip_sha) not in {40, 64}:
        return {"ok": False, "guard": "verified_bundle_required", "error": "branch_tip_unavailable"}

    try:
        destination = _bundle_dir(repo, bundle_dir)
        destination.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return {
            "ok": False,
            "guard": "verified_bundle_required",
            "error": "bundle_dir_unwritable",
            "detail": str(exc)[:_RECLAIM_DETAIL_CAP],
        }

    bundle_path = destination / f"{_branch_bundle_key(branch)}-{tip_sha}.bundle"
    integration = _run_reclaim_command(
        ["git", "-C", str(repo), "show-ref", "--verify", "--quiet", f"refs/heads/{_RECLAIM_INTEGRATION_REF}"]
    )
    if integration is None or integration.returncode not in {0, 1}:
        return {"ok": False, "guard": "verified_bundle_required", "error": "integration_ref_probe_failed"}
    bundle_mode = "full"
    if integration.returncode == 0:
        merge_base = _run_reclaim_command(["git", "-C", str(repo), "merge-base", tip_sha, _RECLAIM_INTEGRATION_REF])
        base_sha = _probe_command_stdout(merge_base).strip()
        if merge_base is not None and merge_base.returncode == 0 and base_sha:
            bundle_mode = "delta"
        elif merge_base is None or merge_base.returncode != 1:
            # Full-history archives are permitted only for conclusively missing
            # or disjoint history. Timeout/corruption is ambiguity [RLSE-08].
            return {"ok": False, "guard": "verified_bundle_required", "error": "merge_base_probe_failed"}
    fallback_reason = "" if bundle_mode == "delta" else "merge_base_unreachable"

    # A fully merged tip makes ``tip ^main`` an empty revision set, which Git
    # refuses to bundle. Keep the head as the single delta object and name its
    # first parent as the prerequisite; main necessarily has both at reap time.
    exclusion = _RECLAIM_INTEGRATION_REF
    if bundle_mode == "delta":
        ahead = _run_reclaim_command(
            ["git", "-C", str(repo), "rev-list", "--count", tip_sha, f"^{_RECLAIM_INTEGRATION_REF}"]
        )
        if _probe_command_stdout(ahead).strip() == "0":
            parent = _run_reclaim_command(["git", "-C", str(repo), "rev-list", "--parents", "-n", "1", tip_sha])
            ancestry = _probe_command_stdout(parent).split()
            if parent is None or parent.returncode != 0 or not ancestry or ancestry[0] != tip_sha:
                return {
                    "ok": False,
                    "guard": "verified_bundle_required",
                    "error": "merge_parent_probe_failed",
                }
            if len(ancestry) == 1:
                # A conclusively parentless tip has no possible prerequisite.
                # Archive its complete history before deletion [RLSE-08].
                bundle_mode = "full"
                fallback_reason = "merged_root_commit"
            else:
                exclusion = ancestry[1]

    existing_mode = "delta" if _bundle_prerequisite_shas(bundle_path) else "full"
    verification_errors: list[str] = []
    reused = (
        bundle_path.exists()
        and existing_mode == bundle_mode
        and _verified_bundle_contains(repo, bundle_path, tip_sha, errors=verification_errors)
    )
    if verification_errors:
        # A stored bundle rejected by unbundle is replaceable. Keep timeouts,
        # reachability failures, and other verification errors as hard refusals.
        if verification_errors != ["bundle_unbundle_failed"]:
            return {"ok": False, "guard": "verified_bundle_required", "error": verification_errors[0]}
        verification_errors.clear()
    temporary_path: Path | None = None
    if not reused:
        temporary_path = destination / f".{bundle_path.name}.{os.getpid()}.{time.time_ns()}.tmp"
        try:
            descriptor = os.open(temporary_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
            temporary_path.unlink()
            revisions = [ref]
            if bundle_mode == "delta":
                revisions.append(f"^{exclusion}")
            created = _run_reclaim_command(
                ["git", "-C", str(repo), "bundle", "create", str(temporary_path), *revisions]
            )
            if created is None or created.returncode != 0:
                detail = (getattr(created, "stderr", "") or "bundle_create_unrunnable").strip()
                return {
                    "ok": False,
                    "guard": "verified_bundle_required",
                    "error": "bundle_create_failed",
                    "detail": detail[:_RECLAIM_DETAIL_CAP],
                }
            if not _verified_bundle_contains(repo, temporary_path, tip_sha, errors=verification_errors):
                return {
                    "ok": False,
                    "guard": "verified_bundle_required",
                    "error": verification_errors[0] if verification_errors else "bundle_verification_failed",
                }
            with temporary_path.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary_path, bundle_path)
            temporary_path = None
            directory_fd = os.open(destination, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            if not _verified_bundle_contains(repo, bundle_path, tip_sha, errors=verification_errors):
                return {
                    "ok": False,
                    "guard": "verified_bundle_required",
                    "error": verification_errors[0] if verification_errors else "bundle_verification_failed",
                }
        except OSError as exc:
            return {
                "ok": False,
                "guard": "verified_bundle_required",
                "error": "bundle_dir_unwritable",
                "detail": str(exc)[:_RECLAIM_DETAIL_CAP],
            }
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass
    # ``bundle_lane_branch`` reuses an already-verified bundle for an unchanged
    # tip and returns it *without touching the file*. Selection by mtime would
    # then read "most recently written" as "most recently reaped" -- wrong the
    # moment a branch is re-cut, reaped at a newer tip, then reset back and
    # reaped again. Stamping here makes mtime mean what unreap reads it to mean.
    try:
        os.utime(bundle_path, None)
        identity = _bundle_verification_identity(
            bundle_path,
            tip_sha,
            sorted(set(_bundle_prerequisite_shas(bundle_path))),
        )
        _write_bundle_verification_receipt(bundle_path, identity)
    except OSError:
        pass
    pruned = _prune_branch_bundles(
        root,
        branch,
        retention_count=retention_count,
        keep=bundle_path,
        bundle_dir=bundle_dir,
    )
    pruned += _prune_bundle_dir_to_cap(root, keep=bundle_path, bundle_dir=bundle_dir)
    try:
        bundle_bytes = bundle_path.stat().st_size
    except OSError as exc:
        return {
            "ok": False,
            "guard": "verified_bundle_required",
            "error": "bundle_verification_failed",
            "detail": str(exc)[:_RECLAIM_DETAIL_CAP],
        }
    return {
        "ok": True,
        "bundle_path": str(bundle_path),
        "tip_sha": tip_sha,
        "bundle_mode": bundle_mode,
        "bundle_bytes": bundle_bytes,
        "bundle_fallback_reason": fallback_reason,
        "bundle_reused": reused,
        "bundle_retention_count": retention_count,
        "bundles_pruned": pruned,
    }


def unreap_lane_branch(
    root: Path | str,
    branch: str,
    *,
    bundle_path: Path | str | None = None,
) -> dict[str, object]:
    """Restore a reaped branch to its exact bundled tip.

    The rollback half of the reap contract: a bundle nobody can restore from is
    not a rollback [RLSE-08]. Refuses when the ref already exists rather than
    moving a live branch.
    """
    repo = Path(root)
    cleaned = branch.strip() if isinstance(branch, str) else ""
    if not cleaned:
        return {"ok": False, "error": "branch is required."}

    if bundle_path is None:
        try:
            candidates = sorted(
                _bundle_dir(repo).glob(f"{_branch_bundle_key(cleaned)}-*.bundle"),
                key=lambda item: (item.stat().st_mtime, item.name),
            )
        except OSError as exc:
            return {"ok": False, "error": "bundle_dir_unreadable", "branch": cleaned, "detail": str(exc)}
        if not candidates:
            return {"ok": False, "error": "no_bundle_for_branch", "branch": cleaned}
        selected = candidates[-1]
    else:
        selected = Path(bundle_path)

    missing_prerequisites: list[str] = []
    for sha in _bundle_prerequisite_shas(selected):
        present = _run_reclaim_command(["git", "-C", str(repo), "cat-file", "-e", f"{sha}^{{commit}}"])
        if present is None or present.returncode != 0:
            missing_prerequisites.append(sha)
    if missing_prerequisites:
        return {
            "ok": False,
            "error": "bundle_prerequisites_missing",
            "branch": cleaned,
            "bundle_path": str(selected),
            "missing_shas": missing_prerequisites,
        }

    verified = _run_reclaim_command(["git", "-C", str(repo), "bundle", "verify", str(selected)])
    if verified is None or getattr(verified, "returncode", None) != 0:
        return {
            "ok": False,
            "error": "bundle_verification_failed",
            "branch": cleaned,
            "bundle_path": str(selected),
        }
    existing = _run_reclaim_command(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"refs/heads/{cleaned}^{{commit}}"]
    )
    if existing is not None and getattr(existing, "returncode", None) == 0:
        return {
            "ok": False,
            "error": "branch_already_exists",
            "branch": cleaned,
            "tip_sha": _probe_command_stdout(existing).strip(),
        }
    fetched = _run_reclaim_command(
        ["git", "-C", str(repo), "fetch", str(selected), f"refs/heads/{cleaned}:refs/heads/{cleaned}"]
    )
    if fetched is None or getattr(fetched, "returncode", None) != 0:
        detail = (getattr(fetched, "stderr", "") or "").strip() if fetched is not None else "fetch_unrunnable"
        return {
            "ok": False,
            "error": "bundle_fetch_failed",
            "branch": cleaned,
            "bundle_path": str(selected),
            "detail": detail[:_RECLAIM_DETAIL_CAP],
        }
    resolved = _run_reclaim_command(["git", "-C", str(repo), "rev-parse", f"refs/heads/{cleaned}"])
    tip_sha = _probe_command_stdout(resolved).strip() if resolved is not None else ""
    # A rollback that reports ``ok`` while the ref sits on some *other* commit is
    # worse than a failed rollback, because nothing downstream would ever catch
    # it. The bundle names its own tip in its filename; assert the ref landed
    # there [OBS-08][RLSE-08].
    expected = _bundle_filename_tip_sha(selected)
    if expected and tip_sha and expected != tip_sha:
        return {
            "ok": False,
            "error": "restored_tip_mismatch",
            "branch": cleaned,
            "bundle_path": str(selected),
            "tip_sha": tip_sha,
            "expected_tip_sha": expected,
        }
    return {
        "ok": True,
        "branch": cleaned,
        "bundle_path": str(selected),
        "tip_sha": tip_sha,
        "expected_tip_sha": expected,
    }


def _reference_transaction_hook_state(root: Path) -> tuple[str, str]:
    """``("absent", "")``, ``("present", path)`` or ``("unknown", detail)`` for the reference-transaction hook.

    Git runs only an executable regular file as this hook, so any other entry is
    absent. A probe that cannot locate or stat the hook is unknown, never absent:
    reading a failed probe as absent would let ``core.hooksPath`` bypass a real
    policy hook.
    """
    proc = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--git-path", "hooks/reference-transaction"])
    if proc is None or getattr(proc, "returncode", 1) != 0:
        return ("unknown", "hook_location_probe_failed")
    located = _probe_command_stdout(proc).strip()
    if not located:
        return ("unknown", "hook_location_empty")
    path = Path(located)
    if not path.is_absolute():
        path = root / path
    try:
        mode = path.stat().st_mode
    except FileNotFoundError:
        return ("absent", "")
    except OSError as exc:
        return ("unknown", f"hook_stat_failed:{type(exc).__name__}")
    if stat.S_ISREG(mode) and os.access(path, os.X_OK):
        return ("present", str(path))
    return ("absent", "")


def _delete_merged_branch(
    root: Path,
    branch: str,
    *,
    bundle_record: dict[str, object] | None = None,
    retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
    expected_tip: str | None = None,
    close_authority: Mapping[str, Any] | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> tuple[bool, str]:
    """Bundle, then safe-delete. ``-d`` refuses unmerged work as the final fail-safe.

    Ordering is the contract: the verified rollback bundle lands *before* the ref
    disappears, and a bundle that cannot be written or verified refuses the
    delete outright [RLSE-08][CON-05]. ``expected_tip`` is the close-authorized
    tip: a branch that has moved off it is refused before anything is bundled,
    and a bundle of any other commit is refused before the delete.
    """
    if expected_tip is not None:
        current = _run_reclaim_command(
            ["git", "-C", str(root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"]
        )
        current_sha = _probe_command_stdout(current).strip() if current is not None else ""
        if not current_sha:
            return (False, "branch_probe_failed")
        if current_sha != expected_tip:
            if bundle_record is not None:
                bundle_record["reap_guard"] = "tip_moved_after_close"
                bundle_record["reap_refusal"] = "tip_moved_after_close"
                bundle_record["reap_detail"] = f"authorized {expected_tip} but ref now {current_sha}"
            return (False, "tip_moved_after_close")
    bundle = _bundle_before_reap(root, branch, retention_count=retention_count)
    if not bundle.get("ok"):
        if bundle_record is not None:
            bundle_record["reap_guard"] = bundle.get("guard") or "verified_bundle_required"
            bundle_record["reap_refusal"] = "bundle_failed"
            bundle_record["reap_detail"] = bundle.get("error") or ""
        return (False, str(bundle.get("error") or "bundle_failed"))
    if bundle_record is not None:
        bundle_record.update({key: bundle[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle})
    # ``git branch -d`` takes no expected-SHA, so bundle-then-delete is a
    # check-then-act across concurrent writers: if the tip advanced to a
    # different still-merged commit, ``-d`` would happily delete it and the
    # recorded ``bundle_path``/``tip_sha`` would name a tip that is no longer
    # what was destroyed. Re-read and refuse on drift [CON-05].
    bundled_sha = str(bundle.get("tip_sha") or "")
    if expected_tip is not None and bundled_sha != expected_tip:
        if bundle_record is not None:
            bundle_record["reap_guard"] = "tip_moved_after_close"
            bundle_record["reap_refusal"] = "tip_moved_after_close"
            bundle_record["reap_detail"] = f"authorized {expected_tip} but bundled {bundled_sha}"
        return (False, "tip_moved_after_close")
    current = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"])
    current_sha = _probe_command_stdout(current).strip() if current is not None else ""
    if bundled_sha and current_sha and current_sha != bundled_sha:
        if bundle_record is not None:
            bundle_record["reap_guard"] = "tip_moved_after_bundle"
            bundle_record["reap_refusal"] = "tip_moved_after_bundle"
            bundle_record["reap_detail"] = f"bundled {bundled_sha} but ref now {current_sha}"
        return (False, "tip_moved_after_bundle")
    # Both actuators take their ``-d`` argv from branch_reclaim_delete, the designated delete module.
    from .orchestration.branch_reclaim_delete import (  # noqa: PLC0415
        _write_sha_guard_hook,
        merged_branch_delete_argv,
    )

    if bundled_sha:
        # ``-d`` cannot name the bundled tip. The delete runs under the same
        # reference-transaction guard as branch_reclaim_delete: its hook re-reads the
        # ref while the ref lock is held. A repository hook would be bypassed by
        # core.hooksPath, so refuse instead of deleting unguarded [CON-05].
        hook_state, hook_detail = _reference_transaction_hook_state(root)
        if hook_state != "absent":
            # An unknown probe is refused before any core.hooksPath override, the same as a present hook.
            outcome = f"reference_transaction_hook_{hook_state}"
            if bundle_record is not None:
                bundle_record["reap_guard"] = outcome
                bundle_record["reap_refusal"] = outcome
                bundle_record["reap_detail"] = hook_detail
            return (False, outcome)
        with tempfile.TemporaryDirectory(prefix="workbay-reclaim-hook-") as hook_dir:
            _write_sha_guard_hook(Path(hook_dir))
            refusal, proc, release = _branch_delete_under_authority(
                close_authority,
                task_ref=task_ref,
                lane_id=lane_id,
                actuator=lambda: _run_reclaim_command(
                    merged_branch_delete_argv(root, branch, hooks_path=hook_dir),
                    env={**os.environ, "WORKBAY_AUTH_REF": f"refs/heads/{branch}", "WORKBAY_AUTH_SHA": bundled_sha},
                ),
            )
    else:
        refusal, proc, release = _branch_delete_under_authority(
            close_authority,
            task_ref=task_ref,
            lane_id=lane_id,
            actuator=lambda: _run_reclaim_command(merged_branch_delete_argv(root, branch)),
        )
    unreleased = ""
    if release:
        # The delete ran under the fence. Its release state is reported beside the delete's own result,
        # never as a refusal; ``release_failed`` means the row stays fenced [AGT-10].
        if bundle_record is not None:
            bundle_record["reap_fence_release"] = release
        if release != "released":
            unreleased = f"{_REAP_FENCE_RELEASE_DETAIL}{release}"
            _logger.warning(
                "branch %s delete ran, but the reclaim fence release for lane %s (task %s) was %s",
                branch,
                lane_id,
                task_ref,
                release,
            )
    if refusal:
        # The verified bundle above stays as the rollback handle; only the delete is refused.
        if bundle_record is not None:
            bundle_record["reap_guard"] = refusal
            bundle_record["reap_refusal"] = refusal
            bundle_record["reap_detail"] = "branch delete refused under the row's reclaim fence"
        return (False, refusal)
    if proc is None:
        return (False, "branch_delete_unrunnable")
    if getattr(proc, "returncode", None) == 0:
        return (True, unreleased)
    if bundled_sha:
        # A refused guarded delete with the ref off the bundled tip is the drift
        # refused above, not an unrelated delete error.
        now = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"])
        now_sha = _probe_command_stdout(now).strip() if now is not None else ""
        if now_sha and now_sha != bundled_sha:
            if bundle_record is not None:
                bundle_record["reap_guard"] = "tip_moved_after_bundle"
                bundle_record["reap_refusal"] = "tip_moved_after_bundle"
                bundle_record["reap_detail"] = f"bundled {bundled_sha} but ref now {now_sha}"
            return (False, "tip_moved_after_bundle")
    detail = (getattr(proc, "stderr", "") or getattr(proc, "stdout", "") or "").strip()
    return (False, detail[:_RECLAIM_DETAIL_CAP])


def reap_merged_registry_worktrees(
    *,
    apply: bool = False,
    repo_root: Path | str | None = None,
    integration_ref: str = _RECLAIM_INTEGRATION_REF,
    owner_probe: Callable[[object], tuple[str, str]] | None = None,
    min_age_hours: float = _REGISTRY_SWEEP_MIN_AGE_HOURS,
    bundle_retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
    rowless_only: bool = False,
    deadline: float | None = None,
) -> dict:
    """Reclaim merged worktrees and branches found in git itself, not rows.

    Candidates come from ``git worktree list --porcelain`` and ``refs/heads``
    at ``repo_root``; a worktree candidate qualifies only when its branch is an
    ancestor of ``integration_ref``, is not sitting *on* that ref (a
    never-started lane is trivially an ancestor of the ref it was cut from),
    and was registered at least ``min_age_hours`` ago. ``rowless_only`` is the
    post-merge hook arm: it admits only clean sibling worktrees with no lane row
    for either path or branch, an integration-contained tip, and the dedicated
    ``WORKBAY_ROWLESS_REAP_GRACE_S`` registration grace. Worktree removal reuses the guarded
    :func:`_reclaim_lane_worktree` chain (clean, ignored, owner, heartbeat,
    shared-path, flock, removal-time re-probe). Branch deletion runs after —
    and only after — the worktree is gone, always from the integration root,
    always ``-d``. A branch named by any non-terminal lane row is refused
    outright. Dry-run by default; ordinary failures return structured errors.
    The hook watchdog may unwind the sweep immediately.

    A branch not in ``integration_ref`` is still reclaimable when a live task
    integration branch (``target_branch`` of an in-progress or review handoff
    task) preserves it: an ancestor of that ref, or with zero unlanded
    patch-equivalent commits. Such a worktree is removed with ``preserved_by``
    set, but its branch is KEPT (``branch_kept: preserved_by_non_main_ref``)
    because ``-d`` answers only to ``integration_ref``. A branch at the tip of
    a live ref is ``never_started`` and its worktree is kept. An unreadable
    live-ref list degrades to ``integration_ref`` only, reported as
    ``integration_refs_degraded``. The hook arm instead stops on that degradation
    and passes its absolute monotonic ``deadline`` through SQLite and git waits.
    """
    reclaimed: list[dict[str, object]] = []
    would_reclaim: list[dict[str, object]] = []
    deleted_branches: list[dict[str, object]] = []
    would_delete_branch: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    rowless_index_token: contextvars.Token[_RowlessLaneRowIndex | None] | None = None
    integration_refs_degraded: str | None = None
    removal_intents: list[dict[str, object]] = []
    pending_removal_failures: list[dict[str, object]] = []
    deadline_token = _REGISTRY_REAP_DEADLINE.set(deadline)
    removal_intents_token = _REGISTRY_REMOVAL_INTENTS.set(apply)

    def result(**extra: object) -> dict:
        return _with_bundle_telemetry(
            {
                "ok": not pending_removal_failures,
                "applied": apply,
                "reclaimed": reclaimed,
                "would_reclaim": would_reclaim,
                "deleted_branches": deleted_branches,
                "would_delete_branch": would_delete_branch,
                "skipped": skipped,
                "failed": failed,
                # The hook aggregates its first failure. Include the recovery identity
                # in the top-level error too, so its ok=False report stays actionable.
                **({"error": pending_removal_failures[0]["detail"]} if pending_removal_failures else {}),
                **({"integration_refs_degraded": integration_refs_degraded} if integration_refs_degraded else {}),
                **({"removal_intents": removal_intents} if removal_intents else {}),
                **extra,
            }
        )

    try:
        if repo_root is None:
            repo_root = _workspace_root()
        root = Path(repo_root)
        # Finish interrupted removals before listing, so their stale entries never reach the guards.
        removal_intents = _recover_registry_removal_intents(root, apply=apply)
        # The hook forwards failed entries, not removal_intents. Report every pending
        # recovery on every sweep while leaving its durable intent and branch intact.
        pending_removal_failures = [
            {
                **record,
                "stage": "removal_intent_recovery",
                "detail": (
                    f"{record['detail']}: worktree_path={record.get('worktree_path')!r} "
                    f"branch={record.get('branch')!r} intent={record['intent']}"
                ),
            }
            for record in removal_intents
            if record.get("state") == "pending"
        ]
        failed.extend(pending_removal_failures)
        listing = _run_reclaim_command(["git", "-C", str(root), "worktree", "list", "--porcelain"])
        if listing is None or getattr(listing, "returncode", None) != 0:
            return result(ok=False, error="worktree_registry_unavailable")
        registry = _parse_worktree_registry(_probe_command_stdout(listing))
        root_resolved = _resolved_path_text(root).rstrip("/")
        current_resolved: str | None = None
        if rowless_only:
            current = _run_reclaim_command(["git", "rev-parse", "--show-toplevel"])
            if (
                current is None
                or getattr(current, "returncode", None) != 0
                or not _probe_command_stdout(current).strip()
            ):
                return result(ok=False, error="rowless_current_worktree_unknown")
            current_resolved = _resolved_path_text(_probe_command_stdout(current).strip()).rstrip("/")
            rowless_index_token = _ROWLESS_LANE_ROW_INDEX.set(_load_rowless_lane_row_index())
        root_parent = Path(root_resolved).parent
        rowless_grace_s = _rowless_reap_grace_seconds() if rowless_only else 0.0
        checked_out = {str(entry.get("branch")) for entry in registry if entry.get("branch")}
        extra_refs, integration_refs_degraded = _live_task_integration_refs(root, integration_ref)
        _registry_reap_remaining()
        if rowless_only and integration_refs_degraded:
            return result(ok=False, error=integration_refs_degraded)
        root_branch: str | None = None
        resolved_registry_paths: dict[str, str] = {}
        for entry in registry:
            entry_path = str(entry.get("path"))
            resolved_entry_path = _resolved_path_text(entry_path).rstrip("/")
            resolved_registry_paths[entry_path] = resolved_entry_path
            if resolved_entry_path == root_resolved:
                root_branch = entry.get("branch")  # type: ignore[assignment]

        for entry in registry:
            _registry_reap_remaining()
            path = str(entry.get("path") or "")
            branch = entry.get("branch")
            record: dict[str, object] = {"worktree_path": path, "branch": branch}
            preserved_by: str | None = None
            try:
                resolved_path = resolved_registry_paths.get(path)
                if resolved_path is None:
                    resolved_path = _resolved_path_text(path).rstrip("/")
                rowless_index = _ROWLESS_LANE_ROW_INDEX.get()
                if rowless_index is not None:
                    rowless_index.resolved_worktree_paths[path] = resolved_path
                if resolved_path == root_resolved or (rowless_only and resolved_path == current_resolved):
                    if rowless_only:
                        skipped.append({**record, "reason": "keep:rowless_protected_worktree"})
                    continue
                if rowless_only and Path(resolved_path).parent != root_parent:
                    skipped.append({**record, "reason": "keep:rowless_outside_sibling_prefix"})
                    continue
                if entry.get("detached") or not branch:
                    reason = "keep:rowless_branchless" if rowless_only else "detached_or_branchless"
                    skipped.append({**record, "reason": reason})
                    continue
                branch = str(branch)
                if branch == integration_ref or branch == root_branch:
                    reason = "keep:rowless_integration_branch" if rowless_only else "integration_branch"
                    skipped.append({**record, "reason": reason})
                    continue
                if rowless_only:
                    no_row, row_detail = _rowless_lane_row_verdict(path, branch)
                    if no_row is None:
                        failed.append({**record, "stage": "rowless_row_probe", "detail": row_detail})
                        continue
                    if not no_row:
                        skipped.append({**record, "reason": "keep:rowless_has_row", "detail": row_detail})
                        continue
                    landed, landed_detail = _rowless_worktree_landed_verdict(
                        repo_root=root,
                        branch=branch,
                        worktree_path=path,
                        integration_ref=integration_ref,
                    )
                    if landed is None:
                        failed.append({**record, "stage": "rowless_merge_probe", "detail": landed_detail})
                        continue
                    if not landed:
                        # Not contained in main: a live task integration branch may still preserve it.
                        preserved_by, preserve_reason, preserve_detail = _preserving_integration_ref(
                            repo_root=root,
                            branch=branch,
                            worktree_path=path,
                            refs=extra_refs,
                        )
                        if preserve_reason == "merge_unknown":
                            failed.append({**record, "stage": "rowless_merge_probe", "detail": preserve_detail})
                            continue
                        if preserved_by is None:
                            kept_reason = (
                                "keep:rowless_never_started"
                                if preserve_reason == "never_started"
                                else "keep:rowless_unique_commits"
                            )
                            skipped.append(
                                {**record, "reason": kept_reason, "detail": preserve_detail or landed_detail}
                            )
                            continue
                    clean, clean_detail = _probe_worktree_clean(path)
                    if clean is None:
                        failed.append({**record, "stage": "rowless_status_probe", "detail": clean_detail})
                        continue
                    if not clean:
                        skipped.append({**record, "reason": "keep:rowless_dirty", "detail": clean_detail})
                        continue
                    age_hours = _worktree_registration_age_hours(path)
                    if age_hours is None or age_hours * 3600.0 < rowless_grace_s:
                        age_detail = (
                            "registration_age_unknown"
                            if age_hours is None
                            else f"age_s={age_hours * 3600.0:.1f} grace_s={rowless_grace_s:g}"
                        )
                        skipped.append({**record, "reason": "keep:rowless_grace", "detail": age_detail})
                        continue
                else:
                    merged = _branch_merged_into_integration(root, branch, integration_ref)
                    if merged is False:
                        # Not contained in main: a live task integration branch may still preserve it.
                        preserved_by, preserve_reason, preserve_detail = _preserving_integration_ref(
                            repo_root=root,
                            branch=branch,
                            worktree_path=path,
                            refs=extra_refs,
                        )
                        if preserved_by is None:
                            skipped.append(
                                {
                                    **record,
                                    "reason": preserve_reason,
                                    **({"detail": preserve_detail} if preserve_detail else {}),
                                }
                            )
                            continue
                    elif merged is not True:
                        skipped.append({**record, "reason": "merge_unknown"})
                        continue
                    else:
                        at_tip = _branch_is_at_integration_tip(root, branch, integration_ref)
                        if at_tip is not False:
                            skipped.append(
                                {
                                    **record,
                                    "reason": "never_started" if at_tip else "integration_tip_unknown",
                                }
                            )
                            continue
                    age_hours = _worktree_registration_age_hours(path)
                    if age_hours is None:
                        skipped.append({**record, "reason": "registration_age_unknown"})
                        continue
                    if age_hours < min_age_hours:
                        skipped.append(
                            {
                                **record,
                                "reason": "within_registration_grace",
                                "detail": f"age_hours={age_hours:.2f} floor={min_age_hours:g}",
                            }
                        )
                        continue
                    open_row, open_detail = _open_lane_row_names_branch(branch)
                    if open_row is None:
                        failed.append(
                            {
                                **record,
                                "stage": "open_row_probe",
                                "detail": open_detail,
                            }
                        )
                        continue
                    if open_row:
                        skipped.append(
                            {
                                **record,
                                "reason": "open_lane_row",
                                "detail": open_detail,
                            }
                        )
                        continue
                try:
                    task_ref, lane_id = _row_identity_for_worktree_path(path)
                except Exception:  # noqa: BLE001 — an unreadable row never authorizes removal
                    failed.append({**record, "stage": "close_authority", "detail": "lane_row_read_failed"})
                    continue
                if lane_id is None:
                    lane_id = f"registry-{Path(path).name}"
                # A branch kept for a live non-main ref is never queued. Any other removal
                # records its branch hand-off in the removal intent before ``git worktree
                # remove``, so an interruption after the remove still queues the branch.
                handoff = (
                    None
                    if preserved_by
                    else _RegistryBranchHandoff(
                        *_registry_reclaim_queue_identity(task_ref, lane_id, branch), branch=branch
                    )
                )
                handoff_token = _REGISTRY_BRANCH_HANDOFF.set(handoff)
                try:
                    outcome, detail = _reclaim_lane_worktree(
                        worktree_path=path,
                        branch=branch,
                        repo_root=root,
                        apply=apply,
                        owner_probe=owner_probe,
                        integration_ref=integration_ref,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        allow_rowless_work_complete=rowless_only,
                        preserving_ref=preserved_by,
                    )
                finally:
                    _REGISTRY_BRANCH_HANDOFF.reset(handoff_token)
                record = {**record, **_worktree_reclaim_release_fields(detail)}
                # A worktree preserved only by a live task ref leaves its branch alone:
                # ``-d`` answers to the integration root's main ancestry, not to that ref.
                kept_fields: dict[str, object] = (
                    {"preserved_by": preserved_by, "branch_kept": _PRESERVED_BRANCH_KEPT} if preserved_by else {}
                )
                if outcome == "would_reclaim":
                    attributed_record = {
                        **record,
                        "detail": detail,
                        "ignored_rule_histogram": _ignored_rule_histogram_from_detail(detail),
                        **kept_fields,
                    }
                    would_reclaim.append(attributed_record)
                    if not kept_fields:
                        would_delete_branch.append(attributed_record)
                elif outcome == "reclaimed":
                    reclaim_record: dict[str, object] = {
                        **record,
                        "detail": detail,
                        "ignored_rule_histogram": _ignored_rule_histogram_from_detail(detail),
                        **kept_fields,
                    }
                    reclaimed.append(reclaim_record)
                    # Worktree is gone; drop the pre-remove snapshot so a
                    # failed delete can be retried by the branch-only arm
                    # in this same run instead of waiting for the next tick.
                    checked_out.discard(branch)
                    if kept_fields:
                        continue
                    # The close authority binds the branch delete as it does on the
                    # branch-only arm. The worktree is already gone, so a refusal leaves
                    # the branch for that arm or the next tick.
                    identity_row, authority, authority_refusal = _durable_close_authority(
                        task_ref=task_ref, lane_id=lane_id
                    )
                    if authority_refusal:
                        failed.append({**record, "stage": "close_authority", "detail": authority_refusal})
                        continue
                    authorized_tip = None if authority is None else str(authority["authorized_tip"])
                    if handoff is None or not handoff.queued:
                        _enqueue_merged_registry_reclaim_candidate(
                            root=root,
                            branch=branch,
                            task_ref=task_ref,
                            lane_id=lane_id,
                            failed=failed,
                            worktree_path=path,
                            authorized_sha=authorized_tip,
                        )
                    bundle_record: dict[str, object] = {}
                    deleted, delete_detail = _delete_merged_branch(
                        root,
                        branch,
                        bundle_record=bundle_record,
                        retention_count=bundle_retention_count,
                        expected_tip=authorized_tip,
                        close_authority=authority,
                        task_ref=task_ref,
                        lane_id=lane_id,
                    )
                    rollback = {key: bundle_record[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle_record}
                    if rollback:
                        # The reclaimed record is the operator's rollback handle;
                        # it must name the bundle, not merely report a deletion.
                        reclaim_record.update(rollback)
                    if deleted:
                        deleted_branches.append({**record, **rollback, **_reap_fence_release(bundle_record)})
                    else:
                        entry = {**record, **bundle_record, "stage": "branch_delete", "detail": delete_detail}
                        if delete_detail == "tip_moved_after_close" and identity_row is not None:
                            entry["reopen"] = _reopen_moved_closed_row(identity_row)
                        failed.append(entry)
                else:
                    rowless_reasons = {
                        "refused_dirty": "keep:rowless_dirty",
                        "refused_owned": "keep:rowless_live_owner",
                        "refused_owner_unknown": "keep:rowless_live_owner",
                        "refused_session_live": "keep:rowless_live_owner",
                        "refused_shared_path": "keep:rowless_live_owner",
                        "refused_lock_held": "keep:rowless_live_owner",
                        "refused_rowless_row_present": "keep:rowless_has_row",
                        "refused_unmerged": "keep:rowless_unique_commits",
                    }
                    reason = rowless_reasons.get(outcome, f"keep:rowless_{outcome}") if rowless_only else outcome
                    skipped.append({**record, "reason": reason, "detail": detail})
            except Exception as exc:  # noqa: BLE001 — per-entry degrade
                _registry_reap_remaining()
                failed.append({**record, "stage": "worktree", "detail": str(exc)})

        if rowless_only:
            if rowless_index_token is not None:
                _ROWLESS_LANE_ROW_INDEX.reset(rowless_index_token)
                rowless_index_token = None
            return result(skipped_counts=_registry_skip_counts(skipped))

        heads = _run_reclaim_command(
            ["git", "-C", str(root), "for-each-ref", "--format=%(refname:short)", "refs/heads"]
        )
        if heads is not None and getattr(heads, "returncode", None) == 0:
            for branch in _probe_command_stdout(heads).splitlines():
                branch = branch.strip()
                record = {"worktree_path": None, "branch": branch}
                try:
                    if not branch:
                        continue
                    if any(str(item.get("branch")) == branch for item in deleted_branches):
                        continue
                    if branch == integration_ref or branch == root_branch:
                        skipped.append({**record, "reason": "integration_branch"})
                        continue
                    merged = _branch_merged_into_integration(root, branch, integration_ref)
                    if merged is not True:
                        skipped.append({**record, "reason": "unmerged" if merged is False else "merge_unknown"})
                        continue
                    open_row, open_detail = _open_lane_row_names_branch(branch)
                    if open_row is None:
                        failed.append(
                            {
                                **record,
                                "stage": "open_row_probe",
                                "detail": open_detail,
                            }
                        )
                        continue
                    if open_row:
                        skipped.append(
                            {
                                **record,
                                "reason": "open_lane_row",
                                "detail": open_detail,
                            }
                        )
                        continue
                    try:
                        task_ref, lane_id = _row_identity_for_branch(branch)
                    except Exception:  # noqa: BLE001 — an unreadable row never authorizes deletion
                        failed.append({**record, "stage": "close_authority", "detail": "lane_row_read_failed"})
                        continue
                    if branch in checked_out:
                        skipped.append({**record, "reason": "skipped_checked_out"})
                        if apply:
                            _enqueue_merged_registry_reclaim_skip(
                                root=root,
                                branch=branch,
                                task_ref=task_ref,
                                lane_id=lane_id,
                                failed=failed,
                                reason="skipped_checked_out",
                            )
                        continue
                    # A closed row's authorized tip binds the delete. A rowless
                    # or legacy branch has no authority, so it keeps today's path.
                    identity_row, authority, authority_refusal = _durable_close_authority(
                        task_ref=task_ref, lane_id=lane_id
                    )
                    if authority_refusal:
                        failed.append({**record, "stage": "close_authority", "detail": authority_refusal})
                        continue
                    if not apply:
                        would_delete_branch.append(record)
                        continue
                    authorized_tip = None if authority is None else str(authority["authorized_tip"])
                    _enqueue_merged_registry_reclaim_candidate(
                        root=root,
                        branch=branch,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        failed=failed,
                        authorized_sha=authorized_tip,
                    )
                    bundle_record = {}
                    deleted, delete_detail = _delete_merged_branch(
                        root,
                        branch,
                        bundle_record=bundle_record,
                        retention_count=bundle_retention_count,
                        expected_tip=authorized_tip,
                        close_authority=authority,
                        task_ref=task_ref,
                        lane_id=lane_id,
                    )
                    rollback = {key: bundle_record[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle_record}
                    if deleted:
                        deleted_branches.append({**record, **rollback, **_reap_fence_release(bundle_record)})
                        failed[:] = [
                            item
                            for item in failed
                            if not (item.get("branch") == branch and item.get("stage") == "branch_delete")
                        ]
                    else:
                        entry = {**record, **bundle_record, "stage": "branch_delete", "detail": delete_detail}
                        if delete_detail == "tip_moved_after_close" and identity_row is not None:
                            entry["reopen"] = _reopen_moved_closed_row(identity_row)
                        failed.append(entry)
                except Exception as exc:  # noqa: BLE001 — per-entry degrade
                    failed.append({**record, "stage": "branch", "detail": str(exc)})
        else:
            failed.append(
                {"worktree_path": None, "branch": None, "stage": "branch_listing", "detail": "refs_unavailable"}
            )
    except Exception as exc:  # noqa: BLE001 — never-raise sweep [RES-07]
        if rowless_index_token is not None:
            _ROWLESS_LANE_ROW_INDEX.reset(rowless_index_token)
            rowless_index_token = None
        return result(ok=False, error=str(exc))
    finally:
        if rowless_index_token is not None:
            _ROWLESS_LANE_ROW_INDEX.reset(rowless_index_token)
        _REGISTRY_REMOVAL_INTENTS.reset(removal_intents_token)
        _REGISTRY_REAP_DEADLINE.reset(deadline_token)

    return result(skipped_counts=_registry_skip_counts(skipped))
