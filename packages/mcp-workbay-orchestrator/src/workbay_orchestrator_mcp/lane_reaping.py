"""Blocked-lane aging and reaping operations."""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from .lanes_support import (
    _BRANCH_PROBE_TIMEOUT_S,
    _LANE_STATUS_BLOCKED,
    _LANE_STATUS_CLOSED_STALE,
    CLOSEABLE_LANE_STATUSES,
    DEFAULT_BLOCKED_LANE_REAP_BATCH,
    _get_db_connection,
    _json_response,
    _normalize_optional_text,
    _workspace_root,
)


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
) -> bool | None:
    """Return True if branch is deleted or merged into HEAD; False if live; None if unknown.

    Resolution order:
    1. Normalize the name (strip ``refs/heads/``; strip a leading ``<remote>/``
       only when that segment names a configured remote — never a blind first
       path-segment strip).
    2. If ``refs/heads/<name>`` exists, judge by merge into HEAD.
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
            configured = {
                line.strip() for line in _probe_command_stdout(remotes_list).splitlines() if line.strip()
            }
            if maybe_remote in configured:
                ref = ref[slash + 1 :]

        def _merged_into_head(git_ref: str) -> bool | None:
            merged = _run_reclaim_command(
                ["git", "-C", cwd_str, "merge-base", "--is-ancestor", git_ref, "HEAD"],
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
            return _merged_into_head(f"refs/heads/{ref}")

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
                verdict = _merged_into_head(remote_ref)
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
#: One ps snapshot of every process. STAT is requested so zombies can be
#: excluded: ``ps -p <pid>`` exits 0 for a zombie, so a dead worker otherwise
#: reads as a live owner and no worktree is ever reclaimable.
_PS_SNAPSHOT_CMD: tuple[str, ...] = ("ps", "-Ao", "pid=,stat=,command=")
#: One lsof snapshot of every process cwd (``-Fpn`` → ``p<pid>`` / ``n<path>``).
#: Deliberately NOT ``+D <worktree>``: that stats the whole tree per lane, and
#: this runs twice per candidate inside a batch sweep.
_LSOF_CWD_SNAPSHOT_CMD: tuple[str, ...] = ("lsof", "-w", "-d", "cwd", "-Fpn")
_RECLAIM_PROBE_TIMEOUT_S = 20.0
_RECLAIM_DETAIL_CAP = 300
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
) -> subprocess.CompletedProcess[str] | None:
    """Run a probe/removal command. ``None`` means the command could not run."""
    _assert_no_reaper_write_lock(reason="subprocess")
    try:
        return subprocess.run(  # noqa: S603 — fixed argv, no shell
            list(argv),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired, subprocess.SubprocessError, UnicodeError):
        return None


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


def _ps_argv_worktree_owners(ps_output: str, *, worktree_paths: Sequence[str]) -> list[str]:
    """PURE: ``pid stat command`` records whose argv names one of the paths.

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
        pid, stat, command = parts
        if not pid.isdigit() or stat.startswith("Z"):
            continue
        if any(needle in command for needle in needles):
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
        proc = _run_reclaim_command(_PS_SNAPSHOT_CMD)
        if proc is None or proc.returncode != 0:
            return ("unknown", "ps_probe_failed")
        ps_output = proc.stdout or ""
    argv_owners = _ps_argv_worktree_owners(ps_output, worktree_paths=paths)
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


def _probe_worktree_ignored(worktree_path: str) -> tuple[bool | None, str]:
    """Refuse when the worktree holds gitignored content (secrets/logs).

    ``git status --porcelain`` hides ignored paths, so a tree that looks clean
    can still hold ``.env`` files that ``git worktree remove`` would delete
    unrecoverably. ``None`` means the probe could not answer (fail-closed).
    """
    proc = _run_reclaim_command(["git", "-C", worktree_path, "status", "--porcelain", "--ignored"])
    if proc is None or proc.returncode != 0:
        return (None, "ignored_probe_failed")
    ignored = [line for line in (proc.stdout or "").splitlines() if line.startswith("!!")]
    if ignored:
        return (False, f"ignored_content:{len(ignored)}")
    return (True, "")


def _name_only_path_set(stdout: str | None) -> set[str]:
    return {line for line in (stdout or "").splitlines() if line}


def _merged_by_tree_untrusted_graph_reason(cwd: Path | str) -> str | None:
    """Refuse when grafts or a shallow file rewrite the stored graph.

    ``--no-replace-objects`` does not disable grafts. A shallow file
    truncates history so merge-base is not reachability in the stored
    object graph. Either file is ``None`` / ``merged_by_tree_probe_failed``.
    An unreadable common-dir lookup is the same refuse — never land.
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
        if (common / "shallow").is_file():
            return "merged_by_tree_probe_failed"
    except OSError:
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
    Grafts and a shallow file rewrite history; those refuse the same way.
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
    untrusted = _merged_by_tree_untrusted_graph_reason(cwd)
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


def _shared_path_blocks_reclaim(
    *,
    worktree_path: str,
    task_ref: str | None,
    lane_id: str | None,
) -> tuple[bool, str]:
    """Fail-closed extra gate: another lane owns the same worktree path."""
    try:
        from workbay_orchestrator_mcp.orchestration.lane_reclaim import (  # noqa: PLC0415
            _list_lanes_by_worktree_path,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed
        return (True, f"shared_path_import_failed:{exc}"[:_RECLAIM_DETAIL_CAP])
    try:
        env = _list_lanes_by_worktree_path(worktree_path=worktree_path)
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
    for owner in lanes:
        if not isinstance(owner, dict):
            return (True, "shared_path_malformed_owner")
        owner_id = owner.get("lane_id")
        owner_task = owner.get("task_ref")
        if owner_id == lane_id and owner_task == task_ref:
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
    raise CloseWindowBarrierExpired(
        f"close-window barrier expired waiting for ensure_lane_worktree entry at {inside}"
    )


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
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _write_lock_claim_payload(handle, claim=claim)
        return (handle, "")
    except BlockingIOError:
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass
        return (None, "worker_lock_held")
    except OSError as exc:
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass
        return (None, f"worker_lock_unavailable:{exc}"[:_RECLAIM_DETAIL_CAP])


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
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        handle.close()
    except Exception:  # noqa: BLE001 — best-effort release
        pass


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
    triage: list[str],
    sweep_label: str,
) -> dict[str, object]:
    """Skip a row whose reaping claim could not be taken. Returns the updated entry.

    Peer-held → alive (retry next tick). Structural → persist a blocker, put
    the row on the failed/WARN path, and stamp ``blocker`` so this sweep's
    dashboard line renders the text the operator actually sees.
    """
    reason = f"reaping_claim_unavailable: {claim_detail}"
    if _is_peer_held_reaping_claim(claim_detail):
        updated = {**entry, "verdict": "alive", "reason": reason}
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


def _remove_lane_worktree(
    repo_root: Path | str,
    worktree_path: str,
    *,
    runner: Callable[[list[str]], object] | None = None,
) -> tuple[bool, str]:
    """Remove a linked worktree with the SAFE form of ``git worktree remove``.

    No escalation exists here on purpose: git's own refusal (dirty tree, locked
    worktree, submodules) is the last guard, and this reaper has no mandate to
    override it. A refusal is reported and the worktree is left alone. Branch
    refs are never touched — a worktree is removed, never a ref.
    """
    argv = ["git", "-C", str(repo_root), "worktree", "remove", str(worktree_path)]
    run = runner if runner is not None else _run_reclaim_command
    proc = run(argv)
    if proc is None:
        return (False, "git_worktree_remove_unavailable")
    # None / non-int returncode is FAILURE — `int(rc or 0)` would coerce None to success.
    rc = getattr(proc, "returncode", 1)
    if type(rc) is not int or rc != 0:
        detail = str(getattr(proc, "stderr", "") or getattr(proc, "stdout", "") or "").strip()
        return (False, (detail or "git_worktree_remove_failed")[:_RECLAIM_DETAIL_CAP])
    return (True, "")


def _worktree_head_is_recorded_branch(worktree_path: str, branch: object) -> bool | None:
    """Is this worktree's HEAD attached to the recorded lane branch?

    ``None`` when HEAD or the recorded branch name cannot be read. Detached
    HEAD and HEAD attached to any other branch are ``False``. The squash-land
    override may apply only when this returns exactly ``True``.
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
) -> tuple[bool | None, str]:
    """SHA merge plus the recorded-branch tree override.

    The override applies only when worktree HEAD is the recorded lane
    branch. Detached HEAD, a different attached branch, and an unreadable
    HEAD all refuse rather than inherit a landed-true for the recorded ref.
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
        if (
            merged_detail == "worktree_head_not_merged"
            and _worktree_head_is_recorded_branch(worktree_path, branch) is not True
        ):
            return (False, merged_detail)
        tree_eq, tree_detail = _probe_branch_merged_by_tree(
            branch, repo_root=repo_root, integration_ref=integration_ref
        )
        if tree_eq is True:
            return (True, tree_detail)
        return (False, merged_detail)
    return (True, merged_detail)


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
) -> tuple[str, str]:
    """Remove one lane worktree if — and only if — every guard proves it safe.

    Guard order (cheapest and most absolute first; each one fails closed):

    1. a worktree path is recorded and a repo root resolves;
    2. the path is a REGISTERED LINKED worktree of that repo (this is what
       excludes the primary checkout and any unrelated directory);
    2b. the resolved target is not the repo root itself (a linked worktree
        used as ``repo_root`` must not be a legal removal target of its own
        pass);
    3. the branch (and the tree's HEAD) is fully merged into ``integration_ref``;
    4. ``git status --porcelain`` in the worktree is empty;
    4b. ``git status --porcelain --ignored`` reports no ignored content;
    5. no live, non-zombie process owns the worktree;
    5b. ``_session_live`` heartbeat is not live (``None``/unknown fail-closed)
        and no cross-task peer owns the path;
    5c. the lane's worker flock is acquired exclusively non-blocking around
        the removal-time re-probe + remove (TOCTOU). When the caller already
        holds a durable reaping claim, pass it as ``held_lock`` so this helper
        neither re-acquires nor releases the spanning claim.

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

    merged, merged_detail = _landed_reclaim_verdict(
        repo_root=root,
        branch=branch,
        worktree_path=path,
        integration_ref=integration_ref,
    )
    if merged is None:
        return ("refused_merge_unknown", merged_detail)
    if merged is False:
        return ("refused_unmerged", merged_detail)
    authorizing = (merged, merged_detail)

    clean, clean_detail = _probe_worktree_clean(path)
    if clean is None:
        return ("refused_status_unknown", clean_detail)
    if clean is False:
        return ("refused_dirty", clean_detail)

    ignored_ok, ignored_detail = _probe_worktree_ignored(path)
    if ignored_ok is None:
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
    )
    if shared_blocks:
        return ("refused_shared_path", shared_detail)

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
        if not apply:
            return ("would_reclaim", merged_detail)

        # Re-derive liveness AT REMOVAL TIME while holding the worker flock
        # so a peer cannot claim the tree between the verdict and the delete.
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

        # Re-run dirty + ignored under the flock. Pre-lock probes can pass and
        # still lose a TOCTOU against ignored content created before remove.
        clean, clean_detail = _probe_worktree_clean(path)
        if clean is None:
            return ("refused_status_unknown", clean_detail)
        if clean is False:
            return ("refused_dirty", clean_detail)

        ignored_ok, ignored_detail = _probe_worktree_ignored(path)
        if ignored_ok is None:
            return ("refused_ignored_unknown", ignored_detail)
        if ignored_ok is False:
            return ("refused_ignored", ignored_detail)

        # Re-run merge + landed override under the flock. A commit created
        # after the pre-lock sample is invisible to porcelain; the early
        # True is not permission to delete.
        locked_merged, locked_detail = _landed_reclaim_verdict(
            repo_root=root,
            branch=branch,
            worktree_path=path,
            integration_ref=integration_ref,
        )
        if locked_merged is None:
            return ("refused_merge_unknown", locked_detail)
        if locked_merged is False:
            return ("refused_unmerged", locked_detail)
        if (locked_merged, locked_detail) != authorizing:
            return ("refused_merge_changed", locked_detail)
        merged_detail = locked_detail

        removed, remove_detail = _remove_lane_worktree(root, path)
        if removed:
            return ("reclaimed", merged_detail)
        return ("remove_failed", remove_detail)
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
    raced/skipped telemetry reason. Tree-equality closes re-run the tree
    probe rather than the SHA branch probe (which can still be False).
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
        return None
    if branch_dead is True:
        try:
            dead = branch_probe(branch)
        except Exception:  # noqa: BLE001 — recency degrade is a race
            dead = None
        if dead is not True:
            return "liveness raced: branch no longer dead"
    return None


DEFAULT_BRANCH_RECLAIM_DRAIN_BATCH = 25


def _branch_reclaim_item_summary(item: Any, result: Any, *, acked: bool | None = None) -> dict[str, object]:
    summary: dict[str, object] = {
        "task_ref": item.task_ref,
        "lane_id": item.lane_id,
        "branch": item.branch,
        "authorized_sha": item.authorized_sha,
        "reason": item.reason,
        "force_authorized": item.force_authorized,
        "delete_reason": result.reason,
    }
    if acked is not None:
        summary["acked"] = acked
    return summary


def _drain_one_branch_reclaim_item(
    item: Any,
    *,
    apply: bool,
    orchestrator_root: Path,
    integration_ref: str,
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
    result = delete_authorized_branch(
        orchestrator_root=orchestrator_root,
        lane_id=item.lane_id,
        branch=item.branch,
        authorized_sha=item.authorized_sha,
        apply=apply,
        task_ref=item.task_ref,
        integration_ref=integration_ref,
    )
    if result.reason == "would_delete":
        would_drain.append(_branch_reclaim_item_summary(item, result))
        return None
    if apply and (result.deleted or result.reason == "branch_missing"):
        drained.append(_branch_reclaim_item_summary(item, result, acked=False))
        return item
    if apply and result.reason == "probe_failed":
        record_branch_reclaim_failure(item=item, error=result.detail or result.reason)
        probe_failed.append(_branch_reclaim_item_summary(item, result, acked=False))
        return None
    if apply and result.reason in {"live_proof_failed", "delete_failed"}:
        record_branch_reclaim_failure(item=item, error=result.detail or result.reason)
    skipped.append(_branch_reclaim_item_summary(item, result))
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
) -> None:
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415
    from workbay_handoff_mcp.shared_schema import connect_handoff_db  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
        acknowledge_branch_reclaim_item,
        store_branch_reclaim_drain_cursor,
    )

    if not pending_acks and not examined_any:
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
            if examined_any:
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
            to_ack = _drain_one_branch_reclaim_item(
                item,
                apply=apply,
                orchestrator_root=root,
                integration_ref=integration_ref,
                drained=result["drained"],  # type: ignore[arg-type]
                would_drain=result["would_drain"],  # type: ignore[arg-type]
                skipped=result["skipped"],  # type: ignore[arg-type]
                probe_failed=result["probe_failed"],  # type: ignore[arg-type]
            )
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
        )
        return result
    except Exception as exc:  # noqa: BLE001 — never-raise reaper arm
        result["ok"] = False
        result["error"] = str(exc)
        return result


def _with_branch_reclaim_drain(
    payload: dict[str, object],
    *,
    apply: bool,
    task_ref: str | None,
    orchestrator_root: Path | str | None,
) -> dict[str, object]:
    payload["branch_reclaim"] = drain_branch_reclaim_queue(
        apply=apply,
        task_ref=task_ref,
        orchestrator_root=orchestrator_root,
    )
    return payload


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
) -> dict:
    """Report non-terminal lane age/task/blocker; CAS-close conclusive-dead to ``closed_stale``.

    Naming note: historically this reaper only selected ``status='blocked'``
    (hence the name). The candidate set is now every **non-terminal** status
    (``COALESCE(status, '') NOT IN`` :data:`_ARCHIVED_ORPHAN_TERMINAL_STATUSES`).
    The rename blast radius across daemon/tests is deferred; the name understates
    the widened scope.

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

    path_probe = worktree_probe or _probe_worktree_gone
    br_probe = branch_probe or _probe_branch_dead

    def _attach_registry_sweep(payload: dict) -> dict:
        # Row-driven candidates cannot see resources whose rows are already
        # terminal; the registry sweep closes that recall hole from git itself.
        # A scoped call is a blast-radius cap; the sweep has no task_ref and
        # must not widen it into repo-wide ref deletion.
        if reclaim_worktrees:
            if scoped is not None:
                payload["registry_sweep"] = {"skipped": "scoped_call"}
            else:
                try:
                    payload["registry_sweep"] = reap_merged_registry_worktrees(
                        apply=apply,
                        repo_root=reclaim_repo_root,
                        owner_probe=worktree_owner_probe,
                    )
                except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]
                    payload["registry_sweep"] = {"ok": False, "applied": apply, "error": str(exc)}
        return payload
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
            scope_sql = ""
            if scoped is not None:
                scope_sql = "AND task_ref = ? "
                params.append(scoped)
            # Blocked-first within the LIMIT so younger blocked rows are not
            # starved by older non-blocked fillers (A2).
            params.append(_LANE_STATUS_BLOCKED)
            params.append(batch)
            rows = conn.execute(
                f"""
                SELECT id, task_ref, lane_id, title, objective, worktree_path, branch,
                       owner_agent, status, notes, created_at, updated_at
                FROM worktree_lanes
                WHERE COALESCE(status, '') NOT IN ({placeholders})
                  AND (
                    status = ?
                    OR datetime(COALESCE(updated_at, created_at))
                       <= datetime(?, ?)
                  )
                  {scope_sql}
                ORDER BY CASE WHEN status = ? THEN 0 ELSE 1 END,
                         datetime(COALESCE(updated_at, created_at)) ASC, id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()

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

                entries = collect_blocked_lane_aging_entries(
                    lane_maps,
                    reports=reports,
                    now=clock,
                    conn=conn,
                )
                by_pk = {int(cast(int, e["id"])): e for e in entries if e.get("id") is not None}

                for row in rows:
                    entry = by_pk.get(int(row["id"]))
                    if entry is None:
                        # Non-blocked candidate admitted by the widened query;
                        # collect_blocked_lane_aging_entries only shapes blocked rows.
                        lane_id = _normalize_optional_text(row["lane_id"]) or str(row["id"])
                        row_task = _normalize_optional_text(row["task_ref"]) or "?"
                        status = str(row["status"] or "")
                        age = format_lane_age_label(row["updated_at"], row["created_at"], now=clock)
                        blocker = _last_blocker_text_for_lane(
                            conn,
                            task_ref=row_task,
                            lane_id=lane_id,
                            notes=row["notes"],
                            reports=reports,
                        )
                        entry = {
                            "id": row["id"],
                            "task_ref": row_task,
                            "lane_id": lane_id,
                            "status": status,
                            "worktree_path": row["worktree_path"],
                            "branch": row["branch"],
                            "updated_at": row["updated_at"],
                            "created_at": row["created_at"],
                            "age": age,
                            "blocker": blocker,
                            "notes": row["notes"],
                        }
                    materialized.append((dict(row), dict(entry)))

        if empty_payload is not None:
            return _json_response(
                _with_branch_reclaim_drain(
                    _attach_registry_sweep(empty_payload),
                    apply=apply,
                    task_ref=task_ref,
                    orchestrator_root=reclaim_repo_root,
                )
            )

        # Probe phase: no write transaction is open. Git/ps/lsof/heartbeat
        # subprocesses must not run while this process holds the WAL writer.
        for row, entry in materialized:
            reported.append(entry)
            try:
                worktree_gone = path_probe(row["worktree_path"])
            except Exception as exc:  # noqa: BLE001 — per-row degrade
                worktree_gone = None
                triage.append(f"lane {entry['lane_id']}: worktree probe raised: {exc}")
            try:
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
                    branch_dead = True
                    verdict, reason = _classify_blocked_lane_liveness(
                        worktree_gone=worktree_gone,
                        branch_dead=True,
                    )
                    reason = f"merged_by_tree: {reason}"
                    entry = {**entry, "merged_by_tree": True, "merged_by_tree_detail": tree_detail}
                # Probe error or trees differ: keep alive (fail-closed).
            entry = {
                **entry,
                "worktree_gone": worktree_gone,
                "branch_dead": branch_dead,
                "verdict": verdict,
                "reason": reason,
            }
            # Refresh reported list item with probe fields.
            reported[-1] = entry

            if verdict == "alive":
                alive.append(entry)
                continue

            claim_handle: Any | None = None
            claim_transferred = False
            try:
                will_reclaim = (
                    reclaim_worktrees and verdict != "dead" and branch_dead is True and worktree_gone is False
                )
                if apply and (will_reclaim or verdict == "dead"):
                    claim_handle, claim_detail = _try_row_reaping_claim(entry.get("lane_id"))
                    if claim_handle is None:
                        # Exclusive claim failed on either arm: a live worker
                        # or in-flight rematerialize owns the lock, or we
                        # cannot resolve it. Do not reclaim or CAS-close a
                        # row we could not exclusively claim — the will_reclaim
                        # arm is the one that removes a worktree, and dropping
                        # the spanning flock there is the close-after-rebuild hole.
                        # Peer-held is retryable alive; structural is owned via
                        # a persisted blocker on the failed/WARN path (OBS-08).
                        entry = _record_unclaimed_reaping_row(
                            entry=entry,
                            claim_detail=claim_detail,
                            alive=alive,
                            failed=failed,
                            triage=triage,
                            sweep_label="blocked lane",
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
                        )
                    except Exception as exc:  # noqa: BLE001 — per-row degrade
                        reclaim_outcome = "reclaim_probe_raised"
                        reclaim_detail = str(exc)
                    entry = {
                        **entry,
                        "worktree_reclaim": reclaim_outcome,
                        "worktree_reclaim_detail": reclaim_detail,
                    }
                    reported[-1] = entry
                    if reclaim_outcome == "reclaimed":
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
                    elif reclaim_outcome != "would_reclaim":
                        triage.append(
                            f"blocked lane {entry['lane_id']} task={entry['task_ref']}: "
                            f"worktree reclaim {reclaim_outcome}: {reclaim_detail}"
                        )

                if verdict != "dead":
                    if reclaim_outcome == "would_reclaim":
                        # Dry-run: nothing was removed. Report the reclaim and
                        # the close it would unblock, in that order.
                        would_reclaim.append(entry)
                        would_close.append(
                            {
                                **entry,
                                "note": (
                                    "closed_stale by blocked-lane reaper: would reclaim worktree then close_stale"
                                ),
                            }
                        )
                        continue
                    ambiguous.append(entry)
                    triage.append(
                        f"blocked lane {entry['lane_id']} task={entry['task_ref']} age={entry['age']}: {reason}"
                    )
                    continue

                note = f"closed_stale by blocked-lane reaper: {reason}"
                close_entry = {**entry, "note": note}
                would_close.append(close_entry)
                if apply:
                    pending_closes.append((row, close_entry, note, claim_handle))
                    claim_transferred = True
            finally:
                if claim_handle is not None and not claim_transferred:
                    _release_lane_worker_lock(claim_handle)

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
        return _json_response(
            {
                "ok": True,
                "applied": apply,
                "max_batch": batch,
                "truncated": len(reported) >= batch,
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
                "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
            }
        )

    payload = {
        "ok": True,
        "applied": apply,
        "max_batch": batch,
        # PMH-F12 parity with reap_task_archived_orphan_lanes: a full batch
        # means the LIMIT was hit and more non-terminal orphans may remain.
        "truncated": len(reported) >= batch,
        "reported": reported,
        "closed": closed,
        "would_close": would_close,
        "ambiguous": ambiguous,
        "alive": alive,
        "triage": triage,
        "failed": failed,
        "reclaimed": reclaimed,
        "would_reclaim": would_reclaim,
        "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
    }
    return _json_response(
        _with_branch_reclaim_drain(
            _attach_registry_sweep(payload),
            apply=apply,
            task_ref=task_ref,
            orchestrator_root=reclaim_repo_root,
        )
    )


# 0112 Bug 2: statuses a lane is already terminal in — never re-close these.
# Source of truth: CLOSEABLE_LANE_STATUSES (lanes.py:19) plus the reaper's
# closed_stale; 'archived' is a defensive extra (not a canonical LANE_STATUS).
_ARCHIVED_ORPHAN_TERMINAL_STATUSES: tuple[str, ...] = tuple(
    sorted(CLOSEABLE_LANE_STATUSES | {_LANE_STATUS_CLOSED_STALE, "archived"})
)


def reap_task_archived_orphan_lanes(
    *,
    apply: bool = False,
    max_batch: int = DEFAULT_BLOCKED_LANE_REAP_BATCH,
    task_ref: str | None = None,
    now: datetime | None = None,
) -> dict:
    """CAS-close lanes whose owning task is ARCHIVED to ``closed_stale`` (0112 Bug 2).

    A lane orphaned by task archival — ``task_ref`` present in ``task_archives`` and
    ABSENT from live ``handoff_state`` — is reaped regardless of lane status
    (including ``blocked``, which may also be visible to
    :func:`reap_blocked_lanes`). This is an unconditional archival path, not a
    claim that those rows are invisible to the blocked-lane reaper. A LIVE-task
    lane (``task_ref`` still in ``handoff_state``) is NEVER touched (the
    deliberate no-force-close contract holds). Reuses the blocked-lane CAS-close
    path. Pass ``task_ref`` to scope to one finishing task (daemon-less self-heal,
    internal); omit it for the daemon periodic sweep. Dry-run by default. Never
    raises.

    The CAS on ``updated_at`` catches a *writer* that already mutated the row.
    It does not catch an in-flight rematerialize that holds the worker flock
    and has not written yet (DDIA ch. 7 single-writer). This sweep therefore
    takes the same per-lane reaping claim as :func:`reap_blocked_lanes` before
    CAS, skips the row when acquisition fails, and releases only after the
    CAS transaction ends.
    """
    del now  # accepted for signature parity with reap_blocked_lanes
    try:
        batch = max(1, int(max_batch))
    except (TypeError, ValueError):
        batch = DEFAULT_BLOCKED_LANE_REAP_BATCH

    reported: list[dict[str, object]] = []
    closed: list[dict[str, object]] = []
    would_close: list[dict[str, object]] = []
    # Honest empties for this unconditional archival reaper (no liveness probe).
    ambiguous: list[dict[str, object]] = []
    alive: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    triage: list[str] = []
    pending_closes: list[tuple[Any, dict[str, object], str, Any]] = []

    placeholders = ", ".join("?" for _ in _ARCHIVED_ORPHAN_TERMINAL_STATUSES)
    scoped = _normalize_optional_text(task_ref)
    try:
        with _get_db_connection() as conn:
            params: list[object] = list(_ARCHIVED_ORPHAN_TERMINAL_STATUSES)
            scope_sql = ""
            if scoped is not None:
                scope_sql = "AND wl.task_ref = ? "
                params.append(scoped)
            params.append(batch)
            rows = conn.execute(
                f"""
                SELECT id, task_ref, lane_id, worktree_path, branch, status, created_at, updated_at
                FROM worktree_lanes wl
                WHERE COALESCE(wl.status, '') NOT IN ({placeholders})
                  AND wl.task_ref IN (SELECT task_ref FROM task_archives WHERE task_ref IS NOT NULL)
                  -- ``IS NOT NULL`` guard: a single NULL task_ref row in
                  -- handoff_state would make ``NOT IN`` evaluate to NULL for every
                  -- lane and silently reap nothing (a dark sweep). Filtering NULLs
                  -- keeps the live-task exclusion honest.
                  AND wl.task_ref NOT IN (SELECT task_ref FROM handoff_state WHERE task_ref IS NOT NULL)
                  {scope_sql}
                ORDER BY datetime(COALESCE(wl.updated_at, wl.created_at)) ASC, id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            selected = [dict(row) for row in rows]

        for row in selected:
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
            note = f"closed_stale by task-archived-orphan reaper: task {ref} archived (was {status})"
            if not apply:
                would_close.append({**entry, "note": note})
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
            would_close.append(close_entry)
            pending_closes.append((row, close_entry, note, claim_handle))

        # Write phase: claim stays held across the CAS transaction, then
        # released — mirror reap_blocked_lanes 1584-1589.
        if apply:
            for index, (row, close_entry, note, claim_handle) in enumerate(pending_closes):
                try:
                    row_status = str(row["status"] or "")
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
                        failed.append({**close_entry, "error": str(exc)})
                        continue
                    if ok:
                        closed.append({**close_entry, "status": _LANE_STATUS_CLOSED_STALE})
                    else:
                        triage.append(
                            f"archived-orphan lane {close_entry['lane_id']} "
                            f"task={close_entry['task_ref']}: CAS miss; re-probe next tick"
                        )
                finally:
                    if claim_handle is not None:
                        _release_lane_worker_lock(claim_handle)
                        pending_closes[index] = (row, close_entry, note, None)
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
        _release_pending_reaping_claims(pending_closes)
        triage.append(f"task-archived-orphan sweep failed: {exc}")
        return _json_response(
            {
                "ok": True,
                "applied": apply,
                "max_batch": batch,
                # Computed here — do not omit for a helper to invent False.
                "truncated": len(reported) >= batch,
                "error": str(exc),
                "reported": reported,
                "closed": closed,
                "would_close": would_close,
                "ambiguous": ambiguous,
                "alive": alive,
                "failed": failed,
                "triage": triage,
            }
        )

    return _json_response(
        {
            "ok": True,
            "applied": apply,
            "max_batch": batch,
            # PMH-F12: a full batch means the LIMIT was hit and more orphan lanes
            # may remain unreaped this sweep. Surface it so callers (daemon log,
            # task-finish receipt) can distinguish a truncated sweep from a clean
            # one instead of a silent partial reap that reads as complete.
            "truncated": len(reported) >= batch,
            "reported": reported,
            "closed": closed,
            "would_close": would_close,
            "ambiguous": ambiguous,
            "alive": alive,
            "failed": failed,
            "triage": triage,
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
            cur = {"path": line[len("worktree "):], "branch": None, "detached": False}
        elif line.startswith("branch "):
            ref = line[len("branch "):]
            if ref.startswith("refs/heads/"):
                ref = ref[len("refs/heads/"):]
            cur["branch"] = ref
        elif line == "detached":
            cur["detached"] = True
    if cur.get("path"):
        entries.append(cur)
    return entries


def _branch_merged_into_integration(
    root: Path, branch: str, integration_ref: str
) -> bool | None:
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
    rather than refusing it as foreign forever.
    """
    resolved = _resolved_path_text(path).rstrip("/")
    try:
        with _get_db_connection() as conn:
            rows = conn.execute(
                "SELECT task_ref, lane_id, worktree_path FROM worktree_lanes "
                "WHERE worktree_path IS NOT NULL ORDER BY id DESC",
            ).fetchall()
    except Exception:  # noqa: BLE001 — identity is best-effort
        return (None, None)
    for row in rows:
        candidate = str(row["worktree_path"] or "")
        if candidate.rstrip("/") == path.rstrip("/") or (
            _resolved_path_text(candidate).rstrip("/") == resolved
        ):
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
    """Newest lane row naming this branch (any status) → (task_ref, lane_id)."""
    try:
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT task_ref, lane_id FROM worktree_lanes "
                "WHERE branch = ? ORDER BY id DESC LIMIT 1",
                (branch,),
            ).fetchone()
    except Exception:  # noqa: BLE001 — identity is best-effort
        return (None, None)
    if row is None:
        return (None, None)
    return (row["task_ref"], row["lane_id"])


def _enqueue_merged_registry_reclaim_candidate(
    *,
    root: Path,
    branch: str,
    task_ref: str | None,
    lane_id: str | None,
    failed: list[dict[str, object]],
    worktree_path: object = None,
) -> None:
    """Enqueue one authorized tip for a qualifying merged branch.

    Queue writes must never fail the sweep: a raise is recorded on
    ``failed`` and the caller continues [Release It!].
    """
    record: dict[str, object] = {"worktree_path": worktree_path, "branch": branch}
    try:
        from workbay_orchestrator_mcp.orchestration.branch_reclaim_queue import (  # noqa: PLC0415
            enqueue_branch_reclaim_candidate,
        )

        sha = _observe_branch_tip(root, branch)
        if sha is None:
            failed.append(
                {**record, "stage": "branch_reclaim_enqueue", "detail": "tip_unresolvable"}
            )
            return
        queued_lane = (lane_id or "").strip() or f"registry-{branch}"
        queued_task = (task_ref or "").strip() or "merged-registry-reclaim"
        enqueue_branch_reclaim_candidate(
            task_ref=queued_task,
            lane_id=queued_lane,
            branch=branch,
            authorized_sha=sha,
        )
    except Exception as exc:  # noqa: BLE001 — never fail the reap
        failed.append(
            {
                **record,
                "stage": "branch_reclaim_enqueue",
                "detail": str(exc)[:_RECLAIM_DETAIL_CAP],
            }
        )


def _delete_merged_branch(root: Path, branch: str) -> tuple[bool, str]:
    """Safe delete only. ``-d`` refuses unmerged work as the final fail-safe."""
    proc = _run_reclaim_command(["git", "-C", str(root), "branch", "-d", branch])
    if proc is None:
        return (False, "branch_delete_unrunnable")
    if getattr(proc, "returncode", None) == 0:
        return (True, "")
    detail = (getattr(proc, "stderr", "") or getattr(proc, "stdout", "") or "").strip()
    return (False, detail[:_RECLAIM_DETAIL_CAP])


def reap_merged_registry_worktrees(
    *,
    apply: bool = False,
    repo_root: Path | str | None = None,
    integration_ref: str = _RECLAIM_INTEGRATION_REF,
    owner_probe: Callable[[object], tuple[str, str]] | None = None,
) -> dict:
    """Reclaim merged worktrees and branches found in git itself, not rows.

    Candidates come from ``git worktree list --porcelain`` and ``refs/heads``
    at ``repo_root``; a candidate qualifies only when its branch is an
    ancestor of ``integration_ref``. Worktree removal reuses the guarded
    :func:`_reclaim_lane_worktree` chain (clean, ignored, owner, heartbeat,
    shared-path, flock, removal-time re-probe). Branch deletion runs after —
    and only after — the worktree is gone, always from the integration root,
    always ``-d``. A branch named by any non-terminal lane row is refused
    outright. Dry-run by default; never raises.
    """
    reclaimed: list[dict[str, object]] = []
    would_reclaim: list[dict[str, object]] = []
    deleted_branches: list[dict[str, object]] = []
    would_delete_branch: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    try:
        if repo_root is None:
            repo_root = _workspace_root()
        root = Path(repo_root)
        listing = _run_reclaim_command(
            ["git", "-C", str(root), "worktree", "list", "--porcelain"]
        )
        if listing is None or getattr(listing, "returncode", None) != 0:
            return {"ok": False, "applied": apply, "error": "worktree_registry_unavailable"}
        registry = _parse_worktree_registry(_probe_command_stdout(listing))
        root_resolved = _resolved_path_text(root).rstrip("/")
        checked_out = {
            str(entry.get("branch"))
            for entry in registry
            if entry.get("branch")
        }
        root_branch: str | None = None
        for entry in registry:
            if _resolved_path_text(str(entry.get("path"))).rstrip("/") == root_resolved:
                root_branch = entry.get("branch")  # type: ignore[assignment]

        for entry in registry:
            path = str(entry.get("path") or "")
            branch = entry.get("branch")
            record: dict[str, object] = {"worktree_path": path, "branch": branch}
            try:
                if _resolved_path_text(path).rstrip("/") == root_resolved:
                    continue
                if entry.get("detached") or not branch:
                    skipped.append({**record, "reason": "detached_or_branchless"})
                    continue
                branch = str(branch)
                if branch == integration_ref or branch == root_branch:
                    skipped.append({**record, "reason": "integration_branch"})
                    continue
                merged = _branch_merged_into_integration(root, branch, integration_ref)
                if merged is not True:
                    skipped.append(
                        {**record, "reason": "unmerged" if merged is False else "merge_unknown"}
                    )
                    continue
                open_row, open_detail = _open_lane_row_names_branch(branch)
                if open_row is not False:
                    skipped.append(
                        {
                            **record,
                            "reason": "open_lane_row" if open_row else "open_row_probe_failed",
                            "detail": open_detail,
                        }
                    )
                    continue
                task_ref, lane_id = _row_identity_for_worktree_path(path)
                if lane_id is None:
                    lane_id = f"registry-{Path(path).name}"
                outcome, detail = _reclaim_lane_worktree(
                    worktree_path=path,
                    branch=branch,
                    repo_root=root,
                    apply=apply,
                    owner_probe=owner_probe,
                    integration_ref=integration_ref,
                    task_ref=task_ref,
                    lane_id=lane_id,
                )
                if outcome == "would_reclaim":
                    would_reclaim.append(record)
                    would_delete_branch.append(record)
                    _enqueue_merged_registry_reclaim_candidate(
                        root=root,
                        branch=branch,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        failed=failed,
                        worktree_path=path,
                    )
                elif outcome == "reclaimed":
                    reclaimed.append(record)
                    # Worktree is gone; drop the pre-remove snapshot so a
                    # failed delete can be retried by the branch-only arm
                    # in this same run instead of waiting for the next tick.
                    checked_out.discard(branch)
                    _enqueue_merged_registry_reclaim_candidate(
                        root=root,
                        branch=branch,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        failed=failed,
                        worktree_path=path,
                    )
                    deleted, delete_detail = _delete_merged_branch(root, branch)
                    if deleted:
                        deleted_branches.append(record)
                    else:
                        failed.append(
                            {**record, "stage": "branch_delete", "detail": delete_detail}
                        )
                else:
                    skipped.append({**record, "reason": outcome, "detail": detail})
            except Exception as exc:  # noqa: BLE001 — per-entry degrade
                failed.append({**record, "stage": "worktree", "detail": str(exc)})

        heads = _run_reclaim_command(
            ["git", "-C", str(root), "for-each-ref", "--format=%(refname:short)", "refs/heads"]
        )
        if heads is not None and getattr(heads, "returncode", None) == 0:
            for branch in _probe_command_stdout(heads).splitlines():
                branch = branch.strip()
                record = {"worktree_path": None, "branch": branch}
                try:
                    if not branch or branch in checked_out:
                        continue
                    if any(str(item.get("branch")) == branch for item in deleted_branches):
                        continue
                    if branch == integration_ref or branch == root_branch:
                        continue
                    merged = _branch_merged_into_integration(root, branch, integration_ref)
                    if merged is not True:
                        skipped.append(
                            {**record, "reason": "unmerged" if merged is False else "merge_unknown"}
                        )
                        continue
                    open_row, open_detail = _open_lane_row_names_branch(branch)
                    if open_row is not False:
                        skipped.append(
                            {
                                **record,
                                "reason": "open_lane_row" if open_row else "open_row_probe_failed",
                                "detail": open_detail,
                            }
                        )
                        continue
                    task_ref, lane_id = _row_identity_for_branch(branch)
                    _enqueue_merged_registry_reclaim_candidate(
                        root=root,
                        branch=branch,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        failed=failed,
                    )
                    if not apply:
                        would_delete_branch.append(record)
                        continue
                    deleted, delete_detail = _delete_merged_branch(root, branch)
                    if deleted:
                        deleted_branches.append(record)
                        failed[:] = [
                            item
                            for item in failed
                            if not (
                                item.get("branch") == branch
                                and item.get("stage") == "branch_delete"
                            )
                        ]
                    else:
                        failed.append(
                            {**record, "stage": "branch_delete", "detail": delete_detail}
                        )
                except Exception as exc:  # noqa: BLE001 — per-entry degrade
                    failed.append({**record, "stage": "branch", "detail": str(exc)})
        else:
            failed.append(
                {"worktree_path": None, "branch": None, "stage": "branch_listing", "detail": "refs_unavailable"}
            )
    except Exception as exc:  # noqa: BLE001 — never-raise sweep [RES-07]
        return {"ok": False, "applied": apply, "error": str(exc)}

    return {
        "ok": True,
        "applied": apply,
        "reclaimed": reclaimed,
        "would_reclaim": would_reclaim,
        "deleted_branches": deleted_branches,
        "would_delete_branch": would_delete_branch,
        "skipped": skipped,
        "failed": failed,
    }
