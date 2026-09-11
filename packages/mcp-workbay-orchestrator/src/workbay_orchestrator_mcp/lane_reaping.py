"""Blocked-lane aging and reaping operations."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
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
            configured = {line.strip() for line in _probe_command_stdout(remotes_list).splitlines() if line.strip()}
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
#: * ``.task-state/branch_naming_push_overrides.log`` and
#:   ``.task-state/lifecycle_guard_bypass.jsonl`` -- sole-copy operator audit
#:   trails of publish-time leniency and of lifecycle-guard bypasses.  Nothing
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
) -> subprocess.CompletedProcess[str] | None:
    """Run a probe/removal command. ``None`` means the command could not run.

    ``stdin_text`` exists for ``git hash-object --stdin``, which is the only way
    to hash bytes git would store that are not the bytes on disk -- a symlink,
    whose blob is its target path.
    """
    _assert_no_reaper_write_lock(reason="subprocess")
    try:
        return subprocess.run(  # noqa: S603 — fixed argv, no shell
            list(argv),
            input=stdin_text,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            timeout=timeout,
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


def _bootstrap_surface_root(entry: object) -> str | None:
    """Return a root only for a bootstrap-generated surface."""
    if not isinstance(entry, Mapping) or entry.get("source") not in _REGENERABLE_SURFACE_SOURCES:
        return None
    normalized = _normalized_reclaim_relpath(entry.get("path"))
    if normalized is None or "/" not in normalized:
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
    roots = _roots_from_ledger_entries(surfaces, _bootstrap_surface_root)
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
        eligible = leaf.endswith((".pyc", ".pyo"))
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
    """Whether two regular files hold the same bytes.  Any doubt answers False."""
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
    if rule is None:
        return None
    candidate = _contained_worktree_path(worktree_path, normalized)
    # A link is not the state: its target may be the only copy of something the
    # worktree does not own, so only a real regular file authorizes anything.
    if candidate is None or _regular_file_size(candidate) is None:
        return None
    return rule


def _ignored_path_regenerable_rule(
    path: str,
    *,
    managed_roots: Mapping[str, str],
    identity: _ContentIdentityContext | None = None,
    worktree_path: str | None = None,
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
    return (
        _root_rule(normalized, hardcoded_roots, prefix="hardcoded_root")
        or _root_rule(normalized, managed_roots, prefix="ledger_surface")
        # Consulted last: a ledger label that already authorizes the path needs
        # no filesystem comparison, and content identity only ever *adds* a
        # positive classification the labels alone could not reach.
        or _content_identity_rule(normalized, identity)
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
) -> _IgnoredContentVerdict:
    """Combine per-path authority rules into one auditable verdict."""
    classified = [
        (
            path,
            _ignored_path_regenerable_rule(
                path,
                managed_roots=managed_roots,
                identity=identity,
                worktree_path=worktree_path,
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
    verdict = nested_verdict or _classification_verdict(ignored, managed_roots, identity, worktree_path)
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
) -> tuple[bool | None, str]:
    """SHA merge verdict for worktree reclaim. Tree equality is diagnostic only.

    ``_probe_branch_merged_by_tree`` may still report ``merged_by_tree`` or
    ``merged_by_tree_untouched``, but that result does not authorize reclaim
    (AGT-10). A recorded attached branch that adds then deletes a product
    path can have an unchanged tree and nonempty
    ``git rev-list integration..lane``; that history stays retained.
    Detached HEAD, a different attached branch, and an unreadable HEAD all
    refuse. Identity-bound squash/rebase receipts are a separate contract.
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
    4b. every ignored entry enumerated by ``git ls-files`` is proven
        regenerable, with incomplete and nested-repository probes refused;
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
        return _dirty_refusal_outcome(root, path, clean_detail)

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
            return ("would_reclaim", f"{merged_detail};{ignored_detail}")

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
            return _dirty_refusal_outcome(root, path, clean_detail)

        ignored_ok, ignored_detail = _probe_worktree_ignored(path)
        if ignored_ok is None:
            if ignored_detail == "ignored_probe_timeout":
                return ("refused_ignored_timeout", ignored_detail)
            return ("refused_ignored_unknown", ignored_detail)
        if ignored_ok is False:
            return ("refused_ignored", ignored_detail)

        # Re-run SHA merge under the flock. A commit created after the
        # pre-lock sample is invisible to porcelain; the early True is not
        # permission to delete. Tree equality is diagnostic only.
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
            return ("reclaimed", f"{merged_detail};{ignored_detail}")
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
    if acked is not None:
        summary["acked"] = acked
    if bundle is not None:
        summary.update({key: bundle[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle})
    return summary


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
                       owner_agent, status, notes, created_at, updated_at
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
            row_progress_charged = False
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
            # Refresh reported list item with probe fields.
            reported[-1] = entry

            if verdict == "alive":
                alive.append(entry)
                continue

            claim_handle: Any | None = None
            claim_transferred = False
            try:
                will_reclaim = reclaim_worktrees and branch_dead is True and worktree_gone is False
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
                    elif reclaim_outcome != "would_reclaim":
                        triage.append(
                            f"blocked lane {entry['lane_id']} task={entry['task_ref']}: "
                            f"worktree reclaim {reclaim_outcome}: {reclaim_detail}"
                        )

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
    """Newest lane row naming this branch (any status) → (task_ref, lane_id)."""
    try:
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT task_ref, lane_id FROM worktree_lanes WHERE branch = ? ORDER BY id DESC LIMIT 1",
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
            failed.append({**record, "stage": "branch_reclaim_enqueue", "detail": "tip_unresolvable"})
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
        queued_lane = (lane_id or "").strip() or f"registry-{branch}"
        queued_task = (task_ref or "").strip() or "merged-registry-reclaim"
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


def _verified_bundle_contains(root: Path, path: Path, tip_sha: str, *, errors: list[str] | None = None) -> bool:
    listed = _run_reclaim_command(["git", "-C", str(root), "bundle", "list-heads", str(path)])
    verified = _run_reclaim_command(["git", "-C", str(root), "bundle", "verify", str(path)])
    listed_shas = {line.split(maxsplit=1)[0] for line in _probe_command_stdout(listed).splitlines() if line.strip()}
    if not (
        listed is not None
        and listed.returncode == 0
        and verified is not None
        and verified.returncode == 0
        and tip_sha in listed_shas
    ):
        return False
    # Source alternates would hide missing bundle objects. Copy only the closure
    # of declared prerequisites into isolated storage before unpacking [RLSE-08].
    try:
        # Keep prerequisite packs on the archive filesystem, whose capacity the
        # operator selected for rollback storage, and clean up on every exit.
        with tempfile.TemporaryDirectory(prefix="workbay-bundle-verify-", dir=path.resolve().parent) as scratch:
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
                return False
            prerequisites = _bundle_prerequisite_shas(path)
            if prerequisites:
                packed = subprocess.run(  # noqa: S603 — fixed argv, validated object ids
                    [
                        "git",
                        "-C",
                        str(root),
                        "pack-objects",
                        "--revs",
                        str(Path(scratch) / "objects" / "pack" / "pack"),
                    ],
                    input="\n".join(prerequisites) + "\n",
                    capture_output=True,
                    text=True,
                    timeout=_RECLAIM_PROBE_TIMEOUT_S,
                    check=False,
                )
                if packed.returncode != 0:
                    return False
            unpacked = _run_reclaim_command(["git", "-C", scratch, "bundle", "unbundle", str(path.resolve())])
            if unpacked is None or unpacked.returncode != 0:
                return False
            connected = _run_reclaim_command(
                ["git", "-C", scratch, "rev-list", "--objects", "--missing=error", f"{tip_sha}^{{commit}}"]
            )
            if connected is None or connected.returncode != 0:
                if errors is not None:
                    errors.append("bundle_tip_unreachable")
                return False
            return True
    except (OSError, subprocess.SubprocessError):
        return False


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
        return {"ok": False, "guard": "verified_bundle_required", "error": verification_errors[0]}
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


def _delete_merged_branch(
    root: Path,
    branch: str,
    *,
    bundle_record: dict[str, object] | None = None,
    retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
) -> tuple[bool, str]:
    """Bundle, then safe-delete. ``-d`` refuses unmerged work as the final fail-safe.

    Ordering is the contract: the verified rollback bundle lands *before* the ref
    disappears, and a bundle that cannot be written or verified refuses the
    delete outright [RLSE-08][CON-05].
    """
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
    current = _run_reclaim_command(["git", "-C", str(root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"])
    current_sha = _probe_command_stdout(current).strip() if current is not None else ""
    if bundled_sha and current_sha and current_sha != bundled_sha:
        if bundle_record is not None:
            bundle_record["reap_guard"] = "tip_moved_after_bundle"
            bundle_record["reap_refusal"] = "tip_moved_after_bundle"
            bundle_record["reap_detail"] = f"bundled {bundled_sha} but ref now {current_sha}"
        return (False, "tip_moved_after_bundle")
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
    min_age_hours: float = _REGISTRY_SWEEP_MIN_AGE_HOURS,
    bundle_retention_count: int = _DEFAULT_BUNDLE_RETENTION_COUNT,
) -> dict:
    """Reclaim merged worktrees and branches found in git itself, not rows.

    Candidates come from ``git worktree list --porcelain`` and ``refs/heads``
    at ``repo_root``; a worktree candidate qualifies only when its branch is an
    ancestor of ``integration_ref``, is not sitting *on* that ref (a
    never-started lane is trivially an ancestor of the ref it was cut from),
    and was registered at least ``min_age_hours`` ago. Worktree removal reuses the guarded
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

    def result(**extra: object) -> dict:
        return _with_bundle_telemetry(
            {
                "ok": True,
                "applied": apply,
                "reclaimed": reclaimed,
                "would_reclaim": would_reclaim,
                "deleted_branches": deleted_branches,
                "would_delete_branch": would_delete_branch,
                "skipped": skipped,
                "failed": failed,
                **extra,
            }
        )

    try:
        if repo_root is None:
            repo_root = _workspace_root()
        root = Path(repo_root)
        listing = _run_reclaim_command(["git", "-C", str(root), "worktree", "list", "--porcelain"])
        if listing is None or getattr(listing, "returncode", None) != 0:
            return result(ok=False, error="worktree_registry_unavailable")
        registry = _parse_worktree_registry(_probe_command_stdout(listing))
        root_resolved = _resolved_path_text(root).rstrip("/")
        checked_out = {str(entry.get("branch")) for entry in registry if entry.get("branch")}
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
                    skipped.append({**record, "reason": "unmerged" if merged is False else "merge_unknown"})
                    continue
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
                    attributed_record = {
                        **record,
                        "detail": detail,
                        "ignored_rule_histogram": _ignored_rule_histogram_from_detail(detail),
                    }
                    would_reclaim.append(attributed_record)
                    would_delete_branch.append(attributed_record)
                    _enqueue_merged_registry_reclaim_candidate(
                        root=root,
                        branch=branch,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        failed=failed,
                        worktree_path=path,
                    )
                elif outcome == "reclaimed":
                    reclaim_record: dict[str, object] = {
                        **record,
                        "detail": detail,
                        "ignored_rule_histogram": _ignored_rule_histogram_from_detail(detail),
                    }
                    reclaimed.append(reclaim_record)
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
                    bundle_record: dict[str, object] = {}
                    deleted, delete_detail = _delete_merged_branch(
                        root,
                        branch,
                        bundle_record=bundle_record,
                        retention_count=bundle_retention_count,
                    )
                    rollback = {key: bundle_record[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle_record}
                    if rollback:
                        # The reclaimed record is the operator's rollback handle;
                        # it must name the bundle, not merely report a deletion.
                        reclaim_record.update(rollback)
                    if deleted:
                        deleted_branches.append({**record, **rollback})
                    else:
                        failed.append({**record, **bundle_record, "stage": "branch_delete", "detail": delete_detail})
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
                    task_ref, lane_id = _row_identity_for_branch(branch)
                    if branch in checked_out:
                        skipped.append({**record, "reason": "skipped_checked_out"})
                        _enqueue_merged_registry_reclaim_skip(
                            root=root,
                            branch=branch,
                            task_ref=task_ref,
                            lane_id=lane_id,
                            failed=failed,
                            reason="skipped_checked_out",
                        )
                        continue
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
                    bundle_record = {}
                    deleted, delete_detail = _delete_merged_branch(
                        root,
                        branch,
                        bundle_record=bundle_record,
                        retention_count=bundle_retention_count,
                    )
                    rollback = {key: bundle_record[key] for key in _BUNDLE_RECORD_FIELDS if key in bundle_record}
                    if deleted:
                        deleted_branches.append({**record, **rollback})
                        failed[:] = [
                            item
                            for item in failed
                            if not (item.get("branch") == branch and item.get("stage") == "branch_delete")
                        ]
                    else:
                        failed.append({**record, **bundle_record, "stage": "branch_delete", "detail": delete_detail})
                except Exception as exc:  # noqa: BLE001 — per-entry degrade
                    failed.append({**record, "stage": "branch", "detail": str(exc)})
        else:
            failed.append(
                {"worktree_path": None, "branch": None, "stage": "branch_listing", "detail": "refs_unavailable"}
            )
    except Exception as exc:  # noqa: BLE001 — never-raise sweep [RES-07]
        return result(ok=False, error=str(exc))

    return result(skipped_counts=_registry_skip_counts(skipped))
