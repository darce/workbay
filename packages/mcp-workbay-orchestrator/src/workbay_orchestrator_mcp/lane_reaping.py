"""Blocked-lane aging and reaping operations."""

from __future__ import annotations

import json
import sqlite3
import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

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
        if lane_id is not None and r_lane is not None and r_lane != lane_id:
            continue
        if task_ref is not None and r_task is not None and r_task != task_ref:
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
                  AND (lane_id = ? OR lane_id IS NULL OR lane_id = '')
                ORDER BY
                  CASE WHEN lane_id = ? THEN 0 ELSE 1 END,
                  datetime(created_at) DESC,
                  id DESC
                LIMIT 1
                """,
                (task_ref, lane_id, lane_id),
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
    Unexpected git exit codes and exceptions degrade to None. Never raises.
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
            remotes_list = subprocess.run(
                ["git", "-C", cwd_str, "remote"],
                capture_output=True,
                text=True,
                timeout=_BRANCH_PROBE_TIMEOUT_S,
                check=False,
            )
            if remotes_list.returncode == 0:
                configured = {line.strip() for line in (remotes_list.stdout or "").splitlines() if line.strip()}
                if maybe_remote in configured:
                    ref = ref[slash + 1 :]
            else:
                # Unexpected remote listing failure: degrade rather than guess.
                return None

        def _merged_into_head(git_ref: str) -> bool | None:
            merged = subprocess.run(
                ["git", "-C", cwd_str, "merge-base", "--is-ancestor", git_ref, "HEAD"],
                capture_output=True,
                timeout=_BRANCH_PROBE_TIMEOUT_S,
                check=False,
            )
            if merged.returncode == 0:
                return True
            if merged.returncode == 1:
                return False
            return None

        # Local branch ref.
        show = subprocess.run(
            ["git", "-C", cwd_str, "show-ref", "--verify", "--quiet", f"refs/heads/{ref}"],
            capture_output=True,
            timeout=_BRANCH_PROBE_TIMEOUT_S,
            check=False,
        )
        if show.returncode == 0:
            return _merged_into_head(f"refs/heads/{ref}")
        if show.returncode != 1:
            # Unexpected (e.g. not a git repo, fatal error) → unknown.
            return None

        # Local missing: consult remote-tracking refs before concluding dead.
        remotes_list = subprocess.run(
            ["git", "-C", cwd_str, "remote"],
            capture_output=True,
            text=True,
            timeout=_BRANCH_PROBE_TIMEOUT_S,
            check=False,
        )
        if remotes_list.returncode != 0:
            return None
        remotes = [line.strip() for line in (remotes_list.stdout or "").splitlines() if line.strip()]
        saw_remote = False
        any_unmerged = False
        any_unknown = False
        for remote in remotes:
            remote_ref = f"refs/remotes/{remote}/{ref}"
            rshow = subprocess.run(
                ["git", "-C", cwd_str, "show-ref", "--verify", "--quiet", remote_ref],
                capture_output=True,
                timeout=_BRANCH_PROBE_TIMEOUT_S,
                check=False,
            )
            if rshow.returncode == 0:
                saw_remote = True
                verdict = _merged_into_head(remote_ref)
                if verdict is False:
                    any_unmerged = True
                elif verdict is None:
                    any_unknown = True
            elif rshow.returncode != 1:
                return None
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


def reap_blocked_lanes(
    *,
    apply: bool = False,
    max_batch: int = DEFAULT_BLOCKED_LANE_REAP_BATCH,
    worktree_probe: Callable[[object], bool | None] | None = None,
    branch_probe: Callable[[object], bool | None] | None = None,
    now: datetime | None = None,
    min_age_hours: float = 24.0,
    task_ref: str | None = None,
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

    Pass ``task_ref`` to scope candidates (and closes) to one task; omit for
    repo-wide sweep. Scoped filter applies to the SELECT that drives both
    reporting and closing — a scoped call cannot close foreign rows.
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

    path_probe = worktree_probe or _probe_worktree_gone
    br_probe = branch_probe or _probe_branch_dead

    try:
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
                return _json_response(
                    {
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
                        "dashboard_lines": [],
                    }
                )

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
                    task_ref = _normalize_optional_text(row["task_ref"]) or "?"
                    status = str(row["status"] or "")
                    age = format_lane_age_label(row["updated_at"], row["created_at"], now=clock)
                    blocker = _last_blocker_text_for_lane(
                        conn,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        notes=row["notes"],
                        reports=reports,
                    )
                    entry = {
                        "id": row["id"],
                        "task_ref": task_ref,
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
                if verdict != "dead":
                    ambiguous.append(entry)
                    triage.append(
                        f"blocked lane {entry['lane_id']} task={entry['task_ref']} age={entry['age']}: {reason}"
                    )
                    continue

                note = f"closed_stale by blocked-lane reaper: {reason}"
                close_entry = {**entry, "note": note}
                would_close.append(close_entry)
                if not apply:
                    continue
                row_status = str(row["status"] or _LANE_STATUS_BLOCKED)
                try:
                    ok = _close_blocked_lane_cas(
                        conn,
                        lane_pk=int(row["id"]),
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
                        f"blocked lane {entry['lane_id']} task={entry['task_ref']}: CAS miss; re-probe next tick"
                    )
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
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
                "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
            }
        )

    return _json_response(
        {
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
            "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
        }
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

            for row in rows:
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
                would_close.append({**entry, "note": note})
                if not apply:
                    continue
                try:
                    ok = _close_blocked_lane_cas(
                        conn,
                        lane_pk=int(cast(int, row["id"])),
                        probed_updated_at=row["updated_at"],
                        note=note,
                        expected_status=status,
                    )
                except sqlite3.Error as exc:
                    failed.append({**entry, "error": str(exc)})
                    continue
                if ok:
                    closed.append({**entry, "status": _LANE_STATUS_CLOSED_STALE})
                else:
                    triage.append(f"archived-orphan lane {lane_id} task={ref}: CAS miss; re-probe next tick")
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
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
