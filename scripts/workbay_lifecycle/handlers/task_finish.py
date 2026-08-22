"""Mutating ``task-finish`` subcommand.

Wraps the canonical end-of-task close sequence documented at
``packages/workbay-system/skills/branch-lifecycle/body.md`` step 9 in a
single Make-callable target so the order is mechanical and a stalled
operator never leaves an inconsistent dashboard / unarchived task row /
orphaned linked worktree behind.

The sequence:

1. ``mcp-workbay-handoff set --task-ref <ref> --status done --status-only``
   — flip the live row to ``done`` so the row is archive-eligible.
2. (Best-effort) auto-reap open worktree lanes for the *finishing*
   task only (T11). Closes via the orchestrator/handoff surface when
   available, else updates the local state DB; status is ``merged``
   when ``target_branch`` is merged into primary HEAD, else ``closed``.
   Each reaped lane is named in the receipt (``reaped_lanes``). Lanes
   that still cannot be closed surface ``lane_close_skipped``. Absent
   a state DB we proceed silently. Other tasks' lanes are never touched.
3. ``sync-task-plan-checklist --quiet`` — final full-plan sweep, VERIFY-ONLY
   (dry-run, no ``--apply``) while the active row's ``task_plan_path`` and
   decision/test evidence are still readable. task-finish runs POST-merge in
   a worktree it is about to delete, and plan docs reach the integration
   branch ONLY via the feature-branch merge — so writing ticks here would
   silently discard them. A non-zero dry-run ``ticked`` therefore means the
   merged plan is missing evidence-backed boxes (the operator skipped the
   pre-merge ``finalize-plan``); it is surfaced as a ``plan_checklist_drift``
   warning rather than written. The persisting sweep lives in the
   ``finalize-plan`` subcommand, run on the feature branch BEFORE merge.
   Failure-as-warning: a malformed plan never blocks the close sequence; the
   slim sync receipt is merged under the ``checklist_sync`` key.
4. ``mcp-workbay-handoff archive --operation archive --task-ref <ref> --apply``
   — move the row into ``task_archives`` and snapshot status.
5. ``mcp-workbay-handoff render-handoff --kind dashboard`` — regenerate
   ``DASHBOARD.txt`` from the updated state.
6. ``git worktree remove`` for the row's ``target_worktree_path`` when
   it points at a real linked worktree distinct from the primary
   repo — the linked worktree is no longer needed once the task is
   archived. The close sequence itself dirties the worktree (step 3's
   ``sync-task-plan-checklist --apply`` ticks a plan box — an uncommitted
   tracked edit) and ``make task-start`` provisions a ``.venv``, so the
   *safe* ``git worktree remove`` fails on essentially every finished
   task. ``--force`` is therefore applied automatically, but ONLY when
   ``target_branch`` is fully merged into the primary HEAD: then every
   committed change is already preserved on the integration branch and the
   only working-tree content discarded is the close sequence's own
   regenerable side-effects. An unmerged/unknown branch never auto-forces
   (its worktree may hold the only copy of unmerged work). Failure is
   reported but does not flip ``ok`` to false: the canonical state already
   reflects the close, and the operator can finish teardown manually.
7. ``git branch -d`` for the row's ``target_branch`` when it exists
   locally, is fully merged into the current HEAD, is not the branch
   currently checked out, and is not checked out in another linked
   worktree. ``-d`` is the safe variant — git itself refuses unmerged
   branches — so the worst case is a ``skipped_unmerged`` receipt
   field, never a destructive surprise.

Step ordering is load-bearing: archive MUST run before the worktree
remove so the write-side guard still sees a live worktree at the time
the archive write lands. The branch delete MUST run after the worktree
remove because git refuses to delete a branch that is checked out in a
worktree — including the linked worktree we just tore down. Inverting
the archive/worktree order is the failure mode the
``mcp-workbay-handoff`` write-side-guard scope captures (see
``docs/scopes/handoff-write-side-guard-archive-no-worktree-scope.md``).

Folding the branch delete into this target removes the previous
``manual git branch -d`` step from the branch-lifecycle skill and
eliminates the post-merge contingency where the row's ``target_branch``
no longer mapped to a live worktree at the time the close ran (the row
is set/archive-written *before* the branch is deleted here). It also
keeps the cleanup inside an authorized make target, so the auto-mode
classifier never sees a standalone ``git branch -d`` invocation.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import resolver
import session_heartbeat

# Re-export for sibling importers/tests that probe the task-finish surface.
worktree_has_live_session = session_heartbeat.worktree_has_live_session

import uv_provisioning

from . import _common


def _read_active_task_ref(repo: Path) -> str | None:
    """Derive the active task_ref via ``render-handoff --no-write``.

    internal: the on-disk ``CURRENT_TASK.json`` is no longer
    consulted; the singular ``task_ref`` is derived from MCP's live
    state on each call. ``workspace_ambiguous`` and ``none`` both yield
    ``None`` — task-finish must not pick a winner; the operator-supplied
    ``--task`` flag is the disambiguation surface.
    """
    view = _common.derive_workspace_summary_view(repo)
    if view.shape != "single":
        return None
    return view.task_ref if view.task_ref else None


def _read_handoff_identity(repo: Path, task_ref: str) -> dict[str, Any]:
    """Read the row's identity directly from the handoff DB by exact task_ref.

    Goes through the local sqlite store rather than ``mcp-workbay-handoff
    state`` so the lookup is fully bound by the requested ``task_ref``.
    The CLI's ``state`` subcommand falls back to cwd-active resolution
    when its positional argument is dropped or shadowed by argparse —
    that ambiguity has bitten the dogfood close where ``task-finish``
    runs from the primary worktree (whose cwd-active row is a different
    MAINT task) on behalf of the finishing task. A direct row read
    eliminates the fallback surface entirely and mirrors the pattern
    already used by ``_open_lanes_for_task`` below.

    Returns ``{}`` on any missing/stale state — callers collapse to
    ``skipped_unset`` for the optional teardown steps that need
    ``target_worktree_path`` / ``target_branch``.

    When no live row exists the lookup falls back to the
    ``task_archives`` snapshot (see ``_read_archived_identity``): a prior
    ``task-finish`` may have archived the row — clearing ``handoff_state``
    — but left the linked worktree behind because the branch was unmerged
    at the time. Recovering identity from the archive lets a re-run AFTER
    a manual merge still reap that orphan worktree.
    """
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return {"source": "absent"}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT target_branch, target_worktree_path, status "
                "FROM handoff_state WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()
            if row is not None:
                return {
                    "target_branch": row[0] or "",
                    "target_worktree_path": row[1] or "",
                    "status": row[2] or "",
                    "source": "live",
                }
            # Live row gone — recover identity from the archive snapshot so
            # an already-archived task can still have its orphan worktree
            # reaped on a re-run. Shares the open connection. A present
            # snapshot marks the task already-terminal (source='archived') so
            # the close sequence skips the redundant (and crash-prone) set-done
            # + archive writes; an empty result is a never-existed/stale ref.
            archived = _read_archived_identity(conn, task_ref)
            if archived:
                return {**archived, "source": "archived"}
            return {"source": "absent"}
    except sqlite3.Error:
        return {"source": "absent"}


def _read_archived_identity(conn: sqlite3.Connection, task_ref: str) -> dict[str, Any]:
    """Recover identity from the ``task_archives`` snapshot for ``task_ref``.

    ``archive_task_state`` snapshots the live row BEFORE clearing its
    worktree pointer, so ``snapshot_json["active"]`` retains the pre-clear
    ``target_branch`` / ``target_worktree_path``. Returns those, or ``{}``
    when no archive row exists or the ``task_archives`` table is absent
    (older DB) — the close sequence never raises.

    When an archive row exists but ``snapshot_json`` is unparseable, empty,
    or lacks an ``active`` object from which a worktree path can be read,
    the result is marked ``snapshot_corrupt=True`` so the receipt can emit
    a durable warning instead of looking like a clean ``skipped_unset``
    teardown of a sticky orphan. A well-formed ``active`` object with an
    intentionally empty worktree path is NOT corrupt (worktree already
    reaped on a prior run).

    The snapshot's ``active.target_branch`` is preferred over the
    ``archived_branch`` column: the latter records the archive write
    actor's branch, which is frequently ``main`` resolved from the primary
    worktree rather than the task's real feature branch.
    """
    try:
        arow = conn.execute(
            "SELECT archived_branch, snapshot_json "
            "FROM task_archives WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    except sqlite3.Error:
        return {}
    if arow is None:
        return {}
    archived_branch, snapshot_json = arow[0], arow[1]
    active: dict[str, Any] = {}
    snapshot_corrupt = False
    if not snapshot_json:
        # Row present but snapshot body missing — cannot recover worktree.
        snapshot_corrupt = True
    else:
        try:
            snapshot = json.loads(snapshot_json)
        except (ValueError, TypeError):
            snapshot = None
            snapshot_corrupt = True
        if isinstance(snapshot, dict) and isinstance(snapshot.get("active"), dict):
            active = snapshot["active"]
            # active present but no worktree path field at all → cannot
            # distinguish "never had a worktree" from "path lost in a
            # partial snapshot"; only flag when the key is absent AND
            # target_branch is also absent from active (pure junk active).
            if (
                "target_worktree_path" not in active
                and "target_branch" not in active
            ):
                snapshot_corrupt = True
        elif not snapshot_corrupt:
            # Parseable JSON but wrong shape (no active object) — lacks
            # the worktree path the archive is supposed to carry.
            snapshot_corrupt = True
    target_branch = active.get("target_branch") or archived_branch or ""
    target_worktree_path = active.get("target_worktree_path") or ""
    result: dict[str, Any] = {
        "target_branch": str(target_branch),
        "target_worktree_path": str(target_worktree_path),
    }
    if snapshot_corrupt:
        result["snapshot_corrupt"] = True
    return result


#: Terminal lane statuses that no longer need task-finish teardown (T11).
_TERMINAL_LANE_STATUSES = frozenset(
    {"closed", "archived", "merged", "closed_stale"}
)


def _open_lanes_for_task(repo: Path, task_ref: str) -> list[str]:
    """Return open lane_ids for ``task_ref``, empty when state DB absent.

    Reads ``worktree_lanes`` directly from the local handoff DB rather
    than shelling out to the orchestrator — there is no
    ``mcp-workbay-orchestrator`` CLI surface for ``manage_worktree_lane``
    so this is the only way to detect lanes from a Make target. Any
    error path returns empty so the close sequence is not blocked by
    transient detection failures.
    """
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.execute(
                "SELECT lane_id FROM worktree_lanes "
                "WHERE task_ref = ? AND COALESCE(status, '') NOT IN "
                "('closed', 'archived', 'merged', 'closed_stale')",
                (task_ref,),
            )
            return [str(row[0]) for row in cursor.fetchall() if row[0]]
    except sqlite3.Error:
        return []


def _orchestrator_lane_close_available() -> bool:
    """True when the orchestrator package can close lanes in-process."""
    try:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: F401, PLC0415

        return True
    except Exception:
        try:
            from workbay_handoff_mcp.lanes_api import close_lane  # noqa: F401, PLC0415

            return True
        except Exception:
            return False


def _close_lane_via_surface(
    *,
    lane_id: str,
    task_ref: str,
    status: str,
    notes: str,
) -> bool:
    """Close one lane via orchestrator or handoff surface. Never raises."""
    try:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        result = manage_worktree_lane(
            operation="close",
            lane_id=lane_id,
            task_ref=task_ref,
            status=status,
            notes=notes,
        )
        if isinstance(result, dict) and result.get("ok") is True:
            return True
        # Some adapters wrap the envelope under data/ok differently.
        if isinstance(result, dict) and (result.get("data") or {}).get("lane"):
            return True
    except Exception:
        pass
    try:
        from workbay_handoff_mcp.lanes_api import close_lane  # noqa: PLC0415

        result = close_lane(
            lane_id=lane_id,
            task_ref=task_ref,
            status=status,
            notes=notes,
        )
        if isinstance(result, dict) and result.get("ok") is True:
            return True
    except Exception:
        pass
    return False


def _close_lane_via_sql(
    repo: Path,
    *,
    lane_id: str,
    task_ref: str,
    status: str,
    notes: str,
) -> bool:
    """Fallback: mark the finishing task's lane terminal in the local state DB."""
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return False
    try:
        with sqlite3.connect(str(db_path)) as conn:
            # Scope by task_ref so we never force-close another task's lanes (T11).
            cursor = conn.execute(
                "UPDATE worktree_lanes "
                "SET status = ?, "
                "    notes = COALESCE(?, notes), "
                "    updated_at = datetime('now') "
                "WHERE task_ref = ? AND lane_id = ? "
                "  AND COALESCE(status, '') NOT IN "
                "('closed', 'archived', 'merged', 'closed_stale')",
                (status, notes, task_ref, lane_id),
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error:
        # Older schemas may lack notes/updated_at — try a minimal update.
        try:
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.execute(
                    "UPDATE worktree_lanes SET status = ? "
                    "WHERE task_ref = ? AND lane_id = ?",
                    (status, task_ref, lane_id),
                )
                conn.commit()
                return cursor.rowcount > 0
        except sqlite3.Error:
            return False


def _reap_open_lanes_for_task(
    repo: Path,
    task_ref: str,
    *,
    target_branch: str = "",
    primary: Path | None = None,
) -> tuple[list[dict[str, str]], list[str]]:
    """Auto-reap the finishing task's open lanes (T11 safety net).

    When the state DB (or orchestrator/handoff package) is present, each
    open lane for *this* ``task_ref`` is closed: ``merged`` when
    ``target_branch`` is fully merged into primary HEAD (ancestor OR
    patch-equivalent content — squash/rebase merges of single commits
    classify as merged; a multi-commit squash may still fall through to
    ``closed``, see ``_branch_is_merged``), else ``closed`` (schema has no
    ``abandoned`` terminal; ``closed`` is the unmerged terminal, NOT proof
    the branch was abandoned). Absent state DB → empty result (silent
    skip). Failed closes remain listed as open so the caller can warn.

    Returns ``(reaped, still_open)`` where each reaped entry is
    ``{"lane_id": ..., "status": ...}``.
    """
    open_lanes = _open_lanes_for_task(repo, task_ref)
    if not open_lanes:
        return [], []

    root = primary or (resolver.canonical_workspace_root(repo) or repo)
    close_status = (
        "merged" if _branch_is_merged(root, target_branch) else "closed"
    )
    notes = f"task-finish auto-reap for {task_ref}"
    surface_ok = _orchestrator_lane_close_available()
    reaped: list[dict[str, str]] = []
    still_open: list[str] = []

    for lane_id in open_lanes:
        closed = False
        if surface_ok:
            closed = _close_lane_via_surface(
                lane_id=lane_id,
                task_ref=task_ref,
                status=close_status,
                notes=notes,
            )
        if not closed:
            # State-DB fallback when package surface is missing or rejected
            # the write; still scoped to this task_ref only.
            closed = _close_lane_via_sql(
                repo,
                lane_id=lane_id,
                task_ref=task_ref,
                status=close_status,
                notes=notes,
            )
        if closed:
            reaped.append({"lane_id": lane_id, "status": close_status})
        else:
            still_open.append(lane_id)

    return reaped, still_open


def _set_status_done(repo: Path, task_ref: str) -> tuple[bool, str | None]:
    argv = _common.handoff_command_argv(
        repo, "set",
        "--task-ref", task_ref,
        "--status", "done",
        "--status-only",
        *_common.worktree_write_context_argv(repo, task_ref=task_ref),
    )
    proc = _common.run_handoff_subprocess(repo, argv)
    if proc.returncode == 0:
        return True, None
    return False, (proc.stderr or proc.stdout or "").strip()[:300]


def _archive(repo: Path, task_ref: str) -> tuple[bool, str | None]:
    # `archive` is a move+snapshot, not a row-attribution write: it has no
    # provenance surface (the `archive` subcommand declares no --branch /
    # --commit-sha — passing them exits 2 "unrecognized arguments"). Attribution
    # is carried by the preceding `_set_status_done` write, which the snapshot
    # captures. So — unlike set/record — do NOT thread worktree_write_context_argv
    # here (matches `_auto_reap_stale_rows`, which already archives bare).
    argv = _common.handoff_command_argv(
        repo, "archive",
        "--operation", "archive",
        "--task-ref", task_ref,
        "--apply",
    )
    proc = _common.run_handoff_subprocess(repo, argv)
    if proc.returncode == 0:
        return True, None
    return False, (proc.stderr or proc.stdout or "").strip()[:300]


def _render_dashboard(repo: Path) -> tuple[bool, str | None]:
    argv = _common.handoff_command_argv(
        repo, "render-handoff", "--kind", "dashboard",
    )
    proc = _common.run_handoff_subprocess(repo, argv)
    if proc.returncode == 0:
        return True, None
    return False, (proc.stderr or proc.stdout or "").strip()[:300]


def _task_finish_auto_reap_enabled() -> bool:
    flag = os.environ.get("WORKBAY_HANDOFF_TASK_FINISH_AUTO_REAP")
    if flag is None:
        return True
    return flag.strip().lower() not in {"0", "false", "off", "no"}


def _auto_reap_stale_rows(
    repo: Path, task_ref: str
) -> tuple[list[str], str | None]:
    """Bounded post-finish sweep: reap closeable live rows and done non-scratch rows.

    Scoped to ``task_ref`` so finishing one task cannot archive sibling live rows.
    """
    if not _task_finish_auto_reap_enabled():
        return [], None
    reaped: list[str] = []
    for operation in ("reap", "reap_done"):
        argv = _common.handoff_command_argv(
            repo,
            "archive",
            "--operation",
            operation,
            "--apply",
            "--task-ref",
            task_ref,
        )
        proc = _common.run_handoff_subprocess(repo, argv)
        if proc.returncode != 0:
            return reaped, (proc.stderr or proc.stdout or "").strip()[:300]
        raw = (proc.stdout or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return reaped, f"auto_reap_{operation}_malformed_json"
        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        if isinstance(data, dict):
            for ref in data.get("reaped") or []:
                ref_str = str(ref)
                if ref_str not in reaped:
                    reaped.append(ref_str)
    for ref in reaped:
        sys.stderr.write(f"task-finish: auto-reap archived {ref}\n")
    return reaped, None


#: Verified macOS cwd-under-path probe (implementation note spike). ``+D <worktree>`` appended.
_LSOF_CWD_PROBE = ("lsof", "-a", "-d", "cwd", "+D")

#: Porcelain paths matching these prefixes are close-sequence regenerable artifacts.
_REGENERABLE_PATH_PREFIXES = (
    ".venv/",
    "__pycache__/",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".task-state/checklist_sync.json",
)


def _path_is_regenerable(rel_path: str) -> bool:
    normalized = rel_path.replace("\\", "/")
    if any(
        normalized == prefix.rstrip("/")
        or normalized.startswith(prefix)
        or f"/{prefix}" in f"/{normalized}/"
        for prefix in _REGENERABLE_PATH_PREFIXES
    ):
        return True
    if normalized.endswith("-task-plan.md"):
        return True
    if normalized.startswith("docs/plans/") and normalized.endswith(".md"):
        return True
    return False


def _parse_lsof_pids(stdout: str) -> list[int]:
    pids: list[int] = []
    for line in stdout.splitlines():
        if not line or line.startswith("COMMAND"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            pids.append(int(parts[1]))
    return pids


def _run_lsof_cwd_probe(worktree: Path) -> subprocess.CompletedProcess[str]:
    try:
        probe_root = str(worktree.resolve())
    except OSError:
        probe_root = str(worktree)
    return _common.run_subprocess([*_LSOF_CWD_PROBE, probe_root])


def _has_live_process_in(
    worktree: Path,
    *,
    exclude_pid: int,
    lsof_runner: Callable[[Path], subprocess.CompletedProcess[str]] | None = None,
) -> tuple[bool, str | None]:
    """True when a process other than ``exclude_pid`` has cwd under ``worktree``.

    ``lsof`` missing or erroring fails closed (active) — never raises.
    """
    runner = lsof_runner or _run_lsof_cwd_probe
    proc = runner(worktree)
    if proc.returncode == 127:
        return True, "lsof_missing"
    if proc.returncode > 1:
        return True, "lsof_error"
    for pid in _parse_lsof_pids(proc.stdout or ""):
        if pid != exclude_pid:
            return True, f"live_process:{pid}"
    if proc.returncode == 1:
        stderr = (proc.stderr or "").strip().lower()
        # lsof on Linux hosts running containers emits benign mount-table
        # warnings for filesystems unrelated to the probed worktree —
        # "WARNING: can't stat() overlay file system /var/lib/docker/..."
        # (note the EMPTY parens) plus "Output information may be
        # incomplete." — on every invocation. Those must not fail the probe
        # closed or no worktree is ever removable on such hosts. A genuine
        # probe failure names the path inside the parens: "can't stat(<path>)".
        suspicious = [
            line
            for line in stderr.splitlines()
            if ("can't stat" in line or "permission denied" in line)
            and "can't stat() " not in line
        ]
        if suspicious:
            return True, "lsof_probe_failed"
    return False, None


def _worktree_dirty_nonregenerable(worktree: Path) -> tuple[bool, str | None]:
    """True when ``git status --porcelain`` shows non-regenerable dirty paths."""
    proc = _common.run_subprocess(
        ["git", "-C", str(worktree), "status", "--porcelain", "--untracked-files=all"]
    )
    if proc.returncode != 0:
        return True, "git_status_failed"
    for line in (proc.stdout or "").splitlines():
        if len(line) < 4:
            continue
        path_part = line[3:].strip()
        if " -> " in path_part:
            path_part = path_part.split(" -> ", 1)[1].strip()
        if not _path_is_regenerable(path_part):
            return True, f"dirty:{path_part}"
    return False, None


def _heartbeat_live_in_worktree(
    primary: Path,
    worktree: Path,
    exclude_session_id: str,
) -> tuple[bool, str | None]:
    """PRIMARY liveness signal: durable session heartbeat under ``worktree``."""
    if session_heartbeat.worktree_has_live_session(
        primary,
        str(worktree),
        exclude_session_id=exclude_session_id,
    ):
        return True, "live_session_heartbeat"
    return False, None


def _worktree_is_active(
    worktree: Path | str,
    *,
    primary: Path | None = None,
    self_pid: int | None = None,
    exclude_session_id: str = "",
    live_process_probe: Callable[[Path], tuple[bool, str | None]] | None = None,
    dirty_probe: Callable[[Path], tuple[bool, str | None]] | None = None,
    heartbeat_probe: Callable[[Path, Path, str], tuple[bool, str | None]] | None = None,
) -> tuple[bool, str | None]:
    """Return ``(active, reason)`` from OS/filesystem signals only."""
    target = Path(worktree)
    pid = self_pid if self_pid is not None else os.getpid()
    if primary is not None:
        hb = heartbeat_probe or _heartbeat_live_in_worktree
        hb_active, hb_reason = hb(primary, target, exclude_session_id)
        if hb_active:
            return True, hb_reason
    live = live_process_probe or (lambda wt: _has_live_process_in(wt, exclude_pid=pid))
    dirty = dirty_probe or _worktree_dirty_nonregenerable
    live_active, live_reason = live(target)
    if live_active:
        return True, live_reason
    dirty_active, dirty_reason = dirty(target)
    if dirty_active:
        return True, dirty_reason
    return False, None


def _branch_is_merged(primary: Path, branch: str) -> bool:
    """True when ``branch``'s work is preserved on the primary worktree HEAD.

    Two checks, cheapest first (S6-A-01):

    1. ``git merge-base --is-ancestor <branch> HEAD`` — exits 0 iff every
       commit on ``branch`` is reachable from HEAD (regular/ff merges).
    2. Content equivalence via ``git cherry HEAD <branch>`` — a squash or
       rebase merge rewrites SHAs, so the ancestor check is false even
       though the work landed; ``git cherry`` marks each branch commit
       ``-`` when a patch-equivalent change already exists on HEAD. All
       ``-`` (or no commits beyond HEAD) → merged.

    Known limitation: a squash merge that collapses SEVERAL commits into
    one is not patch-equivalent per-commit, so it can still classify as
    NOT merged. That stays on the safe side (never force-discard, lanes
    close as ``closed``); callers should not treat ``closed`` as proof the
    branch was abandoned. An empty branch name or any git error is treated
    as NOT merged (the safe default: do not force).
    """
    if not branch:
        return False
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "merge-base", "--is-ancestor", branch, "HEAD"]
    )
    if proc.returncode == 0:
        return True
    cherry = _common.run_subprocess(
        ["git", "-C", str(primary), "cherry", "HEAD", branch]
    )
    if cherry.returncode != 0:
        return False
    lines = [ln for ln in (cherry.stdout or "").splitlines() if ln.strip()]
    if not lines:
        # No commits beyond HEAD — content already present.
        return True
    return all(ln.startswith("-") for ln in lines)


#: Bound the best-effort prune so a slow/contended cache volume cannot hang
#: task-finish after the worktree is already removed and the task archived.
#: A timeout surfaces as rc 124 via ``_common.run_subprocess`` → skip-warning.
_CACHE_PRUNE_TIMEOUT_S = 120.0


def _maybe_prune_uv_cache(events: list[str], warnings: list[str]) -> None:
    """Best-effort ``uv cache prune`` after a worktree (and its venv) is removed."""
    proc = _common.run_subprocess(
        [uv_provisioning.uv_bin(), "cache", "prune"],
        timeout=_CACHE_PRUNE_TIMEOUT_S,
    )
    if proc.returncode == 0:
        events.append("cache_pruned")
        return
    err = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()[:300]
    warnings.append(f"cache_prune_skipped: {err}")


# Return codes from ``_ensure_handoff_runtime`` [F1 / G4 / G6].
_HANDOFF_RUNTIME_CONFIGURED = "configured"
_HANDOFF_RUNTIME_ABSENT = "absent"  # package missing → hermetic degrade
_HANDOFF_RUNTIME_NO_DB = "no_db"  # no handoff.db → intentional degrade
_HANDOFF_RUNTIME_FAILED = "failed"  # package present, configure failed → refuse


def _ensure_handoff_runtime(workspace: Path) -> str:
    """Configure the handoff runtime for in-process lane-row reads [F1].

    Fresh lifecycle CLI processes never inherit an MCP-server
    ``configure_runtime`` call. Without this, shared-path readers raise
    ``RuntimeNotConfiguredError`` and the guard degrades to no-block — a
    production no-op. Mirrors ``slice_commit``'s pattern
    (``RuntimeConfig.for_workspace`` / ``for_repo`` + ``configure_runtime``).

    Returns one of:
      * ``configured`` — runtime ready for lane-row reads against THIS
        workspace's handoff.db (bound db_path matches expected) [H1 / SECD-03]
      * ``absent`` — handoff package not importable (hermetic degrade)
      * ``no_db`` — ``handoff.db`` missing at the path-derived location;
        intentional degrade (no local lanes to protect). Never *creates*
        handoff.db when absent from this teardown path [G6]. An already-
        present 0-byte/stale file is a real DB path: first reader open may
        migrate it (creation-from-absent is what we refuse, not migrate of
        an existing file).
      * ``failed`` — package present but configuration failed; callers MUST
        refuse teardown as ``cross_task_path_lookup_failed`` [G4 / SECD-05]
    """
    try:
        from workbay_handoff_mcp import (  # noqa: PLC0415
            RuntimeConfig,
            configure_runtime,
            get_runtime_config,
        )
    except ImportError:
        return _HANDOFF_RUNTIME_ABSENT
    not_configured_cls: type[BaseException] | None
    try:
        from workbay_handoff_mcp.runtime import (  # noqa: PLC0415
            RuntimeNotConfiguredError as _NotConfigured,
        )

        not_configured_cls = _NotConfigured
    except ImportError:
        not_configured_cls = None

    # Resolve expected DB path without opening (opening of an *absent* path
    # would mkdir+create; see no_db arm) [G6].
    try:
        runtime = RuntimeConfig.for_repo(workspace)
    except Exception:  # noqa: BLE001 — package present; config construction failed
        return _HANDOFF_RUNTIME_FAILED
    try:
        db_path = Path(runtime.db_path)
    except Exception:  # noqa: BLE001
        return _HANDOFF_RUNTIME_FAILED

    def _db_paths_match(bound_raw: object, expected: Path) -> bool:
        """True when a bound runtime's db_path is the same file as expected."""
        try:
            bound = Path(str(bound_raw))
        except (TypeError, ValueError):
            return False
        try:
            return bound.resolve() == expected.resolve()
        except (OSError, RuntimeError):
            # Resolve failed (missing parents, permissions): compare expanded
            # string form so a present-file path still matches when resolve
            # cannot complete on one side only.
            return str(bound.expanduser()) == str(expected.expanduser())

    # Probe an already-bound runtime BEFORE path-derived no_db [H1]:
    # a wrong-repo bind must reconfigure (or fail), not short-circuit as
    # "configured" while readers still hit the foreign DB. Path-derived
    # no_db still wins when THIS workspace has no handoff.db file — there
    # are then no local lane rows to protect, and we must not rebind a
    # foreign runtime just to scan another repo's owners against this path.
    need_configure = True
    try:
        bound = get_runtime_config()
        if _db_paths_match(getattr(bound, "db_path", None), db_path):
            # Already bound to THIS workspace's expected DB.
            need_configure = False
        else:
            # Wrong-DB bind: re-configure below if local DB exists.
            # configure_runtime only swaps a ContextVar — rebinding is safe
            # for this in-process CLI call (no shared global pool to tear
            # down). Refuse-as-failed would be the alternative if rebinding
            # ever becomes unsafe; today rebind is preferred so teardown
            # still mediates THIS workspace's foreign owners [H1 / SECD-03].
            need_configure = True
    except Exception as exc:  # noqa: BLE001 — probe only
        if not_configured_cls is not None and isinstance(exc, not_configured_cls):
            need_configure = True  # typed unconfigured: fall through
        elif isinstance(exc, RuntimeError) and not_configured_cls is None:
            # Older handoff builds raise plain RuntimeError for unconfigured.
            # Gate on typed sentinel ABSENT: on current builds a real
            # RuntimeError must map to failed, not masquerade as unconfigured
            # [REVA3-r0810-runtimeerror-arm-ungated / SECD-05].
            need_configure = True
        else:
            return _HANDOFF_RUNTIME_FAILED

    if not need_configure:
        return _HANDOFF_RUNTIME_CONFIGURED

    if not db_path.is_file():
        # Intentional: no handoff.db at the path-derived location means no
        # local lane rows to protect. Do not configure_runtime / open that
        # would *create* the file from absence [G6]. Decided AFTER the
        # bound-runtime probe so a matching bind is not overridden, while a
        # wrong-repo bind with no local DB still degrades (no local owners).
        return _HANDOFF_RUNTIME_NO_DB

    try:
        configure_runtime(runtime)
    except Exception:  # noqa: BLE001 — package present; configure failed [G4]
        return _HANDOFF_RUNTIME_FAILED
    return _HANDOFF_RUNTIME_CONFIGURED


def _list_nonterminal_lanes_with_worktree_path() -> dict[str, Any]:
    """All non-terminal path-owner rows for shared-path identity [G1 / SECD-03].

    Full scan + both-sides normalize in the guard; exact-SQL path spelling
    cannot recover a stored symlink alias from a differently-resolved query.
    """
    from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
        list_nonterminal_lanes_with_worktree_path,
    )

    return list_nonterminal_lanes_with_worktree_path()


def _normalize_shared_path(raw: str) -> str:
    """Normalize a worktree path for shared-path identity comparison.

    Applies strip, ``expanduser``, ``resolve``, and trailing-slash collapse so
    alternate spellings of the same directory compare equal (relative vs
    absolute, symlink aliases such as ``/tmp`` vs ``/private/tmp``, trailing
    slash). Falls back to expanduser + trailing-slash strip when resolve fails.

    Limitation [F7]: does not casefold. On case-insensitive volumes (APFS
    default, Windows) two spellings that differ only by letter case may be
    the same directory on disk but compare unequal here. Callers that store
    mixed-case spellings of one path can still miss identity; a future
    casefold step would need an explicit volume-policy decision.
    """
    text = (raw or "").strip()
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve())
    except (OSError, RuntimeError, ValueError):
        collapsed = text.rstrip("/\\")
        try:
            return str(Path(collapsed).expanduser()) if collapsed else text
        except (OSError, RuntimeError, ValueError):
            return collapsed or text


# Terminal statuses for the shared-path predicate (predicate a / lane brief).
# Includes closed_stale (reaper / orphan-backstop terminal) so a dead foreign
# row cannot permanently wedge teardown of a shared path.
# Includes ``archived`` for forward-safety / alignment with same-file
# ``_TERMINAL_LANE_STATUSES`` even though the current worktree_lanes CHECK
# constraint does not list it — if schema later allows it, a dead archived
# row must not permanently wedge shared-path teardown [F6 / SECD-05].
_SHARED_PATH_TERMINAL_STATUSES = frozenset(
    {"closed", "merged", "closed_stale", "archived"}
)


def _shared_path_active_block_reason(
    target_worktree_path: str,
    target_branch: str,
    *,
    exclude_lane_id: str = "",
    handoff_runtime_status: str = _HANDOFF_RUNTIME_CONFIGURED,
) -> str | None:
    """Return a ``skipped_active`` reason when another non-terminal lane holds the path.

    Distinct error dispositions (must not be conflated):

    * ``handoff_runtime_status == failed`` (package present, configure failed)
      → refuse as ``cross_task_path_lookup_failed`` [G4 / SECD-05].
    * ``handoff_runtime_status`` in ``{absent, no_db}`` → degrade (no block):
      package missing is hermetic; missing handoff.db means no lanes to
      protect and must not *create* the DB from absence on teardown [G6].
    * Reader returns a failed or malformed envelope → refuse removal (unsafe).
      Only ``ok is True`` proceeds; missing/falsy ``ok`` refuses [G7].
    * Reader raises ``AttributeError`` or a non-hermetic ``RuntimeError``
      (skewed / broken reader surface) → refuse as
      ``cross_task_path_lookup_failed`` [SECD-05].
    * Reader is not importable (``ImportError``) **or** raises the typed
      ``RuntimeNotConfiguredError`` hermetic sentinel → degrade scope and
      continue (not evidence of a sibling). After F1 production always
      configures the runtime before this guard runs when a DB exists; the
      unconfigured arm is retained as a hermetic safety net [F4].

    Predicate (a): refuse when any lane at the path has non-terminal status
    (not in ``closed``/``merged``/``closed_stale``/``archived``), is not the
    caller's own row (excluded by ``exclude_lane_id`` lane identity), **and**
    its ``branch`` differs from ``target_branch``. Path identity uses a
    full non-terminal path-owner scan and both-sides ``_normalize_shared_path``
    comparison so stored symlink aliases still mediate against resolved
    queries [G1 / SECD-03].
    """
    if handoff_runtime_status == _HANDOFF_RUNTIME_FAILED:
        return "cross_task_path_lookup_failed"
    if handoff_runtime_status in (
        _HANDOFF_RUNTIME_ABSENT,
        _HANDOFF_RUNTIME_NO_DB,
    ):
        return None
    raw = (target_worktree_path or "").strip()
    if not raw:
        # Empty / whitespace-only path: no shared-path evidence to consult.
        # Documented disposition is no-block (caller may still skip_unset /
        # skip_missing). Do not invoke the reader with an empty path.
        return None
    path = _normalize_shared_path(raw) or raw
    try:
        # G1: reader-side identity — full non-terminal path-owner scan.
        # Exact-SQL spelling unions cannot recover a differently-stored
        # symlink alias from a resolved query (live probe: stored alias +
        # resolved query → miss → teardown would proceed).
        cross_env = _list_nonterminal_lanes_with_worktree_path()
    except ImportError:
        # Missing module/symbol: cannot prove a global empty universe.
        return None
    except AttributeError:
        # Skewed reader surface (symbol shape broken): refuse [SECD-05].
        return "cross_task_path_lookup_failed"
    except Exception as exc:
        # Typed unconfigured sentinel (subclass of RuntimeError): hermetic
        # safety net only — production configures runtime in run() [F1/F4].
        # Do NOT substring-match RuntimeError prose (false degrade on
        # "connection pool not configured", silent flip on reword) [F4].
        not_configured_cls: type[BaseException] | None
        try:
            from workbay_handoff_mcp.runtime import (  # noqa: PLC0415
                RuntimeNotConfiguredError as _NotConfigured,
            )

            not_configured_cls = _NotConfigured
        except ImportError:
            not_configured_cls = None
        if not_configured_cls is not None and isinstance(exc, not_configured_cls):
            return None
        # Real RuntimeError / any other unexpected exception: refuse [SECD-05].
        return "cross_task_path_lookup_failed"
    # G7: only ok is True proceeds; missing/falsy ok → refuse (fail-closed).
    if not isinstance(cross_env, dict) or cross_env.get("ok") is not True:
        return "cross_task_path_lookup_failed"
    cross_data = cross_env.get("data") if isinstance(cross_env, dict) else None
    cross_lanes = (
        cross_data.get("lanes") if isinstance(cross_data, dict) else None
    )
    if not isinstance(cross_lanes, list):
        return "cross_task_path_lookup_malformed"
    blockers: list[str] = []
    for owner in cross_lanes:
        if not isinstance(owner, dict):
            return "cross_task_path_lookup_malformed"
        # Both-sides path identity: spelling variants of the same directory
        # (trailing slash, symlink alias, relative vs absolute) still match.
        owner_raw = owner.get("worktree_path")
        if not isinstance(owner_raw, str) or not owner_raw.strip():
            continue
        if _normalize_shared_path(owner_raw) != path:
            continue
        status = owner.get("status")
        if status in _SHARED_PATH_TERMINAL_STATUSES:
            continue
        owner_id = owner.get("lane_id")
        # Caller's own row: exclude by lane identity so empty / mismatched
        # target_branch cannot self-wedge teardown [SECD-03].
        if (
            exclude_lane_id
            and owner_id is not None
            and str(owner_id) == exclude_lane_id
        ):
            continue
        owner_branch = owner.get("branch") or ""
        # Same-branch exclusion: finishing task's own lanes (and any peer on
        # the same branch) must not block when target_branch is known.
        if owner_branch == target_branch:
            continue
        owner_task = owner.get("task_ref")
        if owner_task is not None and owner_task != "":
            blockers.append(f"{owner_id}@{owner_task}")
        else:
            blockers.append(f"{owner_id}")
    if not blockers:
        return None
    return "shared_path_active_lanes:" + ",".join(blockers)


def _resolve_finishing_lane_id(
    *,
    task_ref: str,
    target_worktree_path: str,
    open_lanes: list[str],
    reaped_lanes: list[dict[str, Any]],
) -> str:
    """Derive the finishing process's own ``lane_id`` for self-exclusion [F3].

    Authoritative sources already in ``run()`` scope (no new env vars):

    1. Unambiguous remaining open lane for this task (still live after reap).
    2. Path-owner rows whose ``task_ref`` matches the finishing task — same
       identity key task_finish already uses (worktree_path + task_ref) via
       handoff identity resolution.
    3. A single reaped lane_id from this finish (defensive; usually terminal).

    Empty string when no authoritative identity can be established, **or when
    any candidate set is ambiguous** (multiple open / path-owner / reaped
    ids). Ambiguity must not pick ``matches[0]`` / ``cleaned_open[0]`` — that
    is arbitrary and can exclude the wrong lane. Returning "" excludes
    nothing (safe self-wedge direction) [G5 / SECD-05]. Branch-equality
    exclusion still covers the common same-branch case.
    """
    cleaned_open = [str(x) for x in open_lanes if x]
    if len(cleaned_open) == 1:
        return cleaned_open[0]
    # Multiple still-open lanes: ambiguous — do not pick an arbitrary id [G5].
    if len(cleaned_open) > 1:
        return ""

    raw = (target_worktree_path or "").strip()
    if raw and task_ref:
        try:
            env = _list_nonterminal_lanes_with_worktree_path()
        except Exception:  # noqa: BLE001 — exclusion is best-effort
            env = None
        if isinstance(env, dict) and env.get("ok") is True:
            data = env.get("data") if isinstance(env.get("data"), dict) else None
            lanes = data.get("lanes") if isinstance(data, dict) else None
            if isinstance(lanes, list):
                path = _normalize_shared_path(raw) or raw
                matches: list[str] = []
                for row in lanes:
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("task_ref") or "") != task_ref:
                        continue
                    owner_raw = row.get("worktree_path")
                    if isinstance(owner_raw, str) and owner_raw.strip():
                        if _normalize_shared_path(owner_raw) != path:
                            continue
                    lid = row.get("lane_id")
                    if lid is None or lid == "":
                        continue
                    matches.append(str(lid))
                if len(matches) == 1:
                    return matches[0]
                # 0 or >1 matches: unambiguous empty, or ambiguous → "" [G5].
                return ""

    reaped_ids = [
        str(entry.get("lane_id"))
        for entry in reaped_lanes
        if isinstance(entry, dict) and entry.get("lane_id")
    ]
    if len(reaped_ids) == 1:
        return reaped_ids[0]
    return ""


def _remove_worktree(
    primary: Path,
    target_worktree_path: str,
    target_branch: str = "",
    *,
    operator_force: bool = False,
    self_pid: int | None = None,
    exclude_session_id: str = "",
    exclude_lane_id: str = "",
    handoff_runtime_status: str = _HANDOFF_RUNTIME_CONFIGURED,
) -> tuple[str, str | None]:
    """Remove the linked worktree at ``target_worktree_path``.

    Returns a status string + optional warning. Status values:

    * ``removed`` — the safe ``git worktree remove`` exited 0 (clean worktree).
    * ``removed_force`` — the safe remove refused because the worktree was
      dirty, but ``target_branch`` is fully merged into the primary HEAD, so
      every committed change is already preserved on the integration branch;
      the remove was retried with ``--force``. The only working-tree content
      discarded is the close sequence's own regenerable side-effects — the
      step-3 ``sync-task-plan-checklist`` tick (an uncommitted edit task-finish
      itself just made), the ``make task-start`` ``.venv``, and caches — which
      otherwise make the safe remove fail on essentially every finished task.
    * ``skipped_active`` — the worktree is active (live session heartbeat, live
      process, non-regenerable dirty work, or a non-terminal sibling lane on a
      different branch sharing this path) and ``operator_force`` was not set;
      teardown is refused.
    * ``skipped_primary`` — the path resolves to the primary worktree;
      removing it would teardown the operator's main checkout.
    * ``skipped_missing`` — the path does not exist on disk.
    * ``skipped_unset`` — the row had no ``target_worktree_path``.
    * ``failed`` — the safe remove exited non-zero and the worktree could not
      be safely force-removed: ``target_branch`` is unmerged or unknown (its
      worktree may hold the only copy of unmerged commits / genuine
      uncommitted work), or ``--force`` itself errored (e.g. a locked worktree).
    """
    if not target_worktree_path:
        return "skipped_unset", None
    target = Path(target_worktree_path)
    try:
        if target.resolve() == primary.resolve():
            return "skipped_primary", None
    except OSError:
        return "skipped_missing", None
    if not target.exists():
        return "skipped_missing", None
    # A CLEAN worktree's safe ``git worktree remove`` below exits 0, so the
    # active guard further down — only reached when the safe remove REFUSES a
    # dirty tree — never runs for a clean worktree. That bypasses the durable
    # session heartbeat (the PRIMARY liveness signal) in exactly the case it
    # exists for: a live peer session whose work is all committed (clean tree).
    # Consult the cheap, external-binary-free heartbeat up front so a live
    # session blocks teardown regardless of dirty state. The lsof/dirty signals
    # stay on the post-refuse path below: keeping them there preserves the
    # ``unmerged + dirty -> failed`` contract and avoids forcing an lsof walk
    # (which fails closed when lsof is absent) on every clean finish.
    hb_active, hb_reason = _heartbeat_live_in_worktree(
        primary,
        target,
        exclude_session_id,
    )
    if hb_active and not operator_force:
        return "skipped_active", hb_reason
    # Cross-task shared-path mediation [SECD-03]: worktree_path is not unique
    # (schema UNIQUE is only task_ref+lane_id). Full non-terminal path-owner
    # scan + both-sides normalize closes identity (G1); operator_force retains
    # the escape hatch (same as heartbeat).
    if not operator_force:
        shared_reason = _shared_path_active_block_reason(
            target_worktree_path,
            target_branch,
            exclude_lane_id=exclude_lane_id,
            handoff_runtime_status=handoff_runtime_status,
        )
        if shared_reason is not None:
            return "skipped_active", shared_reason
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "worktree", "remove", str(target)]
    )
    if proc.returncode == 0:
        return "removed", None
    # The safe remove refuses a dirty worktree. ``--force`` is applied ONLY
    # when ``target_branch`` is fully merged into the primary HEAD: then all
    # committed work is already on the integration branch and the discard is
    # limited to the close sequence's own regenerable artifacts (see the
    # ``removed_force`` doc above). An unmerged/unknown branch never auto-forces
    # — silently discarding the only copy of unmerged work would be the
    # destructive surprise this guard exists to prevent, so the operator keeps
    # the manual ``--force`` escape hatch there.
    if not _branch_is_merged(primary, target_branch):
        return "failed", (proc.stderr or proc.stdout or "").strip()[:300]
    active, active_reason = _worktree_is_active(
        target,
        primary=primary,
        self_pid=self_pid if self_pid is not None else os.getpid(),
        exclude_session_id=exclude_session_id,
    )
    if active and not operator_force:
        return "skipped_active", active_reason
    forced = _common.run_subprocess(
        ["git", "-C", str(primary), "worktree", "remove", "--force", str(target)]
    )
    if forced.returncode == 0:
        return "removed_force", None
    return "failed", (forced.stderr or forced.stdout or "").strip()[:300]


def _current_branch(primary: Path) -> str:
    """Return the branch HEAD points at in the primary worktree, or empty."""
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "symbolic-ref", "--short", "-q", "HEAD"]
    )
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def _branch_checked_out_in_other_worktree(primary: Path, branch: str) -> bool:
    """True when ``branch`` is checked out by any worktree other than primary.

    Uses ``git worktree list --porcelain`` so the result is robust to
    detached HEADs and arbitrary worktree paths.
    """
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "worktree", "list", "--porcelain"]
    )
    if proc.returncode != 0:
        return False
    primary_resolved = ""
    try:
        primary_resolved = str(primary.resolve())
    except OSError:
        primary_resolved = str(primary)
    current_path = ""
    for line in (proc.stdout or "").splitlines():
        if line.startswith("worktree "):
            current_path = line[len("worktree "):].strip()
        elif line.startswith("branch "):
            ref = line[len("branch "):].strip()
            short = ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else ref
            if short != branch:
                continue
            try:
                resolved = str(Path(current_path).resolve())
            except OSError:
                resolved = current_path
            if resolved != primary_resolved:
                return True
    return False


def _linked_worktree_path_for_branch(primary: Path, branch: str) -> Path | None:
    """Return the linked worktree path where ``branch`` is checked out, if any."""
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "worktree", "list", "--porcelain"]
    )
    if proc.returncode != 0:
        return None
    current_path = ""
    for line in (proc.stdout or "").splitlines():
        if line.startswith("worktree "):
            current_path = line[len("worktree "):].strip()
        elif line.startswith("branch "):
            ref = line[len("branch "):].strip()
            short = ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else ref
            if short == branch and current_path:
                return Path(current_path)
    return None


def _delete_merged_branch(
    primary: Path,
    target_branch: str,
    *,
    operator_force: bool = False,
    self_pid: int | None = None,
    exclude_session_id: str = "",
) -> tuple[str, str | None]:
    """Run ``git branch -d`` for ``target_branch`` if safe.

    Returns a status string + optional warning. Status values:

    * ``deleted`` — ``git branch -d`` exited 0.
    * ``skipped_active`` — the branch's linked worktree is active and
      ``operator_force`` was not set; branch delete is refused.
    * ``skipped_unset`` — the row had no ``target_branch``.
    * ``skipped_missing`` — the branch does not exist locally.
    * ``skipped_primary`` — the branch is the primary worktree's HEAD;
      deleting it is never the right call from this target.
    * ``skipped_checked_out`` — the branch is checked out in another
      linked worktree (git would refuse anyway).
    * ``skipped_unmerged`` — ``git branch -d`` refused: the branch has
      commits not reachable from HEAD.
    * ``failed`` — non-zero exit for any other reason.
    """
    if not target_branch:
        return "skipped_unset", None
    exists = _common.run_subprocess(
        ["git", "-C", str(primary), "show-ref", "--verify", "--quiet",
         f"refs/heads/{target_branch}"]
    )
    if exists.returncode != 0:
        return "skipped_missing", None
    if target_branch == _current_branch(primary):
        return "skipped_primary", None
    wt_path = _linked_worktree_path_for_branch(primary, target_branch)
    if wt_path is not None:
        active, active_reason = _worktree_is_active(
            wt_path,
            primary=primary,
            self_pid=self_pid if self_pid is not None else os.getpid(),
            exclude_session_id=exclude_session_id,
        )
        if active and not operator_force:
            return "skipped_active", active_reason
    if _branch_checked_out_in_other_worktree(primary, target_branch):
        return "skipped_checked_out", None
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "branch", "-d", target_branch]
    )
    if proc.returncode == 0:
        return "deleted", None
    stderr = (proc.stderr or proc.stdout or "").strip()
    # ``git branch -d`` prints "not fully merged" for the unmerged case.
    if "not fully merged" in stderr.lower():
        return "skipped_unmerged", None
    return "failed", stderr[:300]


# Bound .task-state/codemap-reindex.log growth ([RES-07] / RB-05).
CODEMAP_REINDEX_LOG_MAX_BYTES = 256 * 1024


def _rotate_codemap_reindex_log(log_path: Path) -> None:
    """Rotate oversized log to ``.log.1`` (single generation) before append."""
    try:
        if not log_path.is_file():
            return
        if log_path.stat().st_size <= CODEMAP_REINDEX_LOG_MAX_BYTES:
            return
        rotated = log_path.with_name(log_path.name + ".1")
        if rotated.is_file():
            rotated.unlink()
        log_path.replace(rotated)
    except OSError:
        # Rotation is best-effort; spawn path still opens the log.
        pass


def _codemap_runner_inflight(state_dir: Path) -> bool:
    """True when a codemap-reindex process holds the spawn-debounce flock (RB-07).

    The CLI runner holds LOCK_EX on ``codemap-reindex.spawn.lock`` for its full
    lifetime. task-finish probes with LOCK_NB; if the lock is busy, skip Popen
    (request alone is enough while the live runner drains). Probe releases
    immediately so the child can re-acquire.
    """
    import fcntl  # noqa: PLC0415 — stdlib; works on macOS and Linux

    gate_path = state_dir / "codemap-reindex.spawn.lock"
    try:
        with open(gate_path, "a+", encoding="utf-8") as gate_fd:
            try:
                fcntl.flock(gate_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            # We got the lock: no live runner. Release before return so the
            # child we are about to spawn can hold it for the full reindex.
            fcntl.flock(gate_fd.fileno(), fcntl.LOCK_UN)
            return False
    except OSError:
        return False


def _request_codemap_reindex(repo: Path, sha: str) -> dict[str, Any] | None:
    """Request a singleton codemap reindex and spawn one detached runner.

    implementation note S2: task-finish is request-only for coordination — it must
    never block on or fail because of reindexing ([RES-02]). Lazy-imports
    the lease + runner surfaces the same way the optional orchestrator
    imports above do; any failure degrades to a note dict.

    Liveness reporting ([AGT-10] / RB-03): spawn_failed and import/request
    failures return ``ok=False`` so nested consumers cannot treat "queued
    but never scheduled" as success. ``receipt["ok"]`` still stays True
    (non-blocking finish), but warnings are first-class via
    :func:`_note_codemap_reindex_on_receipt`.
    """
    if not sha:
        # No finish sha to reindex against (both primary and repo HEAD were
        # unresolvable). This is an intentional skip, not a failure and not a
        # never-ran hook, so attach an ok=True note rather than staying wholly
        # silent. ok=True keeps it off receipt["warnings"] (see
        # _codemap_warning_token / _CODEMAP_WARN_NOTE_PREFIXES).
        return {
            "ok": True,
            "spawned": False,
            "note": "codemap_skip_no_finish_sha",
        }
    try:
        from workbay_handoff_mcp.codemap_lease import (  # noqa: PLC0415
            read_requested_shas,
            request_reindex,
            resolve_repo_instance_id,
        )
    except Exception as exc:  # noqa: BLE001 — optional surface
        return {"ok": False, "note": f"codemap_import_failed: {type(exc).__name__}: {exc}"}

    try:
        workspace = resolver.canonical_workspace_root(repo) or repo
        state_dir = workspace / ".task-state"
        db_path = state_dir / "handoff.db"
        if not db_path.is_file():
            return {"ok": False, "note": "handoff_db_missing"}

        repo_instance_id = resolve_repo_instance_id(db_path, repo_path=workspace)

        # request_reindex returns None by contract (append-only queue write).
        # Populate ``pending`` from the real queue so the note is honest.
        request_reindex(db_path, repo_instance_id=repo_instance_id, sha=sha)
        pending = read_requested_shas(
            db_path, repo_instance_id=repo_instance_id
        )

        # RB-07: skip Popen when a codemap-reindex process already holds the
        # spawn gate (live runner). Request alone is enough for that holder
        # to drain the queue; avoids N-1 short-lived held children.
        if _codemap_runner_inflight(state_dir):
            return {
                "ok": True,
                "pending": pending,
                "spawned": False,
                "note": "spawn_skipped_inflight",
                "repo_instance_id": repo_instance_id,
                "sha": sha,
            }

        # Child stdout/stderr go to .task-state/codemap-reindex.log so typed
        # runner statuses (cli_missing/timeout/failed/fenced/pending_remaining)
        # are not lost. Log is rotated when oversized ([RES-07]).
        # Prefer __file__ when still on disk (happy path). When task-finish
        # runs from a linked worktree and has already removed that tree,
        # Path(__file__) points into the deleted worktree — fall back to the
        # primary workspace copy so the detached runner argv remains valid.
        lifecycle_pkg = Path(__file__).resolve().parent.parent
        if not lifecycle_pkg.is_dir():
            candidate = workspace / "scripts" / "workbay_lifecycle"
            if candidate.is_dir():
                lifecycle_pkg = candidate
        log_path = state_dir / "codemap-reindex.log"
        _rotate_codemap_reindex_log(log_path)
        try:
            with open(log_path, "a", encoding="utf-8") as log_f:
                subprocess.Popen(
                    [
                        sys.executable,
                        str(lifecycle_pkg),
                        "codemap-reindex",
                        "--repo-instance-id",
                        repo_instance_id,
                        "--db-path",
                        str(db_path),
                        "--repo-path",
                        str(workspace),
                    ],
                    cwd=str(workspace),
                    stdin=subprocess.DEVNULL,
                    stdout=log_f,
                    stderr=log_f,
                    start_new_session=True,
                )
            spawned = True
        except OSError as spawn_exc:
            # Queue is filled but no runner will start — this is NOT success
            # (RB-03). task-finish still does not fail ([RES-02]); the note
            # is ok=False and surfaces on warnings[].
            return {
                "ok": False,
                "pending": pending,
                "spawned": False,
                "note": f"spawn_failed: {type(spawn_exc).__name__}: {spawn_exc}",
            }
        return {
            "ok": True,
            "pending": pending,
            "spawned": spawned,
            "repo_instance_id": repo_instance_id,
            "sha": sha,
        }
    except Exception as exc:  # noqa: BLE001 — never fail task-finish
        return {
            "ok": False,
            "note": f"codemap_reindex_request_failed: {type(exc).__name__}: {exc}",
        }


# Notes from ``_request_codemap_reindex`` that are real failures and must
# surface on warnings[] (RB-03 / [AGT-10]). Prefix match: notes carry a
# ``: {ExcType}: {msg}`` suffix (e.g. ``spawn_failed: OSError: ...``).
#
# Allowlist (not denylist): expected-absence notes on the optional path —
# ``handoff_db_missing`` (fresh install, no handoff.db) and
# ``codemap_import_failed`` (optional surface import) — stay off warnings[].
# Unrecognised notes default to *no* warning so a future optional-path note
# cannot silently train operators to ignore this channel; the full note still
# lives on ``receipt["codemap_reindex"]`` for scanners. ``spawn_skipped_inflight``
# is ok=True and never reaches this path.
_CODEMAP_WARN_NOTE_PREFIXES: tuple[str, ...] = (
    "spawn_failed",
    "codemap_reindex_request_failed",
)


def _codemap_warning_token(codemap_note: dict[str, Any]) -> str | None:
    """Build a warnings[] entry when the codemap note is a real failure.

    Only known failure-note prefixes warn (see ``_CODEMAP_WARN_NOTE_PREFIXES``).
    Expected absences (``handoff_db_missing``, ``codemap_import_failed``) and
    intentional skips (``spawn_skipped_inflight``, which is ok=True) do not.
    Unrecognised ok=False notes default to no warning — receipt still carries
    ``codemap_reindex``; ``warnings[]`` stays reserved for confirmed failures.
    """
    if codemap_note.get("ok") is not False:
        return None
    note = str(codemap_note.get("note") or "")
    if not note.startswith(_CODEMAP_WARN_NOTE_PREFIXES):
        return None
    return f"codemap_reindex_failed: {note}"


def _archived_orphan_reap_degradation_warnings(orphan_reap: object) -> list[str]:
    """Surface truncated / error / failed / triage signals from a reaper receipt.

    The archived-orphan backstop already returns these fields; the task-finish
    consumer must not drop them (OBS-08). CAS misses land in ``triage`` (not
    ``failed``), so a non-empty triage list must also surface — otherwise a
    zero-close, zero-fail, triage-only receipt looks byte-identical to a clean
    sweep. Clean receipts yield ``[]`` so the happy path stays byte-stable.
    Never raises (RES-07). ``failed`` / ``triage`` counts use list length only
    so a stray string value cannot emit a misleading character count.
    """
    if not isinstance(orphan_reap, dict):
        return []
    out: list[str] = []
    try:
        if orphan_reap.get("truncated"):
            out.append("archived_orphan_reap_degraded: truncated")
        err = orphan_reap.get("error")
        if err:
            out.append(f"archived_orphan_reap_degraded: error={err}")
        failed = orphan_reap.get("failed") or []
        if isinstance(failed, list) and failed:
            out.append(f"archived_orphan_reap_degraded: failed_rows={len(failed)}")
        # Hostile shapes (absent / None / non-list) degrade to no triage
        # warning — never raise [RES-07].
        triage = orphan_reap.get("triage")
        if isinstance(triage, list) and triage:
            out.append(f"archived_orphan_reap_degraded: triage_rows={len(triage)}")
    except Exception:  # noqa: BLE001 — RES-07 never-raise on the backstop path
        return out
    return out


def _note_codemap_reindex_on_receipt(
    receipt: dict[str, Any],
    repo: Path,
    sha: str,
) -> None:
    """Attach codemap reindex note to a task-finish receipt. Never raises.

    Absolute barrier: even if ``_request_codemap_reindex`` itself raises
    (tests inject this), ``receipt["ok"]`` is left untouched ([RES-02]).

    RB-03 / [AGT-10]: when the nested note is not a clean spawn success,
    append a first-class ``warnings[]`` entry so the human stderr one-liner
    (warnings count) and agent receipt scanners both notice.
    """
    try:
        codemap_note = _request_codemap_reindex(repo, sha)
        if codemap_note is not None:
            receipt["codemap_reindex"] = codemap_note
            warn = _codemap_warning_token(codemap_note)
            if warn is not None:
                warnings = receipt.setdefault("warnings", [])
                if isinstance(warnings, list):
                    warnings.append(warn)
    except Exception as exc:  # noqa: BLE001
        note = f"codemap_reindex_failed: {type(exc).__name__}: {exc}"
        receipt["codemap_reindex"] = {
            "ok": False,
            "note": note,
        }
        warnings = receipt.setdefault("warnings", [])
        if isinstance(warnings, list):
            warnings.append(note)


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle task-finish", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument(
        "--force",
        dest="operator_force",
        action="store_true",
        default=False,
        help="Override active-worktree guard and proceed with teardown",
    )
    args = parser.parse_args(argv)

    repo = _common.repo_root()
    if repo is None:
        _common.emit(
            {
                "ok": False,
                "command": "task-finish",
                "task_ref": None,
                "events": [],
                "error": "not_in_git_repo",
            }
        )
        return 2

    task_ref = (args.task or "").strip().upper() or _read_active_task_ref(repo)
    if not task_ref:
        _common.emit(
            {
                "ok": False,
                "command": "task-finish",
                "task_ref": None,
                "events": [],
                "error": "task_ref_required",
            }
        )
        return 2

    preflight_err = _common.handoff_cli_required_preflight_error(repo)
    if preflight_err is not None:
        sys.stderr.write(f"task-finish: {preflight_err}\n")
        _common.emit(
            {
                "ok": False,
                "command": "task-finish",
                "task_ref": task_ref,
                "events": [],
                "error": "handoff_cli_unavailable",
                "message": preflight_err,
            }
        )
        return 127

    # internal+3: best-effort dual-spool reclaimer after preflight.
    # Never raise / never fail task-finish ([REF-20]/[AGT-10]).
    # warnings sink carries the degrade cause into the receipt [OBS-08][AGT-10].
    warnings: list[str] = []
    # Re-exec heal degrades (give-up / -S / sentinel) into the same sink so a
    # receipt consumer need not read stderr [OBS-08, AGT-10].
    _common.maybe_drain_lifecycle_reexec_degrades(repo, warnings=warnings)
    agent_errors_replay = _common.maybe_replay_agent_error_spool(
        repo, warnings=warnings
    )
    terminal_guard_replay = _common.maybe_replay_terminal_guard_spool(
        repo, warnings=warnings
    )
    # internal: belt-and-suspenders stale /tmp/workbay-* reclaimer.
    stale_dev_temp_reap = _common.maybe_reap_stale_dev_temp(repo, warnings=warnings)
    events_seed: list[str] = []
    if agent_errors_replay is not None:
        events_seed.append("agent_errors_replayed")
    if terminal_guard_replay is not None:
        events_seed.append("terminal_guard_replayed")
    if stale_dev_temp_reap is not None and (stale_dev_temp_reap.get("removed") or []):
        events_seed.append("stale_dev_temp_reaped")

    primary = resolver.canonical_workspace_root(repo) or repo
    identity = _read_handoff_identity(repo, task_ref)
    target_worktree_path = ""
    raw_target = identity.get("target_worktree_path")
    if isinstance(raw_target, str):
        target_worktree_path = raw_target
    target_branch = ""
    raw_branch = identity.get("target_branch")
    if isinstance(raw_branch, str):
        target_branch = raw_branch.strip()

    events: list[str] = list(events_seed)

    # Archive row with unparseable / shapeless snapshot_json: still treat as
    # terminal (no set-done / re-archive), but never emit a clean receipt —
    # a sticky orphan with a corrupt snapshot must surface as a durable warning.
    if identity.get("source") == "archived" and identity.get("snapshot_corrupt"):
        warnings.append(
            f"corrupt_archive_snapshot: task_ref={task_ref} "
            "snapshot_json unparseable or missing worktree identity"
        )

    # An already-terminal task (no live handoff row, but an archive snapshot
    # exists) needs no set-done / archive writes — those are no-ops that would
    # otherwise crash with WriteActorAttributionError when the CLI subprocess
    # lacks agent-attribution env. Recover identity from the snapshot (above)
    # and go straight to teardown so the orphan worktree + merged branch are
    # still reaped. Gate on archive-present, NOT live-absent: never-existed and
    # live-but-active rows must keep the normal set-done path.
    already_terminal = identity.get("source") == "archived"
    live_done = (
        identity.get("source") == "live" and identity.get("status") == "done"
    )

    if already_terminal:
        events.append("skipped_terminal")
    elif live_done:
        events.append("skipped_live_done")
    else:
        status_ok, status_err = _set_status_done(repo, task_ref)
        if not status_ok:
            _common.emit(
                {
                    "ok": False,
                    "command": "task-finish",
                    "task_ref": task_ref,
                    "events": events,
                    "error": "set_status_done_failed",
                    "stderr_summary": status_err,
                }
            )
            return 2
        events.append("status_done_set")

    # T11: auto-reap this task's open lanes before archive. Names each
    # reaped lane in the receipt; does not touch other tasks' lanes.
    # Absent state DB → silent skip (empty open list). Degrade path
    # keeps the historical lane_close_skipped warning for leftovers.
    reaped_lanes, open_lanes = _reap_open_lanes_for_task(
        repo,
        task_ref,
        target_branch=target_branch,
        primary=primary,
    )
    if reaped_lanes:
        events.append("lanes_reaped")
        for entry in reaped_lanes:
            sys.stderr.write(
                f"task-finish: reaped lane {entry['lane_id']} "
                f"(status={entry['status']})\n"
            )
    if open_lanes:
        warnings.append(
            "lane_close_skipped: open lanes detected "
            f"({', '.join(sorted(open_lanes))}); close via MCP "
            "manage_worktree_lane(operation='close') before rerunning"
        )

    # Final full-plan checklist sweep, VERIFY-ONLY (apply=False). task-finish
    # runs POST-merge in the linked worktree it is about to delete, and plan
    # docs reach the integration branch ONLY via the feature-branch merge — so
    # a sweep that *wrote* ticks here would silently discard them (the bug this
    # replaces). Instead we dry-run: a non-zero ``ticked`` means the merged
    # plan is missing boxes whose evidence is recorded, i.e. the operator
    # skipped the pre-merge ``make finalize-plan``. Surface that as drift rather
    # than losing it. The lookup still runs BEFORE archive (the row's
    # ``task_plan_path`` is unreadable once archived). Failure stays a warning.
    checklist_sync = _common.run_checklist_sync(repo, task_ref, apply=False)
    if checklist_sync.get("ok"):
        would_tick = checklist_sync.get("ticked", 0)
        if would_tick:
            warnings.append(
                f"plan_checklist_drift: {would_tick} task-plan box(es) are "
                "unticked but their evidence is recorded; these ticks were NOT "
                "persisted (post-merge task-finish cannot write them to the "
                "integration branch). Run `make finalize-plan TASK=<ref>` on the "
                "feature branch BEFORE merging so they ride into the merge."
            )
    else:
        warning_text = checklist_sync.get("warning") or "sync_not_ok"
        warnings.append(f"checklist_sync_failed: {warning_text}")

    archived_now = False
    if not already_terminal:
        archive_ok, archive_err = _archive(repo, task_ref)
        if not archive_ok:
            _common.emit(
                {
                    "ok": False,
                    "command": "task-finish",
                    "task_ref": task_ref,
                    "events": events,
                    "open_lanes": open_lanes,
                    "warnings": warnings,
                    "error": "archive_failed",
                    "stderr_summary": archive_err,
                }
            )
            return 2
        events.append("archived")
        archived_now = True

    # 0112 Bug 2 / internal (PMH-F9): daemon-less self-heal BACKSTOP. The task is
    # in task_archives and cleared from handoff_state, so the archived-orphan reaper
    # can CAS-close any non-terminal lane the primary self-reap above
    # (_reap_open_lanes_for_task) failed to close — the daemon reaper is the only
    # other GC and never runs in a daemon-less /offload flow. Runs whenever the task
    # is archived: just now (archived_now) OR already archived by a prior (e.g.
    # crashed) run (already_terminal on entry). Gating this on `not already_terminal`
    # (the prior bug) made it unreachable on exactly the re-invocation crash-recovery
    # path it exists for. Scoped to THIS finishing task; best-effort; never breaks.
    if archived_now or already_terminal:
        try:
            from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
                reap_task_archived_orphan_lanes,
            )

            orphan_reap = reap_task_archived_orphan_lanes(task_ref=task_ref, apply=True)
            if isinstance(orphan_reap, dict):
                reaped_now: list[str] = []
                for entry in orphan_reap.get("closed") or []:
                    lane_id = entry.get("lane_id")
                    if not lane_id:
                        continue
                    lane_id = str(lane_id)
                    reaped_now.append(lane_id)
                    if not any(r.get("lane_id") == lane_id for r in reaped_lanes):
                        reaped_lanes.append({"lane_id": lane_id, "status": "closed_stale"})
                if reaped_now:
                    events.append("archived_orphan_lanes_reaped")
                    # PMH-F10: reconcile the pre-archive open-lane report. The
                    # backstop just closed these lanes, so they are no longer
                    # "open"; drop the now-stale lane_close_skipped warning (and
                    # rebuild it only if other lanes still remain open) — otherwise
                    # the receipt reports the same lane as both open AND reaped and
                    # tells the operator to manually close an already-closed lane.
                    reaped_set = set(reaped_now)
                    still_open = [lane for lane in open_lanes if lane not in reaped_set]
                    if still_open != open_lanes:
                        open_lanes = still_open
                        warnings[:] = [
                            w for w in warnings if not w.startswith("lane_close_skipped:")
                        ]
                        if open_lanes:
                            warnings.append(
                                "lane_close_skipped: open lanes detected "
                                f"({', '.join(sorted(open_lanes))}); close via MCP "
                                "manage_worktree_lane(operation='close') before rerunning"
                            )
                # OBS-08: do not drop truncated / error / failed / triage signals —
                # a degraded reap must not look identical to a clean one.
                warnings.extend(_archived_orphan_reap_degradation_warnings(orphan_reap))
        except Exception as exc:  # noqa: BLE001 — backstop must never break the finish
            warnings.append(f"archived_orphan_reap_failed: {exc}")

    dashboard_ok, dashboard_err = _render_dashboard(repo)
    if dashboard_ok:
        events.append("dashboard_rendered")
    else:
        warnings.append(f"render_dashboard_failed: {dashboard_err}")

    operator_force = bool(args.operator_force)
    self_pid = os.getpid()
    session_id, _, _ = session_heartbeat.resolve_session(primary)
    active_guard: dict[str, Any] = {"worktree": None, "branch": None, "forced": operator_force}

    # F1: configure handoff runtime so the shared-path reader can open the
    # primary workspace DB in a fresh CLI process (same trap as
    # _common._live_task_refs / slice_commit). Status dispositions [G4/G6]:
    # configured | absent (hermetic) | no_db (no lanes) | failed (refuse).
    handoff_runtime_status = _ensure_handoff_runtime(primary)
    # F3: exclude this finish's own lane_id so empty/mismatched target_branch
    # cannot self-wedge against the finishing task's still-open row.
    exclude_lane_id = _resolve_finishing_lane_id(
        task_ref=task_ref,
        target_worktree_path=target_worktree_path,
        open_lanes=open_lanes,
        reaped_lanes=reaped_lanes,
    )

    worktree_status, worktree_err = _remove_worktree(
        primary,
        target_worktree_path,
        target_branch,
        operator_force=operator_force,
        self_pid=self_pid,
        exclude_session_id=session_id,
        exclude_lane_id=exclude_lane_id,
        handoff_runtime_status=handoff_runtime_status,
    )
    if worktree_status == "skipped_active":
        active_guard["worktree"] = worktree_err
    if worktree_status in ("removed", "removed_force"):
        events.append("worktree_removed")
        _maybe_prune_uv_cache(events, warnings)
    elif worktree_status == "failed":
        warnings.append(f"worktree_remove_failed: {worktree_err}")

    branch_status, branch_err = _delete_merged_branch(
        primary,
        target_branch,
        operator_force=operator_force,
        self_pid=self_pid,
        exclude_session_id=session_id,
    )
    if branch_status == "skipped_active":
        active_guard["branch"] = branch_err
    if branch_status == "deleted":
        events.append("feature_branch_deleted")
    elif branch_status == "failed":
        warnings.append(f"feature_branch_delete_failed: {branch_err}")

    # Pass primary (not cwd-anchored ``repo``): by this point the linked
    # worktree may already be removed, so ``repo`` can be a deleted path and
    # ``canonical_workspace_root`` soft-fails — the archive sweep would pin
    # --workspace-root at the dead tree instead of the primary DB (R3).
    auto_reaped, auto_reap_err = _auto_reap_stale_rows(primary, task_ref)
    if auto_reaped:
        events.append("auto_reap")
    if auto_reap_err:
        warnings.append(f"auto_reap_failed: {auto_reap_err}")

    receipt: dict[str, Any] = {
        "ok": True,
        "command": "task-finish",
        "task_ref": task_ref,
        "target_worktree_path": target_worktree_path,
        "worktree_status": worktree_status,
        "target_branch": target_branch,
        "branch_status": branch_status,
        "open_lanes": open_lanes,
        "reaped_lanes": reaped_lanes,
        "events": events,
        "warnings": warnings,
        "checklist_sync": checklist_sync,
        "auto_reaped": auto_reaped,
        "active_guard": active_guard,
        "agent_errors_replay": agent_errors_replay,
        "terminal_guard_replay": terminal_guard_replay,
        "stale_dev_temp_reap": stale_dev_temp_reap,
    }

    # implementation note S2: request singleton codemap reindex for the finishing
    # HEAD SHA and spawn one detached runner. Never blocks teardown and
    # never flips ok — codemap problems are at most a receipt note.
    # Pass primary (not cwd-anchored ``repo``): by this point the linked
    # worktree may already be removed, so ``repo`` can be a deleted path
    # and the queue write would silently no-op as handoff_db_missing.
    finish_sha = resolver.head_sha(primary) or resolver.head_sha(repo) or ""
    _note_codemap_reindex_on_receipt(receipt, primary, finish_sha)

    if not args.emit_json:
        sync_summary = (
            f"sync={'ok' if checklist_sync.get('ok') else 'warn'}"
            f" ticked={checklist_sync.get('ticked', 0)}"
        )
        sys.stderr.write(
            f"task-finish: task_ref={task_ref} archived dashboard={'ok' if dashboard_ok else 'warn'} "
            f"worktree={worktree_status} branch={branch_status} "
            f"{sync_summary}"
            + (f" warnings={len(warnings)}" if warnings else "")
            + "\n"
        )

    _common.emit(receipt)
    return 0
