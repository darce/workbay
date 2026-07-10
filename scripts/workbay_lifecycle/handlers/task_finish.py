"""Mutating ``task-finish`` subcommand.

Wraps the canonical end-of-task close sequence documented at
``packages/workbay-system/skills/branch-lifecycle/body.md`` step 9 in a
single Make-callable target so the order is mechanical and a stalled
operator never leaves an inconsistent dashboard / unarchived task row /
orphaned linked worktree behind.

The sequence:

1. ``mcp-workbay-handoff set --task-ref <ref> --status done --status-only``
   — flip the live row to ``done`` so the row is archive-eligible.
2. (Best-effort) record any open worktree lanes for the task. Lane
   close itself has no direct CLI surface in
   ``mcp-workbay-orchestrator``; if open lanes are detected the receipt
   surfaces a ``lane_close_skipped`` warning so the operator knows to
   close them via MCP before re-running. Absent a state DB or
   orchestrator install we proceed silently.
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
    when no archive row exists, the snapshot is unparseable, or the
    ``task_archives`` table is absent (older DB) — every degraded path
    collapses to ``skipped_unset`` so the close sequence never raises.

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
    if snapshot_json:
        try:
            snapshot = json.loads(snapshot_json)
        except (ValueError, TypeError):
            snapshot = None
        if isinstance(snapshot, dict) and isinstance(snapshot.get("active"), dict):
            active = snapshot["active"]
    target_branch = active.get("target_branch") or archived_branch or ""
    target_worktree_path = active.get("target_worktree_path") or ""
    return {
        "target_branch": str(target_branch),
        "target_worktree_path": str(target_worktree_path),
    }


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
                "WHERE task_ref = ? AND COALESCE(status, '') NOT IN ('closed', 'archived')",
                (task_ref,),
            )
            return [str(row[0]) for row in cursor.fetchall() if row[0]]
    except sqlite3.Error:
        return []


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


def _auto_reap_stale_rows(repo: Path) -> tuple[list[str], str | None]:
    """Bounded post-finish sweep: reap closeable live rows and done non-scratch rows."""
    if not _task_finish_auto_reap_enabled():
        return [], None
    reaped: list[str] = []
    for operation in ("reap", "reap_done"):
        argv = _common.handoff_command_argv(
            repo, "archive", "--operation", operation, "--apply",
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
        if stderr and ("can't stat" in stderr or "permission denied" in stderr):
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
    """True when ``branch`` is fully merged into the primary worktree HEAD.

    ``git merge-base --is-ancestor <branch> HEAD`` exits 0 iff every commit
    on ``branch`` is reachable from HEAD — i.e. the branch's committed work
    is already preserved on the integration branch. An empty branch or any
    git error is treated as NOT merged (the safe default: do not force).
    """
    if not branch:
        return False
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "merge-base", "--is-ancestor", branch, "HEAD"]
    )
    return proc.returncode == 0


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


def _remove_worktree(
    primary: Path,
    target_worktree_path: str,
    target_branch: str = "",
    *,
    operator_force: bool = False,
    self_pid: int | None = None,
    exclude_session_id: str = "",
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
      process, or non-regenerable dirty work) and ``operator_force`` was not
      set; teardown is refused.
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

    preflight_err = _common.handoff_cli_required_preflight_error()
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
    agent_errors_replay = _common.maybe_replay_agent_error_spool(repo)
    terminal_guard_replay = _common.maybe_replay_terminal_guard_spool(repo)
    events_seed: list[str] = []
    if agent_errors_replay is not None:
        events_seed.append("agent_errors_replayed")
    if terminal_guard_replay is not None:
        events_seed.append("terminal_guard_replayed")

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
    warnings: list[str] = []

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

    open_lanes = _open_lanes_for_task(repo, task_ref)
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

    dashboard_ok, dashboard_err = _render_dashboard(repo)
    if dashboard_ok:
        events.append("dashboard_rendered")
    else:
        warnings.append(f"render_dashboard_failed: {dashboard_err}")

    operator_force = bool(args.operator_force)
    self_pid = os.getpid()
    session_id, _, _ = session_heartbeat.resolve_session(primary)
    active_guard: dict[str, Any] = {"worktree": None, "branch": None, "forced": operator_force}

    worktree_status, worktree_err = _remove_worktree(
        primary,
        target_worktree_path,
        target_branch,
        operator_force=operator_force,
        self_pid=self_pid,
        exclude_session_id=session_id,
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

    auto_reaped, auto_reap_err = _auto_reap_stale_rows(repo)
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
        "events": events,
        "warnings": warnings,
        "checklist_sync": checklist_sync,
        "auto_reaped": auto_reaped,
        "active_guard": active_guard,
        "agent_errors_replay": agent_errors_replay,
        "terminal_guard_replay": terminal_guard_replay,
    }

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
