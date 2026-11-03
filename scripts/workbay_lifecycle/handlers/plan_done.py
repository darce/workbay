"""Mutating ``plan-done`` subcommand.

Terminal helper for on-main ``MAINT-*`` planning/audit passes. Sets
``status=done`` (explicit close signal) and archives the row without a
feature-branch merge proof path, then runs the on-main MAINT ``tasks_gc``
sweep.

Both terminal writes go through the handoff ``complete-task`` port with the
exact selected row (revision/status/target): the port re-qualifies the target
and refuses any row changed since selection, so a concurrent writer is never
overwritten or archived. ``tasks_gc`` stays the authority on whether a MAINT
row may archive at all (e.g. a parented row waits for its parent).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import resolver

from . import _common


def _integration_ref(repo: Path) -> str:
    primary = resolver.canonical_workspace_root(repo) or repo
    proc = _common.run_subprocess(
        ["git", "-C", str(primary), "symbolic-ref", "--short", "-q", "refs/remotes/origin/HEAD"]
    )
    branch = (proc.stdout or "").strip().removeprefix("origin/") if proc.returncode == 0 else ""
    return branch or "main"


def _is_integration_target_branch(target_branch: str | None, repo: Path) -> bool:
    branch = (target_branch or "").strip()
    if not branch:
        return False
    integration = _integration_ref(repo)
    protected = {"main", "master", integration, f"origin/{integration}"}
    return branch in protected


def _resolve_task_ref(repo: Path, raw: str) -> tuple[str, list[str]]:
    """Prefer exact stored spelling, refusing ambiguous case-insensitive refs."""
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return raw.upper(), []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            tables = ("handoff_state", "task_archives")
            for table in tables:
                try:
                    row = conn.execute(
                        f"SELECT task_ref FROM {table} WHERE task_ref = ? LIMIT 1",
                        (raw,),
                    ).fetchone()
                except sqlite3.Error:
                    continue
                if row is not None:
                    return str(row[0]), []

            matches: set[str] = set()
            for table in tables:
                try:
                    rows = conn.execute(
                        f"SELECT task_ref FROM {table} WHERE lower(task_ref) = lower(?)",
                        (raw,),
                    ).fetchall()
                except sqlite3.Error:
                    # Older databases may contain only one of these tables.
                    continue
                matches.update(str(row[0]) for row in rows)
            candidates = sorted(matches)
            if len(candidates) == 1:
                return candidates[0], candidates
            if candidates:
                return raw, candidates
    except sqlite3.Error:
        pass
    return raw.upper(), []


def _read_handoff_identity(repo: Path, task_ref: str) -> dict[str, Any]:
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return {}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT task_ref, target_branch, status, target_worktree_path FROM handoff_state WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()
            if row is None:
                return {}
            identity: dict[str, Any] = {
                "task_ref": str(row[0] or ""),
                "target_branch": str(row[1] or ""),
                "status": str(row[2] or ""),
                "target_worktree_path": str(row[3] or ""),
                "revision": None,
            }
            try:
                revision = conn.execute(
                    "SELECT revision FROM handoff_state WHERE task_ref = ?",
                    (task_ref,),
                ).fetchone()
            except sqlite3.Error:
                # Legacy schema without a revision: the row cannot be selected
                # exactly, so terminal writes fail closed below.
                revision = None
            if revision is not None and isinstance(revision[0], int):
                identity["revision"] = revision[0]
            return identity
    except sqlite3.Error:
        return {}


def _archived_snapshot_exists(repo: Path, task_ref: str) -> bool:
    """Return True if ``task_ref`` has a ``task_archives`` snapshot.

    Lets the caller distinguish a legitimate idempotent re-run (the task was
    real and already torn down → an archive row exists) from a typo'd or
    never-existed ref (no live row AND no archive snapshot). A missing DB or
    missing ``task_archives`` table (older schema) collapses to False, so the
    safe default is to treat the ref as never-existed rather than silently
    succeed.
    """
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return False
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT 1 FROM task_archives WHERE task_ref = ? LIMIT 1",
                (task_ref,),
            ).fetchone()
            return row is not None
    except sqlite3.Error:
        return False


def _is_onmain_maint_identity(identity: dict[str, Any], repo: Path) -> bool:
    task_ref = str(identity.get("task_ref") or "")
    target_branch = str(identity.get("target_branch") or "")
    return task_ref.startswith("MAINT-") and _is_integration_target_branch(target_branch, repo)


def _complete_task(
    repo: Path,
    task_ref: str,
    identity: dict[str, Any],
    *,
    operation: str,
    expected_revision: int,
    expected_status: str,
    integration_ref: str,
) -> tuple[bool, dict[str, Any]]:
    """Run one fenced ``complete-task`` write against the selected row.

    The port owns the qualification and the compare-and-write; this caller
    only reports its envelope. Any non-success (conflict, held, missing port,
    unparseable output) leaves the decision to the port and fails closed.
    """
    argv = _common.handoff_command_argv(
        repo,
        "complete-task",
        "--operation",
        operation,
        "--task-ref",
        task_ref,
        "--expected-revision",
        str(expected_revision),
        "--expected-status",
        expected_status,
        "--expected-target-branch",
        str(identity.get("target_branch") or ""),
        "--expected-target-worktree-path",
        str(identity.get("target_worktree_path") or ""),
        "--integration-ref",
        integration_ref,
    )
    proc = _common.run_handoff_subprocess(repo, argv)
    try:
        envelope = json.loads(proc.stdout or "")
    except (ValueError, TypeError):
        envelope = None
    data = envelope.get("data") if isinstance(envelope, dict) else None
    data = data if isinstance(data, dict) else {}
    if proc.returncode == 0 and isinstance(envelope, dict) and envelope.get("ok") is True:
        return True, data
    detail = data.get("error") or proc.stderr or proc.stdout or ""
    return False, {
        "outcome": str(data.get("outcome") or "failed"),
        "error_code": data.get("error_code"),
        "detail_reason": str(detail).strip()[:300],
        "current_revision": data.get("current_revision"),
        "current_status": data.get("current_status"),
    }


def _set_status_done(
    repo: Path, task_ref: str, identity: dict[str, Any], integration_ref: str
) -> tuple[bool, dict[str, Any]]:
    return _complete_task(
        repo,
        task_ref,
        identity,
        operation="done",
        expected_revision=int(identity["revision"]),
        expected_status=str(identity.get("status") or ""),
        integration_ref=integration_ref,
    )


def _archive_done_row(
    repo: Path, task_ref: str, identity: dict[str, Any], done_revision: int, integration_ref: str
) -> tuple[bool, dict[str, Any]]:
    return _complete_task(
        repo,
        task_ref,
        identity,
        operation="archive",
        expected_revision=done_revision,
        expected_status="done",
        integration_ref=integration_ref,
    )


def _run_tasks_gc(repo: Path, *, apply: bool) -> tuple[bool, dict[str, list[str]] | None, str | None]:
    """Run ``tasks_gc``; ``None`` data means the envelope was unparseable."""
    argv = _common.handoff_command_argv(
        repo, "archive", "--operation", "gc", *(["--apply"] if apply else [])
    )
    proc = _common.run_handoff_subprocess(repo, argv)
    if proc.returncode != 0:
        return False, None, (proc.stderr or proc.stdout or "").strip()[:300]
    try:
        envelope = json.loads(proc.stdout)
    except (ValueError, TypeError):
        return True, None, None
    data = envelope.get("data") if isinstance(envelope, dict) else None
    if not isinstance(data, dict):
        return True, None, None
    lists: dict[str, list[str]] = {}
    for key in ("archived", "would_archive"):
        raw = data.get(key)
        lists[key] = [str(item) for item in raw if isinstance(item, str)] if isinstance(raw, list) else []
    return True, lists, None


def _render_dashboard(repo: Path) -> tuple[bool, str | None]:
    argv = _common.handoff_command_argv(repo, "render-handoff", "--kind", "dashboard")
    proc = _common.run_handoff_subprocess(repo, argv)
    if proc.returncode == 0:
        return True, None
    return False, (proc.stderr or proc.stdout or "").strip()[:300]


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle plan-done", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = _common.repo_root()
    if repo is None:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": None,
                "events": [],
                "error": "not_in_git_repo",
            }
        )
        return 2

    task_ref = (args.task or "").strip()
    if not task_ref:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": None,
                "events": [],
                "error": "task_ref_required",
            }
        )
        return 2

    task_ref, candidates = _resolve_task_ref(repo, task_ref)
    if len(candidates) > 1:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": [],
                "error": "task_ref_ambiguous_case",
                "candidates": candidates,
            }
        )
        return 2

    identity = _read_handoff_identity(repo, task_ref)
    if not identity:
        # Live row gone. Distinguish an idempotent re-run of a real task
        # (already archived → ok) from a typo'd / never-existed ref (no live
        # row AND no archive snapshot → keep the hard error so the operator
        # learns the task name was wrong rather than seeing a false success).
        if _archived_snapshot_exists(repo, task_ref):
            _common.emit(
                {
                    "ok": True,
                    "command": "plan-done",
                    "task_ref": task_ref,
                    "events": ["skipped_gone"],
                    "archived": [],
                    "warnings": [],
                }
            )
            return 0
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": [],
                "error": "task_row_missing",
            }
        )
        return 2
    if not _is_onmain_maint_identity(identity, repo):
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": [],
                "error": "not_onmain_maint_task",
                "target_branch": identity.get("target_branch", ""),
            }
        )
        return 2

    events: list[str] = []
    warnings: list[str] = []
    integration_ref = _integration_ref(repo)

    try:
        from workbay_handoff_mcp.import_export import task_completion_hold_reason

        hold = task_completion_hold_reason(
            task_ref=task_ref,
            status=str(identity.get("status") or ""),
            target_branch=identity.get("target_branch"),
            target_worktree_path=identity.get("target_worktree_path"),
            workspace_root=resolver.canonical_workspace_root(repo) or repo,
            integration_ref=integration_ref,
            explicit_completion=True,
        )
    except (ImportError, OSError, RuntimeError):
        hold = "completion guard unavailable"
    if hold:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": [],
                "error": "task_completion_held",
                "detail_reason": hold,
            }
        )
        return 2

    selected_revision = identity.get("revision")
    if not isinstance(selected_revision, int):
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": [],
                "error": "task_revision_unknown",
                "detail_reason": "selected row has no revision; refusing an unfenced terminal write",
            }
        )
        return 2

    if identity.get("status") == "done":
        # Already done: no status write, but the archive below still
        # re-qualifies the target at this exact selected revision.
        events.append("skipped_live_done")
        done_revision = selected_revision
    else:
        status_ok, done = _set_status_done(repo, task_ref, identity, integration_ref)
        if not status_ok:
            outcome = done.get("outcome")
            _common.emit(
                {
                    "ok": False,
                    "command": "plan-done",
                    "task_ref": task_ref,
                    "events": events,
                    "error": {
                        "conflict": "task_changed_after_qualification",
                        "held": "task_completion_held",
                    }.get(str(outcome), "set_status_done_failed"),
                    "outcome": outcome,
                    "detail_reason": done.get("detail_reason"),
                    "current_revision": done.get("current_revision"),
                    "current_status": done.get("current_status"),
                }
            )
            return 2
        events.append("status_done_set")
        done_revision = done.get("revision")
        if not isinstance(done_revision, int) or isinstance(done_revision, bool):
            _common.emit(
                {
                    "ok": False,
                    "command": "plan-done",
                    "task_ref": task_ref,
                    "events": events,
                    "error": "archive_not_applied",
                    "archived": [],
                    "detail_reason": "done_revision_unknown",
                }
            )
            return 2

    # tasks_gc decides whether this MAINT row may archive at all; the
    # subject itself is then archived only at the carried Done revision.
    gc_ok, gc_plan, gc_err = _run_tasks_gc(repo, apply=False)
    if not gc_ok:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": events,
                "error": "tasks_gc_failed",
                "stderr_summary": gc_err,
            }
        )
        return 2
    if gc_plan is None or task_ref not in gc_plan["would_archive"]:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": events,
                "error": "archive_not_applied",
                "archived": [],
                "detail_reason": "tasks_gc_eligibility_unknown" if gc_plan is None else "tasks_gc_left_row_live",
            }
        )
        return 2

    archive_ok, archive = _archive_done_row(repo, task_ref, identity, done_revision, integration_ref)
    if not archive_ok:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": events,
                "error": "archive_not_applied",
                "archived": [],
                "outcome": archive.get("outcome"),
                "detail_reason": archive.get("detail_reason"),
                "current_revision": archive.get("current_revision"),
                "current_status": archive.get("current_status"),
            }
        )
        return 2
    events.append("conditional_archive_applied")
    archived = [task_ref]

    if _read_handoff_identity(repo, task_ref):
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": events,
                "error": "archive_not_applied",
                "archived": [],
                "detail_reason": "conditional_archive_left_row_live",
            }
        )
        return 2

    # The subject is archived; the remaining on-main MAINT sweep keeps its
    # own per-row fencing and is reported, not required.
    sweep_ok, sweep, sweep_err = _run_tasks_gc(repo, apply=True)
    if not sweep_ok:
        warnings.append(f"tasks_gc_sweep_failed: {sweep_err}")
    else:
        events.append("tasks_gc_applied")
        for ref in (sweep or {}).get("archived", []):
            if ref not in archived:
                archived.append(ref)

    render_ok, render_err = _render_dashboard(repo)
    if not render_ok:
        warnings.append(f"dashboard_render_skipped: {render_err}")
    else:
        events.append("dashboard_rendered")

    _common.emit(
        {
            "ok": True,
            "command": "plan-done",
            "task_ref": task_ref,
            "events": events,
            "archived": archived,
            "warnings": warnings,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
