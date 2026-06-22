"""Mutating ``plan-done`` subcommand.

Terminal helper for on-main ``MAINT-*`` planning/audit passes. Sets
``status=done`` (explicit close signal) and runs the on-main MAINT
``tasks_gc`` sweep so the row archives without a feature-branch merge
proof path.
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
        ["git", "-C", str(primary), "symbolic-ref", "--short", "-q", "HEAD"]
    )
    branch = (proc.stdout or "").strip()
    return branch or "main"


def _is_integration_target_branch(target_branch: str | None, repo: Path) -> bool:
    branch = (target_branch or "").strip()
    if not branch:
        return False
    integration = _integration_ref(repo)
    protected = {"main", "master", integration, f"origin/{integration}"}
    return branch in protected


def _read_handoff_identity(repo: Path, task_ref: str) -> dict[str, Any]:
    canonical = resolver.canonical_workspace_root(repo) or repo
    db_path = canonical / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return {}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT task_ref, target_branch FROM handoff_state WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()
            if row is None:
                return {}
            return {
                "task_ref": str(row[0] or ""),
                "target_branch": str(row[1] or ""),
            }
    except sqlite3.Error:
        return {}


def _is_onmain_maint_identity(identity: dict[str, Any], repo: Path) -> bool:
    task_ref = str(identity.get("task_ref") or "")
    target_branch = str(identity.get("target_branch") or "")
    return task_ref.startswith("MAINT-") and _is_integration_target_branch(target_branch, repo)


def _set_status_done(repo: Path, task_ref: str) -> tuple[bool, str | None]:
    argv = _common.handoff_command_argv(
        repo,
        "set",
        "--task-ref",
        task_ref,
        "--status",
        "done",
        "--status-only",
    )
    proc = _common.run_subprocess(argv)
    if proc.returncode == 0:
        return True, None
    return False, (proc.stderr or proc.stdout or "").strip()[:300]


def _run_tasks_gc(repo: Path) -> tuple[bool, list[str], str | None]:
    argv = _common.handoff_command_argv(repo, "archive", "--operation", "gc", "--apply")
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return False, [], (proc.stderr or proc.stdout or "").strip()[:300]
    archived: list[str] = []
    try:
        envelope = json.loads(proc.stdout)
        data = envelope.get("data") if isinstance(envelope, dict) else None
        if isinstance(data, dict):
            raw = data.get("archived")
            if isinstance(raw, list):
                archived = [str(item) for item in raw if isinstance(item, str)]
    except (ValueError, TypeError, json.JSONDecodeError):
        pass
    return True, archived, None


def _render_dashboard(repo: Path) -> tuple[bool, str | None]:
    argv = _common.handoff_command_argv(repo, "render-handoff", "--kind", "dashboard")
    proc = _common.run_subprocess(argv)
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

    task_ref = (args.task or "").strip().upper()
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

    identity = _read_handoff_identity(repo, task_ref)
    if not identity:
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

    status_ok, status_err = _set_status_done(repo, task_ref)
    if not status_ok:
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": events,
                "error": "set_status_done_failed",
                "stderr_summary": status_err,
            }
        )
        return 2
    events.append("status_done_set")

    gc_ok, archived, gc_err = _run_tasks_gc(repo)
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
    events.append("tasks_gc_applied")

    if _read_handoff_identity(repo, task_ref):
        _common.emit(
            {
                "ok": False,
                "command": "plan-done",
                "task_ref": task_ref,
                "events": events,
                "error": "archive_not_applied",
                "archived": archived,
                "detail_reason": "tasks_gc_left_row_live",
            }
        )
        return 2

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
