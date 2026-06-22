"""Mutating ``finalize-plan`` subcommand.

Persist the FINAL task-plan checklist ticks onto the feature branch BEFORE
the merge, so they ride into the integration branch with the merge commit.

Why a dedicated pre-merge step: task-plan ``- [ ]`` boxes flip to ``- [x]``
from recorded handoff evidence via ``sync-task-plan-checklist``. ``make
slice-commit`` runs that sweep per slice, so ticks are committed as work
lands. But evidence recorded AFTER the last slice-commit (a final
``close_slice``, a stand-alone commit) leaves boxes whose proof exists yet
whose ticks were never committed. ``task-finish`` runs POST-merge in the
linked worktree it is about to delete, so any ticks it writes there are
discarded — and plan docs reach the integration branch ONLY via the
feature-branch merge, never a direct commit to it. ``finalize-plan`` closes
that gap: run it on the feature branch right before merging.

The sweep is targeted — only the resolved task-plan file is staged and
committed, so unrelated working-tree changes are never swept in. When there
is nothing to tick the command is a clean no-op (``commit_status`` =
``nothing_to_tick``). Failure-as-warning: a malformed/unresolved plan never
hard-fails the command (mirrors ``run_checklist_sync``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from . import _common


def _git(repo: Path, *args: str) -> tuple[int, str]:
    proc = _common.run_subprocess(["git", "-C", str(repo), *args])
    return proc.returncode, (proc.stdout or "").strip()


def _commit_plan(plan_path: Path, task_ref: str) -> tuple[str, str | None]:
    """Stage + commit ONLY ``plan_path`` on its worktree's branch.

    Returns ``(status, detail)``. Status values:

    * ``committed`` — a commit was created; detail is the short SHA.
    * ``nothing_staged`` — the plan already matched HEAD after staging (no-op).
    * ``failed`` — a git step errored; detail is the message.
    """
    rc, toplevel = _git(plan_path.parent, "rev-parse", "--show-toplevel")
    if rc != 0 or not toplevel:
        return "failed", f"not a git worktree: {plan_path.parent}"
    top = Path(toplevel)
    rc, err = _git(top, "add", "--", str(plan_path))
    if rc != 0:
        return "failed", f"git add: {err[:200]}"
    # Nothing to commit if the staged plan already matches HEAD.
    staged = _common.run_subprocess(
        ["git", "-C", str(top), "diff", "--cached", "--quiet", "--", str(plan_path)]
    )
    if staged.returncode == 0:
        return "nothing_staged", None
    # Pathspec commit: only the plan file is included regardless of the rest
    # of the index, so a finalize never sweeps in unrelated working changes.
    proc = _common.run_subprocess(
        ["git", "-C", str(top), "commit",
         "-m", f"chore(plan): finalize checklist ticks for {task_ref}",
         "--", str(plan_path)]
    )
    if proc.returncode != 0:
        return "failed", f"git commit: {(proc.stderr or proc.stdout or '').strip()[:200]}"
    rc, sha = _git(top, "rev-parse", "--short", "HEAD")
    return "committed", (sha if rc == 0 else None)


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle finalize-plan", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = _common.repo_root()
    if repo is None:
        _common.emit({"ok": False, "command": "finalize-plan", "error": "not_in_git_repo"})
        return 2

    task_ref = (args.task or "").strip().upper()
    if not task_ref:
        view = _common.derive_workspace_summary_view(repo)
        if view.shape == "single" and view.task_ref:
            task_ref = view.task_ref
    if not task_ref:
        _common.emit({"ok": False, "command": "finalize-plan", "error": "task_ref_required"})
        return 2

    sync = _common.run_checklist_sync(repo, task_ref, apply=True)
    plan_path_str = sync.get("plan_path")
    commit_status = "skipped"
    commit_sha: str | None = None
    warnings: list[str] = []

    if not sync.get("ok"):
        commit_status = "sync_failed"
        warnings.append(f"checklist_sync_failed: {sync.get('warning') or 'sync_not_ok'}")
    elif sync.get("skipped"):
        commit_status = "skipped_no_plan"
    elif not sync.get("applied"):
        commit_status = "nothing_to_tick"
    elif not plan_path_str:
        commit_status = "failed"
        warnings.append("sync_applied_without_plan_path")
    else:
        commit_status, commit_sha = _commit_plan(Path(plan_path_str), task_ref)
        if commit_status == "failed":
            warnings.append(f"plan_commit_failed: {commit_sha}")
            commit_sha = None

    receipt: dict[str, Any] = {
        "ok": True,
        "command": "finalize-plan",
        "task_ref": task_ref,
        "ticked": sync.get("ticked", 0),
        "plan_path": plan_path_str,
        "commit_status": commit_status,
        "commit_sha": commit_sha,
        "warnings": warnings,
        "checklist_sync": sync,
    }
    if not args.emit_json:
        sys.stderr.write(
            f"finalize-plan: task_ref={task_ref} ticked={sync.get('ticked', 0)} "
            f"commit={commit_status}"
            + (f" sha={commit_sha}" if commit_sha else "")
            + (f" warnings={len(warnings)}" if warnings else "")
            + "\n"
        )
    _common.emit(receipt)
    return 0
