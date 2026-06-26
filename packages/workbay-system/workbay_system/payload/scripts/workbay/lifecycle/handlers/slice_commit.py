"""Mutating ``slice-commit`` subcommand.

Stages the current diff and creates one git commit for the active
slice. After a successful commit the handler shells out to
``mcp-workbay-handoff set`` so the actor-resolved write context refreshes
``handoff_state.updated_commit_sha`` from the new worktree HEAD — without
that projection, downstream commit-guarded writes (``update_review_finding``
in particular) see the stale slice-N-1 sha and reject fixes that actually
landed on a descendant (internal).

The projection is best-effort: a non-zero return from the read or write
CLI flips ``handoff_projection`` to ``"pending"`` with a
``projection_warning`` field but never fails the slice-commit itself.

Before ``git add``/``commit``, the handler invokes
``sync-task-plan-checklist --apply --quiet`` (internal; ordering
fix from internal) so any box the sync flips lands inside the
slice commit instead of being left as an uncommitted edit. The slim
sync receipt is merged under the parent receipt's ``checklist_sync``
key. The sync runs against the just-recorded ``close_slice`` decision's
``changed_files`` (by convention the agent records ``close_slice``
before invoking ``make slice-commit``, so its decision row is already
in the DB). Sync failure surfaces as ``checklist_sync.ok = False`` with
a ``warning`` field but never fails the slice-commit itself — a
malformed plan must not block a real close.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

import resolver

from . import _common


def _untracked_paths(repo: Path) -> list[str]:
    proc = _common.run_subprocess(["git", "-C", str(repo), "status", "--porcelain=v1", "-uall"])
    if proc.returncode != 0:
        return []
    paths: list[str] = []
    for line in proc.stdout.splitlines():
        if not line.startswith("?? "):
            continue
        paths.append(line[3:])
    return paths


def _project_commit_sha(
    repo: Path, task_ref: str
) -> tuple[str, str | None]:
    """Project the new HEAD commit_sha into the active row.

    Returns ``(projection_status, warning_or_None)`` where
    ``projection_status`` is ``"synced"`` on success or ``"pending"`` on
    any failure. Best-effort: never raises, never fails the parent
    command. Issues exactly two CLI calls: an identity read to capture
    the row's current revision (for the optimistic-concurrency guard),
    and a single ``set --commit-sha <head> --branch <branch>`` call
    that drives the row's ``updated_commit_sha`` / ``updated_branch``
    directly via the explicit-actor channel introduced in internal
    implementation note — bypassing the resolver's stored-row task_git fallback that
    used to require a calibrate-then-project two-call dance.
    """
    read_argv = _common.handoff_command_argv(
        repo, "state", "--sections", "identity", task_ref
    )
    read_proc = _common.run_subprocess(read_argv)
    if read_proc.returncode != 0:
        return "pending", (
            f"projection_read_failed: rc={read_proc.returncode} "
            f"stderr={read_proc.stderr.strip()[:200]!r}"
        )
    try:
        envelope = json.loads(read_proc.stdout)
    except json.JSONDecodeError as exc:
        return "pending", f"projection_read_unparseable: {exc!s}"
    active = (envelope.get("data") or {}).get("active") or {}
    revision = active.get("revision")
    if not isinstance(revision, int):
        return "pending", "projection_revision_missing"
    head = resolver.head_sha(repo)
    branch = resolver.current_branch(repo)
    if not head or not branch:
        return "pending", "projection_git_context_unavailable"
    set_argv = _common.handoff_command_argv(
        repo, "set",
        "--task-ref", task_ref,
        "--expected-revision", str(revision),
        "--commit-sha", head,
        "--branch", branch,
    )
    set_proc = _common.run_subprocess(set_argv)
    if set_proc.returncode != 0:
        return "pending", (
            f"projection_write_failed: rc={set_proc.returncode} "
            f"stderr={set_proc.stderr.strip()[:200]!r}"
        )
    return "synced", None


def _emit_error(
    reason: str,
    *,
    task_ref: str | None = None,
    branch: str = "",
    head: str = "",
    worktree_path: str = "",
    msg: str = "",
    dirty_summary: dict[str, int] | None = None,
    untracked_paths: list[str] | None = None,
    included_untracked: bool = False,
) -> int:
    receipt: dict[str, Any] = {
        "ok": False,
        "command": "slice-commit",
        "task_ref": task_ref,
        "branch": branch,
        "worktree_path": worktree_path,
        "head": head,
        "handoff_projection": "pending",
        "events": [],
        "commit_message": msg,
        "commit_sha": "",
        "previous_head": head,
        "dirty_summary": dirty_summary or {"staged": 0, "unstaged": 0, "untracked": 0, "total": 0},
        "untracked_paths": untracked_paths or [],
        "included_untracked": included_untracked,
        "error": reason,
    }
    _common.emit(receipt)
    return 2


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle slice-commit", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--msg", dest="msg", default="")
    parser.add_argument(
        "--include-untracked",
        dest="include_untracked",
        action="store_true",
        default=False,
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    msg = (args.msg or "").strip()
    if not msg:
        return _emit_error("msg_required", msg=msg, included_untracked=args.include_untracked)

    repo = _common.repo_root()
    if repo is None:
        return _emit_error("not_in_git_repo", msg=msg, included_untracked=args.include_untracked)

    git_facts = _common.gather_git_facts(repo)
    untracked_paths = _untracked_paths(repo)
    task_ref = (args.task or "").strip().upper() or git_facts.derived_task_ref
    if not task_ref:
        return _emit_error(
            "task_ref_required",
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
        )

    if git_facts.dirty_summary.get("total", 0) == 0:
        return _emit_error(
            "nothing_to_commit",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
        )

    if untracked_paths and not args.include_untracked:
        return _emit_error(
            "untracked_files_present",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=False,
        )

    checklist_sync = _common.run_checklist_sync(repo, task_ref)

    add_argv = ["git", "-C", str(repo), "add", "-A" if args.include_untracked else "-u"]
    add_proc = _common.run_subprocess(add_argv)
    if add_proc.returncode != 0:
        return _emit_error(
            "git_add_failed",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
        )

    commit_proc = _common.run_subprocess(
        ["git", "-C", str(repo), "commit", "-m", msg]
    )
    if commit_proc.returncode != 0:
        return _emit_error(
            "git_commit_failed",
            task_ref=task_ref,
            branch=git_facts.branch,
            head=git_facts.head,
            worktree_path=str(repo),
            msg=msg,
            dirty_summary=git_facts.dirty_summary,
            untracked_paths=untracked_paths,
            included_untracked=args.include_untracked,
        )

    commit_sha = resolver.head_sha(repo) or ""
    projection_status, projection_warning = _project_commit_sha(repo, task_ref)
    events = ["staged_all" if args.include_untracked else "staged_tracked", "commit_created"]
    if projection_status == "synced":
        events.append("commit_sha_projected")
    if checklist_sync.get("ok") and checklist_sync.get("ticked", 0):
        events.append("checklist_sync_applied")
    receipt = {
        "ok": True,
        "command": "slice-commit",
        "task_ref": task_ref,
        "branch": git_facts.branch,
        "worktree_path": str(repo),
        "head": commit_sha,
        "handoff_projection": projection_status,
        "events": events,
        "commit_message": msg,
        "commit_sha": commit_sha,
        "previous_head": git_facts.head,
        "dirty_summary": git_facts.dirty_summary,
        "untracked_paths": untracked_paths,
        "included_untracked": args.include_untracked,
        "checklist_sync": checklist_sync,
    }
    if projection_warning is not None:
        receipt["projection_warning"] = projection_warning

    if not args.emit_json:
        sync_summary = (
            f"sync={'ok' if checklist_sync.get('ok') else 'warn'}"
            f" ticked={checklist_sync.get('ticked', 0)}"
        )
        sys.stderr.write(
            f"slice-commit: task_ref={task_ref} branch={git_facts.branch} "
            f"previous_head={git_facts.head[:12]} commit_sha={commit_sha[:12]} "
            f"msg={shlex.quote(msg)} projection={projection_status} "
            f"{sync_summary}\n"
        )
        if projection_warning is not None:
            sys.stderr.write(
                f"slice-commit: projection_warning: {projection_warning}\n"
            )

    _common.emit(receipt)
    return 0
