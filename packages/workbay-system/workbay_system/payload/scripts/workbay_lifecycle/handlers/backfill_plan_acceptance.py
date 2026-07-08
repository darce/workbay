"""``plan-accept-backfill`` subcommand (internal).

One-shot acceptance walk over every live ``handoff_state`` row whose
plan baseline is missing from ``main``. For each row, the handler
applies the same gate as :mod:`plan_accept` -- latest planning verdict
exactly ``pass``, zero open planning findings -- and emits a per-task
docs-only commit recipe when the gate clears. Rows that already have
their plan on ``main`` are reported as ``already_accepted`` so re-runs
are no-ops.

The handler is intentionally additive in receipt-only mode (default):
it prints what *would* be accepted but never touches the index.
``--local`` performs the docs-only checkout+commit cycle inline once
per ready task; pre-flight enforces canonical-root, ``main`` checkout,
and clean tree just like the single-task handler.
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
from .plan_baseline import evaluate_plan_baseline


_LIVE_STATUSES = ("in_progress", "review", "blocked")


def _query_handoff_rows(repo: Path) -> list[dict[str, Any]]:
    """Return live handoff rows from the MCP CLI.

    Returns an empty list on any failure so the backfill degrades to a
    no-op receipt rather than crashing.
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(workspace),
        "handoff-rows",
        "--status", *_LIVE_STATUSES,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return []
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _plan_exists_on_branch(repo: Path, branch: str, plan_path: str) -> bool:
    """Return True when ``<branch>:<plan_path>`` resolves in the local repo.

    Uses ``git cat-file -e <branch>:<path>`` so a missing branch, missing
    blob, or unreadable repo all collapse to False. For target branches,
    that means the row is not safe to accept yet because ``plan-show``
    would not be able to read the registered plan.
    """
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "cat-file", "-e", f"{branch}:{plan_path}"],
    )
    return proc.returncode == 0


def _plan_exists_on_main(repo: Path, plan_path: str) -> bool:
    return _plan_exists_on_branch(repo, "main", plan_path)


def _build_accept_command(*, task_ref: str, branch: str, plan_path: str) -> str:
    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    return (
        f"git switch main && "
        f"git checkout {shlex.quote(branch)} -- {shlex.quote(plan_path)} && "
        f"git add {shlex.quote(plan_path)} && "
        f"git commit -m {shlex.quote(msg)}"
    )


def _is_worktree_clean(repo: Path) -> bool:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "status", "--porcelain"],
    )
    return proc.returncode == 0 and not proc.stdout.strip()


def _current_branch(repo: Path) -> str:
    proc = _common.run_subprocess(
        ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _restore_path_from_head(repo: Path, plan_path: str) -> None:
    restore = _common.run_subprocess(
        ["git", "-C", str(repo), "restore", "--source=HEAD", "--staged", "--worktree", "--", plan_path],
    )
    if restore.returncode == 0:
        return
    _common.run_subprocess(["git", "-C", str(repo), "reset", "--", plan_path])
    if _plan_exists_on_branch(repo, "HEAD", plan_path):
        _common.run_subprocess(["git", "-C", str(repo), "checkout", "--", plan_path])
        return
    try:
        (repo / plan_path).unlink()
    except FileNotFoundError:
        pass


def _apply_local_accept(
    repo: Path, *, task_ref: str, branch: str, plan_path: str
) -> tuple[bool, str | None]:
    msg = f"docs({task_ref.lower()}): accept plan {plan_path}"
    steps: list[list[str]] = [
        ["git", "-C", str(repo), "checkout", branch, "--", plan_path],
        ["git", "-C", str(repo), "add", plan_path],
        ["git", "-C", str(repo), "commit", "-m", msg],
    ]
    for argv in steps:
        proc = _common.run_subprocess(argv)
        if proc.returncode != 0:
            _restore_path_from_head(repo, plan_path)
            return False, f"{argv[3]}_failed: {proc.stderr.strip() or proc.stdout.strip()}"
    return True, None


def _evaluate_row(
    repo: Path,
    row: dict[str, Any],
) -> dict[str, Any]:
    """Return a per-task receipt entry for one handoff row."""
    task_ref = row.get("task_ref") or ""
    plan_path = row.get("task_plan_path")
    target_branch = row.get("target_branch")

    entry: dict[str, Any] = {
        "task_ref": task_ref,
        "target_branch": target_branch,
        "task_plan_path": plan_path,
    }
    baseline = evaluate_plan_baseline(
        repo,
        task_ref=str(task_ref),
        task_plan_path=str(plan_path) if isinstance(plan_path, str) else None,
        target_branch=str(target_branch) if isinstance(target_branch, str) else None,
    )
    entry.update(baseline.to_dict())

    if not baseline.acceptance_ready:
        entry["action"] = "skip"
        entry["reason"] = baseline.reason
        return entry

    entry["action"] = "accept"
    entry["next_command"] = _build_accept_command(
        task_ref=task_ref, branch=target_branch, plan_path=plan_path,
    )
    return entry


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle plan-accept-backfill", add_help=True
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument("--task", dest="task_ref", default="")
    parser.add_argument(
        "--local",
        dest="local",
        action="store_true",
        default=False,
        help=(
            "Apply each ready task's docs-only checkout+commit inline "
            "(requires canonical-root checkout on main with a clean tree)."
        ),
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    canonical = resolver.canonical_workspace_root(repo) or repo

    task_ref_filter = args.task_ref.strip().upper()
    rows = _query_handoff_rows(repo)
    if task_ref_filter:
        rows = [row for row in rows if str(row.get("task_ref") or "").upper() == task_ref_filter]
    entries = [_evaluate_row(repo, row) for row in rows]

    accepted_count = sum(1 for e in entries if e.get("action") == "accept")
    skipped_count = sum(1 for e in entries if e.get("action") == "skip")

    local_errors: list[str] = []
    if args.local and accepted_count > 0:
        if repo.resolve() != canonical.resolve():
            local_errors.append("local_requires_canonical_root")
        elif _current_branch(repo) != "main":
            local_errors.append("local_requires_main_checkout")
        elif not _is_worktree_clean(repo):
            local_errors.append("local_requires_clean_tree")
        else:
            for entry in entries:
                if entry.get("action") != "accept":
                    continue
                ok, err = _apply_local_accept(
                    repo,
                    task_ref=entry["task_ref"],
                    branch=entry["target_branch"],
                    plan_path=entry["task_plan_path"],
                )
                if ok:
                    entry["accepted"] = True
                else:
                    local_errors.append(f"{entry['task_ref']}:{err or 'local_accept_failed'}")
                    entry["action"] = "skip"
                    entry["reason"] = err or "local_accept_failed"
                    accepted_count -= 1
                    skipped_count += 1

    receipt: dict[str, Any] = {
        "ok": not local_errors,
        "command": "plan-accept-backfill",
        "worktree_path": str(repo),
        "accepted_count": accepted_count,
        "skipped_count": skipped_count,
        "tasks": entries,
        "events": ["plan_accept_backfill_evaluated"],
    }
    if local_errors:
        receipt["local_errors"] = local_errors

    if not args.emit_json:
        sys.stderr.write(
            f"plan-accept-backfill: accepted={accepted_count} skipped={skipped_count}\n"
        )
        for entry in entries:
            if entry.get("action") == "accept":
                sys.stderr.write(
                    f"  accept {entry['task_ref']}: {entry.get('next_command', '')}\n"
                )
            else:
                sys.stderr.write(
                    f"  skip   {entry['task_ref']}: {entry.get('reason', '')}\n"
                )

    _common.emit(receipt)
    return 0 if not local_errors else 2
