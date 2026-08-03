"""Read-only ``tasks`` subcommand (internal)."""

from __future__ import annotations

import argparse
import json
import os
import sys
import subprocess
from pathlib import Path
from typing import Any

import resolver
from receipts import ReceiptWarning, TaskEntry, TasksReceipt

from . import _common


DEFAULT_LIMIT = 50
FALLBACK_ACTIVE_STATUSES = _common.FALLBACK_ACTIVE_STATUSES
DEFAULT_HANDOFF_TIMEOUT = _common.DEFAULT_HANDOFF_TIMEOUT


def _classify_workspace_role(branch: str | None, derived_task_ref: str | None) -> str:
    """Mirror status._classify_workspace_role for the tasks receipt (internal)."""
    if branch in ("main", "master"):
        return "control_plane"
    if branch and derived_task_ref:
        return "implementation_plane"
    return "unknown"


def _plan_exists(task: dict[str, Any]) -> bool:
    task_plan_path = task.get("task_plan_path")
    target_worktree_path = task.get("target_worktree_path")
    if not isinstance(task_plan_path, str) or not isinstance(target_worktree_path, str):
        return False
    return (Path(target_worktree_path) / task_plan_path).is_file()


def _live_active_statuses() -> tuple[str, ...]:
    return _common.live_active_statuses()


def _legacy_fallback_rows(repo: Path) -> list[dict[str, Any]]:
    env = os.environ.copy()
    source_root = repo / "packages" / "mcp-workbay-handoff" / "src"
    if source_root.is_dir():
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = f"{source_root}{os.pathsep}{existing}" if existing else str(source_root)
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "workbay_handoff_mcp.plan_cli",
                "--workspace-root",
                str(repo),
                "list",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_HANDOFF_TIMEOUT,
            env=env,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"legacy enumeration failed: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("legacy enumeration timed out") from exc
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "legacy enumeration failed")

    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not stripped.startswith("=== ") or not stripped.endswith(" ==="):
            continue
        fields = {
            key: value
            for key, value in (
                token.split("=", 1)
                for token in stripped.removeprefix("=== ").removesuffix(" ===").split()
                if "=" in token
            )
        }
        task_ref = fields.get("task_ref")
        if not isinstance(task_ref, str) or not task_ref:
            continue
        branch = fields.get("branch")
        task_plan_path = fields.get("path")
        rows.append(
            {
                "task_ref": task_ref,
                "status": None,
                "target_branch": None if branch in (None, "<unset>") else branch,
                "target_worktree_path": None,
                "task_plan_path": None if task_plan_path in (None, "<unset>") else task_plan_path,
                "updated_at": None,
            }
        )
    return rows


def _should_try_legacy_fallback(proc: subprocess.CompletedProcess[str]) -> bool:
    stderr = proc.stderr.lower()
    return proc.returncode == 2 and "handoff-rows" in stderr


def _load_tasks_rows(repo: Path) -> tuple[list[dict[str, Any]], list[ReceiptWarning], bool, int]:
    warnings: list[ReceiptWarning] = []
    proc = _common.run_subprocess(
        _common.handoff_command_argv(repo, "handoff-rows", "--status", *_live_active_statuses()),
        timeout=DEFAULT_HANDOFF_TIMEOUT,
    )
    if proc.returncode == 124:
        return (
            [],
            [
                ReceiptWarning(
                    field="tasks",
                    reason="timeout",
                    exception_type="TimeoutExpired",
                )
            ],
            False,
            0,
        )
    if proc.returncode != 0:
        if _should_try_legacy_fallback(proc):
            try:
                rows = _legacy_fallback_rows(repo)
            except Exception as exc:
                return (
                    [],
                    [
                        ReceiptWarning(
                            field="tasks",
                            reason="unavailable",
                            exception_type=type(exc).__name__,
                        )
                    ],
                    False,
                    0,
                )
            warnings.extend(
                [
                    ReceiptWarning(
                        field="tasks_source",
                        reason="list_handoff_rows unavailable; using legacy enumeration",
                        exception_type="AttributeError",
                    ),
                    ReceiptWarning(
                        field="active_set_semantics",
                        reason="may include stale done rows",
                        exception_type=None,
                    ),
                ]
            )
            return rows, warnings, True, 0
        return ([], [ReceiptWarning(field="tasks", reason="unavailable")], False, 0)

    try:
        rows = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return (
            [],
            [ReceiptWarning(field="tasks", reason="malformed", exception_type="JSONDecodeError")],
            False,
            0,
        )

    if not isinstance(rows, list):
        return (
            [],
            [ReceiptWarning(field="tasks", reason="malformed", exception_type="UnexpectedPayload")],
            False,
            0,
        )
    return rows, warnings, True, 0


def _filter_done_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    filtered_rows: list[dict[str, Any]] = []
    stale_done_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = row.get("status")
        if status == "done":
            stale_done_count += 1
            continue
        filtered_rows.append(row)
    return filtered_rows, stale_done_count


def _emit_human(receipt: TasksReceipt) -> None:
    if not receipt.tasks:
        sys.stdout.write("no active tasks\n")
        return
    for task in receipt.tasks:
        sys.stdout.write(
            "\t".join(
                [
                    task.task_ref or "-",
                    task.status or "-",
                    task.target_branch or "-",
                    task.task_plan_path or "-",
                ]
            )
            + "\n"
        )


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle tasks", add_help=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args(argv)

    repo = resolver.repo_root()
    if repo is None:
        _common.emit(
            {
                "ok": False,
                "command": "tasks",
                "status": "error",
                "error": "not_in_git_repo",
            }
        )
        return 2

    worktree = resolver.current_worktree(repo) or repo
    if not (worktree / "Makefile").is_file():
        _common.emit(
            {
                "ok": False,
                "command": "tasks",
                "status": "error",
                "error": "missing_makefile",
            }
        )
        return 2

    limit = max(1, args.limit)
    cwd = Path.cwd()
    git_facts = _common.gather_git_facts(repo)
    rows, warnings, handoff_available, stale_done_count = _load_tasks_rows(repo)
    filtered_rows, filtered_done_count = _filter_done_rows(rows)
    stale_done_count += filtered_done_count
    truncated = len(filtered_rows) > limit
    visible_rows = filtered_rows[:limit]
    tasks = [
        TaskEntry(
            task_ref=row.get("task_ref") if isinstance(row.get("task_ref"), str) else None,
            status=row.get("status") if isinstance(row.get("status"), str) else None,
            target_branch=row.get("target_branch") if isinstance(row.get("target_branch"), str) else None,
            target_worktree_path=(
                row.get("target_worktree_path")
                if isinstance(row.get("target_worktree_path"), str)
                else None
            ),
            task_plan_path=row.get("task_plan_path") if isinstance(row.get("task_plan_path"), str) else None,
            task_plan_exists=_plan_exists(row),
            cwd_matches_target=(
                Path(row["target_worktree_path"]) == cwd
                if isinstance(row.get("target_worktree_path"), str)
                else False
            ),
            updated_at=row.get("updated_at") if isinstance(row.get("updated_at"), str) else None,
        )
        for row in visible_rows
        if isinstance(row, dict)
    ]
    receipt = TasksReceipt(
        ok=True,
        command="tasks",
        branch=git_facts.branch,
        worktree_path=str(worktree),
        head=git_facts.head,
        repo_root=str(repo),
        cwd=str(cwd),
        tasks=tasks,
        active_count=len(filtered_rows),
        stale_done_count=stale_done_count,
        handoff_available=handoff_available,
        truncated=truncated,
        limit=limit,
        warnings=warnings,
        workspace_role=_classify_workspace_role(git_facts.branch, git_facts.derived_task_ref),
    )
    if args.emit_json:
        _common.emit(receipt.to_dict())
    else:
        _emit_human(receipt)
    return 0