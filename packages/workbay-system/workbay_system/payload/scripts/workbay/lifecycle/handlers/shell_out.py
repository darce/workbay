"""Shell-out wrappers (implementation note).

``review-run`` / ``handoff-review-run`` shell out to ``mcp-workbay-handoff
review-runs --operation record --review-mode={branch,planning} ...``;
``handoff-close-check`` shells out to ``mcp-workbay-handoff
integrity-check --kind close``. All three pass ``--workspace-root`` so the
underlying CLI does not fall back to cwd, and the two review-run
wrappers synthesize the record-required identifiers
(``--review-run-id``, ``--session``, ``--subject-path``,
``--task-ref``) from the workspace state when the operator does not
override them on the command line. When the underlying CLI is
unavailable the receipt records ``delegated_exit_code: 127`` and
``ok: false``.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sqlite3
from pathlib import Path
from typing import Any

from . import _common

_LIVE_ACTIVE_STATUSES: tuple[str, ...] = ("in_progress", "review", "blocked")

DELEGATED_TO_TEMPLATES: dict[str, str] = {
    "review-run": "mcp-workbay-handoff review-runs record --review-mode=branch",
    "handoff-review-run": "mcp-workbay-handoff review-runs record --review-mode=planning",
    "handoff-close-check": "mcp-workbay-handoff integrity-check --kind close",
}

REVIEW_MODE_BY_COMMAND: dict[str, str] = {
    "review-run": "branch",
    "handoff-review-run": "planning",
}

SUBJECT_KIND_BY_COMMAND: dict[str, str] = {
    "review-run": "branch",
    "handoff-review-run": "task_plan",
}


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _summarize(stdout: str, limit: int = 200) -> str:
    return stdout if len(stdout) <= limit else stdout[:limit]


def _resolve_active_task_ref_via_cwd(workspace_root: Path) -> str | None:
    """Resolve the active task whose ``target_worktree_path`` matches the
    operator's cwd (or any ancestor of cwd) via the canonical handoff DB.

    Walks cwd up to filesystem root, returning the unique task_ref at the
    closest ancestor that has exactly one active row. ``None`` for any
    of: missing DB, no match anywhere on the parent chain, multiple
    matches at the same tier, or sqlite errors.

    Walking the parent chain (rather than only matching cwd exactly)
    keeps the wrapper deterministic when the operator runs from a
    subdirectory of the worktree (e.g. ``cd packages/foo && make
    handoff-close-check``); the previous exact-match form silently fell
    back to the workspace-singular ``CURRENT_TASK.json`` in that case,
    which can be stale relative to the worktree the subdir actually
    belongs to.
    """
    db_path = workspace_root / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return None
    try:
        cwd_resolved = Path.cwd().resolve()
    except (FileNotFoundError, OSError):
        return None
    candidates: list[Path] = [cwd_resolved, *cwd_resolved.parents]
    try:
        with sqlite3.connect(str(db_path)) as conn:
            placeholders = ",".join(["?"] * len(_LIVE_ACTIVE_STATUSES))
            for candidate in candidates:
                rows = conn.execute(
                    f"SELECT task_ref FROM handoff_state "
                    f"WHERE status IN ({placeholders}) "
                    f"AND target_worktree_path = ?",
                    (*_LIVE_ACTIVE_STATUSES, str(candidate)),
                ).fetchall()
                if len(rows) == 1:
                    task_ref = rows[0][0]
                    if isinstance(task_ref, str) and task_ref:
                        return task_ref
                    return None
                if len(rows) > 1:
                    # Ambiguous at this tier — bail rather than guess.
                    return None
    except sqlite3.Error:
        return None
    return None


def _read_active_task_ref(workspace_root: Path | None) -> str | None:
    """Resolve the active task_ref the wrapper should bind to.

    Prefers a cwd-keyed sqlite lookup against the canonical handoff DB
    (deterministic per worktree even when multiple tasks are
    in_progress); falls back to a derived workspace summary via
    ``render-handoff --no-write`` (internal) so historical
    callers without a seeded DB keep working. Returns ``None`` when
    neither source resolves a task — the wrapper then leans on the
    operator-supplied ``--task-ref`` (or the underlying CLI's own
    ambiguity guard).

    ``workspace_ambiguous`` from the derived view yields ``None`` — the
    operator's ``--task-ref`` flag is the disambiguation surface.
    """
    if workspace_root is None:
        return None
    sqlite_match = _resolve_active_task_ref_via_cwd(workspace_root)
    if sqlite_match:
        return sqlite_match
    view = _common.derive_workspace_summary_view(workspace_root)
    if view.shape != "single":
        return None
    return view.task_ref if view.task_ref else None


def _current_branch(workspace_root: Path | None) -> str | None:
    if workspace_root is None:
        return None
    proc = _common.run_subprocess(
        ["git", "-C", str(workspace_root), "rev-parse", "--abbrev-ref", "HEAD"]
    )
    if proc.returncode != 0:
        return None
    name = proc.stdout.strip()
    return name or None


def _build_review_run_argv(
    command: str, args: argparse.Namespace, workspace_root: Path
) -> list[str]:
    """Compose the review-runs record argv with derived defaults."""
    review_mode = REVIEW_MODE_BY_COMMAND[command]
    task_ref = args.task_ref or _read_active_task_ref(workspace_root) or ""

    if command == "handoff-review-run":
        subject_path = args.subject_path or args.doc or ""
    else:  # review-run (branch)
        subject_path = (
            args.subject_path or _current_branch(workspace_root) or "HEAD"
        )

    subject_kind = args.subject_kind or SUBJECT_KIND_BY_COMMAND[command]
    stamp = _utc_stamp()
    review_run_id = args.review_run_id or f"br-{review_mode}-{task_ref or 'unknown'}-{stamp}"
    session = args.session or f"sess-{review_mode}-{task_ref or 'unknown'}-{stamp}"

    # ``--workspace-root`` is registered on the parent parser before
    # ``add_subparsers`` on the real ``mcp-workbay-handoff`` adapter, so
    # it MUST precede the subcommand. Putting it after exits 2 with
    # ``unrecognized arguments``. Guarded by
    # ``test_review_run_workspace_root_precedes_subcommand``.
    argv: list[str] = [
        "--workspace-root", str(workspace_root),
        "review-runs",
        "--operation", "record",
        "--review-mode", review_mode,
        "--review-run-id", review_run_id,
        "--session", session,
        "--subject-path", subject_path,
        "--subject-kind", subject_kind,
    ]
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    if args.verdict:
        argv.extend(["--verdict", args.verdict])
    if args.verdict_decision:
        argv.extend(["--verdict-decision", args.verdict_decision])
    return argv


def _build_close_check_argv(
    args: argparse.Namespace, workspace_root: Path
) -> list[str]:
    # ``--workspace-root`` is a *parent* flag on the real adapter
    # (registered before ``add_subparsers``); positioning it after the
    # subcommand exits 2. Guarded by
    # ``test_handoff_close_check_workspace_root_precedes_subcommand``.
    argv: list[str] = [
        "--workspace-root",
        str(workspace_root),
        "integrity-check",
        "--kind",
        "close",
    ]
    task_ref = args.task_ref or _read_active_task_ref(workspace_root)
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    if args.enforce:
        argv.append("--enforce")
    if args.allow_no_active_task:
        argv.append("--allow-no-active-task")
    if args.require_fresh_tests:
        argv.append("--require-fresh-tests")
    if args.current_commit_sha:
        argv.extend(["--current-commit-sha", args.current_commit_sha])
    return argv


def _parse_review_run_args(command: str, argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(prog=f"lifecycle {command}", add_help=False)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument("--task-ref", dest="task_ref", default=None)
    parser.add_argument("--review-run-id", dest="review_run_id", default=None)
    parser.add_argument("--session", dest="session", default=None)
    parser.add_argument("--subject-path", dest="subject_path", default=None)
    parser.add_argument("--subject-kind", dest="subject_kind", default=None)
    parser.add_argument("--verdict", dest="verdict", default=None)
    parser.add_argument("--verdict-decision", dest="verdict_decision", default=None)
    if command == "handoff-review-run":
        parser.add_argument("--doc", dest="doc", default=None)
    else:
        parser.set_defaults(doc=None)
    return parser.parse_known_args(argv)


def _parse_close_check_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(prog="lifecycle handoff-close-check", add_help=False)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument("--task-ref", dest="task_ref", default=None)
    parser.add_argument("--enforce", action="store_true", default=False)
    parser.add_argument("--allow-no-active-task", dest="allow_no_active_task", action="store_true", default=False)
    parser.add_argument("--require-fresh-tests", dest="require_fresh_tests", action="store_true", default=False)
    parser.add_argument("--current-commit-sha", dest="current_commit_sha", default=None)
    return parser.parse_known_args(argv)


def run(command: str, argv: list[str]) -> int:
    workspace_root = _common.repo_root()
    if workspace_root is None:
        receipt = {
            "ok": False,
            "command": command,
            "delegation_mode": "shell_out",
            "delegated_to": DELEGATED_TO_TEMPLATES[command],
            "delegated_exit_code": None,
            "events": [],
            "error": "not_in_git_repo",
        }
        _common.emit(receipt)
        return 2

    if command == "handoff-close-check":
        args, passthrough = _parse_close_check_args(argv)
        base_argv = _build_close_check_argv(args, workspace_root)
    else:
        args, passthrough = _parse_review_run_args(command, argv)
        base_argv = _build_review_run_argv(command, args, workspace_root)

    full_cmd = [_common.mcp_handoff_bin(), *base_argv, *passthrough]
    proc = _common.run_subprocess(full_cmd)

    receipt: dict[str, Any] = {
        "ok": proc.returncode == 0,
        "command": command,
        "delegation_mode": "shell_out",
        "delegated_to": DELEGATED_TO_TEMPLATES[command],
        "delegated_exit_code": proc.returncode,
        "delegated_stdout_summary": _summarize(proc.stdout),
        "events": [f"{command}_delegated"],
    }

    _common.emit(receipt)
    return 0 if proc.returncode == 0 else proc.returncode
