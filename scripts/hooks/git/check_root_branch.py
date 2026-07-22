#!/usr/bin/env python3
"""Pre-commit gate: refuse commits in the PRIMARY worktree on non-main branches.

Defense-in-depth complement to guard-bash-main-branch (A1): catches raw-terminal
and non-Bash paths that can switch root off main and then commit.

Scoped to multi-worktree workflows only (linked worktrees present). Single-worktree
consumers and commits inside LINKED worktrees are unaffected.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_PROTECTED_BRANCHES = frozenset({"main", "master"})


def _repo_toplevel(cwd: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(cwd), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return proc.stdout.strip()


def _current_branch(cwd: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(cwd), "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _build_block_message(branch: str) -> str:
    return (
        "BLOCKED: refusing to commit in the PRIMARY (root) worktree on non-main "
        f"branch '{branch}'.\n\n"
        "The root worktree must stay on main so a concurrent session's "
        "main-integration commit never lands on a feature branch. Use a LINKED "
        "worktree for feature work:\n"
        '  make task-start TASK=<task-ref> OBJECTIVE="..."\n'
        "  # or, ad hoc:\n"
        "  git worktree add ../<repo>-<task-id> -b <branch>\n\n"
        "To bypass this check intentionally:\n"
        "  git commit --no-verify\n\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory"
    )


def main() -> int:
    hooks_git_dir = Path(__file__).resolve().parent
    hooks_dir = hooks_git_dir.parent
    if str(hooks_dir) not in sys.path:
        sys.path.insert(0, str(hooks_dir))

    try:
        from _worktree_identity import has_linked_worktrees, primary_workspace_root
    except ImportError as exc:
        print(f"check_root_branch: import failed — {exc}", file=sys.stderr)
        return 0  # fail-open: do not block commits when helper unavailable

    cwd = Path.cwd()
    toplevel = _repo_toplevel(cwd)
    if not toplevel:
        return 0

    branch = _current_branch(cwd)
    if not branch or branch in _PROTECTED_BRANCHES:
        return 0

    try:
        primary = str(Path(primary_workspace_root(cwd)).resolve(strict=False))
        committing = str(Path(toplevel).resolve(strict=False))
    except Exception:  # noqa: BLE001 — fail-open parity with A1's broad guard
        return 0

    if committing != primary:
        return 0
    if not has_linked_worktrees(primary):
        return 0

    print(_build_block_message(branch), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
