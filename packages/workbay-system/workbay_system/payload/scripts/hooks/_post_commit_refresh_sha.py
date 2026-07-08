#!/usr/bin/env python3
"""Best-effort refresh of the active task's commit_sha after a git commit.

Called by scripts/hooks/git/post-commit. Resolves the active handoff task
from the current workspace and stamps HEAD's SHA onto it via a no-op
set_handoff_state update (actor.commit_sha only). All failures are
swallowed: a git hook must never break a successful commit.

Motivation: handoff_close_check and the SHA-provenance discipline both
expect the active task's commit_sha to track HEAD. Agents forget to do
this manually after each commit; a post-commit hook closes that gap.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str], cwd: Path) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5, check=False, cwd=cwd)
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _repo_root() -> Path:
    resolved = _run(["git", "rev-parse", "--show-toplevel"], Path.cwd())
    return Path(resolved) if resolved else Path.cwd()


def main() -> int:
    repo_root = _repo_root()
    head_sha = _run(["git", "rev-parse", "HEAD"], repo_root)
    branch = _run(["git", "branch", "--show-current"], repo_root)
    if not head_sha or not branch:
        return 0
    if branch in {"main", "master"}:
        # Skip on main: ad-hoc MAINT tasks often share main; no single active
        # task to stamp and set_handoff_state resolution would be ambiguous.
        return 0

    try:
        from workbay_handoff_mcp import (
            RuntimeConfig,
            configure_runtime,
            get_handoff_state,
            set_handoff_state,
        )
    except ImportError:
        return 0

    try:
        configure_runtime(RuntimeConfig.for_repo(repo_root))
    except Exception:
        return 0

    try:
        state = get_handoff_state(sections="identity")
    except Exception:
        return 0

    active = (state or {}).get("data", {}).get("active") if isinstance(state, dict) else None
    task_ref = active.get("task_ref") if isinstance(active, dict) else None
    revision = active.get("revision") if isinstance(active, dict) else None
    if not task_ref or revision is None:
        return 0

    try:
        set_handoff_state(
            task_ref=task_ref,
            expected_revision=revision,
            actor={"commit_sha": head_sha, "branch": branch},
        )
    except Exception:
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
