#!/usr/bin/env python3
"""Shared active-task resolver for branch-isolation and advisory hooks.

Lifted from `_worktree_drift.py` so multiple hooks (PreToolUse drift
guard, SessionStart / UserPromptSubmit advisory) can resolve the active
task identity from the same source of truth without duplicating the
`workbay_handoff_mcp` import dance and the canonicalization logic.
"""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _resolve_package_src() -> Path:
    base = Path(__file__).resolve()
    candidates = (
        base.parents[2] / "packages" / "mcp-workbay-handoff" / "src",
        base.parents[3] / "mcp-workbay-handoff" / "src",
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


PACKAGE_SRC = _resolve_package_src()

if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))


@dataclass(frozen=True)
class ActiveTaskContext:
    task_ref: str | None
    target_worktree: str | None
    target_branch: str | None
    primary_worktree: str
    # implementation note D3: carried so advisory surfaces can detect plan-bound drift
    # (task_plan_path is a docs/plans/<NNNN>-*.md but the branch/worktree lack
    # the matching -plan<NNNN> suffix). Present in the same identity envelope
    # already fetched, so reading it needs no extra MCP round-trip. Defaulted so
    # the partial/empty constructions below stay 4-positional.
    task_plan_path: str | None = None


def _load_handoff_exports() -> tuple[Any, Any, Any, Any] | None:
    try:
        module = importlib.import_module("workbay_handoff_mcp")
    except ImportError:
        return None
    try:
        return (
            getattr(module, "RuntimeConfig"),
            getattr(module, "configure_runtime"),
            getattr(module, "get_handoff_state"),
            getattr(module, "UnresolvedTaskContextError", ValueError),
        )
    except AttributeError:
        return None


def _primary_workspace_root(workspace_root: Path) -> str:
    from _worktree_identity import primary_workspace_root

    return primary_workspace_root(workspace_root)


def _load_active_task(workspace_root: Path) -> ActiveTaskContext:
    exports = _load_handoff_exports()
    if exports is None:
        primary = _primary_workspace_root(workspace_root)
        return ActiveTaskContext(None, None, None, primary)
    RuntimeConfig, configure_runtime, get_handoff_state, unresolved_task_context_error = exports

    try:
        runtime = RuntimeConfig.for_repo(workspace_root)
        configure_runtime(runtime)
        raw = get_handoff_state(sections="identity")
    except Exception as exc:
        if isinstance(exc, unresolved_task_context_error):
            raise
        primary = _primary_workspace_root(workspace_root)
        return ActiveTaskContext(None, None, None, primary)

    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return ActiveTaskContext(None, None, None, str(Path(runtime.workspace_root).resolve(strict=False)))

    if isinstance(parsed, dict) and parsed.get("ok") is False:
        error = parsed.get("error")
        if not isinstance(error, str):
            data = parsed.get("data")
            if isinstance(data, dict) and isinstance(data.get("error"), str):
                error = data.get("error")
        if isinstance(error, str) and (
            "Ambiguous active task" in error or "No active task in handoff_state" in error
        ):
            raise ValueError(error)

    data = parsed.get("data") if isinstance(parsed, dict) else None
    active = data.get("active") if isinstance(data, dict) else None
    if not isinstance(active, dict):
        return ActiveTaskContext(None, None, None, str(Path(runtime.workspace_root).resolve(strict=False)))

    task_ref = active.get("task_ref")
    target_worktree_path = active.get("target_worktree_path")
    target_branch = active.get("target_branch")
    task_plan_path = active.get("task_plan_path")
    return ActiveTaskContext(
        str(task_ref) if isinstance(task_ref, str) and task_ref else None,
        str(target_worktree_path) if isinstance(target_worktree_path, str) and target_worktree_path else None,
        str(target_branch) if isinstance(target_branch, str) and target_branch else None,
        str(runtime.workspace_root),
        str(task_plan_path) if isinstance(task_plan_path, str) and task_plan_path else None,
    )


def _workspace_root() -> Path:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return Path.cwd()
    if proc.returncode == 0 and proc.stdout.strip():
        return Path(proc.stdout.strip())
    return Path.cwd()


def _canonical_target_worktree(target_worktree_path: str | None) -> str | None:
    if not target_worktree_path:
        return None
    return str(Path(target_worktree_path).expanduser().resolve(strict=False))
