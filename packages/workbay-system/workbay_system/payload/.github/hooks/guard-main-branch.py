#!/usr/bin/env python3
"""PreToolUse hook: block protected edits on main/master in the VS Code harness."""
from __future__ import annotations

import datetime
import json
import subprocess
import sys
from pathlib import Path

HELPER_DIR = Path(__file__).resolve().parents[2] / "scripts" / "hooks"
if str(HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(HELPER_DIR))

from _harness_protocol import (  # noqa: E402
    HarnessContractMissingError,
    find_permitted_main_surface,
    load_branch_isolation_policy,
)
from _branch_isolation_guard import (  # noqa: E402
    build_branch_naming_block_reason as _build_branch_naming_block_reason,
    check_branch_naming as _check_branch_naming,
    check_file_edit as _check_file_edit,
    extract_candidate_paths as _extract_candidate_paths,
    find_dirty_protected_paths as _check_dirty_protected_paths,
    resolve_path_branch as _resolve_path_branch,
    to_repo_relative as _to_repo_relative,
)
from _interp import resolve_env_alias  # noqa: E402


_PROTECTED_BRANCHES = {"main", "master"}


def _run_git(*args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _repo_root() -> str:
    return _run_git("rev-parse", "--show-toplevel")


def _current_branch() -> str:
    return _run_git("branch", "--show-current")


def _build_reason(branch: str, blocked_paths: list[str]) -> str:
    rendered_paths = "\n".join(f"  - {path}" for path in blocked_paths)
    return (
        "BLOCKED: Protected edits are not allowed on the main branch.\n\n"
        f"Branch: {branch}\n"
        "Files:\n"
        f"{rendered_paths}\n\n"
        "Create a feature branch first:\n"
        "  git checkout -b feature/<task-id>-<slug>\n\n"
        "If you already have dirty code changes on main, move them to a feature branch or stash them before continuing.\n\n"
        "Isolation options:\n"
        "  1. Feature branch for single-agent work\n"
        "  2. Worktree isolation for delegated subtasks\n"
        "  3. Lane orchestration for multi-agent parallel work\n\n"
        "Only explicitly permitted operator docs/config surfaces remain allowed on main.\n"
        "Planning docs and implementation files now require a feature branch from the first edit.\n"
        "See: docs/workbay/rules/development-workflow.md#branch-isolation-protocol-mandatory"
    )


def _build_dirty_reason(branch: str, dirty_paths: list[str]) -> str:
    rendered_paths = "\n".join(f"  - {path}" for path in dirty_paths)
    return (
        "BLOCKED: Protected code files are already dirty on the main branch.\n\n"
        f"Branch: {branch}\n"
        "Dirty files:\n"
        f"{rendered_paths}\n\n"
        "Move the work onto a feature branch or stash it before making more edits.\n\n"
        "Recommended recovery:\n"
        "  1. git checkout -b feature/<task-id>-<slug>\n"
        "  2. keep the dirty changes on that branch, or stash them intentionally\n"
        "  3. return to main only after the protected paths are clean again\n\n"
        "See: docs/workbay/rules/development-workflow.md#branch-isolation-protocol-mandatory"
    )


def _permitted_candidate_paths(tool_name: str, tool_input: dict, repo_root: str, policy) -> list[str]:
    candidate_paths = [
        _to_repo_relative(raw_path, repo_root)
        for raw_path in _extract_candidate_paths(tool_name, tool_input)
    ]
    normalized = [path for path in candidate_paths if path]
    if not normalized:
        return []
    if not all(find_permitted_main_surface(path, policy) is not None for path in normalized):
        return []
    return normalized


def _log_telemetry(tool_name: str, blocked_paths: list[str], branch: str, *, outcome: str) -> None:
    try:
        state_dir = Path(".task-state")
        state_dir.mkdir(exist_ok=True)
        record = {
            "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            "tool": tool_name,
            "branch": branch,
            "outcome": outcome,
            "paths": blocked_paths,
        }
        with (state_dir / "branch_isolation_guard.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    except OSError:
        pass


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        sys.exit(0)

    tool_name = data.get("toolName") or data.get("tool_name") or ""
    tool_input = data.get("toolInput") or data.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        sys.exit(0)

    branch = _current_branch()
    repo_root = _repo_root()
    workspace_root = Path(repo_root or Path.cwd())
    try:
        policy = load_branch_isolation_policy(workspace_root)
    except HarnessContractMissingError as exc:
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "block",
                        "permissionDecisionReason": str(exc),
                    }
                }
            )
        )
        sys.exit(0)

    # Branch-naming gate (internal). Fires before the
    # protected-path check so non-conforming branches reject
    # uniformly. ``WORKBAY_ALLOW_NONCONFORMING_BRANCH=1`` is the
    # documented escape valve; pre-commit / pre-push gates honour
    # their own env vars per implementation note §4 / §4b.
    non_conforming = _check_branch_naming(branch)
    if non_conforming is not None and resolve_env_alias("WORKBAY_ALLOW_NONCONFORMING_BRANCH") != "1":
        _log_telemetry(tool_name, [], non_conforming, outcome="non_conforming_branch")
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "block",
                        "permissionDecisionReason": _build_branch_naming_block_reason(non_conforming),
                    }
                }
            )
        )
        sys.exit(0)

    result = _check_file_edit(
        tool_name,
        tool_input,
        branch=branch,
        repo_root=repo_root,
        policy=policy,
        protected_branches=_PROTECTED_BRANCHES,
    )
    if result is not None:
        resolved_branch, blocked_paths = result
        _log_telemetry(tool_name, blocked_paths, resolved_branch, outcome="attempted_protected_edit")
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "block",
                        "permissionDecisionReason": _build_reason(resolved_branch, blocked_paths),
                    }
                }
            )
        )
        sys.exit(0)

    dirty_result = _check_dirty_protected_paths(
        branch=branch,
        repo_root=repo_root,
        policy=policy,
        protected_branches=_PROTECTED_BRANCHES,
    )
    if dirty_result is None:
        sys.exit(0)

    resolved_branch, dirty_paths = dirty_result

    # Per-path worktree resolution: if every candidate edit path resolves
    # (via its own worktree) to a non-protected branch, the edit isn't a
    # main-branch write at all and the dirty-paths-on-main check shouldn't
    # apply. This unblocks edits to sibling worktrees while the harness cwd
    # remains on main.
    raw_paths = [p for p in _extract_candidate_paths(tool_name, tool_input) if p]
    if raw_paths:
        resolved = [(_resolve_path_branch(p) or branch) for p in raw_paths]
        if all(b not in _PROTECTED_BRANCHES for b in resolved):
            sys.exit(0)

    candidate_paths = _permitted_candidate_paths(tool_name, tool_input, repo_root, policy)
    if candidate_paths and not any(path in set(dirty_paths) for path in candidate_paths):
        sys.exit(0)

    _log_telemetry(tool_name, dirty_paths, resolved_branch, outcome="dirty_protected_main_paths")
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "block",
                    "permissionDecisionReason": _build_dirty_reason(resolved_branch, dirty_paths),
                }
            }
        )
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
