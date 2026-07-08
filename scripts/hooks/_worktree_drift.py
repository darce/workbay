#!/usr/bin/env python3
"""PreToolUse drift guard: block wrong-worktree edits by default."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from _active_task_context import (
    ActiveTaskContext,
    _canonical_target_worktree,
    _load_active_task,
    _primary_workspace_root,
    _workspace_root,
)
from _harness_protocol import (
    HarnessContractMissingError,
    HarnessContractMissingPolicy,
    find_permitted_main_surface,
    handle_missing_contract,
    load_branch_isolation_policy,
)


MAIN_BRANCHES = frozenset({"main", "master"})


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
PACKAGE_ROOT = PACKAGE_SRC / "workbay_handoff_mcp"
TRACE_LOG_NAME = "branch_isolation_guard.jsonl"

if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))


@dataclass(frozen=True)
class DriftDecision:
    outcome: str
    reason: str | None = None
    primary_worktree: str | None = None
    path: str | None = None
    candidate_worktree: str | None = None
    target_worktree: str | None = None
    task_ref: str | None = None
    repo_relative_path: str | None = None
    matched_pattern: str | None = None
    matched_reason: str | None = None


def _payload_value(payload: dict[str, Any], snake_key: str, camel_key: str, default: Any = "") -> Any:
    if snake_key in payload and payload[snake_key] not in (None, ""):
        return payload[snake_key]
    if camel_key in payload and payload[camel_key] not in (None, ""):
        return payload[camel_key]
    return default


def _extract_candidate_paths(
    tool_name: str,
    tool_input: dict[str, Any],
    workspace_root: Path | None = None,
) -> list[str]:
    if not tool_name:
        file_path = _payload_value(tool_input, "file_path", "filePath")
        return [str(file_path)] if isinstance(file_path, str) and file_path.strip() else []

    if tool_name in {
        "Edit",
        "Write",
        "create_file",
        "replace_string_in_file",
        "multi_replace_string_in_file",
    }:
        file_path = _payload_value(tool_input, "file_path", "filePath")
        return [str(file_path)] if isinstance(file_path, str) and file_path.strip() else []

    if tool_name == "apply_patch":
        patch_input = tool_input.get("input")
        if not isinstance(patch_input, str) or not patch_input.strip():
            return []
        paths: list[str] = []
        for line in patch_input.splitlines():
            if not line.startswith("*** ") or " File: " not in line:
                continue
            _, raw_path = line.split(" File: ", 1)
            parsed_path = raw_path.split(" -> ", 1)[0].strip()
            if parsed_path:
                paths.append(parsed_path)
        return paths

    # FU-02: Bash-dispatched mutations must flow through the drift check too,
    # otherwise formatters / write-verbs / git restore silently write to
    # whichever worktree the shell is cwd'd in — the exact recurrence that
    # produced the 2026-04-18 "formatter drift" stash after internal shipped.
    if tool_name == "Bash":
        return _extract_bash_candidate_paths(tool_input, workspace_root=workspace_root)

    return []


def _extract_bash_candidate_paths(
    tool_input: dict[str, Any], *, workspace_root: Path | None = None
) -> list[str]:
    command = _payload_value(tool_input, "command", "command")
    if not isinstance(command, str) or not command.strip():
        return []
    try:
        from _bash_isolation_guard import extract_raw_write_targets, scan_bash_command
        from _harness_protocol import HarnessContractMissingError, load_branch_isolation_policy
    except ImportError:
        return []
    workspace = workspace_root if workspace_root is not None else _workspace_root()
    try:
        policy = load_branch_isolation_policy(workspace)
    except HarnessContractMissingError:
        return []
    blocked = scan_bash_command(command, workspace, policy)
    paths: list[str] = []
    formatter_detected = False
    for entry in blocked:
        if entry.endswith("(formatter)"):
            formatter_detected = True
            continue
        paths.append(entry)

    # BR-01: scan_bash_command drops paths resolving *outside* `workspace`
    # (via _to_repo_relative). That leaves a cross-worktree bleed open for
    # both absolute paths (`sed -i /<primary>/packages/foo.py`) and relative
    # paths that escape the workspace via `..` (`sed -i ../context-alt-...`).
    # Pass both shapes through to the drift comparison so
    # _candidate_worktree_root can resolve their hosting worktree and reject
    # the edit when it diverges from the active task's target_worktree.
    workspace_resolved = workspace.resolve(strict=False)
    seen: set[str] = set(paths)
    for raw in extract_raw_write_targets(command):
        if not raw or not isinstance(raw, str):
            continue
        raw_path = Path(raw).expanduser()
        if raw_path.is_absolute():
            candidate = raw_path
        else:
            candidate = (workspace_resolved / raw_path)
        try:
            resolved = candidate.resolve(strict=False)
        except OSError:
            resolved = candidate
        try:
            resolved.relative_to(workspace_resolved)
            inside_workspace = True
        except ValueError:
            inside_workspace = False
        if inside_workspace:
            continue  # already handled by scan_bash_command's relative branch
        token = str(resolved)
        if token in seen:
            continue
        seen.add(token)
        paths.append(token)

    if formatter_detected:
        # Formatter invocations implicitly write across the cwd's worktree; use
        # the resolved workspace root so _candidate_worktree_root reports the
        # hosting worktree for the drift comparison.
        paths.append(str(workspace))
    return paths


def _detect_current_branch(workspace: Path) -> str:
    """Return the current branch name, or empty string on failure."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(workspace), "branch", "--show-current"],
            capture_output=True, text=True, timeout=3, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _candidate_abspath(raw_path: str, workspace_root: Path) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = workspace_root / candidate
    return candidate.resolve(strict=False)


def _candidate_worktree_root(candidate_path: Path) -> str | None:
    probe_dir = candidate_path if candidate_path.is_dir() else candidate_path.parent
    try:
        proc = subprocess.run(
            ["git", "-C", str(probe_dir), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return str(Path(proc.stdout.strip()).resolve(strict=False))


def _repo_relative_path(path: Path, repo_root: str) -> str | None:
    try:
        return str(path.resolve(strict=False).relative_to(Path(repo_root).resolve(strict=False))).replace("\\", "/")
    except ValueError:
        return None


def _log_trace(decision: DriftDecision) -> None:
    if not decision.primary_worktree:
        return
    try:
        state_dir = Path(decision.primary_worktree) / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "tool": "guard-worktree-drift",
            "outcome": decision.outcome,
            "task_ref": decision.task_ref,
            "path": decision.path,
            "candidate_worktree": decision.candidate_worktree,
            "target_worktree": decision.target_worktree,
            "repo_relative_path": decision.repo_relative_path,
            "matched_pattern": decision.matched_pattern,
            "matched_reason": decision.matched_reason,
            "reason": decision.reason,
        }
        with (state_dir / TRACE_LOG_NAME).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    except OSError:
        pass


def evaluate_payload(
    payload: dict[str, Any],
    *,
    workspace_root: Path | None = None,
    active_task: ActiveTaskContext | tuple[str | None, str | None] | tuple[str | None, str | None, str | None] | None = None,
) -> DriftDecision | None:
    root = workspace_root or _workspace_root()
    tool_name = _payload_value(payload, "tool_name", "toolName")
    tool_input = _payload_value(payload, "tool_input", "toolInput", {})
    if not isinstance(tool_name, str) or not isinstance(tool_input, dict):
        return None

    candidate_paths = _extract_candidate_paths(tool_name, tool_input, workspace_root=root)
    if not candidate_paths:
        return None

    if isinstance(active_task, ActiveTaskContext):
        context = active_task
    elif isinstance(active_task, tuple):
        primary_worktree = _primary_workspace_root(root)
        if len(active_task) == 2:
            context = ActiveTaskContext(active_task[0], active_task[1], None, primary_worktree)
        else:
            context = ActiveTaskContext(active_task[0], active_task[1], active_task[2], primary_worktree)
    else:
        try:
            context = _load_active_task(root)
        except ValueError as exc:
            return DriftDecision(
                outcome="block",
                reason=f"UnresolvedTaskContextError: {exc}",
                primary_worktree=_primary_workspace_root(root),
                task_ref="(unresolved task context)",
            )

    if not context.target_worktree:
        return None

    target_worktree = _canonical_target_worktree(context.target_worktree)
    if not target_worktree:
        return None
    primary_worktree = _canonical_target_worktree(context.primary_worktree) or _primary_workspace_root(root)
    if context.task_ref and context.task_ref.startswith("MAINT-"):
        return DriftDecision(
            outcome="maintenance_bypass",
            reason="MAINT task bypass",
            primary_worktree=primary_worktree,
            task_ref=context.task_ref,
            target_worktree=target_worktree,
        )
    # BR-18: main-branch targeting tasks (non-MAINT) still enforce drift. The
    # previous blanket bypass `if target_branch in MAIN_BRANCHES: return None`
    # silently disabled drift detection for any main-targeting task, creating a
    # blind spot for internal autonomous loops. MAINT tasks already bypass above;
    # non-MAINT main tasks must edit from the root/primary worktree.
    if os.environ.get("ALT_ALLOW_WORKTREE_DRIFT") == "1":
        return DriftDecision(
            outcome="env_bypass",
            reason="ALT_ALLOW_WORKTREE_DRIFT=1",
            primary_worktree=primary_worktree,
            task_ref=context.task_ref,
            target_worktree=target_worktree,
        )

    # Root-worktree-on-non-main guard: if the agent is editing inside the
    # root (primary) worktree while on a feature branch, block the edit.
    # The root worktree must stay on main/master; feature work belongs in
    # linked worktrees.
    root_resolved = str(root.resolve(strict=False))
    if root_resolved == primary_worktree:
        actual_branch = _detect_current_branch(root)
        if actual_branch and actual_branch not in MAIN_BRANCHES:
            return DriftDecision(
                outcome="block",
                reason=(
                    "RootWorktreeNotOnMainError: the root worktree is checked "
                    f"out on branch '{actual_branch}', not main.\n\n"
                    "The root worktree must stay on main. Use a linked worktree "
                    "for feature branches:\n"
                    "  git checkout main\n"
                    f"  git worktree add ../<repo>-<task-id> -b {actual_branch}\n\n"
                    "Escape hatch: ALT_ALLOW_WORKTREE_DRIFT=1"
                ),
                primary_worktree=primary_worktree,
                task_ref=context.task_ref or "(unknown task)",
                target_worktree=target_worktree,
                path=candidate_paths[0] if candidate_paths else None,
            )

    allowlisted_decisions: list[DriftDecision] = []
    policy = None
    for raw_path in candidate_paths:
        candidate_path = _candidate_abspath(raw_path, root)
        candidate_worktree = _candidate_worktree_root(candidate_path)
        if not candidate_worktree or candidate_worktree == target_worktree:
            continue

        repo_relative = _repo_relative_path(candidate_path, primary_worktree)
        if candidate_worktree == primary_worktree and repo_relative:
            try:
                if policy is None:
                    policy = load_branch_isolation_policy(Path(primary_worktree))
            except HarnessContractMissingError as exc:
                # internal: a fresh ``--profile minimal`` consumer
                # install legitimately ships without the contract overlay.
                # The drift guard is a background detector with a
                # non-blocking fallback path, so route the missing-contract
                # case through HarnessContractMissingPolicy.WARN — emit a
                # structured stderr warning and skip the allow-list check
                # rather than hard-blocking the edit.
                handle_missing_contract(
                    exc, policy=HarnessContractMissingPolicy.WARN
                )
                return None
            matched_surface = find_permitted_main_surface(repo_relative, policy)
            if matched_surface is not None:
                allowlisted_decisions.append(
                    DriftDecision(
                    outcome="allowlisted_main_surface",
                    reason=f"allow-listed main surface: {matched_surface.reason}",
                    primary_worktree=primary_worktree,
                    path=str(candidate_path),
                    candidate_worktree=candidate_worktree,
                    target_worktree=target_worktree,
                    task_ref=context.task_ref or "(unknown task)",
                    repo_relative_path=repo_relative,
                    matched_pattern=matched_surface.pattern,
                    matched_reason=matched_surface.reason,
                )
                )
                continue

        return DriftDecision(
            outcome="block",
            primary_worktree=primary_worktree,
            path=str(candidate_path),
            candidate_worktree=candidate_worktree,
            target_worktree=target_worktree,
            task_ref=context.task_ref or "(unknown task)",
            repo_relative_path=repo_relative,
        )
    if allowlisted_decisions:
        return allowlisted_decisions[0]
    return None


def _build_block_reason(decision: DriftDecision) -> str:
    assert decision.path is not None
    assert decision.candidate_worktree is not None
    assert decision.target_worktree is not None
    return (
        "WorkspaceRootDriftError: edit resolved into the wrong worktree.\n\n"
        f"Task: {decision.task_ref}\n"
        f"Edit path: {decision.path}\n"
        f"Edit worktree: {decision.candidate_worktree}\n"
        f"Target worktree: {decision.target_worktree}\n\n"
        "Escape hatches:\n"
        "  1. Use a `MAINT-*` task ref for intentional main-worktree maintenance edits.\n"
        "  2. Set `ALT_ALLOW_WORKTREE_DRIFT=1` for a shell-scoped warn-only bypass.\n"
        "  3. Add the path to `branch_isolation.permitted_main_surfaces` in harness-protocol.yaml."
    )


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(payload, dict):
        return 0

    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(payload, expected="PreToolUse")
    except ImportError:
        pass

    decision = evaluate_payload(payload)
    if decision is None:
        return 0
    if decision.outcome != "silent":
        _log_trace(decision)
    if decision.outcome != "block":
        return 0

    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "block",
                    "permissionDecisionReason": decision.reason or _build_block_reason(decision),
                }
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
