#!/usr/bin/env python3
"""Advisory hook: nudge the model to cd into the active task's worktree.

Fires on ``SessionStart`` and ``UserPromptSubmit``. When the agent's cwd
diverges from the active task's ``target_worktree_path``, emits a
non-blocking ``hookSpecificOutput.additionalContext`` instructing the
model to run ``cd <target>`` before any further tool calls. The
directive lands ahead of cwd-resolving MCP reads (`load_session`,
`search_handoff`, `review_findings`) so the right active task resolves
from the start, eliminating the ``context_drift`` warning chain.

This hook is advisory; it never blocks. The PreToolUse blocker in
``_worktree_drift.py`` remains the enforcement layer. MAINT-* tasks,
no-active-task, ambiguous-task, and missing-target-worktree all stay
silent (no stdout payload).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from _active_task_context import (
    _canonical_target_worktree,
    _load_active_task,
    _workspace_root,
)


_VALID_EVENT_NAMES = ("SessionStart", "UserPromptSubmit")


def _payload_event_name(payload: dict[str, Any]) -> str | None:
    name = payload.get("hook_event_name") or payload.get("hookEventName")
    if isinstance(name, str) and name in _VALID_EVENT_NAMES:
        return name
    return None


def _payload_cwd(payload: dict[str, Any]) -> str | None:
    cwd = payload.get("cwd")
    if isinstance(cwd, str) and cwd:
        return cwd
    return None


def _cwd_worktree(cwd: str | None) -> str | None:
    """Return the canonical git worktree root for ``cwd`` if discoverable."""
    base = Path(cwd) if cwd else Path.cwd()
    try:
        proc = subprocess.run(
            ["git", "-C", str(base), "rev-parse", "--show-toplevel"],
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


def _build_directive(event_name: str, task_ref: str, target: str, cwd: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": event_name,
            "additionalContext": (
                f"Active task {task_ref} targets {target}; current cwd is {cwd}. "
                f"cd {target} before any further tool calls."
            ),
        }
    }


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(payload, dict):
        return 0

    event_name = _payload_event_name(payload)
    if event_name is None:
        # Unrecognized event shape — emit the wire-shape warning and exit.
        try:
            from _protocol import validate_event  # type: ignore[import-not-found]

            validate_event(payload, expected="SessionStart")
        except ImportError:
            pass
        return 0

    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(payload, expected=event_name)
    except ImportError:
        pass

    workspace = _workspace_root()
    try:
        context = _load_active_task(workspace)
    except Exception:
        # UnresolvedTaskContextError, MCP read errors, etc. → stay silent.
        return 0

    if context.task_ref and context.task_ref.startswith("MAINT-"):
        return 0

    if not context.target_worktree:
        return 0

    target = _canonical_target_worktree(context.target_worktree)
    if not target:
        return 0

    payload_cwd = _payload_cwd(payload)
    cwd_worktree = _cwd_worktree(payload_cwd)
    if cwd_worktree == target:
        return 0

    cwd_display = payload_cwd or str(Path.cwd())
    directive = _build_directive(
        event_name,
        context.task_ref or "(active task)",
        target,
        cwd_display,
    )
    print(json.dumps(directive))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
