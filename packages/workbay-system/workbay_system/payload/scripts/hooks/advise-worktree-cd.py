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
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from _active_task_context import (
    ActiveTaskContext,
    _canonical_target_worktree,
    _load_active_task,
    _workspace_root,
)


_VALID_EVENT_NAMES = ("SessionStart", "UserPromptSubmit")

# implementation note D3: a numbered plan doc ``docs/plans/<NNNN>-<slug>.md`` -> ``<NNNN>``.
_PLAN_DOC_ID_RE = re.compile(r"(?:^|/)(\d{4})-[^/]+\.md$")


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


def _cd_nudge_message(task_ref: str, target: str, cwd: str) -> str:
    return (
        f"Active task {task_ref} targets {target}; current cwd is {cwd}. "
        f"cd {target} before any further tool calls."
    )


def _plan_suffix_drift(context: ActiveTaskContext) -> str | None:
    """Return the expected ``<NNNN>`` plan id when the active task is plan-bound
    (``task_plan_path`` is a ``docs/plans/<NNNN>-*.md``) but its ``target_branch``
    lacks the matching ``-plan<NNNN>`` suffix, else ``None``.

    implementation note D3: this is warn-only signal. The hook never renames — mid-session
    mutation can strand a half-renamed worktree, break a running MCP server's
    cwd, or race a concurrent session — so it only surfaces the relink command.
    """
    plan_path = context.task_plan_path
    if not plan_path:
        return None
    match = _PLAN_DOC_ID_RE.search(plan_path)
    if match is None:
        return None
    plan_id = match.group(1)
    branch = context.target_branch or ""
    if branch.endswith(f"-plan{plan_id}"):
        return None
    return plan_id


def _plan_drift_message(task_ref: str, plan_path: str, branch: str, plan_id: str) -> str:
    return (
        f"Active task {task_ref} is plan-bound ({plan_path}) but its branch "
        f"{branch} lacks the -plan{plan_id} suffix; run "
        f"`make plan-accept TASK={task_ref}` to relink branch+worktree+row "
        f"(never rename by hand)."
    )


def _build_directive(event_name: str, additional_context: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": event_name,
            "additionalContext": additional_context,
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

    task_label = context.task_ref or "(active task)"
    messages: list[str] = []

    # cwd-drift nudge: only when the agent is not already in the target worktree.
    payload_cwd = _payload_cwd(payload)
    cwd_worktree = _cwd_worktree(payload_cwd)
    if cwd_worktree != target:
        cwd_display = payload_cwd or str(Path.cwd())
        messages.append(_cd_nudge_message(task_label, target, cwd_display))

    # implementation note D3: plan-suffix drift warn. Independent of the cwd-drift check —
    # a correctly-targeted but wrongly-named (bare) worktree must still warn.
    plan_id = _plan_suffix_drift(context)
    if plan_id is not None:
        messages.append(
            _plan_drift_message(
                task_label,
                context.task_plan_path or "",
                context.target_branch or "",
                plan_id,
            )
        )

    if not messages:
        return 0

    directive = _build_directive(event_name, " ".join(messages))
    print(json.dumps(directive))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
