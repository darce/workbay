#!/usr/bin/env python3
"""PreToolUse(Agent) hook: under ``remote_only``, implementation lanes go to the VM.

Operator directive (2026-09-08): when the install was provisioned with
``--with-remote`` (install ledger ``execution_mode: remote_only``), every
implement / fix / repair lane is dispatched to the remote VM through
``dispatch_wave`` / ``run_offload_pass``; in-process harness subagents are
limited to **read-only** grunt work (classification, prior-art lookup, fetching,
report drafting). A coordinator that spawns a local ``Agent`` to "just edit the
small thing" has silently dropped that policy -- the same prohibited
substitution as a local backend fallback -- and nothing complained at the time.

A rule that depends on an agent remembering it is a buffer, not a drain: this
hook is the drain. It refuses the implementation-shaped local subagent before it
runs and names the sanctioned primitive.

Contract (Claude Code harness; ``Agent`` is the in-process subagent tool):
    stdin  : JSON payload with tool_name and tool_input.{prompt,description,subagent_type}
    args   : none
    stderr : BLOCKED message naming dispatch_wave / run_offload_pass + the override
             (exit-2 PreToolUse feedback is read from stderr; stdout is dropped)
    exit 0 : allow
    exit 2 : block

Predicate -- ALL of:
    1. the ledger at ``$CLAUDE_PROJECT_DIR`` (or cwd) reads ``execution_mode: remote_only``
       (same file precedence as ``workbay_protocol.bootstrap.load_execution_mode``;
       any other value, a missing file, or unreadable JSON reads as ``local_ok``);
    2. the subagent type is not tool-restricted read-only (``Explore``, ``Plan``, ...);
    3. the prompt/description does not declare itself read-only;
    4. the prompt/description carries implementation intent (implement / fix /
       edit / patch / refactor / commit / resolve conflict / ...).

Easy override (the directive: "an easy override so work does not get blocked"):
    - put ``[local-ok]`` anywhere in the Agent prompt or description, or
    - set ``WORKBAY_ALLOW_LOCAL_IMPLEMENT_SUBAGENT=1`` in the harness environment.
    Every bypass is audit-logged to ``.task-state/agent_remote_only_bypass.jsonl``.

Fail-open: any parse error, foreign tool name, or unexpected shape allows. This
guard exists to stop *forgetting*, not to resist an adversarial coordinator.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import sys
from pathlib import Path

_BYPASS_ENV = "WORKBAY_ALLOW_LOCAL_IMPLEMENT_SUBAGENT"
_BYPASS_MARKER = "[local-ok]"
_AUDIT_FILE = "agent_remote_only_bypass.jsonl"

# Mirrors workbay_protocol.paths.MANIFEST_NAME_PRECEDENCE. Duplicated on purpose:
# hooks run under the harness's python3, not the repo .venv, so importing the
# package would make the guard's availability depend on an editable install.
_MANIFEST_NAME_PRECEDENCE = (".workbay-bootstrap.json", ".workbay-overlay.json")

_GUARDED_TOOL_NAMES = frozenset({"Agent", "Task"})

# Subagent types whose tool set excludes Edit/Write/NotebookEdit. They cannot
# implement regardless of what the prompt says.
_READ_ONLY_SUBAGENT_TYPES = frozenset({"Explore", "Plan", "claude-code-guide"})

# An affirmative read-only declaration in the brief wins over implementation
# vocabulary: "READ-ONLY: report which fix landed" is a classifier, not a lane.
_READ_ONLY_DECLARATION = re.compile(
    r"\bread[\s_-]?only\b|\bdo not (?:edit|modify|write|touch|change)\b|\bno (?:edits|writes|code changes)\b",
    re.IGNORECASE,
)

# Implementation intent. Word-bounded so "fixture" / "commitment" / "editor" do
# not match; verbs are anchored to their common objects where the bare verb is
# too ambiguous ("write a report" is read-only, "write the module" is not).
_IMPLEMENTATION_INTENT = re.compile(
    r"\b(?:"
    r"implement(?:s|ed|ing)?"
    r"|fix(?:es|ed|ing)?"
    r"|patch(?:es|ed|ing)?"
    r"|refactor(?:s|ed|ing)?"
    r"|edit(?:s|ed|ing)?"
    r"|modify(?:ing)?|modifies|modified"
    r"|rewrite(?:s|ing)?|rewrote"
    r"|resolve (?:the |these |all )?(?:merge )?conflicts?"
    r"|complete the merge"
    r"|git commit|slice-commit|slice-start|stage the (?:result|changes|diff)"
    r"|apply (?:the |this |a )?(?:patch|diff|change|fix)"
    r"|(?:write|create|add|update|change) (?:the |a |an |this |new )?(?:file|module|function|class|test|hook|script|code)s?"
    r")\b",
    re.IGNORECASE,
)


def _repo_root() -> Path:
    return Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())


def _execution_mode(root: Path) -> str:
    """Literal ``remote_only`` only; everything else (incl. errors) is ``local_ok``."""
    for name in _MANIFEST_NAME_PRECEDENCE:
        candidate = root / name
        try:
            raw = candidate.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            data = json.loads(raw)
        except ValueError:
            return "local_ok"
        if isinstance(data, dict) and data.get("execution_mode") == "remote_only":
            return "remote_only"
        return "local_ok"
    return "local_ok"


def _brief_text(tool_input: dict) -> str:
    parts = []
    for key in ("prompt", "description"):
        value = tool_input.get(key)
        if isinstance(value, str):
            parts.append(value)
    return "\n".join(parts)


def classify(tool_input: dict) -> tuple[str, str | None]:
    """Return ``(verdict, detail)`` where verdict is ``allow`` | ``block``.

    Pure function over the tool input so the decision table is unit-testable
    without the ledger; ``main`` layers the ledger and the bypasses on top.
    """
    subagent_type = tool_input.get("subagent_type")
    if isinstance(subagent_type, str) and subagent_type in _READ_ONLY_SUBAGENT_TYPES:
        return "allow", f"read-only subagent_type {subagent_type!r}"
    text = _brief_text(tool_input)
    if not text:
        return "allow", "no brief text"
    if _READ_ONLY_DECLARATION.search(text):
        return "allow", "brief declares itself read-only"
    match = _IMPLEMENTATION_INTENT.search(text)
    if match is None:
        return "allow", "no implementation intent"
    return "block", match.group(0)


def _log_bypass(*, via: str, tool_input: dict, intent: str | None) -> None:
    """Append a bypass audit record; best-effort, never breaks the bypass."""
    record = {
        "event": "agent_remote_only_bypass",
        "via": via,
        "bypass_env": _BYPASS_ENV,
        "bypass_marker": _BYPASS_MARKER,
        "subagent_type": tool_input.get("subagent_type"),
        "description": tool_input.get("description"),
        "matched_intent": intent,
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    try:
        state_dir = _repo_root() / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / _AUDIT_FILE).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError:
        pass


def _blocked_message(intent: str, subagent_type: object) -> str:
    return (
        "BLOCKED: local implementation subagent under execution_mode=remote_only.\n\n"
        f"This install was provisioned with --with-remote: implementation lanes go to "
        f"the remote VM, not to an in-process Agent (matched intent: {intent!r}, "
        f"subagent_type={subagent_type!r}).\n\n"
        "Dispatch it instead:\n"
        "  - dispatch_wave / dispatch_lane_work on workbay-orchestrator-mcp (lane fan-out), or\n"
        "  - run_offload_pass for a single bounded pass.\n"
        "Model and effort are chosen per session; do not hardcode one in the brief.\n\n"
        "Local Agent subagents stay allowed for read-only work (classification, "
        "prior-art lookup, fetching, report drafting) -- say so in the brief "
        "(e.g. 'READ-ONLY:'), or use a read-only subagent_type such as Explore.\n\n"
        f"Override when the VM genuinely cannot do it: put `{_BYPASS_MARKER}` in the "
        f"Agent prompt, or set {_BYPASS_ENV}=1 in the harness environment. Every "
        f"bypass is logged to .task-state/{_AUDIT_FILE}.\n"
        "See docs/workbay/rules/offload-remote-playbook.md (When remote preference applies)."
    )


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(payload, dict):
        return 0
    if payload.get("tool_name", payload.get("toolName")) not in _GUARDED_TOOL_NAMES:
        return 0
    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        return 0
    if _execution_mode(_repo_root()) != "remote_only":
        return 0

    verdict, detail = classify(tool_input)
    if verdict == "allow":
        return 0

    if os.environ.get(_BYPASS_ENV) == "1":
        _log_bypass(via="env", tool_input=tool_input, intent=detail)
        return 0
    if _BYPASS_MARKER in _brief_text(tool_input):
        _log_bypass(via="marker", tool_input=tool_input, intent=detail)
        return 0

    print(_blocked_message(detail or "", tool_input.get("subagent_type")), file=sys.stderr)
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001 - fail-open guard: never block on our own bug
        sys.exit(0)
