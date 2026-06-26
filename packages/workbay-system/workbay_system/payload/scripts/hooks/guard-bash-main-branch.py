#!/usr/bin/env python3
"""PreToolUse(Bash) hook: block destructive shell edits to protected paths on main.

Covers the BR-17 bypass where `sed -i`, `echo > file`, `tee`, `rm`, `python -c
"open(..., 'w')"`, `git restore`, etc. ran via the Bash tool and were never
scanned by the editor-tool-only main-branch guard.

Contract (Claude Code + VS Code harnesses):
    stdin  : JSON payload with tool_name and tool_input.command
    args   : none
    stdout : BLOCKED message when a write to a protected path is detected
    exit 0 : allow
    exit 2 : block
"""

from __future__ import annotations

import datetime as _datetime
import json
import os
import subprocess
import sys
from pathlib import Path

# internal: WORKBAY_* is the primary bypass name per the Tier-4
# env-var convention; the ALT_* form remains a deprecated legacy fallback.
_BYPASS_ENV_PRIMARY = "WORKBAY_ALLOW_BASH_MAIN_WRITE"
_BYPASS_ENV_LEGACY = "ALT_ALLOW_BASH_MAIN_WRITE"


def _env_bypass_set(var_name: str) -> bool:
    """True when ``var_name`` requests a bypass via the environment.

    The ``WORKBAY_*`` primary resolves through the shared ``_interp`` alias
    (mirroring the sibling ``_guard_main_branch_inline.py``); ``ALT_*`` stays a
    raw legacy read. Falls back to a raw read when ``_interp`` is unavailable (it
    is a sibling on the hooks-dir ``sys.path`` inserted in :func:`main` before
    this runs).
    """
    if var_name.startswith("WORKBAY_"):
        try:
            from _interp import resolve_env_alias
        except ImportError:
            return os.environ.get(var_name) == "1"
        return resolve_env_alias(var_name) == "1"
    return os.environ.get(var_name) == "1"


def _bypass_request(command: str) -> tuple[str, str] | None:
    """Return ``(source, var_name)`` when a bypass is requested, else None.

    ``source`` is ``"env"`` (variable set in the environment that launched
    the harness) or ``"inline"`` (a leading ``VAR=1`` assignment on the FIRST
    stage of the command). Pre-fix the printed advice suggested an inline
    assignment, but the check only read ``os.environ`` — which the hook
    process evaluates *before* the user's command runs, so the inline form
    could never work. Only a first-stage leading assignment counts: a
    mid-command ``&& VAR=1 cmd`` does not bypass earlier stages.
    """
    for var_name in (_BYPASS_ENV_PRIMARY, _BYPASS_ENV_LEGACY):
        if _env_bypass_set(var_name):
            return "env", var_name
    try:
        from _bash_isolation_guard import _iter_words
    except ImportError:
        return None
    stages = _iter_words(command)
    if not stages:
        return None
    first_joiner, first_tokens = stages[0]
    if first_joiner is not None:
        return None
    for token in first_tokens:
        name, sep, value = token.partition("=")
        if not sep or not name.isidentifier():
            break  # past the leading-assignment prefix
        if name in (_BYPASS_ENV_PRIMARY, _BYPASS_ENV_LEGACY) and value == "1":
            return "inline", name
    return None


def _log_bypass(
    repo_root: Path,
    command: str,
    blocked: list[str],
    *,
    source: str,
    var_name: str,
) -> None:
    """Append a bypass audit record to .task-state/branch_isolation_guard.jsonl.

    Best-effort: an unwritable state dir must never break the bypass itself.
    """
    record = {
        "event": "bash_main_write_bypass",
        "bypass_source": source,
        "bypass_var": var_name,
        "command": command,
        "blocked_paths": blocked,
        "ts": _datetime.datetime.now(_datetime.timezone.utc).isoformat(),
    }
    try:
        state_dir = repo_root / ".task-state"
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / "branch_isolation_guard.jsonl").open(
            "a", encoding="utf-8"
        ) as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError:
        pass


def _repo_root() -> Path:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return Path.cwd()
    if proc.returncode != 0:
        return Path.cwd()
    return Path(proc.stdout.strip() or ".")


def _current_branch(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "branch", "--show-current"],
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


def _load_payload() -> dict:
    try:
        return json.load(sys.stdin)
    except Exception:
        return {}


def _extract_command(payload: dict) -> str:
    tool_input = payload.get("toolInput") or payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        return ""
    command = tool_input.get("command")
    if not isinstance(command, str):
        return ""
    return command


def main() -> int:
    repo_root = _repo_root()
    branch = _current_branch(repo_root)
    if branch not in {"main", "master"}:
        return 0

    payload = _load_payload()
    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(payload, expected="PreToolUse")
    except ImportError:
        pass
    tool_name = payload.get("toolName") or payload.get("tool_name") or ""
    if tool_name != "Bash":
        return 0

    command = _extract_command(payload)
    if not command:
        return 0

    sys.path.insert(0, str(repo_root / "scripts" / "hooks"))
    try:
        from _bash_isolation_guard import scan_bash_command
        from _harness_protocol import (
            HarnessContractMissingError,
            HarnessContractMissingPolicy,
            handle_missing_contract,
            load_branch_isolation_policy,
        )
    except ImportError as exc:
        print(f"guard-bash-main-branch: import failed — {exc}", file=sys.stderr)
        return 0

    # internal: this is an end-user PreToolUse hook; a missing
    # contract YAML must warn and exit 0 instead of blocking the user's
    # Bash command. Hard-fail enforcement lives in the internal
    # verification suite (``check_main_clean.py --mode block``).
    try:
        policy = load_branch_isolation_policy(repo_root)
    except HarnessContractMissingError as exc:
        return handle_missing_contract(exc, policy=HarnessContractMissingPolicy.WARN)

    blocked = scan_bash_command(command, repo_root, policy)
    if not blocked:
        return 0

    bypass = _bypass_request(command)
    if bypass is not None:
        source, var_name = bypass
        if var_name == _BYPASS_ENV_LEGACY:
            print(
                f"(deprecated) {_BYPASS_ENV_LEGACY} is the legacy bypass name; "
                f"use {_BYPASS_ENV_PRIMARY}=1 instead.",
                file=sys.stderr,
            )
        print(
            f"(bypass) {var_name}=1 ({source}) — allowing but logging",
            file=sys.stderr,
        )
        _log_bypass(repo_root, command, blocked, source=source, var_name=var_name)
        return 0

    rendered = "\n".join(f"  - {path}" for path in blocked)
    print(
        "BLOCKED: Bash command appears to write to or delete protected paths on main.\n\n"
        f"Branch: {branch}\n"
        f"Protected paths touched by this command:\n{rendered}\n\n"
        "Use the Edit/Write tool (which has proper path semantics) or move the change\n"
        "onto a feature branch first:\n"
        "  git checkout -b feature/<task-id>-<slug>\n\n"
        "If the detection is a false positive (e.g. scanning, not writing), re-run\n"
        "with the bypass token prefixed to the WHOLE command:\n"
        f"  {_BYPASS_ENV_PRIMARY}=1 <your full command>\n"
        "(a mid-command assignment after && or ; does not bypass). Every bypass is\n"
        "logged to .task-state/branch_isolation_guard.jsonl.\n\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
