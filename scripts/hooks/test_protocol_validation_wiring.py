"""Cross-hook test that every entrypoint routes stdin through the
shared ``_protocol.validate_event`` helper.

The contract is: when ``WORKBAY_HOOK_PROTOCOL_STRICT=1`` is set in the
environment, a payload that does not validate against the hook event
schema must cause the hook to exit non-zero (specifically exit 2 from
``raise SystemExit(2)`` inside the helper). Without strict mode, the
same malformed payload is logged to stderr but allowed through.

This guards against future hooks regressing to ad-hoc parsing without
calling the helper. Add hooks to ``WIRED_HOOKS`` as they are migrated.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).parent

WIRED_HOOKS: tuple[tuple[str, str], ...] = (
    ("record-file-touch.py", "PostToolUse"),
    ("filter-test-output.py", "PostToolUse"),
    ("slim-handoff-response.py", "PostToolUse"),
    ("guard-rationale-size.py", "PreToolUse"),
    ("validate-mcp-dict-params.py", "PreToolUse"),
    ("advise-worktree-cd.py", "SessionStart"),
    ("advise-worktree-cd.py", "UserPromptSubmit"),
    ("compact-session.py", "Stop"),
)
# guard-bash-main-branch.py and guard-task-plan-findings.py also import
# validate_event but gate it behind early-returns (current branch check
# / event-kind check) so a generic mismatched-event payload short-
# circuits before validation runs. Locking them in here would require
# either reshaping their entrypoints or scaffolding repo state per
# test, both out of scope for this slice.


def _run_hook(script: str, payload: dict, *, strict: bool) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if strict:
        env["WORKBAY_HOOK_PROTOCOL_STRICT"] = "1"
    else:
        env.pop("WORKBAY_HOOK_PROTOCOL_STRICT", None)
    return subprocess.run(
        [sys.executable, str(HOOKS_DIR / script)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )


@pytest.mark.parametrize(("script", "expected_event"), WIRED_HOOKS)
def test_strict_mode_blocks_mismatched_event_name(script: str, expected_event: str) -> None:
    """A payload claiming the wrong event type must be rejected under strict mode."""
    wrong_name = "Stop" if expected_event != "Stop" else "PreToolUse"
    payload = {
        "hook_event_name": wrong_name,
        "session_id": "test-session",
        "tool_name": "Bash",
        "tool_input": {"command": "echo ok"},
        "tool_response": {"stdout": "", "stderr": "", "exitCode": 0},
        "prompt": "",
    }
    result = _run_hook(script, payload, strict=True)
    assert result.returncode != 0, (
        f"{script}: strict mode did not block payload with mismatched "
        f"hook_event_name={wrong_name!r} (expected {expected_event!r}). "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


@pytest.mark.parametrize(("script", "expected_event"), WIRED_HOOKS)
def test_lenient_mode_allows_mismatched_event_name(script: str, expected_event: str) -> None:
    """Without strict mode, the same mismatch must log but exit 0."""
    wrong_name = "Stop" if expected_event != "Stop" else "PreToolUse"
    payload = {
        "hook_event_name": wrong_name,
        "session_id": "test-session",
        "tool_name": "Bash",
        "tool_input": {"command": "echo ok"},
        "tool_response": {"stdout": "", "stderr": "", "exitCode": 0},
        "prompt": "",
    }
    result = _run_hook(script, payload, strict=False)
    assert result.returncode == 0, (
        f"{script}: lenient mode unexpectedly blocked. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    # Drift warning is observable on stderr — confirms the hook actually
    # called the helper rather than skipping validation.
    assert "[hook-protocol]" in result.stderr, (
        f"{script}: no [hook-protocol] drift warning on stderr — hook "
        f"may not be wired through validate_event. "
        f"stderr={result.stderr!r}"
    )
