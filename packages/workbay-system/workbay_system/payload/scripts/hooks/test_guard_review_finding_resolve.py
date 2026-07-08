from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HOOK = Path(__file__).parent / "guard-review-finding-resolve.py"


def _run_hook(review: dict, *, tool_name: str = "mcp__workbay-handoff-mcp__review_findings") -> subprocess.CompletedProcess:
    payload = {
        "hook_event_name": "PreToolUse",
        "session_id": "test-session",
        "tool_name": tool_name,
        "tool_input": {"review": review},
    }
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=5,
    )


def test_blocks_update_fixed_with_resolve_call_shape() -> None:
    result = _run_hook({"operation": "update", "status": "fixed", "finding_id": "F-1"})

    assert result.returncode == 2
    assert 'review_findings(review={"operation":"resolve"' in result.stderr
    assert '"all_open":true' in result.stderr


def test_passes_non_fixed_updates_and_resolve_ops() -> None:
    assert _run_hook({"operation": "update", "status": "deferred"}).returncode == 0
    assert _run_hook({"operation": "resolve", "all_open": True}).returncode == 0
    assert _run_hook({"operation": "list"}).returncode == 0


def test_ignores_unrelated_tools() -> None:
    result = _run_hook({"operation": "update", "status": "fixed"}, tool_name="Bash")

    assert result.returncode == 0
