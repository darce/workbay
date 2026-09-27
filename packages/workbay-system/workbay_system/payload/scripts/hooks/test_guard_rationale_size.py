"""Tests for the guard-rationale-size PreToolUse hook."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOK = Path(__file__).parent / "guard-rationale-size.py"


def run_hook(payload: dict) -> tuple[int, str, str]:
    """Run the hook and return (exit_code, stdout, stderr)."""
    result = subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def make_record_event(rationale: str, event_kind: str = "decision", decision: str = "") -> dict:
    return {
        "hook_event_name": "PreToolUse",
        "session_id": "test-session",
        "tool_name": "mcp_workbay-handoff-mcp_record_event",
        "tool_input": {
            "event": {
                "event_kind": event_kind,
                "rationale": rationale,
                "decision": decision,
            }
        },
    }


def make_close_slice(rationale: str, decision: str = "slice_complete_test") -> dict:
    return {
        "hook_event_name": "PreToolUse",
        "session_id": "test-session",
        "tool_name": "mcp_workbay-handoff-mcp_close_slice",
        "tool_input": {
            "rationale": rationale,
            "decision": decision,
        },
    }


def make_structured_slice_rationale(total_chars: int) -> str:
    base = (
        "## Changes\n"
        "- ok\n\n"
        "## Verification\n"
        "- ok\n\n"
        "## Schema / Contract Changes\n"
        "- none\n\n"
        "## Open Threads\n"
        "- "
    )
    if total_chars <= len(base):
        msg = f"total_chars must exceed {len(base)} to keep all sections non-empty"
        raise ValueError(msg)
    rationale = base + ("x" * (total_chars - len(base) - 1)) + "\n"
    assert len(rationale) == total_chars
    return rationale


# ---------------------------------------------------------------------------
# Normal-size rationale: should pass
# ---------------------------------------------------------------------------


class TestAllowed:
    def test_short_rationale(self):
        code, stdout, stderr = run_hook(
            make_record_event("Added error handling for edge case.")
        )
        assert code == 0
        assert not stderr

    def test_no_rationale(self):
        code, _, stderr = run_hook(make_record_event(""))
        assert code == 0
        assert not stderr

    def test_non_decision_event(self):
        code, _, stderr = run_hook(
            make_record_event("x" * 5000, event_kind="test_result")
        )
        assert code == 0
        assert not stderr

    def test_just_under_limit(self):
        code, _, stderr = run_hook(make_record_event("x" * 2999))
        assert code == 0
        assert not stderr


# ---------------------------------------------------------------------------
# Soft warning zone (1500-3000 chars)
# ---------------------------------------------------------------------------


class TestSoftWarning:
    def test_warning_injected(self):
        code, stdout, stderr = run_hook(make_record_event("x" * 2000))
        assert code == 0
        assert not stderr
        resp = json.loads(stdout) if stdout else {}
        ctx = (resp.get("hookSpecificOutput") or {}).get("additionalContext", "")
        assert "2,000 chars" in ctx

    def test_no_warning_below_soft_limit(self):
        code, stdout, _ = run_hook(make_record_event("x" * 1400))
        assert code == 0
        resp = json.loads(stdout) if stdout else {}
        assert not resp.get("hookSpecificOutput")


# ---------------------------------------------------------------------------
# Hard block (>3000 chars for regular, >4000 for slice_complete)
# ---------------------------------------------------------------------------


class TestBlocked:
    def test_regular_decision_blocked_over_3000(self):
        code, _, stderr = run_hook(make_record_event("x" * 3500))
        assert code == 2
        assert "3,500" in stderr
        assert "3,000" in stderr

    def test_slice_complete_allowed_at_3500(self):
        """Slice-complete has a higher limit of 4000."""
        code, _, stderr = run_hook(
            make_record_event(
                make_structured_slice_rationale(3500),
                event_kind="decision",
                decision="cla_slice_complete_internal_test",
            )
        )
        assert code == 0
        assert not stderr

    def test_slice_complete_blocked_over_4000(self):
        code, _, stderr = run_hook(
            make_record_event(
                make_structured_slice_rationale(4500),
                event_kind="decision",
                decision="cla_slice_complete_internal_test",
            )
        )
        assert code == 2
        assert "4,500" in stderr

    def test_close_slice_blocked_over_4000(self):
        code, _, stderr = run_hook(make_close_slice(make_structured_slice_rationale(4500)))
        assert code == 2
        assert "4,500" in stderr

    def test_close_slice_allowed_under_4000(self):
        code, _, stderr = run_hook(make_close_slice(make_structured_slice_rationale(3800)))
        assert code == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_malformed_json(self):
        result = subprocess.run(
            [sys.executable, str(HOOK)],
            input="not json",
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0

    def test_empty_payload(self):
        # Empty payload is structurally invalid against the hook event
        # contract; the protocol helper logs a single-line drift warning
        # to stderr but the hook still allows (exit 0) so legacy callers
        # never get blocked by missing schema fields.
        code, _, stderr = run_hook({})
        assert code == 0
        assert "[hook-protocol]" in stderr or not stderr
