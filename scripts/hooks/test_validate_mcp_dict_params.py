"""Behavior tests for the validate-mcp-dict-params PreToolUse hook.

Covers the corrective-error path the hook is designed to surface
(string-serialised dict params on record_event / review_findings) and
asserts that schema-valid payloads do not trip the workbay-protocol
drift warning that lives behind the same entrypoint.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOK = Path(__file__).parent / "validate-mcp-dict-params.py"


def _run_hook(payload: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=5,
    )


def make_record_event_payload(event: object) -> dict:
    return {
        "hook_event_name": "PreToolUse",
        "session_id": "test-session",
        "tool_name": "mcp_workbay-handoff-mcp_record_event",
        "tool_input": {"event": event},
    }


# ---------------------------------------------------------------------------
# Allow path
# ---------------------------------------------------------------------------


class TestAllowed:
    def test_event_as_native_dict_passes_through(self) -> None:
        payload = make_record_event_payload(
            {"event_kind": "decision", "rationale": "ok"}
        )
        result = _run_hook(payload)
        assert result.returncode == 0
        # No corrective stderr, and no protocol drift warning.
        assert result.stderr == "", result.stderr

    def test_unrelated_tool_is_ignored(self) -> None:
        payload = {
            "hook_event_name": "PreToolUse",
            "session_id": "test-session",
            "tool_name": "Bash",
            "tool_input": {"command": "ls"},
        }
        result = _run_hook(payload)
        assert result.returncode == 0
        assert result.stderr == ""

    def test_missing_event_field_is_allowed(self) -> None:
        # The hook only complains when the expected param IS present
        # but is the wrong type. Absent is the responsibility of the
        # MCP server's own pydantic model.
        payload = {
            "hook_event_name": "PreToolUse",
            "session_id": "test-session",
            "tool_name": "mcp_workbay-handoff-mcp_record_event",
            "tool_input": {},
        }
        result = _run_hook(payload)
        assert result.returncode == 0
        assert result.stderr == ""


# ---------------------------------------------------------------------------
# Block path: corrective stderr message
# ---------------------------------------------------------------------------


class TestBlocked:
    def test_event_as_json_string_is_blocked_with_corrected_form(self) -> None:
        serialised = json.dumps({"event_kind": "decision", "rationale": "ok"})
        payload = make_record_event_payload(serialised)
        result = _run_hook(payload)
        assert result.returncode == 2
        # The corrective message must echo both forms so the agent can
        # see exactly what to swap. Drift warning would prepend if the
        # payload were schema-incomplete — assert it does NOT.
        assert "[hook-protocol]" not in result.stderr, result.stderr
        assert "was passed as a JSON string" in result.stderr
        assert "Replace:" in result.stderr
        assert "With:" in result.stderr

    def test_event_as_non_json_string_is_blocked_without_corrected_form(self) -> None:
        payload = make_record_event_payload("not even json")
        result = _run_hook(payload)
        assert result.returncode == 2
        assert "[hook-protocol]" not in result.stderr, result.stderr
        assert "was passed as a non-JSON string" in result.stderr

    def test_event_as_json_array_is_blocked_with_type_message(self) -> None:
        serialised = json.dumps(["not", "a", "dict"])
        payload = make_record_event_payload(serialised)
        result = _run_hook(payload)
        assert result.returncode == 2
        assert "[hook-protocol]" not in result.stderr, result.stderr
        assert "expected dict" in result.stderr

    def test_review_findings_review_param_is_also_guarded(self) -> None:
        payload = {
            "hook_event_name": "PreToolUse",
            "session_id": "test-session",
            "tool_name": "mcp_workbay-handoff-mcp_review_findings",
            "tool_input": {"review": json.dumps({"findings": []})},
        }
        result = _run_hook(payload)
        assert result.returncode == 2
        assert "[hook-protocol]" not in result.stderr, result.stderr
        assert "review_findings/review" in result.stderr


# ---------------------------------------------------------------------------
# Tolerance: malformed payloads must not crash the hook
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_malformed_json_is_allowed(self) -> None:
        result = subprocess.run(
            [sys.executable, str(HOOK)],
            input="not json",
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0

    def test_protocol_incomplete_payload_warns_but_allows(self) -> None:
        # Missing hook_event_name / session_id triggers the helper's
        # drift warning on stderr but the hook itself stays exit 0
        # because no dict-param violation is present.
        payload = {
            "tool_name": "mcp_workbay-handoff-mcp_record_event",
            "tool_input": {"event": {"event_kind": "decision"}},
        }
        result = _run_hook(payload)
        assert result.returncode == 0
        assert "[hook-protocol]" in result.stderr
