"""Tests for the slim-handoff-response PostToolUse hook."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

HOOK = Path(__file__).parent / "slim-handoff-response.py"


def _run_hook_raw(payload: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=5,
    )


def run_hook(payload: dict) -> dict:
    """Run the hook with a schema-valid *payload* and return parsed JSON stdout.

    Schema-clean payloads must not trip the workbay-protocol drift
    warning; if they do, either the payload is missing a required
    protocol field or the helper started rejecting a previously
    accepted shape — both are regressions worth seeing in CI.
    """
    result = _run_hook_raw(payload)
    assert result.returncode == 0, f"Hook exited {result.returncode}: {result.stderr}"
    assert "[hook-protocol]" not in result.stderr, (
        f"protocol drift on schema-valid payload: {result.stderr!r}"
    )
    return json.loads(result.stdout)


def run_hook_lenient(payload: dict) -> dict:
    """Run the hook with an intentionally malformed *payload*.

    Edge cases that omit required protocol fields use this runner so the
    helper's drift warning on stderr does not flag as a test regression.
    """
    result = _run_hook_raw(payload)
    assert result.returncode == 0, f"Hook exited {result.returncode}: {result.stderr}"
    return json.loads(result.stdout)


def get_context(resp: dict) -> str | None:
    hso = resp.get("hookSpecificOutput")
    if hso:
        return hso.get("additionalContext")
    return None


def get_suggestion(resp: dict) -> dict | None:
    """Return the internal structured handoffSuggestion block, if present."""
    hso = resp.get("hookSpecificOutput")
    if hso:
        return hso.get("handoffSuggestion")
    return None


def make_handoff_payload(
    response_chars: int,
    *,
    read_shape: dict | None = None,
    read_budget: dict | None = None,
) -> dict:
    """Create a payload simulating a get_handoff_state response of given size.

    When ``read_shape`` or ``read_budget`` are supplied they are attached
    to the envelope's ``data`` object so the hook can consume the
    internal structured metadata.
    """
    padding = "x" * response_chars
    data: dict[str, Any] = {
        "active": {"task_ref": "TEST-1", "objective": "test"},
        "decisions": [{"rationale": padding}],
    }
    if read_shape is not None:
        data["read_shape"] = read_shape
    if read_budget is not None:
        data["read_budget"] = read_budget
    return {
        "hook_event_name": "PostToolUse",
        "session_id": "test-session",
        "tool_name": "mcp_workbay-handoff-mcp_get_handoff_state",
        "tool_input": {"task_ref": "TEST-1"},
        "tool_response": {"data": data},
    }


# ---------------------------------------------------------------------------
# Below threshold: no-op
# ---------------------------------------------------------------------------


class TestBelowThreshold:
    def test_small_response(self):
        resp = run_hook(make_handoff_payload(100))
        assert get_context(resp) is None

    def test_just_under_threshold(self):
        resp = run_hook(make_handoff_payload(7000))
        assert get_context(resp) is None
        assert get_suggestion(resp) is None


# ---------------------------------------------------------------------------
# Above threshold: advisory injected
# ---------------------------------------------------------------------------


class TestAboveThreshold:
    def test_large_response_warns(self):
        resp = run_hook(make_handoff_payload(10_000))
        ctx = get_context(resp)
        assert ctx is not None
        assert "tokens" in ctx
        assert "read_profile" in ctx or "sections=" in ctx

    def test_advisory_mentions_levers(self):
        resp = run_hook(make_handoff_payload(15_000))
        ctx = get_context(resp)
        assert 'read_profile="hot_summary"' in ctx
        assert 'sections="identity"' in ctx
        assert 'detail="summary"' in ctx

    def test_load_session_also_triggers(self):
        payload = make_handoff_payload(10_000)
        payload["tool_name"] = "mcp_workbay-handoff-mcp_load_session"
        resp = run_hook(payload)
        ctx = get_context(resp)
        assert ctx is not None


# ---------------------------------------------------------------------------
# internal: structured suggestion block
# ---------------------------------------------------------------------------


class TestStructuredSuggestion:
    """The hook must emit a structured ``handoffSuggestion`` block on every
    oversize response so transport-side consumers can drive automatic
    retries without scraping the free-text advisory."""

    def test_unbudgeted_oversize_suggests_hot_summary(self):
        """No read_shape / read_budget metadata — suggest hot_summary + default budget."""
        resp = run_hook(make_handoff_payload(15_000))
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] == "hot_summary"
        assert suggestion["suggested_budget_bytes"] == 8_000
        assert "without a read_profile" in suggestion["rationale"]

    def test_budgeted_oversize_with_full_debug_downgrades(self):
        """If the caller already used a broad profile, suggest one notch tighter."""
        resp = run_hook(
            make_handoff_payload(
                15_000,
                read_shape={
                    "applied_profile": "full_debug",
                    "applied_reductions": [],
                    "omitted_sections": [],
                },
            )
        )
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] == "review_packet"
        assert suggestion["suggested_budget_bytes"] == 8_000
        assert "full_debug" in suggestion["rationale"]

    def test_budgeted_oversize_with_review_packet_downgrades(self):
        resp = run_hook(
            make_handoff_payload(
                15_000,
                read_shape={
                    "applied_profile": "review_packet",
                    "applied_reductions": ["detail_to_summary"],
                    "omitted_sections": [],
                },
            )
        )
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] == "hot_summary"
        assert "review_packet" in suggestion["rationale"]

    def test_load_session_nested_shape_downgrades(self):
        """load_session nests the state shape under read_shape.state."""
        payload = make_handoff_payload(
            15_000,
            read_shape={
                "state": {
                    "applied_profile": "review_packet",
                    "omitted_sections": [],
                },
                "session": {"omitted_sections": []},
            },
        )
        payload["tool_name"] = "mcp_workbay-handoff-mcp_load_session"

        resp = run_hook(payload)
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] == "hot_summary"
        assert "review_packet" in suggestion["rationale"]

    def test_specialized_profile_suggests_budget_without_claiming_unprofiled(self):
        resp = run_hook(
            make_handoff_payload(
                15_000,
                read_shape={"applied_profile": "open_items"},
            )
        )
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] is None
        assert suggestion["suggested_budget_bytes"] == 8_000
        assert "open_items" in suggestion["rationale"]
        assert "without a read_profile" not in suggestion["rationale"]

    def test_fail_policy_retry_with_takes_precedence(self):
        """``budget_policy=fail`` retry_with hints win over profile downgrade."""
        resp = run_hook(
            make_handoff_payload(
                15_000,
                read_shape={"applied_profile": "review_packet"},
                read_budget={
                    "requested_bytes": 4_000,
                    "policy": "fail",
                    "estimated_initial_bytes": 12_000,
                    "estimated_after_bytes": 12_000,
                    "applied_reductions": [],
                    "omitted_sections": [],
                    "over_budget_after": True,
                    "retry_with": {
                        "read_profile": "hot_summary",
                        "response_budget_bytes": 6_000,
                        "budget_policy": "auto_summary",
                    },
                },
            )
        )
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] == "hot_summary"
        assert suggestion["suggested_budget_bytes"] == 6_000
        assert "retry_with" in suggestion["rationale"]

    def test_suggestion_includes_planner_trace(self):
        """When the planner already applied reductions, rationale mentions them."""
        resp = run_hook(
            make_handoff_payload(
                15_000,
                read_budget={
                    "applied_reductions": [
                        "detail_to_summary",
                        "lowered_top_n_decisions_5_to_2",
                    ],
                    "omitted_sections": ["tests_recent"],
                },
            )
        )
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        rationale = suggestion["rationale"]
        assert "detail_to_summary" in rationale
        assert "tests_recent" in rationale


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
        assert json.loads(result.stdout) == {}

    def test_string_response(self):
        # Payload omits hook_event_name / session_id intentionally —
        # exercises the legacy non-schema callers the helper tolerates.
        payload = {
            "tool_name": "mcp_workbay-handoff-mcp_get_handoff_state",
            "tool_response": "x" * 10_000,
        }
        resp = run_hook_lenient(payload)
        ctx = get_context(resp)
        assert ctx is not None

    def test_empty_response(self):
        payload = {
            "tool_name": "mcp_workbay-handoff-mcp_get_handoff_state",
            "tool_response": {},
        }
        resp = run_hook_lenient(payload)
        assert get_context(resp) is None
