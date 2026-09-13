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
        resp = run_hook(make_handoff_payload(15_000))
        assert get_context(resp) is None
        assert get_suggestion(resp) is None


# ---------------------------------------------------------------------------
# Above threshold: advisory injected
# ---------------------------------------------------------------------------


class TestAboveThreshold:
    def test_large_response_warns(self):
        resp = run_hook(make_handoff_payload(20_000))
        ctx = get_context(resp)
        assert ctx is not None
        assert "tokens" in ctx
        assert "read_profile" in ctx or "sections=" in ctx

    def test_advisory_mentions_levers(self):
        resp = run_hook(make_handoff_payload(25_000))
        ctx = get_context(resp)
        assert 'read_profile="hot_summary"' in ctx
        assert 'sections="identity"' in ctx
        assert 'detail="summary"' in ctx

    def test_load_session_also_triggers(self):
        payload = make_handoff_payload(20_000)
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
        resp = run_hook(make_handoff_payload(25_000))
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] == "hot_summary"
        assert suggestion["suggested_budget_bytes"] == 16_000
        assert "without a read_profile" in suggestion["rationale"]

    def test_budgeted_oversize_with_full_debug_downgrades(self):
        """If the caller already used a broad profile, suggest one notch tighter."""
        resp = run_hook(
            make_handoff_payload(
                25_000,
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
        assert suggestion["suggested_budget_bytes"] == 16_000
        assert "full_debug" in suggestion["rationale"]

    def test_budgeted_oversize_with_review_packet_downgrades(self):
        resp = run_hook(
            make_handoff_payload(
                25_000,
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
            25_000,
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
                25_000,
                read_shape={"applied_profile": "open_items"},
            )
        )
        suggestion = get_suggestion(resp)
        assert suggestion is not None
        assert suggestion["suggested_profile"] is None
        assert suggestion["suggested_budget_bytes"] == 16_000
        assert "open_items" in suggestion["rationale"]
        assert "without a read_profile" not in suggestion["rationale"]

    def test_fail_policy_retry_with_takes_precedence(self):
        """``budget_policy=fail`` retry_with hints win over profile downgrade."""
        resp = run_hook(
            make_handoff_payload(
                25_000,
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
                25_000,
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
            "tool_response": "x" * 20_000,
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


# ---------------------------------------------------------------------------
# Hard-truncate (implementation note / T23): section-level, JSON-valid, marker field
# ---------------------------------------------------------------------------


class TestHardTruncate:
    def test_oversize_dict_is_hard_truncated_with_marker(self):
        resp = run_hook(make_handoff_payload(25_000))
        hso = resp["hookSpecificOutput"]
        assert hso.get("truncation", {}).get("truncated") is True
        assert "dropped_sections" in hso["truncation"]
        truncated = hso.get("truncatedToolResponse") or hso.get("updatedMCPToolOutput")
        assert isinstance(truncated, dict)
        # JSON stays valid (already parsed) and under the char threshold.
        assert len(json.dumps(truncated)) <= 16_000 + 500  # marker overhead allowance
        marker = (truncated.get("data") or {}).get("truncation") or truncated.get("truncation")
        assert marker is not None
        assert marker["marker"] == "slim_handoff_hard_truncate"
        assert "HARD-TRUNCATED" in (get_context(resp) or "")

    def test_hard_truncate_pure_function_drops_sections_not_mid_string(self):
        # Import the hook module by path.
        import importlib.util

        spec = importlib.util.spec_from_file_location("slim_handoff", HOOK)
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        big = {
            "ok": True,
            "data": {
                "active": {"task_ref": "T"},
                "limits": {"write": {"rationale_soft_chars": 1500}},
                "decisions_recent": [{"rationale": "y" * 12_000}],
                "tests_recent": [{"result": "z" * 4_000}],
            },
        }
        out, marker = mod.hard_truncate_response(big, budget_chars=2_000)
        assert marker is not None
        assert marker["truncated"] is True
        # Whole sections dropped, not sliced mid-string inside rationale.
        assert "decisions_recent" not in out["data"] or out["data"]["decisions_recent"] == []
        # Identity keys preserved.
        assert out["data"]["active"]["task_ref"] == "T"
        assert "truncation" in out["data"]
        # Round-trip JSON.
        json.loads(json.dumps(out))

    def test_hard_truncate_preserves_load_session_state_identity(self):
        """BR-0108-S1-07: never drop data.state wholesale; keep active + limits.

        load_session nests identity under data.state; hard-truncate must drain
        nested optional sections only ([OBS-08]).
        """
        import importlib.util

        spec = importlib.util.spec_from_file_location("slim_handoff", HOOK)
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        assert "state" not in mod._SECTION_DROP_ORDER

        big = {
            "ok": True,
            "tool": "load_session",
            "data": {
                "state": {
                    "active": {
                        "task_ref": "internal",
                        "status": "in_progress",
                        "objective": "keep me",
                    },
                    "limits": {"write": {"rationale_soft_chars": 1500}},
                    "decisions_recent": [{"rationale": "d" * 10_000}],
                    "tests_recent": [{"result": "t" * 6_000}],
                    "findings_open": [{"description": "f" * 4_000}],
                },
                "open_findings": [{"description": "o" * 4_000}],
                "touched_files": [{"path": "p" * 2_000}],
            },
        }
        out, marker = mod.hard_truncate_response(big, budget_chars=2_000)
        assert marker is not None
        assert marker["truncated"] is True
        # Nested state object retained with identity keys.
        assert "state" in out["data"]
        assert isinstance(out["data"]["state"], dict)
        assert out["data"]["state"]["active"]["task_ref"] == "internal"
        assert out["data"]["state"]["limits"]["write"]["rationale_soft_chars"] == 1500
        # Optional nested sections drained or cleared — not mid-string.
        state = out["data"]["state"]
        for key in ("decisions_recent", "tests_recent", "findings_open"):
            if key in state:
                assert state[key] == [] or not state[key]
        # Drop marker must not claim wholesale state removal as primary path.
        assert "data.state" not in marker["dropped_sections"]
        json.loads(json.dumps(out))


# ---------------------------------------------------------------------------
# HARM-A-01: threshold drift-check against the handoff server budget constant
# ---------------------------------------------------------------------------


def _literal_assignment(source: str, name: str) -> int:
    import ast

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = node.value
                    if isinstance(value, ast.Constant) and isinstance(value.value, int):
                        return value.value
    raise AssertionError(f"no int literal assignment for {name}")


def _find_read_budget_py() -> Path | None:
    rel = Path("packages") / "mcp-workbay-handoff" / "src" / "workbay_handoff_mcp" / "read_budget.py"
    for ancestor in Path(__file__).resolve().parents:
        candidate = ancestor / rel
        if candidate.is_file():
            return candidate
    return None


def test_char_threshold_mirrors_server_bare_call_budget():
    """The hook cannot import the handoff package (bare subprocess), so it
    mirrors CANONICAL_RESPONSE_BUDGET_BYTES (which feeds the server bare-call
    default); this test pins the two
    literals together so they cannot drift silently (HARM-A-01)."""
    read_budget = _find_read_budget_py()
    if read_budget is None:
        pytest.skip("handoff package source not co-located (standalone payload deploy)")
    server_value = _literal_assignment(
        read_budget.read_text(encoding="utf-8"),
        "CANONICAL_RESPONSE_BUDGET_BYTES",
    )
    hook_source = HOOK.read_text(encoding="utf-8")
    for hook_name in ("CHAR_THRESHOLD", "DEFAULT_SUGGESTED_BUDGET_BYTES"):
        hook_value = _literal_assignment(hook_source, hook_name)
        assert hook_value == server_value, (
            f"slim-handoff-response {hook_name}={hook_value} drifted from "
            f"read_budget CANONICAL_RESPONSE_BUDGET_BYTES={server_value}"
        )
