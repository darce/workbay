#!/usr/bin/env python3
"""PostToolUse hook: context-budget advisory for verbose handoff responses.

Fires after ``get_handoff_state`` and ``load_session`` MCP calls. The hook
consumes the internal ``data.read_shape`` and ``data.read_budget``
metadata produced by Layer 1 (profiles) and Layer 2 (budget planner) to
emit a structured next-call suggestion. Transport-side consumers can
drive automatic retries from the structured block without scraping the
free-text advisory.

Hook contract (Claude Code PostToolUse):
  stdin:  JSON with tool_response (MCP result envelope)
  stdout: JSON with hookSpecificOutput.additionalContext + suggestion
  exit 0 always (observational hook, never blocks)

Suggestion contract (emitted under hookSpecificOutput):
  {
    "suggested_profile": "hot_summary" | "review_packet" | "identity" | None,
    "suggested_budget_bytes": int | None,
    "rationale": str,
  }
"""

from __future__ import annotations

import json
import sys
from typing import Any

# Threshold: ~2K tokens. The handoff server's own oversize_response
# advisory fires at ~20KB / ~5K tokens. This hook fires earlier to
# steer behavior before the expensive call.
CHAR_THRESHOLD = 8_000

# Default budget the hook suggests when an oversize response did not
# already carry a structured retry hint. Aligned with CHAR_THRESHOLD so
# the next call should not retrip this hook.
DEFAULT_SUGGESTED_BUDGET_BYTES = 8_000

# Ordered profile fallbacks: when the active profile is already tighter
# than the threshold can accommodate, no profile downgrade is suggested
# (the budget planner takes over).
_PROFILE_DOWNGRADE: dict[str, str] = {
    "full_debug": "review_packet",
    "review_packet": "hot_summary",
    "hot_summary": "identity",
}


def _estimate_tokens(text: str) -> int:
    return len(text) // 4


def _count_sections(response_text: str) -> dict[str, int]:
    """Rough section counts from the JSON keys present."""
    counts: dict[str, int] = {}
    for key in (
        "decisions",
        "verified_tests",
        "findings_open",
        "findings_all",
        "blockers",
        "next_actions",
    ):
        occurrences = response_text.count(f'"{key}"')
        if occurrences:
            counts[key] = occurrences
    return counts


def _extract_data(response: Any) -> dict[str, Any]:
    """Return the envelope's ``data`` dict if present, else an empty dict."""
    if not isinstance(response, dict):
        return {}
    data = response.get("data")
    if isinstance(data, dict):
        return data
    # Legacy / flattened envelope — treat the response itself as data.
    return response


def _extract_state_read_shape(read_shape: Any) -> dict[str, Any]:
    """Return the state-side read_shape for state or compound responses."""
    if not isinstance(read_shape, dict):
        return {}
    nested_state = read_shape.get("state")
    if isinstance(nested_state, dict):
        return nested_state
    return read_shape


def _list_from_metadata(*values: Any) -> list[Any] | None:
    """Return the first list-valued metadata field from newest to legacy shape."""
    for value in values:
        if isinstance(value, list):
            return value
    return None


def _build_suggestion(
    *,
    response: Any,
    char_count: int,
) -> dict[str, Any]:
    """Build the structured next-call suggestion from envelope metadata.

    Priority:
      1. ``data.read_budget.retry_with`` (planner's own retry hint).
      2. Downgrade ``data.read_shape.applied_profile`` one notch.
      3. Suggest ``hot_summary`` + default budget for unshaped oversize reads.
    """
    data = _extract_data(response)
    read_shape = (
        data.get("read_shape") if isinstance(data.get("read_shape"), dict) else {}
    )
    state_read_shape = _extract_state_read_shape(read_shape)
    read_budget = (
        data.get("read_budget") if isinstance(data.get("read_budget"), dict) else {}
    )

    retry_with = (
        read_budget.get("retry_with") if isinstance(read_budget, dict) else None
    )
    if isinstance(retry_with, dict):
        return {
            "suggested_profile": retry_with.get("read_profile"),
            "suggested_budget_bytes": retry_with.get("response_budget_bytes"),
            "rationale": (
                "Server returned budget_policy=fail with retry_with hint; "
                f"retry with the planner's suggested shape (estimated "
                f"{read_budget.get('estimated_initial_bytes')} bytes > "
                f"{read_budget.get('requested_bytes')} budget)."
            ),
        }

    applied_profile = state_read_shape.get("applied_profile")
    applied_reductions = _list_from_metadata(
        read_budget.get("applied_reductions"),
        state_read_shape.get("applied_reductions"),
    )
    omitted_sections = _list_from_metadata(
        read_budget.get("omitted_sections"),
        state_read_shape.get("omitted_sections"),
    )

    if isinstance(applied_profile, str) and applied_profile in _PROFILE_DOWNGRADE:
        downgrade = _PROFILE_DOWNGRADE[applied_profile]
        return {
            "suggested_profile": downgrade,
            "suggested_budget_bytes": DEFAULT_SUGGESTED_BUDGET_BYTES,
            "rationale": (
                f"Response was {char_count:,} chars while using "
                f"read_profile={applied_profile!r}. Downgrade to "
                f"read_profile={downgrade!r} with response_budget_bytes="
                f"{DEFAULT_SUGGESTED_BUDGET_BYTES} for the next call."
            ),
        }

    if isinstance(applied_profile, str):
        return {
            "suggested_profile": None,
            "suggested_budget_bytes": DEFAULT_SUGGESTED_BUDGET_BYTES,
            "rationale": (
                f"Response was {char_count:,} chars while using "
                f"read_profile={applied_profile!r}. Keep the specialized "
                "profile and add response_budget_bytes="
                f"{DEFAULT_SUGGESTED_BUDGET_BYTES} so the server can trim "
                "limits or optional sections."
            ),
        }

    # No profile applied (or applied profile is already 'identity') — the
    # caller is reading the broadest shape. Steer them to hot_summary.
    rationale_parts = [
        f"Response was {char_count:,} chars without a read_profile.",
        'Use read_profile="hot_summary" with '
        f"response_budget_bytes={DEFAULT_SUGGESTED_BUDGET_BYTES} on the next call.",
    ]
    if isinstance(applied_reductions, list) and applied_reductions:
        rationale_parts.append(
            f"Planner already applied: {', '.join(applied_reductions)}."
        )
    if isinstance(omitted_sections, list) and omitted_sections:
        rationale_parts.append(
            f"Planner already omitted: {', '.join(omitted_sections)}."
        )
    return {
        "suggested_profile": "hot_summary",
        "suggested_budget_bytes": DEFAULT_SUGGESTED_BUDGET_BYTES,
        "rationale": " ".join(rationale_parts),
    }


def _format_advisory(
    *,
    char_count: int,
    token_est: int,
    suggestion: dict[str, Any],
    sections: dict[str, int],
) -> str:
    profile = suggestion.get("suggested_profile")
    budget = suggestion.get("suggested_budget_bytes")
    lines = [
        f"Handoff response: ~{char_count:,} chars (~{token_est:,} tokens).",
        "Reduce context usage on the next call:",
    ]
    if profile:
        lines.append(f'  read_profile="{profile}"  -- bounded shape for the next call')
    if budget is not None:
        lines.append(
            f"  response_budget_bytes={budget}  -- server applies budget_policy='auto_summary'"
        )
    lines.append('  sections="identity"  -- routine identity-only checks')
    lines.append(
        '  detail="summary"     -- truncate rationale/fix/evidence to 200 chars'
    )

    if sections:
        section_parts = [f"{k}={v}" for k, v in sections.items() if v > 1]
        if section_parts:
            lines.append(f"  Sections present: {', '.join(section_parts)}")

    return "\n".join(lines)


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, OSError):
        print("{}")
        return

    if isinstance(payload, dict):
        try:
            from _protocol import validate_event  # type: ignore[import-not-found]

            validate_event(payload, expected="PostToolUse")
        except ImportError:
            pass

    response = payload.get("tool_response") or payload.get("tool_output") or {}
    if isinstance(response, str):
        response_text = response
        response_for_meta: Any = response
    elif isinstance(response, dict):
        response_text = json.dumps(response)
        response_for_meta = response
    else:
        print("{}")
        return

    char_count = len(response_text)
    if char_count < CHAR_THRESHOLD:
        print("{}")
        return

    token_est = _estimate_tokens(response_text)
    sections = _count_sections(response_text)
    suggestion = _build_suggestion(response=response_for_meta, char_count=char_count)
    advisory = _format_advisory(
        char_count=char_count,
        token_est=token_est,
        suggestion=suggestion,
        sections=sections,
    )

    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PostToolUse",
                    "additionalContext": advisory,
                    "handoffSuggestion": suggestion,
                }
            }
        )
    )


if __name__ == "__main__":
    main()
