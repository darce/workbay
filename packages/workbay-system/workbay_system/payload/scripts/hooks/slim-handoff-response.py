#!/usr/bin/env python3
"""PostToolUse hook: context-budget advisory + hard-truncate for handoff responses.

Fires after ``get_handoff_state`` and ``load_session`` MCP calls. The hook
consumes the internal ``data.read_shape`` and ``data.read_budget``
metadata produced by Layer 1 (profiles) and Layer 2 (budget planner) to
emit a structured next-call suggestion. When a response still exceeds
the char budget, the hook hard-truncates at the section level (never
mid-string) and keeps JSON valid, attaching a truncation marker field.

Hook contract (Claude Code PostToolUse):
  stdin:  JSON with tool_response (MCP result envelope)
  stdout: JSON with hookSpecificOutput.additionalContext + suggestion
          and, when truncated, truncatedToolResponse + truncation marker
  exit 0 always (never blocks the tool call)

Suggestion contract (emitted under hookSpecificOutput):
  {
    "suggested_profile": "hot_summary" | "review_packet" | "identity" | None,
    "suggested_budget_bytes": int | None,
    "rationale": str,
  }
"""

from __future__ import annotations

import copy
import json
import sys
from typing import Any

# Threshold mirrors the handoff server's canonical cross-surface response
# budget so the hook and the server agree on what "oversize" means (HARM-A-01).
# Canonical source: workbay_handoff_mcp.read_budget.CANONICAL_RESPONSE_BUDGET_BYTES
# (packages/mcp-workbay-handoff/src/workbay_handoff_mcp/read_budget.py), which
# also feeds the server's DEFAULT_BARE_CALL_RESPONSE_BUDGET_BYTES. The hook
# runs as a bare subprocess without the package on sys.path, so the value is
# MIRRORED here; a drift-check test compares the two literals.
CHAR_THRESHOLD = 16_000

# Default budget the hook suggests when an oversize response did not
# already carry a structured retry hint. Mirrors the same canonical
# constant as CHAR_THRESHOLD (see above) so a suggested retry budget is
# exactly the size the server plans for; the drift-check test pins it too.
DEFAULT_SUGGESTED_BUDGET_BYTES = 16_000

# Ordered profile fallbacks: when the active profile is already tighter
# than the threshold can accommodate, no profile downgrade is suggested
# (the budget planner takes over).
_PROFILE_DOWNGRADE: dict[str, str] = {
    "full_debug": "review_packet",
    "review_packet": "hot_summary",
    "hot_summary": "identity",
}

# Section drop order for hard-truncate (heaviest / most optional first).
# Identity keys (active, limits, task_ref, ok, read_shape, read_budget)
# are never dropped. Top-level ``state`` is also never dropped wholesale
# ([OBS-08]/ BR-0108-S1-07): load_session envelopes nest identity under
# data.state.active / data.state.limits — drain nested optional sections only.
_SECTION_DROP_ORDER: tuple[str, ...] = (
    "decisions_recent",
    "decisions",
    "slices_completed",
    "tests_recent",
    "verified_tests",
    "findings_all",
    "findings_open",
    "worker_reports_recent",
    "lane_messages_open",
    "blockers_open",
    "blockers",
    "actions_pending",
    "next_actions",
    "open_findings",
    "touched_files",
    "context_refresh",
    "current_lane",
)

# Keys protected under data.state (and top-level data) during hard-truncate.
_STATE_IDENTITY_KEYS: frozenset[str] = frozenset(
    {"active", "limits", "read_shape", "read_budget", "truncation"}
)


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
    truncated: bool = False,
    dropped: list[str] | None = None,
) -> str:
    profile = suggestion.get("suggested_profile")
    budget = suggestion.get("suggested_budget_bytes")
    lines = [
        f"Handoff response: ~{char_count:,} chars (~{token_est:,} tokens).",
        "Reduce context usage on the next call:",
    ]
    if truncated:
        drop_note = f" dropped={dropped}" if dropped else ""
        lines.insert(
            1,
            f"HARD-TRUNCATED at section level to fit budget ({CHAR_THRESHOLD} chars).{drop_note}",
        )
    if profile:
        lines.append(f'  read_profile="{profile}"  -- bounded shape for the next call')
    if budget is not None:
        lines.append(
            f"  response_budget_bytes={budget}  -- server applies budget_policy='auto_summary'"
        )
    lines.append('  sections="identity"  -- routine identity-only checks')
    lines.append(
        '  detail="summary"     -- truncate rationale/test/evidence to 200 chars'
    )

    if sections:
        section_parts = [f"{k}={v}" for k, v in sections.items() if v > 1]
        if section_parts:
            lines.append(f"  Sections present: {', '.join(section_parts)}")

    return "\n".join(lines)


def _payload_char_count(obj: Any) -> int:
    return len(json.dumps(obj, separators=(",", ":")))


def _drop_section(container: dict[str, Any], key: str) -> bool:
    """Drop ``key`` from ``container`` if present. Return True when dropped."""
    if key in container:
        del container[key]
        return True
    return False


def hard_truncate_response(
    response: dict[str, Any],
    *,
    budget_chars: int = CHAR_THRESHOLD,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Section-level hard truncate of a handoff MCP response.

    Returns ``(possibly_truncated_response, truncation_marker_or_None)``.
    Never corrupts JSON: only whole keys are removed. [OBS-08] typed
    truncation marker records what was dropped.
    """
    if _payload_char_count(response) <= budget_chars:
        return response, None

    truncated = copy.deepcopy(response)
    dropped: list[str] = []

    # Prefer mutating data.* ; fall back to top-level keys for flat envelopes.
    data = truncated.get("data")
    targets: list[tuple[str, dict[str, Any]]] = []
    if isinstance(data, dict):
        targets.append(("data", data))
        nested_state = data.get("state")
        if isinstance(nested_state, dict):
            targets.append(("data.state", nested_state))
    else:
        targets.append(("", truncated))

    for section in _SECTION_DROP_ORDER:
        if _payload_char_count(truncated) <= budget_chars:
            break
        for path_prefix, container in targets:
            if _drop_section(container, section):
                path = f"{path_prefix}.{section}" if path_prefix else section
                dropped.append(path)
                if _payload_char_count(truncated) <= budget_chars:
                    break

    # Last-resort: strip write tools schema if still over budget.
    if _payload_char_count(truncated) > budget_chars:
        for path_prefix, container in targets:
            limits = container.get("limits")
            if not isinstance(limits, dict):
                continue
            write = limits.get("write")
            if isinstance(write, dict) and "tools" in write:
                del write["tools"]
                path = f"{path_prefix}.limits.write.tools" if path_prefix else "limits.write.tools"
                dropped.append(path)

    # If still over after dropping optional sections, empty large list remnants
    # under data / data.state while keeping identity structure keys. Never drop
    # data.state wholesale — protect active + limits ([OBS-08], BR-0108-S1-07).
    if _payload_char_count(truncated) > budget_chars:
        drain_targets: list[tuple[str, dict[str, Any]]] = []
        if isinstance(data, dict):
            drain_targets.append(("data", data))
            nested_state = data.get("state")
            if isinstance(nested_state, dict):
                drain_targets.append(("data.state", nested_state))
        for path_prefix, container in drain_targets:
            for key, value in list(container.items()):
                if _payload_char_count(truncated) <= budget_chars:
                    break
                if key in _STATE_IDENTITY_KEYS or key == "state":
                    continue
                if isinstance(value, list) and value:
                    container[key] = []
                    dropped.append(f"{path_prefix}.{key}[cleared]")
                elif isinstance(value, str) and len(value) > 200:
                    container[key] = value[:200] + "…"
                    dropped.append(f"{path_prefix}.{key}[string_cap]")

    original_chars = _payload_char_count(response)
    final_chars = _payload_char_count(truncated)
    marker = {
        "truncated": True,
        "budget_chars": budget_chars,
        "original_chars": original_chars,
        "final_chars": final_chars,
        "dropped_sections": dropped,
        "marker": "slim_handoff_hard_truncate",
    }

    # Attach marker on the data object (or top-level) so consumers see it.
    if isinstance(truncated.get("data"), dict):
        truncated["data"]["truncation"] = marker
    else:
        truncated["truncation"] = marker

    # If attaching the marker pushed us over, leave it — marker is mandatory.
    return truncated, marker


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
        response_dict: dict[str, Any] | None = None
    elif isinstance(response, dict):
        response_text = json.dumps(response)
        response_for_meta = response
        response_dict = response
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

    truncated_response: dict[str, Any] | None = None
    truncation_marker: dict[str, Any] | None = None
    if response_dict is not None:
        truncated_response, truncation_marker = hard_truncate_response(
            response_dict, budget_chars=CHAR_THRESHOLD
        )

    advisory = _format_advisory(
        char_count=char_count,
        token_est=token_est,
        suggestion=suggestion,
        sections=sections,
        truncated=truncation_marker is not None,
        dropped=(truncation_marker or {}).get("dropped_sections"),
    )

    hook_out: dict[str, Any] = {
        "hookEventName": "PostToolUse",
        "additionalContext": advisory,
        "handoffSuggestion": suggestion,
    }
    if truncation_marker is not None:
        hook_out["truncation"] = truncation_marker
    if truncated_response is not None and truncation_marker is not None:
        # Harnesses that honor updatedMCPToolOutput replace the tool result;
        # others still see the marker + advisory in additionalContext.
        hook_out["updatedMCPToolOutput"] = truncated_response
        hook_out["truncatedToolResponse"] = truncated_response

    print(json.dumps({"hookSpecificOutput": hook_out}))


if __name__ == "__main__":
    main()
