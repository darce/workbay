#!/usr/bin/env python3
"""PreToolUse hook: guard against oversized decision rationale.

Fires before record_event and close_slice MCP calls.  If the rationale
field exceeds the character limit, blocks the call with guidance on
trimming.

Rationale from handoff.db analysis: 133 decisions exceeded 2000 chars,
with the worst at 17,374 chars (a full branch review verdict stored as
decision rationale).  Each heavy decision burns 1.5-4.5K tokens when
loaded by get_handoff_state's default top_n_decisions=3.

Hook contract (Claude Code PreToolUse):
  stdin:  JSON with tool_input (MCP call arguments)
  stdout: (unused on block; stderr carries the reason)
  exit 0 to allow, exit 2 to block (stderr shown as reason)
"""

from __future__ import annotations

import json
import sys

RATIONALE_HARD_LIMIT = 3_000   # chars — block above this
RATIONALE_SOFT_LIMIT = 1_500   # chars — warn via additionalContext

# Slice-complete decisions use a structured template that tends to run
# longer.  Allow a higher limit for those.
SLICE_COMPLETE_HARD_LIMIT = 4_000

# Required markdown sections for slice_complete_* decisions.
# Must match shared_primitives.SLICE_COMPLETE_REQUIRED_SECTIONS.
SLICE_COMPLETE_REQUIRED_SECTIONS = (
    "## Changes",
    "## Verification",
    "## Schema / Contract Changes",
    "## Open Threads",
)


def _get_rationale(tool_input: dict) -> tuple[str, str]:
    """Extract the rationale text and event_kind from the tool input.

    Returns (rationale, event_kind).  Works for both record_event and
    close_slice call shapes.
    """
    # record_event shape: tool_input.event.rationale
    event = tool_input.get("event") or {}
    rationale = event.get("rationale", "")
    event_kind = event.get("event_kind", "")

    if not rationale:
        # close_slice shape: tool_input.rationale
        rationale = tool_input.get("rationale", "")
        if rationale:
            event_kind = "slice_complete"

    return rationale, event_kind


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, OSError):
        sys.exit(0)  # allow on parse failure

    if isinstance(payload, dict):
        try:
            from _protocol import validate_event  # type: ignore[import-not-found]

            validate_event(payload, expected="PreToolUse")
        except ImportError:
            pass

    tool_input = payload.get("tool_input") or {}
    rationale, event_kind = _get_rationale(tool_input)

    # Only guard decision and slice_complete events.  Other event kinds
    # (test_result, blocker) may carry a rationale-like field but have
    # different verbosity norms.
    if event_kind and event_kind not in ("decision", "slice_complete"):
        sys.exit(0)

    if not rationale:
        sys.exit(0)

    # Determine the applicable limit
    is_slice = "slice_complete" in event_kind or "slice_complete" in (
        tool_input.get("event", {}).get("decision", "")
        or tool_input.get("decision", "")
        or ""
    )
    hard_limit = SLICE_COMPLETE_HARD_LIMIT if is_slice else RATIONALE_HARD_LIMIT

    # Block slice_complete decisions that are missing required sections.
    # This catches structural errors before the server validates them,
    # saving the agent a full round-trip retry.
    if is_slice:
        missing = [s for s in SLICE_COMPLETE_REQUIRED_SECTIONS if s not in rationale]
        if missing:
            missing_list = ", ".join(f'"{s}"' for s in missing)
            print(
                f"Slice-complete rationale is missing required sections: {missing_list}. "
                f"All four sections must be non-empty: ## Changes, ## Verification, "
                f"## Schema / Contract Changes, ## Open Threads. "
                f"Template: docs/workbay/templates/slice-complete-template.md.",
                file=sys.stderr,
            )
            sys.exit(2)

    char_count = len(rationale)

    if char_count > hard_limit:
        over_by = char_count - hard_limit
        kind_label = "Slice-complete" if is_slice else "Decision"
        print(
            f"{kind_label} rationale is {char_count:,} chars "
            f"({over_by:,} over the {hard_limit:,}-char limit).  "
            f"Trim to essentials: keep the decision + key reason.  "
            f"Move verbose details to verification_evidence or changed_files_json.  "
            f"Slice-complete decisions should follow "
            f"docs/workbay/templates/slice-complete-template.md.",
            file=sys.stderr,
        )
        sys.exit(2)

    # Soft warning — allow but inject advice via stdout
    if char_count > RATIONALE_SOFT_LIMIT:
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "additionalContext": (
                    f"Note: rationale is {char_count:,} chars.  "
                    f"Consider trimming on future calls to stay under "
                    f"{RATIONALE_SOFT_LIMIT:,} chars for context efficiency."
                ),
            }
        }))
    sys.exit(0)


if __name__ == "__main__":
    main()
