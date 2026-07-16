#!/usr/bin/env python3
"""PreToolUse hook: guard against string-serialised dict parameters on MCP tools.

Fires before record_event. When `event` is passed as a JSON string instead of
a native dict, the MCP server raises:

    Input should be a valid dictionary or object to extract fields from
    [type=model_attributes_type, input_type=str]

This hook catches the pattern before the call reaches the server, blocks with
a clear corrective message, and shows the corrected form — so the agent retries
without an opaque Pydantic traceback.

Common cause: the LLM constructs the event payload as a string when the
rationale field contains curly braces, multiline content, or embedded JSON.

Hook contract (Claude Code PreToolUse):
  stdin:  JSON with tool_input (MCP call arguments)
  stdout: optional JSON with additionalContext (used for soft warnings)
  exit 0 to allow, exit 2 to block (stderr shown as reason)
"""

from __future__ import annotations

import json
import sys

# Fields that must be dicts (not strings) on these MCP tools.
# Maps tool_name_fragment → list of param names that must be dict.
DICT_PARAMS: dict[str, list[str]] = {
    "record_event": ["event"],
    "review_findings": ["review"],
    "review_runs": ["review"],
    "set_handoff_state": [],  # no nested-dict params requiring this guard
}

# Max chars of the raw string value to echo in the error message.
_ECHO_LIMIT = 120


def _check_param(tool_input: dict, param: str, tool_hint: str) -> str | None:
    """Return an error message if tool_input[param] is a string that should be a dict."""
    value = tool_input.get(param)
    if value is None or isinstance(value, dict):
        return None  # absent or already correct

    if not isinstance(value, str):
        return None  # unexpected type — let the server surface its own error

    # String received — attempt to parse as JSON to give a corrective example.
    preview = value[:_ECHO_LIMIT] + ("..." if len(value) > _ECHO_LIMIT else "")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return (
            f"{tool_hint}: `{param}` was passed as a non-JSON string. "
            f"It must be a native dict (JSON object). "
            f"Received: {preview!r}"
        )

    if not isinstance(parsed, dict):
        return (
            f"{tool_hint}: `{param}` parsed as {type(parsed).__name__}, expected dict. "
            f"Pass a JSON object, not an array or scalar."
        )

    corrected = json.dumps(parsed)
    corrected_preview = corrected[:_ECHO_LIMIT] + ("..." if len(corrected) > _ECHO_LIMIT else "")
    return (
        f"{tool_hint}: `{param}` was passed as a JSON string, not a dict.\n"
        f"Replace:  {param}='{preview}'\n"
        f"With:     {param}={corrected_preview}\n"
        f"(Pass the object directly — do not quote it as a string.)"
    )


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, OSError):
        sys.exit(0)

    if isinstance(payload, dict):
        try:
            from _protocol import validate_event  # type: ignore[import-not-found]

            validate_event(payload, expected="PreToolUse")
        except ImportError:
            pass

    tool_name: str = payload.get("tool_name", "") or ""
    tool_input: dict = payload.get("tool_input") or {}

    errors: list[str] = []
    for fragment, params in DICT_PARAMS.items():
        if fragment in tool_name:
            for param in params:
                msg = _check_param(tool_input, param, f"{tool_name}/{param}")
                if msg:
                    errors.append(msg)

    if errors:
        print("\n\n".join(errors), file=sys.stderr)
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
