"""Shared result-text parsing for CLI execution adapters.

Extracted from ``claude_code.py`` (implementation note D2, Fowler rule-of-three): the
claude adapter and the grok adapter both need to recover a ``BackendResult``
dict from a CLI's JSON envelope — sometimes emitted as a clean top-level
object, sometimes narrated inside a ``text``/``result`` string. ``codex_cli.py``
is intentionally untouched (it reads codex's native ``--output-schema`` result
file and keeps its own private usage regex).

The claude adapter consumes these helpers behavior-identically; the grok
adapter (S3) layers a grok-specific extraction on top (fenced block -> first
balanced ``{...}`` -> ``structuredOutput`` fallback) without changing claude.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from typing import Any


def find_embedded_json_object(text: str) -> str | None:
    """Return the first-``{``-to-last-``}`` JSON substring of ``text``, or None.

    Greedy scan (``re.DOTALL``): matches from the first opening brace to the
    last closing brace. This is the historical claude fallback behavior and is
    preserved exactly; callers that need fenced-block preference or balanced
    matching layer that on top.
    """
    match = re.search(r"(\{.*\})", text, re.DOTALL)
    return match.group(1) if match else None


def find_first_balanced_json(text: str) -> str | None:
    """Return the first brace-balanced ``{...}`` substring of ``text``, or None.

    Unlike :func:`find_embedded_json_object` (greedy first-{ to last-}), this
    tracks brace depth and string context so it returns the FIRST complete
    object and does not mismatch across sibling objects or braces embedded in
    string literals. Used by the grok parse chain (implementation note D2), where the
    assistant may narrate several ``{...}`` fragments and only the first is the
    result payload.
    """
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    escaped = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def find_fenced_json_block(text: str) -> str | None:
    """Return the first balanced JSON object inside a fenced code block, or None.

    Matches a ```` ```json ```` or bare ```` ``` ```` fence and returns the first
    brace-balanced object within its body. Grok (implementation note D2, Evidence #7)
    tends to fence its result JSON inside ``text`` while leaving the structured
    channel null, so fenced extraction is preferred over the ``structuredOutput``
    field.
    """
    fence = re.search(r"```(?:[A-Za-z0-9_-]+)?\s*\n?(.*?)```", text, re.DOTALL)
    if not fence:
        return None
    return find_first_balanced_json(fence.group(1))


def _iter_balanced_json_objects(text: str) -> Iterator[str]:
    """Yield each brace-balanced ``{...}`` substring in ``text``, left to right.

    Repeatedly applies :func:`find_first_balanced_json` to the remaining tail so
    a narrated string containing several sibling objects surfaces each of them,
    without the greedy first-``{``-to-last-``}`` mismatch of
    :func:`find_embedded_json_object`.
    """
    pos = 0
    while pos < len(text):
        block = find_first_balanced_json(text[pos:])
        if block is None:
            return
        pos += text[pos:].find(block) + len(block)
        yield block


def extract_result_payload(response: dict[str, Any]) -> dict[str, Any]:
    """Extract the worker result payload from a CLI JSON envelope.

    When ``--output-format json`` is used, the assistant's ``BackendResult`` may
    arrive in several shapes; the search order is honest about how each is
    unwrapped:

    1. The response itself is a ``BackendResult`` (has ``handoff_action``) —
       returned directly.
    2. It is nested under a ``result`` / ``content`` / ``response`` envelope key,
       either as a dict (returned when it has ``handoff_action``) or as a JSON
       *string*. A string candidate is parsed and, when it yields a dict, this
       function **recurses** so a deeper ``handoff_action`` is surfaced (e.g. a
       doubly-wrapped ``{"result": {"result": {"handoff_action": ...}}}``). A
       candidate whose recursion surfaces ``handoff_action`` wins; otherwise the
       first plain parsed dict is remembered as a fallback so claude's
       plain-string-result path (a bare ``{"status": ...}`` under ``result``)
       does not regress.
    3. Last resort: every string value is scanned for embedded JSON. Brace-
       *balanced* objects are preferred (and one carrying ``handoff_action`` is
       returned first) over the greedy first-``{``-to-last-``}`` scan, which
       mismatches across sibling objects and loses a recoverable payload.

    Falls back to the original ``response`` only when nothing parseable is found.
    """
    if "handoff_action" in response:
        return response

    fallback: dict[str, Any] | None = None

    # Common envelope keys.
    for key in ("result", "content", "response"):
        candidate = response.get(key)
        if isinstance(candidate, dict) and "handoff_action" in candidate:
            return candidate
        if isinstance(candidate, str):
            try:
                parsed = json.loads(candidate)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(parsed, dict):
                unwrapped = extract_result_payload(parsed)
                if "handoff_action" in unwrapped:
                    return unwrapped
                if fallback is None:
                    fallback = parsed

    # Last resort: look for an embedded JSON block in any string value. Prefer a
    # brace-balanced object (and one carrying ``handoff_action`` first) before
    # falling back to the greedy first-``{``-to-last-``}`` scan.
    for value in response.values():
        if not isinstance(value, str):
            continue
        matched_any = False
        for block in _iter_balanced_json_objects(value):
            try:
                parsed = json.loads(block)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            matched_any = True
            if "handoff_action" in parsed:
                return parsed
            if fallback is None:
                fallback = parsed
        if not matched_any:
            embedded = find_embedded_json_object(value)
            if embedded:
                try:
                    parsed = json.loads(embedded)
                except (json.JSONDecodeError, TypeError):
                    parsed = None
                if isinstance(parsed, dict):
                    if "handoff_action" in parsed:
                        return parsed
                    if fallback is None:
                        fallback = parsed

    if fallback is not None:
        return fallback
    return response


_USAGE_TOKEN_KEYS = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)


def _as_int(value: Any) -> int:
    """Coerce a usage token field to an int, mapping ``None`` (explicit JSON
    ``null``) to ``0``.

    The CLI may emit an explicit ``null`` for a token field; ``dict.get(k, 0)``
    then returns ``None`` and the subsequent ``+`` raises ``TypeError``. Absent
    keys (already defaulted to ``0`` by the caller) and normal ints pass through
    unchanged.
    """
    return 0 if value is None else value


def normalize_cli_usage(response: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a CLI ``--output-format json`` ``usage`` block.

    The CLI JSON output may contain a ``usage`` key at the top level with
    ``input_tokens`` / ``output_tokens`` (plus optional cache counts). We
    normalize this into the same ``{last: {...}, total: {...}}`` shape used by
    the codex-subagent bridge so the downstream observability pipeline handles
    it uniformly.

    Only the claude-style snake_case token keys are recognized. A *non-empty*
    usage dict that carries none of the recognized keys (e.g. grok's unverified
    ``promptTokens`` / ``completionTokens``) is unrecognized telemetry: we return
    ``None`` rather than fabricate an all-zeros breakdown falsely stamped
    ``usage_source='observed'``. An empty usage dict keeps the historical
    absent-keys-default-to-zero behavior.
    """
    usage = response.get("usage")
    if not isinstance(usage, dict):
        return None
    if usage and not any(key in usage for key in _USAGE_TOKEN_KEYS):
        return None
    input_tokens = _as_int(usage.get("input_tokens", 0))
    output_tokens = _as_int(usage.get("output_tokens", 0))
    cache_read = _as_int(usage.get("cache_read_input_tokens", 0))
    cache_creation = _as_int(usage.get("cache_creation_input_tokens", 0))
    total_tokens = input_tokens + output_tokens
    breakdown = {
        "cached_input_tokens": cache_read + cache_creation,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "reasoning_output_tokens": 0,
        "total_tokens": total_tokens,
    }
    return {
        "last": breakdown,
        "total": breakdown,
        "model_context_window": None,
        "usage_source": "observed",
    }
