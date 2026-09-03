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

SHAPED-PAYLOAD RECOVERY CONTRACT v1 (orchestrator half; VM sibling owns the
script side) lives here so the two Python scanners cannot drift [REF-26]:

1. SCANNING — a ``{`` that never balances MUST NOT abandon the scan.
2. SHAPE — ``handoff_action`` in the known enum OR list-valued ``findings``.
3. SELECTION — among shaped objects, last in text order wins.
4. OBSERVABILITY — non-strict recovery stamps a tier flag and logs a warning.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from typing import Any

_LOGGER = logging.getLogger(__name__)

#: Known ``handoff_action`` enum values (worker reporting contract). Key presence
#: alone is NOT shape — null / "" / unknown values are rejected (contract §2).
KNOWN_HANDOFF_ACTIONS = frozenset({"merge_ready", "needs_guidance"})

#: Public recovery-tier key stamped onto payloads recovered by non-strict parse
#: (contract §4 / DURREV-RP-F5). Absent when a strict top-level parse won.
SHAPED_PAYLOAD_RECOVERY_KEY = "shaped_payload_recovery"

#: Tier names written into :data:`SHAPED_PAYLOAD_RECOVERY_KEY`.
RECOVERY_TIER_BALANCED = "balanced"
RECOVERY_TIER_EMBEDDED = "embedded"
#: Parsed a well-formed dict that failed the shape gate (off-enum / null /
#: absent ``handoff_action`` without list-valued ``findings``). Selection keeps
#: the payload so a committed turn's summary/tests_run survive; callers clamp
#: ``handoff_action`` fail-closed rather than treating this as unparseable.
RECOVERY_TIER_UNSHAPED = "unshaped"

#: Keys that distinguish a worker/result report from a CLI envelope wrapper.
#: Used only for the unshaped envelope-root fallthrough so a session/usage
#: envelope is never promoted as a result when every tier failed.
_RESULTISH_KEYS = frozenset(
    {
        "handoff_action",
        "findings",
        "summary",
        "tests_run",
        "blockers",
        "details",
        "merge_ready",
        "changed_files",
    }
)


def is_shaped_result_payload(d: dict[str, Any]) -> bool:
    """Return True when ``d`` is a SHAPED result/review payload (contract §2).

    An object is shaped if it carries EITHER:

    (a) a ``handoff_action`` key whose value is a non-empty string belonging to
        :data:`KNOWN_HANDOFF_ACTIONS`, OR
    (b) a ``findings`` key whose value is a list.

    Key presence alone is not sufficient: ``handoff_action`` of null, ``""``, or
    an unknown value is not shaped. Aligns orchestrator selection with the VM
    gate so a findings-only review payload is not destroyed (DURREV-VM-F7).
    """
    action = d.get("handoff_action")
    if isinstance(action, str) and action in KNOWN_HANDOFF_ACTIONS:
        return True
    if isinstance(d.get("findings"), list):
        return True
    return False


def stamp_recovery_tier(payload: dict[str, Any], tier: str) -> dict[str, Any]:
    """Return a shallow copy of ``payload`` stamped with the recovery tier.

    Always logs a warning so recovery is never silent [OBS-08] / [AGT-10] /
    contract §4. Callers pass the returned dict onward as the emitted payload.
    """
    out = dict(payload)
    out[SHAPED_PAYLOAD_RECOVERY_KEY] = tier
    _LOGGER.warning(
        "shaped payload recovered via non-strict parse (tier=%s)",
        tier,
    )
    return out


def is_resultish_payload(d: dict[str, Any]) -> bool:
    """Return True when ``d`` carries worker/result report keys (not a CLI shell)."""
    return any(k in d for k in _RESULTISH_KEYS)


def handoff_action_needs_clamp(payload: dict[str, Any]) -> bool:
    """Return True when the caller must fail-closed-clamp ``handoff_action``.

    Canonical rule lives on ``backend_adapter`` so ``from_dict`` and the
    adapter clamp share one verdict: absent/null/blank is defaulted with no
    blocker; only a present non-enum value (or unshaped recovery) is invalid.
    """
    from ..backend_adapter import (
        handoff_action_needs_clamp as _core_handoff_action_needs_clamp,
    )

    return _core_handoff_action_needs_clamp(payload)


def recover_unshaped_payload(
    texts: list[str],
    *,
    text_dicts_fn: Any,
    envelope: dict[str, Any] | None,
    loads_dict_fn: Any,
) -> dict[str, Any] | None:
    """After shaped selection failed, return a well-formed dict if any was parsed.

    Prefer the schema-forced ``structuredOutput`` channel over narrated text
    (OFFLOAD-RESULT-TEXT-ACTION-SPLITBRAIN-02), then generic dict-valued envelope
    fields, then narrated text last-wins, then a resultish envelope root —
    **without** the shape filter. Stamps :data:`RECOVERY_TIER_UNSHAPED` so
    callers can preserve summary/tests_run while clamping the action fail-closed.
    Returns None only when nothing parseable was found (true unparseable).
    """
    if envelope is not None:
        structured = envelope.get("structuredOutput")
        if isinstance(structured, dict):
            return stamp_recovery_tier(structured, RECOVERY_TIER_UNSHAPED)
        if isinstance(structured, str):
            candidate = loads_dict_fn(structured)
            if isinstance(candidate, dict):
                return stamp_recovery_tier(candidate, RECOVERY_TIER_UNSHAPED)
        for key in ("result", "content", "output", "message"):
            value = envelope.get(key)
            if isinstance(value, dict):
                return stamp_recovery_tier(value, RECOVERY_TIER_UNSHAPED)
    for text in texts:
        dicts = [d for d in text_dicts_fn(text) if isinstance(d, dict)]
        if dicts:
            return stamp_recovery_tier(dicts[-1], RECOVERY_TIER_UNSHAPED)
    if envelope is not None and is_resultish_payload(envelope):
        return stamp_recovery_tier(envelope, RECOVERY_TIER_UNSHAPED)
    return None


def find_embedded_json_object(text: str) -> str | None:
    """Return the first-``{``-to-last-``}`` JSON substring of ``text``, or None.

    Greedy scan (``re.DOTALL``): matches from the first opening brace to the
    last closing brace. This is the historical claude fallback behavior and is
    preserved exactly; callers that need fenced-block preference or balanced
    matching layer that on top.
    """
    match = re.search(r"(\{.*\})", text, re.DOTALL)
    return match.group(1) if match else None


def _iter_balanced_json_objects(text: str) -> Iterator[str]:
    """Yield each brace-balanced ``{...}`` substring in ``text``, left to right.

    Escape-aware string handling: braces inside JSON string literals are not
    structural. A ``{`` that never balances does NOT abandon the scan
    (SHAPED-PAYLOAD RECOVERY CONTRACT v1 §1 / DURREV-RP-F3): advance to that
    brace's offset + 1 and continue scanning so a banner brace cannot hide a
    later well-formed object.
    """
    i = 0
    n = len(text)
    while i < n:
        start = text.find("{", i)
        if start == -1:
            return
        depth = 0
        in_str = False
        escaped = False
        closed_at: int | None = None
        for j in range(start, n):
            ch = text[j]
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
                    closed_at = j
                    break
        if closed_at is not None:
            yield text[start : closed_at + 1]
            i = closed_at + 1
        else:
            # Unbalanced brace: advance past it and keep scanning (contract §1).
            i = start + 1


def find_first_balanced_json(text: str) -> str | None:
    """Return the first brace-balanced ``{...}`` substring of ``text``, or None.

    Unlike :func:`find_embedded_json_object` (greedy first-{ to last-}), this
    tracks brace depth and string context so it returns the FIRST complete
    object. Unbalanced leading braces are skipped (contract §1) rather than
    abandoning the remainder of the text.
    """
    for block in _iter_balanced_json_objects(text):
        return block
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


def select_last_shaped_payload(
    text: str,
    *,
    shaped: Any | None = None,
) -> dict[str, Any] | None:
    """Scan ``text`` for balanced JSON dicts and return the last shaped one.

    Uses :func:`_iter_balanced_json_objects` (contract §1) and selects the last
    object for which ``shaped(d)`` is true (default:
    :func:`is_shaped_result_payload`). On success stamps
    :data:`RECOVERY_TIER_BALANCED` (contract §4). Returns None when nothing
    shaped is found.
    """
    predicate = shaped if shaped is not None else is_shaped_result_payload
    last: dict[str, Any] | None = None
    for block in _iter_balanced_json_objects(text):
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and predicate(parsed):
            last = parsed
    if last is None:
        return None
    return stamp_recovery_tier(last, RECOVERY_TIER_BALANCED)


def extract_result_payload(response: dict[str, Any]) -> dict[str, Any]:
    """Extract the worker result payload from a CLI JSON envelope.

    When ``--output-format json`` is used, the assistant's ``BackendResult`` may
    arrive in several shapes; the search order is honest about how each is
    unwrapped:

    1. The response itself is a shaped payload (contract §2) — returned
       directly.
    2. It is nested under a ``result`` / ``content`` / ``response`` envelope key,
       either as a dict (returned when shaped) or as a JSON *string*. A string
       candidate is parsed and, when it yields a dict, this function **recurses**
       so a deeper shaped payload is surfaced. A candidate whose recursion
       surfaces a shaped payload wins; otherwise the first plain parsed dict is
       remembered as a fallback so claude's plain-string-result path (a bare
       ``{"status": ...}`` under ``result``) does not regress.
    3. Last resort: every string value is scanned for embedded JSON. Brace-
       *balanced* objects are preferred (and a shaped one is returned first,
       last-wins among shaped per contract §3) over the greedy
       first-``{``-to-last-``}`` scan.

    Falls back to the original ``response`` only when nothing parseable is found.
    """
    if is_shaped_result_payload(response):
        return response

    fallback: dict[str, Any] | None = None

    # Common envelope keys.
    for key in ("result", "content", "response"):
        candidate = response.get(key)
        if isinstance(candidate, dict) and is_shaped_result_payload(candidate):
            return candidate
        if isinstance(candidate, str):
            try:
                parsed = json.loads(candidate)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(parsed, dict):
                unwrapped = extract_result_payload(parsed)
                if is_shaped_result_payload(unwrapped):
                    return unwrapped
                if fallback is None:
                    fallback = parsed

    # Last resort: look for an embedded JSON block in any string value. Prefer a
    # brace-balanced shaped object (last-wins) before falling back to the greedy
    # first-``{``-to-last-``}`` scan.
    for value in response.values():
        if not isinstance(value, str):
            continue
        matched_any = False
        last_shaped: dict[str, Any] | None = None
        for block in _iter_balanced_json_objects(value):
            try:
                parsed = json.loads(block)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            matched_any = True
            if is_shaped_result_payload(parsed):
                last_shaped = parsed
            elif fallback is None:
                fallback = parsed
        if last_shaped is not None:
            return stamp_recovery_tier(last_shaped, RECOVERY_TIER_BALANCED)
        if not matched_any:
            embedded = find_embedded_json_object(value)
            if embedded:
                try:
                    parsed = json.loads(embedded)
                except (json.JSONDecodeError, TypeError):
                    parsed = None
                if isinstance(parsed, dict):
                    if is_shaped_result_payload(parsed):
                        return stamp_recovery_tier(parsed, RECOVERY_TIER_EMBEDDED)
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
