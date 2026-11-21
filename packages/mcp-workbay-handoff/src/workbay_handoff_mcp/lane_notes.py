"""Preserve the producer's JSON envelope when recording lane close prose."""

from __future__ import annotations

import json
import re
from typing import Any

REGISTRY_CORRECTION_KEY = "registry_correction"
# The registry and review engine own these fields (review_ratchet.py).
_RESERVED_FIELDS = ("task_subject", REGISTRY_CORRECTION_KEY, "listed_finding_ids", "prior_reviewed_tip")
_CLOSE_NOTES_KEY = "close_notes"
_MEMBER_SEPARATOR = re.compile(r"[ \t\r\n]*[{:,][ \t\r\n]*")


class ReservedLaneNotesError(ValueError):
    def __init__(self, fields: list[str]) -> None:
        self.fields = fields
        super().__init__(f"Lane close cannot write reserved notes field(s): {', '.join(fields)}.")


def notes_envelope(notes: object) -> dict[str, Any] | None:
    if not isinstance(notes, str) or not notes.strip().startswith("{"):
        return None
    try:
        decoded = json.loads(notes)
    except ValueError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _updated_envelope(raw: str, envelope: dict[str, Any], updates: dict[str, Any]) -> str:
    # Walk validated top-level members to replace changed values individually.
    # For legacy duplicate keys, replace the last occurrence that readers see.
    decoder = json.JSONDecoder()
    opening = _MEMBER_SEPARATOR.match(raw)
    assert opening is not None
    key_start = opening.end()
    spans: dict[str, tuple[int, int]] = {}
    while raw[key_start] != "}":
        key, key_end = decoder.raw_decode(raw, key_start)
        colon = _MEMBER_SEPARATOR.match(raw, key_end)
        assert colon is not None
        value_start = colon.end()
        _, value_end = decoder.raw_decode(raw, value_start)
        if key in updates:
            spans[key] = (value_start, value_end)
        comma = _MEMBER_SEPARATOR.match(raw, value_end)
        if comma is None:
            break
        key_start = comma.end()

    parts = []
    cursor = 0
    for key, (start, end) in sorted(spans.items(), key=lambda item: item[1]):
        parts.extend((raw[cursor:start], json.dumps(updates[key])))
        cursor = end
    end = raw.rindex("}")
    parts.append(raw[cursor:end])
    additions = [f"{json.dumps(key)}: {json.dumps(value)}" for key, value in updates.items() if key not in spans]
    if additions:
        if envelope:
            parts.append(", ")
        parts.append(", ".join(additions))
    parts.append(raw[end:])
    return "".join(parts)


def merge_lane_notes(existing: str | None, note: str | None, *, append_to_prose: bool = False) -> str | None:
    """Record a close note or receipt audit, preserving the other raw JSON members."""
    if note is None:
        return existing

    envelope = notes_envelope(existing)
    incoming = notes_envelope(note)
    durable = envelope or {}
    reserved = (*_RESERVED_FIELDS, "close_authority") if "close_authority" in durable else _RESERVED_FIELDS
    forged = sorted(
        key
        for key in reserved
        if incoming is not None and key in incoming and (key not in durable or incoming[key] != durable[key])
    )
    if forged:
        raise ReservedLaneNotesError(forged)
    if envelope is None:
        return f"{existing}\n{note}" if append_to_prose and existing else note

    assert existing is not None
    if incoming is not None:
        # Existing structured close callers carry metadata such as disposition_pending.
        updates = {key: value for key, value in incoming.items() if key not in envelope or value != envelope[key]}
        return _updated_envelope(existing, envelope, updates) if updates else existing

    prior = envelope.get(_CLOSE_NOTES_KEY)
    entries = prior if isinstance(prior, list) else ([] if prior is None else [prior])
    return _updated_envelope(existing, envelope, {_CLOSE_NOTES_KEY: [*entries, note]})
