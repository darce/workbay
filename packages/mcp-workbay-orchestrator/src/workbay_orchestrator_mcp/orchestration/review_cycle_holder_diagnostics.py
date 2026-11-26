"""Format evaluated review-cycle holders without reading or changing authority."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from copy import deepcopy
from hashlib import sha256
from typing import Any

_IDENTITY_FIELDS = ("dispatch_id", "lane_id", "task_ref", "source_branch", "subject_branch", "receipt")
_MAX_PAGE_SIZE = 50
_CURSOR = re.compile(r"v1\.([0-9a-f]{64})\.([1-9][0-9]{0,18})")
_ACTIONS = {
    "unknown": "Inspect the retained holder and its recorded evidence in a reconciliation preview.",
    "overlap": "Land, discard (lane_discarded_<task>_<lane>_<tip40>) or wait for the holder's unlanded commits on overlap_paths.",
    "missing_incoming_scope": "Provide the incoming task's authorized scope and rerun subject resolution.",
    "evidence_unavailable": "Check access to the recorded holder evidence and rerun subject resolution.",
}
_CODE_ALIASES = {
    "holder_scope_overlap": "overlap",
    "incoming_scope_missing": "missing_incoming_scope",
    "holder_evidence_unavailable": "evidence_unavailable",
    "holder_custody_unavailable": "evidence_unavailable",
    "review_cycle_holder_release_ledger_unavailable": "evidence_unavailable",
    # review_cycle_holds.holder_blocks reasons.
    "paths_overlap": "overlap",
    "holder_tip_unreadable": "evidence_unavailable",
    "integration_unreadable": "evidence_unavailable",
}


def _blocker(row: Mapping[str, Any]) -> str:
    code = row.get("holder_code")
    signals = (code,) if code is not None else (row.get("reason"), row.get("missing_proof"))
    for signal in signals:
        if isinstance(signal, str):
            blocker = _CODE_ALIASES.get(signal, signal)
            if blocker in _ACTIONS:
                return blocker
    return "unknown"


def format_holder_diagnostics(
    rows: Iterable[Mapping[str, Any]],
    *,
    limit: int = 10,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Return one page of retained holders and blocker counts across all rows.

    Pass JSON-compatible evaluated rows with the six holder identity fields.
    Each returned row keeps its raw fields and adds ``blocker`` and
    ``next_action``. A supplied ``holder_code`` takes precedence. Legacy
    ``reason`` and ``missing_proof`` are recognized by exact code only.
    The blocker values are ``unknown``, ``overlap``, ``missing_incoming_scope``,
    and ``evidence_unavailable``.

    ``limit`` must be positive and is capped at 50. Pass ``next_cursor`` back
    with the same evaluated rows (in any order) to read the next page. Invalid
    cursors and cursors for changed rows raise ``ValueError``.
    """
    if type(limit) is not int or limit < 1:
        raise ValueError("holder diagnostics limit must be a positive integer")
    limit = min(limit, _MAX_PAGE_SIZE)

    # Use the full identity to distinguish holders that share an individual ID.
    # Canonical row content breaks ties and binds the cursor to the evaluated input.
    ordered = []
    blocker_counts = dict.fromkeys(_ACTIONS, 0)
    for row in rows:
        identity = tuple(row.get(field) or "" for field in _IDENTITY_FIELDS)
        raw = json.dumps(dict(row), sort_keys=True, separators=(",", ":"))
        blocker = _blocker(row)
        ordered.append((identity, raw, blocker, row))
        blocker_counts[blocker] += 1
    ordered.sort(key=lambda entry: (entry[0], entry[1]))

    # Compact JSON escapes newlines within values, so each row occupies one line.
    snapshot = sha256()
    for _, raw, _, _ in ordered:
        snapshot.update(raw.encode("ascii") + b"\n")
    fingerprint = snapshot.hexdigest()
    total = len(ordered)
    offset = 0
    if cursor is not None:
        match = _CURSOR.fullmatch(cursor) if isinstance(cursor, str) else None
        if match is None or match[1] != fingerprint or int(match[2]) >= total:
            raise ValueError("invalid or stale holder diagnostics cursor")
        offset = int(match[2])

    end = min(offset + limit, total)
    holders = [
        dict(deepcopy(dict(row)), blocker=blocker, next_action=_ACTIONS[blocker])
        for _, _, blocker, row in ordered[offset:end]
    ]
    return {
        "holders": holders,
        "total_count": total,
        "returned_count": len(holders),
        "blocker_counts": blocker_counts,
        "next_cursor": f"v1.{fingerprint}.{end}" if end < total else None,
    }
