"""Status edges refused by the direct Handoff lane patch."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

# Decision 48437. The orchestrator upsert has a separate phase-one contract
# that permits and audits this edge. Direct patches must activate first.
_DENIED_DIRECT_EDGES = frozenset({("planned", "review")})


def direct_lane_status_error(*, task_ref: str, lane_id: str, source: str, target: str | None) -> dict[str, Any] | None:
    """Return a typed refusal for a denied edge without storing a receipt."""
    if (source, target) not in _DENIED_DIRECT_EDGES:
        return None
    return {
        "error_kind": "illegal_transition",
        "error": f"Direct lane status change {source!r} to {target!r} refused.",
        "receipt": {
            "schema": "workbay/illegal-transition@1",
            "task_ref": task_ref,
            "lane_id": lane_id,
            "from": source,
            "to": target,
            "chart_id": None,
            "chart_version": None,
            "enforced": True,
            "caller": "update_lane",
            "reason": "direct_edge_denied",
            "at": datetime.now(UTC).isoformat(),
        },
    }
