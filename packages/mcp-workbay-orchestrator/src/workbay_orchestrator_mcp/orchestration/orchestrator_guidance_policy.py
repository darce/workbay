#!/usr/bin/env python3
from __future__ import annotations

from typing import Any


def _normalized_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip().lower() for item in value if str(item).strip()]


def resolve_assignment(
    task_ref: str,
    lane_id: str,
    text: str,
    activity: dict[str, Any],
) -> tuple[str, str] | None:
    """Return a task-specific fallback assignment, or ``None``.

    This module intentionally keeps task-specific heuristics out of the
    generic orchestrator daemon. The daemon should prefer lane-stamped
    pending actions first; this policy hook is only a fallback.
    """
    from lane_manifest import guidance_fallbacks

    normalized = text.lower()
    for fallback in guidance_fallbacks(task_ref, lane_id):
        subject = str(fallback.get("subject", "")).strip()
        message = str(fallback.get("message", "")).strip()
        if not subject or not message:
            continue

        match_any = _normalized_list(fallback.get("match_any"))
        match_all = _normalized_list(fallback.get("match_all"))
        if match_all and not all(marker in normalized for marker in match_all):
            continue
        if match_any and not any(marker in normalized for marker in match_any):
            continue
        if match_any or match_all:
            return (subject, message)

    return None
