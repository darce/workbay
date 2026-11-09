"""Production handoff reader for ``render_task_as_canvas`` (BR-01 fix).

:func:`default_handoff_reader` calls ``workbay_handoff_mcp.get_handoff_state``
over the public ``workbay-handoff-mcp`` package surface and adapts the
envelope dict into the ``{"task_ref": str, "decisions": list[dict]}``
shape consumed by :func:`build_canvas_from_handoff`.

The import-time dependency on ``workbay_handoff_mcp`` is bound at module
load. Failures (network, DB unavailable, missing row) surface as an
``ok=False`` envelope from ``get_handoff_state``; the adapter degrades
to an empty-decisions snapshot so ``render_task_as_canvas`` still emits
a lone-task-node Canvas instead of raising.
"""

from __future__ import annotations

from typing import Any

from workbay_handoff_mcp import get_handoff_state


def default_handoff_reader(task_ref: str) -> dict[str, Any]:
    """Read handoff state for ``task_ref`` via the public package API.

    Returns a snapshot in the shape consumed by
    :func:`build_canvas_from_handoff`: ``{"task_ref": ..., "decisions": [...]}``.
    Each decision row carries at least a ``decision`` id string;
    additional fields are passed through unchanged.

    When the underlying call reports ``ok=False`` (e.g. unknown
    ``task_ref``, ambiguous workspace), the adapter returns an
    empty-decisions snapshot so the renderer still produces a valid
    lone-task-node Canvas.
    """

    envelope = get_handoff_state(task_ref=task_ref, sections="decisions_recent")
    if not envelope.get("ok"):
        return {"task_ref": task_ref, "decisions": []}
    data = envelope.get("data") or {}
    decisions = data.get("decisions_recent") or []
    return {"task_ref": task_ref, "decisions": list(decisions)}
