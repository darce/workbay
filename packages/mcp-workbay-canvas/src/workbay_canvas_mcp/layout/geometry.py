"""Shared geometry helpers for canvas-mcp layout algorithms.

Pure functions only — no I/O, no global state. Kept separate from the
per-algorithm modules so hierarchical (Sugiyama) and force-directed
(Fruchterman-Reingold, implementation note) can share coordinate math without
importing each other.
"""

from __future__ import annotations

from collections.abc import Sequence


def median(values: Sequence[float]) -> float:
    """Return the median of ``values``.

    Even-length inputs return the average of the two middle elements.
    Raises ``ValueError`` on an empty sequence so callers do not silently
    rank a node with no positioned parents.
    """

    if not values:
        raise ValueError("median() requires at least one value")
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0
