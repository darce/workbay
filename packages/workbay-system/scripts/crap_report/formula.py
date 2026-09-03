"""CRAP formula: Change Risk Anti-Patterns score per method."""

from __future__ import annotations


def compute_crap(comp: int, cov: float) -> float:
    """Return CRAP(m) = comp² × (1 − cov/100)³ + comp.

    Parameters
    ----------
    comp:
        Cyclomatic complexity (non-negative integer).
    cov:
        Coverage percentage; clamped to [0, 100].
    """
    if comp < 0:
        raise ValueError(f"comp must be non-negative, got {comp}")
    c = max(0.0, min(100.0, float(cov)))
    uncovered = 1.0 - (c / 100.0)
    return float(comp) ** 2 * uncovered**3 + float(comp)
