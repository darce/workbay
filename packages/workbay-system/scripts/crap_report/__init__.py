"""CRAP (Change Risk Anti-Patterns) scoring library.

Pure mechanism for ranking methods by change risk from cyclomatic complexity
and test coverage. CLI/MCP adapters live outside this package surface.
"""

from __future__ import annotations

from crap_report.formula import compute_crap
from crap_report.join import score_methods
from crap_report.models import CrapReport, MethodScore, MethodUnit

__all__ = [
    "CrapReport",
    "MethodScore",
    "MethodUnit",
    "compute_crap",
    "score_methods",
]
