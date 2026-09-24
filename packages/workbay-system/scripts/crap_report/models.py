"""Stable data structures for CRAP reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

CoverageStatus = Literal["measured", "missing_file", "empty_range"]


@dataclass(frozen=True, slots=True)
class MethodUnit:
    """One complexity unit (function or method) from a CC collector."""

    file: str
    name: str
    line_start: int
    line_end: int
    comp: int


@dataclass(frozen=True, slots=True)
class MethodScore:
    """Scored method with coverage join metadata."""

    file: str
    name: str
    line_start: int
    line_end: int
    comp: int
    cov: float
    crap: float
    coverage_unknown: bool = False
    coverage_status: CoverageStatus = "measured"
    excluded: bool = False


@dataclass(slots=True)
class CrapReport:
    """Versioned report payload (schema_version=1)."""

    schema_version: int = 1
    formula: str = "comp**2 * (1 - cov/100)**3 + comp"
    threshold: float = 30.0
    coverage_kind: str = "line"
    provenance: dict = field(default_factory=dict)
    methods: list[MethodScore] = field(default_factory=list)
    # Informational high-CC rows omitted from ranking (unmeasured).
    unmeasured_high_cc: list[MethodScore] = field(default_factory=list)
    # Unmeasured units below unmeasured_cc_min dropped under measured_only.
    unmeasured_dropped_low_cc: int = 0

    @property
    def summary(self) -> dict[str, int | float]:
        crappy = sum(
            1 for m in self.methods if m.crap > self.threshold and not m.excluded
        )
        measured = sum(1 for m in self.methods if not m.coverage_unknown)
        return {
            "methods": len(self.methods),
            "crappy": crappy,
            "threshold": self.threshold,
            "measured": measured,
            "unmeasured_omitted": len(self.unmeasured_high_cc),
            "unmeasured_dropped_low_cc": self.unmeasured_dropped_low_cc,
            "unknown_in_rank": sum(1 for m in self.methods if m.coverage_unknown),
        }
