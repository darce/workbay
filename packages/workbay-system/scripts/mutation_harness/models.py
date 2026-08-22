"""Stable data structures for the mutation-guard harness."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

MutantStatus = Literal["killed", "survived", "hung", "error"]


@dataclass(frozen=True, slots=True)
class Mutant:
    """One targeted mutant from a consumer-supplied manifest entry."""

    id: str
    target: str
    mutation: dict[str, Any]
    tests: tuple[str, ...] = ()
    allowed_survivor: bool = False
    allowed_survivor_rationale: str | None = None
    timeout: float | None = None
    expected_duration: float | None = None


@dataclass(slots=True)
class MutantResult:
    """Outcome of running a single mutant."""

    mutant_id: str
    status: MutantStatus
    killing_tests: list[str] = field(default_factory=list)
    duration: float = 0.0
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mutant_id": self.mutant_id,
            "status": self.status,
            "killing_tests": list(self.killing_tests),
            "duration": self.duration,
            "error_message": self.error_message,
        }


@dataclass(slots=True)
class BaselineReport:
    """Node-ID baseline reconciliation result."""

    ok: bool
    expected_count: int = 0
    observed_count: int = 0
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "expected_count": self.expected_count,
            "observed_count": self.observed_count,
            "added": list(self.added),
            "removed": list(self.removed),
            "message": self.message,
        }


@dataclass(slots=True)
class SweepVerdict:
    """Machine-readable sweep outcome (results in manifest order)."""

    schema_version: int = 1
    results: list[MutantResult] = field(default_factory=list)
    baseline: BaselineReport | None = None
    jobs: int = 1
    default_timeout: float = 60.0
    full_suite: bool = False
    schedule_mode: str = "manifest"  # "manifest" | "lpt"
    exit_code: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "results": [r.to_dict() for r in self.results],
            "baseline": self.baseline.to_dict() if self.baseline else None,
            "jobs": self.jobs,
            "default_timeout": self.default_timeout,
            "full_suite": self.full_suite,
            "schedule_mode": self.schedule_mode,
            "exit_code": self.exit_code,
        }

    def to_jsonable(self) -> dict[str, Any]:
        return self.to_dict()


def compute_exit_code(
    results: list[MutantResult],
    mutants: list[Mutant],
    baseline: BaselineReport | None,
) -> int:
    """Exit 0 only when every non-allowed mutant is killed AND baseline reconciles.

    hung and error always fail closed (non-zero). allowed survivors may report
    survived without failing the gate.
    """
    if baseline is not None and not baseline.ok:
        return 1
    by_id = {m.id: m for m in mutants}
    for r in results:
        if r.status in ("hung", "error"):
            return 1
        if r.status == "survived":
            m = by_id.get(r.mutant_id)
            if m is None or not m.allowed_survivor:
                return 1
        # killed is fine; unknown statuses fail closed
        if r.status not in ("killed", "survived", "hung", "error"):
            return 1
    return 0
