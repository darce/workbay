"""Parallel mutation-guard verification harness (mechanism only).

Consumer supplies domain mutants via a manifest; this package owns sandboxing,
LPT-scheduled parallel sweep, per-mutant timeouts, JSON verdicts, and node-ID
baseline adjudication. Domain mutant content and allowed-survivor policy stay
in the consumer.
"""

from __future__ import annotations

from mutation_harness.models import Mutant, MutantResult, SweepVerdict

__all__ = ["Mutant", "MutantResult", "SweepVerdict"]
