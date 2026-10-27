"""Classify every offload pass outcome for design D2.

Unknown outcomes fail closed as verdicts and consume review budget.
"""

from collections.abc import Mapping
from enum import Enum
from types import MappingProxyType

from workbay_orchestrator_mcp.orchestration.offload_pass import OffloadOutcome
from workbay_orchestrator_mcp.orchestration.preflight_attestation import STALE_WORKER_CONFIGURATION_REASON


class PassOutcomeClass(str, Enum):
    """Whether a pass stopped on infrastructure, was judged, or is unfinished."""

    INFRA_STOP = "infra_stop"
    VERDICT = "verdict"
    PENDING = "pending"


OUTCOME_CLASS: Mapping[str, PassOutcomeClass] = MappingProxyType({
    OffloadOutcome.HANDOFF_READY.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.REVIEW_COMPLETE.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.ESCALATED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.NEEDS_GUIDANCE.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.RATE_LIMITED.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.TRANSPORT_FAILURE.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.COMPLETED_UNREVIEWED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.NO_ACTIONABLE_WORK.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.UNCOMMITTED_WORK.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.TOKEN_BUDGET_EXCEEDED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.TIMEOUT.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.ERROR.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.STILL_RUNNING.value: PassOutcomeClass.PENDING,
    OffloadOutcome.LANE_NOT_FOUND.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.SELF_VERIFY_FAILED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.SELF_VERIFY_INCONCLUSIVE.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.COMPOSER_VIOLATION_QUARANTINED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.CHECKPOINT.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.SERVER_STALE_RESTART_REQUIRED.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.REMOTE_JOB_PENDING.value: PassOutcomeClass.PENDING,
    OffloadOutcome.ADMISSION_DEFERRED.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.ADMISSION_REFUSED.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.DISPATCH_REFUSED.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.REMOTE_REQUIRED.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.WORKTREE_UNRECOVERABLE.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.WORKTREE_CLAIM_HELD.value: PassOutcomeClass.INFRA_STOP,
    OffloadOutcome.CEREMONY_FAILED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.REVIEW_INCOMPLETE.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.VERIFICATION_FAILED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.VERIFICATION_SKIPPED.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.VERIFICATION_HARNESS_ERROR.value: PassOutcomeClass.VERDICT,
    OffloadOutcome.GUIDANCE_CONTRACT_INVALID.value: PassOutcomeClass.VERDICT,
})

INFRA_REFUSAL_REASONS: frozenset[str] = frozenset({
    STALE_WORKER_CONFIGURATION_REASON,
    "catalogue_unavailable",
})


def classify_pass_outcome(outcome: str, *, refusal_reason: str | None = None) -> PassOutcomeClass:
    """Apply pre-spawn infrastructure refusals before the fail-closed lookup."""
    if refusal_reason in INFRA_REFUSAL_REASONS:
        return PassOutcomeClass.INFRA_STOP
    return OUTCOME_CLASS.get(outcome, PassOutcomeClass.VERDICT)
