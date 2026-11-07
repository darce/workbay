"""Read-only native evidence bridge; never a review-cycle grant or resolver.

A host must provide its current trusted producer, not a JSON observation file.
The public runtime has no such producer by default and returns a named hold.
R0 Allow.effects_authorized remains False even after qualified observation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from workbay_protocol.resource_lifecycle import (
    Admission,
    ExecutionObservation,
    ExecutionRequest,
    GitSource,
    Held,
    Hold,
    Proof,
    ResourceIdentity,
    admit_execution,
)

from .resource_dependencies import proof_problem


@dataclass(frozen=True)
class NativeIdentity:
    task_ref: str
    lane_id: str
    lane_row_id: int
    identity_seq: int
    cycle_epoch: int
    source_ordinal: int
    branch: str
    source: GitSource
    policy_version: str

    def __post_init__(self):
        if any(
            type(getattr(self, n)) is not int or getattr(self, n) < 1
            for n in ("lane_row_id", "identity_seq", "cycle_epoch", "source_ordinal")
        ):
            raise ValueError("native_identity_unknown")
        if any(not getattr(self, n).strip() for n in ("task_ref", "lane_id", "branch", "policy_version")):
            raise ValueError("native_identity_unknown")


@dataclass(frozen=True)
class NativeEvidence:
    identity: NativeIdentity
    observation: ExecutionObservation
    # These proofs come from the existing native producer in its own authority.
    # Neither a current HEAD alone nor a catalogue listing proves this history.
    source_history: Proof
    preflight: Proof
    policy: Proof
    failure_reason: str | None = None
    holds: tuple[str, ...] = ()


class NativeProducer(Protocol):
    def current(self, identity: NativeIdentity) -> NativeEvidence:
        """Read current task/lane/checkout/model/preflight evidence without effects."""
        ...


@dataclass(frozen=True)
class NativeAdmissionResult:
    outcome: Admission
    observation: ExecutionObservation
    holds: tuple[str, ...]
    native_failure_reason: str | None


class NativeAdmissionBridge:
    def __init__(self, producer: NativeProducer | None = None):
        self.producer = producer

    def verify_current(self, context=None, *, now):
        """Host-only current request/identity resolver; no wire observations.

        The existing native owner must install this resolver after resolving
        its own subject. This bridge neither chooses a subject nor rewrites it.
        """
        if self.producer is None:
            return self._held("native_producer_unavailable")
        if context is None:
            return self._held("native_context_unavailable")
        try:
            request, expected = context.current()
            if not isinstance(request, ExecutionRequest) or not isinstance(expected, NativeIdentity):
                return self._held("native_context_unqualified")
            return self.observe(request, expected, now=now)
        except Exception:
            return self._held("native_context_unreadable")

    def observe(self, request: ExecutionRequest, expected: NativeIdentity, *, now: datetime) -> NativeAdmissionResult:
        if self.producer is None:
            return self._held("native_producer_unavailable")
        try:
            native = self.producer.current(expected)
        except Exception:
            return self._held("native_evidence_unreadable")
        if not isinstance(native, NativeEvidence):
            return self._held("native_evidence_unqualified")
        observed = ExecutionObservation.model_validate(native.observation.model_dump(mode="json"))
        holds = list(native.holds)
        if native.failure_reason:
            holds.append("native_execution_refused")
        if native.identity != expected:
            holds.append("native_identity_changed")
        wb = request.correlation.workbay
        if wb is None or wb.task != expected.task_ref or wb.lane != expected.lane_id:
            holds.append("native_task_lane_mismatch")
        if request.source != expected.source or observed.source != expected.source:
            holds.append("native_source_changed")
        for name in ("source_history", "preflight", "policy"):
            if problem := proof_problem(getattr(native, name), now=now):
                holds.append("native_" + name + "_" + problem)
        outcome = admit_execution(request, observed, now=now)
        if holds:
            # Native holds survive even if R0's independent comparisons pass.
            base = outcome.holds if isinstance(outcome, Held) else ()
            outcome = Held(scope="execution", holds=(*base, Hold(code="proof_unknown", subject="native.qualification")))
        return NativeAdmissionResult(outcome, observed, tuple(dict.fromkeys(holds)), native.failure_reason)

    def _held(self, reason):
        return NativeAdmissionResult(
            Held(scope="execution", holds=(Hold(code="proof_unknown", subject="native.execution"),)),
            ExecutionObservation(),
            (reason,),
            None,
        )


@dataclass(frozen=True)
class ResourceEffectBinding:
    """Out-of-band permission from the authentic host caller, never wire input.

    The producer separately proves the exact resource and allowed operation;
    native model eligibility alone is insufficient effect permission.
    """

    resource: ResourceIdentity
    source: GitSource
    operation: str
    policy_version: str
    native_identity: NativeIdentity
    authorization: Proof
    device: int
    inode: int


class NativeResourceAuthorizer:
    def __init__(self, *, bridge, request, expected, resource_producer):
        self.bridge, self.request, self.expected = bridge, request, expected
        self.resource_producer = resource_producer

    def authorize(self, plan, probe, *, now):
        result = self.bridge.observe(self.request, self.expected, now=now)
        if isinstance(result.outcome, Held) or result.holds:
            return False
        binding = self.resource_producer.current(plan.registration, self.expected)
        return (
            isinstance(binding, ResourceEffectBinding)
            and binding.native_identity == self.expected
            and binding.resource == plan.registration.resource
            and binding.source == plan.registration.source
            and binding.operation == plan.registration.operation
            and binding.policy_version == plan.registration.policy_version == self.expected.policy_version
            and proof_problem(binding.authorization, now=now) is None
            and self.resource_producer.matches_descriptor(binding)
        )

    def authorize_recovery(self, record, *, now):
        # Recovery authority is separately qualified; target absence grants none.
        return self.resource_producer.authorize_recovery(record, self.expected, now=now) is True
