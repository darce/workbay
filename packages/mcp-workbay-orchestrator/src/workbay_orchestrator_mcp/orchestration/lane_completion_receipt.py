"""Map completion evidence and fence storage removal behind durable attention.

This adapter authorizes no Git action itself. Callers retain identity, expected
tip and custody fences. An actuator returns True only when storage was removed;
recovery uses its separate observation method and never repeats removal. Git
and SQLite are independent: a prepared warning survives a crash or a stale
post-removal authorization, and remains visible until explicitly discharged.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Protocol

from workbay_handoff_mcp import lane_completion_attention as journal
from workbay_handoff_mcp.lane_completion_recording import ContractRecord, canonical_hash
from workbay_handoff_mcp.shared_schema import _get_db_connection

from . import lane_completion as lc


class ReceiptOutcomeKind(str, Enum):
    NO_WARNING_NEEDED = "no_warning_needed"
    HELD_UNKNOWN = "held_unknown"
    HELD_RECEIPT_FAILED = "held_receipt_failed"
    HELD_AUTHORITY_STALE = "held_authority_stale"
    REMOVED_WITH_WARNING = "removed_with_warning"
    HELD_WITH_WARNING = "held_with_warning"
    REMOVED_AFTER_STALE_AUTHORITY = "removed_after_stale_authority"


@dataclass(frozen=True)
class ReceiptOutcome:
    kind: ReceiptOutcomeKind
    next_action: str
    authorization: journal.CompletionAuthorization | None = None
    refusal: journal.CompletionRefusal | None = None
    storage_removed: bool = False


class RecoverableActuator(Protocol):
    def __call__(self) -> bool: ...

    def observe_removed(self) -> bool: ...


class CompletionReceipt(journal.CompletionReceipt):
    schema_version: int
    candidate_sha: str
    integration_ref: str
    integration_sha: str
    landing_commit_sha: str | None
    evidence_refs: list[str]
    source_holds: list[str]
    custody_holds: list[str]


def build_completion_receipt(
    assessment: lc.CompletionAssessment,
    identity: lc.LaneIdentity,
    contract_record: ContractRecord,
    *,
    policy_action: str,
    snapshot: lc.CompletionSnapshot,
) -> CompletionReceipt:
    """Pure mapping; snapshot supplies facts absent from CompletionAssessment.

    Reuse the evaluator's canonical snapshot encoding to check the supplied
    assessment binding without rerunning its completion rules. Advisory policy
    text and clocks do not contribute to the assessment ID.
    """
    digest = canonical_hash(
        {"evaluator_version": 1, "snapshot": lc._canonical(snapshot), "verdict": assessment.verdict.value}
    )
    if digest != assessment.assessment_hash or assessment.phase != snapshot.lane.target.phase:
        raise ValueError("assessment does not match snapshot")
    scope = snapshot.lane.identity_scope
    if identity != scope.identity or asdict(identity) != contract_record.contract["identity"]:
        raise ValueError("identity does not match snapshot and recorded contract")
    if scope.contract is not None and asdict(scope.contract) != contract_record.contract:
        raise ValueError("contract does not match recorded contract")

    unmet = snapshot.lane.unmet
    obligations = unmet.obligations if unmet.state == lc.EvidenceState.PRESENT else ()
    assignments = {item["finding_id"]: item["db_id"] for item in contract_record.assigned_findings}
    finding_ids = sorted(assignments[item.id] for item in obligations if item.kind == lc.ObligationKind.FINDING)
    acceptance_ids = sorted({item.id for item in obligations if item.kind == lc.ObligationKind.ACCEPTANCE})
    positive = {
        lc.CompletionReason.KNOWN_UNMET,
        lc.CompletionReason.NEVER_PRODUCED,
        lc.CompletionReason.SOME_PRODUCTION,
        lc.CompletionReason.COMPLETED_CANDIDATE,
        lc.CompletionReason.COMPLETED_INTEGRATION,
        lc.CompletionReason.CARRIER_COMPLETED,
    }
    missing = {reason.value for reason in assessment.reasons if reason not in positive}
    carrier = None
    if assessment.verdict == lc.CompletionVerdict.SUPERSEDED:
        # The assessed transfer chain has already been validated by implementation note.
        # Follow only that chain and reuse its direct predicate check to find
        # the completed carrier, including multi-hop replacement.
        source = identity
        for _ in range(8):
            edge = next(edge for edge in snapshot.transfers if edge.source == source)
            carried = next(item for item in snapshot.carriers if item.lane.identity_scope.identity == edge.carrier)
            if not lc._direct_reasons(carried.lane):
                carrier = {
                    **asdict(edge.carrier),
                    "assessment_id": carried.assessment_id,
                    "landing_commit_sha": carried.lane.target.landing_sha,
                    "integration_ref": carried.lane.target.integration_ref,
                    "integration_sha": carried.lane.target.integration_sha,
                }
                break
            source = edge.carrier
    elif assessment.verdict != lc.CompletionVerdict.COMPLETED and scope.contract is not None:
        missing.update(reason.value for reason in lc._direct_reasons(snapshot.lane))
    target = snapshot.lane.target
    return CompletionReceipt(
        schema_version=1,
        assessment_id=assessment.assessment_hash,
        task_ref=identity.task_ref,
        lane_id=identity.lane_id,
        lane_row_id=identity.lane_row_id,
        identity_revision=str(identity.identity_seq) if identity.identity_seq is not None else identity.lane_revision,
        contract_revision=contract_record.revision,
        contract_hash=contract_record.contract_hash,
        phase=assessment.phase.value,
        candidate_sha=target.candidate_sha,
        integration_ref=target.integration_ref,
        integration_sha=target.integration_sha,
        landing_commit_sha=target.landing_sha,
        verdict=assessment.verdict.value,
        reason_codes=[reason.value for reason in assessment.reasons],
        unmet_finding_ids=finding_ids,
        unmet_acceptance_ids=acceptance_ids,
        missing_evidence=sorted(missing),
        evidence_refs=list(assessment.evidence_ids),
        carrier=carrier,
        policy_action=policy_action,
        source_holds=list(assessment.source_holds),
        custody_holds=list(assessment.custody_holds),
    )


def _held(
    refusal: journal.CompletionRefusal,
    next_action: str,
    authorization: journal.CompletionAuthorization | None = None,
) -> ReceiptOutcome:
    kind = (
        ReceiptOutcomeKind.HELD_AUTHORITY_STALE
        if refusal.code == journal.CompletionRefusalCode.AUTHORITY_STALE
        else ReceiptOutcomeKind.HELD_RECEIPT_FAILED
    )
    return ReceiptOutcome(kind, next_action, authorization, refusal)


def _stored_outcome(authorization: journal.CompletionAuthorization) -> str:
    # Prepare returns an authorization, not event state. Read the keyed journal
    # row directly: the attention inbox is paginated and omits resolved events,
    # so it cannot reliably distinguish replay from a new storage operation.
    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT assessment_id, obligation_fingerprint, contract_hash, storage_outcome "
            "FROM lane_completion_events WHERE event_id=?",
            (authorization.event_id,),
        ).fetchone()
    if row is None or any(
        row[key] != getattr(authorization, key) for key in ("assessment_id", "obligation_fingerprint", "contract_hash")
    ):
        raise journal.CompletionRefusal(journal.CompletionRefusalCode.AUTHORITY_STALE, "event authorization changed")
    return row["storage_outcome"]


def _record(
    authorization: journal.CompletionAuthorization,
    removed: bool,
    next_action: str,
) -> ReceiptOutcome:
    try:
        journal.record_completion_outcome(
            authorization.event_id, "storage_removed" if removed else "held", authorization
        )
    except journal.CompletionRefusal as refusal:
        if removed and refusal.code == journal.CompletionRefusalCode.AUTHORITY_STALE:
            return ReceiptOutcome(
                ReceiptOutcomeKind.REMOVED_AFTER_STALE_AUTHORITY,
                next_action,
                authorization,
                refusal,
                True,
            )
        # An outcome write failure after removal cannot truthfully be reported
        # as held. Leave the prepared warning pending and surface the failure.
        raise
    kind = ReceiptOutcomeKind.REMOVED_WITH_WARNING if removed else ReceiptOutcomeKind.HELD_WITH_WARNING
    return ReceiptOutcome(kind, next_action, authorization, storage_removed=removed)


def _replay(authorization: journal.CompletionAuthorization, next_action: str) -> ReceiptOutcome | None:
    outcome = _stored_outcome(authorization)
    if outcome == "storage_removed":
        return ReceiptOutcome(ReceiptOutcomeKind.REMOVED_WITH_WARNING, next_action, authorization, storage_removed=True)
    if outcome == "held":
        return ReceiptOutcome(ReceiptOutcomeKind.HELD_WITH_WARNING, next_action, authorization)
    return None


def with_completion_receipt(
    receipt: journal.CompletionReceipt,
    *,
    operation_intent: str,
    next_action: str,
    observer: str,
    actuator: Callable[[], bool],
) -> ReceiptOutcome:
    """Commit unfinished-work attention before invoking the storage actuator.

    Actuators report removal with the boolean True; other returns mean held.
    Reuse operation_intent on replay. After an uncertain crash, use recovery
    with observe_removed instead of invoking the actuator again.
    """
    verdict = receipt["verdict"]
    if verdict in (lc.CompletionVerdict.COMPLETED, lc.CompletionVerdict.SUPERSEDED):
        removed = actuator() is True
        return ReceiptOutcome(ReceiptOutcomeKind.NO_WARNING_NEEDED, next_action, storage_removed=removed)
    if verdict not in (lc.CompletionVerdict.UNFINISHED_PARTIAL, lc.CompletionVerdict.UNFINISHED_NEVER_PRODUCED):
        return ReceiptOutcome(ReceiptOutcomeKind.HELD_UNKNOWN, next_action)
    authorization = None
    try:
        fingerprint = journal.get_obligation_fingerprint(receipt["task_ref"], receipt["lane_id"])
        authorization = journal.prepare_completion_warning(
            receipt,
            operation_intent=operation_intent,
            obligation_fingerprint=fingerprint,
            next_action=next_action,
            observer=observer,
        )
        replay = _replay(authorization, next_action)
        if replay is not None:
            return replay
        if fingerprint != authorization.obligation_fingerprint:
            raise journal.CompletionRefusal(
                journal.CompletionRefusalCode.AUTHORITY_STALE,
                "prepared operation obligations changed",
            )
    except journal.CompletionRefusal as refusal:
        return _held(refusal, next_action, authorization)
    except Exception as exc:
        refusal = journal.CompletionRefusal(
            journal.CompletionRefusalCode.RECEIPT_FAILED, "warning authority read failed"
        )
        refusal.__cause__ = exc
        return _held(refusal, next_action, authorization)
    try:
        removed = actuator() is True
    except Exception as exc:
        try:
            _record(authorization, False, next_action)
        except Exception as record_error:
            exc.add_note(f"Completion held outcome could not be recorded: {record_error}")
            raise exc from record_error
        raise
    return _record(authorization, removed, next_action)


def recover_completion_operation(
    authorization: journal.CompletionAuthorization,
    *,
    actuator: RecoverableActuator,
    next_action: str,
) -> ReceiptOutcome:
    """Reconcile the prepare replay's event using storage observation only.

    The callable actuator supplies observe_removed() -> bool independently of
    __call__. Recovery never invokes __call__, including when storage remains.
    A prepared event after a crash is evidence of intent, not proof of removal.
    """
    replay = _replay(authorization, next_action)
    if replay is not None:
        return replay
    removed = actuator.observe_removed() is True
    return _record(authorization, removed, next_action)
