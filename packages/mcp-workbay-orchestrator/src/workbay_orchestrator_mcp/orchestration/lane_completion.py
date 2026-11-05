"""Pure completion rules over adapter-verified, immutable evidence.

P/F/A/T describe verified predicates, not raw receipts or status strings. The
future snapshot adapter owns IO, authentication, latest-row selection and
content verification. This module neither collects facts nor authorizes cleanup.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum


class CompletionVerdict(str, Enum):
    COMPLETED = "completed"
    SUPERSEDED = "superseded"
    UNFINISHED_NEVER_PRODUCED = "unfinished_never_produced"
    UNFINISHED_PARTIAL = "unfinished_partial"
    UNKNOWN = "unknown"


class CompletionPhase(str, Enum):
    CANDIDATE = "candidate"
    INTEGRATION = "integration"


class EvidenceState(str, Enum):
    """PRESENT means the named predicate was positively verified by the adapter."""

    PRESENT = "present"
    OBSERVED_ABSENT = "observed_absent"
    UNAVAILABLE = "unavailable"


class CompletionReason(str, Enum):
    IDENTITY_INVALID = "identity_invalid"
    CONTRACT_ABSENT = "contract_absent"
    CONTRACT_INVALID = "contract_invalid"
    EVIDENCE_INVALID = "evidence_invalid"
    TARGET_MISMATCH = "target_mismatch"
    CONTRADICTORY_EVIDENCE = "contradictory_evidence"
    PRODUCT_UNPROVEN = "product_unproven"
    FINDINGS_UNPROVEN = "findings_unproven"
    ACCEPTANCE_UNPROVEN = "acceptance_unproven"
    TESTS_UNPROVEN = "tests_unproven"
    COMPLETED_INTEGRATION = "completed_integration"
    COMPLETED_CANDIDATE = "completed_candidate"
    CARRIER_COMPLETED = "carrier_completed"
    CARRIER_UNPROVEN = "carrier_unproven"
    TRANSFER_UNAUTHENTICATED = "transfer_unauthenticated"
    TRANSFER_COVERAGE_INCOMPLETE = "transfer_coverage_incomplete"
    CARRIER_TARGET_MISMATCH = "carrier_target_mismatch"
    CARRIER_CYCLE = "carrier_cycle"
    CARRIER_CONFLICT = "carrier_conflict"
    TRANSFER_DEPTH_EXCEEDED = "transfer_depth_exceeded"
    KNOWN_UNMET = "known_unmet"
    NEVER_PRODUCED = "never_produced"
    SOME_PRODUCTION = "some_production"
    PRODUCTION_UNKNOWN = "production_unknown"
    UNMET_UNPROVEN = "unmet_unproven"


class ObligationKind(str, Enum):
    FINDING = "finding"
    ACCEPTANCE = "acceptance"
    TEST = "test"


class ProductProofKind(str, Enum):
    NATURAL_LANDING = "natural_landing"
    INTEGRATION_CONTENT = "integration_content"
    CANDIDATE_CONTENT = "candidate_content"


@dataclass(frozen=True)
class LaneIdentity:
    repository: str
    task_ref: str
    lane_id: str
    lane_row_id: int
    identity_seq: int
    cycle_id: str | None
    branch: str
    admission_base: str
    product_tip: str
    lane_revision: str
    task_revision: str


@dataclass(frozen=True)
class CompletionTarget:
    """Resolved target and optional distinct historical landing event.

    The adapter verifies event reachability and T at the applicable landed SHA;
    advancing the integration ref does not make those OIDs equal.
    """

    phase: CompletionPhase
    integration_ref: str
    integration_sha: str
    candidate_sha: str
    merge_tree_oid: str | None = None
    landing_sha: str | None = None


@dataclass(frozen=True)
class AcceptanceVerifier:
    acceptance_id: str
    kind: str
    version: int
    command_or_check_id: str
    expected_outcome: str
    artifact_format: str


@dataclass(frozen=True)
class CompletionContract:
    identity: LaneIdentity
    revision: int
    contract_hash: str
    objective_hash: str
    # None is absent; () is an explicit owner declaration of no findings/tests.
    finding_ids: tuple[str, ...] | None
    acceptance_ids: tuple[str, ...]
    required_commands: tuple[str, ...] | None
    approved: bool
    exhaustive: bool
    evidence_ids: tuple[str, ...]
    non_test_acceptance_id: str | None = None
    acceptance_verifiers: tuple[AcceptanceVerifier, ...] = ()


@dataclass(frozen=True)
class IdentityScopeEvidence:
    """I: coherent canonical identity, ownership and approved exhaustive scope."""

    identity: LaneIdentity
    contract: CompletionContract | None
    state: EvidenceState = EvidenceState.UNAVAILABLE
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class EvidenceBinding:
    identity: LaneIdentity
    contract_revision: int
    contract_hash: str
    target: CompletionTarget


@dataclass(frozen=True, kw_only=True)
class PredicateEvidence:
    state: EvidenceState = EvidenceState.UNAVAILABLE
    binding: EvidenceBinding | None = None
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True, kw_only=True)
class ProductOnTargetEvidence(PredicateEvidence):
    """P: nonempty contracted product verified at the exact assessed target."""

    proof_kind: ProductProofKind | None = None


@dataclass(frozen=True, kw_only=True)
class FindingsAddressedEvidence(PredicateEvidence):
    """F: target-verified finding resolutions; branch resolutions only pre-merge."""

    finding_ids: tuple[str, ...] = ()


@dataclass(frozen=True, kw_only=True)
class AcceptanceEvidence(PredicateEvidence):
    """A: latest deterministic receipts at this content and contract revision."""

    acceptance_ids: tuple[str, ...] = ()


@dataclass(frozen=True, kw_only=True)
class TestFreshnessEvidence(PredicateEvidence):
    """T: latest applicable rows (including failures), at the assessed SHA/tree."""

    commands: tuple[str, ...] = ()


@dataclass(frozen=True)
class Obligation:
    kind: ObligationKind
    id: str


@dataclass(frozen=True, kw_only=True)
class KnownUnmetEvidence(PredicateEvidence):
    """U: positive evidence of a declared obligation that remains unmet."""

    obligations: tuple[Obligation, ...] = ()


@dataclass(frozen=True, kw_only=True)
class NeverProducedEvidence(PredicateEvidence):
    """N: complete authenticated cycle/history proves zero product ever produced."""


@dataclass(frozen=True, kw_only=True)
class SomeProductionEvidence(PredicateEvidence):
    """R: durable positive production evidence, including reset/integrated work."""


@dataclass(frozen=True)
class LaneCompletionEvidence:
    identity_scope: IdentityScopeEvidence
    target: CompletionTarget
    product: ProductOnTargetEvidence = ProductOnTargetEvidence()
    findings: FindingsAddressedEvidence = FindingsAddressedEvidence()
    acceptance: AcceptanceEvidence = AcceptanceEvidence()
    tests: TestFreshnessEvidence = TestFreshnessEvidence()
    unmet: KnownUnmetEvidence = KnownUnmetEvidence()
    never_produced: NeverProducedEvidence = NeverProducedEvidence()
    some_production: SomeProductionEvidence = SomeProductionEvidence()


@dataclass(frozen=True)
class ObligationTransfer:
    predecessor: Obligation
    carrier: Obligation


@dataclass(frozen=True)
class TransferEvidence:
    source: LaneIdentity
    carrier: LaneIdentity
    source_contract_hash: str
    carrier_contract_hash: str
    mappings: tuple[ObligationTransfer, ...]
    authenticated: bool
    authority_id: str
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True)
class CarrierEvidence:
    lane: LaneCompletionEvidence
    assessment_id: str
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True)
class CompletionSnapshot:
    lane: LaneCompletionEvidence
    carriers: tuple[CarrierEvidence, ...] = ()
    transfers: tuple[TransferEvidence, ...] = ()
    # Custody is independent. These holds are retained, never discharged here.
    source_holds: tuple[str, ...] = ()
    custody_holds: tuple[str, ...] = ()
    # Storage observations deliberately do not participate in any predicate.
    ancestry_contained: bool | None = None
    ahead_of_integration: int | None = None


@dataclass(frozen=True)
class CompletionAssessment:
    verdict: CompletionVerdict
    phase: CompletionPhase
    reasons: tuple[CompletionReason, ...]
    evidence_ids: tuple[str, ...]
    assessment_hash: str
    source_holds: tuple[str, ...]
    custody_holds: tuple[str, ...]


def _canonical(value):
    if is_dataclass(value):
        return {
            field.name: _canonical(getattr(value, field.name))
            for field in fields(value)
            # Keep the slice-1 encoding when no verifier declaration exists.
            if not (
                isinstance(value, CompletionContract)
                and field.name == "acceptance_verifiers"
                and value.acceptance_verifiers == ()
            )
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        # All snapshot collections are sets of facts, not ordered event logs.
        return sorted((_canonical(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    return value


def _assessment(snapshot, verdict, reasons, evidence_ids=()):
    payload = {"evaluator_version": 1, "snapshot": _canonical(snapshot), "verdict": verdict.value}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return CompletionAssessment(
        verdict,
        snapshot.lane.target.phase,
        tuple(reasons),
        tuple(sorted(set(evidence_ids))),
        digest,
        snapshot.source_holds,
        snapshot.custody_holds,
    )


def _predicates(lane):
    return (
        lane.product,
        lane.findings,
        lane.acceptance,
        lane.tests,
        lane.unmet,
        lane.never_produced,
        lane.some_production,
    )


def _identity_valid(identity):
    return (
        all(
            isinstance(value, str) and value
            for value in (
                identity.repository,
                identity.task_ref,
                identity.lane_id,
                identity.branch,
                identity.admission_base,
                identity.product_tip,
                identity.lane_revision,
                identity.task_revision,
            )
        )
        and type(identity.lane_row_id) is int
        and identity.lane_row_id > 0
        and type(identity.identity_seq) is int
        and identity.identity_seq >= 0
        and (identity.cycle_id is None or bool(identity.cycle_id))
    )


def _ids_valid(ids):
    return isinstance(ids, tuple) and all(isinstance(id, str) and id for id in ids) and len(ids) == len(set(ids))


def _verifiers_valid(contract):
    verifiers = contract.acceptance_verifiers
    return (
        isinstance(verifiers, tuple)
        and all(
            isinstance(item, AcceptanceVerifier)
            and type(item.version) is int
            and item.version > 0
            and all(
                isinstance(value, str) and value
                for value in (
                    item.acceptance_id,
                    item.kind,
                    item.command_or_check_id,
                    item.expected_outcome,
                    item.artifact_format,
                )
            )
            for item in verifiers
        )
        and len(verifiers) == len(contract.acceptance_ids)
        and {item.acceptance_id for item in verifiers} == set(contract.acceptance_ids)
    )


def _validate(lane):
    scope = lane.identity_scope
    contract = scope.contract
    if not _identity_valid(scope.identity) or scope.state != EvidenceState.PRESENT or not scope.evidence_ids:
        return CompletionReason.IDENTITY_INVALID
    if contract is None:
        return CompletionReason.CONTRACT_ABSENT
    if (
        contract.identity != scope.identity
        or contract.approved is not True
        or contract.exhaustive is not True
        or type(contract.revision) is not int
        or contract.revision < 1
        or not contract.contract_hash
        or not contract.objective_hash
        or not contract.evidence_ids
        or not _ids_valid(contract.finding_ids)
        or not _ids_valid(contract.acceptance_ids)
        or not contract.acceptance_ids
        or not _verifiers_valid(contract)
        or not _ids_valid(contract.required_commands)
        or (not contract.required_commands and contract.non_test_acceptance_id not in contract.acceptance_ids)
    ):
        return CompletionReason.CONTRACT_INVALID
    target = lane.target
    if (
        not isinstance(target.phase, CompletionPhase)
        or not target.integration_ref
        or not target.integration_sha
        or not target.candidate_sha
        or target.candidate_sha != scope.identity.product_tip
    ):
        return CompletionReason.TARGET_MISMATCH
    expected = EvidenceBinding(scope.identity, contract.revision, contract.contract_hash, target)
    for predicate in _predicates(lane):
        if not isinstance(predicate.state, EvidenceState) or not _ids_valid(predicate.evidence_ids):
            return CompletionReason.EVIDENCE_INVALID
        if predicate.binding is not None and predicate.binding != expected:
            return CompletionReason.TARGET_MISMATCH
        if predicate.state == EvidenceState.PRESENT and (predicate.binding is None or not predicate.evidence_ids):
            return CompletionReason.EVIDENCE_INVALID
    if lane.product.state == EvidenceState.PRESENT and not isinstance(lane.product.proof_kind, ProductProofKind):
        return CompletionReason.EVIDENCE_INVALID
    if lane.unmet.state == EvidenceState.PRESENT:
        obligations = lane.unmet.obligations
        if (
            not obligations
            or not isinstance(obligations, tuple)
            or any(not isinstance(item.kind, ObligationKind) for item in obligations)
            or not set(obligations) <= _obligations(lane)
        ):
            return CompletionReason.EVIDENCE_INVALID
    n = lane.never_produced.state == EvidenceState.PRESENT
    r = lane.some_production.state == EvidenceState.PRESENT
    if n and (r or lane.product.state == EvidenceState.PRESENT):
        return CompletionReason.CONTRADICTORY_EVIDENCE
    fulfilled = set()
    for predicate, kind, covered in (
        (lane.findings, ObligationKind.FINDING, lane.findings.finding_ids),
        (lane.acceptance, ObligationKind.ACCEPTANCE, lane.acceptance.acceptance_ids),
        (lane.tests, ObligationKind.TEST, lane.tests.commands),
    ):
        if not _ids_valid(covered):
            return CompletionReason.EVIDENCE_INVALID
        if predicate.state == EvidenceState.PRESENT:
            fulfilled.update(Obligation(kind, id) for id in covered)
    if lane.unmet.state == EvidenceState.PRESENT and fulfilled.intersection(lane.unmet.obligations):
        return CompletionReason.CONTRADICTORY_EVIDENCE
    return None


def _direct_reasons(lane):
    contract = lane.identity_scope.contract
    reasons = []
    allowed_proofs = (
        (ProductProofKind.CANDIDATE_CONTENT,)
        if lane.target.phase == CompletionPhase.CANDIDATE
        else (ProductProofKind.NATURAL_LANDING, ProductProofKind.INTEGRATION_CONTENT)
    )
    if lane.product.state != EvidenceState.PRESENT or lane.product.proof_kind not in allowed_proofs:
        reasons.append(CompletionReason.PRODUCT_UNPROVEN)
    if lane.findings.state != EvidenceState.PRESENT or set(lane.findings.finding_ids) != set(contract.finding_ids):
        reasons.append(CompletionReason.FINDINGS_UNPROVEN)
    if lane.acceptance.state != EvidenceState.PRESENT or set(lane.acceptance.acceptance_ids) != set(
        contract.acceptance_ids
    ):
        reasons.append(CompletionReason.ACCEPTANCE_UNPROVEN)
    if lane.tests.state != EvidenceState.PRESENT or set(lane.tests.commands) != set(contract.required_commands):
        reasons.append(CompletionReason.TESTS_UNPROVEN)
    return tuple(reasons)


def _lane_evidence_ids(lane):
    ids = set(lane.identity_scope.evidence_ids)
    if lane.identity_scope.contract is not None:
        ids.update(lane.identity_scope.contract.evidence_ids)
    for predicate in _predicates(lane):
        ids.update(predicate.evidence_ids)
    return ids


def _obligations(lane):
    contract = lane.identity_scope.contract
    return {
        Obligation(kind, id)
        for kind, ids in (
            (ObligationKind.FINDING, contract.finding_ids),
            (ObligationKind.ACCEPTANCE, contract.acceptance_ids),
            (ObligationKind.TEST, contract.required_commands),
        )
        for id in ids
    }


def _lane_key(identity):
    return identity.repository, identity.task_ref, identity.lane_id


def _carrier_result(snapshot, ids):
    source = snapshot.lane
    seen = {_lane_key(source.identity_scope.identity)}
    seen_rows = {(source.identity_scope.identity.repository, source.identity_scope.identity.lane_row_id)}
    for depth in range(1, 9):
        edges = set(
            edge for edge in snapshot.transfers if _lane_key(edge.source) == _lane_key(source.identity_scope.identity)
        )
        if not edges:
            return CompletionReason.CARRIER_UNPROVEN if depth > 1 else None
        if len(edges) != 1:
            for edge in edges:
                ids.update(edge.evidence_ids)
                if edge.authority_id:
                    ids.add(edge.authority_id)
            return CompletionReason.CARRIER_CONFLICT
        edge = next(iter(edges))
        ids.update(edge.evidence_ids)
        ids.add(edge.authority_id)
        if (
            edge.authenticated is not True
            or not edge.authority_id
            or not edge.evidence_ids
            or edge.source != source.identity_scope.identity
            or edge.source_contract_hash != source.identity_scope.contract.contract_hash
        ):
            return CompletionReason.TRANSFER_UNAUTHENTICATED
        key = _lane_key(edge.carrier)
        if key in seen:
            return CompletionReason.CARRIER_CYCLE
        seen.add(key)
        row_key = (edge.carrier.repository, edge.carrier.lane_row_id)
        if row_key in seen_rows:
            return CompletionReason.CARRIER_CYCLE
        seen_rows.add(row_key)
        carriers = set(
            carrier for carrier in snapshot.carriers if _lane_key(carrier.lane.identity_scope.identity) == key
        )
        if len(carriers) > 1:
            for carrier in carriers:
                ids.update(carrier.evidence_ids)
                if carrier.assessment_id:
                    ids.add(carrier.assessment_id)
            return CompletionReason.CARRIER_CONFLICT
        if not carriers:
            return CompletionReason.CARRIER_UNPROVEN
        carrier = next(iter(carriers))
        ids.update(carrier.evidence_ids)
        ids.add(carrier.assessment_id)
        lane = carrier.lane
        ids.update(_lane_evidence_ids(lane))
        if (
            not carrier.assessment_id
            or not carrier.evidence_ids
            or _validate(lane)
            or edge.carrier != lane.identity_scope.identity
            or edge.carrier_contract_hash != lane.identity_scope.contract.contract_hash
        ):
            return CompletionReason.CARRIER_UNPROVEN
        if (
            lane.identity_scope.identity.repository != snapshot.lane.identity_scope.identity.repository
            or lane.target.phase != CompletionPhase.INTEGRATION
            or lane.target.integration_ref != snapshot.lane.target.integration_ref
            or lane.target.integration_sha != snapshot.lane.target.integration_sha
        ):
            return CompletionReason.CARRIER_TARGET_MISMATCH
        mappings = set(edge.mappings)
        if (
            {mapping.predecessor for mapping in mappings} != _obligations(source)
            or len(mappings) != len(_obligations(source))
            or not {mapping.carrier for mapping in mappings} <= _obligations(lane)
        ):
            return CompletionReason.TRANSFER_COVERAGE_INCOMPLETE
        if not _direct_reasons(lane):
            return CompletionReason.CARRIER_COMPLETED
        source = lane
    return CompletionReason.TRANSFER_DEPTH_EXCEEDED


def evaluate(snapshot: CompletionSnapshot) -> CompletionAssessment:
    """Assess one immutable snapshot, with no IO or implicit negative evidence."""
    lane = snapshot.lane
    ids = _lane_evidence_ids(lane)
    invalid = _validate(lane)
    if invalid:
        return _assessment(snapshot, CompletionVerdict.UNKNOWN, (invalid,), ids)
    reasons = _direct_reasons(lane)
    if not reasons:
        reason = (
            CompletionReason.COMPLETED_CANDIDATE
            if lane.target.phase == CompletionPhase.CANDIDATE
            else CompletionReason.COMPLETED_INTEGRATION
        )
        return _assessment(snapshot, CompletionVerdict.COMPLETED, (reason,), ids)
    carrier_reason = _carrier_result(snapshot, ids)
    if carrier_reason is not None:
        verdict = (
            CompletionVerdict.SUPERSEDED
            if carrier_reason == CompletionReason.CARRIER_COMPLETED
            else CompletionVerdict.UNKNOWN
        )
        return _assessment(snapshot, verdict, (carrier_reason,), ids)
    if lane.unmet.state == EvidenceState.PRESENT and lane.never_produced.state == EvidenceState.PRESENT:
        return _assessment(
            snapshot,
            CompletionVerdict.UNFINISHED_NEVER_PRODUCED,
            (CompletionReason.KNOWN_UNMET, CompletionReason.NEVER_PRODUCED),
            ids,
        )
    if lane.unmet.state == EvidenceState.PRESENT and lane.some_production.state == EvidenceState.PRESENT:
        return _assessment(
            snapshot,
            CompletionVerdict.UNFINISHED_PARTIAL,
            (CompletionReason.KNOWN_UNMET, CompletionReason.SOME_PRODUCTION),
            ids,
        )
    if lane.unmet.state == EvidenceState.PRESENT:
        reasons = (CompletionReason.KNOWN_UNMET, CompletionReason.PRODUCTION_UNKNOWN)
    else:
        reasons = (*reasons, CompletionReason.UNMET_UNPROVEN)
    return _assessment(snapshot, CompletionVerdict.UNKNOWN, reasons, ids)
