"""R0 consumer retention wire contract and side-effect-free admission.

No I/O, discovery, graph traversal, transaction, provider call, or deletion lives
here. Verified observations are assertions supplied by qualified consumers, not
proof manufactured by this module. JSON Schema validates shape; the admission
functions additionally compare identities, bindings and freshness.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    RootModel,
    StrictBool,
    StrictInt,
    StringConstraints,
    WithJsonSchema,
)

from .convergence import AwareRfc3339, Sha40, Sha256Digest

_TEXT_PATTERN = r"^[^\u0000-\u001f]*[^\s\u0000-\u001f][^\u0000-\u001f]*$"
_NAMESPACE_PATTERN = r"^[a-z][a-z0-9+.-]*:[^\s]+$"
_OPAQUE_PATTERN = r"^opaque:[A-Za-z0-9._/-]+$"
_REPOSITORY_PATTERN = r"^repo:[A-Za-z0-9][A-Za-z0-9._/-]*$"


def _string_schema(pattern: str) -> dict:
    # Python/ECMAScript `$` may match before a trailing LF; explicitly forbid it.
    return {"type": "string", "minLength": 1, "pattern": pattern, "not": {"pattern": r"[\u0000-\u001f]"}}


Text = Annotated[
    str,
    StringConstraints(strict=True, min_length=1, pattern=_TEXT_PATTERN),
    WithJsonSchema(_string_schema(_TEXT_PATTERN)),
]
Namespace = Annotated[
    str,
    StringConstraints(strict=True, min_length=1, pattern=_NAMESPACE_PATTERN),
    WithJsonSchema(_string_schema(_NAMESPACE_PATTERN)),
]
OpaqueRef = Annotated[
    str, StringConstraints(strict=True, pattern=_OPAQUE_PATTERN), WithJsonSchema(_string_schema(_OPAQUE_PATTERN))
]
RepositoryNamespace = Annotated[
    str,
    StringConstraints(strict=True, pattern=_REPOSITORY_PATTERN),
    WithJsonSchema(_string_schema(_REPOSITORY_PATTERN)),
]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
ProofState = Literal["verified", "unknown", "denied", "contradictory"]
Operation = Literal["archive", "retire", "withdraw_materialization"]


def _timestamp_input(value: object) -> object:
    if not isinstance(value, (str, datetime)):
        raise ValueError("timestamp must be an aware datetime or RFC3339 string")
    return value


Timestamp = Annotated[AwareRfc3339, BeforeValidator(_timestamp_input)]


def _literal_false(value: object) -> object:
    if value is not False:
        raise ValueError("R0 cannot authorize effects; effects_authorized must be literal false")
    return value


NoEffects = Annotated[Literal[False], BeforeValidator(_literal_false)]


class ContractModel(BaseModel):
    """Closed wire objects; nested collections are immutable tuples."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class EvidenceRef(ContractModel):
    """Immutable private evidence reference; never embed credentials or payloads."""

    ref: Namespace
    digest: Sha256Digest


class Owner(ContractModel):
    """Repository namespace, e.g. repo:github.com/darce/workbay; never a local ID."""

    repository: RepositoryNamespace


class ResourceIdentity(ContractModel):
    owner: Owner
    resource_kind: Literal["directory", "artifact", "git_repository", "git_worktree", "docker_image", "docker_volume"]
    identity: Text


class GitSource(ContractModel):
    kind: Literal["git"] = "git"
    revision: Sha40
    tree: Sha40


class DockerSource(ContractModel):
    kind: Literal["docker"] = "docker"
    digest: Sha256Digest


SourceIdentity = Annotated[GitSource | DockerSource, Field(discriminator="kind")]


class Proof(ContractModel):
    """The named claim is true only with positive, fresh, immutable evidence.

    Empty/unknown/denied inventories are representable and always hold admission.
    An empty privileged result must not be converted into a verified claim.
    """

    state: ProofState = "unknown"
    evidence: tuple[EvidenceRef, ...] = ()
    observed_at: Timestamp | None = None
    valid_until: Timestamp | None = None


class WorkBayIdentity(ContractModel):
    task: Text | None = None
    lane: Text | None = None
    pass_id: Text | None = None
    job: Text | None = None


class OrcaIdentity(ContractModel):
    run: Text | None = None
    task: Text | None = None
    dispatch: Text | None = None


class Correlation(ContractModel):
    """IDs stay in their authority's namespace. Presence alone proves no join."""

    workbay: WorkBayIdentity | None = None
    orca: OrcaIdentity | None = None
    verified_join: Proof | None = None


class StreamExpectation(ContractModel):
    """R3 evaluates cadence/deadline + registered grace, separately from age.

    Active streams require a cadence or deadline. Idle/dormant/completed history
    has no age-based failure. The collector's original cadence is 60 seconds.
    """

    state: Literal["active", "idle", "dormant", "completed"]
    cadence_seconds: PositiveInt | None = None
    deadline: Timestamp | None = None
    grace_seconds: NonNegativeInt
    connected_visibility_target_seconds: Literal[30] = 30
    stale_after_missed_expectation_target_seconds: Literal[30] = 30


class Registration(ContractModel):
    kind: Literal["registration"] = "registration"
    resource: ResourceIdentity
    source: SourceIdentity
    operation: Operation
    policy_version: Text
    evidence: tuple[EvidenceRef, ...] = Field(min_length=1)
    correlation: Correlation = Field(default_factory=Correlation)
    stream: StreamExpectation | None = None


class LifecycleFacts(ContractModel):
    """Independent facts; terminal/age never implies review, release or custody."""

    execution_terminal: Proof = Field(default_factory=Proof)
    harvested: Proof = Field(default_factory=Proof)
    reviewed: Proof = Field(default_factory=Proof)
    merged: Proof = Field(default_factory=Proof)
    published: Proof = Field(default_factory=Proof)
    activated: Proof = Field(default_factory=Proof)
    verified: Proof = Field(default_factory=Proof)
    custody_acknowledged: Proof = Field(default_factory=Proof)
    retired: Proof = Field(default_factory=Proof)


class DependencyEdge(ContractModel):
    """Consumer -> required provider. R1 supplies reachability/SCC verdicts."""

    consumer: ResourceIdentity
    provider: ResourceIdentity
    relation: Literal[
        "git", "symlink", "editable", "process", "mount", "container", "child_image", "rollback", "db_volume", "other"
    ]
    state: Literal["required", "released", "unknown"]
    proof: Proof


class DependencySnapshot(ContractModel):
    """No guessed order: retained consumers hold providers and whole SCCs.

    R1 must inventory incoming/reachable consumers and condense cycles before
    asserting no_live_consumers and scc_clear. This is a verdict interface, not
    a graph algorithm. Unknown privileged inventory must remain unknown.
    """

    edges: tuple[DependencyEdge, ...] = ()
    inventory_complete: Proof
    no_live_consumers: Proof
    scc_clear: Proof
    component_members: tuple[ResourceIdentity, ...] = ()


CustodyScope = Literal["full_bytes", "types", "links", "refs", "reflogs", "ignored"]


class CustodyAcknowledgement(ContractModel):
    resource: ResourceIdentity
    source: SourceIdentity
    scope: tuple[CustodyScope, ...]
    archive: EvidenceRef
    acknowledgement: Proof
    fidelity: Proof
    restored_fsck: Proof | None = None


class RetentionPlan(ContractModel):
    """Immutable dry-run intent, including dependency/custody observations."""

    kind: Literal["plan"] = "plan"
    plan_id: Text
    operation_id: Text
    registration: Registration
    dependencies: DependencySnapshot
    custody: CustodyAcknowledgement
    created_at: Timestamp


class DockerFacts(ContractModel):
    """R2 asserts absence with separate proofs, including stopped containers.

    DB/recovery volume clearance additionally requires explicit permission;
    dangling images, age and a failed privileged read grant no clearance.
    """

    inventory_complete: Proof
    no_running_containers: Proof
    no_stopped_containers: Proof
    no_child_images: Proof
    rollback_released: Proof
    db_volume_released: Proof
    db_volume_permission: Proof | None = None


class RetentionProbe(ContractModel):
    kind: Literal["probe"] = "probe"
    plan_digest: Sha256Digest
    registration: Registration
    dependencies_digest: Sha256Digest
    custody_digest: Sha256Digest
    owner_verified: Proof
    source_verified: Proof
    policy_allowed: Proof
    completion: Proof = Field(
        description="Operation-specific durable release/withdrawal receipt; terminal, age and merged markers are not substitutes."
    )
    no_live_use: Proof
    dependencies_clear: Proof
    custody_valid: Proof
    docker: DockerFacts | None = None


class ApplyRequest(ContractModel):
    """R4/R5 must hold a per-resource lock across re-probe and any effects."""

    kind: Literal["apply"] = "apply"
    plan: RetentionPlan
    expected_plan_digest: Sha256Digest
    probe: RetentionProbe
    dry_run: StrictBool = True


class ExecutionRequest(ContractModel):
    kind: Literal["execution_request"] = "execution_request"
    resource: ResourceIdentity
    source: GitSource
    execution_user: Text
    agent_binary: Text
    workspace: Text
    backend: Text
    provider: Text
    account_ref: OpaqueRef
    profile_ref: OpaqueRef
    model: Text
    effort: Literal["low", "medium", "high", "xhigh", "max"]
    service_tier: Literal["standard", "priority", "flex"]
    correlation: Correlation = Field(default_factory=Correlation)


class ExecutionObservation(ContractModel):
    """Observed values, never copied from request/config as served-model proof.

    Account/profile fields contain opaque nonsecret references only. Catalogue
    support and observed served execution are separate positive observations.
    Missing public handoff rows, wrong imported runtime or inaccessible users
    remain real unknown/denied observations, not fabricated running states.
    """

    kind: Literal["execution_observation"] = "execution_observation"
    resource: ResourceIdentity | None = None
    source: GitSource | None = None
    execution_user: Text | None = None
    agent_binary: Text | None = None
    workspace: Text | None = None
    workspace_accessible: StrictBool | None = None
    backend: Text | None = None
    provider: Text | None = None
    account_ref: OpaqueRef | None = None
    profile_ref: OpaqueRef | None = None
    served_model: Text | None = None
    effort: Text | None = None
    service_tier: Text | None = None
    supported_models: tuple[Text, ...] | None = None
    identity_verified: Proof = Field(default_factory=Proof)
    binary_executable: Proof = Field(default_factory=Proof)
    workspace_verified: Proof = Field(default_factory=Proof)
    source_verified: Proof = Field(default_factory=Proof)
    authenticated: Proof = Field(default_factory=Proof)
    catalogue_verified: Proof = Field(default_factory=Proof)
    served_execution_verified: Proof = Field(default_factory=Proof)
    remote_available: Proof = Field(default_factory=Proof)
    cost_allowed: Proof = Field(default_factory=Proof)
    runtime_verified: Proof = Field(default_factory=Proof)
    handoff_available: Proof = Field(default_factory=Proof)


class Hold(ContractModel):
    code: Literal[
        "proof_unknown",
        "proof_denied",
        "proof_contradictory",
        "proof_missing_evidence",
        "proof_missing_freshness",
        "proof_stale",
        "proof_future",
        "proof_invalid_window",
        "execution_user_unknown",
        "execution_user_mismatch",
        "agent_binary_unknown",
        "agent_binary_mismatch",
        "agent_binary_not_absolute",
        "workspace_unknown",
        "workspace_mismatch",
        "workspace_not_absolute",
        "workspace_access_unknown",
        "workspace_inaccessible",
        "source_unknown",
        "source_mismatch",
        "resource_unknown",
        "resource_mismatch",
        "foreign_owner",
        "backend_unknown",
        "backend_mismatch",
        "provider_unknown",
        "provider_mismatch",
        "account_ref_unknown",
        "account_ref_mismatch",
        "profile_ref_unknown",
        "profile_ref_mismatch",
        "served_model_unknown",
        "served_model_mismatch",
        "effort_unknown",
        "effort_mismatch",
        "service_tier_unknown",
        "service_tier_mismatch",
        "model_catalogue_unknown",
        "model_unsupported",
        "plan_changed",
        "registration_changed",
        "dependencies_changed",
        "custody_changed",
        "custody_scope_missing",
        "custody_source_mismatch",
        "custody_resource_mismatch",
        "custody_fsck_missing",
        "dependency_required",
        "dependency_unknown",
        "scc_held",
        "docker_facts_missing",
        "db_volume_permission_missing",
        "stream_expectation_missing",
    ]
    subject: Text


class Allow(ContractModel):
    kind: Literal["allow"] = "allow"
    scope: Literal["execution", "retention"]
    effects_authorized: NoEffects = False


class Held(ContractModel):
    kind: Literal["held"] = "held"
    scope: Literal["execution", "retention"]
    holds: tuple[Hold, ...] = Field(min_length=1)
    effects_authorized: NoEffects = False


Admission = Allow | Held


class ByteAccounting(ContractModel):
    measured_bytes: NonNegativeInt | None = None
    estimated_bytes: NonNegativeInt | None = None
    evidence: tuple[EvidenceRef, ...] = ()


class LifecycleEvent(ContractModel):
    """R3/R5 persist atomically with authority; replay retains original payload.

    Event authority/history must live outside retired checkouts. transaction_ref
    is a durable authority reference, never an independently written second log.
    """

    kind: Literal["event"] = "event"
    event_id: Sha256Digest
    operation_id: Text
    phase: Literal["registered", "planned", "probed", "applied", "failed", "held"]
    resource: ResourceIdentity
    source: SourceIdentity
    policy_version: Text
    evidence: tuple[EvidenceRef, ...] = Field(min_length=1)
    correlation: Correlation = Field(default_factory=Correlation)
    outcome: Admission
    facts: LifecycleFacts = Field(default_factory=LifecycleFacts)
    observations: tuple[Proof, ...] = Field(min_length=1)
    accounting: ByteAccounting = Field(default_factory=ByteAccounting)
    durable_committed_at: Timestamp
    transaction_ref: Namespace
    synthetic_tag: Text | None = None


class ResourceLifecycleContract(RootModel):
    """Generated Draft 2020-12 discriminated packet schema, version 1."""

    root: Annotated[
        Registration
        | RetentionPlan
        | RetentionProbe
        | ApplyRequest
        | ExecutionRequest
        | ExecutionObservation
        | Allow
        | Held
        | LifecycleEvent,
        Field(discriminator="kind"),
    ]


def canonical_digest(value: ContractModel) -> str:
    """SHA256 of UTF-8, sorted compact JSON; tuples serialize as JSON arrays."""
    payload = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def event_identity(resource: ResourceIdentity, operation_id: str, phase: str) -> str:
    """Stable event key. R3 must retain the first payload for this key on replay."""
    payload = json.dumps([resource.model_dump(mode="json"), operation_id, phase], sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _proof_holds(proof: Proof | None, subject: str, now: datetime) -> list[Hold]:
    if proof is None or proof.state == "unknown":
        return [Hold(code="proof_unknown", subject=subject)]
    if proof.state != "verified":
        return [Hold(code="proof_" + proof.state, subject=subject)]
    if not proof.evidence:
        return [Hold(code="proof_missing_evidence", subject=subject)]
    if proof.observed_at is None or proof.valid_until is None:
        return [Hold(code="proof_missing_freshness", subject=subject)]
    if proof.valid_until < proof.observed_at:
        return [Hold(code="proof_invalid_window", subject=subject)]
    if proof.observed_at > now:
        return [Hold(code="proof_future", subject=subject)]
    if now > proof.valid_until:
        return [Hold(code="proof_stale", subject=subject)]
    return []


def _require_aware_now(now: datetime) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")


def _result(scope: Literal["execution", "retention"], holds: list[Hold]) -> Admission:
    return Held(scope=scope, holds=tuple(holds)) if holds else Allow(scope=scope)


def admit_execution(requested: ExecutionRequest, observed: ExecutionObservation, *, now: datetime) -> Admission:
    """Compare requested vs independently observed execution; no launch/effects.

    A catalogue/model_not_found error is a real hold. This function never
    substitutes provider/model/user, retries, or invents WorkBay/Orca states.
    """
    _require_aware_now(now)
    holds: list[Hold] = []
    for name in (
        "resource",
        "source",
        "execution_user",
        "agent_binary",
        "workspace",
        "backend",
        "provider",
        "account_ref",
        "profile_ref",
        "effort",
        "service_tier",
    ):
        expected, actual = getattr(requested, name), getattr(observed, name)
        if actual is None:
            holds.append(Hold(code=name + "_unknown", subject=name))
        elif actual != expected:
            code = "foreign_owner" if name == "resource" and actual.owner != expected.owner else name + "_mismatch"
            holds.append(Hold(code=code, subject=name))
    for name in ("agent_binary", "workspace"):
        for label, value in (("requested", getattr(requested, name)), ("observed", getattr(observed, name))):
            if value is not None and (not value.startswith("/") or ".." in value.split("/")):
                holds.append(Hold(code=name + "_not_absolute", subject=label + "." + name))
    if observed.workspace_accessible is not True:
        holds.append(
            Hold(
                code="workspace_access_unknown" if observed.workspace_accessible is None else "workspace_inaccessible",
                subject="workspace",
            )
        )
    if observed.supported_models is None:
        holds.append(Hold(code="model_catalogue_unknown", subject="supported_models"))
    elif requested.model not in observed.supported_models:
        holds.append(Hold(code="model_unsupported", subject="supported_models"))
    if observed.served_model is None:
        holds.append(Hold(code="served_model_unknown", subject="served_model"))
    elif observed.served_model != requested.model:
        holds.append(Hold(code="served_model_mismatch", subject="served_model"))
    for name in (
        "identity_verified",
        "binary_executable",
        "workspace_verified",
        "source_verified",
        "authenticated",
        "catalogue_verified",
        "served_execution_verified",
        "remote_available",
        "cost_allowed",
        "runtime_verified",
        "handoff_available",
    ):
        holds.extend(_proof_holds(getattr(observed, name), name, now))
    return _result("execution", holds)


def admit_retention(request: ApplyRequest, *, owner: Owner, now: datetime) -> Admission:
    """Validate immutable plan/probe bindings and supplied clearances, no effects.

    Even allow is only pure eligibility. R4/R5 still owe locking, fresh re-probe,
    transactional authority/events, crash reconciliation and effect permission.
    R1/R2 owe complete reachable inventories; supplied facts are not discovered.
    """
    _require_aware_now(now)
    plan, probe = request.plan, request.probe
    registration = plan.registration
    holds: list[Hold] = []
    if registration.resource.owner != owner or probe.registration.resource.owner != owner:
        holds.append(Hold(code="foreign_owner", subject="resource.owner"))
    digest = canonical_digest(plan)
    if request.expected_plan_digest != digest or probe.plan_digest != digest:
        holds.append(Hold(code="plan_changed", subject="plan"))
    if probe.registration.source != registration.source:
        holds.append(Hold(code="source_mismatch", subject="registration.source"))
    if probe.registration != registration:
        holds.append(Hold(code="registration_changed", subject="registration"))
    if probe.dependencies_digest != canonical_digest(plan.dependencies):
        holds.append(Hold(code="dependencies_changed", subject="dependencies"))
    if probe.custody_digest != canonical_digest(plan.custody):
        holds.append(Hold(code="custody_changed", subject="custody"))
    for name in (
        "owner_verified",
        "source_verified",
        "policy_allowed",
        "completion",
        "no_live_use",
        "dependencies_clear",
        "custody_valid",
    ):
        holds.extend(_proof_holds(getattr(probe, name), name, now))
    for name in ("inventory_complete", "no_live_consumers", "scc_clear"):
        holds.extend(_proof_holds(getattr(plan.dependencies, name), "dependencies." + name, now))
    if len(plan.dependencies.component_members) > 1:
        holds.append(Hold(code="scc_held", subject="dependencies.component_members"))
    for edge in plan.dependencies.edges:
        holds.extend(_proof_holds(edge.proof, "dependency.edge", now))
        if edge.provider == registration.resource and edge.state != "released":
            holds.append(
                Hold(
                    code="dependency_required" if edge.state == "required" else "dependency_unknown",
                    subject="dependency.provider",
                )
            )
    custody = plan.custody
    if custody.resource != registration.resource:
        holds.append(Hold(code="custody_resource_mismatch", subject="custody.resource"))
    if custody.source != registration.source:
        holds.append(Hold(code="custody_source_mismatch", subject="custody.source"))
    required_scope = {"full_bytes", "types", "links", "ignored"}
    if registration.source.kind == "git":
        required_scope.update(("refs", "reflogs"))
        if custody.restored_fsck is None:
            holds.append(Hold(code="custody_fsck_missing", subject="custody.restored_fsck"))
        else:
            holds.extend(_proof_holds(custody.restored_fsck, "custody.restored_fsck", now))
    if not required_scope.issubset(custody.scope):
        holds.append(Hold(code="custody_scope_missing", subject="custody.scope"))
    for name in ("acknowledgement", "fidelity"):
        holds.extend(_proof_holds(getattr(custody, name), "custody." + name, now))
    if registration.resource.resource_kind in ("docker_image", "docker_volume"):
        if probe.docker is None:
            holds.append(Hold(code="docker_facts_missing", subject="docker"))
        else:
            for name in (
                "inventory_complete",
                "no_running_containers",
                "no_stopped_containers",
                "no_child_images",
                "rollback_released",
                "db_volume_released",
            ):
                holds.extend(_proof_holds(getattr(probe.docker, name), "docker." + name, now))
            if registration.resource.resource_kind == "docker_volume":
                if probe.docker.db_volume_permission is None:
                    holds.append(Hold(code="db_volume_permission_missing", subject="docker.db_volume_permission"))
                else:
                    holds.extend(_proof_holds(probe.docker.db_volume_permission, "docker.db_volume_permission", now))
    stream = registration.stream
    if stream is not None and stream.state == "active" and stream.cadence_seconds is None and stream.deadline is None:
        holds.append(Hold(code="stream_expectation_missing", subject="registration.stream"))
    return _result("retention", holds)
