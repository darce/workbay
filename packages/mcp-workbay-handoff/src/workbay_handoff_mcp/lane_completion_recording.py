"""Handoff-owned durable completion contracts; no orchestrator dependency.

``ContractRecord.contract`` maps 1:1 to the orchestrator CompletionContract:
construct LaneIdentity from ``identity`` and AcceptanceVerifier from each
``acceptance_verifiers`` entry before constructing CompletionContract. The
remaining record fields preserve admission declarations outside that vocabulary.

Hashing uses implementation note's canonical JSON rule: dataclass fields, enum values,
set-like tuples sorted by their JSON representation, sorted object keys, compact
separators, UTF-8, SHA256. Mappings additionally support the typed-dict boundary.
The contract hash covers the entire stored declaration (including revision),
with its own ``contract_hash`` field blanked to avoid self-reference. The
idempotency key uses the same payload with revision zero and hash blanked.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import TypedDict

from .shared_schema import _get_db_connection


class LaneIdentityDict(TypedDict):
    repository: str
    task_ref: str
    lane_id: str
    lane_row_id: int
    identity_seq: int | None
    cycle_id: str | None
    branch: str
    admission_base: str
    product_tip: str
    lane_revision: str
    task_revision: str


class AcceptanceVerifierDict(TypedDict):
    acceptance_id: str
    kind: str
    version: int
    command_or_check_id: str
    expected_outcome: str
    artifact_format: str


class CompletionContractDict(TypedDict):
    identity: LaneIdentityDict
    revision: int
    contract_hash: str
    objective_hash: str
    finding_ids: tuple[str, ...] | None
    acceptance_ids: tuple[str, ...]
    required_commands: tuple[str, ...] | None
    approved: bool
    exhaustive: bool
    evidence_ids: tuple[str, ...]
    non_test_acceptance_id: str | None
    acceptance_verifiers: tuple[AcceptanceVerifierDict, ...]


class FindingAssignment(TypedDict):
    db_id: int
    finding_id: str


class ObligationDict(TypedDict):
    kind: str
    id: str


class ObligationTransferDict(TypedDict):
    predecessor: ObligationDict
    carrier: ObligationDict


class TransferEdge(TypedDict):
    """Admission transfer; carrier hash is supplied by the resulting record.

    Matches the evaluator's TransferEvidence fields except carrier_contract_hash,
    which cannot be inside its own hashed declaration. Source identity includes
    product_tip; authority and evidence name the owner's transfer decision.
    This declaration never establishes carrier completion.
    """

    source: LaneIdentityDict
    carrier: LaneIdentityDict
    source_contract_hash: str
    mappings: tuple[ObligationTransferDict, ...]
    authenticated: bool
    authority_id: str
    evidence_ids: tuple[str, ...]


class RefusalCode(str, Enum):
    EMPTY_ACCEPTANCE = "empty_acceptance"
    ASSIGNMENT_ABSENT = "assignment_absent"
    INVALID_ASSIGNMENT = "invalid_assignment"
    UNKNOWN_FINDING = "unknown_finding"
    FINDING_IDENTITY_MISMATCH = "finding_identity_mismatch"
    COMMANDS_ABSENT = "commands_absent"
    INVALID_VERIFIERS = "invalid_verifiers"
    INVALID_CONTRACT = "invalid_contract"
    IDENTITY_MISMATCH = "identity_mismatch"
    AMBIGUOUS_ASSIGNMENT = "ambiguous_assignment"
    INVALID_TRANSFER = "invalid_transfer"
    CONTRACT_ABSENT = "contract_absent"
    CONTRACT_MISMATCH = "contract_mismatch"


class ContractRefusal(ValueError):
    """Typed refusal. Consumers must hold admission on every such result."""

    hold = True

    def __init__(self, code: RefusalCode, detail: str):
        self.code = code
        super().__init__(f"{code.value}: {detail}")


@dataclass(frozen=True)
class ContractRecord:
    contract: CompletionContractDict
    assigned_findings: tuple[FindingAssignment, ...]
    empty_assignment_declared: bool
    objective: str
    integration_ref: str
    transfers: tuple[TransferEdge, ...]
    non_test_reason: str | None
    test_target_policy: tuple[str, ...]
    idempotency_key: str

    @property
    def revision(self) -> int:
        return self.contract["revision"]

    @property
    def contract_hash(self) -> str:
        return self.contract["contract_hash"]


def _canonical(value):
    if is_dataclass(value):
        return {field.name: _canonical(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return sorted((_canonical(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, Mapping):
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def canonical_json(value) -> str:
    return json.dumps(_canonical(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def canonical_hash(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _require(condition, code, detail):
    if not condition:
        raise ContractRefusal(code, detail)


def _text(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _strings(value, code, name, *, nonempty=False):
    _require(isinstance(value, (tuple, list)), code, f"{name} must be an explicit sequence")
    _require(all(_text(item) for item in value), code, f"{name} contains an empty/non-string identity")
    _require(len(set(value)) == len(value), code, f"{name} contains duplicates")
    _require(not nonempty or bool(value), code, f"{name} must not be empty")
    return tuple(sorted(value))


def _identity(value):
    code = RefusalCode.IDENTITY_MISMATCH
    _require(
        isinstance(value, dict) and value.keys() == LaneIdentityDict.__annotations__.keys(), code, "identity fields"
    )
    nullable = {"identity_seq", "cycle_id"}
    integers = {"lane_row_id", "identity_seq"}
    for key, item in value.items():
        if key in nullable and item is None:
            continue
        if key in integers:
            _require(type(item) is int and item > 0, code, f"invalid {key}")
        else:
            _require(_text(item), code, f"invalid {key}")
    return dict(value)


def _verifiers(value, acceptance_ids):
    code = RefusalCode.INVALID_VERIFIERS
    _require(isinstance(value, (tuple, list)), code, "verifiers must be explicit")
    ids = []
    for entry in value:
        _require(
            isinstance(entry, dict) and entry.keys() == AcceptanceVerifierDict.__annotations__.keys(),
            code,
            "each verifier requires exactly the AcceptanceVerifier fields",
        )
        _require(
            type(entry["version"]) is int and entry["version"] > 0, code, "verifier version must be a positive int"
        )
        _require(all(_text(v) for k, v in entry.items() if k != "version"), code, "empty verifier field")
        ids.append(entry["acceptance_id"])
    _require(
        len(ids) == len(set(ids)) and set(ids) == set(acceptance_ids), code, "exactly one verifier per acceptance id"
    )
    return tuple(dict(entry) for entry in sorted(value, key=lambda entry: entry["acceptance_id"]))


def _assignments(value, empty_declared):
    _require(isinstance(value, (tuple, list)), RefusalCode.ASSIGNMENT_ABSENT, "assigned findings must be explicit")
    code = RefusalCode.INVALID_ASSIGNMENT
    _require(type(empty_declared) is bool, code, "empty declaration must be boolean")
    if not value:
        _require(empty_declared, RefusalCode.ASSIGNMENT_ABSENT, "empty assignment needs an owner declaration")
    else:
        _require(not empty_declared, code, "nonempty assignment conflicts with empty declaration")
    for entry in value:
        _require(
            isinstance(entry, dict) and set(entry) == {"db_id", "finding_id"}, code, "finding requires both identities"
        )
        _require(type(entry["db_id"]) is int and entry["db_id"] > 0, code, "invalid finding DB id")
        _require(_text(entry["finding_id"]), code, "invalid stable finding id")
    _require(len({v["db_id"] for v in value}) == len(value), code, "duplicate finding DB id")
    _require(len({v["finding_id"] for v in value}) == len(value), code, "duplicate stable finding id")
    return tuple(dict(entry) for entry in sorted(value, key=lambda entry: entry["db_id"]))


def _transfer_shape(transfers):
    code = RefusalCode.INVALID_TRANSFER
    _require(isinstance(transfers, (tuple, list)), code, "transfers must be a sequence")
    result = []
    for edge in transfers:
        _require(isinstance(edge, dict) and edge.keys() == TransferEdge.__annotations__.keys(), code, "transfer fields")
        source, carrier = _identity(edge["source"]), _identity(edge["carrier"])
        _require(source != carrier, code, "transfer requires a different carrier")
        _require(edge["authenticated"] is True and _text(edge["authority_id"]), code, "transfer needs owner authority")
        evidence = _strings(edge["evidence_ids"], code, "transfer evidence", nonempty=True)
        _require(_text(edge["source_contract_hash"]), code, "source contract hash absent")
        _require(
            isinstance(edge["mappings"], (tuple, list)) and bool(edge["mappings"]), code, "transfer coverage absent"
        )
        mappings = []
        for mapping in edge["mappings"]:
            _require(isinstance(mapping, dict) and set(mapping) == {"predecessor", "carrier"}, code, "mapping fields")
            for obligation in mapping.values():
                _require(isinstance(obligation, dict) and set(obligation) == {"kind", "id"}, code, "obligation fields")
                _require(
                    obligation["kind"] in ("finding", "acceptance", "test") and _text(obligation["id"]),
                    code,
                    "obligation",
                )
            mappings.append({key: dict(item) for key, item in mapping.items()})
        result.append(dict(edge, source=source, carrier=carrier, mappings=tuple(mappings), evidence_ids=evidence))
    return tuple(result)


def _payload(record):
    return {field.name: getattr(record, field.name) for field in fields(record) if field.name != "idempotency_key"}


def _digests(payload):
    # JSON copy detaches the stored/result dictionaries before blanking fields.
    content = json.loads(canonical_json(payload))
    content["contract"]["contract_hash"] = ""
    digest = canonical_hash(content)
    content["contract"]["revision"] = 0
    return digest, canonical_hash(content)


def _decode(row) -> ContractRecord:
    """Verify indexed identity, revision and hash before exposing obligations."""
    code = RefusalCode.CONTRACT_MISMATCH
    try:
        payload = json.loads(row["payload_json"])
        contract = payload["contract"]
        identity = contract["identity"]
        digest, key = _digests(payload)
        _require(
            contract["revision"] == row["revision"] and contract["contract_hash"] == row["contract_hash"] == digest,
            code,
            "contract hash/revision mismatch; hold",
        )
        _require(
            all(identity[k] == row[k] for k in ("task_ref", "lane_id", "lane_row_id", "identity_seq"))
            and key == row["idempotency_key"],
            code,
            "indexed identity/idempotency mismatch; hold",
        )
        _require(set(contract) == CompletionContractDict.__annotations__.keys(), code, "contract shape mismatch")
        contract["identity"] = _identity(identity)
        for field in ("finding_ids", "acceptance_ids", "required_commands", "evidence_ids"):
            contract[field] = tuple(contract[field]) if contract[field] is not None else None
        contract["acceptance_verifiers"] = _verifiers(contract["acceptance_verifiers"], contract["acceptance_ids"])
        assignments = _assignments(payload["assigned_findings"], payload["empty_assignment_declared"])
        _require(
            set(contract["finding_ids"]) == {f["finding_id"] for f in assignments}, code, "finding declaration mismatch"
        )
        _require(
            hashlib.sha256(payload["objective"].encode()).hexdigest() == contract["objective_hash"],
            code,
            "objective hash",
        )
        transfers = _transfer_shape(payload["transfers"])
        return ContractRecord(
            contract,
            assignments,
            payload["empty_assignment_declared"],
            payload["objective"],
            payload["integration_ref"],
            transfers,
            payload["non_test_reason"],
            tuple(payload["test_target_policy"]),
            key,
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        if isinstance(exc, ContractRefusal) and exc.code == code:
            raise
        raise ContractRefusal(code, "malformed or replaced contract; hold") from exc


def _latest(conn, task_ref, lane_id):
    return conn.execute(
        "SELECT * FROM lane_completion_contracts WHERE task_ref=? AND lane_id=? ORDER BY revision DESC LIMIT 1",
        (task_ref, lane_id),
    ).fetchone()


def _obligations(contract):
    return {
        (kind, item)
        for kind, items in (
            ("finding", contract["finding_ids"]),
            ("acceptance", contract["acceptance_ids"]),
            ("test", contract["required_commands"]),
        )
        for item in items or ()
    }


def _validate_transfers(conn, record):
    code = RefusalCode.INVALID_TRANSFER
    for edge in record.transfers:
        source = edge["source"]
        row = _latest(conn, source["task_ref"], source["lane_id"])
        _require(row is not None, code, "source contract absent")
        previous = _decode(row)
        _require(
            previous.contract["identity"] == source
            and previous.contract_hash == edge["source_contract_hash"]
            and edge["carrier"] == record.contract["identity"],
            code,
            "transfer identity/hash mismatch",
        )
        for mapping in edge["mappings"]:
            before, after = mapping["predecessor"], mapping["carrier"]
            _require(
                (before["kind"], before["id"]) in _obligations(previous.contract)
                and (after["kind"], after["id"]) in _obligations(record.contract),
                code,
                "transfer names unassigned obligations",
            )


def _validate_ownership(conn, record):
    identity = record.contract["identity"]
    rows = conn.execute(
        "SELECT c.* FROM lane_completion_contracts c "
        "WHERE c.task_ref=? AND c.lane_id!=? AND c.revision=("
        "SELECT MAX(p.revision) FROM lane_completion_contracts p WHERE p.task_ref=c.task_ref AND p.lane_id=c.lane_id)",
        (identity["task_ref"], identity["lane_id"]),
    ).fetchall()
    for row in rows:
        previous = _decode(row)
        overlap = set(previous.contract["finding_ids"]) & set(record.contract["finding_ids"])
        for finding in overlap:
            covered = any(
                edge["source"] == previous.contract["identity"]
                and edge["source_contract_hash"] == previous.contract_hash
                and edge["carrier"] == identity
                and any(
                    mapping["predecessor"] == {"kind": "finding", "id": finding}
                    and mapping["carrier"] == {"kind": "finding", "id": finding}
                    for mapping in edge["mappings"]
                )
                for edge in record.transfers
            )
            _require(covered, RefusalCode.AMBIGUOUS_ASSIGNMENT, f"{finding} is already assigned to {row['lane_id']}")


def record_contract(
    *,
    identity: LaneIdentityDict,
    objective: str,
    integration_ref: str,
    acceptance_ids: Sequence[str],
    acceptance_verifiers: Sequence[AcceptanceVerifierDict],
    approved: bool,
    exhaustive: bool,
    evidence_ids: Sequence[str],
    assigned_findings: Sequence[FindingAssignment] | None = None,
    empty_assignment_declared: bool = False,
    required_commands: Sequence[str] | None = None,
    non_test_acceptance_id: str | None = None,
    transfers: Sequence[TransferEdge] = (),
    non_test_reason: str | None = None,
    test_target_policy: Sequence[str] = ("candidate", "integration"),
) -> ContractRecord:
    """Validate ownership and atomically append; identical declarations reuse a revision.

    Explicit ``assigned_findings=(), empty_assignment_declared=True`` declares no
    findings. Explicit ``required_commands=()`` requires a named non-test
    acceptance verifier, owner-decision evidence and reason. No row is written
    on refusal. Test target policy names the implementation note completion phases.
    """
    identity = _identity(identity)
    code = RefusalCode.INVALID_CONTRACT
    _require(_text(objective) and _text(integration_ref), code, "objective/integration ref required")
    acceptance = _strings(acceptance_ids, RefusalCode.EMPTY_ACCEPTANCE, "acceptance ids", nonempty=True)
    verifiers = _verifiers(acceptance_verifiers, acceptance)
    assignments = _assignments(assigned_findings, empty_assignment_declared)
    commands = _strings(required_commands, RefusalCode.COMMANDS_ABSENT, "required commands")
    evidence = _strings(evidence_ids, code, "owner evidence", nonempty=True)
    _require(approved is True and exhaustive is True, code, "approved exhaustive scope required")
    _require(
        non_test_acceptance_id is None or non_test_acceptance_id in acceptance, code, "unknown non-test acceptance"
    )
    _require(
        bool(commands) or non_test_acceptance_id in acceptance,
        code,
        "no tests requires explicit alternative acceptance",
    )
    _require(non_test_reason is None or _text(non_test_reason), code, "invalid non-test reason")
    _require(
        bool(commands)
        or (
            _text(non_test_reason)
            and any(v["acceptance_id"] == non_test_acceptance_id and v["kind"] != "test" for v in verifiers)
        ),
        code,
        "no tests requires a reason and alternative verifier",
    )
    target_policy = _strings(test_target_policy, code, "test target policy", nonempty=True)
    _require(set(target_policy) <= {"candidate", "integration"}, code, "unknown completion phase in test policy")
    edges = _transfer_shape(transfers)
    contract: CompletionContractDict = {
        "identity": identity,
        "revision": 0,
        "contract_hash": "",
        "objective_hash": hashlib.sha256(objective.encode()).hexdigest(),
        "finding_ids": tuple(sorted(f["finding_id"] for f in assignments)),
        "acceptance_ids": acceptance,
        "required_commands": commands,
        "approved": approved,
        "exhaustive": exhaustive,
        "evidence_ids": evidence,
        "non_test_acceptance_id": non_test_acceptance_id,
        "acceptance_verifiers": verifiers,
    }
    provisional = ContractRecord(
        contract,
        assignments,
        empty_assignment_declared,
        objective,
        integration_ref,
        edges,
        non_test_reason,
        target_policy,
        "",
    )
    _, key = _digests(_payload(provisional))
    with _get_db_connection(begin_immediate=True) as conn:
        lane = conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (identity["lane_row_id"],)).fetchone()
        _require(
            lane is not None and all(lane[k] == identity[k] for k in ("task_ref", "lane_id", "identity_seq", "branch")),
            RefusalCode.IDENTITY_MISMATCH,
            "lane row identity changed or absent",
        )
        task = conn.execute("SELECT revision FROM handoff_state WHERE task_ref=?", (identity["task_ref"],)).fetchone()
        _require(
            task is not None and str(task["revision"]) == identity["task_revision"],
            RefusalCode.IDENTITY_MISMATCH,
            "task identity/revision changed or absent",
        )
        for assignment in assignments:
            findings = conn.execute(
                "SELECT task_ref, finding_id FROM review_findings WHERE id=? UNION ALL "
                "SELECT task_ref, finding_id FROM review_findings_archive WHERE id=?",
                (assignment["db_id"], assignment["db_id"]),
            ).fetchall()
            _require(bool(findings), RefusalCode.UNKNOWN_FINDING, f"unknown DB finding {assignment['db_id']}")
            _require(
                all(
                    f["task_ref"] == identity["task_ref"] and f["finding_id"] == assignment["finding_id"]
                    for f in findings
                ),
                RefusalCode.FINDING_IDENTITY_MISMATCH,
                "DB/stable finding identity mismatch",
            )
        existing = conn.execute("SELECT * FROM lane_completion_contracts WHERE idempotency_key=?", (key,)).fetchone()
        if existing is not None:
            return _decode(existing)
        latest = _latest(conn, identity["task_ref"], identity["lane_id"])
        contract["revision"] = _decode(latest).revision + 1 if latest is not None else 1
        contract["contract_hash"], _ = _digests(_payload(provisional))
        record = ContractRecord(
            contract,
            assignments,
            empty_assignment_declared,
            objective,
            integration_ref,
            edges,
            non_test_reason,
            target_policy,
            key,
        )
        _validate_transfers(conn, record)
        _validate_ownership(conn, record)
        conn.execute(
            "INSERT INTO lane_completion_contracts "
            "(task_ref, lane_row_id, lane_id, identity_seq, revision, contract_hash, payload_json, idempotency_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                identity["task_ref"],
                identity["lane_row_id"],
                identity["lane_id"],
                identity["identity_seq"],
                record.revision,
                record.contract_hash,
                canonical_json(_payload(record)),
                key,
            ),
        )
        # Return exactly the same normalized shape as a later fresh-connection read.
        return _decode(_latest(conn, identity["task_ref"], identity["lane_id"]))


def load_contract(task_ref: str, lane_id: str, revision: int | None = None) -> ContractRecord:
    """Read the latest or exact revision; missing or mismatched contracts hold."""
    code = RefusalCode.INVALID_CONTRACT
    _require(_text(task_ref) and _text(lane_id), code, "task/lane required")
    _require(revision is None or (type(revision) is int and revision > 0), code, "invalid revision")
    with _get_db_connection() as conn:
        row = (
            _latest(conn, task_ref, lane_id)
            if revision is None
            else conn.execute(
                "SELECT * FROM lane_completion_contracts WHERE task_ref=? AND lane_id=? AND revision=?",
                (task_ref, lane_id, revision),
            ).fetchone()
        )
        _require(row is not None, RefusalCode.CONTRACT_ABSENT, "contract revision unavailable; hold")
        return _decode(row)
