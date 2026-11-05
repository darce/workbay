"""Append-only storage for lane-production-v1; no N/R or writer authentication.

Trusted writer adapters authenticate callers before constructing ProductionAppend.
This layer preserves writer_authority_ref verbatim and checks current DB identity
and contract, replay bytes, predecessor CAS and operation ordering atomically.
Facts carry the full canonical admission/operation/seal/transition receipts; this
module does not attest their source completeness or authenticate their authority.

Manifest facts use ``manifest`` (path-keyed mapping or entries with ``path``),
``manifest_paths`` or ``paths``. The reader counts these recursively. Immutable
external manifests remain references: resolving/verifying them belongs to the
collector, and a reference alone never certifies complete production history.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from typing import Any, TypedDict

from .lane_completion_recording import ContractRefusal, LaneIdentityDict, _identity
from .lane_completion_recording import _decode as _decode_contract
from .shared_primitives import _envelope
from .shared_schema import _HANDOFF_REQUIRED_BASE_COLUMNS, _HANDOFF_REQUIRED_COLUMNS, _get_db_connection

FORMAT_VERSION = "lane-production-v1"
MAX_PAGE_ROWS = 200
MAX_PAGES = 2
MAX_DECODED_BYTES = 512 * 1024
MAX_MANIFEST_PATHS = 200


class ProductionTarget(TypedDict):
    phase: str
    candidate_sha: str
    integration_ref: str
    integration_sha: str
    merge_tree_oid: str | None
    landing_sha: str | None


class ProductionBinding(TypedDict):
    identity: LaneIdentityDict
    contract_revision: int
    contract_hash: str
    target: ProductionTarget | None


class RefusalCode(str, Enum):
    INVALID_RECORD = "invalid_record"
    IDENTITY_MISMATCH = "identity_mismatch"
    CONTRACT_MISMATCH = "contract_mismatch"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    HEAD_CONFLICT = "head_conflict"
    ADMISSION_ABSENT = "admission_absent"
    ADMISSION_AMBIGUOUS = "admission_ambiguous"
    ADMISSION_CONFLICT = "admission_conflict"
    OPERATION_ABSENT = "operation_absent"
    OPERATION_CONFLICT = "operation_conflict"
    OPEN_OPERATIONS = "open_operations"
    UNSUPPORTED_KIND = "unsupported_kind"
    CORRUPT_RECORD = "corrupt_record"
    INVALID_CURSOR = "invalid_cursor"


class ProductionRefusal(ValueError):
    def __init__(self, code: RefusalCode, detail: str):
        self.code = code
        super().__init__(detail)


def _require(condition: bool, code: RefusalCode, detail: str) -> None:
    if not condition:
        raise ProductionRefusal(code, detail)


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _plain(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _plain(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        _require(all(isinstance(key, str) for key in value), RefusalCode.INVALID_RECORD, "JSON keys must be strings")
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        # Source order is a fact. Unlike unordered contract declarations, do
        # not sort operation manifests/cursor intervals during normalization.
        return [_plain(item) for item in value]
    return value


def _json(value: Any) -> str:
    return json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class ProductionHead:
    sequence: int
    evidence_id: str | None


@dataclass(frozen=True, kw_only=True)
class ProductionAppend:
    binding: ProductionBinding
    admission_id: str
    operation_id: str
    producer_kind: str
    producer_version: str
    writer_authority_ref: str
    expected_head: ProductionHead
    facts: Mapping[str, Any]


@dataclass(frozen=True)
class ProductionRecord:
    evidence_id: str
    sequence: int
    previous_evidence_id: str | None
    evidence_kind: str
    payload_json: str

    @property
    def head(self) -> ProductionHead:
        return ProductionHead(self.sequence, self.evidence_id)


class ProductionCursor(TypedDict):
    lineage_digest: str
    head: dict[str, Any]
    after_sequence: int
    page: int
    remaining: int
    decoded_bytes: int
    manifest_paths: int


def _binding(value: Any, *, seal: bool = False) -> dict:
    try:
        value = _plain(value)
        _require(
            isinstance(value, dict) and set(value) == set(ProductionBinding.__annotations__),
            RefusalCode.INVALID_RECORD,
            "binding fields",
        )
        try:
            value["identity"] = _identity(value["identity"])
        except ContractRefusal as exc:
            raise ProductionRefusal(RefusalCode.IDENTITY_MISMATCH, "full lane identity required") from exc
        _require(
            type(value["contract_revision"]) is int
            and value["contract_revision"] > 0
            and _text(value["contract_hash"]),
            RefusalCode.CONTRACT_MISMATCH,
            "contract revision/hash required",
        )
        target = value["target"]
        _require(not seal or target is not None, RefusalCode.INVALID_RECORD, "seal requires exact target")
        if target is not None:
            _require(
                isinstance(target, dict) and set(target) == set(ProductionTarget.__annotations__),
                RefusalCode.INVALID_RECORD,
                "target fields",
            )
            _require(
                target["phase"] in ("candidate", "integration")
                and all(_text(target[k]) for k in ("candidate_sha", "integration_ref", "integration_sha"))
                and all(target[k] is None or _text(target[k]) for k in ("merge_tree_oid", "landing_sha")),
                RefusalCode.INVALID_RECORD,
                "invalid target",
            )
        return value
    except (TypeError, KeyError, AttributeError) as exc:
        raise ProductionRefusal(RefusalCode.INVALID_RECORD, "malformed binding") from exc


def _validate_binding(conn, binding: dict, *, native_cycle: bool = False) -> None:
    identity = binding["identity"]
    lane = conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (identity["lane_row_id"],)).fetchone()
    task = conn.execute("SELECT revision FROM handoff_state WHERE task_ref=?", (identity["task_ref"],)).fetchone()
    _require(
        lane is not None
        and all(lane[k] == identity[k] for k in ("task_ref", "lane_id", "identity_seq", "branch"))
        and lane["updated_at"] == identity["lane_revision"]
        and (not native_cycle or str(lane["cycle_epoch"]) == identity["cycle_id"])
        and task is not None
        and str(task["revision"]) == identity["task_revision"],
        RefusalCode.IDENTITY_MISMATCH,
        "current lane/task identity changed or absent",
    )
    row = conn.execute(
        "SELECT * FROM lane_completion_contracts WHERE task_ref=? AND lane_id=? ORDER BY revision DESC LIMIT 1",
        (identity["task_ref"], identity["lane_id"]),
    ).fetchone()
    _require(
        row is not None
        and row["revision"] == binding["contract_revision"]
        and row["contract_hash"] == binding["contract_hash"],
        RefusalCode.CONTRACT_MISMATCH,
        "current contract revision/hash changed or absent",
    )
    try:
        contract = _decode_contract(row).contract
    except ContractRefusal as exc:
        raise ProductionRefusal(RefusalCode.CONTRACT_MISMATCH, "invalid stored contract") from exc
    _require(contract["identity"] == identity, RefusalCode.IDENTITY_MISMATCH, "complete contract identity mismatch")
    _require(
        contract["approved"] is True and contract["exhaustive"] is True,
        RefusalCode.CONTRACT_MISMATCH,
        "approved exhaustive contract required",
    )


def _lineage(binding, admission_id):
    identity = binding["identity"]
    return (
        identity["repository"],
        identity["task_ref"],
        identity["lane_row_id"],
        identity["identity_seq"],
        admission_id,
    )


def _validate_retained_binding(conn, binding: dict) -> None:
    """Read authority only: an immutable revision, never the current lane row."""
    identity = binding["identity"]
    row = conn.execute(
        "SELECT * FROM lane_completion_contracts WHERE task_ref=? AND lane_id=? AND revision=?",
        (identity["task_ref"], identity["lane_id"], binding["contract_revision"]),
    ).fetchone()
    _require(row is not None, RefusalCode.CONTRACT_MISMATCH, "retained contract absent")
    try:
        contract = _decode_contract(row).contract
    except ContractRefusal as exc:
        raise ProductionRefusal(RefusalCode.CONTRACT_MISMATCH, "invalid retained contract") from exc
    _require(
        contract["contract_hash"] == binding["contract_hash"]
        and contract["approved"] is True
        and contract["exhaustive"] is True,
        RefusalCode.CONTRACT_MISMATCH,
        "retained approved exhaustive contract revision/hash mismatch",
    )
    _require(contract["identity"] == identity, RefusalCode.IDENTITY_MISMATCH, "full retained identity mismatch")


def _receipt_columns() -> str:
    return ", ".join(
        sorted(
            (
                _HANDOFF_REQUIRED_COLUMNS["lane_completion_evidence"]
                | _HANDOFF_REQUIRED_BASE_COLUMNS["lane_completion_evidence"]
            )
            - {"payload_json"}
        )
    )


def _bounded_receipt(conn, evidence_id: str, byte_limit: int):
    return conn.execute(
        "SELECT "
        + _receipt_columns()
        + ", CASE WHEN length(CAST(payload_json AS BLOB))<=? THEN payload_json END AS payload_json, "
        "length(CAST(payload_json AS BLOB)) AS payload_bytes "
        "FROM lane_completion_evidence WHERE evidence_id=?",
        (byte_limit, evidence_id),
    ).fetchone()


def _match_root(record: ProductionRecord, binding: dict) -> dict:
    payload = json.loads(record.payload_json)
    retained = _binding(payload["binding"])
    _require(
        record.sequence == 1 and record.previous_evidence_id is None and record.evidence_kind == "production_admission",
        RefusalCode.CORRUPT_RECORD,
        "invalid admission root",
    )
    _require(retained["identity"] == binding["identity"], RefusalCode.IDENTITY_MISMATCH, "full root identity mismatch")
    _require(
        retained["contract_revision"] == binding["contract_revision"]
        and retained["contract_hash"] == binding["contract_hash"],
        RefusalCode.CONTRACT_MISMATCH,
        "root contract revision/hash mismatch",
    )
    return retained


def _same_point(left: dict, right: dict) -> bool:
    """Targets may change per operation; identity and contract move only by link."""
    return all(left[key] == right[key] for key in ("identity", "contract_revision", "contract_hash"))


def _verify_predecessor(record: ProductionRecord, sequence: int, previous: str | None) -> None:
    _require(
        record.sequence == sequence and record.previous_evidence_id == previous,
        RefusalCode.CORRUPT_RECORD,
        "broken production predecessor chain",
    )


def _resolve_current_binding(conn, root: ProductionRecord, binding: dict) -> tuple[dict, int]:
    """Authenticate the complete bounded lineage in the caller's DB snapshot.

    Contract hashes cover their revisions, so each before hash names the exact
    immutable revision at the walk's current point. Never rewrite old bindings.
    """
    _validate_binding(conn, binding, native_cycle=True)
    payload = json.loads(root.payload_json)
    retained = _binding(payload["binding"])
    _match_root(root, retained)
    admission_id = payload["admission_id"]
    lineage = _lineage(retained, admission_id)
    _require(
        _lineage(binding, admission_id) == lineage,
        RefusalCode.IDENTITY_MISMATCH,
        "foreign admission lineage",
    )
    _validate_retained_binding(conn, retained)
    head = _head(conn, lineage[0], admission_id)
    row_cap = MAX_PAGE_ROWS * MAX_PAGES
    _require(head.sequence <= row_cap, RefusalCode.CORRUPT_RECORD, "transition lookup row cap exceeded")
    rows = conn.execute(
        "SELECT "
        + _receipt_columns()
        + ", CASE WHEN length(CAST(payload_json AS BLOB))<=? THEN payload_json END AS payload_json, "
        "length(CAST(payload_json AS BLOB)) AS payload_bytes FROM lane_completion_evidence "
        "WHERE repository_key=? AND admission_id=? AND evidence_kind LIKE 'production_%' "
        "ORDER BY sequence LIMIT ?",
        (MAX_DECODED_BYTES, lineage[0], admission_id, row_cap + 1),
    )
    point, previous = retained, None
    transitions, count, decoded_bytes, manifest_paths = 0, 0, 0, 0
    before_points = set()
    for row in rows:
        _require(count < row_cap, RefusalCode.CORRUPT_RECORD, "transition lookup row cap exceeded")
        _require(
            row["payload_json"] is not None and row["payload_bytes"] <= MAX_DECODED_BYTES - decoded_bytes,
            RefusalCode.CORRUPT_RECORD,
            "transition lookup byte cap exceeded",
        )
        record = _decode(row)
        _verify_predecessor(record, count + 1, previous)
        receipt = json.loads(record.payload_json)
        receipt_binding = _binding(receipt["binding"])
        _require(
            _lineage(receipt_binding, receipt["admission_id"]) == lineage
            and receipt_binding["identity"]["lane_id"] == retained["identity"]["lane_id"],
            RefusalCode.IDENTITY_MISMATCH,
            "foreign receipt lineage",
        )
        _validate_retained_binding(conn, receipt_binding)
        facts = receipt["facts"]
        manifest_paths += _path_count(facts)
        _require(
            manifest_paths <= MAX_MANIFEST_PATHS,
            RefusalCode.CORRUPT_RECORD,
            "transition lookup path cap exceeded",
        )
        if record.evidence_kind == "production_contract_link":
            _require(
                isinstance(facts, dict) and facts.get("predecessor_head") == asdict(ProductionHead(count, previous)),
                RefusalCode.CORRUPT_RECORD,
                "transition predecessor head mismatch",
            )
            _require(
                facts.get("after_identity") == receipt_binding["identity"]
                and facts.get("after_contract_hash") == receipt_binding["contract_hash"],
                RefusalCode.CORRUPT_RECORD,
                "transition after does not match receipt binding",
            )
            before = _json((facts.get("before_identity"), facts.get("before_contract_hash")))
            _require(
                before not in before_points,
                RefusalCode.ADMISSION_AMBIGUOUS,
                "branching production identity transitions",
            )
            before_points.add(before)
            _require(
                facts.get("before_identity") == point["identity"]
                and facts.get("before_contract_hash") == point["contract_hash"],
                RefusalCode.IDENTITY_MISMATCH,
                "transition before does not match retained binding",
            )
            point = receipt_binding
            transitions += 1
        else:
            _require(
                _same_point(receipt_binding, point),
                RefusalCode.IDENTITY_MISMATCH,
                "receipt binding has no retained identity transition",
            )
        previous = record.evidence_id
        count += 1
        decoded_bytes += row["payload_bytes"]
    _require(
        count == head.sequence and previous == head.evidence_id,
        RefusalCode.CORRUPT_RECORD,
        "incomplete production chain at retained head",
    )
    _require(
        _same_point(point, binding),
        RefusalCode.IDENTITY_MISMATCH,
        "transition chain does not reach current binding" if transitions else "no retained identity transition",
    )
    return retained, transitions


def get_lane_production_admission(*, task_ref: str, lane_id: str) -> dict:
    """Discover one retained root for the current lane and latest contract.

    The returned binding remains subject to current validation on every append.
    No admission is created or inferred; targets come from the retained root.
    """
    _require(_text(task_ref) and _text(lane_id), RefusalCode.INVALID_RECORD, "task_ref/lane_id required")
    with _get_db_connection() as conn:
        conn.execute("BEGIN")
        lane = conn.execute(
            "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (task_ref, lane_id)
        ).fetchone()
        _require(lane is not None, RefusalCode.IDENTITY_MISMATCH, "current lane absent")
        row = conn.execute(
            "SELECT * FROM lane_completion_contracts WHERE task_ref=? AND lane_id=? ORDER BY revision DESC LIMIT 1",
            (task_ref, lane_id),
        ).fetchone()
        _require(row is not None, RefusalCode.CONTRACT_MISMATCH, "current contract absent")
        try:
            contract = _decode_contract(row).contract
        except ContractRefusal as exc:
            raise ProductionRefusal(RefusalCode.CONTRACT_MISMATCH, "invalid current contract") from exc
        binding = dict(
            identity=contract["identity"],
            contract_revision=contract["revision"],
            contract_hash=contract["contract_hash"],
            target=None,
        )
        _validate_binding(conn, binding, native_cycle=True)
        roots = conn.execute(
            "SELECT evidence_id, admission_id FROM lane_completion_evidence "
            "WHERE task_ref=? AND lane_id=? AND lane_row_id=? AND identity_seq IS ? "
            "AND evidence_kind='production_admission' ORDER BY evidence_id LIMIT 2",
            (task_ref, lane_id, lane["id"], lane["identity_seq"]),
        ).fetchall()
        _require(bool(roots), RefusalCode.ADMISSION_ABSENT, "current admission absent")
        _require(len(roots) == 1, RefusalCode.ADMISSION_AMBIGUOUS, "multiple live admission roots")
        root = _bounded_receipt(conn, roots[0]["evidence_id"], MAX_DECODED_BYTES)
        _require(root["payload_json"] is not None, RefusalCode.CORRUPT_RECORD, "admission exceeds lookup byte cap")
        record = _decode(root)
        retained = _binding(json.loads(record.payload_json)["binding"])
        transitions = 0
        if (
            retained["identity"] == binding["identity"]
            and retained["contract_revision"] == binding["contract_revision"]
        ):
            _match_root(record, binding)
        else:
            # Preserve the existing refusal for an unlinked contract-only refresh.
            linked = conn.execute(
                "SELECT 1 FROM lane_completion_evidence WHERE repository_key=? AND admission_id=? "
                "AND evidence_kind='production_contract_link' LIMIT 1",
                (binding["identity"]["repository"], roots[0]["admission_id"]),
            ).fetchone()
            if linked is None and retained["identity"] == binding["identity"]:
                _match_root(record, binding)
            retained, transitions = _resolve_current_binding(conn, record, binding)
        binding["target"] = retained["target"]
        data = {"task_ref": task_ref, "admission_id": roots[0]["admission_id"], "binding": binding}
        if transitions:
            data.update(root_binding=retained, transitions=transitions)
    return _envelope(ok=True, tool="get_lane_production_admission", data=data, task_ref=task_ref)


def _head(conn, repository, admission_id) -> ProductionHead:
    row = conn.execute(
        "SELECT sequence, evidence_id FROM lane_completion_evidence "
        "WHERE repository_key=? AND admission_id=? AND evidence_kind LIKE 'production_%' "
        "ORDER BY sequence DESC LIMIT 1",
        (repository, admission_id),
    ).fetchone()
    return ProductionHead(row["sequence"], row["evidence_id"]) if row else ProductionHead(0, None)


def _open_operations(conn, repository, admission_id, high_water):
    return conn.execute(
        "SELECT COUNT(*) FROM lane_completion_evidence p WHERE p.repository_key=? AND p.admission_id=? "
        "AND p.evidence_kind='production_prepare' AND p.sequence<=? AND NOT EXISTS ("
        "SELECT 1 FROM lane_completion_evidence s WHERE s.repository_key=p.repository_key "
        "AND s.admission_id=p.admission_id AND s.operation_id=p.operation_id "
        "AND s.evidence_kind='production_settle' AND s.sequence<=?)",
        (repository, admission_id, high_water, high_water),
    ).fetchone()[0]


def _indexed(payload: dict) -> dict:
    binding = payload["binding"]
    identity = binding["identity"]
    target = binding["target"] or {}
    return {
        "schema_version": payload["schema_version"],
        "evidence_kind": payload["evidence_kind"],
        "repository_key": identity["repository"],
        **{key: identity[key] for key in ("task_ref", "lane_id", "lane_row_id", "identity_seq", "cycle_id")},
        "admission_id": payload["admission_id"],
        "contract_revision": binding["contract_revision"],
        "contract_hash": binding["contract_hash"],
        "target_phase": target.get("phase"),
        **{
            key: target.get(key)
            for key in ("candidate_sha", "integration_ref", "integration_sha", "merge_tree_oid", "landing_sha")
        },
        **{
            key: payload[key]
            for key in (
                "sequence",
                "previous_evidence_id",
                "operation_id",
                "operation_phase",
                "producer_kind",
                "producer_version",
                "writer_authority_ref",
            )
        },
        "source_cursor": _json(payload["facts"]["source_cursors"]) if "source_cursors" in payload["facts"] else None,
        "source_digest": payload["facts"].get("source_digest"),
    }


def _decode(row) -> ProductionRecord:
    try:
        raw = row["payload_json"]
        payload = json.loads(raw)
        _require(
            _json(payload) == raw
            and _digest(raw) == row["payload_sha256"] == row["evidence_id"]
            and all(row[key] == value for key, value in _indexed(payload).items())
            and row["idempotency_key"] == _idempotency(payload),
            RefusalCode.CORRUPT_RECORD,
            "replaced payload/indexed identity; hold",
        )
        return ProductionRecord(
            row["evidence_id"], row["sequence"], row["previous_evidence_id"], row["evidence_kind"], raw
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, ProductionRefusal):
            raise
        raise ProductionRefusal(RefusalCode.CORRUPT_RECORD, "malformed production receipt; hold") from exc


def _idempotency(payload):
    lineage = _lineage(payload["binding"], payload["admission_id"])
    if payload["evidence_kind"] == "production_seal":
        parts = (
            lineage,
            payload["expected_head"]["evidence_id"],
            payload["facts"]["coverage_policy_digest"],
            _digest(_json(payload["binding"])),
        )
    else:
        parts = (lineage, payload["producer_kind"], payload["operation_id"], payload["operation_phase"])
    return _digest(_json(parts))


def _insert(conn, payload, head):
    payload = dict(payload, sequence=head.sequence + 1, previous_evidence_id=head.evidence_id)
    raw = _json(payload)
    evidence_id = _digest(raw)
    values = dict(
        _indexed(payload),
        evidence_id=evidence_id,
        idempotency_key=_idempotency(payload),
        payload_sha256=evidence_id,
        payload_json=raw,
    )
    columns = ", ".join(values)
    conn.execute(
        f"INSERT INTO lane_completion_evidence ({columns}) VALUES ({', '.join('?' for _ in values)})",
        tuple(values.values()),
    )
    return _decode(
        conn.execute("SELECT * FROM lane_completion_evidence WHERE evidence_id=?", (evidence_id,)).fetchone()
    )


def _append(request: ProductionAppend, kind: str) -> ProductionRecord:
    code = RefusalCode.INVALID_RECORD
    _require(isinstance(request, ProductionAppend), code, "ProductionAppend required")
    binding = _binding(request.binding, seal=kind == "production_seal")
    _require(
        all(
            _text(v)
            for v in (
                request.admission_id,
                request.operation_id,
                request.producer_kind,
                request.producer_version,
                request.writer_authority_ref,
            )
        ),
        code,
        "receipt identities required",
    )
    head = request.expected_head
    _require(
        isinstance(head, ProductionHead)
        and type(head.sequence) is int
        and head.sequence >= 0
        and ((head.sequence == 0 and head.evidence_id is None) or (head.sequence > 0 and _text(head.evidence_id))),
        code,
        "explicit predecessor sequence/digest required",
    )
    _require(isinstance(request.facts, Mapping), code, "explicit canonical facts required")
    try:
        facts = json.loads(_json(request.facts))
        _require(facts.get("source_digest") is None or _text(facts["source_digest"]), code, "invalid source digest")
        _require(
            "source_cursors" not in facts or isinstance(facts["source_cursors"], dict),
            code,
            "source cursors must be a map",
        )
        _require(
            "coverage_policy_version" not in facts or _text(facts["coverage_policy_version"]),
            code,
            "invalid coverage policy version",
        )
        if kind == "production_seal":
            _require(_text(facts.get("coverage_policy_digest")), code, "seal coverage policy digest required")
        payload = {
            "schema_version": FORMAT_VERSION,
            "evidence_kind": kind,
            "binding": binding,
            "admission_id": request.admission_id,
            "operation_id": request.operation_id,
            "operation_phase": kind.removeprefix("production_"),
            "producer_kind": request.producer_kind,
            "producer_version": request.producer_version,
            "writer_authority_ref": request.writer_authority_ref,
            "expected_head": asdict(head),
            "facts": facts,
        }
        key = _idempotency(payload)
    except (TypeError, ValueError, OverflowError) as exc:
        if isinstance(exc, ProductionRefusal):
            raise
        raise ProductionRefusal(code, "facts must be finite canonical JSON") from exc
    repository = binding["identity"]["repository"]
    with _get_db_connection(begin_immediate=True) as conn:
        # Root equality and replay bytes cannot prove current cycle authority:
        # the native epoch can advance without a new second-precision revision.
        _validate_binding(conn, binding, native_cycle=kind not in ("production_admission", "production_contract_link"))
        existing = conn.execute("SELECT * FROM lane_completion_evidence WHERE idempotency_key=?", (key,)).fetchone()
        if existing is not None:
            record = _decode(existing)
            previous = json.loads(record.payload_json)
            del previous["sequence"], previous["previous_evidence_id"]
            _require(
                _json(previous) == _json(payload),
                RefusalCode.IDEMPOTENCY_CONFLICT,
                "conflicting replay; original retained",
            )
            return record
        admission = conn.execute(
            "SELECT * FROM lane_completion_evidence WHERE repository_key=? AND admission_id=? "
            "AND evidence_kind='production_admission' ORDER BY sequence LIMIT 1",
            (repository, request.admission_id),
        ).fetchone()
        if kind == "production_admission":
            _require(admission is None, RefusalCode.ADMISSION_CONFLICT, "admission already has an immutable root")
        else:
            _require(admission is not None, RefusalCode.ADMISSION_ABSENT, "admission root absent")
            root = json.loads(_decode(admission).payload_json)
            _require(
                _lineage(root["binding"], request.admission_id) == _lineage(binding, request.admission_id),
                RefusalCode.IDENTITY_MISMATCH,
                "foreign admission lineage",
            )
            if kind != "production_contract_link" and not _same_point(root["binding"], binding):
                _resolve_current_binding(conn, _decode(admission), binding)
        if kind in ("production_prepare", "production_settle"):
            same_phase = conn.execute(
                "SELECT 1 FROM lane_completion_evidence WHERE repository_key=? AND admission_id=? "
                "AND operation_id=? AND evidence_kind=? LIMIT 1",
                (repository, request.admission_id, request.operation_id, kind),
            ).fetchone()
            _require(same_phase is None, RefusalCode.OPERATION_CONFLICT, "operation phase already recorded")
        if kind == "production_settle":
            prepared = conn.execute(
                "SELECT 1 FROM lane_completion_evidence WHERE repository_key=? AND admission_id=? "
                "AND operation_id=? AND evidence_kind='production_prepare' LIMIT 1",
                (repository, request.admission_id, request.operation_id),
            ).fetchone()
            _require(prepared is not None, RefusalCode.OPERATION_ABSENT, "settlement requires retained prepare")
        current = _head(conn, repository, request.admission_id)
        _require(current == head, RefusalCode.HEAD_CONFLICT, "expected history head changed")
        if kind == "production_seal":
            _require(
                _open_operations(conn, repository, request.admission_id, current.sequence) == 0,
                RefusalCode.OPEN_OPERATIONS,
                "open operations cannot be sealed",
            )
        if kind == "production_prepare":
            seal = conn.execute(
                "SELECT s.* FROM lane_completion_evidence s WHERE s.repository_key=? AND s.admission_id=? "
                "AND s.evidence_kind='production_seal' AND NOT EXISTS ("
                "SELECT 1 FROM lane_completion_evidence g WHERE g.repository_key=s.repository_key "
                "AND g.admission_id=s.admission_id AND g.operation_phase='seal_invalidation' "
                "AND json_extract(g.payload_json, '$.facts.invalidated_evidence_id')=s.evidence_id) "
                "ORDER BY s.sequence DESC LIMIT 1",
                (repository, request.admission_id),
            ).fetchone()
            if seal is not None:
                _decode(seal)
                invalidation = dict(
                    payload,
                    evidence_kind="production_gap",
                    operation_phase="seal_invalidation",
                    operation_id=seal["evidence_id"],
                    facts={"invalidated_evidence_id": seal["evidence_id"]},
                )
                current = _insert(conn, invalidation, current).head
        return _insert(conn, payload, current)


def record_production_admission(request: ProductionAppend) -> ProductionRecord:
    return _append(request, "production_admission")


def record_production_prepare(request: ProductionAppend) -> ProductionRecord:
    return _append(request, "production_prepare")


def record_production_settle(request: ProductionAppend) -> ProductionRecord:
    return _append(request, "production_settle")


def record_production_seal(request: ProductionAppend) -> ProductionRecord:
    return _append(request, "production_seal")


def record_production_contract_link(request: ProductionAppend) -> ProductionRecord:
    return _append(request, "production_contract_link")


def record_production_gap(request: ProductionAppend) -> ProductionRecord:
    return _append(request, "production_gap")


def record_production_backfill(request: ProductionAppend) -> ProductionRecord:
    raise ProductionRefusal(RefusalCode.UNSUPPORTED_KIND, "backfill writer belongs to implementation note.12")


def _path_count(value):
    if isinstance(value, dict):
        count = 0
        for key, item in value.items():
            if key == "path" and isinstance(item, str):
                count += 1
            elif key in ("paths", "manifest_paths") and isinstance(item, list):
                count += len(item)
            elif key == "manifest" and isinstance(item, dict):
                count += len(item)
            else:
                count += _path_count(item)
        return count
    if isinstance(value, list):
        return sum(_path_count(item) for item in value)
    return 0


_HISTORY_WHERE = (
    "repository_key=? AND task_ref=? AND lane_row_id=? AND identity_seq IS ? AND admission_id=? "
    "AND evidence_kind LIKE 'production_%'"
)


def get_lane_production_history(
    binding: ProductionBinding,
    admission_id: str,
    limit: int = MAX_PAGE_ROWS,
    *,
    read_limit: int | None = None,
    cursor: ProductionCursor | None = None,
    historical: bool = False,
) -> dict:
    """One ascending page, at most two per captured-head collection.

    Feed data.next_cursor into the second call. A terminal cap returns no next
    cursor and explicit has_more/truncation; it is never a complete-history
    assertion. Limits/bytes/paths are cumulative across continuation. Callers
    must repeat the bounded collection and check coherence before assessment.

    historical=True authenticates the root binding and each returned receipt's
    immutable contract and predecessor chain without consulting current rows.
    A truncated collection only verifies its returned prefix, never the unseen
    suffix. This mode grants no write authority and does not relax appends.
    """
    binding = _binding(binding)
    _require(type(historical) is bool, RefusalCode.INVALID_RECORD, "historical must be a boolean")
    _require(
        _text(admission_id) and type(limit) is int and limit > 0,
        RefusalCode.INVALID_RECORD,
        "positive page limit/admission required",
    )
    _require(
        read_limit is None or (type(read_limit) is int and read_limit > 0),
        RefusalCode.INVALID_RECORD,
        "positive read_limit required",
    )
    lineage = _lineage(binding, admission_id)
    lineage_digest = _digest(_json(lineage))
    page, after, spent_bytes, spent_paths = 1, 0, 0, 0
    remaining = min(read_limit, MAX_PAGE_ROWS * MAX_PAGES) if read_limit is not None else MAX_PAGE_ROWS * MAX_PAGES
    captured = None
    if cursor is not None:
        _require(
            isinstance(cursor, dict)
            and set(cursor) == set(ProductionCursor.__annotations__)
            and cursor["lineage_digest"] == lineage_digest,
            RefusalCode.INVALID_CURSOR,
            "foreign/malformed continuation",
        )
        _require(
            all(
                type(cursor[k]) is int
                for k in ("page", "after_sequence", "remaining", "decoded_bytes", "manifest_paths")
            )
            and cursor["page"] == 2
            and 0 < cursor["remaining"] <= MAX_PAGE_ROWS * MAX_PAGES
            and 0 <= cursor["decoded_bytes"] <= MAX_DECODED_BYTES
            and 0 <= cursor["manifest_paths"] <= MAX_MANIFEST_PATHS
            and isinstance(cursor["head"], dict)
            and set(cursor["head"]) == {"sequence", "evidence_id"}
            and type(cursor["head"]["sequence"]) is int
            and 0 < cursor["after_sequence"] <= cursor["head"]["sequence"]
            and _text(cursor["head"]["evidence_id"]),
            RefusalCode.INVALID_CURSOR,
            "invalid collection bounds",
        )
        page, after = cursor["page"], cursor["after_sequence"]
        spent_bytes, spent_paths = cursor["decoded_bytes"], cursor["manifest_paths"]
        remaining = min(remaining, cursor["remaining"])
        captured = ProductionHead(**cursor["head"])
    rows, reasons = [], []
    decoded_bytes, manifest_paths = 0, 0
    coverage_policy_version, source_cursors = None, {}
    with _get_db_connection() as conn:
        conn.execute("BEGIN")
        if not historical:
            _validate_binding(conn, binding)
        current = _head(conn, lineage[0], admission_id)
        captured = captured or current
        previous = None
        if historical:
            root = conn.execute(
                "SELECT evidence_id FROM lane_completion_evidence WHERE repository_key=? AND admission_id=? "
                "AND evidence_kind='production_admission' ORDER BY sequence LIMIT 1",
                (lineage[0], admission_id),
            ).fetchone()
            if root is None:
                foreign = conn.execute(
                    "SELECT 1 FROM lane_completion_evidence WHERE admission_id=? LIMIT 1", (admission_id,)
                ).fetchone()
                _require(foreign is None, RefusalCode.IDENTITY_MISMATCH, "foreign admission repository")
                raise ProductionRefusal(RefusalCode.ADMISSION_ABSENT, "retained admission absent")
            root_row = _bounded_receipt(conn, root["evidence_id"], MAX_DECODED_BYTES - spent_bytes)
            if root_row["payload_json"] is None:
                reasons.append("byte_cap")
            else:
                retained = _match_root(_decode(root_row), binding)
                _validate_retained_binding(conn, retained)
                # Page one decodes the root in the normal page budget. On a
                # continuation, charge the revalidated root and boundary row.
                if after:
                    decoded_bytes += root_row["payload_bytes"]
                    boundary = conn.execute(
                        "SELECT evidence_id FROM lane_completion_evidence WHERE " + _HISTORY_WHERE + " AND sequence=?",
                        (*lineage, after),
                    ).fetchone()
                    _require(boundary is not None, RefusalCode.CORRUPT_RECORD, "missing continuation predecessor")
                    boundary = _bounded_receipt(conn, boundary[0], MAX_DECODED_BYTES - spent_bytes - decoded_bytes)
                    if boundary["payload_json"] is None:
                        reasons.append("byte_cap")
                    else:
                        previous = _decode(boundary).evidence_id
                        _validate_retained_binding(conn, _binding(json.loads(boundary["payload_json"])["binding"]))
                        decoded_bytes += boundary["payload_bytes"]
        if captured.sequence:
            root = conn.execute(
                "SELECT evidence_id, evidence_kind FROM lane_completion_evidence WHERE "
                + _HISTORY_WHERE
                + " AND sequence=1",
                lineage,
            ).fetchone()
            _require(
                root is not None and root["evidence_kind"] == "production_admission",
                RefusalCode.IDENTITY_MISMATCH,
                "admission lineage mismatch",
            )
            match = conn.execute(
                "SELECT evidence_id FROM lane_completion_evidence WHERE " + _HISTORY_WHERE + " AND sequence=?",
                (*lineage, captured.sequence),
            ).fetchone()
            _require(
                match is not None and match[0] == captured.evidence_id,
                RefusalCode.INVALID_CURSOR,
                "captured head replaced",
            )
        total = conn.execute(
            "SELECT COUNT(*) FROM lane_completion_evidence WHERE " + _HISTORY_WHERE + " AND sequence<=?",
            (*lineage, captured.sequence),
        ).fetchone()[0]
        open_count = _open_operations(conn, lineage[0], admission_id, captured.sequence)
        page_limit = min(limit, MAX_PAGE_ROWS, remaining)
        columns = _receipt_columns()
        query = conn.execute(
            "SELECT "
            + columns
            + ", CASE WHEN length(CAST(payload_json AS BLOB))<=? THEN payload_json END AS payload_json, "
            "length(CAST(payload_json AS BLOB)) AS payload_bytes FROM lane_completion_evidence WHERE "
            + _HISTORY_WHERE
            + " AND sequence>? AND sequence<=? ORDER BY sequence ASC LIMIT ?",
            (MAX_DECODED_BYTES - spent_bytes, *lineage, after, captured.sequence, page_limit),
        )
        for row in query:
            if reasons:
                break
            if row["payload_bytes"] > MAX_DECODED_BYTES - spent_bytes - decoded_bytes:
                reasons.append("byte_cap")
                break
            record = _decode(row)
            if historical:
                _verify_predecessor(record, after + len(rows) + 1, previous)
                receipt_binding = _binding(json.loads(record.payload_json)["binding"])
                _require(
                    _lineage(receipt_binding, admission_id) == lineage
                    and receipt_binding["identity"]["lane_id"] == binding["identity"]["lane_id"],
                    RefusalCode.IDENTITY_MISMATCH,
                    "foreign receipt lineage",
                )
                _validate_retained_binding(conn, receipt_binding)
            item = {
                "evidence_id": record.evidence_id,
                "sequence": record.sequence,
                "previous_evidence_id": record.previous_evidence_id,
                "evidence_kind": record.evidence_kind,
                "operation_id": row["operation_id"],
                "operation_phase": row["operation_phase"],
                "payload": json.loads(record.payload_json),
            }
            size = len(json.dumps(item).encode())
            paths = _path_count(item["payload"]["facts"])
            if size + spent_bytes + decoded_bytes > MAX_DECODED_BYTES:
                reasons.append("byte_cap")
                break
            if paths + spent_paths + manifest_paths > MAX_MANIFEST_PATHS:
                reasons.append("path_cap")
                break
            rows.append(item)
            previous = record.evidence_id
            decoded_bytes += size
            manifest_paths += paths
        # Bounded projections of the latest policy/source facts at this head,
        # including admission facts that may precede this page's keyset cursor.
        for field in ("coverage_policy_version", "source_cursors"):
            meta = conn.execute(
                "SELECT json_extract(payload_json, ?) AS value FROM lane_completion_evidence WHERE "
                + _HISTORY_WHERE
                + " AND sequence<=? AND json_type(payload_json, ?) IS NOT NULL ORDER BY sequence DESC LIMIT 1",
                (f"$.facts.{field}", *lineage, captured.sequence, f"$.facts.{field}"),
            ).fetchone()
            if meta is not None:
                value = meta[0]
                size = len(str(value).encode())
                if size + spent_bytes + decoded_bytes > MAX_DECODED_BYTES:
                    if "byte_cap" not in reasons:
                        reasons.append("byte_cap")
                else:
                    decoded_bytes += size
                    if field == "coverage_policy_version":
                        coverage_policy_version = value
                    else:
                        source_cursors = json.loads(value) if isinstance(value, str) else value
        last_sequence = rows[-1]["sequence"] if rows else after
        more_rows = bool(
            conn.execute(
                "SELECT 1 FROM lane_completion_evidence WHERE "
                + _HISTORY_WHERE
                + " AND sequence>? AND sequence<=? LIMIT 1",
                (*lineage, last_sequence, captured.sequence),
            ).fetchone()
        )
        if historical and not more_rows and not reasons:
            _require(
                total == captured.sequence and last_sequence == captured.sequence and previous == captured.evidence_id,
                RefusalCode.CORRUPT_RECORD,
                "incomplete production chain at retained head",
            )
        if more_rows and not reasons:
            if page == MAX_PAGES:
                reasons.append("page_cap")
            elif remaining == len(rows):
                reasons.append("read_limit")
            else:
                reasons.append("page_limit")
        if current != captured:
            reasons.append("head_changed")
    next_cursor = None
    if more_rows and reasons == ["page_limit"]:
        next_cursor = {
            "lineage_digest": lineage_digest,
            "head": asdict(captured),
            "after_sequence": last_sequence,
            "page": page + 1,
            "remaining": remaining - len(rows),
            "decoded_bytes": spent_bytes + decoded_bytes,
            "manifest_paths": spent_paths + manifest_paths,
        }
    data = {
        "task_ref": binding["identity"]["task_ref"],
        "history": rows,
        "returned": len(rows),
        "total_matching": total,
        "has_more": more_rows or bool(reasons),
        "head": asdict(captured),
        "coverage_policy_version": coverage_policy_version,
        "source_cursors": source_cursors,
        "open_operation_count": open_count,
        "truncated": bool(reasons),
        "truncation": {"truncated": bool(reasons), "reasons": reasons},
        "next_cursor": next_cursor,
        "decoded_bytes": decoded_bytes,
        "manifest_paths": manifest_paths,
    }
    return _envelope(ok=True, tool="get_lane_production_history", data=data, task_ref=binding["identity"]["task_ref"])
