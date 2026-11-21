"""Private, append-only evidence for one incarnation of a lane closure."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from .enums import TERMINAL_LANE_STATUSES

Kind = Literal["legacy_registration", "measurement", "historical_backfill"]
TABLE = "worktree_lane_close_evidence"
OPENINGS = "worktree_lane_close_openings"
_TERMINAL_SQL = ", ".join(f"'{status}'" for status in sorted(TERMINAL_LANE_STATUSES))
_CUTOFF = "lane_close_evidence_cutoff"
_OPENING_COVERAGE = "lane_close_opening_coverage_epoch"
_KINDS = {"legacy_registration", "measurement", "historical_backfill"}

_DDL = {
    OPENINGS: f"""CREATE TABLE {OPENINGS} (
    lane_row_id INTEGER PRIMARY KEY,
    first_epoch INTEGER CHECK (first_epoch > 0)
)""",
    # The native status writer allocates its next epoch in the same SQLite
    # statement. Mark the reopening before that allocation and bind its actual
    # value at the counter. A pending opening cannot authorize any pass.
    "lane_close_opening_reopen": f"""CREATE TRIGGER lane_close_opening_reopen
BEFORE UPDATE OF status ON worktree_lanes
WHEN OLD.status IN ({_TERMINAL_SQL}) AND NEW.status != OLD.status
BEGIN
    INSERT INTO {OPENINGS} VALUES (OLD.id, NULL)
    ON CONFLICT(lane_row_id) DO UPDATE SET first_epoch=NULL;
END""",
    "lane_close_opening_allocate": f"""CREATE TRIGGER lane_close_opening_allocate
AFTER UPDATE OF value ON worktree_lanes_cycle_epoch_counter
WHEN NEW.value > OLD.value
BEGIN
    UPDATE {OPENINGS} SET first_epoch=NEW.value WHERE first_epoch IS NULL;
END""",
    TABLE: f"""CREATE TABLE {TABLE} (
    record_id TEXT NOT NULL PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    kind TEXT NOT NULL CHECK (kind IN ('legacy_registration', 'measurement', 'historical_backfill')),
    task_ref TEXT NOT NULL,
    lane_id TEXT NOT NULL,
    lane_row_id INTEGER NOT NULL CHECK (lane_row_id > 0),
    identity_seq INTEGER NOT NULL CHECK (identity_seq > 0),
    closed_epoch INTEGER NOT NULL CHECK (closed_epoch > 0),
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
)""",
    "lane_close_evidence_closure": f"""CREATE UNIQUE INDEX lane_close_evidence_closure
ON {TABLE}(lane_row_id, identity_seq, closed_epoch, (kind = 'legacy_registration'))""",
    "lane_close_evidence_no_update": f"""CREATE TRIGGER lane_close_evidence_no_update
BEFORE UPDATE ON {TABLE}
BEGIN SELECT RAISE(ABORT, 'lane_close_evidence_immutable'); END""",
    "lane_close_evidence_no_delete": f"""CREATE TRIGGER lane_close_evidence_no_delete
BEFORE DELETE ON {TABLE}
BEGIN SELECT RAISE(ABORT, 'lane_close_evidence_immutable'); END""",
    # REPLACE can bypass DELETE triggers when recursive_triggers is disabled.
    "lane_close_evidence_no_replace": f"""CREATE TRIGGER lane_close_evidence_no_replace
BEFORE INSERT ON {TABLE}
WHEN EXISTS (SELECT 1 FROM {TABLE} WHERE record_id = NEW.record_id OR (
    lane_row_id = NEW.lane_row_id AND identity_seq = NEW.identity_seq AND closed_epoch = NEW.closed_epoch
    AND (kind = 'legacy_registration') = (NEW.kind = 'legacy_registration')
))
BEGIN SELECT RAISE(ABORT, 'lane_close_evidence_conflict'); END""",
}


class EvidenceStoreError(ValueError):
    """The local store or requested closure cannot be authenticated."""


def _objects(conn: sqlite3.Connection) -> dict:
    names = ",".join("?" for _ in _DDL)
    return {
        row[0]: row[1]
        for row in conn.execute(f"SELECT name, sql FROM sqlite_master WHERE name IN ({names})", tuple(_DDL))
    }


def opening_coverage(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT value FROM schema_meta WHERE key=?", (_OPENING_COVERAGE,)).fetchone()
    return row[0] if row is not None and type(row[0]) is int and row[0] >= 0 else None


def schema_present(conn: sqlite3.Connection) -> bool:
    return _objects(conn) == _DDL and _cutoff(conn) is not None and opening_coverage(conn) is not None


def _cutoff(conn: sqlite3.Connection) -> int | None:
    table = conn.execute("SELECT 1 FROM sqlite_master WHERE name='schema_meta' AND type='table'").fetchone()
    if table is None:
        return None
    row = conn.execute("SELECT value FROM schema_meta WHERE key = ?", (_CUTOFF,)).fetchone()
    return row[0] if row is not None and type(row[0]) is int else None


def _cutoff_path(conn: sqlite3.Connection) -> Path | None:
    name = conn.execute("PRAGMA database_list").fetchone()[2]
    if not name:
        return None
    path = Path(name).resolve()
    return path.with_name(path.name + ".lane-close-cutoff")


def _read_cutoff(path: Path) -> int:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as marker:
        info = os.fstat(marker.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise EvidenceStoreError("close_evidence_cutoff_custody")
        raw = marker.read(64)
    if re.fullmatch(rb"[1-9][0-9]{0,18}\n", raw) is None:
        raise EvidenceStoreError("close_evidence_cutoff_invalid")
    return int(raw)


def _claim_cutoff(conn: sqlite3.Connection) -> tuple[int, bool]:
    """Latch per DB location before enrollment, including across file restores.

    A crash after creating the latch can lose eligibility. Reopening the cohort
    on an ambiguous restart could instead grant eligibility to a later close.
    """
    latch = _cutoff_path(conn)
    if latch is None:
        return 0, False
    nonce = secrets.randbelow(2**63 - 1) + 1
    try:
        fd = os.open(latch, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return _read_cutoff(latch), False
    with os.fdopen(fd, "wb") as marker:
        marker.write(f"{nonce}\n".encode())
        marker.flush()
        os.fsync(marker.fileno())
    directory = os.open(latch.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return nonce, True


@dataclass(frozen=True)
class ClosureIdentity:
    task_ref: str
    lane_id: str
    row_id: int
    identity_seq: int
    closed_epoch: int
    branch: str
    worktree_path: str
    status: str

    def __post_init__(self) -> None:
        if not all(type(x) is int and x > 0 for x in self.key):
            raise EvidenceStoreError("closure_generation_invalid")
        if not all(
            isinstance(x, str) and x.strip() for x in (self.task_ref, self.lane_id, self.branch, self.worktree_path)
        ):
            raise EvidenceStoreError("closure_identity_invalid")
        if self.status not in TERMINAL_LANE_STATUSES:
            raise EvidenceStoreError("issuer_not_closed")

    @property
    def key(self) -> tuple[int, int, int]:
        return self.row_id, self.identity_seq, self.closed_epoch

    @classmethod
    def from_lane(cls, row) -> ClosureIdentity:
        return cls(
            row["task_ref"],
            row["lane_id"],
            row["id"],
            row["identity_seq"],
            row["cycle_epoch"],
            row["branch"],
            row["worktree_path"],
            row["status"],
        )

    def fields(self) -> dict:
        return dict(
            task_ref=self.task_ref,
            lane_id=self.lane_id,
            row_id=self.row_id,
            identity_seq=self.identity_seq,
            closed_epoch=self.closed_epoch,
            branch=self.branch,
            worktree_path=self.worktree_path,
            status=self.status,
        )


def _encoded(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _oid(value: object, size: int = 40) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % size, value) is not None


@dataclass(frozen=True)
class CloseEvidence:
    kind: Kind
    closure: ClosureIdentity
    created_at: str
    facts: dict

    @property
    def record_id(self) -> str:
        return _sha(
            "workbay/lane-close-evidence@1\0"
            + _encoded(
                [
                    self.kind,
                    self.closure.fields(),
                    self.facts.get("repo"),
                    self.facts.get("receipt_ids"),
                ]
            )
        )

    def payload(self) -> dict:
        data = {
            **self.facts,
            **self.closure.fields(),
            "kind": self.kind,
            "schema_version": 1,
            "created_at": self.created_at,
        }
        if self.kind == "historical_backfill":
            data["backfill_id"] = self.record_id
        return data

    def _validate(self) -> None:
        if self.kind not in _KINDS or datetime.fromisoformat(self.created_at).utcoffset() is None:
            raise EvidenceStoreError("close_evidence_header_invalid")
        if self.kind == "legacy_registration":
            if self.facts:
                raise EvidenceStoreError("legacy_registration_contains_evidence")
            return
        if (
            not isinstance(self.facts.get("repo"), str)
            or not Path(self.facts["repo"]).is_absolute()
            or self.closure.status == "merged"
        ):
            raise EvidenceStoreError("close_evidence_identity_invalid")
        facts = self.facts
        reasons = facts.get("reasons")
        disposition = facts.get("disposition")
        if (
            disposition
            not in {"unknown", "contribution_observed", "issuer_work_ref_present", "closed_without_contribution"}
            or not isinstance(reasons, list)
            or not all(isinstance(item, str) for item in reasons)
            or (disposition == "unknown" and not reasons)
        ):
            raise EvidenceStoreError("close_evidence_disposition_invalid")
        for field in ("base_sha", "terminal_tip_sha", "terminal_tree", "integration_head", "landing_commit_sha"):
            if facts.get(field) is not None and not _oid(facts[field]):
                raise EvidenceStoreError("close_evidence_oid_invalid")
        for field in ("clean", "commit_landed"):
            if facts.get(field) is not None and type(facts[field]) is not bool:
                raise EvidenceStoreError("close_evidence_fact_invalid")
        count = facts.get("changed_file_count")
        if count is not None and (type(count) is not int or count < 0):
            raise EvidenceStoreError("close_evidence_fact_invalid")
        for field, valid_item in (
            ("receipt_ids", lambda item: _oid(item, 64)),
            ("checkpoint_commits", _oid),
            ("dispatch_refs_present", lambda item: isinstance(item, str) and item.startswith("refs/")),
        ):
            items = facts.get(field)
            if items is not None and (
                not isinstance(items, list) or not all(valid_item(x) for x in items) or sorted(set(items)) != items
            ):
                raise EvidenceStoreError("close_evidence_inventory_invalid")
        combined = facts.get("witness_kind") == "combined_no_contribution_v1"
        if disposition == "closed_without_contribution" and (
            not _oid(facts.get("base_sha"))
            or facts.get("commit_landed") is not False
            or "landing_commit_sha" not in facts
            or facts["landing_commit_sha"] is not None
            or facts.get("checkpoint_commits") != []
            or facts.get("dispatch_refs_present") != []
            or not facts.get("receipt_ids")
            or facts.get("tip_on_integration") is not True
            or (combined and not _combined_sources_valid(facts))
            or (not combined and (
                facts.get("terminal_tip_sha") != facts["base_sha"] or count != 0 or facts.get("clean") is not True
            ))
        ):
            raise EvidenceStoreError("close_evidence_empty_conjunction_invalid")
        if self.kind == "historical_backfill" and (
            disposition != "closed_without_contribution"
            or type(facts.get("operator_uid")) is not int
            or facts["operator_uid"] < 0
            or not isinstance(facts.get("operator_login"), str)
            or not facts["operator_login"]
            or not _historical_sources_valid(facts, self.closure)
        ):
            raise EvidenceStoreError("close_evidence_backfill_invalid")
        if set(facts) & (self.closure.fields().keys() | {"schema_version", "kind", "created_at", "backfill_id"}):
            raise EvidenceStoreError("close_evidence_projection_conflict")


def _combined_sources_valid(facts: dict) -> bool:
    passes = facts.get("passes")
    manifest = facts.get("source_manifest")
    return (
        facts.get("no_contribution") is True
        and _oid(facts.get("base_tree"))
        and all(field in facts and facts[field] is None for field in (
            "terminal_tip_sha", "terminal_tree", "changed_file_count", "clean",
        ))
        and isinstance(passes, dict) and bool(passes)
        and isinstance(facts.get("pass_sha256s"), dict) and passes.keys() == facts["pass_sha256s"].keys()
        and all(
            isinstance(item, dict) and (
                item.get("model_started") is False and item.get("activity_source") in {"abandoned", "admission_deferred"}
                or item.get("model_started") is True
                and type(item.get("patch_bytes")) is int and item["patch_bytes"] == 0
                and item.get("sandbox_base_tree") == facts["base_tree"]
                and item.get("base_sha") == item.get("tip_sha") == facts["base_sha"]
                and item.get("checkpoint_commits") == [] and item.get("commit_landed") is False
            )
            for item in passes.values()
        )
        and isinstance(manifest, list) and bool(manifest)
        and all(
            isinstance(source, dict) and isinstance(source.get("locator"), str)
            and Path(source["locator"]).is_absolute() and isinstance(source.get("schema"), str)
            and type(source.get("bytes")) is int and source["bytes"] >= 0 and _oid(source.get("sha256"), 64)
            for source in manifest
        )
        and facts.get("file_sha256s") == {source["locator"]: source["sha256"] for source in manifest}
        and isinstance(facts.get("sqlite_source"), dict)
        and _oid(facts["sqlite_source"].get("projection_sha256"), 64)
    )


def _historical_sources_valid(facts: dict, closure: ClosureIdentity) -> bool:
    identifiers = facts.get("input")
    terminal = facts.get("terminal_sources")
    receipts = facts.get("receipt_sha256s")
    passes = facts.get("pass_sha256s")
    derived_tip = facts["base_sha"] if facts.get("witness_kind") == "combined_no_contribution_v1" else facts["terminal_tip_sha"]
    return (
        isinstance(identifiers, dict)
        and set(identifiers) <= {"task_ref", "lane_id", "receipt_ids", "expected_tip"}
        and identifiers.get("task_ref") == closure.task_ref
        and identifiers.get("lane_id") == closure.lane_id
        and identifiers.get("receipt_ids") == facts["receipt_ids"]
        and ("expected_tip" not in identifiers or identifiers["expected_tip"] == derived_tip)
        and isinstance(receipts, dict)
        and set(receipts) == set(facts["receipt_ids"])
        and isinstance(passes, dict)
        and bool(passes)
        and all(_oid(value, 64) for value in (*receipts.values(), *passes.values()))
        and isinstance(terminal, list)
        and bool(terminal)
        and all(
            isinstance(source, dict)
            and set(source) == {"locator", "sha256"}
            and isinstance(source["locator"], str)
            and Path(source["locator"]).is_absolute()
            and _oid(source["sha256"], 64)
            for source in terminal
        )
    )


def _insert(conn: sqlite3.Connection, record: CloseEvidence) -> None:
    record._validate()
    raw = _encoded(record.payload())
    closure = record.closure
    conn.execute(
        f"""INSERT INTO {TABLE}(record_id, schema_version, kind, task_ref, lane_id, lane_row_id,
        identity_seq, closed_epoch, created_at, payload_json, payload_sha256)
        VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            record.record_id,
            record.kind,
            closure.task_ref,
            closure.lane_id,
            *closure.key,
            record.created_at,
            raw,
            _sha(raw),
        ),
    )


def read_close_evidence(conn: sqlite3.Connection, closure: ClosureIdentity) -> dict[Kind, CloseEvidence]:
    """Read one exact closure, checking the schema, bytes and projected identity."""
    if not schema_present(conn):
        raise EvidenceStoreError("close_evidence_store_unavailable")
    path = _cutoff_path(conn)
    try:
        if path is not None and _read_cutoff(path) != _cutoff(conn):
            raise EvidenceStoreError("close_evidence_cutoff_identity")
    except OSError as exc:
        raise EvidenceStoreError("close_evidence_cutoff_unavailable") from exc
    result = {}
    for row in conn.execute(
        f"""SELECT record_id, schema_version, kind, task_ref, lane_id, created_at, payload_json, payload_sha256
        FROM {TABLE} WHERE lane_row_id = ? AND identity_seq = ? AND closed_epoch = ?""",
        closure.key,
    ):
        record_id, version, kind, task, lane, stamp, raw, digest = row
        try:
            payload = json.loads(raw)
            identity = closure.fields()
            if (
                type(version) is not int
                or version != 1
                or task != closure.task_ref
                or lane != closure.lane_id
                or not isinstance(payload, dict)
                or digest != _sha(raw)
            ):
                raise EvidenceStoreError("close_evidence_integrity")
            facts = {
                key: value
                for key, value in payload.items()
                if key not in identity and key not in {"schema_version", "kind", "created_at", "backfill_id"}
            }
            record = CloseEvidence(kind, closure, stamp, facts)
            record._validate()
            if record.record_id != record_id or _encoded(record.payload()) != raw:
                raise EvidenceStoreError("close_evidence_integrity")
        except (TypeError, ValueError) as exc:
            raise EvidenceStoreError("close_evidence_integrity") from exc
        result[kind] = record
    return result


def append_close_evidence(conn: sqlite3.Connection, record: CloseEvidence) -> CloseEvidence:
    """Append within the caller's guarded write transaction; equal retries converge."""
    if not conn.in_transaction or record.kind == "legacy_registration":
        raise EvidenceStoreError("close_evidence_writer_invalid")
    previous = read_close_evidence(conn, record.closure)
    old = previous.get(record.kind)
    if old is not None and old.payload() == record.payload():
        return old
    if "measurement" in previous or "historical_backfill" in previous:
        raise EvidenceStoreError("close_evidence_conflict")
    if record.kind == "historical_backfill" and "legacy_registration" not in previous:
        raise EvidenceStoreError("issuer_not_enrolled")
    _insert(conn, record)
    return record


def is_post_upgrade_closure(conn: sqlite3.Connection, closure: ClosureIdentity) -> bool:
    """Use the first local cutover epoch even when legacy eligibility was lost."""
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key='lane_close_cutoff_epoch'"
    ).fetchone()
    return row is not None and type(row[0]) is int and 0 <= row[0] < closure.closed_epoch


def migrate(conn: sqlite3.Connection, *, eligible_upgrade: bool) -> None:
    """Install/repair objects and seal the initial cohort within bootstrap."""
    installed = _objects(conn)
    sealed = _cutoff(conn) is not None
    nonce, first_at_location = _claim_cutoff(conn)
    # The preceding opening table forbade the pending (NULL) marker. Its
    # inferred floors cannot be retained across the change of allocator.
    conn.execute("DROP TRIGGER IF EXISTS lane_close_opening_register")
    previous_openings = _DDL[OPENINGS].replace("first_epoch INTEGER CHECK", "first_epoch INTEGER NOT NULL CHECK")
    if installed.get(OPENINGS) == previous_openings:
        conn.execute(f"DROP TABLE {OPENINGS}")
        conn.execute(_DDL[OPENINGS])
    for name, statement in _DDL.items():
        if name not in installed:
            conn.execute(statement)
        elif installed[name] != statement and name not in {TABLE, OPENINGS}:
            object_type = "INDEX" if name == "lane_close_evidence_closure" else "TRIGGER"
            conn.execute(f"DROP {object_type} {name}")
            conn.execute(statement)
    # Start coverage at a known live epoch. A repaired trigger cannot attest
    # to transitions that may have occurred while coverage was missing.
    if opening_coverage(conn) is None or any(installed.get(name) != _DDL[name] for name in (
        OPENINGS, "lane_close_opening_allocate", "lane_close_opening_reopen",
    )):
        conn.execute(
            "INSERT INTO schema_meta(key,value) SELECT ?,value FROM worktree_lanes_cycle_epoch_counter "
            "WHERE id=1 ON CONFLICT(key) DO UPDATE SET value=excluded.value", (_OPENING_COVERAGE,),
        )
        conn.execute(f"DELETE FROM {OPENINGS}")
        conn.execute(f"""INSERT INTO {OPENINGS} SELECT id, cycle_epoch FROM worktree_lanes
            WHERE status NOT IN ({_TERMINAL_SQL}) AND cycle_epoch > 0""")
    if eligible_upgrade and first_at_location and not sealed and not installed:
        stamp = datetime.now(timezone.utc).isoformat()
        for identity in _legacy_identities(conn):
            _insert(conn, CloseEvidence("legacy_registration", identity, stamp, {}))
    if first_at_location and not sealed:
        conn.execute(
            "INSERT INTO schema_meta(key, value) SELECT 'lane_close_cutoff_epoch', value "
            "FROM worktree_lanes_cycle_epoch_counter WHERE id=1 ON CONFLICT(key) DO NOTHING"
        )
    conn.execute("INSERT INTO schema_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING", (_CUTOFF, nonce))


def _legacy_identities(conn: sqlite3.Connection):
    # Import-created rows omit native execution metadata. A live identity
    # still needs original producer evidence for any subsequent backfill.
    rows = conn.execute(
        """SELECT * FROM worktree_lanes
        WHERE status IN (SELECT value FROM json_each(?)) AND backend IS NOT NULL
        AND identity_seq > 0 AND cycle_epoch > 0""",
        (json.dumps(sorted(TERMINAL_LANE_STATUSES)),),
    )
    enrolled = set()
    for row in rows:
        try:
            identity = ClosureIdentity.from_lane(row)
        except EvidenceStoreError:
            continue
        enrolled.add(identity.key)
        yield identity

    if (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lane_resource_producer_evidence'"
        ).fetchone()
        is None
    ):
        return
    from .import_export import registration_history

    try:
        history = registration_history(conn)
    except (TypeError, ValueError):
        return  # Ambiguous local archive identity conveys no eligibility.
    registrations = {row["id"]: row for row in history}
    for raw, digest in conn.execute("SELECT payload_json, digest FROM lane_resource_producer_evidence"):
        try:
            producer = json.loads(raw)
            if producer["operation"] != "close_lane" or digest != "sha256:" + _sha(raw):
                continue
            identity = ClosureIdentity.from_lane(producer["lane"])
            if identity != ClosureIdentity.from_lane(registrations[identity.row_id]) or identity.key in enrolled:
                continue
        except (KeyError, TypeError, ValueError):
            continue
        enrolled.add(identity.key)
        yield identity
