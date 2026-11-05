"""External SQLite retention authority; R3 events and state share one commit.

Canon v0.26.2 DATA-14, No dual writes (engineering.md:232, Designing
Data-Intensive Applications ch. 11); DOM-06, Outbox or it didn't happen
(engineering.md:684, Learning Domain-Driven Design ch. 9). No second history.
The clock on an R3 event is producer supplied, not an observed commit clock.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from workbay_protocol.resource_lifecycle import (
    Allow,
    ApplyRequest,
    EvidenceRef,
    Held,
    Hold,
    LifecycleEvent,
    Owner,
    Proof,
    Registration,
    RetentionPlan,
    canonical_digest,
    event_identity,
)

from . import resource_events as events
from .resource_retention_cli import EffectReceipt, OperationRecord, RecordPage, RecordSummary


def private_path(path: Path, *, directory: bool = True) -> Path:
    """Reject symlinked/foreign/writable authority paths without changing modes."""
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("authority_path_invalid")
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError("authority_symlink")
    if path.exists():
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & (0o077 if directory else 0o022):
            raise ValueError("authority_owner_or_mode")
        if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
            raise ValueError("authority_type")
    return path


def _json(value):
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


class SQLiteRetentionLedger:
    """Durable registration/plan CAS, intents, evidence and immutable R3 history.

    root is provisioned by the host, outside every registered target. The public
    wire API never accepts a database path. Connections are short lived; restart
    and target disappearance do not affect this authority. SQLite FULL commits
    precede every return from a mutator. No native database is rewritten.
    """

    def __init__(self, root: Path, *, clock=None):
        self.storage_root = private_path(root)
        self.storage_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        private_path(self.storage_root)
        self.path = private_path(self.storage_root / "resources.sqlite", directory=False)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        with self.transaction() as conn:
            events.initialize_resource_events(conn)
            for sql in (
                "CREATE TABLE IF NOT EXISTS retention_authority (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS retention_registrations (key TEXT PRIMARY KEY, packet TEXT NOT NULL)",
                """CREATE TABLE IF NOT EXISTS retention_plans (
                    owner TEXT NOT NULL, plan_id TEXT NOT NULL, operation_key TEXT NOT NULL UNIQUE,
                    packet TEXT NOT NULL, PRIMARY KEY(owner,plan_id))""",
                "CREATE TABLE IF NOT EXISTS retention_operations (key TEXT PRIMARY KEY, packet TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS retention_artifacts (key TEXT PRIMARY KEY, plan_digest TEXT NOT NULL, receipt TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS retention_evidence (ref TEXT PRIMARY KEY, digest TEXT NOT NULL, payload BLOB NOT NULL)",
            ):
                conn.execute(sql)

    @contextmanager
    def transaction(self):
        private_path(self.storage_root)
        private_path(self.path, directory=False)
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def reader(self):
        private_path(self.path, directory=False)
        conn = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=5)
        try:
            yield conn
        finally:
            conn.close()

    def _outside(self, registration):
        if registration.resource.resource_kind not in ("docker_image", "docker_volume"):
            target = Path(registration.resource.identity)
            if not target.is_absolute() or ".." in target.parts or target == Path("/"):
                raise ValueError("resource_path_invalid")
            if target.resolve() != target:
                raise ValueError("resource_symlink")
            if self.storage_root == target or self.storage_root.is_relative_to(target):
                raise ValueError("receipt_storage_unsafe")

    def _event(self, registration, operation_id, phase, evidence, *, outcome=None, observations=None, accounting=None):
        values = dict(
            event_id=event_identity(registration.resource, operation_id, phase),
            operation_id=operation_id,
            phase=phase,
            resource=registration.resource,
            source=registration.source,
            policy_version=registration.policy_version,
            correlation=registration.correlation,
            evidence=evidence,
            observations=observations or (Proof(),),
            outcome=outcome or Held(scope="retention", holds=(Hold(code="proof_unknown", subject="completion"),)),
            durable_committed_at=self.clock(),
            transaction_ref="sqlite-retention:" + hashlib.sha256(str(self.path).encode()).hexdigest(),
        )
        if accounting is not None:
            values["accounting"] = accounting
        return LifecycleEvent(**values)

    def register(self, registration: Registration) -> bool:
        registration = Registration.model_validate(registration.model_dump(mode="json"))
        self._outside(registration)
        key, packet = events.resource_key(registration.resource), _json(registration)
        with self.transaction() as conn:
            owner = registration.resource.owner.repository
            bound = conn.execute("SELECT owner FROM retention_authority WHERE id=1").fetchone()
            if bound is not None and bound[0] != owner:
                return False
            existing_owners = conn.execute(
                "SELECT DISTINCT json_extract(packet,'$.resource.owner.repository') FROM retention_registrations LIMIT 2"
            ).fetchall()
            if any(row[0] != owner for row in existing_owners):
                return False
            conn.execute("INSERT OR IGNORE INTO retention_authority VALUES (1,?)", (owner,))
            if registration.resource.resource_kind not in ("docker_image", "docker_volume"):
                alias = conn.execute(
                    "SELECT key FROM retention_registrations WHERE json_extract(packet,'$.resource.identity')=? AND key!=? LIMIT 1",
                    (registration.resource.identity, key),
                ).fetchone()
                if alias:
                    return False
            prior = conn.execute("SELECT packet FROM retention_registrations WHERE key=?", (key,)).fetchone()
            if prior:
                return prior[0] == packet
            value = self._event(
                registration, "registration:" + canonical_digest(registration), "registered", registration.evidence
            )
            events.record_resource_event(
                conn,
                value,
                registration=registration,
                state_change=lambda c: c.execute("INSERT INTO retention_registrations VALUES (?,?)", (key, packet)),
            )
        return True

    def registrations(self, owner: Owner) -> tuple[Registration, ...]:
        with self.reader() as conn:
            # Bounded configuration; no global or provider discovery.
            rows = conn.execute("SELECT packet FROM retention_registrations ORDER BY key LIMIT 1025").fetchall()
        if len(rows) > 1024:
            raise ValueError("registration_limit")
        return tuple(r for row in rows if (r := Registration.model_validate_json(row[0])).resource.owner == owner)

    def save_plan(self, plan: RetentionPlan) -> bool:
        plan = RetentionPlan.model_validate(plan.model_dump(mode="json"))
        self._outside(plan.registration)
        owner, packet = plan.registration.resource.owner.repository, _json(plan)
        opkey = event_identity(plan.registration.resource, plan.operation_id, "applied")
        with self.transaction() as conn:
            reg = conn.execute(
                "SELECT packet FROM retention_registrations WHERE key=?",
                (events.resource_key(plan.registration.resource),),
            ).fetchone()
            if not reg or reg[0] != _json(plan.registration):
                return False
            prior = conn.execute(
                "SELECT packet FROM retention_plans WHERE (owner=? AND plan_id=?) OR operation_key=?",
                (owner, plan.plan_id, opkey),
            ).fetchall()
            if prior:
                return len(prior) == 1 and prior[0][0] == packet
            ref = self._put(conn, packet.encode(), "retention-plan")
            value = self._event(plan.registration, plan.operation_id, "planned", (ref,))
            events.record_resource_event(
                conn,
                value,
                registration=plan.registration,
                state_change=lambda c: c.execute(
                    "INSERT INTO retention_plans VALUES (?,?,?,?)", (owner, plan.plan_id, opkey, packet)
                ),
            )
        return True

    def read_plan(self, owner, plan_id):
        with self.reader() as conn:
            row = conn.execute(
                "SELECT packet FROM retention_plans WHERE owner=? AND plan_id=?", (owner.repository, plan_id)
            ).fetchone()
        return RetentionPlan.model_validate_json(row[0]) if row else None

    def lookup(self, resource, operation_id):
        key = event_identity(resource, operation_id, "applied")
        with self.reader() as conn:
            row = conn.execute("SELECT packet FROM retention_operations WHERE key=?", (key,)).fetchone()
        return OperationRecord.model_validate_json(row[0]) if row else None

    def reserve(self, request: ApplyRequest) -> bool:
        request = ApplyRequest.model_validate(request.model_dump(mode="json"))
        plan = request.plan
        if request.dry_run or request.expected_plan_digest != canonical_digest(plan):
            return False
        key = event_identity(plan.registration.resource, plan.operation_id, "applied")
        record = OperationRecord(request=request, state="pending")
        with self.transaction() as conn:
            stored = conn.execute(
                "SELECT packet FROM retention_plans WHERE owner=? AND plan_id=?",
                (plan.registration.resource.owner.repository, plan.plan_id),
            ).fetchone()
            if (
                not stored
                or stored[0] != _json(plan)
                or conn.execute("SELECT 1 FROM retention_operations WHERE key=?", (key,)).fetchone()
            ):
                return False
            ref = self._put(conn, _json(request).encode(), "retention-intent")
            value = self._event(
                plan.registration, plan.operation_id, "probed", (ref,), observations=(request.probe.completion,)
            )
            events.record_resource_event(
                conn,
                value,
                registration=plan.registration,
                state_change=lambda c: c.execute("INSERT INTO retention_operations VALUES (?,?)", (key, _json(record))),
            )
        return True

    def complete(self, record: OperationRecord, effect: EffectReceipt) -> OperationRecord:
        record = OperationRecord.model_validate(record.model_dump(mode="json"))
        effect = EffectReceipt.model_validate(effect.model_dump(mode="json"))
        plan = record.request.plan
        if (
            effect.resource != plan.registration.resource
            or effect.operation_id != plan.operation_id
            or effect.plan_digest != canonical_digest(plan)
            or effect.completed_at > self.clock()
        ):
            raise ValueError("receipt_invalid")
        key = event_identity(effect.resource, effect.operation_id, "applied")
        with self.transaction() as conn:
            prior = conn.execute("SELECT packet FROM retention_operations WHERE key=?", (key,)).fetchone()
            if not prior:
                raise ValueError("intent_missing")
            current = OperationRecord.model_validate_json(prior[0])
            if current.request != record.request:
                raise ValueError("cas_changed")
            if current.state == "applied":
                return current  # First receipt retained even if a replay supplies different bytes.
            receipt = self._put(conn, _json(effect).encode(), "retention-receipt")
            terminal = OperationRecord(request=current.request, state="applied", effect=effect, receipt=receipt)
            value = self._event(
                plan.registration,
                plan.operation_id,
                "applied",
                (receipt, *effect.evidence),
                outcome=Allow(scope="retention"),
                accounting=effect.accounting,
                observations=(current.request.probe.completion,),
            )

            def change(c):
                changed = c.execute(
                    "UPDATE retention_operations SET packet=? WHERE key=? AND packet=?",
                    (_json(terminal), key, prior[0]),
                ).rowcount
                if changed != 1:
                    raise ValueError("cas_changed")

            events.record_resource_event(conn, value, registration=plan.registration, state_change=change)
        return terminal

    def _put(self, conn, payload: bytes, namespace: str) -> EvidenceRef:
        if len(payload) > 16 * 1024 * 1024:
            raise ValueError("evidence_limit")
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        ref = EvidenceRef(ref=namespace + ":" + digest[7:], digest=digest)
        conn.execute("INSERT OR IGNORE INTO retention_evidence VALUES (?,?,?)", (ref.ref, digest, payload))
        prior = conn.execute("SELECT digest,payload FROM retention_evidence WHERE ref=?", (ref.ref,)).fetchone()
        if prior != (digest, payload):
            raise ValueError("evidence_conflict")
        return ref

    def put_evidence(self, payload: bytes, namespace="retention-evidence"):
        with self.transaction() as conn:
            return self._put(conn, payload, namespace)

    def evidence(self, ref: EvidenceRef):
        with self.reader() as conn:
            row = conn.execute("SELECT digest,payload FROM retention_evidence WHERE ref=?", (ref.ref,)).fetchone()
        if not row or row[0] != ref.digest or "sha256:" + hashlib.sha256(row[1]).hexdigest() != ref.digest:
            raise ValueError("evidence_unknown")
        return row[1]

    def archive_effect(self, plan, payload, *, now, qualification=None):
        """Archive bytes + exact effect evidence in this authority, never a target write.

        The artifact is the effect. Its operation-bound receipt commits in the
        same transaction, so recovery never guesses from target absence.
        """
        key = event_identity(plan.registration.resource, plan.operation_id, "applied")
        with self.transaction() as conn:
            prior = conn.execute("SELECT plan_digest,receipt FROM retention_artifacts WHERE key=?", (key,)).fetchone()
            if prior:
                if prior[0] != canonical_digest(plan):
                    raise ValueError("cas_changed")
                return EffectReceipt.model_validate_json(prior[1])
            pending = conn.execute("SELECT packet FROM retention_operations WHERE key=?", (key,)).fetchone()
            if not pending:
                raise ValueError("intent_missing")
            record = OperationRecord.model_validate_json(pending[0])
            if record.request.plan != plan or record.state != "pending":
                raise ValueError("cas_changed")
            archive = self._put(conn, payload, "retention-archive")
            qualification_ref = (
                self._put(conn, qualification, "retention-effect-qualification") if qualification is not None else None
            )
            receipt = EffectReceipt(
                resource=plan.registration.resource,
                operation_id=plan.operation_id,
                plan_digest=canonical_digest(plan),
                evidence=(archive, qualification_ref) if qualification_ref is not None else (archive,),
                completed_at=now,
            )
            conn.execute(
                "INSERT INTO retention_artifacts VALUES (?,?,?)", (key, canonical_digest(plan), _json(receipt))
            )
        return receipt

    def archived_effect(self, record):
        plan = record.request.plan
        key = event_identity(plan.registration.resource, plan.operation_id, "applied")
        with self.reader() as conn:
            prior = conn.execute("SELECT plan_digest,receipt FROM retention_artifacts WHERE key=?", (key,)).fetchone()
        if prior is None:
            return None
        if prior[0] != canonical_digest(plan):
            raise ValueError("cas_changed")
        result = EffectReceipt.model_validate_json(prior[1])
        for ref in result.evidence:
            self.evidence(ref)
        return result

    def page(self, owner, *, cursor, limit, history):
        if type(cursor) is not int or cursor < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("page_bounds")

        def planned(alias):
            # R3 can also contain native/synthetic operations with no retention
            # plan. They remain in raw public lifecycle history; no plan digest
            # is invented to squeeze them into R4's retention summary DTO.
            return f"""EXISTS (SELECT 1 FROM retention_plans p
                WHERE p.owner={alias}.owner_repository
                AND json_extract(p.packet,'$.operation_id')={alias}.operation_id
                AND json_extract(p.packet,'$.registration.resource.identity')=json_extract({alias}.payload_json,'$.resource.identity')
                AND json_extract(p.packet,'$.registration.resource.resource_kind')=json_extract({alias}.payload_json,'$.resource.resource_kind'))"""

        # Latest retention status derives from the same immutable authority.
        where = (
            ""
            if history
            else f"""AND e.id=(SELECT MAX(n.id) FROM resource_events n
            WHERE n.resource_key=e.resource_key AND json_extract(n.payload_json,'$.phase')!='registered'
            AND {planned("n")})"""
        )
        with self.reader() as conn:
            rows = conn.execute(
                """SELECT e.id,e.payload_json FROM resource_events e
                WHERE e.owner_repository=? AND e.id>? AND json_extract(e.payload_json,'$.phase')!='registered' AND """
                + planned("e")
                + " "
                + where
                + " ORDER BY e.id LIMIT ?",
                (owner.repository, cursor, limit + 1),
            ).fetchall()
            result = []
            for seq, packet in rows[:limit]:
                event = LifecycleEvent.model_validate_json(packet)
                state = event.phase
                if (
                    state == "probed"
                    and conn.execute(
                        "SELECT 1 FROM retention_operations WHERE key=? AND json_extract(packet,'$.state')='pending'",
                        (event_identity(event.resource, event.operation_id, "applied"),),
                    ).fetchone()
                ):
                    state = "pending"
                result.append(
                    RecordSummary(
                        sequence=seq,
                        resource=event.resource,
                        operation_id=event.operation_id,
                        plan_digest=self._plan_digest(conn, event),
                        state=state,
                        receipt=event.evidence[0] if event.phase == "applied" else None,
                    )
                )
        return RecordPage(items=tuple(result), next_cursor=result[-1].sequence if len(rows) > limit else None)

    def _plan_digest(self, conn, event):
        row = conn.execute(
            "SELECT packet FROM retention_plans WHERE operation_key=?",
            (event_identity(event.resource, event.operation_id, "applied"),),
        ).fetchone()
        if not row:
            raise ValueError("plan_unknown")
        return canonical_digest(RetentionPlan.model_validate_json(row[0]))
