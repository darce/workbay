"""Lane DML and R3 outbox on the caller's SQLite authority; no external effects.

Prepare Git identity before the writer lock. The outer savepoint includes the
native DML (and its identity/epoch triggers), retained snapshot and R3 event.
The R3 callback only stores evidence; it never commits or performs I/O.

Canon v0.26.2: DATA-14, No dual writes (Designing Data-Intensive Applications,
ch. 11; engineering:232); DOM-06, Outbox or it didn't happen (Learning
Domain-Driven Design, ch. 9; engineering:684). Original corpus unavailable.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

from .lifecycle_resource_producer import ProducerBinding, _event_store, _git, prepare_binding
from .shared_write_context import ResolvedWriteContext


class LaneResourceConflict(ValueError):
    """An explicit delivery or pre-lock lane identity no longer matches."""


def prepare_lane_binding(worktree_path: str, branch: str) -> ProducerBinding:
    """Qualify the actual lane checkout, independently of the caller's cwd.

    Caller attribution is separate from resource source identity. No Orca join,
    runtime model, cadence, custody or admission proof follows from this probe.
    """
    from pathlib import Path

    _event_store()  # Load the optional storage capability before any SQLite writer lock.
    revision = _git(Path(worktree_path), "rev-parse", "HEAD")
    ctx = ResolvedWriteContext(
        agent=None,
        branch=branch.removeprefix("refs/heads/"),
        commit_sha=revision,
        lane_id=None,
        model=None,
        model_label=None,
        reasoning_level=None,
    )
    return prepare_binding(ctx, worktree_path)


def _operation(event_id: str) -> str:
    if not isinstance(event_id, str) or not event_id.strip() or any(ord(c) < 32 for c in event_id):
        raise LaneResourceConflict("lane_event_id_invalid")
    return "lane:" + hashlib.sha256(event_id.strip().encode()).hexdigest()


def _row(conn, task_ref, lane_id):
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    try:
        value = cursor.execute(
            "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (task_ref, lane_id)
        ).fetchone()
        return dict(value) if value is not None else None
    finally:
        cursor.close()


def replay_receipt(conn, *, task_ref: str, lane_id: str, operation: str, event_id: str | None) -> dict | None:
    """Recover an explicit delivery before CAS/DML; never replay across lanes."""
    if event_id is None:
        return None
    key = _operation(event_id)
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lane_resource_producer_evidence'"
    ).fetchone():
        return None
    row = conn.execute(
        "SELECT payload_json FROM lane_resource_producer_evidence WHERE operation_id=?", (key,)
    ).fetchone()
    if row is None:
        return None
    snapshot = json.loads(row[0])
    if (snapshot["task_ref"], snapshot["lane_id"], snapshot["operation"]) != (task_ref, lane_id, operation):
        raise LaneResourceConflict("lane_event_identity_mismatch")
    event = conn.execute(
        "SELECT id,event_id FROM resource_events WHERE operation_id=? AND wb_task=? AND wb_lane=?",
        (key, task_ref, lane_id),
    ).fetchone()
    if event is None:
        raise LaneResourceConflict("lane_event_receipt_incomplete")
    return {
        "knowledge": "observed",
        "sequence": event[0],
        "event_id": event[1],
        "inserted": False,
        "snapshot": snapshot,
    }


def execute_lane_change(
    conn: sqlite3.Connection,
    sql: str,
    parameters: tuple,
    *,
    binding: ProducerBinding,
    task_ref: str,
    lane_id: str,
    operation: str,
    expected_row: dict | None,
    event_id: str | None = None,
) -> tuple[sqlite3.Cursor, dict]:
    """Run native SQL unchanged, then retain its actual triggered row atomically.

    Missing optional R0/R3/source capability preserves native behavior with an
    explicit unknown receipt. A known event-store failure rolls back this unit.
    No registration stream is invented: lane status has no heartbeat contract.
    """
    if not conn.in_transaction:
        raise ValueError("lane_resource_transaction_required")
    key = _operation(event_id) if event_id is not None else "lane:" + uuid4().hex
    current = _row(conn, task_ref, lane_id)
    identity_columns = ("id", "task_ref", "lane_id", "identity_seq", "cycle_epoch", "worktree_path", "branch")
    if (current is None) != (expected_row is None) or (
        current is not None and any(current.get(k) != expected_row.get(k) for k in identity_columns)
    ):
        raise LaneResourceConflict("lane_resource_identity_changed")
    store = _event_store()
    reason = binding.reason
    if store is None:
        reason = reason or "resource_events_package_unavailable"
    name = "lane_producer_" + uuid4().hex
    conn.execute(f"SAVEPOINT {name}")
    try:
        cursor = conn.execute(sql, parameters)
        if cursor.rowcount == 0:
            receipt = {"knowledge": "unknown", "reason": "lane_mutation_not_applied"}
        elif reason or binding.resource is None or binding.source is None:
            receipt = {"knowledge": "unknown", "reason": reason or "source_unavailable"}
        else:
            from workbay_protocol.resource_lifecycle import (
                Correlation,
                EvidenceRef,
                Held,
                Hold,
                LifecycleEvent,
                Proof,
                Registration,
                WorkBayIdentity,
                event_identity,
            )

            lane = _row(conn, task_ref, lane_id)
            snapshot = {
                "task_ref": task_ref,
                "lane_id": lane_id,
                "operation": operation,
                "lane": lane,
                "producer_host": binding.host,
            }
            payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
            digest = "sha256:" + hashlib.sha256(payload.encode()).hexdigest()
            evidence = EvidenceRef(ref="sqlite:lane-mutation/" + key.split(":", 1)[1], digest=digest)
            correlation = Correlation(workbay=WorkBayIdentity(task=task_ref, lane=lane_id))
            registration = Registration(
                resource=binding.resource,
                source=binding.source,
                operation="withdraw_materialization",
                policy_version="consumer-retention-v1",
                evidence=(evidence,),
                correlation=correlation,
            )
            event = LifecycleEvent(
                event_id=event_identity(binding.resource, key, "registered"),
                operation_id=key,
                phase="registered",
                resource=binding.resource,
                source=binding.source,
                policy_version=registration.policy_version,
                evidence=(evidence,),
                correlation=correlation,
                outcome=Held(scope="retention", holds=(Hold(code="proof_unknown", subject="retention.qualification"),)),
                observations=(Proof(evidence=(evidence,)),),
                durable_committed_at=datetime.now(timezone.utc),
                transaction_ref=evidence.ref,
                synthetic_tag=event_id if event_id and event_id.startswith("synthetic:") else None,
            )
            store.initialize_resource_events(conn)
            conn.execute("""CREATE TABLE IF NOT EXISTS lane_resource_producer_evidence (
                operation_id TEXT PRIMARY KEY, ref TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL, digest TEXT NOT NULL
            )""")

            def retain_evidence(connection):
                connection.execute(
                    "INSERT INTO lane_resource_producer_evidence VALUES (?,?,?,?)",
                    (key, evidence.ref, payload, digest),
                )

            sequence, inserted = store.record_resource_event(
                conn,
                event,
                registration=registration,
                state_change=retain_evidence,
            )
            receipt = {"knowledge": "observed", "sequence": sequence, "event_id": event.event_id, "inserted": inserted}
    except BaseException:
        conn.execute(f"ROLLBACK TO {name}")
        conn.execute(f"RELEASE {name}")
        raise
    conn.execute(f"RELEASE {name}")
    return cursor, receipt
