"""SQLite lifecycle authority/outbox seam for R5; no provider or execution effects.

The caller owns the connection, durable database location, BEGIN/COMMIT/ROLLBACK,
SQLite durability settings and producer qualification. Keep this database outside
retirable resources. ``record_resource_event`` requires an active transaction;
its optional state_change callback performs database DML on that SAME connection
and must not commit, roll back, close it, or perform external effects. A savepoint
rolls back this unit on failure without discarding earlier caller work. Replayed
events do not call the writer. The return value acknowledges a pending database
write, never a commit, deletion, native admission, or an automatic viewer update.

Resource keys hash the frozen owner/kind/identity, independently of optional lane
correlation. Frozen R0 lifecycle phases/facts are retained as supplied; task-start
or job state must be written by a qualified R5 producer, not guessed here.
History uses SQLite sequence order, never wall-clock last-write-wins. Cursors are
owner/consumer scoped and unfiltered; filtered history has explicit page offsets.
No compaction deletes history. All APIs retain the caller's row_factory.

Canon v0.26.2: DATA-14, No dual writes (Designing Data-Intensive Applications,
ch. 11; lexicons/engineering.md:232); DOM-06, Outbox or it didn't happen
(Learning Domain-Driven Design, ch. 9; engineering.md:684); OBS-02, One wide
event per unit of work (Observability Engineering, ch. 5; engineering.md:608);
OBS-08, Silence is not success (same work, ch. 18; engineering.md:614).
Published rows are verified; original distilled corpus is unavailable.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta
from uuid import uuid4

from workbay_protocol.resource_lifecycle import (
    Correlation,
    LifecycleEvent,
    Owner,
    Registration,
    ResourceIdentity,
    WorkBayIdentity,
    canonical_digest,
    event_identity,
)

READ_LIMIT_MAX = 500
READ_LIMIT_DEFAULT = 100


class ResourceEventHold(ValueError):
    """Storage/integration refusal; separate from frozen R0 admission holds."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _text(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
        raise ValueError("expected nonempty text without control characters")
    return value


def _integer(value: int, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"expected integer >= {minimum}")
    return value


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("expected timezone-aware datetime")
    return value


def _json(value) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def resource_key(resource: ResourceIdentity) -> str:
    """Collision-resistant resource namespace; never a lane or native marker."""
    resource = ResourceIdentity.model_validate(resource.model_dump(mode="python"))
    return "resource:" + canonical_digest(resource).removeprefix("sha256:")


def _rows(conn, sql, args=()):
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    try:
        return cursor.execute(sql, args).fetchall()
    finally:
        cursor.close()


def _transaction_required(conn):
    if not conn.in_transaction:
        raise ResourceEventHold("transaction_required")


@contextmanager
def _savepoint(conn):
    _transaction_required(conn)
    name = "resource_event_" + uuid4().hex
    conn.execute(f"SAVEPOINT {name}")
    try:
        yield
    except BaseException:
        conn.execute(f"ROLLBACK TO {name}")
        conn.execute(f"RELEASE {name}")
        raise
    else:
        conn.execute(f"RELEASE {name}")


def initialize_resource_events(conn: sqlite3.Connection) -> None:
    """Idempotent schema setup; never commits, opens or closes a connection."""
    # execute(), not executescript(): the latter commits an existing transaction.
    conn.execute("""CREATE TABLE IF NOT EXISTS resource_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        resource_key TEXT NOT NULL,
        owner_repository TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        registration_json TEXT NOT NULL,
        registration_digest TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        synthetic_tag TEXT,
        wb_task TEXT, wb_lane TEXT, wb_pass TEXT, wb_job TEXT,
        orca_run TEXT, orca_task TEXT, orca_dispatch TEXT,
        UNIQUE(owner_repository, synthetic_tag)
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS resource_events_resource_operation
        ON resource_events(resource_key, operation_id, id)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS resource_events_workbay
        ON resource_events(owner_repository, wb_task, wb_lane, id)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS resource_events_orca
        ON resource_events(owner_repository, orca_run, orca_task, orca_dispatch, id)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS resource_events_owner
        ON resource_events(owner_repository, id)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS resource_lifecycle_state (
        resource_key TEXT PRIMARY KEY,
        latest_event_id INTEGER NOT NULL REFERENCES resource_events(id)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS resource_event_consumers (
        owner_repository TEXT NOT NULL, consumer TEXT NOT NULL,
        last_event_id INTEGER NOT NULL, PRIMARY KEY(owner_repository, consumer)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS resource_event_visibility (
        owner_repository TEXT NOT NULL, consumer TEXT NOT NULL,
        event_id INTEGER NOT NULL REFERENCES resource_events(id),
        observed_at TEXT NOT NULL,
        PRIMARY KEY(owner_repository, consumer, event_id)
    )""")


def record_resource_event(
    conn: sqlite3.Connection,
    event: LifecycleEvent,
    *,
    registration: Registration,
    state_change: Callable[[sqlite3.Connection], object] | None = None,
) -> tuple[int, bool]:
    """Stage state + immutable R0 event atomically; return (sequence, inserted).

    R5 invokes this inside its authoritative task/lane transaction. state_change
    is a trusted, DML-only producer hook (see module contract). An identical
    duplicate is a no-op. Changed delivery raises event_payload_conflict and
    retains the first payload. A new operation is needed to change registration.
    Event durable_committed_at is the producer's supplied timestamp, not a
    SQLite commit callback or independently measured commit time.
    """
    _transaction_required(conn)
    # Revalidate even model_construct/model_copy inputs; never trust bypassed shape checks.
    event = LifecycleEvent.model_validate(event.model_dump(mode="python"))
    registration = Registration.model_validate(registration.model_dump(mode="python"))
    if event.event_id != event_identity(event.resource, event.operation_id, event.phase):
        raise ResourceEventHold("event_identity_mismatch")
    if any(
        getattr(event, name) != getattr(registration, name)
        for name in ("resource", "source", "policy_version", "correlation")
    ):
        raise ResourceEventHold("registration_mismatch")
    stream = registration.stream
    if stream and stream.state == "active" and stream.cadence_seconds is None and stream.deadline is None:
        raise ResourceEventHold("stream_expectation_missing")
    initialize_resource_events(conn)
    key = resource_key(event.resource)
    owner = event.resource.owner.repository
    payload, binding = _json(event), _json(registration)
    with _savepoint(conn):
        prior = _rows(conn, "SELECT * FROM resource_events WHERE event_id = ?", (event.event_id,))
        if prior:
            if prior[0]["payload_json"] != payload:
                raise ResourceEventHold("event_payload_conflict")
            if prior[0]["registration_json"] != binding:
                raise ResourceEventHold("registration_changed")
            return int(prior[0]["id"]), False
        prior = _rows(
            conn,
            "SELECT registration_json FROM resource_events WHERE resource_key=? AND operation_id=? LIMIT 1",
            (key, event.operation_id),
        )
        if prior and prior[0]["registration_json"] != binding:
            raise ResourceEventHold("registration_changed")
        if event.synthetic_tag is not None and _rows(
            conn,
            "SELECT id FROM resource_events WHERE owner_repository=? AND synthetic_tag=?",
            (owner, event.synthetic_tag),
        ):
            raise ResourceEventHold("synthetic_tag_conflict")
        wb, orca = event.correlation.workbay, event.correlation.orca
        cursor = conn.execute(
            """INSERT INTO resource_events (
            event_id, resource_key, owner_repository, operation_id,
            registration_json, registration_digest, payload_json, payload_digest, synthetic_tag,
            wb_task, wb_lane, wb_pass, wb_job, orca_run, orca_task, orca_dispatch
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                event.event_id,
                key,
                owner,
                event.operation_id,
                binding,
                canonical_digest(registration),
                payload,
                canonical_digest(event),
                event.synthetic_tag,
                wb.task if wb else None,
                wb.lane if wb else None,
                wb.pass_id if wb else None,
                wb.job if wb else None,
                orca.run if orca else None,
                orca.task if orca else None,
                orca.dispatch if orca else None,
            ),
        )
        sequence = int(cursor.lastrowid)
        if state_change is not None:
            state_change(conn)
        conn.execute(
            """INSERT INTO resource_lifecycle_state(resource_key, latest_event_id) VALUES (?,?)
            ON CONFLICT(resource_key) DO UPDATE SET latest_event_id=excluded.latest_event_id""",
            (key, sequence),
        )
    return sequence, True


def _record(row):
    return {
        "id": int(row["id"]),
        "resource_key": row["resource_key"],
        "payload": json.loads(row["payload_json"]),
        "payload_digest": row["payload_digest"],
        "registration": json.loads(row["registration_json"]),
    }


def _filters(owner, resource=None, correlation=None):
    owner = Owner.model_validate(owner.model_dump(mode="python"))
    clauses, args = ["owner_repository=?"], [owner.repository]
    if resource is not None:
        if resource.owner != owner:
            raise ResourceEventHold("foreign_owner")
        clauses.append("resource_key=?")
        args.append(resource_key(resource))
    if correlation is not None:
        correlation = Correlation.model_validate(correlation.model_dump(mode="python"))
        fields = (
            (
                correlation.workbay,
                (("task", "wb_task"), ("lane", "wb_lane"), ("pass_id", "wb_pass"), ("job", "wb_job")),
            ),
            (correlation.orca, (("run", "orca_run"), ("task", "orca_task"), ("dispatch", "orca_dispatch"))),
        )
        found = False
        for identity, columns in fields:
            for field, column in columns:
                value = getattr(identity, field, None)
                if value is not None:
                    clauses.append(column + "=?")  # column is internal, values are bound.
                    args.append(value)
                    found = True
        if not found:
            raise ValueError("correlation query requires at least one explicit ID")
    return " AND ".join(clauses), args


def resource_history(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    resource: ResourceIdentity | None = None,
    correlation: Correlation | None = None,
    after_id: int = 0,
    limit: int = READ_LIMIT_DEFAULT,
) -> dict:
    """Bounded history; correlation matches IDs, never asserts a verified join."""
    after_id = _integer(after_id)
    limit = min(_integer(limit, 1), READ_LIMIT_MAX)
    where, args = _filters(owner, resource, correlation)
    initialize_resource_events(conn)
    rows = _rows(
        conn, f"SELECT * FROM resource_events WHERE {where} AND id>? ORDER BY id LIMIT ?", (*args, after_id, limit + 1)
    )
    page = rows[:limit]
    return {
        "events": [_record(row) for row in page],
        "next_id": int(page[-1]["id"]) if page else after_id,
        "has_more": len(rows) > limit,
    }


def _offset(conn, consumer, owner):
    _text(consumer)
    owner = Owner.model_validate(owner.model_dump(mode="python"))
    rows = _rows(
        conn,
        "SELECT last_event_id FROM resource_event_consumers WHERE owner_repository=? AND consumer=?",
        (owner.repository, consumer),
    )
    return int(rows[0][0]) if rows else 0


def read_resource_events(
    conn: sqlite3.Connection, consumer: str, *, owner: Owner, limit: int = READ_LIMIT_DEFAULT
) -> dict:
    """At-least-once owner-scoped replay; reads do not acknowledge delivery."""
    initialize_resource_events(conn)
    return resource_history(conn, owner=owner, after_id=_offset(conn, consumer, owner), limit=limit)


def ack_resource_events(conn: sqlite3.Connection, consumer: str, upto_id: int, *, owner: Owner) -> int:
    """Stage a monotonic cursor in caller transaction; reject foreign/future IDs.

    Caller acknowledges only after processing the whole unfiltered prefix. Put
    projection DML and this ack on this connection for atomic replay consumption.
    """
    _transaction_required(conn)
    _text(consumer)
    upto_id = _integer(upto_id)
    owner = Owner.model_validate(owner.model_dump(mode="python"))
    initialize_resource_events(conn)
    if upto_id and not _rows(
        conn, "SELECT id FROM resource_events WHERE owner_repository=? AND id=?", (owner.repository, upto_id)
    ):
        raise ResourceEventHold("cursor_target_unknown")
    conn.execute(
        """INSERT INTO resource_event_consumers(owner_repository, consumer, last_event_id) VALUES (?,?,?)
        ON CONFLICT(owner_repository, consumer) DO UPDATE SET
        last_event_id=MAX(resource_event_consumers.last_event_id,excluded.last_event_id)""",
        (owner.repository, consumer, upto_id),
    )
    return _offset(conn, consumer, owner)


def resource_status(conn: sqlite3.Connection, resource: ResourceIdentity) -> dict:
    """Latest committed/staged facts on this connection; no invented runtime state."""
    initialize_resource_events(conn)
    rows = _rows(
        conn,
        """SELECT e.* FROM resource_lifecycle_state s JOIN resource_events e ON e.id=s.latest_event_id
        WHERE s.resource_key=?""",
        (resource_key(resource),),
    )
    return {"knowledge": "observed" if rows else "unknown", "latest": _record(rows[0]) if rows else None}


def task_history(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    task_ref: str,
    lane_id: str | None = None,
    after_id: int = 0,
    limit: int = READ_LIMIT_DEFAULT,
) -> dict:
    """WorkBay task/lane history, independent of optional Orca correlation."""
    return resource_history(
        conn,
        owner=owner,
        correlation=Correlation(workbay=WorkBayIdentity(task=task_ref, lane=lane_id)),
        after_id=after_id,
        limit=limit,
    )


def task_status(conn: sqlite3.Connection, *, owner: Owner, task_ref: str, lane_id: str | None = None) -> dict:
    """Latest recorded task/lane lifecycle snapshot; absence means unknown."""
    initialize_resource_events(conn)
    where, args = _filters(owner, correlation=Correlation(workbay=WorkBayIdentity(task=task_ref, lane=lane_id)))
    rows = _rows(conn, f"SELECT * FROM resource_events WHERE {where} ORDER BY id DESC LIMIT 1", args)
    return {"knowledge": "observed" if rows else "unknown", "latest": _record(rows[0]) if rows else None}


def _connection(connected):
    if connected is not None and type(connected) is not bool:
        raise ValueError("connected must be bool or None")
    return "connected" if connected is True else "disconnected" if connected is False else "unknown"


def resource_freshness(
    conn: sqlite3.Connection, resource: ResourceIdentity, *, now: datetime, connected: bool | None
) -> dict:
    """Active staleness immediately after earliest cadence/deadline + grace.

    Call/query at least every 30 seconds to meet stale_visible_by. This function
    does not schedule a poll, activate a collector, or infer connectivity from age.
    """
    _aware(now)
    result = {
        "connection": _connection(connected),
        "stream_freshness": "unknown",
        "historical_age_seconds": None,
        "expected_by": None,
        "stale_visible_by": None,
    }
    latest = resource_status(conn, resource)["latest"]
    if latest is None:
        return result
    value = LifecycleEvent.model_validate(latest["payload"])
    age = (now - value.durable_committed_at).total_seconds()
    result["historical_age_seconds"] = age
    if age < 0:
        return result
    stream = Registration.model_validate(latest["registration"]).stream
    if stream is None:
        return result
    if stream.state != "active":
        result["stream_freshness"] = "not_applicable"
        return result
    expectations = []
    if stream.cadence_seconds is not None:
        expectations.append(value.durable_committed_at + timedelta(seconds=stream.cadence_seconds))
    if stream.deadline is not None:
        expectations.append(stream.deadline)
    if expectations:
        expected = min(expectations) + timedelta(seconds=stream.grace_seconds)
        result.update(
            expected_by=expected.isoformat(),
            stale_visible_by=(expected + timedelta(seconds=30)).isoformat(),
            stream_freshness="stale" if now > expected else "fresh",
        )
    return result


def observe_synthetic_event(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    consumer: str,
    tag: str,
    committed_at: datetime,
    now: datetime,
    connected: bool | None,
) -> dict:
    """Stage first connected query observation of a uniquely tagged event.

    Requires caller transaction. committed_at comes from the qualified producer;
    it is checked against the frozen event timestamp, never guessed for a missing
    event. arrival_seconds measures FIRST QUERY OBSERVATION, not automatic viewer
    rendering. A late first observation fails this measurement; it cannot prove
    when an unobserved viewer first displayed the event. Rollback loses the
    observation receipt. Disconnected/unknown queries do not record an arrival.
    """
    _transaction_required(conn)
    _aware(now)
    _aware(committed_at)
    _text(consumer)
    _text(tag)
    owner = Owner.model_validate(owner.model_dump(mode="python"))
    connection = _connection(connected)
    initialize_resource_events(conn)
    if connected is not True:
        return {"visibility": connection, "target_met": None, "arrival_seconds": None}
    if now < committed_at:
        return {"visibility": "unknown", "target_met": None, "arrival_seconds": None}
    rows = _rows(
        conn, "SELECT * FROM resource_events WHERE owner_repository=? AND synthetic_tag=?", (owner.repository, tag)
    )
    if not rows:
        overdue = (now - committed_at).total_seconds() > 30
        return {
            "visibility": "overdue" if overdue else "pending",
            "target_met": False if overdue else None,
            "arrival_seconds": None,
        }
    value = LifecycleEvent.model_validate_json(rows[0]["payload_json"])
    if value.durable_committed_at != committed_at:
        raise ResourceEventHold("commit_time_mismatch")
    sequence = rows[0]["id"]
    conn.execute(
        """INSERT INTO resource_event_visibility(owner_repository, consumer, event_id, observed_at) VALUES (?,?,?,?)
        ON CONFLICT(owner_repository, consumer, event_id) DO NOTHING""",
        (owner.repository, consumer, sequence, now.isoformat()),
    )
    observed = _rows(
        conn,
        "SELECT observed_at FROM resource_event_visibility WHERE owner_repository=? AND consumer=? AND event_id=?",
        (owner.repository, consumer, sequence),
    )[0][0]
    elapsed = (datetime.fromisoformat(observed) - committed_at).total_seconds()
    return {"visibility": "visible", "target_met": 0 <= elapsed <= 30, "arrival_seconds": elapsed}
