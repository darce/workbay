"""Bounded read views of the immutable R3 authority, independent of target files.

No provider effects, caller authentication, or Orca authority live here. The
optional orchestrator package supplies R3; absence is an explicit capability
failure. Configure WORKBAY_RESOURCE_OWNER with the registered repository
namespace for generated workspace views. Public queries always require it.

Canon v0.26.2: DATA-14 No dual writes (Designing Data-Intensive Applications,
ch. 11; lexicons/engineering.md:232), OBS-08 Silence is not success
(Observability Engineering, ch. 18; engineering.md:614). Original corpus
unavailable. Views derive from retained events; age never implies connectivity.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path

from workbay_protocol.resource_lifecycle import (
    Correlation,
    Owner,
    ResourceIdentity,
)

PAGE_MAX = 100
RESPONSE_BYTES_MAX = 262_144
_VIEW_OWNER: ContextVar[str | None] = ContextVar("resource_view_owner", default=None)


@contextmanager
def resource_view_scope(owner: Owner):
    """Bind a trusted runtime owner for renders without changing process env."""
    token = _VIEW_OWNER.set(owner.repository)
    try:
        yield
    finally:
        _VIEW_OWNER.reset(token)


def event_store():
    return import_module("workbay_orchestrator_mcp.orchestration.resource_events")


@contextmanager
def existing_authority(db_path: Path):
    """Open a configured existing authority, never create an empty replacement.

    Deliberately narrower than the native bootstrap factory: mode=rw prevents
    creation and a 1s lock bound keeps reconnect polls below the visibility
    interval. No journal-mode changes. FULL synchronous and foreign keys apply
    to cursor/projection transactions; R3 controls its additive schema.
    """
    conn = sqlite3.connect(db_path.resolve().as_uri() + "?mode=rw", uri=True, timeout=1)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=1000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        with conn:
            yield conn
    finally:
        conn.close()


def page_bounds(after_id: int, limit: int) -> tuple[int, int]:
    if type(after_id) is not int or after_id < 0:
        raise ValueError("after_id must be a nonnegative integer")
    if type(limit) is not int or not 1 <= limit <= PAGE_MAX:
        raise ValueError(f"limit must be between 1 and {PAGE_MAX}")
    return after_id, limit


def _bounded(value: dict) -> dict:
    if len(json.dumps(value, ensure_ascii=True).encode()) > RESPONSE_BYTES_MAX:
        raise ValueError("resource_response_too_large; request a smaller history page")
    return value


def history(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    resource: ResourceIdentity | None = None,
    correlation: Correlation | None = None,
    after_id: int = 0,
    limit: int = 25,
) -> dict:
    after_id, limit = page_bounds(after_id, limit)
    result = event_store().resource_history(
        conn, owner=owner, resource=resource, correlation=correlation, after_id=after_id, limit=limit
    )
    return _bounded({**result, "connection": "connected", "caller_identity": "unverified"})


def status(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    resource: ResourceIdentity | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    now: datetime | None = None,
    connected: bool | None = True,
) -> dict:
    store = event_store()
    if resource is not None:
        if resource.owner != owner:
            raise ValueError("foreign_owner")
        result = store.resource_status(conn, resource)
    elif task_ref is not None:
        result = store.task_status(conn, owner=owner, task_ref=task_ref, lane_id=lane_id)
    else:
        raise ValueError("resource or task_ref required")
    latest = result["latest"]
    identity = resource or (ResourceIdentity.model_validate(latest["payload"]["resource"]) if latest else None)
    freshness = (
        store.resource_freshness(conn, identity, now=now or datetime.now(UTC), connected=connected)
        if identity is not None
        else {
            "connection": "connected" if connected is True else "disconnected" if connected is False else "unknown",
            "stream_freshness": "unknown",
            "historical_age_seconds": None,
        }
    )
    # A matching task row is a WorkBay fact, never proof of an Orca join.
    native = None
    native_lane = None
    configured_owner = _VIEW_OWNER.get() or os.environ.get("WORKBAY_RESOURCE_OWNER", "").strip()
    native_scope_known = result.get("reason") != "resource_store_uninitialized" and (
        latest is not None or configured_owner == owner.repository
    )
    if (
        native_scope_known
        and task_ref is not None
        and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='handoff_state'").fetchone()
    ):
        cursor = conn.cursor()
        cursor.row_factory = sqlite3.Row
        try:
            row = cursor.execute(
                "SELECT task_ref, objective, focus, status, revision, target_branch, target_worktree_path "
                "FROM handoff_state WHERE task_ref=?",
                (task_ref,),
            ).fetchone()
            native = dict(row) if row else None
        finally:
            cursor.close()
    if (
        native_scope_known
        and task_ref is not None
        and lane_id is not None
        and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worktree_lanes'").fetchone()
    ):
        cursor = conn.cursor()
        cursor.row_factory = sqlite3.Row
        try:
            row = cursor.execute(
                "SELECT id, task_ref, lane_id, status, branch, worktree_path, notes "
                "FROM worktree_lanes WHERE task_ref=? AND lane_id=?",
                (task_ref, lane_id),
            ).fetchone()
            native_lane = dict(row) if row else None
        finally:
            cursor.close()
    return _bounded(
        {
            **result,
            "freshness": freshness,
            "native_task": native,
            "native_lane": native_lane,
            "caller_identity": "unverified",
        }
    )


def snapshot(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    task_ref: str | None = None,
    lane_id: str | None = None,
    limit: int = 25,
    now: datetime | None = None,
) -> dict:
    """Latest bounded resource state plus recent history, including retired tasks.

    History pagination is explicit and independent of any shared replay cursor.
    Synthetic records are marked and never counted as collector activity.
    """
    page_bounds(0, limit)
    store = event_store()
    where, args = "owner_repository=?", [owner.repository]
    if task_ref is not None:
        where += " AND wb_task=?"
        args.append(task_ref)
    if lane_id is not None:
        if task_ref is None:
            raise ValueError("lane_id requires task_ref")
        where += " AND wb_lane=?"
        args.append(lane_id)
    if not store.resource_store_initialized(conn):
        return {
            "connection": "connected",
            "owner_repository": owner.repository,
            "caller_identity": "unverified",
            "knowledge": "unknown",
            "reason": "resource_store_uninitialized",
            "resources": [],
            "history": [],
            "history_has_more": False,
            "resources_has_more": False,
            "history_before_id": None,
        }
    # IDs and references are read from the authority, never from a retired path.
    rows = conn.execute(
        f"SELECT id FROM resource_events WHERE {where} ORDER BY id DESC LIMIT ?", (*args, limit + 1)
    ).fetchall()
    ids = [int(row[0]) for row in rows[:limit]]
    recent = []
    for sequence in reversed(ids):
        record = store.resource_history(conn, owner=owner, after_id=sequence - 1, limit=1)["events"][0]
        recent.append(record)
    resources = conn.execute(
        "SELECT e.payload_json FROM resource_lifecycle_state s "
        "JOIN resource_events e ON e.id=s.latest_event_id "
        f"WHERE {where} ORDER BY e.id DESC LIMIT ?",
        (*args, limit + 1),
    ).fetchall()
    current = []
    for row in resources[:limit]:
        identity = ResourceIdentity.model_validate(json.loads(row[0])["resource"])
        current.append(status(conn, owner=owner, resource=identity, now=now))
    return _bounded(
        {
            "connection": "connected",
            "owner_repository": owner.repository,
            "caller_identity": "unverified",
            "resources": current,
            "history": recent,
            "history_has_more": len(rows) > limit,
            "resources_has_more": len(resources) > limit,
            "history_before_id": min(ids) if ids else None,
        }
    )


def configured_snapshot(conn: sqlite3.Connection, *, task_ref: str | None = None) -> dict:
    namespace = _VIEW_OWNER.get() or os.environ.get("WORKBAY_RESOURCE_OWNER", "").strip()
    if not namespace:
        return {"connection": "unknown", "knowledge": "unknown", "reason": "resource_owner_unconfigured"}
    try:
        return snapshot(conn, owner=Owner(repository=namespace), task_ref=task_ref)
    except ValueError:
        return {"connection": "unknown", "knowledge": "unknown", "reason": "resource_snapshot_invalid_or_too_large"}
    except (ImportError, sqlite3.Error):
        # Never expose paths/SQL/payloads from exception text to the viewer.
        return {"connection": "disconnected", "knowledge": "unknown", "reason": "resource_authority_unavailable"}


def render_snapshot(view: dict) -> str:
    """Compact human view; structured facts/evidence remain in public history."""
    lines = [f"connection={view.get('connection', 'unknown')}"]
    if view.get("reason"):
        lines.append(str(view["reason"]))
    for item in view.get("resources", []):
        latest = item["latest"]
        event = latest["payload"]
        wb = event["correlation"].get("workbay") or {}
        orca = event["correlation"].get("orca") or {}
        label = "synthetic" if event.get("synthetic_tag") else "resource"
        freshness = item["freshness"]
        lines.append(
            f"{label} event={latest['id']} phase={event['phase']} "
            f"task={wb.get('task')} lane={wb.get('lane')} pass={wb.get('pass_id')} job={wb.get('job')} "
            f"orca_run={orca.get('run')} orca_task={orca.get('task')} orca_dispatch={orca.get('dispatch')} "
            f"stream={freshness['stream_freshness']} tag={event.get('synthetic_tag')}"
        )
    lines.append("recent_history=" + ",".join(str(record["id"]) for record in view.get("history", [])))
    return "\n".join(lines)


def consume_page(conn: sqlite3.Connection, *, owner: Owner, consumer: str, limit: int = 100) -> dict:
    """Caller transaction: projection and unfiltered owner cursor move together.

    Durable snapshot pointers survive restarts. They reference immutable R3
    records, not target files or copies of producer facts. Rendering is retried
    on every connected poll, even when the replay prefix is empty.
    """
    if not conn.in_transaction:
        raise ValueError("transaction_required")
    page_bounds(0, limit)
    store = event_store()
    store.initialize_resource_events(conn)
    page = store.read_resource_events(conn, consumer, owner=owner, limit=limit)
    conn.execute("""CREATE TABLE IF NOT EXISTS resource_viewer_projection (
        owner_repository TEXT NOT NULL, consumer TEXT NOT NULL, resource_key TEXT NOT NULL,
        latest_event_id INTEGER NOT NULL REFERENCES resource_events(id),
        PRIMARY KEY(owner_repository, consumer, resource_key))""")
    for record in page["events"]:
        conn.execute(
            """INSERT INTO resource_viewer_projection VALUES (?,?,?,?)
            ON CONFLICT(owner_repository,consumer,resource_key) DO UPDATE SET
            latest_event_id=MAX(resource_viewer_projection.latest_event_id,excluded.latest_event_id)""",
            (owner.repository, consumer, record["resource_key"], record["id"]),
        )
    if page["events"]:
        store.ack_resource_events(conn, consumer, page["next_id"], owner=owner)
    return page
