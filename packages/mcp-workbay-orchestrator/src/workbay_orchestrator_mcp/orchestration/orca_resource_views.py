"""Replaceable Orca correlation reader and WorkBay connected viewer consumer.

WorkBay owns lifecycle state. Orca is an optional catalogue, never the producer
authority. A process/SSH job is never converted into an Orca Dispatch. This repo
does not contain the external Orca application viewer or its catalogue client;
its host must implement OrcaCatalogue and consume these bounded snapshots.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from workbay_protocol.resource_lifecycle import Correlation, OrcaIdentity, Owner, WorkBayIdentity

from . import resource_events


class OrcaCatalogue(Protocol):
    """Host-normalized actual records; each blocking lookup must have a timeout.

    The external host translates its real schema to id/run_id/task_id/status.
    None means no record; transport failures raise. No filesystem/command/API
    credentials are accepted by this adapter.
    """

    def run(self, run_id: str) -> Mapping[str, str | None] | None: ...

    def task(self, task_id: str) -> Mapping[str, str | None] | None: ...

    def dispatch(self, dispatch_id: str) -> Mapping[str, str | None] | None: ...


def orca_history(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    orca: OrcaIdentity,
    workbay: WorkBayIdentity | None = None,
    after_id: int = 0,
    limit: int = 25,
) -> dict:
    from workbay_handoff_mcp.lifecycle_resource_views import history

    # No fallback from a missing Orca filter to all owner events.
    if not any((orca.run, orca.task, orca.dispatch)):
        raise ValueError("explicit Orca identity required")
    return history(
        conn, owner=owner, correlation=Correlation(orca=orca, workbay=workbay), after_id=after_id, limit=limit
    )


def orca_task_view(
    conn: sqlite3.Connection,
    *,
    owner: Owner,
    orca: OrcaIdentity,
    workbay: WorkBayIdentity | None = None,
    catalogue: OrcaCatalogue | None = None,
    after_id: int = 0,
    limit: int = 25,
) -> dict:
    """Host consumer seam: current WorkBay task/lane, Orca joins, and history.

    Catalogue relationships are checked separately from event correlation. A
    ready Orca Task can have no Dispatch. No provider/process ledger is read.
    All current/history data remains available after the target disappears.
    """
    from workbay_handoff_mcp.lifecycle_resource_views import status

    page = orca_history(conn, owner=owner, orca=orca, workbay=workbay, after_id=after_id, limit=limit)
    latest = None
    if page.get("reason") != "resource_store_uninitialized":
        # Latest matching event must be independent of the requested history page.
        clauses, args = ["owner_repository=?"], [owner.repository]
        for identity, fields in (
            (orca, (("run", "orca_run"), ("task", "orca_task"), ("dispatch", "orca_dispatch"))),
            (workbay, (("task", "wb_task"), ("lane", "wb_lane"), ("pass_id", "wb_pass"), ("job", "wb_job"))),
        ):
            for name, column in fields:
                value = getattr(identity, name, None)
                if value is not None:
                    clauses.append(column + "=?")
                    args.append(value)
        row = conn.execute(
            "SELECT id FROM resource_events WHERE " + " AND ".join(clauses) + " ORDER BY id DESC LIMIT 1", args
        ).fetchone()
        latest = (
            resource_events.resource_history(conn, owner=owner, after_id=int(row[0]) - 1, limit=1)["events"][0]
            if row
            else None
        )
    correlation = (
        Correlation.model_validate(latest["payload"]["correlation"])
        if latest
        else Correlation(orca=orca, workbay=workbay)
    )
    wb = correlation.workbay
    current = status(conn, owner=owner, task_ref=wb.task, lane_id=wb.lane) if wb and wb.task else None
    result = {
        "connection": "connected",
        "knowledge": "observed" if latest else "unknown",
        "latest": latest,
        "workbay_status": current,
        "history": page,
        "catalogue": orca_join(correlation, catalogue=catalogue),
        "caller_identity": "unverified",
    }
    if page.get("reason") == "resource_store_uninitialized":
        result["reason"] = "resource_store_uninitialized"
    if len(json.dumps(result, ensure_ascii=True).encode()) > 262_144:
        raise ValueError("orca_view_response_too_large; request a smaller history page")
    return result


def orca_join(correlation: Correlation, *, catalogue: OrcaCatalogue | None = None) -> dict:
    """Verify supplied IDs against actual catalogue relationships, preserving null.

    Matching IDs in an event alone are correlation, not a verified join. Host
    lookup failures do not erase WorkBay history or imply a missing Dispatch.
    The returned join is an observation, never a native execution receipt.
    """
    ids = correlation.orca
    result = {
        "workbay": correlation.workbay.model_dump(mode="json") if correlation.workbay else None,
        "orca": ids.model_dump(mode="json") if ids else None,
        "join": "unknown",
        "catalogue_connection": "unknown",
        "join_scope": "orca_catalogue_only",
        "workbay_join": "unverified",
        "producer_join_proof": (
            correlation.verified_join.model_dump(mode="json") if correlation.verified_join else None
        ),
        "run": None,
        "task": None,
        "dispatch": None,
        "external_work_summary": "No Orca Dispatch is asserted by WorkBay correlation.",
        "caller_identity": "unverified",
    }
    if ids is None or not any((ids.run, ids.task, ids.dispatch)) or catalogue is None:
        result["reason"] = "orca_catalogue_unavailable" if catalogue is None else "orca_correlation_absent"
        return result
    try:
        records = {
            "run": catalogue.run(ids.run) if ids.run else None,
            "task": catalogue.task(ids.task) if ids.task else None,
            "dispatch": catalogue.dispatch(ids.dispatch) if ids.dispatch else None,
        }
    except (OSError, TimeoutError, ConnectionError):
        result.update(catalogue_connection="disconnected", reason="orca_catalogue_disconnected")
        return result
    result["catalogue_connection"] = "connected"
    for kind, record in records.items():
        expected = getattr(ids, kind)
        if expected is not None and (record is None or record.get("id") != expected):
            result.update(join="unresolved", reason="orca_record_missing_or_mismatched")
            return result
    task, dispatch = records["task"], records["dispatch"]
    if task is not None and ids.run is not None and task.get("run_id") != ids.run:
        result.update(join="contradictory", reason="orca_task_run_mismatch")
        return result
    if dispatch is not None and (
        (ids.task is not None and dispatch.get("task_id") != ids.task)
        or (ids.run is not None and dispatch.get("run_id") != ids.run)
    ):
        result.update(join="contradictory", reason="orca_dispatch_parent_mismatch")
        return result
    for kind, record in records.items():
        if record is not None:
            # Explicit allowlist; never copy arbitrary host payloads.
            result[kind] = {key: record.get(key) for key in ("id", "run_id", "task_id", "status")}
    result["join"] = "observed_catalogue_relationships"
    if ids.dispatch is None:
        result["external_work_summary"] = "External work summary; no Orca Dispatch supplied."
    else:
        result["external_work_summary"] = "Observed Orca catalogue relationships; WorkBay remains authoritative."
    return result


def refresh_resource_viewer(
    *,
    db_path: Path,
    owner: Owner,
    consumer: str,
    task_ref: str,
    lane_id: str | None = None,
    now: datetime | None = None,
) -> dict:
    """One bounded connected poll, with durable replay and fresh snapshot.

    Opens only the configured existing authority (mode=rw); a disconnected
    database is never silently replaced by an empty one. The caller's existing
    poll loop repeats this call. Up to 100 unfiltered owner events are projected
    per poll. Snapshot reads latest authority immediately, including backlog.
    """
    from workbay_handoff_mcp.lifecycle_resource_views import consume_page, existing_authority, snapshot

    try:
        with existing_authority(db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            page = consume_page(conn, owner=owner, consumer=consumer)
            view = snapshot(conn, owner=owner, task_ref=task_ref, lane_id=lane_id, now=now)
        view["consumer"] = consumer
        view["replay"] = {"next_id": page["next_id"], "has_more": page["has_more"]}
        # Independent wall clock at the consumer, not an inferred commit clock.
        view["observed_at"] = datetime.now(UTC).isoformat()
        return view
    except (sqlite3.Error, OSError):
        return {
            "connection": "disconnected",
            "knowledge": "unknown",
            "reason": "resource_authority_unavailable",
            "consumer": consumer,
        }


def observe_rendered_synthetics(*, db_path: Path, owner: Owner, consumer: str, view: dict) -> list[dict]:
    """Record query/render observations only after the automatic sink succeeded.

    R3 timestamps remain producer supplied. Actual commit-to-sink wall time is
    measured separately by acceptance fixtures, never inferred from this table.
    """
    if view.get("connection") != "connected":
        return []
    receipts = []
    from workbay_handoff_mcp.lifecycle_resource_views import existing_authority

    with existing_authority(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        for record in view.get("history", []):
            payload = record["payload"]
            tag = payload.get("synthetic_tag")
            if tag:
                receipts.append(
                    resource_events.observe_synthetic_event(
                        conn,
                        owner=owner,
                        consumer=consumer,
                        tag=tag,
                        committed_at=datetime.fromisoformat(payload["durable_committed_at"].replace("Z", "+00:00")),
                        now=datetime.now(UTC),
                        connected=True,
                    )
                )
    return receipts
