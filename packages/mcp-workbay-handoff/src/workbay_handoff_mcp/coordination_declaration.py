"""Native execution-domain declaration on the existing R3 lane event authority.

No new status table or column. SQLite orders declaration, dispatch observations
and terminal authorization. The cleanup journal is only its replay projection.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Literal

from .shared_schema import (
    EPOCH_PREFIX,
    EPOCH_SCHEMA,
    EPOCH_SESSION,
    EPOCH_TASK,
    IDENTITY_TRIGGER_SHA256,
    _installed_triggers,
    _migrate_add_worktree_lanes_identity_seq,
    _record_observation_epoch,
    _stale_identity_triggers,
)

ExecutionDomain = Literal["local_coordination"]
CoordinationState = Literal["queued", "unknown", "held", "failure", "terminal"]
ROUTING = ("backend", "model", "reasoning_effort", "speed", "tier")


class CoordinationHold(ValueError):
    """Known evidence refuses the requested coordination transition."""


class CoordinationFailure(ValueError):
    """An unavailable probe or authority leaves a retryable obligation."""


class CoordinationUnknown(CoordinationHold):
    """Native dispatch history is incomplete: an unknown obligation, never zero."""


HISTORY_INCOMPLETE = "coordination_dispatch_history_incomplete"
OBSERVED_TABLES = ("lane_messages", "worker_reports", "turn_metrics", "plan_cursors")
DISPATCH_SCHEMA = "workbay/coordination-dispatch-observation@1"
NATIVE_ACTOR_FIELDS = (
    "lane_id",
    "agent",
    "harness",
    "model",
    "model_label",
    "reasoning_level",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "branch",
    "commit_sha",
    "slice_number",
)


def immutable_fact_condition(ref: str = "") -> str:
    """SQL predicate naming immutable coordination facts (``ref`` is ``OLD.`` or a table alias).

    The single definition used by the protection triggers and by native
    maintenance (system-decision GC, archive prune), so retention cannot drift.
    """
    return (
        f"(({ref}session='native-dispatch' AND ({ref}decision GLOB 'coordination_dispatch:*'"
        f" OR {ref}decision GLOB '{EPOCH_PREFIX}*'))"
        f" OR ({ref}session='landing-cleanup' AND {ref}decision GLOB 'coordination_terminal:*')"
        f" OR ({ref}session='native-launch' AND {ref}decision GLOB 'native_execution_reserved:*'))"
    )


PROOF_ERRORS = (ValueError, OSError, sqlite3.Error, subprocess.SubprocessError, ImportError)


def pending_error(exc: BaseException) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "pending",
        "state": (
            "unknown"
            if isinstance(exc, CoordinationUnknown)
            else "held"
            if isinstance(exc, CoordinationHold)
            else "failure"
        ),
        "reason": str(exc) or "coordination_proof_unknown",
        "error_type": type(exc).__name__,
    }


def declaration_errors(function):
    """Translate failures after unwinding the transaction and the worker fence."""

    @wraps(function)
    def guarded(**kwargs):
        try:
            return function(**kwargs)
        except PROOF_ERRORS as exc:
            if kwargs.get("execution_domain") is None:
                raise
            from .shared_primitives import _envelope

            return _envelope(
                ok=False,
                tool="open_lane",
                entity="lane",
                data={"error_kind": "coordination_declaration_held", "error": str(exc), **pending_error(exc)},
            )

    return guarded


IDENTITY = ("id", "identity_seq", "task_ref", "lane_id", "branch", "worktree_path", "owner_agent", "branch_tip_sha")


def owner_identity(row: dict[str, Any]) -> dict[str, Any]:
    return {k: row.get(k) for k in IDENTITY}


def execution_absence(conn: sqlite3.Connection, row: dict[str, Any], *, pending_insert: bool = False) -> None:
    """Known complete native stores plus actual process/lock checks, not None routing.

    Empty present tables prove nothing unless trigger coverage was intact for
    the row's entire life; ``observation_coverage`` decides that, last.
    """
    task, lane = row["task_ref"], row["lane_id"]
    if conn.execute(
        "SELECT 1 FROM decisions WHERE task_ref=? AND decision=? LIMIT 1", (task, "native_execution_reserved:" + lane)
    ).fetchone():
        raise CoordinationHold("coordination_native_dispatch_reserved")
    for table in ("worker_reports", "turn_metrics", "lane_messages", "plan_cursors"):
        if conn.execute(f"SELECT 1 FROM {table} WHERE task_ref=? AND lane_id=? LIMIT 1", (task, lane)).fetchone():
            raise CoordinationHold("coordination_execution_observed:" + table)
    if conn.execute(
        "SELECT 1 FROM decisions WHERE task_ref=? AND decision=? LIMIT 1", (task, "coordination_dispatch:" + lane)
    ).fetchone():
        raise CoordinationHold("coordination_dispatch_observed")
    observation_coverage(conn, row, pending_insert=pending_insert)


def probe_worker_history(row: dict[str, Any]) -> None:
    """Filesystem/process observations happen outside the SQLite writer lock."""
    from .runtime import get_runtime_config

    root = Path(get_runtime_config().state_dir)
    lane = row["lane_id"]
    for path in (
        root / f"worker-{lane}.status.json",
        root / f"worker-{lane}.jsonl",
        root / "logs" / f"worker-{lane}.jsonl",
    ):
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        raise CoordinationHold("coordination_worker_history_present")


def probe_execution_process(row: dict[str, Any]) -> None:
    from .shared_write_context import assert_no_write_lock_held

    assert_no_write_lock_held("subprocess")
    lane = row["lane_id"]
    probe_worker_history(row)
    probe = subprocess.run(
        ["pgrep", "-af", "[w]orker_daemon.*--lane-id[ =]" + re.escape(lane) + r"([[:space:]]|$)"],
        capture_output=True,
        timeout=5,
    )
    if probe.returncode == 0:
        raise CoordinationHold("coordination_worker_present")
    if probe.returncode != 1:
        raise CoordinationFailure("coordination_worker_probe_unknown")


@contextmanager
def worker_fence(row: dict[str, Any]) -> Iterator[None]:
    from workbay_orchestrator_mcp.orchestration.worker_lock import try_acquire_worker_lock

    from .runtime import get_runtime_config

    path = Path(get_runtime_config().state_dir) / f"worker-{row['lane_id']}.lock"
    handle = try_acquire_worker_lock(path)
    if handle is None:
        raise CoordinationHold("coordination_worker_active")
    try:
        # Existing payload is execution evidence even if its process has exited.
        handle.seek(0)
        payload = handle.read().strip()
        if payload:
            record = json.loads(payload)
            # The native local reaper uses the same kernel lock. Its released
            # reclamation claim is not a worker dispatch or backend execution.
            if not isinstance(record, dict) or record.get("claim") != "reaping" or record.get("released") is not True:
                raise CoordinationHold("coordination_worker_history_present")
        yield
    finally:
        # This is a read-only fence, not a worker or reaper claim. Closing
        # releases the kernel flock without rewriting an existing receipt or
        # unlinking its inode. The native registry prunes closed handles.
        handle.close()


def observation_triggers(*, legacy: bool = False) -> dict[str, str]:
    """Exact native trigger set; stored SQL must match byte-for-byte to count as coverage."""
    triggers: dict[str, str] = {}
    for table in OBSERVED_TABLES:
        for event in ("INSERT", "UPDATE"):
            name = f"coordination_observe_{table}_{event.lower()}"
            # BEFORE observes an attempted write, including sources with an
            # auto-assigned id. Capture the NEW source and lane binding here,
            # in the same decision that already records the observation.
            rationale = (
                f"'{table}'"
                if legacy
                else f"""json_object(
                         'schema','{DISPATCH_SCHEMA}', 'table','{table}', 'operation','{event}',
                         'source_created_at',NEW.created_at,
                         'lane_row_id',l.id, 'identity_seq',l.identity_seq,
                         'observed_at',datetime('now'),
                         'epoch_id',(SELECT MAX(id) FROM decisions WHERE task_ref='{EPOCH_TASK}'
                            AND session='{EPOCH_SESSION}' AND decision GLOB '{EPOCH_PREFIX}*'))"""
            )
            lane_join = (
                ""
                if legacy
                else """
                       FROM (SELECT 1) LEFT JOIN worktree_lanes l
                         ON l.task_ref=NEW.task_ref AND l.lane_id=NEW.lane_id"""
            )
            triggers[name] = f"""CREATE TRIGGER {name}
            BEFORE {event} ON {table}
            BEGIN
              SELECT CASE WHEN EXISTS(SELECT 1 FROM decisions d JOIN worktree_lanes l
                ON l.task_ref=NEW.task_ref AND l.lane_id=NEW.lane_id
                WHERE d.task_ref=l.task_ref AND d.decision='coordination_terminal:' || l.id || ':' || l.identity_seq)
                THEN RAISE(ABORT,'coordination_terminal_execution_forbidden') END;
              INSERT INTO decisions(task_ref,session,decision,rationale,decision_origin)
                SELECT NEW.task_ref,'native-dispatch','coordination_dispatch:' || NEW.lane_id,
                       {rationale},'system'{lane_join} WHERE NEW.lane_id IS NOT NULL AND NOT EXISTS(
                         SELECT 1 FROM decisions WHERE task_ref=NEW.task_ref
                           AND decision='coordination_dispatch:' || NEW.lane_id);
            END"""
    triggers.update(_decision_guards())
    return triggers


def _decision_guards() -> dict[str, str]:
    triggers = {}
    condition = immutable_fact_condition("OLD.")
    triggers["coordination_fact_delete"] = f"""CREATE TRIGGER coordination_fact_delete BEFORE DELETE ON decisions
        WHEN {condition} BEGIN SELECT RAISE(ABORT,'coordination_fact_immutable'); END"""
    triggers["coordination_fact_update"] = f"""CREATE TRIGGER coordination_fact_update
        BEFORE UPDATE OF task_ref,session,decision,rationale ON decisions
        WHEN {condition} BEGIN SELECT RAISE(ABORT,'coordination_fact_immutable'); END"""
    # The epoch's identity includes its timestamp and origin: no column may change.
    triggers["coordination_epoch_update"] = f"""CREATE TRIGGER coordination_epoch_update BEFORE UPDATE ON decisions
        WHEN OLD.session='{EPOCH_SESSION}' AND OLD.decision GLOB '{EPOCH_PREFIX}*'
        BEGIN SELECT RAISE(ABORT,'coordination_fact_immutable'); END"""
    return triggers


def _stale_triggers(conn: sqlite3.Connection, expected: dict[str, str]) -> list[str]:
    installed = _installed_triggers(conn, tuple(expected))
    return sorted(name for name, sql in expected.items() if installed.get(name) != sql)


def _epoch_bound(conn: sqlite3.Connection) -> tuple[int, float] | None:
    """Most conservative bound over all immutable epochs; malformed evidence is unknown."""
    rows = conn.execute(
        "SELECT rationale, julianday(created_at), decision_origin FROM decisions"
        " WHERE task_ref=? AND session=? AND decision GLOB ?",
        (EPOCH_TASK, EPOCH_SESSION, EPOCH_PREFIX + "*"),
    ).fetchall()
    if not rows:
        return None
    high, since = 0, 0.0
    for rationale, created, origin in rows:
        try:
            data = json.loads(rationale)
        except (TypeError, ValueError):
            raise CoordinationUnknown(HISTORY_INCOMPLETE) from None
        if not isinstance(data, dict):
            raise CoordinationUnknown(HISTORY_INCOMPLETE)
        lanes = data.get("lanes_high_water")
        if (
            origin != "system"
            or data.get("schema") != EPOCH_SCHEMA
            or type(lanes) is not int
            or lanes < 0
            or not isinstance(created, float)
        ):
            raise CoordinationUnknown(HISTORY_INCOMPLETE)
        high, since = max(high, lanes), max(since, created)
    return high, since


def install_dispatch_observation(conn: sqlite3.Connection) -> None:
    """Begin (or resume) uninterrupted dispatch coverage at the native writer boundary.

    Runs inside the caller's write transaction. When the exact trigger set is
    absent or altered, it is recreated and a new immutable epoch records the
    lane-id high-water in the same transaction, so every row that existed while
    coverage was missing stays permanently unproven. Nothing about earlier
    history is inferred here.
    """
    if not conn.in_transaction:
        raise CoordinationFailure("coordination_observation_transaction_required")
    expected = observation_triggers()
    stale = _stale_triggers(conn, expected)
    # A byte-for-byte predecessor set can be upgraded under the same writer
    # lock without an observation gap. Other drift still starts a new epoch.
    uninterrupted = (not stale or not _stale_triggers(conn, observation_triggers(legacy=True))) and (
        _epoch_bound(conn) is not None
    )
    for name in stale:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        conn.execute(expected[name])
    # If generation coverage lapsed, restart it under this same writer lock.
    # Keep the repair on the existing epoch ledger so an unchanged integer can
    # never requalify a receipt from before the recorded gap.
    stale_identity = _stale_identity_triggers(conn)
    if stale_identity:
        _migrate_add_worktree_lanes_identity_seq(conn)
        if _stale_identity_triggers(conn):
            raise CoordinationFailure("coordination_identity_trigger_repair_failed")
    if uninterrupted and not stale_identity:
        return
    _record_observation_epoch(conn, stale + stale_identity)


def observation_coverage(conn: sqlite3.Connection, row: dict[str, Any], *, pending_insert: bool = False) -> None:
    """Prove the row's whole life is inside one uninterrupted observation epoch.

    Holds as unknown when coverage is currently broken, no epoch exists, or the
    row (by AUTOINCREMENT id or creation time) predates the latest epoch. A row
    about to be inserted in this same transaction (``pending_insert``) is new
    only when coverage is already intact before its insert.
    """
    if _stale_triggers(conn, observation_triggers()) and _stale_triggers(conn, observation_triggers(legacy=True)):
        raise CoordinationUnknown(HISTORY_INCOMPLETE)
    bound = _epoch_bound(conn)
    if bound is None:
        raise CoordinationUnknown(HISTORY_INCOMPLETE)
    if pending_insert:
        return
    high, since = bound
    row_id, created = row.get("id"), row.get("created_at")
    if type(row_id) is not int or row_id <= high or not isinstance(created, str):
        raise CoordinationUnknown(HISTORY_INCOMPLETE)
    after = conn.execute("SELECT julianday(?) >= ?", (created, since)).fetchone()[0]
    if after != 1:
        raise CoordinationUnknown(HISTORY_INCOMPLETE)


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _sha(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def _native_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").isoformat(sep=" ") == value
    except ValueError:
        return False


def _epoch_receipt(conn: sqlite3.Connection, row: sqlite3.Row, data: dict) -> bool:
    replaced = data.get("replaced_triggers")
    trigger_names = observation_triggers().keys() | IDENTITY_TRIGGER_SHA256.keys()
    if not (
        row["task_ref"] == EPOCH_TASK
        and set(data) == {"schema", "lanes_high_water", "lane_messages_high_water", "replaced_triggers"}
        and data["schema"] == EPOCH_SCHEMA
        and all(type(data[k]) is int and data[k] >= 0 for k in ("lanes_high_water", "lane_messages_high_water"))
        and isinstance(replaced, list)
        and all(isinstance(name, str) and name in trigger_names for name in replaced)
        and replaced == sorted(set(replaced))
    ):
        return False
    ordinal = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE task_ref=? AND session=? AND decision GLOB ? AND id<=?",
        (EPOCH_TASK, EPOCH_SESSION, EPOCH_PREFIX + "*", row["id"]),
    ).fetchone()[0]
    return row["decision"] == EPOCH_PREFIX + str(ordinal)


def _native_envelope(row: sqlite3.Row) -> bool:
    return (
        row["decision_origin"] == "system"
        and all(row[k] is None for k in NATIVE_ACTOR_FIELDS)
        and row["changed_files_json"] == "[]"
        and _native_timestamp(row["created_at"])
    )


def _stored_epoch_payload(conn: sqlite3.Connection, row: sqlite3.Row) -> dict | None:
    if not _native_envelope(row):
        return None
    try:
        data = json.loads(row["rationale"])
    except (TypeError, ValueError):
        return None
    return data if isinstance(data, dict) and _epoch_receipt(conn, row, data) else None


def has_stored_observation_epoch(conn: sqlite3.Connection) -> bool:
    """Check committed predecessors in ledger order even if the wall clock stepped back."""
    rows = conn.execute(
        "SELECT * FROM decisions WHERE task_ref=? AND session=? AND decision GLOB ? ORDER BY id DESC",
        (EPOCH_TASK, EPOCH_SESSION, EPOCH_PREFIX + "*"),
    )
    # An older ledger may still be waiting for its attribution columns.
    columns = {column[0] for column in rows.description}
    if not {*NATIVE_ACTOR_FIELDS, "changed_files_json", "decision_origin", "rationale"} <= columns:
        return False
    return any(_stored_epoch_payload(conn, row) is not None for row in rows)


def _protection_intact(conn: sqlite3.Connection, row: sqlite3.Row, *, uses_generation: bool) -> bool:
    guards = _decision_guards()
    if _stale_triggers(conn, guards) or (uses_generation and _stale_identity_triggers(conn)):
        return False
    required = set(guards)
    if uses_generation:
        required.update(IDENTITY_TRIGGER_SHA256)
    # An observation-only restart does not invalidate the event-time binding of
    # a typed receipt. A repair of a guard it depends on breaks continuity.
    for epoch in conn.execute(
        "SELECT * FROM decisions WHERE task_ref=? AND session=? AND decision GLOB ? AND id>?",
        (EPOCH_TASK, EPOCH_SESSION, EPOCH_PREFIX + "*", row["id"]),
    ):
        data = _stored_epoch_payload(conn, epoch)
        if data is None or required.intersection(data["replaced_triggers"]):
            return False
    return True


def _preceding_epoch(conn: sqlite3.Connection, row: sqlite3.Row) -> sqlite3.Row | None:
    # Decision IDs order the producer's writes even within the same second.
    epoch = conn.execute(
        "SELECT * FROM decisions WHERE task_ref=? AND session=? AND decision GLOB ? AND id<? ORDER BY id DESC LIMIT 1",
        (EPOCH_TASK, EPOCH_SESSION, EPOCH_PREFIX + "*", row["id"]),
    ).fetchone()
    if (
        epoch is None
        or native_fact_provenance_issue(conn, epoch) is not None
        or epoch["created_at"] > row["created_at"]
    ):
        return None
    return epoch


def _dispatch_provenance(conn: sqlite3.Connection, row: sqlite3.Row, lane: sqlite3.Row, data: dict) -> str | None:
    if (
        set(data)
        != {
            "schema",
            "table",
            "operation",
            "source_created_at",
            "lane_row_id",
            "identity_seq",
            "observed_at",
            "epoch_id",
        }
        or data["schema"] != DISPATCH_SCHEMA
        or data["table"] not in OBSERVED_TABLES
        or data["operation"] not in ("INSERT", "UPDATE")
        or not _native_timestamp(data["source_created_at"])
        or data["observed_at"] != row["created_at"]
        or data["source_created_at"] > row["created_at"]
    ):
        return "coordination_dispatch_source_unverified"
    if (
        not _positive_int(data["lane_row_id"])
        or not _positive_int(data["identity_seq"])
        or (data["lane_row_id"], data["identity_seq"]) != (lane["id"], lane["identity_seq"])
        or not _native_timestamp(lane["created_at"])
        or lane["created_at"] > data["source_created_at"]
    ):
        return "coordination_dispatch_lane_identity_unverified"
    if not _positive_int(data["epoch_id"]):
        return "coordination_dispatch_epoch_unverified"
    # A later coverage restart cannot retroactively supply this observation's epoch.
    epoch = _preceding_epoch(conn, row)
    if epoch is None or epoch["id"] != data["epoch_id"]:
        return "coordination_dispatch_epoch_unverified"
    return None


def _legacy_dispatch_provenance(conn: sqlite3.Connection, row: sqlite3.Row, lane: sqlite3.Row) -> str | None:
    # Both legacy lane-message triggers record the same first observation. The
    # earlier native reservation binds the slug to the still-current generation.
    # A surviving brief corroborates activity in that lifetime.
    try:
        observation_coverage(conn, dict(lane))
    except CoordinationUnknown:
        return "coordination_dispatch_coverage_unverified"

    epoch = conn.execute(
        "SELECT * FROM decisions WHERE task_ref=? AND session=? AND decision GLOB ? ORDER BY id DESC LIMIT 1",
        (EPOCH_TASK, EPOCH_SESSION, EPOCH_PREFIX + "*"),
    ).fetchone()
    if (
        epoch is None
        or epoch["id"] >= row["id"]
        or native_fact_provenance_issue(conn, epoch) is not None
        or not _native_timestamp(lane["created_at"])
        or not epoch["created_at"] <= lane["created_at"] <= row["created_at"]
    ):
        return "coordination_dispatch_epoch_unverified"

    reservation = conn.execute(
        "SELECT * FROM decisions WHERE task_ref=? AND session='native-launch' AND decision=?",
        (row["task_ref"], "native_execution_reserved:" + lane["lane_id"]),
    ).fetchone()
    if (
        reservation is None
        or not epoch["id"] < reservation["id"] < row["id"]
        or native_fact_provenance_issue(conn, reservation) is not None
        or not lane["created_at"] <= reservation["created_at"] <= row["created_at"]
    ):
        return "coordination_dispatch_reservation_unverified"
    if conn.execute(
        "SELECT 1 FROM decisions WHERE task_ref=? AND decision=? AND id<? LIMIT 1",
        (row["task_ref"], row["decision"], row["id"]),
    ).fetchone():
        return "coordination_dispatch_observation_conflict"

    message_high_water = json.loads(epoch["rationale"])["lane_messages_high_water"]
    # Brief fields can be supplied by normal message writers or imports. Only
    # the native marker and reservation establish authority and ledger order.
    messages = conn.execute(
        """SELECT agent, branch, commit_sha, dispatch_id, created_at FROM lane_messages
           WHERE task_ref=? AND lane_id=? AND id>? AND created_at BETWEEN ? AND ?
             AND session='dispatch_lane_work' AND direction='orchestrator_to_worker'""",
        (row["task_ref"], lane["lane_id"], message_high_water, lane["created_at"], row["created_at"]),
    ).fetchall()
    if not messages or any(
        message["branch"] != lane["branch"]
        or not isinstance(message["agent"], str)
        or not message["agent"].strip()
        or not _sha(message["commit_sha"])
        or not isinstance(message["dispatch_id"], str)
        or not message["dispatch_id"].strip()
        or not _native_timestamp(message["created_at"])
        for message in messages
    ):
        return "coordination_dispatch_source_unverified"
    return None


def native_fact_provenance_issue(conn: sqlite3.Connection, row: sqlite3.Row) -> str | None:
    """Validate a candidate from immutable_fact_condition without attributing it to an actor.

    The ledger and its native SQLite writers are trusted. Public decision and
    import writes carry actor metadata. The legacy lane-message arm validates
    observed activity for a reserved generation using existing ledger facts.
    """
    if not _native_envelope(row):
        return "coordination_fact_envelope_unverified"
    try:
        data = json.loads(row["rationale"])
    except (TypeError, ValueError):
        data = None
    kind, _, key = row["decision"].partition(":")
    if not _protection_intact(conn, row, uses_generation=kind != "coordination_observation_epoch"):
        return "coordination_fact_protection_unverified"
    legacy = kind == "coordination_dispatch" and row["rationale"] == "lane_messages"
    if not legacy and not isinstance(data, dict):
        return "coordination_fact_payload_unverified"
    if kind == "coordination_observation_epoch":
        return None if _epoch_receipt(conn, row, data) else "coordination_observation_epoch_unverified"

    if kind == "coordination_dispatch":
        lane_id = key
    else:
        owner = data if kind == "native_execution_reserved" else data.get("identity")
        if not isinstance(owner, dict) or not isinstance(owner.get("lane_id"), str):
            return "coordination_fact_lane_identity_unverified"
        lane_id = owner["lane_id"]
    lane = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (row["task_ref"], lane_id)
    ).fetchone()
    if lane is None or not lane_id:
        return "coordination_fact_lane_identity_unverified"
    if kind == "coordination_dispatch":
        if legacy:
            return _legacy_dispatch_provenance(conn, row, lane)
        return _dispatch_provenance(conn, row, lane, data)

    # Launch snapshots describe the event-time owner and tip. Those two fields
    # may advance without changing the lane's database-owned identity sequence.
    if (
        set(owner) != set(IDENTITY)
        or not _positive_int(owner["id"])
        or not _positive_int(owner["identity_seq"])
        or any(owner[k] != lane[k] for k in IDENTITY if k not in ("owner_agent", "branch_tip_sha"))
        or (owner["owner_agent"] is not None and not isinstance(owner["owner_agent"], str))
        or (owner["branch_tip_sha"] is not None and not _sha(owner["branch_tip_sha"]))
        or row["rationale"] != json.dumps(data, sort_keys=True)
    ):
        return "coordination_fact_lane_identity_unverified"
    if kind == "native_execution_reserved":
        if key != lane_id:
            return "coordination_fact_lane_identity_unverified"
        if not _native_timestamp(lane["created_at"]) or row["created_at"] < lane["created_at"]:
            return "coordination_reservation_predates_lane"
        return None if _preceding_epoch(conn, row) is not None else "coordination_reservation_epoch_unverified"

    if (
        key != f"{lane['id']}:{lane['identity_seq']}"
        or set(data) != {"identity", "intent_id", "reason", "status", "state", "ok"}
        or data["reason"] != "declared_zero_dispatch"
        or data["status"] != "not_required"
        or data["state"] != "terminal"
        or data["ok"] is not True
        or not isinstance(data["intent_id"], str)
        or re.fullmatch(r"[0-9a-f]{64}", data["intent_id"]) is None
        or not _sha(owner["branch_tip_sha"])
        or owner != owner_identity(dict(lane))
        or lane["status"] != "merged"
        or not _sha(lane["landing_commit_sha"])
        or _preceding_epoch(conn, row) is None
    ):
        return "coordination_terminal_unverified"
    try:
        if declaration(conn, dict(lane)) is not None:
            execution_absence(conn, dict(lane))
            return None
    except (CoordinationHold, TypeError, ValueError, sqlite3.Error):
        pass
    return "coordination_terminal_unverified"


def prepare_declaration(
    conn: sqlite3.Connection,
    *,
    current,
    values: dict[str, Any],
    domain: ExecutionDomain,
    proof: dict | None,
    event_id: str | None,
) -> dict[str, Any]:
    if domain != "local_coordination" or not event_id or not values.get("owner_agent"):
        raise CoordinationHold("coordination_declaration_invalid")
    if any(values.get(k) is not None for k in ROUTING):
        raise CoordinationHold("coordination_execution_routing_present")
    if current is not None:
        if (
            not isinstance(proof, dict)
            or type(proof.get("id")) is not int
            or type(proof.get("identity_seq")) is not int
        ):
            raise CoordinationHold("coordination_owner_cas_failed")
        if (
            not isinstance(proof, dict)
            or {k: proof.get(k) for k in IDENTITY} != owner_identity(dict(current))
            or proof.get("owner_agent") != values["owner_agent"]
        ):
            raise CoordinationHold("coordination_owner_cas_failed")
        if any(current[k] is not None for k in ROUTING):
            raise CoordinationHold("coordination_execution_routing_present")
        if any(values.get(k) != current[k] for k in ("branch", "worktree_path", "branch_tip_sha")):
            raise CoordinationHold("coordination_adoption_identity_changed")
    if not values.get("branch_tip_sha"):
        raise CoordinationHold("coordination_source_tip_required")
    # Coverage must already be intact (or begin now, recording an epoch that
    # every existing row predates). Only a row inserted after it is complete.
    install_dispatch_observation(conn)
    row = dict(current) if current is not None else dict(values)
    execution_absence(conn, row, pending_insert=current is None)
    return dict(execution_domain=domain, owner_proof=proof, event_id=event_id)


def declaration(conn: sqlite3.Connection, row: dict[str, Any]) -> dict[str, Any] | None:
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lane_resource_producer_evidence'"
    ).fetchone():
        return None
    rows = conn.execute(
        """SELECT e.payload_json,e.digest FROM lane_resource_producer_evidence e
        JOIN resource_events v ON v.operation_id=e.operation_id
        WHERE v.wb_task=? AND v.wb_lane=? AND json_extract(e.payload_json,'$.execution_domain')='local_coordination'
        ORDER BY v.id DESC LIMIT 1""",
        (row["task_ref"], row["lane_id"]),
    ).fetchall()
    if not rows:
        return None
    for payload, digest in rows:
        if not isinstance(payload, str) or "sha256:" + hashlib.sha256(payload.encode()).hexdigest() != digest:
            raise CoordinationHold("coordination_declaration_unreadable")
        data = json.loads(payload)
        if not isinstance(data, dict) or not isinstance(data.get("lane"), dict):
            raise CoordinationHold("coordination_declaration_unreadable")
        if owner_identity(data["lane"]) == owner_identity(row):
            return data
    raise CoordinationHold("coordination_owner_generation_changed")


def verify_declaration_replay(
    conn: sqlite3.Connection,
    snapshot: dict,
    current: dict | None,
    *,
    domain: ExecutionDomain,
    proof: dict | None,
    values: dict,
) -> None:
    """A receipt replay is historical, never a new owner or route mutation."""
    recorded = snapshot["lane"]
    if proof is not None and (
        not isinstance(proof, dict) or type(proof.get("id")) is not int or type(proof.get("identity_seq")) is not int
    ):
        raise CoordinationHold("coordination_owner_cas_failed")
    if (
        snapshot.get("execution_domain") != domain
        or snapshot.get("owner_proof") != proof
        or any(recorded.get(k) != v for k, v in values.items() if k not in ROUTING)
        or any(values.get(k) is not None for k in ROUTING)
    ):
        raise CoordinationHold("coordination_event_identity_mismatch")
    if current is None or owner_identity(current) != owner_identity(recorded):
        raise CoordinationHold("coordination_owner_generation_changed")
    if declaration(conn, current) is None:
        raise CoordinationHold("coordination_declaration_unreadable")


def verify_terminal_authorization(conn: sqlite3.Connection, intent, proof: dict | None) -> None:
    """Verify a journal projection against its immutable native authorization."""
    if not isinstance(proof, dict) or not isinstance(proof.get("identity"), dict):
        raise CoordinationHold("coordination_terminal_unverified")
    owner = proof["identity"]
    snapshot, identity = intent["lane_row"], intent["identity"]
    if (
        type(owner.get("id")) is not int
        or type(owner.get("identity_seq")) is not int
        or owner != owner_identity(snapshot)
        or owner.get("task_ref") != identity["task_ref"]
        or owner.get("lane_id") != identity["lane_id"]
        or owner.get("branch_tip_sha") != identity["expected_tip"]
        or proof.get("intent_id") != intent["intent_id"]
        or proof.get("reason") != "declared_zero_dispatch"
        or proof.get("status") != "not_required"
        or proof.get("state") != "terminal"
        or proof.get("ok") is not True
    ):
        raise CoordinationHold("coordination_terminal_identity_conflict")
    decision = f"coordination_terminal:{owner['id']}:{owner['identity_seq']}"
    event = conn.execute(
        "SELECT rationale FROM decisions WHERE task_ref=? AND decision=?", (identity["task_ref"], decision)
    ).fetchone()
    if event is None or json.loads(event[0]) != proof:
        raise CoordinationHold("coordination_terminal_unverified")


def authorize_not_required(conn: sqlite3.Connection, intent, rows: list[dict]) -> dict | None:
    """Commit under BEGIN IMMEDIATE and worker flock; the journal follows."""
    identity, snapshot = intent["identity"], intent["lane_row"]
    current = next(
        (r for r in rows if r.get("task_ref") == identity["task_ref"] and r.get("lane_id") == identity["lane_id"]), None
    )
    if current is None:
        raise CoordinationHold("coordination_registry_unknown")
    if declaration(conn, current) is None:
        return None
    if (
        intent["local_status"] != "complete"
        or current.get("status") != "merged"
        or current.get("branch_tip_sha") != identity["expected_tip"]
        or current.get("landing_commit_sha") != identity["landing_commit"]
        or any(current.get(k) != snapshot.get(k) for k in IDENTITY)
    ):
        raise CoordinationHold("coordination_owner_cas_failed")
    if any(current.get(k) is not None for k in ROUTING):
        raise CoordinationHold("coordination_execution_routing_present")
    if any(
        r.get("id") != current["id"]
        and (r.get("branch") == current["branch"] or r.get("worktree_path") == current["worktree_path"])
        for r in rows
    ):
        raise CoordinationHold("coordination_foreign_resource_owner")
    if conn.execute(
        "SELECT 1 FROM decisions WHERE task_ref=? AND decision=? LIMIT 1",
        (identity["task_ref"], "coordination_dispatch:" + identity["lane_id"]),
    ).fetchone():
        raise CoordinationHold("coordination_dispatch_observed")
    execution_absence(conn, current)
    decision = f"coordination_terminal:{current['id']}:{current['identity_seq']}"
    proof = dict(
        identity=owner_identity(current),
        intent_id=intent["intent_id"],
        reason="declared_zero_dispatch",
        status="not_required",
        state="terminal",
        ok=True,
    )
    prior = conn.execute(
        "SELECT rationale FROM decisions WHERE task_ref=? AND decision=?", (current["task_ref"], decision)
    ).fetchone()
    if prior is None:
        conn.execute(
            "INSERT INTO decisions(task_ref,session,decision,rationale,decision_origin) VALUES (?,?,?,?,'system')",
            (current["task_ref"], "landing-cleanup", decision, json.dumps(proof, sort_keys=True)),
        )
    elif json.loads(prior[0]) != proof:
        raise CoordinationHold("coordination_terminal_identity_conflict")
    return proof


def pre_retire_refusal(task_ref: str, lane_id: str) -> str | None:
    """A changed/unknown declared owner must hold before local resource mutation."""
    from .shared_schema import _get_db_connection

    try:
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (task_ref, lane_id)
            ).fetchone()
            if row is None:
                return "coordination_registry_unknown"
            row = dict(row)
            if declaration(conn, row) is None:
                return None
            execution_absence(conn, row)
            if conn.execute(
                "SELECT 1 FROM decisions WHERE task_ref=? AND decision=? LIMIT 1",
                (task_ref, "coordination_dispatch:" + lane_id),
            ).fetchone():
                return "coordination_dispatch_observed"
            if any(row.get(k) is not None for k in ROUTING):
                return "coordination_execution_routing_present"
        with worker_fence(row):
            probe_execution_process(row)
    except PROOF_ERRORS as exc:
        return str(exc) or "coordination_proof_unknown"
    return None


def retired_adoption_binding(row: dict, proof: dict | None):
    """Reclassify a retired legacy carrier only with exact owner CAS + local custody.

    Does not modify the immutable intent or the original source custody. The
    declaration's source is the current integration, its resource is the retired
    lane materialization named by the native owner row.
    """
    from dataclasses import replace

    from workbay_orchestrator_mcp.lane_reaping import _verified_bundle_contains
    from workbay_orchestrator_mcp.orchestration import landing_cleanup

    from .lane_resource_producer import prepare_lane_binding
    from .runtime import get_runtime_config

    root = Path(get_runtime_config().git_workspace_root)
    if not isinstance(proof, dict) or not isinstance(proof.get("cleanup_intent_id"), str):
        raise CoordinationHold("coordination_retired_adoption_proof_required")
    intent = landing_cleanup.load_intent(root, proof["cleanup_intent_id"])
    if intent is not None:
        intent = landing_cleanup.read_verified_intent(root, identity=intent["identity"])
    if (
        not intent
        or intent.get("local_status") != "complete"
        or intent["identity"]["task_ref"] != row["task_ref"]
        or intent["identity"]["lane_id"] != row["lane_id"]
        or intent["identity"]["expected_tip"] != row["branch_tip_sha"]
        or intent["identity"]["landing_commit"] != row["landing_commit_sha"]
        or any(intent["lane_row"].get(k) != row.get(k) for k in IDENTITY)
    ):
        raise CoordinationHold("coordination_retired_adoption_owner_mismatch")
    local = intent.get("local_result") or {}
    if not local.get("bundle_path") or not _verified_bundle_contains(
        root, Path(local["bundle_path"]), row["branch_tip_sha"]
    ):
        raise CoordinationHold("coordination_retired_adoption_custody_unknown")
    if Path(row["worktree_path"]).exists() or Path(row["worktree_path"]).is_symlink():
        raise CoordinationHold("coordination_local_retirement_pending")
    tip, error = landing_cleanup._commit_for_ref(root, row["branch"], error_prefix="coordination_branch")
    if tip is not None or error != "coordination_branch_absent":
        raise CoordinationHold(error or "coordination_local_retirement_pending")
    branch = subprocess.run(
        ["git", "-C", str(root), "branch", "--show-current"], capture_output=True, text=True, timeout=5, check=True
    ).stdout.strip()
    binding = prepare_lane_binding(str(root), branch)
    if binding.reason or binding.resource is None:
        raise CoordinationHold("coordination_retired_adoption_source_unknown")
    return replace(binding, resource=binding.resource.model_copy(update={"identity": row["worktree_path"]}))


def reserve_native_execution(task_ref: str, lane_id: str) -> None:
    """One SQLite order for native launch vs declaration; I/O follows commit.

    A reservation is an attempted execution, never proof a remote job vanished.
    Declaration cannot race an already-authorized native launch into zero dispatch.
    """
    from .shared_schema import _get_db_connection

    with _get_db_connection(begin_immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (task_ref, lane_id)
        ).fetchone()
        if row is None:
            return
        install_dispatch_observation(conn)
        if declaration(conn, dict(row)) is not None:
            raise CoordinationHold("coordination_execution_forbidden")
        decision = "native_execution_reserved:" + lane_id
        if not conn.execute(
            "SELECT 1 FROM decisions WHERE task_ref=? AND decision=? LIMIT 1", (task_ref, decision)
        ).fetchone():
            conn.execute(
                "INSERT INTO decisions(task_ref,session,decision,rationale,decision_origin) VALUES (?,?,?,?,'system')",
                (task_ref, "native-launch", decision, json.dumps(owner_identity(dict(row)), sort_keys=True)),
            )
