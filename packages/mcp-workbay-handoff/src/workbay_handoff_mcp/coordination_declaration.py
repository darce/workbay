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
from functools import wraps
from pathlib import Path
from typing import Any, Literal

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
# DB-wide observation epochs are immutable system facts outside any task. Each
# one records where uninterrupted trigger coverage (re)began; rows that existed
# before it may have dispatch history that the triggers never saw.
EPOCH_TASK = "__workbay_coordination_observation__"
EPOCH_SESSION = "native-dispatch"
EPOCH_PREFIX = "coordination_observation_epoch:"
EPOCH_SCHEMA = "workbay/coordination-observation-epoch@1"


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


def observation_triggers() -> dict[str, str]:
    """Exact native trigger set; stored SQL must match byte-for-byte to count as coverage."""
    triggers: dict[str, str] = {}
    for table in OBSERVED_TABLES:
        for event in ("INSERT", "UPDATE"):
            name = f"coordination_observe_{table}_{event.lower()}"
            triggers[name] = f"""CREATE TRIGGER {name}
            BEFORE {event} ON {table}
            BEGIN
              SELECT CASE WHEN EXISTS(SELECT 1 FROM decisions d JOIN worktree_lanes l
                ON l.task_ref=NEW.task_ref AND l.lane_id=NEW.lane_id
                WHERE d.task_ref=l.task_ref AND d.decision='coordination_terminal:' || l.id || ':' || l.identity_seq)
                THEN RAISE(ABORT,'coordination_terminal_execution_forbidden') END;
              INSERT INTO decisions(task_ref,session,decision,rationale,decision_origin)
                SELECT NEW.task_ref,'native-dispatch','coordination_dispatch:' || NEW.lane_id,
                       '{table}','system' WHERE NEW.lane_id IS NOT NULL AND NOT EXISTS(
                         SELECT 1 FROM decisions WHERE task_ref=NEW.task_ref
                           AND decision='coordination_dispatch:' || NEW.lane_id);
            END"""
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
    installed = {
        name: sql
        for name, sql in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND name IN (%s)" % ",".join("?" * len(expected)),
            tuple(expected),
        )
    }
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
    if not stale and _epoch_bound(conn) is not None:
        return
    for name in stale:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        conn.execute(expected[name])
    # One statement under the writer lock: the high-water and the epoch commit together.
    conn.execute(
        """INSERT INTO decisions(task_ref,session,decision,rationale,decision_origin)
        SELECT ?, ?, ? || (SELECT COUNT(*) + 1 FROM decisions WHERE task_ref=? AND session=? AND decision GLOB ?),
          json_object(
            'schema', ?,
            'lanes_high_water', MAX(COALESCE((SELECT seq FROM sqlite_sequence WHERE name='worktree_lanes'), 0),
                                    COALESCE((SELECT MAX(id) FROM worktree_lanes), 0)),
            'lane_messages_high_water', MAX(COALESCE((SELECT seq FROM sqlite_sequence WHERE name='lane_messages'), 0),
                                            COALESCE((SELECT MAX(id) FROM lane_messages), 0)),
            'replaced_triggers', json(?)),
          'system'""",
        (
            EPOCH_TASK,
            EPOCH_SESSION,
            EPOCH_PREFIX,
            EPOCH_TASK,
            EPOCH_SESSION,
            EPOCH_PREFIX + "*",
            EPOCH_SCHEMA,
            json.dumps(stale),
        ),
    )


def observation_coverage(conn: sqlite3.Connection, row: dict[str, Any], *, pending_insert: bool = False) -> None:
    """Prove the row's whole life is inside one uninterrupted observation epoch.

    Holds as unknown when coverage is currently broken, no epoch exists, or the
    row (by AUTOINCREMENT id or creation time) predates the latest epoch. A row
    about to be inserted in this same transaction (``pending_insert``) is new
    only when coverage is already intact before its insert.
    """
    if _stale_triggers(conn, observation_triggers()):
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
