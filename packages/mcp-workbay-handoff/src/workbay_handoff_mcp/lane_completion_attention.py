"""Durable completion attention, independent of storage actuators.

The caller supplies the evaluator's content-addressed assessment receipt in the
ratified design envelope. This module checks DB authority, not completion/Git
predicates. ``operation_intent`` is a stable caller operation key (including any
operation-specific target identity); replays must reuse it. No session read is
an acknowledgement. Completed receipts can be journaled through the same API
and referenced by explicit discharge; candidate completion cannot discharge.

``identity_revision`` is the lane's identity_seq as a string, or its contract
lane_revision for legacy rows without a sequence. Authorizations bind DB
obligations only: future adapters must also fence live identity, tips, custody
and the actual storage mutation. Replaying an outcome returns a receipt, never
new permission to run an actuator.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Literal, TypedDict

from .lane_completion_recording import ContractRefusal, _decode, _latest, canonical_hash, canonical_json
from .shared_schema import _get_db_connection


class CompletionRefusalCode(str, Enum):
    RECEIPT_FAILED = "completion_receipt_failed"
    AUTHORITY_STALE = "completion_authority_stale"
    EVENT_INVALID = "completion_event_invalid"


class CompletionRefusal(ValueError):
    """Every refusal holds storage; a failed transaction yields no authorization."""

    hold = True

    def __init__(self, code: CompletionRefusalCode, detail: str):
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class CompletionReceipt(TypedDict):
    assessment_id: str
    task_ref: str
    lane_id: str
    lane_row_id: int
    identity_revision: str
    contract_revision: int
    contract_hash: str
    phase: Literal["candidate", "integration"]
    verdict: Literal["completed", "superseded", "unfinished_never_produced", "unfinished_partial", "unknown"]
    reason_codes: list[str]
    unmet_finding_ids: list[int | str]
    unmet_acceptance_ids: list[str]
    missing_evidence: list[str]
    carrier: dict | None
    policy_action: str


class Reassignment(TypedDict):
    task_ref: str
    lane_id: str
    identity_revision: str
    next_action: str


@dataclass(frozen=True)
class CompletionAuthorization:
    event_id: str
    assessment_id: str
    obligation_fingerprint: str
    contract_hash: str


@dataclass(frozen=True)
class CompletionEvent:
    event_id: str
    assessment_id: str
    state: str
    storage_outcome: str
    resolution: dict | None


class CompletionAttention(TypedDict):
    count: int
    cursor: str | None
    events: list[dict]


def _require(condition: bool, detail: str, *, stale: bool = False) -> None:
    if not condition:
        code = CompletionRefusalCode.AUTHORITY_STALE if stale else CompletionRefusalCode.EVENT_INVALID
        raise CompletionRefusal(code, detail)


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    # The exception boundary includes opening, committing and closing. In
    # particular, never return an authorization from a failed COMMIT.
    try:
        with _get_db_connection(begin_immediate=True) as conn:
            yield conn
    except CompletionRefusal:
        raise
    except ContractRefusal as exc:
        raise CompletionRefusal(CompletionRefusalCode.AUTHORITY_STALE, str(exc)) from exc
    except Exception as exc:
        raise CompletionRefusal(
            CompletionRefusalCode.RECEIPT_FAILED, "warning journal transaction failed; hold"
        ) from exc


def _contract(conn: sqlite3.Connection, task_ref: str, lane_id: str):
    row = _latest(conn, task_ref, lane_id)
    _require(row is not None, "latest contract is unavailable", stale=True)
    return _decode(row)


def _fingerprint(conn: sqlite3.Connection, task_ref: str, lane_id: str) -> str:
    record = _contract(conn, task_ref, lane_id)
    findings = []
    for assignment in record.assigned_findings:
        rows = conn.execute(
            "SELECT task_ref, finding_id, status, updated_at, reopen_count FROM review_findings WHERE id=? "
            "UNION ALL SELECT task_ref, finding_id, status, updated_at, reopen_count "
            "FROM review_findings_archive WHERE id=? ORDER BY status, updated_at, reopen_count",
            (assignment["db_id"], assignment["db_id"]),
        ).fetchall()
        _require(
            all(r["task_ref"] == task_ref and r["finding_id"] == assignment["finding_id"] for r in rows),
            "assigned finding identity changed",
            stale=True,
        )
        # Empty rows are explicit missing evidence, never an empty assignment.
        # reopen_count detects a close/reopen/close within one timestamp tick.
        findings.append({"assignment": assignment, "rows": [dict(r) for r in rows]})
    newest_test = conn.execute("SELECT MAX(id) FROM verified_tests WHERE task_ref=?", (task_ref,)).fetchone()[0]
    return canonical_hash(
        {
            "contract_revision": record.revision,
            "contract_hash": record.contract_hash,
            "findings": findings,
            "newest_verified_test_id": newest_test,
        }
    )


def get_obligation_fingerprint(task_ref: str, lane_id: str) -> str:
    """Read one consistent snapshot; prepare recomputes it under its write lock."""
    with _get_db_connection() as conn:
        conn.execute("BEGIN")
        return _fingerprint(conn, task_ref, lane_id)


def _validate_receipt(receipt: CompletionReceipt) -> dict:
    _require(isinstance(receipt, dict), "receipt must be an object")
    _require(CompletionReceipt.__annotations__.keys() <= receipt.keys(), "incomplete assessment receipt")
    for key in ("assessment_id", "task_ref", "lane_id", "identity_revision", "contract_hash", "policy_action"):
        _require(_text(receipt[key]), f"missing {key}")
    for key in ("lane_row_id", "contract_revision"):
        _require(type(receipt[key]) is int and receipt[key] > 0, f"invalid {key}")
    _require(receipt["phase"] in ("candidate", "integration"), "unknown phase")
    _require(
        receipt["verdict"] in ("completed", "superseded", "unfinished_never_produced", "unfinished_partial", "unknown"),
        "unknown verdict",
    )
    for key in ("reason_codes", "unmet_finding_ids", "unmet_acceptance_ids", "missing_evidence"):
        values = receipt[key]
        _require(isinstance(values, (list, tuple)), f"invalid {key}")
        _require(
            all(_text(v) or (key == "unmet_finding_ids" and type(v) is int and v > 0) for v in values),
            f"invalid {key} item",
        )
    _require(receipt["carrier"] is None or isinstance(receipt["carrier"], dict), "invalid carrier")
    if receipt["verdict"] == "completed":
        _require(
            not any(receipt[k] for k in ("unmet_finding_ids", "unmet_acceptance_ids", "missing_evidence")),
            "completed receipt has unmet evidence",
        )
    # Detach caller-owned lists/dicts, preserving additional evidence/hold fields.
    return json.loads(canonical_json(receipt))


def _authorization(row: sqlite3.Row) -> CompletionAuthorization:
    return CompletionAuthorization(
        *(row[k] for k in ("event_id", "assessment_id", "obligation_fingerprint", "contract_hash"))
    )


def _event(row: sqlite3.Row) -> CompletionEvent:
    return CompletionEvent(
        row["event_id"],
        row["assessment_id"],
        row["state"],
        row["storage_outcome"],
        json.loads(row["resolution_json"]) if row["resolution_json"] else None,
    )


def _event_row(conn: sqlite3.Connection, event_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM lane_completion_events WHERE event_id=?", (event_id,)).fetchone()
    _require(row is not None, "completion event unavailable")
    return row


def prepare_completion_warning(
    receipt: CompletionReceipt,
    *,
    operation_intent: str,
    obligation_fingerprint: str,
    next_action: str,
    observer: str,
) -> CompletionAuthorization:
    """Commit intent, system decision and task preservation in one transaction.

    Idempotent by (assessment_id, operation_intent). A replay returns the stored
    authorization even after a contract change; it cannot authorize a new
    outcome without passing that outcome's fresh fingerprint check.
    """
    receipt = _validate_receipt(receipt)
    _require(
        all(_text(v) for v in (operation_intent, obligation_fingerprint, next_action, observer)),
        "operation, fingerprint, next action and observer required",
    )
    with _transaction() as conn:
        existing = conn.execute(
            "SELECT * FROM lane_completion_events WHERE assessment_id=? AND operation_intent=?",
            (receipt["assessment_id"], operation_intent),
        ).fetchone()
        if existing is not None:
            _require(
                existing["receipt_json"] == canonical_json(receipt) and existing["next_action"] == next_action,
                "operation replay conflicts with its durable receipt",
            )
            return _authorization(existing)
        record = _contract(conn, receipt["task_ref"], receipt["lane_id"])
        identity = record.contract["identity"]
        lane = conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (receipt["lane_row_id"],)).fetchone()
        revision = str(identity["identity_seq"]) if identity["identity_seq"] is not None else identity["lane_revision"]
        _require(
            record.revision == receipt["contract_revision"]
            and record.contract_hash == receipt["contract_hash"]
            and identity["lane_row_id"] == receipt["lane_row_id"]
            and revision == receipt["identity_revision"]
            and lane is not None
            and all(lane[k] == identity[k] for k in ("task_ref", "lane_id", "identity_seq", "branch")),
            "contract or lane identity changed",
            stale=True,
        )
        _require(
            _fingerprint(conn, receipt["task_ref"], receipt["lane_id"]) == obligation_fingerprint,
            "obligations changed before prepare",
            stale=True,
        )
        task = conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (receipt["task_ref"],)).fetchone()
        _require(task is not None, "task unavailable", stale=True)
        event_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO lane_completion_events (event_id, assessment_id, operation_intent, task_ref, lane_id, "
            "lane_row_id, identity_revision, contract_revision, contract_hash, phase, verdict, reason_codes_json, "
            "unmet_finding_ids_json, unmet_acceptance_ids_json, missing_evidence_json, carrier_json, policy_action, "
            "obligation_fingerprint, state, next_action, observer, receipt_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?)",
            (
                event_id,
                receipt["assessment_id"],
                operation_intent,
                receipt["task_ref"],
                receipt["lane_id"],
                receipt["lane_row_id"],
                receipt["identity_revision"],
                record.revision,
                record.contract_hash,
                receipt["phase"],
                receipt["verdict"],
                canonical_json(receipt["reason_codes"]),
                canonical_json(receipt["unmet_finding_ids"]),
                canonical_json(receipt["unmet_acceptance_ids"]),
                canonical_json(receipt["missing_evidence"]),
                canonical_json(receipt["carrier"]),
                receipt["policy_action"],
                obligation_fingerprint,
                next_action,
                observer,
                canonical_json(receipt),
            ),
        )
        owned_findings = {v for f in record.assigned_findings for v in (f["db_id"], f["finding_id"])}
        owned_unmet = bool(
            owned_findings.intersection(receipt["unmet_finding_ids"])
            or set(record.contract["acceptance_ids"]).intersection(receipt["unmet_acceptance_ids"])
        )
        if (
            receipt["verdict"] in ("unfinished_never_produced", "unfinished_partial")
            and task["status"] == "done"
            and owned_unmet
        ):
            conn.execute(
                "UPDATE handoff_state SET status='in_progress', revision=revision+1, updated_at=datetime('now'), "
                "updated_by=? WHERE task_ref=? AND status='done'",
                (observer, receipt["task_ref"]),
            )
        conn.execute(
            "INSERT INTO decisions(task_ref, lane_id, session, decision, rationale, decision_origin) VALUES (?, ?, ?, ?, ?, 'system')",
            (
                receipt["task_ref"],
                receipt["lane_id"],
                observer,
                f"Completion attention {event_id}: {receipt['verdict']}; unmet findings={canonical_json(receipt['unmet_finding_ids'])}; "
                f"unmet acceptance={canonical_json(receipt['unmet_acceptance_ids'])}; next={next_action}",
                f"assessment={receipt['assessment_id']}; operation={operation_intent}; policy={receipt['policy_action']}",
            ),
        )
        authorization = _authorization(_event_row(conn, event_id))
    return authorization


def record_completion_outcome(
    event_id: str,
    outcome: Literal["storage_removed", "held"],
    authorization: CompletionAuthorization,
) -> CompletionEvent:
    """Reconcile an operation after a crash; never acknowledge its obligations."""
    _require(outcome in ("storage_removed", "held"), "invalid storage outcome")
    with _transaction() as conn:
        row = _event_row(conn, event_id)
        _require(
            isinstance(authorization, CompletionAuthorization) and authorization == _authorization(row),
            "authorization does not match event",
            stale=True,
        )
        if row["storage_outcome"] == outcome:
            return _event(row)
        _require(row["state"] == "prepared", "outcome conflicts with recorded operation")
        if outcome == "storage_removed":
            _require(row["verdict"] != "unknown", "unknown completion must hold")
            _require(
                _fingerprint(conn, row["task_ref"], row["lane_id"]) == row["obligation_fingerprint"],
                "obligations changed before storage outcome",
                stale=True,
            )
        conn.execute(
            "UPDATE lane_completion_events SET state=?, storage_outcome=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE event_id=?",
            (outcome, outcome, event_id),
        )
        event = _event(_event_row(conn, event_id))
    return event


def _resolve(conn: sqlite3.Connection, row: sqlite3.Row, state: str, resolution: dict) -> CompletionEvent:
    if row["state"] in ("discharged", "reassigned"):
        _require(
            row["state"] == state and row["resolution_json"] == canonical_json(resolution),
            "conflicting event resolution",
        )
        return _event(row)
    conn.execute(
        "UPDATE lane_completion_events SET state=?, resolution_json=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE event_id=?",
        (state, canonical_json(resolution), row["event_id"]),
    )
    return _event(_event_row(conn, row["event_id"]))


def discharge_completion_event(event_id: str, assessment_id_of_newer_completed: str) -> CompletionEvent:
    """Explicitly discharge against a newer, current integration completion.

    The adapter is responsible for evaluating/verifying the completed receipt
    before publication. A bare ID or a candidate-phase receipt is insufficient.
    """
    resolution = {"assessment_id": assessment_id_of_newer_completed}
    with _transaction() as conn:
        row = _event_row(conn, event_id)
        if row["state"] == "discharged":
            return _resolve(conn, row, "discharged", resolution)
        completed = conn.execute(
            "SELECT * FROM lane_completion_events WHERE assessment_id=? "
            "AND task_ref=? AND lane_id=? AND lane_row_id=? AND identity_revision=? AND verdict='completed' "
            "AND phase='integration' AND event_sequence>? ORDER BY event_sequence DESC LIMIT 1",
            (
                assessment_id_of_newer_completed,
                row["task_ref"],
                row["lane_id"],
                row["lane_row_id"],
                row["identity_revision"],
                row["event_sequence"],
            ),
        ).fetchone()
        _require(
            completed is not None and completed["assessment_id"] != row["assessment_id"],
            "newer completed integration assessment required",
        )
        _require(
            completed["obligation_fingerprint"] == _fingerprint(conn, row["task_ref"], row["lane_id"]),
            "completed assessment is stale",
            stale=True,
        )
        event = _resolve(conn, row, "discharged", resolution)
    return event


def reassign(event_id: str, *, carrier: Reassignment, authority_id: str) -> CompletionEvent:
    """Record explicit owner reassignment, retaining the original receipt/history."""
    _require(
        isinstance(carrier, dict)
        and all(_text(carrier.get(k)) for k in Reassignment.__annotations__)
        and _text(authority_id),
        "named carrier, next action and owner authority required",
    )
    resolution = {"carrier": carrier, "authority_id": authority_id}
    with _transaction() as conn:
        row = _event_row(conn, event_id)
        _require(
            (carrier["task_ref"], carrier["lane_id"]) != (row["task_ref"], row["lane_id"]),
            "reassignment requires another carrier",
        )
        event = _resolve(conn, row, "reassigned", resolution)
    return event


def read_completion_attention(
    task_ref: str | None, *, limit: int = 20, cursor: str | None = None
) -> CompletionAttention:
    """Bounded pending inbox; cursor is the last returned event sequence.

    ``limit=0`` returns the mandatory count and an initial cursor, allowing a
    budgeted load_session to omit details without acknowledging any event.
    Call with that cursor and a positive limit to recover the details.
    """
    _require(type(limit) is int and 0 <= limit <= 100, "attention limit must be between 0 and 100")
    _require(cursor is None or (isinstance(cursor, str) and cursor.isdecimal()), "invalid attention cursor")
    if task_ref is None:
        return {"count": 0, "cursor": None, "events": []}
    after = int(cursor) if cursor is not None else 0
    with _get_db_connection() as conn:
        conn.execute("BEGIN")
        pending = "task_ref=? AND state IN ('prepared', 'storage_removed', 'held')"
        count = conn.execute(f"SELECT COUNT(*) FROM lane_completion_events WHERE {pending}", (task_ref,)).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM lane_completion_events WHERE {pending} AND event_sequence>? ORDER BY event_sequence LIMIT ?",
            (task_ref, after, limit + 1),
        ).fetchall()
        events = []
        for row in rows[:limit]:
            events.append(
                {
                    **{
                        k: row[k]
                        for k in (
                            "event_id",
                            "assessment_id",
                            "task_ref",
                            "lane_id",
                            "lane_row_id",
                            "identity_revision",
                            "phase",
                            "verdict",
                            "policy_action",
                            "storage_outcome",
                            "next_action",
                        )
                    },
                    **{
                        k: json.loads(row[f"{k}_json"])
                        for k in (
                            "reason_codes",
                            "unmet_finding_ids",
                            "unmet_acceptance_ids",
                            "missing_evidence",
                            "carrier",
                        )
                    },
                }
            )
        next_cursor = (
            str(rows[limit - 1]["event_sequence"])
            if limit and len(rows) > limit
            else (str(after) if rows and limit == 0 else None)
        )
    return {"count": count, "cursor": next_cursor, "events": events}
