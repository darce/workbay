"""Durable branch-reclaim outcome queue backed by the handoff ledger.

The worktree lifecycle deleter and the reporting-only reclaim scan are producers;
the lane reaper is the consumer.  Queue rows are ordinary system decisions so
this package does not fork the handoff database schema.  A queue identity is
stable for one ``(task_ref, lane_id, authorized_sha)`` tuple: retry outcomes may
refresh that job, while a branch that moved gets a distinct row and cannot
silently inherit deletion authority from the old tip.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import quote

from workbay_handoff_mcp.runtime import get_runtime_config
from workbay_handoff_mcp.shared_schema import connect_handoff_db

_QUEUE_DECISION_PREFIX = "lane_branch_reclaim_queue_"
_CANDIDATE_DECISION_PREFIX = "lane_branch_reclaim_candidate_"
_DRAIN_CURSOR_DECISION_PREFIX = "lane_branch_reclaim_drain_cursor_"
_DRAIN_CURSOR_SESSION = "lane-branch-reclaim-drain-cursor"
_QUEUE_SCHEMA_VERSION = 1
_DRAIN_CURSOR_SCHEMA_VERSION = 1
_FULL_SHA_LENGTH = 40
_ACK_SAVEPOINT = "branch_reclaim_ack"

# These are the exact task_finish outcome tokens.  Do not normalize aliases at
# this boundary: operators and retry policy depend on the original reason.
NON_DELETED_BRANCH_OUTCOMES = frozenset(
    {
        "skipped_active",
        "skipped_unset",
        "skipped_missing",
        "skipped_primary",
        "skipped_checked_out",
        "skipped_unmerged",
        "failed",
    }
)
DEAD_LETTER_REASON = "dead_letter"
DEFAULT_DEAD_LETTER_FAILURES = 3
QUEUE_REASONS = NON_DELETED_BRANCH_OUTCOMES | {"candidate", DEAD_LETTER_REASON}
# skipped_unmerged is a refusal token, never deletion authority.
RETRYABLE_QUEUE_REASONS = frozenset(
    {
        "candidate",
        "skipped_active",
        "skipped_checked_out",
    }
)


@dataclass(frozen=True)
class BranchReclaimQueueItem:
    """One authorized branch tip awaiting collection."""

    task_ref: str
    lane_id: str
    branch: str
    authorized_sha: str
    reason: str
    observed_at: str
    source: str
    row_ids: tuple[int, ...]
    force_authorized: bool
    failure_count: int = 0
    last_error: str = ""

    @property
    def retryable(self) -> bool:
        return self.reason in RETRYABLE_QUEUE_REASONS


def _safe_log(log: Callable[..., Any] | None, level: str, event: str, **fields: Any) -> None:
    if not callable(log):
        return
    try:
        log(level, event, **fields)
    except Exception:  # noqa: BLE001 - queue persistence must not fail through logging
        pass


def _normalize_required_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_sha(value: object) -> str | None:
    normalized = _normalize_required_text(value)
    if normalized is None or len(normalized) != _FULL_SHA_LENGTH:
        return None
    lowered = normalized.lower()
    if any(char not in "0123456789abcdef" for char in lowered):
        return None
    return lowered


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _encode_id_component(value: str) -> str:
    # urllib leaves underscores unescaped even with safe=""; encode them so
    # the human-readable separator cannot introduce tuple collisions.
    return quote(value, safe="").replace("_", "%5F")


def branch_reclaim_queue_decision_id(
    *,
    lane_id: str,
    authorized_sha: str,
    event_id: str | None = None,
) -> str:
    """Return the queue decision prefix, optionally for one immutable event."""

    normalized_lane = _normalize_required_text(lane_id)
    normalized_sha = _normalize_sha(authorized_sha)
    if normalized_lane is None:
        raise ValueError("lane_id must be a non-empty string")
    if normalized_sha is None:
        raise ValueError("authorized_sha must be a full 40-hex SHA")
    base = f"{_QUEUE_DECISION_PREFIX}{_encode_id_component(normalized_lane)}_{normalized_sha}"
    if event_id is None:
        return base
    normalized_event = _normalize_required_text(event_id)
    if normalized_event is None:
        raise ValueError("event_id must be a non-empty string when supplied")
    return f"{base}_{_encode_id_component(normalized_event)}"


def _normalize_failure_count(value: object) -> int:
    try:
        count = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0
    return count if count > 0 else 0


def _queue_input_or_none(
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    authorized_sha: str,
    reason: str,
    observed_at: str | None,
    failure_count: int = 0,
    last_error: str | None = None,
) -> dict[str, str] | None:
    normalized_task = _normalize_required_text(task_ref)
    normalized_lane = _normalize_required_text(lane_id)
    normalized_branch = _normalize_required_text(branch)
    normalized_sha = _normalize_sha(authorized_sha)
    timestamp = _normalize_required_text(observed_at) if observed_at is not None else _utc_timestamp()
    if (
        normalized_task is None
        or normalized_lane is None
        or normalized_branch is None
        or normalized_sha is None
        or reason not in QUEUE_REASONS
        or timestamp is None
    ):
        return None
    payload = {
        "schema_version": str(_QUEUE_SCHEMA_VERSION),
        "task_ref": normalized_task,
        "lane_id": normalized_lane,
        "branch": normalized_branch,
        "sha": normalized_sha,
        "reason": reason,
        "ts": timestamp,
    }
    count = _normalize_failure_count(failure_count)
    if count:
        payload["failure_count"] = str(count)
    normalized_error = _normalize_required_text(last_error) if last_error is not None else None
    if normalized_error is not None:
        payload["last_error"] = normalized_error
    return payload


def _persistable_queue_payload(payload: dict[str, str]) -> dict[str, object]:
    persisted: dict[str, object] = {
        "schema_version": _QUEUE_SCHEMA_VERSION,
        "task_ref": payload["task_ref"],
        "lane_id": payload["lane_id"],
        "branch": payload["branch"],
        "sha": payload["sha"],
        "reason": payload["reason"],
        "ts": payload["ts"],
    }
    count = _normalize_failure_count(payload.get("failure_count"))
    if count:
        persisted["failure_count"] = count
    last_error = payload.get("last_error")
    if last_error:
        persisted["last_error"] = last_error
    return persisted


def _queue_record_accepted(raw: object) -> bool:
    if isinstance(raw, str):
        raw = json.loads(raw)
    mutation = raw.get("mutation") if isinstance(raw, dict) else None
    operation = mutation.get("operation") if isinstance(mutation, dict) else None
    return isinstance(raw, dict) and raw.get("ok") is True and operation in {"insert", "update", "noop"}


def _log_queue_event(
    log: Callable[..., Any] | None,
    event: str,
    payload: dict[str, str],
    **fields: Any,
) -> None:
    _safe_log(
        log,
        "ERROR",
        event,
        task_ref=payload["task_ref"],
        lane=payload["lane_id"],
        branch=payload["branch"],
        sha=payload["sha"],
        reason=payload["reason"],
        **fields,
    )


def _record_queue_decision(payload: dict[str, str], log: Callable[..., Any] | None) -> bool:
    queue_event_id = uuid.uuid4().hex
    decision_id = branch_reclaim_queue_decision_id(
        lane_id=payload["lane_id"],
        authorized_sha=payload["sha"],
        event_id=queue_event_id,
    )
    try:
        from workbay_handoff_mcp import record_decision  # noqa: PLC0415

        raw = record_decision(
            session=f"lane-branch-reclaim-queue-{queue_event_id}",
            decision=decision_id,
            rationale=json.dumps(_persistable_queue_payload(payload), sort_keys=True, separators=(",", ":")),
            actor={"agent": "orchestrator-daemon", "lane_id": payload["lane_id"]},
            task_ref=payload["task_ref"],
            decision_origin="system",
            refresh_rationale_on_conflict=False,
        )
        if _queue_record_accepted(raw):
            return True
        _log_queue_event(log, "branch_reclaim_queue_record_rejected", payload, payload=raw)
        return False
    except Exception as exc:  # noqa: BLE001 - one failed outcome must not abort a reap pass
        _log_queue_event(
            log,
            "branch_reclaim_queue_record_failed",
            payload,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return False


def enqueue_branch_reclaim_outcome(
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    authorized_sha: str,
    reason: str,
    observed_at: str | None = None,
    failure_count: int = 0,
    last_error: str | None = None,
    log: Callable[..., Any] | None = None,
) -> bool:
    """Persist a non-deleted deleter outcome or positive scan candidate.

    The function is a narrow importable seam for producers.  It never raises;
    invalid input and ledger failures return ``False`` and emit a best-effort
    diagnostic.  ``reason`` preserves the task_finish token exactly.
    """

    payload = _queue_input_or_none(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=authorized_sha,
        reason=reason,
        observed_at=observed_at,
        failure_count=failure_count,
        last_error=last_error,
    )
    if payload is None:
        _safe_log(
            log,
            "ERROR",
            "branch_reclaim_queue_input_invalid",
            task_ref=task_ref,
            lane=lane_id,
            branch=branch,
            sha=authorized_sha,
            reason=reason,
        )
        return False
    return _record_queue_decision(payload, log)


def enqueue_branch_reclaim_candidate(
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    authorized_sha: str,
    observed_at: str | None = None,
    log: Callable[..., Any] | None = None,
) -> bool:
    """Persist a positive scan authorization as a retryable queue job."""

    return enqueue_branch_reclaim_outcome(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=authorized_sha,
        reason="candidate",
        observed_at=observed_at,
        log=log,
    )


def _json_object(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _candidate_payload(raw: object) -> dict[str, Any] | None:
    """Decode both bare-JSON and existing reason-prefix + JSON rationales."""

    if not isinstance(raw, str):
        return None
    bare = _json_object(raw)
    if bare is not None:
        return bare
    _prefix, separator, body = raw.partition("\n")
    if not separator:
        return None
    return _json_object(body)


def _queue_item(
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    sha: str,
    reason: str,
    observed_at: str,
    source: str,
    row_id: int,
    force_authorized: bool = False,
    failure_count: int = 0,
    last_error: str = "",
) -> BranchReclaimQueueItem:
    return BranchReclaimQueueItem(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=sha,
        reason=reason,
        observed_at=observed_at,
        source=source,
        row_ids=(row_id,),
        force_authorized=force_authorized,
        failure_count=_normalize_failure_count(failure_count),
        last_error=last_error,
    )


def _outcome_identity(payload: dict[str, Any], row: sqlite3.Row) -> tuple[str, str, str, str, str, str] | None:
    row_task = _normalize_required_text(row["task_ref"])
    row_lane = _normalize_required_text(row["lane_id"])
    if row_task is None or row_lane is None or payload.get("schema_version") != _QUEUE_SCHEMA_VERSION:
        return None
    task_ref = _normalize_required_text(payload.get("task_ref"))
    lane_id = _normalize_required_text(payload.get("lane_id"))
    branch = _normalize_required_text(payload.get("branch"))
    sha = _normalize_sha(payload.get("sha"))
    reason = payload.get("reason")
    observed_at = _normalize_required_text(payload.get("ts"))
    if task_ref != row_task or lane_id != row_lane or branch is None or sha is None:
        return None
    if reason not in QUEUE_REASONS or observed_at is None:
        return None
    expected_prefix = branch_reclaim_queue_decision_id(lane_id=lane_id, authorized_sha=sha) + "_"
    if not str(row["decision"] or "").startswith(expected_prefix):
        return None
    return task_ref, lane_id, branch, sha, str(reason), observed_at


def _decode_outcome_row(row: sqlite3.Row) -> BranchReclaimQueueItem | None:
    payload = _json_object(row["rationale"])
    if payload is None:
        return None
    identity = _outcome_identity(payload, row)
    if identity is None:
        return None
    task_ref, lane_id, branch, sha, reason, observed_at = identity
    return _queue_item(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        sha=sha,
        reason=reason,
        observed_at=observed_at,
        source="outcome",
        row_id=int(row["id"]),
        failure_count=_normalize_failure_count(payload.get("failure_count")),
        last_error=_normalize_required_text(payload.get("last_error")) or "",
    )


def _decode_candidate_row(row: sqlite3.Row) -> BranchReclaimQueueItem | None:
    row_task = _normalize_required_text(row["task_ref"])
    row_lane = _normalize_required_text(row["lane_id"])
    payload = _candidate_payload(row["rationale"])
    observed = payload.get("observed") if isinstance(payload, dict) else None
    if row_task is None or row_lane is None or not isinstance(payload, dict):
        return None
    if payload.get("consumed") is True or payload.get("reclaimable") is not True:
        return None
    if not isinstance(observed, dict):
        return None
    lane_id = _normalize_required_text(observed.get("lane_id"))
    task_ref = _normalize_required_text(observed.get("task_ref"))
    branch = _normalize_required_text(observed.get("branch"))
    sha = _normalize_sha(observed.get("branch_sha"))
    observed_at = _normalize_required_text(payload.get("evaluated_at"))
    if task_ref != row_task or lane_id != row_lane or branch is None or sha is None or observed_at is None:
        return None
    return _queue_item(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        sha=sha,
        reason="candidate",
        observed_at=observed_at,
        source="candidate",
        row_id=int(row["id"]),
    )


def _decode_queue_row(row: sqlite3.Row) -> BranchReclaimQueueItem | None:
    decision = str(row["decision"] or "")
    if decision.startswith(_QUEUE_DECISION_PREFIX):
        return _decode_outcome_row(row)
    if decision.startswith(_CANDIDATE_DECISION_PREFIX):
        return _decode_candidate_row(row)
    return None


def _coalesce_queue_item(
    prior: BranchReclaimQueueItem,
    item: BranchReclaimQueueItem,
) -> BranchReclaimQueueItem:
    chosen = item if item.source == "outcome" else prior
    sources = {prior.source, item.source}
    return BranchReclaimQueueItem(
        task_ref=chosen.task_ref,
        lane_id=chosen.lane_id,
        branch=chosen.branch,
        authorized_sha=chosen.authorized_sha,
        reason=chosen.reason,
        observed_at=chosen.observed_at,
        source=chosen.source,
        row_ids=tuple(sorted((*prior.row_ids, *item.row_ids))),
        force_authorized=(prior.force_authorized or item.force_authorized or sources == {"candidate", "outcome"}),
        failure_count=max(chosen.failure_count, prior.failure_count, item.failure_count),
        last_error=chosen.last_error or item.last_error or prior.last_error,
    )


def list_branch_reclaim_queue_with_conn(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
) -> list[BranchReclaimQueueItem]:
    """Read pending outcome rows and existing positive candidate rows.

    Duplicate producer rows for the same lane tip are coalesced into one item;
    every backing row id remains attached so acknowledgement consumes the
    whole authorization instead of leaking a candidate after its outcome row.
    """

    normalized_task = _normalize_required_text(task_ref) if task_ref is not None else None
    if task_ref is not None and normalized_task is None:
        return []
    where_task = "task_ref = ? AND" if normalized_task is not None else ""
    params: tuple[object, ...] = (
        (normalized_task, f"{_QUEUE_DECISION_PREFIX}*", f"{_CANDIDATE_DECISION_PREFIX}*")
        if normalized_task is not None
        else (f"{_QUEUE_DECISION_PREFIX}*", f"{_CANDIDATE_DECISION_PREFIX}*")
    )
    rows = conn.execute(
        f"""
        SELECT id, task_ref, lane_id, decision, rationale, created_at
        FROM decisions
        WHERE {where_task} (decision GLOB ? OR decision GLOB ?)
        ORDER BY id ASC
        """,
        params,
    ).fetchall()
    coalesced: dict[tuple[str, str, str], BranchReclaimQueueItem] = {}
    for row in rows:
        item = _decode_queue_row(row)
        if item is None:
            continue
        key = (item.task_ref, item.lane_id, item.authorized_sha)
        prior = coalesced.get(key)
        coalesced[key] = item if prior is None else _coalesce_queue_item(prior, item)
    return list(coalesced.values())


def list_branch_reclaim_queue(*, task_ref: str | None = None) -> list[BranchReclaimQueueItem]:
    """Open the configured durable ledger read-only and list queue jobs."""

    db_path = get_runtime_config().db_path
    with closing(connect_handoff_db(db_path, read_only=True)) as conn:
        return list_branch_reclaim_queue_with_conn(conn, task_ref=task_ref)


def _mark_candidate_consumed(raw: str) -> str | None:
    payload = _candidate_payload(raw)
    if payload is None:
        return None
    marked = dict(payload)
    marked["consumed"] = True
    body = json.dumps(marked, sort_keys=True, separators=(",", ":"))
    prefix, separator, _rest = raw.partition("\n")
    if separator and _json_object(raw) is None:
        return f"{prefix}\n{body}"
    return body


def _ack_one_row(conn: sqlite3.Connection, row_id: int, rationale: str, decision: str) -> bool:
    if decision.startswith(_CANDIDATE_DECISION_PREFIX):
        consumed = _mark_candidate_consumed(rationale)
        if consumed is None:
            return False
        cursor = conn.execute(
            "UPDATE decisions SET rationale = ? WHERE id = ? AND rationale = ?",
            (consumed, row_id, rationale),
        )
    else:
        cursor = conn.execute(
            "DELETE FROM decisions WHERE id = ? AND rationale = ?",
            (row_id, rationale),
        )
    return cursor.rowcount == 1


def _load_ack_rows(
    conn: sqlite3.Connection,
    item: BranchReclaimQueueItem,
    normalized_sha: str,
) -> list[tuple[int, str, str]] | None:
    stored: list[tuple[int, str, str]] = []
    for row_id in item.row_ids:
        row = conn.execute(
            """
            SELECT id, task_ref, lane_id, decision, rationale, created_at
            FROM decisions WHERE id = ?
            """,
            (row_id,),
        ).fetchone()
        if row is None:
            return None
        decoded = _decode_queue_row(row)
        if (
            decoded is None
            or decoded.task_ref != item.task_ref
            or decoded.lane_id != item.lane_id
            or decoded.authorized_sha != normalized_sha
        ):
            return None
        stored.append((row_id, str(row["rationale"] or ""), str(row["decision"] or "")))
    return stored


def _rollback_ack_savepoint(conn: sqlite3.Connection) -> None:
    conn.execute(f"ROLLBACK TO SAVEPOINT {_ACK_SAVEPOINT}")
    conn.execute(f"RELEASE SAVEPOINT {_ACK_SAVEPOINT}")


def acknowledge_branch_reclaim_item(
    conn: sqlite3.Connection,
    *,
    item: BranchReclaimQueueItem,
    authorized_sha: str,
) -> bool:
    """Consume an item with a connection-local SHA-bound compare-and-delete.

    The caller owns the surrounding transaction and can reconcile the lane row
    before commit.  All backing rows are decoded and checked before any mutate;
    a changed, malformed, missing, or differently authorized row refuses with
    no partial acknowledgement.  Candidate audit rows are marked consumed
    rather than deleted.
    """

    normalized_sha = _normalize_sha(authorized_sha)
    if normalized_sha is None or normalized_sha != item.authorized_sha or not item.row_ids:
        return False
    stored = _load_ack_rows(conn, item, normalized_sha)
    if stored is None:
        return False
    conn.execute(f"SAVEPOINT {_ACK_SAVEPOINT}")
    try:
        for row_id, rationale, decision in stored:
            if not _ack_one_row(conn, row_id, rationale, decision):
                _rollback_ack_savepoint(conn)
                return False
        conn.execute(f"RELEASE SAVEPOINT {_ACK_SAVEPOINT}")
        return True
    except Exception:  # noqa: BLE001 - refuse rather than commit a partial ack
        _rollback_ack_savepoint(conn)
        return False


def queue_item_is_drainable(item: BranchReclaimQueueItem) -> bool:
    """Retryable jobs drain; coalesced jobs drain only when force-authorized."""

    if item.reason == DEAD_LETTER_REASON:
        return False
    return bool(item.force_authorized or item.retryable)


def queue_item_cursor_id(item: BranchReclaimQueueItem) -> int:
    """Stable visit position for a coalesced job: the newest backing row id."""

    return max(item.row_ids) if item.row_ids else 0


def _drain_cursor_task_ref(task_ref: str | None) -> str:
    return _normalize_required_text(task_ref) or "*"


def branch_reclaim_drain_cursor_decision_id(task_ref: str | None) -> str:
    """Return the durable drain-cursor decision id for one task."""

    return f"{_DRAIN_CURSOR_DECISION_PREFIX}{_encode_id_component(_drain_cursor_task_ref(task_ref))}"


def load_branch_reclaim_drain_cursor(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
) -> int:
    """Return the last examined queue id for ``task_ref``, or 0 if none."""

    key = _drain_cursor_task_ref(task_ref)
    row = conn.execute(
        """
        SELECT rationale FROM decisions
        WHERE task_ref = ? AND decision = ? AND session = ?
        """,
        (key, branch_reclaim_drain_cursor_decision_id(key), _DRAIN_CURSOR_SESSION),
    ).fetchone()
    if row is None:
        return 0
    payload = _json_object(row["rationale"])
    if payload is None:
        return 0
    try:
        last_id = int(payload.get("last_id", 0))
    except (TypeError, ValueError):
        return 0
    return last_id if last_id > 0 else 0


def store_branch_reclaim_drain_cursor(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None,
    last_id: int,
) -> None:
    """Persist the last examined queue id in the same ledger as queue rows."""

    key = _drain_cursor_task_ref(task_ref)
    try:
        cursor_id = int(last_id)
    except (TypeError, ValueError):
        cursor_id = 0
    if cursor_id < 0:
        cursor_id = 0
    payload = json.dumps(
        {
            "schema_version": _DRAIN_CURSOR_SCHEMA_VERSION,
            "task_ref": key,
            "last_id": cursor_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO decisions (
            task_ref, session, decision, rationale, decision_origin, created_at
        )
        VALUES (?, ?, ?, ?, 'system', datetime('now'))
        ON CONFLICT(task_ref, decision, session)
        DO UPDATE SET rationale = excluded.rationale
        """,
        (key, _DRAIN_CURSOR_SESSION, branch_reclaim_drain_cursor_decision_id(key), payload),
    )


def record_branch_reclaim_failure(
    *,
    item: BranchReclaimQueueItem,
    error: str,
    log: Callable[..., Any] | None = None,
) -> bool:
    """Persist a consecutive failure; dead-letter after the bound is reached."""

    next_count = _normalize_failure_count(item.failure_count) + 1
    last_error = _normalize_required_text(error) or "failed"
    reason = DEAD_LETTER_REASON if next_count >= DEFAULT_DEAD_LETTER_FAILURES else item.reason
    if reason not in QUEUE_REASONS:
        reason = DEAD_LETTER_REASON
    return enqueue_branch_reclaim_outcome(
        task_ref=item.task_ref,
        lane_id=item.lane_id,
        branch=item.branch,
        authorized_sha=item.authorized_sha,
        reason=reason,
        failure_count=next_count,
        last_error=last_error,
        log=log,
    )


__all__ = [
    "BranchReclaimQueueItem",
    "DEAD_LETTER_REASON",
    "DEFAULT_DEAD_LETTER_FAILURES",
    "NON_DELETED_BRANCH_OUTCOMES",
    "QUEUE_REASONS",
    "RETRYABLE_QUEUE_REASONS",
    "acknowledge_branch_reclaim_item",
    "branch_reclaim_drain_cursor_decision_id",
    "branch_reclaim_queue_decision_id",
    "enqueue_branch_reclaim_candidate",
    "enqueue_branch_reclaim_outcome",
    "list_branch_reclaim_queue",
    "list_branch_reclaim_queue_with_conn",
    "load_branch_reclaim_drain_cursor",
    "queue_item_cursor_id",
    "queue_item_is_drainable",
    "record_branch_reclaim_failure",
    "store_branch_reclaim_drain_cursor",
]
