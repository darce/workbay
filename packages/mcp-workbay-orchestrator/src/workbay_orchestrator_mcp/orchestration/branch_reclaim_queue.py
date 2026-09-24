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
_DEAD_LETTER_RELEASE_DECISION_PREFIX = "lane_branch_reclaim_dead_letter_release_"
_DRAIN_CURSOR_SESSION = "lane-branch-reclaim-drain-cursor"
_DEAD_LETTER_RELEASE_SESSION = "lane-branch-reclaim-dead-letter-release"
_QUEUE_SESSION = "lane-branch-reclaim-queue"
_QUEUE_SCHEMA_VERSION = 2
_DRAIN_CURSOR_SCHEMA_VERSION = 1
_FULL_SHA_LENGTH = 40
_ACK_SAVEPOINT = "branch_reclaim_ack"
_RELEASE_SAVEPOINT = "branch_reclaim_dead_letter_release"

# These are exact producer tokens. Do not normalize aliases at this boundary:
# operators and retry policy depend on the original reason. In particular,
# ``skipped_identity_unrecorded`` must never be laundered into ``candidate``.
TYPED_REFUSAL_QUEUE_REASONS = frozenset(
    {
        "skipped_active",
        "skipped_unset",
        "skipped_missing",
        "skipped_primary",
        "skipped_checked_out",
        "skipped_unmerged",
        "skipped_identity_unrecorded",
        "failed",
    }
)
RETRYABLE_BRANCH_OUTCOMES = frozenset({"worktree_claim_busy", "terminal_lane"})
NON_DELETED_BRANCH_OUTCOMES = TYPED_REFUSAL_QUEUE_REASONS | RETRYABLE_BRANCH_OUTCOMES
DEAD_LETTER_REASON = "dead_letter"
DEFAULT_DEAD_LETTER_FAILURES = 3
QUEUE_OUTCOMES = NON_DELETED_BRANCH_OUTCOMES | {"candidate"}
# Compatibility exports for callers which still name the old overloaded field.
QUEUE_REASONS = QUEUE_OUTCOMES | {DEAD_LETTER_REASON}
RETRYABLE_QUEUE_REASONS = RETRYABLE_BRANCH_OUTCOMES | {"candidate"}

DRAINABILITY_RETRYABLE = "retryable"
DRAINABILITY_TERMINAL = "terminal"
QUEUE_STATE_PENDING = "pending"
QUEUE_STATE_DEAD_LETTER = "dead_letter"

PRODUCER_REPORTING_SCAN = "reporting_scan"
PRODUCER_MERGED_REGISTRY = "merged_registry"
PRODUCER_WORKTREE_DELETER = "worktree_deleter"
PRODUCER_QUEUE_CONSUMER = "queue_consumer"
PRODUCER_TERMINAL_LANE = "terminal_lane"
QUEUE_PRODUCERS = frozenset(
    {
        PRODUCER_REPORTING_SCAN,
        PRODUCER_MERGED_REGISTRY,
        PRODUCER_WORKTREE_DELETER,
        PRODUCER_QUEUE_CONSUMER,
        PRODUCER_TERMINAL_LANE,
    }
)
_PRODUCER_AUTHORITY = {
    PRODUCER_REPORTING_SCAN: 10,
    PRODUCER_MERGED_REGISTRY: 20,
    PRODUCER_WORKTREE_DELETER: 30,
    PRODUCER_TERMINAL_LANE: 35,
    PRODUCER_QUEUE_CONSUMER: 40,
}
_ABSOLUTE_VETO_OUTCOMES = frozenset({"skipped_identity_unrecorded"})
_RECOVERABLE_TRANSIENT_OUTCOMES = frozenset({"skipped_checked_out"})


@dataclass(frozen=True)
class BranchReclaimQueueItem:
    """One branch identity with independently typed queue facts."""

    task_ref: str
    lane_id: str
    branch: str
    authorized_sha: str
    outcome: str
    drainability: str
    queue_state: str
    observed_at: str
    producer: str
    row_ids: tuple[int, ...]
    force_authorized: bool
    force_authorization_provenance: str | None = None
    failure_count: int = 0
    last_error: str = ""

    @property
    def retryable(self) -> bool:
        return self.drainability == DRAINABILITY_RETRYABLE

    @property
    def reason(self) -> str:
        """Compatibility view for the unchanged drain/reporting surfaces."""

        if self.queue_state == QUEUE_STATE_DEAD_LETTER:
            return DEAD_LETTER_REASON
        return self.outcome

    @property
    def source(self) -> str:
        """Compatibility name for producer provenance."""

        return "candidate" if self.producer == PRODUCER_REPORTING_SCAN else "outcome"


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


def _outcome_drainability(outcome: str) -> str:
    return DRAINABILITY_RETRYABLE if outcome in RETRYABLE_QUEUE_REASONS else DRAINABILITY_TERMINAL


def _queue_input_or_none(
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    authorized_sha: str,
    outcome: str,
    producer: str,
    drainability: str | None,
    queue_state: str,
    force_authorization_provenance: str | None,
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
        or outcome not in QUEUE_OUTCOMES
        or producer not in QUEUE_PRODUCERS
        or queue_state not in {QUEUE_STATE_PENDING, QUEUE_STATE_DEAD_LETTER}
        or timestamp is None
    ):
        return None
    resolved_drainability = drainability or _outcome_drainability(outcome)
    if resolved_drainability not in {DRAINABILITY_RETRYABLE, DRAINABILITY_TERMINAL}:
        return None
    if queue_state == QUEUE_STATE_DEAD_LETTER:
        resolved_drainability = DRAINABILITY_TERMINAL
    authorization = _normalize_required_text(force_authorization_provenance)
    payload = {
        "schema_version": str(_QUEUE_SCHEMA_VERSION),
        "task_ref": normalized_task,
        "lane_id": normalized_lane,
        "branch": normalized_branch,
        "sha": normalized_sha,
        "outcome": outcome,
        "drainability": resolved_drainability,
        "queue_state": queue_state,
        "producer": producer,
        "ts": timestamp,
    }
    if authorization is not None:
        payload["force_authorization_provenance"] = authorization
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
        "outcome": payload["outcome"],
        "drainability": payload["drainability"],
        "queue_state": payload["queue_state"],
        "producer": payload["producer"],
        "ts": payload["ts"],
    }
    authorization = payload.get("force_authorization_provenance")
    if authorization:
        persisted["force_authorization_provenance"] = authorization
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
        outcome=payload["outcome"],
        drainability=payload["drainability"],
        queue_state=payload["queue_state"],
        producer=payload["producer"],
        **fields,
    )


def _record_queue_decision(payload: dict[str, str], log: Callable[..., Any] | None) -> bool:
    # One stable row per producer fact bounds ledger growth. A dead-letter has
    # its own absorbing state marker, so ordinary producer refreshes cannot
    # overwrite or reactivate the shed.
    event = (
        f"state:{QUEUE_STATE_DEAD_LETTER}"
        if payload["queue_state"] == QUEUE_STATE_DEAD_LETTER
        else f"outcome:{payload['producer']}:{payload['outcome']}"
    )
    decision_id = branch_reclaim_queue_decision_id(
        lane_id=payload["lane_id"],
        authorized_sha=payload["sha"],
        event_id=event,
    )
    try:
        from workbay_handoff_mcp import record_decision  # noqa: PLC0415

        raw = record_decision(
            session=_QUEUE_SESSION,
            decision=decision_id,
            rationale=json.dumps(_persistable_queue_payload(payload), sort_keys=True, separators=(",", ":")),
            actor={"agent": "orchestrator-daemon", "lane_id": payload["lane_id"]},
            task_ref=payload["task_ref"],
            decision_origin="system",
            refresh_rationale_on_conflict=True,
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
    _producer: str = PRODUCER_WORKTREE_DELETER,
    _drainability: str | None = None,
    _queue_state: str = QUEUE_STATE_PENDING,
    _force_authorization_provenance: str | None = None,
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
        outcome=reason,
        producer=_producer,
        drainability=_drainability,
        queue_state=_queue_state,
        force_authorization_provenance=_force_authorization_provenance,
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
    reason: str | None = None,
    observed_at: str | None = None,
    log: Callable[..., Any] | None = None,
) -> bool:
    """Persist a reporting candidate without laundering a typed refusal.

    A positive candidate is an explicit authorization fact and records its
    provenance. If a producer supplies a typed outcome, that exact outcome is
    retained and receives its own drainability instead of becoming
    ``candidate`` on the way into the ledger.
    """

    outcome = "candidate" if reason is None else reason
    authorization = "merged_registry_positive_candidate" if outcome == "candidate" else None

    return enqueue_branch_reclaim_outcome(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=authorized_sha,
        reason=outcome,
        observed_at=observed_at,
        log=log,
        _producer=PRODUCER_MERGED_REGISTRY,
        _force_authorization_provenance=authorization,
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
    outcome: str,
    drainability: str,
    queue_state: str,
    observed_at: str,
    producer: str,
    row_id: int,
    force_authorized: bool = False,
    force_authorization_provenance: str | None = None,
    failure_count: int = 0,
    last_error: str = "",
) -> BranchReclaimQueueItem:
    return BranchReclaimQueueItem(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=sha,
        outcome=outcome,
        drainability=drainability,
        queue_state=queue_state,
        observed_at=observed_at,
        producer=producer,
        row_ids=(row_id,),
        force_authorized=force_authorized,
        force_authorization_provenance=force_authorization_provenance,
        failure_count=_normalize_failure_count(failure_count),
        last_error=last_error,
    )


def _outcome_identity(
    payload: dict[str, Any], row: sqlite3.Row
) -> tuple[str, str, str, str, str, str, str, str, str | None, str] | None:
    row_task = _normalize_required_text(row["task_ref"])
    row_lane = _normalize_required_text(row["lane_id"])
    schema_version = payload.get("schema_version")
    if row_task is None or row_lane is None or schema_version not in {1, _QUEUE_SCHEMA_VERSION}:
        return None
    task_ref = _normalize_required_text(payload.get("task_ref"))
    lane_id = _normalize_required_text(payload.get("lane_id"))
    branch = _normalize_required_text(payload.get("branch"))
    sha = _normalize_sha(payload.get("sha"))
    if schema_version == 1:
        legacy_reason = payload.get("reason")
        queue_state = QUEUE_STATE_DEAD_LETTER if legacy_reason == DEAD_LETTER_REASON else QUEUE_STATE_PENDING
        outcome = "candidate" if legacy_reason == DEAD_LETTER_REASON else legacy_reason
        producer = PRODUCER_QUEUE_CONSUMER if queue_state == QUEUE_STATE_DEAD_LETTER else PRODUCER_WORKTREE_DELETER
        drainability = (
            DRAINABILITY_TERMINAL if queue_state == QUEUE_STATE_DEAD_LETTER else _outcome_drainability(str(outcome))
        )
        authorization = None
    else:
        outcome = payload.get("outcome")
        drainability = payload.get("drainability")
        queue_state = payload.get("queue_state")
        producer = payload.get("producer")
        authorization = _normalize_required_text(payload.get("force_authorization_provenance"))
    observed_at = _normalize_required_text(payload.get("ts"))
    if task_ref != row_task or lane_id != row_lane or branch is None or sha is None:
        return None
    if (
        outcome not in QUEUE_OUTCOMES
        or drainability not in {DRAINABILITY_RETRYABLE, DRAINABILITY_TERMINAL}
        or queue_state not in {QUEUE_STATE_PENDING, QUEUE_STATE_DEAD_LETTER}
        or producer not in QUEUE_PRODUCERS
        or observed_at is None
    ):
        return None
    if queue_state == QUEUE_STATE_DEAD_LETTER and drainability != DRAINABILITY_TERMINAL:
        return None
    if outcome == "skipped_identity_unrecorded" and drainability != DRAINABILITY_TERMINAL:
        return None
    expected_prefix = branch_reclaim_queue_decision_id(lane_id=lane_id, authorized_sha=sha) + "_"
    if not str(row["decision"] or "").startswith(expected_prefix):
        return None
    return (
        task_ref,
        lane_id,
        branch,
        sha,
        str(outcome),
        str(drainability),
        str(queue_state),
        str(producer),
        authorization,
        observed_at,
    )


def _decode_outcome_row(row: sqlite3.Row) -> BranchReclaimQueueItem | None:
    payload = _json_object(row["rationale"])
    if payload is None:
        return None
    identity = _outcome_identity(payload, row)
    if identity is None:
        return None
    (
        task_ref,
        lane_id,
        branch,
        sha,
        outcome,
        drainability,
        queue_state,
        producer,
        authorization,
        observed_at,
    ) = identity
    return _queue_item(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        sha=sha,
        outcome=outcome,
        drainability=drainability,
        queue_state=queue_state,
        observed_at=observed_at,
        producer=producer,
        row_id=int(row["id"]),
        force_authorized=authorization is not None,
        force_authorization_provenance=authorization,
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
        outcome="candidate",
        drainability=DRAINABILITY_RETRYABLE,
        queue_state=QUEUE_STATE_PENDING,
        observed_at=observed_at,
        producer=PRODUCER_REPORTING_SCAN,
        row_id=int(row["id"]),
        force_authorized=True,
        force_authorization_provenance=str(row["decision"]),
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
    def precedence(candidate: BranchReclaimQueueItem) -> tuple[int, str]:
        # Producer authority precedes time, so a fresh reporting observation
        # cannot erase an authoritative deleter outcome merely by arriving
        # later. Time is only a tiebreak between equal-authority producers.
        return (_PRODUCER_AUTHORITY[candidate.producer], candidate.observed_at)

    candidates = (prior, item)
    vetoes = [candidate for candidate in candidates if candidate.outcome in _ABSOLUTE_VETO_OUTCOMES]
    producer_facts = [candidate for candidate in candidates if candidate.producer != PRODUCER_QUEUE_CONSUMER]
    outcome_candidates = producer_facts or list(candidates)
    chosen = max(vetoes or outcome_candidates, key=precedence)

    # A positive observation made after a transient checkout refusal proves
    # that the condition cleared. This is the narrow recovery exception to
    # producer-first ordering; immutable identity refusals above remain vetoes.
    positive_candidates = [
        candidate for candidate in outcome_candidates if candidate.outcome == "candidate" and candidate.force_authorized
    ]
    if not vetoes and chosen.outcome in _RECOVERABLE_TRANSIENT_OUTCOMES and positive_candidates:
        newest_positive = max(positive_candidates, key=lambda candidate: candidate.observed_at)
        if newest_positive.observed_at > chosen.observed_at:
            chosen = newest_positive
    queue_state = (
        QUEUE_STATE_DEAD_LETTER
        if QUEUE_STATE_DEAD_LETTER in {prior.queue_state, item.queue_state}
        else QUEUE_STATE_PENDING
    )
    authorization_items = [
        candidate
        for candidate in candidates
        if candidate.force_authorized and candidate.producer != PRODUCER_QUEUE_CONSUMER
    ]
    authorization_item = max(authorization_items, key=precedence) if authorization_items else None
    authorized = authorization_item is not None
    authorization_source = authorization_item.force_authorization_provenance if authorization_item is not None else None
    failure_item = max(candidates, key=lambda candidate: (candidate.failure_count, candidate.observed_at))
    drainability = chosen.drainability
    if queue_state == QUEUE_STATE_DEAD_LETTER:
        drainability = DRAINABILITY_TERMINAL
    elif chosen.outcome in _ABSOLUTE_VETO_OUTCOMES:
        drainability = DRAINABILITY_TERMINAL
    return BranchReclaimQueueItem(
        task_ref=chosen.task_ref,
        lane_id=chosen.lane_id,
        branch=chosen.branch,
        authorized_sha=chosen.authorized_sha,
        outcome=chosen.outcome,
        drainability=drainability,
        queue_state=queue_state,
        observed_at=chosen.observed_at,
        producer=chosen.producer,
        row_ids=tuple(sorted((*prior.row_ids, *item.row_ids))),
        force_authorized=authorized,
        force_authorization_provenance=authorization_source,
        failure_count=failure_item.failure_count,
        last_error=failure_item.last_error,
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
    """Return the independently resolved drainability decision.

    Positive authorization provenance may make ``skipped_unmerged`` drainable,
    but no authorization may reactivate ``skipped_identity_unrecorded`` or a
    dead-letter shed. Dead-letter release is a separate policy action.
    """

    force_drainable = item.outcome == "skipped_unmerged" and item.force_authorized
    return item.queue_state == QUEUE_STATE_PENDING and (item.retryable or force_drainable)


def queue_item_cursor_id(item: BranchReclaimQueueItem) -> int:
    """Stable visit position for a coalesced job: the newest backing row id."""

    return max(item.row_ids) if item.row_ids else 0


def branch_reclaim_dead_letter_release_decision_id(*, lane_id: str, authorized_sha: str) -> str:
    """Return the stable audit id for explicit dead-letter releases."""

    normalized_lane = _normalize_required_text(lane_id)
    normalized_sha = _normalize_sha(authorized_sha)
    if normalized_lane is None:
        raise ValueError("lane_id must be a non-empty string")
    if normalized_sha is None:
        raise ValueError("authorized_sha must be a full 40-hex SHA")
    return f"{_DEAD_LETTER_RELEASE_DECISION_PREFIX}{_encode_id_component(normalized_lane)}_{normalized_sha}"


def release_branch_reclaim_dead_letter(
    conn: sqlite3.Connection,
    *,
    item: BranchReclaimQueueItem,
    operator_decision: str,
) -> bool:
    """Release an absorbing shed through an explicit, durable policy decision.

    Ordinary producer events cannot perform this transition. A caller must
    supply a non-empty operator/policy rationale; this function records it in a
    stable audit row and removes the matching dead-letter marker and consumer
    failure state in one savepoint. The stable audit row keeps repeated
    releases bounded, while removing consumer state renews the full consecutive
    failure budget.
    """

    rationale = _normalize_required_text(operator_decision)
    if item.reason != DEAD_LETTER_REASON or rationale is None:
        return False
    dead_rows: list[tuple[int, str]] = []
    consumer_rows: list[tuple[int, str]] = []
    for row_id in item.row_ids:
        row = conn.execute(
            """
            SELECT id, task_ref, lane_id, decision, rationale, created_at
            FROM decisions WHERE id = ?
            """,
            (row_id,),
        ).fetchone()
        decoded = _decode_queue_row(row) if row is not None else None
        if (
            decoded is not None
            and decoded.task_ref == item.task_ref
            and decoded.lane_id == item.lane_id
            and decoded.authorized_sha == item.authorized_sha
            and decoded.reason == DEAD_LETTER_REASON
        ):
            dead_rows.append((row_id, str(row["rationale"] or "")))
        if (
            decoded is not None
            and decoded.task_ref == item.task_ref
            and decoded.lane_id == item.lane_id
            and decoded.authorized_sha == item.authorized_sha
            and decoded.producer == PRODUCER_QUEUE_CONSUMER
        ):
            consumer_rows.append((row_id, str(row["rationale"] or "")))
    if not dead_rows:
        return False

    audit_id = branch_reclaim_dead_letter_release_decision_id(
        lane_id=item.lane_id,
        authorized_sha=item.authorized_sha,
    )
    prior_audit = conn.execute(
        "SELECT rationale FROM decisions WHERE task_ref = ? AND decision = ? AND session = ?",
        (item.task_ref, audit_id, _DEAD_LETTER_RELEASE_SESSION),
    ).fetchone()
    prior_payload = _json_object(prior_audit["rationale"]) if prior_audit is not None else None
    release_count = _normalize_failure_count((prior_payload or {}).get("release_count")) + 1
    audit_payload = json.dumps(
        {
            "schema_version": _QUEUE_SCHEMA_VERSION,
            "task_ref": item.task_ref,
            "lane_id": item.lane_id,
            "branch": item.branch,
            "sha": item.authorized_sha,
            "operator_decision": rationale,
            "release_count": release_count,
            "released_at": _utc_timestamp(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    conn.execute(f"SAVEPOINT {_RELEASE_SAVEPOINT}")
    try:
        conn.execute(
            """
            INSERT INTO decisions (
                task_ref, lane_id, session, decision, rationale,
                decision_origin, created_at
            ) VALUES (?, ?, ?, ?, ?, 'system', datetime('now'))
            ON CONFLICT(task_ref, decision, session)
            DO UPDATE SET rationale = excluded.rationale
            """,
            (item.task_ref, item.lane_id, _DEAD_LETTER_RELEASE_SESSION, audit_id, audit_payload),
        )
        reset_rows = dict((*dead_rows, *consumer_rows))
        for row_id, stored_rationale in reset_rows.items():
            cursor = conn.execute(
                "DELETE FROM decisions WHERE id = ? AND rationale = ?",
                (row_id, stored_rationale),
            )
            if cursor.rowcount != 1:
                _rollback_release_savepoint(conn)
                return False
        conn.execute(f"RELEASE SAVEPOINT {_RELEASE_SAVEPOINT}")
        return True
    except Exception:  # noqa: BLE001 - refuse rather than partially release a shed
        _rollback_release_savepoint(conn)
        return False


def _rollback_release_savepoint(conn: sqlite3.Connection) -> None:
    conn.execute(f"ROLLBACK TO SAVEPOINT {_RELEASE_SAVEPOINT}")
    conn.execute(f"RELEASE SAVEPOINT {_RELEASE_SAVEPOINT}")


def _dead_letter_release_payload(
    *,
    outcome: str,
    task_ref: str = "",
    lane_id: str = "",
    branch: str = "",
    sha: str = "",
    decision_id: str = "",
    release_count: int = 0,
    error: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "ok": outcome in {"released", "already_released"},
        "outcome": outcome,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "branch": branch,
        "sha": sha,
        "decision_id": decision_id,
        "release_count": release_count,
    }
    if error is not None:
        payload["error"] = error
    return payload


def _load_dead_letter_release_audit(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    decision_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT rationale FROM decisions WHERE task_ref = ? AND decision = ? AND session = ?",
        (task_ref, decision_id, _DEAD_LETTER_RELEASE_SESSION),
    ).fetchone()
    if row is None:
        return None
    return _json_object(row["rationale"])


def release_dead_letter_by_identity(
    *,
    task_ref: str,
    lane_id: str,
    authorized_sha: str,
    operator_decision: str,
    log: Callable[..., Any] | None = None,
) -> dict[str, object]:
    """Release one dead-lettered reclaim item by exact queue identity.

    Opens the configured ledger read-write, selects the single item whose
    ``(task_ref, lane_id, authorized_sha)`` matches exactly (full SHA; no
    prefix), and commits only when ``release_branch_reclaim_dead_letter``
    returns True. A repeat call is idempotent: an existing audit row plus a
    non-dead-letter item is ``already_released``.
    """

    normalized_task = _normalize_required_text(task_ref) or ""
    raw_lane = lane_id if isinstance(lane_id, str) else ""
    raw_sha = authorized_sha if isinstance(authorized_sha, str) else ""
    try:
        decision_id = branch_reclaim_dead_letter_release_decision_id(
            lane_id=lane_id,
            authorized_sha=authorized_sha,
        )
    except ValueError:
        payload = _dead_letter_release_payload(
            outcome="invalid_identity",
            task_ref=normalized_task,
            lane_id=_normalize_required_text(lane_id) or raw_lane,
            sha=_normalize_sha(authorized_sha) or raw_sha,
        )
        _safe_log(log, "warning", "branch_reclaim_dead_letter_release_invalid_identity", **payload)
        return payload

    normalized_lane = _normalize_required_text(lane_id) or ""
    normalized_sha = _normalize_sha(authorized_sha) or ""
    identity: dict[str, str] = {
        "task_ref": normalized_task,
        "lane_id": normalized_lane,
        "sha": normalized_sha,
        "decision_id": decision_id,
    }
    rationale = _normalize_required_text(operator_decision)
    if rationale is None:
        payload = _dead_letter_release_payload(outcome="invalid_decision", **identity)
        _safe_log(log, "warning", "branch_reclaim_dead_letter_release_invalid_decision", **payload)
        return payload

    db_path = get_runtime_config().db_path
    with closing(connect_handoff_db(db_path)) as conn:
        try:
            match = next(
                (
                    item
                    for item in list_branch_reclaim_queue_with_conn(conn, task_ref=normalized_task)
                    if item.task_ref == normalized_task
                    and item.lane_id == normalized_lane
                    and item.authorized_sha == normalized_sha
                ),
                None,
            )
            if match is None:
                payload = _dead_letter_release_payload(outcome="not_found", **identity)
                _safe_log(log, "warning", "branch_reclaim_dead_letter_release_not_found", **payload)
                return payload
            identity["branch"] = match.branch
            if match.reason != DEAD_LETTER_REASON:
                audit = _load_dead_letter_release_audit(
                    conn,
                    task_ref=normalized_task,
                    decision_id=decision_id,
                )
                if audit is not None:
                    payload = _dead_letter_release_payload(
                        outcome="already_released",
                        release_count=_normalize_failure_count(audit.get("release_count")),
                        **identity,
                    )
                    _safe_log(log, "info", "branch_reclaim_dead_letter_already_released", **payload)
                    return payload
                payload = _dead_letter_release_payload(outcome="not_dead_letter", **identity)
                _safe_log(log, "warning", "branch_reclaim_dead_letter_release_not_dead_letter", **payload)
                return payload
            released = release_branch_reclaim_dead_letter(
                conn,
                item=match,
                operator_decision=rationale,
            )
            if not released:
                conn.rollback()
                payload = _dead_letter_release_payload(outcome="release_refused", **identity)
                _safe_log(log, "warning", "branch_reclaim_dead_letter_release_refused", **payload)
                return payload
            audit = _load_dead_letter_release_audit(
                conn,
                task_ref=normalized_task,
                decision_id=decision_id,
            )
            conn.commit()
            payload = _dead_letter_release_payload(
                outcome="released",
                release_count=_normalize_failure_count((audit or {}).get("release_count")),
                **identity,
            )
            _safe_log(log, "info", "branch_reclaim_dead_letter_released", **payload)
            return payload
        except Exception as exc:  # noqa: BLE001 - refuse rather than partially commit a release
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - rollback is best-effort after the causal failure
                pass
            payload = _dead_letter_release_payload(
                outcome="release_refused",
                error=str(exc),
                **identity,
            )
            _safe_log(log, "error", "branch_reclaim_dead_letter_release_failed", **payload)
            return payload


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
    queue_state = QUEUE_STATE_DEAD_LETTER if next_count >= DEFAULT_DEAD_LETTER_FAILURES else QUEUE_STATE_PENDING
    return enqueue_branch_reclaim_outcome(
        task_ref=item.task_ref,
        lane_id=item.lane_id,
        branch=item.branch,
        authorized_sha=item.authorized_sha,
        reason=item.outcome,
        failure_count=next_count,
        last_error=last_error,
        log=log,
        _producer=PRODUCER_QUEUE_CONSUMER,
        _drainability=item.drainability,
        _queue_state=queue_state,
        # Retry/dead-letter rows carry consumer metadata only. Authorization is
        # retained on the producer fact that actually recorded it.
        _force_authorization_provenance=None,
    )


__all__ = [
    "BranchReclaimQueueItem",
    "DEAD_LETTER_REASON",
    "DEFAULT_DEAD_LETTER_FAILURES",
    "DRAINABILITY_RETRYABLE",
    "DRAINABILITY_TERMINAL",
    "NON_DELETED_BRANCH_OUTCOMES",
    "PRODUCER_MERGED_REGISTRY",
    "PRODUCER_QUEUE_CONSUMER",
    "PRODUCER_REPORTING_SCAN",
    "PRODUCER_WORKTREE_DELETER",
    "QUEUE_OUTCOMES",
    "QUEUE_REASONS",
    "QUEUE_STATE_DEAD_LETTER",
    "QUEUE_STATE_PENDING",
    "RETRYABLE_BRANCH_OUTCOMES",
    "RETRYABLE_QUEUE_REASONS",
    "TYPED_REFUSAL_QUEUE_REASONS",
    "acknowledge_branch_reclaim_item",
    "branch_reclaim_dead_letter_release_decision_id",
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
    "release_branch_reclaim_dead_letter",
    "release_dead_letter_by_identity",
    "store_branch_reclaim_drain_cursor",
]
