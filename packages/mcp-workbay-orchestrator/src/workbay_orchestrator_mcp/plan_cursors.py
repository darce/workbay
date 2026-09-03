"""Plan cursor domain operations.

Sits below ``lanes`` in the one-directional import DAG
``lanes -> {plan_cursors, lane_reaping} -> lanes_support``. This module must
never import from ``.lanes`` at any scope — module level or lazily inside a
function — or the cycle internal removed comes straight back. Shared
helpers come from ``.lanes_support``, which both this module and
``lane_reaping`` sit on top of. ``test_plan_cursors_has_no_import_of_lanes``
enforces this statically and in a fresh subprocess.

Seam note — both patch seams are module globals *of this module*, resolved at
call time:

* ``workbay_orchestrator_mcp.plan_cursors.list_plan_cursors`` — the
  ``plan_cursor`` wrapper below calls it as an unqualified module global.
* ``workbay_orchestrator_mcp.plan_cursors._get_db_connection`` — every read
  and write below opens its connection through it.

Patching either name on ``workbay_orchestrator_mcp.lanes`` does not intercept
calls made from within this module: ``lanes`` re-exports these symbols but is
not on the call path.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast

from .lanes_support import (
    _PLAN_CURSOR_IDENTITY_FIELDS,
    _effective_limit,
    _get_db_connection,
    _json_response,
    _normalize_optional_text,
    _paginated_query,
    _resolve_task_ref,
    _row_to_dict,
    _shape_list_payload,
    _summarize_generic_row,
)


def _evaluate_clean_slice_gate(
    conn: "sqlite3.Connection",
    task_ref: str,
    lane_id: str | None,
    since: str | None,
) -> dict | None:
    """Check clean-slice preconditions. Returns error payload dict or None if clean."""
    from workbay_handoff_mcp.enums import FindingSeverity, FindingStatus  # noqa: PLC0415

    open_high_query = [
        "SELECT COUNT(*) AS count FROM review_findings WHERE task_ref = ? AND status = ? AND severity = ?"
    ]
    open_high_params: list[object] = [task_ref, FindingStatus.OPEN, FindingSeverity.HIGH]
    if lane_id is not None:
        open_high_query.append("AND lane_id = ?")
        open_high_params.append(lane_id)
    open_high_count = int(conn.execute(" ".join(open_high_query), tuple(open_high_params)).fetchone()["count"])
    test_query = ["SELECT COUNT(*) AS count FROM verified_tests WHERE task_ref = ?"]
    test_params: list[object] = [task_ref]
    if since is not None:
        test_query.append("AND verified_at >= ?")
        test_params.append(since)
    fresh_test_count = int(conn.execute(" ".join(test_query), tuple(test_params)).fetchone()["count"])
    missing_gates: list[str] = []
    if open_high_count > 0:
        missing_gates.append("open_high_findings")
    if fresh_test_count == 0:
        missing_gates.append("missing_recent_test")
    if not missing_gates:
        return None
    return {
        "ok": False,
        "error": "require_clean_slice gate failed.",
        "missing_gates": missing_gates,
        "gate": {
            "require_clean_slice": True,
            "lane_scope": lane_id,
            "task_ref": task_ref,
            "open_high_count": open_high_count,
            "fresh_test_count": fresh_test_count,
            "tests_since": since,
        },
    }


@dataclass(frozen=True)
class _PlanCursorUpsertRequest:
    plan_item_id: str
    state: str
    lane_id: str | None
    mcp_action_id: int | None
    worker_message_id: int | None
    source_heading: str | None
    summary: str | None


def _valid_plan_cursor_states(*, include_all: bool = False, include_expired: bool = False) -> frozenset[str]:
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    states: set[str] = {
        PlanCursorState.DISPATCHED,
        PlanCursorState.COMPLETED,
        PlanCursorState.SKIPPED,
        PlanCursorState.ESCALATED,
    }
    if include_expired:
        # ``expired`` is reaper-owned: only reachable via internal reaper
        # writes and listable/filterable, never settable through the
        # caller-facing upsert path.
        states.add(PlanCursorState.EXPIRED)
    if include_all:
        states.add("all")
    return frozenset(states)


def _validate_plan_cursor_upsert(
    plan_item_id: str,
    state: str,
    lane_id: str | None,
    mcp_action_id: int | None,
    worker_message_id: int | None,
    source_heading: str | None,
    summary: str | None,
) -> tuple[_PlanCursorUpsertRequest | None, dict | None]:
    valid_states = _valid_plan_cursor_states()
    normalized_plan_item_id = _normalize_optional_text(plan_item_id)
    if normalized_plan_item_id is None:
        return None, _json_response({"ok": False, "error": "plan_item_id is required."})
    if state not in valid_states:
        return None, _json_response({"ok": False, "error": f"Invalid state. Valid: {', '.join(sorted(valid_states))}"})
    return (
        _PlanCursorUpsertRequest(
            plan_item_id=normalized_plan_item_id,
            state=state,
            lane_id=_normalize_optional_text(lane_id),
            mcp_action_id=mcp_action_id,
            worker_message_id=worker_message_id,
            source_heading=_normalize_optional_text(source_heading),
            summary=_normalize_optional_text(summary),
        ),
        None,
    )


def _find_plan_cursor(conn: "sqlite3.Connection", task_ref: str, plan_item_id: str) -> "sqlite3.Row | None":
    row = conn.execute(
        "SELECT * FROM plan_cursors WHERE task_ref = ? AND plan_item_id = ?",
        (task_ref, plan_item_id),
    ).fetchone()
    return cast("sqlite3.Row | None", row)


def _coalesce_plan_cursor_fields(request: _PlanCursorUpsertRequest, existing: "sqlite3.Row"):
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    prior_state = str(existing["state"])
    raw_prior_summary = existing["summary"]
    prior_summary = str(raw_prior_summary) if raw_prior_summary is not None else None
    recovering_from_expired = prior_state == PlanCursorState.EXPIRED and request.state != PlanCursorState.EXPIRED
    if recovering_from_expired:
        # There is no dedicated persisted column for provenance (schema is
        # out of scope here), so fold the reaper's note into the persisted
        # summary itself -- otherwise it is lost the moment this write lands
        # and a later get/list can never recover it. The reaper's own reason
        # is appended at the TAIL of its note (see import_export.py's
        # ``f"{summary} [{note}]"`` shape), so truncation must keep the tail,
        # not the head, or it clips exactly the reason text.
        base_summary = request.summary if request.summary is not None else "revived from expired"
        if prior_summary is None:
            next_summary = base_summary
        elif len(prior_summary) > 200:
            next_summary = f"{base_summary} (was: …{prior_summary[-200:]})"
        else:
            next_summary = f"{base_summary} (was: {prior_summary})"
    elif request.summary is not None:
        next_summary = request.summary
    else:
        next_summary = prior_summary if prior_summary is not None else str(existing["summary"])
    fields = {
        "lane_id": request.lane_id or _normalize_optional_text(existing["lane_id"]),
        "summary": next_summary,
        "source_heading": request.source_heading or _normalize_optional_text(existing["source_heading"]),
        "mcp_action_id": (request.mcp_action_id if request.mcp_action_id is not None else existing["mcp_action_id"]),
        "worker_message_id": (
            request.worker_message_id if request.worker_message_id is not None else existing["worker_message_id"]
        ),
        "dispatch_count": int(existing["dispatch_count"] or 0)
        + (1 if request.state == PlanCursorState.DISPATCHED else 0),
    }
    if recovering_from_expired:
        # Still surfaced as a discrete field on the response for callers that
        # want the prior note without parsing it back out of the summary.
        fields["previous_summary"] = prior_summary
    return fields


def _insert_plan_cursor(
    conn: "sqlite3.Connection",
    task_ref: str,
    request: _PlanCursorUpsertRequest,
) -> dict:
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    cur = conn.execute(
        """
        INSERT INTO plan_cursors (
            task_ref, plan_item_id, state, lane_id, mcp_action_id, worker_message_id,
            source_heading, summary, dispatch_count, dispatched_at, completed_at, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?,
            CASE WHEN ? = ? THEN 1 ELSE 0 END,
            CASE WHEN ? = ? THEN datetime('now') ELSE NULL END,
            CASE WHEN ? = ? THEN datetime('now') ELSE NULL END,
            datetime('now'), datetime('now')
        )
        """,
        (
            task_ref,
            request.plan_item_id,
            request.state,
            request.lane_id,
            request.mcp_action_id,
            request.worker_message_id,
            request.source_heading,
            request.summary,
            request.state,
            PlanCursorState.DISPATCHED,
            request.state,
            PlanCursorState.DISPATCHED,
            request.state,
            PlanCursorState.COMPLETED,
        ),
    )
    row = _row_to_dict(conn.execute("SELECT * FROM plan_cursors WHERE id = ?", (cur.lastrowid,)).fetchone())
    assert row is not None
    row["state"] = PlanCursorState(request.state)
    return row


def _apply_plan_cursor_update(
    conn: "sqlite3.Connection",
    task_ref: str,
    request: _PlanCursorUpsertRequest,
    candidate: "sqlite3.Row",
) -> tuple[dict, bool] | None:
    """Attempt a single compare-and-set UPDATE against the ``candidate`` read.

    Returns ``(row, recovered)`` if the CAS write applied (exactly one row
    matched both the identity and the pre-read ``state``), or ``None`` if the
    row's state had already moved out from under ``candidate`` (rowcount 0).
    """
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    next_fields = _coalesce_plan_cursor_fields(request, candidate)
    prior_state = str(candidate["state"])
    recovered = prior_state == PlanCursorState.EXPIRED and request.state != PlanCursorState.EXPIRED
    cur = conn.execute(
        """
        UPDATE plan_cursors
        SET state = ?, lane_id = ?, mcp_action_id = ?, worker_message_id = ?,
            source_heading = ?, summary = ?, dispatch_count = ?,
            dispatched_at = CASE WHEN ? = ? THEN datetime('now') ELSE dispatched_at END,
            completed_at = CASE WHEN ? = ? THEN datetime('now') ELSE completed_at END,
            updated_at = datetime('now')
        WHERE task_ref = ? AND plan_item_id = ? AND state = ?
        """,
        (
            request.state,
            next_fields["lane_id"],
            next_fields["mcp_action_id"],
            next_fields["worker_message_id"],
            next_fields["source_heading"],
            next_fields["summary"],
            next_fields["dispatch_count"],
            request.state,
            PlanCursorState.DISPATCHED,
            request.state,
            PlanCursorState.COMPLETED,
            task_ref,
            request.plan_item_id,
            prior_state,
        ),
    )
    if cur.rowcount == 0:
        return None
    row = _row_to_dict(_find_plan_cursor(conn, task_ref, request.plan_item_id))
    assert row is not None
    row["state"] = PlanCursorState(request.state)
    if "previous_summary" in next_fields:
        row["previous_summary"] = next_fields["previous_summary"]
    return row, recovered


def _update_plan_cursor(
    conn: "sqlite3.Connection",
    task_ref: str,
    request: _PlanCursorUpsertRequest,
    existing: "sqlite3.Row",
) -> tuple[dict, bool] | dict:
    """Compare-and-set the cursor row, retrying once against a fresh read.

    Returns ``(row, recovered)`` on a successful write, or the same
    ``{"ok": False, "error": ...}`` refusal shape used elsewhere in this
    module if the row keeps changing out from under the write.
    """
    candidate = existing
    for attempt in range(2):
        result = _apply_plan_cursor_update(conn, task_ref, request, candidate)
        if result is not None:
            return result
        if attempt == 0:
            refreshed = _find_plan_cursor(conn, task_ref, request.plan_item_id)
            if refreshed is None:
                return _json_response({"ok": False, "error": "Plan cursor was deleted concurrently; retry the upsert."})
            candidate = refreshed
    return _json_response({"ok": False, "error": "Plan cursor was modified concurrently; retry the upsert."})


def upsert_plan_cursor(
    plan_item_id: str,
    state: str,
    lane_id: str | None = None,
    mcp_action_id: int | None = None,
    worker_message_id: int | None = None,
    source_heading: str | None = None,
    summary: str | None = None,
    task_ref: str | None = None,
    require_clean_slice: bool = False,
) -> dict:
    request, error = _validate_plan_cursor_upsert(
        plan_item_id,
        state,
        lane_id,
        mcp_action_id,
        worker_message_id,
        source_heading,
        summary,
    )
    if error is not None:
        return error
    assert request is not None
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing = _find_plan_cursor(conn, resolved_task_ref, request.plan_item_id)
        if existing is None and request.summary is None:
            return _json_response({"ok": False, "error": "summary is required when creating a new plan cursor."})
        if require_clean_slice:
            next_lane_id = (
                request.lane_id or _normalize_optional_text(existing["lane_id"])
                if existing is not None
                else request.lane_id
            )
            since_value = existing["updated_at"] if existing is not None else None
            gate_failure = _evaluate_clean_slice_gate(conn, resolved_task_ref, next_lane_id, since_value)
            if gate_failure is not None:
                return _json_response(gate_failure)
        if existing is None:
            row = _insert_plan_cursor(conn, resolved_task_ref, request)
            return _json_response({"ok": True, "cursor": row})
        result = _update_plan_cursor(conn, resolved_task_ref, request, existing)
        if isinstance(result, dict):
            return result
        row, recovered_from_expired = result
        response: dict[str, object] = {"ok": True, "cursor": row}
        if recovered_from_expired:
            response["recovered_from_state"] = "expired"
        return _json_response(response)


def get_plan_cursor(plan_item_id: str, task_ref: str | None = None) -> dict:
    normalized_plan_item_id = _normalize_optional_text(plan_item_id)
    if normalized_plan_item_id is None:
        return _json_response({"ok": False, "error": "plan_item_id is required."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            "SELECT * FROM plan_cursors WHERE task_ref = ? AND plan_item_id = ?",
            (resolved_task_ref, normalized_plan_item_id),
        ).fetchone()
        return _json_response({"ok": True, "task_ref": resolved_task_ref, "cursor": _row_to_dict(row)})


def list_plan_cursors(
    task_ref: str | None = None,
    state: str = "all",
    lane_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_cursors: int | None = None,
) -> dict:
    valid_states = _valid_plan_cursor_states(include_all=True, include_expired=True)
    if state not in valid_states:
        return _json_response({"ok": False, "error": f"Invalid state. Valid: {', '.join(sorted(valid_states))}"})
    limit = _effective_limit(limit, top_n_cursors)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if state != "all":
            where_sql += " AND state = ?"
            params.append(state)
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        total, rows = _paginated_query(
            conn, "plan_cursors", where_sql, tuple(params), limit, offset, "updated_at DESC, id DESC"
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "state": state,
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "cursors": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="cursors",
                identity_fields=_PLAN_CURSOR_IDENTITY_FIELDS,
                summary_fn=_summarize_generic_row,
            )
        )


def plan_cursor(
    operation: str,
    plan_item_id: str | None = None,
    state: str | None = None,
    lane_id: str | None = None,
    mcp_action_id: int | None = None,
    worker_message_id: int | None = None,
    source_heading: str | None = None,
    summary: str | None = None,
    task_ref: str | None = None,
    require_clean_slice: bool = False,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_cursors: int | None = None,
) -> dict:
    """Discriminated wrapper for plan cursor upsert, get, and list operations."""
    valid_operations = {"get", "list", "upsert"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "upsert":
        return upsert_plan_cursor(
            plan_item_id=str(plan_item_id or ""),
            state=str(state or ""),
            lane_id=lane_id,
            mcp_action_id=mcp_action_id,
            worker_message_id=worker_message_id,
            source_heading=source_heading,
            summary=summary,
            task_ref=task_ref,
            require_clean_slice=require_clean_slice,
        )
    if operation == "get":
        return get_plan_cursor(plan_item_id=str(plan_item_id or ""), task_ref=task_ref)
    return list_plan_cursors(
        task_ref=task_ref,
        state=state or "all",
        lane_id=lane_id,
        limit=limit,
        offset=offset,
        sections=sections,
        detail=detail,
        fields=fields,
        top_n_cursors=top_n_cursors,
    )


# ---------------------------------------------------------------------------
# internal — blocked-lane aging report + conclusive-close
# ---------------------------------------------------------------------------
