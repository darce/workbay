"""Hub-owned worktree_lanes read/write operations."""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any
from urllib.parse import quote

from .shared_primitives import _envelope, _normalize_optional_text, _resolve_task_ref, _row_to_dict
from .shared_schema import WORKTREE_LANE_BRANCH_TIP_SOURCES, _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, collect_target_context_warnings

_log = logging.getLogger(__name__)

# closed_stale is the reaper terminal status (schema CHECK + reclaim scans);
# operators open/update through LANE_STATUSES; close_lane stays closed|merged.
LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "merged", "closed", "closed_stale"})
CLOSEABLE_LANE_STATUSES = frozenset({"closed", "merged"})
# Non-close writes must not set closed|merged: only close_lane runs the
# lane_messages reclaimer (pruned_lane_messages). closed_stale remains for the
# reaper CAS path via update/open round-trip [cs0166-r12-16].
OPENABLE_LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "closed_stale"})

# Sentinel so "after_id omitted" and "after_id=None" are distinct modes.
# Omitted -> legacy OFFSET paging; None -> keyset first page (PLAN0181-S2KEYSETSEED-01).
_UNSET = object()

# Machine-readable cause when the CURRENT_TASK side-effect fails after a
# committed lane SQL write (FW2-WV04-F1 / OBS-04). Callers must not string-match
# the prose ``error`` field.
CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE = "current_task_side_effect_failed"
# Programming / contract errors (TypeError, AttributeError, ImportError) from the
# side-effect path must not be typed as infrastructure failure (OBS-08).
CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE = "current_task_side_effect_programming_error"

# Exception classes that indicate a programming/contract bug in the side-effect
# call path rather than an infrastructure/render failure.
_PROGRAMMING_SIDE_EFFECT_ERRORS = (TypeError, AttributeError, ImportError)

# Full identity only. A short SHA is refused, never truncated or padded.
_FULL_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _parse_full_commit_sha(value: object, field: str) -> tuple[str | None, str | None]:
    """Return ``(normalized_sha_or_none, error_or_none)``.

    ``None`` / blank means omit (preserve the stored column). Any other
    present value must be a full 40-hex SHA; mixed case is normalized to
    lowercase. Never truncate, pad, or expand an abbreviation.
    """
    if value is None:
        return None, None
    if not isinstance(value, str):
        return None, f"{field} must be a full 40-hex commit sha."
    stripped = value.strip()
    if not stripped:
        return None, None
    normalized = stripped.lower()
    if not _FULL_COMMIT_SHA_RE.fullmatch(normalized):
        return None, f"{field} must be a full 40-hex commit sha."
    return normalized, None


def _echo_identity_shas(lane: dict[str, Any] | None) -> dict[str, object]:
    if not isinstance(lane, dict):
        return {
            "landing_commit_sha": None,
            "branch_tip_sha": None,
            "branch_tip_source": None,
            "branch_tip_observed_at": None,
        }
    return {
        "landing_commit_sha": lane.get("landing_commit_sha"),
        "branch_tip_sha": lane.get("branch_tip_sha"),
        "branch_tip_source": lane.get("branch_tip_source"),
        "branch_tip_observed_at": lane.get("branch_tip_observed_at"),
    }


def _current_task_md_written_from_result(result: object) -> bool:
    """Interpret a ``generate_current_task_md`` / ``render_handoff`` envelope.

    Production returns a v2 envelope ``{ok: True, data: {written: bool, ...}}``.
    Only a recognised success envelope with nested ``written is True`` reports
    written; every other shape (including unrecognised) is NOT written (OBS-08 /
    FW2-WV04-F2). The ``ok is not True`` guard is load-bearing: a falsy ok with a
    valid nested ``data.written is True`` must still report NOT written (TEST-15 /
    CARD-07).
    """
    if not isinstance(result, dict):
        return False
    if result.get("ok") is not True:
        return False
    data = result.get("data")
    if not isinstance(data, dict):
        return False
    return data.get("written") is True


def _write_current_task_md_for_task(conn: object, task_ref: str) -> tuple[bool, str | None]:
    """Regenerate CURRENT_TASK side-effect after a successful lane SQL write.

    Returns ``(written, error_type)``. ``written`` is True only when the render
    call returns a recognised success envelope with nested ``data.written is
    True``. Unrecognised shapes, ``ok is not True``, and ``written is not True``
    all report ``written=False`` with ``error_type`` set to the infrastructure
    side-effect constant (OBS-08). Never raises: callers surface dual-write
    outcome on the MCP envelope instead of leaking exceptions
    [cs0166-r13-17]. ``conn`` is discarded — generate_current_task_md opens its
    own connection. Callers must invoke this **after** the primary write
    transaction commits (HOLDERCLASS-R1-F3 / CON-18): the nested open + full
    render + file write must not run while RESERVED is held.

    On swallowed exceptions, logs the exception type (OBS-08) and classifies
    TypeError / AttributeError / ImportError under the programming-error
    ``error_type`` so a signature/refactor bug is not misreported as
    infrastructure failure. Other exceptions keep the infrastructure type.
    Broad ``Exception`` is still caught and not re-raised.
    """
    del conn
    try:
        from . import generate_current_task_md  # noqa: PLC0415

        result = generate_current_task_md(task_ref=task_ref, write_file=True)
    except Exception as exc:
        # OBS-08: do not hide the exception class when swallowing.
        _log.warning(
            "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
            task_ref,
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        if isinstance(exc, _PROGRAMMING_SIDE_EFFECT_ERRORS):
            return False, CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE
        return False, CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE
    if _current_task_md_written_from_result(result):
        return True, None
    return False, CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE


def _get_lane_row(conn: sqlite3.Connection, task_ref: str, lane_id: str) -> sqlite3.Row | None:
    row: sqlite3.Row | None = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    return row


def open_lane(
    *,
    lane_id: str,
    worktree_path: str,
    branch: str,
    title: str | None = None,
    objective: str | None = None,
    owner_agent: str | None = None,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    test_cmd: str | None = None,
    lane_kind: str | None = None,
    status: str = "planned",
    notes: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict[str, Any]:
    """Insert or update a worktree lane row (upsert).

    ``lane_kind`` is ``'implement'`` (default) or ``'review'`` (implementation note R3).
    Omitting it (``None``) defaults a NEW lane to ``'implement'`` and PRESERVES the
    stored kind on a re-upsert (so a status/notes refresh never resets a review
    lane) — the same omit-preserve contract test_cmd uses, kept NOT NULL via the
    column default.

    Omit-preserve on re-upsert also applies to optional text fields whose Python
    default is ``None``: ``title``, ``objective``, ``notes``, ``owner_agent``,
    ``model``, ``backend``, ``reasoning_effort``, and ``test_cmd``. A caller that
    refreshes ``worktree_path``/``branch``/``status`` without re-supplying those
    fields must not wipe stored values via bare ``excluded.*`` (NULL wins)
    [cs0166-r13-18]. Required path/branch and explicit status still overwrite.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_path = _normalize_optional_text(worktree_path)
    normalized_branch = _normalize_optional_text(branch)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="open_lane", data={"error": "lane_id is required."}, entity="lane")
    if normalized_path is None:
        return _envelope(ok=False, tool="open_lane", data={"error": "worktree_path is required."}, entity="lane")
    if normalized_branch is None:
        return _envelope(ok=False, tool="open_lane", data={"error": "branch is required."}, entity="lane")
    if status not in OPENABLE_LANE_STATUSES:
        return _envelope(
            ok=False,
            tool="open_lane",
            data={
                "error": (
                    f"Invalid status for open_lane. Valid: "
                    f"{', '.join(sorted(OPENABLE_LANE_STATUSES))}. "
                    "Use close_lane for closed|merged (reclaims lane_messages)."
                )
            },
            entity="lane",
        )
    if lane_kind is not None and lane_kind not in ("implement", "review"):
        return _envelope(
            ok=False,
            tool="open_lane",
            data={"error": "Invalid lane_kind. Valid: implement, review"},
            entity="lane",
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        warnings: list[str] = []
        resolved_owner = owner_agent
        if actor is not None:
            ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
            warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
            resolved_owner = owner_agent or ctx.agent
        conn.execute(
            """
            INSERT INTO worktree_lanes (
                task_ref, lane_id, title, objective, worktree_path, branch,
                owner_agent, model, backend, reasoning_effort, test_cmd, lane_kind, status, notes,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, 'implement'), ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(task_ref, lane_id) DO UPDATE SET
                title = COALESCE(excluded.title, worktree_lanes.title),
                objective = COALESCE(excluded.objective, worktree_lanes.objective),
                worktree_path = excluded.worktree_path,
                branch = excluded.branch,
                owner_agent = COALESCE(excluded.owner_agent, worktree_lanes.owner_agent),
                model = COALESCE(excluded.model, worktree_lanes.model),
                backend = COALESCE(excluded.backend, worktree_lanes.backend),
                reasoning_effort = COALESCE(excluded.reasoning_effort, worktree_lanes.reasoning_effort),
                test_cmd = COALESCE(excluded.test_cmd, worktree_lanes.test_cmd),
                lane_kind = COALESCE(?, worktree_lanes.lane_kind),
                status = excluded.status,
                notes = COALESCE(excluded.notes, worktree_lanes.notes),
                updated_at = datetime('now')
            """,
            (
                resolved_task_ref,
                normalized_lane_id,
                title,
                objective,
                normalized_path,
                normalized_branch,
                resolved_owner,
                model,
                backend,
                reasoning_effort,
                test_cmd,
                lane_kind,
                status,
                notes,
                lane_kind,
            ),
        )
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        lane_dict = _row_to_dict(row)
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.
        # Primary SQL write lands on with-block exit; render opens its own
        # connection and must not hold RESERVED across nested open + file I/O.
    md_written, side_effect_error_type = _write_current_task_md_for_task(object(), resolved_task_ref)
    if not md_written:
        # Primary SQL write already landed; keep diagnostics + typed cause
        # so callers can tell the row committed (FW2-WV04-F1 / OBS-04 /
        # DATA-01). current_task_md_written is False (attempted+failed),
        # not None (never attempted) — OBS-08 three-state.
        return _envelope(
            ok=False,
            tool="open_lane",
            data={
                "error": "Lane row written but CURRENT_TASK side-effect failed.",
                "error_type": side_effect_error_type or CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE,
                "lane": lane_dict,
                "task_ref": resolved_task_ref,
                "current_task_md_written": False,
            },
            entity="lane",
            warnings=warnings,
        )
    return _envelope(
        ok=True,
        tool="open_lane",
        data={
            "lane": lane_dict,
            "task_ref": resolved_task_ref,
            "current_task_md_written": True,
        },
        entity="lane",
        warnings=warnings,
    )


def update_lane(
    *,
    lane_id: str,
    title: str | None = None,
    objective: str | None = None,
    worktree_path: str | None = None,
    branch: str | None = None,
    owner_agent: str | None = None,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    test_cmd: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict[str, Any]:
    """Patch an existing worktree lane row."""
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="update_lane", data={"error": "lane_id is required."}, entity="lane")
    if status is not None and status not in OPENABLE_LANE_STATUSES:
        return _envelope(
            ok=False,
            tool="update_lane",
            data={
                "error": (
                    f"Invalid status for update_lane. Valid: "
                    f"{', '.join(sorted(OPENABLE_LANE_STATUSES))}. "
                    "Use close_lane for closed|merged (reclaims lane_messages)."
                )
            },
            entity="lane",
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        warnings: list[str] = []
        if actor is not None:
            ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
            warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        existing = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if existing is None:
            return _envelope(
                ok=False,
                tool="update_lane",
                data={"error": f"Lane '{normalized_lane_id}' not found for task '{resolved_task_ref}'."},
                entity="lane",
            )
        patch: dict[str, object | None] = {
            "title": title,
            "objective": objective,
            "worktree_path": _normalize_optional_text(worktree_path) if worktree_path is not None else None,
            "branch": _normalize_optional_text(branch) if branch is not None else None,
            "owner_agent": owner_agent if owner_agent is not None else (ctx.agent if actor is not None else None),
            "model": model,
            "backend": backend,
            "reasoning_effort": reasoning_effort,
            "test_cmd": test_cmd,
            "status": status,
            "notes": notes,
        }
        fields: list[str] = []
        params: list[object] = []
        for column, value in patch.items():
            if value is not None:
                fields.append(f"{column} = ?")
                params.append(value)
        if not fields:
            # No SQL patch and no CURRENT_TASK render attempt. OBS-08 three-state:
            # None = never attempted (distinct from False = attempted and failed).
            return _envelope(
                ok=True,
                tool="update_lane",
                data={
                    "lane": _row_to_dict(existing),
                    "task_ref": resolved_task_ref,
                    "current_task_md_written": None,
                },
                entity="lane",
                warnings=warnings,
            )
        fields.append("updated_at = datetime('now')")
        params.extend([resolved_task_ref, normalized_lane_id])
        conn.execute(
            f"UPDATE worktree_lanes SET {', '.join(fields)} WHERE task_ref = ? AND lane_id = ?",
            tuple(params),
        )
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        lane_dict = _row_to_dict(row)
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.
    md_written, side_effect_error_type = _write_current_task_md_for_task(object(), resolved_task_ref)
    if not md_written:
        # Primary SQL write already landed; keep diagnostics + typed cause
        # so callers can tell the row committed (FW2-WV04-F1 / OBS-04 /
        # DATA-01). current_task_md_written is False (attempted+failed).
        return _envelope(
            ok=False,
            tool="update_lane",
            data={
                "error": "Lane row written but CURRENT_TASK side-effect failed.",
                "error_type": side_effect_error_type or CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE,
                "lane": lane_dict,
                "task_ref": resolved_task_ref,
                "current_task_md_written": False,
            },
            entity="lane",
            warnings=warnings,
        )
    return _envelope(
        ok=True,
        tool="update_lane",
        data={
            "lane": lane_dict,
            "task_ref": resolved_task_ref,
            "current_task_md_written": True,
        },
        entity="lane",
        warnings=warnings,
    )


def close_lane(
    *,
    lane_id: str,
    status: str = "closed",
    notes: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    landing_commit_sha: str | None = None,
    branch_tip_sha: str | None = None,
    branch_tip_source: str | None = None,
) -> dict[str, Any]:
    """Transition a lane to a closeable terminal status.

    ``landing_commit_sha`` and ``branch_tip_sha`` are omit-preserve: a
    missing/blank value keeps the stored column via ``COALESCE``. A present
    value must be a full 40-hex SHA; a short SHA is an envelope error and
    does not mutate the row.

    Writing a new ``branch_tip_sha`` also stamps ``branch_tip_source``
    (``branch`` for a live ref, ``manifest`` for a caller-supplied value)
    and ``branch_tip_observed_at``. Omit-preserve leaves those columns
    untouched so a derived tip stays observationally distinct from
    never-observed.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="close_lane", data={"error": "lane_id is required."}, entity="lane")
    if status not in CLOSEABLE_LANE_STATUSES:
        return _envelope(
            ok=False,
            tool="close_lane",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(CLOSEABLE_LANE_STATUSES))}"},
            entity="lane",
        )
    parsed_landing, landing_error = _parse_full_commit_sha(landing_commit_sha, "landing_commit_sha")
    parsed_tip, tip_error = _parse_full_commit_sha(branch_tip_sha, "branch_tip_sha")
    sha_error = landing_error or tip_error
    if sha_error is not None:
        return _envelope(ok=False, tool="close_lane", data={"error": sha_error}, entity="lane")
    parsed_source: str | None = None
    if parsed_tip is not None:
        normalized_source = _normalize_optional_text(branch_tip_source)
        if normalized_source is None:
            parsed_source = "manifest"
        elif normalized_source not in WORKTREE_LANE_BRANCH_TIP_SOURCES:
            return _envelope(
                ok=False,
                tool="close_lane",
                data={
                    "error": (
                        "branch_tip_source must be one of: "
                        + ", ".join(WORKTREE_LANE_BRANCH_TIP_SOURCES)
                    )
                },
                entity="lane",
            )
        else:
            parsed_source = normalized_source
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        warnings: list[str] = []
        if actor is not None:
            ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
            warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        existing = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if existing is None:
            return _envelope(
                ok=False,
                tool="close_lane",
                data={"error": f"Lane '{normalized_lane_id}' not found for task '{resolved_task_ref}'."},
                entity="lane",
            )
        conn.execute(
            """
            UPDATE worktree_lanes
            SET status = ?,
                notes = COALESCE(?, notes),
                landing_commit_sha = COALESCE(?, landing_commit_sha),
                branch_tip_sha = COALESCE(?, branch_tip_sha),
                branch_tip_source = CASE WHEN ? IS NOT NULL THEN ? ELSE branch_tip_source END,
                branch_tip_observed_at = CASE WHEN ? IS NOT NULL THEN datetime('now') ELSE branch_tip_observed_at END,
                updated_at = datetime('now')
            WHERE task_ref = ? AND lane_id = ?
            """,
            (
                status,
                notes,
                parsed_landing,
                parsed_tip,
                parsed_tip,
                parsed_source,
                parsed_tip,
                resolved_task_ref,
                normalized_lane_id,
            ),
        )
        # Steady-state reclaimer (internal S1): brief/inbox rows have
        # no other deletion path, so a terminal lane status reclaims its inbox to
        # bound accumulation. S1-A-003: prune only already-terminal messages
        # (acknowledged/closed); an 'open' message is unresolved history that
        # archive_task must still snapshot before deleting — close_lane must not
        # destroy it ahead of that snapshot.
        pruned = conn.execute(
            "DELETE FROM lane_messages WHERE task_ref = ? AND lane_id = ? AND status != 'open'",
            (resolved_task_ref, normalized_lane_id),
        )
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        lane_dict = _row_to_dict(row)
        pruned_count = pruned.rowcount
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.
        # The DB transition (status + message reclaim) commits on with-block
        # exit; render must not extend the RESERVED window.
    md_written, side_effect_error_type = _write_current_task_md_for_task(object(), resolved_task_ref)
    if not md_written:
        # Direction for FW2-WV04-F3 / DATA-01 / CLM-04: the DB transition
        # (status + message reclaim) is the primary effect and is committed
        # by ``_get_db_connection`` on with-block exit. Returning ok=False
        # would misreport that committed mutation as a failure and is more
        # likely to provoke a naive retry. It does not, by itself, change
        # retry prune accounting — a second close still reports
        # pruned_lane_messages=0 after a successful first reclaim. Honest
        # envelope is success with an explicit typed side-effect warning so
        # a retry is less likely to be provoked.
        typed_cause = side_effect_error_type or CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE
        side_effect_warnings = list(warnings)
        side_effect_warnings.append(typed_cause)
        return _envelope(
            ok=True,
            tool="close_lane",
            data={
                "lane": lane_dict,
                "task_ref": resolved_task_ref,
                "pruned_lane_messages": pruned_count,
                # OBS-08 three-state: False = attempted and failed.
                "current_task_md_written": False,
                "error_type": typed_cause,
                **_echo_identity_shas(lane_dict),
            },
            entity="lane",
            warnings=side_effect_warnings,
        )
    return _envelope(
        ok=True,
        tool="close_lane",
        data={
            "lane": lane_dict,
            "task_ref": resolved_task_ref,
            "pruned_lane_messages": pruned_count,
            "current_task_md_written": True,
            **_echo_identity_shas(lane_dict),
        },
        entity="lane",
        warnings=warnings,
    )


def get_lane(*, lane_id: str, task_ref: str | None = None) -> dict[str, Any]:
    """Return a single lane row."""
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="get_lane", data={"error": "lane_id is required."}, entity="lane")
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if row is None:
            return _envelope(
                ok=False,
                tool="get_lane",
                data={"error": f"Lane '{normalized_lane_id}' not found for task '{resolved_task_ref}'."},
                entity="lane",
            )
        return _envelope(
            ok=True,
            tool="get_lane",
            data={"lane": _row_to_dict(row), "task_ref": resolved_task_ref},
            entity="lane",
        )


def latest_lane_landing(*, lane_id: str, task_ref: str) -> dict[str, Any]:
    """Return the newest recorded landing commit for a lane, if one exists.

    Landing rows are ``lane_landed_<task_ref>_<lane_id>`` decisions written by the
    orchestrator before every MERGED transition. Each re-land inserts a new row
    (SHA-scoped session), so ``session`` is deliberately not part of the match;
    ``created_at`` is ``datetime('now')`` at 1-second resolution, so ``id DESC``
    is the deterministic tie-break. An absent landing is ``ok=True`` with
    ``landing=None`` -- it is a normal predicate state, not a read error.

    Both ``lane_id`` and ``task_ref`` are required and keyword-only: the only
    intended caller always knows both values, and the two opaque strings must
    not be swapped by positional call order.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(
            ok=False,
            tool="latest_lane_landing",
            data={"error": "lane_id is required."},
            entity="lane",
        )
    normalized_task_ref = _normalize_optional_text(task_ref)
    if normalized_task_ref is None:
        return _envelope(
            ok=False,
            tool="latest_lane_landing",
            data={"error": "task_ref is required."},
            entity="lane",
        )
    with _get_db_connection() as conn:
        decision_id = f"lane_landed_{normalized_task_ref}_{normalized_lane_id}"
        row = conn.execute(
            """
            SELECT id, task_ref, lane_id, session, decision, branch, commit_sha, agent,
                   decision_origin, created_at
            FROM decisions
            WHERE task_ref = ?
              AND decision = ?
              AND commit_sha IS NOT NULL
              AND TRIM(commit_sha) <> ''
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (normalized_task_ref, decision_id),
        ).fetchone()
        # Cheap defence: decision ids embed task_ref + lane_id and are ambiguous
        # under underscore-containing components; refuse a row whose column
        # task_ref disagrees with the query (writer inconsistency / SQL drift).
        if row is not None and str(row["task_ref"]) != normalized_task_ref:
            return _envelope(
                ok=False,
                tool="latest_lane_landing",
                data={
                    "error": "landing row task_ref does not match query task_ref.",
                    "task_ref": normalized_task_ref,
                    "lane_id": normalized_lane_id,
                    "decision": decision_id,
                    "row_task_ref": str(row["task_ref"]),
                },
                task_ref=normalized_task_ref,
                entity="lane",
            )
        return _envelope(
            ok=True,
            tool="latest_lane_landing",
            data={
                "task_ref": normalized_task_ref,
                "lane_id": normalized_lane_id,
                "decision": decision_id,
                "landing": _row_to_dict(row),
            },
            task_ref=normalized_task_ref,
            entity="lane",
        )


def _encode_reclaim_id_component(value: str) -> str:
    """Percent-encode one id component so the underscore join stays injective.

    ``urllib.parse.quote`` leaves RFC 3986 unreserved characters (including
    ``_``) alone even with ``safe=""``. Encode ``_`` explicitly so
    ``(a, b_c)`` and ``(a_b, c)`` cannot compose the same decision string.
    """
    return quote(value, safe="").replace("_", "%5F")


def reclaim_candidate_decision_id(*, task_ref: str, lane_id: str) -> str:
    """Build the decision id for a reclaim-candidate ledger row.

    Components are stripped, then percent-encoded, then joined on ``_``. The
    strip lives **here and nowhere else**: writer and reader share this one
    constructor so the two packages cannot disagree about whitespace at the
    edge (PLAN0181-S2IDFMT-01) [CON-05][REF-19]. Pushing normalisation back to
    callers would let an unstripped caller compose a different id and read
    ``candidate=None`` -- indistinguishable from "never scanned".

    The encoding is injective over the stripped components, which is the
    property the underscore join needs: ``('a', 'b_c')`` and ``('a_b', 'c')``
    compose distinct ids. It is deliberately *not* injective over surrounding
    whitespace -- ``'internal '`` and ``'internal'`` name the same task, so
    collapsing them onto one ledger row is correct, not a collision. Do not
    substitute a "safe" separator.
    """
    return (
        "lane_reclaim_candidate_"
        f"{_encode_reclaim_id_component(task_ref.strip())}_"
        f"{_encode_reclaim_id_component(lane_id.strip())}"
    )


def branch_reclaim_candidate_decision_id(*, task_ref: str, lane_id: str) -> str:
    """Build the decision id for a branch-reclaim-candidate ledger row.

    This is deliberately a peer of :func:`reclaim_candidate_decision_id`, not
    a transformation of its output.  Both constructors apply the same
    strip-and-encode discipline so embedded prefixes and separators remain
    data rather than structure [CON-05][DATA-03].
    """
    return (
        "lane_branch_reclaim_candidate_"
        f"{_encode_reclaim_id_component(task_ref.strip())}_"
        f"{_encode_reclaim_id_component(lane_id.strip())}"
    )


def _latest_reclaim_candidate(
    *,
    lane_id: str,
    task_ref: str,
    decision_id_builder: Any,
    tool: str,
) -> dict[str, Any]:
    """Shared exact-id reader for worktree and branch reclaim ledgers."""
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(
            ok=False,
            tool=tool,
            data={"error": "lane_id is required."},
            entity="lane",
        )
    normalized_task_ref = _normalize_optional_text(task_ref)
    if normalized_task_ref is None:
        return _envelope(
            ok=False,
            tool=tool,
            data={"error": "task_ref is required."},
            entity="lane",
        )
    with _get_db_connection() as conn:
        decision_id = decision_id_builder(task_ref=normalized_task_ref, lane_id=normalized_lane_id)
        row = conn.execute(
            """
            SELECT id, task_ref, lane_id, session, decision, branch, commit_sha, agent,
                   decision_origin, rationale, created_at
            FROM decisions
            WHERE task_ref = ?
              AND decision = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (normalized_task_ref, decision_id),
        ).fetchone()
        # The decision id embeds lane_id, but decisions.lane_id comes from the
        # actor and can disagree. Refuse ambiguous ownership [CON-05].
        if row is not None:
            stored_lane = _normalize_optional_text(row["lane_id"])
            if stored_lane != normalized_lane_id:
                return _envelope(
                    ok=False,
                    tool=tool,
                    data={
                        "error": ("reclaim-candidate row lane_id does not match query lane_id."),
                        "task_ref": normalized_task_ref,
                        "lane_id": normalized_lane_id,
                        "decision": decision_id,
                        "row_lane_id": (None if row["lane_id"] is None else str(row["lane_id"])),
                    },
                    task_ref=normalized_task_ref,
                    entity="lane",
                )
        return _envelope(
            ok=True,
            tool=tool,
            data={
                "task_ref": normalized_task_ref,
                "lane_id": normalized_lane_id,
                "decision": decision_id,
                "candidate": _row_to_dict(row),
            },
            task_ref=normalized_task_ref,
            entity="lane",
        )


def latest_reclaim_candidate(*, lane_id: str, task_ref: str) -> dict[str, Any]:
    """Return the newest recorded reclaim-candidate decision for a lane, if any.

    Candidate rows use :func:`reclaim_candidate_decision_id` (percent-encoded
    components) written by the orchestrator scan. Re-evaluations reuse a stable
    per-lane session so they collapse onto one row via the
    ``(task_ref, decision, session)`` unique key (newest-wins rationale when the
    writer opts into refresh). ``created_at`` is frozen on that row (creation
    fact); when multiple historical rows still exist, ``id DESC`` is the
    deterministic tie-break after ``created_at DESC``. An absent candidate is
    ``ok=True`` with ``candidate=None`` -- a normal state for lanes that have
    never been scanned, not a read error.

    Both ``lane_id`` and ``task_ref`` are required and keyword-only.
    """
    return _latest_reclaim_candidate(
        lane_id=lane_id,
        task_ref=task_ref,
        decision_id_builder=reclaim_candidate_decision_id,
        tool="latest_reclaim_candidate",
    )


def latest_branch_reclaim_candidate(*, lane_id: str, task_ref: str) -> dict[str, Any]:
    """Return the newest branch-reclaim-candidate decision for a lane, if any.

    The envelope and absence contract exactly match
    :func:`latest_reclaim_candidate`; only the decision-id namespace differs.
    """
    return _latest_reclaim_candidate(
        lane_id=lane_id,
        task_ref=task_ref,
        decision_id_builder=branch_reclaim_candidate_decision_id,
        tool="latest_branch_reclaim_candidate",
    )


def list_lanes(
    *,
    task_ref: str | None = None,
    all_tasks: bool = False,
    status: str = "all",
    limit: int = 100,
    offset: int = 0,
    after_id: int | None = _UNSET,  # type: ignore[assignment]
) -> dict[str, Any]:
    """List lane rows for a task with optional status filter.

    Mode selection is separable from cursor value (PLAN0181-S2KEYSETSEED-01):

    - ``after_id`` omitted (default ``_UNSET``) -> legacy OFFSET paging by
      ``updated_at DESC, id DESC`` so existing callers keep working.
      OFFSET pages never emit ``next_after_id`` (PLAN0181-S2GATE-NEXTAFTERID-OFFSET-01):
      that cursor is only valid under keyset ``ORDER BY id DESC``, not under the
      OFFSET ordering. A keyset sweep must seed with ``after_id=None`` and must
      not start from an OFFSET page's cursor. [API-01][DATA-03]
    - ``after_id=None`` -> keyset first page: ``ORDER BY id DESC``, no id filter.
    - ``after_id`` int -> keyset continuation: ``WHERE id < ? ORDER BY id DESC``.
      Booleans are refused (``bool`` subclasses ``int``) so a wire/JSON true is
      not silently bound as ``id < 1`` (PLAN0181-S2GATE-AFTERID-BOOL-01). [DATA-03]

    Keyset pages fetch limit+1 so ``has_more`` / ``next_after_id`` are
    cursor-derived. Offset paging remains available for existing callers.
    """
    limit = max(1, limit)
    offset = max(0, offset)
    valid_statuses = {"all", *LANE_STATUSES}
    if status not in valid_statuses:
        return _envelope(
            ok=False,
            tool="list_lanes",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"},
            entity="lane",
        )
    with _get_db_connection() as conn:
        if all_tasks and task_ref is not None:
            return _envelope(
                ok=False,
                tool="list_lanes",
                data={"error": "task_ref and all_tasks=True are mutually exclusive."},
                entity="lane",
            )
        resolved_task_ref = None if all_tasks else _resolve_task_ref(conn, task_ref)
        params: list[object] = [] if all_tasks else [resolved_task_ref]
        where_sql = "1 = 1" if all_tasks else "task_ref = ?"
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total = int(
            conn.execute(
                f"SELECT COUNT(*) AS count FROM worktree_lanes WHERE {where_sql}",
                tuple(params),
            ).fetchone()["count"]
        )
        if after_id is not _UNSET:
            # Keyset over immutable id: ORDER BY id DESC.
            # None seeds the first page (no id filter); a real int continues below it.
            # type(x) is int rejects bool (bool subclasses int) so True/False cannot
            # silently become WHERE id < 1 / id < 0 (PLAN0181-S2GATE-AFTERID-BOOL-01).
            # Fetch limit+1 so has_more is derived from the cursor, not OFFSET math.
            if after_id is None:
                fetched = [
                    _row_to_dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM worktree_lanes WHERE {where_sql} ORDER BY id DESC LIMIT ?",
                        (*params, limit + 1),
                    ).fetchall()
                ]
            elif type(after_id) is int:
                keyset_params: list[object] = [*params, after_id]
                fetched = [
                    _row_to_dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM worktree_lanes WHERE {where_sql} AND id < ? ORDER BY id DESC LIMIT ?",
                        (*keyset_params, limit + 1),
                    ).fetchall()
                ]
            else:
                return _envelope(
                    ok=False,
                    tool="list_lanes",
                    data={"error": (f"after_id must be an int or None; got {type(after_id).__name__}.")},
                    entity="lane",
                )
            has_more = len(fetched) > limit
            rows = fetched[:limit]
            # next_after_id is keyset-only: it is an id DESC cursor, not valid for
            # OFFSET order (PLAN0181-S2GATE-NEXTAFTERID-OFFSET-01). [API-01]
            next_after_id = rows[-1]["id"] if rows and has_more else None
        else:
            rows = [
                _row_to_dict(row)
                for row in conn.execute(
                    f"SELECT * FROM worktree_lanes WHERE {where_sql} "
                    f"ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?",
                    (*params, limit, offset),
                ).fetchall()
            ]
            has_more = offset + len(rows) < total
            # OFFSET must not advertise a keyset cursor (PLAN0181-S2GATE-NEXTAFTERID-OFFSET-01).
            next_after_id = None
        return _envelope(
            ok=True,
            tool="list_lanes",
            data={
                "task_ref": resolved_task_ref,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": has_more,
                "next_after_id": next_after_id,
                "lanes": rows,
            },
            entity="lane",
        )


def list_lanes_by_worktree_path(*, worktree_path: str) -> dict[str, Any]:
    """Return every lane row whose stored ``worktree_path`` equals *worktree_path*.

    Cross-task by design: no ``task_ref`` filter. The schema only enforces
    ``UNIQUE(task_ref, lane_id)`` — ``worktree_path`` is not unique — so two
    tasks may legally register the same checkout. Task-scoped ``list_lanes``
    is blind to that collision; reclaim's shared-path guard uses this reader
    for a genuinely global view (PLAN0181-S2GATE2-SHAREDPATH-SCOPE-01).

    Exact string match on the stored column (table scan; no index required).
    Callers that need path-equality across spellings resolve and compare
    themselves after reading.
    """
    normalized_path = _normalize_optional_text(worktree_path)
    if normalized_path is None:
        return _envelope(
            ok=False,
            tool="list_lanes_by_worktree_path",
            data={"error": "worktree_path is required."},
            entity="lane",
        )
    with _get_db_connection() as conn:
        rows = [
            _row_to_dict(row)
            for row in conn.execute(
                "SELECT * FROM worktree_lanes WHERE worktree_path = ? ORDER BY id DESC",
                (normalized_path,),
            ).fetchall()
        ]
        return _envelope(
            ok=True,
            tool="list_lanes_by_worktree_path",
            data={
                "worktree_path": normalized_path,
                "total_matching": len(rows),
                "returned": len(rows),
                "lanes": rows,
            },
            entity="lane",
        )


# Terminal statuses excluded from the non-terminal path-owner scan.
# Matches the worktree_lanes CHECK terminals (closed/merged/closed_stale).
# Shared-path guards that treat additional forward-safety terminals (e.g.
# archived) filter those after reading.
_NONTERMINAL_WORKTREE_PATH_SCAN_TERMINALS = frozenset({"closed", "merged", "closed_stale"})


def list_nonterminal_lanes_with_worktree_path() -> dict[str, Any]:
    """Return all non-terminal lane rows that carry a non-empty ``worktree_path``.

    Cross-task full-table scan for shared-path identity [SECD-03]. Exact-SQL
    ``WHERE worktree_path = ?`` cannot recover a stored symlink alias when the
    query only supplies a differently-resolved spelling; path identity must
    close on the reader side by returning every live path owner so callers
    can normalize both sides in Python (expanduser+resolve with a resolve-
    failure fallback).

    Row volume is small; a teardown-path full scan is acceptable. Does not
    filter by path spelling — callers perform identity comparison.
    """
    terminals = tuple(sorted(_NONTERMINAL_WORKTREE_PATH_SCAN_TERMINALS))
    placeholders = ", ".join("?" for _ in terminals)
    with _get_db_connection() as conn:
        rows = [
            _row_to_dict(row)
            for row in conn.execute(
                f"""
                SELECT * FROM worktree_lanes
                WHERE worktree_path IS NOT NULL
                  AND TRIM(worktree_path) != ''
                  AND status NOT IN ({placeholders})
                ORDER BY id DESC
                """,
                terminals,
            ).fetchall()
        ]
        return _envelope(
            ok=True,
            tool="list_nonterminal_lanes_with_worktree_path",
            data={
                "total_matching": len(rows),
                "returned": len(rows),
                "lanes": rows,
            },
            entity="lane",
        )
