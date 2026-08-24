"""Lanes domain module.

Contains worktree lane management, turn metrics, worker reports, and lane messages.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

from .lanes_support import (
    CLOSEABLE_LANE_STATUSES,
    DEFAULT_BLOCKED_LANE_REAP_BATCH,  # noqa: F401 -- re-exported; cli.py imports it via `from .lanes import`
    _effective_limit,
    _get_db_connection,
    _invalid_sections_error,
    _json_response,
    _normalize_optional_text,
    _normalize_read_detail,
    _paginated_query,
    _parse_projection_fields,
    _parse_sections,
    _project_mapping,
    _resolve_task_ref,
    _row_to_dict,
    _shape_list_payload,
    _summarize_generic_row,
    _summarize_value,
    _truncate_text,
    _workspace_root,
)

_logger = logging.getLogger(__name__)

LANE_MESSAGE_DIRECTIONS = frozenset({"orchestrator_to_worker", "worker_to_orchestrator"})
# closed_stale: schema v27 terminal for blocked-lane reclaimer (internal).
LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "merged", "closed", "closed_stale"})
MESSAGE_STATUSES = frozenset({"open", "acknowledged", "closed"})
REPORT_STATUSES = frozenset({"submitted", "acknowledged", "superseded"})
# Terminal consumption statuses written by acknowledge_worker_report (never 'submitted').
REPORT_ACK_STATUSES = frozenset({"acknowledged", "superseded"})
# "no_actionable_work" is the canonical empty-inbox outcome shared with
# worker_start / run_offload_pass (HARM-A-006); "no_work" is retained as a legacy
# alias so historical worker_reports rows remain valid.
WORKER_REPORT_OUTCOMES = frozenset({"finished", "failed", "exhausted", "stopped", "no_actionable_work", "no_work"})
REVIEW_KINDS = frozenset({"branch", "planning"})

# Named condition when a handoff subprocess would execute a PATH/uv-tool install
# that is not the running package (OFFLOAD-WAVE-LANES-DIE-AT-HANDOFF-ON-THE-STALE-
# INSTALLED-UV-TOOL-01 / CARD-07). Must not surface as generic handoff_subprocess_failed.
INSTALLED_ORCHESTRATOR_VERSION_SKEW = "installed_orchestrator_version_skew"

# OBS-08 / D1: pre-try fallback for the CURRENT_TASK side-effect guard when the
# handoff writer module itself cannot be imported. Distinct from handoff's
# infrastructure / programming error types so a consumer that only checks for a
# non-null error_type never confuses import failure with success or with a
# classified writer failure.
CURRENT_TASK_SIDE_EFFECT_WRITER_UNAVAILABLE_TYPE = "current_task_side_effect_writer_unavailable"


@dataclass
class TokenUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None
    total_tokens: int | None = None
    usage_source: str | None = None


@dataclass
class PromptMetrics:
    model_context_window: int | None = None
    prompt_tokens: int | None = None
    prompt_chars: int | None = None
    prompt_token_source: str | None = None
    utilization_ratio: float | None = None
    domain_signal_ratio: float | None = None
    pressure_level: str | None = None


class WriteActor(TypedDict, total=False):
    agent: str
    model: str
    model_label: str
    reasoning_level: str
    branch: str
    commit_sha: str
    lane_id: str


def _decode_lane_message_row_dict(row: dict[str, object]) -> dict[str, object]:
    payload_json = row.get("payload_json")
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            row["payload"] = payload
    return row


def _decode_turn_metric_row_dict(row: dict[str, object]) -> dict[str, object]:
    for key, empty in (("attribution_json", {}), ("section_sizes_json", {}), ("raw_usage_json", None)):
        raw_value = row.get(key)
        if not isinstance(raw_value, str) or not raw_value.strip():
            row[key.removesuffix("_json")] = empty
            continue
        try:
            row[key.removesuffix("_json")] = json.loads(raw_value)
        except json.JSONDecodeError:
            row[key.removesuffix("_json")] = empty
    return row


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        normalized = _normalize_optional_text(item)
        if normalized is not None:
            result.append(normalized)
    return result


def _normalize_lane_message_payload(payload: object) -> tuple[dict[str, object] | None, str | None]:
    if payload is None:
        return None, None
    if not isinstance(payload, dict):
        return None, "lane message payload must be an object when provided."
    normalized: dict[str, object] = {}
    for key in ("source_lane", "reason", "summary", "dispatch_id"):
        value = _normalize_optional_text(payload.get(key))
        if value is not None:
            normalized[key] = value
    for key in ("required_actions", "artifacts"):
        values = _coerce_string_list(payload.get(key))
        if values:
            normalized[key] = values
    raw_override = payload.get("owned_paths_override")
    if isinstance(raw_override, str):
        raw_override = [raw_override]
    override_values = _coerce_string_list(raw_override)
    if override_values:
        normalized["owned_paths_override"] = override_values
    return normalized, None


def _normalize_path_for_match(path_value: str | Path) -> str:
    return os.path.normcase(str(Path(path_value).expanduser().resolve()))


def running_orchestrator_module_location() -> Path:
    """Return the filesystem directory of the *running* ``workbay_orchestrator_mcp`` package.

    Derived from the imported package (``__file__``), not from PATH or a uv tool
    install. Lane handoff subprocesses must pin to this tree so a version-skewed
    ``~/.local/share/uv/tools/...`` copy cannot steal the write path.
    """
    import workbay_orchestrator_mcp as pkg  # noqa: PLC0415

    return Path(pkg.__file__).resolve().parent


def resolve_handoff_orchestrator_cli(
    *,
    interpreter: str | None = None,
    installed_cli_path: str | Path | None = None,
    allow_installed: bool = False,
    expected_module_location: str | Path | None = None,
) -> dict[str, object]:
    """Resolve interpreter + module argv for the lane handoff orchestrator CLI.

    Primary path: ``sys.executable -m workbay_orchestrator_mcp`` with PYTHONPATH
    pinned to the import root of the *running* package (this process). That is
    the same source tree the parent already imported — never a PATH/uv-tool
    console script.

    If ``allow_installed`` is True and ``installed_cli_path`` points at a CLI
    whose package location differs from the running module, return a *named*
    failure (``installed_orchestrator_version_skew``) that names both the
    expected and found locations. Skew must not collapse into a generic
    ``handoff_subprocess_failed``.

    Returns a stable shape::

        {"ok": True, "argv": [...], "env": {...}, "module_location": str,
         "interpreter": str, "import_root": str}
        # or
        {"ok": False, "condition": "installed_orchestrator_version_skew",
         "error": str, "expected_location": str, "found_location": str}
    """
    running = (
        Path(expected_module_location).resolve()
        if expected_module_location is not None
        else running_orchestrator_module_location()
    )
    import_root = running.parent
    py = str(interpreter or sys.executable)
    argv = [py, "-m", "workbay_orchestrator_mcp"]

    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    pythonpath = str(import_root) if not existing_pythonpath else f"{import_root}{os.pathsep}{existing_pythonpath}"
    env = {
        "WORKBAY_ORCHESTRATOR_PYTHON": py,
        "WORKBAY_ORCHESTRATOR_MODULE_ROOT": str(running),
        "PYTHONPATH": pythonpath,
    }

    if allow_installed and installed_cli_path is not None:
        found = Path(installed_cli_path).expanduser().resolve()
        # A console-script / decoy path is "skewed" when it is not the running
        # interpreter itself and does not live under the running module tree.
        # (We cannot reliably open another install's site-packages from here;
        # location disagreement is the CARD-07 signal.)
        under_running = False
        try:
            found.relative_to(running)
            under_running = True
        except ValueError:
            try:
                found.relative_to(import_root)
                under_running = True
            except ValueError:
                under_running = False
        if found != Path(py).resolve() and not under_running:
            expected = str(running)
            found_s = str(found)
            return {
                "ok": False,
                "condition": INSTALLED_ORCHESTRATOR_VERSION_SKEW,
                "error": (
                    f"{INSTALLED_ORCHESTRATOR_VERSION_SKEW}: installed orchestrator CLI "
                    f"is version-skewed relative to the running package; "
                    f"expected={expected} found={found_s}"
                ),
                "expected_location": expected,
                "found_location": found_s,
            }

    return {
        "ok": True,
        "argv": argv,
        "env": env,
        "module_location": str(running),
        "interpreter": py,
        "import_root": str(import_root),
    }


def handoff_subprocess_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Env mapping for handoff subprocesses: pin CLI to the running package.

    Merges :func:`resolve_handoff_orchestrator_cli` env keys into *base* (or
    ``os.environ``). Callers that shell to ``worktree-lane`` must use this so the
    bash helper prefers ``python -m workbay_orchestrator_mcp`` over a PATH/uv-tool
    install.

    Production always resolves the running package (``allow_installed=False``),
    which returns ``ok=True``. The installed-CLI skew arm lives in
    :func:`resolve_handoff_orchestrator_cli` for explicit probes and in bash
    when a MODULE_ROOT pin cannot open a real package tree — not here (OBS-08).
    """
    resolved = resolve_handoff_orchestrator_cli()
    env = dict(base if base is not None else os.environ)
    # Drop an ambient PATH-style override so a hermetic/test stub or operator
    # WORKBAY_ORCHESTRATOR_CMD cannot re-introduce the uv-tool skew on the
    # handoff write path. Callers that truly need a stub must set it after this.
    env.pop("WORKBAY_ORCHESTRATOR_CMD", None)
    extra = resolved.get("env")
    if isinstance(extra, dict):
        for key, value in extra.items():
            env[str(key)] = str(value)
    return env


def _adapt_lane_envelope(envelope: dict[str, object]) -> dict[str, object]:
    """Map handoff v2 lane envelopes to orchestrator JSON responses.

    Failure branch must preserve machine-readable discriminators from nested
    ``data`` (``error_type``, ``current_task_md_written``, ``lane``, …) so a
    committed dual-write partial failure is distinguishable from a genuine
    no-write refusal (DATA-01 / OBS-04). Root-level ``warnings`` must also
    reach the client on both success and failure (OBS-04) — they live on the
    envelope root, not inside ``data``.
    """
    if envelope.get("schema_version") == 2:
        data = envelope.get("data")
        if not isinstance(data, dict):
            data = {}
        warnings = envelope.get("warnings")
        if envelope.get("ok"):
            result: dict[str, object] = {"ok": True, **data}
            if isinstance(warnings, list) and warnings:
                result["warnings"] = warnings
            return _json_response(result)
        # Keep error key for legacy readers; spread the rest so discriminators
        # survive rather than being dropped to a bare {ok, error} dict.
        error = data.get("error", "lane operation failed")
        result = {"ok": False, **data, "error": error}
        if isinstance(warnings, list) and warnings:
            result["warnings"] = warnings
        return _json_response(result)
    return _json_response(envelope)


def _resolve_current_lane_row(conn: sqlite3.Connection, task_ref: str) -> dict[str, object] | None:
    del conn
    from workbay_handoff_mcp.lanes_api import list_lanes  # noqa: PLC0415

    workspace_path = _normalize_path_for_match(_workspace_root())
    listed = list_lanes(task_ref=task_ref, status="all", limit=1000, offset=0)
    if not listed.get("ok"):
        return None
    data = listed.get("data") if isinstance(listed.get("data"), dict) else listed
    lanes = data.get("lanes") if isinstance(data, dict) else None
    if not isinstance(lanes, list):
        return None
    for row in lanes:
        if not isinstance(row, dict):
            continue
        raw_path = _normalize_optional_text(row.get("worktree_path"))
        if raw_path is None:
            continue
        if _normalize_path_for_match(raw_path) == workspace_path:
            return row
    return None


def _fetch_handoff_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    where_sql: str,
    order_sql: str,
    limit: int,
    params: tuple[object, ...],
) -> list[dict[str, object]]:
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE {where_sql} ORDER BY {order_sql} LIMIT ?",
        (*params, limit),
    ).fetchall()
    payload = [dict(row) for row in rows]
    if table == "lane_messages":
        return [_decode_lane_message_row_dict(row) for row in payload]
    if table == "turn_metrics":
        return [_decode_turn_metric_row_dict(row) for row in payload]
    return payload


def _excerpt_text(value: str | None, *, limit: int = 240) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    collapsed = " ".join(normalized.split())
    if len(collapsed) <= limit:
        return collapsed
    if limit <= 3:
        return "." * limit
    return f"{collapsed[: limit - 3].rstrip()}..."


def _count_by_value(
    conn: sqlite3.Connection,
    *,
    table: str,
    field: str,
    task_ref: str,
    lane_id: str,
    allowed_values: frozenset[str],
) -> dict[str, int]:
    counts = {value: 0 for value in sorted(allowed_values)}
    rows = conn.execute(
        f"SELECT {field} AS value, COUNT(*) AS count FROM {table} WHERE task_ref = ? AND lane_id = ? GROUP BY {field}",
        (task_ref, lane_id),
    ).fetchall()
    for row in rows:
        value = _normalize_optional_text(row["value"])
        if value is not None and value in counts:
            counts[value] = int(row["count"])
    return counts


def _build_archival_lane_activity_summary(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
) -> dict[str, object]:
    decisions_total_row = conn.execute(
        "SELECT COUNT(*) AS count FROM decisions WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    latest_decision_row = conn.execute(
        """
        SELECT rationale
        FROM decisions
        WHERE task_ref = ? AND lane_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (task_ref, lane_id),
    ).fetchone()
    reports_total_row = conn.execute(
        "SELECT COUNT(*) AS count FROM worker_reports WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    latest_report_row = conn.execute(
        """
        SELECT merge_ready
        FROM worker_reports
        WHERE task_ref = ? AND lane_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (task_ref, lane_id),
    ).fetchone()
    tests_summary_row = conn.execute(
        """
        SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed
        FROM verified_tests
        WHERE task_ref = ? AND lane_id = ?
        """,
        (task_ref, lane_id),
    ).fetchone()
    tests_total = int(tests_summary_row["total"]) if tests_summary_row else 0
    tests_passed = int(tests_summary_row["passed"]) if tests_summary_row else 0
    return {
        "decisions": {
            "count": int(decisions_total_row["count"]) if decisions_total_row else 0,
            "latest_rationale_excerpt": _excerpt_text(
                str(latest_decision_row["rationale"])
                if latest_decision_row and latest_decision_row["rationale"] is not None
                else None
            ),
        },
        "findings": {
            "counts_by_status": _count_by_value(
                conn,
                table="review_findings",
                field="status",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=frozenset({"open", "fixed", "wontfix", "deferred", "resolved_on_branch", "integrated"}),
            ),
        },
        "reports": {
            "count": int(reports_total_row["count"]) if reports_total_row else 0,
            "latest_merge_ready": (
                bool(latest_report_row["merge_ready"])
                if latest_report_row is not None and latest_report_row["merge_ready"] is not None
                else None
            ),
            "counts_by_outcome": _count_by_value(
                conn,
                table="worker_reports",
                field="outcome",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=WORKER_REPORT_OUTCOMES,
            ),
        },
        "messages": {
            "counts_by_direction": _count_by_value(
                conn,
                table="lane_messages",
                field="direction",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=LANE_MESSAGE_DIRECTIONS,
            ),
            "counts_by_status": _count_by_value(
                conn,
                table="lane_messages",
                field="status",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=MESSAGE_STATUSES,
            ),
        },
        "tests": {
            "total": tests_total,
            "passed": tests_passed,
            "pass_rate": round(tests_passed / tests_total, 3) if tests_total else None,
        },
    }


def _resolve_write_actor(
    conn: sqlite3.Connection,
    actor: WriteActor | None,
    *,
    task_ref: str | None = None,
    allow_missing_worktree_fallback: bool = False,
    derive_worktree_for_branch: str | None = None,
):
    from workbay_handoff_mcp.shared_write_context import (
        _resolve_write_actor as _handoff_resolve_write_actor,  # noqa: PLC0415
    )

    return _handoff_resolve_write_actor(
        conn,
        actor,
        task_ref=task_ref,
        allow_missing_worktree_fallback=allow_missing_worktree_fallback,
        derive_worktree_for_branch=derive_worktree_for_branch,
    )


_LANE_MESSAGE_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "status"})
_TURN_METRIC_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "session", "phase", "backend", "model"})
_WORKER_REPORT_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "session", "status", "merge_ready", "outcome"})
_LANE_ACTIVITY_LANE_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "status", "title", "objective"})
_LANE_ACTIVITY_DECISION_IDENTITY_FIELDS = frozenset({"id", "decision", "created_at"})
_LANE_ACTIVITY_TEST_IDENTITY_FIELDS = frozenset({"id", "command", "passed", "verified_at"})
_LANE_ACTIVITY_BLOCKER_IDENTITY_FIELDS = frozenset({"id", "description", "status", "created_at"})
_LANE_ACTIVITY_ACTION_IDENTITY_FIELDS = frozenset({"id", "action", "status", "priority", "updated_at"})
_LANE_ACTIVITY_FINDING_IDENTITY_FIELDS = frozenset({"id", "title", "severity", "status", "created_at"})


def _get_lane_row(conn: sqlite3.Connection, task_ref: str, lane_id: str) -> dict[str, object] | None:
    del conn
    from workbay_handoff_mcp.lanes_api import get_lane  # noqa: PLC0415

    envelope = get_lane(lane_id=lane_id, task_ref=task_ref)
    if not envelope.get("ok"):
        return None
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else envelope
    lane = data.get("lane") if isinstance(data, dict) else None
    return lane if isinstance(lane, dict) else None


def _summarize_turn_metric_row(row: dict[str, object]) -> dict[str, object]:
    summarized = dict(row)
    summarized.pop("attribution_json", None)
    summarized.pop("section_sizes_json", None)
    summarized.pop("raw_usage_json", None)
    for key in ("attribution", "section_sizes", "raw_usage"):
        if key in summarized:
            summarized[key] = _summarize_value(summarized.get(key))
    return summarized


def _summarize_worker_report_row(row: dict[str, object]) -> dict[str, object]:
    summarized = dict(row)
    summarized.pop("changed_files_json", None)
    summarized.pop("test_commands_json", None)
    summarized.pop("blockers_json", None)
    return summarized


def _summarize_lane_message_row(row: dict[str, object]) -> dict[str, object]:
    summarized = dict(row)
    summarized["message"] = _truncate_text(summarized.get("message"), 240)
    summarized.pop("payload_json", None)
    if "payload" in summarized:
        summarized["payload"] = _summarize_value(summarized.get("payload"))
    return summarized


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def upsert_worktree_lane(
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
) -> dict:
    valid_statuses = LANE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_path = _normalize_optional_text(worktree_path)
    normalized_branch = _normalize_optional_text(branch)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if normalized_path is None:
        return _json_response({"ok": False, "error": "worktree_path is required."})
    if normalized_branch is None:
        return _json_response({"ok": False, "error": "branch is required."})
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    from workbay_handoff_mcp.lanes_api import open_lane  # noqa: PLC0415

    return _adapt_lane_envelope(
        open_lane(
            lane_id=normalized_lane_id,
            worktree_path=normalized_path,
            branch=normalized_branch,
            title=title,
            objective=objective,
            owner_agent=owner_agent,
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            test_cmd=test_cmd,
            lane_kind=lane_kind,
            status=status,
            notes=notes,
            task_ref=task_ref,
        )
    )


def close_worktree_lane(
    lane_id: str,
    status: str = "closed",
    notes: str | None = None,
    task_ref: str | None = None,
) -> dict:
    """Transition a worktree lane to closed or merged status in the handoff database."""
    valid_close_statuses = CLOSEABLE_LANE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if status not in valid_close_statuses:
        return _json_response(
            {"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_close_statuses))}"}
        )
    from workbay_handoff_mcp.lanes_api import close_lane  # noqa: PLC0415

    return _adapt_lane_envelope(
        close_lane(
            lane_id=normalized_lane_id,
            status=status,
            notes=notes,
            task_ref=task_ref,
        )
    )


def list_worktree_lanes(task_ref: str | None = None, status: str = "all", limit: int = 100, offset: int = 0) -> dict:
    limit = max(1, limit)
    offset = max(0, offset)
    valid_statuses = {"all", *LANE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    from workbay_handoff_mcp.lanes_api import list_lanes  # noqa: PLC0415

    return _adapt_lane_envelope(list_lanes(task_ref=task_ref, status=status, limit=limit, offset=offset))


def manage_worktree_lane(
    operation: str,
    lane_id: str | None = None,
    worktree_path: str | None = None,
    branch: str | None = None,
    title: str | None = None,
    objective: str | None = None,
    owner_agent: str | None = None,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    test_cmd: str | None = None,
    lane_kind: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    task_ref: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Discriminated wrapper for worktree lane upsert, close, and list operations."""
    valid_operations = {"close", "list", "upsert"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "upsert":
        return upsert_worktree_lane(
            lane_id=str(lane_id or ""),
            worktree_path=str(worktree_path or ""),
            branch=str(branch or ""),
            title=title,
            objective=objective,
            owner_agent=owner_agent,
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            test_cmd=test_cmd,
            lane_kind=lane_kind,
            status=status or "planned",
            notes=notes,
            task_ref=task_ref,
        )
    if operation == "close":
        return close_worktree_lane(
            lane_id=str(lane_id or ""),
            status=status or "closed",
            notes=notes,
            task_ref=task_ref,
        )
    return list_worktree_lanes(
        task_ref=task_ref,
        status=status or "all",
        limit=limit,
        offset=offset,
    )


def record_turn_metric(
    session: str,
    phase: str,
    backend: str,
    cycle: int | None = None,
    lane_id: str | None = None,
    model: str | None = None,
    thread_id: str | None = None,
    turn_id: str | None = None,
    token_usage: TokenUsage | None = None,
    prompt_metrics: PromptMetrics | None = None,
    attribution: dict[str, Any] | None = None,
    section_sizes: dict[str, Any] | None = None,
    raw_usage: dict[str, Any] | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    duration_seconds: float | None = None,
    replace_prior_same_phase: bool = False,
) -> dict:
    if _normalize_optional_text(session) is None:
        return _json_response({"ok": False, "error": "session is required."})
    normalized_phase = _normalize_optional_text(phase)
    if normalized_phase is None:
        return _json_response({"ok": False, "error": "phase is required."})
    normalized_backend = _normalize_optional_text(backend)
    if normalized_backend is None:
        return _json_response({"ok": False, "error": "backend is required."})
    resolved_usage_source = token_usage.usage_source if token_usage else None
    resolved_prompt_token_source = prompt_metrics.prompt_token_source if prompt_metrics else None
    # usage_source allows grok_context_delta (implementation note S2 / PR-0094-01); prompt
    # token sources remain the observed/estimate set only (different unit).
    from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (  # noqa: PLC0415
        USAGE_SOURCE_GROK_CONTEXT_DELTA,
    )

    valid_usage_sources = {
        "observed",
        "tokenizer_estimate",
        "char_estimate",
        USAGE_SOURCE_GROK_CONTEXT_DELTA,
    }
    valid_prompt_sources = {"observed", "tokenizer_estimate", "char_estimate"}
    if resolved_usage_source is not None and resolved_usage_source not in valid_usage_sources:
        return _json_response({"ok": False, "error": "Invalid usage_source."})
    if resolved_prompt_token_source is not None and resolved_prompt_token_source not in valid_prompt_sources:
        return _json_response({"ok": False, "error": "Invalid prompt_token_source."})
    resolved_duration: float | None = None
    if duration_seconds is not None:
        try:
            resolved_duration = float(duration_seconds)
        except (TypeError, ValueError):
            return _json_response({"ok": False, "error": "duration_seconds must be a number."})
        if resolved_duration < 0:
            return _json_response({"ok": False, "error": "duration_seconds must be non-negative."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor)
        resolved_lane_id = _normalize_optional_text(lane_id) or ctx.lane_id
        # Latest-wins for lane_prep (and any future same-phase replace). The DELETE and the
        # INSERT below execute on the same connection inside one `_get_db_connection()` block,
        # which opens a single implicit transaction committed together on block exit — and
        # rolled back together on any error, so a failed INSERT never leaves a delete-without-
        # replacement (the prior row survives; latest-wins degrades to prior-wins, never loss).
        # That one transaction, plus SQLite WAL single-writer serialization (busy_timeout
        # bounded), is what makes concurrent prep writers converge on exactly one row — no
        # duplicate can be left in a window between a *separate* DELETE and INSERT (CON-11).
        # Contract: replace collapses ALL prior rows for (task_ref, lane_id, phase) regardless
        # of session/cycle — it means "exactly one durable row per (task_ref, lane_id, phase)",
        # so do not enable it for a phase whose per-session history must be retained. Scope is
        # strictly per lane: when no lane_id is resolvable we intentionally skip the prune rather
        # than issue a lane-less DELETE that would over-broadly clear every lane's same-phase
        # rows for the task (a documented no-op without a lane).
        if replace_prior_same_phase and resolved_lane_id is not None:
            conn.execute(
                "DELETE FROM turn_metrics WHERE task_ref = ? AND lane_id = ? AND phase = ?",
                (resolved_task_ref, resolved_lane_id, normalized_phase),
            )
        cur = conn.execute(
            """
            INSERT INTO turn_metrics (
                task_ref, lane_id, session, cycle, phase, backend, model, thread_id, turn_id,
                input_tokens, output_tokens, cached_input_tokens, reasoning_output_tokens,
                total_tokens, usage_source, model_context_window, prompt_tokens, prompt_chars,
                prompt_token_source, utilization_ratio, domain_signal_ratio, pressure_level,
                attribution_json, section_sizes_json, raw_usage_json, duration_seconds, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                resolved_lane_id,
                session,
                cycle,
                normalized_phase,
                normalized_backend,
                _normalize_optional_text(model),
                _normalize_optional_text(thread_id),
                _normalize_optional_text(turn_id),
                token_usage.input_tokens if token_usage else None,
                token_usage.output_tokens if token_usage else None,
                token_usage.cached_input_tokens if token_usage else None,
                token_usage.reasoning_output_tokens if token_usage else None,
                token_usage.total_tokens if token_usage else None,
                resolved_usage_source,
                prompt_metrics.model_context_window if prompt_metrics else None,
                prompt_metrics.prompt_tokens if prompt_metrics else None,
                prompt_metrics.prompt_chars if prompt_metrics else None,
                resolved_prompt_token_source,
                prompt_metrics.utilization_ratio if prompt_metrics else None,
                prompt_metrics.domain_signal_ratio if prompt_metrics else None,
                _normalize_optional_text(prompt_metrics.pressure_level if prompt_metrics else None),
                json.dumps(attribution or {}, sort_keys=True),
                json.dumps(section_sizes or {}, sort_keys=True),
                json.dumps(raw_usage, sort_keys=True) if raw_usage is not None else None,
                resolved_duration,
            ),
        )
        row = conn.execute("SELECT * FROM turn_metrics WHERE id = ?", (cur.lastrowid,)).fetchone()
        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "turn_metric": _decode_turn_metric_row_dict(_row_to_dict(row) or {}),
            }
        )


def list_turn_metrics(
    task_ref: str | None = None,
    lane_id: str | None = None,
    backend: str | None = None,
    model: str | None = None,
    phase: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_turn_metrics: int | None = None,
) -> dict:
    limit = _effective_limit(limit, top_n_turn_metrics)
    offset = max(0, offset)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        normalized_phase = _normalize_optional_text(phase)
        for field_name, value in (
            ("lane_id", _normalize_optional_text(lane_id)),
            ("backend", _normalize_optional_text(backend)),
            ("model", _normalize_optional_text(model)),
            ("phase", normalized_phase),
        ):
            if value is None:
                continue
            where_sql += f" AND {field_name} = ?"
            params.append(value)
        # Default list excludes lane_prep footprint telemetry so agent-turn windows
        # stay readable; pass phase='lane_prep' to opt in (internal).
        if normalized_phase is None:
            where_sql += " AND COALESCE(phase, '') != 'lane_prep'"
        total, rows = _paginated_query(
            conn,
            "turn_metrics",
            where_sql,
            tuple(params),
            limit,
            offset,
            "created_at DESC, id DESC",
            _decode_turn_metric_row_dict,
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": _normalize_optional_text(lane_id),
                    "backend": _normalize_optional_text(backend),
                    "model": _normalize_optional_text(model),
                    "phase": _normalize_optional_text(phase),
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "turn_metrics": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="turn_metrics",
                identity_fields=_TURN_METRIC_IDENTITY_FIELDS,
                summary_fn=_summarize_turn_metric_row,
            )
        )


def get_turn_metrics_summary(
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> dict:
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        normalized_lane_id = _normalize_optional_text(lane_id)
        params: list[object] = [resolved_task_ref]
        # Exclude per-lane-prep footprint rows (phase='lane_prep', internal-
        # FOOTPRINT-01 S2): they are token-less observability rows self-written by the
        # short-lived lane_prompt subprocess, NOT agent execution turns. Counting them
        # in total_turns / usage coverage would systematically bias every agent-turn
        # summary and the offload burn-scoring reads (COALESCE keeps any legacy
        # NULL-phase rows in the agent-turn population).
        where_sql = "task_ref = ? AND COALESCE(phase, '') != 'lane_prep'"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)

        rows = conn.execute(
            f"""
            SELECT usage_source, prompt_token_source, pressure_level, backend, model, lane_id,
                   total_tokens, prompt_tokens, input_tokens, output_tokens
            FROM turn_metrics
            WHERE {where_sql}
            """,
            tuple(params),
        ).fetchall()
        total_turns = len(rows)
        from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (  # noqa: PLC0415
            USAGE_SOURCE_GROK_CONTEXT_DELTA,
        )
        from workbay_orchestrator_mcp.orchestration.model_prices import (  # noqa: PLC0415
            lookup_price,
            price_key,
        )

        usage_counts = {
            "observed": 0,
            "tokenizer_estimate": 0,
            "char_estimate": 0,
            USAGE_SOURCE_GROK_CONTEXT_DELTA: 0,
        }
        prompt_counts = {"observed": 0, "tokenizer_estimate": 0, "char_estimate": 0}
        pressure_counts: dict[str, int] = {}
        tokens_by_lane: dict[str, int] = {}
        tokens_by_backend_model: dict[str, int] = {}
        tokens_by_usage_source: dict[str, int] = {
            "observed": 0,
            "tokenizer_estimate": 0,
            "char_estimate": 0,
            USAGE_SOURCE_GROK_CONTEXT_DELTA: 0,
        }
        prompt_tokens_total = 0
        total_tokens_total = 0
        comparable_turns = 0
        exact_preflight_turns = 0
        estimated_preflight_turns = 0
        drift_sum = 0
        abs_drift_sum = 0
        max_abs_drift = 0
        # implementation note S1: read-time derived USD cost (never stored per row).
        cost_by_backend_model: dict[str, float] = {}
        cost_approximate_by_backend_model: dict[str, float] = {}
        unpriced_turns = 0
        unpriced_by_reason: dict[str, int] = {
            "missing_price": 0,
            "no_usable_tokens": 0,
            "grok_context_delta_only": 0,
        }
        priced_turns = 0
        total_cost_usd = 0.0
        total_cost_approximate_usd = 0.0

        for row in rows:
            usage = row["usage_source"]
            prompt_source = row["prompt_token_source"]
            pressure_level = row["pressure_level"] or "unknown"
            lane_key = str(row["lane_id"] or "unscoped")
            backend_model_key = price_key(row["backend"], row["model"])
            if isinstance(usage, str) and usage in usage_counts:
                usage_counts[usage] += 1
            if isinstance(prompt_source, str) and prompt_source in prompt_counts:
                prompt_counts[prompt_source] += 1
            pressure_counts[str(pressure_level)] = pressure_counts.get(str(pressure_level), 0) + 1
            total_tokens_value = int(row["total_tokens"] or 0)
            prompt_tokens_value = int(row["prompt_tokens"] or 0)
            input_tokens_value = row["input_tokens"]
            output_tokens_value = row["output_tokens"]
            # PR-0094-05: grok_context_delta is cumulative context fill — a different
            # unit from observed input/output. Label/bucket it; never sum into the
            # observed-style totals (total_tokens / by_lane / by_backend_model).
            if isinstance(usage, str) and usage in tokens_by_usage_source:
                tokens_by_usage_source[usage] += total_tokens_value
            if usage == USAGE_SOURCE_GROK_CONTEXT_DELTA:
                # S1: context-delta-only rows are unpriced until S2 lands splits.
                unpriced_turns += 1
                unpriced_by_reason["grok_context_delta_only"] += 1
                continue
            total_tokens_total += total_tokens_value
            prompt_tokens_total += prompt_tokens_value
            tokens_by_lane[lane_key] = tokens_by_lane.get(lane_key, 0) + total_tokens_value
            tokens_by_backend_model[backend_model_key] = (
                tokens_by_backend_model.get(backend_model_key, 0) + total_tokens_value
            )
            if prompt_tokens_value > 0 and input_tokens_value is not None:
                comparable_turns += 1
                drift = int(input_tokens_value) - prompt_tokens_value
                drift_sum += drift
                abs_drift = abs(drift)
                abs_drift_sum += abs_drift
                if abs_drift > max_abs_drift:
                    max_abs_drift = abs_drift
                if prompt_source == "observed":
                    exact_preflight_turns += 1
                elif isinstance(prompt_source, str):
                    estimated_preflight_turns += 1

            # Derived cost: input_per_mtok * (input if observed else prompt-est)
            # + output_per_mtok * output_tokens. Estimate-fed terms are approximate.
            price = lookup_price(row["backend"], row["model"])
            if price is None:
                unpriced_turns += 1
                unpriced_by_reason["missing_price"] += 1
                continue
            input_term: float | None = None
            input_is_estimate = False
            if input_tokens_value is not None:
                try:
                    input_term = float(int(input_tokens_value))
                except (TypeError, ValueError):
                    input_term = None
            if input_term is None and prompt_tokens_value > 0:
                input_term = float(prompt_tokens_value)
                input_is_estimate = True
            output_term: float | None = None
            if output_tokens_value is not None:
                try:
                    output_term = float(int(output_tokens_value))
                except (TypeError, ValueError):
                    output_term = None
            if input_term is None and output_term is None:
                unpriced_turns += 1
                unpriced_by_reason["no_usable_tokens"] += 1
                continue
            row_cost = 0.0
            if input_term is not None:
                row_cost += price.input_per_mtok * (input_term / 1_000_000.0)
            if output_term is not None:
                row_cost += price.output_per_mtok * (output_term / 1_000_000.0)
            priced_turns += 1
            total_cost_usd += row_cost
            cost_by_backend_model[backend_model_key] = cost_by_backend_model.get(backend_model_key, 0.0) + row_cost
            if input_is_estimate:
                total_cost_approximate_usd += row_cost
                cost_approximate_by_backend_model[backend_model_key] = (
                    cost_approximate_by_backend_model.get(backend_model_key, 0.0) + row_cost
                )

        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "summary": {
                    "total_turns": total_turns,
                    "usage_source_counts": usage_counts,
                    "prompt_token_source_counts": prompt_counts,
                    "pressure_level_counts": pressure_counts,
                    "total_tokens": total_tokens_total,
                    "prompt_tokens": prompt_tokens_total,
                    "by_lane_total_tokens": tokens_by_lane,
                    "by_backend_model_total_tokens": tokens_by_backend_model,
                    "total_tokens_by_usage_source": tokens_by_usage_source,
                    "preflight_observed_drift": {
                        "comparable_turns": comparable_turns,
                        "exact_preflight_turns": exact_preflight_turns,
                        "estimated_preflight_turns": estimated_preflight_turns,
                        "net_token_drift": drift_sum,
                        "mean_signed_token_drift": (
                            round(drift_sum / comparable_turns, 3) if comparable_turns else None
                        ),
                        "mean_absolute_token_drift": (
                            round(abs_drift_sum / comparable_turns, 3) if comparable_turns else None
                        ),
                        "max_absolute_token_drift": max_abs_drift if comparable_turns else None,
                    },
                    "derived_cost": {
                        "priced_turns": priced_turns,
                        "unpriced_turns": unpriced_turns,
                        "unpriced_by_reason": unpriced_by_reason,
                        "total_cost_usd": round(total_cost_usd, 6),
                        "total_cost_approximate_usd": round(total_cost_approximate_usd, 6),
                        "by_backend_model_usd": {
                            key: round(value, 6) for key, value in sorted(cost_by_backend_model.items())
                        },
                        "by_backend_model_approximate_usd": {
                            key: round(value, 6) for key, value in sorted(cost_approximate_by_backend_model.items())
                        },
                    },
                },
            }
        )


def get_lane_activity(
    lane_id: str,
    task_ref: str | None = None,
    limit_decisions: int = 20,
    limit_tests: int = 20,
    limit_blockers: int = 20,
    limit_actions: int = 20,
    limit_findings: int = 20,
    limit_reports: int = 20,
    limit_messages: int = 20,
    format: str = "full",
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_decisions: int | None = None,
    top_n_tests: int | None = None,
    top_n_blockers: int | None = None,
    top_n_actions: int | None = None,
    top_n_findings: int | None = None,
    top_n_reports: int | None = None,
    top_n_messages: int | None = None,
) -> dict:
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if format not in {"full", "archival"}:
        return _json_response({"ok": False, "error": "Invalid format. Valid: archival, full."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        lane = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if lane is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        requested_fields = _parse_projection_fields(fields)
        if format == "archival":
            summary = _build_archival_lane_activity_summary(
                conn,
                task_ref=resolved_task_ref,
                lane_id=normalized_lane_id,
            )
            valid_sections = frozenset({"identity", "lane", "summary"})
            requested_sections = _parse_sections(sections, valid_sections)
            if sections is not None and requested_sections == frozenset():
                return _json_response(_invalid_sections_error(valid_sections))
            archival_sections: frozenset[str] = requested_sections or valid_sections
            archival_payload: dict[str, object] = {"ok": True, "task_ref": resolved_task_ref, "format": format}
            if "identity" in archival_sections or "lane" in archival_sections:
                archival_payload["lane"] = _project_mapping(
                    dict(lane), requested_fields, _LANE_ACTIVITY_LANE_IDENTITY_FIELDS
                )
            if "summary" in archival_sections:
                archival_payload["summary"] = summary
            return _json_response(archival_payload)

        detail = _normalize_read_detail(detail)
        valid_sections = frozenset(
            {"identity", "lane", "decisions", "tests", "blockers", "actions", "findings", "reports", "messages"}
        )
        requested_sections = _parse_sections(sections, valid_sections)
        if sections is not None and requested_sections == frozenset():
            return _json_response(_invalid_sections_error(valid_sections))
        activity_sections: frozenset[str] = requested_sections or valid_sections
        activity_payload: dict[str, object] = {"ok": True, "task_ref": resolved_task_ref, "format": format}
        if "identity" in activity_sections or "lane" in activity_sections:
            lane_row = _summarize_generic_row(dict(lane)) if detail == "summary" else dict(lane)
            activity_payload["lane"] = _project_mapping(lane_row, requested_fields, _LANE_ACTIVITY_LANE_IDENTITY_FIELDS)

        section_fetchers: dict[str, Callable[[], list[dict[str, object]]]] = {
            "decisions": lambda: _fetch_handoff_rows(
                conn,
                table="decisions",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="created_at DESC, id DESC",
                limit=_effective_limit(limit_decisions, top_n_decisions),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "tests": lambda: _fetch_handoff_rows(
                conn,
                table="verified_tests",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="verified_at DESC, id DESC",
                limit=_effective_limit(limit_tests, top_n_tests),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "blockers": lambda: _fetch_handoff_rows(
                conn,
                table="blockers",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="created_at DESC, id DESC",
                limit=_effective_limit(limit_blockers, top_n_blockers),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "actions": lambda: _fetch_handoff_rows(
                conn,
                table="next_actions",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="updated_at DESC, id DESC",
                limit=_effective_limit(limit_actions, top_n_actions),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "findings": lambda: _fetch_handoff_rows(
                conn,
                table="review_findings",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="COALESCE(updated_at, created_at) DESC, id DESC",
                limit=_effective_limit(limit_findings, top_n_findings),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "reports": lambda: _fetch_handoff_rows(
                conn,
                table="worker_reports",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="created_at DESC, id DESC",
                limit=_effective_limit(limit_reports, top_n_reports),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "messages": lambda: _fetch_handoff_rows(
                conn,
                table="lane_messages",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="updated_at DESC, id DESC",
                limit=_effective_limit(limit_messages, top_n_messages),
                params=(resolved_task_ref, normalized_lane_id),
            ),
        }

        section_specs: tuple[tuple[str, frozenset[str], Callable[[dict[str, object]], dict[str, object]]], ...] = (
            ("decisions", _LANE_ACTIVITY_DECISION_IDENTITY_FIELDS, _summarize_generic_row),
            ("tests", _LANE_ACTIVITY_TEST_IDENTITY_FIELDS, _summarize_generic_row),
            ("blockers", _LANE_ACTIVITY_BLOCKER_IDENTITY_FIELDS, _summarize_generic_row),
            ("actions", _LANE_ACTIVITY_ACTION_IDENTITY_FIELDS, _summarize_generic_row),
            ("findings", _LANE_ACTIVITY_FINDING_IDENTITY_FIELDS, _summarize_generic_row),
            ("reports", _WORKER_REPORT_IDENTITY_FIELDS, _summarize_worker_report_row),
            ("messages", _LANE_MESSAGE_IDENTITY_FIELDS, _summarize_lane_message_row),
        )
        for section_name, identity_fields, summary_fn in section_specs:
            if section_name not in activity_sections:
                continue
            rows = section_fetchers[section_name]()
            shaped_rows: list[dict[str, object]] = []
            for row in rows:
                summarized = summary_fn(row) if detail == "summary" else dict(row)
                shaped_rows.append(_project_mapping(summarized, requested_fields, identity_fields))
            activity_payload[section_name] = shaped_rows
        return _json_response(activity_payload)


def get_latest_slice_review_packet(
    task_ref: str | None = None,
    lane_id: str | None = None,
    review_kind: str | None = None,
    slice_decision_id: str | None = None,
    slice_label: str | None = None,
) -> dict:
    """Return one slice review packet for ``task_ref`` (latest by default).

    ``slice_decision_id`` selects a specific historical slice and matches the
    ``decision`` id **string** (e.g. ``cdx_slice_complete_<work>_<slug>``) — NOT
    the numeric ``decision_id`` returned inside a packet. Pass the projected
    ``decision`` value from ``search_handoff(decision_fields=["decision"])``;
    passing the numeric id resolves nothing and returns ``ok=False`` (it does
    not silently fall back to the latest slice). ``slice_label`` is the
    alternative selector; supplying both is rejected. With neither selector the
    latest ``slice_complete_*`` packet is returned.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_review_kind = _normalize_optional_text(review_kind)
    normalized_slice_decision_id = _normalize_optional_text(slice_decision_id)
    normalized_slice_label = _normalize_optional_text(slice_label)
    if normalized_review_kind is not None and normalized_review_kind not in REVIEW_KINDS:
        valid_review_kinds = ", ".join(sorted(REVIEW_KINDS))
        return _json_response({"ok": False, "error": f"Invalid review_kind. Valid: {valid_review_kinds}."})
    if normalized_slice_decision_id is not None and normalized_slice_label is not None:
        return _json_response(
            {
                "ok": False,
                "error": "Provide only one of slice_decision_id or slice_label, not both.",
            }
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        from .orchestration.slice_review_packet import get_latest_slice_review_packet_data  # noqa: PLC0415

        packet = get_latest_slice_review_packet_data(
            conn,
            workspace_root=_workspace_root(),
            task_ref=resolved_task_ref,
            lane_id=normalized_lane_id,
            review_kind=normalized_review_kind,
            slice_decision_id=normalized_slice_decision_id,
            slice_label=normalized_slice_label,
        )
        if packet is None:
            return _json_response(
                {
                    "ok": False,
                    "error": "No matching slice review packet found.",
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "review_kind": normalized_review_kind,
                    "slice_decision_id": normalized_slice_decision_id,
                    "slice_label": normalized_slice_label,
                }
            )
        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "review_kind": normalized_review_kind or packet["review_kind"],
                "slice_decision_id": normalized_slice_decision_id,
                "slice_label": normalized_slice_label,
                "packet": packet,
            }
        )


def turn_metrics(
    operation: str,
    session: str | None = None,
    phase: str | None = None,
    backend: str | None = None,
    cycle: int | None = None,
    lane_id: str | None = None,
    model: str | None = None,
    thread_id: str | None = None,
    turn_id: str | None = None,
    token_usage: TokenUsage | None = None,
    prompt_metrics: PromptMetrics | None = None,
    attribution: dict[str, Any] | None = None,
    section_sizes: dict[str, Any] | None = None,
    raw_usage: dict[str, Any] | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    duration_seconds: float | None = None,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_turn_metrics: int | None = None,
) -> dict:
    """Discriminated wrapper for turn metric record, list, and summary operations."""
    valid_operations = {"list", "record", "summary"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "record":
        return record_turn_metric(
            session=str(session or ""),
            phase=str(phase or ""),
            backend=str(backend or ""),
            cycle=cycle,
            lane_id=lane_id,
            model=model,
            thread_id=thread_id,
            turn_id=turn_id,
            token_usage=token_usage,
            prompt_metrics=prompt_metrics,
            attribution=attribution,
            section_sizes=section_sizes,
            raw_usage=raw_usage,
            actor=actor,
            task_ref=task_ref,
            duration_seconds=duration_seconds,
        )
    if operation == "list":
        return list_turn_metrics(
            task_ref=task_ref,
            lane_id=lane_id,
            backend=backend,
            model=model,
            phase=phase,
            limit=limit,
            offset=offset,
            sections=sections,
            detail=detail,
            fields=fields,
            top_n_turn_metrics=top_n_turn_metrics,
        )
    return get_turn_metrics_summary(task_ref=task_ref, lane_id=lane_id)


def record_worker_report(
    lane_id: str,
    session: str,
    summary: str,
    changed_files: list[str] | None = None,
    test_commands: list[str] | None = None,
    blockers: list[str] | None = None,
    merge_ready: bool = False,
    status: str = "submitted",
    outcome: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    valid_statuses = REPORT_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    raw_outcome = _normalize_optional_text(outcome)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    # implementation note S1: every NEW worker_reports row carries a non-NULL outcome.
    # Accept pass-engine vocabulary (PASS_OUTCOMES) or already-mapped report
    # enum values; map via the total table. Unknown non-empty strings still
    # reject so typos surface instead of silently becoming failed.
    from workbay_orchestrator_mcp.orchestration.worker_report_outcome import (  # noqa: PLC0415
        PASS_OUTCOME_TO_WORKER_REPORT,
        map_pass_outcome_to_worker_report,
    )

    if (
        raw_outcome is not None
        and raw_outcome not in WORKER_REPORT_OUTCOMES
        and raw_outcome not in PASS_OUTCOME_TO_WORKER_REPORT
    ):
        return _json_response(
            {
                "ok": False,
                "error": (
                    f"Invalid outcome. Valid report outcomes: {', '.join(sorted(WORKER_REPORT_OUTCOMES))}; "
                    f"or pass-engine outcomes: {', '.join(sorted(PASS_OUTCOME_TO_WORKER_REPORT))}."
                ),
            }
        )
    normalized_outcome = map_pass_outcome_to_worker_report(raw_outcome)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        if _get_lane_row(conn, resolved_task_ref, normalized_lane_id) is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        cur = conn.execute(
            """
            INSERT INTO worker_reports (
                task_ref, lane_id, session, summary, changed_files_json, test_commands_json, blockers_json,
                merge_ready, status, outcome, agent, branch, commit_sha, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                normalized_lane_id,
                session,
                summary,
                json.dumps(changed_files or []),
                json.dumps(test_commands or []),
                json.dumps(blockers or []),
                1 if merge_ready else 0,
                status,
                normalized_outcome,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
            ),
        )
        row = _row_to_dict(conn.execute("SELECT * FROM worker_reports WHERE id = ?", (cur.lastrowid,)).fetchone())
        # HOLDERCLASS-R1-F3 / CON-18: capture primary-write result, exit the
        # with-block so RESERVED is released, then run CURRENT_TASK render.
        # Two-region guard lives below so a raise or non-tuple return cannot
        # roll back the primary INSERT (DATA-01 / CLM-04 / OBS-04) — and so
        # the nested open + full render + file write does not hold RESERVED.

    # After commit: CURRENT_TASK side-effect (handoff-owned, FW2-WV04-N4).
    # Uniform guard shape (D3): pre-try fallback = writer-unavailable; after
    # import succeeds, fallback becomes handoff infrastructure type; writer
    # classification passthrough with fallback only when writer supplied
    # nothing (D1 / D2). Region 1 (import + writer call): ANY exception →
    # infrastructure classification (provenance-based; OBS-08 / CLM-04 — a
    # writer-raised TypeError must not be labelled orchestrator programming
    # error). Region 2 (unpack/validate returned value): TypeError →
    # programming error. Programming-error type is bound only after import
    # succeeds (no dead pre-try literal).
    md_written = False
    side_effect_error_type: str | None = None
    _side_effect_fallback: str = CURRENT_TASK_SIDE_EFFECT_WRITER_UNAVAILABLE_TYPE
    try:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE,
            CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE,
            _write_current_task_md_for_task,
        )

        _side_effect_fallback = CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE
        _programming_error_type = CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE
        # conn discarded by writer (opens its own connection).
        _writer_result = _write_current_task_md_for_task(object(), resolved_task_ref)
    except Exception as exc:
        _logger.warning(
            "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
            resolved_task_ref,
            type(exc).__name__,
            exc,
        )
        md_written = False
        side_effect_error_type = _side_effect_fallback
    else:
        try:
            md_written, side_effect_error_type = _writer_result
        except Exception as exc:
            _logger.warning(
                "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
                resolved_task_ref,
                type(exc).__name__,
                exc,
            )
            md_written = False
            # TypeError at orchestrator unpack/validate is a programming error
            # (OBS-08); keep every other exception on the infrastructure fallback.
            if isinstance(exc, TypeError):
                side_effect_error_type = _programming_error_type
            else:
                side_effect_error_type = _side_effect_fallback
    data: dict[str, object] = {
        "ok": True,
        "report": row,
        # OBS-08 three-state: True = attempted ok; False = attempted failed.
        "current_task_md_written": md_written,
    }
    if md_written is False:
        data["error_type"] = side_effect_error_type or _side_effect_fallback
    return _json_response(data)


def acknowledge_worker_report(
    lane_id: str,
    report_id: int,
    status: str,
    task_ref: str | None = None,
) -> dict:
    """CAS-transition a worker_report ``status`` out of ``submitted``.

    First UPDATE path on ``worker_reports`` (``record_worker_report`` is INSERT-only).
    Writes only ``status`` — never ``outcome`` ([DATA-14]). Compare-and-set predicate
    is ``WHERE id=? AND lane_id=? AND status='submitted'`` so concurrent ack/insert
    cannot clobber a terminal row ([CON-11]); re-acking matches 0 rows = no-op ([RES-01]).
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    try:
        normalized_report_id = int(report_id)
    except (TypeError, ValueError):
        return _json_response({"ok": False, "error": "report_id must be an integer."})
    if status not in REPORT_ACK_STATUSES:
        return _json_response(
            {"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(REPORT_ACK_STATUSES))}"}
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing = conn.execute(
            """
            SELECT * FROM worker_reports
            WHERE id = ? AND task_ref = ? AND lane_id = ?
            """,
            (normalized_report_id, resolved_task_ref, normalized_lane_id),
        ).fetchone()
        if existing is None:
            return _json_response(
                {
                    "ok": False,
                    "error": "Worker report not found for task_ref/lane_id.",
                    "updated": False,
                }
            )
        cur = conn.execute(
            """
            UPDATE worker_reports
            SET status = ?
            WHERE id = ? AND task_ref = ? AND lane_id = ? AND status = 'submitted'
            """,
            (status, normalized_report_id, resolved_task_ref, normalized_lane_id),
        )
        updated = int(cur.rowcount or 0) > 0
        row = _row_to_dict(
            conn.execute(
                "SELECT * FROM worker_reports WHERE id = ?",
                (normalized_report_id,),
            ).fetchone()
        )
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.
        # OBS-08 / FW2-WV04-N6: None = write not attempted; bool = attempted.

    # After commit: run side-effect only when the CAS UPDATE landed.
    # Uniform guard shape (D3) with D1 pre-try fallback and D2 passthrough.
    # Two-region guard (OBS-08 / CLM-04): region 1 import+call → infrastructure
    # on any exception; region 2 unpack/validate → TypeError is programming
    # error. Programming-error type bound only after import succeeds.
    md_written: bool | None = None
    side_effect_error_type: str | None = None
    _side_effect_fallback: str = CURRENT_TASK_SIDE_EFFECT_WRITER_UNAVAILABLE_TYPE
    if updated:
        # Region 1 + 2 so a raise or non-tuple return cannot roll back
        # the primary UPDATE (DATA-01 / CLM-04 / OBS-04) — already committed.
        try:
            from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
                CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE,
                CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE,
                _write_current_task_md_for_task,
            )

            _side_effect_fallback = CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE
            _programming_error_type = CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE
            _writer_result = _write_current_task_md_for_task(object(), resolved_task_ref)
        except Exception as exc:
            _logger.warning(
                "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
                resolved_task_ref,
                type(exc).__name__,
                exc,
            )
            md_written = False
            side_effect_error_type = _side_effect_fallback
        else:
            try:
                md_written, side_effect_error_type = _writer_result
            except Exception as exc:
                _logger.warning(
                    "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
                    resolved_task_ref,
                    type(exc).__name__,
                    exc,
                )
                md_written = False
                if isinstance(exc, TypeError):
                    side_effect_error_type = _programming_error_type
                else:
                    side_effect_error_type = _side_effect_fallback
    data: dict[str, object] = {
        "ok": True,
        "updated": updated,
        "report": row,
        "task_ref": resolved_task_ref,
        "lane_id": normalized_lane_id,
        "current_task_md_written": md_written,
    }
    if md_written is False:
        data["error_type"] = side_effect_error_type or _side_effect_fallback
    return _json_response(data)


def consume_lane_worker_reports(
    lane_id: str,
    *,
    report_id: int | None = None,
    task_ref: str | None = None,
) -> dict:
    """Close-cycle consumer: acknowledge the merge-ready report; supersede other submitted rows.

    Idempotent — already-terminal rows are CAS no-ops. Does not write ``outcome``.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        submitted = list(
            conn.execute(
                """
                SELECT id, merge_ready, created_at
                FROM worker_reports
                WHERE task_ref = ? AND lane_id = ? AND status = 'submitted'
                ORDER BY created_at DESC, id DESC
                """,
                (resolved_task_ref, normalized_lane_id),
            ).fetchall()
        )
        if not submitted:
            return _json_response(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "acknowledged": [],
                    "superseded": [],
                    "noop": True,
                }
            )

    ack_target_id: int | None = None
    if report_id is not None:
        try:
            want = int(report_id)
        except (TypeError, ValueError):
            return _json_response({"ok": False, "error": "report_id must be an integer."})
        if any(int(row["id"]) == want for row in submitted):
            ack_target_id = want
    if ack_target_id is None:
        for row in submitted:
            if int(row["merge_ready"] or 0) == 1:
                ack_target_id = int(row["id"])
                break

    acknowledged: list[int] = []
    superseded: list[int] = []
    for row in submitted:
        rid = int(row["id"])
        if ack_target_id is not None and rid == ack_target_id:
            result = acknowledge_worker_report(
                lane_id=normalized_lane_id,
                report_id=rid,
                status="acknowledged",
                task_ref=resolved_task_ref,
            )
            if result.get("updated"):
                acknowledged.append(rid)
        else:
            result = acknowledge_worker_report(
                lane_id=normalized_lane_id,
                report_id=rid,
                status="superseded",
                task_ref=resolved_task_ref,
            )
            if result.get("updated"):
                superseded.append(rid)
    return _json_response(
        {
            "ok": True,
            "task_ref": resolved_task_ref,
            "lane_id": normalized_lane_id,
            "acknowledged": acknowledged,
            "superseded": superseded,
            "noop": not acknowledged and not superseded,
        }
    )


def backfill_worker_report_acks(
    *,
    task_ref: str | None = None,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict:
    """Mark stranded ``status='submitted'`` worker_reports as ``acknowledged``.

    Historical rows never left ``submitted`` because no consumer existed. CAS-guarded
    per row so concurrent acks and re-runs are safe ([RES-01]). Empty backlog is a
    successful no-op.
    """
    with _get_db_connection() as conn:
        params: list[object] = []
        where = "status = 'submitted'"
        if task_ref is not None:
            resolved = _resolve_task_ref(conn, task_ref)
            where += " AND task_ref = ?"
            params.append(resolved)
        else:
            resolved = None
        sql = f"SELECT id, task_ref, lane_id FROM worker_reports WHERE {where} ORDER BY created_at ASC, id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = list(conn.execute(sql, tuple(params)).fetchall())

    if dry_run:
        return _json_response(
            {
                "ok": True,
                "dry_run": True,
                "would_update": len(rows),
                "updated": 0,
                "noop": len(rows) == 0,
                "task_ref": resolved,
            }
        )

    updated = 0
    for row in rows:
        result = acknowledge_worker_report(
            lane_id=str(row["lane_id"]),
            report_id=int(row["id"]),
            status="acknowledged",
            task_ref=str(row["task_ref"]),
        )
        if result.get("updated"):
            updated += 1
    return _json_response(
        {
            "ok": True,
            "dry_run": False,
            "would_update": len(rows),
            "updated": updated,
            "noop": updated == 0,
            "task_ref": resolved,
        }
    )


def list_worker_reports(
    task_ref: str | None = None,
    lane_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_reports: int | None = None,
) -> dict:
    limit = _effective_limit(limit, top_n_reports)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        total, rows = _paginated_query(
            conn, "worker_reports", where_sql, tuple(params), limit, offset, "created_at DESC, id DESC"
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "reports": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="reports",
                identity_fields=_WORKER_REPORT_IDENTITY_FIELDS,
                summary_fn=_summarize_worker_report_row,
            )
        )


def worker_reports(
    operation: str,
    lane_id: str | None = None,
    session: str | None = None,
    summary: str | None = None,
    changed_files: list[str] | None = None,
    test_commands: list[str] | None = None,
    blockers: list[str] | None = None,
    merge_ready: bool = False,
    status: str | None = None,
    outcome: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    limit: int = 20,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_reports: int | None = None,
    report_id: int | None = None,
) -> dict:
    """Discriminated wrapper for worker report record, list, and acknowledge operations."""
    valid_operations = {"list", "record", "acknowledge", "consume", "backfill_acks"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "record":
        return record_worker_report(
            lane_id=str(lane_id or ""),
            session=str(session or ""),
            summary=str(summary or ""),
            changed_files=changed_files,
            test_commands=test_commands,
            blockers=blockers,
            merge_ready=merge_ready,
            status=status or "submitted",
            outcome=outcome,
            task_ref=task_ref,
            actor=actor,
        )
    if operation == "acknowledge":
        return acknowledge_worker_report(
            lane_id=str(lane_id or ""),
            report_id=int(report_id) if report_id is not None else -1,
            status=status or "acknowledged",
            task_ref=task_ref,
        )
    if operation == "consume":
        return consume_lane_worker_reports(
            lane_id=str(lane_id or ""),
            report_id=report_id,
            task_ref=task_ref,
        )
    if operation == "backfill_acks":
        # Full backlog by default; ``limit`` from the list default is not applied.
        return backfill_worker_report_acks(task_ref=task_ref, dry_run=False, limit=None)
    return list_worker_reports(
        task_ref=task_ref,
        lane_id=lane_id,
        limit=limit,
        offset=offset,
        sections=sections,
        detail=detail,
        fields=fields,
        top_n_reports=top_n_reports,
    )


def record_lane_message(
    lane_id: str,
    session: str,
    direction: str,
    message: str,
    subject: str | None = None,
    status: str = "open",
    payload: dict[str, object] | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    valid_directions = LANE_MESSAGE_DIRECTIONS
    valid_statuses = MESSAGE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if direction not in valid_directions:
        return _json_response(
            {"ok": False, "error": f"Invalid direction. Valid: {', '.join(sorted(valid_directions))}"}
        )
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    normalized_payload, payload_error = _normalize_lane_message_payload(payload)
    if payload_error is not None:
        return _json_response({"ok": False, "error": payload_error})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        if _get_lane_row(conn, resolved_task_ref, normalized_lane_id) is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        dispatch_id = normalized_payload.get("dispatch_id") if normalized_payload is not None else None
        duplicate_dispatch = False
        try:
            cur = conn.execute(
                """
                INSERT INTO lane_messages (task_ref, lane_id, session, direction, subject, message, status, dispatch_id, payload_json, agent, branch, commit_sha, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """,
                (
                    resolved_task_ref,
                    normalized_lane_id,
                    session,
                    direction,
                    subject,
                    message,
                    status,
                    dispatch_id,
                    json.dumps(normalized_payload, sort_keys=True) if normalized_payload is not None else None,
                    ctx.agent,
                    ctx.branch,
                    ctx.commit_sha,
                ),
            )
            message_id = cur.lastrowid
        except sqlite3.IntegrityError:
            # HARM-A-005: the idx_lane_messages_dispatch_id unique index makes a
            # repeated (task_ref, lane_id, dispatch_id) a no-op replay, not a crash.
            # Mirror dispatch_lane_work: return the existing row with a marker.
            if dispatch_id is None:
                raise
            duplicate_dispatch = True
            existing = conn.execute(
                "SELECT id FROM lane_messages WHERE task_ref = ? AND lane_id = ? AND dispatch_id = ?",
                (resolved_task_ref, normalized_lane_id, dispatch_id),
            ).fetchone()
            message_id = existing[0] if existing is not None else None
        row = _row_to_dict(conn.execute("SELECT * FROM lane_messages WHERE id = ?", (message_id,)).fetchone())
        if row is not None:
            row = _decode_lane_message_row_dict(row)
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.

    # After commit: CURRENT_TASK side-effect. Two-region guard so a raise or
    # non-tuple return cannot roll back the primary INSERT (DATA-01 / CLM-04 /
    # OBS-04) — already committed. Uniform guard shape (D3) with D1 pre-try
    # fallback and D2 writer classification passthrough. Region 1 import+call
    # → infrastructure on any exception; region 2 unpack/validate → TypeError
    # is programming error (OBS-08 / CLM-04). Programming-error type bound
    # only after import succeeds.
    md_written = False
    side_effect_error_type: str | None = None
    _side_effect_fallback: str = CURRENT_TASK_SIDE_EFFECT_WRITER_UNAVAILABLE_TYPE
    try:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE,
            CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE,
            _write_current_task_md_for_task,
        )

        _side_effect_fallback = CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE
        _programming_error_type = CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE
        _writer_result = _write_current_task_md_for_task(object(), resolved_task_ref)
    except Exception as exc:
        _logger.warning(
            "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
            resolved_task_ref,
            type(exc).__name__,
            exc,
        )
        md_written = False
        side_effect_error_type = _side_effect_fallback
    else:
        try:
            md_written, side_effect_error_type = _writer_result
        except Exception as exc:
            _logger.warning(
                "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
                resolved_task_ref,
                type(exc).__name__,
                exc,
            )
            md_written = False
            if isinstance(exc, TypeError):
                side_effect_error_type = _programming_error_type
            else:
                side_effect_error_type = _side_effect_fallback
    data: dict[str, object] = {
        "ok": True,
        "message": row,
        "duplicate_dispatch": duplicate_dispatch,
        "current_task_md_written": md_written,
    }
    if md_written is False:
        data["error_type"] = side_effect_error_type or _side_effect_fallback
    return _json_response(data)


def record_lane_brief(
    lane_id: str,
    session: str,
    source_lane: str,
    reason: str,
    summary: str,
    message: str | None = None,
    required_actions: list[str] | None = None,
    artifacts: list[str] | None = None,
    status: str = "open",
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    normalized_reason = _normalize_optional_text(reason)
    normalized_summary = _normalize_optional_text(summary)
    normalized_source_lane = _normalize_optional_text(source_lane)
    if normalized_reason is None:
        return _json_response({"ok": False, "error": "reason is required."})
    if normalized_summary is None:
        return _json_response({"ok": False, "error": "summary is required."})
    if normalized_source_lane is None:
        return _json_response({"ok": False, "error": "source_lane is required."})
    brief_payload: dict[str, object] = {
        "source_lane": normalized_source_lane,
        "reason": normalized_reason,
        "summary": normalized_summary,
    }
    if required_actions:
        brief_payload["required_actions"] = [
            item for item in required_actions if isinstance(item, str) and item.strip()
        ]
    if artifacts:
        brief_payload["artifacts"] = [item for item in artifacts if isinstance(item, str) and item.strip()]
    return record_lane_message(
        lane_id=lane_id,
        session=session,
        direction="orchestrator_to_worker",
        subject=f"brief:{normalized_reason}",
        message=(message or normalized_summary),
        status=status,
        payload=brief_payload,
        task_ref=task_ref,
        actor=actor,
    )


def update_lane_message(
    message_id: int,
    status: str,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    valid_statuses = MESSAGE_STATUSES
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            "SELECT * FROM lane_messages WHERE id = ? AND task_ref = ?", (message_id, resolved_task_ref)
        ).fetchone()
        if row is None:
            return _json_response({"ok": False, "error": "Message not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        conn.execute(
            "UPDATE lane_messages SET status = ?, agent = COALESCE(agent, ?), branch = COALESCE(branch, ?), commit_sha = COALESCE(commit_sha, ?), updated_at = datetime('now') WHERE id = ? AND task_ref = ?",
            (status, ctx.agent, ctx.branch, ctx.commit_sha, message_id, resolved_task_ref),
        )
        updated = _row_to_dict(conn.execute("SELECT * FROM lane_messages WHERE id = ?", (message_id,)).fetchone())
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.

    # After commit: CURRENT_TASK side-effect. Two-region guard so a raise or
    # non-tuple return cannot roll back the primary UPDATE (DATA-01 / CLM-04 /
    # OBS-04) — already committed. Uniform guard shape (D3) with D1 pre-try
    # fallback and D2 writer classification passthrough. Region 1 import+call
    # → infrastructure on any exception; region 2 unpack/validate → TypeError
    # is programming error (OBS-08 / CLM-04). Programming-error type bound
    # only after import succeeds.
    md_written = False
    side_effect_error_type: str | None = None
    _side_effect_fallback: str = CURRENT_TASK_SIDE_EFFECT_WRITER_UNAVAILABLE_TYPE
    try:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE,
            CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE,
            _write_current_task_md_for_task,
        )

        _side_effect_fallback = CURRENT_TASK_SIDE_EFFECT_ERROR_TYPE
        _programming_error_type = CURRENT_TASK_SIDE_EFFECT_PROGRAMMING_ERROR_TYPE
        _writer_result = _write_current_task_md_for_task(object(), resolved_task_ref)
    except Exception as exc:
        _logger.warning(
            "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
            resolved_task_ref,
            type(exc).__name__,
            exc,
        )
        md_written = False
        side_effect_error_type = _side_effect_fallback
    else:
        try:
            md_written, side_effect_error_type = _writer_result
        except Exception as exc:
            _logger.warning(
                "CURRENT_TASK side-effect failed for task_ref=%s: %s: %s",
                resolved_task_ref,
                type(exc).__name__,
                exc,
            )
            md_written = False
            if isinstance(exc, TypeError):
                side_effect_error_type = _programming_error_type
            else:
                side_effect_error_type = _side_effect_fallback
    data: dict[str, object] = {
        "ok": True,
        "message": updated,
        "current_task_md_written": md_written,
    }
    if md_written is False:
        data["error_type"] = side_effect_error_type or _side_effect_fallback
    return _json_response(data)


def list_lane_messages(
    task_ref: str | None = None,
    lane_id: str | None = None,
    status: str = "all",
    limit: int = 20,
    offset: int = 0,
    direction: str | None = None,
    subject_prefix: str | None = None,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_messages: int | None = None,
) -> dict:
    """List lane messages with optional scope and content filters.

    ``direction`` restricts to a specific message direction (e.g. ``"orchestrator_to_worker"``).
    ``subject_prefix`` restricts to messages whose subject starts with the given prefix
    (e.g. ``"brief:"``), making this function capable of subsuming ``list_lane_briefs``.
    """
    valid_statuses = {"all", *MESSAGE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    limit = _effective_limit(limit, top_n_messages)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        inferred_lane = None
        if normalized_lane_id is None:
            inferred_lane_row = _resolve_current_lane_row(conn, resolved_task_ref)
            if inferred_lane_row is not None:
                normalized_lane_id = str(inferred_lane_row["lane_id"])
                # _resolve_current_lane_row already returns a dict; copy it (matching the
                # prior _row_to_dict semantics) instead of re-converting a non-Row.
                inferred_lane = dict(inferred_lane_row)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        if direction is not None:
            where_sql += " AND direction = ?"
            params.append(direction)
        if subject_prefix is not None:
            where_sql += " AND subject LIKE ? ESCAPE '\\'"
            params.append(f"{_escape_like(subject_prefix)}%")
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total, rows = _paginated_query(
            conn,
            "lane_messages",
            where_sql,
            tuple(params),
            limit,
            offset,
            "updated_at DESC, id DESC",
            _decode_lane_message_row_dict,
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "current_lane": inferred_lane,
                    "status": status,
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "messages": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="messages",
                identity_fields=_LANE_MESSAGE_IDENTITY_FIELDS,
                summary_fn=_summarize_lane_message_row,
            )
        )


def list_lane_briefs(
    task_ref: str | None = None, lane_id: str | None = None, status: str = "open", limit: int = 20, offset: int = 0
) -> dict:
    valid_statuses = {"all", *MESSAGE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    limit = max(1, limit)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref, "orchestrator_to_worker", "brief:%"]
        where_sql = "task_ref = ? AND direction = ? AND subject LIKE ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total, rows = _paginated_query(
            conn,
            "lane_messages",
            where_sql,
            tuple(params),
            limit,
            offset,
            "updated_at DESC, id DESC",
            _decode_lane_message_row_dict,
        )
        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": offset + len(rows) < total,
                "briefs": rows,
            }
        )


def lane_communication(
    kind: str,
    operation: str,
    lane_id: str | None = None,
    session: str | None = None,
    direction: str | None = None,
    message: str | None = None,
    subject: str | None = None,
    status: str = "open",
    payload: dict[str, object] | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    source_lane: str | None = None,
    reason: str | None = None,
    summary: str | None = None,
    required_actions: list[str] | None = None,
    artifacts: list[str] | None = None,
    message_id: int | None = None,
    limit: int = 20,
    offset: int = 0,
    subject_prefix: str | None = None,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_messages: int | None = None,
) -> dict:
    """Discriminated wrapper for lane message and brief operations."""
    valid_kinds = {"message", "brief"}
    valid_operations = {"record", "update", "list"}
    if kind not in valid_kinds:
        return _json_response({"ok": False, "error": f"Invalid kind. Valid: {', '.join(sorted(valid_kinds))}"})
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )

    if operation == "record":
        if kind == "message":
            return record_lane_message(
                lane_id=str(lane_id or ""),
                session=str(session or ""),
                direction=str(direction or ""),
                message=str(message or ""),
                subject=subject,
                status=status,
                payload=payload,
                task_ref=task_ref,
                actor=actor,
            )
        return record_lane_brief(
            lane_id=str(lane_id or ""),
            session=str(session or ""),
            source_lane=str(source_lane or ""),
            reason=str(reason or ""),
            summary=str(summary or ""),
            message=message,
            required_actions=required_actions,
            artifacts=artifacts,
            status=status,
            task_ref=task_ref,
            actor=actor,
        )

    if operation == "update":
        if message_id is None:
            return _json_response({"ok": False, "error": "message_id is required for update."})
        return update_lane_message(
            message_id=message_id,
            status=status,
            task_ref=task_ref,
            actor=actor,
        )

    if kind == "message":
        return list_lane_messages(
            task_ref=task_ref,
            lane_id=lane_id,
            status=status,
            limit=limit,
            offset=offset,
            direction=direction,
            subject_prefix=subject_prefix,
            sections=sections,
            detail=detail,
            fields=fields,
            top_n_messages=top_n_messages,
        )
    return list_lane_briefs(
        task_ref=task_ref,
        lane_id=lane_id,
        status=status,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# Plan cursor CRUD (moved from workbay-handoff-mcp/core.py in internal)
# ---------------------------------------------------------------------------


# Facade-only re-exports.
from .lane_reaping import (  # noqa: E402, F401
    _ARCHIVED_ORPHAN_TERMINAL_STATUSES,
    _close_blocked_lane_cas,
    _probe_branch_dead,
    _probe_worktree_gone,
    collect_blocked_lane_aging_entries,
    format_blocked_lane_aging_line,
    format_lane_age_label,
    reap_blocked_lanes,
    reap_task_archived_orphan_lanes,
)
from .plan_cursors import (  # noqa: E402, F401
    list_plan_cursors,
    plan_cursor,
)
