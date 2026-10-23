"""Lanes domain module.

Contains worktree lane management, turn metrics, worker reports, and lane messages.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, TypedDict

from pydantic import Field

from .lanes_support import (
    _BRANCH_PROBE_TIMEOUT_S,
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
from .orchestration.codex_lane_config import CODEX_MODEL_TIERS
from .orchestration.lane_routing import (
    InvalidSpeed,
    RoutingDisagreement,
    _speed_not_applicable,
    normalize_speed,
    resolve_routing_quad,
)
from .orchestration.lane_test_cmd import classify_test_cmd
from .plan_cursors import (  # noqa: F401 -- re-exported for the daemon and api.py
    list_plan_cursors,
    plan_cursor,
)

# Reaping remains below this domain in the import DAG, but loading it also
# loads the MCP API. Raw review-cycle admission imports the lane constants;
# defer these exact re-exports until used so reserve and EXIT settlement do
# not each start that unrelated API stack.
_REAPING_EXPORTS = frozenset({
    "_ARCHIVED_ORPHAN_TERMINAL_STATUSES", "_DEFAULT_BUNDLE_RETENTION_COUNT",
    "_close_authority_row_refusal", "_close_blocked_lane_cas", "_parse_close_authority",
    "_probe_branch_dead", "_probe_worktree_gone", "_reopen_moved_closed_row",
    "collect_blocked_lane_aging_entries", "format_blocked_lane_aging_line",
    "format_lane_age_label", "reap_blocked_lanes", "reap_task_archived_orphan_lanes",
    "unreap_lane_branch",
})


def __getattr__(name: str):
    if name not in _REAPING_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from . import lane_reaping

    value = getattr(lane_reaping, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | _REAPING_EXPORTS)


_logger = logging.getLogger(__name__)

LANE_MESSAGE_DIRECTIONS = frozenset({"orchestrator_to_worker", "worker_to_orchestrator"})
# closed_stale: schema v27 terminal for blocked-lane reclaimer (internal).
LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "merged", "closed", "closed_stale"})
MESSAGE_STATUSES = frozenset({"open", "acknowledged", "closed"})
# Success terminals that may CAS-acknowledge an open orchestrator_to_worker brief.
# Failure / residual outcomes leave the brief open so a later turn can resume.
BRIEF_ACK_SUCCESS_OUTCOMES = frozenset({"handoff_ready", "review_complete", "completed_unreviewed"})
# Outside the brief:% namespace so latest-open brief lookups cannot see it.
RETRY_NOTE_SUBJECT = "note:execution"
_RETRY_NOTE_MAX_CHARS = 1199
_RETRY_NOTE_PAYLOAD_DISPATCH_KEY = "dispatch_id"


def is_retry_note_subject(subject: str | None) -> bool:
    """True when *subject* is the retry-note namespace (not an assignment brief)."""
    return (subject or "") == RETRY_NOTE_SUBJECT


def is_assignment_brief_subject(subject: str | None) -> bool:
    """True for live assignment briefs; retry notes are never assignment briefs."""
    text = subject or ""
    return text.startswith("brief:") and not is_retry_note_subject(text)


def open_assignment_brief_sql_predicate() -> str:
    """SQL conjunct for assignment briefs; excludes retry notes.

    Latest-open-dispatch consumers (``offload_pass._open_dispatch_id``) should
    include this predicate. This lane does not own that caller.
    """
    return "subject LIKE 'brief:%' AND subject != ?"


def open_assignment_brief_sql_params() -> tuple[str, ...]:
    """Bound parameters for :func:`open_assignment_brief_sql_predicate`."""
    return (RETRY_NOTE_SUBJECT,)


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

HANDOFF_LANE_MESSAGE_DELIVERY_ID_ENV = "WORKBAY_LANE_MESSAGE_DELIVERY_ID"
HANDOFF_LANE_MESSAGE_DELIVERY_EFFECT_ENV = "WORKBAY_LANE_MESSAGE_DELIVERY_EFFECT"


def _handoff_lane_message_dispatch_id(delivery_id: str, effect: str) -> str:
    """Map one handoff delivery effect onto lane_messages' existing claim."""
    return f"handoff-delivery:{effect}:{delivery_id}"


def _worker_report_content_fingerprint(
    *,
    summary: str,
    test_commands_json: str,
    merge_ready: bool | int,
) -> str:
    """Fingerprint caller-supplied content covered by an identified delivery claim."""
    encoded = json.dumps(
        [summary, test_commands_json, bool(merge_ready)],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


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
            # Briefs recorded without an explicit message store their summary in
            # ``message`` only. Rehydrate the historical payload.summary view at
            # the read boundary so existing consumers do not need a migration.
            subject = row.get("subject")
            message = row.get("message")
            if (
                "summary" not in payload
                and isinstance(subject, str)
                and subject.startswith("brief:")
                and isinstance(message, str)
            ):
                payload = {**payload, "summary": message}
            row["payload"] = payload
    return row


def _attach_list_collection(payload: dict[str, object], *, row_key: str) -> dict[str, object]:
    """State a named collection, count, and has_more on the list payload.

    Matching rows were assembled under the row key at the envelope root. Callers
    that read ``data`` therefore saw a success flag beside an empty dict — a
    dropped projection, not a genuine empty result. Mirror the collection under
    ``data`` so absence is stated rather than implied by omission.
    """
    rows = payload.get(row_key)
    if not isinstance(rows, list):
        rows = []
    payload[row_key] = rows
    count = len(rows)
    payload["count"] = count
    payload.setdefault("has_more", False)
    payload["data"] = {
        row_key: rows,
        "count": count,
        "has_more": payload["has_more"],
    }
    return payload


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


_LANE_MESSAGE_PAYLOAD_TEXT_KEYS = frozenset({"source_lane", "reason", "summary", "dispatch_id"})
_LANE_MESSAGE_PAYLOAD_LIST_KEYS = frozenset({"required_actions", "artifacts", "owned_paths_override"})
_LANE_MESSAGE_PAYLOAD_SPECIAL_KEYS = _LANE_MESSAGE_PAYLOAD_TEXT_KEYS | _LANE_MESSAGE_PAYLOAD_LIST_KEYS


def _normalize_lane_message_payload(payload: object) -> tuple[dict[str, object] | None, str | None]:
    if payload is None:
        return None, None
    if not isinstance(payload, dict):
        return None, "lane message payload must be an object when provided."
    normalized: dict[str, object] = {
        key: value for key, value in payload.items() if key not in _LANE_MESSAGE_PAYLOAD_SPECIAL_KEYS
    }
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
    # dispatch_lane_work persists this semantic record directly. Retain the
    # complete record (including nested fields) so retry comparison cannot
    # silently accept a different review subject or payload under the same ID.
    for key in ("review_context_payload", "dispatch_request"):
        if key in payload:
            normalized[key] = payload[key]
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


def _absolutize_pythonpath(raw: str) -> tuple[list[str], list[list[str]]]:
    """Pin inherited PYTHONPATH entries to the resolution-time cwd.

    ``PYTHONPATH=pkg/src`` means "relative to *my* cwd". Lane subprocesses run
    with ``cwd=<lane tmpdir>``, so passing that string through unchanged
    silently redefines it — usually to nothing, after which imports fall
    through to whatever a site ``.pth`` points at. The resulting failure is a
    wrong-tree import surfacing as an unrelated ``TypeError`` in the callee,
    not an ``ImportError``. Resolving here preserves the parent's intent and
    keeps the rewrite observable. [CARD-07] [DBG-05] [OBS-08]

    Returns ``(entries, rewritten)`` where ``rewritten`` pairs each relative
    entry with the absolute path it was pinned to.
    """
    entries: list[str] = []
    rewritten: list[list[str]] = []
    for entry in raw.split(os.pathsep):
        if not entry:
            continue
        if os.path.isabs(entry):
            entries.append(entry)
            continue
        resolved = str(Path(entry).resolve())
        entries.append(resolved)
        rewritten.append([entry, resolved])
    if rewritten:
        _logger.warning(
            "pinned relative PYTHONPATH entries to cwd=%s: %s",
            os.getcwd(),
            ", ".join(f"{src} -> {dst}" for src, dst in rewritten),
        )
    return entries, rewritten


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

    inherited, rewritten = _absolutize_pythonpath(os.environ.get("PYTHONPATH", ""))
    pythonpath = os.pathsep.join([str(import_root), *inherited])
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
        "pythonpath_rewritten": rewritten,
    }


DEFAULT_HANDOFF_AGENT_ENV = "WORKBAY_HANDOFF_DEFAULT_AGENT"
_FALLBACK_HARNESS_TRANSCRIPT_ENV_VARS: tuple[str, ...] = (
    "CLAUDE_SESSION_TRANSCRIPT_PATH",
    "CODEX_SESSION_TRANSCRIPT_PATH",
    "GROK_SESSION_TRANSCRIPT_PATH",
    "VSCODE_TARGET_SESSION_LOG",
)


def _harness_transcript_env_present(env: Mapping[str, str]) -> bool:
    """True when a harness transcript identity can already attribute writes."""
    for var in _FALLBACK_HARNESS_TRANSCRIPT_ENV_VARS:
        if str(env.get(var, "") or "").strip():
            return True
    return False


def _canonical_harness_identity_present(
    env: Mapping[str, str],
    *,
    workspace_root: str | Path | None = None,
) -> bool:
    """True when canonical compaction-contract detection already attributes writes.

    Uses :func:`workbay_handoff_mcp.compaction_contract.detect_active_harness`
    then :func:`detect_harness_from_identity_markers`, honoring ambiguity so a
    fallback agent cannot mask a transcript or marker conflict. Workspace comes
    from *workspace_root* when provided and non-blank, otherwise the configured
    handoff runtime.

    A provided-but-blank *workspace_root* fails closed (identity present) and
    must not call ``get_runtime_config()``. Contract/runtime load errors
    (``FileNotFoundError``, ``RuntimeError`` including
    ``RuntimeNotConfiguredError``, ``ValueError``, ``ImportError``) degrade to
    the hardcoded transcript-var list plus ``CLAUDE_CODE_SESSION_ID``, and log
    a warning naming the exception class. Any other exception class fails
    closed — it must not fall back to the weak list.
    """
    source = {str(key): str(value) for key, value in env.items()}
    # Provided-but-whitespace root is not "absent": never mask via parent runtime.
    if workspace_root is not None and not str(workspace_root).strip():
        return True
    try:
        from workbay_handoff_mcp.compaction_contract import (  # noqa: PLC0415
            detect_active_harness,
            detect_harness_from_identity_markers,
            load_compaction_contract,
        )

        if workspace_root is not None:
            contract_root = Path(workspace_root)
        else:
            from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

            contract_root = get_runtime_config().workspace_root
        contract = load_compaction_contract(contract_root)
        transcript = detect_active_harness(contract, env=source)
        if transcript.harness is not None or transcript.ambiguous:
            return True
        markers = detect_harness_from_identity_markers(contract, env=source)
        if markers.harness is not None or markers.ambiguous:
            return True
        return False
    except (FileNotFoundError, RuntimeError, ValueError, ImportError) as exc:
        _logger.warning(
            "canonical harness identity detection degraded (%s): %s",
            type(exc).__name__,
            exc,
        )
        if str(source.get("CLAUDE_CODE_SESSION_ID", "") or "").strip():
            return True
        return _harness_transcript_env_present(source)
    except Exception as exc:
        _logger.warning(
            "canonical harness identity detection failed closed (%s): %s",
            type(exc).__name__,
            exc,
        )
        return True


def handoff_subprocess_env(
    base: Mapping[str, str] | None = None,
    *,
    default_agent: str | None = None,
    workspace_root: str | Path | None = None,
) -> dict[str, str]:
    """Env mapping for handoff subprocesses: pin CLI to the running package.

    Merges :func:`resolve_handoff_orchestrator_cli` env keys into *base* (or
    ``os.environ``). Callers that shell to ``worktree-lane`` must use this so the
    bash helper prefers ``python -m workbay_orchestrator_mcp`` over a PATH/uv-tool
    install.

    Production always resolves the running package (``allow_installed=False``),
    which returns ``ok=True``. The installed-CLI skew arm lives in
    :func:`resolve_handoff_orchestrator_cli` for explicit probes and in bash
    when a MODULE_ROOT pin cannot open a real package tree — not here (OBS-08).

    When *default_agent* is provided, inject ``WORKBAY_HANDOFF_DEFAULT_AGENT``
    only if the caller has not already set it and canonical harness detection
    (transcript vars, then identity markers) has not already attributed the
    write — including the ambiguous case. Never overrides an explicit identity
    (lifecycle Q1).

    When *workspace_root* is provided, canonical detection loads the contract
    from that root instead of the parent runtime. A provided-but-blank root
    fails closed (refuses injection) and must not call ``get_runtime_config()``.
    Callers that already know the child workspace (final handoff) must pass it
    so a parent runtime lookup failure cannot mask child-resolvable identity
    markers.
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
    agent = (default_agent or "").strip()
    if (
        agent
        and not str(env.get(DEFAULT_HANDOFF_AGENT_ENV, "") or "").strip()
        and not _canonical_harness_identity_present(env, workspace_root=workspace_root)
    ):
        env[DEFAULT_HANDOFF_AGENT_ENV] = agent
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
_LANE_MESSAGE_ACK_IDENTITY_FIELDS = frozenset({"message_id", "lane_id", "kind", "status"})
_LANE_MESSAGE_WRITE_SECTIONS = frozenset({"identity", "message"})
_LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES = 8192
_LANE_COMMUNICATION_ERROR_TEXT_LIMIT = 180
_LANE_COMMUNICATION_CURSOR_PAYLOAD_KEY = "_lane_communication_cursor"
_LANE_COMMUNICATION_DETAIL_LEVELS = frozenset({"full", "summary"})
_LANE_MESSAGE_ROW_FIELDS = frozenset(
    {
        "id",
        "task_ref",
        "lane_id",
        "session",
        "direction",
        "subject",
        "message",
        "status",
        "dispatch_id",
        "payload_json",
        "payload",
        "agent",
        "branch",
        "commit_sha",
        "created_at",
        "updated_at",
        "kind",
        "bytes_stored",
        "message_id",
        "ok",
        "duplicate_dispatch",
        "current_task_md_written",
        "error_type",
        "error",
        "error_kind",
        "operation",
        "truncated",
        "scoped_fetch",
        "omitted_fields",
        "projection",
        "committed_revision",
        "chunk_offset",
        "chunk_end",
        "chunk_total_bytes",
    }
)
_LANE_COMMUNICATION_WRITE_FIELDS = _LANE_MESSAGE_ROW_FIELDS | _LANE_MESSAGE_ACK_IDENTITY_FIELDS
_LANE_COMMUNICATION_LIST_FIELDS = frozenset(
    {
        "id",
        "task_ref",
        "lane_id",
        "session",
        "direction",
        "subject",
        "message",
        "status",
        "dispatch_id",
        "payload_json",
        "payload",
        "agent",
        "branch",
        "commit_sha",
        "created_at",
        "updated_at",
        "kind",
        "bytes_stored",
        "message_id",
    }
)
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


def _lane_message_kind(row: dict[str, object], kind: str | None) -> str:
    if kind in {"brief", "message"}:
        return kind
    subject = row.get("subject")
    return "brief" if isinstance(subject, str) and subject.startswith("brief:") else "message"


def _lane_message_bytes_stored(row: dict[str, object]) -> int:
    return sum(
        len(value.encode("utf-8")) for value in (row.get("message"), row.get("payload_json")) if isinstance(value, str)
    )


def _compact_lane_message_ack(row: dict[str, object], *, kind: str | None) -> dict[str, object]:
    return {
        "ok": True,
        "message_id": row.get("id"),
        "lane_id": row.get("lane_id"),
        "kind": _lane_message_kind(row, kind),
        "subject": row.get("subject"),
        "direction": row.get("direction"),
        "status": row.get("status"),
        "bytes_stored": _lane_message_bytes_stored(row),
        "created_at": row.get("created_at"),
    }


def _mcp_wire_utf8(payload: Mapping[str, object]) -> bytes:
    """Serialize like FastMCP Tool.convert_result then MCP CallToolResult JSON."""
    import pydantic_core
    from fastmcp.tools.base import default_serializer
    from mcp.types import CallToolResult, TextContent

    raw = dict(payload)
    structured = pydantic_core.to_jsonable_python(raw)
    text = default_serializer(raw)
    return (
        CallToolResult(
            content=[TextContent(type="text", text=text)],
            structuredContent=structured,
            isError=False,
        )
        .model_dump_json(by_alias=True, exclude_none=True)
        .encode("utf-8")
    )


def _mcp_envelope_utf8_bytes(payload: Mapping[str, object]) -> int:
    """UTF-8 size of the supported FastMCP/MCP CallToolResult envelope."""
    try:
        return len(_mcp_wire_utf8(payload))
    except Exception:
        return _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES + 1


def _clip_error_text(text: object) -> str:
    rendered = str(text)
    if len(rendered) <= _LANE_COMMUNICATION_ERROR_TEXT_LIMIT:
        return rendered
    return rendered[:_LANE_COMMUNICATION_ERROR_TEXT_LIMIT] + "..."


def _clip_selector_value(value: object) -> str:
    rendered = "" if value is None else str(value)
    if len(rendered) <= 120:
        return rendered
    return rendered[:120] + "..."


def _utf8_offset_is_boundary(encoded: bytes, offset: int) -> bool:
    if offset < 0 or offset > len(encoded):
        return False
    if offset == 0 or offset == len(encoded):
        return True
    return encoded[offset] & 0xC0 != 0x80


def _utf8_slice_bytes(encoded: bytes, offset: int, limit: int) -> tuple[str, int]:
    if offset < 0 or limit <= 0 or offset >= len(encoded):
        return "", 0
    if not _utf8_offset_is_boundary(encoded, offset):
        return "", 0
    end = min(len(encoded), offset + limit)
    while end < len(encoded) and encoded[end] & 0xC0 == 0x80:
        end += 1
    piece = encoded[offset:end]
    while piece:
        try:
            return piece.decode("utf-8"), len(piece)
        except UnicodeDecodeError:
            piece = piece[:-1]
    return "", 0


def _bounded_error_payload(payload: Mapping[str, object]) -> dict[str, object]:
    current = dict(payload)
    current["ok"] = False
    if isinstance(current.get("error"), str):
        current["error"] = _clip_error_text(current["error"])
    if "invalid_value" in current:
        current["invalid_value"] = _clip_selector_value(current["invalid_value"])
    drop_order = (
        "message",
        "allowed",
        "invalid_value",
        "task_ref",
        "lane_id",
        "kind",
        "operation",
        "error",
        "error_kind",
        "invalid_selector",
    )
    if _mcp_envelope_utf8_bytes(current) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
        return current
    for key in drop_order:
        if key in current:
            current.pop(key, None)
            if _mcp_envelope_utf8_bytes(current) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
                return current
    return {
        "ok": False,
        "error_kind": "envelope_budget_exceeded",
        "error": "Error response exceeds the 8192-byte MCP envelope budget.",
    }


def _validate_list_chunk_args(*, chunk_offset: int | None, chunk_limit: int | None) -> dict[str, object] | None:
    if chunk_limit is not None and int(chunk_limit) <= 0:
        return _bounded_error_payload(
            {
                "ok": False,
                "error_kind": "invalid_chunk",
                "error": "chunk_limit must be a positive byte count.",
                "invalid_selector": "chunk_limit",
                "invalid_value": chunk_limit,
            }
        )
    if chunk_offset is not None and int(chunk_offset) < 0:
        return _bounded_error_payload(
            {
                "ok": False,
                "error_kind": "invalid_chunk",
                "error": "chunk_offset must be a non-negative UTF-8 boundary.",
                "invalid_selector": "chunk_offset",
                "invalid_value": chunk_offset,
            }
        )
    return None


def _list_identity_budget_refusal(*, task_ref: object, lane_id: object, kind: str) -> dict[str, object]:
    del task_ref, lane_id
    return _bounded_error_payload(
        {
            "ok": False,
            "error_kind": "identity_budget_exceeded",
            "error": "Required list identity exceeds the 8192-byte MCP envelope budget.",
            "invalid_selector": "identity",
            "allowed": ["task_ref", "lane_id", "message_id", "committed_revision"],
            "kind": kind,
            "operation": "list",
        }
    )


def _committed_revision_of(row: Mapping[str, object]) -> object:
    return row.get("revision")


def _committed_revision_matches(row: Mapping[str, object], expected_revision: str) -> bool:
    revision = _committed_revision_of(row)
    return (
        isinstance(revision, int)
        and not isinstance(revision, bool)
        and revision >= 0
        and f"{revision:019d}" == expected_revision
    )


def _legacy_committed_revision_matches(row: Mapping[str, object], expected_revision: str) -> bool:
    revision = row.get("updated_at") or row.get("created_at") or row.get("id")
    return str(revision) == expected_revision


def _public_lane_message_row(row: Mapping[str, object]) -> dict[str, object]:
    public_row = dict(row)
    public_row.pop("revision", None)
    return public_row


def _public_lane_message_list_result(result: dict[str, object], *, row_key: str) -> dict[str, object]:
    public_result = dict(result)
    rows = public_result.get(row_key)
    if isinstance(rows, list):
        public_rows = [_public_lane_message_row(row) if isinstance(row, dict) else row for row in rows]
        public_result[row_key] = public_rows
        data = public_result.get("data")
        if isinstance(data, dict) and row_key in data:
            public_data = dict(data)
            public_data[row_key] = public_rows
            public_result["data"] = public_data
    return public_result


def _ensure_lane_message_revision_schema(conn: sqlite3.Connection) -> None:
    """Upgrade legacy messages atomically while keeping upgraded reads lock-free."""
    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(lane_messages)").fetchall()}
    if not columns:
        return
    if "revision" in columns:
        triggers = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'lane_messages'"
            )
        }
        if {"lane_messages_revision_after_update", "lane_messages_revision_after_insert"} <= triggers:
            if conn.execute("SELECT 1 FROM lane_messages WHERE revision IS NULL LIMIT 1").fetchone() is None:
                return

    # Current callers enter before their first write. The factory's finite
    # busy_timeout bounds acquisition; re-read all schema state under the lock.
    from workbay_handoff_mcp.write_retry import DbBusyError, is_sqlite_lock_error

    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        if not is_sqlite_lock_error(exc):
            raise
        timeout_ms = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
        raise DbBusyError(
            tool="lane_communication",
            budget_seconds=timeout_ms / 1000,
            last_error=str(exc),
        ) from exc
    try:
        _upgrade_lane_message_revision_schema_locked(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _upgrade_lane_message_revision_schema_locked(conn: sqlite3.Connection) -> None:
    """Add a monotonic message revision to databases created by older versions."""
    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(lane_messages)").fetchall()}
    if not columns:
        return
    if "revision" not in columns:
        try:
            conn.execute("ALTER TABLE lane_messages ADD COLUMN revision INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError as exc:
            # Accept only a confirmed duplicate, never an unrelated DDL failure.
            if "duplicate column name: revision" not in str(exc).lower():
                raise
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(lane_messages)").fetchall()}
            if "revision" not in columns:
                raise
    elif conn.execute("SELECT 1 FROM lane_messages WHERE revision IS NULL LIMIT 1").fetchone() is not None:
        conn.execute("UPDATE lane_messages SET revision = 0 WHERE revision IS NULL")

    triggers = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'lane_messages'")
    }
    if "lane_messages_revision_after_update" not in triggers:
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS lane_messages_revision_after_update
            AFTER UPDATE ON lane_messages
            WHEN NEW.revision IS NULL OR OLD.revision IS NULL OR NEW.revision <= OLD.revision
            BEGIN
                UPDATE lane_messages
                SET revision = COALESCE(OLD.revision, 0) + 1
                WHERE id = NEW.id;
            END
            """
        )
    if "lane_messages_revision_after_insert" not in triggers:
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS lane_messages_revision_after_insert
            AFTER INSERT ON lane_messages
            WHEN NEW.revision IS NULL
            BEGIN
                UPDATE lane_messages SET revision = 0 WHERE id = NEW.id;
            END
            """
        )


def _committed_revision_pin_of(row: Mapping[str, object]) -> str:
    """Return a revision token bound to this immutable message row ID."""
    revision = _committed_revision_of(row)
    if isinstance(revision, int) and not isinstance(revision, bool) and revision >= 0:
        revision_text = f"{revision:019d}"
    else:
        revision_text = str(revision)
    return json.dumps(
        {"id": row.get("id"), "revision": revision_text},
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_committed_revision_pin(value: str) -> tuple[int, str] | None:
    """Decode an ID-bound pin; timestamp-only strings are legacy pins."""
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(decoded, dict) or set(decoded) != {"id", "revision"}:
        return None
    message_id = decoded.get("id")
    revision = decoded.get("revision")
    if not isinstance(message_id, int) or isinstance(message_id, bool) or not isinstance(revision, str):
        return None
    return message_id, revision


def _is_committed_revision_pin_candidate(value: str) -> bool:
    """Identify malformed ID-bound pins so they cannot fall back to timestamp lookup."""
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return False
    return isinstance(decoded, dict) and ("id" in decoded or "revision" in decoded)


def _lane_communication_cursor_payload(
    *,
    chunk_offset: int,
    committed_revision: object | None,
    message_id: object | None,
    chunk_limit: int | None = None,
    more_rows_after: bool | None = None,
) -> dict[str, object]:
    state: dict[str, object] = {"chunk_offset": chunk_offset}
    if committed_revision is not None:
        state["committed_revision"] = str(committed_revision)
    if message_id is not None:
        state["message_id"] = message_id
    if chunk_limit is not None:
        state["chunk_limit"] = chunk_limit
    if more_rows_after is not None:
        state["more_rows_after"] = more_rows_after
    return {_LANE_COMMUNICATION_CURSOR_PAYLOAD_KEY: state}


def _decode_lane_communication_cursor_payload(
    payload: dict[str, object] | None,
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    if not isinstance(payload, dict) or _LANE_COMMUNICATION_CURSOR_PAYLOAD_KEY not in payload:
        return None, None
    state = payload.get(_LANE_COMMUNICATION_CURSOR_PAYLOAD_KEY)
    if not isinstance(state, dict):
        return None, {
            "ok": False,
            "error_kind": "invalid_chunk",
            "error": "Malformed lane_communication continuation payload.",
        }
    chunk_offset = state.get("chunk_offset")
    chunk_limit = state.get("chunk_limit")
    message_id = state.get("message_id")
    revision = state.get("committed_revision")
    more_rows_after = state.get("more_rows_after")
    if not isinstance(chunk_offset, int) or isinstance(chunk_offset, bool) or chunk_offset < 0:
        return None, {
            "ok": False,
            "error_kind": "invalid_chunk",
            "error": "Continuation chunk_offset must be a non-negative integer.",
        }
    if chunk_limit is not None and (not isinstance(chunk_limit, int) or isinstance(chunk_limit, bool)):
        return None, {
            "ok": False,
            "error_kind": "invalid_chunk",
            "error": "Continuation chunk_limit must be a positive integer.",
        }
    if message_id is not None and (not isinstance(message_id, int) or isinstance(message_id, bool)):
        return None, {
            "ok": False,
            "error_kind": "invalid_chunk",
            "error": "Continuation message_id must be an integer.",
        }
    if revision is not None and not isinstance(revision, str):
        return None, {
            "ok": False,
            "error_kind": "invalid_chunk",
            "error": "Continuation committed_revision must be a string.",
        }
    if more_rows_after is not None and not isinstance(more_rows_after, bool):
        return None, {
            "ok": False,
            "error_kind": "invalid_chunk",
            "error": "Continuation more_rows_after must be a boolean.",
        }
    normalized: dict[str, object] = {"chunk_offset": chunk_offset}
    if chunk_limit is not None:
        normalized["chunk_limit"] = chunk_limit
    if message_id is not None:
        normalized["message_id"] = message_id
    if revision is not None:
        normalized["committed_revision"] = revision
    if more_rows_after is not None:
        normalized["more_rows_after"] = more_rows_after
    return normalized, None


def _unknown_csv_selectors(raw: str | None, allowed: frozenset[str]) -> list[str]:
    if raw is None:
        return []
    if not raw.strip():
        return [raw]
    return [part.strip() for part in raw.split(",") if part.strip() and part.strip() not in allowed]


def _invalid_projection_refusal(selector: str, invalid: object, allowed: frozenset[str]) -> dict[str, object]:
    allowed_list = sorted(allowed)
    invalid_display = _clip_selector_value(invalid)
    return {
        "ok": False,
        "error_kind": "invalid_projection",
        "error": f"Invalid {selector}: {invalid_display!r}. Valid: {', '.join(allowed_list)}",
        "invalid_selector": selector,
        "invalid_value": invalid if not isinstance(invalid, str) or len(invalid) <= 120 else invalid_display,
        "allowed": allowed_list,
    }


def _validate_lane_communication_projection(
    *,
    operation: str,
    kind: str | None,
    sections: str | None,
    fields: str | None,
    detail: str | None = None,
) -> dict[str, object] | None:
    if operation in {"record", "update"}:
        valid_sections = _LANE_MESSAGE_WRITE_SECTIONS
        valid_fields = _LANE_COMMUNICATION_WRITE_FIELDS
    else:
        row_key = "briefs" if kind == "brief" else "messages"
        valid_sections = frozenset({"identity", "counts", row_key})
        valid_fields = _LANE_COMMUNICATION_LIST_FIELDS
    if detail is not None:
        normalized = detail.strip() if isinstance(detail, str) else ""
        if normalized not in _LANE_COMMUNICATION_DETAIL_LEVELS:
            return _invalid_projection_refusal("detail", detail, _LANE_COMMUNICATION_DETAIL_LEVELS)
    unknown_sections = _unknown_csv_selectors(sections, valid_sections)
    if unknown_sections:
        return _invalid_projection_refusal(
            "sections", ",".join(unknown_sections) if sections and sections.strip() else sections or "", valid_sections
        )
    unknown_fields = _unknown_csv_selectors(fields, valid_fields)
    if unknown_fields:
        return _invalid_projection_refusal(
            "fields", ",".join(unknown_fields) if fields and fields.strip() else fields or "", valid_fields
        )
    return None


def _scoped_fetch_for_row(
    row: Mapping[str, object],
    *,
    kind: str | None,
    chunk_offset: int = 0,
) -> dict[str, object]:
    fetch: dict[str, object] = {
        "tool": "lane_communication",
        "kind": _lane_message_kind(dict(row), kind),
        "operation": "list",
        "task_ref": row.get("task_ref"),
        "lane_id": row.get("lane_id"),
        "message_id": row.get("id"),
        "status": "all",
        "detail": "full",
    }
    revision = _committed_revision_pin_of(row)
    fetch["payload"] = _lane_communication_cursor_payload(
        chunk_offset=chunk_offset,
        committed_revision=revision,
        message_id=row.get("id"),
    )
    return fetch


def _write_truncation_receipt(
    row: dict[str, object],
    *,
    kind: str | None,
    operation: str,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    compact = _compact_lane_message_ack(row, kind=kind)
    compact.update(
        {
            "task_ref": row.get("task_ref"),
            "operation": operation,
            "truncated": True,
            "omitted_fields": ["message", "payload", "payload_json"],
            "projection": {"detail": "summary", "reason": "mcp_envelope_budget"},
            "committed_revision": _committed_revision_pin_of(row),
            "scoped_fetch": _scoped_fetch_for_row(row, kind=kind),
        }
    )
    if extra:
        compact.update(dict(extra))
    if isinstance(compact.get("error"), str):
        compact["error"] = _clip_error_text(compact["error"])
    return compact


def _carry_write_extra(shaped: Mapping[str, object]) -> dict[str, object]:
    extra: dict[str, object] = {}
    for key in (
        "duplicate_dispatch",
        "error_kind",
        "error",
        "error_type",
        "current_task_md_written",
        "truncated",
        "projection",
        "committed_revision",
        "operation",
        "task_ref",
    ):
        if key in shaped:
            extra[key] = shaped[key]
    if "error" in extra:
        extra["error"] = _clip_error_text(extra["error"])
    return extra


def _minimal_write_receipt(
    row: Mapping[str, object],
    *,
    kind: str | None,
    operation: str,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    receipt: dict[str, object] = {
        "ok": True,
        "message_id": row.get("id"),
        "lane_id": row.get("lane_id"),
        "kind": _lane_message_kind(dict(row), kind),
        "status": row.get("status"),
        "task_ref": row.get("task_ref"),
        "operation": operation,
        "truncated": True,
        "committed_revision": _committed_revision_pin_of(row),
        "scoped_fetch": _scoped_fetch_for_row(row, kind=kind),
        "projection": {"detail": "summary", "reason": "mcp_envelope_budget"},
    }
    if extra:
        for key, value in extra.items():
            if key == "padding":
                continue
            receipt[key] = value
    if "duplicate_dispatch" not in receipt and extra is not None and "duplicate_dispatch" in extra:
        receipt["duplicate_dispatch"] = extra["duplicate_dispatch"]
    if isinstance(receipt.get("error"), str):
        receipt["error"] = _clip_error_text(receipt["error"])
    return receipt


def _strip_write_receipt_to_budget(receipt: dict[str, object]) -> dict[str, object]:
    current = dict(receipt)
    drop_order = (
        "subject",
        "direction",
        "bytes_stored",
        "created_at",
        "omitted_fields",
        "current_task_md_written",
        "error_type",
        "padding",
        "error",
        "projection",
        "scoped_fetch",
    )
    if _mcp_envelope_utf8_bytes(current) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
        return current
    for key in drop_order:
        if key in current:
            current.pop(key, None)
            if _mcp_envelope_utf8_bytes(current) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
                return current
    return current


def _identity_budget_refusal(
    *,
    task_ref: object,
    lane_id: object,
    operation: str,
    kind: str | None,
) -> dict[str, object] | None:
    probe_row = {
        "id": 10**15,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "status": "acknowledged",
        "updated_at": "2026-12-31 23:59:59",
        "created_at": "2026-12-31 23:59:59",
        "subject": "",
        "direction": "worker_to_orchestrator",
        "message": "",
        "payload_json": None,
    }
    probe = _minimal_write_receipt(
        probe_row,
        kind=kind,
        operation=operation,
        extra={"duplicate_dispatch": False},
    )
    if _mcp_envelope_utf8_bytes(probe) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
        return None
    return {
        "ok": False,
        "error_kind": "identity_budget_exceeded",
        "error": "Required receipt identity exceeds the 8192-byte MCP envelope budget.",
        "invalid_selector": "identity",
        "allowed": ["task_ref", "lane_id", "message_id", "committed_revision"],
    }


def _apply_write_envelope_budget(
    shaped: dict[str, object],
    *,
    row: dict[str, object],
    kind: str | None,
    operation: str,
) -> dict[str, object]:
    if shaped.get("ok") is False:
        return shaped
    extra = _carry_write_extra(shaped)
    if _mcp_envelope_utf8_bytes(shaped) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
        return shaped
    receipt = _write_truncation_receipt(row, kind=kind, operation=operation, extra=extra)
    if _mcp_envelope_utf8_bytes(receipt) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
        return receipt
    return _strip_write_receipt_to_budget(_minimal_write_receipt(row, kind=kind, operation=operation, extra=extra))


def _replace_list_collection(
    shaped: dict[str, object],
    *,
    row_key: str,
    rows: list[dict[str, object]],
    has_more: bool | None = None,
) -> dict[str, object]:
    out = dict(shaped)
    out[row_key] = rows
    out["returned"] = len(rows)
    out["count"] = len(rows)
    if has_more is not None:
        out["has_more"] = has_more
    data = out.get("data")
    if isinstance(data, dict):
        out["data"] = {
            **data,
            row_key: rows,
            "count": len(rows),
            "has_more": bool(out.get("has_more", False)),
        }
    return out


def _list_scoped_fetch(
    fetch_base: Mapping[str, object],
    *,
    kind: str,
    **overrides: object,
) -> dict[str, object]:
    fetch: dict[str, object] = {
        "tool": "lane_communication",
        "kind": kind,
        "operation": "list",
    }
    for key in (
        "task_ref",
        "lane_id",
        "status",
        "offset",
        "limit",
        "message_id",
        "committed_revision",
        "detail",
        "chunk_offset",
        "chunk_limit",
        "sections",
        "fields",
        "direction",
        "subject_prefix",
        "payload",
    ):
        value = overrides[key] if key in overrides else fetch_base.get(key)
        if value is not None:
            fetch[key] = value
    return fetch


def _payload_field_names(row: Mapping[str, object]) -> list[str]:
    return [key for key in ("payload", "payload_json") if key in row and row.get(key) is not None]


def _with_more_rows_cursor(
    shaped: dict[str, object],
    *,
    kind: str,
    fetch_base: Mapping[str, object],
    next_offset: int,
) -> dict[str, object] | None:
    candidate = dict(shaped)
    candidate["has_more"] = True
    candidate["scoped_fetch"] = _list_scoped_fetch(
        fetch_base,
        kind=kind,
        offset=next_offset,
        chunk_offset=None,
        message_id=None,
        committed_revision=None,
        payload=None,
    )
    if _mcp_envelope_utf8_bytes(candidate) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
        return candidate
    stripped = dict(candidate)
    for key in ("current_lane", "data"):
        stripped.pop(key, None)
        if _mcp_envelope_utf8_bytes(stripped) <= _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES:
            return stripped
    return None


def _list_content_fetch_overrides(
    row: Mapping[str, object],
    *,
    exact_row: bool,
    original_offset: int,
    next_offset: int,
    fetch_base: Mapping[str, object],
    more_rows_after: bool,
) -> dict[str, object]:
    overrides: dict[str, object] = {
        "detail": "full",
        "chunk_offset": next_offset,
    }
    payload_cursor = not exact_row or bool(fetch_base.get("cursor_payload_mode"))
    if payload_cursor:
        raw_chunk_limit = fetch_base.get("chunk_limit")
        overrides.update(
            {
                "payload": _lane_communication_cursor_payload(
                    chunk_offset=next_offset,
                    committed_revision=fetch_base.get("committed_revision") or _committed_revision_pin_of(row),
                    message_id=row.get("id"),
                    chunk_limit=raw_chunk_limit if isinstance(raw_chunk_limit, int) else None,
                    more_rows_after=more_rows_after,
                ),
                "chunk_offset": None,
                "chunk_limit": None,
                "committed_revision": None,
            }
        )
    if exact_row:
        overrides["message_id"] = row.get("id")
        overrides["status"] = fetch_base.get("status") or "all" if fetch_base.get("cursor_payload_mode") else "all"
        overrides["offset"] = None
        return overrides
    overrides["message_id"] = row.get("id") if payload_cursor else None
    overrides["offset"] = original_offset
    return overrides


def _exact_payload_fetch(
    row: Mapping[str, object],
    *,
    kind: str,
    fetch_base: Mapping[str, object],
) -> dict[str, object]:
    return _list_scoped_fetch(
        fetch_base,
        kind=kind,
        message_id=row.get("id"),
        status="all",
        offset=None,
        chunk_offset=None,
        chunk_limit=None,
        committed_revision=None,
        payload=_lane_communication_cursor_payload(
            chunk_offset=0,
            committed_revision=_committed_revision_pin_of(row),
            message_id=row.get("id"),
        ),
        detail="full",
    )


def _compact_payload_page(
    working: dict[str, object],
    *,
    row_key: str,
    row: Mapping[str, object],
    kind: str,
    fetch_base: Mapping[str, object],
    more_rows_after: bool,
    original_offset: int,
) -> dict[str, object]:
    compact_row = {key: row.get(key) for key in ("id", "status", "lane_id", "task_ref") if key in row}
    omitted = ["message"]
    if "payload" in row and row.get("payload") is not None:
        compact_row["payload"] = row["payload"]
        if "payload_json" in row:
            omitted.append("payload_json")
    elif "payload_json" in row and row.get("payload_json") is not None:
        compact_row["payload_json"] = row["payload_json"]
    trial = _replace_list_collection(working, row_key=row_key, rows=[compact_row], has_more=more_rows_after)
    trial["truncated"] = True
    trial["omitted_fields"] = omitted
    if more_rows_after:
        trial["has_more"] = True
        trial["scoped_fetch"] = _list_scoped_fetch(
            fetch_base,
            kind=kind,
            offset=original_offset + 1,
            chunk_offset=None,
            message_id=None,
            committed_revision=None,
            payload=None,
        )
    else:
        trial.pop("scoped_fetch", None)
    for key in ("current_lane", "data"):
        trial.pop(key, None)
    return trial


def _apply_list_envelope_budget(
    shaped: dict[str, object],
    *,
    kind: str,
    row_key: str,
    fetch_base: Mapping[str, object],
) -> dict[str, object]:
    if shaped.get("ok") is not True:
        return _bounded_error_payload(shaped)
    rows_raw = shaped.get(row_key)
    rows: list[dict[str, object]] = (
        [dict(row) for row in rows_raw if isinstance(row, dict)] if isinstance(rows_raw, list) else []
    )
    start = int(fetch_base.get("chunk_offset") or 0)
    original_offset = int(fetch_base.get("offset") or 0)
    original_row_count = len(rows)
    original_has_more = bool(shaped.get("has_more"))
    exact_row = fetch_base.get("message_id") is not None
    more_rows_after = (not exact_row) and (original_has_more or original_row_count > 1)
    cursor_more_rows_after = exact_row and fetch_base.get("cursor_more_rows_after") is True
    continuation_rows_after = more_rows_after or cursor_more_rows_after
    budget = _LANE_COMMUNICATION_MCP_ENVELOPE_BUDGET_BYTES
    if start == 0 and fetch_base.get("chunk_limit") is None and _mcp_envelope_utf8_bytes(shaped) <= budget:
        if not original_has_more:
            return shaped
        attached = _with_more_rows_cursor(
            shaped,
            kind=kind,
            fetch_base=fetch_base,
            next_offset=original_offset + original_row_count,
        )
        if attached is not None:
            return attached
        return _list_identity_budget_refusal(
            task_ref=fetch_base.get("task_ref"),
            lane_id=fetch_base.get("lane_id"),
            kind=kind,
        )

    working = dict(shaped)
    working["truncated"] = True
    if start == 0 and fetch_base.get("chunk_limit") is None:
        while len(rows) > 1:
            rows = rows[:-1]
            candidate = _replace_list_collection(working, row_key=row_key, rows=rows, has_more=True)
            candidate["truncated"] = True
            candidate["scoped_fetch"] = _list_scoped_fetch(
                fetch_base,
                kind=kind,
                offset=original_offset + len(rows),
                chunk_offset=None,
                message_id=None,
            )
            if _mcp_envelope_utf8_bytes(candidate) <= budget:
                return candidate
            working = candidate
    elif rows:
        rows = [dict(rows[0])]

    def _finish_if_within_budget(candidate: dict[str, object]) -> dict[str, object] | None:
        if _mcp_envelope_utf8_bytes(candidate) <= budget:
            return candidate
        stripped = dict(candidate)
        for key in ("current_lane", "data"):
            stripped.pop(key, None)
            if _mcp_envelope_utf8_bytes(stripped) <= budget:
                return stripped
        return None

    if rows:
        row = dict(rows[0])
        message = row.get("message") if isinstance(row.get("message"), str) else ""
        encoded = message.encode("utf-8") if isinstance(message, str) else b""
        if start != 0 and not _utf8_offset_is_boundary(encoded, start):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "invalid_chunk",
                    "error": "chunk_offset must land on a UTF-8 code-point boundary.",
                    "invalid_selector": "chunk_offset",
                    "invalid_value": start,
                }
            )
        remaining = max(0, len(encoded) - start)
        if fetch_base.get("chunk_limit") is not None:
            remaining = min(remaining, int(fetch_base["chunk_limit"]))
        payload_names = _payload_field_names(row)
        minimum_budget: int | None = None

        def _payload_budget_refusal() -> dict[str, object]:
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "projection_budget_exceeded",
                    "error": "Requested full-row payload exceeds the 8192-byte MCP envelope budget.",
                    "invalid_selector": "payload",
                }
            )

        def _search_content(*, include_payload: bool, require_payload_continuation: bool) -> dict[str, object] | None:
            nonlocal minimum_budget
            lo, hi = 0, remaining
            best: dict[str, object] | None = None
            while lo <= hi:
                mid = (lo + hi) // 2
                piece, consumed = _utf8_slice_bytes(encoded, start, mid)
                trial_row = dict(row)
                trial_row["message"] = piece
                omitted: list[str] = []
                if not include_payload:
                    for key in ("payload", "payload_json"):
                        if key in trial_row:
                            trial_row.pop(key, None)
                            omitted.append(key)
                next_offset = start + consumed
                more_content = next_offset < len(encoded)
                if more_content and consumed < 1:
                    lo = mid + 1
                    continue
                dropped_requested_payload = bool(omitted)
                page_has_more = (
                    more_content
                    or continuation_rows_after
                    or (dropped_requested_payload and require_payload_continuation)
                )
                trial = _replace_list_collection(working, row_key=row_key, rows=[trial_row], has_more=page_has_more)
                trial["truncated"] = True
                if omitted:
                    trial["omitted_fields"] = omitted
                if more_content:
                    trial["scoped_fetch"] = _list_scoped_fetch(
                        fetch_base,
                        kind=kind,
                        **_list_content_fetch_overrides(
                            row,
                            exact_row=exact_row,
                            original_offset=original_offset,
                            next_offset=next_offset,
                            fetch_base=fetch_base,
                            more_rows_after=continuation_rows_after,
                        ),
                    )
                elif dropped_requested_payload and require_payload_continuation:
                    trial["scoped_fetch"] = _list_scoped_fetch(
                        fetch_base,
                        kind=kind,
                        **_list_content_fetch_overrides(
                            row,
                            exact_row=exact_row,
                            original_offset=original_offset,
                            next_offset=len(encoded),
                            fetch_base=fetch_base,
                            more_rows_after=continuation_rows_after,
                        ),
                    )
                    trial["has_more"] = True
                    trial["truncated"] = True
                elif continuation_rows_after:
                    trial["scoped_fetch"] = _list_scoped_fetch(
                        fetch_base,
                        kind=kind,
                        offset=original_offset + 1,
                        chunk_offset=None,
                        message_id=None,
                        committed_revision=None,
                        payload=None,
                    )
                else:
                    trial.pop("scoped_fetch", None)
                    trial["truncated"] = start > 0
                    if not trial["truncated"]:
                        trial.pop("truncated", None)
                minimal = dict(trial)
                for key in ("current_lane", "data"):
                    minimal.pop(key, None)
                required = _mcp_envelope_utf8_bytes(minimal)
                minimum_budget = required if minimum_budget is None else min(minimum_budget, required)
                fitted = _finish_if_within_budget(trial)
                if fitted is not None:
                    best = fitted
                    lo = mid + 1
                else:
                    hi = mid - 1
            return best

        kept_payload = _search_content(include_payload=True, require_payload_continuation=False)
        if kept_payload is not None:
            return kept_payload
        if payload_names:
            compact_payload = _compact_payload_page(
                working,
                row_key=row_key,
                row=row,
                kind=kind,
                fetch_base=fetch_base,
                more_rows_after=more_rows_after,
                original_offset=original_offset,
            )
            compact_fits = _mcp_envelope_utf8_bytes(compact_payload) <= budget
            if not compact_fits:
                return _payload_budget_refusal()
            if compact_fits and start >= len(encoded):
                return compact_payload
            dropped_payload = _search_content(
                include_payload=False,
                require_payload_continuation=compact_fits,
            )
            if dropped_payload is not None and (
                compact_fits or dropped_payload.get("truncated") is True or dropped_payload.get("has_more") is True
            ):
                if dropped_payload.get("omitted_fields") and dropped_payload.get("truncated") is not True:
                    return _payload_budget_refusal()
                return dropped_payload
            if compact_fits:
                return compact_payload
            return _payload_budget_refusal()
        dropped_only = _search_content(include_payload=False, require_payload_continuation=False)
        if dropped_only is not None:
            return dropped_only
        if minimum_budget is None:
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "invalid_chunk",
                    "error": "chunk_limit cannot fit the next UTF-8 code point.",
                    "invalid_selector": "chunk_limit",
                }
            )
        return _bounded_error_payload(
            {
                "ok": False,
                "error_kind": "envelope_too_small",
                "error": "The envelope cannot fit a progressing content page.",
                "minimum_budget_bytes": minimum_budget,
            }
        )

    empty = _replace_list_collection(working, row_key=row_key, rows=[], has_more=original_has_more or more_rows_after)
    if original_has_more or start > 0:
        empty["truncated"] = True
        empty["scoped_fetch"] = _list_scoped_fetch(
            fetch_base,
            kind=kind,
            offset=original_offset,
            message_id=fetch_base.get("message_id"),
            chunk_offset=start or None,
            detail="full",
        )
    else:
        empty.pop("truncated", None)
        empty.pop("scoped_fetch", None)
    for key in ("current_lane", "data"):
        if _mcp_envelope_utf8_bytes(empty) <= budget:
            return empty
        empty.pop(key, None)
    if _mcp_envelope_utf8_bytes(empty) <= budget:
        return empty
    return _list_identity_budget_refusal(
        task_ref=fetch_base.get("task_ref"),
        lane_id=fetch_base.get("lane_id"),
        kind=kind,
    )


def _finalize_lane_message_write(
    data: dict[str, object],
    *,
    row: dict[str, object],
    kind: str | None,
    sections: str | None,
    detail: str,
    fields: str | None,
    operation: str,
) -> dict[str, object]:
    try:
        shaped = _shape_lane_message_write(
            data,
            row=row,
            kind=kind,
            sections=sections,
            detail=detail,
            fields=fields,
        )
    except Exception as exc:
        extra: dict[str, object] = {
            "error_kind": "shaping_failed",
            "error": _clip_error_text(f"{type(exc).__name__}: {exc}"),
            "truncated": True,
        }
        if "duplicate_dispatch" in data:
            extra["duplicate_dispatch"] = data["duplicate_dispatch"]
        fallback = _compact_lane_message_ack(row, kind=kind)
        fallback.update(
            {
                "task_ref": row.get("task_ref"),
                "operation": operation,
                "committed_revision": _committed_revision_pin_of(row),
            }
        )
        fallback.update(extra)
        return _json_response(_apply_write_envelope_budget(fallback, row=row, kind=kind, operation=operation))
    if shaped.get("ok") is False:
        return _json_response(shaped)
    return _json_response(_apply_write_envelope_budget(shaped, row=row, kind=kind, operation=operation))


def _shape_lane_message_write(
    data: dict[str, object],
    *,
    row: dict[str, object],
    kind: str | None,
    sections: str | None,
    detail: str,
    fields: str | None,
) -> dict[str, object]:
    """Shape a message write without echoing caller-authored content by default."""
    requested_sections = _parse_sections(sections, _LANE_MESSAGE_WRITE_SECTIONS)
    if sections is not None and requested_sections == frozenset():
        return _invalid_sections_error(_LANE_MESSAGE_WRITE_SECTIONS)

    # Only the explicit full spelling may echo caller-authored content.
    normalized_detail = "full" if detail == "full" else "summary"
    if requested_sections is None:
        requested_sections = frozenset({"identity" if normalized_detail == "summary" else "message"})

    shaped: dict[str, object] = {"ok": True}
    if "identity" in requested_sections:
        ack = _compact_lane_message_ack(row, kind=kind)
        requested_fields = _parse_projection_fields(fields)
        if requested_fields is not None:
            # ``id`` is the stored-row spelling; accept it as an alias for the
            # public compact acknowledgement's ``message_id``.
            if "id" in requested_fields:
                requested_fields = requested_fields | {"message_id"}
            requested_fields = requested_fields or _LANE_MESSAGE_ACK_IDENTITY_FIELDS
            ack = {key: value for key, value in ack.items() if key == "ok" or key in requested_fields}
        shaped.update(ack)

    if "message" in requested_sections:
        projected_row = _public_lane_message_row(row)
        if normalized_detail != "full":
            # Summary writes are acknowledgements, never a shortened echo of
            # caller-authored content. ``detail=full`` is the sole opt-in.
            projected_row.pop("message", None)
            projected_row.pop("payload", None)
            projected_row.pop("payload_json", None)
        projected_row = _project_mapping(
            projected_row,
            _parse_projection_fields(fields),
            _LANE_MESSAGE_IDENTITY_FIELDS,
        )
        shaped["message"] = projected_row
        for key, value in data.items():
            if key not in {"ok", "message"}:
                shaped[key] = value
    return shaped


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _optional_ref(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _json_object(text: object) -> dict[str, Any] | None:
    if not isinstance(text, str) or not text.strip().startswith("{"):
        return None
    try:
        decoded = json.loads(text)
    except (TypeError, ValueError):
        return None
    return dict(decoded) if isinstance(decoded, dict) else None


def _git_run(repo: Path, *args: str) -> tuple[int, str]:
    try:
        completed = subprocess.run(  # noqa: S603
            ["git", *args],
            cwd=str(repo),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    return completed.returncode, stdout or stderr


def _resolve_commit(repo: Path, ref: str) -> str | None:
    code, output = _git_run(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if code == 0 and output:
        return output.splitlines()[0].strip() or None
    return None


_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
NOTES_PROSE_KEY = "prose"


def _is_full_sha(value: object) -> bool:
    return isinstance(value, str) and _FULL_SHA_RE.fullmatch(value) is not None


def _lane_git_repo(worktree_path: str) -> Path | None:
    """Resolve a lane worktree to an existing git repository, never an ambient cwd."""
    candidate = Path(worktree_path).expanduser()
    if not candidate.is_absolute():
        candidate = _workspace_root() / candidate
    try:
        resolved = candidate.resolve()
    except (OSError, RuntimeError):
        return None
    if not resolved.is_dir() or not (resolved / ".git").exists():
        return None
    return resolved


def _load_lane_manifest_entry(task_ref: str, lane_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    path = _workspace_root() / "config" / "lane-orchestration" / f"{task_ref}.json"
    if not path.is_file():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return None, None
    if not isinstance(data, dict):
        return None, None
    lanes = data.get("lanes")
    entry = lanes.get(lane_id) if isinstance(lanes, dict) else None
    return data, entry if isinstance(entry, dict) else None


def _review_subject_missing(tried: list[str], extra: str | None = None) -> dict[str, Any]:
    detail = f"{extra}. " if extra else ""
    tried_text = "; ".join(tried) if tried else "review_base_ref, manifest, merge-base"
    return {
        "ok": False,
        "error_kind": "review_subject_missing",
        "error": (
            f"remote review upsert requires a derivable review_subject; {detail}"
            f"tried {tried_text}. pass review_base_ref (and optionally review_tip_ref) "
            "to manage_worktree_lane upsert"
        ),
    }


def _review_subject_pinned(previous_base: str, previous_tip: str, new_base: str, new_tip: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error_kind": "review_subject_pinned",
        "error": (
            f"existing review_subject is pinned to {previous_base}..{previous_tip}; "
            f"implicit upsert would move it to {new_base}..{new_tip}. "
            "pass update_review_subject=true to manage_worktree_lane upsert"
        ),
    }


def _review_context_pin_exception(
    repo: Path, previous_base: str, previous_tip: str, new_base: str, new_tip: str
) -> tuple[bool, str]:
    """Allow exactly one context-only, single-parent commit above the pin."""
    from workbay_orchestrator_mcp.orchestration.secure_sandbox import REVIEW_CONTEXT_DIRNAME

    if new_base != previous_base:
        return False, "review_base_changed"
    code, parents = _git_run(repo, "rev-list", "--parents", "-n", "1", new_tip)
    if code != 0:
        return False, "context_parent_unavailable"
    if parents.split() != [new_tip, previous_tip]:
        return False, "context_requires_single_parent_at_pinned_tip"
    code, names = _git_run(repo, "diff", "--name-only", "--no-renames", "-z", previous_tip, new_tip)
    if code != 0:
        return False, "context_paths_unavailable"
    paths = [path for path in names.split("\0") if path]
    if not paths:
        return False, "context_commit_empty"
    if any(not path.startswith(f"{REVIEW_CONTEXT_DIRNAME}/") for path in paths):
        return False, "context_commit_changes_other_paths"
    return True, "single_context_commit"


def _merge_notes_envelope(notes: str | None, previous_notes: str | None) -> dict[str, Any]:
    incoming = _json_object(notes)
    if incoming is not None:
        return incoming
    envelope = _json_object(previous_notes) or {}
    if notes is not None:
        envelope[NOTES_PROSE_KEY] = notes
    elif previous_notes is not None and _json_object(previous_notes) is None:
        envelope[NOTES_PROSE_KEY] = previous_notes
    return envelope


def _tier_backend_disagreement(tier: str, backend: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": (f"tier {tier!r} is a codex-remote selector and cannot be persisted with backend {backend!r}"),
        "outcome": "routing_refused",
        "failure_class": "routing",
        "reason": "tier_backend_disagreement",
        "error_kind": "tier_backend_disagreement",
    }


def _derive_remote_review_notes(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    branch: str,
    notes: str | None,
    previous_notes: str | None,
    review_base_ref: str | None,
    review_tip_ref: str | None,
    branch_tip_sha: str | None,
    branch_tip_source: str | None,
    update_review_subject: bool = False,
) -> tuple[dict[str, Any] | None, str | None]:
    previous_envelope = _json_object(previous_notes)
    previous_subject = previous_envelope.get("review_subject") if previous_envelope is not None else None
    if (
        isinstance(previous_subject, dict)
        and _is_full_sha(previous_subject.get("base_ref"))
        and _is_full_sha(previous_subject.get("tip_ref"))
        and _optional_ref(review_base_ref) is None
        and _optional_ref(review_tip_ref) is None
        and not update_review_subject
    ):
        # A status-only upsert does not request a new subject. Preserve the
        # creation-time identity instead of deriving it from the moving branch.
        envelope = _merge_notes_envelope(notes, previous_notes)
        envelope["review_subject"] = previous_subject
        return None, json.dumps(envelope, sort_keys=True)

    repo = _lane_git_repo(worktree_path)
    tried: list[str] = [f"worktree_path={worktree_path!r}"]
    if repo is None:
        return _review_subject_missing(tried, "worktree_path is not an existing git repository"), None
    tip_ref = _optional_ref(review_tip_ref)
    if tip_ref is None:
        tried.append("review_tip_ref (omitted)")
        tip_ref = _optional_ref(branch_tip_sha)
        if tip_ref is None:
            tried.append("branch_tip_sha (omitted)")
            if _optional_ref(branch_tip_source) in {None, "branch"}:
                from workbay_orchestrator_mcp.orchestration.offload_preflight import (  # noqa: PLC0415
                    _canonical_branch_ref,
                )

                canonical, _error = _canonical_branch_ref(branch)
                if canonical is None:
                    tried.append(f"branch {branch!r}")
                    return _review_subject_missing(tried, f"branch {branch!r} is not a canonical heads ref"), None
                tip_ref = canonical
                tried.append(f"branch {canonical!r}")
            else:
                tried.append(f"branch_tip_source={branch_tip_source!r}")
    else:
        tried.append(f"review_tip_ref={tip_ref!r}")
    if tip_ref is None:
        return _review_subject_missing(tried, "tip_ref was not derivable"), None
    resolved_tip = _resolve_commit(repo, tip_ref)
    if resolved_tip is None:
        return _review_subject_missing(tried, f"tip_ref {tip_ref!r} did not resolve to a commit"), None

    base_ref = _optional_ref(review_base_ref)
    derived_from = "explicit"
    if base_ref is None:
        tried.append("review_base_ref (omitted)")
        manifest, entry = _load_lane_manifest_entry(task_ref, lane_id)
        subject = entry.get("review_subject") if entry is not None else None
        if isinstance(subject, dict):
            base_ref = _optional_ref(subject.get("base_ref")) or _optional_ref(subject.get("base_sha"))
        if base_ref is not None:
            derived_from = "manifest"
            tried.append(f"manifest review_subject/base_sha={base_ref!r}")
        else:
            tried.append("manifest review_subject.base_ref/base_sha (absent)")
            integration = "main"
            if isinstance(manifest, dict):
                integration = _optional_ref(manifest.get("integration_branch")) or "main"
            tried.append(f"git merge-base {integration} {tip_ref}")
            code, merge_base = _git_run(repo, "merge-base", integration, resolved_tip)
            if code != 0 or not merge_base:
                tried.append(f"merge-base failed: {merge_base}")
                return _review_subject_missing(tried), None
            base_ref = merge_base.splitlines()[0].strip()
            derived_from = "merge_base"
    else:
        tried.append(f"review_base_ref={base_ref!r}")

    resolved_base = _resolve_commit(repo, base_ref)
    if resolved_base is None:
        return _review_subject_missing(tried, f"base_ref {base_ref!r} did not resolve to a commit"), None
    ancestor_code, _ancestor_out = _git_run(repo, "merge-base", "--is-ancestor", resolved_base, resolved_tip)
    if ancestor_code != 0:
        return _review_subject_missing(tried, f"{base_ref!r} is not an ancestor of tip {tip_ref!r}"), None
    count_code, count_text = _git_run(repo, "rev-list", "--count", f"{resolved_base}..{resolved_tip}")
    try:
        count = int(count_text.splitlines()[0]) if count_code == 0 and count_text else 0
    except ValueError:
        count = 0
    if count <= 0:
        return _review_subject_missing(tried, f"{base_ref}..{tip_ref} is empty"), None
    names_code, names = _git_run(repo, "diff", "--name-only", "--no-renames", f"{resolved_base}..{resolved_tip}")
    changed_files = [line for line in names.splitlines() if line] if names_code == 0 else []
    if (
        isinstance(previous_subject, dict)
        and not update_review_subject
        and _is_full_sha(previous_subject.get("base_ref"))
        and _is_full_sha(previous_subject.get("tip_ref"))
        and (previous_subject.get("base_ref") != resolved_base or previous_subject.get("tip_ref") != resolved_tip)
    ):
        allowed, reason = _review_context_pin_exception(
            repo,
            str(previous_subject.get("base_ref")),
            str(previous_subject.get("tip_ref")),
            resolved_base,
            resolved_tip,
        )
        if not allowed:
            refusal = _review_subject_pinned(
                str(previous_subject["base_ref"]), str(previous_subject["tip_ref"]), resolved_base, resolved_tip
            )
            refusal["reason"] = reason
            return refusal, None
        envelope = _merge_notes_envelope(notes, previous_notes)
        envelope["review_subject"] = previous_subject
        return None, json.dumps(envelope, sort_keys=True)
    envelope = _merge_notes_envelope(notes, previous_notes)
    envelope["review_subject"] = {
        "base_ref": resolved_base,
        "tip_ref": resolved_tip,
        "changed_files": changed_files,
        "derived_from": derived_from,
    }
    return None, json.dumps(envelope, sort_keys=True)


def _reserved_engine_notes_refusal(notes: str | None, previous_notes: str | None) -> dict[str, Any] | None:
    """Refuse caller-authored D1 ratchet context or registry producer bindings."""
    from workbay_orchestrator_mcp.orchestration.review_ratchet import (  # noqa: PLC0415
        ENGINE_LANE_FIELDS,
        PRODUCER_LANE_FIELDS,
    )

    incoming = _json_object(notes)
    if incoming is None:
        return None
    durable = _json_object(previous_notes) or {}
    # An unchanged round trip of the durable row is not authorship; any other value is.
    forged = sorted(
        key
        for key in (*ENGINE_LANE_FIELDS, *PRODUCER_LANE_FIELDS)
        if key in incoming and incoming[key] != durable.get(key)
    )
    if not forged:
        return None
    return {
        "ok": False,
        "error_kind": "reserved_engine_field",
        "error": (
            f"lane notes field(s) {', '.join(forged)} are derived by the review engine or registry producer "
            "from canonical evidence and cannot be written by manage_worktree_lane upsert"
        ),
        "fields": forged,
    }


def _project_review_engine_context(
    *, task_ref: str, worktree_path: str, branch: str, notes: str | None, previous_notes: str | None
) -> tuple[dict[str, Any] | None, str | None]:
    """Project receipt-derived listed_finding_ids/prior_reviewed_tip into the durable notes envelope."""
    from workbay_orchestrator_mcp.orchestration.review_ratchet import (  # noqa: PLC0415
        ENGINE_LANE_FIELDS,
        RatchetRefused,
        load_context,
    )

    envelope = _json_object(notes)
    if envelope is None:
        envelope = _merge_notes_envelope(notes, previous_notes)
    subject = envelope.get("review_subject")
    refs = None
    if isinstance(subject, dict) and _is_full_sha(subject.get("base_ref")) and _is_full_sha(subject.get("tip_ref")):
        refs = (str(subject["base_ref"]), str(subject["tip_ref"]))
    repo = _lane_git_repo(worktree_path)
    context = None
    if repo is not None:
        short_branch = branch[len("refs/heads/") :] if branch.startswith("refs/heads/") else branch
        root = _workspace_root()
        try:
            # Read before open_lane takes its write transaction; the caller's runtime is kept.
            context = load_context(
                repo,
                root / ".task-state",
                task_ref=task_ref,
                branch=short_branch,
                orchestrator_root=root,
                review_subject=refs,
                configure=False,
            )
        except RatchetRefused as exc:
            return {
                "ok": False,
                "error_kind": "review_ratchet_refused",
                "reason": str(exc),
                "error": f"review lane ratchet context is unresolvable: {exc}",
            }, notes
    stale = any(key in envelope for key in ENGINE_LANE_FIELDS)
    for key in ENGINE_LANE_FIELDS:
        envelope.pop(key, None)
    if context is not None:
        envelope["listed_finding_ids"] = context.listed_finding_ids
        envelope["prior_reviewed_tip"] = context.prior_reviewed_tip
    elif not stale:
        return None, notes
    return None, json.dumps(envelope, sort_keys=True)


def _project_task_bindings(
    *, task_ref: str, lane_id: str, worktree_path: str, branch: str, notes: str | None, previous_notes: str | None
) -> tuple[dict[str, Any] | None, str | None]:
    """Carry producer-owned bindings and pin a separate canonical task's own subject.

    The registry producer establishes ``task_subject`` from the durable task,
    manifest owned scope and Git; a public upsert never authors, drops or moves it.
    """
    from workbay_orchestrator_mcp.orchestration.review_ratchet import (  # noqa: PLC0415
        PRODUCER_LANE_FIELDS,
        TASK_SUBJECT_FIELD,
        RatchetRefused,
        pin_task_subject,
    )

    durable = _json_object(previous_notes) or {}
    carried = {key: durable[key] for key in PRODUCER_LANE_FIELDS if key in durable}
    repo = _lane_git_repo(worktree_path)
    if repo is not None:
        short_branch = branch[len("refs/heads/") :] if branch.startswith("refs/heads/") else branch
        try:
            binding = pin_task_subject(
                repo,
                _workspace_root(),
                task_ref=task_ref,
                lane_id=lane_id,
                branch=short_branch,
                worktree_path=worktree_path,
                pinned=carried.get(TASK_SUBJECT_FIELD),
            )
        except RatchetRefused as exc:
            return {
                "ok": False,
                "error_kind": "task_subject_unverified",
                "reason": str(exc),
                "error": f"task subject evidence is unverifiable: {exc}",
            }, notes
        if binding is not None:
            carried[TASK_SUBJECT_FIELD] = binding
    if not carried or (notes is None and all(durable.get(key) == value for key, value in carried.items())):
        return None, notes
    envelope = _json_object(notes)
    if envelope is None:
        envelope = _merge_notes_envelope(notes, previous_notes)
    envelope.update(carried)
    return None, json.dumps(envelope, sort_keys=True)


def _validate_effective_lane_row(
    *,
    effective_backend: object,
    effective_tier: object,
    effective_lane_kind: object,
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    branch: str,
    notes: str | None,
    previous_notes: str | None,
    review_base_ref: str | None,
    review_tip_ref: str | None,
    branch_tip_sha: str | None,
    branch_tip_source: str | None,
    update_review_subject: bool = False,
) -> tuple[dict[str, Any] | None, str | None]:
    """One write-boundary check for the effective row (REF-26 / TIERSPD01RV-001)."""
    normalized_tier = str(effective_tier or "").strip()
    normalized_backend = str(effective_backend or "").strip()
    if normalized_tier in {"junior", "senior"} and normalized_backend != "codex-remote":
        return _tier_backend_disagreement(normalized_tier, normalized_backend), notes
    from workbay_orchestrator_mcp.orchestration.secure_sandbox import (  # noqa: PLC0415
        REVIEW_CONTEXT_PAYLOAD_BACKENDS,
    )

    if str(effective_lane_kind or "implement") != "review" or normalized_backend not in REVIEW_CONTEXT_PAYLOAD_BACKENDS:
        return None, notes
    return _derive_remote_review_notes(
        task_ref=task_ref,
        lane_id=lane_id,
        worktree_path=worktree_path,
        branch=branch,
        notes=notes,
        previous_notes=previous_notes,
        review_base_ref=review_base_ref,
        review_tip_ref=review_tip_ref,
        branch_tip_sha=branch_tip_sha,
        branch_tip_source=branch_tip_source,
        update_review_subject=update_review_subject,
    )


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
    speed: str | None = None,
    tier: str | None = None,
    test_cmd: str | None = None,
    lane_kind: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    task_ref: str | None = None,
    landing_commit_sha: str | None = None,
    branch_tip_sha: str | None = None,
    branch_tip_source: str | None = None,
    review_base_ref: str | None = None,
    review_tip_ref: str | None = None,
    update_review_subject: bool = False,
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
    if status is not None:
        status = _normalize_optional_text(status)
        if status is not None and status not in valid_statuses:
            return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    from workbay_handoff_mcp.lanes_api import get_lane, open_lane  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.backend_registry import cost_class_for_backend  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.host_resources import COST_REMOTE  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_test_cmd import classify_remote_test_cmd  # noqa: PLC0415

    try:
        task_ref = _normalize_optional_text(task_ref)
        if task_ref is None:
            with _get_db_connection() as conn:
                task_ref = _resolve_task_ref(conn, task_ref)
        if not isinstance(task_ref, str) or not task_ref.strip():
            raise ValueError("Lane lookup could not resolve task identity")
        observed = get_lane(lane_id=normalized_lane_id, task_ref=task_ref)
        data = observed["data"]
        if observed["ok"] is True and isinstance(data.get("lane"), dict):
            previous = data["lane"]
        elif (
            observed["ok"] is False
            and isinstance(data.get("error"), str)
            # The read port's complete response attributes absence to this task/lane.
            and data["error"] == f"Lane '{normalized_lane_id}' not found for task '{task_ref}'."
        ):
            previous = None
        else:
            raise ValueError("Lane lookup did not establish presence or absence")
    except Exception as exc:  # noqa: BLE001 — unknown reads must not authorize writes
        return _json_response({"ok": False, "error_kind": "lane_row_read_failed", "error": str(exc)})

    if status is None:
        if isinstance(previous, dict):
            status = str(previous.get("status") or "planned")
        else:
            status = "planned"
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})

    supplied = {"backend": backend, "lane_kind": lane_kind, "test_cmd": test_cmd, "tier": tier}
    defaults = {"backend": None, "lane_kind": "implement", "test_cmd": None, "tier": None}
    baseline = previous or defaults
    effective = {
        key: value if value is not None else baseline.get(key, defaults[key]) for key, value in supplied.items()
    }
    changed = previous is None or any(
        effective[key] != previous.get(key) for key in ("backend", "lane_kind", "test_cmd")
    )
    if (
        changed
        and effective["lane_kind"] == "implement"
        and cost_class_for_backend(effective["backend"]) == COST_REMOTE
    ):
        warnings: list[str] = []
        decision, reason = classify_remote_test_cmd(
            effective["test_cmd"], repository_root=_workspace_root(), capability_warnings=warnings
        )
        if decision == "refuse":
            return _json_response(
                {"ok": False, "error_kind": "test_cmd_unusable", "error": reason, "warnings": warnings}
            )

    route_values = {
        "backend": backend,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "speed": speed,
        "tier": tier,
    }
    routing_manifest_entry: dict[str, object] | None = None
    route_changed = isinstance(previous, dict) and any(
        value is not None and value != previous.get(field) for field, value in route_values.items()
    )
    if route_changed:
        manifest_path = _workspace_root() / "config" / "lane-orchestration" / f"{task_ref}.json"
        if manifest_path.is_file():
            try:
                from .orchestration.lane_manifest import load_manifest  # noqa: PLC0415

                manifest = load_manifest(task_ref, orchestrator_root=str(_workspace_root()))
                entries = manifest.get("lanes")
                candidate = entries.get(normalized_lane_id) if isinstance(entries, dict) else None
                if isinstance(candidate, dict):
                    routing_manifest_entry = candidate
            except Exception as exc:  # noqa: BLE001 — a broken mirror cannot authorize a direct route write
                return _routing_refusal_response(
                    f"routing switch failed: {exc}",
                    reason="routing_switch_failed",
                )
    guarded_route = route_changed and routing_manifest_entry is not None and isinstance(previous, dict)

    previous_notes = (
        str(previous.get("notes")) if isinstance(previous, dict) and previous.get("notes") is not None else None
    )
    refusal = _reserved_engine_notes_refusal(notes, previous_notes)
    if refusal is not None:
        return _json_response(refusal)
    refusal, notes = _validate_effective_lane_row(
        effective_backend=effective["backend"],
        # open_lane keeps an omitted tier, so the row inherits it. The guarded
        # switch instead re-derives an omitted tier from the caller model and
        # refuses a tier model on another backend itself (resolve_routing_quad);
        # there only an explicitly supplied tier is the request's own.
        effective_tier=tier if guarded_route else effective["tier"],
        effective_lane_kind=effective["lane_kind"],
        task_ref=task_ref,
        lane_id=normalized_lane_id,
        worktree_path=normalized_path,
        branch=normalized_branch,
        notes=notes,
        previous_notes=previous_notes,
        review_base_ref=review_base_ref,
        review_tip_ref=review_tip_ref,
        branch_tip_sha=branch_tip_sha,
        branch_tip_source=branch_tip_source,
        update_review_subject=update_review_subject,
    )
    if refusal is not None:
        return _json_response(refusal)
    if effective["lane_kind"] == "review":
        refusal, notes = _project_review_engine_context(
            task_ref=task_ref,
            worktree_path=normalized_path,
            branch=normalized_branch,
            notes=notes,
            previous_notes=previous_notes,
        )
        if refusal is not None:
            return _json_response(refusal)
    refusal, notes = _project_task_bindings(
        task_ref=task_ref,
        lane_id=normalized_lane_id,
        worktree_path=normalized_path,
        branch=normalized_branch,
        notes=notes,
        previous_notes=previous_notes,
    )
    if refusal is not None:
        return _json_response(refusal)

    open_lane_values: dict[str, object] = {
        "lane_id": normalized_lane_id,
        "worktree_path": normalized_path,
        "branch": normalized_branch,
        "title": title,
        "objective": objective,
        "owner_agent": owner_agent,
        "model": model,
        "backend": backend,
        "reasoning_effort": reasoning_effort,
        "speed": speed,
        "tier": tier,
        "test_cmd": test_cmd,
        "lane_kind": lane_kind,
        "status": status,
        "notes": notes,
        "task_ref": task_ref,
        "landing_commit_sha": landing_commit_sha,
        "branch_tip_sha": branch_tip_sha,
        "branch_tip_source": branch_tip_source,
    }
    if guarded_route:
        lock_path, intent_path = _routing_state_paths(task_ref, normalized_lane_id)
        lock_fd = _try_acquire_routing_guard(lock_path)
        if lock_fd is None:
            return _routing_refusal_response(
                f"A routing switch is already in progress for lane '{normalized_lane_id}'.",
                reason="routing_switch_in_progress",
            )
        try:
            route_result = _update_worktree_lane_routing_locked(
                task_ref=task_ref,
                lane_id=normalized_lane_id,
                caller={},
                intent_path=intent_path,
                upsert_route_values=route_values,
                upsert_values=open_lane_values,
            )
            if route_result.get("ok") is not True:
                return route_result
            # Route state has already gone through the guarded manifest/DB
            # protocol. Keep the other upsert fields on their existing path.
            for field in _ROUTING_MANIFEST_FIELDS:
                open_lane_values[field] = None
            return _with_routing_recovery(
                _adapt_lane_envelope(open_lane(**open_lane_values)), route_result.get("routing_recovery")
            )
        finally:
            _release_routing_guard(lock_path, lock_fd)

    return _adapt_lane_envelope(open_lane(**open_lane_values))


def rebind_worktree_lane(
    *,
    from_task_ref: str,
    lane_id: str,
    to_task_ref: str,
    expected_row_id: int,
    expected_identity_seq: int | None,
    expected_task_revision: int,
    reason: str,
    to_lane_id: str | None = None,
    actor: str | None = None,
) -> dict:
    """Correct a lane registered under the wrong task without rewriting history.

    The canonical task must first prove it owns the registered branch's source
    (the same producer evidence that pins ``task_subject``). Its own
    registration of that branch and worktree is created by the ordinary upsert
    when absent, then the handoff port records the fenced, audited
    supersession. The misregistered row, its custody and cleanup history are
    never modified; a new upsert alone never moves or hides it.
    """
    from workbay_handoff_mcp.lanes_api import get_lane, rebind_lane  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.review_ratchet import (  # noqa: PLC0415
        RatchetRefused,
        derive_task_subject,
        task_record,
    )
    from workbay_orchestrator_mcp.orchestration.secure_sandbox import (  # noqa: PLC0415
        review_subject_from_lane_row,
    )

    source_task = _normalize_optional_text(from_task_ref)
    source_lane = _normalize_optional_text(lane_id)
    target_task = _normalize_optional_text(to_task_ref)
    target_lane = _normalize_optional_text(to_lane_id) or source_lane
    if (source_task is None or source_lane is None or target_task is None or source_task == target_task
            or _normalize_optional_text(reason) is None or type(expected_row_id) is not int
            or type(expected_task_revision) is not int
            or not (expected_identity_seq is None or type(expected_identity_seq) is int)):
        return _json_response(
            {"ok": False, "error_kind": "lane_rebind_invalid", "error": "from_task_ref, lane_id, to_task_ref required."}
        )

    def lookup(task: str, lane: str) -> tuple[bool, dict | None]:
        observed = get_lane(lane_id=lane, task_ref=task)
        data = observed.get("data") or {}
        if observed.get("ok") is True and isinstance(data.get("lane"), dict):
            return True, data["lane"]
        return data.get("error") == f"Lane '{lane}' not found for task '{task}'.", None

    read_ok, old = lookup(source_task, source_lane)
    if old is None:
        kind = "lane_rebind_source_missing" if read_ok else "lane_row_read_failed"
        return _json_response({"ok": False, "error_kind": kind, "error": f"lane {source_task}/{source_lane} unread"})
    task = task_record(target_task)
    if (
        old.get("id") != expected_row_id
        or old.get("identity_seq") != expected_identity_seq
        or task is None
        or task["revision"] != expected_task_revision
    ):
        # Checked again inside the write transaction; refuse early so a stale
        # request never registers anything.
        return _json_response({"ok": False, "error_kind": "lane_rebind_stale", "error": "fenced identity changed"})
    worktree_path, branch = str(old["worktree_path"]), str(old["branch"])
    source_owner = task_record(source_task)
    if source_owner and str(source_owner.get("target_branch") or "").removeprefix("refs/heads/") == branch.removeprefix("refs/heads/"):
        return _json_response({"ok": False, "error_kind": "lane_rebind_source_owns_branch",
                               "error": "source task still owns this branch"})
    repo = _lane_git_repo(worktree_path)
    try:
        proof = (
            derive_task_subject(
                repo,
                _workspace_root(),
                task_ref=target_task,
                lane_id=target_lane,
                branch=branch.removeprefix("refs/heads/"),
                worktree_path=worktree_path,
            )
            if repo is not None
            else None
        )
    except RatchetRefused as exc:
        return _json_response(
            {"ok": False, "error_kind": "lane_rebind_source_unverified", "reason": str(exc), "error": str(exc)}
        )
    if proof is None:
        return _json_response(
            {
                "ok": False,
                "error_kind": "lane_rebind_source_unproven",
                "error": (
                    f"task {target_task!r} does not prove it owns {branch!r}: its canonical task record, manifest "
                    f"lane {target_lane!r} owned scope/base and Git source must agree"
                ),
            }
        )
    read_ok, corrected = lookup(target_task, target_lane)
    if corrected is None:
        if not read_ok:
            return _json_response({"ok": False, "error_kind": "lane_row_read_failed", "error": "corrected lane unread"})
        subject = review_subject_from_lane_row(old)
        created = upsert_worktree_lane(
            lane_id=target_lane,
            worktree_path=worktree_path,
            branch=branch,
            title=old.get("title"),
            objective=old.get("objective"),
            model=old.get("model"),
            backend=old.get("backend"),
            reasoning_effort=old.get("reasoning_effort"),
            test_cmd=old.get("test_cmd"),
            lane_kind=old.get("lane_kind"),
            status=old["status"] if old.get("status") in {"planned", "active", "blocked", "review"} else "active",
            task_ref=target_task,
            review_base_ref=subject[0] if subject else None,
            review_tip_ref=subject[1] if subject else None,
        )
        if created.get("ok") is not True:
            return created
    return _adapt_lane_envelope(
        rebind_lane(
            from_task_ref=source_task,
            lane_id=source_lane,
            to_task_ref=target_task,
            to_lane_id=target_lane,
            expected_row_id=expected_row_id,
            expected_identity_seq=expected_identity_seq,
            expected_task_revision=expected_task_revision,
            reason=reason,
            actor=actor,
        )
    )


_ROUTING_MANIFEST_FIELDS = {
    "backend": "preferred_backend",
    "model": "preferred_model",
    "reasoning_effort": "preferred_reasoning_effort",
    "speed": "preferred_speed",
    "tier": "preferred_tier",
}
_ROUTING_COLUMNS = tuple(_ROUTING_MANIFEST_FIELDS)
_ROUTING_INTENT_STATE_KEYS = frozenset({"database_before", "manifest_before", "routing_after"})
_ROUTING_INTENT_IDENTITY_KEYS = frozenset({"task_ref", "lane_id"})


def _manifest_route_from_row(row: Mapping[str, object] | sqlite3.Row) -> dict[str, object]:
    return {_ROUTING_MANIFEST_FIELDS[column]: row[column] for column in _ROUTING_COLUMNS}


def _mirror_route(route: Mapping[str, object]) -> dict[str, object]:
    """Manifest spelling of a DB route: a stored empty-string clear is an absent key."""
    return {key: None if isinstance(value, str) and not value.strip() else value for key, value in route.items()}


def _with_routing_recovery(response: dict[str, object], recovery: object) -> dict[str, object]:
    """Report a legacy-field recovery on whatever the guarded call returns."""
    if isinstance(recovery, dict):
        response["routing_recovery"] = recovery
    return response


def _routing_refusal_response(error: str, *, reason: str) -> dict[str, object]:
    return _json_response(
        {
            "ok": False,
            "error": error,
            "outcome": "routing_refused",
            "failure_class": "routing",
            "reason": reason,
        }
    )


def _upsert_routing_caller(
    *,
    route_values: dict[str, object],
    previous: dict[str, object],
    manifest_entry: dict[str, object],
) -> dict[str, object]:
    core_fields = ("backend", "model", "reasoning_effort", "speed")
    if not any(route_values[field] is not None for field in core_fields):
        return {"tier": route_values["tier"]}

    caller: dict[str, object] = {}
    for field in core_fields:
        value = route_values[field]
        if value is None and field == "speed" and _speed_not_applicable(_normalize_optional_text(caller["backend"])):
            # Service-tier speed is provider-only: an omitted speed stays
            # omitted for a speedless backend, so the resolver clears the
            # inherited value instead of refusing it as explicit caller input.
            continue
        if value is None:
            value = previous.get(field)
        if value is None:
            value = manifest_entry.get(_ROUTING_MANIFEST_FIELDS[field])
        # A clear value makes a complete caller quad when both persisted
        # sources left this coordinate unset, matching the old NULL row value.
        caller[field] = value if value is not None else ""
    if route_values["tier"] is not None:
        caller["tier"] = route_values["tier"]
    return caller


def _routing_upsert_rejection(*, values: dict[str, object], row: dict[str, object]) -> dict | None:
    """Preflight open_lane's rejection conditions before either route store changes.

    The identity parser and status set belong to the row writer. Keep its
    terminal/released-row checks here because the route switch precedes that
    writer, and never authorize a request it would reject.
    """
    from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
        OPENABLE_LANE_STATUSES,
        _parse_lane_identity,
    )

    _, _, _, identity_error = _parse_lane_identity(
        values.get("landing_commit_sha"), values.get("branch_tip_sha"), values.get("branch_tip_source")
    )
    if identity_error is not None:
        return _json_response({"ok": False, "error": identity_error, "error_kind": "invalid_lane_identity"})
    if values.get("lane_kind") is not None and values["lane_kind"] not in ("implement", "review"):
        return _json_response({"ok": False, "error": "Invalid lane_kind. Valid: implement, review"})
    status = values["status"]
    status_changed = status != row["status"]
    released = row["status"] == "closed" and str(row.get("notes") or "").lower().startswith("released_no_work:")
    effective_notes = values.get("notes") if values.get("notes") is not None else row.get("notes")
    if released and (status_changed or not str(effective_notes or "").lower().startswith("released_no_work:")):
        return _json_response(
            {
                "ok": False,
                "error": "Lane status changed or the released row is terminal.",
                "error_kind": "lane_status_conflict",
            }
        )
    if status_changed and (
        row["status"] in {"closed", "merged", "closed_stale"} or row.get("landing_commit_sha") is not None
    ):
        return _json_response(
            {
                "ok": False,
                "error": "Terminal lanes require an explicit reopen_reason before changing status.",
                "error_kind": "lane_terminal",
                "lane": row,
            }
        )
    if status not in OPENABLE_LANE_STATUSES and status_changed:
        return _json_response(
            {
                "ok": False,
                "error": f"Invalid status for open_lane. Valid: {', '.join(sorted(OPENABLE_LANE_STATUSES))}. "
                "Use close_lane for closed|merged (reclaims lane_messages).",
            }
        )
    return None


def _routing_manifest_snapshot(entry: dict[str, object]) -> dict[str, dict[str, object]]:
    return {
        manifest_key: {"present": manifest_key in entry, "value": entry.get(manifest_key)}
        for manifest_key in _ROUTING_MANIFEST_FIELDS.values()
    }


def _update_manifest_routing(
    *,
    manifest_path: Path,
    lane_id: str,
    values: dict[str, object],
    restore: bool = False,
) -> dict[str, dict[str, object]]:
    """Write the lane's routing keys; return their snapshot from under the manifest lock."""
    from .orchestration.lane_manifest import atomic_update_manifest  # noqa: PLC0415

    prior: dict[str, dict[str, object]] = {}

    def _apply(manifest: dict[str, Any]) -> None:
        entries = manifest.get("lanes")
        lane = entries.get(lane_id) if isinstance(entries, dict) else None
        if not isinstance(lane, dict):
            raise RuntimeError(f"lane {lane_id!r} missing from routing manifest")
        prior.update(_routing_manifest_snapshot(lane))
        if restore:
            for key, state in values.items():
                if not isinstance(state, dict):
                    raise RuntimeError(f"invalid routing intent snapshot for {key!r}")
                if state.get("present"):
                    lane[key] = state.get("value")
                else:
                    lane.pop(key, None)
            return
        for key, value in values.items():
            if value is None:
                lane.pop(key, None)
            else:
                lane[key] = value

    atomic_update_manifest(manifest_path, _apply)
    return prior


def _routing_state_paths(task_ref: str, lane_id: str) -> tuple[Path, Path]:
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

    state_dir = Path(get_runtime_config().state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_digest = hashlib.sha256(f"{task_ref}\0{lane_id}".encode("utf-8")).hexdigest()
    lock_path = state_dir / f"lane-routing-{lock_digest}.lock"
    intent_path = state_dir / f"lane-routing-{lock_digest}.intent.json"
    return lock_path, intent_path


def _try_acquire_routing_guard(lock_path: Path) -> int | None:
    """Acquire a per-lane flock while serializing opens against guard cleanup."""
    state_dir_fd = os.open(lock_path.parent, os.O_RDONLY)
    lock_fd: int | None = None
    lane_busy = False
    try:
        fcntl.flock(state_dir_fd, fcntl.LOCK_EX)
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lane_busy = True
    except BaseException:
        if lock_fd is not None:
            os.close(lock_fd)
            lock_fd = None
        raise
    finally:
        try:
            os.close(state_dir_fd)
        except BaseException:
            if lock_fd is not None:
                os.close(lock_fd)
                lock_fd = None
            raise
    if lane_busy:
        if lock_fd is not None:
            os.close(lock_fd)
        return None
    return lock_fd


def _release_routing_guard(lock_path: Path, lock_fd: int) -> None:
    """Release and remove a per-lane guard without racing another opener."""
    state_dir_fd: int | None = None
    active_lock_fd: int | None = lock_fd
    try:
        state_dir_fd = os.open(lock_path.parent, os.O_RDONLY)
        fcntl.flock(state_dir_fd, fcntl.LOCK_EX)
        os.close(active_lock_fd)
        active_lock_fd = None
        lock_path.unlink(missing_ok=True)
    finally:
        if active_lock_fd is not None:
            os.close(active_lock_fd)
        if state_dir_fd is not None:
            os.close(state_dir_fd)


def _write_routing_intent(path: Path, payload: dict[str, object]) -> None:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def _remove_routing_intent(path: Path) -> None:
    path.unlink(missing_ok=True)
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _load_routing_intent(
    intent_path: Path, *, task_ref: str, lane_id: str
) -> tuple[dict[str, object], dict[str, dict[str, object]], dict[str, object]]:
    """Parse the routing journal strictly; refuse any payload this protocol never writes.

    Intents written before the identity keys existed carry only the three state
    keys and stay recoverable; an identity naming another task or lane is foreign.
    """
    try:
        payload = json.loads(intent_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RoutingDisagreement("routing_intent_malformed", f"routing intent is not valid JSON: {exc}") from exc
    if (
        not isinstance(payload, dict)
        or not _ROUTING_INTENT_STATE_KEYS.issubset(payload)
        or set(payload) - _ROUTING_INTENT_STATE_KEYS - _ROUTING_INTENT_IDENTITY_KEYS
    ):
        raise RoutingDisagreement(
            "routing_intent_malformed",
            "routing intent must hold exactly database_before, manifest_before and routing_after "
            "(plus optional task_ref/lane_id)",
        )
    for key, expected in (("task_ref", task_ref), ("lane_id", lane_id)):
        if key in payload and payload[key] != expected:
            raise RoutingDisagreement(
                "routing_intent_foreign", f"routing intent belongs to {key} {payload[key]!r}, not {expected!r}"
            )
    routing_keys = set(_ROUTING_MANIFEST_FIELDS.values())
    for key in ("database_before", "routing_after"):
        route = payload[key]
        if (
            not isinstance(route, dict)
            or set(route) != routing_keys
            or any(value is not None and not isinstance(value, str) for value in route.values())
        ):
            raise RoutingDisagreement("routing_intent_malformed", f"routing intent {key} is not a routing route")
    snapshot = payload["manifest_before"]
    if (
        not isinstance(snapshot, dict)
        or set(snapshot) != routing_keys
        or any(
            not isinstance(state, dict)
            or set(state) != {"present", "value"}
            or not isinstance(state["present"], bool)
            or (state["value"] is not None and not isinstance(state["value"], str))
            or (not state["present"] and state["value"] is not None)
            for state in snapshot.values()
        )
    ):
        raise RoutingDisagreement(
            "routing_intent_malformed", "routing intent manifest_before is not a routing snapshot"
        )
    return payload["database_before"], snapshot, payload["routing_after"]


def _legacy_route_canonical(route: Mapping[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    """Split a DB route into its canonical form and the obsolete OpenAI-only values it held.

    The API contract leaves one representable speed and tier for a registered
    backend other than codex-remote: unset. ``normalize_speed`` leaves speed unset
    for a backend without declared service tiers and refuses any non-blank value;
    the row write boundary (``_validate_effective_lane_row``) and the manifest
    validator treat junior/senior tier as a codex-remote selector. Only those
    known OpenAI values are cleared. Backend, model and effort are never touched,
    and every other value stays in place for the normal validators to refuse.
    """
    from .orchestration.backend_registry import BACKENDS  # noqa: PLC0415
    from .orchestration.codex_lane_config import CODEX_SPEED_TO_SERVICE_TIER  # noqa: PLC0415

    canonical = dict(route)
    cleared: dict[str, object] = {}
    backend = route[_ROUTING_MANIFEST_FIELDS["backend"]]
    if not isinstance(backend, str) or backend == "codex-remote" or backend not in BACKENDS:
        return canonical, cleared
    speed_key = _ROUTING_MANIFEST_FIELDS["speed"]
    speed = route[speed_key]
    if speed in CODEX_SPEED_TO_SERVICE_TIER and normalize_speed(backend, None) is None:
        try:
            normalize_speed(backend, speed)
        except InvalidSpeed:
            cleared[speed_key] = speed
            canonical[speed_key] = None
    tier_key = _ROUTING_MANIFEST_FIELDS["tier"]
    if route[tier_key] in ("junior", "senior"):
        cleared[tier_key] = route[tier_key]
        canonical[tier_key] = None
    return canonical, cleared


def _check_route_journaled(
    *,
    route: dict[str, object],
    previous_database: dict[str, object],
    target: dict[str, object],
) -> None:
    """Refuse to settle a pending intent on a row it does not account for.

    The journal accounts for its two recorded sides and for the one row this
    recovery itself commits: the canonical form of the after-side, written only
    when every cleared value was inherited from the before-side
    (``_check_legacy_route_clear``). A crash between that commit and the intent
    removal leaves exactly that row. Any other row was moved by a writer outside
    the routing guard after the intent, so neither mirroring it nor deleting the
    journal is authorized.
    """
    if route == previous_database or route == target:
        return
    canonical, cleared = _legacy_route_canonical(target)
    if route == canonical and all(previous_database[key] == value for key, value in cleared.items()):
        return
    raise RoutingDisagreement(
        "routing_moved_since_intent",
        "lane routing matches neither journaled side of the pending routing intent",
    )


def _check_legacy_route_clear(
    *,
    route: dict[str, object],
    previous_database: dict[str, object],
    canonical: dict[str, object],
    cleared: dict[str, object],
) -> None:
    """Refuse a legacy clear the journal does not authorize or that would bless an invalid route.

    ``_check_route_journaled`` has already tied ``route`` to a journaled side.
    """
    backend = str(route[_ROUTING_MANIFEST_FIELDS["backend"]])
    for key, value in cleared.items():
        if previous_database[key] == value:
            continue
        # The interrupted request introduced this value, so it was explicitly
        # requested; dropping it would silently change the request.
        if key == _ROUTING_MANIFEST_FIELDS["speed"]:
            raise InvalidSpeed(backend, str(value))
        raise RoutingDisagreement(
            "tier_backend_disagreement",
            f"tier {value!r} is a codex-remote selector and cannot be persisted with backend {backend!r}",
        )
    # Clearing speed/tier must not bless an unrelated invalid route: the
    # resolver refuses a codex tier model on this backend or an unknown tier,
    # and normalize_speed refuses a speed that was not an OpenAI service tier.
    resolved = resolve_routing_quad(
        row={column: canonical[_ROUTING_MANIFEST_FIELDS[column]] for column in _ROUTING_COLUMNS},
        tier_table=CODEX_MODEL_TIERS,
    )
    normalize_speed(resolved.backend, resolved.speed)


def _clear_legacy_route_fields(
    *,
    task_ref: str,
    lane_id: str,
    manifest_path: Path,
    route: dict[str, object],
    canonical: dict[str, object],
) -> dict[str, dict[str, object]]:
    """Publish the canonical mirror, then compare-and-set the row; roll the mirror back on failure.

    Lock order (CON-13): caller's routing guard, manifest sidecar, SQLite write
    lock. Retain publication exclusion through the CAS and every rollback so
    compensation cannot overwrite a supported publisher (CON-05). The native
    manifest lock is same-thread reentrant: both mirror writes reuse it. Its
    named timeout and SQLite busy_timeout bound acquisition (RES-02); no wait
    depends on a publisher acquiring this held lock (CON-18). This order breaks
    circular wait (CON-21). Returns the mirror's prior snapshot.
    """
    from .orchestration.lane_manifest import manifest_write_lock  # noqa: PLC0415

    # save_manifest, atomic_update_manifest, create_or_update_manifest and the
    # compiler using it take this fence. Direct file writes bypass it and are
    # unsupported. DB-only writers (including handoff open_lane) take SQLite's
    # write lock; a route they committed before our CAS is refused unchanged.
    with manifest_write_lock(manifest_path):
        manifest_prior = _update_manifest_routing(
            manifest_path=manifest_path,
            lane_id=lane_id,
            values=_mirror_route(canonical),
        )
        try:
            with _get_db_connection(begin_immediate=True) as conn:
                current = conn.execute(
                    "SELECT backend, model, reasoning_effort, speed, tier FROM worktree_lanes "
                    "WHERE task_ref = ? AND lane_id = ?",
                    (task_ref, lane_id),
                ).fetchone()
                if current is None or _manifest_route_from_row(current) != route:
                    raise RoutingDisagreement(
                        "routing_moved_during_recovery",
                        f"lane {lane_id!r} routing changed during routing recovery",
                    )
                conn.execute(
                    "UPDATE worktree_lanes SET speed = ?, tier = ?, updated_at = datetime('now') "
                    "WHERE task_ref = ? AND lane_id = ?",
                    (
                        canonical[_ROUTING_MANIFEST_FIELDS["speed"]],
                        canonical[_ROUTING_MANIFEST_FIELDS["tier"]],
                        task_ref,
                        lane_id,
                    ),
                )
        except Exception as exc:
            try:
                _update_manifest_routing(
                    manifest_path=manifest_path,
                    lane_id=lane_id,
                    values=manifest_prior,
                    restore=True,
                )
            except Exception as rollback_exc:  # noqa: BLE001 -- report both; the intent stays for the next call
                raise RuntimeError(f"{exc}; manifest rollback failed: {rollback_exc}") from exc
            raise
        return manifest_prior


def _settle_routing_intent(
    *,
    task_ref: str,
    lane_id: str,
    intent_path: Path,
    route: dict[str, object],
    manifest_path: Path | None = None,
    expected_manifest_snapshot: dict[str, dict[str, object]] | None = None,
    tolerate_cleanup_errors: bool = False,
) -> None:
    """Remove the journal only while the row, and with ``manifest_path`` this lane's mirror, still hold ``route``.

    Resources, in the one global order every multi-lock routing site follows
    (CON-13): the caller's per-lane routing guard, then the manifest sidecar
    flock (``manifest_write_lock``, bounded by its named timeout), then the
    SQLite write lock (``BEGIN IMMEDIATE``, bounded by busy_timeout). The flock
    is taken before SQLite and never waited for under it (CON-18), so circular
    wait is designed out (CON-21). The journal unlink, under both locks after
    both stores are re-read, is the settlement's linearization point: a writer
    that committed before it is refused here, and one that commits after it
    runs after a finished recovery.

    Supported accessors. Every writer of ``worktree_lanes`` must take the SQLite
    write lock to commit, whether or not it takes the routing guard (the
    handoff ``open_lane`` does not). Every manifest publisher that goes through
    ``manifest_write_lock`` (``save_manifest``, ``atomic_update_manifest``,
    ``create_or_update_manifest`` and the compiler recompile that uses it) is
    serialized against the mirror check. A manifest write that bypasses the
    sidecar lock (a direct ``write_text``/``os.replace``, ``generate_lane_manifest
    --stdout`` redirected onto the file, a hand edit) is unsupported and not
    fenced. Without ``manifest_path`` only the row is checked.

    Only this lane's routing keys are compared, so a publication of another
    lane or of non-routing metadata never manufactures a conflict. The mirror
    must equal ``expected_manifest_snapshot`` exactly (a before-state restore);
    by default it must hold ``route`` in its manifest spelling, where an absent
    key and an empty-string clear are the same value (``_mirror_route``).

    Refusals write nothing and keep the journal: ``routing_manifest_moved_during_recovery``
    (checked first) when a publisher moved this lane's routing, and
    ``routing_moved_during_recovery`` when a writer moved the row.
    Fresh settlement may opt into tolerating journal removal I/O failures only
    after these fenced coherence checks; recovery callers propagate them by default.
    """
    from contextlib import nullcontext  # noqa: PLC0415

    from .orchestration.lane_manifest import manifest_write_lock  # noqa: PLC0415

    with manifest_write_lock(manifest_path) if manifest_path is not None else nullcontext():
        if manifest_path is not None:
            document = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else None
            entries = document.get("lanes") if isinstance(document, dict) else None
            entry = entries.get(lane_id) if isinstance(entries, dict) else None
            if not isinstance(entry, dict):
                raise RoutingDisagreement(
                    "routing_manifest_moved_during_recovery",
                    f"lane {lane_id!r} left the routing manifest during routing recovery",
                )
            if expected_manifest_snapshot is not None:
                actual_snapshot = _routing_manifest_snapshot(entry)
                expected = {key: state.get("value") for key, state in expected_manifest_snapshot.items()}
                actual = {key: state.get("value") for key, state in actual_snapshot.items()}
                moved = actual_snapshot != expected_manifest_snapshot
            else:
                expected = _mirror_route(route)
                actual = _mirror_route({key: entry.get(key) for key in _ROUTING_MANIFEST_FIELDS.values()})
                moved = actual != expected
            if moved:
                raise RoutingDisagreement(
                    "routing_manifest_moved_during_recovery",
                    f"lane {lane_id!r} routing mirror changed during routing recovery",
                    disagreements=tuple(
                        (key, expected.get(key), actual.get(key))
                        for key in _ROUTING_MANIFEST_FIELDS.values()
                        if expected.get(key) != actual.get(key)
                    ),
                )
        with _get_db_connection(begin_immediate=True) as conn:
            current = conn.execute(
                "SELECT backend, model, reasoning_effort, speed, tier FROM worktree_lanes "
                "WHERE task_ref = ? AND lane_id = ?",
                (task_ref, lane_id),
            ).fetchone()
            if current is None or _manifest_route_from_row(current) != route:
                raise RoutingDisagreement(
                    "routing_moved_during_recovery",
                    f"lane {lane_id!r} routing changed during routing recovery",
                )
            try:
                _remove_routing_intent(intent_path)
            except OSError as cleanup_exc:
                if not tolerate_cleanup_errors:
                    raise
                _logger.warning("routing intent cleanup failed for %s/%s: %s", task_ref, lane_id, cleanup_exc)


def _recover_interrupted_routing_update(
    *,
    task_ref: str,
    lane_id: str,
    manifest_path: Path,
    intent_path: Path,
) -> dict[str, object] | None:
    """Reconcile an interrupted manifest mirror from the authoritative DB row.

    Runs under the caller's per-lane routing guard. A row still at the journaled
    before-state (crash before the DB commit) restores the journaled manifest;
    otherwise a row the journal accounts for (``_check_route_journaled``) is
    mirrored, and any other row is refused with the intent, row and manifest
    kept as they are. Those two ordinary branches settle through the manifest
    fence (``_settle_routing_intent``): a supported publisher that moved this
    lane's routing mirror after the recovery publication is refused with
    ``routing_manifest_moved_during_recovery``, keeping its publication and the
    intent; a row moved after the read restores the mirror the publication
    replaced. A row holding obsolete OpenAI-only speed/tier
    on a backend that cannot carry them (``_legacy_route_canonical``) cannot be
    mirrored verbatim, so those values are cleared in the mirror and then, by
    compare-and-set, in the row; backend, model and effort stay as the row holds
    them. The intent stays until both stores agree, so a crash on either side of
    that commit converges on the next guarded call. Returns a receipt when
    legacy values were cleared.
    """
    if not intent_path.is_file():
        return None
    if not manifest_path.is_file():
        raise RuntimeError("routing manifest is missing while a routing intent is pending")
    previous_database, previous_manifest, target = _load_routing_intent(intent_path, task_ref=task_ref, lane_id=lane_id)
    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT backend, model, reasoning_effort, speed, tier FROM worktree_lanes "
            "WHERE task_ref = ? AND lane_id = ?",
            (task_ref, lane_id),
        ).fetchone()
    if row is None:
        raise RuntimeError("lane is missing while a routing intent is pending")
    db_target = _manifest_route_from_row(row)
    receipt: dict[str, object] | None = None
    if db_target == previous_database and db_target != target:
        from .orchestration.lane_manifest import manifest_write_lock  # noqa: PLC0415

        manifest_prior = _update_manifest_routing(
            manifest_path=manifest_path,
            lane_id=lane_id,
            values=previous_manifest,
            restore=True,
        )
        # Hold the manifest lock from the settlement's mirror check through any
        # rollback, so a refused settlement restores exactly its own publication.
        with manifest_write_lock(manifest_path):
            try:
                _settle_routing_intent(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    intent_path=intent_path,
                    route=db_target,
                    manifest_path=manifest_path,
                    expected_manifest_snapshot=previous_manifest,
                )
            except Exception as exc:
                # The unlink is the settlement point: a journal already gone was
                # settled under the write lock, so its mirror already matches the row.
                # A supported publisher moved this lane's mirror after the restore:
                # keep its publication and the journal.
                if not intent_path.is_file() or (
                    isinstance(exc, RoutingDisagreement) and exc.reason == "routing_manifest_moved_during_recovery"
                ):
                    raise
                try:
                    _update_manifest_routing(
                        manifest_path=manifest_path,
                        lane_id=lane_id,
                        values=manifest_prior,
                        restore=True,
                    )
                except Exception as rollback_exc:  # noqa: BLE001 -- report both; the intent stays for the next call
                    raise RuntimeError(f"{exc}; manifest rollback failed: {rollback_exc}") from exc
                raise
        return None
    else:
        _check_route_journaled(route=db_target, previous_database=previous_database, target=target)
        canonical, cleared = _legacy_route_canonical(db_target)
        if not cleared:
            from .orchestration.lane_manifest import manifest_write_lock  # noqa: PLC0415

            # Lock order: routing guard, then the manifest lock (released after
            # the publication, retaken for the settlement), then the SQLite
            # write lock for the settlement.
            manifest_prior = _update_manifest_routing(
                manifest_path=manifest_path,
                lane_id=lane_id,
                values=_mirror_route(db_target),
            )
            with manifest_write_lock(manifest_path):
                try:
                    _settle_routing_intent(
                        task_ref=task_ref,
                        lane_id=lane_id,
                        intent_path=intent_path,
                        route=db_target,
                        manifest_path=manifest_path,
                    )
                except RoutingDisagreement as exc:
                    # A supported publisher moved this lane's mirror after the
                    # publication above: keep its publication and the journal.
                    if exc.reason == "routing_manifest_moved_during_recovery":
                        raise
                    # A writer outside the guard moved the row after the read above:
                    # keep its route and the journal, and put the mirror back as it was.
                    try:
                        _update_manifest_routing(
                            manifest_path=manifest_path,
                            lane_id=lane_id,
                            values=manifest_prior,
                            restore=True,
                        )
                    except Exception as rollback_exc:  # noqa: BLE001 -- report both; the intent stays for the next call
                        raise RuntimeError(f"{exc}; manifest rollback failed: {rollback_exc}") from exc
                    raise
            return None
        else:
            _check_legacy_route_clear(
                route=db_target,
                previous_database=previous_database,
                canonical=canonical,
                cleared=cleared,
            )
            manifest_prior = _clear_legacy_route_fields(
                task_ref=task_ref,
                lane_id=lane_id,
                manifest_path=manifest_path,
                route=db_target,
                canonical=canonical,
            )
            # The clear committed and released the SQLite write lock, so a writer
            # outside the guard may already have moved the row. Retire the journal
            # only inside the serialized settlement on the canonical row; the
            # cleanup below then finds it gone, and no other writer can recreate
            # it while the caller holds the routing guard.
            from .orchestration.lane_manifest import manifest_write_lock  # noqa: PLC0415

            # Lock order (CON-13): routing guard -> manifest sidecar -> SQLite.
            # Settlement and rollback reenter this same-thread manifest lock;
            # retaining it across DB refusal prevents a supported publisher
            # from entering after the mirror check and being overwritten by
            # compensation (CON-05). No publisher-dependent wait occurs here
            # (CON-18); the resource order prevents circular wait (CON-21).
            # The helper's journal unlink remains the settlement point, and
            # its directory fsync completes before a success receipt is built.
            with manifest_write_lock(manifest_path):
                try:
                    _settle_routing_intent(
                        task_ref=task_ref,
                        lane_id=lane_id,
                        intent_path=intent_path,
                        route=canonical,
                        manifest_path=manifest_path,
                    )
                except RoutingDisagreement as exc:
                    # A moved mirror is that publisher's write: keep it as published.
                    if exc.reason == "routing_manifest_moved_during_recovery":
                        raise
                    # A moved row: keep its route and the journal, and put the
                    # mirror back as it was before the clear published.
                    try:
                        _update_manifest_routing(
                            manifest_path=manifest_path,
                            lane_id=lane_id,
                            values=manifest_prior,
                            restore=True,
                        )
                    except Exception as rollback_exc:  # noqa: BLE001 -- report both; the intent stays for the next call
                        raise RuntimeError(f"{exc}; manifest rollback failed: {rollback_exc}") from exc
                    raise
            receipt = {
                "outcome": "legacy_fields_cleared",
                "cleared": cleared,
                "route": _mirror_route(canonical),
                "manifest_before": manifest_prior,
            }
            _logger.warning(
                "routing recovery cleared obsolete OpenAI-only fields %s for %s/%s on backend %s",
                cleared,
                task_ref,
                lane_id,
                db_target[_ROUTING_MANIFEST_FIELDS["backend"]],
            )
    _remove_routing_intent(intent_path)
    return receipt


def _compensate_failed_routing_switch(
    *,
    task_ref: str,
    lane_id: str,
    manifest_path: Path,
    intent_path: Path,
    expected_manifests: tuple[dict[str, Any], ...],
) -> None:
    """Replay this fresh request only if its publication has not been replaced.

    Lock order: caller's routing guard -> manifest sidecar -> SQLite (CON-13),
    breaking circular wait (CON-21). The sidecar's named timeout and SQLite's
    busy_timeout bound acquisition; no publisher-dependent wait occurs under
    exclusion (CON-18). Retain the sidecar from revalidation through recovery's
    rollback and serialized settlement (CON-05), including journal retirement.
    save_manifest, atomic_update_manifest, create_or_update_manifest and the
    compiler using it take this fence; direct file writes are unsupported.
    DB-only writers, including handoff open_lane, are checked by recovery's
    SQLite settlement. Compare the full document so even a metadata-only
    publication survives byte for byte, with this request's intent retained.
    """
    from .orchestration.lane_manifest import manifest_write_lock  # noqa: PLC0415

    with manifest_write_lock(manifest_path):
        actual = json.loads(manifest_path.read_text(encoding="utf-8"))
        if actual not in expected_manifests:
            raise RoutingDisagreement(
                "routing_manifest_moved_during_recovery",
                f"lane {lane_id!r} manifest changed before routing switch compensation",
            )
        _recover_interrupted_routing_update(
            task_ref=task_ref,
            lane_id=lane_id,
            manifest_path=manifest_path,
            intent_path=intent_path,
        )


def _update_worktree_lane_routing_locked(
    *,
    task_ref: str,
    lane_id: str,
    caller: dict[str, object],
    intent_path: Path,
    upsert_route_values: dict[str, object] | None = None,
    upsert_values: dict[str, object] | None = None,
) -> dict[str, object]:
    manifest_path = _workspace_root() / "config" / "lane-orchestration" / f"{task_ref}.json"
    intent_written = False
    database_committed = False
    compensation_manifests: tuple[dict[str, Any], ...] = ()
    recovery: dict[str, object] | None = None
    recovered = False
    try:
        recovery = _recover_interrupted_routing_update(
            task_ref=task_ref,
            lane_id=lane_id,
            manifest_path=manifest_path,
            intent_path=intent_path,
        )
        recovered = True
        with _get_db_connection() as conn:
            row = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
                (task_ref, lane_id),
            ).fetchone()
        if row is None:
            return _json_response({"ok": False, "error": f"Lane '{lane_id}' not found for task '{task_ref}'."})
        if upsert_values is not None:
            rejection = _routing_upsert_rejection(values=upsert_values, row=_row_to_dict(row))
            if rejection is not None:
                return _with_routing_recovery(rejection, recovery)
        previous_database = _manifest_route_from_row(row)
        previous_database_route = tuple(previous_database.values())

        manifest_entry: dict[str, object] | None = None
        if manifest_path.is_file():
            from .orchestration.lane_manifest import load_manifest  # noqa: PLC0415

            manifest = load_manifest(task_ref, orchestrator_root=str(_workspace_root()))
            entries = manifest.get("lanes")
            candidate = entries.get(lane_id) if isinstance(entries, dict) else None
            if not isinstance(candidate, dict):
                raise RuntimeError(f"lane {lane_id!r} missing from routing manifest")
            manifest_entry = candidate
        if upsert_route_values is not None:
            caller = _upsert_routing_caller(
                route_values=upsert_route_values,
                previous=_row_to_dict(row),
                manifest_entry=manifest_entry or {},
            )
        resolved = resolve_routing_quad(
            caller=caller,
            row=_row_to_dict(row),
            manifest_entry=manifest_entry,
            tier_table=CODEX_MODEL_TIERS,
        )
        routing_values: dict[str, object] = {
            _ROUTING_MANIFEST_FIELDS["backend"]: resolved.backend,
            _ROUTING_MANIFEST_FIELDS["model"]: resolved.model,
            _ROUTING_MANIFEST_FIELDS["reasoning_effort"]: resolved.effort,
            _ROUTING_MANIFEST_FIELDS["speed"]: resolved.speed,
            _ROUTING_MANIFEST_FIELDS["tier"]: resolved.tier,
        }
        requested_speed = (upsert_route_values if upsert_route_values is not None else caller).get("speed")
        if _normalize_optional_text(requested_speed) is not None:
            # Speed is an OpenAI service tier: an explicit request the resolved
            # backend cannot carry is refused before either store changes.
            normalize_speed(resolved.backend, requested_speed)

        if manifest_entry is not None:
            from copy import deepcopy  # noqa: PLC0415

            from .orchestration.lane_manifest import atomic_update_manifest  # noqa: PLC0415

            def _publish_switch(document: dict[str, Any]) -> None:
                nonlocal intent_written, compensation_manifests
                entries = document.get("lanes")
                lane = entries.get(lane_id) if isinstance(entries, dict) else None
                if not isinstance(lane, dict):
                    raise RuntimeError(f"lane {lane_id!r} missing from routing manifest")
                # Capture under the publication lock, rather than rereading
                # after return when another publisher may already have won.
                compensation_manifests = (json.loads(manifest_path.read_text(encoding="utf-8")),)
                _write_routing_intent(
                    intent_path,
                    {
                        "database_before": previous_database,
                        "manifest_before": _routing_manifest_snapshot(lane),
                        "routing_after": routing_values,
                        "task_ref": task_ref,
                        "lane_id": lane_id,
                    },
                )
                intent_written = True
                for key, value in routing_values.items():
                    if value is None:
                        lane.pop(key, None)
                    else:
                        lane[key] = value
                # If publication itself raises, either the original document
                # or this candidate can still be ours; any other one is kept.
                compensation_manifests += (deepcopy(document),)

            published_manifest = atomic_update_manifest(manifest_path, _publish_switch)
            # The returned validated document identifies the exact successful
            # publication even if a supported publisher ran before return.
            compensation_manifests = (published_manifest,)

        with _get_db_connection(begin_immediate=True) as conn:
            current = conn.execute(
                "SELECT backend, model, reasoning_effort, speed, tier FROM worktree_lanes "
                "WHERE task_ref = ? AND lane_id = ?",
                (task_ref, lane_id),
            ).fetchone()
            if current is None or tuple(current) != previous_database_route:
                raise RuntimeError(f"lane {lane_id!r} routing changed during routing switch")
            cur = conn.execute(
                """
                UPDATE worktree_lanes
                SET backend = ?, model = ?, reasoning_effort = ?, speed = ?, tier = ?,
                    updated_at = datetime('now')
                WHERE task_ref = ? AND lane_id = ?
                """,
                (
                    resolved.backend,
                    resolved.model,
                    resolved.effort,
                    resolved.speed,
                    resolved.tier,
                    task_ref,
                    lane_id,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"lane {lane_id!r} changed during routing switch")
            row = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
                (task_ref, lane_id),
            ).fetchone()
        database_committed = True
        if intent_written:
            # Publication sets every routing key from routing_values and
            # removes keys whose value is None. Bind that exact snapshot
            # from our request, even when a publisher wrapper returns None;
            # rereading here could adopt another publisher's moved route.
            # Revalidate it and the committed row before retiring the
            # intent. The helper takes the manifest fence before SQLite
            # (CON-13), with bounded acquisition and no
            # publisher-dependent wait (CON-18). A refusal keeps both stores
            # and the journal; this committed switch must not compensate.
            _settle_routing_intent(
                task_ref=task_ref,
                lane_id=lane_id,
                intent_path=intent_path,
                route=_manifest_route_from_row(row),
                manifest_path=manifest_path,
                expected_manifest_snapshot=_routing_manifest_snapshot(
                    {key: value for key, value in routing_values.items() if value is not None}
                ),
                tolerate_cleanup_errors=True,
            )
    except RoutingDisagreement as exc:
        return _with_routing_recovery(
            _json_response(
                {
                    "ok": False,
                    "error": str(exc),
                    "outcome": "routing_refused",
                    "failure_class": exc.failure_class,
                    "reason": exc.reason,
                }
            ),
            recovery,
        )
    except Exception as exc:  # noqa: BLE001 -- restore the manifest to the SQLite source of truth
        compensation_error: str | None = None
        # A failed recovery already rolled back its own mirror write and kept
        # the intent; replaying it here would only repeat the same refusal.
        if recovered and intent_written and intent_path.is_file() and not database_committed:
            try:
                _compensate_failed_routing_switch(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    manifest_path=manifest_path,
                    intent_path=intent_path,
                    expected_manifests=compensation_manifests,
                )
            except Exception as rollback_exc:  # noqa: BLE001
                compensation_error = f"; manifest compensation failed: {rollback_exc}"
        return _with_routing_recovery(
            _json_response(
                {
                    "ok": False,
                    "error": f"routing switch failed: {exc}{compensation_error or ''}",
                    "outcome": "routing_refused",
                    "failure_class": "routing",
                    "reason": "routing_switch_failed",
                }
            ),
            recovery,
        )
    return _with_routing_recovery(_json_response({"ok": True, "lane": _row_to_dict(row)}), recovery)


def update_worktree_lane_routing(
    *,
    lane_id: str,
    task_ref: str | None,
    backend: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    speed: str | None = None,
    tier: str | None = None,
) -> dict[str, object]:
    """Switch routing columns and the manifest mirror without a cross-lock wait.

    SQLite is authoritative. A durable intent captures the prior manifest
    before either structure changes. After a crash between the manifest update
    and the DB commit, the next routing update restores the manifest to the DB
    route before processing its new request. Readers fail closed on the
    temporary row-manifest disagreement. A DB route holding obsolete
    OpenAI-only speed/tier on another backend is canonicalized by that
    recovery and reported as ``routing_recovery``; an explicit speed the
    resolved backend cannot carry is refused with ``invalid_speed``.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    supplied = {
        "backend": backend,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "speed": speed,
        "tier": tier,
    }
    caller = {key: value for key, value in supplied.items() if value is not None}
    if not caller:
        return _json_response({"ok": False, "error": "at least one routing field is required."})
    try:
        with _get_db_connection() as conn:
            ref = _resolve_task_ref(conn, task_ref)
        lock_path, intent_path = _routing_state_paths(ref, normalized_lane_id)
        lock_fd = _try_acquire_routing_guard(lock_path)
        if lock_fd is None:
            return _json_response(
                {
                    "ok": False,
                    "error": f"A routing switch is already in progress for lane '{normalized_lane_id}'.",
                    "outcome": "routing_refused",
                    "failure_class": "routing",
                    "reason": "routing_switch_in_progress",
                }
            )
        try:
            return _update_worktree_lane_routing_locked(
                task_ref=ref,
                lane_id=normalized_lane_id,
                caller=caller,
                intent_path=intent_path,
            )
        finally:
            _release_routing_guard(lock_path, lock_fd)
    except Exception as exc:  # noqa: BLE001 -- routing remains a typed tool result
        return _json_response(
            {
                "ok": False,
                "error": f"routing switch failed: {exc}",
                "outcome": "routing_refused",
                "failure_class": "routing",
                "reason": "routing_switch_failed",
            }
        )


_FULL_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _omit_blank_sha(value: object) -> object:
    """Treat None/blank as omit; leave any other present value untouched."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _git_rev_parse_full_sha(repo: Path, ref: str | None) -> str | None:
    """Resolve *ref* to a full 40-hex SHA, or None if it does not resolve.

    Never truncates, pads, or writes a short SHA. A non-zero git exit, a
    timeout, or a result that is not 40 hex is treated as unresolved.
    Branch names are pinned as ``refs/heads/<name>^{commit}`` behind
    ``--end-of-options`` so a dash-prefixed or non-commit object cannot be
    mistaken for a commit SHA.
    """
    normalized_ref = _normalize_optional_text(ref)
    if normalized_ref is None:
        return None
    if _FULL_COMMIT_SHA_RE.fullmatch(normalized_ref.lower()):
        spec = f"{normalized_ref}^{{commit}}"
    else:
        spec = f"refs/heads/{normalized_ref}^{{commit}}"
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "rev-parse",
                "--verify",
                "--end-of-options",
                spec,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=_BRANCH_PROBE_TIMEOUT_S,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    sha = completed.stdout.strip().lower()
    if _FULL_COMMIT_SHA_RE.fullmatch(sha):
        return sha
    return None


def _git_nth_parent_sha(repo: Path, sha: str, n: int) -> str | None:
    """Resolve *sha*^n to a full commit SHA, or None if that parent is absent."""
    if n < 1 or not _FULL_COMMIT_SHA_RE.fullmatch(sha.lower()):
        return None
    spec = f"{sha}^{n}^{{commit}}"
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "rev-parse",
                "--verify",
                "--end-of-options",
                spec,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=_BRANCH_PROBE_TIMEOUT_S,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    parent = completed.stdout.strip().lower()
    if _FULL_COMMIT_SHA_RE.fullmatch(parent):
        return parent
    return None


def _handoff_target_branch(task_ref: str | None) -> tuple[str | None, str | None]:
    """Return ``(target_branch, error_reason)``.

    ``error_reason`` is ``target_lookup_failed`` when the lookup raises;
    otherwise ``None``. A missing/blank stored value is ``(None, None)``.
    """
    try:
        with _get_db_connection() as conn:
            resolved = _resolve_task_ref(conn, task_ref)
            row = conn.execute(
                "SELECT target_branch FROM handoff_state WHERE task_ref = ?",
                (resolved,),
            ).fetchone()
    except Exception as exc:  # noqa: BLE001 — derivation degrades to underived
        _logger.warning(
            "target branch lookup failed for task_ref=%s: %s: %s",
            task_ref,
            type(exc).__name__,
            exc,
        )
        return None, "target_lookup_failed"
    if row is None:
        return None, None
    value = row["target_branch"] if isinstance(row, sqlite3.Row) else row[0]
    return _normalize_optional_text(value), None


class _LandingLedgerReadFailed(Exception):
    """The landing ledger could not be read completely."""


def _recorded_landing_sha(*, lane_id: str, task_ref: str | None) -> str | None:
    """Return the newest recorded landing SHA for the lane, if it is 40 hex.

    The value is a projection that needs a verified receipt before use. A
    successful empty read returns None; a failed or malformed read raises
    ``_LandingLedgerReadFailed`` so derivation cannot treat unknown as absence.
    ``RuntimeNotConfiguredError`` remains visible to the close caller.
    """
    normalized_task_ref = _normalize_optional_text(task_ref)
    if normalized_task_ref is None:
        return None
    from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415

    try:
        from workbay_handoff_mcp.lanes_api import latest_lane_landing  # noqa: PLC0415

        envelope = latest_lane_landing(lane_id=lane_id, task_ref=normalized_task_ref)
    except RuntimeNotConfiguredError:
        _logger.error(
            "latest_lane_landing lookup requires configured handoff runtime for lane_id=%s task_ref=%s",
            lane_id,
            normalized_task_ref,
        )
        raise
    except Exception as exc:  # noqa: BLE001 — failed reads are unknown, never absence
        _logger.warning(
            "latest_lane_landing lookup failed for lane_id=%s task_ref=%s: %s: %s",
            lane_id,
            normalized_task_ref,
            type(exc).__name__,
            exc,
        )
        raise _LandingLedgerReadFailed("landing_record_read_failed") from exc
    if isinstance(envelope, str):
        try:
            envelope = json.loads(envelope)
        except (TypeError, json.JSONDecodeError) as exc:
            raise _LandingLedgerReadFailed("landing_record_read_failed") from exc
    if not isinstance(envelope, dict) or envelope.get("ok") is not True:
        raise _LandingLedgerReadFailed("landing_record_read_failed")
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else envelope
    if not isinstance(data, dict):
        raise _LandingLedgerReadFailed("landing_record_read_failed")
    landing = data.get("landing")
    if landing is None:
        return None
    if not isinstance(landing, dict):
        raise _LandingLedgerReadFailed("landing_record_read_failed")
    sha = landing.get("commit_sha")
    if not isinstance(sha, str):
        raise _LandingLedgerReadFailed("landing_record_read_failed")
    normalized = sha.strip().lower()
    if _FULL_COMMIT_SHA_RE.fullmatch(normalized):
        return normalized
    raise _LandingLedgerReadFailed("landing_record_read_failed")


_MERGED_CLOSE_FIRST_PARENT_WALK_LIMIT = 64


def _underived_close_shas(reason: str) -> tuple[None, None, list[str]]:
    """Return the fail-closed merged-close identity triple for *reason*."""
    return None, None, [f"landing_sha_underived:{reason}"]


def _lane_branch_for_merged_close(
    *,
    lane_id: str,
    task_ref: str | None,
) -> tuple[str | None, str | None]:
    """Return ``(branch, error_reason)`` for merged-close derivation.

    ``error_reason`` is ``lane_lookup_failed`` when ``get_lane`` raises or
    returns a non-ok envelope. A successful lookup with a blank branch is
    ``(None, None)`` — that is a missing ref, not a lookup failure.
    """
    try:
        from workbay_handoff_mcp.lanes_api import get_lane  # noqa: PLC0415

        lane_env = get_lane(lane_id=lane_id, task_ref=task_ref)
    except Exception as exc:  # noqa: BLE001 — derivation degrades to underived
        _logger.warning(
            "lane lookup failed for lane_id=%s task_ref=%s: %s: %s",
            lane_id,
            task_ref,
            type(exc).__name__,
            exc,
        )
        return None, "lane_lookup_failed"
    if not isinstance(lane_env, dict) or lane_env.get("ok") is not True:
        _logger.warning(
            "lane lookup returned non-ok envelope for lane_id=%s task_ref=%s ok=%s",
            lane_id,
            task_ref,
            None if not isinstance(lane_env, dict) else lane_env.get("ok"),
        )
        return None, "lane_lookup_failed"
    lane_data = lane_env.get("data")
    lane = lane_data.get("lane") if isinstance(lane_data, dict) else None
    if not isinstance(lane, dict):
        return None, None
    return _normalize_optional_text(lane.get("branch")), None


def _merge_commit_for_second_parent(
    repo: Path,
    start_sha: str,
    tip: str,
) -> str | None:
    """Return the newest first-parent ancestor of *start_sha* whose second parent is *tip*.

    Bounded so a long target history cannot become an unbounded git walk.
    Equality of *start_sha* with *tip* is not a landing: an empty cut and a
    fast-forward share that shape and need a recorded SHA or a merge commit.
    """
    sha: str | None = start_sha
    seen: set[str] = set()
    for _ in range(_MERGED_CLOSE_FIRST_PARENT_WALK_LIMIT):
        if sha is None or sha in seen:
            return None
        seen.add(sha)
        second = _git_nth_parent_sha(repo, sha, 2)
        if second is not None and second == tip:
            return sha
        sha = _git_nth_parent_sha(repo, sha, 1)
    return None


def _containment_underived_reason(contained: bool | str | None) -> str:
    """Map a landing-predicate result to a ``landing_sha_underived`` reason."""
    if contained == "empty":
        return "empty_cut"
    if contained == "timeout":
        return "containment_probe_timeout"
    if contained is None:
        return "absence_of_evidence"
    if contained is True:
        return "no_recorded_landing"
    return "not_contained"


def _lane_tip_contained_in(
    repo: Path,
    candidate_sha: str,
    lane_ref: str,
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
    base_sha: str | None = None,
) -> bool | str | None:
    """Return the landing-predicate result for *lane_ref* in *candidate_sha*.

    Calls ``_lane_branch_contained_in`` with recorded-base evidence
    (``base_sha`` / ``task_ref`` / ``lane_id``) and never with
    ``data_loss_safety=True``. An empty cut is ``"empty"``, not a landing.
    Probe exceptions are absence of evidence (``None``), not ``False``.
    """
    try:
        from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
            _lane_branch_contained_in,
        )

        contained = _lane_branch_contained_in(
            repo,
            candidate_sha,
            lane_ref,
            base_sha=base_sha,
            task_ref=task_ref,
            lane_id=lane_id,
        )
    except Exception as exc:  # noqa: BLE001 — fail closed, do not fabricate
        _logger.warning(
            "containment probe failed for tip=%s candidate=%s: %s: %s",
            lane_ref,
            candidate_sha,
            type(exc).__name__,
            exc,
        )
        return None
    return contained


def _derive_merged_close_shas(
    *,
    lane_id: str,
    task_ref: str | None,
) -> tuple[str | None, str | None, list[str]]:
    """Derive landing and tip SHAs for a merged close.

    A recorded ``latest_lane_landing`` SHA is only a projection. Use it when
    a verified receipt for this task/lane names that same revision. If the
    lane ref is gone, recover the tip from the receipt commit's second parent.
    Without a recorded projection, only a ``--no-ff`` merge whose second
    parent is the lane tip is an integration event (walk first-parent from
    target HEAD so a later descendant is not stored). An unresolvable tip is
    ``landing_sha_underived:tip_unresolved``. An empty cut is
    ``landing_sha_underived:empty_cut``. Other containment outcomes keep
    their daemon vocabulary (timeout / absence / not_contained).
    """
    primary_repo = _merged_close_workspace_root()
    if primary_repo is None:
        return _underived_close_shas("workspace_root_unavailable")

    lane_branch, lane_err = _lane_branch_for_merged_close(lane_id=lane_id, task_ref=task_ref)
    if lane_err is not None:
        return _underived_close_shas(lane_err)
    tip = _git_rev_parse_full_sha(primary_repo, lane_branch)

    try:
        recorded = _recorded_landing_sha(lane_id=lane_id, task_ref=task_ref)
    except _LandingLedgerReadFailed:
        return _underived_close_shas("landing_record_read_failed")
    if recorded is not None:
        target_branch, target_error = _handoff_target_branch(task_ref)
        if target_error is not None:
            return _underived_close_shas(target_error)
        if target_branch is None:
            return _underived_close_shas("no_target_branch")
        try:
            from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
                _lookup_integration_receipt_landed_revision,
            )
        except Exception as exc:  # noqa: BLE001 — receipt reader unavailable is unknown
            _logger.warning(
                "landing receipt reader unavailable for lane_id=%s task_ref=%s: %s: %s",
                lane_id,
                task_ref,
                type(exc).__name__,
                exc,
            )
            return _underived_close_shas("landing_receipt_read_failed")
        try:
            verified_landing = _lookup_integration_receipt_landed_revision(
                primary_repo,
                task_ref=task_ref or "",
                lane_id=lane_id,
                integration_tip=target_branch,
            )
        except Exception as exc:  # noqa: BLE001 — receipt read failures cannot authorize a close
            reason = getattr(exc, "reason", None)
            _logger.warning(
                "landing receipt lookup failed for lane_id=%s task_ref=%s: %s: %s",
                lane_id,
                task_ref,
                type(exc).__name__,
                exc,
            )
            return _underived_close_shas(reason if isinstance(reason, str) else "landing_receipt_read_failed")
        if verified_landing != recorded:
            return _underived_close_shas("recorded_landing_unverified")
        return _recorded_close_shas(primary_repo, recorded, tip)

    return _derive_unrecorded_close_shas(
        primary_repo=primary_repo,
        lane_id=lane_id,
        task_ref=task_ref,
        lane_branch=lane_branch,
        tip=tip,
    )


def _merged_close_workspace_root() -> Path | None:
    """Resolve the workspace for merged-close git probes, fail-closed."""
    try:
        return _workspace_root()
    except Exception as exc:  # noqa: BLE001 — derivation degrades to underived
        _logger.warning(
            "workspace root unavailable during merged-close derivation: %s: %s",
            type(exc).__name__,
            exc,
        )
        return None


def _recorded_close_shas(
    primary_repo: Path,
    recorded: str,
    tip: str | None,
) -> tuple[str, str | None, list[str]]:
    """Complete a recorded landing with the merge's second-parent tip if needed."""
    if tip is None:
        tip = _git_nth_parent_sha(primary_repo, recorded, 2)
    return recorded, tip, []


def _derive_unrecorded_close_shas(
    *,
    primary_repo: Path,
    lane_id: str,
    task_ref: str | None,
    lane_branch: str | None,
    tip: str | None,
) -> tuple[str | None, str | None, list[str]]:
    """Derive an unrecorded landing only from target-branch evidence."""
    target_branch, lookup_reason = _handoff_target_branch(task_ref)
    if lookup_reason is not None:
        return _underived_close_shas(lookup_reason)
    if target_branch is None:
        return _underived_close_shas("no_target_branch")
    target_head = _git_rev_parse_full_sha(primary_repo, target_branch)
    if target_head is None:
        return _underived_close_shas("target_ref_unresolved")
    if tip is None:
        return _underived_close_shas("tip_unresolved")

    merge_landing = _merge_commit_for_second_parent(primary_repo, target_head, tip)
    if merge_landing is not None:
        return merge_landing, tip, []

    contained = _lane_tip_contained_in(
        primary_repo,
        target_head,
        lane_branch or tip,
        task_ref=task_ref,
        lane_id=lane_id,
        base_sha=tip,
    )
    return _underived_close_shas(_containment_underived_reason(contained))


def _close_tip_source(resolved_tip: object, *, derived_tip: bool) -> str | None:
    """Return provenance for a close tip, if one will be written."""
    if isinstance(resolved_tip, str) and resolved_tip.strip():
        return "branch" if derived_tip else "manifest"
    return None


def _receipt_tip_for_close(lane_id: str, task_ref: str | None, landing_commit_sha: str) -> str | None:
    primary_repo = _merged_close_workspace_root()
    if primary_repo is None or task_ref is None:
        return None
    try:
        from workbay_orchestrator_mcp.orchestration.landing_log import (  # noqa: PLC0415
            receipt_tip_for_landing,
        )

        return receipt_tip_for_landing(primary_repo, landing_commit_sha, task_ref=task_ref, lane_id=lane_id)
    except Exception:  # noqa: BLE001 — an unavailable receipt parser cannot invent a tip
        return None


def _existing_close_tip_provenance(lane_id: str, task_ref: str | None) -> tuple[str | None, str | None]:
    """Read the existing lane tip and provenance in the same lane scope."""
    try:
        from workbay_handoff_mcp.lanes_api import get_lane  # noqa: PLC0415

        envelope = get_lane(lane_id=lane_id, task_ref=task_ref)
    except Exception:  # noqa: BLE001 — failed reads cannot establish provenance
        return None, None
    if not isinstance(envelope, dict) or envelope.get("ok") is not True:
        return None, None
    data = envelope.get("data")
    lane = data.get("lane") if isinstance(data, dict) else None
    if not isinstance(lane, dict):
        return None, None
    tip = lane.get("branch_tip_sha")
    source = lane.get("branch_tip_source")
    return (
        tip if isinstance(tip, str) and tip.strip() else None,
        source if isinstance(source, str) and source.strip() else None,
    )


def _resolve_close_identity(
    *,
    lane_id: str,
    status: str,
    task_ref: str | None,
    landing_commit_sha: str | None,
    branch_tip_sha: str | None,
) -> tuple[object, object, str | None, list[str]]:
    """Normalize caller identity and derive missing merged-close values."""
    resolved_landing = _omit_blank_sha(landing_commit_sha)
    resolved_tip = _omit_blank_sha(branch_tip_sha)
    caller_supplied_tip = resolved_tip is not None
    extra_warnings: list[str] = []
    derived_tip = False
    if status == "merged":
        if resolved_landing is None:
            resolved_landing, maybe_tip, extra_warnings = _derive_merged_close_shas(
                lane_id=lane_id,
                task_ref=task_ref,
            )
        else:
            maybe_tip = _receipt_tip_for_close(lane_id, task_ref, resolved_landing)
        if resolved_tip is None:
            resolved_tip = maybe_tip
            derived_tip = maybe_tip is not None
        if resolved_tip is None and resolved_landing is not None and not caller_supplied_tip:
            extra_warnings.append("branch_tip_sha_unverified:receipt_unavailable")
    if isinstance(resolved_tip, str) and resolved_tip.strip():
        existing_tip, existing_source = _existing_close_tip_provenance(lane_id, task_ref)
        if (
            isinstance(existing_tip, str)
            and existing_tip.lower() == resolved_tip.lower()
            and existing_source == "branch"
        ):
            # close_lane treats every newly supplied tip as a manifest by
            # default. Omit an identical tip already proven from the branch so
            # both its provenance and observation timestamp remain intact.
            resolved_tip = None
    return resolved_landing, resolved_tip, _close_tip_source(resolved_tip, derived_tip=derived_tip), extra_warnings


def _merge_close_warnings(result: dict[str, object], extra_warnings: list[str]) -> dict[str, object]:
    """Append derivation warnings to the adapted handoff response."""
    if not extra_warnings:
        return result
    existing = result.get("warnings")
    merged = list(existing) if isinstance(existing, list) else []
    merged.extend(extra_warnings)
    result["warnings"] = merged
    return result


def _close_lane_kwargs(
    *,
    lane_id: str,
    status: str,
    notes: str | None,
    task_ref: str | None,
    landing_commit_sha: object,
    branch_tip_sha: object,
    branch_tip_source: str | None,
) -> dict[str, object]:
    """Build the handoff close call while omitting absent tip provenance."""
    kwargs: dict[str, object] = {
        "lane_id": lane_id,
        "status": status,
        "notes": notes,
        "task_ref": task_ref,
        "landing_commit_sha": landing_commit_sha,
        "branch_tip_sha": branch_tip_sha,
    }
    if branch_tip_source is not None:
        kwargs["branch_tip_source"] = branch_tip_source
    return kwargs


def close_worktree_lane(
    lane_id: str,
    status: str = "closed",
    notes: str | None = None,
    task_ref: str | None = None,
    landing_commit_sha: str | None = None,
    branch_tip_sha: str | None = None,
) -> dict:
    """Close a worktree lane in the handoff database.
    Merged closes derive omitted identity SHAs; other statuses never derive.
    Failures remain NULL with a machine-readable warning. Derived tips use
    ``branch`` provenance; caller-supplied tips use ``manifest``."""
    valid_close_statuses = CLOSEABLE_LANE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if status not in valid_close_statuses:
        return _json_response(
            {"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_close_statuses))}"}
        )
    resolved_landing, resolved_tip, tip_source, extra_warnings = _resolve_close_identity(
        lane_id=normalized_lane_id,
        status=status,
        task_ref=task_ref,
        landing_commit_sha=landing_commit_sha,
        branch_tip_sha=branch_tip_sha,
    )
    from workbay_handoff_mcp.lanes_api import close_lane  # noqa: PLC0415

    close_kwargs = _close_lane_kwargs(
        lane_id=normalized_lane_id,
        status=status,
        notes=notes,
        task_ref=task_ref,
        landing_commit_sha=resolved_landing,
        branch_tip_sha=resolved_tip,
        branch_tip_source=tip_source,
    )
    result = _adapt_lane_envelope(close_lane(**close_kwargs))
    result = _merge_close_warnings(result, extra_warnings)
    if status == "merged" and result.get("ok") is True:
        result["vm_marker"] = _write_close_vm_merged_marker(result, lane_id=normalized_lane_id)
    return result


def _write_close_vm_merged_marker(result: dict[str, object], *, lane_id: str) -> dict[str, object]:
    """Write the merged marker for a lane after its close has committed."""
    lane = result.get("lane")
    branch = lane.get("branch") if isinstance(lane, dict) else None
    if not isinstance(branch, str) or not branch.strip():
        error = "merged close row does not contain a branch"
        _logger.warning("VM merged marker delivery failed for lane %s: %s", lane_id, error)
        return {"ok": False, "error": error}

    raw_tip = lane.get("branch_tip_sha") if isinstance(lane, dict) else None
    tip = raw_tip.strip() if isinstance(raw_tip, str) else ""
    try:
        repo = _merged_close_workspace_root()
        if repo is None:
            raise RuntimeError("workspace root unavailable")
        from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

        from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
            _write_vm_merged_marker,
        )

        return _write_vm_merged_marker(branch, tip, repo=repo, state_dir=get_runtime_config().state_dir)
    except Exception as exc:  # noqa: BLE001 — the lane close has already committed
        error = f"{type(exc).__name__}: {exc}"
        _logger.warning("VM merged marker delivery failed for lane %s branch %s: %s", lane_id, branch, error)
        return {"ok": False, "error": error}


def _repair_registration_branch_tip_cas(
    *,
    task_ref: str,
    lane_id: str,
    landing_commit_sha: str,
    expected_branch_tip_sha: str,
    expected_branch_tip_source: str,
    branch_tip_sha: str,
) -> bool:
    """Migrate only the observed stale registration projection under SQL CAS."""
    try:
        with _get_db_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? ORDER BY id DESC LIMIT 1",
                (task_ref, lane_id),
            ).fetchone()
            if row is None:
                conn.rollback()
                return False
            lane_pk = row["id"] if isinstance(row, sqlite3.Row) else row[0]
            updated = conn.execute(
                "UPDATE worktree_lanes SET branch_tip_sha = ?, branch_tip_source = ?, updated_at = datetime('now') "
                "WHERE id = ? AND task_ref = ? AND lane_id = ? AND status = 'merged' "
                "AND landing_commit_sha = ? AND branch_tip_sha IS ? AND branch_tip_source IS ?",
                (
                    branch_tip_sha,
                    "backfill",
                    lane_pk,
                    task_ref,
                    lane_id,
                    landing_commit_sha,
                    expected_branch_tip_sha,
                    expected_branch_tip_source,
                ),
            )
            conn.commit()
            return int(updated.rowcount or 0) == 1
    except Exception:  # noqa: BLE001 — a failed CAS leaves the observed row untouched
        return False


def list_worktree_lanes(task_ref: str | None = None, status: str = "all", limit: int = 100, offset: int = 0) -> dict:
    limit = max(1, limit)
    offset = max(0, offset)
    valid_statuses = {"all", *LANE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    from workbay_handoff_mcp.lanes_api import list_lanes  # noqa: PLC0415

    return _adapt_lane_envelope(list_lanes(task_ref=task_ref, status=status, limit=limit, offset=offset))


def _retire_envelope(
    *,
    ok: bool,
    outcome: str,
    lane_id: str = "",
    task_ref: str = "",
    worktree_path: str | None = None,
    branch: str = "",
    bundle_path: object = None,
    bundle_tip_sha: object = None,
    worktree_reclaim: str = "",
    worktree_reclaim_detail: str = "",
    branch_deleted: bool = False,
    dry_run: bool = True,
    error_kind: str | None = None,
    error: str | None = None,
) -> dict:
    payload: dict[str, object] = {
        "ok": ok,
        "outcome": outcome,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "worktree_path": worktree_path,
        "branch": branch,
        "bundle_path": bundle_path,
        "bundle_tip_sha": bundle_tip_sha,
        "worktree_reclaim": worktree_reclaim,
        "worktree_reclaim_detail": worktree_reclaim_detail,
        "branch_deleted": branch_deleted,
        "dry_run": dry_run,
    }
    if error_kind is not None:
        payload["error_kind"] = error_kind
    if error is not None:
        payload["error"] = error
    elif not ok:
        payload["error_kind"] = outcome
        payload["error"] = outcome
    return _json_response(payload)


def _retire_worktree_lane(
    *,
    lane_id: str | None,
    task_ref: str | None,
    apply: bool = False,
    delete_merged_branch: bool = False,
    landing_integration_ref: str | None = None,
    landing_repo_root: str | Path | None = None,
    landing_manifest_root: str | Path | None = None,
    landing_commit: str | None = None,
    landing_expected_tip: str | None = None,
    landing_run_id: str | None = None,
    landing_is_contained: bool = False,
    expected_tip: str | None = None,
    expected_identity: dict[str, object] | None = None,
    # True only when the caller already holds _landing_mutation_lock for this repo in
    # this process (lane_land._retire). Every other caller keeps False and takes it.
    landing_lock_held: bool = False,
) -> dict:
    """Bundle, then reclaim one terminal lane worktree through the reaper helper."""
    from workbay_handoff_mcp.lanes_api import get_lane  # noqa: PLC0415

    from .lane_reaping import (  # noqa: PLC0415
        _acquire_lane_worker_lock,
        _bundle_before_reap,
        _probe_command_stdout,
        _reclaim_lane_worktree,
        _ref_lock_busy_info,
        _release_lane_worker_lock,
        _run_reclaim_command,
        recover_stale_ref_lock,
    )
    from .orchestration.branch_reclaim_delete import fence_release_of  # noqa: PLC0415
    from .orchestration.lane_postmerge import TERMINAL_STATUSES  # noqa: PLC0415

    # Preserve the existing domain seams, including caller/test overrides.
    domain = sys.modules[__name__]
    _close_authority_row_refusal = getattr(domain, "_close_authority_row_refusal")
    _parse_close_authority = getattr(domain, "_parse_close_authority")
    _probe_worktree_gone = getattr(domain, "_probe_worktree_gone")
    _reopen_moved_closed_row = getattr(domain, "_reopen_moved_closed_row")

    apply_flag = bool(apply)
    delete_branch = bool(delete_merged_branch)
    dry_run = not apply_flag
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_task_ref = _normalize_optional_text(task_ref)
    # Every fenced step that ran but left its fence unreleased, carried on every later envelope,
    # success or failure, beside the step's own result and never folded into it [AGT-10].
    fence_releases: list[dict[str, str]] = []
    reclaim_effects: dict[str, object] = {}

    def note_fence_release(step: str, release: str) -> None:
        record = {"step": step, "release": release}
        if release and release != "released" and record not in fence_releases:
            fence_releases.append(record)

    def note_worktree_reclaim(outcome: str, detail: str, bundle_path: object, bundle_tip_sha: object) -> None:
        # Record the applied step before any later authority read or branch operation can refuse.
        note_fence_release("worktree_reclaim", fence_release_of(detail))
        reclaim_effects.update(
            worktree_reclaim=outcome,
            worktree_reclaim_detail=detail,
            bundle_path=bundle_path,
            bundle_tip_sha=bundle_tip_sha,
        )

    def envelope(**kwargs: object) -> dict:
        for key, value in reclaim_effects.items():
            kwargs.setdefault(key, value)
        note_fence_release("worktree_reclaim", fence_release_of(kwargs.get("worktree_reclaim_detail")))
        kwargs.setdefault("lane_id", normalized_lane_id or str(lane_id or ""))
        kwargs.setdefault("task_ref", normalized_task_ref or "")
        kwargs.setdefault("dry_run", dry_run)
        pin_ref = kwargs.pop("pin_ref", None)
        diagnostics = {
            key: kwargs.pop(key)
            for key in ("detail", "lock_path", "age_s", "size", "stale_after_s", "lock_recovery")
            if key in kwargs
        }
        payload = _retire_envelope(**kwargs)
        if isinstance(pin_ref, str):
            payload["pin_ref"] = pin_ref
        payload.update(diagnostics)
        if fence_releases:
            payload["fence_release"] = [dict(record) for record in fence_releases]
            payload["cleanup_pending"] = "reclaim_fence_release"
        return payload

    if normalized_lane_id is None:
        return envelope(
            ok=False,
            outcome="blank_lane_id",
            error_kind="blank_lane_id",
            error="lane_id is required.",
        )
    if normalized_task_ref is None:
        return envelope(
            ok=False,
            outcome="blank_task_ref",
            error_kind="blank_task_ref",
            error=(
                "blank_task_ref: task_ref was blank or omitted; an empty scope would "
                "silently widen retire to repo-wide. Pass a real task_ref."
            ),
        )

    try:
        observed = get_lane(lane_id=normalized_lane_id, task_ref=normalized_task_ref)
    except Exception as exc:  # noqa: BLE001 — unknown reads must not authorize removal
        return envelope(
            ok=False,
            outcome="lane_row_read_failed",
            error_kind="lane_row_read_failed",
            error=str(exc),
        )
    data = observed.get("data") if isinstance(observed.get("data"), dict) else observed
    if not isinstance(data, dict):
        data = {}
    lane = data.get("lane") if isinstance(data.get("lane"), dict) else None
    if observed.get("ok") is not True or not isinstance(lane, dict):
        error = data.get("error") if isinstance(data.get("error"), str) else "lane_not_found"
        return envelope(
            ok=False,
            outcome="lane_not_found",
            error_kind="lane_not_found",
            error=str(error),
        )

    status = str(lane.get("status") or "")
    # The shared schema-1 parser is the only validator: a malformed authority, including
    # notes that are not JSON but name close_authority, never reads as a legacy row.
    close_authority, malformed = _parse_close_authority(lane.get("notes"))
    if malformed:
        return envelope(ok=False, outcome="authority_mismatch", error_kind="authority_mismatch")
    if close_authority is not None:
        if (
            (expected_tip is not None and expected_tip != close_authority["authorized_tip"])
            or (expected_identity is not None and expected_identity != close_authority["closed_identity"])
            or (landing_expected_tip is not None and landing_expected_tip != close_authority["authorized_tip"])
        ):
            return envelope(ok=False, outcome="authority_mismatch", error_kind="authority_mismatch")
        expected_tip = close_authority["authorized_tip"]
        expected_identity = close_authority["closed_identity"]
    branch = _normalize_optional_text(lane.get("branch")) or ""
    recorded_path = _normalize_optional_text(lane.get("worktree_path"))
    trusted_landing = None
    if all(
        isinstance(value, str) and value.strip()
        for value in (landing_integration_ref, landing_commit, landing_expected_tip, landing_run_id)
    ):
        trusted_landing = {
            "task_ref": normalized_task_ref,
            "lane_id": normalized_lane_id,
            "integration_ref": landing_integration_ref,
            "landing_commit": landing_commit,
            "expected_tip": landing_expected_tip,
            "run_id": landing_run_id,
            "contained": bool(landing_is_contained),
            "manifest_root": landing_manifest_root,
        }
    identity: dict[str, object] = {
        "lane_id": normalized_lane_id,
        "task_ref": normalized_task_ref,
        "worktree_path": recorded_path,
        "branch": branch,
    }
    if status not in TERMINAL_STATUSES and close_authority is None:
        return envelope(
            ok=False,
            outcome="not_terminal",
            error_kind="not_terminal",
            error=f"status {status or 'unset'} is not terminal",
            **identity,
        )

    try:
        repo_root = _workspace_root()
    except Exception as exc:  # noqa: BLE001 — fail closed
        return envelope(
            ok=False,
            outcome="repo_root_unresolved",
            error_kind="repo_root_unresolved",
            error=str(exc),
            **identity,
        )

    integration_ref = landing_integration_ref or "main"
    if landing_integration_ref is None:
        from .orchestration.lane_land import _registered_integration_ref  # noqa: PLC0415

        integration_ref, registration_error = _registered_integration_ref(
            root=repo_root,
            task_ref=normalized_task_ref,
            requested_ref=None,
        )
        if registration_error is not None or integration_ref is None:
            return envelope(
                ok=False,
                outcome="integration_ref_unresolved",
                error_kind="integration_ref_unresolved",
                error=registration_error or "task_integration_ref_unresolved",
                **identity,
            )

    if landing_repo_root is not None:
        if trusted_landing is None or not isinstance(landing_integration_ref, str):
            return envelope(
                ok=False,
                outcome="landing_repo_root_untrusted",
                error_kind="landing_repo_root_untrusted",
                error="landing repo root requires a complete landing identity",
                **identity,
            )
        try:
            actual_root = Path(landing_repo_root).expanduser().resolve(strict=True)
            runtime_root = Path(repo_root).expanduser().resolve(strict=True)
            if not actual_root.is_dir() or not runtime_root.is_dir():
                raise ValueError("repo_root_not_directory")

            def _git_common_dir(path: Path) -> Path:
                common = _run_reclaim_command(
                    ["git", "-C", str(path), "rev-parse", "--path-format=absolute", "--git-common-dir"]
                )
                if common is None or common.returncode != 0 or not (common.stdout or "").strip():
                    raise ValueError("git_common_dir_unavailable")
                return Path(common.stdout.strip()).expanduser().resolve(strict=True)

            if _git_common_dir(actual_root) != _git_common_dir(runtime_root):
                raise ValueError("git_common_dir_mismatch")
            top_level = _run_reclaim_command(["git", "-C", str(actual_root), "rev-parse", "--show-toplevel"])
            if (
                top_level is None
                or top_level.returncode != 0
                or Path((top_level.stdout or "").strip()).resolve(strict=True) != actual_root
            ):
                raise ValueError("landing_repo_root_not_worktree_root")

            from .orchestration.lane_land import _registered_integration_ref  # noqa: PLC0415

            registered_ref, registration_error = _registered_integration_ref(
                root=actual_root,
                task_ref=normalized_task_ref,
                requested_ref=landing_integration_ref,
                manifest_root=landing_manifest_root,
            )
            if registration_error is not None or registered_ref is None:
                raise ValueError(registration_error or "integration_ref_not_task_registered")
            normalized_registered_ref = registered_ref.removeprefix("refs/heads/")
            current_branch = _run_reclaim_command(
                ["git", "-C", str(actual_root), "symbolic-ref", "--quiet", "--short", "HEAD"]
            )
            if (
                current_branch is None
                or current_branch.returncode != 0
                or (current_branch.stdout or "").strip() != normalized_registered_ref
            ):
                raise ValueError("landing_repo_root_not_integration_worktree")
            integration_commit = _run_reclaim_command(
                [
                    "git",
                    "-C",
                    str(actual_root),
                    "rev-parse",
                    "--verify",
                    f"{landing_integration_ref}^{{commit}}",
                ]
            )
            if integration_commit is None or integration_commit.returncode != 0:
                raise ValueError("landing_integration_ref_unresolved")
        except Exception as exc:  # noqa: BLE001 — cleanup root must be independently authenticated
            return envelope(
                ok=False,
                outcome="landing_repo_root_unverified",
                error_kind="landing_repo_root_unverified",
                error=str(exc) or type(exc).__name__,
                **identity,
            )
        repo_root = actual_root

    gone = _probe_worktree_gone(recorded_path)
    if gone is None:
        return envelope(
            ok=False,
            outcome="worktree_probe_failed",
            error_kind="worktree_probe_failed",
            error="worktree path existence could not be probed",
            **identity,
        )

    def _branch_present() -> bool | None:
        if not branch:
            return False
        proc = _run_reclaim_command(
            ["git", "-C", str(repo_root), "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"]
        )
        if proc is None:
            return None
        if proc.returncode == 0:
            return True
        if proc.returncode == 1:
            return False
        return None

    present = _branch_present()
    if present is None:
        return envelope(
            ok=False,
            outcome="branch_probe_failed",
            error_kind="branch_probe_failed",
            error="branch presence could not be probed",
            **identity,
        )

    def _delete_merged_ref(
        *,
        bundle_path: object,
        bundle_tip_sha: object,
        worktree_reclaim: str,
        worktree_reclaim_detail: str,
        success_outcome: str,
    ) -> dict:
        if expected_tip is not None:
            authority_refusal = _check_close_authority()
            if authority_refusal is not None:
                return authority_refusal
            from .orchestration.branch_reclaim_delete import delete_authorized_branch  # noqa: PLC0415

            deleted = delete_authorized_branch(
                orchestrator_root=repo_root,
                lane_id=normalized_lane_id,
                branch=branch,
                authorized_sha=expected_tip,
                apply=True,
                task_ref=normalized_task_ref,
                integration_ref=integration_ref,
                _lock_held=landing_lock_held,
            )
            note_fence_release("branch_delete", deleted.fence_release)
            reason = (
                "retired"
                if deleted.deleted
                else "tip_moved_after_close"
                if deleted.reason == "authorized_sha_changed"
                else deleted.reason
            )
            return envelope(
                ok=deleted.deleted,
                outcome=reason,
                error_kind=None if deleted.deleted else reason,
                detail=deleted.detail,
                bundle_path=bundle_path,
                bundle_tip_sha=bundle_tip_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                branch_deleted=deleted.deleted,
                **identity,
            )
        delete_argv = ["git", "-C", str(repo_root), "branch", "-d", branch]
        from .orchestration.branch_reclaim_delete import (  # noqa: PLC0415
            CloseAuthorityBinding,
            bind_close_authority,
            run_under_close_authority,
        )

        # No close authority was read for this row: bind what was observed and re-prove it
        # immediately before each delete, refusing a row that gained authority or changed since.
        binding = bind_close_authority(task_ref=normalized_task_ref, lane_id=normalized_lane_id, branch=branch)
        if not binding.refusal and binding.authority is not None:
            binding = CloseAuthorityBinding("row_changed_after_close")

        def _bound_delete() -> tuple[str, subprocess.CompletedProcess[str] | None]:
            refusal, proc, _release = run_under_close_authority(binding, lambda: _run_reclaim_command(delete_argv))
            return refusal, proc

        refusal, proc = _bound_delete()
        if refusal:
            return envelope(
                ok=False,
                outcome=refusal,
                error_kind=refusal,
                bundle_path=bundle_path,
                bundle_tip_sha=bundle_tip_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        lock_info: dict[str, object] | None = None
        lock_recovery: dict[str, object] = {}
        if proc is None:
            return envelope(
                ok=False,
                outcome="branch_delete_unrunnable",
                error_kind="branch_delete_unrunnable",
                bundle_path=bundle_path,
                bundle_tip_sha=bundle_tip_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        if proc.returncode != 0:
            lock_info = _ref_lock_busy_info(repo_root, proc.stderr or proc.stdout or "")
            if lock_info is not None:
                lock_path = lock_info.get("lock_path")
                if isinstance(lock_path, str):
                    lock_recovery = recover_stale_ref_lock(repo_root, lock_path)
                else:
                    lock_recovery = {
                        "recovered": False,
                        "status": "lock_path_unavailable",
                        "stale_after_s": lock_info.get("stale_after_s"),
                    }
                if lock_recovery.get("recovered") is True:
                    refusal, proc = _bound_delete()
                    if refusal:
                        return envelope(
                            ok=False,
                            outcome=refusal,
                            error_kind=refusal,
                            lock_recovery=lock_recovery,
                            bundle_path=bundle_path,
                            bundle_tip_sha=bundle_tip_sha,
                            worktree_reclaim=worktree_reclaim,
                            worktree_reclaim_detail=worktree_reclaim_detail,
                            **identity,
                        )
                    if proc is None:
                        return envelope(
                            ok=False,
                            outcome="branch_delete_unrunnable",
                            error_kind="branch_delete_unrunnable",
                            lock_recovery=lock_recovery,
                            bundle_path=bundle_path,
                            bundle_tip_sha=bundle_tip_sha,
                            worktree_reclaim=worktree_reclaim,
                            worktree_reclaim_detail=worktree_reclaim_detail,
                            **identity,
                        )
                    if proc.returncode != 0:
                        lock_info = _ref_lock_busy_info(repo_root, proc.stderr or proc.stdout or "")
            if proc.returncode != 0 and lock_info is not None:
                detail = (proc.stderr or proc.stdout or "ref_lock_busy").strip()[:300]
                return envelope(
                    ok=False,
                    outcome="ref_lock_busy",
                    error_kind="ref_lock_busy",
                    error=detail,
                    detail=detail,
                    lock_path=lock_info.get("lock_path"),
                    age_s=lock_info.get("age_s"),
                    size=lock_info.get("size"),
                    stale_after_s=lock_info.get("stale_after_s"),
                    lock_recovery=lock_recovery,
                    bundle_path=bundle_path,
                    bundle_tip_sha=bundle_tip_sha,
                    worktree_reclaim=worktree_reclaim,
                    worktree_reclaim_detail=worktree_reclaim_detail,
                    **identity,
                )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "branch_delete_refused").strip()
            return envelope(
                ok=False,
                outcome="branch_delete_refused",
                error_kind="branch_delete_refused",
                error=detail,
                lock_recovery=lock_recovery,
                bundle_path=bundle_path,
                bundle_tip_sha=bundle_tip_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        return envelope(
            ok=True,
            outcome=success_outcome,
            bundle_path=bundle_path,
            bundle_tip_sha=bundle_tip_sha,
            lock_recovery=lock_recovery,
            worktree_reclaim=worktree_reclaim,
            worktree_reclaim_detail=worktree_reclaim_detail,
            branch_deleted=True,
            **identity,
        )

    held_lock = None

    def _check_close_authority(*, check_tip: bool = True) -> dict | None:
        if close_authority is None and expected_tip is None and expected_identity is None:
            return None
        try:
            fresh = get_lane(lane_id=normalized_lane_id, task_ref=normalized_task_ref)
            fresh_data = fresh.get("data") if isinstance(fresh.get("data"), dict) else fresh
            current_row = fresh_data.get("lane") if fresh.get("ok") is True else None
        except Exception as exc:  # noqa: BLE001 — failed reads never authorize disposal
            return envelope(
                ok=False, outcome="lane_row_read_failed", error_kind="lane_row_read_failed", error=str(exc), **identity
            )
        row_changed = not isinstance(current_row, dict)
        if not row_changed and expected_identity is not None:
            row_changed = any(current_row.get(key) != value for key, value in expected_identity.items())
        if not row_changed and close_authority is not None:
            row_changed = _close_authority_row_refusal(current_row, close_authority) != ""
        if row_changed:
            return envelope(
                ok=False, outcome="row_changed_after_close", error_kind="row_changed_after_close", **identity
            )
        if not check_tip or expected_tip is None:
            return None
        current = _run_reclaim_command(
            [
                "git",
                "-C",
                str(repo_root),
                "rev-parse",
                "--verify",
                f"refs/heads/{branch.removeprefix('refs/heads/')}^{{commit}}",
            ]
        )
        if current is None or current.returncode != 0:
            return envelope(ok=False, outcome="branch_probe_failed", error_kind="branch_probe_failed", **identity)
        current_tip = _probe_command_stdout(current).strip()
        if current_tip == expected_tip:
            return None
        diagnostics = {}
        if close_authority is not None and apply_flag:
            reopen_detail = _reopen_moved_closed_row(current_row)
            if reopen_detail:
                diagnostics["detail"] = reopen_detail
        return envelope(
            ok=False, outcome="tip_moved_after_close", error_kind="tip_moved_after_close", **diagnostics, **identity
        )

    def _bundle_or_refuse(*, worktree_reclaim: str, worktree_reclaim_detail: str) -> dict | tuple[str, str]:
        refusal = _check_close_authority()
        if refusal is not None:
            return refusal
        if not branch:
            return envelope(
                ok=False,
                outcome="bundle_failed",
                error_kind="branch_tip_unavailable",
                error="branch_tip_unavailable",
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        bundle = _bundle_before_reap(repo_root, branch)
        if not bundle.get("ok"):
            detail = str(bundle.get("error") or "bundle_failed")
            return envelope(
                ok=False,
                outcome="bundle_failed",
                error_kind=detail,
                error=detail,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        bundled_sha = str(bundle.get("tip_sha") or "")
        bundle_path = bundle.get("bundle_path")

        def _tip_unverified() -> dict:
            return envelope(
                ok=False,
                outcome="tip_unverified_after_bundle",
                error_kind="tip_unverified_after_bundle",
                error="branch tip could not be verified after bundle",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )

        if re.fullmatch(r"[0-9a-fA-F]{40}", bundled_sha) is None:
            return _tip_unverified()
        if (expected_tip is not None and bundled_sha != expected_tip) or (
            trusted_landing is not None and bundled_sha != landing_expected_tip
        ):
            return envelope(
                ok=False,
                outcome="tip_moved_after_bundle",
                error_kind="tip_moved_after_bundle",
                error=f"expected tip {expected_tip or landing_expected_tip} but bundle preserved {bundled_sha}",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )

        current = _run_reclaim_command(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"]
        )
        if current is None or current.returncode != 0:
            return _tip_unverified()
        current_sha = _probe_command_stdout(current).strip() if current is not None else ""
        if re.fullmatch(r"[0-9a-fA-F]{40}", current_sha) is None:
            return _tip_unverified()
        if current_sha != bundled_sha:
            if current_sha.casefold() == bundled_sha.casefold():
                return _tip_unverified()
            return envelope(
                ok=False,
                outcome="tip_moved_after_bundle",
                error_kind="tip_moved_after_bundle",
                error=f"bundled {bundled_sha} but ref now {current_sha}",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                worktree_reclaim=worktree_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        return (str(bundle_path or ""), bundled_sha)

    def _retire_terminal_unmerged(
        *,
        worktree_reclaim_detail: str,
        worktree_already_gone: bool = False,
    ) -> dict:
        from .orchestration.branch_reclaim_delete import (  # noqa: PLC0415
            _check_recovery_policy,
            _parse_authorized_target,
            _pin_authorized_tip,
            delete_authorized_branch,
        )

        pending_reclaim = "skipped_no_worktree_path" if worktree_already_gone else "would_reclaim"
        bundled = _bundle_or_refuse(
            worktree_reclaim=pending_reclaim,
            worktree_reclaim_detail=worktree_reclaim_detail,
        )
        if isinstance(bundled, dict):
            return bundled
        bundle_path, bundled_sha = bundled

        plan = _parse_authorized_target(normalized_lane_id, branch, bundled_sha)
        if not hasattr(plan, "reclaim_ref"):
            reason = getattr(plan, "reason", "invalid_branch")
            return envelope(
                ok=False,
                outcome=reason,
                error_kind=reason,
                error=getattr(plan, "detail", "branch authorization failed"),
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                worktree_reclaim=pending_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )
        pin_ref = str(plan.reclaim_ref)
        recovery_refusal = _check_recovery_policy(repo_root, plan)
        if recovery_refusal is not None:
            reason = recovery_refusal.reason
            return envelope(
                ok=False,
                outcome=reason,
                error_kind=reason,
                error=recovery_refusal.detail,
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=pending_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )

        authority_refusal = _check_close_authority()
        if authority_refusal is not None:
            return authority_refusal
        pin_refusal = _pin_authorized_tip(repo_root, plan)
        if pin_refusal is not None:
            reason = pin_refusal.reason
            return envelope(
                ok=False,
                outcome=reason,
                error_kind=reason,
                error=pin_refusal.detail,
                detail=pin_refusal.detail,
                lock_path=pin_refusal.lock_path,
                age_s=pin_refusal.age_s,
                size=pin_refusal.size,
                stale_after_s=pin_refusal.stale_after_s,
                lock_recovery=pin_refusal.lock_recovery,
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=pending_reclaim,
                worktree_reclaim_detail=worktree_reclaim_detail,
                **identity,
            )

        if worktree_already_gone:
            reclaim_outcome = "skipped_no_worktree_path"
        else:
            reclaim_outcome, reclaim_detail = _reclaim_lane_worktree(
                worktree_path=recorded_path,
                branch=branch,
                repo_root=repo_root,
                apply=True,
                integration_ref=integration_ref,
                task_ref=normalized_task_ref,
                lane_id=normalized_lane_id,
                allow_terminal_never_started=True,
                allow_terminal_unmerged_bundled=True,
                trusted_landing=trusted_landing,
                held_lock=held_lock,
                expected_tip=expected_tip,
                # Forwarded only for the landing caller, so every other fake and caller keeps its call.
                **({"landing_lock_held": True} if landing_lock_held else {}),
            )
            note_worktree_reclaim(reclaim_outcome, reclaim_detail, bundle_path, bundled_sha)
            if reclaim_outcome != "reclaimed":
                return envelope(
                    ok=False,
                    outcome=reclaim_outcome,
                    error_kind=reclaim_outcome,
                    bundle_path=bundle_path,
                    bundle_tip_sha=bundled_sha,
                    pin_ref=pin_ref,
                    worktree_reclaim=reclaim_outcome,
                    worktree_reclaim_detail=reclaim_detail,
                    lock_recovery=plan.lock_recovery,
                    **identity,
                )

        pin_probe = _run_reclaim_command(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", f"{pin_ref}^{{commit}}"]
        )
        pinned_sha = _probe_command_stdout(pin_probe).strip() if pin_probe is not None else ""
        if pin_probe is None or pin_probe.returncode != 0:
            return envelope(
                ok=False,
                outcome="pin_failed",
                error_kind="pin_failed",
                error="pin could not be verified after worktree reclaim",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=reclaim_outcome,
                worktree_reclaim_detail="terminal_unmerged_bundled",
                lock_recovery=plan.lock_recovery,
                **identity,
            )
        if pinned_sha != bundled_sha:
            return envelope(
                ok=False,
                outcome="pin_conflict",
                error_kind="pin_conflict",
                error=f"pinned {pinned_sha} but bundle preserved {bundled_sha}",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=reclaim_outcome,
                worktree_reclaim_detail="terminal_unmerged_bundled",
                lock_recovery=plan.lock_recovery,
                **identity,
            )

        current = _run_reclaim_command(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"]
        )
        current_sha = _probe_command_stdout(current).strip() if current is not None else ""
        if current is None or current.returncode != 0:
            return envelope(
                ok=False,
                outcome="branch_probe_failed",
                error_kind="branch_probe_failed",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=reclaim_outcome,
                worktree_reclaim_detail="terminal_unmerged_bundled",
                lock_recovery=plan.lock_recovery,
                **identity,
            )
        if current_sha != bundled_sha:
            return envelope(
                ok=False,
                outcome="tip_moved_after_bundle",
                error_kind="tip_moved_after_bundle",
                error=f"bundled {bundled_sha} but ref now {current_sha}",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=reclaim_outcome,
                worktree_reclaim_detail="terminal_unmerged_bundled",
                lock_recovery=plan.lock_recovery,
                **identity,
            )

        deleted = delete_authorized_branch(
            orchestrator_root=repo_root,
            lane_id=normalized_lane_id,
            branch=branch,
            authorized_sha=bundled_sha,
            apply=True,
            task_ref=normalized_task_ref,
            integration_ref=integration_ref,
            _lock_held=landing_lock_held,
        )
        note_fence_release("branch_delete", deleted.fence_release)
        if not deleted.deleted:
            reason = "tip_moved_after_bundle" if deleted.reason == "authorized_sha_changed" else deleted.reason
            return envelope(
                ok=False,
                outcome=reason,
                error_kind=reason,
                error=deleted.detail or reason,
                detail=deleted.detail or reason,
                lock_path=deleted.lock_path,
                age_s=deleted.age_s,
                size=deleted.size,
                stale_after_s=deleted.stale_after_s,
                lock_recovery=deleted.lock_recovery or plan.lock_recovery,
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                pin_ref=pin_ref,
                worktree_reclaim=reclaim_outcome,
                worktree_reclaim_detail="terminal_unmerged_bundled",
                **identity,
            )
        return envelope(
            ok=True,
            outcome="retired",
            bundle_path=bundle_path,
            bundle_tip_sha=bundled_sha,
            pin_ref=pin_ref,
            worktree_reclaim=reclaim_outcome,
            worktree_reclaim_detail="terminal_unmerged_bundled",
            branch_deleted=True,
            lock_recovery=plan.lock_recovery,
            **identity,
        )

    def _terminal_unmerged_delete_authorized() -> tuple[bool, str]:
        from dataclasses import replace

        from .orchestration import lane_disposition  # noqa: PLC0415
        from .orchestration.branch_reclaim_delete import (  # noqa: PLC0415
            _terminal_dispose_apply_values,
            _terminal_dispose_classification_enabled,
        )

        try:
            snapshot = lane_disposition.collect_snapshot(repo_root, branch, integration_ref=integration_ref)
            # An attached lane is previewed before its worktree is reclaimed.
            # On resume, retain every liveness observation: another worktree
            # must still block retirement. The monitor re-probes before delete.
            disposition = lane_disposition.decide(snapshot if gone else replace(snapshot, worktree_paths=()))
        except Exception as exc:  # noqa: BLE001 — unknown classification never authorizes disposal
            return (False, f"disposition_probe_failed:{type(exc).__name__}")
        if disposition.branch_action != "delete_D_after_pin":
            return (False, disposition.reason)
        if not _terminal_dispose_classification_enabled():
            return (False, "terminal_dispose_disabled")
        if disposition.kind.value not in _terminal_dispose_apply_values():
            return (False, "terminal_dispose_apply_not_allowed")
        return (True, disposition.reason)

    def _retire_locked() -> dict:
        authority_refusal = _check_close_authority(check_tip=not (gone and not present))
        if authority_refusal is not None:
            return authority_refusal
        if gone and not present:
            return envelope(
                ok=True,
                outcome="already_retired",
                worktree_reclaim="skipped_no_worktree_path",
                **identity,
            )
        if gone and not delete_branch:
            return envelope(
                ok=True,
                outcome="already_retired",
                worktree_reclaim="skipped_no_worktree_path",
                **identity,
            )

        if not gone:
            reclaim_outcome, reclaim_detail = _reclaim_lane_worktree(
                worktree_path=recorded_path,
                branch=branch,
                repo_root=repo_root,
                apply=False,
                integration_ref=integration_ref,
                task_ref=normalized_task_ref,
                lane_id=normalized_lane_id,
                allow_terminal_never_started=True,
                allow_terminal_unmerged_bundled=True,
                trusted_landing=trusted_landing,
                held_lock=held_lock,
                expected_tip=expected_tip,
                # Forwarded only for the landing caller, so every other fake and caller keeps its call.
                **({"landing_lock_held": True} if landing_lock_held else {}),
            )
            if reclaim_outcome != "would_reclaim":
                return envelope(
                    ok=False,
                    outcome=reclaim_outcome,
                    error_kind=reclaim_outcome,
                    worktree_reclaim=reclaim_outcome,
                    worktree_reclaim_detail=reclaim_detail,
                    **identity,
                )
            terminal_unmerged = reclaim_detail.startswith("terminal_unmerged_bundled;")
            if terminal_unmerged:
                delete_authorized, delete_detail = _terminal_unmerged_delete_authorized()
                if not delete_authorized:
                    return envelope(
                        ok=False,
                        outcome="refused_unmerged",
                        error_kind="refused_unmerged",
                        error=delete_detail,
                        worktree_reclaim="refused_unmerged",
                        worktree_reclaim_detail="worktree_head_not_merged",
                        **identity,
                    )
            if not apply_flag:
                return envelope(
                    ok=True,
                    outcome="would_retire",
                    worktree_reclaim=reclaim_outcome,
                    worktree_reclaim_detail=reclaim_detail,
                    **identity,
                )
            if terminal_unmerged:
                return _retire_terminal_unmerged(worktree_reclaim_detail=reclaim_detail)
            bundled = _bundle_or_refuse(worktree_reclaim="would_reclaim", worktree_reclaim_detail=reclaim_detail)
            if isinstance(bundled, dict):
                return bundled
            bundle_path, bundled_sha = bundled
            reclaim_outcome, reclaim_detail = _reclaim_lane_worktree(
                worktree_path=recorded_path,
                branch=branch,
                repo_root=repo_root,
                apply=True,
                integration_ref=integration_ref,
                task_ref=normalized_task_ref,
                lane_id=normalized_lane_id,
                allow_terminal_never_started=True,
                allow_terminal_unmerged_bundled=True,
                trusted_landing=trusted_landing,
                held_lock=held_lock,
                expected_tip=expected_tip,
                # Forwarded only for the landing caller, so every other fake and caller keeps its call.
                **({"landing_lock_held": True} if landing_lock_held else {}),
            )
            note_worktree_reclaim(reclaim_outcome, reclaim_detail, bundle_path, bundled_sha)
            if reclaim_outcome != "reclaimed":
                return envelope(
                    ok=False,
                    outcome=reclaim_outcome,
                    error_kind=reclaim_outcome,
                    bundle_path=bundle_path,
                    bundle_tip_sha=bundled_sha,
                    worktree_reclaim=reclaim_outcome,
                    worktree_reclaim_detail=reclaim_detail,
                    **identity,
                )
            if delete_branch:
                return _delete_merged_ref(
                    bundle_path=bundle_path,
                    bundle_tip_sha=bundled_sha,
                    worktree_reclaim=reclaim_outcome,
                    worktree_reclaim_detail=reclaim_detail,
                    success_outcome="retired",
                )
            return envelope(
                ok=True,
                outcome="retired",
                bundle_path=bundle_path,
                bundle_tip_sha=bundled_sha,
                worktree_reclaim=reclaim_outcome,
                worktree_reclaim_detail=reclaim_detail,
                **identity,
            )

        ancestry = _run_reclaim_command(
            ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", branch, integration_ref]
        )
        terminal_unmerged = ancestry is None or ancestry.returncode != 0
        if terminal_unmerged:
            delete_authorized, delete_detail = _terminal_unmerged_delete_authorized()
            if not delete_authorized:
                return envelope(
                    ok=False,
                    outcome="refused_unmerged",
                    error_kind="refused_unmerged",
                    error=delete_detail,
                    worktree_reclaim="skipped_no_worktree_path",
                    worktree_reclaim_detail="worktree_head_not_merged",
                    **identity,
                )

        if not apply_flag:
            return envelope(
                ok=True,
                outcome="would_retire",
                worktree_reclaim="skipped_no_worktree_path",
                **identity,
            )
        if terminal_unmerged:
            return _retire_terminal_unmerged(
                worktree_reclaim_detail="terminal_unmerged_bundled",
                worktree_already_gone=True,
            )
        bundled = _bundle_or_refuse(worktree_reclaim="skipped_no_worktree_path", worktree_reclaim_detail="")
        if isinstance(bundled, dict):
            return bundled
        bundle_path, bundled_sha = bundled
        return _delete_merged_ref(
            bundle_path=bundle_path,
            bundle_tip_sha=bundled_sha,
            worktree_reclaim="skipped_no_worktree_path",
            worktree_reclaim_detail="",
            success_outcome="retired",
        )

    # The flock is non-reentrant: share this handle with the reclaim helper.
    # Acquire before any bundle and retain it through pin, reclaim and delete.
    held_lock, lock_detail = _acquire_lane_worker_lock(normalized_lane_id)
    if held_lock is None:
        return envelope(
            ok=False, outcome="refused_lock_held", error_kind="refused_lock_held", detail=lock_detail, **identity
        )
    try:
        result = _retire_locked()
        if close_authority is not None and result.get("outcome") == "tip_moved_after_close":
            # Reclaim and the branch CAS can detect a move after our pre-pin
            # check. Apply the same reopen/event policy to their refusals.
            refusal = _check_close_authority()
            if refusal is not None and refusal.get("detail"):
                result["detail"] = refusal["detail"]
        return result
    finally:
        _release_lane_worker_lock(held_lock)


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
    speed: str | None = None,
    tier: str | None = None,
    test_cmd: str | None = None,
    lane_kind: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    task_ref: str | None = None,
    limit: int = 100,
    offset: int = 0,
    landing_commit_sha: str | None = None,
    branch_tip_sha: str | None = None,
    branch_tip_source: str | None = None,
    review_base_ref: str | None = None,
    review_tip_ref: str | None = None,
    update_review_subject: bool = False,
    apply: bool = False,
    delete_merged_branch: bool = False,
    expected_tip: str | None = None,
    run_id: str | None = None,
    integration_ref: str | None = None,
    superseded_by: str | None = None,
    plan_digest: str | None = None,
    allow_main: bool = False,
) -> dict:
    """Discriminated wrapper for worktree lane upsert, close, list, retire, land, and supersede operations."""
    valid_operations = {"close", "list", "update_routing", "upsert", "retire", "land", "supersede"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "upsert":
        try:
            _, normalized_test_cmd = classify_test_cmd(test_cmd)
        except (TypeError, ValueError) as exc:
            return _json_response({"ok": False, "error_kind": "test_cmd_unusable", "error": str(exc)})
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
            speed=speed,
            tier=tier,
            test_cmd=normalized_test_cmd,
            lane_kind=lane_kind,
            status=status,
            notes=notes,
            task_ref=task_ref,
            landing_commit_sha=landing_commit_sha,
            branch_tip_sha=branch_tip_sha,
            branch_tip_source=branch_tip_source,
            review_base_ref=review_base_ref,
            review_tip_ref=review_tip_ref,
            update_review_subject=update_review_subject,
        )
    if operation == "update_routing":
        return update_worktree_lane_routing(
            lane_id=str(lane_id or ""),
            task_ref=task_ref,
            backend=backend,
            model=model,
            reasoning_effort=reasoning_effort,
            speed=speed,
            tier=tier,
        )
    if operation == "close":
        return close_worktree_lane(
            lane_id=str(lane_id or ""),
            status=status or "closed",
            notes=notes,
            task_ref=task_ref,
            landing_commit_sha=landing_commit_sha,
            branch_tip_sha=branch_tip_sha,
        )
    if operation == "retire":
        return _retire_worktree_lane(
            lane_id=lane_id,
            task_ref=task_ref,
            apply=apply,
            delete_merged_branch=delete_merged_branch,
        )
    if operation == "land":
        from .orchestration import lane_land  # noqa: PLC0415

        return _json_response(
            lane_land.land(
                task_ref=str(task_ref or ""),
                lane_id=str(lane_id or ""),
                expected_tip=str(expected_tip or ""),
                run_id=run_id,
                integration_ref=integration_ref,
            )
        )
    if operation == "supersede":
        if apply and not plan_digest:
            return _json_response(
                {
                    "ok": False,
                    "error": "apply requires the plan_digest returned by a supersede preview",
                    "error_code": "plan_digest_required",
                    "field": "plan_digest",
                }
            )
        from dataclasses import asdict  # noqa: PLC0415

        from .orchestration import lane_tombstone  # noqa: PLC0415

        result = lane_tombstone.supersede(
            _workspace_root(),
            branch=str(branch or ""),
            superseded_by=str(superseded_by or ""),
            integration_ref=str(integration_ref or ""),
            apply=apply,
            allow_main=allow_main,
            plan_digest=plan_digest,
        )
        return _json_response(asdict(result))
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
                if section_name == "messages":
                    summarized = _public_lane_message_row(summarized)
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
    delivery_id: str | None = None,
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
    normalized_delivery_id = _normalize_optional_text(delivery_id)
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
    changed_files_json = json.dumps(changed_files or [])
    test_commands_json = json.dumps(test_commands or [])
    blockers_json = json.dumps(blockers or [])
    merge_ready_value = 1 if merge_ready else 0
    content_fingerprint = _worker_report_content_fingerprint(
        summary=summary,
        test_commands_json=test_commands_json,
        merge_ready=merge_ready_value,
    )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        if _get_lane_row(conn, resolved_task_ref, normalized_lane_id) is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        cur = conn.execute(
            """
            INSERT INTO worker_reports (
                task_ref, lane_id, session, delivery_id, summary, changed_files_json, test_commands_json, blockers_json,
                merge_ready, status, outcome, agent, branch, commit_sha, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(delivery_id) DO NOTHING
            RETURNING id
            """,
            (
                resolved_task_ref,
                normalized_lane_id,
                session,
                normalized_delivery_id,
                summary,
                changed_files_json,
                test_commands_json,
                blockers_json,
                merge_ready_value,
                status,
                normalized_outcome,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
            ),
        )
        inserted = cur.fetchone()
        if inserted is None:
            row = _row_to_dict(
                conn.execute(
                    "SELECT * FROM worker_reports WHERE delivery_id = ?",
                    (normalized_delivery_id,),
                ).fetchone()
            )
            if row is None or row.get("task_ref") != resolved_task_ref or row.get("lane_id") != normalized_lane_id:
                return _json_response(
                    {
                        "ok": False,
                        "error_code": "delivery_id_conflict",
                        "error": "delivery_id is already claimed by a different task or lane.",
                    }
                )
            persisted_fingerprint = _worker_report_content_fingerprint(
                summary=str(row.get("summary") or ""),
                test_commands_json=str(row.get("test_commands_json") or "[]"),
                merge_ready=int(row.get("merge_ready") or 0),
            )
            if persisted_fingerprint != content_fingerprint:
                return _json_response(
                    {
                        "ok": False,
                        "error_code": "delivery_id_content_mismatch",
                        "error": "delivery_id is already claimed by a report with different content.",
                    }
                )
            return _json_response(
                {
                    "ok": True,
                    "report": row,
                    "idempotent_replay": True,
                    "current_task_md_written": None,
                }
            )
        row = _row_to_dict(conn.execute("SELECT * FROM worker_reports WHERE id = ?", (inserted["id"],)).fetchone())
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
    delivery_id: Annotated[
        str | None,
        Field(description="Stable delivery identifier; it must be unique across all lanes and tasks."),
    ] = None,
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
            delivery_id=delivery_id,
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


def _lane_message_content_matches(
    existing, *, direction: str, subject: str | None, message: str, payload: object
) -> bool:
    """Compare immutable content before read-time summary hydration, excluding provenance/status."""
    existing_payload, existing_error = _normalize_lane_message_payload(
        json.loads(existing["payload_json"]) if existing["payload_json"] is not None else None
    )
    requested_payload, requested_error = _normalize_lane_message_payload(payload)
    return (
        existing_error is None
        and requested_error is None
        and existing_payload == requested_payload
        and all(
            _normalize_optional_text(existing[field]) == _normalize_optional_text(value)
            for field, value in (("direction", direction), ("subject", subject), ("message", message))
        )
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
    delivery_id: str | None = None,
    delivery_effect: str = "message",
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    kind: str | None = None,
) -> dict:
    valid_directions = LANE_MESSAGE_DIRECTIONS
    valid_statuses = MESSAGE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    projection_refusal = _validate_lane_communication_projection(
        operation="record",
        kind=kind,
        sections=sections,
        fields=fields,
        detail=detail,
    )
    if projection_refusal is not None:
        return _json_response(projection_refusal)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if direction not in valid_directions:
        return _json_response(
            {"ok": False, "error": f"Invalid direction. Valid: {', '.join(sorted(valid_directions))}"}
        )
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    normalized_delivery_id = _normalize_optional_text(delivery_id)
    if normalized_delivery_id is not None and (payload is None or isinstance(payload, dict)):
        claimed_payload = dict(payload or {})
        claimed_payload["dispatch_id"] = _handoff_lane_message_dispatch_id(
            normalized_delivery_id,
            _normalize_optional_text(delivery_effect) or "message",
        )
        payload = claimed_payload
    normalized_payload, payload_error = _normalize_lane_message_payload(payload)
    if payload_error is not None:
        return _json_response({"ok": False, "error": payload_error})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        if _get_lane_row(conn, resolved_task_ref, normalized_lane_id) is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        identity_refusal = _identity_budget_refusal(
            task_ref=resolved_task_ref,
            lane_id=normalized_lane_id,
            operation="record",
            kind=kind,
        )
        if identity_refusal is not None:
            return _json_response(identity_refusal)
        ctx = _resolve_write_actor(conn, actor)
        _ensure_lane_message_revision_schema(conn)
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
        except sqlite3.IntegrityError as exc:
            # DATA-14 / DDIA end-to-end idempotency: SQLite is the authority;
            # only the dispatch-key constraint can establish a replay.
            if (
                dispatch_id is None
                or exc.sqlite_errorcode != sqlite3.SQLITE_CONSTRAINT_UNIQUE
                or str(exc)
                != (
                    "UNIQUE constraint failed: lane_messages.task_ref, lane_messages.lane_id, lane_messages.dispatch_id"
                )
            ):
                raise
            existing = conn.execute(
                "SELECT * FROM lane_messages WHERE task_ref = ? AND lane_id = ? AND dispatch_id = ?",
                (resolved_task_ref, normalized_lane_id, dispatch_id),
            ).fetchone()
            if existing is None:
                raise
            if not _lane_message_content_matches(
                existing, direction=direction, subject=subject, message=message, payload=normalized_payload
            ):
                return _json_response(
                    _bounded_error_payload(
                        {
                            "ok": False,
                            "error_code": "idempotency_conflict",
                            "error": "dispatch_id is already claimed by a message with different content.",
                            "message": _public_lane_message_row(_decode_lane_message_row_dict(dict(existing))),
                        }
                    )
                )
            duplicate_dispatch = True
            message_id = existing["id"]
        row = _row_to_dict(conn.execute("SELECT * FROM lane_messages WHERE id = ?", (message_id,)).fetchone())
        if row is not None:
            row = _decode_lane_message_row_dict(row)
        # HOLDERCLASS-R1-F3 / CON-18: defer CURRENT_TASK render past commit.

    if duplicate_dispatch:
        data = {
            "ok": True,
            "message": row,
            "duplicate_dispatch": True,
            "current_task_md_written": None,
        }
        return _finalize_lane_message_write(
            data,
            row=row or {},
            kind=kind,
            sections=sections,
            detail=detail,
            fields=fields,
            operation="record",
        )

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
    return _finalize_lane_message_write(
        data,
        row=row or {},
        kind=kind,
        sections=sections,
        detail=detail,
        fields=fields,
        operation="record",
    )


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
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
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
    normalized_message = _normalize_optional_text(message)
    brief_payload: dict[str, object] = {
        "source_lane": normalized_source_lane,
        "reason": normalized_reason,
    }
    if normalized_message is not None:
        brief_payload["summary"] = normalized_summary
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
        message=message if normalized_message is not None else normalized_summary,
        status=status,
        payload=brief_payload,
        task_ref=task_ref,
        actor=actor,
        sections=sections,
        detail=detail,
        fields=fields,
        kind="brief",
    )


def acknowledge_lane_brief_on_completion(
    *,
    task_ref: str,
    lane_id: str,
    dispatch_id: str | None,
    outcome: str,
) -> dict[str, object]:
    """CAS-acknowledge the open orchestrator_to_worker brief for ``dispatch_id``.

    Applied only for :data:`BRIEF_ACK_SUCCESS_OUTCOMES`. Any other outcome is a
    no-write (the brief stays ``open``). Blank ``dispatch_id`` refuses — never
    fall back to "latest open brief". Re-ack of an already-acknowledged row is
    a compare-and-set no-op (``updated=False``), matching
    :func:`acknowledge_worker_report`. Targets the dispatch brief only:
    :func:`open_assignment_brief_sql_predicate` so a coexisting retry note
    (``note:execution``, outside ``brief:%``) is not consumed.
    """
    normalized_dispatch_id = _normalize_optional_text(dispatch_id)
    if normalized_dispatch_id is None:
        return {"updated": False, "message_id": None, "reason": "dispatch_id_required"}
    if outcome not in BRIEF_ACK_SUCCESS_OUTCOMES:
        return {"updated": False, "message_id": None, "reason": "outcome_not_terminal_success"}
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return {"updated": False, "message_id": None, "reason": "no_open_brief_for_dispatch"}

    ack_status = "acknowledged"
    open_status = "open"
    if ack_status not in MESSAGE_STATUSES or open_status not in MESSAGE_STATUSES:
        return {"updated": False, "message_id": None, "reason": "no_open_brief_for_dispatch"}

    with _get_db_connection() as conn:
        _ensure_lane_message_revision_schema(conn)
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            f"""
            UPDATE lane_messages
            SET status = ?, updated_at = datetime('now')
            WHERE task_ref = ?
              AND lane_id = ?
              AND dispatch_id = ?
              AND direction = 'orchestrator_to_worker'
              AND {open_assignment_brief_sql_predicate()}
              AND status = ?
            RETURNING id
            """,
            (
                ack_status,
                resolved_task_ref,
                normalized_lane_id,
                normalized_dispatch_id,
                *open_assignment_brief_sql_params(),
                open_status,
            ),
        ).fetchone()
        if row is None:
            return {"updated": False, "message_id": None, "reason": "no_open_brief_for_dispatch"}
        return {"updated": True, "message_id": int(row["id"]), "reason": "acknowledged"}


def build_retry_note(
    *,
    lane_id: str,
    prior_outcome: str,
    prior_summary: str,
    landed_commit: str | None = None,
    first_action: str | None = None,
) -> str:
    """Build the standard EXECUTION NOTE text for a failed prior pass."""
    _ = lane_id
    landed = _normalize_optional_text(landed_commit) or "nothing"
    action = "" if first_action is None else str(first_action)
    outcome_text = "" if prior_outcome is None else str(prior_outcome)
    summary = "" if prior_summary is None else str(prior_summary)
    prefix = f"EXECUTION NOTE (retry): a prior pass ended {outcome_text}; "
    suffix = f". Landed: {landed}. Resume, do not restart. Begin with the FIRST ACTION immediately: {action}."
    budget = _RETRY_NOTE_MAX_CHARS - len(prefix) - len(suffix)
    if budget < 0:
        note = f"{prefix}{suffix.lstrip('. ')}"
        return note[:_RETRY_NOTE_MAX_CHARS]
    if len(summary) > budget:
        ellipsis = "..."
        keep = max(0, budget - len(ellipsis))
        summary = summary[:keep] + ellipsis
        overflow = len(prefix) + len(summary) + len(suffix) - _RETRY_NOTE_MAX_CHARS
        if overflow > 0:
            summary = summary[: max(0, len(summary) - overflow)]
    return f"{prefix}{summary}{suffix}"


def _retry_note_payload_json(dispatch_id: str) -> str:
    """Idempotency key for a retry note: the original dispatch_id, not a derived slot.

    ``idx_lane_messages_dispatch_id`` is UNIQUE on ``(task_ref, lane_id, dispatch_id)``
    where ``dispatch_id IS NOT NULL``. Occupying the original slot reopens F1;
    occupying ``{dispatch_id}:execution-note`` reopens F4 (latest-open alias).
    The note therefore stores ``dispatch_id`` NULL and keys the original id in
    payload so it is invisible to latest-open-dispatch consumers.
    """
    return json.dumps({_RETRY_NOTE_PAYLOAD_DISPATCH_KEY: dispatch_id}, sort_keys=True)


def record_retry_note(
    *,
    task_ref: str,
    lane_id: str,
    dispatch_id: str,
    note: str,
) -> dict[str, object]:
    """Insert an open retry note; idempotent per original failed dispatch.

    Subject is :data:`RETRY_NOTE_SUBJECT` (``note:execution``), outside
    ``brief:%``. The row does not occupy a dispatch_id slot. Replay of the
    same original ``dispatch_id`` returns ``inserted=False`` and the original
    ``message_id``. A blank note fails closed with ``reason=note_required``.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_dispatch_id = _normalize_optional_text(dispatch_id)
    if normalized_lane_id is None or normalized_dispatch_id is None:
        return {"inserted": False, "message_id": None}
    if not isinstance(note, str) or not note.strip():
        return {"inserted": False, "message_id": None, "reason": "note_required"}

    open_status = "open"
    if open_status not in MESSAGE_STATUSES:
        return {"inserted": False, "message_id": None}

    payload_json = _retry_note_payload_json(normalized_dispatch_id)
    with _get_db_connection() as conn:
        _ensure_lane_message_revision_schema(conn)
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing = conn.execute(
            """
            SELECT id FROM lane_messages
            WHERE task_ref = ?
              AND lane_id = ?
              AND subject = ?
              AND json_extract(payload_json, '$.dispatch_id') = ?
            """,
            (
                resolved_task_ref,
                normalized_lane_id,
                RETRY_NOTE_SUBJECT,
                normalized_dispatch_id,
            ),
        ).fetchone()
        if existing is not None:
            return {"inserted": False, "message_id": int(existing["id"])}
        try:
            cur = conn.execute(
                """
                INSERT INTO lane_messages (
                    task_ref, lane_id, session, direction, subject, message, status, dispatch_id,
                    payload_json, created_at, updated_at
                )
                VALUES (?, ?, ?, 'orchestrator_to_worker', ?, ?, ?, NULL, ?, datetime('now'), datetime('now'))
                """,
                (
                    resolved_task_ref,
                    normalized_lane_id,
                    f"{resolved_task_ref}-{normalized_lane_id}",
                    RETRY_NOTE_SUBJECT,
                    note,
                    open_status,
                    payload_json,
                ),
            )
            return {"inserted": True, "message_id": int(cur.lastrowid)}
        except sqlite3.IntegrityError:
            replay = conn.execute(
                """
                SELECT id FROM lane_messages
                WHERE task_ref = ?
                  AND lane_id = ?
                  AND subject = ?
                  AND json_extract(payload_json, '$.dispatch_id') = ?
                """,
                (
                    resolved_task_ref,
                    normalized_lane_id,
                    RETRY_NOTE_SUBJECT,
                    normalized_dispatch_id,
                ),
            ).fetchone()
            if replay is not None:
                return {"inserted": False, "message_id": int(replay["id"])}
            return {
                "inserted": False,
                "message_id": None,
                "reason": "dispatch_id_already_used",
            }


def update_lane_message(
    message_id: int,
    status: str,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    kind: str | None = None,
) -> dict:
    valid_statuses = MESSAGE_STATUSES
    projection_refusal = _validate_lane_communication_projection(
        operation="update",
        kind=kind,
        sections=sections,
        fields=fields,
        detail=detail,
    )
    if projection_refusal is not None:
        return _json_response(projection_refusal)
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            "SELECT * FROM lane_messages WHERE id = ? AND task_ref = ?", (message_id, resolved_task_ref)
        ).fetchone()
        if row is None:
            return _json_response({"ok": False, "error": "Message not found for task_ref."})
        identity_refusal = _identity_budget_refusal(
            task_ref=resolved_task_ref,
            lane_id=row["lane_id"] if isinstance(row, sqlite3.Row) else dict(row).get("lane_id"),
            operation="update",
            kind=kind,
        )
        if identity_refusal is not None:
            return _json_response(identity_refusal)
        ctx = _resolve_write_actor(conn, actor)
        _ensure_lane_message_revision_schema(conn)
        conn.execute(
            "UPDATE lane_messages SET status = ?, agent = COALESCE(agent, ?), branch = COALESCE(branch, ?), commit_sha = COALESCE(commit_sha, ?), updated_at = datetime('now'), revision = revision + 1 WHERE id = ? AND task_ref = ?",
            (status, ctx.agent, ctx.branch, ctx.commit_sha, message_id, resolved_task_ref),
        )
        updated = _row_to_dict(conn.execute("SELECT * FROM lane_messages WHERE id = ?", (message_id,)).fetchone())
        if updated is not None:
            updated = _decode_lane_message_row_dict(updated)
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
    return _finalize_lane_message_write(
        data,
        row=updated or {},
        kind=kind,
        sections=sections,
        detail=detail,
        fields=fields,
        operation="update",
    )


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
    message_id: int | None = None,
    chunk_offset: int | None = None,
    chunk_limit: int | None = None,
    committed_revision: str | None = None,
    cursor_payload_mode: bool = False,
    cursor_more_rows_after: bool = False,
) -> dict:
    """List lane messages with optional scope and content filters.

    ``direction`` restricts to a specific message direction (e.g. ``"orchestrator_to_worker"``).
    ``subject_prefix`` restricts to messages whose subject starts with the given prefix
    (e.g. ``"brief:"``), making this function capable of subsuming ``list_lane_briefs``.
    """
    valid_statuses = {"all", *MESSAGE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    projection_refusal = _validate_lane_communication_projection(
        operation="list",
        kind="message",
        sections=sections,
        fields=fields,
        detail=detail,
    )
    if projection_refusal is not None:
        return _json_response(_bounded_error_payload(projection_refusal))
    chunk_refusal = _validate_list_chunk_args(chunk_offset=chunk_offset, chunk_limit=chunk_limit)
    if chunk_refusal is not None:
        return _json_response(chunk_refusal)
    limit = _effective_limit(limit, top_n_messages)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        _ensure_lane_message_revision_schema(conn)
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
        revision_pin = _decode_committed_revision_pin(committed_revision) if committed_revision is not None else None
        if (
            committed_revision is not None
            and revision_pin is None
            and _is_committed_revision_pin_candidate(committed_revision)
        ):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "Committed revision pin is missing a valid message ID or revision.",
                }
            )
        expected_revision: str | None = None
        expected_legacy_revision: str | None = None
        if revision_pin is not None:
            pinned_message_id, expected_revision = revision_pin
            if message_id is not None and int(message_id) != pinned_message_id:
                return _bounded_error_payload(
                    {
                        "ok": False,
                        "error_kind": "revision_mismatch",
                        "error": "Committed revision message ID conflicts with message_id.",
                    }
                )
        elif message_id is not None:
            pinned_message_id = int(message_id)
            if committed_revision is not None:
                expected_legacy_revision = str(committed_revision)
        else:
            pinned_message_id = None
        if pinned_message_id is not None:
            where_sql += " AND id = ?"
            params.append(pinned_message_id)
        elif committed_revision is not None:
            # Legacy clients send a timestamp-only revision. Keep its original
            # timestamp search behavior when no immutable row ID is available.
            where_sql += " AND COALESCE(NULLIF(updated_at, ''), NULLIF(created_at, ''), CAST(id AS TEXT)) = ?"
            params.append(str(committed_revision))
        query_limit = 1 if pinned_message_id is not None else limit
        query_offset = 0 if pinned_message_id is not None else offset
        total, rows = _paginated_query(
            conn,
            "lane_messages",
            where_sql,
            tuple(params),
            query_limit,
            query_offset,
            "updated_at DESC, id DESC",
            _decode_lane_message_row_dict,
        )
        if committed_revision is not None and not rows:
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "No row matches the committed revision.",
                }
            )
        if (
            expected_legacy_revision is not None
            and rows
            and not _legacy_committed_revision_matches(rows[0], expected_legacy_revision)
        ):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "The pinned message changed after this revision was committed.",
                }
            )
        if expected_revision is not None and rows and not _committed_revision_matches(rows[0], expected_revision):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "The pinned message changed after this revision was committed.",
                }
            )
        shaped = _shape_list_payload(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "current_lane": inferred_lane,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": query_offset + len(rows) < total,
                "messages": rows,
            },
            sections=sections,
            detail=detail,
            fields=fields,
            row_key="messages",
            identity_fields=_LANE_MESSAGE_IDENTITY_FIELDS,
            summary_fn=_summarize_lane_message_row,
        )
        if shaped.get("ok") is True and sections is None:
            _attach_list_collection(shaped, row_key="messages")
        budgeted = _apply_list_envelope_budget(
            shaped,
            kind="message",
            row_key="messages",
            fetch_base={
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "status": status,
                "offset": query_offset if pinned_message_id is None else offset,
                "limit": limit,
                "message_id": pinned_message_id,
                "committed_revision": committed_revision,
                "detail": detail,
                "chunk_offset": chunk_offset or 0,
                "chunk_limit": chunk_limit,
                "sections": sections,
                "fields": fields,
                "direction": direction,
                "subject_prefix": subject_prefix,
                "cursor_payload_mode": cursor_payload_mode,
                "cursor_more_rows_after": cursor_more_rows_after,
            },
        )
        return _json_response(_public_lane_message_list_result(budgeted, row_key="messages"))


def list_lane_briefs(
    task_ref: str | None = None,
    lane_id: str | None = None,
    status: str = "open",
    limit: int = 20,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_messages: int | None = None,
    message_id: int | None = None,
    chunk_offset: int | None = None,
    chunk_limit: int | None = None,
    committed_revision: str | None = None,
    cursor_payload_mode: bool = False,
    cursor_more_rows_after: bool = False,
) -> dict:
    valid_statuses = {"all", *MESSAGE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    projection_refusal = _validate_lane_communication_projection(
        operation="list",
        kind="brief",
        sections=sections,
        fields=fields,
        detail=detail,
    )
    if projection_refusal is not None:
        return _json_response(_bounded_error_payload(projection_refusal))
    chunk_refusal = _validate_list_chunk_args(chunk_offset=chunk_offset, chunk_limit=chunk_limit)
    if chunk_refusal is not None:
        return _json_response(chunk_refusal)
    limit = _effective_limit(limit, top_n_messages)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        _ensure_lane_message_revision_schema(conn)
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref, "orchestrator_to_worker", "brief:%"]
        where_sql = "task_ref = ? AND direction = ? AND subject LIKE ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        revision_pin = _decode_committed_revision_pin(committed_revision) if committed_revision is not None else None
        if (
            committed_revision is not None
            and revision_pin is None
            and _is_committed_revision_pin_candidate(committed_revision)
        ):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "Committed revision pin is missing a valid message ID or revision.",
                }
            )
        expected_revision: str | None = None
        expected_legacy_revision: str | None = None
        if revision_pin is not None:
            pinned_message_id, expected_revision = revision_pin
            if message_id is not None and int(message_id) != pinned_message_id:
                return _bounded_error_payload(
                    {
                        "ok": False,
                        "error_kind": "revision_mismatch",
                        "error": "Committed revision message ID conflicts with message_id.",
                    }
                )
        elif message_id is not None:
            pinned_message_id = int(message_id)
            if committed_revision is not None:
                expected_legacy_revision = str(committed_revision)
        else:
            pinned_message_id = None
        if pinned_message_id is not None:
            where_sql += " AND id = ?"
            params.append(pinned_message_id)
        elif committed_revision is not None:
            # Legacy clients send a timestamp-only revision. Keep its original
            # timestamp search behavior when no immutable row ID is available.
            where_sql += " AND COALESCE(NULLIF(updated_at, ''), NULLIF(created_at, ''), CAST(id AS TEXT)) = ?"
            params.append(str(committed_revision))
        query_limit = 1 if pinned_message_id is not None else limit
        query_offset = 0 if pinned_message_id is not None else offset
        total, rows = _paginated_query(
            conn,
            "lane_messages",
            where_sql,
            tuple(params),
            query_limit,
            query_offset,
            "updated_at DESC, id DESC",
            _decode_lane_message_row_dict,
        )
        if committed_revision is not None and not rows:
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "No row matches the committed revision.",
                }
            )
        if (
            expected_legacy_revision is not None
            and rows
            and not _legacy_committed_revision_matches(rows[0], expected_legacy_revision)
        ):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "The pinned message changed after this revision was committed.",
                }
            )
        if expected_revision is not None and rows and not _committed_revision_matches(rows[0], expected_revision):
            return _bounded_error_payload(
                {
                    "ok": False,
                    "error_kind": "revision_mismatch",
                    "error": "The pinned message changed after this revision was committed.",
                }
            )
        shaped = _shape_list_payload(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": query_offset + len(rows) < total,
                "briefs": rows,
            },
            sections=sections,
            detail=detail,
            fields=fields,
            row_key="briefs",
            identity_fields=_LANE_MESSAGE_IDENTITY_FIELDS,
            summary_fn=_summarize_lane_message_row,
        )
        if shaped.get("ok") is True and sections is None:
            _attach_list_collection(shaped, row_key="briefs")
        budgeted = _apply_list_envelope_budget(
            shaped,
            kind="brief",
            row_key="briefs",
            fetch_base={
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "status": status,
                "offset": query_offset if pinned_message_id is None else offset,
                "limit": limit,
                "message_id": pinned_message_id,
                "committed_revision": committed_revision,
                "detail": detail,
                "chunk_offset": chunk_offset or 0,
                "chunk_limit": chunk_limit,
                "sections": sections,
                "fields": fields,
                "cursor_payload_mode": cursor_payload_mode,
                "cursor_more_rows_after": cursor_more_rows_after,
            },
        )
        return _json_response(_public_lane_message_list_result(budgeted, row_key="briefs"))


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
    detail: str | None = None,
    fields: str | None = None,
    top_n_messages: int | None = None,
    chunk_offset: int | None = None,
    chunk_limit: int | None = None,
    committed_revision: str | None = None,
) -> dict:
    """Discriminated wrapper for lane message and brief operations.

    Write receipts encode ``committed_revision`` as JSON with ``id`` and
    ``revision`` keys. Timestamp-only committed revisions remain supported as
    a legacy selector for clients that do not provide a message ID.
    """
    valid_kinds = frozenset({"message", "brief"})
    valid_operations = frozenset({"record", "update", "list"})
    if kind not in valid_kinds:
        return _json_response(_bounded_error_payload(_invalid_projection_refusal("kind", kind, valid_kinds)))
    if operation not in valid_operations:
        return _json_response(
            _bounded_error_payload(_invalid_projection_refusal("operation", operation, valid_operations))
        )
    projection_refusal = _validate_lane_communication_projection(
        operation=operation,
        kind=kind,
        sections=sections,
        fields=fields,
        detail=detail,
    )
    if projection_refusal is not None:
        return _json_response(_bounded_error_payload(projection_refusal))

    cursor_payload_mode = False
    cursor_more_rows_after = False
    if operation == "list":
        cursor_state, cursor_refusal = _decode_lane_communication_cursor_payload(payload)
        if cursor_refusal is not None:
            return _json_response(_bounded_error_payload(cursor_refusal))
        if cursor_state is not None:
            if chunk_offset is not None or chunk_limit is not None or committed_revision is not None:
                return _json_response(
                    _bounded_error_payload(
                        {
                            "ok": False,
                            "error_kind": "invalid_chunk",
                            "error": "Continuation payload cannot be combined with explicit chunk selectors.",
                        }
                    )
                )
            chunk_offset = int(cursor_state["chunk_offset"])
            chunk_limit = cursor_state.get("chunk_limit")  # type: ignore[assignment]
            committed_revision = cursor_state.get("committed_revision")  # type: ignore[assignment]
            cursor_message_id = cursor_state.get("message_id")
            if cursor_message_id is not None:
                if message_id is not None and int(message_id) != int(cursor_message_id):
                    return _json_response(
                        _bounded_error_payload(
                            {
                                "ok": False,
                                "error_kind": "revision_mismatch",
                                "error": "Continuation message ID conflicts with message_id.",
                            }
                        )
                    )
                message_id = int(cursor_message_id)
            cursor_payload_mode = True
            cursor_more_rows_after = cursor_state.get("more_rows_after") is True

    if operation == "record":
        if kind == "message":
            handoff_delivery_id = os.environ.get(HANDOFF_LANE_MESSAGE_DELIVERY_ID_ENV)
            handoff_delivery_effect = os.environ.get(HANDOFF_LANE_MESSAGE_DELIVERY_EFFECT_ENV, "message")
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
                delivery_id=handoff_delivery_id,
                delivery_effect=handoff_delivery_effect,
                sections=sections,
                detail=detail or "summary",
                fields=fields,
                kind="message",
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
            sections=sections,
            detail=detail or "summary",
            fields=fields,
        )

    if operation == "update":
        if message_id is None:
            return _json_response({"ok": False, "error": "message_id is required for update."})
        return update_lane_message(
            message_id=message_id,
            status=status,
            task_ref=task_ref,
            actor=actor,
            sections=sections,
            detail=detail or "summary",
            fields=fields,
            kind=kind,
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
            detail=detail or "full",
            fields=fields,
            top_n_messages=top_n_messages,
            message_id=message_id,
            chunk_offset=chunk_offset,
            chunk_limit=chunk_limit,
            committed_revision=committed_revision,
            cursor_payload_mode=cursor_payload_mode,
            cursor_more_rows_after=cursor_more_rows_after,
        )
    return list_lane_briefs(
        task_ref=task_ref,
        lane_id=lane_id,
        status=status,
        limit=limit,
        offset=offset,
        sections=sections,
        detail=detail or "full",
        fields=fields,
        top_n_messages=top_n_messages,
        message_id=message_id,
        chunk_offset=chunk_offset,
        chunk_limit=chunk_limit,
        committed_revision=committed_revision,
        cursor_payload_mode=cursor_payload_mode,
        cursor_more_rows_after=cursor_more_rows_after,
    )


# Plan cursor CRUD and blocked-lane reaping live in .plan_cursors /
# .lane_reaping; its public re-exports resolve lazily through __getattr__.
