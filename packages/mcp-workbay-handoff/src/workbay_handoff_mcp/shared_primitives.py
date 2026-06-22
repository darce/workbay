"""Primitive cross-cutting utilities for workbay_handoff_mcp.

Extracted from _shared.py (M-1058-01 / M-1058-02 follow-up). Contains
the lowest-level helpers that every focused module needs without creating
circular imports:

  - Typed containers: TokenUsage, PromptMetrics, ReviewFindingDetails,
    LaneMessagePayload
  - Domain constants: frozensets from enums, integer validation limits
  - Workspace path helpers: _workspace_root, _current_task_path, _exports_dir
  - Text utilities: _normalize_optional_text, _first_present, _utcnow_iso,
    _json_response, _excerpt_text
  - DB row utilities: _row_to_dict, _coerce_string_list, _resolve_task_ref,
    _decode_lane_message_row_dict, _decode_turn_metric_row_dict
  - Datetime utilities: _parse_sqlite_datetime
  - Normalization helpers: _normalize_review_mode, _normalize_lane_message_payload,
    _normalize_path_for_match, _resolve_current_lane_row
  - Test-result utilities: _summarize_test_result
  - Decision/slice utilities: _has_structured_slice_summary, _validate_decision_payload
  - Review/import helpers: _parse_review_finding_details, _resolve_import_row_actor,
    _resolve_import_lane_id

No imports from .shared_write_context, .shared_schema, .shared_db_utils,
.shared_archival, .current_task_rendering, or .shared_tool_adapters (avoids
circular dependencies).  _shared.py re-exports from here for backward compat.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, TypedDict, cast

from .enums import (
    ActionStatus,
    BlockerStatus,
    FindingSeverity,
    FindingStatus,
    HandoffStatus,
    LaneMessageDirection,
    LaneStatus,
    MessageStatus,
    PlanCursorState,
    ReportStatus,
    ReviewKind,
    ReviewMode,
    ReviewScopeSource,
    normalize_model_identity,
    normalize_model_label,
    normalize_reasoning_level,
)
from .runtime import get_runtime_config
from .slice_decision import (
    is_slice_complete_decision,
    validate_decision_id,
)

# ---------------------------------------------------------------------------
# Regex constants
# ---------------------------------------------------------------------------

_FTS5_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_VERIFIED_TEST_RESULT_HINT_RE = re.compile(
    r"\b(pass(?:ed)?|fail(?:ed)?|error(?:s)?|warning(?:s)?|clean|ready|not ready|ok)\b",
    re.IGNORECASE,
)
_VERIFIED_TEST_RESULT_MAX_CHARS = 280
RATIONALE_SOFT_LIMIT_CHARS = 1_500
RATIONALE_HARD_LIMIT_CHARS = 3_000
SLICE_COMPLETE_HARD_LIMIT_CHARS = 4_000
SLICE_COMPLETE_REQUIRED_SECTIONS: tuple[str, ...] = (
    "## Changes",
    "## Verification",
    "## Schema / Contract Changes",
    "## Open Threads",
)

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------

DEFAULT_HANDOFF_LIMITS = {
    "blockers": 5,
    "actions": 5,
    "decisions": 3,
    "slices": 20,
    "tests": 3,
    "findings": 10,
}
HANDOFF_ACTIVE_STATUSES = frozenset(status.value for status in HandoffStatus)
# Subset of HANDOFF_ACTIVE_STATUSES that resolver/renderer surfaces treat as
# "live" for active-task selection. status='done' rows are archive-eligible
# only and never active-eligible — see internal.
LIVE_ACTIVE_STATUSES: tuple[str, ...] = ("in_progress", "review", "blocked")
BLOCKER_STATUSES = frozenset(status.value for status in BlockerStatus)
ACTION_STATUSES = frozenset(status.value for status in ActionStatus)
REVIEW_FINDING_STATUSES = frozenset(status.value for status in FindingStatus)
REVIEW_FINDING_SEVERITIES = frozenset(status.value for status in FindingSeverity)
REVIEW_MODES = frozenset(mode.value for mode in ReviewMode)
REVIEW_KINDS = frozenset(kind.value for kind in ReviewKind)
REVIEW_SCOPE_SOURCES = frozenset(source.value for source in ReviewScopeSource)
LANE_STATUSES = frozenset(status.value for status in LaneStatus)
CLOSEABLE_LANE_STATUSES = frozenset({LaneStatus.MERGED.value, LaneStatus.CLOSED.value})
REPORT_STATUSES = frozenset(status.value for status in ReportStatus)
MESSAGE_STATUSES = frozenset(status.value for status in MessageStatus)
LANE_MESSAGE_DIRECTIONS = frozenset(direction.value for direction in LaneMessageDirection)
PLAN_CURSOR_STATES = frozenset(state.value for state in PlanCursorState)
MANDATORY_SLICE_DECISION_HEADINGS = (
    "## Changes",
    "## Verification",
    "## Schema / Contract Changes",
    "## Open Threads",
)
MAX_RESOLUTION_NOTES_LENGTH = 500
MAX_REOPEN_REASON_LENGTH = 500
MAX_VERIFICATION_EVIDENCE_LENGTH = 2000

# Soft cap on response payload size before the envelope appends an oversize
# warning naming the bounded-read levers (`detail="summary"`, lower `top_n_*`,
# `sections="identity"`). The byte threshold corresponds to roughly 5,000
# tokens at the typical ~4 chars/token ratio, which is the budget level at
# which routine handoff reads should already have been narrowed via the
# bounded-read parameters introduced by internal / internal. The check is not
# a hard cap; it just nudges callers toward the documented narrowing levers
# the same way the internal conftest guard nudges callers toward the
# Makefile test target. internal added the warning after a real
# `get_handoff_state(top_n_decisions=10, detail="full")` call returned
# ~17.6k tokens because the slice-complete decision rationales account for
# the bulk of the payload, and internal / internal wire-format optimizations
# only attack the wrapper, not the rationale text itself.
RESPONSE_OVERSIZE_WARN_BYTES = 8_000
BATCH_CLOSE_WINDOW_SECONDS = 60
BATCH_CLOSE_THRESHOLD = 2
REOPEN_ESCALATION_THRESHOLD = 2
SUBPROCESS_TIMEOUT = 10

# ---------------------------------------------------------------------------
# Typed containers
# ---------------------------------------------------------------------------


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


class ReviewFindingDetails(TypedDict, total=False):
    line_start: int
    line_end: int
    fix: str


class LaneMessagePayload(TypedDict, total=False):
    source_lane: str
    reason: str
    summary: str
    required_actions: list[str]
    artifacts: list[str]


# ---------------------------------------------------------------------------
# Workspace / path utilities
# ---------------------------------------------------------------------------


def _workspace_root() -> Path:
    return get_runtime_config().workspace_root


def _current_task_path() -> Path:
    return get_runtime_config().current_task_path


def _exports_dir() -> Path:
    return get_runtime_config().exports_dir


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------


def _normalize_optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized if normalized != "" else None


def _first_present(values: list[object]) -> object | None:
    for value in values:
        if isinstance(value, str):
            if value.strip() != "":
                return value
            continue
        if value is not None:
            return value
    return None


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_response(payload: Mapping[str, object]) -> dict:
    """Return a tool response as a native dict.

    Historically returned a JSON string serialised by ``json.dumps``;
    internal finished internal's implementation note (dict return at the MCP boundary),
    so every handler now returns a real dict. FastMCP serialises the dict
    once on its way out — there is no longer a `json.dumps -> json.loads`
    round trip and no `structured_content={"result": "<escaped JSON>"}`
    double-encoding on the wire.
    """
    return dict(payload)


def _envelope(
    *,
    ok: bool,
    tool: str,
    data: Mapping[str, object],
    task_ref: str | None = None,
    entity: str | None = None,
    mutation: dict | None = None,
    artifacts: list[dict] | None = None,
    warnings: list[str] | None = None,
) -> dict:
    """Build a v2 response envelope as a native ``dict``.

    The nested ``data`` block is the canonical v2 shape. Callers must
    read payload fields from ``result["data"][...]``, not from the
    envelope root. The legacy top-level mirror that internal introduced
    was removed in the first half of internal. The string-return path
    that internal deferred (implementation note — "Dict Return at MCP Boundary")
    was completed in the second half of internal: this function now
    returns a ``dict`` instead of ``json.dumps(dict)`` and every tool
    handler is annotated ``-> dict``. FastMCP receives the dict
    directly and serialises it once on the wire, eliminating the
    ``structured_content={"result": "<escaped JSON>"}`` double-encoding
    that previously inflated every response by 30-50%.

    ``schema_version`` stays at ``2`` because the envelope fields and
    contract are unchanged — the wire format went from JSON-string to
    JSON-object, but the field set, names, and semantics are identical.
    """
    scope: dict[str, str | None] = {"task_ref": task_ref}
    if entity is not None:
        scope["entity"] = entity
    payload: dict[str, object] = {
        "ok": ok,
        "schema_version": 2,
        "tool": tool,
        "scope": scope,
        "data": dict(data),
    }
    if mutation is not None:
        payload["mutation"] = mutation
    if artifacts:
        payload["artifacts"] = artifacts
    accumulated_warnings: list[str] = list(warnings) if warnings else []
    # Oversize-response advisory: emit a warning when the serialised
    # payload exceeds RESPONSE_OVERSIZE_WARN_BYTES, naming the bounded-read
    # levers callers should adopt. The warning is purely advisory — the
    # response is still returned in full so the caller is not silently
    # truncated. See internal for the motivating incident
    # (`get_handoff_state(top_n_decisions=10, detail="full")` returned
    # ~17.6k tokens against internal because slice-complete decision
    # rationales dominate the payload).
    try:
        approx_bytes = len(json.dumps(payload, default=str))
    except Exception:
        approx_bytes = 0
    if approx_bytes > RESPONSE_OVERSIZE_WARN_BYTES:
        accumulated_warnings.append(
            f"oversize_response: ~{approx_bytes} bytes (~{approx_bytes // 4} tokens) exceeds "
            f"{RESPONSE_OVERSIZE_WARN_BYTES}-byte advisory threshold. internal: prefer "
            f'read_profile="hot_summary"/"review_packet"/"identity" or set '
            f"response_budget_bytes (with budget_policy='auto_summary' or 'fail') so the "
            f"server can plan reductions before materialising heavy rows. Manual levers "
            f'(detail="summary", lower top_n_decisions/top_n_tests/top_n_findings, '
            f'sections="identity", fields=... projections) remain available. See '
            f"packages/mcp-workbay-handoff/docs/guides/token-efficient-usage.md."
        )
    if accumulated_warnings:
        payload["warnings"] = accumulated_warnings
    if task_ref is not None:
        payload["task_ref"] = task_ref
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


# ---------------------------------------------------------------------------
# DB row utilities
# ---------------------------------------------------------------------------


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def _enrich_handoff_active(active: dict | None) -> dict | None:
    """Add resolved task-plan fields to an active handoff row dict.

    Unconditionally populates three derived keys on every active row,
    so the published ActiveTask contract is satisfied even when
    ``task_plan_path`` is unset or empty:

    - ``task_plan_abs_path``: absolute path string when a plan path is
      provided and resolvable, else ``None``.
    - ``task_plan_exists``: whether the absolute path resolves to a
      file. Always ``False`` when no plan path is present.
        - ``task_plan_resolution``: one of
            ``"worktree" | "workspace" | "absolute" | "unresolved" | None``.
            ``"absolute"`` means the stored path was already absolute;
            ``"worktree"`` means it was resolved against
            ``target_worktree_path``; ``"workspace"`` means it was resolved
            against the workspace root fallback; ``"unresolved"`` means a
            relative plan path was present but workspace-root resolution
            failed; ``None`` means no plan path was set.

    Returns the same dict mutated in place (and the dict itself), or
    ``None`` if ``active`` is ``None``.

    Boundary validation: after enrichment, the dict is round-tripped
    through ``workbay_protocol.ActiveTask`` to detect drift between the
    handoff DB shape and the published cross-repo contract. Validation
    failures are logged but do not raise — the DB row is still returned
    so a contract regression cannot brick a running server. Failures
    will surface in the contract test suite.
    """
    if active is None:
        return None
    active["task_plan_abs_path"] = None
    active["task_plan_exists"] = False
    active["task_plan_resolution"] = None
    plan_path = active.get("task_plan_path") if isinstance(active, dict) else None
    if not plan_path or not isinstance(plan_path, str):
        _validate_active_against_protocol(active)
        return active
    plan_path = plan_path.strip()
    if not plan_path:
        _validate_active_against_protocol(active)
        return active
    candidate = Path(plan_path)
    abs_path: Path | None
    resolution: str
    if candidate.is_absolute():
        abs_path = candidate
        resolution = "absolute"
    else:
        worktree = active.get("target_worktree_path") if isinstance(active, dict) else None
        if isinstance(worktree, str) and worktree.strip():
            abs_path = Path(worktree.strip()) / plan_path
            resolution = "worktree"
        else:
            try:
                abs_path = Path(_workspace_root()) / plan_path
                resolution = "workspace"
            except Exception:
                abs_path = None
                resolution = "unresolved"
    active["task_plan_abs_path"] = str(abs_path) if abs_path is not None else None
    active["task_plan_exists"] = bool(abs_path is not None and abs_path.is_file())
    active["task_plan_resolution"] = resolution
    _validate_active_against_protocol(active)
    return active


_protocol_warning_logged = False


def _validate_active_against_protocol(active: dict) -> None:
    """Round-trip ``active`` through workbay_protocol.ActiveTask.

    Imported lazily so the package remains importable in environments
    where ``workbay-protocol`` has not yet been installed (e.g. partial
    rollouts during the founding-plan migration).
    """
    global _protocol_warning_logged
    try:
        from workbay_protocol import ActiveTask
    except ImportError:
        if not _protocol_warning_logged:
            import logging

            logging.getLogger(__name__).debug("workbay-protocol not installed; skipping handoff contract validation.")
            _protocol_warning_logged = True
        return
    try:
        ActiveTask.model_validate(active)
    except Exception as exc:  # noqa: BLE001
        import logging

        logging.getLogger(__name__).warning("handoff active row failed workbay_protocol.ActiveTask validation: %s", exc)


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if isinstance(item, str):
            normalized = item.strip()
            if normalized:
                result.append(normalized)
    return result


def _get_handoff_row_for_task(conn: sqlite3.Connection, task_ref: str) -> sqlite3.Row | None:
    return cast(
        sqlite3.Row | None,
        conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone(),
    )


def _is_onmain_maint_row(row: sqlite3.Row) -> bool:
    from .import_export import _is_integration_target_branch  # noqa: PLC0415

    task_ref = str(row["task_ref"])
    if not task_ref.startswith("MAINT-"):
        return False
    return _is_integration_target_branch(row["target_branch"])


def _pick_onmain_maint_tiebreak(rows: list[sqlite3.Row]) -> sqlite3.Row:
    return max(
        rows,
        key=lambda row: (
            str(row["updated_at"] or ""),
            str(row["task_ref"] or ""),
        ),
    )


def _resolve_demoted_contention(rows: list[sqlite3.Row]) -> sqlite3.Row | None:
    """Demote on-main MAINT rows from ambiguity contention without hiding them."""
    contention_rows = [row for row in rows if not _is_onmain_maint_row(row)]
    onmain_rows = [row for row in rows if _is_onmain_maint_row(row)]
    if len(contention_rows) == 1:
        return contention_rows[0]
    if len(contention_rows) > 1:
        return None
    if len(onmain_rows) == 1:
        return onmain_rows[0]
    if len(onmain_rows) > 1:
        return _pick_onmain_maint_tiebreak(onmain_rows)
    return None


def _resolve_workspace_handoff_row(
    conn: sqlite3.Connection,
    task_ref: str | None = None,
) -> sqlite3.Row | None:
    # An explicit task_ref is the operator's disambiguation: honor it
    # before workspace ambiguity, returning the named row or None (never
    # _raise_ambiguous, which fires only on the no-task_ref path below).
    if task_ref:
        return _get_handoff_row_for_task(conn, task_ref)

    placeholders = ",".join(["?"] * len(LIVE_ACTIVE_STATUSES))
    rows = conn.execute(
        f"SELECT * FROM handoff_state WHERE status IN ({placeholders}) ORDER BY updated_at DESC, task_ref ASC",
        LIVE_ACTIVE_STATUSES,
    ).fetchall()
    if not rows:
        return None
    if len(rows) == 1:
        return cast(sqlite3.Row, rows[0])

    # Tier candidates so cwd resolves before the primary-worktree fallback.
    # Why: RuntimeConfig.for_repo collapses every linked worktree to the
    # primary root, so _workspace_root() is the repo root even when the
    # caller is inside a feature worktree. Without tiering, a MAINT row
    # pinned to the repo root and a feature row pinned to the linked
    # worktree both produce exact matches and ambiguity is raised — even
    # though the cwd unambiguously identifies the feature row.
    candidate_tiers: list[str] = []
    for raw_candidate in (os.getcwd(), str(_workspace_root())):
        try:
            normalized = _normalize_path_for_match(raw_candidate)
        except (FileNotFoundError, OSError, RuntimeError):
            continue
        if normalized not in candidate_tiers:
            candidate_tiers.append(normalized)

    normalized_targets: list[tuple[sqlite3.Row, str]] = []
    for row in rows:
        raw_target = _normalize_optional_text(row["target_worktree_path"])
        if raw_target is None:
            continue
        try:
            normalized_target = _normalize_path_for_match(raw_target)
        except (FileNotFoundError, OSError, RuntimeError):
            continue
        normalized_targets.append((cast(sqlite3.Row, row), normalized_target))

    for candidate in candidate_tiers:
        exact_matches: list[sqlite3.Row] = []
        prefix_matches: list[sqlite3.Row] = []
        for row, normalized_target in normalized_targets:
            if candidate == normalized_target:
                exact_matches.append(row)
            elif candidate.startswith(normalized_target + os.sep):
                prefix_matches.append(row)

        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            demoted = _resolve_demoted_contention(exact_matches)
            if demoted is not None:
                return demoted
            _raise_ambiguous(conn, exact_matches, reason="multiple target_worktree_path exact matches")
        if len(prefix_matches) == 1:
            return prefix_matches[0]
        if len(prefix_matches) > 1:
            demoted = _resolve_demoted_contention(prefix_matches)
            if demoted is not None:
                return demoted
            _raise_ambiguous(conn, prefix_matches, reason="multiple target_worktree_path prefix matches")

    # internal: no sentinel bootstrap fallback. The former
    # branch returned the id=1 row whenever no target_worktree_path
    # registrations existed; that path violated the canonical
    # unresolved-context rule and is removed.
    # internal: raise the structured AmbiguousWorkspaceContextError
    # so read paths can surface candidates to the caller instead of
    # emitting an opaque string.
    demoted = _resolve_demoted_contention(list(rows))
    if demoted is not None:
        return demoted
    _raise_ambiguous(
        conn,
        list(rows),
        reason="no target_worktree_path match",
        hint="Pass task_ref explicitly or run from a registered target_worktree_path.",
    )


WORKBAY_HANDOFF_ACTIVE_TASK_ENV = "WORKBAY_HANDOFF_ACTIVE_TASK"


@dataclass(frozen=True, slots=True)
class ActiveTaskResolution:
    task_ref: str
    tiebreak_note: str | None = None


def _pick_tiebreak_task_ref(candidates: list[dict[str, object]]) -> str:
    chosen = max(
        candidates,
        key=lambda candidate: (
            str(candidate.get("updated_at") or ""),
            str(candidate.get("task_ref") or ""),
        ),
    )
    return str(chosen["task_ref"])


def _format_tiebreak_note(chosen_ref: str, candidates: list[dict[str, object]]) -> str:
    refs = ", ".join(sorted(str(candidate.get("task_ref") or "") for candidate in candidates))
    return (
        f"ambiguous active task: chose {chosen_ref} (most recent); "
        f"set WORKBAY_HANDOFF_ACTIVE_TASK to pin; candidates: {refs}"
    )


def _resolve_pinned_active_task(conn: sqlite3.Connection, pinned: str) -> str:
    row = _get_handoff_row_for_task(conn, pinned)
    if row is None:
        raise ValueError(f"WORKBAY_HANDOFF_ACTIVE_TASK={pinned!r} does not match any handoff_state row")
    status = str(row["status"])
    if status not in LIVE_ACTIVE_STATUSES:
        raise ValueError(
            f"WORKBAY_HANDOFF_ACTIVE_TASK={pinned!r} is not a live active task "
            f"(status={status!r}; expected one of in_progress, review, blocked)"
        )
    return pinned


def _raise_ambiguous(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    *,
    reason: str,
    hint: str | None = None,
) -> NoReturn:
    from .import_export import _classify_live_row  # noqa: PLC0415
    from .shared_write_context import AmbiguousWorkspaceContextError  # noqa: PLC0415

    # This raise fires on the hot fail-fast read path (get_handoff_state /
    # load_session / `make context`). Each _classify_live_row spawns up to
    # three git subprocesses, so classify only a bounded prefix of the
    # candidates — enough to give the operator an actionable hint without
    # turning an "ambiguous" error into a multi-second subprocess fan-out when
    # many live rows have accumulated.
    _CLASSIFY_CAP = 12
    prefix = (
        "Ambiguous active task"
        if reason == "no target_worktree_path match"
        else "Ambiguous active task for workspace path"
    )
    candidates: list[dict[str, object]] = []
    classifications: list[str] = []
    closeable_refs: list[str] = []
    for idx, row in enumerate(rows):
        ref = str(row["task_ref"])
        candidate: dict[str, object] = {
            "task_ref": ref,
            "target_branch": _normalize_optional_text(row["target_branch"]),
            "target_worktree_path": _normalize_optional_text(row["target_worktree_path"]),
            "objective": _normalize_optional_text(row["objective"]),
            "status": _normalize_optional_text(row["status"]),
            "updated_at": _normalize_optional_text(row["updated_at"]),
        }
        if idx < _CLASSIFY_CAP:
            bucket, bucket_reason = _classify_live_row(conn, row)
            candidate["reaper_bucket"] = bucket
            candidate["reaper_reason"] = bucket_reason
            classifications.append(f"{ref}={bucket}")
            if bucket == "closeable":
                closeable_refs.append(ref)
        candidates.append(candidate)
    task_refs = ", ".join(sorted(str(c["task_ref"]) for c in candidates))
    parts = [f"{prefix}."]
    if hint:
        parts.append(hint)
    parts.append(f"Known task_refs: {task_refs}")
    parts.append(
        "Recovery: run `make task-reap` (dry-run), then `make task-reap REAP_ARGS=--apply` "
        "to close closeable rows only."
    )
    if classifications:
        parts.append(f"Reaper classification: {', '.join(classifications)}.")
    if closeable_refs:
        parts.append(f"Closeable now: {', '.join(sorted(closeable_refs))}.")
    from .import_export import _is_integration_target_branch  # noqa: PLC0415

    onmain_maint_refs = sorted(
        str(candidate["task_ref"])
        for candidate in candidates
        if str(candidate.get("task_ref") or "").startswith("MAINT-")
        and _is_integration_target_branch(_normalize_optional_text(candidate.get("target_branch")))
    )
    if onmain_maint_refs:
        parts.append(
            "On-main MAINT passes close explicitly with "
            + ", ".join(f"`make plan-done TASK={ref}`" for ref in onmain_maint_refs)
            + "."
        )
    raise AmbiguousWorkspaceContextError(" ".join(parts), candidates=candidates)


def resolve_active_task_ref_ex(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
    allow_tiebreak: bool = False,
) -> ActiveTaskResolution:
    """Four-step Resolution Rule entry point with optional hook tiebreak."""
    if task_ref:
        return ActiveTaskResolution(task_ref=task_ref)

    lane_bound = _resolve_lane_bound_task_ref(conn)
    if lane_bound is not None:
        return ActiveTaskResolution(task_ref=lane_bound)

    pinned = os.environ.get(WORKBAY_HANDOFF_ACTIVE_TASK_ENV, "").strip()
    if pinned and allow_tiebreak:
        return ActiveTaskResolution(task_ref=_resolve_pinned_active_task(conn, pinned))

    from .shared_write_context import AmbiguousWorkspaceContextError, UnresolvedTaskContextError

    try:
        row = _resolve_workspace_handoff_row(conn)
    except AmbiguousWorkspaceContextError as exc:
        if allow_tiebreak and exc.candidates:
            chosen_ref = _pick_tiebreak_task_ref(exc.candidates)
            return ActiveTaskResolution(
                task_ref=chosen_ref,
                tiebreak_note=_format_tiebreak_note(chosen_ref, exc.candidates),
            )
        raise
    except UnresolvedTaskContextError:
        raise
    except ValueError as exc:
        raise UnresolvedTaskContextError(str(exc)) from exc
    if row is None:
        raise ValueError("No active task in handoff_state. Call set_handoff_state first or pass task_ref explicitly.")
    return ActiveTaskResolution(task_ref=str(row["task_ref"]))


def resolve_active_task_ref_for_hook(conn: sqlite3.Connection) -> ActiveTaskResolution:
    """Hook-scoped resolver: tiebreak on ambiguity, never silent on writes."""
    return resolve_active_task_ref_ex(conn, allow_tiebreak=True)


def resolve_active_task_ref(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
    allow_tiebreak: bool = False,
) -> str:
    """Four-step Resolution Rule entry point (internal).

    The four-step Resolution Rule is the shared spec implemented on
    both the server (here) and the client-side lifecycle CLI:

        1. Explicit ``task_ref`` argument.
        2. ``WORKBAY_LANE_ID`` env var binding (sub-implementation note.2).
        2.5. ``WORKBAY_HANDOFF_ACTIVE_TASK`` env pin when set.
        3. Unique active task for the canonical workspace root.
        4. Multiple tasks share the canonical workspace root → raise
           the structured ``AmbiguousWorkspaceContextError`` (no
           "last writer wins" fallback), unless ``allow_tiebreak`` is
           True for hook callers.

    Step 2 wires ``WORKBAY_LANE_ID`` into
    ``_resolve_lane_bound_task_ref``.
    """
    return resolve_active_task_ref_ex(
        conn,
        task_ref=task_ref,
        allow_tiebreak=allow_tiebreak,
    ).task_ref


WORKBAY_LANE_ID_ENV = "WORKBAY_LANE_ID"


def _resolve_lane_bound_task_ref(conn: sqlite3.Connection) -> str | None:
    """Resolution Rule step 2 — ``WORKBAY_LANE_ID`` env var binding.

    Returns the ``task_ref`` of the matching ``worktree_lanes`` row
    when:

            - ``WORKBAY_LANE_ID`` is set in the environment to a non-empty
                value. Empty string is treated as unset; subprocess env propagation
                often passes empty strings rather than omitting keys.
      - Exactly one ``worktree_lanes`` row matches that ``lane_id``.

    Returns ``None`` otherwise so the four-step rule falls through to
    step 3 (workspace-root resolution). A stale env var or unknown
    lane id must NOT hard-fail — the workspace-root resolver may
    still find the right answer, and the step-4 ambiguity error is
    where genuine non-resolution surfaces.

    When multiple lanes share the env-bound ``lane_id`` (rare; the
    schema's ``UNIQUE(task_ref, lane_id)`` constraint allows the
    same ``lane_id`` across different ``task_ref`` rows), the most
    recently updated one wins. This matches the orchestrator's "the
    spawned worker is bound to its current lane" intent.
    """
    from workbay_protocol import resolve_env_alias

    raw = resolve_env_alias(WORKBAY_LANE_ID_ENV)
    if raw is None:
        return None
    lane_id = raw.strip()
    if not lane_id:
        return None
    # internal: require a LIVE_ACTIVE_STATUSES handoff_state row for
    # the lane's task_ref. archive_task_state leaves worktree_lanes rows
    # in place unless prune_working_rows=true, and task-finish only warns
    # about open lanes — so without this join an env-bound worker can
    # short-circuit implementation note onto an archived or finished task.
    placeholders = ",".join(["?"] * len(LIVE_ACTIVE_STATUSES))
    row = conn.execute(
        f"SELECT wl.task_ref FROM worktree_lanes wl "
        f"INNER JOIN handoff_state hs ON hs.task_ref = wl.task_ref "
        f"WHERE wl.lane_id = ? "
        f"AND COALESCE(wl.status, '') NOT IN ('closed', 'archived') "
        f"AND hs.status IN ({placeholders}) "
        f"ORDER BY wl.updated_at DESC, wl.id DESC LIMIT 1",
        (lane_id, *LIVE_ACTIVE_STATUSES),
    ).fetchone()
    if row is None:
        return None
    task_ref = row["task_ref"]
    return str(task_ref) if task_ref else None


def _resolve_task_ref(conn: sqlite3.Connection, task_ref: str | None) -> str:
    """Backward-compatible alias for ``resolve_active_task_ref``.

    Existing call sites in ``decisions.py``, ``review_findings_queries.py``,
    ``touched_files.py``, etc. pass ``task_ref`` positionally. Routing
    through the new entry point preserves their semantics while letting
    new callers use the canonical four-step name.
    """
    return resolve_active_task_ref(conn, task_ref=task_ref)


def _decode_lane_message_row_dict(row: dict) -> dict:
    payload_json = row.get("payload_json")
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            row["payload"] = payload
    return row


def _decode_turn_metric_row_dict(row: dict) -> dict:
    for key, empty in (
        ("attribution_json", {}),
        ("section_sizes_json", {}),
        ("raw_usage_json", None),
    ):
        raw_value = row.get(key)
        if not isinstance(raw_value, str) or not raw_value.strip():
            row[key.removesuffix("_json")] = empty
            continue
        try:
            row[key.removesuffix("_json")] = json.loads(raw_value)
        except json.JSONDecodeError:
            row[key.removesuffix("_json")] = empty
    return row


# ---------------------------------------------------------------------------
# Datetime utilities
# ---------------------------------------------------------------------------


def _parse_sqlite_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if normalized == "":
        return None
    try:
        return datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except ValueError:
        try:
            return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _normalize_review_mode(value: object) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    if normalized not in REVIEW_MODES:
        raise ValueError(f"Invalid review_mode. Valid: {', '.join(sorted(REVIEW_MODES))}")
    return normalized


def _normalize_lane_message_payload(payload: object) -> tuple[dict[str, object] | None, str | None]:
    if payload is None:
        return None, None
    if not isinstance(payload, dict):
        return None, "lane message payload must be an object when provided."
    normalized: dict[str, object] = {}
    source_lane = _normalize_optional_text(payload.get("source_lane"))
    if source_lane is not None:
        normalized["source_lane"] = source_lane
    reason = _normalize_optional_text(payload.get("reason"))
    if reason is not None:
        normalized["reason"] = reason
    summary = _normalize_optional_text(payload.get("summary"))
    if summary is not None:
        normalized["summary"] = summary
    required_actions = _coerce_string_list(payload.get("required_actions"))
    if required_actions:
        normalized["required_actions"] = required_actions
    artifacts = _coerce_string_list(payload.get("artifacts"))
    if artifacts:
        normalized["artifacts"] = artifacts
    _raw_override = payload.get("owned_paths_override")
    if isinstance(_raw_override, str):
        _raw_override = [_raw_override]
    owned_paths_override = _coerce_string_list(_raw_override)
    if owned_paths_override:
        normalized["owned_paths_override"] = owned_paths_override
    return normalized, None


def _normalize_path_for_match(path_value: str | Path) -> str:
    return os.path.normcase(str(Path(path_value).expanduser().resolve()))


def _resolve_current_lane_row(conn: sqlite3.Connection, task_ref: str) -> sqlite3.Row | None:
    workspace_path = _normalize_path_for_match(_workspace_root())
    lane_rows = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? ORDER BY updated_at DESC, id DESC",
        (task_ref,),
    ).fetchall()
    for row in lane_rows:
        raw_path = _normalize_optional_text(row["worktree_path"])
        if raw_path is None:
            continue
        if _normalize_path_for_match(raw_path) == workspace_path:
            return cast(sqlite3.Row, row)
    return None


# ---------------------------------------------------------------------------
# Test-result utilities
# ---------------------------------------------------------------------------


def _summarize_test_result(result: str | None) -> str | None:
    normalized = _normalize_optional_text(result)
    if normalized is None:
        return None
    lines = [re.sub(r"\s+", " ", line).strip() for line in normalized.splitlines() if line.strip()]
    if not lines:
        return None
    summary = next((line for line in reversed(lines) if _VERIFIED_TEST_RESULT_HINT_RE.search(line)), lines[-1])
    if len(summary) <= _VERIFIED_TEST_RESULT_MAX_CHARS:
        return summary
    return summary[: _VERIFIED_TEST_RESULT_MAX_CHARS - 3].rstrip() + "..."


# ---------------------------------------------------------------------------
# Decision / slice utilities
# ---------------------------------------------------------------------------


def _has_structured_slice_summary(text: str) -> bool:
    normalized = _normalize_optional_text(text)
    if normalized is None:
        return False
    section_content: dict[str, list[str]] = {heading: [] for heading in MANDATORY_SLICE_DECISION_HEADINGS}
    current_heading: str | None = None
    for raw_line in normalized.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line in section_content:
            current_heading = line
            continue
        if current_heading is not None:
            section_content[current_heading].append(line)
    return all(section_content[heading] for heading in MANDATORY_SLICE_DECISION_HEADINGS)


def _validate_decision_payload(decision: str, rationale: str | None) -> str | None:
    decision_validation = validate_decision_id(decision)
    if not bool(decision_validation.get("ok")):
        return str(decision_validation.get("error"))
    rationale_size_error = _validate_decision_rationale_size(decision, rationale)
    if rationale_size_error is not None:
        return rationale_size_error
    if is_slice_complete_decision(decision) and not _has_structured_slice_summary(str(rationale or "")):
        headings = ", ".join(MANDATORY_SLICE_DECISION_HEADINGS)
        return f"slice_complete_* decisions require a structured rationale with non-empty sections for: {headings}."
    return None


def _decision_rationale_hard_limit(decision: str) -> int:
    return SLICE_COMPLETE_HARD_LIMIT_CHARS if is_slice_complete_decision(decision) else RATIONALE_HARD_LIMIT_CHARS


def _validate_decision_rationale_size(decision: str, rationale: str | None) -> str | None:
    normalized = _normalize_optional_text(rationale)
    if normalized is None:
        return None
    char_count = len(normalized)
    hard_limit = _decision_rationale_hard_limit(decision)
    if char_count <= hard_limit:
        return None
    kind_label = "Slice-complete" if is_slice_complete_decision(decision) else "Decision"
    return (
        f"{kind_label} rationale is {char_count:,} chars, which exceeds the {hard_limit:,}-char limit. "
        f"Trim to the decision and key reason. Move verbose evidence into verification summaries, "
        f"artifacts, or changed_files metadata."
    )


def _decision_rationale_size_warning(decision: str, rationale: str | None) -> str | None:
    normalized = _normalize_optional_text(rationale)
    if normalized is None:
        return None
    char_count = len(normalized)
    if char_count <= RATIONALE_SOFT_LIMIT_CHARS:
        return None
    hard_limit = _decision_rationale_hard_limit(decision)
    kind_label = "Slice-complete" if is_slice_complete_decision(decision) else "Decision"
    return (
        f"{kind_label} rationale is {char_count:,} chars. Prefer staying under "
        f"{RATIONALE_SOFT_LIMIT_CHARS:,} chars for context efficiency; hard limit is {hard_limit:,} chars."
    )


# ---------------------------------------------------------------------------
# Review finding helpers
# ---------------------------------------------------------------------------


def _parse_review_finding_details(details: ReviewFindingDetails | None) -> tuple[int | None, int | None, str | None]:
    if not details:
        return None, None, None
    return details.get("line_start"), details.get("line_end"), details.get("fix")


# ---------------------------------------------------------------------------
# Import helpers
# ---------------------------------------------------------------------------


def _resolve_import_row_actor(
    row: dict,
    *,
    fallback_agent: str,
    fallback_branch: str,
    fallback_commit: str | None,
) -> tuple[str, str, str | None, str | None, str | None, str | None]:
    model = _normalize_optional_text(row.get("model"))
    model_label = _normalize_optional_text(row.get("model_label")) or normalize_model_label(model)
    reasoning_level = normalize_reasoning_level(row.get("reasoning_level"))
    derived_agent = normalize_model_identity(model_label, reasoning_level)
    return (
        derived_agent or _normalize_optional_text(row.get("agent")) or fallback_agent,
        _normalize_optional_text(row.get("branch")) or fallback_branch,
        _normalize_optional_text(row.get("commit_sha")) or fallback_commit,
        model,
        model_label,
        reasoning_level,
    )


def _resolve_import_lane_id(row: dict) -> str | None:
    return _normalize_optional_text(row.get("lane_id"))
