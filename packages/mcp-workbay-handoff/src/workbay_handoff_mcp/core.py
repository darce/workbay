"""Core handoff module — thin re-export layer.

Domain logic lives in focused submodules. This file keeps: plan cursor
functions, FTS search + search_handoff, compound tools (load_session,
close_slice), artifact tools, deprecated aliases, and re-exports needed
by api.py / __init__.py / tests.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from . import artifact_index as artifact_index
from . import read_budget as read_budget_module
from . import read_profiles as read_profiles_module
from .compaction import build_context_refresh_packet
from .current_task_rendering import (  # noqa: F401
    _build_current_task_state_from_snapshot,
    _collect_all_deferred_findings,
    _collect_all_open_findings,
    _collect_dashboard_rows,
    _collect_task_snapshot,
    _fetch_related_open_findings,
    _render_current_task_json,
    _render_current_task_md,
    _write_workspace_current_task_json,
    _write_current_task_md_from_state,
)
from .decisions import (  # noqa: F401
    _collect_task_provenance_integrity,
    _normalize_changed_files_payload,
    _record_decision_with_conn,
    audit_decision_ids,
    handoff_close_check,
    list_next_actions,
    record_decision,
    record_test_result,
    report_blocker,
    update_next_actions,
)

# Re-exports: domain modules (all accessed via api.py as core.X)
from .handoff_state import _set_handoff_state_with_conn, get_handoff_state, set_handoff_state  # noqa: F401
from .import_export import (  # noqa: F401
    _import_snapshot,
    _set_import_active_state,
    archive_task_state,
    archives_retention_gc,
    classify_live_tasks,
    export_handoff_state,
    get_archived_task,
    import_handoff_state,
    reap_done_nonscratch_handoff_rows,
    reap_scratch_handoff_rows,
    reap_stale_plan_cursors,
    reap_tasks,
    switch_task,
    tasks_gc,
    update_task_status,
)
from .lanes_api import close_lane, get_lane, list_lanes, open_lane, update_lane  # noqa: F401
from .lanes_recording import latest_lane_landing, latest_reclaim_candidate  # noqa: F401
from .orientation_reads import record_orientation_read_once, suppress_orientation_read_boundary
from .review_findings import (  # noqa: F401
    _collect_review_findings_integrity,
    apply_stale_findings_gc,
    batch_record_review_findings,
    collect_stale_nonscratch_open_findings,
    get_review_coverage,
    get_review_findings_summary,
    list_review_findings,
    list_review_runs,
    reconcile_review_findings,
    reconcile_reviewer_scratch_findings_gc,
    record_review_finding,
    record_review_run,
    repair_review_finding_provenance,
    resolve_review_findings,
    update_review_finding,
)
from .review_findings_updates import (  # noqa: F401
    _check_batch_close_guard,
    _check_commit_relation_guard,
    _check_reopen_escalation_guard,
)
from .runtime import get_runtime_config
from .shared_db_utils import _count_task_rows, _fetch_handoff_rows, _paginated_query  # noqa: F401
from .shared_primitives import (  # noqa: F401
    ACTION_STATUSES,
    BATCH_CLOSE_THRESHOLD,
    BATCH_CLOSE_WINDOW_SECONDS,
    BLOCKER_STATUSES,
    CLOSEABLE_LANE_STATUSES,
    DEFAULT_HANDOFF_LIMITS,
    HANDOFF_ACTIVE_STATUSES,
    LANE_MESSAGE_DIRECTIONS,
    LANE_STATUSES,
    MANDATORY_SLICE_DECISION_HEADINGS,
    MAX_REOPEN_REASON_LENGTH,
    MAX_RESOLUTION_NOTES_LENGTH,
    MAX_VERIFICATION_EVIDENCE_LENGTH,
    MESSAGE_STATUSES,
    PLAN_CURSOR_STATES,
    REOPEN_ESCALATION_THRESHOLD,
    REPORT_STATUSES,
    REVIEW_FINDING_SEVERITIES,
    REVIEW_FINDING_STATUSES,
    REVIEW_KINDS,
    REVIEW_MODES,
    REVIEW_SCOPE_SOURCES,
    LaneMessagePayload,
    PromptMetrics,
    ReviewFindingDetails,
    TokenUsage,
    _envelope,
    _json_response,
    _normalize_lane_message_payload,
    _normalize_optional_text,
    _resolve_task_ref,
    _resolve_workspace_handoff_row,
    _row_to_dict,
    _summarize_test_result,
    _validate_decision_payload,
    _workspace_root,
)
from .shared_schema import (  # noqa: F401
    HANDOFF_FTS_SCHEMA_SQL,
    HANDOFF_SCHEMA_SQL,
    _get_db_connection,
)
from .shared_tool_adapters import _invoke_tool  # noqa: F401
from .shared_write_context import (  # noqa: F401
    ResolvedWriteContext,
    WriteActor,
    WorktreeNotFoundError,
    _canonical_worktree_for_task,
    _classify_commit_relation,
    _detect_git_write_context,
    _detect_git_write_context_at,
    _resolve_write_actor,
    _worktree_derivation_enabled,
    _workspace_git_context,
    build_write_actor,
)
from .slice_decision import (  # noqa: F401
    classify_decision_id,
    extract_slice_label,
    is_canonical_decision,
    is_slice_complete_decision,
)
from .touched_files import DEFAULT_TOUCHED_FILES_LIMIT, ChangeKind, get_touched_files, record_file_touch  # noqa: F401
from .verified_tests import get_verified_tests  # noqa: F401
from .working_tree import (  # noqa: F401
    _check_working_tree_integrity,
    post_merge_integrity_check,
    working_tree_integrity_check,
)

# FTS search constants and search_handoff
_FTS5_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")

_SLICE_NUMBER_MISSING_WARNING = (
    "slice_number not provided; plan-checklist sync will fall back to slug parsing for this decision"
)

_VALID_RECORD_TYPES: frozenset[str] = frozenset({"decision", "finding", "blocker", "action", "verified_test"})

_RECORD_TYPE_FTS_MAP: dict[str, tuple[str, bool]] = {
    "decision": ("decisions_fts", False),
    "finding": ("findings_fts", True),
    "blocker": ("blockers_fts", True),
    "action": ("actions_fts", True),
    "verified_test": ("verified_tests_fts", False),
}

_VALID_DETAIL_LEVELS: frozenset[str] = frozenset({"full", "summary"})
_ARTIFACT_TEXT_SUMMARY_TRUNCATE = 200
_ARTIFACT_CHUNK_TITLE_SUMMARY_TRUNCATE = 120
_ARTIFACT_CHUNK_SUMMARY_LIMIT = 3
_HANDOFF_SEARCH_SUMMARY_TRUNCATE = 80

_VALID_ARTIFACT_HIT_FIELDS: frozenset[str] = frozenset(
    {
        "source_id",
        "source_label",
        "source_summary",
        "task_ref",
        "lane_id",
        "app_root",
        "source_kind",
        "content_type",
        "title",
        "snippet",
        "rank",
    }
)
_VALID_ARTIFACT_SOURCE_LIST_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "task_ref",
        "lane_id",
        "app_root",
        "source_kind",
        "source_label",
        "content_type",
        "content_hash",
        "metadata_json",
        "summary",
        "created_at",
        "updated_at",
    }
)
_VALID_ARTIFACT_GET_FIELDS: frozenset[str] = _VALID_ARTIFACT_SOURCE_LIST_FIELDS | frozenset(
    {"metadata", "chunk_count", "chunks"}
)
_VALID_HANDOFF_SEARCH_FIELDS: frozenset[str] = frozenset(
    {"record_type", "record_id", "task_ref", "lane_id", "status", "snippet"}
)
_VALID_DECISION_PROJECTION_FIELDS: frozenset[str] = frozenset(
    {
        "decision",
        "rationale",
        "branch",
        "commit_sha",
        "lane_id",
        "created_at",
        "agent",
        "model",
        "model_label",
        "reasoning_level",
        "changed_files_json",
        "slice_number",
    }
)

_ARTIFACT_HIT_IDENTITY_FIELDS: frozenset[str] = frozenset({"source_id", "source_label", "title", "snippet"})
_ARTIFACT_SOURCE_IDENTITY_FIELDS: frozenset[str] = frozenset(
    {"id", "task_ref", "source_label", "source_kind", "content_type"}
)
_ARTIFACT_GET_IDENTITY_FIELDS: frozenset[str] = frozenset(
    {"id", "task_ref", "source_label", "source_kind", "content_type", "chunk_count"}
)
_HANDOFF_SEARCH_IDENTITY_FIELDS: frozenset[str] = frozenset({"record_type", "record_id", "task_ref", "snippet"})


def _normalize_detail(detail: str) -> str:
    return detail if detail in _VALID_DETAIL_LEVELS else "full"


def _parse_projection_fields(fields: str | None, valid_fields: frozenset[str]) -> frozenset[str] | None:
    if fields is None:
        return None
    requested = frozenset(part.strip().lower() for part in fields.split(",") if part.strip())
    return requested & valid_fields


def _truncate_text(value: object, limit: int) -> object:
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "..."
    return value


def _project_mapping(
    mapping: dict[str, object],
    requested_fields: frozenset[str] | None,
    identity_fields: frozenset[str],
) -> dict[str, object]:
    if requested_fields is None:
        allowed_fields: frozenset[str] | None = None
    else:
        allowed_fields = requested_fields or identity_fields
    return {key: value for key, value in mapping.items() if allowed_fields is None or key in allowed_fields}


def _summarize_artifact_hit(hit: dict[str, object]) -> dict[str, object]:
    summarized = dict(hit)
    summarized["source_summary"] = _truncate_text(summarized.get("source_summary"), _ARTIFACT_TEXT_SUMMARY_TRUNCATE)
    summarized["snippet"] = _truncate_text(summarized.get("snippet"), _ARTIFACT_TEXT_SUMMARY_TRUNCATE)
    return summarized


def _summarize_artifact_source(source: dict[str, object]) -> dict[str, object]:
    summarized = dict(source)
    summarized["summary"] = _truncate_text(summarized.get("summary"), _ARTIFACT_TEXT_SUMMARY_TRUNCATE)
    summarized["metadata_json"] = _truncate_text(summarized.get("metadata_json"), _ARTIFACT_TEXT_SUMMARY_TRUNCATE)
    chunks = summarized.get("chunks")
    if isinstance(chunks, list):
        chunk_preview: list[object] = []
        for chunk in chunks[:_ARTIFACT_CHUNK_SUMMARY_LIMIT]:
            if isinstance(chunk, dict):
                summarized_chunk = dict(chunk)
                summarized_chunk["title"] = _truncate_text(
                    summarized_chunk.get("title"),
                    _ARTIFACT_CHUNK_TITLE_SUMMARY_TRUNCATE,
                )
                summarized_chunk["body"] = _truncate_text(
                    summarized_chunk.get("body"),
                    _ARTIFACT_TEXT_SUMMARY_TRUNCATE,
                )
                chunk_preview.append(summarized_chunk)
            else:
                chunk_preview.append(chunk)
        summarized["chunks"] = chunk_preview
    return summarized


def search_handoff(
    queries: list[str] | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    record_types: list[str] | None = None,
    limit: int = 20,
    detail: str = "full",
    fields: str | None = None,
    decision_fields: list[str] | None = None,
    include_system: bool = False,
) -> dict:
    """Search canonical handoff records by keyword with optional scope filters.

    `decision_fields` is a decision-scoped projection: when supplied, the named
    columns from the `decisions` table are merged onto result rows whose
    `record_type == "decision"`. Non-decision rows retain the global projection
    unchanged. Allowed values: see `_VALID_DECISION_PROJECTION_FIELDS`.

    `include_system` (default False) controls whether decision rows with
    ``decision_origin='system'`` appear in results. Filtering is applied in the
    FTS SQL before LIMIT so result pages still fill when system rows dominate
    rank. NULL origins are treated as agent (remain searchable by default).
    Non-decision record types ignore this flag.
    """
    if not queries:
        return _envelope(
            ok=False, tool="search_handoff", data={"error": "queries must be a non-empty list of search terms."}
        )
    detail = _normalize_detail(detail)
    requested_fields = _parse_projection_fields(fields, _VALID_HANDOFF_SEARCH_FIELDS)

    decision_projection_fields: list[str] | None = None
    if decision_fields is not None:
        invalid_decision_fields = sorted({f for f in decision_fields if f not in _VALID_DECISION_PROJECTION_FIELDS})
        if invalid_decision_fields:
            return _envelope(
                ok=False,
                tool="search_handoff",
                data={
                    "error": (
                        f"Invalid decision_fields: {invalid_decision_fields}. "
                        f"Valid: {sorted(_VALID_DECISION_PROJECTION_FIELDS)}."
                    )
                },
            )
        decision_projection_fields = list(dict.fromkeys(decision_fields))

    validated_types: list[str]
    if record_types is None:
        validated_types = sorted(_VALID_RECORD_TYPES)
    else:
        invalid = set(record_types) - _VALID_RECORD_TYPES
        if invalid:
            return _envelope(
                ok=False,
                tool="search_handoff",
                data={"error": f"Invalid record_types: {sorted(invalid)}. Valid: {sorted(_VALID_RECORD_TYPES)}."},
            )
        validated_types = list(dict.fromkeys(record_types))

    clamped_limit = max(1, min(int(limit), 200))
    fts_terms: list[str] = []
    for q in queries:
        stripped = _FTS5_CONTROL_RE.sub(" ", q).strip()
        if stripped:
            fts_terms.append('"' + stripped.replace('"', '""') + '"')
    if not fts_terms:
        return _envelope(
            ok=False, tool="search_handoff", data={"error": "All query strings are empty after stripping."}
        )
    fts_query = " OR ".join(fts_terms)

    results: list[dict] = []
    with _get_db_connection() as conn:
        tables_exist = (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','shadow') AND name = 'decisions_fts'",
            ).fetchone()[0]
            > 0
        )
        if not tables_exist:
            return _envelope(
                ok=False,
                tool="search_handoff",
                data={
                    "error": "Structured FTS index is unavailable (FTS5 not enabled). Run 'mcp-workbay-handoff doctor' to verify."
                },
            )
        effective_task_ref: str | None = task_ref
        if effective_task_ref is None:
            try:
                resolved_row = _resolve_workspace_handoff_row(conn)
            except ValueError as exc:
                return _envelope(ok=False, tool="search_handoff", data={"error": str(exc)})
            effective_task_ref = str(resolved_row["task_ref"]) if resolved_row is not None else None

        for rtype in validated_types:
            fts_table, has_status = _RECORD_TYPE_FTS_MAP[rtype]
            status_col = f"{fts_table}.status" if has_status else "NULL AS status"
            # Decision origin filter must live in the FTS SQL *before* LIMIT so
            # system-dominated rank cannot under-fill agent result pages.
            # COALESCE is mandatory: SQL three-valued logic would drop NULL origins.
            if rtype == "decision":
                where_parts = [f"{fts_table} MATCH ?"]
                params: list[object] = [1 if include_system else 0, fts_query]
                if effective_task_ref:
                    where_parts.append(f"{fts_table}.task_ref = ?")
                    params.append(effective_task_ref)
                if lane_id:
                    where_parts.append(f"{fts_table}.lane_id = ?")
                    params.append(lane_id)
                where_sql = " AND ".join(where_parts)
                sql = f"""
                    SELECT {fts_table}.record_id AS record_id,
                           {fts_table}.task_ref AS task_ref,
                           {fts_table}.lane_id AS lane_id,
                           {status_col},
                           snippet({fts_table}, 0, '', '', '...', 12) AS snippet,
                           rank
                    FROM {fts_table}
                    JOIN decisions ON decisions.id = {fts_table}.rowid
                      AND (? OR COALESCE(decisions.decision_origin, 'agent') != 'system')
                    WHERE {where_sql}
                    ORDER BY rank
                    LIMIT ?
                    """
            else:
                where_parts = [f"{fts_table} MATCH ?"]
                params = [fts_query]
                if effective_task_ref:
                    where_parts.append("task_ref = ?")
                    params.append(effective_task_ref)
                if lane_id:
                    where_parts.append("lane_id = ?")
                    params.append(lane_id)
                where_sql = " AND ".join(where_parts)
                sql = f"""
                    SELECT record_id, task_ref, lane_id, {status_col},
                           snippet({fts_table}, 0, '', '', '...', 12) AS snippet,
                           rank
                    FROM {fts_table}
                    WHERE {where_sql}
                    ORDER BY rank
                    LIMIT ?
                    """
            try:
                rows = conn.execute(sql, (*params, clamped_limit)).fetchall()
            except sqlite3.OperationalError as exc:
                return _envelope(ok=False, tool="search_handoff", data={"error": f"FTS5 query error: {exc}"})
            for row in rows:
                results.append(
                    {
                        "record_type": rtype,
                        "record_id": int(row["record_id"]),
                        "task_ref": row["task_ref"],
                        "lane_id": row["lane_id"],
                        "status": row["status"],
                        "snippet": (row["snippet"] or "").strip(),
                        "_rank": float(row["rank"] or 0.0),
                    }
                )

        results.sort(key=lambda r: r["_rank"])
        ranked = results[:clamped_limit]

        decision_extras_by_id: dict[int, dict[str, object]] = {}
        if decision_projection_fields:
            decision_ids = [r["record_id"] for r in ranked if r["record_type"] == "decision"]
            if decision_ids:
                # Quoted identifiers are safe because each name was validated against
                # `_VALID_DECISION_PROJECTION_FIELDS` above (no untrusted strings).
                select_cols = ", ".join(f'"{c}"' for c in decision_projection_fields)
                placeholders = ",".join("?" for _ in decision_ids)
                decision_rows = conn.execute(
                    f"SELECT id, {select_cols} FROM decisions WHERE id IN ({placeholders})",
                    decision_ids,
                ).fetchall()
                for dr in decision_rows:
                    decision_extras_by_id[int(dr["id"])] = {c: dr[c] for c in decision_projection_fields}

    shaped_results: list[dict[str, object]] = []
    for result in ranked:
        shaped = dict(result)
        shaped.pop("_rank", None)
        if detail == "summary":
            shaped["snippet"] = _truncate_text(shaped.get("snippet"), _HANDOFF_SEARCH_SUMMARY_TRUNCATE)
        projected = _project_mapping(shaped, requested_fields, _HANDOFF_SEARCH_IDENTITY_FIELDS)
        if result["record_type"] == "decision":
            extra = decision_extras_by_id.get(int(result["record_id"]))
            if extra:
                projected.update(extra)
        shaped_results.append(projected)
    return _envelope(
        ok=True,
        tool="search_handoff",
        data={
            "results": shaped_results,
            "total": len(results),
            "query": fts_query,
            "record_types_searched": validated_types,
        },
        task_ref=effective_task_ref,
    )


# Artifact tools (depend on artifact_index and config)
def record_artifact(
    source_kind: str,
    source_label: str,
    content: str,
    task_ref: str | None = None,
    lane_id: str | None = None,
    app_root: str | None = None,
    content_type: str = "text/plain",
    summary: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Index an artifact source in the sidecar artifact database."""
    config = get_runtime_config()
    sk = _normalize_optional_text(source_kind)
    sl = _normalize_optional_text(source_label)
    if sk is None:
        return _envelope(ok=False, tool="record_artifact", data={"error": "source_kind is required."})
    if sl is None:
        return _envelope(ok=False, tool="record_artifact", data={"error": "source_label is required."})
    if not content:
        return _envelope(ok=False, tool="record_artifact", data={"error": "content is required."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
    try:
        result = artifact_index.upsert_source(
            task_ref=resolved_task_ref,
            lane_id=_normalize_optional_text(lane_id),
            app_root=_normalize_optional_text(app_root),
            source_kind=sk,
            source_label=sl,
            content_type=content_type or "text/plain",
            summary=_normalize_optional_text(summary),
            content=content,
            metadata=metadata,
            artifact_db_path=config.artifact_db_path,
        )
        return _envelope(
            ok=True,
            tool="record_artifact",
            data=result,
            task_ref=resolved_task_ref,
            mutation={"entity": "artifact_source", "operation": "upsert"},
        )
    except RuntimeError as exc:
        return _envelope(ok=False, tool="record_artifact", data={"error": str(exc)}, task_ref=resolved_task_ref)


def search_artifacts(
    queries: list[str] | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    app_root: str | None = None,
    source_kind: str | None = None,
    content_type: str | None = None,
    limit: int = 10,
    offset: int = 0,
    detail: str = "full",
    fields: str | None = None,
) -> dict:
    """Search indexed artifact chunks, or list sources when no queries given."""
    config = get_runtime_config()
    detail = _normalize_detail(detail)
    if not queries:
        requested_fields = _parse_projection_fields(fields, _VALID_ARTIFACT_SOURCE_LIST_FIELDS)
        resolved_task_ref: str | None = None
        if task_ref:
            with _get_db_connection() as conn:
                resolved_task_ref = _resolve_task_ref(conn, task_ref)
        try:
            rows = artifact_index.list_artifact_sources(
                task_ref=resolved_task_ref,
                lane_id=_normalize_optional_text(lane_id),
                app_root=_normalize_optional_text(app_root),
                source_kind=_normalize_optional_text(source_kind),
                limit=max(1, int(limit)),
                offset=max(0, int(offset)),
                artifact_db_path=config.artifact_db_path,
            )
            shaped_rows = [
                _project_mapping(
                    _summarize_artifact_source(dict(row)) if detail == "summary" else dict(row),
                    requested_fields,
                    _ARTIFACT_SOURCE_IDENTITY_FIELDS,
                )
                for row in rows
            ]
            return _envelope(
                ok=True,
                tool="search_artifacts",
                data={"mode": "sources", "total": len(rows), "sources": shaped_rows},
                task_ref=resolved_task_ref,
            )
        except RuntimeError as exc:
            return _envelope(ok=False, tool="search_artifacts", data={"error": str(exc)}, task_ref=resolved_task_ref)
    requested_fields = _parse_projection_fields(fields, _VALID_ARTIFACT_HIT_FIELDS)
    scope: dict[str, str | None] = {}
    if task_ref:
        with _get_db_connection() as conn:
            scope["task_ref"] = _resolve_task_ref(conn, task_ref)
    else:
        scope["task_ref"] = None
    try:
        hits = artifact_index.search_artifacts(
            queries=queries,
            task_ref=scope["task_ref"],
            lane_id=_normalize_optional_text(lane_id),
            app_root=_normalize_optional_text(app_root),
            source_kind=_normalize_optional_text(source_kind),
            content_type=_normalize_optional_text(content_type),
            limit=max(1, int(limit)),
            artifact_db_path=config.artifact_db_path,
        )
        shaped_hits = [
            _project_mapping(
                _summarize_artifact_hit(dict(hit)) if detail == "summary" else dict(hit),
                requested_fields,
                _ARTIFACT_HIT_IDENTITY_FIELDS,
            )
            for hit in hits
        ]
        return _envelope(
            ok=True,
            tool="search_artifacts",
            data={"mode": "search", "total": len(hits), "hits": shaped_hits},
            task_ref=scope["task_ref"],
        )
    except RuntimeError as exc:
        return _envelope(ok=False, tool="search_artifacts", data={"error": str(exc)}, task_ref=scope["task_ref"])


def get_artifact(
    source_id: int | None = None,
    task_ref: str | None = None,
    source_label: str | None = None,
    include_terms: bool = False,
    top_n_terms: int = 10,
    detail: str = "full",
    fields: str | None = None,
) -> dict:
    """Return the full artifact source record, optionally with distinctive terms."""
    config = get_runtime_config()
    detail = _normalize_detail(detail)
    requested_fields = _parse_projection_fields(fields, _VALID_ARTIFACT_GET_FIELDS)
    if source_id is None and not (task_ref and source_label):
        return _envelope(
            ok=False, tool="get_artifact", data={"error": "Provide source_id or both task_ref and source_label."}
        )
    resolved_task_ref: str | None = None
    if task_ref:
        with _get_db_connection() as conn:
            resolved_task_ref = _resolve_task_ref(conn, task_ref)
    try:
        source = artifact_index.get_artifact_source(
            source_id=source_id,
            task_ref=resolved_task_ref,
            source_label=_normalize_optional_text(source_label),
            artifact_db_path=config.artifact_db_path,
        )
        if source is None:
            return _envelope(
                ok=False, tool="get_artifact", data={"error": "Artifact source not found."}, task_ref=resolved_task_ref
            )
        shaped_source = _project_mapping(
            _summarize_artifact_source(dict(source)) if detail == "summary" else dict(source),
            requested_fields,
            _ARTIFACT_GET_IDENTITY_FIELDS,
        )
        data: dict[str, object] = {"source": shaped_source}
        if include_terms:
            resolved_source_id = source["id"]
            terms = artifact_index.get_distinctive_terms(
                source_id=resolved_source_id,
                artifact_db_path=config.artifact_db_path,
                top_n=max(1, int(top_n_terms)),
            )
            data["source_id"] = resolved_source_id
            data["terms"] = terms
        return _envelope(ok=True, tool="get_artifact", data=data, task_ref=resolved_task_ref)
    except RuntimeError as exc:
        return _envelope(ok=False, tool="get_artifact", data={"error": str(exc)}, task_ref=resolved_task_ref)


def purge_artifacts(
    task_ref: str | None = None,
    lane_id: str | None = None,
    app_root: str | None = None,
    older_than_days: int | None = None,
) -> dict:
    """Delete artifact sources and their FTS chunks."""
    config = get_runtime_config()
    resolved_task_ref: str | None = None
    if task_ref:
        with _get_db_connection() as conn:
            resolved_task_ref = _resolve_task_ref(conn, task_ref)
    if resolved_task_ref is None and lane_id is None and app_root is None and older_than_days is None:
        return _envelope(
            ok=False,
            tool="purge_artifacts",
            data={"error": "Provide task_ref, lane_id, app_root, older_than_days, or a combination."},
        )
    try:
        result = artifact_index.purge_artifacts(
            task_ref=resolved_task_ref,
            lane_id=_normalize_optional_text(lane_id),
            app_root=_normalize_optional_text(app_root),
            older_than_days=older_than_days,
            artifact_db_path=config.artifact_db_path,
        )
        return _envelope(
            ok=True,
            tool="purge_artifacts",
            data=result,
            task_ref=resolved_task_ref,
            mutation={"entity": "artifact_source", "operation": "delete"},
        )
    except RuntimeError as exc:
        return _envelope(ok=False, tool="purge_artifacts", data={"error": str(exc)}, task_ref=resolved_task_ref)


# Compound tools (cross-module orchestration)
def load_session(
    task_ref: str | None = None,
    sections: str | None = None,
    detail: str | None = None,
    top_n_blockers: int | None = None,
    top_n_actions: int | None = None,
    top_n_decisions: int | None = None,
    top_n_slices: int | None = None,
    top_n_tests: int | None = None,
    top_n_findings: int | None = None,
    top_n_touched_files: int | None = None,
    read_profile: str | None = None,
    open_findings_limit: int | None = None,
    open_findings_detail: str | None = None,
    response_budget_bytes: int | None = None,
    budget_policy: str | None = None,
    source: str | None = None,
    session: str | None = None,
    include_context_refresh: bool = False,
    last_injected_compaction_id: str | None = None,
    last_injected_continuation_id: str | None = None,
    include_write_schemas: bool = False,
) -> dict:
    """Load session context: get_handoff_state + open findings + touched files.

    Passes ``sections`` / ``detail`` through to ``get_handoff_state`` and
    ``list_review_findings`` so callers can reduce payload size without
    making two separate calls. ``top_n_touched_files`` bounds the additive
    ``touched_files`` list.

    internal: ``read_profile`` selects a named intent shape
    (``identity``, ``hot_summary``, ``review_packet``, ``open_items``,
    ``full_debug``). The state shape is forwarded to ``get_handoff_state``
    (which performs its own profile expansion). The compound add-on shape
    — open findings and touched files — is resolved here. A zero limit on
    an add-on is a sentinel meaning "omit this section entirely"; the
    omission is recorded in ``data.read_shape.session.omitted_sections``
    rather than clamped to one row.

    Newest continuation packet (when present) is attached at
    ``data.continuation`` with compact shape
    ``{packet_id, saved_at, lane_id, sections}``. Pass
    ``last_injected_continuation_id`` to dedupe a packet the caller already
    injected (matching id yields ``{packet_id, deduped: true}`` only).
    Budget/zero-omit machinery may drop the section and record
    ``continuation`` under ``data.read_shape.session.omitted_sections``.

    ``include_write_schemas`` (default False) is forwarded to
    ``get_handoff_state`` so routine compound reads omit the ~13KB
    ``limits.write.tools`` registry unless the caller opts in.
    """
    # [DATA-14] bare-call default budget (no profile + no explicit budget +
    # no explicit sections= shape; S10-A-02 exemption).
    response_budget_bytes, budget_policy, _bare_budget_applied = read_budget_module.apply_bare_call_budget_defaults(
        read_profile=read_profile,
        response_budget_bytes=response_budget_bytes,
        budget_policy=budget_policy,
        sections=sections,
        detail=detail,
    )
    # Resolve compound add-on shape locally. ``get_handoff_state`` handles
    # its own state shape (we just forward the caller's intent).
    try:
        add_on = read_profiles_module.resolve_session_add_on_shape(
            read_profile=read_profile,
            open_findings_limit=open_findings_limit,
            open_findings_detail=open_findings_detail,
            top_n_touched_files=top_n_touched_files,
        )
    except read_profiles_module.UnknownProfileError as exc:
        return _envelope(
            ok=False,
            tool="load_session",
            task_ref=task_ref,
            data={
                "error": (
                    f"Unknown read_profile {exc.name!r}. "
                    f"Valid profiles: {list(read_profiles_module.VALID_PROFILE_NAMES)}."
                ),
                "valid_profiles": list(read_profiles_module.VALID_PROFILE_NAMES),
            },
        )

    # internal: compound budget planning. We resolve the state
    # shape locally too so the planner can choose reductions across the
    # whole load_session envelope before either ``get_handoff_state`` or
    # the add-on fetches materialise heavy rows.
    try:
        state_shape = read_profiles_module.resolve_state_shape(
            read_profile=read_profile,
            sections=sections,
            detail=detail,
            top_n_blockers=top_n_blockers,
            top_n_actions=top_n_actions,
            top_n_decisions=top_n_decisions,
            top_n_slices=top_n_slices,
            top_n_tests=top_n_tests,
            top_n_findings=top_n_findings,
        )
    except read_profiles_module.UnknownProfileError as exc:
        return _envelope(
            ok=False,
            tool="load_session",
            task_ref=task_ref,
            data={
                "error": (
                    f"Unknown read_profile {exc.name!r}. "
                    f"Valid profiles: {list(read_profiles_module.VALID_PROFILE_NAMES)}."
                ),
                "valid_profiles": list(read_profiles_module.VALID_PROFILE_NAMES),
            },
        )

    try:
        effective_policy = read_budget_module.resolve_policy(
            response_budget_bytes=response_budget_bytes, budget_policy=budget_policy
        )
    except read_budget_module.UnknownBudgetPolicyError as exc:
        return _envelope(
            ok=False,
            tool="load_session",
            task_ref=task_ref,
            data={
                "error": (
                    f"Unknown budget_policy {exc.policy!r}. Valid policies: {list(read_budget_module.VALID_POLICIES)}."
                ),
                "valid_policies": list(read_budget_module.VALID_POLICIES),
            },
        )

    planned_state_shape, planned_add_on, budget_plan = read_budget_module.plan_session_read(
        state_shape=state_shape,
        add_on=add_on,
        response_budget_bytes=response_budget_bytes,
        budget_policy=effective_policy,
    )
    if budget_plan.fail_now:
        return _envelope(
            ok=False,
            tool="load_session",
            task_ref=task_ref,
            data={
                "error": (
                    f"response_budget_bytes={response_budget_bytes} cannot fit requested compound shape "
                    f"(estimated {budget_plan.estimated_initial_bytes} bytes). "
                    "Retry with the suggested narrower profile or budget_policy='auto_summary'."
                ),
                "read_budget": read_budget_module.budget_payload(budget_plan),
            },
        )
    add_on = planned_add_on

    # Build the sections string forwarded to ``get_handoff_state``. The
    # compound planner may have decided to omit state-side sections to
    # fit the budget; strip those from the forwarded sections request so
    # ``get_handoff_state`` does not fetch them.
    state_omitted = {s for s in budget_plan.omitted_sections if s not in ("open_findings", "touched_files")}
    if state_omitted and planned_state_shape.sections is not None:
        active_tokens = [
            t.strip() for t in planned_state_shape.sections.split(",") if t.strip() and t.strip() not in state_omitted
        ]
        forwarded_sections: str | None = ",".join(active_tokens) if active_tokens else "identity"
    elif state_omitted and planned_state_shape.sections is None:
        # planner reduced from "all sections" -> explicit list minus omitted
        from .handoff_state import _VALID_SECTIONS as _ALL  # noqa: PLC0415

        keep = sorted(set(_ALL) - state_omitted)
        forwarded_sections = ",".join(keep) if keep else "identity"
    else:
        forwarded_sections = planned_state_shape.sections

    # Suppress the boundary around the delegated state read so the compound
    # ``load_session`` records exactly one ``load_session`` row rather than
    # double-counting a nested ``get_handoff_state`` row. The boundary (set by
    # the MCP dispatch wrapper for ``load_session``) is restored on exit, so
    # the ``record_orientation_read_once`` below still fires at the boundary.
    with suppress_orientation_read_boundary():
        state_envelope = get_handoff_state(
            task_ref=task_ref,
            sections=forwarded_sections,
            detail=planned_state_shape.detail,
            top_n_blockers=planned_state_shape.top_n_blockers,
            top_n_actions=planned_state_shape.top_n_actions,
            top_n_decisions=planned_state_shape.top_n_decisions,
            top_n_slices=planned_state_shape.top_n_slices,
            top_n_tests=planned_state_shape.top_n_tests,
            top_n_findings=planned_state_shape.top_n_findings,
            read_profile=read_profile,
            source=source,
            session=session,
            # Nested state read already budgeted at the compound layer;
            # forward an explicit budget so get_handoff_state does not
            # re-apply bare-call defaults, and honor write-schema opt-in.
            response_budget_bytes=response_budget_bytes,
            budget_policy=budget_policy,
            include_write_schemas=include_write_schemas,
        )
    if not state_envelope.get("ok"):
        return state_envelope
    state_data = state_envelope.get("data", {}) or {}
    resolved_task_ref = state_envelope.get("scope", {}).get("task_ref")
    record_orientation_read_once(
        tool="load_session",
        task_ref=str(resolved_task_ref) if resolved_task_ref else task_ref,
        resolution_outcome="explicit" if task_ref is not None else "resolved",
        source=source,
        session=session,
        read_profile=read_profile,
    )

    # Resolve effective detail for findings: explicit ``open_findings_detail``
    # wins; otherwise when no profile is requested, fall back to the
    # caller's ``detail`` (preserves pre-internal behavior where the same
    # ``detail`` value was forwarded to both the state read and the
    # findings list); otherwise use the profile add-on detail.
    if open_findings_detail is not None:
        findings_detail = open_findings_detail
    elif read_profile is None and detail is not None:
        findings_detail = detail
    else:
        findings_detail = add_on.open_findings_detail

    omitted_sections: list[str] = []

    # Zero-limit sentinel: omit additive sections entirely rather than
    # round 0 -> 1 the way row-limit clamps would.
    if add_on.open_findings_limit <= 0:
        open_findings: list[dict] = []
        open_findings_count = 0
        omitted_sections.append("open_findings")
    else:
        findings_envelope = list_review_findings(
            task_ref=resolved_task_ref,
            status="open",
            detail=findings_detail,
            limit=add_on.open_findings_limit,
        )
        findings_data = findings_envelope.get("data", {}) or {}
        findings_ok = bool(findings_envelope.get("ok"))
        open_findings = findings_data.get("findings", []) if findings_ok else []
        open_findings_count = findings_data.get("total_matching", 0) if findings_ok else 0

    if add_on.top_n_touched_files <= 0:
        touched_files: list[dict] = []
        omitted_sections.append("touched_files")
    else:
        touches_envelope = get_touched_files(task_ref=resolved_task_ref, limit=add_on.top_n_touched_files)
        touches_data = touches_envelope.get("data", {}) or {}
        touches_ok = bool(touches_envelope.get("ok"))
        touched_files = touches_data.get("touches", []) if touches_ok else []

    data: dict[str, object] = {
        "state": state_data,
        "open_findings": open_findings,
        "open_findings_count": open_findings_count,
        "touched_files": touched_files,
    }
    if "slices_completed" in state_data:
        data["slices_completed"] = state_data["slices_completed"]
    if "compaction_advisory" in state_data:
        data["compaction_advisory"] = state_data["compaction_advisory"]
        for key, value in state_data.items():
            if key == "compaction_advisory" or not key.startswith("compaction_"):
                continue
            data[key] = value
    if include_context_refresh:
        advisory = data.get("compaction_advisory")
        from .compaction import extract_visible_texts_for_semantic_reinjection

        visible_texts = extract_visible_texts_for_semantic_reinjection(state_data)
        data["context_refresh"] = build_context_refresh_packet(
            task_ref=resolved_task_ref,
            last_injected_compaction_id=last_injected_compaction_id,
            advisory=advisory if isinstance(advisory, dict) else None,
            visible_texts=visible_texts,
        )

    # internal: auto-attach newest continuation packet (add-on).
    # Budget planner may list "continuation" in omitted_sections (zero/omit
    # sentinel or auto_summary trim); honor that before attaching.
    continuation_omitted = "continuation" in budget_plan.omitted_sections
    if continuation_omitted:
        if "continuation" not in omitted_sections:
            omitted_sections.append("continuation")
    else:
        from .continuation import build_session_continuation

        continuation_payload = build_session_continuation(
            str(resolved_task_ref) if resolved_task_ref is not None else task_ref,
            last_injected_continuation_id=last_injected_continuation_id,
        )
        if continuation_payload is not None:
            data["continuation"] = continuation_payload

    # Attach read_shape only when the caller requested a profile. The
    # state-side shape is mirrored from get_handoff_state; the session
    # add-on shape is rendered here.
    if read_profile is not None:
        state_read_shape = state_data.get("read_shape") if isinstance(state_data, dict) else None
        data["read_shape"] = {
            "state": state_read_shape,
            "session": read_profiles_module.session_add_on_payload(add_on, omitted_sections=omitted_sections),
        }
    # internal: compound read_budget attached whenever the caller
    # supplied a budget or the planner applied any compound reductions.
    if response_budget_bytes is not None or budget_plan.applied_reductions:
        data["read_budget"] = read_budget_module.budget_payload(budget_plan)
    return _envelope(ok=True, tool="load_session", data=data, task_ref=resolved_task_ref)


# T8 / [RES-01]: bounded optimistic auto-retry for idempotent writes only.
# close_slice is idempotent by (task_ref, decision, session); status_only
# updates are safe to re-apply. Full set_handoff_state writes keep the hard guard.
MAX_OPTIMISTIC_REVISION_ATTEMPTS = 3

# Exception classes that indicate a programming/contract bug in a post-commit
# side-effect path rather than infrastructure failure (mirrors lanes_recording).
_CLOSE_SLICE_PROGRAMMING_SIDE_EFFECT_ERRORS = (TypeError, AttributeError, ImportError)


def _close_slice_side_effect_error_class(exc: BaseException) -> str:
    """Classify a post-commit side-effect failure as system vs application ([RES-06])."""
    if isinstance(exc, _CLOSE_SLICE_PROGRAMMING_SIDE_EFFECT_ERRORS):
        return "application"
    return "system"


def _run_close_slice_postcommit_side_effects(
    *,
    resolved_task_ref: str,
    rationale: str | None,
    mutation: dict,
    warnings: list[str],
    include_embed: bool = True,
) -> dict:
    """Run close_slice post-commit side effects outside any write lock.

    Failures never raise: each site appends a self-identifying warning and a
    typed ``side_effect_failures`` entry (effect name + system|application
    class). Callers surface ``partial=True`` when the list is non-empty so
    ``ok=True`` is not mistaken for full success ([RES-02], [RES-06]).

    Safe to re-run on idempotent close_slice replay so a crash between commit
    and projection can heal filesystem drift ([RES-01]).
    """
    side_effect_failures: list[dict] = []

    if include_embed and mutation.get("operation") in {"insert", "update"}:
        affected_ids = mutation.get("affected_ids") or []
        if affected_ids:
            try:
                from .concept_embed_hook import embed_concept_on_write  # noqa: PLC0415

                outcome = embed_concept_on_write(
                    "decision.rationale", affected_ids[0], resolved_task_ref, rationale
                )
                # Production failures are swallowed inside embed_concept_best_effort
                # and returned as ``failed`` — they never raise ([RES-02]).
                if outcome == "failed":
                    msg = (
                        "embed_concept_on_write failed during close_slice: "
                        "provider/store returned failed"
                    )
                    warnings.append(msg)
                    side_effect_failures.append(
                        {
                            "effect": "embed_concept_on_write",
                            "error_class": "system",
                            "detail": msg,
                        }
                    )
            except Exception as exc:
                msg = f"embed_concept_on_write failed during close_slice: {exc}"
                warnings.append(msg)
                side_effect_failures.append(
                    {
                        "effect": "embed_concept_on_write",
                        "error_class": _close_slice_side_effect_error_class(exc),
                        "detail": msg,
                    }
                )

    try:
        from .current_task_rendering import _write_per_task_projection  # noqa: PLC0415

        _write_per_task_projection(resolved_task_ref)
    except Exception as exc:
        msg = f"per-task projection failed during close_slice: {exc}"
        warnings.append(msg)
        side_effect_failures.append(
            {
                "effect": "per_task_projection",
                "error_class": _close_slice_side_effect_error_class(exc),
                "detail": msg,
            }
        )

    current_task_md_written = False
    try:
        current_task_md_written = bool(_write_current_task_md_from_state(resolved_task_ref))
    except Exception as exc:
        msg = f"current_task.md write failed during close_slice: {exc}"
        warnings.append(msg)
        side_effect_failures.append(
            {
                "effect": "current_task_md",
                "error_class": _close_slice_side_effect_error_class(exc),
                "detail": msg,
            }
        )

    dashboard_written = False
    dashboard_path = "DASHBOARD.txt"
    try:
        from .dashboard_rendering import generate_dashboard_md  # noqa: PLC0415

        dashboard_result = generate_dashboard_md(write_file=True)
        dashboard_written = bool(dashboard_result.get("written"))
        rendered_path = dashboard_result.get("path")
        if rendered_path:
            dashboard_path = str(rendered_path)
    except Exception as exc:
        msg = f"dashboard render failed during close_slice: {exc}"
        warnings.append(msg)
        side_effect_failures.append(
            {
                "effect": "dashboard",
                "error_class": _close_slice_side_effect_error_class(exc),
                "detail": msg,
            }
        )

    return {
        "current_task_md_written": current_task_md_written,
        "dashboard_written": dashboard_written,
        "dashboard_path": dashboard_path,
        "side_effect_failures": side_effect_failures,
    }


def _apply_close_slice_side_effect_fields(data: dict, side_effects: dict) -> None:
    """Merge post-commit outcomes into close_slice envelope data ([RES-06])."""
    data["current_task_md_written"] = side_effects["current_task_md_written"]
    data["dashboard_written"] = side_effects["dashboard_written"]
    failures = list(side_effects.get("side_effect_failures") or [])
    if failures:
        data["partial"] = True
        data["side_effect_failures"] = failures
    else:
        data["partial"] = False
    # projection_stale means filesystem projections (per-task / current_task_md)
    # may be behind the durable commit. Dashboard-only and embed-only failures
    # do NOT set it: dashboard is a separate artifact, and embed staleness is
    # carried by the side_effect_failures entry (effect=embed_concept_on_write)
    # plus agent_errors.embedding_failed — not by this flag ([OBS-08], [RES-06]).
    if any(
        f.get("effect") not in ("dashboard", "embed_concept_on_write")
        for f in failures
    ):
        data["projection_stale"] = True


def _heal_close_slice_postcommit(
    *,
    task_ref: str,
    decision_row: dict | None,
    warnings: list[str],
) -> dict:
    """Re-run post-commit projections on idempotent replay ([RES-01]).

    Decision+state rows are already durable; embed has an independent backfill.
    Projection / CURRENT_TASK / DASHBOARD do not — replaying the same close must
    re-attempt them so a crash between commit and projection can heal.
    Appends any degrade warnings onto the caller's ``warnings`` list.
    """
    # Prefer the stored decision rationale for embed heal when available.
    rationale = None
    mutation: dict = {}
    if isinstance(decision_row, dict):
        rationale = decision_row.get("rationale")
        decision_id = decision_row.get("id")
        if decision_id is not None:
            mutation = {"operation": "update", "affected_ids": [decision_id]}
    return _run_close_slice_postcommit_side_effects(
        resolved_task_ref=task_ref,
        rationale=str(rationale) if rationale is not None else None,
        mutation=mutation,
        warnings=warnings,
        include_embed=bool(mutation),
    )


def _idempotent_close_slice_envelope(
    *,
    task_ref: str,
    decision: str,
    session: str,
    revision_retries: int = 0,
) -> dict | None:
    with _get_db_connection() as conn:
        existing = conn.execute(
            "SELECT * FROM decisions WHERE task_ref = ? AND decision = ? AND session = ?",
            (task_ref, decision, session),
        ).fetchone()
        if existing is None:
            return None
        decision_row = _row_to_dict(existing)
        revision_row = conn.execute(
            "SELECT revision FROM handoff_state WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    current_revision = int(revision_row["revision"]) if revision_row is not None else None
    # RES-01: re-run post-commit pipeline on replay so filesystem projections
    # heal after a crash between commit and side effects.
    heal_warnings: list[str] = []
    side_effects = _heal_close_slice_postcommit(
        task_ref=task_ref,
        decision_row=decision_row if isinstance(decision_row, dict) else None,
        warnings=heal_warnings,
    )
    data: dict = {
        "decision_recorded": False,
        "state_updated": False,
        "state_error": None,
        "idempotent": True,
        "current_task_md_written": False,
        "dashboard_written": False,
        "decision": decision_row,
        "task_revision": current_revision,
        "revision_retries": revision_retries,
    }
    _apply_close_slice_side_effect_fields(data, side_effects)
    return _envelope(
        ok=True,
        tool="close_slice",
        data=data,
        task_ref=task_ref,
        artifacts=[
            {
                "type": "current_task_md",
                "path": "CURRENT_TASK.json",
                "written": bool(data.get("current_task_md_written")),
            },
            {
                "type": "dashboard",
                "path": str(side_effects.get("dashboard_path") or "DASHBOARD.txt"),
                "written": bool(data.get("dashboard_written")),
            },
        ],
        warnings=heal_warnings or None,
    )


def _close_slice_revision_conflict(
    *,
    task_ref: str,
    expected_revision: int | None,
    current_revision: int | None,
    revision_retries: int,
) -> dict:
    return _envelope(
        ok=False,
        tool="close_slice",
        data={
            "error": "Revision conflict.",
            "state_error": "Revision conflict.",
            "decision_recorded": False,
            "state_updated": False,
            "expected_revision": expected_revision,
            "current_revision": current_revision,
            "current_task_md_written": False,
            "revision_retries": revision_retries,
        },
        task_ref=task_ref,
    )


def close_slice(
    session: str,
    decision: str,
    rationale: str | None = None,
    actor: WriteActor | None = None,
    expected_revision: int | None = None,
    task_ref: str | None = None,
    focus: str | None = None,
    changed_files: list[str] | None = None,
    slice_number: int | None = None,
) -> dict:
    """Record a slice-complete decision and update handoff state.

    Keeps non-done tasks ``in_progress``. When the task is already ``done``,
    the status is preserved (T10) while the slice decision is still recorded.
    Concurrent revision conflicts on this idempotent write path are auto-retried
    up to ``MAX_OPTIMISTIC_REVISION_ATTEMPTS`` times ([RES-01] / T8).

    Always regenerates DASHBOARD.txt. CURRENT_TASK.json is regenerated only when
    ``current_task_auto_regen`` is enabled; otherwise it is refreshed on demand via
    ``render_handoff(kind='current_task')``. The result reports ``dashboard_written``
    and ``current_task_md_written``.
    """
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        task_row = conn.execute(
            "SELECT revision, status FROM handoff_state WHERE task_ref = ?",
            (resolved_task_ref,),
        ).fetchone()

    has_active_row = task_row is not None
    if has_active_row:
        idempotent_envelope = _idempotent_close_slice_envelope(
            task_ref=resolved_task_ref,
            decision=decision,
            session=session,
        )
        if idempotent_envelope is not None:
            return idempotent_envelope
        if expected_revision is None:
            current_revision: int | None = int(task_row["revision"])
            return _envelope(
                ok=False,
                tool="close_slice",
                data={
                    "error": (
                        "expected_revision is required for updates. "
                        "Fetch the active row first via get_handoff_state(sections='identity') "
                        "and pass its revision field as expected_revision."
                    ),
                    "state_error": (
                        "expected_revision is required for updates. "
                        "Fetch the active row first via get_handoff_state(sections='identity') "
                        "and pass its revision field as expected_revision."
                    ),
                    "decision_recorded": False,
                    "state_updated": False,
                    "current_revision": current_revision,
                    "current_task_md_written": False,
                    "revision_retries": 0,
                },
                task_ref=resolved_task_ref,
            )
    else:
        with _get_db_connection() as conn2:
            archived = conn2.execute(
                "SELECT 1 FROM task_archives WHERE task_ref = ?",
                (resolved_task_ref,),
            ).fetchone()
        if archived is not None:
            return _envelope(
                ok=False,
                tool="close_slice",
                data={
                    "error": "Cannot close a slice on an archived task. Switch to it first or use update_task_status.",
                    "state_error": "Cannot close a slice on an archived task. Switch to it first or use update_task_status.",
                    "decision_recorded": False,
                    "state_updated": False,
                    "current_task_md_written": False,
                    "revision_retries": 0,
                },
                task_ref=resolved_task_ref,
            )

    validation_error = _validate_decision_payload(decision, rationale)
    if validation_error is not None:
        return _envelope(ok=False, tool="close_slice", data={"error": validation_error})
    normalized_changed_files, changed_files_error = _normalize_changed_files_payload(changed_files)
    if changed_files_error is not None:
        return _envelope(ok=False, tool="close_slice", data={"error": changed_files_error})

    # Preparation (actor resolve, changed-files derivation, target-context
    # precompute) runs inside the retry loop *after* the idempotent short-
    # circuit so a replayed close_slice returns the recorded envelope without
    # redoing prep ([internal], [ARCH-13], [RES-07]). Prep still completes
    # before any BEGIN IMMEDIATE open ([internal], [internal]).
    derivation_warnings: list[str] = []
    pre_resolved_ctx: ResolvedWriteContext | None = None
    precomputed_target_worktree_path: str | None = None
    prep_done = False

    # T8: bounded optimistic auto-retry. close_slice is idempotent by decision id,
    # so adopting the freshest revision token after a concurrent bump is safe.
    working_revision = expected_revision
    revision_retries = 0
    decision_envelope: dict = {}
    state_envelope: dict = {}
    decision_payload: dict = {}
    warnings: list[str] = []
    status_preserved: str | None = None
    # When a concurrent writer lands the same decision inside the IMMEDIATE
    # block, capture the idempotent payload and exit the lock before heal
    # ([CON-18]) — never run post-commit side effects under BEGIN IMMEDIATE.
    idempotent_in_lock: dict | None = None

    for attempt in range(MAX_OPTIMISTIC_REVISION_ATTEMPTS):
        idempotent_envelope = _idempotent_close_slice_envelope(
            task_ref=resolved_task_ref,
            decision=decision,
            session=session,
            revision_retries=revision_retries,
        )
        if idempotent_envelope is not None:
            return idempotent_envelope

        if has_active_row:
            with _get_db_connection() as rev_conn:
                rev_row = rev_conn.execute(
                    "SELECT revision, status FROM handoff_state WHERE task_ref = ?",
                    (resolved_task_ref,),
                ).fetchone()
            if rev_row is None:
                has_active_row = False
            else:
                current_revision = int(rev_row["revision"])
                if working_revision is None:
                    return _envelope(
                        ok=False,
                        tool="close_slice",
                        data={
                            "error": (
                                "expected_revision is required for updates. "
                                "Fetch the active row first via get_handoff_state(sections='identity') "
                                "and pass its revision field as expected_revision."
                            ),
                            "state_error": (
                                "expected_revision is required for updates. "
                                "Fetch the active row first via get_handoff_state(sections='identity') "
                                "and pass its revision field as expected_revision."
                            ),
                            "decision_recorded": False,
                            "state_updated": False,
                            "current_revision": current_revision,
                            "current_task_md_written": False,
                            "revision_retries": revision_retries,
                        },
                        task_ref=resolved_task_ref,
                    )
                if working_revision != current_revision:
                    # SWEEP-01 / HANDOFF-REV-002: only a *stale* token — one
                    # lower than the live revision, i.e. plausibly read before a
                    # concurrent bump ([RES-01] / T8) — may be auto-adopted. A
                    # token AHEAD of the live revision was never valid, so
                    # "healing" it would let the state update (and the
                    # CURRENT_TASK artifact writes that follow state success)
                    # proceed off a fabricated caller view.
                    if working_revision > current_revision or attempt >= MAX_OPTIMISTIC_REVISION_ATTEMPTS - 1:
                        return _close_slice_revision_conflict(
                            task_ref=resolved_task_ref,
                            expected_revision=working_revision,
                            current_revision=current_revision,
                            revision_retries=revision_retries,
                        )
                    working_revision = current_revision
                    revision_retries += 1
                    continue

        # Prep once after the short-circuit confirms this is not a pure replay.
        # Deferred connections only — never under BEGIN IMMEDIATE.
        if not prep_done:
            if changed_files is None:
                from .changed_files_derivation import resolve_omitted_changed_files  # noqa: PLC0415

                with _get_db_connection() as derive_conn:
                    resolved_task_ref = _resolve_task_ref(derive_conn, task_ref)
                    pre_resolved_ctx = _resolve_write_actor(derive_conn, actor, task_ref=resolved_task_ref)
                runtime = get_runtime_config()
                repo_root = Path(runtime.git_workspace_root or runtime.workspace_root)
                derived_paths, derive_warning = resolve_omitted_changed_files(
                    repo_root, pre_resolved_ctx.commit_sha
                )
                if derived_paths is not None:
                    normalized_changed_files = derived_paths
                elif derive_warning:
                    derivation_warnings.append(f"changed_files_derivation:{derive_warning}")
                    normalized_changed_files = []

            if pre_resolved_ctx is None:
                with _get_db_connection() as prep_conn:
                    prep_task_ref = _resolve_task_ref(prep_conn, task_ref)
                    pre_resolved_ctx = _resolve_write_actor(prep_conn, actor, task_ref=prep_task_ref)

            # Precompute worktree path outside the exclusive lock so derivation
            # never shells out under BEGIN IMMEDIATE ([internal], [ARCH-13],
            # [CON-11], [RES-02], [CON-22]). Do not precompute the warning list:
            # collect_target_context_warnings also raises BranchMismatchError, and
            # a precomputed list would skip the in-lock pure-SQL recheck against
            # committed target_branch (stale across revision-conflict retries).
            with _get_db_connection() as ctx_conn:
                ctx_task_ref = _resolve_task_ref(ctx_conn, task_ref)
                branch_row = ctx_conn.execute(
                    "SELECT target_branch FROM handoff_state WHERE task_ref = ?",
                    (ctx_task_ref,),
                ).fetchone()
                target_branch_for_ctx = (
                    _normalize_optional_text(branch_row["target_branch"])
                    if branch_row is not None and branch_row["target_branch"]
                    else None
                )
                if (
                    precomputed_target_worktree_path is None
                    and target_branch_for_ctx is not None
                    and _worktree_derivation_enabled()
                ):
                    try:
                        precomputed_target_worktree_path = _canonical_worktree_for_task(target_branch_for_ctx)
                    except WorktreeNotFoundError:
                        precomputed_target_worktree_path = None
            prep_done = True

        # Write lock up front via BEGIN IMMEDIATE ([internal], [RES-01],
        # [ARCH-13]): structure owns the property so close_slice cannot take a
        # deferred snapshot then fail on an unretryable upgrade. Contention
        # waits on busy_timeout at acquisition ([RES-02]). Actor/git resolution
        # and target-context precompute already completed above so this block
        # is database work only ([internal], [internal], [RES-02]).
        with _get_db_connection(begin_immediate=True) as conn:
            decision_envelope = _record_decision_with_conn(
                conn,
                session=session,
                decision=decision,
                rationale=rationale,
                actor=actor,
                task_ref=task_ref,
                changed_files=normalized_changed_files,
                slice_number=slice_number,
                resolved_ctx=pre_resolved_ctx,
                precomputed_target_worktree_path=precomputed_target_worktree_path,
            )
            if not decision_envelope.get("ok"):
                conn.rollback()
                return decision_envelope
            warnings = list(derivation_warnings)
            decision_warnings = decision_envelope.get("warnings")
            if isinstance(decision_warnings, list):
                warnings.extend(str(item) for item in decision_warnings if isinstance(item, str))
            if slice_number is None and is_slice_complete_decision(decision):
                warnings.append(_SLICE_NUMBER_MISSING_WARNING)
            decision_data = decision_envelope.get("data", {}) or {}
            decision_payload = decision_data.get("decision", {}) or {}
            resolved_task_ref = str(
                decision_envelope.get("scope", {}).get("task_ref") or decision_payload.get("task_ref") or task_ref
            )
            if decision_data.get("idempotent"):
                conn.rollback()
                revision_row = conn.execute(
                    "SELECT revision FROM handoff_state WHERE task_ref = ?",
                    (resolved_task_ref,),
                ).fetchone()
                current_revision = int(revision_row["revision"]) if revision_row is not None else None
                # Capture payload and break so heal runs AFTER the IMMEDIATE
                # connection exits ([CON-18], [RES-01]).
                idempotent_in_lock = {
                    "decision": decision_payload,
                    "task_revision": current_revision,
                    "warnings": list(warnings),
                    "task_ref": resolved_task_ref,
                    "revision_retries": revision_retries,
                }
                break

            # T10: preserve an existing done status; still record the slice decision.
            status_row = conn.execute(
                "SELECT status FROM handoff_state WHERE task_ref = ?",
                (resolved_task_ref,),
            ).fetchone()
            existing_status = str(status_row["status"]) if status_row is not None else None
            if existing_status == "done":
                next_status = "done"
                status_preserved = "done"
            else:
                next_status = "in_progress"
                status_preserved = None

            state_envelope = _set_handoff_state_with_conn(
                conn,
                task_ref=resolved_task_ref,
                focus=focus,
                status=next_status,
                expected_revision=working_revision,
                actor=actor,
                resolved_ctx=pre_resolved_ctx,
                precomputed_target_worktree_path=precomputed_target_worktree_path,
            )
            if not state_envelope.get("ok"):
                conn.rollback()
                state_data = state_envelope.get("data", {}) or {}
                state_error = state_data.get("error")
                state_warnings = state_envelope.get("warnings")
                if isinstance(state_warnings, list):
                    warnings.extend(str(item) for item in state_warnings if isinstance(item, str))
                if state_error == "Revision conflict.":
                    latest = state_data.get("current_revision")
                    if attempt >= MAX_OPTIMISTIC_REVISION_ATTEMPTS - 1:
                        return _close_slice_revision_conflict(
                            task_ref=resolved_task_ref,
                            expected_revision=working_revision,
                            current_revision=int(latest) if latest is not None else None,
                            revision_retries=revision_retries,
                        )
                    if latest is not None:
                        working_revision = int(latest)
                    revision_retries += 1
                    continue
                return _envelope(
                    ok=False,
                    tool="close_slice",
                    data={
                        "error": state_error,
                        "state_error": state_error,
                        "decision_recorded": False,
                        "state_updated": False,
                        "current_task_md_written": False,
                        "revision_retries": revision_retries,
                    },
                    task_ref=resolved_task_ref,
                    warnings=warnings or None,
                )
            # commit-on-success is owned by _get_db_connection
            break
    else:
        # Loop exhausted without break — should be unreachable (conflicts return).
        return _close_slice_revision_conflict(
            task_ref=resolved_task_ref,
            expected_revision=working_revision,
            current_revision=working_revision,
            revision_retries=revision_retries,
        )

    if idempotent_in_lock is not None:
        # Match pure-replay heal ([OBS-08] F-HA-03): start from an empty warning
        # base so concurrent timing does not inject prep/derivation/decision
        # warnings from the abandoned write attempt into the same logical
        # idempotent outcome. Heal appends only post-commit side-effect warnings.
        heal_warnings: list[str] = []
        heal_task_ref = str(idempotent_in_lock.get("task_ref") or resolved_task_ref)
        decision_for_heal = idempotent_in_lock.get("decision")
        side_effects = _heal_close_slice_postcommit(
            task_ref=heal_task_ref,
            decision_row=decision_for_heal if isinstance(decision_for_heal, dict) else None,
            warnings=heal_warnings,
        )
        data = {
            "decision_recorded": False,
            "state_updated": False,
            "state_error": None,
            "idempotent": True,
            "current_task_md_written": False,
            "dashboard_written": False,
            "decision": decision_for_heal,
            "task_revision": idempotent_in_lock.get("task_revision"),
            "revision_retries": idempotent_in_lock.get("revision_retries", revision_retries),
        }
        _apply_close_slice_side_effect_fields(data, side_effects)
        return _envelope(
            ok=True,
            tool="close_slice",
            data=data,
            task_ref=heal_task_ref,
            artifacts=[
                {
                    "type": "current_task_md",
                    "path": "CURRENT_TASK.json",
                    "written": bool(data.get("current_task_md_written")),
                },
                {
                    "type": "dashboard",
                    "path": str(side_effects.get("dashboard_path") or "DASHBOARD.txt"),
                    "written": bool(data.get("dashboard_written")),
                },
            ],
            warnings=heal_warnings or None,
        )

    mutation = m if isinstance((m := decision_envelope.get("mutation")), dict) else {}
    # Post-commit region: decision and state writes are already committed
    # ([RES-01], [RES-02], [OBS-08]). Side-effect failures must degrade to
    # warnings + typed side_effect_failures — never raise or imply the close
    # failed (internal part a). Shared helper is also used on idempotent
    # replay so projection drift can heal ([RES-01]).
    state_data = state_envelope.get("data", {}) or {}
    active_block = state_data.get("active", {}) or {}
    state_warnings = state_envelope.get("warnings")
    if isinstance(state_warnings, list):
        warnings.extend(str(item) for item in state_warnings if isinstance(item, str))
    side_effects = _run_close_slice_postcommit_side_effects(
        resolved_task_ref=resolved_task_ref,
        rationale=rationale,
        mutation=mutation,
        warnings=warnings,
        include_embed=True,
    )
    result_data: dict = {
        "decision_recorded": True,
        "state_updated": True,
        "state_error": None,
        "decision": decision_payload,
        "task_revision": active_block.get("revision"),
        "revision_retries": revision_retries,
    }
    _apply_close_slice_side_effect_fields(result_data, side_effects)
    if status_preserved is not None:
        result_data["status_preserved"] = status_preserved
    return _envelope(
        ok=True,
        tool="close_slice",
        data=result_data,
        task_ref=resolved_task_ref,
        mutation={
            "entity": "decision",
            "operation": "close_slice",
            "task_revision": active_block.get("revision"),
        },
        artifacts=[
            {
                "type": "current_task_md",
                "path": "CURRENT_TASK.json",
                "written": bool(result_data.get("current_task_md_written")),
            },
            {
                "type": "dashboard",
                "path": str(side_effects.get("dashboard_path") or "DASHBOARD.txt"),
                "written": bool(result_data.get("dashboard_written")),
            },
        ],
        warnings=warnings or None,
    )


def find_related_prior_work(
    *,
    text: str | None = None,
    finding_id: str | None = None,
    task_ref: str | None = None,
    limit: int = 5,
    min_score: float | None = None,
) -> dict:
    """Keyword-only prior-art retrieval over concept_embeddings (advisory only).

    Lazy-imports the embeddings subpackage so the core module stays importable
    without numpy. See ``embeddings.related_prior_work`` for full semantics and
    typed empty states (AXI-5). Similarity is ranking-only (CAL-03): never a gate.
    """
    from .embeddings.related_prior_work import (
        DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE,
        find_related_prior_work_public,
    )

    return find_related_prior_work_public(
        text=text,
        finding_id=finding_id,
        task_ref=task_ref,
        limit=limit,
        min_score=DEFAULT_RELATED_PRIOR_WORK_MIN_SCORE if min_score is None else min_score,
    )
