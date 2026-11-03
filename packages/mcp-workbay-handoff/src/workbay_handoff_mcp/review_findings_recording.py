"""Record and merge operations for review findings."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from typing import TypedDict

from .concept_embed_hook import embed_finding_from_envelope
from .enums import FindingStatus
from .projection_event_dedupe import (
    claim_projection_event,
    complete_projection_event,
    fetch_projection_event,
    normalize_event_id,
)
from .review_findings_support import (
    _auto_merge_session,
    _canonical_task_ref,
    _canonicalize_finding_path,
    _current_task_revision,
    _normalize_source_task_refs,
    _validate_impact_origin,
    _validate_review_mode_or_envelope,
    _write_current_task_md_for_active_context,
    cast_details,
)
from .shared_primitives import (
    REVIEW_FINDING_SEVERITIES,
    REVIEW_FINDING_STATUSES,
    ReviewFindingDetails,
    _envelope,
    _parse_review_finding_details,
    _resolve_task_ref,
    _row_to_dict,
    _side_effect_error_class,
    _surface_side_effect_failure,
)
from .shared_schema import _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, collect_target_context_warnings

_BATCH_MAX_SIZE = 100
_FINDING_PATH_ERROR = "file_path must be monorepo-relative (no absolute or escaping paths)."
_BATCH_TOOL_NAME = "review_findings.batch_record"
_BATCH_RECEIPT_SEP = "\n"


def _finding_content_fingerprint(file_path: str, description: str) -> str:
    """Full-content identity, independent of review labels and moving line numbers.

    Review recording prefixes the original description with one category tag.
    Strip that wrapper and protocol tags, never a truncated description or a
    similarity score. Other bracketed content inside the description survives.
    Body case and whitespace are literal evidence (case-sensitive routes, code
    snippets); only the edges and the separators of stripped tags are metadata.
    """
    text = re.sub(r"\[(?:origin|impact|category)\s*[:=]\s*[^]\n]+\]\s*", "", description, flags=re.I)
    text = re.sub(r"^\s*\[format_stage\]\s*", "", text, flags=re.I)
    text = re.sub(
        r"^\s*\[(?:GAP|ANTIPATTERN|DEAD_CODE|COMPLEXITY|STYLE|FORMAT|FORMATTING|LINT|BUG)\]\s*",
        "",
        text,
        count=1,
        flags=re.I,
    )
    canonical = [_canonicalize_finding_path(file_path), text.strip()]
    return hashlib.sha256(json.dumps(canonical, ensure_ascii=False).encode("utf-8")).hexdigest()


def _reusable_review_finding(conn: sqlite3.Connection, task_ref: str, file_path: str, description: str):
    """Find an exact prior follow-up/disposition in this logical task subject.

    Fixed/integrated findings can recur as real regressions. Only a tracked
    follow-up, deferred item or wontfix disposition may suppress new rework.
    The caller owns the write transaction; reusing a row never rewrites history.
    """
    from workbay_protocol.review_impact import classify_impact

    fingerprint = _finding_content_fingerprint(file_path, description)
    rows = conn.execute(
        "SELECT * FROM review_findings WHERE task_ref = ? "
        "AND status IN ('open', 'deferred', 'wontfix') ORDER BY "
        "CASE WHEN status IN ('deferred', 'wontfix') THEN 0 ELSE 1 END, id",
        (task_ref,),
    )
    for row in rows:
        if _canonicalize_finding_path(row["file_path"]) != _canonicalize_finding_path(file_path):
            continue
        if row["status"] == "open" and classify_impact(dict(row))[0] != "follow_up":
            continue
        if _finding_content_fingerprint(row["file_path"], row["description"]) == fingerprint:
            return row
    return None


def link_prior_review_findings(task_ref: str, findings: list[dict]) -> list[dict]:
    """Read prior dispositions for raw reviewer findings; leave source tags intact."""
    linked = []
    with _get_db_connection() as conn:
        task_ref = _canonical_task_ref_for_finding_write(conn, task_ref)
        for finding in findings:
            description = f"[{finding.get('category') or 'GAP'}] {finding.get('description') or ''}"
            row = _reusable_review_finding(conn, task_ref, str(finding.get("file_path") or ""), description)
            if row is None:
                linked.append(dict(finding))
                continue
            linked.append(
                {
                    **finding,
                    "impact": "follow_up",
                    "reported_impact": finding.get("impact"),
                    "linked_finding_id": row["finding_id"],
                    "prior_disposition": row["status"],
                }
            )
    return linked


def _batch_payload_fingerprint(findings: list[dict[str, object]]) -> str:
    canonical = [
        {
            "finding_id": item.get("finding_id"),
            "severity": item.get("severity"),
            "file_path": item.get("file_path"),
            "description": item.get("description"),
            **{key: item[key] for key in ("failure_scenario", "impact_reason") if item.get(key) is not None},
        }
        for item in findings
    ]
    blob = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _encode_batch_receipt(affected_ids: list[str], fingerprint: str, results: list[dict] | None = None) -> str:
    payload = json.dumps(
        {"affected_ids": affected_ids, "fingerprint": fingerprint, "results": results},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"{_BATCH_TOOL_NAME}{_BATCH_RECEIPT_SEP}{payload}"


def _decode_batch_receipt(tool_name: object) -> tuple[list[str] | None, str | None]:
    if not isinstance(tool_name, str) or not tool_name:
        return None, None
    if tool_name == _BATCH_TOOL_NAME:
        return None, None
    prefix = _BATCH_TOOL_NAME + _BATCH_RECEIPT_SEP
    if not tool_name.startswith(prefix):
        return None, None
    try:
        payload = json.loads(tool_name[len(prefix) :])
    except (ValueError, TypeError):
        return None, None
    if not isinstance(payload, dict):
        return None, None
    ids = payload.get("affected_ids")
    fingerprint = payload.get("fingerprint")
    if not isinstance(ids, list) or not all(isinstance(item, str) for item in ids):
        return None, None
    return ids, fingerprint if isinstance(fingerprint, str) else None


def _committed_ids_for_replay(conn: sqlite3.Connection, existing_event: sqlite3.Row | None) -> list[str] | None:
    if existing_event is None:
        return None
    stored_ids, _fingerprint = _decode_batch_receipt(existing_event["tool_name"])
    if stored_ids:
        return stored_ids
    if existing_event["target_id"] is not None:
        row = conn.execute(
            "SELECT finding_id FROM review_findings WHERE id = ?",
            (existing_event["target_id"],),
        ).fetchone()
        if row is not None and row["finding_id"]:
            return [str(row["finding_id"])]
    return None


def _noop_batch_envelope(
    *,
    resolved_task_ref: str,
    event_id: str | None,
    affected_ids: list[str],
    task_revision: int | None,
    stored_event: sqlite3.Row | None = None,
) -> dict:
    results = [{"finding_id": fid, "action": "noop"} for fid in affected_ids]
    if stored_event is not None:
        try:
            payload = json.loads(str(stored_event["tool_name"]).split(_BATCH_RECEIPT_SEP, 1)[1])
            stored_results = payload.get("results") if isinstance(payload, dict) else None
            if isinstance(stored_results, list) and all(isinstance(item, dict) for item in stored_results):
                results = [{**item, "action": "noop"} for item in stored_results]
        except (IndexError, ValueError, TypeError):
            pass
    return _envelope(
        ok=True,
        tool="batch_record_review_findings",
        data={
            "task_ref": resolved_task_ref,
            "written": 0,
            "findings_recorded_count": 0,
            "results": results,
            "linked_finding_ids": affected_ids,
            "idempotent": True,
            "event_id": event_id,
        },
        task_ref=resolved_task_ref,
        entity="finding",
        mutation={
            "entity": "finding",
            "operation": "noop",
            "affected_ids": affected_ids,
            "task_revision": task_revision,
        },
    )


def _new_batch_receipt_id() -> str:
    return f"wb.batch.{uuid.uuid4().hex}"


def _store_batch_receipt(
    conn: sqlite3.Connection,
    *,
    event_id: str | None,
    affected_ids: list[str],
    fingerprint: str,
    results: list[dict] | None = None,
) -> None:
    normalized = normalize_event_id(event_id)
    if normalized is None:
        return
    conn.execute(
        "UPDATE projection_event_dedupe SET tool_name = ? WHERE event_id = ?",
        (_encode_batch_receipt(affected_ids, fingerprint, results), normalized),
    )


def load_stored_batch_receipt(event_id: str | None) -> tuple[list[str] | None, str | None]:
    """Return (affected_ids, task_ref) from durable batch receipt storage."""
    with _get_db_connection() as conn:
        row = fetch_projection_event(conn, event_id)
        if row is None:
            return None, None
        ids, _fingerprint = _decode_batch_receipt(row["tool_name"])
        stored_task = row["task_ref"]
        return ids, str(stored_task) if stored_task else None


def _canonical_task_ref_for_finding_write(conn: sqlite3.Connection, resolved_task_ref: str) -> str:
    """Remap known task refs while preserving unregistered harvester scopes."""
    return _canonical_task_ref(conn, resolved_task_ref)


def _validated_finding_path(file_path: str) -> tuple[str | None, str | None]:
    """Return the one storage identity used by every finding write path."""
    canonical = _canonicalize_finding_path(file_path)
    if canonical is None:
        return None, _FINDING_PATH_ERROR
    return canonical, None


def record_review_finding(
    session: str,
    finding_id: str,
    severity: str,
    file_path: str,
    description: str,
    details: ReviewFindingDetails | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    review_mode: str | None = None,
    impact: str | None = None,
    origin: str | None = None,
    failure_scenario: str | None = None,
    impact_reason: str | None = None,
) -> dict:
    """Public entry: delegate, then embed the finding's text fields after they commit."""
    result = _record_review_finding_impl(
        session,
        finding_id,
        severity,
        file_path,
        description,
        details=details,
        actor=actor,
        task_ref=task_ref,
        review_mode=review_mode,
        impact=impact,
        origin=origin,
        failure_scenario=failure_scenario,
        impact_reason=impact_reason,
    )
    # Capture per-field outcomes into side_effect_failures + warnings (same
    # shape as close_slice). Partial list failure: each ``failed`` is surfaced
    # independently; primary write stays ok ([DATA-13], [OBS-08], [RES-02]).
    try:
        outcomes = embed_finding_from_envelope(result)
        for idx, outcome in enumerate(outcomes):
            if outcome == "failed":
                msg = f"embed_finding_from_envelope failed during record_review_finding: outcome[{idx}] returned failed"
                _surface_side_effect_failure(
                    result,
                    effect="embed_finding_from_envelope",
                    detail=msg,
                    error_class="system",
                )
    except Exception as exc:
        msg = f"embed_finding_from_envelope failed during record_review_finding: {exc}"
        _surface_side_effect_failure(
            result,
            effect="embed_finding_from_envelope",
            detail=msg,
            error_class=_side_effect_error_class(exc),
        )
    return result


def _record_review_finding_impl(
    session: str,
    finding_id: str,
    severity: str,
    file_path: str,
    description: str,
    details: ReviewFindingDetails | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    review_mode: str | None = None,
    impact: str | None = None,
    origin: str | None = None,
    failure_scenario: str | None = None,
    impact_reason: str | None = None,
) -> dict:
    canonical_file_path, path_error = _validated_finding_path(file_path)
    if path_error is not None:
        return _envelope(
            ok=False,
            tool="record_review_finding",
            data={"error": path_error},
            entity="finding",
        )
    if severity not in REVIEW_FINDING_SEVERITIES:
        return _envelope(
            ok=False,
            tool="record_review_finding",
            data={"error": f"Invalid severity. Valid: {', '.join(sorted(REVIEW_FINDING_SEVERITIES))}"},
            entity="finding",
        )
    normalized_review_mode, error = _validate_review_mode_or_envelope(
        review_mode,
        tool="record_review_finding",
    )
    if error is not None:
        return error
    impact_error = _validate_impact_origin(impact, origin)
    if impact_error is not None:
        return _envelope(ok=False, tool="record_review_finding", data={"error": impact_error}, entity="finding")

    line_start, line_end, fix = _parse_review_finding_details(details)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        resolved_task_ref = _canonical_task_ref_for_finding_write(conn, resolved_task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        # Resolve external actor evidence before taking the transactional write lock.
        conn.execute("BEGIN IMMEDIATE")
        reused = _reusable_review_finding(conn, resolved_task_ref, canonical_file_path, description)
        if reused is not None:
            return _envelope(
                ok=True,
                tool="record_review_finding",
                data={"finding": _row_to_dict(reused), "linked_finding_id": reused["finding_id"], "reused": True},
                task_ref=resolved_task_ref,
                entity="finding",
                mutation={
                    "entity": "finding",
                    "operation": "noop",
                    "affected_ids": [reused["finding_id"]],
                    "task_revision": _current_task_revision(conn, resolved_task_ref),
                },
                warnings=warnings or None,
            )
        existing = conn.execute(_RERECORD_EXISTING_COLUMNS, (resolved_task_ref, finding_id)).fetchone()
        guard_error = _ordinary_rerecord_guard(
            existing,
            finding_id=finding_id,
            file_path=canonical_file_path,
            description=description,
            tool="record_review_finding",
        )
        if guard_error is not None:
            return guard_error
        clear_anchors = int(_should_clear_lifecycle_anchors(existing, canonical_file_path, description))
        conn.execute(
            """
            INSERT INTO review_findings (
                task_ref, lane_id, finding_id, severity, file_path, line_start, line_end, description, fix, status, review_mode, session, agent, harness, branch, commit_sha, impact, origin, failure_scenario, impact_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(task_ref, finding_id) DO UPDATE SET
                severity = excluded.severity,
                impact = COALESCE(excluded.impact, review_findings.impact),
                origin = COALESCE(excluded.origin, review_findings.origin),
                failure_scenario = COALESCE(excluded.failure_scenario, review_findings.failure_scenario),
                impact_reason = COALESCE(excluded.impact_reason, review_findings.impact_reason),
                file_path = excluded.file_path,
                line_start = excluded.line_start,
                line_end = excluded.line_end,
                description = excluded.description,
                fix = excluded.fix,
                status = 'open',
                review_mode = COALESCE(excluded.review_mode, review_findings.review_mode),
                resolved_at = NULL,
                resolution_notes = NULL,
                verification_evidence = CASE WHEN ? THEN NULL ELSE verification_evidence END,
                resolved_on_branch_at_commit = CASE WHEN ? THEN NULL ELSE resolved_on_branch_at_commit END,
                resolved_on_branch_ref = CASE WHEN ? THEN NULL ELSE resolved_on_branch_ref END,
                resolved_on_branch_at_ts = CASE WHEN ? THEN NULL ELSE resolved_on_branch_at_ts END,
                integrated_at_commit = CASE WHEN ? THEN NULL ELSE integrated_at_commit END,
                integrated_at_ref = CASE WHEN ? THEN NULL ELSE integrated_at_ref END,
                integrated_at_ts = CASE WHEN ? THEN NULL ELSE integrated_at_ts END,
                reopen_count = CASE WHEN review_findings.status <> 'open' THEN COALESCE(review_findings.reopen_count, 0) + 1 ELSE COALESCE(review_findings.reopen_count, 0) END,
                last_reopen_reason = CASE WHEN review_findings.status <> 'open' THEN 'Re-recorded via review-record.' ELSE review_findings.last_reopen_reason END,
                last_reopened_at = CASE WHEN review_findings.status <> 'open' THEN datetime('now') ELSE review_findings.last_reopened_at END,
                updated_at = datetime('now'),
                session = excluded.session,
                lane_id = COALESCE(review_findings.lane_id, excluded.lane_id),
                agent = COALESCE(review_findings.agent, excluded.agent),
                harness = COALESCE(review_findings.harness, excluded.harness),
                branch = COALESCE(review_findings.branch, excluded.branch),
                commit_sha = COALESCE(review_findings.commit_sha, excluded.commit_sha)
            """,
            (
                resolved_task_ref,
                ctx.lane_id,
                finding_id,
                severity,
                canonical_file_path,
                line_start,
                line_end,
                description,
                fix,
                normalized_review_mode,
                session,
                ctx.agent,
                ctx.harness,
                ctx.branch,
                ctx.commit_sha,
                impact,
                origin,
                failure_scenario,
                impact_reason,
                clear_anchors,
                clear_anchors,
                clear_anchors,
                clear_anchors,
                clear_anchors,
                clear_anchors,
                clear_anchors,
            ),
        )
        row = conn.execute(
            "SELECT * FROM review_findings WHERE task_ref = ? AND finding_id = ?", (resolved_task_ref, finding_id)
        ).fetchone()
        _write_current_task_md_for_active_context(conn, resolved_task_ref)
        data: dict[str, object] = {"finding": _row_to_dict(row)}
        if existing is not None and str(existing["status"]) != FindingStatus.OPEN.value:
            data["reopened"] = True
        task_revision = _current_task_revision(conn, resolved_task_ref)
        return _envelope(
            ok=True,
            tool="record_review_finding",
            data=data,
            task_ref=resolved_task_ref,
            entity="finding",
            mutation={
                "entity": "finding",
                "operation": "upsert",
                "affected_ids": [finding_id],
                "task_revision": task_revision,
            },
            warnings=warnings or None,
        )


class BatchFindingItem(TypedDict, total=False):
    finding_id: str
    severity: str
    file_path: str
    description: str
    review_mode: str | None
    details: ReviewFindingDetails | None
    merged_from_json: str | None
    status: str
    # Source-row provenance for merge copies (T18). When set, the merge path
    # stamps these instead of the coordinator write-context HEAD so later
    # resolve does not see a false diverged relation.
    branch: str | None
    commit_sha: str | None
    resolved_at: str | None
    resolution_notes: str | None
    verification_evidence: str | None
    resolved_on_branch_at_commit: str | None
    resolved_on_branch_ref: str | None
    resolved_on_branch_at_ts: str | None
    integrated_at_commit: str | None
    integrated_at_ref: str | None
    integrated_at_ts: str | None
    reopen_count: int | None
    last_reopen_reason: str | None
    last_reopened_at: str | None
    impact: str | None
    origin: str | None
    failure_scenario: str | None
    impact_reason: str | None


def _is_merge_copy(item: BatchFindingItem) -> bool:
    merged_from = item.get("merged_from_json")
    return isinstance(merged_from, str) and bool(merged_from.strip())


def _merge_copy_status(item: BatchFindingItem) -> str:
    """Return the validated status for a merge-copy item.

    A missing/None status means the source row predates the status column and
    legitimately defaults to ``open``. Any other value must be a known
    ``FindingStatus``; unknown strings are a caller bug and raise instead of
    being silently coerced to ``open``.
    """
    status = item.get("status")
    if status is None:
        return FindingStatus.OPEN.value
    if isinstance(status, str) and status in REVIEW_FINDING_STATUSES:
        return status
    raise ValueError(f"Invalid merge-copy status {status!r}. Valid: {', '.join(sorted(REVIEW_FINDING_STATUSES))}")


_RERECORD_EXISTING_COLUMNS = """
    SELECT status, file_path, description, verification_evidence,
           resolved_on_branch_at_commit, resolved_on_branch_ref, resolved_on_branch_at_ts,
           integrated_at_commit, integrated_at_ref, integrated_at_ts,
           merged_from_json, branch, commit_sha
    FROM review_findings WHERE task_ref = ? AND finding_id = ?
"""


def _same_merge_source(existing_raw: object, incoming_raw: object) -> bool:
    """True when two merged_from_json payloads identify the same source row.

    Sources are compared by (task_ref, finding_id) identity; unparseable payloads
    fall back to raw string equality. Missing payloads never match.
    """
    if not existing_raw or not incoming_raw:
        return False
    try:
        existing = json.loads(existing_raw) if isinstance(existing_raw, str) else None
        incoming = json.loads(incoming_raw) if isinstance(incoming_raw, str) else None
    except ValueError:
        return str(existing_raw) == str(incoming_raw)
    if isinstance(existing, dict) and isinstance(incoming, dict):
        return (existing.get("task_ref"), existing.get("finding_id")) == (
            incoming.get("task_ref"),
            incoming.get("finding_id"),
        )
    return existing == incoming


def _merge_provenance_overwrite_blocked(
    existing,
    *,
    incoming_merged_from_json: object,
    merge_branch: str | None,
    merge_commit_sha: str | None,
) -> bool:
    """S4-A-01: guard merge provenance re-inheritance on open target rows.

    A re-merge (or a colliding finding_id merged from a *different* source row)
    must not overwrite an open target row's already-correct branch/commit_sha.
    Re-inheritance stays allowed only when the incoming item's source IS the
    prior source of the existing row (the T18 wrong-cwd-stamp repair path).
    """
    if existing is None:
        return False
    if str(existing["status"]) != FindingStatus.OPEN.value:
        return False
    provenance_differs = (existing["branch"] or None) != (merge_branch or None) or (existing["commit_sha"] or None) != (
        merge_commit_sha or None
    )
    if not provenance_differs:
        return False
    return not _same_merge_source(existing["merged_from_json"], incoming_merged_from_json)


def _finding_material_changed(
    existing_file_path: str | None,
    existing_description: str | None,
    incoming_file_path: str,
    incoming_description: str,
) -> bool:
    return (existing_file_path or "") != incoming_file_path or (existing_description or "") != incoming_description


def _finding_has_lifecycle_anchors(row) -> bool:
    if row is None:
        return False
    return any(
        row[column] not in (None, "")
        for column in (
            "verification_evidence",
            "resolved_on_branch_at_commit",
            "resolved_on_branch_ref",
            "resolved_on_branch_at_ts",
            "integrated_at_commit",
            "integrated_at_ref",
            "integrated_at_ts",
        )
    )


def _should_clear_lifecycle_anchors(existing, file_path: str, description: str) -> bool:
    if existing is None:
        return False
    status = str(existing["status"])
    if status != FindingStatus.OPEN.value:
        return True
    return _finding_material_changed(
        existing["file_path"], existing["description"], file_path, description
    ) and _finding_has_lifecycle_anchors(existing)


def _ordinary_rerecord_error(
    *,
    tool: str,
    finding_id: str,
    status: str,
) -> dict:
    return _envelope(
        ok=False,
        tool=tool,
        data={
            "error": (
                f"Finding {finding_id!r} is terminal (status={status!r}) with different "
                "file_path or description. Use a new finding_id."
            )
        },
        entity="finding",
    )


def _ordinary_rerecord_guard(existing, *, finding_id: str, file_path: str, description: str, tool: str) -> dict | None:
    if existing is None:
        return None
    status = str(existing["status"])
    if status == FindingStatus.OPEN.value:
        return None
    if _finding_material_changed(existing["file_path"], existing["description"], file_path, description):
        return _ordinary_rerecord_error(tool=tool, finding_id=finding_id, status=status)
    return None


def _validate_ordinary_batch_rerecords(
    conn: sqlite3.Connection,
    *,
    resolved_task_ref: str,
    findings: list[BatchFindingItem],
) -> dict | None:
    for index, item in enumerate(findings):
        if _is_merge_copy(item):
            continue
        finding_id = item.get("finding_id")
        if not finding_id:
            continue
        if (
            _reusable_review_finding(
                conn, resolved_task_ref, str(item.get("file_path") or ""), str(item.get("description") or "")
            )
            is not None
        ):
            continue
        existing = conn.execute(_RERECORD_EXISTING_COLUMNS, (resolved_task_ref, finding_id)).fetchone()
        error = _ordinary_rerecord_guard(
            existing,
            finding_id=finding_id,
            file_path=str(item.get("file_path") or ""),
            description=str(item.get("description") or ""),
            tool="batch_record_review_findings",
        )
        if error is not None:
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} (finding_id={finding_id!r}): {error['data']['error']}"},
                entity="finding",
            )
    return None


def _batch_record_review_findings_in_conn(
    conn: sqlite3.Connection,
    *,
    resolved_task_ref: str,
    ctx,
    session: str,
    findings: list[BatchFindingItem],
) -> list[dict[str, object]]:
    """Validated batch upsert loop on a caller-owned connection."""
    results: list[dict[str, object]] = []
    for item in findings:
        finding_id = item["finding_id"]
        severity = item["severity"]
        file_path, path_error = _validated_finding_path(item["file_path"])
        if path_error is not None:
            raise ValueError(path_error)  # validated by the public batch boundary
        description = item["description"]
        normalized_review_mode, _ = _validate_review_mode_or_envelope(
            item.get("review_mode"),
            tool="batch_record_review_findings",
        )
        line_start, line_end, fix = _parse_review_finding_details(item.get("details"))

        if not _is_merge_copy(item):
            reused = _reusable_review_finding(conn, resolved_task_ref, file_path, description)
            if reused is not None:
                results.append(
                    {
                        "finding_id": finding_id,
                        "linked_finding_id": reused["finding_id"],
                        "action": "reused",
                        "prior_disposition": reused["status"],
                    }
                )
                continue
        existing = conn.execute(_RERECORD_EXISTING_COLUMNS, (resolved_task_ref, finding_id)).fetchone()

        if _is_merge_copy(item):
            merge_status = _merge_copy_status(item)
            # T18: inherit source-row branch/commit_sha so resolve does not
            # treat a coordinator-cwd HEAD stamp as diverged provenance.
            merge_branch = item.get("branch") if item.get("branch") is not None else ctx.branch
            merge_commit_sha = item.get("commit_sha") if item.get("commit_sha") is not None else ctx.commit_sha
            incoming_merged_from_json = item.get("merged_from_json")
            # S4-A-01: an open target row keeps its provenance (and source link)
            # when the incoming merge item comes from a different source row —
            # only the prior source may repair/re-inherit branch/commit_sha.
            if _merge_provenance_overwrite_blocked(
                existing,
                incoming_merged_from_json=incoming_merged_from_json,
                merge_branch=merge_branch,
                merge_commit_sha=merge_commit_sha,
            ):
                merge_branch = None
                merge_commit_sha = None
                incoming_merged_from_json = None
            # Disposition preservation (implementation note): when the existing
            # coordinator row already carries a non-open local disposition
            # (e.g. the coordinator marked the merged copy fixed/deferred),
            # a re-merge must NOT clobber it back to the source's status —
            # the coordinator's status and resolution metadata win. Only an
            # open (or absent) target row adopts the source's disposition.
            # Open rows also re-inherit source provenance on re-merge so a
            # prior wrong cwd stamp can be repaired without repair_provenance.
            conn.execute(
                """
                INSERT INTO review_findings (
                    task_ref, lane_id, finding_id, severity, file_path, line_start, line_end,
                    description, fix, status, review_mode, session, agent, harness, branch, commit_sha,
                    merged_from_json, resolved_at, resolution_notes, verification_evidence,
                    resolved_on_branch_at_commit, resolved_on_branch_ref, resolved_on_branch_at_ts,
                    integrated_at_commit, integrated_at_ref, integrated_at_ts,
                    reopen_count, last_reopen_reason, last_reopened_at, impact, origin, failure_scenario, impact_reason,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(task_ref, finding_id) DO UPDATE SET
                    severity = excluded.severity,
                    impact = COALESCE(excluded.impact, review_findings.impact),
                    origin = COALESCE(excluded.origin, review_findings.origin),
                failure_scenario = COALESCE(excluded.failure_scenario, review_findings.failure_scenario),
                impact_reason = COALESCE(excluded.impact_reason, review_findings.impact_reason),
                    file_path = excluded.file_path,
                    line_start = excluded.line_start,
                    line_end = excluded.line_end,
                    description = excluded.description,
                    fix = excluded.fix,
                    status = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.status ELSE excluded.status END,
                    review_mode = COALESCE(excluded.review_mode, review_findings.review_mode),
                    resolved_at = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.resolved_at ELSE excluded.resolved_at END,
                    resolution_notes = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.resolution_notes ELSE excluded.resolution_notes END,
                    verification_evidence = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.verification_evidence ELSE excluded.verification_evidence END,
                    resolved_on_branch_at_commit = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.resolved_on_branch_at_commit ELSE excluded.resolved_on_branch_at_commit END,
                    resolved_on_branch_ref = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.resolved_on_branch_ref ELSE excluded.resolved_on_branch_ref END,
                    resolved_on_branch_at_ts = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.resolved_on_branch_at_ts ELSE excluded.resolved_on_branch_at_ts END,
                    integrated_at_commit = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.integrated_at_commit ELSE excluded.integrated_at_commit END,
                    integrated_at_ref = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.integrated_at_ref ELSE excluded.integrated_at_ref END,
                    integrated_at_ts = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.integrated_at_ts ELSE excluded.integrated_at_ts END,
                    reopen_count = CASE WHEN review_findings.status <> 'open'
                        THEN COALESCE(review_findings.reopen_count, 0)
                        ELSE MAX(COALESCE(review_findings.reopen_count, 0), COALESCE(excluded.reopen_count, 0)) END,
                    last_reopen_reason = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.last_reopen_reason
                        ELSE COALESCE(excluded.last_reopen_reason, review_findings.last_reopen_reason) END,
                    last_reopened_at = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.last_reopened_at
                        ELSE COALESCE(excluded.last_reopened_at, review_findings.last_reopened_at) END,
                    updated_at = datetime('now'),
                    session = excluded.session,
                    lane_id = COALESCE(review_findings.lane_id, excluded.lane_id),
                    agent = COALESCE(review_findings.agent, excluded.agent),
                    harness = COALESCE(review_findings.harness, excluded.harness),
                    branch = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.branch ELSE COALESCE(excluded.branch, review_findings.branch) END,
                    commit_sha = CASE WHEN review_findings.status <> 'open'
                        THEN review_findings.commit_sha ELSE COALESCE(excluded.commit_sha, review_findings.commit_sha) END,
                    merged_from_json = COALESCE(excluded.merged_from_json, review_findings.merged_from_json)
                """,
                (
                    resolved_task_ref,
                    ctx.lane_id,
                    finding_id,
                    severity,
                    file_path,
                    line_start,
                    line_end,
                    description,
                    fix,
                    merge_status,
                    normalized_review_mode,
                    session,
                    ctx.agent,
                    ctx.harness,
                    merge_branch,
                    merge_commit_sha,
                    incoming_merged_from_json,
                    item.get("resolved_at"),
                    item.get("resolution_notes"),
                    item.get("verification_evidence"),
                    item.get("resolved_on_branch_at_commit"),
                    item.get("resolved_on_branch_ref"),
                    item.get("resolved_on_branch_at_ts"),
                    item.get("integrated_at_commit"),
                    item.get("integrated_at_ref"),
                    item.get("integrated_at_ts"),
                    item.get("reopen_count") or 0,
                    item.get("last_reopen_reason"),
                    item.get("last_reopened_at"),
                    item.get("impact"),
                    item.get("origin"),
                    item.get("failure_scenario"),
                    item.get("impact_reason"),
                ),
            )
        else:
            clear_anchors = int(_should_clear_lifecycle_anchors(existing, file_path, description))
            conn.execute(
                """
                INSERT INTO review_findings (
                    task_ref, lane_id, finding_id, severity, file_path, line_start, line_end,
                    description, fix, status, review_mode, session, agent, harness, branch, commit_sha,
                    merged_from_json, impact, origin, failure_scenario, impact_reason, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(task_ref, finding_id) DO UPDATE SET
                    severity = excluded.severity,
                    impact = COALESCE(excluded.impact, review_findings.impact),
                    origin = COALESCE(excluded.origin, review_findings.origin),
                failure_scenario = COALESCE(excluded.failure_scenario, review_findings.failure_scenario),
                impact_reason = COALESCE(excluded.impact_reason, review_findings.impact_reason),
                    file_path = excluded.file_path,
                    line_start = excluded.line_start,
                    line_end = excluded.line_end,
                    description = excluded.description,
                    fix = excluded.fix,
                    status = 'open',
                    review_mode = COALESCE(excluded.review_mode, review_findings.review_mode),
                    resolved_at = NULL,
                    resolution_notes = NULL,
                    verification_evidence = CASE WHEN ? THEN NULL ELSE verification_evidence END,
                    resolved_on_branch_at_commit = CASE WHEN ? THEN NULL ELSE resolved_on_branch_at_commit END,
                    resolved_on_branch_ref = CASE WHEN ? THEN NULL ELSE resolved_on_branch_ref END,
                    resolved_on_branch_at_ts = CASE WHEN ? THEN NULL ELSE resolved_on_branch_at_ts END,
                    integrated_at_commit = CASE WHEN ? THEN NULL ELSE integrated_at_commit END,
                    integrated_at_ref = CASE WHEN ? THEN NULL ELSE integrated_at_ref END,
                    integrated_at_ts = CASE WHEN ? THEN NULL ELSE integrated_at_ts END,
                    reopen_count = CASE WHEN review_findings.status <> 'open'
                        THEN COALESCE(review_findings.reopen_count, 0) + 1
                        ELSE COALESCE(review_findings.reopen_count, 0) END,
                    last_reopen_reason = CASE WHEN review_findings.status <> 'open'
                        THEN 'Re-recorded via review-record.'
                        ELSE review_findings.last_reopen_reason END,
                    last_reopened_at = CASE WHEN review_findings.status <> 'open'
                        THEN datetime('now')
                        ELSE review_findings.last_reopened_at END,
                    updated_at = datetime('now'),
                    session = excluded.session,
                    lane_id = COALESCE(review_findings.lane_id, excluded.lane_id),
                    agent = COALESCE(review_findings.agent, excluded.agent),
                    harness = COALESCE(review_findings.harness, excluded.harness),
                    branch = COALESCE(review_findings.branch, excluded.branch),
                    commit_sha = COALESCE(review_findings.commit_sha, excluded.commit_sha),
                    merged_from_json = COALESCE(excluded.merged_from_json, review_findings.merged_from_json)
                """,
                (
                    resolved_task_ref,
                    ctx.lane_id,
                    finding_id,
                    severity,
                    file_path,
                    line_start,
                    line_end,
                    description,
                    fix,
                    normalized_review_mode,
                    session,
                    ctx.agent,
                    ctx.harness,
                    ctx.branch,
                    ctx.commit_sha,
                    item.get("merged_from_json"),
                    item.get("impact"),
                    item.get("origin"),
                    item.get("failure_scenario"),
                    item.get("impact_reason"),
                    clear_anchors,
                    clear_anchors,
                    clear_anchors,
                    clear_anchors,
                    clear_anchors,
                    clear_anchors,
                    clear_anchors,
                ),
            )

        item_result: dict[str, object] = {"finding_id": finding_id, "action": "inserted"}
        if existing is not None:
            item_result["action"] = "updated"
            if not _is_merge_copy(item) and str(existing["status"]) != FindingStatus.OPEN.value:
                item_result["reopened"] = True
        results.append(item_result)
    return results


def _retire_merged_source_rows(
    conn: sqlite3.Connection,
    *,
    source_ids: list[int],
    target_task_ref: str,
    session: str,
) -> int:
    if not source_ids:
        return 0
    note = f"merged into {target_task_ref} @ {session}"
    return _supersede_review_findings(conn, source_ids=source_ids, resolution_note=note)


def _supersede_review_findings(
    conn: sqlite3.Connection,
    *,
    source_ids: list[int],
    resolution_note: str,
) -> int:
    if not source_ids:
        return 0
    placeholders = ",".join(["?"] * len(source_ids))
    cursor = conn.execute(
        f"""
        UPDATE review_findings
        SET status = ?,
            resolution_notes = CASE
                WHEN resolution_notes IS NULL OR trim(resolution_notes) = '' THEN ?
                WHEN instr(resolution_notes, ?) > 0 THEN resolution_notes
                ELSE resolution_notes || ' | ' || ?
            END,
            updated_at = datetime('now')
        WHERE id IN ({placeholders}) AND status <> ?
        """,
        (
            FindingStatus.SUPERSEDED.value,
            resolution_note,
            resolution_note,
            resolution_note,
            *source_ids,
            FindingStatus.SUPERSEDED.value,
        ),
    )
    return int(cursor.rowcount)


def batch_record_review_findings(
    session: str,
    findings: list[BatchFindingItem],
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    event_id: str | None = None,
) -> dict:
    """Record or reopen multiple review findings in a single atomic write."""
    if len(findings) > _BATCH_MAX_SIZE:
        return _envelope(
            ok=False,
            tool="batch_record_review_findings",
            data={"error": f"Batch exceeds maximum size of {_BATCH_MAX_SIZE} items."},
            entity="finding",
        )
    if not findings:
        return _envelope(
            ok=True,
            tool="batch_record_review_findings",
            data={
                "task_ref": task_ref or "unknown",
                "written": 0,
                "findings_recorded_count": 0,
                "results": [],
            },
            task_ref=task_ref,
            entity="finding",
        )

    canonical_findings: list[BatchFindingItem] = []
    for index, item in enumerate(findings):
        finding_id = item.get("finding_id")
        if not finding_id:
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} is missing finding_id."},
                entity="finding",
            )
        severity = item.get("severity")
        if severity not in REVIEW_FINDING_SEVERITIES:
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={
                    "error": f"Item {index} (finding_id={finding_id!r}): Invalid severity. Valid: {', '.join(sorted(REVIEW_FINDING_SEVERITIES))}",
                },
                entity="finding",
            )
        canonical_file_path, path_error = _validated_finding_path(item.get("file_path", ""))
        if path_error is not None:
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} (finding_id={finding_id!r}): {path_error}"},
                entity="finding",
            )
        if not item.get("description"):
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} (finding_id={finding_id!r}): missing description."},
                entity="finding",
            )
        _, error = _validate_review_mode_or_envelope(
            item.get("review_mode"),
            tool="batch_record_review_findings",
        )
        if error is not None:
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} (finding_id={finding_id!r}): {error['data']['error']}"},
                entity="finding",
            )
        impact_error = _validate_impact_origin(item.get("impact"), item.get("origin"))
        if impact_error is not None:
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} (finding_id={finding_id!r}): {impact_error}"},
                entity="finding",
            )
        if _is_merge_copy(item):
            try:
                _merge_copy_status(item)
            except ValueError as exc:
                return _envelope(
                    ok=False,
                    tool="batch_record_review_findings",
                    data={"error": f"Item {index} (finding_id={finding_id!r}): {exc}"},
                    entity="finding",
                )
        canonical_item = dict(item)
        canonical_item["file_path"] = canonical_file_path
        canonical_findings.append(canonical_item)

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        resolved_task_ref = _canonical_task_ref_for_finding_write(conn, resolved_task_ref)
        caller_event_id = normalize_event_id(event_id)
        existing_event = fetch_projection_event(conn, caller_event_id)
        if existing_event is not None:
            _, fingerprint = _decode_batch_receipt(existing_event["tool_name"])
            if fingerprint is not None and fingerprint != _batch_payload_fingerprint(canonical_findings):
                return _envelope(ok=False, tool="batch_record_review_findings",
                                 data={"error": "batch_receipt_payload_mismatch", "event_id": caller_event_id},
                                 task_ref=resolved_task_ref, entity="finding")
            task_revision = _current_task_revision(conn, resolved_task_ref)
            stored_ids = _committed_ids_for_replay(conn, existing_event)
            if stored_ids is None:
                return _envelope(
                    ok=False,
                    tool="batch_record_review_findings",
                    data={
                        "error": "event_id already committed; original affected_ids are unavailable.",
                        "event_id": caller_event_id,
                    },
                    task_ref=resolved_task_ref,
                    entity="finding",
                )
            return _noop_batch_envelope(
                resolved_task_ref=resolved_task_ref,
                event_id=caller_event_id,
                affected_ids=stored_ids,
                task_revision=task_revision,
                stored_event=existing_event,
            )
        durable_event_id = caller_event_id or _new_batch_receipt_id()
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        guard_error = _validate_ordinary_batch_rerecords(
            conn, resolved_task_ref=resolved_task_ref, findings=canonical_findings
        )
        if guard_error is not None:
            return guard_error
        claimed = claim_projection_event(
            conn,
            event_id=durable_event_id,
            tool_name=_BATCH_TOOL_NAME,
            target_table="review_findings",
            task_ref=resolved_task_ref,
        )
        if claimed is not None:
            task_revision = _current_task_revision(conn, resolved_task_ref)
            stored_ids = _committed_ids_for_replay(conn, claimed)
            if stored_ids is None:
                return _envelope(
                    ok=False,
                    tool="batch_record_review_findings",
                    data={
                        "error": "event_id already committed; original affected_ids are unavailable.",
                        "event_id": durable_event_id,
                    },
                    task_ref=resolved_task_ref,
                    entity="finding",
                )
            return _noop_batch_envelope(
                resolved_task_ref=resolved_task_ref,
                event_id=durable_event_id,
                affected_ids=stored_ids,
                task_revision=task_revision,
                stored_event=claimed,
            )
        results = _batch_record_review_findings_in_conn(
            conn,
            resolved_task_ref=resolved_task_ref,
            ctx=ctx,
            session=session,
            findings=canonical_findings,
        )
        _write_current_task_md_for_active_context(conn, resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)
        first_id = None
        if results:
            first = results[0]
            finding_id = (
                (first.get("linked_finding_id") or first.get("finding_id")) if isinstance(first, dict) else None
            )
            if finding_id:
                row = conn.execute(
                    "SELECT id FROM review_findings WHERE task_ref = ? AND finding_id = ?",
                    (resolved_task_ref, finding_id),
                ).fetchone()
                if row is not None:
                    first_id = int(row[0])
        complete_projection_event(
            conn,
            event_id=durable_event_id,
            target_table="review_findings",
            target_id=first_id,
            task_ref=resolved_task_ref,
        )
        affected_ids = [str(item.get("linked_finding_id") or item["finding_id"]) for item in results]
        _store_batch_receipt(
            conn,
            event_id=durable_event_id,
            affected_ids=affected_ids,
            fingerprint=_batch_payload_fingerprint(canonical_findings),
            results=results,
        )

    affected_ids = [str(item.get("linked_finding_id") or item["finding_id"]) for item in results]
    return _envelope(
        ok=True,
        tool="batch_record_review_findings",
        data={
            "task_ref": resolved_task_ref,
            "written": len(findings),
            "findings_recorded_count": len(findings),
            "results": results,
            "created_count": sum(item["action"] == "inserted" for item in results),
            "reused_count": sum(item["action"] == "reused" for item in results),
            "linked_finding_ids": affected_ids,
            "event_id": durable_event_id,
        },
        task_ref=resolved_task_ref,
        entity="finding",
        mutation={
            "entity": "finding",
            "operation": "batch_upsert",
            "affected_ids": affected_ids,
            "task_revision": task_revision,
        },
        warnings=warnings or None,
    )


def _merge_row_to_batch_item(row: sqlite3.Row) -> BatchFindingItem:
    source_triple = {
        "task_ref": str(row["task_ref"]),
        "session": str(row["session"]),
        "finding_id": str(row["finding_id"]),
    }
    details_payload: ReviewFindingDetails | None = None
    if row["line_start"] is not None or row["line_end"] is not None or row["fix"] is not None:
        details_payload = cast_details(line_start=row["line_start"], line_end=row["line_end"], fix=row["fix"])
    # Prefer source-row keys; keys() check keeps older SELECT shapes tolerant.
    keys = set(row.keys()) if hasattr(row, "keys") else set()
    source_branch = str(row["branch"]) if "branch" in keys and row["branch"] else None
    source_commit = str(row["commit_sha"]) if "commit_sha" in keys and row["commit_sha"] else None
    return {
        "finding_id": str(row["finding_id"]),
        "severity": str(row["severity"]),
        "file_path": str(row["file_path"]),
        "description": str(row["description"]),
        "review_mode": str(row["review_mode"]) if row["review_mode"] else None,
        "details": details_payload,
        "merged_from_json": json.dumps(source_triple, sort_keys=True),
        "status": str(row["status"]),
        # v45: a merge copy keeps the source's classification so a follow-up stays a follow-up.
        "impact": str(row["impact"]) if "impact" in keys and row["impact"] else None,
        "origin": str(row["origin"]) if "origin" in keys and row["origin"] else None,
        "failure_scenario": row["failure_scenario"] if "failure_scenario" in keys else None,
        "impact_reason": row["impact_reason"] if "impact_reason" in keys else None,
        "branch": source_branch,
        "commit_sha": source_commit,
        "resolved_at": row["resolved_at"],
        "resolution_notes": row["resolution_notes"],
        "verification_evidence": row["verification_evidence"],
        "resolved_on_branch_at_commit": row["resolved_on_branch_at_commit"],
        "resolved_on_branch_ref": row["resolved_on_branch_ref"],
        "resolved_on_branch_at_ts": row["resolved_on_branch_at_ts"],
        "integrated_at_commit": row["integrated_at_commit"],
        "integrated_at_ref": row["integrated_at_ref"],
        "integrated_at_ts": row["integrated_at_ts"],
        "reopen_count": row["reopen_count"],
        "last_reopen_reason": row["last_reopen_reason"],
        "last_reopened_at": row["last_reopened_at"],
    }


def merge_review_findings(
    session: str | None,
    source_task_refs: list[str],
    target_task_ref: str,
    actor: WriteActor | None = None,
    retire_sources: bool = True,
) -> dict:
    """Merge source task_refs' review findings into a coordinator target task_ref."""
    clean_sources = _normalize_source_task_refs(source_task_refs)
    if not clean_sources:
        return _envelope(
            ok=False,
            tool="merge_review_findings",
            data={"error": "source_task_refs must be a non-empty list of task_ref strings."},
            entity="finding",
        )
    if not isinstance(target_task_ref, str) or not target_task_ref.strip():
        return _envelope(
            ok=False,
            tool="merge_review_findings",
            data={"error": "target_task_ref must be a non-empty string."},
            entity="finding",
        )
    target = target_task_ref.strip()
    effective_session = session.strip() if isinstance(session, str) and session.strip() else _auto_merge_session(target)

    placeholders = ",".join(["?"] * len(clean_sources))
    with _get_db_connection() as conn:
        resolved_target = _resolve_task_ref(conn, target)
        target = _canonical_task_ref_for_finding_write(conn, resolved_target)
        clean_sources = [_canonical_task_ref_for_finding_write(conn, source) for source in clean_sources]
        clean_sources = _normalize_source_task_refs(clean_sources)
        if target in set(clean_sources):
            return _envelope(
                ok=False,
                tool="merge_review_findings",
                data={"error": "target_task_ref must not appear in source_task_refs."},
                entity="finding",
            )
        placeholders = ",".join(["?"] * len(clean_sources))
        rows = conn.execute(
            f"""
            SELECT id, task_ref, session, finding_id, severity, file_path,
                   line_start, line_end, description, fix, review_mode, status,
                   branch, commit_sha,
                   resolved_at, resolution_notes, verification_evidence,
                   resolved_on_branch_at_commit, resolved_on_branch_ref, resolved_on_branch_at_ts,
                   integrated_at_commit, integrated_at_ref, integrated_at_ts,
                   reopen_count, last_reopen_reason, last_reopened_at, impact, origin, failure_scenario, impact_reason
            FROM review_findings
            WHERE task_ref IN ({placeholders})
            ORDER BY task_ref, id
            """,
            tuple(clean_sources),
        ).fetchall()

        if not rows:
            return _envelope(
                ok=False,
                tool="merge_review_findings",
                data={
                    "error": "no findings available to merge from the given source_task_refs.",
                    "source_task_refs": clean_sources,
                },
                entity="finding",
            )

        active_rows = [row for row in rows if str(row["status"]) != FindingStatus.SUPERSEDED.value]
        already_retired = len(rows) - len(active_rows)
        if not active_rows:
            return _envelope(
                ok=True,
                tool="merge_review_findings",
                data={
                    "task_ref": target,
                    "session": effective_session,
                    "source_task_refs": clean_sources,
                    "written": 0,
                    "already_retired": already_retired,
                    "retired": 0,
                    "results": [],
                },
                task_ref=target,
                entity="finding",
                mutation={
                    "entity": "finding",
                    "operation": "merge",
                    "affected_ids": [],
                    "task_revision": _current_task_revision(conn, target),
                },
            )

        items = [_merge_row_to_batch_item(row) for row in active_rows]
        resolved_task_ref = target
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        # No ordinary-rerecord guard here: merge items are all merge-copies
        # (they carry merged_from_json), which the guard skips by design, so it
        # would be a no-op. The disposition-preservation ON CONFLICT branch in
        # _batch_record_review_findings_in_conn handles terminal coordinator rows.
        results = _batch_record_review_findings_in_conn(
            conn,
            resolved_task_ref=resolved_task_ref,
            ctx=ctx,
            session=effective_session,
            findings=items,
        )
        retired = 0
        if retire_sources:
            retired = _retire_merged_source_rows(
                conn,
                source_ids=[int(row["id"]) for row in active_rows],
                target_task_ref=target,
                session=effective_session,
            )
        _write_current_task_md_for_active_context(conn, resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)

    return _envelope(
        ok=True,
        tool="merge_review_findings",
        data={
            "task_ref": target,
            "session": effective_session,
            "source_task_refs": clean_sources,
            "written": len(items),
            "retired": retired,
            "already_retired": already_retired,
            "results": results,
        },
        task_ref=target,
        entity="finding",
        mutation={
            "entity": "finding",
            "operation": "merge",
            "affected_ids": [item["finding_id"] for item in items],
            "task_revision": task_revision,
        },
        warnings=warnings or None,
    )
