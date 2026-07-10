"""Record and merge operations for review findings."""

from __future__ import annotations

import json
import sqlite3
from typing import TypedDict

from .concept_embed_hook import embed_finding_from_envelope
from .enums import FindingStatus
from .review_findings_support import (
    _auto_merge_session,
    _current_task_revision,
    _normalize_source_task_refs,
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
)
from .shared_schema import _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, collect_target_context_warnings

_BATCH_MAX_SIZE = 100


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
    )
    embed_finding_from_envelope(result)
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
) -> dict:
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

    line_start, line_end, fix = _parse_review_finding_details(details)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        existing = conn.execute(_RERECORD_EXISTING_COLUMNS, (resolved_task_ref, finding_id)).fetchone()
        guard_error = _ordinary_rerecord_guard(
            existing,
            finding_id=finding_id,
            file_path=file_path,
            description=description,
            tool="record_review_finding",
        )
        if guard_error is not None:
            return guard_error
        clear_anchors = int(_should_clear_lifecycle_anchors(existing, file_path, description))
        conn.execute(
            """
            INSERT INTO review_findings (
                task_ref, lane_id, finding_id, severity, file_path, line_start, line_end, description, fix, status, review_mode, session, agent, harness, branch, commit_sha, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(task_ref, finding_id) DO UPDATE SET
                severity = excluded.severity,
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
           integrated_at_commit, integrated_at_ref, integrated_at_ts
    FROM review_findings WHERE task_ref = ? AND finding_id = ?
"""


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
        file_path = item["file_path"]
        description = item["description"]
        normalized_review_mode, _ = _validate_review_mode_or_envelope(
            item.get("review_mode"),
            tool="batch_record_review_findings",
        )
        line_start, line_end, fix = _parse_review_finding_details(item.get("details"))

        existing = conn.execute(_RERECORD_EXISTING_COLUMNS, (resolved_task_ref, finding_id)).fetchone()

        if _is_merge_copy(item):
            merge_status = _merge_copy_status(item)
            # Disposition preservation (implementation note): when the existing
            # coordinator row already carries a non-open local disposition
            # (e.g. the coordinator marked the merged copy fixed/deferred),
            # a re-merge must NOT clobber it back to the source's status —
            # the coordinator's status and resolution metadata win. Only an
            # open (or absent) target row adopts the source's disposition.
            conn.execute(
                """
                INSERT INTO review_findings (
                    task_ref, lane_id, finding_id, severity, file_path, line_start, line_end,
                    description, fix, status, review_mode, session, agent, harness, branch, commit_sha,
                    merged_from_json, resolved_at, resolution_notes, verification_evidence,
                    resolved_on_branch_at_commit, resolved_on_branch_ref, resolved_on_branch_at_ts,
                    integrated_at_commit, integrated_at_ref, integrated_at_ts,
                    reopen_count, last_reopen_reason, last_reopened_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(task_ref, finding_id) DO UPDATE SET
                    severity = excluded.severity,
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
                    merge_status,
                    normalized_review_mode,
                    session,
                    ctx.agent,
                    ctx.harness,
                    ctx.branch,
                    ctx.commit_sha,
                    item.get("merged_from_json"),
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
                ),
            )
        else:
            clear_anchors = int(_should_clear_lifecycle_anchors(existing, file_path, description))
            conn.execute(
                """
                INSERT INTO review_findings (
                    task_ref, lane_id, finding_id, severity, file_path, line_start, line_end,
                    description, fix, status, review_mode, session, agent, harness, branch, commit_sha,
                    merged_from_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(task_ref, finding_id) DO UPDATE SET
                    severity = excluded.severity,
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
            data={"task_ref": task_ref or "unknown", "written": 0, "results": []},
            task_ref=task_ref,
            entity="finding",
        )

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
        if not item.get("file_path"):
            return _envelope(
                ok=False,
                tool="batch_record_review_findings",
                data={"error": f"Item {index} (finding_id={finding_id!r}): missing file_path."},
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

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        guard_error = _validate_ordinary_batch_rerecords(conn, resolved_task_ref=resolved_task_ref, findings=findings)
        if guard_error is not None:
            return guard_error
        results = _batch_record_review_findings_in_conn(
            conn,
            resolved_task_ref=resolved_task_ref,
            ctx=ctx,
            session=session,
            findings=findings,
        )
        _write_current_task_md_for_active_context(conn, resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)

    affected_ids = [item["finding_id"] for item in findings]
    return _envelope(
        ok=True,
        tool="batch_record_review_findings",
        data={
            "task_ref": resolved_task_ref,
            "written": len(findings),
            "results": results,
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
    return {
        "finding_id": str(row["finding_id"]),
        "severity": str(row["severity"]),
        "file_path": str(row["file_path"]),
        "description": str(row["description"]),
        "review_mode": str(row["review_mode"]) if row["review_mode"] else None,
        "details": details_payload,
        "merged_from_json": json.dumps(source_triple, sort_keys=True),
        "status": str(row["status"]),
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
    if target in set(clean_sources):
        return _envelope(
            ok=False,
            tool="merge_review_findings",
            data={"error": "target_task_ref must not appear in source_task_refs."},
            entity="finding",
        )
    effective_session = session.strip() if isinstance(session, str) and session.strip() else _auto_merge_session(target)

    placeholders = ",".join(["?"] * len(clean_sources))
    with _get_db_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, task_ref, session, finding_id, severity, file_path,
                   line_start, line_end, description, fix, review_mode, status,
                   resolved_at, resolution_notes, verification_evidence,
                   resolved_on_branch_at_commit, resolved_on_branch_ref, resolved_on_branch_at_ts,
                   integrated_at_commit, integrated_at_ref, integrated_at_ts,
                   reopen_count, last_reopen_reason, last_reopened_at
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
        resolved_task_ref = _resolve_task_ref(conn, target)
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
