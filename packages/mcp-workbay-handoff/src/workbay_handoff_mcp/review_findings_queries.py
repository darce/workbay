"""Query, integrity, and review-run operations for review findings."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .enums import FindingSeverity, FindingStatus, HandoffStatus
from .review_findings_recording import _retire_merged_source_rows
from .review_findings_support import (
    _annotate_review_finding,
    _canonical_task_ref,
    _canonical_task_ref_from_handoff,
    _canonicalize_finding_path,
    _current_task_revision,
    _validate_review_mode_or_envelope,
    _write_current_task_md_for_active_context,
    finding_file_modified_after,
)
from .shared_db_utils import _paginated_query
from .shared_primitives import (
    REVIEW_FINDING_SEVERITIES,
    REVIEW_FINDING_STATUSES,
    _envelope,
    _json_response,
    _normalize_optional_text,
    _workspace_root,
)
from .shared_schema import _get_db_connection
from .shared_write_context import (
    WriteActor,
    _resolve_write_actor,
    _workspace_git_context,
    collect_target_context_warnings,
)

_FINDING_SUMMARY_FIELDS = ("description", "fix", "resolution_notes", "verification_evidence")
_FINDING_SUMMARY_TRUNCATE = 200
_REVIEW_RUN_VERDICTS: frozenset[str] = frozenset({"pass", "pass_with_findings", "fail", "conditional_pass"})
_SUCCESS_VERDICTS: frozenset[str] = frozenset({"pass", "pass_with_findings", "conditional_pass"})
_REVIEW_RUN_SUBJECT_KINDS: frozenset[str] = frozenset({"task_plan", "epic", "branch", "adr", "roadmap", "other"})
DEFAULT_MAX_REVIEW_ROUNDS = 5
ENFORCE_REVIEW_ROUND_BOUND = False
REVIEW_ROUND_BOUND_EPOCH = "2026-08-30 00:00:00"
_REVIEW_EXHAUSTED_REASON = "review_exhausted"
_HIGH_SUCCESS_VERDICT_REMEDY = (
    "Open high-severity findings on this subject block a success verdict. "
    "Disposition or re-anchor each high finding in-flow "
    "(review_findings disposition/reanchor), then re-record the review run."
)
_UNKNOWN_TASK_REF_REMEDY = (
    "task_ref must name a known task. Pass the stored task_ref "
    "(whitespace and case are canonicalized to the handoff_state row)."
)

# internal — debt digest stamp ([OBS-08] freshness).
FINDING_DEBT_DIGEST_FILENAME = "finding_debt_digest.json"


def _dedupe_review_findings(conn: sqlite3.Connection, task_ref: str) -> int:
    duplicate_ids = [
        int(row["id"])
        for row in conn.execute(
            """
            SELECT id
            FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY finding_id
                           ORDER BY COALESCE(updated_at, resolved_at, created_at) DESC, id DESC
                       ) AS row_num
                FROM review_findings
                WHERE task_ref = ?
            )
            WHERE row_num > 1
            """,
            (task_ref,),
        ).fetchall()
    ]
    if not duplicate_ids:
        return 0
    placeholders = ",".join("?" for _ in duplicate_ids)
    conn.execute(f"DELETE FROM review_findings WHERE id IN ({placeholders})", tuple(duplicate_ids))
    return len(duplicate_ids)


def list_review_findings(
    task_ref: str | None = None,
    status: str = "all",
    severity: str = "all",
    limit: int = 100,
    offset: int = 0,
    review_mode: str | None = None,
    finding_id: str | None = None,
    finding_db_id: int | None = None,
    detail: str = "full",
) -> dict:
    if detail not in ("full", "summary"):
        detail = "full"

    def _apply_finding_detail(finding: dict) -> dict:
        if detail != "summary":
            return finding
        out = dict(finding)
        for field in _FINDING_SUMMARY_FIELDS:
            value = out.get(field)
            if isinstance(value, str) and len(value) > _FINDING_SUMMARY_TRUNCATE:
                out[field] = value[:_FINDING_SUMMARY_TRUNCATE] + "..."
        return out

    if finding_id is not None or finding_db_id is not None:
        if finding_id is not None and finding_db_id is not None:
            return _envelope(
                ok=False,
                tool="list_review_findings",
                data={"error": "Pass exactly one of finding_id or finding_db_id, not both."},
                entity="finding",
            )
        with _get_db_connection() as conn:
            if task_ref is None:
                if finding_db_id is not None:
                    rows = conn.execute("SELECT * FROM review_findings WHERE id = ?", (finding_db_id,)).fetchall()
                else:
                    normalized_fid = finding_id.strip() if isinstance(finding_id, str) else None
                    if not normalized_fid:
                        return _envelope(
                            ok=False,
                            tool="list_review_findings",
                            data={"error": "finding_id must not be empty."},
                            entity="finding",
                        )
                    rows = conn.execute(
                        "SELECT * FROM review_findings WHERE finding_id = ?", (normalized_fid,)
                    ).fetchall()
                if not rows:
                    return _envelope(
                        ok=False,
                        tool="list_review_findings",
                        data={"error": "Finding not found."},
                        entity="finding",
                    )
                if len(rows) > 1:
                    candidate_scopes = sorted({str(row["task_ref"]) for row in rows})
                    return _envelope(
                        ok=False,
                        tool="list_review_findings",
                        data={
                            "error": f"Ambiguous finding_id: {len(rows)} rows across task_refs {candidate_scopes}. Pass task_ref explicitly to disambiguate.",
                        },
                        entity="finding",
                    )
                row = rows[0]
                resolved_task_ref = str(row["task_ref"])
            else:
                resolved_task_ref = _canonical_task_ref(conn, task_ref)
                if finding_db_id is not None:
                    row = conn.execute(
                        "SELECT * FROM review_findings WHERE id = ? AND task_ref = ?",
                        (finding_db_id, resolved_task_ref),
                    ).fetchone()
                else:
                    normalized_fid = finding_id.strip() if isinstance(finding_id, str) else None
                    if not normalized_fid:
                        return _envelope(
                            ok=False,
                            tool="list_review_findings",
                            data={"error": "finding_id must not be empty."},
                            task_ref=resolved_task_ref,
                            entity="finding",
                        )
                    row = conn.execute(
                        "SELECT * FROM review_findings WHERE finding_id = ? AND task_ref = ?",
                        (normalized_fid, resolved_task_ref),
                    ).fetchone()
                if row is None:
                    return _envelope(
                        ok=False,
                        tool="list_review_findings",
                        data={"error": "Finding not found for task."},
                        task_ref=resolved_task_ref,
                        entity="finding",
                    )
            workspace_git = _workspace_git_context()
            finding = _apply_finding_detail(
                _annotate_review_finding(
                    dict(row),
                    workspace_branch=workspace_git["branch"],
                    workspace_commit_sha=workspace_git["commit_sha"],
                    # Single-finding read: embed closure preconditions (actionable
                    # only when acting on one finding). List/summary rows below do NOT.
                    include_closure_requirements=True,
                )
            )
            return _envelope(
                ok=True,
                tool="list_review_findings",
                data={
                    "task_ref": resolved_task_ref,
                    "workspace_git": workspace_git,
                    "filters": {"finding_id": finding_id, "finding_db_id": finding_db_id},
                    "total_matching": 1,
                    "returned": 1,
                    "has_more": False,
                    "counts": {"status": {str(row["status"]): 1}, "severity": {str(row["severity"]): 1}},
                    "findings": [finding],
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )

    valid_statuses = {"all", *REVIEW_FINDING_STATUSES}
    if status not in valid_statuses:
        return _envelope(
            ok=False,
            tool="list_review_findings",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"},
            entity="finding",
        )
    valid_severities = {"all", *REVIEW_FINDING_SEVERITIES}
    if severity not in valid_severities:
        return _envelope(
            ok=False,
            tool="list_review_findings",
            data={"error": f"Invalid severity. Valid: {', '.join(sorted(valid_severities))}"},
            entity="finding",
        )
    normalized_review_mode, error = _validate_review_mode_or_envelope(
        review_mode,
        tool="list_review_findings",
    )
    if error is not None:
        return error
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    with _get_db_connection() as conn:
        resolved_task_ref = _canonical_task_ref(conn, task_ref)
        where_parts = ["task_ref = ?"]
        params: list[object] = [resolved_task_ref]
        if status != "all":
            # internal (BR-001): the lifecycle compatibility matrix
            # declares ``status='fixed'`` as the wire-compat alias filter that
            # must return the union of ``fixed``, ``resolved_on_branch``, and
            # ``integrated`` rows during the rollout window. Specific
            # ``resolved_on_branch`` / ``integrated`` filters keep their exact
            # semantics.
            if status == "fixed":
                where_parts.append("status IN (?, ?, ?)")
                params.extend(("fixed", "resolved_on_branch", "integrated"))
            else:
                where_parts.append("status = ?")
                params.append(status)
        if severity != "all":
            where_parts.append("severity = ?")
            params.append(severity)
        if normalized_review_mode == "branch":
            where_parts.append("(review_mode = 'branch' OR review_mode IS NULL)")
        elif normalized_review_mode in ("release_audit", "planning"):
            where_parts.append("review_mode = ?")
            params.append(normalized_review_mode)
        where_sql = " AND ".join(where_parts)
        findings_order = "CASE status WHEN 'open' THEN 0 WHEN 'deferred' THEN 1 WHEN 'fixed' THEN 2 WHEN 'wontfix' THEN 3 WHEN 'superseded' THEN 3 END, CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 END, COALESCE(updated_at, created_at) DESC, id DESC"
        total, raw_findings = _paginated_query(
            conn,
            "review_findings",
            where_sql,
            tuple(params),
            limit,
            offset,
            findings_order,
        )
        status_counts = {key: 0 for key in sorted(REVIEW_FINDING_STATUSES)}
        for row in conn.execute(
            f"SELECT status, COUNT(*) AS count FROM review_findings WHERE {where_sql} GROUP BY status",
            tuple(params),
        ).fetchall():
            status_counts[str(row["status"])] = int(row["count"])
        severity_counts = {key: 0 for key in sorted(REVIEW_FINDING_SEVERITIES)}
        for row in conn.execute(
            f"SELECT severity, COUNT(*) AS count FROM review_findings WHERE {where_sql} GROUP BY severity",
            tuple(params),
        ).fetchall():
            severity_counts[str(row["severity"])] = int(row["count"])
    workspace_git = _workspace_git_context()
    findings = [
        _apply_finding_detail(
            _annotate_review_finding(
                row,
                workspace_branch=workspace_git["branch"],
                workspace_commit_sha=workspace_git["commit_sha"],
            )
        )
        for row in raw_findings
    ]
    return _envelope(
        ok=True,
        tool="list_review_findings",
        data={
            "task_ref": resolved_task_ref,
            "workspace_git": workspace_git,
            "filters": {
                "status": status,
                "severity": severity,
                "review_mode": normalized_review_mode,
                "limit": limit,
                "offset": offset,
            },
            "total_matching": total,
            "returned": len(findings),
            "has_more": (offset + len(findings)) < total,
            "counts": {"status": status_counts, "severity": severity_counts},
            "findings": findings,
        },
        task_ref=resolved_task_ref,
        entity="finding",
    )


def get_review_findings_summary(
    task_ref: str | None = None,
    top_n_open: int = 5,
    top_n_recent_updates: int = 3,
    review_mode: str | None = None,
) -> dict:
    top_n_open = max(1, top_n_open)
    top_n_recent_updates = max(1, top_n_recent_updates)
    normalized_review_mode, error = _validate_review_mode_or_envelope(
        review_mode,
        tool="get_review_findings_summary",
    )
    if error is not None:
        return _json_response({"ok": False, "error": error["data"]["error"]})
    with _get_db_connection() as conn:
        resolved_task_ref = _canonical_task_ref(conn, task_ref)
        where_parts = ["task_ref = ?"]
        params: list[object] = [resolved_task_ref]
        if normalized_review_mode == "branch":
            where_parts.append("(review_mode = 'branch' OR review_mode IS NULL)")
        elif normalized_review_mode in ("release_audit", "planning"):
            where_parts.append("review_mode = ?")
            params.append(normalized_review_mode)
        where_sql = " AND ".join(where_parts)
        total_row = conn.execute(
            f"SELECT COUNT(*) AS total FROM review_findings WHERE {where_sql}", tuple(params)
        ).fetchone()
        status_counts = {key: 0 for key in sorted(REVIEW_FINDING_STATUSES)}
        for row in conn.execute(
            f"SELECT status, COUNT(*) AS count FROM review_findings WHERE {where_sql} GROUP BY status",
            tuple(params),
        ).fetchall():
            status_counts[str(row["status"])] = int(row["count"])
        severity_counts = {key: 0 for key in sorted(REVIEW_FINDING_SEVERITIES)}
        for row in conn.execute(
            f"SELECT severity, COUNT(*) AS count FROM review_findings WHERE {where_sql} GROUP BY severity",
            tuple(params),
        ).fetchall():
            severity_counts[str(row["severity"])] = int(row["count"])
        raw_open_findings = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM review_findings WHERE {where_sql} AND status = 'open' ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 END, COALESCE(updated_at, created_at) DESC, id DESC LIMIT ?",
                (*params, top_n_open),
            ).fetchall()
        ]
        raw_recent_updates = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM review_findings WHERE {where_sql} ORDER BY COALESCE(updated_at, resolved_at, created_at) DESC, id DESC LIMIT ?",
                (*params, top_n_recent_updates),
            ).fetchall()
        ]
    workspace_git = _workspace_git_context()
    open_findings = [
        _annotate_review_finding(
            row, workspace_branch=workspace_git["branch"], workspace_commit_sha=workspace_git["commit_sha"]
        )
        for row in raw_open_findings
    ]
    recent_updates = [
        _annotate_review_finding(
            row, workspace_branch=workspace_git["branch"], workspace_commit_sha=workspace_git["commit_sha"]
        )
        for row in raw_recent_updates
    ]
    return _json_response(
        {
            "ok": True,
            "task_ref": resolved_task_ref,
            "workspace_git": workspace_git,
            "review_mode": normalized_review_mode,
            "counts": {
                "total": int(total_row["total"]) if total_row else 0,
                "status": status_counts,
                "severity": severity_counts,
            },
            "open_top": open_findings,
            "recent_updates": recent_updates,
            "limits": {"top_n_open": top_n_open, "top_n_recent_updates": top_n_recent_updates},
        }
    )


_COORDINATOR_TERMINAL_STATUSES = frozenset(
    {
        FindingStatus.FIXED.value,
        FindingStatus.WONTFIX.value,
        FindingStatus.DEFERRED.value,
        FindingStatus.RESOLVED_ON_BRANCH.value,
        FindingStatus.INTEGRATED.value,
    }
)

# Discriminators for SUPERSEDED surviving merge copies in _collect_reviewer_scratch_drift.
# SUPERSEDED is intentionally NOT in _COORDINATOR_TERMINAL_STATUSES: a blanket allow
# would retire scratch rows whose copy was only merge-upward into a still-open
# finding (destroying the only surviving live-record path).
# Notes are written by _supersede_review_findings callers:
#   apply_stale_findings_gc → "stale_task_gc"
#   _retire_merged_source_rows → "merged into {target} @ {session}"
# Historical archive tombstone (pre internal) → "task_archived".
# Current archive triage does NOT supersede: _triage_open_findings_for_archive
# migrates open low/medium to internal-* with status still open and
# note "plan:0097 archive auto-migrate from {source} to {backlog}" via
# _migrate_open_findings_to_backlog. The collector therefore follows merged_from
# to non-scratch survivors (not only coordinator_task_ref).
# The former plan:0097 SUPERSEDED arm was unreachable on real SUPERSEDED writers
# (SGC-R1-A-01); pure plan:0097 SUPERSEDED rows are zero on the host corpus.
_SUPERSEDED_MERGE_UPWARD_NOTE_MARKER = "merged into "
_SUPERSEDED_STALE_GC_NOTE_MARKER = "stale_task_gc"
_SUPERSEDED_TASK_ARCHIVED_NOTE_MARKER = "task_archived"


_REVIEWER_SCRATCH_MARKER = "-REV-"


def is_reviewer_scratch_task_ref(task_ref: str) -> bool:
    return _REVIEWER_SCRATCH_MARKER in task_ref


def coordinator_task_ref_from_scratch(scratch_task_ref: str) -> str | None:
    if _REVIEWER_SCRATCH_MARKER not in scratch_task_ref:
        return None
    coordinator, _suffix = scratch_task_ref.rsplit(_REVIEWER_SCRATCH_MARKER, 1)
    return coordinator or None


def _parse_merged_from_json(raw: object) -> dict[str, object] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _superseded_coordinator_is_disposition_terminal(resolution_notes: object) -> bool:
    """True when a SUPERSEDED surviving copy is a disposition, not a merge-upward.

    Discriminator is row-local (``resolution_notes`` only):

    - merge-upward (``merged into ``) → False: scratch must stay open; the live
      finding may still exist on the merge target.
    - disposed (``stale_task_gc`` or ``task_archived``) → True: copy was retired
      by stale-task GC or historical archive tombstone; scratch may retire.
    - neither shape → False (conservative default: leave scratch open).

    Merge-upward is checked first so a note that somehow carries both markers
    never retires the scratch.

    Note: current archive triage migrates (status stays open) rather than
    superseding; ``task_archived`` covers historical SUPERSEDED archive rows.
    The retired ``plan:0097`` SUPERSEDED arm was unreachable on production
    SUPERSEDED writers (see SGC-R1-A-01).
    """
    notes = str(resolution_notes or "")
    if _SUPERSEDED_MERGE_UPWARD_NOTE_MARKER in notes:
        return False
    if _SUPERSEDED_STALE_GC_NOTE_MARKER in notes:
        return True
    if _SUPERSEDED_TASK_ARCHIVED_NOTE_MARKER in notes:
        return True
    return False


def _coordinator_copy_is_terminal_for_scratch_retirement(
    status: str,
    resolution_notes: object,
) -> bool:
    """Whether a coordinator-row status qualifies as terminal for scratch retirement.

    OPEN never qualifies. SUPERSEDED qualifies only via the disposition
    discriminator (not a blanket allow). Other statuses use
    ``_COORDINATOR_TERMINAL_STATUSES``.
    """
    if status == FindingStatus.OPEN.value:
        return False
    if status == FindingStatus.SUPERSEDED.value:
        return _superseded_coordinator_is_disposition_terminal(resolution_notes)
    return status in _COORDINATOR_TERMINAL_STATUSES


def _collect_reviewer_scratch_drift(
    conn: sqlite3.Connection,
    coordinator_task_ref: str,
    *,
    apply: bool = False,
) -> dict:
    """Report (and optionally retire) open rows on coordinator-scoped reviewer scratch refs.

    Surviving merge copies are located by ``merged_from`` provenance on any
    non-scratch ``task_ref`` (not only ``coordinator_task_ref``). Archive triage
    migrates the coordinator copy onto ``internal-*`` while keeping
    ``merged_from_json``; restricting the lookup to the coordinator permanently
    orphaned those scratch sources after archive.

    Safety (SGC-C-R1-01): retirement is a *universal* over matching survivors,
    not an existential. Collect every non-scratch row whose ``merged_from`` names
    the open scratch pair; retire only when there is at least one and *every*
    one is terminal. Cross-task authorization is intentional (archive backlog
    survivors must still authorize); the fix is the AND, not a narrower query.

    Safety (SGC2A-R1-02 / SGC3A-R1-02): a coordinator copy of the same
    ``finding_id`` that is not terminal by
    ``_coordinator_copy_is_terminal_for_scratch_retirement`` vetoes retirement
    even when it carries no ``merged_from`` provenance (and so is not a matching
    survivor). Without this check an unrelated terminal row that still names
    the scratch pair would authorize retirement while a non-terminal
    coordinator copy remains (OPEN, or SUPERSEDED without a disposition-terminal
    note, or any other non-terminal status). The veto uses the same terminal
    predicate as the matching-survivor universal — not a hardcoded OPEN-only
    SQL filter. Fail-safe defaults ([SECD-05]): mixed evidence degrades to not
    acting.

    ``coordinator_status`` is reported only when eligible. Among matching
    survivors prefer the row on ``coordinator_task_ref`` when present; otherwise
    the lexicographically smallest ``task_ref``. That keeps the scalar from
    naming an unrelated terminal while a live coordinator copy still exists.
    """
    scratch_refs = [
        str(row["task_ref"])
        for row in conn.execute(
            """
            SELECT DISTINCT task_ref
            FROM review_findings
            WHERE task_ref LIKE ?
            ORDER BY task_ref
            """,
            (f"{coordinator_task_ref}-REV-%",),
        ).fetchall()
    ]

    # Non-scratch rows that carry merge provenance — the only copies that can
    # authorize scratch retirement. Includes coordinator copies and rows that
    # archive triage migrated onto a backlog (or any other non-scratch ref).
    # Intentionally unscoped to coordinator_task_ref (archive-migrate path).
    surviving_merge_copies = conn.execute(
        """
        SELECT finding_id, status, merged_from_json, resolution_notes, task_ref
        FROM review_findings
        WHERE merged_from_json IS NOT NULL
          AND task_ref NOT LIKE ?
        """,
        (f"%{_REVIEWER_SCRATCH_MARKER}%",),
    ).fetchall()

    items: list[dict[str, object]] = []
    retire_groups: dict[tuple[str, str], list[int]] = {}

    for scratch_ref in scratch_refs:
        open_rows = conn.execute(
            """
            SELECT id, finding_id, session
            FROM review_findings
            WHERE task_ref = ? AND status = ?
            ORDER BY id
            """,
            (scratch_ref, FindingStatus.OPEN.value),
        ).fetchall()
        for row in open_rows:
            finding_id = str(row["finding_id"])
            # Collect ALL matching survivors for this scratch pair, then decide.
            # First-terminal break was the SGC-C-R1-01 defect (OR over survivors).
            matching_survivors: list[sqlite3.Row] = []
            for cand in surviving_merge_copies:
                merged_from = _parse_merged_from_json(cand["merged_from_json"])
                if (
                    not merged_from
                    or merged_from.get("task_ref") != scratch_ref
                    or merged_from.get("finding_id") != finding_id
                ):
                    continue
                matching_survivors.append(cand)

            eligible = bool(matching_survivors) and all(
                _coordinator_copy_is_terminal_for_scratch_retirement(str(cand["status"]), cand["resolution_notes"])
                for cand in matching_survivors
            )
            # SGC2A-R1-02 / SGC3A-R1-02: a non-terminal coordinator copy of the
            # same finding_id vetoes even when it carries no merged_from
            # provenance (not a matching survivor). Use the same terminal
            # predicate as the survivor universal — not status=OPEN only —
            # so SUPERSEDED without disposition-terminal notes also blocks.
            if eligible:
                coordinator_copies = conn.execute(
                    """
                    SELECT status, resolution_notes
                    FROM review_findings
                    WHERE task_ref = ?
                      AND finding_id = ?
                    """,
                    (coordinator_task_ref, finding_id),
                ).fetchall()
                if any(
                    not _coordinator_copy_is_terminal_for_scratch_retirement(
                        str(coord_row["status"]), coord_row["resolution_notes"]
                    )
                    for coord_row in coordinator_copies
                ):
                    eligible = False

            item: dict[str, object] = {
                "scratch_task_ref": scratch_ref,
                "scratch_row_id": int(row["id"]),
                "finding_id": finding_id,
                "eligible_for_retirement": eligible,
            }
            if eligible:
                # Prefer the coordinator's own survivor; else stable lex order.
                status_authority = next(
                    (cand for cand in matching_survivors if str(cand["task_ref"]) == coordinator_task_ref),
                    None,
                )
                if status_authority is None:
                    status_authority = min(
                        matching_survivors,
                        key=lambda cand: (str(cand["task_ref"]), str(cand["finding_id"])),
                    )
                item["coordinator_status"] = str(status_authority["status"])
            items.append(item)

            if apply and eligible:
                session = str(row["session"]) if row["session"] is not None else ""
                retire_groups.setdefault((scratch_ref, session), []).append(int(row["id"]))

    retired_total = 0
    if apply:
        for (scratch_ref, session), source_ids in retire_groups.items():
            retired_total += _retire_merged_source_rows(
                conn,
                source_ids=source_ids,
                target_task_ref=coordinator_task_ref,
                session=session or scratch_ref,
            )

    eligible_count = sum(1 for item in items if item["eligible_for_retirement"])
    return {
        "count": len(items),
        "eligible_count": eligible_count,
        "retired": retired_total,
        "scratch_refs": scratch_refs,
        "items": items,
        "is_violation": eligible_count > 0,
    }


def _collect_review_findings_integrity(conn: sqlite3.Connection, task_ref: str, *, apply: bool = False) -> dict:
    duplicate_rows = conn.execute(
        """
        SELECT finding_id, COUNT(*) AS count
        FROM review_findings
        WHERE task_ref = ?
        GROUP BY finding_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC, finding_id ASC
        """,
        (task_ref,),
    ).fetchall()
    duplicates = [{"finding_id": row["finding_id"], "count": int(row["count"])} for row in duplicate_rows]
    deduped_rows_removed = 0
    if apply and duplicates:
        deduped_rows_removed = _dedupe_review_findings(conn, task_ref)
        duplicate_rows = conn.execute(
            """
            SELECT finding_id, COUNT(*) AS count
            FROM review_findings
            WHERE task_ref = ?
            GROUP BY finding_id
            HAVING COUNT(*) > 1
            ORDER BY count DESC, finding_id ASC
            """,
            (task_ref,),
        ).fetchall()
        duplicates = [{"finding_id": row["finding_id"], "count": int(row["count"])} for row in duplicate_rows]
    open_count = int(
        conn.execute(
            "SELECT COUNT(*) AS count FROM review_findings WHERE task_ref = ? AND status = ?",
            (task_ref, FindingStatus.OPEN),
        ).fetchone()["count"]
    )
    task_row = conn.execute("SELECT status FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
    active_status = str(task_row["status"]) if task_row is not None else None
    done_with_open_findings = bool(active_status == HandoffStatus.DONE and open_count > 0)
    stale_open_findings = [
        annotated
        for row in conn.execute(
            "SELECT id, finding_id, file_path, created_at, updated_at FROM review_findings WHERE task_ref = ? AND status = ? ORDER BY COALESCE(updated_at, created_at) DESC, id DESC",
            (task_ref, FindingStatus.OPEN),
        ).fetchall()
        if (annotated := finding_file_modified_after(row)) is not None
    ]
    missing_provenance = [
        {
            "id": int(row["id"]),
            "finding_id": str(row["finding_id"]),
            "agent": row["agent"],
            "branch": row["branch"],
            "commit_sha": row["commit_sha"],
        }
        for row in conn.execute(
            "SELECT id, finding_id, agent, branch, commit_sha FROM review_findings WHERE task_ref = ? AND (agent IS NULL OR TRIM(agent) = '' OR branch IS NULL OR TRIM(branch) = '') ORDER BY id DESC",
            (task_ref,),
        ).fetchall()
    ]
    reopen_metadata = [
        {
            "id": int(row["id"]),
            "finding_id": str(row["finding_id"]),
            "reopen_count": int(row["reopen_count"]),
            "last_reopen_reason": row["last_reopen_reason"],
            "last_reopened_at": row["last_reopened_at"],
        }
        for row in conn.execute(
            "SELECT id, finding_id, reopen_count, last_reopen_reason, last_reopened_at FROM review_findings WHERE task_ref = ? AND COALESCE(reopen_count, 0) > 0 AND (last_reopen_reason IS NULL OR TRIM(last_reopen_reason) = '' OR last_reopened_at IS NULL OR TRIM(last_reopened_at) = '') ORDER BY id DESC",
            (task_ref,),
        ).fetchall()
    ]
    reviewer_scratch_drift = _collect_reviewer_scratch_drift(conn, task_ref, apply=apply)
    # NOTE: reviewer_scratch_drift is reported as a separate section only and is
    # deliberately NOT folded into `healthy`. Plan internal
    # (L41/L273/L331) requires close-check semantics stay unchanged: handoff_close_check
    # consumes this `healthy` flag (decisions.py:_evaluate_close_failures), and gating on
    # eligible-but-not-yet-retired scratch drift would block close for coordinators that
    # merged with retire_sources=False until an out-of-band reconcile(apply=True). Surface
    # the drift under checks for the reconcile tool / skill recipe; do not gate on it.
    healthy = (
        len(duplicates) == 0
        and not done_with_open_findings
        and len(stale_open_findings) == 0
        and len(missing_provenance) == 0
        and len(reopen_metadata) == 0
    )
    return {
        "healthy": healthy,
        "checks": {
            "duplicates": {"count": len(duplicates), "items": duplicates, "deduped_rows_removed": deduped_rows_removed},
            "done_with_open_findings": {
                "active_status": active_status,
                "open_count": open_count,
                "is_violation": done_with_open_findings,
            },
            "stale_open_findings": {"count": len(stale_open_findings), "items": stale_open_findings},
            "missing_provenance": {"count": len(missing_provenance), "items": missing_provenance},
            "reopen_metadata": {"count": len(reopen_metadata), "items": reopen_metadata},
            "reviewer_scratch_drift": reviewer_scratch_drift,
        },
    }


def reconcile_review_findings(task_ref: str | None = None, apply: bool = False) -> dict:
    with _get_db_connection() as conn:
        resolved_task_ref = _canonical_task_ref(conn, task_ref)
        report = _collect_review_findings_integrity(conn, resolved_task_ref, apply=apply)
        if apply and report["checks"]["done_with_open_findings"]["active_status"] is not None:
            if (
                int(report["checks"]["duplicates"]["deduped_rows_removed"]) > 0
                or int(report["checks"]["reviewer_scratch_drift"]["retired"]) > 0
            ):
                _write_current_task_md_for_active_context(conn, resolved_task_ref)
    return _json_response(
        {"ok": True, "task_ref": resolved_task_ref, "healthy": report["healthy"], "checks": report["checks"]}
    )


def reconcile_reviewer_scratch_findings_gc(*, apply: bool = False) -> dict:
    """Bulk-retire open reviewer-scratch findings by looping per-coordinator reconcile."""
    with _get_db_connection() as conn:
        scratch_refs = [
            str(row["task_ref"])
            for row in conn.execute(
                """
                SELECT DISTINCT task_ref
                FROM review_findings
                WHERE task_ref LIKE ? AND status = ?
                ORDER BY task_ref
                """,
                (f"%{_REVIEWER_SCRATCH_MARKER}%", FindingStatus.OPEN.value),
            ).fetchall()
        ]

    coordinators = sorted(
        {
            coordinator
            for scratch_ref in scratch_refs
            if (coordinator := coordinator_task_ref_from_scratch(scratch_ref)) is not None
        }
    )
    invalid_scratch_refs = [ref for ref in scratch_refs if coordinator_task_ref_from_scratch(ref) is None]

    per_coordinator: list[dict[str, object]] = []
    total_retired = 0
    total_eligible = 0
    orphaned_items: list[dict[str, object]] = []

    for coordinator in coordinators:
        result = reconcile_review_findings(task_ref=coordinator, apply=apply)
        drift = result["checks"]["reviewer_scratch_drift"]
        orphaned = [item for item in drift.get("items", []) if not item.get("eligible_for_retirement")]
        orphaned_items.extend(orphaned)
        total_retired += int(drift["retired"])
        total_eligible += int(drift["eligible_count"])
        per_coordinator.append(
            {
                "coordinator_task_ref": coordinator,
                "retired": drift["retired"],
                "eligible_count": drift["eligible_count"],
                "count": drift["count"],
                "scratch_refs": drift["scratch_refs"],
                "orphaned_count": len(orphaned),
            }
        )

    return _json_response(
        {
            "ok": True,
            "applied": apply,
            "coordinators_processed": coordinators,
            "scratch_refs_with_open_findings": scratch_refs,
            "retired": total_retired,
            "eligible_count": total_eligible,
            "orphaned_count": len(orphaned_items),
            "orphaned_items": orphaned_items,
            "invalid_scratch_refs": invalid_scratch_refs,
            "per_coordinator": per_coordinator,
        }
    )


def _collect_stale_nonscratch_open_finding_items(
    conn: sqlite3.Connection, *, batch_size: int | None = None
) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT rf.task_ref, rf.finding_id, rf.id, rf.severity, rf.file_path
        FROM review_findings rf
        WHERE rf.status = ?
          AND rf.task_ref NOT LIKE ?
        ORDER BY rf.task_ref, rf.id
        """,
        (FindingStatus.OPEN.value, f"%{_REVIEWER_SCRATCH_MARKER}%"),
    ).fetchall()
    live_statuses: dict[str, str | None] = {}
    for row in conn.execute("SELECT task_ref, status FROM handoff_state").fetchall():
        live_statuses[str(row["task_ref"])] = str(row["status"])

    items: list[dict[str, object]] = []
    for row in rows:
        ref = str(row["task_ref"])
        live_status = live_statuses.get(ref)
        if live_status is not None and live_status != HandoffStatus.DONE.value:
            continue
        items.append(
            {
                "task_ref": ref,
                "finding_id": str(row["finding_id"]),
                "finding_db_id": int(row["id"]),
                "severity": str(row["severity"]),
                "file_path": str(row["file_path"]),
                "has_live_handoff_row": live_status is not None,
                "handoff_status": live_status,
            }
        )
    if batch_size is None:
        return items
    return items[: max(1, int(batch_size))]


def _count_stale_pending_next_actions(conn: sqlite3.Connection, task_refs: list[str]) -> int:
    if not task_refs:
        return 0
    placeholders = ",".join("?" for _ in task_refs)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM next_actions
        WHERE status = 'pending'
          AND task_ref IN ({placeholders})
        """,
        tuple(task_refs),
    ).fetchone()
    return int(row["count"]) if row is not None else 0


def _skip_stale_pending_next_actions(conn: sqlite3.Connection, task_refs: list[str]) -> int:
    if not task_refs:
        return 0
    placeholders = ",".join("?" for _ in task_refs)
    cursor = conn.execute(
        f"""
        UPDATE next_actions
        SET status = 'skipped',
            updated_at = datetime('now')
        WHERE status = 'pending'
          AND task_ref IN ({placeholders})
        """,
        tuple(task_refs),
    )
    return int(cursor.rowcount)


def apply_stale_findings_gc(*, apply: bool = False, batch_size: int = 200) -> dict:
    """Collect or supersede stale non-scratch open findings; never delete content."""
    from .review_findings_recording import _supersede_review_findings

    with _get_db_connection() as conn:
        # Compute the TRUE total of stale findings (unbounded) as well as the
        # bounded batch actually dispositioned this run. Reporting only the batch
        # ``count`` let doctor undercount ~933 stale rows to the 200 batch cap and
        # imply one collect-run would clear them.
        all_items = _collect_stale_nonscratch_open_finding_items(conn, batch_size=None)
        total_stale = len(all_items)
        bounded = max(1, int(batch_size))
        items = all_items[:bounded]
        stale_task_refs = sorted({str(item["task_ref"]) for item in items})
        would_skip_next_actions = _count_stale_pending_next_actions(conn, stale_task_refs)
        retired = 0
        skipped_next_actions = 0
        if apply and items:
            retired = _supersede_review_findings(
                conn,
                source_ids=[int(str(item["finding_db_id"])) for item in items],
                resolution_note="stale_task_gc",
            )
            skipped_next_actions = _skip_stale_pending_next_actions(conn, stale_task_refs)

    orphaned_refs = sorted({str(item["task_ref"]) for item in items if not item["has_live_handoff_row"]})
    return _json_response(
        {
            "ok": True,
            "applied": apply,
            # ``count`` stays the batched item count for back-compat; ``total_stale``
            # is the unbounded true total so callers (doctor) see the real backlog.
            "count": len(items),
            "total_stale": total_stale,
            "batch_size": bounded,
            "batch_capped": total_stale > len(items),
            "retired": retired,
            "would_skip_next_actions": would_skip_next_actions,
            "skipped_next_actions": skipped_next_actions,
            "orphaned_ref_count": len(orphaned_refs),
            "orphaned_refs": orphaned_refs,
            "items": items,
        }
    )


def collect_stale_nonscratch_open_findings(*, apply: bool = False, batch_size: int = 200) -> dict:
    """Report or supersede open findings on non-scratch refs with no live row or status=done."""
    return apply_stale_findings_gc(apply=apply, batch_size=batch_size)


def _finding_debt_digest_path(state_dir: Path | None = None) -> Path:
    if state_dir is not None:
        return Path(state_dir) / FINDING_DEBT_DIGEST_FILENAME
    try:
        from .runtime import get_runtime_config

        return get_runtime_config().state_dir / FINDING_DEBT_DIGEST_FILENAME
    except Exception:
        return Path(".task-state") / FINDING_DEBT_DIGEST_FILENAME


def _parse_sqlite_ts(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    # SQLite datetime('now') is typically 'YYYY-MM-DD HH:MM:SS'
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            parsed = datetime.strptime(text.replace("Z", ""), fmt.replace("Z", ""))
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        # fromisoformat tolerates offset forms
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _is_dead_path(file_path: str, *, workspace_root: Path | None) -> bool:
    """Mechanical dead-path signal for the weekly debt digest.

    Treats pre-rebrand ``workstate`` anchors as dead (brand-check: allow, implementation note)
    and missing workspace-relative files as dead. Pure path heuristics — does not
    require a rename map.
    """
    normalized = (file_path or "").strip().replace("\\", "/")
    if not normalized:
        return False
    if "workstate" in normalized.lower():  # brand-check: allow — pre-rebrand dead-path detector literal (implementation note)
        return True
    if workspace_root is None:
        return False
    if normalized.startswith("/"):
        return False
    candidate = Path(workspace_root) / normalized
    try:
        # Missing file → dead; existing file → live
        return not candidate.is_file()
    except OSError:
        return True


def collect_finding_debt_digest(
    conn: sqlite3.Connection,
    *,
    workspace_root: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compute open-by-severity, age buckets, and dead-path count.

    Empty/no-findings degrades cleanly to zeros. Callers that want the
    [OBS-08] freshness stamp should pass the result through
    ``stamp_finding_debt_digest``.
    """
    root = Path(workspace_root) if workspace_root is not None else None
    if root is None:
        try:
            from .runtime import get_runtime_config

            root = get_runtime_config().workspace_root
        except Exception:
            root = None

    clock = now or datetime.now(UTC)
    open_by_severity = {
        FindingSeverity.HIGH.value: 0,
        FindingSeverity.MEDIUM.value: 0,
        FindingSeverity.LOW.value: 0,
    }
    age_buckets = {"0-7d": 0, "8-30d": 0, "30d+": 0}
    dead_path_count = 0

    rows = conn.execute(
        """
        SELECT severity, file_path, created_at
        FROM review_findings
        WHERE status = ?
        """,
        (FindingStatus.OPEN.value,),
    ).fetchall()

    for row in rows:
        sev = str(row["severity"] or "").lower()
        if sev in open_by_severity:
            open_by_severity[sev] += 1
        else:
            # Unknown severity — count under low so totals stay coherent.
            open_by_severity[FindingSeverity.LOW.value] += 1
        created = _parse_sqlite_ts(row["created_at"])
        if created is None:
            age_buckets["30d+"] += 1
        else:
            age_days = max(0, (clock - created).total_seconds() / 86400.0)
            if age_days <= 7:
                age_buckets["0-7d"] += 1
            elif age_days <= 30:
                age_buckets["8-30d"] += 1
            else:
                age_buckets["30d+"] += 1
        if _is_dead_path(str(row["file_path"] or ""), workspace_root=root):
            dead_path_count += 1

    open_total = sum(open_by_severity.values())
    return {
        "ok": True,
        "open_total": open_total,
        "open_by_severity": open_by_severity,
        "age_buckets": age_buckets,
        "dead_path_count": dead_path_count,
        "plan": "0097",
        "slice": 4,
    }


def stamp_finding_debt_digest(
    digest: dict[str, Any] | None = None,
    *,
    conn: sqlite3.Connection | None = None,
    state_dir: Path | None = None,
    workspace_root: Path | None = None,
    last_run_at: str | None = None,
    source: str = "digest",
) -> dict[str, Any]:
    """Persist a debt-digest snapshot with last-run freshness ([OBS-08]).

    Distinguishes healthy-zero dead-path from "classifier/GC has not run".
    """
    if digest is None:
        if conn is None:
            with _get_db_connection() as owned:
                digest = collect_finding_debt_digest(owned, workspace_root=workspace_root)
        else:
            digest = collect_finding_debt_digest(conn, workspace_root=workspace_root)
    stamped = dict(digest)
    stamped["last_run_at"] = last_run_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    stamped["source"] = source
    path = _finding_debt_digest_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(stamped, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    stamped["path"] = str(path)
    return stamped


def load_finding_debt_digest_stamp(state_dir: Path | None = None) -> dict[str, Any] | None:
    """Load the last stamped debt digest, or None if classifier/GC never ran."""
    path = _finding_debt_digest_path(state_dir)
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def format_finding_debt_digest_line(
    stamp: dict[str, Any] | None,
    *,
    live: dict[str, Any] | None = None,
) -> str:
    """Render the DASHBOARD debt line with [OBS-08] freshness.

    Open/age prefer live DB counts. Dead-path + last-run come from the
    classifier/GC stamp so a stalled pass reads as *stale*, not healthy-zero.
    """
    counts_src = live if live is not None else stamp
    if counts_src is None and stamp is None:
        return "FINDING DEBT: classifier/GC pass has not run (stale) — last-run: never"
    counts_src = counts_src or {}
    by_sev = counts_src.get("open_by_severity") or {}
    high = int(by_sev.get("high") or 0)
    medium = int(by_sev.get("medium") or 0)
    low = int(by_sev.get("low") or 0)
    ages = counts_src.get("age_buckets") or {}
    a0 = int(ages.get("0-7d") or 0)
    a1 = int(ages.get("8-30d") or 0)
    a2 = int(ages.get("30d+") or 0)
    if stamp is None:
        return (
            f"FINDING DEBT: open high={high} medium={medium} low={low} | "
            f"age 0-7d={a0} 8-30d={a1} 30d+={a2} | "
            f"dead-path: unknown (classifier/GC pass has not run) | "
            f"last-run: never (stale)"
        )
    dead = int(stamp.get("dead_path_count") or 0)
    last_run = str(stamp.get("last_run_at") or "unknown")
    return (
        f"FINDING DEBT: open high={high} medium={medium} low={low} | "
        f"age 0-7d={a0} 8-30d={a1} 30d+={a2} | "
        f"dead-path={dead} | last-run: {last_run}"
    )


def _canonicalize_subject_path(raw: str) -> str | None:
    """Workspace-relative subject key used for both the round count and the stored row."""
    if (raw or "").strip().replace("\\", "/") == ".":
        return "."
    return _canonicalize_finding_path(raw)


def _file_path_in_subject(file_path: str, subject_path: str) -> bool:
    file_canon = _canonicalize_finding_path(file_path)
    if file_canon is None:
        return True
    if not subject_path:
        return False
    file_key = file_canon.casefold()
    subject_key = subject_path.casefold()
    if subject_key == ".":
        return True
    if file_key == subject_key:
        return True
    return file_key.startswith(subject_key.rstrip("/") + "/")


def _existing_canonical_subject_path(conn: sqlite3.Connection, task_ref: str, subject_path: str) -> str:
    """Reuse the first stored spelling so case variants share one key."""
    row = conn.execute(
        """
        SELECT subject_path
        FROM review_runs
        WHERE task_ref = ? AND lower(subject_path) = lower(?)
        ORDER BY id ASC
        LIMIT 1
        """,
        (task_ref, subject_path),
    ).fetchone()
    if row is None:
        return subject_path
    return str(row["subject_path"])


def _open_findings_for_severity_gate(
    conn: sqlite3.Connection, task_ref: str, subject_path: str, subject_kind: str
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Subject-scoped open ids plus task-wide advisory ids.

    A finding counts against the run only when its file_path equals the
    canonical subject, or is under that subject when the subject names a
    directory. Task-wide totals are returned for advisory fields only.
    """
    rows = conn.execute(
        """
        SELECT finding_id, severity, file_path
        FROM review_findings
        WHERE task_ref = ? AND status = ?
        ORDER BY id ASC
        """,
        (task_ref, FindingStatus.OPEN.value),
    ).fetchall()
    subject_high: list[str] = []
    subject_low_med: list[str] = []
    task_high: list[str] = []
    task_low_med: list[str] = []
    workspace = _workspace_root().resolve()
    candidate = workspace / subject_path
    try:
        candidate.resolve(strict=True).relative_to(workspace)
        subject_is_workspace_path = True
    except (FileNotFoundError, OSError, ValueError):
        subject_is_workspace_path = False
    fail_closed_task_scope = subject_kind == "branch" or not subject_is_workspace_path
    for row in rows:
        finding_id = str(row["finding_id"])
        severity = str(row["severity"])
        in_subject = fail_closed_task_scope or _file_path_in_subject(str(row["file_path"] or ""), subject_path)
        if severity == FindingSeverity.HIGH.value:
            task_high.append(finding_id)
            if in_subject:
                subject_high.append(finding_id)
        elif severity in {FindingSeverity.LOW.value, FindingSeverity.MEDIUM.value}:
            task_low_med.append(finding_id)
            if in_subject:
                subject_low_med.append(finding_id)
    return subject_high, subject_low_med, task_high, task_low_med


def _count_prior_review_rounds(
    conn: sqlite3.Connection,
    task_ref: str,
    subject_path: str,
    review_mode: str,
    *,
    since: str | None = None,
) -> int:
    sql = """
        SELECT COUNT(*) AS n
        FROM review_runs
        WHERE task_ref = ?
          AND lower(subject_path) = lower(?)
          AND review_mode = ?
          AND verdict IS NOT NULL
    """
    params: list[object] = [task_ref, subject_path, review_mode]
    if since is not None:
        sql += " AND reviewed_at >= ?"
        params.append(since)
    row = conn.execute(sql, tuple(params)).fetchone()
    return int(row["n"] if row is not None else 0)


def record_review_run(
    review_run_id: str,
    session: str,
    subject_path: str,
    subject_kind: str = "task_plan",
    review_mode: str = "planning",
    verdict: str | None = None,
    verdict_decision: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    *,
    max_review_rounds: int = DEFAULT_MAX_REVIEW_ROUNDS,
) -> dict:
    review_run_id = (review_run_id or "").strip()
    session = (session or "").strip()
    if not review_run_id:
        return _envelope(
            ok=False, tool="record_review_run", data={"error": "review_run_id is required."}, entity="review_run"
        )
    if not session:
        return _envelope(
            ok=False, tool="record_review_run", data={"error": "session is required."}, entity="review_run"
        )
    canonical_subject = _canonicalize_subject_path(subject_path)
    if canonical_subject is None:
        return _envelope(
            ok=False, tool="record_review_run", data={"error": "subject_path is required."}, entity="review_run"
        )
    if subject_kind not in _REVIEW_RUN_SUBJECT_KINDS:
        return _envelope(
            ok=False,
            tool="record_review_run",
            data={
                "error": f"Invalid subject_kind '{subject_kind}'. Valid: {', '.join(sorted(_REVIEW_RUN_SUBJECT_KINDS))}",
            },
            entity="review_run",
        )
    normalized_review_mode, error = _validate_review_mode_or_envelope(
        review_mode,
        tool="record_review_run",
        entity="review_run",
    )
    if error is not None:
        return error
    if verdict is not None and verdict not in _REVIEW_RUN_VERDICTS:
        return _envelope(
            ok=False,
            tool="record_review_run",
            data={"error": f"Invalid verdict '{verdict}'. Valid: {', '.join(sorted(_REVIEW_RUN_VERDICTS))}"},
            entity="review_run",
        )
    try:
        requested_bound = int(max_review_rounds)
    except (TypeError, ValueError):
        return _envelope(
            ok=False,
            tool="record_review_run",
            data={"error": "max_review_rounds must be a positive integer."},
            entity="review_run",
        )
    if requested_bound < 1:
        return _envelope(
            ok=False,
            tool="record_review_run",
            data={"error": "max_review_rounds must be a positive integer."},
            entity="review_run",
        )
    round_bound = min(requested_bound, DEFAULT_MAX_REVIEW_ROUNDS)
    normalized_task_ref = _normalize_optional_text(task_ref)
    if normalized_task_ref is None:
        return _envelope(
            ok=False,
            tool="record_review_run",
            data={
                "error": "task_ref is required for record_review_run. Pass task_ref explicitly; no active-task fallback exists.",
            },
            entity="review_run",
        )
    stored_verdict = verdict
    stored_verdict_decision = verdict_decision
    extra_data: dict[str, object] = {}
    # Actor resolution may inspect git. Keep it outside the RESERVED write
    # transaction so no subprocess runs while SQLite is write-locked.
    with _get_db_connection() as conn:
        known_task_ref = _canonical_task_ref_from_handoff(conn, normalized_task_ref)
        if known_task_ref is None:
            return _envelope(
                ok=False,
                tool="record_review_run",
                data={
                    "blocked": True,
                    "error": "unknown_task_ref",
                    "remedy": _UNKNOWN_TASK_REF_REMEDY,
                },
                task_ref=normalized_task_ref,
                entity="review_run",
            )
        normalized_task_ref = known_task_ref
        resolved_actor = _resolve_write_actor(conn, actor, task_ref=normalized_task_ref)
        warnings = collect_target_context_warnings(conn, resolved_actor, task_ref=normalized_task_ref)

    with _get_db_connection(begin_immediate=True) as conn:
        existing = conn.execute("SELECT id FROM review_runs WHERE review_run_id = ?", (review_run_id,)).fetchone()
        if existing is not None:
            return _envelope(
                ok=False,
                tool="record_review_run",
                data={
                    "error": f"review_run_id '{review_run_id}' already exists (id={existing['id']}). Use a unique id for each run.",
                },
                task_ref=normalized_task_ref,
                entity="review_run",
            )
        canonical_subject = _existing_canonical_subject_path(conn, normalized_task_ref, canonical_subject)
        if ENFORCE_REVIEW_ROUND_BOUND:
            prior_rounds = _count_prior_review_rounds(
                conn,
                normalized_task_ref,
                canonical_subject,
                normalized_review_mode,
                since=REVIEW_ROUND_BOUND_EPOCH,
            )
            if prior_rounds >= round_bound:
                if stored_verdict != "fail":
                    return _envelope(
                        ok=False,
                        tool="record_review_run",
                        data={
                            "blocked": True,
                            "error": "review_rounds_exhausted",
                            "remedy": (
                                f"Review rounds exhausted for task_ref={normalized_task_ref} "
                                f"subject_path={canonical_subject} (observed {prior_rounds}, "
                                f"max {round_bound}). Only verdict=fail is accepted; "
                                f"it is recorded with reason {_REVIEW_EXHAUSTED_REASON}."
                            ),
                            "observed_count": prior_rounds,
                            "max_review_rounds": round_bound,
                        },
                        task_ref=normalized_task_ref,
                        entity="review_run",
                    )
                stored_verdict_decision = _REVIEW_EXHAUSTED_REASON
                extra_data["reason"] = _REVIEW_EXHAUSTED_REASON
                extra_data["observed_count"] = prior_rounds
                extra_data["max_review_rounds"] = round_bound
        if stored_verdict in _SUCCESS_VERDICTS:
            subject_high, subject_low_med, task_high, task_low_med = _open_findings_for_severity_gate(
                conn, normalized_task_ref, canonical_subject, subject_kind
            )
            extra_data["advisory_task_open_high_count"] = len(task_high)
            extra_data["advisory_task_open_low_medium_count"] = len(task_low_med)
            if subject_high:
                return _envelope(
                    ok=False,
                    tool="record_review_run",
                    data={
                        "blocked": True,
                        "error": "open_high_findings_block_pass_verdict",
                        "remedy": _HIGH_SUCCESS_VERDICT_REMEDY,
                        "high_finding_ids": subject_high,
                        "open_high_count": len(subject_high),
                        "open_low_medium_count": len(subject_low_med),
                        "advisory_task_open_high_count": len(task_high),
                        "advisory_task_open_low_medium_count": len(task_low_med),
                    },
                    task_ref=normalized_task_ref,
                    entity="review_run",
                )
            if subject_low_med:
                extra_data["advisory_open_low_medium_finding_ids"] = subject_low_med
                extra_data["advisory_open_low_medium_count"] = len(subject_low_med)
        findings_recorded_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM review_findings WHERE task_ref = ? AND session = ?",
                (normalized_task_ref, session),
            ).fetchone()[0]
        )
        extra_data["findings_recorded_count"] = findings_recorded_count
        conn.execute(
            """
            INSERT INTO review_runs (
                review_run_id, task_ref, subject_path, subject_kind, review_mode,
                verdict_decision, verdict, session, agent, model, model_label,
                branch, commit_sha
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                review_run_id,
                normalized_task_ref,
                canonical_subject,
                subject_kind,
                normalized_review_mode,
                stored_verdict_decision,
                stored_verdict,
                session,
                resolved_actor.agent,
                resolved_actor.model,
                resolved_actor.model_label,
                resolved_actor.branch,
                resolved_actor.commit_sha,
            ),
        )
        row = dict(conn.execute("SELECT * FROM review_runs WHERE review_run_id = ?", (review_run_id,)).fetchone())
        task_revision = _current_task_revision(conn, normalized_task_ref)
    return _envelope(
        ok=True,
        tool="record_review_run",
        data={"review_run": row, **extra_data},
        task_ref=normalized_task_ref,
        entity="review_run",
        mutation={
            "entity": "review_run",
            "operation": "insert",
            "affected_ids": [row["id"]],
            "affected_keys": [review_run_id],
            "task_revision": task_revision,
        },
        warnings=warnings or None,
    )


def list_review_runs(
    task_ref: str | None = None,
    subject_path: str | None = None,
    limit: int = 20,
    offset: int = 0,
    review_mode: str | None = None,
    verdict: str | None = None,
) -> dict:
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    if verdict is not None and verdict not in _REVIEW_RUN_VERDICTS:
        return _envelope(
            ok=False,
            tool="list_review_runs",
            data={"error": f"Invalid verdict '{verdict}'. Valid: {', '.join(sorted(_REVIEW_RUN_VERDICTS))}"},
            entity="review_run",
        )
    normalized_review_mode = None
    if review_mode is not None:
        normalized_review_mode, error = _validate_review_mode_or_envelope(
            review_mode,
            tool="list_review_runs",
            entity="review_run",
        )
        if error is not None:
            return error
    with _get_db_connection() as conn:
        where_parts: list[str] = []
        params: list[object] = []
        if task_ref is not None:
            known_task_ref = _canonical_task_ref_from_handoff(conn, task_ref)
            where_parts.append("task_ref = ?")
            params.append(known_task_ref if known_task_ref is not None else task_ref)
        if subject_path is not None:
            canonical_subject = _canonicalize_subject_path(subject_path)
            filter_subject = canonical_subject if canonical_subject is not None else subject_path
            where_parts.append("lower(subject_path) = lower(?)")
            params.append(filter_subject)
        if normalized_review_mode is not None:
            where_parts.append("review_mode = ?")
            params.append(normalized_review_mode)
        if verdict is not None:
            where_parts.append("verdict = ?")
            params.append(verdict)
        where_sql = " AND ".join(where_parts) if where_parts else "1=1"
        total, raw_runs = _paginated_query(
            conn,
            "review_runs",
            where_sql,
            tuple(params),
            limit,
            offset,
            "reviewed_at DESC, id DESC",
        )
    return _envelope(
        ok=True,
        tool="list_review_runs",
        data={
            "filters": {
                "task_ref": task_ref,
                "subject_path": subject_path,
                "review_mode": normalized_review_mode,
                "verdict": verdict,
                "limit": limit,
                "offset": offset,
            },
            "total_matching": total,
            "returned": len(raw_runs),
            "has_more": (offset + len(raw_runs)) < total,
            "runs": raw_runs,
        },
        task_ref=task_ref,
        entity="review_run",
    )


def get_review_coverage(
    task_ref: str | None = None,
    subject_path: str | None = None,
) -> dict:
    if task_ref is None and subject_path is None:
        return _envelope(
            ok=False,
            tool="get_review_coverage",
            data={"error": "Provide at least one of task_ref or subject_path."},
            entity="review_coverage",
        )
    with _get_db_connection() as conn:
        payload = _collect_review_coverage(conn, task_ref=task_ref, subject_path=subject_path)
    is_ok = bool(payload.get("ok", False))
    data = {key: value for key, value in payload.items() if key != "ok"}
    return _envelope(
        ok=is_ok,
        tool="get_review_coverage",
        data=data,
        task_ref=task_ref,
        entity="review_coverage",
    )


def _collect_review_coverage(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
    subject_path: str | None = None,
) -> dict[str, object]:
    if task_ref is None and subject_path is None:
        return {"ok": False, "error": "Provide at least one of task_ref or subject_path."}

    run_where_parts: list[str] = []
    run_params: list[object] = []
    coverage_task_ref: str | None = None
    if task_ref is not None:
        coverage_task_ref = _canonical_task_ref(conn, task_ref)
        run_where_parts.append("task_ref = ?")
        run_params.append(coverage_task_ref)
    if subject_path is not None:
        canonical_subject = _canonicalize_subject_path(subject_path)
        filter_subject = canonical_subject if canonical_subject is not None else subject_path
        run_where_parts.append("lower(subject_path) = lower(?)")
        run_params.append(filter_subject)
    run_where_sql = " AND ".join(run_where_parts)
    runs = [
        dict(row)
        for row in conn.execute(
            f"SELECT * FROM review_runs WHERE {run_where_sql} ORDER BY reviewed_at DESC, id DESC",
            tuple(run_params),
        ).fetchall()
    ]
    latest_run = runs[0] if runs else None
    latest_review_run_id = latest_run["review_run_id"] if latest_run else None
    latest_verdict = latest_run["verdict"] if latest_run else None
    recent_run_ids = [run["review_run_id"] for run in runs[:5]]

    open_severity_counts: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
    reopened_count = 0
    if coverage_task_ref is not None:
        for row in conn.execute(
            "SELECT severity, COUNT(*) AS cnt FROM review_findings WHERE task_ref = ? AND status = 'open' GROUP BY severity",
            (coverage_task_ref,),
        ).fetchall():
            open_severity_counts[str(row["severity"])] = int(row["cnt"])
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM review_findings WHERE task_ref = ? AND reopen_count > 0",
            (coverage_task_ref,),
        ).fetchone()
        reopened_count = int(row["cnt"]) if row else 0
    elif runs:
        run_ids = [run["review_run_id"] for run in runs]
        placeholders = ",".join("?" * len(run_ids))
        for row in conn.execute(
            f"SELECT severity, COUNT(*) AS cnt FROM review_findings WHERE review_run_id IN ({placeholders}) AND status = 'open' GROUP BY severity",
            tuple(run_ids),
        ).fetchall():
            open_severity_counts[str(row["severity"])] = int(row["cnt"])
        row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM review_findings WHERE review_run_id IN ({placeholders}) AND reopen_count > 0",
            tuple(run_ids),
        ).fetchone()
        reopened_count = int(row["cnt"]) if row else 0

    return {
        "ok": True,
        "task_ref": task_ref,
        "subject_path": subject_path,
        "run_count": len(runs),
        "latest_review_run_id": latest_review_run_id,
        "latest_verdict": latest_verdict,
        "recent_run_ids": recent_run_ids,
        "open_findings_by_severity": open_severity_counts,
        "reopened_findings_count": reopened_count,
    }
