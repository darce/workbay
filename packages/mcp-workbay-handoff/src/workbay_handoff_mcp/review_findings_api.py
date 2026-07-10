from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, Field, TypeAdapter

from .api_contract_shared import ActorParam, TaskRefParam, dump_actor
from .core import ReviewFindingDetails
from .review_findings import (
    BatchFindingItem,
    batch_record_review_findings,
    list_review_findings,
    merge_review_findings,
    record_review_finding,
)
from .review_findings import (
    integrate_review_findings as _integrate_review_findings,
)
from .review_findings import (
    reanchor_review_finding as _reanchor_review_finding,
)
from .review_findings import (
    repair_review_finding_provenance as _repair_review_finding_provenance,
)
from .review_findings import (
    resolve_review_findings as _resolve_review_findings,
)
from .review_findings import (
    update_review_finding as _update_review_finding,
)


class ReviewFindingDetailsInput(BaseModel):
    line_start: Annotated[int | None, Field(description="Optional 1-based start line for the finding.")] = None
    line_end: Annotated[int | None, Field(description="Optional 1-based end line for the finding.")] = None
    fix: Annotated[str | None, Field(description="Optional suggested fix text.")] = None


class ReviewFindingBatchItemInput(BaseModel):
    finding_id: Annotated[str, Field(description="Stable finding identifier to persist or reopen.")]
    severity: Annotated[Literal["high", "medium", "low"], Field(description="Finding severity.")]
    file_path: Annotated[str, Field(description="Workspace-relative file path for the finding.")]
    description: Annotated[str, Field(description="Human-readable finding description.")]
    review_mode: Annotated[
        Literal["branch", "release_audit", "planning"] | None,
        Field(description="Optional review mode label for the finding."),
    ] = None
    details: Annotated[
        ReviewFindingDetailsInput | None,
        Field(description="Optional structured line/fix metadata for the finding."),
    ] = None


class ReviewFindingsRecordOp(BaseModel):
    operation: Literal["record"]
    session: Annotated[str, Field(description="Session identifier for the review-finding write.")]
    finding_id: Annotated[str, Field(description="Stable finding identifier to persist or reopen.")]
    severity: Annotated[Literal["high", "medium", "low"], Field(description="Finding severity.")]
    file_path: Annotated[str, Field(description="Workspace-relative file path for the finding.")]
    description: Annotated[str, Field(description="Human-readable finding description.")]
    details: Annotated[
        ReviewFindingDetailsInput | None,
        Field(description="Optional structured line/fix metadata for the finding."),
    ] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None
    review_mode: Annotated[
        Literal["branch", "release_audit", "planning"] | None,
        Field(description="Optional review mode label for the finding."),
    ] = None


class ReviewFindingsBatchRecordOp(BaseModel):
    operation: Literal["batch_record"]
    session: Annotated[str, Field(description="Session identifier for the batch review-finding write.")]
    findings: Annotated[
        list[ReviewFindingBatchItemInput],
        Field(description="One or more review findings to write atomically."),
    ]
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class ReviewFindingsUpdateOp(BaseModel):
    operation: Literal["update"]
    status: Annotated[
        Literal[
            "open",
            "fixed",
            "deferred",
            "wontfix",
            "resolved_on_branch",
            "integrated",
            "superseded",
        ],
        Field(
            description=(
                "New finding status to apply. Note: 'integrated' is integrate-managed and "
                "rejected on direct write (use operation='integrate'), and 'superseded' is "
                "merge-managed and rejected on direct write (use operation='merge' with "
                "retire_sources). 'resolved_on_branch' is write-derived from 'fixed' when "
                "the lifecycle feature flag is on."
            ),
        ),
    ]
    finding_id: Annotated[str | None, Field(description="Stable finding identifier to update.")] = None
    finding_db_id: Annotated[int | None, Field(description="Numeric finding id to update.")] = None
    resolution_notes: Annotated[
        str | None,
        Field(description="Optional notes describing how the finding was resolved or dispositioned."),
    ] = None
    reopen_reason: Annotated[
        str | None,
        Field(description="Required when moving a non-open finding back to open."),
    ] = None
    task_ref: TaskRefParam = None
    session: Annotated[str | None, Field(description="Optional session identifier for the update.")] = None
    actor: ActorParam = None
    verified_commit_sha: Annotated[
        str | None,
        Field(description="Optional commit SHA that verified a fixed finding."),
    ] = None
    verification_evidence: Annotated[
        str | None,
        Field(description="Optional verification evidence used when closing a finding as fixed."),
    ] = None


class ReviewFindingsResolveOp(BaseModel):
    operation: Literal["resolve"]
    task_ref: TaskRefParam = None
    session: Annotated[str | None, Field(description="Optional session identifier for resolve-driven updates.")] = None
    finding_ids: Annotated[
        list[str] | None,
        Field(description="Optional explicit finding ids to reconcile. Mutually exclusive with all_open."),
    ] = None
    all_open: Annotated[
        bool,
        Field(description="When true, reconcile all open findings for the resolved task."),
    ] = False
    resolution_notes: Annotated[
        str | None,
        Field(description="Optional human-authored resolution notes to apply when descendant fixes are closed."),
    ] = None
    verification_evidence: Annotated[
        str | None,
        Field(description="Optional verification evidence applied to each fixed finding in the resolve batch."),
    ] = None
    actor: ActorParam = None


class ReviewFindingsRepairProvenanceOp(BaseModel):
    operation: Literal["repair_provenance"]
    session: Annotated[
        str,
        Field(description="Session identifier for the repair audit-trail decision row."),
    ]
    finding_id: Annotated[
        str,
        Field(description="Stable finding identifier of the row whose source branch/commit_sha must be repaired."),
    ]
    expected_branch: Annotated[
        str,
        Field(
            description="The branch currently stored on the row. The repair refuses to apply unless this matches exactly — concurrency / mistake guard."
        ),
    ]
    expected_commit_sha: Annotated[
        str,
        Field(
            description="The commit_sha currently stored on the row (full or abbreviated; auto-expanded). Must match exactly after expansion."
        ),
    ]
    new_branch: Annotated[
        str,
        Field(description="The corrected branch the row should reference."),
    ]
    new_commit_sha: Annotated[
        str,
        Field(
            description="The corrected commit_sha. Validated against the active git repo and auto-expanded to its 40-char form."
        ),
    ]
    reason: Annotated[
        str,
        Field(
            description="At least 20 characters explaining why the original attribution was wrong. Recorded in the audit decision row.",
            min_length=20,
        ),
    ]
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class ReviewFindingsMergeOp(BaseModel):
    operation: Literal["merge"]
    source_task_refs: Annotated[
        list[str],
        Field(
            description=(
                "Non-empty list of source task_refs whose review findings should be merged "
                "into the coordinator target_task_ref. Duplicate entries are deduplicated."
            )
        ),
    ]
    target_task_ref: Annotated[
        str,
        Field(description="Coordinator task_ref under which the merged rows should live."),
    ]
    session: Annotated[
        str | None,
        Field(
            description=(
                "Optional session prefix for merged rows. When omitted, auto-generated "
                "as merge-<target_task_ref>-<utc-ts>."
            )
        ),
    ] = None
    retire_sources: Annotated[
        bool,
        Field(
            description=(
                "When true (default), retire merged source rows to status='superseded' in the "
                "same transaction. Set false for legacy additive-only merge behavior."
            )
        ),
    ] = True
    actor: ActorParam = None


class ReviewFindingsIntegrateOp(BaseModel):
    """internal: lifecycle promotion entry point.

    Distinct from ``operation='reconcile'`` (internal orchestrator
    integrity/dedup). The integrate op promotes ``resolved_on_branch``
    findings whose anchor commit is reachable from ``integration_ref`` HEAD
    to ``status='integrated'``.
    """

    operation: Literal["integrate"]
    task_ref: TaskRefParam = None
    integration_ref: Annotated[
        str,
        Field(description="Git ref representing the integration branch HEAD (default: 'main')."),
    ] = "main"
    actor: ActorParam = None


class ReviewFindingsDispositionOp(BaseModel):
    """Admin disposition for findings on done/archived/orphan task_refs (no live worktree required)."""

    operation: Literal["disposition"]
    task_ref: Annotated[str, Field(description="Task ref owning the finding row.")]
    finding_id: Annotated[str, Field(description="Stable finding identifier to disposition.")]
    status: Annotated[
        Literal["deferred", "wontfix", "fixed"],
        Field(description="Terminal disposition status to apply."),
    ]
    resolution_notes: Annotated[
        str | None,
        Field(description="Required notes explaining the disposition decision."),
    ] = None
    disposition_evidence: Annotated[
        str | None,
        Field(
            description=(
                "Machine-readable provenance (e.g. rename-map entry old→new) recorded into "
                "resolution_notes. implementation note bulk-disposition trail."
            ),
        ),
    ] = None
    actor: ActorParam = None


class ReviewFindingsReanchorOp(BaseModel):
    """Open-preserving file_path rewrite (implementation note re-anchor).

    Status stays ``open``; only the anchor path is corrected. Not a terminal
    disposition — use ``disposition`` for deferred/wontfix/fixed.
    """

    operation: Literal["reanchor"]
    task_ref: Annotated[str, Field(description="Task ref owning the finding row.")]
    finding_id: Annotated[str, Field(description="Stable finding identifier to re-anchor.")]
    file_path: Annotated[
        str,
        Field(description="Corrected workspace-relative file path (open-preserving rewrite)."),
    ]
    expected_file_path: Annotated[
        str | None,
        Field(
            description=(
                "Optional concurrency guard: refuse if live file_path differs from this "
                "expected value (classify-time path)."
            ),
        ),
    ] = None
    resolution_notes: Annotated[
        str | None,
        Field(description="Optional provenance note (e.g. rename-map evidence)."),
    ] = None
    actor: ActorParam = None


class ReviewFindingsListOp(BaseModel):
    operation: Literal["list"]
    task_ref: TaskRefParam = None
    status: Annotated[str, Field(description="Finding status filter.")] = "all"
    severity: Annotated[str, Field(description="Finding severity filter.")] = "all"
    limit: Annotated[int, Field(description="Maximum number of findings to return.")] = 100
    offset: Annotated[int, Field(description="Pagination offset.")] = 0
    review_mode: Annotated[
        Literal["branch", "release_audit", "planning"] | None,
        Field(description="Optional review-mode filter."),
    ] = None
    finding_id: Annotated[str | None, Field(description="Optional stable finding identifier lookup.")] = None
    finding_db_id: Annotated[int | None, Field(description="Optional numeric finding id lookup.")] = None
    detail: Annotated[
        Literal["full", "summary"],
        Field(description="Detail level for returned finding rows."),
    ] = "full"


ReviewFindingsParam = Annotated[
    ReviewFindingsRecordOp
    | ReviewFindingsBatchRecordOp
    | ReviewFindingsUpdateOp
    | ReviewFindingsResolveOp
    | ReviewFindingsRepairProvenanceOp
    | ReviewFindingsMergeOp
    | ReviewFindingsIntegrateOp
    | ReviewFindingsDispositionOp
    | ReviewFindingsReanchorOp
    | ReviewFindingsListOp,
    Field(discriminator="operation"),
]

_ValidatedReviewFindingOp = (
    ReviewFindingsRecordOp
    | ReviewFindingsBatchRecordOp
    | ReviewFindingsUpdateOp
    | ReviewFindingsResolveOp
    | ReviewFindingsRepairProvenanceOp
    | ReviewFindingsMergeOp
    | ReviewFindingsIntegrateOp
    | ReviewFindingsDispositionOp
    | ReviewFindingsReanchorOp
    | ReviewFindingsListOp
)

_REVIEW_FINDINGS_ADAPTER: TypeAdapter[_ValidatedReviewFindingOp] = TypeAdapter(ReviewFindingsParam)


def _dump_review_finding_details(details: ReviewFindingDetailsInput | None) -> dict[str, Any] | None:
    if details is None:
        return None
    payload = details.model_dump(exclude_none=True)
    return payload or None


def _dump_batch_review_finding_item(item: ReviewFindingBatchItemInput) -> dict[str, Any]:
    payload = item.model_dump(exclude_none=True)
    if item.details is not None:
        payload["details"] = _dump_review_finding_details(item.details)
    return payload


def _compose_disposition_resolution_notes(
    resolution_notes: str | None,
    disposition_evidence: str | None,
) -> str | None:
    """Fold disposition_evidence provenance into resolution_notes (implementation note)."""
    notes = (resolution_notes or "").strip() or None
    evidence = (disposition_evidence or "").strip() or None
    if evidence is None:
        return notes
    evidence_clause = f"plan:0097 disposition_evidence={evidence}"
    if notes is None:
        return evidence_clause
    if evidence_clause in notes or f"disposition_evidence={evidence}" in notes:
        return notes
    return f"{notes}; {evidence_clause}"


def _validate_review_findings(review: ReviewFindingsParam) -> _ValidatedReviewFindingOp:
    return _REVIEW_FINDINGS_ADAPTER.validate_python(review)


def review_findings(
    review: Annotated[
        ReviewFindingsParam,
        Field(
            description=(
                "Typed review-findings payload. operation selects one of the record, batch_record, "
                "update, resolve, repair_provenance, merge, integrate, disposition, reanchor, or list variants."
            )
        ),
    ],
) -> dict:
    review_payload = _validate_review_findings(review)
    if isinstance(review_payload, ReviewFindingsRecordOp):
        return record_review_finding(
            session=review_payload.session,
            finding_id=review_payload.finding_id,
            severity=review_payload.severity,
            file_path=review_payload.file_path,
            description=review_payload.description,
            details=cast(ReviewFindingDetails | None, _dump_review_finding_details(review_payload.details)),
            actor=dump_actor(review_payload.actor),
            task_ref=review_payload.task_ref,
            review_mode=review_payload.review_mode,
        )
    if isinstance(review_payload, ReviewFindingsBatchRecordOp):
        return batch_record_review_findings(
            session=review_payload.session,
            findings=cast(
                list[BatchFindingItem], [_dump_batch_review_finding_item(item) for item in review_payload.findings]
            ),
            actor=dump_actor(review_payload.actor),
            task_ref=review_payload.task_ref,
        )
    if isinstance(review_payload, ReviewFindingsUpdateOp):
        return _update_review_finding(
            status=review_payload.status,
            finding_id=review_payload.finding_id,
            finding_db_id=review_payload.finding_db_id,
            resolution_notes=review_payload.resolution_notes,
            reopen_reason=review_payload.reopen_reason,
            task_ref=review_payload.task_ref,
            session=review_payload.session,
            actor=dump_actor(review_payload.actor),
            verified_commit_sha=review_payload.verified_commit_sha,
            verification_evidence=review_payload.verification_evidence,
        )
    if isinstance(review_payload, ReviewFindingsResolveOp):
        return _resolve_review_findings(
            task_ref=review_payload.task_ref,
            session=review_payload.session,
            finding_ids=review_payload.finding_ids,
            all_open=review_payload.all_open,
            resolution_notes=review_payload.resolution_notes,
            verification_evidence=review_payload.verification_evidence,
            actor=dump_actor(review_payload.actor),
        )
    if isinstance(review_payload, ReviewFindingsRepairProvenanceOp):
        return _repair_review_finding_provenance(
            session=review_payload.session,
            finding_id=review_payload.finding_id,
            expected_branch=review_payload.expected_branch,
            expected_commit_sha=review_payload.expected_commit_sha,
            new_branch=review_payload.new_branch,
            new_commit_sha=review_payload.new_commit_sha,
            reason=review_payload.reason,
            task_ref=review_payload.task_ref,
            actor=dump_actor(review_payload.actor),
        )
    if isinstance(review_payload, ReviewFindingsMergeOp):
        return merge_review_findings(
            session=review_payload.session,
            source_task_refs=review_payload.source_task_refs,
            target_task_ref=review_payload.target_task_ref,
            actor=dump_actor(review_payload.actor),
            retire_sources=review_payload.retire_sources,
        )
    if isinstance(review_payload, ReviewFindingsIntegrateOp):
        return _integrate_review_findings(
            task_ref=review_payload.task_ref,
            integration_ref=review_payload.integration_ref,
            actor=dump_actor(review_payload.actor),
        )
    if isinstance(review_payload, ReviewFindingsDispositionOp):
        composed_notes = _compose_disposition_resolution_notes(
            review_payload.resolution_notes,
            review_payload.disposition_evidence,
        )
        return _update_review_finding(
            status=review_payload.status,
            finding_id=review_payload.finding_id,
            task_ref=review_payload.task_ref,
            resolution_notes=composed_notes,
            actor=dump_actor(review_payload.actor),
            allow_missing_worktree_fallback=True,
        )
    if isinstance(review_payload, ReviewFindingsReanchorOp):
        return _reanchor_review_finding(
            finding_id=review_payload.finding_id,
            file_path=review_payload.file_path,
            task_ref=review_payload.task_ref,
            expected_file_path=review_payload.expected_file_path,
            resolution_notes=review_payload.resolution_notes,
            actor=dump_actor(review_payload.actor),
        )
    return list_review_findings(
        task_ref=review_payload.task_ref,
        status=review_payload.status,
        severity=review_payload.severity,
        limit=review_payload.limit,
        offset=review_payload.offset,
        review_mode=review_payload.review_mode,
        finding_id=review_payload.finding_id,
        finding_db_id=review_payload.finding_db_id,
        detail=review_payload.detail,
    )
