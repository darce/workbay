"""Commit-backed review-finding resolution outcome classifier.

Pure helper that maps a (finding, workspace) state into a structured
``FindingResolutionOutcome``. The classifier owns the decision contract
shared by ``review_findings(operation="update", status="fixed")`` and the
forthcoming ``review_findings(operation="resolve")`` entry point. It performs
no I/O and no DB writes; callers translate outcomes into envelopes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ResolutionOutcomeKind(str, Enum):
    FIXED = "fixed"
    # internal: lifecycle-flag-on equivalent of FIXED. Emitted only when the
    # caller passes ``lifecycle_states_enabled=True`` and the same descendant /
    # same-with-verified guard would otherwise have emitted FIXED. INTEGRATED
    # is not produced by this classifier — it is integrate-managed.
    RESOLVED_ON_BRANCH = "resolved_on_branch"
    PENDING_UNCOMMITTED = "pending_uncommitted"
    STILL_OPEN = "still_open"
    BLOCKED_BY_CONTEXT = "blocked_by_context"
    ERROR = "error"


@dataclass(frozen=True)
class FindingResolutionOutcome:
    kind: ResolutionOutcomeKind
    reason: str | None = None
    verified_commit_sha: str | None = None
    finding_commit_sha: str | None = None
    workspace_commit_sha: str | None = None
    commit_relation: str | None = None
    # internal: anchor commit for a successful resolution. Populated only
    # when ``kind`` is RESOLVED_ON_BRANCH (i.e., flag-on close); FIXED keeps
    # this None so legacy consumers do not start depending on it before the
    # rollout completes.
    resolution_anchor_commit: str | None = None


def classify_resolution_outcome(
    *,
    finding_commit_sha: str | None,
    workspace_commit_sha: str | None,
    verified_commit_sha: str | None,
    commit_relation: str,
    has_uncommitted_changes: bool,
    lifecycle_states_enabled: bool = False,
) -> FindingResolutionOutcome:
    if has_uncommitted_changes and verified_commit_sha is None:
        return FindingResolutionOutcome(
            kind=ResolutionOutcomeKind.PENDING_UNCOMMITTED,
            reason=(
                "Workspace has uncommitted changes; commit the fix and rerun the "
                "resolution command with verified_commit_sha to mark the finding fixed."
            ),
            finding_commit_sha=finding_commit_sha,
            workspace_commit_sha=workspace_commit_sha,
            commit_relation=commit_relation,
        )
    if commit_relation in {"diverged", "ancestor"}:
        return FindingResolutionOutcome(
            kind=ResolutionOutcomeKind.BLOCKED_BY_CONTEXT,
            reason=(
                f"Workspace commit relation is '{commit_relation}'; a finding can only be "
                "marked fixed from the same commit or a descendant commit."
            ),
            finding_commit_sha=finding_commit_sha,
            workspace_commit_sha=workspace_commit_sha,
            commit_relation=commit_relation,
        )
    if commit_relation == "descendant":
        if verified_commit_sha is None:
            return FindingResolutionOutcome(
                kind=ResolutionOutcomeKind.BLOCKED_BY_CONTEXT,
                reason=("verified_commit_sha is required when fixing a finding from a newer descendant commit."),
                finding_commit_sha=finding_commit_sha,
                workspace_commit_sha=workspace_commit_sha,
                commit_relation=commit_relation,
            )
        if workspace_commit_sha is not None and verified_commit_sha != workspace_commit_sha:
            return FindingResolutionOutcome(
                kind=ResolutionOutcomeKind.BLOCKED_BY_CONTEXT,
                reason=(
                    "verified_commit_sha must match the current workspace commit when "
                    "resolving from a newer descendant commit."
                ),
                finding_commit_sha=finding_commit_sha,
                workspace_commit_sha=workspace_commit_sha,
                verified_commit_sha=verified_commit_sha,
                commit_relation=commit_relation,
            )
        return FindingResolutionOutcome(
            kind=(
                ResolutionOutcomeKind.RESOLVED_ON_BRANCH if lifecycle_states_enabled else ResolutionOutcomeKind.FIXED
            ),
            verified_commit_sha=verified_commit_sha,
            finding_commit_sha=finding_commit_sha,
            workspace_commit_sha=workspace_commit_sha,
            commit_relation=commit_relation,
            resolution_anchor_commit=verified_commit_sha if lifecycle_states_enabled else None,
        )
    if commit_relation == "same":
        if verified_commit_sha is not None:
            return FindingResolutionOutcome(
                kind=(
                    ResolutionOutcomeKind.RESOLVED_ON_BRANCH
                    if lifecycle_states_enabled
                    else ResolutionOutcomeKind.FIXED
                ),
                verified_commit_sha=verified_commit_sha,
                finding_commit_sha=finding_commit_sha,
                workspace_commit_sha=workspace_commit_sha,
                commit_relation=commit_relation,
                resolution_anchor_commit=verified_commit_sha if lifecycle_states_enabled else None,
            )
        return FindingResolutionOutcome(
            kind=ResolutionOutcomeKind.STILL_OPEN,
            reason=("Workspace commit matches the finding commit and no fix has been made; the finding remains open."),
            finding_commit_sha=finding_commit_sha,
            workspace_commit_sha=workspace_commit_sha,
            commit_relation=commit_relation,
        )
    if commit_relation == "unknown":
        return FindingResolutionOutcome(
            kind=ResolutionOutcomeKind.BLOCKED_BY_CONTEXT,
            reason=(
                "Could not determine commit ancestry between the finding and the current workspace commit. "
                "Record or resolve the relevant commit SHA, then retry the resolution command."
            ),
            finding_commit_sha=finding_commit_sha,
            workspace_commit_sha=workspace_commit_sha,
            commit_relation=commit_relation,
        )
    return FindingResolutionOutcome(
        kind=ResolutionOutcomeKind.ERROR,
        reason=f"Unknown commit relation: {commit_relation!r}",
        finding_commit_sha=finding_commit_sha,
        workspace_commit_sha=workspace_commit_sha,
        commit_relation=commit_relation,
    )
