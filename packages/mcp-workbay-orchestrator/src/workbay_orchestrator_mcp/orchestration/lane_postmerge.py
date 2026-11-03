"""Event-driven post-merge reap of a lane worktree, branch, and row.

Merged-ness is derived from the live branch tip being an ancestor of the
integration branch (``git merge-base --is-ancestor``). When containment
is false, a second question asks whether every commit the branch
introduces has a change-identity equivalent already on the integration
branch (``git cherry`` / patch-id, not a raw revision comparison).
Equivalent content under different revisions is ``content_landed`` and
a reap candidate; genuinely new commits are ``unlanded_content``, a
merge candidate that carries the unlanded count. Neither outcome
deletes a branch, worktree, or row: that arm classifies only. A cherry
probe failure is ``probe_failed`` with no positive classification, never
a silent default to either class. A recorded ``landing_commit_sha`` on
the integration branch proves only that SHA landed; it does not prove
the live tip is consumed.

Default is dry-run. Dry-run and apply classify the same worktree file set
(ignored files are not dirty). Apply CAS-closes the row against the probed
status and consumed tip, retires that lane's manifest ``owned_paths`` /
``commit_paths``, then removes the worktree without ``--force``,
then deletes the branch with ``git branch -d`` (git itself refuses unmerged
tips), then records an idempotent decision. Failures never count as merged.

If ownership retirement or a git step fails after that CAS close, a
compensating CAS reopens the row to its pre-close status with a
``reap_git_failed`` note so the next apply retries instead of treating
the row as already reaped. A missing task manifest is a typed no-op.
``postmerge_reap`` is recorded only when the worktree and branch are both
gone. A recorder result is fail-closed the same way CAS close is: only
an explicit ``ok is True`` mapping counts, and an unknown shape is a
miss. After git has already removed the checkout and branch, a record
miss restores the consumed ref so the closed row stays retry-classified
until the decision exists. Raised ``_GitError`` (including timeout)
after CAS is caught and routed through that compensation; a missed
reopen is ``reopen_missed`` and the next apply retries git when close
notes were written by this reaper.

A missing branch still names a checkout: dirty and shared probes refuse
without CAS, and apply removes the tree before closing the row. Never
``applied=True`` while the checkout exists.

A merged row never removes a worktree path that another non-terminal row
still names (same batch or a cross-task full-scan of live path owners).
Owners are matched by ``Path.resolve`` identity so trailing-slash and
symlink spellings still count. Lookup or resolve failure is
``shared_path_unverified``, not an empty owner set.

A terminal retry is not an unguarded second remove. ``path.exists()`` is
not evidence of an unfinished reap: a live successor may now occupy that
path. A parseable close-note row is retry work when the branch still
names the consumed tip or the named path is still this row's leftover
checkout. Leftover identity is branch name AND HEAD SHA equal to the
consumed tip; the same branch name at another tip is reuse, not ours.
Identity and leftover-branch probes fail closed: ``_GitError`` or a
nonzero ``rev-parse`` is never leftover unless git names an explicit
absent-working-tree token. A successor that reused the path — including
the same branch name at a different SHA — is idle, not retry, unless the
consumed ref still names the pinned tip. Idle terminals never reach an
actuator that can remove. Retry no-ops unless
``_terminal_needs_retry`` is true, and the present-path arm re-proves
checkout identity immediately before ``git worktree remove``. Retry runs
the same dirty and shared-path probes as a live reap and skips the
remove when any non-terminal row names the resolved path. When identity
says the path is not ours, retry still finishes the leftover branch at
the pinned tip and never removes the occupier. When the worktree is
gone, retry probes the branch. If the ref still names the consumed tip,
it runs ``git branch -d`` with the existing tip pin and records
``postmerge_reap`` only once both the worktree and branch are gone. A
remaining branch after a real ``-d`` miss is ``branch_remains``, never a
recorded reap. A leftover-ref probe timeout is ``probe_failed``, not
``branch_remains``. A retry that finishes leftover git and writes the
reap key is ``retry_reaped``; ``row_already_terminal`` is only the idle
case where retry found nothing to do.
Terminals that need no git or decision retry do not consume
``max_batch``; non-terminal rows are selected first.

The production listing is paged (keyset ``after_id``, OFFSET fallback)
until live or retry work is exhausted. A single ``limit=1000`` page of
recently-closed rows must not hide an older live lane. ``truncated`` is
set when more live/retry work remains, including when the listing
reports ``has_more`` after the batch is full. A ``list_lanes`` envelope
with ``ok is False`` fails the batch as ``probe_failed`` with
``truncated=True``. A missing or non-list ``lanes`` key is malformed.
An empty page with ``has_more``, or ``result is None`` after a prior
``has_more``, is truncated failure, not exhaustion.

Pre-CAS git probes that raise ``_GitError`` (including timeout) become
``probe_failed`` for that lane; the rest of the batch is still visited.

Tech-debt: dirty probe uses porcelain without ``--ignored``, so ignored
sibling files do not themselves refuse a remove.
Tech-debt: ``git worktree remove`` is invoked without ``--force``.
Tech-debt: terminal retry only parses ``post-merge reap of`` close notes.
Tech-debt: missing-branch close does not record a ``postmerge_reap`` decision.
Tech-debt: compensation reopen hardcodes expected_status ``closed``.
"""

from __future__ import annotations

import contextvars
import inspect
import json
import logging
import os
import secrets
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_logger = logging.getLogger(__name__)

__all__ = (
    "CANDIDATE_MERGE",
    "CANDIDATE_REAP",
    "KIND_BRANCH_MISSING",
    "KIND_BRANCH_REMAINS",
    "KIND_CONTENT_LANDED",
    "KIND_MERGED_PLANNED",
    "KIND_MERGED_REAPED",
    "KIND_MERGED_WORKTREE_ABSENT",
    "KIND_MERGED_WORKTREE_DIRTY",
    "KIND_NOT_MERGED",
    "KIND_MANIFEST_RETIRE_FAILED",
    "KIND_PROBE_FAILED",
    "KIND_REOPEN_MISSED",
    "KIND_RETRY_REAPED",
    "KIND_ROW_ALREADY_TERMINAL",
    "KIND_SHARED_PATH_IN_USE",
    "KIND_SHARED_PATH_UNVERIFIED",
    "KIND_UNLANDED_CONTENT",
    "LaneVerdict",
    "PostMergeReport",
    "TERMINAL_STATUSES",
    "VERDICT_KINDS",
    "reap_merged_lanes",
    "reap_preserved_lanes_cross_session",
)

KIND_MERGED_REAPED = "merged_reaped"
KIND_MERGED_PLANNED = "merged_planned"
KIND_MERGED_WORKTREE_DIRTY = "merged_worktree_dirty"
KIND_MERGED_WORKTREE_ABSENT = "merged_worktree_absent"
KIND_NOT_MERGED = "not_merged"
KIND_CONTENT_LANDED = "content_landed"
KIND_UNLANDED_CONTENT = "unlanded_content"
KIND_BRANCH_MISSING = "branch_missing"
KIND_BRANCH_REMAINS = "branch_remains"
KIND_ROW_ALREADY_TERMINAL = "row_already_terminal"
KIND_PROBE_FAILED = "probe_failed"
KIND_MANIFEST_RETIRE_FAILED = "manifest_retire_failed"
KIND_REOPEN_MISSED = "reopen_missed"
KIND_RETRY_REAPED = "retry_reaped"
KIND_SHARED_PATH_UNVERIFIED = "shared_path_unverified"
KIND_SHARED_PATH_IN_USE = "shared_path_in_use"
CANDIDATE_REAP = "reap"
CANDIDATE_MERGE = "merge"

VERDICT_KINDS = frozenset(
    {
        KIND_MERGED_REAPED,
        KIND_MERGED_PLANNED,
        KIND_MERGED_WORKTREE_DIRTY,
        KIND_MERGED_WORKTREE_ABSENT,
        KIND_NOT_MERGED,
        KIND_CONTENT_LANDED,
        KIND_UNLANDED_CONTENT,
        KIND_BRANCH_MISSING,
        KIND_BRANCH_REMAINS,
        KIND_ROW_ALREADY_TERMINAL,
        KIND_PROBE_FAILED,
        KIND_MANIFEST_RETIRE_FAILED,
        KIND_REOPEN_MISSED,
        KIND_RETRY_REAPED,
        KIND_SHARED_PATH_UNVERIFIED,
        KIND_SHARED_PATH_IN_USE,
    }
)

# Non-terminal rows are the reap candidates. ``superseded`` is named by the
# post-merge brief; ``closed_stale`` is the existing reaper terminal.
TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale", "superseded"})

_GIT_TIMEOUT_S = 30
_DEFAULT_MAX_BATCH = 50
_DEFAULT_AGE_FLOOR_SECONDS = 0
_PRESERVED_KINDS = frozenset({"merged_ancestry", "merged_content", "review_output_preserved", "superseded_tombstone"})
_EMPTY_LANE_KIND = "empty_lane"
_EMPTY_LANE_EQUALS_BASE = "empty_lane_branch_equals_manifest_base_sha"
_EMPTY_LANE_FIRST_PARENT = "empty_lane_branch_on_first_parent_lineage"
_EMPTY_LANE_PREDATES_CREATED = "empty_lane_branch_tip_predates_lane_created_at"
_EMPTY_LANE_CREATED_AT_UNRESOLVABLE = "empty_lane_created_at_unresolvable"
_EMPTY_LANE_TIMESTAMP_INCONCLUSIVE = "empty_lane_timestamp_inconclusive"
_EMPTY_LANE_COMMITTER_UNRESOLVABLE = "empty_lane_branch_tip_committer_unresolvable"
_OWN_COMMIT_COUNT_AMBIGUOUS = "own_commit_count_ambiguous_on_first_parent_lineage"
# Tip = preserved base + orchestrator review-payload commits only. The payload
# is regenerable governed context, so the worktree is reclaimable; the branch
# is never deleted because the payload commit is not on the integration ref.
_REVIEW_PAYLOAD_ONLY_REASON = "review_payload_only_over_preserved_base"
_PAYLOAD_BRANCH_KEPT = f"branch kept: {_REVIEW_PAYLOAD_ONLY_REASON}"
_LIST_PAGE_LIMIT = 1000
_LIST_PAGE_CAP = 64
_OMIT = object()
_MISSING_REF_TOKENS = (
    "needed a single revision",
    "unknown revision",
    "bad revision",
    "not a valid object name",
)
_DIRTY_TOKENS = (
    "modified or untracked",
    "untracked files",
    "use --force",
)
_ABSENT_TOKENS = (
    "does not exist",
    "not a working tree",
    "is not a working tree",
)
_UNMERGED_TOKENS = (
    "not fully merged",
    "not merged",
)


@dataclass(frozen=True)
class LaneVerdict:
    """Typed per-lane post-merge outcome. Never a bare bool."""

    lane_id: str
    branch: str
    tip_sha: str | None
    kind: str
    detail: str
    applied: bool
    unlanded_count: int | None = None
    candidate: str | None = None
    vm_marker: dict[str, object] | None = None


@dataclass
class PostMergeReport:
    """Bounded batch result for ``reap_merged_lanes``."""

    verdicts: list[LaneVerdict] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    truncated: bool = False


class _GitError(Exception):
    """A git invocation failed in a way that is not a yes/no ancestry answer."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class _ListingFailed(Exception):
    """``list_lanes`` returned a failed envelope. Never treat as empty complete."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def reap_merged_lanes(
    task_ref: str,
    *,
    root: Path | str,
    integration_branch: str = "main",
    apply: bool = False,
    list_rows: Callable[..., Any] | None = None,
    close_row: Callable[..., Any] | None = None,
    record_decision: Callable[..., Any] | None = None,
    run_git: Callable[..., Any] | None = None,
    list_by_path: Callable[..., Any] | None = None,
    reopen_row: Callable[..., Any] | None = None,
    max_batch: int = _DEFAULT_MAX_BATCH,
    orchestrator_root: Path | str | None = None,
    retire_ownership: Callable[[str], tuple[bool, str]] | None = None,
    state_dir: Path | str | None = None,
) -> PostMergeReport:
    """Reap lanes whose live branch tip is consumed by the integration branch.

    ``apply=False`` (default) writes nothing. Per-lane verdicts are typed; a git
    probe error is never treated as merged. Non-terminal rows are selected
    first. Terminals that need no git or decision retry do not consume
    ``max_batch``. ``truncated`` is set when more work remains, including when
    the listing still reports ``has_more`` after the batch is full. A failed
    ``list_lanes`` envelope is ``probe_failed`` with ``truncated=True``, never
    an empty complete listing. An empty page with ``has_more``, a missing or
    non-list ``lanes`` key, or ``None`` after a prior ``has_more`` is the
    same truncated failure.
    """
    repo = Path(root)
    active_state_dir = _postmerge_state_dir(repo, state_dir)
    git = run_git if run_git is not None else _default_run_git(repo)
    rows_fn = list_rows if list_rows is not None else _default_list_rows()
    closer = close_row if close_row is not None else _default_close_row(task_ref)
    recorder = record_decision if record_decision is not None else _default_record_decision(task_ref)
    path_fn = list_by_path if list_by_path is not None else _default_list_by_path()
    reopener = reopen_row if reopen_row is not None else _default_reopen_row(task_ref)
    retirer = (
        retire_ownership
        if retire_ownership is not None
        else _default_retire_ownership(task_ref, repo=repo, orchestrator_root=orchestrator_root)
    )

    limit = _normalize_max_batch(max_batch)
    try:
        rows, listing_truncated = _page_postmerge_rows(
            rows_fn,
            task_ref,
            repo=repo,
            git=git,
            work_limit=limit,
        )
    except _ListingFailed as exc:
        failed = _verdict(
            "list_lanes",
            "",
            None,
            KIND_PROBE_FAILED,
            exc.detail,
            applied=False,
        )
        return PostMergeReport(
            verdicts=[failed],
            counts=_count_kinds([failed]),
            truncated=True,
        )
    batch, truncated = _select_postmerge_batch(rows, repo=repo, git=git, limit=limit)
    truncated = truncated or listing_truncated

    verdicts: list[LaneVerdict] = []
    for row in batch:
        try:
            verdicts.append(
                _reap_one(
                    row,
                    task_ref=task_ref,
                    integration_branch=integration_branch,
                    apply=apply,
                    git=git,
                    close_row=closer,
                    record_decision=recorder,
                    repo=repo,
                    batch=batch,
                    list_by_path=path_fn,
                    reopen_row=reopener,
                    retire_ownership=retirer,
                    state_dir=active_state_dir,
                )
            )
        except _GitError as exc:
            lane_id = _text(_row_get(row, "lane_id")) or ""
            branch = _text(_row_get(row, "branch")) or ""
            verdicts.append(_verdict(lane_id, branch, None, KIND_PROBE_FAILED, exc.detail, applied=False))
    return PostMergeReport(verdicts=verdicts, counts=_count_kinds(verdicts), truncated=truncated)


def _reap_one(
    row: object,
    *,
    task_ref: str,
    integration_branch: str,
    apply: bool,
    git: Callable[..., Any],
    close_row: Callable[..., Any],
    record_decision: Callable[..., Any],
    repo: Path,
    batch: Sequence[object],
    list_by_path: Callable[..., Any] | None,
    reopen_row: Callable[..., Any],
    retire_ownership: Callable[[str], tuple[bool, str]],
    state_dir: Path | str | None = None,
) -> LaneVerdict:
    lane_id = _text(_row_get(row, "lane_id")) or ""
    branch = _text(_row_get(row, "branch")) or ""
    status = _text(_row_get(row, "status")) or ""
    worktree_path = _text(_row_get(row, "worktree_path"))

    if status in TERMINAL_STATUSES:
        if apply:
            refusal = _retry_missing_reap_decision(
                row,
                lane_id=lane_id,
                branch=branch,
                task_ref=task_ref,
                record_decision=record_decision,
                git=git,
                worktree_path=worktree_path,
                repo=repo,
                integration_branch=integration_branch,
                batch=batch,
                list_by_path=list_by_path,
                state_dir=state_dir,
            )
            if refusal is not None:
                return refusal
        return _verdict(lane_id, branch, None, KIND_ROW_ALREADY_TERMINAL, status or "terminal", applied=False)

    if not branch:
        return _close_missing_branch(
            lane_id=lane_id,
            branch=branch,
            status=status,
            worktree_path=worktree_path,
            apply=apply,
            close_row=close_row,
            git=git,
            repo=repo,
            batch=batch,
            list_by_path=list_by_path,
            task_ref=task_ref,
            detail="branch missing",
        )

    try:
        tip_sha = _probe_tip_sha(git, branch)
    except _GitError as exc:
        return _verdict(lane_id, branch, None, KIND_PROBE_FAILED, exc.detail, applied=False)
    if tip_sha is None:
        return _close_missing_branch(
            lane_id=lane_id,
            branch=branch,
            status=status,
            worktree_path=worktree_path,
            apply=apply,
            close_row=close_row,
            git=git,
            repo=repo,
            batch=batch,
            list_by_path=list_by_path,
            task_ref=task_ref,
            detail="branch missing",
        )

    try:
        merged = _is_ancestor(git, tip_sha, integration_branch)
    except _GitError as exc:
        return _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, exc.detail, applied=False)

    if not merged:
        try:
            unlanded = _unlanded_commit_count(git, tip_sha, integration_branch)
        except _GitError as exc:
            return _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, exc.detail, applied=False)
        if unlanded == 0:
            return _verdict(
                lane_id,
                branch,
                tip_sha,
                KIND_CONTENT_LANDED,
                "content already landed under different revisions",
                applied=False,
                unlanded_count=0,
                candidate=CANDIDATE_REAP,
            )
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_UNLANDED_CONTENT,
            f"unlanded_commits={unlanded}",
            applied=False,
            unlanded_count=unlanded,
            candidate=CANDIDATE_MERGE,
        )

    return _reap_merged(
        lane_id=lane_id,
        branch=branch,
        tip_sha=tip_sha,
        status=status,
        worktree_path=worktree_path,
        task_ref=task_ref,
        integration_branch=integration_branch,
        apply=apply,
        git=git,
        close_row=close_row,
        record_decision=record_decision,
        repo=repo,
        batch=batch,
        list_by_path=list_by_path,
        reopen_row=reopen_row,
        retire_ownership=retire_ownership,
        state_dir=state_dir,
    )


def _reap_merged(
    *,
    lane_id: str,
    branch: str,
    tip_sha: str,
    status: str,
    worktree_path: str | None,
    task_ref: str,
    integration_branch: str,
    apply: bool,
    git: Callable[..., Any],
    close_row: Callable[..., Any],
    record_decision: Callable[..., Any],
    repo: Path,
    batch: Sequence[object],
    list_by_path: Callable[..., Any] | None,
    reopen_row: Callable[..., Any],
    retire_ownership: Callable[[str], tuple[bool, str]],
    state_dir: Path | str | None = None,
) -> LaneVerdict:
    absent, refusal = _live_worktree_refusal(
        lane_id=lane_id,
        branch=branch,
        tip_sha=tip_sha,
        worktree_path=worktree_path,
        task_ref=task_ref,
        repo=repo,
        batch=batch,
        list_by_path=list_by_path,
        git=git,
    )
    if refusal is not None:
        return refusal
    if not apply:
        return _verdict(lane_id, branch, tip_sha, KIND_MERGED_PLANNED, "dry-run", applied=False)

    closed, close_detail = _cas_close(
        close_row,
        lane_id=lane_id,
        expected_status=status,
        notes=_close_notes(branch, tip_sha),
        expected_tip_sha=tip_sha,
    )
    if not closed:
        return _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, f"close_row: {close_detail}", applied=False)

    try:
        retired, retire_detail = retire_ownership(lane_id)
    except Exception as exc:  # noqa: BLE001 — fail closed and compensate
        retired, retire_detail = False, str(exc) or type(exc).__name__
    if not retired:
        return _compensate_git_failure(
            reopen_row,
            lane_id=lane_id,
            branch=branch,
            tip_sha=tip_sha,
            restore_status=status,
            kind=KIND_MANIFEST_RETIRE_FAILED,
            detail=f"manifest: {retire_detail}",
        )

    try:
        if not absent:
            removed, remove_detail, remove_kind = _remove_worktree(git, worktree_path)
            if remove_kind == KIND_MERGED_WORKTREE_DIRTY:
                return _compensate_git_failure(
                    reopen_row,
                    lane_id=lane_id,
                    branch=branch,
                    tip_sha=tip_sha,
                    restore_status=status,
                    kind=KIND_MERGED_WORKTREE_DIRTY,
                    detail=remove_detail,
                )
            if remove_kind == KIND_PROBE_FAILED:
                return _compensate_git_failure(
                    reopen_row,
                    lane_id=lane_id,
                    branch=branch,
                    tip_sha=tip_sha,
                    restore_status=status,
                    kind=KIND_PROBE_FAILED,
                    detail=f"worktree: {remove_detail}",
                )
            if remove_kind == KIND_MERGED_WORKTREE_ABSENT:
                absent = True
            elif not removed:
                return _compensate_git_failure(
                    reopen_row,
                    lane_id=lane_id,
                    branch=branch,
                    tip_sha=tip_sha,
                    restore_status=status,
                    kind=KIND_PROBE_FAILED,
                    detail=f"worktree: {remove_detail}",
                )

        deleted, branch_detail = _delete_branch(git, branch, tip_sha, integration_branch)
        if not deleted:
            return _compensate_git_failure(
                reopen_row,
                lane_id=lane_id,
                branch=branch,
                tip_sha=tip_sha,
                restore_status=status,
                kind=KIND_PROBE_FAILED,
                detail=branch_detail,
            )
    except _GitError as exc:
        return _compensate_git_failure(
            reopen_row,
            lane_id=lane_id,
            branch=branch,
            tip_sha=tip_sha,
            restore_status=status,
            kind=KIND_PROBE_FAILED,
            detail=exc.detail,
        )

    recorded, rec_detail = _record(
        record_decision,
        decision_id=f"postmerge_reap:{task_ref}:{lane_id}:{tip_sha}",
        rationale=_decision_rationale(lane_id, branch, tip_sha),
        task_ref=task_ref,
    )
    if not recorded:
        _restore_consumed_branch(git, branch, tip_sha)
        return _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, f"decision: {rec_detail}", applied=False)

    vm_marker = _write_vm_merged_marker(branch, tip_sha, repo=repo, state_dir=state_dir)
    kind = KIND_MERGED_WORKTREE_ABSENT if absent else KIND_MERGED_REAPED
    return _verdict(lane_id, branch, tip_sha, kind, "reaped", applied=True, vm_marker=vm_marker)


def _live_worktree_refusal(
    *,
    lane_id: str,
    branch: str,
    tip_sha: str | None,
    worktree_path: str | None,
    task_ref: str,
    repo: Path,
    batch: Sequence[object],
    list_by_path: Callable[..., Any] | None,
    git: Callable[..., Any],
) -> tuple[bool, LaneVerdict | None]:
    """Probe absent / dirty / shared. Return ``(absent, refusal_or_none)``."""
    absent = _worktree_is_absent(worktree_path, repo)
    if absent:
        return True, None
    try:
        dirty, dirty_detail, dirty_failed = _worktree_is_dirty(git, worktree_path)
    except _GitError as exc:
        return False, _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, exc.detail, applied=False)
    if dirty_failed:
        return False, _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, dirty_detail, applied=False)
    if dirty:
        return False, _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_MERGED_WORKTREE_DIRTY,
            dirty_detail or "dirty worktree",
            applied=False,
        )
    shared = _shared_path_refusal(
        lane_id=lane_id,
        branch=branch,
        tip_sha=tip_sha,
        worktree_path=worktree_path,
        task_ref=task_ref,
        repo=repo,
        batch=batch,
        list_by_path=list_by_path,
    )
    if shared is not None:
        return False, shared
    return False, None


def _close_missing_branch(
    *,
    lane_id: str,
    branch: str,
    status: str,
    worktree_path: str | None,
    apply: bool,
    close_row: Callable[..., Any],
    git: Callable[..., Any],
    repo: Path,
    batch: Sequence[object],
    list_by_path: Callable[..., Any] | None,
    task_ref: str,
    detail: str,
) -> LaneVerdict:
    absent, refusal = _live_worktree_refusal(
        lane_id=lane_id,
        branch=branch,
        tip_sha=None,
        worktree_path=worktree_path,
        task_ref=task_ref,
        repo=repo,
        batch=batch,
        list_by_path=list_by_path,
        git=git,
    )
    if refusal is not None:
        return refusal
    if not apply:
        return _verdict(lane_id, branch, None, KIND_BRANCH_MISSING, detail, applied=False)

    if not absent:
        try:
            removed, remove_detail, remove_kind = _remove_worktree(git, worktree_path)
        except _GitError as exc:
            return _verdict(lane_id, branch, None, KIND_PROBE_FAILED, exc.detail, applied=False)
        if remove_kind == KIND_MERGED_WORKTREE_DIRTY:
            return _verdict(
                lane_id,
                branch,
                None,
                KIND_MERGED_WORKTREE_DIRTY,
                remove_detail,
                applied=False,
            )
        if remove_kind == KIND_PROBE_FAILED or not removed:
            return _verdict(
                lane_id,
                branch,
                None,
                KIND_PROBE_FAILED,
                f"worktree: {remove_detail}",
                applied=False,
            )

    if not _worktree_is_absent(worktree_path, repo):
        return _verdict(
            lane_id,
            branch,
            None,
            KIND_PROBE_FAILED,
            "worktree still present",
            applied=False,
        )

    notes = f"branch missing: {branch}" if branch else "branch missing"
    closed, close_detail = _cas_close(close_row, lane_id=lane_id, expected_status=status, notes=notes)
    if not closed:
        return _verdict(lane_id, branch, None, KIND_PROBE_FAILED, f"close_row: {close_detail}", applied=False)
    return _verdict(lane_id, branch, None, KIND_BRANCH_MISSING, detail, applied=True)


def _probe_tip_sha(git: Callable[..., Any], branch: str) -> str | None:
    ref = _heads_ref(branch)
    show = _git(git, ["show-ref", "--verify", "--quiet", ref])
    if show.returncode == 1:
        return None
    if show.returncode != 0:
        raise _GitError(_proc_detail(show) or "show-ref failed")
    parsed = _git(git, ["rev-parse", "--verify", "--end-of-options", ref])
    if parsed.returncode != 0:
        if _looks_like_missing_ref(_proc_detail(parsed)):
            return None
        raise _GitError(_proc_detail(parsed) or "rev-parse failed")
    sha = (parsed.stdout or "").strip()
    if not sha:
        raise _GitError("rev-parse returned empty sha")
    return sha


def _is_ancestor(git: Callable[..., Any], commit: str, tip: str) -> bool:
    proc = _git(git, ["merge-base", "--is-ancestor", commit, tip])
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    raise _GitError(_proc_detail(proc) or "merge-base --is-ancestor failed")


def _unlanded_commit_count(
    git: Callable[..., Any],
    tip_sha: str,
    integration_branch: str,
) -> int:
    """Count unique commits on ``tip_sha`` with no equivalent on integration.

    ``git cherry`` compares change identity (patch-id), not raw revisions.
    A nonzero status or an unparseable line is a probe failure, never a
    silent default to landed or unlanded.
    """
    proc = _git(git, ["cherry", integration_branch, tip_sha])
    if proc.returncode != 0:
        raise _GitError(_proc_detail(proc) or "git cherry failed")
    count = 0
    for raw in (proc.stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        mark = line[0]
        if mark == "+":
            count += 1
            continue
        if mark == "-":
            continue
        raise _GitError(f"git cherry: unexpected output {line!r}")
    return count


def _worktree_is_absent(worktree_path: str | None, repo: Path) -> bool:
    if not worktree_path:
        return True
    path = _resolve_worktree(worktree_path, repo)
    try:
        return not path.exists()
    except OSError:
        return False


def _worktree_is_dirty(git: Callable[..., Any], worktree_path: str | None) -> tuple[bool, str, bool]:
    """Shared dry-run/apply dirty probe: git's own porcelain, no ``--ignored``."""
    if not worktree_path:
        return False, "", False
    try:
        proc = _git(
            git,
            ["-C", worktree_path, "status", "--porcelain"],
        )
    except _GitError as exc:
        return False, exc.detail, True
    if proc.returncode != 0:
        detail = _proc_detail(proc) or "worktree status failed"
        if _looks_like_absent(detail):
            return False, detail, False
        return False, detail, True
    porcelain = (proc.stdout or "").strip()
    if porcelain:
        return True, "dirty worktree", False
    return False, "", False


def _shared_path_refusal(
    *,
    lane_id: str,
    branch: str,
    tip_sha: str | None,
    worktree_path: str | None,
    task_ref: str,
    repo: Path,
    batch: Sequence[object],
    list_by_path: Callable[..., Any] | None,
) -> LaneVerdict | None:
    """Fail closed before remove if a live sibling still names this path."""
    key, key_err = _resolved_worktree_key(worktree_path, repo)
    if key_err:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_UNVERIFIED,
            key_err,
            applied=False,
        )
    if key is None:
        return None
    sibling, sibling_err = _same_batch_live_owner(
        lane_id=lane_id,
        path_key=key,
        repo=repo,
        batch=batch,
    )
    if sibling_err:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_UNVERIFIED,
            sibling_err,
            applied=False,
        )
    if sibling is not None:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_IN_USE,
            f"shared with lane {sibling}",
            applied=False,
        )
    if list_by_path is None:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_UNVERIFIED,
            "list_by_path unavailable",
            applied=False,
        )
    try:
        result = _call_list_by_path(list_by_path, worktree_path or "")
    except Exception as exc:  # noqa: BLE001 — path lookup is fail-closed
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_UNVERIFIED,
            str(exc) or type(exc).__name__,
            applied=False,
        )
    owners, err = _owners_from_list_by_path(result)
    if owners is None:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_UNVERIFIED,
            err,
            applied=False,
        )
    for owner in owners:
        owner_id = _text(_row_get(owner, "lane_id"))
        if not owner_id:
            return _verdict(
                lane_id,
                branch,
                tip_sha,
                KIND_SHARED_PATH_UNVERIFIED,
                "list_by_path malformed owner",
                applied=False,
            )
        owner_task = _text(_row_get(owner, "task_ref")) or task_ref
        if owner_id == lane_id and owner_task == task_ref:
            continue
        status = _text(_row_get(owner, "status")) or ""
        if status in TERMINAL_STATUSES:
            continue
        owner_key, owner_err = _resolved_worktree_key(_text(_row_get(owner, "worktree_path")), repo)
        if owner_err:
            return _verdict(
                lane_id,
                branch,
                tip_sha,
                KIND_SHARED_PATH_UNVERIFIED,
                owner_err,
                applied=False,
            )
        if owner_key is None or owner_key != key:
            continue
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_SHARED_PATH_IN_USE,
            f"shared with lane {owner_id}",
            applied=False,
        )
    return None


def _same_batch_live_owner(
    *,
    lane_id: str,
    path_key: str,
    repo: Path,
    batch: Sequence[object],
) -> tuple[str | None, str | None]:
    """Return another non-terminal batch row naming the same resolved path."""
    for row in batch:
        other_id = _text(_row_get(row, "lane_id")) or ""
        if not other_id or other_id == lane_id:
            continue
        status = _text(_row_get(row, "status")) or ""
        if status in TERMINAL_STATUSES:
            continue
        other_key, other_err = _resolved_worktree_key(_text(_row_get(row, "worktree_path")), repo)
        if other_err:
            return None, other_err
        if other_key is not None and other_key == path_key:
            return other_id, None
    return None, None


def _resolved_worktree_key(worktree_path: str | None, repo: Path) -> tuple[str | None, str | None]:
    """Return ``(resolved_key, error)``. Resolve errors fail closed."""
    if not worktree_path:
        return None, None
    path = _resolve_worktree(worktree_path, repo)
    try:
        return str(path.expanduser().resolve()), None
    except OSError as exc:
        return None, str(exc) or "path resolve failed"


def _owners_from_list_by_path(result: object) -> tuple[list[Any] | None, str]:
    if result is None:
        return None, "list_by_path unavailable"
    if isinstance(result, Mapping):
        if result.get("ok") is False:
            err = result.get("error")
            if err is None:
                data = result.get("data")
                if isinstance(data, Mapping):
                    err = data.get("error")
            return None, str(err or "list_by_path failed")
        data = result.get("data", result)
        if isinstance(data, Mapping) and "lanes" in data:
            lanes = data.get("lanes")
            if not isinstance(lanes, list):
                return None, "list_by_path malformed"
            return lanes, ""
        if "lanes" in result:
            lanes = result.get("lanes")
            if not isinstance(lanes, list):
                return None, "list_by_path malformed"
            return list(lanes), ""
        return None, "list_by_path malformed"
    if isinstance(result, (list, tuple)):
        return list(result), ""
    return None, "list_by_path malformed"


def _call_list_by_path(list_by_path: Callable[..., Any], worktree_path: str) -> Any:
    """Prefer a path-free full-scan; fall back to exact-string collaborators."""
    try:
        return list_by_path()
    except TypeError:
        try:
            return list_by_path(worktree_path=worktree_path)
        except TypeError:
            return list_by_path(worktree_path)


def _remove_worktree(git: Callable[..., Any], worktree_path: str | None) -> tuple[bool, str, str | None]:
    if not worktree_path:
        return True, "worktree absent", KIND_MERGED_WORKTREE_ABSENT
    try:
        proc = _git(git, ["worktree", "remove", worktree_path])
    except _GitError as exc:
        return False, exc.detail, KIND_PROBE_FAILED
    if proc.returncode == 0:
        return True, "", None
    detail = _proc_detail(proc) or "git worktree remove failed"
    if _looks_like_dirty(detail):
        return False, detail, KIND_MERGED_WORKTREE_DIRTY
    if _looks_like_absent(detail):
        return True, detail, KIND_MERGED_WORKTREE_ABSENT
    return False, detail, KIND_PROBE_FAILED


def _delete_branch(
    git: Callable[..., Any],
    branch: str,
    tip_sha: str,
    integration_branch: str,
) -> tuple[bool, str]:
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError as exc:
        return False, exc.detail
    if current is None:
        return True, "branch missing"
    if current != tip_sha:
        return False, "ref_moved"
    try:
        if not _is_ancestor(git, tip_sha, integration_branch):
            return False, "not fully merged"
    except _GitError as exc:
        return False, exc.detail
    name = _branch_short_name(branch)
    try:
        proc = _git(git, ["branch", "-d", name])
    except _GitError as exc:
        return False, exc.detail
    if proc.returncode == 0:
        return True, ""
    detail = _proc_detail(proc) or "git branch -d failed"
    lowered = detail.lower()
    if _looks_like_unmerged(lowered):
        return False, "not fully merged"
    if _looks_like_missing_ref(detail) or "not found" in lowered:
        return True, detail
    return False, f"branch: {detail}"


def _compensate_git_failure(
    reopen_row: Callable[..., Any],
    *,
    lane_id: str,
    branch: str,
    tip_sha: str,
    restore_status: str,
    kind: str,
    detail: str,
) -> LaneVerdict:
    """Reopen the CAS-closed row so a later apply retries git, not a false reap."""
    note = f"reap_git_failed: {detail}"
    reopened, reopen_detail = _cas_reopen(
        reopen_row,
        lane_id=lane_id,
        expected_status="closed",
        restore_status=restore_status,
        notes=note,
        expected_tip_sha=tip_sha,
    )
    if not reopened:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_REOPEN_MISSED,
            f"{note}; reopen: {reopen_detail}",
            applied=False,
        )
    return _verdict(lane_id, branch, tip_sha, kind, note, applied=False)


def _recorded_cas_tip(row: object) -> str | None:
    """CAS fence value: the row's recorded ``branch_tip_sha`` (NULL unconstrained)."""
    return _text(_row_get(row, "branch_tip_sha"))


def _cas_close_failure_reason(
    close_detail: str,
    *,
    recorded_tip: str | None,
    probed_tip: str | None,
) -> str:
    """OBS-08: a CAS miss must name both the recorded fence and the probed git tip."""
    recorded = recorded_tip or "null"
    probed = probed_tip or "null"
    return f"close_row: {close_detail} recorded={recorded} probed={probed}"


def _cas_close(
    close_row: Callable[..., Any],
    *,
    lane_id: str,
    expected_status: str,
    notes: str,
    expected_tip_sha: str | None = None,
) -> tuple[bool, str]:
    kwargs: dict[str, Any] = {"expected_status": expected_status, "notes": notes}
    if expected_tip_sha is not None:
        kwargs["expected_tip_sha"] = expected_tip_sha
    try:
        result = close_row(lane_id, **kwargs)
    except TypeError:
        try:
            result = close_row(lane_id, expected_status, notes)
        except Exception as exc:  # noqa: BLE001 — CAS close is fail-closed
            return False, str(exc) or type(exc).__name__
    except Exception as exc:  # noqa: BLE001 — CAS close is fail-closed
        return False, str(exc) or type(exc).__name__
    return _interpret_cas_result(result)


def _cas_reopen(
    reopen_row: Callable[..., Any],
    *,
    lane_id: str,
    expected_status: str,
    restore_status: str,
    notes: str,
    expected_tip_sha: str | None = None,
) -> tuple[bool, str]:
    kwargs: dict[str, Any] = {
        "expected_status": expected_status,
        "restore_status": restore_status,
        "notes": notes,
    }
    if expected_tip_sha is not None:
        kwargs["expected_tip_sha"] = expected_tip_sha
    try:
        result = reopen_row(lane_id, **kwargs)
    except TypeError:
        try:
            result = reopen_row(lane_id, expected_status, restore_status, notes)
        except Exception as exc:  # noqa: BLE001 — CAS reopen is fail-closed
            return False, str(exc) or type(exc).__name__
    except Exception as exc:  # noqa: BLE001 — CAS reopen is fail-closed
        return False, str(exc) or type(exc).__name__
    return _interpret_cas_result(result)


def _interpret_cas_result(result: object) -> tuple[bool, str]:
    if not isinstance(result, Mapping):
        return False, "cas_unknown_result"
    ok = result.get("ok")
    if ok is False:
        err = result.get("error") or result.get("detail") or "cas_failed"
        return False, str(err)
    if ok is True:
        return True, ""
    return False, "cas_unknown_result"


def _sql_cas_close_lane(
    conn: Any,
    *,
    task_ref: str,
    lane_id: str,
    expected_status: str,
    notes: str | None,
    expected_tip_sha: str | None = None,
) -> bool:
    """Single UPDATE that closes only the row still at ``expected_status``.

    When ``expected_tip_sha`` is set, a recorded ``branch_tip_sha`` must match
    (NULL recorded tip is treated as unconstrained). Rowcount 1 is success.
    A notes-only write against an already-closed row keeps ``updated_at`` so
    the terminal generation in a retry marker stays stable (REAP07RV-001).
    A recorded close authority in the row's notes is carried into the written
    notes, so the close never drops the tip the owner authorized.
    """
    notes, writable = _close_notes_keeping_authority(
        conn, task_ref=task_ref, lane_id=lane_id, expected_status=expected_status, notes=notes
    )
    if not writable:
        return False
    sql = (
        "UPDATE worktree_lanes "
        "SET status = ?, notes = COALESCE(?, notes), "
        "updated_at = CASE WHEN status = 'closed' THEN updated_at ELSE datetime('now') END "
        "WHERE task_ref = ? AND lane_id = ? AND status = ?"
    )
    params: list[object] = ["closed", notes, task_ref, lane_id, expected_status]
    if expected_tip_sha:
        sql += " AND (branch_tip_sha IS NULL OR branch_tip_sha = ?)"
        params.append(expected_tip_sha)
    cur = conn.execute(sql, params)
    return int(cur.rowcount or 0) == 1


def _close_notes_keeping_authority(
    conn: Any,
    *,
    task_ref: str,
    lane_id: str,
    expected_status: str,
    notes: str | None,
) -> tuple[str | None, bool]:
    """Return ``(notes_to_write, writable)`` for a close of the row at ``expected_status``.

    A row whose notes carry a valid close authority keeps it byte-identical: the
    new note is appended to ``close_notes`` of the same JSON envelope that
    ``lane_retire_sweep`` writes. A malformed authority, or more than one row
    matching the CAS, is not writable, so the close refuses rather than drop it.
    Runs in the closer's transaction, so the read and the write are one decision.
    """
    if notes is None:
        return (None, True)
    from workbay_orchestrator_mcp.lane_reaping import _parse_close_authority  # noqa: PLC0415

    matched = conn.execute(
        "SELECT notes FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? AND status = ?",
        (task_ref, lane_id, expected_status),
    ).fetchall()
    if len(matched) > 1:
        return (None, False)
    if not matched:
        return (notes, True)
    prior = matched[0][0]
    authority, malformed = _parse_close_authority(prior)
    if malformed:
        return (None, False)
    if authority is None:
        return (notes, True)
    if not any(column[1] == "identity_seq" for column in conn.execute("PRAGMA table_info(worktree_lanes)").fetchall()):
        # Old schemas verify identity by exact note bytes, so the close note is not appended there.
        return (None, True)
    envelope = json.loads(prior)
    prior_notes = envelope.get("close_notes")
    if notes.startswith("post-merge reap of "):
        # Replace this reaper's state in one UPDATE: appending completion would
        # leave old retry intent live in the durable authority envelope.
        entries = prior_notes if isinstance(prior_notes, list) else ([] if prior_notes is None else [prior_notes])
        prior_notes = [
            entry
            for entry in entries
            if not (isinstance(entry, str) and entry.startswith(f"post-merge reap of {lane_id} at "))
        ]
    if isinstance(prior_notes, list):
        close_notes = [*prior_notes, notes]
    else:
        close_notes = [notes] if prior_notes is None else [prior_notes, notes]
    return (json.dumps({**envelope, "close_notes": close_notes}, sort_keys=True), True)


def _sql_cas_reopen_lane(
    conn: Any,
    *,
    task_ref: str,
    lane_id: str,
    expected_status: str,
    restore_status: str,
    notes: str | None,
    expected_tip_sha: str | None = None,
) -> bool:
    """Single UPDATE that restores status only while the row is still closed."""
    sql = (
        "UPDATE worktree_lanes "
        "SET status = ?, notes = COALESCE(?, notes), updated_at = datetime('now') "
        "WHERE task_ref = ? AND lane_id = ? AND status = ?"
    )
    params: list[object] = [restore_status, notes, task_ref, lane_id, expected_status]
    if expected_tip_sha:
        sql += " AND (branch_tip_sha IS NULL OR branch_tip_sha = ?)"
        params.append(expected_tip_sha)
    cur = conn.execute(sql, params)
    return int(cur.rowcount or 0) == 1


def _record(
    record_decision: Callable[..., Any],
    *,
    decision_id: str,
    rationale: str,
    task_ref: str,
) -> tuple[bool, str]:
    try:
        signature = inspect.signature(record_decision)
        if "task_ref" not in signature.parameters and not any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
        ):
            return False, "recorder cannot accept task_ref"
        call_shapes: list[tuple[tuple[object, ...], dict[str, object]]] = []
        task_ref_parameter = signature.parameters.get("task_ref")
        if task_ref_parameter is not None and task_ref_parameter.kind == inspect.Parameter.POSITIONAL_ONLY:
            call_shapes.append(((task_ref,), {"id": decision_id, "rationale": rationale}))
        call_shapes.extend(
            (
                ((), {"id": decision_id, "rationale": rationale, "task_ref": task_ref}),
                ((decision_id, rationale), {"task_ref": task_ref}),
                ((decision_id, rationale, task_ref), {}),
            )
        )
        for args, kwargs in call_shapes:
            try:
                bound = signature.bind(*args, **kwargs)
            except TypeError:
                continue
            extra_keywords = next(
                (
                    bound.arguments.get(name, {})
                    for name, parameter in signature.parameters.items()
                    if parameter.kind == inspect.Parameter.VAR_KEYWORD
                ),
                {},
            )
            # A successful bind can still put positional values into the wrong
            # fields, or hide a positional-only field's keyword in **kwargs.
            # Prove all three write fields reach their named destinations.
            expected_fields = (
                (("task_ref",), task_ref),
                (("id", "decision_id"), decision_id),
                (("rationale",), rationale),
            )
            if not all(
                next(
                    (bound.arguments.get(name) for name in names if name in signature.parameters),
                    extra_keywords.get(names[0]),
                )
                == value
                for names, value in expected_fields
            ):
                continue
            break
        else:
            return False, "recorder cannot accept task_ref"
        # Binding errors select compatibility; callback errors never trigger retries.
        result = record_decision(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — decision write is fail-closed
        return False, str(exc) or type(exc).__name__
    return _interpret_cas_result(result)


def _default_run_git(repo: Path) -> Callable[[Sequence[str]], subprocess.CompletedProcess[str]]:
    run_git_bytes = _default_run_git_bytes(repo)

    def run_git(args: Sequence[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        # Only an explicit ``text=False`` opts in; every other caller keeps text mode.
        if kwargs.get("text") is False:
            return run_git_bytes(args)  # type: ignore[return-value]
        return subprocess.run(
            ["git", "-C", str(repo), *list(args)],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_TIMEOUT_S,
        )

    return run_git


def _default_run_git_bytes(repo: Path) -> Callable[[Sequence[str | bytes]], subprocess.CompletedProcess[bytes]]:
    """Byte-exact runner: no universal-newline or locale decoding of path output."""

    def run_git_bytes(args: Sequence[str | bytes], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["git", "-C", str(repo), *list(args)],
            capture_output=True,
            text=False,
            check=False,
            timeout=_GIT_TIMEOUT_S,
        )

    return run_git_bytes


def _default_list_rows() -> Callable[..., Any]:
    def list_rows(task_ref: str, **kwargs: object) -> Any:
        from workbay_handoff_mcp.lanes_recording import list_lanes  # noqa: PLC0415

        status = str(kwargs.get("status") or "all")
        try:
            limit = int(kwargs.get("limit") or _LIST_PAGE_LIMIT)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            limit = _LIST_PAGE_LIMIT
        call_kw: dict[str, Any] = {
            "task_ref": task_ref,
            "status": status,
            "limit": max(1, limit),
        }
        if "after_id" in kwargs:
            call_kw["after_id"] = kwargs["after_id"]
        elif "offset" in kwargs:
            try:
                call_kw["offset"] = max(0, int(kwargs.get("offset") or 0))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                call_kw["offset"] = 0
        else:
            # Keyset seed: id DESC, not OFFSET updated_at DESC.
            call_kw["after_id"] = None
        return list_lanes(**call_kw)

    return list_rows


def _default_list_by_path() -> Callable[..., Any]:
    """Production default: cross-task full-scan of live path owners.

    ``list_nonterminal_lanes_with_worktree_path`` returns every non-terminal
    row that still names a worktree. Callers compare ``Path.resolve``
    identities in Python so trailing-slash and symlink spellings still
    match. Imported at call time from the lanes recording module
    (``lanes.py`` is not edited here). Import or query failure is
    classified as ``shared_path_unverified`` by the caller.
    """

    def list_by_path(*_args: object, **_kwargs: object) -> Any:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            list_nonterminal_lanes_with_worktree_path,
        )

        return list_nonterminal_lanes_with_worktree_path()

    return list_by_path


def _default_close_row(task_ref: str) -> Callable[..., Any]:
    def close_row(
        lane_id: str,
        expected_status: str | None = None,
        notes: str | None = None,
        expected_tip_sha: str | None = None,
        **_kwargs: object,
    ) -> Any:
        if not expected_status:
            return {"ok": False, "error": "cas_failed"}
        try:
            from workbay_handoff_mcp.shared_primitives import _resolve_task_ref  # noqa: PLC0415
            from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

            with _get_db_connection(begin_immediate=True) as conn:
                resolved = _resolve_task_ref(conn, task_ref)
                ok = _sql_cas_close_lane(
                    conn,
                    task_ref=resolved,
                    lane_id=lane_id,
                    expected_status=expected_status,
                    notes=notes,
                    expected_tip_sha=expected_tip_sha,
                )
        except Exception as exc:  # noqa: BLE001 — CAS close is fail-closed
            return {"ok": False, "error": str(exc) or type(exc).__name__}
        if not ok:
            return {"ok": False, "error": "cas_failed"}
        return {"ok": True}

    return close_row


def _default_reopen_row(task_ref: str) -> Callable[..., Any]:
    def reopen_row(
        lane_id: str,
        expected_status: str | None = None,
        notes: str | None = None,
        restore_status: str | None = None,
        expected_tip_sha: str | None = None,
        **_kwargs: object,
    ) -> Any:
        if not expected_status or not restore_status:
            return {"ok": False, "error": "cas_failed"}
        try:
            from workbay_handoff_mcp.shared_primitives import _resolve_task_ref  # noqa: PLC0415
            from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

            with _get_db_connection(begin_immediate=True) as conn:
                resolved = _resolve_task_ref(conn, task_ref)
                ok = _sql_cas_reopen_lane(
                    conn,
                    task_ref=resolved,
                    lane_id=lane_id,
                    expected_status=expected_status,
                    restore_status=restore_status,
                    notes=notes,
                    expected_tip_sha=expected_tip_sha,
                )
        except Exception as exc:  # noqa: BLE001 — CAS reopen is fail-closed
            import sqlite3

            if isinstance(exc, sqlite3.IntegrityError) and "released_no_work_row_is_terminal" in str(exc):
                return {
                    "ok": False,
                    "error": "Lane status changed or the released row is terminal.",
                    "error_kind": "lane_status_conflict",
                }
            return {"ok": False, "error": str(exc) or type(exc).__name__}
        if not ok:
            return {"ok": False, "error": "cas_failed"}
        return {"ok": True}

    return reopen_row


def _default_record_decision(task_ref: str) -> Callable[..., Any]:
    def record(*, id: str, rationale: str, **_kwargs: object) -> Any:
        from workbay_handoff_mcp import record_decision  # noqa: PLC0415

        return record_decision(
            session=id,
            decision="postmerge_reap",
            rationale=rationale,
            task_ref=task_ref,
            event_id=id,
        )

    return record


def _git(git: Callable[..., Any], args: Sequence[str]) -> Any:
    try:
        proc = git(list(args))
    except TypeError:
        try:
            proc = git(*args)
        except Exception as exc:  # noqa: BLE001 — probe errors are verdicts
            raise _GitError(str(exc) or type(exc).__name__) from exc
    except subprocess.TimeoutExpired as exc:
        raise _GitError("git timeout") from exc
    except Exception as exc:  # noqa: BLE001 — probe errors are verdicts
        raise _GitError(str(exc) or type(exc).__name__) from exc
    if proc is None:
        raise _GitError("git unavailable")
    return proc


def _call_list_rows(list_rows: Callable[..., Any], task_ref: str, **kwargs: object) -> Any:
    cleaned = {key: value for key, value in kwargs.items() if value is not _OMIT}
    try:
        return list_rows(task_ref, **cleaned)
    except TypeError:
        try:
            return list_rows(task_ref)
        except TypeError:
            return list_rows()


def _listing_error_detail(result: Mapping[Any, Any]) -> str:
    err = result.get("error")
    if err is None:
        data = result.get("data")
        if isinstance(data, Mapping):
            err = data.get("error")
    return f"list_lanes: {err}" if err else "list_lanes failed"


def _lanes_from_listing(src: object, result: Mapping[Any, Any]) -> list[Any]:
    """Type-check ``lanes`` the same way shared-path lookup does."""
    lanes: object = None
    found = False
    if isinstance(src, Mapping) and "lanes" in src:
        lanes = src.get("lanes")
        found = True
    elif "lanes" in result:
        lanes = result.get("lanes")
        found = True
    if not found or not isinstance(lanes, list):
        raise _ListingFailed("list_lanes malformed")
    return lanes


def _coerce_listing(result: object) -> tuple[list[Any], bool, object]:
    """Return ``(rows, has_more, next_after_id)`` from a listing envelope or list.

    ``ok is False`` fails the batch. A missing or non-list ``lanes`` key is
    malformed, not exhaustion. ``result is None`` is an empty page; the
    pager treats that as truncated after a prior ``has_more``.
    """
    if result is None:
        return [], False, None
    if isinstance(result, Mapping):
        if result.get("ok") is False:
            raise _ListingFailed(_listing_error_detail(result))
        data = result.get("data", result)
        if isinstance(data, Mapping) and data.get("ok") is False:
            raise _ListingFailed(_listing_error_detail(data))
        src = data if isinstance(data, Mapping) else result
        lanes = _lanes_from_listing(src, result)
        has_more = False
        next_after_id: object = None
        for mapping in (src, result):
            if not isinstance(mapping, Mapping):
                continue
            if mapping.get("has_more"):
                has_more = True
            if "next_after_id" in mapping and mapping.get("next_after_id") is not None:
                next_after_id = mapping.get("next_after_id")
        return lanes, has_more, next_after_id
    if isinstance(result, (list, tuple)):
        return list(result), False, None
    raise _ListingFailed("list_lanes malformed")


def _coerce_rows(result: object) -> list[Any]:
    return _coerce_listing(result)[0]


def _page_postmerge_rows(
    list_rows: Callable[..., Any],
    task_ref: str,
    *,
    repo: Path,
    git: Callable[..., Any],
    work_limit: int,
) -> tuple[list[Any], bool]:
    """Page the listing until live/retry work is exhausted or the listing ends.

    A single OFFSET page of recently-closed rows must not hide an older live
    lane. ``truncated`` is True when more live/retry work may remain, including
    when the listing reports ``has_more`` after the batch is full. An empty
    page with ``has_more``, or ``None`` after a prior ``has_more``, is
    ``_ListingFailed``, not completion.
    """
    collected: list[Any] = []
    seen: set[str] = set()
    live_retry = 0
    after_id: object = None
    offset = 0
    use_keyset = True
    for _ in range(_LIST_PAGE_CAP):
        raw = _call_list_rows(
            list_rows,
            task_ref,
            status="all",
            limit=_LIST_PAGE_LIMIT,
            after_id=after_id if use_keyset else _OMIT,
            offset=offset if not use_keyset else _OMIT,
        )
        page, has_more, next_after_id = _coerce_listing(raw)
        if not page:
            if has_more or after_id is not None or offset > 0:
                raise _ListingFailed("list_lanes truncated")
            return collected, False
        new_on_page = 0
        for row in page:
            lane_id = _text(_row_get(row, "lane_id")) or ""
            if lane_id and lane_id in seen:
                continue
            if lane_id:
                seen.add(lane_id)
            collected.append(row)
            new_on_page += 1
            status = _text(_row_get(row, "status")) or ""
            if status not in TERMINAL_STATUSES or _terminal_needs_retry(row, repo, git):
                live_retry += 1
        if work_limit > 0 and live_retry >= work_limit:
            return collected, True if has_more or live_retry > work_limit else False
        if not has_more:
            return collected, False
        if next_after_id is not None:
            after_id = next_after_id
            use_keyset = True
            continue
        if new_on_page == 0:
            return collected, True
        use_keyset = False
        after_id = None
        offset += len(page)
    return collected, True


def _row_get(row: object, key: str) -> object:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _lane_sort_key(row: object) -> str:
    return _text(_row_get(row, "lane_id")) or ""


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _heads_ref(branch: str) -> str:
    name = branch.strip()
    if name.startswith("refs/heads/") or name.startswith("refs/"):
        return name
    return f"refs/heads/{name}"


def _branch_short_name(branch: str) -> str:
    name = branch.strip()
    if name.startswith("refs/heads/"):
        return name[len("refs/heads/") :]
    return name


def _resolve_worktree(worktree_path: str, repo: Path) -> Path:
    path = Path(worktree_path)
    if not path.is_absolute():
        path = repo / path
    return path


def _looks_like_missing_ref(detail: str) -> bool:
    lowered = detail.lower()
    return any(token in lowered for token in _MISSING_REF_TOKENS)


def _looks_like_dirty(detail: str) -> bool:
    lowered = detail.lower()
    return any(token in lowered for token in _DIRTY_TOKENS)


def _looks_like_absent(detail: str) -> bool:
    lowered = detail.lower()
    return any(token in lowered for token in _ABSENT_TOKENS)


def _looks_like_unmerged(detail: str) -> bool:
    lowered = detail.lower()
    return any(token in lowered for token in _UNMERGED_TOKENS)


def _proc_detail(proc: object) -> str:
    stderr = str(getattr(proc, "stderr", "") or "").strip()
    stdout = str(getattr(proc, "stdout", "") or "").strip()
    return stderr or stdout


def _close_notes(branch: str, tip_sha: str) -> str:
    return f"post-merge reap of {branch} at {tip_sha}"


_CLOSE_NOTES_MARKER = " at "


_REAP_COMPLETE_MARKER = "reap_complete:"
# Decision recorded, completion ack missed: retry intent stays durable and the pass fails loudly.
_COMPLETION_UNACKNOWLEDGED = "reap_completion_unacknowledged"
_RETRY_IDENTITY_MARKER = "reap_retry_identity:"
_RETRY_GEN_MARKER = "reap_gen:"
_RETRY_NONCE_MARKER = "reap_nonce:"
_RETRY_EFFECT_MARKER = "reap_effect:"
_REAP_GUARD_REFUSED_MARKER = "reap_guard_refused:"
_UNACKNOWLEDGED_FAILURE = "unacknowledged_failure"
# Terminal row with neither a retry identity nor the kept marker.
_RETRY_MARKER_ABSENT = "retry_marker_absent"


def _strip_reap_guard_refused(value: str | None) -> str:
    """Remove stale guard outcomes before deriving a new reason or note."""
    if not value:
        return ""
    parts = [part.strip() for part in str(value).split(";")]
    kept = [part for part in parts if _REAP_GUARD_REFUSED_MARKER not in part]
    return "; ".join(part for part in kept if part)


def _postmerge_notes(notes: str | None) -> str | None:
    """Read the current reap note while retaining the owner's authority envelope."""
    if not notes or not notes.lstrip().startswith("{"):
        return notes
    try:
        envelope = json.loads(notes)
    except (TypeError, ValueError):
        return notes
    if not isinstance(envelope, dict):
        return notes
    entries = envelope.get("close_notes")
    if isinstance(entries, str):
        entries = [entries]
    if isinstance(entries, list):
        for entry in reversed(entries):
            if isinstance(entry, str) and entry.startswith("post-merge reap of "):
                return entry
    return notes


def _tip_from_close_notes(notes: str | None) -> str | None:
    """Recover the reaped tip from notes written in the same CAS close.

    Used to retry a missing ``postmerge_reap`` decision after the branch is
    already gone. Notes that were not written by this reaper return None.
    Only the SHA token after `` at `` is returned so a resumable suffix
    (preserving identity, retry marker) cannot pollute the pin.
    """
    notes = _postmerge_notes(notes)
    prefix = "post-merge reap of "
    if not notes or not notes.startswith(prefix):
        return None
    idx = notes.find(_CLOSE_NOTES_MARKER)
    if idx < 0:
        return None
    rest = notes[idx + len(_CLOSE_NOTES_MARKER) :].strip()
    token = rest.split()[0].rstrip(";,") if rest else ""
    return token or None


def _notes_have_retry_identity(notes: str | None) -> bool:
    notes = _postmerge_notes(notes)
    return bool(notes) and _RETRY_IDENTITY_MARKER in notes


def _row_generation(row: object) -> str:
    """Stable-enough generation for a retry marker (id, else close timestamp)."""
    row_id = _row_get(row, "id")
    updated = _text(_row_get(row, "updated_at")) or ""
    if row_id is not None and str(row_id).strip() != "":
        ident = str(row_id).strip()
        return f"id:{ident}:{updated}" if updated else f"id:{ident}"
    if updated:
        return updated
    created = _text(_row_get(row, "created_at"))
    return created or ""


def _generation_from_notes(notes: str | None) -> str | None:
    notes = _postmerge_notes(notes)
    if not notes or _RETRY_GEN_MARKER not in notes:
        return None
    rest = notes[notes.find(_RETRY_GEN_MARKER) + len(_RETRY_GEN_MARKER) :]
    token = rest.split(";")[0].strip().rstrip(",") if rest else ""
    return token or None


def _retry_generation_matches(row: object, notes: str | None) -> bool:
    expected = _generation_from_notes(notes)
    current = _row_generation(row)
    return bool(expected) and bool(current) and expected == current


def _nonce_from_notes(notes: str | None) -> str | None:
    notes = _postmerge_notes(notes)
    if not notes or _RETRY_NONCE_MARKER not in notes:
        return None
    rest = notes[notes.find(_RETRY_NONCE_MARKER) + len(_RETRY_NONCE_MARKER) :]
    token = rest.split()[0].split(";")[0].strip().rstrip(",") if rest else ""
    return token or None


def _preserving_identity_from_notes(notes: str | None) -> tuple[str | None, str | None]:
    """Recover preserving ref and SHA from resumable close notes."""
    notes = _postmerge_notes(notes)
    if not notes:
        return None, None
    marker = "preserved-by "
    idx = notes.find(marker)
    if idx < 0:
        return None, None
    rest = notes[idx + len(marker) :].strip()
    parts = rest.split()
    ref = parts[0].rstrip(";,") if parts else None
    sha = None
    if "sha" in parts:
        at = parts.index("sha")
        if at + 1 < len(parts):
            sha = parts[at + 1].rstrip(";,")
    return ref or None, sha or None


def _select_postmerge_batch(
    rows: Sequence[object],
    *,
    repo: Path,
    git: Callable[..., Any],
    limit: int,
) -> tuple[list[Any], bool]:
    """Prefer live rows. Idle terminals do not consume ``max_batch``."""
    live: list[Any] = []
    retry: list[Any] = []
    idle: list[Any] = []
    for row in sorted(rows, key=_lane_sort_key):
        status = _text(_row_get(row, "status")) or ""
        if status not in TERMINAL_STATUSES:
            live.append(row)
            continue
        if _terminal_needs_retry(row, repo, git):
            retry.append(row)
        else:
            idle.append(row)
    work = live + retry
    if work:
        return work[:limit], len(work) > limit
    return idle[:limit], len(idle) > limit


def _branch_names_consumed_tip(
    git: Callable[..., Any],
    branch: str,
    tip_sha: str,
) -> bool:
    """True when ``branch`` still points at the consumed tip.

    Probe errors fail closed: uncertainty is never leftover.
    """
    if not branch or not tip_sha:
        return False
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError:
        return False
    return current is not None and current == tip_sha


def _path_still_our_checkout(
    git: Callable[..., Any],
    worktree_path: str,
    branch: str,
    tip_sha: str,
) -> bool:
    """True when the path is this row's leftover checkout, not a successor.

    Leftover identity is branch name AND HEAD SHA equal to the consumed
    tip. A same-branch occupier at another tip is reuse, not ours.
    ``_GitError`` and a nonzero ``rev-parse`` fail closed (never leftover)
    unless git names an explicit absent-working-tree token, which is
    leftover unfinished git and still retries.
    """
    try:
        named = _git(git, ["-C", worktree_path, "rev-parse", "--abbrev-ref", "HEAD"])
        head = _git(git, ["-C", worktree_path, "rev-parse", "--verify", "HEAD"])
    except _GitError:
        return False
    if named.returncode != 0 or head.returncode != 0:
        detail = _proc_detail(named if named.returncode != 0 else head)
        return _looks_like_absent(detail)
    current_branch = (named.stdout or "").strip()
    head_sha = (head.stdout or "").strip()
    ours = _branch_short_name(branch) if branch else ""
    if ours and current_branch == ours and tip_sha and head_sha == tip_sha:
        return True
    return False


def _terminal_needs_retry(row: object, repo: Path, git: Callable[..., Any]) -> bool:
    """Retry when close notes parse and git is still unfinished.

    Unfinished means the branch still names the consumed tip, or the
    named path is still this row's leftover checkout. ``path.exists()``
    alone does not decide: a live successor may reuse the path. A durable
    ``reap_retry_identity:`` marker stays retryable even when both
    artifacts are already gone (OBS-08).
    """
    notes = _text(_row_get(row, "notes"))
    if _notes_have_retry_identity(notes):
        return True
    tip_sha = _tip_from_close_notes(notes)
    if tip_sha is None:
        return False
    branch = _text(_row_get(row, "branch")) or ""
    worktree_path = _text(_row_get(row, "worktree_path"))
    if _worktree_is_absent(worktree_path, repo) and _payload_completion_is_current(git, notes, branch):
        # The retained branch is settled only after durable decision acknowledgement,
        # and only while the exact branch still names the completed tip.
        return False
    if _branch_names_consumed_tip(git, branch, tip_sha):
        return True
    if _worktree_is_absent(worktree_path, repo) or not worktree_path:
        return False
    return _path_still_our_checkout(git, worktree_path, branch, tip_sha)


def _restore_consumed_branch(git: Callable[..., Any], branch: str, tip_sha: str) -> bool:
    """Recreate the consumed ref so a record miss stays retry-classified.

    ``postmerge_reap`` is recorded only after the worktree and branch are
    both gone. If that write misses, restoring the tip-pinned ref makes
    ``_terminal_needs_retry`` true until the decision exists. A ref that
    already exists is left untouched. Nonzero ``git branch`` rc or a
    restored ref that does not name ``tip_sha`` is failure.
    """
    if not branch or not tip_sha:
        return False
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError:
        return False
    if current is not None:
        return current == tip_sha
    name = _branch_short_name(branch)
    try:
        proc = _git(git, ["branch", name, tip_sha])
    except _GitError:
        return False
    rc = getattr(proc, "returncode", 1)
    if type(rc) is not int or rc != 0:
        return False
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError:
        return False
    return current is not None and current == tip_sha


def _retry_finish_branch(
    git: Callable[..., Any],
    branch: str,
    tip_sha: str,
    integration_branch: str,
) -> bool:
    """Delete the reaped branch if it still names ``tip_sha``.

    True means the leftover ref is gone. False means a real ``-d`` miss
    left the consumed tip in place. Probe errors raise ``_GitError`` so
    the caller can report an unknown instead of ``branch_remains``. A
    confirmation-probe timeout restores the consumed ref so the row
    stays retry-classified instead of going idle with no reap key.
    """
    if not branch:
        return True
    current = _probe_tip_sha(git, branch)
    if current is None:
        return True
    deleted, _detail = _delete_branch(git, branch, tip_sha, integration_branch)
    if not deleted:
        return False
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError:
        _restore_consumed_branch(git, branch, tip_sha)
        raise
    return current is None


def _retry_finish_and_record(
    *,
    git: Callable[..., Any],
    repo: Path,
    branch: str,
    tip_sha: str,
    integration_branch: str,
    record_decision: Callable[..., Any],
    decision_id: str,
    rationale: str,
    lane_id: str,
    task_ref: str,
    state_dir: Path | str | None = None,
) -> LaneVerdict:
    """Delete the leftover consumed ref and record. Never removes a worktree.

    Success is ``retry_reaped`` (applied stays False: the row was already
    closed). A probe timeout is ``probe_failed``, not ``branch_remains``.
    ``branch_remains`` is only a still-present ref after a real ``-d`` miss.
    """
    try:
        finished = _retry_finish_branch(git, branch, tip_sha, integration_branch)
    except _GitError as exc:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_PROBE_FAILED,
            exc.detail,
            applied=False,
        )
    if not finished:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_BRANCH_REMAINS,
            "branch_remains",
            applied=False,
        )
    recorded, rec_detail = _record(record_decision, decision_id=decision_id, rationale=rationale, task_ref=task_ref)
    if not recorded:
        _restore_consumed_branch(git, branch, tip_sha)
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_PROBE_FAILED,
            f"decision: {rec_detail}",
            applied=False,
        )
    vm_marker = _write_vm_merged_marker(branch, tip_sha, repo=repo, state_dir=state_dir)
    return _verdict(
        lane_id,
        branch,
        tip_sha,
        KIND_RETRY_REAPED,
        "retry_reaped",
        applied=False,
        vm_marker=vm_marker,
    )


def _retry_missing_reap_decision(
    row: object,
    *,
    lane_id: str,
    branch: str,
    task_ref: str,
    record_decision: Callable[..., Any],
    git: Callable[..., Any],
    worktree_path: str | None,
    repo: Path,
    integration_branch: str,
    batch: Sequence[object],
    list_by_path: Callable[..., Any] | None,
    state_dir: Path | str | None = None,
) -> LaneVerdict | None:
    """Finish git and write the reap key if a prior apply closed the row.

    Close notes from this reaper (``post-merge reap of ... at <sha>``) mean
    git may still be unfinished when reopen CAS missed. Retry is a no-op
    unless ``_terminal_needs_retry`` is true; that no-op is the only path
    that returns ``None`` so ``_reap_one`` can report already-terminal.
    A retry that removes leftover git and records the reap key returns
    ``retry_reaped``. Retry never removes a path without dirty and
    shared-path probes, and re-proves checkout identity immediately
    before ``git worktree remove``. A present path is not itself proof
    the original checkout remains. When identity says the path is not
    ours, retry still deletes the leftover consumed ref and never
    removes the occupier.
    """
    tip_sha = _tip_from_close_notes(_text(_row_get(row, "notes")))
    if not tip_sha:
        return None
    if not _terminal_needs_retry(row, repo, git):
        return None
    decision_id = f"postmerge_reap:{task_ref}:{lane_id}:{tip_sha}"
    rationale = _decision_rationale(lane_id, branch, tip_sha)

    if _worktree_is_absent(worktree_path, repo):
        # The path is gone. Probe the branch: a prior apply may have
        # removed the worktree and then failed ``git branch -d``. Record
        # ``postmerge_reap`` only once both the worktree and branch are
        # gone. A remaining ref is a typed refusal, never a recorded reap.
        return _retry_finish_and_record(
            git=git,
            repo=repo,
            branch=branch,
            tip_sha=tip_sha,
            integration_branch=integration_branch,
            record_decision=record_decision,
            decision_id=decision_id,
            rationale=rationale,
            lane_id=lane_id,
            task_ref=task_ref,
            state_dir=state_dir,
        )

    if worktree_path and not _path_still_our_checkout(git, worktree_path, branch, tip_sha):
        # Occupied by a different checkout. Finish the leftover branch
        # only; never ``git worktree remove`` the occupier.
        return _retry_finish_and_record(
            git=git,
            repo=repo,
            branch=branch,
            tip_sha=tip_sha,
            integration_branch=integration_branch,
            record_decision=record_decision,
            decision_id=decision_id,
            rationale=rationale,
            lane_id=lane_id,
            task_ref=task_ref,
            state_dir=state_dir,
        )

    _absent, refusal = _live_worktree_refusal(
        lane_id=lane_id,
        branch=branch,
        tip_sha=tip_sha,
        worktree_path=worktree_path,
        task_ref=task_ref,
        repo=repo,
        batch=batch,
        list_by_path=list_by_path,
        git=git,
    )
    if refusal is not None:
        return refusal
    removed, remove_detail, remove_kind = _remove_worktree(git, worktree_path)
    if remove_kind == KIND_MERGED_WORKTREE_DIRTY:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_MERGED_WORKTREE_DIRTY,
            remove_detail,
            applied=False,
        )
    if remove_kind == KIND_PROBE_FAILED:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_PROBE_FAILED,
            f"worktree: {remove_detail}",
            applied=False,
        )
    if remove_kind != KIND_MERGED_WORKTREE_ABSENT and not removed:
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_PROBE_FAILED,
            f"worktree: {remove_detail}",
            applied=False,
        )
    if not _worktree_is_absent(worktree_path, repo):
        return _verdict(
            lane_id,
            branch,
            tip_sha,
            KIND_PROBE_FAILED,
            "worktree still present",
            applied=False,
        )
    return _retry_finish_and_record(
        git=git,
        repo=repo,
        branch=branch,
        tip_sha=tip_sha,
        integration_branch=integration_branch,
        record_decision=record_decision,
        decision_id=decision_id,
        rationale=rationale,
        lane_id=lane_id,
        task_ref=task_ref,
        state_dir=state_dir,
    )


def _decision_rationale(lane_id: str, branch: str, tip_sha: str) -> str:
    return f"reaped merged lane {lane_id} branch {branch} at {tip_sha}"


def _postmerge_state_dir(repo: Path, state_dir: Path | str | None) -> Path:
    if state_dir is not None:
        return Path(state_dir).expanduser()
    configured = os.environ.get("WORKBAY_HANDOFF_STATE_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return repo / ".task-state"


def _write_vm_merged_marker(
    branch: str,
    receipt_sha: str,
    *,
    repo: Path,
    state_dir: Path | str | None = None,
) -> dict[str, object]:
    """Mark a closed VM lane and request the shared reaper without undoing closure."""
    errors: list[str] = []
    try:
        from workbay_orchestrator_mcp.orchestration.remote_sandbox_reap import (  # noqa: PLC0415
            write_merged_marker,
        )

        write_result = write_merged_marker(({"branch": branch, "receipt_sha": receipt_sha},), repo_root=repo)
        write_ok = write_result.get("ok") if isinstance(write_result, Mapping) else getattr(write_result, "ok", None)
        if write_ok is not True:
            write_error = (
                write_result.get("error") if isinstance(write_result, Mapping) else getattr(write_result, "error", None)
            )
            errors.append(str(write_error or "merged marker writer returned an unsuccessful result"))
    except Exception as exc:  # noqa: BLE001 — marker delivery must not undo lifecycle closure
        errors.append(f"{type(exc).__name__}: {exc}")
    try:
        from workbay_orchestrator_mcp.orchestration.lane_census import (  # noqa: PLC0415
            request_remote_sandbox_reap,
        )

        request_remote_sandbox_reap(_postmerge_state_dir(repo, state_dir))
    except Exception as exc:  # noqa: BLE001 — cadence remains available if the one-shot request misses
        errors.append(f"{type(exc).__name__}: {exc}")

    if errors:
        error = "; ".join(errors)
        _logger.warning("VM merged marker delivery failed for branch %s at %s: %s", branch, receipt_sha, error)
        return {"ok": False, "error": error}
    return {"ok": True}


def _verdict(
    lane_id: str,
    branch: str,
    tip_sha: str | None,
    kind: str,
    detail: str,
    *,
    applied: bool,
    unlanded_count: int | None = None,
    candidate: str | None = None,
    vm_marker: dict[str, object] | None = None,
) -> LaneVerdict:
    return LaneVerdict(
        lane_id=lane_id,
        branch=branch,
        tip_sha=tip_sha,
        kind=kind,
        detail=detail,
        applied=applied,
        unlanded_count=unlanded_count,
        candidate=candidate,
        vm_marker=vm_marker,
    )


def _count_kinds(verdicts: Sequence[LaneVerdict]) -> dict[str, int]:
    counts = {kind: 0 for kind in sorted(VERDICT_KINDS)}
    for verdict in verdicts:
        counts[verdict.kind] = counts.get(verdict.kind, 0) + 1
    return counts


def _normalize_max_batch(max_batch: object) -> int:
    try:
        value = int(max_batch)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return _DEFAULT_MAX_BATCH
    return max(0, value)


def _normalize_age_floor(age_floor_seconds: object) -> int:
    try:
        value = int(age_floor_seconds)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return _DEFAULT_AGE_FLOOR_SECONDS
    return max(0, value)


def reap_preserved_lanes_cross_session(
    *,
    task_ref: str,
    root: Path | str,
    integration_refs: Sequence[str] | None = None,
    apply: bool = False,
    max_batch: int = _DEFAULT_MAX_BATCH,
    age_floor_seconds: int = _DEFAULT_AGE_FLOOR_SECONDS,
    list_rows: Callable[..., Any] | None = None,
    close_row: Callable[..., Any] | None = None,
    record_decision: Callable[..., Any] | None = None,
    run_git: Callable[..., Any] | None = None,
    probe_owner: Callable[..., Any] | None = None,
    probe_lease: Callable[..., Any] | None = None,
    probe_clean: Callable[..., Any] | None = None,
    probe_merged: Callable[..., Any] | None = None,
    now: datetime | None = None,
    orchestrator_root: Path | str | None = None,
    retire_ownership: Callable[[str], tuple[bool, str]] | None = None,
) -> dict[str, Any]:
    """Reap preserved, clean, unowned lanes of an in-progress task (RES-07).

    Preservation, ownership, and cleanliness are independent probes. A lane is
    reclaimed only when all three agree: the tip is preserved on an integration
    ref, the worktree is porcelain-clean, and nothing live owns it. Unintegrated
    unowned lanes older than ``age_floor_seconds`` are counted as backlog, never
    deleted. Any probe failure is ``keep:probe_failed`` (CARD-07 / OBS-08).
    Apply-time ``guard_refusals`` fold into ``ok=False`` (REAP06RV-001).
    Report mode runs the same guard probes read-only and exposes their refusal
    count as ``would_refuse``; an unavailable probe is typed as ``unprobed``.
    """
    repo = Path(root)
    git = run_git if run_git is not None else _default_run_git(repo)
    rows_fn = list_rows if list_rows is not None else _default_list_rows()
    closer = close_row if close_row is not None else _default_close_row(task_ref)
    recorder = record_decision if record_decision is not None else _default_record_decision(task_ref)
    clock = now if now is not None else datetime.now(timezone.utc)
    limit = _normalize_max_batch(max_batch)
    age_floor = _normalize_age_floor(age_floor_seconds)
    refs = _resolve_integration_refs(
        task_ref,
        integration_refs=integration_refs,
        repo=repo,
        orchestrator_root=orchestrator_root,
    )
    # The first configured ref is the primary integration target for the
    # reference monitor. Other refs may support preservation classification,
    # but cannot independently authorize branch deletion.
    authorization_integration_ref = refs[0]
    owner_fn = probe_owner if probe_owner is not None else _default_probe_owner
    lease_fn = probe_lease if probe_lease is not None else _default_probe_lease
    clean_fn = probe_clean if probe_clean is not None else _default_probe_clean
    merged_fn = probe_merged if probe_merged is not None else _default_probe_merged
    retirer = (
        retire_ownership
        if retire_ownership is not None
        else _default_retire_ownership(task_ref, repo=repo, orchestrator_root=orchestrator_root)
    )

    empty = _empty_cross_session_report(apply=apply, max_batch=limit)
    try:
        rows, listing_truncated = _page_postmerge_rows(
            rows_fn,
            task_ref,
            repo=repo,
            git=git,
            work_limit=max(limit, 1),
        )
    except _ListingFailed as exc:
        empty["truncated"] = True
        empty["probe_failures"] = 1
        empty["rows_scanned"] = 0
        empty["kept"].append(
            _cross_session_item(
                lane_id="list_lanes",
                branch="",
                verdict="keep:probe_failed",
                preservation="unintegrated",
                ownership="probe_failed:list_lanes",
                cleanliness="probe_failed:list_lanes",
                reason=exc.detail,
            )
        )
        empty["verdicts"] = list(empty["kept"])
        return empty

    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        lane_status_is_reclaim_eligible,
    )

    would_reclaim: list[dict[str, Any]] = []
    reclaimed: list[dict[str, Any]] = []
    backlog: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    verdicts: list[dict[str, Any]] = []
    probe_failures = 0
    guard_refusals = 0
    would_refuse: int | str = 0
    guard_unprobed = listing_truncated and not apply
    preservation_moved = 0
    durable_failures = 0
    applied_reclaims = 0
    truncated = listing_truncated

    for row in rows:
        item = _classify_cross_session_row(
            row,
            repo=repo,
            git=git,
            refs=refs,
            owner_fn=owner_fn,
            lease_fn=lease_fn,
            clean_fn=clean_fn,
            merged_fn=merged_fn,
            clock=clock,
            age_floor=age_floor,
            eligible=lane_status_is_reclaim_eligible,
            task_ref=task_ref,
            orchestrator_root=orchestrator_root,
        )
        verdicts.append(item)
        if item["verdict"] == "keep:probe_failed":
            probe_failures += 1
        if item["verdict"] == "reclaim":
            would_reclaim.append(item)
            if not apply:
                guard_outcome, guard_detail = _probe_cross_session_reclaim_guard(
                    row=row,
                    item=item,
                    repo=repo,
                    task_ref=task_ref,
                )
                item["guard_outcome"] = guard_outcome or "would_reclaim"
                item["guard_detail"] = guard_detail
                if guard_outcome == "unprobed":
                    guard_unprobed = True
                    item["would_refuse"] = "unprobed"
                elif guard_outcome is not None and guard_outcome.startswith("refused_"):
                    if isinstance(would_refuse, int):
                        would_refuse += 1
                    item["would_refuse"] = True
                else:
                    item["would_refuse"] = False
            if apply:
                if limit and applied_reclaims >= limit:
                    truncated = True
                    item["reason"] = "reclaim deferred: max_batch"
                    continue
                row_notes = _text(_row_get(row, "notes")) or ""
                legacy_payload_close = (
                    _PAYLOAD_BRANCH_KEPT in row_notes
                    and not _notes_have_retry_identity(row_notes)
                    and not _payload_reclaim_complete(row_notes)
                )
                # Older interrupted closes have no fenced retry intent. Re-enter
                # the ordinary claimed close to persist intent before any effect.
                if _terminal_needs_retry(row, repo, git) and not legacy_payload_close:
                    applied_item = _resume_cross_session_terminal_retry(
                        item,
                        row=row,
                        repo=repo,
                        git=git,
                        close_row=closer,
                        record_decision=recorder,
                        task_ref=task_ref,
                        batch=rows,
                        merged_fn=merged_fn,
                        refs=refs,
                        integration_ref=authorization_integration_ref,
                        list_rows=rows_fn,
                        retire_ownership=retirer,
                        orchestrator_root=orchestrator_root,
                    )
                else:
                    applied_item = _apply_cross_session_reclaim(
                        item,
                        row=row,
                        repo=repo,
                        git=git,
                        close_row=closer,
                        record_decision=recorder,
                        task_ref=task_ref,
                        batch=rows,
                        merged_fn=merged_fn,
                        integration_ref=authorization_integration_ref,
                        list_rows=rows_fn,
                        retire_ownership=retirer,
                    )
                # Keep the classified item in would_reclaim; replace with apply result.
                would_reclaim[-1] = applied_item
                verdicts[-1] = applied_item
                if applied_item["verdict"] == "keep:preservation_moved":
                    preservation_moved += 1
                    durable_failures += 1
                    kept.append(applied_item)
                    continue
                if applied_item["verdict"] in _CROSS_SESSION_GUARD_REFUSALS:
                    guard_refusals += 1
                    kept.append(applied_item)
                    continue
                if applied_item["verdict"] == "keep:probe_failed":
                    probe_failures += 1
                    durable_failures += 1
                    kept.append(applied_item)
                    continue
                if applied_item.get("applied"):
                    reclaimed.append(applied_item)
                    applied_reclaims += 1
                else:
                    kept.append(applied_item)
            continue
        if item["verdict"] == "backlog":
            backlog.append(item)
            continue
        if item["verdict"] == "keep:preservation_moved":
            preservation_moved += 1
        kept.append(item)

    oldest_age = None
    oldest_lane = None
    for item in backlog:
        age = item.get("age_seconds")
        if not isinstance(age, int):
            continue
        if oldest_age is None or age > oldest_age:
            oldest_age = age
            oldest_lane = item.get("lane_id")

    empty_lane_ids = [
        str(item.get("lane_id") or "")
        for item in verdicts
        if item.get("verdict") == "keep:empty_lane" or item.get("preservation") == _EMPTY_LANE_KIND
    ]
    empty_lane_ids = [lane for lane in empty_lane_ids if lane]

    return {
        # Guard refusals are a typed non-success: fold them into ok rather
        # than adding a parallel report field (REAP06RV-001).
        "ok": probe_failures == 0 and durable_failures == 0 and guard_refusals == 0,
        "applied": apply,
        "rows_scanned": len(rows),
        "probe_failures": probe_failures,
        "guard_refusals": guard_refusals,
        "would_refuse": "unprobed" if guard_unprobed else would_refuse,
        "preservation_moved": preservation_moved,
        "would_reclaim": would_reclaim,
        "reclaimed": reclaimed,
        "backlog": backlog,
        "kept": kept,
        "verdicts": verdicts,
        "empty_lanes": len(empty_lane_ids),
        "empty_lane_ids": empty_lane_ids,
        "unintegrated_backlog": {
            "count": len(backlog),
            "oldest_age_seconds": oldest_age,
            "oldest_lane_id": oldest_lane,
        },
        "truncated": truncated,
        "integration_refs": list(refs),
    }


def _empty_cross_session_report(*, apply: bool, max_batch: int) -> dict[str, Any]:
    del max_batch
    return {
        "ok": False,
        "applied": apply,
        "rows_scanned": 0,
        "probe_failures": 0,
        "guard_refusals": 0,
        "would_refuse": 0,
        "preservation_moved": 0,
        "would_reclaim": [],
        "reclaimed": [],
        "backlog": [],
        "kept": [],
        "verdicts": [],
        "empty_lanes": 0,
        "empty_lane_ids": [],
        "unintegrated_backlog": {
            "count": 0,
            "oldest_age_seconds": None,
            "oldest_lane_id": None,
        },
        "truncated": False,
        "integration_refs": [],
    }


def _cross_session_item(
    *,
    lane_id: str,
    branch: str,
    verdict: str,
    preservation: str,
    ownership: str,
    cleanliness: str,
    reason: str,
    tip_sha: str | None = None,
    preserving_ref: str | None = None,
    preserving_sha: str | None = None,
    age_seconds: int | None = None,
    applied: bool = False,
) -> dict[str, Any]:
    return {
        "lane_id": lane_id,
        "branch": branch,
        "tip_sha": tip_sha,
        "verdict": verdict,
        "preservation": preservation,
        "ownership": ownership,
        "cleanliness": cleanliness,
        "reason": reason,
        "preserving_ref": preserving_ref,
        "preserving_sha": preserving_sha,
        "age_seconds": age_seconds,
        "applied": applied,
    }


class ManifestResolutionError(Exception):
    """An existing task manifest could not be read, parsed, or validated."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def _resolve_integration_refs(
    task_ref: str,
    *,
    integration_refs: Sequence[str] | None,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> list[str]:
    """Resolve integration refs. ``['main']`` is the missing-manifest default.

    An existing manifest that cannot be read, parsed, or validated raises
    ``ManifestResolutionError`` rather than failing open to ``main``. The
    optional ``integration_branch`` key may be absent; when present, it must
    contain a non-empty valid name or list of names, and every declared name
    must resolve to a branch commit. Manifest branches are returned as
    qualified refs to prevent a tag with the same name from changing
    subsequent preservation probes.
    """
    if integration_refs is not None:
        refs: list[str] = []
        for value in integration_refs:
            if not isinstance(value, str):
                raise ManifestResolutionError(
                    f"integration ref has invalid type {type(value).__name__} (expected branch name)"
                )
            ref = value.strip()
            if not ref:
                continue
            if not _is_valid_integration_branch_name(ref):
                raise ManifestResolutionError(f"integration ref is not a valid branch name: {ref!r}")
            if ref not in refs:
                refs.append(ref)
        return refs or ["main"]
    refs = ["main"]
    declared_refs = _read_manifest_integration_branches(
        task_ref,
        repo=repo,
        orchestrator_root=orchestrator_root,
    )
    for declared in declared_refs or []:
        if declared not in refs:
            refs.append(declared)
    return refs


def _manifest_path_is_present_regular(path: Path) -> bool:
    """True when ``path`` is a regular file or a symlink to one.

    Policy: follow a symlink whose target is a regular file. Only
    ``FileNotFoundError`` on the path itself means absent. A directory,
    dangling symlink, other non-regular entry, symlink to a non-regular
    target, or any other presence ``OSError`` raises
    ``ManifestResolutionError``.
    """
    import stat  # noqa: PLC0415 — classifier stays in the resolver region

    try:
        st = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ManifestResolutionError(f"task manifest {path} could not be resolved: {exc}") from exc
    if stat.S_ISLNK(st.st_mode):
        try:
            target_st = path.stat()
        except FileNotFoundError as exc:
            raise ManifestResolutionError(f"task manifest {path} is a dangling symlink") from exc
        except OSError as exc:
            raise ManifestResolutionError(f"task manifest {path} could not be resolved: {exc}") from exc
        if not stat.S_ISREG(target_st.st_mode):
            raise ManifestResolutionError(f"task manifest {path} is a symlink whose target is not a regular file")
        return True
    if stat.S_ISDIR(st.st_mode):
        raise ManifestResolutionError(f"task manifest {path} is a directory, not a regular file")
    if not stat.S_ISREG(st.st_mode):
        raise ManifestResolutionError(f"task manifest {path} is not a regular file")
    return True


def _is_valid_integration_branch_name(name: str) -> bool:
    """Validate a branch name using Git's ``check-ref-format --branch`` rules."""
    if (
        not name
        or name == "@"
        or name == "HEAD"
        or name.startswith(("-", "/"))
        or name.endswith("/")
        or name.endswith(".")
        or "//" in name
        or ".." in name
        or "@{" in name
    ):
        return False
    for ch in name:
        if ch.isspace() or ord(ch) < 32 or ch == "\x7f" or ch in {"~", "^", ":", "?", "*", "[", "\\"}:
            return False
    if any(component.startswith(".") or component.endswith(".lock") for component in name.split("/")):
        return False
    return True


def _declared_integration_branches(raw: Mapping[object, object], *, path: Path) -> list[str]:
    if "integration_branch" not in raw:
        # The writer omits this key; validation is present-only and provisioning defaults to main.
        return []
    value = raw["integration_branch"]
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list) and value and all(isinstance(ref, str) for ref in value):
        values = value
    else:
        raise ManifestResolutionError(
            f"task manifest {path} field integration_branch has invalid type "
            f"{type(value).__name__} (expected non-empty string or non-empty list of strings)"
        )
    refs: list[str] = []
    for ref in values:
        if not _is_valid_integration_branch_name(ref):
            raise ManifestResolutionError(
                f"task manifest {path} field integration_branch has invalid branch name: {ref!r}"
            )
        if ref not in refs:
            refs.append(ref)
    return refs


def _declared_integration_branch(raw: Mapping[object, object], *, path: Path) -> str | None:
    """Compatibility reader for callers that consume a single integration ref."""
    declared = _declared_integration_branches(raw, path=path)
    return declared[0] if declared else None


def _task_manifest_candidates(
    task_ref: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> list[Path]:
    """``orchestrator_root`` first, then ``repo``, ``config/lane-orchestration/<task>.json``."""
    bases: list[Path] = []
    if orchestrator_root is not None:
        bases.append(Path(orchestrator_root))
    bases.append(repo)
    seen: set[str] = set()
    out: list[Path] = []
    for base in bases:
        path = base / "config" / "lane-orchestration" / f"{task_ref}.json"
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _locate_task_manifest(
    task_ref: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> Path | None:
    for path in _task_manifest_candidates(task_ref, repo=repo, orchestrator_root=orchestrator_root):
        if not _manifest_path_is_present_regular(path):
            continue
        return path
    return None


def _default_retire_ownership(
    task_ref: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> Callable[[str], tuple[bool, str]]:
    def retire(lane_id: str) -> tuple[bool, str]:
        try:
            path = _locate_task_manifest(task_ref, repo=repo, orchestrator_root=orchestrator_root)
        except ManifestResolutionError as exc:
            return False, str(exc)
        if path is None:
            return True, "manifest_absent"
        try:
            from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
                retire_lane_ownership,
            )

            result = retire_lane_ownership(path, lane_id, reason="postmerge_reap", missing_ok=True)
            if (
                isinstance(result, dict)
                and result.get("retired") is False
                and result.get("reason") == "not_in_manifest"
            ):
                return True, "not_in_manifest"
            return True, "retired"
        except Exception as exc:  # noqa: BLE001 — typed failure for compensation
            return False, str(exc) or type(exc).__name__

    return retire


def _read_manifest_integration_branch(
    task_ref: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> str | None:
    """Read the primary integration branch for single-ref legacy consumers.

    A missing file or absent optional ``integration_branch`` key returns
    ``None`` (caller keeps ``['main']``). An existing declared value must be a
    valid integration branch; unreadable, malformed, invalid-ref, or
    non-regular paths raise ``ManifestResolutionError``.

    Policy: follow a symlink whose target is a regular JSON file.
    """
    declared = _read_manifest_integration_branches(task_ref, repo=repo, orchestrator_root=orchestrator_root)
    return declared[0] if declared else None


def _read_manifest_integration_branches(
    task_ref: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> list[str] | None:
    """Resolve declared integration branches; an absent optional key returns an empty list."""
    for path in _task_manifest_candidates(task_ref, repo=repo, orchestrator_root=orchestrator_root):
        if not _manifest_path_is_present_regular(path):
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeError) as exc:
            raise ManifestResolutionError(f"task manifest {path} could not be resolved: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise ManifestResolutionError(f"task manifest {path} is not a mapping: {type(raw).__name__}")
        declared_refs = _declared_integration_branches(raw, path=path)
        run_git = _default_run_git(repo)
        branch_refs: list[str] = []
        for ref in declared_refs:
            branch_ref = ref if ref.startswith("refs/heads/") else f"refs/heads/{ref}"
            try:
                result = run_git(["rev-parse", "--verify", "--quiet", f"{branch_ref}^{{commit}}"])
            except Exception as exc:  # noqa: BLE001 — manifest refs fail closed
                raise ManifestResolutionError(
                    f"task manifest {path} field integration_branch ref {ref!r} could not be resolved: {exc}"
                ) from exc
            if result.returncode != 0 or not result.stdout.strip():
                raise ManifestResolutionError(
                    f"task manifest {path} field integration_branch ref {ref!r} does not resolve to a branch commit"
                )
            if branch_ref not in branch_refs:
                branch_refs.append(branch_ref)
        return branch_refs
    return None


def _declared_lane_base_sha(
    spec: Mapping[object, object],
    *,
    path: Path,
    lane_id: str,
) -> str | None:
    """Return a pinned ``base_sha`` or raise when present evidence is malformed.

    A missing key is absence (legacy empty-lane arms still run). A present
    null, non-string, empty, or non-40-hex value is invalid proof and must
    not be coerced into absence.
    """
    if "base_sha" not in spec:
        return None
    value = spec["base_sha"]
    if not isinstance(value, str):
        raise ManifestResolutionError(
            f"task manifest {path} lane {lane_id} field base_sha has invalid type "
            f"{type(value).__name__} (expected 40-hex commit sha)"
        )
    coerced = _coerce_commit_sha(value)
    if coerced is None:
        raise ManifestResolutionError(
            f"task manifest {path} lane {lane_id} field base_sha has invalid type "
            f"{type(value).__name__} (not a 40-hex commit sha: {value!r})"
        )
    return coerced


def _read_manifest_lane_base_sha(
    task_ref: str,
    lane_id: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None = None,
) -> str | None:
    """Read a pinned per-lane ``base_sha`` from the task manifest, if present.

    Production ``list_lanes`` rows are SELECT-* DB dictionaries and have no
    ``base_sha`` column. The immutable cut is already on the task manifest.
    A missing file, missing ``lanes`` key, missing lane, or missing ``base_sha``
    key is absence (other empty-lane arms still run). A present malformed
    ``lanes`` container, present malformed lane spec, present malformed
    ``base_sha`` value, or an existing path that cannot be read, parsed, or
    validated as a mapping, raises ``ManifestResolutionError`` rather than
    failing open to a later candidate or weaker empty heuristics. Callers fail
    closed at rev-list when a present SHA is not a git object.
    """
    for path in _task_manifest_candidates(task_ref, repo=repo, orchestrator_root=orchestrator_root):
        if not _manifest_path_is_present_regular(path):
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeError) as exc:
            raise ManifestResolutionError(f"task manifest {path} could not be resolved: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise ManifestResolutionError(f"task manifest {path} is not a mapping: {type(raw).__name__}")
        if "lanes" not in raw:
            continue
        lanes = raw["lanes"]
        if not isinstance(lanes, Mapping):
            raise ManifestResolutionError(
                f"task manifest {path} field lanes has invalid type {type(lanes).__name__} (expected mapping)"
            )
        if lane_id not in lanes:
            continue
        spec = lanes[lane_id]
        if not isinstance(spec, Mapping):
            raise ManifestResolutionError(
                f"task manifest {path} lane {lane_id} has invalid type {type(spec).__name__} (expected mapping)"
            )
        return _declared_lane_base_sha(spec, path=path, lane_id=lane_id)
    return None


def _declared_row_base_sha(
    row: object | None,
    *,
    lane_id: str | None = None,
) -> str | None:
    """Return a pinned row ``base_sha`` or raise when present evidence is malformed.

    A missing mapping key, or an object/dataclass producer whose attribute is
    absent or ``None``, is absence (legacy empty-lane arms still run). A
    present null, non-string, empty, or non-40-hex mapping value is invalid
    proof and must not be coerced into absence.
    """
    if row is None:
        return None
    label = _text(lane_id) or _text(_row_get(row, "lane_id")) or "unknown"
    if isinstance(row, Mapping):
        if "base_sha" not in row:
            return None
        value = row["base_sha"]
    else:
        value = getattr(row, "base_sha", None)
        if value is None:
            return None
    if not isinstance(value, str):
        raise ManifestResolutionError(
            f"lane {label} row field base_sha has invalid type {type(value).__name__} (expected 40-hex commit sha)"
        )
    coerced = _coerce_commit_sha(value)
    if coerced is None:
        raise ManifestResolutionError(
            f"lane {label} row field base_sha has invalid type "
            f"{type(value).__name__} (not a 40-hex commit sha: {value!r})"
        )
    return coerced


def _authoritative_lane_base_sha(
    *,
    row: object | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    repo: Path | None = None,
    orchestrator_root: Path | str | None = None,
) -> str | None:
    """Resolve the immutable per-lane base from the task manifest, then the row.

    Present manifest evidence is authoritative: a valid pin beats a conflicting
    or malformed lower-priority row, and a present malformed manifest is not
    bypassed by a valid row. Only true absence of manifest evidence may use
    fallback row proof.
    """
    task = _text(task_ref) or (_text(_row_get(row, "task_ref")) if row is not None else None)
    lane = _text(lane_id) or (_text(_row_get(row, "lane_id")) if row is not None else None)
    if task and lane and repo is not None:
        from_manifest = _read_manifest_lane_base_sha(
            task,
            lane,
            repo=repo,
            orchestrator_root=orchestrator_root,
        )
        if from_manifest is not None:
            return from_manifest
    return _declared_row_base_sha(row, lane_id=lane)


def _try_authoritative_lane_base_sha(
    *,
    row: object | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    repo: Path | None = None,
    orchestrator_root: Path | str | None = None,
) -> tuple[str | None, str | None]:
    """Resolve the pinned base. ``(None, detail)`` is present malformed proof.

    ``(sha, None)`` is a valid pin. ``(None, None)`` is true absence and may
    use legacy empty-lane arms. ``(None, detail)`` must fail closed.
    """
    try:
        return (
            _authoritative_lane_base_sha(
                row=row,
                task_ref=task_ref,
                lane_id=lane_id,
                repo=repo,
                orchestrator_root=orchestrator_root,
            ),
            None,
        )
    except ManifestResolutionError as exc:
        return None, exc.detail


def _classify_cross_session_row(
    row: object,
    *,
    repo: Path,
    git: Callable[..., Any],
    refs: Sequence[str],
    owner_fn: Callable[..., Any],
    lease_fn: Callable[..., Any],
    clean_fn: Callable[..., Any],
    merged_fn: Callable[..., Any],
    clock: datetime,
    age_floor: int,
    eligible: Callable[[object], bool],
    task_ref: str | None = None,
    orchestrator_root: Path | str | None = None,
) -> dict[str, Any]:
    lane_id = _text(_row_get(row, "lane_id")) or ""
    branch = _text(_row_get(row, "branch")) or ""
    status = _text(_row_get(row, "status")) or ""
    worktree_path = _text(_row_get(row, "worktree_path"))
    lane_kind = (_text(_row_get(row, "lane_kind")) or "").lower()
    age_seconds = _row_age_seconds(row, clock)

    if not eligible(status):
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:ineligible_status",
            preservation="unintegrated",
            ownership="unowned",
            cleanliness="clean",
            reason=f"status {status or 'unset'} is not reclaim-eligible",
            age_seconds=age_seconds,
        )
    if not branch:
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:no_branch",
            preservation="unintegrated",
            ownership="unowned",
            cleanliness="clean",
            reason="no branch recorded",
            age_seconds=age_seconds,
        )

    base_sha, base_error = _try_authoritative_lane_base_sha(
        row=row,
        task_ref=task_ref,
        repo=repo,
        orchestrator_root=orchestrator_root,
    )
    if base_error:
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:probe_failed",
            preservation="unintegrated",
            ownership="unowned",
            cleanliness="clean",
            reason=f"preservation:probe_failed:{base_error}",
            age_seconds=age_seconds,
        )

    if _terminal_needs_retry(row, repo, git):
        preservation, preserving_ref, preserving_sha, tip_sha, pres_failed, empty_reason = _probe_preservation(
            repo=repo,
            git=git,
            branch=branch,
            worktree_path=worktree_path,
            refs=refs,
            lane_kind=lane_kind,
            merged_fn=merged_fn,
            base_sha=base_sha,
            created_at=_row_get(row, "created_at"),
        )
        preservation_label = _preservation_label(preservation)
        if pres_failed:
            return _cross_session_item(
                lane_id=lane_id,
                branch=branch,
                verdict="keep:probe_failed",
                preservation=preservation_label,
                ownership="unowned",
                cleanliness="clean",
                reason=f"preservation:{preservation}",
                tip_sha=tip_sha,
                preserving_ref=preserving_ref,
                preserving_sha=preserving_sha,
                age_seconds=age_seconds,
            )
        if preservation == _EMPTY_LANE_KIND:
            prior = _preservation_kind_from_notes(_text(_row_get(row, "notes")))
            if prior in _PRESERVED_KINDS:
                # A prior preserved close must not be relabeled empty_lane on
                # retry: that skip would drop identity fencing (REAP08RV-001).
                preservation = prior
                preservation_label = prior
            elif prior is None and status in TERMINAL_STATUSES:
                return _cross_session_item(
                    lane_id=lane_id,
                    branch=branch,
                    verdict="keep:already_terminal",
                    preservation="unintegrated",
                    ownership="unowned",
                    cleanliness="clean",
                    reason="terminal retry has no recorded preservation kind; keeping terminal label",
                    tip_sha=tip_sha,
                    preserving_ref=preserving_ref,
                    preserving_sha=preserving_sha,
                    age_seconds=age_seconds,
                )
            else:
                return _cross_session_item(
                    lane_id=lane_id,
                    branch=branch,
                    verdict="keep:empty_lane",
                    preservation=_EMPTY_LANE_KIND,
                    ownership="unowned",
                    cleanliness="clean",
                    reason=empty_reason or _EMPTY_LANE_FIRST_PARENT,
                    tip_sha=tip_sha,
                    preserving_ref=preserving_ref,
                    preserving_sha=preserving_sha,
                    age_seconds=age_seconds,
                )
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="reclaim",
            preservation=preservation_label,
            ownership="unowned",
            cleanliness="clean",
            reason=f"terminal retry of pinned leftover effects via {preservation_label}",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )

    row_notes = _text(_row_get(row, "notes")) or ""
    if (
        status in TERMINAL_STATUSES
        and _worktree_is_absent(worktree_path, repo)
        and _payload_completion_is_current(git, row_notes, branch)
    ):
        # Reclaimed earlier with the branch kept by design: nothing is owed, so do not reclaim it again.
        # A branch that moved since the completion is probed below like any other row.
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:already_terminal",
            preservation=_preservation_kind_from_notes(row_notes) or "unintegrated",
            ownership="unowned",
            cleanliness="clean",
            reason=f"terminal row; {_PAYLOAD_BRANCH_KEPT}; worktree reclaimed",
            age_seconds=age_seconds,
        )

    preservation, preserving_ref, preserving_sha, tip_sha, pres_failed, empty_reason = _probe_preservation(
        repo=repo,
        git=git,
        branch=branch,
        worktree_path=worktree_path,
        refs=refs,
        lane_kind=lane_kind,
        merged_fn=merged_fn,
        base_sha=base_sha,
        created_at=_row_get(row, "created_at"),
    )
    ownership, owner_failed = _probe_ownership(
        worktree_path=worktree_path,
        repo=repo,
        lane_id=lane_id,
        owner_fn=owner_fn,
        lease_fn=lease_fn,
    )
    cleanliness, clean_failed = _probe_cleanliness(
        worktree_path=worktree_path,
        repo=repo,
        clean_fn=clean_fn,
    )

    failed = pres_failed or owner_failed or clean_failed
    if failed:
        reasons = []
        if pres_failed:
            reasons.append(f"preservation:{preservation}")
        if owner_failed:
            reasons.append(f"ownership:{ownership}")
        if clean_failed:
            reasons.append(f"cleanliness:{cleanliness}")
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:probe_failed",
            preservation=_preservation_label(preservation),
            ownership=ownership,
            cleanliness=cleanliness,
            reason="; ".join(reasons) or "probe_failed",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if ownership.startswith("owned:"):
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:owned",
            preservation=preservation,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=f"live owner {ownership}",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if cleanliness.startswith("dirty"):
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:dirty",
            preservation=preservation,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=f"dirty worktree {cleanliness}",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if preservation == _EMPTY_LANE_KIND:
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:empty_lane",
            preservation=_EMPTY_LANE_KIND,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=empty_reason or _EMPTY_LANE_FIRST_PARENT,
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if preservation in _PRESERVED_KINDS and ownership == "unowned" and cleanliness == "clean":
        ref_label = preserving_ref or "integration"
        sha_label = preserving_sha or tip_sha or "unknown"
        payload_note = f"; {_PAYLOAD_BRANCH_KEPT}" if empty_reason == _REVIEW_PAYLOAD_ONLY_REASON else ""
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="reclaim",
            preservation=preservation,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=f"preserved by {ref_label} at {sha_label} via {preservation}{payload_note}",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if status in TERMINAL_STATUSES:
        detail_suffix = f"; {empty_reason}" if empty_reason else ""
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:already_terminal",
            preservation=preservation,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=f"terminal row preservation={preservation}; no leftover retry identity{detail_suffix}",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if preservation == "unintegrated" and ownership == "unowned":
        if age_seconds is None:
            return _cross_session_item(
                lane_id=lane_id,
                branch=branch,
                verdict="keep:age_unknown",
                preservation=preservation,
                ownership=ownership,
                cleanliness=cleanliness,
                reason="unintegrated but age could not be parsed",
                tip_sha=tip_sha,
                age_seconds=age_seconds,
            )
        if age_seconds >= age_floor:
            return _cross_session_item(
                lane_id=lane_id,
                branch=branch,
                verdict="backlog",
                preservation=preservation,
                ownership=ownership,
                cleanliness=cleanliness,
                reason=f"unintegrated for {age_seconds}s (floor {age_floor}s)",
                tip_sha=tip_sha,
                age_seconds=age_seconds,
            )
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:too_young",
            preservation=preservation,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=f"unintegrated age {age_seconds}s is below floor {age_floor}s",
            tip_sha=tip_sha,
            age_seconds=age_seconds,
        )
    return _cross_session_item(
        lane_id=lane_id,
        branch=branch,
        verdict=f"keep:{preservation}",
        preservation=preservation,
        ownership=ownership,
        cleanliness=cleanliness,
        reason=f"not reclaimable: preservation={preservation} ownership={ownership} cleanliness={cleanliness}",
        tip_sha=tip_sha,
        preserving_ref=preserving_ref,
        preserving_sha=preserving_sha,
        age_seconds=age_seconds,
    )


def _coerce_commit_sha(value: object) -> str | None:
    text = _text(value)
    if text is None:
        return None
    raw = text.lower()
    if len(raw) == 40 and all(char in "0123456789abcdef" for char in raw):
        return raw
    return None


def _preservation_label(kind: str) -> str:
    if kind in _PRESERVED_KINDS or kind in {"unintegrated", _EMPTY_LANE_KIND}:
        return kind
    return "unintegrated"


def _rev_list_range(git: Callable[..., Any], start: str, end: str, *, option: str = "--count") -> Any:
    """Shared revision-range query for counts and merge-content verification."""
    return _git(git, ["rev-list", option, f"{start}..{end}"])


def _rev_list_count(git: Callable[..., Any], start: str, end: str) -> int:
    proc = _rev_list_range(git, start, end)
    if proc.returncode != 0:
        raise _GitError(_proc_detail(proc) or "rev-list --count failed")
    raw = (proc.stdout or "").strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise _GitError(f"rev-list --count unparseable: {raw!r}") from exc


def _tip_on_first_parent_lineage(git: Callable[..., Any], tip_sha: str, ref: str) -> bool:
    ref_sha = _ref_sha(git, ref)
    if not ref_sha:
        raise _GitError(f"ref unreadable: {ref}")
    if tip_sha.lower() == ref_sha.lower():
        return True
    exclusive = _rev_list_count(git, tip_sha, ref_sha)
    if exclusive < 0:
        raise _GitError("rev-list --count negative")
    proc = _git(git, ["rev-list", "--first-parent", f"-n{exclusive + 1}", ref_sha])
    if proc.returncode != 0:
        raise _GitError(_proc_detail(proc) or "rev-list --first-parent failed")
    lineage = {line.strip().lower() for line in (proc.stdout or "").splitlines() if line.strip()}
    return tip_sha.lower() in lineage


def _commit_committer_epoch(git: Callable[..., Any], sha: str) -> int | None:
    proc = _git(git, ["log", "-1", "--format=%ct", sha])
    if proc.returncode != 0:
        return None
    raw = (proc.stdout or "").strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _worktree_reflog_proves_no_commit_after_base(
    worktree_path: str | None,
    branch: str,
    base_sha: str,
    *,
    created_at: object | None,
) -> bool:
    """Prove an attached lane worktree only fast-forwarded past its pinned base.

    A positive ``base..tip`` range on integration history is ambiguous unless
    the lane worktree's own HEAD reflog shows it was registered at the pinned
    base and contains no commit action. Fast-forward entries do not count as
    commits; a missing, stale, or malformed reflog stays inconclusive.
    """
    if not worktree_path or not branch:
        return False
    marker = Path(worktree_path) / ".git"
    expected_ref = branch.strip()
    if not expected_ref.startswith("refs/heads/"):
        expected_ref = f"refs/heads/{expected_ref}"
    try:
        marker_lines = marker.read_text(encoding="utf-8").splitlines()
        if not marker_lines or not marker_lines[0].strip().lower().startswith("gitdir:"):
            return False
        pointer = marker_lines[0].split(":", 1)[1].strip()
        if not pointer:
            return False
        admin = Path(pointer)
        if not admin.is_absolute():
            admin = marker.parent / admin
        admin = admin.resolve()
        if (admin / "HEAD").read_text(encoding="utf-8").strip() != f"ref: {expected_ref}":
            return False
        registered_epoch = int((admin / "gitdir").stat().st_mtime)
        log_lines = (admin / "logs" / "HEAD").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError, ValueError):
        return False

    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        LANE_TIMESTAMP_SKEW_TOLERANCE_S,
        _lane_created_at_epoch,
    )

    if created_at is not None:
        created_epoch = _lane_created_at_epoch(created_at)
        if created_epoch is None or abs(registered_epoch - created_epoch) > LANE_TIMESTAMP_SKEW_TOLERANCE_S:
            return False
    if not log_lines:
        return False
    first = log_lines[0].split(maxsplit=2)
    if len(first) < 2 or first[0] != "0" * 40 or first[1].lower() != base_sha.lower():
        return False
    for line in log_lines[1:]:
        action = line.partition("\t")[2].strip().lower()
        if action.startswith(
            ("commit", "cherry-pick", "rebase (pick)", "rebase (edit)", "rebase (reword)", "rebase (squash)")
        ):
            return False
    return True


def _own_commit_empty_lane(
    git: Callable[..., Any],
    tip_sha: str,
    refs: Sequence[str],
    *,
    base_sha: str | None,
    created_at: object | None,
    worktree_path: str | None = None,
    branch: str | None = None,
) -> tuple[str, str | None, str | None, bool, str] | None:
    """Own-commit gate. Return empty/probe_failed, or None to continue.

    Arms in order: manifest ``base_sha`` count, first-parent lineage per ref,
    then ancestor-but-off-first-parent committer-epoch vs ``created_at``.
    A positive ``base..tip`` count is own-work proof only when the tip is
    off every integration ref's first-parent lineage. On that lineage the
    count may include inherited commits, so the result is unknown. Unknown
    never becomes preserved. A worktree registered at or
    before the tip disproves the epoch arm: DB ``created_at`` after live git
    work is not proof the lane never started, and must not skip retry identity
    fencing.
    """
    if base_sha is not None:
        coerced = _coerce_commit_sha(base_sha) if isinstance(base_sha, str) else None
        if coerced is None:
            detail = "malformed_base_sha"
            return (f"probe_failed:{detail}", None, None, True, detail)
        try:
            count = _rev_list_count(git, coerced, tip_sha)
        except _GitError as exc:
            return (f"probe_failed:{exc.detail}", None, None, True, exc.detail)
        if count == 0:
            return ("empty_lane", None, None, False, _EMPTY_LANE_EQUALS_BASE)
        if count < 0:
            detail = "rev-list --count negative"
            return (f"probe_failed:{detail}", None, None, True, detail)
        if not refs:
            detail = "empty_lane_first_parent_lineage_unresolvable"
            return (f"probe_failed:{detail}", None, None, True, detail)
        for ref in refs:
            try:
                on_first_parent = _tip_on_first_parent_lineage(git, tip_sha, ref)
                if _worktree_reflog_proves_no_commit_after_base(
                    worktree_path,
                    branch or "",
                    coerced,
                    created_at=created_at,
                ):
                    return (
                        "empty_lane",
                        ref,
                        _ref_sha(git, ref) or tip_sha,
                        False,
                        "empty_lane_base_fast_forward_without_lane_commit",
                    )
                if on_first_parent:
                    return (
                        f"probe_failed:{_OWN_COMMIT_COUNT_AMBIGUOUS}",
                        None,
                        None,
                        True,
                        _OWN_COMMIT_COUNT_AMBIGUOUS,
                    )
            except _GitError as exc:
                return (f"probe_failed:{exc.detail}", None, None, True, exc.detail)
        return None

    for ref in refs:
        try:
            if _tip_on_first_parent_lineage(git, tip_sha, ref):
                return (
                    "empty_lane",
                    ref,
                    _ref_sha(git, ref) or tip_sha,
                    False,
                    _EMPTY_LANE_FIRST_PARENT,
                )
        except _GitError as exc:
            return (f"probe_failed:{exc.detail}", None, None, True, exc.detail)

    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        _lane_created_at_epoch,
        _worktree_reflog_tip_evidence,
    )

    for ref in refs:
        try:
            if not _is_ancestor(git, tip_sha, ref):
                continue
        except _GitError as exc:
            detail = f"empty_lane_ancestry_probe_failed: {exc.detail}"
            return (f"probe_failed:{detail}", None, None, True, detail)
        created_epoch = _lane_created_at_epoch(created_at)
        reflog_evidence = None
        if created_epoch is not None:
            reflog_evidence = _worktree_reflog_tip_evidence(
                worktree_path,
                branch,
                tip_sha,
                lane_created_at=created_at,
            )
        if reflog_evidence == "empty":
            return (
                "empty_lane",
                ref,
                _ref_sha(git, ref) or tip_sha,
                False,
                "empty_lane_branch_equals_worktree_start_sha",
            )
        if reflog_evidence == "own":
            return None
        if created_epoch is None:
            return (
                f"probe_failed:{_EMPTY_LANE_CREATED_AT_UNRESOLVABLE}",
                None,
                None,
                True,
                _EMPTY_LANE_CREATED_AT_UNRESOLVABLE,
            )
        try:
            tip_epoch = _commit_committer_epoch(git, tip_sha)
        except _GitError as exc:
            return (f"probe_failed:{exc.detail}", None, None, True, exc.detail)
        if tip_epoch is None:
            return (
                f"probe_failed:{_EMPTY_LANE_COMMITTER_UNRESOLVABLE}",
                None,
                None,
                True,
                _EMPTY_LANE_COMMITTER_UNRESOLVABLE,
            )
        if _worktree_produced_commits_after_registration(worktree_path, tip_epoch):
            return None
        from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
            LANE_TIMESTAMP_SKEW_TOLERANCE_S,
        )

        if tip_epoch < created_epoch - LANE_TIMESTAMP_SKEW_TOLERANCE_S:
            return (
                "empty_lane",
                ref,
                _ref_sha(git, ref) or tip_sha,
                False,
                _EMPTY_LANE_PREDATES_CREATED,
            )
        if tip_epoch > created_epoch + LANE_TIMESTAMP_SKEW_TOLERANCE_S:
            return None
        detail = _EMPTY_LANE_TIMESTAMP_INCONCLUSIVE
        return (f"probe_failed:{detail}", None, None, True, detail)
    return None


def _worktree_produced_commits_after_registration(worktree_path: str | None, tip_epoch: int) -> bool:
    """True when the tip is strictly newer than linked-worktree registration.

    Same-second cuts stay under the ``created_at`` epoch arm so an empty
    checkout of an already-merged parent is not treated as own work.
    """
    if not worktree_path:
        return False
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _worktree_registration_epoch,
    )

    registered = _worktree_registration_epoch(worktree_path)
    if registered is None:
        return False
    return tip_epoch > registered


def _preservation_kind_from_notes(notes: str | None) -> str | None:
    """Read the ``via <kind>`` token from a post-merge close note, if present."""
    notes = _postmerge_notes(notes)
    text = _text(notes)
    if not text:
        return None
    marker = " via "
    idx = text.rfind(marker)
    if idx < 0:
        return None
    token = text[idx + len(marker) :].strip().split()[0].rstrip(";,")
    if token in _PRESERVED_KINDS or token == _EMPTY_LANE_KIND:
        return token
    return None


def _probe_preservation(
    *,
    repo: Path,
    git: Callable[..., Any],
    branch: str,
    worktree_path: str | None,
    refs: Sequence[str],
    lane_kind: str,
    merged_fn: Callable[..., Any],
    base_sha: str | None = None,
    created_at: object | None = None,
) -> tuple[str, str | None, str | None, str | None, bool, str]:
    """Return ``(kind, preserving_ref, preserving_sha, tip_sha, probe_failed, reason)``."""
    try:
        tip_sha = _probe_tip_sha(git, branch)
    except _GitError as exc:
        return (f"probe_failed:{exc.detail}", None, None, None, True, exc.detail)
    if tip_sha is None:
        return ("unintegrated", None, None, None, False, "")

    refs = _independent_preservation_refs(git, branch, refs)
    if not refs:
        return ("unintegrated", None, None, tip_sha, False, "")

    # Before the empty-lane gate: a payload-only tip is not an empty lane.
    payload_base, payload_failed, payload_detail = _review_payload_base(repo, tip_sha)
    if payload_failed:
        return (f"probe_failed:{payload_detail}", None, None, tip_sha, True, payload_detail)
    if payload_base is not None:
        for ref in refs:
            try:
                # Never DWIM an integration name: tags must not shadow branches.
                integration_sha = _ref_sha(git, _payload_integration_ref(ref))
                if integration_sha is None:
                    raise _GitError(f"integration branch unresolved: {_heads_ref(ref)}")
                landed = _payload_base_landed(git, payload_base, integration_sha)
            except _GitError as exc:
                return (f"probe_failed:{exc.detail}", None, None, tip_sha, True, exc.detail)
            if landed:
                return (
                    "review_output_preserved",
                    ref,
                    integration_sha,
                    tip_sha,
                    False,
                    _REVIEW_PAYLOAD_ONLY_REASON,
                )
        # A payload with an unproved base must not reach a weaker DWIM proof.
        return ("unintegrated", None, None, tip_sha, False, "")

    empty = _own_commit_empty_lane(
        git,
        tip_sha,
        refs,
        base_sha=base_sha,
        created_at=created_at,
        worktree_path=worktree_path,
        branch=branch,
    )
    if empty is not None:
        kind, pref, psha, failed, reason = empty
        return (kind, pref, psha, tip_sha, failed, reason)

    worktree_present = bool(worktree_path) and not _worktree_is_absent(worktree_path, repo)
    ancestry_failures: list[str] = []
    ancestry_disproved = False
    for ref in refs:
        if worktree_present and worktree_path:
            try:
                merged, reason = merged_fn(
                    repo_root=repo,
                    branch=branch,
                    worktree_path=worktree_path,
                    integration_ref=ref,
                )
            except Exception as exc:  # noqa: BLE001 — probe errors are typed
                ancestry_failures.append(str(exc) or type(exc).__name__)
                continue
            if merged is True:
                return ("merged_ancestry", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False, "")
            if merged is None:
                ancestry_failures.append(reason or "merged_probe_failed")
                continue
            ancestry_disproved = True
            continue
        try:
            if _is_ancestor(git, tip_sha, ref):
                return ("merged_ancestry", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False, "")
        except _GitError as exc:
            ancestry_failures.append(exc.detail)
            continue
        ancestry_disproved = True

    content_failures: list[str] = []
    for ref in refs:
        try:
            unlanded = _unlanded_commit_count(git, tip_sha, ref)
        except _GitError as exc:
            content_failures.append(exc.detail)
            continue
        if unlanded == 0:
            return ("merged_content", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False, "")

    if lane_kind == "review":
        review_failures: list[str] = []
        review_disproved = False
        for ref in refs:
            kept, detail = _review_output_preserved(git, tip_sha, ref)
            if kept is True:
                return ("review_output_preserved", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False, "")
            if kept is None:
                review_failures.append(detail)
                continue
            review_disproved = True
        if not review_disproved and review_failures:
            return (f"probe_failed:{review_failures[0]}", None, None, tip_sha, True, review_failures[0])

    if content_failures:
        return (f"probe_failed:{content_failures[0]}", None, None, tip_sha, True, content_failures[0])
    if not ancestry_disproved and ancestry_failures:
        return (f"probe_failed:{ancestry_failures[0]}", None, None, tip_sha, True, ancestry_failures[0])
    integration_ref = refs[0] if refs else None
    if integration_ref is not None:
        verified, integration_tip, detail, probe_failed = _probe_superseded_tombstone(
            repo=repo,
            branch=branch,
            tip=tip_sha,
            integration_ref=integration_ref,
        )
        if probe_failed:
            reason = detail or "superseded_tombstone_probe_failed"
            return (f"probe_failed:{reason}", None, None, tip_sha, True, reason)
        if verified:
            return ("superseded_tombstone", integration_ref, integration_tip, tip_sha, False, "")
        if detail:
            return ("unintegrated", None, None, tip_sha, False, detail)
    return ("unintegrated", None, None, tip_sha, False, "")


def _probe_superseded_tombstone(
    *,
    repo: Path,
    branch: str,
    tip: str,
    integration_ref: str,
    expected_integration_tip: str | None = None,
) -> tuple[bool | None, str | None, str | None, bool]:
    """Verify a current tombstone only when the disposition table authorizes it."""
    from workbay_orchestrator_mcp.orchestration import lane_disposition  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.landing_log import scan_first_parent  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_terminal_dispose import (  # noqa: PLC0415
        BranchDisposition,
    )

    try:
        scan = scan_first_parent(repo, main_ref=integration_ref)
    except Exception as exc:  # noqa: BLE001 — unknown scan state must keep the ref
        return None, None, f"tombstone_scan_failed:{str(exc) or type(exc).__name__}", True
    if not scan.complete:
        return None, None, f"tombstone_scan_failed:{scan.error or 'scan_incomplete'}", True
    branch_name = _branch_short_name(branch)
    matching = [
        tombstone
        for _commit, tombstone in scan.tombstones
        if _branch_short_name(tombstone.branch) == branch_name
        and tombstone.tip == tip
        and tombstone.disposition == "superseded"
        and tombstone.kind == "superseded_by_integration"
    ]
    if not matching:
        return False, None, None, False

    try:
        snapshot = lane_disposition.collect_snapshot(repo, branch_name, integration_ref=integration_ref)
        if snapshot.tip != tip:
            return None, snapshot.integration_tip, "branch_tip_moved_during_tombstone_probe", True
        if expected_integration_tip is not None and snapshot.integration_tip != expected_integration_tip:
            return False, snapshot.integration_tip, "integration_tip_moved", False
        decision = lane_disposition.decide(snapshot)
    except Exception as exc:  # noqa: BLE001 — unknown authority state must keep the ref
        return None, None, f"tombstone_disposition_failed:{str(exc) or type(exc).__name__}", True

    if decision.rule == 0:
        seam = decision.reason.removeprefix("probe_failed:")
        detail = snapshot.probes.get(seam) or decision.reason
        return None, snapshot.integration_tip, f"tombstone_disposition_failed:{detail}", True
    if decision.kind is BranchDisposition.SUPERSEDED_TOMBSTONE and decision.branch_action == "delete_D_after_pin":
        return True, snapshot.integration_tip, None, False

    failures = [
        verdict.failed_check
        for _commit, group in snapshot.tombstone_groups
        for verdict in group
        if verdict.failed_check
    ]
    if failures:
        return False, snapshot.integration_tip, f"superseded_tombstone_unverified:{failures[0]}", False
    return False, snapshot.integration_tip, f"superseded_tombstone_not_authorized:{decision.kind.value}", False


def _resolved_refname(git: Callable[..., Any], ref: str) -> str | None:
    """Resolve a ref spelling to its full refname, dereferencing symbolic refs."""
    try:
        symbolic = _git(git, ["symbolic-ref", "--quiet", ref])
    except _GitError:
        symbolic = None
    if symbolic is not None and symbolic.returncode == 0:
        name = (symbolic.stdout or "").strip()
        return name if name.startswith("refs/") else None

    try:
        full = _git(git, ["rev-parse", "--symbolic-full-name", "--verify", ref])
    except _GitError:
        full = None
    if full is not None and full.returncode == 0:
        name = (full.stdout or "").strip()
        if name.startswith("refs/"):
            return name
    if ref.startswith("refs/"):
        return ref
    if ref and not ref.startswith("-"):
        return f"refs/heads/{ref}"
    return None


def _independent_preservation_refs(
    git: Callable[..., Any],
    branch: str,
    refs: Sequence[str],
) -> list[str]:
    """Remove the lane branch and every ref spelling that aliases it."""
    lane_ref = _resolved_refname(git, _heads_ref(branch))
    if lane_ref is None:
        return []
    independent: list[str] = []
    for ref in refs:
        if not isinstance(ref, str) or not ref.strip():
            continue
        candidate = ref.strip()
        if _resolved_refname(git, candidate) != lane_ref:
            independent.append(candidate)
    return independent


def _review_output_preserved(
    git: Callable[..., Any],
    tip_sha: str,
    integration_ref: str,
) -> tuple[bool | None, str]:
    try:
        proc = _git(
            git,
            [
                "diff",
                "--name-only",
                "--diff-filter=A",
                f"{integration_ref}...{tip_sha}",
                "--",
                "docs/reviews/",
            ],
        )
    except _GitError as exc:
        return None, exc.detail
    if proc.returncode != 0:
        return None, _proc_detail(proc) or "review diff failed"
    paths = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip().startswith("docs/reviews/")]
    if not paths:
        return False, "no review output paths"
    for path in paths:
        lane_blob = _blob_at(git, tip_sha, path)
        if lane_blob is None:
            return None, f"review blob missing on lane: {path}"
        integ_blob = _blob_at(git, integration_ref, path)
        if integ_blob is None or integ_blob != lane_blob:
            return False, f"review output mismatch: {path}"
    try:
        tree_proc = _git(
            git,
            [
                "diff",
                "--name-only",
                f"{integration_ref}...{tip_sha}",
            ],
        )
    except _GitError as exc:
        return None, exc.detail
    if tree_proc.returncode != 0:
        return None, _proc_detail(tree_proc) or "review tree diff failed"
    for raw in (tree_proc.stdout or "").splitlines():
        path = raw.strip()
        if not path:
            continue
        lane_blob = _blob_at(git, tip_sha, path)
        integ_blob = _blob_at(git, integration_ref, path)
        if lane_blob is None or integ_blob is None or lane_blob != integ_blob:
            if path.startswith("docs/reviews/"):
                return False, f"review output mismatch: {path}"
            return False, f"non-review change not on integration: {path}"
    return True, integration_ref


def _review_payload_base(repo: Path, tip_sha: str) -> tuple[str | None, bool, str]:
    """Unwind orchestrator review-payload commits off ``tip_sha``.

    Returns ``(base, False, "")`` when at least one payload commit sits on
    ``base``. Returns ``(None, False, "")`` when the tip carries no payload
    commit, or the payload chain exceeds the walk limit: such a tip is not
    payload-only, so the existing proofs decide. Returns ``(None, True, detail)``
    on any probe failure (SECD-05). The subject and path proof is the one
    ``secure_sandbox`` uses to unwind its own payloads.
    """
    from workbay_orchestrator_mcp.orchestration.secure_sandbox import (  # noqa: PLC0415
        ReviewContextPayloadRefusal,
        _unwind_payload_tip,
    )

    try:
        base, walked = _unwind_payload_tip(repo, tip_sha)
    except ReviewContextPayloadRefusal as exc:
        if exc.code == "review_subject_self_referential":
            return None, False, ""
        return None, True, exc.detail
    except Exception as exc:  # noqa: BLE001 — any unwind failure is a probe failure
        return None, True, str(exc) or type(exc).__name__
    if walked == 0:
        return None, False, ""
    return base, False, ""


def _payload_integration_ref(ref: str) -> str:
    if ref.startswith("refs/") and not ref.startswith("refs/heads/"):
        raise _GitError(f"integration target is not a local branch: {ref}")
    return _heads_ref(ref)


def _git_bytes(git: Callable[..., Any], args: Sequence[str | bytes], failure: str) -> bytes:
    """Run a path-bearing probe in bytes mode; text output cannot prove path identity.

    Universal newlines rewrite a lone CR to LF, so decoded ``-z`` output can
    alias two distinct paths. A runner that cannot return ``bytes`` is a probe
    failure (SECD-05), never a decoded fallback.
    """
    try:
        proc = git(list(args), text=False)
    except subprocess.TimeoutExpired as exc:
        raise _GitError("git timeout") from exc
    except Exception as exc:  # noqa: BLE001 — probe errors are verdicts
        raise _GitError(str(exc) or type(exc).__name__) from exc
    if proc is None:
        raise _GitError("git unavailable")
    if getattr(proc, "returncode", None) != 0:
        detail = ""
        for stream in (getattr(proc, "stderr", None), getattr(proc, "stdout", None)):
            if isinstance(stream, bytes):
                stream = stream.decode("utf-8", errors="backslashreplace")
            detail = str(stream or "").strip()
            if detail:
                break
        raise _GitError(detail or failure)
    stdout = getattr(proc, "stdout", None)
    if not isinstance(stdout, bytes):
        raise _GitError(f"{failure}: git output is not byte-exact")
    return stdout


def _single_object_id(raw: bytes) -> bytes | None:
    """The one full lowercase object id in ``raw``, or ``None`` for anything else."""
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        return None
    sha = raw[:-1]
    if len(sha) not in (40, 64) or sha.strip(b"0123456789abcdef"):
        return None
    return sha


def _exact_local_branch_tip(git: Callable[..., Any], branch: str) -> bytes | None:
    """The commit named by exactly ``refs/heads/<branch>``, or ``None``.

    Only the full local branch ref is read (``show-ref --verify``), never
    rev-parse shorthand: a tag named ``refs/heads/<branch>`` cannot answer
    for a deleted or ambiguous branch. ``branch`` must be a plain branch
    name; a ``refs/`` prefix, an invalid or expanding name, a missing ref,
    more than one line, or any git error is ``None``. Callers treat ``None``
    as a tip mismatch and keep (Canon 0.26.2 SECD-05, CANON#secd-05; CON-11,
    CANON#con-11).
    """
    if not branch or branch.startswith("refs/"):
        return None
    try:
        checked = _git_bytes(git, ["check-ref-format", "--branch", branch], "branch name rejected")
        if checked != branch.encode("utf-8") + b"\n":
            return None
        listed = _single_object_id(
            _git_bytes(git, ["show-ref", "--verify", "--hash", "--", f"refs/heads/{branch}"], "branch ref unresolved")
        )
        if listed is None:
            return None
        peeled = _git_bytes(
            git,
            ["rev-parse", "--verify", "--quiet", "--end-of-options", listed.decode("ascii") + "^{commit}"],
            "branch tip is not a commit",
        )
    except (UnicodeError, _GitError):
        return None
    return _single_object_id(peeled)


def _payload_completion_is_current(git: Callable[..., Any], notes: str | None, branch: str) -> bool:
    """A payload completion settles the row only while the exact branch still names its tip.

    A moved, deleted or unreadable branch, or a completion without a recorded
    tip, re-evaluates the row from the live tip instead (Canon 0.26.2 CON-11,
    CANON#con-11).
    """
    if not _payload_reclaim_complete(notes):
        return False
    tip = _tip_from_close_notes(notes)
    live = _exact_local_branch_tip(git, branch)
    return bool(tip) and live is not None and live == (tip or "").encode("ascii", errors="replace")


def _stored_tree_entry(git: Callable[..., Any], rev: str, path: bytes) -> bytes | None:
    """Read the stored mode, type and object id, without diff drivers or filters.

    ``path`` is passed back to git as the exact bytes git listed, as a literal pathspec.
    """
    raw = _git_bytes(
        git,
        ["--literal-pathspecs", "ls-tree", "-z", "--full-tree", rev, "--", path],
        "tree entry verification failed",
    )
    if not raw:
        return None
    if not raw.endswith(b"\0") or raw.count(b"\0") != 1 or raw[:-1].partition(b"\t")[2] != path:
        raise _GitError(f"unexpected tree entry for {path!r}")
    return raw


def _payload_base_landed(git: Callable[..., Any], base: str, integration_ref: str) -> bool:
    """True when ``base`` is merged into, or content-equal to, ``integration_ref``."""
    if _is_ancestor(git, base, integration_ref):
        return True
    if _unlanded_commit_count(git, base, integration_ref) != 0:
        return False
    # git cherry omits merge commits, including unique conflict resolutions.
    merges = _rev_list_range(git, integration_ref, base, option="--merges")
    if merges.returncode != 0:
        raise _GitError(_proc_detail(merges) or "merge enumeration failed")
    # Paths stay the exact bytes git stores; a decoded name can alias another path.
    paths: set[bytes] = set()
    for merge in (merges.stdout or "").splitlines():
        touched = _git_bytes(
            git,
            [
                "diff-tree",
                "-m",
                "--no-commit-id",
                "--name-only",
                "--no-relative",
                "--no-renames",
                "--no-ext-diff",
                "--no-textconv",
                "--ignore-submodules=none",
                "-r",
                "-z",
                merge,
            ],
            "merge path enumeration failed",
        )
        if touched and not touched.endswith(b"\0"):
            raise _GitError("merge path enumeration output is not NUL-terminated")
        paths.update(path for path in touched.split(b"\0") if path)
    # Compare stored entries, including executable modes and 160000 gitlinks.
    # A configurable diff can hide content (e.g. diff.ignoreSubmodules=all).
    for path in sorted(paths):
        base_entry = _stored_tree_entry(git, base, path)
        integration_entry = _stored_tree_entry(git, integration_ref, path)
        if base_entry is None or integration_entry is None or base_entry != integration_entry:
            return False
    return True


def _review_payload_proof(
    git: Callable[..., Any],
    repo: Path,
    tip_sha: str,
    integration_ref: str,
    *,
    integration_sha: str | None = None,
) -> tuple[bool | None, str]:
    """Prove ``tip_sha`` is a preserved base plus review-payload commits only.

    ``(True, "")`` when proven. ``(False, detail)`` when the tip is not a
    payload tip, or its base is not on ``integration_ref``. ``(None, detail)``
    on a probe failure, which every caller keeps (fail closed).
    """
    base, failed, detail = _review_payload_base(repo, tip_sha)
    if failed:
        return None, detail
    if base is None:
        return False, "no review payload commit"
    try:
        integration_sha = integration_sha or _ref_sha(git, _payload_integration_ref(integration_ref))
        if integration_sha is None:
            raise _GitError(f"integration branch unresolved: {_heads_ref(integration_ref)}")
        landed = _payload_base_landed(git, base, integration_sha)
    except _GitError as exc:
        return None, exc.detail
    if landed:
        return True, ""
    return False, f"review payload base not on {integration_ref}"


def _blob_at(git: Callable[..., Any], rev: str, path: str) -> str | None:
    try:
        proc = _git(git, ["rev-parse", "--verify", "--quiet", f"{rev}:{path}"])
    except _GitError:
        return None
    if proc.returncode != 0:
        return None
    sha = (proc.stdout or "").strip()
    return sha or None


def _ref_sha(git: Callable[..., Any], ref: str) -> str | None:
    try:
        proc = _git(git, ["rev-parse", "--verify", f"{ref}^{{commit}}"])
    except _GitError:
        return None
    if proc.returncode != 0:
        return None
    sha = (proc.stdout or "").strip()
    return sha or None


def _probe_ownership(
    *,
    worktree_path: str | None,
    repo: Path,
    lane_id: str,
    owner_fn: Callable[..., Any],
    lease_fn: Callable[..., Any],
) -> tuple[str, bool]:
    if worktree_path and not _worktree_is_absent(worktree_path, repo):
        try:
            state, detail = owner_fn(worktree_path)
        except Exception as exc:  # noqa: BLE001 — probe errors are typed
            return (f"probe_failed:{exc}", True)
        if state == "owned":
            label = detail or "process"
            return (f"owned:{label}", False)
        if state == "unknown":
            return (f"probe_failed:{detail or 'owner_unknown'}", True)
        if state not in {"free", "unowned"}:
            return (f"probe_failed:{state}:{detail}", True)
    try:
        lease = lease_fn(lane_id)
    except Exception as exc:  # noqa: BLE001 — probe errors are typed
        return (f"probe_failed:{exc}", True)
    lease_text = "unowned" if lease is None else str(lease).strip() or "unowned"
    if lease_text.startswith("owned"):
        return (lease_text if lease_text.startswith("owned:") else f"owned:{lease_text}", False)
    if lease_text.startswith("probe_failed"):
        return (
            lease_text if ":" in lease_text else f"probe_failed:{lease_text}",
            True,
        )
    return ("unowned", False)


def _probe_cleanliness(
    *,
    worktree_path: str | None,
    repo: Path,
    clean_fn: Callable[..., Any],
) -> tuple[str, bool]:
    if not worktree_path or _worktree_is_absent(worktree_path, repo):
        return ("clean", False)
    try:
        clean, detail = clean_fn(worktree_path)
    except Exception as exc:  # noqa: BLE001 — probe errors are typed
        return (f"probe_failed:{exc}", True)
    if clean is None:
        return (f"probe_failed:{detail or 'status_probe_failed'}", True)
    if clean is True:
        return ("clean", False)
    paths = _dirty_paths(worktree_path)
    if paths:
        return ("dirty:" + ",".join(paths[:3]), False)
    text = detail or "dirty worktree"
    return (text if text.startswith("dirty") else f"dirty:{text}", False)


def _dirty_paths(worktree_path: str) -> list[str]:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _run_reclaim_command,
    )

    proc = _run_reclaim_command(["git", "-C", worktree_path, "status", "--porcelain"])
    if proc is None or proc.returncode != 0:
        return []
    paths: list[str] = []
    for line in (proc.stdout or "").splitlines():
        if not line.strip():
            continue
        rest = line[3:] if len(line) > 3 else line.strip()
        if " -> " in rest:
            rest = rest.split(" -> ", 1)[1]
        rest = rest.strip().strip('"')
        if rest:
            paths.append(rest)
        if len(paths) >= 3:
            break
    return paths


def _row_age_seconds(row: object, clock: datetime) -> int | None:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _parse_sqlite_utc,
    )

    stamp = _parse_sqlite_utc(_row_get(row, "updated_at")) or _parse_sqlite_utc(_row_get(row, "created_at"))
    if stamp is None:
        return None
    current = clock if clock.tzinfo is not None else clock.replace(tzinfo=timezone.utc)
    return max(0, int((current - stamp).total_seconds()))


def _default_probe_owner(worktree_path: object) -> tuple[str, str]:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _probe_worktree_process_owner,
    )

    return _probe_worktree_process_owner(worktree_path)


def _default_probe_lease(lane_id: object) -> str:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _acquire_lane_worker_lock,
        _is_peer_held_reaping_claim,
        _normalize_optional_text,
        _release_lane_worker_lock,
    )

    key = _normalize_optional_text(lane_id)
    if key is None:
        return "probe_failed:lane_id_missing"
    handle, detail = _acquire_lane_worker_lock(key)
    if handle is not None:
        _release_lane_worker_lock(handle)
        return "unowned"
    if _is_peer_held_reaping_claim(detail):
        return f"owned:lease:{detail}"
    return f"probe_failed:{detail or 'lease_probe_failed'}"


def _default_probe_clean(worktree_path: str) -> tuple[bool | None, str]:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _probe_worktree_clean,
    )

    return _probe_worktree_clean(worktree_path)


def _default_probe_merged(
    *,
    repo_root: Path,
    branch: object,
    worktree_path: str,
    integration_ref: str,
) -> tuple[bool | None, str]:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _probe_worktree_merged,
    )

    return _probe_worktree_merged(
        repo_root=repo_root,
        branch=branch,
        worktree_path=worktree_path,
        integration_ref=integration_ref,
    )


def _probe_cross_session_reclaim_guard(
    *,
    row: object,
    item: Mapping[str, Any],
    repo: Path,
    task_ref: str,
) -> tuple[str | None, str]:
    """Run the apply guard probes read-only for a dry-run preview.

    ``_under_claim_worktree_reclaim_guards`` does not acquire a lock or invoke
    an actuator; it only samples linked-worktree, identity, liveness, dirty,
    ignored, heartbeat, and shared-path state. A raised probe is typed as
    ``unprobed`` so a report never presents an unknown guard result as zero.
    """
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _under_claim_worktree_reclaim_guards,
    )

    try:
        worktree_path = _text(_row_get(row, "worktree_path"))
        branch = _text(item.get("branch"))
        tip_sha = _text(item.get("tip_sha"))
        worktree_present = bool(worktree_path) and not _worktree_is_absent(worktree_path, repo)
        if worktree_present and worktree_path:
            tree_outcome, tree_detail = _under_claim_worktree_reclaim_guards(
                worktree_path=worktree_path,
                branch=branch,
                repo_root=repo,
                task_ref=task_ref,
                lane_id=item.get("lane_id"),
            )
            if tree_outcome is not None:
                return tree_outcome, tree_detail

        if not branch or not tip_sha:
            return (None, "branch deletion would not be attempted")
        current_tip = _probe_tip_sha(_default_run_git(repo), branch)
        if current_tip != tip_sha:
            return (None, "branch tip moved; branch-only deletion would not be attempted")
        if not worktree_present:
            return _under_claim_worktree_reclaim_guards(
                worktree_path=None,
                branch=branch,
                repo_root=repo,
                task_ref=task_ref,
                lane_id=item.get("lane_id"),
            )

        # The apply path removes the candidate worktree before its branch-only
        # probe. Exclude that same path from report mode's successor-checkout
        # scan so the current checkout does not become a stale refusal.
        from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
            _branch_checkout_blocks_reclaim,
            _branch_ledger_owner_blocks_reclaim,
        )

        checkout_blocks, checkout_detail = _branch_checkout_blocks_reclaim(
            repo_root=repo,
            branch=branch,
            exclude_path=worktree_path,
        )
        if checkout_blocks:
            return ("refused_owned", checkout_detail)
        ledger_blocks, ledger_detail = _branch_ledger_owner_blocks_reclaim(
            branch=branch,
            task_ref=task_ref,
            lane_id=_text(item.get("lane_id")),
        )
        if ledger_blocks:
            return ("refused_owned", ledger_detail)
        return (None, "branch_only")
    except Exception as exc:  # noqa: BLE001 — dry-run probes fail closed
        return ("unprobed", str(exc) or type(exc).__name__)


def _delete_authorized_preserved_branch(
    *,
    repo: Path,
    task_ref: str,
    lane_id: str,
    branch: str,
    tip_sha: str | None,
    integration_ref: str,
) -> tuple[bool, str]:
    """Delete only after the disposition reference monitor authorizes the tip."""
    if not tip_sha:
        return False, "probe_failed:authorized_tip_missing"

    from workbay_orchestrator_mcp.orchestration.branch_reclaim_delete import (  # noqa: PLC0415
        FENCE_RELEASE_DETAIL,
        delete_authorized_branch,
        exception_fence_release,
    )

    try:
        result = delete_authorized_branch(
            orchestrator_root=repo,
            lane_id=lane_id,
            branch=branch,
            authorized_sha=tip_sha,
            apply=True,
            task_ref=task_ref,
            integration_ref=integration_ref,
        )
    except Exception as exc:  # noqa: BLE001 — unknown authorization failure keeps the ref
        detail = str(exc) or type(exc).__name__
        release = exception_fence_release(exc)
        if release:
            _note_cross_session_fence_release("branch_delete", release)
            detail = f"{detail};{FENCE_RELEASE_DETAIL}{release}"
        return False, f"authorization_failed:{detail}"

    _note_cross_session_fence_release("branch_delete", result.fence_release)
    if result.deleted:
        # A delete that ran under the row's fence but could not release it says so beside its success.
        unreleased = result.fence_release not in {"", "released"}
        return True, f"{FENCE_RELEASE_DETAIL}{result.fence_release}" if unreleased else ""
    reason = str(result.reason or "authorization_refused")
    if result.detail:
        reason = f"{reason}: {result.detail}"
    return False, reason


#: Unreleased reclaim-fence states of the cross-session reclaim in progress, one list per item.
_CROSS_SESSION_FENCE_RELEASES: contextvars.ContextVar[list[dict[str, str]] | None] = contextvars.ContextVar(
    "_CROSS_SESSION_FENCE_RELEASES", default=None
)


def _note_cross_session_fence_release(step: str, release: str) -> None:
    """Record a fenced step that ran but left its fence ``not_ours`` or ``release_failed`` [AGT-10]."""
    releases = _CROSS_SESSION_FENCE_RELEASES.get()
    record = {"step": step, "release": release}
    if releases is not None and release and release != "released" and record not in releases:
        releases.append(record)


def _with_cross_session_fence_releases(
    reclaim: Callable[..., dict[str, Any]], item: dict[str, Any], **kwargs: Any
) -> dict[str, Any]:
    """Run one cross-session reclaim and carry every unreleased fence onto its result, applied or kept.

    The release state stays beside the item's own verdict, so a later failure or a
    successful ``applied`` never erases it; an exception carries it as a note.
    """
    releases: list[dict[str, str]] = []
    token = _CROSS_SESSION_FENCE_RELEASES.set(releases)
    try:
        result = reclaim(item, **kwargs)
    except BaseException as exc:
        if releases:
            exc.add_note(f"fence_release={json.dumps(releases, sort_keys=True)}")
        raise
    finally:
        _CROSS_SESSION_FENCE_RELEASES.reset(token)
    if releases and isinstance(result, dict):
        result = {**result, "fence_release": releases, "cleanup_pending": "reclaim_fence_release"}
    return result


_CROSS_SESSION_RECLAIM_KEEP = {
    "refused_ignored": "keep:ignored",
    "refused_ignored_timeout": "keep:ignored",
    "refused_ignored_unknown": "keep:probe_failed",
    "refused_dirty": "keep:dirty",
    "refused_dirty_recoverable": "keep:dirty",
    "refused_owned": "keep:owned",
    "refused_owner_unknown": "keep:probe_failed",
    "refused_session_live": "keep:owned",
    "refused_shared_path": "keep:shared_path",
    "refused_not_linked_worktree": "keep:identity_mismatch",
    "refused_repo_root": "keep:identity_mismatch",
    "refused_registry_unavailable": "keep:probe_failed",
    "refused_status_unknown": "keep:probe_failed",
    "refused_head_mismatch": "keep:identity_mismatch",
    "refused_head_unknown": "keep:probe_failed",
    "skipped_no_worktree_path": "keep:probe_failed",
}

_CROSS_SESSION_GUARD_REFUSALS = frozenset(
    {
        "keep:ignored",
        "keep:shared_path",
        "keep:dirty",
        "keep:owned",
        "keep:identity_mismatch",
        "keep:claim_failed",
    }
)


def _cross_session_keep(item: dict[str, Any], verdict: str, reason: str) -> dict[str, Any]:
    failed = dict(item)
    failed["verdict"] = verdict
    failed["reason"] = reason
    failed["applied"] = False
    return failed


def _cross_session_keep_from_reclaim(
    item: dict[str, Any],
    outcome: str,
    detail: str,
) -> dict[str, Any]:
    verdict = _CROSS_SESSION_RECLAIM_KEEP.get(outcome, "keep:probe_failed")
    return _cross_session_keep(item, verdict, detail or outcome)


def _stamp_row_notes(row: object | None, notes: str) -> bool:
    """Last-ditch in-memory stamp so retry classification can see the marker."""
    if row is None:
        return False
    try:
        setattr(row, "notes", notes)
        return True
    except Exception:  # noqa: BLE001 — in-memory stamp is best-effort
        pass
    if isinstance(row, dict):
        row["notes"] = notes
        return True
    return False


def _fresh_lane_row(
    list_rows: Callable[..., Any] | None,
    task_ref: str | None,
    lane_id: str,
) -> object | None:
    if list_rows is None or not lane_id:
        return None
    try:
        raw = _call_list_rows(
            list_rows,
            task_ref or "",
            status="all",
            limit=_LIST_PAGE_LIMIT,
        )
        rows = _coerce_rows(raw)
    except Exception:  # noqa: BLE001 — unverified persist is a miss
        return None
    for candidate in rows:
        if _text(_row_get(candidate, "lane_id")) == lane_id:
            return candidate
    return None


def _retry_notes_are_durable(row: object | None, tip_sha: str | None) -> bool:
    if row is None:
        return False
    notes = _text(_row_get(row, "notes"))
    if not _notes_have_retry_identity(notes):
        return False
    if tip_sha and tip_sha not in (notes or ""):
        return False
    return True


def _terminal_generation_after_close(
    row: object,
    list_rows: Callable[..., Any] | None,
    task_ref: str | None,
    lane_id: str,
) -> str:
    """Generation of the post-close terminal row, never the pre-close snapshot."""
    fresh = _fresh_lane_row(list_rows, task_ref, lane_id)
    if fresh is not None:
        gen = _row_generation(fresh)
        if gen:
            return gen
    return _row_generation(row)


def _fallback_persist_retry_notes(
    *,
    row: object | None,
    notes: str,
    lane_id: str,
    tip_sha: str | None,
    task_ref: str | None = None,
    list_rows: Callable[..., Any] | None = None,
) -> bool:
    """Reuse the agent-error spool as the durable failure sink (OBS-08).

    Sink success is not acknowledgement. An in-memory stamp is not
    acknowledgement. Only a fresh lane-row read that already carries the
    retry marker counts (REAP07RV-002). Repo-scoped agent-error telemetry
    cannot put that marker on the lane row, so it stays unacknowledged.
    """
    try:
        from workbay_handoff_mcp.agent_errors import (  # noqa: PLC0415
            record_agent_error_direct,
        )

        record_agent_error_direct(
            error_class="reap_retry_persist_failed",
            summary=f"cross-session reap retry marker for {lane_id}",
            detail=notes,
            task_ref=task_ref,
            tool_name="reap_preserved_lanes_cross_session",
        )
    except Exception:  # noqa: BLE001 — telemetry miss does not ack
        pass
    fresh = _fresh_lane_row(list_rows, task_ref, lane_id)
    if _retry_notes_are_durable(fresh, tip_sha):
        _stamp_row_notes(row, notes)
        return True
    return False


def _persist_cross_session_closed_notes(
    close_row: Callable[..., Any],
    *,
    lane_id: str,
    notes: str,
    tip_sha: str | None,
    row: object | None = None,
    task_ref: str | None = None,
    expected_status: str = "closed",
    list_rows: Callable[..., Any] | None = None,
    expected_tip_sha: str | None = None,
) -> bool:
    """Write failure-state notes and verify them through a fresh row read.

    A discarded CAS result looks like success while the retry marker is
    missing. Retry once, then fall back to the existing agent-error spool.
    The closer's boolean is never acknowledgement by itself.
    ``expected_tip_sha`` is the row's recorded tip (NULL unconstrained);
    ``tip_sha`` is the probed git tip that must remain in the notes.
    """
    for _attempt in range(2):
        closed, _detail = _cas_close(
            close_row,
            lane_id=lane_id,
            expected_status=expected_status,
            notes=notes,
            expected_tip_sha=expected_tip_sha,
        )
        if not closed:
            continue
        fresh = _fresh_lane_row(list_rows, task_ref, lane_id)
        if _retry_notes_are_durable(fresh, tip_sha):
            _stamp_row_notes(row, notes)
            return True
    return _fallback_persist_retry_notes(
        row=row,
        notes=notes,
        lane_id=lane_id,
        tip_sha=tip_sha,
        task_ref=task_ref,
        list_rows=list_rows,
    )


def _payload_reclaim_complete(notes: str | None) -> bool:
    notes = _postmerge_notes(notes)
    tip = _tip_from_close_notes(notes)
    return bool(
        tip
        and _PAYLOAD_BRANCH_KEPT in (notes or "")
        and f"{_REAP_COMPLETE_MARKER}{tip}" in (notes or "")
        and not _notes_have_retry_identity(notes)
    )


def _payload_kept_notes(base: str, tip_sha: str) -> str:
    """The settled branch-kept marker: ``base`` plus kept payload branch and completion of ``tip_sha``."""
    return f"{base}; {_PAYLOAD_BRANCH_KEPT}; {_REAP_COMPLETE_MARKER}{tip_sha}"


def _ack_payload_reclaim(
    close_row: Callable[..., Any],
    *,
    list_rows: Callable[..., Any] | None,
    task_ref: str,
    lane_id: str,
    tip_sha: str,
    expected_tip_sha: str | None,
) -> bool:
    """One durable notes update acknowledges recording and consumes retry intent."""
    fresh = _fresh_lane_row(list_rows, task_ref, lane_id)
    notes = _text(_row_get(fresh, "notes")) or ""
    if _tip_from_close_notes(notes) != tip_sha or not _notes_have_retry_identity(notes):
        return False
    ref, sha = _preserving_identity_from_notes(notes)
    if not ref or not sha:
        return False
    kind = _preservation_kind_from_notes(notes) or "review_output_preserved"
    completed = _payload_kept_notes(_cross_session_close_notes(lane_id, tip_sha, ref, sha, kind=kind), tip_sha)
    closed, _detail = _cas_close(
        close_row,
        lane_id=lane_id,
        expected_status="closed",
        notes=completed,
        expected_tip_sha=expected_tip_sha,
    )
    return closed and _payload_reclaim_complete(_text(_row_get(_fresh_lane_row(list_rows, task_ref, lane_id), "notes")))


def _cross_session_retry_notes(
    notes: str,
    extra: str,
    tip_sha: str,
    generation: str = "",
    nonce: str = "",
) -> str:
    notes = _strip_reap_guard_refused(notes)
    suffix = extra.strip()
    token = nonce or secrets.token_hex(8)
    marker = f"{_RETRY_IDENTITY_MARKER}{tip_sha} {_RETRY_NONCE_MARKER}{token}"
    if generation:
        marker = f"{marker} {_RETRY_GEN_MARKER}{generation}"
    if suffix:
        return f"{notes}; {suffix}; {marker}"
    return f"{notes}; {marker}"


def _retry_snapshot_matches(claimed: object, current: object) -> bool:
    """Compare the claim's identity, generation and notes without normalizing bytes.

    Include both monotonic generations and the legacy timestamp generation,
    as well as the recorded branch observation and authority fence. A notes-only
    match cannot authorize settlement of a re-registered row (Canon 0.26.2
    CON-11, CANON#con-11).
    """
    return all(
        _row_get(claimed, key) == _row_get(current, key)
        for key in (
            "id",
            "task_ref",
            "lane_id",
            "branch",
            "worktree_path",
            "status",
            "owner_agent",
            "lane_kind",
            "branch_tip_sha",
            "branch_tip_observed_at",
            "branch_tip_source",
            "landing_commit_sha",
            "identity_seq",
            "cycle_epoch",
            "reclaim_fence",
            "created_at",
            "updated_at",
            "notes",
        )
    )


def _refuse_unauthoritative_retry_row(
    *,
    listed: object,
    fresh: object | None,
    item: Mapping[str, Any],
    tip_sha: str | None,
) -> tuple[str, str] | None:
    """Refuse a retry whose fresh row is missing, unreadable, or no longer the claim."""
    if fresh is None:
        return ("keep:probe_failed", "retry_row_unreadable")
    if not _retry_snapshot_matches(listed, fresh):
        return ("keep:identity_mismatch", "retry_row_changed_under_claim")
    status = _text(_row_get(fresh, "status")) or ""
    if status not in TERMINAL_STATUSES:
        return ("keep:identity_mismatch", "retry_row_changed_under_claim")
    fresh_notes = _text(_row_get(fresh, "notes")) or ""
    if not _notes_have_retry_identity(fresh_notes) and not _notes_have_retry_identity(_text(_row_get(listed, "notes"))):
        # An old close note is not a fenced replay intent: it never authorizes
        # an effect. The caller settles the row once to branch kept; see
        # ``_settle_markerless_terminal_kept``.
        return ("keep:probe_failed", _RETRY_MARKER_ABSENT)
    if not tip_sha or f"{_RETRY_IDENTITY_MARKER}{tip_sha}" not in fresh_notes:
        return ("keep:identity_mismatch", "retry_row_changed_under_claim")
    if not _nonce_from_notes(fresh_notes):
        return ("keep:identity_mismatch", "retry_marker_unfenced")
    listed_nonce = _nonce_from_notes(_text(_row_get(listed, "notes")))
    fresh_nonce = _nonce_from_notes(fresh_notes)
    if listed_nonce and listed_nonce != fresh_nonce:
        return ("keep:identity_mismatch", "retry_row_changed_under_claim")
    if _generation_from_notes(fresh_notes) and not _retry_generation_matches(fresh, fresh_notes):
        return ("keep:identity_mismatch", "retry_generation_mismatch")
    return None


def _write_kept_with_decision(
    *,
    task_ref: str,
    row: object,
    git: Callable[..., Any],
    tip_sha: str,
    notes: str,
    decision_id: str,
    rationale: str,
) -> tuple[bool, str]:
    """Record the kept decision and write the kept marker in one SQLite transaction.

    In one explicit deferred transaction, re-read the entire claim identity
    and verify the exact local branch ref still names the validated tip before
    either write. The read snapshot fences the subsequent write upgrade:
    a concurrent writer makes it fail, never silently adopt a newer snapshot.
    Git runs before RESERVED, respecting the blocking-work guard.
    A refused or failed write rolls the decision back with it,
    so both are durable or neither is. This writes the handoff DB directly,
    as the default closer and recorder do: two injected callables cannot
    share one transaction. Pinned canon 0.26.2: RES-19 (CANON#res-19),
    CON-11 (CANON#con-11).
    """
    from workbay_handoff_mcp.decisions import _record_decision_with_conn  # noqa: PLC0415
    from workbay_handoff_mcp.shared_primitives import _resolve_task_ref  # noqa: PLC0415
    from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415
    from workbay_handoff_mcp.sqlite_lock_errors import SqliteLockKind, classify_sqlite_lock_error  # noqa: PLC0415

    lane_id = _text(_row_get(row, "lane_id")) or ""
    status = _text(_row_get(row, "status")) or ""
    try:
        with _get_db_connection() as conn:
            conn.execute("BEGIN")
            resolved = _resolve_task_ref(conn, task_ref)
            current = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
                (resolved, lane_id),
            ).fetchall()
            if len(current) != 1 or not _retry_snapshot_matches(row, dict(current[0])):
                conn.rollback()
                return False, "retry_row_changed_under_claim"
            live_tip = _exact_local_branch_tip(git, str(_row_get(row, "branch") or ""))
            if live_tip is None or live_tip != tip_sha.encode("ascii", errors="replace"):
                conn.rollback()
                return False, "retry_row_changed_under_claim"
            recorded = _record_decision_with_conn(
                conn,
                session=decision_id,
                decision="postmerge_reap",
                rationale=rationale,
                task_ref=resolved,
                event_id=decision_id,
            )
            if not recorded.get("ok"):
                conn.rollback()
                data = recorded.get("data")
                error = data.get("error") if isinstance(data, Mapping) else None
                return False, f"decision: {error or recorded.get('error') or 'record_failed'}"
            written, writable = _close_notes_keeping_authority(
                conn, task_ref=resolved, lane_id=lane_id, expected_status=status, notes=notes
            )
            if not writable or written is None:
                conn.rollback()
                return False, "kept_marker_unwritable"
            cur = conn.execute(
                "UPDATE worktree_lanes SET notes = ? WHERE task_ref = ? AND lane_id = ? AND status = ? AND notes IS ?",
                (written, resolved, lane_id, current[0]["status"], current[0]["notes"]),
            )
            if int(cur.rowcount or 0) != 1:
                conn.rollback()
                return False, "kept_marker_cas_miss"
    except Exception as exc:  # noqa: BLE001 — the context manager rolled both writes back
        if classify_sqlite_lock_error(exc) in {
            SqliteLockKind.BUSY_SNAPSHOT,
            SqliteLockKind.BUSY,
            SqliteLockKind.LOCKED,
        }:
            return False, "retry_row_changed_under_claim"
        return False, str(exc) or type(exc).__name__
    return True, ""


def _settle_markerless_terminal_kept(
    item: dict[str, Any],
    *,
    listed: object,
    fresh: object,
    git: Callable[..., Any],
    repo: Path,
    task_ref: str,
    lane_id: str,
    preserving_ref: str,
    preserving_sha: str,
) -> dict[str, Any]:
    """Converge a markerless terminal payload row once to its safe state, branch kept.

    The first apply records one ``retry_marker_absent`` decision with outcome
    kept and writes the existing kept marker in the same transaction. Later
    applies see the marker and change nothing. No Git effect runs here: the
    branch and worktree stay as they are. A refused write records nothing and
    is reported, so the next apply retries. Pinned canon 0.26.2: RES-19
    (CANON#res-19), SECD-05 (CANON#secd-05), AGT-10 (CANON#agt-10).
    """
    fresh_notes = _text(_row_get(fresh, "notes")) or ""
    tip_sha = _tip_from_close_notes(fresh_notes)
    if not _retry_snapshot_matches(listed, fresh) or not tip_sha:
        return _cross_session_keep(item, "keep:identity_mismatch", "retry_row_changed_under_claim")
    branch = _text(_row_get(fresh, "branch")) or ""
    if _payload_reclaim_complete(fresh_notes):
        if _payload_completion_is_current(git, fresh_notes, branch):
            return _cross_session_keep(item, "keep:already_terminal", f"{_PAYLOAD_BRANCH_KEPT}; {_RETRY_MARKER_ABSENT}")
        # The branch left the completed tip: never complete the same tip twice.
        return _cross_session_keep(item, "keep:identity_mismatch", "retry_row_changed_under_claim")
    payload_kept, payload_detail = _review_payload_proof(
        git, repo, tip_sha, preserving_ref, integration_sha=preserving_sha
    )
    if payload_kept is None:
        return _cross_session_keep(
            item, "keep:probe_failed", f"{_RETRY_MARKER_ABSENT}: preservation:probe_failed:{payload_detail}"
        )
    if not payload_kept:
        # Not a payload row: the kept marker would misstate it. Keep everything, loudly.
        return _cross_session_keep(item, "keep:probe_failed", _RETRY_MARKER_ABSENT)
    settled = _payload_kept_notes(_postmerge_notes(fresh_notes) or fresh_notes, tip_sha)
    decision_id = f"postmerge_reap:{task_ref}:{lane_id}:{tip_sha}:kept"
    written, detail = _write_kept_with_decision(
        task_ref=task_ref,
        row=listed,
        git=git,
        tip_sha=tip_sha,
        notes=settled,
        decision_id=decision_id,
        rationale=f"{settled}; reason {_RETRY_MARKER_ABSENT}; outcome kept",
    )
    if not written:
        if detail == "retry_row_changed_under_claim":
            return _cross_session_keep(item, "keep:identity_mismatch", detail)
        return _cross_session_keep(item, "keep:probe_failed", f"{_RETRY_MARKER_ABSENT}: kept_write_refused: {detail}")
    kept = _cross_session_keep(item, "keep:already_terminal", _RETRY_MARKER_ABSENT)
    kept["decision_id"] = decision_id
    kept["outcome"] = "kept"
    return kept


def _cas_retry_effect(
    close_row: Callable[..., Any],
    *,
    lane_id: str,
    expected_status: str,
    notes: str,
    tip_sha: str | None,
    effect: str,
    expected_tip_sha: str | None = None,
) -> tuple[bool, str]:
    """CAS-fence one retry actuator on the terminal row (REAP08RV-001)."""
    del tip_sha
    fenced = _postmerge_notes(notes) or notes
    marker = f"{_RETRY_EFFECT_MARKER}{effect}"
    if marker not in fenced:
        fenced = f"{fenced}; {marker}"
    return _cas_close(
        close_row,
        lane_id=lane_id,
        expected_status=expected_status,
        notes=fenced,
        expected_tip_sha=expected_tip_sha,
    )


def _keep_after_persist(
    item: dict[str, Any],
    persist_failure: Callable[[str], bool],
    extra: str,
    *,
    verdict: str,
    reason: str,
    reclaim_outcome: str | None = None,
    reclaim_detail: str | None = None,
) -> dict[str, Any]:
    acked = persist_failure(extra)
    if not acked:
        return _cross_session_keep(
            item,
            "keep:probe_failed",
            f"{_UNACKNOWLEDGED_FAILURE}: {extra}",
        )
    if reclaim_outcome is not None:
        return _cross_session_keep_from_reclaim(item, reclaim_outcome, reclaim_detail or "")
    return _cross_session_keep(item, verdict, reason)


def _cross_session_close_notes(
    lane_id: str,
    tip_sha: str,
    preserving_ref: str,
    preserving_sha: str,
    kind: str | None = None,
) -> str:
    """Resumable close note: consumed tip plus preserving identity.

    Must start with ``post-merge reap of`` so ``_tip_from_close_notes`` can
    recover the pinned SHA. Preserving identity uses ``sha``, not `` at ``,
    so the parser's rfind does not pick the wrong token. The preservation
    kind is appended so empty-lane keeps cannot be mistaken for a real
    integration.
    """
    notes = f"post-merge reap of {lane_id} at {tip_sha}; preserved-by {preserving_ref} sha {preserving_sha}"
    if kind:
        return f"{notes} via {kind}"
    return notes


def _cross_session_identity_mismatch(
    *,
    lane_id: str,
    branch: str,
    tip_sha: str | None,
    worktree_path: str,
    repo: Path,
    git: Callable[..., Any],
    batch: Sequence[object],
) -> str | None:
    """Reuse linked-worktree, shared-path, and checkout-identity guards."""
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _linked_worktree_paths,
        _resolved_path_text,
    )

    linked, registry_detail = _linked_worktree_paths(repo)
    if linked is None:
        return registry_detail or "worktree_list_failed"
    target = _resolved_path_text(worktree_path).rstrip("/")
    if not target or target not in linked:
        return "not a linked worktree"
    key, key_err = _resolved_worktree_key(worktree_path, repo)
    if key_err:
        return key_err
    if key is not None:
        sibling, sibling_err = _same_batch_live_owner(
            lane_id=lane_id,
            path_key=key,
            repo=repo,
            batch=batch,
        )
        if sibling_err:
            return sibling_err
        if sibling is not None:
            return f"shared with lane {sibling}"
    if not tip_sha or not _path_still_our_checkout(git, worktree_path, branch, tip_sha):
        return "checkout_identity_mismatch"
    return None


def _cross_session_preservation_moved(
    item: Mapping[str, Any],
    *,
    git: Callable[..., Any],
    repo: Path,
    worktree_path: str | None,
    merged_fn: Callable[..., Any],
) -> str | None:
    """Re-run the classification-selected proof at the moment of effect."""
    kind = str(item.get("preservation") or "")
    branch = str(item.get("branch") or "")
    recorded_tip = _text(item.get("tip_sha"))
    recorded_ref = _text(item.get("preserving_ref"))
    recorded_sha = _text(item.get("preserving_sha"))
    if not recorded_tip or not recorded_ref:
        return "preservation_identity_missing"
    try:
        live_tip = _probe_tip_sha(git, branch)
    except _GitError as exc:
        return exc.detail or "tip_unreadable"
    if live_tip != recorded_tip:
        return "tip_moved"
    proof_ref = _heads_ref(recorded_ref) if kind == "review_output_preserved" else recorded_ref
    live_ref_sha = _ref_sha(git, proof_ref)
    if live_ref_sha is None or live_ref_sha != recorded_sha:
        return "preserving_ref_moved"
    try:
        if kind == "merged_ancestry":
            worktree_present = bool(worktree_path) and not _worktree_is_absent(worktree_path, repo)
            if worktree_present and worktree_path:
                merged, reason = merged_fn(
                    repo_root=repo,
                    branch=branch,
                    worktree_path=worktree_path,
                    integration_ref=recorded_ref,
                )
                if merged is not True:
                    return reason or "ancestry_unproved"
            elif not _is_ancestor(git, recorded_tip, recorded_ref):
                return "ancestry_unproved"
        elif kind == "merged_content":
            unlanded = _unlanded_commit_count(git, recorded_tip, recorded_ref)
            if unlanded != 0:
                return "content_unproved"
        elif kind == "review_output_preserved":
            kept, detail = _review_output_preserved(git, recorded_tip, recorded_sha or proof_ref)
            if kept is not True:
                payload_kept, payload_detail = _review_payload_proof(
                    git, repo, recorded_tip, recorded_ref, integration_sha=recorded_sha
                )
                if payload_kept is not True:
                    return detail or payload_detail or "review_unproved"
        elif kind == "superseded_tombstone":
            verified, _integration_tip, detail, probe_failed = _probe_superseded_tombstone(
                repo=repo,
                branch=branch,
                tip=recorded_tip,
                integration_ref=recorded_ref,
                expected_integration_tip=recorded_sha,
            )
            if verified is not True:
                return detail or ("superseded_tombstone_probe_failed" if probe_failed else "tombstone_unverified")
        else:
            return f"unknown_proof:{kind}"
    except _GitError as exc:
        return exc.detail or "preservation_uncertain"
    except Exception as exc:  # noqa: BLE001 — uncertainty is preservation_moved
        return str(exc) or type(exc).__name__
    return None


def _cross_session_close_authority(
    item: dict[str, Any],
    *,
    task_ref: str,
    lane_id: str,
    branch: str,
    repo: Path,
    worktree_path: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Gate a cross-session reap on the row's close authority, before any close or Git effect.

    Returns ``(authority, kept)``. ``kept`` is a finished keep-item when the
    authority refuses or the authorized tip moved; otherwise ``authority`` is the
    recorded close authority, or None for a rowless or legacy lane. A moved
    closed row is reopened, so the next retire refuses on the tip.
    """
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _branch_ref_state,
        _durable_close_authority,
        _reopen_moved_closed_row,
    )

    row, authority, refusal = _durable_close_authority(task_ref=task_ref, lane_id=lane_id)
    if refusal:
        return (None, _cross_session_keep(item, "keep:close_authority", refusal))
    if authority is None:
        return (None, None)
    state, observed = _branch_ref_state(repo, branch)
    if state == "unknown":
        return (None, _cross_session_keep(item, "keep:probe_failed", observed))
    branch_gone = state == "absent"
    if branch_gone and (not worktree_path or _worktree_is_absent(worktree_path, repo)):
        # Branch and worktree are both gone: nothing destructive is left to guard,
        # so a recorded decision can still finish.
        return (authority, None)
    if branch_gone or observed != authority["authorized_tip"]:
        kept = _cross_session_keep(item, "keep:tip_moved_after_close", "tip_moved_after_close")
        reopened = _reopen_moved_closed_row(row) if row is not None else ""
        if reopened:
            kept["reopen"] = reopened
        return (None, kept)
    return (authority, None)


def _apply_cross_session_reclaim(item: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    """Cross-session reclaim of one item; see ``_apply_cross_session_reclaim_item``."""
    return _with_cross_session_fence_releases(_apply_cross_session_reclaim_item, item, **kwargs)


def _apply_cross_session_reclaim_item(
    item: dict[str, Any],
    *,
    row: object,
    repo: Path,
    git: Callable[..., Any],
    close_row: Callable[..., Any],
    record_decision: Callable[..., Any],
    task_ref: str,
    batch: Sequence[object],
    merged_fn: Callable[..., Any],
    integration_ref: str,
    list_rows: Callable[..., Any] | None = None,
    retire_ownership: Callable[[str], tuple[bool, str]],
) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _release_lane_worker_lock,
        _remove_lane_worktree,
        _reopen_tip_moved_at_removal,
        _try_row_reaping_claim,
        _under_claim_worktree_reclaim_guards,
    )
    from workbay_orchestrator_mcp.orchestration.branch_reclaim_delete import (  # noqa: PLC0415
        fence_release_of,
    )

    lane_id = str(item.get("lane_id") or "")
    branch = str(item.get("branch") or "")
    tip_sha = _text(item.get("tip_sha"))
    recorded_tip = _recorded_cas_tip(row)
    worktree_path = _text(_row_get(row, "worktree_path"))
    status = _text(_row_get(row, "status")) or ""
    preserving_ref = _text(item.get("preserving_ref")) or "integration"
    preserving_sha = _text(item.get("preserving_sha")) or tip_sha or "unknown"

    claim_handle, claim_detail = _try_row_reaping_claim(lane_id)
    if claim_handle is None:
        return _cross_session_keep(item, "keep:claim_failed", claim_detail or "claim_failed")
    try:
        authority, kept = _cross_session_close_authority(
            item,
            task_ref=task_ref,
            lane_id=lane_id,
            branch=branch,
            repo=repo,
            worktree_path=worktree_path,
        )
        if kept is not None:
            return kept
        if worktree_path and not _worktree_is_absent(worktree_path, repo):
            ident_err = _cross_session_identity_mismatch(
                lane_id=lane_id,
                branch=branch,
                tip_sha=tip_sha,
                worktree_path=worktree_path,
                repo=repo,
                git=git,
                batch=batch,
            )
            if ident_err:
                return _cross_session_keep(item, "keep:identity_mismatch", ident_err)

        moved = _cross_session_preservation_moved(
            item,
            git=git,
            repo=repo,
            worktree_path=worktree_path,
            merged_fn=merged_fn,
        )
        if moved:
            if not tip_sha:
                return _cross_session_keep(item, "keep:preservation_moved", moved)
            extra = f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
            notes = _cross_session_close_notes(
                lane_id,
                tip_sha,
                preserving_ref,
                preserving_sha,
                kind=_text(item.get("preservation")),
            )
            closed, close_detail = _cas_close(
                close_row,
                lane_id=lane_id,
                expected_status=status or "active",
                notes=notes,
                expected_tip_sha=recorded_tip,
            )
            if not closed:
                return _cross_session_keep(
                    item,
                    "keep:probe_failed",
                    _cas_close_failure_reason(close_detail, recorded_tip=recorded_tip, probed_tip=tip_sha),
                )
            generation = _terminal_generation_after_close(row, list_rows, task_ref, lane_id)
            acked = _persist_cross_session_closed_notes(
                close_row,
                lane_id=lane_id,
                notes=_cross_session_retry_notes(notes, extra, tip_sha, generation),
                tip_sha=tip_sha,
                expected_tip_sha=recorded_tip,
                row=row,
                task_ref=task_ref,
                list_rows=list_rows,
            )
            if not acked:
                return _cross_session_keep(
                    item,
                    "keep:probe_failed",
                    f"{_UNACKNOWLEDGED_FAILURE}: {extra}",
                )
            return _cross_session_keep(item, "keep:preservation_moved", moved)

        if not tip_sha:
            return _cross_session_keep(item, "keep:probe_failed", "tip_missing")
        # A payload-only tip keeps its branch: the payload commit is not on the integration ref.
        payload_kept, payload_detail = _review_payload_proof(
            git, repo, tip_sha, preserving_ref, integration_sha=preserving_sha
        )
        if payload_kept is None:
            return _cross_session_keep(item, "keep:probe_failed", f"preservation:probe_failed:{payload_detail}")
        notes = _cross_session_close_notes(
            lane_id,
            tip_sha,
            preserving_ref,
            preserving_sha,
            kind=_text(item.get("preservation")),
        )
        if payload_kept:
            notes = f"{notes}; {_PAYLOAD_BRANCH_KEPT}"
        pending_notes = _cross_session_retry_notes(notes, "reclaim_pending", tip_sha) if payload_kept else notes
        closed, close_detail = _cas_close(
            close_row,
            lane_id=lane_id,
            expected_status=status,
            notes=pending_notes,
            expected_tip_sha=recorded_tip,
        )
        if not closed:
            return _cross_session_keep(
                item,
                "keep:probe_failed",
                _cas_close_failure_reason(close_detail, recorded_tip=recorded_tip, probed_tip=tip_sha),
            )
        generation = _terminal_generation_after_close(row, list_rows, task_ref, lane_id)

        def persist_failure(extra: str) -> bool:
            return _persist_cross_session_closed_notes(
                close_row,
                lane_id=lane_id,
                notes=_cross_session_retry_notes(notes, extra, tip_sha, generation),
                tip_sha=tip_sha,
                expected_tip_sha=recorded_tip,
                row=row,
                task_ref=task_ref,
                list_rows=list_rows,
            )

        if payload_kept and not persist_failure("reclaim_pending"):
            return _cross_session_keep(item, "keep:probe_failed", "reclaim_intent_unacknowledged")

        try:
            retired, retire_detail = retire_ownership(lane_id)
        except Exception as exc:  # noqa: BLE001 — fail closed, persist retry notes
            retired, retire_detail = False, str(exc) or type(exc).__name__
        if not retired:
            return _keep_after_persist(
                item,
                persist_failure,
                f"manifest: {retire_detail}",
                verdict="keep:probe_failed",
                reason=f"manifest: {retire_detail}",
            )

        git_fail: str | None = None
        if worktree_path and not _worktree_is_absent(worktree_path, repo):
            moved = _cross_session_preservation_moved(
                item,
                git=git,
                repo=repo,
                worktree_path=worktree_path,
                merged_fn=merged_fn,
            )
            if moved:
                extra = f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                return _keep_after_persist(
                    item,
                    persist_failure,
                    extra,
                    verdict="keep:preservation_moved",
                    reason=moved,
                )
            guard_outcome, guard_detail = _under_claim_worktree_reclaim_guards(
                worktree_path=worktree_path,
                branch=branch,
                repo_root=repo,
                task_ref=task_ref,
                lane_id=lane_id,
            )
            if guard_outcome is not None:
                return _keep_after_persist(
                    item,
                    persist_failure,
                    f"reap_guard_refused: {guard_outcome}: {guard_detail}",
                    verdict="keep:probe_failed",
                    reason=guard_detail,
                    reclaim_outcome=guard_outcome,
                    reclaim_detail=guard_detail,
                )
            removed, remove_detail = _remove_lane_worktree(
                repo,
                worktree_path,
                expected_tip=None if authority is None else str(authority["authorized_tip"]),
                lane_id=lane_id,
                task_ref=task_ref,
            )
            _note_cross_session_fence_release("worktree_remove", fence_release_of(remove_detail))
            if not removed and not _worktree_is_absent(worktree_path, repo):
                git_fail = f"worktree: {remove_detail}"
                if remove_detail.endswith("tip_moved_after_intent"):
                    # Same reopen policy as the ordinary reclaim: a moved closed row is
                    # reopened with its blocker, not left closed behind a plain failure.
                    reopen_detail = _reopen_tip_moved_at_removal(
                        task_ref=task_ref, lane_id=lane_id, authority=authority, locked_row=row
                    )
                    if reopen_detail:
                        git_fail = f"{git_fail};{reopen_detail}"
        # Payload tips retain their branch until a dedicated retention contract is available.
        if git_fail is None and branch and not payload_kept:
            moved = _cross_session_preservation_moved(
                item,
                git=git,
                repo=repo,
                worktree_path=worktree_path,
                merged_fn=merged_fn,
            )
            if moved:
                extra = f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                return _keep_after_persist(
                    item,
                    persist_failure,
                    extra,
                    verdict="keep:preservation_moved",
                    reason=moved,
                )
            guard_outcome, guard_detail = _under_claim_worktree_reclaim_guards(
                worktree_path=None,
                branch=branch,
                repo_root=repo,
                task_ref=task_ref,
                lane_id=lane_id,
            )
            if guard_outcome is not None:
                return _keep_after_persist(
                    item,
                    persist_failure,
                    f"reap_guard_refused: {guard_outcome}: {guard_detail}",
                    verdict="keep:probe_failed",
                    reason=guard_detail,
                    reclaim_outcome=guard_outcome,
                    reclaim_detail=guard_detail,
                )
            deleted, branch_detail = _delete_authorized_preserved_branch(
                repo=repo,
                task_ref=task_ref,
                lane_id=lane_id,
                branch=branch,
                tip_sha=tip_sha,
                integration_ref=integration_ref,
            )
            if not deleted:
                git_fail = f"branch: {branch_detail}"
        if git_fail:
            return _keep_after_persist(
                item,
                persist_failure,
                f"reap_git_failed: {git_fail}",
                verdict="keep:probe_failed",
                reason=git_fail,
            )

        recorded, rec_detail = _record(
            record_decision,
            decision_id=f"postmerge_reap:{task_ref}:{lane_id}:{tip_sha}",
            rationale=notes,
            task_ref=task_ref,
        )
        if not recorded:
            restored = _restore_consumed_branch(git, branch, tip_sha)
            extra = f"record_failed: {rec_detail}"
            if not restored:
                extra = f"{extra}; restore_failed"
            return _keep_after_persist(
                item,
                persist_failure,
                extra,
                verdict="keep:probe_failed",
                reason=f"decision: {rec_detail}",
            )
        if payload_kept and not _ack_payload_reclaim(
            close_row,
            list_rows=list_rows,
            task_ref=task_ref,
            lane_id=lane_id,
            tip_sha=tip_sha,
            expected_tip_sha=recorded_tip,
        ):
            return _keep_after_persist(
                item,
                persist_failure,
                _COMPLETION_UNACKNOWLEDGED,
                verdict="keep:probe_failed",
                reason=_COMPLETION_UNACKNOWLEDGED,
            )

        applied = dict(item)
        applied["reason"] = notes
        applied["applied"] = True
        return applied
    finally:
        _release_lane_worker_lock(claim_handle)


def _resume_cross_session_terminal_retry(item: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    """Terminal retry of one cross-session item; see ``_resume_cross_session_terminal_retry_item``."""
    return _with_cross_session_fence_releases(_resume_cross_session_terminal_retry_item, item, **kwargs)


def _resume_cross_session_terminal_retry_item(
    item: dict[str, Any],
    *,
    row: object,
    repo: Path,
    git: Callable[..., Any],
    close_row: Callable[..., Any],
    record_decision: Callable[..., Any],
    task_ref: str,
    batch: Sequence[object],
    merged_fn: Callable[..., Any],
    refs: Sequence[str],
    integration_ref: str,
    list_rows: Callable[..., Any] | None = None,
    retire_ownership: Callable[[str], tuple[bool, str]],
    orchestrator_root: Path | str | None = None,
) -> dict[str, Any]:
    """Finish pinned leftover effects for a marker-bearing terminal row.

    Re-runs shared-path guards and preservation proof only for surviving
    artifacts. Missing completion is a typed durable failure, never backlog.
    A retry marker is bound to the terminal row generation; a reopened or
    re-registered row does not authorize the old branch delete.
    """
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _release_lane_worker_lock,
        _remove_lane_worktree,
        _reopen_tip_moved_at_removal,
        _try_row_reaping_claim,
        _under_claim_worktree_reclaim_guards,
    )
    from workbay_orchestrator_mcp.orchestration.branch_reclaim_delete import (  # noqa: PLC0415
        fence_release_of,
    )

    listed = row
    lane_id = str(item.get("lane_id") or "")
    branch = str(item.get("branch") or "")
    listed_notes = _text(_row_get(listed, "notes")) or ""
    tip_sha = _tip_from_close_notes(listed_notes) or _text(item.get("tip_sha"))
    preserving_ref = _text(item.get("preserving_ref"))
    preserving_sha = _text(item.get("preserving_sha"))
    parsed_ref, parsed_sha = _preserving_identity_from_notes(listed_notes)
    preserving_ref = preserving_ref or parsed_ref or "integration"
    preserving_sha = preserving_sha or parsed_sha or tip_sha or "unknown"
    retry_item = dict(item)
    retry_item["tip_sha"] = tip_sha
    retry_item["preserving_ref"] = preserving_ref
    retry_item["preserving_sha"] = preserving_sha

    claim_handle, claim_detail = _try_row_reaping_claim(lane_id)
    if claim_handle is None:
        return _cross_session_keep(item, "keep:claim_failed", claim_detail or "claim_failed")
    try:
        # Before any notes-derived tip or Git effect: a retry marker never
        # authorizes a moved tip, and the row's close authority decides.
        authority, kept = _cross_session_close_authority(
            item,
            task_ref=task_ref,
            lane_id=lane_id,
            branch=branch,
            repo=repo,
            worktree_path=_text(_row_get(listed, "worktree_path")) or "",
        )
        if kept is not None:
            return kept
        fresh = _fresh_lane_row(list_rows, task_ref, lane_id)
        refused = _refuse_unauthoritative_retry_row(
            listed=listed,
            fresh=fresh,
            item=item,
            tip_sha=tip_sha,
        )
        if refused is not None:
            verdict, reason = refused
            if reason == _RETRY_MARKER_ABSENT and fresh is not None:
                return _settle_markerless_terminal_kept(
                    item,
                    listed=listed,
                    fresh=fresh,
                    git=git,
                    repo=repo,
                    task_ref=task_ref,
                    lane_id=lane_id,
                    preserving_ref=preserving_ref,
                    preserving_sha=preserving_sha,
                )
            return _cross_session_keep(item, verdict, reason)
        assert fresh is not None
        row = fresh
        notes = _text(_row_get(row, "notes")) or ""
        tip_sha = _tip_from_close_notes(notes) or tip_sha
        recorded_tip = _recorded_cas_tip(row)
        worktree_path = _text(_row_get(row, "worktree_path"))
        status = _text(_row_get(row, "status")) or "closed"
        generation = _row_generation(row)
        nonce = _nonce_from_notes(notes) or ""
        if not tip_sha:
            return _cross_session_keep(item, "keep:probe_failed", "retry_tip_missing")
        retry_item["tip_sha"] = tip_sha

        def persist_failure(extra: str) -> bool:
            return _persist_cross_session_closed_notes(
                close_row,
                lane_id=lane_id,
                notes=_cross_session_retry_notes(
                    _cross_session_close_notes(
                        lane_id,
                        tip_sha,
                        preserving_ref,
                        preserving_sha,
                        kind=_text(item.get("preservation")),
                    ),
                    f"{extra}; {_PAYLOAD_BRANCH_KEPT}" if _PAYLOAD_BRANCH_KEPT in notes else extra,
                    tip_sha,
                    generation,
                    nonce,
                ),
                tip_sha=tip_sha,
                expected_tip_sha=recorded_tip,
                row=row,
                task_ref=task_ref,
                list_rows=list_rows,
            )

        # A payload-only tip keeps its branch: the payload commit is not on the integration ref.
        payload_kept, payload_detail = _review_payload_proof(
            git, repo, tip_sha, preserving_ref, integration_sha=preserving_sha
        )
        if payload_kept is None:
            extra = f"preservation:probe_failed:{payload_detail}"
            return _keep_after_persist(
                item,
                persist_failure,
                extra,
                verdict="keep:probe_failed",
                reason=extra,
            )

        try:
            retired, retire_detail = retire_ownership(lane_id)
        except Exception as exc:  # noqa: BLE001 — fail closed, persist retry notes
            retired, retire_detail = False, str(exc) or type(exc).__name__
        if not retired:
            return _keep_after_persist(
                item,
                persist_failure,
                f"manifest: {retire_detail}",
                verdict="keep:probe_failed",
                reason=f"manifest: {retire_detail}",
            )

        def fence_effect(effect: str) -> dict[str, Any] | None:
            closed, _detail = _cas_retry_effect(
                close_row,
                lane_id=lane_id,
                expected_status=status,
                notes=notes,
                tip_sha=tip_sha,
                expected_tip_sha=recorded_tip,
                effect=effect,
            )
            if closed:
                return None
            return _cross_session_keep(item, "keep:identity_mismatch", "retry_effect_cas_miss")

        surviving_tree = bool(worktree_path) and not _worktree_is_absent(worktree_path, repo)
        surviving_branch = False
        if branch:
            try:
                live_tip = _probe_tip_sha(git, branch)
            except _GitError as exc:
                return _keep_after_persist(
                    item,
                    persist_failure,
                    f"retry_tip_unreadable: {exc.detail}",
                    verdict="keep:probe_failed",
                    reason=exc.detail,
                )
            surviving_branch = live_tip == tip_sha

        if surviving_tree or surviving_branch:
            if surviving_tree and worktree_path:
                ident_err = _cross_session_identity_mismatch(
                    lane_id=lane_id,
                    branch=branch,
                    tip_sha=tip_sha,
                    worktree_path=worktree_path,
                    repo=repo,
                    git=git,
                    batch=batch,
                )
                if ident_err:
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        f"retry_identity: {ident_err}",
                        verdict="keep:identity_mismatch",
                        reason=ident_err,
                    )
            moved = _cross_session_preservation_moved(
                retry_item,
                git=git,
                repo=repo,
                worktree_path=worktree_path,
                merged_fn=merged_fn,
            )
            if moved:
                kind = str(retry_item.get("preservation") or "")
                if kind not in _PRESERVED_KINDS:
                    lane_kind = (_text(_row_get(row, "lane_kind")) or "").lower()
                    base_sha, base_error = _try_authoritative_lane_base_sha(
                        row=row,
                        task_ref=task_ref,
                        repo=repo,
                        orchestrator_root=orchestrator_root,
                    )
                    if base_error:
                        extra = f"preservation:probe_failed:{base_error}"
                        return _keep_after_persist(
                            item,
                            persist_failure,
                            extra,
                            verdict="keep:probe_failed",
                            reason=f"preservation:probe_failed:{base_error}",
                        )
                    preservation, pref, psha, _live, pres_failed, empty_reason = _probe_preservation(
                        repo=repo,
                        git=git,
                        branch=branch,
                        worktree_path=worktree_path,
                        refs=refs,
                        lane_kind=lane_kind,
                        merged_fn=merged_fn,
                        base_sha=base_sha,
                        created_at=_row_get(row, "created_at"),
                    )
                    if preservation == _EMPTY_LANE_KIND:
                        extra = f"empty_lane tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                        return _keep_after_persist(
                            item,
                            persist_failure,
                            extra,
                            verdict="keep:empty_lane",
                            reason=empty_reason or moved,
                        )
                    if pres_failed or preservation not in _PRESERVED_KINDS:
                        extra = f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                        return _keep_after_persist(
                            item,
                            persist_failure,
                            extra,
                            verdict="keep:preservation_moved",
                            reason=moved,
                        )
                    retry_item["preservation"] = preservation
                    retry_item["preserving_ref"] = pref or preserving_ref
                    retry_item["preserving_sha"] = psha or preserving_sha
                    moved = _cross_session_preservation_moved(
                        retry_item,
                        git=git,
                        repo=repo,
                        worktree_path=worktree_path,
                        merged_fn=merged_fn,
                    )
                if moved:
                    extra = f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        extra,
                        verdict="keep:preservation_moved",
                        reason=moved,
                    )
            if surviving_tree and worktree_path:
                guard_outcome, guard_detail = _under_claim_worktree_reclaim_guards(
                    worktree_path=worktree_path,
                    branch=branch,
                    repo_root=repo,
                    task_ref=task_ref,
                    lane_id=lane_id,
                )
                if guard_outcome is not None:
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        f"reap_guard_refused: {guard_outcome}: {guard_detail}",
                        verdict="keep:probe_failed",
                        reason=guard_detail,
                        reclaim_outcome=guard_outcome,
                        reclaim_detail=guard_detail,
                    )
                missed = fence_effect("worktree_remove")
                if missed is not None:
                    return missed
                removed, remove_detail = _remove_lane_worktree(
                    repo,
                    worktree_path,
                    expected_tip=None if authority is None else str(authority["authorized_tip"]),
                    lane_id=lane_id,
                    task_ref=task_ref,
                )
                _note_cross_session_fence_release("worktree_remove", fence_release_of(remove_detail))
                if not removed and not _worktree_is_absent(worktree_path, repo):
                    kept_reason = f"worktree: {remove_detail}"
                    if remove_detail.endswith("tip_moved_after_intent"):
                        # Same reopen policy as the ordinary reclaim; see the cross-session arm.
                        reopen_detail = _reopen_tip_moved_at_removal(
                            task_ref=task_ref, lane_id=lane_id, authority=authority, locked_row=row
                        )
                        if reopen_detail:
                            kept_reason = f"{kept_reason};{reopen_detail}"
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        kept_reason,
                        verdict="keep:probe_failed",
                        reason=kept_reason,
                    )
            if surviving_branch and not payload_kept:
                guard_outcome, guard_detail = _under_claim_worktree_reclaim_guards(
                    worktree_path=None,
                    branch=branch,
                    repo_root=repo,
                    task_ref=task_ref,
                    lane_id=lane_id,
                )
                if guard_outcome is not None:
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        f"reap_guard_refused: {guard_outcome}: {guard_detail}",
                        verdict="keep:probe_failed",
                        reason=guard_detail,
                        reclaim_outcome=guard_outcome,
                        reclaim_detail=guard_detail,
                    )
                missed = fence_effect("delete_authorized_branch")
                if missed is not None:
                    return missed
                deleted, branch_detail = _delete_authorized_preserved_branch(
                    repo=repo,
                    task_ref=task_ref,
                    lane_id=lane_id,
                    branch=branch,
                    tip_sha=tip_sha,
                    integration_ref=integration_ref,
                )
                if not deleted:
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        f"branch: {branch_detail}",
                        verdict="keep:probe_failed",
                        reason=f"branch: {branch_detail}",
                    )

        missed = fence_effect("record_decision")
        if missed is not None:
            return missed
        recorded, rec_detail = _record(
            record_decision,
            decision_id=f"postmerge_reap:{task_ref}:{lane_id}:{tip_sha}",
            rationale=notes
            or _cross_session_close_notes(
                lane_id,
                tip_sha,
                preserving_ref,
                preserving_sha,
                kind=_text(item.get("preservation")),
            ),
            task_ref=task_ref,
        )
        if not recorded:
            restored = _restore_consumed_branch(git, branch, tip_sha)
            extra = f"record_failed: {rec_detail}"
            if not restored:
                extra = f"{extra}; restore_failed"
            return _keep_after_persist(
                item,
                persist_failure,
                extra,
                verdict="keep:probe_failed",
                reason=f"decision: {rec_detail}",
            )
        if payload_kept and not _ack_payload_reclaim(
            close_row,
            list_rows=list_rows,
            task_ref=task_ref,
            lane_id=lane_id,
            tip_sha=tip_sha,
            expected_tip_sha=recorded_tip,
        ):
            return _keep_after_persist(
                item,
                persist_failure,
                _COMPLETION_UNACKNOWLEDGED,
                verdict="keep:probe_failed",
                reason=_COMPLETION_UNACKNOWLEDGED,
            )

        applied = dict(item)
        applied["reason"] = _strip_reap_guard_refused(
            f"terminal retry reaped via {item.get('preservation') or 'unintegrated'}"
        )
        applied["applied"] = True
        applied["verdict"] = "reclaim"
        return applied
    finally:
        _release_lane_worker_lock(claim_handle)
