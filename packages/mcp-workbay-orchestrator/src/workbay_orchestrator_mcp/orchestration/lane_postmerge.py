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
status and consumed tip, then removes the worktree without ``--force``,
then deletes the branch with ``git branch -d`` (git itself refuses unmerged
tips), then records an idempotent decision. Failures never count as merged.

If a git step fails after that CAS close, a compensating CAS reopens the
row to its pre-close status with a ``reap_git_failed`` note so the next
apply retries git instead of treating the row as already reaped.
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

import json
import secrets
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
_PRESERVED_KINDS = frozenset({"merged_ancestry", "merged_content", "review_output_preserved"})
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
    git = run_git if run_git is not None else _default_run_git(repo)
    rows_fn = list_rows if list_rows is not None else _default_list_rows()
    closer = close_row if close_row is not None else _default_close_row(task_ref)
    recorder = record_decision if record_decision is not None else _default_record_decision(task_ref)
    path_fn = list_by_path if list_by_path is not None else _default_list_by_path()
    reopener = reopen_row if reopen_row is not None else _default_reopen_row(task_ref)

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
                )
            )
        except _GitError as exc:
            lane_id = _text(_row_get(row, "lane_id")) or ""
            branch = _text(_row_get(row, "branch")) or ""
            verdicts.append(
                _verdict(lane_id, branch, None, KIND_PROBE_FAILED, exc.detail, applied=False)
            )
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
    )
    if not recorded:
        _restore_consumed_branch(git, branch, tip_sha)
        return _verdict(lane_id, branch, tip_sha, KIND_PROBE_FAILED, f"decision: {rec_detail}", applied=False)

    kind = KIND_MERGED_WORKTREE_ABSENT if absent else KIND_MERGED_REAPED
    return _verdict(lane_id, branch, tip_sha, kind, "reaped", applied=True)


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
    """
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


def _record(record_decision: Callable[..., Any], *, decision_id: str, rationale: str) -> tuple[bool, str]:
    try:
        result = record_decision(id=decision_id, rationale=rationale)
    except TypeError:
        try:
            result = record_decision(decision_id, rationale)
        except Exception as exc:  # noqa: BLE001 — decision write is fail-closed
            return False, str(exc) or type(exc).__name__
    except Exception as exc:  # noqa: BLE001 — decision write is fail-closed
        return False, str(exc) or type(exc).__name__
    return _interpret_cas_result(result)


def _default_run_git(repo: Path) -> Callable[[Sequence[str]], subprocess.CompletedProcess[str]]:
    def run_git(args: Sequence[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(repo), *list(args)],
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_TIMEOUT_S,
        )

    return run_git


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


_RETRY_IDENTITY_MARKER = "reap_retry_identity:"
_RETRY_GEN_MARKER = "reap_gen:"
_RETRY_NONCE_MARKER = "reap_nonce:"
_RETRY_EFFECT_MARKER = "reap_effect:"
_UNACKNOWLEDGED_FAILURE = "unacknowledged_failure"


def _tip_from_close_notes(notes: str | None) -> str | None:
    """Recover the reaped tip from notes written in the same CAS close.

    Used to retry a missing ``postmerge_reap`` decision after the branch is
    already gone. Notes that were not written by this reaper return None.
    Only the SHA token after `` at `` is returned so a resumable suffix
    (preserving identity, retry marker) cannot pollute the pin.
    """
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
    if not notes or _RETRY_NONCE_MARKER not in notes:
        return None
    rest = notes[notes.find(_RETRY_NONCE_MARKER) + len(_RETRY_NONCE_MARKER) :]
    token = rest.split()[0].split(";")[0].strip().rstrip(",") if rest else ""
    return token or None


def _preserving_identity_from_notes(notes: str | None) -> tuple[str | None, str | None]:
    """Recover preserving ref and SHA from resumable close notes."""
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
    branch: str,
    tip_sha: str,
    integration_branch: str,
    record_decision: Callable[..., Any],
    decision_id: str,
    rationale: str,
    lane_id: str,
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
    recorded, rec_detail = _record(
        record_decision, decision_id=decision_id, rationale=rationale
    )
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
    return _verdict(
        lane_id,
        branch,
        tip_sha,
        KIND_RETRY_REAPED,
        "retry_reaped",
        applied=False,
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
            branch=branch,
            tip_sha=tip_sha,
            integration_branch=integration_branch,
            record_decision=record_decision,
            decision_id=decision_id,
            rationale=rationale,
            lane_id=lane_id,
        )

    if worktree_path and not _path_still_our_checkout(git, worktree_path, branch, tip_sha):
        # Occupied by a different checkout. Finish the leftover branch
        # only; never ``git worktree remove`` the occupier.
        return _retry_finish_and_record(
            git=git,
            branch=branch,
            tip_sha=tip_sha,
            integration_branch=integration_branch,
            record_decision=record_decision,
            decision_id=decision_id,
            rationale=rationale,
            lane_id=lane_id,
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
        branch=branch,
        tip_sha=tip_sha,
        integration_branch=integration_branch,
        record_decision=record_decision,
        decision_id=decision_id,
        rationale=rationale,
        lane_id=lane_id,
    )


def _decision_rationale(lane_id: str, branch: str, tip_sha: str) -> str:
    return f"reaped merged lane {lane_id} branch {branch} at {tip_sha}"


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
) -> dict[str, Any]:
    """Reap preserved, clean, unowned lanes of an in-progress task (RES-07).

    Preservation, ownership, and cleanliness are independent probes. A lane is
    reclaimed only when all three agree: the tip is preserved on an integration
    ref, the worktree is porcelain-clean, and nothing live owns it. Unintegrated
    unowned lanes older than ``age_floor_seconds`` are counted as backlog, never
    deleted. Any probe failure is ``keep:probe_failed`` (CARD-07 / OBS-08).
    Apply-time ``guard_refusals`` fold into ``ok=False`` (REAP06RV-001); the
    report does not grow a parallel non-success field.
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
    owner_fn = probe_owner if probe_owner is not None else _default_probe_owner
    lease_fn = probe_lease if probe_lease is not None else _default_probe_lease
    clean_fn = probe_clean if probe_clean is not None else _default_probe_clean
    merged_fn = probe_merged if probe_merged is not None else _default_probe_merged

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
        )
        verdicts.append(item)
        if item["verdict"] == "keep:probe_failed":
            probe_failures += 1
        if item["verdict"] == "reclaim":
            would_reclaim.append(item)
            if apply:
                if limit and applied_reclaims >= limit:
                    truncated = True
                    item["reason"] = "reclaim deferred: max_batch"
                    continue
                if _terminal_needs_retry(row, repo, git):
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
                        list_rows=rows_fn,
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
                        list_rows=rows_fn,
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

    return {
        # Guard refusals are a typed non-success: fold them into ok rather
        # than adding a parallel report field (REAP06RV-001).
        "ok": probe_failures == 0 and durable_failures == 0 and guard_refusals == 0,
        "applied": apply,
        "rows_scanned": len(rows),
        "probe_failures": probe_failures,
        "guard_refusals": guard_refusals,
        "preservation_moved": preservation_moved,
        "would_reclaim": would_reclaim,
        "reclaimed": reclaimed,
        "backlog": backlog,
        "kept": kept,
        "verdicts": verdicts,
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
        "preservation_moved": 0,
        "would_reclaim": [],
        "reclaimed": [],
        "backlog": [],
        "kept": [],
        "verdicts": [],
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
    """Resolve integration refs. ``['main']`` only for a missing manifest or field.

    An existing manifest that cannot be read, parsed, or validated raises
    ``ManifestResolutionError`` rather than failing open to ``main``.
    A present ``integration_branch`` must be a non-empty valid branch name;
    only a missing key keeps the ``['main']`` default.
    """
    if integration_refs is not None:
        refs = [str(ref).strip() for ref in integration_refs if str(ref).strip()]
        return refs or ["main"]
    refs = ["main"]
    declared = _read_manifest_integration_branch(task_ref, repo=repo, orchestrator_root=orchestrator_root)
    if declared and declared not in refs:
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
            raise ManifestResolutionError(
                f"task manifest {path} is a symlink whose target is not a regular file"
            )
        return True
    if stat.S_ISDIR(st.st_mode):
        raise ManifestResolutionError(f"task manifest {path} is a directory, not a regular file")
    if not stat.S_ISREG(st.st_mode):
        raise ManifestResolutionError(f"task manifest {path} is not a regular file")
    return True


def _is_valid_integration_branch_name(name: str) -> bool:
    """Strict local branch-name check (no git; this reader is hermetic).

    Rejects empty names, whitespace, ``..``, a leading ``-``, and control
    characters. The reaper ``_default_run_git`` seam is repo-scoped and
    outside this reader, so ``git check-ref-format`` is not used here.
    """
    if not name or name.startswith("-") or ".." in name:
        return False
    for ch in name:
        if ch.isspace() or ord(ch) < 32 or ch == "\x7f":
            return False
    return True


def _declared_integration_branch(raw: Mapping[object, object], *, path: Path) -> str | None:
    if "integration_branch" not in raw:
        return None
    value = raw["integration_branch"]
    if not isinstance(value, str):
        raise ManifestResolutionError(
            f"task manifest {path} field integration_branch has invalid type "
            f"{type(value).__name__} (expected non-empty string)"
        )
    if not _is_valid_integration_branch_name(value):
        raise ManifestResolutionError(
            f"task manifest {path} field integration_branch has invalid type "
            f"{type(value).__name__} (not a valid branch name: {value!r})"
        )
    return value


def _read_manifest_integration_branch(
    task_ref: str,
    *,
    repo: Path,
    orchestrator_root: Path | str | None,
) -> str | None:
    """Read ``integration_branch`` from the task manifest. Never guess names.

    A missing file or missing field returns ``None`` (caller keeps ``['main']``).
    An existing path that cannot be read, parsed, or validated — including a
    present non-string or invalid ``integration_branch``, a directory, or a
    dangling symlink — raises ``ManifestResolutionError``.

    Policy: follow a symlink whose target is a regular JSON file.
    """
    bases: list[Path] = []
    if orchestrator_root is not None:
        bases.append(Path(orchestrator_root))
    bases.append(repo)
    seen: set[str] = set()
    for base in bases:
        path = base / "config" / "lane-orchestration" / f"{task_ref}.json"
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if not _manifest_path_is_present_regular(path):
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeError) as exc:
            raise ManifestResolutionError(f"task manifest {path} could not be resolved: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise ManifestResolutionError(f"task manifest {path} is not a mapping: {type(raw).__name__}")
        return _declared_integration_branch(raw, path=path)
    return None


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

    if _terminal_needs_retry(row, repo, git):
        notes = _text(_row_get(row, "notes"))
        tip_sha = _tip_from_close_notes(notes)
        preserving_ref, preserving_sha = _preserving_identity_from_notes(notes)
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="reclaim",
            preservation="unintegrated",
            ownership="unowned",
            cleanliness="clean",
            reason="terminal retry of pinned leftover effects",
            tip_sha=tip_sha,
            preserving_ref=preserving_ref,
            preserving_sha=preserving_sha,
            age_seconds=age_seconds,
        )
    if status in TERMINAL_STATUSES:
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="keep:already_terminal",
            preservation="unintegrated",
            ownership="unowned",
            cleanliness="clean",
            reason="terminal row has no leftover retry identity",
            age_seconds=age_seconds,
        )

    preservation, preserving_ref, preserving_sha, tip_sha, pres_failed = _probe_preservation(
        repo=repo,
        git=git,
        branch=branch,
        worktree_path=worktree_path,
        refs=refs,
        lane_kind=lane_kind,
        merged_fn=merged_fn,
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
            preservation=preservation if preservation in _PRESERVED_KINDS or preservation == "unintegrated" else "unintegrated",
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
    if preservation in _PRESERVED_KINDS and ownership == "unowned" and cleanliness == "clean":
        ref_label = preserving_ref or "integration"
        sha_label = preserving_sha or tip_sha or "unknown"
        return _cross_session_item(
            lane_id=lane_id,
            branch=branch,
            verdict="reclaim",
            preservation=preservation,
            ownership=ownership,
            cleanliness=cleanliness,
            reason=f"preserved by {ref_label} at {sha_label} via {preservation}",
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


def _probe_preservation(
    *,
    repo: Path,
    git: Callable[..., Any],
    branch: str,
    worktree_path: str | None,
    refs: Sequence[str],
    lane_kind: str,
    merged_fn: Callable[..., Any],
) -> tuple[str, str | None, str | None, str | None, bool]:
    """Return ``(kind, preserving_ref, preserving_sha, tip_sha, probe_failed)``."""
    try:
        tip_sha = _probe_tip_sha(git, branch)
    except _GitError as exc:
        return (f"probe_failed:{exc.detail}", None, None, None, True)
    if tip_sha is None:
        return ("unintegrated", None, None, None, False)

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
                return ("merged_ancestry", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False)
            if merged is None:
                ancestry_failures.append(reason or "merged_probe_failed")
                continue
            ancestry_disproved = True
            continue
        try:
            if _is_ancestor(git, tip_sha, ref):
                return ("merged_ancestry", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False)
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
            return ("merged_content", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False)

    if lane_kind == "review":
        review_failures: list[str] = []
        review_disproved = False
        for ref in refs:
            kept, detail = _review_output_preserved(git, tip_sha, ref)
            if kept is True:
                return ("review_output_preserved", ref, _ref_sha(git, ref) or tip_sha, tip_sha, False)
            if kept is None:
                review_failures.append(detail)
                continue
            review_disproved = True
        if not review_disproved and review_failures:
            return (f"probe_failed:{review_failures[0]}", None, None, tip_sha, True)

    if content_failures:
        return (f"probe_failed:{content_failures[0]}", None, None, tip_sha, True)
    if not ancestry_disproved and ancestry_failures:
        return (f"probe_failed:{ancestry_failures[0]}", None, None, tip_sha, True)
    return ("unintegrated", None, None, tip_sha, False)


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
    paths = [
        line.strip()
        for line in (proc.stdout or "").splitlines()
        if line.strip().startswith("docs/reviews/")
    ]
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


def _delete_preserved_branch(
    git: Callable[..., Any],
    branch: str,
    tip_sha: str,
) -> tuple[bool, str]:
    """Identity-guarded delete: ``update-ref -d <ref> <tip>`` refuses a moved tip."""
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError as exc:
        return False, exc.detail
    if current is None:
        return True, "branch missing"
    if current != tip_sha:
        return False, "ref_moved"
    ref = _heads_ref(branch)
    try:
        proc = _git(git, ["update-ref", "-d", ref, tip_sha])
    except _GitError as exc:
        return False, exc.detail
    if proc.returncode == 0:
        return True, ""
    detail = _proc_detail(proc) or "git update-ref -d failed"
    lowered = detail.lower()
    if _looks_like_missing_ref(detail) or "not found" in lowered:
        return True, detail
    return False, f"branch: {detail}"


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
) -> bool:
    """Write failure-state notes and verify them through a fresh row read.

    A discarded CAS result looks like success while the retry marker is
    missing. Retry once, then fall back to the existing agent-error spool.
    The closer's boolean is never acknowledgement by itself.
    """
    for _attempt in range(2):
        closed, _detail = _cas_close(
            close_row,
            lane_id=lane_id,
            expected_status=expected_status,
            notes=notes,
            expected_tip_sha=tip_sha,
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


def _cross_session_retry_notes(
    notes: str,
    extra: str,
    tip_sha: str,
    generation: str = "",
    nonce: str = "",
) -> str:
    suffix = extra.strip()
    token = nonce or secrets.token_hex(8)
    marker = f"{_RETRY_IDENTITY_MARKER}{tip_sha} {_RETRY_NONCE_MARKER}{token}"
    if generation:
        marker = f"{marker} {_RETRY_GEN_MARKER}{generation}"
    if suffix:
        return f"{notes}; {suffix}; {marker}"
    return f"{notes}; {marker}"


def _retry_paths_match(left: str | None, right: str | None) -> bool:
    listed = _text(left)
    fresh = _text(right)
    if not listed and not fresh:
        return True
    if not listed or not fresh:
        return False
    return listed.rstrip("/") == fresh.rstrip("/")


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
    listed_lane = _text(_row_get(listed, "lane_id")) or _text(item.get("lane_id"))
    fresh_lane = _text(_row_get(fresh, "lane_id"))
    listed_task = _text(_row_get(listed, "task_ref"))
    fresh_task = _text(_row_get(fresh, "task_ref"))
    listed_branch = _text(_row_get(listed, "branch")) or _text(item.get("branch"))
    fresh_branch = _text(_row_get(fresh, "branch"))
    listed_path = _text(_row_get(listed, "worktree_path"))
    fresh_path = _text(_row_get(fresh, "worktree_path"))
    if (
        fresh_lane != listed_lane
        or (listed_task and fresh_task != listed_task)
        or fresh_branch != listed_branch
        or (listed_path and not _retry_paths_match(listed_path, fresh_path))
    ):
        return ("keep:identity_mismatch", "retry_row_changed_under_claim")
    status = _text(_row_get(fresh, "status")) or ""
    if status not in TERMINAL_STATUSES:
        return ("keep:identity_mismatch", "retry_row_changed_under_claim")
    fresh_notes = _text(_row_get(fresh, "notes")) or ""
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


def _cas_retry_effect(
    close_row: Callable[..., Any],
    *,
    lane_id: str,
    expected_status: str,
    notes: str,
    tip_sha: str | None,
    effect: str,
) -> tuple[bool, str]:
    """CAS-fence one retry actuator on the terminal row (REAP08RV-001)."""
    fenced = notes
    marker = f"{_RETRY_EFFECT_MARKER}{effect}"
    if marker not in notes:
        fenced = f"{notes}; {marker}"
    return _cas_close(
        close_row,
        lane_id=lane_id,
        expected_status=expected_status,
        notes=fenced,
        expected_tip_sha=tip_sha,
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
) -> str:
    """Resumable close note: consumed tip plus preserving identity.

    Must start with ``post-merge reap of`` so ``_tip_from_close_notes`` can
    recover the pinned SHA. Preserving identity uses ``sha``, not `` at ``,
    so the parser's rfind does not pick the wrong token.
    """
    return (
        f"post-merge reap of {lane_id} at {tip_sha}; "
        f"preserved-by {preserving_ref} sha {preserving_sha}"
    )


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
    live_ref_sha = _ref_sha(git, recorded_ref)
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
            kept, detail = _review_output_preserved(git, recorded_tip, recorded_ref)
            if kept is not True:
                return detail or "review_unproved"
        else:
            return f"unknown_proof:{kind}"
    except _GitError as exc:
        return exc.detail or "preservation_uncertain"
    except Exception as exc:  # noqa: BLE001 — uncertainty is preservation_moved
        return str(exc) or type(exc).__name__
    return None


def _apply_cross_session_reclaim(
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
    list_rows: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        _release_lane_worker_lock,
        _remove_lane_worktree,
        _try_row_reaping_claim,
        _under_claim_worktree_reclaim_guards,
    )

    lane_id = str(item.get("lane_id") or "")
    branch = str(item.get("branch") or "")
    tip_sha = _text(item.get("tip_sha"))
    worktree_path = _text(_row_get(row, "worktree_path"))
    status = _text(_row_get(row, "status")) or ""
    preserving_ref = _text(item.get("preserving_ref")) or "integration"
    preserving_sha = _text(item.get("preserving_sha")) or tip_sha or "unknown"

    claim_handle, claim_detail = _try_row_reaping_claim(lane_id)
    if claim_handle is None:
        return _cross_session_keep(item, "keep:claim_failed", claim_detail or "claim_failed")
    try:
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
            notes = _cross_session_close_notes(lane_id, tip_sha, preserving_ref, preserving_sha)
            closed, close_detail = _cas_close(
                close_row,
                lane_id=lane_id,
                expected_status=status or "active",
                notes=notes,
                expected_tip_sha=tip_sha,
            )
            if not closed:
                return _cross_session_keep(item, "keep:probe_failed", f"close_row: {close_detail}")
            generation = _terminal_generation_after_close(row, list_rows, task_ref, lane_id)
            acked = _persist_cross_session_closed_notes(
                close_row,
                lane_id=lane_id,
                notes=_cross_session_retry_notes(notes, extra, tip_sha, generation),
                tip_sha=tip_sha,
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
        notes = _cross_session_close_notes(lane_id, tip_sha, preserving_ref, preserving_sha)
        closed, close_detail = _cas_close(
            close_row,
            lane_id=lane_id,
            expected_status=status,
            notes=notes,
            expected_tip_sha=tip_sha,
        )
        if not closed:
            return _cross_session_keep(item, "keep:probe_failed", f"close_row: {close_detail}")
        generation = _terminal_generation_after_close(row, list_rows, task_ref, lane_id)

        def persist_failure(extra: str) -> bool:
            return _persist_cross_session_closed_notes(
                close_row,
                lane_id=lane_id,
                notes=_cross_session_retry_notes(notes, extra, tip_sha, generation),
                tip_sha=tip_sha,
                row=row,
                task_ref=task_ref,
                list_rows=list_rows,
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
            removed, remove_detail = _remove_lane_worktree(repo, worktree_path)
            if not removed and not _worktree_is_absent(worktree_path, repo):
                git_fail = f"worktree: {remove_detail}"
        if git_fail is None and branch:
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
            deleted, branch_detail = _delete_preserved_branch(git, branch, tip_sha)
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

        applied = dict(item)
        applied["reason"] = notes
        applied["applied"] = True
        return applied
    finally:
        _release_lane_worker_lock(claim_handle)


def _resume_cross_session_terminal_retry(
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
    list_rows: Callable[..., Any] | None = None,
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
        _try_row_reaping_claim,
        _under_claim_worktree_reclaim_guards,
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
        fresh = _fresh_lane_row(list_rows, task_ref, lane_id)
        refused = _refuse_unauthoritative_retry_row(
            listed=listed,
            fresh=fresh,
            item=item,
            tip_sha=tip_sha,
        )
        if refused is not None:
            verdict, reason = refused
            return _cross_session_keep(item, verdict, reason)
        assert fresh is not None
        row = fresh
        notes = _text(_row_get(row, "notes")) or ""
        tip_sha = _tip_from_close_notes(notes) or tip_sha
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
                    _cross_session_close_notes(lane_id, tip_sha, preserving_ref, preserving_sha),
                    extra,
                    tip_sha,
                    generation,
                    nonce,
                ),
                tip_sha=tip_sha,
                row=row,
                task_ref=task_ref,
                list_rows=list_rows,
            )

        def fence_effect(effect: str) -> dict[str, Any] | None:
            closed, _detail = _cas_retry_effect(
                close_row,
                lane_id=lane_id,
                expected_status=status,
                notes=notes,
                tip_sha=tip_sha,
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
                    preservation, pref, psha, _live, pres_failed = _probe_preservation(
                        repo=repo,
                        git=git,
                        branch=branch,
                        worktree_path=worktree_path,
                        refs=refs,
                        lane_kind=lane_kind,
                        merged_fn=merged_fn,
                    )
                    if pres_failed or preservation not in _PRESERVED_KINDS:
                        extra = (
                            f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                        )
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
                    extra = (
                        f"preservation_moved tip {tip_sha} ref {preserving_ref} sha {preserving_sha}"
                    )
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
                removed, remove_detail = _remove_lane_worktree(repo, worktree_path)
                if not removed and not _worktree_is_absent(worktree_path, repo):
                    return _keep_after_persist(
                        item,
                        persist_failure,
                        f"worktree: {remove_detail}",
                        verdict="keep:probe_failed",
                        reason=f"worktree: {remove_detail}",
                    )
            if surviving_branch:
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
                missed = fence_effect("update-ref")
                if missed is not None:
                    return missed
                deleted, branch_detail = _delete_preserved_branch(git, branch, tip_sha)
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
            rationale=notes or _cross_session_close_notes(lane_id, tip_sha, preserving_ref, preserving_sha),
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

        applied = dict(item)
        applied["reason"] = notes
        applied["applied"] = True
        applied["verdict"] = "reclaim"
        return applied
    finally:
        _release_lane_worker_lock(claim_handle)
