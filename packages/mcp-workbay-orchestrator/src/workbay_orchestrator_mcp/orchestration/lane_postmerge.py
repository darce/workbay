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

import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
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
    """
    sql = (
        "UPDATE worktree_lanes "
        "SET status = ?, notes = COALESCE(?, notes), updated_at = datetime('now') "
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


def _tip_from_close_notes(notes: str | None) -> str | None:
    """Recover the reaped tip from notes written in the same CAS close.

    Used to retry a missing ``postmerge_reap`` decision after the branch is
    already gone. Notes that were not written by this reaper return None.
    """
    prefix = "post-merge reap of "
    if not notes or not notes.startswith(prefix):
        return None
    idx = notes.rfind(_CLOSE_NOTES_MARKER)
    if idx < 0:
        return None
    candidate = notes[idx + len(_CLOSE_NOTES_MARKER) :].strip()
    return candidate or None


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
    alone does not decide: a live successor may reuse the path.
    """
    tip_sha = _tip_from_close_notes(_text(_row_get(row, "notes")))
    if tip_sha is None:
        return False
    branch = _text(_row_get(row, "branch")) or ""
    worktree_path = _text(_row_get(row, "worktree_path"))
    if _branch_names_consumed_tip(git, branch, tip_sha):
        return True
    if _worktree_is_absent(worktree_path, repo) or not worktree_path:
        return False
    return _path_still_our_checkout(git, worktree_path, branch, tip_sha)


def _restore_consumed_branch(git: Callable[..., Any], branch: str, tip_sha: str) -> None:
    """Recreate the consumed ref so a record miss stays retry-classified.

    ``postmerge_reap`` is recorded only after the worktree and branch are
    both gone. If that write misses, restoring the tip-pinned ref makes
    ``_terminal_needs_retry`` true until the decision exists. A ref that
    already exists is left untouched.
    """
    if not branch or not tip_sha:
        return
    try:
        current = _probe_tip_sha(git, branch)
    except _GitError:
        return
    if current is not None:
        return
    name = _branch_short_name(branch)
    try:
        _git(git, ["branch", name, tip_sha])
    except _GitError:
        return


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
