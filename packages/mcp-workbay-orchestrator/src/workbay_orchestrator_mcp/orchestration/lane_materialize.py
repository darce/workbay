"""Deterministic lane-branch + worktree materialization for dispatch.

``ensure_lane_worktree`` can rebuild a checkout only when the lane branch
already exists in the primary repository. This module is the missing
pre-flight: create the branch at the manifest ``base_sha`` when absent,
verify ancestry when present, then hand off to ``ensure_lane_worktree``.

Manifest + ``worktree_lanes`` row are the system of record; git state is a
derived view that is reconciled, never guessed (DDIA). Typed refusals make
the safe path the only path (Release It!). Dry-run is the default; apply is
opt-in. Never checkout in the primary repo. Never delete. Never force-move
a branch.

Citations
---------
``[Release It!]`` / make the safe path the only path
    Short, unknown, or off-base SHAs refuse with a named kind. There is no
    "just create it anyway" branch.

``[DDIA]`` / derived state vs source of record
    Branch name and base come from the manifest; worktree path from the
    ``worktree_lanes`` row when present. Git is updated to match, not the
    other way around.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_GIT_TIMEOUT_SECONDS = 30

REFUSAL_NO_MANIFEST_ENTRY = "no_manifest_entry"
REFUSAL_NO_ROW = "no_row"
REFUSAL_EMPTY_WORKTREE_PATH = "empty_worktree_path"
REFUSAL_SHORT_BASE_SHA = "short_base_sha"
REFUSAL_BASE_SHA_UNKNOWN = "base_sha_unknown"
REFUSAL_BRANCH_NOT_FROM_BASE = "branch_not_from_base"
REFUSAL_BRANCH_CREATE_FAILED = "branch_create_failed"
REFUSAL_GIT_TIMEOUT = "git_timeout"

BRANCH_CREATED = "created"
BRANCH_PRESENT = "present"
BRANCH_PLANNED = "planned"
BRANCH_REFUSED = "refused"

WORKTREE_CREATED = "created"
WORKTREE_PRESENT = "present"
WORKTREE_PLANNED = "planned"
WORKTREE_SKIPPED = "skipped"

LoadManifest = Callable[[str], Mapping[str, Any]]
GetRow = Callable[[str, str], Mapping[str, Any] | None]


class RunGit(Protocol):
    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path | str,
    ) -> subprocess.CompletedProcess[str]: ...


class EnsureWorktree(Protocol):
    def __call__(
        self,
        *,
        primary_repo: Path | str,
        worktree_path: Path | str,
        branch: str,
        lane_id: str = "",
        task_ref: str = "",
    ) -> Any: ...


@dataclass(frozen=True)
class MaterializeResult:
    """Typed outcome of :func:`materialize_lane`.

    ``branch_outcome`` is one of ``created``, ``present``, ``planned``,
    ``refused``. ``worktree_outcome`` is ``planned`` / ``present`` /
    ``created`` / ``skipped``, or the ``EnsureLaneWorktreeResult.outcome``
    string when ensure refuses. ``applied`` is True only when *apply* ran
    far enough to mutate git or invoke ensure.
    """

    lane_id: str
    branch: str
    base_sha: str
    worktree_path: str
    branch_outcome: str
    worktree_outcome: str
    refusal_kind: str | None
    detail: str
    applied: bool


def _text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _resolve_worktree_path(raw: str, *, root: Path) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _paths_match(left: str, right: str) -> bool:
    try:
        return Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
    except OSError:
        return os.path.normpath(left) == os.path.normpath(right)


def _cmd_tokens(cmd: object) -> list[str]:
    if isinstance(cmd, str):
        return [cmd]
    if isinstance(cmd, Sequence) and not isinstance(cmd, (bytes, bytearray)):
        return [str(part) for part in cmd]
    return []


def _refuse_git_timeout(
    *,
    lane_id: str,
    exc: subprocess.TimeoutExpired,
    args: Sequence[str] | None = None,
    branch: str = "",
    base_sha: str = "",
    worktree_path: str = "",
) -> MaterializeResult:
    fallback = ["git", *[str(part) for part in args]] if args is not None else ["git"]
    cmd = _cmd_tokens(exc.cmd) or fallback
    timeout = exc.timeout if exc.timeout is not None else _GIT_TIMEOUT_SECONDS
    detail = f"git timed out after {timeout}s: {cmd}"
    verbs = cmd[1:] if cmd[:1] == ["git"] else cmd
    if branch and verbs[:1] == ["branch"]:
        detail = f"{detail}; leftover branch {branch!r} was not deleted"
    return _refuse(
        lane_id=lane_id,
        kind=REFUSAL_GIT_TIMEOUT,
        detail=detail,
        branch=branch,
        base_sha=base_sha,
        worktree_path=worktree_path,
    )


def _default_run_git(args: Sequence[str], *, cwd: Path | str) -> subprocess.CompletedProcess[str]:
    cmd = ["git", *list(args)]
    try:
        return subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        # Call sites convert this into REFUSAL_GIT_TIMEOUT; keep cmd + timeout.
        raise subprocess.TimeoutExpired(
            cmd=_cmd_tokens(exc.cmd) or cmd,
            timeout=exc.timeout if exc.timeout is not None else _GIT_TIMEOUT_SECONDS,
            output=exc.output,
            stderr=exc.stderr,
        ) from exc


def _default_load_manifest(task_ref: str) -> Mapping[str, Any]:
    from workbay_orchestrator_mcp.orchestration.lane_manifest import load_manifest

    return load_manifest(task_ref)


def _default_get_row(task_ref: str, lane_id: str) -> Mapping[str, Any] | None:
    """Look up the ``worktree_lanes`` row; None when missing or unreadable."""
    try:
        from workbay_orchestrator_mcp.lanes import list_worktree_lanes
    except ImportError:
        return None
    offset = 0
    limit = 100
    for _ in range(10_000):
        try:
            listed = list_worktree_lanes(task_ref=task_ref, status="all", limit=limit, offset=offset)
        except Exception:  # noqa: BLE001 — unreadable row store is a missing row
            return None
        if isinstance(listed, str):
            return None
        if not isinstance(listed, dict) or listed.get("ok") is not True:
            return None
        lanes = listed.get("lanes")
        if not isinstance(lanes, list):
            return None
        for row in lanes:
            if isinstance(row, dict) and str(row.get("lane_id") or "") == lane_id:
                return row
        if listed.get("has_more") is not True or not lanes:
            return None
        offset += len(lanes)
    return None


def _default_ensure_worktree(**kwargs: Any) -> Any:
    from workbay_orchestrator_mcp.orchestration.lane_worktree import ensure_lane_worktree

    return ensure_lane_worktree(**kwargs)


def _lane_entry(manifest: Mapping[str, Any] | object, lane_id: str) -> Mapping[str, Any] | None:
    if not isinstance(manifest, Mapping):
        return None
    lanes = manifest.get("lanes")
    if not isinstance(lanes, Mapping):
        return None
    entry = lanes.get(lane_id)
    return entry if isinstance(entry, Mapping) else None


def _registered_worktree_on_branch(
    *,
    run_git: RunGit,
    root: Path,
    worktree_path: str,
    branch: str,
) -> bool:
    if not worktree_path:
        return False
    worktree_list = ["worktree", "list", "--porcelain"]
    try:
        proc = run_git(worktree_list, cwd=root)
    except subprocess.TimeoutExpired as exc:
        raise subprocess.TimeoutExpired(
            cmd=_cmd_tokens(exc.cmd) or ["git", *worktree_list],
            timeout=exc.timeout if exc.timeout is not None else _GIT_TIMEOUT_SECONDS,
            output=exc.output,
            stderr=exc.stderr,
        ) from exc
    if proc.returncode != 0:
        return False
    want_branch = f"refs/heads/{branch}"
    current_path: str | None = None
    current_branch: str | None = None

    def _flush() -> bool:
        return bool(
            current_path is not None
            and _paths_match(current_path, worktree_path)
            and current_branch == want_branch
        )

    for raw in (proc.stdout or "").split("\n"):
        line = raw[:-1] if raw.endswith("\r") else raw
        if line.startswith("worktree "):
            if _flush():
                return True
            current_path = line[len("worktree ") :]
            current_branch = None
        elif line.startswith("branch "):
            current_branch = line[len("branch ") :]
        elif line == "":
            if _flush():
                return True
            current_path = None
            current_branch = None
    return _flush()


def _refuse(
    *,
    lane_id: str,
    kind: str,
    detail: str,
    branch: str = "",
    base_sha: str = "",
    worktree_path: str = "",
) -> MaterializeResult:
    return MaterializeResult(
        lane_id=lane_id,
        branch=branch,
        base_sha=base_sha,
        worktree_path=worktree_path,
        branch_outcome=BRANCH_REFUSED,
        worktree_outcome=WORKTREE_SKIPPED,
        refusal_kind=kind,
        detail=detail,
        applied=False,
    )


def materialize_lane(
    task_ref: str,
    lane_id: str,
    *,
    root: Path | str,
    apply: bool = False,
    load_manifest: LoadManifest | None = None,
    get_row: GetRow | None = None,
    run_git: RunGit | None = None,
    ensure_worktree: EnsureWorktree | None = None,
) -> MaterializeResult:
    """Reconcile the lane branch and worktree to the manifest + row.

    Dry-run by default (``apply=False``): reports ``planned`` / ``present``
    without creating refs or worktrees. ``apply=True`` creates a missing
    branch at ``base_sha`` (never force-updates an existing one) and then
    calls ``ensure_lane_worktree``. An empty ``worktree_path`` on both the
    row and the manifest refuses before any branch write; resolving an
    empty path would otherwise aim ensure at cwd.
    """
    primary = Path(root).expanduser().resolve()
    loader = load_manifest if load_manifest is not None else _default_load_manifest
    row_lookup = get_row if get_row is not None else _default_get_row
    git = run_git if run_git is not None else _default_run_git

    try:
        manifest = loader(task_ref)
    except FileNotFoundError:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_MANIFEST_ENTRY,
            detail=f"no manifest entry for lane {lane_id!r} on task {task_ref!r}",
        )

    entry = _lane_entry(manifest, lane_id)
    if entry is None:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_MANIFEST_ENTRY,
            detail=f"no manifest entry for lane {lane_id!r} on task {task_ref!r}",
        )

    branch = _text(entry.get("branch"))
    base_sha = _text(entry.get("base_sha"))
    manifest_path = _text(entry.get("worktree_path"))
    if not branch:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_MANIFEST_ENTRY,
            detail=f"manifest entry for lane {lane_id!r} is missing a branch",
            base_sha=base_sha,
            worktree_path=manifest_path,
        )

    row = row_lookup(task_ref, lane_id)
    if not isinstance(row, Mapping):
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_ROW,
            detail=f"no worktree_lanes row for lane {lane_id!r} on task {task_ref!r}",
            branch=branch,
            base_sha=base_sha,
            worktree_path=manifest_path,
        )

    row_path = _text(row.get("worktree_path"))
    worktree_raw = row_path or manifest_path
    if not worktree_raw:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_EMPTY_WORKTREE_PATH,
            detail=(
                "worktree_path is empty on both the worktree_lanes row and the "
                "manifest entry; refusing to create a branch or ensure a worktree at cwd"
            ),
            branch=branch,
            base_sha=base_sha,
            worktree_path="",
        )
    worktree_path = str(_resolve_worktree_path(worktree_raw, root=primary))

    if not _FULL_SHA_RE.fullmatch(base_sha):
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_SHORT_BASE_SHA,
            detail="base_sha must be a 40-character lowercase hex commit",
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
        )

    def _git(args: Sequence[str]) -> subprocess.CompletedProcess[str] | MaterializeResult:
        try:
            return git(args, cwd=primary)
        except subprocess.TimeoutExpired as exc:
            return _refuse_git_timeout(
                lane_id=lane_id,
                exc=exc,
                args=args,
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )

    cat = _git(["cat-file", "-e", f"{base_sha}^{{commit}}"])
    if isinstance(cat, MaterializeResult):
        return cat
    if cat.returncode != 0:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_BASE_SHA_UNKNOWN,
            detail=f"base_sha {base_sha} does not name a commit in the primary repo",
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
        )

    show = _git(["show-ref", "--verify", f"refs/heads/{branch}"])
    if isinstance(show, MaterializeResult):
        return show
    branch_exists = show.returncode == 0

    def _dry_worktree_outcome() -> str | MaterializeResult:
        try:
            registered = _registered_worktree_on_branch(
                run_git=git,
                root=primary,
                worktree_path=worktree_path,
                branch=branch,
            )
        except subprocess.TimeoutExpired as exc:
            return _refuse_git_timeout(
                lane_id=lane_id,
                exc=exc,
                args=["worktree", "list", "--porcelain"],
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )
        if registered:
            return WORKTREE_PRESENT
        return WORKTREE_PLANNED

    if not branch_exists:
        if not apply:
            worktree_outcome = _dry_worktree_outcome()
            if isinstance(worktree_outcome, MaterializeResult):
                return worktree_outcome
            return MaterializeResult(
                lane_id=lane_id,
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
                branch_outcome=BRANCH_PLANNED,
                worktree_outcome=worktree_outcome,
                refusal_kind=None,
                detail=f"dry-run: would create {branch!r} at {base_sha} and ensure worktree {worktree_path}",
                applied=False,
            )
        created = _git(["branch", "--", branch, base_sha])
        if isinstance(created, MaterializeResult):
            return created
        if created.returncode != 0:
            detail = (created.stderr or created.stdout or "").strip() or f"exit {created.returncode}"
            return _refuse(
                lane_id=lane_id,
                kind=REFUSAL_BRANCH_CREATE_FAILED,
                detail=f"git branch {branch!r} {base_sha} failed: {detail}",
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )
        branch_outcome = BRANCH_CREATED
    else:
        ancestor = _git(["merge-base", "--is-ancestor", base_sha, branch])
        if isinstance(ancestor, MaterializeResult):
            return ancestor
        if ancestor.returncode != 0:
            return _refuse(
                lane_id=lane_id,
                kind=REFUSAL_BRANCH_NOT_FROM_BASE,
                detail=(
                    f"lane branch {branch!r} does not contain base_sha {base_sha} as an ancestor; "
                    "refusing to move or force the branch"
                ),
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )
        if not apply:
            worktree_outcome = _dry_worktree_outcome()
            if isinstance(worktree_outcome, MaterializeResult):
                return worktree_outcome
            return MaterializeResult(
                lane_id=lane_id,
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
                branch_outcome=BRANCH_PRESENT,
                worktree_outcome=worktree_outcome,
                refusal_kind=None,
                detail=f"dry-run: branch {branch!r} already present from {base_sha}",
                applied=False,
            )
        branch_outcome = BRANCH_PRESENT

    ensurer = ensure_worktree if ensure_worktree is not None else _default_ensure_worktree
    try:
        ensured = ensurer(
            primary_repo=primary,
            worktree_path=worktree_path,
            branch=branch,
            lane_id=lane_id,
            task_ref=task_ref,
        )
    except subprocess.TimeoutExpired as exc:
        return _refuse_git_timeout(
            lane_id=lane_id,
            exc=exc,
            args=["worktree"],
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
        )
    if not getattr(ensured, "ok", False):
        outcome = getattr(ensured, "outcome", None) or "refused"
        failure_kind = getattr(ensured, "failure_kind", None)
        error = _text(getattr(ensured, "error", None)) or f"ensure_lane_worktree refused ({failure_kind or outcome})"
        return MaterializeResult(
            lane_id=lane_id,
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
            branch_outcome=branch_outcome,
            worktree_outcome=str(outcome),
            refusal_kind=str(failure_kind) if failure_kind else str(outcome),
            detail=error,
            applied=True,
        )

    rematerialized = bool(getattr(ensured, "rematerialized", False))
    worktree_outcome = WORKTREE_CREATED if rematerialized else WORKTREE_PRESENT
    ensured_path = getattr(ensured, "worktree_path", None)
    if ensured_path:
        worktree_path = str(Path(ensured_path).expanduser().resolve())
    return MaterializeResult(
        lane_id=lane_id,
        branch=branch,
        base_sha=base_sha,
        worktree_path=worktree_path,
        branch_outcome=branch_outcome,
        worktree_outcome=worktree_outcome,
        refusal_kind=None,
        detail=f"lane {lane_id!r} branch {branch_outcome}, worktree {worktree_outcome}",
        applied=True,
    )
