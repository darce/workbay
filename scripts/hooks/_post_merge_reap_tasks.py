#!/usr/bin/env python3
"""Best-effort auto-reap of closeable tasks after a merge into main.

Invoked by ``scripts/hooks/git/post-merge`` when the current branch is ``main`` or
``master``. Identifies task rows whose ``target_branch`` tip matches the commit
just merged into the integration branch and runs a scoped
``reap_tasks(apply=True, task_ref=...)``. Rows that are active or ambiguous are
left untouched — the reaper is conservative by construction. Failures are
recorded to the shared hook-failure sink and the process still exits 0: a git
hook must never break a merge.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_INTEGRATION_BRANCHES = frozenset({"main", "master"})
_SOURCE = "post-merge-reap"


def _hooks_dir() -> str:
    return str(Path(__file__).resolve().parent)


def _ensure_hooks_dir_on_path() -> None:
    hook_dir = _hooks_dir()
    if hook_dir not in sys.path:
        sys.path.insert(0, hook_dir)


def _record_failure(*, kind: str, detail: str, task_ref: str | None = None) -> None:
    """Append one durable failure record; never raise into the hook.

    When ``task_ref`` is known it is emitted as a structured ``task_ref=`` token
    inside ``detail=`` so a later successful pass can purge *only* that task's
    own records (never a foreign source, never an unrelated soft-failure).
    """
    try:
        _ensure_hooks_dir_on_path()
        from _hook_failure_sink import record_hook_failure

        if task_ref:
            # Structured token first so selective purge can match without
            # substring-matching free-text detail from other writers.
            detail = f"task_ref={task_ref} {detail}"
        record_hook_failure(source=_SOURCE, kind=kind, detail=detail)
    except Exception:  # noqa: BLE001 -- sink must never break the merge
        pass


def _is_own_record(line: str) -> bool:
    """True when ``line`` is one of THIS hook's sink records.

    Matches on field POSITION, not substring. The record shape is
    ``ts=<iso> source=<name> kind=<kind> detail=<one line>`` and ``detail`` is
    free text, so an unanchored ``" source=post-merge-reap "`` test would also
    match another writer's record whose detail happens to quote ours — deleting
    a foreign record inside the very function written to stop cross-source
    deletion.
    """
    fields = line.split()
    return len(fields) > 1 and fields[1] == f"source={_SOURCE}"


def _task_ref_from_own_record(line: str) -> str | None:
    """Extract the structured ``task_ref=`` token from an own-source record."""
    if not _is_own_record(line):
        return None
    for tok in line.split():
        if tok.startswith("task_ref="):
            value = tok[len("task_ref=") :]
            return value or None
        # detail= may glue the token: detail=task_ref=TASK-X
        if tok.startswith("detail=task_ref="):
            value = tok[len("detail=task_ref=") :]
            return value or None
    return None


def _purge_resolved_hook_failures_safe(resolved_task_refs: set[str]) -> None:
    """Drop own-source records only for task_refs this pass proved resolved.

    Never purges when ``resolved_task_refs`` is empty (a pass that reaped
    nothing is not evidence that any prior failure is fixed). Never drops
    foreign sources' rows or own rows for task_refs not in the resolved set.
    Rewrite is flock-serialized with the sink's append/compaction path.
    """
    if not resolved_task_refs:
        return
    try:
        _ensure_hooks_dir_on_path()
        from _hook_failure_sink import (
            resolve_hook_failures_log_path,
            rewrite_hook_failures_under_lock,
        )

        path = resolve_hook_failures_log_path()
        if not path or not os.path.isfile(path):
            return

        def keep(line: str) -> bool:
            if not _is_own_record(line):
                return True
            tr = _task_ref_from_own_record(line)
            # Own record without a parseable task_ref: leave it (conservative).
            if tr is None:
                return True
            # Drop only when this pass successfully reaped that task_ref.
            return tr not in resolved_task_refs

        rewrite_hook_failures_under_lock(path, keep)
    except Exception:  # noqa: BLE001 -- purge must never break the merge
        pass


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False, cwd=cwd)
    except (subprocess.TimeoutExpired, OSError):
        # A hung or unspawnable git call must never break the merge; surface it
        # to callers as a non-zero result they already treat as "unknown".
        return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="")


def _repo_root() -> Path:
    proc = _run(["git", "rev-parse", "--show-toplevel"], Path.cwd())
    if proc.returncode != 0 or not proc.stdout.strip():
        return Path.cwd()
    return Path(proc.stdout.strip())


def _current_branch(repo: Path) -> str:
    proc = _run(["git", "branch", "--show-current"], repo)
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _commit_sha(repo: Path, ref: str) -> str:
    proc = _run(["git", "rev-parse", ref], repo)
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _parents(repo: Path, ref: str = "HEAD") -> list[str]:
    """Return the parent SHAs of ``ref`` in order (first parent first)."""
    proc = _run(["git", "cat-file", "-p", ref], repo)
    if proc.returncode != 0:
        return []
    parents: list[str] = []
    for line in proc.stdout.splitlines():
        if line.startswith("parent "):
            parents.append(line.split(" ", 1)[1].strip())
        elif not line.strip():
            break  # blank line terminates the commit header
    return parents


def _merged_tips(repo: Path) -> list[str]:
    """Return SHAs for branch tips that were just merged into HEAD.

    A real merge commit records the pre-merge tip as its *first* parent and sets
    ``ORIG_HEAD`` to that same commit; the merged branch tips are therefore the
    remaining parents (``HEAD^2..HEAD^N`` for an octopus merge). Discriminating
    on ``first_parent == ORIG_HEAD`` rather than raw parent count matters when
    the merged branch's own tip is itself a merge commit and the integration
    branch is fast-forwarded onto it: there HEAD has two parents but the whole
    tip (HEAD) was merged, not its second parent.
    """
    orig = _commit_sha(repo, "ORIG_HEAD")
    head = _commit_sha(repo, "HEAD")
    if not head:
        return []
    parents = _parents(repo)
    if orig and parents and parents[0] == orig and len(parents) > 1:
        # True merge commit (incl. octopus): every non-first parent is a tip.
        return parents[1:]
    if orig and orig != head:
        # Fast-forward: HEAD advanced to the merged tip; no new commit created.
        return [head]
    return []


def _resolve_branch_tip(repo: Path, branch: str) -> str:
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        proc = _run(["git", "rev-parse", ref], repo)
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    proc = _run(["git", "rev-parse", branch], repo)
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout.strip()
    return ""


def _live_task_refs_for_merged_tips(repo: Path, tips: set[str]) -> list[str]:
    from workbay_handoff_mcp.api import list_handoff_rows
    from workbay_handoff_mcp.shared_primitives import LIVE_ACTIVE_STATUSES

    if not tips:
        return []

    refs: list[str] = []
    rows = list_handoff_rows(status_filter=list(LIVE_ACTIVE_STATUSES))
    for row in rows:
        if not isinstance(row, dict):
            continue
        target_branch = row.get("target_branch")
        if not isinstance(target_branch, str) or not target_branch:
            continue
        tip = _resolve_branch_tip(repo, target_branch)
        if tip not in tips:
            continue
        ref = row.get("task_ref")
        if isinstance(ref, str) and ref and ref not in refs:
            refs.append(ref)
    return refs


def _reap_scoped(task_ref: str, integration_ref: str) -> bool:
    """Reap one task. Return True when the call reported a soft failure.

    ``reap_tasks`` reports per-stage problems in a ``failed`` list rather than
    by raising, so an exception-only guard treats them as success. Printing them
    to stderr is not durable — git discards hook output — so each one is also
    written to the shared sink ([AGT-10]: a swallowed error that keeps the
    session alive must still land in a log). The boolean is what stops a pass
    with soft failures from being treated as clean by the caller's purge.
    """
    from workbay_handoff_mcp.api import reap_tasks

    result = reap_tasks(apply=True, task_ref=task_ref, integration_ref=integration_ref)
    failed = []
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            failed = data.get("failed") or []
        else:
            failed = result.get("failed") or []
    saw_failure = False
    for entry in failed:
        if not isinstance(entry, dict):
            continue
        saw_failure = True
        entry_ref = entry.get("task_ref", task_ref) or task_ref
        detail = (
            f"stage={entry.get('stage', '?')}: {entry.get('error', '')}"
        )
        print(
            f"post-merge-reap: failed task_ref={entry_ref} {detail}",
            file=sys.stderr,
        )
        _record_failure(
            kind="returncode",
            detail=detail,
            task_ref=str(entry_ref) if entry_ref else task_ref,
        )
    return saw_failure


def main(argv: list[str] | None = None) -> int:
    # Harness-agnostic interpreter self-heal: re-exec under project .venv so
    # bare ``python3`` (no workbay_handoff_mcp) does not silent-no-op. Non-fatal
    # if the sibling ``_interp`` helper is unavailable. See scripts/hooks/_interp.py.
    try:
        _ensure_hooks_dir_on_path()
        from _interp import ensure_deps_interpreter

        ensure_deps_interpreter()
    except Exception:  # noqa: BLE001 -- fail open if _interp missing
        pass

    args = argv if argv is not None else sys.argv[1:]
    if args and args[0] == "1":
        # Squash merges do not preserve a second parent; identifying the
        # source branch/task is unreliable — skip auto-reap.
        return 0

    repo = _repo_root()
    branch = _current_branch(repo)
    if branch not in _INTEGRATION_BRANCHES:
        return 0

    try:
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    except ImportError as exc:
        _record_failure(kind="exception", detail=f"ImportError: {exc}")
        return 0

    try:
        configure_runtime(RuntimeConfig.for_repo(repo))
    except Exception as exc:  # noqa: BLE001 -- degrade loudly, never break merge
        _record_failure(
            kind="exception",
            detail=f"configure_runtime: {type(exc).__name__}: {exc}",
        )
        return 0

    # Discovery (list_handoff_rows import/query) and reap run under one broad
    # guard: a DB, import, or git error here must never escape and break the
    # merge — the shell ``|| true`` is a backstop, not the contract. Failures
    # are recorded to the shared sink ([AGT-10]).
    # Track only task_refs this pass *successfully* reaped; purge is scoped
    # to those refs alone (never "all own records" on a clean-looking pass).
    resolved_task_refs: set[str] = set()
    try:
        tips = set(_merged_tips(repo))
        if not tips:
            # No-op pass: nothing was reaped, so this is NOT evidence that any
            # previously recorded failure is resolved. Do not purge here.
            return 0
        for task_ref in _live_task_refs_for_merged_tips(repo, tips):
            try:
                if _reap_scoped(task_ref, integration_ref=branch):
                    # Soft failure recorded; do not mark resolved.
                    continue
                resolved_task_refs.add(task_ref)
            except Exception as exc:  # noqa: BLE001 -- per-task; continue others
                _record_failure(
                    kind="exception",
                    detail=f"{type(exc).__name__}: {exc}",
                    task_ref=task_ref,
                )
                continue
    except Exception as exc:  # noqa: BLE001 -- degrade loudly, never break merge
        _record_failure(kind="exception", detail=f"{type(exc).__name__}: {exc}")
        return 0

    # Purge only records this pass proved resolved. Empty set → no-op
    # (covers zero matching task rows and all-failed passes).
    _purge_resolved_hook_failures_safe(resolved_task_refs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
