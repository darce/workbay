#!/usr/bin/env python3
"""Best-effort auto-reap of closeable tasks after a merge into main (internal).

Invoked by ``scripts/hooks/git/post-merge`` when the current branch is ``main`` or
``master``. Identifies task rows whose ``target_branch`` tip matches the commit
just merged into the integration branch and runs a scoped
``reap_tasks(apply=True, task_ref=...)``. Rows that are active or ambiguous are
left untouched — the reaper is conservative by construction. All failures are
swallowed: a git hook must never break a merge.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_INTEGRATION_BRANCHES = frozenset({"main", "master"})


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


def _reap_scoped(task_ref: str, integration_ref: str) -> None:
    from workbay_handoff_mcp.api import reap_tasks

    result = reap_tasks(apply=True, task_ref=task_ref, integration_ref=integration_ref)
    failed = []
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            failed = data.get("failed") or []
        else:
            failed = result.get("failed") or []
    for entry in failed:
        if not isinstance(entry, dict):
            continue
        print(
            f"post-merge-reap: failed {entry.get('task_ref', '?')} "
            f"stage={entry.get('stage', '?')}: {entry.get('error', '')}",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
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
    except ImportError:
        return 0

    try:
        configure_runtime(RuntimeConfig.for_repo(repo))
    except Exception:
        return 0

    # Discovery (list_handoff_rows import/query) and reap run under one broad
    # guard: a DB, import, or git error here must never escape and break the
    # merge — the shell ``|| true`` is a backstop, not the contract.
    try:
        tips = set(_merged_tips(repo))
        if not tips:
            return 0
        for task_ref in _live_task_refs_for_merged_tips(repo, tips):
            try:
                _reap_scoped(task_ref, integration_ref=branch)
            except Exception:
                continue
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
