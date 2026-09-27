"""Shared git merge/reachability helpers."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass

from .runtime import get_runtime_config


@dataclass(frozen=True)
class AncestryFatal:
    """Determinate git failure: the subprocess completed with a non-0/1 exit.

    ``git merge-base --is-ancestor`` exits 128 when an object is missing
    and other non-0/1 codes for usage or environment fatals. Those are
    finished answers, not undetermined checks. Distinct from ``None``
    (timeout, missing git binary, OS error).

    Falsy so ``if result:`` stays fail-closed for callers that have not
    switched to ``is True``. Never equal to ``True``, ``False``, or ``None``.
    """

    returncode: int
    stderr: str = ""

    def __bool__(self) -> bool:
        return False


def is_ancestor_of_ref(candidate: str, integration_ref: str) -> bool | AncestryFatal | None:
    """Return whether ``candidate`` is an ancestor of (or equal to) ``integration_ref``.

    Four-state, matching ``git merge-base --is-ancestor``:

    - ``True``: exit 0 — ``candidate`` is an ancestor of ``integration_ref``
    - ``False``: exit 1 — both objects exist and ``candidate`` is not an ancestor
    - ``AncestryFatal``: any other completed exit — determinate git failure
      (missing object, usage error, or other fatal); retrying cannot
      succeed. Carries the real return code and trimmed stderr.
    - ``None``: no result was obtained (missing git binary, timeout, OS
      error). Transient; a later retry may work.
    """
    config = get_runtime_config()
    try:
        proc = subprocess.run(
            ["git", "merge-base", "--is-ancestor", candidate, integration_ref],
            cwd=str(config.git_workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return AncestryFatal(
        returncode=proc.returncode,
        stderr=(proc.stderr or "").strip(),
    )


def branch_is_merged(target_branch: str, integration_ref: str = "main") -> bool:
    """True when ``target_branch`` is fully merged into ``integration_ref``."""
    if not target_branch:
        return False
    if is_ancestor_of_ref(target_branch, integration_ref) is True:
        return True
    # Post-merge on main often deletes the local feature branch while the
    # remote-tracking ref still resolves (typical ``git pull`` of a PR merge).
    return is_ancestor_of_ref(f"origin/{target_branch}", integration_ref) is True


def branch_exists(target_branch: str) -> bool:
    """True when a local branch ref exists for ``target_branch``."""
    if not target_branch:
        return False
    config = get_runtime_config()
    try:
        proc = subprocess.run(
            ["git", "show-ref", "--verify", f"refs/heads/{target_branch}"],
            cwd=str(config.git_workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0
