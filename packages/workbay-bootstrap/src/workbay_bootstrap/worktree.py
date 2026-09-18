"""Pure linked-worktree / overlay-root detection helpers (implementation note, Slice S0).

A linked git worktree (``git worktree add``, Claude Code's
``.claude/worktrees/<name>/``) shares the primary's ``.git`` but not gitignored
files, so the bootstrap overlay (``.workbay/``, ``.workbay-bootstrap.json``,
the materialized shared surfaces) is absent until it is adopted. The adopt path
needs three pure questions answered before it mutates anything:

* Is this path a *linked* worktree (vs the primary, a bare repo, or a plain
  directory)? — :func:`is_linked_worktree`.
* Where is the *primary* overlay root, identified by its authoritative marker
  ``.workbay-bootstrap.json`` rather than by assuming the git root? —
  :func:`primary_overlay_root`.
* Is a given root actually materialized (marker + clone present)? —
  :func:`overlay_is_materialized`.

These helpers shell out to ``git`` read-only and never write. The marker name
and clone subdirectory are imported from :mod:`workbay_bootstrap.install` so
there is a single source of truth.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from workbay_bootstrap.install import BOOTSTRAP_MANIFEST_NAME, CLONE_SUBDIR


class WorktreeError(RuntimeError):
    """Base error for worktree / overlay resolution."""


class NotAGitRepositoryError(WorktreeError):
    """``path`` is not inside a git repository."""


class OverlayMarkerNotFoundError(WorktreeError):
    """No ``.workbay-bootstrap.json`` marker at or above the primary git root."""


def _git_out(path: Path, *args: str) -> str:
    """Run ``git -C <path> <args>`` read-only, returning stripped stdout.

    Raises :class:`NotAGitRepositoryError` when ``path`` is not inside a git
    repository (git exits non-zero, typically 128).
    """
    from workbay_bootstrap.external import run_external

    try:
        result = run_external(
            ["git", "-C", str(path), *args],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        # git binary absent from PATH — surface the typed contract error rather
        # than leaking a raw FileNotFoundError (mirrors install.py's git guard).
        raise NotAGitRepositoryError(
            f"git executable not found while inspecting {path}"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise NotAGitRepositoryError(f"not inside a git repository: {path}") from exc
    return result.stdout.strip()


def _resolve_git_dir(path: Path, value: str) -> Path:
    """Resolve a ``git rev-parse`` dir output (which may be relative) absolutely."""
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = path / candidate
    return candidate.resolve()


def _common_and_git_dir(path: Path) -> tuple[Path, Path]:
    """Return ``(--git-common-dir, --git-dir)`` resolved to absolute paths."""
    common = _resolve_git_dir(path, _git_out(path, "rev-parse", "--git-common-dir"))
    git_dir = _resolve_git_dir(path, _git_out(path, "rev-parse", "--git-dir"))
    return common, git_dir


def is_linked_worktree(path: Path) -> bool:
    """Return True only when ``path`` is a git *linked* worktree.

    A linked worktree has its own ``--git-dir`` (``<primary>/.git/worktrees/<name>``)
    that differs from the shared ``--git-common-dir`` (``<primary>/.git``). The
    primary worktree has them equal. A bare repository has no worktree and is
    handled explicitly. Detached HEAD is orthogonal — a detached *linked*
    worktree still returns True; a detached *primary* still returns False.

    Raises :class:`NotAGitRepositoryError` when ``path`` is not in a git repo.
    """
    path = Path(path)
    if _git_out(path, "rev-parse", "--is-bare-repository") == "true":
        return False
    common, git_dir = _common_and_git_dir(path)
    return common != git_dir


def primary_overlay_root(path: Path) -> Path:
    """Resolve the primary overlay root by marker, searching upward.

    Starts at ``parent(--git-common-dir)`` (the primary git root — for a linked
    worktree this is the *primary*, not the worktree) and walks toward the
    filesystem root looking for the first directory that contains
    ``.workbay-bootstrap.json``. The marker is authoritative: the overlay is
    NOT assumed to sit at the git root, because nested-source layouts place the
    git repo inside the overlay directory.

    The walk prefers a **materialized** overlay (marker + ``.workbay/remote``
    clone): an *unmaterialized* stray marker — a leftover marker file in an
    ancestor (a developer home dir, a sibling repo's overlay one level up) with
    no clone — is skipped so a real materialized overlay higher up wins, instead
    of silently mis-resolving the primary to the stray. If a marker exists but
    *no* overlay at/above the start is materialized, the nearest marker is
    returned so the caller surfaces the specific "not materialized" error rather
    than a misleading "no marker found".

    Trade-off (intended): preferring a materialized ancestor is what makes the
    nested-source layout (git repo *inside* the overlay) resolvable at all, since
    the primary git root legitimately carries no marker there. The cost is that a
    not-yet-installed project that ships a tracked marker but sits *under* a
    separately-materialized ancestor overlay resolves to that ancestor (adopt
    proceeds against it) instead of failing loudly. This is an accepted,
    recoverable best-effort outcome (re-install in the project to fix it), not a
    silent data hazard; the supported ``make task-start`` flow (sibling worktrees
    of a normally-bootstrapped primary) never hits it.

    Raises :class:`OverlayMarkerNotFoundError` when no marker is found at all
    (the primary is un-adopted / not bootstrapped), and
    :class:`NotAGitRepositoryError` when ``path`` is not in a git repo.
    """
    path = Path(path)
    common, _ = _common_and_git_dir(path)
    start = common.parent
    nearest_marker: Path | None = None
    current = start
    while True:
        if (current / BOOTSTRAP_MANIFEST_NAME).is_file():
            if overlay_is_materialized(current):
                return current  # prefer a materialized overlay
            if nearest_marker is None:
                nearest_marker = current  # fallback if none is materialized
        if current.parent == current:  # reached the filesystem root
            break
        current = current.parent
    if nearest_marker is not None:
        return nearest_marker
    raise OverlayMarkerNotFoundError(
        f"no {BOOTSTRAP_MANIFEST_NAME} overlay marker found at or above {start}"
    )


def overlay_is_materialized(root: Path) -> bool:
    """Return True when ``root`` carries both the marker and the clone.

    The clone lives at ``root/.workbay/remote`` (``CLONE_SUBDIR``). Both the
    marker file and the clone directory must exist for the overlay to be
    considered materialized.
    """
    root = Path(root)
    if not (root / BOOTSTRAP_MANIFEST_NAME).is_file():
        return False
    clone = root.joinpath(*CLONE_SUBDIR)
    return clone.exists()
