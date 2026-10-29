"""Stdlib git resolver for the lifecycle runner (internal).

Pure ``git`` shell-outs — no third-party dependencies — so the runner
works in a freshly bootstrapped consumer that has only ``git``, POSIX
shell, and the Python stdlib. Every helper returns ``None`` / an empty
collection on failure; nothing raises.

The branch-naming grammar duplicated in :func:`derive_task_ref` is the
canonical regex from
``packages/workbay-protocol/src/workbay_protocol/branch_naming.py``;
inlined here because consumer-bootstrapped surfaces cannot import
``workbay_protocol`` from the monorepo source layout. Keep the two in
sync — see ``check_branch_naming.py`` for the same duplication
discipline.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Any

# Mirror of workbay_protocol.branch_naming.TASK_REF_RE. Kept inline so
# the lifecycle runner does not depend on packages/workbay-protocol/.
_TASK_REF_RE: re.Pattern[str] = re.compile(
    r"^feature/"
    r"(?=[a-z])"
    r"(?=[a-z0-9-]*\d)"
    r"(?P<task_ref>[a-z0-9]+(?:-[a-z0-9]+)+)"
    r"$"
)


def _run_git(repo: Path, *args: str) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def repo_root(start: Path | None = None) -> Path | None:
    """Return ``git rev-parse --show-toplevel`` from ``start`` (or cwd)."""
    cwd = Path(start) if start is not None else Path.cwd()
    proc = _run_git(cwd, "rev-parse", "--show-toplevel")
    if proc is None or proc.returncode != 0:
        return None
    out = proc.stdout.strip()
    return Path(out) if out else None


def current_worktree(start: Path | None = None) -> Path | None:
    """Return the linked worktree path that owns ``start`` (or cwd).

    For a primary worktree this matches :func:`repo_root`. For an
    internal-style ``-<task>`` worktree it points at that linked worktree.
    """
    cwd = Path(start) if start is not None else Path.cwd()
    proc = _run_git(cwd, "rev-parse", "--show-toplevel")
    if proc is None or proc.returncode != 0:
        return None
    out = proc.stdout.strip()
    return Path(out) if out else None


def canonical_workspace_root(repo: Path | None = None) -> Path | None:
    """Return the primary worktree path even from a linked worktree.

    Resolves ``git rev-parse --git-common-dir`` and returns its parent
    directory. The primary worktree owns ``CURRENT_TASK.json`` and the
    handoff DB; linked worktrees must thread this path back to the
    canonical state instead of writing to their own ``.task-state``.
    """
    start = Path(repo) if repo is not None else Path.cwd()
    proc = _run_git(start, "rev-parse", "--git-common-dir")
    if proc is None or proc.returncode != 0:
        return None
    raw = proc.stdout.strip()
    if not raw:
        return None
    common = Path(raw)
    if not common.is_absolute():
        common = (start / common).resolve()
    return common.parent


def current_branch(repo: Path) -> str | None:
    """Return the current branch name; ``None`` on detached HEAD or error."""
    proc = _run_git(repo, "branch", "--show-current")
    if proc is None or proc.returncode != 0:
        return None
    name = proc.stdout.strip()
    return name or None


def head_sha(repo: Path) -> str | None:
    """Return the full 40-char ``HEAD`` SHA, or ``None`` on error."""
    proc = _run_git(repo, "rev-parse", "HEAD")
    if proc is None or proc.returncode != 0:
        return None
    sha = proc.stdout.strip()
    return sha if len(sha) == 40 else None


def merge_base(repo: Path, target: str = "main") -> str | None:
    """Return ``git merge-base HEAD <target>`` or ``None`` if unresolvable."""
    proc = _run_git(repo, "merge-base", "HEAD", target)
    if proc is None or proc.returncode != 0:
        return None
    sha = proc.stdout.strip()
    return sha if len(sha) == 40 else None


def dirty_summary(repo: Path) -> dict[str, int]:
    """Return staged / unstaged / untracked / total file counts.

    Parses ``git status --porcelain=v1 -uall``. ``staged`` counts files
    with a non-space index status; ``unstaged`` counts non-space
    worktree status; ``untracked`` counts ``??`` entries. A renamed
    staged file counts once as staged.
    """
    empty = {"staged": 0, "unstaged": 0, "untracked": 0, "total": 0}
    proc = _run_git(repo, "status", "--porcelain=v1", "-uall")
    if proc is None or proc.returncode != 0:
        return empty
    staged = unstaged = untracked = 0
    for line in proc.stdout.splitlines():
        if not line:
            continue
        if line.startswith("??"):
            untracked += 1
            continue
        # Porcelain v1: index status at col 0, worktree status at col 1.
        index_status = line[0:1]
        worktree_status = line[1:2]
        if index_status not in (" ", "?"):
            staged += 1
        if worktree_status not in (" ", "?"):
            unstaged += 1
    return {
        "staged": staged,
        "unstaged": unstaged,
        "untracked": untracked,
        "total": staged + unstaged + untracked,
    }


def linked_worktrees(repo: Path) -> list[dict[str, Any]]:
    """Enumerate linked worktrees via ``git worktree list --porcelain``.

    Each entry: ``{"path": <abs str>, "head": <40-char sha>, "branch":
    <ref name without refs/heads/, or None for detached>}``.
    """
    proc = _run_git(repo, "worktree", "list", "--porcelain")
    if proc is None or proc.returncode != 0:
        return []
    items: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    for raw in proc.stdout.splitlines():
        if not raw:
            if current:
                items.append(current)
                current = {}
            continue
        if raw.startswith("worktree "):
            current["path"] = raw[len("worktree "):]
            current.setdefault("head", "")
            current.setdefault("branch", None)
        elif raw.startswith("HEAD "):
            current["head"] = raw[len("HEAD "):].strip()
        elif raw.startswith("branch "):
            ref = raw[len("branch "):].strip()
            if ref.startswith("refs/heads/"):
                ref = ref[len("refs/heads/"):]
            current["branch"] = ref
        elif raw.strip() == "detached":
            current["branch"] = None
    if current:
        items.append(current)
    return items


try:
    # internal: lifecycle and workbay_handoff_mcp must resolve
    # ``(task_ref, slug)`` to the *same* canonical branch name. The
    # canonical implementation lives in ``workbay_protocol.branch_naming``
    # (the documented "single source of truth" for the feature-branch
    # grammar enforced by the post-checkout / PreToolUse / pre-commit
    # / pre-push gates). Delegating by reference (not copy) is what
    # the internal plan calls "identity is the contract".
    from workbay_protocol.branch_naming import (
        extract_plan_id,
        format_suggested_branch_name,
        select_task_ref_candidate as _select_task_ref_candidate,
    )

    format_branch_name = format_suggested_branch_name
except ImportError:  # pragma: no cover — only hit when running outside the monorepo install
    _select_task_ref_candidate = None  # type: ignore[assignment]

    def format_branch_name(
        task_ref: str | None,
        *,
        slug: str | None = None,
        plan_id: str | None = None,
    ) -> str | None:
        """Stdlib-only fallback when ``workbay_protocol`` is unavailable."""
        if not task_ref:
            return None
        base = f"feature/{task_ref.lower()}"
        if slug:
            base = f"{base}-{slug.lower()}"
        if plan_id:
            base = f"{base}-plan{plan_id.lower()}"
        return base

    _PLAN_ID_RE = re.compile(r"(?:^|[\\/])(\d+)-[^\\/]+$")

    def extract_plan_id(plan_path: str | None) -> str | None:
        if not plan_path:
            return None
        match = _PLAN_ID_RE.search(plan_path)
        if match is None:
            return None
        return match.group(1)


def _candidates_stdlib(branch: str) -> list[str]:
    match = _TASK_REF_RE.match(branch)
    if match is None:
        return []
    full = match.group("task_ref")
    segments = full.split("-")
    candidates: list[str] = []
    for end in range(len(segments), 0, -1):
        candidate = "-".join(segments[:end])
        if not any(ch.isdigit() for ch in candidate):
            break
        candidates.append(candidate)
    return candidates


def derive_task_ref(
    branch: str | None,
    *,
    known_task_refs: Iterable[str] | None = None,
) -> str | None:
    """Return the canonical UPPERCASE task ref for a branch.

    Delegates to ``workbay_protocol.branch_naming.select_task_ref_candidate``
    when the package is importable (production path). The selector picks
    the most-specific registered candidate when ``known_task_refs`` is
    non-empty (and returns ``None`` when it is non-empty but no
    candidate intersects — internal), falling back to the shortest
    digit-bearing prefix only when no registry context is available at
    all (``None`` / empty).

    A stdlib-only fallback is used when ``workbay_protocol`` is
    unavailable (partial checkout / consumer-bootstrap before
    ``uv sync``); behavior is identical.
    """
    if not branch:
        return None
    if _select_task_ref_candidate is not None:
        return _select_task_ref_candidate(branch, known_task_refs=known_task_refs)
    candidates = _candidates_stdlib(branch)
    if not candidates:
        return None
    if known_task_refs is None:
        return candidates[-1].upper()
    known_upper = {ref.upper() for ref in known_task_refs}
    if not known_upper:
        return candidates[-1].upper()
    for candidate in candidates:
        if candidate.upper() in known_upper:
            return candidate.upper()
    return None
