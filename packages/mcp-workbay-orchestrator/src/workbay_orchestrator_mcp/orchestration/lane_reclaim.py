"""internal — lane worktree reclaim safety predicate.

Conditions 1–9. Condition 9 sweeps unaccounted orphans and pins them under
``refs/reclaimed/`` before the verdict may authorise reclaim. A failed probe or
failed preserve refuses so nothing can authorise deletion without preservation.

This module is a pure predicate: it never deletes directories or invokes
``git`` removal subcommands (implementation note owns that path).
"""

from __future__ import annotations

import hashlib
import importlib.util
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

from workbay_handoff_mcp import get_handoff_state, latest_lane_landing
from workbay_handoff_mcp.lanes_api import get_lane, list_lanes
from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (
    WORKBAY_LIFECYCLE_DIR_ENV,
    _git_is_ancestor,
    _lane_branch_contained_in,
    _lifecycle_dir,
    _safe_log,
    record_reclaim_candidate,
)

# Seams re-exported into this module namespace so tests can
# ``mock.patch.object(lane_reclaim, "<name>")`` by name. Normative contract
# from implementation note / test_lane_reclaim.py / test_lane_reclaim_scan.py.
__all__ = [
    "ReclaimVerdict",
    "ReclaimCandidate",
    "ScanResult",
    "NudgeFailure",
    "OrphanScan",
    "lane_worktree_reclaimable",
    "lane_branch_reclaimable",
    "scan_terminal_lanes",
    "nudge_reclaim_candidate",
    "record_reclaim_candidate",
    "_unaccounted_orphans",
    "get_lane",
    "latest_lane_landing",
    "get_handoff_state",
    "list_lanes",
    "_session_live",
    "_worktree_status",
]

_TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})

# Lane / review transport paths: never production content for blob-preservation.
# A review lane is a copy of its implementation branch; deleting it loses nothing
# when those blobs survive elsewhere, even if main never absorbed them.
_ARTIFACT_DIR_PREFIXES = (
    "docs/reviews/",
    "docs/assessments/",
    ".lane-brief/",
    ".review/",
)
_ARTIFACT_BASENAMES = frozenset(
    {
        "LANE-REPORT.md",
        "REVIEW-FINDINGS.md",
        "REVIEW_FINDINGS.md",
        "REVIEW_DIFF.patch",
    }
)
_INTEGRATION_TARGET_NAMES = frozenset({"main", "master"})

# Rebase/sequencer todo: verbs that name a commit (long form + single-letter
# aliases). Verb-anchored only — label/exec/break/update-ref never contribute
# a tip even when their argument text looks like hex.
_TODO_COMMIT_VERBS = frozenset(
    {
        "pick",
        "p",
        "reword",
        "r",
        "edit",
        "e",
        "squash",
        "s",
        "fixup",
        "f",
        "drop",
        "d",
        "merge",
        "m",
        "reset",
        "t",
    }
)

# Optional flags between verb and SHA (git: fixup/merge [-C | -c] <commit>).
# Recognised once for every commit-naming verb rather than per-verb special cases.
_TODO_SHA_FLAGS = frozenset({"-C", "-c"})

_SHA_TOKEN_RE = re.compile(r"^[0-9a-fA-F]{4,40}$")

# Files under rebase-merge/ / rebase-apply/ that may name a commit tip.
_REBASE_HEAD_FILES = (
    "orig-head",
    "onto",
    "stopped-sha",
    "amend",
    "head",
    "rewritten-list",
)


@dataclass
class ReclaimVerdict:
    """Typed reclaim decision: never a bare bool [OBS-08]."""

    reclaimable: bool
    reason: str | None
    observed: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReclaimCandidate:
    """One terminal-on-disk lane evaluated by the Slice-2 scan.

    Stores the whole ``ReclaimVerdict`` so a refusal's ``observed`` map remains
    diagnosable [OBS-06]. ``recorded`` reports whether the ledger write succeeded.
    """

    task_ref: str
    lane_id: str
    worktree_path: str
    verdict: ReclaimVerdict
    recorded: bool


class ScanResult(tuple):
    """Empty-tuple-compatible scan outcome that carries completeness.

    A failed ``list_lanes`` page and a genuinely empty task must not collapse
    onto the same bare ``()`` [CON-05][PLAN0181-S2SCANEMPTY-01]. Older pins
    assert ``candidates == ()`` on the refusal path, so this subclass still
    compares equal to the empty tuple while remaining distinguishable by type,
    public attributes, and ``repr``. Callers may iterate and unpack it exactly
    like a plain tuple.

    ``complete`` answers whether listing finished without a truncated-exit
    refusal (every page answered, totals matched collected length when the
    seam reported an int total). Per-lane evaluation faults and ledger write
    failures are separate questions answered by ``failed_lanes`` and
    ``unrecorded_lanes`` (PLAN0181-S2SCANPARTIAL-01 / PLAN0181-S2RECFALSE-01)
    — they do not flip ``complete``.

    ``snapshot_consistent`` answers a narrower question: whether the collected
    universe is known to come from a single ``list_lanes`` page (one seam
    transaction). Multi-page keyset walks are not snapshot-isolated; equal-
    count membership churn can still yield a never-existed set while
    ``complete`` stays True (PLAN0181-S2GATE3-KEYSET-EQUALCOUNT-CHURN-01).
    Readers that authorise shared-path guards must consult this flag, not
    only ``complete``.
    """

    complete: bool
    refusal_reason: str | None
    failed_lanes: tuple[str, ...]
    unrecorded_lanes: tuple[str, ...]
    snapshot_consistent: bool

    def __new__(
        cls,
        items: tuple = (),
        *,
        complete: bool = True,
        refusal_reason: str | None = None,
        failed_lanes: tuple[str, ...] = (),
        unrecorded_lanes: tuple[str, ...] = (),
        snapshot_consistent: bool = True,
    ) -> ScanResult:
        instance = super().__new__(cls, items)
        instance.complete = complete
        instance.refusal_reason = refusal_reason
        instance.failed_lanes = tuple(failed_lanes)
        instance.unrecorded_lanes = tuple(unrecorded_lanes)
        instance.snapshot_consistent = bool(snapshot_consistent)
        return instance

    def __repr__(self) -> str:
        return (
            f"ScanResult({tuple.__repr__(self)}, "
            f"complete={self.complete!r}, "
            f"refusal_reason={self.refusal_reason!r}, "
            f"failed_lanes={self.failed_lanes!r}, "
            f"unrecorded_lanes={self.unrecorded_lanes!r}, "
            f"snapshot_consistent={self.snapshot_consistent!r})"
        )


@dataclass(frozen=True)
class NudgeFailure:
    """Typed third state for ``nudge_reclaim_candidate`` outages.

    Distinguishes a seam/evaluation failure from a genuine decline (``None``)
    so a transition-site caller can act on an outage rather than waiting for
    the next full sweep (PLAN0181-S2NUDGERET-01).
    """

    lane_id: str
    reason: str


@dataclass
class OrphanScan:
    """Result of the unaccounted-orphan sweep (internal wave B1).

    ``ok`` is False iff some git probe exited non-zero. Empty-but-successful
    probe output is a real empty answer, not a failure — do not route these
    probes through ``_git_stdout``, which collapses both cases to ``None``.
    """

    ok: bool
    failed_probe: str | None
    stderr: str
    commits: tuple[str, ...]
    tips: tuple[str, ...]


@dataclass
class PreserveResult:
    """Result of writing ``refs/reclaimed/`` pins for unaccounted orphans (wave B2).

    ``ok`` is False iff some orphan could not be preserved. ``reason`` is then
    ``orphan_preservation_locked`` (git reported a held ``.lock`` on the failed
    ``update-ref``) or ``orphan_preservation_failed`` (permanent refuse).
    Discrimination uses git's contemporaneous stderr, not a post-hoc disk
    probe of the ``.lock`` path (PLAN0181-S2GATE2-PRESERVE-LOCKRACE-01).
    """

    ok: bool
    reason: str | None
    refs: tuple[str, ...]
    failed_commit: str | None


def _fallback_reclaim_slug(lane_id: str) -> str:
    """Fallback slug when the lane_id cannot be written as a ref component."""
    return "x-" + hashlib.sha1(lane_id.encode()).hexdigest()[:12]


def _reclaim_ref_lock_path(root: Path, slug: str, sha: str) -> Path | None:
    """Path of the loose-ref lock file for ``refs/reclaimed/<slug>/<sha>``.

    Resolves the ref store via ``git rev-parse --git-common-dir`` so the probe
    works when ``root`` is a linked worktree (``.git`` is a file, not a dir).
    Loose refs under ``refs/reclaimed/`` live in the common dir, not the
    per-worktree dir that ``--absolute-git-dir`` returns.

    Returns ``None`` when the common dir cannot be resolved (non-zero exit or
    empty stdout). Kept for diagnostics and test fixtures; preserve refusal
    discrimination no longer depends on a post-hoc ``is_file`` of this path
    (PLAN0181-S2GATE2-PRESERVE-LOCKRACE-01).
    """
    proc = _git_run(root, "rev-parse", "--git-common-dir")
    if proc.returncode != 0:
        return None
    # Empty stdout must fail explicitly: Path("") is PosixPath(".") and would
    # silently treat the process CWD as the ref store (S1B1-REV-10 hardening).
    raw = (proc.stdout or "").strip()
    if not raw:
        return None
    common = Path(raw)
    if not common.is_absolute():
        # Primary checkouts print relative ".git"; resolve against root.
        common = (root / common).resolve()
    # slug may contain path separators (lane_ids with '/'); join as components.
    base = common / "refs" / "reclaimed"
    for part in slug.split("/"):
        if part == "":
            # empty segment (e.g. illegal '') — keep path under reclaimed/
            continue
        base = base / part
    return base / f"{sha}.lock"


def _update_ref_reports_lock_contention(stderr: str | None) -> bool:
    """True when *stderr* from a failed ``update-ref`` reports a held ``.lock``.

    Key on stable substrings only — git wording varies by version. A real held
    lock emits both ``.lock`` and ``File exists`` (e.g. ``Unable to create
    '...ref.lock': File exists``). D/F conflicts and over-long paths also say
    ``cannot lock ref`` but do **not** pair those two tokens; grepping the
    broader phrase alone would mis-label permanent refusals as transient.
    Ambiguous or empty evidence returns False so the caller keeps the
    fail-closed permanent reason (PLAN0181-S2GATE2-PRESERVE-LOCKRACE-01).
    """
    text = stderr or ""
    return ".lock" in text and "File exists" in text


def _preserve_orphans(
    *,
    orchestrator_root: Path | str,
    lane_id: str,
    commits: list[str] | tuple[str, ...],
) -> PreserveResult:
    """Write ``refs/reclaimed/<slug>/<40-hex>`` for every unaccounted orphan.

    The slug is decided by *attempting* the write, never by a predicate:
    at 256 bytes ``check-ref-format`` accepts what ``update-ref`` rejects, so a
    check-ref-format-gated sanitizer strands the lane permanently. Per commit
    (not per lane): one blocked leaf must not force siblings onto the fallback.
    """
    root = Path(orchestrator_root)
    fallback_slug = _fallback_reclaim_slug(lane_id)
    written: list[str] = []

    for sha in commits:
        verbatim_ref = f"refs/reclaimed/{lane_id}/{sha}"
        fallback_ref = f"refs/reclaimed/{fallback_slug}/{sha}"

        verbatim = _git_run(root, "update-ref", verbatim_ref, sha)
        if verbatim.returncode == 0:
            written.append(verbatim_ref)
            continue

        fallback = _git_run(root, "update-ref", fallback_ref, sha)
        if fallback.returncode == 0:
            written.append(fallback_ref)
            continue

        # Both writes failed. Discriminate transient vs permanent from git's
        # own contemporaneous stderr — never from a later is_file() probe of
        # the .lock path, which races both ways (lock released after failure
        # → permanent; unrelated lock created after failure → locked).
        # PLAN0181-S2GATE2-PRESERVE-LOCKRACE-01.
        locked = _update_ref_reports_lock_contention(
            verbatim.stderr
        ) or _update_ref_reports_lock_contention(fallback.stderr)
        if locked:
            return PreserveResult(
                ok=False,
                reason="orphan_preservation_locked",
                refs=tuple(sorted(written)),
                failed_commit=sha,
            )
        return PreserveResult(
            ok=False,
            reason="orphan_preservation_failed",
            refs=tuple(sorted(written)),
            failed_commit=sha,
        )

    return PreserveResult(
        ok=True,
        reason=None,
        refs=tuple(sorted(written)),
        failed_commit=None,
    )


def _worktree_status(worktree: Path | str) -> str | None:
    """Condition-5 cleanliness probe (module-level seam; tests patch by name).

    Returns ``""`` for a clean tree, porcelain text for a dirty one, and
    ``None`` when the tree could not be read. Built on the exit code — not
    ``_git_stdout``, which maps both failure and empty-success to ``None``.
    """
    result = subprocess.run(
        ["git", "-C", str(worktree), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def _git_run(cwd: Path | str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run git with exit code exposed. Empty stdout is not failure."""
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _list_lanes_by_worktree_path(*, worktree_path: str) -> dict[str, Any]:
    """Cross-task worktree-path owners for the shared-path guard.

    Function-local import (PLAN0181-S2GATE2-MODULE-LEVEL-XPKG-IMPORT-01): a
    skewed handoff install that lacks the brand-new symbol must not fail the
    entire ``lane_reclaim`` import surface at load time. Same pattern as the
    handoff imports in ``orchestrator_lanes.py``.
    """
    from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
        list_lanes_by_worktree_path,
    )

    return list_lanes_by_worktree_path(worktree_path=worktree_path)


def _lane_refname(lane_branch: str) -> str:
    if lane_branch.startswith("refs/"):
        return lane_branch
    return f"refs/heads/{lane_branch}"


def _scan_fail(
    probe: str,
    stderr: str,
    tips: tuple[str, ...] = (),
) -> OrphanScan:
    return OrphanScan(
        ok=False,
        failed_probe=probe,
        stderr=stderr or "",
        commits=(),
        tips=tips,
    )


def _resolve_commit(cwd: Path | str, rev: str) -> str | None:
    """Resolve *rev* to a full 40-char commit SHA, or None if it is not a commit.

    Tips fed to ``rev-list --no-walk`` must name commits. Abbreviated sequencer
    SHAs expand here; non-commit tokens (ref names that fail, trees, garbage in
    rebase files) are dropped rather than poisoning the rev-list probe.
    """
    token = rev.strip()
    if not token:
        return None
    proc = _git_run(cwd, "rev-parse", "--verify", f"{token}^{{commit}}")
    if proc.returncode != 0:
        return None
    sha = (proc.stdout or "").strip()
    return sha or None


def _todo_line_sha(line: str) -> str | None:
    """Parse a rebase/sequencer todo line as verb → optional flags → SHA.

    Returns the SHA token when the line names a commit, else None. Unknown
    leading verbs (``exec``, ``label``, ``x``, ``l``, ``break``, …) yield
    None without scanning argument text. Known flags (``-C``/``-c``) are
    skipped; any other non-SHA token after the verb stops the parse with no
    match — unrecognised flags do not fall through to a bare hex scan.
    """
    parts = line.split()
    if not parts:
        return None
    verb = parts[0]
    if verb not in _TODO_COMMIT_VERBS:
        return None
    i = 1
    while i < len(parts) and parts[i] in _TODO_SHA_FLAGS:
        i += 1
    if i >= len(parts):
        return None
    candidate = parts[i]
    if _SHA_TOKEN_RE.fullmatch(candidate) is None:
        return None
    return candidate


def _collect_todo_shas(todo_path: Path, cwd: Path | str, tips: set[str]) -> str | None:
    """Parse a present todo file into *tips*.

    Returns a probe name when the file exists but cannot be read (fail-closed,
    PLAN0181-S2GATE2-COND9-TIPREAD-FAILOPEN-01). Missing files are not callers'
    concern — only call this when ``is_file()`` is True.
    """
    try:
        text = todo_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        # Stable probe token; detail rides in the scan stderr via the caller.
        return f"todo_unreadable|{exc}"
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        sha_token = _todo_line_sha(stripped)
        if sha_token is None:
            continue
        resolved = _resolve_commit(cwd, sha_token)
        if resolved is not None:
            tips.add(resolved)
    return None


def _collect_rebase_and_sequencer_tips(
    worktree: Path | str, tips: set[str]
) -> str | None:
    """Add rebase-merge/rebase-apply heads and sequencer todo SHAs.

    Returns a probe name on hard failure locating the worktree git dir, or when
    a present tip/todo file exists but is unreadable (fail-closed like the
    CHERRY_PICK_HEAD / MERGE_HEAD path — PLAN0181-S2GATE2-COND9-TIPREAD-FAILOPEN-01).
    Missing rebase/sequencer state is normal and not a failure.
    """
    proc = _git_run(worktree, "rev-parse", "--absolute-git-dir")
    if proc.returncode != 0:
        return "absolute_git_dir"
    # Empty stdout must fail explicitly: Path("") is PosixPath(".") and
    # is_dir() is True for the process CWD, which is not the git dir.
    git_dir_raw = (proc.stdout or "").strip()
    if not git_dir_raw:
        return "absolute_git_dir"
    git_dir = Path(git_dir_raw)
    if not git_dir.is_dir():
        return "absolute_git_dir"

    for dirname in ("rebase-merge", "rebase-apply"):
        rebase_dir = git_dir / dirname
        if not rebase_dir.is_dir():
            continue
        for name in _REBASE_HEAD_FILES:
            path = rebase_dir / name
            if not path.is_file():
                continue
            try:
                raw = path.read_text(encoding="utf-8", errors="replace").strip()
            except OSError as exc:
                # Present-but-unreadable is unknown, not empty [SECD-05].
                return f"rebase_tip_unreadable|{dirname}/{name}|{exc}"
            if not raw:
                continue
            # First token/line only — these files are single-value or SHA lists.
            for token in raw.replace("\n", " ").split():
                resolved = _resolve_commit(worktree, token)
                if resolved is not None:
                    tips.add(resolved)
        todo = rebase_dir / "git-rebase-todo"
        if todo.is_file():
            todo_err = _collect_todo_shas(todo, worktree, tips)
            if todo_err is not None:
                return todo_err

    sequencer_todo = git_dir / "sequencer" / "todo"
    if sequencer_todo.is_file():
        todo_err = _collect_todo_shas(sequencer_todo, worktree, tips)
        if todo_err is not None:
            return todo_err
    return None


def _unaccounted_orphans(
    *,
    orchestrator_root: Path | str,
    worktree_path: Path | str,
    lane_branch: str,
    task_branch: str,
) -> OrphanScan:
    """Sweep tips for commits unaccounted after lane-branch deletion (wave B1).

    Builds the tip set (live pointers + whole reflogs; no anchor, no cut;
    FETCH_HEAD deliberately excluded), subtracts survivors excluding the lane
    branch, then applies the churn filter with merges exempt. Writes no ref and
    removes nothing. Condition 9's sentinel is intentionally not wired here.
    """
    root = Path(orchestrator_root)
    worktree = Path(worktree_path)
    tip_set: set[str] = set()

    # --- live pointers (required) ------------------------------------------
    head_proc = _git_run(worktree, "rev-parse", "HEAD")
    if head_proc.returncode != 0:
        return _scan_fail("rev_parse_head", head_proc.stderr or head_proc.stdout)
    head_sha = (head_proc.stdout or "").strip()
    if head_sha:
        tip_set.add(head_sha)

    lane_proc = _git_run(root, "rev-parse", lane_branch)
    if lane_proc.returncode != 0:
        return _scan_fail(
            "rev_parse_lane_branch",
            lane_proc.stderr or lane_proc.stdout,
            tips=tuple(sorted(tip_set)),
        )
    lane_sha = (lane_proc.stdout or "").strip()
    if lane_sha:
        tip_set.add(lane_sha)

    # Per-worktree namespaces — only visible from inside the lane worktree.
    wt_refs = _git_run(
        worktree,
        "for-each-ref",
        "--format=%(objectname)",
        "refs/worktree/",
        "refs/bisect/",
    )
    if wt_refs.returncode != 0:
        return _scan_fail(
            "for_each_ref_worktree",
            wt_refs.stderr or wt_refs.stdout,
            tips=tuple(sorted(tip_set)),
        )
    for line in (wt_refs.stdout or "").splitlines():
        sha = line.strip()
        if sha:
            tip_set.add(sha)

    # In-progress operation pointers. File-absent is not a failure; file-present
    # but unresolvable must refuse — rev-parse exits 128 for both cases, so the
    # file check is what separates them (fail-closed for the structure between
    # reclaim and destroyed work).
    abs_git_proc = _git_run(worktree, "rev-parse", "--absolute-git-dir")
    if abs_git_proc.returncode != 0:
        return _scan_fail(
            "absolute_git_dir",
            abs_git_proc.stderr or abs_git_proc.stdout,
            tips=tuple(sorted(tip_set)),
        )
    # Empty stdout must fail explicitly: Path("") is PosixPath(".") and
    # is_dir() is True for the process CWD, which is not the git dir.
    git_dir_raw = (abs_git_proc.stdout or "").strip()
    if not git_dir_raw:
        return _scan_fail(
            "absolute_git_dir",
            abs_git_proc.stderr or abs_git_proc.stdout or "absolute-git-dir empty",
            tips=tuple(sorted(tip_set)),
        )
    git_dir = Path(git_dir_raw)
    if not git_dir.is_dir():
        return _scan_fail(
            "absolute_git_dir",
            abs_git_proc.stderr or abs_git_proc.stdout or "absolute-git-dir unreadable",
            tips=tuple(sorted(tip_set)),
        )

    for pointer in ("CHERRY_PICK_HEAD", "MERGE_HEAD", "REVERT_HEAD", "ORIG_HEAD"):
        # ORIG_HEAD is unconditional when present; no reflog-ordinal scoping.
        if not (git_dir / pointer).is_file():
            continue
        ptr = _git_run(worktree, "rev-parse", pointer)
        if ptr.returncode != 0:
            return _scan_fail(
                f"pointer_{pointer.lower()}",
                ptr.stderr or ptr.stdout,
                tips=tuple(sorted(tip_set)),
            )
        sha = (ptr.stdout or "").strip()
        if sha:
            tip_set.add(sha)

    abs_err = _collect_rebase_and_sequencer_tips(worktree, tip_set)
    if abs_err is not None:
        # Fails on absolute-git-dir problems OR present-but-unreadable tip/todo
        # files (PLAN0181-S2GATE2-COND9-TIPREAD-FAILOPEN-01). Missing rebase
        # state is fine. Probe names may carry detail after ``|``; strip it for
        # the stable failed_probe token and keep the full string as stderr.
        if abs_err == "absolute_git_dir":
            abs_proc = _git_run(worktree, "rev-parse", "--absolute-git-dir")
            probe = abs_err
            stderr = abs_proc.stderr or abs_proc.stdout or "absolute-git-dir unreadable"
        else:
            probe = abs_err.split("|", 1)[0]
            stderr = abs_err
        return _scan_fail(
            probe,
            stderr,
            tips=tuple(sorted(tip_set)),
        )

    # --- historical pointers: whole reflog, no cut ---------------------------
    head_reflog = _git_run(worktree, "reflog", "show", "--format=%H", "HEAD")
    if head_reflog.returncode != 0:
        return _scan_fail(
            "reflog_head",
            head_reflog.stderr or head_reflog.stdout,
            tips=tuple(sorted(tip_set)),
        )
    for line in (head_reflog.stdout or "").splitlines():
        sha = line.strip()
        if sha:
            tip_set.add(sha)

    lane_reflog = _git_run(root, "reflog", "show", "--format=%H", lane_branch)
    if lane_reflog.returncode != 0:
        return _scan_fail(
            "reflog_lane_branch",
            lane_reflog.stderr or lane_reflog.stdout,
            tips=tuple(sorted(tip_set)),
        )
    for line in (lane_reflog.stdout or "").splitlines():
        sha = line.strip()
        if sha:
            tip_set.add(sha)

    # FETCH_HEAD deliberately excluded: not a gc root; a FETCH_HEAD-only commit
    # does not survive ``gc --prune=now --aggressive`` even while the file remains.

    tips = tuple(sorted(tip_set))

    # --- survivors: every ref except the lane branch -------------------------
    survivors_proc = _git_run(root, "for-each-ref", "--format=%(refname)")
    if survivors_proc.returncode != 0:
        return _scan_fail(
            "for_each_ref_survivors",
            survivors_proc.stderr or survivors_proc.stdout,
            tips=tips,
        )
    lane_ref = _lane_refname(lane_branch)
    survivors = [
        ref
        for ref in (survivors_proc.stdout or "").splitlines()
        if ref.strip() and ref.strip() != lane_ref
    ]

    # --- raw orphan set via rev-list --no-walk tips --not survivors -----------
    if not tips:
        # HEAD + lane branch always contribute when probes succeed; empty is
        # defensive only.
        return OrphanScan(
            ok=True, failed_probe=None, stderr="", commits=(), tips=tips
        )

    rev_list_cmd = ["rev-list", "--no-walk", *tips, "--not", *survivors]
    rev_list = _git_run(root, *rev_list_cmd)
    if rev_list.returncode != 0:
        return _scan_fail(
            "rev_list",
            rev_list.stderr or rev_list.stdout,
            tips=tips,
        )
    raw_orphans = [
        line.strip()
        for line in (rev_list.stdout or "").splitlines()
        if line.strip()
    ]

    # --- churn filter: merges exempt; else keep iff git cherry scores '+' ----
    kept: list[str] = []
    for commit in raw_orphans:
        parents_proc = _git_run(root, "rev-list", "--no-walk", "--parents", commit)
        if parents_proc.returncode != 0:
            return _scan_fail(
                "rev_list_parents",
                parents_proc.stderr or parents_proc.stdout,
                tips=tips,
            )
        parts = (parents_proc.stdout or "").strip().split()
        # parts[0] is the commit itself; remaining tokens are parents.
        if len(parts) > 2:
            # Merge: NEVER filtered — git cherry emits no line for merges, so
            # any(startswith '+') would wrongly drop unique conflict resolutions.
            kept.append(commit)
            continue

        cherry = _git_run(root, "cherry", task_branch, commit)
        if cherry.returncode != 0:
            return _scan_fail(
                "cherry",
                cherry.stderr or cherry.stdout,
                tips=tips,
            )
        if any(
            line.startswith("+") for line in (cherry.stdout or "").splitlines()
        ):
            kept.append(commit)

    # Full 40-char SHAs, sorted — preservation (B2) names refs by these.
    commits = tuple(sorted({c for c in kept if len(c) == 40}))
    return OrphanScan(
        ok=True,
        failed_probe=None,
        stderr="",
        commits=commits,
        tips=tips,
    )


# Cache of path-loaded session_heartbeat modules keyed by resolved absolute path
# of session_heartbeat.py — never by bare name or root — so multi-repo probes in
# one long-lived process cannot collide.
_SESSION_HEARTBEAT_BY_PATH: dict[str, ModuleType] = {}


def _load_session_heartbeat(lifecycle_dir: Path) -> ModuleType:
    """Load root-resolved session_heartbeat by explicit file path (no sys.path).

    The real lifecycle ``session_heartbeat`` does a bare ``import resolver`` at
    module scope. That absolute import is process-global; the window that binds
    ``sys.modules["resolver"]`` is therefore bounded and restored rather than
    permanent. Stubs that do not import ``resolver`` still load when only
    ``session_heartbeat.py`` is present (resolver is one level deep and optional
    until exec needs it).
    """
    heartbeat_path = (lifecycle_dir / "session_heartbeat.py").resolve()
    resolver_path = (lifecycle_dir / "resolver.py").resolve()
    if not heartbeat_path.is_file():
        raise FileNotFoundError(f"session_heartbeat.py missing under {lifecycle_dir}")

    cache_key = str(heartbeat_path)
    cached = _SESSION_HEARTBEAT_BY_PATH.get(cache_key)
    if cached is not None:
        return cached

    # Unique module names derived from the resolved absolute path so two
    # repositories' scanners cannot collide in sys.modules (the bare-name bug).
    path_digest = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
    resolver_mod_name = f"_workbay_lane_reclaim_resolver_{path_digest}"
    heartbeat_mod_name = f"_workbay_lane_reclaim_session_heartbeat_{path_digest}"

    resolver_module: ModuleType | None = None
    if resolver_path.is_file():
        resolver_spec = importlib.util.spec_from_file_location(
            resolver_mod_name, resolver_path
        )
        if resolver_spec is None or resolver_spec.loader is None:
            raise ImportError(f"cannot load resolver from {resolver_path}")
        resolver_module = importlib.util.module_from_spec(resolver_spec)
        resolver_spec.loader.exec_module(resolver_module)

    heartbeat_spec = importlib.util.spec_from_file_location(
        heartbeat_mod_name, heartbeat_path
    )
    if heartbeat_spec is None or heartbeat_spec.loader is None:
        raise ImportError(f"cannot load session_heartbeat from {heartbeat_path}")
    heartbeat_module = importlib.util.module_from_spec(heartbeat_spec)

    # Bounded window: when this repo ships resolver.py, bind it under the bare
    # name only while exec_module(session_heartbeat) runs, then restore (or
    # delete) the previous binding. A concurrent bare ``import resolver`` in
    # another thread during this window would see this repository's copy;
    # accepted because the alternative is the prior permanent mis-binding via
    # sys.path + bare import.
    previous_resolver = sys.modules.get("resolver")
    had_resolver = "resolver" in sys.modules
    bound_resolver = resolver_module is not None
    if bound_resolver:
        sys.modules["resolver"] = resolver_module
    try:
        # Register under the unique name only — never bare "session_heartbeat".
        sys.modules[heartbeat_mod_name] = heartbeat_module
        heartbeat_spec.loader.exec_module(heartbeat_module)
    finally:
        if bound_resolver:
            if had_resolver:
                sys.modules["resolver"] = previous_resolver  # type: ignore[assignment]
            else:
                sys.modules.pop("resolver", None)

    _SESSION_HEARTBEAT_BY_PATH[cache_key] = heartbeat_module
    return heartbeat_module


def _session_live(*, orchestrator_root: Path | str, worktree: Path | str) -> bool:
    """Condition-6 liveness probe (module-level seam; tests patch by name).

    Mirrors the heartbeat-first half of ``task_finish._remove_worktree``: a live
    session under the worktree blocks reclaim. Fail closed to the documented
    safe state when the probe cannot run [SECD-05]. Resolves
    ``session_heartbeat`` via ``orchestrator_lanes._lifecycle_dir`` (honours
    ``WORKBAY_LIFECYCLE_DIR``); import is lazy so pins can prove the unreachable
    case. Probe failures that keep the session path alive still land on stderr
    [AGT-10].
    """
    try:
        lifecycle_dir = _lifecycle_dir(Path(orchestrator_root))
        if lifecycle_dir is None:
            sys.stderr.write(
                "orchestrator: lifecycle session probe entry point not found "
                f"(set {WORKBAY_LIFECYCLE_DIR_ENV} or add scripts/workbay_lifecycle "
                f"under {orchestrator_root}); refusing reclaim until liveness "
                f"can be established for worktree {worktree}\n"
            )
            return True

        heartbeat_path = lifecycle_dir / "session_heartbeat.py"
        if not heartbeat_path.is_file():
            # Same fail-closed direction as missing lifecycle dir [SECD-05];
            # name the missing file so operators can act [AGT-10].
            sys.stderr.write(
                "orchestrator: session probe file session_heartbeat.py not found "
                f"under {lifecycle_dir}; refusing reclaim until liveness can be "
                f"established for worktree {worktree}\n"
            )
            return True

        session_heartbeat = _load_session_heartbeat(lifecycle_dir)

        # repo arg is retained for call-site compatibility; scan is worktree-rooted.
        return bool(
            session_heartbeat.worktree_has_live_session(
                Path(orchestrator_root), str(worktree)
            )
        )
    except Exception as exc:
        # Unknown liveness is not "no session" — refuse reclaim, and degrade
        # loudly so the cause is written to stderr [AGT-10].
        sys.stderr.write(
            f"orchestrator: session liveness probe failed ({exc!r}); "
            f"refusing reclaim for worktree {worktree}\n"
        )
        return True


def _refuse(reason: str, observed: dict[str, Any]) -> ReclaimVerdict:
    return ReclaimVerdict(reclaimable=False, reason=reason, observed=observed)


def _short_branch_name(branch: str) -> str:
    """Strip a ``refs/heads/`` or ``refs/remotes/`` prefix when present."""
    if branch.startswith("refs/heads/"):
        return branch[len("refs/heads/") :]
    if branch.startswith("refs/remotes/"):
        # Keep remote/name form (e.g. origin/feature) — git's short remote-tracking name.
        return branch[len("refs/remotes/") :]
    return branch


def _is_lane_artifact_path(path: str) -> bool:
    """True when *path* is review/lane transport, not production content."""
    # removeprefix only, never lstrip('./'): lstrip takes a character *set*, so
    # ".review/x.md" becomes "review/x.md" and never matches the ".review/" prefix.
    norm = path.replace("\\", "/").removeprefix("./")
    for prefix in _ARTIFACT_DIR_PREFIXES:
        if norm == prefix.rstrip("/") or norm.startswith(prefix):
            return True
    return Path(norm).name in _ARTIFACT_BASENAMES


def _worktrees_checking_out_branch(
    root: Path, branch: str
) -> list[str] | None:
    """Paths of registered worktrees checking out *branch*, or None if unreadable.

    Reuses the porcelain worktree listing [CON-05]: non-zero exit is unknown,
    not an empty checkout set.
    """
    result = _git_run(root, "worktree", "list", "--porcelain")
    if result.returncode != 0:
        return None
    want = _lane_refname(branch)
    want_short = _short_branch_name(branch)
    checked_out: list[str] = []
    current_path: str | None = None
    current_branch: str | None = None

    def _flush() -> None:
        nonlocal current_path, current_branch
        if current_path is not None and current_branch is not None:
            short = _short_branch_name(current_branch)
            if current_branch == want or short == want_short:
                checked_out.append(current_path)
        current_path = None
        current_branch = None

    for line in (result.stdout or "").splitlines():
        if line.startswith("worktree "):
            _flush()
            current_path = line[len("worktree ") :]
            current_branch = None
        elif line.startswith("branch "):
            current_branch = line[len("branch ") :]
        elif line.strip() == "":
            _flush()
    _flush()
    return checked_out


def _branch_introduced_paths(
    root: Path, *, integration_ref: str, branch: str
) -> tuple[list[str] | None, str]:
    """Files the branch introduced vs its merge-base with *integration_ref*.

    Three-dot diff is merge-base..branch — the branch's own changes. Only
    *added* paths (``--diff-filter=A``) count as introduced: a modified file
    already exists on the integration side and is not a branch-only blob
    introduction. Null-delimited (``-z``) so space/quotepath names parse
    correctly. Returns ``(paths, stderr)`` where *paths* is None on probe
    failure (never collapse failure with a genuine empty diff [CON-05][RLSE-05]).
    """
    proc = _git_run(
        root,
        "diff",
        "-z",
        "--name-only",
        "--diff-filter=A",
        f"{integration_ref}...{branch}",
    )
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout or "").strip()
    # -z: paths separated by NUL; git does not C-quote under -z.
    paths = [p for p in (proc.stdout or "").split("\0") if p]
    return paths, ""


def _path_blob_absent_error(stderr: str) -> bool:
    """True when rev-parse/cat-file reports a conclusive missing path (not a probe error)."""
    text = (stderr or "").lower()
    # git rev-parse <ref>:<path> — path not in tree
    if "does not exist" in text:
        return True
    # git: "path 'x' exists on disk, but not in '<ref>'"
    if "exists on disk, but not in" in text:
        return True
    return False


def _path_blob_at(
    root: Path, ref: str, path: str
) -> tuple[str | None, str | None]:
    """Resolve ``ref:path`` to a blob SHA.

    Returns ``(sha, None)`` on success, ``(None, None)`` when the path is
    conclusively absent at *ref* (expected answer, not a probe failure), and
    ``(None, stderr)`` when the probe itself errors [OBS-08].
    """
    proc = _git_run(root, "rev-parse", f"{ref}:{path}")
    if proc.returncode == 0:
        sha = (proc.stdout or "").strip()
        return (sha or None), None
    err = (proc.stderr or proc.stdout or "").strip()
    if _path_blob_absent_error(err):
        return None, None
    return None, err


def _path_blob_index(root: Path, ref: str) -> dict[str, str] | None:
    """Map path → blob SHA for *ref*, or None when the ls-tree probe fails.

    Uses null-delimited ``ls-tree -z`` so paths with spaces or characters that
    core.quotepath would C-quote still key correctly.
    """
    proc = _git_run(root, "ls-tree", "-r", "-z", ref)
    if proc.returncode != 0:
        return None
    index: dict[str, str] = {}
    for entry in (proc.stdout or "").split("\0"):
        if not entry or "\t" not in entry:
            continue
        # <mode> SP <type> SP <object> TAB <file>
        meta, path = entry.split("\t", 1)
        parts = meta.split()
        if len(parts) < 3:
            continue
        obj_type, obj_sha = parts[1], parts[2]
        if obj_type != "blob":
            continue
        index[path] = obj_sha
    return index


def _survivor_blob_indexes(
    root: Path, *, branch: str, integration_ref: str
) -> tuple[dict[str, set[str]] | None, list[str], list[str], bool]:
    """Build path → blob-SHA sets across surviving local refs.

    Candidates are the integration ref plus every local branch except the one
    under evaluation. Returns
    ``(index, candidate_refs, failed_refs, degraded)``.

    *index* is None only when the for-each-ref probe itself fails [CON-05].
    When any candidate ls-tree fails, *degraded* is True and *failed_refs*
    lists them — callers must refuse reclaim rather than silently continue on
    a partial index [RES-07][OBS-08].
    """
    heads = _git_run(
        root, "for-each-ref", "--format=%(refname)", "refs/heads/"
    )
    if heads.returncode != 0:
        return None, [], [], False
    branch_ref = _lane_refname(branch)
    branch_short = _short_branch_name(branch)
    candidates: list[str] = []
    seen: set[str] = set()
    for raw in (integration_ref, *[
        line.strip() for line in (heads.stdout or "").splitlines() if line.strip()
    ]):
        ref = raw.strip()
        if not ref:
            continue
        short = _short_branch_name(ref)
        if ref == branch_ref or short == branch_short:
            continue
        key = _lane_refname(ref) if not ref.startswith("refs/") else ref
        if key in seen or ref in seen:
            continue
        seen.add(key)
        seen.add(ref)
        candidates.append(ref)

    combined: dict[str, set[str]] = {}
    failed: list[str] = []
    for ref in candidates:
        index = _path_blob_index(root, ref)
        if index is None:
            # Partial index is degraded — never treat as a complete answer.
            failed.append(ref)
            continue
        for path, sha in index.items():
            combined.setdefault(path, set()).add(sha)
    degraded = bool(failed)
    return combined, candidates, failed, degraded


def _branch_unreachable_commits(
    root: Path, branch: str
) -> OrphanScan:
    """Commits reachable only from *branch* (would vanish if it were deleted).

    Empty successful rev-list is a real empty answer, not a probe failure
    [CON-05][RLSE-05].
    """
    survivors_proc = _git_run(root, "for-each-ref", "--format=%(refname)")
    if survivors_proc.returncode != 0:
        return _scan_fail(
            "for_each_ref_survivors",
            survivors_proc.stderr or survivors_proc.stdout,
        )
    lane_ref = _lane_refname(branch)
    survivors = [
        ref.strip()
        for ref in (survivors_proc.stdout or "").splitlines()
        if ref.strip() and ref.strip() != lane_ref
    ]
    # rev-list <branch> --not <survivors>: commits that become unreachable.
    cmd = ["rev-list", branch, "--not", *survivors] if survivors else ["rev-list", branch]
    rev_list = _git_run(root, *cmd)
    if rev_list.returncode != 0:
        return _scan_fail(
            "rev_list_branch_orphans",
            rev_list.stderr or rev_list.stdout,
        )
    commits = tuple(
        sorted(
            {
                line.strip()
                for line in (rev_list.stdout or "").splitlines()
                if line.strip() and len(line.strip()) == 40
            }
        )
    )
    return OrphanScan(
        ok=True,
        failed_probe=None,
        stderr="",
        commits=commits,
        tips=(),
    )


def _resolve_lane_worktree_path(
    orchestrator_root: Path | str,
    worktree_path: Path | str,
) -> Path:
    """Resolve a lane ``worktree_path`` against ``orchestrator_root``, never CWD.

    Absolute paths keep their meaning; relative paths join to
    ``orchestrator_root``. Single chokepoint used by the scan-target filter,
    the reclaim predicate, and the shared-path equality guard
    (PLAN0181-S2GATE2-RELPATH-CWD-SPLIT-01).
    """
    raw = Path(str(worktree_path))
    if raw.is_absolute():
        return raw
    return Path(orchestrator_root) / raw


def _paths_equal(
    a: Path | str | None,
    b: Path | str | None,
    *,
    orchestrator_root: Path | str,
) -> bool:
    """True when both paths name the same directory under *orchestrator_root*.

    Relative spellings on either side resolve against ``orchestrator_root``
    so mixed absolute/relative rows that share a disk path still match
    (PLAN0181-S2GATE2-RELPATH-CWD-SPLIT-01). Process CWD is never consulted.
    """
    if a is None or b is None:
        return False
    try:
        left = _resolve_lane_worktree_path(orchestrator_root, a)
        right = _resolve_lane_worktree_path(orchestrator_root, b)
        return left.resolve() == right.resolve()
    except OSError:
        return str(a) == str(b)


def _seam_envelope_ok(env: Any) -> bool:
    """True only when the seam reported an explicit healthy envelope.

    Missing ``ok`` is treated as failure (fail-safe default [SECD-05]): healthy
    seams in this stack always carry ``ok: True``, and an envelope that omits
    the flag cannot be distinguished from a partial/stale payload.
    """
    return isinstance(env, dict) and env.get("ok") is True


def _registered_worktree_paths(root: Path) -> set[Path] | None:
    """Return resolved paths from ``git worktree list --porcelain``, or None if unreadable."""
    result = subprocess.run(
        ["git", "-C", str(root), "worktree", "list", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    paths: set[Path] = set()
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            raw = line[len("worktree ") :]
            try:
                paths.add(Path(raw).resolve())
            except OSError:
                paths.add(Path(raw))
    return paths


def lane_worktree_reclaimable(
    *,
    orchestrator_root: Path | str,
    task_ref: str,
    lane_id: str,
    sibling_lanes: list[Any] | None = None,
) -> ReclaimVerdict:
    """Return whether the lane worktree may be reclaimed (conditions 1–9).

    Evaluation order is fixed 1 → 9; first refusal wins as ``reason``. Condition 9
    joins the orphan sweep to ``refs/reclaimed/`` preservation: a failed probe or
    failed preserve refuses; only a successful empty or fully-preserved sweep
    returns ``reclaimable=True``.

    ``sibling_lanes`` is optional [API-09]. When omitted (``None``), the
    predicate pages ``list_lanes`` itself for within-task path equality —
    the historical default, so every existing caller is unaffected. When the
    caller already holds the full sibling universe for this ``task_ref``
    (e.g. ``scan_terminal_lanes`` after a complete ``_list_all_lanes``),
    pass that list to skip the N+1 re-page [RES-12][RES-09].

    Sentinel discipline [RLSE-05]: ``None`` means "not supplied, self-page";
    an empty list means "supplied empty universe" and runs the within-task
    guard against zero siblings. Collapsing those two would silently
    disable the shared-path guard for every scanned lane. The caller that
    supplies a list owns the completeness obligation for the same
    ``task_ref`` — this function does not re-verify the list against the DB.

    Cross-task path owners are loaded separately via
    ``list_lanes_by_worktree_path`` (PLAN0181-S2GATE2-SHAREDPATH-SCOPE-01):
    task-scoped ``list_lanes`` cannot see another task's registration of the
    same ``worktree_path``, and the schema only UNIQUE(task_ref, lane_id).
    """
    root = Path(orchestrator_root)
    observed: dict[str, Any] = {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "orchestrator_root": str(root),
    }

    # -- load seams ----------------------------------------------------------
    lane_env = get_lane(lane_id=lane_id, task_ref=task_ref)
    landing_env = latest_lane_landing(lane_id=lane_id, task_ref=task_ref)
    handoff_env = get_handoff_state(task_ref=task_ref)
    # Sibling universe: caller-supplied list skips the re-page [RES-12];
    # None falls back to self-paging. Do not treat None as [].
    if sibling_lanes is None:
        # Page list_lanes fully — the shared-path guard must not miss siblings
        # past the default limit=100 page the same way the scan must not.
        sibling_lanes, _lanes_env = _list_all_lanes(task_ref=task_ref)
    # else: honour the supplied universe (including empty) without re-paging.

    # Failed envelopes must not be trusted even when their payload parses.
    for seam_name, env in (
        ("get_lane", lane_env),
        ("latest_lane_landing", landing_env),
        ("get_handoff_state", handoff_env),
    ):
        if not _seam_envelope_ok(env):
            observed["failed_seam"] = seam_name
            return _refuse("seam_unavailable", observed)

    # Sibling list is the shared-path guard's only input. An unusable list is
    # not "no siblings" — refuse rather than silently disable the guard.
    # Only the self-paging path can produce None here; a caller-supplied
    # list is already a concrete universe (possibly empty).
    if sibling_lanes is None:
        observed["failed_seam"] = "list_lanes"
        return _refuse("lane_list_unavailable", observed)
    observed["sibling_lane_count"] = len(sibling_lanes)

    lane_data = lane_env.get("data") if isinstance(lane_env, dict) else None
    lane = (
        lane_data.get("lane")
        if isinstance(lane_data, dict) and isinstance(lane_data.get("lane"), dict)
        else {}
    )
    status = lane.get("status") if isinstance(lane, dict) else None
    worktree_path_raw = lane.get("worktree_path") if isinstance(lane, dict) else None
    branch = lane.get("branch") if isinstance(lane, dict) else None
    observed["status"] = status
    observed["worktree_path"] = worktree_path_raw
    observed["branch"] = branch

    landing_data = landing_env.get("data") if isinstance(landing_env, dict) else None
    landing = (
        landing_data.get("landing")
        if isinstance(landing_data, dict)
        else None
    )
    observed["landing"] = landing
    landing_sha = (
        landing.get("commit_sha")
        if isinstance(landing, dict)
        else None
    )
    observed["landing_commit_sha"] = landing_sha

    handoff_data = handoff_env.get("data") if isinstance(handoff_env, dict) else None
    active = (
        handoff_data.get("active")
        if isinstance(handoff_data, dict) and isinstance(handoff_data.get("active"), dict)
        else {}
    )
    task_branch_tip = active.get("target_branch") if isinstance(active, dict) else None
    target_worktree_path = (
        active.get("target_worktree_path") if isinstance(active, dict) else None
    )
    observed["target_branch"] = task_branch_tip
    observed["target_worktree_path"] = target_worktree_path

    # -- 1: terminal status --------------------------------------------------
    if status not in _TERMINAL_STATUSES:
        return _refuse("not_terminal", observed)

    # -- 2: landing record exists (existence only) ---------------------------
    if landing is None:
        return _refuse("no_landing_record", observed)

    # -- 3: landing SHA is ancestor of task tip ------------------------------
    # Must use _git_is_ancestor (exit-code success, empty stdout). Building on
    # _git_stdout maps success to None and refuses every lane forever.
    if not landing_sha or not task_branch_tip:
        observed["landing_is_ancestor"] = None
        return _refuse("landing_not_ancestor", observed)
    is_anc = _git_is_ancestor(root, str(landing_sha), str(task_branch_tip))
    observed["landing_is_ancestor"] = is_anc
    if not is_anc:
        return _refuse("landing_not_ancestor", observed)

    # -- 4: lane branch fully contained in task tip --------------------------
    if not branch or not task_branch_tip:
        observed["lane_branch_contained"] = None
        return _refuse("unmerged_commits", observed)
    contained = _lane_branch_contained_in(root, str(task_branch_tip), str(branch))
    observed["lane_branch_contained"] = contained
    # None = unresolvable refs → fail closed to the documented safe state [SECD-05]
    if contained is not True:
        return _refuse("unmerged_commits", observed)

    # -- 5: worktree clean (tracked AND untracked via status --porcelain) ----
    # Relative worktree_path joins to orchestrator_root — never process CWD —
    # so the probe tree matches the scan-target path (RELPATH-CWD-SPLIT-01).
    worktree: Path | None = None
    if isinstance(worktree_path_raw, str) and worktree_path_raw.strip():
        worktree = _resolve_lane_worktree_path(root, worktree_path_raw)
    porcelain: str | None = None
    if worktree is not None:
        porcelain = _worktree_status(worktree)
    observed["status_porcelain"] = porcelain
    if worktree is not None and porcelain is None:
        return _refuse("status_unreadable", observed)
    if porcelain:
        return _refuse("dirty_tree", observed)

    # -- 6: no live session --------------------------------------------------
    session_live = False
    if worktree is not None:
        session_live = bool(
            _session_live(orchestrator_root=root, worktree=worktree)
        )
    observed["session_live"] = session_live
    if session_live:
        return _refuse("session_live", observed)

    # -- 7: path safety ------------------------------------------------------
    path_ok = True
    path_notes: list[str] = []
    if worktree is None:
        path_ok = False
        path_notes.append("missing_worktree_path")
    else:
        try:
            resolved = worktree.resolve()
            observed["worktree_resolved"] = str(resolved)
            if not worktree.exists():
                path_ok = False
                path_notes.append("path_missing")
            if _paths_equal(resolved, root, orchestrator_root=root):
                path_ok = False
                path_notes.append("is_primary_root")
            # Allow-list rather than deny-list: require a registered worktree of
            # this repository, not merely "exists and is not the root". A
            # primary-tree subdirectory would otherwise pass the clean and
            # contained gates by answering from the primary .git, and an
            # unreadable listing denies rather than admits [SECD-05].
            registered = _registered_worktree_paths(root)
            observed["registered_worktree_probe"] = (
                None if registered is None else sorted(str(p) for p in registered)
            )
            if registered is None:
                path_ok = False
                path_notes.append("worktree_list_unreadable")
            elif resolved not in registered:
                path_ok = False
                path_notes.append("not_registered_worktree")
        except OSError as exc:
            path_ok = False
            path_notes.append(f"resolve_error:{exc}")
            observed["worktree_resolve_error"] = str(exc)

        if target_worktree_path and _paths_equal(
            worktree, target_worktree_path, orchestrator_root=root
        ):
            path_ok = False
            path_notes.append("is_task_target_worktree")

        # Cross-task path owners (PLAN0181-S2GATE2-SHAREDPATH-SCOPE-01).
        # Task-scoped sibling_lanes cannot see another task's registration of
        # the same worktree_path; UNIQUE is only (task_ref, lane_id).
        path_share_scope = "task_scoped"
        if isinstance(worktree_path_raw, str) and worktree_path_raw.strip():
            try:
                cross_env = _list_lanes_by_worktree_path(
                    worktree_path=worktree_path_raw.strip()
                )
            except (RuntimeError, ImportError, AttributeError) as exc:
                # Hermetic unit tests and skewed handoff installs (missing
                # brand-new symbol via the function-local import) cannot prove
                # a global empty universe. Scope is recorded so a reader never
                # confuses task-local emptiness with global emptiness.
                observed["cross_task_path_lookup_error"] = str(exc)
                path_share_scope = "task_scoped_runtime_unavailable"
            else:
                if not _seam_envelope_ok(cross_env):
                    path_ok = False
                    path_notes.append("cross_task_path_lookup_failed")
                else:
                    cross_data = (
                        cross_env.get("data")
                        if isinstance(cross_env, dict)
                        else None
                    )
                    cross_lanes = (
                        cross_data.get("lanes")
                        if isinstance(cross_data, dict)
                        else None
                    )
                    if not isinstance(cross_lanes, list):
                        path_ok = False
                        path_notes.append("cross_task_path_lookup_malformed")
                    else:
                        # Handoff matches worktree_path by verbatim string
                        # equality only. Alternate spellings of the same
                        # directory (relative vs absolute, trailing slash,
                        # symlink) are invisible here; do not claim a true
                        # global universe was enumerated.
                        path_share_scope = "global_exact_string_only"
                        observed["cross_task_path_owner_count"] = len(
                            cross_lanes
                        )
                        for owner_index, owner in enumerate(cross_lanes):
                            if not isinstance(owner, dict):
                                path_ok = False
                                path_notes.append(
                                    f"malformed_path_owner:{owner_index}"
                                )
                                break
                            owner_id = owner.get("lane_id")
                            owner_task = owner.get("task_ref")
                            if (
                                owner_id == lane_id
                                and owner_task == task_ref
                            ):
                                continue
                            path_ok = False
                            if (
                                owner_task is not None
                                and owner_task != task_ref
                            ):
                                path_notes.append(
                                    f"shared_with_lane:{owner_id}"
                                    f"@task:{owner_task}"
                                )
                            else:
                                path_notes.append(
                                    f"shared_with_lane:{owner_id}"
                                )
                            break
        observed["path_share_scope"] = path_share_scope

        for sib_index, sib in enumerate(sibling_lanes):
            # A degraded sibling universe cannot prove path uniqueness — fail
            # closed rather than skipping the only row that may share the path
            # (PLAN0181-S2GATE2-NONDICT-SIBLING-SKIPPED-01). Also covers
            # within-task spelling variants that the exact-path cross-task
            # reader does not see.
            if not isinstance(sib, dict):
                path_ok = False
                path_notes.append(f"malformed_sibling:{sib_index}")
                break
            sib_id = sib.get("lane_id")
            if sib_id == lane_id:
                continue
            sib_path = sib.get("worktree_path")
            if sib_path and _paths_equal(
                worktree, sib_path, orchestrator_root=root
            ):
                path_ok = False
                path_notes.append(f"shared_with_lane:{sib_id}")
                break

    observed["path_safe"] = path_ok
    observed["path_notes"] = path_notes
    if not path_ok:
        return _refuse("path_unsafe", observed)

    # -- 8: worktree HEAD itself contained -----------------------------------
    # Conditions 1–7 never look at checkout HEAD; a detached clean worktree
    # holding unique commits would otherwise pass and lose them on discard.
    assert worktree is not None  # path_ok guarantees it
    head_proc = subprocess.run(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    observed["head_rev_parse_returncode"] = head_proc.returncode
    head_stderr = (head_proc.stderr or "").strip()
    head_stdout = (head_proc.stdout or "").strip()
    # Always record stderr text so a probe failure that keeps the session path
    # alive still lands in a durable diagnostic [AGT-10]; row 21 asserts a
    # non-empty string appears in observed values.
    observed["head_rev_parse_stderr"] = head_stderr or head_stdout or (
        f"rev-parse HEAD exited {head_proc.returncode}"
    )
    if head_proc.returncode != 0 or not head_stdout:
        return _refuse("head_unresolvable", observed)

    head_sha = head_stdout
    observed["head_sha"] = head_sha
    if not task_branch_tip:
        observed["head_is_ancestor"] = None
        return _refuse("head_not_contained", observed)
    head_anc = _git_is_ancestor(root, head_sha, str(task_branch_tip))
    observed["head_is_ancestor"] = head_anc
    if not head_anc:
        return _refuse("head_not_contained", observed)

    # -- 9: orphan preservation under refs/reclaimed/ ------------------------
    # Conditions 1–8 have already guaranteed branch and task_branch_tip are
    # non-empty and that worktree exists. Refuse when the orphan set cannot be
    # known or cannot be pinned; only then may the verdict authorise reclaim
    # [SEC-05] [SECD-05].
    scan = _unaccounted_orphans(
        orchestrator_root=root,
        worktree_path=worktree,
        lane_branch=str(branch),
        task_branch=str(task_branch_tip),
    )
    if not scan.ok:
        # A failed probe is unknown, not empty — name which probe broke [OBS-08].
        observed["orphan_scan_failed_probe"] = scan.failed_probe
        observed["orphan_scan_stderr"] = scan.stderr
        return _refuse(f"unknown:{scan.failed_probe}", observed)

    # Scan succeeded: record the orphan set on every subsequent exit.
    observed["unaccounted_orphans"] = list(scan.commits)

    if not scan.commits:
        # Nothing to preserve — reclaimable with an explicit empty ref list.
        observed["preserved_refs"] = []
        return ReclaimVerdict(reclaimable=True, reason=None, observed=observed)

    result = _preserve_orphans(
        orchestrator_root=root,
        lane_id=lane_id,
        commits=scan.commits,
    )
    # Partial refs are reporting only, never permission [S1B2-REV-02].
    observed["preserved_refs"] = list(result.refs)
    if not result.ok:
        # ok=False means preservation did NOT happen (fails fast). Refuse the
        # whole reclaim so the unattempted tail is not destroyed [SEC-05].
        # Pass result.reason through unchanged (locked vs failed are distinct).
        return _refuse(result.reason or "orphan_preservation_failed", observed)

    return ReclaimVerdict(reclaimable=True, reason=None, observed=observed)


def lane_branch_reclaimable(
    *,
    orchestrator_root: Path | str,
    task_ref: str,
    lane_id: str,
    integration_ref: str = "main",
) -> ReclaimVerdict:
    """Return whether the lane's feature branch may be deleted [OBS-08].

    Worktree reclaim never deletes branches, so feature refs accumulate without
    bound. This predicate is the missing counterpart: a pure verdict that never
    deletes. Evaluation order is fixed C1→C7; first refusal wins.

    Ancestry and patch-id absorption both under-reclaim on this repo because
    lane work lands squashed, via an integration branch, or as a re-applied
    patch. The check that works is blob preservation (C6): every production
    blob the branch introduced must still appear at the same path on the
    integration ref or another surviving local branch. A review lane that is
    a byte-identical copy of a surviving implementation branch is therefore
    reclaimable even when main never absorbed that content.

    Deleting a branch is irreversible; any uncertainty refuses [FM-08].
    """
    root = Path(orchestrator_root)
    observed: dict[str, Any] = {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "orchestrator_root": str(root),
        "integration_ref": integration_ref,
    }

    # -- load seams ----------------------------------------------------------
    lane_env = get_lane(lane_id=lane_id, task_ref=task_ref)
    if not _seam_envelope_ok(lane_env):
        observed["failed_seam"] = "get_lane"
        return _refuse("seam_unavailable", observed)

    lane_data = lane_env.get("data") if isinstance(lane_env, dict) else None
    lane = (
        lane_data.get("lane")
        if isinstance(lane_data, dict) and isinstance(lane_data.get("lane"), dict)
        else {}
    )
    status = lane.get("status") if isinstance(lane, dict) else None
    branch_raw = lane.get("branch") if isinstance(lane, dict) else None
    observed["status"] = status
    observed["branch"] = branch_raw

    # -- C1: branch recorded on the lane row ---------------------------------
    # Without a branch name there is nothing to reclaim and nothing to probe.
    if not isinstance(branch_raw, str) or not branch_raw.strip():
        return _refuse("branch_unknown", observed)
    branch = branch_raw.strip()
    observed["branch"] = branch
    branch_short = _short_branch_name(branch)
    branch_ref = _lane_refname(branch)

    # -- C2: branch still exists as a local ref ------------------------------
    # Missing ref means nothing to reclaim; distinguish from unknown [RLSE-05].
    verify = _git_run(root, "rev-parse", "--verify", branch_ref)
    observed["branch_rev_parse_returncode"] = verify.returncode
    if verify.returncode != 0:
        observed["branch_rev_parse_stderr"] = (
            (verify.stderr or verify.stdout or "").strip()
        )
        return _refuse("branch_missing", observed)
    branch_sha = (verify.stdout or "").strip()
    observed["branch_sha"] = branch_sha

    # -- C3: never reclaim the integration targets themselves ----------------
    # Guard, not an assumption: main/master deletion is never a reclaim win.
    if branch_short in _INTEGRATION_TARGET_NAMES:
        return _refuse("branch_is_integration_target", observed)

    # -- C4: every lane row naming this branch must be terminal --------------
    # Not merely the row identified by lane_id — a still-active sibling on the
    # same branch means deleting it would strand live work.
    sibling_lanes, _lanes_env = _list_all_lanes(task_ref=task_ref)
    if sibling_lanes is None:
        observed["failed_seam"] = "list_lanes"
        return _refuse("lane_list_unavailable", observed)
    observed["sibling_lane_count"] = len(sibling_lanes)
    non_terminal: list[dict[str, Any]] = []
    # Include the identified row even if list_lanes omitted it.
    rows_to_check: list[dict[str, Any]] = list(sibling_lanes)
    if isinstance(lane, dict) and lane:
        rows_to_check = [lane, *sibling_lanes]
    seen_ids: set[tuple[Any, Any]] = set()
    for row in rows_to_check:
        if not isinstance(row, dict):
            continue
        row_branch = row.get("branch")
        if not isinstance(row_branch, str):
            continue
        if _short_branch_name(row_branch.strip()) != branch_short:
            continue
        row_key = (row.get("task_ref", task_ref), row.get("lane_id"))
        if row_key in seen_ids:
            continue
        seen_ids.add(row_key)
        row_status = row.get("status")
        if row_status not in _TERMINAL_STATUSES:
            non_terminal.append(
                {
                    "lane_id": row.get("lane_id"),
                    "status": row_status,
                }
            )
    observed["branch_lane_rows"] = len(seen_ids)
    observed["non_terminal_branch_lanes"] = non_terminal
    if non_terminal:
        return _refuse("lane_not_terminal", observed)

    # -- C5: branch must not be checked out in any registered worktree -------
    checked_out = _worktrees_checking_out_branch(root, branch)
    if checked_out is None:
        observed["worktree_list_unreadable"] = True
        return _refuse("worktree_list_unreadable", observed)
    observed["branch_checked_out_worktrees"] = checked_out
    if checked_out:
        return _refuse("branch_checked_out", observed)

    # -- C6: every introduced production blob must survive elsewhere ---------
    # WHY: squash / re-apply / integration-branch landing means ancestry and
    # cherry patch-id both reclaim ~0 real branches. Blob identity at path is
    # the check that matches how lane work actually lands.
    introduced, diff_err = _branch_introduced_paths(
        root, integration_ref=integration_ref, branch=branch
    )
    if introduced is None:
        # Failed probe ≠ empty diff [CON-05].
        observed["introduced_paths_stderr"] = diff_err
        return _refuse("introduced_paths_unreadable", observed)
    observed["introduced_paths"] = list(introduced)
    production_paths = [p for p in introduced if not _is_lane_artifact_path(p)]
    observed["production_paths"] = list(production_paths)

    survivor_index, candidate_refs, failed_refs, degraded = _survivor_blob_indexes(
        root, branch=branch, integration_ref=integration_ref
    )
    if survivor_index is None:
        observed["survivor_index_failed"] = True
        return _refuse("survivor_index_unreadable", observed)
    observed["survivor_refs"] = list(candidate_refs)
    observed["survivor_refs_failed"] = list(failed_refs)
    # Partial survivor index after a failed ls-tree degrades: refuse by verdict,
    # never raise and never silently continue as if the index were complete.
    if degraded:
        observed["degraded"] = True
        return _refuse("survivor_index_degraded", observed)

    unpreserved: list[str] = []
    for path in production_paths:
        blob_sha, blob_err = _path_blob_at(root, branch, path)
        if blob_err is not None:
            # Probe ERROR — cannot prove preservation [FM-08][OBS-08].
            observed["blob_probe_failed_path"] = path
            observed["blob_probe_stderr"] = blob_err
            return _refuse("blob_probe_failed", observed)
        if blob_sha is None:
            # Conclusive absence at branch tip: no blob to preserve for this path.
            continue
        observed.setdefault("branch_blobs", {})[path] = blob_sha
        survivors_at_path = survivor_index.get(path, set())
        if blob_sha not in survivors_at_path:
            unpreserved.append(path)
    observed["unpreserved_paths"] = list(unpreserved)
    if unpreserved:
        return _refuse("blobs_unpreserved", observed)

    # -- C7: pin commits that would become unreachable under refs/reclaimed/ -
    # Mirrors condition 9 of lane_worktree_reclaimable: preserve before any
    # future delete is authorised. Failed probe or failed preserve refuses.
    scan = _branch_unreachable_commits(root, branch)
    if not scan.ok:
        observed["orphan_scan_failed_probe"] = scan.failed_probe
        observed["orphan_scan_stderr"] = scan.stderr
        return _refuse(f"unknown:{scan.failed_probe}", observed)
    observed["unaccounted_orphans"] = list(scan.commits)
    if not scan.commits:
        observed["preserved_refs"] = []
        return ReclaimVerdict(reclaimable=True, reason=None, observed=observed)

    result = _preserve_orphans(
        orchestrator_root=root,
        lane_id=lane_id,
        commits=scan.commits,
    )
    observed["preserved_refs"] = list(result.refs)
    if not result.ok:
        return _refuse(result.reason or "orphan_preservation_failed", observed)

    return ReclaimVerdict(reclaimable=True, reason=None, observed=observed)


def _lane_row_is_reclaim_scan_target(
    lane: dict[str, Any],
    *,
    orchestrator_root: Path | str,
) -> tuple[str, str] | None:
    """Return ``(lane_id, worktree_path)`` when the row is terminal and on-disk.

    The candidate set is defined by the ``worktree_lanes`` table state plus a
    path-existence check — never by a list of transition call sites.

    Relative ``worktree_path`` values are resolved against ``orchestrator_root``
    so the check is independent of process CWD. A real directory and a symlink
    (including a dangling one left by partial cleanup) are both scan targets;
    a plain file is not. ``Path.is_symlink`` is lstat-based so it sees broken
    links that ``is_dir`` (which stats through) cannot (PLAN0181-S2LEXISTS-01).
    """
    status = lane.get("status")
    if status not in _TERMINAL_STATUSES:
        return None
    worktree_path_raw = lane.get("worktree_path")
    if not worktree_path_raw:
        return None
    path = _resolve_lane_worktree_path(orchestrator_root, worktree_path_raw)
    # Directory OR symlink: dangling links are reclaimable disk nodes; plain files are not.
    if not (path.is_dir() or path.is_symlink()):
        return None
    lane_id = str(lane.get("lane_id") or "").strip()
    if not lane_id:
        return None
    return lane_id, str(path)


# Match ``list_lanes`` default page size. Scan must page; never rely on one page.
_LIST_LANES_PAGE_SIZE = 100
# Hard cap so a stuck has_more cannot spin forever [RLSE-05].
_LIST_LANES_MAX_PAGES = 10_000


class _LaneSweep(list):
    """Collected lane rows plus how many ``list_lanes`` seam calls produced them.

    ``page_count`` is the transaction count of the walk. Callers that need
    snapshot isolation must require ``page_count == 1`` rather than inferring
    page span from ``len(self)`` (a multi-page walk can still finish with
    ``len == _LIST_LANES_PAGE_SIZE`` after mid-flight deletes —
    PLAN0181-S2GATE4-SNAPSHOTFLAG-FALSE-TRUE-01).
    """

    __slots__ = ("page_count",)

    def __init__(self, items: list[Any], *, page_count: int) -> None:
        super().__init__(items)
        self.page_count = int(page_count)


def _list_all_lanes(*, task_ref: str) -> tuple[list[Any] | None, Any]:
    """Page ``list_lanes`` until exhausted; honour ``has_more``.

    Pages by immutable ``after_id`` keyset so a concurrent write cannot
    *duplicate or skip existing rows under an OFFSET cursor*
    (PLAN0181-S2PAGERACE-01). Keyset does **not** provide snapshot isolation
    across pages: each ``list_lanes`` call is its own seam transaction.

    Concurrent-write detection here is **cardinality only**: when the final
    page reports an int ``total_matching``, it is compared to
    ``len(all_lanes)`` and a mismatch refuses rather than handing a short
    universe to the shared-path guard
    (PLAN0181-S2GATE2-KEYSET-BLIND-TO-NEW-LANES-01). That check does **not**
    detect equal-count membership churn (delete an already-read id and insert
    a new higher id so totals still match) — a multi-page walk can therefore
    return a set that never existed at any instant
    (PLAN0181-S2GATE3-KEYSET-EQUALCOUNT-CHURN-01). Callers must not treat a
    multi-page success as snapshot-consistent; ``scan_terminal_lanes`` surfaces
    that residual risk on ``ScanResult.snapshot_consistent`` from this
    function's reported ``page_count`` (``_LaneSweep.page_count``), not from
    collected length. Single-page success (``page_count == 1``) is one
    transaction and is snapshot-consistent.

    Passes ``after_id`` on every call, including the first (``None``), so the
    seam selects keyset mode rather than OFFSET.

    Returns ``(lanes, first_envelope)`` only when paging finished without a
    truncated exit. On success ``lanes`` is a ``_LaneSweep`` whose
    ``page_count`` is the number of ``list_lanes`` seam calls made. Every
    truncated exit — first-page failure, later-page failure, empty page still
    claiming ``has_more``, unusable pagination contract (absent or non-bool
    ``has_more``), total_matching cardinality drift, or the page-cap runaway
    guard — returns ``(None, failing_envelope)`` so callers refuse rather than
    act on a silently short list under a healthy page-1 envelope
    (PLAN0181-S2PARTIAL-01 / PLAN0181-S2PAGECAP-01 / PLAN0181-S2HASMORE-01).
    """
    all_lanes: list[Any] = []
    after_id: int | None = None
    first_env: Any = None
    last_env: Any = None
    page_count = 0
    for _ in range(_LIST_LANES_MAX_PAGES):
        # Always pass after_id (including None on page one) so the seam's
        # keyset branch is selected; omitting it keeps an OFFSET reader.
        env = list_lanes(
            task_ref=task_ref,
            limit=_LIST_LANES_PAGE_SIZE,
            after_id=after_id,
        )
        page_count += 1
        last_env = env
        if first_env is None:
            first_env = env
        if not _seam_envelope_ok(env):
            return None, env
        data = env.get("data") if isinstance(env, dict) else None
        if not isinstance(data, dict):
            return None, env
        page = data.get("lanes")
        if not isinstance(page, list):
            return None, env
        all_lanes.extend(page)
        # Absent / null / non-bool has_more is not "done" — refuse the
        # unusable pagination contract rather than stop after one page
        # (PLAN0181-S2HASMORE-01).
        has_more = data.get("has_more")
        if not isinstance(has_more, bool):
            return None, env
        if not has_more:
            # total_matching cardinality check only: a pure insert/delete mid-
            # sweep drifts collected length from the reported total. Equal-
            # count membership swaps are NOT detected here
            # (PLAN0181-S2GATE2-KEYSET-BLIND-TO-NEW-LANES-01 /
            # PLAN0181-S2GATE3-KEYSET-EQUALCOUNT-CHURN-01).
            total = data.get("total_matching")
            if type(total) is int and total != len(all_lanes):
                return None, env
            return _LaneSweep(all_lanes, page_count=page_count), first_env
        # Empty page + has_more is a truncated universe, not a complete one.
        if not page:
            return None, env
        # has_more True without a usable int cursor is unresumable — refuse
        # immediately rather than re-request the same page up to the page cap
        # (PLAN0181-S2CURSOR-01).
        next_cursor = data.get("next_after_id")
        # type() is int, not isinstance: bool subclasses int, so a JSON true
        # would pass isinstance and be forwarded as after_id=True (DATA-03 /
        # MODEL-03; PLAN0181-S2GATE-CURSORBOOL-01). list_lanes already uses
        # type(after_id) is int on the writer side — stay consistent.
        if type(next_cursor) is not int:
            return None, env
        after_id = next_cursor
    # Page cap hit with unread rows remaining — refuse, do not act short.
    return None, last_env if last_env is not None else first_env


def scan_terminal_lanes(
    *,
    orchestrator_root: Path | str,
    task_ref: str,
    log: Any | None = None,
) -> ScanResult:
    """Scan ``worktree_lanes`` for terminal rows whose worktree still exists.

    Always returns a ``ScanResult`` (tuple subclass of candidates) so every
    path exposes the same question surface [AGT-10][CON-05]
    (PLAN0181-S2SCANASYM-01):

    - ``complete`` is True when the full lane table was read and scanned;
      False when listing was refused and no candidate universe is trusted.
    - ``refusal_reason`` is None on a complete scan; a short stable token
      (e.g. ``list_unavailable``) when the scan was refused.

    For each match, evaluates ``lane_worktree_reclaimable`` and records the
    verdict via ``record_reclaim_candidate``. Removes nothing. Keyword-only so
    ``task_ref`` cannot be swapped with a path by positional call order.

    Pages ``list_lanes`` until ``has_more`` is false so the candidate universe
    is not silently truncated at the default limit of 100 (PLAN0181-S2SCAN-01).
    Isolates per-lane predicate faults so one sick worktree cannot abort the
    rest of the cycle (PLAN0181-S2FAULT-01).

    Does not call ``nudge_reclaim_candidate`` — the nudge is a latency
    optimisation applied at transition sites, not the scan mechanism.
    """
    root = Path(orchestrator_root)
    lanes, env = _list_all_lanes(task_ref=task_ref)
    if lanes is None:
        # PLAN0181-S2GATE-LOGRAISE-01: sink faults must not escape [AGT-10].
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_scan_list_unavailable",
            task_ref=task_ref,
            envelope=env,
        )
        # Distinguish list refusal from a genuine empty candidate set
        # [CON-05][PLAN0181-S2SCANEMPTY-01]. Still == () for older pins.
        # failed_lanes / unrecorded_lanes stay () — listing never reached
        # per-lane evaluation (PLAN0181-S2SCANPARTIAL-01 / S2RECFALSE-01).
        return ScanResult(
            (),
            complete=False,
            refusal_reason="list_unavailable",
            failed_lanes=(),
            unrecorded_lanes=(),
            snapshot_consistent=False,
        )

    candidates: list[ReclaimCandidate] = []
    failed_lanes: list[str] = []
    unrecorded_lanes: list[str] = []
    # Derive snapshot provenance before any durable write so every recorded
    # verdict carries torn-ness rather than discovering it after the loop.
    # Rule is unchanged: page_count == 1 (PLAN0181-S2GATE4-SNAPSHOTFLAG-FALSE-TRUE-01).
    page_count = getattr(lanes, "page_count", None)
    snapshot_consistent = page_count == 1
    for row_index, raw_lane in enumerate(lanes):
        if not isinstance(raw_lane, dict):
            # Malformed row must not masquerade as a clean complete sweep
            # (PLAN0181-S2GATE-NONDICT-SKIP-01) [OBS-08][RES-13][AGT-10].
            token = f"<non-dict:{row_index}>"
            failed_lanes.append(token)
            _safe_log(
                log,
                "ERROR",
                "lane_reclaim_scan_lane_malformed",
                task_ref=task_ref,
                row_index=row_index,
                row_type=type(raw_lane).__name__,
                failed_token=token,
            )
            continue
        # Identity hints for logging if the scan-target predicate itself raises
        # (EACCES on parent dirs) before the resolved path is known
        # [PLAN0181-S2STATRAISE-01]. Blank lane_id gets a positional token so
        # two dropped blank rows do not collide on '' (FAILEDLANES-EMPTY-TOKEN-01).
        lane_id = str(raw_lane.get("lane_id") or "").strip()
        if not lane_id:
            lane_id = f"<blank-id:{row_index}>"
        worktree_path = str(raw_lane.get("worktree_path") or "")
        # Per-lane isolation: git/OS faults — including unstattable parents on
        # the scan-target predicate — must not abort the cycle for every
        # remaining terminal lane [AGT-10][RLSE-05][PLAN0181-S2STATRAISE-01].
        try:
            target = _lane_row_is_reclaim_scan_target(
                raw_lane, orchestrator_root=root
            )
            if target is None:
                continue
            lane_id, worktree_path = target
            # Call through the module namespace so tests can patch these seams.
            # Pass the already-paged universe so each candidate does not
            # re-page list_lanes (PLAN0181-S2GATE-SCAN-NPLUS1-01) [RES-12].
            # Caller obligation: ``lanes`` is the complete same-task_ref set
            # (we refused above when listing was unusable).
            # No TypeError retry on sibling_lanes: a string-matched fallback
            # would mask real signature drift, re-introduce 1+N re-paging for
            # stale wrappers, and double-invoke condition-9 preserve writes
            # (PLAN0181-S2GATE-TYPEERR-FALLBACK-01) [TEST-11][ERR-04][RES-12].
            verdict = lane_worktree_reclaimable(
                orchestrator_root=root,
                task_ref=task_ref,
                lane_id=lane_id,
                sibling_lanes=lanes,
            )
        except Exception as exc:  # noqa: BLE001 — scan cycle must not die here
            # Name the dropped lane BEFORE the log attempt so a raising sink
            # cannot lose the durable record (PLAN0181-S2GATE-LOGRAISE-01)
            # [AGT-10][OBS-06].
            failed_lanes.append(lane_id)
            _safe_log(
                log,
                "ERROR",
                "lane_reclaim_scan_lane_failed",
                lane=lane_id,
                task_ref=task_ref,
                worktree_path=worktree_path,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            # Invalidate any durable reclaimable:true so a failed re-eval
            # cannot leave a stale authorisation in the ledger
            # (PLAN0181-S2GATE2-FAILED-LANE-LEAVES-STALE-VERDICT-01).
            # Guarded: the failure cause may be the ledger itself; a second
            # fault must not abort the sweep or mask the original error.
            # Falsy return is the same class of loss as a raise: the stale
            # authorisation was not invalidated, so name the lane in
            # unrecorded_lanes (PLAN0181-S2GATE3-FAILEDLANE-INVALIDATE-FALSY-IGNORED-01).
            invalidated = False
            try:
                inv_observed: dict[str, Any] = {
                    "task_ref": task_ref,
                    "lane_id": lane_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "snapshot_consistent": snapshot_consistent,
                    "page_count": page_count,
                }
                invalidated = bool(
                    record_reclaim_candidate(
                        task_ref=task_ref,
                        lane_id=lane_id,
                        verdict=ReclaimVerdict(
                            reclaimable=False,
                            reason="evaluation_failed",
                            observed=inv_observed,
                        ),
                        log=log,
                    )
                )
            except Exception as inv_exc:  # noqa: BLE001 — never abort on invalidate
                _safe_log(
                    log,
                    "ERROR",
                    "lane_reclaim_scan_invalidate_failed",
                    lane=lane_id,
                    task_ref=task_ref,
                    error=str(inv_exc),
                    error_type=type(inv_exc).__name__,
                )
                invalidated = False
            if not invalidated:
                unrecorded_lanes.append(lane_id)
            continue
        # Stamp scan-universe provenance into the verdict before the durable
        # write so a ledger reader can tell a torn multi-page sweep from a
        # single-snapshot one.
        if isinstance(getattr(verdict, "observed", None), dict):
            verdict.observed["snapshot_consistent"] = snapshot_consistent
            verdict.observed["page_count"] = page_count
        # Ledger write isolation: a raise from record_reclaim_candidate must
        # degrade like a falsy return — name the lane in unrecorded_lanes and
        # keep scanning (PLAN0181-S2GATE-RECORDRAISE-01) [RES-13][AGT-10].
        recorded = False
        try:
            recorded = bool(
                record_reclaim_candidate(
                    task_ref=task_ref, lane_id=lane_id, verdict=verdict, log=log
                )
            )
        except Exception as exc:  # noqa: BLE001 — one sick write must not abort
            _safe_log(
                log,
                "ERROR",
                "lane_reclaim_scan_record_failed",
                lane=lane_id,
                task_ref=task_ref,
                worktree_path=worktree_path,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            recorded = False
        if not recorded:
            unrecorded_lanes.append(lane_id)
        candidates.append(
            ReclaimCandidate(
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=worktree_path,
                verdict=verdict,
                recorded=recorded,
            )
        )
    # One aggregate degraded event per cycle when any lane was dropped or
    # left unrecorded — in addition to per-lane failure lines [OBS-06]
    # (PLAN0181-S2SCANPARTIAL-01 / PLAN0181-S2RECFALSE-01).
    if failed_lanes or unrecorded_lanes:
        # PLAN0181-S2GATE-LOGRAISE-01: shared _safe_log, not a hand-rolled try
        # [REF-19] ("raising log must not escape").
        # ``affected`` is the unique-lane count: a lane can land in both
        # failed and unrecorded (eval raise + invalidate fail) without
        # inflating operator aggregates to two problem lanes.
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_scan_degraded",
            task_ref=task_ref,
            failed=len(failed_lanes),
            unrecorded=len(unrecorded_lanes),
            affected=len(set(failed_lanes) | set(unrecorded_lanes)),
            failed_lanes=list(failed_lanes),
            unrecorded_lanes=list(unrecorded_lanes),
        )
    # Same ScanResult surface as the refusal path so complete/refusal_reason
    # /failed_lanes/unrecorded_lanes/snapshot_consistent are total attributes
    # [AGT-10][CON-05] (PLAN0181-S2SCANASYM-01).
    # snapshot_consistent was derived before the per-lane loop (and stamped
    # into each recorded verdict); rule is still page_count == 1
    # (PLAN0181-S2GATE4-SNAPSHOTFLAG-FALSE-TRUE-01 /
    # PLAN0181-S2GATE3-KEYSET-EQUALCOUNT-CHURN-01).
    return ScanResult(
        tuple(candidates),
        complete=True,
        refusal_reason=None,
        failed_lanes=tuple(failed_lanes),
        unrecorded_lanes=tuple(unrecorded_lanes),
        snapshot_consistent=snapshot_consistent,
    )


def nudge_reclaim_candidate(
    *,
    orchestrator_root: Path | str,
    task_ref: str,
    lane_id: str,
    log: Any | None = None,
) -> ReclaimCandidate | NudgeFailure | None:
    """Evaluate and record one lane if it is a terminal, on-disk candidate.

    Same filters as ``scan_terminal_lanes`` applied to a single lane. Returns
    ``None`` (and records nothing) when the lane is not a candidate. Keyword-only.
    Latency optimisation only — never a bypass of the table-backed filter.

    Predicate faults are logged at ERROR and return ``NudgeFailure`` rather than
    propagating — the same isolation and observability the scan applies to
    per-lane failures so a sick worktree at a transition site is not silent.
    ``None`` keeps exactly one meaning: this lane is not a reclaim candidate
    (PLAN0181-S2NUDGERET-01).

    A failed or unusable ``get_lane`` envelope is also logged at ERROR and
    returned as ``NudgeFailure`` so a seam outage is not indistinguishable from
    "not a candidate" (PLAN0181-S2SILENT-01 / PLAN0181-S2NUDGERET-01).
    """
    root = Path(orchestrator_root)
    env = get_lane(lane_id=lane_id, task_ref=task_ref)
    if not _seam_envelope_ok(env):
        # PLAN0181-S2GATE-LOGRAISE-01: sink faults must not escape into the
        # transition site [AGT-10][RES-13].
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_nudge_get_lane_unavailable",
            lane=lane_id,
            task_ref=task_ref,
            envelope=env,
        )
        return NudgeFailure(lane_id=lane_id, reason="get_lane_unavailable")
    data = env.get("data") if isinstance(env, dict) else None
    lane = data.get("lane") if isinstance(data, dict) else None
    if not isinstance(lane, dict):
        # Distinct event from the envelope failure above [CON-05][OBS-06]
        # (PLAN0181-S2EVTNAME-01): ok envelope with unusable lane payload.
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_nudge_lane_payload_malformed",
            lane=lane_id,
            task_ref=task_ref,
            envelope=env,
        )
        return NudgeFailure(lane_id=lane_id, reason="lane_payload_malformed")
    # Identity hints before the predicate resolves the path; EACCES on an
    # unsearchable parent must log and return NudgeFailure, not raise into the
    # transition site [PLAN0181-S2STATRAISE-01][PLAN0181-S2NUDGERET-01].
    resolved_lane_id = str(lane.get("lane_id") or lane_id or "").strip()
    worktree_path = str(lane.get("worktree_path") or "")
    try:
        target = _lane_row_is_reclaim_scan_target(lane, orchestrator_root=root)
        if target is None:
            return None
        resolved_lane_id, worktree_path = target
        verdict = lane_worktree_reclaimable(
            orchestrator_root=root,
            task_ref=task_ref,
            lane_id=resolved_lane_id,
        )
    except Exception as exc:  # noqa: BLE001 — nudge must not raise into callers
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_nudge_lane_failed",
            lane=resolved_lane_id or lane_id,
            task_ref=task_ref,
            worktree_path=worktree_path,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return NudgeFailure(
            lane_id=resolved_lane_id or lane_id,
            reason="evaluation_failed",
        )
    # Ledger write isolation: the documented "never raises" contract on
    # record_reclaim_candidate is not trusted — any breach must still yield a
    # typed value, not crash a transition site
    # (PLAN0181-S2GATE2-NUDGE-UNGUARDED-CALLSITE-01) [RES-13][AGT-10].
    recorded = False
    try:
        recorded = bool(
            record_reclaim_candidate(
                task_ref=task_ref,
                lane_id=resolved_lane_id,
                verdict=verdict,
                log=log,
            )
        )
    except Exception as exc:  # noqa: BLE001 — never-raise contract may be broken
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_nudge_record_failed",
            lane=resolved_lane_id or lane_id,
            task_ref=task_ref,
            worktree_path=worktree_path,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return NudgeFailure(
            lane_id=resolved_lane_id or lane_id,
            reason="record_failed",
        )
    return ReclaimCandidate(
        task_ref=task_ref,
        lane_id=resolved_lane_id,
        worktree_path=worktree_path,
        verdict=verdict,
        recorded=recorded,
    )
