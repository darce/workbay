"""Read-only Git evidence for one incoming edge of a legacy holder."""

from __future__ import annotations

import re
from pathlib import Path

_OID = re.compile(r"[0-9a-f]{40}")


def proves_reference_independence(
    repo: Path,
    proof: dict,
    *,
    incoming_worktree: str,
    incoming_roots: set[str],
    holder_worktree: str,
    holder_roots: set[str],
    holder_branch: str,
    holder_head: str,
    holder_tree: str,
    holder_recorded_tip: str | None,
) -> bool:
    """Check one protected, foreign, self-source holder against this candidate.

    The caller must authenticate the exact legacy receipt, native reservation,
    original owner and protected worktree/scope records before calling. The
    root sets must cover original and subsequent protected scope and applicable
    reservations. Call only for the incoming subject's own admission.

    Reach(incoming tip) minus Reach(integration) is empty when the two
    commits are identical. A missing later holder tip cannot supply an
    incoming-only commit in that case. This evidence applies to the measured
    pair and source pin. Global holder custody, settlement and accounting still
    require their own evidence.

    Canon v0.26.2 GRPH-14 (Building Knowledge Graphs, ch. 8 + Mental
    Models, ch. 8): join the original custody records to live Git evidence.
    """
    from .review_cycle_dispatch import _disjoint_roots, _subject_probe
    from .review_ratchet import _short_branch, review_source_ref

    incoming_branch = _short_branch(proof.get("subject_branch"))
    integration_branch = _short_branch(proof.get("integration_branch"))
    source_branch = _short_branch(holder_branch)
    pin = proof.get("tip_sha")
    if (
        not incoming_branch
        or not integration_branch
        or not source_branch
        or incoming_branch in {integration_branch, source_branch}
        or source_branch == integration_branch
        or not isinstance(pin, str)
        or not _OID.fullmatch(pin)
        or proof.get("base_sha") != pin
        or proof.get("changed_files") != []
        or review_source_ref(repo) not in {"HEAD", pin}
        or not incoming_roots
        or not holder_roots
        or {"", "."} & (incoming_roots | holder_roots)
        or not _disjoint_roots(holder_roots, incoming_roots)
    ):
        return False

    incoming_ref = f"refs/heads/{incoming_branch}"
    integration_ref = f"refs/heads/{integration_branch}"
    holder_commits = (holder_head,) if holder_recorded_tip is None else (holder_head, holder_recorded_tip)
    if any(not isinstance(oid, str) or not _OID.fullmatch(oid) for oid in holder_commits):
        return False

    def probe(*args: str) -> str:
        return _git(repo, *args)

    try:
        checkout = Path(incoming_worktree)
        if not checkout.is_absolute():
            return False
        top = Path(probe("rev-parse", "--show-toplevel").rstrip("\n"))
        if checkout.resolve() != repo.resolve() or top.resolve() != repo.resolve():
            return False
        probe("check-ref-format", incoming_ref)
        probe("check-ref-format", integration_ref)
        if probe("rev-parse", "--is-shallow-repository") != "false\n":
            return False
        grafts = Path(probe("rev-parse", "--path-format=absolute", "--git-path", "info/grafts").rstrip("\n"))
        if not _path_absent(grafts):
            return False

        if probe("symbolic-ref", "--no-recurse", "HEAD").strip() != incoming_ref:
            return False
        for ref in (integration_ref, incoming_ref):
            symbolic = _subject_probe(repo, "symbolic-ref", "--quiet", "--no-recurse", ref)
            if symbolic.returncode != 1 or symbolic.stdout or symbolic.stderr:
                return False
            if probe("show-ref", "--verify", "--hash", ref) != pin + "\n":
                return False
        if probe("rev-parse", "--verify", "HEAD^{commit}").strip() != pin:
            return False
        if probe("rev-parse", "--verify", holder_head + "^{tree}").strip() != holder_tree:
            return False
        for oid in set(holder_commits):
            if probe("cat-file", "-t", oid) != "commit\n":
                return False
            probe("merge-base", "--is-ancestor", oid, pin)

        if probe(
            "-c",
            "core.fsmonitor=false",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ):
            return False
        if not protected_index_visible(repo, incoming_roots):
            return False
        if not holder_source_absent(repo, holder_worktree, source_branch, pin):
            return False
        # Recheck the live refs after the worktree reads. This result is a
        # current observation and must be recomputed for a later admission.
        return (
            probe("symbolic-ref", "--no-recurse", "HEAD").strip() == incoming_ref
            and probe("show-ref", "--verify", "--hash", integration_ref) == pin + "\n"
            and probe("rev-parse", "--verify", "HEAD^{commit}").strip() == pin
        )
    except (OSError, RuntimeError, TypeError, ValueError):
        return False


def _git(repo: Path, *args: str) -> str:
    from .review_cycle_dispatch import _subject_probe

    result = _subject_probe(repo, "--no-optional-locks", "--no-replace-objects", *args)
    if result.returncode or result.stderr:
        raise ValueError("reference_independence_git_evidence_unavailable")
    return result.stdout


def protected_index_visible(repo: Path, incoming_roots: set[str]) -> bool:
    """Require visible index entries throughout the authorized incoming grant."""
    from .review_cycle_dispatch import _disjoint_roots

    # These flags can hide worktree edits from status. Outside the grant they
    # cannot change the pinned commit graph or authorize edits to those paths.
    flagged = {
        entry[2:] for entry in _git(repo, "ls-files", "-v", "-z").split("\0")
        if entry and (entry[0] == "S" or entry[0].islower())
    }
    if incoming_roots & {"", "."}:
        return not flagged
    return _disjoint_roots(flagged, incoming_roots)


def holder_source_absent(repo: Path, worktree: str, branch: str, contained_by: str) -> bool:
    """Veto an exact surviving checkout or a ref carrying unexplained history."""
    from .review_cycle_dispatch import _ref_exists

    retired = Path(worktree)
    holder_ref = f"refs/heads/{branch}"
    _git(repo, "check-ref-format", holder_ref)
    if _ref_exists(repo, holder_ref) or not _path_absent(retired):
        return False
    # Remote-tracking refs are local observations. Compare them to the exact
    # evidence commit without fetching or consulting a remote.
    for ref in _git(repo, "for-each-ref", "--format=%(refname)", "refs/remotes").splitlines():
        if ref.removeprefix("refs/remotes/").endswith("/" + branch):
            _git(repo, "merge-base", "--is-ancestor", ref, contained_by)
    listing = _git(repo, "worktree", "list", "--porcelain", "-z")
    if not listing.startswith("worktree ") or not listing.endswith("\0\0"):
        return False
    for record in listing[:-2].split("\0\0"):
        location, *fields = record.split("\0")
        path = Path(location.removeprefix("worktree "))
        if (
            not location.startswith("worktree ")
            or not path.is_absolute()
            or path.resolve() == retired.resolve()
            or f"branch {holder_ref}" in fields
        ):
            return False
    return not _ref_exists(repo, holder_ref) and _path_absent(retired)


def _path_absent(path: Path) -> bool:
    if not path.is_absolute():
        return False
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    return False
