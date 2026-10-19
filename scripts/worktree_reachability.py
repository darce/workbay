#!/usr/bin/env python3
"""Score every worktree by whether its uncommitted content is *recoverable*.

The reap census gates on ``dirty``: does the worktree have staged, unstaged,
unmerged or untracked changes. That predicate is wrong in both directions and
it has been wrong expensively.

* **False alarm.** A rescue-named checkout reported nine staged
  and nine dirty files and was held out of every reap as "uncommitted work no
  gate has scored". It is an abandoned merge: the index carries stages 1/2/3
  for eight paths and ``MERGE_HEAD`` is gone, so ``git merge --abort`` answers
  ``fatal: There is no merge to abort`` and ``--continue`` refuses too. It looks
  maximally alarming and permanently stuck. But stage 2 is byte-identical to
  ``HEAD`` for all eight paths, and stage 3 is byte-identical to a blob on
  ``feature/wb-landpipe-01`` (5 paths) or
  ``feature/wb-lowtier-dag-01-plan0221-coordinator`` (3 paths). Both inputs
  survive as branches. Zero bytes are at risk; the worktree is a scratch
  directory wearing a data-loss warning.

* **False assurance.** Content that was ``git add``ed and then removed from the
  working tree lives only in the index. It is real work, no working-tree scan
  sees it, and ``git worktree remove --force`` discards it.

So the predicate computed here is the one that actually licenses the decision:

    for every path with uncommitted content, does an identical blob exist in
    some commit reachable from a ref?

``UNREACHABLE`` is the only status that makes removing a worktree lossy. That
is the number a reap gate should read.

Canon: [EVAL-25] a judgment pool is incomplete by design, so an item outside
it is unpooled rather than known-nonrelevant -- a blob found in any commit is
proven safe, while "not found" is only ever "not found in the refs I
enumerated". Hence the pool is *all* refs, its depth is disclosed on every
record, and the unreachable list is printed in full rather than counted. [OBS-08] a gate
that cannot say why it held tells the operator nothing. [GRPH-27] the status
vocabulary is a closed set named up front, so a tree that cannot be inspected
gets ``UNKNOWN`` rather than falling through to a neighbouring "looks fine".

Exit status is always 0: this is an instrument, not a gate. It reports; the
operator decides, and destructive reclaim stays operator-authorized.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

CLEAN = "CLEAN"  # nothing uncommitted at all
RECOVERABLE = "RECOVERABLE"  # uncommitted, but every blob exists in a commit
UNREACHABLE = "UNREACHABLE"  # holds bytes no commit has -- removing this loses work
UNKNOWN = "UNKNOWN"  # could not be inspected; never treat as safe

# A conflict marker is exactly ``marker-size`` (default 7) repeats of one
# character, optionally followed by a space and a label. Anchored and exact --
# a hand-typed ``=========================`` underline is 25 characters, does
# not match, and therefore stays a content line.
_MARKER_RE = re.compile(r"^(<{7}|\|{7}|={7}|>{7})(?: .*)?$")

#: ``git merge-file`` conflict styles, tried in order. The repository's
#: ``merge.conflictStyle`` decides which one produced the file on disk and this
#: instrument must not depend on reading that config correctly, so all three
#: are candidates and any match is accepted. An unsupported flag on an older
#: git exits nonzero with empty stdout and drops out on its own.
_CONFLICT_STYLE_FLAGS: tuple[tuple[str, ...], ...] = ((), ("--diff3",), ("--zdiff3",))

#: Every subprocess here is a local git read. Bounded so a wedged git turns
#: into a reported failure rather than a census that never returns.
_SUBPROCESS_TIMEOUT_S = 120.0


#: Markers git writes a label after: the ref name on the ``ours`` opener, the
#: merged-in ref on the ``theirs`` closer, the merge base on the ``|||||||``
#: divider. ``git merge`` labels them with the ref, ``git checkout --merge``
#: regenerates them as ``ours``/``theirs``, so the label carries no information
#: about whether the *content* is work. ``=======`` is deliberately absent: git
#: always writes it bare, so it is compared verbatim and text appended to it
#: stays visible as the hand edit it is.
_LABELLED_MARKERS = frozenset({"<<<<<<<", "|||||||", ">>>>>>>"})


def _normalize_conflict_lines(text: str) -> list[str]:
    """Structural marker labels dropped; every other line kept verbatim.

    A marker is only structural where the conflict grammar admits one: an
    opener outside a block, a base divider inside the ours section, a
    ``=======`` in ours or base, a closer in theirs. Shape alone is not enough.
    A *content* line of seven ``>`` and a label -- a quoted email, a diff pasted
    into prose -- sits outside any block, and erasing its tail there would hide
    a hand edit to real text. The state is the discriminator, so the walk
    carries one [GRPH-27].

    Only the labelled markers lose their tail. Everything else, ``=======``
    included, still has to match exactly, so line order and line content are
    otherwise untouched by this pass.
    """
    outside, ours, base, theirs = 0, 1, 2, 3
    out: list[str] = []
    state = outside
    for line in text.splitlines():
        match = _MARKER_RE.match(line)
        token = match.group(1) if match else None
        structural = True
        if token == "<<<<<<<" and state == outside:
            state = ours
        elif token == "|||||||" and state == ours:
            state = base
        elif token == "=======" and state in (ours, base):
            state = theirs
        elif token == ">>>>>>>" and state == theirs:
            state = outside
        else:
            structural = False
        out.append(token if structural and token in _LABELLED_MARKERS else line)
    return out


def _is_unresolved_conflict_artifact(path: Path, stage_texts: dict[str, str]) -> bool:
    """True only when git can regenerate this file byte for byte from the stages.

    A working-tree file at an unmerged path is normally written *by git* from
    the stages, so it is regenerable (``git checkout --merge -- <path>``) and
    is not work. The same structural state after a hand resolution means the
    opposite: those bytes exist nowhere else. Keyed on the full tuple rather
    than left to fall into whichever neighbour the control flow reaches first
    [GRPH-27].

    The rule is re-derivation, not inspection. An earlier version asked whether
    every non-marker line appeared in some stage, which catches only *added*
    lines: deleting the unwanted half of a conflict, or reordering the two
    halves, is equally a resolution and equally exists in no commit, and both
    scored as derived. Deleting one side is the *common* half-resolution.
    ``git checkout --merge -- <path>`` is deterministic, so the sound form of
    the question is "is this exactly what git would write", and every other
    edit -- addition, deletion, reordering -- falls to the at-risk side by
    construction. (Not *every* other edit: ``str.splitlines`` collapses the
    line terminator, so a pure CRLF/LF conversion or a stripped final newline
    still reads as derived. A known blind spot, not a guarantee.)

    Re-derivation is necessary and not sufficient: the run must also have
    *conflicted*. ``git merge-file`` exits 0 and prints a clean, marker-free
    merge whenever the two sides do not overlap, and ``git read-tree -m``,
    ``git rerere`` and ``update-index --index-info`` all leave stages 1/2/3 at
    such a path. Accepting that output would report the merge product -- bytes
    that are in no commit and are recomputable only from that worktree's index
    -- as derived, i.e. as nothing to lose. So a candidate output must carry a
    conflict, by both signals git offers: a positive exit status (the conflict
    count) and a structural marker in the rendered text.

    ``stage_texts`` maps stage number to blob text. Stages 2 and 3 are both
    required: without them there is nothing to re-derive, so the file is scored
    as ordinary content rather than guessed at.

    Text is carried with ``surrogateescape`` end to end so the comparison is
    byte-exact even when a stage is not valid UTF-8. ``replace`` is not
    interchangeable: it maps a bad byte to ``?`` on one side and ``U+FFFD`` on
    the other, and the two never compare equal.
    """
    if "2" not in stage_texts or "3" not in stage_texts:
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="surrogateescape")
    except OSError:
        return False
    want = _normalize_conflict_lines(text)
    with tempfile.TemporaryDirectory(prefix="wb-conflict-") as tmp:
        root = Path(tmp)
        names = {"1": root / "base", "2": root / "ours", "3": root / "theirs"}
        for stage, dest in names.items():
            dest.write_text(stage_texts.get(stage, ""), encoding="utf-8", errors="surrogateescape")
        for flags in _CONFLICT_STYLE_FLAGS:
            proc = subprocess.run(
                [
                    "git",
                    "merge-file",
                    "-p",
                    *flags,
                    "-L",
                    "ours",
                    "-L",
                    "base",
                    "-L",
                    "theirs",
                    str(names["2"]),
                    str(names["1"]),
                    str(names["3"]),
                ],
                capture_output=True,
                text=True,
                errors="surrogateescape",
                check=False,
                timeout=_SUBPROCESS_TIMEOUT_S,
            )
            if not proc.stdout:
                continue
            # Exit status is the conflict count, so 0 is a clean merge of
            # non-overlapping stages: real bytes, in no commit, and destroyed
            # with the index they came from. Marker presence is checked
            # independently so a git that reports differently cannot slip past.
            if proc.returncode <= 0:
                continue
            got = _normalize_conflict_lines(proc.stdout)
            if not any(_MARKER_RE.match(line) for line in got):
                continue
            if got == want:
                return True
    return False


def _git(cwd: Path, *args: str) -> str:
    """git stdout, or ``""``.

    ``errors="surrogateescape"``, not the default strict decode. A single blob
    or path that is not valid UTF-8 anywhere in this repository would otherwise
    raise ``UnicodeDecodeError`` out of the census and print nothing at all --
    breaking the contract in this module's own docstring that a tree which
    cannot be inspected is reported ``UNKNOWN`` and that the exit status is
    always 0. An instrument that dies on the input it exists to characterise is
    worse than one that says it does not know.
    """
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        errors="surrogateescape",
        check=False,
        timeout=_SUBPROCESS_TIMEOUT_S,
    )
    return proc.stdout


def _blob_index(repo: Path) -> dict[str, set[str]]:
    """Map ``path -> {blob sha}`` over every commit reachable from every ref.

    One ``rev-list --objects`` walk over ``--all``, not a ``rev-parse`` per
    (path, ref): the naive form is O(paths x refs) subprocesses and this repo
    carries ~140 refs, which turns a safety check into a coffee break -- and a
    safety check nobody runs is not a safety check.
    """
    index: dict[str, set[str]] = defaultdict(set)
    for line in _git(repo, "rev-list", "--all", "--objects").splitlines():
        sha, _, path = line.partition(" ")
        if path:
            index[path].add(sha)
    return index


def _hash_worktree_file(repo: Path, wt: Path, rel: str) -> str | None:
    """Hash an on-disk file the way git would, without writing an object.

    ``None`` means there is nothing on disk to hash -- a deletion. A deletion
    cannot be lost content, so it is not this instrument's concern.

    A symlink is hashed as git stores it: the blob is the target path, not the
    bytes it points at. Hashing through the link would both misreport the
    content and let a link into an unrelated tree read as "already committed".
    """
    target = wt / rel
    if target.is_symlink():
        # ``is_symlink`` then ``readlink`` is a TOCTOU on a live lane; an
        # uncaught OSError here would abort the whole census for a race in one
        # path. Treat it as "nothing hashable", same as a deletion.
        try:
            link_target = os.readlink(target)
        except OSError:
            return None
        proc = subprocess.run(
            ["git", "-C", str(repo), "hash-object", "--stdin"],
            input=link_target,
            capture_output=True,
            text=True,
            errors="surrogateescape",
            check=False,
            timeout=_SUBPROCESS_TIMEOUT_S,
        )
        return proc.stdout.strip() or None
    if not target.is_file():
        return None
    return _git(repo, "hash-object", "--", str(target)).strip() or None


def _uncommitted_paths(wt: Path) -> list[str]:
    """Working-tree paths ``git status`` reports as changed.

    Parses ``--porcelain=v1 -z`` rather than splitting on newlines: a path
    containing a newline would otherwise drop silently out of a safety check,
    which is the one place a parser is not allowed to be approximate.
    """
    # ``-uall`` is load-bearing, not a flourish. Plain ``--porcelain`` collapses
    # an untracked directory to a single ``dir/`` entry, which is not a regular
    # file, so a per-path file scan skips it as though it were a deletion -- and
    # a lane worktree routinely carries whole untracked directories of work.
    # Reporting that as "all in commits" is the exact false assurance this
    # module exists to remove.
    fields = [f for f in _git(wt, "status", "--porcelain=v1", "-z", "-uall").split("\0") if f]
    paths: list[str] = []
    i = 0
    while i < len(fields):
        entry = fields[i]
        code, rel = entry[:2], entry[3:]
        paths.append(rel)
        if "R" in code or "C" in code:
            i += 1  # a rename/copy record is followed by its source path
        i += 1
    return paths


def inspect_worktree(repo: Path, wt: Path, blobs: dict[str, set[str]]) -> dict:
    """Classify one worktree by the reachability of its uncommitted content."""
    if not wt.is_dir():
        return {
            "worktree": str(wt),
            "status": UNKNOWN,
            "reason": "path_absent",
            "unreachable": [],
            "recoverable": [],
            "derived": [],
        }

    paths = _uncommitted_paths(wt)

    # Index content is content too, and no working-tree scan can see it. Two
    # distinct cells live here and both were missed by reading only the tree:
    #   * unmerged stages 1/2/3 from an in-progress or abandoned merge, and
    #   * a path staged and then unlinked (``git status`` reports ``AD``), whose
    #     only copy is the stage-0 index entry.
    # The second is the false-assurance case: ``git worktree remove --force``
    # discards it and nothing anywhere ever held it.
    tree_hash = {rel: _hash_worktree_file(repo, wt, rel) for rel in paths}

    # ``-z`` on every ``ls-files`` call, not just on ``status``. Without it
    # ``ls-files`` honours ``core.quotePath`` and prints a path with any
    # non-ASCII byte -- or a newline -- as a C-quoted string while ``status -z``
    # printed it raw, so the ``got == rel`` comparison below fails and the index
    # blob drops silently out of the scoring. That is the exact "a parser is not
    # allowed to be approximate" failure ``_uncommitted_paths`` was written to
    # avoid, one call over.
    staged: dict[str, set[str]] = defaultdict(set)
    unmerged_stages: dict[str, dict[str, str]] = {}
    for record in _git(wt, "ls-files", "-u", "-z").split("\0"):
        if not record:
            continue
        meta, _, rel = record.partition("\t")
        parts = meta.split()
        if len(parts) == 3:
            staged[rel].add(parts[1])
            unmerged_stages.setdefault(rel, {})[parts[2]] = parts[1]
    unmerged = set(unmerged_stages)
    # "Staged" means the index differs from HEAD -- not merely that a stage-0
    # entry exists, which is true of every tracked path. Reading stage 0 for an
    # ordinary unstaged edit yields the pre-change blob, already in a commit,
    # and reports the path twice.
    cached = {f for f in _git(wt, "diff", "--cached", "--name-only", "-z").split("\0") if f}
    for rel in paths:
        if rel in unmerged or rel not in cached:
            continue
        for record in _git(wt, "ls-files", "--stage", "-z", "--", rel).split("\0"):
            if not record:
                continue
            meta, _, got = record.partition("\t")
            parts = meta.split()
            if len(parts) == 3 and got == rel and parts[1] != tree_hash.get(rel):
                staged[rel].add(parts[1])

    if not paths and not staged:
        return {
            "worktree": str(wt),
            "status": CLEAN,
            "unreachable": [],
            "recoverable": [],
            "derived": [],
        }

    unreachable: list[dict] = []
    recoverable: list[dict] = []
    all_blobs: set[str] | None = None

    def score(rel: str, sha: str, origin: str) -> None:
        nonlocal all_blobs
        if sha in blobs.get(rel, ()):
            recoverable.append({"path": rel, "blob": sha, "origin": origin})
            return
        # Same bytes under a different path still means the content survives;
        # only compute the flattened set if the cheap per-path lookup missed.
        if all_blobs is None:
            all_blobs = set().union(*blobs.values()) if blobs else set()
        if sha in all_blobs:
            recoverable.append({"path": rel, "blob": sha, "origin": origin, "moved": True})
        else:
            unreachable.append({"path": rel, "blob": sha, "origin": origin})

    derived: list[dict] = []
    for rel in paths:
        sha = tree_hash.get(rel)
        if sha is None:
            continue
        stage_texts = {
            stage: _git(wt, "cat-file", "-p", blob) for stage, blob in sorted(unmerged_stages.get(rel, {}).items())
        }
        if rel in unmerged and _is_unresolved_conflict_artifact(wt / rel, stage_texts):
            derived.append({"path": rel, "blob": sha, "origin": "conflict-artifact"})
            continue
        score(rel, sha, "worktree")
    for rel, shas in staged.items():
        for sha in shas:
            score(rel, sha, "index-stage")

    return {
        "worktree": str(wt),
        "status": UNREACHABLE if unreachable else RECOVERABLE,
        "unreachable": unreachable,
        "recoverable": recoverable,
        "derived": derived,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Reap safety by reachability, not dirtiness.")
    ap.add_argument("--repo", default=".", help="path inside the repository")
    ap.add_argument("--json", action="store_true", help="emit the full record")
    ap.add_argument(
        "--only-unreachable",
        action="store_true",
        help="print only worktrees holding content no commit has",
    )
    args = ap.parse_args(argv)

    repo = Path(args.repo).resolve()
    blobs = _blob_index(repo)

    worktrees = [
        Path(line.split(" ", 1)[1])
        for line in _git(repo, "worktree", "list", "--porcelain").splitlines()
        if line.startswith("worktree ")
    ]
    records = [inspect_worktree(repo, wt, blobs) for wt in worktrees]

    if args.json:
        json.dump({"blob_paths_indexed": len(blobs), "worktrees": records}, sys.stdout, indent=2)
        print()
        return 0

    tally: dict[str, int] = defaultdict(int)
    for rec in records:
        tally[rec["status"]] += 1
        if args.only_unreachable and rec["status"] != UNREACHABLE:
            continue
        name = Path(rec["worktree"]).name
        if rec["status"] == UNREACHABLE:
            print(f"{UNREACHABLE:<13} {name}  ({len(rec['unreachable'])} blob(s) in no commit)")
            for item in rec["unreachable"]:
                print(f"                 - {item['path']}  [{item['origin']}]")
        elif not args.only_unreachable:
            extra = ""
            if rec["status"] == RECOVERABLE:
                extra = f"  ({len(rec['recoverable'])} uncommitted blob(s), all in commits)"
            elif rec["status"] == UNKNOWN:
                extra = f"  ({rec.get('reason', 'uninspectable')})"
            print(f"{rec['status']:<13} {name}{extra}")
        # A derived path was skipped, not scored, and folding that into the
        # same "all in commits" line as a scored path is the difference the
        # reader most needs and cannot otherwise see [OBS-08]. --json has
        # carried the list since the rule landed; the table now says so too.
        for item in rec.get("derived", ()):
            print(f"                 ~ {item['path']}  [derived, not scored]")

    print(
        "\n"
        + "  ".join(f"{k}={tally[k]}" for k in (CLEAN, RECOVERABLE, UNREACHABLE, UNKNOWN) if tally[k])
        + f"   (indexed {len(blobs)} paths across all refs)"
    )
    print("UNREACHABLE is the only status where removing the worktree loses work. Reclaim remains operator-authorized.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
