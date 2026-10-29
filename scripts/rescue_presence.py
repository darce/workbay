#!/usr/bin/env python3
"""Score a rescued sandbox patch against a repository: is this work already landed?

A remote sandbox is not a branch. Work sits in one with no branch and no lane
row, and the disk pressure that forces a reap is exactly when it gets lost. The
rescue directory (``docs/rescues/<date>-vm-sandboxes/``) exists so that reaping
is reversible. This script answers the one question a reaper needs answered:
does the repository already contain every line the sandbox added?

Two lessons from the 2026-09-06 rescue are encoded here, because both produced
a wrong "safe to reap" reading:

1. ``git diff HEAD`` is blind to commits. Eleven sandboxes held local commits
   above their history-stripped base, and a dirty-diff-only capture reported
   them as clean. A complete capture is two patches per sandbox: the working
   tree against HEAD, and the base commit against HEAD.
2. A sandbox does not carry its origin repository. Three sandboxes in that
   rescue were rooted in a *different* monorepo, so their patches refused to
   apply and read as unrecoverable work. Pass the right ``repo``; a patch whose
   paths are absent from every candidate revision is reported as such rather
   than being silently scored 0.

Presence is per file and per line: an added line counts as present if it
appears verbatim in any candidate revision's copy of that path. That is
deliberately loose. It answers "is this content here" and never "is this the
same change", so a PARTIAL or UNLANDED verdict is a reason to read the patch,
not a reason to apply it blind. LANDED is the only verdict that licenses a reap.

Exit status is always 0. This is an instrument, not a gate: a gate that guessed
at absence would eventually authorize deleting the only copy of some work.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def _git(repo: str, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", repo, *args],
        capture_output=True,
        text=True,
    )
    return proc.stdout if proc.returncode == 0 else ""


def resolve_pool(
    repo: str, revs: list[str], discovered_total: int | None = None
) -> dict[str, object]:
    """Split the requested revisions into those that name a commit and those that do not.

    ``git show <rev>:<path>`` fails identically for "this revision does not
    exist" and "this revision does not contain that path", and ``score_path``
    reads both as no-contribution. So a mistyped or since-deleted branch used
    to shrink the search pool without changing a single character of the
    verdict, which is an operator typo silently upgrading itself into a
    stronger absence claim. Resolution is decided once, here, against
    ``rev-parse --verify``, and both halves are reported. [EVAL-25]
    """
    resolved: list[str] = []
    unresolved: list[str] = []
    for rev in revs:
        if _git(repo, "rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}").strip():
            resolved.append(rev)
        else:
            unresolved.append(rev)
    return {
        "requested": list(revs),
        "resolved": resolved,
        "unresolved": unresolved,
        # Candidates discovery found but the bound never opened. Zero is a
        # searched-everything claim; anything else is not. [OBS-08]
        "truncated": max(0, (discovered_total or len(revs)) - len(revs)),
    }


DEFAULT_POOL_LIMIT = 64


def discover_revs(repo: str, integration: str = "main") -> list[str]:
    """Every local branch tip worth searching, integration tip first.

    Ordered by commit date, newest first, because a rescued sandbox is
    contemporary with the lanes that were running when it was reaped, and a
    truncated pool should drop the oldest candidates rather than arbitrary
    ones. The integration tip is pinned to the front so it survives any bound.
    """
    out = _git(
        repo,
        "for-each-ref",
        "--sort=-committerdate",
        "--format=%(refname:short)",
        "refs/heads/",
    )
    branches = [ln.strip() for ln in out.splitlines() if ln.strip()]
    ordered = [b for b in branches if b != integration]
    if integration in branches or _git(
        repo, "rev-parse", "--verify", "--quiet", f"{integration}^{{commit}}"
    ).strip():
        ordered.insert(0, integration)
    return ordered


def default_revs(
    repo: str, integration: str = "main", limit: int = DEFAULT_POOL_LIMIT
) -> list[str]:
    """The candidate pool to use when the operator names none.

    Defaulting to ``main`` alone is a self-pool: it scores every lane branch as
    known-nonrelevant without opening one, and the reason to run this
    instrument at all is not knowing where the work went. So the default is
    discovered, not assumed. It is bounded, because discovery is unbounded by
    nature -- but the caller is told how many candidates the bound dropped, so
    a truncated search never reads as an exhausted one. [EVAL-25]
    """
    return discover_revs(repo, integration)[:limit]


def added_lines_by_path(patch_text: str) -> dict[str, list[str]]:
    """Map each patched path to the non-blank lines the patch adds.

    Paths come from the ``+++ b/`` header rather than ``diff --git`` so that a
    rename records the destination, which is where the content has to be found.
    """
    out: dict[str, list[str]] = {}
    current: str | None = None
    for line in patch_text.splitlines():
        if line.startswith("diff --git a/") and " b/" in line:
            current = line.split(" b/", 1)[1]
            out.setdefault(current, [])
        elif line.startswith("+++ b/"):
            current = line[6:]
            out.setdefault(current, [])
        elif line.startswith("+++ /dev/null"):
            current = None
        elif current is not None and line.startswith("+") and not line.startswith("+++"):
            body = line[1:].strip()
            if body:
                out[current].append(body)
    return out


def score_path(repo: str, path: str, adds: list[str], revs: list[str]) -> dict[str, object]:
    pools: list[set[str]] = []
    for rev in revs:
        blob = _git(repo, "show", f"{rev}:{path}")
        if blob:
            pools.append({ln.strip() for ln in blob.splitlines()})
    present = sum(1 for a in adds if any(a in pool for pool in pools)) if pools else 0
    return {
        "path": path,
        "added": len(adds),
        "present": present,
        "path_found": bool(pools),
    }


def score_patch(
    repo: str, patch: Path, revs: list[str], discovered_total: int | None = None
) -> dict[str, object]:
    pool = resolve_pool(repo, revs, discovered_total)
    by_path = added_lines_by_path(patch.read_text(errors="replace"))
    files = [
        score_path(repo, path, adds, pool["resolved"])
        for path, adds in sorted(by_path.items())
        if adds
    ]
    added = sum(int(f["added"]) for f in files)
    present = sum(int(f["present"]) for f in files)
    if added and present == added:
        verdict = "LANDED"
    elif present:
        verdict = "PARTIAL"
    else:
        verdict = "UNLANDED"
    return {
        "patch": patch.name,
        "repo": repo,
        "revisions": revs,
        "pool": pool,
        "verdict": verdict,
        "present": present,
        "added": added,
        "files": files,
    }


def render(report: dict[str, object]) -> str:
    """Render the verdict with its pool on the same line.

    Presence generalizes from a subset and absence does not: a line found in
    ``main`` is found, but a line missing from ``main`` is only missing from
    ``main``. So a negative verdict is a claim about the pool, not about the
    repository, and printing it without the pool is what lets it get quoted as
    an unqualified fact. The pool rides on the verdict line rather than one
    level down in the JSON, because the verdict line is what gets copied into
    a finding. [EVAL-25]
    """
    pool = report.get("pool") or {}
    resolved = list(pool.get("resolved", []))  # type: ignore[union-attr]
    unresolved = list(pool.get("unresolved", []))  # type: ignore[union-attr]
    truncated = int(pool.get("truncated", 0) or 0)  # type: ignore[union-attr]
    shown = resolved if len(resolved) <= 4 else resolved[:4] + [f"+{len(resolved) - 4} more"]
    scope = f"vs {','.join(shown) if resolved else '<empty pool>'}"
    if unresolved:
        scope += f" (unresolved: {','.join(unresolved)})"
    if truncated:
        scope += f" (truncated: {truncated} candidates never opened)"
    lines = [
        f"{report['patch']}\t{report['verdict']}\t"
        f"{report['present']}/{report['added']}\t{scope}"
    ]
    for f in report["files"]:  # type: ignore[index]
        note = "" if f["path_found"] else "  (path absent in every resolved revision)"
        lines.append(f"    {f['present']}/{f['added']}\t{f['path']}{note}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ and __doc__.splitlines()[0])
    parser.add_argument("patch", type=Path, help="rescued patch to score")
    parser.add_argument(
        "--repo",
        default=".",
        help="repository the sandbox was rooted in (a sandbox does not carry this)",
    )
    parser.add_argument(
        "--rev",
        action="append",
        default=None,
        dest="revs",
        help="candidate revision; repeatable. Defaults to every local branch tip.",
    )
    parser.add_argument(
        "--pool-limit",
        type=int,
        default=DEFAULT_POOL_LIMIT,
        help=f"cap on discovered candidates (default {DEFAULT_POOL_LIMIT}); "
        "anything dropped is reported, never hidden",
    )
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = parser.parse_args(argv)

    if not args.patch.is_file():
        print(f"no such patch: {args.patch}", file=sys.stderr)
        return 0

    if args.revs:
        revs, discovered = args.revs, None
    else:
        discovered = len(discover_revs(args.repo))
        revs = default_revs(args.repo, limit=args.pool_limit)

    report = score_patch(args.repo, args.patch, revs, discovered_total=discovered)
    print(json.dumps(report, indent=2) if args.json else render(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
