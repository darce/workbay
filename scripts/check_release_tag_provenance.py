#!/usr/bin/env python3
"""release-tag-provenance check (REQUEST 2026-08-23): a monorepo release tag
must be annotated, pushed to the dev ``origin``, and carry a truthful
``distro:`` mapping to the published ``darce/workbay`` tag.

The observed failure mode: ``v0.1.58`` existed only as a LIGHTWEIGHT,
LOCAL-ONLY tag in the dev monorepo while the published distro carried a
different SHA under the same name — unverifiable except by hand. This gate
makes the three provenance properties checkable:

  (i)   the tag is annotated (``git cat-file -t`` -> ``tag``), so it carries
        tagger/date/message provenance;
  (ii)  the tag exists on ``origin`` (``git ls-remote``), so other clones can
        fetch and verify it;
  (iii) its tag-message ``distro:`` line is either the explicit degrade value
        ``distro: pending`` or ``distro: darce/workbay@<sha>`` matching the
        live distro tag SHA from ``git ls-remote``.

Exit 0 when all hold; exit 1 with one line per violated check otherwise, or
when the repo could not be READ at all (a failed tag enumeration is instrument
failure, never a pass); exit 2 on a usage error (an empty tag argument).
``--offline`` skips the two ls-remote checks (CI/sandbox); the local facts
(annotated + well-formed ``distro:`` line) are still enforced. The distro URL
defaults to ``PUBLIC_GIT_REMOTE`` in the neighboring ``release_public.py``
(single source — mirrors how ``release.sh`` factors it); ``--distro-url``
overrides it, e.g. with a ``file://`` fixture. Stdlib-only; follows the
main()/SystemExit style of ``scripts/check_distribution_tag.py``.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

DISTRO_SLUG = "darce/workbay"
DISTRO_LINE_RE = re.compile(
    rf"^distro: (?:pending|{re.escape(DISTRO_SLUG)}@(?P<sha>[0-9a-f]{{40}}))$"
)
PUBLIC_REMOTE_RE = re.compile(r'^PUBLIC_GIT_REMOTE = "(?P<url>.+)"$', re.MULTILINE)
SEMVER_TAG = re.compile(r"^v\d+\.\d+\.\d+$")


def _git(repo: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", repo, *args],
        capture_output=True,
        text=True,
    )


class TagEnumerationError(RuntimeError):
    """``git tag --list`` itself failed — the repo could not be READ.

    Kept distinct from "enumeration succeeded and matched nothing": the second
    is a legitimate SKIP, this one is INSTRUMENT FAILURE (``--repo`` pointing
    at a non-repo, a dubious-ownership refusal, a broken or absent git) and
    must never be reported as a pass.
    """


def newest_release_tag(repo: str) -> str | None:
    """Newest monorepo ``vX.Y.Z`` tag (mirrors check_distribution_tag.py).

    Returns None ONLY for a successful enumeration that matched no release
    tag. Raises TagEnumerationError when git itself failed, so a caller cannot
    collapse "nothing to check" into "could not check".
    """
    args = ("tag", "--list", "v[0-9]*", "--sort=-v:refname")
    proc = _git(repo, *args)
    if proc.returncode != 0:
        raise TagEnumerationError(
            f"git -C {repo} {' '.join(args)} failed (exit {proc.returncode}): "
            f"{proc.stderr.strip() or '<no stderr>'}"
        )
    for tag in proc.stdout.split():
        if SEMVER_TAG.match(tag):
            return tag
    return None


def default_distro_url() -> str | None:
    """PUBLIC_GIT_REMOTE from the neighboring release_public.py (text parse:
    stdlib-only, and importing the release script could drag in its deps)."""
    source = Path(__file__).resolve().parent / "release_public.py"
    try:
        match = PUBLIC_REMOTE_RE.search(source.read_text(encoding="utf-8"))
    except OSError:
        return None
    return match.group("url") if match else None


def _ls_remote_tag_sha(repo: str, remote: str, tag: str) -> str | None:
    """SHA of ``refs/tags/<tag>`` on ``remote`` (exact ref, not the peeled
    ``^{}`` line), or None when absent / unreachable."""
    proc = _git(repo, "ls-remote", "--tags", remote, f"refs/tags/{tag}")
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == f"refs/tags/{tag}":
            return parts[0]
    return None


def check_tag(
    tag: str, *, repo: str, distro_url: str | None, offline: bool
) -> list[str]:
    """Return one violation line per failed check (empty = all pass)."""
    violations: list[str] = []

    tag_type = _git(repo, "cat-file", "-t", tag)
    if tag_type.returncode != 0:
        return [f"tag {tag} does not exist in {repo}"]
    if tag_type.stdout.strip() != "tag":
        violations.append(
            f"tag {tag} is not annotated (lightweight '{tag_type.stdout.strip()}' "
            "object — cut with 'git tag -a')"
        )

    message = _git(repo, "tag", "-l", "--format=%(contents)", tag).stdout
    distro_lines = [
        line for line in message.splitlines() if line.startswith("distro:")
    ]
    recorded_sha: str | None = None
    pending = False
    if not distro_lines:
        violations.append(
            f"tag {tag} message has no 'distro:' line "
            f"(expected 'distro: pending' or 'distro: {DISTRO_SLUG}@<sha>')"
        )
    else:
        match = DISTRO_LINE_RE.match(distro_lines[0])
        if match is None:
            violations.append(
                f"tag {tag} 'distro:' line is malformed: {distro_lines[0]!r}"
            )
        elif match.group("sha") is None:
            pending = True
        else:
            recorded_sha = match.group("sha")

    if offline:
        return violations

    if _ls_remote_tag_sha(repo, "origin", tag) is None:
        violations.append(
            f"tag {tag} not found on origin — run: git push origin {tag}"
        )

    if recorded_sha is not None:
        if distro_url is None:
            violations.append(
                f"tag {tag} records a distro SHA but no distro URL is "
                "resolvable (release_public.py PUBLIC_GIT_REMOTE missing "
                "and no --distro-url)"
            )
        else:
            live_sha = _ls_remote_tag_sha(repo, distro_url, tag)
            if live_sha != recorded_sha:
                violations.append(
                    f"tag {tag} distro mismatch: message records "
                    f"{DISTRO_SLUG}@{recorded_sha} but {distro_url} has "
                    f"{live_sha or '<no such tag>'}"
                )
    elif pending:
        # 'distro: pending' is a valid degrade value by contract — the cut
        # never fails on an unpublished distro tag, and neither does this gate.
        # But 'pending' with NO resolvable distro URL is indistinguishable from
        # the silent-degrade failure mode (release_public.py's
        # PUBLIC_GIT_REMOTE drifted and every cut records pending forever), so
        # online it is a violation of the distro check, not a pass.
        if distro_url is None:
            violations.append(
                f"tag {tag} records 'distro: pending' but no distro URL is "
                "resolvable (release_public.py PUBLIC_GIT_REMOTE not "
                "parseable and no --distro-url) — cannot confirm the pending "
                "degrade is genuine"
            )

    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "tag",
        nargs="?",
        default=None,
        help="release tag to check (default: newest v* tag in the repo)",
    )
    parser.add_argument(
        "--repo",
        default=".",
        help="path to the git repo to check (default: current directory)",
    )
    parser.add_argument(
        "--distro-url",
        default=None,
        help=(
            "published distro remote URL (default: PUBLIC_GIT_REMOTE from "
            "scripts/release_public.py)"
        ),
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="skip the two ls-remote checks (origin presence, distro SHA)",
    )
    args = parser.parse_args()

    # `is not None`, NOT truthiness: an EMPTY tag argument is a caller bug
    # (a shell caller quoting an unset variable), not a request to fall back
    # to the newest tag. Under truthiness it would have silently taken the
    # branch below and could SKIP at exit 0 -- failing open on exactly the
    # caller that lost its tag. Rejected here with a distinct exit 2 (usage),
    # so it can never be confused with a clean pass or a real violation.
    if args.tag is not None:
        if not args.tag.strip():
            sys.stderr.write(
                "release-tag-provenance: empty tag argument -- pass a real "
                "vX.Y.Z tag, or omit the argument entirely to check the "
                "newest release tag in the repo\n"
            )
            return 2
        tag = args.tag
    else:
        try:
            tag = newest_release_tag(args.repo)
        except TagEnumerationError as exc:
            # Enumeration could not RUN. Before this split both outcomes
            # returned None and collapsed into the SKIP below, which turned
            # instrument failure into a pass.
            sys.stderr.write(f"release-tag-provenance: {exc}\n")
            return 1
        if tag is None:
            # Reached only when no tag was named on the command line AND the
            # enumeration SUCCEEDED but matched nothing. Benign causes are the
            # common ones (fresh clone, CI shallow checkout, a history-stripped
            # tree with no release cut yet), but this branch is not limited to
            # them -- it is every successful-but-empty enumeration; a git that
            # could not read the repo at all raises above and exits 1 instead.
            # A tag named explicitly on the command line that cannot be
            # resolved does NOT reach here -- it becomes `tag` above and fails
            # in check_tag() with a real "does not exist" violation (exit 1),
            # which is correct: the caller asserted a specific tag that is not
            # there. Absence of a subject is not a violation by the subject, so
            # this prints a SKIP line -- distinguishable from the "ok" pass
            # line below at a glance -- and exits 0, so the check can be wired
            # into a chain that also has to pass on a tagless tree.
            print(
                "release-tag-provenance: SKIP -- no vX.Y.Z release tag found "
                "and none given (nothing to verify)"
            )
            return 0

    distro_url = args.distro_url or default_distro_url()
    if args.distro_url is None and distro_url is None:
        sys.stderr.write(
            "release-tag-provenance: PUBLIC_GIT_REMOTE not parseable from "
            "release_public.py and no --distro-url given\n"
        )
    violations = check_tag(
        tag, repo=args.repo, distro_url=distro_url, offline=args.offline
    )
    if violations:
        sys.stderr.write(
            f"release-tag-provenance: {len(violations)} check(s) failed for {tag}:\n"
        )
        for line in violations:
            sys.stderr.write(f"  {line}\n")
        return 1
    mode = " (offline: remote checks skipped)" if args.offline else ""
    print(f"release-tag-provenance: ok ({tag}){mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
