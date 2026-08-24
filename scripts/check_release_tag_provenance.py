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

Exit 0 when all hold; exit 1 with one line per violated check otherwise.
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


def newest_release_tag(repo: str) -> str | None:
    """Newest monorepo ``vX.Y.Z`` tag (mirrors check_distribution_tag.py)."""
    proc = _git(repo, "tag", "--list", "v[0-9]*", "--sort=-v:refname")
    if proc.returncode != 0:
        return None
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

    tag = args.tag or newest_release_tag(args.repo)
    if tag is None:
        sys.stderr.write(
            "release-tag-provenance: no vX.Y.Z release tag found and none given\n"
        )
        return 1

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
