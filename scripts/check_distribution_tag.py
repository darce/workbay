#!/usr/bin/env python3
"""distribution-tag-check (audit PPSSOT-TAG-01): consumer install docs must pin
the SAME monorepo tag.

The consumer-facing install surfaces (the shared ``CONSUMER_INSTALL_DOCS`` set:
root README, CONSUMER, UPGRADING, RELEASING, and each package README) each
hand-copied a `git+...@<tag>` pin, and they drifted apart (v0.1.28 / v0.2.1 / …)
with no gate — a Shotgun-Surgery hazard where a front-door release silently
leaves stale, mutually-inconsistent install snippets. This gate FAILS when the
version-pinned tags disagree, or when the single agreed pin is not a real
released git tag (the phantom-pin guard — e.g. the retired ``--remote-ref
v0.2.1``). It only WARNS when the agreed pin is a real tag that merely lags the
latest release: doc pins legitimately trail a fresh tag until the follow-up
doc-bump commit lands (the release flow does not auto-rewrite them), and a
maintainer may pre-stage a bump before its tag is cut, so a hard fail there
false-reds `main` for a purely advisory lag. `@main` and doc links
(`/tree/main`, `/blob/main`) are branch refs, not version pins, and are ignored.
Mirrors the fail-closed posture of `scripts/check_brand.py`.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from _distribution_scan import CONSUMER_INSTALL_DOCS

REPO_ROOT = Path(__file__).resolve().parents[1]
SCAN_GLOBS = CONSUMER_INSTALL_DOCS
# A version pin in any of four spellings that appear in the install snippets and
# MUST agree: the git spec ``darce/workbay(.git)?@v<semver>``, the bootstrap flag
# ``--remote-ref v<semver>``, the pyproject / ``uv add`` TOML form
# ``tag = "v<semver>"``, or the shell-var assignment ``REF=v<semver>`` the
# copy-paste closure snippets lead with (``REF=v0.1.35`` then ``R="…@$REF"``).
# Catching only the git spec let ``--remote-ref v0.2.1`` drift to a phantom tag
# while ``@v0.1.35`` stayed green; omitting ``REF=`` left the package READMEs
# whose snippets pin ONLY via the shell var (workbay, workbay-bootstrap,
# mcp-workbay-orchestrator) invisible to the gate (PPSSOT-TAG / HARM). Branch
# refs like ``@main`` / ``--remote-ref main`` are not version pins.
PIN_RE = re.compile(
    r"(?:darce/workbay(?:\.git)?@|--remote-ref |tag\s*=\s*[\"']|REF=)(v\d+\.\d+\.\d+)"
)
# A bare monorepo release tag, for validating scan hits against ``git tag``.
SEMVER_TAG = re.compile(r"^v\d+\.\d+\.\d+$")


def scan(repo_root: Path = REPO_ROOT) -> dict[str, list[str]]:
    """Return {tag: [path:line hits]} of version-pinned install refs."""
    proc = subprocess.run(
        [
            "git",
            "grep",
            "-nIE",
            r"(darce/workbay(\.git)?@|--remote-ref |tag[[:space:]]*=[[:space:]]*[\"']|REF=)v[0-9]+\.[0-9]+\.[0-9]+",
            "--",
            *SCAN_GLOBS,
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if proc.returncode > 1:
        raise RuntimeError(f"git grep failed (rc={proc.returncode}): {proc.stderr}")
    by_tag: dict[str, list[str]] = {}
    for line in proc.stdout.splitlines():
        for tag in PIN_RE.findall(line):
            path_line = ":".join(line.split(":", 2)[:2])
            by_tag.setdefault(tag, []).append(path_line)
    return by_tag


def released_monorepo_tags(repo_root: Path = REPO_ROOT) -> list[str]:
    """Monorepo ``vX.Y.Z`` release tags, newest first. Per-package
    ``<pkg>-vX.Y.Z`` tags start with the package name, not ``v``, and are
    excluded. Empty in a shallow / tagless checkout (existence check is then
    skipped rather than false-failing)."""
    proc = subprocess.run(
        ["git", "tag", "--list", "v[0-9]*", "--sort=-v:refname"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git tag failed (rc={proc.returncode}): {proc.stderr}")
    return [t for t in proc.stdout.split() if SEMVER_TAG.match(t)]


def main() -> int:
    by_tag = scan()
    if len(by_tag) > 1:
        sys.stderr.write(
            "distribution-tag-check: consumer install docs pin DIFFERENT monorepo "
            "tags — update them all to one released tag:\n"
        )
        for tag in sorted(by_tag):
            sys.stderr.write(f"  {tag}: {', '.join(by_tag[tag])}\n")
        return 1
    if not by_tag:
        print("distribution-tag-check: ok (no version-pinned install refs found)")
        return 0
    (tag,) = by_tag  # exactly one agreed pin
    released = released_monorepo_tags()
    if not released:
        # No monorepo tags in this checkout (shallow / tagless clone): fall back
        # to the intra-doc consistency guarantee; existence cannot be verified.
        print(
            f"distribution-tag-check: ok (consumer install docs consistently pin {tag}; "
            "tag existence unverified — no release tags in this checkout)"
        )
        return 0
    if tag not in released:
        sys.stderr.write(
            f"distribution-tag-check: consumer install docs pin {tag}, which is NOT a "
            f"released monorepo tag (latest released: {released[0]}) — pins must name a "
            f"real released tag:\n  {', '.join(by_tag[tag])}\n"
        )
        return 1
    if tag != released[0]:
        # Lagging (not phantom): WARN, do not fail. The pin names a real released
        # tag that is simply not the newest. Doc pins legitimately trail a fresh
        # tag until the follow-up doc-bump commit lands, and a maintainer may
        # pre-stage a bump before its tag exists; a hard fail here false-reds
        # `main` for a purely advisory lag. Mutual drift (len > 1) and phantom
        # pins (tag not in `released` at all) above stay hard failures — those
        # are the real hazards this gate exists to catch.
        sys.stderr.write(
            f"distribution-tag-check: WARNING — consumer install docs pin {tag}, but the "
            f"latest released monorepo tag is {released[0]}. Bump the pins when convenient "
            f"(advisory, not a failure):\n  {', '.join(by_tag[tag])}\n"
        )
        print(
            f"distribution-tag-check: ok (consumer install docs consistently pin {tag}, "
            f"a real released tag lagging latest {released[0]} — see warning above)"
        )
        return 0
    print(
        f"distribution-tag-check: ok (consumer install docs consistently pin {tag}, "
        "the latest released monorepo tag)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
