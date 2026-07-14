#!/usr/bin/env python3
"""Guard that a releasing package's CHANGELOG entry is not a bare authoring stub.

`release_prepare.py` seeds every version bump with a placeholder bullet
(``- TODO: summarize this release.``) so the author cannot forget the entry
exists. The public export (`export_public.py`) *silently strips* ``TODO``
bullets when it condenses the changelog — so a forgotten stub does not fail the
release, it ships as a **blank** public entry. The 2026-07-07 v0.1.37 release
would have shipped four such entries had they not been filled by hand.

This gate closes that hole: for each git-mirror package, it locates the section
for the package's *current* pyproject version and fails if that section is
missing or contains no substantive bullet (every bullet is a ``TODO`` stub).

It is deliberately format-tolerant: a version heading is any ``## `` line that
contains the current version string (``## [0.3.9] — 2026-07-07`` and
``## 0.3.9`` both match), and the section runs to the next ``## `` heading.

Run standalone (`make changelog-ready-check`) or via `release.sh` preflight.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11 — tomllib is stdlib only on 3.11+
    sys.stderr.write(
        "check-changelog-ready requires Python 3.11+ (tomllib); "
        f"got {sys.version.split()[0]}\n"
    )
    raise SystemExit(2) from None

_MANIFEST_REL = Path("config") / "release" / "packages.json"
# An authoring stub: a bullet whose text is a bare TODO. Mirrors
# ``export_public.py``'s ``_PLACEHOLDER_RE`` so the two agree on what "empty"
# means — the export strips exactly what this gate refuses to ship.
_BULLET_RE = re.compile(r"^\s*[-*]\s+(.*)$")
_PLACEHOLDER_RE = re.compile(r"^\s*TODO\b", re.IGNORECASE)
_HEADING_RE = re.compile(r"^\s*##\s+")


@dataclass
class Unready:
    name: str
    version: str
    reason: str


def _publishable_packages(repo_root: Path) -> list[dict]:
    manifest = json.loads((repo_root / _MANIFEST_REL).read_text(encoding="utf-8"))
    return [p for p in manifest.get("packages", []) if p.get("git_mirror")]


def _package_version(repo_root: Path, pkg_path: str) -> str:
    pyproject = tomllib.loads(
        (repo_root / pkg_path / "pyproject.toml").read_text(encoding="utf-8")
    )
    return str(pyproject["project"]["version"])


def _changelog_path(repo_root: Path, entry: dict) -> Path | None:
    changelog = entry.get("changelog")
    if changelog:
        return repo_root / str(changelog)
    default = repo_root / str(entry["path"]) / "CHANGELOG.md"
    return default if default.is_file() else None


def _section_bullets(text: str, version: str) -> list[str] | None:
    """Return the bullet lines under the heading naming ``version``.

    ``None`` means no heading for that version exists. An empty list means the
    heading exists but carries no bullets at all.
    """
    # Match the version as a delimited token, not a substring: current version
    # ``0.3.9`` must NOT match a ``## [0.3.90]`` heading. Version chars are
    # digits and dots, so require a non-[0-9.] boundary (or string edge) around
    # the match — covers ``## [0.3.9]``, ``## 0.3.9 — date``, ``## v0.3.9``.
    version_re = re.compile(rf"(?<![0-9.]){re.escape(version)}(?![0-9.])")
    lines = text.splitlines()
    start: int | None = None
    for i, line in enumerate(lines):
        if _HEADING_RE.match(line) and version_re.search(line):
            start = i + 1
            break
    if start is None:
        return None
    bullets: list[str] = []
    for line in lines[start:]:
        if _HEADING_RE.match(line):
            break
        match = _BULLET_RE.match(line)
        if match:
            bullets.append(match.group(1).strip())
    return bullets


def check(repo_root: Path, only: str | None = None) -> list[Unready]:
    unready: list[Unready] = []
    for entry in _publishable_packages(repo_root):
        name = str(entry["name"])
        if only is not None and name != only:
            continue
        version = _package_version(repo_root, str(entry["path"]))
        path = _changelog_path(repo_root, entry)
        if path is None:
            unready.append(
                Unready(name, version, "no CHANGELOG.md found for the package")
            )
            continue
        bullets = _section_bullets(path.read_text(encoding="utf-8"), version)
        if bullets is None:
            unready.append(
                Unready(
                    name,
                    version,
                    f"no CHANGELOG heading names version {version} "
                    f"({path.relative_to(repo_root)})",
                )
            )
            continue
        substantive = [b for b in bullets if b and not _PLACEHOLDER_RE.match(b)]
        if not substantive:
            unready.append(
                Unready(
                    name,
                    version,
                    f"CHANGELOG entry for {version} is an empty/TODO stub — "
                    "the public export would ship a blank entry "
                    f"({path.relative_to(repo_root)})",
                )
            )
    return unready


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="check-changelog-ready")
    parser.add_argument("--repo-root", default=None)
    parser.add_argument(
        "--package",
        default=None,
        help="Only check this git-mirror package (default: all).",
    )
    args = parser.parse_args(argv)

    if args.repo_root:
        repo_root = Path(args.repo_root).resolve()
    else:
        import subprocess

        top = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
        )
        repo_root = Path(top.stdout.strip() or ".").resolve()

    if args.package is not None:
        names = {p["name"] for p in _publishable_packages(repo_root)}
        if args.package not in names:
            sys.stderr.write(
                f"check-changelog-ready: --package {args.package!r} is not a "
                "git_mirror:true package in config/release/packages.json\n"
            )
            return 2

    unready = check(repo_root, only=args.package)
    if not unready:
        print("ok: every git-mirror package has a substantive CHANGELOG entry")
        return 0

    print("release changelog not ready — these entries would ship blank/stubbed:")
    for u in unready:
        print(f"  - {u.name} @ {u.version}: {u.reason}")
    print("Fill in the CHANGELOG entry (replace the TODO stub) before releasing.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
