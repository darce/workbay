#!/usr/bin/env python3
"""Guard: forbid `DASHBOARD.md` reappearing in tracked non-archive paths.

internal renamed the operator snapshot from `DASHBOARD.md` to
`DASHBOARD.txt`. Archived task plans and test fixtures may still mention
the old name verbatim — those are excluded. Every other tracked file
must reference `DASHBOARD.txt`.

Wired into `make lint-dashboard-txt` and `make check-all` so CI catches
regressions on every branch. internal.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable

OFFENDING_TOKEN = "DASHBOARD.md"

EXCLUDED_SUBSTRINGS = (
    "docs/tasks/archive/",
    "/test_fixtures/",
    "/tests/fixtures/",
    "/tests/",  # any nested tests/fixtures/ path (matched below too)
)

EXCLUDED_EXACT_FILES = {
    "scripts/hooks/lint-dashboard-txt.py",
    "scripts/hooks/test_lint_dashboard_txt.py",
    ".gitignore",
    "scripts/check_harness_sync.py",
    "scripts/test_check_harness_sync.py",
}

EXCLUDED_PATH_PREFIXES = (
    "docs/assessments/dashboard-md-vs-txt-",
)

ALLOW_MARKER = "<!-- lint-dashboard-txt: allow -->"


def is_excluded(path: Path) -> bool:
    posix = path.as_posix()
    for exact in EXCLUDED_EXACT_FILES:
        if posix == exact or posix.endswith("/" + exact):
            return True
    for prefix in EXCLUDED_PATH_PREFIXES:
        if prefix in posix:
            return True
    if "docs/tasks/archive/" in posix:
        return True
    if "/test_fixtures/" in posix or posix.startswith("test_fixtures/"):
        return True
    parts = path.parts
    for i, part in enumerate(parts):
        if part == "tests" and i + 1 < len(parts) and parts[i + 1] == "fixtures":
            return True
        if part == "tests" and any(
            p.endswith(".py") for p in parts[i + 1 :]
        ):
            return True
    return False


def scan_paths(paths: Iterable[Path]) -> list[str]:
    violations: list[str] = []
    for path in paths:
        if not path.is_file():
            continue
        if is_excluded(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if OFFENDING_TOKEN in line and ALLOW_MARKER not in line:
                violations.append(f"{path}:{lineno}: {line.strip()}")
    return violations


def _tracked_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [repo_root / line for line in result.stdout.splitlines() if line]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Repository root (defaults to the monorepo root).",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    tracked = _tracked_files(repo_root)
    # Normalize to repo-relative so the exclusion patterns match.
    rel_paths = [p.relative_to(repo_root) for p in tracked]
    violations = scan_paths(rel_paths)

    if violations:
        print("DASHBOARD.md drift detected — the canonical file is DASHBOARD.txt.", file=sys.stderr)
        print("Offending references:", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        print(
            "\nFix: replace `DASHBOARD.md` with `DASHBOARD.txt`, or move the reference "
            "into `docs/tasks/archive/**` or a test fixture if it is intentional.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
