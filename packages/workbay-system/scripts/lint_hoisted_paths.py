#!/usr/bin/env python3
"""Detect portability leaks in hoisted workbay-system surfaces."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from scripts.overlay_resolver import OverlayResolverError, resolve_surface
except ModuleNotFoundError:
    from overlay_resolver import OverlayResolverError, resolve_surface

REPO_ROOT = Path(__file__).resolve().parents[1]
SURFACE_KINDS = ("skills", "hooks", "commands", "prompts", "contracts")
DIRECT_SCAN_ROOTS = {
    "workflows": (Path("config/agent-workflows"),),
}
SKIPPED_DIR_NAMES = {"__pycache__"}
SKIPPED_SUFFIXES = {".pyc"}
ALLOWED_RULES_BY_PATH: dict[str, set[str]] = {
    ".github/hooks/guard-main-branch.py": {"brittle-file-parent-walk"},
    ".github/hooks/guard-worktree-drift.py": {"brittle-file-parent-walk"},
    "scripts/hooks/_worktree_drift.py": {"brittle-file-parent-walk"},
    "scripts/hooks/lint-dashboard-txt.py": {"brittle-file-parent-walk"},
    "scripts/hooks/lint-expected-revision.py": {"brittle-file-parent-walk"},
    "scripts/hooks/lint-no-inline-python-heredoc.py": {"brittle-file-parent-walk"},
}


@dataclass(frozen=True)
class ScanTarget:
    category: str
    path: Path


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    categories: frozenset[str] | None = None

    def applies_to(self, category: str) -> bool:
        return self.categories is None or category in self.categories


_LEGACY_REPO_NAME = "context" + "-alt-text-" + "monorepo"
_LEGACY_APP_PREFIX = "apps/" + "prototype"

RULES: tuple[Rule, ...] = (
    Rule("hardcoded-user-home", re.compile(r"/Users/[^/\s]+(?:/|$)")),
    Rule("repo-name-assumption", re.compile(rf"\b{_LEGACY_REPO_NAME}(?:-[a-z0-9-]+)?\b")),
    Rule("python-version-probe", re.compile(r"\.python-version\b")),
    Rule(
        "monorepo-app-path",
        re.compile(rf"\b{_LEGACY_APP_PREFIX}(?:-[a-z0-9-]+)?\b"),
        frozenset({"commands", "prompts", "hooks", "workflows"}),
    ),
    Rule(
        "brittle-file-parent-walk",
        re.compile(r"Path\(__file__\)\.resolve\(\)\.parents\[\d+\]"),
        frozenset({"commands", "hooks", "workflows"}),
    ),
)


def _iter_targets(repo_root: Path) -> Iterable[ScanTarget]:
    seen: set[tuple[str, Path]] = set()

    for kind in SURFACE_KINDS:
        for resolved in resolve_surface(kind, repo_root):
            key = (kind, resolved.effective_path.resolve())
            if key in seen:
                continue
            seen.add(key)
            yield ScanTarget(category=kind, path=resolved.effective_path)

    for category, roots in DIRECT_SCAN_ROOTS.items():
        for root in roots:
            absolute_root = repo_root / root
            if not absolute_root.exists():
                continue
            key = (category, absolute_root.resolve())
            if key in seen:
                continue
            seen.add(key)
            yield ScanTarget(category=category, path=absolute_root)


def _iter_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return

    for candidate in sorted(path.rglob("*")):
        if not candidate.is_file():
            continue
        if any(part in SKIPPED_DIR_NAMES for part in candidate.parts):
            continue
        if candidate.suffix in SKIPPED_SUFFIXES:
            continue
        yield candidate


def _is_test_fixture(relative_path: Path) -> bool:
    if any(part == "tests" for part in relative_path.parts):
        return True
    name = relative_path.name
    return name.startswith("test_") or name.endswith("_test.py")


def _iter_findings(file_path: Path, *, repo_root: Path, category: str) -> Iterable[str]:
    relative_path = file_path.relative_to(repo_root)
    if _is_test_fixture(relative_path):
        return

    try:
        lines = file_path.read_text().splitlines()
    except UnicodeDecodeError:
        return

    allowed_rules = ALLOWED_RULES_BY_PATH.get(relative_path.as_posix(), set())
    for line_number, line in enumerate(lines, start=1):
        for rule in RULES:
            if rule.name in allowed_rules or not rule.applies_to(category):
                continue
            if not rule.pattern.search(line):
                continue
            findings_line = f"{relative_path.as_posix()}:{line_number}: {rule.name}"
            yield findings_line


def lint_hoisted_paths(*, repo_root: Path = REPO_ROOT) -> tuple[list[str], int]:
    repo_root = repo_root.resolve()
    findings: list[str] = []

    try:
        for target in _iter_targets(repo_root):
            for file_path in _iter_files(target.path):
                findings.extend(_iter_findings(file_path, repo_root=repo_root, category=target.category))
    except OverlayResolverError as exc:
        return [f"infrastructure error: {exc}"], 1

    findings.sort()
    return findings, 0 if not findings else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root to scan. Defaults to the current monorepo root.",
    )
    args = parser.parse_args(argv)

    findings, exit_code = lint_hoisted_paths(repo_root=Path(args.repo_root))
    if findings:
        heading = "lint-hoisted-paths: FAILED" if not findings[0].startswith("infrastructure error:") else "lint-hoisted-paths: ERROR"
        stream = sys.stderr
        print(heading, file=stream)
        for finding in findings:
            print(f"  - {finding}", file=stream)
        return exit_code

    print("lint-hoisted-paths: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())