#!/usr/bin/env python3
"""Fail when one pytest invocation's test roots share a module basename.

Pytest collects both roots in ``Makefile:271`` without package markers, so two
``test_*.py`` files with the same basename collide during import. This gate
lists those files directly; it never imports test modules or runs pytest. The
comparison is across the two collection roots: nested test packages under the
workbay-system tree have ``__init__.py`` markers and therefore qualified names.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]

# Keep these roots in lockstep with the single pytest invocation at Makefile:271.
# If that collection command changes, update this list in the same change.
COLLECTED_TEST_TREES = (
    Path("packages/workbay-system/tests"),
    Path("scripts/tests"),
)


def duplicate_test_basenames(repo_root: Path) -> dict[str, tuple[str, ...]]:
    """Return cross-root ``test_*.py`` basename collisions and their paths."""
    paths_by_tree: list[dict[str, list[str]]] = []
    missing_trees: list[str] = []

    for relative_tree in COLLECTED_TEST_TREES:
        tree = repo_root / relative_tree
        if not tree.is_dir():
            missing_trees.append(relative_tree.as_posix())
            continue
        tree_paths: defaultdict[str, list[str]] = defaultdict(list)
        for path in tree.rglob("test_*.py"):
            if path.is_file():
                tree_paths[path.name].append(path.relative_to(repo_root).as_posix())
        paths_by_tree.append(tree_paths)

    if missing_trees:
        missing = ", ".join(missing_trees)
        raise FileNotFoundError(f"pytest collection tree(s) missing: {missing}")

    names_by_tree: defaultdict[str, list[list[str]]] = defaultdict(list)
    for tree_paths in paths_by_tree:
        for basename, paths in tree_paths.items():
            names_by_tree[basename].append(paths)

    return {
        basename: tuple(sorted(path for paths in paths_by_name for path in paths))
        for basename, paths_by_name in sorted(names_by_tree.items())
        if len(paths_by_name) > 1
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="fail when collected test trees contain duplicate test_*.py basenames")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()

    try:
        duplicates = duplicate_test_basenames(repo_root)
    except OSError as exc:
        print(f"duplicate-test-basenames: FAIL: cannot list collected test trees — {exc}", file=sys.stderr)
        return 1

    if duplicates:
        for basename, paths in duplicates.items():
            print(
                f"duplicate-test-basenames: FAIL: shared basename {basename!r} appears in:",
                file=sys.stderr,
            )
            for path in paths:
                print(f"  {path}", file=sys.stderr)
            print("  rename one of these test files so pytest module names stay unique", file=sys.stderr)
        return 1

    print("duplicate-test-basenames: ok (no duplicate test_*.py basenames across the check-system roots)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
