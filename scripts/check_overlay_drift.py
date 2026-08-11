#!/usr/bin/env python3
"""Fail when root overlay docs drift from the canonical payload copies.

internal: ``packages/workbay-system/workbay_system/payload/`` is the
single source of truth. Root ``docs/workbay/contracts/`` and
``docs/workbay/rules/`` are install-materialized surfaces that must match the
payload canon byte-for-byte.

Self-host operators refresh root surfaces with
``make dogfood DOGFOOD_SOURCE=worktree`` (implementation note). Public git export still
depends on tracked ``docs/workbay/contracts/`` via ``export_public.py``'s
``git ls-files`` selection; keep that tree generated from payload, not edited
in place.
"""

from __future__ import annotations

import filecmp
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PAYLOAD_ROOT = (
    REPO_ROOT
    / "packages"
    / "workbay-system"
    / "workbay_system"
    / "payload"
    / "docs"
    / "workbay"
)

OVERLAY_SURFACES = (
    ("contracts", REPO_ROOT / "docs" / "workbay" / "contracts"),
    ("rules", REPO_ROOT / "docs" / "workbay" / "rules"),
)


def _diff_trees(canon: Path, derived: Path) -> list[str]:
    diffs: list[str] = []
    canon_files = {
        path.relative_to(canon).as_posix()
        for path in canon.rglob("*")
        if path.is_file()
    }
    derived_files = {
        path.relative_to(derived).as_posix()
        for path in derived.rglob("*")
        if path.is_file()
    }
    for rel in sorted(canon_files ^ derived_files):
        diffs.append(f"presence: {rel}")
    for rel in sorted(canon_files & derived_files):
        if not filecmp.cmp(canon / rel, derived / rel, shallow=False):
            diffs.append(f"content: {rel}")
    return diffs


def main() -> int:
    findings: list[str] = []
    for name, root in OVERLAY_SURFACES:
        canon = PAYLOAD_ROOT / name
        if not canon.is_dir():
            findings.append(f"missing payload canon: {canon}")
            continue
        if not root.is_dir():
            findings.append(f"missing root overlay surface: {root}")
            continue
        diffs = _diff_trees(canon, root)
        for diff in diffs:
            findings.append(f"{name}: {diff}")

    if findings:
        print("overlay drift gate — root surfaces diverged from payload canon:", file=sys.stderr)
        for line in findings:
            print(f"  {line}", file=sys.stderr)
        print(
            "refresh with: make dogfood DOGFOOD_SOURCE=worktree",
            file=sys.stderr,
        )
        return 1

    print("ok: root overlay docs/workbay/{contracts,rules} match payload canon")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())