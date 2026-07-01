#!/usr/bin/env python3
"""Lint workflow source files for recordkeeping discipline (internal).

Two regressions are caught:

1. ``record_decision`` calls outside slice/blocker/explicit-checkpoint
   contexts. Per-file-write decisions inflate the decision ledger and
   are explicitly disallowed.
2. ``make dashboard`` mentions inside cold-start orientation blocks.
   The dashboard auto-regenerates on ``close_slice``, explicit
   ``render_handoff(kind='dashboard')``, and ``resolve_review_findings``
   when findings are fixed (internal); the manual command is a
   deprecation alias.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

DEFAULT_ROOT = Path(__file__).resolve().parents[1]
MAX_SOURCE_BYTES = 256 * 1024

ORIENTATION_BLOCK_MARKERS = (
    "On cold start:",
    "At session start",
)
PER_FILE_TRIGGER_PATTERNS = (
    re.compile(r"after each (file|write|edit)", re.IGNORECASE),
    re.compile(r"per[\s-](file|write|edit)", re.IGNORECASE),
    re.compile(r"every (file|write|edit)", re.IGNORECASE),
    re.compile(r"for each (file|write|edit)", re.IGNORECASE),
    re.compile(r"on each (file|write|edit)", re.IGNORECASE),
)
PER_FILE_WRITE_PATTERN = re.compile(r"\brecord_decision\s*\(", re.IGNORECASE)
MAKE_DASHBOARD_PATTERN = re.compile(r"`make dashboard\b")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser.parse_args()


def _iter_source_files(root: Path) -> list[Path]:
    files = sorted((root / "skills").glob("*/body.md"))
    prompts_root = root / "config" / "agent-workflows" / "prompts"
    if prompts_root.is_dir():
        files.extend(sorted(prompts_root.rglob("*.md")))
    return files


def _orientation_blocks(text: str) -> list[tuple[int, str]]:
    """Re-implements the workflow-facade block grouping for cold-start blocks."""

    lines = text.splitlines()
    blocks: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not any(marker in line for marker in ORIENTATION_BLOCK_MARKERS):
            index += 1
            continue
        start = index
        end = index + 1
        while end < len(lines):
            current = lines[end]
            if not current.strip():
                end += 1
                continue
            if re.match(r"^##\s+", current) or re.match(r"^\d+\.\s", current):
                break
            if not current.startswith((" ", "\t", "-", "*", "|")):
                break
            end += 1
        blocks.append((start + 1, "\n".join(lines[start:end])))
        index = end
    return blocks


def _record_decision_in_per_file_context(text: str) -> list[tuple[int, str]]:
    """Return (line_number, snippet) tuples for offending record_decision mentions."""

    issues: list[tuple[int, str]] = []
    lines = text.splitlines()
    for line_no, line in enumerate(lines, start=1):
        if not PER_FILE_WRITE_PATTERN.search(line):
            continue
        window_start = max(0, line_no - 5)
        window_end = min(len(lines), line_no + 5)
        window_text = "\n".join(lines[window_start:window_end])
        if any(pattern.search(window_text) for pattern in PER_FILE_TRIGGER_PATTERNS):
            issues.append((line_no, line.strip()))
    return issues


def _make_dashboard_in_cold_start(text: str) -> list[tuple[int, str]]:
    issues: list[tuple[int, str]] = []
    for line_no, block in _orientation_blocks(text):
        if MAKE_DASHBOARD_PATTERN.search(block):
            issues.append((line_no, block.splitlines()[0]))
    return issues


def check_root(root: Path) -> list[str]:
    errors: list[str] = []
    for path in _iter_source_files(root):
        if path.stat().st_size > MAX_SOURCE_BYTES:
            continue
        text = path.read_text(encoding="utf-8-sig")
        rel = path.relative_to(root)

        for line_no, snippet in _record_decision_in_per_file_context(text):
            errors.append(
                f"{rel}:{line_no}: record_decision outside slice/blocker boundary — "
                f"record decisions only at close_slice / blocker / explicit checkpoint contexts. "
                f"Snippet: {snippet[:120]}"
            )

        for line_no, _block_first in _make_dashboard_in_cold_start(text):
            errors.append(
                f"{rel}:{line_no}: cold-start block mentions `make dashboard`; the dashboard "
                f"auto-regenerates on close_slice, render_handoff(kind='dashboard'), and "
                f"resolve_review_findings when findings are fixed (internal)."
            )
    return errors


def main() -> int:
    args = _parse_args()
    errors = check_root(args.root)
    if errors:
        print("recordkeeping check failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print("recordkeeping check: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
