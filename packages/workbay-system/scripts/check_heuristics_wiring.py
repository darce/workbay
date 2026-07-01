#!/usr/bin/env python3
"""Rot-guard: review guides must reference engineering-heuristics.md; activation
surfaces must not cite dangling lexicon anchors (internal S6)."""

from __future__ import annotations

import re
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PAYLOAD_ROOT = PACKAGE_ROOT / "workbay_system" / "payload"
RULES_DIR = PAYLOAD_ROOT / "docs" / "workbay" / "rules"
HEURISTICS_DOC = RULES_DIR / "engineering-heuristics.md"

GUIDE_DOCS = ("branch-review-guide.md", "planning-review-guide.md")
ANCHOR_REF = re.compile(r"engineering-heuristics\.md#([a-z0-9-]+)", re.IGNORECASE)
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)


def _heading_slugs(doc: Path) -> set[str]:
    slugs: set[str] = set()
    for heading in HEADING.findall(doc.read_text(encoding="utf-8")):
        slug = re.sub(r"[^\w\s-]", "", heading).strip().lower()
        slugs.add(re.sub(r"\s+", "-", slug))
    return slugs


def _activation_surfaces() -> list[Path]:
    paths: list[Path] = []
    paths.extend(sorted(PAYLOAD_ROOT.glob("skills/*/body.md")))
    paths.extend(sorted(PAYLOAD_ROOT.glob("config/agent-workflows/prompts/**/*.md")))
    for name in GUIDE_DOCS:
        paths.append(RULES_DIR / name)
    return paths


def collect_violations() -> list[str]:
    errors: list[str] = []
    if not HEURISTICS_DOC.is_file():
        return [f"missing lexicon: {HEURISTICS_DOC}"]

    valid_slugs = _heading_slugs(HEURISTICS_DOC)

    for name in GUIDE_DOCS:
        guide = RULES_DIR / name
        text = guide.read_text(encoding="utf-8")
        if "engineering-heuristics.md" not in text:
            errors.append(f"{name}: missing engineering-heuristics.md reference")

    for path in _activation_surfaces():
        rel = path.relative_to(PAYLOAD_ROOT)
        for slug in ANCHOR_REF.findall(path.read_text(encoding="utf-8")):
            normalized = slug.lower()
            if normalized not in valid_slugs:
                errors.append(f"{rel}: dangling engineering-heuristics.md#{slug}")

    return sorted(set(errors))


def main() -> int:
    violations = collect_violations()
    if violations:
        print("check-heuristics-wiring: FAIL", file=sys.stderr)
        for item in violations:
            print(f"  {item}", file=sys.stderr)
        return 1
    print("check-heuristics-wiring: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
