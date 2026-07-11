#!/usr/bin/env python3
"""Rot-guard: review guides must reference engineering-heuristics.md; activation
surfaces must not cite dangling lexicon anchors (internal S6).

implementation note S8 (T17): offload/branch-review/auto-fix bodies must carry the
versionless heuristics link; version/date pins near heuristics references under
skills/** and prompts/** are banned.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PAYLOAD_ROOT = PACKAGE_ROOT / "workbay_system" / "payload"
RULES_DIR = PAYLOAD_ROOT / "docs" / "workbay" / "rules"
HEURISTICS_DOC = RULES_DIR / "engineering-heuristics.md"

GUIDE_DOCS = ("branch-review-guide.md", "planning-review-guide.md")
# Skills that previously lacked any heuristics link (0108 S8 / T17).
HEURISTICS_REQUIRED_SKILLS = ("offload", "branch-review", "auto-fix")
ANCHOR_REF = re.compile(r"engineering-heuristics\.md#([a-z0-9-]+)", re.IGNORECASE)
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)
# Ban version/date *pins* near heuristics references (never pin canon).
# Deliberately does NOT match incidental vN tokens inside anchors
# (engineering-heuristics.md#rule-v1-foo) or loose prose ("… for v1 API").
# Pins are: delimiter-adjacent vN/date right after the ref, or multipartite
# version / ISO date after a short non-anchor gap (BR-0108-S8-02).
VERSION_PIN_NEAR_HEURISTICS = re.compile(
    r"engineering-heuristics(?:\.md)?"
    r"(?:"
    r"[\s@:(,-]+(?:v\d+(?:\.\d+)*|\d{4}-\d{2}(?:-\d{2})?)\b"
    r"|"
    r"[^\n#]{1,80}?(?:(?<=[\s@:(,-])v\d+\.\d+(?:\.\d+)*\b|\b\d{4}-\d{2}(?:-\d{2})?\b)"
    r"|"
    # Word-form pins after a short non-anchor gap (BR / S8-A-02):
    # "rev 3", "revision 3", "version 2", "ver 1.4", "dated 2026-07".
    r"[^\n#]{0,80}?\b(?:rev(?:ision)?|version|ver)\.?\s+\d+(?:\.\d+)*\b"
    r"|"
    r"[^\n#]{0,80}?\bdated\s+\d{4}-\d{2}(?:-\d{2})?\b"
    r")",
    re.IGNORECASE,
)

# Markdown link targets that point at the heuristics lexicon; used to verify
# the link RESOLVES relative to the citing body (S8-A-01: presence alone let a
# one-level-too-deep ../../../ path pass the gate while being a dead link).
HEURISTICS_LINK_TARGET = re.compile(
    r"\]\(([^)\s]*engineering-heuristics\.md)(?:#[^)\s]*)?\)"
)


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


def _version_pin_surfaces() -> list[Path]:
    """Surfaces scanned for banned version/date pins near heuristics refs."""
    paths: list[Path] = []
    paths.extend(sorted(PAYLOAD_ROOT.glob("skills/**/*.md")))
    paths.extend(sorted(PAYLOAD_ROOT.glob("prompts/**/*.md")))
    paths.extend(sorted(PAYLOAD_ROOT.glob("config/agent-workflows/prompts/**/*.md")))
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

    for slug in HEURISTICS_REQUIRED_SKILLS:
        body = PAYLOAD_ROOT / "skills" / slug / "body.md"
        if not body.is_file():
            errors.append(f"skills/{slug}/body.md: missing")
            continue
        text = body.read_text(encoding="utf-8")
        if "engineering-heuristics.md" not in text:
            errors.append(f"skills/{slug}/body.md: missing engineering-heuristics.md reference")
            continue
        # Link RESOLUTION, not just presence: every markdown link target that
        # names the lexicon must exist relative to the body file (S8-A-01).
        targets = HEURISTICS_LINK_TARGET.findall(text)
        if not targets:
            errors.append(
                f"skills/{slug}/body.md: engineering-heuristics.md mentioned but "
                "no markdown link target found"
            )
        for target in targets:
            resolved = (body.parent / target).resolve()
            if not resolved.is_file():
                errors.append(
                    f"skills/{slug}/body.md: heuristics link does not resolve "
                    f"relative to the body: {target}"
                )

    for path in _activation_surfaces():
        rel = path.relative_to(PAYLOAD_ROOT)
        for slug in ANCHOR_REF.findall(path.read_text(encoding="utf-8")):
            normalized = slug.lower()
            if normalized not in valid_slugs:
                errors.append(f"{rel}: dangling engineering-heuristics.md#{slug}")

    for path in _version_pin_surfaces():
        rel = path.relative_to(PAYLOAD_ROOT)
        text = path.read_text(encoding="utf-8")
        if VERSION_PIN_NEAR_HEURISTICS.search(text):
            errors.append(f"{rel}: version/date pin near engineering-heuristics reference")

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
