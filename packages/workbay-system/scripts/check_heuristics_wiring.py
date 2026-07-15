#!/usr/bin/env python3
"""Rot-guard: review guides must reference engineering-heuristics.md; activation
surfaces must not cite dangling lexicon anchors (internal S6).

implementation note S8 (T17): offload/branch-review/auto-fix bodies must carry the
versionless heuristics link; version/date pins near heuristics references under
skills/** and prompts/** are banned.

internal: lexicon set is manifest-driven (all 6 local filenames).
Anchor resolution is per-lexicon; reference-only (consumed=False) lexicons need
not have consumers; absent SSOTs are tolerated except engineering.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Manifest lives alongside this script.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from heuristics_canon_manifest import LEXICONS  # noqa: E402

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PAYLOAD_ROOT = PACKAGE_ROOT / "workbay_system" / "payload"
RULES_DIR = PAYLOAD_ROOT / "docs" / "workbay" / "rules"
# Primary required lexicon (must exist; guides + required skills must cite it).
PRIMARY_LEXICON_FILENAME = "engineering-heuristics.md"
HEURISTICS_DOC = RULES_DIR / PRIMARY_LEXICON_FILENAME

GUIDE_DOCS = ("branch-review-guide.md", "planning-review-guide.md")
# Skills that previously lacked any heuristics link (0108 S8 / T17).
HEURISTICS_REQUIRED_SKILLS = ("offload", "branch-review", "auto-fix")

_LEXICON_FILENAMES = tuple(lex.local_filename for lex in LEXICONS)
_LEXICON_FILE_ALT = "|".join(re.escape(name) for name in _LEXICON_FILENAMES)
_LEXICON_STEM_ALT = "|".join(
    re.escape(Path(name).stem) for name in _LEXICON_FILENAMES
)

# Match any manifest local_filename#slug; group 1 = filename, group 2 = slug.
ANCHOR_REF = re.compile(
    rf"({_LEXICON_FILE_ALT})#([a-z0-9-]+)",
    re.IGNORECASE,
)
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)
# Ban version/date *pins* near heuristics references (never pin canon).
# Deliberately does NOT match incidental vN tokens inside anchors
# (engineering-heuristics.md#rule-v1-foo) or loose prose ("… for v1 API").
# Pins are: delimiter-adjacent vN/date right after the ref, or multipartite
# version / ISO date after a short non-anchor gap (BR-0108-S8-02).
VERSION_PIN_NEAR_HEURISTICS = re.compile(
    rf"(?:{_LEXICON_STEM_ALT})(?:\.md)?"
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

# Markdown link targets that point at any manifest lexicon; used to verify
# the link RESOLVES relative to the citing body (S8-A-01: presence alone let a
# one-level-too-deep ../../../ path pass the gate while being a dead link).
HEURISTICS_LINK_TARGET = re.compile(
    rf"\]\(([^)\s]*(?:{_LEXICON_FILE_ALT}))(?:#[^)\s]*)?\)",
    re.IGNORECASE,
)


def _heading_slugs(doc: Path) -> set[str]:
    slugs: set[str] = set()
    for heading in HEADING.findall(doc.read_text(encoding="utf-8")):
        slug = re.sub(r"[^\w\s-]", "", heading).strip().lower()
        slugs.add(re.sub(r"\s+", "-", slug))
    return slugs


def _activation_surfaces(
    payload_root: Path | None = None,
    rules_dir: Path | None = None,
) -> list[Path]:
    payload_root = PAYLOAD_ROOT if payload_root is None else payload_root
    rules_dir = RULES_DIR if rules_dir is None else rules_dir
    paths: list[Path] = []
    paths.extend(sorted(payload_root.glob("skills/*/body.md")))
    paths.extend(sorted(payload_root.glob("config/agent-workflows/prompts/**/*.md")))
    for name in GUIDE_DOCS:
        paths.append(rules_dir / name)
    return paths


def _version_pin_surfaces(payload_root: Path | None = None) -> list[Path]:
    """Surfaces scanned for banned version/date pins near heuristics refs."""
    payload_root = PAYLOAD_ROOT if payload_root is None else payload_root
    paths: list[Path] = []
    paths.extend(sorted(payload_root.glob("skills/**/*.md")))
    paths.extend(sorted(payload_root.glob("prompts/**/*.md")))
    paths.extend(sorted(payload_root.glob("config/agent-workflows/prompts/**/*.md")))
    return paths


def _lexicon_slug_map(rules_dir: Path) -> dict[str, set[str]]:
    """Map lowercased local_filename → heading slugs for SSOTs present on disk."""
    out: dict[str, set[str]] = {}
    for lex in LEXICONS:
        path = rules_dir / lex.local_filename
        if path.is_file():
            out[lex.local_filename.lower()] = _heading_slugs(path)
    return out


def _rel_to_payload(path: Path, payload_root: Path) -> str:
    try:
        return str(path.relative_to(payload_root))
    except ValueError:
        return str(path)


def collect_violations(
    payload_root: Path | None = None,
    rules_dir: Path | None = None,
) -> list[str]:
    """Return wiring violations for the payload tree.

    ``payload_root`` / ``rules_dir`` default to the live package payload so the
    CLI and tree-level tests stay unchanged; fixture tests inject tmp roots.
    """
    payload_root = PAYLOAD_ROOT if payload_root is None else payload_root
    rules_dir = RULES_DIR if rules_dir is None else rules_dir
    errors: list[str] = []

    primary = rules_dir / PRIMARY_LEXICON_FILENAME
    if not primary.is_file():
        return [f"missing lexicon: {primary}"]

    slug_map = _lexicon_slug_map(rules_dir)

    for name in GUIDE_DOCS:
        guide = rules_dir / name
        text = guide.read_text(encoding="utf-8")
        if PRIMARY_LEXICON_FILENAME not in text:
            errors.append(f"{name}: missing {PRIMARY_LEXICON_FILENAME} reference")

    for slug in HEURISTICS_REQUIRED_SKILLS:
        body = payload_root / "skills" / slug / "body.md"
        if not body.is_file():
            errors.append(f"skills/{slug}/body.md: missing")
            continue
        text = body.read_text(encoding="utf-8")
        if PRIMARY_LEXICON_FILENAME not in text:
            errors.append(
                f"skills/{slug}/body.md: missing {PRIMARY_LEXICON_FILENAME} reference"
            )
            continue
        # Link RESOLUTION, not just presence: every markdown link target that
        # names a lexicon must exist relative to the body file (S8-A-01).
        targets = HEURISTICS_LINK_TARGET.findall(text)
        eng_targets = [
            t for t in targets if PRIMARY_LEXICON_FILENAME.lower() in t.lower()
        ]
        if not eng_targets:
            errors.append(
                f"skills/{slug}/body.md: {PRIMARY_LEXICON_FILENAME} mentioned but "
                "no markdown link target found"
            )
        for target in targets:
            resolved = (body.parent / target).resolve()
            if not resolved.is_file():
                errors.append(
                    f"skills/{slug}/body.md: heuristics link does not resolve "
                    f"relative to the body: {target}"
                )

    activation = _activation_surfaces(payload_root=payload_root, rules_dir=rules_dir)

    # Per-lexicon anchor resolution (absent SSOT → dangling when cited).
    for path in activation:
        if not path.is_file():
            continue
        rel = _rel_to_payload(path, payload_root)
        text = path.read_text(encoding="utf-8")
        for match in ANCHOR_REF.finditer(text):
            filename = match.group(1)
            anchor_slug = match.group(2).lower()
            key = filename.lower()
            file_slugs = slug_map.get(key)
            if file_slugs is None:
                errors.append(
                    f"{rel}: dangling {filename}#{match.group(2)} "
                    f"(lexicon not on disk)"
                )
            elif anchor_slug not in file_slugs:
                errors.append(f"{rel}: dangling {filename}#{match.group(2)}")

    # consumed=True lexicons that exist must be referenced by ≥1 activation surface.
    # consumed=False (reference-only) never required. Absent SSOTs skipped.
    consumer_blob = ""
    for path in activation:
        if path.is_file():
            consumer_blob += path.read_text(encoding="utf-8")
            consumer_blob += "\n"
    for lex in LEXICONS:
        if not lex.consumed:
            continue
        if not (rules_dir / lex.local_filename).is_file():
            continue
        if lex.local_filename not in consumer_blob:
            errors.append(
                f"{lex.local_filename}: consumed lexicon has no consumer reference"
            )

    for path in _version_pin_surfaces(payload_root=payload_root):
        if not path.is_file():
            continue
        rel = _rel_to_payload(path, payload_root)
        text = path.read_text(encoding="utf-8")
        if VERSION_PIN_NEAR_HEURISTICS.search(text):
            errors.append(f"{rel}: version/date pin near heuristics reference")

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
