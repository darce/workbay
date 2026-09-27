#!/usr/bin/env python3
"""One-shot migration: lift skills out of .claude/skills/ into a
model-agnostic skills/<slug>/{skill.yaml, body.md} layout.

Source-of-truth flips from .claude/skills/<slug>/SKILL.md (Claude-shape)
to skills/<slug>/{skill.yaml, body.md} (neutral). The Claude path
becomes a generated artifact written by scripts/generate_agent_workflows.py.

Behaviour:
  - For each <slug> under <package>/.claude/skills/:
      * If SKILL.md is a real file with frontmatter, split it into
        skill.yaml (structured) + body.md (prose) under
        <package>/skills/<slug>/.
            * If SKILL.md is a dangling symlink, rescue the source from
                WORKBAY_SKILL_RESCUE_SOURCE/<slug>/SKILL.md.
      * scope: harness is added if missing.
      * Claude-only frontmatter keys ('disable-model-invocation') are
        moved into a side-car claude_overrides field on the structured
        manifest under the 'generator' key, so the generator can compose
        them back into the Claude-shape SKILL.md it emits.
  - Idempotent: re-running on an already-migrated tree is a no-op.
  - This script is run once, the result is committed, and the script
    is kept as documentation for downstream monorepos that need the
    same migration.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
from typing import Any

import yaml

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]
LEGACY_SKILLS_ROOT = PACKAGE_ROOT / ".claude" / "skills"
NEUTRAL_SKILLS_ROOT = PACKAGE_ROOT / "skills"

# Optional source for skills that were dangling symlinks in the source tree.
_RESCUE_SOURCE_RAW = os.environ.get("WORKBAY_SKILL_RESCUE_SOURCE")
RESCUE_SOURCE = pathlib.Path(_RESCUE_SOURCE_RAW).expanduser() if _RESCUE_SOURCE_RAW else None

# Frontmatter keys that are Claude-Code-specific. They get moved out of
# the canonical structured manifest into a generator-side override map
# so the generator can re-attach them when emitting Claude-shape SKILL.md.
CLAUDE_ONLY_KEYS = {"disable-model-invocation"}


def _load_skill_md(path: pathlib.Path) -> tuple[dict[str, Any], str]:
    """Parse YAML frontmatter + Markdown body from a SKILL.md file."""
    text = path.read_text()
    if not text.startswith("---\n"):
        raise SystemExit(f"{path}: missing YAML frontmatter")
    try:
        _, fm_raw, body = text.split("---\n", 2)
    except ValueError as exc:
        raise SystemExit(f"{path}: malformed frontmatter delimiters") from exc
    fm = yaml.safe_load(fm_raw) or {}
    if not isinstance(fm, dict):
        raise SystemExit(f"{path}: frontmatter must parse to a mapping")
    return fm, body.lstrip("\n")


def _resolve_source(slug_dir: pathlib.Path) -> pathlib.Path:
    """Return the on-disk SKILL.md to migrate from, rescuing dangling symlinks."""
    skill_md = slug_dir / "SKILL.md"
    if skill_md.is_file():
        return skill_md
    if RESCUE_SOURCE is None:
        raise SystemExit(
            f"{slug_dir.name}: SKILL.md is missing or dangling and WORKBAY_SKILL_RESCUE_SOURCE is unset"
        )
    rescue = RESCUE_SOURCE / slug_dir.name / "SKILL.md"
    if not rescue.is_file():
        raise SystemExit(
            f"{slug_dir.name}: SKILL.md is missing or dangling and no rescue source at {rescue}"
        )
    return rescue


def _split_frontmatter(fm: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split frontmatter into (canonical, claude_overrides)."""
    canonical: dict[str, Any] = {}
    claude_overrides: dict[str, Any] = {}
    for key, value in fm.items():
        if key in CLAUDE_ONLY_KEYS:
            claude_overrides[key] = value
        else:
            canonical[key] = value
    canonical.setdefault("scope", "harness")
    return canonical, claude_overrides


def _write_neutral_layout(
    slug: str, canonical: dict[str, Any], claude_overrides: dict[str, Any], body: str
) -> None:
    target = NEUTRAL_SKILLS_ROOT / slug
    target.mkdir(parents=True, exist_ok=True)

    if claude_overrides:
        canonical = {**canonical, "generator": {"claude_overrides": claude_overrides}}

    skill_yaml_path = target / "skill.yaml"
    body_path = target / "body.md"

    skill_yaml_path.write_text(
        yaml.safe_dump(canonical, sort_keys=False, default_flow_style=False)
    )
    body_path.write_text(body if body.endswith("\n") else body + "\n")


def migrate(*, dry_run: bool = False) -> int:
    if not LEGACY_SKILLS_ROOT.exists():
        print(f"already migrated: {LEGACY_SKILLS_ROOT} not present", file=sys.stderr)
        return 0

    slugs = sorted(p for p in LEGACY_SKILLS_ROOT.iterdir() if p.is_dir())
    if not slugs:
        print("no skills to migrate", file=sys.stderr)
        return 0

    migrated = 0
    rescued = 0
    for slug_dir in slugs:
        slug = slug_dir.name
        skill_md = slug_dir / "SKILL.md"

        try:
            source = _resolve_source(slug_dir)
        except SystemExit:
            raise
        if source != skill_md:
            rescued += 1

        fm, body = _load_skill_md(source)
        canonical, claude_overrides = _split_frontmatter(fm)

        if dry_run:
            print(
                f"  {slug}: canonical={list(canonical.keys())} "
                f"claude_overrides={list(claude_overrides.keys())} "
                f"body={len(body)}B "
                f"{'(rescued)' if source != skill_md else ''}"
            )
        else:
            _write_neutral_layout(slug, canonical, claude_overrides, body)
        migrated += 1

    print(
        f"migrated {migrated} skills "
        f"({rescued} rescued from {RESCUE_SOURCE or 'no rescue source'})",
        file=sys.stderr,
    )
    if dry_run:
        return 0
    print(
        f"now safe to delete {LEGACY_SKILLS_ROOT.relative_to(PACKAGE_ROOT)} "
        "(do this in a follow-up commit after verifying the new layout).",
        file=sys.stderr,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be migrated without writing files.",
    )
    args = parser.parse_args()
    return migrate(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
