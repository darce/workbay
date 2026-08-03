"""Explicit tracked-vs-overlay ownership boundary (implementation note / internal S6).

Overlay-delivered surfaces are gitignored and managed by bootstrap install.
Consumer-owned paths stay tracked and must not be silently ignored. Self-host
repos track real overlay source under :data:`TRACKED_OVERLAY_SOURCE_PATHS`.
"""

from __future__ import annotations

from pathlib import Path

from workbay_bootstrap.install import (
    _git_path_is_ignored,
    _git_path_is_tracked,
    _leaking_overlay_entries,
)

# Self-hosting monorepo paths that ship real overlay source (not symlinks).
# Mirrors ``tests.test_gitignore_block._TRACKED_SURFACE_DIRS`` plus managed hook
# config files tracked as generated goldens.
TRACKED_OVERLAY_SOURCE_PATHS: tuple[str, ...] = (
    "scripts/hooks",
    "Makefile.d",
    "scripts/workbay",
    # implementation note: lifecycle top-level sibling tracked as monorepo source.
    "scripts/workbay_lifecycle",
    "docs/workbay/contracts",
    "docs/workbay/rules",
    "docs/workbay/templates",
    ".github/hooks",
    ".github/prompts",
    ".codex/hooks.json",
    ".cursor/hooks.json",
)

TRACKED_DUPLICATE_SKILL_PREFIX = "skills/"


def validate_tracked_overlay_boundary(target: Path) -> list[dict[str, str]]:
    """Return boundary violations under ``target`` (empty when clean).

    Each finding is ``{kind, path, message}`` where ``kind`` is one of:

    - ``tracked_overlay_duplicate`` — git tracks ``skills/<slug>/SKILL.md`` while
      bootstrap also materialized ``.cursor/skills/<slug>`` as an overlay symlink.
    - ``tracked_source_would_leak_ignore`` — a declared self-host source path would
      receive a managed gitignore entry (defensive; should not occur when tracked).
    """
    target = Path(target).resolve()
    findings: list[dict[str, str]] = []

    leaking = {entry.lstrip("/") for entry in _leaking_overlay_entries(target)}
    for rel in TRACKED_OVERLAY_SOURCE_PATHS:
        if rel in leaking:
            findings.append(
                {
                    "kind": "tracked_source_would_leak_ignore",
                    "path": rel,
                    "message": (
                        f"declared tracked overlay source {rel!r} would be "
                        "silently gitignored by the managed overlay block"
                    ),
                }
            )

    skills_root = target / "skills"
    if skills_root.is_dir():
        for skill_md in sorted(skills_root.glob("*/SKILL.md")):
            slug = skill_md.parent.name
            rel = f"{TRACKED_DUPLICATE_SKILL_PREFIX}{slug}/SKILL.md"
            if not _git_path_is_tracked(target, rel):
                continue
            cursor_skill = target / ".cursor" / "skills" / slug
            if cursor_skill.is_symlink():
                findings.append(
                    {
                        "kind": "tracked_overlay_duplicate",
                        "path": rel,
                        "message": (
                            f"tracked {rel} duplicates overlay symlink "
                            f".cursor/skills/{slug}"
                        ),
                    }
                )

    return findings


def consumer_path_is_gitignored(target: Path, rel: str) -> bool:
    """True when git ignores a consumer-owned relative path."""
    return _git_path_is_ignored(target, rel)


def consumer_path_is_tracked(target: Path, rel: str) -> bool:
    """True when git tracks a consumer-owned relative path."""
    return _git_path_is_tracked(target, rel)
