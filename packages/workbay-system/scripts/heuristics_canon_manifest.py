#!/usr/bin/env python3
"""Declarative map of heuristics-canon lexicons → local payload SSOTs.

Single source of truth for which lexicons are mirrored, how they are named
locally, whether they are skill-consumed, and which localization ruleset
applies (engineering only; others are identity).

internal — consumed by ``sync_heuristics_canon.py`` and (later)
the generalized wiring guard.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


CANON_URL = "https://github.com/darce/heuristics-canon"
CANON_REPO = "darce/heuristics-canon"
# Canon layout: lexicons/<canon_name>.md (engineering-v2.md at repo root is retired).
CANON_LEXICON_DIR = "lexicons"

# Localization ruleset identifiers. ``None`` / identity means verbatim mirror.
LOCALIZATION_ENGINEERING = "engineering"


@dataclass(frozen=True)
class Lexicon:
    """One canon lexicon and its local materialization rules."""

    canon_name: str
    """Basename under canon ``lexicons/`` (no ``.md``)."""

    local_filename: str
    """Filename under ``payload/docs/workbay/rules/``."""

    consumed: bool
    """True when skills/guides are expected to cite this lexicon today."""

    localization: str | None
    """Ruleset name for apply_localizations, or None for identity/verbatim."""

    @property
    def canon_path_in_repo(self) -> str:
        return f"{CANON_LEXICON_DIR}/{self.canon_name}.md"


# Engineering keeps the historical filename ``engineering-heuristics.md``.
# All others are ``<canon_name>-heuristics.md``.
LEXICONS: Sequence[Lexicon] = (
    Lexicon(
        canon_name="engineering",
        local_filename="engineering-heuristics.md",
        consumed=True,
        localization=LOCALIZATION_ENGINEERING,
    ),
    Lexicon(
        canon_name="security",
        local_filename="security-heuristics.md",
        consumed=True,
        localization=None,
    ),
    Lexicon(
        canon_name="accessibility",
        local_filename="accessibility-heuristics.md",
        consumed=False,
        localization=None,
    ),
    Lexicon(
        canon_name="business-marketing",
        local_filename="business-marketing-heuristics.md",
        consumed=False,
        localization=None,
    ),
    Lexicon(
        canon_name="design-aesthetics",
        local_filename="design-aesthetics-heuristics.md",
        consumed=False,
        localization=None,
    ),
    Lexicon(
        canon_name="writing",
        local_filename="writing-heuristics.md",
        consumed=False,
        localization=None,
    ),
)


def lexicon_by_name(name: str) -> Lexicon:
    """Return the Lexicon entry for ``canon_name`` or raise KeyError."""
    for entry in LEXICONS:
        if entry.canon_name == name:
            return entry
    raise KeyError(f"unknown lexicon: {name}")


def consumed_lexicons() -> tuple[Lexicon, ...]:
    return tuple(e for e in LEXICONS if e.consumed)


def reference_only_lexicons() -> tuple[Lexicon, ...]:
    return tuple(e for e in LEXICONS if not e.consumed)
