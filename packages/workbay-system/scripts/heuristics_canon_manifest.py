#!/usr/bin/env python3
"""Discovery of heuristics-canon lexicons → local payload SSOTs.

The canon is the authority on *which* lexicons exist. This module derives the
set rather than declaring it, so a lexicon added upstream reaches agents on the
next sync with no code edit here.

Two discovery surfaces, deliberately separate:

``discover_canon_lexicons(canon_dir)``
    Canon-driven, used by the sync. Enumerates the canon's ``lexicons/*.md``,
    so a newly published lexicon is materialized the first time it appears.

``discover_local_lexicons(rules_dir)``
    Payload-driven, used by every offline consumer (notably the wiring guard).
    Enumerates the SSOTs already materialized under ``payload/.../rules/``, so
    ``make check-heuristics-wiring`` never needs the network.

Only two things are *not* derivable from a lexicon's name and so remain
declared below: ``LOCALIZATIONS`` (a canon-text transform) and ``CONSUMED``
(a local wiring-policy decision). Both are keyed by canon name and both fail
open — an unlisted lexicon is verbatim and reference-only, which is the safe
default for a lexicon nobody here has integrated yet.

internal introduced this map as a hard-coded 6-entry tuple. That
scoped correctly to the canon of 2026-07-12 and then rotted: the canon grew to
ten lexicons and four of them (graph-theory, ml-systems, interaction-ux,
epistemics) were invisible to every agent in this repo, because nothing read
them. 0118's own diagnosis names the shape one level up — "nothing moves here
until someone remembers to diff". An allowlist is that same manual hop applied
to set membership, so the allowlist is gone.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


CANON_URL = "https://github.com/darce/heuristics-canon"
CANON_REPO = "darce/heuristics-canon"
# Canon layout: lexicons/<canon_name>.md (engineering-v2.md at repo root is retired).
CANON_LEXICON_DIR = "lexicons"

# Localization ruleset identifiers. ``None`` / identity means verbatim mirror.
LOCALIZATION_ENGINEERING = "engineering"

# Local SSOTs are ``<canon_name>-heuristics.md``, uniformly. Engineering's
# historical filename happens to satisfy the same rule, so there is no special
# case to carry.
LOCAL_FILENAME_SUFFIX = "-heuristics.md"

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES_DIR = (
    PACKAGE_ROOT / "workbay_system" / "payload" / "docs" / "workbay" / "rules"
)

# Canon text that needs a local transform. Absent → verbatim mirror.
LOCALIZATIONS: dict[str, str] = {
    "engineering": LOCALIZATION_ENGINEERING,
}

# Which lexicons skills/guides are expected to cite *today*. This is a local
# wiring decision, not canon data: it gates check_heuristics_wiring, which
# would fail a freshly discovered lexicon that no skill references yet. A
# lexicon absent here is still synced and still readable by agents — it simply
# does not yet gate the guard. Promote a name into this set when the skills
# start citing it.
CONSUMED: frozenset[str] = frozenset({"engineering", "security"})


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


def local_filename_for(canon_name: str) -> str:
    """``engineering`` → ``engineering-heuristics.md``."""
    return f"{canon_name}{LOCAL_FILENAME_SUFFIX}"


def canon_name_for(local_filename: str) -> str:
    """``engineering-heuristics.md`` → ``engineering``. Inverse of the above."""
    if not local_filename.endswith(LOCAL_FILENAME_SUFFIX):
        raise ValueError(
            f"not a lexicon SSOT filename (want *{LOCAL_FILENAME_SUFFIX}): "
            f"{local_filename}"
        )
    return local_filename[: -len(LOCAL_FILENAME_SUFFIX)]


def lexicon_for(canon_name: str) -> Lexicon:
    """Build the entry for ``canon_name`` from the naming rule + override maps."""
    return Lexicon(
        canon_name=canon_name,
        local_filename=local_filename_for(canon_name),
        consumed=canon_name in CONSUMED,
        localization=LOCALIZATIONS.get(canon_name),
    )


def discover_canon_lexicons(canon_dir: Path) -> tuple[Lexicon, ...]:
    """Every lexicon the canon publishes, from a canon checkout or fetch cache.

    ``canon_dir`` holds ``<canon_name>.md`` files (the layout ``fetch_canon``
    writes and ``--canon-dir`` expects). This is the set the sync materializes,
    so a lexicon published upstream appears locally without a code change.
    """
    if not canon_dir.is_dir():
        raise ValueError(f"canon dir not a directory: {canon_dir}")
    names = sorted(p.stem for p in canon_dir.glob("*.md"))
    if not names:
        raise ValueError(f"no canon lexicons found under {canon_dir}")
    return tuple(lexicon_for(name) for name in names)


def discover_local_lexicons(rules_dir: Path | None = None) -> tuple[Lexicon, ...]:
    """Every lexicon SSOT already materialized in the payload. Never networked.

    This is what offline consumers see. It is intentionally the *materialized*
    set rather than the canon set: a guard must check what is actually on disk,
    not what upstream would like to be there.
    """
    target = DEFAULT_RULES_DIR if rules_dir is None else rules_dir
    if not target.is_dir():
        raise ValueError(f"payload rules dir not a directory: {target}")
    names = sorted(
        canon_name_for(p.name) for p in target.glob(f"*{LOCAL_FILENAME_SUFFIX}")
    )
    if not names:
        raise ValueError(
            f"no lexicon SSOTs found under {target}; expected at least "
            f"{local_filename_for('engineering')}"
        )
    return tuple(lexicon_for(name) for name in names)


# Default view for offline consumers. Canon-driven callers (the sync) pass their
# own entries from discover_canon_lexicons.
LEXICONS: Sequence[Lexicon] = discover_local_lexicons()


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
