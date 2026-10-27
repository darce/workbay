"""Review finding impact and origin predicates (review scope v2).

Canon 0.26.2 PROD-47 (scope hammering): a review blocks only on findings
that cause real problems on a realistic path; everything else is recorded
as a tracked follow-up. SECD-05 (fail-safe defaults): every value this
module cannot validate fails closed. An invalid or non-string ``impact``
blocks, an invalid or non-string ``origin`` counts as ``introduced``, and
legacy rows with no ``impact`` keep the historical high/medium blocking
rule.

Findings are plain mappings. Two optional fields extend them:

- ``impact``: ``"blocking"`` or ``"follow_up"``.
- ``origin``: ``"introduced"``, ``"listed_open"`` or ``"pre_existing"``.

Format stage (D6): formatting, lint and style are removed by the formatter
stage, so a finding in a ``FORMAT_STAGE_CATEGORIES`` category is always a
follow-up with reason ``"format_stage"``, whatever its impact tag or severity.

Pure functions only: no I/O, no reads of anything outside the mapping.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

__all__ = [
    "FORMAT_STAGE_CATEGORIES",
    "FORMAT_STAGE_REASON",
    "IMPACT_VALUES",
    "ORIGIN_VALUES",
    "classify_impact",
    "effective_impact",
    "effective_origin",
    "is_blocking",
]

IMPACT_VALUES: tuple[str, ...] = ("blocking", "follow_up")
ORIGIN_VALUES: tuple[str, ...] = ("introduced", "listed_open", "pre_existing")
FORMAT_STAGE_CATEGORIES: tuple[str, ...] = ("style", "format", "formatting", "lint")
FORMAT_STAGE_REASON = "format_stage"

_LEGACY_BLOCKING_SEVERITIES: frozenset[str] = frozenset({"high", "medium"})
_FORMAT_STAGE_FOLDED: frozenset[str] = frozenset(category.strip().casefold() for category in FORMAT_STAGE_CATEGORIES)


def _normalized_severity(finding: Mapping[str, Any]) -> str | None:
    """Return the severity folded for comparison, or ``None`` if not a string."""
    raw = finding.get("severity")
    if not isinstance(raw, str):
        return None
    return raw.strip().casefold()


def effective_impact(finding: Mapping[str, Any]) -> str:
    """Return ``"blocking"`` or ``"follow_up"`` for one finding.

    An explicit ``impact`` wins when it is one of ``IMPACT_VALUES`` after
    case folding and stripping. A present but invalid ``impact`` (including
    a non-string) fails closed to ``"blocking"``. An absent or ``None``
    ``impact`` falls back to the legacy severity rule: high or medium blocks,
    anything else is a follow-up.
    """
    raw = finding.get("impact")
    if raw is None:
        return "blocking" if _normalized_severity(finding) in _LEGACY_BLOCKING_SEVERITIES else "follow_up"
    if isinstance(raw, str):
        candidate = raw.strip().casefold()
        if candidate in IMPACT_VALUES:
            return candidate
    return "blocking"


def effective_origin(finding: Mapping[str, Any]) -> str:
    """Return the finding origin; absent, ``None`` or invalid means ``"introduced"``.

    Origin is matched exactly against ``ORIGIN_VALUES``. Anything else,
    including a case variant such as ``"PRE_EXISTING"``, fails closed to
    ``"introduced"`` so it can never loosen the gate.
    """
    raw = finding.get("origin")
    if isinstance(raw, str) and raw in ORIGIN_VALUES:
        return raw
    return "introduced"


def _is_format_stage(finding: Mapping[str, Any]) -> bool:
    """True when the finding category is a format-stage category (case-folded, stripped)."""
    raw = finding.get("category")
    return isinstance(raw, str) and raw.strip().casefold() in _FORMAT_STAGE_FOLDED


def classify_impact(finding: Mapping[str, Any]) -> tuple[str, str | None]:
    """Return ``(impact, reason)`` for one finding.

    A format-stage category returns ``("follow_up", "format_stage")`` regardless
    of the impact tag or severity: formatting belongs to the formatter stage and
    never holds a verdict. Every other finding returns
    ``(effective_impact(finding), None)``.
    """
    if _is_format_stage(finding):
        return "follow_up", FORMAT_STAGE_REASON
    return effective_impact(finding), None


def is_blocking(finding: Mapping[str, Any]) -> bool:
    """Return True when the finding holds the review verdict.

    A finding blocks when its classified impact is ``"blocking"``, except a
    ``pre_existing`` finding, which blocks only when its severity is high
    (the pre-existing critical case). A format-stage finding never blocks.
    """
    impact, _reason = classify_impact(finding)
    return impact == "blocking" and not (
        effective_origin(finding) == "pre_existing" and _normalized_severity(finding) != "high"
    )
