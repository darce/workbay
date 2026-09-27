from __future__ import annotations

import re

LEGACY_SLICE_COMPLETE_RE = re.compile(r"^slice_complete_\w+$")
PREFIXED_SLICE_COMPLETE_RE = re.compile(r"^[a-z]{2,12}_slice_complete_[A-Za-z0-9_-]+_\w+$")
_PREFIXED_SLICE_COMPLETE_CANDIDATE_RE = re.compile(
    r"^(?P<author_tag>[a-z]{2,12})_slice_complete_(?P<work_ref>[A-Za-z0-9_-]+)_(?P<slug>.+)$"
)
_CANONICAL_DECISION_PARTS_RE = re.compile(
    r"^(?P<author_tag>[a-z]{2,12})_(?P<decision_kind>[a-z][a-z0-9_]*)_"
    r"(?P<work_ref>[A-Za-z0-9][A-Za-z0-9_-]*)_(?P<slug>[a-z0-9][a-z0-9_]*)$"
)
_SLUG_NORMALIZE_RE = re.compile(r"[\s-]+")

# Full canonical grammar: <author_tag>_<decision_kind>_<work_ref>_<slug>
# - author_tag:    [a-z]{2,12}
# - decision_kind: one or more underscore-delimited lowercase words, e.g. slice_complete
# - work_ref:      task/epic reference, e.g. internal, ADPH-4, or any alphanumeric+hyphen token
# - slug:          [a-z0-9][a-z0-9_]* (at least one char, starts with alphanumeric)
# The plan slice number is carried structurally via close_slice(slice_number=N)
# on the decision row — the slug is a human label, not the machine-read slice id.
CANONICAL_DECISION_RE = re.compile(_CANONICAL_DECISION_PARTS_RE.pattern)


def _canonical_parts(decision: str) -> dict[str, str] | None:
    match = _CANONICAL_DECISION_PARTS_RE.match(decision)
    if match is None:
        return None
    return match.groupdict()


def _suggest_slice_complete_decision(decision: str) -> str | None:
    match = _PREFIXED_SLICE_COMPLETE_CANDIDATE_RE.match(decision)
    if match is None:
        return None
    normalized_slug = _SLUG_NORMALIZE_RE.sub("_", match.group("slug")).strip("_")
    if not normalized_slug or normalized_slug == match.group("slug"):
        return None
    suggested = compose_slice_complete_decision_id(
        author_tag=match.group("author_tag"),
        work_ref=match.group("work_ref"),
        slug=normalized_slug,
    )
    if not is_prefixed_slice_complete_decision(suggested):
        return None
    return suggested


def is_legacy_slice_complete_decision(decision: str) -> bool:
    return bool(LEGACY_SLICE_COMPLETE_RE.match(decision))


def is_prefixed_slice_complete_decision(decision: str) -> bool:
    return bool(PREFIXED_SLICE_COMPLETE_RE.match(decision))


def is_slice_complete_decision(decision: str) -> bool:
    """Return True if the decision is a slice-complete id in either supported format."""
    return is_legacy_slice_complete_decision(decision) or is_prefixed_slice_complete_decision(decision)


def is_canonical_decision(decision: str) -> bool:
    """Return True if the decision id conforms to the full canonical grammar.

    Canonical form: ``<author_tag>_<decision_kind>_<work_ref>_<slug>``

    This accepts any decision kind, not just ``slice_complete``.
    """
    return _canonical_parts(decision) is not None


def compose_slice_complete_decision_id(author_tag: str, work_ref: str, slug: str) -> str:
    """Compose the canonical prefixed slice-complete decision identifier."""
    return f"{author_tag}_slice_complete_{work_ref}_{slug}"


def classify_decision_id(decision: str) -> str:
    """Classify a decision id string into one of four categories.

    Returns:
        ``"canonical"``       – matches the full canonical grammar.
        ``"legacy_slice"``    – matches the legacy ``slice_complete_*`` form (grandfathered).
        ``"malformed_slice"`` – contains ``slice_complete`` but violates the grammar.
        ``"freeform"``        – does not use slice_complete at all and is not canonical.
    """
    if is_canonical_decision(decision):
        return "canonical"
    if is_legacy_slice_complete_decision(decision):
        return "legacy_slice"
    if "slice_complete" in decision:
        return "malformed_slice"
    return "freeform"


def validate_decision_id(decision: str, decision_kind: str | None = None) -> dict[str, object]:
    """Validate a decision id without performing any writes.

    With no ``decision_kind``, this mirrors the mutation-path id checks:
    malformed slice-complete identifiers are rejected while freeform non-slice
    identifiers remain valid. When ``decision_kind`` is provided, the id must
    be canonical for that kind.
    """
    category = classify_decision_id(decision)
    result: dict[str, object] = {
        "ok": True,
        "decision": decision,
        "category": category,
    }
    if decision_kind is not None:
        result["decision_kind"] = decision_kind

    if is_legacy_slice_complete_decision(decision):
        result["ok"] = False
        result["error"] = (
            "Legacy slice-complete ids are grandfathered for historical rows only. "
            "New writes must use <author_tag>_slice_complete_<work_ref>_<slug>."
        )
        return result

    if decision.startswith("slice_complete_"):
        result["ok"] = False
        result["error"] = (
            "Malformed slice-complete id. New writes must use <author_tag>_slice_complete_<work_ref>_<slug>."
        )
        return result

    if "_slice_complete_" in decision and not is_prefixed_slice_complete_decision(decision):
        result["ok"] = False
        result["error"] = "Malformed slice-complete id. Expected <author_tag>_slice_complete_<work_ref>_<slug>."
        suggested = _suggest_slice_complete_decision(decision)
        if suggested is not None:
            result["suggested"] = suggested
        return result

    if decision_kind is None:
        return result

    if decision_kind == "slice_complete":
        if is_prefixed_slice_complete_decision(decision):
            return result
        result["ok"] = False
        result["error"] = (
            "Decision id is not canonical for decision_kind='slice_complete'. Use <author_tag>_slice_complete_<work_ref>_<slug>."
        )
        return result

    canonical_parts = _canonical_parts(decision)
    if canonical_parts is None:
        result["ok"] = False
        result["error"] = (
            f"Decision id is not canonical for decision_kind={decision_kind!r}. "
            f"Use <author_tag>_{decision_kind}_<work_ref>_<slug>."
        )
        return result

    if canonical_parts["decision_kind"] != decision_kind:
        result["ok"] = False
        result["error"] = (
            f"Decision id uses decision kind {canonical_parts['decision_kind']!r}; expected {decision_kind!r}."
        )
    return result


def extract_slice_label(decision: str) -> str:
    """Extract the label payload from a slice-complete decision id."""
    if is_legacy_slice_complete_decision(decision):
        return decision[len("slice_complete_") :]
    parts = decision.split("_slice_complete_", 1)
    if len(parts) == 2:
        return parts[1]
    return decision
