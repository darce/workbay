"""Structured heuristic critique over UxMap (internal S3).

Rules are pure checks with stable canon IDs. Severity maps lexicon tiers:
Blocker→high, Should→medium, Judgment→low.

Rule triggers (stable):
- [RLSE-04] screen.states empty or only default
- [NAV-11] deep_link on *stateful* routes (workbench/overlay/`?`) without url_params
- [AGT-02] dangling flow/action screen_id or primary_action_id
- [INT-07] costly|irreversible without preview_required
- [HAI-01] ai_review zone without evidence_linked=true
- [HAI-16] forced_choice without positive max_candidates
- [INT-05] job_entry screen/overlay missing primary_action_id (exits exempt)
- [UI-06] irreversible action with hierarchy=primary on non-overlay screens
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, NamedTuple

from workbay_canvas_mcp.ux_map.models import (
    ActionHierarchy,
    MapState,
    Screen,
    ScreenKind,
    UxMap,
    ZoneRole,
)

RULE_PACK_REVISION = "uxmap-critique/1"
CANON_REVISION = "resume-20260912"
CANON_ROOT = "docs/reviews/resume-20260912/canon/lexicons"


class UnknownRuleIdError(ValueError):
    """Raised when a rule id is missing from RULE_MEANINGS or fails lexicon lookup."""


class RuleMeaning(NamedTuple):
    title: str
    lexicon_relpath: str
    line: int
    next_check: str
    limitation: str


RULE_MEANINGS: dict[str, RuleMeaning] = {
    "AGT-02": RuleMeaning(
        title="No unresolved anchors",
        lexicon_relpath="engineering.md",
        line=75,
        next_check="Confirm each cited path, symbol, or identifier against its source, not memory.",
        limitation=(
            "proxy: screen_id and primary_action_id record declared map identifiers; "
            "map cannot establish that those identifiers resolve in product code "
            "(AGT-02 No unresolved anchors)"
        ),
    ),
    "INT-05": RuleMeaning(
        title="Prominent Done",
        lexicon_relpath="interaction-ux.md",
        line=162,
        next_check="Is there one unmistakable final action after the last input?",
        limitation=(
            "proxy: primary_action_id records an intended final action; "
            "map cannot establish contrast, placement, or control kind (INT-05 Prominent Done)"
        ),
    ),
    "INT-07": RuleMeaning(
        title="Preview before commit",
        lexicon_relpath="interaction-ux.md",
        line=164,
        next_check="Can the user see the result before they pay / print / apply?",
        limitation=(
            "proxy: preview_required records an intended preview flag; "
            "map cannot establish that an outcome sample is shown before commit "
            "(INT-07 Preview before commit)"
        ),
    ),
    "NAV-11": RuleMeaning(
        title="Deep Links encode place and state",
        lexicon_relpath="interaction-ux.md",
        line=138,
        next_check="Does opening the URL restore the same place and view?",
        limitation=(
            "proxy: url_params records intended deep-link parameters; "
            "map cannot establish that opening the URL restores place and view state "
            "(NAV-11 Deep Links encode place and state)"
        ),
    ),
    "HAI-01": RuleMeaning(
        title="Evidence before label",
        lexicon_relpath="interaction-ux.md",
        line=210,
        next_check="Can the reviewer inspect decisive evidence in one action?",
        limitation=(
            "proxy: evidence_linked records an intended evidence link; "
            "map cannot establish that a reviewer can inspect source observations in one action "
            "(HAI-01 Evidence before label)"
        ),
    ),
    "HAI-16": RuleMeaning(
        title="Bound the forced-choice set",
        lexicon_relpath="interaction-ux.md",
        line=225,
        next_check=(
            "How many candidates were on screen when this decision was recorded, "
            "and was that cap checked for human pick accuracy?"
        ),
        limitation=(
            "proxy: max_candidates records an intended bound; "
            "map cannot establish human pick accuracy at that bound "
            "(HAI-16 Bound the forced-choice set)"
        ),
    ),
    "UI-06": RuleMeaning(
        title="Hierarchy over semantics",
        lexicon_relpath="engineering.md",
        line=674,
        next_check="Is delete competing with the page's true primary action?",
        limitation=(
            "proxy: actions[].hierarchy records an intended action rank; "
            "map cannot establish visual prominence or destructive styling "
            "(UI-06 Hierarchy over semantics)"
        ),
    ),
    "RLSE-04": RuleMeaning(
        title="Undesigned state is a bug",
        lexicon_relpath="engineering.md",
        line=695,
        next_check="Which of the seven states did nobody design?",
        limitation=(
            "proxy: screens[].states records declared state names; "
            "map cannot establish designed UI for those states "
            "(RLSE-04 Undesigned state is a bug)"
        ),
    ),
}


def validate_rule_pack(rule_ids: Iterable[str], *, repo_root: Path) -> None:
    """Check each id has a lexicon-backed meaning. Test-only; not called at import."""
    for rule_id in rule_ids:
        meaning = RULE_MEANINGS.get(rule_id)
        if meaning is None:
            raise UnknownRuleIdError(rule_id)
        lexicon_path = Path(repo_root) / CANON_ROOT / meaning.lexicon_relpath
        try:
            lines = lexicon_path.read_text(encoding="utf-8").splitlines()
            line_text = lines[meaning.line - 1]
        except (OSError, IndexError) as exc:
            raise UnknownRuleIdError(rule_id) from exc
        prefix = f"| {rule_id}<a name="
        if not line_text.startswith(prefix) or f"**{meaning.title}**" not in line_text:
            raise UnknownRuleIdError(rule_id)


@dataclass(frozen=True, slots=True)
class CritiqueFinding:
    rule_id: str
    severity: str  # high | medium | low
    pointer: str
    fix: str
    detail: str = ""
    pack_revision: str = RULE_PACK_REVISION
    canon_revision: str = CANON_REVISION
    evidence_kind: str = "heuristic"
    next_check: str = ""
    limitation: str = ""


RuleFn = Callable[[UxMap], list[CritiqueFinding]]


def _is_stateful_deep_link_surface(screen: Screen) -> bool:
    """NAV-11 only for surfaces that typically encode restorable view state."""
    route = (screen.route or "").lower()
    if "?" in route or "workbench" in route:
        return True
    if screen.kind is ScreenKind.OVERLAY:
        return True
    return False


def _rlse04_states(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    for s in map_.screens:
        states = list(s.states)
        if not states or set(states) <= {MapState.DEFAULT}:
            out.append(
                CritiqueFinding(
                    rule_id="RLSE-04",
                    severity="medium",
                    pointer=f"screens[{s.id}].states",
                    fix="Declare non-default states (loading/empty/error/…) per [RLSE-04].",
                    detail="states empty or only default",
                )
            )
    return out


def _nav11_deep_links(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    for s in map_.screens:
        if not s.deep_link:
            continue
        if not s.route:
            continue
        if not _is_stateful_deep_link_surface(s):
            continue
        if not s.url_params:
            out.append(
                CritiqueFinding(
                    rule_id="NAV-11",
                    severity="medium",
                    pointer=f"screens[{s.id}].url_params",
                    fix="Declare url_params for stateful deep links, or set deep_link=false.",
                    detail=f"stateful route={s.route!r} has no url_params",
                )
            )
    return out


def _agt02_dangling_refs(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    known_screens = {s.id for s in map_.screens}
    known_actions = {a.id for a in map_.actions}
    for f in map_.flows:
        for i, step in enumerate(f.steps):
            if step.screen_id not in known_screens:
                out.append(
                    CritiqueFinding(
                        rule_id="AGT-02",
                        severity="high",
                        pointer=f"flows[{f.id}].steps[{i}].screen_id",
                        fix=f"Point screen_id at a declared screen (unknown {step.screen_id!r}).",
                        detail="dangling flow step screen_id",
                    )
                )
    for a in map_.actions:
        if a.screen_id and a.screen_id not in known_screens:
            out.append(
                CritiqueFinding(
                    rule_id="AGT-02",
                    severity="high",
                    pointer=f"actions[{a.id}].screen_id",
                    fix=f"Point screen_id at a declared screen (unknown {a.screen_id!r}).",
                    detail="dangling action screen_id",
                )
            )
    for s in map_.screens:
        if s.primary_action_id and s.primary_action_id not in known_actions:
            out.append(
                CritiqueFinding(
                    rule_id="AGT-02",
                    severity="high",
                    pointer=f"screens[{s.id}].primary_action_id",
                    fix=f"Point primary_action_id at a declared action (unknown {s.primary_action_id!r}).",
                    detail="dangling primary_action_id",
                )
            )
    return out


def _int07_preview(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    for a in map_.actions:
        if (a.costly or a.irreversible) and not a.preview_required:
            out.append(
                CritiqueFinding(
                    rule_id="INT-07",
                    severity="high",
                    pointer=f"actions[{a.id}].preview_required",
                    fix="Set preview_required=true for costly/irreversible actions.",
                    detail="costly/irreversible without preview",
                )
            )
    return out


def _hai01_evidence(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    for s in map_.screens:
        for z in s.zones:
            if z.role is ZoneRole.AI_REVIEW and z.evidence_linked is not True:
                out.append(
                    CritiqueFinding(
                        rule_id="HAI-01",
                        severity="high",
                        pointer=f"screens[{s.id}].zones[{z.id}].evidence_linked",
                        fix="Set evidence_linked=true for ai_review zones.",
                        detail="AI review zone without evidence link",
                    )
                )
    return out


def _hai16_max_candidates(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    for s in map_.screens:
        for z in s.zones:
            if z.role is ZoneRole.FORCED_CHOICE and (z.max_candidates is None or z.max_candidates < 1):
                out.append(
                    CritiqueFinding(
                        rule_id="HAI-16",
                        severity="medium",
                        pointer=f"screens[{s.id}].zones[{z.id}].max_candidates",
                        fix="Set max_candidates to a positive bound for forced_choice zones.",
                        detail="forced_choice without max_candidates",
                    )
                )
    return out


def _int05_job_entry_primary(map_: UxMap) -> list[CritiqueFinding]:
    out: list[CritiqueFinding] = []
    for s in map_.screens:
        if s.kind is ScreenKind.EXIT:
            continue
        if s.job_entry and not s.primary_action_id:
            out.append(
                CritiqueFinding(
                    rule_id="INT-05",
                    severity="medium",
                    pointer=f"screens[{s.id}].primary_action_id",
                    fix="Set primary_action_id on job-entry screens/overlays (exits exempt).",
                    detail="job_entry screen missing primary action",
                )
            )
    return out


def _ui06_destructive_primary(map_: UxMap) -> list[CritiqueFinding]:
    """Irreversible actions must not be PRIMARY on normal pages; overlays may confirm."""
    out: list[CritiqueFinding] = []
    screens_by_id = {s.id: s for s in map_.screens}
    for a in map_.actions:
        if a.hierarchy is not ActionHierarchy.PRIMARY or not a.irreversible:
            continue
        screen = screens_by_id.get(a.screen_id) if a.screen_id else None
        if screen is not None and screen.kind is ScreenKind.OVERLAY:
            continue  # confirmation step may use primary
        out.append(
            CritiqueFinding(
                rule_id="UI-06",
                severity="medium",
                pointer=f"actions[{a.id}].hierarchy",
                fix="Use hierarchy=destructive/secondary for irreversible actions on screens; reserve primary for confirmation overlays or constructive actions.",
                detail="irreversible action marked primary on non-overlay surface",
            )
        )
    return out


RULE_PACK: list[tuple[str, RuleFn]] = [
    ("RLSE-04", _rlse04_states),
    ("NAV-11", _nav11_deep_links),
    ("AGT-02", _agt02_dangling_refs),
    ("INT-07", _int07_preview),
    ("HAI-01", _hai01_evidence),
    ("HAI-16", _hai16_max_candidates),
    ("INT-05", _int05_job_entry_primary),
    ("UI-06", _ui06_destructive_primary),
]


def _annotate_finding(finding: CritiqueFinding) -> CritiqueFinding:
    meaning = RULE_MEANINGS[finding.rule_id]
    evidence_kind = "structural" if finding.rule_id == "AGT-02" else "heuristic"
    return replace(
        finding,
        pack_revision=RULE_PACK_REVISION,
        canon_revision=CANON_REVISION,
        evidence_kind=evidence_kind,
        next_check=meaning.next_check,
        limitation=meaning.limitation,
    )


def critique_map(map_: UxMap) -> list[CritiqueFinding]:
    """Run the full rule pack; return findings in stable rule order."""
    if not RULE_PACK:
        raise RuntimeError("critique RULE_PACK is empty — fail closed")
    findings: list[CritiqueFinding] = []
    for _name, fn in RULE_PACK:
        findings.extend(fn(map_))
    return [_annotate_finding(finding) for finding in findings]
