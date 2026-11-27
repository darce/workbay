"""Recognize explicit policy signals in proposed decisions without file or network I/O."""

from __future__ import annotations

import re

_ADR = r"(?:ADRs?(?:[- ]\d+|(?![- ]\d))|architecture decision records?)"
_ADR_AMENDMENT = re.compile(
    rf"\b(?:amend|update|supersede|revise)\s+(?:(?:the|existing|recorded|current)\s+){{0,3}}{_ADR}\b"
    r"(?![ \t]+(?:later|after\b))",
    re.IGNORECASE,
)
_NEGATION = re.compile(
    r"\b(?:avoid(?:ing)?|reject(?:ing)?|prevent(?:ing)?|no|never|without|"
    r"not|don['\u2019]t|won['\u2019]t|instead of|rather than|prohibit(?:ing)?)\s+"
    r"(?:(?:use|using|add|adding|any|the|a|an|implement|introduce)\s+){0,3}$",
    re.IGNORECASE,
)
_RULES = (
    (
        "DATA-14", "B", "decision.dual-write",
        "Write to a single source of truth and derive the other copy from it.",
        r"\b(?:dual[- ]writ(?:es?|ing)|"
        r"manual(?:ly)?\s+sync(?:hroniz(?:e|ation)|ing)?(?:\s+[^.;\n]{0,55})?\s+configs?\b|"
        r"manually\s+keep\s+[^.;\n]{0,55}\bconfigs?\s+in\s+sync|"
        r"(?:sync|keep)\s+[^.;\n]{0,55}\bconfigs?\b[^.;\n]{0,24}\b(?:manually|by hand)|"
        r"writ(?:e|ing)\s+(?:the same\s+[^.;\n]{1,35}\s+to\s+)?(?:to\s+)?both\s+[^.;\n]{0,40}\bconfigs?)\b",
    ),
    (
        "AGT-13", "S", "decision.adr-override",
        "Amend or supersede the recorded ADR before implementing the conflicting decision.",
        rf"\b(?:(?:overrid(?:e|es|ing)|contradict(?:s|ing)?|ignor(?:e|es|ing)|"
        rf"depart(?:s|ing)?\s+from|deviat(?:e|es|ing)\s+from)\s+"
        rf"(?:(?:the|an?|existing|recorded|current)\s+){{0,3}}{_ADR})\b",
    ),
    (
        "SECD-05", "B", "decision.fail-open",
        "Keep verification enabled and fail to a documented safe state on error.",
        r"\b(?:fail[- ]open|silent(?:ly)?\s+fall\s*back|fall\s*back\s+silently|"
        r"(?:skip(?:ping)?|bypass(?:ing)?|disabl(?:e|ing))\s+"
        r"(?:(?:the|all|signature|certificate|security|auth(?:entication|orization)?)\s+){0,3}"
        r"(?:verification|security checks?|auth(?:entication|orization)?(?: checks?)?)|"
        r"(?:allow|grant)\s+access\s+(?:on|after)\s+(?:any\s+)?(?:error|failure))\b",
    ),
    (
        "RES-02", "B", "decision.unbounded-wait",
        "Set finite timeouts on blocking calls and give retries or loops a stopping condition.",
        r"\b(?:unbounded\s+retr(?:y|ies)|unlimited\s+retr(?:y|ies)|retry\s+(?:forever|indefinitely)|"
        r"infinite\s+loops?|while\s+true|"
        r"(?:omit(?:ting)?|disabl(?:e|ing)|skip(?:ping)?)\s+(?:(?:the|all|request|HTTP)\s+){0,3}timeouts?|"
        r"(?:without|no)\s+(?:an?\s+)?timeouts?|timeout\s*=\s*none)\b",
    ),
)
_CHECKS = tuple((*rule[:4], re.compile(rule[4], re.IGNORECASE)) for rule in _RULES)


def _first_signal(pattern: re.Pattern[str], texts: tuple[str, str]) -> str | None:
    for text in texts:
        negated_end = None
        for match in pattern.finditer(text):
            prefix = text[max(0, match.start() - 80):match.start()]
            if _NEGATION.search(prefix) or (
                negated_end is not None and text[negated_end:match.start()].strip().lower() == "or"
            ):
                negated_end = match.end()
                continue
            return match.group()
    return None


def lint_decision_text(decision: str, rationale: str = "") -> list[dict]:
    """Return at most one advisory per rule, in stable rule order."""
    texts = (decision, rationale)
    advisories = []
    for rule_id, tier, check_id, message, pattern in _CHECKS:
        signal = _first_signal(pattern, texts)
        if signal is None or (rule_id == "AGT-13" and _first_signal(_ADR_AMENDMENT, texts)):
            continue
        advisories.append({
            "rule_id": rule_id,
            "tier": tier,
            "check_id": check_id,
            "message": message,
            "matched_signal": signal,
        })
    return advisories
