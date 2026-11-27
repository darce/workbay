"""Check standalone generated placeholders in finished Markdown prose."""

from __future__ import annotations

import re

_CHECK = "markdown.finished-scaffold"
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_PLACEHOLDER = re.compile(r"^ {0,3}\[insert (?:source|citation) here\][ \t]*$", re.IGNORECASE)
_DRAFT = re.compile(
    r"^\s*(?:(?:layout|status)\s*:\s*|#{1,6}\s+)(?:draft|template|proposal|awaiting triage)\b",
    re.IGNORECASE,
)


def scan(source: str, check_ids: frozenset[str]) -> list[tuple[str, int, str]]:
    if _CHECK not in check_ids:
        return []
    lines = source.splitlines()
    if any(_DRAFT.match(line) for line in lines[:12]):
        return []

    fence = ""
    comment = False
    findings = []
    for index, line in enumerate(lines):
        marker = _FENCE.match(line)
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                fence = ""
            continue
        if comment or "<!--" in line:
            if "<!--" in line:
                comment = "-->" not in line.rsplit("<!--", 1)[1]
            elif "-->" in line:
                comment = False
            continue
        if marker:
            fence = marker[1]
            continue
        previous_is_blank = index == 0 or not lines[index - 1].strip()
        next_is_blank = index + 1 == len(lines) or not lines[index + 1].strip()
        if previous_is_blank and next_is_blank and _PLACEHOLDER.fullmatch(line):
            findings.append((_CHECK, index + 1, line.strip()))
    return findings
