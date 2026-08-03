#!/usr/bin/env python3
"""PostToolUse hook: detect ACE rule references in review findings.

Fires after mcp__workbay-handoff-mcp__review_findings calls. Scans each
finding description for [sr-NNN]/[rg-NNN] patterns and appends matches
to .task-state/ace_reflect_log.jsonl for later processing by
`make ace-reflect`.

Hook contract (Claude Code / VS Code PostToolUse):
    - stdin: JSON with tool_name/toolName, tool_input/toolInput, tool_output/toolOutput
  - stdout: JSON with {"result": "continue"} or error
  - exit 0 to continue, non-zero to block (we never block)
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

# Inline the detection regex to avoid import dependency on scripts/ace/
import re

_RULE_REF_RE = re.compile(r"\[(?:sr|rg)-\d{3}\]")
_CONTRADICTION_KEYWORDS = frozenset(
    ["violat", "missing", "contradict", "breaks", "broke", "fail", "ignored", "bypass", "incorrect"]
)


def _detect_refs(text: str) -> list[str]:
    seen: dict[str, None] = {}
    for raw in _RULE_REF_RE.findall(text):
        seen[raw[1:-1]] = None
    return list(seen)


def _is_contradiction(text: str, rule_id: str) -> bool:
    text_lower = text.lower()
    pattern = re.compile(re.escape(f"[{rule_id}]"))
    for m in pattern.finditer(text_lower):
        start = max(0, m.start() - 80)
        end = min(len(text_lower), m.end() + 80)
        neighbourhood = text_lower[start:end]
        if any(kw in neighbourhood for kw in _CONTRADICTION_KEYWORDS):
            return True
    return False


def main() -> None:
    try:
        hook_input = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, OSError):
        print(json.dumps({"result": "continue"}))
        return

    tool_name = hook_input.get("tool_name") or hook_input.get("toolName") or ""
    if "review_findings" not in tool_name:
        print(json.dumps({"result": "continue"}))
        return

    tool_input = hook_input.get("tool_input") or hook_input.get("toolInput") or {}
    review = tool_input.get("review", {})
    operation = review.get("operation", "")

    # Extract finding descriptions from record or batch_record operations
    descriptions: list[tuple[str, str]] = []  # (finding_id, description)
    if operation == "record":
        fid = review.get("finding_id", "unknown")
        desc = review.get("description", "")
        if desc:
            descriptions.append((fid, desc))
    elif operation == "batch_record":
        for finding in review.get("findings", []):
            fid = finding.get("finding_id", "unknown")
            desc = finding.get("description", "")
            if desc:
                descriptions.append((fid, desc))

    if not descriptions:
        print(json.dumps({"result": "continue"}))
        return

    # Detect rule references
    records: list[dict] = []
    for finding_id, description in descriptions:
        refs = _detect_refs(description)
        for rule_id in refs:
            records.append({
                "finding_id": finding_id,
                "rule_id": rule_id,
                "contradicts": _is_contradiction(description, rule_id),
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            })

    if records:
        state_dir = Path(".task-state")
        state_dir.mkdir(parents=True, exist_ok=True)
        reflect_log = state_dir / "ace_reflect_log.jsonl"
        with reflect_log.open("a", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")

    print(json.dumps({"result": "continue"}))


if __name__ == "__main__":
    main()
