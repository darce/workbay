"""Markdown bundle: inventory + ASCII + Mermaid."""

from __future__ import annotations

import re

from workbay_canvas_mcp.ux_map.models import UxMap
from workbay_canvas_mcp.ux_map.render_ascii import render_ascii_layout, render_flow_ascii
from workbay_canvas_mcp.ux_map.render_mermaid import render_flow_mermaid

MARKDOWN_RENDER_VERSION = 1

# Collapse CR/LF (and other C0 controls) so table rows stay single-line.
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")


def _md_cell(text: str) -> str:
    """Sanitize a value for a Markdown table cell (plain text, not code span)."""
    text = _CONTROL_RE.sub(" ", text)
    return text.replace("|", "\\|").replace("`", "\\`")


def _md_code_cell(text: str) -> str:
    """Sanitize a value placed inside a Markdown code span in a table cell."""
    return _CONTROL_RE.sub(" ", text).replace("|", "\\|")


def _fence(body: str) -> str:
    """Backtick fence longer than any run of consecutive backticks in ``body``."""
    longest = max((len(run) for run in re.findall(r"`+", body)), default=0)
    return "`" * max(3, longest + 1)


def render_markdown_bundle(map_: UxMap) -> str:
    parts: list[str] = []
    parts.append(f"# UX Map — {map_.map_ref}")
    parts.append("")
    parts.append(f"**Product:** `{map_.product}`")
    if map_.source_fixture:
        parts.append(f"**Source fixture:** `{map_.source_fixture}`")
    parts.append("")

    if map_.goals:
        parts.append("## Goals")
        for g in map_.goals:
            parts.append(f"- {g}")
        parts.append("")

    if map_.jobs:
        parts.append("## Jobs")
        for j in map_.jobs:
            parts.append(f"- `{j.id}` — {j.label}")
        parts.append("")

    if map_.screens:
        parts.append("## Screens")
        parts.append("| id | kind | route | title |")
        parts.append("| --- | --- | --- | --- |")
        for s in map_.screens:
            parts.append(
                f"| `{_md_code_cell(s.id)}` | {_md_cell(s.kind.value)} | "
                f"`{_md_code_cell(s.route)}` | {_md_cell(s.title)} |"
            )
        parts.append("")

    code_refs: list[tuple[str, str]] = []
    for screen in map_.screens:
        if screen.code_ref is not None:
            code_refs.append((f"screen:{screen.id}", screen.code_ref))
        for zone in screen.zones:
            if zone.code_ref is not None:
                code_refs.append((f"zone:{screen.id}/{zone.id}", zone.code_ref))
    for action in map_.actions:
        if action.code_ref is not None:
            code_refs.append((f"action:{action.id}", action.code_ref))
    if code_refs:
        parts.append("## Code references")
        for owner, code_ref in code_refs:
            parts.append(f"- `{_md_code_cell(owner)}` — `{_md_code_cell(code_ref)}`")
        parts.append("")

    for s in map_.screens:
        parts.append(f"### {s.title} (`{s.id}`)")
        parts.append("")
        screen_ascii = render_ascii_layout(map_, screen_id=s.id).rstrip("\n")
        screen_fence = _fence(screen_ascii)
        parts.append(screen_fence)
        parts.append(screen_ascii)
        parts.append(screen_fence)
        parts.append("")

    if map_.flows:
        parts.append("## Flows")
        for f in map_.flows:
            label = f.label or f.id
            parts.append(f"### {label} (`{f.id}`)")
            parts.append("")
            flow_ascii = render_flow_ascii(map_, flow_id=f.id).rstrip("\n")
            flow_fence = _fence(flow_ascii)
            parts.append(flow_fence)
            parts.append(flow_ascii)
            parts.append(flow_fence)
            parts.append("")
            mermaid = render_flow_mermaid(map_, flow_id=f.id).rstrip("\n")
            mermaid_fence = _fence(mermaid)
            parts.append(f"{mermaid_fence}mermaid")
            parts.append(mermaid)
            parts.append(mermaid_fence)
            parts.append("")

    if map_.open_questions:
        parts.append("## Open questions")
        for q in map_.open_questions:
            parts.append(f"- {q}")
        parts.append("")

    if map_.not_doing:
        parts.append("## Not doing")
        for n in map_.not_doing:
            parts.append(f"- {n}")
        parts.append("")

    return "\n".join(parts)
