"""Mermaid flowchart renders for UX map flows.

Screen-graph semantics: steps sharing a ``screen_id`` collapse to one node
(self-loops when consecutive). Edge labels come from the *source* step's
``branch_label`` (last step label is not drawn as an edge without a successor).
"""

from __future__ import annotations

from workbay_canvas_mcp.export import _escape_label, _sanitize_id
from workbay_canvas_mcp.ux_map.models import UxMap

MERMAID_RENDER_VERSION = 1


def _id_map_for_screens(screen_ids: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for sid in sorted(set(screen_ids)):
        base = _sanitize_id(sid)
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        mapping[sid] = candidate
        used.add(candidate)
    return mapping


def render_flow_mermaid(map_: UxMap, *, flow_id: str) -> str:
    flow = next((f for f in map_.flows if f.id == flow_id), None)
    if flow is None:
        raise KeyError(f"unknown flow_id: {flow_id!r}")

    lines = ["flowchart TD"]
    title = flow.label or flow.id
    lines.append(f"  %% flow: {_escape_label(title)} job={_escape_label(flow.job)}")

    steps = flow.steps
    if not steps:
        lines.append('  empty["no steps"]')
        return "\n".join(lines) + "\n"

    id_map = _id_map_for_screens([s.screen_id for s in steps])
    declared: set[str] = set()

    def ensure_node(screen_id: str) -> str:
        nid = id_map[screen_id]
        if nid not in declared:
            screen = map_.screen_by_id(screen_id)
            label = screen.title if screen else screen_id
            kind = screen.kind.value if screen else "?"
            text = _escape_label(f"{label} ({kind})")
            lines.append(f'  {nid}["{text}"]')
            declared.add(nid)
        return nid

    for i, step in enumerate(steps):
        nid = ensure_node(step.screen_id)
        if i + 1 < len(steps):
            nxt = steps[i + 1]
            nid2 = ensure_node(nxt.screen_id)
            edge = (step.branch_label or "").strip()
            if edge:
                lines.append(f"  {nid} -->|{_escape_label(edge)}| {nid2}")
            else:
                lines.append(f"  {nid} --> {nid2}")

    return "\n".join(lines) + "\n"
