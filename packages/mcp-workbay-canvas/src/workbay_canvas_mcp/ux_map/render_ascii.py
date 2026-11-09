"""ASCII box layouts for UX map screens (grayscale structure)."""

from __future__ import annotations

import re

from workbay_canvas_mcp.ux_map.models import Action, ActionHierarchy, UxMap

_WIDTH = 62
_INNER = _WIDTH - 4  # content between "| " and " |"

# Collapse CR/LF/tab and other C0 controls so labels never split box rows.
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")


_HIERARCHY_ORDER = {
    ActionHierarchy.PRIMARY: 0,
    ActionHierarchy.SECONDARY: 1,
    ActionHierarchy.TERTIARY: 2,
    ActionHierarchy.DESTRUCTIVE: 3,
}

_HIERARCHY_MARK = {
    ActionHierarchy.PRIMARY: "PRIMARY",
    ActionHierarchy.SECONDARY: "secondary",
    ActionHierarchy.TERTIARY: "tertiary",
    ActionHierarchy.DESTRUCTIVE: "DESTRUCTIVE",
}


def _sanitize_cell(text: str) -> str:
    """Collapse control characters so a cell never contains a newline."""
    return _CONTROL_RE.sub(" ", text)


def _wrap_chunks(text: str, width: int) -> list[str]:
    """Wrap ``text`` to ``width`` with a visible trailing ``\\`` on continued lines.

    Overflow continues on the next line with a two-space indent. Shared by
    screen box rows (inner width) and flow ASCII (full line width).
    """
    text = _sanitize_cell(text)
    if width < 2:
        return [text[:width]] if width > 0 else [""]
    chunks: list[str] = []
    remaining = text
    continuation = False
    while True:
        piece = f"  {remaining}" if continuation else remaining
        if len(piece) <= width:
            chunks.append(piece)
            break
        chunks.append(piece[: width - 1] + "\\")
        remaining = piece[width - 1 :]
        continuation = True
    return chunks


def _row(text: str = "") -> str:
    chunks = _wrap_chunks(text, _INNER)
    return "\n".join(f"| {chunk:<{_INNER}} |" for chunk in chunks)


def _rule() -> str:
    return "+" + "-" * (_WIDTH - 2) + "+"


def _actions_for_screen(map_: UxMap, screen_id: str) -> list[Action]:
    screen = map_.screen_by_id(screen_id)
    assert screen is not None
    actions = [a for a in map_.actions if a.screen_id == screen_id]
    by_id = {a.id: a for a in actions}
    if screen.primary_action_id and screen.primary_action_id not in by_id:
        primary = map_.action_by_id(screen.primary_action_id)
        if primary is not None:
            actions = [primary, *actions]
            by_id[primary.id] = primary

    def sort_key(a: Action) -> tuple[int, int, str]:
        # pin declared primary first
        pin = 0 if screen.primary_action_id and a.id == screen.primary_action_id else 1
        return (pin, _HIERARCHY_ORDER.get(a.hierarchy, 9), a.id)

    return sorted(actions, key=sort_key)


def render_ascii_layout(map_: UxMap, *, screen_id: str) -> str:
    screen = map_.screen_by_id(screen_id)
    if screen is None:
        raise KeyError(f"unknown screen_id: {screen_id!r}")

    actions = _actions_for_screen(map_, screen_id)
    lines: list[str] = [_rule()]
    kind = screen.kind.value
    lines.append(_row(f"{screen.title}  [{kind}]  {screen.route}"))
    lines.append(_row(screen.purpose))
    lines.append(_rule())

    if screen.zones:
        lines.append(_row("ZONES"))
        for z in screen.zones:
            st = ",".join(s.value for s in z.states) if z.states else "-"
            lines.append(_row(f"  - {z.label} ({z.role.value}) states=[{st}]"))
        lines.append(_rule())

    if actions:
        lines.append(_row("ACTIONS"))
        for a in actions:
            mark = _HIERARCHY_MARK.get(a.hierarchy, a.hierarchy.value)
            flags: list[str] = []
            if a.costly:
                flags.append("costly")
            if a.preview_required:
                flags.append("preview")
            if a.irreversible:
                flags.append("irreversible")
            flag_s = f" ({','.join(flags)})" if flags else ""
            lines.append(_row(f"  [{mark}] {a.verb} -> {a.target}{flag_s}"))
        lines.append(_rule())

    if screen.states:
        states = " | ".join(s.value for s in screen.states)
        lines.append(_row(f"states: {states}"))
        lines.append(_rule())

    out = "\n".join(lines) + "\n"
    # Defence in depth: pad/truncate to _WIDTH rather than crash.
    fixed: list[str] = []
    for line in out.splitlines():
        if line.startswith("|") or line.startswith("+"):
            if len(line) < _WIDTH:
                line = line + " " * (_WIDTH - len(line))
            elif len(line) > _WIDTH:
                line = line[:_WIDTH]
        fixed.append(line)
    return "\n".join(fixed) + "\n"


FLOW_ASCII_WIDTH = 62  # same as _WIDTH
ASCII_LAYOUT_VERSION = 1
FLOW_ASCII_VERSION = 1


def _flow_physical_lines(text: str) -> list[str]:
    return [chunk.rstrip(" ") for chunk in _wrap_chunks(text, FLOW_ASCII_WIDTH)]


def render_flow_ascii(map_: UxMap, *, flow_id: str) -> str:
    flow = next((f for f in map_.flows if f.id == flow_id), None)
    if flow is None:
        raise KeyError(f"unknown flow_id: {flow_id!r}")

    label = _sanitize_cell(flow.label or "").strip()
    title = label if label else flow.id
    lines: list[str] = []
    lines.extend(_flow_physical_lines(f"flow: {title} ({flow.id}) job={flow.job}"))

    steps = flow.steps
    if not steps:
        lines.extend(_flow_physical_lines("  (no steps)"))
        return "\n".join(lines) + "\n"

    visits: dict[str, int] = {}
    for i, step in enumerate(steps):
        visits[step.screen_id] = visits.get(step.screen_id, 0) + 1
        visit_n = visits[step.screen_id]
        screen = map_.screen_by_id(step.screen_id)
        screen_title = screen.title if screen is not None else step.screen_id
        visit_suffix = f" (visit {visit_n})" if visit_n >= 2 else ""
        lines.extend(_flow_physical_lines(f"  {i + 1}. {screen_title} [{step.screen_id}]{visit_suffix}"))
        has_successor = i + 1 < len(steps)
        edge = _sanitize_cell((step.branch_label or "").strip())
        if has_successor:
            edge_line = f"     --{edge}-->" if edge else "     -->"
            lines.extend(_flow_physical_lines(edge_line))

    return "\n".join(lines) + "\n"
