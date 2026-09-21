"""Small fail-closed reader for the harness overlay-source contract."""

from __future__ import annotations

from pathlib import Path

_CONTRACT_PATH = Path("docs/workbay/contracts/harness-protocol.yaml")
_KEY = "tracked_overlay_source_paths:"


def load_tracked_overlay_source_paths(
    workspace_root: Path | str,
) -> frozenset[str] | None:
    """Read ``tracked_overlay_source_paths`` without adding a YAML dependency.

    The contract field is a flat YAML sequence.  ``None`` distinguishes an
    unreadable/missing key from a valid empty sequence, allowing callers to
    classify fail-closed instead of silently treating dead instrumentation as
    "no overlay paths" ([OBS-08]).
    """
    path = Path(workspace_root).expanduser().resolve() / _CONTRACT_PATH
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    key_indent: int | None = None
    values: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        indent = len(raw) - len(raw.lstrip(" "))
        if key_indent is None:
            if stripped == _KEY:
                key_indent = indent
            continue
        if not stripped or stripped.startswith("#"):
            continue
        if indent <= key_indent:
            break
        if not stripped.startswith("- "):
            # A nested mapping begins: the flat sequence has ended.
            break
        value = stripped[2:].split(" #", 1)[0].strip().strip("'\"")
        if value:
            values.append(value.rstrip("/"))
    if key_indent is None:
        return None
    return frozenset(values)
