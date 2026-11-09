"""Filesystem load/save for UX maps (file SSOT)."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from workbay_canvas_mcp.ux_map.models import UxMap
from workbay_canvas_mcp.ux_map.parse import parse_map


def load_map(path: str | Path) -> UxMap:
    """Load and validate a map. Raises FileNotFoundError, JSONDecodeError, MapSchemaError."""
    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    return parse_map(raw)


def save_map(map_: UxMap, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = map_.model_dump(mode="json", exclude_none=True)
    payload["schema_version"] = map_.schema_version
    data = json.dumps(payload, indent=2) + "\n"
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=p.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
        os.replace(tmp, p)  # atomic; on failure the original p is untouched
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_renders(map_: UxMap, out_dir: str | Path) -> list[Path]:
    """Write Markdown bundle (ASCII+Mermaid embedded) under ``out_dir``.

    Returns paths written. Primary artifact: ``<map_ref>.md``.
    """
    from workbay_canvas_mcp.ux_map.render_markdown import render_markdown_bundle

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    md_path = out / f"{map_.map_ref}.md"
    md_path.write_text(render_markdown_bundle(map_), encoding="utf-8")
    return [md_path]
