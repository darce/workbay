"""Shared filesystem/JSON helpers (internal, RF29-S3-01).

Public home for helpers that grew up as ``install.py`` privates but are
consumed across modules (``harnesses.py``). ``install.py`` re-imports them
under the legacy private aliases for its internal call sites.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def deep_merge(dst: dict[str, Any], src: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge ``src`` into ``dst`` and return ``dst``.

    Dict-into-dict merges recurse. Any non-dict value in ``src`` (including
    lists) replaces the corresponding key in ``dst`` outright — list-concat
    semantics would silently grow user config across reruns.
    """
    for key, value in src.items():
        existing = dst.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            deep_merge(existing, value)
        elif isinstance(value, Mapping):
            new_dict: dict[str, Any] = {}
            deep_merge(new_dict, value)
            dst[key] = new_dict
        else:
            dst[key] = value
    return dst


def write_json_file(
    path: Path, payload: dict[str, Any], *, manifest_path: str | None = None
) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2) + "\n"
    manifest_entry_path = manifest_path or path.as_posix()
    if path.exists():
        previous = path.read_text()
        if previous == content:
            return {"path": manifest_entry_path, "action": "unchanged"}
        path.write_text(content)
        return {"path": manifest_entry_path, "action": "updated"}
    path.write_text(content)
    return {"path": manifest_entry_path, "action": "created"}
