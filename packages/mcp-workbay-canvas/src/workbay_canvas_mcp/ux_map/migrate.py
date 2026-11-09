"""Inspectable in-memory migration of UX-map documents (WBC-02 C03)."""

from __future__ import annotations

import copy
import difflib
import json


def migrate_document(raw: dict) -> dict:
    migrated = copy.deepcopy(raw)
    migrated.setdefault("schema_version", 1)
    return migrated


def migrate_texts(src_text: str, *, fromfile: str, tofile: str) -> tuple[str, str]:
    """Return (migrated_json_text, unified_diff). JSON: indent=2, trailing newline, UTF-8."""
    loaded = json.loads(src_text)
    if not isinstance(loaded, dict):
        raise TypeError("map document must be a JSON object")
    migrated = migrate_document(loaded)
    json_text = json.dumps(migrated, indent=2, ensure_ascii=False) + "\n"
    marked: list[str] = []
    for line in difflib.unified_diff(
        src_text.splitlines(keepends=True),
        json_text.splitlines(keepends=True),
        fromfile=fromfile,
        tofile=tofile,
    ):
        if line.endswith("\n"):
            marked.append(line)
        else:
            marked.append(line + "\n")
            marked.append("\\ No newline at end of file\n")
    return json_text, "".join(marked)
