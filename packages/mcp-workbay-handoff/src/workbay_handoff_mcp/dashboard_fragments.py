"""Dashboard fragment renderer (internal).

Splits the rendered DASHBOARD markdown into per-section fragment files
under ``.task-state/DASHBOARD.d/`` so prompt-cache invalidation is
scoped to the section that actually changed.

The renderer is fragment-aware but back-compat: ``DASHBOARD.txt`` still
exists as a concatenated index for one minor release.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

FRAGMENT_DIR_NAME = "DASHBOARD.d"
MANIFEST_FILENAME = "dashboard_fragments.manifest.json"
HEADER_PREFIX = "## "


@dataclass(frozen=True)
class DashboardFragment:
    filename: str
    content: str
    dirty_key: str
    section_title: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def _slugify_section_title(title: str) -> str:
    """Convert a section heading to a stable filename slug."""

    text = title.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "_", text).strip("_")
    return text or "section"


def _hash_content(content: str) -> str:
    """Stable per-fragment content hash used for dirty-flagging."""

    return hashlib.sha256(content.encode("utf-8")).hexdigest()


_SETEXT_UNDERLINE_RE = re.compile(r"^[-=]{3,}\s*$")


def _is_setext_h2_pair(title_line: str, underline_line: str | None) -> bool:
    """Return True for plain-text + ``---``/``===`` underline pairs.

    The production dashboard uses Setext-style headings (``SECTION\\n---``)
    rather than ATX (``## SECTION``). BR-03 needs the fragmenter to
    recognize both so the real render path actually splits per-section.
    """

    if underline_line is None:
        return False
    title = title_line.rstrip("\n")
    if not title.strip():
        return False
    if title.startswith(HEADER_PREFIX):
        return False
    return bool(_SETEXT_UNDERLINE_RE.match(underline_line.rstrip("\n")))


def collect_dashboard_fragments(markdown: str) -> list[DashboardFragment]:
    """Split rendered ``DASHBOARD`` markdown into per-section fragments.

    Each section heading starts a new fragment; the fragment content
    includes the heading line(s) and runs until the next heading or
    end of file. Both ATX (``## title``) and Setext (``title\\n---``)
    H2 headings open a new fragment — the production dashboard emits
    Setext, while the unit-test corpora still use ATX.
    """

    fragments: list[DashboardFragment] = []
    lines = markdown.splitlines(keepends=True)
    current_title: str | None = None
    current_lines: list[str] = []
    seen_slugs: dict[str, int] = {}

    def _flush() -> None:
        if current_title is None:
            return
        slug = _slugify_section_title(current_title)
        ordinal = seen_slugs.get(slug, 0)
        seen_slugs[slug] = ordinal + 1
        filename = f"{slug}.md" if ordinal == 0 else f"{slug}_{ordinal}.md"
        content = "".join(current_lines).rstrip("\n") + "\n"
        fragments.append(
            DashboardFragment(
                filename=filename,
                content=content,
                dirty_key=_hash_content(content),
                section_title=current_title,
            )
        )

    i = 0
    while i < len(lines):
        line = lines[i]
        next_line = lines[i + 1] if i + 1 < len(lines) else None
        if line.startswith(HEADER_PREFIX):
            _flush()
            current_title = line[len(HEADER_PREFIX) :].strip()
            current_lines = [line]
            i += 1
            continue
        if _is_setext_h2_pair(line, next_line):
            _flush()
            current_title = line.strip()
            current_lines = [line, next_line]  # type: ignore[list-item]
            i += 2
            continue
        if current_title is not None:
            current_lines.append(line)
        i += 1
    _flush()

    return fragments


def _read_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        return {"fragments": {}}
    try:
        data = json.loads(manifest_path.read_text())
    except json.JSONDecodeError:
        return {"fragments": {}}
    if not isinstance(data, dict):
        return {"fragments": {}}
    if not isinstance(data.get("fragments"), dict):
        data["fragments"] = {}
    return data


def maybe_write_dashboard_fragments(
    state_dir: Path,
    fragments: list[DashboardFragment],
) -> dict[str, Any]:
    """Write only fragments whose ``dirty_key`` differs from the manifest.

    Maintains ``state_dir/dashboard_fragments.manifest.json`` with per-
    fragment hashes and last-write timestamps. Returns a result dict
    with ``written`` and ``unchanged`` filename lists.
    """

    state_dir = Path(state_dir)
    fragment_dir = state_dir / FRAGMENT_DIR_NAME
    fragment_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = state_dir / MANIFEST_FILENAME
    manifest = _read_manifest(manifest_path)
    manifest_fragments: dict[str, dict[str, Any]] = manifest["fragments"]

    now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    written: list[str] = []
    unchanged: list[str] = []

    for fragment in fragments:
        prior = manifest_fragments.get(fragment.filename)
        if prior is not None and prior.get("dirty_key") == fragment.dirty_key:
            unchanged.append(fragment.filename)
            continue
        target = fragment_dir / fragment.filename
        target.write_text(fragment.content)
        manifest_fragments[fragment.filename] = {
            "dirty_key": fragment.dirty_key,
            "section_title": fragment.section_title,
            "last_written_at": now_iso,
        }
        written.append(fragment.filename)

    manifest["last_render_at"] = now_iso
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))

    return {
        "written": written,
        "unchanged": unchanged,
        "manifest_path": str(manifest_path),
        "fragment_dir": str(fragment_dir),
    }
