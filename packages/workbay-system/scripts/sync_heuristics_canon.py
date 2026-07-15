#!/usr/bin/env python3
"""Sync heuristics-canon lexicons into payload SSOT ``*-heuristics.md`` files.

internal. Network is isolated in ``fetch_canon`` only; pure merge /
localization / sync logic is driven by tests against a local fixture canon dir.

Dry-run is the default (prints a per-lexicon section-level summary, writes
nothing). ``--execute`` writes SSOTs and the provenance sidecar
``.heuristics_canon_state.json`` (outside ``payload/`` so version pins never
land in the lexicons themselves).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from heuristics_canon_manifest import (
    CANON_REPO,
    CANON_URL,
    LEXICONS,
    LOCALIZATION_ENGINEERING,
    Lexicon,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
DEFAULT_PAYLOAD_RULES_DIR = (
    PACKAGE_ROOT / "workbay_system" / "payload" / "docs" / "workbay" / "rules"
)
DEFAULT_STATE_PATH = SCRIPTS_DIR / ".heuristics_canon_state.json"

# Table data rows: first cell is an ID like AGT-01, SEC-01, WEB-12.
ROW_ID_RE = re.compile(r"^\| ([A-Z][A-Z0-9]*-\d+) \|")
SECTION_RE = re.compile(r"^## (.+?)\s*$")
# Local-owned rows carry findings-cluster in the Src column (forward-provisioning).
# All other Src values are canon-owned, including book/source slugs and the
# literal ``bootstrap`` marker used in security.md.
LOCAL_SRC_MARKER = "findings-cluster"

ABOUT_AGENT_SKILLS = "concept vocabulary for agent skills"
ABOUT_WORKBAY_SKILLS = "concept vocabulary for WorkBay skills"

CONSUMPTION_CANON_PREFIX = "**Consumption**:"
CONSUMPTION_LOCAL_PREFIX = "**Consumption contract**:"

# Local addition to the engineering About: a sync-provenance note. Applied as a
# localization so it is re-materialized on every sync (DATA-14 — the SSOT is
# never hand-edited) and is idempotent (presence-checked before insertion).
ABOUT_SYNC_SENTENCE = (
    "> _Synced from the heuristics canon by `make heuristics-sync` — "
    "edit the canon, not the book-derived rows here._"
)


class SyncError(Exception):
    """Hard merge failure (local-only section, ID collision, etc.)."""


@dataclass
class TableRow:
    row_id: str
    line: str
    src: str

    @property
    def is_local_owned(self) -> bool:
        return LOCAL_SRC_MARKER in self.src


@dataclass
class Section:
    title: str
    lines: list[str] = field(default_factory=list)

    @property
    def heading_line(self) -> str:
        return f"## {self.title}"

    def table_rows(self) -> list[TableRow]:
        rows: list[TableRow] = []
        for line in self.lines:
            parsed = _parse_table_row(line)
            if parsed is not None:
                rows.append(parsed)
        return rows

    def row_ids(self) -> list[str]:
        return [r.row_id for r in self.table_rows()]


@dataclass
class ParsedDoc:
    """Lexicon markdown split into pre-section preamble + ``##`` sections."""

    preamble: list[str]
    sections: list[Section]

    def section_titles(self) -> list[str]:
        return [s.title for s in self.sections]

    def section_by_title(self) -> dict[str, Section]:
        return {s.title: s for s in self.sections}

    def all_row_ids(self) -> set[str]:
        ids: set[str] = set()
        for section in self.sections:
            ids.update(section.row_ids())
        return ids

    def render(self) -> str:
        # Reconstruct line-for-line from parse partitions so round-trips are
        # byte-stable (required for same-SHA re-sync idempotency).
        parts: list[str] = list(self.preamble)
        for section in self.sections:
            parts.append(section.heading_line)
            parts.extend(section.lines)
        text = "\n".join(parts)
        if not text.endswith("\n"):
            text += "\n"
        return text


@dataclass
class SectionSummary:
    title: str
    status: str  # new | removed | unchanged | changed
    added_ids: list[str] = field(default_factory=list)
    removed_ids: list[str] = field(default_factory=list)
    preserved_local_ids: list[str] = field(default_factory=list)


@dataclass
class LexiconSummary:
    canon_name: str
    local_filename: str
    action: str  # create | update | unchanged
    sections: list[SectionSummary] = field(default_factory=list)
    sha: str = ""

    def format_lines(self) -> list[str]:
        lines = [f"{self.canon_name} ({self.local_filename}): {self.action}"]
        for sec in self.sections:
            detail_bits: list[str] = []
            if sec.added_ids:
                detail_bits.append(f"+{len(sec.added_ids)} ids")
            if sec.removed_ids:
                detail_bits.append(f"-{len(sec.removed_ids)} ids")
            if sec.preserved_local_ids:
                detail_bits.append(
                    f"local:{','.join(sec.preserved_local_ids)}"
                )
            detail = f" ({', '.join(detail_bits)})" if detail_bits else ""
            lines.append(f"  [{sec.status}] ## {sec.title}{detail}")
        return lines


def _parse_table_row(line: str) -> TableRow | None:
    match = ROW_ID_RE.match(line)
    if not match:
        return None
    row_id = match.group(1)
    # "| ID | ... | Src |" → cells[0] and cells[-1] are empty edge slots.
    cells = [c.strip() for c in line.strip().split("|")]
    if len(cells) >= 3 and cells[0] == "" and cells[-1] == "":
        inner = cells[1:-1]
    else:
        inner = [c for c in cells if c != ""]
    src = inner[-1] if inner else ""
    return TableRow(row_id=row_id, line=line.rstrip("\n"), src=src)


def _id_sort_key(row_id: str) -> tuple[str, int, str]:
    match = re.match(r"^([A-Z][A-Z0-9]*)-(\d+)$", row_id)
    if match:
        return (match.group(1), int(match.group(2)), row_id)
    return (row_id, 0, row_id)


def parse_lexicon_doc(text: str) -> ParsedDoc:
    """Split a lexicon markdown doc into preamble + ## sections."""
    lines = text.splitlines()
    preamble: list[str] = []
    sections: list[Section] = []
    current: Section | None = None

    for line in lines:
        sec_match = SECTION_RE.match(line)
        if sec_match:
            current = Section(title=sec_match.group(1).strip())
            sections.append(current)
            continue
        if current is None:
            preamble.append(line)
        else:
            current.lines.append(line)

    return ParsedDoc(preamble=preamble, sections=sections)


def extract_consumption_contract(local_text: str) -> str | None:
    """Return the local ``**Consumption contract**: ...`` line, if present."""
    for line in local_text.splitlines():
        if line.startswith(CONSUMPTION_LOCAL_PREFIX):
            return line
    return None


def rewrite_cross_lexicon_filenames(text: str) -> str:
    """Rewrite canon lexicon filenames to their local ``*-heuristics.md`` names.

    Canon lexicons cross-link each other by their canon basename (e.g. the
    engineering ``## Security`` tombstone links ``[security.md](security.md)``),
    but the local SSOTs use the ``<name>-heuristics.md`` filenames. Rewrite the
    link target and inline-code forms so cross-lexicon links resolve locally.
    Applied to every lexicon (universal), idempotent because it always starts
    from canon text. Only touches markdown-link/backtick contexts to avoid
    rewriting incidental prose.
    """
    out = text
    for lex in LEXICONS:
        canon = f"{lex.canon_name}.md"
        local = lex.local_filename
        if canon == local:
            continue
        out = out.replace(f"]({canon})", f"]({local})")
        out = out.replace(f"]({canon}#", f"]({local}#")
        out = out.replace(f"`{canon}`", f"`{local}`")
    return out


def _insert_about_sync_sentence(text: str) -> str:
    """Insert the sync-provenance note at the end of the About section (once)."""
    if ABOUT_SYNC_SENTENCE in text:
        return text
    lines = text.splitlines()
    about_idx: int | None = None
    for i, line in enumerate(lines):
        if line.strip() == "## About this document":
            about_idx = i
            break
    if about_idx is None:
        return text
    end = len(lines)
    for j in range(about_idx + 1, len(lines)):
        if lines[j].startswith("## "):
            end = j
            break
    # Trim trailing blank lines inside the About block, then append the note
    # followed by one blank line before the next section heading.
    body_end = end
    while body_end > about_idx + 1 and lines[body_end - 1].strip() == "":
        body_end -= 1
    block = ["", ABOUT_SYNC_SENTENCE, ""]
    new_lines = lines[:body_end] + block + lines[end:]
    out = "\n".join(new_lines)
    if text.endswith("\n") and not out.endswith("\n"):
        out += "\n"
    return out


def apply_localizations(
    text: str,
    ruleset: str | None,
    *,
    consumption_contract: str | None = None,
) -> str:
    """Apply per-lexicon localization transforms. Identity when ruleset is None."""
    if ruleset is None:
        return text
    if ruleset != LOCALIZATION_ENGINEERING:
        raise SyncError(f"unknown localization ruleset: {ruleset!r}")

    out = text.replace(ABOUT_AGENT_SKILLS, ABOUT_WORKBAY_SKILLS)

    if consumption_contract is not None:
        replaced_lines: list[str] = []
        did_replace = False
        for line in out.splitlines():
            if line.startswith(CONSUMPTION_CANON_PREFIX) or line.startswith(
                CONSUMPTION_LOCAL_PREFIX
            ):
                replaced_lines.append(consumption_contract)
                did_replace = True
            else:
                replaced_lines.append(line)
        if not did_replace:
            raise SyncError(
                "engineering localization (b): no **Consumption** line found in canon text"
            )
        out = "\n".join(replaced_lines)
        if text.endswith("\n") and not out.endswith("\n"):
            out += "\n"

    out = _insert_about_sync_sentence(out)
    return out


def _append_local_rows(section: Section, local_rows: Sequence[TableRow]) -> Section:
    """Return a copy of section with local rows appended after the last table row."""
    if not local_rows:
        return Section(title=section.title, lines=list(section.lines))

    ordered = sorted(local_rows, key=lambda r: _id_sort_key(r.row_id))
    lines = list(section.lines)

    # Find last table data row index; insert after it.
    last_row_idx: int | None = None
    for i, line in enumerate(lines):
        if _parse_table_row(line) is not None:
            last_row_idx = i

    insert_at = (last_row_idx + 1) if last_row_idx is not None else len(lines)
    new_lines = lines[:insert_at] + [r.line for r in ordered] + lines[insert_at:]
    return Section(title=section.title, lines=new_lines)


def merge_lexicon(
    canon_text: str,
    existing_local_text: str | None,
    lexicon: Lexicon,
) -> str:
    """Merge localized canon with local-owned findings-cluster rows.

    Merge policy (row-level):
    - Section set = canon's.
    - Canon-owned rows (Src book/source slug or ``bootstrap``) come from canon.
    - Local-owned rows (Src matches ``findings-cluster``) are preserved and
      re-appended to their section in ID order after canon rows.
    - Local-only section → hard error ([OBS-08]).
    - ID collision (same ID in canon and local rows) → hard error.
    """
    consumption: str | None = None
    if lexicon.localization == LOCALIZATION_ENGINEERING and existing_local_text:
        consumption = extract_consumption_contract(existing_local_text)

    # Universal: rewrite canon cross-lexicon filenames to local *-heuristics.md.
    canon_text = rewrite_cross_lexicon_filenames(canon_text)
    localized = apply_localizations(
        canon_text,
        lexicon.localization,
        consumption_contract=consumption,
    )
    canon_doc = parse_lexicon_doc(localized)

    if existing_local_text is None:
        # No local SSOT yet: materialize localized canon via the stable render path.
        return canon_doc.render()

    local_doc = parse_lexicon_doc(existing_local_text)
    canon_titles = set(canon_doc.section_titles())
    local_only = [t for t in local_doc.section_titles() if t not in canon_titles]
    if local_only:
        names = ", ".join(f"## {t}" for t in local_only)
        raise SyncError(
            f"[{lexicon.canon_name}] local-only section(s) absent from canon: {names} "
            f"(cite [OBS-08] — section set must equal canon's; move or drop the local section)"
        )

    canon_ids = canon_doc.all_row_ids()
    local_by_section = local_doc.section_by_title()
    merged_sections: list[Section] = []

    for section in canon_doc.sections:
        local_section = local_by_section.get(section.title)
        local_owned: list[TableRow] = []
        if local_section is not None:
            for row in local_section.table_rows():
                if not row.is_local_owned:
                    continue
                if row.row_id in canon_ids:
                    raise SyncError(
                        f"[{lexicon.canon_name}] ID collision on {row.row_id}: "
                        f"present in both canon and local findings-cluster row "
                        f"(canon renumbered under us — needs a human)"
                    )
                local_owned.append(row)
        merged_sections.append(_append_local_rows(section, local_owned))

    merged = ParsedDoc(preamble=list(canon_doc.preamble), sections=merged_sections)
    return merged.render()


def _normalize_trailing_newline(text: str) -> str:
    if not text.endswith("\n"):
        return text + "\n"
    return text


def _content_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _section_summaries(
    before: ParsedDoc | None,
    after: ParsedDoc,
    preserved_local_ids: set[str],
) -> list[SectionSummary]:
    summaries: list[SectionSummary] = []
    before_map = before.section_by_title() if before else {}
    after_titles = after.section_titles()
    before_titles = before.section_titles() if before else []

    for title in after_titles:
        after_sec = after.section_by_title()[title]
        after_ids = set(after_sec.row_ids())
        if title not in before_map:
            summaries.append(
                SectionSummary(
                    title=title,
                    status="new",
                    added_ids=sorted(after_ids, key=_id_sort_key),
                    preserved_local_ids=sorted(
                        after_ids & preserved_local_ids, key=_id_sort_key
                    ),
                )
            )
            continue
        before_ids = set(before_map[title].row_ids())
        added = sorted(after_ids - before_ids, key=_id_sort_key)
        removed = sorted(before_ids - after_ids, key=_id_sort_key)
        preserved = sorted(after_ids & preserved_local_ids, key=_id_sort_key)
        # Content change without id set change still counts as changed.
        before_body = "\n".join(before_map[title].lines)
        after_body = "\n".join(after_sec.lines)
        if not added and not removed and before_body == after_body:
            status = "unchanged"
        else:
            status = "changed"
        summaries.append(
            SectionSummary(
                title=title,
                status=status,
                added_ids=added,
                removed_ids=removed,
                preserved_local_ids=preserved,
            )
        )

    for title in before_titles:
        if title not in after.section_by_title():
            before_ids = set(before_map[title].row_ids())
            summaries.append(
                SectionSummary(
                    title=title,
                    status="removed",
                    removed_ids=sorted(before_ids, key=_id_sort_key),
                )
            )
    return summaries


def _local_owned_ids(text: str | None) -> set[str]:
    if not text:
        return set()
    doc = parse_lexicon_doc(text)
    ids: set[str] = set()
    for section in doc.sections:
        for row in section.table_rows():
            if row.is_local_owned:
                ids.add(row.row_id)
    return ids


def fetch_canon(canon_ref: str, cache_dir: Path) -> dict[str, str]:
    """Fetch all manifest lexicons from the private canon via ``gh`` only.

    This is the ONLY network function. Not exercised by unit tests.
    Returns ``{canon_name: sha}`` for the fetched blobs.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    shas: dict[str, str] = {}
    for entry in LEXICONS:
        path_in_repo = entry.canon_path_in_repo
        # GitHub Contents API: authenticated via gh; never anonymous git.
        api_path = f"repos/{CANON_REPO}/contents/{path_in_repo}?ref={canon_ref}"
        proc = subprocess.run(
            ["gh", "api", api_path, "--jq", "{content: .content, sha: .sha}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise SyncError(
                f"fetch_canon failed for {path_in_repo}@{canon_ref}: {proc.stderr.strip()}"
            )
        payload = json.loads(proc.stdout)
        content_b64 = payload["content"].replace("\n", "")
        raw = base64.b64decode(content_b64)
        out_path = cache_dir / f"{entry.canon_name}.md"
        out_path.write_bytes(raw)
        shas[entry.canon_name] = payload["sha"]
    return shas


def sync(
    canon_dir: Path,
    payload_rules_dir: Path,
    *,
    execute: bool = False,
    canon_ref: str = "main",
    canon_url: str = CANON_URL,
    state_path: Path | None = None,
    lexicons: Sequence[Lexicon] | None = None,
    precomputed_shas: dict[str, str] | None = None,
) -> list[LexiconSummary]:
    """Sync lexicons from a local canon dir into payload rules.

    Pure over the filesystem inputs (no network). When ``execute`` is False
    (default), nothing is written. When True, writes each SSOT and the
    provenance sidecar (if ``state_path`` is set).
    """
    entries: Sequence[Lexicon] = lexicons if lexicons is not None else LEXICONS
    summaries: list[LexiconSummary] = []
    sha_map: dict[str, str] = dict(precomputed_shas or {})

    for entry in entries:
        canon_file = canon_dir / f"{entry.canon_name}.md"
        if not canon_file.is_file():
            raise SyncError(f"missing canon file: {canon_file}")
        canon_text = canon_file.read_text(encoding="utf-8")
        if entry.canon_name not in sha_map:
            sha_map[entry.canon_name] = _content_sha(canon_text)

        local_path = payload_rules_dir / entry.local_filename
        existing: str | None = None
        if local_path.is_file():
            existing = local_path.read_text(encoding="utf-8")

        merged = merge_lexicon(canon_text, existing, entry)
        merged = _normalize_trailing_newline(merged)

        before_doc = parse_lexicon_doc(existing) if existing is not None else None
        after_doc = parse_lexicon_doc(merged)
        preserved = _local_owned_ids(existing)
        sections = _section_summaries(before_doc, after_doc, preserved)

        if existing is None:
            action = "create"
        elif existing == merged:
            action = "unchanged"
        else:
            action = "update"

        summaries.append(
            LexiconSummary(
                canon_name=entry.canon_name,
                local_filename=entry.local_filename,
                action=action,
                sections=sections,
                sha=sha_map[entry.canon_name],
            )
        )

        if execute:
            payload_rules_dir.mkdir(parents=True, exist_ok=True)
            local_path.write_text(merged, encoding="utf-8")

    if execute and state_path is not None:
        state = {
            "canon_url": canon_url,
            "canon_ref": canon_ref,
            "synced_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "lexicons": {
                name: {"sha": sha_map[name]}
                for name in (e.canon_name for e in entries)
            },
        }
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps(state, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return summaries


def format_sync_summary(summaries: Iterable[LexiconSummary]) -> str:
    lines: list[str] = []
    for summary in summaries:
        lines.extend(summary.format_lines())
    return "\n".join(lines) + ("\n" if lines else "")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync darce/heuristics-canon lexicons into payload *-heuristics.md SSOTs. "
            "Dry-run by default; pass --execute to write."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write SSOTs and provenance sidecar (default is dry-run)",
    )
    parser.add_argument(
        "--canon-ref",
        default="main",
        help="Canon git ref to fetch (default: main)",
    )
    parser.add_argument(
        "--canon-dir",
        type=Path,
        default=None,
        help="Local canon dir of <name>.md files; bypasses network fetch (tests/local)",
    )
    parser.add_argument(
        "--payload-rules-dir",
        type=Path,
        default=DEFAULT_PAYLOAD_RULES_DIR,
        help="Target rules directory for *-heuristics.md SSOTs",
    )
    parser.add_argument(
        "--state-path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="Provenance sidecar path (written only on --execute)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Cache dir for fetched canon files (default: under package build area)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    precomputed_shas: dict[str, str] | None = None

    if args.canon_dir is not None:
        canon_dir = args.canon_dir
        if not canon_dir.is_dir():
            print(f"error: --canon-dir not a directory: {canon_dir}", file=sys.stderr)
            return 2
    else:
        cache_dir = args.cache_dir or (
            PACKAGE_ROOT / ".cache" / "heuristics-canon" / args.canon_ref
        )
        try:
            precomputed_shas = fetch_canon(args.canon_ref, cache_dir)
        except SyncError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        canon_dir = cache_dir

    try:
        summaries = sync(
            canon_dir,
            args.payload_rules_dir,
            execute=args.execute,
            canon_ref=args.canon_ref,
            state_path=args.state_path if args.execute else None,
            precomputed_shas=precomputed_shas,
        )
    except SyncError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    mode = "execute" if args.execute else "dry-run"
    print(f"heuristics-canon sync ({mode})")
    print(format_sync_summary(summaries), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
