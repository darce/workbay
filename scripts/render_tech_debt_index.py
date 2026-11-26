#!/usr/bin/env python3
"""Render or verify the corresponding implementation note from tech-debt entry files.

Canon v0.26.2:
- [REF-19] No information leakage: the index is derived from entry files.
- [DATA-14] Single writer: README.md is generated only by this renderer.
- [TEST-15] Prove green can go red: --check fails nonzero on missing or drifted README.md.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

HEADER = """\
# Tech debt: index

This is the one entry point for tech debt in this repository. Each entry is one file in this directory, with a title
and a metadata block (date, status, severity, component, decisions). Add a new entry as a new file here, never
elsewhere, then regenerate this index; do not edit the tables by hand.

Status is quoted from each file's own Status line, first sentence only. When a file carries a dated re-triage line
(`**Status YYYY-MM-DD:**`), the newest one wins and its date is shown; an empty date means the original Status
line. "not stated" means the file has no Status line yet. Attachments live in [`attachments/`](attachments/).
"""

STATUS_LINE_RE = re.compile(
    r"^\s*(?:>\s*)?(?:-\s*)?\**status(?:\s+(\d{4}-\d{2}-\d{2}))?\**\s*:?\**\s*:?\**\s*(.+)$",
    re.I,
)
FIRST_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.;])\s")
CLOSED_STATUS_RE = re.compile(
    r"^(resolved|closed|implemented|record-only|done|delivered|historical)\b",
    re.I,
)
MAX_STATUS_LEN = 140


def _default_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "docs" / "tech-debt"


def _extract_title(text: str, stem: str) -> str:
    for line in text.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            if title:
                return title.replace("|", "/")
    return stem.replace("|", "/")


def _normalize_status(raw: str) -> str:
    first = FIRST_SENTENCE_SPLIT_RE.split(raw.strip(), maxsplit=1)[0]
    cleaned = first.replace("*", "").replace("|", "/").strip().rstrip(".;:")
    if len(cleaned) > MAX_STATUS_LEN:
        prefix = cleaned[: MAX_STATUS_LEN - 1]
        if " " in prefix:
            prefix = prefix.rsplit(" ", 1)[0]
        cleaned = prefix.rstrip(" ,;:") + "…"
    return cleaned


def _extract_status(text: str) -> tuple[str, str]:
    found: list[tuple[str, str]] = []
    for line in text.splitlines():
        match = STATUS_LINE_RE.match(line)
        if not match:
            continue
        date = match.group(1) or ""
        status = _normalize_status(match.group(2))
        if status:
            found.append((date, status))
    if not found:
        return "", "not stated"
    return max(found, key=lambda item: item[0])


def _is_closed(status: str) -> bool:
    return bool(CLOSED_STATUS_RE.match(status))


def render_index(tech_debt_dir: Path) -> tuple[str, int, int, int]:
    """Render the tech-debt index markdown and return (content, open_count, closed_count, other_count)."""
    md_files = sorted(
        (p for p in tech_debt_dir.iterdir() if p.is_file() and p.suffix == ".md" and p.name != "README.md"),
        key=lambda p: p.name.lower(),
    )
    other_files = sorted(
        (p for p in tech_debt_dir.iterdir() if p.is_file() and p.suffix != ".md"),
        key=lambda p: p.name.lower(),
    )

    open_rows: list[tuple[str, str, str, str]] = []
    closed_rows: list[tuple[str, str, str, str]] = []

    for path in md_files:
        text = path.read_text(encoding="utf-8")
        title = _extract_title(text, path.stem)
        date, status = _extract_status(text)
        row = (title, path.name, status, date)
        if _is_closed(status):
            closed_rows.append(row)
        else:
            open_rows.append(row)

    lines: list[str] = [
        HEADER.rstrip("\n"),
        "",
        f"## Open, partial or queued ({len(open_rows)})",
        "",
        "| Entry | Status | Status date |",
        "|---|---|---|",
    ]
    for title, name, status, date in open_rows:
        lines.append(f"| [{title}]({name}) | {status} | {date} |")

    lines.extend(
        [
            "",
            f"## Done, resolved or record-only ({len(closed_rows)})",
            "",
            "| Entry | Status | Status date |",
            "|---|---|---|",
        ]
    )
    for title, name, status, date in closed_rows:
        lines.append(f"| [{title}]({name}) | {status} | {date} |")

    if other_files:
        lines.extend(["", "## Other files in this directory", ""])
        for path in other_files:
            lines.append(f"- [`{path.name}`]({path.name})")

    return "\n".join(lines) + "\n", len(open_rows), len(closed_rows), len(other_files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render or verify the corresponding implementation note.")
    parser.add_argument(
        "--dir",
        type=Path,
        default=_default_dir(),
        help="Path to the tech-debt directory (default: <repo_root>/docs/tech-debt).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that <dir>/README.md matches rendered output byte-for-byte; exit 1 on drift.",
    )
    args = parser.parse_args(argv)

    tech_debt_dir = args.dir
    if not tech_debt_dir.is_dir():
        print(f"error: tech-debt directory does not exist: {tech_debt_dir}", file=sys.stderr)
        return 1

    rendered, open_count, closed_count, other_count = render_index(tech_debt_dir)
    rendered_bytes = rendered.encode("utf-8")
    readme_path = tech_debt_dir / "README.md"

    if args.check:
        if not readme_path.is_file():
            print(
                f"error: {readme_path} is missing; run `make tech-debt-index` to generate it.",
                file=sys.stderr,
            )
            return 1
        actual_bytes = readme_path.read_bytes()
        if actual_bytes != rendered_bytes:
            print(
                f"error: {readme_path} is out of date with tech-debt entries; "
                "run `make tech-debt-index` to regenerate it.",
                file=sys.stderr,
            )
            return 1
        return 0

    readme_path.write_bytes(rendered_bytes)
    print(
        f"wrote {readme_path} ({open_count} open, {closed_count} closed, {other_count} other)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
