"""Agent-error harvest report + export (internal / internal).

The maintainer-facing read side of the ``agent_errors`` ledger:

- ``errors_report`` clusters rows by ``(error_class, package_name)``
  with a package-version range per cluster, emitting counts,
  first/last seen, distinct repo-instance counts, and a representative
  sample designed to seed one MAINT-* task per cluster. Local mode
  (no sources) reads the primary repo's ``.task-state/handoff.db``
  via the git common dir — the same resolution as ``errors-record``;
  collect mode merges N handoff.db paths or export bundles with a
  ``(repo_instance_id, id)`` dedup so a DB and its own export never
  double-count.
- ``errors_export`` emits the rows as a JSONL-able list a consumer can
  hand the maintainer. Rows are already redacted at write time
  (implementation note), so export is a plain projection — no re-redaction pass.

Like the implementation note direct path, everything here runs outside the
configured runtime: DBs are opened read-only and only the
``agent_errors`` table is required (older companion tables are
irrelevant to a read), so a newer maintainer install can harvest an
older consumer DB.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

from .agent_errors import _resolve_primary_state_dir

_AGENT_ERROR_COLUMNS = (
    "id",
    "repo_instance_id",
    "task_ref",
    "harness",
    "error_class",
    "summary",
    "detail",
    "tool_name",
    "command_preview",
    "package_name",
    "package_version",
    "workbay_release",
    "occurrence_count",
    "created_at",
    "last_seen_at",
)

_SAMPLE_FIELDS = (
    "summary",
    "detail",
    "tool_name",
    "command_preview",
    "package_version",
    "task_ref",
    "harness",
)


def _rows_from_db(path: Path) -> list[dict]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        table = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'agent_errors'").fetchone()
        if table is None:
            raise ValueError(f"{path}: no agent_errors table (schema predates v12)")
        return [dict(row) for row in conn.execute("SELECT * FROM agent_errors ORDER BY created_at ASC, id ASC")]
    finally:
        conn.close()


def _rows_from_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise ValueError(f"{path}:{lineno}: invalid JSON line: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"{path}:{lineno}: expected a JSON object per line")
        rows.append(row)
    return rows


def _load_source_rows(source: Path) -> list[dict]:
    """Rows from one source: a handoff.db or an errors-export JSONL bundle."""
    if not source.is_file():
        raise ValueError(f"{source}: no such file")
    if source.suffix == ".jsonl":
        return _rows_from_jsonl(source)
    try:
        return _rows_from_db(source)
    except sqlite3.Error as exc:
        raise ValueError(f"{source}: not a readable SQLite database: {exc}") from exc


def collect_agent_error_rows(sources: list[Path], *, since: str | None = None) -> dict:
    """Merge rows across sources with ``(repo_instance_id, id)`` dedup.

    Timestamps in the ledger are ``datetime('now')`` strings, so the
    ``since`` filter (on ``last_seen_at``) is a plain string compare.
    """
    rows: list[dict] = []
    seen: set[tuple] = set()
    source_receipts: list[dict] = []
    for source in sources:
        source_rows = _load_source_rows(Path(source))
        kept = 0
        for row in source_rows:
            if since is not None and str(row.get("last_seen_at") or "") < since:
                continue
            key = (row.get("repo_instance_id"), row.get("id"))
            if row.get("id") is not None and key in seen:
                continue
            seen.add(key)
            rows.append(row)
            kept += 1
        source_receipts.append({"path": str(source), "rows": kept})
    return {"rows": rows, "sources": source_receipts}


_VERSION_SEGMENT_RE = re.compile(r"\d+|[A-Za-z]+")


def _version_sort_key(version: str) -> tuple:
    """Numeric-aware sort key so 0.10.0 orders after 0.9.0 (REV-B-002).

    Splits into digit runs (compared as ints) and letter runs (compared
    case-insensitively). Rank order at each position is
    letter-run (0) < terminator (1) < digit-run (2): the trailing
    terminator sorts a bare release above its own pre-releases
    (1.2.0rc1 < 1.2.0, PEP 440-ish; REV-D-001) while extra numeric
    segments still extend upward (0.9 < 0.9.1). The raw string is the
    final tiebreaker for determinism.
    """
    parts: list[tuple] = []
    for segment in _VERSION_SEGMENT_RE.findall(version):
        if segment.isdigit():
            parts.append((2, int(segment)))
        else:
            parts.append((0, segment.lower()))
    parts.append((1,))  # terminator
    return (tuple(parts), version)


def _version_range(versions: list[str]) -> list[str] | None:
    if not versions:
        return None
    ordered = sorted(set(versions), key=_version_sort_key)
    return [ordered[0], ordered[-1]]


def build_errors_report(rows: list[dict]) -> dict:
    """Cluster rows by ``(error_class, package_name, tool_name)``.

    ``tool_name`` is in the key because ``package_name`` is NULL on most
    server-captured rows (e.g. ``mcp_write_rejected``), which otherwise
    collapses many distinct rejection causes into one cluster with a
    single sample. Per cluster: row/occurrence counts, distinct repo
    instances, package-version range, first/last seen, and a
    representative sample — the row with the highest
    ``occurrence_count`` (latest ``last_seen_at`` breaks ties), i.e. the
    variant a MAINT-* task should reproduce first. Clusters sort
    busiest-first.
    """
    clusters: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (
            str(row.get("error_class") or "other"),
            row.get("package_name"),
            row.get("tool_name"),
        )
        clusters.setdefault(key, []).append(row)

    rendered = []
    for (error_class, package_name, tool_name), members in clusters.items():
        sample_row = max(
            members,
            key=lambda r: (int(r.get("occurrence_count") or 1), str(r.get("last_seen_at") or "")),
        )
        versions = [str(r["package_version"]) for r in members if r.get("package_version")]
        rendered.append(
            {
                "error_class": error_class,
                "package_name": package_name,
                "tool_name": tool_name,
                "package_version_range": _version_range(versions),
                "row_count": len(members),
                "occurrence_count": sum(int(r.get("occurrence_count") or 1) for r in members),
                "repo_instance_count": len({r.get("repo_instance_id") for r in members if r.get("repo_instance_id")}),
                "first_seen": min(str(r.get("created_at") or "") for r in members),
                "last_seen": max(str(r.get("last_seen_at") or "") for r in members),
                "sample": {field: sample_row.get(field) for field in _SAMPLE_FIELDS},
            }
        )
    rendered.sort(
        key=lambda c: (
            -c["occurrence_count"],
            c["error_class"],
            c["package_name"] or "",
            c["tool_name"] or "",
        )
    )

    return {
        "total_rows": len(rows),
        "total_occurrences": sum(int(r.get("occurrence_count") or 1) for r in rows),
        "clusters": rendered,
    }


def _resolve_local_db(cwd: Path | str | None) -> Path | dict:
    resolved_cwd = Path(cwd) if cwd is not None else Path.cwd()
    state_dir = _resolve_primary_state_dir(resolved_cwd)
    if state_dir is None:
        return {"ok": False, "error": "not inside a git repository; pass --source explicitly."}
    db_path = state_dir / "handoff.db"
    if not db_path.is_file():
        return {"ok": False, "error": f"{db_path}: no primary handoff.db to report on."}
    return db_path


def errors_report(
    sources: list[Path] | None = None,
    *,
    since: str | None = None,
    cwd: Path | str | None = None,
) -> dict:
    """Cluster agent_errors rows from the given sources (or the local DB)."""
    mode = "collect" if sources else "local"
    if not sources:
        local = _resolve_local_db(cwd)
        if isinstance(local, dict):
            return local
        sources = [local]
    try:
        collected = collect_agent_error_rows(list(sources), since=since)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    return {
        "ok": True,
        "mode": mode,
        "since": since,
        "sources": collected["sources"],
        "report": build_errors_report(collected["rows"]),
    }


def errors_export(
    *,
    db_path: Path | str | None = None,
    since: str | None = None,
    cwd: Path | str | None = None,
) -> dict:
    """Rows from one DB as an exportable list (already redacted at write)."""
    if db_path is None:
        local = _resolve_local_db(cwd)
        if isinstance(local, dict):
            return local
        db_path = local
    try:
        collected = collect_agent_error_rows([Path(db_path)], since=since)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    return {
        "ok": True,
        "since": since,
        "db_path": str(db_path),
        "rows": collected["rows"],
    }
