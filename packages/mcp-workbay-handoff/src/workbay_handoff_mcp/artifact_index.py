"""Sidecar artifact indexing for mcp-workbay-handoff.

Maintains a separate SQLite/FTS5 database (.task-state/mcp-artifacts.db) so
large evidence blobs can be stored and later retrieved by scoped full-text
search without polluting the handoff snapshot or prompt context.
"""

from __future__ import annotations

import hashlib
import json as _json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import TypedDict, cast

_FTS5_AVAILABLE: bool | None = None


class ArtifactChunk(TypedDict):
    chunk_order: int
    title: str
    body: str


class ArtifactSource(TypedDict, total=False):
    """Typed shape of an artifact source record (returned by get_artifact_source / list_artifact_sources)."""

    id: int
    task_ref: str
    lane_id: str | None
    app_root: str | None
    source_kind: str
    source_label: str
    content_type: str
    content_hash: str
    metadata_json: str | None
    summary: str | None
    created_at: str
    updated_at: str
    # Fields added by get_artifact_source (not present in list_artifact_sources rows)
    metadata: dict | None
    chunk_count: int
    chunks: list[ArtifactChunk]


class ArtifactSearchResult(TypedDict):
    """Typed shape of a single FTS search hit (returned by search_artifacts)."""

    source_id: int
    source_label: str
    source_summary: str
    task_ref: str
    lane_id: str | None
    app_root: str | None
    source_kind: str
    content_type: str
    title: str
    snippet: str
    rank: float


class ArtifactUpsertResult(TypedDict):
    """Typed shape of the result returned by upsert_source."""

    source_id: int
    source_label: str
    was_updated: bool
    chunk_count: int


ARTIFACT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS artifact_sources (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_ref      TEXT NOT NULL,
    lane_id       TEXT,
    app_root      TEXT,
    source_kind   TEXT NOT NULL,
    source_label  TEXT NOT NULL,
    content_type  TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    metadata_json TEXT,
    summary       TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(task_ref, lane_id, source_kind, source_label)
);

CREATE VIRTUAL TABLE IF NOT EXISTS artifact_chunks_fts USING fts5(
    title,
    body,
    source_id    UNINDEXED,
    task_ref     UNINDEXED,
    lane_id      UNINDEXED,
    app_root     UNINDEXED,
    source_kind  UNINDEXED,
    content_type UNINDEXED,
    tokenize='porter unicode61'
);
"""


def check_fts5_available(conn: sqlite3.Connection) -> bool:
    """Return True if the connected SQLite build supports FTS5."""
    global _FTS5_AVAILABLE
    if _FTS5_AVAILABLE is not None:
        return _FTS5_AVAILABLE
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _fts5_probe USING fts5(body)")
        conn.execute("DROP TABLE IF EXISTS _fts5_probe")
        _FTS5_AVAILABLE = True
    except sqlite3.OperationalError:
        _FTS5_AVAILABLE = False
    return _FTS5_AVAILABLE


def _artifact_schema_bootstrapped(conn: sqlite3.Connection) -> bool:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?)",
        ("artifact_sources", "artifact_chunks_fts"),
    ).fetchall()
    return {str(row["name"]) for row in rows} == {"artifact_sources", "artifact_chunks_fts"}


def get_artifact_db_connection(artifact_db_path: Path) -> sqlite3.Connection:
    """Open (or create) the sidecar artifact database, apply the schema, and return the connection.

    Raises RuntimeError if the local SQLite build does not support FTS5.
    """
    artifact_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(artifact_db_path))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        if not check_fts5_available(conn):
            raise RuntimeError(
                "SQLite FTS5 extension is not available on this system. "
                "mcp-workbay-handoff artifact indexing requires FTS5. "
                "Rebuild SQLite with SQLITE_ENABLE_FTS5 or use a Python distribution "
                "that bundles FTS5 (e.g. system Python on macOS 10.15+ or major Linux distros)."
            )
        if not _artifact_schema_bootstrapped(conn):
            conn.executescript(ARTIFACT_SCHEMA_SQL)
    except Exception:
        conn.close()
        raise
    return conn


# ---------------------------------------------------------------------------
# Chunkers
# ---------------------------------------------------------------------------

_FTS5_SPECIAL_RE = re.compile(r"[^\w\s]|[\x00-\x1f\x7f]", re.UNICODE)


def _build_fts5_match_query(queries: list[str]) -> str | None:
    """Turn a list of query strings into a single FTS5 MATCH expression.

    Each query string is sanitised and treated as an AND of its individual
    words. Multiple queries are OR-joined so any match wins.
    Words within a single query must all appear in the same chunk (FTS5 AND).
    """
    groups: list[str] = []
    for q in queries:
        # Strip FTS5 metacharacters so callers don't need FTS5 syntax knowledge
        cleaned = _FTS5_SPECIAL_RE.sub(" ", q).strip()
        if cleaned:
            terms = ['"' + term.replace('"', '""') + '"' for term in cleaned.split() if term]
            if terms:
                groups.append(" ".join(terms))
    if not groups:
        return None
    if len(groups) == 1:
        return groups[0]
    return " OR ".join(f"({group})" for group in groups)


_HEADING_RE = re.compile(r"^(#{1,3})\s+(.+)$", re.MULTILINE)


def chunk_markdown(content: str, source_label: str) -> list[tuple[str, str]]:
    """Split markdown text into (title, body) tuples on H1-H3 headings."""
    positions = [(m.start(), m.group(2).strip()) for m in _HEADING_RE.finditer(content)]
    if not positions:
        stripped = content.strip()
        return [(source_label, stripped)] if stripped else []

    chunks: list[tuple[str, str]] = []

    # Content before the first heading
    first_start = positions[0][0]
    if first_start > 0:
        preamble = content[:first_start].strip()
        if preamble:
            chunks.append((source_label, preamble))

    for i, (start, title) in enumerate(positions):
        end = positions[i + 1][0] if i + 1 < len(positions) else len(content)
        body = content[start:end].strip()
        if body:
            chunks.append((title, body))

    return chunks if chunks else [(source_label, content.strip())]


def chunk_plaintext(content: str, source_label: str, lines_per_chunk: int = 50) -> list[tuple[str, str]]:
    """Split plaintext into (title, body) chunks of up to *lines_per_chunk* lines."""
    lines = content.splitlines()
    if not lines:
        return []

    total_parts = max(1, (len(lines) + lines_per_chunk - 1) // lines_per_chunk)
    chunks: list[tuple[str, str]] = []
    for idx in range(0, len(lines), lines_per_chunk):
        group = lines[idx : idx + lines_per_chunk]
        body = "\n".join(group).strip()
        if not body:
            continue
        part_num = idx // lines_per_chunk + 1
        title = f"{source_label} (part {part_num}/{total_parts})" if total_parts > 1 else source_label
        chunks.append((title, body))

    return chunks


def chunk_json(content: str, source_label: str) -> list[tuple[str, str]]:
    """Split JSON into chunks: one per top-level dict key or list item."""
    try:
        data = _json.loads(content)
    except (ValueError, TypeError):
        return chunk_plaintext(content, source_label)

    chunks: list[tuple[str, str]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            body = _json.dumps(value, indent=2, ensure_ascii=False)
            if body:
                chunks.append((f"{source_label}.{key}", body))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            body = _json.dumps(item, indent=2, ensure_ascii=False)
            if body:
                chunks.append((f"{source_label}[{i}]", body))
    else:
        stripped = content.strip()
        if stripped:
            chunks.append((source_label, stripped))

    return chunks if chunks else chunk_plaintext(content, source_label)


def chunk_content(content: str, content_type: str, source_label: str) -> list[tuple[str, str]]:
    """Dispatch to the appropriate chunker based on *content_type*."""
    ct = (content_type or "").lower()
    if "markdown" in ct or ct in ("text/md", "md", "text/markdown"):
        return chunk_markdown(content, source_label)
    if "json" in ct:
        return chunk_json(content, source_label)
    return chunk_plaintext(content, source_label)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Term extraction
# ---------------------------------------------------------------------------

_STOPWORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "up",
        "about",
        "into",
        "through",
        "during",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "must",
        "shall",
        "can",
        "this",
        "that",
        "these",
        "those",
        "its",
        "he",
        "she",
        "they",
        "we",
        "you",
        "me",
        "him",
        "her",
        "us",
        "them",
        "what",
        "which",
        "who",
        "when",
        "where",
        "why",
        "how",
        "all",
        "each",
        "every",
        "both",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "not",
        "only",
        "same",
        "so",
        "than",
        "too",
        "very",
        "as",
        "if",
        "then",
        "else",
        "while",
        "after",
        "before",
        "since",
        "because",
        "return",
        "def",
        "class",
        "import",
        "none",
        "true",
        "false",
        "self",
        "test",
        "var",
        "let",
        "const",
        "get",
        "set",
    }
)

_WORD_RE: re.Pattern[str] = re.compile(r"\b[a-zA-Z][a-zA-Z0-9_-]{2,}\b")


def get_distinctive_terms(
    *,
    source_id: int,
    artifact_db_path: Path,
    top_n: int = 10,
) -> list[str]:
    """Return the most distinctive terms from an indexed artifact source.

    Extracts top-N words by frequency from the source's FTS5 chunks, after
    filtering stopwords and short tokens.  Useful as suggested retrieval
    queries for freshly indexed artifacts.
    """
    conn = get_artifact_db_connection(artifact_db_path)
    try:
        rows = conn.execute(
            "SELECT title, body FROM artifact_chunks_fts WHERE source_id = ? ORDER BY rowid",
            [str(source_id)],
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    combined = " ".join(f"{r['title']} {r['body']}" for r in rows)
    words = [w.lower() for w in _WORD_RE.findall(combined) if w.lower() not in _STOPWORDS]
    counts = Counter(words)
    return [term for term, _ in counts.most_common(top_n)]


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


def upsert_source(
    *,
    task_ref: str,
    lane_id: str | None,
    app_root: str | None,
    source_kind: str,
    source_label: str,
    content_type: str,
    summary: str | None,
    content: str,
    metadata: dict | None = None,
    artifact_db_path: Path,
) -> ArtifactUpsertResult:
    """Insert or replace an artifact source and re-index its FTS5 chunks.

    If the content hash matches an existing source, the source and its chunks
    are left unchanged (dedupe-on-reindex). Returns a result dict with
    ``source_id``, ``source_label``, ``was_updated``, and ``chunk_count``.
    """
    conn = get_artifact_db_connection(artifact_db_path)
    try:
        with conn:
            new_hash = _content_hash(content)
            existing = conn.execute(
                """
                SELECT id, content_hash FROM artifact_sources
                WHERE task_ref = ? AND lane_id IS ? AND source_kind = ? AND source_label = ?
                """,
                [task_ref, lane_id, source_kind, source_label],
            ).fetchone()

            if existing and existing["content_hash"] == new_hash:
                chunk_count = conn.execute(
                    "SELECT COUNT(*) FROM artifact_chunks_fts WHERE source_id = ?",
                    [str(existing["id"])],
                ).fetchone()[0]
                result: ArtifactUpsertResult = {
                    "source_id": existing["id"],
                    "source_label": source_label,
                    "was_updated": False,
                    "chunk_count": chunk_count,
                }
            else:
                metadata_json = _json.dumps(metadata) if metadata else None
                if existing:
                    conn.execute(
                        """
                        UPDATE artifact_sources
                        SET content_type = ?, content_hash = ?, metadata_json = ?,
                            summary = ?, updated_at = datetime('now')
                        WHERE id = ?
                        """,
                        [
                            content_type,
                            new_hash,
                            metadata_json,
                            summary,
                            existing["id"],
                        ],
                    )
                    source_id: int = existing["id"]
                else:
                    source_id = cast(
                        int,
                        conn.execute(
                            """
                        INSERT INTO artifact_sources
                            (task_ref, lane_id, app_root, source_kind, source_label,
                             content_type, content_hash, metadata_json, summary)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            [
                                task_ref,
                                lane_id,
                                app_root,
                                source_kind,
                                source_label,
                                content_type,
                                new_hash,
                                metadata_json,
                                summary,
                            ],
                        ).lastrowid,
                    )

                # Delete stale FTS chunks and rebuild
                conn.execute(
                    "DELETE FROM artifact_chunks_fts WHERE source_id = ?",
                    [str(source_id)],
                )
                chunks = chunk_content(content, content_type, source_label)
                conn.executemany(
                    """
                    INSERT INTO artifact_chunks_fts
                        (title, body, source_id, task_ref, lane_id, app_root,
                         source_kind, content_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            title,
                            body,
                            str(source_id),
                            task_ref,
                            lane_id or "",
                            app_root or "",
                            source_kind,
                            content_type,
                        )
                        for title, body in chunks
                    ],
                )
                result = {
                    "source_id": source_id,
                    "source_label": source_label,
                    "was_updated": True,
                    "chunk_count": len(chunks),
                }
    finally:
        conn.close()

    return result


def search_artifacts(
    *,
    queries: list[str],
    task_ref: str | None = None,
    lane_id: str | None = None,
    app_root: str | None = None,
    source_kind: str | None = None,
    content_type: str | None = None,
    limit: int = 10,
    artifact_db_path: Path,
) -> list[ArtifactSearchResult]:
    """Search artifact chunks by relevance with optional scope filters.

    Returns a list of hit dicts containing source metadata, chunk title,
    BM25 rank, and a compact highlighted snippet.
    """
    sanitized = [q.replace('"', '""').strip() for q in queries if q and q.strip()]
    if not sanitized:
        return []

    match_query = _build_fts5_match_query(queries)
    if match_query is None:
        return []

    conn = get_artifact_db_connection(artifact_db_path)
    try:
        extra_filters: list[str] = []
        params: list[object] = [match_query]

        if task_ref:
            extra_filters.append("task_ref = ?")
            params.append(task_ref)
        if lane_id:
            extra_filters.append("lane_id = ?")
            params.append(lane_id)
        if app_root:
            extra_filters.append("app_root = ?")
            params.append(app_root)
        if source_kind:
            extra_filters.append("source_kind = ?")
            params.append(source_kind)
        if content_type:
            extra_filters.append("content_type = ?")
            params.append(content_type)

        where_parts = ["artifact_chunks_fts MATCH ?"]
        where_parts.extend(extra_filters)
        where_clause = " AND ".join(where_parts)

        params.append(limit)
        fts_sql = f"""
            SELECT source_id, task_ref, lane_id, app_root, source_kind, content_type, title,
                   snippet(artifact_chunks_fts, 1, '**', '**', '...', 20) AS snippet,
                   rank
            FROM artifact_chunks_fts
            WHERE {where_clause}
            ORDER BY rank
            LIMIT ?
        """
        rows = conn.execute(fts_sql, params).fetchall()
        if not rows:
            return []

        # Enrich with source_label and summary from the metadata table
        source_ids = list({r["source_id"] for r in rows})
        placeholders = ",".join("?" * len(source_ids))
        source_map: dict[str, dict[str, str | None]] = {
            str(r["id"]): {"source_label": r["source_label"], "summary": r["summary"]}
            for r in conn.execute(
                f"SELECT id, source_label, summary FROM artifact_sources WHERE id IN ({placeholders})",
                [int(sid) for sid in source_ids],
            ).fetchall()
        }

        results: list[ArtifactSearchResult] = []
        for r in rows:
            meta = source_map.get(r["source_id"], {})
            results.append(
                {
                    "source_id": int(r["source_id"]),
                    "source_label": meta.get("source_label") or "",
                    "source_summary": meta.get("summary") or "",
                    "task_ref": r["task_ref"],
                    "lane_id": r["lane_id"] or None,
                    "app_root": r["app_root"] or None,
                    "source_kind": r["source_kind"],
                    "content_type": r["content_type"],
                    "title": r["title"],
                    "snippet": r["snippet"],
                    "rank": r["rank"],
                }
            )
        return results
    finally:
        conn.close()


def get_artifact_source(
    *,
    source_id: int | None = None,
    task_ref: str | None = None,
    source_label: str | None = None,
    artifact_db_path: Path,
) -> ArtifactSource | None:
    """Return the full artifact source record, or None if not found.

    Lookup priority: *source_id* > (*task_ref* + *source_label*).
    """
    conn = get_artifact_db_connection(artifact_db_path)
    try:
        if source_id is not None:
            row = conn.execute("SELECT * FROM artifact_sources WHERE id = ?", [source_id]).fetchone()
        elif task_ref and source_label:
            row = conn.execute(
                """
                SELECT * FROM artifact_sources
                WHERE task_ref = ? AND source_label = ?
                LIMIT 1
                """,
                [task_ref, source_label],
            ).fetchone()
        else:
            return None

        if row is None:
            return None

        result = cast(ArtifactSource, dict(row))
        metadata_json = result.get("metadata_json")
        if isinstance(metadata_json, str) and metadata_json:
            try:
                result["metadata"] = _json.loads(metadata_json)
            except (ValueError, TypeError):
                result["metadata"] = None
        else:
            result["metadata"] = None

        chunk_rows = conn.execute(
            "SELECT rowid, title, body FROM artifact_chunks_fts WHERE source_id = ? ORDER BY rowid",
            [str(result["id"])],
        ).fetchall()
        result["chunk_count"] = len(chunk_rows)
        result["chunks"] = [
            {"chunk_order": i + 1, "title": row["title"], "body": row["body"]} for i, row in enumerate(chunk_rows)
        ]
        return result
    finally:
        conn.close()


def list_artifact_sources(
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
    app_root: str | None = None,
    source_kind: str | None = None,
    limit: int = 50,
    offset: int = 0,
    artifact_db_path: Path,
) -> list[ArtifactSource]:
    """Return a paginated list of artifact sources matching the given filters."""
    conn = get_artifact_db_connection(artifact_db_path)
    try:
        conditions: list[str] = []
        params: list[object] = []
        if task_ref:
            conditions.append("task_ref = ?")
            params.append(task_ref)
        if lane_id:
            conditions.append("lane_id = ?")
            params.append(lane_id)
        if app_root:
            conditions.append("app_root = ?")
            params.append(app_root)
        if source_kind:
            conditions.append("source_kind = ?")
            params.append(source_kind)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params += [limit, offset]
        rows = conn.execute(
            f"SELECT * FROM artifact_sources {where} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()
        return [cast(ArtifactSource, dict(r)) for r in rows]
    finally:
        conn.close()


def purge_artifacts(
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
    app_root: str | None = None,
    older_than_days: int | None = None,
    artifact_db_path: Path,
) -> dict:
    """Delete artifact sources and their FTS chunks.

    *task_ref*: delete all sources for that task.
    *lane_id*: delete all sources for that lane (e.g. after lane closure).
    *app_root*: delete all sources for that app root.
    *older_than_days*: delete sources whose ``updated_at`` is older than N days.
    Conditions are ANDed when multiple are provided. At least one must be given.
    """
    conditions: list[str] = []
    params: list[object] = []
    if task_ref:
        conditions.append("task_ref = ?")
        params.append(task_ref)
    if lane_id:
        conditions.append("lane_id = ?")
        params.append(lane_id)
    if app_root:
        conditions.append("app_root = ?")
        params.append(app_root)
    if older_than_days is not None:
        conditions.append("updated_at < datetime('now', ?)")
        params.append(f"-{older_than_days} days")
    if not conditions:
        return {"purged_sources": 0, "ok": True}

    conn = get_artifact_db_connection(artifact_db_path)
    try:
        with conn:
            where = "WHERE " + " AND ".join(conditions)
            ids = [str(r[0]) for r in conn.execute(f"SELECT id FROM artifact_sources {where}", params).fetchall()]
            if not ids:
                return {"purged_sources": 0, "ok": True}

            id_placeholders = ",".join("?" * len(ids))
            conn.execute(
                f"DELETE FROM artifact_chunks_fts WHERE source_id IN ({id_placeholders})",
                ids,
            )
            conn.execute(
                f"DELETE FROM artifact_sources WHERE id IN ({id_placeholders})",
                [int(i) for i in ids],
            )
    finally:
        conn.close()

    return {"purged_sources": len(ids), "ok": True}


def maybe_record_artifact(
    *,
    task_ref: str,
    lane_id: str | None,
    app_root: str | None,
    source_kind: str,
    source_label: str,
    content: str,
    content_type: str,
    summary: str | None,
    artifact_db_path: Path,
    min_bytes: int = 4096,
    min_lines: int = 80,
) -> ArtifactUpsertResult | None:
    """Index *content* only when it meets the configured size thresholds.

    Returns the upsert result dict if indexed, or None if below threshold.
    """
    if len(content.encode("utf-8")) < min_bytes and content.count("\n") < min_lines:
        return None

    return upsert_source(
        task_ref=task_ref,
        lane_id=lane_id,
        app_root=app_root,
        source_kind=source_kind,
        source_label=source_label,
        content_type=content_type,
        summary=summary,
        content=content,
        artifact_db_path=artifact_db_path,
    )
