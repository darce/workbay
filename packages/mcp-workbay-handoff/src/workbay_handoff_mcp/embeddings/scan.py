"""Bounded scans over ``concept_embeddings`` and the embeddable source tables.

One primitive per direction, shared by the read and write paths so neither
holds the corpus in Python (finding 32527, [RES-04], [RES-05]):

* :func:`iter_concept_vector_chunks` streams same-model concept rows with
  ``cursor.fetchmany(chunk)``. The ``model_id`` predicate, the reclaim-candidate
  exclusion and the caller's membership/exclusion predicates are SQL, so rows
  of another model never reach Python. Each chunk resolves interned payloads
  with one chunked prefetch, checks compaction residual lengths with one
  batched lookup (unknown length stays oversized: fail-closed, BR-I7-01 /
  CARD-05) and decodes into one ``(n, dim)`` float32 matrix.
* :func:`iter_source_concepts` streams the embeddable source concepts one
  keyset page at a time, in the same order the former whole-corpus gather
  produced them, with ``kinds`` pushed into the SELECT (a table with no
  requested kind is never read; only requested text columns are selected).

Both check an optional ``threading.Event`` before every statement they issue
and raise :class:`ScanCancelled` once it is set, so an abandoned query stops
at the next chunk boundary instead of finishing the scan ([CON-03]).

Resident memory is O(chunk) rows (plus O(k) for a caller's top-k), never
O(corpus). Exact scoring stays exact: there is no approximate index here.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Collection, Iterator, Sequence
from dataclasses import dataclass

import numpy as np

DEFAULT_SCAN_CHUNK_ROWS: int = 1024
DEFAULT_SOURCE_BATCH_ROWS: int = 1024

# SQLite builds before 3.32 cap bound variables at 999; newer ones at 32,766.
_FALLBACK_MAX_SQL_VARIABLES = 999
# Headroom for the non-list parameters that share a statement with an IN list.
_SQL_VARIABLE_HEADROOM = 16

_COMPACTION_RESIDUAL_KIND = "compaction.prose_residual"
_DECISION_KINDS_SQL = "('decision.title', 'decision.rationale')"


class ScanCancelled(RuntimeError):
    """The caller's cancel token was set; the scan stopped at a chunk boundary.

    Raised only when a caller passed a cancel event and set it, i.e. when the
    request that wanted this result is already gone. Nothing should answer it.
    """


def check_cancelled(cancel: threading.Event | None) -> None:
    """Raise :class:`ScanCancelled` if ``cancel`` is set."""
    if cancel is not None and cancel.is_set():
        raise ScanCancelled("prior-work scan cancelled by its caller")


def max_in_list_items(conn: sqlite3.Connection, arity: int = 1) -> int:
    """Largest IN-list length (in ``arity``-tuples) one statement can bind on ``conn``."""
    getlimit = getattr(conn, "getlimit", None)
    limit = _FALLBACK_MAX_SQL_VARIABLES
    if callable(getlimit):
        try:
            limit = int(getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
        except (sqlite3.Error, AttributeError, TypeError, ValueError):
            limit = _FALLBACK_MAX_SQL_VARIABLES
    return max(1, (limit - _SQL_VARIABLE_HEADROOM) // max(1, arity))


def split_for_in_list(items: Sequence[object], size: int) -> Iterator[Sequence[object]]:
    """Yield consecutive slices of ``items`` of at most ``size`` entries."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def values_placeholders(count: int, arity: int) -> str:
    """``VALUES (?, ?), (?, ?)`` body for a row-value IN list."""
    row = "(" + ", ".join("?" for _ in range(arity)) + ")"
    return "VALUES " + ", ".join(row for _ in range(count))


# --- read side: concept vector chunks -------------------------------------


@dataclass(frozen=True)
class ConceptVectorChunk:
    """One decoded chunk: ``matrix[i]`` is the stored vector of ``keys[i]``.

    ``keys`` rows are ``(entity_kind, entity_id, task_ref, model_id)``.
    ``rows_seen`` counts every row the scan SQL returned for this chunk,
    including rows then skipped (oversized/unknown residual, missing interned
    owner, dimension mismatch), so callers can tell "no candidate rows" from
    "candidate rows, none scorable".
    """

    keys: tuple[tuple[str, str, str, str], ...]
    matrix: np.ndarray
    rows_seen: int


def concept_scan_from_sql(conn: sqlite3.Connection, *, joins_sql: str = "") -> str:
    """``FROM`` clause shared by the scan and its EXISTS probes.

    ``c`` is ``concept_embeddings``; ``d`` is the decision row behind a
    ``decision.*`` concept (NULL otherwise), used by the reclaim exclusion.
    ``joins_sql`` may add trusted joins (membership filters); never user text.
    """
    sql = (
        " FROM concept_embeddings c LEFT JOIN decisions d "
        f"ON c.entity_kind IN {_DECISION_KINDS_SQL} "
        "AND d.id = c.entity_id AND d.task_ref = c.task_ref"
    )
    if joins_sql:
        sql += " " + joins_sql
    return sql


def reclaim_exclusion_sql(conn: sqlite3.Connection) -> str:
    """SQL twin of the reclaim-candidate skip: decision rows must be eligible sources."""
    from . import store

    return f"(d.decision IS NULL OR {store.source_row_eligibility_sql(conn, 'decisions', alias='d')})"


def concept_rows_exist(
    conn: sqlite3.Connection,
    *,
    predicates: Sequence[str],
    params: Sequence[object],
    joins_sql: str = "",
    cancel: threading.Event | None = None,
) -> bool:
    """``EXISTS`` over the scan's FROM clause: evidence without fetching rows."""
    check_cancelled(cancel)
    sql = "SELECT EXISTS(SELECT 1" + concept_scan_from_sql(conn, joins_sql=joins_sql)
    if predicates:
        sql += " WHERE " + " AND ".join(predicates)
    sql += ")"
    row = conn.execute(sql, list(params)).fetchone()
    return bool(row[0]) if row is not None else False


def _residual_lengths(
    conn: sqlite3.Connection,
    compaction_ids: Sequence[str],
) -> dict[str, int | None]:
    """``compaction_id -> length(prose_residual)`` for the ids present in the table."""
    lengths: dict[str, int | None] = {}
    if not compaction_ids:
        return lengths
    unique = list(dict.fromkeys(compaction_ids))
    for part in split_for_in_list(unique, max_in_list_items(conn)):
        placeholders = ",".join("?" for _ in part)
        for compaction_id, length in conn.execute(
            f"SELECT compaction_id, length(prose_residual) FROM session_compactions WHERE compaction_id IN ({placeholders})",
            list(part),
        ).fetchall():
            lengths[str(compaction_id)] = None if length is None else int(length)
    return lengths


def iter_concept_vector_chunks(
    conn: sqlite3.Connection,
    *,
    model_id: str,
    dim: int,
    where_sql: Sequence[str] = (),
    params: Sequence[object] = (),
    joins_sql: str = "",
    chunk: int = DEFAULT_SCAN_CHUNK_ROWS,
    cancel: threading.Event | None = None,
    residual_max_chars: int | None = None,
    decode: Callable[[bytes, int | None], np.ndarray] | None = None,
) -> Iterator[ConceptVectorChunk]:
    """Stream same-model, reclaim-eligible concept rows as decoded chunks.

    ``where_sql`` are extra trusted predicates (``AND``-ed, aliases ``c``/``d``
    plus whatever ``joins_sql`` adds) with ``params`` bound in order after
    ``model_id``. ``residual_max_chars`` (when set) skips
    ``compaction.prose_residual`` rows whose source text is longer, missing or
    NULL. ``decode(blob, stored_dim)`` defaults to
    :func:`~workbay_handoff_mcp.embeddings.store.deserialize_vector`; a payload
    whose length matches neither encoding still raises, exactly as the
    unchunked loop did. Rows decoding to a length other than ``dim`` are
    skipped. Peak memory is one chunk of rows plus its ``(n, dim)`` matrix.
    """
    from . import store

    if chunk < 1:
        raise ValueError(f"chunk must be >= 1, got {chunk}")
    decoder = (
        decode if decode is not None else (lambda blob, stored_dim: store.deserialize_vector(blob, dim=stored_dim))
    )
    predicates = ["c.model_id = ?", reclaim_exclusion_sql(conn), *where_sql]
    sql = (
        "SELECT c.entity_kind, c.entity_id, c.task_ref, c.vector, c.model_id, c.text_hash, c.dim"
        + concept_scan_from_sql(conn, joins_sql=joins_sql)
        + " WHERE "
        + " AND ".join(predicates)
    )
    check_cancelled(cancel)
    cursor = conn.execute(sql, [model_id, *params])
    try:
        while True:
            check_cancelled(cancel)
            rows = cursor.fetchmany(chunk)
            if not rows:
                return
            residual_len: dict[str, int | None] = {}
            if residual_max_chars is not None:
                residual_ids = [str(r[1]) for r in rows if str(r[0]) == _COMPACTION_RESIDUAL_KIND]
                if residual_ids:
                    check_cancelled(cancel)
                    residual_len = _residual_lengths(conn, residual_ids)
            intern_keys = [(str(r[5]), str(r[4])) for r in rows if not bytes(r[3] or b"")]
            owned_cache: dict[tuple[str, str], bytes] = {}
            if intern_keys:
                check_cancelled(cancel)
                owned_cache = store.prefetch_owned_vector_blobs(conn, intern_keys)
            keys: list[tuple[str, str, str, str]] = []
            matrix = np.empty((len(rows), dim), dtype=np.float32)
            for row in rows:
                entity_kind = str(row[0])
                entity_id = str(row[1])
                if residual_max_chars is not None and entity_kind == _COMPACTION_RESIDUAL_KIND:
                    # BR-I7-01 / CARD-05: unproven length is oversized — do not rank.
                    length = residual_len.get(entity_id)
                    if length is None or length > residual_max_chars:
                        continue
                row_model_id = str(row[4])
                blob = store.resolve_vector_blob(conn, row[3], str(row[5]), row_model_id, owned_cache=owned_cache)
                if not blob:
                    continue
                vec = decoder(blob, None if row[6] is None else int(row[6]))
                if vec.shape[0] != dim:
                    continue
                matrix[len(keys)] = vec
                keys.append((entity_kind, entity_id, str(row[2]), row_model_id))
            yield ConceptVectorChunk(keys=tuple(keys), matrix=matrix[: len(keys)], rows_seen=len(rows))
    finally:
        cursor.close()


def score_chunk(matrix: np.ndarray, anchor: np.ndarray) -> list[float]:
    """Exact cosine of every chunk row against a unit ``anchor``.

    One float32 contraction per chunk. ``einsum`` (no BLAS dispatch) reduces
    each row with the same loop wherever it sits in the chunk, so byte-identical
    vectors always tie exactly and the ``(-score, kind, id)`` order is stable.
    """
    if matrix.shape[0] == 0:
        return []
    scores = np.einsum("ij,j->i", matrix, anchor.astype(np.float32, copy=False))
    return [float(s) for s in scores.tolist()]


# --- write side: source concept pages --------------------------------------

SourceConcept = tuple[str, str, str, str | None]


def _source_tables() -> list[tuple[str, str, list[tuple[str, str]]]]:
    """``[(table, id_column, [(entity_kind, text_column), ...]), ...]`` in gather order.

    Derived from :data:`~workbay_handoff_mcp.embeddings.store.CONCEPT_BACKFILL_SOURCES`
    so the row-identity contract has one source of truth.
    """
    from . import store

    tables: list[tuple[str, str, list[tuple[str, str]]]] = []
    for kind, table, key, column in store.CONCEPT_BACKFILL_SOURCES:
        if tables and tables[-1][0] == table:
            tables[-1][2].append((kind, column))
        else:
            tables.append((table, key, [(kind, column)]))
    return tables


def iter_source_concepts(
    conn: sqlite3.Connection,
    task_ref: str | None,
    *,
    kinds: Collection[str] | None = None,
    batch: int = DEFAULT_SOURCE_BATCH_ROWS,
    cancel: threading.Event | None = None,
) -> Iterator[list[SourceConcept]]:
    """Yield pages of ``(entity_kind, entity_id, task_ref, text)`` source concepts.

    Same concepts, same order and same entity ids as embed-on-write: row id
    for decisions/findings/blockers, task_ref for handoff_state, compaction_id
    for compactions; reclaim-candidate decisions are excluded through
    :func:`~workbay_handoff_mcp.embeddings.store.source_row_eligibility_sql`.

    Each page is one keyset SELECT (``rowid > last ORDER BY rowid LIMIT
    batch``) read to completion before it is yielded, so no statement stays
    open while the consumer writes between pages. Membership is the source
    rows present when iteration starts: before the first page, each scanned
    table's ``max(rowid)`` is read and pages stop there, so rows appended while
    a consumer works (e.g. a writer racing a backfill) wait for the next run,
    as they did with the former up-front gather. A row's text is read when its
    page is, which is why backfill revalidates hashes under ``BEGIN IMMEDIATE``. ``kinds`` (``None`` = all)
    is pushed into SQL: tables with no requested kind are skipped and only the
    requested text columns are selected. A page holds at most ``batch`` source
    rows.
    """
    from . import store

    if batch < 1:
        raise ValueError(f"batch must be >= 1, got {batch}")
    wanted = None if kinds is None else frozenset(kinds)
    plan: list[tuple[str, str, list[tuple[str, str]], int]] = []
    for table, key, table_kinds in _source_tables():
        selected = [(kind, column) for kind, column in table_kinds if wanted is None or kind in wanted]
        if not selected:
            continue
        check_cancelled(cancel)
        bound = conn.execute(f"SELECT max(rowid) FROM {table}").fetchone()
        if bound is None or bound[0] is None:
            continue
        plan.append((table, key, selected, int(bound[0])))
    for table, key, selected, max_rowid in plan:
        columns = ", ".join(column for _kind, column in selected)
        predicates: list[str] = []
        params: list[object] = []
        eligibility = store.source_row_eligibility_sql(conn, table)
        if eligibility != "1":
            predicates.append(eligibility)
        if task_ref:
            predicates.append("task_ref = ?")
            params.append(task_ref)
        predicates.append("rowid <= ?")
        params.append(max_rowid)
        last_rowid: int | None = None
        while True:
            check_cancelled(cancel)
            page_predicates = list(predicates)
            page_params = list(params)
            if last_rowid is not None:
                page_predicates.append("rowid > ?")
                page_params.append(last_rowid)
            sql = f"SELECT rowid, {key}, task_ref, {columns} FROM {table}"
            if page_predicates:
                sql += " WHERE " + " AND ".join(page_predicates)
            sql += " ORDER BY rowid LIMIT ?"
            page_params.append(batch)
            rows = conn.execute(sql, page_params).fetchall()
            if not rows:
                break
            page: list[SourceConcept] = []
            for row in rows:
                entity_id = str(row[1])
                row_task_ref = str(row[2])
                for offset, (kind, _column) in enumerate(selected):
                    page.append((kind, entity_id, row_task_ref, row[3 + offset]))
            last_rowid = int(rows[-1][0])
            yield page
            if len(rows) < batch:
                break
