"""SQLite-backed persistence for canvas-mcp Canvases.

Schema is owned by this module: one ``canvases`` row per canvas_ref,
plus child rows in ``nodes`` and ``edges`` keyed by ``(canvas_ref,
node_id)`` / ``(canvas_ref, edge_id)``. ``upsert_canvas`` is a full
replace: existing children for the canvas_ref are deleted and rewritten
from the supplied :class:`Canvas`. ``get_canvas`` returns ``None`` for
an unknown ref so callers can distinguish "empty canvas" from "no such
canvas".
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

from workbay_canvas_mcp.annotations import Annotation, AnnotationError
from workbay_canvas_mcp.component_roster import ComponentSpec
from workbay_canvas_mcp.design_document import DesignDecision, DesignDocument
from workbay_canvas_mcp.models import Canvas, CanvasSpec, Edge, EdgeKind, Node, NodeKind

_DDL = """
CREATE TABLE IF NOT EXISTS canvases (
    canvas_ref TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS nodes (
    canvas_ref  TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    kind        TEXT NOT NULL,
    label       TEXT NOT NULL,
    task_ref    TEXT,
    decision_id TEXT,
    finding_id  TEXT,
    lane_id     TEXT,
    x           REAL,
    y           REAL,
    PRIMARY KEY (canvas_ref, node_id),
    FOREIGN KEY (canvas_ref) REFERENCES canvases(canvas_ref) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS edges (
    canvas_ref TEXT NOT NULL,
    edge_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,
    source     TEXT NOT NULL,
    target     TEXT NOT NULL,
    label      TEXT,
    PRIMARY KEY (canvas_ref, edge_id),
    FOREIGN KEY (canvas_ref) REFERENCES canvases(canvas_ref) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id        TEXT PRIMARY KEY,
    canvas_ref           TEXT NOT NULL,
    node_id              TEXT NOT NULL,
    author               TEXT NOT NULL,
    body                 TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    parent_annotation_id TEXT,
    FOREIGN KEY (canvas_ref) REFERENCES canvases(canvas_ref) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_annotations_canvas_created
    ON annotations (canvas_ref, created_at, annotation_id);
CREATE INDEX IF NOT EXISTS idx_annotations_canvas_node_created
    ON annotations (canvas_ref, node_id, created_at, annotation_id);
CREATE TABLE IF NOT EXISTS component_roster (
    design_ref        TEXT NOT NULL,
    component_id      TEXT NOT NULL,
    name              TEXT NOT NULL,
    description       TEXT,
    props_schema      TEXT NOT NULL DEFAULT '{}',
    slots             TEXT NOT NULL DEFAULT '[]',
    token_refs        TEXT NOT NULL DEFAULT '[]',
    source_canvas_ref TEXT,
    source_node_ids   TEXT NOT NULL DEFAULT '[]',
    status            TEXT NOT NULL DEFAULT 'active',
    created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (design_ref, component_id)
);
CREATE TABLE IF NOT EXISTS design_documents (
    design_ref       TEXT PRIMARY KEY,
    title            TEXT,
    goals            TEXT NOT NULL DEFAULT '[]',
    audience         TEXT NOT NULL DEFAULT '[]',
    tokens           TEXT NOT NULL DEFAULT '{}',
    component_refs   TEXT NOT NULL DEFAULT '[]',
    layout_decisions TEXT NOT NULL DEFAULT '[]',
    scaffold_targets TEXT NOT NULL DEFAULT '[]',
    open_questions   TEXT NOT NULL DEFAULT '[]',
    not_doing        TEXT NOT NULL DEFAULT '[]',
    created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class CanvasStore:
    """SQLite-backed Canvas persistence keyed by ``canvas_ref``.

    Supports the context-manager protocol so callers can write::

        with CanvasStore(path) as store:
            store.upsert_canvas(canvas)
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False because FastMCP dispatches sync tool
        # handlers from a worker threadpool; the lock below serializes
        # access so SQLite still sees one writer at a time.
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_DDL)
        self._migrate_schema()
        self._conn.commit()

    def _migrate_schema(self) -> None:
        """Idempotently add columns that CREATE TABLE IF NOT EXISTS cannot.

        ``executescript(_DDL)`` only creates missing tables; it never
        alters an existing one. Tables created before a column was added
        need that column backfilled in place (additive, defaulting so
        existing rows stay valid).
        """

        node_cols = {row[1] for row in self._conn.execute("PRAGMA table_info(nodes)")}
        if "lane_id" not in node_cols:
            self._conn.execute("ALTER TABLE nodes ADD COLUMN lane_id TEXT")

        doc_cols = {row[1] for row in self._conn.execute("PRAGMA table_info(design_documents)")}
        if "not_doing" not in doc_cols:
            self._conn.execute("ALTER TABLE design_documents ADD COLUMN not_doing TEXT NOT NULL DEFAULT '[]'")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> CanvasStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def upsert_canvas(self, canvas: Canvas, *, expected_updated_at: str | None = None) -> Canvas:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            if expected_updated_at is not None:
                cur.execute(
                    "SELECT updated_at FROM canvases WHERE canvas_ref = ?",
                    (canvas.canvas_ref,),
                )
                row = cur.fetchone()
                if row is not None and row[0] != expected_updated_at:
                    from workbay_canvas_mcp.canvas_service import StaleDraftError

                    raise StaleDraftError(expected=expected_updated_at, actual=row[0])
            cur.execute(
                "INSERT INTO canvases (canvas_ref, created_at, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT (canvas_ref) DO UPDATE SET updated_at = excluded.updated_at",
                (canvas.canvas_ref, now, now),
            )
            cur.execute("DELETE FROM nodes WHERE canvas_ref = ?", (canvas.canvas_ref,))
            cur.execute("DELETE FROM edges WHERE canvas_ref = ?", (canvas.canvas_ref,))
            cur.executemany(
                "INSERT INTO nodes "
                "(canvas_ref, node_id, kind, label, task_ref, decision_id, finding_id, lane_id, x, y) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        canvas.canvas_ref,
                        n.id,
                        n.kind.value,
                        n.label,
                        n.task_ref,
                        n.decision_id,
                        n.finding_id,
                        n.lane_id,
                        n.x,
                        n.y,
                    )
                    for n in canvas.nodes
                ],
            )
            cur.executemany(
                "INSERT INTO edges (canvas_ref, edge_id, kind, source, target, label) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        canvas.canvas_ref,
                        e.id,
                        e.kind.value,
                        e.source,
                        e.target,
                        e.label,
                    )
                    for e in canvas.edges
                ],
            )
            self._conn.commit()
        # Echo the freshly written ``updated_at`` without reloading: a
        # ``get_canvas`` round-trip would re-sort nodes by id (its own
        # ``ORDER BY node_id``) and so silently reorder the returned Canvas
        # relative to the caller's insertion order — which downstream
        # consumers (e.g. ``template seed`` reading ``canvas.nodes``) rely on.
        return canvas.model_copy(update={"updated_at": now})

    def append_annotation(
        self,
        *,
        canvas_ref: str,
        node_id: str,
        author: str,
        body: str,
        parent_annotation_id: str | None = None,
    ) -> Annotation:
        annotation = Annotation(
            annotation_id=str(uuid.uuid4()),
            canvas_ref=canvas_ref,
            node_id=node_id,
            author=author,
            body=body,
            created_at=datetime.now(UTC).isoformat(),
            parent_annotation_id=parent_annotation_id,
        )
        with self._lock:
            if parent_annotation_id is not None:
                cur = self._conn.execute(
                    "SELECT 1 FROM annotations WHERE annotation_id = ? AND canvas_ref = ?",
                    (parent_annotation_id, canvas_ref),
                )
                if cur.fetchone() is None:
                    raise AnnotationError(
                        "parent_missing",
                        f"parent annotation {parent_annotation_id!r} not found on canvas {canvas_ref!r}",
                    )
            self._conn.execute(
                "INSERT INTO annotations "
                "(annotation_id, canvas_ref, node_id, author, body, created_at, parent_annotation_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    annotation.annotation_id,
                    annotation.canvas_ref,
                    annotation.node_id,
                    annotation.author,
                    annotation.body,
                    annotation.created_at,
                    annotation.parent_annotation_id,
                ),
            )
            self._conn.commit()
        return annotation

    def get_annotation(self, annotation_id: str) -> Annotation | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT annotation_id, canvas_ref, node_id, author, body, "
                "created_at, parent_annotation_id "
                "FROM annotations WHERE annotation_id = ?",
                (annotation_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return Annotation(
            annotation_id=row[0],
            canvas_ref=row[1],
            node_id=row[2],
            author=row[3],
            body=row[4],
            created_at=row[5],
            parent_annotation_id=row[6],
        )

    def list_annotations(
        self,
        *,
        canvas_ref: str,
        node_id: str | None = None,
        include_detached: bool = True,
    ) -> list[Annotation]:
        with self._lock:
            cur = self._conn.cursor()
            if node_id is None:
                cur.execute(
                    "SELECT annotation_id, canvas_ref, node_id, author, body, "
                    "created_at, parent_annotation_id "
                    "FROM annotations WHERE canvas_ref = ? "
                    "ORDER BY created_at, annotation_id",
                    (canvas_ref,),
                )
            else:
                cur.execute(
                    "SELECT annotation_id, canvas_ref, node_id, author, body, "
                    "created_at, parent_annotation_id "
                    "FROM annotations WHERE canvas_ref = ? AND node_id = ? "
                    "ORDER BY created_at, annotation_id",
                    (canvas_ref, node_id),
                )
            rows = cur.fetchall()

            if not include_detached and rows:
                cur.execute(
                    "SELECT node_id FROM nodes WHERE canvas_ref = ?",
                    (canvas_ref,),
                )
                live_node_ids = {r[0] for r in cur.fetchall()}
                rows = [r for r in rows if r[2] in live_node_ids]

        return [
            Annotation(
                annotation_id=r[0],
                canvas_ref=r[1],
                node_id=r[2],
                author=r[3],
                body=r[4],
                created_at=r[5],
                parent_annotation_id=r[6],
            )
            for r in rows
        ]

    def list_canvas_specs(self) -> list[CanvasSpec]:
        """Return a lightweight per-canvas summary for every stored canvas.

        One :class:`CanvasSpec` per ``canvases`` row, ordered by
        ``canvas_ref``, with ``node_count`` / ``edge_count`` computed via
        correlated subqueries so canvases with zero nodes or edges still
        appear (a plain join would drop them). ``created_at`` /
        ``updated_at`` are the verbatim TEXT timestamps from the row.
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT c.canvas_ref, "
                "(SELECT COUNT(*) FROM nodes n WHERE n.canvas_ref = c.canvas_ref), "
                "(SELECT COUNT(*) FROM edges e WHERE e.canvas_ref = c.canvas_ref), "
                "c.created_at, c.updated_at "
                "FROM canvases c ORDER BY c.canvas_ref"
            )
            rows = cur.fetchall()
        return [
            CanvasSpec(
                canvas_ref=row[0],
                node_count=row[1],
                edge_count=row[2],
                created_at=row[3],
                updated_at=row[4],
            )
            for row in rows
        ]

    def upsert_component(self, *, design_ref: str, component: ComponentSpec) -> None:
        """Insert or replace a roster component keyed by ``(design_ref, component_id)``."""

        with self._lock:
            self._conn.execute(
                "INSERT INTO component_roster "
                "(design_ref, component_id, name, description, props_schema, slots, "
                " token_refs, source_canvas_ref, source_node_ids, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (design_ref, component_id) DO UPDATE SET "
                "  name = excluded.name, description = excluded.description, "
                "  props_schema = excluded.props_schema, slots = excluded.slots, "
                "  token_refs = excluded.token_refs, "
                "  source_canvas_ref = excluded.source_canvas_ref, "
                "  source_node_ids = excluded.source_node_ids, "
                "  status = excluded.status, updated_at = CURRENT_TIMESTAMP",
                (
                    design_ref,
                    component.component_id,
                    component.name,
                    component.description,
                    json.dumps(component.props_schema),
                    json.dumps([slot.model_dump() for slot in component.slots]),
                    json.dumps(component.token_refs),
                    component.source_canvas_ref,
                    json.dumps(component.source_node_ids),
                    component.status,
                ),
            )
            self._conn.commit()

    def archive_component(self, *, design_ref: str, component_id: str) -> None:
        """Flip a component's ``status`` to ``"archived"``; retain the row.

        Raises ``ValueError`` when no component matches ``(design_ref,
        component_id)``.
        """

        with self._lock:
            cur = self._conn.execute(
                "UPDATE component_roster SET status = 'archived', "
                "updated_at = CURRENT_TIMESTAMP "
                "WHERE design_ref = ? AND component_id = ?",
                (design_ref, component_id),
            )
            self._conn.commit()
            if cur.rowcount == 0:
                raise ValueError(f"unknown component_id {component_id!r} for design_ref {design_ref!r}")

    def list_components(self, *, design_ref: str) -> list[ComponentSpec]:
        """Return every roster component for ``design_ref`` ordered by id."""

        with self._lock:
            cur = self._conn.execute(
                "SELECT component_id, name, description, props_schema, slots, "
                "token_refs, source_canvas_ref, source_node_ids, status "
                "FROM component_roster WHERE design_ref = ? ORDER BY component_id",
                (design_ref,),
            )
            rows = cur.fetchall()
        return [
            ComponentSpec(
                component_id=row[0],
                name=row[1],
                description=row[2],
                props_schema=json.loads(row[3]),
                slots=json.loads(row[4]),
                token_refs=json.loads(row[5]),
                source_canvas_ref=row[6],
                source_node_ids=json.loads(row[7]),
                status=row[8],
            )
            for row in rows
        ]

    def upsert_design_document(self, document: DesignDocument) -> DesignDocument:
        """Full-replace persist of a design document keyed by ``design_ref``."""

        with self._lock:
            self._conn.execute(
                "INSERT INTO design_documents "
                "(design_ref, title, goals, audience, tokens, component_refs, "
                " layout_decisions, scaffold_targets, open_questions, not_doing) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (design_ref) DO UPDATE SET "
                "  title = excluded.title, goals = excluded.goals, "
                "  audience = excluded.audience, tokens = excluded.tokens, "
                "  component_refs = excluded.component_refs, "
                "  layout_decisions = excluded.layout_decisions, "
                "  scaffold_targets = excluded.scaffold_targets, "
                "  open_questions = excluded.open_questions, "
                "  not_doing = excluded.not_doing, "
                "  updated_at = CURRENT_TIMESTAMP",
                (
                    document.design_ref,
                    document.title,
                    json.dumps(document.goals),
                    json.dumps(document.audience),
                    json.dumps(document.tokens),
                    json.dumps(document.component_refs),
                    json.dumps([d.model_dump() for d in document.layout_decisions]),
                    json.dumps(document.scaffold_targets),
                    json.dumps(document.open_questions),
                    json.dumps(document.not_doing),
                ),
            )
            self._conn.commit()
        return document

    def get_design_document(self, *, design_ref: str) -> DesignDocument | None:
        """Return the design document for ``design_ref``, or ``None`` if absent."""

        with self._lock:
            cur = self._conn.execute(
                "SELECT title, goals, audience, tokens, component_refs, "
                "layout_decisions, scaffold_targets, open_questions, not_doing "
                "FROM design_documents WHERE design_ref = ?",
                (design_ref,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return DesignDocument(
            design_ref=design_ref,
            title=row[0],
            goals=json.loads(row[1]),
            audience=json.loads(row[2]),
            tokens=json.loads(row[3]),
            component_refs=json.loads(row[4]),
            layout_decisions=[DesignDecision(**d) for d in json.loads(row[5])],
            scaffold_targets=json.loads(row[6]),
            open_questions=json.loads(row[7]),
            not_doing=json.loads(row[8]),
        )

    def get_canvas(self, canvas_ref: str) -> Canvas | None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT canvas_ref, updated_at FROM canvases WHERE canvas_ref = ?",
                (canvas_ref,),
            )
            canvas_row = cur.fetchone()
            if canvas_row is None:
                return None
            cur.execute(
                "SELECT node_id, kind, label, task_ref, decision_id, finding_id, lane_id, x, y "
                "FROM nodes WHERE canvas_ref = ? ORDER BY node_id",
                (canvas_ref,),
            )
            node_rows = cur.fetchall()
            cur.execute(
                "SELECT edge_id, kind, source, target, label FROM edges WHERE canvas_ref = ? ORDER BY edge_id",
                (canvas_ref,),
            )
            edge_rows = cur.fetchall()

        nodes = [
            Node(
                id=row[0],
                kind=NodeKind(row[1]),
                label=row[2],
                task_ref=row[3],
                decision_id=row[4],
                finding_id=row[5],
                lane_id=row[6],
                x=row[7],
                y=row[8],
            )
            for row in node_rows
        ]
        edges = [
            Edge(
                id=row[0],
                kind=EdgeKind(row[1]),
                source=row[2],
                target=row[3],
                label=row[4],
            )
            for row in edge_rows
        ]
        return Canvas(canvas_ref=canvas_ref, nodes=nodes, edges=edges, updated_at=canvas_row[1])
