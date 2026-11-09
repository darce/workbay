"""Detection logic for ``reconcile_canvas`` (internal).

``reconcile_canvas_pure`` walks a :class:`Canvas` and classifies every
node's handoff/orchestrator reference (``task_ref``, ``decision_id``,
``finding_id``, ``lane_id``) into a :class:`DriftKind` against the live
state surfaced by four injectable reader callables. Detection is pure:
no I/O, no global state, deterministic for a given (canvas, reader
return values) tuple.

The production server wires :func:`reconcile_canvas_pure` to readers
that consult ``workbay-handoff-mcp`` (``list_handoff_rows``,
``archive(operation="get", ...)``, ``search_handoff``) and
``workbay-orchestrator-mcp`` (``manage_worktree_lane(operation="list",
...)``). Reader failures degrade to :data:`DriftKind.AMBIGUOUS` for the
affected entry and an unhealthy :class:`ReaderHealth` row in the
report's :class:`SourceSnapshot` — the report never raises on a reader
fault, so a partial-outage probe still surfaces actionable verdicts for
the ref kinds that did resolve.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from workbay_canvas_mcp.models import Canvas, NodeKind


class DriftKind(str, Enum):
    PRESENT = "present"
    ARCHIVED = "archived"
    RENAMED = "renamed"
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"


RefKind = Literal["task_ref", "decision_id", "finding_id", "lane_id"]


TaskRefStatus = Literal["present", "archived", "missing"]
"""Verdict the task-ref reader returns for a single ``task_ref``.

The production adapter checks live ``handoff_state`` rows first; on a
miss it probes ``archive(operation="get", ...)`` and treats a payload
as ``"archived"`` and a not-found as ``"missing"``.
"""


class RefLookupResult(BaseModel):
    """Search outcome for a single ``decision_id`` or ``finding_id``.

    ``exact`` is True when an id-equal match exists in the live index.
    ``candidates`` carries prefix-matched ids (used to suggest renames
    when ``exact`` is False).
    """

    exact: bool
    candidates: list[str] = Field(default_factory=list)


TaskRefReader = Callable[[str], TaskRefStatus]
RefLookupReader = Callable[[str, str | None], RefLookupResult]
"""``ref_lookup(ref_value, task_ref)`` for decision/finding ids.

``task_ref`` is the node-local scope when the Canvas node carries one.
Readers may use ``None`` as an unscoped lookup for freeform nodes.
"""
LaneLookupReader = Callable[[str, str], bool]
"""``lane_lookup(task_ref, lane_id) -> True`` when the lane is live."""


class ReconciliationEntry(BaseModel):
    """One drift verdict for one (node, ref kind) pair."""

    node_id: str
    ref_kind: RefKind
    ref_value: str
    drift: DriftKind
    suggested: str | None = None
    candidates: list[str] = Field(default_factory=list)


class ReaderHealth(BaseModel):
    """Aggregate health for one reader across the whole report.

    ``ok`` is the AND of every call's success. ``detail`` carries the
    most recent failure message (when any call failed) or stays None.
    """

    name: str
    ok: bool
    detail: str | None = None


class SourceSnapshot(BaseModel):
    """Per-report metadata: when it was generated and which readers ran."""

    generated_at: str
    readers: list[ReaderHealth] = Field(default_factory=list)


class ReconciliationReport(BaseModel):
    """Full drift report for a Canvas."""

    canvas_ref: str
    entries: list[ReconciliationEntry] = Field(default_factory=list)
    source_snapshot: SourceSnapshot


def _derive_lane_id(node_lane_id: str | None, node_id: str, node_kind: NodeKind) -> str | None:
    """Return the lane id to check for this node.

    Pre-internal LANE nodes carry the lane in the ``lane:<id>`` prefix
    on ``Node.id`` rather than the additive ``Node.lane_id`` field.
    """

    if node_lane_id is not None:
        return node_lane_id
    if node_kind is NodeKind.LANE and node_id.startswith("lane:"):
        candidate = node_id.split(":", 1)[1]
        return candidate or None
    return None


class _ReaderTracker:
    """Mutable aggregator for :class:`ReaderHealth` rows."""

    def __init__(self) -> None:
        self._rows: dict[str, ReaderHealth] = {}

    def mark(self, name: str, *, ok: bool, detail: str | None = None) -> None:
        existing = self._rows.get(name)
        if existing is None:
            self._rows[name] = ReaderHealth(name=name, ok=ok, detail=detail)
            return
        # AND the success bit; preserve the first failure detail.
        new_ok = existing.ok and ok
        new_detail = existing.detail if existing.detail is not None else detail
        self._rows[name] = ReaderHealth(name=name, ok=new_ok, detail=new_detail)

    def rows(self) -> list[ReaderHealth]:
        return list(self._rows.values())


def reconcile_canvas_pure(
    canvas: Canvas,
    *,
    task_ref_reader: TaskRefReader,
    decision_reader: RefLookupReader,
    finding_reader: RefLookupReader,
    lane_reader: LaneLookupReader,
) -> ReconciliationReport:
    """Classify every reference on every node and return a drift report.

    The detector iterates nodes in their declared order so output stays
    deterministic for a given input. Each ref kind is checked
    independently; a node may produce zero entries (nothing to check),
    one (single ref), or up to four (all ref kinds set).
    """

    entries: list[ReconciliationEntry] = []
    tracker = _ReaderTracker()

    for node in canvas.nodes:
        if node.task_ref is not None:
            entries.append(_classify_task_ref(node.id, node.task_ref, task_ref_reader, tracker))
        if node.decision_id is not None:
            entries.append(
                _classify_ref_lookup(
                    node.id,
                    ref_kind="decision_id",
                    ref_value=node.decision_id,
                    task_ref=node.task_ref,
                    reader=decision_reader,
                    tracker=tracker,
                )
            )
        if node.finding_id is not None:
            entries.append(
                _classify_ref_lookup(
                    node.id,
                    ref_kind="finding_id",
                    ref_value=node.finding_id,
                    task_ref=node.task_ref,
                    reader=finding_reader,
                    tracker=tracker,
                )
            )
        lane_value = _derive_lane_id(node.lane_id, node.id, node.kind)
        if lane_value is not None:
            entries.append(
                _classify_lane(
                    node.id,
                    ref_value=lane_value,
                    task_ref=node.task_ref or "",
                    reader=lane_reader,
                    tracker=tracker,
                )
            )

    snapshot = SourceSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        readers=tracker.rows(),
    )
    return ReconciliationReport(canvas_ref=canvas.canvas_ref, entries=entries, source_snapshot=snapshot)


def _classify_task_ref(
    node_id: str,
    ref_value: str,
    reader: TaskRefReader,
    tracker: _ReaderTracker,
) -> ReconciliationEntry:
    try:
        status = reader(ref_value)
    except Exception as exc:  # noqa: BLE001 — reader contract is best-effort
        tracker.mark("task_ref", ok=False, detail=f"{type(exc).__name__}: {exc}")
        return ReconciliationEntry(
            node_id=node_id,
            ref_kind="task_ref",
            ref_value=ref_value,
            drift=DriftKind.AMBIGUOUS,
        )
    tracker.mark("task_ref", ok=True)
    drift = _TASK_REF_STATUS_TO_DRIFT[status]
    return ReconciliationEntry(node_id=node_id, ref_kind="task_ref", ref_value=ref_value, drift=drift)


_TASK_REF_STATUS_TO_DRIFT: dict[TaskRefStatus, DriftKind] = {
    "present": DriftKind.PRESENT,
    "archived": DriftKind.ARCHIVED,
    "missing": DriftKind.MISSING,
}


def _classify_ref_lookup(
    node_id: str,
    *,
    ref_kind: RefKind,
    ref_value: str,
    task_ref: str | None,
    reader: RefLookupReader,
    tracker: _ReaderTracker,
) -> ReconciliationEntry:
    try:
        result = reader(ref_value, task_ref)
    except Exception as exc:  # noqa: BLE001 — reader contract is best-effort
        tracker.mark(ref_kind, ok=False, detail=f"{type(exc).__name__}: {exc}")
        return ReconciliationEntry(
            node_id=node_id,
            ref_kind=ref_kind,
            ref_value=ref_value,
            drift=DriftKind.AMBIGUOUS,
        )
    tracker.mark(ref_kind, ok=True)
    if result.exact:
        return ReconciliationEntry(node_id=node_id, ref_kind=ref_kind, ref_value=ref_value, drift=DriftKind.PRESENT)
    if len(result.candidates) == 1:
        suggested = result.candidates[0]
        return ReconciliationEntry(
            node_id=node_id,
            ref_kind=ref_kind,
            ref_value=ref_value,
            drift=DriftKind.RENAMED,
            suggested=suggested,
            candidates=[suggested],
        )
    if len(result.candidates) > 1:
        return ReconciliationEntry(
            node_id=node_id,
            ref_kind=ref_kind,
            ref_value=ref_value,
            drift=DriftKind.AMBIGUOUS,
            candidates=list(result.candidates),
        )
    return ReconciliationEntry(node_id=node_id, ref_kind=ref_kind, ref_value=ref_value, drift=DriftKind.MISSING)


def _classify_lane(
    node_id: str,
    *,
    ref_value: str,
    task_ref: str,
    reader: LaneLookupReader,
    tracker: _ReaderTracker,
) -> ReconciliationEntry:
    try:
        present = reader(task_ref, ref_value)
    except Exception as exc:  # noqa: BLE001 — reader contract is best-effort
        tracker.mark("lane_id", ok=False, detail=f"{type(exc).__name__}: {exc}")
        return ReconciliationEntry(
            node_id=node_id,
            ref_kind="lane_id",
            ref_value=ref_value,
            drift=DriftKind.AMBIGUOUS,
        )
    tracker.mark("lane_id", ok=True)
    drift = DriftKind.PRESENT if present else DriftKind.MISSING
    return ReconciliationEntry(node_id=node_id, ref_kind="lane_id", ref_value=ref_value, drift=drift)
