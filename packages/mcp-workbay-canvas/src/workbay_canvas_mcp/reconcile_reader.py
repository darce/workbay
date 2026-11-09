"""Production readers for ``reconcile_canvas`` (internal).

:func:`default_reconcile_readers` builds the four callables that
:func:`workbay_canvas_mcp.reconcile.reconcile_canvas_pure` expects, each
bound to the live ``workbay-handoff-mcp`` and ``workbay-orchestrator-mcp``
package surfaces.

The factory returns *fresh* callables on every call so per-reconcile
state (e.g. the cached set of live ``task_ref`` rows) is scoped to one
report and does not leak between MCP invocations.
"""

from __future__ import annotations

from typing import Any

from workbay_handoff_mcp import (
    archive,
    list_active_tasks,
    list_review_findings,
    search_handoff,
)
from workbay_handoff_mcp.api import ArchiveOpGet
from workbay_orchestrator_mcp.api import manage_worktree_lane  # type: ignore[import-untyped]

from workbay_canvas_mcp.reconcile import (
    LaneLookupReader,
    RefLookupReader,
    RefLookupResult,
    TaskRefReader,
    TaskRefStatus,
)


def default_reconcile_readers() -> tuple[TaskRefReader, RefLookupReader, RefLookupReader, LaneLookupReader]:
    """Build the (task_ref, decision, finding, lane) reader tuple.

    Each callable is self-contained and tolerates the upstream
    ``ok=False`` envelope shape: a downstream MCP fault degrades the
    affected entry to ``DriftKind.AMBIGUOUS`` via the exception path in
    :func:`reconcile_canvas_pure`, rather than corrupting the report.
    """

    return (
        _build_task_ref_reader(),
        _build_decision_reader(),
        _build_finding_reader(),
        _build_lane_reader(),
    )


def _build_task_ref_reader() -> TaskRefReader:
    live_cache: dict[str, bool] = {}

    def _read(ref_value: str) -> TaskRefStatus:
        if not live_cache:
            rows = list_active_tasks()
            for row in rows:
                key = str(row.get("task_ref") or "")
                if key:
                    live_cache[key] = True
        if live_cache.get(ref_value):
            return "present"
        envelope = archive(payload=ArchiveOpGet(operation="get", task_ref=ref_value))
        if envelope.get("ok"):
            return "archived"
        return "missing"

    return _read


def _build_decision_reader() -> RefLookupReader:
    def _read(ref_value: str, task_ref: str | None) -> RefLookupResult:
        envelope = search_handoff(
            queries=[ref_value],
            task_ref=task_ref if task_ref else "",
            record_types=["decision"],
            decision_fields=["decision"],
            limit=20,
        )
        if not envelope.get("ok"):
            return RefLookupResult(exact=False, candidates=[])
        results = envelope.get("data", {}).get("results", []) or []
        ids: list[str] = []
        for row in results:
            decision_id = row.get("decision")
            if isinstance(decision_id, str) and decision_id:
                ids.append(decision_id)
        if ref_value in ids:
            return RefLookupResult(exact=True, candidates=[])
        candidates = [d for d in dict.fromkeys(ids) if d != ref_value]
        return RefLookupResult(exact=False, candidates=candidates)

    return _read


def _build_finding_reader() -> RefLookupReader:
    def _read(ref_value: str, task_ref: str | None) -> RefLookupResult:
        exact_envelope = (
            list_review_findings(task_ref=task_ref, finding_id=ref_value)
            if task_ref
            else list_review_findings(finding_id=ref_value)
        )
        if exact_envelope.get("ok"):
            return RefLookupResult(exact=True, candidates=[])
        # FTS search for prefix candidates. Finding ids are surfaced
        # only by their numeric ``record_id`` in search_handoff output,
        # so we cannot derive string ids from this index. The
        # production reader returns no candidates on miss — operators
        # see ``DriftKind.MISSING`` and rerun with a corrected ref or
        # use the apply tool to remove the dangling node.
        return RefLookupResult(exact=False, candidates=[])

    return _read


def _build_lane_reader() -> LaneLookupReader:
    cache: dict[str, set[str]] = {}

    def _read(task_ref: str, lane_id: str) -> bool:
        if not task_ref:
            return False
        if task_ref not in cache:
            envelope: dict[str, Any] = manage_worktree_lane(operation="list", task_ref=task_ref)
            if not envelope.get("ok"):
                cache[task_ref] = set()
            else:
                cache[task_ref] = {str(row["lane_id"]) for row in (envelope.get("lanes") or []) if row.get("lane_id")}
        return lane_id in cache[task_ref]

    return _read


__all__ = [
    "default_reconcile_readers",
]
