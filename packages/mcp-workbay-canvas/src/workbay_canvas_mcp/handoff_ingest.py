"""Map a handoff snapshot to a :class:`Canvas` (implementation note).

:func:`build_canvas_from_handoff` is a pure transformation. The reader is
an injected callable so tests can supply an in-process stub; the
production reader that adapts ``workbay-handoff-mcp``'s ``get_handoff_state``
lives in implementation note.

Node / edge shape (pinned by ``tests/test_handoff_ingest.py``):

- one ``kind="task"`` node per call;
- one ``kind="slice"`` node per slice-complete decision (decision id
  contains ``_slice_complete_``), preserving the snapshot's order
  (callers pass newest-first);
- one ``kind="decision"`` node per non-slice-complete decision;
- one ``derives_from`` edge from each non-task node to the task node;
- one ``depends_on`` edge between each pair of successive slice nodes
  (newer -> older); count == ``max(0, num_slice_nodes - 1)``.
"""

from __future__ import annotations

from typing import Any, Callable

from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

SLICE_COMPLETE_MARKER = "_slice_complete_"

HandoffReader = Callable[[str], dict[str, Any]]


def _is_slice_complete(decision_id: str) -> bool:
    return SLICE_COMPLETE_MARKER in decision_id


def build_canvas_from_handoff(*, reader: HandoffReader, task_ref: str) -> Canvas:
    """Render the handoff snapshot for ``task_ref`` as a :class:`Canvas`.

    The caller-supplied ``reader`` returns a dict with a ``decisions``
    list ordered newest-first. Each decision must carry a ``decision``
    string id; other fields are ignored by this slice.
    """

    snapshot = reader(task_ref)
    decisions: list[dict[str, Any]] = list(snapshot.get("decisions", []) or [])

    task_node = Node(
        id=f"task:{task_ref}",
        kind=NodeKind.TASK,
        label=task_ref,
        task_ref=task_ref,
    )

    slice_nodes: list[Node] = []
    decision_nodes: list[Node] = []
    for row in decisions:
        decision_id = row["decision"]
        if _is_slice_complete(decision_id):
            slice_nodes.append(
                Node(
                    id=f"slice:{decision_id}",
                    kind=NodeKind.SLICE,
                    label=decision_id,
                    task_ref=task_ref,
                    decision_id=decision_id,
                )
            )
        else:
            decision_nodes.append(
                Node(
                    id=f"decision:{decision_id}",
                    kind=NodeKind.DECISION,
                    label=decision_id,
                    task_ref=task_ref,
                    decision_id=decision_id,
                )
            )

    edges: list[Edge] = []
    for node in (*slice_nodes, *decision_nodes):
        edges.append(
            Edge(
                id=f"derives:{node.id}",
                kind=EdgeKind.DERIVES_FROM,
                source=node.id,
                target=task_node.id,
            )
        )
    for i in range(len(slice_nodes) - 1):
        newer = slice_nodes[i]
        older = slice_nodes[i + 1]
        edges.append(
            Edge(
                id=f"depends:{newer.id}->{older.id}",
                kind=EdgeKind.DEPENDS_ON,
                source=newer.id,
                target=older.id,
            )
        )

    return Canvas(
        canvas_ref=f"task:{task_ref}",
        nodes=[task_node, *slice_nodes, *decision_nodes],
        edges=edges,
    )
