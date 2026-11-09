"""Read-only Fleet routes. LocalRequestGuard supplies the loopback boundary.

GET never refreshes snapshots or writes a database. The orchestrator is imported
inside handlers so importing the local-server module adds no collector activity.
"""

from __future__ import annotations

import json
from pathlib import Path

from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from workbay_canvas_mcp.layout import LayoutAlgorithm, LayoutError, layout_canvas_pure
from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

_HEADERS = {"Cache-Control": "no-store"}


def _error(code, status, **fields):
    return JSONResponse({"error": code, **fields}, status_code=status, headers=_HEADERS)


def register_fleet_api(server, *, workspace_root):
    root = Path(workspace_root)
    cache = root / ".task-state/fleet/snapshot.json"

    def read_cache():
        # Read bytes directly: read_snapshot_cache would rewrite age fields in memory.
        return cache.read_bytes()

    def read_document():
        value = json.loads(read_cache())
        if not isinstance(value, dict):
            raise ValueError("invalid snapshot")
        return value

    @server.custom_route("/api/fleet/snapshot", methods=["GET"])
    async def snapshot(_request: Request):
        try:
            return Response(read_cache(), media_type="application/json", headers=_HEADERS)
        except FileNotFoundError:
            return _error("no_snapshot", 404, hint="run workbay fleet")
        except OSError:
            return _error("snapshot_unavailable", 503)

    @server.custom_route("/api/fleet/tree", methods=["GET"])
    async def tree(request: Request):
        from workbay_orchestrator_mcp.orchestration.fleet_snapshot import build_lineage_tree, prompt_summaries

        try:
            if set(request.query_params) - {"root", "limit"}:
                raise ValueError("unsupported query")
            limit = int(request.query_params.get("limit", "200"))
            if not 1 <= limit <= 1000:
                raise ValueError("limit must be 1..1000")
        except ValueError as exc:
            return _error("invalid_query", 400, message=str(exc))
        try:
            observed = read_document()
            graph = build_lineage_tree(observed, root_id=request.query_params.get("root"), limit=limit)
            nodes = [
                Node(
                    id=n["id"],
                    kind={"task": NodeKind.TASK, "lane": NodeKind.LANE}.get(n["kind"], NodeKind.FREEFORM),
                    label=n.get("lane_id") or n.get("pass_id") or n["id"],
                    task_ref=n.get("task_ref"),
                    lane_id=n.get("lane_id"),
                )
                for n in graph["nodes"]
            ]
            edges = []
            for index, e in enumerate(graph["edges"]):
                # Builder containment is parent->child; lineage already consumer->input.
                child, parent = (
                    (e["target"], e["source"]) if e["relation"] == "contains" else (e["source"], e["target"])
                )
                edges.append(
                    Edge(
                        id=f"fleet:{index}",
                        kind=EdgeKind.DERIVES_FROM,
                        source=child,
                        target=parent,
                        label=e["relation"] + (" (inferred)" if e["inferred"] else ""),
                    )
                )
            canvas = Canvas(
                canvas_ref="fleet",
                nodes=nodes,
                edges=edges,
                metadata={"fleet": {**graph, "summaries": prompt_summaries(root, observed)}},
            )
            laid_out = layout_canvas_pure(canvas, LayoutAlgorithm.HIERARCHICAL)
            # The shared layout returns its own metadata; retain Fleet evidence too.
            laid_out.metadata["fleet"] = canvas.metadata["fleet"]
            return JSONResponse(laid_out.model_dump(mode="json"), headers=_HEADERS)
        except FileNotFoundError:
            return _error("no_snapshot", 404, hint="run workbay fleet")
        except LayoutError as exc:
            return _error("layout_" + exc.reason, 422, message=exc.message)
        except (ValueError, TypeError, KeyError):
            return _error("invalid_snapshot", 422)
        except OSError:
            return _error("snapshot_unavailable", 503)

    @server.custom_route("/api/fleet/brief", methods=["GET"])
    async def brief(request: Request):
        from workbay_orchestrator_mcp.orchestration.fleet_snapshot import read_fleet_brief

        if set(request.query_params) != {"pass_id"} or not request.query_params.get("pass_id"):
            return _error("invalid_query", 400)
        try:
            data = read_fleet_brief(root, read_document(), request.query_params["pass_id"])
            return Response(data, media_type="text/plain", headers=_HEADERS)
        except PermissionError:
            return Response("brief_forbidden", status_code=403, media_type="text/plain", headers=_HEADERS)
        except (KeyError, FileNotFoundError):
            return Response("brief_unknown", status_code=404, media_type="text/plain", headers=_HEADERS)
        except (OSError, ValueError, TypeError):
            return Response("brief_unavailable", status_code=422, media_type="text/plain", headers=_HEADERS)
