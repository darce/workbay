"""Browser-facing /api/** routes for canvas-mcp."""

from __future__ import annotations

import functools
import json
import logging
from dataclasses import asdict
from typing import Any, Awaitable, Callable

from fastmcp import FastMCP
from pydantic import BaseModel, ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from workbay_canvas_mcp import lab_experiments
from workbay_canvas_mcp.annotations import AnnotationError
from workbay_canvas_mcp.canvas_service import CanvasService, StaleDraftError
from workbay_canvas_mcp.export import ExportError
from workbay_canvas_mcp.models import Canvas
from workbay_canvas_mcp.reconcile_apply import ApplyReconciliationError, ReconciliationPlan
from workbay_canvas_mcp.review_errors import ReviewServiceError

logger = logging.getLogger(__name__)

_REVIEW_HTTP_STATUS: dict[str, int] = {
    "bundle_not_found": 404,
    "asset_not_found": 404,
    "asset_changed": 409,
    "asset_forbidden": 403,
    "path_outside_workspace": 400,
    "map_invalid": 400,
    "config_invalid": 400,
    "bundle_too_large": 413,
    "collector_failed": 502,
    "refresh_in_progress": 409,
    "internal_error": 500,
}
_REVIEW_INTERNAL_MESSAGE = "internal error"
_REVIEW_INVALID_BODY_MESSAGE = "invalid refresh body"


def _error_response(
    *,
    status: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message, "details": details or {}}},
        status_code=status,
    )


def _guarded(
    handler: Callable[[Request], Awaitable[Response]],
) -> Callable[[Request], Awaitable[Response]]:
    """Catch-all so every non-2xx response uses the contract error envelope."""

    @functools.wraps(handler)
    async def wrapper(request: Request) -> Response:
        try:
            return await handler(request)
        except Exception:  # noqa: BLE001 - contract: enveloped 500, never raw HTML
            return _error_response(status=500, code="internal_error", message=_REVIEW_INTERNAL_MESSAGE)

    return wrapper


async def _read_json(request: Request) -> dict[str, Any]:
    body = await request.body()
    if not body:
        return {}
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise json.JSONDecodeError("request body must be a JSON object", body.decode("utf-8", "replace"), 0)
    return parsed


def _model_json(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def _review_error_response(exc: ReviewServiceError) -> JSONResponse:
    return _error_response(
        status=_REVIEW_HTTP_STATUS.get(exc.code, 500),
        code=exc.code,
        message=exc.message,
        details=exc.details,
    )


def _review_internal_error() -> JSONResponse:
    return _error_response(
        status=500,
        code="internal_error",
        message=_REVIEW_INTERNAL_MESSAGE,
    )


def _review_invalid_body() -> JSONResponse:
    return _error_response(
        status=400,
        code="map_invalid",
        message=_REVIEW_INVALID_BODY_MESSAGE,
    )


def _parse_refresh_body(payload: dict[str, Any]) -> tuple[bool, str | None, str | None] | None:
    emit_handoff = payload.get("emit_handoff", False)
    task_ref = payload.get("task_ref")
    session = payload.get("session")
    if not isinstance(emit_handoff, bool):
        return None
    if task_ref is not None and not isinstance(task_ref, str):
        return None
    if session is not None and not isinstance(session, str):
        return None
    return emit_handoff, task_ref, session


def register_browser_api(server: FastMCP, service: CanvasService) -> None:
    """Attach /api/** custom routes to a FastMCP server."""

    def api_route(
        path: str, methods: list[str]
    ) -> Callable[[Callable[[Request], Awaitable[Response]]], Callable[[Request], Awaitable[Response]]]:
        def decorator(
            handler: Callable[[Request], Awaitable[Response]],
        ) -> Callable[[Request], Awaitable[Response]]:
            return server.custom_route(path, methods=methods)(_guarded(handler))

        return decorator

    def experiment_invalid(exc: lab_experiments.ExperimentValidationError) -> JSONResponse:
        return JSONResponse({"error": exc.code, "pointer": exc.pointer, "message": str(exc)}, status_code=400)

    @api_route("/api/critique", methods=["GET"])
    async def get_critique(request: Request) -> Response:
        map_ref = request.query_params.get("map_ref")
        if not map_ref:
            response: Response = _error_response(status=400, code="validation_error", message="map_ref is required")
        elif service.review_service is None:
            response = _review_internal_error()
        else:
            try:
                findings = await run_in_threadpool(service.review_service.critique_base_map, map_ref)
                response = JSONResponse({"map_ref": map_ref, "findings": [
                    {"rule_id": f.rule_id, "severity": f.severity, "pointer": f.pointer,
                     "fix": f.fix, "detail": f.detail} for f in findings
                ]})
            except ReviewServiceError as exc:
                response = _review_error_response(exc)
        response.headers["Cache-Control"] = "no-store"
        return response

    @api_route("/api/experiments", methods=["GET"])
    async def get_experiments(request: Request) -> Response:
        try:
            items = await run_in_threadpool(lab_experiments.list_experiments, service.workspace_info().workspace_root)
            return JSONResponse(items, headers={"Cache-Control": "no-store"})
        except lab_experiments.ExperimentValidationError as exc:
            return experiment_invalid(exc)

    @api_route("/api/experiments/{slug}", methods=["GET"])
    async def get_experiment(request: Request) -> Response:
        try:
            loaded = await run_in_threadpool(
                lab_experiments.read_experiment, service.workspace_info().workspace_root, request.path_params["slug"]
            )
            return Response(loaded.raw, media_type="application/json", headers={
                "ETag": f'"{loaded.sha256}"', "Cache-Control": "no-store",
                "X-Experiment-Outdated": str(loaded.outdated).lower(),
                "X-Experiment-Orphaned": str(loaded.orphaned).lower(),
            })
        except FileNotFoundError:
            return JSONResponse({"error": "not_found"}, status_code=404)
        except lab_experiments.ExperimentValidationError as exc:
            return experiment_invalid(exc)

    @api_route("/api/experiments/{slug}", methods=["PUT"])
    async def put_experiment(request: Request) -> Response:
        # The local HTTP reference monitor protects every mutating /api route
        # with X-Workbay-Canvas-Session (including this one).
        matches = request.headers.getlist("if-match")
        creates = request.headers.getlist("if-none-match")
        if not matches and not creates:
            return JSONResponse({"error": "precondition_required"}, status_code=428)
        if len(matches) + len(creates) != 1:
            return JSONResponse({"error": "invalid_precondition", "pointer": ""}, status_code=400)
        if_match = None
        if matches:
            value = matches[0]
            if len(value) != 66 or value[0] != '"' or value[-1] != '"' or not lab_experiments.SHA256.fullmatch(value[1:-1]):
                return JSONResponse({"error": "invalid_precondition", "pointer": "/if_match"}, status_code=400)
            if_match = value[1:-1]
        elif creates[0] != "*":
            return JSONResponse({"error": "invalid_precondition", "pointer": "/create"}, status_code=400)
        try:
            raw = bytearray()
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > lab_experiments.MAX_FILE_BYTES:
                    raise lab_experiments.ExperimentValidationError("file_too_large", "", "experiment exceeds 1 MiB")
            document = lab_experiments.decode_experiment(bytes(raw))
            receipt = await run_in_threadpool(
                lab_experiments.save_experiment, service.workspace_info().workspace_root,
                request.path_params["slug"], document, if_match=if_match, create=bool(creates),
            )
            return JSONResponse(asdict(receipt), headers={"ETag": f'"{receipt.sha256}"', "Cache-Control": "no-store"})
        except lab_experiments.ExperimentConflict as exc:
            return JSONResponse(exc.as_dict(), status_code=409)
        except lab_experiments.ExperimentValidationError as exc:
            return experiment_invalid(exc)

    @api_route("/api/assets", methods=["GET"])
    async def get_bundle_asset(request: Request) -> Response:
        headers = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
        review = service.review_service
        params = request.query_params
        if not all(params.get(key) for key in ("map_ref", "path", "digest")):
            response: Response = _error_response(
                status=400, code="validation_error", message="map_ref, path and digest are required"
            )
        elif review is None:
            response = _review_internal_error()
        else:
            try:
                content, media = await run_in_threadpool(
                    review.open_bundle_asset, params["map_ref"], params["path"], params["digest"]
                )
                response = Response(content=content, media_type=media)
                if media == "image/svg+xml":
                    headers["Content-Security-Policy"] = "sandbox; script-src 'none'"
            except ReviewServiceError as exc:
                response = _review_error_response(exc)
            except Exception:
                logger.warning("bundle asset read failed", exc_info=True)
                response = _review_internal_error()
        response.headers.update(headers)
        return response

    @api_route("/api/workspace", methods=["GET"])
    async def workspace(request: Request) -> Response:
        info = service.workspace_info()
        return JSONResponse(
            {
                "workspace_root": info.workspace_root,
                "db_path": info.db_path,
                "status": info.status,
            }
        )

    @api_route("/api/canvases", methods=["GET"])
    async def list_canvases(request: Request) -> Response:
        specs = service.list_canvases()
        return JSONResponse([_model_json(s) for s in specs])

    @api_route("/api/canvases/{canvas_ref}", methods=["GET"])
    async def get_canvas(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        canvas = service.get_canvas(canvas_ref)
        if canvas is None:
            return _error_response(status=404, code="not_found", message=f"unknown canvas: {canvas_ref}")
        return JSONResponse(_model_json(canvas))

    @api_route("/api/canvases/{canvas_ref}", methods=["PUT"])
    async def put_canvas(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        try:
            payload = await _read_json(request)
            expected = payload.pop("expected_updated_at", None)
            canvas = Canvas.model_validate({**payload, "canvas_ref": canvas_ref})
            saved = service.upsert_canvas(canvas, expected_updated_at=expected)
        except StaleDraftError:
            return _error_response(
                status=409,
                code="stale_draft",
                message="Canvas changed on the server. Reload before saving.",
            )
        except (ValidationError, json.JSONDecodeError, TypeError) as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        except ValueError as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        return JSONResponse(_model_json(saved))

    @api_route("/api/canvases/{canvas_ref}/annotations/{node_id}", methods=["GET"])
    async def list_annotations(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        node_id = request.path_params["node_id"]
        include = request.query_params.get("include_detached", "true").lower() != "false"
        items = service.list_annotations(
            canvas_ref=canvas_ref,
            node_id=node_id,
            include_detached=include,
        )
        return JSONResponse([_model_json(a) for a in items])

    @api_route("/api/canvases/{canvas_ref}/annotations/{node_id}", methods=["POST"])
    async def post_annotation(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        node_id = request.path_params["node_id"]
        try:
            payload = await _read_json(request)
            annotation = service.append_annotation(
                canvas_ref=canvas_ref,
                node_id=node_id,
                author=payload["author"],
                body=payload["body"],
                parent_annotation_id=payload.get("parent_annotation_id"),
            )
        except KeyError as exc:
            return _error_response(status=400, code="validation_error", message=f"missing field: {exc}")
        except AnnotationError as exc:
            status = 404 if exc.reason in {"parent_missing", "annotation_missing"} else 400
            code = "not_found" if status == 404 else "validation_error"
            return _error_response(status=status, code=code, message=str(exc))
        except json.JSONDecodeError as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        except ValueError as exc:
            return _error_response(status=404, code="not_found", message=str(exc))
        return JSONResponse(_model_json(annotation), status_code=201)

    @api_route("/api/canvases/{canvas_ref}/reconciliation", methods=["GET"])
    async def get_reconciliation(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        try:
            report = service.reconcile_canvas(canvas_ref)
        except ValueError as exc:
            if "reconcile_canvas requires reconcile_readers" in str(exc):
                return _error_response(
                    status=503,
                    code="workspace_unavailable",
                    message="Reconciliation readers are not configured for this workspace.",
                )
            if "unknown canvas_ref" in str(exc):
                return _error_response(status=404, code="not_found", message=str(exc))
            return _error_response(
                status=500,
                code="internal_error",
                message=_REVIEW_INTERNAL_MESSAGE,
            )
        return JSONResponse(_model_json(report))

    @api_route("/api/canvases/{canvas_ref}/reconciliation/apply", methods=["POST"])
    async def apply_reconciliation(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        try:
            payload = await _read_json(request)
            plan = ReconciliationPlan.model_validate(payload)
            canvas = service.apply_reconciliation(canvas_ref, plan)
        except (ValidationError, json.JSONDecodeError) as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        except ApplyReconciliationError as exc:
            if exc.reason == "node_not_found":
                return _error_response(status=404, code="not_found", message=str(exc))
            return _error_response(status=400, code="validation_error", message=str(exc))
        except ValueError as exc:
            if "unknown canvas_ref" in str(exc):
                return _error_response(status=404, code="not_found", message=str(exc))
            return _error_response(status=400, code="validation_error", message=str(exc))
        return JSONResponse(_model_json(canvas))

    @api_route("/api/designs/{design_ref}/document", methods=["GET"])
    async def get_design_document(request: Request) -> Response:
        design_ref = request.path_params["design_ref"]
        document = service.get_design_document(design_ref)
        return JSONResponse(_model_json(document))

    @api_route("/api/designs/{design_ref}/document", methods=["PATCH"])
    async def patch_design_document(request: Request) -> Response:
        design_ref = request.path_params["design_ref"]
        try:
            payload = await _read_json(request)
            document = service.patch_design_document(design_ref, payload)
        except (ValidationError, json.JSONDecodeError) as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        except ValueError as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        return JSONResponse(_model_json(document))

    @api_route("/api/designs/{design_ref}/components", methods=["GET"])
    async def list_components(request: Request) -> Response:
        design_ref = request.path_params["design_ref"]
        components = service.list_components(design_ref)
        return JSONResponse([_model_json(c) for c in components])

    @api_route("/api/designs/{design_ref}/components/{component_id}", methods=["PUT"])
    async def put_component(request: Request) -> Response:
        design_ref = request.path_params["design_ref"]
        component_id = request.path_params["component_id"]
        try:
            payload = await _read_json(request)
            payload.setdefault("component_id", component_id)
            receipt = service.upsert_component(design_ref, payload)
        except (ValidationError, json.JSONDecodeError) as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        except ValueError as exc:
            return _error_response(status=400, code="validation_error", message=str(exc))
        return JSONResponse(_model_json(receipt))

    @api_route(
        "/api/designs/{design_ref}/components/{component_id}/archive",
        methods=["POST"],
    )
    async def archive_component(request: Request) -> Response:
        design_ref = request.path_params["design_ref"]
        component_id = request.path_params["component_id"]
        try:
            receipt = service.archive_component(design_ref, component_id)
        except ValueError as exc:
            return _error_response(status=404, code="not_found", message=str(exc))
        return JSONResponse(_model_json(receipt))

    @api_route("/api/canvases/{canvas_ref}/export", methods=["GET"])
    async def export_canvas(request: Request) -> Response:
        canvas_ref = request.path_params["canvas_ref"]
        fmt = request.query_params.get("format", "mermaid")
        try:
            result = service.export_canvas(canvas_ref, fmt)
        except ExportError as exc:
            if exc.reason == "canvas_not_found":
                return _error_response(status=404, code="not_found", message=str(exc.detail))
            if exc.reason == "missing_dependency":
                # Server-side capability gap, not a client error (rg-015).
                return _error_response(status=500, code="internal_error", message=str(exc.detail))
            return _error_response(status=400, code="validation_error", message=str(exc.detail))
        # ExportResult serialises content_bytes as base64 via ser_json_bytes config.
        return JSONResponse(_model_json(result))

    @api_route("/api/review-bundles/{map_ref:path}/refresh", methods=["POST"])
    async def post_review_bundle_refresh(request: Request) -> Response:
        review = service.review_service
        if review is None:
            return _review_internal_error()
        map_ref = request.path_params["map_ref"]
        try:
            try:
                payload = await _read_json(request)
            except (ValueError, RecursionError):
                return _review_invalid_body()
            parsed = _parse_refresh_body(payload)
            if parsed is None:
                return _review_invalid_body()
            emit_handoff, task_ref, session = parsed
            result = await run_in_threadpool(
                review.refresh_bundle,
                map_ref,
                emit_handoff=emit_handoff,
                task_ref=task_ref,
                session=session,
            )
            return JSONResponse(
                {
                    "status": "ok",
                    "digest": result.digest,
                    "bundle": json.loads(result.bundle_text),
                    "handoff_emit": result.handoff_emit,
                }
            )
        except ReviewServiceError as exc:
            return _review_error_response(exc)
        except Exception:
            logger.warning("review-bundle refresh failed", exc_info=True)
            return _review_internal_error()

    @api_route("/api/review-bundles/{map_ref:path}/refresh", methods=["GET", "PUT", "DELETE", "PATCH", "OPTIONS"])
    async def review_bundle_refresh_method_not_allowed(request: Request) -> Response:
        response = _error_response(
            status=405,
            code="method_not_allowed",
            message="method not allowed",
        )
        response.headers["Allow"] = "POST"
        return response

    @api_route("/api/review-bundles/{map_ref:path}/status", methods=["GET"])
    async def get_review_bundle_status(request: Request) -> Response:
        review = service.review_service
        if review is None:
            return _review_internal_error()
        map_ref = request.path_params["map_ref"]
        try:
            status = await run_in_threadpool(review.get_bundle_status, map_ref)
            return JSONResponse(asdict(status))
        except ReviewServiceError as exc:
            return _review_error_response(exc)
        except Exception:
            logger.warning("review-bundle status failed", exc_info=True)
            return _review_internal_error()

    @api_route("/api/review-bundles", methods=["GET"])
    async def list_review_bundles(request: Request) -> Response:
        review = service.review_service
        if review is None:
            return _review_internal_error()
        try:
            summaries = await run_in_threadpool(review.list_bundles)
            return JSONResponse({"bundles": [asdict(summary) for summary in summaries]})
        except ReviewServiceError as exc:
            return _review_error_response(exc)
        except Exception:
            logger.warning("review-bundle list failed", exc_info=True)
            return _review_internal_error()

    @api_route("/api/review-bundles/{map_ref:path}", methods=["GET"])
    async def get_review_bundle(request: Request) -> Response:
        review = service.review_service
        if review is None:
            return _review_internal_error()
        map_ref = request.path_params["map_ref"]
        try:
            bundle_text = await run_in_threadpool(review.get_bundle, map_ref)
            return Response(content=bundle_text, media_type="application/json")
        except ReviewServiceError as exc:
            return _review_error_response(exc)
        except Exception:
            logger.warning("review-bundle get failed", exc_info=True)
            return _review_internal_error()
