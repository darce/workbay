"""Console-script entry point for the canvas MCP server (implementation note) + UX map CLI (S4).

Default (no subcommand or ``serve``): run the FastMCP server.

UX map subcommands (filesystem SSOT; advisory critique exits 0; emit-handoff exits nonzero on a failed receipt):

- ``canvas-mcp ux-map render --path|--map-ref …``
- ``canvas-mcp ux-map critique …``
- ``canvas-mcp ux-map project --flow-id …``

Persistence resolution for serve follows the Persistence Contract: explicit
args > ``$DESIGN_CANVAS_MCP_DB`` >
``<cwd>/.task-state/canvas-mcp/canvas.db``.
"""

from __future__ import annotations

import argparse
import difflib
import importlib.resources
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from workbay_canvas_mcp.design_inventory.collect_runner import (
    NODE_MISSING_MESSAGE,
    InventoryCollectError,
    resolve_collector,
    run_collector,
)
from workbay_canvas_mcp.design_inventory.config import (
    DEFAULT_CONFIG_PATH,
    load_design_sources,
    resolve_config_path,
)
from workbay_canvas_mcp.design_inventory.errors import InventoryConfigError
from workbay_canvas_mcp.design_inventory.normalize import normalize_inventory
from workbay_canvas_mcp.design_inventory.refresh import check_generated, refresh_generated
from workbay_canvas_mcp.handoff_reader import default_handoff_reader
from workbay_canvas_mcp.lane_reader import default_lane_reader
from workbay_canvas_mcp.reconcile_reader import default_reconcile_readers
from workbay_canvas_mcp.review_errors import ReviewServiceError
from workbay_canvas_mcp.review_service import ReviewService
from workbay_canvas_mcp.server import build_server, resolve_db_path
from workbay_canvas_mcp.store import CanvasStore
from workbay_canvas_mcp.ux_map.critique import critique_map
from workbay_canvas_mcp.ux_map.fs_store import load_map, write_renders
from workbay_canvas_mcp.ux_map.handoff_emit import emit_critique_to_handoff
from workbay_canvas_mcp.ux_map.migrate import migrate_texts
from workbay_canvas_mcp.ux_map.parse import MapSchemaError, parse_map
from workbay_canvas_mcp.ux_map.paths import resolve_map_path
from workbay_canvas_mcp.ux_map.project_canvas import project_flow_to_canvas
from workbay_canvas_mcp.ux_map.render_ascii import render_ascii_layout, render_flow_ascii
from workbay_canvas_mcp.ux_map.render_markdown import render_markdown_bundle
from workbay_canvas_mcp.ux_map.render_mermaid import render_flow_mermaid
from workbay_canvas_mcp.ux_map.source_check import source_report_to_dict, verify_code_refs

# Collapse C0, DEL, C1, and Unicode line/paragraph separators so map-controlled
# source-check fields cannot forge extra report lines (WBC02-S9-04).
_SOURCE_CHECK_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029 ]+")


def _sanitize_source_check_field(value: str) -> str:
    return _SOURCE_CHECK_CONTROL_RE.sub(" ", value)


def _write_text_atomic(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _add_serve_args(parser: argparse.ArgumentParser, *, default_port: int = 8765) -> None:
    parser.set_defaults(default_port=default_port)
    parser.add_argument(
        "--db-path",
        default=None,
        help="Explicit SQLite path. Overrides $DESIGN_CANVAS_MCP_DB and the workspace default.",
    )
    parser.add_argument(
        "--workspace-root",
        default=None,
        help="Workspace root used to derive the default DB path. Defaults to cwd.",
    )
    parser.add_argument(
        "--transport",
        default="stdio",
        choices=["stdio", "http"],
        help="Transport for the MCP server. 'stdio' (default) for local clients; "
        "'http' serves streamable-http over --host/--port for the browser SPA.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind when --transport http. Defaults to 127.0.0.1 (loopback only).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"HTTP port. Defaults to {default_port}" + (" (a free port)." if default_port == 0 else "."),
    )
    parser.add_argument(
        "--allow-origin",
        action="append",
        default=None,
        metavar="URL",
        help="Additional loopback Origin allowed by the HTTP guard. Repeatable.",
    )


def _add_inventory_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        default=None,
        help="Workspace-relative design-sources.json (default docs/design/design-sources.json).",
    )
    parser.add_argument(
        "--workspace-root",
        default=None,
        help="Workspace root for config and generated paths. Defaults to cwd.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Workspace-relative generated dir (default from config generated_dir).",
    )
    parser.add_argument(
        "--collector",
        default=None,
        help="Path to collect-design-inventory.mjs (Decision 5; never under --workspace-root).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON on stdout.",
    )


def _add_map_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--path", default=None, help="Explicit path to a .uxmap.json file.")
    parser.add_argument("--map-ref", default=None, help="Map ref; resolves under --maps-dir.")
    parser.add_argument(
        "--maps-dir",
        default=None,
        help="Maps directory (absolute or relative to --workspace-root). Default: <workspace-root>/docs/ux-maps.",
    )
    parser.add_argument(
        "--workspace-root",
        default=None,
        help="Workspace root for maps-dir and path resolution. Defaults to cwd.",
    )


def _add_bundle_args(parser: argparse.ArgumentParser, *, map_ref_required: bool) -> None:
    parser.add_argument(
        "--workspace-root",
        default=None,
        help="Workspace root for config and generated paths. Defaults to cwd.",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Workspace-relative design-sources.json (default docs/design/design-sources.json).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON on stdout.",
    )
    parser.add_argument(
        "--map-ref",
        required=map_ref_required,
        default=None,
        help="Map ref (SAFE_ID).",
    )


def _resolve_serve_port(args: argparse.Namespace) -> argparse.Namespace:
    if hasattr(args, "port"):
        args.port_explicit = args.port is not None
        if args.port is None:
            args.port = args.default_port
    return args


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse argv with backward-compatible default serve when no subcommand."""
    raw = list(sys.argv[1:] if argv is None else argv)

    # Legacy / default: no subcommand or flags-only → serve
    if not raw or raw[0].startswith("-"):
        parser = argparse.ArgumentParser(
            prog="canvas-mcp",
            description="Run the canvas MCP server.",
        )
        _add_serve_args(parser)
        ns = parser.parse_args(raw)
        ns.command = "serve"
        return _resolve_serve_port(ns)

    parser = argparse.ArgumentParser(
        prog="canvas-mcp",
        description="Canvas MCP server and UX map CLI.",
    )
    sub = parser.add_subparsers(dest="command")

    serve_p = sub.add_parser("serve", help="Run the canvas MCP server (default).")
    _add_serve_args(serve_p)

    local_p = sub.add_parser("local", help="Launch the local loopback canvas web app.")
    _add_serve_args(local_p, default_port=0)
    local_p.add_argument(
        "--web-dist",
        default=None,
        help="Directory of built web assets (default: packaged web_dist).",
    )
    local_p.add_argument("--view", choices=["flows", "library", "lab"], default=None)
    browser = local_p.add_mutually_exclusive_group()
    browser.add_argument("--open", dest="open_browser", action="store_true", help="Open the browser.")
    browser.add_argument("--no-open", dest="open_browser", action="store_false", help="Do not open the browser.")
    local_p.set_defaults(open_browser=None)

    experiment = sub.add_parser("experiment", help="Save or show a git-owned Lab experiment.")
    experiment_sub = experiment.add_subparsers(dest="experiment_command", required=True)
    save = experiment_sub.add_parser("save", help="Save with exclusive create or exact-file SHA256 CAS.")
    show = experiment_sub.add_parser("show", help="Print the document and exact-file SHA256.")
    for command in (save, show):
        command.add_argument("slug")
        command.add_argument("--workspace-root", default=None)
    save.add_argument("--file", required=True)
    precondition = save.add_mutually_exclusive_group(required=True)
    precondition.add_argument("--if-match", default=None)
    precondition.add_argument("--create", action="store_true")

    ux = sub.add_parser("ux-map", help="UX map filesystem tools.")
    ux_sub = ux.add_subparsers(dest="ux_command", required=True)

    render_p = ux_sub.add_parser("render", help="Render ascii/mermaid/markdown for a map.")
    _add_map_common(render_p)
    render_p.add_argument(
        "--format",
        dest="fmt",
        default="markdown",
        choices=["ascii", "ascii-flow", "mermaid", "markdown", "all"],
        help="Output format (default markdown).",
    )
    render_p.add_argument("--flow-id", default=None, help="Flow id for ascii-flow and mermaid render.")
    render_p.add_argument("--screen-id", default=None, help="Screen id for ascii render.")
    render_p.add_argument(
        "--out-dir",
        default=None,
        help="If set, write the markdown bundle (and companion outputs) here.",
    )
    render_p.add_argument(
        "--check",
        action="store_true",
        help="Exit 0 iff the Markdown file equals the deterministic render; print a unified diff otherwise.",
    )
    render_p.add_argument("check_json", nargs="?", help="Map JSON path used with --check.")
    render_p.add_argument("check_markdown", nargs="?", help="Rendered Markdown path used with --check.")

    critique_p = ux_sub.add_parser("critique", help="Run heuristic critique (exit 0; advisory).")
    _add_map_common(critique_p)
    critique_p.add_argument(
        "--json",
        action="store_true",
        help="Emit findings as JSON (array, or object with handoff_emit when --emit-handoff).",
    )
    critique_p.add_argument(
        "--task-ref",
        default=None,
        help="Optional handoff task_ref for --emit-handoff.",
    )
    critique_p.add_argument(
        "--session",
        default=None,
        help="Session id for handoff emit (default ux-map-critique-<map_ref>).",
    )
    critique_p.add_argument(
        "--emit-handoff",
        action="store_true",
        help="Batch-record findings under --task-ref via workbay-handoff review_findings.",
    )

    project_p = ux_sub.add_parser(
        "project",
        help="Project a flow onto a FREEFORM canvas and persist it.",
    )
    _add_map_common(project_p)
    project_p.add_argument("--flow-id", required=True, help="Flow id to project.")
    project_p.add_argument(
        "--db-path",
        default=None,
        help="Canvas SQLite path for persistence (default workspace canvas.db).",
    )

    migrate_p = ux_sub.add_parser("migrate", help="Write a versioned copy and print a unified diff.")
    migrate_p.add_argument("--path", required=True, help="Input .uxmap.json path.")
    migrate_p.add_argument("--out", required=True, help="Output path (must differ from --path).")

    source_p = ux_sub.add_parser("source-check", help="Verify map code_ref paths (advisory; exit 0).")
    _add_map_common(source_p)
    source_p.add_argument(
        "--json",
        action="store_true",
        help="Emit the source-check report as JSON.",
    )

    inv_p = ux_sub.add_parser("inventory", help="Collect or check the living design inventory.")
    inv_sub = inv_p.add_subparsers(dest="inventory_command", required=True)
    collect_p = inv_sub.add_parser("collect", help="Refresh generated inventory artifacts.")
    _add_inventory_args(collect_p)
    check_p = inv_sub.add_parser("check", help="Check generated inventory artifacts without writing.")
    _add_inventory_args(check_p)

    bundle_p = ux_sub.add_parser("bundle", help="List, get, status, or refresh review bundles.")
    bundle_sub = bundle_p.add_subparsers(dest="bundle_command", required=True)
    list_p = bundle_sub.add_parser("list", help="List review bundles.")
    _add_bundle_args(list_p, map_ref_required=False)
    get_p = bundle_sub.add_parser("get", help="Print last-good review bundle JSON.")
    _add_bundle_args(get_p, map_ref_required=True)
    status_p = bundle_sub.add_parser("status", help="Print review bundle status.")
    _add_bundle_args(status_p, map_ref_required=True)
    refresh_p = bundle_sub.add_parser("refresh", help="Refresh a review bundle.")
    _add_bundle_args(refresh_p, map_ref_required=True)
    refresh_p.add_argument(
        "--emit-handoff",
        action="store_true",
        help="Batch-record critique findings under --task-ref after refresh.",
    )
    refresh_p.add_argument(
        "--task-ref",
        default=None,
        help="Optional handoff task_ref for --emit-handoff.",
    )
    refresh_p.add_argument(
        "--session",
        default=None,
        help="Session id for handoff emit (default canvas-refresh).",
    )

    return _resolve_serve_port(parser.parse_args(raw))


def _load_map_from_ns(ns: argparse.Namespace):
    path = resolve_map_path(
        path=ns.path,
        map_ref=ns.map_ref,
        maps_dir=ns.maps_dir,
        workspace_root=ns.workspace_root,
    )
    return load_map(path), path


def _launch_kwargs(args: argparse.Namespace) -> dict:
    return {
        "workspace_root": Path(args.workspace_root) if args.workspace_root else None,
        "db_path": Path(args.db_path) if args.db_path else None,
        "host": args.host,
        "port": args.port,
        "port_explicit": args.port_explicit,
        "allow_origins": tuple(args.allow_origin or ()),
    }


def _run_local(args: argparse.Namespace) -> int:
    from workbay_canvas_mcp import local_web

    if args.web_dist is not None:
        web_dist = Path(args.web_dist)
    else:
        web_dist = Path(str(importlib.resources.files("workbay_canvas_mcp").joinpath("web_dist")))
    return local_web.run_local(
        **_launch_kwargs(args),
        web_dist=web_dist,
        pin_workspace_db=True,
        view=args.view,
        open_browser=sys.stdout.isatty() if args.open_browser is None else args.open_browser,
    )


def _run_serve(args: argparse.Namespace) -> int:
    if args.transport == "http":
        from workbay_canvas_mcp import local_web

        return local_web.run_local(
            **_launch_kwargs(args),
            web_dist=None,
            pin_workspace_db=False,
        )
    server = build_server(
        db_path=args.db_path,
        workspace_root=args.workspace_root,
        handoff_reader=default_handoff_reader,
        lane_reader=default_lane_reader,
        reconcile_readers=default_reconcile_readers(),
    )
    server.run()
    return 0


def _run_ux_map_render(args: argparse.Namespace) -> int:
    if args.check:
        if not args.check_json or not args.check_markdown:
            sys.stderr.write("error: render --check requires <json> and <md> paths\n")
            return 2
        if args.path or args.map_ref or args.out_dir or args.fmt != "markdown":
            sys.stderr.write("error: render --check cannot be combined with map selectors, --out-dir, or --format\n")
            return 2

        source_path = Path(args.check_json)
        rendered_path = Path(args.check_markdown)
        expected = render_markdown_bundle(load_map(source_path))
        actual = rendered_path.read_text(encoding="utf-8")
        if actual == expected:
            return 0

        diff = difflib.unified_diff(
            actual.splitlines(keepends=True),
            expected.splitlines(keepends=True),
            fromfile=str(rendered_path),
            tofile=f"deterministic:{source_path}",
        )
        sys.stdout.writelines(diff)
        return 1

    if args.check_json or args.check_markdown:
        sys.stderr.write("error: positional <json> and <md> paths require --check\n")
        return 2

    map_, _path = _load_map_from_ns(args)
    fmt = args.fmt
    chunks: list[str] = []
    flow_id = args.flow_id or (map_.flows[0].id if map_.flows else None)

    if fmt in ("markdown", "all"):
        chunks.append(render_markdown_bundle(map_))
    if fmt in ("ascii", "all"):
        if args.screen_id:
            chunks.append(render_ascii_layout(map_, screen_id=args.screen_id))
        elif map_.screens:
            chunks.append("\n\n".join(render_ascii_layout(map_, screen_id=s.id) for s in map_.screens))
        else:
            chunks.append("")
    if fmt in ("ascii-flow", "all"):
        if flow_id is not None:
            chunks.append(render_flow_ascii(map_, flow_id=flow_id))
    if fmt in ("mermaid", "all"):
        if flow_id is None:
            chunks.append('flowchart TD\n  empty["no flows"]\n')
        else:
            chunks.append(render_flow_mermaid(map_, flow_id=flow_id))

    text = "\n\n".join(chunks)
    if args.out_dir:
        written = write_renders(map_, args.out_dir)
        print("\n".join(str(p) for p in written))
    elif text or fmt != "ascii-flow":
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
    return 0


def _run_ux_map_critique(args: argparse.Namespace) -> int:
    map_, path = _load_map_from_ns(args)
    findings = critique_map(map_)
    rows = [
        {
            "rule_id": f.rule_id,
            "severity": f.severity,
            "pointer": f.pointer,
            "fix": f.fix,
            "detail": f.detail,
            "pack_revision": f.pack_revision,
            "canon_revision": f.canon_revision,
            "evidence_kind": f.evidence_kind,
            "next_check": f.next_check,
            "limitation": f.limitation,
        }
        for f in findings
    ]
    receipt = None
    if args.emit_handoff:
        if not args.task_ref:
            sys.stderr.write("error: --emit-handoff requires --task-ref\n")
            return 2
        try:
            from workbay_handoff_mcp import RuntimeConfig, configure_runtime

            configure_runtime(RuntimeConfig.for_workspace(args.workspace_root or Path.cwd()))
            receipt = emit_critique_to_handoff(
                findings,
                task_ref=args.task_ref,
                session=args.session or f"ux-map-critique-{map_.map_ref}",
                map_ref=map_.map_ref,
                map_path=str(path),
                workspace_root=args.workspace_root,
            )
        except Exception as exc:  # noqa: BLE001 — advisory emit
            receipt = {
                "ok": False,
                "written": 0,
                "finding_ids": [],
                "error": f"{type(exc).__name__}: {exc}",
            }

    if args.json:
        payload: dict | list = rows
        if args.emit_handoff:
            payload = {"findings": rows, "handoff_emit": receipt}
        sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
    else:
        if not findings:
            sys.stdout.write("No findings.\n")
        for f in findings:
            sys.stdout.write(
                f"[{f.severity}] {f.rule_id} @ {f.pointer}: {f.fix}" + (f" ({f.detail})" if f.detail else "") + "\n"
            )
        if receipt is not None:
            sys.stdout.write(
                f"handoff_emit: ok={receipt.get('ok')} written={receipt.get('written')}"
                + (f" error={receipt.get('error')}" if receipt.get("error") else "")
                + "\n"
            )
    if args.emit_handoff and not (receipt and receipt.get("ok")):
        return 1
    return 0


def _run_ux_map_migrate(args: argparse.Namespace) -> int:
    in_arg = args.path
    out_arg = args.out
    in_path = Path(in_arg)
    out_path = Path(out_arg)
    same = in_path.resolve() == out_path.resolve()
    if not same and out_path.exists():
        try:
            same = os.path.samefile(in_path, out_path)
        except OSError:
            same = False
    if same:
        sys.stderr.write("error: --out is the same file as --path; refusing to overwrite the input\n")
        return 2
    src_text = in_path.read_text(encoding="utf-8")
    try:
        parse_map(json.loads(src_text))
    except (json.JSONDecodeError, MapSchemaError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    json_text, diff = migrate_texts(src_text, fromfile=in_arg, tofile=out_arg)
    _write_text_atomic(out_path, json_text)
    sys.stdout.write(diff)
    return 0


def _run_ux_map_source_check(args: argparse.Namespace) -> int:
    try:
        map_, _path = _load_map_from_ns(args)
    except (json.JSONDecodeError, MapSchemaError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    root = Path(args.workspace_root) if args.workspace_root else Path.cwd()
    report = verify_code_refs(map_, workspace_root=root, codemap=None)
    if args.json:
        sys.stdout.write(json.dumps(source_report_to_dict(report), indent=2) + "\n")
    elif not report.items:
        sys.stdout.write("No code refs.\n")
    else:
        for item in report.items:
            line = (
                f"{_sanitize_source_check_field(item.status)} "
                f"{_sanitize_source_check_field(item.kind)} "
                f"{_sanitize_source_check_field(item.pointer)} "
                f"{_sanitize_source_check_field(item.code_ref)}"
            )
            if item.detail:
                line += f" ({_sanitize_source_check_field(item.detail)})"
            sys.stdout.write(line + "\n")
    return 0


def _resolve_explicit_collector(explicit: str) -> Path:
    candidate = Path(explicit)
    path = candidate if candidate.is_absolute() else Path.cwd() / candidate
    try:
        found = path.is_file() and path.name == "collect-design-inventory.mjs"
    except OSError:
        found = False
    if not found:
        raise InventoryCollectError("inventory collector not found", reason="collector_not_found")
    return resolve_collector(explicit)


def _require_node() -> None:
    node = os.environ.get("WORKBAY_NODE") or shutil.which("node")
    if not node:
        raise InventoryCollectError(NODE_MISSING_MESSAGE, reason="node_not_found")
    try:
        exists = Path(node).exists()
    except OSError:
        exists = False
    if not exists:
        raise InventoryCollectError(NODE_MISSING_MESSAGE, reason="node_not_found")


def _inventory_error_exit(exc: BaseException, *, code: int) -> int:
    sys.stderr.write(f"error: {exc}\n")
    return code


_BUNDLE_CLI_EXIT_CODES = {
    "path_outside_workspace": 2,
    "map_invalid": 2,
    "config_invalid": 2,
    "bundle_not_found": 1,
    "bundle_too_large": 1,
    "collector_failed": 1,
    "refresh_in_progress": 1,
    "internal_error": 1,
}


def _bundle_error_exit(exc: ReviewServiceError) -> int:
    payload = {"error": {"code": exc.code, "message": exc.message, "details": exc.details}}
    sys.stdout.write(json.dumps(payload) + "\n")
    return _BUNDLE_CLI_EXIT_CODES.get(exc.code, 1)


def _run_ux_map_bundle(args: argparse.Namespace) -> int:
    workspace_root = (Path(args.workspace_root) if args.workspace_root else Path.cwd()).resolve()
    service = ReviewService(workspace_root=workspace_root, config=args.config)
    try:
        if args.bundle_command == "list":
            payload = {"bundles": [asdict(summary) for summary in service.list_bundles()]}
            sys.stdout.write(json.dumps(payload) + "\n")
            return 0
        if args.bundle_command == "get":
            sys.stdout.write(service.get_bundle(args.map_ref))
            return 0
        if args.bundle_command == "status":
            sys.stdout.write(json.dumps(asdict(service.get_bundle_status(args.map_ref))) + "\n")
            return 0
        if args.bundle_command == "refresh":
            result = service.refresh_bundle(
                args.map_ref,
                emit_handoff=args.emit_handoff,
                task_ref=args.task_ref,
                session=args.session,
            )
            payload = {
                "status": result.status,
                "digest": result.digest,
                "handoff_emit": result.handoff_emit,
            }
            sys.stdout.write(json.dumps(payload) + "\n")
            return 0
        raise SystemExit(f"unknown ux-map bundle command: {args.bundle_command!r}")
    except ReviewServiceError as exc:
        return _bundle_error_exit(exc)
    except Exception as exc:
        sys.stderr.write(f"ux-map bundle {args.bundle_command}: internal error ({type(exc).__name__})\n")
        return _bundle_error_exit(ReviewServiceError(code="internal_error", message="internal error"))


def _run_ux_map_inventory(args: argparse.Namespace) -> int:
    workspace_root = Path(args.workspace_root) if args.workspace_root else Path.cwd()
    config_rel = args.config or DEFAULT_CONFIG_PATH
    out_pointer = "/--out-dir" if args.out_dir else "/generated_dir"
    try:
        config_path = resolve_config_path(
            config_rel,
            workspace_root=workspace_root,
            pointer="/--config",
        )
        config = load_design_sources(config_path, workspace_root=workspace_root)
        out_rel = args.out_dir or config.generated_dir
        out_dir = resolve_config_path(out_rel, workspace_root=workspace_root, pointer=out_pointer)
        if args.collector is not None:
            collector = _resolve_explicit_collector(args.collector)
        else:
            collector = resolve_collector(None)
        _require_node()
        raw = run_collector(config, workspace_root=workspace_root, collector=collector)
        product = workspace_root.resolve().name
        doc = normalize_inventory(raw, config=config, workspace_root=workspace_root, product=product)
        if args.inventory_command == "collect":
            report = refresh_generated(out_dir=out_dir, doc=doc)
            if args.json:
                payload = {
                    "inventory": doc.model_dump(mode="json"),
                    "change_report": report.model_dump(mode="json"),
                }
                sys.stdout.write(json.dumps(payload) + "\n")
            else:
                for name in report.changed:
                    sys.stdout.write(f"{name}\n")
            return 0
        summary = check_generated(out_dir=out_dir, doc=doc)
        if summary is None:
            return 0
        sys.stderr.write(summary if summary.endswith("\n") else summary + "\n")
        return 1
    except InventoryConfigError as exc:
        return _inventory_error_exit(exc, code=2)
    except InventoryCollectError as exc:
        if getattr(exc, "reason", None) in {"node_not_found", "collector_not_found"}:
            return _inventory_error_exit(exc, code=2)
        return _inventory_error_exit(exc, code=1)
    except (TypeError, ValueError) as exc:
        return _inventory_error_exit(exc, code=1)
    except OSError as exc:
        return _inventory_error_exit(exc, code=1)


def _run_ux_map_project(args: argparse.Namespace) -> int:
    map_, _path = _load_map_from_ns(args)
    canvas = project_flow_to_canvas(map_, flow_id=args.flow_id)
    db = resolve_db_path(db_path=args.db_path, workspace_root=args.workspace_root)
    with CanvasStore(db) as store:
        persisted = store.upsert_canvas(canvas)
    sys.stdout.write(f"{persisted.canvas_ref}\n")
    return 0


def _run_experiment(args: argparse.Namespace) -> int:
    from dataclasses import asdict

    from workbay_canvas_mcp import lab_experiments as lab

    root = Path(args.workspace_root) if args.workspace_root else Path.cwd()
    try:
        if args.experiment_command == "show":
            loaded = lab.read_experiment(root, args.slug)
            result = {"document": loaded.document, "sha256": loaded.sha256}
        else:
            with Path(args.file).open("rb") as source:
                document = lab.decode_experiment(source.read(lab.MAX_FILE_BYTES + 1))
            result = asdict(lab.save_experiment(root, args.slug, document, if_match=args.if_match, create=args.create))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except lab.ExperimentConflict as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False))
        return 3
    except lab.ExperimentValidationError as exc:
        print(json.dumps({"error": exc.code, "pointer": exc.pointer, "message": str(exc)}))
        return 2
    except OSError as exc:
        print(json.dumps({"error": "io_error", "message": str(exc)}))
        return 1


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch serve (default) or ux-map subcommands."""

    args = _parse_args(argv)
    if args.command == "serve" or args.command is None:
        return _run_serve(args)
    if args.command == "local":
        return _run_local(args)
    if args.command == "experiment":
        return _run_experiment(args)
    if args.command == "ux-map":
        if args.ux_command == "render":
            return _run_ux_map_render(args)
        if args.ux_command == "critique":
            return _run_ux_map_critique(args)
        if args.ux_command == "project":
            return _run_ux_map_project(args)
        if args.ux_command == "migrate":
            return _run_ux_map_migrate(args)
        if args.ux_command == "source-check":
            return _run_ux_map_source_check(args)
        if args.ux_command == "inventory":
            return _run_ux_map_inventory(args)
        if args.ux_command == "bundle":
            return _run_ux_map_bundle(args)
        raise SystemExit(f"unknown ux-map command: {args.ux_command!r}")
    raise SystemExit(f"unknown command: {args.command!r}")


def workbay_front_door(argv: Sequence[str] | None = None) -> int:
    """Dispatch ``workbay canvas``: flags-only argv means ``local``."""

    print("workbay canvas is now workbay studio", file=sys.stderr)
    raw = list(sys.argv[1:] if argv is None else argv)
    if not raw or raw[0].startswith("-"):
        return main(["local", *raw])
    return main(raw)


def workbay_studio_front_door(argv: Sequence[str] | None = None) -> int:
    """Launch Studio destinations or pass experiment commands through unchanged."""
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw and raw[0] in {"flows", "library", "lab"}:
        return main(["local", "--view", raw[0], *raw[1:]])
    if not raw or raw[0].startswith("-"):
        return main(["local", *raw])
    return main(raw)


def serve_fleet(argv: Sequence[str] | None = None) -> int:
    """Public Fleet launcher sharing the local server, without a Studio destination."""
    args = _parse_args(["local", *list(sys.argv[1:] if argv is None else argv)])
    args.view = "fleet"
    return _run_local(args)


def ux_map_main(argv: Sequence[str] | None = None) -> int:
    """Dispatch the dedicated ``ux-map`` console script."""

    raw = list(sys.argv[1:] if argv is None else argv)
    return main(["ux-map", *raw])


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
