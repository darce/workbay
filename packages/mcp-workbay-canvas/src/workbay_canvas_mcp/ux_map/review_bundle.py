"""Deterministic derived review bundle from a UX map (WBC-01-REVIEW-DEMO)."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from workbay_canvas_mcp.ux_map.critique import CANON_REVISION, RULE_PACK_REVISION, critique_map
from workbay_canvas_mcp.ux_map.models import UxMap
from workbay_canvas_mcp.ux_map.render_ascii import (
    ASCII_LAYOUT_VERSION,
    FLOW_ASCII_VERSION,
    render_ascii_layout,
    render_flow_ascii,
)
from workbay_canvas_mcp.ux_map.render_markdown import MARKDOWN_RENDER_VERSION, render_markdown_bundle
from workbay_canvas_mcp.ux_map.render_mermaid import MERMAID_RENDER_VERSION, render_flow_mermaid
from workbay_canvas_mcp.ux_map.source_check import (
    CodemapResolver,
    SourceCheckReport,
    source_report_to_dict,
    verify_code_refs,
)

if TYPE_CHECKING:
    from workbay_canvas_mcp.design_inventory.models import InventoryDocument

REVIEW_BUNDLE_VERSION = 1
MAX_SCREENS = 10
MAX_FLOWS = 10
MAX_FLOW_STEPS = 1000
MAX_BUNDLE_BYTES = 256 * 1024
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _require_safe_id(value: str, field: str) -> None:
    if SAFE_ID.fullmatch(value) is None:
        raise ValueError(f"{field} must match {SAFE_ID.pattern}")


def normalized_map_bytes(map_: UxMap) -> bytes:
    return json.dumps(
        map_.model_dump(mode="json", exclude_none=True, exclude={"schema_version"}),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def dumps_review_bundle(bundle: dict) -> str:
    return json.dumps(bundle, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def build_review_bundle(
    map_: UxMap,
    *,
    workspace_root: Path | None = None,
    previous_source_report: SourceCheckReport | None = None,
    codemap: CodemapResolver | None = None,
    inventory: InventoryDocument | None = None,
) -> dict:
    if len(map_.screens) > MAX_SCREENS:
        raise ValueError(f"maps may have at most {MAX_SCREENS} screens (got {len(map_.screens)})")
    if len(map_.flows) > MAX_FLOWS:
        raise ValueError(f"maps may have at most {MAX_FLOWS} flows (got {len(map_.flows)})")
    if sum(len(flow.steps) for flow in map_.flows) > MAX_FLOW_STEPS:
        raise ValueError(f"maps may have at most {MAX_FLOW_STEPS} total flow steps")
    if len(map_.screens) == 0:
        raise ValueError("maps must have at least 1 screen")
    _require_safe_id(map_.map_ref, "map_ref")
    for index, screen in enumerate(map_.screens):
        _require_safe_id(screen.id, f"screens[{index}].id")
    for index, flow in enumerate(map_.flows):
        _require_safe_id(flow.id, f"flows[{index}].id")
        for step_index, step in enumerate(flow.steps):
            _require_safe_id(step.screen_id, f"flows[{index}].steps[{step_index}].screen_id")

    screens = [
        {
            "id": screen.id,
            "title": screen.title,
            "ascii": render_ascii_layout(map_, screen_id=screen.id),
            "states": [state.value for state in screen.states],
            "zones": [{"id": zone.id, "label": zone.label} for zone in screen.zones],
        }
        for screen in map_.screens
    ]
    flows = [
        {
            "id": flow.id,
            "label": flow.label if flow.label else flow.id,
            "mermaid": render_flow_mermaid(map_, flow_id=flow.id),
            "ascii": render_flow_ascii(map_, flow_id=flow.id),
            "steps": [{"screen_id": step.screen_id, "branch_label": step.branch_label} for step in flow.steps],
        }
        for flow in map_.flows
    ]
    digest = "sha256:" + hashlib.sha256(normalized_map_bytes(map_)).hexdigest()
    bundle = {
        "bundle_version": REVIEW_BUNDLE_VERSION,
        "map_ref": map_.map_ref,
        "product": map_.product,
        "source_digest": digest,
        "renderer_versions": {
            "ascii_layout": ASCII_LAYOUT_VERSION,
            "flow_ascii": FLOW_ASCII_VERSION,
            "flow_mermaid": MERMAID_RENDER_VERSION,
            "markdown": MARKDOWN_RENDER_VERSION,
        },
        "rule_pack_revision": RULE_PACK_REVISION,
        "canon_revision": CANON_REVISION,
        "screens": screens,
        "flows": flows,
        "markdown": render_markdown_bundle(map_),
        "source_report": source_report_to_dict(
            verify_code_refs(
                map_,
                workspace_root=workspace_root,
                codemap=codemap,
                previous=previous_source_report,
            )
        ),
        "findings": [asdict(finding) for finding in critique_map(map_)],
    }
    if inventory is not None:
        from workbay_canvas_mcp.design_inventory import models as inventory_models

        bundle["inventory"] = inventory_models.InventoryDocument.model_dump(inventory, mode="json")
    serialized = dumps_review_bundle(bundle).encode("utf-8")
    if len(serialized) > MAX_BUNDLE_BYTES:
        raise ValueError(f"review bundle exceeds 256 KiB ({MAX_BUNDLE_BYTES} bytes; got {len(serialized)})")
    return bundle
