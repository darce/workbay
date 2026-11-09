"""JSON Schema export from Pydantic SSOT (committed artifact for tooling).

Regenerate after model changes::

    python -c 'from workbay_canvas_mcp.design_inventory.schema_export import write_committed_schema; write_committed_schema()'
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from workbay_canvas_mcp.design_inventory.models import InventoryDocument

_SCHEMA_PATH = Path(__file__).resolve().parent / "schema" / "inventory.schema.json"


def model_schema() -> dict[str, Any]:
    return InventoryDocument.model_json_schema()


def write_committed_schema(path: Path | None = None) -> Path:
    target = path or _SCHEMA_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(model_schema(), indent=2) + "\n", encoding="utf-8")
    return target


def load_committed_schema(path: Path | None = None) -> dict[str, Any]:
    target = path or _SCHEMA_PATH
    return json.loads(target.read_text(encoding="utf-8"))
