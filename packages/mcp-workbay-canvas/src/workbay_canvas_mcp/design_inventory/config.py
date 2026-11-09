"""Parse, resolve, and bound design-sources.json (WBC-03 S5-GREEN-CONFIG)."""

from __future__ import annotations

import json
import posixpath
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, ValidationError, field_validator

from workbay_canvas_mcp.component_roster import SlotSpec
from workbay_canvas_mcp.design_inventory.errors import InventoryConfigError
from workbay_canvas_mcp.ux_map.models import _StrictModel
from workbay_canvas_mcp.ux_map.parse import json_pointer_from_loc

DEFAULT_CONFIG_PATH = "docs/design/design-sources.json"
DEFAULT_GENERATED_DIR = "docs/design/generated"

MAX_MAPS = 100
MAX_COMPONENTS = 500
MAX_CSS_INPUTS = 50
MAX_ASSET_ROOTS = 20
MAX_TOKENS = 2000
MAX_ASSETS = 1000
MAX_FILES = 200
MAX_CONFIG_BYTES = 256 * 1024
MAX_SOURCE_FILE_BYTES = 1 << 20
MAX_CSS_BYTES = 8 << 20
MAX_ASSET_FILE_BYTES = 8 << 20
MAX_TOTAL_READ_BYTES = 20 << 20
MAX_INVENTORY_JSON_BYTES = 2 << 20
_MAX_TOKEN_REFS = 50

_SCHEMA_PATH = Path(__file__).resolve().parent / "schema" / "design_sources.schema.json"


class ProvenanceInput(_StrictModel):
    path: str
    sha256: str


class CompiledCssProvenance(_StrictModel):
    inputs: list[ProvenanceInput]
    output_digest: str


class CompiledCssInput(_StrictModel):
    output: str
    provenance: CompiledCssProvenance


class ExampleRef(_StrictModel):
    component_id: str
    path: str


class Attribution(_StrictModel):
    path: str
    license: str
    credit: str


class DeclaredComponent(_StrictModel):
    id: str
    export_name: str
    module_path: str
    description: str
    slots: list[SlotSpec]
    example_props: dict[str, Any]
    token_refs: list[str] = Field(default_factory=list, max_length=_MAX_TOKEN_REFS)


class DesignSources(_StrictModel):
    config_version: Literal[1]
    maps: list[str]
    components: list[DeclaredComponent]
    css_inputs: list[str]
    asset_roots: list[str]
    compiled_css: list[CompiledCssInput] = Field(default_factory=list)
    examples: list[ExampleRef] = Field(default_factory=list)
    attributions: list[Attribution] = Field(default_factory=list)
    generated_dir: str = DEFAULT_GENERATED_DIR

    @field_validator("config_version", mode="before")
    @classmethod
    def _config_version_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("config_version must not be a boolean")
        if isinstance(value, (int, float)) and value == 1:
            return 1
        return value


def parse_design_sources(raw: object) -> DesignSources:
    if not isinstance(raw, dict):
        raise InventoryConfigError(
            pointer="",
            code="invalid",
            message="design-sources document must be a JSON object",
        )
    if "config_version" not in raw:
        raise InventoryConfigError(
            pointer="/config_version",
            code="invalid",
            message="config_version is required",
        )
    version = raw["config_version"]
    if not (isinstance(version, (int, float)) and not isinstance(version, bool) and version == 1):
        raise InventoryConfigError(
            pointer="/config_version",
            code="unknown_version",
            message=f"unsupported config_version {version!r}",
        )
    if type(version) is int:
        payload = raw
    else:
        payload = dict(raw)
        payload["config_version"] = 1
    try:
        model = DesignSources.model_validate(payload)
    except ValidationError as exc:
        errors = exc.errors()
        err = errors[0] if errors else None
        if err is None:
            raise InventoryConfigError(pointer="", code="invalid", message=str(exc)) from exc
        loc = tuple(err.get("loc", ()))
        pointer = json_pointer_from_loc(loc)
        code = "unknown_field" if err.get("type") == "extra_forbidden" else "invalid"
        message = str(err.get("msg") or exc)
        raise InventoryConfigError(pointer=pointer, code=code, message=message) from exc

    seen: dict[str, int] = {}
    for index, component in enumerate(model.components):
        previous = seen.get(component.id)
        if previous is not None:
            raise InventoryConfigError(
                pointer=f"/components/{index}/id",
                code="duplicate_id",
                message=f"duplicate component id {component.id!r}",
            )
        seen[component.id] = index

    _check_count(model.maps, MAX_MAPS, "MAX_MAPS", "/maps")
    _check_count(model.components, MAX_COMPONENTS, "MAX_COMPONENTS", "/components")
    _check_count(model.css_inputs, MAX_CSS_INPUTS, "MAX_CSS_INPUTS", "/css_inputs")
    _check_count(model.asset_roots, MAX_ASSET_ROOTS, "MAX_ASSET_ROOTS", "/asset_roots")

    for rel, pointer in _iter_path_fields(model):
        error = _syntactic_path_error(rel)
        if error is not None:
            raise InventoryConfigError(pointer=pointer, code="escape", message=error)
    return model


def resolve_config_path(rel: str, *, workspace_root: Path, pointer: str) -> Path:
    error = _syntactic_path_error(rel)
    if error is not None:
        raise InventoryConfigError(pointer=pointer, code="escape", message=error)
    root = workspace_root.resolve()
    try:
        resolved = (root / rel).resolve()
    except (RuntimeError, OSError) as exc:
        raise InventoryConfigError(
            pointer=pointer,
            code="escape",
            message="path cannot be resolved",
        ) from exc
    if not resolved.is_relative_to(root):
        raise InventoryConfigError(
            pointer=pointer,
            code="escape",
            message="path escapes workspace",
        )
    return resolved


def load_design_sources(path: Path, *, workspace_root: Path) -> DesignSources:
    root = workspace_root.resolve()
    try:
        real = path.resolve()
    except (RuntimeError, OSError) as exc:
        raise InventoryConfigError(
            pointer="",
            code="escape",
            message="config path cannot be resolved",
        ) from exc
    if not real.is_relative_to(root):
        raise InventoryConfigError(
            pointer="",
            code="escape",
            message="config path escapes workspace",
        )
    try:
        size = real.stat().st_size
    except OSError as exc:
        raise InventoryConfigError(
            pointer="",
            code="invalid",
            message=f"cannot stat config file: {exc}",
        ) from exc
    if size > MAX_CONFIG_BYTES:
        raise InventoryConfigError(
            pointer="",
            code="oversized",
            message=f"config exceeds MAX_CONFIG_BYTES ({MAX_CONFIG_BYTES})",
        )
    try:
        raw = json.loads(real.read_bytes())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InventoryConfigError(
            pointer="",
            code="invalid",
            message="invalid JSON",
        ) from exc
    model = parse_design_sources(raw)
    for rel, pointer in _iter_path_fields(model):
        error = _syntactic_path_error(rel)
        if error is not None:
            raise InventoryConfigError(pointer=pointer, code="escape", message=error)
        error = _canonical_path_error(rel)
        if error is not None:
            raise InventoryConfigError(pointer=pointer, code="invalid", message=error)
        resolve_config_path(rel, workspace_root=workspace_root, pointer=pointer)
    return model


def design_sources_model_schema() -> dict[str, Any]:
    return DesignSources.model_json_schema()


def write_design_sources_schema(path: Path | None = None) -> Path:
    target = path or _SCHEMA_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(design_sources_model_schema(), indent=2) + "\n", encoding="utf-8")
    return target


def load_design_sources_schema(path: Path | None = None) -> dict[str, Any]:
    target = path or _SCHEMA_PATH
    return json.loads(target.read_text(encoding="utf-8"))


def _check_count(items: list[Any], limit: int, name: str, pointer: str) -> None:
    if len(items) > limit:
        raise InventoryConfigError(
            pointer=pointer,
            code="oversized",
            message=f"{name} exceeded ({limit})",
        )


def _syntactic_path_error(path: str) -> str | None:
    if "\0" in path:
        return "path contains NUL"
    if "://" in path:
        return "path is a URL"
    if not path:
        return "path is empty"
    candidate = Path(path)
    if candidate.is_absolute() or path.startswith("/") or path.startswith("\\"):
        return "path is absolute"
    if ".." in candidate.parts:
        return "path is not workspace-relative"
    return None


def _canonical_path_error(path: str) -> str | None:
    segments = path.split("/")
    if "\\" in path or "" in segments or "." in segments:
        display = posixpath.normpath(path.replace("\\", "/"))
        return f"path is not canonical (use {display!r})"
    return None


def _iter_path_fields(model: DesignSources) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for index, rel in enumerate(model.maps):
        items.append((rel, f"/maps/{index}"))
    for index, component in enumerate(model.components):
        items.append((component.module_path, f"/components/{index}/module_path"))
    for index, rel in enumerate(model.css_inputs):
        items.append((rel, f"/css_inputs/{index}"))
    for index, rel in enumerate(model.asset_roots):
        items.append((rel, f"/asset_roots/{index}"))
    for index, row in enumerate(model.compiled_css):
        items.append((row.output, f"/compiled_css/{index}/output"))
        for input_index, provenance_input in enumerate(row.provenance.inputs):
            items.append(
                (
                    provenance_input.path,
                    f"/compiled_css/{index}/provenance/inputs/{input_index}/path",
                )
            )
    for index, example in enumerate(model.examples):
        items.append((example.path, f"/examples/{index}/path"))
    for index, attribution in enumerate(model.attributions):
        items.append((attribution.path, f"/attributions/{index}/path"))
    items.append((model.generated_dir, "/generated_dir"))
    return items
