"""Pydantic SSOT for inventory.json (WBC-03 S6-GREEN-SCHEMA)."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import (
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    ValidationError,
    field_validator,
    model_serializer,
    model_validator,
)

from workbay_canvas_mcp.component_roster import SlotSpec
from workbay_canvas_mcp.ux_map.models import _StrictModel
from workbay_canvas_mcp.ux_map.parse import json_pointer_from_loc

ALLOWED_ASSET_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".avif": "image/avif",
    ".svg": "image/svg+xml",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
}

_COVERAGE_LIMITATIONS: tuple[CoverageLimitation, ...] = (
    "compiled_css_requires_provenance",
    "css_custom_properties_only",
    "no_usage_inference",
    "registered_components_only",
)

InventorySchemaCode = Literal["unknown_version", "unknown_field", "invalid"]

CoverageLimitation = Literal[
    "registered_components_only",
    "css_custom_properties_only",
    "no_usage_inference",
    "compiled_css_requires_provenance",
]


class InventorySchemaError(ValueError):
    """Inventory document rejected at a JSON Pointer (RFC 6901)."""

    def __init__(
        self,
        *,
        pointer: str,
        code: InventorySchemaCode,
        message: str,
    ) -> None:
        super().__init__(f"{code} at {pointer or '(root)'}: {message}")
        self.pointer = pointer
        self.code = code
        self.message = message


class SourceDigest(_StrictModel):
    path: str
    digest: str


class CompiledCssFreshness(_StrictModel):
    output: str
    status: Literal["verified", "stale", "unsupported"]
    reason: Literal["no_provenance", "input_changed", "output_changed", "input_missing"] | None
    stale_paths: list[str]

    @model_validator(mode="after")
    def _reason_matches_status(self) -> Self:
        if (self.reason is None) != (self.status == "verified"):
            raise ValueError("reason is None iff status is verified")
        self.stale_paths = sorted(self.stale_paths)
        return self


class Coverage(_StrictModel):
    registered_components: int
    verified_exports: int
    unresolved_exports: int
    css_inputs: int
    tokens: int
    unsupported_tokens: int
    assets: int
    missing_assets: int
    oversized_assets: int
    skipped_asset_files: int
    limitations: list[CoverageLimitation]

    @model_validator(mode="after")
    def _limitations_complete_sorted(self) -> Self:
        expected = sorted(_COVERAGE_LIMITATIONS)
        if set(self.limitations) != set(_COVERAGE_LIMITATIONS) or len(self.limitations) != 4:
            raise ValueError("coverage.limitations must contain all four MVP limitations")
        self.limitations = expected
        return self


class TokenResolvedValue(_StrictModel):
    kind: Literal["color", "length", "number"]
    value: str


class TokenResolvedFontFamily(_StrictModel):
    kind: Literal["font_family"]
    families: list[str] = Field(min_length=1)
    generic: str | None = None

    @model_serializer(mode="wrap")
    def _omit_absent_generic(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.generic is None:
            data.pop("generic", None)
        return data


class TokenResolvedUnsupported(_StrictModel):
    kind: Literal["unsupported"]
    reason: Literal[
        "alias_cycle",
        "runtime_expression",
        "url_expression",
        "unresolved_alias",
        "not_scalar",
        "font_shorthand",
    ]


TokenResolved = Annotated[
    TokenResolvedValue | TokenResolvedFontFamily | TokenResolvedUnsupported,
    Field(discriminator="kind"),
]


class InventoryToken(_StrictModel):
    name: str
    expression: str
    context: str
    source_ref: str
    resolved: TokenResolved
    status: Literal["resolved", "unsupported"]


class ExportRef(_StrictModel):
    path: str
    export_name: str
    status: Literal["verified", "unresolved"]


class InventoryComponent(_StrictModel):
    id: str
    name: str
    description: str | None
    export_ref: ExportRef
    declared_props: dict[str, str]
    slots: list[SlotSpec]
    token_refs: list[str]
    unresolved_token_refs: list[str]
    example_refs: list[str]


class InventoryAsset(_StrictModel):
    path: str
    mime: str
    width: int | None
    height: int | None
    digest: str | None
    bytes: int | None
    license: str
    references: list[str]
    status: Literal["present", "missing", "oversized"]


class FontWeight(_StrictModel):
    min: float = Field(ge=1, le=1000)
    max: float = Field(ge=1, le=1000)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.min > self.max:
            raise ValueError("font weight min exceeds max")
        return self


class FontSource(_StrictModel):
    asset_path: str
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    format: str | None = None

    @model_serializer(mode="wrap")
    def _omit_absent_format(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.format is None:
            data.pop("format", None)
        return data


class InventoryFontFace(_StrictModel):
    family: str = Field(min_length=1)
    weight: FontWeight
    style: str
    unicode_range: str | None = None
    display: str | None = None
    src: list[FontSource] | None = Field(min_length=1)
    status: Literal["resolved", "unresolved"]
    reason: str | None = None

    @model_validator(mode="after")
    def _resolution_matches_sources(self) -> Self:
        if (self.status == "resolved") != (self.src is not None):
            raise ValueError("font face is resolved iff src is nonempty")
        if self.status == "unresolved" and not self.reason:
            raise ValueError("unresolved font face requires a reason")
        return self

    @model_serializer(mode="wrap")
    def _omit_absent_descriptors(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        for key in ("unicode_range", "display", "reason"):
            if getattr(self, key) is None:
                data.pop(key, None)
        return data


class InventoryDocument(_StrictModel):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {"properties": {"inventory_version": {"const": 1}}, "not": {"required": ["font_faces"]}},
                {"properties": {"inventory_version": {"const": 2}}, "required": ["font_faces"]},
            ],
        }
    )

    inventory_version: Literal[1, 2]
    product: str
    source_digest: str
    coverage: Coverage
    source_digests: list[SourceDigest]
    compiled_css_freshness: list[CompiledCssFreshness]
    tokens: list[InventoryToken]
    components: list[InventoryComponent]
    assets: list[InventoryAsset]
    font_faces: list[InventoryFontFace] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _versioned_faces(cls, value: Any) -> Any:
        if isinstance(value, dict):
            version = value.get("inventory_version")
            if version == 1 and "font_faces" in value:
                raise ValueError("v1 inventory must not carry font_faces")
            if version == 2 and "font_faces" not in value:
                raise ValueError("v2 inventory requires font_faces")
        return value

    @model_serializer(mode="wrap")
    def _versioned_dump(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.inventory_version == 1:
            data.pop("font_faces", None)
        return data

    @field_validator("inventory_version", mode="before")
    @classmethod
    def _reject_bool_version(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("inventory_version must be integer 1 or 2")
        if isinstance(value, (int, float)) and value in (1, 2):
            return int(value)
        return value


_TOKEN_RESOLVED_TAGS = frozenset({"color", "length", "number", "font_family", "unsupported"})


def _pointer_loc(loc: tuple[object, ...]) -> tuple[str | int, ...]:
    """Drop tagged-union discriminator values pydantic inserts into error loc."""
    out: list[str | int] = []
    for token in loc:
        if out and out[-1] == "resolved" and token in _TOKEN_RESOLVED_TAGS:
            continue
        if isinstance(token, (str, int)):
            out.append(token)
        else:
            out.append(str(token))
    return tuple(out)


def parse_inventory(raw: object) -> InventoryDocument:
    if not isinstance(raw, dict):
        raise InventorySchemaError(
            pointer="",
            code="invalid",
            message="inventory document must be a JSON object",
        )
    if "inventory_version" not in raw:
        raise InventorySchemaError(
            pointer="/inventory_version",
            code="invalid",
            message="inventory_version is required",
        )
    version = raw["inventory_version"]
    if not (isinstance(version, (int, float)) and not isinstance(version, bool) and version in (1, 2)):
        raise InventorySchemaError(
            pointer="/inventory_version",
            code="unknown_version",
            message=f"unsupported inventory_version {version!r}",
        )
    if version == 1 and "font_faces" in raw:
        raise InventorySchemaError(
            pointer="/font_faces", code="unknown_field", message="v1 inventory must not carry font_faces"
        )
    if version == 2 and "font_faces" not in raw:
        raise InventorySchemaError(pointer="/font_faces", code="invalid", message="v2 inventory requires font_faces")
    if type(version) is int:
        payload = raw
    else:
        payload = dict(raw)
        payload["inventory_version"] = int(version)
    try:
        return InventoryDocument.model_validate(payload)
    except ValidationError as exc:
        errors = exc.errors()
        err = errors[0] if errors else None
        if err is None:
            raise InventorySchemaError(pointer="", code="invalid", message=str(exc)) from exc
        loc = tuple(err.get("loc", ()))
        pointer = json_pointer_from_loc(_pointer_loc(loc))
        code: InventorySchemaCode = "unknown_field" if err.get("type") == "extra_forbidden" else "invalid"
        message = str(err.get("msg") or exc)
        raise InventorySchemaError(pointer=pointer, code=code, message=message) from exc
