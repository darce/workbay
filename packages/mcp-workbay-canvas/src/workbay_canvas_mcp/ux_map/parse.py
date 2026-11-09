"""Parse UX-map documents with JSON Pointer errors (WBC-02 C03)."""

from __future__ import annotations

from typing import Literal

from pydantic import ValidationError

from workbay_canvas_mcp.ux_map.models import UxMap

MapSchemaCode = Literal["unknown_version", "unknown_field", "invalid"]


class MapSchemaError(ValueError):
    """Map document rejected at a JSON Pointer (RFC 6901)."""

    def __init__(
        self,
        *,
        pointer: str,
        code: MapSchemaCode,
        message: str,
    ) -> None:
        super().__init__(f"{code} at {pointer or '(root)'}: {message}")
        self.pointer = pointer
        self.code = code
        self.message = message


def json_pointer_from_loc(loc: tuple[str | int, ...]) -> str:
    """Return "" for (); else "/" + "/".join(tokens); str tokens escape "~" -> "~0" then "/" -> "~1"."""
    if not loc:
        return ""
    tokens: list[str] = []
    for token in loc:
        if isinstance(token, str):
            tokens.append(token.replace("~", "~0").replace("/", "~1"))
        else:
            tokens.append(str(token))
    return "/" + "/".join(tokens)


def parse_map(raw: object) -> UxMap:
    if not isinstance(raw, dict):
        raise MapSchemaError(
            pointer="",
            code="invalid",
            message="map document must be a JSON object",
        )
    if "schema_version" in raw:
        version = raw["schema_version"]
        if not (isinstance(version, (int, float)) and not isinstance(version, bool) and version == 1):
            raise MapSchemaError(
                pointer="/schema_version",
                code="unknown_version",
                message=f"unsupported schema_version {version!r}",
            )
        if type(version) is int:
            payload = raw
        else:
            payload = dict(raw)
            payload["schema_version"] = 1
    else:
        payload = dict(raw)
        payload.setdefault("schema_version", 1)
    try:
        return UxMap.model_validate(payload)
    except ValidationError as exc:
        errors = exc.errors()
        err = errors[0] if errors else None
        if err is None:
            raise MapSchemaError(pointer="", code="invalid", message=str(exc)) from exc
        loc = tuple(err.get("loc", ()))
        pointer = json_pointer_from_loc(loc)
        code: MapSchemaCode = "unknown_field" if err.get("type") == "extra_forbidden" else "invalid"
        message = str(err.get("msg") or exc)
        raise MapSchemaError(pointer=pointer, code=code, message=message) from exc
