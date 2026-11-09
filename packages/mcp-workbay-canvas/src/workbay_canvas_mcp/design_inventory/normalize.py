"""Normalize collector raw JSON into a validated InventoryDocument (WBC-03 S8)."""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import unquote, urlsplit

from workbay_canvas_mcp.design_inventory.adapter import component_from_declared
from workbay_canvas_mcp.design_inventory.config import (
    MAX_CSS_BYTES,
    MAX_SOURCE_FILE_BYTES,
    MAX_TOKENS,
    MAX_TOTAL_READ_BYTES,
    DesignSources,
    resolve_config_path,
)
from workbay_canvas_mcp.design_inventory.models import (
    ALLOWED_ASSET_TYPES,
    CompiledCssFreshness,
    Coverage,
    CoverageLimitation,
    ExportRef,
    FontSource,
    FontWeight,
    InventoryAsset,
    InventoryComponent,
    InventoryDocument,
    InventoryFontFace,
    InventoryToken,
    SourceDigest,
    TokenResolved,
    TokenResolvedFontFamily,
    TokenResolvedUnsupported,
    TokenResolvedValue,
    parse_inventory,
)

_COVERAGE_LIMITATIONS: tuple[CoverageLimitation, ...] = (
    "compiled_css_requires_provenance",
    "css_custom_properties_only",
    "no_usage_inference",
    "registered_components_only",
)

_VAR_RE = re.compile(r"^var\(\s*(--[A-Za-z0-9_-]+)\s*\)$")
_HEX_RE = re.compile(r"^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$")
_LENGTH_RE = re.compile(r"^-?(?:\d*\.\d+|\d+)(?:px|rem|em|%|vh|vw)$")
_NUMBER_RE = re.compile(r"^-?(?:\d*\.\d+|\d+)$")
_FUNC_RE = re.compile(r"^(rgb|rgba|hsl|hsla|oklch)\(\s*(.+)\s*\)$", re.IGNORECASE)
_NUMERIC_TOKEN_RE = re.compile(r"^-?(?:\d*\.\d+|\d+)(?:[eE][+-]?\d+)?%?$")
_CSS_ESCAPE = r"\\(?:[0-9a-fA-F]{1,6}\s?|[^\r\n\f0-9a-fA-F])"
_CSS_STRING = rf"""(?:"(?:[^"\\\r\n\f]|{_CSS_ESCAPE})*"|'(?:[^'\\\r\n\f]|{_CSS_ESCAPE})*')"""
_CSS_IDENT = rf"-?(?:[a-zA-Z_\u0080-\uffff]|{_CSS_ESCAPE})(?:[a-zA-Z_0-9\-\u0080-\uffff]|{_CSS_ESCAPE})*"
_FAMILY_MEMBER = re.compile(rf"({_CSS_STRING})|({_CSS_IDENT}(?:\s+{_CSS_IDENT})*)")
_GENERIC_FAMILIES = frozenset(
    {
        "serif",
        "sans-serif",
        "monospace",
        "cursive",
        "fantasy",
        "system-ui",
        "ui-serif",
        "ui-sans-serif",
        "ui-monospace",
        "ui-rounded",
        "emoji",
        "math",
        "fangsong",
    }
)
_CSS_GLOBALS = frozenset({"inherit", "initial", "unset", "revert", "revert-layer", "default"})
_FONT_SHORTHAND = re.compile(
    r"^(?:(?:normal|italic|oblique|small-caps|bold|bolder|lighter|[1-9]\d*(?:\.\d+)?|[\w-]*condensed|[\w-]*expanded)\s+)*"
    r"(?:\d*\.?\d+(?:px|rem|em|%|pt|pc|in|cm|mm|ex|ch|vw|vh)|xx-small|x-small|small|medium|large|x-large|xx-large|xxx-large|smaller|larger)"
    r"(?:\s*/\s*(?:normal|\d*\.?\d+(?:[a-z%]+)?))?\s+.+$",
    re.IGNORECASE,
)

_NAMED_COLORS = frozenset(
    {
        "aliceblue",
        "antiquewhite",
        "aqua",
        "aquamarine",
        "azure",
        "beige",
        "bisque",
        "black",
        "blanchedalmond",
        "blue",
        "blueviolet",
        "brown",
        "burlywood",
        "cadetblue",
        "chartreuse",
        "chocolate",
        "coral",
        "cornflowerblue",
        "cornsilk",
        "crimson",
        "cyan",
        "darkblue",
        "darkcyan",
        "darkgoldenrod",
        "darkgray",
        "darkgreen",
        "darkgrey",
        "darkkhaki",
        "darkmagenta",
        "darkolivegreen",
        "darkorange",
        "darkorchid",
        "darkred",
        "darksalmon",
        "darkseagreen",
        "darkslateblue",
        "darkslategray",
        "darkslategrey",
        "darkturquoise",
        "darkviolet",
        "deeppink",
        "deepskyblue",
        "dimgray",
        "dimgrey",
        "dodgerblue",
        "firebrick",
        "floralwhite",
        "forestgreen",
        "fuchsia",
        "gainsboro",
        "ghostwhite",
        "gold",
        "goldenrod",
        "gray",
        "green",
        "greenyellow",
        "grey",
        "honeydew",
        "hotpink",
        "indianred",
        "indigo",
        "ivory",
        "khaki",
        "lavender",
        "lavenderblush",
        "lawngreen",
        "lemonchiffon",
        "lightblue",
        "lightcoral",
        "lightcyan",
        "lightgoldenrodyellow",
        "lightgray",
        "lightgreen",
        "lightgrey",
        "lightpink",
        "lightsalmon",
        "lightseagreen",
        "lightskyblue",
        "lightslategray",
        "lightslategrey",
        "lightsteelblue",
        "lightyellow",
        "lime",
        "limegreen",
        "linen",
        "magenta",
        "maroon",
        "mediumaquamarine",
        "mediumblue",
        "mediumorchid",
        "mediumpurple",
        "mediumseagreen",
        "mediumslateblue",
        "mediumspringgreen",
        "mediumturquoise",
        "mediumvioletred",
        "midnightblue",
        "mintcream",
        "mistyrose",
        "moccasin",
        "navajowhite",
        "navy",
        "oldlace",
        "olive",
        "olivedrab",
        "orange",
        "orangered",
        "orchid",
        "palegoldenrod",
        "palegreen",
        "paleturquoise",
        "palevioletred",
        "papayawhip",
        "peachpuff",
        "peru",
        "pink",
        "plum",
        "powderblue",
        "purple",
        "rebeccapurple",
        "red",
        "rosybrown",
        "royalblue",
        "saddlebrown",
        "salmon",
        "sandybrown",
        "seagreen",
        "seashell",
        "sienna",
        "silver",
        "skyblue",
        "slateblue",
        "slategray",
        "slategrey",
        "snow",
        "springgreen",
        "steelblue",
        "tan",
        "teal",
        "thistle",
        "tomato",
        "transparent",
        "turquoise",
        "violet",
        "wheat",
        "white",
        "whitesmoke",
        "yellow",
        "yellowgreen",
    }
)


def normalize_inventory(
    raw: dict,
    *,
    config: DesignSources,
    workspace_root: Path,
    product: str,
) -> InventoryDocument:
    if not isinstance(raw, dict):
        raise TypeError("collector raw payload must be an object")

    token_rows = _require_list(raw.get("tokens", []), "tokens")
    if len(token_rows) > MAX_TOKENS:
        raise ValueError(f"MAX_TOKENS exceeded ({MAX_TOKENS})")
    tokens = _normalize_tokens(token_rows)
    known_tokens = {token.name for token in tokens}

    export_rows = _require_list(raw.get("exports", []), "exports")
    export_index = _index_exports(export_rows)
    examples_by_id: dict[str, list[str]] = {}
    for example in config.examples:
        examples_by_id.setdefault(example.component_id, []).append(example.path)

    components: list[InventoryComponent] = []
    for declared in config.components:
        key = (declared.export_name, declared.module_path)
        export_row = export_index.get(key)
        if export_row is None:
            export = ExportRef(
                path=declared.module_path,
                export_name=declared.export_name,
                status="unresolved",
            )
            props: dict[str, str] = {}
        else:
            status = export_row.get("status")
            if status not in ("verified", "unresolved"):
                status = "unresolved"
            export = ExportRef(
                path=declared.module_path,
                export_name=declared.export_name,
                status=cast(Literal["verified", "unresolved"], status),
            )
            props = _string_props(export_row.get("props"))
        component = component_from_declared(
            declared,
            export=export,
            known_tokens=known_tokens,
            declared_props=props,
        )
        example_refs = list(examples_by_id.get(declared.id, []))
        if example_refs:
            component = component.model_copy(update={"example_refs": example_refs})
        components.append(component)
    components.sort(key=lambda row: row.id)

    source_digests = _normalize_source_digests(raw.get("source_digests", []))
    assets = _normalize_assets(raw.get("assets", []), config=config, workspace_root=workspace_root)
    font_faces = _normalize_font_faces(
        raw.get("font_faces", []), assets=assets, config=config, workspace_root=workspace_root
    )
    freshness = _compiled_css_freshness(config, workspace_root=workspace_root)

    skipped_asset_files = raw.get("skipped_asset_files", 0)
    if skipped_asset_files is None:
        skipped_asset_files = 0
    if type(skipped_asset_files) is bool or not isinstance(skipped_asset_files, int):
        raise TypeError("skipped_asset_files must be an int")

    verified_exports = sum(1 for row in components if row.export_ref.status == "verified")
    unresolved_exports = sum(1 for row in components if row.export_ref.status == "unresolved")
    coverage = Coverage(
        registered_components=len(config.components),
        verified_exports=verified_exports,
        unresolved_exports=unresolved_exports,
        css_inputs=len(config.css_inputs),
        tokens=len(tokens),
        unsupported_tokens=sum(1 for token in tokens if token.status == "unsupported"),
        assets=len(assets),
        missing_assets=sum(1 for asset in assets if asset.status == "missing"),
        oversized_assets=sum(1 for asset in assets if asset.status == "oversized"),
        skipped_asset_files=skipped_asset_files,
        limitations=list(_COVERAGE_LIMITATIONS),
    )

    source_digest = _canonical_source_digest(config, source_digests)
    document = InventoryDocument(
        inventory_version=2,
        product=product,
        source_digest=source_digest,
        coverage=coverage,
        source_digests=source_digests,
        compiled_css_freshness=freshness,
        tokens=tokens,
        components=components,
        assets=assets,
        font_faces=font_faces,
    )
    return parse_inventory(document.model_dump(mode="json"))


def _require_list(value: object, name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a list")
    return value


def _require_str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _index_exports(rows: list[Any]) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"exports[{i}] must be an object")
        export_name = _require_str(row.get("export_name"), f"exports[{i}].export_name")
        module_path = _require_str(row.get("module_path"), f"exports[{i}].module_path")
        index[(export_name, module_path)] = row
    return index


def _string_props(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError("export props must be an object")
    props: dict[str, str] = {}
    for key, item in value.items():
        if isinstance(item, str):
            props[str(key)] = item
    return props


def _normalize_source_digests(value: object) -> list[SourceDigest]:
    rows = _require_list(value, "source_digests")
    out: list[SourceDigest] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"source_digests[{i}] must be an object")
        path = _require_str(row.get("path"), f"source_digests[{i}].path")
        hex_digest = row.get("sha256")
        if hex_digest is None:
            hex_digest = row.get("digest")
        hex_digest = _require_str(hex_digest, f"source_digests[{i}].sha256")
        if hex_digest.startswith("sha256:"):
            digest = hex_digest
        else:
            digest = f"sha256:{hex_digest}"
        out.append(SourceDigest(path=path, digest=digest))
    out.sort(key=lambda row: row.path)
    return out


def _normalize_tokens(rows: list[Any]) -> list[InventoryToken]:
    parsed: list[dict[str, str]] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"tokens[{i}] must be an object")
        parsed.append(
            {
                "name": _require_str(row.get("name"), f"tokens[{i}].name"),
                "expression": _require_str(row.get("expression"), f"tokens[{i}].expression"),
                "context": _require_str(row.get("context"), f"tokens[{i}].context"),
                "source_ref": _require_str(row.get("source_ref"), f"tokens[{i}].source_ref"),
            }
        )

    index: dict[tuple[str, str], int] = {}
    for i, row in enumerate(parsed):
        index[(row["name"], row["context"])] = i

    cache: dict[int, tuple[TokenResolved, str]] = {}
    visiting: set[int] = set()

    def classify(i: int) -> tuple[TokenResolved, str]:
        result: tuple[TokenResolved, str]
        if i in cache:
            return cache[i]
        if i in visiting:
            result = (
                TokenResolvedUnsupported(kind="unsupported", reason="alias_cycle"),
                "unsupported",
            )
            return result
        expr = parsed[i]["expression"].strip()
        lowered = expr.lower()
        if "url(" in lowered or "image-set(" in lowered or "expression(" in lowered or "@import" in lowered:
            result = (
                TokenResolvedUnsupported(kind="unsupported", reason="url_expression"),
                "unsupported",
            )
            cache[i] = result
            return result
        if "var(" in lowered and ("color-mix(" in lowered or "theme(" in lowered or "calc(" in lowered):
            result = (
                TokenResolvedUnsupported(kind="unsupported", reason="runtime_expression"),
                "unsupported",
            )
            cache[i] = result
            return result
        match = _VAR_RE.match(expr)
        if match is not None:
            target = match.group(1)
            j = index.get((target, parsed[i]["context"]))
            if j is None:
                result = (
                    TokenResolvedUnsupported(kind="unsupported", reason="unresolved_alias"),
                    "unsupported",
                )
                cache[i] = result
                return result
            visiting.add(i)
            result = classify(j)
            visiting.discard(i)
            if result[1] == "unsupported" and getattr(result[0], "reason", None) == "alias_cycle":
                result = (
                    TokenResolvedUnsupported(kind="unsupported", reason="alias_cycle"),
                    "unsupported",
                )
            cache[i] = result
            return result
        scalar = _classify_scalar(expr)
        if scalar is None:
            result = (
                TokenResolvedUnsupported(
                    kind="unsupported", reason="font_shorthand" if _FONT_SHORTHAND.fullmatch(expr) else "not_scalar"
                ),
                "unsupported",
            )
        else:
            result = (scalar, "resolved")
        cache[i] = result
        return result

    tokens = [
        InventoryToken(
            name=row["name"],
            expression=row["expression"],
            context=row["context"],
            source_ref=row["source_ref"],
            resolved=classify(i)[0],
            status=classify(i)[1],  # type: ignore[arg-type]
        )
        for i, row in enumerate(parsed)
    ]
    tokens.sort(key=lambda row: (row.name, row.context))
    return tokens


def _classify_scalar(expr: str) -> TokenResolvedValue | TokenResolvedFontFamily | None:
    if expr == "0":
        return TokenResolvedValue(kind="length", value=expr)
    if _HEX_RE.fullmatch(expr):
        return TokenResolvedValue(kind="color", value=expr)
    if expr.lower() in _NAMED_COLORS:
        return TokenResolvedValue(kind="color", value=expr)
    func = _FUNC_RE.fullmatch(expr)
    if func is not None and _numeric_args(func.group(2)):
        return TokenResolvedValue(kind="color", value=expr)
    if _LENGTH_RE.fullmatch(expr):
        return TokenResolvedValue(kind="length", value=expr)
    if _NUMBER_RE.fullmatch(expr):
        return TokenResolvedValue(kind="number", value=expr)
    family = _parse_font_families(expr)
    if family is not None:
        return TokenResolvedFontFamily(kind="font_family", families=family[0], generic=family[1])
    return None


def _css_unescape(value: str) -> str:
    def decode(match: re.Match[str]) -> str:
        escaped = match.group()[1:]
        if re.match(r"[0-9a-fA-F]", escaped):
            code = int(escaped.strip(), 16)
            return chr(code) if 0 < code <= 0x10FFFF and not 0xD800 <= code <= 0xDFFF else "\ufffd"
        return escaped

    return re.sub(_CSS_ESCAPE, decode, value)


def _css_string_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] in "\"'" and value[-1] == value[0]:
        value = value[1:-1]
    return _css_unescape(value)


def _parse_font_families(expr: str) -> tuple[list[str], str | None] | None:
    """Accept only strings and custom-ident sequences separated by commas.

    families preserves the complete stack in authored order. generic is the
    last unquoted generic, a fallback hint rather than a second stack member.
    """
    rest = expr.strip()
    families: list[str] = []
    generic = None
    while rest:
        match = _FAMILY_MEMBER.match(rest)
        if match is None:
            return None
        quoted = match.group(1) is not None
        family = _css_string_value(match.group()) if quoted else " ".join(_css_unescape(match.group()).split())
        if not family or (not quoted and any(word.lower() in _CSS_GLOBALS for word in family.split())):
            return None
        if not quoted and family.lower() in _GENERIC_FAMILIES:
            family = family.lower()
            generic = family
        families.append(family)
        rest = rest[match.end() :].strip()
        if not rest:
            return families, generic
        if not rest.startswith(",") or not rest[1:].strip():
            return None
        rest = rest[1:].strip()
    return None


def _normalize_font_faces(
    value: object, *, assets: list[InventoryAsset], config: DesignSources, workspace_root: Path
) -> list[InventoryFontFace]:
    asset_index = {asset.path: asset for asset in assets}
    root = workspace_root.resolve()
    out: list[InventoryFontFace] = []
    for row in _require_list(value, "font_faces"):
        if not isinstance(row, dict):
            raise TypeError("font_faces entries must be objects")
        family_text = _require_str(row.get("family"), "font_faces.family")
        family = _parse_font_families(family_text)
        reason = None
        if family is None or len(family[0]) != 1 or family[1] is not None:
            reason = "invalid_family"
        weight_text = _require_str(row.get("weight", "normal"), "font_faces.weight").strip().lower()
        weight_text = {"normal": "400", "bold": "700"}.get(weight_text, weight_text)
        parts = weight_text.split()
        if len(parts) in (1, 2) and all(_NUMBER_RE.fullmatch(p) and 1 <= float(p) <= 1000 for p in parts):
            low, high = float(parts[0]), float(parts[-1])
            if low > high:
                low = high = 400
                reason = "invalid_weight"
        else:
            low = high = 400
            reason = "invalid_weight"
        sources: list[FontSource] = []
        source_path = _require_str(row.get("source_path"), "font_faces.source_path")
        for src in _require_list(row.get("src", []), "font_faces.src"):
            if not isinstance(src, dict):
                raise TypeError("font_faces.src entries must be objects")
            url = _css_string_value(_require_str(src.get("url"), "font_faces.src.url"))
            try:
                parsed = urlsplit(url)
            except ValueError:
                continue
            if parsed.scheme or parsed.netloc or not parsed.path or parsed.path.startswith("/"):
                continue
            rel = posixpath.normpath(posixpath.join(posixpath.dirname(source_path), unquote(parsed.path)))
            asset = asset_index.get(rel)
            if asset is None or asset.status != "present" or not asset.digest or not asset.mime.startswith("font/"):
                continue
            if not _font_asset_confined(rel, root=root, asset_roots=config.asset_roots):
                continue
            source = {"asset_path": rel, "digest": asset.digest}
            if src.get("format"):
                source["format"] = _css_string_value(_require_str(src["format"], "font_faces.src.format"))
            sources.append(FontSource.model_validate(source))
        if not sources:
            reason = reason or "no_resolved_source"
        out.append(
            InventoryFontFace(
                family=family[0][0] if family and len(family[0]) == 1 else family_text or "unknown",
                weight=FontWeight(min=low, max=high),
                style=row.get("style", "normal"),
                unicode_range=row.get("unicode_range"),
                display=row.get("display"),
                src=sources if not reason else None,
                status="unresolved" if reason else "resolved",
                reason=reason,
            )
        )
    return out


def _font_asset_confined(rel: str, *, root: Path, asset_roots: list[str]) -> bool:
    try:
        path = root / rel
        resolved = path.resolve()
        if not resolved.is_relative_to(root):
            return False
        current = root
        for part in Path(rel).parts:
            current = current / part
            if current.is_symlink():
                return False
        return any(resolved.is_relative_to(root / declared) for declared in asset_roots)
    except (OSError, RuntimeError):
        return False


def _numeric_args(body: str) -> bool:
    parts = [part.strip() for part in re.split(r"[\s,/]+", body) if part.strip()]
    if not parts:
        return False
    return all(_NUMERIC_TOKEN_RE.fullmatch(part) for part in parts)


def _normalize_assets(
    value: object,
    *,
    config: DesignSources,
    workspace_root: Path,
) -> list[InventoryAsset]:
    rows = _require_list(value, "assets")
    licenses: dict[str, str] = {}
    for attribution in config.attributions:
        licenses.setdefault(attribution.path, attribution.license)

    assets: list[InventoryAsset] = []
    seen: set[str] = set()
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"assets[{i}] must be an object")
        path = _require_str(row.get("path"), f"assets[{i}].path")
        status = row.get("status") or "present"
        if status not in ("present", "missing", "oversized"):
            raise TypeError(f"assets[{i}].status is invalid")
        mime = row.get("mime")
        if mime is None:
            mime = ALLOWED_ASSET_TYPES.get(Path(path).suffix.lower(), "")
        else:
            mime = _require_str(mime, f"assets[{i}].mime")
        digest = None
        size = None
        width = row.get("width")
        height = row.get("height")
        if status == "present":
            sha = row.get("sha256")
            if isinstance(sha, str) and sha:
                digest = sha if sha.startswith("sha256:") else f"sha256:{sha}"
            size = row.get("bytes")
            if size is not None and (type(size) is bool or not isinstance(size, int)):
                raise TypeError(f"assets[{i}].bytes must be an int")
        else:
            width = None
            height = None
            digest = None
            size = None
        if width is not None and (type(width) is bool or not isinstance(width, int)):
            raise TypeError(f"assets[{i}].width must be an int")
        if height is not None and (type(height) is bool or not isinstance(height, int)):
            raise TypeError(f"assets[{i}].height must be an int")
        assets.append(
            InventoryAsset(
                path=path,
                mime=mime,
                width=width,
                height=height,
                digest=digest,
                bytes=size,
                license=licenses.get(path, "unknown"),
                references=[],
                status=cast(Literal["present", "missing", "oversized"], status),
            )
        )
        seen.add(path)

    for index, attribution in enumerate(config.attributions):
        if attribution.path in seen:
            continue
        candidate = resolve_config_path(
            attribution.path,
            workspace_root=workspace_root,
            pointer=f"/attributions/{index}/path",
        )
        try:
            missing = not candidate.is_file()
        except OSError:
            missing = True
        if not missing:
            continue
        mime = ALLOWED_ASSET_TYPES.get(Path(attribution.path).suffix.lower(), "")
        assets.append(
            InventoryAsset(
                path=attribution.path,
                mime=mime,
                width=None,
                height=None,
                digest=None,
                bytes=None,
                license=attribution.license,
                references=[],
                status="missing",
            )
        )
        seen.add(attribution.path)

    assets.sort(key=lambda row: row.path)
    return assets


def _compiled_css_freshness(config: DesignSources, *, workspace_root: Path) -> list[CompiledCssFreshness]:
    budget = [0]
    rows: list[CompiledCssFreshness] = []
    for index, item in enumerate(config.compiled_css):
        provenance = getattr(item, "provenance", None)
        if provenance is None:
            rows.append(
                CompiledCssFreshness(
                    output=item.output,
                    status="unsupported",
                    reason="no_provenance",
                    stale_paths=[],
                )
            )
            continue
        stale_paths: list[str] = []
        input_missing = False
        input_changed = False
        for input_index, provenance_input in enumerate(provenance.inputs):
            pointer = f"/compiled_css/{index}/provenance/inputs/{input_index}/path"
            current = _hash_workspace_file(
                provenance_input.path,
                workspace_root=workspace_root,
                pointer=pointer,
                max_bytes=MAX_SOURCE_FILE_BYTES,
                budget=budget,
            )
            expected = provenance_input.sha256
            if expected.startswith("sha256:"):
                expected = expected.removeprefix("sha256:")
            if current is None:
                input_missing = True
                stale_paths.append(provenance_input.path)
            elif current != expected:
                input_changed = True
                stale_paths.append(provenance_input.path)
        output_pointer = f"/compiled_css/{index}/output"
        output_hex = _hash_workspace_file(
            item.output,
            workspace_root=workspace_root,
            pointer=output_pointer,
            max_bytes=MAX_CSS_BYTES,
            budget=budget,
        )
        expected_output = provenance.output_digest
        output_changed = False
        if output_hex is None:
            output_changed = True
            stale_paths.append(item.output)
        else:
            actual_output = output_hex if output_hex.startswith("sha256:") else f"sha256:{output_hex}"
            if actual_output != expected_output:
                output_changed = True
                stale_paths.append(item.output)
        if not stale_paths:
            rows.append(
                CompiledCssFreshness(
                    output=item.output,
                    status="verified",
                    reason=None,
                    stale_paths=[],
                )
            )
            continue
        reason: Literal["input_missing", "input_changed", "output_changed"]
        if input_missing:
            reason = "input_missing"
        elif input_changed:
            reason = "input_changed"
        elif output_changed:
            reason = "output_changed"
        else:
            reason = "input_changed"
        rows.append(
            CompiledCssFreshness(
                output=item.output,
                status="stale",
                reason=reason,
                stale_paths=sorted(set(stale_paths)),
            )
        )
    rows.sort(key=lambda row: row.output)
    return rows


def _hash_workspace_file(
    rel: str,
    *,
    workspace_root: Path,
    pointer: str,
    max_bytes: int,
    budget: list[int],
) -> str | None:
    path = resolve_config_path(rel, workspace_root=workspace_root, pointer=pointer)
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if not path.is_file():
        return None
    if size > max_bytes:
        return None
    if budget[0] + size > MAX_TOTAL_READ_BYTES:
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(65536)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError:
        return None
    budget[0] += size
    return digest.hexdigest()


def _canonical_source_digest(config: DesignSources, source_digests: list[SourceDigest]) -> str:
    payload = {
        "config": config.model_dump(mode="json"),
        "source_digests": [row.model_dump(mode="json") for row in source_digests],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
