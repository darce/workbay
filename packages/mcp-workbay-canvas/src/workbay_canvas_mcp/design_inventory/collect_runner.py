"""Walk asset roots and spawn the Node collector (WBC-03 S8)."""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

from workbay_canvas_mcp.design_inventory.config import (
    MAX_ASSET_FILE_BYTES,
    MAX_ASSETS,
    MAX_CSS_BYTES,
    MAX_FILES,
    MAX_INVENTORY_JSON_BYTES,
    MAX_SOURCE_FILE_BYTES,
    MAX_TOTAL_READ_BYTES,
    DesignSources,
    resolve_config_path,
)
from workbay_canvas_mcp.design_inventory.errors import InventoryConfigError
from workbay_canvas_mcp.design_inventory.models import ALLOWED_ASSET_TYPES

_COLLECTOR_NAME = "collect-design-inventory.mjs"
_DEFAULT_COLLECTOR = Path(__file__).resolve().parents[5] / "apps" / "workbay-canvas-web" / "scripts" / _COLLECTOR_NAME
_PACKAGED_COLLECTOR = Path(__file__).resolve().parents[1] / "collector_dist" / _COLLECTOR_NAME
NODE_MISSING_MESSAGE = (
    "node not found: install Node.js >=20 or set WORKBAY_NODE; viewing needs no Node, only Refresh collects sources"
)


class InventoryCollectError(RuntimeError):
    """Collector spawn, timeout, or payload error."""

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class AssetScan:
    listed: list[str]
    oversized: list[str]
    skipped_asset_files: int
    listed_bytes: int = 0


def scan_assets(config: DesignSources, *, workspace_root: Path) -> AssetScan:
    root = workspace_root.resolve()
    listed: list[str] = []
    oversized: list[str] = []
    skipped = 0
    listed_bytes = [0]
    for index, rel_root in enumerate(config.asset_roots):
        pointer = f"/asset_roots/{index}"
        abs_root = resolve_config_path(rel_root, workspace_root=workspace_root, pointer=pointer)
        if not abs_root.exists():
            continue
        if abs_root.is_file():
            skipped = _classify_file(
                abs_root,
                workspace_root=root,
                pointer=pointer,
                listed=listed,
                oversized=oversized,
                skipped=skipped,
                listed_bytes=listed_bytes,
            )
            continue
        for dirpath, _dirnames, filenames in os.walk(abs_root, followlinks=False):
            for name in filenames:
                full = Path(dirpath) / name
                skipped = _classify_file(
                    full,
                    workspace_root=root,
                    pointer=pointer,
                    listed=listed,
                    oversized=oversized,
                    skipped=skipped,
                    listed_bytes=listed_bytes,
                )
    listed.sort()
    oversized.sort()
    return AssetScan(
        listed=listed,
        oversized=oversized,
        skipped_asset_files=skipped,
        listed_bytes=listed_bytes[0],
    )


def resolve_collector(explicit: str | Path | None = None) -> Path:
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(Path(explicit))
    env = os.environ.get("WORKBAY_INVENTORY_COLLECTOR")
    if env:
        candidates.append(Path(env))
    candidates.append(_DEFAULT_COLLECTOR)
    candidates.append(_PACKAGED_COLLECTOR)
    for candidate in candidates:
        path = candidate if candidate.is_absolute() else Path.cwd() / candidate
        try:
            if path.is_file() and path.name == _COLLECTOR_NAME:
                return path
        except OSError:
            continue
    raise InventoryCollectError("inventory collector not found", reason="collector_not_found")


def run_collector(config: DesignSources, *, workspace_root: Path, collector: Path) -> dict:
    scan = scan_assets(config, workspace_root=workspace_root)
    file_count = (
        len({component.module_path for component in config.components}) + len(config.css_inputs) + len(scan.listed)
    )
    if file_count > MAX_FILES:
        raise InventoryConfigError(
            pointer="",
            code="oversized",
            message=f"MAX_FILES exceeded ({MAX_FILES})",
        )
    total_bytes = scan.listed_bytes
    seen_modules: set[str] = set()
    for index, component in enumerate(config.components):
        rel = component.module_path
        if rel in seen_modules:
            continue
        seen_modules.add(rel)
        total_bytes = _size_node_input(
            rel,
            workspace_root=workspace_root,
            pointer=f"/components/{index}/module_path",
            per_file_limit=MAX_SOURCE_FILE_BYTES,
            bound_name="MAX_SOURCE_FILE_BYTES",
            total=total_bytes,
        )
    for index, rel in enumerate(config.css_inputs):
        total_bytes = _size_node_input(
            rel,
            workspace_root=workspace_root,
            pointer=f"/css_inputs/{index}",
            per_file_limit=MAX_CSS_BYTES,
            bound_name="MAX_CSS_BYTES",
            total=total_bytes,
        )
    request = {
        "request_version": 1,
        "workspace_root": str(workspace_root.resolve()),
        "exports": [
            {"export_name": component.export_name, "module_path": component.module_path}
            for component in config.components
        ],
        "css_inputs": list(config.css_inputs),
        "assets": list(scan.listed),
        "max_source_file_bytes": MAX_SOURCE_FILE_BYTES,
        "max_css_bytes": MAX_CSS_BYTES,
        "max_asset_file_bytes": MAX_ASSET_FILE_BYTES,
    }
    env_node = os.environ.get("WORKBAY_NODE")
    node = env_node or shutil.which("node")
    if not node:
        raise InventoryCollectError(NODE_MISSING_MESSAGE, reason="node_not_found")
    if env_node and not (os.path.isfile(env_node) and os.access(env_node, os.X_OK)):
        raise InventoryCollectError(NODE_MISSING_MESSAGE, reason="node_not_found")
    request_bytes = json.dumps(request, separators=(",", ":")).encode("utf-8")
    try:
        proc = subprocess.run(
            [node, str(collector), "--request-stdin"],
            input=request_bytes,
            capture_output=True,
            shell=False,
            cwd=workspace_root,
            timeout=60,
            env={"PATH": os.environ.get("PATH", "")},
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise InventoryCollectError("inventory collector timed out") from exc
    except OSError as exc:
        raise InventoryCollectError("inventory collector failed to spawn", reason="spawn_failed") from exc
    if len(proc.stdout) > MAX_INVENTORY_JSON_BYTES:
        raise InventoryCollectError("collector stdout exceeds MAX_INVENTORY_JSON_BYTES")
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode == 4:
            raise InventoryCollectError(
                f"inventory collector deps missing: {_stderr_code(stderr)}",
                reason="deps_missing",
            )
        code = _stderr_diagnostic_code(stderr)
        raise InventoryCollectError(
            f"inventory collector exited {proc.returncode}" + (f": {code}" if code else ""),
            reason="exit_nonzero",
        )
    try:
        raw = json.loads(proc.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InventoryCollectError("collector stdout is not JSON") from exc
    if not isinstance(raw, dict):
        raise InventoryCollectError("collector stdout must be a JSON object")
    assets = raw.get("assets")
    if not isinstance(assets, list):
        assets = []
        raw["assets"] = assets
    existing = {row.get("path") for row in assets if isinstance(row, dict)}
    for path in scan.oversized:
        if path in existing:
            continue
        ext = Path(path).suffix.lower()
        assets.append(
            {
                "path": path,
                "status": "oversized",
                "sha256": None,
                "bytes": None,
                "width": None,
                "height": None,
                "mime": ALLOWED_ASSET_TYPES.get(ext, ""),
            }
        )
    raw["skipped_asset_files"] = scan.skipped_asset_files
    return raw


def _stderr_code(stderr: str) -> str:
    last = ""
    for line in stderr.splitlines():
        stripped = line.strip()
        if stripped:
            last = stripped
    if not last:
        return "deps_missing"
    try:
        payload = json.loads(last)
    except json.JSONDecodeError:
        return "deps_missing"
    code = payload.get("code") if isinstance(payload, dict) else None
    if isinstance(code, str) and code and "/" not in code and "\\" not in code:
        return code
    return "deps_missing"


def _stderr_diagnostic_code(stderr: str) -> str | None:
    last = next((line.strip() for line in reversed(stderr.splitlines()) if line.strip()), "")
    if not last:
        return None
    try:
        payload = json.loads(last)
    except json.JSONDecodeError:
        return None
    code = payload.get("code") if isinstance(payload, dict) else None
    if isinstance(code, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", code):
        return code
    return None


def _size_node_input(
    rel: str,
    *,
    workspace_root: Path,
    pointer: str,
    per_file_limit: int,
    bound_name: str,
    total: int,
) -> int:
    path = resolve_config_path(rel, workspace_root=workspace_root, pointer=pointer)
    try:
        st = os.stat(path)
    except OSError:
        return total
    if not stat.S_ISREG(st.st_mode):
        return total
    if st.st_size > per_file_limit:
        raise InventoryConfigError(
            pointer=pointer,
            code="oversized",
            message=f"{bound_name} exceeded ({per_file_limit})",
        )
    total += st.st_size
    if total > MAX_TOTAL_READ_BYTES:
        raise InventoryConfigError(
            pointer=pointer,
            code="oversized",
            message=f"MAX_TOTAL_READ_BYTES exceeded ({MAX_TOTAL_READ_BYTES})",
        )
    return total


def _classify_file(
    full: Path,
    *,
    workspace_root: Path,
    pointer: str,
    listed: list[str],
    oversized: list[str],
    skipped: int,
    listed_bytes: list[int],
) -> int:
    try:
        real = Path(os.path.realpath(full))
    except OSError as exc:
        raise InventoryConfigError(
            pointer=pointer,
            code="escape",
            message="path cannot be resolved",
        ) from exc
    if not real.is_relative_to(workspace_root):
        raise InventoryConfigError(
            pointer=pointer,
            code="escape",
            message="path escapes workspace",
        )
    try:
        st = os.stat(real)
    except OSError:
        return skipped
    if not stat.S_ISREG(st.st_mode):
        return skipped
    try:
        rel = full.relative_to(workspace_root).as_posix()
    except ValueError:
        rel = Path(os.path.relpath(full, workspace_root)).as_posix()
    ext = Path(rel).suffix.lower()
    if ext not in ALLOWED_ASSET_TYPES:
        return skipped + 1
    if st.st_size > MAX_ASSET_FILE_BYTES:
        oversized.append(rel)
    else:
        listed.append(rel)
        listed_bytes[0] += st.st_size
    if len(listed) + len(oversized) > MAX_ASSETS:
        raise InventoryConfigError(
            pointer=pointer,
            code="oversized",
            message=f"MAX_ASSETS exceeded ({MAX_ASSETS})",
        )
    if listed_bytes[0] > MAX_TOTAL_READ_BYTES:
        raise InventoryConfigError(
            pointer=pointer,
            code="oversized",
            message=f"MAX_TOTAL_READ_BYTES exceeded ({MAX_TOTAL_READ_BYTES})",
        )
    return skipped
