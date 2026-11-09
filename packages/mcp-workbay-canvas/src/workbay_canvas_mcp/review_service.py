"""Review bundle list/get/refresh domain (WBC-05 S6)."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import stat
import tempfile
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

from workbay_canvas_mcp.design_inventory.collect_runner import (
    NODE_MISSING_MESSAGE,
    InventoryCollectError,
    resolve_collector,
    run_collector,
)
from workbay_canvas_mcp.design_inventory.config import (
    DEFAULT_CONFIG_PATH,
    MAX_ASSET_FILE_BYTES,
    DesignSources,
    load_design_sources,
    resolve_config_path,
)
from workbay_canvas_mcp.design_inventory.errors import InventoryConfigError
from workbay_canvas_mcp.design_inventory.models import ALLOWED_ASSET_TYPES, parse_inventory
from workbay_canvas_mcp.design_inventory.normalize import normalize_inventory
from workbay_canvas_mcp.design_inventory.refresh import refresh_generated
from workbay_canvas_mcp.review_errors import ReviewServiceError
from workbay_canvas_mcp.ux_map import review_bundle
from workbay_canvas_mcp.ux_map.critique import CritiqueFinding, critique_map
from workbay_canvas_mcp.ux_map.fs_store import load_map
from workbay_canvas_mcp.ux_map.handoff_emit import HandoffFindingWriter, emit_critique_to_handoff
from workbay_canvas_mcp.ux_map.models import UxMap
from workbay_canvas_mcp.ux_map.parse import MapSchemaError
from workbay_canvas_mcp.ux_map.review_bundle import SAFE_ID, build_review_bundle, dumps_review_bundle
from workbay_canvas_mcp.ux_map.source_check import SourceCheckItem, SourceCheckReport

HANDOFF_EMIT_TIMEOUT_S = 5.0

_LOG = logging.getLogger(__name__)
_DRIVE_ABS = re.compile(r"^[A-Za-z]:")
_POSIX_ABS_TOKEN = re.compile(r"/(?:[^/\s\"']+/)+[^/\s\"']*")
_WIN_ABS_TOKEN = re.compile(r"[A-Za-z]:[\\/](?:[^\\/\s\"']+[\\/])*[^\\/\s\"']*")
_BUNDLE_SIZE_GOT = re.compile(r"got (\d+)")
_COLLECTOR_REASON_TABLE: tuple[tuple[str, str], ...] = (
    ("node not found", "node_not_found"),
    ("collector not found", "collector_not_found"),
    ("timed out", "timeout"),
    ("failed to spawn", "spawn_failed"),
    ("exceeds max_inventory_json_bytes", "stdout_too_large"),
    ("deps missing", "deps_missing"),
    ("exited", "exit_nonzero"),
    ("not json", "stdout_not_json"),
    ("must be a json object", "stdout_not_json"),
)
_COLLECTOR_MESSAGES = {
    "node_not_found": NODE_MISSING_MESSAGE,
    "collector_not_found": (
        "inventory collector not found: reinstall the Canvas package, or set WORKBAY_INVENTORY_COLLECTOR to a collect-design-inventory.mjs file"
    ),
    "timeout": "collector timed out",
    "spawn_failed": "collector failed to spawn",
    "stdout_too_large": "collector stdout too large",
    "exit_nonzero": "collector exited nonzero",
    "stdout_not_json": "collector stdout is not JSON",
    "parse_error": "collector output failed to parse",
    "deps_missing": (
        "install typescript and postcss in the workspace (`npm install -D typescript postcss`) or run from a checkout"
    ),
}


@dataclass(frozen=True)
class BundleSummary:
    map_ref: str
    digest: str | None
    freshness: Literal["ok", "missing"]
    generated_relpath: str


@dataclass(frozen=True)
class BundleStatus:
    map_ref: str
    digest: str | None
    freshness: Literal["ok", "missing", "stale"]
    refresh_in_progress: bool
    last_error: dict | None


@dataclass(frozen=True)
class RefreshResult:
    status: Literal["ok"]
    digest: str
    bundle_text: str
    handoff_emit: dict | None


def refresh_lock_path(generated_dir: Path, *, workspace_root: Path) -> Path:
    digest = hashlib.sha256(str(generated_dir.resolve()).encode()).hexdigest()[:16]
    return workspace_root.resolve() / ".task-state" / "canvas-mcp" / "refresh-locks" / f"{digest}.lock"


class ReviewService:
    def __init__(
        self,
        *,
        workspace_root: Path,
        handoff_writer: HandoffFindingWriter | None = None,
        collector: Path | None = None,
        config: str = DEFAULT_CONFIG_PATH,
    ) -> None:
        self._workspace_root = Path(workspace_root)
        self.handoff_writer = handoff_writer
        self._collector = collector
        self._config = config
        self._last_errors: dict[str, dict] = {}

    def list_bundles(self) -> list[BundleSummary]:
        config, generated_dir = self._load_config()
        summaries: list[BundleSummary] = []
        seen: set[str] = set()
        for index, rel in enumerate(config.maps):
            map_ = self._load_map_at(rel, pointer=f"/maps/{index}")
            if map_ is None or map_.map_ref in seen:
                continue
            seen.add(map_.map_ref)
            data = _read_bundle_file(_bundle_path(generated_dir, map_.map_ref))
            summaries.append(
                BundleSummary(
                    map_ref=map_.map_ref,
                    digest=_sha256(data) if data is not None else None,
                    freshness="ok" if data is not None else "missing",
                    generated_relpath=f"{config.generated_dir.rstrip('/')}/{map_.map_ref}.review-bundle.json",
                )
            )
        return summaries

    def critique_base_map(self, map_ref: str) -> list[CritiqueFinding]:
        """Critique the configured saved map only; no refresh, write or handoff."""
        _check_map_ref_grammar(map_ref)
        _config, _generated_dir, map_, parse_error = self._lookup_configured_map(map_ref)
        if map_ is None:
            if parse_error is not None:
                raise parse_error
            raise ReviewServiceError(code="bundle_not_found", message="configured map not found")
        return critique_map(map_)

    def get_bundle(self, map_ref: str) -> str:
        _check_map_ref_grammar(map_ref)
        _config, generated_dir, map_, parse_error = self._lookup_configured_map(map_ref)
        if map_ is None and parse_error is None:
            raise ReviewServiceError(code="bundle_not_found", message="bundle not found")
        data = _read_bundle_file(_bundle_path(generated_dir, map_ref))
        if data is None:
            if parse_error is not None:
                raise parse_error
            raise ReviewServiceError(code="bundle_not_found", message="bundle not found")
        return data.decode("utf-8")

    def open_bundle_asset(self, map_ref: str, path: str, digest: str) -> tuple[bytes, str]:
        """Read only a current bundle's listed asset, confined to declared roots.

        Open every component with NOFOLLOW, keeping directory descriptors until
        the bounded read finishes. A path replacement cannot turn this read into
        a symlink traversal between a containment check and open.
        """
        _check_map_ref_grammar(map_ref)
        config, generated_dir, map_, parse_error = self._lookup_configured_map(map_ref)
        if map_ is None:
            raise ReviewServiceError(code="asset_not_found", message="asset not found")
        parts = PurePosixPath(path).parts
        if (
            not parts
            or path.startswith("/")
            or "\\" in path
            or "\x00" in path
            or any(p in (".", "..") for p in parts)
            or "/".join(parts) != path
            or _DRIVE_ABS.match(path)
        ):
            raise ReviewServiceError(code="asset_forbidden", message="asset path refused")
        data = _read_bundle_file(_bundle_path(generated_dir, map_ref), max_bytes=review_bundle.MAX_BUNDLE_BYTES)
        if data is None:
            raise ReviewServiceError(code="asset_not_found", message="asset not found")
        try:
            bundle = json.loads(data)
            inventory = parse_inventory(bundle["inventory"])
        except (ValueError, KeyError, TypeError) as exc:
            raise ReviewServiceError(code="asset_not_found", message="asset inventory unavailable") from exc
        asset = next((a for a in inventory.assets if a.path == path), None)
        media = ALLOWED_ASSET_TYPES.get(PurePosixPath(path).suffix.lower())
        if asset is None or asset.status != "present" or not asset.digest or media is None:
            raise ReviewServiceError(code="asset_not_found", message="asset not found")
        if not any(PurePosixPath(path).is_relative_to(PurePosixPath(declared)) for declared in config.asset_roots):
            raise ReviewServiceError(code="asset_forbidden", message="asset outside declared roots")
        if digest != asset.digest:
            raise ReviewServiceError(code="asset_changed", message="asset digest changed")
        opened = _read_inventory_asset(self._workspace_root.resolve(), parts)
        if _sha256(opened) != asset.digest:
            raise ReviewServiceError(code="asset_changed", message="asset digest changed")
        return opened, media

    def get_bundle_status(self, map_ref: str) -> BundleStatus:
        _check_map_ref_grammar(map_ref)
        _config, generated_dir, map_, parse_error = self._lookup_configured_map(map_ref)
        data = _read_bundle_file(_bundle_path(generated_dir, map_ref))
        if map_ is None:
            if parse_error is None:
                raise ReviewServiceError(code="bundle_not_found", message="bundle not found")
            if data is None:
                raise parse_error
            return BundleStatus(
                map_ref=map_ref,
                digest=_sha256(data),
                freshness="stale",
                refresh_in_progress=_probe_refresh_lock(generated_dir, self._workspace_root),
                last_error={
                    "code": "map_invalid",
                    "failed_stage": "parse",
                    "message": parse_error.message,
                },
            )
        digest = _sha256(data) if data is not None else None
        last_error = self._last_errors.get(map_ref)
        if data is None:
            freshness: Literal["ok", "missing", "stale"] = "missing"
        elif last_error is not None:
            freshness = "stale"
        else:
            freshness = "ok"
        return BundleStatus(
            map_ref=map_ref,
            digest=digest,
            freshness=freshness,
            refresh_in_progress=_probe_refresh_lock(generated_dir, self._workspace_root),
            last_error=last_error,
        )

    def refresh_bundle(
        self,
        map_ref: str,
        *,
        emit_handoff: bool = False,
        task_ref: str | None = None,
        session: str | None = None,
    ) -> RefreshResult:
        _check_map_ref_grammar(map_ref)
        config, generated_dir, map_, parse_error = self._lookup_configured_map(map_ref)
        if map_ is None:
            if parse_error is not None:
                last_good = _read_bundle_file(_bundle_path(generated_dir, map_ref))
                details = dict(parse_error.details)
                details["failed_stage"] = "parse"
                last_good_digest = _sha256(last_good) if last_good is not None else None
                if last_good_digest is not None:
                    details["last_good_digest"] = last_good_digest
                error = ReviewServiceError(
                    code="map_invalid",
                    message=parse_error.message,
                    details=details,
                )
                self._remember_error(map_ref, error)
                raise error
            raise ReviewServiceError(code="bundle_not_found", message="bundle not found")
        _assert_flow01(config, generated_dir, workspace_root=self._workspace_root)
        lock_path = refresh_lock_path(generated_dir, workspace_root=self._workspace_root)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        result: RefreshResult | None = None
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                error = ReviewServiceError(
                    code="refresh_in_progress",
                    message="refresh already in progress",
                    details={"failed_stage": "write"},
                )
                raise error from exc
            bundle_path = _bundle_path(generated_dir, map_ref)
            last_good = _read_bundle_file(bundle_path)
            last_good_digest = _sha256(last_good) if last_good is not None else None
            try:
                result = self._refresh_locked(
                    map_ref=map_ref,
                    map_=map_,
                    config=config,
                    generated_dir=generated_dir,
                    bundle_path=bundle_path,
                    last_good=last_good,
                    last_good_digest=last_good_digest,
                )
            except ReviewServiceError as exc:
                self._remember_error(map_ref, exc)
                raise
            except Exception as exc:
                _LOG.warning("refresh failed", exc_info=True)
                error = ReviewServiceError(
                    code="internal_error",
                    message="refresh failed",
                    details=_failure_details("write", last_good_digest),
                )
                self._remember_error(map_ref, error)
                raise error from exc
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)
        assert result is not None
        self._last_errors.pop(map_ref, None)
        if emit_handoff:
            result = RefreshResult(
                status=result.status,
                digest=result.digest,
                bundle_text=result.bundle_text,
                handoff_emit=self._emit_handoff(map_=map_, task_ref=task_ref, session=session),
            )
        return result

    def _refresh_locked(
        self,
        *,
        map_ref: str,
        map_: UxMap,
        config: DesignSources,
        generated_dir: Path,
        bundle_path: Path,
        last_good: bytes | None,
        last_good_digest: str | None,
    ) -> RefreshResult:
        try:
            collector = resolve_collector(self._collector)
        except InventoryCollectError as exc:
            raise _collector_error(exc, last_good_digest) from exc
        try:
            raw = run_collector(config, workspace_root=self._workspace_root, collector=collector)
        except InventoryConfigError as exc:
            raise _config_error(exc, last_good_digest) from exc
        except InventoryCollectError as exc:
            raise _collector_error(exc, last_good_digest) from exc
        try:
            doc = normalize_inventory(
                raw,
                config=config,
                workspace_root=self._workspace_root,
                product=self._workspace_root.name,
            )
        except (TypeError, ValueError) as exc:
            raise ReviewServiceError(
                code="collector_failed",
                message=_COLLECTOR_MESSAGES["parse_error"],
                details=_failure_details("inventory", last_good_digest, reason="parse_error"),
            ) from exc
        refresh_generated(out_dir=generated_dir, doc=doc)
        previous = _previous_source_report(last_good)
        try:
            bundle = build_review_bundle(
                map_,
                workspace_root=self._workspace_root,
                inventory=doc,
                previous_source_report=previous,
            )
        except MapSchemaError as exc:
            raise ReviewServiceError(
                code="map_invalid",
                message="map is invalid",
                details=_failure_details("bundle", last_good_digest),
            ) from exc
        except ValueError as exc:
            raise _bundle_value_error(exc, last_good_digest) from exc
        text = dumps_review_bundle(bundle)
        data = text.encode("utf-8")
        max_bytes = review_bundle.MAX_BUNDLE_BYTES
        if len(data) > max_bytes:
            raise ReviewServiceError(
                code="bundle_too_large",
                message="review bundle exceeds size limit",
                details=_failure_details("bundle", last_good_digest, bytes=len(data), max_bytes=max_bytes),
            )
        _swap_bundle(bundle_path, data, last_good)
        return RefreshResult(
            status="ok",
            digest=_sha256(data),
            bundle_text=text,
            handoff_emit=None,
        )

    def _emit_handoff(
        self,
        *,
        map_: UxMap,
        task_ref: str | None,
        session: str | None,
    ) -> dict:
        if not task_ref:
            return {
                "ok": False,
                "written": 0,
                "finding_ids": [],
                "error": "emit_handoff requires task_ref",
            }
        timeout = HANDOFF_EMIT_TIMEOUT_S
        session_id = session or "canvas-refresh"
        findings = critique_map(map_)
        box: dict[str, object] = {}

        def worker() -> None:
            try:
                box["receipt"] = emit_critique_to_handoff(
                    findings,
                    task_ref=task_ref,
                    session=session_id,
                    map_ref=map_.map_ref,
                    writer=self.handoff_writer,
                )
            except Exception as exc:  # noqa: BLE001 — receipt must not fail refresh
                box["exc"] = exc

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            return {
                "ok": False,
                "written": 0,
                "finding_ids": [],
                "error": "handoff writer timed out",
            }
        exc = box.get("exc")
        if isinstance(exc, BaseException):
            return {
                "ok": False,
                "written": 0,
                "finding_ids": [],
                "error": f"{type(exc).__name__}: {_sanitize_client_text(str(exc), workspace_root=self._workspace_root)}",
            }
        receipt = box.get("receipt")
        if not isinstance(receipt, Mapping):
            return {
                "ok": False,
                "written": 0,
                "finding_ids": [],
                "error": "handoff writer returned no receipt",
            }
        projected: dict[str, object] = {
            "ok": bool(receipt.get("ok")),
            "written": int(receipt.get("written") or 0),
            "finding_ids": list(receipt.get("finding_ids") or []),
        }
        if not projected["ok"]:
            error = receipt.get("error")
            if error:
                projected["error"] = _sanitize_client_text(str(error), workspace_root=self._workspace_root)
        return projected

    def _load_config(self) -> tuple[DesignSources, Path]:
        try:
            config_path = resolve_config_path(
                self._config,
                workspace_root=self._workspace_root,
                pointer="/config",
            )
            config = load_design_sources(config_path, workspace_root=self._workspace_root)
            generated_dir = resolve_config_path(
                config.generated_dir,
                workspace_root=self._workspace_root,
                pointer="/generated_dir",
            )
        except InventoryConfigError as exc:
            raise _config_error(exc, None) from exc
        return config, generated_dir

    def _lookup_configured_map(
        self, map_ref: str
    ) -> tuple[DesignSources, Path, UxMap | None, ReviewServiceError | None]:
        config, generated_dir = self._load_config()
        matched: UxMap | None = None
        parse_error: ReviewServiceError | None = None
        for index, rel in enumerate(config.maps):
            pointer = f"/maps/{index}"
            map_ = self._load_map_at(rel, pointer=pointer)
            if map_ is None:
                if parse_error is None:
                    parse_error = ReviewServiceError(
                        code="map_invalid",
                        message="configured map is invalid",
                        details={"pointer": pointer},
                    )
                continue
            if map_.map_ref == map_ref:
                matched = map_
                break
        return config, generated_dir, matched, parse_error

    def _resolve_map(self, map_ref: str) -> tuple[DesignSources, Path, UxMap]:
        config, generated_dir, matched, parse_error = self._lookup_configured_map(map_ref)
        if matched is not None:
            return config, generated_dir, matched
        if parse_error is not None:
            raise parse_error
        raise ReviewServiceError(code="bundle_not_found", message="bundle not found")

    def _load_map_at(self, rel: str, *, pointer: str) -> UxMap | None:
        try:
            path = resolve_config_path(rel, workspace_root=self._workspace_root, pointer=pointer)
        except InventoryConfigError as exc:
            raise _config_error(exc, None) from exc
        try:
            return load_map(path)
        except (OSError, json.JSONDecodeError, MapSchemaError, ValueError):
            return None

    def _remember_error(self, map_ref: str, exc: ReviewServiceError) -> None:
        self._last_errors[map_ref] = {
            "code": exc.code,
            "failed_stage": exc.details.get("failed_stage"),
            "message": exc.message,
        }


def _sanitize_client_text(text: str, *, workspace_root: Path) -> str:
    prefixes = {str(workspace_root)}
    try:
        prefixes.add(str(workspace_root.resolve()))
    except OSError:
        pass
    for prefix in sorted(prefixes, key=len, reverse=True):
        if prefix:
            text = text.replace(prefix, "")
    text = _WIN_ABS_TOKEN.sub("<path>", text)
    return _POSIX_ABS_TOKEN.sub("<path>", text)


def _check_map_ref_grammar(map_ref: str) -> None:
    if (
        ".." in map_ref
        or "\0" in map_ref
        or "://" in map_ref
        or map_ref.startswith("/")
        or map_ref.startswith("\\")
        or "\\" in map_ref
        or _DRIVE_ABS.match(map_ref) is not None
    ):
        raise ReviewServiceError(
            code="path_outside_workspace",
            message="map_ref is not a workspace-relative identifier",
        )
    if SAFE_ID.fullmatch(map_ref) is None:
        raise ReviewServiceError(code="map_invalid", message="map_ref is not a valid identifier")


def _assert_flow01(config: DesignSources, generated_dir: Path, *, workspace_root: Path) -> None:
    generated_real = os.path.realpath(generated_dir)
    inputs: list[tuple[str, str]] = []
    for index, rel in enumerate(config.maps):
        inputs.append((rel, f"/maps/{index}"))
    for index, rel in enumerate(config.css_inputs):
        inputs.append((rel, f"/css_inputs/{index}"))
    for index, rel in enumerate(config.asset_roots):
        inputs.append((rel, f"/asset_roots/{index}"))
    for index, component in enumerate(config.components):
        inputs.append((component.module_path, f"/components/{index}/module_path"))
    for index, row in enumerate(config.compiled_css):
        inputs.append((row.output, f"/compiled_css/{index}/output"))
    for rel, pointer in inputs:
        try:
            path = resolve_config_path(rel, workspace_root=workspace_root, pointer=pointer)
        except InventoryConfigError:
            continue
        if os.path.realpath(path) == generated_real:
            raise ReviewServiceError(
                code="config_invalid",
                message="generated_dir must not be an input path",
                details={"pointer": "/generated_dir", "failed_stage": "config"},
            )


def _collector_reason(message: str) -> str:
    lowered = message.lower()
    for substring, reason in _COLLECTOR_REASON_TABLE:
        if substring in lowered:
            return reason
    return "parse_error"


def _collector_error(exc: InventoryCollectError, last_good_digest: str | None) -> ReviewServiceError:
    typed = getattr(exc, "reason", None)
    reason = typed if typed in _COLLECTOR_MESSAGES else _collector_reason(str(exc))
    return ReviewServiceError(
        code="collector_failed",
        message=_COLLECTOR_MESSAGES[reason],
        details=_failure_details("collector", last_good_digest, reason=reason),
    )


def _config_error(exc: InventoryConfigError, last_good_digest: str | None) -> ReviewServiceError:
    details = _failure_details("config", last_good_digest, pointer=exc.pointer, code=exc.code)
    return ReviewServiceError(
        code="config_invalid",
        message="invalid design-sources config",
        details=details,
    )


def _bundle_value_error(exc: ValueError, last_good_digest: str | None) -> ReviewServiceError:
    message = str(exc)
    if message.startswith("review bundle exceeds"):
        match = _BUNDLE_SIZE_GOT.search(message)
        size = int(match.group(1)) if match else 0
        return ReviewServiceError(
            code="bundle_too_large",
            message="review bundle exceeds size limit",
            details=_failure_details(
                "bundle",
                last_good_digest,
                bytes=size,
                max_bytes=review_bundle.MAX_BUNDLE_BYTES,
            ),
        )
    return ReviewServiceError(
        code="map_invalid",
        message="map is invalid",
        details=_failure_details("bundle", last_good_digest),
    )


def _failure_details(
    failed_stage: str,
    last_good_digest: str | None,
    **extra: object,
) -> dict:
    details: dict[str, object] = {"failed_stage": failed_stage, **extra}
    if last_good_digest is not None:
        details["last_good_digest"] = last_good_digest
    return details


def _previous_source_report(last_good: bytes | None) -> SourceCheckReport | None:
    if last_good is None:
        return None
    try:
        payload = json.loads(last_good.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    report = payload.get("source_report")
    if not isinstance(report, dict):
        return None
    raw_items = report.get("items")
    items: list[SourceCheckItem] = []
    if isinstance(raw_items, list):
        for row in raw_items:
            if not isinstance(row, dict):
                continue
            pointer = row.get("pointer")
            code_ref = row.get("code_ref")
            status = row.get("status")
            kind = row.get("kind")
            if not isinstance(pointer, str) or not isinstance(code_ref, str):
                continue
            if not isinstance(status, str) or not isinstance(kind, str):
                continue
            detail = row.get("detail")
            digest = row.get("digest")
            items.append(
                SourceCheckItem(
                    pointer=pointer,
                    code_ref=code_ref,
                    status=status,  # type: ignore[arg-type]
                    kind=kind,  # type: ignore[arg-type]
                    detail=detail if isinstance(detail, str) else "",
                    digest=digest if isinstance(digest, str) else None,
                )
            )
    map_ref = report.get("map_ref")
    codemap_status = report.get("codemap_status")
    return SourceCheckReport(
        map_ref=map_ref if isinstance(map_ref, str) else "",
        codemap_status=codemap_status if isinstance(codemap_status, str) else "unsupported",  # type: ignore[arg-type]
        items=tuple(items),
    )


def _probe_refresh_lock(generated_dir: Path, workspace_root: Path) -> bool:
    path = refresh_lock_path(generated_dir, workspace_root=workspace_root)
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return False
        raise
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    finally:
        os.close(fd)


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = Path(tempfile.mkdtemp(prefix=f".{path.parent.name}.", dir=str(path.parent.parent)))
    name = path.name
    try:
        _write_fsync(tmp / name, data)
        os.replace(str(tmp / name), str(path))
        _fsync_dir(path.parent)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _swap_bundle(path: Path, data: bytes, prior: bytes | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        _atomic_write(path, data)
    except BaseException:
        _restore(path, prior)
        try:
            _fsync_dir(path.parent)
        except OSError:
            pass
        raise


def _write_fsync(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _restore(path: Path, data: bytes | None) -> None:
    if data is None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        return
    if _read_if_exists(path) == data:
        return
    _atomic_write(path, data)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _bundle_path(generated_dir: Path, map_ref: str) -> Path:
    return generated_dir / f"{map_ref}.review-bundle.json"


def _read_bundle_file(path: Path, *, max_bytes: int | None = None) -> bytes | None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno in (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP)):
            raise ReviewServiceError(
                code="path_outside_workspace",
                message="bundle file is not a regular workspace file",
            ) from exc
        return None
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise ReviewServiceError(
                code="path_outside_workspace",
                message="bundle file is not a regular workspace file",
            )
        if max_bytes is not None and file_stat.st_size > max_bytes:
            raise ReviewServiceError(code="bundle_too_large", message="bundle exceeds byte limit")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if max_bytes is not None and size > max_bytes:
                raise ReviewServiceError(code="bundle_too_large", message="bundle exceeds byte limit")
            chunks.append(chunk)
        return b"".join(chunks)
    except OSError:
        return None
    finally:
        os.close(fd)


def _read_inventory_asset(root: Path, parts: tuple[str, ...]) -> bytes:
    fds: list[int] = []
    try:
        fds.append(os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
        for part in parts[:-1]:
            fds.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fds[-1]))
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fds[-1])
        fds.append(fd)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ReviewServiceError(code="asset_not_found", message="asset not found")
        if info.st_size > MAX_ASSET_FILE_BYTES:
            raise ReviewServiceError(code="asset_changed", message="asset exceeds byte limit")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(fd, min(1024 * 1024, MAX_ASSET_FILE_BYTES + 1 - size))
            if not chunk:
                return b"".join(chunks)
            size += len(chunk)
            if size > MAX_ASSET_FILE_BYTES:
                raise ReviewServiceError(code="asset_changed", message="asset exceeds byte limit")
            chunks.append(chunk)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.EACCES, errno.EPERM):
            raise ReviewServiceError(code="asset_forbidden", message="asset path refused") from exc
        raise ReviewServiceError(code="asset_not_found", message="asset not found") from exc
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _read_if_exists(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError:
        return None


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()
