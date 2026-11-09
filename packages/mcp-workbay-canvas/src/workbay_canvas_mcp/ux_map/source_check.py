"""C05 source verification: file, symbol, and runtime-state checks."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from workbay_canvas_mcp.ux_map.models import UxMap

Status = Literal["declared", "verified", "missing", "unsupported", "stale"]
Kind = Literal["file", "symbol", "runtime_state"]
CodemapStatus = Literal["used", "unsupported"]

_RUNTIME_STATE_DETAIL = "map cannot prove a UI state is implemented"


class CodemapResolver(Protocol):
    def resolve_symbol(self, *, path: str, symbol: str) -> Status: ...


@dataclass(frozen=True, slots=True)
class SourceCheckItem:
    pointer: str
    code_ref: str
    status: Status
    kind: Kind
    detail: str
    digest: str | None = None


@dataclass(frozen=True, slots=True)
class SourceCheckReport:
    map_ref: str
    codemap_status: CodemapStatus
    items: tuple[SourceCheckItem, ...]


def source_report_to_dict(report: SourceCheckReport) -> dict:
    """Serialize a report with all six item keys; digest is null when absent."""
    return {
        "map_ref": report.map_ref,
        "codemap_status": report.codemap_status,
        "item_count": len(report.items),
        "items": [
            {
                "pointer": item.pointer,
                "code_ref": item.code_ref,
                "status": item.status,
                "kind": item.kind,
                "detail": item.detail,
                "digest": item.digest,
            }
            for item in report.items
        ],
    }


def verify_code_refs(
    map_: UxMap,
    *,
    workspace_root: Path | None = None,
    codemap: CodemapResolver | None = None,
    previous: SourceCheckReport | None = None,
    max_files: int = 200,
) -> SourceCheckReport:
    items: list[SourceCheckItem] = []
    files_opened = 0
    used_codemap = False
    root = Path(workspace_root).resolve() if workspace_root is not None else None
    previous_digests = _previous_file_digests(previous)

    def add_code_ref(pointer: str, code_ref: str) -> None:
        nonlocal files_opened, used_codemap
        path, symbol = _split_path_symbol(code_ref)
        file_item, files_opened = _check_file(
            pointer=pointer,
            code_ref=code_ref,
            path=path,
            root=root,
            files_opened=files_opened,
            max_files=max_files,
            previous_digests=previous_digests,
        )
        items.append(file_item)
        if symbol is None:
            return
        symbol_item, used = _check_symbol(
            pointer=pointer,
            code_ref=code_ref,
            path=path,
            symbol=symbol,
            root=root,
            file_item=file_item,
            codemap=codemap,
        )
        items.append(symbol_item)
        used_codemap = used_codemap or used

    for i, screen in enumerate(map_.screens):
        if screen.code_ref is not None:
            add_code_ref(f"/screens/{i}/code_ref", screen.code_ref)
        for n, state in enumerate(screen.states):
            items.append(_runtime_state_item(f"/screens/{i}/states/{n}", state))
        for j, zone in enumerate(screen.zones):
            if zone.code_ref is not None:
                add_code_ref(f"/screens/{i}/zones/{j}/code_ref", zone.code_ref)
            for n, state in enumerate(zone.states):
                items.append(_runtime_state_item(f"/screens/{i}/zones/{j}/states/{n}", state))
    for k, action in enumerate(map_.actions):
        if action.code_ref is not None:
            add_code_ref(f"/actions/{k}/code_ref", action.code_ref)

    return SourceCheckReport(
        map_ref=map_.map_ref,
        codemap_status="used" if used_codemap else "unsupported",
        items=tuple(items),
    )


def _runtime_state_item(pointer: str, state: object) -> SourceCheckItem:
    value = getattr(state, "value", state)
    return SourceCheckItem(
        pointer=pointer,
        code_ref=str(value),
        status="declared",
        kind="runtime_state",
        detail=_RUNTIME_STATE_DETAIL,
        digest=None,
    )


def _previous_file_digests(previous: SourceCheckReport | None) -> dict[tuple[str, str], str | None]:
    index: dict[tuple[str, str], str | None] = {}
    if previous is None:
        return index
    for item in previous.items:
        if item.kind == "file":
            index[(item.pointer, item.code_ref)] = item.digest
    return index


def _is_url(value: str) -> bool:
    return "://" in value


def _split_path_symbol(code_ref: str) -> tuple[str, str | None]:
    if _is_url(code_ref) or "\0" in code_ref:
        return code_ref, None
    if ":" not in code_ref:
        return code_ref, None
    path, symbol = code_ref.rsplit(":", 1)
    if path and symbol and "/" not in symbol and "\\" not in symbol and "\0" not in symbol:
        return path, symbol
    return code_ref, None


def _syntactic_path_error(path: str) -> str | None:
    if "\0" in path:
        return "path contains NUL"
    if _is_url(path):
        return "path is a URL"
    if not path:
        return "path is empty"
    candidate = Path(path)
    if candidate.is_absolute() or path.startswith("/") or path.startswith("\\"):
        return "path is absolute"
    if ".." in candidate.parts:
        return "path is not workspace-relative"
    return None


def _item(
    *,
    pointer: str,
    code_ref: str,
    status: Status,
    kind: Kind,
    detail: str,
    digest: str | None = None,
) -> SourceCheckItem:
    return SourceCheckItem(
        pointer=pointer,
        code_ref=code_ref,
        status=status,
        kind=kind,
        detail=detail,
        digest=digest,
    )


def _check_file(
    *,
    pointer: str,
    code_ref: str,
    path: str,
    root: Path | None,
    files_opened: int,
    max_files: int,
    previous_digests: dict[tuple[str, str], str | None],
) -> tuple[SourceCheckItem, int]:
    if root is None:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="declared",
                kind="file",
                detail="",
            ),
            files_opened,
        )
    error = _syntactic_path_error(path)
    if error is not None:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail=error,
            ),
            files_opened,
        )
    try:
        resolved = (root / path).resolve()
    except (RuntimeError, OSError):
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail="path cannot be resolved",
            ),
            files_opened,
        )
    if not resolved.is_relative_to(root):
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail="path escapes workspace",
            ),
            files_opened,
        )
    if files_opened >= max_files:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail="max_files",
            ),
            files_opened,
        )
    files_opened += 1
    try:
        st = os.stat(resolved)
    except (FileNotFoundError, NotADirectoryError):
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="missing",
                kind="file",
                detail="",
            ),
            files_opened,
        )
    except OSError:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail="unreadable",
            ),
            files_opened,
        )
    if not stat.S_ISREG(st.st_mode):
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail="not a regular file",
            ),
            files_opened,
        )
    digest_hash = hashlib.sha256()
    try:
        with open(resolved, "rb") as handle:
            while True:
                chunk = handle.read(65536)
                if not chunk:
                    break
                digest_hash.update(chunk)
    except OSError:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="file",
                detail="unreadable",
            ),
            files_opened,
        )
    digest = digest_hash.hexdigest()
    previous_digest = previous_digests.get((pointer, code_ref))
    status: Status = "stale" if previous_digest is not None and previous_digest != digest else "verified"
    return (
        _item(
            pointer=pointer,
            code_ref=code_ref,
            status=status,
            kind="file",
            detail="",
            digest=digest,
        ),
        files_opened,
    )


def _check_symbol(
    *,
    pointer: str,
    code_ref: str,
    path: str,
    symbol: str,
    root: Path | None,
    file_item: SourceCheckItem,
    codemap: CodemapResolver | None,
) -> tuple[SourceCheckItem, bool]:
    if root is None:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="declared",
                kind="symbol",
                detail="",
            ),
            False,
        )
    if file_item.status == "unsupported" and file_item.detail != "max_files":
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="symbol",
                detail=file_item.detail,
            ),
            False,
        )
    if codemap is None:
        return (
            _item(
                pointer=pointer,
                code_ref=code_ref,
                status="unsupported",
                kind="symbol",
                detail="codemap unavailable",
            ),
            False,
        )
    status = codemap.resolve_symbol(path=path, symbol=symbol)
    return (
        _item(
            pointer=pointer,
            code_ref=code_ref,
            status=status,
            kind="symbol",
            detail="",
        ),
        True,
    )
