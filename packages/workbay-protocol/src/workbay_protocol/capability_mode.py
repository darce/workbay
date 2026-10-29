"""Typed capability-mode reads (implementation note C1).

``ModeRead`` is the fail-loud contract for a single bounded ledger read.
``load_codemap_mode`` is the public caller that consumes those reads.
Legacy ``load_execution_mode`` stays a separate adapter and must not inherit
this 64 KiB cap or refuse-on-corrupt mapping.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from workbay_protocol import paths

MANIFEST_READ_LIMIT_BYTES = 64 * 1024
CODEMAP_MODE_FIELD = "codemap_mode"
CODEMAP_MODE_VALUES = frozenset({"enforced", "available", "off"})

ModeReadStatus = Literal[
    "ok",
    "manifest_missing",
    "field_missing",
    "unreadable",
    "malformed",
    "oversized",
    "invalid_value",
    "unsupported_schema",
    "policy_missing",
]
CodemapModeValue = Literal["enforced", "available", "off"]
ModeOrigin = Literal["explicit", "legacy_derived", "missing"]
ModeIntent = Literal["known", "unknown"]

_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)


@dataclass(frozen=True, slots=True)
class ModeRead:
    """Result of one bounded capability-mode read.

    ``origin`` is ``explicit``, ``legacy_derived``, or ``missing``.
    ``intent`` is ``known`` or ``unknown``. Omitted historical defaults
    cannot recover an operator's old intention, so missing fields stay
    ``missing`` / ``unknown`` rather than an explicit value.
    """

    status: ModeReadStatus
    value: CodemapModeValue | None
    origin: ModeOrigin
    intent: ModeIntent


def _typed_missing(status: ModeReadStatus) -> ModeRead:
    return ModeRead(status=status, value=None, origin="missing", intent="unknown")


def _select_manifest(
    root: Path,
    manifest_path: Path | str | None,
) -> Path | None:
    if manifest_path is not None:
        candidate = Path(manifest_path)
        try:
            os.lstat(candidate)
        except OSError:
            return None
        return candidate
    for name in paths.MANIFEST_NAME_PRECEDENCE:
        candidate = root / name
        try:
            os.lstat(candidate)
        except OSError:
            continue
        return candidate
    return None


def _read_manifest_bytes(path: Path) -> tuple[bytes | None, ModeReadStatus | None]:
    """Read at most 64 KiB from a regular file; never block on a pipe."""
    try:
        fd = os.open(path, _OPEN_FLAGS)
    except OSError:
        return None, "unreadable"
    try:
        try:
            info = os.fstat(fd)
        except OSError:
            return None, "unreadable"
        if not stat.S_ISREG(info.st_mode):
            return None, "unreadable"
        if info.st_size > MANIFEST_READ_LIMIT_BYTES:
            return None, "oversized"
        try:
            raw = os.read(fd, MANIFEST_READ_LIMIT_BYTES + 1)
        except OSError:
            return None, "unreadable"
    finally:
        os.close(fd)
    if len(raw) > MANIFEST_READ_LIMIT_BYTES:
        return None, "oversized"
    return raw, None


def _schema_version_supported(data: dict[object, object]) -> bool:
    if "schema_version" not in data:
        return True
    version = data["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        return False
    return version >= 1


def load_codemap_mode(
    repo_root: Path | str,
    *,
    enforcement_installed: bool = False,
    manifest_path: Path | str | None = None,
) -> ModeRead:
    """Read ``codemap_mode`` as a typed ``ModeRead``.

    ``enforcement_installed`` is the adapter's install-time expectation, not a
    second mutable mode. When it is true, a missing manifest or field is
    ``policy_missing`` (deleting a ledger must not disable a bought policy).
    Explicit valid ``off`` still wins.
    """
    root = Path(repo_root)
    selected = _select_manifest(root, manifest_path)
    if selected is None:
        if enforcement_installed:
            return _typed_missing("policy_missing")
        return _typed_missing("manifest_missing")

    raw, fail_status = _read_manifest_bytes(selected)
    if fail_status is not None or raw is None:
        return _typed_missing(fail_status or "unreadable")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return _typed_missing("unreadable")

    try:
        parsed = json.loads(text)
    except ValueError:
        return _typed_missing("malformed")
    if not isinstance(parsed, dict):
        return _typed_missing("malformed")

    if not _schema_version_supported(parsed):
        return _typed_missing("unsupported_schema")

    if CODEMAP_MODE_FIELD not in parsed or parsed[CODEMAP_MODE_FIELD] is None:
        if enforcement_installed:
            return _typed_missing("policy_missing")
        return _typed_missing("field_missing")

    value = parsed[CODEMAP_MODE_FIELD]
    if value not in CODEMAP_MODE_VALUES:
        return _typed_missing("invalid_value")
    return ModeRead(
        status="ok",
        value=value,
        origin="explicit",
        intent="known",
    )


__all__ = [
    "CODEMAP_MODE_FIELD",
    "CODEMAP_MODE_VALUES",
    "MANIFEST_READ_LIMIT_BYTES",
    "ModeRead",
    "load_codemap_mode",
]
