"""Git-owned Lab experiment files: one encoder and one serialized CAS writer.

No token/map writes, journal, durable undo or automatic Git operations. Raw-byte
readers reject duplicate keys. MCP arguments have already been decoded upstream.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workbay_canvas_mcp.review_service import _fsync_dir

MAX_FILE_BYTES = 1 << 20
MAX_OVERRIDES = 64
MAX_NAME = 120
MAX_TEXT = 8192
SLUG = re.compile(r"[a-z0-9][a-z0-9-]{0,63}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
TOKEN = re.compile(r"--[A-Za-z_][A-Za-z0-9_-]*\Z")
KIT_IDS = frozenset(
    f"kit:{name}" for name in ("Heading", "BodyText", "Button", "TextInput", "Card", "Badge", "Alert", "NavBar")
)
_after_compare: Callable[[], None] | None = None


class ExperimentValidationError(ValueError):
    def __init__(self, code: str, pointer: str = "", message: str = "") -> None:
        self.code, self.pointer = code, pointer
        super().__init__(message or code)


class ExperimentConflict(Exception):
    def __init__(
        self,
        expected: str | None = None,
        actual: str | None = None,
        conflict_path: str | None = None,
        *,
        reason: str = "conflict",
    ) -> None:
        self.reason, self.expected, self.actual, self.conflict_path = reason, expected, actual, conflict_path
        super().__init__(reason)

    def as_dict(self) -> dict[str, Any]:
        if self.reason == "exists":
            return {"error": "exists"}
        return {
            "error": "conflict",
            "expected": self.expected,
            "actual": self.actual,
            "conflict_path": self.conflict_path,
        }


@dataclass(frozen=True)
class SaveReceipt:
    slug: str
    sha256: str
    path: str


@dataclass(frozen=True)
class ExperimentRead:
    document: dict[str, Any]
    sha256: str
    outdated: bool
    orphaned: bool
    raw: bytes


def _pointer(name: str) -> str:
    return name.replace("~", "~0").replace("/", "~1")


def _invalid(pointer: str, message: str, code: str = "invalid_document") -> None:
    raise ExperimentValidationError(code, pointer, message)


class _DuplicateObject(dict[str, Any]):
    """Retain the first duplicate's name until its full pointer is known."""

    def __init__(self, values: dict[str, Any], key: str) -> None:
        super().__init__(values)
        self.duplicate = key


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    duplicate = None
    for key, value in pairs:
        if key in result and duplicate is None:
            duplicate = key
        result[key] = value
    return _DuplicateObject(result, duplicate) if duplicate is not None else result


def _check_duplicates(value: Any, pointer: str = "") -> None:
    if isinstance(value, _DuplicateObject):
        _invalid(pointer + "/" + _pointer(value.duplicate), f"duplicate key: {value.duplicate}", "duplicate_key")
    if isinstance(value, dict):
        for key, child in value.items():
            _check_duplicates(child, pointer + "/" + _pointer(key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _check_duplicates(child, pointer + "/" + str(index))


def decode_experiment(raw: bytes) -> dict[str, Any]:
    """Raw HTTP, CLI input and *all* experiment-file reads use this decoder."""
    if len(raw) > MAX_FILE_BYTES:
        _invalid("", "experiment exceeds 1 MiB", "file_too_large")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicates,
            parse_constant=lambda value: _invalid("", f"invalid JSON constant: {value}"),
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, ExperimentValidationError):
            raise
        raise ExperimentValidationError("invalid_json", "", "invalid JSON document") from exc
    try:
        _check_duplicates(value)
    except RecursionError as exc:
        raise ExperimentValidationError("invalid_json", "", "JSON nesting is too deep") from exc
    if not isinstance(value, dict):
        _invalid("", "experiment must be an object")
    return value


def encode_experiment(document: dict[str, Any]) -> bytes:
    """Canonical UTF-8 JSON; preserve string values verbatim, including spaces."""
    try:
        return (json.dumps(document, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ExperimentValidationError("invalid_document", "", "document cannot be encoded as JSON") from exc


def _slug(slug: str) -> str:
    if not isinstance(slug, str) or SLUG.fullmatch(slug) is None:
        _invalid("/slug", "slug must match ^[a-z0-9][a-z0-9-]{0,63}$", "invalid_slug")
    return slug


def _parts(relative: str, pointer: str) -> tuple[str, ...]:
    if (
        not isinstance(relative, str)
        or not relative
        or len(relative) > MAX_TEXT
        or "\\" in relative
        or ":" in relative
        or "\x00" in relative
        or any(p in ("", ".", "..") for p in relative.split("/"))
    ):
        _invalid(pointer, "path must be workspace-relative without traversal", "unsafe_path")
    return tuple(relative.split("/"))


def _sync_dir(fd: int) -> None:
    # Reuse the existing fsync helper through the already opened directory handle.
    # /dev/fd is supported on the POSIX hosts on which flock is supported here;
    # it keeps a concurrent directory rename/symlink from redirecting the fsync.
    _fsync_dir(Path("/dev/fd") / str(fd))


@contextmanager
def _directory(root: str | Path, parts: tuple[str, ...], *, create: bool = False) -> Iterator[int]:
    """Walk using no-follow directory descriptors; never follow workspace links."""
    path = Path(root).absolute()
    if path != path.resolve():
        _invalid("", "workspace root must not contain symlinks", "unsafe_path")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        for part in parts:
            if create:
                try:
                    os.mkdir(part, mode=0o755, dir_fd=fd)
                except FileExistsError:
                    pass
                else:
                    # Each newly created level becomes durable before descending.
                    _sync_dir(fd)
            try:
                child = os.open(part, flags, dir_fd=fd)
            except OSError as exc:
                if exc.errno in (20, 40):  # ENOTDIR / ELOOP
                    raise ExperimentValidationError("unsafe_path", "", "symlink or non-directory in path") from exc
                raise
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def _regular(fd: int, pointer: str) -> None:
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        _invalid(pointer, "expected a regular file", "unsafe_path")


def _read_at(fd: int, name: str, *, pointer: str = "", limit: int = MAX_FILE_BYTES) -> bytes:
    try:
        file_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    except OSError as exc:
        if exc.errno in (20, 40):
            raise ExperimentValidationError("unsafe_path", pointer, "symlink file refused") from exc
        raise
    try:
        _regular(file_fd, pointer)
        if os.fstat(file_fd).st_size > limit:
            _invalid(pointer, "file exceeds byte limit", "file_too_large")
        with os.fdopen(file_fd, "rb", closefd=False) as source:
            content = source.read(limit + 1)
        if len(content) > limit:
            _invalid(pointer, "file exceeds byte limit", "file_too_large")
        return content
    finally:
        os.close(file_fd)


def _read_relative(root: str | Path, relative: str, *, pointer: str = "", limit: int = MAX_FILE_BYTES) -> bytes:
    parts = _parts(relative, pointer)
    with _directory(root, parts[:-1]) as fd:
        return _read_at(fd, parts[-1], pointer=pointer, limit=limit)


def _check_relative(root: str | Path, relative: str, pointer: str) -> None:
    """Permit missing bases (Outdated) but refuse any existing symlink prefix."""
    parts = _parts(relative, pointer)
    try:
        with _directory(root, parts[:-1]) as fd:
            try:
                info = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if not stat.S_ISREG(info.st_mode):
                _invalid(pointer, "base must be a regular, non-symlink file", "unsafe_path")
    except FileNotFoundError:
        return
    except ExperimentValidationError as exc:
        raise ExperimentValidationError(exc.code, pointer, str(exc)) from exc


def _object(value: Any, pointer: str, required: set[str], optional: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        _invalid(pointer, "expected an object")
    for key in required:
        if key not in value:
            _invalid(pointer + "/" + key, "required field")
    for key in value:
        if not isinstance(key, str) or key not in required | (optional or set()):
            _invalid(pointer + "/" + _pointer(str(key)), "unknown field")
    return value


def _text(value: Any, pointer: str, *, name: bool = False, empty: bool = True) -> None:
    if not isinstance(value, str) or len(value) > (MAX_NAME if name else MAX_TEXT) or (not empty and not value):
        _invalid(pointer, f"expected string of at most {MAX_NAME if name else MAX_TEXT} characters")


def _values(value: Any, pointer: str, *, bindings: bool = False) -> None:
    if not isinstance(value, dict) or len(value) > MAX_OVERRIDES:
        _invalid(pointer, "expected at most 64 named values")
    for key, text in value.items():
        if not isinstance(key, str) or len(key) > MAX_NAME or not TOKEN.fullmatch(key):
            _invalid(pointer, "invalid custom property name (maximum 120 characters)")
        _text(text, pointer + "/" + _pointer(key))
        if bindings and text and (len(text) > MAX_NAME or TOKEN.fullmatch(text) is None):
            _invalid(
                pointer + "/" + _pointer(key), "binding must be a custom property name or empty kit-default selection"
            )


def validate_experiment(root: str | Path, document: dict[str, Any]) -> None:
    doc = _object(document, "", {"schema_version", "base", "overrides", "layout", "target"}, {"bindings", "note"})
    if type(doc["schema_version"]) is not int or doc["schema_version"] != 1:
        _invalid("/schema_version", "schema_version must be 1")
    base = _object(doc["base"], "/base", {"tokens_css", "sha256"})
    _check_relative(root, base["tokens_css"], "/base/tokens_css")
    if not isinstance(base["sha256"], str) or not SHA256.fullmatch(base["sha256"]):
        _invalid("/base/sha256", "expected exact-file SHA256 in lowercase hex")
    _values(doc["overrides"], "/overrides")
    if "bindings" in doc:
        _values(doc["bindings"], "/bindings", bindings=True)
    layout = _object(doc["layout"], "/layout", {"columns", "gutter", "viewport_presets"}, {"defaults"})
    if type(layout["columns"]) is not int or layout["columns"] < 1:
        _invalid("/layout/columns", "columns must be a positive integer")
    _text(layout["gutter"], "/layout/gutter", empty=False)
    widths = layout["viewport_presets"]
    if not isinstance(widths, list) or not widths or any(type(w) is not int or w < 1 for w in widths):
        _invalid("/layout/viewport_presets", "expected positive integer viewport widths")
    if "defaults" in layout:
        _values(layout["defaults"], "/layout/defaults")
    target = doc["target"]
    if not isinstance(target, dict):
        _invalid("/target", "expected a target")
    if target.get("kind") == "component":
        _object(target, "/target", {"kind", "component_id"})
        _text(target["component_id"], "/target/component_id", name=True, empty=False)
    elif target.get("kind") == "screen":
        _object(target, "/target", {"kind", "screen_id", "state"}, {"zone_id"})
        _text(target["screen_id"], "/target/screen_id", name=True, empty=False)
        if target["state"] is not None:
            _text(target["state"], "/target/state", name=True, empty=False)
        if "zone_id" in target:
            _text(target["zone_id"], "/target/zone_id", name=True, empty=False)
    else:
        _invalid("/target/kind", "expected component or screen")
    if "note" in doc:
        _text(doc["note"], "/note")
    if len(encode_experiment(doc)) > MAX_FILE_BYTES:
        _invalid("", "encoded experiment exceeds 1 MiB", "file_too_large")


def _badges(root: str | Path, document: dict[str, Any]) -> tuple[bool, bool]:
    base = document["base"]
    try:
        current = _read_relative(root, base["tokens_css"], pointer="/base/tokens_css", limit=8 << 20)
    except FileNotFoundError:
        outdated = True
    else:
        outdated = hashlib.sha256(current).hexdigest() != base["sha256"]
    target = document["target"]
    if target["kind"] == "component" and target["component_id"] in KIT_IDS:
        return outdated, False
    try:
        raw = _read_relative(root, "docs/design/design-sources.json")
    except FileNotFoundError:
        return outdated, True
    from workbay_canvas_mcp.design_inventory.config import parse_design_sources
    from workbay_canvas_mcp.ux_map.parse import parse_map

    config = parse_design_sources(decode_experiment(raw))
    if target["kind"] == "component":
        return outdated, target["component_id"] not in {item.id for item in config.components}
    for relative in config.maps:
        try:
            raw = _read_relative(root, relative)
        except FileNotFoundError:
            continue
        map_ = parse_map(decode_experiment(raw))
        for screen in map_.screens:
            if screen.id != target["screen_id"]:
                continue
            states = [s.value for s in screen.states]
            state_ok = target["state"] in states if target["state"] is not None else not states
            zone_ok = "zone_id" not in target or target["zone_id"] in {zone.id for zone in screen.zones}
            if state_ok and zone_ok:
                return outdated, False
    return outdated, True


def read_experiment(workspace_root: str | Path, slug: str) -> ExperimentRead:
    relative = f"design/experiments/{_slug(slug)}.lab.json"
    raw = _read_relative(workspace_root, relative)
    document = decode_experiment(raw)
    validate_experiment(workspace_root, document)
    outdated, orphaned = _badges(workspace_root, document)
    return ExperimentRead(document, hashlib.sha256(raw).hexdigest(), outdated, orphaned, raw)


def list_experiments(workspace_root: str | Path) -> list[dict[str, Any]]:
    try:
        with _directory(workspace_root, ("design", "experiments")) as fd:
            names = sorted(os.listdir(fd))
    except FileNotFoundError:
        return []
    result = []
    for name in names:
        if not name.endswith(".lab.json"):
            continue
        slug = _slug(name[:-9])
        loaded = read_experiment(workspace_root, slug)
        result.append(
            {
                "slug": slug,
                "sha256": loaded.sha256,
                "target": loaded.document["target"],
                "outdated": loaded.outdated,
                "orphaned": loaded.orphaned,
            }
        )
    return result


def _publish(fd: int, name: str, data: bytes, *, exclusive: bool, seam: bool = False) -> None:
    temp = f".{name}.{uuid.uuid4().hex}.tmp"
    file_fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
    try:
        with os.fdopen(file_fd, "wb", closefd=False) as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        if seam and _after_compare is not None:
            _after_compare()
        if exclusive:
            os.link(temp, name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
            os.unlink(temp, dir_fd=fd)
        else:
            os.replace(temp, name, src_dir_fd=fd, dst_dir_fd=fd)
        _sync_dir(fd)
    finally:
        os.close(file_fd)
        try:
            os.unlink(temp, dir_fd=fd)
        except FileNotFoundError:
            pass


def save_experiment(
    workspace_root: str | Path,
    slug: str,
    document: dict[str, Any],
    *,
    if_match: str | None = None,
    create: bool = False,
) -> SaveReceipt:
    """The only browser/CLI/MCP writer. Never allow a blind overwrite."""
    slug = _slug(slug)
    if type(create) is not bool or create == (if_match is not None):
        _invalid("", "choose exactly one of create=True or if_match", "precondition_required")
    if if_match is not None and (not isinstance(if_match, str) or not SHA256.fullmatch(if_match)):
        _invalid("/if_match", "expected lowercase raw-file SHA256")
    # Freeze the submitted value before waiting for a writer lock.
    data = encode_experiment(document)
    document = decode_experiment(data)
    validate_experiment(workspace_root, document)
    with _directory(workspace_root, ("design", "experiments"), create=True) as directory:
        with _directory(workspace_root, (".task-state", "canvas-mcp", "experiment-locks"), create=True) as locks:
            try:
                lock = os.open(
                    f"{slug}.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=locks
                )
            except OSError as exc:
                if exc.errno in (20, 40):
                    raise ExperimentValidationError("unsafe_path", "", "symlink lock refused") from exc
                raise
            try:
                _regular(lock, "")
                fcntl.flock(lock, fcntl.LOCK_EX)
                name = f"{slug}.lab.json"
                try:
                    info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    info = None
                if info is not None and not stat.S_ISREG(info.st_mode):
                    _invalid("", "symlink or non-regular experiment refused", "unsafe_path")
                if not create:
                    try:
                        current = _read_at(directory, name)
                    except FileNotFoundError:
                        actual = None
                    else:
                        decode_experiment(current)  # reject duplicate keys on every file read
                        actual = hashlib.sha256(current).hexdigest()
                    if actual != if_match:
                        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
                        conflict = f"{slug}.lab.conflict-{stamp}-{uuid.uuid4().hex}.json"
                        _publish(directory, conflict, data, exclusive=True)
                        raise ExperimentConflict(if_match, actual, f"design/experiments/{conflict}")
                try:
                    _publish(directory, name, data, exclusive=create, seam=True)
                except FileExistsError as exc:
                    raise ExperimentConflict(reason="exists") from exc
                return SaveReceipt(slug, hashlib.sha256(data).hexdigest(), f"design/experiments/{name}")
            finally:
                os.close(lock)  # closes and releases flock only after directory fsync
