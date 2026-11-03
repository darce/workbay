"""Typed authorization records for predecessor retirement during lane landing.

``lane_land`` should resolve the successor's declaration with
``resolve_declared_tip`` and pass the observed predecessor tip to
``authorize_initial``. It must persist an ``Authorized.record`` before writing
the tombstone. On replay, a verified tombstone answers without a record; a
replay that writes must load the original record and call ``check_replay``.
The caller may delete a record only after verifying the tombstone for its
original landing. All branch-tip reads stay in the caller; this module has
no Git dependency.

Only a tip pinned by the declaration authorizes retirement. Fresh
declarations without a readable tip carry ``supersedes_unbound`` and are
refused as ``declaration_unbound``; legacy entries with neither field are
refused as ``declared_tip_missing``. The observed tip is never bound in their
place, and records or tombstones an earlier landing bound that way grant no
authority (``check_replay``, ``check_landing_tombstone``).
"""

from __future__ import annotations

import errno
import json
import os
import secrets
import stat
import threading
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class SupersedeAuthorization:
    """The predecessor tip a successor was authorized to retire."""

    successor: str
    predecessor: str
    predecessor_branch: str
    declared_tip: str | None
    authorized_tip: str
    recorded_at: str
    source: Literal["declared", "bound_at_landing"]


@dataclass(frozen=True, slots=True)
class Authorized:
    """Successful first-landing authorization and the record to persist."""

    record: SupersedeAuthorization


@dataclass(frozen=True, slots=True)
class Refused:
    """A typed refusal to authorize or replay predecessor retirement."""

    reason: str
    declared: str | None = None
    authorized: str | None = None
    current: str | None = None


@dataclass(frozen=True, slots=True)
class Unbound:
    """The manifest does not bind this predecessor to a declared tip."""

    reason: str


@dataclass(frozen=True, slots=True)
class Ok:
    """The persisted authorization still matches the live predecessor tip."""

    record: SupersedeAuthorization


@dataclass(frozen=True, slots=True)
class Persisted:
    """A record was atomically written to ``path``."""

    path: Path
    record: SupersedeAuthorization


@dataclass(frozen=True, slots=True)
class Deleted:
    """A record was removed, or was already absent."""

    path: Path


@dataclass(frozen=True, slots=True)
class RecordError:
    """A typed persistence failure that callers can branch on directly."""

    reason: Literal[
        "record_missing", "record_unreadable", "record_unwritable", "record_unsafe_path", "record_undeletable"
    ]
    path: Path
    detail: str


def resolve_declared_tip(manifest_entry: Mapping[str, Any], predecessor: str) -> str | Unbound:
    """Read the declaration-time SHA for ``predecessor`` from a successor entry."""
    unbound = manifest_entry.get("supersedes_unbound")
    if isinstance(unbound, Mapping) and predecessor in unbound:
        reason = unbound[predecessor]
        if isinstance(reason, str) and reason.strip() and reason != "declared_tip_missing":
            return Unbound(reason=reason)
        return Unbound(reason="declaration_unbound")
    tips = manifest_entry.get("supersedes_tips")
    if not isinstance(tips, Mapping):
        return Unbound(reason="declared_tip_missing")
    tip = tips.get(predecessor)
    if not isinstance(tip, str) or not tip.strip():
        return Unbound(reason="declared_tip_missing")
    return tip


def unpinned_declaration_refusal(declared_tip: Unbound | None, current_tip: str | None = None) -> Refused:
    """Name why a declaration without a pinned tip grants no authority; missing data is not consent."""
    if isinstance(declared_tip, Unbound) and declared_tip.reason != "declared_tip_missing":
        return Refused(reason="declaration_unbound", current=current_tip)
    return Refused(reason="declared_tip_missing", current=current_tip)


def authorize_initial(
    declared_tip: str | Unbound | None,
    current_tip: str,
    *,
    successor: str,
    predecessor: str,
    predecessor_branch: str,
    recorded_at: str,
) -> Authorized | Refused:
    """Authorize exactly the declared tip; never bind the observed tip in its place."""
    if not isinstance(declared_tip, str) or not declared_tip.strip():
        return unpinned_declaration_refusal(declared_tip if isinstance(declared_tip, Unbound) else None, current_tip)
    if not isinstance(current_tip, str) or not current_tip.strip():
        return Refused(reason="predecessor_tip_unavailable", declared=declared_tip, current=current_tip)
    if current_tip != declared_tip:
        return Refused(reason="predecessor_advanced", declared=declared_tip, current=current_tip)
    if not successor.strip() or not predecessor.strip() or not predecessor_branch.strip() or not recorded_at.strip():
        return Refused(reason="authorization_identity_incomplete", declared=declared_tip, current=current_tip)

    return Authorized(
        record=SupersedeAuthorization(
            successor=successor,
            predecessor=predecessor,
            predecessor_branch=predecessor_branch,
            declared_tip=declared_tip,
            authorized_tip=current_tip,
            recorded_at=recorded_at,
            source="declared",
        )
    )


def check_replay(record: SupersedeAuthorization, current_tip: str) -> Ok | Refused:
    """Require a declared record and the predecessor tip it authorized at first landing.

    ``bound_at_landing`` records were written before landing refused unpinned
    declarations; they stay readable so replay can refuse them by name.
    """
    if record.source != "declared" or record.declared_tip is None:
        return Refused(reason="authorization_unbound", authorized=record.authorized_tip, current=current_tip)
    if record.declared_tip != record.authorized_tip:
        return Refused(
            reason="authorization_declaration_mismatch",
            declared=record.declared_tip,
            authorized=record.authorized_tip,
            current=current_tip,
        )
    if current_tip != record.authorized_tip:
        return Refused(reason="predecessor_moved", authorized=record.authorized_tip, current=current_tip)
    return Ok(record=record)


def check_landing_tombstone(manifest: Mapping[str, Any], successor: str, *, branch: str, tip: str) -> Refused | None:
    """Refuse a tombstone carried by ``successor``'s landing unless its declaration pinned ``tip``.

    Returns ``None`` only when the successor's entry declares a predecessor
    whose branch is ``branch`` and pins exactly ``tip``. Everything else is a
    typed refusal: an absent successor or predecessor entry, an entry whose
    declaration was dropped by terminal compaction, or a declaration that never
    names ``branch``. Missing evidence is a designed unknown, never consent.
    """
    lanes = manifest.get("lanes")
    if not isinstance(lanes, Mapping):
        return Refused(reason="declaration_invalid", current=tip)
    entry = lanes.get(successor)
    if entry is None:
        return Refused(reason="declaration_successor_missing", current=tip)
    if not isinstance(entry, Mapping):
        return Refused(reason="declaration_invalid", current=tip)
    predecessors = entry.get("supersedes")
    if predecessors is None:
        # Terminal compaction keeps identity but drops every supersedes field.
        reason = "declaration_compacted" if "compacted" in entry else "declaration_missing"
        return Refused(reason=reason, current=tip)
    if not isinstance(predecessors, list):
        return Refused(reason="declaration_invalid", current=tip)
    refused: Refused | None = None
    predecessor_missing = False
    for predecessor in predecessors:
        predecessor_entry = lanes.get(predecessor) if isinstance(predecessor, str) else None
        if not isinstance(predecessor_entry, Mapping):
            predecessor_missing = True
            continue
        declared_branch = predecessor_entry.get("branch")
        if not isinstance(declared_branch, str) or declared_branch.strip().removeprefix("refs/heads/") != branch:
            continue
        declared_tip = resolve_declared_tip(entry, predecessor)
        if declared_tip == tip:
            return None
        if refused is None and isinstance(declared_tip, str):
            refused = Refused(reason="declared_tip_mismatch", declared=declared_tip, current=tip)
        elif refused is None:
            refused = unpinned_declaration_refusal(declared_tip, tip)
    if refused is not None:
        return refused
    # An unresolvable predecessor could have named this branch; either way no pin is shown.
    return Refused(
        reason="declaration_predecessor_missing" if predecessor_missing else "declaration_missing", current=tip
    )


# Mirrors lane_manifest.MANIFEST_BYTES_LIMIT; a larger file is refused unread.
_MANIFEST_BYTES_LIMIT = 1024 * 1024
# Reads that outlive their caller's deadline keep a slot until storage answers;
# a full bulkhead refuses new reads instead of stacking blocked threads.
_MANIFEST_READ_SLOTS = threading.BoundedSemaphore(4)


@dataclass(frozen=True, slots=True)
class ManifestUnavailable:
    """Why a declaration manifest could not be read as authority evidence."""

    reason: str


def _safe_task_component(task_ref: object) -> bool:
    return (
        isinstance(task_ref, str)
        and bool(task_ref.strip())
        and task_ref not in {".", ".."}
        and "/" not in task_ref
        and "\\" not in task_ref
        and all(32 <= ord(char) <= 126 for char in task_ref)
    )


def _manifest_location(root: Path | str, manifest_dir: Path | str | None) -> tuple[Path, tuple[str, ...]]:
    """Anchor the default directory at the repository and walk it without links."""
    from . import lane_manifest

    directory = lane_manifest.manifest_dir(orchestrator_root=str(root), manifest_dir=manifest_dir)
    repository = Path(root).resolve()
    if directory == (repository / "config" / "lane-orchestration").resolve():
        return repository, ("config", "lane-orchestration")
    return directory, ()


def _read_manifest_bytes(anchor: Path, components: tuple[str, ...], name: str) -> bytes:
    descriptor = os.open(anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for component in components:
            next_descriptor = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        # Refuse links and special files before opening them at all.
        before = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise _UnsafeRecordPath("manifest_not_regular")
        fd = os.open(name, _READ_FLAGS, dir_fd=descriptor)
    finally:
        os.close(descriptor)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != (before.st_dev, before.st_ino):
            raise _UnsafeRecordPath("manifest_not_regular")
        if info.st_size > _MANIFEST_BYTES_LIMIT:
            raise _UnsafeRecordPath("manifest_too_large")
        chunks: list[bytes] = []
        remaining = _MANIFEST_BYTES_LIMIT + 1
        while remaining > 0:
            chunk = os.read(fd, min(remaining, 65536))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        if remaining <= 0:
            raise _UnsafeRecordPath("manifest_too_large")
        return b"".join(chunks)
    finally:
        os.close(fd)


class _ManifestLocationError(Exception):
    """Resolving the manifest directory failed; keeps the pre-read refusal shape."""


def _manifest_failure(exc: BaseException) -> str:
    if isinstance(exc, _ManifestLocationError):
        return f"manifest_unreadable:{exc}"
    if isinstance(exc, FileNotFoundError):
        return "manifest_missing"
    if isinstance(exc, _UnsafeRecordPath):
        return str(exc)
    if isinstance(exc, OSError) and exc.errno in {errno.ELOOP, errno.ENOTDIR}:
        return "manifest_unsafe_path"
    return f"manifest_unreadable:{type(exc).__name__}"


def read_declaration_manifest(
    root: Path | str,
    task_ref: str,
    *,
    deadline: float,
    manifest_dir: Path | str | None = None,
) -> dict[str, Any] | ManifestUnavailable:
    """Read ``task_ref``'s manifest as authority evidence before the monotonic ``deadline``.

    Every component is opened through a directory descriptor without following
    links; only a regular file within the size limit is read. Path resolution
    and storage IO run on an admitted helper thread so a stalled lookup or read
    costs the caller at most its deadline; the helper holds no lock, so no
    holder ever waits on it.
    """
    if not _safe_task_component(task_ref):
        return ManifestUnavailable("unsafe_task_ref")
    if deadline - time.monotonic() <= 0:
        return ManifestUnavailable("manifest_deadline_exhausted")
    if not _MANIFEST_READ_SLOTS.acquire(blocking=False):
        return ManifestUnavailable("manifest_reader_busy")
    outcome: list[bytes | BaseException] = []

    def read() -> None:
        try:
            try:
                anchor, components = _manifest_location(root, manifest_dir)
            except (OSError, RuntimeError, ValueError) as exc:
                raise _ManifestLocationError(type(exc).__name__) from exc
            outcome.append(_read_manifest_bytes(anchor, components, f"{task_ref}.json"))
        except BaseException as exc:  # noqa: BLE001 - every failure is a typed refusal
            outcome.append(exc)
        finally:
            _MANIFEST_READ_SLOTS.release()

    reader = threading.Thread(target=read, name=f"supersede-manifest-{task_ref}", daemon=True)
    reader.start()
    reader.join(max(0.0, deadline - time.monotonic()))
    if not outcome:
        return ManifestUnavailable("manifest_timeout")
    payload = outcome[0]
    if isinstance(payload, BaseException):
        return ManifestUnavailable(_manifest_failure(payload))
    try:
        manifest = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        return ManifestUnavailable(f"manifest_unreadable:{type(exc).__name__}")
    if not isinstance(manifest, dict):
        return ManifestUnavailable("manifest_invalid")
    if manifest.get("task_ref") != task_ref:
        return ManifestUnavailable("manifest_task_ref_mismatch")
    return manifest


# Mirror audit_preservation.py:21-23,118-153: all access is anchored to
# directory descriptors and each component is opened without following links.
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_TEMP_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


class _UnsafeRecordPath(ValueError):
    pass


def _record_target(root: Path | str, components: tuple[str, ...]) -> Path:
    if (
        len(components) != 5
        or components[:2] != (".task-state", "lane-supersede-authorizations")
        or any(
            not isinstance(name, str)
            or not name.strip()
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or any(ord(char) < 32 or ord(char) > 126 for char in name)
            for name in components
        )
    ):
        raise _UnsafeRecordPath("invalid authorization components")
    return Path(root).resolve().joinpath(*components)


def _open_directory_at(parent: int, name: str, *, create: bool) -> int:
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
    except FileNotFoundError:
        if not create:
            raise
        try:
            os.mkdir(name, 0o700, dir_fd=parent)
        except FileExistsError:
            pass  # A concurrent creator must still pass the no-follow open.
        os.fsync(parent)
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)


@contextmanager
def _record_directory(root: Path | str, components: tuple[str, ...], *, create: bool):
    _record_target(root, components)
    descriptor = os.open(Path(root).resolve(), _DIRECTORY_FLAGS)
    try:
        for name in components[:-1]:
            next_descriptor = _open_directory_at(descriptor, name, create=create)
            os.close(descriptor)
            descriptor = next_descriptor
        yield descriptor
    finally:
        os.close(descriptor)


def _record_error(target: Path, exc: Exception, fallback: str) -> RecordError:
    if isinstance(exc, _UnsafeRecordPath) or isinstance(exc, OSError) and exc.errno in {errno.ELOOP, errno.ENOTDIR}:
        reason = "record_unsafe_path"
    elif isinstance(exc, FileNotFoundError) and fallback == "record_unreadable":
        reason = "record_missing"
    else:
        reason = fallback
    return RecordError(reason=reason, path=target, detail=str(exc))


def _require_regular_at(directory: int, name: str) -> None:
    try:
        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode):
        raise _UnsafeRecordPath("authorization record is not a regular file")


def _publish_record(directory: int, name: str, record: SupersedeAuthorization) -> None:
    _require_regular_at(directory, name)
    temporary = f".tmp-{secrets.token_hex(16)}"
    fd = os.open(temporary, _TEMP_FLAGS, 0o600, dir_fd=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as stream:
            json.dump(_record_json(record), stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    finally:
        os.close(fd)
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass


def persist(root: Path | str, components: tuple[str, ...], record: SupersedeAuthorization) -> Persisted | RecordError:
    """Atomically publish a record through the repository descriptor boundary."""
    target = Path(root)
    try:
        target = _record_target(root, components)
        with _record_directory(root, components, create=True) as directory:
            _publish_record(directory, components[-1], record)
    except (OSError, ValueError) as exc:
        return _record_error(target, exc, "record_unwritable")
    return Persisted(path=target, record=record)


def load(root: Path | str, components: tuple[str, ...]) -> SupersedeAuthorization | RecordError:
    """Read a regular record without following any link below the repository."""
    target = Path(root)
    try:
        target = _record_target(root, components)
        with _record_directory(root, components, create=False) as directory:
            fd = os.open(components[-1], _READ_FLAGS, dir_fd=directory)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise _UnsafeRecordPath("authorization record is not a regular file")
                with os.fdopen(fd, "r", encoding="utf-8", closefd=False) as stream:
                    value = json.load(stream)
            finally:
                os.close(fd)
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        return _record_error(target, exc, "record_unreadable")
    record = _record_from_json(value)
    if record is None:
        return RecordError(reason="record_unreadable", path=target, detail="authorization record has an invalid shape")
    return record


def delete(root: Path | str, components: tuple[str, ...]) -> Deleted | RecordError:
    """Reclaim one verified authorization through the descriptor boundary.

    Retain shared task/lane directories: this standalone API does not require
    the landing mutation lock, so pruning parents could race another writer.
    """
    target = Path(root)
    try:
        target = _record_target(root, components)
        with _record_directory(root, components, create=False) as directory:
            _require_regular_at(directory, components[-1])
            os.unlink(components[-1], dir_fd=directory)
            os.fsync(directory)
    except FileNotFoundError:
        return Deleted(path=target)
    except (OSError, ValueError) as exc:
        return _record_error(target, exc, "record_undeletable")
    return Deleted(path=target)


def _record_json(record: SupersedeAuthorization) -> dict[str, str | None]:
    return {
        "successor": record.successor,
        "predecessor": record.predecessor,
        "predecessor_branch": record.predecessor_branch,
        "declared_tip": record.declared_tip,
        "authorized_tip": record.authorized_tip,
        "recorded_at": record.recorded_at,
        "source": record.source,
    }


def _record_from_json(value: Any) -> SupersedeAuthorization | None:
    if not isinstance(value, Mapping):
        return None
    successor = value.get("successor")
    predecessor = value.get("predecessor")
    predecessor_branch = value.get("predecessor_branch")
    declared_tip = value.get("declared_tip")
    authorized_tip = value.get("authorized_tip")
    recorded_at = value.get("recorded_at")
    source = value.get("source")
    if (
        not isinstance(successor, str)
        or not successor.strip()
        or not isinstance(predecessor, str)
        or not predecessor.strip()
        or not isinstance(predecessor_branch, str)
        or not predecessor_branch.strip()
        or declared_tip is not None
        and (not isinstance(declared_tip, str) or not declared_tip.strip())
        or not isinstance(authorized_tip, str)
        or not authorized_tip.strip()
        or not isinstance(recorded_at, str)
        or not recorded_at.strip()
        or not isinstance(source, str)
        or source not in {"declared", "bound_at_landing"}
    ):
        return None
    return SupersedeAuthorization(
        successor=successor,
        predecessor=predecessor,
        predecessor_branch=predecessor_branch,
        declared_tip=declared_tip,
        authorized_tip=authorized_tip,
        recorded_at=recorded_at,
        source=source,
    )
