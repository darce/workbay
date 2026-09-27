"""Durably preserve sole-copy audit files before their worktree is reaped.

Payloads are addressed by the canonical full worktree path, the worktree-
relative source path, and a content hash. Publishing uses an fsynced temporary
file and an atomic no-replace hard link, so retries are idempotent and earlier
versions are never overwritten.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import stat
from pathlib import Path
from typing import Iterable

_ARCHIVE_RELATIVE = (".task-state", "branch-archive", "reaped-audit")
_COPY_CHUNK_SIZE = 1024 * 1024

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
_TEMP_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


def preserve_audit_trails(
    worktree_path: str | Path,
    repo_root: str | Path,
    relative_paths: Iterable[str],
) -> frozenset[str]:
    """Copy and verify each audit path, returning only durable relative paths.

    Invalid paths and per-file I/O failures are omitted from the result. The
    caller must continue treating omitted paths as sole copies that block
    deletion. This helper never removes or changes a source file.
    """
    try:
        worktree = Path(worktree_path).resolve(strict=True)
        repository = Path(repo_root).resolve(strict=True)
        if not worktree.is_dir() or not repository.is_dir():
            return frozenset()
        paths = iter(relative_paths)
    except (OSError, RuntimeError, TypeError, ValueError):
        return frozenset()

    preserved: set[str] = set()
    for relative in paths:
        if not _valid_relative_path(relative):
            continue
        try:
            if _preserve_one(worktree, repository, relative):
                preserved.add(relative)
        except (OSError, RuntimeError, TypeError, ValueError, OverflowError):
            continue
    return frozenset(preserved)


def _valid_relative_path(relative: object) -> bool:
    if not isinstance(relative, str) or not relative or "\x00" in relative or "\\" in relative:
        return False
    if relative.startswith("/"):
        return False
    components = relative.split("/")
    return all(component not in {"", ".", ".."} for component in components)


def _preserve_one(worktree: Path, repository: Path, relative: str) -> bool:
    components = relative.split("/")
    worktree_id = hashlib.sha256(os.fsencode(str(worktree))).hexdigest()
    path_id = hashlib.sha256(relative.encode("utf-8")).hexdigest()

    worktree_fd = _open_absolute_directory(worktree)
    try:
        source_parent_fd, source_fd = _open_relative_file(worktree_fd, components)
        try:
            source_before = os.fstat(source_fd)
            if not stat.S_ISREG(source_before.st_mode):
                return False

            repository_fd = _open_absolute_directory(repository)
            try:
                destination_fd = _open_archive_directory(repository_fd, worktree_id, path_id)
                try:
                    return _copy_publish_verify(
                        source_fd,
                        worktree_fd,
                        components,
                        source_before,
                        destination_fd,
                    )
                finally:
                    os.close(destination_fd)
            finally:
                os.close(repository_fd)
        finally:
            os.close(source_fd)
            os.close(source_parent_fd)
    finally:
        os.close(worktree_fd)


def _open_absolute_directory(path: Path) -> int:
    """Open every component without following symlinks."""
    if not path.is_absolute():
        raise ValueError("directory path must be absolute")
    descriptor = os.open("/", _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:]:
            next_descriptor = _open_directory_at(descriptor, component, create=False)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_directory_at(parent_fd: int, name: str, *, create: bool) -> int:
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
        os.mkdir(name, 0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)


def _open_relative_file(root_fd: int, components: list[str]) -> tuple[int, int]:
    parent_fd = os.dup(root_fd)
    try:
        for component in components[:-1]:
            next_fd = _open_directory_at(parent_fd, component, create=False)
            os.close(parent_fd)
            parent_fd = next_fd
        file_fd = os.open(components[-1], _READ_FLAGS, dir_fd=parent_fd)
        return parent_fd, file_fd
    except BaseException:
        os.close(parent_fd)
        raise


def _open_archive_directory(repository_fd: int, worktree_id: str, path_id: str) -> int:
    descriptor = os.dup(repository_fd)
    try:
        for component in (*_ARCHIVE_RELATIVE, worktree_id, path_id):
            next_descriptor = _open_directory_at(descriptor, component, create=True)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _copy_publish_verify(
    source_fd: int,
    worktree_fd: int,
    components: list[str],
    source_before: os.stat_result,
    destination_fd: int,
) -> bool:
    temp_name = f".tmp-{secrets.token_hex(16)}"
    temp_fd = os.open(temp_name, _TEMP_FLAGS, 0o600, dir_fd=destination_fd)
    temp_exists = True
    try:
        payload_digest, payload_size = _copy_source_to_temp(source_fd, temp_fd)
        source_after = os.fstat(source_fd)
        if _stat_identity(source_before) != _stat_identity(source_after):
            return False
        if not _source_path_still_matches(worktree_fd, components, source_before):
            return False

        os.fchmod(temp_fd, 0o444)
        os.fsync(temp_fd)
        try:
            os.link(
                temp_name,
                payload_digest,
                src_dir_fd=destination_fd,
                dst_dir_fd=destination_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            pass
        os.unlink(temp_name, dir_fd=destination_fd)
        temp_exists = False
        os.fsync(destination_fd)
        return _verify_published_payload(destination_fd, payload_digest, payload_digest, payload_size)
    finally:
        os.close(temp_fd)
        if temp_exists:
            try:
                os.unlink(temp_name, dir_fd=destination_fd)
            except FileNotFoundError:
                pass


def _copy_source_to_temp(source_fd: int, temp_fd: int) -> tuple[str, int]:
    os.lseek(source_fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    size = 0
    while block := os.read(source_fd, _COPY_CHUNK_SIZE):
        digest.update(block)
        size += len(block)
        view = memoryview(block)
        while view:
            written = os.write(temp_fd, view)
            if written <= 0:
                raise OSError("short write while copying audit payload")
            view = view[written:]
    return digest.hexdigest(), size


def _source_path_still_matches(worktree_fd: int, components: list[str], source_before: os.stat_result) -> bool:
    parent_fd, current_fd = _open_relative_file(worktree_fd, components)
    try:
        current = os.fstat(current_fd)
        return stat.S_ISREG(current.st_mode) and _stat_identity(source_before) == _stat_identity(current)
    finally:
        os.close(current_fd)
        os.close(parent_fd)


def _stat_identity(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _verify_published_payload(directory_fd: int, name: str, expected_digest: str, expected_size: int) -> bool:
    payload_fd = os.open(name, _READ_FLAGS, dir_fd=directory_fd)
    try:
        before = os.fstat(payload_fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size != expected_size:
            return False
        digest = hashlib.sha256()
        size = 0
        while block := os.read(payload_fd, _COPY_CHUNK_SIZE):
            digest.update(block)
            size += len(block)
        after = os.fstat(payload_fd)
        return (
            _stat_identity(before) == _stat_identity(after)
            and size == expected_size
            and digest.hexdigest() == expected_digest
        )
    finally:
        os.close(payload_fd)
