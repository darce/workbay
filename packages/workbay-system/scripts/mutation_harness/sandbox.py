"""Isolated per-mutant tree copy and cleanup (GRPH-39 frozen topology)."""

from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class SandboxError(RuntimeError):
    """Sandbox create/cleanup failure."""


class SandboxEscapeError(SandboxError):
    """Mutant target would write outside the sandbox (path/symlink escape)."""


def validate_mutant_target(target: str, *, mutant_id: str | None = None) -> None:
    """Reject absolute and ``..``-bearing targets early (named, before join).

    Manifest targets must be relative paths inside the source tree. Absolute
    paths and parent-directory segments are refused here so failure is early
    and named, not discovered only after a host file is overwritten.
    """
    who = f"mutant {mutant_id!r}: " if mutant_id else ""
    if not isinstance(target, str) or not target.strip():
        raise SandboxEscapeError(f"{who}target must be a non-empty relative path")
    # Reject absolute POSIX/Windows forms before any join (Path / discards base).
    if Path(target).is_absolute() or target.startswith(("/", "\\")) or (
        len(target) >= 2 and target[1] == ":"
    ):
        raise SandboxEscapeError(
            f"{who}absolute target rejected (sandbox escape): {target!r}"
        )
    parts = Path(target).parts
    if ".." in parts:
        raise SandboxEscapeError(
            f"{who}target contains '..' (sandbox escape): {target!r}"
        )
    # Empty parts / pure dots only.
    if target in (".",) or not parts:
        raise SandboxEscapeError(f"{who}target is not a file path: {target!r}")


def _is_within_root(candidate: str, root: str) -> bool:
    """True when ``candidate`` is ``root`` or a path under it (after realpath)."""
    if candidate == root:
        return True
    prefix = root if root.endswith(os.sep) else root + os.sep
    return candidate.startswith(prefix)


def resolve_sandbox_target(
    sandbox: Path | str,
    target: str,
    *,
    mutant_id: str | None = None,
) -> Path:
    """Join ``target`` under ``sandbox`` and refuse any escape.

    After joining, both paths are realpath'd (symlink traversal included).
    Raises :class:`SandboxEscapeError` unless the resolved target is really
    inside the sandbox root.
    """
    validate_mutant_target(target, mutant_id=mutant_id)
    sandbox_root = Path(sandbox)
    # Join only after absolute/.. rejection so Path.__truediv__ cannot discard base.
    joined = sandbox_root / target
    sandbox_real = os.path.realpath(str(sandbox_root))
    target_real = os.path.realpath(str(joined))
    if not _is_within_root(target_real, sandbox_real):
        who = f"mutant {mutant_id!r}: " if mutant_id else ""
        raise SandboxEscapeError(
            f"{who}resolved target escapes sandbox: target={target!r} "
            f"resolved={target_real!r} sandbox={sandbox_real!r}"
        )
    return Path(target_real)


def create_sandbox(
    source_root: Path,
    *,
    mutant_id: str,
    prefix: str = "mutharness-",
    ignore_names: frozenset[str] | None = None,
) -> Path:
    """Copy ``source_root`` into a fresh temporary directory.

    Workers must never share a writable tree. Each mutant gets its own copy.
    Symlinks are *not* preserved: a symlinked path inside the copied tree would
    otherwise remain an escape hatch to host files outside the sandbox.
    """
    source_root = Path(source_root).resolve()
    if not source_root.is_dir():
        raise SandboxError(f"source root is not a directory: {source_root}")

    skip = ignore_names or frozenset(
        {
            ".git",
            ".hg",
            ".svn",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            "node_modules",
            ".venv",
            "venv",
            ".tox",
        }
    )

    def _ignore(directory: str, names: list[str]) -> set[str]:
        return {n for n in names if n in skip}

    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in mutant_id)[:40]
    dest = Path(tempfile.mkdtemp(prefix=f"{prefix}{safe_id}-"))
    try:
        # copytree into empty dest: copy contents.
        # symlinks=False: copy referent content, never re-create host-escaping links.
        for item in source_root.iterdir():
            src = item
            dst = dest / item.name
            if item.is_dir():
                shutil.copytree(src, dst, ignore=_ignore, symlinks=False)
            elif item.is_symlink():
                # File-level symlink at root: materialize content, do not relink.
                if item.is_dir():
                    shutil.copytree(src, dst, ignore=_ignore, symlinks=False)
                else:
                    shutil.copy2(src, dst, follow_symlinks=True)
            else:
                shutil.copy2(src, dst)
    except OSError as exc:
        shutil.rmtree(dest, ignore_errors=True)
        raise SandboxError(f"failed to create sandbox for {mutant_id}: {exc}") from exc
    return dest


def destroy_sandbox(sandbox_root: Path) -> None:
    """Remove a sandbox tree; best-effort, never raises for missing path."""
    root = Path(sandbox_root)
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)


@contextmanager
def sandbox_session(
    source_root: Path,
    *,
    mutant_id: str,
    prefix: str = "mutharness-",
) -> Iterator[Path]:
    """Context manager: create sandbox, yield path, always clean up."""
    path = create_sandbox(source_root, mutant_id=mutant_id, prefix=prefix)
    try:
        yield path
    finally:
        destroy_sandbox(path)
