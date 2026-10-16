#!/usr/bin/env python3
"""Derive tracked root overlay twins from the payload documentation canon."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import check_overlay_drift as drift


class SyncRefused(RuntimeError):
    """Raised when synchronization cannot safely determine every output."""


class SyncWriteError(OSError):
    """Raised when staging or replacing root twins fails after a mutation attempt."""

    def __init__(self, message: str, *, replaced: tuple[str, ...]):
        super().__init__(message)
        self.replaced = replaced


def enumerate_sync_twins(
    *, tracked: set[str] | None = None
) -> drift.TwinEnumeration:
    """Use the drift gate's authoritative tracked-twin partition."""
    return drift.enumerate_twins(tracked)


def _stage_bytes(dest: Path, data: bytes) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(
        prefix=f".{dest.name}.",
        suffix=".tmp",
        dir=str(dest.parent),
    )
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    return tmp


def _cleanup_temps(staged: list[tuple[str, Path, Path]]) -> None:
    for _twin, tmp, _dest in staged:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _replace_staged(staged: list[tuple[str, Path, Path]]) -> None:
    replaced: list[str] = []
    try:
        for twin, tmp, dest in staged:
            os.replace(tmp, dest)
            replaced.append(twin)
    except OSError as exc:
        raise SyncWriteError(str(exc), replaced=tuple(replaced)) from exc


def sync_overlay_canon(
    repo_root: Path | None = None,
    *,
    tracked: set[str] | None = None,
    dry_run: bool = False,
) -> tuple[str, ...]:
    """Copy changed payload canon bytes to their tracked root twins.

    Inputs are validated before any live root path is replaced. Writes are
    staged next to each destination first; a staging failure leaves the live
    tree unchanged. The subsequent replace pass is sequential: a mid-replace
    failure can leave a prefix of destinations updated and raises
    ``SyncWriteError`` naming those twins. It is not a derivation refusal.
    """
    if repo_root is None:
        repo_root = drift.REPO_ROOT

    enumeration = enumerate_sync_twins(tracked=tracked)
    if enumeration.orphans:
        paths = ", ".join(enumeration.orphans)
        raise SyncRefused(f"tracked root path(s) have no payload counterpart: {paths}")
    missing_required = drift.REQUIRED_TWINS - set(enumeration.twins)
    if missing_required:
        paths = ", ".join(sorted(missing_required))
        raise SyncRefused(f"required twin(s) could not be enumerated: {paths}")
    if drift.MIN_COMPARED_FILES < 1:
        raise SyncRefused("MIN_COMPARED_FILES must be at least 1")
    if len(enumeration.twins) < drift.MIN_COMPARED_FILES:
        raise SyncRefused(
            f"enumerated {len(enumeration.twins)} twin(s), below required minimum "
            f"of {drift.MIN_COMPARED_FILES}"
        )

    # DATA-14 / REF-09 / REF-26: payload is the system of record. Refuse
    # symlink / same-file identities before reading or writing so a link
    # cannot compare or copy the payload onto itself.
    for twin in enumeration.twins:
        violation = drift.independent_twin_violation(repo_root, twin)
        if violation:
            raise SyncRefused(violation)

    sources: dict[str, bytes] = {}
    for twin in enumeration.twins:
        payload_path = repo_root / drift.PAYLOAD_DOCS_PREFIX / twin
        if payload_path.is_symlink() or not payload_path.is_file():
            raise SyncRefused(f"payload source is missing: {twin}")
        sources[twin] = payload_path.read_bytes()

    changed = tuple(
        twin
        for twin, payload_bytes in sources.items()
        if not (repo_root / drift.ROOT_DOCS_PREFIX / twin).is_file()
        or (repo_root / drift.ROOT_DOCS_PREFIX / twin).is_symlink()
        or (repo_root / drift.ROOT_DOCS_PREFIX / twin).read_bytes() != payload_bytes
    )
    if dry_run:
        for twin in changed:
            print(f"would sync: {twin}")
        return changed
    if not changed:
        return changed

    staged: list[tuple[str, Path, Path]] = []
    try:
        for twin in changed:
            dest = repo_root / drift.ROOT_DOCS_PREFIX / twin
            staged.append((twin, _stage_bytes(dest, sources[twin]), dest))
        _replace_staged(staged)
    except OSError as exc:
        _cleanup_temps(staged)
        if isinstance(exc, SyncWriteError):
            raise
        raise SyncWriteError(str(exc), replaced=()) from exc

    return changed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--dry-run", action="store_true", help="report changes without writing"
    )
    modes.add_argument(
        "--check",
        action="store_true",
        help="report changes without writing and fail when changes are needed",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        changed = sync_overlay_canon(dry_run=args.dry_run or args.check)
    except SyncRefused as exc:
        # RLSE-02 / RLSE-11: the scripted procedure refuses an unknown or
        # incomplete derivation; the independent drift gate remains strict.
        print(f"overlay canon sync refused: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(
            "overlay canon sync cannot enumerate tracked files: "
            f"{drift.format_subprocess_error(exc)}",
            file=sys.stderr,
        )
        return 1
    except SyncWriteError as exc:
        already = ", ".join(exc.replaced) if exc.replaced else "no files"
        print(
            f"overlay canon sync failed after replacing {already}: {exc}",
            file=sys.stderr,
        )
        return 1
    except (OSError, UnicodeError) as exc:
        print(f"overlay canon sync failed: {exc}", file=sys.stderr)
        return 1

    if not changed:
        print("ok: overlay canon is already synchronized")
    elif not (args.dry_run or args.check):
        for twin in changed:
            print(f"synced: {twin}")
    return 1 if args.check and changed else 0


if __name__ == "__main__":
    raise SystemExit(main())
