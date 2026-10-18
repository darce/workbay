"""Ownership policy for the consumer-facing review-pipeline surfaces.

The review-pipeline make fragment and the package installer both need to
answer the same question before using the consumer's review-gate scripts:
does the receipt account for the scripts, or are the existing files exact
copies of the current payload? Keeping that policy here prevents the two
delivery paths from drifting.

Contract: a present review surface is owned iff the receipt records its
path under any source, or the file is payload-identical (bytes, symlink
target, and executable/traversal bits). Otherwise the result is a conflict
naming the path. A probe that cannot inspect the receipt or payload is
unknown, never owned.
"""

from __future__ import annotations

import argparse
import json
import stat
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import Path

REVIEW_SURFACES = (
    "scripts/review_pipeline.py",
    "scripts/assert_gate_interpreter.sh",
)
RECEIPT_NAME = ".workbay-bootstrap.json"
_EXEC_BITS = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH


@dataclass(frozen=True, slots=True)
class Owned:
    """The review surfaces are authorized for use."""


@dataclass(frozen=True, slots=True)
class Conflict:
    """One or more present review surfaces are not authorized."""

    paths: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "paths", tuple(self.paths))


@dataclass(frozen=True, slots=True)
class Unknown:
    """The ownership state could not be established safely."""

    cause: str


Ownership = Owned | Conflict | Unknown


def _payload_identical(src: Path, dest: Path) -> bool:
    """True when ``dest`` is the current payload identity of ``src``.

    Fail closed: OSError, unsupported file type, type mismatch, extra/missing
    children, byte drift, symlink-target mismatch, or executable/traversal-bit
    mismatch is not identical. Supported types are regular files, directories,
    and symlinks; special nodes (FIFO, device, socket) are never read.
    Directory identity uses the same ``_EXEC_BITS`` mask as files (traversal),
    not full directory mode — umask write-bit drift is not a payload/reaper
    signal, while a traversal-bit mismatch is a local mode customization and
    stays local. Path/name similarity is not evidence. Uses ``lstat`` so a
    foreign symlink is never treated as the payload file it happens to point at.
    """
    try:
        return _payload_identical_unchecked(src, dest)
    except OSError:
        return False


def _payload_identical_unchecked(src: Path, dest: Path) -> bool:
    src_stat = src.lstat()
    dest_stat = dest.lstat()
    src_link = stat.S_ISLNK(src_stat.st_mode)
    dest_link = stat.S_ISLNK(dest_stat.st_mode)
    if src_link or dest_link:
        return src_link and dest_link and src.readlink() == dest.readlink()
    src_dir = stat.S_ISDIR(src_stat.st_mode)
    dest_dir = stat.S_ISDIR(dest_stat.st_mode)
    if src_dir or dest_dir:
        if not (src_dir and dest_dir):
            return False
        if (src_stat.st_mode & _EXEC_BITS) != (dest_stat.st_mode & _EXEC_BITS):
            return False
        try:
            src_names = sorted(child.name for child in src.iterdir())
            dest_names = sorted(child.name for child in dest.iterdir())
        except OSError:
            return False
        if src_names != dest_names:
            return False
        return all(_payload_identical_unchecked(src / name, dest / name) for name in src_names)
    if not (stat.S_ISREG(src_stat.st_mode) and stat.S_ISREG(dest_stat.st_mode)):
        return False
    if (src_stat.st_mode & _EXEC_BITS) != (dest_stat.st_mode & _EXEC_BITS):
        return False
    if src_stat.st_size != dest_stat.st_size:
        return False
    return src.read_bytes() == dest.read_bytes()


def _path_present(path: Path) -> bool:
    """Return whether a path should be treated as present, fail closed."""
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        # An inaccessible path must not be mistaken for an absent path. The
        # identity check will fail closed when it tries to inspect it.
        return True
    return True


def classify(
    root: Path,
    payload_root: Path,
    recorded_paths: Collection[str] | None,
) -> Ownership:
    """Classify ownership of the review-pipeline surfaces.

    ``recorded_paths=None`` represents a consumer with no receipt and is
    intentionally owned. Otherwise each review surface is owned when its path
    is recorded under any receipt source, absent, or byte/mode/symlink
    identical to the payload (including the executable bit). An existing
    unrecorded file that is not payload-identical is a conflict naming that
    path. Inability to inspect the payload is unknown, never owned.
    """
    if recorded_paths is None:
        return Owned()

    try:
        payload_is_dir = payload_root.is_dir()
    except OSError:
        payload_is_dir = False
    if not payload_is_dir:
        return Unknown("payload_missing")

    recorded = set(recorded_paths)
    conflicts: list[str] = []
    for rel in REVIEW_SURFACES:
        if rel in recorded:
            continue
        dest = root / rel
        if not _path_present(dest):
            continue
        payload = payload_root / rel
        if not _path_present(payload):
            return Unknown("payload_missing")
        if not _payload_identical(payload, dest):
            conflicts.append(rel)
    return Conflict(tuple(conflicts)) if conflicts else Owned()


def read_recorded_paths(root: Path) -> Collection[str] | None | Unknown:
    """Read receipt surface paths, returning ``None`` when no receipt exists."""
    receipt = root / RECEIPT_NAME
    if not _path_present(receipt):
        return None
    try:
        payload = json.loads(receipt.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return Unknown("receipt_unreadable")
    if not isinstance(payload, dict):
        return Unknown("receipt_unreadable")
    surfaces = payload.get("surfaces")
    if not isinstance(surfaces, list):
        return Unknown("receipt_unreadable")
    recorded: set[str] = set()
    for entry in surfaces:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            return Unknown("receipt_unreadable")
        recorded.add(entry["path"])
    return recorded


def classify_receipt(root: Path, payload_root: Path) -> Ownership:
    """Classify a consumer using its on-disk bootstrap receipt."""
    recorded_paths = read_recorded_paths(root)
    if isinstance(recorded_paths, Unknown):
        return recorded_paths
    return classify(root, payload_root, recorded_paths)


def _render(result: Ownership) -> str:
    if isinstance(result, Owned):
        return "owned"
    if isinstance(result, Conflict):
        return f"conflict:{' '.join(result.paths)}"
    return f"unknown:{result.cause}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args(argv)
    payload_root = Path(__file__).resolve().parent / "payload"
    print(_render(classify_receipt(args.root, payload_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
