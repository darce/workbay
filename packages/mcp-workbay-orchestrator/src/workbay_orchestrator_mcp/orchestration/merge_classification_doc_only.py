"""Fail-closed classification of a branch's complete merge-base diff."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from typing import Final, Sequence


@dataclass(frozen=True, slots=True)
class DocumentationPathRule:
    """One inspectable allow rule for repository-relative documentation paths."""

    pattern: str
    description: str


# Policy allowlist kept as data in one place. RLSE-02/RLSE-11/RLSE-12 require
# every path in the measured deployable-unit diff to be proven documentation.
# Directory membership is not sufficient; `_DOCUMENTATION_SUFFIXES` must also match.
DOCUMENTATION_PATH_RULES: Final[tuple[DocumentationPathRule, ...]] = (
    DocumentationPathRule("docs/**", "candidates below the repository documentation tree"),
    DocumentationPathRule("*.md", "Markdown documentation at repository root"),
    DocumentationPathRule("**/*.md", "Markdown documentation in nested package trees"),
)

_DOCUMENTATION_SUFFIXES: Final[frozenset[str]] = frozenset({".md", ".rst", ".txt", ".svg"})
_GIT_OVERRIDE_KEYS: Final[tuple[str, ...]] = (
    "GIT_DIR",
    "GIT_COMMON_DIR",
    "GIT_WORK_TREE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_INDEX_FILE",
    "GIT_NAMESPACE",
    "GIT_CEILING_DIRECTORIES",
)


@dataclass(frozen=True, slots=True)
class DocOnlyClassification:
    """The classification and evidence suitable for a gate or human report."""

    doc_only: bool
    reason: str
    non_documentation_paths: tuple[str, ...] = ()
    merge_base: str | None = None
    changed_paths: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _GitResult:
    returncode: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True, slots=True)
class _DiffEntry:
    old_mode: str
    new_mode: str
    old_object: str
    new_object: str
    status: str
    path: str


def _git_environment() -> dict[str, str]:
    """Copy the process environment with ambient git overrides removed.

    ``git -C`` does not bind measurement when ``GIT_DIR`` (or related keys)
    point at another repository. Classification must resolve against the
    classified worktree, not the caller's outer git invocation.
    """
    environment = os.environ.copy()
    for key in _GIT_OVERRIDE_KEYS:
        environment.pop(key, None)
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    environment["LC_ALL"] = "C"
    return environment


def _git(repository: Path, *args: str) -> _GitResult:
    environment = _git_environment()
    try:
        repo = Path(repository).resolve()
    except OSError as exc:
        return _GitResult(128, b"", str(exc).encode("utf-8", errors="replace"))
    try:
        completed = subprocess.run(
            ["git", "-C", os.fspath(repo), *args],
            check=False,
            capture_output=True,
            timeout=30,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return _GitResult(128, b"", str(exc).encode("utf-8", errors="replace"))
    return _GitResult(completed.returncode, completed.stdout, completed.stderr)


def _diff(repository: Path, merge_base: str, branch_commit: str, *measurement: str) -> _GitResult:
    """Measure a complete merge-base diff with submodule ignoring disabled."""
    return _git(
        repository,
        "-c",
        "diff.ignoreSubmodules=none",
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--ignore-submodules=none",
        *measurement,
        merge_base,
        branch_commit,
        "--",
    )


def _display_error(stderr: bytes) -> str:
    text = stderr.decode("utf-8", errors="replace").strip()
    return text or "git returned no diagnostic"


def _resolve_commit(repository: Path, ref: str) -> tuple[str | None, str | None]:
    if not ref or "\x00" in ref:
        return None, f"cannot resolve invalid ref {ref!r}"
    result = _git(repository, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}")
    if result.returncode != 0:
        return None, f"cannot resolve ref {ref!r}: {_display_error(result.stderr)}"
    try:
        commit = result.stdout.decode("ascii").strip()
    except UnicodeDecodeError:
        return None, f"cannot resolve ref {ref!r}: git returned a non-ASCII object id"
    if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", commit) is None:
        return None, f"cannot resolve ref {ref!r}: git returned an ambiguous object id"
    return commit, None


def _parse_path(raw_path: bytes) -> tuple[str | None, str | None]:
    try:
        path = raw_path.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None, "git diff returned a path that is not valid UTF-8"
    parts = PurePosixPath(path).parts
    if (
        not path
        or path == "."
        or path.startswith("/")
        or path != PurePosixPath(path).as_posix()
        or any(part in {".", ".."} for part in parts)
        or any(ord(character) < 32 or ord(character) == 127 for character in path)
    ):
        return None, f"git diff returned an ambiguous path: {path!r}"
    return path, None


def _parse_raw_diff(raw_diff: bytes) -> tuple[tuple[_DiffEntry, ...] | None, str | None]:
    """Parse ``git diff --raw -z`` without discarding type or mode evidence."""
    if not raw_diff:
        return (), None
    if not raw_diff.endswith(b"\x00"):
        return None, "git diff returned an unterminated raw record"
    fields = raw_diff[:-1].split(b"\x00")
    if len(fields) % 2:
        return None, "git diff returned an incomplete raw record"

    entries: list[_DiffEntry] = []
    seen_paths: set[str] = set()
    metadata_pattern = re.compile(
        rb":([0-7]{6}) ([0-7]{6}) ([0-9a-f]+) ([0-9a-f]+) ([A-Z])"
    )
    for metadata, raw_path in zip(fields[0::2], fields[1::2], strict=True):
        match = metadata_pattern.fullmatch(metadata)
        if match is None:
            return None, "git diff returned malformed typed metadata"
        path, error = _parse_path(raw_path)
        if error is not None:
            return None, error
        assert path is not None
        if path in seen_paths:
            return None, f"git diff returned duplicate typed evidence for {path!r}"
        seen_paths.add(path)
        old_object = match.group(3).decode("ascii")
        new_object = match.group(4).decode("ascii")
        if any(re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", object_id) is None for object_id in (old_object, new_object)):
            return None, "git diff returned an ambiguous object id"
        entries.append(
            _DiffEntry(
                old_mode=match.group(1).decode("ascii"),
                new_mode=match.group(2).decode("ascii"),
                old_object=old_object,
                new_object=new_object,
                status=match.group(5).decode("ascii"),
                path=path,
            )
        )
    return tuple(entries), None


def _parse_numstat(raw_numstat: bytes) -> tuple[dict[str, bool] | None, str | None]:
    """Return path -> binary from the same no-rename commit diff."""
    if not raw_numstat:
        return {}, None
    if not raw_numstat.endswith(b"\x00"):
        return None, "git diff returned an unterminated numstat record"
    result: dict[str, bool] = {}
    for record in raw_numstat[:-1].split(b"\x00"):
        parts = record.split(b"\t", 2)
        if len(parts) != 3:
            return None, "git diff returned malformed numstat evidence"
        added, deleted, raw_path = parts
        if (added, deleted) != (b"-", b"-") and not (added.isdigit() and deleted.isdigit()):
            return None, "git diff returned malformed numstat counts"
        path, error = _parse_path(raw_path)
        if error is not None:
            return None, error
        assert path is not None
        if path in result:
            return None, f"git diff returned duplicate evidence for {path!r}"
        result[path] = added == b"-"
    return result, None


def _is_documentation_path(path: str) -> bool:
    return any(fnmatchcase(path, rule.pattern) for rule in DOCUMENTATION_PATH_RULES)


_MACHINE_CONSUMED_SUFFIXES: Final[frozenset[str]] = frozenset({".json", ".toml", ".yaml", ".yml"})
_REGULAR_NONEXECUTABLE_BLOB_MODE: Final[str] = "100644"
_ABSENT_MODE: Final[str] = "000000"


def _entry_rejection(entry: _DiffEntry, *, binary: bool) -> str | None:
    """Explain why typed evidence does not prove a documentation-only entry."""
    if entry.status not in {"A", "D", "M"}:
        return f"unsupported change status {entry.status}"
    expected_presence = {
        "A": (False, True),
        "D": (True, False),
        "M": (True, True),
    }[entry.status]
    actual_presence = (entry.old_mode != _ABSENT_MODE, entry.new_mode != _ABSENT_MODE)
    object_presence = (bool(set(entry.old_object) - {"0"}), bool(set(entry.new_object) - {"0"}))
    if actual_presence != expected_presence or object_presence != expected_presence:
        return "mode, object, and status evidence disagree"
    if entry.old_mode != _ABSENT_MODE and entry.new_mode != _ABSENT_MODE and entry.old_mode != entry.new_mode:
        return f"mode changed from {entry.old_mode} to {entry.new_mode}"
    present_modes = {entry.old_mode, entry.new_mode} - {_ABSENT_MODE}
    if present_modes - {_REGULAR_NONEXECUTABLE_BLOB_MODE}:
        return "entry is not a regular non-executable blob"
    if binary:
        return "content is binary"
    suffix = PurePosixPath(entry.path).suffix.lower()
    if suffix in _MACHINE_CONSUMED_SUFFIXES:
        return "extension is machine-consumed"
    if suffix not in _DOCUMENTATION_SUFFIXES:
        return "extension is not a documentation suffix"
    if not _is_documentation_path(entry.path):
        return "path does not match the documentation allowlist"
    return None


def classify_doc_only(
    repository: str | os.PathLike[str],
    base_ref: str,
    branch_ref: str,
) -> DocOnlyClassification:
    """Classify the complete ``merge-base(base_ref, branch_ref)..branch_ref`` diff.

    Measurement failures are returned as negative classifications. OBS-04 and
    OBS-08 make missing or malformed evidence a loud refusal, never permission.
    """

    repo = Path(repository)
    if not repo.is_dir():
        return DocOnlyClassification(False, f"repository is not a readable directory: {repo}")
    try:
        repo = repo.resolve()
    except OSError:
        return DocOnlyClassification(False, f"repository is not a readable directory: {repo}")

    base_commit, error = _resolve_commit(repo, base_ref)
    if error is not None:
        return DocOnlyClassification(False, error)
    branch_commit, error = _resolve_commit(repo, branch_ref)
    if error is not None:
        return DocOnlyClassification(False, error)
    assert base_commit is not None and branch_commit is not None

    merge_base_result = _git(repo, "merge-base", "--all", base_commit, branch_commit)
    if merge_base_result.returncode != 0:
        return DocOnlyClassification(
            False,
            f"cannot determine merge base: {_display_error(merge_base_result.stderr)}",
        )
    try:
        merge_bases = merge_base_result.stdout.decode("ascii").splitlines()
    except UnicodeDecodeError:
        return DocOnlyClassification(False, "cannot determine merge base: non-ASCII object id")
    if len(merge_bases) != 1:
        return DocOnlyClassification(
            False,
            f"cannot determine a unique merge base: git returned {len(merge_bases)} candidates",
        )
    merge_base = merge_bases[0]
    if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", merge_base) is None:
        return DocOnlyClassification(False, "cannot determine merge base: ambiguous object id")

    diff_result = _diff(repo, merge_base, branch_commit, "--raw", "--abbrev=64", "-z")
    if diff_result.returncode != 0:
        return DocOnlyClassification(
            False,
            f"cannot measure merge-base diff: {_display_error(diff_result.stderr)}",
            merge_base=merge_base,
        )
    entries, error = _parse_raw_diff(diff_result.stdout)
    if error is not None:
        return DocOnlyClassification(False, error, merge_base=merge_base)
    assert entries is not None
    if not entries:
        return DocOnlyClassification(
            False,
            "merge-base diff is empty; documentation-only requires positive evidence",
            merge_base=merge_base,
        )

    numstat_result = _diff(repo, merge_base, branch_commit, "--numstat", "-z")
    if numstat_result.returncode != 0:
        return DocOnlyClassification(
            False,
            f"cannot measure binary diff evidence: {_display_error(numstat_result.stderr)}",
            merge_base=merge_base,
        )
    binary_by_path, error = _parse_numstat(numstat_result.stdout)
    if error is not None:
        return DocOnlyClassification(False, error, merge_base=merge_base)
    assert binary_by_path is not None
    changed_paths = tuple(sorted(entry.path for entry in entries))
    if set(binary_by_path) != set(changed_paths):
        return DocOnlyClassification(
            False,
            "typed diff and binary evidence disagree on changed paths",
            merge_base=merge_base,
            changed_paths=changed_paths,
        )

    rejected = {
        entry.path: reason
        for entry in entries
        if (reason := _entry_rejection(entry, binary=binary_by_path[entry.path])) is not None
    }
    non_documentation_paths = tuple(sorted(rejected))
    if non_documentation_paths:
        listed = ", ".join(f"{path!r} ({rejected[path]})" for path in non_documentation_paths)
        return DocOnlyClassification(
            False,
            f"merge-base diff contains entries not proven to be documentation: {listed}",
            non_documentation_paths=non_documentation_paths,
            merge_base=merge_base,
            changed_paths=changed_paths,
        )
    return DocOnlyClassification(
        True,
        f"documentation-only: all {len(changed_paths)} changed path(s) are typed text documentation blobs",
        merge_base=merge_base,
        changed_paths=changed_paths,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Classify a branch's complete merge-base diff as documentation-only.")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--base", required=True, help="integration/base ref")
    parser.add_argument("--branch", required=True, help="branch ref to classify")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = classify_doc_only(args.repository, args.base, args.branch)
    json.dump(asdict(result), sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if result.doc_only else 1


if __name__ == "__main__":
    raise SystemExit(main())
