"""Read and authenticate the published heuristics-canon/release@5 layout."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Literal, cast

CANON_MANIFEST_FILENAME = "meta/release-manifest.json"
CANON_MANIFEST_SCHEMA = "heuristics-canon/release@5"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_RULE_ID = re.compile(r"[A-Z][A-Z0-9]*-[0-9]+")
_RULE_ROW = re.compile(rb'^\|[ \t]*([A-Z][A-Z0-9]*-[0-9]+)(?:<a name="[a-z0-9-]+"></a>)?[ \t]*\|')
_TIER_CELL = re.compile(rb"([BSJ])\xc2\xb7[a-z]+")
_GROUPS = ("lexicons", "docs", "reasoning", "sources")
Tier = Literal["B", "S", "J"]


class CanonLoadError(ValueError):
    def __init__(self, code: str, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.path = path


@dataclass(frozen=True, slots=True)
class CanonRule:
    rule_id: str
    row_sha256: str
    tier: Tier


@dataclass(frozen=True, slots=True)
class CanonRelease:
    version: str
    manifest_sha256: str
    rules: Mapping[str, CanonRule]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_release_file(root: Path, relative: str, unreadable_code: str) -> bytes:
    path = PurePosixPath(relative)
    if (
        not relative
        or path.is_absolute()
        or any(part in ("", ".", "..") for part in relative.split("/"))
        or "\\" in relative
        or "\x00" in relative
    ):
        raise CanonLoadError("manifest_path_unsafe", "Expected a release-relative file path", path=relative)
    try:
        resolved = (root / relative).resolve(strict=True)
        if not resolved.is_relative_to(root):
            raise CanonLoadError("manifest_path_unsafe", "File resolves outside the release", path=relative)
        if not resolved.is_file():
            raise CanonLoadError(unreadable_code, "Expected a regular release file", path=relative)
        return resolved.read_bytes()
    except PermissionError as exc:
        raise CanonLoadError(
            "canon_root_permission_denied", "Permission denied reading the pinned Canon release", path=relative
        ) from exc
    except (OSError, RuntimeError) as exc:
        raise CanonLoadError(
            unreadable_code, f"Release file is unavailable: {type(exc).__name__}", path=relative
        ) from exc


def _rule_index(ids: object, count: object, relative: str) -> set[str]:
    if not isinstance(ids, list) or any(not isinstance(name, str) or not _RULE_ID.fullmatch(name) for name in ids):
        raise CanonLoadError("manifest_rule_index_invalid", "Expected an index of Canon rule IDs", path=relative)
    result = set(ids)
    if type(count) is not int or count != len(ids) or len(result) != len(ids):
        raise CanonLoadError("manifest_rule_index_invalid", "Rule count is wrong or an ID is duplicated", path=relative)
    return result


def _lexicon_rules(raw: bytes, relative: str) -> dict[str, CanonRule]:
    try:
        raw.decode("utf-8")
    except UnicodeError as exc:
        raise CanonLoadError("lexicon_invalid", "Lexicon is not UTF-8", path=relative) from exc
    rules = {}
    for line in raw.splitlines():
        match = _RULE_ROW.match(line)
        if not match:
            continue
        rule_id = match.group(1).decode("ascii")
        cells = line.rsplit(b"|", 3)
        tier = _TIER_CELL.fullmatch(cells[1].strip()) if len(cells) == 4 else None
        if tier is None or cells[3].strip():
            raise CanonLoadError("lexicon_invalid", f"Invalid tier cell for {rule_id}", path=relative)
        if rule_id in rules:
            raise CanonLoadError("manifest_rule_index_invalid", f"Duplicate rule {rule_id}", path=relative)
        rules[rule_id] = CanonRule(rule_id, _sha256(line), cast(Tier, tier.group(1).decode("ascii")))
    return rules


def _read_manifest(root: Path, expected_version: str, expected_sha256: str) -> tuple[dict, str]:
    raw = _read_release_file(root, CANON_MANIFEST_FILENAME, "manifest_missing")
    digest = _sha256(raw)
    if _SHA256.fullmatch(expected_sha256) is None or digest != expected_sha256:
        raise CanonLoadError("manifest_digest_mismatch", "Release manifest SHA-256 differs from the configured pin")
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise CanonLoadError("manifest_invalid", "Release manifest is not valid JSON") from exc
    if not isinstance(data, dict) or data.get("schema") != CANON_MANIFEST_SCHEMA:
        raise CanonLoadError("manifest_invalid", f"Expected a {CANON_MANIFEST_SCHEMA} manifest")
    if data.get("version") != expected_version:
        raise CanonLoadError("manifest_version_mismatch", f"Expected Canon version {expected_version}")
    return data, digest


def _verified_files(root: Path, manifest: dict) -> dict[str, bytes]:
    lexicons = {}
    seen = set()
    for group in _GROUPS:
        entries = manifest.get(group, {})
        if not isinstance(entries, dict):
            raise CanonLoadError("manifest_invalid", f"Invalid manifest file index {group}")
        for relative, record in entries.items():
            if relative in seen or not isinstance(record, dict):
                raise CanonLoadError("manifest_invalid", "Invalid or repeated file record", path=relative)
            seen.add(relative)
            expected = record.get("sha256")
            if not isinstance(expected, str) or _SHA256.fullmatch(expected) is None:
                raise CanonLoadError("manifest_invalid", "Expected a SHA-256 file digest", path=relative)
            data = _read_release_file(root, relative, "manifest_file_unreadable")
            if _sha256(data) != expected:
                raise CanonLoadError(
                    "manifest_file_digest_mismatch", "Canon file SHA-256 differs from manifest", path=relative
                )
            if group == "lexicons":
                lexicons[relative] = data
    return lexicons


def load_canon(root: Path, *, expected_version: str, expected_manifest_sha256: str) -> CanonRelease:
    """Verify every file before returning the rules indexed in the release."""
    try:
        root = root.resolve(strict=True)
    except PermissionError as exc:
        raise CanonLoadError(
            "canon_root_permission_denied", "Permission denied resolving the pinned Canon root", path=str(root)
        ) from exc
    except (OSError, RuntimeError) as exc:
        raise CanonLoadError("canon_root_unavailable", "The configured pinned Canon release is unavailable") from exc
    manifest, manifest_sha256 = _read_manifest(root, expected_version, expected_manifest_sha256)
    lexicons = _verified_files(root, manifest)
    expected_rules = _rule_index(manifest.get("rule_ids"), manifest.get("rules"), CANON_MANIFEST_FILENAME)
    rules: dict[str, CanonRule] = {}
    if not lexicons or not expected_rules:
        raise CanonLoadError("manifest_rule_index_invalid", "The verified release must index rules and lexicons")
    for relative, content in lexicons.items():
        entry = manifest["lexicons"][relative]
        expected_ids = _rule_index(entry.get("rule_ids"), entry.get("rules"), relative)
        parsed = _lexicon_rules(content, relative)
        if parsed.keys() != expected_ids or rules.keys() & parsed.keys():
            raise CanonLoadError(
                "manifest_rule_index_invalid", "Lexicon rows differ from the rule index", path=relative
            )
        rules.update(parsed)
    if rules.keys() != expected_rules:
        raise CanonLoadError("manifest_rule_index_invalid", "Parsed rows differ from the release rule index")
    return CanonRelease(expected_version, manifest_sha256, MappingProxyType(rules))
