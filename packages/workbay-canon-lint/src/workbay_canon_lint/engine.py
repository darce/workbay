"""Verify the canon and adapter bindings before scanning repository files."""

from __future__ import annotations

import subprocess
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Literal

from pydantic import ValidationError
from workbay_protocol.canon_lint import CanonAdapterConfig, CanonLintFinding, CanonLintReport
from workbay_protocol.cli_contract import ExitCode

from . import registry
from .adapters import markdown, python_ast
from .canon_loader import CanonRelease, load_canon
from .registry import CheckSpec

_ADAPTERS = {
    "markdown-plan": markdown.scan,
    "python-ast": python_ast.scan,
}
_GIT_TIMEOUT_SECONDS = 20


class CanonRunError(ValueError):
    def __init__(self, code: str, message: str, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.path = path


def _git(root: Path, *args: str) -> str:
    try:
        process = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            check=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
        return process.stdout.decode("utf-8")
    except (OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise CanonRunError("repository_unavailable", f"Git {' '.join(args[:2])} failed: {type(exc).__name__}") from exc


def workspace_root(directory: Path) -> Path:
    return Path(_git(directory, "rev-parse", "--show-toplevel").rstrip("\n")).resolve()


def _verified_checks(canon: CanonRelease) -> dict[str, CheckSpec]:
    checks = {}
    for check in registry.CHECKS:
        row = canon.rules.get(check.rule_id)
        if canon.version != check.canon_version or row is None or row.row_sha256 != check.row_sha256:
            raise CanonRunError(
                "check_stale_against_canon",
                f"Check {check.check_id} must be re-verified against {check.rule_id} in Canon {canon.version}",
            )
        checks[check.check_id] = check
    return checks


def _check_route(adapter_id: str, check_ids: Sequence[str], checks: dict[str, CheckSpec]) -> None:
    if adapter_id not in _ADAPTERS:
        raise CanonRunError("unknown_adapter", f"Adapter {adapter_id!r} is not registered")
    if not check_ids:
        raise CanonRunError("invalid_route", f"Route {adapter_id!r} requires at least one check")
    for check_id in check_ids:
        check = checks.get(check_id)
        if check is None or check.adapter_id != adapter_id:
            raise CanonRunError("invalid_route_check", f"Check {check_id!r} is unavailable for adapter {adapter_id!r}")


def _exemptions(
    config: CanonAdapterConfig,
    canon: CanonRelease,
    checks: dict[str, CheckSpec],
) -> set[tuple[str, str]]:
    today = datetime.now(UTC).date()
    seen: set[tuple[str, str]] = set()
    active: set[tuple[str, str]] = set()
    for exemption in config.exemptions:
        check = checks.get(exemption.check_id)
        if check is None or check.rule_id != exemption.rule_id or canon.rules[check.rule_id].tier != "S":
            raise CanonRunError(
                "invalid_exemption",
                f"Exemption {exemption.check_id!r} requires a matching tier S rule",
                exemption.path,
            )
        key = (exemption.check_id, exemption.path)
        if key in seen:
            raise CanonRunError("duplicate_exemption", f"Duplicate exemption for {exemption.check_id}", exemption.path)
        seen.add(key)
        if exemption.expires_on >= today:
            active.add(key)
    return active


def _git_files(root: Path, patterns: Sequence[str], mode: str) -> set[str]:
    # Use Git's recursive glob semantics and exclude ignored untracked files.
    pathspecs = [f":(top,glob){pattern}" for pattern in patterns]
    if mode == "tracked":
        entries = _git(root, "ls-files", "--cached", "-z", "--", *pathspecs)
    else:
        diff_filter = "ACMRT" if mode == "changed" else "ACMR"
        diff = ("diff", "--name-only", f"--diff-filter={diff_filter}", "-z")
        entries = _git(root, *diff, "--cached", "--", *pathspecs)
        if mode == "changed":
            entries += _git(root, *diff, "--", *pathspecs)
            entries += _git(root, "ls-files", "--others", "--exclude-standard", "-z", "--", *pathspecs)
    return set(entries.split("\0")) - {""}


def _explicit_paths(files: Sequence[str]) -> set[str]:
    paths = set()
    for name in files:
        path = PurePosixPath(name)
        if (
            not path.parts or path.is_absolute() or ".." in path.parts
            or PureWindowsPath(name).drive or "\\" in name or "\0" in name
        ):
            raise CanonRunError("invalid_source_path", "Expected a workspace-relative file path", name)
        paths.add(path.as_posix())
    return paths


def _matches_glob(path: tuple[str, ...], pattern: tuple[str, ...]) -> bool:
    if not pattern:
        return not path
    if pattern[0] == "**":
        return any(_matches_glob(path[index:], pattern[1:]) for index in range(len(path) + 1))
    return bool(path) and fnmatchcase(path[0], pattern[0]) and _matches_glob(path[1:], pattern[1:])


def _index_blobs(root: Path, files: Sequence[str]) -> dict[str, str]:
    entries = _git(root, "ls-files", "--stage", "-z", "--", *(f":(literal){name}" for name in files))
    result = {}
    for entry in filter(None, entries.split("\0")):
        metadata, path = entry.split("\t", 1)
        mode, blob, stage = metadata.split()
        if mode in {"100644", "100755"} and stage == "0":
            result[path] = blob
    return result


def _read_blob(root: Path, blob: str, relative: str) -> str:
    try:
        process = subprocess.run(
            ["git", "-C", str(root), "cat-file", "blob", blob],
            capture_output=True,
            check=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
        return process.stdout.decode("utf-8")
    except (OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise CanonRunError("source_unreadable", f"Cannot read source: {type(exc).__name__}", relative) from exc


def _read_source(root: Path, relative: str) -> str | None:
    try:
        path = (root / relative).resolve(strict=True)
        if not path.is_relative_to(root):
            raise CanonRunError("path_outside_workspace", "Source resolves outside the workspace", relative)
        if path.is_file():
            return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        # A tracked file can be removed in the working tree before staging.
        return None
    except (OSError, UnicodeError, RuntimeError) as exc:
        raise CanonRunError("source_unreadable", f"Cannot read source: {type(exc).__name__}", relative) from exc
    return None


def _scan_paths(
    root: Path,
    config: CanonAdapterConfig,
    canon: CanonRelease,
    checks: dict[str, CheckSpec],
    exemptions: set[tuple[str, str]],
    *,
    mode: Literal["tracked", "staged", "changed"],
    files: Sequence[str] | None,
) -> tuple[list[CanonLintFinding], int, int]:
    findings: list[CanonLintFinding] = []
    files_checked = 0
    exempted_count = 0
    routes: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    explicit = _explicit_paths(files) if files is not None else None
    for route in config.routes:
        if explicit is None:
            candidates = _git_files(root, route.paths, mode)
        else:
            patterns = [PurePosixPath(pattern).parts for pattern in route.paths]
            candidates = {
                relative for relative in explicit
                if any(_matches_glob(PurePosixPath(relative).parts, pattern) for pattern in patterns)
            }
        for relative in candidates:
            routes[relative][route.adapter_id].update(route.checks)

    blobs = _index_blobs(root, sorted(routes)) if mode == "staged" and routes else {}
    for relative, adapters in sorted(routes.items()):
        if mode == "staged":
            if relative not in blobs:
                continue
            source = _read_blob(root, blobs[relative], relative)
        else:
            source = _read_source(root, relative)
        if source is None:
            continue
        files_checked += 1
        for adapter_id, check_ids in sorted(adapters.items()):
            try:
                detected = _ADAPTERS[adapter_id](source, frozenset(check_ids))
            except (SyntaxError, ValueError, RecursionError) as exc:
                raise CanonRunError("adapter_parse_failed", str(exc), relative) from exc
            for check_id, line, evidence in detected:
                check = checks[check_id]
                row = canon.rules[check.rule_id]
                if (check_id, relative) in exemptions:
                    exempted_count += 1
                    continue
                try:
                    finding = CanonLintFinding(
                        rule_id=row.rule_id,
                        canon_version=canon.version,
                        manifest_sha256=canon.manifest_sha256,
                        rule_row_sha256=row.row_sha256,
                        check_id=check_id,
                        check_version=check.check_version,
                        adapter_id=adapter_id,
                        path=relative,
                        line=line,
                        evidence_snippet=evidence,
                        tier=row.tier,
                    )
                except ValidationError as exc:
                    raise CanonRunError(
                        "finding_invalid", f"Cannot report finding for {relative!r} from {check_id}", relative
                    ) from exc
                findings.append(finding)

    findings.sort(key=lambda finding: (finding.path, finding.line, finding.check_id))
    return findings, files_checked, exempted_count


def run(
    command: Literal["lint", "check-drift"],
    root: Path,
    canon_root: Path,
    config: CanonAdapterConfig,
    *,
    mode: Literal["tracked", "staged", "changed"] = "tracked",
    files: Sequence[str] | None = None,
) -> tuple[ExitCode, CanonLintReport]:
    canon = load_canon(
        canon_root,
        expected_version=config.canon_version,
        expected_manifest_sha256=config.manifest_sha256,
    )
    checks = _verified_checks(canon)
    if not config.routes:
        raise CanonRunError("no_enabled_routes", "Configure at least one canon adapter route")
    for route in config.routes:
        _check_route(route.adapter_id, route.checks, checks)
    exemptions = _exemptions(config, canon, checks)
    findings, files_checked, exempted_count = ([], 0, 0)
    if command == "lint":
        findings, files_checked, exempted_count = _scan_paths(
            root, config, canon, checks, exemptions, mode=mode, files=files,
        )
    report = CanonLintReport(
        command=command,
        ok=True,
        canon_version=canon.version,
        manifest_sha256=canon.manifest_sha256,
        checks_checked=len(checks),
        files_checked=files_checked,
        exempted_count=exempted_count,
        findings=findings,
    )
    tiers = {finding.tier for finding in findings}
    if "B" in tiers:
        return ExitCode.REFUSED, report
    if "S" in tiers:
        return ExitCode.DEGRADED, report
    return ExitCode.OK, report
