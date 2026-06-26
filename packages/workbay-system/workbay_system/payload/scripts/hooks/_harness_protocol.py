#!/usr/bin/env python3
"""Load the branch-isolation policy from harness-protocol.yaml.

This helper intentionally avoids third-party YAML dependencies because the
branch-isolation hooks run under plain ``python3`` in editor harnesses.
It parses only the small YAML subset used by the ``branch_isolation`` block.
"""

from __future__ import annotations

import ast
import fnmatch
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import IO


CONTRACT_RELATIVE_PATH = Path("docs/workbay/contracts/harness-protocol.yaml")


class HarnessContractMissingError(RuntimeError):
    """Raised when the branch-isolation contract cannot be loaded safely."""


class HarnessContractMissingPolicy(Enum):
    """internal: how a hook should react when the branch-isolation
    contract YAML is absent.

    The contract lives in the bootstrap overlay
    (``docs/workbay/contracts/harness-protocol.yaml``). A fresh
    ``--profile minimal`` consumer install legitimately ships without
    it, so end-user PreToolUse hooks must not hard-block in that case.

    - ``BLOCK``: emit the structured stderr error and exit ``2``. Used
      by the internal verification suite (``check_main_clean.py --mode
      block`` invoked from CI / pre-push gates) where a missing
      contract is a hard regression.
    - ``WARN``: emit the structured stderr message and exit ``0``. The
      default for end-user PreToolUse hooks (``check_main_clean.py``
      default mode, ``guard-bash-main-branch.py``).
    - ``SILENT``: swallow the error entirely. Used by background
      drift-detection helpers that already have a non-blocking
      fallback path.
    """

    BLOCK = "block"
    WARN = "warn"
    SILENT = "silent"


def handle_missing_contract(
    error: HarnessContractMissingError,
    *,
    policy: HarnessContractMissingPolicy,
    stream: IO[str] | None = None,
) -> int:
    """Apply a ``HarnessContractMissingPolicy`` to a missing-contract
    failure and return the exit code the hook should propagate.

    ``BLOCK`` returns ``2`` (hard fail). ``WARN`` returns ``0`` and
    emits a structured warning prefix. ``SILENT`` returns ``0``
    without touching the stream.
    """
    out = stream if stream is not None else sys.stderr
    if policy is HarnessContractMissingPolicy.BLOCK:
        print(str(error), file=out)
        return 2
    if policy is HarnessContractMissingPolicy.WARN:
        print(f"warning: {error}", file=out)
        return 0
    return 0


@dataclass(frozen=True)
class MainSurfacePattern:
    pattern: str
    reason: str


@dataclass(frozen=True)
class BranchIsolationPolicy:
    code_roots: tuple[str, ...]
    protected_extensions: tuple[str, ...]
    root_protected_files: tuple[str, ...]
    protected_main_surfaces: tuple[MainSurfacePattern, ...]
    permitted_main_surfaces: tuple[MainSurfacePattern, ...]
    # internal: split the legacy `protected_main_surfaces` list
    # into two intent-named surfaces. `first_edit_protected_surfaces`
    # drives the PreToolUse file-mutation isolation hooks (planning
    # artefacts that must originate on a task branch).
    # `state_dirty_surfaces` drives the post-merge `check-main-clean`
    # tripwire (runtime state files only). Defaulted to () so existing
    # test fixtures and callers that construct the policy by hand keep
    # working unchanged during the transition.
    state_dirty_surfaces: tuple[MainSurfacePattern, ...] = ()
    first_edit_protected_surfaces: tuple[MainSurfacePattern, ...] = ()


def _contract_error(contract_path: Path, detail: str) -> HarnessContractMissingError:
    return HarnessContractMissingError(
        "HarnessContractMissingError: unable to load branch-isolation policy from "
        f"{contract_path}. {detail} Remediation: restore or fix "
        "docs/workbay/contracts/harness-protocol.yaml before attempting main-branch edits."
    )


def _strip_yaml_comment(raw_line: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    result: list[str] = []
    for char in raw_line:
        if escaped:
            result.append(char)
            escaped = False
            continue
        if char == "\\" and in_double:
            result.append(char)
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            result.append(char)
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            result.append(char)
            continue
        if char == "#" and not in_single and not in_double:
            break
        result.append(char)
    return "".join(result).rstrip()


def _parse_scalar(raw_value: str, *, contract_path: Path) -> str:
    value = _strip_yaml_comment(raw_value).strip()
    if not value:
        return ""
    if value[0] in {"'", '"'}:
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError) as exc:
            raise _contract_error(contract_path, f"Could not parse quoted scalar `{value}`.") from exc
        if not isinstance(parsed, str):
            raise _contract_error(contract_path, f"Expected a string scalar, got `{type(parsed).__name__}`.")
        return parsed
    return value


def _extract_branch_isolation_lines(contract_path: Path) -> list[str]:
    try:
        lines = contract_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise _contract_error(contract_path, "File is missing or unreadable.") from exc

    start: int | None = None
    for idx, raw_line in enumerate(lines):
        if _strip_yaml_comment(raw_line).strip() == "branch_isolation:":
            start = idx + 1
            break
    if start is None:
        raise _contract_error(contract_path, "Top-level `branch_isolation:` block was not found.")

    block: list[str] = []
    for raw_line in lines[start:]:
        stripped = _strip_yaml_comment(raw_line)
        if stripped and not raw_line.startswith((" ", "\t")):
            break
        block.append(raw_line)
    return block


def _parse_branch_isolation_mapping(contract_path: Path) -> dict[str, list[object]]:
    block_lines = _extract_branch_isolation_lines(contract_path)
    parsed: dict[str, list[object]] = {}
    index = 0

    while index < len(block_lines):
        raw_line = block_lines[index]
        stripped_line = _strip_yaml_comment(raw_line)
        if not stripped_line.strip():
            index += 1
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if indent != 2 or not stripped_line.strip().endswith(":"):
            raise _contract_error(
                contract_path,
                "Expected an indented `branch_isolation` key at two-space indent, "
                f"got `{stripped_line.strip()}`.",
            )

        key = stripped_line.strip()[:-1]
        index += 1
        items: list[object] = []

        while index < len(block_lines):
            entry_line = block_lines[index]
            stripped_entry = _strip_yaml_comment(entry_line)
            if not stripped_entry.strip():
                index += 1
                continue

            entry_indent = len(entry_line) - len(entry_line.lstrip(" "))
            if entry_indent <= 2:
                break
            if entry_indent != 4 or not stripped_entry.strip().startswith("- "):
                raise _contract_error(
                    contract_path,
                    f"Expected a list item under `branch_isolation.{key}`, got `{stripped_entry.strip()}`.",
                )

            item_body = stripped_entry.strip()[2:].strip()
            if ":" in item_body:
                item_key, raw_value = item_body.split(":", 1)
                mapping: dict[str, str] = {item_key.strip(): _parse_scalar(raw_value, contract_path=contract_path)}
                index += 1
                while index < len(block_lines):
                    continuation_line = block_lines[index]
                    stripped_continuation = _strip_yaml_comment(continuation_line)
                    if not stripped_continuation.strip():
                        index += 1
                        continue

                    continuation_indent = len(continuation_line) - len(continuation_line.lstrip(" "))
                    if continuation_indent <= 4:
                        break
                    if continuation_indent != 6 or ":" not in stripped_continuation:
                        raise _contract_error(
                            contract_path,
                            f"Expected a mapping entry under `branch_isolation.{key}`, got "
                            f"`{stripped_continuation.strip()}`.",
                        )
                    subkey, raw_subvalue = stripped_continuation.strip().split(":", 1)
                    mapping[subkey.strip()] = _parse_scalar(raw_subvalue, contract_path=contract_path)
                    index += 1
                items.append(mapping)
                continue

            items.append(_parse_scalar(item_body, contract_path=contract_path))
            index += 1

        parsed[key] = items

    return parsed


def _require_string_list(mapping: dict[str, list[object]], key: str, *, contract_path: Path) -> tuple[str, ...]:
    values = mapping.get(key, [])
    if not isinstance(values, list):
        raise _contract_error(contract_path, f"`branch_isolation.{key}` must be a list.")
    parsed: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value:
            raise _contract_error(contract_path, f"`branch_isolation.{key}` entries must be non-empty strings.")
        parsed.append(value)
    return tuple(parsed)


def _load_surface_patterns(
    mapping: dict[str, list[object]],
    *,
    key: str,
    contract_path: Path,
) -> tuple[MainSurfacePattern, ...]:
    values = mapping.get(key, [])
    if not isinstance(values, list):
        raise _contract_error(contract_path, f"`branch_isolation.{key}` must be a list.")

    parsed: list[MainSurfacePattern] = []
    for entry in values:
        if not isinstance(entry, dict):
            raise _contract_error(
                contract_path,
                f"`branch_isolation.{key}` entries must be mappings with `pattern` and `reason`.",
            )
        pattern = entry.get("pattern")
        reason = entry.get("reason")
        if not isinstance(pattern, str) or not pattern:
            raise _contract_error(contract_path, f"Every `{key}` entry requires a non-empty `pattern`.")
        if not isinstance(reason, str) or not reason:
            raise _contract_error(contract_path, f"`{pattern}` is missing a non-empty `reason`.")
        parsed.append(MainSurfacePattern(pattern=pattern, reason=reason))
    return tuple(parsed)


def load_branch_isolation_policy(workspace_root: Path) -> BranchIsolationPolicy:
    contract_path = workspace_root / CONTRACT_RELATIVE_PATH
    mapping = _parse_branch_isolation_mapping(contract_path)
    return BranchIsolationPolicy(
        code_roots=_require_string_list(mapping, "code_roots", contract_path=contract_path),
        protected_extensions=_require_string_list(mapping, "protected_extensions", contract_path=contract_path),
        root_protected_files=_require_string_list(mapping, "root_protected_files", contract_path=contract_path),
        protected_main_surfaces=_load_surface_patterns(
            mapping,
            key="protected_main_surfaces",
            contract_path=contract_path,
        ),
        permitted_main_surfaces=_load_surface_patterns(
            mapping,
            key="permitted_main_surfaces",
            contract_path=contract_path,
        ),
        state_dirty_surfaces=_load_surface_patterns(
            mapping,
            key="state_dirty_surfaces",
            contract_path=contract_path,
        ),
        first_edit_protected_surfaces=_load_surface_patterns(
            mapping,
            key="first_edit_protected_surfaces",
            contract_path=contract_path,
        ),
    )


def is_branch_isolation_protected_path(rel_path: str, policy: BranchIsolationPolicy) -> bool:
    normalized = rel_path.strip().replace("\\", "/").lstrip("/")
    if not normalized:
        return False
    if normalized in policy.root_protected_files:
        return True
    if find_protected_main_surface(normalized, policy) is not None:
        return True
    if PurePosixPath(normalized).suffix not in policy.protected_extensions:
        return False
    return any(normalized.startswith(root) for root in policy.code_roots)


def _matches_surface_pattern(candidate: PurePosixPath, normalized: str, pattern: str) -> bool:
    if fnmatch.fnmatchcase(normalized, pattern):
        return True
    if "/**/" in pattern and fnmatch.fnmatchcase(normalized, pattern.replace("/**/", "/")):
        return True
    if pattern.endswith("/**"):
        prefix = pattern[:-3].rstrip("/")
        if not prefix:
            return True
        return normalized == prefix or normalized.startswith(prefix + "/")
    return False


def _find_surface_match(rel_path: str, surfaces: tuple[MainSurfacePattern, ...]) -> MainSurfacePattern | None:
    normalized = rel_path.strip().replace("\\", "/").lstrip("/")
    if not normalized:
        return None
    candidate = PurePosixPath(normalized)
    for surface in surfaces:
        if _matches_surface_pattern(candidate, normalized, surface.pattern):
            return surface
    return None


def find_protected_main_surface(rel_path: str, policy: BranchIsolationPolicy) -> MainSurfacePattern | None:
    return _find_surface_match(rel_path, policy.protected_main_surfaces)


def find_permitted_main_surface(rel_path: str, policy: BranchIsolationPolicy) -> MainSurfacePattern | None:
    return _find_surface_match(rel_path, policy.permitted_main_surfaces)


def is_permitted_main_surface(rel_path: str, policy: BranchIsolationPolicy) -> tuple[bool, str | None]:
    surface = find_permitted_main_surface(rel_path, policy)
    if surface is None:
        return False, None
    return True, surface.reason


# internal — intent-named predicates.
#
# ``is_state_dirty_path`` answers the post-merge ``check-main-clean``
# question: "is this path a runtime state file whose dirt on main is a
# real regression?" It is intentionally disjoint from
# ``is_first_edit_protected_path``, which answers the PreToolUse
# question: "is this path a planning artefact whose first edit must
# happen on a task branch?"
#
# Splitting the two predicates is the contract change that lets
# ``check-main-clean`` stop tripping on planning files that arrived via
# a clean fast-forward — those still register as
# first-edit-protected for the file-mutation hook, but they no longer
# register as state-dirty for the post-merge tripwire.


def is_state_dirty_path(rel_path: str, policy: BranchIsolationPolicy) -> bool:
    """Return True when ``rel_path`` matches a ``state_dirty_surfaces`` pattern."""
    return _find_surface_match(rel_path, policy.state_dirty_surfaces) is not None


def is_first_edit_protected_path(rel_path: str, policy: BranchIsolationPolicy) -> bool:
    """Return True when ``rel_path`` matches a ``first_edit_protected_surfaces`` pattern."""
    return _find_surface_match(rel_path, policy.first_edit_protected_surfaces) is not None
