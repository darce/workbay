"""Resolve and validate the operator-owned coordination MCP bridge pin.

Only the primary checkout's ``config/lane-orchestration/<task>.json`` can
enable the bridge. Lane-local files, environment variables, prompts, and
caller-authored command fragments are deliberately not configuration sources.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

BRIDGE_SERVER_NAME = "workbay_coord"
BRIDGE_MODULE = "workbay_orchestrator_mcp.orchestration.coordservice"
BRIDGE_SCHEMA_VERSION = 1
BridgeTransport = Literal["stdio"]

_MANIFEST_COUNT_LIMIT = 256
_MANIFEST_BYTES_LIMIT = 1024 * 1024
_MANIFEST_TOTAL_BYTES_LIMIT = 16 * 1024 * 1024
_TASK_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_BINDING_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
_COMMAND_RE = re.compile(r"^/[A-Za-z0-9._/-]+$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


class CoordinationBridgeConfigurationError(Exception):
    """A configured bridge is malformed, ambiguous, or not trusted.

    This intentionally does not inherit from ``ValueError`` or ``RuntimeError``:
    daemon snapshot code has legacy catch-and-degrade arms for those generic
    types, and a broken configured bridge must reach an explicit refusal path.
    """


@dataclass(frozen=True)
class CoordinationBridgeConfig:
    """Validated stdio bridge config plus the trusted lane identity it belongs to."""

    schema_version: int
    transport: BridgeTransport
    command: str
    args: tuple[str, ...]
    task_ref: str
    lane_id: str

    def codex_overrides(self) -> list[str]:
        """Return Codex ``-c`` pairs using TOML values, with no shell joining."""
        command = _toml_string(self.command)
        args = "[" + ", ".join(_toml_string(value) for value in self.args) + "]"
        return [
            "-c",
            f"mcp_servers.{BRIDGE_SERVER_NAME}.command={command}",
            "-c",
            f"mcp_servers.{BRIDGE_SERVER_NAME}.args={args}",
        ]

    def capability_value(self) -> dict[str, object]:
        """Canonical bridge dimension for capability receipts."""
        return {
            "schema_version": self.schema_version,
            "transport": self.transport,
            "server_name": BRIDGE_SERVER_NAME,
            "command": self.command,
            "args": list(self.args),
            "identity": {"task_ref": self.task_ref, "lane_id": self.lane_id},
        }


def _toml_string(value: str) -> str:
    # JSON basic strings use TOML-compatible quoting/escapes. All controls are
    # rejected while validating config, so this cannot emit JSON-only escapes.
    return json.dumps(value, ensure_ascii=False)


def _no_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key {key!r}")
        result[key] = value
    return result


def _primary_repository_root(worktree_path: Path) -> Path | None:
    """Resolve the primary checkout via Git's common dir; never fall back to lane files."""
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(worktree_path),
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    common_text = result.stdout.strip()
    if result.returncode != 0 or not common_text:
        return None
    common = Path(common_text).expanduser().resolve()
    if common.name != ".git":
        return None
    return common.parent.resolve()


def _manifest_worktree(value: object, *, primary_root: Path) -> Path | None:
    if not isinstance(value, str) or not value.strip() or _CONTROL_RE.search(value):
        return None
    expanded = value.replace("{orchestrator_root}", str(primary_root))
    if "{" in expanded or "}" in expanded:
        return None
    try:
        candidate = Path(expanded).expanduser()
        if not candidate.is_absolute():
            candidate = primary_root / candidate
        return candidate.resolve()
    except (OSError, RuntimeError, ValueError):
        return None


def _mentions_target_worktree(raw: bytes, worktree_path: Path, primary_root: Path) -> bool:
    """Spot a damaged target manifest without rejecting unrelated broken JSON."""
    text = raw.decode("utf-8", errors="ignore")
    candidates = {str(worktree_path)}
    try:
        relative = worktree_path.relative_to(primary_root)
    except ValueError:
        relative = Path(os.path.relpath(worktree_path, primary_root))
    if relative is not None:
        candidates.add(str(relative))
        candidates.add("{orchestrator_root}/" + str(relative))
    return any(candidate and candidate in text for candidate in candidates)


def _manifest_targets_worktree(path: Path, worktree: Path, primary_root: Path) -> bool:
    """Identify candidates without parsing unrelated manifests or retaining them.

    Keep one maximum manifest's overlap so even a path split across chunks is
    recognized. Full strict parsing and size limits still apply to candidates.
    """
    pattern = re.compile(r'"worktree_path"\s*:\s*("(?:[^"\\]|\\.)*")')
    try:
        with path.open("rb") as stream:
            overlap = b""
            while chunk := stream.read(64 * 1024):
                raw = overlap + chunk
                if _mentions_target_worktree(raw, worktree, primary_root):
                    return True
                for match in pattern.finditer(raw.decode("utf-8", errors="ignore")):
                    try:
                        value = json.loads(match.group(1))
                    except ValueError:
                        continue
                    if _manifest_worktree(value, primary_root=primary_root) == worktree:
                        return True
                overlap = raw[-_MANIFEST_BYTES_LIMIT:]
    except OSError as exc:
        raise CoordinationBridgeConfigurationError(f"cannot inspect primary lane manifest {path}: {exc}") from exc
    return False


def _read_manifest(path: Path, *, target_worktree: Path, primary_root: Path) -> dict[str, object] | None:
    if path.is_symlink():
        if path.stem:
            raise CoordinationBridgeConfigurationError(
                f"primary lane manifest {path} is a symlink; refusing an untrusted bridge source"
            )
        return None
    try:
        stat = path.stat()
    except OSError as exc:
        raise CoordinationBridgeConfigurationError(f"cannot inspect primary lane manifest {path}: {exc}") from exc
    if not path.is_file():
        return None
    if stat.st_size > _MANIFEST_BYTES_LIMIT:
        raise CoordinationBridgeConfigurationError(
            f"primary lane manifest {path} exceeds {_MANIFEST_BYTES_LIMIT} bytes"
        )
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise CoordinationBridgeConfigurationError(f"cannot read primary lane manifest {path}: {exc}") from exc
    try:
        parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        if _mentions_target_worktree(raw, target_worktree, primary_root):
            raise CoordinationBridgeConfigurationError(
                f"primary lane manifest {path} matching worktree {target_worktree} is malformed: {exc}"
            ) from exc
        return None
    return parsed if isinstance(parsed, dict) else None


def _safe_absolute_remote_path(value: object, *, field: str, suffix: str | None = None) -> str:
    if not isinstance(value, str) or not value or _CONTROL_RE.search(value):
        raise CoordinationBridgeConfigurationError(f"coordination_bridge.{field} must be a safe absolute path")
    path = PurePosixPath(value)
    if not path.is_absolute() or str(path) != value or ".." in path.parts or path == PurePosixPath("/"):
        raise CoordinationBridgeConfigurationError(f"coordination_bridge.{field} must be a normalized absolute path")
    if suffix is not None and path.suffix != suffix:
        raise CoordinationBridgeConfigurationError(f"coordination_bridge.{field} must end in {suffix}")
    return value


def _validate_bridge(raw: object, *, task_ref: str, lane_id: str) -> CoordinationBridgeConfig:
    if not isinstance(raw, dict):
        raise CoordinationBridgeConfigurationError("lane.coordination_bridge must be an object")
    expected_keys = {"schema_version", "transport", "command", "args"}
    unknown = sorted(set(raw) - expected_keys)
    missing = sorted(expected_keys - set(raw))
    if unknown or missing:
        pieces = []
        if missing:
            pieces.append(f"missing key(s) {missing}")
        if unknown:
            pieces.append(f"unknown key(s) {unknown}")
        raise CoordinationBridgeConfigurationError("lane.coordination_bridge " + "; ".join(pieces))
    if type(raw["schema_version"]) is not int or raw["schema_version"] != BRIDGE_SCHEMA_VERSION:
        raise CoordinationBridgeConfigurationError(
            f"coordination_bridge.schema_version must be {BRIDGE_SCHEMA_VERSION}"
        )
    if raw["transport"] != "stdio":
        raise CoordinationBridgeConfigurationError("coordination_bridge.transport must be 'stdio'")

    command = raw["command"]
    command_path = PurePosixPath(command) if isinstance(command, str) else PurePosixPath(".")
    command_name = command_path.name
    is_python = command_name in {"python", "python3"} or bool(re.fullmatch(r"python[0-9]+(?:\.[0-9]+)*", command_name))
    if (
        not isinstance(command, str)
        or not _COMMAND_RE.fullmatch(command)
        or str(command_path) != command
        or ".." in command_path.parts
        or not is_python
    ):
        raise CoordinationBridgeConfigurationError(
            "coordination_bridge.command must be an absolute Python interpreter path with no shell text"
        )

    args = raw["args"]
    if not isinstance(args, list) or len(args) != 7 or not all(isinstance(item, str) for item in args):
        raise CoordinationBridgeConfigurationError("coordination_bridge.args must be the fixed seven-item argv list")
    if any(_CONTROL_RE.search(item) or "{" in item or "}" in item for item in args):
        raise CoordinationBridgeConfigurationError("coordination_bridge.args must not contain controls or placeholders")
    if args[0] != "-m" or args[1] != BRIDGE_MODULE or args[2] != "--bindings":
        raise CoordinationBridgeConfigurationError("coordination_bridge.args must invoke the fixed coordservice module")
    config_path = _safe_absolute_remote_path(args[3], field="args[3]", suffix=".json")
    if args[4] != "--binding" or not _BINDING_RE.fullmatch(args[5]):
        raise CoordinationBridgeConfigurationError("coordination_bridge.args must select one safe --binding alias")
    if args[6] != "--mcp":
        raise CoordinationBridgeConfigurationError("coordination_bridge.args must end with --mcp")

    return CoordinationBridgeConfig(
        schema_version=BRIDGE_SCHEMA_VERSION,
        transport="stdio",
        command=command,
        args=("-m", BRIDGE_MODULE, "--bindings", config_path, "--binding", args[5], "--mcp"),
        task_ref=task_ref,
        lane_id=lane_id,
    )


def resolve_coordination_bridge(
    worktree_path: Path | str,
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> CoordinationBridgeConfig | None:
    """Resolve one exact-worktree bridge config from its primary checkout.

    Absence is a no-op. A malformed config on the exact target lane, a second
    exact-worktree binding, or an unreadable configured target fails closed.
    Branch names and lane-local config never participate in resolution.
    """
    worktree = Path(worktree_path).expanduser().resolve()
    primary_root = _primary_repository_root(worktree)
    if primary_root is None:
        return None
    manifest_dir = primary_root / "config" / "lane-orchestration"
    if manifest_dir.is_symlink():
        raise CoordinationBridgeConfigurationError(
            f"primary lane manifest directory {manifest_dir} is a symlink; refusing an untrusted bridge source"
        )
    if not manifest_dir.exists():
        return None
    if manifest_dir.is_symlink() or not manifest_dir.is_dir():
        raise CoordinationBridgeConfigurationError(
            f"primary lane manifest directory {manifest_dir} is not a trusted directory"
        )
    try:
        manifest_paths = sorted(manifest_dir.glob("*.json"))
    except OSError as exc:
        raise CoordinationBridgeConfigurationError(f"cannot list primary lane manifests: {exc}") from exc
    if task_ref is not None and (not isinstance(task_ref, str) or not _TASK_REF_RE.fullmatch(task_ref)):
        # An opaque/malformed caller task name is not a safe file selector. The
        # exact worktree lookup below remains authoritative and independent.
        task_ref = None

    matches: list[tuple[str, str, bool, object]] = []
    total_bytes = 0
    candidate_count = 0
    for path in manifest_paths:
        if path.is_symlink():
            continue
        # Select the named task before charging parsing limits. Callers with
        # only a worktree use a bounded-memory lexical scan, not a JSON parse
        # of every archived task in the primary checkout.
        if task_ref is not None:
            if path.stem != task_ref:
                continue
        elif not _manifest_targets_worktree(path, worktree, primary_root):
            continue
        candidate_count += 1
        if candidate_count > _MANIFEST_COUNT_LIMIT:
            raise CoordinationBridgeConfigurationError(
                f"primary lane manifest count exceeds bounded lookup limit {_MANIFEST_COUNT_LIMIT}"
            )
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise CoordinationBridgeConfigurationError(f"cannot inspect primary lane manifest {path}: {exc}") from exc
        total_bytes += max(size, 0)
        if total_bytes > _MANIFEST_TOTAL_BYTES_LIMIT:
            raise CoordinationBridgeConfigurationError(
                f"primary lane manifests exceed bounded lookup size {_MANIFEST_TOTAL_BYTES_LIMIT} bytes"
            )
        manifest = _read_manifest(path, target_worktree=worktree, primary_root=primary_root)
        if manifest is None:
            continue
        manifest_task = manifest.get("task_ref")
        lanes = manifest.get("lanes")
        if not isinstance(manifest_task, str) or not _TASK_REF_RE.fullmatch(manifest_task):
            continue
        if manifest_task != path.stem or not isinstance(lanes, dict):
            continue
        for candidate_lane_id, lane in lanes.items():
            if not isinstance(candidate_lane_id, str) or not isinstance(lane, dict):
                continue
            candidate_worktree = _manifest_worktree(lane.get("worktree_path"), primary_root=primary_root)
            if candidate_worktree == worktree:
                matches.append(
                    (
                        manifest_task,
                        candidate_lane_id,
                        "coordination_bridge" in lane,
                        lane.get("coordination_bridge"),
                    )
                )

    # If the explicit task selector found nothing, the exact worktree still
    # gets a bounded global lookup so a wrong/old task hint cannot suppress an
    # operator pin. Re-enter without a selector once; no branch matching used.
    if task_ref is not None and not matches:
        return resolve_coordination_bridge(worktree, task_ref=None, lane_id=lane_id)
    if not matches:
        return None
    if len(matches) != 1:
        descriptions = sorted(f"{task}/{lane}" for task, lane, _present, _bridge in matches)
        raise CoordinationBridgeConfigurationError(
            f"multiple primary lane manifests bind exact worktree {worktree}: {descriptions}"
        )
    matched_task, matched_lane, bridge_present, bridge_raw = matches[0]
    if lane_id is not None and lane_id != matched_lane:
        raise CoordinationBridgeConfigurationError(
            f"exact worktree {worktree} belongs to lane {matched_lane!r}, not requested lane {lane_id!r}"
        )
    if not bridge_present:
        return None
    return _validate_bridge(bridge_raw, task_ref=matched_task, lane_id=matched_lane)
