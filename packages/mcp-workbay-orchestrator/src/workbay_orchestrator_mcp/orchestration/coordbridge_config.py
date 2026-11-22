"""Resolve and validate the operator-owned coordination MCP bridge pin.

Only the primary checkout's ``config/lane-orchestration/<task>.json`` can
enable the bridge. Lane-local files, environment variables, prompts, and
caller-authored command fragments are deliberately not configuration sources.
"""

from __future__ import annotations

import json
import logging
import re
import stat
import subprocess
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import chain
from pathlib import Path, PurePosixPath
from typing import Literal

from . import coordclaims_transport, worktree_root

BRIDGE_SERVER_NAME = "workbay_coord"
BRIDGE_MODULE = "workbay_orchestrator_mcp.orchestration.coordservice"
BRIDGE_SCHEMA_VERSION = 1
BridgeTransport = Literal["stdio"]
BridgePinStatus = Literal["pinned", "unpinned_bindings_missing"]
CoordinationBridgeConfigurationReason = Literal["bindings_unreadable", "bindings_malformed"]
BRIDGE_PINNED: BridgePinStatus = "pinned"
BRIDGE_UNPINNED_BINDINGS_MISSING: BridgePinStatus = "unpinned_bindings_missing"
BINDINGS_UNREADABLE: CoordinationBridgeConfigurationReason = "bindings_unreadable"
BINDINGS_MALFORMED: CoordinationBridgeConfigurationReason = "bindings_malformed"

logger = logging.getLogger(__name__)

_MANIFEST_COUNT_LIMIT = 256
_MANIFEST_TOTAL_BYTES_LIMIT = 16 * 1024 * 1024
_BINDINGS_CONFIG_LIMIT = 1024 * 1024
_TASK_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_BINDING_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
_COMMAND_RE = re.compile(r"^/[A-Za-z0-9._/-]+$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


class CoordinationBridgeConfigurationError(Exception):
    """A configured bridge is malformed, ambiguous, or not trusted.

    This intentionally does not inherit from ``ValueError`` or ``RuntimeError``:
    daemon snapshot code has legacy catch-and-degrade arms for those generic
    types, and a broken configured bridge must reach an explicit refusal path.

    ``reason`` is a stable machine-readable tag for refusals a caller must tell
    apart (see ``BINDINGS_UNREADABLE`` / ``BINDINGS_MALFORMED``); it is ``None``
    for every other refusal.
    """

    def __init__(self, message: str, *, reason: CoordinationBridgeConfigurationReason | None = None) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class CoordinationBridgeConfig:
    """Validated stdio bridge config plus the trusted lane identity it belongs to."""

    schema_version: int
    transport: BridgeTransport
    command: str
    args: tuple[str, ...]
    task_ref: str
    lane_id: str
    binding_digest: str | None = None

    @property
    def pin_status(self) -> BridgePinStatus:
        """Derive pin status from the sole source of truth, the binding digest."""
        return BRIDGE_PINNED if self.binding_digest is not None else BRIDGE_UNPINNED_BINDINGS_MISSING

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
        """Canonical bridge dimension for capability receipts.

        A pinned bridge is evidenced by its ``binding_digest`` and keeps the
        pre-existing key set, so its receipt digest is unchanged. An unpinned
        bridge also carries ``pin_status``: the receipt says why it has no
        content pin instead of leaving ``binding_digest: None`` unexplained.
        """
        value: dict[str, object] = {
            "schema_version": self.schema_version,
            "transport": self.transport,
            "server_name": BRIDGE_SERVER_NAME,
            "command": self.command,
            "args": list(self.args),
            "identity": {"task_ref": self.task_ref, "lane_id": self.lane_id},
            "binding_digest": self.binding_digest,
        }
        if self.pin_status != BRIDGE_PINNED:
            value["pin_status"] = self.pin_status
        return value


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
    try:
        expanded = worktree_root.expand_lane_path(value, orchestrator_root=primary_root)
    except worktree_root.WorktreeRootError as exc:
        raise CoordinationBridgeConfigurationError(f"worktree root unusable: {exc.reason}") from exc
    if "{" in expanded or "}" in expanded:
        return None
    try:
        candidate = Path(expanded).expanduser()
        if not candidate.is_absolute():
            candidate = primary_root / candidate
        return candidate.resolve()
    except (OSError, RuntimeError, ValueError):
        return None


_MANIFEST_TOKEN_RE = re.compile(rb'[ \t\r\n]*+([{}\[\],:"]|[^{}\[\],:" \t\r\n]++)')
_MANIFEST_STRING_BODY_RE = re.compile(rb'[^"\\]*+(?:\\.[^"\\]*+)*+', re.DOTALL)
_MANIFEST_CLOSER = {b"{": b"}", b"[": b"]"}


def _manifest_tokens(chunks: Iterable[bytes], limit: int) -> Iterator[bytes]:
    """Keep string and atom boundaries across bounded reads."""
    quoted = escaped = continuing_atom = False
    string: bytearray | None = None
    for chunk in chunks:
        position = 0
        while position < len(chunk):
            if quoted:
                start = position
                if escaped:
                    position += 1
                body = _MANIFEST_STRING_BODY_RE.match(chunk, position)
                assert body is not None
                position = body.end()
                ending = chunk[position : position + 1]
                position += len(ending)
                escaped = ending == b"\\"
                if string is not None:
                    if len(string) + position - start > limit:
                        string = None
                    else:
                        string.extend(chunk[start:position])
                if ending == b'"':
                    quoted = False
                    # A larger string is opaque to recovery, but still one token.
                    yield bytes(string) if string is not None else b'""'
                continue

            match = _MANIFEST_TOKEN_RE.match(chunk, position)
            if match is None:
                continuing_atom = False
                break
            start = match.start(1)
            first = chunk[start : start + 1]
            position = match.end()
            if first == b'"':
                quoted = True
                string = bytearray(first)
            elif first in b"{}[],:":
                yield first
            elif not continuing_atom or start != 0:
                yield b"?"
            continuing_atom = first not in b'{}[],:"' and position == len(chunk)


def _manifest_string(token: bytes) -> str | None:
    if token.startswith(b'"'):
        try:
            value = json.loads(token.decode("utf-8"))
        except ValueError:
            return None
        if isinstance(value, str):
            return value
    return None


def _manifest_targets_worktree(
    chunks: Iterable[bytes],
    target_worktree: Path,
    primary_root: Path,
    limit: int,
) -> bool:
    """Detect an exact direct lane binding in a structurally readable prefix.

    Only the native parse can supply configuration. This scan supplies scope
    for a refusal after that parse fails or the manifest exceeds its size limit.
    """
    tokens = _manifest_tokens(chunks, limit)

    def skip_value(value: bytes) -> None:
        if value not in _MANIFEST_CLOSER:
            return
        closers = [_MANIFEST_CLOSER[value]]
        while closers:
            token = next(tokens)
            if token in _MANIFEST_CLOSER:
                if len(closers) >= limit:
                    raise ValueError("manifest nesting exceeds recovery budget")
                closers.append(_MANIFEST_CLOSER[token])
            elif token in (b"}", b"]") and token != closers.pop():
                raise ValueError("mismatched manifest container")

    def contains_target(scope: Literal["root", "lanes", "lane"]) -> bool:
        previous_string: bytes | None = None
        awaiting_value = False
        for token in tokens:
            if token == b"}":
                return False
            if token == b"]":
                raise ValueError("mismatched manifest container")
            if token == b":":
                # A colon introduces a member even if the preceding string
                # looked like the value of an incomplete member.
                awaiting_value = previous_string is not None
                continue

            key = previous_string if awaiting_value else None
            previous_string = token if token.startswith(b'"') else None
            # A quoted key at a member boundary may have a missing colon.
            # Commas and unquoted atoms clear the pending member.
            awaiting_value = key is None and previous_string is not None
            if key is None:
                skip_value(token)
                continue

            if scope == "root" and token == b"{" and _manifest_string(key) == "lanes":
                if contains_target("lanes"):
                    return True
            elif scope == "lanes" and token == b"{" and _manifest_string(key) is not None:
                if contains_target("lane"):
                    return True
            elif (
                scope == "lane"
                and previous_string is not None
                and _manifest_string(key) == "worktree_path"
                and _manifest_worktree(_manifest_string(previous_string), primary_root=primary_root) == target_worktree
            ):
                return True
            else:
                skip_value(token)
        return False

    try:
        return next(tokens) == b"{" and contains_target("root")
    except (StopIteration, ValueError):
        return False


def _read_manifest(path: Path, *, target_worktree: Path, primary_root: Path) -> tuple[dict[str, object], int] | None:
    from . import lane_manifest

    limit = lane_manifest.MANIFEST_BYTES_LIMIT
    try:
        if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
            return None
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
            cause: ValueError | RecursionError | None = None
            chunks: Iterable[bytes] = (raw.removeprefix(b"\xef\xbb\xbf"),)
            if len(raw) > limit:
                chunks = chain(chunks, iter(lambda: stream.read(limit), b""))
                problem = f"exceeds {limit} bytes"
            else:
                try:
                    # Validate the whole document before recovering any lane identity.
                    parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
                except (ValueError, RecursionError) as exc:
                    cause = exc
                    problem = f"matching worktree {target_worktree} is malformed: {exc}"
                else:
                    return (parsed, len(raw)) if isinstance(parsed, dict) else None
            if _manifest_targets_worktree(chunks, target_worktree, primary_root, limit):
                raise CoordinationBridgeConfigurationError(f"primary lane manifest {path} {problem}") from cause
            return None
    except OSError as exc:
        raise CoordinationBridgeConfigurationError(f"cannot read primary lane manifest {path}: {exc}") from exc


def _safe_absolute_remote_path(value: object, *, field: str, suffix: str | None = None) -> str:
    if not isinstance(value, str) or not value or _CONTROL_RE.search(value):
        raise CoordinationBridgeConfigurationError(f"coordination_bridge.{field} must be a safe absolute path")
    path = PurePosixPath(value)
    if not path.is_absolute() or str(path) != value or ".." in path.parts or path == PurePosixPath("/"):
        raise CoordinationBridgeConfigurationError(f"coordination_bridge.{field} must be a normalized absolute path")
    if suffix is not None and path.suffix != suffix:
        raise CoordinationBridgeConfigurationError(f"coordination_bridge.{field} must end in {suffix}")
    return value


def _selected_binding_digest(config_path: str, alias: str) -> str | None:
    """Fingerprint the configured binding when its local snapshot is available.

    ``None`` means exactly one thing: the bindings file is absent on this host.
    A file that is present but unreadable or malformed raises, tagged with its
    own ``reason``, and is never conflated with the absent case.
    """
    try:
        with Path(config_path).open("rb") as stream:
            raw = stream.read(_BINDINGS_CONFIG_LIMIT + 1)
    except FileNotFoundError:
        # Some remote bridge configs are only present on the host that launches
        # the MCP process; the caller reports that the bridge is unpinned.
        return None
    except OSError as exc:
        raise CoordinationBridgeConfigurationError(
            f"cannot read coordination bindings {config_path}: {exc}", reason=BINDINGS_UNREADABLE
        ) from exc
    if len(raw) > _BINDINGS_CONFIG_LIMIT:
        raise CoordinationBridgeConfigurationError(
            "coordination bindings config exceeds 1 MiB", reason=BINDINGS_MALFORMED
        )
    try:
        config = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, ValueError) as exc:
        raise CoordinationBridgeConfigurationError(
            f"coordination bindings config {config_path} is invalid", reason=BINDINGS_MALFORMED
        ) from exc
    if (
        not isinstance(config, dict)
        or type(config.get("schema_version")) is not int
        or config["schema_version"] != 1
        or not isinstance(config.get("bindings"), dict)
        or alias not in config["bindings"]
    ):
        raise CoordinationBridgeConfigurationError(
            "coordination bindings config does not contain selected binding", reason=BINDINGS_MALFORMED
        )
    try:
        return coordclaims_transport.binding_digest(config["bindings"][alias])
    except Exception as exc:
        raise CoordinationBridgeConfigurationError(
            "selected coordination binding is invalid", reason=BINDINGS_MALFORMED
        ) from exc


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

    binding_digest = _selected_binding_digest(config_path, args[5])
    bridge_args = ("-m", BRIDGE_MODULE, "--bindings", config_path, "--binding", args[5], "--mcp")
    if binding_digest is not None:
        bridge_args += ("--expected-binding-digest", binding_digest)
    else:
        # Not fail-closed (a remote bridge's bindings live on the launching
        # host), but never silent: the capability receipt carries the status.
        logger.warning(
            "coordination bridge bindings %s (binding %r) are absent on this host: bridge is UNPINNED "
            "(pin_status=%s); argv carries no --expected-binding-digest and the capability receipt "
            "keeps path-only identity",
            config_path,
            args[5],
            BRIDGE_UNPINNED_BINDINGS_MISSING,
        )

    return CoordinationBridgeConfig(
        schema_version=BRIDGE_SCHEMA_VERSION,
        transport="stdio",
        command=command,
        args=bridge_args,
        task_ref=task_ref,
        lane_id=lane_id,
        binding_digest=binding_digest,
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
    The caller's task hint does not narrow the search for exact bindings.
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
    matches: list[tuple[str, str, bool, object]] = []
    total_bytes = 0
    candidate_count = 0
    for path in manifest_paths:
        loaded = _read_manifest(path, target_worktree=worktree, primary_root=primary_root)
        if loaded is None:
            continue
        manifest, size = loaded
        manifest_task = manifest.get("task_ref")
        lanes = manifest.get("lanes")
        if not isinstance(manifest_task, str) or not _TASK_REF_RE.fullmatch(manifest_task):
            continue
        if manifest_task != path.stem or not isinstance(lanes, dict):
            continue
        # Charge each manifest once, after at least one exact lane match.
        first_match = len(matches)
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
        if len(matches) == first_match:
            continue
        candidate_count += 1
        if candidate_count > _MANIFEST_COUNT_LIMIT:
            raise CoordinationBridgeConfigurationError(
                f"primary lane manifest count exceeds bounded lookup limit {_MANIFEST_COUNT_LIMIT}"
            )
        total_bytes += size
        if total_bytes > _MANIFEST_TOTAL_BYTES_LIMIT:
            raise CoordinationBridgeConfigurationError(
                f"primary lane manifests exceed bounded lookup size {_MANIFEST_TOTAL_BYTES_LIMIT} bytes"
            )

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
