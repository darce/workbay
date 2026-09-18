"""Bounded, durable provisioning evidence shared by linked lane worktrees.

Each capability keeps its own provenance and age: refreshing workspace-write
must never extend an old git-write grant (CARD-06 / evidence-before-commitment).
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from .codex_lane_config import PREFLIGHT_ATTESTATION_TTL_S, PREFLIGHT_VERDICT_RECORDS_ENV
from .learned_wave_cap import bounded_file_lock

STATE_FILE = ".task-state/remote-preflight-attestation.json"
TTL_ENV = "WORKBAY_PREFLIGHT_ATTESTATION_TTL_S"
VERDICT_RECORDS_ENV = PREFLIGHT_VERDICT_RECORDS_ENV
CAPABILITY_RECEIPT_ENV = "WORKBAY_CAPABILITY_RECEIPT_ID"
_REFRESH_AHEAD_FRACTION = 0.8
_VERDICTS = frozenset({"positive", "negative", "unknown"})
_STRENGTH = {"operator_env": 1, "operator_file": 1, "live_probe": 2, "vm_commit": 3}
CAPABILITY_UNKNOWN_REASON = "capability_unknown"
CAPABILITY_NEGATIVE_REASON = "capability_negative"
CAPABILITY_MISSING_REASON = "capability_missing"
CAPABILITY_INCOMPLETE_REASON = "capability_incomplete"
RECEIPT_IDENTITY_INCOMPLETE_REASON = "receipt_identity_incomplete"
CAPABILITY_IDENTITY_MISMATCH_REASON = "capability_identity_mismatch"
STALE_WORKER_CONFIGURATION_REASON = "stale_worker_configuration"
TRANSPORT_MISSING_REASON = "transport_missing"
TRANSPORT_VERSION_SKEW_REASON = "transport_version_skew"
LANE_ATTESTATION_MISSING_REASON = "lane_attestation_missing"
UNSUPPORTED_ATTESTATION_SCHEMA_REASON = "unsupported_attestation_schema"
STALE_CAPABILITY_RECEIPT_REASON = "stale_capability_receipt"
CAPABILITY_RECEIPT_UNAVAILABLE_REASON = "capability_receipt_unavailable"
ATTESTATION_SCHEMA_VERSION = 2
_ENVELOPE_KEYS = frozenset({"schema_version", "lanes"})
# Same-pass remints keep a bounded chain so mint/bind/probe carriers all stay
# valid. Old readers still consume the scalar ``predecessor_receipt_id``.
PREDECESSOR_RECEIPT_CAP = 4

# These are the dimensions a mutating remote dispatch must bind before the
# adapter may launch a model. ``codex_version`` is deliberately not required:
# the remote sandbox identity is not observable from the host process, while
# the host, transport, and dispatch coordinates are all locally derivable.
CAPABILITY_REQUIRED_IDENTITY_FIELDS = (
    "lane_id",
    "pass_id",
    "dispatch_id",
    "gate_host",
    "sandbox_flags",
    "worktree_path",
    "config_digest",
    "transport_digest",
)

# ``attestation_root`` is used on several hot paths.  Besides avoiding a git
# subprocess after a caller has already resolved the root, this cache prevents a
# patched/mocked Popen in daemon-start tests from being consumed by root lookup.
_ATTESTATION_ROOT_CACHE: dict[Path, Path] = {}


def attestation_root(worktree: Path, *, resolved_root: Path | str | None = None) -> Path:
    """Use the primary checkout for linked worktrees, not a per-lane cache.

    ``resolved_root`` is the control-plane escape hatch for callers which have
    already resolved the common root.  A successful lookup is cached per
    worktree, so a later dispatch does not shell out merely to locate durable
    state.
    """
    worktree_path = Path(worktree).expanduser().resolve()
    if resolved_root is not None:
        root = Path(resolved_root).expanduser().resolve()
        _ATTESTATION_ROOT_CACHE[worktree_path] = root
        return root
    cached = _ATTESTATION_ROOT_CACHE.get(worktree_path)
    if cached is not None:
        return cached
    try:
        result = subprocess.run(
            ["git", "-C", str(worktree_path), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        common = Path(result.stdout.strip())
        if result.returncode == 0 and common.is_absolute() and common.name == ".git":
            root = common.parent
            _ATTESTATION_ROOT_CACHE[worktree_path] = root
            return root
    except (OSError, subprocess.TimeoutExpired):
        pass
    _ATTESTATION_ROOT_CACHE[worktree_path] = worktree_path
    return worktree_path


def _ttl(env: Mapping[str, str] | None) -> float:
    try:
        value = float((os.environ if env is None else env).get(TTL_ENV, PREFLIGHT_ATTESTATION_TTL_S))
        return value if math.isfinite(value) and value > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


def _identity(gate_host: str, codex_version: str, sandbox_flags: Sequence[str]) -> dict[str, str]:
    flags = list(sandbox_flags)
    if len(flags) % 2 == 0:
        # The probe spells -s before -c; AgentSpec spells -c before -s.
        # Sort independent options, preserving the order of repeated keys.
        pairs = list(zip(flags[::2], flags[1::2], strict=True))
        pairs.sort(key=lambda pair: (pair[0], pair[1].split("=", 1)[0] if pair[0] == "-c" else ""))
        flags = [item for pair in pairs for item in pair]
    return {
        "gate_host": gate_host,
        "codex_version": codex_version,
        "sandbox_flags_hash": hashlib.sha256(json.dumps(flags).encode()).hexdigest(),
    }


def _normalise_flags(sandbox_flags: Sequence[str]) -> list[str]:
    """Canonicalize the argv fragments used in a receipt identity."""
    flags = [str(value) for value in sandbox_flags]
    if len(flags) % 2 == 0:
        pairs = list(zip(flags[::2], flags[1::2], strict=True))
        pairs.sort(key=lambda pair: (pair[0], pair[1].split("=", 1)[0] if pair[0] == "-c" else ""))
        flags = [item for pair in pairs for item in pair]
    return flags


def _canonical_value(value: Any) -> Any:
    """Return JSON-stable values for configuration-digest inputs."""
    if isinstance(value, Path):
        return str(value.expanduser().resolve())
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_canonical_value(item) for item in value)
    return value


def configuration_digest(config: Mapping[str, Any] | None = None, **fields: Any) -> str:
    """Content-address a worker configuration snapshot.

    The helper intentionally accepts arbitrary fields: the controller and the
    worker can agree on a digest without importing each other's dataclasses,
    while Paths and unordered containers retain deterministic representations.
    """
    payload: dict[str, Any] = {}
    if config is not None:
        payload.update(dict(config))
    payload.update(fields)
    encoded = json.dumps(_canonical_value(payload), sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


# Short alias for callers which use the noun rather than the operation.
digest_config = configuration_digest


_CAPABILITY_BOUND_KEYS = ("max_turns", "timeout")


def _capability_cycle_bounds(single_cycle_bounds: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only the cycle-clock keys execute hashes into the receipt digest.

    Adapter derivation may also emit ``timeout_ceiling`` / ``timeout_source`` /
    ``timeout_saturated`` for operator telemetry. Those keys are not part of
    the worker configuration identity; hashing them here would diverge from
    ``remote_exec._current_worker_configuration_digest``.
    """
    bounds = dict(single_cycle_bounds or {})
    return {key: bounds[key] for key in _CAPABILITY_BOUND_KEYS if key in bounds}


def build_capability_configuration(
    *,
    agent: str,
    model: str,
    reasoning_effort: str | None,
    speed: str | None,
    single_cycle_bounds: Mapping[str, Any] | None,
    worktree_path: Path | str,
    transport_digest: str,
) -> dict[str, Any]:
    """Build the canonical configuration snapshot used by every receipt.

    Preflight and the launch adapter must hash the same values. Keeping this
    shape in the attestation module prevents a controller from authorizing a
    partial worker configuration while the adapter hashes a different subset.
    """
    return {
        "agent": str(agent),
        "model": str(model),
        "reasoning_effort": reasoning_effort,
        "speed": speed,
        "single_cycle_bounds": _capability_cycle_bounds(single_cycle_bounds),
        "worktree_path": Path(worktree_path).expanduser().resolve(),
        "transport_digest": str(transport_digest),
    }


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _lane_store_key(lane_id: Any = None, worktree_path: Any = None) -> str:
    """Stable per-lane map key: lane_id, else resolved worktree path."""
    if isinstance(lane_id, str) and lane_id.strip():
        return lane_id.strip()
    if worktree_path is not None and str(worktree_path).strip():
        return str(Path(worktree_path).expanduser().resolve())
    return ""


def _caller_lane_key(identity: Mapping[str, Any]) -> str | None:
    """Return a store key only when the caller named a lane or worktree."""
    lane_id = identity.get("lane_id")
    if isinstance(lane_id, str) and lane_id.strip():
        return lane_id.strip()
    worktree = identity.get("worktree_path")
    if worktree is not None and str(worktree).strip():
        return str(Path(worktree).expanduser().resolve())
    return None


def _strip_envelope(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key not in _ENVELOPE_KEYS}


def _parse_attestation_document(
    raw: Mapping[str, Any],
) -> tuple[int | None, dict[str, dict[str, Any]] | None, str | None]:
    """Return ``(schema_version, lanes, error_reason)`` for a durable attestation file.

    A legacy single-record file is treated as the record of its own ``lane_id``
    (or worktree path). Unknown schema versions fail loudly.
    """
    if not raw:
        return ATTESTATION_SCHEMA_VERSION, {}, None
    version = raw.get("schema_version")
    lanes_value = raw.get("lanes")
    if version is None and not isinstance(lanes_value, dict):
        record = _strip_envelope(raw)
        key = _lane_store_key(record.get("lane_id"), record.get("worktree_path"))
        return 1, {key: record}, None
    if version != ATTESTATION_SCHEMA_VERSION:
        return None, None, UNSUPPORTED_ATTESTATION_SCHEMA_REASON
    if not isinstance(lanes_value, dict):
        return None, None, UNSUPPORTED_ATTESTATION_SCHEMA_REASON
    lanes: dict[str, dict[str, Any]] = {}
    for key, record in lanes_value.items():
        if not isinstance(key, str):
            return None, None, UNSUPPORTED_ATTESTATION_SCHEMA_REASON
        if not isinstance(record, dict):
            return None, None, CAPABILITY_INCOMPLETE_REASON
        lanes[key] = _strip_envelope(record)
    return ATTESTATION_SCHEMA_VERSION, lanes, None


def _predecessor_ids(record: Mapping[str, Any]) -> list[str]:
    """Ordered same-pass predecessor ids, oldest first.

    ``predecessor_receipt_ids`` is the bounded chain. The scalar
    ``predecessor_receipt_id`` is the most recent id and is appended when an
    old record has not yet grown the list.
    """
    ids: list[str] = []
    seen: set[str] = set()
    raw_list = record.get("predecessor_receipt_ids")
    if isinstance(raw_list, list):
        for item in raw_list:
            if isinstance(item, str) and item.strip() and item not in seen:
                ids.append(item)
                seen.add(item)
    scalar = record.get("predecessor_receipt_id")
    if isinstance(scalar, str) and scalar.strip() and scalar not in seen:
        ids.append(scalar)
        seen.add(scalar)
    return ids


def _receipt_matches_record(record: Mapping[str, Any], receipt_id: str) -> bool:
    current = record.get("capability_receipt_id")
    return receipt_id == current or receipt_id in _predecessor_ids(record)


def _same_pass_identity(existing: Mapping[str, Any], record: Mapping[str, Any]) -> bool:
    """Keep the chain only while ``pass_id`` is unset or unchanged.

    Binding ``pass_id`` onto an unbound mint is the same pass. A different
    ``pass_id`` is a new pass and must refuse leftover carriers. Dispatch-id
    retries stay on the same pass (REL0916-H-02).
    """
    existing_pass = existing.get("pass_id")
    new_pass = record.get("pass_id")
    if (
        isinstance(existing_pass, str)
        and existing_pass.strip()
        and isinstance(new_pass, str)
        and new_pass.strip()
        and existing_pass != new_pass
    ):
        return False
    return True


def _remint_predecessors(
    existing: Mapping[str, Any],
    record: Mapping[str, Any],
    extra_predecessor: str | None = None,
) -> list[str]:
    """Return the bounded predecessor chain for a same-pass remint.

    Unobserved mint/bind/probe-completion remints accumulate so every in-flight
    carrier stays valid. A remint of an already-completed receipt keeps only
    the previous current id (REL0916-H-02 retry). A new ``pass_id`` resets.
    ``extra_predecessor`` is the caller receipt that just went stale: it must
    stay on the chain so dispatch can name the predecessor it replaced.
    """
    if existing and not _same_pass_identity(existing, record):
        return []
    ids: list[str] = []
    existing_receipt = existing.get("capability_receipt_id") if existing else None
    if isinstance(existing_receipt, str) and existing_receipt.strip():
        if existing.get("probe_timeout") is not None:
            ids = [existing_receipt]
        else:
            ids = _predecessor_ids(existing)
            if existing_receipt not in ids:
                ids.append(existing_receipt)
    if isinstance(extra_predecessor, str) and extra_predecessor.strip() and extra_predecessor not in ids:
        ids.append(extra_predecessor)
    if len(ids) > PREDECESSOR_RECEIPT_CAP:
        ids = ids[-PREDECESSOR_RECEIPT_CAP:]
    return ids


def _record_for_receipt(
    lanes: Mapping[str, Mapping[str, Any]],
    receipt_id: str,
) -> dict[str, Any] | None:
    """Find the unique lane record whose current or predecessor id matches."""
    matched: dict[str, Any] | None = None
    for record in lanes.values():
        if not isinstance(record, dict):
            continue
        if _receipt_matches_record(record, receipt_id):
            if matched is not None:
                return None
            matched = dict(record)
    return matched


def _write_attestation_document(path: Path, lanes: Mapping[str, Mapping[str, Any]]) -> None:
    document = {
        "schema_version": ATTESTATION_SCHEMA_VERSION,
        "lanes": {key: _strip_envelope(record) for key, record in lanes.items()},
    }
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _receipt_id(record: Mapping[str, Any]) -> str:
    """Bind all persisted evidence, including provenance and terminal outcomes."""
    evidence = {
        key: value for key, value in record.items() if key != "capability_receipt_id" and key not in _ENVELOPE_KEYS
    }
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _expected_identity(
    expected_identity: Mapping[str, Any] | None,
    *,
    expected: Mapping[str, Any] | None = None,
    dispatch_identity: Mapping[str, Any] | None = None,
    lane_id: str | None = None,
    pass_id: str | None = None,
    dispatch_id: str | None = None,
    gate_host: str | None = None,
    codex_version: str | None = None,
    sandbox_flags: Sequence[str] | None = None,
    worktree_path: Path | str | None = None,
    config_digest: str | None = None,
    transport_digest: str | None = None,
) -> dict[str, Any]:
    """Normalize public identity spellings into receipt field names."""
    result: dict[str, Any] = {}
    for source in (expected_identity, expected, dispatch_identity):
        if isinstance(source, Mapping):
            result.update(source)
    aliases = {
        "worktree": "worktree_path",
        "worktree_digest": "config_digest",
        "transport_sha256": "transport_digest",
        "sandbox_flags_hash": "sandbox_flags_hash",
    }
    result = {aliases.get(str(key), str(key)): value for key, value in result.items()}
    explicit = {
        "lane_id": lane_id,
        "pass_id": pass_id,
        "dispatch_id": dispatch_id,
        "gate_host": gate_host,
        "codex_version": codex_version,
        "sandbox_flags": sandbox_flags,
        "worktree_path": worktree_path,
        "config_digest": config_digest,
        "transport_digest": transport_digest,
    }
    result.update({key: value for key, value in explicit.items() if value is not None})
    if "sandbox_flags" in result and result["sandbox_flags"] is not None:
        flags = _normalise_flags(result["sandbox_flags"])
        result["sandbox_flags"] = flags
        result["sandbox_flags_hash"] = _identity("", "", flags)["sandbox_flags_hash"]
    if "worktree_path" in result and result["worktree_path"] is not None:
        result["worktree_path"] = str(Path(result["worktree_path"]).expanduser().resolve())
    return result


def build_capability_identity(
    *,
    expected_identity: Mapping[str, Any] | None = None,
    expected: Mapping[str, Any] | None = None,
    dispatch_identity: Mapping[str, Any] | None = None,
    lane_id: str | None = None,
    pass_id: str | None = None,
    dispatch_id: str | None = None,
    gate_host: str | None = None,
    codex_version: str | None = None,
    sandbox_flags: Sequence[str] | None = None,
    worktree_path: Path | str | None = None,
    config_digest: str | None = None,
    transport_digest: str | None = None,
) -> dict[str, Any]:
    """Return the one normalized dispatch identity used for authorization."""
    return _expected_identity(
        expected_identity,
        expected=expected,
        dispatch_identity=dispatch_identity,
        lane_id=lane_id,
        pass_id=pass_id,
        dispatch_id=dispatch_id,
        gate_host=gate_host,
        codex_version=codex_version,
        sandbox_flags=sandbox_flags,
        worktree_path=worktree_path,
        config_digest=config_digest,
        transport_digest=transport_digest,
    )


def missing_capability_identity_fields(identity: Mapping[str, Any]) -> list[str]:
    """List required mutating-dispatch identity dimensions absent from *identity*."""
    missing: list[str] = []
    for field in CAPABILITY_REQUIRED_IDENTITY_FIELDS:
        value = identity.get(field)
        if field == "sandbox_flags":
            if not isinstance(value, Sequence) or isinstance(value, str):
                missing.append(field)
            continue
        if not isinstance(value, str) or not value.strip():
            missing.append(field)
    return missing


def _identity_mismatches(record: Mapping[str, Any], expected: Mapping[str, Any]) -> list[str]:
    mismatches: list[str] = []
    for key, value in expected.items():
        if value is None:
            continue
        if key == "sandbox_flags":
            actual_flags = record.get("sandbox_flags")
            if isinstance(actual_flags, Sequence) and not isinstance(actual_flags, str):
                if _normalise_flags(actual_flags) != list(value):
                    mismatches.append(key)
            elif record.get("sandbox_flags_hash") != expected.get("sandbox_flags_hash"):
                mismatches.append(key)
            continue
        actual = record.get(key)
        if key == "worktree_path" and actual is not None:
            actual = str(Path(actual).expanduser().resolve())
        if actual != value:
            mismatches.append(key)
    return mismatches


def _gate_failure_reason(gate: Any, *, timestamp: float, ttl: float) -> str | None:
    """Classify one required capability gate without treating absence as pass."""
    if gate is None:
        return CAPABILITY_MISSING_REASON
    if not isinstance(gate, dict):
        return CAPABILITY_INCOMPLETE_REASON
    # ``_normalise_gate`` intentionally defaults a missing verdict for old
    # readers. Authorization is stricter: an old record can still be read, but
    # it cannot silently become a capability grant at a mutating boundary.
    if "verdict" not in gate:
        return CAPABILITY_MISSING_REASON
    verdict = gate["verdict"]
    if verdict == "negative":
        return CAPABILITY_NEGATIVE_REASON
    if verdict == "unknown":
        return CAPABILITY_UNKNOWN_REASON
    if verdict != "positive":
        return CAPABILITY_INCOMPLETE_REASON
    if gate.get("source") is None or gate.get("attested_at") is None:
        return CAPABILITY_INCOMPLETE_REASON
    # New records carry the observed boolean as well as the verdict. Keep
    # readers compatible with old records that do not have it, but never let
    # an internally contradictory positive record authorize a turn.
    if "passed" in gate and gate["passed"] is not True:
        return CAPABILITY_INCOMPLETE_REASON
    if _normalise_gate(gate, now=timestamp, ttl=ttl) is None:
        return "receipt_expired"
    return None


def _transport_failure_reason(
    record: Mapping[str, Any],
    *,
    timestamp: float,
    ttl: float,
    require_complete_capability: bool = True,
) -> str | None:
    digest = record.get("transport_digest")
    if not isinstance(digest, str) or not digest.strip():
        return TRANSPORT_MISSING_REASON
    if len(digest) != 64 or any(character not in "0123456789abcdefABCDEF" for character in digest):
        return CAPABILITY_INCOMPLETE_REASON
    if "transport_verdict" not in record:
        return CAPABILITY_MISSING_REASON
    verdict = record.get("transport_verdict")
    if not isinstance(verdict, str):
        return CAPABILITY_INCOMPLETE_REASON
    if verdict == "negative":
        return CAPABILITY_NEGATIVE_REASON
    if verdict == "unknown":
        return CAPABILITY_UNKNOWN_REASON
    if verdict != "positive":
        return CAPABILITY_INCOMPLETE_REASON
    if not require_complete_capability and record.get("probe_timeout") is None:
        # Freshly minted receipts persist unobserved probe fields (ecd32aecf9).
        # Pre-spawn validation treats that as not-yet-probed; spawn-complete
        # validation still requires a live probe to fill them in.
        return None
    if not isinstance(record.get("probe_timeout"), bool):
        return CAPABILITY_INCOMPLETE_REASON
    if record["probe_timeout"]:
        return CAPABILITY_UNKNOWN_REASON
    cleanup = record.get("probe_cleanup_outcome")
    if cleanup in (None, "unknown"):
        return CAPABILITY_UNKNOWN_REASON
    if cleanup not in {"complete", "completed", "success"}:
        return CAPABILITY_INCOMPLETE_REASON
    return None


def validate_capability_receipt(
    root: Path,
    capability_receipt_id: str | None,
    *,
    expected_identity: Mapping[str, Any] | None = None,
    expected: Mapping[str, Any] | None = None,
    dispatch_identity: Mapping[str, Any] | None = None,
    lane_id: str | None = None,
    pass_id: str | None = None,
    dispatch_id: str | None = None,
    gate_host: str | None = None,
    codex_version: str | None = None,
    sandbox_flags: Sequence[str] | None = None,
    worktree_path: Path | str | None = None,
    config_digest: str | None = None,
    transport_digest: str | None = None,
    require_complete_identity: bool | None = None,
    require_complete_capability: bool = True,
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Authorize a dispatch against one immutable, identity-bound receipt.

    The explicit id is mandatory: a missing id never resolves to whichever
    record happens to be current in the shared linked-worktree cache. Lookup
    is per-lane: a named ``lane_id`` (or worktree) that has no record fails as
    ``lane_attestation_missing`` and never falls back to a peer lane's current
    receipt.  The required write gates and transport evidence are authorization
    inputs, not advisory telemetry; legacy records remain readable through
    ``read_attestation`` but cannot authorize a mutating remote attempt when
    incomplete.

    ``require_complete_capability`` (default True) is the spawn-complete check:
    positive write gates and observed probe fields. Pre-spawn callers pass
    False so a freshly minted receipt (UNKNOWN gates, ``probe_timeout=None``)
    can start a worker; the in-execute live probe must then complete it.
    A same-pass remint may replace the receipt id; prior ids from that pass
    stay in ``predecessor_receipt_ids`` (``predecessor_receipt_id`` is the most
    recent, for old readers) so in-flight mint/bind/probe carriers still match.
    """
    if not isinstance(capability_receipt_id, str) or not capability_receipt_id.strip():
        return {"ok": False, "reason": "receipt_missing"}
    path = Path(root) / STATE_FILE
    if not path.exists():
        return {"ok": False, "reason": "receipt_missing"}
    identity = build_capability_identity(
        expected_identity=expected_identity,
        expected=expected,
        dispatch_identity=dispatch_identity,
        lane_id=lane_id,
        pass_id=pass_id,
        dispatch_id=dispatch_id,
        gate_host=gate_host,
        codex_version=codex_version,
        sandbox_flags=sandbox_flags,
        worktree_path=worktree_path,
        config_digest=config_digest,
        transport_digest=transport_digest,
    )
    with bounded_file_lock(path):
        _version, lanes, error = _parse_attestation_document(_read(path))
        if error is not None or lanes is None:
            return {"ok": False, "reason": error or UNSUPPORTED_ATTESTATION_SCHEMA_REASON}
        named_lane = identity.get("lane_id")
        named_lane = named_lane.strip() if isinstance(named_lane, str) else ""
        if named_lane:
            record = lanes.get(named_lane)
            if not isinstance(record, dict) or not record:
                return {"ok": False, "reason": LANE_ATTESTATION_MISSING_REASON}
        else:
            worktree_key = _caller_lane_key(identity)
            record = lanes.get(worktree_key) if worktree_key else None
            if not isinstance(record, dict) or not record:
                record = _record_for_receipt(lanes, capability_receipt_id)
            if record is None and len(lanes) == 1:
                record = next(iter(lanes.values()))
            if not isinstance(record, dict) or not record:
                return {
                    "ok": False,
                    "reason": STALE_CAPABILITY_RECEIPT_REASON if lanes else "receipt_missing",
                }
        record = _strip_envelope(record)
        current = record.get("capability_receipt_id")
        if not isinstance(current, str) or not current:
            return {"ok": False, "reason": "receipt_missing"}
        if current != _receipt_id(record):
            return {"ok": False, "reason": STALE_CAPABILITY_RECEIPT_REASON}
        if not _receipt_matches_record(record, capability_receipt_id):
            return {"ok": False, "reason": STALE_CAPABILITY_RECEIPT_REASON}
        if require_complete_identity is None:
            require_complete_identity = bool(identity)
        if require_complete_identity:
            missing = missing_capability_identity_fields(identity)
            if missing:
                return {
                    "ok": False,
                    "reason": RECEIPT_IDENTITY_INCOMPLETE_REASON,
                    "missing_fields": missing,
                }
        timestamp = now()
        ttl = _ttl(env)
        gates = record.get("gates")
        if not isinstance(gates, dict):
            return {"ok": False, "reason": CAPABILITY_INCOMPLETE_REASON}
        gate_reasons = [
            _gate_failure_reason(gates.get(name), timestamp=timestamp, ttl=ttl)
            for name in ("workspace_write", "writable_roots")
        ]
        blocking_reasons = [
            CAPABILITY_NEGATIVE_REASON,
            "receipt_expired",
            CAPABILITY_MISSING_REASON,
        ]
        if require_complete_capability:
            blocking_reasons.extend((CAPABILITY_UNKNOWN_REASON, CAPABILITY_INCOMPLETE_REASON))
        for reason in blocking_reasons:
            if reason in gate_reasons:
                return {"ok": False, "reason": reason}
        transport_reason = _transport_failure_reason(
            record,
            timestamp=timestamp,
            ttl=ttl,
            require_complete_capability=require_complete_capability,
        )
        if transport_reason is not None:
            return {"ok": False, "reason": transport_reason}
        mismatches = _identity_mismatches(record, identity)
        if mismatches:
            # A transport skew invalidates the content-addressed executable
            # itself. It takes precedence when a receipt also carries the
            # corresponding config digest, because the config digest embeds
            # transport bytes and would otherwise mislabel the root cause as a
            # generic worker-configuration change.
            if "transport_digest" in mismatches:
                reason = TRANSPORT_VERSION_SKEW_REASON
            elif "config_digest" in mismatches:
                reason = STALE_WORKER_CONFIGURATION_REASON
            else:
                reason = CAPABILITY_IDENTITY_MISMATCH_REASON
            return {"ok": False, "reason": reason, "mismatched_fields": mismatches}
        return {"ok": True, "capability_receipt_id": current}


def _verdict_records_enabled(env: Mapping[str, str] | None) -> bool:
    environ = os.environ if env is None else env
    return environ.get(VERDICT_RECORDS_ENV) == "1"


def _normalise_gate(
    gate: Any,
    *,
    now: float,
    ttl: float,
    preserve_future: bool = False,
) -> dict[str, Any] | None:
    if not isinstance(gate, dict):
        return None
    source = gate.get("source")
    if not isinstance(source, str) or source not in _STRENGTH:
        return None
    verdict = gate.get("verdict", "positive")
    if not isinstance(verdict, str) or verdict not in _VERDICTS:
        return None
    timestamp = gate.get("attested_at")
    if type(timestamp) not in (int, float):
        return None
    try:
        if not math.isfinite(timestamp):
            return None
    except OverflowError:
        return None
    try:
        age = now - timestamp
    except OverflowError:
        return None
    try:
        finite_age = math.isfinite(age)
    except OverflowError:
        finite_age = False
    if not finite_age or age >= ttl or (age < 0 and not (preserve_future or verdict == "negative")):
        return None
    normalised = dict(gate)
    normalised["verdict"] = verdict
    normalised["stale"] = age > ttl * _REFRESH_AHEAD_FRACTION
    if age < 0:
        # A future refusal is retained so a wall-clock rollback cannot erase a
        # newer deny record. Future positive evidence is never exposed by the
        # normal read path; preserving it here is only for chronology-safe
        # merges below.
        normalised["future_dated"] = True
    return normalised


def _valid_gates(
    record: dict[str, Any],
    identity: dict[str, str],
    now: float,
    ttl: float,
    *,
    preserve_future: bool = False,
) -> dict[str, Any]:
    if not identity["gate_host"]:
        return {}
    if any(record.get(key) != value for key, value in identity.items()):
        return {}
    gates = record.get("gates")
    if not isinstance(gates, dict):
        return {}
    valid = {}
    for name in ("workspace_write", "writable_roots"):
        gate = _normalise_gate(gates.get(name), now=now, ttl=ttl, preserve_future=preserve_future)
        if gate is not None:
            valid[name] = gate
    return valid


def _compatibility_projection(gates: Mapping[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Expose only the legacy positive gate shape to pre-verdict readers."""
    return {
        name: {"source": gate["source"], "attested_at": gate["attested_at"]}
        for name, gate in gates.items()
        if gate.get("verdict") == "positive"
    }


_VERDICT_DENY_RANK = {"negative": 2, "unknown": 1, "positive": 0}


def _select_gate(
    candidates: Sequence[tuple[str, Mapping[str, Any]]],
) -> dict[str, Any] | None:
    """Set reduction (REF-26): group by verdict; representative is max
    (strength, attested_at, lane key, source). Winning group is newest member
    attested_at, then negative > unknown > positive (SECD-05). Depends only
    on the candidate set, never on iteration order.
    """
    groups: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for key, gate in candidates:
        groups.setdefault(str(gate["verdict"]), []).append((str(key), gate))
    if not groups:
        return None

    def group_rank(verdict: str) -> tuple[Any, int]:
        newest = max(item[1]["attested_at"] for item in groups[verdict])
        return (newest, _VERDICT_DENY_RANK[verdict])

    winning = max(groups, key=group_rank)
    _key, winner = max(
        groups[winning],
        key=lambda item: (
            _STRENGTH[item[1]["source"]],
            item[1]["attested_at"],
            item[0],
            item[1]["source"],
        ),
    )
    return dict(winner)


def read_attestation(
    root: Path,
    *,
    gate_host: str,
    codex_version: str,
    sandbox_flags: Sequence[str],
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Return unexpired identity-matched evidence, with a legacy projection by default.

    Cross-lane merge is a set reduction (newest competing-verdict group, then
    that group's strongest representative) so the result does not depend on
    ``lanes`` iteration. A merged positive is shared-host evidence, not
    authorization for a specific dispatch; mutating callers must still require
    this dispatch's own complete receipt.
    """
    path = Path(root) / STATE_FILE
    if not path.exists():
        # A read must not materialise state. ``bounded_file_lock`` mkdirs the
        # lock's parent, so taking the lock on a cold cache creates
        # ``.task-state/`` inside the caller's worktree -- a checkout that does
        # not yet ignore it then reads dirty, and a dirty lane worktree is
        # exactly what the dispatch guards refuse. No file means no evidence,
        # and there is nothing to serialise against; a writer racing in after
        # this check yields a miss, which is the fail-closed direction.
        return {}
    with bounded_file_lock(path):
        _version, lanes, error = _parse_attestation_document(_read(path))
        if error is not None or not lanes:
            return {}
        identity = _identity(gate_host, codex_version, sandbox_flags)
        timestamp = now()
        ttl = _ttl(env)
        by_gate: dict[str, list[tuple[str, dict[str, Any]]]] = {
            "workspace_write": [],
            "writable_roots": [],
        }
        for lane_key, record in lanes.items():
            if not isinstance(record, dict):
                continue
            gates = _valid_gates(_strip_envelope(record), identity, timestamp, ttl)
            for name, gate in gates.items():
                by_gate[name].append((str(lane_key), gate))
        merged = {
            name: selected for name, candidates in by_gate.items() if (selected := _select_gate(candidates)) is not None
        }
        return merged if _verdict_records_enabled(env) else _compatibility_projection(merged)


def record_attestation(
    root: Path,
    *,
    gate_host: str,
    codex_version: str,
    sandbox_flags: Sequence[str],
    source: str,
    lane_id: str | None = None,
    pass_id: str | None = None,
    dispatch_id: str | None = None,
    worktree_path: Path | str | None = None,
    config_digest: str | None = None,
    workspace_write: bool | None = None,
    writable_roots: bool | None = None,
    verdict: str = "positive",
    transport_digest: str | None = None,
    transport_verdict: str = "unknown",
    probe_timeout: bool | None = None,
    probe_cleanup_outcome: str = "unknown",
    extra_predecessor_receipt_id: str | None = None,
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> str | None:
    """Atomically merge typed evidence into the caller's per-lane record.

    Concurrent lanes share the on-disk file but keep independent current /
    predecessor receipt chains. A legacy single-record file is migrated on
    first write and kept as the record of its own ``lane_id``.
    """
    if source not in _STRENGTH:
        raise ValueError(f"unknown attestation source: {source}")
    if verdict not in _VERDICTS:
        raise ValueError(f"unknown attestation verdict: {verdict}")
    selected = {
        name: passed
        for name, passed in (
            ("workspace_write", workspace_write),
            ("writable_roots", writable_roots),
        )
        if passed is not None and (verdict != "positive" or bool(passed))
    }
    if verdict != "positive" and not selected:
        selected = {"workspace_write": False, "writable_roots": False}
    transport_supplied = (
        transport_digest is not None
        or transport_verdict != "unknown"
        or probe_timeout is not None
        or probe_cleanup_outcome != "unknown"
    )
    # A transport-only refresh must not erase a still-valid write grant. This
    # is how offload preflight can mint a receipt for the exact script before
    # live sandbox evidence arrives. If no prior grant exists, the refresh
    # deliberately creates UNKNOWN gates: it is observable, but cannot
    # authorize a mutating dispatch until positive evidence replaces them.
    transport_only = not selected and transport_supplied
    # A host-side transport refresh may run before a live sandbox has supplied
    # its codex identity. Keep that provenance in the receipt with unknown write
    # gates; validation will not authorize a mutating dispatch until positive
    # workspace-write and writable-roots evidence replaces them.
    if not gate_host and not transport_only:
        return None
    if not selected and not transport_only:
        return None
    path = Path(root) / STATE_FILE
    identity = _identity(gate_host, codex_version, sandbox_flags)
    with bounded_file_lock(path):
        timestamp = now()
        if type(timestamp) not in (int, float):
            return None
        try:
            finite_timestamp = math.isfinite(timestamp)
        except OverflowError:
            finite_timestamp = False
        if not finite_timestamp:
            return None
        _version, lanes, error = _parse_attestation_document(_read(path))
        if error is not None or lanes is None:
            raise ValueError(error or UNSUPPORTED_ATTESTATION_SCHEMA_REASON)
        lane_key = _lane_store_key(lane_id, worktree_path)
        existing = dict(lanes.get(lane_key) or {})
        same_identity = all(existing.get(key) == value for key, value in identity.items())
        # Keep finite future-dated gates for the merge itself. Dropping one
        # before `_select_gate` would let a clock rollback replace a newer
        # refusal with an older positive candidate.
        gates = _valid_gates(existing, identity, timestamp, _ttl(env), preserve_future=True)
        if transport_only and not gates:
            unknown_gate = {
                "source": source,
                "attested_at": timestamp,
                "verdict": "unknown",
                "passed": False,
                "stale": False,
            }
            gates = {
                "workspace_write": dict(unknown_gate),
                "writable_roots": dict(unknown_gate),
            }
        for name in selected:
            candidate = {
                "source": source,
                "attested_at": timestamp,
                "verdict": verdict,
                "passed": bool(verdict == "positive" and selected[name]),
                "stale": False,
            }
            current = gates.get(name)
            chosen = _select_gate(([("", current)] if current is not None else []) + [("write", candidate)])
            if chosen is not None:
                gates[name] = chosen
        if not gates:
            return None
        strongest = max(gates.values(), key=lambda gate: _STRENGTH[gate["source"]])
        record: dict[str, Any] = {**identity, **strongest, "gates": gates}
        if same_identity:
            # Preserve dispatch identity while a later stronger gate refreshes
            # only one capability. This is what makes a receipt replacement
            # observable instead of silently dropping its binding fields.
            for key in ("sandbox_flags", "lane_id", "pass_id", "dispatch_id", "worktree_path", "config_digest"):
                if key in existing:
                    record[key] = existing[key]
        optional_identity = {
            "sandbox_flags": _normalise_flags(sandbox_flags),
            "lane_id": lane_id,
            "pass_id": pass_id,
            "dispatch_id": dispatch_id,
            "worktree_path": (str(Path(worktree_path).expanduser().resolve()) if worktree_path is not None else None),
            "config_digest": config_digest,
        }
        for key, value in optional_identity.items():
            if value is not None:
                record[key] = value
        # An adapter refresh often arrives with only new gate evidence. Keep the
        # previous transport receipt in that case, while allowing a new transport
        # digest/verdict to replace it atomically when supplied.
        transport_values = {
            "transport_digest": transport_digest,
            "transport_verdict": transport_verdict,
            "probe_timeout": probe_timeout,
            "probe_cleanup_outcome": probe_cleanup_outcome,
        }
        if same_identity:
            for key, value in transport_values.items():
                if value in (None, "unknown") and key in existing:
                    transport_values[key] = existing[key]
        record.update(transport_values)
        # Same-pass remints keep every receipt a live carrier may still hold
        # (mint -> dispatch bind -> live-probe completion). A new pass_id
        # resets the chain so an earlier pass cannot authorize.
        predecessors = _remint_predecessors(
            existing,
            record,
            extra_predecessor=extra_predecessor_receipt_id,
        )
        if predecessors:
            record["predecessor_receipt_ids"] = predecessors
            record["predecessor_receipt_id"] = predecessors[-1]
        record["capability_receipt_id"] = _receipt_id(record)
        lanes[lane_key] = record
        _write_attestation_document(path, lanes)
        return record["capability_receipt_id"]


def capability_receipt_record(
    root: Path,
    capability_receipt_id: str | None,
    *,
    lane_id: str | None = None,
) -> dict[str, Any] | None:
    """Return the durable record for a named current or predecessor receipt."""
    if not isinstance(capability_receipt_id, str) or not capability_receipt_id.strip():
        return None
    path = Path(root) / STATE_FILE
    if not path.exists():
        return None
    with bounded_file_lock(path):
        _version, lanes, error = _parse_attestation_document(_read(path))
        if error is not None or not lanes:
            return None
        named = lane_id.strip() if isinstance(lane_id, str) else ""
        if named:
            record = lanes.get(named)
            if isinstance(record, dict) and _receipt_matches_record(record, capability_receipt_id):
                return _strip_envelope(record)
            return None
        return _record_for_receipt(lanes, capability_receipt_id)


def remint_capability_receipt(
    root: Path,
    *,
    predecessor_receipt_id: str,
    gate_host: str,
    sandbox_flags: Sequence[str],
    lane_id: str | None,
    pass_id: str | None,
    dispatch_id: str,
    worktree_path: Path | str | None,
    config_digest: str | None,
    transport_digest: str,
    codex_version: str = "",
    source: str = "operator_file",
    transport_verdict: str = "positive",
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Re-mint a same-pass receipt after ``stale_capability_receipt``.

    This is the same persist path as the original preflight mint. TTL and
    ``_REFRESH_AHEAD_FRACTION`` stay inside ``record_attestation`` /
    ``_normalise_gate``; this helper does not invent a second expiry policy.
    """
    try:
        receipt_id = record_attestation(
            root,
            gate_host=gate_host,
            codex_version=codex_version,
            sandbox_flags=sandbox_flags,
            source=source,
            lane_id=lane_id,
            pass_id=pass_id,
            dispatch_id=dispatch_id,
            worktree_path=worktree_path,
            config_digest=config_digest,
            transport_digest=transport_digest,
            transport_verdict=transport_verdict,
            extra_predecessor_receipt_id=predecessor_receipt_id,
            env=env,
            now=now,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return {"ok": False, "reason": CAPABILITY_RECEIPT_UNAVAILABLE_REASON, "error": str(exc)}
    if not isinstance(receipt_id, str) or not receipt_id:
        return {"ok": False, "reason": CAPABILITY_RECEIPT_UNAVAILABLE_REASON}
    return {
        "ok": True,
        "capability_receipt_id": receipt_id,
        "dispatch_id": dispatch_id,
        "predecessor_receipt_id": predecessor_receipt_id,
        "capability_config_digest": config_digest,
    }
