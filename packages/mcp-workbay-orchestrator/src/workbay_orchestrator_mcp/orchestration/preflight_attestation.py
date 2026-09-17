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
        "single_cycle_bounds": dict(single_cycle_bounds or {}),
        "worktree_path": Path(worktree_path).expanduser().resolve(),
        "transport_digest": str(transport_digest),
    }


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _receipt_id(record: Mapping[str, Any]) -> str:
    """Bind all persisted evidence, including provenance and terminal outcomes."""
    evidence = {key: value for key, value in record.items() if key != "capability_receipt_id"}
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
    record happens to be current in the shared linked-worktree cache.  The
    required write gates and transport evidence are authorization inputs, not
    advisory telemetry; legacy records remain readable through ``read_attestation``
    but cannot authorize a mutating remote attempt when incomplete.

    ``require_complete_capability`` (default True) is the spawn-complete check:
    positive write gates and observed probe fields. Pre-spawn callers pass
    False so a freshly minted receipt (UNKNOWN gates, ``probe_timeout=None``)
    can start a worker; the in-execute live probe must then complete it.
    A live-probe refresh may replace the receipt id; the mint id is retained
    as ``predecessor_receipt_id`` so the worker's original carrier still matches.
    """
    if not isinstance(capability_receipt_id, str) or not capability_receipt_id.strip():
        return {"ok": False, "reason": "receipt_missing"}
    path = Path(root) / STATE_FILE
    if not path.exists():
        return {"ok": False, "reason": "receipt_missing"}
    with bounded_file_lock(path):
        record = _read(path)
        current = record.get("capability_receipt_id")
        if not isinstance(current, str) or not current:
            return {"ok": False, "reason": "receipt_missing"}
        if current != _receipt_id(record):
            return {"ok": False, "reason": "stale_capability_receipt"}
        predecessor = record.get("predecessor_receipt_id")
        if capability_receipt_id != current and capability_receipt_id != predecessor:
            return {"ok": False, "reason": "stale_capability_receipt"}
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
            blocking_reasons.extend(
                (CAPABILITY_UNKNOWN_REASON, CAPABILITY_INCOMPLETE_REASON)
            )
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


def _candidate_wins(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> bool:
    """Use chronology for competing verdicts and source strength within one verdict."""
    if existing is None:
        return True
    existing_timestamp = existing["attested_at"]
    candidate_timestamp = candidate["attested_at"]
    if candidate["verdict"] != existing["verdict"] and candidate_timestamp != existing_timestamp:
        return candidate_timestamp > existing_timestamp
    return _STRENGTH[candidate["source"]] >= _STRENGTH[existing["source"]]


def read_attestation(
    root: Path,
    *,
    gate_host: str,
    codex_version: str,
    sandbox_flags: Sequence[str],
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Return unexpired identity-matched evidence, with a legacy projection by default."""
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
        gates = _valid_gates(_read(path), _identity(gate_host, codex_version, sandbox_flags), now(), _ttl(env))
        return gates if _verdict_records_enabled(env) else _compatibility_projection(gates)


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
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> str | None:
    """Atomically merge typed evidence with chronology-first verdict changes."""
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
        existing = _read(path)
        same_identity = all(existing.get(key) == value for key, value in identity.items())
        # Keep finite future-dated gates for the merge itself. Dropping one
        # before `_candidate_wins` would let a clock rollback replace a newer
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
            if _candidate_wins(gates.get(name), candidate):
                gates[name] = candidate
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
            "worktree_path": (
                str(Path(worktree_path).expanduser().resolve()) if worktree_path is not None else None
            ),
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
        if same_identity:
            existing_receipt = existing.get("capability_receipt_id")
            existing_predecessor = existing.get("predecessor_receipt_id")
            existing_probe = existing.get("probe_timeout")
            completing_probe = existing_probe is None and isinstance(
                transport_values.get("probe_timeout"), bool
            )
            # Only a live-probe completion of an unobserved mint keeps the mint
            # id as predecessor. A same-identity replacement of already-complete
            # evidence must still invalidate the previous receipt id.
            if completing_probe and isinstance(existing_receipt, str) and existing_receipt.strip():
                record["predecessor_receipt_id"] = (
                    existing_predecessor
                    if isinstance(existing_predecessor, str) and existing_predecessor.strip()
                    else existing_receipt
                )
            elif isinstance(existing_predecessor, str) and existing_predecessor.strip():
                record["predecessor_receipt_id"] = existing_predecessor
        record["capability_receipt_id"] = _receipt_id(record)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return record["capability_receipt_id"]
