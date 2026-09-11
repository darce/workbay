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
_REFRESH_AHEAD_FRACTION = 0.8
_VERDICTS = frozenset({"positive", "negative", "unknown"})
_STRENGTH = {"operator_env": 1, "operator_file": 1, "live_probe": 2, "vm_commit": 3}


def attestation_root(worktree: Path) -> Path:
    """Use the primary checkout for linked worktrees, not a per-lane cache."""
    try:
        result = subprocess.run(
            ["git", "-C", str(worktree), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        common = Path(result.stdout.strip())
        if result.returncode == 0 and common.is_absolute() and common.name == ".git":
            return common.parent
    except (OSError, subprocess.TimeoutExpired):
        pass
    return worktree


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


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


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
    if not identity["gate_host"] or not identity["codex_version"]:
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
    workspace_write: bool | None = None,
    writable_roots: bool | None = None,
    verdict: str = "positive",
    env: Mapping[str, str] | None = None,
    now: Callable[[], float] = time.time,
) -> None:
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
    if not gate_host or not codex_version or not selected:
        return
    path = Path(root) / STATE_FILE
    identity = _identity(gate_host, codex_version, sandbox_flags)
    with bounded_file_lock(path):
        timestamp = now()
        if type(timestamp) not in (int, float):
            return
        try:
            finite_timestamp = math.isfinite(timestamp)
        except OverflowError:
            finite_timestamp = False
        if not finite_timestamp:
            return
        # Keep finite future-dated gates for the merge itself. Dropping one
        # before `_candidate_wins` would let a clock rollback replace a newer
        # refusal with an older positive candidate.
        gates = _valid_gates(_read(path), identity, timestamp, _ttl(env), preserve_future=True)
        for name in selected:
            candidate = {"source": source, "attested_at": timestamp, "verdict": verdict, "stale": False}
            if _candidate_wins(gates.get(name), candidate):
                gates[name] = candidate
        if not gates:
            return
        strongest = max(gates.values(), key=lambda gate: _STRENGTH[gate["source"]])
        record = {**identity, **strongest, "gates": gates}
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
