"""Producer-owned measurement provenance for the landing gate.

This module is the out-of-band completion and identity surface. A printable
pytest summary is forensic evidence only; it cannot certify that a measured
run finished [RLSE-05, AGT-04].

Compatibility contract [DATA-03]
    Identity is ``producer_fingerprint``: SHA-256 over ``COMPAT_VERSION`` and
    the bytes of every producer layer actually invoked (``landing_gate.py``,
    this module, and the ``gate_ids.sh`` wrapper that executed). It is *not*
    the git HEAD of the measured worktree and it is not a self-asserted
    token. The local trusted-producer boundary is those files next to the
    consumer process: the verifier recomputes the digest from that exact set
    and rejects a receipt or scope whose fingerprint does not match. A
    selected sibling named ``gate_ids.sh`` is only the default when the
    executed wrapper is unknown; producers must bind the wrapper path that
    actually ran [LANDGA-S1C-M04]. This proves the artifacts were produced by
    the producer code the consumer is executing; it is not PKI and does not
    authenticate a remote party. Baseline and subject HEADs and worktree
    paths may differ; ``LANDING_GATE_SCRIPT`` already allows one producer for
    both measurements. Missing identity is ``scope_unverified``; unequal or
    untrusted fingerprints or compat tokens are ``producer_mismatched``.

Bounded lifetimes [RES-02, RES-06]
    ``run_bounded_pytest`` applies a finite wall-clock bound to the pytest
    process group, kills the tree on expiry, and returns a typed timeout
    outcome. Callers must not publish success-shaped id/scope/receipt artifacts
    for a timed-out or unavailable run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

COMPAT_VERSION = "landing-measurement/v1"
RECEIPT_SCHEMA = "landing_measurement_receipt/v1"
ATTEMPT_SCHEMA = "landing_measurement_attempt/v1"
RECEIPT_COMPLETE = "complete"
BOUNDED_PYTEST_PRODUCER = "run_bounded_pytest"
DEFAULT_TIMEOUT_SEC = 3600
TIMEOUT_EXIT = 5
UNAVAILABLE_EXIT = 4
COMPLETE_REFUSED_EXIT = 2
SIGNAL_EXIT = 6
WRAPPER_LAYER_NAME = "gate_ids.sh"
RUN_MARKER_ENV = "LANDING_GATE_RUN_MARKER"
DESCENDANT_SNAPSHOT_UNAVAILABLE = "descendant_snapshot_unavailable"
PROCESS_TABLE_SNAPSHOT_ATTEMPTS = 3
PROCESS_TABLE_SNAPSHOT_TIMEOUT_SEC = 1


def sha256_file(path: Path | str | None) -> str | None:
    if path is None:
        return None
    candidate = Path(path)
    if not candidate.is_file():
        return None
    digest = hashlib.sha256()
    with candidate.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_file_snapshot(path: Path | str) -> dict[str, Any]:
    """Read bytes once and return path, text, and SHA-256 of those exact bytes."""
    candidate = Path(path)
    data = candidate.read_bytes()
    return {
        "path": candidate,
        "text": data.decode("utf-8", errors="surrogateescape"),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def try_file_snapshot(path: Path | str | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        return read_file_snapshot(path)
    except OSError:
        return None


def producer_fingerprint(*paths: Path | str, layer_names: Sequence[str] | None = None) -> str:
    """Immutable extractor/producer identity. Worktree HEADs are not this value.

    ``layer_names`` pins the digest slot for a relocated executed wrapper so a
    ``/tmp`` snapshot of ``gate_ids.sh`` still occupies the wrapper layer
    rather than hashing an accidental basename [LANDGA-S1C-M04].
    """
    digest = hashlib.sha256()
    digest.update(COMPAT_VERSION.encode("utf-8"))
    for index, raw in enumerate(paths):
        path = Path(raw)
        name = layer_names[index] if layer_names is not None else path.name
        digest.update(b"\0")
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


#: Security-relevant producer layers next to ``landing_gate.py``. Split modules
#: are included so a fingerprint cannot silently omit parser/scope/probe/policy
#: bytes after REVIEW-M-07.
_PRODUCER_SIBLINGS = (
    "landing_measurement.py",
    "gate_ids.sh",
    "landing_gate_parsing.py",
    "landing_gate_scope.py",
    "landing_gate_probe.py",
    "landing_gate_policy.py",
)


def default_producer_paths(
    gate_script: Path | str | None = None,
    *,
    wrapper: Path | str | None = None,
) -> list[Path]:
    """Return the invoked gate script plus every security-relevant producer layer.

    Sibling lookup prefers files next to ``gate_script``. Missing required
    split modules then fall back to this module's directory so a relocated
    wrapper cannot silently drop parser/scope/probe/policy bytes.

    When ``wrapper`` is supplied it occupies the ``gate_ids.sh`` layer: the
    bytes that executed, not a selected sibling of the same name
    [LANDGA-S1C-M04]. A missing wrapper path is fail-closed (the selected
    sibling is not substituted).
    """
    executing_dir = Path(__file__).resolve().parent
    gate = Path(gate_script).resolve() if gate_script is not None else executing_dir / "landing_gate.py"
    paths = [gate]
    seen = {gate.resolve()}
    search_dirs: list[Path] = []
    for directory in (gate.parent, executing_dir):
        resolved_dir = directory.resolve()
        if resolved_dir not in search_dirs:
            search_dirs.append(resolved_dir)
    wrapper_path = Path(wrapper) if wrapper is not None else None
    if wrapper_path is not None and not wrapper_path.is_file():
        raise FileNotFoundError(f"executed producer wrapper is not a file: {wrapper_path}")

    def _add(candidate: Path) -> bool:
        if not candidate.is_file():
            return False
        resolved = candidate.resolve()
        if resolved in seen:
            return True
        paths.append(resolved)
        seen.add(resolved)
        return True

    for name in _PRODUCER_SIBLINGS:
        if name == WRAPPER_LAYER_NAME and wrapper_path is not None:
            _add(wrapper_path)
            continue
        for directory in search_dirs:
            if _add(directory / name):
                break
    _add(Path(__file__).resolve())
    return paths


def _layer_names_for(paths: Sequence[Path], *, wrapper: Path | str | None) -> list[str]:
    wrapper_resolved = Path(wrapper).resolve() if wrapper is not None else None
    names: list[str] = []
    for path in paths:
        if wrapper_resolved is not None and path.resolve() == wrapper_resolved:
            names.append(WRAPPER_LAYER_NAME)
        else:
            names.append(path.name)
    return names


def trusted_producer_fingerprint(
    gate_script: Path | str | None = None,
    *,
    wrapper: Path | str | None = None,
) -> str:
    """Digest of the local trusted producer files. Not a bearer token."""
    paths = default_producer_paths(gate_script, wrapper=wrapper)
    return producer_fingerprint(*paths, layer_names=_layer_names_for(paths, wrapper=wrapper))


def _is_terminal_returncode(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def receipt_path_for_raw(raw_path: Path | str) -> Path:
    return Path(raw_path).with_suffix(".receipt")


def attempt_path_for_raw(raw_path: Path | str) -> Path:
    return Path(raw_path).with_suffix(".attempt")


def write_attempt_marker(
    path: Path | str,
    *,
    run_id: str | None,
    status: str = "incomplete",
    pid: int | None = None,
) -> Path:
    """Publish the current attempt before any later crash can leave stale complete evidence."""
    destination = Path(path)
    payload: dict[str, Any] = {
        "schema": ATTEMPT_SCHEMA,
        "status": status if isinstance(status, str) and status.strip() else "incomplete",
        "run_id": run_id if isinstance(run_id, str) and run_id.strip() else None,
        "pid": pid,
    }
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


def load_attempt(path: Path | str) -> dict[str, Any] | None:
    return load_receipt(path)


def attempt_run_id_from_payload(payload: Mapping[str, Any] | None) -> str | None:
    if not payload:
        return None
    if payload.get("schema") != ATTEMPT_SCHEMA:
        return None
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    return run_id.strip()


def _attempt_payload_from_snapshot(snapshot: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not snapshot:
        return None
    text = snapshot.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _json_payload_from_snapshot(snapshot: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not snapshot:
        return None
    text = snapshot.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _nonempty_path(value: Any) -> bool:
    # Keep text validation strict: argparse supplies path flags as Path objects,
    # while fingerprints, run ids, and other text fields must remain strings.
    if not isinstance(value, (str, os.PathLike)):
        return False
    try:
        path_text = os.fspath(value)
    except TypeError:
        return False
    return isinstance(path_text, str) and bool(path_text.strip())


def _producer_status_is_complete(payload: Mapping[str, Any] | None, raw_hash: str | None) -> bool:
    """Accept completion only from the bounded pytest producer's returned proof."""
    if payload is None or payload.get("producer") != BOUNDED_PYTEST_PRODUCER:
        return False
    if payload.get("outcome") != "returned" or payload.get("timed_out") is not False:
        return False
    if not _is_terminal_returncode(payload.get("returncode")):
        return False
    if payload.get("pytest_attested") is not True:
        return False
    if not _nonempty_text(payload.get("run_token")):
        return False
    status_hash = payload.get("raw_sha256")
    return _nonempty_text(status_hash) and status_hash == raw_hash


def _resolved_manifest_path(value: Any) -> str | None:
    if not _nonempty_path(value):
        return None
    try:
        return str(Path(value).resolve())
    except OSError:
        return None


def write_completion_receipt(
    path: Path | str,
    *,
    status: str,
    pytest_returncode: int | None,
    raw: Path | str | None,
    ids: Path | str | None = None,
    nids: Path | str | None = None,
    scope: Path | str | None = None,
    fingerprint: str,
    timeout_seconds: int,
    error: str | None = None,
    run_id: str | None = None,
    producer_status: Mapping[str, Any] | None = None,
    producer_script: Path | str | None = None,
    producer_wrapper: Path | str | None = None,
    trusted_fingerprint: str | None = None,
) -> Path:
    destination = Path(path)
    raw_hash = sha256_file(raw)
    ids_hash = sha256_file(ids)
    nids_hash = sha256_file(nids) if nids is not None else None
    scope_hash = sha256_file(scope)
    if status == RECEIPT_COMPLETE:
        if not _producer_status_is_complete(producer_status, raw_hash):
            status = "incomplete"
        if not _is_terminal_returncode(pytest_returncode):
            status = "incomplete"
        if not raw_hash or not ids_hash or not nids_hash or not scope_hash:
            status = "incomplete"
        if not _nonempty_path(producer_script) or not _nonempty_path(producer_wrapper):
            status = "incomplete"
        if not _nonempty_text(fingerprint):
            status = "incomplete"
        if not _nonempty_text(run_id):
            status = "incomplete"
        if producer_status is not None and producer_status.get("run_id") not in (None, run_id):
            status = "incomplete"
        if trusted_fingerprint is not None and fingerprint != trusted_fingerprint:
            status = "incomplete"
    if status != RECEIPT_COMPLETE:
        ids_hash = None
        nids_hash = None
        scope_hash = None
    payload: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "status": status,
        "pytest_returncode": pytest_returncode if _is_terminal_returncode(pytest_returncode) else None,
        "raw_sha256": raw_hash,
        "ids_sha256": ids_hash,
        "nids_sha256": nids_hash,
        "scope_sha256": scope_hash,
        "producer_fingerprint": fingerprint,
        "compat": COMPAT_VERSION,
        "producer_compat": COMPAT_VERSION,
        "producer": BOUNDED_PYTEST_PRODUCER,
        "producer_script": _resolved_manifest_path(producer_script),
        "producer_wrapper": _resolved_manifest_path(producer_wrapper),
        "producer_run_token": (
            producer_status.get("run_token")
            if isinstance(producer_status, Mapping) and _nonempty_text(producer_status.get("run_token"))
            else None
        ),
        "timeout_seconds": int(timeout_seconds),
        "run_id": run_id if isinstance(run_id, str) and run_id.strip() else None,
        "artifacts": {
            "raw": str(Path(raw).resolve()) if raw is not None else None,
            "ids": str(Path(ids).resolve()) if ids is not None else None,
            "nids": str(Path(nids).resolve()) if nids is not None else None,
            "scope": str(Path(scope).resolve()) if scope is not None else None,
        },
    }
    if error:
        payload["error"] = error
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


def load_receipt(path: Path | str) -> dict[str, Any] | None:
    candidate = Path(path)
    try:
        text = candidate.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def _current_attempt_bind(
    raw_path: Path,
    receipt_run_id: str,
    *,
    attempt_payload: Mapping[str, Any] | None = None,
    allow_reread: bool = True,
) -> bool:
    """Refuse a complete receipt that is not the currently published attempt.

    A missing attempt marker is fail-closed: a completion receipt without the
    producer's current-attempt publication cannot distinguish a prior success
    from a new crash after lock setup [LANDGA-S1C-H02].
    """
    payload: Mapping[str, Any] | None = attempt_payload
    if payload is None:
        if not allow_reread:
            return False
        marker = attempt_path_for_raw(raw_path)
        if not marker.is_file():
            return False
        payload = load_attempt(marker)
        if payload is None:
            return False
    marker_run = attempt_run_id_from_payload(payload)
    if marker_run is None:
        return False
    return marker_run == receipt_run_id.strip() and payload.get("status") == RECEIPT_COMPLETE


def receipt_authenticates_raw(
    raw_path: Path,
    receipt: Mapping[str, Any] | None,
    *,
    ids_path: Path | str | None = None,
    scope_path: Path | str | None = None,
    trusted_fingerprint: str | None = None,
    raw_sha256: str | None = None,
    ids_sha256: str | None = None,
    scope_sha256: str | None = None,
    allow_reread: bool = True,
    attempt_payload: Mapping[str, Any] | None = None,
) -> bool:
    """Authenticate the exact caller-supplied artifact paths as one manifest.

    Sibling filenames derived from ``raw_path`` are not consulted. A complete
    receipt must attest a returned run, an integer pytest return code, required
    compatibility/producer/run identity, and hashes for raw, IDs, and scope.
    When ``nids_sha256`` is present it is the consumed canonical ID bytes
    (``.nids``), not forensic raw ``.ids`` [LANDGA-S1C-M03]. Callers that
    already hashed an immutable snapshot may pass those digests instead of
    re-reading the files [LANDGA-S1C-M06]. A published current-attempt marker
    must bind the receipt run id [LANDGA-S1C-H02].
    """
    if not receipt:
        return False
    if receipt.get("schema") != RECEIPT_SCHEMA:
        return False
    if receipt.get("status") != RECEIPT_COMPLETE:
        return False
    if not _is_terminal_returncode(receipt.get("pytest_returncode")):
        return False
    if receipt.get("compat") != COMPAT_VERSION or receipt.get("producer_compat") != COMPAT_VERSION:
        return False
    if receipt.get("producer") != BOUNDED_PYTEST_PRODUCER:
        return False
    fingerprint = receipt.get("producer_fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        return False
    producer_script = receipt.get("producer_script")
    producer_wrapper = receipt.get("producer_wrapper")
    if not _nonempty_path(producer_script) or not _nonempty_path(producer_wrapper):
        return False
    if not _nonempty_text(receipt.get("producer_run_token")):
        return False
    try:
        expected = (
            trusted_fingerprint
            if trusted_fingerprint is not None
            else trusted_producer_fingerprint(producer_script, wrapper=producer_wrapper)
        )
    except (OSError, ValueError):
        return False
    if fingerprint.strip() != str(expected).strip():
        return False
    run_id = receipt.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return False
    if not _current_attempt_bind(
        raw_path,
        run_id,
        attempt_payload=attempt_payload,
        allow_reread=allow_reread,
    ):
        return False
    observed_raw = raw_sha256 if raw_sha256 is not None else (sha256_file(raw_path) if allow_reread else None)
    if receipt.get("raw_sha256") != observed_raw:
        return False
    ids_hash = receipt.get("ids_sha256")
    nids_hash = receipt.get("nids_sha256")
    scope_hash = receipt.get("scope_sha256")
    if not isinstance(ids_hash, str) or not ids_hash:
        return False
    if not isinstance(nids_hash, str) or not nids_hash:
        return False
    if not isinstance(scope_hash, str) or not scope_hash:
        return False
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return False
    expected_paths = {
        "raw": str(Path(raw_path).resolve()),
        "nids": _resolved_manifest_path(ids_path),
        "scope": _resolved_manifest_path(scope_path),
    }
    if expected_paths["nids"] is None or expected_paths["scope"] is None:
        return False
    for name, expected_path in expected_paths.items():
        if artifacts.get(name) != expected_path:
            return False
    if not _nonempty_text(artifacts.get("ids")):
        return False
    observed_ids = ids_sha256 if ids_sha256 is not None else (sha256_file(ids_path) if allow_reread else None)
    observed_scope = scope_sha256 if scope_sha256 is not None else (sha256_file(scope_path) if allow_reread else None)
    if ids_path is None or observed_ids != nids_hash:
        return False
    if scope_path is None or observed_scope != scope_hash:
        return False
    return True


def measurement_completeness(
    raw_logs: Mapping[str, Path | None],
    *,
    ids_logs: Mapping[str, Path | None] | None = None,
    scope_logs: Mapping[str, Path | None] | None = None,
    error_cls: type[Exception] = RuntimeError,
    artifact_snapshots: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Completeness requires a producer-owned receipt bound to the supplied artifacts.

    A terminal pytest summary, wrapped or bare, is not enough. IDs and scope
    are authenticated at the caller-supplied paths, never as inferred siblings.
    When ``artifact_snapshots`` is supplied, hashes come from that immutable
    read rather than a second open of the same paths [LANDGA-S1C-M06].
    """
    ids_logs = ids_logs or {}
    scope_logs = scope_logs or {}
    provided = {label: path for label, path in sorted(raw_logs.items()) if path is not None}
    if not provided:
        return {"outcome": "not_measured", "incomplete": [], "checked": []}
    incomplete: list[str] = []
    for label, path in sorted(raw_logs.items()):
        if path is None:
            incomplete.append(label)
            continue
        supplied = artifact_snapshots.get(label) if artifact_snapshots else None
        if supplied is not None:
            raw_snap = supplied.get("raw")
            if not isinstance(raw_snap, dict) or not raw_snap.get("sha256"):
                raise error_cls(f"could not read the {label} pytest log at {path}: snapshot missing")
            ids_snap = supplied.get("ids") if isinstance(supplied.get("ids"), dict) else None
            scope_snap = supplied.get("scope") if isinstance(supplied.get("scope"), dict) else None
            attempt_snap = supplied.get("attempt") if isinstance(supplied.get("attempt"), dict) else None
            receipt_snap = supplied.get("receipt") if isinstance(supplied.get("receipt"), dict) else None
            raw_hash = raw_snap.get("sha256")
            ids_hash = ids_snap.get("sha256") if ids_snap else None
            scope_hash = scope_snap.get("sha256") if scope_snap else None
        else:
            try:
                raw_snap = read_file_snapshot(path)
            except OSError as exc:
                raise error_cls(f"could not read the {label} pytest log at {path}: {exc}") from exc
            ids_snap = try_file_snapshot(ids_logs.get(label))
            scope_snap = try_file_snapshot(scope_logs.get(label))
            attempt_snap = try_file_snapshot(attempt_path_for_raw(path))
            receipt_snap = try_file_snapshot(receipt_path_for_raw(path))
            raw_hash = raw_snap["sha256"]
            ids_hash = ids_snap["sha256"] if ids_snap else None
            scope_hash = scope_snap["sha256"] if scope_snap else None
        receipt = _json_payload_from_snapshot(receipt_snap)
        attempt_payload = _attempt_payload_from_snapshot(attempt_snap if isinstance(attempt_snap, dict) else None)
        if receipt_snap is None or receipt is None or attempt_snap is None or attempt_payload is None:
            incomplete.append(label)
            continue
        if not receipt_authenticates_raw(
            Path(path),
            receipt,
            ids_path=ids_logs.get(label),
            scope_path=scope_logs.get(label),
            raw_sha256=raw_hash if isinstance(raw_hash, str) else None,
            ids_sha256=ids_hash if isinstance(ids_hash, str) else None,
            scope_sha256=scope_hash if isinstance(scope_hash, str) else None,
            allow_reread=False,
            attempt_payload=attempt_payload,
        ):
            incomplete.append(label)
    return {
        "outcome": "incomplete" if incomplete else "complete",
        "incomplete": incomplete,
        "checked": sorted(str(item) for item in provided),
    }


def _processes_with_marker(marker: str) -> set[int]:
    """Find only descendants carrying this run's inherited environment marker."""
    needle = f"{RUN_MARKER_ENV}={marker}".encode()
    found: set[int] = set()
    proc_root = Path("/proc")
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return found
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            if needle in (entry / "environ").read_bytes().split(b"\0"):
                found.add(int(entry.name))
        except (OSError, ValueError):
            continue
    return found


def _descendants_from_process_table(
    root_pid: int,
    *,
    signals: list[str] | None = None,
    attempts: int = PROCESS_TABLE_SNAPSHOT_ATTEMPTS,
) -> set[int]:
    """Return the transitive descendants from a bounded portable ``ps`` snapshot."""
    # The pre-kill snapshot is the only reading that can retain escaped
    # descendants, so retry a bounded number of times on a busy host. Each
    # attempt still has its own finite timeout; a degraded snapshot must not
    # turn the reaper's timeout into a hang.
    for _ in range(attempts):
        try:
            snapshot = subprocess.run(
                ["ps", "-A", "-o", "pid=,ppid="],
                capture_output=True,
                text=True,
                check=False,
                timeout=PROCESS_TABLE_SNAPSHOT_TIMEOUT_SEC,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if snapshot.returncode != 0 or not isinstance(snapshot.stdout, str):
            continue

        children: dict[int, set[int]] = {}
        for line in snapshot.stdout.splitlines():
            fields = line.split()
            if len(fields) != 2:
                continue
            try:
                pid, ppid = (int(field) for field in fields)
            except ValueError:
                continue
            if pid <= 0 or ppid < 0:
                continue
            children.setdefault(ppid, set()).add(pid)

        descendants: set[int] = set()
        pending = [root_pid]
        current_pid = os.getpid()
        while pending:
            parent = pending.pop()
            for child in children.get(parent, set()):
                if child in descendants or child == root_pid or child == current_pid:
                    continue
                descendants.add(child)
                pending.append(child)
        return descendants

    if signals is not None and DESCENDANT_SNAPSHOT_UNAVAILABLE not in signals:
        signals.append(DESCENDANT_SNAPSHOT_UNAVAILABLE)
    return set()


def _kill_process_group(proc: subprocess.Popen[Any], *, marker: str | None = None) -> list[str]:
    signals: list[str] = []
    fallback_enabled = False
    fallback_snapshot: set[int] = set()
    fallback_pending: set[int] = set()
    fallback_signalled: set[int] = set()
    # The fallback retains IDs only for this bounded 60-iteration (~3-second)
    # cleanup window. Once SIGKILL succeeds or the target is gone, it is marked
    # handled and never signalled again, so a PID reused within that window is
    # not re-killed; every pending ID came from a descendant snapshot.
    if marker is not None:
        # A non-empty marker scan is the precise Linux fast path. An empty scan
        # means either that /proc is unavailable or that this run has no
        # marker-visible target, so the process-table fallback is safe and has
        # the same descendant-only outcome.
        fallback_enabled = not _processes_with_marker(marker)
        if fallback_enabled:
            # This must precede killpg: detached grandchildren become children
            # of pid 1 as soon as their direct parent is killed.
            # Retain this initial closure because after killpg the direct child
            # is dead and ps no longer has a parent chain back to proc.pid; a
            # later fork by an already-detached descendant is therefore a known
            # limit of ps-based tracking without /proc, not an oversight.
            fallback_snapshot = _descendants_from_process_table(proc.pid, signals=signals)
            fallback_pending.update(fallback_snapshot)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except OSError:
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
    if marker is None:
        return signals
    # A child can call setsid(2) and escape the process group. Repeatedly scan
    # for this run's private marker until every matching process is gone; the
    # marker prevents collateral kills of unrelated work [LANDGA-L0BR-M03].
    for _ in range(60):
        pids = _processes_with_marker(marker)
        if fallback_enabled:
            # Follow-up scans retain the original one-attempt bound; only the
            # irreplaceable pre-kill closure receives the bounded retry budget.
            fresh_snapshot = _descendants_from_process_table(proc.pid, signals=signals, attempts=1)
            fallback_snapshot.update(fresh_snapshot)
            fallback_pending.update(pid for pid in fresh_snapshot if pid not in fallback_signalled)
            pids.update(fallback_pending)
        if not pids:
            return signals
        for pid in pids:
            if pid == os.getpid():
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                if fallback_enabled and pid in fallback_pending:
                    fallback_pending.discard(pid)
                    fallback_signalled.add(pid)
                continue
            except (PermissionError, OSError):
                continue
            if fallback_enabled and pid in fallback_pending:
                fallback_pending.discard(pid)
                fallback_signalled.add(pid)
        time.sleep(0.05)
    return signals


def _bounded_pytest_status(
    *,
    outcome: str,
    timed_out: bool,
    returncode: int | None,
    error: str | None,
    raw: Path,
    pytest_attested: bool,
    run_id: str | None,
    run_token: str,
    argv_sha256: str,
    signals: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "timed_out": timed_out,
        "returncode": returncode,
        "error": error,
        "raw_sha256": sha256_file(raw),
        "producer": BOUNDED_PYTEST_PRODUCER,
        "pytest_attested": pytest_attested,
        "run_id": run_id,
        "run_token": run_token,
        "argv_sha256": argv_sha256,
        "signals": list(signals),
    }


def _pytest_command_matches_current_interpreter(argv: Sequence[str]) -> bool:
    if len(argv) < 3 or str(argv[1]) != "-m" or str(argv[2]) != "pytest":
        return False
    try:
        return Path(str(argv[0])).resolve() == Path(os.path.realpath(__import__("sys").executable)).resolve()
    except OSError:
        return False


def _pytest_runtime_available(
    argv: Sequence[str], *, cwd: Path | str | None, env: Mapping[str, str], timeout_seconds: int
) -> tuple[bool, str | None]:
    """Prove the selected interpreter can import the pytest it will execute."""
    try:
        probe = subprocess.run(
            [str(argv[0]), "-c", "import pytest; print('landing-gate-pytest-runtime-ok')"],
            cwd=str(cwd) if cwd is not None else None,
            env=dict(env),
            capture_output=True,
            text=True,
            check=False,
            timeout=min(10, max(1, timeout_seconds)),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"pytest runtime probe failed: {type(exc).__name__}: {exc}"
    if probe.returncode != 0 or "landing-gate-pytest-runtime-ok" not in probe.stdout:
        detail = (probe.stderr or probe.stdout).strip()[:400]
        return False, f"pytest runtime probe exited {probe.returncode}: {detail}"
    return True, None


def run_bounded_pytest(
    argv: Sequence[str],
    *,
    cwd: Path | str | None,
    raw_path: Path | str,
    timeout_seconds: int,
    env: Mapping[str, str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run argv, capturing stdout/stderr into ``raw_path``, with a finite wait.

    Return shape is closed: ``returned``, ``timeout``, or ``unavailable``.
    """
    raw = Path(raw_path)
    run_token = secrets.token_hex(16)
    command = [str(item) for item in argv]
    argv_sha256 = hashlib.sha256(json.dumps(command, separators=(",", ":")).encode("utf-8")).hexdigest()
    pytest_attested = _pytest_command_matches_current_interpreter(command)
    child_env = dict(env) if env is not None else dict(os.environ)
    child_env[RUN_MARKER_ENV] = run_token
    try:
        raw.parent.mkdir(parents=True, exist_ok=True)
        with raw.open("wb") as handle:
            runtime_error = None
            if pytest_attested:
                pytest_attested, runtime_error = _pytest_runtime_available(
                    command,
                    cwd=cwd,
                    env=child_env,
                    timeout_seconds=timeout_seconds,
                )
                if not pytest_attested:
                    return _bounded_pytest_status(
                        outcome="unavailable",
                        timed_out=False,
                        returncode=None,
                        error=runtime_error,
                        raw=raw,
                        pytest_attested=False,
                        run_id=run_id,
                        run_token=run_token,
                        argv_sha256=argv_sha256,
                    )
            try:
                proc = subprocess.Popen(
                    command,
                    cwd=str(cwd) if cwd is not None else None,
                    env=child_env,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as exc:
                return _bounded_pytest_status(
                    outcome="unavailable",
                    timed_out=False,
                    returncode=None,
                    error=f"{type(exc).__name__}: {exc}",
                    raw=raw,
                    pytest_attested=pytest_attested,
                    run_id=run_id,
                    run_token=run_token,
                    argv_sha256=argv_sha256,
                )
            try:
                returncode = proc.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                cleanup_signals = _kill_process_group(proc, marker=run_token)
                return _bounded_pytest_status(
                    outcome="timeout",
                    timed_out=True,
                    returncode=None,
                    error=None,
                    raw=raw,
                    pytest_attested=pytest_attested,
                    run_id=run_id,
                    run_token=run_token,
                    argv_sha256=argv_sha256,
                    signals=cleanup_signals,
                )
            outcome = "signaled" if isinstance(returncode, int) and returncode < 0 else "returned"
            return _bounded_pytest_status(
                outcome=outcome,
                timed_out=False,
                returncode=returncode,
                error=None,
                raw=raw,
                pytest_attested=pytest_attested,
                run_id=run_id,
                run_token=run_token,
                argv_sha256=argv_sha256,
            )
    except OSError as exc:
        return _bounded_pytest_status(
            outcome="unavailable",
            timed_out=False,
            returncode=None,
            error=f"{type(exc).__name__}: {exc}",
            raw=raw,
            pytest_attested=pytest_attested,
            run_id=run_id,
            run_token=run_token,
            argv_sha256=argv_sha256,
        )


def write_status(path: Path | str, payload: Mapping[str, Any]) -> None:
    Path(path).write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_timeout(value: str) -> int:
    try:
        timeout = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a positive integer") from exc
    if timeout <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive integer")
    return timeout


def _run_pytest_cli(args: argparse.Namespace) -> int:
    if not _pytest_command_matches_current_interpreter(args.command):
        raw = Path(args.raw)
        try:
            raw.parent.mkdir(parents=True, exist_ok=True)
            raw.touch()
        except OSError:
            pass
        result = _bounded_pytest_status(
            outcome="unavailable",
            timed_out=False,
            returncode=None,
            error="--run-pytest requires the current interpreter followed by -m pytest",
            raw=raw,
            pytest_attested=False,
            run_id=args.run_id,
            run_token=secrets.token_hex(16),
            argv_sha256=hashlib.sha256(
                json.dumps([str(item) for item in args.command], separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        )
        if args.status is not None:
            write_status(args.status, result)
        return UNAVAILABLE_EXIT
    result = run_bounded_pytest(
        args.command,
        cwd=args.cwd,
        raw_path=args.raw,
        timeout_seconds=args.timeout,
        run_id=args.run_id,
    )
    if args.status is not None:
        write_status(args.status, result)
    if result["outcome"] == "returned" and result.get("pytest_attested") is True:
        return 0
    if result["outcome"] == "timeout":
        return TIMEOUT_EXIT
    if result["outcome"] == "signaled":
        return SIGNAL_EXIT
    return UNAVAILABLE_EXIT


def _fingerprint_from_args(args: argparse.Namespace) -> str:
    wrapper = args.fingerprint_wrapper
    if args.fingerprint:
        return str(args.fingerprint)
    if args.fingerprint_script:
        return trusted_producer_fingerprint(args.fingerprint_script, wrapper=wrapper)
    if args.scope and Path(args.scope).is_file():
        try:
            payload = json.loads(Path(args.scope).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            payload = {}
        if isinstance(payload, dict) and payload.get("producer_fingerprint"):
            return str(payload["producer_fingerprint"])
    return trusted_producer_fingerprint(wrapper=wrapper)


def _trusted_fingerprint_from_args(args: argparse.Namespace) -> str:
    wrapper = args.fingerprint_wrapper
    if args.fingerprint_script:
        return trusted_producer_fingerprint(args.fingerprint_script, wrapper=wrapper)
    return trusted_producer_fingerprint(wrapper=wrapper)


def _load_status_payload(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        loaded = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _is_producer_status(payload: Mapping[str, Any]) -> bool:
    if payload.get("producer") != BOUNDED_PYTEST_PRODUCER:
        return False
    return payload.get("outcome") in {"returned", "timeout", "signaled", "unavailable"}


def _can_issue_complete(args: argparse.Namespace, payload: Mapping[str, Any]) -> bool:
    if args.raw is None or not _is_producer_status(payload):
        return False
    if not _producer_status_is_complete(payload, sha256_file(args.raw)):
        return False
    if args.run_id is not None and payload.get("run_id") != args.run_id:
        return False
    return True


def _write_receipt_cli(args: argparse.Namespace) -> int:
    status_payload = _load_status_payload(args.from_status)
    source_outcome = status_payload.get("outcome")
    requested_complete = args.receipt_status == RECEIPT_COMPLETE or source_outcome == "returned"
    error = status_payload.get("error")
    if source_outcome in {"timeout", "signaled", "unavailable"}:
        status = source_outcome
        pytest_returncode = None
        fingerprint = _fingerprint_from_args(args)
    elif requested_complete and _can_issue_complete(args, status_payload):
        status = RECEIPT_COMPLETE
        pytest_returncode = status_payload.get("returncode")
        fingerprint = _trusted_fingerprint_from_args(args)
    else:
        status = args.receipt_status or source_outcome or "incomplete"
        if status in {RECEIPT_COMPLETE, "returned"}:
            status = "incomplete"
            if requested_complete and not error:
                error = "complete receipt requires producer-issued bounded-pytest status"
        if status not in {RECEIPT_COMPLETE, "timeout", "incomplete", "unavailable"}:
            status = "incomplete"
        pytest_returncode = args.pytest_returncode
        if pytest_returncode is None and "returncode" in status_payload:
            pytest_returncode = status_payload.get("returncode")
        if status in {"timeout", "unavailable"}:
            pytest_returncode = None
        fingerprint = _fingerprint_from_args(args)
    run_id = args.run_id or status_payload.get("run_id")
    try:
        written = write_completion_receipt(
            args.write_receipt,
            status=status,
            pytest_returncode=pytest_returncode,
            raw=args.raw,
            ids=args.ids,
            nids=args.nids,
            scope=args.scope,
            fingerprint=fingerprint,
            timeout_seconds=args.timeout,
            error=error if isinstance(error, str) else None,
            run_id=str(run_id) if run_id else None,
            producer_status=status_payload,
            producer_script=args.fingerprint_script,
            producer_wrapper=args.fingerprint_wrapper,
            trusted_fingerprint=(_trusted_fingerprint_from_args(args) if status == RECEIPT_COMPLETE else None),
        )
    except OSError:
        return UNAVAILABLE_EXIT
    issued = load_receipt(written)
    if requested_complete and (not issued or issued.get("status") != RECEIPT_COMPLETE):
        return COMPLETE_REFUSED_EXIT
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-pytest", action="store_true")
    parser.add_argument("--write-receipt", type=Path)
    parser.add_argument("--raw", type=Path)
    parser.add_argument("--run-status", dest="status", type=Path)
    parser.add_argument("--from-status", type=Path)
    parser.add_argument("--ids", type=Path)
    parser.add_argument("--nids", type=Path)
    parser.add_argument("--scope", type=Path)
    parser.add_argument("--cwd", type=Path)
    parser.add_argument("--timeout", type=_parse_timeout, default=DEFAULT_TIMEOUT_SEC)
    parser.add_argument("--receipt-status", dest="receipt_status", type=str)
    parser.add_argument("--pytest-returncode", type=int, default=None)
    parser.add_argument("--run-id", dest="run_id", default=None)
    parser.add_argument("--fingerprint", default=None)
    parser.add_argument("--fingerprint-script", type=Path)
    parser.add_argument("--fingerprint-wrapper", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.run_pytest:
        command = list(args.command)
        if command and command[0] == "--":
            command = command[1:]
        if not command or args.raw is None:
            parser.error("--run-pytest requires --raw and a command after --")
        args.command = command
        return _run_pytest_cli(args)
    if args.write_receipt is not None:
        return _write_receipt_cli(args)
    parser.error("specify --run-pytest or --write-receipt")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
