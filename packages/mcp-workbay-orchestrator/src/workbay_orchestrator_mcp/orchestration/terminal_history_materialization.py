"""Authenticated same-terminal-job materialization of retained native artifacts (R2).

The original detached job is terminal and its canonical local outputs are
absent. The only native path that can fetch them is the existing collection
transport. This module drives its explicitly governed pure staging mode
(``remote_agent.sh collect --stage-dir``): the same SSH stream and the same
bundle/done/stdout/manifest validation as ``collect``, with no stdout,
destination install, review-cycle bridge or VM ``collected_at`` stamp.

The staged bundle is then bound to the native records before anything is
published: ``done.json`` names the same job and the ledger's terminal rc, and
the staged source provenance passes the native validator against the host
dispatch receipt with the identical nonce/ref. Only then is each absent
spec-named canonical artifact created exclusively (0600); identical existing
bytes are accepted and different bytes are a conflict, never overwritten.

Canon 0.26.2: GRPH-14 lineage (same job, same carrier, same nonce); DATA-14
(no rewrite of any original record); SECD-05 (typed holds, staging always
removed); AGT-21 (job rc and transport outcome are distinct); RES-02 (bounded
transport and reads).
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import offload_pass_recovery as recovery
from .offload_pass_recovery import EvidenceHold

STAGE_DIRNAME = "terminal-materialization"
# Pure staging capability marker of the paired root/payload remote_agent.sh.
STAGE_MARKER = b"--stage-dir)"
DEFAULT_TIMEOUT_SECONDS = 300.0
_CARRIER_LIMIT = 16 * 1024 * 1024
_BUNDLE_LIMIT = 256 * 1024 * 1024
# Bundle artifact key -> canonical artifact name of ``_detached_artifact_paths``.
_BUNDLE_NAMES = {
    "result-out": "result",
    "debug-out": "debug",
    "stream-out": "stream",
    "selfverify-out": "selfverify",
    "uncommitted-out": "uncommitted",
    "phases-out": "phases",
    "provenance-out": "provenance",
}


@dataclass(frozen=True)
class Staged:
    """An authenticated staged bundle: canonical artifact name -> bytes."""

    job_id: str
    rc: int
    files: dict[str, bytes]


def carrier_supported(argv: object) -> tuple[bool, str | None]:
    """Whether the original carrier ``argv[0]`` offers the governed staging mode."""
    if not isinstance(argv, list) or not argv or not isinstance(argv[0], str):
        return False, "carrier_unavailable"
    carrier = Path(argv[0])
    try:
        info = os.stat(carrier)
        if not carrier.is_absolute() or not stat.S_ISREG(info.st_mode) or not os.access(carrier, os.X_OK):
            return False, "carrier_unavailable"
        if info.st_size > _CARRIER_LIMIT or STAGE_MARKER not in carrier.read_bytes():
            return False, "carrier_without_stage_mode"
    except OSError:
        return False, "carrier_unavailable"
    return True, None


def _identity_env(spec: Mapping[str, Any]) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        WORKBAY_REMOTE_JOB_TASK_REF=str(spec["task_ref"]),
        WORKBAY_REMOTE_JOB_LANE_ID=str(spec["lane_id"]),
        WORKBAY_REMOTE_JOB_PASS_ID=str(spec["pass_id"]),
        WORKBAY_REMOTE_JOB_ATTEMPT=str(spec["attempt"]),
    )
    return env


def _private_stage(state_dir: Path, job_id: str) -> Path:
    root = Path(state_dir) / "remote-jobs" / STAGE_DIRNAME
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = os.lstat(root)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise EvidenceHold("materialization_stage_unsafe", str(root))
    return Path(tempfile.mkdtemp(prefix=f"{job_id}.", dir=root))


def _read_staged(stage: Path, job_id: str, rc: int) -> Staged:
    """Re-bind the shell-validated bundle to this job and its terminal rc."""

    def read(relative: str) -> bytes:
        return recovery.regular_file_bytes(stage / relative, root=stage, limit=_BUNDLE_LIMIT)

    try:
        done = json.loads(read("done.json"))
        stdout = read("stdout")
        manifest = json.loads(read("artifacts.json"))
    except (OSError, ValueError) as exc:
        raise EvidenceHold("materialization_bundle_invalid") from exc
    if not isinstance(done, dict) or done.get("job_id") != job_id:
        raise EvidenceHold("materialization_job_mismatch")
    if type(done.get("rc")) is not int or done["rc"] != rc:
        raise EvidenceHold("materialization_rc_mismatch")
    files_digest = manifest.get("files") if isinstance(manifest, dict) else None
    if (
        done.get("stdout_sha256") != hashlib.sha256(stdout).hexdigest()
        or manifest.get("job_id") != job_id
        or not isinstance(files_digest, dict)
        or set(files_digest) - set(_BUNDLE_NAMES)
    ):
        raise EvidenceHold("materialization_bundle_invalid")
    files = {"patch": stdout}
    for key, digest in files_digest.items():
        try:
            data = read(f"artifacts/{key}")
        except (OSError, EvidenceHold) as exc:
            raise EvidenceHold("materialization_bundle_invalid") from exc
        if hashlib.sha256(data).hexdigest() != digest:
            raise EvidenceHold("materialization_bundle_invalid")
        files[_BUNDLE_NAMES[key]] = data
    return Staged(job_id=job_id, rc=rc, files=files)


def _bind_provenance(stage_file: Path, receipt: Mapping[str, Any]) -> None:
    """The staged provenance is the host request's own source and nonce."""
    from .adapters.remote_exec import _source_provenance_from_json  # noqa: PLC0415

    proof, failure = _source_provenance_from_json(
        stage_file,
        branch=str(receipt["source_branch"]),
        head_sha=receipt["source_head"],
        head_tree=receipt["source_tree"],
    )
    if proof is None:
        raise EvidenceHold(
            "retained_evidence_missing" if failure == "missing" else f"source_provenance_{failure or 'malformed'}"
        )
    native = receipt.get("native_dispatch")
    if not isinstance(native, dict) or native != {
        "dispatch_nonce": proof.get("dispatch_nonce"),
        "dispatch_ref": proof.get("dispatch_ref"),
    }:
        raise EvidenceHold("native_dispatch_mismatch")


def stage(
    *,
    repo: Path,
    state_dir: Path,
    job_id: str,
    spec: Mapping[str, Any],
    rc: int,
    receipt: Mapping[str, Any],
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Staged:
    """Collect the same terminal job once in pure staging mode and authenticate it."""
    from .adapters import remote_job_runner  # noqa: PLC0415

    if (
        receipt.get("state") != "transport_started"
        or not isinstance(receipt.get("native_dispatch"), dict)
        or receipt.get("source_branch") != spec.get("branch")
        or not all(isinstance(receipt.get(key), str) for key in ("source_head", "source_tree"))
    ):
        raise EvidenceHold("dispatch_identity_mismatch")
    supported, reason = carrier_supported(spec.get("argv"))
    if not supported:
        raise EvidenceHold("materialization_unsupported", reason)
    directory = _private_stage(Path(state_dir), job_id)
    try:
        completed = remote_job_runner.materialize_only(
            list(spec["argv"]), cwd=str(repo), env=_identity_env(spec), timeout=timeout, stage_dir=directory
        )
        if completed.returncode == 75:
            raise EvidenceHold("job_not_terminal")
        if completed.returncode != 0:
            hold = EvidenceHold("materialization_transport_failed")
            hold.transport_rc = completed.returncode  # type: ignore[attr-defined]
            raise hold
        staged = _read_staged(directory, job_id, rc)
        if "provenance" not in staged.files or "stream" not in staged.files:
            raise EvidenceHold("retained_evidence_missing", "materialized bundle")
        _bind_provenance(directory / "artifacts" / "provenance-out", receipt)
        return staged
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def _create_exclusive(path: Path, data: bytes) -> None:
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".materialize", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        # link() never replaces an existing canonical file.
        os.link(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def publish(staged: Staged, artifact_paths: Mapping[str, Path], *, evidence_root: Path) -> dict[str, dict[str, Any]]:
    """Idempotently create each absent spec-named artifact; conflicts never overwrite."""
    plan: list[tuple[Path, bytes]] = []
    for name in sorted(artifact_paths):
        if name not in staged.files:
            continue
        path = Path(artifact_paths[name])
        if not path.is_absolute() or not Path(os.path.realpath(path.parent)).is_relative_to(
            Path(os.path.realpath(evidence_root))
        ):
            raise EvidenceHold("evidence_path_identity_invalid", str(path))
        try:
            existing = recovery.regular_file_bytes(path, root=evidence_root)
        except FileNotFoundError:
            if os.path.lexists(path):
                raise EvidenceHold("evidence_path_identity_invalid", str(path)) from None
            plan.append((path, staged.files[name]))
            continue
        if existing != staged.files[name]:
            raise EvidenceHold("materialization_conflict", str(path))
    for path, data in plan:
        try:
            _create_exclusive(path, data)
        except FileExistsError:
            if recovery.regular_file_bytes(path, root=evidence_root) != data:
                raise EvidenceHold("materialization_conflict", str(path)) from None
    return {
        name: {
            "path": str(artifact_paths[name]),
            "sha256": hashlib.sha256(staged.files[name]).hexdigest(),
            "bytes": len(staged.files[name]),
        }
        for name in sorted(artifact_paths)
        if name in staged.files
    }
