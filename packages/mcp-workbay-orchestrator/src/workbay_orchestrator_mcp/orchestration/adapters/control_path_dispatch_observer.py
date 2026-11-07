"""Observe host dispatch receipts and publish under operator-created authority.

Worker results, streams, self-verification and patch claims are never identity
inputs. This module does not bootstrap policy or wire a release caller. Its
callers must be trusted host code: ``publish_proof`` accepts observed identities,
not worker verdict identity claims, and does not confer trust on arbitrary dicts.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Mapping

from ..codex_lane_config import CODEX_SPEED_TO_SERVICE_TIER
from . import control_path_review_policy as policy

_IDENTITY_KEYS = policy._IDENTITY_KEYS
_OBJECT_ID = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_QUARANTINE_NAME = re.compile(r"[0-9a-f]{64}\.json")
_MAX_ARTIFACT_BYTES = 1024 * 1024


class ObserverRefusal(ValueError):
    """A typed fail-closed observation or publication failure."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _host_directory(path: Path) -> int:
    """Walk the canonical host directory with protected descriptors.

    Like the policy common-dir resolver, canonicalize the host-supplied anchor
    first (macOS tempfile roots can start with /var, an alias for /private/var).
    Artifact leaves are always opened separately with O_NOFOLLOW.
    """
    parts = path.resolve().parts[1:]
    owners = policy._trusted_owners()
    fd = os.open("/", policy._DIRECTORY_FLAGS)
    try:
        policy._protected(os.fstat(fd), owners, directory=True, shared_ancestor=True)
        for index, part in enumerate(parts):
            if part in {".", ".."}:
                raise ObserverRefusal("artifact_path_invalid")
            child = policy._child_directory(fd, part, owners, shared_ancestor=index < len(parts) - 1)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _artifact(path: Path, name: str, *, raw: bool = False, private: bool = False):
    try:
        directory = _host_directory(path.parent)
        try:
            fd = os.open(path.name, policy._READ_FLAGS, dir_fd=directory)
        finally:
            os.close(directory)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            policy._protected(info, policy._trusted_owners(), directory=False)
            if private and stat.S_IMODE(info.st_mode) != 0o600:
                raise ObserverRefusal(name + "_untrusted")
            data = stream.read(_MAX_ARTIFACT_BYTES + 1)
        if len(data) > _MAX_ARTIFACT_BYTES:
            raise ObserverRefusal(name + "_too_large")
        if raw:
            return data
        value = json.loads(data, object_pairs_hook=policy._unique_object)
        if not isinstance(value, dict):
            raise ObserverRefusal(name + "_invalid")
        return value
    except FileNotFoundError as exc:
        raise ObserverRefusal(name + "_missing") from exc
    except (OSError, policy.PolicyHold, UnicodeError, ValueError) as exc:
        if isinstance(exc, ObserverRefusal):
            raise
        reason = "untrusted" if isinstance(exc, (OSError, policy.PolicyHold)) else "invalid"
        raise ObserverRefusal(name + "_" + reason) from exc


def _quarantine_records(spool: Path, pass_path: Path) -> list[dict]:
    # Match the producer's managed state root and its tempfile fallback. Never
    # search other spools, worker outputs, or the rest of the repository.
    records = []
    for root in dict.fromkeys((pass_path.parent, spool.parent)):
        directory_path = root / "control-path-quarantine" / spool.name
        try:
            directory = _host_directory(directory_path)
        except FileNotFoundError:
            continue
        except (OSError, policy.PolicyHold) as exc:
            raise ObserverRefusal("quarantine_untrusted") from exc
        try:
            names = sorted(name for name in os.listdir(directory) if _QUARANTINE_NAME.fullmatch(name))
        finally:
            os.close(directory)
        records.extend(_artifact(directory_path / name, "quarantine") for name in names)
    if not records:
        raise ObserverRefusal("quarantine_missing")
    return records


def _argv_coordinates(argv: list[str]) -> dict:
    """Decode the exact codex flags emitted by the AgentSpec producer."""
    model = None
    configs = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in {"-m", "--model", "-c"}:
            if index + 1 == len(argv):
                raise ObserverRefusal("route_argv_mismatch")
            value = argv[index + 1]
            if token == "-c":
                key, separator, value = value.partition("=")
                if not separator:
                    raise ObserverRefusal("route_argv_mismatch")
                configs[key] = value
            else:
                model = value
            index += 1
        index += 1
    return {"model": model, "effort": configs.get("model_reasoning_effort"), "service_tier": configs.get("service_tier")}


def observed_dispatch_identity(spool_dir: str | Path, pass_record_path: str | Path) -> dict:
    """Link spec, argv, process-start provenance, pass and quarantine receipts.

    Quarantine metadata is discovered at the producer's state-root/spool-name
    anchor. Every record there must agree; a receipt for a different lane or
    spool cannot be ignored in favour of a convenient matching record.
    """
    spool = Path(spool_dir).resolve()
    pass_path = Path(pass_record_path).absolute()
    pass_path = pass_path.parent.resolve() / pass_path.name
    spec = _artifact(spool / "spec.json", "spec", private=True)
    argv_bytes = _artifact(spool / "spec.argv", "argv", raw=True, private=True)
    provenance = _artifact(spool / "source-provenance.json", "provenance")
    passed = _artifact(pass_path, "pass_record")
    records = _quarantine_records(spool, pass_path)
    lane, dispatch = passed.get("lane_id"), passed.get("dispatch_id")
    if not _text(lane):
        raise ObserverRefusal("lane_missing")
    if not _text(dispatch):
        raise ObserverRefusal("dispatch_id_missing")
    if not _text(spec.get("branch")) or spec["branch"] != passed.get("branch"):
        raise ObserverRefusal("branch_mismatch")
    try:
        route = policy._route(spec.get("routing_effective"))
        resolved = passed.get("routing_resolved")
        if not isinstance(resolved, dict):
            raise ObserverRefusal("route_missing")
        if route != policy._route({key: value for key, value in resolved.items() if key != "source_by_field"}):
            raise ObserverRefusal("route_mismatch")
    except policy.PolicyHold as exc:
        raise ObserverRefusal("route_invalid") from exc
    if route["backend"] != spec.get("backend_id"):
        raise ObserverRefusal("backend_mismatch")
    argv = spec.get("argv")
    if (
        not isinstance(argv, list) or not argv
        or any(not isinstance(arg, str) or "\0" in arg for arg in argv)
        or argv_bytes != b"".join(arg.encode("utf-8") + b"\0" for arg in argv)
    ):
        raise ObserverRefusal("route_argv_mismatch")
    if route["speed"] is not None and route["speed"] not in CODEX_SPEED_TO_SERVICE_TIER:
        raise ObserverRefusal("route_argv_mismatch")
    if _argv_coordinates(argv) != {
        "model": route["model"], "effort": route["effort"],
        "service_tier": CODEX_SPEED_TO_SERVICE_TIER.get(route["speed"]),
    }:
        raise ObserverRefusal("route_argv_mismatch")
    head = spec.get("head_sha")
    if not isinstance(head, str) or not _OBJECT_ID.fullmatch(head):
        raise ObserverRefusal("source_invalid")
    if (
        provenance.get("remote_source_commit") != head or provenance.get("agent_spec_head_sha") != head
        or provenance.get("requested_branch") != spec["branch"]
        or provenance.get("requested_ref") != "refs/heads/" + spec["branch"]
    ):
        raise ObserverRefusal("source_mismatch")
    ref, nonce = provenance.get("dispatch_ref"), provenance.get("dispatch_nonce")
    if not _text(ref):
        raise ObserverRefusal("dispatch_ref_missing")
    if not _text(nonce) or not ref.startswith("refs/heads/") or not ref.endswith("-" + nonce):
        raise ObserverRefusal("dispatch_ref_mismatch")
    if provenance.get("schema_version") != 1 or provenance.get("history_stripped") is not True:
        raise ObserverRefusal("provenance_invalid")
    if provenance.get("model_process_started") is not True:
        raise ObserverRefusal("process_not_observed")
    for record in records:
        if record.get("lane_id") != lane:
            raise ObserverRefusal("lane_mismatch")
        recorded_spool = record.get("spool_path")
        if not _text(recorded_spool) or not Path(recorded_spool).is_absolute() or Path(recorded_spool).resolve() != spool:
            raise ObserverRefusal("spool_mismatch")
    return {**route, "lane": lane, "dispatch_id": dispatch, "execution_location": "remote_vm"}


def _validate_context(loaded: policy.ReviewPolicy, context: object) -> dict:
    """Mirror check_dispatch_proof's context checks before creating its input.

    The reader has no separate context validator. Keep these checks in parity
    (including diagnostics) while sharing its keys, digest and route helpers.
    """
    if not isinstance(context, dict) or set(context) != policy._CONTEXT_KEYS:
        raise ObserverRefusal("review_policy_release_context_missing")
    if any(not _text(value) for key, value in context.items() if key != "source_ordinal"):
        raise ObserverRefusal("review_policy_release_context_invalid")
    if (
        type(context["source_ordinal"]) is not int or context["source_ordinal"] < 1
        or _OBJECT_ID.fullmatch(context["source_commit"]) is None
    ):
        raise ObserverRefusal("review_dispatch_source_invalid")
    for key in ("repo_root", "quarantine_path"):
        path = Path(context[key])
        if not path.is_absolute() or str(path.resolve()) != context[key]:
            raise ObserverRefusal("review_policy_release_context_invalid")
    if loaded.authority_identity is None or context["repo_root"] != loaded.repo_root:
        raise ObserverRefusal("review_policy_release_context_invalid")
    if any(re.fullmatch(r"[0-9a-f]{64}", context[key]) is None for key in ("patch_sha256", "verdict_digest")):
        raise ObserverRefusal("review_policy_release_context_invalid")
    if _OBJECT_ID.fullmatch(context["expected_tip"]) is None:
        raise ObserverRefusal("review_policy_release_context_invalid")
    return dict(context)


def _identity(value: object, route: dict) -> dict:
    if not isinstance(value, Mapping) or set(value) != _IDENTITY_KEYS:
        raise ObserverRefusal("review_dispatch_identity_invalid")
    observed = dict(value)
    if any(not _text(observed[key]) for key in ("lane", "dispatch_id", "execution_location")):
        raise ObserverRefusal("review_dispatch_identity_invalid")
    if observed["execution_location"] != "remote_vm":
        raise ObserverRefusal("review_dispatch_location_mismatch")
    if policy._route({key: observed[key] for key in policy._ROUTE_KEYS}) != route:
        raise ObserverRefusal("review_dispatch_route_mismatch")
    return observed


def _unlink_staging_links(proofs: int, digest: str, name: str) -> None:
    """Remove this context's staging names that still link to the final proof.

    A publisher that stopped between its link and its staging unlink leaves the
    complete proof with st_nlink 2, which the reader refuses. Only names that
    fully match this digest's staging pattern and are the final's inode are
    removed, without following symlinks; another inode may be a concurrent
    publisher's unfinished file. Safe to repeat: a second pass finds nothing.
    """
    final = os.stat(name, dir_fd=proofs, follow_symlinks=False)
    if not stat.S_ISREG(final.st_mode) or final.st_nlink <= 1:
        return
    pattern = re.compile(r"\." + re.escape(digest) + r"\.[0-9a-f]{16}\.staging")
    for entry in os.listdir(proofs):
        if not pattern.fullmatch(entry):
            continue
        try:
            info = os.stat(entry, dir_fd=proofs, follow_symlinks=False)
            if (info.st_dev, info.st_ino) == (final.st_dev, final.st_ino):
                os.unlink(entry, dir_fd=proofs)
        except FileNotFoundError:
            continue
    os.fsync(proofs)


def publish_proof(repo_root: str | Path, context: object, *, author: object, reviewer: object) -> dict:
    """Publish once under an existing, pinned distinct-dispatch authority.

    The complete proof is written and fsynced under a per-attempt staging name
    (``.<digest>.<16 hex>.staging``, never the reader's ``<digest>.json``).
    O_EXCL on that name fences only this attempt; the no-replace ``os.link`` to
    the final name is the publication point, so the final name only ever
    appears complete and a completed proof is never overwritten. A failure
    before the link leaves no final name and blocks no retry. A stop after the
    link is healed by the next publish of this context, which still refuses
    with ``proof_exists``. Nothing fences unrelated quarantine/pass writers.
    """
    try:
        loaded = policy.load_policy(repo_root)
        if loaded.mode != "distinct_dispatch":
            raise ObserverRefusal("policy_not_distinct_dispatch")
        context = _validate_context(loaded, context)
        author = _identity(author, loaded.document["author_route"])
        reviewer = _identity(reviewer, loaded.document["reviewer_route"])
        if author["lane"] != context["lane"]:
            raise ObserverRefusal("review_dispatch_source_mismatch")
        if author["lane"] == reviewer["lane"] or author["dispatch_id"] == reviewer["dispatch_id"]:
            raise ObserverRefusal("review_dispatch_not_independent")
        policy._expiry(loaded.document["expires_at"])
        proof = {
            "schema": policy._PROOF_SCHEMA, "policy_digest": loaded.policy_digest,
            "expires_at": loaded.document["expires_at"], "context": context, "author": author, "reviewer": reviewer,
        }
        data = (json.dumps(proof, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
        if len(data) > policy._MAX_BYTES:
            raise ObserverRefusal("review_dispatch_proof_too_large")
        authority = policy._open_authority(loaded.common_dir)
        try:
            info = os.fstat(authority)
            if (info.st_dev, info.st_ino) != loaded.authority_identity:
                raise ObserverRefusal("review_policy_authority_replaced")
            proofs = policy._child_directory(authority, "proofs", policy._trusted_owners())
            try:
                digest = policy.digest(context)
                name = digest + ".json"
                staging = f".{digest}.{secrets.token_hex(8)}.staging"
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
                fd = os.open(staging, flags, 0o600, dir_fd=proofs)
                try:
                    with os.fdopen(fd, "wb") as stream:
                        os.fchmod(stream.fileno(), 0o600)
                        if os.fstat(stream.fileno()).st_uid != os.geteuid():
                            raise ObserverRefusal("proof_owner_mismatch")
                        stream.write(data)
                        stream.flush()
                        os.fsync(stream.fileno())
                    try:
                        os.link(staging, name, src_dir_fd=proofs, dst_dir_fd=proofs, follow_symlinks=False)
                    except FileExistsError:
                        _unlink_staging_links(proofs, digest, name)
                        raise
                finally:
                    try:
                        os.unlink(staging, dir_fd=proofs)
                    except FileNotFoundError:
                        pass
                    os.fsync(proofs)
            finally:
                os.close(proofs)
        finally:
            os.close(authority)
        return {
            "proof_source": os.path.join(loaded.common_dir, *policy._AUTHORITY_PARTS, "proofs", name),
            "proof_digest": policy.digest(proof), "proof": proof,
        }
    except FileExistsError as exc:
        raise ObserverRefusal("proof_exists") from exc
    except policy.PolicyHold as exc:
        raise ObserverRefusal(str(exc)) from exc
    except (OSError, UnicodeError, ValueError) as exc:
        if isinstance(exc, ObserverRefusal):
            raise
        raise ObserverRefusal("proof_publication_unavailable") from exc
