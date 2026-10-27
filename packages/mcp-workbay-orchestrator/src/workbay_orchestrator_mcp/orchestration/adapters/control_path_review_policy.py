"""Host-authorized independence policy; no worker/caller configuration inputs.

Deployment contract (inactive until independently reviewed operator bootstrap):
the one authority is ``<Git common dir>/workbay/control-path-review``, beside
the native review-cycle authority (``review_cycle_dispatch.authority``). The
common directory is derived here on every use from the release repository with
``git_environment()``, the environment the release CAS also uses, so it is the
ref store the release updates. Verdict fields, release context, ``GIT_DIR`` /
``GIT_COMMON_DIR``, tracked files and per-worktree Git directories cannot select
it. A root that is not inside any Git repository has no authority.

``policy.json`` is schema ``workbay/control-path-review-policy@1``, mode
``different_model`` or ``distinct_dispatch``, with ``expires_at`` (UTC RFC3339).
The latter also has ``author_route`` and ``reviewer_route``: already resolved
RoutingQuad wire records (backend/model/effort/speed/tier). This module compares
executed routes; it never resolves preferences, aliases, capabilities or
fallback a second time.

For distinct_dispatch, a trusted host observer must publish
``proofs/<context digest>.json`` with schema
``workbay/control-path-review-proof@1``, policy_digest, expires_at, context,
author and reviewer. Context binds canonical
repo_root/quarantine_path, lane_branch, expected_tip, lane, patch_sha256 and
verdict_digest plus source_commit and source_ordinal. Release callers must
provide those two source claims; only a matching observer record proves them.
Author/reviewer are *observed executed* identity records:
lane, dispatch_id, backend/model/effort/speed/tier, execution_location=remote_vm.
The observer must authenticate native dispatch IDs, source lineage/ordinal,
actual executed routes and exact reviewed bytes before publication. It must not
copy worker assertions. The current quarantine producer does NOT supply this
proof. Missing proof is a HOLD, never an implicit approval.

Trusted owners are fixed here on every use, never supplied: UID 0 and the
effective UID of the host runtime evaluating the release. The supported
user-prefix managed install therefore bootstraps and publishes as its ordinary
host user, and a privileged install may publish root-owned records; neither
needs new privileges. Remote workers run on the VM and cannot write the host
Git common directory. (The superseded fixed ``/etc`` anchor required root to
publish every per-review proof; CONTROL-POLICY-R1-F1.)

Every component from ``/`` to a record is opened without following symlinks,
must be owned by a trusted owner and must not be group/world writable. Only a
root-owned sticky ancestor above the common directory (``/tmp``) is exempt:
sticky semantics stop other users replacing the trusted entry beneath it. The
authority directory identity is pinned between the policy and proof reads.
Proofs are not accepted from stdin, env, repository files or receipts. JSON is
bounded and strict. No code here creates/activates policy or proof, or grants
bootstrap authority.

Canon 0.26.2: [AGT-17] Separate policy from mechanism (The Art of UNIX
Programming, ch. 1); [SECD-03] Check every access at the reference monitor and
[SECD-05] Fail-safe defaults (Security Engineering); [RLSE-33] Inspect hermetic
configuration before effects (The Site Reliability Workbook, ch. 15).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from ..lane_routing import RoutingQuad

_ROOT_UID = 0
_MAX_BYTES = 65536
_GIT_TIMEOUT_SECONDS = 10
_ROUTE_KEYS = set(RoutingQuad().as_dict())
_IDENTITY_KEYS = _ROUTE_KEYS | {"lane", "dispatch_id", "execution_location"}
_CONTEXT_KEYS = {
    "repo_root",
    "lane_branch",
    "expected_tip",
    "lane",
    "quarantine_path",
    "patch_sha256",
    "verdict_digest",
    "source_commit",
    "source_ordinal",
}
_POLICY_SCHEMA = "workbay/control-path-review-policy@1"
_PROOF_SCHEMA = "workbay/control-path-review-proof@1"
_AUTHORITY_PARTS = ("workbay", "control-path-review")
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


class PolicyHold(ValueError):
    """A precise fail-closed producer/configuration diagnostic."""


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def git_environment() -> dict[str, str]:
    """Release Git environment: the repository comes only from the path."""
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    for variable in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_PREFIX"):
        env.pop(variable, None)
    return env


def _trusted_owners() -> frozenset[int]:
    return frozenset({_ROOT_UID, os.geteuid()})


def _protected(info: os.stat_result, owners: frozenset[int], *, directory: bool, shared_ancestor: bool = False) -> None:
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if info.st_uid not in owners or not expected_type(info.st_mode):
        raise PolicyHold("review_policy_authority_untrusted")
    if info.st_mode & 0o022 and not (shared_ancestor and info.st_uid == _ROOT_UID and info.st_mode & stat.S_ISVTX):
        raise PolicyHold("review_policy_authority_untrusted")
    if not directory and info.st_nlink != 1:
        raise PolicyHold("review_policy_authority_untrusted")


def _git_common_dir(repo_root: str | Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            env={**git_environment(), "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyHold("review_policy_authority_unavailable") from exc
    if completed.returncode:
        if b"not a git repository" in completed.stderr.lower():
            return None
        raise PolicyHold("review_policy_authority_unavailable")
    common_dir = os.fsdecode(completed.stdout).removesuffix("\n")
    if not os.path.isabs(common_dir):
        raise PolicyHold("review_policy_authority_unavailable")
    return os.path.realpath(common_dir)


def _child_directory(parent: int, name: str, owners: frozenset[int], *, shared_ancestor: bool = False) -> int:
    child = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
    try:
        _protected(os.fstat(child), owners, directory=True, shared_ancestor=shared_ancestor)
    except BaseException:
        os.close(child)
        raise
    return child


def _open_authority(common_dir: str) -> int:
    """Walk descriptors from ``/`` through the canonical common dir."""
    owners = _trusted_owners()
    ancestors = Path(common_dir).parts[1:]
    fd = os.open("/", _DIRECTORY_FLAGS)
    try:
        _protected(os.fstat(fd), owners, directory=True, shared_ancestor=True)
        for index, component in enumerate((*ancestors, *_AUTHORITY_PARTS)):
            child = _child_directory(fd, component, owners, shared_ancestor=index < len(ancestors) - 1)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise PolicyHold("review_policy_duplicate_key")
        result[key] = value
    return result


def _read(parent: int, name: str) -> dict:
    fd = os.open(name, _READ_FLAGS, dir_fd=parent)
    with os.fdopen(fd, "rb") as stream:
        _protected(os.fstat(stream.fileno()), _trusted_owners(), directory=False)
        raw = stream.read(_MAX_BYTES + 1)
    if len(raw) > _MAX_BYTES:
        raise PolicyHold("review_policy_document_too_large")
    value = json.loads(raw, object_pairs_hook=_unique_object)
    if not isinstance(value, dict):
        raise PolicyHold("review_policy_invalid_document")
    return value


def _expiry(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise PolicyHold("review_policy_invalid_expiry")
    try:
        result = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise PolicyHold("review_policy_invalid_expiry") from exc
    if result <= datetime.now(timezone.utc):
        raise PolicyHold("review_policy_expired")
    return result


def _route(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _ROUTE_KEYS:
        raise PolicyHold("review_policy_invalid_route")
    if any(
        not isinstance(value[k], str) or not value[k] or value[k] != value[k].strip()
        for k in ("backend", "model", "effort")
    ):
        raise PolicyHold("review_policy_invalid_route")
    if any(
        value[k] is not None and (not isinstance(value[k], str) or not value[k] or value[k] != value[k].strip())
        for k in ("speed", "tier")
    ):
        raise PolicyHold("review_policy_invalid_route")
    return RoutingQuad(**value).as_dict()


@dataclass(frozen=True)
class ReviewPolicy:
    mode: str
    source: str
    policy_digest: str
    document: dict
    repo_root: str = ""
    common_dir: str = ""
    authority_identity: tuple[int, int] | None = None
    trusted_owner_uids: tuple[int, ...] = ()

    def to_dict(self) -> dict:
        resolved = {"mode": self.mode, "source": self.source, "digest": self.policy_digest, "resolved": self.document}
        if self.authority_identity is not None:
            resolved["authority"] = {
                "anchor": "git_common_dir",
                "common_dir": self.common_dir,
                "trusted_owner_uids": list(self.trusted_owner_uids),
            }
        return resolved


def _default_policy() -> ReviewPolicy:
    value = {"schema": _POLICY_SCHEMA, "mode": "different_model"}
    return ReviewPolicy("different_model", "builtin", digest(value), value)


def load_policy(repo_root: str | Path | None) -> ReviewPolicy:
    """Read the repository's host authority; absence preserves the legacy rule."""
    if repo_root is None:
        return _default_policy()
    common_dir = _git_common_dir(repo_root)
    if common_dir is None:
        return _default_policy()
    source = os.path.join(common_dir, *_AUTHORITY_PARTS, "policy.json")
    try:
        # Absence grants nothing: it selects the strict legacy rule whatever the
        # ancestry. Any present policy must pass the full protected walk below;
        # this probe cannot authorize it.
        os.lstat(source)
        fd = _open_authority(common_dir)
    except FileNotFoundError:
        return _default_policy()
    try:
        info = os.fstat(fd)
        try:
            value = _read(fd, "policy.json")
        except FileNotFoundError:
            return _default_policy()
    finally:
        os.close(fd)
    base_keys = {"schema", "mode", "expires_at"}
    mode = value.get("mode")
    if mode not in {"different_model", "distinct_dispatch"}:
        raise PolicyHold("review_policy_unknown_mode")
    keys = base_keys | ({"author_route", "reviewer_route"} if mode == "distinct_dispatch" else set())
    if set(value) != keys or value.get("schema") != _POLICY_SCHEMA:
        raise PolicyHold("review_policy_invalid_document")
    _expiry(value["expires_at"])
    if mode == "distinct_dispatch":
        _route(value["author_route"])
        reviewer = _route(value["reviewer_route"])
        if reviewer["backend"] != "codex-remote":
            raise PolicyHold("reviewer_backend_untrusted")
    return ReviewPolicy(
        mode,
        source,
        digest(value),
        value,
        str(Path(repo_root).resolve()),
        common_dir,
        (info.st_dev, info.st_ino),
        tuple(sorted(_trusted_owners())),
    )


def check_dispatch_proof(policy: ReviewPolicy, context: object, *, author: object, reviewer: object) -> dict:
    """Pure comparisons after bounded trusted reads; never mint dispatch proof."""
    if not isinstance(context, dict) or set(context) != _CONTEXT_KEYS:
        raise PolicyHold("review_policy_release_context_missing")
    if any(not isinstance(v, str) or not v or v != v.strip() for k, v in context.items() if k != "source_ordinal"):
        raise PolicyHold("review_policy_release_context_invalid")
    if (
        type(context["source_ordinal"]) is not int
        or context["source_ordinal"] < 1
        or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", context["source_commit"]) is None
    ):
        raise PolicyHold("review_dispatch_source_invalid")
    for key in ("repo_root", "quarantine_path"):
        path = Path(context[key])
        if not path.is_absolute() or str(path.resolve()) != context[key]:
            raise PolicyHold("review_policy_release_context_invalid")
    if policy.authority_identity is None or context["repo_root"] != policy.repo_root:
        raise PolicyHold("review_policy_release_context_invalid")
    for key, size in (("patch_sha256", 64), ("verdict_digest", 64)):
        if re.fullmatch(r"[0-9a-f]{" + str(size) + "}", context[key]) is None:
            raise PolicyHold("review_policy_release_context_invalid")
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", context["expected_tip"]) is None:
        raise PolicyHold("review_policy_release_context_invalid")
    proof_name = digest(context) + ".json"
    try:
        fd = _open_authority(policy.common_dir)
        try:
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) != policy.authority_identity:
                raise PolicyHold("review_policy_authority_replaced")
            proof_dir = _child_directory(fd, "proofs", _trusted_owners())
            try:
                proof = _read(proof_dir, proof_name)
            finally:
                os.close(proof_dir)
        finally:
            os.close(fd)
    except FileNotFoundError as exc:
        raise PolicyHold("review_dispatch_proof_missing:trusted_host_observer_required") from exc
    if (
        set(proof) != {"schema", "policy_digest", "expires_at", "context", "author", "reviewer"}
        or proof.get("schema") != _PROOF_SCHEMA
    ):
        raise PolicyHold("review_dispatch_proof_invalid")
    if proof["policy_digest"] != policy.policy_digest or digest(proof["context"]) != digest(context):
        raise PolicyHold("review_dispatch_proof_context_mismatch")
    if _expiry(proof["expires_at"]) > _expiry(policy.document["expires_at"]):
        raise PolicyHold("review_dispatch_proof_expiry_mismatch")
    for role, supplied, route_key in (("author", author, "author_route"), ("reviewer", reviewer, "reviewer_route")):
        observed = proof[role]
        if not isinstance(observed, dict) or set(observed) != _IDENTITY_KEYS:
            raise PolicyHold("review_dispatch_identity_invalid")
        if any(
            not isinstance(observed[k], str) or not observed[k] or observed[k] != observed[k].strip()
            for k in ("lane", "dispatch_id", "execution_location")
        ):
            raise PolicyHold("review_dispatch_identity_invalid")
        if not isinstance(supplied, Mapping) or dict(supplied) != observed:
            raise PolicyHold("review_dispatch_identity_mismatch")
        if observed["execution_location"] != "remote_vm":
            raise PolicyHold("review_dispatch_location_mismatch")
        if _route({key: observed[key] for key in _ROUTE_KEYS}) != policy.document[route_key]:
            raise PolicyHold("review_dispatch_route_mismatch")
    if proof["author"]["lane"] != context["lane"]:
        raise PolicyHold("review_dispatch_source_mismatch")
    if (
        proof["author"]["lane"] == proof["reviewer"]["lane"]
        or proof["author"]["dispatch_id"] == proof["reviewer"]["dispatch_id"]
    ):
        raise PolicyHold("review_dispatch_not_independent")
    return {
        "proof_source": os.path.join(policy.common_dir, *_AUTHORITY_PARTS, "proofs", proof_name),
        "proof_digest": digest(proof),
        "proof": proof,
    }
