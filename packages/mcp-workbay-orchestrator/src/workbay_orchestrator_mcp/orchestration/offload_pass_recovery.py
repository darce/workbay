"""Native terminal-evidence derivation shared by completion, repair and history.

One derivation authenticates the retained artifacts of a detached remote job
(named by its RemoteJobLedger spec argv) against the host-retained review-cycle
dispatch receipt, and projects them onto a terminal receipt. Three callers use
it: first publication of a nonzero exit in ``offload_pass._complete_remote_pass``,
the public ``terminal_history_repair`` preview/apply, and the cached readers
(``complete_remote_pass`` on a completed job, ``_matching_terminal_passes``).

Canon 0.26.2: GRPH-14 provenance lineage (every enriched fact names the bytes
it came from); SECD-05 fail-safe defaults (anything unauthenticated is a typed
hold, unknown token usage stays ``None``, never 0); DATA-14 no dual writes (the
original receipt is never rewritten; an approved enrichment is one
content-addressed intent in protected custody, and every reader re-derives its
view from the native records it names -- see ``terminal_history_contract``).

This module has no import-time dependency on ``offload_pass`` or the review
cycle modules, so the engine can import it lazily without a cycle.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EVIDENCE_SCHEMA = "workbay/native-terminal-evidence@1"
# Version 2 is the complete native authority contract (terminal_history_contract);
# version 1 records were self-hashed caller-chosen views and are never served.
INTENT_SCHEMA = "workbay/terminal-evidence-enrichment@2"
INTENT_DIRNAME = "terminal-evidence"
# Result keys owned by this projection. A thin receipt carries none of the
# first three; ``terminal_evidence`` may record why first publication held.
ENRICHMENT_KEYS = frozenset({"dispatch_receipt", "tokens", "token_accounting", "terminal_evidence"})
# Retained native artifacts that must exist for an authentic enrichment.
REQUIRED_ARTIFACTS = ("provenance", "stream")
_JOB_ID = re.compile(r"j[0-9a-f]{20}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MAX_ARTIFACT_BYTES = 256 * 1024 * 1024
_MAX_INTENT_BYTES = 16 * 1024 * 1024


class EvidenceHold(Exception):
    """A typed refusal (AGT-21); ``reason`` is the stable machine-readable code."""

    def __init__(self, reason: str, needed: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.needed = needed


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_sha256(value: Any) -> str:
    return sha256_hex(canonical_bytes(value))


def regular_file_bytes(path: Path, *, root: Path | None = None, limit: int = _MAX_ARTIFACT_BYTES) -> bytes:
    """Read one producer-owned regular file without following a link.

    ``FileNotFoundError`` propagates (callers map absence to their own hold).
    A symlink, non-regular file, foreign owner, world-writable mode, or a
    parent outside ``root`` is ``evidence_path_identity_invalid``; more than
    ``limit`` bytes is ``retained_evidence_oversized`` (RES-02 bounded read).
    """
    path = Path(path)
    if not path.is_absolute():
        raise EvidenceHold("evidence_path_identity_invalid", str(path))
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o002:
        raise EvidenceHold("evidence_path_identity_invalid", str(path))
    if root is not None and not Path(os.path.realpath(path.parent)).is_relative_to(Path(os.path.realpath(root))):
        raise EvidenceHold("evidence_path_identity_invalid", str(path))
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
            raise EvidenceHold("evidence_path_identity_invalid", str(path))
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
    finally:
        if fd >= 0:
            os.close(fd)
    if len(data) > limit:
        raise EvidenceHold("retained_evidence_oversized", str(path))
    return data


# ---------------------------------------------------------------------------
# Review-cycle dispatch receipt (host-retained request), read-only
# ---------------------------------------------------------------------------


def dispatch_receipt_path(repo: Path, dispatch_id: str) -> Path:
    """The native receipt locator, without creating its directory.

    ``review_cycle_dispatch._receipt_path`` names the file but also creates the
    authority directory; a read must never create it, so call it only once the
    directory is known to exist (the call is then a no-op).
    """
    from . import review_cycle_dispatch as dispatch  # noqa: PLC0415

    root = dispatch.authority(Path(repo)) / "review_cycle_dispatch"
    if not root.is_dir() or root.is_symlink():
        raise EvidenceHold("dispatch_receipt_missing", str(root))
    return dispatch._receipt_path(Path(repo), dispatch_id)


def read_dispatch_receipt(repo: Path, dispatch_id: str) -> tuple[Path, dict[str, Any], bytes]:
    path = dispatch_receipt_path(repo, dispatch_id)
    try:
        raw = regular_file_bytes(path, limit=_MAX_INTENT_BYTES)
        receipt = json.loads(raw)
    except FileNotFoundError as exc:
        raise EvidenceHold("dispatch_receipt_missing", str(path)) from exc
    except (OSError, ValueError) as exc:
        raise EvidenceHold("dispatch_receipt_unreadable", str(path)) from exc
    if not isinstance(receipt, dict):
        raise EvidenceHold("dispatch_receipt_unreadable", str(path))
    return path, receipt, raw


# ---------------------------------------------------------------------------
# Shared derivation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NativeTerminalEvidence:
    """Authenticated facts of one paid native attempt; every field names its source."""

    job_id: str
    rc: int
    backend: str
    route: dict[str, Any]
    review_cycle: dict[str, Any]
    native_dispatch: dict[str, str]
    source: dict[str, Any]
    stream: dict[str, int]
    turns: int
    turns_complete: bool
    artifacts: dict[str, Any]
    evidence_sha256: str


def stream_turn_facts(raw: bytes) -> dict[str, int]:
    """Count native ``turn.*`` events in a retained JSONL stream (no model call)."""
    facts = {"turn_completed": 0, "turn_started": 0, "turn_failed": 0, "malformed_lines": 0}
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            facts["malformed_lines"] += 1
            continue
        kind = event.get("type") if isinstance(event, dict) else None
        if kind == "turn.completed":
            facts["turn_completed"] += 1
        elif kind == "turn.started":
            facts["turn_started"] += 1
        elif kind == "turn.failed":
            facts["turn_failed"] += 1
    return facts


def _enforced_route(spec: Mapping[str, Any], row: Mapping[str, Any], completion_config: Mapping[str, Any]) -> dict:
    """Routing facts the reservation enforced before transport started.

    ``shell_reserve`` moves a receipt to ``transport_started`` only after
    ``enforce_route`` accepted the executed spec, so the reserved route is the
    requested route that ran. A route the build argv or the pass's completion
    config states must agree with it under the same native check. This is the
    requested route, never served identity.
    """
    from . import review_cycle_dispatch as dispatch  # noqa: PLC0415

    required = row.get("required_route")
    backend = spec.get("backend")
    if not isinstance(required, dict) or not isinstance(backend, str) or not backend:
        raise EvidenceHold("route_unavailable")
    argv = [str(item) for item in spec.get("argv") or []] if isinstance(spec.get("argv"), list) else []
    stated: list[tuple[list[str], Any]] = []
    if any(item in {"-m", "--model", "--effort"} for item in argv):
        stated.append((argv, backend))
    model, effort = completion_config.get("model"), completion_config.get("reasoning_effort")
    model = model if isinstance(model, str) and model else None
    effort = effort if isinstance(effort, str) and effort else None
    if model is not None or effort is not None:
        # An unset (adapter-default) part states nothing; a stated part must agree.
        stated.append((
            ["-m", model or str(required.get("model")), "--effort", effort or str(required.get("effort"))],
            completion_config.get("backend") or backend,
        ))
    try:
        for executed, executed_backend in stated:
            dispatch.enforce_route({"argv": executed, "backend_id": executed_backend}, row)
    except (ValueError, KeyError, TypeError) as exc:
        raise EvidenceHold("route_mismatch") from exc
    return {"backend": backend, "model": required.get("model"), "effort": required.get("effort"),
            "source": "review_cycle_reservation_enforced"}


def derive_native_terminal_evidence(
    *,
    job_id: str,
    rc: int,
    spec: Mapping[str, Any],
    receipt: Mapping[str, Any],
    artifact_paths: Mapping[str, Path],
    evidence_root: Path,
    completion_config: Mapping[str, Any],
    resolve_tree: Callable[[str], str],
) -> NativeTerminalEvidence:
    """Authenticate retained native evidence of one detached job, or hold.

    Identity: the receipt is this job's ``transport_started`` reservation and
    its source branch/head are the spec's. Evidence: the required retained
    artifacts are producer-owned regular files under ``evidence_root``; the
    source provenance passes the native validator against the receipt source;
    its nonce/ref is the host-retained ``native_dispatch``; it proves the model
    process started. Turns come from ``turn.completed`` events only.
    """
    from .adapters.remote_exec import _source_provenance_from_json  # noqa: PLC0415

    identity = receipt.get("identity")
    decision = receipt.get("decision")
    row = decision.get("row") if isinstance(decision, dict) else None
    if (
        not isinstance(identity, dict)
        or identity.get("dispatch_id") != job_id
        or receipt.get("state") != "transport_started"
        or not isinstance(decision, dict)
        or decision.get("status") != "reserved"
        or not isinstance(row, dict)
        or row.get("dispatch_id") != job_id
        or (row.get("subject_branch"), row.get("kind")) != (identity.get("subject_branch"), identity.get("kind"))
    ):
        raise EvidenceHold("dispatch_identity_mismatch")
    branch, head, tree = receipt.get("source_branch"), receipt.get("source_head"), receipt.get("source_tree")
    if not all(isinstance(value, str) and value for value in (branch, head, tree)) or branch != spec.get("branch"):
        raise EvidenceHold("dispatch_identity_mismatch")
    start_head = completion_config.get("start_head_ref")
    if start_head is not None and start_head != head:
        raise EvidenceHold("pass_identity_mismatch")
    route = _enforced_route(spec, row, completion_config)

    # Retained artifacts named by the persisted build argv (GRPH-14 lineage).
    producer = receipt.get("producer_artifacts")
    if isinstance(producer, dict):
        for name in ("result", "provenance", "patch"):
            named = producer.get(name)
            if named and (name not in artifact_paths or Path(named) != Path(artifact_paths[name])):
                raise EvidenceHold("evidence_path_identity_invalid", str(named))
    contents: dict[str, bytes] = {}
    manifest: dict[str, Any] = {}
    for name in sorted(artifact_paths):
        path = Path(artifact_paths[name])
        try:
            data = regular_file_bytes(path, root=evidence_root)
        except FileNotFoundError:
            manifest[name] = None
            continue
        except OSError as exc:
            raise EvidenceHold("evidence_path_identity_invalid", str(path)) from exc
        contents[name] = data
        manifest[name] = {"path": str(path), "sha256": sha256_hex(data), "bytes": len(data)}
    for name in REQUIRED_ARTIFACTS:
        if name not in contents:
            raise EvidenceHold("retained_evidence_missing", str(artifact_paths.get(name, f"--{name}-out")))

    # The native source validator, bound to the host receipt and the object store.
    if resolve_tree(head) != tree:
        raise EvidenceHold("source_provenance_identity_mismatch")
    proof, failure = _source_provenance_from_json(
        Path(artifact_paths["provenance"]), branch=branch, head_sha=head, head_tree=tree
    )
    if proof is None:
        raise EvidenceHold(
            "retained_evidence_missing" if failure == "missing" else f"source_provenance_{failure or 'malformed'}"
        )
    try:
        retained = json.loads(contents["provenance"])
    except ValueError as exc:
        raise EvidenceHold("stale_evidence_sha256") from exc
    if retained != proof:
        # The validated bytes are not the bytes this derivation hashed.
        raise EvidenceHold("stale_evidence_sha256")
    native = receipt.get("native_dispatch")
    if not isinstance(native, dict) or native != {
        "dispatch_nonce": proof.get("dispatch_nonce"),
        "dispatch_ref": proof.get("dispatch_ref"),
    }:
        raise EvidenceHold("native_dispatch_mismatch")
    if proof.get("model_process_started") is not True:
        raise EvidenceHold("model_process_not_started")

    stream = stream_turn_facts(contents["stream"])
    turns = stream["turn_completed"]
    turns_complete = (
        stream["malformed_lines"] == 0 and stream["turn_failed"] == 0 and stream["turn_started"] <= turns
    )
    artifacts = {"schema": EVIDENCE_SCHEMA, "artifacts": manifest}
    return NativeTerminalEvidence(
        job_id=job_id,
        rc=rc,
        backend=route["backend"],
        route=route,
        review_cycle={
            "dispatch_id": job_id,
            "subject_branch": identity["subject_branch"],
            "kind": identity["kind"],
            "round": row.get("round"),
        },
        native_dispatch={"dispatch_nonce": native["dispatch_nonce"], "dispatch_ref": native["dispatch_ref"]},
        source={
            "branch": branch,
            "head_sha": head,
            "head_tree": tree,
            "remote_source_commit": proof["remote_source_commit"],
            "sandbox_base_commit": proof["sandbox_base_commit"],
            "history_stripped": proof["history_stripped"],
        },
        stream=stream,
        turns=turns,
        turns_complete=turns_complete,
        artifacts=manifest,
        evidence_sha256=canonical_sha256(artifacts),
    )


def thin_terminal_base(result: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in result.items() if key not in ENRICHMENT_KEYS}


def enriched_terminal_result(base: Mapping[str, Any], evidence: NativeTerminalEvidence) -> dict[str, Any]:
    """Project authenticated history onto a terminal receipt; never certify more.

    Paid usage is explicitly unknown (``token_accounting: "unknown"``,
    ``cumulative_total: None``, ``tokens_complete: False``). The requested route
    is recorded as requested, never as served model identity. No review ran on
    an execute-stage failure, so rounds/high findings are zero; the outcome,
    ``ok`` and merge facts of the original receipt are untouched.
    """
    counts = {"turns": evidence.turns, "rounds": 0, "high_finding_count": 0}
    pass_history: dict[str, Any] = {**counts, "routing_history": []}
    if not evidence.turns_complete:
        pass_history["counts_complete"] = False
    receipt = {
        "backend_id": evidence.backend,
        "resolved_model": None,
        "served_model": None,
        "tracked_pin": None,
        "pin_source": "unavailable",
        "gate_host": None,
        "observed_backend": evidence.backend,
        "tier": None,
        "originating_tier": None,
        "routing_effective": None,
        "routing_requested": dict(evidence.route),
        **counts,
        "escalated_from": None,
        "cost_per_accepted_output": None,
        "observed_tokens": None,
        "routing_history": [],
        "tokens_complete": False,
        "pass_history": pass_history,
        "model_process_started": True,
        "process_start_source": "native_source_provenance",
    }
    return {
        **thin_terminal_base(base),
        "dispatch_receipt": receipt,
        "tokens": {
            "cumulative_total": None,
            "tokens_complete": False,
            "usage_source": "unavailable",
            "reason": "native_usage_not_retained",
        },
        "token_accounting": "unknown",
        "terminal_evidence": {
            "status": "authenticated",
            "schema": EVIDENCE_SCHEMA,
            "job_id": evidence.job_id,
            "rc": evidence.rc,
            "evidence_sha256": evidence.evidence_sha256,
            "artifacts": evidence.artifacts,
            "model_process_started": True,
            "native_dispatch": dict(evidence.native_dispatch),
            "source": dict(evidence.source),
            "stream": dict(evidence.stream),
            "review_cycle": dict(evidence.review_cycle),
        },
    }


# ---------------------------------------------------------------------------
# Durable enrichment intent (one content-addressed file per approved job)
# ---------------------------------------------------------------------------


def intent_dir(state_dir: Path) -> Path:
    """Under the native RemoteJobLedger root, beside ``ledger/``."""
    return Path(state_dir) / "remote-jobs" / INTENT_DIRNAME


def intent_path(state_dir: Path, job_id: str, digest: str) -> Path:
    return intent_dir(state_dir) / f"{job_id}.{digest}.json"


def read_enrichment_intents(state_dir: Path, job_id: str) -> list[tuple[Path, str, dict[str, Any]]] | None:
    """Every intent for ``job_id``; ``None`` when any candidate is invalid (fail closed).

    A candidate must be in protected producer custody (no link, this uid, no
    group/world bits), content-addressed, canonical, and the complete
    versioned shape. Shape is not authority: readers still authenticate the
    native records it names (``terminal_history_contract.authenticated_result``).
    """
    from . import terminal_history_contract as contract  # noqa: PLC0415

    if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
        return None
    root = intent_dir(state_dir)
    if not root.is_dir():
        return []
    found: list[tuple[Path, str, dict[str, Any]]] = []
    try:
        candidates = sorted(root.glob(f"{job_id}.*.json"))
    except OSError:
        return None
    for path in candidates:
        digest = path.name[len(job_id) + 1 : -len(".json")]
        if not contract.protected_custody(path, root):
            return None
        try:
            raw = regular_file_bytes(path, root=root, limit=_MAX_INTENT_BYTES)
            intent = json.loads(raw)
        except (OSError, ValueError, EvidenceHold):
            return None
        if (
            _SHA256.fullmatch(digest) is None
            or sha256_hex(raw) != digest
            or raw != canonical_bytes(intent)
            or not contract.well_formed(intent, job_id)
        ):
            return None
        found.append((path, digest, intent))
    return found


def applied_enrichment(
    state_dir: Path, job_id: str, *, receipt_sha256: str
) -> tuple[Path, str, dict[str, Any]] | None:
    """The single valid intent bound to the exact original receipt bytes, else None."""
    found = read_enrichment_intents(state_dir, job_id)
    if not found or len(found) != 1:
        return None
    path, digest, intent = found[0]
    if intent["original_receipt"].get("sha256") != receipt_sha256:
        return None
    return path, digest, intent


def approved_terminal_result(
    state_dir: Path, *, pass_id: str, job_id: str, recorded: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Read-only cached view: the authenticated enrichment of ``recorded``, else None.

    The single intent for the job is an index only. The view is served when
    every native record it names re-validates (exact original pass bytes and
    identity, first-publication lane cycle, terminal job record, dispatch
    receipt nonce/source/route, the native settlement row, retained artifact
    digests) and the result re-derives exactly. Anything else keeps the thin
    receipt and its unknown usage (SECD-05). A read never writes.
    """
    from . import terminal_history_contract as contract  # noqa: PLC0415

    if (
        not isinstance(job_id, str)
        or _JOB_ID.fullmatch(job_id) is None
        or not isinstance(recorded, Mapping)
        or not intent_dir(state_dir).is_dir()
    ):
        return None
    found = read_enrichment_intents(state_dir, job_id)
    if not found or len(found) != 1:
        return None
    try:
        return contract.authenticated_result(Path(state_dir), found[0][2], pass_id=pass_id, job_id=job_id,
                                             recorded=dict(recorded))
    except (EvidenceHold, OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    except Exception:  # noqa: BLE001 — an unverifiable authority (e.g. git/canon unavailable) is no authority
        return None
