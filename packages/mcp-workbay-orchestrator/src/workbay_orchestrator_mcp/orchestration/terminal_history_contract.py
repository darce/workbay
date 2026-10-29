"""Versioned native authority for recovered terminal history (one record, replayable views).

``terminal_history_repair.apply`` is the only producer of an enrichment intent.
The intent is an index into native records, never proof by itself: a reader
serves its view only when every native record it names re-validates, and the
served result is re-derived by the same projection the producer used.

Named records (GRPH-14 lineage), each re-checked on read:

- the original thin pass receipt (exact bytes digest, identity, first-publication
  lane cycle binding) -- never rewritten;
- the RemoteJobLedger record of the same terminal job (stage, rc, spec identity);
- the host-retained review-cycle dispatch receipt (exact bytes digest, identity,
  native dispatch nonce/ref, source branch/head/tree, reserved route);
- the review-cycle ledger row settled by the native settlement producer;
- the retained artifacts of the evidence manifest, each at the locator the
  terminal job's own build argv names (re-hashed; stream recounted).

Custody: the intent is a regular, non-symlink file owned by this uid with no
group/world permission bits, in a directory without group/world write.

Canon 0.26.2: DATA-14 no dual writes (CARD-02 correction at source); GRPH-14
lineage; SECD-05 fail-safe defaults (anything unproven keeps the thin receipt
and unknown usage); AGT-21 typed outcomes.
"""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import offload_pass_recovery as recovery

INTENT_SCHEMA = recovery.INTENT_SCHEMA
PRODUCER = "terminal_history_repair.apply"
INTENT_MODE = 0o600
INTENT_KEYS = frozenset({
    "schema", "producer", "job_id", "pass_id", "task_ref", "lane_id", "attempt", "canonical",
    "terminal_revision", "evidence_sha256", "evidence", "original_receipt", "dispatch_receipt",
    "settlement", "result",
})
CANONICAL_KEYS = frozenset({
    "job_id", "pass_id", "task_ref", "lane_id", "attempt", "branch", "rc", "lane_cycle",
    "native_dispatch", "source", "route", "review_cycle",
})
EVIDENCE_KEYS = frozenset({"schema", "artifacts", "stream", "turns", "turns_complete"})
_IDENTITY = ("job_id", "pass_id", "task_ref", "lane_id", "attempt")


def evidence_facts(evidence: recovery.NativeTerminalEvidence) -> dict[str, Any]:
    """The evidence facts an intent binds (manifest digest is ``evidence_sha256``)."""
    return {
        "schema": recovery.EVIDENCE_SCHEMA,
        "artifacts": evidence.artifacts,
        "stream": dict(evidence.stream),
        "turns": evidence.turns,
        "turns_complete": evidence.turns_complete,
    }


def build_intent(
    *,
    canonical: Mapping[str, Any],
    terminal_revision: str,
    evidence: recovery.NativeTerminalEvidence,
    original_receipt: Mapping[str, str],
    dispatch_receipt: Mapping[str, str],
    settlement: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """The one complete versioned intent (no clock: replay is byte-identical)."""
    return {
        "schema": INTENT_SCHEMA,
        "producer": PRODUCER,
        **{key: canonical[key] for key in _IDENTITY},
        "canonical": dict(canonical),
        "terminal_revision": terminal_revision,
        "evidence_sha256": evidence.evidence_sha256,
        "evidence": evidence_facts(evidence),
        "original_receipt": dict(original_receipt),
        "dispatch_receipt": dict(dispatch_receipt),
        "settlement": dict(settlement),
        "result": dict(result),
    }


def well_formed(intent: object, job_id: str) -> bool:
    """Exact versioned shape; values are authenticated separately against native records."""
    if not isinstance(intent, dict) or set(intent) != INTENT_KEYS:
        return False
    canonical, evidence = intent.get("canonical"), intent.get("evidence")
    return (
        intent["schema"] == INTENT_SCHEMA
        and intent["producer"] == PRODUCER
        and intent["job_id"] == job_id
        and isinstance(canonical, dict)
        and set(canonical) == CANONICAL_KEYS
        and all(canonical[key] == intent[key] for key in _IDENTITY)
        and isinstance(evidence, dict)
        and set(evidence) == EVIDENCE_KEYS
        and all(isinstance(intent[key], dict) for key in ("original_receipt", "dispatch_receipt", "settlement", "result"))
        and type(intent["attempt"]) is int
    )


def protected_custody(path: Path, root: Path) -> bool:
    """Producer custody: no link, this uid, no group/world bits (directory: no group/world write)."""
    try:
        info, parent = os.lstat(path), os.lstat(root)
    except OSError:
        return False
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and not info.st_mode & 0o077
        and stat.S_ISDIR(parent.st_mode)
        and parent.st_uid == os.getuid()
        and not parent.st_mode & 0o022
        and Path(os.path.realpath(path.parent)) == Path(os.path.realpath(root))
    )


def evidence_from_intent(intent: Mapping[str, Any]) -> recovery.NativeTerminalEvidence:
    canonical, evidence = intent["canonical"], intent["evidence"]
    return recovery.NativeTerminalEvidence(
        job_id=canonical["job_id"],
        rc=canonical["rc"],
        backend=canonical["route"]["backend"],
        route=dict(canonical["route"]),
        review_cycle=dict(canonical["review_cycle"]),
        native_dispatch=dict(canonical["native_dispatch"]),
        source=dict(canonical["source"]),
        stream=dict(evidence["stream"]),
        turns=evidence["turns"],
        turns_complete=evidence["turns_complete"],
        artifacts=evidence["artifacts"],
        evidence_sha256=intent["evidence_sha256"],
    )


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise recovery.EvidenceHold(reason)


def _verify_pass(state_dir: Path, intent: Mapping[str, Any], recorded: Mapping[str, Any]) -> dict[str, Any]:
    canonical = intent["canonical"]
    pass_path = Path(state_dir) / f"offload-pass-{intent['pass_id']}.json"
    raw = recovery.regular_file_bytes(pass_path, limit=16 * 1024 * 1024)
    persisted = json.loads(raw)
    binding = persisted.get("lane_cycle_binding") if isinstance(persisted, dict) else None
    _require(
        isinstance(persisted, dict)
        and intent["original_receipt"] == {"path": str(pass_path), "sha256": recovery.sha256_hex(raw)}
        and persisted.get("status") == "done"
        and persisted.get("result") == recorded
        and persisted.get("job_id") == intent["job_id"]
        and persisted.get("pass_id", intent["pass_id"]) == intent["pass_id"]
        and (persisted.get("task_ref"), persisted.get("lane_id")) == (intent["task_ref"], intent["lane_id"])
        and type(persisted.get("attempt")) is int
        and persisted["attempt"] == intent["attempt"]
        and isinstance(binding, dict)
        and binding.get("v") == 1
        and (binding.get("task_ref"), binding.get("lane_id")) == (intent["task_ref"], intent["lane_id"])
        and {"row_id": binding.get("row_id"), "cycle_epoch": binding.get("cycle_epoch")} == canonical["lane_cycle"]
        and isinstance(persisted.get("worktree_path"), str),
        "original_receipt_unbound",
    )
    return persisted


def _verify_job(state_dir: Path, intent: Mapping[str, Any]) -> Any:
    """The terminal job record of the intent's identity; returns its build argv (the artifact locators)."""
    from .remote_job_ledger import RemoteJobLedger  # noqa: PLC0415

    canonical = intent["canonical"]
    job = json.loads(recovery.regular_file_bytes(RemoteJobLedger(Path(state_dir) / "remote-jobs")._job_path(intent["job_id"])))
    spec = job.get("spec") if isinstance(job, dict) else None
    _require(
        isinstance(spec, dict)
        and job.get("stage") in {"done", "completed"}
        and job.get("remote_state") != "lost"
        and type(job.get("rc")) is int
        and job["rc"] not in (0, 75)
        and job["rc"] == canonical["rc"]
        and all(spec.get(key) == intent[key] for key in _IDENTITY)
        and spec.get("branch") == canonical["branch"],
        "job_record_unbound",
    )
    return spec.get("argv")


def _verify_dispatch(worktree: Path, intent: Mapping[str, Any]) -> None:
    from . import review_cycle_dispatch as dispatch  # noqa: PLC0415
    from .review_cycle_ledger import ReviewCycleLedger  # noqa: PLC0415

    canonical, settlement = intent["canonical"], intent["settlement"]
    path, receipt, raw = recovery.read_dispatch_receipt(worktree, intent["job_id"])
    decision = receipt.get("decision")
    reserved = decision.get("row") if isinstance(decision, dict) else None
    source = canonical["source"]
    route = canonical["route"]
    _require(
        intent["dispatch_receipt"] == {"path": str(path), "sha256": recovery.sha256_hex(raw)}
        and isinstance(reserved, dict)
        and {key: (receipt.get("identity") or {}).get(key) for key in ("dispatch_id", "subject_branch", "kind")}
        == settlement.get("identity")
        and receipt.get("native_dispatch") == canonical["native_dispatch"]
        and (receipt.get("source_branch"), receipt.get("source_head"), receipt.get("source_tree"))
        == (source.get("branch"), source.get("head_sha"), source.get("head_tree"))
        and isinstance(reserved.get("required_route"), dict)
        and (reserved["required_route"].get("model"), reserved["required_route"].get("effort"))
        == (route.get("model"), route.get("effort")),
        "dispatch_receipt_unbound",
    )
    ledger = ReviewCycleLedger(dispatch.authority(worktree) / "review_cycle_ledger.json", dispatch.trusted_policy())
    row = ledger._read()["rows"].get(intent["job_id"])
    _require(
        isinstance(row, dict)
        and row.get("decision") == "reserved"
        and row.get("completion") is not None
        and row["completion"] == settlement.get("completion")
        and row.get("round") == settlement.get("round")
        and all(row.get(key) == reserved.get(key) for key in ("dispatch_id", "subject_branch", "kind", "required_route")),
        "settlement_unproven",
    )


def _verify_evidence(state_dir: Path, intent: Mapping[str, Any], locators: Mapping[str, Path]) -> None:
    """``locators`` are the original job's artifact paths: the manifest names exactly those, never its own."""
    evidence = intent["evidence"]
    manifest = evidence["artifacts"]
    _require(
        evidence["schema"] == recovery.EVIDENCE_SCHEMA
        and isinstance(manifest, dict)
        and set(manifest) == set(locators)
        and intent["evidence_sha256"]
        == recovery.canonical_sha256({"schema": recovery.EVIDENCE_SCHEMA, "artifacts": manifest})
        and all(isinstance(manifest.get(name), dict) for name in recovery.REQUIRED_ARTIFACTS),
        "stale_evidence_sha256",
    )
    contents: dict[str, bytes] = {}
    for name, entry in manifest.items():
        if entry is None:
            continue
        path = Path(locators[name])
        _require(isinstance(entry, dict) and entry.get("path") == str(path), "evidence_path_identity_invalid")
        data = recovery.regular_file_bytes(path, root=Path(state_dir))
        _require(entry == {"path": str(path), "sha256": recovery.sha256_hex(data), "bytes": len(data)},
                 "stale_evidence_sha256")
        contents[name] = data
    stream = recovery.stream_turn_facts(contents["stream"])
    turns_complete = stream["malformed_lines"] == 0 and stream["turn_failed"] == 0 and stream["turn_started"] <= stream["turn_completed"]
    _require(
        evidence["stream"] == stream
        and evidence["turns"] == stream["turn_completed"]
        and evidence["turns_complete"] is turns_complete,
        "stale_evidence_sha256",
    )


def authenticated_result(
    state_dir: Path, intent: Mapping[str, Any], *, pass_id: str, job_id: str, recorded: Mapping[str, Any]
) -> dict[str, Any]:
    """Re-validate every native record the intent names; return the re-derived view or hold."""
    from .offload_pass import _detached_artifact_paths  # noqa: PLC0415

    _require(well_formed(intent, job_id) and intent["pass_id"] == pass_id, "enrichment_intent_invalid")
    persisted = _verify_pass(Path(state_dir), intent, recorded)
    argv = _verify_job(Path(state_dir), intent)
    worktree = Path(persisted["worktree_path"]).expanduser()
    _verify_dispatch(worktree, intent)
    # The same locator derivation the producer (terminal_history_repair.apply) used.
    _verify_evidence(Path(state_dir), intent, _detached_artifact_paths(argv, worktree.resolve()))
    derived = recovery.enriched_terminal_result(recorded, evidence_from_intent(intent))
    _require(derived == intent["result"], "enrichment_result_not_derived")
    return json.loads(json.dumps(derived))
