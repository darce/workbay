"""Live D3B dispatch boundary over the D3A ledger and canonical D2 classifier.

Canon 0.26.2 FM-08 (AI Engineering): reserve before transport; partial/unknown
model outcomes charge. No task-lifetime counters or caller budget overrides.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
from contextvars import ContextVar
from dataclasses import asdict
from pathlib import Path

from .review_cycle_ledger import LedgerDecision, ReviewCycleLedger, TrustedPolicy, _Hold

# Operator-supplied immutable release, overriding the older repository consumer
# pin for this integration. This is a release digest, never proposal-derived.
_CANON_DIGEST = "46636aa71f2e2e0c413b6ccff1cae4959543026c6ee7e4abde2bc7f30645a260"
_PUBLISHED_CANON_LINK = Path("Development/heuristics-canon-current")
ACTIVE_DISPATCH: ContextVar[dict | None] = ContextVar("review_cycle_dispatch", default=None)
# Gate-authored collection artifacts that exist before the model boundary. Any
# other collected artifact (result, stream, debug, self-verify, remainder) is
# model output and keeps the attempt charged.
_PRE_MODEL_ARTIFACTS = frozenset({"phases-out", "provenance-out"})
# Execution facts a pre-model refusal record never carries. Present at all,
# even as null or zero ("unknown"), they are not proof that no model ran.
_EXECUTION_FACTS = frozenset({"process_ran", "served_model", "observed_model", "observed_backend", "resolved_model",
                              "routing_effective", "turns", "rounds", "observed_tokens", "tokens", "job_id"})
# Host-native phases that exit before any model boundary, with their typed
# refusal reason. Only these positive proofs may classify an infra stop.
NATIVE_PRE_MODEL_SCHEMA = "workbay/native-pre-model-refusal@1"
_NATIVE_PRE_MODEL_REASONS = {
    "history": "native_history_incomplete",  # persisted pass history incomplete: no turn starts
    "attestation": "native_attestation_refused",  # capability mint refused before worker setup
    # The shell bridge refused the engine's one-use transfer before any
    # transport; the typed bridge reason is bound as ``detail``.
    "shell_reserve": "native_shell_reserve_refused",
}
_DETAIL_PHASES = frozenset({"shell_reserve"})
_DETAIL_LIMIT = 256


def _git(repo: Path, *args: str) -> str:
    env = {key: value for key, value in os.environ.items()
           if key not in {"GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}}
    result = subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True,
                            check=True, timeout=5, env=env)
    return result.stdout.strip()


def authority(repo: Path) -> Path:
    return Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir")) / "workbay"


def _trusted_canon_root() -> Path:
    """Deployment canon root: process environment, else the published host link.

    An explicit value, even an empty or bad one, is final and never falls back.
    Only an absent variable may use the documented deployment default, the
    publish hook's link to its newest read-only verified release folder
    (docs/upstream-requests/REQUEST-canon-pointer-must-reach-every-harness-
    through-global-instructions-not-per-harness-user-files-20261004.md, item 4).
    Spec env, tool arguments and proposals are never consulted.
    """
    configured = os.environ.get("WORKBAY_HEURISTICS_CANON_ROOT")
    if configured is not None:
        if not configured:
            raise ValueError("trusted_canon_root_unavailable")
        return Path(configured).resolve(strict=True)
    try:
        link = Path.home() / _PUBLISHED_CANON_LINK
        if not link.is_symlink():
            raise ValueError("trusted_canon_root_unavailable")
        root = link.resolve(strict=True)
        published = (root, root / "meta/release-manifest.json")
        if any(path.stat().st_mode & 0o222 for path in published):
            raise ValueError("trusted_canon_published_link_writable")
    except (OSError, RuntimeError) as exc:
        raise ValueError("trusted_canon_root_unavailable") from exc
    return root


def trusted_policy() -> TrustedPolicy:
    # Only deployment authority (see _trusted_canon_root). Never read spec.env.
    root = _trusted_canon_root()
    raw = (root / "meta/release-manifest.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != _CANON_DIGEST:
        raise ValueError("trusted_canon_manifest_mismatch")
    manifest = json.loads(raw)
    if manifest["schema"] != "heuristics-canon/release@5" or manifest["version"] != "0.26.2":
        raise ValueError("trusted_canon_version_mismatch")
    rules = []
    for relative, entry in manifest["lexicons"].items():
        path = (root / relative).resolve(strict=True)
        if not path.is_relative_to(root) or not relative.startswith("lexicons/"):
            raise ValueError("trusted_canon_path_invalid")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            raise ValueError("trusted_canon_lexicon_mismatch")
        actual = re.findall(rb"^\| ([A-Z][A-Z0-9]*-[0-9]+)(?:<|\s)", content, re.M)
        ids = [item.decode() for item in actual]
        if sorted(ids) != sorted(entry["rule_ids"]) or len(ids) != entry["rules"]:
            raise ValueError("trusted_canon_rule_index_mismatch")
        rules.extend(ids)
    if (len(rules) != manifest["rules"] or len(set(rules)) != len(rules)
            or sorted(rules) != sorted(manifest["rule_ids"])):
        raise ValueError("trusted_canon_rule_index_mismatch")
    return TrustedPolicy(manifest["version"], _CANON_DIGEST, frozenset(rules))


def runtime_ledger(repo: Path) -> ReviewCycleLedger:
    policy = trusted_policy()  # validate before creating any authority files
    root = authority(repo)
    root.mkdir(parents=True, exist_ok=True)
    ledger = ReviewCycleLedger(root / "review_cycle_ledger.json", policy)
    activated = ledger.activate()  # idempotent, no import of historical counts
    if activated.status != "activated":
        raise ValueError(activated.reason)
    return ledger


def dispatch(request: dict, *, repo: Path | None = None) -> LedgerDecision | dict:
    """Frozen stdin protocol; root/policy/threshold are never request fields."""
    try:
        operation = request["operation"]
        if operation in {"inspect_unjudged", "settle_unjudged"}:
            return unjudged_gate(request, repo=repo or Path.cwd())
        allowed = {"operation", "dispatch_id", "subject_branch", "kind"}
        allowed |= ({"canon_proposal_path"} if operation == "reserve" else
                    {"outcome", "refusal_reason", "blocking_ids", "blocking_fingerprints"})
        if operation not in {"reserve", "settle"} or set(request) - allowed:
            return LedgerDecision("ledger_hold", "invalid_protocol")
        ledger = runtime_ledger(repo or Path.cwd())
        identity = {key: request[key] for key in ("dispatch_id", "subject_branch", "kind")}
        if not all(isinstance(value, str) and value for value in identity.values()):
            return LedgerDecision("ledger_hold", "invalid_identity")
        if operation == "reserve":
            _import_legacy_sources(repo or Path.cwd(), ledger, identity["subject_branch"])
            proposal = request.get("canon_proposal_path")
            return ledger.reserve(**identity, canon_proposal_path=Path(proposal) if proposal else None)
        return ledger.settle(**identity, **{key: value for key, value in request.items()
                                          if key not in {"operation", *identity}})
    except (_Hold, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        return LedgerDecision("ledger_hold", str(exc))


def _import_legacy_sources(repo: Path, ledger: ReviewCycleLedger, subject: str) -> None:
    """Join B18's canonical migration before reserving any fresh model call.

    An unjoined checkout may run new-work component tests, but cannot start a
    known legacy work unit without its migration adapter and producer evidence.
    Existing native subjects retain their rows and pending gates unchanged.
    """
    from . import review_cycle_ledger

    adapter = getattr(review_cycle_ledger, "import_legacy_source_history", None)
    if adapter is not None:
        result = adapter(repo, ledger, subject)
        if isinstance(result, LedgerDecision) and result.status == "ledger_hold":
            raise ValueError(result.reason)
        if isinstance(result, dict) and (result.get("ok") is False or result.get("status") in {"ledger_hold", "unavailable", "held"}):
            raise ValueError(result.get("reason") or "review_cycle_legacy_import_unavailable")
        return
    if any(row["subject_branch"] == subject for row in ledger._read()["rows"].values()):
        return
    from .review_ratchet import verified_legacy_source_history

    if verified_legacy_source_history(repo, subject):
        raise ValueError("review_cycle_legacy_import_unavailable")


def _receipt_path(repo: Path, dispatch_id: str) -> Path:
    root = authority(repo) / "review_cycle_dispatch"
    root.mkdir(parents=True, exist_ok=True)
    return root / (hashlib.sha256(dispatch_id.encode()).hexdigest() + ".json")


def _save(path: Path, value: dict) -> None:
    # The sibling kernel lock is held by the caller. Reuse the canonical atomic
    # writer used by engine pass receipts (fsync + replacement).
    from .pass_rescue import _atomic_write
    _atomic_write(path, json.dumps(value, sort_keys=True))


def prepare(repo: Path, *, dispatch_id: str, subject_branch: str, kind: str,
            canon_proposal_path: str | None = None, settlement_owner: str = "shell",
            managed_identity: dict | None = None) -> dict:
    scope = source_review_scope(repo, dict(branch=_git(repo, "symbolic-ref", "--short", "HEAD"),
                                          head_sha=_git(repo, "rev-parse", "HEAD")), subject_branch) if kind == "gate" else None
    decision = dispatch(dict(operation="reserve", dispatch_id=dispatch_id, subject_branch=subject_branch,
                             kind=kind, canon_proposal_path=canon_proposal_path), repo=repo)
    receipt = {"decision": asdict(decision), "identity": dict(dispatch_id=dispatch_id,
                subject_branch=subject_branch, kind=kind)}
    if decision.status != "reserved" or decision.reason == "replay":
        if decision.reason == "replay":
            receipt["decision"] = asdict(LedgerDecision("ledger_hold", "dispatch_already_reserved", decision.row))
        return receipt
    path = _receipt_path(repo, dispatch_id)
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        receipt.update(token=secrets.token_hex(24), state="prepared", settlement_owner=settlement_owner)
        if managed_identity is not None:
            # Host-owned identity frozen before the adapter is entered. Neither
            # worker output nor a later HEAD may supply this binding.
            receipt["managed_identity"] = dict(managed_identity)
            # Keep this request snapshot separate from shell-produced source_*
            # lineage: merely preparing a pre-model refusal is not a source call.
            receipt["managed_source"] = dict(branch=_git(repo, "symbolic-ref", "--short", "HEAD"),
                                             head_sha=_git(repo, "rev-parse", "HEAD"),
                                             head_tree=_git(repo, "rev-parse", "HEAD^{tree}"))
        if scope is not None:
            receipt["source_review_scope"] = scope
        _save(path, receipt)
    return receipt


def _model_or_unknown_evidence(record: object) -> str | None:
    """Name the model-start or execution fact in a pre-model record, else None.

    The one negative-evidence predicate shared by the receiver, settlement's
    reader and host reconciliation. ``model_process_started``/``model_started``
    must be absent or exactly ``False``; a ``dispatch_receipt`` carrying any
    execution or unknown fact dominates every refusal marker (SECD-05).
    """
    if not isinstance(record, dict):
        return "record"
    for key in ("model_process_started", "model_started"):
        if key in record and record[key] is not False:
            return key
    if "dispatch_receipt" in record:
        executed = record["dispatch_receipt"]
        if not isinstance(executed, dict) or _EXECUTION_FACTS & set(executed):
            return "dispatch_receipt"
    return None


def _adapter_refusal_reason(receipt: dict, raw: dict) -> str | None:
    """Validate constructor evidence, never prose, a timeout or a false marker.

    The receiver strips adapter_refusal from model output (including nested
    raw_payload). Only the trusted adapter's failed capability constructor can
    supply this conjunction. The protected host request supplies identity.
    Any model-start or execution fact refuses the proof outright.
    Canon 0.26.2 SECD-03 (Security Engineering): check on every consumption.
    """
    try:
        if _model_or_unknown_evidence(raw) is not None:
            return None
        refusal = raw["pre_dispatch_refusal"]
        reason = refusal["reason"]
        source = raw["selected_source"]
        if (receipt["state"] != "prepared" or receipt["settlement_owner"] != "engine"
                or not isinstance(reason, str) or not reason or refusal["outcome"] != "dispatch_refused"
                or raw.get("typed_outcome") is not True or raw.get("adapter_refusal") is not True
                or raw.get("retryable") is not False
                or raw.get("refusal_cause") != "policy_or_classifier_refusal"
                or raw.get("failure_reason") != reason
                or raw.get("attestation") != {"status": "failed", "reason": reason}
                or raw.get("review_cycle") != receipt["decision"]
                or raw.get("consumed_source") is not None
                or raw.get("uncommitted_remainder_present") is not False
                or any(key in raw for key in ("handoff_action", "findings", "source_provenance", "job_id"))
                or (source["branch"], source["head_sha"], source["head_tree"]) != (
                    receipt["managed_source"]["branch"], receipt["managed_source"]["head_sha"],
                    receipt["managed_source"]["head_tree"])
                or source["transport_source"] != "worktree"
                or Path(source["transport_path"]).resolve() != (
                    Path(receipt["managed_identity"]["worktree_path"]) / "scripts/remote_agent.sh").resolve()
                or re.fullmatch(r"[0-9a-f]{64}", source["transport_digest"]) is None):
            return None
        return reason
    except (KeyError, TypeError, AttributeError):
        return None


def _record_adapter_refusal(repo: Path, result_path: Path, managed_identity: dict) -> None:
    """Trusted receiver write-ahead observation, before handoff normalization.

    Not a public protocol operation. An ACTIVE_DISPATCH token, frozen task/lane/
    pass/source binding and actual constructor evidence are all required. Keep
    the evidence in Git-common-dir authority, outside the remote worker tree.
    No existing completion or observation may be replaced.
    """
    active = ACTIVE_DISPATCH.get()
    if active is None or active.get("managed_identity") != managed_identity:
        return
    encoded = result_path.read_bytes()
    result = json.loads(encoded)
    raw = result.get("raw_payload")
    if not isinstance(raw, dict) or _adapter_refusal_reason(active, raw) is None:
        return
    if _git(repo, "rev-parse", "HEAD") != active["managed_source"]["head_sha"]:
        return
    # Actual judgment artifacts dominate the constructor marker. Unreadable
    # evidence raises, rather than being reinterpreted as an empty result.
    model_result = Path(raw["result_path"])
    if model_result.is_file() and model_result.stat().st_size:
        return
    if result.get("merge_ready") or result.get("changed_files") or result.get("findings"):
        return
    path = _receipt_path(repo, active["identity"]["dispatch_id"])
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt = json.loads(path.read_bytes())
        prior = receipt.get("adapter_pre_dispatch_refusal")
        base = {key: value for key, value in receipt.items() if key != "adapter_pre_dispatch_refusal"}
        expected = {key: value for key, value in active.items() if key != "adapter_pre_dispatch_refusal"}
        if base != expected:
            raise ValueError("adapter_refusal_dispatch_changed")
        evidence = dict(schema="workbay/adapter-pre-dispatch-refusal@1",
                        managed_identity=managed_identity, envelope_path=str(result_path),
                        envelope_sha256=hashlib.sha256(encoded).hexdigest(), envelope_json=encoded.decode())
        if prior is not None:
            if prior != evidence:
                raise ValueError("adapter_refusal_observation_changed")
            return
        receipt["adapter_pre_dispatch_refusal"] = evidence
        _save(path, receipt)


def _native_pre_model_proof(receipt: dict, phase: str, detail: str | None = None) -> dict:
    """The exact proof a host-native pre-model exit binds to its reservation.

    Only the shell-bridge phase carries ``detail``: its own typed refusal reason.
    """
    proof = dict(schema=NATIVE_PRE_MODEL_SCHEMA, phase=phase, reason=_NATIVE_PRE_MODEL_REASONS[phase],
                 model_boundary_reached=False, identity=receipt["identity"], reservation=receipt["decision"],
                 managed_identity=receipt["managed_identity"],
                 token_sha256=hashlib.sha256(receipt["token"].encode()).hexdigest())
    if phase in _DETAIL_PHASES:
        if not isinstance(detail, str) or not detail or len(detail) > _DETAIL_LIMIT:
            raise ValueError("native_pre_model_refusal_detail_invalid")
        proof["detail"] = detail
    elif detail is not None:
        raise ValueError("native_pre_model_refusal_detail_invalid")
    return proof


def _native_pre_model_reason(receipt: dict, proof: object) -> str | None:
    """Validate a positive native pre-model proof against its own receipt.

    Only a still-``prepared`` engine-owned managed reservation (no transport,
    native dispatch, completion or adapter observation) qualifies; the proof
    must bind that receipt's identity, reservation, managed identity and token.
    """
    try:
        phase = proof["phase"]
        if (phase not in _NATIVE_PRE_MODEL_REASONS
                or proof != _native_pre_model_proof(receipt, phase, proof.get("detail"))
                or receipt["state"] != "prepared" or receipt["settlement_owner"] != "engine"
                or receipt["decision"]["status"] != "reserved"
                or any(key in receipt for key in ("adapter_pre_dispatch_refusal", "source_branch", "source_head",
                                                  "source_tree", "native_dispatch", "native_completion"))
                or _model_or_unknown_evidence(receipt) is not None):
            return None
        return proof["reason"]
    except (KeyError, TypeError, AttributeError, ValueError):
        return None


REVIEW_HISTORY = "review"
NONREVIEW_HISTORY = "nonreview"


def review_history_type(repo: Path, state_dir: Path, record: dict, *, source: str) -> str:
    """Read engine history without changing receipts, locks, or accounting.

    Only typed implementation results or a refusal joined to its protected
    producer are nonreview. Unknown review history retains the ratchet holds.
    Canon 0.26.2 SECD-05 (lexicons/security.md:268, Security Engineering),
    GRPH-14 (lexicons/graph-theory.md:117, Building Knowledge Graphs ch. 8).
    """
    from .offload_pass import read_pass_state
    from .review_ratchet import RatchetRefused, review_pass_view

    contradiction = "review_ratchet_history_type_contradiction"
    unavailable = "review_ratchet_nonreview_proof_unavailable"

    def no_turn(harvest: object) -> bool:
        return (isinstance(harvest, dict) and harvest.get("status") == "skipped"
                and harvest.get("reason") == "no_review_turn"
                and harvest.get("salvage_suppressed") == "dispatch_refusal")

    if source == "receipt":
        if not no_turn(record.get("harvest")):
            return REVIEW_HISTORY
        identity = record.get("identity") or {}
        try:
            prior = review_pass_view(read_pass_state(state_dir, identity["pass_id"]))
            if any(not identity.get(key) or prior.get(key) != identity[key] for key in
                   ("task_ref", "lane_id", "pass_id", "branch", "base_sha", "tip_sha", "worktree_path")):
                raise ValueError("receipt_identity")
            if prior.get("findings_harvest") != record["harvest"]:
                raise ValueError("receipt_harvest")
            if review_history_type(repo, state_dir, prior, source="pass") != NONREVIEW_HISTORY:
                raise ValueError("receipt_pass")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            if isinstance(exc, RatchetRefused):
                raise
            raise RatchetRefused(unavailable) from exc
        return NONREVIEW_HISTORY
    if source != "pass":
        raise RatchetRefused(unavailable)

    inner = record.get("result")
    result = inner if isinstance(inner, dict) else {}
    verdict = result.get("harvest_verdict", record.get("harvest_verdict"))
    refusal = no_turn(record.get("findings_harvest"))
    if verdict != "not_attempted" and not refusal:
        return REVIEW_HISTORY
    if ("harvest_verdict" in record and "harvest_verdict" in result
            and record["harvest_verdict"] != result["harvest_verdict"]):
        raise RatchetRefused(contradiction)
    if any(key in record and key in result and record[key] != result[key] for key in
           ("task_ref", "lane_id", "pass_id", "branch", "base_sha", "tip_sha", "worktree_path")):
        raise RatchetRefused(contradiction)
    outer = record.get("review_cycle")
    final = result.get("review_cycle")
    if final is not None and not isinstance(final, dict):
        raise RatchetRefused(contradiction)
    reservation = final.get("reservation") if isinstance(final, dict) else None
    decisions = [value for value in (outer, reservation) if value is not None]
    if (any(not isinstance(value, dict) or value.get("status") != "reserved"
            or not isinstance(value.get("row"), dict) for value in decisions)
            or (len(decisions) == 2 and decisions[0] != decisions[1])):
        raise RatchetRefused(contradiction)
    decision = decisions[0] if decisions else None
    if decision is not None and any(key in record and record[key] != decision["row"].get(row_key)
                                   for key, row_key in (("review_cycle_dispatch_id", "dispatch_id"),
                                                        ("review_cycle_subject_branch", "subject_branch"))):
        raise RatchetRefused(contradiction)
    if verdict == "not_attempted":
        specs = [value["lane_spec"] for value in (record, result) if "lane_spec" in value]
        if (any(not isinstance(spec, dict) for spec in specs)
                or refusal or any(spec.get("lane_kind") == "review" for spec in specs)
                or (decision is not None and decision["row"].get("kind") != "implement")
                or (state_dir / f"review-receipt-{record.get('pass_id')}.json").exists()):
            raise RatchetRefused(contradiction)
        if (record.get("status") == "done" and result.get("harvest_verdict") == "not_attempted"
                and decision is not None):
            return NONREVIEW_HISTORY
        return REVIEW_HISTORY

    if result.get("provider_error_code") == "turn_failed" and refusal:
        from .offload_pass_recovery import EvidenceHold
        try:
            dispatch_id = record["review_cycle_dispatch_id"]
            ledger = ReviewCycleLedger(authority(repo) / "review_cycle_ledger.json", trusted_policy())
            row = ledger._read()["rows"][dispatch_id]
            proof = _attached_unjudged(repo, state_dir, dispatch_id, row)
            completion = row.get("completion") or {}
            if (completion.get("outcome") != "review_incomplete" or completion.get("judgment") != "incomplete"
                    or completion.get("evidence") != proof or completion.get("usage") != "UNKNOWN"):
                raise ValueError("unjudged_settlement_required")
            return NONREVIEW_HISTORY
        except (EvidenceHold, OSError, ValueError, KeyError, TypeError, AttributeError, _Hold,
                subprocess.SubprocessError) as exc:
            raise RatchetRefused(unavailable) from exc

    try:
        dispatch_id = record["review_cycle_dispatch_id"]
        if (record.get("status") != "done" or not isinstance(inner, dict)
                or not isinstance(dispatch_id, str) or not dispatch_id or decision is None
                or any(result.get(key) for key in ("model_started", "model_process_started", "commit_landed",
                                                   "merge_ready", "changed_files", "findings"))):
            raise ValueError("dispatch_identity")
        # Do not use _receipt_path: a history reader must never mkdir or mint a lock.
        path = authority(repo) / "review_cycle_dispatch" / (hashlib.sha256(dispatch_id.encode()).hexdigest() + ".json")
        protected = json.loads(path.read_bytes())
        managed = {key: record[key] for key in ("task_ref", "lane_id", "pass_id", "worktree_path")}
        expected = dict(dispatch_id=dispatch_id, subject_branch=record["review_cycle_subject_branch"],
                        kind=decision["row"]["kind"])
        if (protected["managed_identity"] != managed or protected["identity"] != expected
                or protected["decision"] != decision
                or protected["managed_source"]["branch"] != record["branch"]
                or protected["managed_source"]["head_sha"] != record["tip_sha"]
                or decision["row"].get("dispatch_id") != dispatch_id
                or decision["row"].get("subject_branch") != expected["subject_branch"]
                or _model_or_unknown_evidence(protected) is not None):
            raise ValueError("producer_join")
        native = protected.get("native_pre_model_refusal")
        if native is not None:
            if _native_pre_model_reason(protected, native) is None:
                raise ValueError("native_proof")
            digest_key = "proof_sha256"
            digest = hashlib.sha256(json.dumps(native, sort_keys=True).encode()).hexdigest()
        else:
            evidence = protected["adapter_pre_dispatch_refusal"]
            encoded = evidence["envelope_json"].encode()
            if (evidence["schema"] != "workbay/adapter-pre-dispatch-refusal@1"
                    or evidence["managed_identity"] != managed
                    or hashlib.sha256(encoded).hexdigest() != evidence["envelope_sha256"]):
                raise ValueError("adapter_envelope")
            envelope = json.loads(encoded)
            if (_model_or_unknown_evidence(envelope) is not None
                    or _adapter_refusal_reason(protected, envelope.get("raw_payload", {})) is None):
                raise ValueError("adapter_proof")
            digest_key, digest = "envelope_sha256", evidence["envelope_sha256"]
        if "pre_dispatch_refusal" in result:
            observed = result["pre_dispatch_refusal"]
            if not isinstance(observed, dict) or observed.get(digest_key) != digest:
                raise ValueError("refusal_digest")
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as exc:
        raise RatchetRefused(unavailable) from exc
    return NONREVIEW_HISTORY


def _record_shell_reserve_refusal(repo: Path, *, dispatch_id: str, subject_branch: str, kind: str,
                                  token: str, detail: str) -> dict | None:
    """Bind the bridge's typed refusal of an engine transfer to that reservation.

    Positive native producer evidence, written under the dispatch lock only for
    the engine-owned managed receipt whose one-use token this bridge was handed
    and which is still exactly ``prepared`` (no transport, source, native
    dispatch or completion). The token is not consumed and the row is not
    settled here; the engine's shared reader judges the proof (SECD-05). An
    existing proof is never replaced (DATA-14). Anything else records nothing:
    exit 76 alone never proves that no model ran.
    """
    path = _receipt_path(repo, dispatch_id)
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            receipt = json.loads(path.read_text())
        except (OSError, ValueError):
            return None
        if (not isinstance(receipt, dict) or receipt.get("token") != token
                or receipt.get("identity") != dict(dispatch_id=dispatch_id, subject_branch=subject_branch, kind=kind)
                or "native_pre_model_refusal" in receipt):
            return None
        try:
            proof = _native_pre_model_proof(receipt, "shell_reserve", detail[:_DETAIL_LIMIT] or "refused")
        except (KeyError, TypeError, AttributeError, ValueError):
            return None
        candidate = {**receipt, "native_pre_model_refusal": proof}
        if _native_pre_model_reason(candidate, proof) is None:
            return None
        _save(path, candidate)
        return proof


def record_native_pre_model_refusal(repo: Path, *, phase: str) -> dict:
    """Write-ahead positive proof that a host-native phase exits before the model.

    Recorded BEFORE settlement under the dispatch receipt lock (CON-11), and
    only for the ACTIVE_DISPATCH managed reservation whose durable receipt is
    still exactly the prepared one. An existing different proof is never
    replaced (DATA-14). Any failure raises; the caller must not settle as a
    refusal without this proof (SECD-05).
    """
    active = ACTIVE_DISPATCH.get()
    if (phase not in _NATIVE_PRE_MODEL_REASONS or not isinstance(active, dict)
            or not isinstance(active.get("managed_identity"), dict) or not isinstance(active.get("token"), str)):
        raise ValueError("native_pre_model_refusal_unbound")
    path = _receipt_path(repo, active["identity"]["dispatch_id"])
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt = json.loads(path.read_bytes())
        prior = receipt.get("native_pre_model_refusal")
        base = {key: value for key, value in receipt.items() if key != "native_pre_model_refusal"}
        if base != active:
            raise ValueError("native_pre_model_refusal_dispatch_changed")
        proof = _native_pre_model_proof(receipt, phase)
        if prior is not None:
            if prior != proof:
                raise ValueError("native_pre_model_refusal_changed")
            return prior
        receipt["native_pre_model_refusal"] = proof
        if _native_pre_model_reason(receipt, proof) is None:
            raise ValueError("native_pre_model_refusal_unbound")
        _save(path, receipt)
        return proof


def managed_refusal_evidence(repo: Path, active: dict) -> dict | None:
    """Read the protected pre-model observation; missing proof never forgives.

    One reader for both positive proofs: the trusted adapter constructor's
    retained envelope, or a host-native pre-model exit (for example the
    history guard). Retained bytes support settlement recovery even if the
    temporary envelope has gone away. This does not amend an already settled
    ledger completion. Model-start or execution facts refuse either proof.
    """
    if not isinstance(active.get("identity"), dict):
        raise ValueError("adapter_refusal_evidence_mismatch")
    path = _receipt_path(repo, active["identity"]["dispatch_id"])
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt = json.loads(path.read_bytes())
        evidence = receipt.get("adapter_pre_dispatch_refusal")
        native = receipt.get("native_pre_model_refusal")
        if evidence is None and native is None:
            return None
        if any(receipt.get(key) != active.get(key) for key in ("identity", "token", "managed_identity",
                                                                "managed_source")):
            raise ValueError("adapter_refusal_evidence_mismatch")
        if native is not None:
            reason = _native_pre_model_reason(receipt, native)
            if reason is None:
                raise ValueError("adapter_refusal_evidence_invalid")
            return dict(outcome="dispatch_refused", reason=reason, phase=native["phase"], receipt_path=str(path),
                        proof_sha256=hashlib.sha256(json.dumps(native, sort_keys=True).encode()).hexdigest())
        if (not isinstance(evidence, dict)
                or evidence.get("schema") != "workbay/adapter-pre-dispatch-refusal@1"
                or evidence.get("managed_identity") != receipt["managed_identity"]
                or re.fullmatch(r"[0-9a-f]{64}", evidence.get("envelope_sha256", "")) is None):
            raise ValueError("adapter_refusal_evidence_mismatch")
        if not isinstance(evidence.get("envelope_json"), str):
            raise ValueError("adapter_refusal_evidence_invalid")
        encoded = evidence["envelope_json"].encode()
        if hashlib.sha256(encoded).hexdigest() != evidence["envelope_sha256"]:
            raise ValueError("adapter_refusal_evidence_digest_mismatch")
        envelope = json.loads(encoded)
        if not isinstance(envelope, dict) or _model_or_unknown_evidence(envelope) is not None:
            raise ValueError("adapter_refusal_evidence_invalid")
        reason = _adapter_refusal_reason(receipt, envelope.get("raw_payload", {}))
        if reason is None:
            raise ValueError("adapter_refusal_evidence_invalid")
        return dict(outcome="dispatch_refused", reason=reason, receipt_path=str(path),
                    envelope_sha256=evidence["envelope_sha256"])


def normalized_model(value: object) -> object:
    """Registry spellings of the same reserved model (claude-opus-5-5 / opus-5.5)."""
    return {"claude-opus-5-5": "opus-5.5", "claude-opus-5.5": "opus-5.5"}.get(value, value)


def enforce_route(spec: dict, row: dict) -> None:
    """Check executed argv, not the spec's descriptive routing receipt."""
    argv = spec["argv"]
    if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
        raise ValueError("review_cycle_route_invalid")
    models = [argv[i + 1] for i, item in enumerate(argv[:-1]) if item in {"-m", "--model"}]
    efforts = [argv[i + 1] for i, item in enumerate(argv[:-1]) if item == "--effort"]
    efforts += [item.split("=", 1)[1].strip('"\'') for item in argv
                if item.startswith("model_reasoning_effort=")]
    required = row["required_route"]
    expected = required["model"]
    # Registry uses claude-opus-5-5 / opus-5.5 spellings for the same model.
    normalized = normalized_model
    if (len(models) != 1 or normalized(models[0]) != normalized(expected)
            or len(efforts) != 1 or efforts[0].casefold() != required["effort"]):
        raise ValueError("review_cycle_required_route_mismatch")
    backend = spec.get("backend_id")
    canonical = normalized(expected)
    if (expected.startswith("gpt-") and backend != "codex-remote") or (
            canonical.startswith("opus") and backend not in {"claude-code", "claude-remote"}) or (
            expected == "claude-haiku-5-5" and backend != "claude-remote"):
        raise ValueError("review_cycle_required_backend_mismatch")


def shell_reserve(repo: Path, spec_path: Path, *, dispatch_id: str, subject_branch: str,
                  kind: str, canon_proposal_path: str | None = None, token: str | None = None) -> dict:
    """One-use engine→shell transfer; raw shell entries reserve here themselves."""
    try:
        spec = json.loads(spec_path.read_text())
        source_proof = validate_shell_subject(repo, spec, subject_branch, dispatch_id)
        if kind == "gate" and not token:
            source_review_scope(repo, spec, subject_branch)  # refuse a mismatched spec before charging
        if token:
            path = _receipt_path(repo, dispatch_id)
            with path.with_suffix(".lock").open("a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                receipt = json.loads(path.read_text())
                if (receipt.get("token") != token or receipt.get("state") != "prepared"
                        or receipt["identity"] != dict(dispatch_id=dispatch_id, subject_branch=subject_branch, kind=kind)):
                    raise ValueError("dispatch_transfer_invalid_or_consumed")
                if "native_pre_model_refusal" in receipt:
                    # A transfer already proven refused before transport stays refused.
                    raise ValueError("dispatch_transfer_refused_pre_model")
                if kind == "gate":
                    scope = receipt.get("source_review_scope")
                    if not isinstance(scope, dict):
                        raise ValueError("review_cycle_review_scope_unavailable")
                    if (scope.get("source_branch"), scope.get("tip_sha"), scope.get("source_tree")) != (
                            spec.get("branch"), spec.get("head_sha"),
                            _git(repo, "rev-parse", spec["head_sha"] + "^{tree}")):
                        raise ValueError("review_cycle_review_scope_mismatch")
                # Re-read canonical ledger and trusted context on the consumer.
                decision = dispatch(dict(operation="reserve", **receipt["identity"]), repo=repo)
                if decision.status != "reserved" or decision.row.get("completion") is not None:
                    raise ValueError("dispatch_transfer_not_pending")
                enforce_route(spec, decision.row)
                receipt["state"] = "transport_started"
                receipt.update(source_branch=spec.get("branch"), source_head=spec.get("head_sha"),
                               source_tree=_git(repo, "rev-parse", spec["head_sha"] + "^{tree}"))
                if source_proof is not None:
                    receipt["source_task_subject"] = source_proof
                else:
                    unproven = _source_task_subject_unproven(repo, spec, subject_branch)
                    if unproven is not None:
                        receipt["source_task_subject_unproven"] = unproven
                _save(path, receipt)
                return receipt
        receipt = prepare(repo, dispatch_id=dispatch_id, subject_branch=subject_branch, kind=kind,
                          canon_proposal_path=canon_proposal_path)
        if receipt["decision"]["status"] != "reserved":
            return receipt
        enforce_route(spec, receipt["decision"]["row"])
        return shell_reserve(repo, spec_path, dispatch_id=dispatch_id, subject_branch=subject_branch,
                             kind=kind, token=receipt["token"])
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        if "receipt" in locals() and receipt.get("decision", {}).get("status") == "reserved" and str(exc).startswith("review_cycle_"):
            settle_actual(repo, receipt["identity"], outcome="dispatch_refused")
        return {"decision": asdict(LedgerDecision("ledger_hold", str(exc)))}


def settle_actual(repo: Path, identity: dict, *, outcome: str, model_started: bool = False,
                  judgment: bool = False, commit_landed: bool = False, refusal_reason: str | None = None,
                  harvest: dict | None = None) -> LedgerDecision:
    """Producer evidence dominates model claims of pending/infra forgiveness."""
    from .pass_outcome_class import PassOutcomeClass, classify_pass_outcome
    if model_started or judgment or commit_landed:
        if classify_pass_outcome(outcome, refusal_reason=refusal_reason) != PassOutcomeClass.VERDICT:
            outcome, refusal_reason = "error", None
    request = dict(operation="settle", **identity, outcome=outcome, refusal_reason=refusal_reason)
    if identity["kind"] == "gate" and classify_pass_outcome(outcome, refusal_reason=refusal_reason) == PassOutcomeClass.VERDICT:
        from .offload_pass import HARVEST_VERDICT_REVIEWED_CLEAN, _harvest_verdict
        if (isinstance(harvest, dict) and _harvest_verdict(harvest) == HARVEST_VERDICT_REVIEWED_CLEAN
                and not harvest.get("unrecorded_ids")):
            harvest = {**harvest, "blocking_findings": []}
        if not isinstance(harvest, dict) or "blocking_findings" not in harvest:
            return LedgerDecision("ledger_hold", "gate_harvest_required")
        from workbay_handoff_mcp.review_findings_recording import _finding_content_fingerprint
        # Harvest has already applied workbay_protocol.review_impact.is_blocking.
        rows = harvest["blocking_findings"]
        try:
            named = listed_scope(reserved_row(repo, identity), harvest)
        except ValueError as exc:
            return LedgerDecision("ledger_hold", str(exc))
        if named is not None:
            rows = [row for row in rows if str(row["finding_id"]) in named]
        request.update(blocking_ids=[str(row["finding_id"]) for row in rows], blocking_fingerprints=[
            _finding_content_fingerprint(row["file_path"], row["description"]) for row in rows])
    return dispatch(request, repo=repo)


def reserved_row(repo: Path, identity: dict) -> dict:
    """Read-only copy of this dispatch's reservation from the trusted ledger."""
    try:
        row = runtime_ledger(repo)._read()["rows"].get(identity["dispatch_id"])
    except (_Hold, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        raise ValueError("review_cycle_reservation_unreadable") from exc
    if (not isinstance(row, dict) or row["decision"] != "reserved"
            or (row["subject_branch"], row["kind"]) != (identity["subject_branch"], identity["kind"])):
        raise ValueError("review_cycle_reservation_unavailable")
    return row


def listed_scope(row: dict, harvest: dict) -> set[str] | None:
    """Named IDs a listed-only final gate may judge; None for an unscoped gate.

    Named blockers the canonical harvest still carries as unverified stay in
    scope, so a remaining named blocker always stops. Unlisted candidates
    never widen it.
    """
    if not row.get("listed_open_only"):
        return None
    carried = harvest.get("unverified_listed_finding_ids") or []
    return {str(fid) for fid in (*row["listed_blocking_ids"], *carried)}


def scope_harvest(repo: Path, identity: dict, harvest: dict) -> dict:
    """Bind a canonical gate harvest to its reserved listed-only scope.

    Severity, impact and finding status are untouched. Recorded out-of-scope
    candidates stay visible as follow-ups rather than terminal blockers.
    """
    cycle = identity.get("review_cycle") or {}
    row = cycle.get("row") or {}
    if cycle.get("status") != "reserved" or row.get("kind") != "gate" or "blocking_findings" not in harvest:
        return harvest
    try:
        named = listed_scope(reserved_row(repo, row), harvest)
    except ValueError as exc:
        # Unscoped is the stricter result; settlement re-reads the ledger itself.
        return {**harvest, "review_cycle_scope": {"status": "unavailable", "reason": str(exc)}}
    if named is None:
        return harvest
    rows = harvest["blocking_findings"]
    return {**harvest, "blocking_findings": [item for item in rows if str(item["finding_id"]) in named],
            "review_cycle_scope": {"status": "listed_open_only", "listed_finding_ids": sorted(named),
                                   "followup_finding_ids": sorted(str(item["finding_id"]) for item in rows
                                                                  if str(item["finding_id"]) not in named)}}


def _pre_model_phases(path: Path) -> bool:
    """Validate the native VM phase schema; later or unknown phases charge."""
    phases = json.loads(path.read_bytes())
    if (not isinstance(phases, dict) or type(phases.get("schema_version")) is not int
            or phases["schema_version"] != 1
            or set(phases) != {"schema_version", "partial", "warm_skip", "vm_span", "vm_setup", "phases"}
            or type(phases["partial"]) is not bool or type(phases["warm_skip"]) is not bool
            or type(phases["vm_span"]) is not int or phases["vm_span"] < 0
            or type(phases["vm_setup"]) is not int or phases["vm_setup"] != phases["vm_span"]
            or not isinstance(phases["phases"], dict)):
        return False
    before_model = {"remote_preamble", "archive_extract", "git_init", "sync", "uv_sync", "uv_cache", "agent_launch"}
    for name, phase in phases["phases"].items():
        if (name not in before_model or not isinstance(phase, dict)
                or not {"side", "start_ts", "end_ts", "duration_s"} <= set(phase)
                or set(phase) - {"side", "start_ts", "end_ts", "duration_s", "status"}
                or phase["side"] != "vm"
                or any(type(phase[key]) is not int or phase[key] < 0
                       for key in ("start_ts", "end_ts", "duration_s"))
                or phase["end_ts"] - phase["start_ts"] != phase["duration_s"]
                or ("status" in phase and not isinstance(phase["status"], str))):
            return False
    return True


def native_collect_outcome(bundle: Path, dispatch_id: str, recorded_rc: str | None, *,
                           receipt: dict | None = None) -> str:
    """Refund only native pre-model proof bound to the retained host request.

    Wrapper digests authenticate bytes, not their meaning. Reuse the native
    source-to-sandbox schema validator, then bind the nonce/ref to the host's
    submit receipt. Missing authority, false markers and contradictory phase
    evidence charge; the synthetic sandbox commit is never the original head.
    """
    from .adapters.remote_exec import _RC_ADMISSION_DEFERRED, _source_provenance_from_json
    from .offload_pass import OffloadOutcome
    error = OffloadOutcome.ERROR.value
    try:
        done = json.loads((bundle / "done.json").read_bytes())
        stdout = (bundle / "stdout").read_bytes()
        manifest = json.loads((bundle / "artifacts.json").read_bytes())
        files = manifest["files"]
        present = {path.name for path in (bundle / "artifacts").iterdir()}
        authentic = (type(done["schema_version"]) is int and done["schema_version"] == 1
                     and done["job_id"] == dispatch_id and type(done["rc"]) is int and str(done["rc"]) == recorded_rc
                     and done["reason"] is None and type(done["stdout_bytes"]) is int
                     and done["stdout_bytes"] == len(stdout)
                     and done["stdout_sha256"] == hashlib.sha256(stdout).hexdigest()
                     and set(manifest) == {"job_id", "files"} and manifest["job_id"] == dispatch_id
                     and isinstance(files, dict) and present == set(files)
                     and all(hashlib.sha256((bundle / "artifacts" / key).read_bytes()).hexdigest() == digest
                             for key, digest in files.items()))
        if (not authentic or done["rc"] != _RC_ADMISSION_DEFERRED or stdout
                or not present <= _PRE_MODEL_ARTIFACTS or "provenance-out" not in present):
            return error
        if receipt["identity"]["dispatch_id"] != dispatch_id or receipt["state"] != "transport_started":
            return error
        native = receipt["native_dispatch"]
        proof, reason = _source_provenance_from_json(bundle / "artifacts/provenance-out",
            branch=receipt["source_branch"], head_sha=receipt["source_head"], head_tree=receipt["source_tree"])
        fields = {"schema_version", "dispatch_nonce", "requested_branch", "requested_ref", "agent_spec_head_sha",
                  "dispatch_ref", "remote_source_commit", "remote_source_tree", "sandbox_base_commit",
                  "sandbox_base_tree", "history_stripped"}
        if (reason or proof is None or set(proof) != fields or type(proof["schema_version"]) is not int
                or re.fullmatch(r"[0-9]+-[0-9a-f]{16}", native["dispatch_nonce"]) is None
                or proof["dispatch_nonce"] != native["dispatch_nonce"]
                or proof["dispatch_ref"] != native["dispatch_ref"]):
            return error
        if "phases-out" in present and not _pre_model_phases(bundle / "artifacts/phases-out"):
            return error
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return error
    return OffloadOutcome.ADMISSION_DEFERRED.value


def _attached_unjudged(repo: Path, state_dir: Path, dispatch_id: str, row: dict) -> dict:
    """Read the attached carrier's actual producers, never a submitted result.

    No detached native_dispatch/native_completion is inferred. The publisher's
    invocation, the protected transfer token and managed identity are bound by
    the evidence digest, together with the exact immutable producer bytes.
    """
    from .adapters.remote_exec import _provider_error_from_stream
    from .offload_pass import _pass_state_path
    from .offload_pass_recovery import read_dispatch_receipt, regular_file_bytes, stream_turn_facts
    from .review_ratchet import review_pass_view

    _, protected, protected_raw = read_dispatch_receipt(repo, dispatch_id)
    managed = protected["managed_identity"]
    pass_id = managed["pass_id"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", pass_id):
        raise ValueError("unjudged_pass_identity")
    pass_path = _pass_state_path(state_dir, pass_id)
    pass_raw = regular_file_bytes(pass_path, root=state_dir)
    record = review_pass_view(json.loads(pass_raw))
    result = record.get("result") or {}
    harvest = result.get("findings_harvest") or {}
    expected = dict(dispatch_id=dispatch_id, subject_branch=row["subject_branch"], kind="gate")
    reservation = protected["decision"]
    stable = ("dispatch_id", "subject_branch", "kind", "decision", "round", "required_route", "activated_at")
    if (protected["identity"] != expected or row["kind"] != "gate"
            or protected.get("state") != "transport_started" or protected.get("settlement_owner") != "engine"
            or not isinstance(protected.get("token"), str) or not protected["token"]
            or reservation.get("status") != "reserved"
            or any(reservation["row"].get(key) != row.get(key) for key in stable)
            or record.get("review_cycle") != reservation
            or (result.get("review_cycle") or {}).get("reservation") != reservation
            or record.get("status") != "done" or record.get("superseded_invocations")
            or result.get("outcome") != "error"
            or result.get("failed_stage") != "execute" or result.get("history_complete") is not True
            or result.get("commit_landed") is not False
            or harvest.get("status") != "skipped" or harvest.get("reason") != "no_review_turn"
            or harvest.get("salvage_suppressed") != "dispatch_refusal"
            or result.get("review_verdict") is not None or result.get("provider_error_code") != "turn_failed"
            or any(record.get(key) != value or result.get(key) != value for key, value in managed.items())
            or any(record.get(key) != result.get(key) for key in
                   ("branch", "base_sha", "tip_sha", "agent_stream_path", "review_cycle_dispatch_id",
                    "review_cycle_subject_branch", "dispatch_id"))
            or record.get("dispatch_id") != pass_id + "-prepared"
            or record.get("review_cycle_dispatch_id") != dispatch_id
            or record.get("review_cycle_subject_branch") != row["subject_branch"]
            or protected["managed_source"]["branch"] != record.get("branch")
            or protected["managed_source"]["head_sha"] != record.get("tip_sha")
            or re.fullmatch(r"[0-9a-f]{32}", record.get("invocation_token", "")) is None):
        raise ValueError("unjudged_attached_identity")
    scope = protected["source_review_scope"]
    if any(scope.get(key) != value for key, value in {
        "task_ref": managed["task_ref"], "lane_id": managed["lane_id"],
        "tip_sha": record["tip_sha"], "source_branch": record["branch"],
        "source_tree": protected["managed_source"]["head_tree"],
        "review_cycle_subject_branch": row["subject_branch"],
    }.items()):
        raise ValueError("unjudged_source_scope")
    stream_path = Path(record["agent_stream_path"])
    stream_raw = regular_file_bytes(stream_path, root=state_dir)
    # The adapter's native spool spec names the branch, source and enforced route.
    spec_raw = regular_file_bytes(stream_path.parent / "spec.json", root=state_dir)
    spec = json.loads(spec_raw)
    route = spec["routing_effective"]
    facts = stream_turn_facts(stream_raw)
    events = [json.loads(line) for line in stream_raw.splitlines() if line.strip()]
    if (spec.get("spec_version") != 2 or spec.get("backend_id") != "codex-remote"
            or spec.get("branch") != record["branch"] or spec.get("head_sha") != record["tip_sha"]
            or route.get("model") != row["required_route"]["model"]
            or route.get("effort") != row["required_route"]["effort"]
            or facts != dict(turn_started=1, turn_completed=0, turn_failed=1, malformed_lines=0)
            or not events or events[-1].get("type") != "turn.failed"
            or _provider_error_from_stream(stream_path) is None):
        raise ValueError("unjudged_terminal_provider_required")
    # Re-read after the adapter parser, so its path read cannot introduce an
    # observation from different bytes. All producer reads are regular/no-link.
    if regular_file_bytes(stream_path, root=state_dir) != stream_raw:
        raise ValueError("unjudged_evidence_changed")
    proof = dict(schema="workbay/attached-unjudged-gate@1", dispatch_id=dispatch_id,
                 invocation_token=record["invocation_token"], managed_identity=managed,
                 transfer_token_sha256=hashlib.sha256(protected["token"].encode()).hexdigest(),
                 pass_sha256=hashlib.sha256(pass_raw).hexdigest(),
                 receipt_sha256=hashlib.sha256(protected_raw).hexdigest(),
                 stream_sha256=hashlib.sha256(stream_raw).hexdigest(),
                 spec_sha256=hashlib.sha256(spec_raw).hexdigest())
    return proof


def unjudged_gate(request: dict, *, repo: Path, state_dir: Path | None = None) -> dict:
    """Public attached SAME-JOB inspect/settle; stdin accepts no result or paths.

    inspect_unjudged returns CAS values; settle_unjudged requires exactly those
    values. Only the ledger is written. The old pass and all receipts stay bytes.
    """
    from .offload_pass import _pass_state_path
    from .offload_pass_recovery import EvidenceHold, read_dispatch_receipt
    from .terminal_history_repair import _dispatch_lock, _pass_publication
    state_dir = state_dir or repo / ".task-state"
    try:
        operation, dispatch_id = request["operation"], request["dispatch_id"]
        fields = {"operation", "dispatch_id"}
        if operation == "settle_unjudged":
            fields |= {"expected_pass_sha256", "expected_receipt_sha256", "expected_evidence_sha256"}
        if operation not in {"inspect_unjudged", "settle_unjudged"} or set(request) != fields:
            raise ValueError("invalid_protocol")
        ledger = ReviewCycleLedger(authority(repo) / "review_cycle_ledger.json", trusted_policy())
        receipt_path, protected, _ = read_dispatch_receipt(repo, dispatch_id)
        def authenticate(row):
            try:
                proof = _attached_unjudged(repo, state_dir, dispatch_id, row)
            except (EvidenceHold, OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                raise _Hold(str(exc)) from exc
            digest = hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest()
            if operation == "settle_unjudged" and any(request["expected_" + key] != value for key, value in {
                "pass_sha256": proof["pass_sha256"], "receipt_sha256": proof["receipt_sha256"],
                "evidence_sha256": digest,
            }.items()):
                raise _Hold("unjudged_evidence_changed")
            return proof
        if operation == "inspect_unjudged":
            proof = authenticate(ledger._read()["rows"][dispatch_id])
            return dict(status="ready", dispatch_id=dispatch_id, usage="UNKNOWN", judgment="incomplete",
                        expected_pass_sha256=proof["pass_sha256"], expected_receipt_sha256=proof["receipt_sha256"],
                        expected_evidence_sha256=hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest())
        # Same producer fences as native publication, then the atomic ledger
        # transaction re-authenticates evidence before any mutation (CON-11).
        with _dispatch_lock(receipt_path), _pass_publication(
                _pass_state_path(state_dir, protected["managed_identity"]["pass_id"])):
            return asdict(ledger.settle_unjudged(dispatch_id, authenticate=authenticate))
    except (EvidenceHold, _Hold, OSError, ValueError, KeyError, TypeError, AttributeError,
            subprocess.SubprocessError) as exc:
        return dict(status="ledger_hold", reason=str(exc))


def main() -> None:
    try:
        request = json.load(sys.stdin)
        result = dispatch(request)
    except (ValueError, TypeError) as exc:
        result = LedgerDecision("ledger_hold", str(exc))
    print(json.dumps(asdict(result) if isinstance(result, LedgerDecision) else result))


if __name__ == "__main__":
    main()


def _publish_native_completion(path: Path, rc: str, outcome: str) -> dict:
    """Persist the host-observed rc/outcome before harvest reads the receipt.

    The lock is released on return: the evidence consumer takes it
    non-blocking and fails closed while a producer holds it.
    """
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        receipt = json.loads(path.read_text())
        receipt["native_completion"] = dict(rc=rc, outcome=outcome)
        _save(path, receipt)
    return receipt


def shell_entry() -> None:
    """Private shell bridge; the public module stdin protocol stays reserve/settle."""
    mode, dispatch_id, subject, kind, spec, proposal = sys.argv[1:7]
    repo = Path.cwd()
    if mode == "reserve":
        token = os.environ.get("WORKBAY_REVIEW_CYCLE_TOKEN")
        receipt = shell_reserve(repo, Path(spec), dispatch_id=dispatch_id, subject_branch=subject, kind=kind,
                                canon_proposal_path=proposal or None, token=token)
        decision = receipt["decision"]
        if token and decision["status"] != "reserved":
            try:
                _record_shell_reserve_refusal(repo, dispatch_id=dispatch_id, subject_branch=subject, kind=kind,
                                              token=token, detail=str(decision.get("reason") or ""))
            except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
                pass  # no proof: the attempt stays charged (SECD-05)
    else:
        receipt = json.loads(_receipt_path(repo, dispatch_id).read_text())
        rc, result_path, provenance_path, patch_path = sys.argv[7:11]
        def read(path):
            try:
                return json.loads(Path(path).read_text()) if path else {}
            except (OSError, ValueError):
                return {}
        result, provenance = read(result_path), read(provenance_path)
        started = provenance.get("model_process_started") is True
        patch = bool(patch_path and Path(patch_path).is_file() and Path(patch_path).stat().st_size)
        # Queue receipt is never model output. Before-model admission is the
        # only rc-only forgiveness; unknown/partial transport fails closed.
        outcome = "remote_job_pending" if mode == "pending" else (
            "transport_failure" if mode == "premodel" and not started and not result and not patch else "error")
        path = _receipt_path(repo, dispatch_id)
        if mode == "settle":  # only a completed synchronous run has a native rc
            receipt = _publish_native_completion(path, rc, outcome)
        settlement = settle_actual(repo, receipt["identity"], outcome=outcome, model_started=started,
                                   judgment=bool(result), commit_landed=patch,
                                   harvest=harvest_shell_gate(repo, receipt, result) if kind == "gate" and result else None)
        decision = asdict(settlement)
        with path.with_suffix(".lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            receipt = json.loads(path.read_text())
            # These extra pending arguments come from the host submit call, never
            # from collected artifacts or the worker's result payload.
            if mode == "pending" and len(sys.argv) == 13:
                nonce, ref = sys.argv[11:13]
                if re.fullmatch(r"[0-9]+-[0-9a-f]{16}", nonce) is None or not ref.startswith("refs/heads/"):
                    raise ValueError("native_dispatch_identity_invalid")
                native = dict(dispatch_nonce=nonce, dispatch_ref=ref)
                if receipt.get("native_dispatch", native) != native:
                    raise ValueError("native_dispatch_identity_changed")
                receipt["native_dispatch"] = native
            receipt["settlement"] = decision
            receipt["producer_artifacts"] = dict(result=result_path, provenance=provenance_path, patch=patch_path)
            _save(path, receipt)
    print("remote_agent: review_cycle " + json.dumps(decision, sort_keys=True), file=sys.stderr)
    if mode == "reserve" and decision["status"] != "reserved":
        raise SystemExit(76)


def canonical_subject(repo: Path, state_dir: Path, *, task_ref: str, branch: str,
                      pass_id: str, orchestrator_root: Path) -> str:
    """Reuse D1's validated registry/host-receipt subject binding.

    Without a preceding receipt, the registry producer's re-verified proof of a
    separate canonical task is consumed by the retained-ancestry hold; an
    absent receipt alone never proves a fresh task.
    """
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.runtime import configure_runtime

    from .review_ratchet import load_context, proven_task_subject
    context = load_context(repo, state_dir, task_ref=task_ref, branch=branch, current_pass_id=pass_id,
                           orchestrator_root=orchestrator_root)
    if context is None or not context.preceding_pass_id:
        configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
        return retained_subject(repo, branch, proof=proven_task_subject(repo, orchestrator_root, task_ref, branch=branch),
                                orchestrator_root=orchestrator_root)
    receipt = json.loads((state_dir / f"review-receipt-{context.preceding_pass_id}.json").read_text())
    identity = receipt["identity"]
    return identity.get("review_cycle_subject_branch") or identity["branch"]


def registered_lane(repo: Path, branch: str) -> tuple[object, dict] | None:
    """Read the existing primary-worktree runtime registry when deployed.

    Retained rows of every status still count; only a validated registry
    correction sets a misregistered row aside.
    """
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.lanes_api import resolve_lane_by_branch
    from workbay_handoff_mcp.runtime import configure_runtime
    config = RuntimeConfig.for_repo(repo)
    if not config.db_path.exists():
        return None
    configure_runtime(config)
    response = resolve_lane_by_branch(branch)
    if not response.get("ok"):
        raise ValueError("review_cycle_registry_unavailable")
    lanes = (response.get("data") or {}).get("current")
    if not isinstance(lanes, list) or len(lanes) > 1:
        raise ValueError("review_cycle_registry_ambiguous")
    return (config, lanes[0]) if lanes else None


def _released(repo: Path, proof: dict, subject: str, source_branch: str, head: str) -> bool:
    """Release a proven task from completed, landed ancestor work only.

    A main-target maintenance parent may stay active for unrelated features.
    Its original lane's typed merged identity, rechecked against Git, proves
    that work unit landed; its status or a prose claim alone does not. A task
    targeting the old feature still requires task completion as before.
    """
    from workbay_handoff_mcp.lanes_api import resolve_lane_by_branch

    from .review_ratchet import _commit, _short_branch, _subject_is_ancestor, task_record
    if subject == proof["subject_branch"]:
        return False
    landed = _commit(repo, f"refs/heads/{proof['integration_branch']}")
    if landed is None or not _subject_is_ancestor(repo, proof["base_sha"], landed):
        return False
    result = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", head, landed],
                            capture_output=True, check=False)
    if result.returncode == 1:
        return False
    if result.returncode:
        raise ValueError("review_cycle_subject_unresolvable")
    lanes = []
    for name in {subject, source_branch}:
        response = resolve_lane_by_branch(name)
        if not response.get("ok"):
            raise ValueError("review_cycle_registry_unavailable")
        lanes.extend(response["data"]["current"])
    if not lanes:
        return False
    for row in lanes:
        task = task_record(row["task_ref"]) or {}
        if row["task_ref"] == proof["task_ref"] and _short_branch(task.get("target_branch")) != "main":
            return False
        if task.get("status") == "done":
            continue
        if (task.get("status") != "in_progress"
                or _short_branch(task.get("target_branch")) != "main"
                or _short_branch(task.get("target_branch")) != proof["integration_branch"]
                or row.get("status") != "merged"
                or row.get("branch_tip_source") not in {"branch", "backfill"}):
            return False
        tip, landing = row.get("branch_tip_sha"), row.get("landing_commit_sha")
        if any(not isinstance(sha, str) or not re.fullmatch("[0-9a-f]{40}", sha) for sha in (tip, landing)):
            return False
        if _commit(repo, tip) != tip or _commit(repo, landing) != landing:
            return False
        if not all(_subject_is_ancestor(repo, older, newer) for older, newer in (
                (head, tip), (tip, landing), (landing, landed))):
            return False
    return True


def _authorized_roots(repo: Path, orchestrator_root: Path, binding: dict) -> set[str] | None:
    """A subject's original roots plus its current producer authorization.

    The pinned binding keeps source-time scope immutable while the registry
    producer records later expansions beside it, so neither alone is the work
    unit's resource claim. Authenticated on each use; ``None`` holds.
    SECD-03, SECD-05 (Security Engineering); REF-09 (Refactoring, ch. 9).
    """
    from .review_ratchet import _owned_roots, source_task_subject_scope

    original = _owned_roots(binding.get("owned_paths"))
    current = None if original is None else source_task_subject_scope(repo, orchestrator_root, binding)
    return None if current is None else set(original) | set(current)


def _repair_baseline(repo: Path, proof: dict, receipt: dict, directory: Path, recorded: dict,
                     orchestrator_root: Path) -> bool:
    """A landed subject's later repair source shared only integration history.

    Receipts predating source_task_subject keep their subject, round and
    charge. A repair dispatched from main after its subject's typed landing
    gives a proven sibling no data. The repair's own branch, any head carrying
    repair commits, and a sibling owning (originally or by later authorization)
    a path that the subject's recorded repair heads still change all hold.
    GRPH-32 (practice label); GRPH-09 (Graph Theory with Applications, ch. 8).
    """
    from workbay_handoff_mcp.lanes_api import resolve_lane_by_branch

    from .review_ratchet import _commit, _short_branch, _subject_is_ancestor, task_record
    subject, head = receipt["identity"]["subject_branch"], receipt["source_head"]
    if (receipt.get("source_task_subject") is not None or proof["integration_branch"] != "main"
            or proof["subject_branch"] in {subject, receipt["source_branch"]}):
        return False
    landed = _commit(repo, "refs/heads/main")
    if landed is None or not _subject_is_ancestor(repo, head, landed):
        return False
    response = resolve_lane_by_branch(subject)
    if not response.get("ok"):
        raise ValueError("review_cycle_registry_unavailable")
    lanes = response["data"]["current"]
    if not lanes:
        return False
    for row in lanes:
        task = task_record(row["task_ref"]) or {}
        tip, landing = row.get("branch_tip_sha"), row.get("landing_commit_sha")
        if ((task.get("status") != "done" and (task.get("status") != "in_progress"
                                               or _short_branch(task.get("target_branch")) != "main"))
                or row.get("status") != "merged" or row.get("branch_tip_source") not in {"branch", "backfill"}
                or any(not isinstance(sha, str) or not re.fullmatch("[0-9a-f]{40}", sha) for sha in (tip, landing))
                or _commit(repo, tip) != tip or _commit(repo, landing) != landing
                or not _subject_is_ancestor(repo, tip, landing) or not _subject_is_ancestor(repo, landing, head)):
            return False
    if subject not in recorded:
        recorded[subject] = set()
        for path in directory.glob("*.json"):
            other = json.loads(path.read_text())
            if other.get("source_branch") and other.get("source_head") and other["identity"]["subject_branch"] == subject:
                try:
                    base = _git(repo, "merge-base", landed, other["source_head"])
                    changed = _git(repo, "diff", "--name-only", "--no-renames", base, other["source_head"], "--")
                except subprocess.CalledProcessError as exc:
                    raise ValueError("review_cycle_subject_unresolvable") from exc
                recorded[subject].update(changed.splitlines())
    roots = _authorized_roots(repo, orchestrator_root, proof)
    if roots is None:
        return False
    return not any(path == root or path.startswith(root + "/")
                   for path in recorded[subject] for root in roots)


def _independent_baseline_source(repo: Path, proof: dict, receipt: dict, orchestrator_root: Path) -> bool:
    """A shared starting commit is not an edge between independently owned work.

    Only the host's source-time producer snapshot participates. Reconstructing
    it from a later registration would let an unfinished legacy task shed its
    history. Recheck the original binding and resource partition before
    distinguishing two work units.
    """
    original = receipt.get("source_task_subject")
    if not isinstance(original, dict) or not isinstance(original.get("task_ref"), str):
        return False
    identity = ("task_ref", "lane_id", "subject_branch")
    if (all(original.get(key) == proof.get(key) for key in identity)
            or original.get("subject_branch") != receipt["identity"]["subject_branch"]
            or original.get("integration_branch") != "main" or proof["integration_branch"] != "main"
            or original.get("base_sha") != receipt.get("source_head")
            or original.get("tip_sha") != receipt.get("source_head")):
        return False
    # Both sides' source-time and currently authorized resources participate.
    # An expansion of either the older or the incoming subject must not create
    # an overlap escape around an old receipt; either unauthenticated holds.
    older = _authorized_roots(repo, orchestrator_root, original)
    incoming = None if older is None else _authorized_roots(repo, orchestrator_root, proof)
    if incoming is None:
        return False
    return _disjoint_roots(older, incoming)


def _disjoint_roots(older: set[str], incoming: set[str]) -> bool:
    """A shared baseline is independent only across disjoint owned resources.

    Equal roots or a path-prefix in either direction is an edge.
    GRPH-09 (Graph Theory with Applications, ch. 8); GRPH-32 (practice label).
    """
    return not any(left == right or left.startswith(right + "/") or right.startswith(left + "/")
                   for left in older for right in incoming)


def _terminal_producer_source(repo: Path, proof: dict, receipt: dict, orchestrator_root: Path) -> bool:
    """A terminal work unit's zero-ahead source on main left no unlanded work.

    Only the registry status and Git participate; notes, labels, titles,
    manifests and caller text never do. Every current row naming the
    producer's branches must be closed or merged, every surviving ref landed,
    and its source-time resources disjoint from the incoming subject's.
    Missing or unreadable evidence holds; an operational error raises. Re-read
    on every call, so a reopened row holds again.
    SECD-03, SECD-05 (Security Engineering); DATA-14 (Designing Data-Intensive
    Applications, ch. 11); GRPH-09 (Graph Theory with Applications, ch. 8).
    """
    from workbay_handoff_mcp.lanes_api import resolve_lane_by_branch
    from workbay_handoff_mcp.lanes_recording import CLOSEABLE_LANE_STATUSES

    from .review_ratchet import _commit, _owned_roots, _scope_path, _scope_record, _subject_is_ancestor
    original, head = receipt.get("source_task_subject"), receipt.get("source_head")
    if not isinstance(original, dict) or not isinstance(original.get("task_ref"), str) or not isinstance(head, str):
        return False
    subject = receipt["identity"]["subject_branch"]
    if (original.get("subject_branch") != subject or subject == proof["subject_branch"]
            or original.get("integration_branch") != "main" or proof["integration_branch"] != "main"
            or original.get("base_sha") != head or original.get("tip_sha") != head):
        return False
    landed = _commit(repo, "refs/heads/main")
    if (landed is None or not _subject_is_ancestor(repo, head, landed)
            or not _subject_is_ancestor(repo, proof["base_sha"], landed)):
        return False
    names = {subject, receipt["source_branch"]}
    rows = []
    for name in names:
        response = resolve_lane_by_branch(name)
        if not response.get("ok"):
            raise ValueError("review_cycle_registry_unavailable")
        rows.extend(response["data"]["current"])
    if not rows or any(row.get("status") not in CLOSEABLE_LANE_STATUSES for row in rows):
        return False
    for name in names:
        tip = _commit(repo, f"refs/heads/{name}")
        if tip is None:
            # Only a ref that is really gone is deleted; a dangling one is unreadable.
            result = subprocess.run(["git", "-C", str(repo), "show-ref", "--verify", "--quiet", f"refs/heads/{name}"],
                                    capture_output=True, check=False)
            if result.returncode == 0:
                return False
            if result.returncode != 1:
                raise ValueError("review_cycle_subject_unresolvable")
        elif not _subject_is_ancestor(repo, tip, landed):
            return False
    older = _owned_roots(original.get("owned_paths"))
    if older is None:
        return False
    older = set(older)
    if _scope_path(repo, original).exists():
        record = _scope_record(repo, original)
        if record is None:
            return False
        older |= set(record["authorized_scopes"][-1])
    incoming = _authorized_roots(repo, orchestrator_root, proof)
    return incoming is not None and _disjoint_roots(older, incoming)


class SubjectAmbiguous(ValueError):
    """The legacy refusal reason with the receipts that keep their subjects."""

    def __init__(self, holding: list[dict]) -> None:
        super().__init__("review_cycle_subject_ambiguous")
        self.holding = holding


def subject_hold_report(repo: Path, branch: str, proof: dict | None = None, *,
                        orchestrator_root: Path | None = None) -> list[dict]:
    """One decision table for retained subjects and their holding receipts.

    Preserve the historical first direct-branch match, which bypasses ancestry
    and release checks and ends the walk. Otherwise report only ancestor
    receipts that every applicable release predicate left holding. A stamp's
    presence is diagnostic here; only the existing predicates authenticate it.
    """
    directory = authority(repo) / "review_cycle_dispatch"
    holding, recorded = [], {}
    for path in directory.glob("*.json"):
        receipt = json.loads(path.read_text())
        source_branch, head = receipt.get("source_branch"), receipt.get("source_head")
        if not source_branch or not head:
            continue
        subject = receipt["identity"]["subject_branch"]
        row = dict(receipt=path.name, subject=subject, source_branch=source_branch, source_head=head,
                   has_source_task_subject=receipt.get("source_task_subject") is not None,
                   predicates_tried=[])
        if source_branch == branch:
            row["predicates_tried"] = ["direct_match"]
            holding.append(row)
            return holding
        result = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", head, "HEAD"],
                                capture_output=True, check=False)
        if result.returncode == 0:
            if proof is not None and (
                    _independent_baseline_source(repo, proof, receipt, orchestrator_root or repo)
                    or _terminal_producer_source(repo, proof, receipt, orchestrator_root or repo)):
                continue
            if proof is None or not (_released(repo, proof, subject, source_branch, head)
                                     or _repair_baseline(repo, proof, receipt, directory, recorded,
                                                         orchestrator_root or repo)):
                if proof is not None:
                    row["predicates_tried"] = ["_independent_baseline_source", "_terminal_producer_source",
                                               "_released", "_repair_baseline"]
                holding.append(row)
        elif result.returncode != 1:
            raise ValueError("review_cycle_subject_unresolvable")
    return holding


def retained_subject(repo: Path, branch: str, proof: dict | None = None, *,
                     orchestrator_root: Path | None = None) -> str:
    """Keep the first direct match or a unique subject from the shared report."""
    holding = subject_hold_report(repo, branch, proof, orchestrator_root=orchestrator_root)
    for row in holding:
        if row["source_branch"] == branch:
            return row["subject"]
    matches = {row["subject"] for row in holding}
    if len(matches) > 1:
        raise SubjectAmbiguous(holding)
    if matches:
        return next(iter(matches))
    return proof["subject_branch"] if proof else branch


def _source_task_subject_unproven(repo: Path, spec: dict, subject: str) -> dict | None:
    """Source-time evidence only; this record never releases a retained receipt.

    Use the same registry manifest/default integration as the proof producer.
    A missing integration ref stays unproven without preventing an otherwise
    valid mint; operational Git failures still raise through the usual boundary.
    """
    from .review_ratchet import _commit, _manifest_lane, _short_branch, _subject_is_ancestor, proven_task_subject

    branch = spec.get("branch") or _git(repo, "symbolic-ref", "--short", "HEAD")
    registered = registered_lane(repo, branch)
    if registered is None:
        return None
    config, lane = registered
    candidate = proven_task_subject(repo, config.workspace_root, lane["task_ref"], branch=subject)
    reason = "proof_subject_mismatch" if candidate is not None and candidate["subject_branch"] != subject else "no_producer_proof"
    located = _manifest_lane(config.workspace_root, lane["task_ref"], lane["lane_id"])
    integration = (_short_branch(located[0].get("integration_branch")) if located else None) or "main"
    landed = _commit(repo, f"refs/heads/{integration}")
    return dict(reason=reason, head_on_integration=landed is not None and _subject_is_ancestor(repo, spec["head_sha"], landed),
                integration_sha=landed)


def validate_shell_subject(repo: Path, spec: dict, subject: str, dispatch_id: str) -> dict | None:
    from .review_ratchet import proven_task_subject, source_task_subject_holds

    branch = spec.get("branch") or _git(repo, "symbolic-ref", "--short", "HEAD")
    registered = registered_lane(repo, branch)
    if registered:
        config, lane = registered
        expected = canonical_subject(repo, config.state_dir, task_ref=lane["task_ref"], branch=branch,
                                     pass_id=dispatch_id, orchestrator_root=config.workspace_root)
    else:
        expected = retained_subject(repo, branch)
    if subject != expected:
        raise ValueError("review_cycle_canonical_subject_mismatch")
    # Stamp only a producer proof consumed by this boundary, never a value
    # supplied in spec/notes or inferred later from branch/task labels.
    if registered:
        proof = proven_task_subject(repo, config.workspace_root, lane["task_ref"], branch=subject)
        if proof is not None and proof["subject_branch"] == subject:
            return proof
        # An already-started source retains its original identity baseline as
        # main advances. Only protected source-time receipts can supply it.
        for path in (authority(repo) / "review_cycle_dispatch").glob("*.json"):
            prior = json.loads(path.read_text())
            snapshot = prior.get("source_task_subject")
            if (prior.get("identity", {}).get("subject_branch") == subject
                    and isinstance(snapshot, dict) and snapshot.get("subject_branch") == subject
                    and snapshot.get("task_ref") == lane["task_ref"]
                    and source_task_subject_holds(repo, config.workspace_root, snapshot)):
                return snapshot
    return None


def source_review_scope(repo: Path, spec: dict, subject: str) -> dict:
    """Freeze the current integration-to-candidate range, never the identity base.

    native-review-scope-v1: only host registry/manifest and Git supply authority.
    Spec/model scope declarations are ignored. Both shell paths call this before
    transport; collection consumes the frozen receipt even after main advances.
    """
    from .review_ratchet import _commit, _manifest_lane, _short_branch, _subject_is_ancestor

    branch, tip = spec.get("branch"), spec.get("head_sha")
    registered = registered_lane(repo, branch) if isinstance(branch, str) else None
    if registered is None:
        raise ValueError("review_cycle_review_scope_unavailable")
    config, lane = registered
    located = _manifest_lane(config.workspace_root, lane["task_ref"], lane["lane_id"])
    if located is None:
        raise ValueError("review_cycle_review_scope_unavailable")
    integration = _short_branch(located[0].get("integration_branch")) or "main"
    base = _commit(repo, f"refs/heads/{integration}")
    if (not isinstance(tip, str) or not re.fullmatch("[0-9a-f]{40}", tip)
            or base is None or _commit(repo, tip) != tip
            or _commit(repo, f"refs/heads/{branch}") != tip or _commit(repo, "HEAD") != tip
            or not _subject_is_ancestor(repo, base, tip)
            or not _git(repo, "diff", "--name-only", "--no-renames", base, tip, "--")):
        raise ValueError("review_cycle_review_scope_mismatch")
    return dict(schema_version=1, task_ref=lane["task_ref"], lane_id=lane["lane_id"], source_branch=branch,
                review_cycle_subject_branch=subject, integration_branch=integration, base_sha=base,
                tip_sha=tip, source_tree=_git(repo, "rev-parse", tip + "^{tree}"))


def harvest_shell_gate(repo: Path, receipt: dict, result: dict) -> dict | None:
    """Late join to C19's independently owned native evidence consumer."""
    from .native_review_evidence import harvest_native_review

    return harvest_native_review(repo, receipt, result)


def shell_collect() -> None:
    """Observe and settle an existing raw job; never reserve during collection."""
    repo, dispatch_id, bundle, recorded_rc = Path.cwd(), sys.argv[1], Path(sys.argv[2]), sys.argv[3]
    path = _receipt_path(repo, dispatch_id)
    if not path.exists():
        return  # observation of a legacy job
    receipt = json.loads(path.read_text())
    if receipt.get("settlement_owner") != "shell":
        return  # engine completion must harvest before settling its gate
    def read(name):
        try:
            value = json.loads((bundle / "artifacts" / name).read_text())
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}
    result, provenance = read("result-out"), read("provenance-out")
    patch = (bundle / "stdout").stat().st_size > 0
    outcome = native_collect_outcome(bundle, dispatch_id, recorded_rc, receipt=receipt)
    receipt = _publish_native_completion(path, recorded_rc, outcome)
    # Like shell_entry, only a collected result is a review product to harvest.
    harvest = harvest_shell_gate(repo, receipt, result) if receipt["identity"]["kind"] == "gate" and result else None
    settled = settle_actual(repo, receipt["identity"], outcome=outcome, judgment=bool(result), commit_landed=patch,
                            model_started=provenance.get("model_process_started") is True, harvest=harvest)
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        receipt = json.loads(path.read_text())
        receipt["settlement"] = asdict(settled)
        _save(path, receipt)
    print("remote_agent: review_cycle " + json.dumps(asdict(settled)), file=sys.stderr)
