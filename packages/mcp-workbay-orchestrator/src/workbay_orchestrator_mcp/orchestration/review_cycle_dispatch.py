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


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True, check=True)
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


def dispatch(request: dict, *, repo: Path | None = None) -> LedgerDecision:
    """Frozen stdin protocol; root/policy/threshold are never request fields."""
    try:
        operation = request["operation"]
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
            proposal = request.get("canon_proposal_path")
            return ledger.reserve(**identity, canon_proposal_path=Path(proposal) if proposal else None)
        return ledger.settle(**identity, **{key: value for key, value in request.items()
                                          if key not in {"operation", *identity}})
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        return LedgerDecision("ledger_hold", str(exc))


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
            canon_proposal_path: str | None = None, settlement_owner: str = "shell") -> dict:
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
        _save(path, receipt)
    return receipt


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
    def normalized(value):
        return {"claude-opus-5-5": "opus-5.5", "claude-opus-5.5": "opus-5.5"}.get(value, value)
    if (len(models) != 1 or normalized(models[0]) != normalized(expected)
            or len(efforts) != 1 or efforts[0].casefold() != required["effort"]):
        raise ValueError("review_cycle_required_route_mismatch")
    backend = spec.get("backend_id")
    if (expected.startswith("gpt-") and backend != "codex-remote") or (
            expected.startswith("opus") and backend not in {"claude-code", "claude-remote"}):
        raise ValueError("review_cycle_required_backend_mismatch")


def shell_reserve(repo: Path, spec_path: Path, *, dispatch_id: str, subject_branch: str,
                  kind: str, canon_proposal_path: str | None = None, token: str | None = None) -> dict:
    """One-use engine→shell transfer; raw shell entries reserve here themselves."""
    try:
        spec = json.loads(spec_path.read_text())
        validate_shell_subject(repo, spec, subject_branch, dispatch_id)
        if token:
            path = _receipt_path(repo, dispatch_id)
            with path.with_suffix(".lock").open("a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                receipt = json.loads(path.read_text())
                if (receipt.get("token") != token or receipt.get("state") != "prepared"
                        or receipt["identity"] != dict(dispatch_id=dispatch_id, subject_branch=subject_branch, kind=kind)):
                    raise ValueError("dispatch_transfer_invalid_or_consumed")
                # Re-read canonical ledger and trusted context on the consumer.
                decision = dispatch(dict(operation="reserve", **receipt["identity"]), repo=repo)
                if decision.status != "reserved" or decision.row.get("completion") is not None:
                    raise ValueError("dispatch_transfer_not_pending")
                enforce_route(spec, decision.row)
                receipt["state"] = "transport_started"
                receipt.update(source_branch=spec.get("branch"), source_head=spec.get("head_sha"),
                               source_tree=_git(repo, "rev-parse", spec["head_sha"] + "^{tree}"))
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


def main() -> None:
    try:
        request = json.load(sys.stdin)
        result = dispatch(request)
    except (ValueError, TypeError) as exc:
        result = LedgerDecision("ledger_hold", str(exc))
    print(json.dumps(asdict(result)))


if __name__ == "__main__":
    main()


def shell_entry() -> None:
    """Private shell bridge; the public module stdin protocol stays reserve/settle."""
    mode, dispatch_id, subject, kind, spec, proposal = sys.argv[1:7]
    repo = Path.cwd()
    if mode == "reserve":
        receipt = shell_reserve(repo, Path(spec), dispatch_id=dispatch_id, subject_branch=subject, kind=kind,
                                canon_proposal_path=proposal or None,
                                token=os.environ.get("WORKBAY_REVIEW_CYCLE_TOKEN"))
        decision = receipt["decision"]
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
        settlement = settle_actual(repo, receipt["identity"], outcome=outcome, model_started=started,
                                   judgment=bool(result), commit_landed=patch,
                                   harvest=harvest_shell_gate(repo, receipt, result) if kind == "gate" and result else None)
        decision = asdict(settlement)
        path = _receipt_path(repo, dispatch_id)
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
    if context is None:
        configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
        return retained_subject(repo, branch, proof=proven_task_subject(repo, orchestrator_root, task_ref))
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
    """A proven separate task releases only a completed task's already-landed subject."""
    from workbay_handoff_mcp.lanes_api import resolve_lane_by_branch

    from .review_ratchet import task_record
    if subject == proof["subject_branch"]:
        return False
    result = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", head, proof["base_sha"]],
                            capture_output=True, check=False)
    if result.returncode == 1:
        return False
    if result.returncode:
        raise ValueError("review_cycle_subject_unresolvable")
    owners = set()
    for name in {subject, source_branch}:
        response = resolve_lane_by_branch(name)
        if not response.get("ok"):
            raise ValueError("review_cycle_registry_unavailable")
        owners |= {row["task_ref"] for row in response["data"]["current"]}
    return (bool(owners) and proof["task_ref"] not in owners
            and all((task_record(owner) or {}).get("status") == "done" for owner in owners))


def retained_subject(repo: Path, branch: str, proof: dict | None = None) -> str:
    """A child without a registry cannot silently abandon a retained subject.

    An ancestry match is a hold against fresh authority, not proof of a new
    task. A validated registry binding takes precedence over this fallback;
    a producer ``proof`` releases only subjects that :func:`_released` allows.
    """
    directory = authority(repo) / "review_cycle_dispatch"
    matches = set()
    for path in directory.glob("*.json"):
        receipt = json.loads(path.read_text())
        source_branch, head = receipt.get("source_branch"), receipt.get("source_head")
        if not source_branch or not head:
            continue
        subject = receipt["identity"]["subject_branch"]
        if source_branch == branch:
            return subject
        result = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", head, "HEAD"],
                                capture_output=True, check=False)
        if result.returncode == 0:
            if proof is None or not _released(repo, proof, subject, source_branch, head):
                matches.add(subject)
        elif result.returncode != 1:
            raise ValueError("review_cycle_subject_unresolvable")
    if len(matches) > 1:
        raise ValueError("review_cycle_subject_ambiguous")
    if matches:
        return next(iter(matches))
    return proof["subject_branch"] if proof else branch


def validate_shell_subject(repo: Path, spec: dict, subject: str, dispatch_id: str) -> None:
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


def harvest_shell_gate(repo: Path, receipt: dict, result: dict) -> dict | None:
    """Use the existing canonical harvest, with a durable registry identity."""
    registered = registered_lane(repo, receipt.get("source_branch") or receipt["identity"]["subject_branch"])
    if registered is None:
        return None
    config, lane = registered
    from .offload_pass import _harvest_review_findings
    route = receipt["decision"]["row"]["required_route"]
    identity = dict(pass_id=receipt["identity"]["dispatch_id"], dispatch_id=receipt["identity"]["dispatch_id"],
                    task_ref=lane["task_ref"], lane_id=lane["lane_id"], branch=receipt["source_branch"],
                    base_sha=receipt["source_head"], tip_sha=receipt["source_head"],
                    backend="codex-remote", model=route["model"], reasoning_effort=route["effort"],
                    worktree_path=str(repo), agent_stream_path=None,
                    review_cycle_subject_branch=receipt["identity"]["subject_branch"],
                    review_cycle=receipt["decision"])
    return _harvest_review_findings(result, None, task_ref=lane["task_ref"], lane_id=lane["lane_id"],
                                   session="review-cycle:" + identity["pass_id"], orchestrator_root=config.workspace_root,
                                   receipt_path=config.state_dir / f"review-receipt-{identity['pass_id']}.json",
                                   receipt_identity=identity)


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
    # Like shell_entry, only a collected result is a review product to harvest.
    harvest = harvest_shell_gate(repo, receipt, result) if receipt["identity"]["kind"] == "gate" and result else None
    outcome = native_collect_outcome(bundle, dispatch_id, recorded_rc, receipt=receipt)
    settled = settle_actual(repo, receipt["identity"], outcome=outcome, judgment=bool(result), commit_landed=patch,
                            model_started=provenance.get("model_process_started") is True, harvest=harvest)
    with path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        receipt = json.loads(path.read_text())
        receipt["native_completion"] = dict(rc=recorded_rc, outcome=outcome)
        receipt["settlement"] = asdict(settled)
        _save(path, receipt)
    print("remote_agent: review_cycle " + json.dumps(asdict(settled)), file=sys.stderr)
