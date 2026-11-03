"""Authenticated owner-maintenance qualification, without dispatch or settlement.

Public command: python -m workbay_orchestrator_mcp.orchestration.review_cycle_bootstrap
Read one JSON object on stdin (CHECK_FIELDS below). Exit 0 means qualified for
maintenance inspection; exit 2 is an evidence hold. Neither is model admission.
No apply operation exists: unproven legacy custody cannot safely be repaired by
writing a new source stamp, consuming an approval, or rewriting native history.

Canon v0.26.2: GRPH-02 A cycle is one indivisible unit (Graph Theory with
Applications, ch. 10): source authentication must not depend on the discovery
path being repaired. DATA-14 No dual writes (Designing Data-Intensive
Applications, ch. 11): the native ledger remains the only consumption authority.
AGT-10 Degrade loudly (The Art of UNIX Programming, ch. 1): missing original
producer facts are typed holds, never inferred permissions.
"""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import quote

from . import review_cycle_dispatch as cycle
from . import review_ratchet as ratchet
from .review_cycle_ledger import ReviewCycleLedger, _Hold, read_approvals, subject_accounting
from .review_cycle_reconciliation import (
    EvidenceHold,
    _host,
    _read,
    _require,
    bootstrap_charged_subject,
    bootstrap_holder_report,
)

CHECK_FIELDS = frozenset({
    "operation", "task_ref", "lane_id", "row_id", "identity_seq", "cycle_epoch", "branch",
    "worktree_path", "source_head", "source_tree", "approval_id", "round",
})
_OID = re.compile(r"[0-9a-f]{40}")


@dataclass
class BootstrapReceipt:
    schema: str = "workbay/review-cycle-bootstrap@1"
    status: str = "evidence_hold"
    reason: str = ""
    identity: dict = field(default_factory=dict)
    source_authentication: dict = field(default_factory=lambda: {"status": "unverified"})
    current_authorization: dict = field(default_factory=lambda: {"status": "unverified"})
    # Diagnostic only: the retained subject native dispatch charges, never an input.
    charged_subject: dict = field(default_factory=lambda: {"status": "unverified"})
    grant: dict = field(default_factory=lambda: {"state": "unverified"})
    holders: list = field(default_factory=list)
    producer_records: dict = field(default_factory=dict)
    required_host_evidence: str | None = None
    maintenance_admission: bool = False
    native_admission: bool = False
    model_started: bool = False
    OrcaDispatch: None = None
    observed_served_model: None = None
    # This observation is never a transfer token or a persisted authorization.
    execution_authorization: str = "not_granted; native owner must reauthenticate before dispatch"


def _lane(config, row_id: int) -> dict | None:
    """No schema initialization, migrations, roster mutation or write transaction."""
    try:
        with closing(sqlite3.connect("file:" + quote(str(config.db_path)) + "?mode=ro", uri=True, timeout=0.05)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (row_id,)).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None
    except sqlite3.Error as exc:
        raise EvidenceHold("native_registry_unavailable", "existing canonical worktree_lanes row") from exc


def _source(repo: Path, request: dict) -> None:
    _require(
        str(repo) == request["worktree_path"]
        and cycle._git(repo, "rev-parse", "--show-toplevel") == str(repo)
        and cycle._git(repo, "symbolic-ref", "--short", "HEAD") == request["branch"]
        and cycle._git(repo, "rev-parse", "HEAD") == request["source_head"]
        and cycle._git(repo, "rev-parse", "HEAD^{tree}") == request["source_tree"]
        and cycle._git(repo, "rev-parse", "refs/heads/" + request["branch"]) == request["source_head"],
        "source_changed", "exact registered worktree, branch, source commit and tree",
    )


def check(request: dict, *, repo: Path | None = None) -> dict:
    """Re-read native authority; qualify source/authorization/grant/holders separately.

    Request fields are exact expected identities, not authority. No scope,
    snapshot, producer path, test flag, route or policy can be supplied by a caller.
    Qualification neither reserves a round nor clears a native evidence hold.
    The grant and counters belong to the subject native dispatch would charge
    (``retained_subject``), which can differ from the incoming source proof.
    """
    result = BootstrapReceipt()
    try:
        _require(
            isinstance(request, dict) and set(request) == CHECK_FIELDS and request.get("operation") == "check",
            "invalid_protocol", "exact check schema; apply and caller-supplied authority are unsupported",
        )
        _require(
            all(type(request[k]) is int and request[k] > 0 for k in ("row_id", "identity_seq", "cycle_epoch", "round"))
            and all(isinstance(request[k], str) and request[k] and len(request[k]) <= 4096
                    for k in CHECK_FIELDS - {"operation", "row_id", "identity_seq", "cycle_epoch", "round"})
            and all(_OID.fullmatch(request[k]) for k in ("source_head", "source_tree")),
            "invalid_identity", "positive native row/identity/cycle/round and exact Git source",
        )
        repo = Path(repo or Path.cwd()).resolve()
        primary, private = _host(repo)
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime

        config = RuntimeConfig.for_repo(primary)
        configure_runtime(config)
        row = _lane(config, request["row_id"])
        expected = {k: request[k] for k in ("task_ref", "lane_id", "identity_seq", "cycle_epoch", "branch", "worktree_path")}
        _require(row is not None and all(row.get(k) == v for k, v in expected.items()),
                 "native_identity_changed", "original canonical row id, task/lane, identity_seq, cycle_epoch, branch and worktree")
        result.identity = {"row_id": row["id"], **expected, "round": request["round"]}
        _require(row.get("status") in {"active", "blocked", "review"} and row.get("lane_kind") == "implement",
                 "current_authorization_unavailable", "unfinished canonical implementation lane")
        _source(repo, request)
        proof = ratchet.task_subject_binding(row)
        _require(isinstance(proof, dict) and proof.get("task_ref") == request["task_ref"]
                 and proof.get("lane_id") == request["lane_id"] and proof.get("subject_branch") == request["branch"],
                 "source_authentication_unavailable", "original registry-produced task_subject binding")
        # This is the existing historical authentication seam. Do not change
        # proven_task_subject globally or promote ancestry to source authority.
        roots = ratchet.source_task_subject_scope(repo, primary, proof)
        _require(roots is not None, "source_authentication_unavailable",
                 "source_task_subject_scope authenticated original binding and current producer scope")
        scope_path = ratchet._scope_path(repo, proof)
        scope_record, scope_sha = _read(scope_path, private)
        _require(ratchet._scope_record(repo, proof) == scope_record,
                 "source_authentication_unavailable", "private original scope publication from public lane producer")
        result.producer_records[str(scope_path)] = scope_sha
        result.source_authentication = dict(status="authenticated", binding=proof,
            source_head=request["source_head"], source_tree=request["source_tree"])
        # Historical scope proves identity; present edits must independently
        # remain in the currently authorized resource set.
        integration = cycle._git(repo, "rev-parse", "refs/heads/" + proof["integration_branch"])
        base = cycle._git(repo, "merge-base", integration, request["source_head"])
        changed = cycle._git(repo, "diff", "--name-only", "--no-renames", base, request["source_head"], "--").splitlines()
        _require(all(any(p == root or p.startswith(root + "/") for root in roots) for p in changed),
                 "current_source_outside_authorization", "current source delta within producer-authorized roots")
        _require(not cycle._git(repo, "status", "--porcelain", "--untracked-files=all"),
                 "worktree_changed", "clean owned source checkout; uncommitted source requires separate native custody")
        result.current_authorization = dict(status="verified", owned_paths=roots, integration_sha=integration,
                                            source_base=base, changed_files=changed)
        ledger = ReviewCycleLedger(private / "review_cycle_ledger.json", cycle.trusted_policy())
        _, ledger_sha = _read(ledger.path, private)
        data = ledger._read()
        result.producer_records[str(ledger.path)] = ledger_sha
        approvals = read_approvals(private)
        matches = [a for a in approvals if a["approval_id"] == request["approval_id"]]
        _require(len(matches) == 1, "grant_unavailable", "one authentic original native approval id")
        approval = matches[0]
        # File custody and original bytes, not an approval-shaped JSON request.
        approval_path = private / "review_cycle_approvals" / (approval["approval_id"] + ".json")
        _require(approval_path.parent == private / "review_cycle_approvals", "grant_unavailable", "native approval locator")
        original, approval_sha = _read(approval_path, private)
        _require(approval == {**original, "sha256": approval_sha}, "grant_mismatch", "original native grant bytes")
        # The source proof stays the incoming identity. Native dispatch charges
        # the retained subject, so the grant and counters are that subject's.
        result.charged_subject, receipt_records = bootstrap_charged_subject(repo, primary, proof, data)
        _require(result.charged_subject["status"] == "derived", "charged_subject_ambiguous",
                 "one applicable retained subject; native settlement/release evidence for the other holders")
        charged = result.charged_subject["subject_branch"]
        _require((approval["subject_branch"], approval["kind"], approval["round"]) == (charged, "implement", request["round"]),
                 "grant_mismatch", "original native grant for this exact charged subject/kind/round")
        consumed = any((r.get("operator_approval") or {}).get("approval_id") == approval["approval_id"]
                       for r in data["rows"].values())
        next_round = subject_accounting(data, charged, ledger.policy)["charged_rows"]["implement"] + 1
        _require(not consumed and next_round == request["round"], "grant_stale_or_consumed",
                 "unconsumed original native grant for the current exact next round; no refunds or resets")
        result.producer_records[str(approval_path)] = approval_sha
        result.grant = dict(approval_id=approval["approval_id"], sha256=approval_sha, state="unconsumed",
                            subject_branch=approval["subject_branch"], kind="implement", round=approval["round"])
        report = bootstrap_holder_report(repo, primary, config, proof, data)
        directory = private / "review_cycle_dispatch"
        _require({p: sha for p, sha in report["producer_records"].items() if Path(p).parent == directory}
                 == receipt_records, "stale_observation", "unchanged native receipts since charged-subject derivation")
        result.holders = report["holders"]
        result.producer_records.update(report["producer_records"])
        _require(_lane(config, request["row_id"]) == row, "native_identity_changed", "unchanged original native lane row")
        _source(repo, request)
        _require(ratchet.source_task_subject_scope(repo, primary, proof) == roots,
                 "source_authentication_changed", "unchanged source authentication and current authorized scope")
        _require(all(_read(Path(p), private if Path(p).is_relative_to(private) else config.state_dir)[1] == sha
                     for p, sha in result.producer_records.items()),
                 "stale_observation", "unchanged canonical ledger, grant, scope and holder producer bytes")
        _require(not result.holders, "retained_subject_evidence_hold",
                 "original source-time scope/custody and native settlement/landing evidence for each listed holder")
        result.status = "qualified"
        result.maintenance_admission = True
    except EvidenceHold as exc:
        result.reason, result.required_host_evidence = str(exc), exc.needed
    except (_Hold, OSError, ValueError, KeyError, TypeError, AttributeError, sqlite3.Error) as exc:
        result.reason = str(exc) if isinstance(exc, _Hold) else "native_authority_unavailable"
        result.required_host_evidence = "readable authentic native Git/registry/source/grant/holder authority; no export substitution"
    except subprocess.SubprocessError:
        # Git operational errors contain no permission. Never print producer
        # bytes or exception text that might include secrets.
        result.reason = "native_source_unreadable"
        result.required_host_evidence = "resolvable original Git source and worktree"
    return asdict(result)


def main() -> int:
    try:
        raw = sys.stdin.read(65537)
        request = json.loads(raw) if len(raw) <= 65536 else None
    except (ValueError, UnicodeError):
        request = None
    result = check(request)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "qualified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
