"""Read-only collection of completion facts; no actuator imports or writes.

``handoff_reader`` supplies the existing ``get_verified_tests`` and
``list_review_findings`` public APIs (for example, handoff.core). Calls use
one bounded page, never a passing-only filter. The public readers own their
connections, so collect and re-read full rows rather than pretending they
share a SQLite transaction. Changes invalidate the entire observation. This
is a preview; apply-time epoch/version fencing belongs to the actuator slice.

Acceptance kind ``verified_test``, version 1, uses artifact format
``completion_acceptance_v1``: the test's JSON ``result`` must contain
acceptance_id, contract_revision, contract_hash, verifier_version and outcome.
It also requires the normal passing/exit/evidence and exact-SHA test binding.
No command is executed here. Other kinds/versions/formats are unavailable.
Candidate targets naming a proposed merge tree also hold: this receipt format
does not bind a tree OID and integration parent, so it cannot verify that tree.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import selectors
import subprocess
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from workbay_handoff_mcp import lane_completion_production as production

from . import lane_completion as lc
from . import lane_production_coverage as coverage

_MAX_BYTES = 512 * 1024
_MAX_PATHS = 200
_OID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
_PRESENT = lc.EvidenceState.PRESENT
_ABSENT = lc.EvidenceState.OBSERVED_ABSENT
_UNKNOWN = lc.EvidenceState.UNAVAILABLE


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _git(root, *args, deadline, allowed=(0,)):
    """Cap elapsed time and pipe bytes, including an unexpectedly large diff."""
    remaining = min(5.0, deadline - time.monotonic())
    if remaining <= 0:
        raise TimeoutError("completion git deadline")
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    argv = ["git", "--no-replace-objects", "--literal-pathspecs", "-c", "core.fsmonitor=false", "-C", str(root), *args]
    with subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env) as proc:
        output = bytearray()
        end = time.monotonic() + remaining
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(proc.stdout, selectors.EVENT_READ)
                while True:
                    wait = end - time.monotonic()
                    if wait <= 0 or not selector.select(wait):
                        raise TimeoutError("completion git timeout")
                    chunk = os.read(proc.stdout.fileno(), min(65536, _MAX_BYTES + 1 - len(output)))
                    if not chunk:
                        break
                    output.extend(chunk)
                    if len(output) > _MAX_BYTES:
                        raise ValueError("completion git output truncated")
            code = proc.wait(timeout=max(0.001, end - time.monotonic()))
            if code not in allowed:
                raise ValueError("completion git probe failed")
            return bytes(output), code
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=1)


def _resolve(root, ref, deadline):
    raw, _ = _git(root, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}", deadline=deadline)
    oid = raw.decode("ascii").strip()
    if not _OID.fullmatch(oid):
        raise ValueError("invalid commit oid")
    return oid


def _ancestor(root, commit, tip, deadline):
    # Resolve first: a missing/shallow object is unknown, never non-ancestry.
    if not isinstance(commit, str) or not _OID.fullmatch(commit):
        raise ValueError("finding/landing evidence must bind an immutable oid")
    commit = _resolve(root, commit, deadline)
    _, code = _git(root, "merge-base", "--is-ancestor", commit, tip, deadline=deadline, allowed=(0, 1))
    if code == 1:
        shallow, _ = _git(root, "rev-parse", "--is-shallow-repository", deadline=deadline)
        if shallow.strip() == b"true":
            raise ValueError("incomplete ancestry history")
    return code == 0


def _product(root, identity, target, deadline):
    base = _resolve(root, identity.admission_base, deadline)
    if base != identity.admission_base:
        raise ValueError("admission base must be pinned")
    tip = _resolve(root, identity.product_tip, deadline)
    raw, _ = _git(
        root,
        "diff",
        "--raw",
        "--no-abbrev",
        "--no-renames",
        "--no-ext-diff",
        "--no-textconv",
        "-z",
        base,
        tip,
        "--",
        deadline=deadline,
    )
    records = raw.split(b"\0")
    changes = {}
    for index in range(0, len(records) - 1, 2):
        header, path = records[index : index + 2]
        old_mode, mode, old_oid, oid, status = header.removeprefix(b":").split()
        if status not in (b"A", b"M", b"D", b"T") or not path:
            raise ValueError("unsupported product manifest")
        changes[path] = None if mode == b"000000" else (mode, oid)
        if len(changes) > _MAX_PATHS:
            raise ValueError("product manifest truncated")
    if not changes:
        return _ABSENT, (), False
    assessed = tip if target.phase == lc.CompletionPhase.CANDIDATE else target.integration_sha
    tree, _ = _git(root, "ls-tree", "-z", assessed, "--", *(os.fsdecode(p) for p in changes), deadline=deadline)
    actual = {}
    for record in tree.split(b"\0"):
        if record:
            header, path = record.split(b"\t", 1)
            mode, kind, oid = header.split()
            actual[path] = (mode, oid)
    matches = all(actual.get(path) == expected for path, expected in changes.items())
    manifest = {
        os.fsdecode(path): None if item is None else [part.decode("ascii") for part in item]
        for path, item in changes.items()
    }
    return _PRESENT if matches else _ABSENT, (f"git:manifest:{_digest(manifest)}:{assessed}",), True


def _page(payload, key, limit, task_ref):
    """Accept only complete producer envelopes, including count consistency."""
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return None
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("task_ref") != task_ref:
        return None
    for section in (payload, data):
        truncation = section.get("truncation")
        if section.get("has_more", False) is not False or section.get("truncated", False) is not False:
            return None
        if truncation and (not isinstance(truncation, dict) or truncation.get("truncated", True) is not False):
            return None
    rows = data.get(key)
    if (
        not isinstance(rows, list)
        or len(rows) > limit
        or any(not isinstance(row, dict) for row in rows)
        or data.get("has_more") is not False
        or type(data.get("total_matching")) is not int
        or data["total_matching"] != len(rows)
        or data.get("returned") != len(rows)
    ):
        return None
    if len(json.dumps(payload).encode()) > _MAX_BYTES:
        return None
    # Detach mutable API results immediately, before any subsequent read.
    return json.loads(json.dumps(rows))


def _read(reader, identity, limit):
    tests = findings = None
    try:
        tests = _page(
            reader.get_verified_tests(
                task_ref=identity.task_ref,
                lane_id=identity.lane_id,
                branch=identity.branch,
                limit=limit,
                offset=0,
            ),
            "tests",
            limit,
            identity.task_ref,
        )
    except Exception:  # API/transport failures are unavailable, never empty.
        pass
    try:
        findings = _page(
            reader.list_review_findings(
                task_ref=identity.task_ref,
                status="all",
                detail="full",
                limit=limit,
                offset=0,
            ),
            "findings",
            limit,
            identity.task_ref,
        )
    except Exception:
        pass
    return tests, findings


def _timestamp(value):
    if not isinstance(value, str) or not value:
        raise ValueError("missing row timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _latest(rows, identity, key, timestamp, *, lane_scoped=False):
    if rows is None:
        return None
    latest = {}
    for row in rows:
        if (
            row.get("task_ref") != identity.task_ref
            or type(row.get("id")) is not int
            or row["id"] < 1
            or not isinstance(row.get(key), str)
            or not row[key]
        ):
            raise ValueError("malformed evidence row")
        if lane_scoped and (row.get("lane_id") != identity.lane_id or row.get("branch") != identity.branch):
            raise ValueError("test identity mismatch")
        order = (_timestamp(row.get(timestamp)), row["id"])
        previous = latest.get(row[key])
        if previous is None or order > previous[0]:
            latest[row[key]] = (order, row)
    return {key: value[1] for key, value in latest.items()}


def _row_ref(kind, row):
    return f"{kind}:{row['id']}:{_digest(row)}"


def _test_state(row, assessed_sha):
    if row is None:
        return _ABSENT
    if row.get("commit_sha") != assessed_sha:
        return _ABSENT
    if type(row.get("passed")) is not bool or type(row.get("exit_code")) is not int:
        return _UNKNOWN
    if not isinstance(row.get("result"), str) or not row["result"].strip():
        return _UNKNOWN
    if row["passed"] is False and row["exit_code"] != 0:
        return _ABSENT
    if row["passed"] is True and row["exit_code"] == 0:
        return _PRESENT
    return _UNKNOWN


def _combine(states):
    return _UNKNOWN if _UNKNOWN in states else _ABSENT if _ABSENT in states else _PRESENT


def _findings(root, rows, contract, target, deadline):
    if rows is None:
        return _UNKNOWN, (), ()
    states, refs, unmet = [], ["findings:complete-hot-page"], []
    for fid in contract.finding_ids:
        row = rows.get(fid)
        if row is None:
            # The public API exposes hot rows only. A moved-to-archive row
            # cannot be used as a live resolution receipt.
            states.append(_ABSENT)
            continue
        refs.append(_row_ref("finding", row))
        status = row.get("status")
        if status in ("open", "deferred", "wontfix"):
            states.append(_ABSENT)
            unmet.append(lc.Obligation(lc.ObligationKind.FINDING, fid))
        elif status == "integrated" or (
            status == "resolved_on_branch" and target.phase == lc.CompletionPhase.CANDIDATE
        ):
            prefix = "integrated" if status == "integrated" else "resolved_on_branch"
            commit = row.get(f"{prefix}_at_commit")
            ref = row.get(f"{prefix}_at_ref") if prefix == "integrated" else row.get("resolved_on_branch_ref")
            stamp = row.get(f"{prefix}_at_ts")
            if prefix == "integrated":
                ref = row.get("integrated_at_ref")
                stamp = row.get("integrated_at_ts")
            if not commit or not ref or not stamp or not row.get("verification_evidence"):
                states.append(_UNKNOWN)
                continue
            _timestamp(stamp)
            assessed = target.candidate_sha if target.phase == lc.CompletionPhase.CANDIDATE else target.integration_sha
            expected_ref = contract.identity.branch if prefix == "resolved_on_branch" else target.integration_ref
            states.append(_PRESENT if ref == expected_ref and _ancestor(root, commit, assessed, deadline) else _ABSENT)
        elif status in ("fixed", "resolved_on_branch", "superseded"):
            states.append(_ABSENT)
        else:
            states.append(_UNKNOWN)
    return _combine(states), tuple(refs), tuple(unmet)


def _tests(rows, contract, assessed_sha):
    if rows is None:
        return _UNKNOWN, (), ()
    states, refs, unmet = [], ["tests:complete-page"], []
    for command in contract.required_commands:
        row = rows.get(command)
        state = _test_state(row, assessed_sha)
        states.append(state)
        if row is not None:
            refs.append(_row_ref("verified_test", row))
            if state == _ABSENT and row.get("commit_sha") == assessed_sha and row.get("passed") is False:
                unmet.append(lc.Obligation(lc.ObligationKind.TEST, command))
    return _combine(states), tuple(refs), tuple(unmet)


def _acceptance(rows, contract, assessed_sha, *, merge_tree_oid=None):
    if rows is None or merge_tree_oid is not None or not lc._verifiers_valid(contract):
        return _UNKNOWN, (), ()
    states, refs, unmet = [], ["acceptance:complete-page"], []
    for verifier in contract.acceptance_verifiers:
        if (
            verifier.kind != "verified_test"
            or verifier.version != 1
            or verifier.artifact_format != "completion_acceptance_v1"
            or verifier.expected_outcome != "passed"
        ):
            states.append(_UNKNOWN)
            continue
        row = rows.get(verifier.command_or_check_id)
        state = _test_state(row, assessed_sha)
        if row is None:
            states.append(_ABSENT)
            continue
        refs.append(_row_ref("verified_test", row))
        try:
            receipt = json.loads(row["result"])
        except (ValueError, TypeError):
            receipt = None
        if (
            not isinstance(receipt, dict)
            or receipt.get("acceptance_id") != verifier.acceptance_id
            or type(receipt.get("contract_revision")) is not int
            or receipt["contract_revision"] != contract.revision
            or receipt.get("contract_hash") != contract.contract_hash
            or type(receipt.get("verifier_version")) is not int
            or receipt["verifier_version"] != verifier.version
        ):
            states.append(_UNKNOWN)
        elif receipt.get("outcome") == "failed" and state == _ABSENT and row.get("commit_sha") == assessed_sha:
            states.append(_ABSENT)
            unmet.append(lc.Obligation(lc.ObligationKind.ACCEPTANCE, verifier.acceptance_id))
        elif receipt.get("outcome") == verifier.expected_outcome:
            states.append(state)
        else:
            states.append(_UNKNOWN)
    return _combine(states), tuple(refs), tuple(unmet)


class _ProductionHold(ValueError):
    """A machine-readable reason, retained on the unavailable N predicate."""


def _production_require(condition, reason):
    if not condition:
        raise _ProductionHold(reason)


def _production_collection(identity, contract, target, limit, deadline):
    """Use only the public store, including its captured-head continuation."""
    if time.monotonic() >= deadline:
        raise _ProductionHold("deadline")
    admission = production.get_lane_production_admission(task_ref=identity.task_ref, lane_id=identity.lane_id)
    _production_require(admission["ok"] is True, "admission_unavailable")
    admission = admission["data"]
    point = {
        "identity": lc._canonical(identity),
        "contract_revision": contract.revision,
        "contract_hash": contract.contract_hash,
        "target": admission["binding"]["target"],
    }
    _production_require(admission["binding"] == point, "binding_mismatch")
    pages, cursor, rows = [], None, []
    for _ in range(production.MAX_PAGES):
        if time.monotonic() >= deadline:
            raise _ProductionHold("deadline")
        page = production.get_lane_production_history(
            point,
            admission["admission_id"],
            limit=limit,
            read_limit=limit if limit < production.MAX_PAGE_ROWS else production.MAX_PAGE_ROWS * production.MAX_PAGES,
            cursor=cursor,
        )
        _production_require(page["ok"] is True, "history_unavailable")
        data = page["data"]
        _production_require(
            isinstance(data["history"], list)
            and data["returned"] == len(data["history"])
            and data["task_ref"] == identity.task_ref,
            "invalid_history",
        )
        if pages:
            _production_require(
                all(data[k] == pages[0][k] for k in ("head", "total_matching", "open_operation_count")),
                "history_changed",
            )
        pages.append(data)
        rows.extend(data["history"])
        cursor = data["next_cursor"]
        if cursor is None:
            break
        _production_require(
            data["has_more"] is True and data["truncation"]["reasons"] == ["page_limit"],
            "history_truncated",
        )
    last = pages[-1]
    complete = (
        last["has_more"] is False
        and last["truncated"] is False
        and last["truncation"] == {"truncated": False, "reasons": []}
        and cursor is None
        and len(rows) == last["total_matching"] == last["head"]["sequence"]
    )
    # Rebuild the single complete envelope consumed by the coverage validator;
    # retain the actual rows and metadata, never replace a missing suffix by [].
    data = {**last, "history": rows, "returned": len(rows)}
    _production_require(len(json.dumps(data).encode()) <= production.MAX_DECODED_BYTES, "history_truncated")
    previous = None
    for sequence, row in enumerate(rows, 1):
        payload = row["payload"]
        _production_require(
            row["sequence"] == payload["sequence"] == sequence
            and row["previous_evidence_id"] == payload["previous_evidence_id"] == previous
            and row["evidence_id"] == _digest(payload)
            and row["evidence_kind"] == payload["evidence_kind"]
            and payload["admission_id"] == admission["admission_id"],
            "invalid_history",
        )
        previous = row["evidence_id"]
    if complete:
        _production_require(previous == data["head"]["evidence_id"], "invalid_history")
    return admission, {"ok": True, "data": data}, complete


def _historical_product(root, rows, deadline):
    """Recognize the landed offload producer's retained commits and DB output.

    A prepare alone is pending. Unknown classifications never certify zero.
    A reset cannot subtract a successfully verified historical product.
    """
    refs = []
    for row in rows:
        payload, facts = row["payload"], row["payload"]["facts"]
        if (
            payload["evidence_kind"] != "production_settle"
            or payload["producer_kind"] != "offload_pass"
            or payload["producer_version"] != "1"
            or facts.get("production") != "positive"
        ):
            continue
        # The landed host collector also classifies persisted review output as
        # positive. Its retained finding IDs survive later hot-row archival;
        # requiring a current Git diff would erase this non-Git production.
        harvest = facts.get("findings_harvest")
        recorded = harvest.get("recorded_ids") if isinstance(harvest, dict) else None
        if (
            isinstance(recorded, list)
            and recorded
            and all(isinstance(item, str) and item.strip() for item in recorded)
            and facts.get("pass_id") == payload["operation_id"]
        ):
            refs.append(row["evidence_id"])
        commits = list(facts.get("checkpoint_commits", [])) + list(facts.get("native_commits", []))
        if facts.get("kind") == "checkpoint":
            oid = _resolve(root, facts["oid"], deadline)
            parent, _ = _git(root, "show", "-s", "--format=%P", oid, deadline=deadline)
            tree, _ = _git(root, "rev-parse", f"{oid}^{{tree}}", deadline=deadline)
            _production_require(
                parent.decode().strip() == facts["parent"] and tree.decode().strip() == facts["tree"],
                "product_unverifiable",
            )
            commits.append(oid)
        for oid in commits:
            _resolve(root, oid, deadline)
            diff, _ = _git(root, "diff-tree", "--root", "--no-commit-id", "--raw", "-r", oid, deadline=deadline)
            if diff:
                refs.append(row["evidence_id"])
    return tuple(sorted(set(refs)))


def _zero_operations(root, data, admission, deadline):
    """Only known pre-access operations can be zero without a source recorder.

    This is a closed producer/version allowlist, not an empty-result heuristic.
    Executed routes must pass the native coverage validator. No such worker
    route is N-capable in this tree; a seal cannot upgrade its capability.
    """
    rows = data["history"]
    _production_require(rows and rows[0]["evidence_kind"] == "production_admission", "admission_unavailable")
    initial = rows[0]["payload"]
    facts = initial["facts"]
    point = admission["binding"]
    base = point["identity"]["admission_base"]
    tree, _ = _git(root, "rev-parse", f"{base}^{{tree}}", deadline=deadline)
    _production_require(
        initial["producer_kind"] == "lane_materialize"
        and initial["producer_version"] == "1"
        and facts.get("phase") == "intent"
        and facts.get("branch_preexisting") is False
        and "predecessor" not in facts
        and facts.get("base_oid") == facts.get("initial_tip_oid") == base
        and facts.get("base_tree_oid") == facts.get("initial_tip_tree_oid") == tree.decode().strip(),
        "admission_unverifiable",
    )
    pending = {}
    admitted = False
    seen_seals = set()
    for row in rows:
        payload = row["payload"]
        kind, operation, facts = payload["evidence_kind"], payload["operation_id"], payload["facts"]
        if kind == "production_seal":
            seen_seals.add(row["evidence_id"])
            continue
        if kind == "production_gap":
            # The store invalidates a previous seal atomically with prepare.
            # This closes that seal; it is not missing source history. A new
            # terminal seal may cover the settled, still-zero operation.
            _production_require(
                payload["binding"] == point
                and payload["operation_phase"] == "seal_invalidation"
                and operation in seen_seals
                and facts == {"invalidated_evidence_id": operation},
                "history_gap",
            )
            continue
        _production_require(payload["binding"] == point, "binding_mismatch")
        if kind == "production_admission":
            continue
        _production_require(payload["producer_version"] == "1", "unsupported_producer")
        if kind == "production_prepare":
            _production_require(operation not in pending, "invalid_history")
            pending[operation] = payload
            continue
        _production_require(kind == "production_settle" and operation in pending, "unsupported_history")
        prepared = pending.pop(operation)
        _production_require(prepared["producer_kind"] == payload["producer_kind"], "invalid_history")
        if payload["producer_kind"] == "lane_materialize":
            _production_require(
                operation == initial["operation_id"]
                and not admitted
                and facts == {**initial["facts"], "phase": "settled", "empty_branch_created": True}
                and prepared["facts"] == initial["facts"],
                "admission_unverifiable",
            )
            admitted = True
        elif payload["producer_kind"] == "offload_pass":
            before = prepared["facts"]
            _production_require(
                before.get("pass_id") == facts.get("pass_id") == operation
                and before.get("expected_old_oid") == base
                and before.get("expected_old_tree") == tree.decode().strip()
                and before.get("worker_source_access_started") is False,
                "operation_unverifiable",
            )
            if facts.get("worker_source_access_started") is not False:
                policy = coverage.CoveragePolicy(
                    binding=point,
                    admission_id=admission["admission_id"],
                    routes=tuple(initial["facts"].get("routes", ())),
                    registry=coverage.ROUTE_REGISTRY,
                    assessment_cursors=data["source_cursors"],
                    authenticate=lambda _: False,
                )
                result = coverage.validate_coverage({"ok": True, "data": data}, policy)
                _production_require(result.status == "covered", "coverage_unavailable")
            _production_require(
                facts.get("production") == "verified_zero"
                and facts.get("source_complete") is True
                and facts.get("worker_source_access_started") is False
                and facts.get("checkpoint_commits") == []
                and facts.get("native_commits") == []
                and facts.get("native_artifacts") == {}
                and facts.get("findings_harvest") == {},
                "operation_unverifiable",
            )
        else:
            raise _ProductionHold("unsupported_producer")
    _production_require(not pending and data["open_operation_count"] == 0, "open_operation")
    _production_require(admitted, "admission_unverifiable")


def _production_evidence(root, identity, contract, target, limit, deadline):
    """Collect twice and derive mutually exclusive N/R, without recording rows."""
    binding = lc.EvidenceBinding(identity, contract.revision, contract.contract_hash, target)
    refs, positive = (), ()
    try:
        first = _production_collection(identity, contract, target, limit, deadline)
        second = _production_collection(identity, contract, target, limit, deadline)
        _production_require(first == second, "history_changed")
        admission, history, complete = first
        # Recheck current DB identity/contract even if the history head is stable.
        current = production.get_lane_production_admission(task_ref=identity.task_ref, lane_id=identity.lane_id)
        _production_require(current["ok"] is True and current["data"] == admission, "binding_changed")
        data = history["data"]
        refs = tuple(row["evidence_id"] for row in data["history"])
        positive = _historical_product(root, data["history"], deadline)
        _production_require(complete, "history_truncated")
        _production_require(data["open_operation_count"] == 0, "open_operation")
        seals = [row for row in data["history"] if row["evidence_kind"] == "production_seal"]
        if positive:
            _production_require(
                not seals
                or seals[-1] != data["history"][-1]
                or seals[-1]["payload"]["facts"].get("production") != "verified_zero",
                "contradictory_evidence",
            )
            return (
                lc.NeverProducedEvidence(state=_ABSENT, binding=binding, evidence_ids=refs),
                lc.SomeProductionEvidence(state=_PRESENT, binding=binding, evidence_ids=positive),
            )
        _zero_operations(root, data, admission, deadline)
        _production_require(bool(seals) and seals[-1] == data["history"][-1], "seal_unavailable")
        seal = seals[-1]
        payload = seal["payload"]
        _production_require(
            payload["binding"]
            == {
                **admission["binding"],
                "target": lc._canonical(target),
            },
            "binding_mismatch",
        )
        _production_require(
            payload["producer_kind"] == "completion_collector"
            and payload["producer_version"] == "1"
            and payload["facts"].get("coverage_policy_digest") == "pre-access-v1"
            and payload["facts"].get("complete_through")
            == {
                "sequence": seal["sequence"] - 1,
                "evidence_id": seal["previous_evidence_id"],
            }
            and payload["facts"].get("production") == "verified_zero",
            "seal_unverifiable",
        )
        _production_require(identity.admission_base == identity.product_tip, "unclassified_commit")
        return (
            lc.NeverProducedEvidence(state=_PRESENT, binding=binding, evidence_ids=refs),
            lc.SomeProductionEvidence(state=_ABSENT, binding=binding, evidence_ids=refs),
        )
    except production.ProductionRefusal as exc:
        reason = exc.code.value
    except _ProductionHold as exc:
        reason = str(exc)
    except Exception:  # A failed public reader is unavailable, never zero.
        reason = "history_unavailable"
    # Contradictions deliberately preserve both predicates for the evaluator.
    n_state = _PRESENT if reason == "contradictory_evidence" else _ABSENT if positive else _UNKNOWN
    return (
        lc.NeverProducedEvidence(state=n_state, binding=binding, evidence_ids=(*refs, f"production:{reason}")),
        lc.SomeProductionEvidence(state=_PRESENT if positive else _UNKNOWN, binding=binding, evidence_ids=positive),
    )


def build_snapshot(
    repo_root,
    *,
    contract: lc.CompletionContract,
    identity: lc.LaneIdentity,
    target: lc.CompletionTarget,
    phase: lc.CompletionPhase,
    handoff_reader,
    read_limit: int = 200,
    source_holds: tuple[str, ...] = (),
    custody_holds: tuple[str, ...] = (),
) -> lc.CompletionSnapshot:
    """Build slice-1 types, holding on incomplete, stale or unsupported evidence.

    Identity/contract are caller-supplied canonical values, never inferred from
    prose or stored here. The public read boundary is limited to two pages per
    collection, repeated once for coherence; no pagination/retry loop. Git has
    a shared 20s deadline, 5s per process, 512KiB output and 200 changed paths.
    Production history has its own bounded two-page collection, repeated once.
    N supports sealed pre-access history only; transfers and carriers stay unavailable.
    """
    root = Path(repo_root).resolve()
    scope = lc.IdentityScopeEvidence(identity, contract, _UNKNOWN, ("snapshot:unavailable",))
    lane = lc.LaneCompletionEvidence(
        scope, target, never_produced=lc.NeverProducedEvidence(evidence_ids=("production:snapshot_unavailable",))
    )
    binding = lc.EvidenceBinding(identity, contract.revision, contract.contract_hash, target)

    def snapshot():
        observed = lane
        if lane.never_produced.state == _UNKNOWN and not lane.never_produced.evidence_ids:
            observed = replace(
                lane, never_produced=lc.NeverProducedEvidence(evidence_ids=("production:snapshot_unavailable",))
            )
        return lc.CompletionSnapshot(observed, source_holds=source_holds, custody_holds=custody_holds)

    if (
        phase != target.phase
        or not isinstance(phase, lc.CompletionPhase)
        or contract.identity != identity
        or target.candidate_sha != identity.product_tip
        or type(read_limit) is not int
        or not 1 <= read_limit <= 200
        or not lc._ids_valid(contract.finding_ids)
        or not lc._ids_valid(contract.required_commands)
    ):
        return snapshot()
    deadline = time.monotonic() + 20
    try:
        if (
            Path(identity.repository).resolve() != root
            or _resolve(root, identity.branch, deadline) != target.candidate_sha
            or _resolve(root, target.integration_ref, deadline) != target.integration_sha
        ):
            return snapshot()
        _resolve(root, target.candidate_sha, deadline)
        if target.landing_sha and not _ancestor(root, target.landing_sha, target.integration_sha, deadline):
            return snapshot()
        product_state, product_refs, produced = _product(root, identity, target, deadline)
        initial = _read(handoff_reader, identity, read_limit)
        tests = _latest(initial[0], identity, "command", "verified_at", lane_scoped=True)
        findings = _latest(initial[1], identity, "finding_id", "updated_at")
        f_state, f_refs, f_unmet = _findings(root, findings, contract, target, deadline)
        assessed_sha = target.candidate_sha if phase == lc.CompletionPhase.CANDIDATE else target.integration_sha
        t_state, t_refs, t_unmet = _tests(tests, contract, assessed_sha)
        a_state, a_refs, a_unmet = _acceptance(
            tests,
            contract,
            assessed_sha,
            merge_tree_oid=target.merge_tree_oid if phase == lc.CompletionPhase.CANDIDATE else None,
        )
        if (
            _resolve(root, identity.branch, deadline) != target.candidate_sha
            or _resolve(root, target.integration_ref, deadline) != target.integration_sha
        ):
            return snapshot()
        lane = replace(
            lane,
            identity_scope=replace(
                scope, state=_PRESENT, evidence_ids=(f"identity:{_digest(lc._canonical(identity))}",)
            ),
            product=lc.ProductOnTargetEvidence(
                state=product_state,
                binding=binding,
                evidence_ids=product_refs,
                proof_kind=lc.ProductProofKind.CANDIDATE_CONTENT
                if phase == lc.CompletionPhase.CANDIDATE
                else lc.ProductProofKind.INTEGRATION_CONTENT,
            ),
            findings=lc.FindingsAddressedEvidence(
                state=f_state, binding=binding, evidence_ids=f_refs, finding_ids=contract.finding_ids
            ),
            tests=lc.TestFreshnessEvidence(
                state=t_state, binding=binding, evidence_ids=t_refs, commands=contract.required_commands
            ),
            acceptance=lc.AcceptanceEvidence(
                state=a_state, binding=binding, evidence_ids=a_refs, acceptance_ids=contract.acceptance_ids
            ),
        )
        unmet = tuple(sorted(set((*f_unmet, *t_unmet, *a_unmet)), key=lambda item: (item.kind.value, item.id)))
        if unmet:
            lane = replace(
                lane,
                unmet=lc.KnownUnmetEvidence(
                    state=_PRESENT,
                    binding=binding,
                    evidence_ids=tuple(sorted(set((*f_refs, *t_refs, *a_refs)))),
                    obligations=unmet,
                ),
            )
        never, some = _production_evidence(root, identity, contract, target, read_limit, deadline)
        lane = replace(lane, never_produced=never, some_production=some)
        if (
            initial != _read(handoff_reader, identity, read_limit)
            or _resolve(root, identity.branch, deadline) != target.candidate_sha
            or _resolve(root, target.integration_ref, deadline) != target.integration_sha
        ):
            lane = lc.LaneCompletionEvidence(scope, target)
            return snapshot()
        if produced:
            lane = replace(
                lane,
                some_production=lc.SomeProductionEvidence(state=_PRESENT, binding=binding, evidence_ids=product_refs),
            )
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError, TimeoutError):
        # Probe failure cannot manufacture absent evidence or permission.
        lane = lc.LaneCompletionEvidence(scope, target)
    return snapshot()
