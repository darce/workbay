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

from . import lane_completion as lc

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
    N, transfers and carrier history remain unavailable in this slice.
    """
    root = Path(repo_root).resolve()
    scope = lc.IdentityScopeEvidence(identity, contract, _UNKNOWN, ("snapshot:unavailable",))
    lane = lc.LaneCompletionEvidence(scope, target)
    binding = lc.EvidenceBinding(identity, contract.revision, contract.contract_hash, target)

    def snapshot():
        return lc.CompletionSnapshot(lane, source_holds=source_holds, custody_holds=custody_holds)

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
            initial != _read(handoff_reader, identity, read_limit)
            or _resolve(root, identity.branch, deadline) != target.candidate_sha
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
        if produced:
            lane = replace(
                lane,
                some_production=lc.SomeProductionEvidence(state=_PRESENT, binding=binding, evidence_ids=product_refs),
            )
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError, TimeoutError):
        # Probe failure cannot manufacture absent evidence or permission.
        lane = lc.LaneCompletionEvidence(scope, target)
    return snapshot()
