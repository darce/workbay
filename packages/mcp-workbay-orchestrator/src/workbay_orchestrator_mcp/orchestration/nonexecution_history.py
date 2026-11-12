"""Host-owned nonexecution receipts and bounded historical recovery.

No key is created by a reader. Historical labels never authorize exclusion.
The operator attestation uses the existing Git-common-dir/OS-owner/TTY trust
boundary of review_cycle_approval, and is deliberately absent from the SDK's
write operations. Original pass bytes remain in place and in append-only custody.

Canon v0.26.2: ARCH-13 Architecture hoisting (Just Enough Software Architecture,
ch. 2); DATA-14 No dual writes (Designing Data-Intensive Applications, ch. 11);
CON-11 Atomicity violation (Operating Systems: Three Easy Pieces, ch. 32).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import pwd
import re
import subprocess
import sys
import tempfile
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path

from .invocation_disposition import _json, _pass_path, ensure_state_directory, regular_bytes
from .review_cycle_dispatch import authority, registered_lane


class NonexecutionHold(ValueError):
    """Typed uncertainty; never interpreted as permission to omit history."""


def _require(value, reason):
    if not value:
        raise NonexecutionHold(reason)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True, timeout=10
    ).stdout.strip()


def _root(repo):
    return authority(repo) / "offload-nonexecution"


def _private_bytes(path):
    # Same owner-only canonical host custody as approval/dispatch authority.
    raw = regular_bytes(path)
    for p in (path, path.parent):
        info = p.stat()
        _require(info.st_uid == os.getuid() and not info.st_mode & 0o077, "nonexecution_custody_invalid")
    return raw


def _append(path, raw):
    """Publish complete bytes without replacing an existing identity."""
    ensure_state_directory(path.parent)
    if os.path.lexists(path):
        _require(_private_bytes(path) == raw, "nonexecution_identity_conflict")
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".receipt-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                _require(_private_bytes(path) == raw, "nonexecution_identity_conflict")
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)


def _binding(repo, task_ref, lane_id, branch, worktree, *, incarnation=False):
    registered = registered_lane(repo, branch)
    _require(registered is not None, "nonexecution_lane_unavailable")
    config, lane = registered
    _require(
        (lane["task_ref"], lane["lane_id"], lane["branch"]) == (task_ref, lane_id, branch)
        and Path(lane["worktree_path"]).resolve() == Path(worktree).resolve(),
        "nonexecution_subject_mismatch",
    )
    binding = {k: lane.get(k) for k in ("id", "task_ref", "lane_id", "branch", "worktree_path", "cycle_epoch")}
    if incarnation:
        from .lane_postmerge import _try_authoritative_lane_base_sha

        # Storage owns this cursor: inserts and path/branch changes advance it;
        # status changes only advance cycle_epoch. Never infer an old cursor
        # from today's row ID, epoch, timestamp or global counter arithmetic.
        binding["identity_seq"] = lane.get("identity_seq")
        base, detail = _try_authoritative_lane_base_sha(
            row=lane,
            task_ref=task_ref,
            lane_id=lane_id,
            repo=config.workspace_root,
        )
        _require(detail is None, "nonexecution_constructor_source_unavailable")
        binding["source_base_sha"] = base
    return config, binding


def preview_pass(*, orchestrator_root, task_ref, lane_id, worktree_path, pass_id, request, state_dir):
    """A preview cannot call setup, reserve, settle, sweep, or publish a pass."""
    from .codex_lane_config import CODEX_MODEL_TIERS
    from .lane_routing import normalize_speed, resolve_routing_quad
    from .worker_daemon_ctl import (
        WorktreeCapabilityReceiptRefusal,
        is_remote_worker_backend,
        mint_worktree_capability_receipt,
    )

    repo = Path(worktree_path).resolve()
    _pass_path(Path(state_dir), pass_id)  # validate identity, without creating state
    branch = _git(repo, "symbolic-ref", "--short", "HEAD")
    _, binding = _binding(repo, task_ref, lane_id, branch, repo)
    head = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    quad = resolve_routing_quad(
        caller={k: request.get(k) for k in ("backend", "model", "speed", "tier")}
        | {"effort": request.get("reasoning_effort")},
        row=registered_lane(repo, branch)[1],
        tier_table=CODEX_MODEL_TIERS,
        allow_partial_caller=True,
    )
    backend = quad.backend or request["backend"]
    speed = normalize_speed(backend, quad.speed)
    proposal = request.get("canon_proposal_path")
    proposal_sha = _sha(regular_bytes(Path(proposal).resolve())) if proposal else None
    identity = dict(
        mode="preview",
        pass_id=pass_id,
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        worktree_path=str(repo),
        source_head=head,
        source_tree=tree,
        binding=binding,
        request={**request, "canon_proposal_sha256": proposal_sha},
    )
    path = _root(repo) / "previews" / (_sha(pass_id.encode()) + ".json")

    def replay():
        saved = _json(_private_bytes(path))
        _require(
            saved.get("schema") == "workbay/offload-nonexecution@1"
            and saved.get("producer") == "public_preview"
            and saved.get("identity") == identity,
            "nonexecution_identity_conflict",
        )
        result = saved.get("result")
        _require(
            isinstance(result, dict)
            and result.get("mode") == "preview"
            and result.get("model_dispatched") is False
            and result.get("execution_admitted") is False,
            "nonexecution_preview_receipt_invalid",
        )
        return {**result, "nonexecution_receipt": str(path), "request_sha256": _sha(_encoded(identity))}

    if os.path.lexists(path):
        return replay()
    # No creation of an execution-state constructor, even for an existing pass
    # ID. Distinct namespace; immutable full identity handles same-ID retries.
    result = dict(
        ok=True,
        terminal=True,
        outcome="preview_complete",
        mode="preview",
        pass_id=pass_id,
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        model_dispatched=False,
        execution_admitted=False,
        commit_landed=False,
        summary="Offload preview; execution was not admitted.",
    )
    if is_remote_worker_backend(backend):
        receipt = mint_worktree_capability_receipt(
            orchestrator_root=Path(orchestrator_root),
            backend=backend,
            model=quad.model,
            speed=speed,
            reasoning_effort=quad.effort or "inherit",
            worktree_path=repo,
            grok_max_turns=request.get("grok_max_turns"),
            grok_timeout=None,
            adapter_timeout=request.get("turn_timeout_seconds") or request["timeout_seconds"],
            token_budget=request["token_budget"],
            lane_id=lane_id,
            task_ref=task_ref,
            pass_id=pass_id,
            dispatch_id=f"offload-preview:{task_ref}:{lane_id}:{pass_id}",
            persist=False,
        )
        if isinstance(receipt, WorktreeCapabilityReceiptRefusal):
            result.update(
                ok=False, outcome="preview_refused", reason=receipt.reason, error=receipt.error, summary=receipt.error
            )
    record = dict(schema="workbay/offload-nonexecution@1", producer="public_preview", identity=identity, result=result)
    try:
        _append(path, _encoded(record))
    except NonexecutionHold:
        # A concurrent, identical request may have observed a different remote
        # readiness result. Its first complete receipt owns the retry outcome.
        if not os.path.lexists(path):
            raise
    return replay()


def constructor_result(repo, *, pass_id, task_ref, lane_id, request, result, invocation_token):
    """Called only on the engine's positive pre-exposure control-flow arm."""
    branch = _git(repo, "symbolic-ref", "--short", "HEAD")
    config, binding = _binding(repo, task_ref, lane_id, branch, repo, incarnation=True)
    retained = _json(regular_bytes(_pass_path(config.state_dir, pass_id)))
    frozen = retained.get("lane_cycle_binding")
    if isinstance(frozen, dict):
        _require(
            frozen.get("source_base_sha") == binding["source_base_sha"],
            "nonexecution_constructor_source_mismatch",
        )
    _require(
        retained.get("invocation_token") == invocation_token
        and (retained.get("task_ref"), retained.get("lane_id")) == (task_ref, lane_id),
        "nonexecution_constructor_identity_mismatch",
    )
    identity = dict(
        mode="admission_refusal",
        pass_id=pass_id,
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        source_head=_git(repo, "rev-parse", "HEAD"),
        source_tree=_git(repo, "rev-parse", "HEAD^{tree}"),
        binding=binding,
        request=request,
        invocation_token=invocation_token,
        pass_state={k: v for k, v in retained.items() if k not in ("status", "result")},
    )
    record = dict(
        schema="workbay/offload-nonexecution@2", producer="engine_before_admission", identity=identity, result=result
    )
    # One immutable receipt per exact attempt/result; never rewrites execution.
    path = _root(repo) / "refusals" / (_sha(_encoded(record)) + ".json")
    _append(path, _encoded(record))
    return {
        **result,
        "mode": "admission_refusal",
        "nonexecution_receipt": str(path),
        "model_dispatched": False,
        "execution_admitted": False,
    }


def _require_no_execution(state, result, completion):
    # These are disconfirmers, NOT affirmative nonexecution proof. Only the
    # protected producer or trusted operator attestation supplies the fact.
    for obj in (state, result, completion):
        _require(
            not any(
                obj.get(k)
                for k in (
                    "job_id",
                    "attempt",
                    "agent_stream_path",
                    "review_cycle_dispatch_id",
                    "review_cycle",
                    "checkpoint_commits",
                    "commit_landed",
                    "source_provenance",
                    "consumed_source",
                    "changed_files",
                    "findings",
                    "model_process_started",
                    "model_started",
                    "model_call_in_flight",
                )
            ),
            "nonexecution_execution_or_unknown_evidence",
        )
        _require(
            all(key not in obj or obj[key] is False for key in ("model_started", "model_process_started")),
            "nonexecution_execution_or_unknown_evidence",
        )
        receipt = obj.get("dispatch_receipt") or {}
        _require(isinstance(receipt, dict), "nonexecution_execution_or_unknown_evidence")
        _require(
            not receipt.get("process_ran")
            and not receipt.get("routing_effective")
            and not any(receipt.get(k) for k in ("turns", "rounds", "observed_tokens", "routing_history")),
            "nonexecution_execution_or_unknown_evidence",
        )
        _require(
            ("process_ran" not in receipt or receipt["process_ran"] is False)
            and receipt.get("tokens_complete", True) is True
            and not any(receipt.get(k) for k in ("served_model", "observed_backend", "tokens_incomplete_reason")),
            "nonexecution_execution_or_unknown_evidence",
        )
        tokens = obj.get("tokens") or {}
        _require(
            isinstance(tokens, dict)
            and not any(
                tokens.get(k) for k in ("cumulative_total", "observed_tokens_lower_bound", "tokens_incomplete_reason")
            ),
            "nonexecution_execution_or_unknown_evidence",
        )
    progress = state.get("progress") or {}
    _require(
        isinstance(progress, dict)
        and progress.get("model_call_in_flight") is None
        and not progress.get("observed_tokens"),
        "nonexecution_execution_or_unknown_evidence",
    )


def _historical(repo, state_dir, pass_id):
    path = _pass_path(state_dir, pass_id)
    raw = regular_bytes(path)
    state = _json(raw)
    completion = state.get("completion_config") or {}
    result = state.get("result") or {}
    binding = state.get("lane_cycle_binding") or {}
    _require(
        all(isinstance(value, dict) for value in (completion, result, binding)),
        "nonexecution_historical_shape_unproven",
    )
    _require(
        state.get("status") == "done" and completion.get("pass_id") == pass_id, "nonexecution_not_terminal_constructor"
    )
    refusal_shape = (
        completion.get("dry_run") is False
        and result.get("outcome") == "dispatch_refused"
        and result.get("reason") == "ledger_hold"
        and str(result.get("error", "")).startswith("review_ratchet_legacy_source_")
    )
    # Affected versions refused before assigning the outer branch. Retain that
    # absence in the original bytes; the registered binding is only a candidate
    # identity for the operator's exact-record attestation, never proof by itself.
    branch = state.get("branch")
    if branch is None and refusal_shape:
        branch = binding.get("branch")
    config, live = _binding(repo, state.get("task_ref"), state.get("lane_id"), branch, state.get("worktree_path"))
    _require(config.state_dir.resolve() == state_dir.resolve(), "nonexecution_state_directory_mismatch")
    for k in ("task_ref", "lane_id", "branch", "worktree_path"):
        _require(binding.get(k) == (branch if k == "branch" else state.get(k)), "nonexecution_binding_mismatch")
    for k in (
        "task_ref",
        "lane_id",
        "branch",
        "worktree_path",
        "backend",
        "model",
        "reasoning_effort",
        "completion_config",
        "lane_cycle_binding",
    ):
        _require(k not in result or result[k] == state.get(k), "nonexecution_result_contradiction")
    review_path = state_dir / f"review-receipt-{pass_id}.json"
    _require(not os.path.lexists(review_path), "nonexecution_review_evidence_present")
    for key, current in (("row_id", "id"), ("cycle_epoch", "cycle_epoch")):
        _require(binding.get(key) is None or binding[key] == live[current], "nonexecution_binding_mismatch")
    head = completion.get("start_head_ref")
    _require(isinstance(head, str) and re.fullmatch("[0-9a-f]{40}", head), "nonexecution_source_unavailable")
    _require(_git(repo, "rev-parse", head + "^{commit}") == head, "nonexecution_source_mismatch")
    _git(repo, "merge-base", "--is-ancestor", head, "refs/heads/" + branch)
    base = binding.get("source_base_sha")
    if base is not None:
        _require(isinstance(base, str) and re.fullmatch("[0-9a-f]{40}", base), "nonexecution_source_unavailable")
        _git(repo, "merge-base", "--is-ancestor", base, head)
    _require_no_execution(state, result, completion)
    _require(state.get("tip_sha") in (None, head), "nonexecution_source_changed")
    _require(not completion.get("dispatch_id"), "nonexecution_execution_identity_present")
    if completion.get("dry_run") is True and result.get("outcome") == "needs_guidance":
        classification = "preview"
    elif (
        completion.get("dry_run") is False
        and result.get("outcome") == "dispatch_refused"
        and result.get("reason") == "ledger_hold"
        and str(result.get("error", "")).startswith("review_ratchet_legacy_source_")
    ):
        classification = "pre_model_refusal"
    else:
        raise NonexecutionHold("nonexecution_historical_shape_unproven")
    identity = dict(
        pass_id=pass_id,
        task_ref=state["task_ref"],
        lane_id=state["lane_id"],
        branch=branch,
        worktree_path=state["worktree_path"],
        source_head=head,
        source_tree=_git(repo, "rev-parse", head + "^{tree}"),
        binding=binding,
        completion_config=completion,
        started_at=state.get("started_at"),
    )
    return raw, identity, classification


def _paths(repo, pass_id):
    key = _sha(pass_id.encode())
    return (_root(repo) / "attestations" / (key + ".json"), _root(repo) / "recoveries" / (key + ".json"))


def _validate_attestation(raw, identity, record_raw, classification):
    value = _json(raw)
    _require(
        value.get("schema") == "workbay/offload-nonexecution-attestation@1"
        and value.get("identity") == identity
        and value.get("classification") == classification
        and value.get("record_sha256") == _sha(record_raw)
        and value.get("request_sha256") == _sha(_encoded(identity))
        and value.get("operator_uid") == os.getuid()
        and value.get("approved_by") == pwd.getpwuid(os.getuid()).pw_name
        and isinstance(value.get("reason"), str)
        and value["reason"].strip()
        and isinstance(value.get("created_at"), str),
        "nonexecution_attestation_mismatch",
    )
    evidence = base64.b64decode(value["evidence_base64"], validate=True)
    _require(
        bool(evidence) and value["evidence_sha256"] == _sha(evidence), "nonexecution_attestation_evidence_mismatch"
    )
    return value


def authenticated_nonexecution(repo, state_dir, pass_id, raw):
    """Read-only, exact bytes + current subject + retained authority. No labels."""
    attestation_path, custody_path = _paths(repo, pass_id)
    if not os.path.lexists(custody_path):
        return _authenticated_constructor(repo, state_dir, pass_id, raw)
    record_raw, identity, classification = _historical(repo, state_dir, pass_id)
    _require(record_raw == raw, "nonexecution_record_changed")
    attestation = _private_bytes(attestation_path)
    _validate_attestation(attestation, identity, raw, classification)
    custody = _json(_private_bytes(custody_path))
    _require(
        custody
        == dict(
            schema="workbay/offload-nonexecution-recovery@1",
            identity=identity,
            classification=classification,
            record_sha256=_sha(raw),
            original_base64=base64.b64encode(raw).decode(),
            attestation_sha256=_sha(attestation),
        ),
        "nonexecution_custody_mismatch",
    )
    return True


def authenticated_current_invocation(repo, state_dir, pass_id, raw):
    """Authenticate only the current constructor, never superseded execution.

    A true result is not permission to omit the pass from charged history.
    Whole-pass consumers must use ``authenticated_nonexecution`` instead.
    """
    return _authenticated_constructor(repo, state_dir, pass_id, raw, current_invocation=True)


def _constructor_source(repo, state_dir, worktree):
    """Prove a retained lane is a live worktree of this host authority.

    A receipt's source path is not itself authority. Same objects in a clone,
    a Git-directory redirect or a retired path do not prove this relationship.
    No authenticated source archive contract is available here.
    """
    from workbay_handoff_mcp import RuntimeConfig

    reason = "nonexecution_constructor_source_mismatch"
    _require(isinstance(worktree, str) and Path(worktree).is_absolute(), reason)
    source = Path(worktree)
    try:
        _require(source == source.resolve(strict=True) and source.is_dir(), reason)
        marker = source / ".git"
        _require(not marker.is_symlink(), reason)
        marker_raw = regular_bytes(marker) if marker.is_file() else None
        common = authority(repo).parent.resolve(strict=True)
        _require(authority(source).parent.resolve(strict=True) == common, reason)
        _require(_git(source, "rev-parse", "--show-toplevel") == str(source), reason)
        git_dir = Path(_git(source, "rev-parse", "--absolute-git-dir"))
        _require(git_dir == git_dir.resolve(strict=True), reason)
        if git_dir == common:
            _require(marker_raw is None and marker.resolve(strict=True) == common, reason)
        else:
            _require(git_dir.parent == common / "worktrees" and marker_raw is not None, reason)
            # Check both ends of Git's linked-worktree relationship. Merely
            # pointing .git at another lane's Git directory is not membership.
            backlink = regular_bytes(git_dir / "gitdir")
            _require(backlink.decode().strip() == str(marker), reason)
            _require(regular_bytes(marker) == marker_raw, reason)
        _require(
            "worktree " + str(source) in _git(repo, "worktree", "list", "--porcelain", "-z").split("\0")
            and RuntimeConfig.for_repo(source).state_dir.resolve(strict=True) == state_dir.resolve(strict=True),
            reason,
        )
        if marker_raw is not None:
            _require(regular_bytes(marker) == marker_raw, reason)
            _require(regular_bytes(git_dir / "gitdir") == backlink, reason)
    except (OSError, subprocess.SubprocessError) as exc:
        raise NonexecutionHold("nonexecution_constructor_source_unavailable") from exc


def _authenticated_constructor(repo, state_dir, pass_id, raw, *, current_invocation=False):
    """Positive host producer receipt for one invocation before admission.

    This mode can never be acquired by a historical execution. The producer
    seals the original invocation token, request, source, binding and result;
    the pass writer fences that terminal identity against subsequent reuse.
    """
    state = _json(raw)
    result = state.get("result")
    if not isinstance(result, dict) or "nonexecution_receipt" not in result:
        return False
    path = Path(result["nonexecution_receipt"])
    root = _root(repo) / "refusals"
    _require(
        path.parent == root and re.fullmatch("[0-9a-f]{64}\\.json", path.name),
        "nonexecution_constructor_locator_invalid",
    )
    proof_raw = _private_bytes(path)
    _require(path.stem == _sha(proof_raw), "nonexecution_constructor_digest_mismatch")
    proof = _json(proof_raw)
    identity = proof["identity"]
    _require(
        proof["schema"] in {"workbay/offload-nonexecution@1", "workbay/offload-nonexecution@2"}
        and proof["producer"] == "engine_before_admission"
        and identity["mode"] == result.get("mode") == "admission_refusal"
        and (identity["pass_id"], identity["task_ref"], identity["lane_id"])
        == (pass_id, state.get("task_ref"), state.get("lane_id"))
        and identity["invocation_token"] == state.get("invocation_token")
        and result.get("model_dispatched") is False
        and result.get("execution_admitted") is False
        and state.get("status") == "done"
        and not set(state)
        - {"status", "task_ref", "lane_id", "result", "lane_cycle_binding", "invocation_token", "started_at",
           "superseded_invocations"},
        "nonexecution_constructor_identity_mismatch",
    )
    actual = {
        k: v
        for k, v in result.items()
        if k not in ("mode", "nonexecution_receipt", "model_dispatched", "execution_admitted")
    }
    _require(actual == proof["result"], "nonexecution_constructor_result_mismatch")
    # Authenticate the original invocation, not permission for a new one.
    # v2 additionally sealed the retained state and incarnation cursor. v1
    # sealed the token/request/result/source and original lane binding; never
    # manufacture a missing historical cursor from today's registry.
    v2 = proof["schema"] == "workbay/offload-nonexecution@2"
    superseded = state.get("superseded_invocations", [])
    _require(
        isinstance(superseded, list) and (v2 or "superseded_invocations" not in state),
        "nonexecution_constructor_history_unproven",
    )
    if v2:
        _require(
            {k: v for k, v in state.items() if k not in ("status", "result")} == identity["pass_state"],
            "nonexecution_constructor_record_mismatch",
        )
    tokens = {identity["invocation_token"]}
    for prior in superseded:
        _require(
            isinstance(prior, dict) and set(prior) == {"invocation_token", "completion_config"},
            "nonexecution_constructor_history_unproven",
        )
        token, request = prior["invocation_token"], prior["completion_config"]
        _require(
            isinstance(token, str) and re.fullmatch("[0-9a-f]{32}", token) and token not in tokens
            and isinstance(request, dict) and request.get("pass_id") == pass_id,
            "nonexecution_constructor_history_unproven",
        )
        tokens.add(token)
    _require(
        isinstance(identity["request"], dict)
        and isinstance(identity["invocation_token"], str)
        and re.fullmatch("[0-9a-f]{32}", identity["invocation_token"])
        and identity["request"] == actual.get("completion_config")
        and identity["request"].get("pass_id") == pass_id,
        "nonexecution_constructor_request_mismatch",
    )
    _require_no_execution(state, result, identity["request"])
    from workbay_handoff_mcp import RuntimeConfig

    config = RuntimeConfig.for_repo(repo)
    original = identity["binding"]
    frozen = state.get("lane_cycle_binding") or {}
    _require(isinstance(original, dict) and isinstance(frozen, dict), "nonexecution_constructor_source_mismatch")
    _require(
        config.state_dir.resolve() == state_dir.resolve()
        and all(original.get(k) == identity[k] for k in ("task_ref", "lane_id", "branch"))
        and all(type(original.get(k)) is int and original[k] > 0 for k in ("id", "cycle_epoch"))
        and (not v2 or type(original.get("identity_seq")) is int and original["identity_seq"] > 0)
        and all(
            frozen.get(k) == original.get(r)
            for k, r in (
                ("row_id", "id"),
                ("cycle_epoch", "cycle_epoch"),
                ("task_ref", "task_ref"),
                ("lane_id", "lane_id"),
                ("branch", "branch"),
                ("worktree_path", "worktree_path"),
            )
        )
        and (not v2 or frozen.get("source_base_sha") == original.get("source_base_sha"))
        and identity["source_head"] == identity["request"]["start_head_ref"],
        "nonexecution_constructor_source_mismatch",
    )
    _constructor_source(repo, state_dir, original["worktree_path"])
    base = original["source_base_sha"] if v2 else frozen.get("source_base_sha")
    _require(
        isinstance(base, str) and re.fullmatch("[0-9a-f]{40}", base), "nonexecution_constructor_source_unavailable"
    )
    # Validate immutable Git objects from the original invocation. Today's
    # branch, base, cadence and incarnation are mutable execution eligibility,
    # not disconfirmers of a protected past fact (DATA-14, DDIA ch. 11).
    # Fresh admission and the terminal pass publication fence still own their
    # checks at the effect boundary (CON-11, OSTEP ch. 32).
    try:
        _require(
            isinstance(identity["source_head"], str)
            and re.fullmatch("[0-9a-f]{40}", identity["source_head"])
            and _git(repo, "rev-parse", identity["source_head"] + "^{tree}") == identity["source_tree"],
            "nonexecution_constructor_source_mismatch",
        )
        _git(repo, "merge-base", "--is-ancestor", base, identity["source_head"])
    except subprocess.SubprocessError as exc:
        raise NonexecutionHold("nonexecution_constructor_source_mismatch") from exc
    _require(regular_bytes(_pass_path(state_dir, pass_id)) == raw, "nonexecution_record_changed")
    if superseded and not current_invocation:
        # Preserve the whole-pass publication fence. A current positive fact
        # cannot exclude earlier UNKNOWN execution (DATA-14, DDIA ch. 11).
        raise NonexecutionHold("nonexecution_superseded_execution_unknown")
    return True


def reconcile(
    repo, state_dir, pass_id, *, operation="inspect", expected_record_sha256=None, expected_attestation_sha256=None
):
    """SDK inspect/apply consumes authority; it cannot create an attestation."""
    result = dict(ok=False, status="hold", pass_id=pass_id, outcome="nonexecution_hold")
    try:
        _require(operation in ("inspect", "apply"), "nonexecution_invalid_operation")
        from .offload_pass import _pass_publication_lock

        # Same writer lock: no check/use gap between original observation and
        # append-only classification. The legacy reader rechecks exact bytes.
        lock = _pass_publication_lock(_pass_path(state_dir, pass_id)) if operation == "apply" else nullcontext()
        with lock:
            current = regular_bytes(_pass_path(state_dir, pass_id))
            if _authenticated_constructor(repo, state_dir, pass_id, current):
                return {
                    **result,
                    "ok": True,
                    "status": "nonexecution",
                    "outcome": "nonexecution_classified",
                    "classification": "admission_refusal",
                    "record_sha256": _sha(current),
                }
            raw, identity, classification = _historical(repo, state_dir, pass_id)
            result.update(
                record_sha256=_sha(raw), request_sha256=_sha(_encoded(identity)), classification=classification
            )
            attestation_path, custody_path = _paths(repo, pass_id)
            if authenticated_nonexecution(repo, state_dir, pass_id, raw):
                return {
                    **result,
                    "ok": True,
                    "status": "reconciled",
                    "outcome": "nonexecution_reconciled",
                    "custody_path": str(custody_path),
                }
            _require(os.path.lexists(attestation_path), "trusted_attestation_required")
            attestation = _private_bytes(attestation_path)
            _validate_attestation(attestation, identity, raw, classification)
            result["attestation_sha256"] = _sha(attestation)
            if operation == "inspect":
                return {**result, "ok": True, "status": "ready", "outcome": "nonexecution_recovery_ready"}
            _require(
                expected_record_sha256 == _sha(raw) and expected_attestation_sha256 == _sha(attestation),
                "nonexecution_expected_digest_mismatch",
            )
            custody = dict(
                schema="workbay/offload-nonexecution-recovery@1",
                identity=identity,
                classification=classification,
                record_sha256=_sha(raw),
                original_base64=base64.b64encode(raw).decode(),
                attestation_sha256=_sha(attestation),
            )
            _append(custody_path, _encoded(custody))
            return {
                **result,
                "ok": True,
                "status": "reconciled",
                "outcome": "nonexecution_reconciled",
                "custody_path": str(custody_path),
            }
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        return {**result, "reason": str(exc)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["attest"])
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--pass-id", required=True)
    parser.add_argument("--record-sha256", required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--classification", choices=["preview", "pre_model_refusal"], required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--evidence-sha256", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    # Same operator authority as exact-round approvals. No SDK, stdin JSON or
    # worker-selected file can grant this authority. No new secret generation.
    if not sys.stdin.isatty():
        parser.error("operator recovery attestation requires a TTY on stdin")
    try:
        from workbay_handoff_mcp import RuntimeConfig

        config = RuntimeConfig.for_repo(args.repo)
        from .offload_pass import _pass_publication_lock

        with _pass_publication_lock(_pass_path(config.state_dir, args.pass_id)):
            raw, identity, classification = _historical(args.repo, config.state_dir, args.pass_id)
            _require(
                (args.record_sha256, args.request_sha256, args.classification)
                == (_sha(raw), _sha(_encoded(identity)), classification),
                "nonexecution_expected_digest_mismatch",
            )
            evidence_path = args.evidence.absolute()
            _require(
                evidence_path.is_relative_to(authority(args.repo) / "nonexecution-evidence"),
                "nonexecution_evidence_not_in_operator_custody",
            )
            evidence = _private_bytes(evidence_path)
            _require(
                bool(evidence) and _sha(evidence) == args.evidence_sha256 and args.reason.strip(),
                "nonexecution_evidence_digest_mismatch",
            )
            print("Attest no source/model access for this exact record; enter record SHA-256:", file=sys.stderr)
            _require(sys.stdin.readline().strip() == args.record_sha256, "nonexecution_operator_confirmation_mismatch")
            value = dict(
                schema="workbay/offload-nonexecution-attestation@1",
                identity=identity,
                classification=classification,
                record_sha256=_sha(raw),
                request_sha256=_sha(_encoded(identity)),
                evidence_locator=str(evidence_path),
                evidence_sha256=_sha(evidence),
                evidence_base64=base64.b64encode(evidence).decode(),
                operator_uid=os.getuid(),
                approved_by=pwd.getpwuid(os.getuid()).pw_name,
                reason=args.reason.strip(),
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            path, _ = _paths(args.repo, args.pass_id)
            if os.path.lexists(path):
                _validate_attestation(_private_bytes(path), identity, raw, classification)
            else:
                _append(path, _encoded(value))
        print(json.dumps(dict(status="attested", pass_id=args.pass_id)))
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        print(json.dumps(dict(status="hold", reason=str(exc))))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
