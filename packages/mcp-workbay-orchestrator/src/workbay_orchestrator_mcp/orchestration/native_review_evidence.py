"""Collect native review evidence without granting model output scope authority.

Canon 0.26.2: SECD-03 complete mediation (Security Engineering), DATA-14
No dual writes (Designing Data-Intensive Applications, ch. 11), GRPH-14
walkable lineage (Building Knowledge Graphs, ch. 8). Raw artifacts are retained
inputs; findings and typed reviews are projections through the existing APIs.
This consumer neither reserves nor settles a review-cycle attempt.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
import sqlite3
import subprocess
from pathlib import Path
from typing import Any


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, text=True, capture_output=True, timeout=5
    ).stdout.strip()


def _scope(repo: Path, host: dict, config: Any, lane: dict) -> dict:
    from . import review_cycle_dispatch as cycle
    from .lane_manifest import expand_path_template
    from .review_ratchet import _manifest_lane

    scope = host.get("source_review_scope")
    if scope is None:
        raise ValueError("native_review_scope_unavailable")
    if not isinstance(scope, dict) or type(scope.get("schema_version")) is not int or scope["schema_version"] != 1:
        raise ValueError("native_review_scope_invalid")
    expected = dict(
        task_ref=lane["task_ref"],
        lane_id=lane["lane_id"],
        source_branch=host["source_branch"],
        review_cycle_subject_branch=host["identity"]["subject_branch"],
        tip_sha=host["source_head"],
        source_tree=host["source_tree"],
    )
    if any(scope.get(key) != value for key, value in expected.items()):
        raise ValueError("native_review_scope_binding_mismatch")
    manifest_lane = _manifest_lane(config.workspace_root, lane["task_ref"], lane["lane_id"])
    if manifest_lane is None:
        raise ValueError("native_review_context_unavailable")
    manifest, configured_lane = manifest_lane
    source_repo = Path(lane.get("worktree_path") or "").resolve()
    configured_worktree = expand_path_template(
        configured_lane.get("worktree_path") or "", orchestrator_root=str(config.workspace_root)
    )
    if (
        scope.get("integration_branch") != (manifest.get("integration_branch") or "main")
        or configured_lane.get("branch") != host["source_branch"]
        or not lane.get("worktree_path")
        or Path(configured_worktree).resolve() != source_repo
        or _git(source_repo, "rev-parse", "--path-format=absolute", "--git-common-dir")
        != _git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir")
    ):
        raise ValueError("native_review_context_mismatch")
    for key in ("base_sha", "tip_sha", "source_tree"):
        if not isinstance(scope.get(key), str) or not re.fullmatch(r"[0-9a-f]{40}", scope[key]):
            raise ValueError("native_review_git_identity_invalid")
    base, tip = scope["base_sha"], scope["tip_sha"]
    if any(_git(repo, "rev-parse", "--verify", sha + "^{commit}") != sha for sha in (base, tip)):
        raise ValueError("native_review_commit_mismatch")
    if (
        _git(repo, "rev-parse", tip + "^{tree}") != scope["source_tree"]
        or _git(repo, "rev-parse", "--verify", "refs/heads/" + scope["source_branch"]) != tip
        or _git(source_repo, "rev-parse", "--verify", "HEAD") != tip
    ):
        raise ValueError("native_review_source_mismatch")
    integration = _git(repo, "rev-parse", "--verify", "refs/heads/" + scope["integration_branch"] + "^{commit}")
    # Integration may advance after reservation; the frozen base must remain
    # in its history, and the candidate must contain that exact merged input.
    _git(repo, "merge-base", "--is-ancestor", base, integration)
    _git(repo, "merge-base", "--is-ancestor", base, tip)
    if base == tip or not _git(repo, "diff", "--name-only", base + "..." + tip):
        raise ValueError("native_review_range_empty")
    reserved = cycle.reserved_row(repo, host["identity"])
    frozen = host["decision"]["row"]
    for key in ("dispatch_id", "subject_branch", "kind", "required_route", "round", "epoch"):
        if reserved.get(key) != frozen.get(key):
            raise ValueError("native_review_reservation_mismatch")
    return scope


def _judgment(result: dict) -> dict:
    from .offload_pass import _review_completion

    completion = _review_completion(result)
    if not isinstance(result, dict):
        return completion
    declared = result.get("verdict")
    if declared is not None:
        if not isinstance(declared, str) or declared not in {"pass", "pass_with_findings", "fail"}:
            return dict(complete=False, verdict=declared, reason="native_review_judgment_invalid")
        if completion["verdict"] is not None and completion["verdict"] != declared:
            return dict(complete=False, verdict=declared, reason="native_review_judgment_mismatch")
        return dict(complete=True, verdict=declared, reason="native_review_judgment_declared")
    return completion


def _review_run(job: str, scope: dict, actor: dict, judgment: dict, session: str) -> dict:
    from workbay_handoff_mcp.core import record_review_run
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    expected = dict(
        task_ref=scope["task_ref"],
        session=session,
        subject_path=scope["base_sha"] + "..." + scope["tip_sha"],
        subject_kind="branch",
        review_mode="branch",
        verdict=judgment["verdict"],
        branch=actor["branch"],
        commit_sha=actor["commit_sha"],
        model=actor["model"],
        agent=actor["agent"],
        model_label=actor["model_label"],
    )
    with _get_db_connection() as conn:
        existing = conn.execute("SELECT * FROM review_runs WHERE review_run_id = ?", (job,)).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            return dict(status="refused", reason="native_review_run_identity_mismatch")
        return dict(status="recorded", review_run_id=job)
    response = record_review_run(
        review_run_id=job,
        actor=actor,
        **{
            key: value
            for key, value in expected.items()
            if key not in {"branch", "commit_sha", "model", "agent", "model_label"}
        },
    )
    if response.get("ok") is not True:
        return dict(status="refused", reason="native_review_run_refused", response=response)
    return dict(status="recorded", review_run_id=job, response=response)


def harvest_native_review(repo: Path, receipt: dict, result: dict) -> dict:
    """Re-read host authority; retain raw evidence even if projection is held.

    Native return code, declared judgment, findings harvest and typed recording
    are separate fields. Missing legacy scope never falls back to HEAD or the
    original source_task_subject baseline. No new gate is created for repair.
    """
    from . import review_cycle_dispatch as cycle

    output: dict[str, Any] = dict(
        status="unavailable",
        reason="native_review_receipt_unavailable",
        raw_result=result,
        native_rc=None,
        judgment=_judgment(result),
        review_run_receipt={"status": "unavailable"},
    )
    try:
        job = receipt["identity"]["dispatch_id"]
        if not isinstance(job, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", job):
            raise ValueError("native_review_job_invalid")
        path = cycle.authority(repo) / "review_cycle_dispatch" / (hashlib.sha256(job.encode()).hexdigest() + ".json")
        if not path.is_file():
            return output
        with path.with_suffix(".lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            host = json.loads(path.read_text())
            registered = cycle.registered_lane(repo, host.get("source_branch") or "")
            if registered is None:
                raise ValueError("native_review_registry_unavailable")
            config, lane = registered
            from workbay_handoff_mcp.core import record_artifact
            from workbay_handoff_mcp.runtime import configure_runtime

            configure_runtime(config)
            digest = hashlib.sha256(_json(result).encode()).hexdigest()
            native = host.get("native_completion") or {}
            rc = native.get("rc")
            if type(rc) is int or (isinstance(rc, str) and re.fullmatch(r"-?[0-9]+", rc)):
                output["native_rc"] = int(rc)
            evidence = dict(
                result=result,
                identity=host.get("identity"),
                source_review_scope=host.get("source_review_scope"),
                source_task_subject=host.get("source_task_subject"),
                source_branch=host.get("source_branch"),
                source_head=host.get("source_head"),
                source_tree=host.get("source_tree"),
                decision=host.get("decision"),
                native_completion=native,
            )
            evidence_digest = hashlib.sha256(_json(evidence).encode()).hexdigest()
            # Each observation has its own immutable label. A later collection
            # cannot overwrite an earlier judgment or native completion state.
            artifact = record_artifact(
                source_kind="review_evidence",
                source_label=f"native-review:{job}:{evidence_digest}",
                content=_json(evidence),
                content_type="application/json",
                task_ref=lane["task_ref"],
                lane_id=lane["lane_id"],
                metadata=dict(native_job_id=job, result_sha256=digest),
            )
            output["raw_evidence"] = artifact
            if artifact.get("ok") is not True:
                raise ValueError("native_review_artifact_record_failed")
            if (
                host.get("identity", {}).get("kind") != "gate"
                or host.get("identity", {}).get("dispatch_id") != job
                or host.get("state") != "transport_started"
                or not isinstance(host.get("token"), str)
                or not host["token"]
                or host.get("decision", {}).get("status") != "reserved"
            ):
                raise ValueError("native_review_receipt_invalid")
            for key in (
                "identity",
                "token",
                "source_branch",
                "source_head",
                "source_tree",
                "source_review_scope",
                "decision",
            ):
                if receipt.get(key) != host.get(key):
                    raise ValueError("native_review_receipt_binding_mismatch")
            scope = _scope(repo, host, config, lane)
            output.pop("reason", None)
            output["source_review_scope"] = scope
            route = host["decision"]["row"]["required_route"]
            from workbay_handoff_mcp.shared_write_context import build_write_actor

            actor = build_write_actor(
                lane_id=lane["lane_id"],
                branch=scope["source_branch"],
                commit_sha=scope["tip_sha"],
                model=route["model"],
                reasoning_level=route["effort"],
            )
            identity = dict(
                pass_id=job,
                dispatch_id=job,
                task_ref=lane["task_ref"],
                lane_id=lane["lane_id"],
                branch=scope["source_branch"],
                base_sha=scope["base_sha"],
                tip_sha=scope["tip_sha"],
                backend="codex-remote" if route["model"].startswith("gpt-") else "claude-remote",
                model=route["model"],
                reasoning_effort=route["effort"],
                worktree_path=str(Path(lane["worktree_path"]).resolve()),
                agent_stream_path=None,
                review_cycle_subject_branch=host["identity"]["subject_branch"],
                review_cycle=host["decision"],
                native_result_sha256=digest,
                source_task_subject=host.get("source_task_subject"),
            )
            review_path = config.state_dir / f"review-receipt-{job}.json"
            if review_path.exists():
                saved = json.loads(review_path.read_text())
                if saved.get("identity", {}).get("native_result_sha256") != digest:
                    raise ValueError("native_review_result_conflict")
            from .offload_pass import _PASSING_HARVEST_VERDICTS, _harvest_review_findings, _harvest_verdict

            # The native structured format needs a carrier for the canonical
            # parser. Retained raw evidence above is never rewritten or elided.
            carrier = result
            if isinstance(result.get("findings"), list) and result.get("result_parse") != "degraded":
                carrier = {**result, "details": "GROK_REVIEW_FINDINGS_JSON\n" + _json(result["findings"])}
            session = "review-cycle:" + job
            harvest = _harvest_review_findings(
                carrier,
                None,
                task_ref=lane["task_ref"],
                lane_id=lane["lane_id"],
                session=session,
                orchestrator_root=config.workspace_root,
                receipt_path=review_path,
                receipt_identity=identity,
            )
            output.update(harvest)
            output["findings_harvest"] = harvest
            output["judgment"] = _judgment(result)
            from workbay_handoff_mcp.core import record_test_result

            if output["native_rc"] is not None:
                output["test_receipt"] = record_test_result(
                    task_ref=lane["task_ref"],
                    session=session,
                    command=f"Native review collection {job} {scope['base_sha']}...{scope['tip_sha']}",
                    passed=output["native_rc"] == 0
                    and output["judgment"]["complete"]
                    and output["judgment"]["verdict"] in {"pass", "pass_with_findings"},
                    exit_code=output["native_rc"],
                    result=_json(dict(judgment=output["judgment"], harvest=harvest)),
                    traces=[_json(evidence)],
                    actor=actor,
                    event_id="native-review:" + job,
                )
            else:
                output["test_receipt"] = dict(status="unavailable", reason="native_review_rc_unavailable")
            if (
                _harvest_verdict(harvest) in _PASSING_HARVEST_VERDICTS
                and not harvest.get("unrecorded_ids")
                and output["judgment"]["complete"]
                and output["judgment"]["verdict"] in {"pass", "pass_with_findings", "fail"}
            ):
                output["review_run_receipt"] = _review_run(job, scope, actor, output["judgment"], session)
                if output["review_run_receipt"]["status"] == "recorded":
                    output["review_run_id"] = job
                    output["status"] = "recorded"
                    output.pop("reason", None)
                    output.setdefault("blocking_findings", [])
                else:
                    output.update(status="record_pending", reason="native_review_run_refused")
            else:
                output.update(status="record_pending", reason="native_review_projection_incomplete")
            if output["native_rc"] is not None and output["test_receipt"].get("ok") is not True:
                output.update(status="record_pending", reason="native_review_test_record_failed")
            return output
    except (OSError, ValueError, KeyError, TypeError, AttributeError, sqlite3.Error, subprocess.SubprocessError) as exc:
        output.update(status="unavailable", reason=str(exc))
        output["repair"] = (
            "Recover and verify the original producer scope through supported reconciliation; "
            "retain custody and counters. Do not infer a base or reserve another automatic gate."
        )
        return output
