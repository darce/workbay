"""Publish branch review judgments and collect host-bound native evidence.

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


class _ReviewConflict(ValueError):
    """Two producers supplied different known values for the same judgment."""


def _merge_known(previous: dict, observed: dict) -> dict:
    merged = dict(previous)
    for key, value in observed.items():
        if value is not None and merged.get(key) is not None and value != merged[key]:
            raise _ReviewConflict(f"review_run_identity_mismatch:{key}")
        if value is not None or key not in merged:
            merged[key] = value
    return merged


def publish_branch_judgment(
    run_id: str,
    *,
    task_ref: str,
    session: str,
    subject_path: str,
    actor: dict,
    verdict: str | None,
    producer: str,
    judge_pin: dict,
    receipt: dict | None = None,
    cycle_n: int | None = None,
    parent_run_id: str | None = None,
) -> dict:
    """Commit the producer's observation before writing findings or projecting the run.

    The host receipt already binds the managed pass to its reserved dispatch.
    The dispatch and its native collection therefore share the managed pass ID,
    even if native collection happens first. Neither a session label nor the
    number of rows in the database is a run identity.
    """
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    receipt = receipt or {}
    reservation = (receipt.get("decision") or {}).get("row") or {}
    dispatch_id = reservation.get("dispatch_id")
    run_id = (receipt.get("managed_identity") or {}).get("pass_id") or dispatch_id or run_id
    cycle_n = reservation.get("round", cycle_n)
    previous = reservation.get("previous_gate_dispatch_id")
    scope = dict(task_ref=task_ref, branch=actor["branch"], commit_sha=actor["commit_sha"])
    observation = dict(session=session, subject_path=subject_path)
    pin = {key: None if value in ("", "unknown") else value for key, value in judge_pin.items()}
    pin["dispatch_id"] = dispatch_id
    try:
        with _get_db_connection(begin_immediate=True) as conn:
            from workbay_handoff_mcp.review_findings_support import _canonical_task_ref_from_handoff

            canonical_task = _canonical_task_ref_from_handoff(conn, task_ref)
            if canonical_task is None:
                return dict(status="refused", reason="unknown_task_ref", run_id=run_id)
            scope["task_ref"] = task_ref = canonical_task
            if previous:
                parent = conn.execute(
                    """SELECT run_id FROM branch_review_judgments WHERE dispatch_id = ? AND task_ref = ?
                       UNION SELECT review_run_id FROM review_runs WHERE task_ref = ?
                       AND (review_run_id = ? OR CASE WHEN json_valid(judge_pin_json)
                            THEN json_extract(judge_pin_json, '$.dispatch_id') END = ?)""",
                    (previous, task_ref, task_ref, previous, previous),
                ).fetchall()
                if len(parent) == 1:
                    parent_run_id = parent[0][0]
            # A BASE native collector may have used the dispatch ID for this
            # managed pass. Retain the actual row and its foreign dependents.
            aliases = conn.execute(
                """SELECT run_id FROM branch_review_judgments WHERE run_id=? OR dispatch_id=?
                   UNION SELECT review_run_id FROM review_runs WHERE review_run_id IN (?, ?)""",
                (run_id, dispatch_id, run_id, dispatch_id),
            ).fetchall()
            if len(aliases) == 1:
                run_id = aliases[0][0]
            old = conn.execute("SELECT * FROM branch_review_judgments WHERE run_id = ?", (run_id,)).fetchone()
            metadata = dict(verdict=verdict, cycle_n=cycle_n, parent_run_id=parent_run_id, dispatch_id=dispatch_id)
            sources = {producer: observation}
            try:
                if len(aliases) > 1:
                    raise _ReviewConflict("review_run_alias_conflict")
                if old:
                    _merge_known({key: old[key] for key in scope}, scope)
                    metadata = _merge_known({key: old[key] for key in metadata}, metadata)
                    previous_pin = json.loads(old["judge_pin_json"])
                    previous_pin.pop("unavailable_fields", None)
                    pin = _merge_known(previous_pin, pin)
                    sources = json.loads(old["sources_json"])
                    sources[producer] = _merge_known(sources.get(producer, {}), observation)
                state, error = "pending", None
            except _ReviewConflict as exc:
                state, error = "identity_mismatch", str(exc)
                conn.execute(
                    "UPDATE branch_review_judgments SET projection_status=?, projection_error=? WHERE run_id=?",
                    (state, error, run_id),
                )
            pin["unavailable_fields"] = [
                key
                for key, value in (
                    ("model", pin.get("model")),
                    ("effort", pin.get("effort")),
                    ("cycle_n", metadata["cycle_n"]),
                    ("parent_run_id", metadata["parent_run_id"]),
                )
                if value is None
            ]
            conn.execute(
                """INSERT INTO branch_review_judgments
                   (run_id, task_ref, branch, commit_sha, dispatch_id, verdict, cycle_n, parent_run_id,
                    judge_pin_json, sources_json, projection_status, projection_error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (run_id, task_ref, branch, commit_sha) DO UPDATE SET
                       dispatch_id=excluded.dispatch_id, verdict=coalesce(branch_review_judgments.verdict, excluded.verdict),
                       cycle_n=excluded.cycle_n, parent_run_id=excluded.parent_run_id,
                       judge_pin_json=excluded.judge_pin_json, sources_json=excluded.sources_json
                   WHERE branch_review_judgments.projection_status <> 'identity_mismatch'""",
                (
                    run_id,
                    task_ref,
                    scope["branch"],
                    scope["commit_sha"],
                    metadata["dispatch_id"],
                    metadata["verdict"],
                    metadata["cycle_n"],
                    metadata["parent_run_id"],
                    _json(pin),
                    _json(sources),
                    state,
                    error,
                ),
            )
            if error or (old and old["projection_status"] == "identity_mismatch"):
                return dict(status="identity_mismatch", reason=error or old["projection_error"], run_id=run_id)
        return dict(
            status="published",
            run_id=run_id,
            session=session,
            subject_path=subject_path,
            actor=actor,
            verdict=metadata["verdict"],
        )
    except (OSError, sqlite3.Error, ValueError, RuntimeError) as exc:
        return dict(
            status="failed", reason="review_judgment_unavailable", error=f"{type(exc).__name__}: {exc}", run_id=run_id
        )


def project_branch_judgment(
    publication: dict,
    *,
    finding_ids: list[str],
    blocking_ids: list[str],
    blocking_paths: set[str] | None = None,
) -> dict:
    """Project a committed judgment. The existing writer retains its branch review guards."""
    from workbay_handoff_mcp import core
    from workbay_handoff_mcp.api import link_review_run_findings
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    if publication["status"] != "published":
        return publication
    run_id = publication["run_id"]
    try:
        with _get_db_connection(begin_immediate=True) as conn:
            journal = conn.execute("SELECT * FROM branch_review_judgments WHERE run_id=?", (run_id,)).fetchone()
            if journal["projection_status"] == "identity_mismatch":
                raise _ReviewConflict(journal["projection_error"])
            ids = set(json.loads(journal["blocking_ids"] or "[]")) | set(blocking_ids)
            if blocking_paths and finding_ids:
                # Standalone finding IDs are assigned (and sometimes re-keyed)
                # by the findings writer. Use its confirmed rows in the reviewed scope.
                from workbay_protocol.review_impact import is_blocking

                recorded = conn.execute(
                    """SELECT finding_id, severity, impact, origin, file_path FROM review_findings
                       WHERE task_ref=? AND session=? AND status='open'
                       AND finding_id IN (SELECT value FROM json_each(?))""",
                    (journal["task_ref"], publication["session"], _json(finding_ids)),
                )
                ids.update(
                    row["finding_id"]
                    for row in recorded
                    if row["file_path"] in blocking_paths and is_blocking(dict(row))
                )
            conn.execute(
                "UPDATE branch_review_judgments SET blocking_ids=? WHERE run_id=?", (_json(sorted(ids)), run_id)
            )
            existing = conn.execute("SELECT 1 FROM review_runs WHERE review_run_id=?", (run_id,)).fetchone()
        response = None
        if existing is None:
            response = core.record_review_run(
                review_run_id=run_id,
                task_ref=journal["task_ref"],
                subject_kind="branch",
                review_mode="branch",
                session=publication["session"],
                subject_path=publication["subject_path"],
                actor=publication["actor"],
                verdict=journal["verdict"],
                cycle_n=journal["cycle_n"],
                parent_run_id=journal["parent_run_id"],
                judge_pin_json=journal["judge_pin_json"],
            )
        # The writer uses its own transaction. Re-read after it returns: a
        # concurrent collector can win the insert and still be the same run.
        with _get_db_connection(begin_immediate=True) as conn:
            journal = conn.execute("SELECT * FROM branch_review_judgments WHERE run_id=?", (run_id,)).fetchone()
            row = conn.execute("SELECT * FROM review_runs WHERE review_run_id=?", (run_id,)).fetchone()
            if journal["projection_status"] == "identity_mismatch":
                raise _ReviewConflict(journal["projection_error"])
            if row is None:
                data = (response or {}).get("data") or {}
                result = dict(status="refused", reason=data.get("error") or "review_run_refused", response=response)
            else:
                fields = ("task_ref", "branch", "commit_sha", "verdict", "cycle_n", "parent_run_id")
                _merge_known(
                    dict(subject_kind=row["subject_kind"], review_mode=row["review_mode"]),
                    dict(subject_kind="branch", review_mode="branch"),
                )
                merged = _merge_known({key: row[key] for key in fields}, {key: journal[key] for key in fields})
                sources = json.loads(journal["sources_json"])
                known_sessions = {source["session"] for source in sources.values()}
                known_subjects = {source["subject_path"] for source in sources.values()}
                if journal["dispatch_id"]:
                    known_sessions.add("review-cycle:" + journal["dispatch_id"])
                    known_subjects.add(journal["branch"])
                if row["session"] and row["session"] not in known_sessions:
                    raise _ReviewConflict("review_run_identity_mismatch:session")
                if row["subject_path"] not in known_subjects:
                    raise _ReviewConflict("review_run_identity_mismatch:subject_path")
                prior_pin = json.loads(row["judge_pin_json"] or "{}")
                prior_pin.pop("unavailable_fields", None)
                # Older producer pins used a sentinel or nested the executed route.
                for key in ("backend", "model", "effort"):
                    value = prior_pin.get(key) or (prior_pin.get("route") or {}).get(key)
                    prior_pin[key] = None if value in ("", "unknown") else value
                pin = _merge_known(prior_pin, json.loads(journal["judge_pin_json"]))
                model = _merge_known({"model": row["model"]}, {"model": pin.get("model")})["model"]
                conn.execute(
                    """UPDATE review_runs SET task_ref=:task_ref, branch=:branch, commit_sha=:commit_sha,
                       verdict=:verdict, cycle_n=:cycle_n, parent_run_id=:parent_run_id, judge_pin_json=:pin, model=:model
                       WHERE review_run_id=:run""",
                    {**merged, "pin": _json(pin), "run": run_id, "model": model},
                )
                result = dict(status="recorded", review_run_id=run_id)
                link_review_run_findings(conn, run_id, finding_ids, session=publication["session"])
            conn.execute(
                "UPDATE branch_review_judgments SET projection_status=?, projection_error=? WHERE run_id=?",
                (result["status"], result.get("reason"), run_id),
            )
            return {**result, "run_id": run_id}
    except _ReviewConflict as exc:
        result = dict(status="identity_mismatch", reason=str(exc))
    except (sqlite3.Error, OSError, RuntimeError, ValueError, TypeError) as exc:
        result = dict(status="failed", reason="review_projection_failed", error=f"{type(exc).__name__}: {exc}")
    try:
        with _get_db_connection(begin_immediate=True) as conn:
            conn.execute(
                """UPDATE branch_review_judgments SET projection_status=?, projection_error=?
                   WHERE run_id=? AND projection_status <> 'identity_mismatch'
                     AND (projection_status <> 'recorded' OR ? = 'identity_mismatch')""",
                (result["status"], result.get("error") or result.get("reason"), run_id, result["status"]),
            )
    except (sqlite3.Error, RuntimeError, OSError) as exc:
        return dict(status="failed", reason="review_projection_receipt_unavailable", error=str(exc), run_id=run_id)
    return {**result, "run_id": run_id}


def clean_judgment_with_projection_fault(dispatch_id: str, harvest: dict) -> bool:
    """A failed SQLite projection leaves a durable clean gate eligible to settle."""
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    receipt = harvest.get("judgment_receipt") or {}
    if receipt.get("status") != "published":
        return False
    try:
        with _get_db_connection() as conn:
            return (
                conn.execute(
                    """SELECT 1 FROM branch_review_judgments WHERE run_id=? AND dispatch_id=?
                   AND verdict IN ('pass', 'pass_with_findings') AND blocking_ids='[]'
                   AND projection_status IN ('failed', 'refused', 'recorded')""",
                    (receipt["run_id"], dispatch_id),
                ).fetchone()
                is not None
            )
    except (KeyError, sqlite3.Error, OSError, RuntimeError):
        return False


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
            review_path = config.state_dir / f"review-receipt-{job}.json"
            if review_path.exists():
                saved = json.loads(review_path.read_text())
                if saved.get("identity", {}).get("native_result_sha256") != digest:
                    raise ValueError("native_review_result_conflict")
            observed_route = host.get("executed_route") or {}
            from workbay_handoff_mcp.shared_write_context import build_write_actor

            actor = build_write_actor(
                lane_id=lane["lane_id"],
                branch=scope["source_branch"],
                commit_sha=scope["tip_sha"],
                model=observed_route.get("model"),
                reasoning_level=observed_route.get("effort"),
            )
            session = "review-cycle:" + job
            publication = None
            if output["judgment"]["complete"] and output["judgment"]["verdict"] in {"pass", "pass_with_findings", "fail"}:
                publication = publish_branch_judgment(
                    job, task_ref=scope["task_ref"], session=session, subject_path=scope["base_sha"] + "..." + scope["tip_sha"],
                    actor=actor, verdict=output["judgment"]["verdict"], producer="native", receipt=host,
                    judge_pin={key: observed_route.get(key) for key in ("backend", "model", "effort")},
                )
                output["judgment_receipt"] = publication
            route = host.get("executed_route") or host["decision"]["row"]["required_route"]
            if not isinstance(route, dict) or not route.get("model") or not route.get("effort"):
                raise ValueError("native_review_route_unavailable")
            identity = dict(
                pass_id=job,
                dispatch_id=job,
                task_ref=lane["task_ref"],
                lane_id=lane["lane_id"],
                branch=scope["source_branch"],
                base_sha=scope["base_sha"],
                tip_sha=scope["tip_sha"],
                backend=route.get("backend") or (
                    "codex-remote" if route["model"].startswith("gpt-") else "claude-remote"),
                model=route["model"],
                reasoning_effort=route["effort"],
                worktree_path=str(Path(lane["worktree_path"]).resolve()),
                agent_stream_path=None,
                review_cycle_subject_branch=host["identity"]["subject_branch"],
                review_cycle=host["decision"],
                native_result_sha256=digest,
                started_at=host["decision"]["row"]["activated_at"],
                source_task_subject=host.get("source_task_subject"),
            )
            from .offload_pass import _PASSING_HARVEST_VERDICTS, _harvest_review_findings, _harvest_verdict

            # The native structured format needs a carrier for the canonical
            # parser. Retained raw evidence above is never rewritten or elided.
            carrier = result
            if isinstance(result.get("findings"), list) and result.get("result_parse") != "degraded":
                carrier = {**result, "details": "GROK_REVIEW_FINDINGS_JSON\n" + _json(result["findings"])}
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
                output["review_run_receipt"] = project_branch_judgment(
                    publication, finding_ids=harvest.get("recorded_ids") or harvest.get("finding_ids") or [],
                    blocking_ids=[row["finding_id"] for row in harvest.get("blocking_findings", [])],
                )
                if output["review_run_receipt"]["status"] == "identity_mismatch":
                    output["review_run_receipt"].update(status="refused", reason="native_review_run_identity_mismatch")
                if output["review_run_receipt"]["status"] == "recorded":
                    output["review_run_id"] = output["review_run_receipt"]["review_run_id"]
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
