"""One bounded, remote planning-review round over canonical handoff APIs.

Delivery files are disposable, never review authority. Canon 0.26.2:
OPS-76 (planning:109, SRE ch.5); AGT-07 (engineering:81, Programmer's
Brain ch.11, CARD-05/P7); GRPH-33 (graph-theory:190, practice label);
AGT-21 (engineering:94, UNIX Programming Environment ch.5, CARD-01/P18).
DATA-14 (engineering:232, DDIA ch.11, CARD-02/P12): canonical records own truth.
CON-11 (engineering:185, OSTEP ch.32): completion checks bytes at the effect.
OBS-09 (engineering:615, Thinking in Systems ch.6, CARD-11/P15): independent
qualification is required; these receipts do not certify semantic review quality.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import subprocess
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import Any

from workbay_handoff_mcp import api
from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError, get_runtime_config

from .adapters import remote_job_runner
from .backend_spec import build_agent_spec, write_agent_spec
from .execution_preflight import analysis_argv
from .lane_lifecycle_contracts import RemoteJobIdentity, RemoteJobSpec, lane_key_for_branch
from .remote_job_ledger import RemoteJobLedger, RemoteJobLedgerError

# Exit statuses belong to this driver; native rc remains in the error receipt.
FAILURES = {
    "review_parameters_invalid": 20,
    "review_identity_conflict": 21,
    "review_route_unavailable": 22,
    "review_transport_failed": 23,
    "review_result_invalid": 24,
    "review_recording_partial": 25,
    "review_completion_conflict": 26,
    "review_dashboard_unknown": 27,
    "review_busy": 28,
    "review_subject_unavailable": 29,
}


class ReviewFailure(RuntimeError):
    def __init__(self, code: str, detail: Any = None):
        self.code, self.detail = code, detail
        super().__init__(code)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate result key: " + key)
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError("invalid JSON constant: " + value)


def _read_analysis_result(result: Path, schema: Path) -> Any:
    """Validate the caller's requested contract, including non-review analysis.

    This is output validation, never execution authority. External schema
    retrieval is disabled; local references in the requested schema still work.
    """
    from jsonschema.validators import validator_for
    from referencing import Registry

    try:

        def read(path):
            if path.stat().st_size > 1024 * 1024:
                raise ValueError("analysis contract oversized")
            return json.loads(
                path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object, parse_constant=_invalid_constant
            )

        contract, value = read(schema), read(result)
        validator = validator_for(contract)
        validator.check_schema(contract)
        validator(contract, registry=Registry()).validate(value)
        return value
    except Exception as exc:
        raise ReviewFailure("review_result_invalid", str(exc)[:800]) from exc


def _validate_analysis_completion(proof: dict, stream: Path, route: dict) -> None:
    """Consume native completion observations; never remint a launch grant.

    The native boundary already checked the owner's live context and freshness
    immediately before model_process_started. At harvest the owner has exited:
    validate the retained observation at its own finish time, using the existing
    validator, rather than requiring it to be fresh at collection. In particular
    this function cannot turn a failed native producer into success.
    """
    from .offload_pass_recovery import stream_turn_facts
    from .preflight_attestation import validate_execution_preflight

    try:
        preflight = proof["execution_preflight"]
        validation = validate_execution_preflight(
            preflight, capability="analysis-read-only", now=lambda: preflight["finished_at"]
        )
        # Non-PASS observations return their supplied reason before integrity checks.
        if preflight.get("status") != "PASS" or validation.get("reason") not in {
            "native_execution_preflight",
            "execution_owner_unavailable",
        }:
            raise ValueError("analysis preflight: " + str(validation.get("reason")))
        binding = preflight["binding"]
        source = binding["source"]
        owner = preflight.get("owner", {})
        keys = (
            "agent_spec_head_sha",
            "dispatch_nonce",
            "remote_source_commit",
            "remote_source_tree",
            "requested_branch",
            "sandbox_base_commit",
            "sandbox_base_tree",
        )
        if (
            proof.get("model_process_started") is not True
            or "execution_preflight_refusal" in proof
            or binding.get("execution_capability") != "analysis-read-only"
            or any(source.get(key) != proof[key] for key in keys)
            or preflight["source"] != {"commit": proof["sandbox_base_commit"], "tree": proof["sandbox_base_tree"]}
            or type(owner.get("pid")) is not int
            or owner["pid"] <= 0
            or type(owner.get("uid")) is not int
            or owner["uid"] < 0
            or not isinstance(owner.get("start"), str)
            or not owner["start"].isdigit()
            or any(
                binding[name].get(key) != route[key]
                for name in ("routing_requested", "routing_effective")
                for key in ("backend", "model", "effort", "speed")
            )
        ):
            raise ValueError("analysis execution binding mismatch")
        if stream.stat().st_size > 32 * 1024 * 1024:
            raise ValueError("analysis stream oversized")
        raw = stream.read_bytes()
        facts = stream_turn_facts(raw)
        events = [json.loads(line, object_pairs_hook=_unique_object) for line in raw.splitlines() if line.strip()]
        if (
            facts["malformed_lines"]
            or facts["turn_failed"]
            or not facts["turn_completed"]
            or facts["turn_started"] != facts["turn_completed"]
            or not events
            or events[-1].get("type") != "turn.completed"
            or any(not isinstance(event, dict) or event.get("type") == "error" for event in events)
        ):
            raise ValueError("analysis model completion unobserved")
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise ReviewFailure("review_transport_failed", str(exc)[:800]) from exc


def _checked(result: dict, code: str) -> dict:
    if not isinstance(result, dict) or result.get("ok") is not True:
        raise ReviewFailure(code, result)
    return result.get("data", result)


def configure(repo: Path) -> None:
    try:
        config = get_runtime_config()
    except RuntimeNotConfiguredError:
        config = None
    if config is None or config.workspace_root.resolve() != repo.resolve():
        api.configure_runtime(RuntimeConfig.for_repo(repo))


def _runs(task: str):
    offset = 0
    while True:
        data = _checked(
            api.review_runs(
                review=api.ReviewRunsListOp.model_validate(
                    {
                        "operation": "list",
                        "task_ref": task,
                        "review_mode": "planning",
                        "limit": 100,
                        "offset": offset,
                    }
                )
            ),
            "review_subject_unavailable",
        )
        yield from data["runs"]
        if not data.get("has_more"):
            return
        offset += len(data["runs"])
        if not data["runs"]:
            raise ReviewFailure("review_subject_unavailable", "incomplete pagination")


def existing_subject(repo: Path, task: str, run_id: str, doc: Path, source_ref: str) -> dict | None:
    """Read an existing bound row; never replay the insert-only freeze producer."""
    configure(repo)
    rows = [r for r in _runs(task) if r["review_run_id"] == run_id]
    if not rows:
        return None
    row = rows[0]
    try:
        subject = json.loads(row["judge_pin_json"])["plan_review_subject"]
        path = doc.resolve().relative_to(repo.resolve()).as_posix()
        commit = _git(repo, "rev-parse", "--verify", "--end-of-options", source_ref + "^{commit}")
        if (
            subject["task_ref"] != task
            or subject["review_run_id"] != run_id
            or subject["subject_path"] != path
            or subject["source_commit"] != commit
            or row["subject_sha"] != subject["subject_sha256"]
            or _sha(doc.read_bytes()) != subject["subject_sha256"]
        ):
            raise ValueError("changed subject")
        return subject
    except (KeyError, ValueError, TypeError, OSError) as exc:
        raise ReviewFailure("review_identity_conflict", str(exc)) from exc


def _git(repo: Path, *args: str) -> str:
    env = {
        k: v for k, v in os.environ.items() if k not in {"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"}
    }
    try:
        return subprocess.run(
            ["git", "-C", str(repo), *args], env=env, capture_output=True, text=True, check=True, timeout=30
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReviewFailure("review_subject_unavailable", str(exc)) from exc


def _decision(task: str, name: str) -> dict | None:
    data = _checked(
        api.get_handoff_state(
            task_ref=task,
            sections="decisions_recent",
            detail="full",
            decision_id_prefix=name,
            decision_fields=["decision", "rationale"],
            top_n_decisions=100,
        ),
        "review_recording_partial",
    )
    rows = [r for r in data.get("decisions_recent", []) if r["decision"] == name]
    if len(rows) > 1:
        raise ReviewFailure("review_identity_conflict", "ambiguous canonical decision")
    if not rows:
        return None
    try:
        return json.loads(rows[0]["rationale"])
    except (ValueError, TypeError) as exc:
        raise ReviewFailure("review_identity_conflict", "unreadable canonical decision") from exc


def _record_decision(task: str, session: str, name: str, value: dict) -> None:
    text = _json(value)
    if len(text) > 2900:
        raise ReviewFailure("review_parameters_invalid", "decision binding too large")
    data = _checked(
        api.record_event(
            event=api.RecordDecisionEvent.model_validate(
                {
                    "event_kind": "decision",
                    "event_id": name,
                    "decision": name,
                    "task_ref": task,
                    "session": session,
                    "rationale": text,
                }
            )
        ),
        "review_recording_partial",
    )
    stored = data.get("decision", {})
    # Both the first write and an event-ID replay must acknowledge identical data.
    if not isinstance(stored, dict) or stored.get("rationale") != text:
        if _decision(task, name) != value:
            raise ReviewFailure("review_identity_conflict", "decision replay mismatch")


@contextmanager
def _lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReviewFailure("review_busy", "same run already executing") from exc
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _parameters(args: Any, subject: dict) -> dict:
    round_n = getattr(args, "round", 1)
    prior_path = getattr(args, "prior_adjudication", None)
    scope = getattr(args, "scope_decision", None)
    justification = getattr(args, "round_justification", None)
    if (
        type(round_n) is not int
        or round_n < 1
        or (round_n > 2 and (not isinstance(justification, str) or not justification.strip()))
    ):
        raise ReviewFailure("review_parameters_invalid", "round >2 requires explicit justification")
    history = list(_runs(subject["task_ref"]))
    already_complete = any(
        row["review_run_id"] == subject["review_run_id"] and row["verdict"] is not None for row in history
    )
    completed_history = [
        row
        for row in history
        if row["review_run_id"] != subject["review_run_id"]
        and row["subject_path"].casefold() == subject["subject_path"].casefold()
        and row["verdict"] is not None
    ]
    if (
        not already_complete
        and len(completed_history) >= 2
        and (not isinstance(justification, str) or not justification.strip())
    ):
        raise ReviewFailure("review_parameters_invalid", "more than two subject reviews require explicit justification")
    prior = None
    if prior_path:
        try:
            raw = Path(prior_path).read_bytes()
            prior = json.loads(raw)
            if (
                not isinstance(prior, dict)
                or not isinstance(prior.get("review_run_id"), str)
                or not prior["review_run_id"]
                or prior["review_run_id"] == subject["review_run_id"]
                or prior.get("round") != round_n - 1
                or prior.get("task_ref") != subject["task_ref"]
            ):
                raise ValueError("prior identity does not match preceding round/task")
            prior_row = next(
                (row for row in _runs(subject["task_ref"]) if row["review_run_id"] == prior["review_run_id"]), None
            )
            if (
                prior_row is None
                or prior_row["verdict"] is None
                or prior_row["subject_path"] != subject["subject_path"]
                or (prior.get("subject_sha256") is not None and prior["subject_sha256"] != prior_row["subject_sha"])
            ):
                raise ValueError("prior adjudication does not identify a completed canonical subject review")
            prior_binding = _decision(
                subject["task_ref"], "planreview_" + _sha(prior["review_run_id"].encode()) + "_binding"
            )
            if prior_binding is not None and prior_binding.get("round") != round_n - 1:
                raise ValueError("canonical prior round mismatch")
            prior = {"review_run_id": prior["review_run_id"], "sha256": _sha(raw), "adjudication": prior}
        except (OSError, ValueError, TypeError) as exc:
            raise ReviewFailure("review_parameters_invalid", str(exc)) from exc
    if (round_n > 1 and prior is None) or (round_n == 1 and prior is not None):
        raise ReviewFailure(
            "review_parameters_invalid", "round 1 has no prior; later rounds require prior adjudication"
        )
    route = {
        "backend": getattr(args, "review_route", "codex-remote"),
        "model": getattr(args, "model", "gpt-6.1-sol"),
        "effort": getattr(args, "effort", "high"),
        "speed": getattr(args, "speed", "standard"),
    }
    if route["backend"] != "codex-remote" or route["speed"] != "standard":
        raise ReviewFailure(
            "review_route_unavailable", "only explicit catalogue-supported codex-remote STANDARD routes"
        )
    if scope is not None and (not isinstance(scope, str) or not scope.strip() or len(scope) > 200):
        raise ReviewFailure("review_parameters_invalid", "invalid scope decision")
    return {
        "round": round_n,
        "prior": prior,
        "scope_decision": scope,
        "round_justification": justification,
        "model_route": route,
    }


def _literal_schema(value: Any) -> dict:
    if isinstance(value, dict):
        return {
            "type": "object",
            "additionalProperties": False,
            "required": list(value),
            "properties": {key: _literal_schema(item) for key, item in value.items()},
        }
    kind = "null" if value is None else "integer" if type(value) is int else "string"
    return {"type": kind, "enum": [value]}


def _schema(binding: dict) -> dict:
    string = {"type": "string", "minLength": 1}
    properties = {
        "severity": {"type": "string", "enum": ["high", "medium", "low"]},
        "file_path": string,
        "description": string,
        "failure_scenario": string,
        "impact": {"type": "string", "enum": ["blocking", "follow_up"]},
        "origin": {"type": "string", "enum": ["introduced", "listed_open", "pre_existing"]},
        "impact_reason": string,
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["binding", "verdict", "findings", "notes"],
        "properties": {
            "binding": _literal_schema(binding),
            "verdict": {"type": "string", "enum": ["pass", "pass_with_findings", "fail", "conditional_pass"]},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": list(properties),
                    "properties": properties,
                },
            },
            "notes": {"type": "array", "maxItems": 5, "items": {"type": "string", "maxLength": 240}},
        },
    }


def _validate_result(result: Any, binding: dict) -> list[dict]:
    try:
        if (
            not isinstance(result, dict)
            or set(result) != {"binding", "verdict", "findings", "notes"}
            or _json(result["binding"]) != _json(binding)
            or result["verdict"] not in {"pass", "pass_with_findings", "fail", "conditional_pass"}
            or not isinstance(result["findings"], list)
            or not isinstance(result["notes"], list)
            or len(result["notes"]) > 5
            or any(not isinstance(n, str) or len(n) > 240 for n in result["notes"])
        ):
            raise ValueError("strict review result required")
        findings = result["findings"]
        if result["notes"] and not findings:
            raise ValueError("notes-only output is not qualified review authority; return defects in findings[]")
        fields = _schema(binding)["properties"]["findings"]["items"]["properties"]
        for f in findings:
            if not isinstance(f, dict) or set(f) != set(fields):
                raise ValueError("all defect fields required; unknown fields forbidden")
            for key, shape in fields.items():
                if (
                    not isinstance(f[key], str)
                    or not f[key].strip()
                    or ("enum" in shape and f[key] not in shape["enum"])
                ):
                    raise ValueError("invalid finding field: " + key)
        for f in findings:
            path = PurePosixPath(f["file_path"])
            if (
                not path.parts
                or path.is_absolute()
                or ".." in path.parts
                or str(path) != f["file_path"]
                or "\\" in f["file_path"]
                or any(not f[k].strip() for k in ("description", "failure_scenario", "impact_reason"))
            ):
                raise ValueError("invalid finding path or evidence")
        if result["verdict"] == "pass" and findings:
            raise ValueError("pass cannot omit findings from verdict")
        if result["verdict"] in {"pass", "pass_with_findings", "conditional_pass"} and any(
            f["impact"] == "blocking" for f in findings
        ):
            raise ValueError("success verdict with blocking defects")
        return findings
    except (ValueError, KeyError, TypeError) as exc:
        raise ReviewFailure("review_result_invalid", str(exc)[:800]) from exc


def _finding_ids(run_id: str, count: int) -> list[str]:
    return [f"NWI04-{_sha(run_id.encode())[:20]}-{i + 1:06d}" for i in range(count)]


def _read_findings(task: str, ids: list[str]) -> dict[str, dict]:
    rows = {}
    for start in range(0, len(ids), 100):
        wanted = ids[start : start + 100]
        data = _checked(
            api.review_findings(
                review=api.ReviewFindingsListOp.model_validate(
                    {
                        "operation": "list",
                        "task_ref": task,
                        "finding_ids": wanted,
                        "status": "all",
                        "limit": 100,
                        "detail": "full",
                    }
                ),
                response_budget_bytes=1048576,
            ),
            "review_recording_partial",
        )
        for row in data.get("findings", []):
            rows[row["finding_id"]] = row
    return rows


def _batch_event(run_id: str, start: int) -> str:
    return "planreview_" + _sha(run_id.encode()) + f"_findings_{start // 100}"


def _batch_rows(task: str, run_id: str, start: int) -> list[dict] | None:
    result = api.review_findings(
        review=api.ReviewFindingsListOp.model_validate(
            {
                "operation": "list",
                "task_ref": task,
                "event_id": _batch_event(run_id, start),
                "limit": 100,
                "detail": "full",
            }
        ),
        response_budget_bytes=1048576,
    )
    if not result.get("ok") and result.get("data", {}).get("error") == "Unknown or incomplete committed-batch receipt.":
        return None
    data = _checked(result, "review_recording_partial")
    if data.get("has_more") or data.get("missing_ids") or data.get("returned") != data.get("total_matching"):
        raise ReviewFailure("review_recording_partial", "incomplete canonical batch read")
    return data["findings"]


def _completed_ids(task: str, run_id: str, count: int) -> list[str]:
    ids: list[str] = []
    for start in range(0, count, 100):
        rows = _batch_rows(task, run_id, start)
        if rows is None or len(rows) != min(100, count - start):
            raise ReviewFailure("review_recording_partial", "completed batch unavailable")
        ids.extend(row["finding_id"] for row in rows)
    return ids


def _record_findings(task: str, session: str, run_id: str, findings: list[dict]) -> list[str]:
    requested_ids = _finding_ids(run_id, len(findings))
    expected = [dict(f, finding_id=id_, review_mode="planning") for id_, f in zip(requested_ids, findings)]
    ids: list[str] = []
    for start in range(0, len(expected), 100):
        chunk = expected[start : start + 100]
        rows = _batch_rows(task, run_id, start)
        if rows is None:
            # There is no canonical committed-batch receipt authorizing replay.
            # Refuse pre-existing operation IDs rather than reopen a terminal row.
            if _read_findings(task, [f["finding_id"] for f in chunk]):
                raise ReviewFailure("review_identity_conflict", "finding IDs exist without this batch receipt")
            _checked(
                api.review_findings(
                    review=api.ReviewFindingsBatchRecordOp.model_validate(
                        {
                            "operation": "batch_record",
                            "task_ref": task,
                            "session": session,
                            "event_id": _batch_event(run_id, start),
                            "findings": chunk,
                            "detail": "identity",
                        }
                    )
                ),
                "review_recording_partial",
            )
            rows = _batch_rows(task, run_id, start)
        if rows is None or len(rows) != len(chunk):
            raise ReviewFailure("review_recording_partial", "incomplete canonical batch")
        for f, row in zip(chunk, rows):
            # The canonical recorder may link a prior identical follow-up. Its
            # original identity and terminal disposition remain authoritative.
            if any(row.get(k) != v for k, v in f.items() if k != "finding_id"):
                raise ReviewFailure("review_identity_conflict", "canonical finding content mismatch")
            ids.append(row["finding_id"])
    return ids


def _execute(repo: Path, directory: Path, subject: dict, packet: dict, args: Any, *, recovering: bool = False) -> dict:
    from workbay_protocol.remote_probe import resolve_remote_gate_host

    from .adapters.remote_exec import _parse_remote_gate_env_line, _source_provenance_from_json, resolve_transport

    route = packet["binding"]["model_route"]
    try:
        spec = build_agent_spec(route["backend"], model=route["model"], effort=route["effort"], speed=route["speed"])
        # Native analysis validates the complete allowlisted read-only envelope.
        spec = replace(
            spec,
            argv=analysis_argv(
                model=route["model"], effort=route["effort"], service_tier=spec.operator_values[2] or None
            ),
        )
        transport = resolve_transport(repo, recipe_argv=spec.argv)
    except Exception as exc:
        raise ReviewFailure("review_route_unavailable", str(exc)[:600]) from exc
    identity = RemoteJobIdentity(subject["task_ref"], "plan-review", subject["review_run_id"], 0)
    branch = "plan-review-" + _sha(subject["review_run_id"].encode())[:20]
    # Isolated Git delivery binds native analysis HEAD to the requested base even
    # when the caller's checkout has advanced. No author hooks or local model runs.
    delivery = directory / "source"
    if not delivery.exists():
        _git(repo, "clone", "--quiet", "--shared", "--no-checkout", "--", str(repo), str(delivery))
        _git(delivery, "checkout", "--quiet", "-b", branch, subject["source_commit"])
    if _git(delivery, "rev-parse", "HEAD") != subject["source_commit"]:
        raise ReviewFailure("review_identity_conflict", "delivery head changed")
    # Native transport reads operational config at the Git common root. The
    # source clone must keep the caller's selected gate and native settings.
    authority = Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir")).parent
    config_path = authority / ".workbay/remote-gate.env"
    config_bytes = config_path.read_bytes() if config_path.is_file() else b""
    selected_host = resolve_remote_gate_host(authority)
    if not selected_host or selected_host.startswith("-") or any(c.isspace() for c in selected_host):
        raise ReviewFailure("review_route_unavailable", "remote gate is not configured")
    file_settings = {}
    for line in config_bytes.decode("utf-8").split("\n"):
        parsed = _parse_remote_gate_env_line(line)
        if isinstance(parsed, tuple):
            file_settings[parsed[0]] = parsed[1]
    selected_directory = (
        os.environ.get("WORKBAY_REMOTE_GATE_DIR") or file_settings.get("REMOTE_GATE_DIR") or "src/" + authority.name
    )
    if config_bytes:
        delivered_config = delivery / ".workbay/remote-gate.env"
        delivered_config.parent.mkdir(parents=True, exist_ok=True)
        delivered_config.write_bytes(config_bytes)
        delivered_config.chmod(0o600)
    brief, schema, result = directory / "brief.json", directory / "schema.json", directory / "result.json"
    brief.write_text(_json(packet), encoding="utf-8")
    schema.write_text(_json(_schema(packet["binding"])), encoding="utf-8")
    spec_path, _ = write_agent_spec(spec, directory / "agent")
    payload = json.loads(spec_path.read_text())
    payload.update(branch=branch, head_sha=subject["source_commit"])
    spec_path.write_text(_json(payload))
    timeout = getattr(args, "timeout", 900)
    if not 1 <= timeout <= 1800:
        raise ReviewFailure("review_parameters_invalid", "timeout must be 1..1800 seconds")
    command = [
        str(transport.path),
        "build",
        "--branch",
        branch,
        "--brief",
        str(brief),
        "--schema",
        str(schema),
        "--out",
        str(directory / "patch"),
        "--result-out",
        str(result),
        "--debug-out",
        str(directory / "debug.log"),
        "--stream-out",
        str(directory / "stream.jsonl"),
        "--provenance-out",
        str(directory / "provenance.json"),
        "--timeout",
        str(timeout),
        "--agent-spec",
        str(spec_path),
        "--job-id",
        identity.job_id,
        "--review-kind",
        "analysis",
        "--review-subject-branch",
        branch,
    ]
    ledger = RemoteJobLedger(get_runtime_config().state_dir / "remote-jobs")
    try:
        try:
            old = ledger.read_job(identity.job_id)
        except RemoteJobLedgerError as exc:
            if str(exc) != "job_not_found":
                raise
            old = None
        if recovering and old is None:
            raise ReviewFailure(
                "review_transport_failed",
                {"reason": "remote_job_resume_hold", "detail": "retained_spec_missing", "job_id": identity.job_id},
            )
        native_spec = RemoteJobSpec(
            job_id=identity.job_id,
            task_ref=identity.task_ref,
            lane_id=identity.lane_id,
            pass_id=identity.pass_id,
            attempt=0,
            backend=route["backend"],
            branch=branch,
            lane_key=lane_key_for_branch(branch),
            argv=command,
            bound_seconds=timeout,
            created_at=old["spec"]["created_at"] if old else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            extras={
                "plan_review_binding": packet["binding"],
                "transport_digest": transport.digest,
                "transport_source": transport.source,
                "gate_host": selected_host,
                "gate_directory": selected_directory,
                "gate_config_sha256": _sha(config_bytes),
            },
        )
        ledger.record_intent(native_spec)
        env = {
            **os.environ,
            **identity.to_env(),
            "COPYFILE_DISABLE": "1",
            "COPYINGFILE_DISABLE": "1",
            "WORKBAY_REMOTE_GATE_HOST": selected_host,
            "WORKBAY_REMOTE_GATE_DIR": selected_directory,
        }
        snapshot = Path(subject["snapshot_path"])
        logical = repo / subject["subject_path"]
        if (
            snapshot.is_symlink()
            or logical.is_symlink()
            or _sha(snapshot.read_bytes()) != subject["subject_sha256"]
            or _sha(logical.read_bytes()) != subject["subject_sha256"]
        ):
            raise ReviewFailure("review_identity_conflict", "subject changed before native delivery")
        # Intent proves neither submission nor absence. An earlier caller may
        # have left this exact native job queued/running before disconnecting.
        # Full-spec ledger comparison above fences recovery; only a new intent
        # may submit. An unavailable old job holds rather than spending again.
        run = (
            remote_job_runner.collect_only
            if old and old["stage"] in {"done", "completed"}
            else remote_job_runner.resume_pending
            if old
            else remote_job_runner.run_detached
        )
        if brief.read_bytes() != _json(packet).encode("utf-8"):
            raise ReviewFailure("review_identity_conflict", "native brief changed")
        selected = {"transport_path": str(transport.path), "transport_digest": transport.digest}
        with remote_job_runner._authenticated_transport(command, selected) as authenticated:
            completed = run(authenticated, cwd=str(delivery), env=env, timeout=timeout)
        if completed.returncode != 0:
            raise ReviewFailure(
                "review_transport_failed", {"native_rc": completed.returncode, "stderr": completed.stderr[-1500:]}
            )
        ledger.mark_done(identity.job_id, 0)
        proof, error = _source_provenance_from_json(
            directory / "provenance.json",
            branch=branch,
            head_sha=subject["source_commit"],
            head_tree=subject["source_tree"],
        )
        if error or not proof or proof.get("model_process_started") is not True:
            raise ReviewFailure("review_transport_failed", {"provenance": error or "model_start_unobserved"})
        if (directory / "patch").exists() and (directory / "patch").stat().st_size:
            raise ReviewFailure("review_transport_failed", "analysis mutated source")
        _validate_analysis_completion(proof, directory / "stream.jsonl", route)
        value = _read_analysis_result(result, schema)
        _validate_result(value, packet["binding"])
        return value
    except ReviewFailure:
        raise
    except RemoteJobLedgerError as exc:
        code = "review_identity_conflict" if "conflict" in str(exc) else "review_transport_failed"
        raise ReviewFailure(code, str(exc)) from exc
    except (
        OSError,
        ValueError,
        KeyError,
        subprocess.SubprocessError,
        remote_job_runner._TransportBindingRefused,
    ) as exc:
        raise ReviewFailure("review_transport_failed", str(exc)[:800]) from exc


def _dashboard(task: str, session: str, prefix: str) -> None:
    done = prefix + "_dashboard_done"
    if _decision(task, done):
        return
    claim = prefix + "_dashboard_claim"
    if _decision(task, claim):
        # A crash between an external file effect and its acknowledgment cannot
        # establish exactly once. Never hide that ambiguity by rendering again.
        raise ReviewFailure("review_dashboard_unknown", "prior dashboard attempt unacknowledged")
    _record_decision(task, session, claim, {"attempt": 1})
    _checked(api.render_handoff(kind="dashboard", write_file=True), "review_dashboard_unknown")
    _record_decision(task, session, done, {"rendered": True})


def drive(repo: Path, subject: dict, lint: dict, args: Any) -> tuple[int, dict]:
    """Consume one authoritative DTO and return an honest machine receipt."""
    receipt = {
        "schema": "workbay/plan-review-receipt@1",
        "ok": False,
        "review_completed": False,
        "plan_accepted": False,
        "review_subject": subject,
        "anchor_lint": lint,
        "review_run_id": subject["review_run_id"],
        "task_ref": subject["task_ref"],
        "source_commit": subject["source_commit"],
        "source_tree": subject["source_tree"],
        "subject_sha256": subject["subject_sha256"],
        "cas": "not_attempted",
        "finding_ids": [],
        "independent_qualification": "required",
    }
    try:
        configure(repo)
        from plan_anchor_lint import lint_review_subject

        fresh = lint_review_subject(subject, repo=repo)
        if lint["status"] != "valid" or fresh["status"] != "valid" or fresh != lint:
            raise ReviewFailure("review_subject_unavailable", fresh)
        params = _parameters(args, subject)
        content = Path(subject["snapshot_path"]).read_bytes()
        if _sha(content) != subject["subject_sha256"]:
            raise ReviewFailure("review_identity_conflict", "snapshot changed")
        identity = RemoteJobIdentity(subject["task_ref"], "plan-review", subject["review_run_id"], 0)
        binding = {
            k: subject[k]
            for k in (
                "freeze_id",
                "task_ref",
                "review_run_id",
                "subject_path",
                "subject_sha256",
                "source_commit",
                "source_tree",
            )
        }
        binding.update(
            round=params["round"],
            prior_run_id=(params["prior"] or {}).get("review_run_id"),
            prior_sha256=(params["prior"] or {}).get("sha256"),
            scope_decision=params["scope_decision"],
            round_justification=params["round_justification"],
            model_route=params["model_route"],
            job_id=identity.job_id,
        )
        packet = {
            "binding": binding,
            "snapshot_base64": base64.b64encode(content).decode("ascii"),
            "prior_adjudication": (params["prior"] or {}).get("adjudication"),
            "instructions": "Review the exact base64-decoded UTF-8 plan, preserving its bytes. Verify SHA256 against binding.subject_sha256 before judging; code evidence is pinned to source_commit/tree. Execute the planning-review checklist: current-state accuracy, consistency, architecture and API contracts, junior-agent implementability, naming, pipeline readiness and testability. Read source anchors from this immutable checkout. Every real defect MUST be in findings[], including confirmed out-of-scope defects as follow_up with origin and impact_reason. Never drop a defect by severity or put it only in notes. Notes are non-defect observations only. If there are no defects, return both findings and notes as empty arrays. Return the strict schema, binding unchanged. Do not edit source, accept a plan, or launch another reviewer. Schema compliance does not certify semantic completeness; independent qualification remains required.",
        }
        # Digest covers the exact snapshot, prior bytes, route and protocol
        # instructions; the self-referential digest field is excluded.
        binding["packet_sha256"] = _sha(_json(packet).encode())
        receipt["packet_sha256"] = binding["packet_sha256"]
        receipt.update(binding)
        directory = get_runtime_config().state_dir / "plan-review-delivery" / _sha(subject["review_run_id"].encode())
        prefix = "planreview_" + _sha(subject["review_run_id"].encode())
        session = args.session
        with _lock(directory / "operation.lock"):
            prior_binding = _decision(subject["task_ref"], prefix + "_binding")
            if prior_binding is not None and prior_binding != binding:
                raise ReviewFailure("review_identity_conflict", "immutable parameters changed")
            row = next((r for r in _runs(subject["task_ref"]) if r["review_run_id"] == subject["review_run_id"]), None)
            if row is None:
                raise ReviewFailure("review_subject_unavailable")
            if json.loads(row.get("judge_pin_json") or "{}").get("plan_review_subject") != subject:
                raise ReviewFailure("review_identity_conflict", "DTO is not the canonical freeze")
            final = _decision(subject["task_ref"], prefix + "_verdict")
            if row["verdict"] is not None:
                if (
                    prior_binding != binding
                    or final is None
                    or row["verdict"] != final["verdict"]
                    or row["verdict_decision"] != prefix + "_verdict"
                    or row["subject_sha"] != subject["subject_sha256"]
                ):
                    raise ReviewFailure("review_completion_conflict", "completed row does not match operation")
                ids = _completed_ids(subject["task_ref"], subject["review_run_id"], final["finding_count"])
                receipt.update(review_completed=True, cas="complete", finding_ids=ids, **final)
                _dashboard(subject["task_ref"], session, prefix)
                receipt["ok"] = True
                return 0, receipt
            if prior_binding is None:
                _record_decision(subject["task_ref"], session, prefix + "_binding", binding)
            result = _execute(repo, directory, subject, packet, args, recovering=prior_binding is not None)
            findings = _validate_result(result, binding)
            result_sha = _sha(_json(result).encode())
            expected_final = {
                "verdict": result["verdict"],
                "result_sha256": result_sha,
                "finding_count": len(findings),
                "decision": prefix + "_verdict",
            }
            if final is not None and final != expected_final:
                raise ReviewFailure("review_identity_conflict", "collected result changed")
            manifest_name = prefix + "_result"
            manifest = _decision(subject["task_ref"], manifest_name)
            if manifest is not None and manifest != expected_final:
                raise ReviewFailure("review_identity_conflict", "remote result differs from retained manifest")
            if manifest is None:
                _record_decision(subject["task_ref"], session, manifest_name, expected_final)
            try:
                ids = _record_findings(subject["task_ref"], session, subject["review_run_id"], findings)
            except ReviewFailure as exc:
                receipt["finding_ids"] = []
                for start in range(0, len(findings), 100):
                    durable = _batch_rows(subject["task_ref"], subject["review_run_id"], start)
                    if durable:
                        receipt["finding_ids"].extend(row["finding_id"] for row in durable)
                raise exc
            receipt["finding_ids"] = ids
            _record_decision(subject["task_ref"], session, prefix + "_verdict", expected_final)
            receipt["cas"] = "attempted"
            completed = api.review_runs(
                review=api.ReviewRunsCompleteOp.model_validate(
                    {
                        "operation": "complete",
                        "review_run_id": subject["review_run_id"],
                        "session": session,
                        "expected_subject_sha256": subject["subject_sha256"],
                        "verdict": result["verdict"],
                        "verdict_decision": prefix + "_verdict",
                    }
                )
            )
            _checked(completed, "review_completion_conflict")
            receipt.update(review_completed=True, cas="complete", **expected_final)
            RemoteJobLedger(get_runtime_config().state_dir / "remote-jobs").mark_completed(identity.job_id)
            _dashboard(subject["task_ref"], session, prefix)
            receipt["ok"] = True
            return 0, receipt
    except ReviewFailure as exc:
        receipt.update(error=exc.code, detail=exc.detail)
        return FAILURES[exc.code], receipt
    except Exception as exc:
        receipt.update(error="review_recording_partial", detail=f"{type(exc).__name__}: {exc}"[:800])
        return FAILURES["review_recording_partial"], receipt
