"""Host-side ``workbay handoff ingest`` implementation.

Reads a remote VM worker's ``OUTPUT/handoff.json`` (plus optional sibling or
explicit ``receipt.json`` provenance), validates the input schema before any
database access, and writes structured decisions, events, and review findings
into the host ``.task-state/handoff.db`` exclusively through existing
:mod:`workbay_handoff_mcp.api` write functions (single-writer authority, zero
raw SQL).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from . import api

INGEST_SOURCE = "workbay handoff ingest"

_VALID_EVENT_KINDS = frozenset({"decision", "test_result", "blocker", "error"})
_VALID_BLOCKER_OPS = frozenset({"add", "resolve", "reopen"})
_VALID_SEVERITIES = frozenset({"high", "medium", "low"})
_VALID_REVIEW_MODES = frozenset({"branch", "release_audit", "planning"})
_VALID_IMPACTS = frozenset({"blocking", "follow_up"})
_VALID_ORIGINS = frozenset({"introduced", "listed_open", "pre_existing"})
_ACTOR_KEYS = frozenset({"agent", "model", "model_label", "reasoning_level", "branch", "commit_sha", "lane_id"})


class IngestUsageError(ValueError):
    """Raised when ingest CLI arguments or input files fail validation."""


@dataclass(frozen=True)
class PlannedWrite:
    """One validated write operation ready for dry-run reporting or API dispatch."""

    kind: str  # "decision" | "test_result" | "blocker" | "error" | "finding"
    index: int
    key: str
    task_ref: str | None
    session: str | None
    event_id: str | None
    api_target: str  # "record_event" | "review_findings"
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class IngestPlan:
    """Validated ingest plan produced without touching the handoff database."""

    from_path: str
    receipt_path: str | None
    source_sha256: str
    receipt_sha256: str | None
    task_ref: str | None
    provenance: Mapping[str, Any]
    writes: tuple[PlannedWrite, ...]


def _usage_envelope(message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "source": INGEST_SOURCE,
        "error": {
            "kind": "usage",
            "message": message,
        },
    }


def _non_empty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IngestUsageError(f"{field} must be a non-empty string")
    return value.strip()


def _optional_non_empty_str(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise IngestUsageError(f"{field} must be a string when provided")
    stripped = value.strip()
    return stripped or None


def _optional_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise IngestUsageError(f"{field} must be an integer when provided")
    if isinstance(value, int):
        return value
    raise IngestUsageError(f"{field} must be an integer when provided")


def _optional_number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _optional_str_list(value: Any, *, field: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise IngestUsageError(f"{field} must be a list of strings when provided")
    return list(value)


def _validate_actor_override(value: Any, *, field: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise IngestUsageError(f"{field} must be a JSON object when provided")
    result: dict[str, str] = {}
    for key, raw in value.items():
        if key not in _ACTOR_KEYS:
            raise IngestUsageError(f"{field}.{key} is not a supported actor field")
        if raw is None:
            continue
        if not isinstance(raw, str):
            raise IngestUsageError(f"{field}.{key} must be a string")
        stripped = raw.strip()
        if stripped:
            result[key] = stripped
    return result


def _read_json_object(path: Path, *, label: str) -> tuple[dict[str, Any], str]:
    if not path.exists():
        raise IngestUsageError(f"{label} file not found: {path}")
    if not path.is_file():
        raise IngestUsageError(f"{label} path is not a regular file: {path}")
    try:
        raw_bytes = path.read_bytes()
    except OSError as exc:
        raise IngestUsageError(f"Unable to read {label} file {path}: {exc}") from exc
    try:
        parsed = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestUsageError(f"Malformed JSON in {label} file {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise IngestUsageError(f"{label} root must be a JSON object: {path}")
    return parsed, hashlib.sha256(raw_bytes).hexdigest()


def _extract_provenance(
    *,
    handoff: Mapping[str, Any],
    receipt: Mapping[str, Any] | None,
    from_path: Path,
    receipt_path: Path | None,
    source_sha256: str,
    receipt_sha256: str | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    r = receipt or {}

    def _first_str(*candidates: Any) -> str | None:
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    backend = _first_str(r.get("backend"), handoff.get("backend"))
    model = _first_str(
        r.get("model"),
        r.get("observed_served_model"),
        r.get("requested_model"),
        handoff.get("model"),
    )
    effort = _first_str(r.get("effort"), handoff.get("effort"))
    lane_id = _first_str(r.get("lane_id"), handoff.get("lane_id"))
    candidate_commit = _first_str(r.get("candidate_commit"), handoff.get("candidate_commit"))
    branch = _first_str(r.get("branch"), handoff.get("branch"))
    conversation_id = _first_str(r.get("conversation_id"), handoff.get("conversation_id"))

    elapsed_seconds: int | float | None = None
    for candidate in (
        r.get("elapsed_seconds"),
        r.get("seconds"),
        r.get("duration_seconds"),
        handoff.get("elapsed_seconds"),
    ):
        num = _optional_number(candidate)
        if num is not None:
            elapsed_seconds = num
            break

    base_actor: dict[str, str] = {}
    if model is not None:
        base_actor["model"] = model
    elif backend is not None:
        base_actor["agent"] = backend
    if effort is not None:
        base_actor["reasoning_level"] = effort
    if lane_id is not None:
        base_actor["lane_id"] = lane_id
    if candidate_commit is not None:
        base_actor["commit_sha"] = candidate_commit
    if branch is not None:
        base_actor["branch"] = branch

    top_actor = _validate_actor_override(handoff.get("actor"), field="handoff.actor")
    merged_base_actor = {**base_actor, **top_actor}

    provenance: dict[str, Any] = {
        "from_path": str(from_path.resolve()),
        "receipt_path": str(receipt_path.resolve()) if receipt_path is not None else None,
        "source_sha256": source_sha256,
        "receipt_sha256": receipt_sha256,
        "backend": backend,
        "model": model,
        "effort": effort,
        "lane_id": lane_id,
        "candidate_commit": candidate_commit,
        "branch": branch,
        "elapsed_seconds": elapsed_seconds,
        "conversation_id": conversation_id,
        "actor": merged_base_actor,
    }
    return provenance, merged_base_actor


def _derive_event_id(kind: str, fingerprint_fields: Mapping[str, Any]) -> str:
    canonical = json.dumps(fingerprint_fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"wb.ingest.{kind}.{digest}"


def _resolve_session(
    item_session: str | None,
    *,
    top_session: str | None,
    provenance: Mapping[str, Any],
    source_sha256: str,
) -> str:
    if item_session:
        return item_session
    if top_session:
        return top_session
    conv = provenance.get("conversation_id")
    if isinstance(conv, str) and conv.strip():
        return conv.strip()
    lane_id = provenance.get("lane_id")
    if isinstance(lane_id, str) and lane_id.strip():
        return f"remote:{lane_id.strip()}"
    return f"remote-ingest:{source_sha256[:12]}"


def _build_decision_write(
    item: Mapping[str, Any],
    *,
    field_prefix: str,
    index: int,
    default_task_ref: str | None,
    top_session: str | None,
    base_actor: Mapping[str, str],
    provenance: Mapping[str, Any],
    source_sha256: str,
) -> PlannedWrite:
    decision_id = _non_empty_str(item.get("decision"), field=f"{field_prefix}.decision")
    rationale = _optional_non_empty_str(item.get("rationale"), field=f"{field_prefix}.rationale")
    item_task_ref = _optional_non_empty_str(item.get("task_ref"), field=f"{field_prefix}.task_ref") or default_task_ref
    session = _resolve_session(
        _optional_non_empty_str(item.get("session"), field=f"{field_prefix}.session"),
        top_session=top_session,
        provenance=provenance,
        source_sha256=source_sha256,
    )
    item_actor = {**base_actor, **_validate_actor_override(item.get("actor"), field=f"{field_prefix}.actor")}
    changed_files = _optional_str_list(item.get("changed_files"), field=f"{field_prefix}.changed_files")
    input_tokens = _optional_int(item.get("input_tokens"), field=f"{field_prefix}.input_tokens")
    output_tokens = _optional_int(item.get("output_tokens"), field=f"{field_prefix}.output_tokens")
    total_tokens = _optional_int(item.get("total_tokens"), field=f"{field_prefix}.total_tokens")

    explicit_event_id = _optional_non_empty_str(item.get("event_id"), field=f"{field_prefix}.event_id")
    event_id = explicit_event_id or _derive_event_id(
        "decision",
        {
            "task_ref": item_task_ref,
            "session": session,
            "decision": decision_id,
            "lane_id": item_actor.get("lane_id"),
            "commit_sha": item_actor.get("commit_sha"),
        },
    )

    payload: dict[str, Any] = {
        "event_kind": "decision",
        "event_id": event_id,
        "session": session,
        "decision": decision_id,
    }
    if item_task_ref is not None:
        payload["task_ref"] = item_task_ref
    if rationale is not None:
        payload["rationale"] = rationale
    if item_actor:
        payload["actor"] = item_actor
    if changed_files is not None:
        payload["changed_files"] = changed_files
    if input_tokens is not None:
        payload["input_tokens"] = input_tokens
    if output_tokens is not None:
        payload["output_tokens"] = output_tokens
    if total_tokens is not None:
        payload["total_tokens"] = total_tokens

    return PlannedWrite(
        kind="decision",
        index=index,
        key=decision_id,
        task_ref=item_task_ref,
        session=session,
        event_id=event_id,
        api_target="record_event",
        payload=payload,
    )


def _build_event_write(
    item: Mapping[str, Any],
    *,
    index: int,
    default_task_ref: str | None,
    top_session: str | None,
    base_actor: Mapping[str, str],
    provenance: Mapping[str, Any],
    source_sha256: str,
) -> PlannedWrite:
    field_prefix = f"events[{index}]"
    event_kind = _non_empty_str(item.get("event_kind"), field=f"{field_prefix}.event_kind")
    if event_kind not in _VALID_EVENT_KINDS:
        allowed = ", ".join(sorted(_VALID_EVENT_KINDS))
        raise IngestUsageError(f"{field_prefix}.event_kind must be one of {allowed}; got {event_kind!r}")

    if event_kind == "decision":
        return _build_decision_write(
            item,
            field_prefix=field_prefix,
            index=index,
            default_task_ref=default_task_ref,
            top_session=top_session,
            base_actor=base_actor,
            provenance=provenance,
            source_sha256=source_sha256,
        )

    item_task_ref = _optional_non_empty_str(item.get("task_ref"), field=f"{field_prefix}.task_ref") or default_task_ref
    item_actor = {**base_actor, **_validate_actor_override(item.get("actor"), field=f"{field_prefix}.actor")}

    if event_kind == "test_result":
        command = _non_empty_str(item.get("command"), field=f"{field_prefix}.command")
        passed = item.get("passed")
        if not isinstance(passed, bool):
            raise IngestUsageError(f"{field_prefix}.passed must be a boolean")
        session = _resolve_session(
            _optional_non_empty_str(item.get("session"), field=f"{field_prefix}.session"),
            top_session=top_session,
            provenance=provenance,
            source_sha256=source_sha256,
        )
        result_text = _optional_non_empty_str(item.get("result"), field=f"{field_prefix}.result")
        traces = _optional_str_list(item.get("traces"), field=f"{field_prefix}.traces")
        exit_code = _optional_int(item.get("exit_code"), field=f"{field_prefix}.exit_code")
        explicit_event_id = _optional_non_empty_str(item.get("event_id"), field=f"{field_prefix}.event_id")
        event_id = explicit_event_id or _derive_event_id(
            "test_result",
            {
                "task_ref": item_task_ref,
                "session": session,
                "command": command,
                "passed": passed,
                "exit_code": exit_code,
                "lane_id": item_actor.get("lane_id"),
                "commit_sha": item_actor.get("commit_sha"),
            },
        )
        payload: dict[str, Any] = {
            "event_kind": "test_result",
            "event_id": event_id,
            "session": session,
            "command": command,
            "passed": passed,
        }
        if item_task_ref is not None:
            payload["task_ref"] = item_task_ref
        if result_text is not None:
            payload["result"] = result_text
        if traces is not None:
            payload["traces"] = traces
        if exit_code is not None:
            payload["exit_code"] = exit_code
        if item_actor:
            payload["actor"] = item_actor
        return PlannedWrite(
            kind="test_result",
            index=index,
            key=command,
            task_ref=item_task_ref,
            session=session,
            event_id=event_id,
            api_target="record_event",
            payload=payload,
        )

    if event_kind == "blocker":
        operation = _non_empty_str(item.get("operation"), field=f"{field_prefix}.operation")
        if operation not in _VALID_BLOCKER_OPS:
            allowed = ", ".join(sorted(_VALID_BLOCKER_OPS))
            raise IngestUsageError(f"{field_prefix}.operation must be one of {allowed}; got {operation!r}")
        description = _optional_non_empty_str(item.get("description"), field=f"{field_prefix}.description")
        blocker_id = _optional_int(item.get("blocker_id"), field=f"{field_prefix}.blocker_id")
        lane_id = _optional_non_empty_str(item.get("lane_id"), field=f"{field_prefix}.lane_id") or item_actor.get(
            "lane_id"
        )
        if operation == "add" and description is None:
            raise IngestUsageError(f"{field_prefix}.description is required for blocker operation 'add'")
        if operation in {"resolve", "reopen"} and blocker_id is None:
            raise IngestUsageError(f"{field_prefix}.blocker_id is required for blocker operation {operation!r}")
        payload = {
            "event_kind": "blocker",
            "operation": operation,
        }
        if item_task_ref is not None:
            payload["task_ref"] = item_task_ref
        if description is not None:
            payload["description"] = description
        if blocker_id is not None:
            payload["blocker_id"] = blocker_id
        if lane_id is not None:
            payload["lane_id"] = lane_id
        if item_actor:
            payload["actor"] = item_actor
        return PlannedWrite(
            kind="blocker",
            index=index,
            key=description or f"blocker:{blocker_id}",
            task_ref=item_task_ref,
            session=None,
            event_id=None,
            api_target="record_event",
            payload=payload,
        )

    # event_kind == "error"
    error_class = _non_empty_str(item.get("error_class"), field=f"{field_prefix}.error_class")
    summary = _non_empty_str(item.get("summary"), field=f"{field_prefix}.summary")
    payload = {
        "event_kind": "error",
        "error_class": error_class,
        "summary": summary,
    }
    if item_task_ref is not None:
        payload["task_ref"] = item_task_ref
    for opt_field in ("detail", "tool_name", "command_preview", "package_name", "package_version"):
        opt_val = _optional_non_empty_str(item.get(opt_field), field=f"{field_prefix}.{opt_field}")
        if opt_val is not None:
            payload[opt_field] = opt_val
    if item_actor:
        payload["actor"] = item_actor
    return PlannedWrite(
        kind="error",
        index=index,
        key=f"{error_class}:{summary[:48]}",
        task_ref=item_task_ref,
        session=None,
        event_id=None,
        api_target="record_event",
        payload=payload,
    )


def _build_finding_write(
    item: Mapping[str, Any],
    *,
    index: int,
    default_task_ref: str | None,
    top_session: str | None,
    base_actor: Mapping[str, str],
    provenance: Mapping[str, Any],
    source_sha256: str,
) -> PlannedWrite:
    field_prefix = f"findings[{index}]"
    finding_id = _non_empty_str(item.get("finding_id"), field=f"{field_prefix}.finding_id")
    severity = _non_empty_str(item.get("severity"), field=f"{field_prefix}.severity")
    if severity not in _VALID_SEVERITIES:
        allowed = ", ".join(sorted(_VALID_SEVERITIES))
        raise IngestUsageError(f"{field_prefix}.severity must be one of {allowed}; got {severity!r}")
    file_path = _non_empty_str(item.get("file_path"), field=f"{field_prefix}.file_path")
    description = _non_empty_str(item.get("description"), field=f"{field_prefix}.description")

    item_task_ref = _optional_non_empty_str(item.get("task_ref"), field=f"{field_prefix}.task_ref") or default_task_ref
    session = _resolve_session(
        _optional_non_empty_str(item.get("session"), field=f"{field_prefix}.session"),
        top_session=top_session,
        provenance=provenance,
        source_sha256=source_sha256,
    )
    item_actor = {**base_actor, **_validate_actor_override(item.get("actor"), field=f"{field_prefix}.actor")}

    review_mode = _optional_non_empty_str(item.get("review_mode"), field=f"{field_prefix}.review_mode")
    if review_mode is not None and review_mode not in _VALID_REVIEW_MODES:
        allowed = ", ".join(sorted(_VALID_REVIEW_MODES))
        raise IngestUsageError(f"{field_prefix}.review_mode must be one of {allowed}; got {review_mode!r}")

    impact = _optional_non_empty_str(item.get("impact"), field=f"{field_prefix}.impact")
    if impact is not None and impact not in _VALID_IMPACTS:
        allowed = ", ".join(sorted(_VALID_IMPACTS))
        raise IngestUsageError(f"{field_prefix}.impact must be one of {allowed}; got {impact!r}")

    origin = _optional_non_empty_str(item.get("origin"), field=f"{field_prefix}.origin")
    if origin is not None and origin not in _VALID_ORIGINS:
        allowed = ", ".join(sorted(_VALID_ORIGINS))
        raise IngestUsageError(f"{field_prefix}.origin must be one of {allowed}; got {origin!r}")

    failure_scenario = _optional_non_empty_str(item.get("failure_scenario"), field=f"{field_prefix}.failure_scenario")
    impact_reason = _optional_non_empty_str(item.get("impact_reason"), field=f"{field_prefix}.impact_reason")

    details_raw = item.get("details")
    details: dict[str, Any] = {}
    if details_raw is not None:
        if not isinstance(details_raw, Mapping):
            raise IngestUsageError(f"{field_prefix}.details must be a JSON object when provided")
        line_start = _optional_int(details_raw.get("line_start"), field=f"{field_prefix}.details.line_start")
        line_end = _optional_int(details_raw.get("line_end"), field=f"{field_prefix}.details.line_end")
        fix = _optional_non_empty_str(details_raw.get("fix"), field=f"{field_prefix}.details.fix")
    else:
        line_start = _optional_int(item.get("line_start"), field=f"{field_prefix}.line_start")
        line_end = _optional_int(item.get("line_end"), field=f"{field_prefix}.line_end")
        fix = _optional_non_empty_str(item.get("fix"), field=f"{field_prefix}.fix")
    if line_start is not None:
        details["line_start"] = line_start
    if line_end is not None:
        details["line_end"] = line_end
    if fix is not None:
        details["fix"] = fix

    finding_item: dict[str, Any] = {
        "finding_id": finding_id,
        "severity": severity,
        "file_path": file_path,
        "description": description,
    }
    if review_mode is not None:
        finding_item["review_mode"] = review_mode
    if impact is not None:
        finding_item["impact"] = impact
    if origin is not None:
        finding_item["origin"] = origin
    if failure_scenario is not None:
        finding_item["failure_scenario"] = failure_scenario
    if impact_reason is not None:
        finding_item["impact_reason"] = impact_reason
    if details:
        finding_item["details"] = details

    explicit_event_id = _optional_non_empty_str(item.get("event_id"), field=f"{field_prefix}.event_id")
    event_id = explicit_event_id or _derive_event_id(
        "finding",
        {
            "task_ref": item_task_ref,
            "session": session,
            "finding_id": finding_id,
            "file_path": file_path,
            "lane_id": item_actor.get("lane_id"),
            "commit_sha": item_actor.get("commit_sha"),
        },
    )

    payload: dict[str, Any] = {
        "operation": "batch_record",
        "session": session,
        "event_id": event_id,
        "findings": [finding_item],
    }
    if item_task_ref is not None:
        payload["task_ref"] = item_task_ref
    if item_actor:
        payload["actor"] = item_actor

    return PlannedWrite(
        kind="finding",
        index=index,
        key=finding_id,
        task_ref=item_task_ref,
        session=session,
        event_id=event_id,
        api_target="review_findings",
        payload=payload,
    )


def build_ingest_plan(
    *,
    from_path: str | Path,
    receipt_path: str | Path | None = None,
    task_ref: str | None = None,
) -> IngestPlan:
    """Read and validate ``handoff.json`` + optional ``receipt.json`` with zero DB access."""
    if not str(from_path).strip():
        raise IngestUsageError("--from path must not be empty")
    source_file = Path(from_path)
    handoff, source_sha256 = _read_json_object(source_file, label="handoff")

    if task_ref is not None and not task_ref.strip():
        raise IngestUsageError("--task-ref must not be blank when provided")

    resolved_receipt_path: Path | None = None
    receipt: dict[str, Any] | None = None
    receipt_sha256: str | None = None
    if receipt_path is not None:
        if not str(receipt_path).strip():
            raise IngestUsageError("--receipt path must not be empty when provided")
        resolved_receipt_path = Path(receipt_path)
        receipt, receipt_sha256 = _read_json_object(resolved_receipt_path, label="receipt")
    else:
        sibling = source_file.parent / "receipt.json"
        if sibling.exists():
            resolved_receipt_path = sibling
            receipt, receipt_sha256 = _read_json_object(sibling, label="receipt")

    if "schema" in handoff and handoff["schema"] is not None:
        _non_empty_str(handoff["schema"], field="handoff.schema")

    resolved_task_ref = (
        (task_ref.strip() if task_ref is not None else None)
        or _optional_non_empty_str(handoff.get("task_ref"), field="handoff.task_ref")
        or _optional_non_empty_str((receipt or {}).get("task_ref"), field="receipt.task_ref")
    )
    top_session = _optional_non_empty_str(handoff.get("session"), field="handoff.session")

    provenance, base_actor = _extract_provenance(
        handoff=handoff,
        receipt=receipt,
        from_path=source_file,
        receipt_path=resolved_receipt_path,
        source_sha256=source_sha256,
        receipt_sha256=receipt_sha256,
    )

    present_sections = [key for key in ("decisions", "events", "findings") if key in handoff]
    if not present_sections:
        raise IngestUsageError(
            "handoff.json must include at least one of 'decisions', 'events', or 'findings'"
        )

    writes: list[PlannedWrite] = []

    if "decisions" in handoff:
        raw_decisions = handoff["decisions"]
        if not isinstance(raw_decisions, list):
            raise IngestUsageError("handoff.decisions must be a list")
        for idx, item in enumerate(raw_decisions):
            if not isinstance(item, Mapping):
                raise IngestUsageError(f"decisions[{idx}] must be a JSON object")
            writes.append(
                _build_decision_write(
                    item,
                    field_prefix=f"decisions[{idx}]",
                    index=idx,
                    default_task_ref=resolved_task_ref,
                    top_session=top_session,
                    base_actor=base_actor,
                    provenance=provenance,
                    source_sha256=source_sha256,
                )
            )

    if "events" in handoff:
        raw_events = handoff["events"]
        if not isinstance(raw_events, list):
            raise IngestUsageError("handoff.events must be a list")
        for idx, item in enumerate(raw_events):
            if not isinstance(item, Mapping):
                raise IngestUsageError(f"events[{idx}] must be a JSON object")
            writes.append(
                _build_event_write(
                    item,
                    index=idx,
                    default_task_ref=resolved_task_ref,
                    top_session=top_session,
                    base_actor=base_actor,
                    provenance=provenance,
                    source_sha256=source_sha256,
                )
            )

    if "findings" in handoff:
        raw_findings = handoff["findings"]
        if not isinstance(raw_findings, list):
            raise IngestUsageError("handoff.findings must be a list")
        for idx, item in enumerate(raw_findings):
            if not isinstance(item, Mapping):
                raise IngestUsageError(f"findings[{idx}] must be a JSON object")
            writes.append(
                _build_finding_write(
                    item,
                    index=idx,
                    default_task_ref=resolved_task_ref,
                    top_session=top_session,
                    base_actor=base_actor,
                    provenance=provenance,
                    source_sha256=source_sha256,
                )
            )

    if not writes:
        raise IngestUsageError(
            "handoff.json contains no ingestible items across 'decisions', 'events', and 'findings'"
        )

    return IngestPlan(
        from_path=str(source_file.resolve()),
        receipt_path=str(resolved_receipt_path.resolve()) if resolved_receipt_path is not None else None,
        source_sha256=source_sha256,
        receipt_sha256=receipt_sha256,
        task_ref=resolved_task_ref,
        provenance=provenance,
        writes=tuple(writes),
    )


def _count_by_kind(writes: tuple[PlannedWrite, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for w in writes:
        counts[w.kind] = counts.get(w.kind, 0) + 1
    return counts


def _is_idempotent_result(response: Mapping[str, Any]) -> bool:
    data = response.get("data")
    if isinstance(data, Mapping) and data.get("idempotent") is True:
        return True
    mutation = response.get("mutation")
    if isinstance(mutation, Mapping) and mutation.get("operation") == "noop":
        return True
    return False


def execute_ingest_plan(plan: IngestPlan, *, dry_run: bool = False) -> dict[str, Any]:
    """Execute a validated :class:`IngestPlan` via ``api.py`` or return a dry-run summary."""
    by_kind = _count_by_kind(plan.writes)

    if dry_run:
        planned = [
            {
                "kind": w.kind,
                "index": w.index,
                "key": w.key,
                "task_ref": w.task_ref,
                "session": w.session,
                "event_id": w.event_id,
                "api_target": w.api_target,
            }
            for w in plan.writes
        ]
        return {
            "ok": True,
            "tool": "handoff_ingest",
            "source": INGEST_SOURCE,
            "task_ref": plan.task_ref,
            "data": {
                "dry_run": True,
                "from_path": plan.from_path,
                "receipt_path": plan.receipt_path,
                "counts": {
                    "total": len(plan.writes),
                    "planned": len(plan.writes),
                    "written": 0,
                    "idempotent": 0,
                    "failed": 0,
                    "by_kind": by_kind,
                },
                "provenance": dict(plan.provenance),
                "planned": planned,
            },
        }

    results: list[dict[str, Any]] = []
    written = 0
    idempotent = 0
    failed = 0
    first_error_kind: str | None = None
    first_error_message: str | None = None

    for w in plan.writes:
        if w.api_target == "record_event":
            response = api.record_event(event=cast(api.RecordEventParam, dict(w.payload)))
        else:
            response = api.review_findings(review=cast(api.ReviewFindingsParam, dict(w.payload)))

        ok = isinstance(response, Mapping) and response.get("ok") is True
        if ok:
            was_idempotent = _is_idempotent_result(response)
            if was_idempotent:
                idempotent += 1
            else:
                written += 1
            entry: dict[str, Any] = {
                "kind": w.kind,
                "index": w.index,
                "key": w.key,
                "task_ref": response.get("task_ref") or w.task_ref,
                "event_id": w.event_id,
                "ok": True,
                "idempotent": was_idempotent,
                "action": "noop" if was_idempotent else "written",
            }
            warnings = response.get("warnings")
            if warnings:
                entry["warnings"] = warnings
            results.append(entry)
        else:
            failed += 1
            err_msg: str | None = None
            err_kind: str | None = None
            if isinstance(response, Mapping):
                raw_err = response.get("error")
                if isinstance(raw_err, Mapping):
                    err_kind = str(raw_err.get("kind")) if raw_err.get("kind") else None
                    err_msg = str(raw_err.get("message") or raw_err)
                elif raw_err is not None:
                    err_msg = str(raw_err)
                data = response.get("data")
                if isinstance(data, Mapping):
                    if err_kind is None and data.get("error_kind"):
                        err_kind = str(data["error_kind"])
                    if err_msg is None and data.get("error"):
                        err_msg = str(data["error"])
            if first_error_kind is None and err_kind is not None:
                first_error_kind = err_kind
            if first_error_message is None:
                first_error_message = err_msg or f"Failed to ingest {w.kind} {w.key!r}"
            results.append(
                {
                    "kind": w.kind,
                    "index": w.index,
                    "key": w.key,
                    "task_ref": w.task_ref,
                    "event_id": w.event_id,
                    "ok": False,
                    "idempotent": False,
                    "action": "failed",
                    "error": err_msg or "write refused",
                    **({"error_kind": err_kind} if err_kind else {}),
                }
            )

    summary_data: dict[str, Any] = {
        "dry_run": False,
        "from_path": plan.from_path,
        "receipt_path": plan.receipt_path,
        "counts": {
            "total": len(plan.writes),
            "written": written,
            "idempotent": idempotent,
            "failed": failed,
            "by_kind": by_kind,
        },
        "provenance": dict(plan.provenance),
        "results": results,
    }

    if failed > 0:
        return {
            "ok": False,
            "tool": "handoff_ingest",
            "source": INGEST_SOURCE,
            "task_ref": plan.task_ref,
            "error": {
                "kind": first_error_kind or "ingest_failed",
                "message": first_error_message or f"{failed} of {len(plan.writes)} ingest writes failed",
            },
            "data": summary_data,
        }

    return {
        "ok": True,
        "tool": "handoff_ingest",
        "source": INGEST_SOURCE,
        "task_ref": plan.task_ref,
        "data": summary_data,
    }


def dispatch_ingest(args: argparse.Namespace) -> Mapping[str, Any]:
    """Entrypoint called by ``noun_cli.run_noun('handoff', ['ingest', ...])``."""
    try:
        plan = build_ingest_plan(
            from_path=args.from_path,
            receipt_path=getattr(args, "receipt_path", None),
            task_ref=getattr(args, "task_ref", None),
        )
    except IngestUsageError as exc:
        return _usage_envelope(str(exc))

    return execute_ingest_plan(plan, dry_run=bool(getattr(args, "dry_run", False)))


def register_ingest_subparser(
    subparsers: Any,
    *,
    add_runtime_flags: Callable[..., None],
    verb_spec: Any,
) -> argparse.ArgumentParser:
    """Register the ``ingest`` subparser under ``workbay handoff``."""
    sub: argparse.ArgumentParser = subparsers.add_parser(
        "ingest",
        help=(verb_spec.help or "Ingest remote worker handoff.json into host handoff DB.").replace("%", "%%"),
    )
    sub.add_argument(
        "--from",
        dest="from_path",
        required=True,
        help="Path to remote worker OUTPUT/handoff.json.",
    )
    sub.add_argument(
        "--receipt",
        dest="receipt_path",
        default=None,
        help="Optional path to receipt.json for actor provenance (defaults to sibling receipt.json if present).",
    )
    sub.add_argument(
        "--task-ref",
        dest="task_ref",
        default=None,
        help="Optional task reference override (overrides task_ref in handoff.json and receipt.json).",
    )
    sub.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Validate inputs and emit planned ingest summary with exit 0 and zero DB writes.",
    )
    add_runtime_flags(sub, suppress_defaults=True)
    sub.set_defaults(
        _verb_spec=verb_spec,
        _custom_dispatch=dispatch_ingest,
    )
    return sub


__all__ = [
    "INGEST_SOURCE",
    "IngestPlan",
    "IngestUsageError",
    "PlannedWrite",
    "build_ingest_plan",
    "dispatch_ingest",
    "execute_ingest_plan",
    "register_ingest_subparser",
]
