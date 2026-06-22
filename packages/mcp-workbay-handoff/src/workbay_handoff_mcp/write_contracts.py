"""Generalized write-contract registry (internal).

The registry is keyed exclusively on registered MCP tool names — the
namespace ``limits.write`` actually publishes. Python-API helpers
(``record_decision``, ``record_test_result``, ``report_blocker``,
``record_review_run``, ``update_review_finding``) are derived from
registry rows; their grammars surface as ``variants{}`` under the
owning typed-domain tool.

The registry is the single source of truth for required fields and
field grammars. ``validate_write`` is side-effect-free and used by
PreToolUse hooks before forwarding to the real MCP tool.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WriteContract:
    tool_name: str
    required: list[str] = field(default_factory=list)
    optional: list[str] = field(default_factory=list)
    field_grammars: dict[str, str] = field(default_factory=dict)
    variants: dict[str, "WriteContract"] = field(default_factory=dict)
    examples: list[dict[str, Any]] = field(default_factory=list)
    error_codes: list[str] = field(default_factory=list)
    selector_field: str | None = None


_DECISION_VARIANT = WriteContract(
    tool_name="record_event.event_kind=decision",
    required=["decision", "rationale"],
    optional=[
        "session",
        "actor",
        "task_ref",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "changed_files",
        "plan_revision",
    ],
    field_grammars={
        "decision": r"^[A-Za-z][A-Za-z0-9_-]*$",
        # Plan-revision basenames follow the bootstrap rule from Plan
        # 0010 implementation note: every revision after implementation note lands as a separate
        # ``<plan-stem>-rN.md`` file. The grammar pins the suffix shape
        # so malformed values (e.g. raw paths or pre-Slice-7 in-place
        # revision-history strings) are rejected before write.
        "plan_revision": r"^[a-z0-9][a-z0-9-]*-r[0-9]+\.md$",
    },
)

_TEST_RESULT_VARIANT = WriteContract(
    tool_name="record_event.event_kind=test_result",
    required=["command", "passed"],
    optional=["session", "result", "traces", "exit_code", "actor", "task_ref"],
    field_grammars={
        "command": r"^.+$",
    },
)

_BLOCKER_VARIANT = WriteContract(
    tool_name="record_event.event_kind=blocker",
    required=["operation", "description"],
    optional=["session", "blocker_id", "actor", "task_ref"],
    field_grammars={
        "operation": r"^(open|update|resolve)$",
    },
)

_ERROR_VARIANT = WriteContract(
    tool_name="record_event.event_kind=error",
    required=["error_class", "summary"],
    optional=[
        "detail",
        "tool_name",
        "command_preview",
        "package_name",
        "package_version",
        # actor is accepted for record_event envelope uniformity but not
        # persisted — agent_errors has no actor columns (internal
        # review finding REV-A-003).
        "actor",
        "task_ref",
    ],
    field_grammars={
        # Append-only lowercase taxonomy (implementation note): install_drift,
        # mcp_write_rejected, mcp_unreachable, cli_failure, env_misconfig,
        # other, plus future classes matching the same shape.
        "error_class": r"^[a-z][a-z0-9_]*$",
        "summary": r"^.+$",
    },
)


# Schemas below mirror workbay_handoff_mcp.api.ReviewRuns*Op and
# review_findings_api.ReviewFindings*Op. BR-05 (implementation note) pins the
# registry to the real Pydantic shapes — drift here re-introduces
# silent registry-vs-API contradictions.
_REVIEW_RUNS_RECORD_VARIANT = WriteContract(
    tool_name="review_runs.operation=record",
    required=["review_run_id", "session", "subject_path"],
    optional=["subject_kind", "review_mode", "verdict", "verdict_decision", "task_ref", "actor"],
    field_grammars={
        "verdict": r"^(pass|pass_with_findings|conditional_pass|fail)$",
        "review_mode": r"^(branch|release_audit|planning)$",
        "subject_kind": r"^(task_plan|epic|branch|adr|roadmap|other)$",
    },
)
_REVIEW_RUNS_LIST_VARIANT = WriteContract(
    tool_name="review_runs.operation=list",
    required=[],
    optional=["task_ref", "subject_path", "limit", "offset", "review_mode", "verdict"],
    field_grammars={
        "review_mode": r"^(branch|release_audit|planning)$",
        "verdict": r"^(pass|pass_with_findings|conditional_pass|fail)$",
    },
)
_REVIEW_RUNS_COVERAGE_VARIANT = WriteContract(
    tool_name="review_runs.operation=coverage",
    required=[],
    optional=["task_ref", "subject_path"],
)

_TERMINAL_GUARD_TELEMETRY_RECORD_VARIANT = WriteContract(
    tool_name="terminal_guard_telemetry.operation=record",
    required=[
        "harness",
        "tool_name",
        "decision",
        "trigger",
        "command_preview",
        "policy_version",
        "policy_source",
    ],
    optional=[
        "task_ref",
        "worktree_path",
        "native_tool_hint",
        "fallback_source",
        "created_at",
    ],
    field_grammars={
        "decision": r"^(ask|block)$",
    },
)
_TERMINAL_GUARD_TELEMETRY_LIST_VARIANT = WriteContract(
    tool_name="terminal_guard_telemetry.operation=list",
    required=[],
    optional=["task_ref", "decision", "harness", "tool_name", "limit", "offset"],
    field_grammars={
        "decision": r"^(ask|block)$",
    },
)
_TERMINAL_GUARD_TELEMETRY_REPLAY_VARIANT = WriteContract(
    tool_name="terminal_guard_telemetry.operation=replay",
    required=[],
    optional=["spool_path"],
)


_REVIEW_FINDINGS_RECORD_VARIANT = WriteContract(
    tool_name="review_findings.operation=record",
    required=["session", "finding_id", "severity", "file_path", "description"],
    optional=["details", "review_mode", "task_ref", "actor"],
    field_grammars={
        "severity": r"^(high|medium|low)$",
        "review_mode": r"^(branch|release_audit|planning)$",
    },
)
_REVIEW_FINDINGS_BATCH_VARIANT = WriteContract(
    tool_name="review_findings.operation=batch_record",
    required=["session", "findings"],
    optional=["task_ref", "actor"],
)
_REVIEW_FINDINGS_UPDATE_VARIANT = WriteContract(
    tool_name="review_findings.operation=update",
    required=["status"],
    optional=[
        "finding_id",
        "finding_db_id",
        "resolution_notes",
        "reopen_reason",
        "task_ref",
        "session",
        "actor",
        "verified_commit_sha",
        "verification_evidence",
    ],
    field_grammars={
        "status": r"^(open|fixed|deferred|wontfix)$",
    },
)
_REVIEW_FINDINGS_RESOLVE_VARIANT = WriteContract(
    tool_name="review_findings.operation=resolve",
    required=[],
    optional=[
        "task_ref",
        "session",
        "finding_ids",
        "all_open",
        "resolution_notes",
        "verification_evidence",
        "actor",
    ],
)
_REVIEW_FINDINGS_REPAIR_VARIANT = WriteContract(
    tool_name="review_findings.operation=repair_provenance",
    required=[
        "session",
        "finding_id",
        "expected_branch",
        "expected_commit_sha",
        "new_branch",
        "new_commit_sha",
        "reason",
    ],
    optional=["task_ref", "actor"],
    field_grammars={
        # Mirror Pydantic ``min_length=20`` on ``reason``.
        "reason": r"^.{20,}$",
    },
)
_REVIEW_FINDINGS_MERGE_VARIANT = WriteContract(
    tool_name="review_findings.operation=merge",
    required=["source_task_refs", "target_task_ref"],
    optional=["session", "retire_sources", "actor"],
)
_REVIEW_FINDINGS_LIST_VARIANT = WriteContract(
    tool_name="review_findings.operation=list",
    required=[],
    optional=[
        "task_ref",
        "status",
        "severity",
        "limit",
        "offset",
        "review_mode",
        "finding_id",
        "finding_db_id",
        "detail",
    ],
    field_grammars={
        "review_mode": r"^(branch|release_audit|planning)$",
        "detail": r"^(full|summary)$",
    },
)


_CLOSE_SLICE_RATIONALE_GRAMMAR = (
    r"(?s)^.*?##\s*Changes.*?##\s*Verification.*?##\s*Schema\s*/\s*Contract\s*Changes.*?##\s*Open\s*Threads.*$"
)


REGISTRY: dict[str, WriteContract] = {
    "set_handoff_state": WriteContract(
        tool_name="set_handoff_state",
        required=["task_ref"],
        optional=[
            "objective",
            "status",
            "target_branch",
            "target_worktree_path",
            "task_plan_path",
            "expected_revision",
            "actor",
        ],
        field_grammars={
            "task_ref": r"^[A-Z][A-Z0-9_-]+$",
            "status": r"^(in_progress|done|paused|blocked|abandoned|active)$",
        },
    ),
    "record_event": WriteContract(
        tool_name="record_event",
        required=["event"],
        optional=[],
        selector_field="event_kind",
        variants={
            "decision": _DECISION_VARIANT,
            "test_result": _TEST_RESULT_VARIANT,
            "blocker": _BLOCKER_VARIANT,
            "error": _ERROR_VARIANT,
        },
    ),
    "review_runs": WriteContract(
        tool_name="review_runs",
        required=["operation"],
        optional=[],
        selector_field="operation",
        variants={
            "record": _REVIEW_RUNS_RECORD_VARIANT,
            "list": _REVIEW_RUNS_LIST_VARIANT,
            "coverage": _REVIEW_RUNS_COVERAGE_VARIANT,
        },
    ),
    "review_findings": WriteContract(
        tool_name="review_findings",
        required=["operation"],
        optional=[],
        selector_field="operation",
        variants={
            "record": _REVIEW_FINDINGS_RECORD_VARIANT,
            "batch_record": _REVIEW_FINDINGS_BATCH_VARIANT,
            "update": _REVIEW_FINDINGS_UPDATE_VARIANT,
            "resolve": _REVIEW_FINDINGS_RESOLVE_VARIANT,
            "repair_provenance": _REVIEW_FINDINGS_REPAIR_VARIANT,
            "merge": _REVIEW_FINDINGS_MERGE_VARIANT,
            "list": _REVIEW_FINDINGS_LIST_VARIANT,
        },
    ),
    "terminal_guard_telemetry": WriteContract(
        tool_name="terminal_guard_telemetry",
        required=["operation"],
        optional=[],
        selector_field="operation",
        variants={
            "record": _TERMINAL_GUARD_TELEMETRY_RECORD_VARIANT,
            "list": _TERMINAL_GUARD_TELEMETRY_LIST_VARIANT,
            "replay": _TERMINAL_GUARD_TELEMETRY_REPLAY_VARIANT,
        },
    ),
    "next_actions": WriteContract(
        tool_name="next_actions",
        required=["operation"],
        optional=["action_id", "action", "rationale", "actor", "task_ref"],
        # 'list' is the tool's read sub-op (see READ_ONLY_OPERATIONS);
        # included here so the grammar matches the registered surface.
        field_grammars={"operation": r"^(add|update|complete|skip|list)$"},
        selector_field="operation",
    ),
    "close_slice": WriteContract(
        tool_name="close_slice",
        required=["task_ref", "author_tag", "work_ref", "slug", "rationale", "session", "expected_revision"],
        optional=["actor", "context_window_remaining", "input_tokens", "output_tokens", "total_tokens"],
        field_grammars={
            "author_tag": r"^[a-z][a-z0-9_]*$",
            "work_ref": r"^[A-Za-z0-9][A-Za-z0-9_-]*$",
            "slug": r"^[a-z0-9][a-z0-9_]*$",
            "rationale": _CLOSE_SLICE_RATIONALE_GRAMMAR,
        },
    ),
    "archive": WriteContract(
        tool_name="archive",
        required=["operation"],
        optional=[
            "task_ref",
            "notes",
            "clear_active_if_matches",
            "prune_working_rows",
            "allow_destructive_clear",
            "cascade_maint_review",
            "apply",
            "include_snapshot",
            "actor",
            "expected_revision",
        ],
        field_grammars={
            "operation": r"^(archive|gc|reap|retention|get)$",
            "task_ref": r"^[A-Z][A-Z0-9_-]+$",
        },
    ),
    "update_task_status": WriteContract(
        tool_name="update_task_status",
        required=["task_ref", "status"],
        optional=["actor", "expected_revision", "rationale"],
        field_grammars={
            "task_ref": r"^[A-Z][A-Z0-9_-]+$",
            "status": r"^(in_progress|done|paused|blocked|abandoned)$",
        },
    ),
    "record_file_touch": WriteContract(
        tool_name="record_file_touch",
        required=["paths"],
        optional=["actor", "task_ref", "rationale"],
    ),
    "artifacts": WriteContract(
        tool_name="artifacts",
        required=["operation"],
        optional=["artifact_id", "artifact_type", "payload", "actor", "task_ref"],
        field_grammars={"operation": r"^(record|get|search|purge)$"},
    ),
    "compact_session": WriteContract(
        tool_name="compact_session",
        required=["session"],
        optional=["actor", "task_ref"],
    ),
    # The registered MCP tool is ``compaction`` (the legacy
    # ``compact_session`` row above covers only the Python-API helper);
    # without this row compaction write rejections were never
    # self-captured (internal review finding REV-C-002).
    "compaction": WriteContract(
        tool_name="compaction",
        required=["operation"],
        optional=[
            "transcript_path",
            "task_ref",
            "harness",
            "session_id",
            "compaction_id",
            "actor",
        ],
        field_grammars={"operation": r"^(record|get|get_latest|disable|enable|status)$"},
        selector_field="operation",
    ),
    "import_handoff_state": WriteContract(
        tool_name="import_handoff_state",
        required=["payload"],
        optional=["actor"],
    ),
    "export_handoff_state": WriteContract(
        tool_name="export_handoff_state",
        required=[],
        optional=["task_ref", "include_archived"],
    ),
}


# implementation note self-capture scope (internal review finding REV-A-001):
# ok:false from a read-only sub-operation of a multiplexed write tool
# (not-found lookups, list filters) is not a write rejection and must not
# self-capture as ``mcp_write_rejected``. Keyed by registered tool name;
# values are the selector/operation values that perform no mutation.
READ_ONLY_OPERATIONS: dict[str, frozenset[str]] = {
    "archive": frozenset({"get"}),
    "artifacts": frozenset({"get", "search"}),
    "compaction": frozenset({"get", "get_latest", "status"}),
    "next_actions": frozenset({"list"}),
    "review_findings": frozenset({"list"}),
    "review_runs": frozenset({"list", "coverage"}),
    "terminal_guard_telemetry": frozenset({"list"}),
}

# Registered tools that never mutate state — the contract row exists only
# to publish their grammar. ok:false from these is never a write rejection.
READ_ONLY_TOOLS: frozenset[str] = frozenset({"export_handoff_state"})


def get_write_contract(tool_name: str) -> WriteContract | None:
    """Return the :class:`WriteContract` row for ``tool_name`` or ``None``."""

    return REGISTRY.get(tool_name)


def _grammar_match(pattern: str, value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return re.match(pattern, value) is not None
    except re.error:
        return False


def _validate_against(contract: WriteContract, payload: dict[str, Any], errors: list[str]) -> None:
    for required_field in contract.required:
        if required_field not in payload or payload[required_field] in (None, ""):
            errors.append(f"missing required field {required_field!r} for {contract.tool_name}")

    for field_name, pattern in contract.field_grammars.items():
        if field_name in payload and payload[field_name] is not None:
            if not _grammar_match(pattern, payload[field_name]):
                errors.append(f"field {field_name!r} for {contract.tool_name} violates grammar {pattern!r}")


def validate_write(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Side-effect-free validation against the registry.

    Returns ``{ok, errors[], tool_name, variant_selected}``. Variant
    dispatch uses the contract's ``selector_field`` (e.g. ``event_kind``
    for ``record_event``).
    """

    contract = REGISTRY.get(tool_name)
    if contract is None:
        return {
            "ok": False,
            "errors": [f"no registry row for {tool_name!r}; add a WriteContract entry to write_contracts.py."],
            "tool_name": tool_name,
            "variant_selected": None,
        }

    errors: list[str] = []
    variant_selected: str | None = None
    payload = dict(payload) if isinstance(payload, dict) else {}

    _validate_against(contract, payload, errors)

    if contract.variants and contract.selector_field:
        envelope = payload.get("event") if tool_name == "record_event" else payload
        selector_value = None
        if isinstance(envelope, dict):
            selector_value = envelope.get(contract.selector_field)
        if selector_value is None and contract.selector_field in payload:
            selector_value = payload[contract.selector_field]
        if isinstance(selector_value, str) and selector_value in contract.variants:
            variant_selected = selector_value
            variant_payload = envelope if isinstance(envelope, dict) else payload
            _validate_against(contract.variants[selector_value], variant_payload, errors)

    return {
        "ok": not errors,
        "errors": errors,
        "tool_name": tool_name,
        "variant_selected": variant_selected,
    }


def registry_export() -> dict[str, dict[str, Any]]:
    """Serializable view of the registry — used by ``limits.write`` envelope publish."""

    def _serialize(contract: WriteContract) -> dict[str, Any]:
        return {
            "tool_name": contract.tool_name,
            "required": list(contract.required),
            "optional": list(contract.optional),
            "field_grammars": dict(contract.field_grammars),
            "selector_field": contract.selector_field,
            "variants": {name: _serialize(variant) for name, variant in contract.variants.items()},
            "examples": list(contract.examples),
            "error_codes": list(contract.error_codes),
        }

    return {name: _serialize(contract) for name, contract in REGISTRY.items()}
