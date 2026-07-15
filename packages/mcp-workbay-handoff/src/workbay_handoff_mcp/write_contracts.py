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
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .shared_primitives import (
    BATCH_CLOSE_THRESHOLD,
    BATCH_CLOSE_WINDOW_SECONDS,
    MAX_RESOLUTION_NOTES_LENGTH,
    MAX_VERIFICATION_EVIDENCE_LENGTH,
    SLICE_COMPLETE_REQUIRED_SECTIONS,
)

# Inline close_slice prohibition (api.close_slice scans these case-insensitively).
# Not expressible as a registry field_grammar, so it is carried as a first-class
# ``prohibitions`` entry on the close_slice contract — surfaced identically by
# ``WriteContract.to_requirements``, ``close_slice_requirements()`` and
# ``integrity_check(kind='close')`` from this single source.
CLOSE_SLICE_RATIONALE_XML_ANTI_PATTERNS: tuple[str, ...] = (
    "<actor>",
    "<changed_files>",
    "</actor>",
    "</changed_files>",
)


def _close_slice_xml_prohibition() -> dict[str, Any]:
    """The close_slice rationale XML-tag ban, sourced from the shared constant."""

    return {
        "id": "rationale_xml_anti_patterns",
        "field": "rationale",
        "forbidden_substrings": list(CLOSE_SLICE_RATIONALE_XML_ANTI_PATTERNS),
        "remediation": (
            "Do not embed actor or changed_files as XML tags inside rationale. "
            "Pass actor={...} and changed_files=[...] as top-level close_slice parameters."
        ),
    }


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
    # Structured constraints the field-grammar layer cannot express.
    # ``prohibitions``: forbidden-substring / forbidden-value bans a caller must
    # honor before the write (e.g. close_slice rationale XML tags).
    # ``exactly_one_of``: each group is a set of fields of which exactly one must
    # be present (e.g. review_findings update ``finding_id`` / ``finding_db_id``).
    prohibitions: list[dict[str, Any]] = field(default_factory=list)
    exactly_one_of: list[list[str]] = field(default_factory=list)

    def to_requirements(self, state: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Compact machine-readable projection of pre-write requirements.

        Returns the fields a caller must satisfy *before* a write: required
        fields, field grammars, required ``##`` sections (when encoded in a
        rationale grammar), any ``prohibitions`` the contract declares, and
        structured ``constraints`` (e.g. ``exactly_one_of`` field groups).
        ``prohibitions`` is populated only for contracts that actually declare
        one (e.g. ``close_slice``); it is ``[]`` for contracts with none rather
        than an always-empty slot. Parallels :func:`registry_export` but
        collapses to a single selected variant when ``state`` supplies the
        contract's ``selector_field``.

        ``state`` may include the selector value and, for finding-closure
        projections, finding fields such as ``status`` /
        ``workspace_commit_relation``. Unknown selectors degrade to the
        parent contract shape without raising.
        """

        state_map: dict[str, Any] = dict(state) if state else {}
        selected = self
        variant_selected: str | None = None

        if self.variants and self.selector_field:
            selector_value = state_map.get(self.selector_field)
            if isinstance(selector_value, str) and selector_value in self.variants:
                selected = self.variants[selector_value]
                variant_selected = selector_value

        field_grammars = {**self.field_grammars, **selected.field_grammars}
        required_sections = _required_markdown_sections(field_grammars)

        # Prohibitions / structured constraints come from the selected variant
        # when it declares any, else the parent contract.
        prohibitions = selected.prohibitions or self.prohibitions
        exactly_one_of = selected.exactly_one_of or self.exactly_one_of
        constraints: dict[str, Any] = {}
        if exactly_one_of:
            constraints["exactly_one_of"] = [list(group) for group in exactly_one_of]

        out: dict[str, Any] = {
            "tool_name": selected.tool_name,
            # Parent required always; variant fields land in variant_required
            # so nested envelopes (record_event.event) stay unambiguous.
            "required": list(self.required),
            "variant_required": list(selected.required) if variant_selected is not None else [],
            "optional": list(selected.optional),
            "field_grammars": dict(field_grammars),
            "required_sections": required_sections,
            "prohibitions": [dict(p) for p in prohibitions],
            "constraints": constraints,
        }
        if self.selector_field is not None:
            out["selector_field"] = self.selector_field
        if variant_selected is not None:
            out["variant_selected"] = variant_selected
        elif self.variants:
            # Degrade: unknown/missing selector — parent shape only.
            out["available_variants"] = sorted(self.variants.keys())

        return out


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
        # Mirror the real ReviewFindingsUpdateOp.status Literal
        # (review_findings_api.py) — integrated/superseded are accepted at the
        # wire grammar but rejected downstream (integrate-/merge-managed);
        # resolved_on_branch is write-derived from fixed under the lifecycle flag.
        "status": r"^(open|fixed|deferred|wontfix|resolved_on_branch|integrated|superseded)$",
    },
    # The real update handler (review_findings_updates._validate_update_finding_input)
    # unconditionally rejects unless exactly one identifier is present.
    exactly_one_of=[["finding_id", "finding_db_id"]],
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
_REVIEW_FINDINGS_DISPOSITION_VARIANT = WriteContract(
    tool_name="review_findings.operation=disposition",
    required=["task_ref", "finding_id", "status"],
    optional=["resolution_notes", "disposition_evidence", "actor"],
    field_grammars={
        "status": r"^(deferred|wontfix|fixed)$",
    },
)
_REVIEW_FINDINGS_REANCHOR_VARIANT = WriteContract(
    tool_name="review_findings.operation=reanchor",
    required=["task_ref", "finding_id", "file_path"],
    optional=["expected_file_path", "resolution_notes", "actor"],
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
            "disposition": _REVIEW_FINDINGS_DISPOSITION_VARIANT,
            "reanchor": _REVIEW_FINDINGS_REANCHOR_VARIANT,
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
        # api.close_slice scans rationale for these XML tags; surface the ban
        # in to_requirements so a caller building from the projection sees it.
        prohibitions=[_close_slice_xml_prohibition()],
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
            "operation": r"^(archive|gc|reap|reap_scratch|reap_done|retention|get)$",
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
    "continuation": WriteContract(
        tool_name="continuation",
        required=["operation"],
        optional=[
            "task_ref",
            "lane_id",
            "packet_id",
            "done_do_not_redo",
            "next_actions",
            "verified_anchors",
            "gotchas",
        ],
        field_grammars={"operation": r"^(save|load)$"},
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
    "continuation": frozenset({"load"}),
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

    # ``exactly_one_of``: each group requires exactly one present (non-empty) key.
    # Mirrors the real update handler
    # (review_findings_updates._validate_update_finding_input) which rejects unless
    # exactly one of finding_id / finding_db_id is supplied — so the side-effect-free
    # preflight matches the handler for both the missing-both and supplying-both cases.
    for group in contract.exactly_one_of:
        present = [key for key in group if key in payload and payload[key] not in (None, "")]
        if len(present) != 1:
            errors.append(f"exactly one of {list(group)!r} required for {contract.tool_name}; got {present!r}")

    # ``prohibitions``: forbidden-substring bans, scanned case-insensitively to
    # match the real handler (e.g. api.close_slice rejects rationale carrying the
    # <actor>/<changed_files> XML tags). Only forbidden-substring prohibitions are
    # enforceable here; forbidden-value prohibitions (status literals) are already
    # covered by the field grammars / variant dispatch.
    for prohibition in contract.prohibitions:
        prohibited_field = prohibition.get("field")
        forbidden_substrings = prohibition.get("forbidden_substrings") or []
        if not prohibited_field or prohibited_field not in payload:
            continue
        value = payload[prohibited_field]
        if not isinstance(value, str):
            continue
        haystack = value.lower()
        hits = [sub for sub in forbidden_substrings if str(sub).lower() in haystack]
        if hits:
            errors.append(
                f"field {prohibited_field!r} for {contract.tool_name} contains forbidden "
                f"substring(s) {hits!r} (prohibition {prohibition.get('id')!r})"
            )


def validate_write(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Side-effect-free validation against the registry.

    Returns ``{ok, errors[], tool_name, variant_selected}`` and, on failure,
    the structured rejection envelope ``{violated, expected, example, rule_id}``
    (internal — registry-wide default wrapper). Variant dispatch uses
    the contract's ``selector_field`` (e.g. ``event_kind`` for ``record_event``).
    """

    from .structured_rejections import wrap_unclassified_rejection  # noqa: PLC0415

    contract = REGISTRY.get(tool_name)
    if contract is None:
        msg = f"no registry row for {tool_name!r}; add a WriteContract entry to write_contracts.py."
        structured = wrap_unclassified_rejection(
            tool_name=tool_name,
            error=msg,
            rule_id=f"write.unclassified.{tool_name}",
        )
        return {
            "ok": False,
            "errors": [msg],
            "tool_name": tool_name,
            "variant_selected": None,
            **{k: structured[k] for k in ("violated", "expected", "example", "rule_id")},
            "error": structured["error"],
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

    result: dict[str, Any] = {
        "ok": not errors,
        "errors": errors,
        "tool_name": tool_name,
        "variant_selected": variant_selected,
    }
    if errors:
        example: Any = None
        if contract.examples:
            example = contract.examples[0]
        structured = wrap_unclassified_rejection(
            tool_name=tool_name,
            error="; ".join(errors),
            example=example,
            rule_id=f"write.unclassified.{tool_name}",
        )
        result.update({k: structured[k] for k in ("violated", "expected", "example", "rule_id")})
        result["error"] = structured["error"]
    return result


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


def _required_markdown_sections(field_grammars: Mapping[str, str]) -> list[str]:
    """Required ``##`` headings for any contract carrying the shared rationale grammar.

    Single route: a rationale grammar that *is* ``_CLOSE_SLICE_RATIONALE_GRAMMAR``
    encodes the ``SLICE_COMPLETE_REQUIRED_SECTIONS`` headings. Section names come
    from that constant — never re-hardcoded here.
    """

    if field_grammars.get("rationale") == _CLOSE_SLICE_RATIONALE_GRAMMAR:
        return list(SLICE_COMPLETE_REQUIRED_SECTIONS)
    return []


def close_slice_requirements() -> dict[str, Any]:
    """Close-side requirements-on-read for ``close_slice``.

    Combines the registry projection (required fields + rationale ``##``
    section grammar) with the inline XML-tag prohibition that
    ``api.close_slice`` enforces. The XML ban is carried as a first-class
    ``prohibitions`` entry on the close_slice contract, so
    :meth:`WriteContract.to_requirements` already surfaces it — this helper
    keeps a defensive backfill for callers that read it directly (typically
    via ``integrity_check(kind='close')``).
    """

    contract = REGISTRY["close_slice"]
    reqs = contract.to_requirements()
    if not reqs.get("required_sections"):
        reqs["required_sections"] = list(SLICE_COMPLETE_REQUIRED_SECTIONS)
    if not reqs.get("prohibitions"):
        reqs["prohibitions"] = [_close_slice_xml_prohibition()]
    return reqs


def finding_closure_requirements(state: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Compact closure preconditions for a finding row's current state.

    Derived from the ``review_findings`` registry variants (update / resolve /
    disposition) plus the known runtime constraints the registry cannot encode
    (notes length, batch-close threshold, merge-managed superseded, ancestry).
    """

    state_map: dict[str, Any] = dict(state) if state else {}
    status = state_map.get("status")
    if not isinstance(status, str) or not status.strip():
        status = "open"
    commit_relation = state_map.get("workspace_commit_relation")
    if commit_relation is not None and not isinstance(commit_relation, str):
        commit_relation = None

    parent = REGISTRY["review_findings"]
    update_reqs = parent.to_requirements({**state_map, "operation": "update"})
    resolve_reqs = parent.to_requirements({**state_map, "operation": "resolve"})
    disposition_reqs = parent.to_requirements({**state_map, "operation": "disposition"})

    # State-aware required fields for the common update→fixed close path.
    # ``operation`` is mandated by review_findings (the multiplexed selector) and
    # must lead the top-level required list, else a caller building from this
    # summary is rejected for missing ``operation`` before any status check.
    # NOTE: exactly-one-of(finding_id, finding_db_id) is ALWAYS required by the
    # real update handler; it is a first-class structured constraint below
    # (constraints.exactly_one_of), not buried conditional prose.
    close_required: list[str] = ["operation", "status"]
    conditionally_required: dict[str, str] = {
        "verification_evidence": (
            f"required when >= {BATCH_CLOSE_THRESHOLD} other findings were marked fixed "
            f"in the last {BATCH_CLOSE_WINDOW_SECONDS}s for this task"
        ),
        "resolution_notes": ("required when status is deferred/wontfix, or when fixing from a newer descendant commit"),
        "reopen_reason": ("required when transitioning a non-open finding back to status='open'"),
    }
    if status == "open" and commit_relation == "descendant":
        close_required.append("resolution_notes")
    if status in {"deferred", "wontfix"}:
        # Already terminal dispositions — re-disposition still needs notes.
        close_required.append("resolution_notes")

    prohibitions: list[dict[str, Any]] = [
        {
            "id": "superseded_is_merge_managed",
            "forbidden": {"status": "superseded"},
            "use_instead": "review_findings(operation='merge', retire_sources=True)",
        },
        {
            "id": "integrated_is_integrate_managed",
            "forbidden": {"status": "integrated"},
            "use_instead": "review_findings(operation='integrate')",
        },
    ]

    return {
        "tool_name": "review_findings",
        "finding_status": status,
        "workspace_commit_relation": commit_relation,
        "required": close_required,
        "conditionally_required": conditionally_required,
        "constraints": {
            "resolution_notes_max_chars": MAX_RESOLUTION_NOTES_LENGTH,
            "verification_evidence_max_chars": MAX_VERIFICATION_EVIDENCE_LENGTH,
            "batch_close_threshold": BATCH_CLOSE_THRESHOLD,
            "batch_close_window_seconds": BATCH_CLOSE_WINDOW_SECONDS,
            "verified_commit_relation": "same_or_descendant",
            # Always required by the real update handler — exactly one identifier.
            "exactly_one_of": [["finding_id", "finding_db_id"]],
            # The update grammar advertises status='resolved_on_branch', but it is
            # write-derived from status='fixed' only under the lifecycle flag — a
            # caller must not pass it directly.
            "status_notes": {
                "resolved_on_branch": (
                    "lifecycle-gated: write-derived from status='fixed' under the "
                    "resolved-on-branch lifecycle flag; not a caller-supplied status"
                ),
            },
        },
        "prohibitions": prohibitions,
        "operations": {
            "update": update_reqs,
            "resolve": resolve_reqs,
            "disposition": disposition_reqs,
        },
    }
