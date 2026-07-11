from __future__ import annotations

import asyncio
import functools
import inspect
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import TimeoutExpired as SubprocessTimeoutExpired
from typing import Annotated, Any, Callable, Literal, cast

from fastmcp import FastMCP
from fastmcp.client import Client, PythonStdioTransport
from pydantic import BaseModel, Field, TypeAdapter
from workbay_protocol import BRAND_NAME, StructuredSummary, resolve_env_alias

from . import compaction as _compaction_module
from . import core
from .agent_errors import capture_write_rejection, record_agent_error
from .api_contract_shared import (
    ActorParam,
    DecisionChangedFilesParam,
    TaskRefParam,
    WriteActorInput,
)
from .api_contract_shared import (
    dump_actor as _dump_actor,
)
from .compaction import (
    COMPACTION_HARNESS_CHOICES,
    COMPACTION_HARNESS_INPUT_CHOICES,
    CompactionHarness,
    CompactionHarnessInput,
    render_cold_start_compaction,
)
from .config import RuntimeConfig
from .core import PromptMetrics, ResolvedWriteContext, TokenUsage
from .orientation_reads import orientation_read_boundary, record_orientation_read_once
from .review_findings import (
    batch_record_review_findings,
    list_review_findings,
    merge_review_findings,
    record_review_finding,
    repair_review_finding_provenance,
    resolve_review_findings,
)
from .review_findings import (
    integrate_review_findings as _core_integrate_review_findings,
)
from .review_findings_api import (
    ReviewFindingBatchItemInput,
    ReviewFindingDetailsInput,
    ReviewFindingsBatchRecordOp,
    ReviewFindingsListOp,
    ReviewFindingsMergeOp,
    ReviewFindingsParam,
    ReviewFindingsRecordOp,
    ReviewFindingsRepairProvenanceOp,
    ReviewFindingsResolveOp,
    ReviewFindingsUpdateOp,
    review_findings,
)
from .review_findings_updates import _run_opportunistic_integrate_for_task
from .runtime import configure_runtime, get_runtime_config, reset_runtime_config
from .shared_primitives import DEFAULT_HANDOFF_LIMITS, HANDOFF_ACTIVE_STATUSES
from .shared_schema import SchemaVersionMismatchError
from .shared_write_context import BranchMismatchError, WriteActor
from .slice_decision import (
    compose_slice_complete_decision_id,
    is_prefixed_slice_complete_decision,
)
from .slice_decision import (
    validate_decision_id as _validate_decision_id_helper,
)
from .state_init import init_state
from .terminal_telemetry import list_terminal_guard_events, record_terminal_guard_event, replay_terminal_guard_spool

_core_record_agent_error = record_agent_error
_core_record_decision = core.record_decision
build_write_actor = core.build_write_actor
_core_update_next_actions = core.update_next_actions
_core_record_test_result = core.record_test_result
_core_report_blocker = core.report_blocker
_core_update_review_finding = core.update_review_finding
_core_repair_review_finding_provenance = repair_review_finding_provenance
_core_record_review_run = core.record_review_run
list_review_runs = core.list_review_runs
get_review_coverage = core.get_review_coverage
handoff_close_check = core.handoff_close_check
working_tree_integrity_check = core.working_tree_integrity_check
post_merge_integrity_check = core.post_merge_integrity_check
export_handoff_state = core.export_handoff_state
import_handoff_state = core.import_handoff_state
archive_task_state = core.archive_task_state
tasks_gc = core.tasks_gc
reap_tasks = core.reap_tasks
reap_stale_plan_cursors = core.reap_stale_plan_cursors
archives_retention_gc = core.archives_retention_gc
classify_live_tasks = core.classify_live_tasks
get_archived_task = core.get_archived_task
switch_task = core.switch_task
_core_update_task_status = core.update_task_status
_core_set_handoff_state = core.set_handoff_state
get_handoff_state = core.get_handoff_state
list_next_actions = core.list_next_actions
record_file_touch = core.record_file_touch
get_touched_files = core.get_touched_files

record_artifact = core.record_artifact
search_artifacts = core.search_artifacts
get_artifact = core.get_artifact
purge_artifacts = core.purge_artifacts
search_handoff = core.search_handoff
load_session = core.load_session
_core_close_slice = core.close_slice
audit_decision_ids = core.audit_decision_ids


TOOL_DESCRIPTIONS: dict[str, str] = {
    "set_handoff_state": "Update the active task state (objective, focus, status). Optimistic revision guard.",
    "get_handoff_state": "Read task handoff summary (blockers, actions, findings). Pass sections='decisions_recent,findings_open' to select specific sections; active and limits are always included. Pass sections='identity' for an identity-only response (active + limits, no data sections). The identity payload also carries the canonical compaction advisory at data.compaction_advisory (with a mirrored data.compaction_recommended boolean flag) per internal — callers MUST read advisory state from this surface rather than recomputing token/char totals locally. internal: data.compaction_advisory.contract_source publishes the resolved contract path and thresholds, the package-reference contract metadata when available, and a drift flag; data.compaction_advisory.recommended_action names the explicit record action (e.g. 'compaction(operation=record)') when recommended=true. Pass detail='summary' to truncate long rationale and verification fields. For exact-read on the decision ledger, sections must include decisions_recent and pass any of decision_fields (allowlist projection), decision_branch, decision_commit_sha, decision_lane_id, or decision_id_prefix (literal-prefix; LIKE wildcards are escaped) to filter decisions_recent without raw SQL. internal: pass read_profile='identity'|'hot_summary'|'review_packet'|'open_items'|'full_debug' for a named intent shape that expands to the existing sections/detail/top_n_* knobs; explicit lower-level args still override the profile, and the response reports the applied shape under data.read_shape. Pass response_budget_bytes plus budget_policy='warn'|'auto_summary'|'fail' to opt into server-side budget planning: 'auto_summary' (default when a budget is supplied) lowers limits/detail and omits optional sections before heavy fetches; 'fail' rejects with a structured retry_with hint when the requested shape would exceed the budget; 'warn' preserves the requested shape and only attaches budget metadata. Budget metadata is returned at data.read_budget. Pass include_write_schemas=True to opt into the limits.write.tools write-contract registry (~13KB); default False keeps scalar write limits but omits the tools map so identity/hot_summary reads stay small ([T23]). Bare calls with neither read_profile nor response_budget_bytes nor an explicit sections= selection apply the server default budget (DEFAULT_BARE_CALL_RESPONSE_BUDGET_BYTES, currently 16000, aligned with the client hook hard-truncate via CANONICAL_RESPONSE_BUDGET_BYTES) with budget_policy=auto_summary ([DATA-14]); explicit caller args (including sections=) always win.",
    "record_event": "Record a decision, verification result, or blocker mutation through one typed event surface. Set event.event_kind to 'decision', 'test_result', or 'blocker' to select the required fields.",
    "next_actions": "List or mutate next-action items through one typed domain surface. Set action.operation to 'list', 'add', 'update', 'complete', or 'skip'.",
    "review_findings": "Record, batch record, update, resolve, repair provenance, merge, integrate, disposition, reanchor, or list review findings through one typed domain surface. Set review.operation to 'record', 'batch_record', 'update', 'resolve', 'repair_provenance', 'merge', 'integrate', 'disposition', 'reanchor', or 'list'. The 'resolve' operation is the commit-backed reconciliation path that classifies open findings against the current workspace commit, marks eligible findings fixed, and reports pending-uncommitted outcomes without dummy writes. The 'repair_provenance' operation is the bounded admin path for fixing a finding row whose source branch/commit_sha was attributed to the wrong commit (e.g. the reviewer's workspace HEAD instead of the actual buggy code's commit) — see ReviewFindingsRepairProvenanceOp. The 'merge' operation (coordinator-centric) re-records findings from one or more source task_refs under a target coordinator task_ref with merged_from provenance. By default retire_sources=true retires merged source rows to status='superseded' in the same transaction while preserving audit content; retire_sources=false keeps legacy additive source rows — see ReviewFindingsMergeOp. The 'disposition' operation is the admin path for terminal status on findings whose task_ref has no live handoff row or is already status=done (orphan/stale refs); optional disposition_evidence is folded into resolution_notes (implementation note) — see ReviewFindingsDispositionOp. The 'reanchor' operation is the open-preserving file_path rewrite (status stays open) for remappable rename-map findings — see ReviewFindingsReanchorOp.",
    "review_runs": "Record, list, or summarize review-run coverage through one typed domain surface. Set review.operation to 'record', 'list', or 'coverage'.",
    "terminal_guard_telemetry": "Record, list, or replay durable terminal-guard telemetry through one typed domain surface. Set telemetry.operation to 'record', 'list', or 'replay'. Record writes a bounded redacted command preview plus repo-instance identity; list returns persisted rows with optional task/decision/harness filters; replay ingests fallback JSONL rows into the durable ledger with event-key dedupe.",
    "integrity_check": "Run a working-tree, post-merge, or close-readiness integrity check through one typed surface. Set kind to 'working_tree' (compares dirty paths to .task-state/dirty-allowlist), 'post_merge' (diffs working tree against merged_sha), or 'close' (full close-readiness gate: blockers, pending actions, findings, optional fresh-test gate).",
    "audit_decision_ids": "Audit decision IDs for grammar conformance. Returns canonical/malformed/freeform classifications.",
    "validate": "Side-effect-free preflight router for the handoff write surfaces. Set payload.kind='decision_id' to preflight a decision identifier (returns category plus error/suggested fields using the same slice-complete validator as mutation writes); set payload.kind='write' to preflight an MCP write payload against the registry's required fields and field grammars (returns ok, errors[], variant_selected). No DB writes occur.",
    "render_handoff": (
        "Render the handoff surface files through one compound tool. "
        "Set kind='current_task' to produce the machine-readable CURRENT_TASK.json "
        "snapshot for the active task; set kind='dashboard' to produce DASHBOARD.txt — "
        "the human-scoped observatory view with Needs Attention summary, All Tasks table, "
        "cross-task open findings, and optional extension sections (e.g. Lane Health from "
        "workbay-orchestrator-mcp)."
    ),
    "export_handoff_state": "Export the task handoff state to a portable JSON snapshot.",
    "import_handoff_state": "Import a previously exported handoff state snapshot into the local database.",
    "archive": (
        "Archive lifecycle surface. Set operation='archive' to snapshot a completed task into "
        "archive storage; operation='gc' to bulk-archive status=done internal-* "
        "rows whose internal parent is already archived (dry-run by default; pass apply=True to "
        "mutate); operation='reap' to classify live rows into closeable/active/ambiguous and "
        "optionally close+archive closeable rows (dry-run by default); operation='reap_scratch' to "
        "close+archive reviewer-scratch handoff rows with no open findings (includes status=done); "
        "operation='reap_done' to archive status=done non-scratch rows with no open findings and a "
        "merged (or integration-target) branch (dry-run by default); "
        "operation='retention' to "
        "dry-run/prune stale task_archives rows older than older_than_days (default 90); "
        "operation='get' to read an archived row by task_ref (set include_snapshot=False to omit "
        "the parsed snapshot)."
    ),
    "get_verified_tests": "List verified test rows from the handoff ledger with optional task, lane, branch, commit, pass/fail, trace, and changed-file correlation filters.",
    "touched_files": "Record or list task-scoped file-touch rows through one typed surface. Set operation to 'record' (returns the new touch row envelope) or 'list' (returns task-scoped touches with deterministic newest-first ordering and a bounded limit). For 'record', change_kind is one of edit (modified content / git M), add (new file / git A), or delete (removed / git D) — use 'edit' for any in-place modification.",
    "compaction": "Persist, dereference, or fetch the newest session compaction row through one typed surface. Set operation to 'record' (returns the new compaction_id), 'get' (returns a CompactionRecord: the parsed StructuredSummary plus tokens_saved_estimate), or 'get_latest' (returns the newest CompactionRecord or None).",
    "semantic_reinjection_packet": "Build a slice-anchored semantic reinjection packet for a task. Pass anchor_texts (changed_files snippets or rationale excerpts) and optional semantic_content_budget_chars; returns relevant_lines plus status/skip_reason and the full semantic_reinjection payload. Degrades cleanly when the embedding provider is unavailable.",
    "update_task_status": "Update a task status without recording a slice decision. For the active task this requires expected_revision; for archived tasks it updates the archived snapshot status used by the dashboard.",
    "reconcile_reviewer_scratch_findings_gc": (
        "Bulk-retire open reviewer-scratch findings by deriving coordinators from %-REV-% "
        "task_refs and looping reconcile_review_findings per coordinator. Dry-run by default; "
        "pass apply=True to retire eligible rows."
    ),
    "collect_stale_nonscratch_open_findings": (
        "Report open findings on non-scratch task_refs with no live handoff row or with "
        "status=done. Use before admin disposition or reap_done sweeps to surface orphan/stale "
        "refs that need triage."
    ),
    "load_session": "Load session context: get_handoff_state + review_findings(list open) + touched_files in one call. Pass sections to shape the nested state payload, detail to shape both state and findings, and top_n_touched_files (default 20, max 200) to bound the additive touched_files list. The compaction advisory mirrored from get_handoff_state (internal) is exposed at data.compaction_advisory and data.compaction_recommended for cold-start consumers. Pass include_context_refresh=True to attach an opt-in same-session refresh packet for the latest compaction; pass last_injected_compaction_id to dedupe a packet the caller already injected. internal: pass read_profile to apply a named shape across the compound payload (open_findings, touched_files, state); pass open_findings_limit=0 or top_n_touched_files=0 to omit the additive section entirely (the response records the omission under data.read_shape.session.omitted_sections). Pass response_budget_bytes plus budget_policy='warn'|'auto_summary'|'fail' to opt into compound-payload budget planning: 'auto_summary' (default when a budget is supplied) reduces the nested state shape first and then trims add-ons; 'fail' rejects with a structured retry_with hint before fetching; 'warn' preserves the shape and only attaches budget metadata. Compound budget metadata is returned at data.read_budget. Pass include_write_schemas=True to opt into the nested state's limits.write.tools registry (~13KB); default False omits the tools map on the compound payload ([T23]). Bare calls with neither read_profile nor response_budget_bytes nor an explicit sections= selection apply the server default budget (DEFAULT_BARE_CALL_RESPONSE_BUDGET_BYTES, currently 16000, aligned with the client hook hard-truncate via CANONICAL_RESPONSE_BUDGET_BYTES) with budget_policy=auto_summary ([DATA-14]); explicit caller args (including sections=) always win.",
    "close_slice": "Record a slice-complete decision and always regenerate DASHBOARD.txt; CURRENT_TASK.json is regenerated only when current_task_auto_regen is enabled (otherwise refresh it on demand with render_handoff(kind='current_task')). Non-done tasks stay in_progress; an existing done status is preserved (status_preserved: done). Idempotent revision conflicts auto-retry up to 3 times (revision_retries). Requires expected_revision when the target task is currently active. Pass changed_files to persist structured review scope on the nested decision write. Pass slice_number to bind the completion to a plan checklist slice structurally (independent of the decision slug).",
    "artifacts": "Record, search, get, or purge artifact sources through one typed domain surface. Set artifact.operation to 'record', 'search', 'get', or 'purge'.",
    "search_handoff": "Search decisions, findings, blockers, actions, and verified tests by keyword with BM25 ranking. Pass detail='summary' to truncate snippets and fields='record_type,snippet' to project per-result fields. Pass decision_fields=['decision','branch','commit_sha','lane_id',...] to merge decision-table columns onto decision rows for discovery use cases (allowlist; non-decision rows unchanged).",
    "list_handoff_rows": (
        "Enumerate live handoff_state rows. Returns task_ref, status, target_branch, "
        "target_worktree_path, task_plan_path, updated_at, and revision per row. "
        "Pass status_filter=['in_progress','review','blocked'] (LIVE_ACTIVE_STATUSES) to "
        "exclude status=done rows. Replaces the previous 'sqlite3 .task-state/handoff.db' "
        "fallback for tooling that needs to enumerate rows. Archived rows live in "
        "task_archives and are excluded; use get_archived_task for those."
    ),
}

DECISION_ID_DESCRIPTION = (
    "Stable decision identifier to persist in the ledger. For slice-complete writes use the canonical form "
    "<author_tag>_slice_complete_<work_ref>_<slug>, for example "
    "codex_slice_complete_plan0005_render_budget_benchmark."
)

SLICE_COMPLETE_DECISION_ID_DESCRIPTION = (
    "Stable slice-complete decision identifier. Must use the canonical form "
    "<author_tag>_slice_complete_<work_ref>_<slug>, for example "
    "codex_slice_complete_plan0005_render_budget_benchmark."
)


def _resolve_close_slice_decision(
    *,
    decision: str | None,
    author_tag: str | None,
    work_ref: str | None,
    slug: str | None,
) -> tuple[str | None, str | None]:
    semantic_parts = {
        "author_tag": author_tag,
        "work_ref": work_ref,
        "slug": slug,
    }
    provided_parts = {name: value for name, value in semantic_parts.items() if value is not None}
    if decision is None:
        if not provided_parts:
            return None, "decision is required unless author_tag, work_ref, and slug are all supplied."
        missing = [name for name, value in semantic_parts.items() if value is None]
        if missing:
            return None, f"Missing semantic slice id parts: {', '.join(missing)}."
        composed = compose_slice_complete_decision_id(
            author_tag=str(author_tag),
            work_ref=str(work_ref),
            slug=str(slug),
        )
        if not is_prefixed_slice_complete_decision(composed):
            return None, (
                "Semantic slice id parts compose an invalid decision id. "
                "Expected <author_tag>_slice_complete_<work_ref>_<slug>."
            )
        return composed, None

    if provided_parts:
        missing = [name for name, value in semantic_parts.items() if value is None]
        if missing:
            return None, (
                "When supplying semantic slice id parts with decision, all of author_tag, work_ref, and slug "
                "must be provided."
            )
        composed = compose_slice_complete_decision_id(
            author_tag=str(author_tag),
            work_ref=str(work_ref),
            slug=str(slug),
        )
        if decision != composed:
            return None, (
                f"decision conflicts with semantic slice id parts. Pass only decision, or make it match {composed}."
            )
    return decision, None


def _build_doctor_cli_env(module_file: str | Path, env: dict[str, str] | None = None) -> dict[str, str]:
    cli_env = dict(os.environ if env is None else env)
    pythonpath_parts: list[str] = []

    resolved_module_file = Path(module_file).resolve()
    for ancestor in resolved_module_file.parents:
        packages_dir = ancestor / "packages"
        handoff_src = packages_dir / "mcp-workbay-handoff" / "src"
        if not handoff_src.is_dir():
            continue
        pythonpath_parts.append(str(handoff_src))
        bridge_src = packages_dir / "workbay-codex-bridge" / "src"
        if bridge_src.is_dir():
            pythonpath_parts.append(str(bridge_src))
        break

    existing_pythonpath = cli_env.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)

    if pythonpath_parts:
        cli_env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    else:
        cli_env.pop("PYTHONPATH", None)

    return cli_env


class RecordDecisionEvent(BaseModel):
    event_kind: Literal["decision"]
    event_id: Annotated[
        str | None,
        Field(description="Optional projection event id. Duplicate event ids are accepted as idempotent no-ops."),
    ] = None
    session: Annotated[str, Field(description="Session identifier for the decision write.")]
    decision: Annotated[str, Field(description=DECISION_ID_DESCRIPTION)]
    rationale: Annotated[
        str | None,
        Field(
            description=(
                "Optional markdown rationale. Soft limit: 1,500 chars; hard limit: 3,000 chars "
                "(enforced by hook before call reaches server). For slice_complete_* decisions "
                "use close_slice, which enforces required sections."
            )
        ),
    ] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None
    input_tokens: Annotated[int | None, Field(description="Optional prompt token count for this slice.")] = None
    output_tokens: Annotated[int | None, Field(description="Optional completion token count for this slice.")] = None
    total_tokens: Annotated[int | None, Field(description="Optional total token count for this slice.")] = None
    changed_files: DecisionChangedFilesParam = None


class RecordTestResultEvent(BaseModel):
    event_kind: Literal["test_result"]
    event_id: Annotated[
        str | None,
        Field(description="Optional projection event id. Duplicate event ids are accepted as idempotent no-ops."),
    ] = None
    session: Annotated[str, Field(description="Session identifier for the verification run.")]
    command: Annotated[str, Field(description="Verification command that was executed.")]
    passed: Annotated[bool, Field(description="Whether the verification command passed.")]
    result: Annotated[
        str | None,
        Field(description="Optional stdout or summarized verification evidence for the command."),
    ] = None
    traces: Annotated[
        list[str] | None,
        Field(description="Optional raw verification trace payloads to archive alongside the summarized result."),
    ] = None
    exit_code: Annotated[int | None, Field(description="Optional process exit code for the command.")] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class RecordErrorEvent(BaseModel):
    event_kind: Literal["error"]
    error_class: Annotated[
        str,
        Field(
            description=(
                "Error class for downstream harvest clustering, e.g. install_drift, "
                "mcp_write_rejected, mcp_unreachable, cli_failure, env_misconfig, other. "
                "Lowercase snake_case; append-only taxonomy."
            )
        ),
    ]
    summary: Annotated[
        str,
        Field(description="One-line redacted error summary (truncated to 256 chars)."),
    ]
    detail: Annotated[
        str | None,
        Field(description="Optional redacted traceback/output detail (truncated to 4 KiB)."),
    ] = None
    tool_name: Annotated[
        str | None,
        Field(description="Optional harness tool that surfaced the error, e.g. Bash."),
    ] = None
    command_preview: Annotated[
        str | None,
        Field(description="Optional redacted single-line command preview (256 chars)."),
    ] = None
    package_name: Annotated[
        str | None,
        Field(description="Optional workbay package implicated in the error."),
    ] = None
    package_version: Annotated[
        str | None,
        Field(description="Optional installed version of the implicated package."),
    ] = None
    # Accepted for record_event envelope uniformity (every variant takes
    # actor) but not persisted: agent_errors has no actor columns
    # (internal review finding REV-A-003).
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class ReportBlockerEvent(BaseModel):
    event_kind: Literal["blocker"]
    operation: Annotated[
        Literal["add", "resolve", "reopen"],
        Field(description="Blocker mutation to perform."),
    ]
    description: Annotated[
        str | None,
        Field(description="Blocker description. Required when adding a new blocker."),
    ] = None
    blocker_id: Annotated[
        int | None,
        Field(description="Existing blocker id. Required for resolve and reopen."),
    ] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None


RecordEventParam = Annotated[
    RecordDecisionEvent | RecordTestResultEvent | RecordErrorEvent | ReportBlockerEvent,
    Field(discriminator="event_kind"),
]

_RECORD_EVENT_ADAPTER: TypeAdapter[
    RecordDecisionEvent | RecordTestResultEvent | RecordErrorEvent | ReportBlockerEvent
] = TypeAdapter(RecordEventParam)


class DecisionIdValidate(BaseModel):
    kind: Literal["decision_id"]
    decision: Annotated[
        str,
        Field(description="Decision identifier to preflight without writing any ledger rows."),
    ]
    decision_kind: Annotated[
        str | None,
        Field(description="Optional decision kind to require, for example 'slice_complete'."),
    ] = None


class WriteValidate(BaseModel):
    kind: Literal["write"]
    tool_name: Annotated[
        str,
        Field(description="Name of the MCP write tool whose registry row should validate the payload."),
    ]
    payload: Annotated[
        dict,
        Field(description="Payload to preflight against the write-contract registry. No DB writes occur."),
    ]


ValidateParam = Annotated[
    DecisionIdValidate | WriteValidate,
    Field(discriminator="kind"),
]

_VALIDATE_ADAPTER: TypeAdapter[DecisionIdValidate | WriteValidate] = TypeAdapter(ValidateParam)


class ReviewRunsRecordOp(BaseModel):
    operation: Literal["record"]
    review_run_id: Annotated[str, Field(description="Globally unique review run identifier.")]
    session: Annotated[str, Field(description="Session identifier for the review run.")]
    subject_path: Annotated[str, Field(description="Workspace-relative path reviewed in this run.")]
    subject_kind: Annotated[
        Literal["task_plan", "epic", "branch", "adr", "roadmap", "other"],
        Field(description="Kind of artifact reviewed in this run."),
    ] = "task_plan"
    review_mode: Annotated[
        Literal["branch", "release_audit", "planning"],
        Field(description="Review mode label, for example planning or branch."),
    ] = "planning"
    verdict: Annotated[
        Literal["pass", "pass_with_findings", "fail", "conditional_pass"] | None,
        Field(description="Optional review verdict to store with the run."),
    ] = None
    verdict_decision: Annotated[
        str | None,
        Field(description="Optional decision id or summary that explains the verdict."),
    ] = None
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class ReviewRunsListOp(BaseModel):
    operation: Literal["list"]
    task_ref: TaskRefParam = None
    subject_path: Annotated[str | None, Field(description="Optional reviewed artifact path filter.")] = None
    limit: Annotated[int, Field(description="Maximum number of review runs to return.")] = 20
    offset: Annotated[int, Field(description="Pagination offset.")] = 0
    review_mode: Annotated[
        Literal["branch", "release_audit", "planning"] | None,
        Field(description="Optional review-mode filter."),
    ] = None
    verdict: Annotated[
        Literal["pass", "pass_with_findings", "fail", "conditional_pass"] | None,
        Field(description="Optional verdict filter."),
    ] = None


class ReviewRunsCoverageOp(BaseModel):
    operation: Literal["coverage"]
    task_ref: TaskRefParam = None
    subject_path: Annotated[str | None, Field(description="Optional reviewed artifact path scope.")] = None


ReviewRunsParam = Annotated[
    ReviewRunsRecordOp | ReviewRunsListOp | ReviewRunsCoverageOp,
    Field(discriminator="operation"),
]

_REVIEW_RUNS_ADAPTER: TypeAdapter[ReviewRunsRecordOp | ReviewRunsListOp | ReviewRunsCoverageOp] = TypeAdapter(
    ReviewRunsParam
)


class TerminalGuardTelemetryRecordOp(BaseModel):
    operation: Literal["record"]
    task_ref: TaskRefParam = None
    worktree_path: Annotated[
        str | None,
        Field(description="Optional worktree path associated with the telemetry event."),
    ] = None
    harness: Annotated[str, Field(description="Harness that emitted the terminal-guard decision.")]
    tool_name: Annotated[str, Field(description="Tool name that triggered the terminal-guard decision.")]
    decision: Annotated[
        Literal["ask", "block"],
        Field(description="Non-pass terminal-guard decision that was emitted."),
    ]
    trigger: Annotated[
        str | None,
        Field(description="Optional policy trigger category for the decision."),
    ] = None
    native_tool_hint: Annotated[
        str | None,
        Field(description="Optional native-tool replacement hint shown by the guard."),
    ] = None
    command_preview: Annotated[
        str,
        Field(description="Bounded preview of the blocked or ask-worthy terminal command."),
    ]
    policy_version: Annotated[str, Field(description="Policy version identifier that produced this decision.")]
    policy_source: Annotated[str, Field(description="Policy source path or identifier.")]
    fallback_source: Annotated[
        str | None,
        Field(description="Optional fallback spool source when a replayed row originated outside the DB."),
    ] = None
    created_at: Annotated[
        str | None,
        Field(description="Optional UTC timestamp for the telemetry row. Defaults to the DB clock when omitted."),
    ] = None


class TerminalGuardTelemetryListOp(BaseModel):
    operation: Literal["list"]
    task_ref: TaskRefParam = None
    decision: Annotated[
        Literal["ask", "block"] | None,
        Field(description="Optional decision filter."),
    ] = None
    harness: Annotated[
        str | None,
        Field(description="Optional harness filter."),
    ] = None
    tool_name: Annotated[
        str | None,
        Field(description="Optional tool-name filter."),
    ] = None
    limit: Annotated[int, Field(description="Maximum number of telemetry rows to return.")] = 20
    offset: Annotated[int, Field(description="Pagination offset.")] = 0


class TerminalGuardTelemetryReplayOp(BaseModel):
    operation: Literal["replay"]
    spool_path: Annotated[
        str | None,
        Field(description="Optional fallback JSONL spool path. Defaults to .task-state/terminal_guard.jsonl."),
    ] = None


TerminalGuardTelemetryParam = Annotated[
    TerminalGuardTelemetryRecordOp | TerminalGuardTelemetryListOp | TerminalGuardTelemetryReplayOp,
    Field(discriminator="operation"),
]

_TERMINAL_GUARD_TELEMETRY_ADAPTER: TypeAdapter[
    TerminalGuardTelemetryRecordOp | TerminalGuardTelemetryListOp | TerminalGuardTelemetryReplayOp
] = TypeAdapter(TerminalGuardTelemetryParam)


class NextActionsAddOp(BaseModel):
    operation: Literal["add"]
    action: Annotated[str, Field(description="Action text to add.")]
    priority: Annotated[
        int | None,
        Field(description="Optional priority value. Lower numbers sort first; defaults to 100 on add."),
    ] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class NextActionsUpdateOp(BaseModel):
    operation: Literal["update"]
    action_id: Annotated[int, Field(description="Existing action id to update.")]
    action: Annotated[str | None, Field(description="Optional replacement action text.")] = None
    priority: Annotated[int | None, Field(description="Optional replacement priority value.")] = None
    status: Annotated[
        Literal["pending", "done", "skipped"] | None,
        Field(description="Optional explicit status override. Only used for update operations."),
    ] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class NextActionsCompleteOp(BaseModel):
    operation: Literal["complete"]
    action_id: Annotated[int, Field(description="Existing action id to mark complete.")]
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class NextActionsSkipOp(BaseModel):
    operation: Literal["skip"]
    action_id: Annotated[int, Field(description="Existing action id to skip.")]
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class NextActionsListOp(BaseModel):
    operation: Literal["list"]
    task_ref: TaskRefParam = None
    lane_id: Annotated[str | None, Field(description="Optional lane filter.")] = None
    status: Annotated[
        Literal["all", "pending", "done", "skipped"],
        Field(description="Action status filter."),
    ] = "all"
    limit: Annotated[int, Field(description="Maximum number of actions to return.")] = 100
    offset: Annotated[int, Field(description="Pagination offset.")] = 0


NextActionsParam = Annotated[
    NextActionsAddOp | NextActionsUpdateOp | NextActionsCompleteOp | NextActionsSkipOp | NextActionsListOp,
    Field(discriminator="operation"),
]

_NEXT_ACTIONS_ADAPTER: TypeAdapter[
    NextActionsAddOp | NextActionsUpdateOp | NextActionsCompleteOp | NextActionsSkipOp | NextActionsListOp
] = TypeAdapter(NextActionsParam)


class ArtifactsRecordOp(BaseModel):
    operation: Literal["record"]
    source_kind: Annotated[str, Field(description="Artifact source kind.")]
    source_label: Annotated[str, Field(description="Artifact source label.")]
    content: Annotated[str, Field(description="Artifact content to index.")]
    task_ref: TaskRefParam = None
    lane_id: Annotated[str | None, Field(description="Optional lane scope.")] = None
    app_root: Annotated[str | None, Field(description="Optional application root scope.")] = None
    content_type: Annotated[str, Field(description="Artifact content type.")] = "text/plain"
    summary: Annotated[str | None, Field(description="Optional artifact summary.")] = None
    metadata: Annotated[dict[str, Any] | None, Field(description="Optional structured metadata.")] = None


class ArtifactsSearchOp(BaseModel):
    operation: Literal["search"]
    queries: Annotated[
        list[str] | None,
        Field(description="Optional search queries. Omit or pass empty to list sources."),
    ] = None
    task_ref: TaskRefParam = None
    lane_id: Annotated[str | None, Field(description="Optional lane scope.")] = None
    app_root: Annotated[str | None, Field(description="Optional application root scope.")] = None
    source_kind: Annotated[str | None, Field(description="Optional source-kind filter.")] = None
    content_type: Annotated[str | None, Field(description="Optional content-type filter.")] = None
    limit: Annotated[int, Field(description="Maximum number of hits or sources to return.")] = 10
    offset: Annotated[int, Field(description="Pagination offset.")] = 0
    detail: Annotated[Literal["full", "summary"], Field(description="Detail level for returned rows.")] = "full"
    fields: Annotated[str | None, Field(description="Optional comma-separated field projection.")] = None


class ArtifactsGetOp(BaseModel):
    operation: Literal["get"]
    source_id: Annotated[int | None, Field(description="Optional numeric source id lookup.")] = None
    task_ref: TaskRefParam = None
    source_label: Annotated[str | None, Field(description="Optional source label lookup within a task.")] = None
    include_terms: Annotated[bool, Field(description="Whether to include distinctive terms.")] = False
    top_n_terms: Annotated[int, Field(description="Maximum number of distinctive terms to return.")] = 10
    detail: Annotated[Literal["full", "summary"], Field(description="Detail level for the returned source.")] = "full"
    fields: Annotated[str | None, Field(description="Optional comma-separated field projection.")] = None


class ArtifactsPurgeOp(BaseModel):
    operation: Literal["purge"]
    task_ref: TaskRefParam = None
    lane_id: Annotated[str | None, Field(description="Optional lane scope.")] = None
    app_root: Annotated[str | None, Field(description="Optional application root scope.")] = None
    older_than_days: Annotated[int | None, Field(description="Optional age cutoff in days.")] = None


ArtifactsParam = Annotated[
    ArtifactsRecordOp | ArtifactsSearchOp | ArtifactsGetOp | ArtifactsPurgeOp,
    Field(discriminator="operation"),
]

_ARTIFACTS_ADAPTER: TypeAdapter[ArtifactsRecordOp | ArtifactsSearchOp | ArtifactsGetOp | ArtifactsPurgeOp] = (
    TypeAdapter(ArtifactsParam)
)


class CompactionRecordOp(BaseModel):
    operation: Literal["record"]
    transcript_path: Annotated[str, Field(description="Path to the transcript file to compact.")]
    task_ref: Annotated[str, Field(description="Task reference whose session is being compacted.")]
    harness: Annotated[
        CompactionHarnessInput,
        Field(
            description=(
                "Harness label that produced the session. Accepts the canonical labels in "
                "COMPACTION_HARNESS_CHOICES plus the 'cursor' alias, which is normalized to "
                "'vscode' by compaction.compact_session before storage."
            )
        ),
    ]
    session_id: Annotated[str, Field(description="Harness session identifier for the compacted transcript.")]


class CompactionGetOp(BaseModel):
    operation: Literal["get"]
    compaction_id: Annotated[str, Field(description="Stable compaction identifier to dereference.")]


class CompactionGetLatestOp(BaseModel):
    operation: Literal["get_latest"]
    task_ref: TaskRefParam = None


class CompactionDisableOp(BaseModel):
    """internal: persist a disable row in ``compaction_settings``.

    When ``task_ref`` is supplied, the disable is task-scoped and only
    silences the internal compaction surface for that task. When omitted,
    the workspace-default row is upserted and silences every task that
    has no explicit task-scoped row.
    """

    operation: Literal["disable"]
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class CompactionEnableOp(BaseModel):
    """internal: re-enable the internal compaction surface for a scope."""

    operation: Literal["enable"]
    task_ref: TaskRefParam = None
    actor: ActorParam = None


class CompactionStatusOp(BaseModel):
    """internal: report the resolved disable state plus row provenance."""

    operation: Literal["status"]
    task_ref: TaskRefParam = None


CompactionParam = Annotated[
    CompactionRecordOp
    | CompactionGetOp
    | CompactionGetLatestOp
    | CompactionDisableOp
    | CompactionEnableOp
    | CompactionStatusOp,
    Field(discriminator="operation"),
]

_COMPACTION_ADAPTER: TypeAdapter[
    CompactionRecordOp
    | CompactionGetOp
    | CompactionGetLatestOp
    | CompactionDisableOp
    | CompactionEnableOp
    | CompactionStatusOp
] = TypeAdapter(CompactionParam)


class TouchedFilesRecordOp(BaseModel):
    operation: Literal["record"]
    file_path: Annotated[str, Field(description="Monorepo-relative file path that was touched.")]
    change_kind: Annotated[
        Literal["edit", "add", "delete"],
        Field(
            description=(
                "Kind of change recorded for this file. One of: 'edit' = modified content "
                "(git 'M' / what you may call 'modified'); 'add' = new file (git 'A'); "
                "'delete' = removed file (git 'D'). Use 'edit' for any in-place modification — "
                "there is no separate 'modified' value."
            )
        ),
    ]
    session: Annotated[str | None, Field(description="Optional session id for the touch row.")] = None
    commit_sha: Annotated[str | None, Field(description="Optional override commit SHA for provenance.")] = None
    actor: ActorParam = None
    task_ref: TaskRefParam = None


class TouchedFilesListOp(BaseModel):
    operation: Literal["list"]
    task_ref: TaskRefParam = None
    limit: Annotated[int, Field(description="Maximum number of touched-file rows to return.")] = 20
    offset: Annotated[int, Field(description="Pagination offset for the touched-file list.")] = 0


TouchedFilesParam = Annotated[
    TouchedFilesRecordOp | TouchedFilesListOp,
    Field(discriminator="operation"),
]

_TOUCHED_FILES_ADAPTER: TypeAdapter[TouchedFilesRecordOp | TouchedFilesListOp] = TypeAdapter(TouchedFilesParam)


class IntegrityCheckWorkingTreeKind(BaseModel):
    kind: Literal["working_tree"]
    workspace_root: Annotated[
        str | None,
        Field(description="Optional workspace root override; defaults to runtime workspace."),
    ] = None
    expected_dirty: Annotated[
        list[str] | None,
        Field(description="Optional explicit allowlist of dirty paths; supplements .task-state/dirty-allowlist."),
    ] = None


class IntegrityCheckPostMergeKind(BaseModel):
    kind: Literal["post_merge"]
    merged_sha: Annotated[str, Field(description="Merge commit SHA to diff the working tree against.")]
    expected_changed_files: Annotated[
        list[str],
        Field(description="Paths permitted to differ from merged_sha after the fast-forward merge."),
    ] = []
    workspace_root: Annotated[
        str | None,
        Field(description="Optional workspace root override; defaults to runtime workspace."),
    ] = None


class IntegrityCheckCloseKind(BaseModel):
    kind: Literal["close"]
    task_ref: TaskRefParam = None
    allow_no_active_task: Annotated[
        bool,
        Field(description="Treat missing active task as a clean skip (ok=True, skipped=True)."),
    ] = False
    enforce: Annotated[
        bool,
        Field(description="Fail (ok=False) when any close-readiness sub-check fails."),
    ] = False
    require_fresh_tests: Annotated[
        bool,
        Field(description="Require a verified-test row at current_commit_sha for each touched file."),
    ] = False
    current_commit_sha: Annotated[
        str | None,
        Field(description="HEAD SHA used to gate fresh-test requirement and current-commit summary checks."),
    ] = None


IntegrityCheckParam = Annotated[
    IntegrityCheckWorkingTreeKind | IntegrityCheckPostMergeKind | IntegrityCheckCloseKind,
    Field(discriminator="kind"),
]

_INTEGRITY_CHECK_ADAPTER: TypeAdapter[
    IntegrityCheckWorkingTreeKind | IntegrityCheckPostMergeKind | IntegrityCheckCloseKind
] = TypeAdapter(IntegrityCheckParam)


class ArchiveOpArchive(BaseModel):
    operation: Literal["archive"]
    task_ref: TaskRefParam = None
    notes: Annotated[
        str | None,
        Field(description="Optional human-readable notes persisted on the archive row."),
    ] = None
    clear_active_if_matches: Annotated[
        bool,
        Field(description="Clear the active CURRENT_TASK pointer when it matches the archived task."),
    ] = True
    prune_working_rows: Annotated[
        bool,
        Field(
            description="Delete remaining working-row data for the archived task; requires allow_destructive_clear when non-empty."
        ),
    ] = False
    allow_destructive_clear: Annotated[
        bool,
        Field(description="Permit destructive clear when prune_working_rows would otherwise refuse non-empty rows."),
    ] = False
    cascade_maint_review: Annotated[
        bool,
        Field(description="Also archive linked internal-* children of this task."),
    ] = False
    tombstone_findings: Annotated[
        bool,
        Field(
            description="Supersede open review findings for the archived task with resolution_notes='task_archived'."
        ),
    ] = True


class ArchiveOpGc(BaseModel):
    operation: Literal["gc"]
    apply: Annotated[
        bool,
        Field(description="Apply the cascade-archive (mutating). Default is dry-run."),
    ] = False


class ArchiveOpReap(BaseModel):
    operation: Literal["reap"]
    apply: Annotated[
        bool,
        Field(description="Close and archive closeable rows (mutating). Default is dry-run."),
    ] = False
    task_ref: Annotated[
        str | None,
        Field(description="Optional task_ref scope; when set only that row is classified/reaped."),
    ] = None


class ArchiveOpReapScratch(BaseModel):
    operation: Literal["reap_scratch"]
    apply: Annotated[
        bool,
        Field(description="Close and archive reviewer-scratch rows with no open findings. Default is dry-run."),
    ] = False


class ArchiveOpReapDone(BaseModel):
    operation: Literal["reap_done"]
    apply: Annotated[
        bool,
        Field(description="Archive done non-scratch rows with no open findings. Default is dry-run."),
    ] = False


class ArchiveOpRetention(BaseModel):
    operation: Literal["retention"]
    apply: Annotated[
        bool,
        Field(description="Delete stale archive rows (mutating). Default is dry-run."),
    ] = False
    older_than_days: Annotated[
        int,
        Field(description="Age cutoff in days for task_archives rows.", ge=1),
    ] = 90


class ArchiveOpGet(BaseModel):
    operation: Literal["get"]
    task_ref: Annotated[str, Field(description="Archived task_ref to fetch.")]
    include_snapshot: Annotated[
        bool,
        Field(description="Include the parsed snapshot payload in the response."),
    ] = True


ArchiveParam = Annotated[
    ArchiveOpArchive
    | ArchiveOpGc
    | ArchiveOpReap
    | ArchiveOpReapScratch
    | ArchiveOpReapDone
    | ArchiveOpRetention
    | ArchiveOpGet,
    Field(discriminator="operation"),
]

_ARCHIVE_ADAPTER: TypeAdapter[
    ArchiveOpArchive
    | ArchiveOpGc
    | ArchiveOpReap
    | ArchiveOpReapScratch
    | ArchiveOpReapDone
    | ArchiveOpRetention
    | ArchiveOpGet
] = TypeAdapter(ArchiveParam)


def _validate_record_event(
    event: RecordEventParam,
) -> RecordDecisionEvent | RecordTestResultEvent | RecordErrorEvent | ReportBlockerEvent:
    return _RECORD_EVENT_ADAPTER.validate_python(event)


def _validate_validate(payload: ValidateParam) -> DecisionIdValidate | WriteValidate:
    return _VALIDATE_ADAPTER.validate_python(payload)


def _validate_review_runs(review: ReviewRunsParam) -> ReviewRunsRecordOp | ReviewRunsListOp | ReviewRunsCoverageOp:
    return _REVIEW_RUNS_ADAPTER.validate_python(review)


def _validate_terminal_guard_telemetry(
    telemetry: TerminalGuardTelemetryParam,
) -> TerminalGuardTelemetryRecordOp | TerminalGuardTelemetryListOp | TerminalGuardTelemetryReplayOp:
    return _TERMINAL_GUARD_TELEMETRY_ADAPTER.validate_python(telemetry)


def _validate_next_actions(
    action: NextActionsParam,
) -> NextActionsAddOp | NextActionsUpdateOp | NextActionsCompleteOp | NextActionsSkipOp | NextActionsListOp:
    return _NEXT_ACTIONS_ADAPTER.validate_python(action)


def _validate_artifacts(
    artifact: ArtifactsParam,
) -> ArtifactsRecordOp | ArtifactsSearchOp | ArtifactsGetOp | ArtifactsPurgeOp:
    return _ARTIFACTS_ADAPTER.validate_python(artifact)


def _validate_compaction(
    payload: CompactionParam,
) -> (
    CompactionRecordOp
    | CompactionGetOp
    | CompactionGetLatestOp
    | CompactionDisableOp
    | CompactionEnableOp
    | CompactionStatusOp
):
    return _COMPACTION_ADAPTER.validate_python(payload)


def _validate_touched_files(
    payload: TouchedFilesParam,
) -> TouchedFilesRecordOp | TouchedFilesListOp:
    return _TOUCHED_FILES_ADAPTER.validate_python(payload)


def _validate_integrity_check(
    payload: IntegrityCheckParam,
) -> IntegrityCheckWorkingTreeKind | IntegrityCheckPostMergeKind | IntegrityCheckCloseKind:
    return _INTEGRITY_CHECK_ADAPTER.validate_python(payload)


def _validate_archive(
    payload: ArchiveParam,
) -> (
    ArchiveOpArchive
    | ArchiveOpGc
    | ArchiveOpReap
    | ArchiveOpReapScratch
    | ArchiveOpReapDone
    | ArchiveOpRetention
    | ArchiveOpGet
):
    return _ARCHIVE_ADAPTER.validate_python(payload)


def set_handoff_state(
    task_ref: Annotated[
        str, Field(description="Task reference whose active handoff state should be created or updated.")
    ],
    objective: Annotated[
        str | None,
        Field(description="Task objective. Required only when creating a brand-new handoff state row."),
    ] = None,
    focus: Annotated[
        str | None,
        Field(description="Current working focus for the task. Omit to keep the existing focus."),
    ] = None,
    status: Annotated[
        str,
        Field(description="Active handoff status, typically in_progress, blocked, review, or done."),
    ] = "in_progress",
    expected_revision: Annotated[
        int | None,
        Field(description="Optimistic concurrency guard. Required when updating an existing handoff row."),
    ] = None,
    actor: ActorParam = None,
    target_branch: Annotated[
        str | None,
        Field(description="Target git branch for this task. Preserved on update when omitted."),
    ] = None,
    target_worktree_path: Annotated[
        str | None,
        Field(
            description=(
                "Absolute filesystem path of the linked worktree where this task should be implemented. "
                "Used by `make context` and write-side guards to fail-fast when an agent runs from the wrong "
                "directory. Preserved on update when omitted."
            )
        ),
    ] = None,
    task_plan_path: Annotated[
        str | None,
        Field(
            description=(
                "Workspace-relative or absolute task-plan path for this task. "
                "Preserved on update when omitted; pass an empty string to clear it."
            )
        ),
    ] = None,
    event_id: Annotated[
        str | None,
        Field(description="Optional projection event id. Duplicate event ids are accepted as idempotent no-ops."),
    ] = None,
    status_only: Annotated[
        bool,
        Field(
            description=(
                "When True, only update the task status (active row or archived snapshot) without "
                "touching objective/focus or recording a slice decision. Routes through the "
                "three-path concurrency contract: active done elides expected_revision, "
                "active mid-lifecycle requires expected_revision, archived snapshot is revisionless."
            )
        ),
    ] = False,
) -> dict:
    if status_only:
        result = _core_update_task_status(
            task_ref=task_ref,
            status=status,
            expected_revision=expected_revision,
            actor=_dump_actor(actor),
        )
    else:
        result = _core_set_handoff_state(
            task_ref=task_ref,
            objective=objective,
            focus=focus,
            status=status,
            expected_revision=expected_revision,
            actor=_dump_actor(actor),
            target_branch=target_branch,
            target_worktree_path=target_worktree_path,
            task_plan_path=task_plan_path,
            event_id=event_id,
        )
    if isinstance(result, dict) and result.get("ok"):
        _run_opportunistic_integrate_for_task(task_ref)
    return result


def reconcile_reviewer_scratch_findings_gc(
    apply: Annotated[
        bool,
        Field(description="Retire eligible reviewer-scratch findings (mutating). Default is dry-run."),
    ] = False,
) -> dict:
    """Bulk-retire open reviewer-scratch findings by looping per-coordinator reconcile."""
    return core.reconcile_reviewer_scratch_findings_gc(apply=apply)


def collect_stale_nonscratch_open_findings(
    apply: Annotated[
        bool,
        Field(
            description="Supersede collected stale findings and skip stale pending next_actions. Default is dry-run."
        ),
    ] = False,
    batch_size: Annotated[
        int,
        Field(description="Maximum number of stale findings to collect/apply in one call.", ge=1),
    ] = 200,
) -> dict:
    """Report or supersede open findings on non-scratch refs with no live row or status=done."""
    return core.collect_stale_nonscratch_open_findings(apply=apply, batch_size=batch_size)


def integrate_review_findings(
    task_ref: Annotated[
        str | None,
        Field(description="Task reference whose resolved_on_branch findings should be promoted to integrated."),
    ] = None,
    integration_ref: Annotated[
        str,
        Field(description="Git ref representing the integration branch HEAD (default: 'main')."),
    ] = "main",
    actor: ActorParam = None,
) -> dict:
    """internal: lifecycle promotion entry point. Promotes every
    ``resolved_on_branch`` finding for the task whose anchor commit is reachable
    from ``integration_ref`` HEAD to ``status='integrated'``. Distinct from
    internal's ``reconcile_review_findings``."""
    return _core_integrate_review_findings(
        task_ref=task_ref,
        integration_ref=integration_ref,
        actor=_dump_actor(actor),
    )


def update_task_status(
    task_ref: Annotated[
        str,
        Field(description="Task reference whose status should be updated, whether active or archived."),
    ],
    status: Annotated[
        Literal["in_progress", "blocked", "review", "done"],
        Field(description="New task status to persist on the active row or archived snapshot."),
    ],
    expected_revision: Annotated[
        int | None,
        Field(description="Optimistic concurrency guard for active-task updates. Not used for archived-task updates."),
    ] = None,
    actor: ActorParam = None,
) -> dict:
    return _core_update_task_status(
        task_ref=task_ref,
        status=status,
        expected_revision=expected_revision,
        actor=_dump_actor(actor),
    )


def record_decision(
    session: Annotated[str, Field(description="Session identifier for the decision write.")],
    decision: Annotated[str, Field(description=DECISION_ID_DESCRIPTION)],
    rationale: Annotated[
        str | None,
        Field(description="Optional markdown rationale explaining the decision, verification, and open threads."),
    ] = None,
    actor: ActorParam = None,
    task_ref: TaskRefParam = None,
    event_id: Annotated[
        str | None,
        Field(description="Optional projection event id. Duplicate event ids are accepted as idempotent no-ops."),
    ] = None,
    input_tokens: Annotated[int | None, Field(description="Optional prompt token count for this slice.")] = None,
    output_tokens: Annotated[int | None, Field(description="Optional completion token count for this slice.")] = None,
    total_tokens: Annotated[int | None, Field(description="Optional total token count for this slice.")] = None,
    changed_files: DecisionChangedFilesParam = None,
) -> dict:
    return _core_record_decision(
        session=session,
        decision=decision,
        rationale=rationale,
        actor=_dump_actor(actor),
        task_ref=task_ref,
        event_id=event_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        changed_files=changed_files,
    )


def validate(
    payload: Annotated[
        ValidateParam,
        Field(description="Validation request. payload.kind selects the decision_id or write variant."),
    ],
) -> dict:
    """Side-effect-free preflight router.

    ``kind="decision_id"`` runs the decision-id grammar/category check
    used by the slice-complete write path. ``kind="write"`` runs the
    write-contract registry preflight (required fields, field grammars,
    variant selection). Both return the v2 envelope with the underlying
    helper's result exposed under ``data``.
    """

    op = _validate_validate(payload)
    if isinstance(op, DecisionIdValidate):
        result = _validate_decision_id_helper(decision=op.decision, decision_kind=op.decision_kind)
    else:
        from .write_contracts import validate_write as _validate_write_helper

        result = _validate_write_helper(op.tool_name, op.payload)
    return core._envelope(
        ok=bool(result.get("ok")),
        tool="validate",
        data=result,
    )


def validate_decision_id(
    decision: str,
    decision_kind: str | None = None,
) -> dict:
    """Backward-compatible alias dispatching through the consolidated ``validate`` tool."""

    return validate(cast(ValidateParam, {"kind": "decision_id", "decision": decision, "decision_kind": decision_kind}))


def validate_write(tool_name: str, payload: dict) -> dict:
    """Backward-compatible alias dispatching through the consolidated ``validate`` tool."""

    return validate(cast(ValidateParam, {"kind": "write", "tool_name": tool_name, "payload": payload}))


def record_event(
    event: Annotated[
        RecordEventParam,
        Field(
            description="Typed event payload. event_kind selects one of the decision, test_result, or blocker variants."
        ),
    ],
) -> dict:
    event_payload = _validate_record_event(event)
    resolved_actor = _dump_actor(event_payload.actor)
    if isinstance(event_payload, RecordDecisionEvent):
        result = _core_record_decision(
            session=event_payload.session,
            decision=event_payload.decision,
            rationale=event_payload.rationale,
            actor=resolved_actor,
            task_ref=event_payload.task_ref,
            event_id=event_payload.event_id,
            input_tokens=event_payload.input_tokens,
            output_tokens=event_payload.output_tokens,
            total_tokens=event_payload.total_tokens,
            changed_files=event_payload.changed_files,
        )
    elif isinstance(event_payload, RecordTestResultEvent):
        result = _core_record_test_result(
            session=event_payload.session,
            command=event_payload.command,
            passed=event_payload.passed,
            result=event_payload.result,
            traces=event_payload.traces,
            exit_code=event_payload.exit_code,
            actor=resolved_actor,
            task_ref=event_payload.task_ref,
            event_id=event_payload.event_id,
        )
    elif isinstance(event_payload, RecordErrorEvent):
        result = _core_record_agent_error(
            error_class=event_payload.error_class,
            summary=event_payload.summary,
            detail=event_payload.detail,
            tool_name=event_payload.tool_name,
            command_preview=event_payload.command_preview,
            package_name=event_payload.package_name,
            package_version=event_payload.package_version,
            task_ref=event_payload.task_ref,
        )
    else:
        result = _core_report_blocker(
            operation=event_payload.operation,
            description=event_payload.description,
            blocker_id=event_payload.blocker_id,
            actor=resolved_actor,
            task_ref=event_payload.task_ref,
        )
    if isinstance(result, dict) and result.get("ok"):
        _run_opportunistic_integrate_for_task(event_payload.task_ref)
    return result


def update_next_actions(
    operation: Annotated[
        Literal["add", "update", "complete", "skip"],
        Field(description="Mutation to apply to the next-actions table."),
    ],
    action_id: Annotated[
        int | None,
        Field(description="Existing action id. Required for update, complete, and skip operations."),
    ] = None,
    action: Annotated[
        str | None,
        Field(description="Action text. Required for add; optional replacement text for update."),
    ] = None,
    priority: Annotated[
        int | None,
        Field(description="Optional priority value. Lower numbers sort first; defaults to 100 on add."),
    ] = None,
    status: Annotated[
        Literal["pending", "done", "skipped"] | None,
        Field(description="Optional explicit status override. Only used for update operations."),
    ] = None,
    actor: ActorParam = None,
    task_ref: TaskRefParam = None,
) -> dict:
    return _core_update_next_actions(
        operation=operation,
        action_id=action_id,
        action=action,
        priority=priority,
        status=status,
        actor=_dump_actor(actor),
        task_ref=task_ref,
    )


def next_actions(
    action: Annotated[
        NextActionsParam,
        Field(
            description="Typed next-actions payload. operation selects one of the list, add, update, complete, or skip variants."
        ),
    ],
) -> dict:
    action_payload = _validate_next_actions(action)
    if isinstance(action_payload, NextActionsListOp):
        return list_next_actions(
            task_ref=action_payload.task_ref,
            lane_id=action_payload.lane_id,
            status=action_payload.status,
            limit=action_payload.limit,
            offset=action_payload.offset,
        )
    return _core_update_next_actions(
        operation=action_payload.operation,
        action_id=getattr(action_payload, "action_id", None),
        action=getattr(action_payload, "action", None),
        priority=getattr(action_payload, "priority", None),
        status=getattr(action_payload, "status", None),
        actor=_dump_actor(getattr(action_payload, "actor", None)),
        task_ref=getattr(action_payload, "task_ref", None),
    )


def record_test_result(
    session: Annotated[str, Field(description="Session identifier for the verification run.")],
    command: Annotated[str, Field(description="Verification command that was executed.")],
    passed: Annotated[bool, Field(description="Whether the verification command passed.")],
    result: Annotated[
        str | None,
        Field(description="Optional stdout or summarized verification evidence for the command."),
    ] = None,
    traces: Annotated[
        list[str] | None,
        Field(description="Optional raw verification trace payloads to archive alongside the summarized result."),
    ] = None,
    exit_code: Annotated[int | None, Field(description="Optional process exit code for the command.")] = None,
    actor: ActorParam = None,
    task_ref: TaskRefParam = None,
    event_id: Annotated[
        str | None,
        Field(description="Optional projection event id. Duplicate event ids are accepted as idempotent no-ops."),
    ] = None,
) -> dict:
    return _core_record_test_result(
        session=session,
        command=command,
        passed=passed,
        result=result,
        traces=traces,
        exit_code=exit_code,
        actor=_dump_actor(actor),
        task_ref=task_ref,
        event_id=event_id,
    )


def get_verified_tests(
    task_ref: TaskRefParam = None,
    lane_id: Annotated[str | None, Field(description="Optional lane filter.")] = None,
    branch: Annotated[str | None, Field(description="Optional branch filter.")] = None,
    commit_sha: Annotated[str | None, Field(description="Optional commit SHA filter.")] = None,
    passed: Annotated[bool | None, Field(description="Optional pass/fail filter.")] = None,
    include_traces: Annotated[bool, Field(description="When true, include raw archived traces in each row.")] = False,
    correlated_file: Annotated[
        str | None,
        Field(description="Optional monorepo-relative file path used to correlate tests to changed_files decisions."),
    ] = None,
    correlation_window_minutes: Annotated[
        int,
        Field(
            description="Absolute decision/test time window used for changed-file correlation when commit SHAs are missing."
        ),
    ] = 120,
    exclude_never_passed: Annotated[
        bool,
        Field(description="When true, omit commands that never recorded a passing row in the filtered result set."),
    ] = False,
    limit: Annotated[int, Field(description="Maximum number of tests to return.")] = 100,
    offset: Annotated[int, Field(description="Pagination offset.")] = 0,
) -> dict:
    return core.get_verified_tests(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        commit_sha=commit_sha,
        passed=passed,
        include_traces=include_traces,
        correlated_file=correlated_file,
        correlation_window_minutes=correlation_window_minutes,
        exclude_never_passed=exclude_never_passed,
        limit=limit,
        offset=offset,
    )


def compaction(
    payload: Annotated[
        CompactionParam,
        Field(
            description=(
                "Typed compaction payload. operation selects one of the record, get, get_latest, "
                "disable, enable, or status variants (internal added the disable/enable/status trio)."
            )
        ),
    ],
) -> StructuredSummary | _compaction_module.CompactionRecord | dict | None:
    op = _validate_compaction(payload)
    if isinstance(op, CompactionRecordOp):
        receipt = _compaction_module.compact_session(
            transcript_path=op.transcript_path,
            task_ref=op.task_ref,
            harness=op.harness,
            session_id=op.session_id,
        )
        return receipt.model_dump(mode="json")
    if isinstance(op, CompactionGetOp):
        return _compaction_module.get_compaction(op.compaction_id)
    if isinstance(op, CompactionGetLatestOp):
        return _compaction_module.get_latest_compaction(task_ref=op.task_ref)
    if isinstance(op, CompactionDisableOp):
        actor = _dump_actor(op.actor)
        actor_label = actor.get("agent") if isinstance(actor, dict) else None
        disable_receipt = _compaction_module.set_compaction_enabled(
            enabled=False, task_ref=op.task_ref, actor=actor_label
        )
        return disable_receipt.model_dump(mode="json")
    if isinstance(op, CompactionEnableOp):
        actor = _dump_actor(op.actor)
        actor_label = actor.get("agent") if isinstance(actor, dict) else None
        enable_receipt = _compaction_module.set_compaction_enabled(
            enabled=True, task_ref=op.task_ref, actor=actor_label
        )
        return enable_receipt.model_dump(mode="json")
    # status
    status_receipt = _compaction_module.get_compaction_status(task_ref=op.task_ref)
    return status_receipt.model_dump(mode="json")


def semantic_reinjection_packet(
    task_ref: TaskRefParam = None,
    source: Annotated[str | None, Field(description="Optional caller source for orientation-read telemetry.")] = None,
    session: Annotated[
        str | None, Field(description="Optional caller session id for orientation-read telemetry.")
    ] = None,
    anchor_texts: Annotated[
        list[str] | None,
        Field(description="Slice anchor texts (changed_files paths or rationale excerpts); repeatable."),
    ] = None,
    semantic_content_budget_chars: Annotated[
        int | None,
        Field(description="Optional character budget override for semantic content selection."),
    ] = None,
) -> dict[str, object]:
    result = _compaction_module.semantic_reinjection_packet(
        task_ref=task_ref,
        anchor_texts=anchor_texts,
        semantic_content_budget_chars=semantic_content_budget_chars,
    )
    record_orientation_read_once(
        tool="semantic_reinjection_packet",
        task_ref=str(result.get("task_ref") or task_ref or ""),
        resolution_outcome="explicit" if task_ref is not None else "resolved",
        source=source,
        session=session,
        read_profile="semantic_reinjection_packet",
    )
    return result


def get_compaction(
    compaction_id: Annotated[str, Field(description="Stable compaction identifier to dereference.")],
) -> _compaction_module.CompactionRecord:
    result = compaction(cast(CompactionParam, {"operation": "get", "compaction_id": compaction_id}))
    assert isinstance(result, _compaction_module.CompactionRecord)
    return result


# internal: the bare-string ``api.compact_session`` wrapper was
# deleted after the internal caller audit (decision id 662) confirmed no
# external caller depended on it. Internal callers now use the typed
# implementation ``workbay_handoff_mcp.compaction.compact_session`` directly
# (re-exported on the package). It returns ``CompactionRecordReceipt``;
# the bare ``compaction_id`` string lives at ``receipt.compaction_id``.
# To call this through the MCP layer, use the typed op
# ``compaction(operation="record", ...)`` which now returns the receipt
# as ``receipt.model_dump(mode="json")``.
compact_session = _compaction_module.compact_session


def get_latest_compaction(
    task_ref: TaskRefParam = None,
) -> _compaction_module.CompactionRecord | None:
    result = compaction(cast(CompactionParam, {"operation": "get_latest", "task_ref": task_ref}))
    assert result is None or isinstance(result, _compaction_module.CompactionRecord)
    return result


def touched_files(
    payload: Annotated[
        TouchedFilesParam,
        Field(description="Typed touched-files payload. operation selects record or list."),
    ],
) -> dict:
    op = _validate_touched_files(payload)
    if isinstance(op, TouchedFilesRecordOp):
        envelope = core.record_file_touch(
            file_path=op.file_path,
            change_kind=op.change_kind,
            session=op.session,
            commit_sha=op.commit_sha,
            actor=_dump_actor(op.actor),
            task_ref=op.task_ref,
        )
    else:
        envelope = core.get_touched_files(
            task_ref=op.task_ref,
            limit=op.limit,
            offset=op.offset,
        )
    envelope["tool"] = "touched_files"
    return envelope


def integrity_check(
    payload: Annotated[
        IntegrityCheckParam,
        Field(description="Typed integrity-check payload. kind selects working_tree, post_merge, or close."),
    ],
) -> dict:
    op = _validate_integrity_check(payload)
    if isinstance(op, IntegrityCheckWorkingTreeKind):
        envelope = core.working_tree_integrity_check(
            workspace_root=op.workspace_root,
            expected_dirty=op.expected_dirty,
        )
    elif isinstance(op, IntegrityCheckPostMergeKind):
        envelope = core.post_merge_integrity_check(
            merged_sha=op.merged_sha,
            expected_changed_files=op.expected_changed_files,
            workspace_root=op.workspace_root,
        )
    else:
        envelope = core.handoff_close_check(
            task_ref=op.task_ref,
            allow_no_active_task=op.allow_no_active_task,
            enforce=op.enforce,
            require_fresh_tests=op.require_fresh_tests,
            current_commit_sha=op.current_commit_sha,
        )
        # internal: close-side requirements-on-read. The 19×
        # close_slice XML-tag class has no preceding finding read, so
        # surface rationale ## sections + the XML prohibition here — the
        # gate agents read before closing.
        data = envelope.get("data")
        if isinstance(data, dict):
            from .write_contracts import close_slice_requirements  # noqa: PLC0415

            data["close_slice_requirements"] = close_slice_requirements()
    envelope["tool"] = "integrity_check"
    return envelope


def archive(
    payload: Annotated[
        ArchiveParam,
        Field(description="Typed archive payload. operation selects archive, gc, reap, retention, or get."),
    ],
) -> dict:
    op = _validate_archive(payload)
    if isinstance(op, ArchiveOpArchive):
        envelope = core.archive_task_state(
            task_ref=op.task_ref,
            notes=op.notes,
            clear_active_if_matches=op.clear_active_if_matches,
            prune_working_rows=op.prune_working_rows,
            allow_destructive_clear=op.allow_destructive_clear,
            cascade_maint_review=op.cascade_maint_review,
            tombstone_findings=op.tombstone_findings,
        )
    elif isinstance(op, ArchiveOpGc):
        envelope = core.tasks_gc(apply=op.apply)
    elif isinstance(op, ArchiveOpReap):
        envelope = core.reap_tasks(apply=op.apply, task_ref=op.task_ref)
    elif isinstance(op, ArchiveOpReapScratch):
        envelope = core.reap_scratch_handoff_rows(apply=op.apply)
    elif isinstance(op, ArchiveOpReapDone):
        envelope = core.reap_done_nonscratch_handoff_rows(apply=op.apply)
    elif isinstance(op, ArchiveOpRetention):
        envelope = core.archives_retention_gc(apply=op.apply, older_than_days=op.older_than_days)
    else:
        envelope = core.get_archived_task(
            task_ref=op.task_ref,
            include_snapshot=op.include_snapshot,
        )
    envelope["tool"] = "archive"
    return envelope


def report_blocker(
    operation: Annotated[
        Literal["add", "resolve", "reopen"],
        Field(description="Blocker mutation to perform."),
    ],
    description: Annotated[
        str | None,
        Field(description="Blocker description. Required when adding a new blocker."),
    ] = None,
    blocker_id: Annotated[
        int | None,
        Field(description="Existing blocker id. Required for resolve and reopen."),
    ] = None,
    actor: ActorParam = None,
    task_ref: TaskRefParam = None,
) -> dict:
    return _core_report_blocker(
        operation=operation,
        description=description,
        blocker_id=blocker_id,
        actor=_dump_actor(actor),
        task_ref=task_ref,
    )


def update_review_finding(
    status: Annotated[
        Literal[
            "open",
            "fixed",
            "deferred",
            "wontfix",
            "resolved_on_branch",
            "integrated",
            "superseded",
        ],
        Field(
            description=(
                "New finding status to apply. 'integrated' and 'superseded' are managed "
                "statuses and are rejected by the update path with operator guidance."
            )
        ),
    ],
    finding_id: Annotated[
        str | None,
        Field(description="Stable finding identifier. Provide this or finding_db_id."),
    ] = None,
    finding_db_id: Annotated[
        int | None,
        Field(description="Numeric database id alternative to finding_id."),
    ] = None,
    resolution_notes: Annotated[
        str | None,
        Field(description="Optional notes describing how the finding was resolved or dispositioned."),
    ] = None,
    reopen_reason: Annotated[
        str | None,
        Field(description="Required when moving a non-open finding back to open."),
    ] = None,
    task_ref: TaskRefParam = None,
    session: Annotated[str | None, Field(description="Optional session identifier for the update.")] = None,
    actor: ActorParam = None,
    verified_commit_sha: Annotated[
        str | None,
        Field(description="Optional commit SHA that verified a fixed finding."),
    ] = None,
    verification_evidence: Annotated[
        str | None,
        Field(description="Optional verification evidence used when closing a finding as fixed."),
    ] = None,
) -> dict:
    return _core_update_review_finding(
        status=status,
        finding_id=finding_id,
        finding_db_id=finding_db_id,
        resolution_notes=resolution_notes,
        reopen_reason=reopen_reason,
        task_ref=task_ref,
        session=session,
        actor=_dump_actor(actor),
        verified_commit_sha=verified_commit_sha,
        verification_evidence=verification_evidence,
    )


def record_review_run(
    review_run_id: Annotated[str, Field(description="Globally unique review run identifier.")],
    session: Annotated[str, Field(description="Session identifier for the review run.")],
    subject_path: Annotated[str, Field(description="Workspace-relative path reviewed in this run.")],
    subject_kind: Annotated[
        Literal["task_plan", "epic", "branch", "adr", "roadmap", "other"],
        Field(description="Kind of artifact reviewed in this run."),
    ] = "task_plan",
    review_mode: Annotated[str, Field(description="Review mode label, for example planning or branch.")] = "planning",
    verdict: Annotated[
        Literal["pass", "pass_with_findings", "fail", "conditional_pass"] | None,
        Field(description="Optional review verdict to store with the run."),
    ] = None,
    verdict_decision: Annotated[
        str | None,
        Field(description="Optional decision id or summary that explains the verdict."),
    ] = None,
    task_ref: TaskRefParam = None,
    actor: ActorParam = None,
) -> dict:
    return _core_record_review_run(
        review_run_id=review_run_id,
        session=session,
        subject_path=subject_path,
        subject_kind=subject_kind,
        review_mode=review_mode,
        verdict=verdict,
        verdict_decision=verdict_decision,
        task_ref=task_ref,
        actor=_dump_actor(actor),
    )


def review_runs(
    review: Annotated[
        ReviewRunsParam,
        Field(
            description="Typed review-runs payload. operation selects one of the record, list, or coverage variants."
        ),
    ],
) -> dict:
    review_payload = _validate_review_runs(review)
    if isinstance(review_payload, ReviewRunsRecordOp):
        return _core_record_review_run(
            review_run_id=review_payload.review_run_id,
            session=review_payload.session,
            subject_path=review_payload.subject_path,
            subject_kind=review_payload.subject_kind,
            review_mode=review_payload.review_mode,
            verdict=review_payload.verdict,
            verdict_decision=review_payload.verdict_decision,
            task_ref=review_payload.task_ref,
            actor=_dump_actor(review_payload.actor),
        )
    if isinstance(review_payload, ReviewRunsListOp):
        return list_review_runs(
            task_ref=review_payload.task_ref,
            subject_path=review_payload.subject_path,
            limit=review_payload.limit,
            offset=review_payload.offset,
            review_mode=review_payload.review_mode,
            verdict=review_payload.verdict,
        )
    return get_review_coverage(
        task_ref=review_payload.task_ref,
        subject_path=review_payload.subject_path,
    )


def terminal_guard_telemetry(
    telemetry: Annotated[
        TerminalGuardTelemetryParam,
        Field(
            description="Typed terminal-guard telemetry payload. operation selects one of the record, list, or replay variants."
        ),
    ],
) -> dict:
    telemetry_payload = _validate_terminal_guard_telemetry(telemetry)
    if isinstance(telemetry_payload, TerminalGuardTelemetryRecordOp):
        return record_terminal_guard_event(
            task_ref=telemetry_payload.task_ref,
            worktree_path=telemetry_payload.worktree_path,
            harness=telemetry_payload.harness,
            tool_name=telemetry_payload.tool_name,
            decision=telemetry_payload.decision,
            trigger=telemetry_payload.trigger,
            native_tool_hint=telemetry_payload.native_tool_hint,
            command_preview=telemetry_payload.command_preview,
            policy_version=telemetry_payload.policy_version,
            policy_source=telemetry_payload.policy_source,
            fallback_source=telemetry_payload.fallback_source,
            created_at=telemetry_payload.created_at,
        )
    if isinstance(telemetry_payload, TerminalGuardTelemetryReplayOp):
        return replay_terminal_guard_spool(spool_path=telemetry_payload.spool_path)
    return list_terminal_guard_events(
        task_ref=telemetry_payload.task_ref,
        decision=telemetry_payload.decision,
        harness=telemetry_payload.harness,
        tool_name=telemetry_payload.tool_name,
        limit=telemetry_payload.limit,
        offset=telemetry_payload.offset,
    )


def artifacts(
    artifact: Annotated[
        ArtifactsParam,
        Field(
            description="Typed artifacts payload. operation selects one of the record, search, get, or purge variants."
        ),
    ],
) -> dict:
    artifact_payload = _validate_artifacts(artifact)
    if isinstance(artifact_payload, ArtifactsRecordOp):
        return record_artifact(
            source_kind=artifact_payload.source_kind,
            source_label=artifact_payload.source_label,
            content=artifact_payload.content,
            task_ref=artifact_payload.task_ref,
            lane_id=artifact_payload.lane_id,
            app_root=artifact_payload.app_root,
            content_type=artifact_payload.content_type,
            summary=artifact_payload.summary,
            metadata=artifact_payload.metadata,
        )
    if isinstance(artifact_payload, ArtifactsSearchOp):
        return search_artifacts(
            queries=artifact_payload.queries,
            task_ref=artifact_payload.task_ref,
            lane_id=artifact_payload.lane_id,
            app_root=artifact_payload.app_root,
            source_kind=artifact_payload.source_kind,
            content_type=artifact_payload.content_type,
            limit=artifact_payload.limit,
            offset=artifact_payload.offset,
            detail=artifact_payload.detail,
            fields=artifact_payload.fields,
        )
    if isinstance(artifact_payload, ArtifactsGetOp):
        return get_artifact(
            source_id=artifact_payload.source_id,
            task_ref=artifact_payload.task_ref,
            source_label=artifact_payload.source_label,
            include_terms=artifact_payload.include_terms,
            top_n_terms=artifact_payload.top_n_terms,
            detail=artifact_payload.detail,
            fields=artifact_payload.fields,
        )
    return purge_artifacts(
        task_ref=artifact_payload.task_ref,
        lane_id=artifact_payload.lane_id,
        app_root=artifact_payload.app_root,
        older_than_days=artifact_payload.older_than_days,
    )


def close_slice(
    session: Annotated[str, Field(description="Session identifier for the slice completion write.")],
    decision: Annotated[str | None, Field(description=SLICE_COMPLETE_DECISION_ID_DESCRIPTION)] = None,
    rationale: Annotated[
        str | None,
        Field(
            description=(
                "Structured markdown rationale. MUST include non-empty sections: "
                "## Changes, ## Verification, ## Schema / Contract Changes, ## Open Threads. "
                "Soft limit: 1,500 chars; hard limit: 4,000 chars (enforced by hook before call reaches server). "
                "See docs/workbay/templates/slice-complete-template.md."
            )
        ),
    ] = None,
    actor: ActorParam = None,
    expected_revision: Annotated[
        int | None,
        Field(description="Optimistic concurrency guard passed to the nested handoff-state update."),
    ] = None,
    task_ref: TaskRefParam = None,
    focus: Annotated[
        str | None,
        Field(description="Optional new focus to store on the task after recording the decision."),
    ] = None,
    author_tag: Annotated[
        str | None,
        Field(description="Optional author tag used to compose the canonical slice-complete decision id."),
    ] = None,
    work_ref: Annotated[
        str | None,
        Field(description="Optional work reference used to compose the canonical slice-complete decision id."),
    ] = None,
    slug: Annotated[
        str | None,
        Field(description="Optional slug used to compose the canonical slice-complete decision id."),
    ] = None,
    changed_files: DecisionChangedFilesParam = None,
    slice_number: Annotated[
        int | None,
        Field(
            description=(
                "Optional integer slice number from the plan heading "
                "### Checklist for Slice N. Stored on the decision row for plan-checklist sync."
            )
        ),
    ] = None,
) -> dict:
    # internal / Layer 2 of the XML-in-rationale bug class eradication.
    # Reject rationale strings that contain XML-like <actor> or
    # <changed_files> tags — these indicate the caller accidentally
    # embedded the top-level `actor` and `changed_files` parameters
    # inside the rationale string instead of passing them as separate
    # JSON fields. The misattribution is silent (the decision row gets
    # default actor provenance) and the resulting audit trail is polluted
    # with two superseded rows, as happened with internal decisions
    # #1507 and #1508.
    if rationale is not None:
        # internal: case-insensitive scan so <ACTOR>, <Actor>, etc.
        # are caught. The anti-pattern is structural (XML tags inside a
        # markdown rationale), not case-dependent. Constant lives next to
        # close_slice_requirements() so requirements-on-read and the guard
        # share a single source (internal).
        from .write_contracts import CLOSE_SLICE_RATIONALE_XML_ANTI_PATTERNS  # noqa: PLC0415

        rationale_lower = rationale.lower()
        for tag in CLOSE_SLICE_RATIONALE_XML_ANTI_PATTERNS:
            if tag in rationale_lower:
                from .structured_rejections import (  # noqa: PLC0415
                    rejection_close_slice_rationale_xml,
                )

                return core._envelope(
                    ok=False,
                    tool="close_slice",
                    data=rejection_close_slice_rationale_xml(
                        rejected_tag=tag,
                        task_ref=task_ref,
                    ),
                    task_ref=task_ref,
                )
    resolved_decision, decision_error = _resolve_close_slice_decision(
        decision=decision,
        author_tag=author_tag,
        work_ref=work_ref,
        slug=slug,
    )
    if decision_error is not None:
        return core._envelope(
            ok=False,
            tool="close_slice",
            data={
                "error": decision_error,
                "state_error": decision_error,
                "decision_recorded": False,
                "state_updated": False,
                "current_task_md_written": False,
            },
            task_ref=task_ref,
        )
    result = _core_close_slice(
        session=session,
        decision=str(resolved_decision),
        rationale=rationale,
        actor=_dump_actor(actor),
        expected_revision=expected_revision,
        task_ref=task_ref,
        focus=focus,
        changed_files=changed_files,
        slice_number=slice_number,
    )
    if isinstance(result, dict) and result.get("ok"):
        _run_opportunistic_integrate_for_task(task_ref)
    return result


def _apply_tool_descriptions() -> None:
    for name, description in TOOL_DESCRIPTIONS.items():
        tool = globals().get(name)
        if tool is None:
            continue
        existing = getattr(tool, "__doc__", None)
        if existing and existing.strip():
            continue
        tool.__doc__ = description


@dataclass
class ArgSpec:
    """Declarative specification for a single CLI argument."""

    name: str
    type: type = str
    default: Any = None
    required: bool = False
    help: str = ""
    choices: list[str] | None = None
    action: str | None = None  # e.g. "store_true", "append"
    nargs: str | None = None
    dest: str | None = None  # override argparse dest


# Choices used by both the tool registry and CLI for worker reasoning effort.
_WORKER_REASONING_EFFORT_CHOICES = ("inherit", "auto", "low", "medium", "high", "xhigh")


@dataclass
class ToolEntry:
    """Registry entry for a single MCP tool."""

    name: str
    handler: Callable[..., Any]
    description: str
    cli_args: list[ArgSpec] = field(default_factory=list)  # CLI argument specs (single source of truth)
    cli_name: str | None = None  # CLI subcommand name; None = no CLI exposure
    deprecated_since: str | None = None  # Version string; non-None appends [DEPRECATED] to description
    profile: str = "core"  # "core" | "extended" — controls which MCP surface the tool is included in
    surface_class: str = "action"  # "query" | "action" | "generator" — matches contract taxonomy
    entity_family: str = (
        "handoff_state"  # "handoff_state" | "review_findings" | "review_runs" | "artifacts" | "session" | "lifecycle"
    )


def _task_state_tool_entries() -> list[ToolEntry]:
    return [
        # Task state (3)
        ToolEntry(
            "set_handoff_state",
            set_handoff_state,
            TOOL_DESCRIPTIONS["set_handoff_state"],
            cli_name="set",
            cli_args=[
                ArgSpec("--task-ref", required=True),
                ArgSpec("--objective", help="Task objective."),
                ArgSpec("--focus", help="Mutable current-focus text."),
                ArgSpec("--status", default="in_progress"),
                ArgSpec("--expected-revision", type=int),
                ArgSpec(
                    "--target-branch",
                    help="Target git branch for this task. Preserved on update when omitted.",
                ),
                ArgSpec(
                    "--target-worktree-path",
                    help=(
                        "Absolute filesystem path of the linked worktree where this task "
                        "should be implemented. Preserved on update when omitted."
                    ),
                ),
                ArgSpec(
                    "--task-plan-path",
                    help=(
                        "Workspace-relative or absolute task-plan path. "
                        "Preserved on update when omitted; pass an empty string to clear."
                    ),
                ),
                ArgSpec(
                    "--event-id",
                    help="Projection event id. Duplicate event ids are accepted as idempotent no-ops.",
                ),
                ArgSpec(
                    "--status-only",
                    action="store_true",
                    help=(
                        "Update only the task status (active row or archived snapshot) "
                        "without touching objective/focus or recording a slice decision."
                    ),
                ),
                ArgSpec(
                    "--commit-sha",
                    help=(
                        "Explicit commit SHA to write into the row's updated_commit_sha. "
                        "Bypasses the resolver's stored-row task_git fallback so callers "
                        "that already know the projected commit (e.g. slice-commit) don't "
                        "have to coax the resolver via target_worktree_path."
                    ),
                ),
                ArgSpec(
                    "--branch",
                    help=(
                        "Explicit branch name to write into the row's updated_branch. "
                        "Pairs with --commit-sha to form a complete actor override."
                    ),
                ),
            ],
            surface_class="action",
            entity_family="handoff_state",
        ),
        ToolEntry(
            "get_handoff_state",
            get_handoff_state,
            TOOL_DESCRIPTIONS["get_handoff_state"],
            cli_name="state",
            cli_args=[
                ArgSpec("task_ref", nargs="?"),
                ArgSpec("--verbose", action="store_true"),
                ArgSpec(
                    "--sections",
                    help="Comma-separated sections to include (e.g. 'decisions_recent,findings_open'). Use 'identity' for identity-only (active + limits).",
                ),
                ArgSpec(
                    "--detail",
                    default=None,
                    choices=["full", "summary"],
                    help=(
                        "Detail level: full or summary. Omit to inherit the read profile's "
                        "default (or 'full' when no --read-profile is set)."
                    ),
                ),
                ArgSpec(
                    "--decision-fields",
                    nargs="+",
                    help=(
                        "Decision-scoped projection for the decisions_recent section. "
                        "Space-separated decision-table columns (e.g. decision branch commit_sha lane_id created_at)."
                    ),
                ),
                ArgSpec("--decision-branch", help="Equality filter on decisions_recent.branch."),
                ArgSpec("--decision-commit-sha", help="Equality filter on decisions_recent.commit_sha."),
                ArgSpec("--decision-lane-id", help="Equality filter on decisions_recent.lane_id."),
                ArgSpec(
                    "--branch",
                    help=(
                        "Explicit caller branch for read-side task resolution. "
                        "Use from interactive linked worktrees when task_ref is omitted."
                    ),
                ),
                ArgSpec(
                    "--decision-id-prefix",
                    help="Literal prefix filter on decisions_recent.decision (e.g. 'codex_slice_complete_').",
                ),
                ArgSpec(
                    "--top-n-decisions",
                    type=int,
                    default=None,
                    help=(
                        "Limit on the decisions_recent section. Defaults to "
                        "the handoff-state limit (currently 3). Raise it when "
                        "callers — e.g. sync-task-plan-checklist — need the "
                        "full slice-complete history, not just the most "
                        "recent three rows. When --read-profile is set, this "
                        "overrides the profile's default."
                    ),
                ),
                ArgSpec(
                    "--read-profile",
                    dest="read_profile",
                    choices=["identity", "hot_summary", "review_packet", "open_items", "full_debug"],
                    help=(
                        "internal named read profile that expands to the existing "
                        "sections / detail / top_n_* knobs. Explicit lower-level "
                        "flags still override the profile."
                    ),
                ),
                ArgSpec(
                    "--response-budget-bytes",
                    dest="response_budget_bytes",
                    type=int,
                    help=(
                        "internal server-side response budget. When set, the "
                        "budget planner may lower limits/detail and omit "
                        "optional sections before fetching heavy rows. "
                        "Required sections for the active profile are never "
                        "omitted by auto_summary."
                    ),
                ),
                ArgSpec(
                    "--budget-policy",
                    dest="budget_policy",
                    choices=["warn", "auto_summary", "fail"],
                    help=(
                        "internal budget policy. Default is 'auto_summary' when "
                        "--response-budget-bytes is supplied, otherwise 'warn'. "
                        "'fail' rejects with ok=false plus a retry_with hint "
                        "when the requested shape exceeds the budget."
                    ),
                ),
                ArgSpec(
                    "--include-write-schemas",
                    dest="include_write_schemas",
                    action="store_true",
                    help=(
                        "Opt in to the limits.write.tools write-contract registry "
                        "(~13KB). Default omits the tools block; scalar write "
                        "limits remain. [T23]"
                    ),
                ),
            ],
            surface_class="query",
            entity_family="handoff_state",
        ),
        ToolEntry(
            "validate",
            validate,
            TOOL_DESCRIPTIONS["validate"],
            cli_name="validate",
            cli_args=[
                ArgSpec(
                    "--kind",
                    required=True,
                    choices=["decision_id", "write"],
                    help="Validation variant to run.",
                ),
                ArgSpec("--decision", help="kind=decision_id: identifier to preflight."),
                ArgSpec(
                    "--decision-kind",
                    help="kind=decision_id: optional required category, e.g. slice_complete.",
                ),
                ArgSpec("--tool-name", help="kind=write: MCP write tool whose registry row gates the payload."),
                ArgSpec(
                    "--payload-json",
                    dest="payload_json",
                    help="JSON-encoded MCP write payload to preflight against the registry.",
                ),
            ],
            surface_class="query",
            entity_family="handoff_state",
        ),
        # Events (1)
        ToolEntry(
            "record_event",
            record_event,
            TOOL_DESCRIPTIONS["record_event"],
            cli_name="event",
            cli_args=[
                ArgSpec("--event-kind", required=True, choices=["decision", "test_result", "blocker"]),
                ArgSpec("--event-id", help="Projection event id for idempotent replay."),
                ArgSpec("--session"),
                ArgSpec("--decision"),
                ArgSpec("--rationale"),
                ArgSpec("--input-tokens", type=int),
                ArgSpec("--output-tokens", type=int),
                ArgSpec("--total-tokens", type=int),
                ArgSpec(
                    "--changed-files",
                    nargs="+",
                    dest="changed_files",
                    help="Monorepo-relative paths touched by this slice.",
                ),
                ArgSpec("--command", dest="command", help="Test command."),
                ArgSpec("--passed", action="store_true"),
                ArgSpec("--result"),
                ArgSpec(
                    "--trace",
                    action="append",
                    dest="traces",
                    help="Raw verification trace to archive. Repeat for multiple trace payloads.",
                ),
                ArgSpec("--exit-code", type=int),
                ArgSpec("--operation", choices=["add", "resolve", "reopen"]),
                ArgSpec("--description"),
                ArgSpec("--blocker-id", type=int),
                ArgSpec("--task-ref"),
                ArgSpec("--branch", help="Actor branch override for write provenance."),
                ArgSpec("--commit-sha", help="Actor commit SHA override for write provenance."),
                ArgSpec("--lane-id", help="Actor lane id override for write provenance."),
            ],
            surface_class="action",
            entity_family="handoff_state",
        ),
        # Next actions (1)
        ToolEntry(
            "next_actions",
            next_actions,
            TOOL_DESCRIPTIONS["next_actions"],
            cli_name="next-actions",
            cli_args=[
                ArgSpec("--operation", required=True, choices=["list", "add", "update", "complete", "skip"]),
                ArgSpec("--action-id", type=int),
                ArgSpec("--text", dest="action", help="Action text (maps to handler param 'action')."),
                ArgSpec("--lane-id"),
                ArgSpec("--priority", type=int),
                ArgSpec("--status", choices=["all", "pending", "done", "skipped"]),
                ArgSpec("--limit", type=int, default=100),
                ArgSpec("--offset", type=int, default=0),
                ArgSpec("--task-ref"),
            ],
            surface_class="action",
            entity_family="handoff_state",
        ),
    ]


def _review_tool_entries() -> list[ToolEntry]:
    return [
        # Review findings (1)
        ToolEntry(
            "review_findings",
            review_findings,
            TOOL_DESCRIPTIONS["review_findings"],
            cli_name="review-findings",
            cli_args=[
                ArgSpec(
                    "--operation",
                    required=True,
                    choices=[
                        "record",
                        "batch_record",
                        "update",
                        "resolve",
                        "repair_provenance",
                        "merge",
                        "integrate",
                        "disposition",
                        "list",
                    ],
                ),
                ArgSpec("--session"),
                ArgSpec("--finding-id"),
                ArgSpec("--resolve-finding-id", action="append"),
                ArgSpec("--severity", choices=["high", "medium", "low"]),
                ArgSpec("--file-path"),
                ArgSpec("--description"),
                ArgSpec("--line-start", type=int),
                ArgSpec("--line-end", type=int),
                ArgSpec("--fix"),
                ArgSpec("--findings-json"),
                ArgSpec("--findings-file"),
                ArgSpec("--all-open", action="store_true"),
                ArgSpec("--status"),
                ArgSpec("--finding-db-id", type=int),
                ArgSpec("--resolution-notes"),
                ArgSpec("--reopen-reason"),
                ArgSpec("--verified-commit-sha"),
                ArgSpec("--verification-evidence"),
                ArgSpec("--review-mode"),
                ArgSpec("--integration-ref", default="main"),
                ArgSpec("--actor-commit-sha"),
                ArgSpec("--actor-branch"),
                ArgSpec("--limit", type=int, default=20),
                ArgSpec("--offset", type=int, default=0),
                ArgSpec("--detail", default="full", choices=["full", "summary"], help="Detail level: full or summary"),
                ArgSpec("--task-ref"),
            ],
            surface_class="action",
            entity_family="review_findings",
        ),
        # Review runs (1)
        ToolEntry(
            "review_runs",
            review_runs,
            TOOL_DESCRIPTIONS["review_runs"],
            cli_name="review-runs",
            cli_args=[
                ArgSpec("--operation", required=True, choices=["record", "list", "coverage"]),
                ArgSpec("--review-run-id"),
                ArgSpec("--session"),
                ArgSpec("--subject-path"),
                ArgSpec(
                    "--subject-kind",
                    default="task_plan",
                    choices=["task_plan", "epic", "branch", "adr", "roadmap", "other"],
                ),
                ArgSpec("--review-mode", default="planning"),
                ArgSpec("--verdict"),
                ArgSpec("--verdict-decision"),
                ArgSpec("--limit", type=int, default=20),
                ArgSpec("--offset", type=int, default=0),
                ArgSpec("--task-ref"),
            ],
            surface_class="action",
            entity_family="review_runs",
        ),
        ToolEntry(
            "terminal_guard_telemetry",
            terminal_guard_telemetry,
            TOOL_DESCRIPTIONS["terminal_guard_telemetry"],
            cli_name="terminal-guard-telemetry",
            cli_args=[
                ArgSpec("--operation", required=True, choices=["record", "list", "replay"]),
                ArgSpec("--task-ref"),
                ArgSpec("--worktree-path"),
                ArgSpec("--harness"),
                ArgSpec("--tool-name"),
                ArgSpec("--decision", choices=["ask", "block"]),
                ArgSpec("--trigger"),
                ArgSpec("--native-tool-hint"),
                ArgSpec("--command-preview"),
                ArgSpec("--policy-version"),
                ArgSpec("--policy-source"),
                ArgSpec("--fallback-source"),
                ArgSpec("--created-at"),
                ArgSpec("--spool-path"),
                ArgSpec("--limit", type=int, default=20),
                ArgSpec("--offset", type=int, default=0),
            ],
            surface_class="action",
            entity_family="handoff_state",
        ),
    ]


def _lifecycle_tool_entries() -> list[ToolEntry]:
    return [
        # Close check + integrity (1, consolidated)
        ToolEntry(
            "integrity_check",
            integrity_check,
            TOOL_DESCRIPTIONS["integrity_check"],
            cli_name="integrity-check",
            cli_args=[
                ArgSpec("--kind", required=True, choices=["working_tree", "post_merge", "close"]),
                ArgSpec("--expected-dirty", nargs="*"),
                ArgSpec("--merged-sha"),
                ArgSpec("--expected-changed-files", nargs="*", default=[]),
                ArgSpec("--task-ref"),
                ArgSpec("--allow-no-active-task", action="store_true"),
                ArgSpec("--enforce", action="store_true"),
                ArgSpec("--require-fresh-tests", action="store_true"),
                ArgSpec("--current-commit-sha"),
            ],
            surface_class="generator",
            entity_family="lifecycle",
        ),
        ToolEntry(
            "render_handoff",
            render_handoff,
            TOOL_DESCRIPTIONS["render_handoff"],
            cli_name="render-handoff",
            cli_args=[
                ArgSpec("--kind", required=True, choices=["current_task", "dashboard"]),
                ArgSpec("--task-ref", help="Task ref (only used when --kind=current_task)."),
                ArgSpec("--no-write", action="store_true"),
            ],
            surface_class="generator",
            entity_family="lifecycle",
        ),
        # Export / import / archive (3)
        ToolEntry(
            "export_handoff_state",
            export_handoff_state,
            TOOL_DESCRIPTIONS["export_handoff_state"],
            profile="extended",
            cli_name="export",
            cli_args=[
                ArgSpec("--task-ref"),
                ArgSpec("--output-path"),
                ArgSpec("--no-markdown", action="store_true"),
            ],
            surface_class="generator",
            entity_family="lifecycle",
        ),
        ToolEntry(
            "import_handoff_state",
            import_handoff_state,
            TOOL_DESCRIPTIONS["import_handoff_state"],
            profile="extended",
            cli_name="import",
            cli_args=[
                ArgSpec("--input-path", required=True),
                ArgSpec("--mode", default="merge"),
                ArgSpec("--set-active", action="store_true"),
                ArgSpec("--allow-destructive-clear", action="store_true"),
            ],
            surface_class="action",
            entity_family="lifecycle",
        ),
        ToolEntry(
            "archive",
            archive,
            TOOL_DESCRIPTIONS["archive"],
            profile="extended",
            cli_name="archive",
            cli_args=[
                ArgSpec(
                    "--operation",
                    required=True,
                    choices=["archive", "gc", "reap", "reap_scratch", "reap_done", "retention", "get"],
                ),
                ArgSpec("--task-ref"),
                ArgSpec("--notes"),
                ArgSpec("--clear-active-if-matches", action="store_true"),
                ArgSpec("--prune-working-rows", action="store_true"),
                ArgSpec("--allow-destructive-clear", action="store_true"),
                ArgSpec("--cascade-maint-review", action="store_true"),
                ArgSpec("--no-tombstone-findings", dest="tombstone_findings", action="store_false"),
                ArgSpec("--apply", action="store_true"),
                ArgSpec("--no-snapshot", dest="include_snapshot", action="store_false"),
            ],
            surface_class="action",
            entity_family="lifecycle",
        ),
        ToolEntry(
            "reconcile_reviewer_scratch_findings_gc",
            reconcile_reviewer_scratch_findings_gc,
            TOOL_DESCRIPTIONS["reconcile_reviewer_scratch_findings_gc"],
            profile="extended",
            cli_name="reconcile-scratch-gc",
            cli_args=[ArgSpec("--apply", action="store_true")],
            surface_class="action",
            entity_family="review",
        ),
        ToolEntry(
            "collect_stale_nonscratch_open_findings",
            collect_stale_nonscratch_open_findings,
            TOOL_DESCRIPTIONS["collect_stale_nonscratch_open_findings"],
            profile="extended",
            cli_name="collect-stale-findings",
            cli_args=[
                ArgSpec("--apply", action="store_true"),
                ArgSpec("--batch-size", type=int, default=200),
            ],
            surface_class="query",
            entity_family="review",
        ),
        ToolEntry(
            "get_verified_tests",
            get_verified_tests,
            TOOL_DESCRIPTIONS["get_verified_tests"],
            profile="extended",
            cli_name="get-verified-tests",
            cli_args=[
                ArgSpec("--task-ref"),
                ArgSpec("--lane-id"),
                ArgSpec("--branch"),
                ArgSpec("--commit-sha"),
                ArgSpec("--passed", choices=["true", "false"]),
                ArgSpec("--include-traces", action="store_true"),
                ArgSpec("--correlated-file"),
                ArgSpec("--correlation-window-minutes", type=int, default=120),
                ArgSpec("--exclude-never-passed", action="store_true"),
                ArgSpec("--limit", type=int, default=100),
                ArgSpec("--offset", type=int, default=0),
            ],
            surface_class="query",
            entity_family="handoff_state",
        ),
        ToolEntry(
            "touched_files",
            touched_files,
            TOOL_DESCRIPTIONS["touched_files"],
            profile="extended",
            cli_name="touched-files",
            cli_args=[
                ArgSpec("--operation", required=True, choices=["record", "list"]),
                ArgSpec("--file-path"),
                ArgSpec("--change-kind", choices=["edit", "add", "delete"]),
                ArgSpec("--session"),
                ArgSpec("--commit-sha"),
                ArgSpec("--task-ref"),
                ArgSpec("--limit", type=int, default=20),
                ArgSpec("--offset", type=int, default=0),
            ],
            surface_class="action",
            entity_family="handoff_state",
        ),
        ToolEntry(
            "compaction",
            compaction,
            TOOL_DESCRIPTIONS["compaction"],
            profile="extended",
            cli_name="compaction",
            cli_args=[
                ArgSpec(
                    "--operation",
                    required=True,
                    choices=["record", "get", "get_latest", "disable", "enable", "status"],
                ),
                ArgSpec("--transcript-path"),
                ArgSpec("--task-ref"),
                ArgSpec("--harness", choices=list(COMPACTION_HARNESS_INPUT_CHOICES)),
                ArgSpec("--session-id", dest="session_id"),
                ArgSpec("--compaction-id", dest="compaction_id"),
            ],
            surface_class="action",
            entity_family="handoff_state",
        ),
        ToolEntry(
            "semantic_reinjection_packet",
            semantic_reinjection_packet,
            TOOL_DESCRIPTIONS["semantic_reinjection_packet"],
            profile="extended",
            cli_name="semantic-reinjection-packet",
            cli_args=[
                ArgSpec("--task-ref"),
                ArgSpec("--anchor-text", action="append", dest="anchor_texts"),
                ArgSpec("--semantic-content-budget-chars", type=int, dest="semantic_content_budget_chars"),
            ],
            surface_class="query",
            entity_family="handoff_state",
        ),
        # Compound tools (3)
        ToolEntry(
            "load_session",
            load_session,
            TOOL_DESCRIPTIONS["load_session"],
            cli_name="load-session",
            cli_args=[
                ArgSpec("task_ref", nargs="?"),
                ArgSpec(
                    "--sections",
                    help="Comma-separated sections for the nested state payload (e.g. 'identity').",
                ),
                ArgSpec(
                    "--detail",
                    default=None,
                    choices=["full", "summary"],
                    help="Detail level for nested state and open findings.",
                ),
                ArgSpec(
                    "--read-profile",
                    dest="read_profile",
                    choices=["identity", "hot_summary", "review_packet", "open_items", "full_debug"],
                    help="Named read profile applied across state + open_findings + touched_files.",
                ),
                ArgSpec(
                    "--response-budget-bytes",
                    dest="response_budget_bytes",
                    type=int,
                    help="Server-side compound response budget (bytes).",
                ),
                ArgSpec(
                    "--budget-policy",
                    dest="budget_policy",
                    choices=["warn", "auto_summary", "fail"],
                    help="Budget policy when a budget is active (default auto_summary).",
                ),
                ArgSpec(
                    "--include-write-schemas",
                    dest="include_write_schemas",
                    action="store_true",
                    help=(
                        "Opt in to limits.write.tools on the nested state (~13KB). "
                        "Default omits the tools registry; scalar write limits remain. [T23]"
                    ),
                ),
                ArgSpec(
                    "--top-n-touched-files",
                    dest="top_n_touched_files",
                    type=int,
                    help="Bound touched_files (0 omits the section).",
                ),
                ArgSpec(
                    "--open-findings-limit",
                    dest="open_findings_limit",
                    type=int,
                    help="Bound open_findings (0 omits the section).",
                ),
            ],
            surface_class="query",
            entity_family="session",
        ),
        ToolEntry(
            "close_slice",
            close_slice,
            TOOL_DESCRIPTIONS["close_slice"],
            surface_class="action",
            entity_family="lifecycle",
        ),
        ToolEntry(
            "list_handoff_rows",
            list_handoff_rows,
            TOOL_DESCRIPTIONS["list_handoff_rows"],
            cli_name="handoff-rows",
            cli_args=[
                ArgSpec(
                    "--status",
                    dest="status_filter",
                    nargs="+",
                    help=(
                        "One or more handoff_state status values to filter by. "
                        "Use 'in_progress review blocked' for the LIVE_ACTIVE_STATUSES set. "
                        "Omit to return every non-archived row."
                    ),
                ),
            ],
            surface_class="query",
            entity_family="handoff_state",
        ),
        ToolEntry(
            "audit_decision_ids",
            audit_decision_ids,
            TOOL_DESCRIPTIONS["audit_decision_ids"],
            profile="extended",
            cli_name="audit-decisions",
            cli_args=[
                ArgSpec("--task-ref"),
                ArgSpec("--limit", type=int, default=50),
                ArgSpec(
                    "--include-categories",
                    nargs="+",
                    choices=["canonical", "legacy_slice", "malformed_slice", "freeform"],
                    dest="include_categories",
                    help="Categories to include in the violations list (default: malformed_slice freeform).",
                ),
            ],
            surface_class="query",
            entity_family="handoff_state",
        ),
    ]


def _artifact_tool_entries() -> list[ToolEntry]:
    return [
        # Artifact tools (1)
        ToolEntry(
            "artifacts",
            artifacts,
            TOOL_DESCRIPTIONS["artifacts"],
            profile="extended",
            cli_name="artifacts",
            cli_args=[
                ArgSpec("--operation", required=True, choices=["record", "search", "get", "purge"]),
                ArgSpec("--task-ref"),
                ArgSpec("--lane-id"),
                ArgSpec("--app-root"),
                ArgSpec("--source-kind"),
                ArgSpec("--source-label"),
                ArgSpec("--content-type", default="text/plain"),
                ArgSpec("--summary"),
                ArgSpec("--content-file", help="Path to a file whose contents will be used as the artifact content."),
                ArgSpec("--content", help="Artifact content as a string."),
                ArgSpec("--metadata-json"),
                ArgSpec("--query", action="append", dest="queries", help="Search term (repeatable)."),
                ArgSpec("--limit", type=int, default=10),
                ArgSpec("--offset", type=int, default=0),
                ArgSpec("--detail", default="full", choices=["full", "summary"], help="Detail level: full or summary"),
                ArgSpec("--fields", help="Comma-separated fields to keep in returned rows."),
                ArgSpec("--source-id", type=int),
                ArgSpec("--include-terms", action="store_true"),
                ArgSpec("--top-n-terms", type=int, default=10),
                ArgSpec("--older-than-days", type=int),
            ],
            surface_class="action",
            entity_family="artifacts",
        ),
        # Search (1)
        ToolEntry(
            "search_handoff",
            search_handoff,
            TOOL_DESCRIPTIONS["search_handoff"],
            profile="extended",
            cli_name="handoff-search",
            surface_class="generator",
            entity_family="handoff_state",
            cli_args=[
                ArgSpec(
                    "--query",
                    action="append",
                    dest="queries",
                    help="Search term (repeatable; multiple terms are OR-joined). At least one required.",
                ),
                ArgSpec("--task-ref", help="Scope results to a specific task."),
                ArgSpec("--lane-id", help="Scope results to a specific lane."),
                ArgSpec(
                    "--record-types",
                    nargs="+",
                    choices=["decision", "finding", "blocker", "action", "verified_test"],
                    help="Limit search to these record types (decision, finding, blocker, action, verified_test).",
                ),
                ArgSpec("--limit", type=int, default=20, help="Max results (default 20, max 100)."),
                ArgSpec("--detail", default="full", choices=["full", "summary"], help="Detail level: full or summary"),
                ArgSpec("--fields", help="Comma-separated fields to keep in each result row."),
                ArgSpec(
                    "--decision-fields",
                    nargs="+",
                    help=(
                        "Decision-scoped projection. Space-separated decision-table columns to merge "
                        "onto result rows whose record_type == 'decision' (e.g. branch commit_sha "
                        "lane_id created_at). Decision-only fields are not reachable through --fields."
                    ),
                ),
            ],
        ),
    ]


def _build_tool_registry() -> list[ToolEntry]:
    """Build the handoff MCP tool registry (called lazily after all handlers defined)."""
    return _task_state_tool_entries() + _review_tool_entries() + _lifecycle_tool_entries() + _artifact_tool_entries()


def _render_current_task_result(
    task_ref: str | None = None,
    write_file: bool = True,
) -> dict:
    """Render the v2 workspace-summary CURRENT_TASK.json (internal).

    The on-disk file is the workspace summary derived from per-task
    projection files; ``task_ref`` is used only to refresh that task's
    per-task projection alongside the workspace write so the summary
    reflects the caller-requested task.
    """
    from .current_task_rendering import (  # noqa: PLC0415
        _render_current_task_json,
        _write_per_task_projection,
        _write_workspace_summary_current_task_json,
    )

    resolved_task_ref = task_ref
    if resolved_task_ref is None:
        with core._get_db_connection() as conn:
            from .shared_primitives import _resolve_workspace_handoff_row  # noqa: PLC0415

            try:
                active_row = _resolve_workspace_handoff_row(conn)
            except ValueError:
                active_row = None
            resolved_task_ref = (
                str(active_row["task_ref"]) if active_row is not None and active_row["task_ref"] else None
            )

    runtime = get_runtime_config()
    current_task_path = runtime.current_task_path

    if write_file:
        # Refresh the requested task's per-task projection first so the
        # workspace summary derive picks it up. Archived tasks (no live
        # handoff_state row) are skipped — the workspace summary will
        # filter them out anyway.
        if resolved_task_ref is not None:
            try:
                _write_per_task_projection(resolved_task_ref)
            except KeyError:
                pass
        _write_workspace_summary_current_task_json(unconditional=True)
        current_task_json: str | None = None
    else:
        current_task_json = _render_current_task_json()

    artifacts = []
    if write_file:
        artifacts = [
            {"type": "current_task_md", "path": str(current_task_path), "written": True},
        ]
    return core._envelope(
        ok=True,
        tool="render_handoff",
        data={
            "task_ref": resolved_task_ref,
            "path": str(current_task_path),
            "written": write_file,
            "current_task_json": current_task_json,
        },
        task_ref=resolved_task_ref,
        artifacts=artifacts,
    )


def list_active_tasks() -> list[dict[str, Any]]:
    """Return every row currently in the live ``handoff_state`` table.

    Each dict carries ``task_ref``, ``status``, ``target_branch``,
    ``target_worktree_path``, ``updated_at``, and ``revision``. Archived
    tasks are excluded; use ``get_archived_task`` for those.

    This is the public, schema-stable path for tooling that needs to
    enumerate live tasks (e.g. maintenance archival of stale ``MAINT-*``
    rows) without dropping to raw ``sqlite3`` queries (see rg-018).
    """
    rows: list[dict[str, Any]] = []
    with core._get_db_connection() as conn:
        for raw in conn.execute(
            "SELECT task_ref, status, target_branch, target_worktree_path, updated_at, revision "
            "FROM handoff_state ORDER BY updated_at DESC, task_ref ASC"
        ).fetchall():
            rows.append(
                {
                    "task_ref": raw["task_ref"],
                    "status": raw["status"],
                    "target_branch": raw["target_branch"],
                    "target_worktree_path": raw["target_worktree_path"],
                    "updated_at": raw["updated_at"],
                    "revision": raw["revision"],
                }
            )
    return rows


def list_handoff_rows(
    status_filter: list[str] | None = None,
    exclude_archived: bool = True,
) -> list[dict[str, Any]]:
    """Enumerate live ``handoff_state`` rows with optional status filtering.

    internal read-path. Distinct from ``list_active_tasks`` in three
    ways: (1) returns ``task_plan_path`` so callers can discover plans without
    a follow-up ``get_handoff_state`` round-trip; (2) accepts ``status_filter``
    so consumers can request only ``LIVE_ACTIVE_STATUSES`` rows without
    re-implementing the filter; (3) is the recommended replacement for
    ad-hoc ``sqlite3 .task-state/handoff.db`` enumeration.

    ``exclude_archived`` reflects the table semantics: archived rows live in
    ``task_archives`` and are never returned here. The flag is reserved for
    future schema evolution; today only ``True`` is meaningful.
    """
    if not exclude_archived:
        raise ValueError(
            "exclude_archived=False is reserved for future schema evolution; use get_archived_task for archived rows."
        )

    if status_filter is not None:
        invalid = [s for s in status_filter if s not in HANDOFF_ACTIVE_STATUSES]
        if invalid:
            raise ValueError(f"Invalid status filter values: {invalid}. Valid: {sorted(HANDOFF_ACTIVE_STATUSES)}")

    rows: list[dict[str, Any]] = []
    with core._get_db_connection() as conn:
        if status_filter:
            placeholders = ",".join(["?"] * len(status_filter))
            query = (
                "SELECT task_ref, status, target_branch, target_worktree_path, "
                "task_plan_path, updated_at, revision "
                f"FROM handoff_state WHERE status IN ({placeholders}) "
                "ORDER BY updated_at DESC, task_ref ASC"
            )
            cursor = conn.execute(query, status_filter)
        else:
            cursor = conn.execute(
                "SELECT task_ref, status, target_branch, target_worktree_path, "
                "task_plan_path, updated_at, revision "
                "FROM handoff_state ORDER BY updated_at DESC, task_ref ASC"
            )
        for raw in cursor.fetchall():
            rows.append(
                {
                    "task_ref": raw["task_ref"],
                    "status": raw["status"],
                    "target_branch": raw["target_branch"],
                    "target_worktree_path": raw["target_worktree_path"],
                    "task_plan_path": raw["task_plan_path"],
                    "updated_at": raw["updated_at"],
                    "revision": raw["revision"],
                }
            )
    return rows


def _render_dashboard_result(write_file: bool = True, *, full: bool = False) -> dict:
    from .dashboard_rendering import generate_dashboard_md as _generate  # noqa: PLC0415

    result = _generate(write_file=write_file, full=full)
    result["tool"] = "render_handoff"
    return result


def render_handoff(
    kind: Annotated[
        Literal["current_task", "dashboard", "plan_checklist"],
        Field(
            description=(
                "Which handoff surface to render. 'current_task' writes CURRENT_TASK.json "
                "for the requested (or active) task; 'dashboard' writes DASHBOARD.txt — "
                "the cross-task observatory view."
            )
        ),
    ],
    task_ref: Annotated[
        str | None,
        Field(
            description=(
                "Only used when kind='current_task'. Task reference to render. "
                "Defaults to the active task when omitted."
            )
        ),
    ] = None,
    write_file: Annotated[
        bool,
        Field(description="Write the rendered artifact to disk. Defaults to True."),
    ] = True,
    source: Annotated[str | None, Field(description="Optional caller source for orientation-read telemetry.")] = None,
    session: Annotated[
        str | None, Field(description="Optional caller session id for orientation-read telemetry.")
    ] = None,
    full: Annotated[
        bool,
        Field(description="Only used when kind='dashboard'. Include done and archived task history."),
    ] = False,
) -> dict:
    """Compound renderer for CURRENT_TASK.json and DASHBOARD.txt."""
    if kind == "current_task":
        result = _render_current_task_result(task_ref=task_ref, write_file=write_file)
    elif kind == "dashboard":
        result = _render_dashboard_result(write_file=write_file, full=full)
    elif kind == "plan_checklist":
        from .plan_checklist_rendering import render_plan_checklist  # noqa: PLC0415

        if not task_ref:
            return core._envelope(
                ok=False, tool="render_handoff", data={"error": "task_ref required for plan_checklist"}
            )
        result = render_plan_checklist(task_ref)
    else:  # pragma: no cover - pydantic rejects unknown kinds at boundary.
        raise ValueError(f"Unknown render_handoff kind: {kind!r}")
    record_orientation_read_once(
        tool="render_handoff",
        task_ref=str(result.get("task_ref") or task_ref or ""),
        resolution_outcome="explicit" if task_ref is not None else "resolved",
        source=source,
        session=session,
        read_profile=kind,
    )
    return result


def _normalize_handler_result(tool: str, result: object) -> object:
    """Normalize non-dict MCP handler returns into the v2 envelope shape."""

    if isinstance(result, (dict, BaseModel)):
        return result
    if isinstance(result, list):
        return core._envelope(ok=True, tool=tool, data={"rows": result})
    if isinstance(result, str) or result is None:
        return core._envelope(ok=True, tool=tool, data={"result": result})
    raise TypeError(f"{tool} returned unsupported MCP result type: {type(result).__name__}")


def _extract_write_selector(tool_name: str, args: tuple, kwargs: dict) -> str | None:
    """Best-effort sub-operation value (``operation``/``event_kind``) for a call.

    Multiplexed tools carry their selector either as a top-level kwarg or
    inside a payload dict/model; scan both. Returns None when the tool has
    no selector or the value is not present/stringy.
    """
    from .write_contracts import get_write_contract  # noqa: PLC0415

    contract = get_write_contract(tool_name)
    if contract is None:
        return None
    selector = contract.selector_field
    if selector is None and "operation" in contract.required:
        selector = "operation"
    if selector is None:
        return None
    direct = kwargs.get(selector)
    if isinstance(direct, str):
        return direct
    for value in (*args, *kwargs.values()):
        if isinstance(value, dict):
            found = value.get(selector)
        elif isinstance(value, BaseModel):
            found = getattr(value, selector, None)
        else:
            continue
        if isinstance(found, str):
            return found
    return None


def _maybe_capture_write_rejection(tool_name: str, result: object, *, sub_operation: str | None = None) -> None:
    """Self-capture an ok:false write-tool envelope as ``mcp_write_rejected``.

    implementation note of implementation note (internal). Only tools with a
    ``write_contracts`` registry row count as writes — ok:false from a
    read tool is not a write rejection — and read-only sub-operations of
    multiplexed tools (``artifacts.get``, ``review_findings.list``, …)
    are excluded the same way (review finding REV-A-001).
    ``capture_write_rejection`` owns the dedup window, re-entrancy
    guard, and never-fail guarantee, so the caller-visible response is
    returned unchanged regardless.
    """
    from .write_contracts import (  # noqa: PLC0415
        READ_ONLY_OPERATIONS,
        READ_ONLY_TOOLS,
        get_write_contract,
    )

    if not isinstance(result, dict) or result.get("ok") is not False:
        return
    if get_write_contract(tool_name) is None:
        return
    if tool_name in READ_ONLY_TOOLS:
        return
    if sub_operation is not None and sub_operation in READ_ONLY_OPERATIONS.get(tool_name, frozenset()):
        return
    data = result.get("data")
    # Registry-wide structured-rejection wrapper (internal): residual
    # unclassified rejections get the default {violated, expected, example, rule_id}
    # envelope so telemetry + callers never see bare prose alone.
    if isinstance(data, dict):
        from .structured_rejections import ensure_structured_rejection_data  # noqa: PLC0415

        structured = ensure_structured_rejection_data(tool_name, data)
        if structured is not data:
            data = structured
            result["data"] = data
    summary: object = None
    if isinstance(data, dict):
        summary = data.get("error") or data.get("state_error") or data.get("violated")
        if summary is None and isinstance(data.get("errors"), list):
            summary = "; ".join(str(item) for item in data["errors"][:5])
    if not isinstance(summary, str) or not summary.strip():
        summary = f"{tool_name} write rejected"
    scope = result.get("scope")
    task_ref = scope.get("task_ref") if isinstance(scope, dict) else None
    # Prefer the envelope's specific handler name (e.g. merge_review_findings)
    # over the coarse registered tool name for implementation note cluster precision
    # (review finding REV-A-002); fall back to tool.sub_operation.
    envelope_tool = result.get("tool")
    if isinstance(envelope_tool, str) and envelope_tool and envelope_tool != tool_name:
        captured_tool = envelope_tool
    elif sub_operation is not None:
        captured_tool = f"{tool_name}.{sub_operation}"
    else:
        captured_tool = tool_name
    capture_write_rejection(
        tool_name=captured_tool,
        summary=summary,
        task_ref=task_ref if isinstance(task_ref, str) else None,
    )


# The four read tools whose MCP tool-boundary invocations count as orientation
# reads. Internal/in-process calls to these functions (and every non-read tool)
# run outside the boundary and record nothing.
_ORIENTATION_READ_TOOL_NAMES = frozenset(
    {"get_handoff_state", "load_session", "render_handoff", "semantic_reinjection_packet"}
)


def _wrapped_handler_in_orientation_boundary(
    entry_name: str, handler: Callable[..., object], args: tuple, kwargs: dict
) -> object:
    """Invoke ``handler`` inside the orientation-read boundary for read tools.

    Only the four registered read tools mark the boundary; every other tool
    (and the internal reads those tools trigger, e.g. ``close_slice`` ->
    ``render_handoff``) runs with the boundary OFF so no telemetry is recorded.
    """
    if entry_name in _ORIENTATION_READ_TOOL_NAMES:
        with orientation_read_boundary():
            return handler(*args, **kwargs)
    return handler(*args, **kwargs)


def _wrap_branch_mismatch_for_mcp(entry: ToolEntry) -> Callable[..., object]:
    """Return an MCP-only wrapper that normalizes typed connect/write refusals."""

    handler = entry.handler
    signature = inspect.signature(handler)

    @functools.wraps(handler)
    def _wrapped(*args: object, **kwargs: object) -> object:
        try:
            result = _normalize_handler_result(
                entry.name, _wrapped_handler_in_orientation_boundary(entry.name, handler, args, kwargs)
            )
        except BranchMismatchError as exc:
            result = core._envelope(
                ok=False,
                tool=entry.name,
                task_ref=exc.task_ref,
                data={
                    "error": str(exc),
                    "task_ref": exc.task_ref,
                    "expected_branch": exc.expected_branch,
                    "actual_branch": exc.actual_branch,
                },
            )
        except SchemaVersionMismatchError as exc:
            # T15: loud typed refusal naming both versions + remedy ([OBS-08]).
            result = core._envelope(
                ok=False,
                tool=entry.name,
                data=exc.as_data(),
            )
        _maybe_capture_write_rejection(
            entry.name,
            result,
            sub_operation=_extract_write_selector(entry.name, args, kwargs),
        )
        return result

    _wrapped.__signature__ = signature.replace(return_annotation=dict)  # type: ignore[attr-defined]
    return _wrapped


def build_handoff_mcp(config: RuntimeConfig) -> FastMCP:
    configure_runtime(config)
    mcp = FastMCP(
        f"{BRAND_NAME} Handoff MCP",
        instructions=(
            f"You are connected to the {BRAND_NAME} Handoff MCP server. "
            "Use these tools for task state, review findings, exports, and close checks.\n\n"
            "## Task State Model\n\n"
            "Multiple tasks can be active concurrently. Live handoff_state rows are keyed by task_ref, "
            "and callers that omit task_ref are resolved from the current workspace path. "
            "Completed tasks are archived into task_archives with a status snapshot. "
            "DASHBOARD.txt renders the human-readable active-task view plus the cross-task dashboard, "
            "while CURRENT_TASK.json stores the machine-readable active-task snapshot. "
            "The dashboard renders both the active task's live status and each archived task's snapshot status. "
            "Non-archived, non-active tasks default to 'active' in the dashboard — "
            "this is a rendering fallback, not a real stored status.\n\n"
            "## Task Lifecycle\n\n"
            "1. **Start**: `set_handoff_state(task_ref=..., objective=..., status='in_progress')` "
            "or `switch_task(task_ref=...)` (on workbay-orchestrator-mcp).\n"
            "2. **Work**: record decisions with `record_event(event={event_kind:'decision', ...})`, "
            "record test results with `record_event(event={event_kind:'test_result', ...})`, "
            "and record blockers with `record_event(event={event_kind:'blocker', ...})`.\n"
            "3. **Complete slices**: use `close_slice(...)` to record a slice-complete decision. "
            "This keeps the task status as in_progress and always regenerates DASHBOARD.txt; "
            "CURRENT_TASK.json is regenerated only when current_task_auto_regen is enabled "
            "(otherwise refresh it on demand with `render_handoff(kind='current_task')`).\n"
            "4. **Finish task**: when all slices are done, update status to done: "
            "`set_handoff_state(task_ref=..., status='done', status_only=True)`. "
            "Then archive: `archive(payload={'operation': 'archive', 'task_ref': ...})`.\n"
            "5. **Archive without finishing**: if you archive a task while it is still in_progress, "
            "its dashboard status will remain in_progress permanently. "
            "Always set status to done before or after archiving.\n\n"
            "## Key Tool Guidance\n\n"
            "- `load_session`: use at session start to get state + open findings in one call.\n"
            "- `close_slice`: use for slice completions — it records a decision, keeps status "
            "in_progress, and always regenerates DASHBOARD.txt atomically; it regenerates "
            "CURRENT_TASK.json only when current_task_auto_regen is enabled (otherwise refresh it "
            "on demand with `render_handoff(kind='current_task')`). The result reports "
            "dashboard_written and current_task_md_written.\n"
            "- `set_handoff_state(status_only=True, ...)`: use to change status "
            "(in_progress/done/blocked/review) without recording a decision. Works for both "
            "active and archived tasks. Replaces the legacy `update_task_status` tool.\n"
            "- `set_handoff_state`: use to update objective, focus, or status on the active task. "
            "Requires expected_revision.\n"
            "- `archive(payload={'operation': 'archive'|'gc'|'get', ...})`: consolidated tool "
            "covering archive snapshot writes, garbage collection, and archived-task fetch. "
            "The archive operation does not change task status — the archived snapshot preserves "
            "whatever status the task had at archive time.\n"
            "- `render_handoff`: call after any state-changing operation "
            "(record_event, review_findings with record/batch_record/update). "
            "Use kind='current_task' to refresh the machine-readable CURRENT_TASK.json "
            "snapshot, and kind='dashboard' to refresh the human-readable DASHBOARD.txt "
            "observatory view."
        ),
    )
    _apply_tool_descriptions()
    for entry in _build_tool_registry():
        if entry.deprecated_since is not None:
            entry.handler.__doc__ = f"[DEPRECATED since {entry.deprecated_since}] " + (
                entry.handler.__doc__ or entry.description
            )
        # Tool handlers return native dicts via _envelope() / _json_response()
        # in shared_primitives.py. FastMCP serialises the dict once on its way
        # out — there is no longer a `json.dumps -> json.loads` round trip,
        # and the wire payload is a clean nested object instead of the legacy
        # `structured_content={"result": "<escaped JSON>"}` envelope. internal
        # finished internal's implementation note by removing the _make_dict_wrapper shim
        # that used to translate `-> str` handlers into `-> dict` at this
        # registration site; the production handlers are now `-> dict` end to
        # end and the wrapper is dead code.
        mcp.add_tool(_wrap_branch_mismatch_for_mcp(entry))
    _emit_semantic_reinjection_startup_probe()
    # T13: surface src↔installed skew at startup (observability only; no reinstall).
    try:
        from .package_skew import emit_src_installed_skew_startup_log

        emit_src_installed_skew_startup_log()
    except Exception:  # noqa: BLE001 — startup probe must not block server boot
        pass
    return mcp


_DOCTOR_STDIO_TIMEOUT_DEFAULT = 20.0
_DOCTOR_DEADLINE_DEFAULT = 30.0


def _resolve_doctor_deadline_seconds(default: float = _DOCTOR_DEADLINE_DEFAULT) -> float:
    """Resolve the overall ``run_doctor`` probe budget (seconds).

    Override with ``WORKBAY_DOCTOR_DEADLINE_SECONDS``. A non-positive or
    unparseable value falls back to the default so a bad env can never disable
    the bound.
    """
    raw = resolve_env_alias("WORKBAY_DOCTOR_DEADLINE_SECONDS", default="").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _remaining_deadline_seconds(deadline_at: float) -> float:
    return max(0.1, deadline_at - time.monotonic())


def _resolve_doctor_stdio_timeout(
    default: float = _DOCTOR_STDIO_TIMEOUT_DEFAULT,
    *,
    deadline_cap: float | None = None,
) -> float:
    """Resolve the stdio-startup probe ceiling (seconds).

    Override with ``WORKBAY_HANDOFF_DOCTOR_STDIO_TIMEOUT``. A non-positive or
    unparseable value falls back to the default so a bad env can never disable
    the bound. When ``deadline_cap`` is set, the returned value never exceeds it.
    """
    raw = resolve_env_alias("WORKBAY_HANDOFF_DOCTOR_STDIO_TIMEOUT", default="").strip()
    if not raw:
        timeout_s = default
    else:
        try:
            value = float(raw)
        except ValueError:
            timeout_s = default
        else:
            timeout_s = value if value > 0 else default
    if deadline_cap is not None:
        return min(timeout_s, deadline_cap)
    return timeout_s


async def _check_stdio_startup(
    config: RuntimeConfig,
    launcher: Path,
    log_dir: Path,
    *,
    timeout_s: float | None = None,
) -> dict[str, Any]:
    """Run the stdio MCP handshake as an async health probe.

    The whole spawn-plus-initialize handshake is bounded by
    ``asyncio.wait_for``. Without a ceiling a subprocess that never finishes the
    MCP ``initialize`` handshake — e.g. asyncio child-watcher contention after
    many subprocess-spawning tests share one process — hangs ``run_doctor`` (and
    the pytest suite) indefinitely, because the broad ``except`` below only
    catches errors, not hangs. On timeout we return the same diagnostic
    ``ok=False`` shape so non-strict callers keep exiting 0.
    """
    if timeout_s is None:
        timeout_s = _resolve_doctor_stdio_timeout()

    async def _probe() -> dict[str, Any]:
        transport = PythonStdioTransport(
            script_path=launcher,
            args=["--workspace-root", str(config.workspace_root), "serve-stdio"],
            cwd=str(config.workspace_root),
            python_cmd=sys.executable,
            log_file=log_dir / "doctor-stdio.log",
        )
        async with Client(transport) as client:
            tools = await client.list_tools()
            return {"ok": True, "tools": sorted(tool.name for tool in tools)}

    try:
        # TimeoutError must be caught before the broad clause: it is an OSError
        # subclass (and asyncio.TimeoutError aliases it on 3.11+), so the order
        # here is load-bearing.
        return await asyncio.wait_for(_probe(), timeout=timeout_s)
    except TimeoutError:
        return {
            "ok": False,
            "error": f"stdio startup probe timed out after {timeout_s:.0f}s",
            "remediation": (
                "The stdio MCP transport did not complete the initialize handshake within "
                f"{timeout_s:.0f}s. Verify the launcher starts cleanly and the package is "
                f"installed in the active venv: {launcher}. Override the budget with "
                "WORKBAY_HANDOFF_DOCTOR_STDIO_TIMEOUT (seconds)."
            ),
        }
    except (Exception, OSError) as exc:  # noqa: BLE001 — diagnostic capture
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "remediation": (
                "The stdio MCP transport failed to start. "
                "Verify that the launcher exists and the package is installed in the active venv: "
                f"{launcher}"
            ),
        }


def _check_cli_startup(
    config: RuntimeConfig,
    cli_env: dict[str, str],
    *,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Run the CLI `state` subcommand as a subprocess health probe."""
    try:
        probe = subprocess.run(
            [sys.executable, "-m", "workbay_handoff_mcp", "--workspace-root", str(config.workspace_root), "state"],
            cwd=str(config.workspace_root),
            env=cli_env,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        )
        json.loads(probe.stdout)
        return {"ok": True}
    except SubprocessTimeoutExpired:
        budget = f"{timeout:.0f}s" if timeout is not None else "budget"
        return {
            "ok": False,
            "error": f"CLI startup probe timed out after {budget}",
            "remediation": (
                "The `mcp-workbay-handoff state` CLI command did not finish within the "
                f"doctor deadline ({budget}). Check package install and workspace health, or "
                "raise WORKBAY_DOCTOR_DEADLINE_SECONDS."
            ),
        }
    except (subprocess.CalledProcessError, OSError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "remediation": (
                "The `mcp-workbay-handoff state` CLI command failed to start. "
                "Check that the package is installed and the workspace root exists: "
                f"{config.workspace_root}"
            ),
        }


def _check_fts5_available() -> dict[str, Any]:
    import sqlite3

    with sqlite3.connect(":memory:") as probe:
        try:
            probe.execute("CREATE VIRTUAL TABLE _fts5_test USING fts5(body)")
            probe.execute("DROP TABLE IF EXISTS _fts5_test")
            return {"ok": True}
        except sqlite3.OperationalError:
            return {
                "ok": False,
                "remediation": (
                    "SQLite FTS5 extension is not available on this system. "
                    "mcp-workbay-handoff artifact indexing requires FTS5. "
                    "Rebuild SQLite with SQLITE_ENABLE_FTS5 or use a Python distribution "
                    "that bundles FTS5 (e.g. system Python on macOS 10.15+ or major Linux distros)."
                ),
            }


def _check_state_dir_writable(state_dir: Path) -> dict[str, Any]:
    """Probe write access to state_dir.

    Returns ``{"ok": True}`` on success, or ``{"ok": False, "error": ...,
    "remediation": ...}`` when the probe write fails so callers can surface
    actionable guidance rather than a bare exception.
    """
    probe = state_dir / ".write-test"
    try:
        probe.write_text("ok")
        probe.unlink()
        return {"ok": True}
    except OSError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "remediation": (
                f"Ensure the state directory is writable: "
                f"`chmod u+w {state_dir}` or check disk space and filesystem mounts."
            ),
        }


_FTS_TABLES = ("decisions_fts", "findings_fts", "blockers_fts", "actions_fts")


def _check_fts_index_health(db_path: Path) -> dict[str, Any]:
    import sqlite3 as _sqlite3

    with _sqlite3.connect(str(db_path)) as conn:
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?)",
                _FTS_TABLES,
            ).fetchall()
        }
        table_counts: dict[str, int] = {}
        for tbl in _FTS_TABLES:
            if tbl in existing:
                table_counts[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # noqa: S608
            else:
                table_counts[tbl] = -1  # -1 signals table missing

    missing = [t for t, v in table_counts.items() if v < 0]
    if missing:
        return {
            "ok": False,
            "tables": table_counts,
            "remediation": (
                f"FTS index tables are missing: {missing}. "
                f"Run `mcp-workbay-handoff migrate` or delete and reinitialize the database at {db_path}."
            ),
        }
    return {"ok": True, "tables": table_counts}


def _check_embedding_provider() -> dict[str, Any]:
    """Report semantic-reinjection embedding provider state (implementation note/0048).

    Best-effort; never raises. Semantic reinjection is opt-in: an unconfigured
    provider is a healthy degrade (``ok=True, configured=False``) — reinjection
    falls back to recency selection. A *configured* provider has its pinned model
    + tokenizer SHA-256 verified via ``verify_artifacts()`` (hash-only, no session
    load, no network). A corrupt or hash-mismatched artifact surfaces as
    ``ok=False`` before it reaches the embed path.

    When artifacts verify, a lightweight runtime probe imports ``onnxruntime`` and
    ``tokenizers`` (internal). Missing or broken deps yield
    ``runtime_available=False`` with a ``warning`` while keeping ``ok=True``.
    ``runtime_available`` is omitted unless ``artifacts_verified=True``.
    """
    try:
        from .embeddings.provider import (
            MODEL_ID,
            EmbeddingArtifactError,
            EmbeddingProvider,
        )
    except ImportError as exc:
        return {
            "ok": True,
            "configured": False,
            "model_id": None,
            "note": f"embeddings package unavailable ({exc}); semantic reinjection disabled",
        }
    try:
        provider = EmbeddingProvider.from_env()
    except Exception as exc:  # noqa: BLE001 — diagnostic capture
        return {"ok": True, "configured": False, "model_id": MODEL_ID, "note": f"provider env resolution failed: {exc}"}
    if provider is None:
        return {
            "ok": True,
            "configured": False,
            # Distinguishes the deliberate "operator has not pointed at a model"
            # soft-degrade (recency fallback) from a broken runtime: the startup
            # probe stays silent for this but is loud for deps/artifact/runtime
            # failures (implementation note boot probe; REV-A-001).
            "unconfigured": True,
            "model_id": MODEL_ID,
            "note": (
                "embedding artifact env vars unset "
                "(WORKBAY_HANDOFF_EMBEDDING_MODEL/_TOKENIZER + matching _SHA256 pins); "
                "semantic reinjection degrades to recency selection"
            ),
        }
    try:
        provider.verify_artifacts()
    except EmbeddingArtifactError as exc:
        return {
            "ok": False,
            "configured": True,
            "model_id": provider.model_id,
            "artifacts_verified": False,
            "error": str(exc),
            "remediation": (
                "The pinned embedding artifact is missing or its SHA-256 does not match the "
                "configured WORKBAY_HANDOFF_EMBEDDING_*_SHA256. Reinstall the model artifact "
                "or correct the pinned hash."
            ),
        }
    try:
        import onnxruntime  # noqa: F401 — lazy runtime probe (internal)
        from tokenizers import Tokenizer  # noqa: F401
    except Exception as exc:  # noqa: BLE001 — broken wheels may raise OSError, etc.
        return {
            "ok": True,
            "configured": True,
            "model_id": provider.model_id,
            "artifacts_verified": True,
            "runtime_available": False,
            "warning": (
                f"embedding runtime deps unavailable ({exc}); install "
                "'mcp-workbay-handoff[embeddings]' or semantic reinjection will "
                "degrade at embed time (recency fallback)"
            ),
        }
    return {
        "ok": True,
        "configured": True,
        "model_id": provider.model_id,
        "artifacts_verified": True,
        "runtime_available": True,
    }


def _semantic_reinjection_env_enabled() -> bool:
    """True when WORKBAY_REINJECT_SEMANTIC promises semantic mode (implementation note)."""
    return (resolve_env_alias("WORKBAY_REINJECT_SEMANTIC") or "").strip().lower() not in {
        "",
        "0",
        "false",
        "no",
        "off",
    }


def _emit_semantic_reinjection_startup_probe() -> None:
    """Loud boot-time probe when env-on + embedding capability is broken (implementation note).

    Reuses ``_check_embedding_provider`` (doctor parity). Env-off stays silent.
    """
    if not _semantic_reinjection_env_enabled():
        return
    probe = _check_embedding_provider()
    # Fire unless the provider is *fully* healthy: configured AND the runtime
    # imported. Checking only ``artifacts_verified is not False`` /
    # ``runtime_available is not False`` stayed silent for the flagship
    # deps-missing shape — an ImportError makes _check_embedding_provider return
    # ``{configured: False, note: ...}`` with neither key, so both `is not False`
    # tests passed and the promised-but-broken capability went unreported
    # (REV-A-001). Env-off already returned above, so any non-healthy shape here
    # (unconfigured, artifact mismatch, or missing runtime) is worth surfacing.
    healthy = probe.get("configured") is True and probe.get("runtime_available") is True
    # env-on but no model pins is a deliberate soft-degrade to recency, not a
    # broken capability — stay silent (only deps/artifact/runtime failures are loud).
    unconfigured = probe.get("unconfigured") is True
    if healthy or unconfigured:
        return
    message = probe.get("warning") or probe.get("error") or probe.get("note")
    remediation = probe.get("remediation")
    parts = ["[handoff-mcp] semantic reinjection promised (WORKBAY_REINJECT_SEMANTIC=1) but unavailable"]
    if message:
        parts.append(str(message))
    if remediation:
        parts.append(f"remediation: {remediation}")
    print("; ".join(parts), file=sys.stderr)


def _check_embedding_backfill_coverage(db_path: Path) -> dict[str, Any]:
    """Report concept-embedding + anchor coverage (internal).

    Best-effort; never raises. A pre-v15 DB (no ``concept_embeddings`` table) or
    an unused feature degrades to ``ok=True`` with a note and zero counts, so
    `doctor` always exits cleanly. The counts let an operator confirm a backfill
    actually populated the store before flipping the semantic flag on.
    """
    import sqlite3 as _sqlite3

    block: dict[str, Any] = {"ok": True, "embedded_concepts": 0, "model_ids": [], "anchor_vectors": 0}
    try:
        with _sqlite3.connect(str(db_path)) as conn:
            try:
                block["embedded_concepts"] = int(conn.execute("SELECT COUNT(*) FROM concept_embeddings").fetchone()[0])
                block["model_ids"] = [
                    str(row[0])
                    for row in conn.execute(
                        "SELECT DISTINCT model_id FROM concept_embeddings ORDER BY model_id"
                    ).fetchall()
                ]
            except _sqlite3.OperationalError as exc:
                block["note"] = f"concept_embeddings unavailable ({exc}); pre-v15 DB or feature unused"
                return block
            try:
                block["anchor_vectors"] = int(
                    conn.execute("SELECT COUNT(*) FROM session_compactions WHERE anchor_vector IS NOT NULL").fetchone()[
                        0
                    ]
                )
            except _sqlite3.OperationalError:
                block["anchor_vectors"] = 0
    except _sqlite3.Error as exc:  # noqa: BLE001 — diagnostic capture
        block["note"] = f"coverage probe failed ({exc})"
    return block


def run_doctor(config: RuntimeConfig) -> dict[str, Any]:
    configure_runtime(config)
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.exports_dir.mkdir(parents=True, exist_ok=True)

    fts5_check = _check_fts5_available()
    if not fts5_check["ok"]:
        raise RuntimeError(fts5_check.get("remediation", "SQLite FTS5 extension is not available"))

    state_dir_check = _check_state_dir_writable(config.state_dir)
    if not state_dir_check["ok"]:
        raise RuntimeError(state_dir_check.get("remediation", "state directory is not writable"))
    handoff_fts_check = _check_fts_index_health(config.db_path)

    package_src = Path(__file__).resolve().parents[1]
    launcher = package_src / "workbay_handoff_mcp_launcher.py"
    stdio_tools: list[str] = []
    stdio_probe_error: str | None = None
    cli_probe_error: str | None = None
    cli_probe_remediation: str | None = None
    # WORKBAY_HANDOFF_DOCTOR_STRICT=1 makes the stdio handshake a hard gate.
    # Default behaviour is diagnostic: capture
    # the error into stdio_startup.error and continue so fresh consumer venvs
    # — where the fastmcp Client subprocess can race or a transient stdio
    # handshake error can fire — still get an actionable JSON report instead
    # of an opaque exit-1.
    strict_mode = resolve_env_alias(
        "WORKBAY_HANDOFF_DOCTOR_STRICT",
        default="",
    ).strip().lower() in {"1", "true", "yes", "on"}
    with tempfile.TemporaryDirectory() as temp_dir:
        cli_env = _build_doctor_cli_env(__file__)

        # Run the two subprocess startup probes in parallel: each pays a full
        # Python import + package init cost (~6–7s), so running them serially
        # pushes `doctor` past the default pytest-timeout budget on loaded
        # machines. The ThreadPoolExecutor kicks off the CLI probe while the
        # asyncio event loop drives the stdio handshake; the future is
        # awaited afterwards so any CalledProcessError still surfaces.
        #
        # In default (non-strict) mode, both probes are best-effort: errors
        # are captured into stdio_probe_error / cli_probe_error and surfaced
        # in the JSON report under checks.stdio_startup.error /
        # checks.cli_fallback_startup. The doctor still exits 0 unless BOTH
        # probes fail (signal: workspace is structurally broken). In strict
        # mode (WORKBAY_HANDOFF_DOCTOR_STRICT=1) any probe failure re-raises.
        deadline_at = time.monotonic() + _resolve_doctor_deadline_seconds()
        with ThreadPoolExecutor(max_workers=1) as _probe_pool:
            cli_timeout = _remaining_deadline_seconds(deadline_at)
            cli_future = _probe_pool.submit(
                _check_cli_startup,
                config,
                cli_env,
                timeout=cli_timeout,
            )
            try:
                stdio_timeout = _resolve_doctor_stdio_timeout(
                    deadline_cap=_remaining_deadline_seconds(deadline_at),
                )
                stdio_result = asyncio.run(
                    _check_stdio_startup(
                        config,
                        launcher,
                        Path(temp_dir),
                        timeout_s=stdio_timeout,
                    )
                )
                if stdio_result["ok"]:
                    stdio_tools = stdio_result["tools"]
                else:
                    stdio_probe_error = stdio_result.get("error")
            except (Exception, OSError) as exc:  # noqa: BLE001 — diagnostic capture
                stdio_probe_error = f"{type(exc).__name__}: {exc}"
            wait_started = time.monotonic()
            try:
                cli_result = cli_future.result(timeout=_remaining_deadline_seconds(deadline_at))
            except FuturesTimeoutError:
                waited = time.monotonic() - wait_started
                cli_result = {
                    "ok": False,
                    "error": (
                        f"CLI startup probe timed out after {waited:.1f}s wait "
                        f"({_resolve_doctor_deadline_seconds():.0f}s doctor deadline exhausted)"
                    ),
                    "remediation": (
                        "The `mcp-workbay-handoff state` CLI probe did not return within "
                        "the doctor deadline. Check package install and workspace health, "
                        "or raise WORKBAY_DOCTOR_DEADLINE_SECONDS."
                    ),
                }
            if not cli_result["ok"]:
                cli_probe_error = cli_result.get("error")
                cli_probe_remediation = cli_result.get("remediation")

        if strict_mode:
            if stdio_probe_error:
                raise RuntimeError(stdio_probe_error)
            if cli_probe_error:
                raise RuntimeError(cli_probe_error)

    _registry = _build_tool_registry()
    _core_count = sum(1 for e in _registry if e.profile == "core")
    _extended_count = sum(1 for e in _registry if e.profile == "extended")

    # Portable hook semantics discovery: enumerate defined hooks and check
    # for observable evidence of each one's durable output in this workspace.
    ace_reflect_log = config.state_dir / "ace_reflect_log.jsonl"
    worker_log_dir = config.workspace_root / "logs" / "worker-daemon"
    worker_logs_found = any(worker_log_dir.glob("worker-*.jsonl")) if worker_log_dir.exists() else False
    portable_hook_semantics = [
        {
            "name": "after_review_findings_recorded",
            "trigger": "worker-daemon review turn produces new findings",
            "durable_output": ".task-state/ace_reflect_log.jsonl",
            "evidence_path": str(ace_reflect_log),
            "evidence_found": ace_reflect_log.exists(),
        },
        {
            "name": "before_close_check",
            "trigger": "handoff_close_check() is invoked",
            "durable_output": "structured readiness verdict returned synchronously",
            "evidence_path": "MCP tool handoff_close_check (always registered)",
            "evidence_found": True,
        },
        {
            "name": "after_worker_turn",
            "trigger": "worker execution turn completes",
            "durable_output": "logs/worker-daemon/worker-<lane>.jsonl",
            "evidence_path": str(worker_log_dir),
            "evidence_found": worker_logs_found,
        },
        {
            "name": "after_task_switch",
            "trigger": "switch_task() completes",
            "durable_output": "CURRENT_TASK.json regenerated for new active task",
            "evidence_path": str(config.current_task_path),
            "evidence_found": config.current_task_path.exists(),
        },
        {
            "name": "before_review_prompt_build",
            "trigger": "orchestrator or review_runner prepares review prompt for a worker turn",
            "durable_output": "scope_violation event in worker JSONL; prompt metadata in worker_event_history",
            "evidence_path": str(worker_log_dir),
            "evidence_found": worker_logs_found,
        },
    ]

    # Both stdio and CLI probes failed → workspace is structurally broken.
    # Report ok=false so non-strict callers still see a non-positive signal
    # without the doctor itself raising.
    stdio_ok = stdio_probe_error is None
    cli_ok = cli_probe_error is None
    overall_ok = stdio_ok or cli_ok

    stdio_startup_block: dict[str, Any] = {
        "ok": stdio_ok,
        "tool_count": len(stdio_tools),
        "tool_profile": config.tool_profile,
        "registry_counts": {
            "core": _core_count,
            "extended": _extended_count,
            "total": len(_registry),
        },
    }
    if stdio_probe_error is not None:
        stdio_startup_block["error"] = stdio_probe_error
    cli_startup_block: dict[str, Any] = {"ok": cli_ok}
    if cli_probe_error is not None:
        cli_startup_block["error"] = cli_probe_error
    if cli_probe_remediation is not None:
        cli_startup_block["remediation"] = cli_probe_remediation

    # internal: surface semantic-reinjection provider + backfill
    # coverage. Both are informational (opt-in feature) and do not gate
    # overall_ok; a configured-but-corrupt artifact reports its own ok=False.
    embedding_provider_check = _check_embedding_provider()
    embedding_coverage_check = _check_embedding_backfill_coverage(config.db_path)

    # internal / T13: src↔installed skew (surfacing only).
    from .package_skew import detect_src_installed_skew

    src_installed_skew_check = detect_src_installed_skew()

    from . import __version__

    return {
        "ok": overall_ok,
        "version": __version__,
        "workspace_root": str(config.workspace_root),
        "state_dir": str(config.state_dir),
        "db_path": str(config.db_path),
        "artifact_db_path": str(config.artifact_db_path),
        "current_task_path": str(config.current_task_path),
        "exports_dir": str(config.exports_dir),
        "checks": {
            "sqlite": True,
            "fts5_available": True,
            "state_dir_writable": state_dir_check,
            "handoff_fts_index": handoff_fts_check,
            "stdio_startup": stdio_startup_block,
            "cli_fallback_startup": cli_startup_block,
            "embedding_provider": embedding_provider_check,
            "embedding_backfill_coverage": embedding_coverage_check,
            "src_installed_skew": src_installed_skew_check,
        },
        "portable_hook_semantics": portable_hook_semantics,
    }
