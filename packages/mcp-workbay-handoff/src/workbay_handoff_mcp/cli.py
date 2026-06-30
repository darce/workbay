from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, cast

from . import __version__
from .api import (
    ArchiveParam,
    ArgSpec,
    ArtifactsParam,
    CompactionParam,
    IntegrityCheckParam,
    NextActionsParam,
    RecordEventParam,
    ReviewFindingsParam,
    ReviewRunsParam,
    TerminalGuardTelemetryParam,
    TouchedFilesParam,
    ValidateParam,
    archive,
    artifacts,
    build_handoff_mcp,
    compaction,
    configure_runtime,
    export_handoff_state,
    get_handoff_state,
    get_verified_tests,
    import_handoff_state,
    init_state,
    integrity_check,
    next_actions,
    record_event,
    render_handoff,
    review_findings,
    review_runs,
    run_doctor,
    search_handoff,
    set_handoff_state,
    terminal_guard_telemetry,
    touched_files,
    validate,
)
from .config import RuntimeConfig


def _print_json(payload: str | dict) -> None:
    if isinstance(payload, str):
        print(payload)
        return
    print(json.dumps(payload, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Registry infrastructure
# ---------------------------------------------------------------------------


@dataclass
class CliEntry:
    """Registry entry for a single CLI sub-command."""

    name: str
    dispatch: Callable[[argparse.Namespace], Any]
    description: str = ""
    args: list[ArgSpec] = field(default_factory=list)


def _auto_dispatch(handler: Callable[..., Any], cli_args: list[ArgSpec]) -> Callable[[argparse.Namespace], Any]:
    """Generate a dispatch function from ArgSpec definitions.

    Works for tools where every ArgSpec dest matches the handler parameter name directly.
    Use ``_CLI_DISPATCH_OVERRIDES`` for tools that require custom logic (negations, dict
    construction, file reading, etc.).
    """

    def dispatch(args: argparse.Namespace) -> Any:
        kwargs: dict[str, Any] = {}
        for spec in cli_args:
            if spec.name.startswith("-"):
                dest = spec.dest or spec.name.lstrip("-").replace("-", "_")
            else:
                dest = spec.dest or spec.name
            kwargs[dest] = getattr(args, dest, None)
        return handler(**kwargs)

    return dispatch


def _add_arg(sub: argparse.ArgumentParser, spec: ArgSpec) -> None:
    """Add one ArgSpec to a subparser."""
    is_positional = not spec.name.startswith("-")
    kwargs: dict[str, Any] = {}
    if spec.help:
        kwargs["help"] = spec.help
    if spec.action:
        kwargs["action"] = spec.action
        if spec.action == "store_true":
            kwargs.setdefault("default", False)
        elif spec.action == "append":
            kwargs["default"] = spec.default if spec.default is not None else []
    elif not is_positional:
        if spec.type is not str:
            kwargs["type"] = spec.type
        kwargs["default"] = spec.default
    else:
        # positional — type and default handled by nargs
        if spec.type is not str:
            kwargs["type"] = spec.type
    if not is_positional and spec.required:
        kwargs["required"] = True
    if spec.choices:
        kwargs["choices"] = spec.choices
    if spec.nargs:
        kwargs["nargs"] = spec.nargs
    if spec.dest and not is_positional:
        kwargs["dest"] = spec.dest
    sub.add_argument(spec.name, **kwargs)


# ---------------------------------------------------------------------------
# Command dispatch functions
# ---------------------------------------------------------------------------
# Most MCP-registry tools are dispatched automatically via _auto_dispatch() in
# _build_cli_registry(). Only tools that require custom argument handling
# (negations, dict construction, file reading) need an explicit function here.


def _dispatch_review_findings(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.task_ref is not None:
        payload["task_ref"] = args.task_ref

    if args.operation in ("resolve", "update"):
        actor: dict[str, str] = {}
        actor_commit_sha = getattr(args, "actor_commit_sha", None)
        actor_branch = getattr(args, "actor_branch", None)
        if actor_commit_sha is not None:
            actor["commit_sha"] = actor_commit_sha
        if actor_branch is not None:
            actor["branch"] = actor_branch
        if actor:
            payload["actor"] = actor

    if args.operation == "record":
        payload["session"] = args.session
        payload["finding_id"] = args.finding_id
        payload["severity"] = args.severity
        payload["file_path"] = args.file_path
        payload["description"] = args.description
        details: dict[str, Any] = {}
        if args.line_start is not None:
            details["line_start"] = args.line_start
        if args.line_end is not None:
            details["line_end"] = args.line_end
        if args.fix:
            details["fix"] = args.fix
        if details:
            payload["details"] = details
        if args.review_mode is not None:
            payload["review_mode"] = args.review_mode
    elif args.operation == "batch_record":
        payload["session"] = args.session
        findings_json = args.findings_json
        if findings_json is None and args.findings_file:
            findings_json = Path(args.findings_file).read_text()
        payload["findings"] = json.loads(findings_json or "[]")
    elif args.operation == "resolve":
        if args.session is not None:
            payload["session"] = args.session
        if args.resolve_finding_id:
            payload["finding_ids"] = args.resolve_finding_id
        payload["all_open"] = args.all_open
        if args.resolution_notes is not None:
            payload["resolution_notes"] = args.resolution_notes
        if args.verification_evidence is not None:
            payload["verification_evidence"] = args.verification_evidence
    elif args.operation == "integrate":
        if args.integration_ref is not None:
            payload["integration_ref"] = args.integration_ref
    elif args.operation == "disposition":
        if args.finding_id is None:
            raise SystemExit("--finding-id is required when --operation=disposition")
        if args.task_ref is None:
            raise SystemExit("--task-ref is required when --operation=disposition")
        if args.status is None:
            raise SystemExit("--status is required when --operation=disposition")
        payload["finding_id"] = args.finding_id
        payload["status"] = args.status
        if args.resolution_notes is not None:
            payload["resolution_notes"] = args.resolution_notes
    elif args.operation == "update":
        payload["status"] = args.status
        if args.finding_id is not None:
            payload["finding_id"] = args.finding_id
        if args.finding_db_id is not None:
            payload["finding_db_id"] = args.finding_db_id
        if args.resolution_notes is not None:
            payload["resolution_notes"] = args.resolution_notes
        if args.reopen_reason is not None:
            payload["reopen_reason"] = args.reopen_reason
        if args.verified_commit_sha is not None:
            payload["verified_commit_sha"] = args.verified_commit_sha
        if args.verification_evidence is not None:
            payload["verification_evidence"] = args.verification_evidence
        if args.session is not None:
            payload["session"] = args.session
    else:
        if args.status is not None:
            payload["status"] = args.status
        if args.severity is not None:
            payload["severity"] = args.severity
        payload["limit"] = args.limit
        payload["offset"] = args.offset
        payload["detail"] = args.detail
        if args.review_mode is not None:
            payload["review_mode"] = args.review_mode
        if args.finding_id is not None:
            payload["finding_id"] = args.finding_id
        if args.finding_db_id is not None:
            payload["finding_db_id"] = args.finding_db_id

    return review_findings(review=cast(ReviewFindingsParam, payload))


def _dispatch_review_runs(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.task_ref is not None:
        payload["task_ref"] = args.task_ref

    if args.operation == "record":
        payload["review_run_id"] = args.review_run_id
        payload["session"] = args.session
        payload["subject_path"] = args.subject_path
        payload["subject_kind"] = args.subject_kind
        payload["review_mode"] = args.review_mode
        if args.verdict is not None:
            payload["verdict"] = args.verdict
        if args.verdict_decision is not None:
            payload["verdict_decision"] = args.verdict_decision
    elif args.operation == "list":
        if args.subject_path is not None:
            payload["subject_path"] = args.subject_path
        if args.review_mode is not None:
            payload["review_mode"] = args.review_mode
        if args.verdict is not None:
            payload["verdict"] = args.verdict
        payload["limit"] = args.limit
        payload["offset"] = args.offset
    else:
        if args.subject_path is not None:
            payload["subject_path"] = args.subject_path

    return review_runs(review=cast(ReviewRunsParam, payload))


def _dispatch_terminal_guard_telemetry(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.task_ref is not None:
        payload["task_ref"] = args.task_ref

    if args.operation == "record":
        if args.worktree_path is not None:
            payload["worktree_path"] = args.worktree_path
        payload["harness"] = args.harness
        payload["tool_name"] = args.tool_name
        payload["decision"] = args.decision
        if args.trigger is not None:
            payload["trigger"] = args.trigger
        if args.native_tool_hint is not None:
            payload["native_tool_hint"] = args.native_tool_hint
        payload["command_preview"] = args.command_preview
        payload["policy_version"] = args.policy_version
        payload["policy_source"] = args.policy_source
        if args.fallback_source is not None:
            payload["fallback_source"] = args.fallback_source
        if args.created_at is not None:
            payload["created_at"] = args.created_at
    elif args.operation == "replay":
        if args.spool_path is not None:
            payload["spool_path"] = args.spool_path
    else:
        if args.decision is not None:
            payload["decision"] = args.decision
        if args.harness is not None:
            payload["harness"] = args.harness
        if args.tool_name is not None:
            payload["tool_name"] = args.tool_name
        payload["limit"] = args.limit
        payload["offset"] = args.offset

    return terminal_guard_telemetry(telemetry=cast(TerminalGuardTelemetryParam, payload))


def _dispatch_get_verified_tests(args: argparse.Namespace) -> Any:
    passed: bool | None = None
    if args.passed == "true":
        passed = True
    elif args.passed == "false":
        passed = False
    return get_verified_tests(
        task_ref=args.task_ref,
        lane_id=args.lane_id,
        branch=args.branch,
        commit_sha=args.commit_sha,
        passed=passed,
        include_traces=args.include_traces,
        correlated_file=args.correlated_file,
        correlation_window_minutes=args.correlation_window_minutes,
        exclude_never_passed=args.exclude_never_passed,
        limit=args.limit,
        offset=args.offset,
    )


def _dispatch_event_record(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"event_kind": args.event_kind}
    if args.event_id is not None:
        payload["event_id"] = args.event_id
    if args.task_ref is not None:
        payload["task_ref"] = args.task_ref

    actor: dict[str, str] = {}
    for flag in ("branch", "commit_sha", "lane_id"):
        value = getattr(args, flag, None)
        if value is not None:
            actor[flag] = value
    if actor:
        payload["actor"] = actor

    if args.event_kind == "decision":
        payload["session"] = args.session
        payload["decision"] = args.decision
        if args.rationale is not None:
            payload["rationale"] = args.rationale
        if args.input_tokens is not None:
            payload["input_tokens"] = args.input_tokens
        if args.output_tokens is not None:
            payload["output_tokens"] = args.output_tokens
        if args.total_tokens is not None:
            payload["total_tokens"] = args.total_tokens
        if args.changed_files:
            payload["changed_files"] = args.changed_files
    elif args.event_kind == "test_result":
        payload["session"] = args.session
        payload["command"] = args.command
        payload["passed"] = args.passed
        if args.result is not None:
            payload["result"] = args.result
        if args.traces:
            payload["traces"] = args.traces
        if args.exit_code is not None:
            payload["exit_code"] = args.exit_code
    else:
        payload["operation"] = args.operation
        if args.description is not None:
            payload["description"] = args.description
        if args.blocker_id is not None:
            payload["blocker_id"] = args.blocker_id

    return record_event(event=cast(RecordEventParam, payload))


def _resolve_installed_package_version(package_name: str) -> str | None:
    """Best-effort version provenance for the named (module) package.

    Tries the name as a distribution first, then maps module -> dist via
    ``packages_distributions`` (e.g. ``workbay_handoff_mcp`` ->
    ``mcp-workbay-handoff``). Returns None when unresolvable — the
    error row still lands, just without version provenance.
    """
    try:
        from importlib import metadata as importlib_metadata  # noqa: PLC0415

        try:
            return importlib_metadata.version(package_name)
        except importlib_metadata.PackageNotFoundError:
            dists = importlib_metadata.packages_distributions().get(package_name)
            if dists:
                return importlib_metadata.version(dists[0])
    except Exception:
        pass
    return None


def _dispatch_errors_record(args: argparse.Namespace) -> dict:
    from .agent_errors import record_agent_error_direct  # noqa: PLC0415

    package_version = args.package_version
    if package_version is None and args.package_name:
        package_version = _resolve_installed_package_version(args.package_name)

    return record_agent_error_direct(
        error_class=args.error_class,
        summary=args.summary,
        detail=args.detail,
        tool_name=args.tool_name,
        command_preview=args.command_preview,
        package_name=args.package_name,
        package_version=package_version,
        workbay_release=args.workbay_release,
        harness=args.harness,
        task_ref=args.task_ref,
    )


def _dispatch_errors_replay_spool(args: argparse.Namespace) -> dict:
    from .agent_errors import replay_agent_error_spool  # noqa: PLC0415

    return replay_agent_error_spool(
        spool_path=Path(args.spool_path) if args.spool_path else None,
    )


def _dispatch_errors_report(args: argparse.Namespace) -> dict:
    from .agent_errors_report import errors_report  # noqa: PLC0415

    sources = [Path(s) for s in args.sources] if args.sources else None
    return errors_report(sources, since=args.since)


def _dispatch_errors_export(args: argparse.Namespace) -> None:
    from .agent_errors_report import errors_export  # noqa: PLC0415

    result = errors_export(
        db_path=Path(args.source) if args.source else None,
        since=args.since,
    )
    if not result.get("ok"):
        _print_json(result)
        return
    lines = "".join(json.dumps(row, sort_keys=True) + "\n" for row in result["rows"])
    if args.output:
        Path(args.output).write_text(lines, encoding="utf-8")
        _print_json(
            {
                "ok": True,
                "exported": len(result["rows"]),
                "output": args.output,
                "db_path": result["db_path"],
                "since": result["since"],
            }
        )
        return
    sys.stdout.write(lines)


def _dispatch_next_actions(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.task_ref is not None:
        payload["task_ref"] = args.task_ref

    if args.operation == "list":
        if args.lane_id is not None:
            payload["lane_id"] = args.lane_id
        if args.status is not None:
            payload["status"] = args.status
        payload["limit"] = args.limit
        payload["offset"] = args.offset
    else:
        if args.action_id is not None:
            payload["action_id"] = args.action_id
        if args.action is not None:
            payload["action"] = args.action
        if args.priority is not None:
            payload["priority"] = args.priority
        if args.status is not None and args.operation == "update":
            payload["status"] = args.status

    return next_actions(action=cast(NextActionsParam, payload))


def _dispatch_artifacts(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.task_ref is not None:
        payload["task_ref"] = args.task_ref
    if args.lane_id is not None:
        payload["lane_id"] = args.lane_id
    if args.app_root is not None:
        payload["app_root"] = args.app_root

    if args.operation == "record":
        payload["source_kind"] = args.source_kind
        payload["source_label"] = args.source_label
        payload["content_type"] = args.content_type
        if args.summary is not None:
            payload["summary"] = args.summary
        content = args.content
        if content is None and args.content_file:
            content = Path(args.content_file).read_text()
        payload["content"] = content or ""
        if args.metadata_json is not None:
            payload["metadata"] = json.loads(args.metadata_json)
    elif args.operation == "search":
        if args.queries is not None:
            payload["queries"] = args.queries
        if args.source_kind is not None:
            payload["source_kind"] = args.source_kind
        if args.content_type is not None:
            payload["content_type"] = args.content_type
        payload["limit"] = args.limit
        payload["offset"] = args.offset
        payload["detail"] = args.detail
        if args.fields is not None:
            payload["fields"] = args.fields
    elif args.operation == "get":
        if args.source_id is not None:
            payload["source_id"] = args.source_id
        if args.source_label is not None:
            payload["source_label"] = args.source_label
        payload["include_terms"] = args.include_terms
        payload["top_n_terms"] = args.top_n_terms
        payload["detail"] = args.detail
        if args.fields is not None:
            payload["fields"] = args.fields
    else:
        if args.older_than_days is not None:
            payload["older_than_days"] = args.older_than_days

    return artifacts(artifact=cast(ArtifactsParam, payload))


def _dispatch_render_handoff(args: argparse.Namespace) -> Any:
    return render_handoff(
        kind=args.kind,
        task_ref=getattr(args, "task_ref", None),
        write_file=not args.no_write,
    )


def _dispatch_export(args: argparse.Namespace) -> Any:
    return export_handoff_state(
        task_ref=args.task_ref,
        output_path=args.output_path,
        include_markdown=not args.no_markdown,
    )


def _dispatch_artifact_list(args: argparse.Namespace) -> Any:
    return artifacts(
        artifact=cast(
            ArtifactsParam,
            {
                "operation": "search",
                "task_ref": args.task_ref,
                "lane_id": args.lane_id,
                "app_root": args.app_root,
                "source_kind": args.source_kind,
                "limit": args.limit,
                "offset": args.offset,
                "detail": args.detail,
                "fields": args.fields,
            },
        )
    )


def _dispatch_artifact_terms(args: argparse.Namespace) -> Any:
    return artifacts(
        artifact=cast(
            ArtifactsParam,
            {
                "operation": "get",
                "source_id": args.source_id,
                "task_ref": args.task_ref,
                "source_label": args.source_label,
                "include_terms": True,
                "top_n_terms": args.top_n,
                "detail": args.detail,
                "fields": args.fields,
            },
        )
    )


# ---------------------------------------------------------------------------
# CLI registry
# ---------------------------------------------------------------------------


# Tools that need custom dispatch logic (negation flags, dict construction,
# or file-reading side effects). All other MCP tools use _auto_dispatch().
def _dispatch_validate(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"kind": args.kind}
    if args.kind == "decision_id":
        if args.decision is None:
            raise SystemExit("--decision is required when --kind=decision_id")
        payload["decision"] = args.decision
        if args.decision_kind is not None:
            payload["decision_kind"] = args.decision_kind
    else:  # kind == "write"
        if args.tool_name is None:
            raise SystemExit("--tool-name is required when --kind=write")
        if args.payload_json is None:
            raise SystemExit("--payload-json is required when --kind=write")
        try:
            inner_payload = json.loads(args.payload_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"--payload-json is not valid JSON: {exc}") from exc
        if not isinstance(inner_payload, dict):
            raise SystemExit("--payload-json must decode to a JSON object")
        payload["tool_name"] = args.tool_name
        payload["payload"] = inner_payload
    return validate(payload=cast(ValidateParam, payload))


def _dispatch_compaction(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.operation == "record":
        if args.transcript_path is None:
            raise SystemExit("--transcript-path is required when --operation=record")
        if args.task_ref is None:
            raise SystemExit("--task-ref is required when --operation=record")
        if args.harness is None:
            raise SystemExit("--harness is required when --operation=record")
        if args.session_id is None:
            raise SystemExit("--session-id is required when --operation=record")
        payload["transcript_path"] = args.transcript_path
        payload["task_ref"] = args.task_ref
        payload["harness"] = args.harness
        payload["session_id"] = args.session_id
    elif args.operation == "get":
        if args.compaction_id is None:
            raise SystemExit("--compaction-id is required when --operation=get")
        payload["compaction_id"] = args.compaction_id
    elif args.operation == "get_latest":
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
    else:  # disable | enable | status (internal)
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
    return compaction(payload=cast(CompactionParam, payload))


def _dispatch_integrity_check(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"kind": args.kind}
    if args.kind == "working_tree":
        if args.workspace_root is not None:
            payload["workspace_root"] = args.workspace_root
        if args.expected_dirty:
            payload["expected_dirty"] = args.expected_dirty
    elif args.kind == "post_merge":
        if args.merged_sha is None:
            raise SystemExit("--merged-sha is required when --kind=post_merge")
        payload["merged_sha"] = args.merged_sha
        payload["expected_changed_files"] = args.expected_changed_files or []
        if args.workspace_root is not None:
            payload["workspace_root"] = args.workspace_root
    else:  # kind == "close"
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
        payload["allow_no_active_task"] = args.allow_no_active_task
        payload["enforce"] = args.enforce
        payload["require_fresh_tests"] = args.require_fresh_tests
        if args.current_commit_sha is not None:
            payload["current_commit_sha"] = args.current_commit_sha
    return integrity_check(payload=cast(IntegrityCheckParam, payload))


def _dispatch_archive(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.operation == "archive":
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
        if args.notes is not None:
            payload["notes"] = args.notes
        payload["clear_active_if_matches"] = args.clear_active_if_matches
        payload["prune_working_rows"] = args.prune_working_rows
        payload["allow_destructive_clear"] = args.allow_destructive_clear
        payload["cascade_maint_review"] = args.cascade_maint_review
    elif args.operation == "gc":
        payload["apply"] = args.apply
    elif args.operation == "reap":
        payload["apply"] = args.apply
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
    elif args.operation == "reap_scratch":
        payload["apply"] = args.apply
    elif args.operation == "reap_done":
        payload["apply"] = args.apply
    elif args.operation == "retention":
        payload["apply"] = args.apply
        if getattr(args, "older_than_days", None) is not None:
            payload["older_than_days"] = args.older_than_days
    else:  # operation == "get"
        if args.task_ref is None:
            raise SystemExit("--task-ref is required when --operation=get")
        payload["task_ref"] = args.task_ref
        payload["include_snapshot"] = args.include_snapshot
    return archive(payload=cast(ArchiveParam, payload))


def _dispatch_touched_files(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {"operation": args.operation}
    if args.operation == "record":
        if args.file_path is None:
            raise SystemExit("--file-path is required when --operation=record")
        if args.change_kind is None:
            raise SystemExit("--change-kind is required when --operation=record")
        payload["file_path"] = args.file_path
        payload["change_kind"] = args.change_kind
        if args.session is not None:
            payload["session"] = args.session
        if args.commit_sha is not None:
            payload["commit_sha"] = args.commit_sha
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
    else:  # operation == "list"
        if args.task_ref is not None:
            payload["task_ref"] = args.task_ref
        payload["limit"] = args.limit
        payload["offset"] = args.offset
    return touched_files(payload=cast(TouchedFilesParam, payload))


def _dispatch_set_handoff_state(args: argparse.Namespace) -> Any:
    """Custom dispatch for `set` — builds an actor override from --commit-sha / --branch.

    internal: the auto-dispatch can't represent the actor block because
    `set_handoff_state` accepts a structured `actor` parameter rather than flat
    branch/commit_sha args. We mirror the actor-block plumbing already used by
    `_dispatch_event_record` so explicit-commit callers (slice-commit) can write
    a known projection without leaning on the resolver's stored-row fallback.
    """
    kwargs: dict[str, Any] = {
        "task_ref": args.task_ref,
        "objective": args.objective,
        "focus": args.focus,
        "status": args.status,
        "expected_revision": args.expected_revision,
        "target_branch": args.target_branch,
        "target_worktree_path": args.target_worktree_path,
        "task_plan_path": args.task_plan_path,
        "event_id": args.event_id,
        "status_only": args.status_only,
    }
    actor: dict[str, str] = {}
    for flag in ("branch", "commit_sha"):
        value = getattr(args, flag, None)
        if value is not None:
            actor[flag] = value
    if actor:
        kwargs["actor"] = actor
    return set_handoff_state(**kwargs)


_CLI_DISPATCH_OVERRIDES: dict[str, Callable[[argparse.Namespace], Any]] = {
    "set_handoff_state": _dispatch_set_handoff_state,
    "record_event": _dispatch_event_record,
    "next_actions": _dispatch_next_actions,
    "review_findings": _dispatch_review_findings,
    "review_runs": _dispatch_review_runs,
    "terminal_guard_telemetry": _dispatch_terminal_guard_telemetry,
    "get_verified_tests": _dispatch_get_verified_tests,
    "artifacts": _dispatch_artifacts,
    "render_handoff": _dispatch_render_handoff,  # CLI name: render-handoff
    "export_handoff_state": _dispatch_export,
    "validate": _dispatch_validate,
    "compaction": _dispatch_compaction,
    "touched_files": _dispatch_touched_files,
    "integrity_check": _dispatch_integrity_check,
    "archive": _dispatch_archive,
}


def _build_cli_registry() -> list[CliEntry]:
    from .api import _build_tool_registry  # noqa: PLC0415

    # Build entries for MCP tools that declare a cli_name.
    registry: list[CliEntry] = []
    for tool_entry in _build_tool_registry():
        if tool_entry.cli_name is None:
            continue
        override = _CLI_DISPATCH_OVERRIDES.get(tool_entry.name)
        dispatch_fn = override if override is not None else _auto_dispatch(tool_entry.handler, tool_entry.cli_args)
        registry.append(
            CliEntry(
                name=tool_entry.cli_name,
                dispatch=dispatch_fn,
                description=tool_entry.description,
                args=tool_entry.cli_args,
            )
        )

    # CLI-only extras: artifact variants with slightly different arg shapes.
    registry.extend(
        [
            # --- artifact extras (CLI variants with slightly different arg shapes) ---
            CliEntry(
                name="artifact-list",
                dispatch=_dispatch_artifact_list,
                description="List artifact sources.",
                args=[
                    ArgSpec("--task-ref"),
                    ArgSpec("--lane-id"),
                    ArgSpec("--app-root"),
                    ArgSpec("--source-kind"),
                    ArgSpec("--limit", type=int, default=50),
                    ArgSpec("--offset", type=int, default=0),
                    ArgSpec("--detail", default="full", choices=["full", "summary"]),
                    ArgSpec("--fields", help="Comma-separated fields to keep in each listed source."),
                ],
            ),
            CliEntry(
                name="artifact-terms",
                dispatch=_dispatch_artifact_terms,
                description="Return artifact with distinctive terms.",
                args=[
                    ArgSpec("--source-id", type=int),
                    ArgSpec("--task-ref"),
                    ArgSpec("--source-label"),
                    ArgSpec("--top-n", type=int, default=10),
                    ArgSpec("--detail", default="full", choices=["full", "summary"]),
                    ArgSpec("--fields", help="Comma-separated fields to keep in the returned source."),
                ],
            ),
        ]
    )

    return registry


# ---------------------------------------------------------------------------
# Parser and main
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Portable agent handoff MCP server")
    parser.add_argument(
        "--version",
        action="version",
        version=f"mcp-workbay-handoff {__version__}",
    )
    parser.add_argument("--workspace-root")
    parser.add_argument("--state-dir")
    parser.add_argument("--current-task-path")
    # internal: parity with sibling CLIs (compaction_cli, plan_cli).
    # Picked up by RuntimeConfig.from_args() precedence (arg → env).
    parser.add_argument("--dashboard-path")
    parser.add_argument("--exports-dir")
    parser.add_argument(
        "--tool-profile",
        default=None,
        choices=["all"],
        help="Tool-profile selector. Only the unified 'all' surface is supported.",
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # Special-case commands not in the generic registry
    subparsers.add_parser("serve-stdio")
    http_parser = subparsers.add_parser("serve-http")
    http_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address to bind to (default: 127.0.0.1)",
    )
    http_parser.add_argument(
        "--port",
        type=int,
        default=8741,
        help="Port to bind to (default: 8741)",
    )
    subparsers.add_parser("doctor")
    # internal: direct-path agent-error write for harness
    # hooks. Special-cased (not registry-driven) because it must not
    # touch the configured runtime DB path — it resolves the primary
    # repo state via the git common dir and spools on schema mismatch.
    errors_record_parser = subparsers.add_parser(
        "errors-record",
        help=(
            "Record a workbay-related agent error via the direct primary-DB path; "
            "spools to .task-state/agent-errors-spool.jsonl when the DB schema "
            "version differs from this package."
        ),
    )
    errors_record_parser.add_argument("--error-class", required=True)
    errors_record_parser.add_argument("--summary", required=True)
    errors_record_parser.add_argument("--detail")
    errors_record_parser.add_argument("--tool-name")
    errors_record_parser.add_argument("--command-preview")
    errors_record_parser.add_argument("--package-name")
    errors_record_parser.add_argument("--package-version")
    errors_record_parser.add_argument("--workbay-release")
    errors_record_parser.add_argument("--harness", default="hook")
    errors_record_parser.add_argument("--task-ref")
    # Replay half of the spool contract (review finding REV-B-001):
    # drain agent-errors-spool.jsonl into the primary DB once a
    # schema-matching install runs. Mirrors terminal_guard_telemetry's
    # operation=replay precedent.
    errors_replay_parser = subparsers.add_parser(
        "errors-replay-spool",
        help=(
            "Replay spooled agent-error events from "
            ".task-state/agent-errors-spool.jsonl into the primary repo DB; "
            "replayed lines are removed, failed lines kept."
        ),
    )
    errors_replay_parser.add_argument(
        "--spool-path",
        help=(
            "Explicit spool file to drain (default: primary repo .task-state "
            "spool). Overrides only the spool location — the target DB is "
            "always the primary repo's handoff.db resolved from the cwd."
        ),
    )
    # internal: harvest surfaces. Special-cased for the same
    # reason as errors-record — they read handoff.db files (or export
    # bundles) directly, never the configured runtime DB.
    errors_report_parser = subparsers.add_parser(
        "errors-report",
        help=(
            "Cluster agent_errors rows by (error_class, package_name) with "
            "version ranges, counts, and representative samples. No --source "
            "reads the primary repo DB; pass N handoff.db paths or "
            "errors-export bundles to merge consumer repos."
        ),
    )
    errors_report_parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=None,
        help="A handoff.db path or errors-export .jsonl bundle (repeatable).",
    )
    errors_report_parser.add_argument("--since", help="Keep rows with last_seen_at >= this timestamp.")
    errors_export_parser = subparsers.add_parser(
        "errors-export",
        help=(
            "Emit redacted agent_errors rows as a JSONL bundle for the "
            "workbay maintainer; reads the primary repo DB unless --source "
            "names one."
        ),
    )
    errors_export_parser.add_argument("--source", help="handoff.db path to export (default: primary repo DB).")
    errors_export_parser.add_argument("--since", help="Keep rows with last_seen_at >= this timestamp.")
    errors_export_parser.add_argument(
        "--output",
        help="Write the JSONL bundle here and print a receipt; omit to stream JSONL to stdout.",
    )
    init_state_parser = subparsers.add_parser("init-state")
    init_state_parser.add_argument(
        "--check",
        action="store_true",
        help="Report state initialization status without creating files",
    )
    init_state_parser.add_argument(
        "--force-reuse-state",
        action="store_true",
        help="Allow reuse of pre-existing state even when foreign-state checks would refuse",
    )
    init_state_parser.add_argument(
        "--expected-remote-url",
        help="Require an adjacent .workbay-bootstrap.json manifest to match this remote_url before reuse",
    )

    # Registry-driven commands
    for entry in _build_cli_registry():
        sub = subparsers.add_parser(entry.name, help=entry.description)
        for spec in entry.args:
            _add_arg(sub, spec)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config = RuntimeConfig.from_args(args)
    configure_runtime(config)

    # Special-case commands
    if args.subcommand == "serve-stdio":
        build_handoff_mcp(config).run(transport="stdio")
        return
    if args.subcommand == "serve-http":
        build_handoff_mcp(config).run(
            transport="streamable-http",
            host=args.host,
            port=args.port,
        )
        return
    if args.subcommand == "doctor":
        _print_json(run_doctor(config))
        return
    if args.subcommand == "errors-record":
        _print_json(_dispatch_errors_record(args))
        return
    if args.subcommand == "errors-replay-spool":
        _print_json(_dispatch_errors_replay_spool(args))
        return
    if args.subcommand == "errors-report":
        _print_json(_dispatch_errors_report(args))
        return
    if args.subcommand == "errors-export":
        _dispatch_errors_export(args)
        return
    if args.subcommand == "init-state":
        _print_json(
            init_state(
                config,
                check=args.check,
                force_reuse_state=args.force_reuse_state,
                expected_remote_url=args.expected_remote_url,
            )
        )
        return
    # Registry-driven dispatch
    registry_map = {entry.name: entry for entry in _build_cli_registry()}
    entry = registry_map.get(args.subcommand)
    if entry is not None:
        _print_json(entry.dispatch(args))
        return

    parser.error(f"Unknown command: {args.subcommand}")


# internal: without this guard, `python -m workbay_handoff_mcp.cli`
# imports the module and exits silently doing nothing. The real entry points
# are the console script and the package __main__.py; this makes direct module
# invocation run main() too instead of being a silent no-op trap.
if __name__ == "__main__":
    main()
