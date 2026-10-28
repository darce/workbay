import sys
from importlib import import_module
from types import ModuleType

from workbay_protocol.version import version_of

__version__ = version_of("mcp-workbay-handoff", anchor=__file__)

from workbay_protocol.branch_naming import (
    TASK_REF_RE,
    derive_task_ref_candidates,
    extract_plan_id,
    format_suggested_branch_name,
)

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    # The MCP server API is intentionally deferred. Stop-hook consumers use
    # narrow submodules and must not pay for FastMCP's server/client graph.
    "archive": (".api", "archive"),
    "archive_task_state": (".api", "archive_task_state"),
    "artifacts": (".api", "artifacts"),
    "audit_decision_ids": (".api", "audit_decision_ids"),
    "batch_record_review_findings": (".api", "batch_record_review_findings"),
    "build_handoff_mcp": (".api", "build_handoff_mcp"),
    "build_write_actor": (".api", "build_write_actor"),
    "close_slice": (".api", "close_slice"),
    "compact_session": (".api", "compact_session"),
    "configure_runtime": (".api", "configure_runtime"),
    "continuation": (".api", "continuation"),
    "export_handoff_state": (".api", "export_handoff_state"),
    "find_related_prior_work": (".api", "find_related_prior_work"),
    "get_archived_task": (".api", "get_archived_task"),
    "get_artifact": (".api", "get_artifact"),
    "get_compaction": (".api", "get_compaction"),
    "get_handoff_state": (".api", "get_handoff_state"),
    "get_latest_compaction": (".api", "get_latest_compaction"),
    "get_review_coverage": (".api", "get_review_coverage"),
    "get_runtime_config": (".api", "get_runtime_config"),
    "get_touched_files": (".api", "get_touched_files"),
    "get_verified_tests": (".api", "get_verified_tests"),
    "handoff_close_check": (".api", "handoff_close_check"),
    "import_handoff_state": (".api", "import_handoff_state"),
    "init_state": (".api", "init_state"),
    "latest_branch_reclaim_candidate": (".api", "latest_branch_reclaim_candidate"),
    "latest_lane_landing": (".api", "latest_lane_landing"),
    "latest_reclaim_candidate": (".api", "latest_reclaim_candidate"),
    "list_active_tasks": (".api", "list_active_tasks"),
    "list_next_actions": (".api", "list_next_actions"),
    "list_review_findings": (".api", "list_review_findings"),
    "list_review_runs": (".api", "list_review_runs"),
    "load_session": (".api", "load_session"),
    "next_actions": (".api", "next_actions"),
    "post_merge_integrity_check": (".api", "post_merge_integrity_check"),
    "purge_artifacts": (".api", "purge_artifacts"),
    "record_artifact": (".api", "record_artifact"),
    "record_decision": (".api", "record_decision"),
    "record_event": (".api", "record_event"),
    "record_file_touch": (".api", "record_file_touch"),
    "record_review_finding": (".api", "record_review_finding"),
    "record_review_run": (".api", "record_review_run"),
    "record_test_result": (".api", "record_test_result"),
    "render_cold_start_compaction": (".api", "render_cold_start_compaction"),
    "render_handoff": (".api", "render_handoff"),
    "repair_review_finding_provenance": (".api", "repair_review_finding_provenance"),
    "report_blocker": (".api", "report_blocker"),
    "reset_runtime_config": (".api", "reset_runtime_config"),
    "review_findings": (".api", "review_findings"),
    "review_runs": (".api", "review_runs"),
    "run_doctor": (".api", "run_doctor"),
    "search_artifacts": (".api", "search_artifacts"),
    "search_handoff": (".api", "search_handoff"),
    "semantic_reinjection_packet": (".api", "semantic_reinjection_packet"),
    "set_handoff_state": (".api", "set_handoff_state"),
    "switch_task": (".api", "switch_task"),
    "terminal_guard_telemetry": (".api", "terminal_guard_telemetry"),
    "update_next_actions": (".api", "update_next_actions"),
    "update_review_finding": (".api", "update_review_finding"),
    "update_task_status": (".api", "update_task_status"),
    "validate_decision_id": (".api", "validate_decision_id"),
    "working_tree_integrity_check": (".api", "working_tree_integrity_check"),
    "backfill_blocker_lane_ids": (".blocker_lane_backfill", "backfill_blocker_lane_ids"),
    "CompactionRecord": (".compaction", "CompactionRecord"),
    "CompactionSettings": (".compaction", "CompactionSettings"),
    "ConsumerRootResolutionError": (".config", "ConsumerRootResolutionError"),
    "RuntimeConfig": (".config", "RuntimeConfig"),
    "PromptMetrics": (".core", "PromptMetrics"),
    "ResolvedWriteContext": (".core", "ResolvedWriteContext"),
    "ReviewFindingDetails": (".core", "ReviewFindingDetails"),
    "TokenUsage": (".core", "TokenUsage"),
    "WriteActor": (".core", "WriteActor"),
    "DashboardContext": (".dashboard_rendering", "DashboardContext"),
    "DashboardExtension": (".dashboard_rendering", "DashboardExtension"),
    "DashboardSection": (".dashboard_rendering", "DashboardSection"),
    "clear_dashboard_extensions": (".dashboard_rendering", "clear_dashboard_extensions"),
    "register_dashboard_extension": (".dashboard_rendering", "register_dashboard_extension"),
    "ReviewKind": (".enums", "ReviewKind"),
    "ReviewScopeSource": (".enums", "ReviewScopeSource"),
    "orientation_read_boundary": (".orientation_reads", "orientation_read_boundary"),
    "PlanLocation": (".plan_resolve", "PlanLocation"),
    "PlanPathNotRegistered": (".plan_resolve", "PlanPathNotRegistered"),
    "list_active_task_locations": (".plan_resolve", "list_active_task_locations"),
    "plan_show_command": (".plan_resolve", "plan_show_command"),
    "resolve_plan_location": (".plan_resolve", "resolve_plan_location"),
    "CANONICAL_BLOCKER_KINDS": (".preflight", "CANONICAL_BLOCKER_KINDS"),
    "validate_finding_resolution": (".preflight", "validate_finding_resolution"),
    "validate_review_ready": (".preflight", "validate_review_ready"),
    "RuntimeNotConfiguredError": (".runtime", "RuntimeNotConfiguredError"),
    "BranchMismatchError": (".shared_write_context", "BranchMismatchError"),
    "UnresolvedTaskContextError": (".shared_write_context", "UnresolvedTaskContextError"),
}


_LAZY_EXPORT_CACHE: dict[str, object] = {}
_MISSING = object()


class _MissingLazyExportTarget(AttributeError):
    """Signal a missing mapped attribute without conflating import failures."""


def _resolve_lazy_export(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = target
    module = import_module(module_name, __name__)
    module_dict = vars(module)
    if attribute_name not in module_dict and "__getattr__" not in module_dict:
        raise _MissingLazyExportTarget(f"lazy export {name!r} target {module_name!r}.{attribute_name!r} is missing")
    return getattr(module, attribute_name)


def __getattr__(name: str):
    value = _resolve_lazy_export(name)
    _LAZY_EXPORT_CACHE[name] = value
    globals()[name] = value
    return value


class _LazyExportPackage(ModuleType):
    """Keep lazy exports authoritative when import machinery binds sibling modules.

    Plain ``__getattr__`` only handles missing attributes, so it cannot repair a
    sibling module that import machinery has already bound. The package attribute
    therefore favors the mapped export; for a collision, ``import
    package.colliding as alias`` consequently receives the callable, while the
    real module remains importable through ``sys.modules`` and ``from
    package.colliding import ...``.
    """

    def __getattribute__(self, name: str):
        if name.startswith("_"):
            return ModuleType.__getattribute__(self, name)

        package_dict = ModuleType.__getattribute__(self, "__dict__")
        target = package_dict.get("_LAZY_EXPORTS", {}).get(name)
        if target is not None:
            cache = package_dict.get("_LAZY_EXPORT_CACHE", {})
            cached = cache.get(name, _MISSING)
            if cached is not _MISSING:
                return cached

            current = package_dict.get(name, _MISSING)
            if current is not _MISSING and not isinstance(current, ModuleType):
                return current
            try:
                value = _resolve_lazy_export(name)
            except _MissingLazyExportTarget:
                if current is not _MISSING:
                    return current
                raise
            cache[name] = value
            package_dict[name] = value
            return value
        return ModuleType.__getattribute__(self, name)

    def __setattr__(self, name: str, value: object) -> None:
        ModuleType.__setattr__(self, name, value)
        package_dict = ModuleType.__getattribute__(self, "__dict__")
        if name in package_dict.get("_LAZY_EXPORTS", {}):
            package_dict.get("_LAZY_EXPORT_CACHE", {}).pop(name, None)

    def __delattr__(self, name: str) -> None:
        ModuleType.__delattr__(self, name)
        package_dict = ModuleType.__getattribute__(self, "__dict__")
        if name in package_dict.get("_LAZY_EXPORTS", {}):
            package_dict.get("_LAZY_EXPORT_CACHE", {}).pop(name, None)


sys.modules[__name__].__class__ = _LazyExportPackage


def generate_current_task_md(task_ref: str | None = None, write_file: bool = True) -> dict:
    """Backward-compatible alias for rendering CURRENT_TASK.json."""
    return __getattr__("render_handoff")(kind="current_task", task_ref=task_ref, write_file=write_file)


def generate_dashboard_md(write_file: bool = True) -> dict:
    """Backward-compatible alias for rendering DASHBOARD.txt."""
    return __getattr__("render_handoff")(kind="dashboard", write_file=write_file)


__all__ = [
    "__version__",
    "TASK_REF_RE",
    "DashboardContext",
    "DashboardExtension",
    "DashboardSection",
    "BranchMismatchError",
    "CompactionRecord",
    "CompactionSettings",
    "ConsumerRootResolutionError",
    "UnresolvedTaskContextError",
    "PlanLocation",
    "PlanPathNotRegistered",
    "PromptMetrics",
    "ResolvedWriteContext",
    "RuntimeConfig",
    "RuntimeNotConfiguredError",
    "ReviewFindingDetails",
    "ReviewKind",
    "ReviewScopeSource",
    "TokenUsage",
    "WriteActor",
    "artifacts",
    "archive",
    "archive_task_state",
    "audit_decision_ids",
    "backfill_blocker_lane_ids",
    "build_handoff_mcp",
    "build_write_actor",
    "clear_dashboard_extensions",
    "close_slice",
    "compact_session",
    "configure_runtime",
    "continuation",
    "derive_task_ref_candidates",
    "export_handoff_state",
    "extract_plan_id",
    "format_suggested_branch_name",
    "generate_current_task_md",
    "generate_dashboard_md",
    "orientation_read_boundary",
    "render_handoff",
    "register_dashboard_extension",
    "get_archived_task",
    "get_artifact",
    "get_compaction",
    "get_handoff_state",
    "get_latest_compaction",
    "get_review_coverage",
    "get_touched_files",
    "get_verified_tests",
    "get_runtime_config",
    "handoff_close_check",
    "import_handoff_state",
    "init_state",
    "find_related_prior_work",
    "latest_lane_landing",
    "latest_branch_reclaim_candidate",
    "latest_reclaim_candidate",
    "load_session",
    "list_active_task_locations",
    "list_active_tasks",
    "list_next_actions",
    "list_review_findings",
    "list_review_runs",
    "next_actions",
    "plan_show_command",
    "post_merge_integrity_check",
    "purge_artifacts",
    "record_artifact",
    "record_decision",
    "record_event",
    "record_file_touch",
    "record_review_finding",
    "batch_record_review_findings",
    "record_review_run",
    "record_test_result",
    "render_cold_start_compaction",
    "repair_review_finding_provenance",
    "report_blocker",
    "resolve_plan_location",
    "reset_runtime_config",
    "review_findings",
    "review_runs",
    "run_doctor",
    "search_artifacts",
    "search_handoff",
    "semantic_reinjection_packet",
    "set_handoff_state",
    "switch_task",
    "terminal_guard_telemetry",
    "validate_decision_id",
    "validate_finding_resolution",
    "validate_review_ready",
    "CANONICAL_BLOCKER_KINDS",
    "update_task_status",  # library-only; not a served MCP tool
    "update_next_actions",
    "update_review_finding",
    "working_tree_integrity_check",
]
