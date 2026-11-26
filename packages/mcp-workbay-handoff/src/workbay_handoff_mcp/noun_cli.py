"""WorkBay front-door noun CLI for ``mcp-workbay-handoff``.

Projects the existing handoff CLI registry (:func:`workbay_handoff_mcp.cli._build_cli_registry`)
onto the ``workbay`` front-door nouns ``handoff``, ``task``, ``finding``, ``continuation``,
and ``search``. Every verb subparser is constructed directly from its registry
:class:`~workbay_handoff_mcp.cli.CliEntry` using :func:`~workbay_handoff_mcp.cli._add_arg`
— zero :class:`~workbay_handoff_mcp.api.ArgSpec` definitions are duplicated here.

All JSON emission and exit-code mapping flows through
:mod:`workbay_protocol.cli_contract`.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from workbay_protocol.cli_contract import ExitCode, JsonArgumentParser, run_json_command

from . import cli as _cli
from . import ingest_cli as _ingest_cli
from .api import configure_runtime
from .config import RuntimeConfig
from .orientation_reads import resolve_harness


@dataclass(frozen=True)
class VerbSpec:
    """Declarative binding from ``workbay <noun> <path...>`` to one :class:`CliEntry`."""

    noun: str
    path: tuple[str, ...]
    cli_entry_name: str
    fixed_args: Mapping[str, Any] = field(default_factory=dict)
    default_overrides: Mapping[str, Any] = field(default_factory=dict)
    surface_class: str = "query"
    help: str = ""


@dataclass(frozen=True)
class CustomVerbSpec:
    """Declarative metadata for a CLI-only noun verb not backed by a single :class:`CliEntry`."""

    noun: str
    path: tuple[str, ...]
    surface_class: str = "action"
    help: str = ""


# Canonical noun -> verb -> CliEntry projection table.
# Note: ``handoff decision list`` is intentionally omitted because neither
# ``handoff-rows`` (task rows) nor ``audit-decisions`` (decision-ID lint) is a
# general decision-list read, and Wave 1 adds zero new business logic or SQL.
NOUN_VERBS: tuple[VerbSpec, ...] = (
    # task
    VerbSpec(
        noun="task",
        path=("state",),
        cli_entry_name="state",
        surface_class="query",
        help="Read current task handoff state.",
    ),
    VerbSpec(
        noun="task",
        path=("set",),
        cli_entry_name="set",
        surface_class="action",
        help="Create or update task handoff state.",
    ),
    VerbSpec(
        noun="task",
        path=("next",),
        cli_entry_name="next-actions",
        surface_class="action",
        help="List or mutate task next actions.",
    ),
    # handoff
    VerbSpec(
        noun="handoff",
        path=("load-session",),
        cli_entry_name="load-session",
        surface_class="query",
        help="Load compound session handoff context.",
    ),
    VerbSpec(
        noun="handoff",
        path=("search",),
        cli_entry_name="handoff-search",
        surface_class="query",
        help="Search decisions, findings, blockers, actions, and verified tests.",
    ),
    VerbSpec(
        noun="handoff",
        path=("decision", "record"),
        cli_entry_name="event",
        fixed_args={"event_kind": "decision"},
        surface_class="action",
        help="Record a decision event.",
    ),
    # finding
    VerbSpec(
        noun="finding",
        path=("list",),
        cli_entry_name="review-findings",
        fixed_args={"operation": "list"},
        surface_class="query",
        help="List review findings.",
    ),
    VerbSpec(
        noun="finding",
        path=("record",),
        cli_entry_name="review-findings",
        fixed_args={"operation": "record"},
        surface_class="action",
        help="Record a review finding.",
    ),
    # continuation
    VerbSpec(
        noun="continuation",
        path=("load",),
        cli_entry_name="continuation",
        fixed_args={"operation": "load"},
        surface_class="query",
        help="Load a saved continuation packet.",
    ),
    VerbSpec(
        noun="continuation",
        path=("save",),
        cli_entry_name="continuation",
        fixed_args={"operation": "save"},
        surface_class="action",
        help="Save a continuation packet.",
    ),
    # search
    VerbSpec(
        noun="search",
        path=("prior-work",),
        cli_entry_name="find-related-prior-work",
        default_overrides={"semantic": "warm-only"},
        surface_class="query",
        help="Search related prior findings and decisions (warm-only semantic default).",
    ),
)

# Standalone CLI-only noun verbs not backed by a single CliEntry.
# Kept separate from ``NOUN_VERBS`` so registry-closure tests remain exact on
# registry-backed aliases while Wave 2B permission generators read ``ALL_NOUN_VERBS``
# / ``NOUN_VERB_SURFACE_CLASS`` and never auto-allow ``surface_class == "action"``.
CUSTOM_NOUN_VERBS: tuple[CustomVerbSpec, ...] = (
    CustomVerbSpec(
        noun="handoff",
        path=("ingest",),
        surface_class="action",
        help="Ingest a remote worker handoff.json artifact into the host handoff DB.",
    ),
)

ALL_NOUN_VERBS: tuple[VerbSpec | CustomVerbSpec, ...] = (*NOUN_VERBS, *CUSTOM_NOUN_VERBS)

NOUN_VERB_SURFACE_CLASS: Mapping[tuple[str, tuple[str, ...]], str] = {
    (verb.noun, verb.path): verb.surface_class for verb in ALL_NOUN_VERBS
}

# Explicit closure reasons for handler parameters that are intentionally not
# mapped 1:1 by name to a flat CLI ArgSpec on the underlying ToolEntry.
# Verified by ``tests/test_cli_registry_closure.py``.
HANDLER_PARAM_EXEMPTIONS: Mapping[str, Mapping[str, str]] = {
    "set": {
        "actor": "Structured actor dict constructed by _dispatch_set_handoff_state from --commit-sha/--branch.",
        "detail": "MCP write-receipt projection knob; CLI always prints full response envelope.",
        "fields": "MCP write-receipt projection knob; CLI always prints full response envelope.",
        "response_budget_bytes": "MCP token-budget guard; CLI callers consume full local JSON.",
        "budget_policy": "MCP token-budget guard; CLI callers consume full local JSON.",
    },
    "state": {
        "top_n_blockers": "Legacy per-section cap superseded by --read-profile.",
        "top_n_actions": "Legacy per-section cap superseded by --read-profile.",
        "top_n_slices": "Legacy per-section cap superseded by --read-profile.",
        "top_n_tests": "Legacy per-section cap superseded by --read-profile.",
        "top_n_findings": "Legacy per-section cap superseded by --read-profile.",
        "include_archived": "MCP-only archived-row toggle; CLI uses archive --operation get.",
        "source": "MCP orientation-read telemetry metadata.",
        "session": "MCP orientation-read telemetry metadata.",
    },
    "event": {
        "event": "Structured RecordEventParam payload assembled by _dispatch_event_record from flat CLI flags.",
    },
    "review-findings": {
        "review": "Structured ReviewFindingsParam payload assembled by _dispatch_review_findings from flat CLI flags.",
        "response_budget_bytes": "MCP response byte cap; CLI callers consume full local JSON.",
    },
    "continuation": {
        "payload": "Structured ContinuationParam payload assembled by _dispatch_continuation from flat CLI flags.",
    },
    "find-related-prior-work": {
        "source": "MCP orientation-read telemetry metadata.",
        "session": "MCP orientation-read telemetry metadata.",
    },
    "load-session": {
        "top_n_blockers": "Legacy per-section cap superseded by --read-profile.",
        "top_n_actions": "Legacy per-section cap superseded by --read-profile.",
        "top_n_decisions": "Legacy per-section cap superseded by --read-profile.",
        "top_n_slices": "Legacy per-section cap superseded by --read-profile.",
        "top_n_tests": "Legacy per-section cap superseded by --read-profile.",
        "top_n_findings": "Legacy per-section cap superseded by --read-profile.",
        "open_findings_detail": "Controlled via --detail and --read-profile on CLI.",
        "source": "MCP orientation-read telemetry metadata.",
        "session": "MCP orientation-read telemetry metadata.",
        "include_context_refresh": "MCP-only session refresh toggle.",
        "last_injected_compaction_id": "MCP-only compaction dedupe key.",
    },
    "handoff-search": {
        "scope": "Defaults to task scope; explicit --task-ref scopes or omits for active-task resolution.",
    },
}

_RUNTIME_FLAGS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("--workspace-root", {"help": "Workspace root directory."}),
    ("--state-dir", {"help": "Explicit handoff state directory."}),
    ("--current-task-path", {"help": "Explicit CURRENT_TASK.json path."}),
    ("--dashboard-path", {"help": "Explicit dashboard path."}),
    ("--exports-dir", {"help": "Explicit exports directory."}),
    ("--json", {"action": "store_true", "help": "Emit compact JSON envelope (always enabled for noun commands)."}),
)


def _arg_dest(spec: _cli.ArgSpec) -> str:
    if spec.name.startswith("-"):
        return spec.dest or spec.name.lstrip("-").replace("-", "_")
    return spec.dest or spec.name


def _add_runtime_flags(parser: argparse.ArgumentParser, *, suppress_defaults: bool) -> None:
    existing_opts = {opt for action in parser._actions for opt in action.option_strings}
    for flag, extra in _RUNTIME_FLAGS:
        if flag in existing_opts:
            continue
        kwargs = dict(extra)
        if suppress_defaults:
            kwargs["default"] = argparse.SUPPRESS
        elif kwargs.get("action") == "store_true":
            kwargs.setdefault("default", False)
        else:
            kwargs.setdefault("default", None)
        parser.add_argument(flag, **kwargs)


def _registry_by_name() -> dict[str, _cli.CliEntry]:
    return {entry.name: entry for entry in _cli._build_cli_registry()}


def _harness_source_label() -> str:
    if (os.environ.get("WORKBAY_HARNESS") or "").strip():
        return "env:WORKBAY_HARNESS"
    if (os.environ.get("CODEX_HARNESS") or "").strip():
        return "env:CODEX_HARNESS"
    if (os.environ.get("CLAUDECODE") or "").strip():
        return "env:CLAUDECODE"
    if resolve_harness() != "unknown":
        return "env:transcript_contract"
    return "default:unknown"


def _event_actor_provenance(args: argparse.Namespace, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Echo resolved actor provenance (reusing server resolution + resolve_harness)."""
    data = payload.get("data")
    decision_row = data.get("decision") if isinstance(data, Mapping) else None
    row = decision_row if isinstance(decision_row, Mapping) else {}
    harness = resolve_harness()
    agent = row.get("agent") if row else getattr(args, "agent", None)
    model = row.get("model") if row else getattr(args, "model", None)
    model_label = row.get("model_label") if row else getattr(args, "model_label", None)
    reasoning_level = row.get("reasoning_level") if row else getattr(args, "reasoning_level", None)
    return {
        "agent": agent,
        "model": model,
        "model_label": model_label,
        "reasoning_level": reasoning_level,
        "harness": harness,
        "sources": {
            "agent": "flag" if getattr(args, "agent", None) else ("model" if getattr(args, "model", None) else "resolved"),
            "model": "flag" if getattr(args, "model", None) else "unset",
            "model_label": (
                "flag"
                if getattr(args, "model_label", None)
                else ("derived" if model_label is not None else "unset")
            ),
            "reasoning_level": "flag" if getattr(args, "reasoning_level", None) else "unset",
            "harness": _harness_source_label(),
        },
    }


def _populate_leaf_subparser(
    sub: argparse.ArgumentParser,
    verb: VerbSpec,
    entry: _cli.CliEntry,
) -> None:
    fixed_dests = set(verb.fixed_args)
    for spec in entry.args:
        if _arg_dest(spec) in fixed_dests:
            continue
        if entry.name == "find-related-prior-work" and spec.name == "--include-status":
            spec = replace(
                spec,
                help="Filter by task status (handoff_state.status), not finding status.",
            )
        _cli._add_arg(sub, spec)
    _add_runtime_flags(sub, suppress_defaults=True)
    defaults: dict[str, Any] = {
        **dict(verb.default_overrides),
        **dict(verb.fixed_args),
        "_cli_entry": entry,
        "_verb_spec": verb,
    }
    sub.set_defaults(**defaults)


def build_noun_parser(
    noun: str,
    *,
    registry: Mapping[str, _cli.CliEntry] | None = None,
) -> JsonArgumentParser:
    """Build a :class:`JsonArgumentParser` for ``workbay <noun>`` from the CLI registry."""
    verbs = [v for v in NOUN_VERBS if v.noun == noun]
    if not verbs:
        raise ValueError(f"Unknown handoff noun: {noun!r}")
    by_name = dict(registry) if registry is not None else _registry_by_name()

    parser = JsonArgumentParser(
        prog=f"workbay {noun}",
        description=f"WorkBay {noun} commands.",
    )
    _add_runtime_flags(parser, suppress_defaults=False)

    top_subparsers = parser.add_subparsers(dest="_verb_0", required=True)
    nested_groups: dict[str, Any] = {}

    for verb in verbs:
        if verb.cli_entry_name not in by_name:
            raise KeyError(f"Verb {verb.noun} {' '.join(verb.path)} references missing CliEntry {verb.cli_entry_name!r}")
        entry = by_name[verb.cli_entry_name]
        help_text = (verb.help or entry.description).replace("%", "%%")

        if len(verb.path) == 1:
            sub = top_subparsers.add_parser(verb.path[0], help=help_text)
            _populate_leaf_subparser(sub, verb, entry)
        elif len(verb.path) == 2:
            group_name, leaf_name = verb.path
            group_subparsers = nested_groups.get(group_name)
            if group_subparsers is None:
                group_parser = top_subparsers.add_parser(group_name, help=f"{noun} {group_name} commands.")
                _add_runtime_flags(group_parser, suppress_defaults=True)
                group_subparsers = group_parser.add_subparsers(dest="_verb_1", required=True)
                nested_groups[group_name] = group_subparsers
            leaf = group_subparsers.add_parser(leaf_name, help=help_text)
            _populate_leaf_subparser(leaf, verb, entry)
        else:
            raise ValueError(f"Unsupported verb nesting depth for {verb.path!r}")

    for custom_verb in CUSTOM_NOUN_VERBS:
        if custom_verb.noun != noun:
            continue
        if custom_verb.noun == "handoff" and custom_verb.path == ("ingest",):
            _ingest_cli.register_ingest_subparser(
                top_subparsers,
                add_runtime_flags=_add_runtime_flags,
                verb_spec=custom_verb,
            )
        else:
            raise ValueError(f"Unsupported custom verb {custom_verb.noun} {custom_verb.path!r}")

    return parser


def run_noun(noun: str, argv: list[str] | None = None) -> int:
    """Parse ``argv`` for ``workbay <noun>`` and dispatch through ``cli_contract``."""
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_noun_parser(noun)
    try:
        args = parser.parse_args(raw_argv)
    except SystemExit as exc:
        if exc.code is None:
            return int(ExitCode.OK)
        if isinstance(exc.code, int):
            return exc.code
        return int(ExitCode.USAGE)

    verb: VerbSpec | CustomVerbSpec = args._verb_spec
    source = f"workbay {verb.noun} {' '.join(verb.path)}"

    custom_dispatch = getattr(args, "_custom_dispatch", None)
    if custom_dispatch is not None:

        def _execute_custom() -> Mapping[str, object]:
            config = RuntimeConfig.from_args(args)
            configure_runtime(config)
            result = custom_dispatch(args)
            if isinstance(result, Mapping):
                return result
            raise TypeError(f"Expected mapping payload from {source}, got {type(result).__name__}")

        return run_json_command(_execute_custom, source=source)

    entry: _cli.CliEntry = args._cli_entry

    def _execute() -> Mapping[str, object]:
        config = RuntimeConfig.from_args(args)
        configure_runtime(config)
        if entry.name in _cli._EMBEDDING_CLI_COMMANDS:
            _cli._apply_embedding_env_for_config(config)
        try:
            result = entry.dispatch(args)
        except SystemExit as exc:
            if isinstance(exc.code, str):
                return {
                    "ok": False,
                    "source": source,
                    "error": {
                        "kind": "usage",
                        "message": exc.code,
                    },
                }
            raise
        if isinstance(result, Mapping):
            if entry.name == "event" and result.get("ok") is True:
                enriched = dict(result)
                enriched["actor_provenance"] = _event_actor_provenance(args, result)
                return enriched
            return result
        raise TypeError(f"Expected mapping payload from {entry.name}, got {type(result).__name__}")

    return run_json_command(_execute, source=source)


def handoff_main(argv: list[str] | None = None) -> int:
    return run_noun("handoff", argv)


def task_main(argv: list[str] | None = None) -> int:
    return run_noun("task", argv)


def finding_main(argv: list[str] | None = None) -> int:
    return run_noun("finding", argv)


def continuation_main(argv: list[str] | None = None) -> int:
    return run_noun("continuation", argv)


def search_main(argv: list[str] | None = None) -> int:
    return run_noun("search", argv)


__all__ = [
    "ALL_NOUN_VERBS",
    "CUSTOM_NOUN_VERBS",
    "CustomVerbSpec",
    "HANDLER_PARAM_EXEMPTIONS",
    "NOUN_VERBS",
    "NOUN_VERB_SURFACE_CLASS",
    "VerbSpec",
    "build_noun_parser",
    "continuation_main",
    "finding_main",
    "handoff_main",
    "run_noun",
    "search_main",
    "task_main",
]
