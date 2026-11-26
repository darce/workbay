"""``workbay`` console-script entry point.

The front door is a thin wrapper over ``workbay_bootstrap.cli.main``. Its one
piece of behaviour beyond pass-through: when the user runs ``workbay install``
without naming an overlay source, default ``--source package`` so the install
materializes from the published ``workbay-system`` distribution payload — the
tree the front door actually ships. ``workbay embeddings`` is handled locally
as the canonical operator toggle (implicit ``Path.cwd()``); every other
invocation reaches the bootstrap parser verbatim — a blanket inject would break
``workbay doctor`` / ``status`` with "unrecognized arguments: --source package"
(``--source`` is an ``install``-subparser argument only).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from types import MappingProxyType

# Static mirror of ``workbay_bootstrap.cli._build_parser()`` subcommand choices so
# the plugin dispatch path never pays the ``workbay_bootstrap.cli`` import cost.
# Pinned against the live parser by ``tests/test_cli_json_contract.py``.
BOOTSTRAP_SUBCOMMANDS: frozenset[str] = frozenset(
    {
        "adopt-worktree",
        "apply-hooks",
        "bootstrap-surfaces",
        "clean",
        "doctor",
        "embeddings",
        "gc",
        "install",
        "mcp-sync",
        "overrides",
        "permissions-audit",
        "provision-configured-hooks",
        "provision-embeddings",
        "provision-hook-helpers",
        "refresh-lifecycle",
        "refresh-native-launchers",
        "repair",
        "status",
        "update",
        "validate-overrides",
    }
)

# Canonical distribution owner for each shipped WorkBay front-door noun.
NOUN_OWNERS: Mapping[str, str] = MappingProxyType(
    {
        "handoff": "mcp-workbay-handoff",
        "task": "mcp-workbay-handoff",
        "finding": "mcp-workbay-handoff",
        "continuation": "mcp-workbay-handoff",
        "search": "mcp-workbay-handoff",
        "lane": "mcp-workbay-orchestrator",
        "fleet": "mcp-workbay-orchestrator",
    }
)


def _has_explicit_source(argv: list[str]) -> bool:
    """True when the user already passed ``--source`` (space or ``=`` form)."""
    return any(a == "--source" or a.startswith("--source=") for a in argv)


def _install_argv(argv: list[str]) -> list[str]:
    """Default the ``install`` overlay source to ``package``.

    Inject ``--source package`` only when the first token is exactly
    ``install`` and the user did not pass ``--source`` themselves. All other
    argument vectors pass through untouched.
    """
    if argv[:1] == ["install"] and not _has_explicit_source(argv):
        return ["install", "--source", "package", *argv[1:]]
    return list(argv)


def _embeddings_parser() -> argparse.ArgumentParser:
    """Build the ``workbay embeddings`` parser (implicit cwd; no ``--target``)."""
    parser = argparse.ArgumentParser(prog="workbay embeddings")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--status",
        action="store_true",
        help="Print .workbay/embedding.env file-gate status as JSON (not process-env).",
    )
    mode.add_argument(
        "--enable",
        action="store_true",
        help="Enable embeddings via .workbay/embedding.env file gate (does not clear process-env).",
    )
    mode.add_argument(
        "--disable",
        action="store_true",
        help="Disable embeddings via .workbay/embedding.env file gate.",
    )
    return parser


def _run_embeddings(argv: list[str]) -> int:
    """Handle ``workbay embeddings`` via the single ``embedding_provision`` runner."""
    from workbay_bootstrap.embedding_provision import run_embeddings_cli

    args = _embeddings_parser().parse_args(argv[1:])
    return run_embeddings_cli(
        Path.cwd().resolve(),
        status=args.status,
        enable=args.enable,
        disable=args.disable,
    )


def _plugin_commands() -> dict[str, EntryPoint]:
    """Installed ``workbay.commands`` entry points, keyed by name."""
    return {ep.name: ep for ep in entry_points(group="workbay.commands")}


def _bootstrap_subcommand_names() -> frozenset[str]:
    """Bootstrap subcommand names without importing ``workbay_bootstrap.cli``."""
    return BOOTSTRAP_SUBCOMMANDS


def _wants_json(argv: list[str]) -> bool:
    """True when the caller requested machine-readable ``--json`` output."""
    return "--json" in argv


def _collect_handoff_verbs() -> dict[str, list[dict[str, object]]]:
    """Collect verb metadata for handoff nouns from ``NOUN_VERBS`` and ``ToolEntry.surface_class``."""
    try:
        from workbay_handoff_mcp.api import _build_tool_registry
        from workbay_handoff_mcp.noun_cli import NOUN_VERBS
    except ImportError:
        return {}

    tools_by_cli = {
        entry.cli_name: entry
        for entry in _build_tool_registry()
        if entry.cli_name is not None
    }
    by_noun: dict[str, list[dict[str, object]]] = {}
    for verb_spec in NOUN_VERBS:
        tool_entry = tools_by_cli.get(verb_spec.cli_entry_name)
        if verb_spec.fixed_args:
            raw_surface = verb_spec.surface_class
        elif tool_entry is not None and tool_entry.surface_class in ("query", "action"):
            raw_surface = tool_entry.surface_class
        else:
            raw_surface = verb_spec.surface_class
        surface_class = raw_surface if raw_surface in ("query", "action") else "action"
        owner = NOUN_OWNERS.get(verb_spec.noun, "mcp-workbay-handoff")
        by_noun.setdefault(verb_spec.noun, []).append(
            {
                "noun": verb_spec.noun,
                "verb": " ".join(verb_spec.path),
                "surface_class": surface_class,
                "owner_package": owner,
            }
        )
    return by_noun


def _collect_lane_verbs() -> dict[str, list[dict[str, object]]]:
    """Collect verb metadata for the orchestrator ``lane`` noun from ``LANE_VERB_SPECS``."""
    try:
        from workbay_orchestrator_mcp.lane_noun_cli import LANE_VERB_SPECS
    except ImportError:
        return {}

    owner = NOUN_OWNERS.get("lane", "mcp-workbay-orchestrator")
    entries: list[dict[str, object]] = []
    for spec in LANE_VERB_SPECS.values():
        surface_class = spec.surface_class if spec.surface_class in ("query", "action") else "action"
        record: dict[str, object] = {
            "noun": spec.noun,
            "verb": spec.verb,
            "surface_class": surface_class,
            "owner_package": owner,
        }
        if spec.action_flags:
            record["action_flags"] = list(spec.action_flags)
        entries.append(record)
    return {"lane": entries}


def _entry_point_owner(ep: EntryPoint, noun: str) -> str:
    """Resolve the owning distribution name for a ``workbay.commands`` entry point."""
    dist = getattr(ep, "dist", None)
    dist_name = getattr(dist, "name", None) if dist is not None else None
    if isinstance(dist_name, str) and dist_name:
        return dist_name
    return NOUN_OWNERS.get(noun, "unknown")


def collect_commands_manifest() -> list[dict[str, object]]:
    """Discover registered ``workbay.commands`` nouns and their verb metadata.

    Returns a deterministic list sorted by ``(noun, verb)`` where every entry
    carries ``noun``, ``verb``, ``surface_class`` (``"query"`` or ``"action"``),
    and ``owner_package``.
    """
    plugins = _plugin_commands()
    bootstrap_names = _bootstrap_subcommand_names()
    handoff_verbs = _collect_handoff_verbs()
    lane_verbs = _collect_lane_verbs()

    known_providers: dict[str, list[dict[str, object]]] = {
        **handoff_verbs,
        **lane_verbs,
    }

    discovered_nouns: set[str] = {
        name for name in plugins if name not in bootstrap_names
    } | {name for name in known_providers if name not in bootstrap_names}

    manifest: list[dict[str, object]] = []
    for noun in sorted(discovered_nouns):
        if noun in known_providers:
            ep = plugins.get(noun)
            owner_override = _entry_point_owner(ep, noun) if ep is not None else None
            for item in known_providers[noun]:
                entry = dict(item)
                if owner_override is not None and owner_override != "unknown":
                    entry["owner_package"] = owner_override
                manifest.append(entry)
        else:
            ep = plugins[noun]
            # Fail-closed: registered plugins without a declared surface-class
            # table are treated as ``action`` so they are never auto-approved.
            manifest.append(
                {
                    "noun": noun,
                    "verb": "*",
                    "surface_class": "action",
                    "owner_package": _entry_point_owner(ep, noun),
                }
            )

    manifest.sort(key=lambda item: (str(item["noun"]), str(item["verb"])))
    return manifest


def build_commands_manifest() -> dict[str, object]:
    """Build the ``cli_contract`` envelope for ``workbay commands --json``."""
    return {
        "ok": True,
        "source": "workbay commands",
        "schema_version": 1,
        "commands": collect_commands_manifest(),
    }


def _run_commands(argv: list[str]) -> int:
    """Handle ``workbay commands [--json]``."""
    from workbay_protocol.cli_contract import ExitCode, JsonArgumentParser, run_json_command

    parser = JsonArgumentParser(
        prog="workbay commands",
        description="Emit the discovered WorkBay noun/verb command manifest.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the command manifest as a compact cli_contract JSON envelope.",
    )
    try:
        args = parser.parse_args(argv[1:])
    except SystemExit as exc:
        if exc.code is None:
            return int(ExitCode.OK)
        if isinstance(exc.code, int):
            return exc.code
        return int(ExitCode.USAGE)

    if args.json:
        return run_json_command(build_commands_manifest, source="workbay commands")

    for entry in collect_commands_manifest():
        print(
            f"{entry['noun']} {entry['verb']} "
            f"[{entry['surface_class']}] ({entry['owner_package']})"
        )
    return int(ExitCode.OK)


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv[:1] == ["embeddings"]:
        return _run_embeddings(argv)
    if argv[:1] == ["commands"]:
        return _run_commands(argv)

    plugins = _plugin_commands()
    bootstrap_names = _bootstrap_subcommand_names()
    visible = sorted(name for name in plugins if name not in bootstrap_names)

    if not argv or argv[0] in ("-h", "--help"):
        if visible:
            print("Installed commands: " + ", ".join(visible))
    elif argv[0] in plugins:
        name = argv[0]
        if name in bootstrap_names:
            print(
                f"workbay: plugin command '{name}' ignored because it shadows a bootstrap subcommand",
                file=sys.stderr,
            )
        else:
            plugin_argv = argv[1:]
            json_mode = _wants_json(plugin_argv)
            try:
                plugin = plugins[name].load()
            except Exception as exc:
                if json_mode:
                    from workbay_protocol.cli_contract import ExitCode, emit_json

                    emit_json(
                        {
                            "ok": False,
                            "source": f"workbay {name}",
                            "error": {
                                "kind": "plugin_load_failed",
                                "type": type(exc).__name__,
                            },
                        }
                    )
                    return int(ExitCode.UNAVAILABLE)
                print(
                    f"workbay: command '{name}' failed to load: {type(exc).__name__}",
                    file=sys.stderr,
                )
                return 2
            if json_mode:
                try:
                    return plugin(plugin_argv)
                except Exception as exc:
                    from workbay_protocol.cli_contract import ExitCode, emit_json

                    emit_json(
                        {
                            "ok": False,
                            "source": f"workbay {name}",
                            "error": {
                                "kind": "internal",
                                "type": type(exc).__name__,
                            },
                        }
                    )
                    return int(ExitCode.INTERNAL)
            return plugin(plugin_argv)

    if argv and argv[0] in {"studio", "canvas"} and argv[0] not in bootstrap_names:
        print(
            f"workbay: '{argv[0]}' is not installed in this environment; from a checkout run: "
            "bash scripts/install-workbay-studio.sh <checkout>",
            file=sys.stderr,
        )
        return 2

    # Lazy import so ``import workbay.cli`` does not hard-require the bootstrap
    # distribution at module load (keeps the pure argv transform importable on
    # its own and testable without the full dependency tree).
    from workbay_bootstrap.cli import main as bootstrap_main

    return bootstrap_main(_install_argv(argv))


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    raise SystemExit(main())
