"""Plugin override composition helpers (implementation note S4).

Extracted from ``generate_agent_workflows.py`` so the two merge engines stay
under ~60 lines each while preserving internal semantics byte-for-byte.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import ValidationError

from workbay_protocol.bootstrap import (
    PluginMcpServerPatch,
    PluginOverrideManifest,
)

PLUGIN_OVERRIDE_MANIFEST = "overrides.yaml"

# Recognized Claude settings.json hook stages (camelCase event names). An
# override file declaring any other stage key is a typo that would silently
# produce a phantom stage Claude ignores, so it is rejected at read time.
# Must cover every stage Claude Code actually honors — the managed contract
# itself registers Stop hooks, so rejecting valid stages aborts the whole
# install/pin pass for legitimate consumer overrides.
CLAUDE_HOOK_STAGES = (
    "PreToolUse",
    "PostToolUse",
    "UserPromptSubmit",
    "SessionStart",
    "SessionEnd",
    "Stop",
    "SubagentStop",
    "PreCompact",
    "Notification",
)


def sha256_digest(content: str) -> str:
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def merge_file_three_way(base: str, ours: str, theirs: str) -> tuple[str, bool]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_root = Path(tmp)
        ours_file = tmp_root / "consumer"
        base_file = tmp_root / "base"
        theirs_file = tmp_root / "upstream"
        ours_file.write_text(ours)
        base_file.write_text(base)
        theirs_file.write_text(theirs)
        proc = subprocess.run(
            [
                "git",
                "merge-file",
                "-p",
                "-L",
                "consumer",
                "-L",
                "base",
                "-L",
                "upstream",
                str(ours_file),
                str(base_file),
                str(theirs_file),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    if proc.returncode < 0 or proc.returncode > 127:
        raise SystemExit(
            f"git merge-file failed (exit {proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout, proc.returncode != 0


def _read_override_skill(override_root: Path, relative_path: str) -> str:
    override_path = override_root / relative_path
    try:
        return override_path.read_text()
    except FileNotFoundError as exc:
        raise SystemExit(f"plugin override skill not found: {override_path}") from exc


def _skill_replace(
    slug: str,
    override,
    *,
    composed: dict[str, str],
    override_root: Path,
) -> tuple[dict[str, str], dict[str, str]]:
    if slug not in composed:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot replace unknown skill {slug!r}"
        )
    relative_path = override.path
    upstream_digest = override.upstream_digest
    base_digest = sha256_digest(composed[slug])
    stale_component: dict[str, str] = {}
    if upstream_digest != base_digest:
        on_upstream_change = override.on_upstream_change
        if on_upstream_change == "warn":
            stale_component = {
                "status": "stale",
                "override_path": relative_path,
                "recorded_upstream_digest": upstream_digest,
                "current_base_digest": base_digest,
            }
        elif on_upstream_change != "ignore":
            raise SystemExit(
                f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.skills.{slug}.upstream_digest "
                f"{upstream_digest!r} does not match current base digest {base_digest!r}"
            )
    composed_skill = _read_override_skill(override_root, relative_path)
    composed[slug] = composed_skill
    component = {
        "component_kind": "skill",
        "name": slug,
        "mode": "replace",
        "effective_digest": sha256_digest(composed_skill),
        **stale_component,
    }
    return composed, component


def _skill_add(
    slug: str,
    override,
    *,
    composed: dict[str, str],
    skill_order: list[str],
    override_root: Path,
) -> tuple[dict[str, str], list[str], dict[str, str]]:
    if slug in composed:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot add existing skill {slug!r}; "
            "use mode=replace instead"
        )
    composed_skill = _read_override_skill(override_root, override.path)
    composed[slug] = composed_skill
    skill_order.append(slug)
    component = {
        "component_kind": "skill",
        "name": slug,
        "mode": "add",
        "effective_digest": sha256_digest(composed_skill),
    }
    return composed, skill_order, component


def _skill_disable(
    slug: str,
    *,
    composed: dict[str, str],
    skill_order: list[str],
    override_root: Path,
) -> tuple[dict[str, str], list[str], dict[str, str]]:
    if slug not in composed:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot disable unknown skill {slug!r}"
        )
    del composed[slug]
    skill_order = [entry for entry in skill_order if entry != slug]
    component = {
        "component_kind": "skill",
        "name": slug,
        "mode": "disable",
        "effective_digest": sha256_digest(f"disabled:{slug}\n"),
    }
    return composed, skill_order, component


def _skill_patch(
    slug: str,
    override,
    *,
    composed: dict[str, str],
    override_root: Path,
) -> tuple[dict[str, str], dict[str, str]]:
    if slug not in composed:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot patch unknown skill {slug!r}"
        )
    base_file = override_root / override.base_path
    try:
        forked_base = base_file.read_text()
    except FileNotFoundError as exc:
        raise SystemExit(
            f"plugin override base copy not found: {base_file}"
        ) from exc
    if override.upstream_digest != sha256_digest(forked_base):
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.skills.{slug}.upstream_digest "
            f"does not match the stored base copy at {override.base_path}; "
            "re-fork the base copy or run overrides accept-upstream"
        )
    consumer_edit = _read_override_skill(override_root, override.path)
    current_upstream = composed[slug]
    merged, conflicted = merge_file_three_way(
        forked_base, consumer_edit, current_upstream
    )
    if conflicted:
        composed[slug] = consumer_edit
        component = {
            "component_kind": "skill",
            "name": slug,
            "mode": "patch",
            "effective_digest": sha256_digest(consumer_edit),
            "status": "merge_conflict",
            "override_path": override.path,
            "recorded_upstream_digest": override.upstream_digest,
            "current_base_digest": sha256_digest(current_upstream),
        }
        return composed, component
    composed[slug] = merged
    component = {
        "component_kind": "skill",
        "name": slug,
        "mode": "patch",
        "effective_digest": sha256_digest(merged),
        "override_path": override.path,
        "recorded_upstream_digest": override.upstream_digest,
        "current_base_digest": sha256_digest(current_upstream),
    }
    return composed, component


def compose_plugin_skill_overrides(
    rendered_skills: dict[str, str],
    override_root: Path,
    payload: PluginOverrideManifest,
) -> tuple[dict[str, str], list[str], list[dict[str, str]]]:
    composed = dict(rendered_skills)
    skill_order = list(rendered_skills)
    components: list[dict[str, str]] = []
    handlers = {
        "replace": _skill_replace,
        "add": _skill_add,
        "disable": _skill_disable,
        "patch": _skill_patch,
    }
    for slug, override in sorted(payload.components.skills.items()):
        mode = override.mode
        handler = handlers.get(mode)
        if handler is None:
            raise SystemExit(
                f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.skills.{slug}.mode "
                f"must be one of 'replace', 'patch', 'add', or 'disable' for the current "
                f"composition slice; found {mode!r}"
            )
        if mode == "replace":
            composed, component = handler(
                slug, override, composed=composed, override_root=override_root
            )
            components.append(component)
        elif mode == "add":
            composed, skill_order, component = handler(
                slug,
                override,
                composed=composed,
                skill_order=skill_order,
                override_root=override_root,
            )
            components.append(component)
        elif mode == "disable":
            composed, skill_order, component = handler(
                slug, composed=composed, skill_order=skill_order, override_root=override_root
            )
            components.append(component)
        else:
            composed, component = handler(
                slug, override, composed=composed, override_root=override_root
            )
            components.append(component)
    return composed, skill_order, components


def load_plugin_mcp_patch(override_root: Path, relative_path: str) -> PluginMcpServerPatch:
    path = override_root / relative_path
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError as exc:
        raise SystemExit(f"plugin override MCP patch not found: {path}") from exc
    try:
        return PluginMcpServerPatch.model_validate(raw)
    except ValidationError as exc:
        raise SystemExit(f"{path}: {exc}") from exc


def canonical_mcp_server_digest(server: dict[str, object]) -> str:
    return sha256_digest(json.dumps(server, sort_keys=True, ensure_ascii=False))


def base_plugin_mcp_server(
    name: str, mode: str, server: dict[str, object] | None
) -> dict[str, object]:
    if mode == "add":
        return {"name": name, "command": "", "args": []}
    if server is None:
        raise SystemExit(
            f"MCP server override references unknown canonical server {name!r}; use mode='add' to declare a new one"
        )
    return dict(server)


def mcp_override_unconsumed(server: dict | None, plugin_harnesses: set[str]) -> bool:
    if not plugin_harnesses:
        return True
    harnesses = (server or {}).get("harnesses") or []
    return bool(harnesses) and not (set(harnesses) & plugin_harnesses)


def apply_mcp_patch_ops(
    composed: dict[str, object],
    patch: PluginMcpServerPatch,
    *,
    mode: str,
    name: str,
    override_root: Path,
    patch_path: str,
) -> tuple[dict[str, object], list[str], bool]:
    mutated_fields: list[str] = []
    disabled = False
    for op in patch.ops:
        if op.op == "replace_command":
            composed["command"] = op.value
            mutated_fields.append("command")
        elif op.op == "replace_args":
            composed["args"] = list(op.value)
            mutated_fields.append("args")
        elif op.op == "append_args":
            composed["args"] = [*list(composed.get("args", [])), *op.value]
            mutated_fields.append("args")
        elif op.op == "upsert_env":
            env = dict(composed.get("env", {}))
            env[op.name] = op.value
            composed["env"] = env
            mutated_fields.append("env")
        elif op.op == "remove_env":
            env = dict(composed.get("env", {}))
            env.pop(op.name, None)
            if env:
                composed["env"] = env
            else:
                composed.pop("env", None)
            mutated_fields.append("env")
        elif op.op == "disable_server":
            if mode == "add":
                raise SystemExit(
                    f"{override_root / patch_path}: disable_server cannot be used with "
                    f"components.mcp_servers.{name}.mode='add'"
                )
            disabled = True
            break
    return composed, mutated_fields, disabled


def _mcp_disable_one(
    name: str,
    *,
    servers: list[dict],
    by_name: dict[str, dict],
    plugin_harnesses: set[str],
    unconsumed: list[str],
) -> tuple[list[dict], dict[str, dict], dict[str, str], str | None]:
    if name not in by_name:
        raise SystemExit(
            f"cannot disable unknown MCP server {name!r}"
        )
    if mcp_override_unconsumed(by_name[name], plugin_harnesses):
        unconsumed.append(name)
    servers = [entry for entry in servers if entry.get("name") != name]
    by_name.pop(name, None)
    component = {
        "component_kind": "mcp_server",
        "name": name,
        "mode": "disable",
        "effective_digest": sha256_digest(f"disabled:{name}\n"),
    }
    return servers, by_name, component, f"mcp audit: {name} disabled"


def _mcp_patch_or_add_one(
    name: str,
    override,
    *,
    servers: list[dict],
    by_name: dict[str, dict],
    plugin_harnesses: set[str],
    unconsumed: list[str],
    override_root: Path,
) -> tuple[list[dict], dict[str, dict], dict[str, str] | None, str | None, bool]:
    mode = override.mode
    server = by_name.get(name)
    if mode == "patch" and server is None:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot patch unknown MCP server {name!r}"
        )
    if mode == "add" and server is not None:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot add existing MCP server {name!r}; use mode='patch' or 'disable'"
        )
    if not override.requires_trust_ack:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.mcp_servers.{name}.requires_trust_ack "
            "must be true before patching MCP command, args, or env"
        )
    patch = load_plugin_mcp_patch(override_root, override.patch_path)
    if patch.target_server != name:
        raise SystemExit(
            f"{override_root / override.patch_path}: target_server {patch.target_server!r} "
            f"does not match override key {name!r}"
        )
    composed = base_plugin_mcp_server(name, mode, server)
    composed, mutated_fields, disabled = apply_mcp_patch_ops(
        composed,
        patch,
        mode=mode,
        name=name,
        override_root=override_root,
        patch_path=override.patch_path,
    )
    if disabled:
        if mcp_override_unconsumed(server, plugin_harnesses):
            unconsumed.append(name)
        servers = [entry for entry in servers if entry.get("name") != name]
        by_name.pop(name, None)
        return (
            servers,
            by_name,
            {
                "component_kind": "mcp_server",
                "name": name,
                "mode": "disable",
                "effective_digest": sha256_digest(f"disabled:{name}\n"),
            },
            f"mcp audit: {name} disabled",
            True,
        )
    command = composed.get("command")
    args = composed.get("args", [])
    if not isinstance(command, str) or not command:
        raise SystemExit(
            f"{override_root / override.patch_path}: composed MCP server {name!r} must set a non-empty command"
        )
    if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
        raise SystemExit(
            f"{override_root / override.patch_path}: composed MCP server {name!r} must carry args as a list of strings"
        )
    if mode == "add":
        servers.append(composed)
    else:
        for index, existing in enumerate(servers):
            if existing.get("name") == name:
                servers[index] = composed
                break
    if mcp_override_unconsumed(composed, plugin_harnesses):
        unconsumed.append(name)
    by_name[name] = composed
    unique_fields = [
        field for field in ("command", "args", "env") if field in mutated_fields
    ]
    component = {
        "component_kind": "mcp_server",
        "name": name,
        "mode": mode,
        "effective_digest": canonical_mcp_server_digest(composed),
    }
    audit = f"mcp audit: {name} {', '.join(unique_fields)}" if unique_fields else None
    return servers, by_name, component, audit, False


def _apply_mcp_override_entry(
    name: str,
    override,
    *,
    servers: list[dict],
    by_name: dict[str, dict],
    plugin_harnesses: set[str],
    unconsumed: list[str],
    override_root: Path,
) -> tuple[list[dict], dict[str, dict], dict[str, str] | None, str | None]:
    mode = override.mode
    if mode == "disable":
        servers, by_name, component, audit = _mcp_disable_one(
            name,
            servers=servers,
            by_name=by_name,
            plugin_harnesses=plugin_harnesses,
            unconsumed=unconsumed,
        )
        return servers, by_name, component, audit
    if mode not in {"patch", "add"}:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.mcp_servers.{name}.mode "
            f"must be one of 'patch', 'disable', or 'add'; found {mode!r}"
        )
    servers, by_name, component, audit, _ = _mcp_patch_or_add_one(
        name,
        override,
        servers=servers,
        by_name=by_name,
        plugin_harnesses=plugin_harnesses,
        unconsumed=unconsumed,
        override_root=override_root,
    )
    return servers, by_name, component, audit


def compose_plugin_mcp_overrides(
    mcp_manifest: dict,
    override_root: Path,
    payload: PluginOverrideManifest,
) -> tuple[dict, list[dict[str, str]], list[str]]:
    servers = [dict(server) for server in mcp_manifest["mcp_servers"]]
    by_name = {
        server["name"]: server
        for server in servers
        if isinstance(server.get("name"), str)
    }
    components: list[dict[str, str]] = []
    audit_lines: list[str] = []
    plugin_harnesses = {
        harness
        for harness, owner in (mcp_manifest.get("registration") or {}).items()
        if owner == "plugin"
    }
    unconsumed: list[str] = []

    for name, override in sorted(payload.components.mcp_servers.items()):
        servers, by_name, component, audit = _apply_mcp_override_entry(
            name,
            override,
            servers=servers,
            by_name=by_name,
            plugin_harnesses=plugin_harnesses,
            unconsumed=unconsumed,
            override_root=override_root,
        )
        if component is not None:
            components.append(component)
        if audit:
            audit_lines.append(audit)

    if unconsumed:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.mcp_servers "
            f"overrides for {sorted(set(unconsumed))} are consumed by no emitted "
            ".mcp.json: each overridden server needs at least one "
            "`registration: plugin` harness in its effective `harnesses` list "
            "(implementation note). Flip a harness to `registration: plugin` or change "
            "the launch specs in the canonical mcp_servers.yaml + "
            "`make mcp-pins-sync` instead."
        )
    return (
        {
            **mcp_manifest,
            "mcp_servers": servers,
        },
        components,
        audit_lines,
    )


def _import_portable_command_validator():
    import sys
    from pathlib import Path

    script_dir = str(Path(__file__).resolve().parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    from generate_agent_workflows import _validate_command

    return _validate_command


def _read_override_portable_command(override_root: Path, relative_path: str) -> dict:
    override_path = (override_root / relative_path).resolve()
    root = override_root.resolve()
    if not override_path.is_relative_to(root):
        raise SystemExit(
            f"plugin override portable command escapes override root: {relative_path}"
        )
    try:
        raw = json.loads(override_path.read_text())
    except FileNotFoundError as exc:
        raise SystemExit(
            f"plugin override portable command not found: {override_path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"plugin override portable command is not valid JSON: {override_path}"
        ) from exc
    if not isinstance(raw, dict):
        raise SystemExit(
            f"plugin override portable command must be a JSON object: {override_path}"
        )
    return raw


def _portable_command_add(
    command_id: str,
    override,
    *,
    manifest: dict,
    override_root: Path,
    composed_skills: dict[str, str],
) -> tuple[dict, dict[str, str]]:
    if any(entry.get("command_id") == command_id for entry in manifest.get("commands", [])):
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot add portable command {command_id!r}; "
            "an entry with that command_id already exists in the base manifest"
        )
    command = _read_override_portable_command(override_root, override.path)
    if command.get("command_id") != command_id:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.portable_commands.{command_id}.path "
            f"command_id {command.get('command_id')!r} must match override key {command_id!r}"
        )
    validate_command = _import_portable_command_validator()
    validate_command(command, len(manifest.get("commands", [])))
    skill = command["skill"]
    if skill not in composed_skills:
        raise SystemExit(
            f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.portable_commands.{command_id}.path "
            f"references skill {skill!r} that does not resolve in the composed effective tree"
        )
    commands = [*list(manifest.get("commands", [])), command]
    composed = {**manifest, "commands": commands}
    component = {
        "component_kind": "portable_command",
        "name": command_id,
        "mode": "add",
        "effective_digest": sha256_digest(
            json.dumps(command, sort_keys=True, ensure_ascii=False)
        ),
        "override_path": override.path,
    }
    return composed, component


def _read_override_claude_settings_hooks(
    override_root: Path, relative_path: str
) -> dict[str, list[dict[str, Any]]]:
    path = override_root / relative_path
    try:
        payload = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise SystemExit(
            f"plugin override Claude settings hooks not found: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    hooks = payload.get("hooks")
    if not isinstance(hooks, dict):
        raise SystemExit(f"{path}: must contain a top-level hooks object")
    stages: dict[str, list[dict[str, Any]]] = {}
    for stage, entries in hooks.items():
        if not isinstance(stage, str) or not isinstance(entries, list):
            raise SystemExit(
                f"{path}: hooks.{stage!r} must be a list of Claude hook entries"
            )
        if stage not in CLAUDE_HOOK_STAGES:
            raise SystemExit(
                f"{path}: unknown Claude hook stage {stage!r}; must be one of "
                f"{', '.join(CLAUDE_HOOK_STAGES)}"
            )
        # Raise on a malformed (non-dict) entry rather than silently dropping
        # it — symmetric with the stage-type guard above. A swallowed entry is
        # a silently-missing guard hook, the exact drift class this surface
        # exists to eliminate.
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise SystemExit(
                    f"{path}: hooks.{stage}[{index}] must be a Claude hook "
                    f"entry object, found {type(entry).__name__}"
                )
        stages[stage] = list(entries)
    return stages


def merge_claude_hook_stages(
    base: dict[str, list[dict[str, Any]]],
    override: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Compositional per-stage hook merge for ``add`` overrides.

    Every base entry is preserved verbatim — a stage may legitimately carry
    several entries sharing one matcher (the contract renders two ``Bash``
    PostToolUse guards, two ``Edit|Write|...`` PreToolUse guards, and two
    empty-matcher SessionStart hooks), so base entries are never collapsed
    against each other. Each override entry with a non-empty matcher replaces
    the first base entry sharing that exact matcher (override wins on
    collision) and otherwise appends; an entry with an empty/absent matcher
    never collides and is always appended, so matcher-less stages
    (SessionStart, UserPromptSubmit) accumulate rather than silently replace.
    """
    result = {stage: list(entries) for stage, entries in base.items()}
    for stage, override_entries in override.items():
        merged = list(result.get(stage) or [])
        # Indices already consumed by THIS override pass: a second override
        # entry sharing a matcher must not clobber the first override entry it
        # just inserted — it claims the next unclaimed base slot or appends, so
        # sibling override entries coexist instead of collapsing to the last.
        claimed: set[int] = set()
        for entry in override_entries:
            matcher = entry.get("matcher", "")
            replaced = False
            if matcher:
                for index, existing in enumerate(merged):
                    if index in claimed:
                        continue
                    if existing.get("matcher", "") == matcher:
                        merged[index] = entry
                        claimed.add(index)
                        replaced = True
                        break
            if not replaced:
                claimed.add(len(merged))
                merged.append(entry)
        result[stage] = merged
    return result


def replace_claude_hook_stages(
    base: dict[str, list[dict[str, Any]]],
    override: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Replace whole stages declared in ``override``; keep other base stages."""
    result = {stage: list(entries) for stage, entries in base.items()}
    for stage, override_entries in override.items():
        result[stage] = list(override_entries)
    return result


def compose_claude_settings_hooks_overrides(
    base_hooks: dict[str, list[dict[str, Any]]],
    override_root: Path,
    payload: PluginOverrideManifest,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, str]]]:
    composed = {stage: list(entries) for stage, entries in base_hooks.items()}
    components: list[dict[str, str]] = []
    handlers = {
        "add": merge_claude_hook_stages,
        "override": replace_claude_hook_stages,
    }
    for name, override in sorted(payload.components.claude_settings_hooks.items()):
        mode = override.mode
        handler = handlers.get(mode)
        if handler is None:
            raise SystemExit(
                f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: "
                f"components.claude_settings_hooks.{name}.mode must be one of "
                f"'add' or 'override'; found {mode!r}"
            )
        stage_hooks = _read_override_claude_settings_hooks(override_root, override.path)
        composed = handler(composed, stage_hooks)
        components.append(
            {
                "component_kind": "claude_settings_hooks",
                "name": name,
                "mode": mode,
                # Per-component fingerprint of this override's own contribution
                # (mode + the override file's hook payload), matching the
                # per-component digest semantics used by skill / mcp_server /
                # portable_command. Hashing the running ``composed`` accumulator
                # instead would make every component's digest order-dependent.
                "effective_digest": sha256_digest(
                    json.dumps(
                        {"mode": mode, "hooks": stage_hooks},
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                ),
                "override_path": override.path,
            }
        )
    return composed, components


def compose_plugin_portable_command_overrides(
    manifest: dict,
    override_root: Path,
    payload: PluginOverrideManifest,
    *,
    composed_skills: dict[str, str],
) -> tuple[dict, list[dict[str, str]]]:
    composed = deepcopy(manifest)
    components: list[dict[str, str]] = []
    handlers = {
        "add": _portable_command_add,
    }
    for command_id, override in sorted(payload.components.portable_commands.items()):
        mode = override.mode
        handler = handlers.get(mode)
        if handler is None:
            raise SystemExit(
                f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.portable_commands.{command_id}.mode "
                f"must be one of 'add' for the current composition slice; found {mode!r}"
            )
        composed, component = handler(
            command_id,
            override,
            manifest=composed,
            override_root=override_root,
            composed_skills=composed_skills,
        )
        components.append(component)
    return composed, components

def _resolve_override_doc_path(
    override_root: Path, kind: Literal["rules", "guides"], relative_path: str
) -> Path:
    root = override_root.resolve()
    candidates = [
        override_root / relative_path,
        override_root / kind / relative_path,
        override_root / kind / Path(relative_path).name,
    ]
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file() and resolved.is_relative_to(root):
            return resolved
    raise SystemExit(
        f"plugin override {kind} doc not found under {override_root}: {relative_path}"
    )


def compose_doc_surface_overrides(
    *,
    target_rules_dir: Path,
    override_root: Path,
    manifest: PluginOverrideManifest,
    kind: Literal["rules", "guides"],
) -> list[dict[str, str]]:
    declared = getattr(manifest.components, kind)
    components: list[dict[str, str]] = []
    for name, override in sorted(declared.items()):
        source_path = _resolve_override_doc_path(override_root, kind, override.path)
        dest_name = Path(override.path).name
        dest = target_rules_dir / dest_name
        mode = override.mode
        if mode == "add":
            if dest.is_file():
                raise SystemExit(
                    f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot add existing "
                    f"{kind} doc {name!r} at {dest_name!r}; use mode=replace instead"
                )
        elif mode == "replace":
            if not dest.is_file():
                raise SystemExit(
                    f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: cannot replace missing "
                    f"{kind} doc {name!r} at {dest_name!r}; materialize the base surface first"
                )
        else:
            raise SystemExit(
                f"{override_root / PLUGIN_OVERRIDE_MANIFEST}: components.{kind}.{name}.mode "
                f"must be one of 'add' or 'replace'; found {mode!r}"
            )
        content = source_path.read_text()
        target_rules_dir.mkdir(parents=True, exist_ok=True)
        dest.write_text(content)
        components.append(
            {
                "component_kind": kind,
                "name": name,
                "mode": mode,
                "effective_digest": sha256_digest(content),
                "override_path": override.path,
            }
        )
    return components

