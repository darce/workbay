"""Wholesale Claude project settings ownership helpers (implementation note)."""

from __future__ import annotations

import fnmatch
import importlib.util
import json
import os
import re
import stat
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from workbay_bootstrap.harnesses import (
    CLAUDE_SETTINGS_PATH,
    HARNESS_PROTOCOL_REL,
    PLUGIN_MARKETPLACE_NAME,
    PLUGIN_SELECTOR,
)

if TYPE_CHECKING:
    from workbay_protocol.bootstrap import PluginOverrideManifest

CLAUDE_SETTINGS_LOCAL_PATH = Path(".claude") / "settings.local.json"
CLAUDE_SETTINGS_HOOKS_PATH = Path(".claude") / "settings.hooks.json"
CLAUDE_PERMISSIONS_POLICY_REL = Path("config/claude-permissions/policy.json")
MANAGED_BY = "workbay-bootstrap"
MANAGED_TOP_LEVEL_KEYS = frozenset(
    {
        "hooks",
        "enabledPlugins",
        "extraKnownMarketplaces",
        "permissions",
        "_permission_overrides",
        "_managed_by",
    }
)
GENERATOR_SCRIPT = "scripts/generate_agent_workflows.py"
PLUGIN_OVERRIDE_MANIFEST = "overrides.yaml"


class ClaudePermissionError(ValueError):
    """A Claude permission component or effective allow rule is unsafe."""


@dataclass
class LoadedPluginOverrideManifest:
    """Typed plugin overrides plus components the protocol model does not own."""

    manifest: PluginOverrideManifest | None
    separated_components: dict[str, Any]

    @property
    def claude_permissions(self) -> dict[str, list[dict[str, Any]]]:
        return self.separated_components.get("claude_permissions", {"add": [], "remove": []})

    def to_payload(self) -> dict[str, Any]:
        if self.manifest is None:
            raise ValueError("cannot serialize a missing plugin override manifest")
        payload = self.manifest.model_dump(mode="python", exclude_unset=True)
        payload.setdefault("components", {}).update(deepcopy(self.separated_components))
        return payload


def _resolve_existing_in_clone(clone: Path, relpath: str) -> Path:
    from workbay_bootstrap.surfaces import clone_layout_probe_roots

    payload_root, nested_root, clone_root = clone_layout_probe_roots(clone)
    for root in (payload_root, nested_root, clone_root):
        candidate = root / relpath
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"missing {relpath!r} under clone {clone} (probed {payload_root}, {nested_root}, {clone_root})"
    )


def claude_generator_available(clone: Path) -> bool:
    """Whether the Claude settings generator script is resolvable in ``clone``.

    ``write_plugin_pins`` supports a clone-less call shape (``clone or target``)
    for callers that only need the marketplace pins; in that degenerate case
    there is no overlay generator/contract to render managed settings from, so
    the wholesale write must be skipped rather than raising — mirroring the
    ``ClaudeHarnessAdapter.activation_step`` ``skipped_no_contract``
    degradation. Real installs always pass a populated clone, so this returns
    True there and the wholesale write proceeds unchanged.
    """
    try:
        _resolve_existing_in_clone(clone, GENERATOR_SCRIPT)
    except FileNotFoundError:
        return False
    return True


def _load_generator_module(clone: Path):
    script = _resolve_existing_in_clone(clone, GENERATOR_SCRIPT)
    spec = importlib.util.spec_from_file_location("gaw_claude_settings", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load generator module from {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_harness_hooks(clone: Path) -> dict[str, Any]:
    contract_path = _resolve_existing_in_clone(clone, HARNESS_PROTOCOL_REL.as_posix())
    contract = yaml.safe_load(contract_path.read_text()) or {}
    hooks = contract.get("hooks")
    if not isinstance(hooks, dict):
        raise ValueError(f"{contract_path} is missing hooks")
    return hooks


def _validate_permission_rule(rule: Any, *, context: str) -> dict[str, Any]:
    if not isinstance(rule, dict):
        raise ValueError(f"{context} must be an object")
    name = rule.get("rule")
    effect = rule.get("effect")
    canon = rule.get("canon")
    why = rule.get("why")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{context}.rule must be a nonempty string")
    if effect not in {"deny", "ask", "allow"}:
        raise ValueError(f"{context}.effect must be deny, ask, or allow")
    if not isinstance(canon, list) or not canon or any(not isinstance(cite, str) or not cite.strip() for cite in canon):
        raise ValueError(f"{context}.canon must be a nonempty list of citations")
    if not isinstance(why, str) or not why.strip():
        raise ValueError(f"{context}.why must be a nonempty string")
    if set(rule) != {"rule", "effect", "canon", "why"}:
        raise ValueError(f"{context} has unknown fields")
    return {
        "rule": name,
        "effect": effect,
        "canon": list(canon),
        "why": why,
    }


def _load_claude_permission_policy(clone: Path) -> dict[str, Any]:
    try:
        policy_path = _resolve_existing_in_clone(clone, CLAUDE_PERMISSIONS_POLICY_REL.as_posix())
    except FileNotFoundError:
        # Bootstrap's installed workbay-system package ships the same payload
        # and covers clone fixtures or older consumer clones that predate this
        # managed setting.
        from workbay_system import data_root

        policy_path = data_root() / CLAUDE_PERMISSIONS_POLICY_REL
    policy = json.loads(policy_path.read_text())
    if not isinstance(policy, dict) or policy.get("schema") != 1:
        raise ValueError(f"{policy_path} must use Claude permission schema 1")
    rules = policy.get("rules")
    if not isinstance(rules, list) or not rules:
        raise ValueError(f"{policy_path} must contain a nonempty rules list")
    validated = [
        _validate_permission_rule(rule, context=f"{policy_path} rules[{index}]") for index, rule in enumerate(rules)
    ]
    names = [rule["rule"] for rule in validated]
    if len(names) != len(set(names)):
        raise ValueError(f"{policy_path} contains duplicate rule names")
    return {"schema": 1, "rules": validated}


def _validate_claude_permission_overrides(
    declared: Any,
    *,
    manifest_path: Path,
) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(declared, dict):
        raise ClaudePermissionError(f"{manifest_path} components.claude_permissions must be a mapping")
    unknown = set(declared) - {"add", "remove"}
    if unknown:
        raise ClaudePermissionError(
            f"{manifest_path} components.claude_permissions has unknown fields: {', '.join(sorted(unknown))}"
        )
    additions = declared.get("add", [])
    removals = declared.get("remove", [])
    if not isinstance(additions, list) or not isinstance(removals, list):
        raise ClaudePermissionError(f"{manifest_path} permission add and remove must be lists")
    validated_additions = [
        _validate_permission_rule(item, context=f"{manifest_path} add[{index}]") for index, item in enumerate(additions)
    ]
    validated_removals: list[dict[str, Any]] = []
    for index, item in enumerate(removals):
        context = f"{manifest_path} remove[{index}]"
        if not isinstance(item, dict):
            raise ClaudePermissionError(f"{context} must be an object")
        name = item.get("rule")
        why = item.get("why")
        if not isinstance(name, str) or not name.strip():
            raise ClaudePermissionError(f"{context}.rule must be a nonempty string")
        if not isinstance(why, str) or not why.strip():
            raise ClaudePermissionError(f"{context}.why is required to remove a policy rule")
        if set(item) != {"rule", "why"}:
            raise ClaudePermissionError(f"{context} accepts only rule and why")
        validated_removals.append({"rule": name, "why": why})
    removal_names = [item["rule"] for item in validated_removals]
    if len(removal_names) != len(set(removal_names)):
        raise ClaudePermissionError(f"{manifest_path} contains duplicate permission removals")
    return {"add": validated_additions, "remove": validated_removals}


def load_plugin_override_manifest(
    override_root: Path | None,
) -> LoadedPluginOverrideManifest:
    """Validate one plugin manifest and separate its Claude permission extension."""
    if override_root is None:
        return LoadedPluginOverrideManifest(None, {})
    manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    if not manifest_path.is_file():
        return LoadedPluginOverrideManifest(None, {})

    payload = yaml.safe_load(manifest_path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{manifest_path} must contain a YAML mapping")
    components = payload.get("components", {})
    if not isinstance(components, dict):
        raise ValueError(f"{manifest_path} components must be a mapping")
    separated_components: dict[str, Any] = {}
    if "claude_permissions" in components:
        separated_components["claude_permissions"] = _validate_claude_permission_overrides(
            components["claude_permissions"], manifest_path=manifest_path
        )

    # The protocol model remains strict for every established component.
    from workbay_protocol.bootstrap import PluginOverrideManifest

    manifest_payload = deepcopy(payload)
    manifest_payload.get("components", {}).pop("claude_permissions", None)
    manifest = PluginOverrideManifest.model_validate(manifest_payload)
    return LoadedPluginOverrideManifest(manifest, separated_components)


def _load_claude_permission_overrides(
    override_root: Path | None,
) -> dict[str, list[dict[str, Any]]]:
    return load_plugin_override_manifest(override_root).claude_permissions


def _permission_pattern_shadows(allow_rule: str, restricted_rule: str) -> bool:
    # Either pattern may be broader. Discarding a partially shadowed grant is
    # conservative and avoids presenting a misleading allow to Claude Code.
    return fnmatch.fnmatchcase(allow_rule, restricted_rule) or fnmatch.fnmatchcase(restricted_rule, allow_rule)


def _validate_command_executing_allow_rules(allow_rules: list[str]) -> None:
    for name in allow_rules:
        if not name.startswith("Bash(") or not name.endswith(")"):
            continue
        command = name[len("Bash(") : -1]
        tokens = command.split()
        has_make_variable_override = any(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", token) for token in tokens)
        # fnmatch honours *, ?, and bracket classes even inside shell quotes
        # or after backslashes. Reject every class opener, including malformed
        # classes and literal bracket escapes, to keep exact grants fail-closed.
        if any(marker in command for marker in "*?[") or has_make_variable_override:
            raise ClaudePermissionError(
                f"claude permission effective allow rule {name!r} admits a wildcard argument or command variable override"
            )


def _migrate_legacy_managed_allows(
    tracked_allows: list[str], policy_rules: list[dict[str, Any]]
) -> tuple[list[str], list[dict[str, Any]]]:
    exact_make_allows = [
        rule["rule"] for rule in policy_rules if rule["effect"] == "allow" and rule["rule"].startswith("Bash(make ")
    ]
    equivalents = {
        "Bash(make test-* *)": [rule for rule in exact_make_allows if rule.startswith("Bash(make test-")],
        "Bash(make check-* *)": [rule for rule in exact_make_allows if rule.startswith("Bash(make check-")],
        "Bash(make context *)": [rule for rule in exact_make_allows if rule == "Bash(make context)"],
    }
    retained: list[str] = []
    migrated: list[dict[str, Any]] = []
    for rule in tracked_allows:
        replacement = equivalents.get(rule)
        if replacement is None:
            retained.append(rule)
            continue
        migrated.append({"from": rule, "to": list(replacement)})
        retained.extend(replacement)
    return retained, migrated


def _render_claude_permissions(
    policy: dict[str, Any],
    existing_permissions: Any,
    overrides: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, list[str]], dict[str, Any] | None]:
    policy_rules = policy["rules"]
    rules_by_name = {rule["rule"]: rule for rule in policy_rules}
    removed: list[dict[str, Any]] = []
    removal_names = {item["rule"] for item in overrides["remove"]}
    unknown_removals = removal_names - set(rules_by_name)
    if unknown_removals:
        raise ClaudePermissionError(
            "claude permission override can remove only policy rules; unknown: " + ", ".join(sorted(unknown_removals))
        )
    for request in overrides["remove"]:
        removed_rule = rules_by_name[request["rule"]]
        removed.append(
            {
                "rule": removed_rule["rule"],
                "effect": removed_rule["effect"],
                "why": request["why"],
            }
        )

    rules = [rule for rule in policy_rules if rule["rule"] not in removal_names]
    added: list[dict[str, Any]] = []
    for rule in overrides["add"]:
        same_name = [item for item in (*rules, *added) if item["rule"] == rule["rule"]]
        if same_name and any(item["effect"] != rule["effect"] for item in same_name):
            raise ClaudePermissionError(f"claude permission override conflicts with existing rule {rule['rule']!r}")
        if not same_name:
            added.append(rule)

    all_rules = [*rules, *added]
    deny = [rule["rule"] for rule in all_rules if rule["effect"] == "deny"]
    ask = [rule["rule"] for rule in all_rules if rule["effect"] == "ask"]
    allow_candidates = [rule["rule"] for rule in all_rules if rule["effect"] == "allow"]
    tracked_allow: list[str] = []
    if isinstance(existing_permissions, dict):
        tracked_allow = existing_permissions.get("allow", [])
        if tracked_allow is None:
            tracked_allow = []
        if not isinstance(tracked_allow, list) or any(
            not isinstance(rule, str) or not rule.strip() for rule in tracked_allow
        ):
            raise ValueError("tracked Claude permissions.allow must be a list of rules")
        tracked_allow, migrated = _migrate_legacy_managed_allows(tracked_allow, policy_rules)
    else:
        migrated = []
    allow_candidates.extend(tracked_allow)
    unique_allows = list(dict.fromkeys(allow_candidates))
    restrictions = [*deny, *ask]
    shadowed = [
        rule
        for rule in unique_allows
        if any(_permission_pattern_shadows(rule, restricted) for restricted in restrictions)
    ]
    shadowed_set = set(shadowed)
    effective_allows = [rule for rule in unique_allows if rule not in shadowed_set]
    _validate_command_executing_allow_rules(effective_allows)
    permissions = {
        "deny": deny,
        "ask": ask,
        "allow": effective_allows,
    }
    audit: dict[str, Any] = {}
    if overrides["add"]:
        audit["added"] = [dict(rule) for rule in added]
    if removed:
        audit["removed"] = removed
    if migrated:
        audit["migrated_allows"] = migrated
    if shadowed:
        audit["shadowed_allow_count"] = len(shadowed)
        audit["shadowed_allows"] = shadowed
    return permissions, audit or None


def _load_override_manifest(override_root: Path | None):
    return load_plugin_override_manifest(override_root).manifest


def render_managed_hooks(
    clone: Path,
    *,
    override_root: Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    generator = _load_generator_module(clone)
    hooks_spec = _load_harness_hooks(clone)
    manifest = _load_override_manifest(override_root)
    if manifest is None:
        rendered = generator.render_claude_hooks_config(hooks_spec)
    else:
        rendered = generator.render_composed_claude_settings_hooks(
            hooks_spec,
            override_root=override_root,
            override_manifest=manifest,
        )
    hooks = rendered.get("hooks")
    if not isinstance(hooks, dict):
        raise ValueError("rendered Claude hooks payload is missing hooks")
    return hooks


def marketplace_block() -> dict[str, Any]:
    return {
        PLUGIN_MARKETPLACE_NAME: {
            "source": {
                "source": "directory",
                "path": ".",
            }
        }
    }


def build_managed_claude_settings(
    hooks: dict[str, list[dict[str, Any]]],
    *,
    preserve_enabled_plugins: dict[str, Any] | None = None,
    preserve_marketplaces: dict[str, Any] | None = None,
    permissions: dict[str, list[str]] | None = None,
    permission_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    enabled = {PLUGIN_SELECTOR: True}
    if isinstance(preserve_enabled_plugins, dict):
        if PLUGIN_SELECTOR in preserve_enabled_plugins:
            enabled[PLUGIN_SELECTOR] = preserve_enabled_plugins[PLUGIN_SELECTOR]
        for key, value in preserve_enabled_plugins.items():
            if key != PLUGIN_SELECTOR:
                enabled[key] = value
    # The workbay marketplace entry is always (re)stamped from the canonical
    # block, but any user-declared marketplace is preserved so the wholesale
    # overwrite does not drop it — and so a preserved enabledPlugins selector
    # cannot end up referencing a marketplace that no longer exists (F-D3).
    marketplaces = dict(marketplace_block())
    if isinstance(preserve_marketplaces, dict):
        for key, value in preserve_marketplaces.items():
            if key != PLUGIN_MARKETPLACE_NAME:
                marketplaces[key] = deepcopy(value)
    settings = {
        "_managed_by": MANAGED_BY,
        "extraKnownMarketplaces": marketplaces,
        "enabledPlugins": enabled,
        "hooks": hooks,
    }
    if permissions is not None:
        settings["permissions"] = deepcopy(permissions)
    if permission_overrides is not None:
        settings["_permission_overrides"] = deepcopy(permission_overrides)
    return settings


def managed_claude_settings_document(
    clone: Path,
    existing: dict[str, Any] | None,
    *,
    override_root: Path | None = None,
) -> dict[str, Any]:
    hooks = render_managed_hooks(clone, override_root=override_root)
    permission_policy = _load_claude_permission_policy(clone)
    permission_overrides = _load_claude_permission_overrides(override_root)
    existing_permissions = existing.get("permissions") if isinstance(existing, dict) else None
    permissions, permission_audit = _render_claude_permissions(
        permission_policy, existing_permissions, permission_overrides
    )
    preserve_enabled = None
    preserve_marketplaces = None
    if isinstance(existing, dict):
        enabled = existing.get("enabledPlugins")
        if isinstance(enabled, dict):
            preserve_enabled = dict(enabled)
        marketplaces = existing.get("extraKnownMarketplaces")
        if isinstance(marketplaces, dict):
            preserve_marketplaces = dict(marketplaces)
    return build_managed_claude_settings(
        hooks,
        preserve_enabled_plugins=preserve_enabled,
        preserve_marketplaces=preserve_marketplaces,
        permissions=permissions,
        permission_overrides=permission_audit,
    )


def _extract_non_managed_top_level(
    existing: dict[str, Any], *, managed_keys: frozenset[str] = MANAGED_TOP_LEVEL_KEYS
) -> dict[str, Any]:
    residual: dict[str, Any] = {}
    for key, value in existing.items():
        if key not in managed_keys:
            residual[key] = deepcopy(value)
    return residual


# Matches a ``$CLAUDE_PROJECT_DIR/<path>`` token (with optional surrounding
# quotes) anywhere in a hook command string, capturing the repo-relative path.
_PROJECT_DIR_TOKEN_RE = re.compile(r"""["']?\$CLAUDE_PROJECT_DIR/([^"'\s]*)["']?""")

# Known managed-hook path renames (repo-relative, after project-dir strip).
# implementation note Defect 2 / BR-0106-S3-01: re-pointing SessionStart from
# ``.claude/hooks/ensure-agent-surfaces.sh`` to ``scripts/hooks/...`` makes
# raw command strings unequal, so residual migration would park the pre-
# rename Project registration into settings.local while managed settings
# keep the new path (dual SessionStart). Alias old → new so the canonical
# key absorbs the rename.
_HOOK_COMMAND_PATH_ALIASES: tuple[tuple[str, str], ...] = (
    (
        ".claude/hooks/ensure-agent-surfaces.sh",
        "scripts/hooks/ensure-agent-surfaces.sh",
    ),
)


def _normalize_hook_command(command: str) -> str:
    """Collapse ``$CLAUDE_PROJECT_DIR/<path>`` to the bare repo-relative ``<path>``.

    A hook command that was migrated from a repo-relative form
    (``bash .claude/hooks/x.sh``) to the absolute ``$CLAUDE_PROJECT_DIR`` form
    (``bash "$CLAUDE_PROJECT_DIR/.claude/hooks/x.sh"``) — or vice versa — refers
    to the *same* hook. Normalizing both to the bare path makes the dedup key
    recognize them as one entry, so a managed-side rename does not orphan a
    stale copy in ``settings.local.json``. The rewrite is targeted to the
    ``$CLAUDE_PROJECT_DIR/`` token (and the quotes hugging that one path), so
    unrelated quoting elsewhere in the command is left intact.

    After the project-dir strip, known managed path renames are rewritten to
    their current location so residual migration absorbs pre-rename Project
    registrations instead of dual-registering (BR-0106-S3-01).
    """
    command = _PROJECT_DIR_TOKEN_RE.sub(r"\1", command)
    for old, new in _HOOK_COMMAND_PATH_ALIASES:
        if old in command:
            command = command.replace(old, new)
    return command


_CONSUMER_ROOT_TOKEN_RE = re.compile(r"\{\{consumer_root\}\}/?")


def normalize_hook_command_for_compare(command: str, *, target: Path) -> str:
    """Normalize hook command strings for drift comparison across portable forms."""
    import os

    command = _CONSUMER_ROOT_TOKEN_RE.sub("", command)
    command = _normalize_hook_command(command)
    root = str(target.resolve())
    prefix = root + os.sep
    if command.startswith(prefix):
        command = command[len(prefix) :]
    elif command == root:
        command = ""
    return command


def _hook_entry_canonical_key(entry: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
    """Stable dedup key: (matcher, normalized hook commands). Ignores ``_managed_by``, timeout, etc."""
    # ``or ""`` collapses None / missing / "" to one matcher so an explicit
    # ``matcher: null`` canonicalizes identically to an omitted/empty matcher.
    matcher = str(entry.get("matcher") or "")
    hooks = entry.get("hooks")
    if not isinstance(hooks, list):
        return (matcher, ())
    return (
        matcher,
        tuple(_normalize_hook_command(str(hook.get("command", ""))) for hook in hooks if isinstance(hook, dict)),
    )


def _canonical_keys_by_stage(
    document: dict[str, Any] | None,
) -> dict[str, set[tuple[str, tuple[str, ...]]]]:
    keys_by_stage: dict[str, set[tuple[str, tuple[str, ...]]]] = {}
    if not isinstance(document, dict):
        return keys_by_stage
    hooks = document.get("hooks")
    if not isinstance(hooks, dict):
        return keys_by_stage
    for stage, entries in hooks.items():
        if not isinstance(entries, list):
            continue
        stage_keys = keys_by_stage.setdefault(stage, set())
        for entry in entries:
            if isinstance(entry, dict):
                stage_keys.add(_hook_entry_canonical_key(entry))
    return keys_by_stage


def _migrate_residual_hooks(
    existing: dict[str, Any],
    managed: dict[str, Any],
    *,
    existing_local: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Migrate every user hook entry NOT already present in the managed render
    to ``.local.json`` (internal: relocate residual hooks rather than dropping
    them under wholesale ownership).

    A wholesale overwrite of the Project file would otherwise silently drop any
    user hook whose matcher does not collide with a managed matcher (e.g. a
    ``PreToolUse:Read`` hook, or a whole stage the generator never emits such
    as ``Stop``). The managed subtree is the set-difference anchor: an entry
    whose canonical key ``(matcher, tuple(hook commands))`` matches a managed
    render entry is already present in the rewritten Project file and is
    dropped (not relocated), which also makes
    re-running ``pin_surfaces`` idempotent (a second pass reads back the
    managed-only document and migrates nothing — managed PostToolUse hooks are
    never relocated to local, satisfying grok guardrail internalb).
    """
    managed_keys = _canonical_keys_by_stage(managed)
    local_keys = _canonical_keys_by_stage(existing_local)
    migrated: dict[str, list[dict[str, Any]]] = {}
    hooks = existing.get("hooks")
    if not isinstance(hooks, dict):
        return migrated
    for stage, entries in hooks.items():
        if not isinstance(entries, list):
            continue
        stage_managed_keys = managed_keys.get(stage, set())
        stage_local_keys = local_keys.get(stage, set())
        seen_in_stage: set[tuple[str, tuple[str, ...]]] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key = _hook_entry_canonical_key(entry)
            if key in stage_managed_keys or key in stage_local_keys or key in seen_in_stage:
                continue
            seen_in_stage.add(key)
            migrated.setdefault(stage, []).append(deepcopy(entry))
    return migrated


def merge_local_claude_settings(
    existing_local: dict[str, Any] | None,
    *,
    residual_top_level: dict[str, Any],
    migrated_hooks: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    local = deepcopy(existing_local) if isinstance(existing_local, dict) else {}
    for key, value in residual_top_level.items():
        local[key] = deepcopy(value)
    if not migrated_hooks:
        return local
    hooks = local.setdefault("hooks", {})
    if not isinstance(hooks, dict):
        hooks = {}
        local["hooks"] = hooks
    for stage, entries in migrated_hooks.items():
        stage_entries = hooks.setdefault(stage, [])
        if not isinstance(stage_entries, list):
            stage_entries = []
            hooks[stage] = stage_entries
        stage_entries.extend(deepcopy(entries))
    return local


def _prune_local_hooks_against_managed(
    existing_local: dict[str, Any] | None,
    managed: dict[str, Any],
) -> dict[str, Any] | None:
    """Drop local hook entries already owned by the managed render (self-heal).

    The managed Project file is the single source for managed hooks; a copy of
    the same hook lingering in ``.local.json`` is redundant. Before this pass a
    ``$CLAUDE_PROJECT_DIR``-vs-relative rename left the stale local copy
    un-dedupable (its raw command string no longer matched managed), so it
    orphaned permanently. With normalized keys, a local entry whose canonical
    key matches a managed entry — or an exact within-stage duplicate — is pruned
    here, so the next ``pin_surfaces`` pass cleans the orphan instead of
    accreting it. Non-managed residual hooks (keys absent from managed) are
    preserved.
    """
    if not isinstance(existing_local, dict):
        return existing_local
    managed_keys = _canonical_keys_by_stage(managed)
    pruned = deepcopy(existing_local)
    hooks = pruned.get("hooks")
    if not isinstance(hooks, dict):
        return pruned
    for stage, entries in list(hooks.items()):
        if not isinstance(entries, list):
            continue
        stage_managed = managed_keys.get(stage, set())
        seen_in_stage: set[tuple[str, tuple[str, ...]]] = set()
        kept: list[Any] = []
        for entry in entries:
            if not isinstance(entry, dict):
                kept.append(entry)
                continue
            key = _hook_entry_canonical_key(entry)
            if key in stage_managed or key in seen_in_stage:
                continue
            seen_in_stage.add(key)
            kept.append(entry)
        if kept:
            hooks[stage] = kept
        else:
            del hooks[stage]
    if not hooks:
        del pruned["hooks"]
    return pruned


def migrate_residual_claude_settings(
    existing_project: dict[str, Any] | None,
    managed: dict[str, Any],
    existing_local: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(existing_project, dict):
        return None
    pruned_local = _prune_local_hooks_against_managed(existing_local, managed)
    managed_keys = MANAGED_TOP_LEVEL_KEYS
    if "permissions" not in managed:
        # The lower-level builder remains usable by older callers that only
        # manage hooks/plugin pins; they do not own the permissions subtree.
        managed_keys = frozenset(
            key for key in MANAGED_TOP_LEVEL_KEYS if key not in {"permissions", "_permission_overrides"}
        )
    residual_top = _extract_non_managed_top_level(existing_project, managed_keys=managed_keys)
    migrated_hooks = _migrate_residual_hooks(existing_project, managed, existing_local=pruned_local)
    if not residual_top and not migrated_hooks and existing_local is None:
        return None
    return merge_local_claude_settings(
        pruned_local,
        residual_top_level=residual_top,
        migrated_hooks=migrated_hooks,
    )


def ensure_claude_settings_local_gitignore(target: Path) -> dict[str, str]:
    gitignore = target / ".gitignore"
    entry = ".claude/settings.local.json"
    if gitignore.is_file():
        # Normalize existing patterns so an equivalent git-anchored form
        # ("/.claude/settings.local.json") or a parent-directory ignore
        # (".claude/") counts as already-covering; otherwise the append emits a
        # redundant duplicate line (observed: this repo already ignored the
        # anchored form and a bare second line was appended).
        existing = {
            line.strip().lstrip("/")
            for line in gitignore.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        if entry in existing or ".claude/" in existing:
            return {"path": ".gitignore", "action": "already_present"}
        gitignore.write_text(gitignore.read_text().rstrip() + f"\n{entry}\n")
        return {"path": ".gitignore", "action": "appended"}
    gitignore.write_text(f"{entry}\n")
    return {"path": ".gitignore", "action": "created"}


class SidecarSymlinkRefusalError(ValueError):
    """The hook sidecar destination is a symlink and cannot be adopted."""


def adopt_payload_settings_hooks(
    target: Path,
    clone: Path,
    *,
    write_json: Any,
) -> list[dict[str, str]]:
    """Adopt the payload copy of ``.claude/settings.hooks.json`` on install.

    Upstream REQUEST 2026-08-23: the payload has always shipped a correct copy
    of the sidecar while the installer declined to write it, so a stale on-disk
    copy became an ``orphan_hook_config`` the coherence gate then fail-closed
    on with no in-band repair. Materializing the payload copy through the same
    wholesale-write path as the other payload-managed Claude surfaces — and
    returning a config row so the install receipt records the file — makes the
    gate self-healing: the refusal condition is now one install itself repairs.

    Payloads that predate the sidecar (no copy in the clone/package) degrade to
    a noop rather than raising.

    Local precedence (review r1 W3, mirroring the residual-content migration in
    ``migrate_residual_claude_settings``): when an on-disk copy exists, diverges
    from the payload, and the install receipt records no previous adoption of
    it, the divergence may be user edits -- adoption is skipped and the file is
    left untouched. The coherence orphan downgrade keeps the gate a warning
    with an adopt/delete remedy, so nothing fail-closes. A receipt-known copy
    (a previous adoption wrote it) is refreshed as before.
    """
    dest = target / CLAUDE_SETTINGS_HOOKS_PATH
    if dest.is_symlink():
        raise SidecarSymlinkRefusalError(f"Refusing hook sidecar symlink: {dest}")
    try:
        source = _resolve_existing_in_clone(clone, CLAUDE_SETTINGS_HOOKS_PATH.as_posix())
    except FileNotFoundError:
        return []
    payload = json.loads(source.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"{source} must contain a JSON object")
    # The callback remains part of the calling seam, but sidecar IO must stay
    # descriptor-relative throughout; a path-based writer cannot provide this.
    return _adopt_sidecar_nofollow(target, payload)


@contextmanager
def _claude_directory(target: Path, parents: tuple[str, ...] = (), names: tuple[str, ...] = ()):
    """Pin every destination component before any Claude settings write."""
    with _settings_directory(
        target,
        (".claude", *parents),
        names or ("settings.json", "settings.local.json", "settings.hooks.json"),
    ) as directory:
        yield directory


@contextmanager
def _settings_directory(target: Path, parents: tuple[str, ...], names: tuple[str, ...]):
    """Share descriptor-relative, no-follow mediation for adapter destinations."""
    fds = []
    components = (*target.absolute().parts[1:], *parents)
    try:
        fds.append(os.open("/", os.O_RDONLY | os.O_DIRECTORY))
        for index, component in enumerate(components):
            if index >= len(target.absolute().parts) - 1:
                try:
                    os.mkdir(component, dir_fd=fds[-1])
                except FileExistsError:
                    pass
            try:
                fds.append(os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fds[-1]))
            except OSError as exc:
                raise SidecarSymlinkRefusalError(f"Refusing Claude settings parent: {target}") from exc

        def validate():
            for index, component in enumerate(components):
                try:
                    current = os.stat(component, dir_fd=fds[index], follow_symlinks=False)
                except OSError as exc:
                    raise SidecarSymlinkRefusalError(f"Refusing changed Claude settings parent: {target}") from exc
                opened = os.fstat(fds[index + 1])
                if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                    raise SidecarSymlinkRefusalError(f"Refusing swapped Claude settings parent: {target}")
            for name in names:
                try:
                    info = os.stat(name, dir_fd=fds[-1], follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise SidecarSymlinkRefusalError(
                        f"Refusing settings destination: {target.joinpath(*parents, name)}"
                    )

        validate()
        yield fds[-1], validate
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _read_claude_file(parent_fd: int, name: str):
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise SidecarSymlinkRefusalError(f"Refusing Claude settings destination: {name}") from exc
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise SidecarSymlinkRefusalError(f"Refusing Claude settings destination: {name}")
    with os.fdopen(fd, errors="surrogateescape") as stream:
        return stream.read()


def _write_claude_file(directory, name: str, payload: dict[str, Any]):
    parent_fd, validate = directory
    validate()
    previous = _read_claude_file(parent_fd, name)
    content = json.dumps(payload, indent=2) + "\n"
    action = "created" if previous is None else "unchanged" if previous == content else "updated"
    if action != "unchanged":
        temp_name = f".{name}.{uuid.uuid4().hex}.tmp"
        try:
            fd = os.open(temp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent_fd)
            with os.fdopen(fd, "w") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            validate()
            os.replace(temp_name, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            try:
                os.unlink(temp_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
    return {"path": f".claude/{name}", "action": action}


def merge_claude_document(target: Path, relative: Path, merge: Any) -> dict[str, str]:
    """Read, merge in memory, and persist through the sole no-follow writer.

    The callback returns whether a managed entry already existed, retaining
    the adapter receipt's historical created/noop/merged actions.
    """
    if relative.is_absolute() or ".." in relative.parts or len(relative.parts) < 2 or relative.parts[0] != ".claude":
        raise ValueError(f"refusing Claude adapter target: {relative}")
    with _claude_directory(target, relative.parts[1:-1], (relative.name,)) as directory:
        previous = _read_claude_file(directory[0], relative.name)
        doc = json.loads(previous) if previous is not None else {}
        matched = merge(doc)
        _write_claude_file(directory, relative.name, doc)
    action = "created" if previous is None else "noop" if matched else "merged"
    return {"path": relative.as_posix(), "action": action}


def merge_adapter_document(target: Path, relative: Path, merge: Any) -> dict[str, str]:
    """Merge a non-Claude manifest destination using the shared no-follow writer."""
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or not relative.parts
        or any(part.casefold() == ".claude" for part in relative.parts)
    ):
        raise SidecarSymlinkRefusalError(f"refusing adapter target: {relative}")
    with _settings_directory(target, relative.parts[:-1], (relative.name,)) as directory:
        previous = _read_claude_file(directory[0], relative.name)
        doc = json.loads(previous) if previous is not None else {}
        matched = merge(doc)
        _write_claude_file(directory, relative.name, doc)
    action = "created" if previous is None else "noop" if matched else "merged"
    return {"path": relative.as_posix(), "action": action}


def _adopt_sidecar_in_directory(target, payload, directory):
    previous = _read_claude_file(directory[0], CLAUDE_SETTINGS_HOOKS_PATH.name)
    if previous is not None:
        try:
            matches = json.loads(previous) == payload
        except ValueError:
            matches = False
        if not matches and not _settings_hooks_receipt_known(target):
            return []
    entry = _write_claude_file(directory, CLAUDE_SETTINGS_HOOKS_PATH.name, payload)
    entry["ownership"] = "generated"
    return [entry]


def _adopt_sidecar_nofollow(target: Path, payload: dict[str, Any]) -> list[dict[str, str]]:
    with _claude_directory(target) as directory:
        return _adopt_sidecar_in_directory(target, payload, directory)


def _sidecar_matches_payload(dest: Path, payload: dict[str, Any]) -> bool:
    """True when the on-disk sidecar parses to the same document as the
    payload copy. An unreadable/non-JSON file counts as diverged."""
    try:
        return json.loads(dest.read_text()) == payload
    except (OSError, ValueError):
        return False


def _settings_hooks_receipt_known(target: Path) -> bool:
    """True when the on-disk bootstrap receipt records the sidecar -- evidence
    that a previous adoption (not the user) wrote the current copy. A missing
    or unreadable receipt is no such evidence and reads as unknown."""
    from workbay_bootstrap.install import BOOTSTRAP_MANIFEST_NAME

    try:
        receipt = json.loads((target / BOOTSTRAP_MANIFEST_NAME).read_text())
    except (OSError, ValueError):
        return False
    if not isinstance(receipt, dict):
        return False
    rel = CLAUDE_SETTINGS_HOOKS_PATH.as_posix()
    for key in ("configs", "surfaces"):
        entries = receipt.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict) and str(entry.get("path") or "") == rel:
                return entry.get("ownership") == "generated"
    return False


def write_wholesale_claude_settings(
    target: Path,
    clone: Path,
    *,
    override_root: Path | None,
    write_json: Any,
) -> list[dict[str, str]]:
    # Retain the callback API, but all settings IO uses the pinned directory.
    with _claude_directory(target) as directory:

        def read_document(name):
            content = _read_claude_file(directory[0], name)
            if content is None:
                return None
            doc = json.loads(content)
            if not isinstance(doc, dict):
                raise ValueError(f"{name} must contain a JSON object")
            return doc

        existing_project = read_document(CLAUDE_SETTINGS_PATH.name)
        existing_local = read_document(CLAUDE_SETTINGS_LOCAL_PATH.name)
        managed = managed_claude_settings_document(clone, existing_project, override_root=override_root)
        local_doc = migrate_residual_claude_settings(existing_project, managed, existing_local)
        entries = [_write_claude_file(directory, CLAUDE_SETTINGS_PATH.name, managed)]
        if local_doc is not None:
            entries.append(_write_claude_file(directory, CLAUDE_SETTINGS_LOCAL_PATH.name, local_doc))
            entries.append(ensure_claude_settings_local_gitignore(target))
        try:
            source = _resolve_existing_in_clone(clone, CLAUDE_SETTINGS_HOOKS_PATH.as_posix())
        except FileNotFoundError:
            pass
        else:
            payload = json.loads(source.read_text())
            if not isinstance(payload, dict):
                raise ValueError(f"{source} must contain a JSON object")
            entries.extend(_adopt_sidecar_in_directory(target, payload, directory))
        return entries
