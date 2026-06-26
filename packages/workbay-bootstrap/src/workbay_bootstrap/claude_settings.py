"""Wholesale Claude project settings ownership helpers (implementation note)."""

from __future__ import annotations

import importlib.util
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from workbay_bootstrap.harnesses import (
    CLAUDE_SETTINGS_PATH,
    HARNESS_PROTOCOL_REL,
    PLUGIN_MARKETPLACE_NAME,
    PLUGIN_SELECTOR,
)

CLAUDE_SETTINGS_LOCAL_PATH = Path(".claude") / "settings.local.json"
MANAGED_BY = "workbay-bootstrap"
MANAGED_TOP_LEVEL_KEYS = frozenset(
    {"hooks", "enabledPlugins", "extraKnownMarketplaces", "_managed_by"}
)
GENERATOR_SCRIPT = "scripts/generate_agent_workflows.py"
PLUGIN_OVERRIDE_MANIFEST = "overrides.yaml"


def _resolve_existing_in_clone(clone: Path, relpath: str) -> Path:
    from workbay_bootstrap.surfaces import clone_layout_probe_roots

    payload_root, nested_root, clone_root = clone_layout_probe_roots(clone)
    for root in (payload_root, nested_root, clone_root):
        candidate = root / relpath
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"missing {relpath!r} under clone {clone} "
        f"(probed {payload_root}, {nested_root}, {clone_root})"
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


def _load_override_manifest(override_root: Path | None):
    if override_root is None:
        return None
    from workbay_protocol.bootstrap import PluginOverrideManifest

    manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    if not manifest_path.is_file():
        return None
    payload = yaml.safe_load(manifest_path.read_text()) or {}
    return PluginOverrideManifest.model_validate(payload)


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
    return {
        "_managed_by": MANAGED_BY,
        "extraKnownMarketplaces": marketplaces,
        "enabledPlugins": enabled,
        "hooks": hooks,
    }


def managed_claude_settings_document(
    clone: Path,
    existing: dict[str, Any] | None,
    *,
    override_root: Path | None = None,
) -> dict[str, Any]:
    hooks = render_managed_hooks(clone, override_root=override_root)
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
    )


def _extract_non_managed_top_level(existing: dict[str, Any]) -> dict[str, Any]:
    residual: dict[str, Any] = {}
    for key, value in existing.items():
        if key not in MANAGED_TOP_LEVEL_KEYS:
            residual[key] = deepcopy(value)
    return residual


# Matches a ``$CLAUDE_PROJECT_DIR/<path>`` token (with optional surrounding
# quotes) anywhere in a hook command string, capturing the repo-relative path.
_PROJECT_DIR_TOKEN_RE = re.compile(r"""["']?\$CLAUDE_PROJECT_DIR/([^"'\s]*)["']?""")


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
    """
    return _PROJECT_DIR_TOKEN_RE.sub(r"\1", command)


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
        tuple(
            _normalize_hook_command(str(hook.get("command", "")))
            for hook in hooks
            if isinstance(hook, dict)
        ),
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
            if (
                key in stage_managed_keys
                or key in stage_local_keys
                or key in seen_in_stage
            ):
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
    residual_top = _extract_non_managed_top_level(existing_project)
    migrated_hooks = _migrate_residual_hooks(
        existing_project, managed, existing_local=pruned_local
    )
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


def write_wholesale_claude_settings(
    target: Path,
    clone: Path,
    *,
    override_root: Path | None,
    write_json: Any,
) -> list[dict[str, str]]:
    settings_path = target / CLAUDE_SETTINGS_PATH
    local_path = target / CLAUDE_SETTINGS_LOCAL_PATH
    existing_project: dict[str, Any] | None = None
    if settings_path.is_file():
        existing_project = json.loads(settings_path.read_text())
        if not isinstance(existing_project, dict):
            raise ValueError(f"{settings_path} must contain a JSON object")
    existing_local: dict[str, Any] | None = None
    if local_path.is_file():
        existing_local = json.loads(local_path.read_text())
        if not isinstance(existing_local, dict):
            raise ValueError(f"{local_path} must contain a JSON object")

    managed = managed_claude_settings_document(
        clone, existing_project, override_root=override_root
    )
    local_doc = migrate_residual_claude_settings(
        existing_project, managed, existing_local
    )

    entries = [
        write_json(
            settings_path,
            managed,
            manifest_path=CLAUDE_SETTINGS_PATH.as_posix(),
        )
    ]
    if local_doc is not None:
        entries.append(ensure_claude_settings_local_gitignore(target))
        local_path.parent.mkdir(parents=True, exist_ok=True)
        entries.append(
            write_json(
                local_path,
                local_doc,
                manifest_path=CLAUDE_SETTINGS_LOCAL_PATH.as_posix(),
            )
        )
    return entries