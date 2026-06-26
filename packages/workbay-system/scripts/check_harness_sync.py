#!/usr/bin/env python3
"""Check committed harness surfaces against harness-protocol.yaml.

The contract at ``docs/workbay/contracts/harness-protocol.yaml`` defines four
sections that every managed harness must keep in sync:

* ``cold_start.shared_steps``   — phrases that must appear in each shared
                                  cold-start doc (CLAUDE.md, copilot
                                  instructions).
* ``branch_isolation``          — protected branches, code roots, and file
                                  extensions that every enforcer script must
                                  reference.
* ``hooks``                     — matcher+command pairs that each harness
                                  settings file must list.
* ``python_api_fallback``       — package-root symbols the Python fallback
                                  surface must export (checked when
                                  ``--check-api-surface`` is passed).

The validator fails fast with a named error for any drift between the contract
and the committed surface. Generated per-agent artifacts such as
``.claude/settings.json`` are optional at this layer; when present with
``_managed_by: workbay-bootstrap`` the checker validates only the managed
subtree (hooks + plugin pins), leaving non-managed user keys untouched.
"""

from __future__ import annotations

import ast
from collections import Counter
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import yaml
except ModuleNotFoundError as exc:
    yaml = None
    _YAML_IMPORT_ERROR = exc
else:
    _YAML_IMPORT_ERROR = None

try:
    from scripts.overlay_resolver import (
        OverlayResolverError,
        detect_overlay_mode,
        resolve_surface,
    )
except ModuleNotFoundError:
    from overlay_resolver import (
        OverlayResolverError,
        detect_overlay_mode,
        resolve_surface,
    )

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGES_ROOT = REPO_ROOT.parent
CONTRACT_PATH = REPO_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
VSCODE_SETTINGS_PATH = REPO_ROOT / ".vscode" / "settings.json"
PYTHON_EXPORTS_PATH = (
    PACKAGES_ROOT
    / "mcp-workbay-handoff"
    / "src"
    / "workbay_handoff_mcp"
    / "__init__.py"
)
CONTRACT_RELATIVE = Path("docs/workbay/contracts/harness-protocol.yaml")
PACKAGE_CONTRACT_RELATIVE = Path("workbay_system/payload") / CONTRACT_RELATIVE
PAYLOAD_RELATIVE = Path("workbay_system/payload")
GUARD_WRAP_RELATIVE = PAYLOAD_RELATIVE / "scripts" / "_guard_wrap.py"
GENERATOR_RELATIVE = PAYLOAD_RELATIVE / "scripts" / "generate_agent_workflows.py"
CLAUDE_MANAGED_BY = "workbay-bootstrap"
CLAUDE_PLUGIN_SELECTOR = "workbay-system@workbay-marketplace"
CLAUDE_PLUGIN_MARKETPLACE_NAME = "workbay-marketplace"
CLAUDE_OVERRIDE_ROOT_REL = Path("workbay-overrides") / "workbay-system"
FIXTURE_COPY_FILES = (
    Path(".vscode/settings.json"),
    Path(".github/hooks/guard-main-branch.py"),
    Path(".github/hooks/guard-worktree-drift.py"),
    Path("scripts/hooks/_active_task_context.py"),
    Path("scripts/hooks/_branch_isolation_guard.py"),
    Path("scripts/hooks/_guard_main_branch_inline.py"),
    Path("scripts/hooks/_harness_protocol.py"),
    # _interp supplies resolve_env_alias, imported by both the VS Code
    # guard-main-branch.py and the Claude _guard_main_branch_inline.py. It
    # arrived with the WorkBay env-alias bridge but the fixture copy-list was
    # never extended; without it both guards die with ModuleNotFoundError
    # (exit 2, no BLOCKED text), which reads as "did not block code roots" AND
    # "blocked CLAUDE.md" at once. Keep this list a complete closure over the
    # guards' sibling imports.
    Path("scripts/hooks/_interp.py"),
    Path("scripts/hooks/_worktree_drift.py"),
    Path("scripts/hooks/guard-main-branch.sh"),
    Path("scripts/hooks/guard-worktree-drift.sh"),
)
FIXTURE_PACKAGE_SRC = Path("packages/mcp-workbay-handoff/src")
FIXTURE_PROTOCOL_SRC = Path("packages/workbay-protocol/src")
# Identity the fixture's lone handoff write self-attributes with, passed as an
# explicit actor= at the seed call site (see _seed_active_task). Attributing at
# the call site instead of through ambient env keeps the write hermetic
# regardless of who runs the check.
_FIXTURE_WRITE_AGENT = "check-harness-sync-fixture"
EDIT_TOOL_MATCHER = "Edit|Write|apply_patch|create_file|replace_string_in_file|multi_replace_string_in_file"
REQUIRED_VSCODE_SETTINGS = {
    "files.autoSave": "off",
    "files.refactoring.autoSave": False,
    "editor.formatOnSave": False,
}
REQUIRED_PROTECTED_MAIN_PATTERNS = (
    "docs/tasks/**/*.md",
    "docs/assessments/**",
    "docs/scopes/**",
    "docs/epics/**",
    "docs/specs/**",
    "docs/adrs/**",
    "packages/*/docs/tasks/**",
    "packages/*/docs/assessments/**",
    "packages/*/docs/specs/**",
    "packages/*/docs/epics/**",
    "packages/*/docs/adrs/**",
)


def _candidate_path(repo_root: Path, relative: Path) -> Path:
    """Resolve package-local checks against source, payload, then monorepo root."""
    candidates = [repo_root / relative, repo_root / PAYLOAD_RELATIVE / relative]
    if repo_root.name == "workbay-system" and repo_root.parent.name == "packages":
        candidates.append(repo_root.parent.parent / relative)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _load_contract(*, repo_root: Path = REPO_ROOT) -> dict:
    contract_path = _candidate_path(repo_root, CONTRACT_RELATIVE)
    package_contract_path = repo_root / PACKAGE_CONTRACT_RELATIVE
    resolved_contract = next(
        (
            path
            for path in resolve_surface("contracts", repo_root)
            if path.effective_path.name == CONTRACT_RELATIVE.name
        ),
        None,
    )

    if resolved_contract is None:
        if not contract_path.is_file():
            if not package_contract_path.is_file():
                raise OverlayResolverError(
                    f"missing harness contract `{CONTRACT_RELATIVE.as_posix()}` after overlay resolution "
                    "fell back to the source tree"
                )
            contract_path = package_contract_path
        payload = yaml.safe_load(contract_path.read_text()) or {}
    elif resolved_contract.source == "overlapping":
        shared_payload = yaml.safe_load(resolved_contract.shared_path.read_text()) or {}
        local_payload = yaml.safe_load(resolved_contract.local_path.read_text()) or {}
        if not isinstance(shared_payload, dict) or not isinstance(local_payload, dict):
            raise ValueError("harness-protocol.yaml must parse to a mapping")
        payload = dict(shared_payload)
        payload.update(local_payload)
    else:
        payload = yaml.safe_load(resolved_contract.effective_path.read_text()) or {}

    if not isinstance(payload, dict):
        raise ValueError("harness-protocol.yaml must parse to a mapping")
    return payload


def _format_success_message(*, repo_root: Path = REPO_ROOT) -> str:
    # Overlay mode (canonical bootstrap ledger or legacy mapping) is detected
    # through the shared resolver detector, not a raw `.workbay-overlay.json`
    # filename probe, so a canonical `.workbay-bootstrap.json` consumer is
    # reported with surface counts instead of a bare OK.
    if detect_overlay_mode(repo_root) == "source_tree":
        return "check-harness-sync: OK"

    counts: Counter[str] = Counter(
        path.source
        for path in resolve_surface("contracts", repo_root)
        if path.effective_path.name == CONTRACT_RELATIVE.name
    )
    total = sum(counts.values())
    return (
        "check-harness-sync: OK "
        f"(contracts={total}; shared={counts['shared']} local={counts['local']} overlapping={counts['overlapping']})"
    )


def _flatten_claude_entries(stage_entries: list[dict]) -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for entry in stage_entries:
        matcher = entry.get("matcher", "")
        for hook in entry.get("hooks", []):
            command = hook.get("command")
            if isinstance(command, str):
                found.add((matcher, command))
    return found


def _flatten_vscode_entries(stage_entries: list[dict]) -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for entry in stage_entries:
        matcher = entry.get("matcher", "")
        hooks = entry.get("hooks")
        if isinstance(hooks, list):
            for hook in hooks:
                command = hook.get("command")
                if isinstance(command, str):
                    found.add((matcher, command))
            continue
        command = entry.get("command")
        if isinstance(command, str):
            found.add((matcher, command))
    return found


# (contract-stage-key, vscode/claude-stage-key) — vscode + claude share the same
# CamelCase stage names in their respective hook configs.
HOOK_STAGES: tuple[tuple[str, str], ...] = (
    ("pre_tool_use", "PreToolUse"),
    ("post_tool_use", "PostToolUse"),
    ("session_start", "SessionStart"),
    ("user_prompt_submit", "UserPromptSubmit"),
)
GROK_PLUGIN_HOOKS_PATH = Path(
    ".workbay/generated/plugins/workbay-system/base/grok/hooks/hooks.json"
)


def _load_hook_pairs(
    *, repo_root: Path = REPO_ROOT
) -> tuple[set[tuple[str, str]], set[tuple[str, str]], set[tuple[str, str]]]:
    claude_path = _candidate_path(repo_root, Path(".claude/settings.json"))
    vscode_path = _candidate_path(repo_root, Path(".github/hooks/terminal-guard.json"))
    codex_path = _candidate_path(repo_root, Path(".codex/hooks.json"))

    claude_pairs: set[tuple[str, str]] = set()
    if claude_path.is_file():
        claude_hooks = json.loads(claude_path.read_text()).get("hooks", {})
        for _, stage_key in HOOK_STAGES:
            claude_pairs |= _flatten_claude_entries(claude_hooks.get(stage_key) or [])

    vscode_hooks = json.loads(vscode_path.read_text()).get("hooks", {})
    vscode_pairs: set[tuple[str, str]] = set()
    for _, stage_key in HOOK_STAGES:
        vscode_pairs |= _flatten_vscode_entries(vscode_hooks.get(stage_key) or [])

    # Codex mixes flat command entries with nested matcher+hooks[] groups; the
    # tolerant VS Code flattener handles both shapes.
    codex_pairs: set[tuple[str, str]] = set()
    if codex_path.is_file():
        codex_hooks = json.loads(codex_path.read_text()).get("hooks", {})
        for _, stage_key in HOOK_STAGES:
            codex_pairs |= _flatten_vscode_entries(codex_hooks.get(stage_key) or [])

    return (claude_pairs, vscode_pairs, codex_pairs)


def _load_python_exports() -> set[str]:
    module = ast.parse(
        PYTHON_EXPORTS_PATH.read_text(), filename=str(PYTHON_EXPORTS_PATH)
    )
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if not isinstance(node.value, (ast.List, ast.Tuple)):
                        raise ValueError("__all__ must be a list or tuple literal")
                    exports: set[str] = set()
                    for elt in node.value.elts:
                        if not isinstance(elt, ast.Constant) or not isinstance(
                            elt.value, str
                        ):
                            raise ValueError(
                                "__all__ must contain only string literals"
                            )
                        exports.add(elt.value)
                    return exports
    raise ValueError("workbay_handoff_mcp.__all__ not found")


def _load_grok_plugin_hook_pairs(repo_root: Path) -> set[tuple[str, str]]:
    hooks_path = _candidate_path(repo_root, GROK_PLUGIN_HOOKS_PATH)
    if not hooks_path.is_file():
        return set()
    payload = json.loads(hooks_path.read_text())
    hooks = payload.get("hooks") or {}
    entries = hooks.get("PreToolUse") or []
    pairs: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        matcher = entry.get("matcher", "")
        for hook in entry.get("hooks") or []:
            if not isinstance(hook, dict):
                continue
            command = hook.get("command")
            if isinstance(command, str) and command:
                pairs.add((matcher, command))
    return pairs


def _load_wrap_guard_command(repo_root: Path = REPO_ROOT):
    """Import the renderer's ``wrap_guard_command`` so checker and renderer
    classify the internal fail-open wrapper prefix identically
    (single-sourced from the stdlib-only payload module ``_guard_wrap.py`` —
    a local reimplementation would be a second opinion).
    """
    import importlib.util

    module_path = repo_root / GUARD_WRAP_RELATIVE
    if not module_path.is_file():
        module_path = REPO_ROOT / GUARD_WRAP_RELATIVE
    spec = importlib.util.spec_from_file_location(
        "_check_harness_sync_guard_wrap", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load guard-wrap module at {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.wrap_guard_command


def _load_claude_generator_module(repo_root: Path = REPO_ROOT):
    import importlib.util

    module_path = repo_root / GENERATOR_RELATIVE
    if not module_path.is_file():
        module_path = REPO_ROOT / GENERATOR_RELATIVE
    spec = importlib.util.spec_from_file_location(
        "_check_harness_sync_gaw", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load generator module at {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _discover_claude_override_root(repo_root: Path) -> Path | None:
    candidate = repo_root / CLAUDE_OVERRIDE_ROOT_REL
    if (candidate / "overrides.yaml").is_file():
        return candidate
    return None


def _expected_managed_claude_hooks(
    contract: dict, *, repo_root: Path = REPO_ROOT
) -> dict:
    generator = _load_claude_generator_module(repo_root)
    hooks_spec = contract.get("hooks", {})
    override_root = _discover_claude_override_root(repo_root)
    if override_root is None:
        return generator.render_claude_hooks_config(hooks_spec)["hooks"]
    from workbay_protocol.bootstrap import PluginOverrideManifest

    manifest = PluginOverrideManifest.model_validate(
        yaml.safe_load((override_root / "overrides.yaml").read_text()) or {}
    )
    return generator.render_composed_claude_settings_hooks(
        hooks_spec,
        override_root=override_root,
        override_manifest=manifest,
    )["hooks"]


def _check_managed_claude_settings(
    contract: dict, *, repo_root: Path = REPO_ROOT
) -> list[str]:
    settings_path = repo_root / ".claude" / "settings.json"
    errors: list[str] = []
    try:
        payload = json.loads(settings_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return [f"claude_settings: unable to load `.claude/settings.json`: {exc}"]
    if payload.get("_managed_by") != CLAUDE_MANAGED_BY:
        errors.append(
            "claude_settings: managed `.claude/settings.json` must declare "
            f"`_managed_by: {CLAUDE_MANAGED_BY!r}`"
        )
        return errors
    expected_hooks = _expected_managed_claude_hooks(contract, repo_root=repo_root)
    actual_hooks = payload.get("hooks")
    if actual_hooks != expected_hooks:
        errors.append(
            "claude_settings: managed hooks subtree drift from rendered contract golden"
        )
    # Presence-only by design: the wholesale writer preserves the user's
    # enabledPlugins value (so a deliberate `selector: false` opt-out survives
    # regeneration); the checker therefore asserts the selector key is present
    # for activation parity, not that it is truthy.
    enabled = payload.get("enabledPlugins")
    if not isinstance(enabled, dict) or CLAUDE_PLUGIN_SELECTOR not in enabled:
        errors.append(
            f"claude_settings: missing enabledPlugins[{CLAUDE_PLUGIN_SELECTOR!r}]"
        )
    marketplaces = payload.get("extraKnownMarketplaces")
    if not isinstance(marketplaces, dict) or CLAUDE_PLUGIN_MARKETPLACE_NAME not in (
        marketplaces or {}
    ):
        errors.append(
            "claude_settings: missing extraKnownMarketplaces "
            f"{CLAUDE_PLUGIN_MARKETPLACE_NAME!r}"
        )
    return errors


def _check_hooks(contract: dict, *, repo_root: Path = REPO_ROOT) -> list[str]:
    claude_pairs, vscode_pairs, codex_pairs = _load_hook_pairs(repo_root=repo_root)
    wrap = _load_wrap_guard_command()
    grok_pairs = _load_grok_plugin_hook_pairs(repo_root)
    errors: list[str] = []
    # Generated per-agent artifacts are optional at this layer: Claude and
    # Codex wiring is only validated when the harness config file exists.
    claude_settings_path = repo_root / ".claude" / "settings.json"
    check_claude = claude_settings_path.is_file()
    check_codex = (repo_root / ".codex" / "hooks.json").is_file()
    claude_managed_mode = False
    if check_claude:
        try:
            claude_payload = json.loads(claude_settings_path.read_text())
            claude_managed_mode = (
                isinstance(claude_payload, dict)
                and claude_payload.get("_managed_by") == CLAUDE_MANAGED_BY
            )
        except (OSError, json.JSONDecodeError):
            claude_managed_mode = False
    hook_spec = contract.get("hooks", {})
    # The codex_enabled opt-out shape rule only binds codex-aware contracts;
    # a legacy contract pre-dating the Codex fields entirely must not fail.
    codex_aware = any(
        "codex_command" in item or "codex_enabled" in item
        for stage, _ in HOOK_STAGES
        for item in hook_spec.get(stage, [])
    )
    for stage, _vscode_stage in HOOK_STAGES:
        for item in hook_spec.get(stage, []):
            # session_start / user_prompt_submit have no tool matcher.
            matcher = item.get("matcher", "")
            # Harnesses may surface MCP tools under different names (e.g.
            # VS Code uses `mcp_workbay-handoff-mcp_*`, Claude Code and Codex
            # use `mcp__workbay-handoff-mcp__*`). Per-harness matchers
            # override the shared `matcher` when present.
            claude_matcher = item.get("claude_matcher", matcher)
            vscode_matcher = item.get("vscode_matcher", matcher)
            codex_matcher = item.get("codex_matcher", matcher)
            claude_command = item.get("claude_command")
            vscode_command = item.get("vscode_command")
            cursor_command = item.get("cursor_command")
            codex_command = item.get("codex_command")
            has_claude_command = isinstance(claude_command, str) and bool(
                claude_command.strip()
            )
            has_vscode_command = isinstance(vscode_command, str) and bool(
                vscode_command.strip()
            )
            has_cursor_command = isinstance(cursor_command, str) and bool(
                cursor_command.strip()
            )
            has_codex_command = isinstance(codex_command, str) and bool(
                codex_command.strip()
            )
            # Codex opt-out contract: an entry may omit `codex_command` only by
            # declaring `codex_enabled: false` plus a non-empty
            # `unsupported_reason`; silent omission (and the inverse — an
            # opted-out entry that still carries a command) is contract drift.
            # Validated unconditionally: this is a contract-shape rule, not a
            # rendered-config presence check.
            unsupported_reason = item.get("unsupported_reason")
            if not has_codex_command:
                if codex_aware and (
                    item.get("codex_enabled") is not False
                    or not isinstance(unsupported_reason, str)
                    or not unsupported_reason.strip()
                ):
                    errors.append(
                        f"hook `{item['id']}` ({stage}) omits codex_command without "
                        "`codex_enabled: false` + non-empty `unsupported_reason`"
                    )
            elif item.get("codex_enabled") is False:
                errors.append(
                    f"hook `{item['id']}` ({stage}) declares `codex_enabled: false` "
                    "but still sets codex_command"
                )
            # claude_command has no opt-out analogue: every entry is
            # Claude-delivered, so a missing value is named contract drift
            # rather than an uncaught KeyError (the renderer silently skips such
            # entries — the validator must not).
            if not has_claude_command:
                errors.append(f"hook `{item['id']}` ({stage}) omits claude_command")
            # vscode_command opt-out (symmetric with codex): an entry may omit
            # `vscode_command` only by declaring `vscode_enabled: false` plus a
            # non-empty `unsupported_reason` — e.g. a Claude-only SessionStart
            # bootstrap that VS Code cannot trigger and that runs monorepo-only
            # tooling. Silent omission (and the inverse) is contract drift.
            if not has_vscode_command:
                if (
                    item.get("vscode_enabled") is not False
                    or not isinstance(unsupported_reason, str)
                    or not unsupported_reason.strip()
                ):
                    errors.append(
                        f"hook `{item['id']}` ({stage}) omits vscode_command without "
                        "`vscode_enabled: false` + non-empty `unsupported_reason`"
                    )
            elif item.get("vscode_enabled") is False:
                errors.append(
                    f"hook `{item['id']}` ({stage}) declares `vscode_enabled: false` "
                    "but still sets vscode_command"
                )
            # cursor_command opt-out, symmetric with vscode/codex. Cursor is not
            # membership-checked (no rendered cursor pairs here), but the
            # contract-shape rule still binds so the opt-out flag is enforced:
            # omit `cursor_command` only with `cursor_enabled: false` + a
            # non-empty `unsupported_reason`, and never combine the opt-out with
            # a command.
            if not has_cursor_command:
                if (
                    item.get("cursor_enabled") is not False
                    or not isinstance(unsupported_reason, str)
                    or not unsupported_reason.strip()
                ):
                    errors.append(
                        f"hook `{item['id']}` ({stage}) omits cursor_command without "
                        "`cursor_enabled: false` + non-empty `unsupported_reason`"
                    )
            elif item.get("cursor_enabled") is False:
                errors.append(
                    f"hook `{item['id']}` ({stage}) declares `cursor_enabled: false` "
                    "but still sets cursor_command"
                )
            if (
                check_claude
                and not claude_managed_mode
                and has_claude_command
                and (claude_matcher, claude_command) not in claude_pairs
            ):
                errors.append(f"missing Claude hook `{item['id']}` ({stage})")
            # VS Code / Codex / Grok configs are generated with the fail-open
            # wrapper prefix (internal); membership is checked against
            # the wrapped form. Legacy Claude settings stay verify-only on raw
            # commands; wholesale-managed settings compare the managed subtree.
            fail_mode = item.get("fail_mode")
            if (
                has_vscode_command
                and (vscode_matcher, wrap(vscode_command, fail_mode=fail_mode))
                not in vscode_pairs
            ):
                errors.append(f"missing VS Code hook `{item['id']}` ({stage})")
            if (
                check_codex
                and has_codex_command
                and (codex_matcher, wrap(codex_command, fail_mode=fail_mode))
                not in codex_pairs
            ):
                errors.append(f"missing Codex hook `{item['id']}` ({stage})")
    if check_claude and claude_managed_mode:
        errors.extend(_check_managed_claude_settings(contract, repo_root=repo_root))
    errors.extend(_check_grok_plugin_hooks(contract, grok_pairs, repo_root=repo_root))
    return errors


def _check_capture_agent_errors_grok_command(contract: dict) -> list[str]:
    """internal: ``capture-agent-errors`` must declare ``grok_command``.

    Grok delivery rides compat-loaded ``.claude/settings.json`` (implementation note); the
    plugin ``hooks.json`` renderer stays PreToolUse-only — this gate checks
    contract completeness only, not rendered Grok PostToolUse output.
    """
    errors: list[str] = []
    for item in (contract.get("hooks") or {}).get("post_tool_use") or []:
        if not isinstance(item, dict):
            continue
        if item.get("id") != "capture-agent-errors":
            continue
        grok_command = item.get("grok_command")
        if not isinstance(grok_command, str) or not grok_command.strip():
            errors.append(
                "capture-agent-errors (post_tool_use) omits grok_command"
            )
        return errors
    errors.append("capture-agent-errors missing from post_tool_use contract")
    return errors


def _check_codex_status_messages(*, repo_root: Path = REPO_ROOT) -> list[str]:
    """Require a non-empty ``statusMessage`` on every Codex command handler.

    Codex renders handlers without ``statusMessage`` as anonymous "Hook 1 /
    Hook 2" entries, so the generated ``.codex/hooks.json`` must label every
    command handler — both flat entries and nested ``PostToolUse[].hooks[]``
    handlers. Skipped when the repo has no Codex hook config.
    """
    codex_path = repo_root / ".codex" / "hooks.json"
    if not codex_path.is_file():
        return []
    errors: list[str] = []
    codex_hooks = json.loads(codex_path.read_text()).get("hooks", {})
    for stage_key, stage_entries in codex_hooks.items():
        if not isinstance(stage_entries, list):
            continue
        for entry in stage_entries:
            nested = entry.get("hooks")
            handlers = nested if isinstance(nested, list) else [entry]
            for handler in handlers:
                command = handler.get("command")
                if not isinstance(command, str):
                    continue
                message = handler.get("statusMessage")
                if not isinstance(message, str) or not message.strip():
                    errors.append(
                        f"Codex hook handler missing statusMessage: {stage_key} `{command}`"
                    )
    return errors


def _check_grok_plugin_hooks(
    contract: dict,
    grok_pairs: set[tuple[str, str]],
    *,
    repo_root: Path = REPO_ROOT,
) -> list[str]:
    """Assert emitted grok plugin PreToolUse hooks match harness-protocol rows."""
    hook_spec_rows = (contract.get("hooks") or {}).get("pre_tool_use") or []
    # Mirror the codex_aware posture: a legacy contract that pre-dates the
    # grok fields entirely (e.g. a stale consumer mirror) is skipped rather
    # than hard-failed; the live contract is pinned grok-aware by tests.
    grok_aware = any(
        isinstance(item, dict) and "grok_command" in item for item in hook_spec_rows
    )
    if not grok_aware:
        return []
    hooks_path = _candidate_path(repo_root, GROK_PLUGIN_HOOKS_PATH)
    if not hooks_path.is_file():
        return [
            "missing generated grok plugin hooks "
            f"({GROK_PLUGIN_HOOKS_PATH.as_posix()}); run make plugins-build"
        ]

    errors: list[str] = []
    wrap = _load_wrap_guard_command()
    hook_spec = contract.get("hooks", {})
    for item in hook_spec.get("pre_tool_use", []):
        if not isinstance(item, dict):
            continue
        hook_id = item.get("id", "<unknown>")
        grok_command = item.get("grok_command")
        if not isinstance(grok_command, str) or not grok_command.strip():
            errors.append(f"missing grok_command on pre_tool_use guard `{hook_id}`")
            continue
        matcher = item.get("matcher", "")
        wrapped = wrap(grok_command, fail_mode=item.get("fail_mode"))
        if (matcher, wrapped) not in grok_pairs:
            errors.append(f"missing Grok plugin hook `{hook_id}` (pre_tool_use)")
    return errors


def _check_cold_start(contract: dict, *, repo_root: Path = REPO_ROOT) -> list[str]:
    errors: list[str] = []
    steps = ((contract.get("cold_start") or {}).get("shared_steps")) or []
    if not isinstance(steps, list):
        return ["cold_start.shared_steps must be a list"]
    cache: dict[Path, str] = {}
    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            errors.append(
                f"cold_start.shared_steps[{idx}] must be a mapping with id/phrase/references"
            )
            continue
        step_id = step.get("id") or f"<index {idx}>"
        phrase = step.get("phrase")
        references = step.get("references")
        if not isinstance(phrase, str) or not phrase:
            errors.append(f"cold_start: step `{step_id}` missing non-empty `phrase`")
            continue
        if not isinstance(references, list) or not references:
            errors.append(
                f"cold_start: step `{step_id}` missing non-empty `references`"
            )
            continue
        for reference in references:
            if not isinstance(reference, str) or not reference:
                errors.append(
                    f"cold_start: step `{step_id}` has invalid reference entry"
                )
                continue
            full = repo_root / reference
            if not full.exists():
                errors.append(
                    f"cold_start: reference `{reference}` for step `{step_id}` not found"
                )
                continue
            text = cache.get(full)
            if text is None:
                text = full.read_text()
                cache[full] = text
            if phrase not in text:
                errors.append(
                    f"cold_start: `{reference}` does not contain phrase `{phrase}` for step `{step_id}`"
                )
    return errors


def _check_workspace_settings(*, repo_root: Path = REPO_ROOT) -> list[str]:
    settings_path = repo_root / VSCODE_SETTINGS_PATH.relative_to(REPO_ROOT)
    try:
        payload = json.loads(settings_path.read_text())
    except FileNotFoundError:
        return ["workspace_settings: `.vscode/settings.json` not found"]
    except (OSError, json.JSONDecodeError) as exc:
        return [f"workspace_settings: unable to load `.vscode/settings.json`: {exc}"]

    errors: list[str] = []
    missing = object()
    for key, expected in REQUIRED_VSCODE_SETTINGS.items():
        actual = payload.get(key, missing)
        if actual == expected:
            continue
        actual_repr = "<missing>" if actual is missing else repr(actual)
        errors.append(
            "workspace_settings: `.vscode/settings.json` must set "
            f"`{key}` to {expected!r} (found {actual_repr})"
        )
    return errors


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )


def _fixture_env(repo: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_paths = [repo / FIXTURE_PACKAGE_SRC, repo / FIXTURE_PROTOCOL_SRC]
    pythonpath_parts = [str(path) for path in src_paths]
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    env["CLAUDE_PROJECT_DIR"] = str(repo)
    env["WORKBAY_SKIP_ACTIVE_TASK_PROBE"] = "1"
    # Deliberately NOT pinning WORKBAY_HANDOFF_DEFAULT_AGENT here. The fixture's
    # only handoff write — the active-task seed — self-attributes with an
    # explicit actor= at its call site (see _seed_active_task). Attribution via
    # ambient global env was action-at-a-distance: it relied on the var
    # surviving env-copy + uv-run propagation, and made this subprocess one more
    # place that had to remember the pin (the same chore already duplicated
    # across four conftests). The call-site actor is hermetic on its own; do not
    # re-introduce the env pin here.
    return env


def _fixture_python_command() -> list[str]:
    uv_bin = shutil.which("uv")
    if uv_bin:
        return [
            uv_bin,
            "run",
            "--project",
            str(PACKAGES_ROOT / "mcp-workbay-handoff"),
            "python",
        ]
    return [sys.executable]


def _render_contract_yaml(contract: dict) -> str:
    branch_isolation = contract.get("branch_isolation") or {}
    lines = ["version: 1", "", "branch_isolation:"]

    def _append_string_list(key: str) -> None:
        lines.append(f"  {key}:")
        for value in branch_isolation.get(key, []):
            lines.append(f"    - {value}")

    for key in (
        "protected_branches",
        "code_roots",
        "protected_extensions",
        "root_protected_files",
    ):
        _append_string_list(key)

    lines.append("  protected_main_surfaces:")
    for entry in branch_isolation.get("protected_main_surfaces", []):
        lines.append(f"    - pattern: {entry['pattern']!r}")
        lines.append(f"      reason: {entry['reason']!r}")

    lines.append("  permitted_main_surfaces:")
    for entry in branch_isolation.get("permitted_main_surfaces", []):
        lines.append(f"    - pattern: {entry['pattern']!r}")
        lines.append(f"      reason: {entry['reason']!r}")

    lines.append("  enforcers:")
    for entry in branch_isolation.get("enforcers", []):
        lines.append(f"    - path: {entry['path']}")
        lines.append(f"      harness: {entry['harness']}")

    return "\n".join(lines) + "\n"


def _build_guard_fixture(
    contract: dict, *, repo_root: Path
) -> tuple[tempfile.TemporaryDirectory[str], Path]:
    tmpdir = tempfile.TemporaryDirectory(prefix="check-harness-sync-")
    repo = Path(tmpdir.name) / "repo"
    repo.mkdir()

    for relative in FIXTURE_COPY_FILES:
        source = _candidate_path(repo_root, relative)
        destination = repo / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        if destination.suffix == ".sh":
            destination.chmod(0o755)

    package_src = repo / FIXTURE_PACKAGE_SRC
    package_src.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PACKAGES_ROOT / "mcp-workbay-handoff" / "src", package_src)

    protocol_src = repo / FIXTURE_PROTOCOL_SRC
    protocol_src.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PACKAGES_ROOT / "workbay-protocol" / "src", protocol_src)

    contract_path = repo / CONTRACT_RELATIVE
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(_render_contract_yaml(contract), encoding="utf-8")

    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    return tmpdir, repo


def _run_python_hook(
    script: Path, payload: dict, *, cwd: Path, env: dict[str, str]
) -> tuple[int, dict | None, str]:
    proc = subprocess.run(
        [*_fixture_python_command(), str(script)],
        cwd=cwd,
        env=env,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=10,
    )
    stdout_json = json.loads(proc.stdout) if proc.stdout.strip() else None
    return proc.returncode, stdout_json, proc.stderr


def _run_shell_hook(
    script: Path, payload: dict, *, cwd: Path, env: dict[str, str]
) -> tuple[int, dict | None, str]:
    proc = subprocess.run(
        [str(script)],
        cwd=cwd,
        env=env,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=10,
    )
    stdout_json = json.loads(proc.stdout) if proc.stdout.strip() else None
    return proc.returncode, stdout_json, proc.stderr


def _main_guard_paths(contract: dict) -> tuple[str, str]:
    spec = contract.get("branch_isolation")
    if not isinstance(spec, dict):
        raise ValueError("branch_isolation must be a mapping")
    enforcers = spec.get("enforcers") or []
    if not isinstance(enforcers, list):
        raise ValueError("branch_isolation.enforcers must be a list")

    vscode_path = next(
        (
            item.get("path")
            for item in enforcers
            if isinstance(item, dict)
            and item.get("harness") == "vscode"
            and isinstance(item.get("path"), str)
        ),
        None,
    )
    claude_path = next(
        (
            item.get("path")
            for item in enforcers
            if isinstance(item, dict)
            and item.get("harness") == "claude"
            and isinstance(item.get("path"), str)
        ),
        None,
    )
    if not isinstance(vscode_path, str) or not isinstance(claude_path, str):
        raise ValueError(
            "branch_isolation.enforcers must include vscode and claude guard paths"
        )
    return vscode_path, claude_path


def _first_protected_extension(spec: dict) -> str:
    extensions = spec.get("protected_extensions") or []
    if not isinstance(extensions, list) or not extensions:
        raise ValueError(
            "branch_isolation.protected_extensions must be a non-empty list"
        )
    extension = extensions[0]
    if not isinstance(extension, str) or not extension.startswith("."):
        raise ValueError(
            "branch_isolation.protected_extensions entries must be dot-prefixed strings"
        )
    return extension


def _path_for_code_root(code_root: str, extension: str) -> Path:
    return Path(code_root.rstrip("/")) / f"fixture{extension}"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _validate_surface_patterns(
    spec: dict, *, key: str, sample_paths: tuple[str, ...]
) -> list[str]:
    errors: list[str] = []
    entries = spec.get(key) or []
    if not isinstance(entries, list) or not entries:
        return [f"branch_isolation.{key} must be a non-empty list"]

    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            errors.append(f"branch_isolation.{key}[{idx}] must be a mapping")
            continue
        pattern = entry.get("pattern")
        reason = entry.get("reason")
        if not isinstance(pattern, str) or not pattern:
            errors.append(f"branch_isolation.{key}[{idx}] missing non-empty `pattern`")
            continue
        if not isinstance(reason, str) or not reason:
            errors.append(f"branch_isolation.{key}[{idx}] missing non-empty `reason`")
        try:
            for sample in sample_paths:
                Path(sample).match(pattern)
        except (re.error, ValueError) as exc:
            errors.append(
                f"branch_isolation.{key}[{idx}] has invalid glob `{pattern}`: {exc}"
            )
    return errors


def _pattern_targets_planning_surface(pattern: str) -> bool:
    if pattern.startswith("docs/tasks/archive/") or pattern == "docs/tasks/archive/**":
        return False
    return any(
        pattern.startswith(prefix)
        for prefix in (
            "docs/tasks/",
            "docs/assessments/",
            "docs/scopes/",
            "docs/epics/",
            "docs/specs/",
            "docs/adrs/",
            "packages/*/docs/tasks/",
            "packages/*/docs/assessments/",
            "packages/*/docs/specs/",
            "packages/*/docs/epics/",
            "packages/*/docs/adrs/",
        )
    )


def _check_branch_isolation(
    contract: dict, *, repo_root: Path = REPO_ROOT
) -> list[str]:
    errors: list[str] = []
    spec = contract.get("branch_isolation")
    if not isinstance(spec, dict):
        return ["branch_isolation must be a mapping"]
    protected_branches = spec.get("protected_branches") or []
    code_roots = spec.get("code_roots") or []
    root_protected_files = spec.get("root_protected_files") or []
    enforcers = spec.get("enforcers") or []
    if not isinstance(enforcers, list) or not enforcers:
        return ["branch_isolation.enforcers must be a non-empty list"]
    errors.extend(
        _validate_surface_patterns(
            spec,
            key="protected_main_surfaces",
            sample_paths=(
                "docs/tasks/17.0/sample.md",
                "docs/assessments/sample.md",
                "docs/specs/sample.md",
                "packages/foo/docs/tasks/sample.md",
            ),
        )
    )
    errors.extend(
        _validate_surface_patterns(
            spec,
            key="permitted_main_surfaces",
            sample_paths=(
                "CLAUDE.md",
                ".github/copilot-instructions.md",
                "docs/workbay/contracts/harness-protocol.yaml",
                "DASHBOARD.txt",
            ),
        )
    )
    protected_patterns = {
        entry.get("pattern")
        for entry in (spec.get("protected_main_surfaces") or [])
        if isinstance(entry, dict) and isinstance(entry.get("pattern"), str)
    }
    for pattern in REQUIRED_PROTECTED_MAIN_PATTERNS:
        if pattern not in protected_patterns:
            errors.append(
                f"branch_isolation.protected_main_surfaces is missing required planning pattern `{pattern}`"
            )
    for idx, entry in enumerate(spec.get("permitted_main_surfaces") or []):
        if not isinstance(entry, dict):
            continue
        pattern = entry.get("pattern")
        if isinstance(pattern, str) and _pattern_targets_planning_surface(pattern):
            errors.append(
                "branch_isolation.permitted_main_surfaces "
                f"must not include planning pattern `{pattern}` (entry {idx})"
            )
    claude_payload: dict | None = None
    claude_settings_path = repo_root / ".claude" / "settings.json"
    if claude_settings_path.is_file():
        try:
            claude_payload = json.loads(claude_settings_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(
                f"branch_isolation: unable to load `.claude/settings.json`: {exc}"
            )
            return errors
    try:
        vscode_guard_config = _candidate_path(
            repo_root, Path(".github/hooks/terminal-guard.json")
        )
        vscode_payload = json.loads(vscode_guard_config.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(
            f"branch_isolation: unable to load `.github/hooks/terminal-guard.json`: {exc}"
        )
        return errors
    if claude_payload is not None:
        claude_main_guard_entry = next(
            (
                entry
                for entry in claude_payload.get("hooks", {}).get("PreToolUse", [])
                if any(
                    isinstance(hook, dict)
                    and hook.get("command")
                    == 'bash "$CLAUDE_PROJECT_DIR/scripts/hooks/guard-main-branch.sh"'
                    for hook in entry.get("hooks", [])
                )
            ),
            None,
        )
        if claude_main_guard_entry is None:
            errors.append(
                "branch_isolation: Claude PreToolUse missing `guard-main-branch.sh` entry"
            )
        elif claude_main_guard_entry.get("matcher") != EDIT_TOOL_MATCHER:
            errors.append(
                "branch_isolation: Claude `guard-main-branch.sh` entry must scope to "
                f"`{EDIT_TOOL_MATCHER}`"
            )
    # The generated VS Code config carries the internal fail-open
    # wrapper prefix; expectation is derived through the same transform.
    expected_vscode_main_guard = _load_wrap_guard_command()(
        "python3 .github/hooks/guard-main-branch.py"
    )
    main_guard_entry = next(
        (
            entry
            for entry in vscode_payload.get("hooks", {}).get("PreToolUse", [])
            if entry.get("command") == expected_vscode_main_guard
        ),
        None,
    )
    if main_guard_entry is None:
        errors.append(
            "branch_isolation: VS Code PreToolUse missing `guard-main-branch.py` entry"
        )
    elif main_guard_entry.get("matcher") != EDIT_TOOL_MATCHER:
        errors.append(
            "branch_isolation: VS Code `guard-main-branch.py` entry must scope to "
            f"`{EDIT_TOOL_MATCHER}`"
        )
    helper_path = _candidate_path(repo_root, Path("scripts/hooks/_harness_protocol.py"))
    if not helper_path.exists():
        return [
            "branch_isolation: shared contract loader `scripts/hooks/_harness_protocol.py` not found"
        ]
    for idx, enforcer in enumerate(enforcers):
        if not isinstance(enforcer, dict):
            errors.append(
                f"branch_isolation.enforcers[{idx}] must be a mapping with a `path`"
            )
            continue
        path = enforcer.get("path")
        harness = enforcer.get("harness") or "?"
        if not isinstance(path, str) or not path:
            errors.append(f"branch_isolation.enforcers[{idx}] missing non-empty `path`")
            continue
        full = _candidate_path(repo_root, Path(path))
        if not full.exists():
            errors.append(f"branch_isolation: enforcer `{path}` ({harness}) not found")
            continue
        text = full.read_text()
        # internal delegation: if the enforcer delegates to a companion
        # inline Python file, include that file's text when checking for
        # policy-loading evidence.
        companion_dir = full.parent
        for companion_name in ("_guard_main_branch_inline.py",):
            companion = companion_dir / companion_name
            if companion.exists() and companion_name in text:
                text += "\n" + companion.read_text()
        for branch in protected_branches:
            if not isinstance(branch, str):
                errors.append(
                    f"branch_isolation: protected_branches entries must be strings"
                )
                continue
            if branch not in text:
                errors.append(
                    f"branch_isolation: `{path}` ({harness}) does not reference protected branch `{branch}`"
                )
        if (
            "_harness_protocol" not in text
            or "load_branch_isolation_policy" not in text
        ):
            errors.append(
                f"branch_isolation: `{path}` ({harness}) does not load policy from `scripts/hooks/_harness_protocol.py`"
            )

    if errors:
        return errors

    try:
        vscode_path, claude_path = _main_guard_paths(contract)
        extension = _first_protected_extension(spec)
    except ValueError as exc:
        return [f"branch_isolation: {exc}"]

    tmpdir, fixture_repo = _build_guard_fixture(contract, repo_root=repo_root)
    try:
        env = _fixture_env(fixture_repo)
        vscode_guard = fixture_repo / vscode_path
        claude_guard = fixture_repo / claude_path

        for root in code_roots:
            if not isinstance(root, str) or not root:
                errors.append(
                    "branch_isolation: code_roots entries must be non-empty strings"
                )
                continue
            rel_path = _path_for_code_root(root, extension)
            absolute_path = fixture_repo / rel_path
            _ensure_parent(absolute_path)

            _, output, stderr = _run_python_hook(
                vscode_guard,
                {
                    "toolName": "create_file",
                    "toolInput": {"filePath": str(absolute_path)},
                },
                cwd=fixture_repo,
                env=env,
            )
            if (
                output is None
                or output["hookSpecificOutput"]["permissionDecision"] != "block"
            ):
                errors.append(
                    f"branch_isolation: VS Code guard did not block code_root `{root}`"
                )

            shell_code, _, shell_stderr = _run_shell_hook(
                claude_guard,
                {"tool_input": {"file_path": str(absolute_path)}},
                cwd=fixture_repo,
                env=env,
            )
            if shell_code != 2 or "BLOCKED" not in shell_stderr:
                errors.append(
                    f"branch_isolation: Claude guard did not block code_root `{root}`"
                )

        for root_file in root_protected_files:
            if not isinstance(root_file, str) or not root_file:
                errors.append(
                    "branch_isolation: root_protected_files entries must be non-empty strings"
                )
                continue
            absolute_path = fixture_repo / root_file
            _ensure_parent(absolute_path)
            _, output, _ = _run_python_hook(
                vscode_guard,
                {
                    "toolName": "replace_string_in_file",
                    "toolInput": {"filePath": str(absolute_path)},
                },
                cwd=fixture_repo,
                env=env,
            )
            if (
                output is None
                or output["hookSpecificOutput"]["permissionDecision"] != "block"
            ):
                errors.append(
                    f"branch_isolation: VS Code guard did not block root protected file `{root_file}`"
                )

            shell_code, _, shell_stderr = _run_shell_hook(
                claude_guard,
                {"tool_input": {"file_path": str(absolute_path)}},
                cwd=fixture_repo,
                env=env,
            )
            if shell_code != 2 or "BLOCKED" not in shell_stderr:
                errors.append(
                    f"branch_isolation: Claude guard did not block root protected file `{root_file}`"
                )

        planning_path = (
            fixture_repo / "docs" / "tasks" / "12.0" / "fixture-task-plan.md"
        )
        _ensure_parent(planning_path)
        _, output, _ = _run_python_hook(
            vscode_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(planning_path)}},
            cwd=fixture_repo,
            env=env,
        )
        if (
            output is None
            or output["hookSpecificOutput"]["permissionDecision"] != "block"
        ):
            errors.append(
                "branch_isolation: VS Code guard did not block protected planning docs on main"
            )

        shell_code, _, shell_stderr = _run_shell_hook(
            claude_guard,
            {"tool_input": {"file_path": str(planning_path)}},
            cwd=fixture_repo,
            env=env,
        )
        if shell_code != 2 or "BLOCKED" not in shell_stderr:
            errors.append(
                "branch_isolation: Claude guard did not block protected planning docs on main"
            )

        allowed_path = fixture_repo / "CLAUDE.md"
        _ensure_parent(allowed_path)
        _, output, _ = _run_python_hook(
            vscode_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(allowed_path)}},
            cwd=fixture_repo,
            env=env,
        )
        if output is not None:
            errors.append(
                "branch_isolation: VS Code guard blocked allow-listed operator doc `CLAUDE.md`"
            )

        shell_code, _, _ = _run_shell_hook(
            claude_guard,
            {"tool_input": {"file_path": str(allowed_path)}},
            cwd=fixture_repo,
            env=env,
        )
        if shell_code != 0:
            errors.append(
                "branch_isolation: Claude guard blocked allow-listed operator doc `CLAUDE.md`"
            )

        dirty_code_path = fixture_repo / _path_for_code_root(code_roots[0], extension)
        _ensure_parent(dirty_code_path)
        dirty_code_path.write_text("print('dirty main')\n", encoding="utf-8")
        _, output, _ = _run_python_hook(
            vscode_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(allowed_path)}},
            cwd=fixture_repo,
            env=env,
        )
        if output is not None:
            errors.append(
                "branch_isolation: VS Code guard blocked allow-listed operator doc when unrelated protected paths were dirty on main"
            )

        shell_code, _, shell_stderr = _run_shell_hook(
            claude_guard,
            {"tool_input": {"file_path": str(allowed_path)}},
            cwd=fixture_repo,
            env=env,
        )
        if shell_code != 0:
            errors.append(
                "branch_isolation: Claude guard blocked allow-listed operator doc when unrelated protected paths were dirty on main"
            )

        contract_path = fixture_repo / CONTRACT_RELATIVE
        contract_backup = fixture_repo / CONTRACT_RELATIVE.with_suffix(".yaml.bak")
        contract_path.rename(contract_backup)
        missing_target = fixture_repo / "scripts" / f"missing{extension}"
        _ensure_parent(missing_target)
        _, output, _ = _run_python_hook(
            vscode_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(missing_target)}},
            cwd=fixture_repo,
            env=env,
        )
        if (
            output is None
            or output["hookSpecificOutput"]["permissionDecision"] != "block"
        ):
            errors.append(
                "branch_isolation: VS Code guard did not block when harness-protocol.yaml was missing"
            )
        elif (
            "HarnessContractMissingError"
            not in output["hookSpecificOutput"]["permissionDecisionReason"]
        ):
            errors.append(
                "branch_isolation: VS Code guard missing named HarnessContractMissingError reason"
            )

        shell_code, _, shell_stderr = _run_shell_hook(
            claude_guard,
            {"tool_input": {"file_path": str(missing_target)}},
            cwd=fixture_repo,
            env=env,
        )
        if shell_code != 2:
            errors.append(
                "branch_isolation: Claude guard did not block when harness-protocol.yaml was missing"
            )
        elif "HarnessContractMissingError" not in shell_stderr:
            errors.append(
                "branch_isolation: Claude guard missing named HarnessContractMissingError stderr"
            )
        contract_backup.rename(contract_path)
    finally:
        tmpdir.cleanup()
    return errors


def _seed_active_task(
    repo: Path, *, task_ref: str, branch: str, target_worktree_path: Path
) -> None:
    env = _fixture_env(repo)
    proc = subprocess.run(
        [
            *_fixture_python_command(),
            "-c",
            (
                "import json; "
                "from pathlib import Path; "
                "from workbay_handoff_mcp import RuntimeConfig, configure_runtime, get_handoff_state, set_handoff_state; "
                f"configure_runtime(RuntimeConfig.for_repo(Path({str(repo)!r}))); "
                "identity = get_handoff_state(sections='identity'); "
                "parsed = json.loads(identity) if isinstance(identity, str) else identity; "
                "data = parsed.get('data') if isinstance(parsed, dict) else None; "
                "active = data.get('active') if isinstance(data, dict) else None; "
                "expected_revision = active.get('revision') if isinstance(active, dict) else None; "
                f"set_handoff_state(task_ref={task_ref!r}, objective='fixture', status='in_progress', "
                f"target_branch={branch!r}, target_worktree_path={str(target_worktree_path)!r}, "
                "expected_revision=expected_revision, "
                f"actor={{'agent': {_FIXTURE_WRITE_AGENT!r}}})"
            ),
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "failed to seed active task fixture: "
            f"stdout={proc.stdout.strip()!r} stderr={proc.stderr.strip()!r}"
        )


def _create_worktree(repo: Path, *, branch: str, suffix: str) -> Path:
    worktree_path = repo.parent / f"repo-{suffix}"
    _git(repo, "branch", branch, "main")
    _git(repo, "worktree", "add", str(worktree_path), branch)
    return worktree_path


def _check_worktree_drift(contract: dict, *, repo_root: Path = REPO_ROOT) -> list[str]:
    errors: list[str] = []
    tmpdir, fixture_repo = _build_guard_fixture(contract, repo_root=repo_root)
    try:
        env = _fixture_env(fixture_repo)
        feature_worktree = _create_worktree(
            fixture_repo, branch="feature/e17-8", suffix="feature"
        )
        feature_guard = (
            feature_worktree / ".github" / "hooks" / "guard-worktree-drift.py"
        )
        _seed_active_task(
            fixture_repo,
            task_ref="internal",
            branch="feature/e17-8",
            target_worktree_path=feature_worktree,
        )

        blocked_path = fixture_repo / "scripts" / "check.py"
        _ensure_parent(blocked_path)
        code, output, stderr = _run_python_hook(
            feature_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(blocked_path)}},
            cwd=feature_worktree,
            env=env,
        )
        if (
            output is None
            or output["hookSpecificOutput"]["permissionDecision"] != "block"
        ):
            errors.append(
                "worktree_drift: drift hook did not block a main-worktree code edit "
                f"(exit={code}, stderr={stderr.strip()!r}, output={output!r})"
            )
        elif (
            "WorkspaceRootDriftError"
            not in output["hookSpecificOutput"]["permissionDecisionReason"]
        ):
            errors.append(
                "worktree_drift: block reason did not name WorkspaceRootDriftError"
            )

        allowed_path = fixture_repo / "CLAUDE.md"
        _ensure_parent(allowed_path)
        _, output, _ = _run_python_hook(
            feature_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(allowed_path)}},
            cwd=feature_worktree,
            env=env,
        )
        if output is not None:
            errors.append(
                "worktree_drift: drift hook blocked an allow-listed operator doc"
            )

        maint_worktree = _create_worktree(
            fixture_repo,
            branch="feature/maint-dashboard",
            suffix="maint",
        )
        maint_guard = maint_worktree / ".github" / "hooks" / "guard-worktree-drift.py"
        _seed_active_task(
            fixture_repo,
            task_ref="MAINT-dashboard",
            branch="feature/maint-dashboard",
            target_worktree_path=maint_worktree,
        )
        _, output, _ = _run_python_hook(
            maint_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(blocked_path)}},
            cwd=maint_worktree,
            env=env,
        )
        if output is not None:
            errors.append("worktree_drift: drift hook did not honor the MAINT-* bypass")

        env_bypass = env | {"ALT_ALLOW_WORKTREE_DRIFT": "1"}
        _, output, _ = _run_python_hook(
            feature_guard,
            {"toolName": "create_file", "toolInput": {"filePath": str(blocked_path)}},
            cwd=feature_worktree,
            env=env_bypass,
        )
        if output is not None:
            errors.append(
                "worktree_drift: drift hook did not honor ALT_ALLOW_WORKTREE_DRIFT=1"
            )
    finally:
        tmpdir.cleanup()
    return errors


_DASHBOARD_ALLOW_MARKER = "<!-- lint-dashboard-txt: allow -->"
_DASHBOARD_EXTRA_FILES = (
    Path("CLAUDE.md"),
    Path(".github/copilot-instructions.md"),
    Path("Makefile"),
    Path(
        "packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/dashboard_extension.py"
    ),
)


def _iter_dashboard_lint_files(repo_root: Path) -> list[Path]:
    def _should_skip(rel: str) -> bool:
        if rel.startswith("docs/tasks/archive/"):
            return True
        if rel.startswith("docs/assessments/dashboard-md-vs-txt-"):
            return True
        if "/test_fixtures/" in rel or rel.startswith("test_fixtures/"):
            return True
        parts = rel.split("/")
        for i, part in enumerate(parts):
            if part == "tests" and i + 1 < len(parts) and parts[i + 1] == "fixtures":
                return True
        return False

    targets: list[Path] = []
    for relative in _DASHBOARD_EXTRA_FILES:
        candidate = repo_root / relative
        if candidate.exists():
            targets.append(candidate)

    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "ls-files", "-z", "--", "*.md"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return targets

    seen = {p.resolve() for p in targets}
    for rel in proc.stdout.split("\0"):
        if not rel or not rel.endswith(".md") or _should_skip(rel):
            continue
        path = repo_root / rel
        if not path.exists():
            continue
        resolved = path.resolve()
        if resolved in seen:
            continue
        targets.append(path)
        seen.add(resolved)
    return targets


def _check_dashboard_naming(*, repo_root: Path = REPO_ROOT) -> list[str]:
    errors: list[str] = []
    gitignore_path = repo_root / ".gitignore"
    if gitignore_path.exists() and "DASHBOARD.md" not in gitignore_path.read_text():
        errors.append(".gitignore is missing the `DASHBOARD.md` exclusion")

    for path in _iter_dashboard_lint_files(repo_root):
        try:
            lines = path.read_text().splitlines()
        except OSError:
            continue
        for lineno, line in enumerate(lines, start=1):
            if "DASHBOARD.md" in line and _DASHBOARD_ALLOW_MARKER not in line:
                rel = path.relative_to(repo_root)
                errors.append(
                    f"dashboard_naming: stale `DASHBOARD.md` reference in `{rel}:{lineno}`"
                )
    return errors


def _check_python_api_surface(contract: dict) -> list[str]:
    exports = _load_python_exports()
    required = set(contract.get("python_api_fallback", {}).get("required_exports", []))
    missing = sorted(required - exports)
    return [f"missing workbay_handoff_mcp export `{name}`" for name in missing]


def _check_compaction_contract(contract: dict) -> list[str]:
    compaction = contract.get("compaction")
    if not isinstance(compaction, dict):
        return ["compaction must be a mapping"]

    errors: list[str] = []

    advisory_field = compaction.get("advisory_field")
    if advisory_field != "compaction_recommended":
        errors.append("compaction.advisory_field must be `compaction_recommended`")

    for key in ("threshold_tokens", "threshold_chars"):
        value = compaction.get(key)
        if not isinstance(value, int) or value <= 0:
            errors.append(f"compaction.{key} must be a positive integer")

    if compaction.get("unknown_harness") != "warn_and_skip":
        errors.append("compaction.unknown_harness must be `warn_and_skip`")

    transcript_discovery = compaction.get("transcript_discovery")
    if not isinstance(transcript_discovery, dict):
        errors.append("compaction.transcript_discovery must be a mapping")
        return errors

    expected_harnesses = {"claude-code", "codex", "grok", "vscode"}
    actual_harnesses = set(transcript_discovery)
    if actual_harnesses != expected_harnesses:
        errors.append(
            "compaction.transcript_discovery must define exactly "
            "`claude-code`, `codex`, and `vscode`"
        )

    for harness in sorted(expected_harnesses & actual_harnesses):
        rule = transcript_discovery.get(harness)
        if not isinstance(rule, dict):
            errors.append(
                f"compaction.transcript_discovery.{harness} must be a mapping"
            )
            continue
        for key in ("env_var", "fallback_glob"):
            value = rule.get(key)
            if not isinstance(value, str) or not value.strip():
                errors.append(
                    f"compaction.transcript_discovery.{harness}.{key} must be a non-empty string"
                )

    return errors


def run_checks(
    contract: dict, *, check_api_surface: bool = False, repo_root: Path = REPO_ROOT
) -> list[str]:
    errors: list[str] = []
    errors.extend(_check_hooks(contract, repo_root=repo_root))
    # internal: real-contract completeness gate. Lives here (not in
    # _check_hooks) so fixture/legacy contracts passed to _check_hooks by
    # tests are not forced to carry the capture-agent-errors entry.
    errors.extend(_check_capture_agent_errors_grok_command(contract))
    errors.extend(_check_codex_status_messages(repo_root=repo_root))
    errors.extend(_check_cold_start(contract, repo_root=repo_root))
    errors.extend(_check_compaction_contract(contract))
    errors.extend(_check_workspace_settings(repo_root=repo_root))
    errors.extend(_check_branch_isolation(contract, repo_root=repo_root))
    errors.extend(_check_worktree_drift(contract, repo_root=repo_root))
    errors.extend(_check_dashboard_naming(repo_root=repo_root))
    if check_api_surface:
        errors.extend(_check_python_api_surface(contract))
    return errors


def main(argv: list[str]) -> int:
    check_api_surface = "--check-api-surface" in argv
    if _YAML_IMPORT_ERROR is not None:
        print(
            "check-harness-sync: infrastructure error: PyYAML is required to load harness contracts",
            file=sys.stderr,
        )
        return 1
    try:
        contract = _load_contract()
        errors = run_checks(contract, check_api_surface=check_api_surface)
        if errors:
            print("check-harness-sync: FAILED", file=sys.stderr)
            for error in errors:
                print(f"  - {error}", file=sys.stderr)
            return 1
        print(_format_success_message())
        return 0
    except (OverlayResolverError, ValueError, yaml.YAMLError) as exc:
        print(f"check-harness-sync: infrastructure error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
