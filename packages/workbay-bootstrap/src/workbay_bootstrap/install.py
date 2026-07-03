"""Minimal install flow for the workbay-bootstrap CLI.

This slice implements four responsibilities:

1. Clone (or fast-forward) ``<remote_url>`` at ``<remote_ref>`` into
   ``<target>/.workbay/remote/``.
2. Symlink the six known shared overlay surfaces from the clone into the
   consumer repo, preserving any pre-existing real local directory at the
   same path (overlay precedence: local wins per surface).
3. When ``mcp_servers`` is provided, configure the three consumer-tool
   surfaces — ``.mcp.json`` (Claude Code), ``.vscode/mcp.json`` (VS Code),
   and ``.codex/config.toml`` (Codex CLI) — by deep-merging or
   tomlkit-replacing only the managed entries while preserving everything
   else the user had configured.
4. When ``<target>`` is a git repo, point ``core.hooksPath`` at the
   materialized ``scripts/hooks/git`` directory so git resolves shared
   hooks by name (``post-checkout``, ``pre-commit``, ``pre-push`` …).
   The parent ``scripts/hooks/`` symlink ships Python helpers and other
   non-git-hook files; setting ``core.hooksPath`` there makes git
    silently resolve nothing; the bootstrap-managed git hook directory is the
    only valid hooksPath target.
5. Write ``<target>/.workbay-bootstrap.json`` describing the resolved remote,
   the materialized surfaces, and the configs that were touched. Older
   installs wrote ``.workbay-overlay.json``; the legacy file is migrated
   in-place on first run when present.

The ``doctor`` / ``repair`` / ``update`` / ``status`` subcommands are
implemented in adjacent modules and are deliberately out of scope here.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from importlib import metadata as importlib_metadata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Iterable
from typing import Any, Mapping

import tomlkit
import yaml

from workbay_bootstrap._mcp_pins import (
    DEFAULT_MCP_SERVERS as DEFAULT_MCP_SERVERS,
)
from workbay_bootstrap.surfaces import (
    overlay_clone_homes,
    path_resolves_under,
)
from workbay_bootstrap.fsutil import (
    # internal (RF29-S3-01): canonical home is fsutil; legacy
    # private aliases retained for install.py-internal call sites and tests.
    deep_merge as _deep_merge,
    write_json_file as _write_json_file,
)
from workbay_bootstrap._mcp_pins import (
    MCP_REGISTRATION as MCP_REGISTRATION,
)
from workbay_protocol import (
    BOOTSTRAP_MANIFEST_NAME,
    CONTRACTS_DIR,
    LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME,
    LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME,
    MANIFEST_NAME_PRECEDENCE,
    REPO_HTTPS_URL,
    RULES_DIR,
    TEMPLATES_DIR,
    RUNTIME_ROOT_DIRNAME,
)

# Manifest-name constants + precedence are the single source of truth in
# workbay_protocol.paths (shared with mcp-workbay-handoff's state_init). They are
# re-exported here because many workbay_bootstrap modules import them from
# install.py (install_receipt, mcp_sync, worktree, coherence, subcommands, ...).
LEGACY_AGENTIC_OVERLAY_REMEDIATION = (
    "legacy agentic-system overlay detected at {target} ({reason}); "
    "remove legacy .agentic* artifacts and re-run workbay-bootstrap install — "
    f"see {REPO_HTTPS_URL}/blob/main/docs/CONSUMER.md"
)
# Deprecated alias kept for downstream code importing the old name. Points
# at the canonical (new) filename, NOT the legacy file. Reading legacy
# installs goes through _migrate_legacy_manifest below.
OVERLAY_MANIFEST_NAME = BOOTSTRAP_MANIFEST_NAME
SCHEMA_VERSION = 2
CLONE_SUBDIR = (RUNTIME_ROOT_DIRNAME, "remote")


def _build_install_manifest(
    *,
    remote_url: str | None = None,
    remote_ref: str | None = None,
    remote_sha: str | None = None,
    source_kind: str = "git_overlay",
    package_version: str | None = None,
    stack_distribution: str | None = None,
    stack_version: str | None = None,
    stack_members: dict[str, str] | None = None,
    profile: str,
    surfaces: list[dict[str, str]],
    configs: list[dict[str, str]],
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
    plugin_overrides_path: str | None,
) -> dict[str, object]:
    """Build the dict that will be written to ``.workbay-bootstrap.json``.

    ``mcp_servers`` is the mapping that ``install()`` actually used to
    write the harness config surfaces. Persisting the sorted key list as
    ``manifest["mcp_servers"]`` gives ``sync_mcp_configs(prune_removed_managed=True)``
    an authoritative previously-managed provenance: any name in this
    list that disappears from the new managed set is a removal that
    sync may prune from the surface files; everything else is treated
    as third-party and left untouched.
    """
    manifest: dict[str, object] = {"schema_version": SCHEMA_VERSION}
    if source_kind == "package":
        manifest["source_kind"] = "package"
        manifest["package_version"] = package_version
        # Stack provenance (implementation note): recorded only when the workbay
        # anchor is installed; legacy consumers keep the pre-stack shape.
        if stack_distribution is not None:
            manifest["stack_distribution"] = stack_distribution
            manifest["stack_version"] = stack_version
            manifest["stack_members"] = stack_members or {}
    elif source_kind == "worktree":
        manifest["source_kind"] = "worktree"
        manifest["remote_sha"] = remote_sha
    else:
        # git_overlay output stays byte-identical to pre-internal:
        # source_kind is omitted (BootstrapManifest defaults it) so existing
        # manifests and tests are unaffected.
        manifest["remote_url"] = remote_url
        manifest["remote_ref"] = remote_ref
        manifest["remote_sha"] = remote_sha
    manifest["profile"] = profile
    manifest["surfaces"] = surfaces
    manifest["configs"] = configs
    manifest["mcp_servers"] = sorted(mcp_servers) if mcp_servers else []
    if plugin_overrides_path is not None:
        manifest["plugin_overrides_path"] = plugin_overrides_path
    return manifest


def _finalize_install_manifest(
    target: Path,
    manifest: dict[str, object],
    *,
    override_backup_path: str | None = None,
    state_backup_path: str | None = None,
) -> dict[str, object]:
    """Validate the manifest against the protocol shape, write it, and return
    the install result. Shared by the git-overlay and package install paths.

    When workbay-protocol is not installed (partial migrations), validation
    is skipped — the manifest contract is best-effort until the protocol is
    mandatory.
    """
    try:
        from workbay_protocol import BootstrapManifest  # type: ignore[import-not-found]

        BootstrapManifest.model_validate(manifest)
    except ImportError:
        pass
    except Exception as exc:  # noqa: BLE001
        raise BootstrapManifestValidationError(
            f"refusing to write {BOOTSTRAP_MANIFEST_NAME}: workbay_protocol.BootstrapManifest "
            f"validation failed: {exc}"
        ) from exc

    manifest_path = target / BOOTSTRAP_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    result = dict(manifest)
    if override_backup_path is not None:
        result["override_backup_path"] = override_backup_path
    if state_backup_path is not None:
        result["state_backup_path"] = state_backup_path
    return result


# In the workbay, the shared workbay-system surfaces live
# under packages/workbay-system/ rather than at the clone root. We probe this
# subdirectory first when resolving a surface in the clone, and fall back to
# the clone root for legacy/hoisted overlay layouts (and for the
# fake_remote_with_surfaces fixture used elsewhere in the test suite).
WORKBAY_SYSTEM_SUBDIR = "packages/workbay-system"

# implementation note S3: the shipped overlay payload moved under
# packages/workbay-system/workbay_system/payload/. Probe this co-located
# payload root FIRST when resolving a surface in the clone, keeping the pre-S3
# subdir and the clone root as fallbacks for already-installed / hoisted
# consumers (and the fake_remote_with_surfaces test fixture).
WORKBAY_SYSTEM_PAYLOAD_SUBDIR = "packages/workbay-system/workbay_system/payload"

# Shared overlay surfaces materialized as symlinks into ``.workbay/remote``.
# Per-agent surfaces (.claude/skills, .claude/commands, .github/prompts,
# .codex/skills) are no longer canonical in the overlay clone; they are
# generated artifacts produced by generate_agent_workflows.py during install.
# Only the truly shared surfaces remain symlinked here.
SHARED_SURFACES: tuple[str, ...] = (
    ".github/hooks",
    "scripts/hooks",
    CONTRACTS_DIR,
    # Hoist canonical rule docs (development-workflow.md,
    # branch-review-guide.md, planning-artifact-home.md) to consumers.
    # Prior to this entry rule docs sat repo-local even though hooks and
    # skills cite them by path; only contracts propagated.
    RULES_DIR,
    TEMPLATES_DIR,
    # Plan-targets surface and the optional git plan-cat shell wrapper.
    # Hoisted as directory symlinks so consumers
    # inherit Makefile.d/plans.mk and scripts/workbay/git-plan-cat.sh
    # (and any sibling files added in later slices) without re-running
    # bootstrap on every file addition. The Python logic the wrappers
    # invoke lives in the workbay_handoff_mcp package, fetched on demand
    # by uvx — no Python module is hoisted via overlay.
    "Makefile.d",
    "scripts/workbay",
    # internal: the Codex hook config is a generated golden in the
    # workbay-system payload (statusMessage-labelled handlers rendered from
    # harness-protocol.yaml). Shipped as a single shared *file* surface — the
    # rest of .codex/ (config.toml, skills) stays consumer-owned/generated.
    ".codex/hooks.json",
)

# internal Phase A: children of a SHARED_SURFACE that must be *absent*
# from the consumer tree. A whole-directory symlink exposes every child, so a
# surface that has any excluded child is materialized as a real directory with
# an individual symlink per non-excluded child instead — the named children
# simply never appear. The evals harness (config + runner + Make fragment) is
# private operational tooling excluded from the public consumer surface; its
# config (config/evals) was never shipped, so the runner and Make fragment
# that rode the whole-directory symlinks are carved out here to match.
SURFACE_CHILD_EXCLUSIONS: dict[str, frozenset[str]] = {
    "scripts/workbay": frozenset({"evals"}),
    "Makefile.d": frozenset({"evals.mk"}),
}

# Per-agent surfaces written by the generator into the target as real
# directories (not symlinks). Bootstrap ensures these exist as real
# dirs before the generator runs; pre-existing symlinks pointing into
# .workbay/remote/ (left over from the legacy overlay model) are
# replaced. Recorded in the manifest with ``source: "generated"``.
#
# ``.claude/skills``, ``.codex/skills``, and ``.claude/commands`` were
# dropped when the generated plugin tree became canonical; it
# (``.workbay/generated/plugins/workbay-system/base/{claude,codex}/``)
# now owns the Claude/Codex SKILL.md surface and command discovery. The
# legacy generator path emits Copilot prompts and the codex-command-router
# from the manifest path; everything else flows through the plugin
# marketplace pin.
GENERATED_SURFACES: tuple[str, ...] = (".github/prompts",)

from workbay_bootstrap.harnesses import (
    CLAUDE_MARKETPLACE_PATH,
    CLAUDE_SETTINGS_PATH,
    CODEX_CONFIG_PATH,
    CODEX_MARKETPLACE_PATH,
    GROK_PLUGIN_DEST,
    HARNESS_PLUGIN_DELIVERY,
    PLUGIN_DESCRIPTION,
    PLUGIN_GENERATED_ROOT,
    PLUGIN_MARKETPLACE_NAME,
    PLUGIN_NAME,
    PLUGIN_OWNER_NAME,
    PLUGIN_SELECTOR,
    harness_materialized_surfaces as _harness_materialized_surfaces,
    materialize_cursor_plugin as _materialize_cursor_plugin,
    materialize_grok_plugin as _materialize_grok_plugin,
    plugin_tree_out as _plugin_tree_out,
    relative_plugin_tree_path as _relative_plugin_tree_path,
    resolve_in_clone,
    write_codex_plugin_activation_config as _write_codex_plugin_activation_config,
    write_plugin_activation,
    write_plugin_pins as _write_plugin_pins,
)

PLUGIN_OVERRIDE_ROOT: tuple[str, ...] = ("workbay-overrides", PLUGIN_NAME)
PLUGIN_OVERRIDE_MANIFEST = "overrides.yaml"
PLUGIN_OVERRIDE_LOCK = "overrides.lock.json"


# Path to the generator script inside the cloned overlay.
GENERATOR_SCRIPT = "scripts/generate_agent_workflows.py"
GENERATOR_MANIFEST = "config/agent-workflows/portable_commands.json"
GENERATOR_SKILLS_SOURCE = "skills"
GENERATOR_LEDGER_SURFACES: tuple[str, ...] = (
    GENERATOR_SCRIPT,
    GENERATOR_MANIFEST,
    GENERATOR_SKILLS_SOURCE,
)

# Lifecycle profile: hoist the lifecycle Make fragment and the Python runner
# package into the consumer overlay.
# Source paths are resolved through ``_resolve_in_clone`` so they pick
# up the ``packages/workbay-system/`` prefix in the monorepo layout and
# fall back to a flat layout for hoisted fixture remotes. Destination
# paths are flat under the consumer root because the runner/Makefile
# fragment must be reachable from a vanilla consumer with no monorepo
# packaging knowledge.
LIFECYCLE_HOISTS: tuple[tuple[str, str], ...] = (
    ("Makefile.d/lifecycle.mk", "Makefile.d/lifecycle.mk"),
    ("scripts/workbay/lifecycle", "scripts/workbay/lifecycle"),
    # implementation note: the one-shot consumer update surface ships with the
    # lifecycle profile too, so `make workbay-update` exists everywhere
    # the lifecycle targets do.
    ("Makefile.d/update.mk", "Makefile.d/update.mk"),
    ("scripts/workbay/update.sh", "scripts/workbay/update.sh"),
)

# Sentinel block managed by ``_ensure_consumer_makefile_include`` so we
# can recognize and uninstall our edit without clobbering user content.
LIFECYCLE_INCLUDE_SENTINEL_BEGIN = "# >>> WORKBAY_BOOTSTRAP LIFECYCLE INCLUDE >>>"
LIFECYCLE_INCLUDE_SENTINEL_END = "# <<< WORKBAY_BOOTSTRAP LIFECYCLE INCLUDE <<<"
LEGACY_LIFECYCLE_INCLUDE_SENTINEL_BEGIN = (
    "# >>> AGENTIC_BOOTSTRAP LIFECYCLE INCLUDE >>>"
)
# Historical sentinel pairs migrated in place to the current block (implementation note:
# an unrecognized old marker caused a duplicate append in the 2026-06-05
# consumer incident). Order: (begin, end).
LEGACY_LIFECYCLE_INCLUDE_SENTINELS: tuple[tuple[str, str], ...] = (
    (
        LEGACY_LIFECYCLE_INCLUDE_SENTINEL_BEGIN,
        "# <<< AGENTIC_BOOTSTRAP LIFECYCLE INCLUDE <<<",
    ),
    (
        "# >>> WORKBAY_LIFECYCLE_INCLUDE >>>",
        "# <<< WORKBAY_LIFECYCLE_INCLUDE <<<",
    ),
)
LIFECYCLE_INCLUDE_DIRECTIVE = "-include Makefile.d/*.mk"
LIFECYCLE_TARGET_NAMES = frozenset(
    {
        "task-start",
        "task-finish",
        "context",
        "slice-start",
        "slice-commit",
        "review-ready",
        "close-check",
        "handoff-close-check",
        "plan-review",
        "plan-analyze",
        "review-run",
        "handoff-review-run",
        "status",
        "tasks",
        "doctor",
        "project-events-replay",
        "tasks-gc",
        "dashboard",
        "format",
    }
)

# Profile contract. ``all`` is the default for both the library
# ``install()`` API and the CLI, so a no-argument ``workbay-bootstrap
# install`` materializes the full surface set out of the box. ``minimal``
# and ``lifecycle`` remain opt-in.
PROFILE_MINIMAL = "minimal"
PROFILE_LIFECYCLE = "lifecycle"
PROFILE_ALL = "all"
SUPPORTED_PROFILES: frozenset[str] = frozenset(
    {PROFILE_MINIMAL, PROFILE_LIFECYCLE, PROFILE_ALL}
)

# Built-in managed-server map: ``DEFAULT_MCP_SERVERS`` (re-exported from
# the generated ``_mcp_pins`` module, imported at the top of this file).
# The two WorkBay MCP servers ship from this repo and are runnable via
# ``uvx``. Used when callers pass ``mcp_servers="default"`` or, in the
# CLI, when ``--mcp-servers`` is omitted and ``--no-mcp-servers`` is not
# set. Operators wanting a custom managed map keep providing a JSON file
# via ``--mcp-servers <path>``.
#
# implementation note: the map (and the per-harness MCP_REGISTRATION ownership
# table) is GENERATED from the canonical mcp_servers.yaml manifest into
# ``_mcp_pins.py`` by ``scripts/mcp_pins.py sync`` — edit the manifest,
# not that module; ``make mcp-pins-check`` gates drift.


# Distributed MCP servers use WorkBay-native identities
# (``workbay-handoff-mcp`` / ``workbay-orchestrator-mcp``) only. No legacy
# read-side compatibility is carried.
#
# NOTE (D1): the private, non-published MCP server keeps its private/non-distributed
# identity per implementation note §9-D and implementation note §0.5 D1; it is owned by a separate follow-up.


def _local_handoff_project_candidates() -> tuple[tuple[str, str], ...]:
    return (("packages/mcp-workbay-handoff", "mcp-workbay-handoff"),)


def _build_local_handoff_retry_cmd(target: Path, cmd: list[str]) -> list[str] | None:
    if not cmd:
        return None

    tail: list[str] | None = None
    use_from = False
    if cmd[0] == "uvx":
        use_from = len(cmd) >= 4 and cmd[1] == "--from"
        if use_from:
            package_ref = cmd[2]
            tail = cmd[3:]
        elif len(cmd) >= 2:
            package_ref = cmd[1]
            tail = cmd[2:]
        else:
            return None
        if not package_ref.startswith("mcp-workbay-handoff"):
            return None
    elif (
        len(cmd) >= 3
        and cmd[0] in ("python3", "python")
        and "mcp_launch.py" in cmd[1]
        and cmd[2].startswith("workbay-handoff")
    ):
        tail = cmd[3:]
    else:
        return None

    clone = target.joinpath(*CLONE_SUBDIR)
    for relative_path, cli_name in _local_handoff_project_candidates():
        project = clone / relative_path
        if not (project / "pyproject.toml").is_file():
            continue
        # --no-sources: the git_overlay clone keeps `[tool.uv.sources]
        # { workspace = true }` pins that `uv run` rejects outside the
        # workspace (internal missed this retry site).
        base = ["uv", "run", "--no-sources", "--project", str(project)]
        # The ``uvx --from <spec> <console> ...`` form already carries the
        # console name as ``tail[0]``; prepending ``cli_name`` would duplicate
        # it (bogus positional -> retry fails). The bare-uvx and shim forms put
        # only trailing args in ``tail``, so they still need ``cli_name``.
        if use_from:
            return [*base, *(tail or [])]
        return [*base, cli_name, *(tail or [])]

    return None


def _resolve_local_mcp_project(
    target: Path,
    candidates: tuple[tuple[str, str], ...],
    base: Path | None = None,
) -> tuple[str, str] | None:
    # ``base`` is the overlay root under which the mcp-workbay-* uv projects
    # live: the managed clone for git_overlay (default), or the worktree itself
    # for source_kind=worktree (implementation note — the packages ship in-tree, no clone).
    base = base if base is not None else target.joinpath(*CLONE_SUBDIR)
    for relative_path, cli_name in candidates:
        project = base / relative_path
        if not (project / "pyproject.toml").is_file():
            continue
        return project.relative_to(target).as_posix(), cli_name
    return None


_MCP_LAUNCH_SHIM = "scripts/hooks/mcp_launch.py"

# Single source of truth for the in-tree local MCP servers implementation note routes
# through the launcher shim: (server id, package path under the overlay base,
# console script name). Both the launch-spec builder
# (:func:`_build_local_default_mcp_servers`) and the install-time presync
# (:func:`_local_uv_project_from_spec`) consume this so the two cannot drift.
# The payload shim's own SERVERS registry mirrors it and is pinned by
# tests/test_mcp_sync_unit.py::test_shim_servers_registry_matches_install_routing.
_LOCAL_MCP_SERVERS: tuple[tuple[str, str, str], ...] = (
    (
        "workbay-handoff-mcp",
        "packages/mcp-workbay-handoff",
        "mcp-workbay-handoff",
    ),
    (
        "workbay-orchestrator-mcp",
        "packages/mcp-workbay-orchestrator",
        "mcp-workbay-orchestrator",
    ),
)
# server id -> package path, for recovering the project a shim spec provisions.
_LOCAL_MCP_PROJECTS: dict[str, str] = {
    server_id: relpath for server_id, relpath, _console in _LOCAL_MCP_SERVERS
}


def _local_mcp_spec(server_id: str, project: str, cli: str) -> dict[str, Any]:
    """Launch spec for one local MCP server.

    In-tree (``packages/<pkg>``) routes through the stdlib shim
    (``python3 scripts/hooks/mcp_launch.py <server-id>``): per boot the shim
    execs the deps-bearing venv console script directly, skipping ``uv run``'s
    per-invocation project resolution — the boot-miss cause (implementation note). The
    forwarded serve args live in the shim's SERVERS registry. A clone-relative
    project (git_overlay) is a layout the shim's registry does not know, so it
    keeps the ``uv run --no-sync`` launcher unchanged (no regression).
    """
    if project == f"packages/{cli}":
        return {
            "type": "stdio",
            "command": "python3",
            "args": [_MCP_LAUNCH_SHIM, server_id],
        }
    return {
        "type": "stdio",
        "command": "uv",
        "args": [
            "run",
            "--no-sync",
            "--project",
            project,
            # The git_overlay clone keeps the workspace's `[tool.uv.sources]
            # { workspace = true }` pins, which `uv run` rejects outside the
            # workspace; --no-sources falls the cloned package back to its plain
            # [project.dependencies] (internal missed this site).
            "--no-sources",
            cli,
            "--workspace-root",
            ".",
            "serve-stdio",
        ],
    }


def _build_local_default_mcp_servers(
    target: Path, base: Path | None = None
) -> dict[str, dict[str, Any]] | None:
    """Build the launch specs for the locally-cloned MCP servers.

    ``base`` selects the overlay root the mcp-workbay-* uv projects live
    under: the managed clone for git_overlay (default), or the worktree itself
    for source_kind=worktree (implementation note). Returns ``None`` when the packages
    are absent under ``base`` (e.g. a consumer repo with no in-tree source), so
    callers fall back to the published uvx map.

    The serve commands pass ``--no-sync`` so launching a server is a plain exec
    against an already-built environment — uv performs no dependency
    resolution, hits no network, and acquires no shared cache lock on the
    startup hot path. That removes the race where two servers cold-starting at
    once contend on uv's lock and one blows the MCP connection timeout (its
    tools then never register for the session). Environment construction is
    hoisted to install time via :func:`_presync_local_mcp_envs`, which must run
    before these specs are written so ``--no-sync`` always finds a ready venv.
    """
    servers: dict[str, dict[str, Any]] = {}
    for server_id, relpath, cli in _LOCAL_MCP_SERVERS:
        resolved = _resolve_local_mcp_project(target, ((relpath, cli),), base=base)
        if resolved is None:
            return None
        project, cli_name = resolved
        servers[server_id] = _local_mcp_spec(server_id, project, cli_name)
    return servers


def _resolve_install_mcp_servers(
    target: Path,
    remote_ref: str,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> Mapping[str, Mapping[str, Any]] | None:
    # The --no-sync invariant for local launchers is enforced at the shared
    # render/write seam (_canonicalize_managed_servers), so the resolver does
    # not normalize here — install, update, repair, and mcp-sync all converge
    # on the same launcher when their map is serialised. implementation note A1.
    if mcp_servers is not DEFAULT_MCP_SERVERS:
        return mcp_servers
    if remote_ref.startswith("v"):
        return mcp_servers
    return _build_local_default_mcp_servers(target) or mcp_servers


def _resolve_worktree_install_mcp_servers(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> Mapping[str, Mapping[str, Any]] | None:
    """Resolve managed MCP servers for a worktree install (implementation note).

    A worktree install runs the repo's own in-tree code, so the default uvx
    package map is rewritten to local ``uv run --project packages/...``
    launchers rooted at the worktree itself (no clone, no release). There is no
    remote_ref / release-tag exception (worktree is always local source). An
    explicit non-default map is honored verbatim; absent in-tree packages fall
    back to the uvx map. The ``--no-sync`` invariant is injected downstream at
    :func:`_canonicalize_managed_servers`.
    """
    if mcp_servers is not DEFAULT_MCP_SERVERS:
        return mcp_servers
    return _build_local_default_mcp_servers(target, base=target) or mcp_servers


def _manifest_source_kind(target: Path) -> str | None:
    """Read ``source_kind`` from the installed bootstrap/overlay manifest."""
    for name in MANIFEST_NAME_PRECEDENCE:
        manifest_path = target / name
        if not manifest_path.is_file():
            continue
        try:
            payload = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return str(payload.get("source_kind") or "git_overlay")
    return None


def _local_uv_project_from_spec(
    target: Path,
    spec: Mapping[str, Any],
) -> Path | None:
    """Resolve the in-tree uv project a local MCP launch spec provisions.

    Handles both launcher shapes the generator emits for in-tree packages: the
    ``uv run --no-sync --project <dir>`` form (git_overlay clone base) and the
    implementation note stdlib shim form ``python3 scripts/hooks/mcp_launch.py
    <server-id>`` (worktree base). The shim spec carries no project path on its
    command line, so the project is recovered from ``_LOCAL_MCP_PROJECTS`` -- the
    same registry the launch-spec builder uses. Returns ``None`` for any other
    spec (e.g. a published ``uvx`` pin) so presync only touches in-tree projects.
    """
    args = spec.get("args", [])
    if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
        return None
    command = spec.get("command")
    target_root = target.resolve()
    if command == "python3" and len(args) >= 2 and args[0] == _MCP_LAUNCH_SHIM:
        relpath = _LOCAL_MCP_PROJECTS.get(args[1])
        if relpath is None:
            return None
        project = (target_root / relpath).resolve()
    elif command == "uv":
        if not args or args[0] != "run" or "--project" not in args:
            return None
        project_index = args.index("--project") + 1
        if project_index >= len(args):
            return None
        project = (target_root / args[project_index]).resolve()
    else:
        return None
    try:
        project.relative_to(target_root)
    except ValueError:
        return None
    if not (project / "pyproject.toml").is_file():
        return None
    return project


def _target_venv_is_running_env(target: Path) -> bool:
    """True when ``target``'s workspace ``.venv`` is the interpreter running this
    process — a self-host "dogfood into the monorepo" install where the install
    target IS the workspace whose shared ``.venv`` is executing the tool.

    In that case any ``uv sync`` against ``target/.venv`` would uninstall the
    running ``workbay-bootstrap`` (and its siblings) mid-install. Resolved-path
    comparison so symlinked worktrees match. Consumer installs run from a
    separate env (uvx / uv tool), so this is False and normal presync proceeds.
    """
    try:
        target_venv = (target / ".venv").resolve()
    except OSError:
        return False
    return Path(sys.prefix).resolve() == target_venv


def _presync_local_mcp_envs(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> list[Path]:
    """Pre-build each locally-cloned MCP server's uv environment at install time.

    The generated serve commands launch with ``uv run --no-sync`` (see
    :func:`_build_local_default_mcp_servers`), so the environment must already
    exist by the time a session starts a server. Resolution belongs in the
    install phase — run once here, off the server-startup hot path — rather
    than lazily on every server boot. Returns the project dirs that were
    synced (deduplicated) so callers/tests can assert coverage. A spec that
    is not a local ``uv run --project`` launch is skipped.
    """
    if not mcp_servers:
        return []
    # internal: never `uv sync` the shared workspace .venv that
    # is running this installer (self-host dogfood self-target). That exact-sync
    # uninstalls the running workbay-bootstrap mid-install (then the git-only
    # closure import crashes) and corrupts the shared env. The workspace .venv
    # already has every member installed editable, so presync is both
    # unnecessary and destructive here. Command-query separation: this modifier
    # must not mutate state the running process depends on.
    if _target_venv_is_running_env(target):
        return []
    synced: list[Path] = []
    seen: set[Path] = set()
    for spec in mcp_servers.values():
        project = _local_uv_project_from_spec(target, spec)
        if project is None:
            continue
        if project in seen or not (project / "pyproject.toml").is_file():
            continue
        seen.add(project)
        # Match the published uvx pin sites, which carry `[bridge]` (see
        # DEFAULT_MCP_SERVERS): when the project declares a `bridge` optional
        # dependency (the orchestrator's workbay-codex-bridge for the
        # `codex-subagent` backend), presync it too — otherwise a locally
        # launched server probes `declared_not_installed` while the published
        # one works. Projects without the extra keep the cheap base sync.
        # --no-sources: the git_overlay clone keeps `[tool.uv.sources]
        # { workspace = true }` pins that `uv sync` rejects outside the
        # workspace (internal missed this presync site).
        command = ["uv", "sync", "--no-sources", "--project", str(project)]
        if _declares_bridge_extra(project):
            command += ["--extra", "bridge"]
        from workbay_bootstrap.external import run_external

        run_external(command, call_class="uv_sync", cwd=str(target))
        synced.append(project)
    return synced


def _prewarm_uvx_mcp_envs(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> list[str]:
    """Warm uv's tool-env cache for each published ``uvx`` MCP spec.

    The published launch pins resolve their whole environment at session boot
    (``uvx mcp-workbay-orchestrator[bridge]@<ver>``), so an offline host —
    or an index mirror missing the ``[bridge]`` wheel — hard-fails server
    launch where the pre-extra spec degraded to ``declared_not_installed``.
    Hoisting env construction to install time (the
    :func:`_presync_local_mcp_envs` pattern) makes session boots cache-served
    after any online install.

    Best-effort by design: a warm-up failure is reported by uv on stderr and
    skipped — the handoff server must stay installable even when the
    orchestrator extra cannot resolve, and the spec still works on any host
    with a warm cache or a reachable index. Returns the package refs that
    warmed successfully so callers/tests can assert coverage. Local ``uv run``
    specs are :func:`_presync_local_mcp_envs` territory and are skipped.
    """
    if not mcp_servers:
        return []
    warmed: list[str] = []
    seen: set[str] = set()
    for spec in mcp_servers.values():
        if spec.get("command") != "uvx":
            continue
        args = spec.get("args", [])
        if not isinstance(args, list) or not args:
            continue
        if args[0] == "--from" and len(args) >= 2:
            package_ref = args[1]
        else:
            package_ref = args[0]
        if not isinstance(package_ref, str) or package_ref in seen:
            continue
        seen.add(package_ref)
        from workbay_bootstrap.external import DeferredExternalCall, run_external

        try:
            result = run_external(
                ["uvx", "--from", package_ref, "python", "-c", "import sys"],
                call_class="uvx_prewarm",
                check=False,
                cwd=str(target),
            )
        except DeferredExternalCall:
            continue
        if result.returncode == 0:
            warmed.append(package_ref)
    return warmed


def _mcp_specs_use_launch_shim(
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> bool:
    if not mcp_servers:
        return False
    for spec in mcp_servers.values():
        args = spec.get("args")
        if (
            spec.get("command") == "python3"
            and isinstance(args, list)
            and len(args) >= 2
            and args[0] == _MCP_LAUNCH_SHIM
        ):
            return True
    return False


def _resolve_gitonly_member_specs(
    target: Path,
    *,
    source_kind: str,
    remote_url: str | None,
    remote_ref: str | None,
) -> dict[str, str] | None:
    from workbay_bootstrap.gitonly_closure import (
        member_specs_from_git_ref,
        member_specs_from_repo_root,
    )

    if source_kind == "worktree":
        return member_specs_from_repo_root(target)
    clone = target.joinpath(*CLONE_SUBDIR)
    if (
        source_kind == "git_overlay"
        and remote_ref
        and remote_ref.startswith("v")
        and remote_url
    ):
        return member_specs_from_git_ref(
            repo_url=_resolve_git_overlay_remote_url(target, remote_url),
            tag=remote_ref,
        )
    if (clone / "packages" / "mcp-workbay-handoff" / "pyproject.toml").is_file():
        return member_specs_from_repo_root(clone)
    return None


def _install_gitonly_mcp_tools(
    target: Path,
    *,
    member_specs: Mapping[str, str],
) -> list[str]:
    """One-time ``uv tool install`` for the git-sourced MCP server closure (Q5)."""
    from workbay_bootstrap.external import run_external
    from workbay_bootstrap.gitonly_closure import (
        GITONLY_MCP_PACKAGES,
        build_uv_tool_install_argv,
    )

    installed: list[str] = []
    for package in GITONLY_MCP_PACKAGES:
        argv = build_uv_tool_install_argv(
            package=package,
            from_spec=member_specs[package],
            member_specs=dict(member_specs),
        )
        run_external(
            ["uv", *argv],
            call_class="uv_tool_install_gitonly_mcp",
            cwd=str(target),
        )
        installed.append(package)
    return installed


def _declares_bridge_extra(project: Path) -> bool:
    """True when the project's pyproject declares a ``bridge`` optional
    dependency. Unparseable metadata reads as no-extra so presync degrades to
    the base sync instead of failing the install."""
    try:
        payload = tomllib.loads(
            (project / "pyproject.toml").read_text(encoding="utf-8")
        )
    except (OSError, tomllib.TOMLDecodeError):
        return False
    extras = (payload.get("project") or {}).get("optional-dependencies") or {}
    return isinstance(extras, dict) and "bridge" in extras


class BootstrapManifestValidationError(RuntimeError):
    """Raised when the install manifest fails the cross-repo wire-shape contract."""


def _annotate_surface_provenance(
    target: Path,
    manifest: dict[str, object],
    *,
    package_root: Path | None = None,
) -> None:
    """Record the optional additive receipt ``provenance_key`` per surface.

    internal deliverable (REV-B-001): each ``source='shared'``
    surface entry gains the snapshot identity it was mounted from
    (``clone:<sha>`` / ``link:<root>`` / ``package:<version>``). Schema
    additive — v2 receipts without the field keep the live-derivation
    fallback (``coherence._provenance_key``), which stays authoritative for
    the gates; the recorded key is provenance, never a gate substitute.
    """
    from workbay_bootstrap.coherence import _provenance_key

    surfaces = manifest.get("surfaces")
    if not isinstance(surfaces, list):
        return
    resolved_target = Path(target).resolve()
    for entry in surfaces:
        if not isinstance(entry, dict) or entry.get("source") != "shared":
            continue
        rel = entry.get("path")
        if not isinstance(rel, str):
            continue
        path = resolved_target / rel
        if not (path.exists() or path.is_symlink()):
            continue
        key = _provenance_key(resolved_target, path, package_root)
        if key is not None:
            entry["provenance_key"] = key


def _enforce_hook_coherence_gate(
    target: Path,
    manifest: dict[str, object],
    *,
    package_root: Path | None = None,
) -> None:
    """Abort before writing a receipt when installed hook surfaces are broken."""
    from workbay_bootstrap.coherence import assess_hook_coherence

    findings = assess_hook_coherence(
        target, package_root=package_root, receipt=manifest
    )
    errors = [finding for finding in findings if finding.severity == "error"]
    if not errors:
        return
    lines = [
        "refusing to declare install successful: hook-surface coherence "
        "reported error findings. Repair the hook config or materialized "
        "scripts, then re-run install/update."
    ]
    for finding in errors:
        lines.append(f"- {finding.kind}: {finding.path}: {finding.detail}")
    raise BootstrapManifestValidationError("\n".join(lines))


class RemoteUrlMismatchError(RuntimeError):
    """Raised when an existing ``.workbay/remote`` clone tracks a different
    ``origin`` URL than the one passed to ``install``.

    Silently rewriting the manifest while leaving the on-disk clone pointed at
    the old origin would make ``.workbay-bootstrap.json`` lie about provenance.
    Operators get an actionable error instead.
    """


def _managed_clone_can_switch_remote(
    *,
    existing_origin: str,
    existing_manifest_remote_url: str | None,
) -> bool:
    return existing_manifest_remote_url == existing_origin


def _replace_managed_clone_for_remote_switch(
    clone: Path,
    *,
    existing_origin: str,
    remote_url: str,
) -> None:
    dirty = _git("status", "--short", cwd=clone).strip()
    if dirty:
        raise RemoteUrlMismatchError(
            f"{clone} already tracks origin {existing_origin!r}, "
            f"but install was called with remote_url={remote_url!r}. "
            "The existing managed clone also has uncommitted changes; stash or "
            "remove .workbay/remote before switching overlays."
        )
    shutil.rmtree(clone)


class OverrideResetRequiresBackupError(RuntimeError):
    """Raised when ``reset_overrides`` would delete overrides from a dirty
    git worktree without an explicit backup preflight.
    """


def _migrate_legacy_manifest(target: Path) -> Path | None:
    """One-shot rename of a legacy ``.workbay-overlay.json`` manifest to the canonical ``.workbay-bootstrap.json``.

    Renames only when the legacy file looks like a bootstrap manifest
    (top-level dict with a list ``surfaces`` key) so consumer-owned files
    that happen to share the legacy name are not touched. Prefers
    ``git mv`` when ``target`` is a git worktree so the rename is tracked
    in history; falls back to ``Path.rename`` otherwise. Returns the new
    path on success, ``None`` when no migration was needed or the legacy
    file did not match the bootstrap shape.
    """
    canonical = target / BOOTSTRAP_MANIFEST_NAME
    if canonical.exists():
        return None
    for legacy_name in (LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME,):
        legacy = target / legacy_name
        if not legacy.is_file():
            continue
        try:
            data = json.loads(legacy.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict) or not isinstance(data.get("surfaces"), list):
            continue
        try:
            _git("mv", legacy_name, BOOTSTRAP_MANIFEST_NAME, cwd=target)
        except (subprocess.CalledProcessError, FileNotFoundError):
            legacy.rename(canonical)
        return canonical
    return None


def _read_core_hooks_path(target: Path) -> str | None:
    if not (target / ".git").exists():
        return None
    try:
        value = _git("config", "--get", "core.hooksPath", cwd=target).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    return value or None


def _resolve_hooks_path(target: Path, hooks_path: str) -> Path:
    path = Path(hooks_path)
    if not path.is_absolute():
        path = target / path
    return path.resolve()


def _detect_legacy_agentic_overlay(target: Path) -> str | None:
    """Return a short reason when a legacy agentic-system layout is present."""
    if (target / LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME).is_file():
        return LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME

    agentic_remote = target / ".agentic" / "remote"
    if (agentic_remote / ".git").exists():
        try:
            origin = _git("remote", "get-url", "origin", cwd=agentic_remote)
        except (subprocess.CalledProcessError, FileNotFoundError):
            return ".agentic/remote (embedded legacy clone)"
        if "agentic-system" in origin:
            return ".agentic/remote (agentic-system origin)"

    hooks_path = _read_core_hooks_path(target)
    if hooks_path:
        resolved = _resolve_hooks_path(target, hooks_path)
        agentic_root = (target / ".agentic").resolve()
        try:
            if resolved.is_relative_to(agentic_root):
                return "core.hooksPath under .agentic/"
        except ValueError:
            pass

    return None


def _generator_supports_onboard_router_markers(generator_script: Path) -> bool:
    try:
        text = generator_script.read_text()
    except OSError:
        return False
    return "--onboard-router-markers" in text


def _resolve_git_overlay_remote_url(target: Path, remote_url: str | None) -> str:
    """Derive the git_overlay remote URL for install.

    Explicit ``remote_url`` wins. Otherwise prefer the existing managed
    clone's configured origin, then the adjacent manifest's recorded URL,
    then :data:`workbay_bootstrap.cli.DEFAULT_REMOTE_URL`.
    """
    from workbay_bootstrap.cli import DEFAULT_REMOTE_URL

    if remote_url is not None:
        return remote_url
    clone = target.joinpath(*CLONE_SUBDIR)
    if (clone / ".git").exists():
        try:
            return _git("remote", "get-url", "origin", cwd=clone)
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
    manifest_url = _load_existing_manifest_remote_url(target)
    if manifest_url:
        return manifest_url
    return DEFAULT_REMOTE_URL


def _prime_worktree_manifest_for_init_state(target: Path, source: Any) -> None:
    """Pre-write a minimal adjacent manifest before init-state (PD-02).

    ``ForeignStateReuseError`` guard (a) refuses to reuse a pre-existing
    ``handoff.db`` when no adjacent ``.workbay-bootstrap.json`` exists yet.
    A first-ever ``--source worktree`` install writes its full manifest only
    *after* init-state runs, so over a pre-existing DB with no prior manifest
    that guard would fire. Write a minimal valid worktree manifest first; it is
    overwritten by ``_finalize_install_manifest`` at the end of the install.

    Strict no-op unless the source is worktree, a DB already exists, and no
    manifest is present yet — so the common fresh-install path and the
    manifest-already-present dogfood path stay byte-identical.
    """
    if getattr(source, "kind", None) != "worktree":
        return
    db_path = target / ".task-state" / "handoff.db"
    if not db_path.exists():
        return
    if any((target / name).is_file() for name in MANIFEST_NAME_PRECEDENCE):
        return
    stub = {
        "schema_version": SCHEMA_VERSION,
        "source_kind": "worktree",
        "remote_sha": source.remote_sha,
    }
    (target / BOOTSTRAP_MANIFEST_NAME).write_text(json.dumps(stub, indent=2) + "\n")


def _load_existing_manifest_remote_url(target: Path) -> str | None:
    for name in MANIFEST_NAME_PRECEDENCE:
        manifest_path = target / name
        if not manifest_path.is_file():
            continue
        try:
            payload = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        remote_url = payload.get("remote_url")
        if isinstance(remote_url, str) and remote_url:
            return remote_url
    return None


def _prepare_state_for_remote_switch(
    target: Path,
    remote_url: str,
) -> tuple[str | None, str | None]:
    existing_remote_url = _load_existing_manifest_remote_url(target)
    if existing_remote_url is None or existing_remote_url == remote_url:
        return remote_url, None

    state_dir = target / ".task-state"
    backup_path: str | None = None
    if state_dir.exists():
        stamp = _utc_stamp()
        backup_root = target / RUNTIME_ROOT_DIRNAME / "state-backups" / stamp
        archive_target = backup_root / state_dir.name
        archive_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(state_dir), str(archive_target))
        backup_path = backup_root.relative_to(target).as_posix()

    # The old adjacent bootstrap manifest still points at the prior remote
    # until install writes the new one below. Skip the reuse guard for this
    # init only after moving the old runtime state out of the way.
    return None, backup_path


def _git_worktree_is_dirty(target: Path) -> bool:
    if not (target / ".git").exists():
        return False
    return bool(
        _git(
            "status",
            "--short",
            "--",
            ".",
            f":(exclude){'/'.join(CLONE_SUBDIR)}",
            cwd=target,
        ).strip()
    )


def _prune_empty_parent_dirs(root: Path, stop: Path) -> None:
    current = root.resolve()
    stop = stop.resolve()
    try:
        current.relative_to(stop)
    except ValueError:
        return
    while current != stop:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _reset_plugin_overrides(
    target: Path,
    override_root: Path | None,
    *,
    reset_overrides: bool,
    backup_overrides: bool,
) -> tuple[Path | None, str | None]:
    if not reset_overrides or override_root is None:
        return override_root, None

    override_root = override_root.resolve()
    if _git_worktree_is_dirty(target) and not backup_overrides:
        raise OverrideResetRequiresBackupError(
            "refusing to reset plugin overrides from a dirty git worktree without "
            "backup_overrides=True; commit/stash changes first or opt into backup preflight"
        )

    backup_path: str | None = None
    if backup_overrides:
        stamp = _utc_stamp()
        backup_root = target / RUNTIME_ROOT_DIRNAME / "override-backups" / stamp
        archive_target = backup_root / override_root.name
        archive_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(override_root, archive_target)
        backup_path = backup_root.relative_to(target).as_posix()

    shutil.rmtree(override_root)
    _prune_empty_parent_dirs(override_root.parent, target)
    return None, backup_path


def _utc_stamp() -> str:
    """Return a filesystem-safe UTC timestamp (``YYYYMMDDTHHMMSSZ``).

    Shared by every archive/backup path so the stamp format is defined in
    exactly one place. Never invoked at import time — callers pass the
    resulting string into the path builders.
    """
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _git(*args: str, cwd: Path | None = None) -> str:
    """Run ``git`` with the given args, returning stripped stdout."""
    cmd = ["git"]
    if cwd is not None:
        cmd.extend(["-C", str(cwd)])
    cmd.extend(args)
    from workbay_bootstrap.external import run_external

    result = run_external(
        cmd,
        call_class="git",
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _resolve_ref_to_sha(clone: Path, remote_ref: str) -> str:
    """Resolve ``remote_ref`` against the just-fetched clone, preferring the
    fresh remote-tracking branch over any stale local ref.

    Resolution order:

     1. ``refs/remotes/origin/<ref>`` — picks up freshly fetched branch tips
         and avoids the stale local-branch trap that ``git checkout --detach
         <branch>`` falls into after ``fetch``.
    2. ``refs/tags/<ref>`` — tag refs.
    3. ``<ref>`` raw — last-resort for SHAs and exotic refspecs.
    """
    candidates = (
        f"refs/remotes/origin/{remote_ref}",
        f"refs/tags/{remote_ref}",
        remote_ref,
    )
    for candidate in candidates:
        try:
            return _git("rev-parse", "--verify", f"{candidate}^{{commit}}", cwd=clone)
        except subprocess.CalledProcessError:
            continue
    raise RuntimeError(
        f"could not resolve remote_ref {remote_ref!r} in {clone} "
        "(tried remote-tracking branch, tag, and raw ref)"
    )


def _resolve_in_clone(clone: Path, relpath: str) -> Path:
    """Resolve a surface/asset path against the clone (delegates to harnesses)."""
    return resolve_in_clone(clone, relpath)


def _materialize_one_symlink(
    rel: str,
    remote_path: Path,
    target_path: Path,
    clone_resolved: Path,
    remote_subtree_prefix: str,
) -> dict[str, str]:
    """Materialize one ``target_path -> remote_path`` relative symlink.

    Encapsulates the idempotency / repoint / foreign-precedence rules so it
    can be applied to a whole surface or to a single carved child. Returns the
    manifest entry for ``rel``:

    - Target absent: create parent, symlink, record ``source='shared'``.
    - Target already a symlink resolving to the current source: leave it;
        record ``source='shared'`` (idempotent rerun path).
    - Target already a symlink that is a stale/broken bootstrap-owned link
        (stale pointer into our clone, or a dangling link naming a relocated
        ``.workbay/remote`` clone): repoint and record ``source='shared'``.
    - Target a foreign symlink (still resolving, or dangling but not naming our
        clone), or a real file/dir: leave it untouched and record
        ``source='local'`` so overlay precedence is honored.
    """
    expected_rel = os.path.relpath(remote_path, target_path.parent)
    target_is_directory = remote_path.is_dir()

    if target_path.is_symlink():
        raw_target = os.readlink(target_path)
        if os.path.isabs(raw_target):
            abs_target_str = os.path.normpath(raw_target)
        else:
            abs_target_str = os.path.normpath(
                os.path.join(str(target_path.parent), raw_target)
            )
        try:
            resolved = target_path.resolve(strict=False)
        except OSError:
            resolved = None
        resolves_to_expected = (
            resolved is not None and resolved == remote_path.resolve()
        )
        if resolves_to_expected:
            return {"path": rel, "source": "shared"}
        if _is_repointable_bootstrap_symlink(
            target_path, abs_target_str, clone_resolved, remote_subtree_prefix
        ):
            # Stale in-tree pointer, or a dangling bootstrap-owned link whose
            # target names a clone that no longer exists here (relocated
            # primary) — repoint to the current canonical location.
            target_path.unlink()
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.symlink_to(
                expected_rel, target_is_directory=target_is_directory
            )
            print(f"repointed: {rel}")
            return {"path": rel, "source": "shared"}
        # Foreign symlink — local content takes precedence.
        return {"path": rel, "source": "local"}

    if target_path.exists():
        return {"path": rel, "source": "local"}

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.symlink_to(expected_rel, target_is_directory=target_is_directory)
    return {"path": rel, "source": "shared"}


def _raw_symlink_target_path(link_path: Path) -> str:
    raw_target = os.readlink(link_path)
    if os.path.isabs(raw_target):
        return os.path.normpath(raw_target)
    return os.path.normpath(os.path.join(str(link_path.parent), raw_target))


def _points_into_remote_subtree(
    abs_target_str: str,
    clone_resolved: Path,
    remote_subtree_prefix: str,
) -> bool:
    return abs_target_str == str(clone_resolved) or abs_target_str.startswith(
        remote_subtree_prefix
    )


def _is_repointable_bootstrap_symlink(
    target_path: Path,
    abs_target_str: str,
    clone_resolved: Path,
    remote_subtree_prefix: str,
) -> bool:
    """True when a surface symlink is a stale/broken bootstrap-owned link that
    should be repointed to the current clone.

    Single source of the repoint rule, shared by apply
    (:func:`_materialize_one_symlink`) and the drift guard (``adopt._link_drifts``)
    so they cannot disagree. Two cases:

    1. The link points lexically into our own clone subtree but does not resolve
       to the current source (a stale in-tree pointer).
    2. The link is *dangling* and its stale target names a ``.workbay/remote``
       clone — e.g. the primary was relocated, so the link points at a clone
       path that no longer exists here.

    A foreign symlink that still resolves is NOT repointable (local precedence);
    a dangling link whose target does not name our clone subtree is the
    operator's own broken link and is likewise left untouched.
    """
    if _points_into_remote_subtree(
        abs_target_str, clone_resolved, remote_subtree_prefix
    ):
        return True
    if target_path.exists():
        try:
            resolved_str = str(target_path.resolve())
        except OSError:
            resolved_str = ""
        if resolved_str and _names_clone_subtree(resolved_str):
            if not _points_into_remote_subtree(
                resolved_str, clone_resolved, remote_subtree_prefix
            ):
                return True
    return not target_path.exists() and _names_clone_subtree(abs_target_str)


def _names_clone_subtree(abs_target_str: str) -> bool:
    """True when ``abs_target_str`` names a bootstrap clone subtree.

    Matches ``.workbay/remote`` as consecutive path segments (never bare
    substrings).
    """
    parts = abs_target_str.split(os.sep)
    for runtime, remote in (CLONE_SUBDIR,):
        if any(
            parts[i] == runtime and parts[i + 1] == remote
            for i in range(len(parts) - 1)
        ):
            return True
    return False


def _remove_bootstrap_owned_excluded_child(
    child_path: Path,
    clone_resolved: Path,
    remote_subtree_prefix: str,
) -> None:
    if not child_path.is_symlink():
        return
    if _points_into_remote_subtree(
        _raw_symlink_target_path(child_path),
        clone_resolved,
        remote_subtree_prefix,
    ):
        child_path.unlink()


def _lifecycle_hoist_children(surface: str) -> frozenset[str]:
    """Child names of ``surface`` that :data:`LIFECYCLE_HOISTS` owns.

    A carved surface must not symlink these children: the lifecycle hoist
    copies them as real files later in ``install()``, and recording a
    ``source='shared'`` symlink entry here would collide with that pass's
    ``source='lifecycle'`` entry for the same path.
    """
    children: set[str] = set()
    for _src_rel, dest_rel in LIFECYCLE_HOISTS:
        parent, _, child = dest_rel.rpartition("/")
        if parent == surface and child:
            children.add(child)
    return frozenset(children)


def _carved_surface_children(surface: str, remote_path: Path) -> list[Path]:
    """Clone-side children of a carved ``surface`` that should be symlinked.

    The single source of the carve/exclusion rule: every child of
    ``remote_path`` (in name order) except those named in
    :data:`SURFACE_CHILD_EXCLUSIONS` (carved out entirely) or owned by
    :data:`LIFECYCLE_HOISTS` (copied as real files by the lifecycle pass, so
    symlinking here would collide). Shared by the materializer (apply) and the
    drift guard (``--check``) via :func:`iter_expected_surface_targets`.
    """
    excluded = SURFACE_CHILD_EXCLUSIONS[surface] | _lifecycle_hoist_children(surface)
    return [
        child
        for child in sorted(remote_path.iterdir(), key=lambda p: p.name)
        if child.name not in excluded
    ]


@dataclass(frozen=True)
class ExpectedSurfaceTarget:
    """One bootstrap-owned surface symlink that *should* exist in a target.

    ``rel`` is the manifest-relative path; ``remote_path`` is the clone-side
    source the link points at; ``target_path`` is where the link lives;
    ``carved_parent`` is the owning carved surface for a per-child link, else
    ``None`` for a whole-directory plain surface.
    """

    rel: str
    remote_path: Path
    target_path: Path
    carved_parent: str | None = None


def self_host_payload_root(target: Path) -> Path | None:
    """The live in-tree payload root when ``target`` self-hosts workbay-system.

    Self-host means the target repo IS the overlay source: it ships the
    payload at ``packages/workbay-system/workbay_system/payload``. Returns
    ``None`` for ordinary consumers (the overwhelmingly common case), keeping
    their single-clone mount behavior untouched.

    Public so the ``bootstrap-surfaces`` self-host path (implementation note) can gate on
    the same payload-root resolution this module uses internally; the private
    alias below preserves the historical module-local call sites.
    """
    payload = target / WORKBAY_SYSTEM_PAYLOAD_SUBDIR
    return payload if payload.is_dir() else None


# Historical private alias — internal call sites predate the public promotion.
_self_host_payload_root = self_host_payload_root


def iter_expected_surface_targets(
    target: Path, clone: Path
) -> list[ExpectedSurfaceTarget]:
    """Enumerate the bootstrap-owned surface symlink targets for ``target``.

    This is the **single source** of the surface / carve / exclusion
    enumeration, consumed by both the materializer (:func:`_materialize_surfaces`,
    apply) and the adopt drift guard (``adopt._compute_drift``, ``--check``) so
    the two cannot desync — the root cause of the two false-drift bugs fixed on
    the implementation note branch. Each entry is one expected ``target_path ->
    remote_path`` symlink: a whole-directory link for a plain surface, or one
    per non-excluded child for a carved surface. Surfaces absent in the clone
    are skipped (never recorded). It does not inspect the target's current
    state — per-target idempotency / foreign-precedence stays in
    :func:`_materialize_one_symlink` (apply) and ``_link_drifts`` (check).

    internal (self-host mount repoint): when ``target`` itself ships
    the workbay-system payload, every shared surface that exists in the
    LIVE in-tree payload mounts from there instead of the ``.workbay/remote``
    clone — a payload edit can then never skew against a stale clone mount
    (the terminal-guard incident substrate); same-snapshot coherence holds by
    construction. Consumers without an in-tree payload keep the single-clone
    mounts byte-identical. Because apply and the drift guard share this
    enumeration, the stale-clone-link repoint rule in
    :func:`_materialize_one_symlink` performs the receipted migration
    (``repointed:`` audit line) from the original clone symlink, and the
    Phase-0 hand-repointed payload symlink is already the expected state
    (idempotent, no audit line).
    """
    results: list[ExpectedSurfaceTarget] = []
    self_host_payload = _self_host_payload_root(target)
    for surface in SHARED_SURFACES:
        remote_path = _resolve_in_clone(clone, surface)
        if self_host_payload is not None:
            live_path = self_host_payload / surface
            if live_path.exists():
                remote_path = live_path
        if not remote_path.exists():
            continue
        if surface in SURFACE_CHILD_EXCLUSIONS:
            for child in _carved_surface_children(surface, remote_path):
                results.append(
                    ExpectedSurfaceTarget(
                        f"{surface}/{child.name}",
                        child,
                        target / surface / child.name,
                        carved_parent=surface,
                    )
                )
        else:
            results.append(
                ExpectedSurfaceTarget(surface, remote_path, target / surface)
            )
    return results


def _prepare_carved_parent(
    surface: str,
    target_path: Path,
    clone_resolved: Path,
    remote_subtree_prefix: str,
) -> dict[str, str] | None:
    """Prep a carved surface's parent dir before its children are symlinked.

    Returns a ``{'path': surface, 'source': 'local'}`` manifest entry when the
    parent is foreign / real local content — the caller then skips its children
    (local precedence). Otherwise ensures the parent is a real directory
    (replacing a legacy whole-directory symlink into our own clone) and removes
    any bootstrap-owned symlinks for now-excluded children, then returns
    ``None``.
    """
    if target_path.is_symlink():
        if _points_into_remote_subtree(
            _raw_symlink_target_path(target_path),
            clone_resolved,
            remote_subtree_prefix,
        ):
            # Legacy whole-directory symlink into our own clone — replace
            # with a real directory so the excluded children can be carved.
            target_path.unlink()
            print(f"carved: {surface}")
        else:
            # Foreign symlink: local content wins, leave untouched.
            return {"path": surface, "source": "local"}
    elif target_path.exists() and not target_path.is_dir():
        # A real file where a directory surface is expected — foreign/local.
        return {"path": surface, "source": "local"}

    target_path.mkdir(parents=True, exist_ok=True)
    excluded = SURFACE_CHILD_EXCLUSIONS[surface] | _lifecycle_hoist_children(surface)
    for child_name in excluded:
        _remove_bootstrap_owned_excluded_child(
            target_path / child_name,
            clone_resolved,
            remote_subtree_prefix,
        )
    return None


def _materialize_surfaces(target: Path, clone: Path) -> list[dict[str, str]]:
    """Symlink each known shared surface from ``clone`` into ``target``.

    Driven by :func:`iter_expected_surface_targets` (the shared enumeration), so
    apply and ``--check`` cannot disagree on which targets are bootstrap-owned.
    A plain surface is a single whole-directory symlink; a carved surface
    (listed in :data:`SURFACE_CHILD_EXCLUSIONS`) becomes a real directory whose
    non-excluded children are symlinked individually. Carved parents are prepped
    once via :func:`_prepare_carved_parent`; a foreign parent wins (local
    precedence) and its children are skipped. See :func:`_materialize_one_symlink`
    for the per-target idempotency / repoint / foreign-precedence rules.

    A carved surface whose children are *all* excluded yields no enumeration
    entries, so its parent is intentionally not prepped (no real dir created) —
    this matches the drift guard, which likewise expects nothing for it. The two
    real carved surfaces always ship non-excluded children, so this is a
    theoretical edge, not a live path.
    """

    materialized: list[dict[str, str]] = []
    clone_resolved = clone.resolve()
    remote_subtree_prefix = str(clone_resolved) + os.sep

    # carved surface -> True when its parent is foreign (skip its children).
    carved_parent_is_foreign: dict[str, bool] = {}

    for expected in iter_expected_surface_targets(target, clone):
        parent = expected.carved_parent
        if parent is not None:
            if parent not in carved_parent_is_foreign:
                local_entry = _prepare_carved_parent(
                    parent, target / parent, clone_resolved, remote_subtree_prefix
                )
                carved_parent_is_foreign[parent] = local_entry is not None
                if local_entry is not None:
                    materialized.append(local_entry)
            if carved_parent_is_foreign[parent]:
                continue

        materialized.append(
            _materialize_one_symlink(
                expected.rel,
                expected.remote_path,
                expected.target_path,
                clone_resolved,
                remote_subtree_prefix,
            )
        )

    return materialized


def _existing_surface_sources(target: Path) -> dict[str, str]:
    """Read the current receipt's surface source map, if a receipt exists."""
    manifest_path = target / BOOTSTRAP_MANIFEST_NAME
    if not manifest_path.is_file():
        return {}
    try:
        payload = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    surfaces = payload.get("surfaces")
    if not isinstance(surfaces, list):
        return {}
    sources: dict[str, str] = {}
    for entry in surfaces:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        source = entry.get("source")
        if isinstance(path, str) and isinstance(source, str):
            sources[path] = source
    return sources


def _copy_surface_entry(
    src: Path,
    dest: Path,
    rel: str,
    *,
    previous_sources: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy one surface (file or whole directory) from a package data root.

    ``shutil.copy2``/``copytree`` preserve file modes, so executable hook
    scripts keep their ``0o755`` bit. Receipt-owned ``source=shared`` copies are
    replaced for idempotent reruns; everything else already on disk keeps local
    precedence.
    """
    previously_shared = (previous_sources or {}).get(rel) == "shared"
    if dest.is_symlink():
        if not previously_shared:
            return {"path": rel, "source": "local"}
        dest.unlink()
    elif dest.exists():
        if not previously_shared:
            return {"path": rel, "source": "local"}
        if dest.is_dir():
            shutil.rmtree(dest)
        else:
            dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        shutil.copytree(src, dest)
    else:
        shutil.copy2(src, dest)
    return {"path": rel, "source": "shared"}


def _carved_parent_has_managed_children(
    surface: str, previous_sources: Mapping[str, str]
) -> bool:
    prefix = surface + "/"
    return any(
        path.startswith(prefix) and source == "shared"
        for path, source in previous_sources.items()
    )


def _materialize_surfaces_copy(
    target: Path,
    source_root: Path,
    *,
    previous_sources: Mapping[str, str] | None = None,
) -> list[dict[str, str]]:
    """Copy each known shared surface from the package data root into target.

    The package delivery source has no clone to symlink into and runs in an
    ephemeral env, so surfaces are **copied** (the git-overlay path symlinks).
    Carved surfaces drop the same excluded children as the symlink path; the
    carve/exclusion rule is single-sourced via :func:`_carved_surface_children`
    so this copy path cannot desync from :func:`_materialize_surfaces`. A real
    plain surface already present in the target is treated as local and left
    untouched; a carved parent that is a real directory is **merged** child by
    child (each child gets :func:`_copy_surface_entry`'s per-path precedence),
    matching :func:`_prepare_carved_parent` on the git_overlay path.
    """
    materialized: list[dict[str, str]] = []
    previous_sources = previous_sources or {}
    for surface in SHARED_SURFACES:
        src = _resolve_in_clone(source_root, surface)
        if not src.exists():
            continue
        dest = target / surface
        if surface in SURFACE_CHILD_EXCLUSIONS:
            previously_managed = previous_sources.get(
                surface
            ) == "shared" or _carved_parent_has_managed_children(
                surface, previous_sources
            )
            if dest.is_symlink() or (dest.exists() and not dest.is_dir()):
                # Foreign symlink / non-dir file where the carved dir belongs:
                # local precedence wins unless a prior receipt owned this
                # parent (mirrors _prepare_carved_parent on git_overlay).
                if not previously_managed:
                    materialized.append({"path": surface, "source": "local"})
                    continue
                dest.unlink()
            # A pre-existing real directory is merged, never skipped wholesale:
            # each child falls through to _copy_surface_entry's per-path
            # precedence, so consumer files keep winning path by path while
            # managed children still land on first install.
            dest.mkdir(parents=True, exist_ok=True)
            for child in _carved_surface_children(surface, src):
                materialized.append(
                    _copy_surface_entry(
                        child,
                        dest / child.name,
                        f"{surface}/{child.name}",
                        previous_sources=previous_sources,
                    )
                )
        else:
            materialized.append(
                _copy_surface_entry(
                    src,
                    dest,
                    surface,
                    previous_sources=previous_sources,
                )
            )
    return materialized


def _package_source_root(package_root: Path | None) -> Path:
    """Resolve the workbay-system overlay payload root for the package source.

    Uses an explicit ``package_root`` (tests / pinned installs) when given,
    otherwise the installed ``workbay_system`` distribution's data root.
    """
    if package_root is not None:
        return Path(package_root).resolve()
    try:
        from workbay_system import data_root  # type: ignore[import-not-found]
    except ImportError as exc:
        raise FileNotFoundError(
            "source='package' requires the workbay-system distribution to be "
            "installed (import workbay_system failed); install it or pass "
            "package_root explicitly."
        ) from exc
    return Path(data_root()).resolve()


def _package_version(source_root: Path) -> str:
    """Return the installed workbay-system version (distribution metadata),
    falling back to a stable local marker when metadata is unavailable."""
    try:
        return importlib_metadata.version("workbay-system")
    except Exception:  # noqa: BLE001
        return "0.0.0+local"


# The `workbay` front door is the version anchor: its metadata carries the
# exact `==` member pins. (workbay-stack was retired.)
STACK_DISTRIBUTION = "workbay"
_STACK_PIN_RE = re.compile(r"^\s*([A-Za-z0-9._-]+)\s*==\s*([^\s;]+)\s*$")


def _stack_provenance() -> tuple[str | None, str | None, dict[str, str] | None]:
    """Detect the installed ``workbay`` version anchor (implementation note).

    Returns ``(distribution, version, members)`` where ``members`` maps each
    member distribution to the exact version the installed anchor's metadata
    pins (``Requires-Dist: name==X.Y.Z``). All ``None`` when the anchor is
    not installed — legacy consumers keep their pre-stack manifest shape.
    Non-pin requirements (ranges, extras, markers) are ignored: the anchor's
    contract is exact pins only.
    """
    try:
        version = importlib_metadata.version(STACK_DISTRIBUTION)
    except importlib_metadata.PackageNotFoundError:
        return None, None, None
    members: dict[str, str] = {}
    for requirement in importlib_metadata.requires(STACK_DISTRIBUTION) or []:
        match = _STACK_PIN_RE.match(requirement)
        if match:
            members[match.group(1)] = match.group(2)
    return STACK_DISTRIBUTION, version, members


def _prepare_generated_surfaces(target: Path, clone: Path) -> list[dict[str, str]]:
    """Ensure each per-agent generated surface exists as a real directory.

    Pre-existing symlinks pointing into the clone (left over from the
    pre-Plan-0002 overlay model where these surfaces were shared
    symlinks) are replaced with empty directories so the generator can
    write into them. Pre-existing real local content is preserved —
    the operator may have intentionally placed local overrides there;
    the generator's per-file write logic will only replace the files
    it owns.
    """
    materialized: list[dict[str, str]] = []
    clone_resolved = clone.resolve()

    for surface in GENERATED_SURFACES:
        target_path = target / surface

        if target_path.is_symlink():
            try:
                resolved = target_path.resolve(strict=False)
            except OSError:
                resolved = None
            points_into_clone = resolved is not None and str(resolved).startswith(
                str(clone_resolved) + os.sep
            )
            broken = resolved is not None and not target_path.exists()
            if points_into_clone or broken:
                # Legacy overlay symlink, or a dangling symlink whose
                # target is gone — replace with a real directory so the
                # generator can write into it. (A dangling symlink also
                # blocks the mkdir below, since lexists() is True.)
                target_path.unlink()
                target_path.mkdir(parents=True, exist_ok=True)
            # Foreign live symlinks are left alone (operator chose them);
            # the generator will write through them into wherever they point.

        elif not target_path.exists():
            target_path.mkdir(parents=True, exist_ok=True)

        materialized.append({"path": surface, "source": "generated"})

    return materialized




def _prepare_generator_ledger_surfaces(clone: Path) -> list[dict[str, str]]:
    """Record generator inputs in the install manifest surface ledger."""
    entries: list[dict[str, str]] = []
    for rel in GENERATOR_LEDGER_SURFACES:
        if _resolve_in_clone(clone, rel).exists():
            entries.append({"path": rel, "source": "shared"})
    return entries


def _prepare_plugin_generated_surfaces(
    target: Path, clone: Path, override_root: Path | None
) -> list[dict[str, str]]:
    """Record generated plugin trees that install materializes for this target."""
    generator_script = _resolve_in_clone(clone, GENERATOR_SCRIPT)
    manifest_path = _resolve_in_clone(clone, GENERATOR_MANIFEST)
    if not generator_script.is_file() or not manifest_path.is_file():
        return []

    entries = [
        {
            "path": Path(*PLUGIN_GENERATED_ROOT, "base").as_posix(),
            "source": "generated",
        },
        {
            "path": Path(*PLUGIN_GENERATED_ROOT, "effective").as_posix(),
            "source": "generated",
        },
    ]
    return entries


def _plugin_override_root_from_manifest(
    target: Path, manifest: Mapping[str, object] | None
) -> Path | None:
    if manifest is None:
        return None
    raw_path = manifest.get("plugin_overrides_path")
    if not isinstance(raw_path, str) or not raw_path:
        return None
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = target / candidate
    candidate = candidate.resolve()
    if (candidate / PLUGIN_OVERRIDE_MANIFEST).is_file():
        return candidate
    return None


def _plugin_override_root_manifest_path(
    target: Path, override_root: Path | None
) -> str | None:
    if override_root is None:
        return None
    try:
        return override_root.relative_to(target).as_posix()
    except ValueError:
        return override_root.as_posix()


def _discover_plugin_override_root(
    target: Path,
    *,
    manifest: Mapping[str, object] | None = None,
    plugin_overrides: Path | None = None,
) -> Path | None:
    if plugin_overrides is not None:
        candidate = Path(plugin_overrides).expanduser().resolve()
        manifest_path = candidate / PLUGIN_OVERRIDE_MANIFEST
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"plugin override manifest not found: {manifest_path}"
            )
        return candidate

    manifest_root = _plugin_override_root_from_manifest(target, manifest)
    if manifest_root is not None:
        return manifest_root

    override_root = target.joinpath(*PLUGIN_OVERRIDE_ROOT)
    if (override_root / PLUGIN_OVERRIDE_MANIFEST).is_file():
        return override_root

    # internal: an adopted linked worktree shares the primary's generated
    # plugin trees via the adopt symlink at .workbay/generated, but the
    # (untracked) override root only exists in the primary checkout. Any
    # recompose from the worktree writes THROUGH the symlink into the
    # primary's effective tree, so it must use the primary's overrides —
    # otherwise repair/update silently strip them to passthrough.
    adopt_owner = _adopt_primary_root(target)
    if adopt_owner is not None:
        owner_root = adopt_owner.joinpath(*PLUGIN_OVERRIDE_ROOT)
        if (owner_root / PLUGIN_OVERRIDE_MANIFEST).is_file():
            return owner_root
    return None


def _adopt_primary_root(target: Path) -> Path | None:
    """Resolve the primary repo root when ``target`` is an adopted linked
    worktree whose ``.workbay/generated`` is the adopt-managed symlink into
    the primary. Returns None for a primary (real directory) or an
    unmaterialized worktree."""
    generated = target / RUNTIME_ROOT_DIRNAME / "generated"
    if not generated.is_symlink():
        return None
    resolved = generated.resolve(strict=False)
    owner = resolved.parent.parent
    if owner.resolve() == target.resolve():
        return None
    return owner


def _render_plugin_override_lock(
    override_root: Path,
    remote_sha: str,
    accept_provenance: dict[str, dict[str, str]] | None = None,
) -> str:
    from workbay_protocol.bootstrap import PluginOverrideLock, PluginOverrideManifest

    raw_payload = (
        yaml.safe_load((override_root / PLUGIN_OVERRIDE_MANIFEST).read_text()) or {}
    )
    manifest = PluginOverrideManifest.model_validate(raw_payload)
    components: list[dict[str, object]] = []

    # internal: accept-upstream provenance is durable lock state — re-rendering
    # the lock (every install/update) must carry existing entries forward, and
    # an explicit accept event overrides the carried value for that skill.
    existing_accepts: dict[tuple[str, str], dict[str, str]] = {}
    existing_lock_path = override_root / PLUGIN_OVERRIDE_LOCK
    if existing_lock_path.is_file():
        try:
            existing_payload = json.loads(existing_lock_path.read_text())
        except json.JSONDecodeError:
            existing_payload = {}
        for entry in existing_payload.get("components", []):
            if not isinstance(entry, dict):
                continue
            provenance = entry.get("last_accept_upstream")
            kind = entry.get("component_kind")
            name = entry.get("name")
            if (
                isinstance(provenance, dict)
                and isinstance(kind, str)
                and isinstance(name, str)
            ):
                existing_accepts[(kind, name)] = provenance

    for name, override in sorted(manifest.components.skills.items()):
        entry: dict[str, object] = {
            "component_kind": "skill",
            "name": name,
            "mode": override.mode,
        }
        if override.path is not None:
            entry["local_path"] = override.path
        if override.base_path is not None:
            entry["base_path"] = override.base_path
        if override.upstream_digest is not None:
            entry["upstream_digest"] = override.upstream_digest
        provenance = None
        if accept_provenance is not None:
            provenance = accept_provenance.get(name)
        if provenance is None:
            provenance = existing_accepts.get(("skill", name))
        if provenance is not None:
            entry["last_accept_upstream"] = provenance
        components.append(entry)

    for name, override in sorted(manifest.components.mcp_servers.items()):
        entry = {
            "component_kind": "mcp_server",
            "name": name,
            "mode": override.mode,
        }
        if override.patch_path is not None:
            entry["patch_path"] = override.patch_path
        components.append(entry)

    for name, override in sorted(manifest.components.portable_commands.items()):
        entry = {
            "component_kind": "portable_command",
            "name": name,
            "mode": override.mode,
        }
        if override.path is not None:
            entry["local_path"] = override.path
        components.append(entry)

    # iter4-g: claude_settings_hooks overrides must also be tracked in the
    # override lock, else doctor/repair lock-reconciliation is blind to a
    # consumer-declared settings-hook override (the lock schema already accepts
    # the component_kind; only this emit step was missing).
    for name, override in sorted(manifest.components.claude_settings_hooks.items()):
        entry = {
            "component_kind": "claude_settings_hooks",
            "name": name,
            "mode": override.mode,
        }
        if override.path is not None:
            entry["local_path"] = override.path
        components.append(entry)

    for name, override in sorted(manifest.components.rules.items()):
        entry = {
            "component_kind": "rules",
            "name": name,
            "mode": override.mode,
        }
        if override.path is not None:
            entry["local_path"] = override.path
        components.append(entry)

    for name, override in sorted(manifest.components.guides.items()):
        entry = {
            "component_kind": "guides",
            "name": name,
            "mode": override.mode,
        }
        if override.path is not None:
            entry["local_path"] = override.path
        components.append(entry)

    payload = PluginOverrideLock.model_validate(
        {
            "schema_version": 1,
            "plugin": manifest.plugin,
            "base_remote_sha": remote_sha,
            "components": components,
        }
    ).model_dump(mode="json")
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"




def _import_plugin_override_compose(clone: Path):
    script_dir = str(_resolve_in_clone(clone, "scripts/plugin_override_compose.py").parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    import plugin_override_compose

    return plugin_override_compose


def _apply_doc_surface_overrides(
    target: Path, override_root: Path, clone: Path
) -> None:
    """Copy tracked rules/guides overrides into materialized doc surfaces."""
    from workbay_protocol.bootstrap import PluginOverrideManifest
    from workbay_protocol.paths import DOCS_MIRROR_DIR

    manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    if not manifest_path.is_file():
        return
    raw_payload = yaml.safe_load(manifest_path.read_text()) or {}
    manifest = PluginOverrideManifest.model_validate(raw_payload)
    if not manifest.components.rules and not manifest.components.guides:
        return

    compose = _import_plugin_override_compose(clone)
    if manifest.components.rules:
        compose.compose_doc_surface_overrides(
            target_rules_dir=target / RULES_DIR,
            override_root=override_root,
            manifest=manifest,
            kind="rules",
        )
    if manifest.components.guides:
        compose.compose_doc_surface_overrides(
            target_rules_dir=target / DOCS_MIRROR_DIR / "guides",
            override_root=override_root,
            manifest=manifest,
            kind="guides",
        )


def _write_plugin_override_lock(override_root: Path | None, remote_sha: str) -> None:
    if override_root is None:
        return
    lock_path = override_root / PLUGIN_OVERRIDE_LOCK
    lock_path.write_text(_render_plugin_override_lock(override_root, remote_sha))


def _append_config_entry(entries: list[dict[str, str]], entry: dict[str, str]) -> None:
    """Append a manifest config entry, coalescing duplicate managed paths.

    Entries with distinct ``kind`` values are never coalesced — activation
    receipts (``*_plugin_activation``) may reference the same path as
    materialization rows (``grok_plugin``).
    """
    path = entry.get("path")
    kind = entry.get("kind")
    for existing in entries:
        if existing.get("path") == path and existing.get("kind") == kind:
            prior_action = existing.get("action")
            existing.update({k: v for k, v in entry.items() if k != "path"})
            if prior_action is not None and prior_action != entry.get("action"):
                existing["action"] = "merged"
            return
    entries.append(entry)


def _run_generator(
    target: Path, clone: Path, remote_sha: str, override_root: Path | None = None
) -> None:
    """Invoke the agent-workflow generator against the target.

    Uses the generator + manifest + skills source from the overlay
    clone. Writes per-agent surfaces into the target via the
    generator's ``--target`` convenience flag.
    """
    resolved_target = target.resolve()
    generator_script = _resolve_in_clone(clone, GENERATOR_SCRIPT)
    manifest_path = _resolve_in_clone(clone, GENERATOR_MANIFEST)
    skills_source = _resolve_in_clone(clone, GENERATOR_SKILLS_SOURCE)

    if not generator_script.is_file():
        # Older overlays don't ship the generator. That's acceptable when
        # bootstrapping from a legacy ref; emit nothing rather than fail.
        return

    cmd = [
        sys.executable,
        str(generator_script),
        "--manifest",
        str(manifest_path),
        "--skills-source-root",
        str(skills_source),
        "--target",
        str(resolved_target),
    ]
    if _generator_supports_onboard_router_markers(generator_script):
        cmd.append("--onboard-router-markers")
    if override_root is not None:
        cmd.extend(
            [
                "--plugin-overrides",
                str(override_root),
                "--plugin-base-remote-sha",
                remote_sha,
            ]
        )
    from workbay_bootstrap.external import run_external

    run_external(cmd, call_class="generator", check=True, cwd=str(clone))

    base_plugin_out = _plugin_tree_out(resolved_target, "base")
    effective_plugin_out = _plugin_tree_out(resolved_target, "effective")

    base_plugin_cmd = [
        sys.executable,
        str(generator_script),
        "--mode=plugin",
        "--manifest",
        str(manifest_path),
        "--skills-source-root",
        str(skills_source),
        "--plugin-out",
        str(base_plugin_out),
    ]
    run_external(base_plugin_cmd, call_class="generator", check=True, cwd=str(clone))

    # internal always-effective: compose the effective tree on every run. With
    # an override root it composes base + overrides; without one it emits the
    # base tree unchanged plus a passthrough plugin-lock.json receipt.
    effective_plugin_cmd = [
        sys.executable,
        str(generator_script),
        "--mode=plugin",
        "--manifest",
        str(manifest_path),
        "--skills-source-root",
        str(skills_source),
        "--plugin-out",
        str(effective_plugin_out),
        "--plugin-base-remote-sha",
        remote_sha,
    ]
    if override_root is not None:
        effective_plugin_cmd.extend(["--plugin-overrides", str(override_root)])
    else:
        effective_plugin_cmd.append("--plugin-passthrough-lock")
    run_external(
        effective_plugin_cmd, call_class="generator", check=True, cwd=str(clone)
    )


def _install_lifecycle_profile(target: Path, clone: Path) -> list[dict[str, str]]:
    """Hoist the lifecycle Make fragment + runner into ``target``.

    Each entry in :data:`LIFECYCLE_HOISTS` is resolved against the clone
    (preferring the ``packages/workbay-system/`` layout, falling back to
    a flat layout for hoisted fixture remotes), then copied to the
    consumer at the destination relpath. Files use ``shutil.copy2``;
    directories use ``shutil.copytree`` with ``dirs_exist_ok=True`` so
    re-runs are idempotent. Sources missing in the clone are skipped
    silently for older overlay refs.
    """
    entries: list[dict[str, str]] = []
    for src_rel, dest_rel in LIFECYCLE_HOISTS:
        src = _resolve_in_clone(clone, src_rel)
        if not src.exists():
            continue
        dest = target / dest_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Under ``--profile all``, the shared-overlay materialization may
        # already have linked ``dest`` to the same
        # file inside the clone (Makefile.d/ and scripts/workbay/ ride
        # on the overlay symlink path). When dest already resolves to
        # src, ``shutil.copy2``/``copytree`` would raise
        # ``SameFileError``. Treat the existing symlink as the canonical
        # materialization and record the surface entry without copying.
        if dest.exists() and dest.resolve() == src.resolve():
            entries.append({"path": dest_rel, "source": "lifecycle"})
            continue
        if src.is_dir():
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest)
        entries.append({"path": dest_rel, "source": "lifecycle"})
    return entries


def _makefile_declares_live_include_directive(text: str) -> bool:
    """True when ``-include Makefile.d/*.mk`` exists as a live (non-recipe)
    line — regardless of which sentinel (if any) brackets it. A tab-indented
    line is recipe text echoing the directive, not an include."""
    return any(
        line.strip() == LIFECYCLE_INCLUDE_DIRECTIVE and not line.startswith("\t")
        for line in text.splitlines()
    )


def _migrate_legacy_include_block(text: str) -> str | None:
    """Rewrite the first recognized historical sentinel block to the current
    form, in place. Returns the rewritten text, or ``None`` when no complete
    legacy block is present. Lines between the legacy sentinels are replaced
    wholesale with the canonical directive — the block is bootstrap-managed
    by definition, so any extra lines inside it are managed-block remnants,
    not consumer content."""
    for begin, end in LEGACY_LIFECYCLE_INCLUDE_SENTINELS:
        begin_at = text.find(begin)
        if begin_at == -1:
            continue
        end_at = text.find(end, begin_at)
        if end_at == -1:
            continue
        block_end = end_at + len(end)
        if text[block_end : block_end + 1] == "\n":
            block_end += 1
        replacement = (
            f"{LIFECYCLE_INCLUDE_SENTINEL_BEGIN}\n"
            f"{LIFECYCLE_INCLUDE_DIRECTIVE}\n"
            f"{LIFECYCLE_INCLUDE_SENTINEL_END}\n"
        )
        return text[:begin_at] + replacement + text[block_end:]
    return None


def _ensure_consumer_makefile_include(target: Path) -> dict[str, str] | None:
    """Idempotently inject the lifecycle ``-include`` directive into
    ``<target>/Makefile``.

    Wraps the directive in a sentinel-bracketed block so re-runs don't
    duplicate, and a future uninstall can excise it cleanly. Dedupe is
    fail-closed on the *directive*, not just the sentinel (implementation note): any
    recognized historical sentinel block is migrated in place to the current
    form (``action='migrated'``), and a live directive under an unknown
    marker — or bare — is honored as ``already_present`` so install can
    never append a second ``-include Makefile.d/*.mk``. When the consumer
    already declares lifecycle target names, the file is left untouched so
    bootstrap does not inject a wildcard include that overrides repo-owned
    recipes. When the consumer has no Makefile, one is created containing
    only the sentinel block.
    """
    makefile = target / "Makefile"
    block = (
        f"{LIFECYCLE_INCLUDE_SENTINEL_BEGIN}\n"
        f"{LIFECYCLE_INCLUDE_DIRECTIVE}\n"
        f"{LIFECYCLE_INCLUDE_SENTINEL_END}\n"
    )
    if not makefile.exists():
        makefile.write_text(block)
        return {"path": "Makefile", "action": "created"}
    existing = makefile.read_text()
    if LIFECYCLE_INCLUDE_SENTINEL_BEGIN in existing:
        return {"path": "Makefile", "action": "already_present"}
    migrated = _migrate_legacy_include_block(existing)
    if migrated is not None:
        makefile.write_text(migrated)
        return {"path": "Makefile", "action": "migrated"}
    if _makefile_declares_live_include_directive(existing):
        # Unknown marker (or none) but the directive is live: appending would
        # duplicate the include — the 2026-06-05 incident. Honor it as-is.
        return {"path": "Makefile", "action": "already_present"}
    if _makefile_declares_lifecycle_targets(existing):
        return {"path": "Makefile", "action": "skipped_existing_lifecycle_targets"}
    sep = "" if existing.endswith("\n") else "\n"
    makefile.write_text(existing + sep + block)
    return {"path": "Makefile", "action": "appended"}


def _makefile_declares_lifecycle_targets(text: str) -> bool:
    """Return true when user-owned Makefile text already defines lifecycle recipes."""
    for raw_line in text.splitlines():
        if not raw_line or raw_line[0].isspace():
            continue
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        before, after = line.split(":", 1)
        if after.lstrip().startswith("="):
            continue
        for token in before.split():
            if token in LIFECYCLE_TARGET_NAMES:
                return True
    return False


GITIGNORE_SENTINEL_BEGIN = "# >>> WORKBAY_BOOTSTRAP OVERLAY IGNORE >>>"
GITIGNORE_SENTINEL_END = "# <<< WORKBAY_BOOTSTRAP OVERLAY IGNORE <<<"


def _consumer_gitignore_entries() -> list[str]:
    """Overlay paths an installed/adopted consumer should ignore.

    Root-anchored, NON-trailing-slash patterns (implementation note M1): a trailing slash
    matches only directories, but adopted surfaces are SYMLINKS (non-directories),
    so ``Makefile.d/`` would miss an adopted ``Makefile.d`` symlink. A
    non-trailing-slash pattern matches files, dirs, and symlinks, and a matched
    directory ignores its contents too.

    The tracked marker ``.workbay-bootstrap.json`` and the harness
    marketplace pointers (``HARNESS_PLUGIN_DELIVERY[*]["marketplace"]``, e.g.
    ``.claude-plugin/marketplace.json``) are intentionally NOT ignored.

    Materialized harness plugin trees (``HARNESS_PLUGIN_DELIVERY[*]
    ["materialized"]``, e.g. Grok's) ARE ignored: those harnesses have no
    marketplace indirection, so the full tree is re-materialized from the
    effective tree on every install/update — same regenerable-content rule
    as ``GENERATED_SURFACES``.
    """
    entries = [f"/{RUNTIME_ROOT_DIRNAME}", "/.task-state"]
    entries += [f"/{surface}" for surface in SHARED_SURFACES]
    entries += [f"/{surface}" for surface in GENERATED_SURFACES]
    entries += [f"/{surface}" for surface in _harness_materialized_surfaces()]
    return entries


def _git_path_is_tracked(target: Path, rel: str) -> bool:
    """True when ``rel`` (relative to ``target``) has tracked content in git.

    ``git ls-files --error-unmatch`` exits non-zero when the pathspec matches no
    tracked file; for a tracked directory it matches the tracked children and
    exits 0. Any git failure (non-zero exit, git absent, not a repo) is reported
    as "not tracked" so the conservative default keeps the managed block.
    """
    from workbay_bootstrap.external import ExternalCallTimeout, run_external

    try:
        run_external(
            ["git", "-C", str(target), "ls-files", "--error-unmatch", "--", rel],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        )
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        ExternalCallTimeout,
        FileNotFoundError,
        OSError,
    ):
        return False
    return True


def _git_path_is_ignored(target: Path, rel: str) -> bool:
    """True when git already ignores ``rel`` (relative to ``target``).

    ``git check-ignore -q`` exits 0 when the path is ignored, 1 when not, and 128
    on error. Only exit 0 counts as ignored; anything else (including git absent /
    not a repo) is "not ignored" so the conservative default keeps the block.

    Probes a child path as a fallback: a hand-authored ``.gitignore`` commonly uses
    a directory-only pattern (``.task-state/``, ``/.github/prompts/``) which
    ``check-ignore`` will NOT match against the bare path when that directory does
    not yet exist on disk (the very M1 trailing-slash subtlety the managed block
    avoids). An ignored directory ignores its contents, so a child probe matches a
    ``dir/`` pattern regardless of whether the directory has been materialized.
    """
    from workbay_bootstrap.external import ExternalCallTimeout, run_external

    for probe in (rel, f"{rel}/.workbay-overlay-probe"):
        try:
            proc = run_external(
                ["git", "-C", str(target), "check-ignore", "-q", "--", probe],
                call_class="git",
                capture_output=True,
                text=True,
                check=False,
            )
        except (
            subprocess.TimeoutExpired,
            ExternalCallTimeout,
            FileNotFoundError,
            OSError,
        ):
            return False
        if proc.returncode == 0:
            return True
        if proc.returncode != 1:  # 128 / fatal — be conservative, keep the block
            return False
    return False


def _leaking_overlay_entries(target: Path) -> list[str]:
    """Managed overlay entries that would surface in ``git status`` as untracked —
    i.e. neither tracked nor already ignored by the consumer's own config.

    These are the ONLY entries the managed overlay-ignore block needs, and the
    only ones it is *safe* to ignore. An entry that is already tracked is the
    repo's own source — emitting a root-anchored ``/scripts/hooks`` ignore for it
    would silently make that tracked source un-trackable (the footgun). An entry
    already ignored is redundant. Filtering per-entry (rather than all-or-nothing)
    keeps a MIXED self-host safe: a transiently-leaking new surface still gets an
    ignore line while a sibling tracked-source surface never does.

    Order is preserved from :func:`_consumer_gitignore_entries` for a stable block.
    The caller checks for our own sentinel block first, so this never sees a
    previously-written managed block masking the consumer's own config.
    """
    leaking: list[str] = []
    for entry in _consumer_gitignore_entries():
        rel = entry.lstrip("/")
        if not (_git_path_is_tracked(target, rel) or _git_path_is_ignored(target, rel)):
            leaking.append(entry)
    return leaking


def _overlay_surfaces_self_managed(target: Path) -> bool:
    """True when ``target`` already tracks or ignores every managed overlay
    surface on its own — the self-hosting source repo case (no entry leaks).

    The managed overlay-ignore block exists for ONE reason: keep the adopted
    overlay symlinks out of ``git status``. The workbay monorepo self-hosts the
    overlay — it ships a hand-authored ``.gitignore`` that already ignores the
    very same root surfaces (and a tracked-source layout may even version-control
    them). When nothing leaks, the block is wholly unnecessary, so adopt/install
    skip it (and ``adopt --check`` must not report it as drift). A normal external
    consumer — freshly materialized symlinks, untracked and not yet ignored — has
    leaking surfaces and still gets a block.
    """
    return not _leaking_overlay_entries(target)



def _surface_convergence_candidates() -> tuple[str, ...]:
    return tuple(dict.fromkeys([*SHARED_SURFACES, *GENERATED_SURFACES, RULES_DIR]))


def _dedupe_gitignore_workbay_blocks(text: str) -> tuple[str, bool]:
    if text.count(GITIGNORE_SENTINEL_BEGIN) <= 1:
        return text, False
    first = text.find(GITIGNORE_SENTINEL_BEGIN)
    first_end = text.find(GITIGNORE_SENTINEL_END, first)
    if first_end == -1:
        return text, False
    first_end = first_end + len(GITIGNORE_SENTINEL_END)
    if first_end < len(text) and text[first_end] == "\n":
        first_end += 1
    kept = text[first:first_end]
    tail = text[first_end:]
    while GITIGNORE_SENTINEL_BEGIN in tail:
        nxt = tail.find(GITIGNORE_SENTINEL_BEGIN)
        nxt_end = tail.find(GITIGNORE_SENTINEL_END, nxt)
        if nxt_end == -1:
            break
        nxt_end = nxt_end + len(GITIGNORE_SENTINEL_END)
        if nxt_end < len(tail) and tail[nxt_end] == "\n":
            nxt_end += 1
        tail = tail[:nxt] + tail[nxt_end:]
    return text[:first] + kept + tail, True


def _gc_dangling_bootstrap_symlinks(target: Path) -> list[str]:
    removed: list[str] = []
    for rel in _surface_convergence_candidates():
        path = target / rel
        for candidate in (path, path.parent if rel.count("/") else path):
            if not candidate.is_symlink() or candidate.exists():
                continue
            if _names_clone_subtree(_raw_symlink_target_path(candidate)):
                candidate.unlink()
                removed.append(str(candidate.relative_to(target)))
    return removed


def _hooks_path_resolves_through(target: Path, clone_home: Path) -> bool:
    if not (target / ".git").exists():
        return False
    try:
        hooks_rel = _git("config", "--get", "core.hooksPath", cwd=target)
    except subprocess.CalledProcessError:
        return False
    if not hooks_rel:
        return False
    hooks_path = Path(hooks_rel)
    if not hooks_path.is_absolute():
        hooks_path = target / hooks_path
    return path_resolves_under(hooks_path, clone_home)


def _surface_symlinks_resolve_through(target: Path, clone_home: Path) -> bool:
    clone_home = clone_home.resolve()
    for rel in _surface_convergence_candidates():
        path = target / rel
        if not path.is_symlink():
            continue
        raw = _raw_symlink_target_path(path)
        link_target = Path(raw) if os.path.isabs(raw) else path.parent / raw
        if path_resolves_under(link_target, clone_home):
            return True
    return False


def _deep_symlinks_resolve_through(target: Path, clone_home: Path) -> bool:
    """Recursively detect ANY symlink under ``target`` that resolves into the
    clone, not just the top-level registered surfaces.

    ``_surface_symlinks_resolve_through`` only inspects the known surface
    convergence candidates, so a nested / plugin / operator-authored symlink that
    still points into the clone is missed — and the clone would then be
    ``rmtree``'d by the auto-contract or ``clean/gc`` while still load-bearing
    (BA-2). Symlinks located *inside* the clone are internal to it (a git clone
    can carry its own symlinks) and are excluded, so a genuinely orphaned clone
    is still reclaimable.
    """
    # Location of the clone-home entry itself (without following it, so a
    # *symlinked* clone home is not counted as resolving into its own target).
    home_loc = clone_home.parent.resolve() / clone_home.name
    clone_home = clone_home.resolve()
    for path in target.rglob("*"):
        if ".git" in path.parts:
            continue
        if not path.is_symlink():
            continue
        location = path.parent.resolve() / path.name
        if location == home_loc:
            continue  # the clone-home entry itself is not an external resolver
        try:
            location.relative_to(clone_home)
            continue  # link lives inside the clone — internal, not load-bearing
        except ValueError:
            pass
        raw = _raw_symlink_target_path(path)
        link_target = Path(raw) if os.path.isabs(raw) else path.parent / raw
        if path_resolves_under(link_target, clone_home):
            return True
    return False


def _still_resolves_through_clone(target: Path, clone_home: Path) -> bool:
    return (
        _hooks_path_resolves_through(target, clone_home)
        or _surface_symlinks_resolve_through(target, clone_home)
        or _deep_symlinks_resolve_through(target, clone_home)
    )


still_resolves_through_clone = _still_resolves_through_clone


def _contract_orphaned_clone_homes(target: Path) -> None:
    for clone_home in overlay_clone_homes(target):
        if not clone_home.exists():
            continue
        if _still_resolves_through_clone(target, clone_home):
            continue
        shutil.rmtree(clone_home)


def converge_partial_overlay_state(target: Path) -> list[dict[str, str]]:
    """Migrate stale managed blocks and GC dangling bootstrap symlinks before install."""
    actions: list[dict[str, str]] = []
    gitignore = target / ".gitignore"
    if gitignore.is_file():
        existing = gitignore.read_text()
        updated, deduped = _dedupe_gitignore_workbay_blocks(existing)
        if deduped:
            gitignore.write_text(updated)
            actions.append({"path": ".gitignore", "action": "converged_sentinel"})
    for rel in _gc_dangling_bootstrap_symlinks(target):
        actions.append({"path": rel, "action": "gc_dangling_symlink"})
    return actions


def _ensure_consumer_gitignore_block(target: Path) -> dict[str, str]:
    """Idempotently maintain a sentinel-delimited overlay-ignore block in
    ``<target>/.gitignore``.

    Mirrors :func:`_ensure_consumer_makefile_include`: wraps the managed entries
    in a sentinel-bracketed block so re-runs don't duplicate and a future
    uninstall can excise it cleanly, and never clobbers a user-authored
    ``.gitignore`` (the block is appended, preserving prior content). Keeps an
    installed/adopted overlay out of ``git status``. Returns the action taken
    (``created`` / ``appended`` / ``updated`` / ``already_present`` /
    ``skipped_self_managed``).

    Only the *leaking* managed entries are emitted (see
    :func:`_leaking_overlay_entries`): a tracked surface is never ignored (closing
    the self-hosting-source footgun even in a MIXED layout), and an already-ignored
    surface is not duplicated. When nothing leaks the whole block is skipped.

    A pre-existing block is RECONCILED, not trusted: when a managed entry leaks
    even though the sentinel block exists (a surface added to the managed lists
    after the block was first written — e.g. the Grok plugin tree), the missing
    entries are appended inside the sentinels (``updated``). Without this,
    upgrading consumers keep a stale block and the new surface leaks into
    ``git status`` forever.
    """
    leaking = _leaking_overlay_entries(target)
    gitignore = target / ".gitignore"
    existing = gitignore.read_text() if gitignore.exists() else None
    has_block = existing is not None and GITIGNORE_SENTINEL_BEGIN in existing
    if not leaking:
        if has_block:
            return {"path": ".gitignore", "action": "already_present"}
        return {"path": ".gitignore", "action": "skipped_self_managed"}
    if has_block and existing is not None:
        # Stale block: reconcile by inserting the leaking entries that the file
        # does not already carry before the END sentinel (line-set membership,
        # not git state — git probes degrade conservatively outside a repo and
        # would otherwise re-insert forever). Fall through to append a fresh
        # block only when the END sentinel is missing (hand-edited / corrupt).
        end = existing.find(GITIGNORE_SENTINEL_END)
        if end != -1:
            present = {line.strip() for line in existing.splitlines()}
            missing = [entry for entry in leaking if entry not in present]
            if not missing:
                return {"path": ".gitignore", "action": "already_present"}
            insert = "\n".join(missing) + "\n"
            if end > 0 and existing[end - 1] != "\n":
                insert = "\n" + insert
            gitignore.write_text(existing[:end] + insert + existing[end:])
            return {"path": ".gitignore", "action": "updated"}
    block = (
        GITIGNORE_SENTINEL_BEGIN
        + "\n"
        + "\n".join(leaking)
        + "\n"
        + GITIGNORE_SENTINEL_END
        + "\n"
    )
    if existing is None:
        gitignore.write_text(block)
        return {"path": ".gitignore", "action": "created"}
    sep = "" if existing.endswith("\n") else "\n"
    gitignore.write_text(existing + sep + block)
    return {"path": ".gitignore", "action": "appended"}


def install(
    *,
    target: Path,
    remote_url: str | None = None,
    remote_ref: str | None = None,
    source: str = "git_overlay",
    package_root: Path | None = None,
    mcp_servers: Mapping[str, Mapping[str, Any]] | str | None = None,
    plugin_overrides: Path | None = None,
    reset_overrides: bool = False,
    backup_overrides: bool = False,
    enforce_required_surfaces: bool = False,
    profile: str = PROFILE_ALL,
    install_claude_stop_hook_local: bool = False,
    install_codex_stop_hook: bool = False,
    install_vscode_stop_hook: bool = False,
    install_grok_stop_hook: bool = False,
    install_claude_reinject_hook_local: bool = False,
    no_embeddings: bool = False,
) -> dict[str, object]:
    """Clone the shared workbay-system remote, materialize overlay surfaces,
    write consumer-tool configs, and write the overlay manifest.

    Args:
        target: Consumer repository root. Must already exist.
        remote_url: Git URL for the shared workbay-system remote.
        remote_ref: Tag, branch, or SHA to check out (e.g. ``"v0.1.0"``).
        mcp_servers: Mapping of ``<server_name> -> {command, args, env}`` to
            register in ``.mcp.json``, ``.vscode/mcp.json``, and
            ``.codex/config.toml``. Pass the sentinel string ``"default"``
            to use :data:`DEFAULT_MCP_SERVERS` (the two MCP servers shipped
            by this monorepo). When ``None``, the three file-writers are
            skipped. ``core.hooksPath`` is set independently whenever the
            target is a git repo.
        plugin_overrides: Optional explicit plugin override root. When set,
            bootstrap composes the effective plugin tree from this root and
            records the resolved path in the manifest for later doctor /
            update / repair runs.
        reset_overrides: When True, remove the resolved plugin override root
            before regeneration. Marketplace pins still target the effective
            plugin tree; with no overrides it is recomposed as a passthrough
            copy of base.
        backup_overrides: When True together with ``reset_overrides``, archive
            the override root under ``.workbay/override-backups/<timestamp>/``
            before removal.
        enforce_required_surfaces: When True, refuse the install if any
            surface declared as required by the manifest fails to
            materialize. Defaults to False (warn-only).
        profile: Install profile selecting how much overlay surface to
            materialize. One of :data:`PROFILE_MINIMAL`,
            :data:`PROFILE_LIFECYCLE`, or :data:`PROFILE_ALL` (default).
        install_claude_stop_hook_local: When True, write the user-owned,
            gitignored Claude stop-hook wiring at
            ``.claude/settings.local.json``. Off by default.
        install_codex_stop_hook: When True, write the Codex CLI harness
            stop-hook wiring at ``.codex/hooks/stop.json``. Off by default.
        install_vscode_stop_hook: When True, write the VS Code harness
            stop-hook wiring at ``.vscode/workbay-stop-hooks.json``. Off
            by default.
        install_grok_stop_hook: When True, write the Grok harness stop-hook
            wiring at ``.grok/hooks/stop.json``. Off by default.
        install_claude_reinject_hook_local: When True, write the user-owned,
            gitignored Claude SessionStart re-injection wiring at
            ``.claude/settings.local.json``. Off by default.

    Returns:
        The manifest dict that was written to ``<target>/.workbay-bootstrap.json``.

    Raises:
        FileNotFoundError: ``target`` does not exist.
        FileExistsError: ``<target>/.workbay/remote`` exists but is not a git clone.
        RemoteUrlMismatchError: existing clone tracks a different ``origin`` URL.
        subprocess.CalledProcessError: ``git`` command failed.
    """
    from workbay_bootstrap.install_plan import InstallRequest, run_install

    manifest = run_install(
        InstallRequest.from_install_kwargs(
            target=target,
            remote_url=remote_url,
            remote_ref=remote_ref,
            source=source,
            package_root=package_root,
            mcp_servers=mcp_servers,
            plugin_overrides=plugin_overrides,
            reset_overrides=reset_overrides,
            backup_overrides=backup_overrides,
            enforce_required_surfaces=enforce_required_surfaces,
            profile=profile,
            install_claude_stop_hook_local=install_claude_stop_hook_local,
            install_codex_stop_hook=install_codex_stop_hook,
            install_vscode_stop_hook=install_vscode_stop_hook,
            install_grok_stop_hook=install_grok_stop_hook,
            install_claude_reinject_hook_local=install_claude_reinject_hook_local,
        )
    )
    import sys

    from workbay_bootstrap.install_embeddings_consent import apply_install_embeddings_consent

    interactive = sys.stdin.isatty()
    for line in apply_install_embeddings_consent(
        target,
        interactive=interactive,
        no_embeddings=no_embeddings,
    ):
        print(line, file=sys.stderr)
    return manifest


# ---------------------------------------------------------------------------
# Config writers
# ---------------------------------------------------------------------------


HOOKS_PATH_VALUE = "scripts/hooks/git"
"""Workspace-relative ``core.hooksPath`` value.

Points at the ``git/`` subdirectory of the materialized ``scripts/hooks``
surface. The parent directory ships Python helpers, ``.sh`` utilities,
and tests alongside the actual hook scripts; pointing git at the parent
makes git look for hook files by name (``post-checkout`` etc.) at a path
where they do not exist, so it silently resolves nothing. The named
hooks themselves live at ``scripts/hooks/git/<name>``.

Single-line invariant: the on-disk hook layout and this value MUST agree.
The install rehearsal pins both halves of that contract.
"""


def _write_configs(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
    *,
    include_hooks: bool = True,
    receipt: Any | None = None,
) -> list[dict[str, str]]:
    """Run post-install config writers and return per-surface entries
    suitable for ``manifest['configs']``.

    The MCP config file writers run only when ``mcp_servers`` is provided. The git
    ``core.hooksPath`` writer runs whenever the target looks like a git repo.

    internal (S6-01): when an :class:`InstallReceipt` is supplied,
    each MCP config writer records a per-surface ``config_<harness>``
    StepReceipt and a single writer failure no longer aborts the remaining
    surfaces (bulkhead) — the failure lands in ``install_steps`` where
    doctor's existing receipt path surfaces it. Without a receipt the legacy
    raise-through behavior is preserved.
    """
    entries: list[dict[str, str]] = []

    if mcp_servers:
        writers: tuple[tuple[str, Any], ...] = (
            ("config_claude", _write_mcp_json),
            ("config_vscode", _write_vscode_mcp_json),
            ("config_codex", _write_codex_config),
            ("config_cursor", _write_cursor_mcp_json),
        )
        for step_name, writer in writers:
            if receipt is None:
                entries.append(writer(target, mcp_servers))
                continue
            try:
                entries.append(writer(target, mcp_servers))
            except Exception as exc:  # bulkhead: classify, record, continue
                receipt.failed(
                    step_name,
                    reason=str(exc),
                    failure_class="system"
                    if isinstance(exc, OSError)
                    else "application",
                    criticality="continue",
                )
            else:
                receipt.ok(step_name)

    if include_hooks:
        hooks_entry = _set_git_hooks_path(target)
        if hooks_entry is not None:
            entries.append(hooks_entry)

    return entries


def _run_init_state(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
    *,
    expected_remote_url: str | None = None,
) -> None:
    if not mcp_servers:
        return

    spec = mcp_servers.get("workbay-handoff-mcp")
    if spec is None:
        return

    command = spec.get("command")
    if not isinstance(command, str) or not command:
        raise ValueError("workbay-handoff-mcp config must include a non-empty command")

    raw_args = spec.get("args", [])
    if not isinstance(raw_args, list) or not all(
        isinstance(arg, str) for arg in raw_args
    ):
        raise ValueError("workbay-handoff-mcp config args must be a list[str]")

    args = list(raw_args)
    raw_env = spec.get("env")
    env_has_state_dir = (
        isinstance(raw_env, Mapping) and "WORKBAY_HANDOFF_STATE_DIR" in raw_env
    )
    if args and args[-1] in {"serve-stdio", "serve-http", "init-state"}:
        args = args[:-1]
    if not any(
        arg == "--workspace-root" or arg.startswith("--workspace-root=") for arg in args
    ):
        args.extend(["--workspace-root", str(target)])
    if (
        not any(arg == "--state-dir" or arg.startswith("--state-dir=") for arg in args)
        and not env_has_state_dir
    ):
        args.extend(["--state-dir", str(target / ".task-state")])
    args.append("init-state")
    if expected_remote_url is not None:
        args.extend(["--expected-remote-url", expected_remote_url])

    cmd = [command, *args]
    env = os.environ.copy()
    if raw_env is not None:
        if not isinstance(raw_env, Mapping) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in raw_env.items()
        ):
            raise ValueError(
                "workbay-handoff-mcp config env must be a mapping[str, str]"
            )
        env.update(raw_env)

    from workbay_bootstrap.external import run_external

    try:
        run_external(
            cmd,
            call_class="handoff_cli",
            check=True,
            capture_output=True,
            text=True,
            cwd=str(target),
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        retry_cmd = _build_local_handoff_retry_cmd(target, cmd)
        if retry_cmd is None:
            raise
        try:
            run_external(
                retry_cmd,
                call_class="handoff_cli",
                check=True,
                capture_output=True,
                text=True,
                cwd=str(target),
                env=env,
            )
        except subprocess.CalledProcessError as retry_exc:
            raise retry_exc from exc


def _canonicalize_managed_servers(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    """Enforce launch invariants at the render seam, independent of how the
    server map was resolved (install/update/repair/mcp-sync all funnel
    through the renderers). implementation note A1: every managed local
    ``uv run --project`` launcher must pass ``--no-sync`` so server startup
    never re-resolves on the hot path. Idempotent; non-local specs pass
    through unchanged."""
    canonical: dict[str, Mapping[str, Any]] = {}
    target_root = target.resolve()
    for name, spec in mcp_servers.items():
        args = spec.get("args")
        if not (
            spec.get("command") == "uv"
            and isinstance(args, list)
            and args[:1] == ["run"]
            and "--project" in args
            and "--no-sync" not in args
        ):
            canonical[name] = spec
            continue

        project_index = args.index("--project") + 1
        if project_index >= len(args):
            canonical[name] = spec
            continue

        project = (target_root / args[project_index]).resolve()
        try:
            project.relative_to(target_root)
        except ValueError:
            canonical[name] = spec
            continue

        # Eligible managed local launcher: inject --no-sync. The pyproject.toml
        # existence check that _local_uv_project_from_spec / _presync_local_mcp_envs
        # apply is intentionally omitted here — the seam canonicalizes the
        # launch command at write time, independent of whether the env has been
        # built yet, so a fresh install's not-yet-synced launcher still lands
        # contention-free. implementation note A1.
        canonical[name] = {**spec, "args": [args[0], "--no-sync", *args[1:]]}
    return canonical


def _render_mcp_json(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> bytes:
    """Pure render half of the .mcp.json seam: read the existing file
    (if any), deep-merge managed servers under ``mcpServers``, and return
    the bytes that ``_write_mcp_json`` would persist. No filesystem
    mutation.

    ``prune_names`` are server names to remove from the existing
    ``mcpServers`` block before the merge — driven by
    ``sync_mcp_configs(prune_removed_managed=True)`` reading the
    ledger's previously-managed provenance. Default ``()`` keeps the
    install path's behavior byte-identical."""
    path = target / ".mcp.json"
    doc: dict[str, Any] = _load_json_or_empty(path)
    drop_names = tuple(prune_names)
    if drop_names:
        servers = doc.get("mcpServers")
        if isinstance(servers, dict):
            for name in drop_names:
                servers.pop(name, None)
    mcp_servers = _canonicalize_managed_servers(target, mcp_servers)
    incoming = {"mcpServers": {name: dict(spec) for name, spec in mcp_servers.items()}}
    _deep_merge(doc, incoming)
    return (json.dumps(doc, indent=2) + "\n").encode("utf-8")


def _load_json_or_empty(path: Path) -> dict[str, Any]:
    """Return parsed JSON or ``{}`` when the file is missing or malformed.

    Managed surfaces are this tool's own output. If the file is invalid
    JSON (interrupted prior write, hand edit), treat it as empty so the
    next reconcile rewrites it cleanly instead of letting JSONDecodeError
    escape through doctor / mcp-sync. Third-party preservation is
    impossible in that case (the existing content is already lost).
    """
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _write_mcp_json(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> dict[str, str]:
    """Deep-merge managed servers into ``<target>/.mcp.json`` under
    ``mcpServers``. Preserves all other keys and other servers."""
    path = target / ".mcp.json"
    existed = path.exists()
    rendered = _render_mcp_json(target, mcp_servers, prune_names=prune_names)
    path.write_bytes(rendered)
    return {"path": ".mcp.json", "action": "merged" if existed else "created"}


def _render_cursor_mcp_json(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> bytes:
    path = target / ".cursor" / "mcp.json"
    doc: dict[str, Any] = _load_json_or_empty(path)
    drop_names = tuple(prune_names)
    if drop_names:
        servers = doc.get("mcpServers")
        if isinstance(servers, dict):
            for name in drop_names:
                servers.pop(name, None)
    mcp_servers = _canonicalize_managed_servers(target, mcp_servers)
    incoming = {"mcpServers": {name: dict(spec) for name, spec in mcp_servers.items()}}
    _deep_merge(doc, incoming)
    return (json.dumps(doc, indent=2) + "\n").encode("utf-8")


def _write_cursor_mcp_json(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> dict[str, str]:
    """Deep-merge managed servers into ``<target>/.cursor/mcp.json``."""
    path = target / ".cursor" / "mcp.json"
    existed = path.exists()
    rendered = _render_cursor_mcp_json(target, mcp_servers, prune_names=prune_names)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(rendered)
    return {"path": ".cursor/mcp.json", "action": "merged" if existed else "created"}


def _render_vscode_mcp_json(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> bytes:
    """Pure render half of the .vscode/mcp.json seam: read the existing
    file (if any), deep-merge managed servers under ``servers``, and
    return the bytes that ``_write_vscode_mcp_json`` would persist. No
    filesystem mutation (the ``.vscode/`` directory is created by the
    write half).

    ``prune_names`` removes those entries from the existing ``servers``
    block before the merge; see ``_render_mcp_json`` for the contract."""
    path = target / ".vscode" / "mcp.json"
    doc: dict[str, Any] = _load_json_or_empty(path)
    drop_names = tuple(prune_names)
    if drop_names:
        servers = doc.get("servers")
        if isinstance(servers, dict):
            for name in drop_names:
                servers.pop(name, None)
    mcp_servers = _canonicalize_managed_servers(target, mcp_servers)
    incoming = {"servers": {name: dict(spec) for name, spec in mcp_servers.items()}}
    _deep_merge(doc, incoming)
    return (json.dumps(doc, indent=2) + "\n").encode("utf-8")


def _write_vscode_mcp_json(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> dict[str, str]:
    """Deep-merge managed servers into ``<target>/.vscode/mcp.json`` under
    ``servers``. Creates the ``.vscode`` directory if absent."""
    path = target / ".vscode" / "mcp.json"
    existed = path.exists()
    rendered = _render_vscode_mcp_json(target, mcp_servers, prune_names=prune_names)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(rendered)
    return {"path": ".vscode/mcp.json", "action": "merged" if existed else "created"}


def _render_codex_config(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> bytes:
    """Pure render half of the .codex/config.toml seam: read the existing
    TOML (if any), replace the ``[mcp_servers.<name>]`` tables for each
    managed server while preserving every other key and comment, and
    return the bytes that ``_write_codex_config`` would persist. No
    filesystem mutation (the ``.codex/`` directory is created by the
    write half).

    ``prune_names`` removes those tables from ``[mcp_servers]`` before
    the managed tables are added; see ``_render_mcp_json`` for the
    contract."""
    path = target / ".codex" / "config.toml"
    if path.exists():
        try:
            doc = tomlkit.parse(path.read_text())
        except (tomlkit.exceptions.TOMLKitError, UnicodeDecodeError):
            doc = tomlkit.document()
    else:
        doc = tomlkit.document()

    if "mcp_servers" not in doc:
        doc["mcp_servers"] = tomlkit.table(is_super_table=True)
    servers_table = doc["mcp_servers"]

    drop_names = tuple(prune_names)
    for name in drop_names:
        if name in servers_table:
            del servers_table[name]

    mcp_servers = _canonicalize_managed_servers(target, mcp_servers)
    for name, spec in mcp_servers.items():
        new_table = tomlkit.table()
        for spec_key, spec_value in spec.items():
            new_table[spec_key] = spec_value
        servers_table[name] = new_table

    return tomlkit.dumps(doc).encode("utf-8")


def _write_codex_config(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: Iterable[str] = (),
) -> dict[str, str]:
    """Replace the ``[mcp_servers.<name>]`` tables in
    ``<target>/.codex/config.toml`` for each managed server, leaving every
    other root key, table, and comment untouched (tomlkit round-trip)."""
    path = target / ".codex" / "config.toml"
    existed = path.exists()
    rendered = _render_codex_config(target, mcp_servers, prune_names=prune_names)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(rendered)
    return {"path": ".codex/config.toml", "action": "merged" if existed else "created"}


def _set_git_hooks_path(target: Path) -> dict[str, str] | None:
    """If ``target`` is a git repo, set ``core.hooksPath`` to
    ``scripts/hooks/git`` (under the materialized ``scripts/hooks``
    symlink) and return a manifest entry. Otherwise return ``None``
    (silent skip).

    In a *linked worktree* (``.git`` is a file, not a directory) the value is
    written **worktree-locally** via ``extensions.worktreeConfig`` +
    ``git config --worktree`` so adopting or repairing a worktree never mutates
    the primary's shared ``core.hooksPath`` (implementation note isolation invariant).

    See ``HOOKS_PATH_VALUE`` for why the path includes the ``git/``
    subdirectory.
    """
    dot_git = target / ".git"
    if not dot_git.exists():
        return None
    if dot_git.is_file():
        # Linked worktree: scope the write to this worktree's config only.
        _git("config", "extensions.worktreeConfig", "true", cwd=target)
        _git("config", "--worktree", "core.hooksPath", HOOKS_PATH_VALUE, cwd=target)
        return {"path": "core.hooksPath", "action": "set", "scope": "worktree"}
    _git("config", "core.hooksPath", HOOKS_PATH_VALUE, cwd=target)
    return {"path": "core.hooksPath", "action": "set"}


# Manifest-driven hook walker.
#
# ``portable_commands.json`` (schema v2) is the single source of truth
# for the per-harness adapter rows that materialize bootstrap-owned
# hooks. The walker reads the manifest from the cloned overlay, filters
# by install profile and the active set of opt-in flags, verifies the
# hook's ``required_artifacts`` exist in the clone, and dispatches each
# selected adapter on its ``patch.operation``. The previous single-harness
# writer (``_write_claude_settings_hooks``) is replaced by this table-driven
# walk so new harnesses (Codex, VS Code, etc.) can be
# added by appending adapter rows to the manifest rather than by
# growing bespoke writers in this module.
#
# Adapter target strings are NEVER hardcoded here — every ``.claude/...``
# / ``.codex/...`` path comes from the manifest. The walker only knows
# how to dispatch operations.

_TEMPLATE_CONSUMER_ROOT = "{{consumer_root}}"


def _load_portable_manifest(clone: Path) -> dict[str, Any]:
    """Read the v2 portable-commands manifest out of the clone.

    Returns ``{}`` when the manifest is absent (older overlays that
    predate schema v2) so the walker becomes a noop instead of raising.
    """
    manifest_path = _resolve_in_clone(clone, GENERATOR_MANIFEST)
    if not manifest_path.is_file():
        return {}
    return json.loads(manifest_path.read_text())


def _render_template(value: Any, *, target: Path) -> Any:
    """Recursively substitute ``{{consumer_root}}`` with the resolved
    consumer-root path inside the adapter ``entry`` template."""
    if isinstance(value, str):
        return value.replace(_TEMPLATE_CONSUMER_ROOT, str(target))
    if isinstance(value, Mapping):
        return {k: _render_template(v, target=target) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_template(v, target=target) for v in value]
    return value


def _resolve_dotted_path(
    doc: dict[str, Any], json_path: str
) -> tuple[dict[str, Any], str]:
    """Resolve a closed-set JSONPath like ``$.hooks.Stop`` to the parent
    container and the leaf key, creating intermediate dicts as needed.

    The walker only dispatches array-merge patches today, so this parser
    is intentionally narrow: ``$.<seg>(.<seg>)*`` with object-keyed
    segments. Anything else raises ``ValueError`` rather than silently
    accepting a path the dispatcher can't honour.
    """
    if not json_path.startswith("$."):
        raise ValueError(f"unsupported json_path {json_path!r}; must start with '$.'")
    segments = json_path[2:].split(".")
    if not segments or not all(segments):
        raise ValueError(f"unsupported json_path {json_path!r}; empty segment")
    parent: dict[str, Any] = doc
    for seg in segments[:-1]:
        nxt = parent.setdefault(seg, {})
        if not isinstance(nxt, dict):
            raise ValueError(
                f"refusing to merge: {seg!r} along {json_path!r} is not an object"
            )
        parent = nxt
    return parent, segments[-1]


def _apply_merge_array_entry(
    adapter: Mapping[str, Any], *, target: Path
) -> dict[str, str]:
    """Idempotently merge a managed entry into an array container in a
    JSON settings file. ``match_key`` identifies prior managed entries
    for replacement-in-place; everything else is preserved verbatim.
    """
    patch = adapter["patch"]
    target_rel = adapter["target"]
    settings_path = target / target_rel
    settings_path.parent.mkdir(parents=True, exist_ok=True)

    existed = settings_path.exists()
    doc: dict[str, Any] = json.loads(settings_path.read_text()) if existed else {}
    if not isinstance(doc, dict):
        raise ValueError(
            f"refusing to merge hook into {settings_path}: "
            "existing JSON document is not an object"
        )

    parent, leaf_key = _resolve_dotted_path(doc, patch["json_path"])
    array_raw = parent.get(leaf_key, [])
    if not isinstance(array_raw, list):
        raise ValueError(
            f"refusing to merge hook into {settings_path}: "
            f"{patch['json_path']!r} is not a list"
        )

    managed_entry = patch["entry"]
    match_key = patch["match_key"]
    if not isinstance(managed_entry, Mapping) or match_key not in managed_entry:
        raise ValueError(
            f"adapter entry missing match_key {match_key!r}: {managed_entry!r}"
        )
    match_value = managed_entry[match_key]

    new_array: list[Any] = []
    replaced = False
    matched_existing = False
    for item in array_raw:
        if isinstance(item, Mapping) and item.get(match_key) == match_value:
            matched_existing = True
            if not replaced:
                new_array.append(managed_entry)
                replaced = True
            continue
        new_array.append(item)
    if not replaced:
        new_array.append(managed_entry)

    parent[leaf_key] = new_array
    settings_path.write_text(json.dumps(doc, indent=2) + "\n")

    if not existed:
        action = "created"
    elif matched_existing:
        action = "noop"
    else:
        action = "merged"
    return {"path": target_rel, "action": action}


_ADAPTER_DISPATCH: dict[str, Any] = {
    "merge_array_entry": _apply_merge_array_entry,
}


def _walk_hook_adapters(
    *,
    manifest: Mapping[str, Any],
    clone: Path,
    target: Path,
    profile: str,
    active_flags: set[str],
) -> list[dict[str, str]]:
    """Walk ``manifest.hooks`` and apply each adapter whose opt_in_flag is
    in ``active_flags``. Hooks whose ``profiles`` do not include the
    active install profile are skipped. When at least one adapter is
    selected, every ``required_artifacts`` row is verified to exist in
    the clone before any file is touched — opting in to a hook whose
    artifacts are missing is a hard fail."""
    configs: list[dict[str, str]] = []
    if not isinstance(manifest, Mapping):
        return configs
    hooks = manifest.get("hooks")
    if not isinstance(hooks, list):
        return configs

    for hook in hooks:
        if not isinstance(hook, Mapping):
            continue
        hook_profiles = hook.get("profiles") or []
        if profile not in hook_profiles and "all" not in hook_profiles:
            continue
        adapters = hook.get("adapters") or []
        selected = [
            a
            for a in adapters
            if isinstance(a, Mapping) and a.get("opt_in_flag") in active_flags
        ]
        if not selected:
            continue
        # Required-artifacts gate: refuse to silently skip a user-requested
        # hook just because the overlay clone is missing the script.
        for artifact in hook.get("required_artifacts") or []:
            consumer_path = (
                artifact.get("consumer_path") if isinstance(artifact, Mapping) else None
            )
            if not consumer_path:
                continue
            resolved = _resolve_in_clone(clone, consumer_path)
            if not resolved.is_file():
                raise RuntimeError(
                    f"hook {hook.get('hook_id')!r}: required artifact "
                    f"{consumer_path!r} is missing in the overlay clone "
                    f"(expected at {resolved}). Cannot honour the opt-in."
                )
        for adapter in selected:
            op = adapter["patch"]["operation"]
            handler = _ADAPTER_DISPATCH.get(op)
            if handler is None:
                raise NotImplementedError(
                    f"hook {hook.get('hook_id')!r}: unknown adapter "
                    f"patch operation {op!r}"
                )
            row = handler(adapter, target=target)
            # internal: tag the row so doctor/update can tell a
            # hook-adapter config apart from unrelated rows sharing the
            # same path (e.g. `.claude/settings.json` from the settings
            # writer) and so `update` can re-derive the opt-in flags.
            row["kind"] = "hook_adapter"
            row["opt_in_flag"] = str(adapter.get("opt_in_flag"))
            configs.append(row)
            # internal: Claude only auto-ignores `.claude/settings.local.json`
            # when it created the file itself, so a workbay-written
            # user-owned-local Claude settings file must get its .gitignore
            # entry from us — otherwise an opt-in `*-hook-local` install (with
            # no residual migration to trigger the wholesale writer's own
            # gitignore step) leaves a tracked, un-ignored local file. Keyed off
            # the manifest write_kind/harness rather than a hardcoded path so
            # install.py stays free of settings-path literals.
            if (
                adapter.get("harness") == "claude-code"
                and adapter.get("write_kind") == "user_owned_local"
            ):
                from workbay_bootstrap.claude_settings import (
                    ensure_claude_settings_local_gitignore,
                )

                configs.append(ensure_claude_settings_local_gitignore(target))
    return configs
