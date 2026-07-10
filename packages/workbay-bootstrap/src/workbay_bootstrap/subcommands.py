"""Post-install subcommands: status / doctor / update / repair.

Each subcommand is a small library function with a clear contract:

- ``status(target)`` returns a human-readable summary of the overlay manifest.
- ``doctor(target, mcp_servers=None)`` returns a list of drift findings.
- ``update(target, remote_ref, ...)`` (future slice) re-runs install at a new ref.
- ``repair(target, ..., force_dirty)`` (future slice) rewrites drifted overlays.

The CLI in ``workbay_bootstrap.cli`` is a thin argparse wrapper over these.
"""

from __future__ import annotations

import json
import os
import tomllib
from datetime import UTC, datetime, timedelta
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Mapping

import yaml
from workbay_protocol import RUNTIME_ROOT_DIRNAME

from workbay_bootstrap.install import (
    BOOTSTRAP_MANIFEST_NAME,
    CLAUDE_MARKETPLACE_PATH,
    CLONE_RELPATH,
    CLONE_SUBDIR,
    CODEX_CONFIG_PATH,
    CODEX_MARKETPLACE_PATH,
    GENERATOR_MANIFEST,
    GENERATOR_SCRIPT,
    GENERATOR_SKILLS_SOURCE,
    GROK_PLUGIN_DEST,
    PLUGIN_NAME,
    PLUGIN_GENERATED_ROOT,
    PLUGIN_MARKETPLACE_NAME,
    PLUGIN_OVERRIDE_LOCK,
    PLUGIN_OVERRIDE_MANIFEST,
    PLUGIN_OVERRIDE_ROOT,
    RULES_DIR,
    _discover_plugin_override_root,
    _plugin_tree_out,
    _relative_plugin_tree_path,
    _resolve_in_clone,
    SURFACE_CHILD_EXCLUSIONS,
    _migrate_legacy_manifest,
    _names_clone_subtree,
    _package_source_root,
    self_host_payload_root,
    _stack_member_drift_findings,
)
from workbay_bootstrap.harnesses import (
    CURSOR_COMMANDS_DEST,
    CURSOR_HOOKS_PATH,
    CURSOR_SKILLS_DEST,
    cursor_native_surface_problems,
)
from workbay_bootstrap.surfaces import surfaces_for_kind
from workbay_bootstrap.worktree import (
    WorktreeError,
    is_linked_worktree,
    overlay_is_materialized,
    primary_overlay_root,
)

_GENERATOR_INPUT_SURFACES = frozenset(surfaces_for_kind("generator_input"))


def _is_under(surface: str, child: str) -> bool:
    return child == surface or child.startswith(surface + "/")


def _rel_under(surface: str, child: str) -> str:
    if child == surface:
        return ""
    return child[len(surface) + 1 :]


def _unadopted_worktree_primary(target: Path) -> Path | None:
    """Return the primary overlay root when ``target`` is a linked worktree whose
    overlay has not been adopted yet, else ``None``.

    Adoption state is keyed on the CLONE redirect (``.workbay/remote``), not the
    marker: the ``.workbay-bootstrap.json`` marker is typically tracked and so
    survives into a fresh worktree, while the gitignored clone does not. A
    non-git, primary, or already-adopted target returns ``None`` so the regular
    doctor checks proceed untouched.
    """
    try:
        if not is_linked_worktree(target):
            return None
        if target.joinpath(*CLONE_SUBDIR).exists():
            return None  # clone redirect present -> already adopted
        primary = primary_overlay_root(target)
    except WorktreeError:
        return None
    if primary != target and overlay_is_materialized(primary):
        return primary
    return None


def _is_selfhost_worktree(target: Path) -> bool:
    """True when ``target`` is a linked worktree of a *self-host* primary.

    Self-host means the primary ships the in-tree payload
    (``packages/workbay-system/workbay_system/payload``) instead of a
    ``.workbay/remote`` clone — the ``no_overlay_clone`` shape where consumer
    ``adopt-worktree`` is skipped, so a linked worktree carries the tracked
    payload but none of the generated surfaces. Distinguished from
    :func:`_unadopted_worktree_primary` (which requires a *materialized* primary
    with a clone) by the worktree shipping the payload and having no clone.
    """
    try:
        if not is_linked_worktree(target):
            return False
    except WorktreeError:
        return False
    if target.joinpath(*CLONE_SUBDIR).exists():
        return False  # has a clone -> consumer adopt path, not self-host
    return self_host_payload_root(target) is not None


def _selfhost_missing_surface_paths(target: Path) -> list[str]:
    """The bootstrap-surfaces outputs a self-host worktree is missing, if any:
    the effective cursor plugin manifest, root ``.github/prompts``,
    ``.cursor/hooks.json``, and the shared ``docs/workbay/rules`` doc surface
    (the branch-review skill cites it by path, so a worktree that lacks it has
    dangling policy links). Empty when every expected surface is present."""
    missing: list[str] = []
    effective_cursor_plugin = (
        _plugin_tree_out(target, "effective")
        / "cursor"
        / ".cursor-plugin"
        / "plugin.json"
    )
    if not effective_cursor_plugin.is_file():
        # Built from target.joinpath(...), so it is always under target.
        missing.append(effective_cursor_plugin.relative_to(target).as_posix())
    prompts_dir = target / ".github" / "prompts"
    if not (prompts_dir.is_dir() and any(prompts_dir.glob("*.prompt.md"))):
        missing.append(".github/prompts")
    if not (target / CURSOR_HOOKS_PATH).is_file():
        missing.append(CURSOR_HOOKS_PATH.as_posix())
    # Shared doc surface (install.SHARED_SURFACES): a symlink into the in-tree
    # payload on the primary, absent from a bare worktree checkout. Resolve
    # through the symlink and require at least one rule doc so an empty/broken
    # link still flags. Repaired by the same `bootstrap-surfaces` emission.
    rules_dir = target / RULES_DIR
    if not (rules_dir.is_dir() and any(rules_dir.glob("*.md"))):
        missing.append(RULES_DIR)
    return missing


def _doctor_selfhost_worktree_missing_surfaces(target: Path) -> list[Finding]:
    """Single actionable finding for a self-host worktree starved of its
    generated agent surfaces. Distinct from ``unadopted_worktree`` and the
    ``missing_clone`` storm: the fix is local emission (``bootstrap-surfaces``),
    not ``adopt-worktree``. Returns ``[]`` for non-self-host targets or once the
    surfaces are present (idempotent post-heal)."""
    if not _is_selfhost_worktree(target):
        return []
    missing = _selfhost_missing_surface_paths(target)
    if not missing:
        return []
    return [
        {
            "kind": "selfhost_worktree_missing_surfaces",
            "path": ", ".join(missing),
            "message": (
                "self-host linked worktree is missing generated agent surfaces; "
                "run `workbay-bootstrap bootstrap-surfaces` (or repair) to emit them"
            ),
        }
    ]


def _load_manifest(target: Path) -> dict[str, object]:
    _migrate_legacy_manifest(target)
    manifest_path = target / BOOTSTRAP_MANIFEST_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"{manifest_path} not found. Run `workbay-bootstrap install --target "
            f"{target}` first."
        )
    try:
        return json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{manifest_path} is not valid JSON (torn or corrupt write); re-run "
            "`workbay-bootstrap install` to regenerate it."
        ) from exc


# internal: opt-in flag -> install() keyword. Derivation mirrors the
# CLI contract (`--install-x-y` -> `install_x_y`); the explicit map doubles as
# an allowlist so a corrupted manifest row can never inject arbitrary kwargs.
_HOOK_OPT_IN_KWARGS: dict[str, str] = {
    "--install-claude-stop-hook-local": "install_claude_stop_hook_local",
    "--install-codex-stop-hook": "install_codex_stop_hook",
    "--install-vscode-stop-hook": "install_vscode_stop_hook",
    "--install-grok-stop-hook": "install_grok_stop_hook",
    "--install-claude-reinject-hook-local": "install_claude_reinject_hook_local",
    "--install-codex-ensure-agent-surfaces-hook": "install_codex_ensure_agent_surfaces_hook",
    "--install-vscode-ensure-agent-surfaces-hook": "install_vscode_ensure_agent_surfaces_hook",
    "--install-grok-ensure-agent-surfaces-hook": "install_grok_ensure_agent_surfaces_hook",
}


def _preserved_hook_opt_ins(manifest: Mapping[str, object]) -> dict[str, bool]:
    """Return install() kwargs for hook adapters recorded in ``manifest``.

    Updates re-run ``install``, which only applies hook adapters whose opt-in
    flag is passed — without this, a refresh silently drops every previously
    opted-in Stop adapter from management (its file survives but the new
    manifest no longer records it, so doctor/repair stop watching it).
    Rows are matched by the ``kind=hook_adapter`` tag the walker records;
    legacy manifests without tagged rows preserve nothing, matching the
    pre-internal behavior.
    """
    kwargs: dict[str, bool] = {}
    for entry in manifest.get("configs", []) or []:
        if not isinstance(entry, dict) or entry.get("kind") != "hook_adapter":
            continue
        keyword = _HOOK_OPT_IN_KWARGS.get(str(entry.get("opt_in_flag")))
        if keyword is not None:
            kwargs[keyword] = True
    return kwargs


def _preserved_mcp_servers(
    target: Path, manifest: Mapping[str, object]
) -> Mapping[str, Mapping[str, Any]] | None:
    """Return the currently registered managed MCP mapping, if any.

    Updates inherit the existing managed registration by default. This keeps
    `.mcp.json` / `.vscode/mcp.json` / `.codex/config.toml` listed in the
    refreshed manifest and ensures init-state still runs after a managed
    install when the caller omits ``mcp_servers``.
    """
    configs = manifest.get("configs", []) or []
    registered_mcp = any(
        isinstance(entry, dict) and entry.get("path") == ".mcp.json"
        for entry in configs
    )
    if not registered_mcp:
        return None

    mcp_path = target / ".mcp.json"
    if not mcp_path.is_file():
        raise FileNotFoundError(
            f"{mcp_path} missing for managed update; re-run install or pass --mcp-servers."
        )

    try:
        doc = json.loads(mcp_path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{mcp_path} is not valid JSON; repair or replace it before update."
        ) from exc

    servers = doc.get("mcpServers")
    if not isinstance(servers, dict):
        raise ValueError(
            f"{mcp_path} does not contain an mcpServers mapping; repair it before update."
        )

    preserved: dict[str, Mapping[str, Any]] = {}
    for name, spec in servers.items():
        if not (isinstance(name, str) and isinstance(spec, dict)):
            continue
        preserved[name] = spec
    return preserved


def _mcp_servers_from_manifest(
    target: Path, manifest: Mapping[str, object]
) -> Mapping[str, Mapping[str, Any]] | None:
    """Reconstruct the managed MCP mapping from abort-snapshot provenance.

    A presync abort on a fresh install persists only the sorted server-name
    list (no config surfaces yet). Repair can recover the launch specs by
    intersecting those names with the install-time default map resolved for
    this target/ref.
    """
    from workbay_bootstrap.install import (
        DEFAULT_MCP_SERVERS,
        _resolve_install_mcp_servers,
    )

    names = manifest.get("mcp_servers")
    if not isinstance(names, list) or not names:
        return None
    resolved = _resolve_install_mcp_servers(
        target,
        str(manifest.get("remote_ref") or "main"),
        DEFAULT_MCP_SERVERS,
    )
    if not resolved:
        return None
    reconstructed: dict[str, Mapping[str, Any]] = {}
    for name in names:
        if not isinstance(name, str):
            continue
        spec = resolved.get(name)
        if isinstance(spec, dict):
            reconstructed[name] = spec
    return reconstructed or None



def clean_overlay(*, target: Path, dry_run: bool = False, apply: bool = False) -> dict[str, object]:
    """Plan or apply overlay reclaim (orphan clones, stale package dirs)."""
    from workbay_bootstrap.fsutil import execute_overlay_reclaim

    result = execute_overlay_reclaim(target.resolve(), dry_run=dry_run, apply=apply)
    lines: list[str] = []
    reclaimed = set(result.get("reclaimed") or [])
    if apply:
        for path in sorted(reclaimed):
            lines.append(f"reclaimed: {path}")
        for entry in result.get("refused") or []:
            lines.append(f"refused ({entry['reason']}): {entry['path']}")
        for entry in result.get("failed") or []:
            lines.append(f"failed: {entry['path']} ({entry['error']})")
    else:
        for entry in result.get("planned") or []:
            if entry.get("load_bearing"):
                lines.append(f"refused ({entry['reason']}): {entry['path']}")
            else:
                lines.append(
                    f"would reclaim: {entry['path']} ({entry['reason']})"
                )
    result["lines"] = lines
    return result


def status(*, target: Path) -> str:
    """Return a multi-line human-readable summary of the overlay manifest at
    ``<target>/.workbay-bootstrap.json``.

    When the install registered MCP servers (``.mcp.json`` recorded in
    ``configs``), invokes ``init-state --check`` to append the resolved
    state directory, database path, exports directory, and schema
    version. ``--no-mcp-servers`` installs skip this section.

    Raises ``FileNotFoundError`` when the manifest is absent.
    """
    target = Path(target).resolve()
    manifest = _load_manifest(target)

    surfaces = manifest.get("surfaces", []) or []
    configs = manifest.get("configs", []) or []
    shared = sum(1 for s in surfaces if s.get("source") == "shared")
    local = sum(1 for s in surfaces if s.get("source") == "local")
    generated = sum(1 for s in surfaces if s.get("source") == "generated")

    lines = [
        f"workbay-bootstrap overlay at {target}",
        f"  remote_url:  {manifest.get('remote_url')}",
        f"  remote_ref:  {manifest.get('remote_ref')}",
        f"  remote_sha:  {manifest.get('remote_sha')}",
        f"  surfaces:    {len(surfaces)} ({shared} shared, {local} local, {generated} generated)",
        f"  configs:     {len(configs)}",
    ]
    for entry in configs:
        lines.append(f"    - {entry.get('path')} ({entry.get('action')})")

    state_lines = _status_handoff_state_lines(target, configs)
    if state_lines:
        lines.append("  handoff state:")
        lines.extend(f"    {line}" for line in state_lines)

    return "\n".join(lines) + "\n"


def _status_handoff_state_lines(
    target: Path, configs: list[dict[str, Any]]
) -> list[str]:
    """Invoke ``init-state --check`` against the workbay-handoff-mcp entry in
    ``.mcp.json`` and return zero or more summary lines.

    Returns an empty list when the install did not register MCP servers
    (so init-state was never expected to run) — this mirrors doctor's
    state-check gating.
    """

    registered_mcp = any(
        isinstance(entry, dict) and entry.get("path") == ".mcp.json"
        for entry in configs
    )
    if not registered_mcp:
        return []

    mcp_path = target / ".mcp.json"
    if not mcp_path.is_file():
        return ["error: .mcp.json missing — re-run install"]

    try:
        mcp_doc = json.loads(mcp_path.read_text())
    except json.JSONDecodeError:
        return ["error: .mcp.json is not valid JSON"]
    if not isinstance(mcp_doc, dict):
        return ["error: .mcp.json is not a JSON object — re-run install"]
    servers = mcp_doc.get("mcpServers")
    if servers is not None and not isinstance(servers, dict):
        return ["error: .mcp.json mcpServers is malformed — re-run install"]
    spec = None
    if isinstance(servers, dict):
        spec = servers.get("workbay-handoff-mcp")
    if not isinstance(spec, dict):
        return []

    cmd = _resolve_init_state_check_command(target, spec)
    if cmd is None:
        return ["error: workbay-handoff-mcp entry in .mcp.json is malformed"]

    env = os.environ.copy()
    raw_env = spec.get("env")
    if isinstance(raw_env, dict):
        for key, value in raw_env.items():
            if isinstance(key, str) and isinstance(value, str):
                env[key] = value

    from workbay_bootstrap.external import ExternalCallTimeout, run_external

    try:
        proc = run_external(
            cmd,
            call_class="handoff_cli",
            check=False,
            capture_output=True,
            text=True,
            cwd=str(target),
            env=env,
        )
    except (OSError, ExternalCallTimeout) as exc:
        return [f"error: init-state --check failed: {exc}"]

    if proc.returncode != 0:
        first = (proc.stderr or proc.stdout or "").strip().splitlines()
        detail = first[-1] if first else f"exit {proc.returncode}"
        return [f"error: init-state --check failed: {detail}"]

    payload: dict[str, Any]
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return [f"error: init-state --check returned non-JSON: {proc.stdout[:120]!r}"]

    return [
        f"state_dir:      {payload.get('state_dir')}",
        f"db_path:        {payload.get('db_path')}",
        f"exports_dir:    {payload.get('exports_dir')}",
        f"schema_version: {payload.get('schema_version')}",
        f"initialized:    {payload.get('initialized')}",
    ]


def _resolve_init_state_check_command(
    target: Path, spec: dict[str, Any]
) -> list[str] | None:
    """Map the ``.mcp.json`` workbay-handoff-mcp entry to an
    ``init-state --check`` invocation, mirroring install-time resolution
    in ``install._run_init_state``.
    """
    command = spec.get("command")
    raw_args = spec.get("args", [])
    if not isinstance(command, str) or not command:
        return None
    if not isinstance(raw_args, list) or not all(isinstance(a, str) for a in raw_args):
        return None

    args = list(raw_args)
    if args and args[-1] in {"serve-stdio", "serve-http", "init-state"}:
        args = args[:-1]
    if not any(
        a == "--workspace-root" or a.startswith("--workspace-root=") for a in args
    ):
        args.extend(["--workspace-root", str(target)])
    raw_env = spec.get("env")
    env_has_state_dir = (
        isinstance(raw_env, dict) and "WORKBAY_HANDOFF_STATE_DIR" in raw_env
    )
    if (
        not any(a == "--state-dir" or a.startswith("--state-dir=") for a in args)
        and not env_has_state_dir
    ):
        args.extend(["--state-dir", str(target / ".task-state")])
    args.extend(["init-state", "--check"])
    return [command, *args]


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


Finding = dict[str, str]


def _doctor_install_receipt_findings(manifest: dict[str, object]) -> list[Finding]:
    """Trust receipted install-step outcomes before live probing (implementation note S6)."""
    from workbay_bootstrap.install_receipt import receipt_failed_steps

    findings: list[Finding] = []
    for step in receipt_failed_steps(manifest):
        findings.append(
            {
                "kind": "install_step_receipt",
                "path": str(step.get("step", "unknown")),
                "detail": str(step.get("reason") or step.get("status") or ""),
                "severity": "error" if step.get("status") == "failed" else "warning",
            }
        )
    return findings


def _repair_deferred_install_steps(
    target: Path,
    manifest: dict[str, object],
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> list[Finding]:
    """Retry receipt-marked deferred install steps (Recovery-Oriented Computing)."""
    if not mcp_servers:
        return []
    steps = manifest.get("install_steps")
    if not isinstance(steps, list):
        return []
    deferred = {
        step.get("step")
        for step in steps
        if isinstance(step, dict) and step.get("status") == "deferred"
    }
    repaired: list[Finding] = []
    if "prewarm_uvx_mcp" in deferred:
        from workbay_bootstrap.external import reset_offline_latch
        from workbay_bootstrap.install import _prewarm_uvx_mcp_envs

        reset_offline_latch()
        warmed = _prewarm_uvx_mcp_envs(target, mcp_servers)
        if warmed:
            for step in steps:
                if isinstance(step, dict) and step.get("step") == "prewarm_uvx_mcp":
                    step["status"] = "ok"
                    step.pop("reason", None)
                    step.pop("failure_class", None)
            manifest["prewarm_refs"] = list(warmed)
            # attach_to_manifest omits offline_latch when False; mirror that
            # convention rather than persisting an explicit false.
            manifest.pop("offline_latch", None)
            (target / BOOTSTRAP_MANIFEST_NAME).write_text(
                json.dumps(manifest, indent=2) + "\n"
            )
            repaired.append(
                {
                    "kind": "install_step_repair",
                    "path": "prewarm_uvx_mcp",
                    "detail": f"prewarmed {len(warmed)} package ref(s)",
                }
            )
    return repaired


def _repair_failed_presync_install(
    target: Path,
    manifest: dict[str, object],
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> tuple[list[Finding], list[Finding]]:
    """Converge a presync-abort snapshot by re-running the full install
    (internal, S6-02).

    A failed ``presync_local_mcp`` aborts ``execute_install_plan`` BEFORE any
    surface/config/generator/init-state work, so the abort snapshot describes
    an incomplete install. Flipping the receipt step in place would produce a
    manifest that lies about a half-built tree; the only honest convergence
    is a full install re-run from the snapshot's recorded inputs, whose fresh
    manifest supersedes the snapshot. On a still-failing presync the re-run
    aborts again and rewrites an equivalent snapshot — no dummy writes.

    Returns ``(repaired, skipped)`` finding lists in the doctor shape.
    """
    steps = manifest.get("install_steps")
    if not isinstance(steps, list):
        return [], []
    failed_presync = any(
        isinstance(step, dict)
        and step.get("step") == "presync_local_mcp"
        and step.get("status") == "failed"
        for step in steps
    )
    if not failed_presync:
        return [], []
    if mcp_servers is None:
        return [], [
            {
                "kind": "install_step_repair_skipped",
                "path": "presync_local_mcp",
                "detail": (
                    "abort snapshot lacks recoverable MCP server mapping; "
                    "pass --mcp-servers"
                ),
            }
        ]
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    remote_url = manifest.get("remote_url")
    if source_kind != "git_overlay" or not remote_url:
        return [], [
            {
                "kind": "install_step_repair_skipped",
                "path": "presync_local_mcp",
                "detail": (
                    "abort snapshot lacks re-runnable git-overlay install "
                    f"inputs (source_kind={source_kind})"
                ),
            }
        ]
    from workbay_bootstrap.install import install
    from workbay_bootstrap.install_receipt import InstallExecutionError

    try:
        # Recovery path is deliberately lenient: required-surface enforcement
        # belongs to first install / update, not to abort convergence.
        install(
            target=target,
            remote_url=str(remote_url),
            remote_ref=str(manifest.get("remote_ref") or "main"),
            profile=str(manifest.get("profile") or "all"),
            mcp_servers=mcp_servers,
            enforce_required_surfaces=False,
        )
    except InstallExecutionError as exc:
        return [], [
            {
                "kind": "install_step_repair_skipped",
                "path": "presync_local_mcp",
                "detail": f"install re-run still failing: {exc}",
            }
        ]
    fresh = _load_manifest(target)
    steps_after = fresh.get("install_steps")
    presync_ok = isinstance(steps_after, list) and any(
        isinstance(step, dict)
        and step.get("step") == "presync_local_mcp"
        and step.get("status") == "ok"
        for step in steps_after
    )
    if not presync_ok:
        return [], [
            {
                "kind": "install_step_repair_skipped",
                "path": "presync_local_mcp",
                "detail": "install re-run completed without presync convergence",
            }
        ]
    return [
        {
            "kind": "install_step_repair",
            "path": "presync_local_mcp",
            "detail": "full install re-run converged presync abort snapshot",
        }
    ], []


_UPDATE_HINT = "run `make workbay-update` (or `workbay-bootstrap update`)"


def _latest_pypi_version(distribution: str) -> str | None:
    """Latest released version of ``distribution`` per PyPI's JSON API, or
    ``None`` on any failure (offline, 404, malformed). Only used behind the
    explicit ``--check-pypi`` opt-in — doctor stays offline-safe by default."""
    import urllib.request

    url = f"https://pypi.org/pypi/{distribution}/json"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
            payload = json.load(response)
        version = payload["info"]["version"]
        return str(version) if version else None
    except Exception:  # noqa: BLE001 — informational note only, never raise
        return None


def _doctor_package_versions(
    manifest: dict[str, Any], *, check_pypi: bool = False
) -> list[Finding]:
    """Offline package/stack version checks for package-source installs
    (internal).

    - ``package_drift`` — installed workbay-system wheel differs from (or
      is missing vs) ``manifest.package_version``: the overlay no longer
      matches the environment that should have produced it.
    - ``stack_drift`` — the installed anchor or any recorded ``stack_members``
      distribution differs from / is missing vs its recorded exact version.
    - ``stack_provenance_missing`` — legacy package manifest without stack
      fields; informational, backfilled by the next package update.
    - ``stack_update_available`` — opt-in (``check_pypi``) note when PyPI has
      a newer anchor release (package source) or merged-HEAD differs from the
      latest published anchor (worktree/git_overlay sources); informational only.
    """
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    if source_kind in ("worktree", "git_overlay"):
        return _doctor_local_source_pypi_staleness(manifest, check_pypi=check_pypi)
    if source_kind != "package":
        return []
    findings: list[Finding] = []

    def installed_version(distribution: str) -> str | None:
        try:
            return importlib_metadata.version(distribution)
        except importlib_metadata.PackageNotFoundError:
            return None

    recorded = manifest.get("package_version")
    installed = installed_version("workbay-system")
    if installed is None:
        findings.append(
            {
                "kind": "package_drift",
                "path": "workbay-system",
                "detail": (
                    f"workbay-system not installed (manifest recorded "
                    f"{recorded}) — {_UPDATE_HINT}"
                ),
            }
        )
    elif recorded and installed != recorded:
        findings.append(
            {
                "kind": "package_drift",
                "path": "workbay-system",
                "detail": (
                    f"installed {installed} != manifest {recorded} — {_UPDATE_HINT}"
                ),
            }
        )

    stack_distribution = manifest.get("stack_distribution")
    if not stack_distribution:
        findings.append(
            {
                "kind": "stack_provenance_missing",
                "path": BOOTSTRAP_MANIFEST_NAME,
                "severity": "info",
                "detail": (
                    "package manifest predates stack provenance; the next "
                    "package update backfills stack_distribution/"
                    "stack_version/stack_members"
                ),
            }
        )
        return findings

    for finding in _stack_member_drift_findings(
        manifest, version_lookup=installed_version
    ):
        findings.append(
            {
                **finding,
                "detail": f"{finding['detail']} — {_UPDATE_HINT}",
            }
        )

    if check_pypi:
        installed_stack = installed_version(str(stack_distribution))
        latest = _latest_pypi_version(str(stack_distribution))
        if latest and latest != installed_stack:
            findings.append(
                {
                    "kind": "stack_update_available",
                    "path": str(stack_distribution),
                    "severity": "info",
                    "detail": (
                        f"PyPI has {stack_distribution} {latest} "
                        f"(installed {installed_stack}) — {_UPDATE_HINT}"
                    ),
                }
            )
    return findings


def _local_source_version_direction(installed: str, latest: str) -> str:
    """Compare an in-tree install against the published latest.

    Returns ``"ahead"`` (in-tree newer), ``"behind"`` (in-tree older),
    ``"equal"`` (PEP 440-equal even when the version *strings* differ, e.g.
    ``"0.1"`` vs ``"0.1.0"``), or ``"differs"`` when the strings are unequal and
    not both PEP 440 parseable — we never assert a direction we cannot
    substantiate.
    """
    try:
        from packaging.version import InvalidVersion, Version
    except Exception:  # noqa: BLE001 — informational note only, never raise
        return "differs"
    try:
        installed_v = Version(installed)
        latest_v = Version(latest)
    except InvalidVersion:
        return "differs"
    if installed_v > latest_v:
        return "ahead"
    if installed_v < latest_v:
        return "behind"
    return "equal"


def _doctor_local_source_pypi_staleness(
    manifest: dict[str, Any], *, check_pypi: bool
) -> list[Finding]:
    """Opt-in PyPI staleness notes for worktree/git_overlay installs (implementation note S3)."""
    if not check_pypi:
        return []
    stack_distribution = str(manifest.get("stack_distribution") or "workbay")
    try:
        installed_stack = importlib_metadata.version(stack_distribution)
    except importlib_metadata.PackageNotFoundError:
        return []
    latest = _latest_pypi_version(stack_distribution)
    if latest is None:
        # _latest_pypi_version collapses 404 / offline / timeout / malformed into
        # None, so we cannot assert the dist is definitively unpublished — only
        # that no published baseline was reachable this run.
        return [
            {
                "kind": "stack_update_available",
                "path": stack_distribution,
                "severity": "info",
                "detail": (
                    f"{stack_distribution} appears unpublished on PyPI "
                    f"(or PyPI was unreachable) (installed {installed_stack}) "
                    f"— no published baseline to compare"
                ),
            }
        ]
    if latest == installed_stack:
        return []
    direction = _local_source_version_direction(installed_stack, latest)
    if direction == "equal":
        # PEP 440-equal despite a string difference (e.g. "0.1" vs "0.1.0"): the
        # in-tree build matches the published release — emit no staleness note.
        return []
    if direction == "behind":
        # In-tree build is older than the published latest: the actionable fix is
        # to pull the newer release, not republish.
        detail = (
            f"merged HEAD has {stack_distribution} {installed_stack} behind "
            f"PyPI latest {latest} — {_UPDATE_HINT}"
        )
    elif direction == "ahead":
        # In-tree newer than the published wheel — the documented S3 case:
        # package-source consumers are stale until a republish.
        detail = (
            f"merged HEAD has {stack_distribution} {installed_stack} ahead of "
            f"PyPI latest {latest} — republish to refresh package-source consumers"
        )
    else:
        # "differs": at least one version is not PEP 440 parseable (the equal case
        # short-circuits above), so we cannot substantiate ahead-vs-behind — report
        # the divergence neutrally instead of over-stating a republish recommendation.
        detail = (
            f"merged HEAD has {stack_distribution} {installed_stack} differing from "
            f"PyPI latest {latest} — versions not comparable; verify which is newer"
        )
    return [
        {
            "kind": "stack_update_available",
            "path": stack_distribution,
            "severity": "info",
            "detail": detail,
        }
    ]


def _doctor_source_kind_materialization_drift(
    target: Path, manifest: Mapping[str, object]
) -> list[Finding]:
    """Report when manifest ``source_kind`` disagrees with physical overlay."""
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    clone_git = target.joinpath(*CLONE_SUBDIR) / ".git"
    findings: list[Finding] = []
    surfaces = manifest.get("surfaces") or []
    shared_symlinks = [
        entry.get("path", "")
        for entry in surfaces
        if isinstance(entry, dict)
        and entry.get("source") == "shared"
        and (target / str(entry.get("path", ""))).is_symlink()
    ]
    if source_kind == "package" and (clone_git.exists() or shared_symlinks):
        findings.append(
            {
                "kind": "source_kind_drift",
                "path": BOOTSTRAP_MANIFEST_NAME,
                "message": (
                    "manifest records source_kind=package but the overlay is "
                    "clone/symlink-backed; reconcile with "
                    "`make dogfood DOGFOOD_SOURCE=worktree` or "
                    "`make dogfood DOGFOOD_SOURCE=package` after removing "
                    ".workbay/remote."
                ),
            }
        )
    elif source_kind == "worktree" and clone_git.exists():
        findings.append(
            {
                "kind": "source_kind_drift",
                "path": BOOTSTRAP_MANIFEST_NAME,
                "message": (
                    "manifest records source_kind=worktree but "
                    ".workbay/remote/.git still exists; reconcile with "
                    "`make dogfood DOGFOOD_SOURCE=worktree`."
                ),
            }
        )
    elif source_kind == "git_overlay" and not clone_git.exists():
        for entry in surfaces:
            if not isinstance(entry, dict) or entry.get("source") != "shared":
                continue
            surface = entry.get("path", "")
            link = target / str(surface)
            if link.is_symlink():
                findings.append(
                    {
                        "kind": "source_kind_drift",
                        "path": BOOTSTRAP_MANIFEST_NAME,
                        "message": (
                            "manifest records source_kind=git_overlay but no "
                            "managed clone exists while shared surfaces are "
                            "symlinked; reconcile with "
                            "`make dogfood DOGFOOD_SOURCE=git_overlay`."
                        ),
                    }
                )
                break
    return findings


def _resolve_installed_overlay_root(
    target: Path, manifest: Mapping[str, object]
) -> Path:
    """Return the on-disk overlay source root for an installed target."""
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    if source_kind == "package":
        # Package installs never persist an overlay path in the manifest (the
        # installer records only `package_version` + stack provenance). The
        # payload is re-resolved fresh from the installed workbay-system
        # distribution via data_root(), matching exactly what install/update
        # materialize surfaces from. `_package_source_root(None)` does that.
        from workbay_bootstrap.install import _package_source_root

        return _package_source_root(None)
    if source_kind == "worktree":
        return target.resolve()
    return target.joinpath(*CLONE_SUBDIR)


def _doctor_dangling_managed_links(
    target: Path, manifest: Mapping[str, object], overlay_source_root: Path
) -> list[Finding]:
    findings: list[Finding] = []
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    # Package-mode only. A package consumer copies its managed surfaces from the
    # workbay-system payload, so a dangling managed link there is genuinely
    # repairable by `repair --materialize-managed` (which re-resolves the payload
    # from data_root()). For a git_overlay consumer a dangling managed link means
    # the `.workbay/remote` clone/surface is missing — already owned by the
    # missing_clone / surface_drift facets, whose repair rebuilds the clone.
    # materialize would resolve from the (missing) overlay_source_root and be a
    # guaranteed no-op, so stay silent and let those facets own the git_overlay
    # case instead of emitting a duplicate finding pointing at a dead repair.
    if source_kind != "package":
        return findings
    for entry in manifest.get("surfaces") or []:
        if not isinstance(entry, dict) or entry.get("source") not in {
            "shared",
            "generated",
        }:
            continue
        surface = str(entry.get("path") or "")
        if not surface:
            continue
        link = target / surface
        if not link.is_symlink():
            continue
        try:
            raw_target = os.readlink(link)
        except OSError:
            raw_target = ""
        dangling = not link.exists()
        mounts_into_clone = _names_clone_subtree(raw_target)
        if not dangling and not mounts_into_clone:
            continue
        payload_source = overlay_source_root / surface
        # The facet flags two shapes: a still-live symlink mounting into the
        # `.workbay/remote` clone (a hybrid-mount surviving a package migration —
        # package mode should own every surface) and a plain dangling link
        # (readlink target simply missing, which also catches a stale symlink
        # whose clone/target has since been removed). Describe each accurately.
        reason = (
            "mounts into the .workbay/remote clone"
            if mounts_into_clone and not dangling
            else "points at a missing target"
        )
        findings.append(
            {
                "kind": "managed_link_dangling",
                "path": surface,
                "payload_source": str(payload_source),
                "message": (
                    f"managed link {surface} {reason}; "
                    "run `workbay repair --materialize-managed`"
                ),
            }
        )
    return findings


def _doctor_gitonly_mcp_tool_sources(manifest: Mapping[str, object]) -> list[Finding]:
    """Flag git-only MCP tools installed from a non-canonical (local path) source.

    The install receipt records each git-sourced MCP server as
    ``{name, from_spec}`` (internal). The canonical model
    is a pinned git ref (``git+<url>@<tag>#subdirectory=packages/<pkg>``); a
    local filesystem ``from_spec`` means the tool was installed from a dev
    checkout and is not reproducible from the pin. Surface it as a
    ``gitonly_mcp_tool_source`` finding naming the exact ``uv tool install``
    reinstall command. Legacy name-only receipt entries (bare strings) carry no
    source to audit and are skipped — back-compat with pre-slice-4 ledgers.
    """
    findings: list[Finding] = []
    for entry in manifest.get("gitonly_mcp_tools") or []:
        if not isinstance(entry, dict):
            # Legacy name-only entry: no from_spec recorded, nothing to audit.
            continue
        name = str(entry.get("name") or "")
        from_spec = str(entry.get("from_spec") or "")
        if not name or not from_spec:
            continue
        # Canonical sources are pinned git refs. Anything else (an absolute or
        # relative filesystem path) is a local dev install we cannot reproduce.
        if from_spec.startswith("git+"):
            continue
        findings.append(
            {
                "kind": "gitonly_mcp_tool_source",
                "path": name,
                "severity": "warning",
                "detail": (
                    f"{name} was installed from a local path source "
                    f"'{from_spec}', not a pinned git ref; the install is not "
                    "reproducible. Reinstall from the pinned ref, e.g. "
                    "`uv tool install --no-sources --force --from "
                    f"git+<repo-url>@<tag>#subdirectory=packages/{name} {name}`."
                ),
            }
        )
    return findings


def doctor(
    *,
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None = None,
    plugin_overrides: Path | None = None,
    check_pypi: bool = False,
    include_embedding_state: bool = False,
) -> list[Finding]:
    """Return a list of drift findings for the overlay at ``target``.

    Each finding is a dict with at least ``kind`` and ``path``. Recognized
    kinds:

    - ``missing_manifest`` — ``.workbay-bootstrap.json`` is gone.
    - ``missing_clone`` — ``.workbay/remote/.git`` is gone.
    - ``surface_drift`` — a surface recorded as ``shared`` in the manifest is
      no longer a symlink resolving into the clone.
    - ``local_stale`` — a ``source=local`` surface exactly matches an *older*
      payload revision: a bootstrap-era copy silently starving updates
      (internal).
    - ``local_redundant`` — a ``source=local`` surface is identical to the
      current payload; candidate for adoption into a managed surface.
    - ``local_override`` — a ``source=local`` surface matches no payload
      revision: consumer-authored. Emitted with ``severity=info``; never
      affects the doctor exit code and is never repair-eligible.
    - ``generated_drift`` — a surface recorded as ``generated`` differs from
      what ``scripts/generate_agent_workflows.py`` would write today. Detected
      by re-running the generator in ``--check`` mode against the target.
        - ``stale_override`` — a warn-mode replacement override still composes, but
            its recorded upstream digest no longer matches the current base skill.
    - ``config_drift`` — a managed MCP server in ``mcp_servers`` is no longer
      present (or no longer matches) in ``.mcp.json`` / ``.vscode/mcp.json``.
      Only checked when ``mcp_servers`` is provided.
        - ``pin_target_drift`` — a plugin marketplace pin no longer points at the
            expected base/effective generated tree for the current override state.
        - ``plugin_source_drift`` — a plugin marketplace pin resolves to a missing
            or incomplete plugin tree (missing plugin.json, .mcp.json, or skills).
    - ``state_drift`` — ``.task-state/handoff.db`` is missing even though the
      manifest's ``configs`` array recorded ``.mcp.json``. Suppressed when the
      install was ``--no-mcp-servers`` (no managed servers registered, so no
      state init was expected).
    - ``mcp_console_missing`` — a local MCP server routed through the implementation note
      launcher shim has no per-package/root ``.venv`` console script, so the
      shim will degrade to the slower ``uv run`` path (a boot-miss risk under
      concurrent startup). ``severity=warning``. Derived from the INSTALLED local
      server map (implementation note A1), so it fires on the operator path where doctor is
      invoked with no ``--mcp-servers``; only shim-routed (worktree) servers can
      trip it.
    - ``hook_adapter_drift`` — a compact-session Stop adapter that bootstrap
      installed (its target is in the manifest ``configs``) is missing or no
      longer matches the manifest-declared managed entry. Never-installed
      adapters stay optional and are not reported (internal).
    - ``cursor_native_surface_drift`` — root ``.cursor/commands`` or
      ``.cursor/skills`` carry bootstrap-managed duplicates (Path A: any
      generated-tag command markdown or effective-tree-identical SKILL.md;
      Path B: entry-set mismatch vs the effective cursor plugin tree). Inferred
      from native-plugin registration state; repaired by
      ``materialize_cursor_plugin``.
    - ``tracked_overlay_boundary`` — a consumer path crosses the declared
      tracked-vs-overlay boundary (duplicate tracked skill, or a self-host
      source path that would be silently gitignored).
    - ``embedding_artifact_env_unwritten`` / ``embedding_artifact_missing`` / ``embedding_artifact_digest_mismatch`` / ``embedding_extra_absent`` — embedding model delivery-state (info/warning; never blocks unrelated repair).
    - ``selfhost_worktree_missing_surfaces`` — a linked worktree of a self-host
      primary (ships the in-tree payload, no ``.workbay/remote`` clone) is
      missing locally-emitted surfaces (effective cursor plugin, root
      ``.github/prompts``, ``.cursor/hooks.json``). Short-circuit kind, like
      ``unadopted_worktree``; repaired by ``bootstrap-surfaces`` (local
      emission), not ``adopt-worktree``.
    Returns an empty list when everything is clean.
    """
    target = Path(target).resolve()
    findings: list[Finding] = []

    # Worktree short-circuit (M4): a bare linked worktree lacks the whole overlay,
    # so the marker/clone/surface checks below would emit a missing_clone +
    # surface_drift storm whose naive repair targets the worktree's own absent
    # clone. Emit a single actionable finding and stop; repair routes it to
    # adopt_worktree against the primary.
    unadopted_primary = _unadopted_worktree_primary(target)
    if unadopted_primary is not None:
        return [{"kind": "unadopted_worktree", "path": str(unadopted_primary)}]

    # implementation note: a self-host worktree (ships the in-tree payload, no clone) emits
    # its surfaces locally instead of symlinking them from a clone. Short-circuit
    # UNCONDITIONALLY: its health is purely whether those surfaces are present —
    # one actionable finding when starved, else clean ([]). Either way the
    # marker/clone/surface checks below never run their missing_clone +
    # surface_drift storm against the (correctly) absent clone.
    if _is_selfhost_worktree(target):
        return _doctor_selfhost_worktree_missing_surfaces(target)

    _migrate_legacy_manifest(target)
    manifest_path = target / BOOTSTRAP_MANIFEST_NAME
    if not manifest_path.is_file():
        findings.append({"kind": "missing_manifest", "path": BOOTSTRAP_MANIFEST_NAME})
        return findings

    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError:
        # A torn/corrupt manifest is exactly the failure doctor exists to
        # surface — report it instead of crashing on the read.
        findings.append({"kind": "corrupt_manifest", "path": BOOTSTRAP_MANIFEST_NAME})
        return findings
    findings.extend(_doctor_install_receipt_findings(manifest))
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    is_package_source = source_kind == "package"
    is_worktree_source = source_kind == "worktree"
    findings.extend(_doctor_package_versions(manifest, check_pypi=check_pypi))
    findings.extend(_doctor_source_kind_materialization_drift(target, manifest))
    clone = target if is_worktree_source else target.joinpath(*CLONE_SUBDIR)
    clone_resolved = clone.resolve(strict=False)
    overlay_source_root = _resolve_installed_overlay_root(target, manifest)
    findings.extend(_doctor_dangling_managed_links(target, manifest, overlay_source_root))
    findings.extend(_doctor_gitonly_mcp_tool_sources(manifest))

    # Package and worktree sources have no managed git clone; skip missing_clone.
    if (
        not is_package_source
        and not is_worktree_source
        and not (clone / ".git").exists()
    ):
        findings.append({"kind": "missing_clone", "path": CLONE_RELPATH})

    surfaces = manifest.get("surfaces") or []
    for entry in surfaces:
        if entry.get("source") != "shared":
            continue
        surface = entry.get("path", "")
        if surface in _GENERATOR_INPUT_SURFACES:
            # generator_input: ledger-resolved against the source payload/clone, never
            # materialized in the target. Package installs have NO clone, so anchor at
            # the package payload root instead.
            if is_package_source:
                try:
                    source_root = _package_source_root(None)
                except Exception:
                    # workbay_system not importable (partial upgrade / stripped venv):
                    # generator_input inputs are the generator's concern; skip the
                    # drift check rather than crashing doctor.
                    continue
            else:
                source_root = clone
            if not _resolve_in_clone(source_root, surface).exists():
                findings.append({"kind": "surface_drift", "path": surface})
            continue
        link = target / surface
        if is_package_source:
            # Copied surface: drift only when the materialized path is gone.
            if not link.exists():
                findings.append({"kind": "surface_drift", "path": surface})
            continue
        if not link.is_symlink():
            findings.append({"kind": "surface_drift", "path": surface})
            continue
        try:
            resolved = link.resolve(strict=False)
        except OSError:
            findings.append({"kind": "surface_drift", "path": surface})
            continue
        in_clone = resolved == (clone / surface).resolve(strict=False) or str(
            resolved
        ).startswith(str(clone_resolved) + os.sep)
        if not in_clone:
            findings.append({"kind": "surface_drift", "path": surface})

    findings.extend(
        _doctor_local_surfaces(
            target, clone, manifest, is_package_source=is_package_source
        )
    )

    override_root = _discover_plugin_override_root(
        target,
        manifest=manifest,
        plugin_overrides=plugin_overrides,
    )

    # Generated-surface, hidden-override, and stop-adapter re-checks re-run the
    # generator / read the portable manifest from the git clone, which the
    # package source does not keep. They are git_overlay-only for now; the
    # consumer-tree checks below (plugin pins, codex activation, plugin source
    # integrity, override state, handoff state) apply to both sources.
    if not is_package_source and not is_worktree_source:
        findings.extend(
            _doctor_generated_surfaces(target, clone, manifest, override_root)
        )

    if mcp_servers:
        findings.extend(_doctor_mcp_config_drift(target, manifest, mcp_servers))

    # implementation note A1: the shim console-script check inspects the INSTALLED local
    # server map, not the caller-supplied one, so it fires on the operator path
    # (`make doctor` / `workbay-bootstrap doctor` / update.sh all pass no
    # --mcp-servers, which cli.py resolves to None) — not only when a python3
    # shim map is handed in explicitly. The base mirrors _resolve_managed_servers:
    # the worktree itself for source_kind=worktree, else the managed clone. A
    # git_overlay clone resolves to uv-run specs the check skips, and a non-local
    # install resolves to None, so this stays a no-op off the shim-routed path.
    from workbay_bootstrap.install import _build_local_default_mcp_servers

    local_mcp_servers = _build_local_default_mcp_servers(
        target, base=target if is_worktree_source else None
    )
    if local_mcp_servers:
        findings.extend(
            _doctor_local_mcp_console_scripts(target, local_mcp_servers)
        )

    findings.extend(_doctor_plugin_pin_targets(target, override_root))
    findings.extend(_doctor_codex_activation_config(target))
    # Hook-surface coherence (internal): runs for ALL source kinds
    # including source=local surfaces — closing the _doctor_local_surfaces
    # blind spot the terminal-guard incident exploited.
    findings.extend(_doctor_hook_coherence(target))
    # internal: provenance surface present but neither tracked nor
    # ignored (git-guarded severity — absent repo → warning only).
    findings.extend(_doctor_materialized_surface_leaks(target))
    effective_missing = _doctor_effective_tree_missing(target)
    if effective_missing:
        # Whole-tree-missing subsumes per-pin plugin_source_drift noise.
        findings.extend(effective_missing)
    else:
        findings.extend(_doctor_plugin_source_integrity(target))
    if not is_package_source and not is_worktree_source:
        findings.extend(
            _doctor_hidden_override_collisions(target, clone, override_root)
        )
    findings.extend(_doctor_plugin_override_state(target, override_root))
    if not is_package_source:
        findings.extend(_doctor_managed_stop_adapters(target, clone, manifest))
    findings.extend(_doctor_state(target, manifest))
    findings.extend(_doctor_unharvested_agent_errors(target))
    findings.extend(_doctor_agent_errors_silence(target))
    findings.extend(_doctor_telemetry_freshness(target))
    findings.extend(_doctor_stale_dev_temp(target))
    findings.extend(_doctor_tracked_overlay_boundary(target))
    # Embedding delivery-state is operator-facing observability, NOT overlay
    # drift: it must not enter the findings list that drives repair's drift loop
    # / `if not findings` short-circuit (an always-present info/warning note would
    # make every clean install look drifted). The CLI `doctor` command opts in to
    # surface it; repair re-heals the artifact via maybe_provision_embeddings.
    if include_embedding_state:
        findings.extend(_doctor_embedding_artifacts(target))

    return findings



def _doctor_embedding_artifacts(target: Path) -> list[Finding]:
    """Delivery-state for the pinned embedding model (C1 / S5).

    Informational when unprovisioned; warnings when partially delivered, digest-
    mismatched, or the runtime extra is absent while artifacts verify.
    """
    from workbay_bootstrap.embedding_provision import (
        embedding_env_path,
        embeddings_disabled,
        embeddings_gate_disabled_from_file,
        parse_embedding_env_file,
        sha256_file,
    )

    rel_env = ".workbay/embedding.env"
    env_path = embedding_env_path(target)
    findings: list[Finding] = []

    # A worktree that has deliberately opted out (process-env or file gate) is
    # not "drifted": provisioning was intentionally skipped, so stay silent
    # rather than nag with unprovisioned / incomplete-env notes.
    if embeddings_disabled() or embeddings_gate_disabled_from_file(target):
        return findings

    env_vars = parse_embedding_env_file(target)

    model = env_vars.get("WORKBAY_HANDOFF_EMBEDDING_MODEL", "").strip()
    tokenizer = env_vars.get("WORKBAY_HANDOFF_EMBEDDING_TOKENIZER", "").strip()
    model_sha = env_vars.get("WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256", "").strip()
    tok_sha = env_vars.get("WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256", "").strip()

    if not env_path.is_file():
        findings.append(
            {
                "kind": "embedding_artifact_env_unwritten",
                "path": rel_env,
                "severity": "info",
                "message": (
                    "embedding model not provisioned; run "
                    "`workbay-bootstrap provision-embeddings --target <worktree>` "
                    "or re-run install/repair (use --no-embeddings to opt out)"
                ),
            }
        )
        return findings

    specs = (
        ("model", model, model_sha),
        ("tokenizer", tokenizer, tok_sha),
    )
    artifacts_ok = True
    for label, path_str, expected_sha in specs:
        if not path_str or not expected_sha:
            findings.append(
                {
                    "kind": "embedding_artifact_env_unwritten",
                    "path": rel_env,
                    "severity": "warning",
                    "message": f"embedding env incomplete: missing {label} path or digest",
                }
            )
            artifacts_ok = False
            continue
        artifact = Path(path_str)
        if not artifact.is_file():
            findings.append(
                {
                    "kind": "embedding_artifact_missing",
                    "path": str(artifact),
                    "severity": "warning",
                    "message": f"embedding {label} artifact not found at provisioned path",
                }
            )
            artifacts_ok = False
            continue
        actual = sha256_file(artifact)
        if actual != expected_sha:
            findings.append(
                {
                    "kind": "embedding_artifact_digest_mismatch",
                    "path": str(artifact),
                    "severity": "warning",
                    "message": (
                        f"embedding {label} digest mismatch "
                        f"(expected {expected_sha}, got {actual}); re-run repair"
                    ),
                }
            )
            artifacts_ok = False

    if artifacts_ok:
        try:
            import importlib

            if importlib.util.find_spec("onnxruntime") is None or importlib.util.find_spec("tokenizers") is None:
                raise ImportError
        except ImportError:
            findings.append(
                {
                    "kind": "embedding_extra_absent",
                    "path": rel_env,
                    "severity": "warning",
                    "message": (
                        "embedding artifacts are provisioned but the optional "
                        "runtime extra is not installed "
                        "(pip install 'mcp-workbay-handoff[embeddings]')"
                    ),
                }
            )

    return findings

def _doctor_tracked_overlay_boundary(target: Path) -> list[Finding]:
    from .overlay_boundary import validate_tracked_overlay_boundary

    findings: list[Finding] = []
    for item in validate_tracked_overlay_boundary(target):
        findings.append(
            {
                "kind": "tracked_overlay_boundary",
                "path": item["path"],
                "message": item["message"],
                "violation": item["kind"],
            }
        )
    return findings


def _hash_local_files(root: Path) -> dict[str, str] | None:
    """Map of payload-relative path -> git blob OID for a surface's content.

    Works for a single file (key ``""``) or a directory tree. Returns ``None``
    when hashing fails (git unavailable or unreadable content) so the caller
    can skip classification instead of guessing.
    """
    import subprocess

    from workbay_bootstrap.external import run_external

    if root.is_file():
        files: dict[str, Path] = {"": root}
    else:
        files = {
            p.relative_to(root).as_posix(): p
            for p in sorted(root.rglob("*"))
            if p.is_file()
        }
    if not files:
        return {}
    try:
        out = run_external(
            ["git", "hash-object", "--", *[str(p) for p in files.values()]],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        return None
    if len(out) != len(files):
        return None
    return dict(zip(files.keys(), out))


def _payload_blob_index(
    clone: Path, sha: str, payload_rel: str
) -> dict[str, str] | None:
    """Blob OIDs for ``payload_rel`` at revision ``sha``, keyed like
    ``_hash_local_files`` (relative to the payload path, ``""`` for a file)."""
    import subprocess

    from workbay_bootstrap.external import run_external

    try:
        out = run_external(
            ["git", "-C", str(clone), "ls-tree", "-r", sha, "--", payload_rel],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    index: dict[str, str] = {}
    for line in out.splitlines():
        meta, _, path = line.partition("\t")
        parts = meta.split()
        if len(parts) != 3 or parts[1] != "blob":
            continue
        if path == payload_rel:
            rel = ""
        elif path.startswith(payload_rel + "/"):
            rel = path[len(payload_rel) + 1 :]
        else:
            continue
        index[rel] = parts[2]
    return index


def _strip_excluded_children(
    index: dict[str, str], excluded: frozenset[str]
) -> dict[str, str]:
    """Drop carve-excluded children from a surface blob index.

    A carved surface (:data:`SURFACE_CHILD_EXCLUSIONS`) is materialized
    *without* its excluded children, so comparing a local copy against the
    full payload tree would never match — every carved surface would
    misclassify as ``local_override``. Filtering both sides of the comparison
    restores the carve semantics: keys are payload-relative (``""`` for a
    file), so a key's first path component naming an excluded child drops it.
    """
    if not excluded:
        return index
    return {
        rel: oid for rel, oid in index.items() if rel.partition("/")[0] not in excluded
    }


def _matches_older_payload_revision(
    clone: Path,
    payload: Path,
    local_index: dict[str, str],
    *,
    excluded_children: frozenset[str] = frozenset(),
) -> bool:
    """True when the local content exactly matches the payload path at any
    revision in the clone's history — the update-starvation signature.

    ``excluded_children`` filters carve-excluded children out of each
    historical payload index (``local_index`` must already be filtered by the
    caller) so a carved surface can match the rest of an older revision even
    when that revision shipped the excluded children.
    """
    import subprocess

    try:
        payload_rel = payload.resolve().relative_to(clone.resolve()).as_posix()
    except ValueError:
        return False
    from workbay_bootstrap.external import run_external

    try:
        shas = run_external(
            ["git", "-C", str(clone), "log", "--format=%H", "--", payload_rel],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        return False
    for sha in shas:
        index = _payload_blob_index(clone, sha, payload_rel)
        if index is None:
            continue
        if _strip_excluded_children(index, excluded_children) == local_index:
            return True
    return False


def _doctor_local_surfaces(
    target: Path,
    clone: Path,
    manifest: dict[str, Any],
    *,
    is_package_source: bool,
) -> list[Finding]:
    """Classify ``source=local`` surfaces against the payload (implementation note S1).

    - identical to current payload -> ``local_redundant`` finding (adoption
      candidate)
    - identical to an *older* payload revision -> ``local_stale`` finding (a
      bootstrap-era copy silently starving updates)
    - matches no payload revision -> ``local_override`` with ``severity=info``:
      consumer-authored, respected, never repair-eligible, never affects the
      doctor exit code.

    History matching needs the git clone; the package source has none, so its
    divergent local content falls back to ``local_override`` (safe default)
    while ``local_redundant`` still works via filesystem comparison.
    """
    local_entries = [
        entry
        for entry in (manifest.get("surfaces") or [])
        if isinstance(entry, dict) and entry.get("source") == "local"
    ]
    if not local_entries:
        return []
    history_available = False
    if is_package_source:
        from workbay_bootstrap.install import _package_source_root

        try:
            source_root = _package_source_root(None)
        except Exception:
            return []
    else:
        if not (clone / ".git").exists():
            # missing_clone is already reported; nothing to diff against.
            return []
        source_root = clone
        history_available = True
    findings: list[Finding] = []
    for entry in local_entries:
        surface = entry.get("path", "")
        local = target / surface
        if not local.exists():
            # Broken symlink or deleted path: name it instead of skipping
            # silently (the never-silent goal of implementation note). Informational
            # only — nothing exists to drift-classify or adopt.
            findings.append(
                {"kind": "local_missing", "path": surface, "severity": "info"}
            )
            continue
        payload = _resolve_in_clone(source_root, surface)
        if not payload.exists():
            # No shipped counterpart: consumer-owned path, nothing to compare.
            continue
        local_index = _hash_local_files(local)
        if local_index is None:
            # Hashing failed (git unavailable / unreadable content): name the
            # surface rather than leaving it indistinguishable from clean.
            findings.append(
                {"kind": "local_unreadable", "path": surface, "severity": "info"}
            )
            continue
        # Carved surfaces are materialized without their excluded children;
        # compare both sides minus those children or the carve itself would
        # force every carved surface into local_override. Generated-kind
        # children are likewise stripped so derived files do not force
        # local_override.
        generated_under = [
            g for g in surfaces_for_kind("generated") if _is_under(surface, g)
        ]
        if any(g == surface for g in generated_under):
            continue
        excluded = SURFACE_CHILD_EXCLUSIONS.get(surface, frozenset())
        generated_children = frozenset(
            _rel_under(surface, g) for g in generated_under
        )
        excluded = excluded | generated_children
        local_index = _strip_excluded_children(local_index, excluded)
        payload_index = _hash_local_files(payload)
        if payload_index is not None and local_index == _strip_excluded_children(
            payload_index, excluded
        ):
            findings.append({"kind": "local_redundant", "path": surface})
            continue
        if history_available and _matches_older_payload_revision(
            clone, payload, local_index, excluded_children=excluded
        ):
            findings.append({"kind": "local_stale", "path": surface})
            continue
        findings.append({"kind": "local_override", "path": surface, "severity": "info"})
    return findings


_MANAGED_SURFACE_BY_CONFIG_PATH: dict[str, str] = {
    ".mcp.json": "claude",
    ".vscode/mcp.json": "vscode",
    ".codex/config.toml": "codex",
    ".cursor/mcp.json": "cursor",
}
_CONFIG_PATH_BY_MANAGED_SURFACE: dict[str, str] = {
    surface: path for path, surface in _MANAGED_SURFACE_BY_CONFIG_PATH.items()
}


def _registered_managed_surfaces(manifest: Mapping[str, object]) -> list[str]:
    """Return the managed-surface names recorded in the ledger's configs.

    Doctor / repair only reconcile surfaces that ``install`` actually
    wrote. Legacy ledgers without ``.codex/config.toml`` therefore skip
    codex even when the resolved map is supplied.
    """
    registered: list[str] = []
    for entry in manifest.get("configs") or []:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        surface = _MANAGED_SURFACE_BY_CONFIG_PATH.get(str(path))
        if surface is not None and surface not in registered:
            registered.append(surface)
    return registered


def _doctor_mcp_config_drift(
    target: Path,
    manifest: Mapping[str, object],
    mcp_servers: Mapping[str, Mapping[str, Any]],
) -> list[Finding]:
    """Run ``sync_mcp_configs(check_only=True)`` and translate per-surface
    drift into ``config_drift`` findings.

    Filtered to surfaces the ledger says ``install`` wrote so doctor
    does not invent drift for surfaces the consumer never opted into.
    """
    from workbay_bootstrap.mcp_sync import sync_mcp_configs

    surfaces = _registered_managed_surfaces(manifest)
    if not surfaces:
        return []
    report = sync_mcp_configs(target, mcp_servers, surfaces=surfaces, check_only=True)
    return [
        {"kind": "config_drift", "path": s.path} for s in report.surfaces if s.drift
    ]


def _shim_primary_checkout_root(target: Path) -> Path | None:
    """Primary checkout root for a linked worktree, or ``None``.

    Mirrors the launcher shim's ``_primary_checkout_root``: the parent of the
    shared ``--git-common-dir``. A linked worktree (feature / review / harness
    session) often has no ``.venv`` of its own while the primary checkout does,
    and the shim heals to the primary's console script — so this check must probe
    the primary too, or it would false-positive on a perfectly launchable
    worktree. Best-effort: ``None`` when ``target`` is not inside a git repo.
    """
    from workbay_bootstrap.worktree import (
        NotAGitRepositoryError,
        _common_and_git_dir,
    )

    try:
        common, _ = _common_and_git_dir(target)
    except NotAGitRepositoryError:
        return None
    return common.parent


def _local_mcp_console_present(target: Path, relpath: str, console: str) -> bool:
    """True when the shim's fast-path console script resolves for ``target``.

    Mirrors the launcher shim's ``_console_path`` exactly, so the doctor neither
    false-negatives nor false-positives against the real launch decision:

    - probes the git toplevel (``target``) and, for a linked worktree, the
      *primary* checkout root (the shim heals a venv-less worktree to the
      primary);
    - per-package ``<pkg>/.venv`` across all roots, then the shared root
      ``.venv`` — the shim's precedence order;
    - POSIX ``bin/<console>`` and Windows ``Scripts/<console>.exe``;
    - a candidate must *exist and be executable* on POSIX (existence only on
      Windows). A present-but-non-executable script makes the shim skip it and
      drop to the slow ``uv run`` path, so it must not read as present here —
      otherwise the doctor stays silent on exactly the half-built venv this
      check exists to flag.
    """
    roots = [target]
    primary = _shim_primary_checkout_root(target)
    if primary is not None and primary not in roots:
        roots.append(primary)
    venvs = [root / relpath / ".venv" for root in roots]
    venvs += [root / ".venv" for root in roots]
    for venv in venvs:
        for rel in (("bin", console), ("Scripts", f"{console}.exe")):
            candidate = venv.joinpath(*rel)
            if candidate.exists() and (
                os.name == "nt" or os.access(candidate, os.X_OK)
            ):
                return True
    return False


def _doctor_local_mcp_console_scripts(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
) -> list[Finding]:
    """Flag a missing fast-path console script for a shim-routed local server.

    internal. When the generator routes an in-tree MCP server through
    the launcher shim (the cwd-independent ``sh -c`` wrapper), the shim execs
    ``packages/<pkg>/.venv/bin/<console>`` directly. If that script is absent the
    server still starts -- the shim falls back to the slower ``uv run`` -- but
    under concurrent-boot contention that slow path is exactly what overruns the
    MCP startup deadline and silently drops the server's tools for the session.
    Surface it as a ``warning`` so an operator re-runs install/repair (which
    presyncs the per-package venv) before it becomes a silent boot miss.
    uv-run-routed (git_overlay) specs have no shim fast path and are skipped.
    """
    from workbay_bootstrap.install import _LOCAL_MCP_SERVERS, _launch_shim_server_id

    consoles = {sid: (rel, console) for sid, rel, console in _LOCAL_MCP_SERVERS}
    findings: list[Finding] = []
    for server_id, spec in mcp_servers.items():
        # Only the shim launcher (now the cwd-independent ``sh -c`` wrapper, or
        # the legacy ``python3`` form) has the per-package fast-path venv; a
        # uv-run / uvx spec has no shim fast path and is skipped.
        if _launch_shim_server_id(spec.get("command"), spec.get("args")) is None:
            continue
        entry = consoles.get(server_id)
        if entry is None:
            continue
        relpath, console = entry
        if _local_mcp_console_present(target, relpath, console):
            continue
        findings.append(
            {
                "kind": "mcp_console_missing",
                "path": f"{relpath}/.venv/bin/{console}",
                "severity": "warning",
                "detail": (
                    f"{server_id}: shim fast-path console script is missing; MCP "
                    "launch will fall back to the slower uv-run path (a boot-miss "
                    "risk under concurrent startup). Re-run install/repair to "
                    "presync the per-package venv."
                ),
            }
        )
    return findings


def _doctor_state(target: Path, manifest: dict[str, object]) -> list[Finding]:
    # implementation note §4: the manifest's configs array records whether bootstrap
    # registered MCP servers. .mcp.json is only present when an mcp_servers
    # map was provided, so its presence is the gate for expecting init-state
    # to have run. --no-mcp-servers installs leave .mcp.json out of configs
    # and must not trigger state_drift.
    registered_mcp = any(
        isinstance(entry, dict) and entry.get("path") == ".mcp.json"
        for entry in manifest.get("configs") or []
    )
    if not registered_mcp:
        return []

    db_path = target / ".task-state" / "handoff.db"
    if db_path.is_file():
        return []
    return [{"kind": "state_drift", "path": ".task-state/handoff.db"}]


def _doctor_unharvested_agent_errors(target: Path) -> list[Finding]:
    """Informational note when agent_errors rows await harvest (implementation note).

    Plan-review decision 3: ``severity=info`` only — listed with the
    ``note`` prefix and never affecting the doctor exit code, matching
    the implementation note ``local_override`` convention. Silent when the DB is
    missing, unreadable, predates schema v12 (no ``agent_errors``
    table), or the table is empty; a telemetry note must never make
    doctor itself fail.
    """
    import sqlite3

    db_path = target / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'agent_errors'"
            ).fetchone()
            if table is None:
                return []
            total = conn.execute("SELECT COUNT(*) FROM agent_errors").fetchone()[0]
            if not total:
                return []
            top = conn.execute(
                "SELECT error_class, COUNT(*) AS n FROM agent_errors"
                " GROUP BY error_class ORDER BY n DESC, error_class ASC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    return [
        {
            "kind": "unharvested_agent_errors",
            "path": ".task-state/handoff.db",
            "severity": "info",
            "detail": (
                f"{total} unharvested error rows, top class {top[0]} — "
                "run `make errors-report`"
            ),
        }
    ]


def _doctor_agent_errors_silence(target: Path) -> list[Finding]:
    """Warn when error telemetry is silent while other ledgers keep growing."""
    import sqlite3

    db_path = target / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            tables = {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            if "agent_errors" not in tables or "decisions" not in tables:
                return []
            recent_errors = int(
                conn.execute(
                    "SELECT COUNT(*) FROM agent_errors WHERE last_seen_at >= datetime('now', '-7 days')"
                ).fetchone()[0]
            )
            recent_decisions = int(
                conn.execute(
                    "SELECT COUNT(*) FROM decisions WHERE created_at >= datetime('now', '-7 days')"
                ).fetchone()[0]
            )
            stale_maint = 0
            if "handoff_state" in tables:
                stale_maint = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM handoff_state "
                        "WHERE status = 'in_progress' "
                        "AND target_branch = 'main' "
                        "AND updated_at < datetime('now', '-7 days')"
                    ).fetchone()[0]
                )
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    if recent_errors or not recent_decisions:
        return []
    return [
        {
            "kind": "agent_errors_silence",
            "path": ".task-state/handoff.db",
            "severity": "warning",
            "detail": (
                f"0 agent_errors rows in the last 7 days while {recent_decisions} decisions rows grew; "
                f"{stale_maint} stale MAINT-on-main rows"
            ),
        }
    ]


# Freshness gate threshold (hours). Env override: WORKBAY_TELEMETRY_FRESH_MAX_AGE_HOURS.
# Default 24h ships now; later tuning is governed by [PERF-06] measure-don't-guess.
_TELEMETRY_FRESH_MAX_AGE_HOURS_DEFAULT = 24
_TELEMETRY_FRESH_MAX_AGE_ENV = "WORKBAY_TELEMETRY_FRESH_MAX_AGE_HOURS"
# Grounded against Slice-1 agent_errors.py spool names + quarantine stamp.
_AGENT_ERRORS_SPOOL_NAME = "agent-errors-spool.jsonl"
_AGENT_ERRORS_SPOOL_REL = f".task-state/{_AGENT_ERRORS_SPOOL_NAME}"
_SPOOL_AT_FIELD = "spooled_at"
_REPLAY_ATTEMPTS_FIELD = "_replay_attempts"
_SPOOL_TS_FMT = "%Y-%m-%d %H:%M:%S"


def _telemetry_fresh_max_age_hours() -> float:
    """Return max age hours for the freshness gate (default 24h).

    Override via env ``WORKBAY_TELEMETRY_FRESH_MAX_AGE_HOURS`` ([PERF-06] later tuning).
    Invalid/empty values fall back to the default — never raise.
    """
    raw = os.environ.get(_TELEMETRY_FRESH_MAX_AGE_ENV)
    if raw is None or not str(raw).strip():
        return float(_TELEMETRY_FRESH_MAX_AGE_HOURS_DEFAULT)
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return float(_TELEMETRY_FRESH_MAX_AGE_HOURS_DEFAULT)
    if value <= 0:
        return float(_TELEMETRY_FRESH_MAX_AGE_HOURS_DEFAULT)
    return value


def _replay_quarantine_after_attempts() -> int:
    """Poison-line threshold from Slice-1 producer; fallback 3 if import fails."""
    try:
        from workbay_handoff_mcp.agent_errors import (  # noqa: PLC0415
            REPLAY_QUARANTINE_AFTER_ATTEMPTS,
        )

        return int(REPLAY_QUARANTINE_AFTER_ATTEMPTS)
    except Exception:  # noqa: BLE001 - doctor must never raise on import drift
        return 3


def _parse_spool_timestamp(raw: object) -> datetime | None:
    """Parse producer ``spooled_at`` (``%Y-%m-%d %H:%M:%S``, UTC) or None."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    try:
        return datetime.strptime(text, _SPOOL_TS_FMT).replace(tzinfo=UTC)
    except ValueError:
        # Accept trailing Z / offset if a future writer adds them.
        try:
            normalized = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)


def _iter_replayable_spool_ages(
    spool_path: Path, *, quarantine_after: int
) -> list[datetime]:
    """Return ``spooled_at`` times for replayable-undrained spool lines.

    Skips malformed lines and lines at/above the quarantine attempt count
    (poison lines must not hold the gate RED — [OBS-08]↔[OBS-04]). Dead-letter
    sidecar lines live in a separate file and are never present here.
    Never raises.
    """
    ages: list[datetime] = []
    try:
        text = spool_path.read_text(encoding="utf-8")
    except OSError:
        return ages
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            event = json.loads(stripped)
        except ValueError:
            continue
        if not isinstance(event, dict):
            continue
        try:
            attempts = int(event.get(_REPLAY_ATTEMPTS_FIELD) or 0)
        except (TypeError, ValueError):
            attempts = 0
        if attempts >= quarantine_after:
            continue
        ts = _parse_spool_timestamp(event.get(_SPOOL_AT_FIELD))
        if ts is not None:
            ages.append(ts)
    return ages


def _doctor_stale_dev_temp(target: Path) -> list[Finding]:
    """Non-blocking note when stale ``/tmp/workbay-*`` dirs remain (implementation note S4).

    Severity is ``warning`` (or quiet when none) so the doctor exit code stays
    green — same posture as 0096 skew / informational probes, not the 0095
    freshness *error* gate. ``target`` is unused (global /tmp scan); accepted
    for facet signature parity. Never raises.
    """
    del target  # global temp root; facet signature matches siblings
    try:
        from workbay_bootstrap.dev_temp import reap_stale_dev_temp
    except Exception:  # noqa: BLE001 — doctor must never raise on import drift
        return []
    try:
        report = reap_stale_dev_temp(apply=False)
    except Exception:  # noqa: BLE001 — never-raise degrade
        return []
    try:
        count = int(report.get("stale_count") or 0)
    except (TypeError, ValueError):
        count = 0
    if count <= 0:
        return []
    try:
        max_age = float(report.get("max_age_h") or 24)
    except (TypeError, ValueError):
        max_age = 24.0
    tmp_root = str(report.get("tmp_root") or "/tmp")
    return [
        {
            "kind": "stale_dev_temp",
            "path": f"{tmp_root}/workbay-*",
            # Informational note only (same non-blocking notes path as
            # ``_doctor_unharvested_agent_errors``): stale dev-temp is reclaimed
            # on the lifecycle cadence, so it must NOT fall through to repair()'s
            # generic "re-run with --force-dirty to overwrite" skip noise.
            "severity": "info",
            "detail": (
                f"{count} stale workbay-* dir(s) under {tmp_root} older than "
                f"{max_age:g}h — reclaimed on lifecycle cadence "
                f"(reap_stale_dev_temp apply=True) or after preflight self-clean"
            ),
        }
    ]


def _doctor_telemetry_freshness(target: Path) -> list[Finding]:
    """Fail-loud when a replayable spool entry is itself stale.

    internal / [OBS-08]: severity MUST be ``error`` so
    ``workbay_bootstrap.cli`` classifies the finding as actionable (info/warning
    land in non-exit ``notes``). Removing Makefile ``|| true`` alone is inert
    without this severity.

    FAIL when a replayable-undrained spool line has ``spooled_at`` older than the
    threshold (default 24h; env ``WORKBAY_TELEMETRY_FRESH_MAX_AGE_HOURS``).

    This is the sound signal: drained lines LEAVE the spool, so an undrained line
    IS the lag — and its own ``spooled_at`` age is what matters. A broken drain
    still trips this within ``max_age``. Comparing the spool to the newest
    ``agent_errors`` row was dropped (REV-S2-1): in a low-error repo the table's
    newest row is naturally old, so a FRESH undrained line (spooled just now, will
    drain next lifecycle event) would false-positive the gate RED.

    Quarantined / dead-lettered lines are excluded (attempt stamp ≥ N, or already
    moved to the dead-letter sidecar). Quiet systems (no/empty spool, or only
    quarantined lines) PASS. Missing spool / malformed lines never raise.
    """
    spool_path = target / ".task-state" / _AGENT_ERRORS_SPOOL_NAME
    if not spool_path.is_file():
        return []

    quarantine_after = _replay_quarantine_after_attempts()
    ages = _iter_replayable_spool_ages(spool_path, quarantine_after=quarantine_after)
    if not ages:
        # Empty spool, only poison lines, or unparseable — not a harvest stall.
        return []

    max_age = timedelta(hours=_telemetry_fresh_max_age_hours())
    now = datetime.now(tz=UTC)
    oldest_spool = min(ages)

    reasons: list[str] = []
    stale_count = sum(1 for ts in ages if now - ts > max_age)
    if stale_count:
        age_hours = (now - oldest_spool).total_seconds() / 3600.0
        reasons.append(
            f"{stale_count} replayable spool entr"
            f"{'y' if stale_count == 1 else 'ies'} older than "
            f"{max_age.total_seconds() / 3600.0:g}h "
            f"(oldest spooled_at={oldest_spool.strftime(_SPOOL_TS_FMT)}Z, "
            f"~{age_hours:.1f}h ago)"
        )

    if not reasons:
        return []
    return [
        {
            "kind": "telemetry_freshness",
            "path": _AGENT_ERRORS_SPOOL_REL,
            "severity": "error",
            "detail": (
                "; ".join(reasons)
                + " — run `make context` (or `mcp-workbay-handoff errors-replay-spool`) "
                "to drain the agent-errors spool"
            ),
        }
    ]


_COMPACT_SESSION_HOOK_ID = "compact-session"


def _manifest_hook_adapters(clone: Path) -> list[tuple[str, str, Mapping[str, Any]]]:
    """Return every manifest hook adapter as ``(hook_id, target, adapter)``.

    internal generalized this from the compact-session-only
    loader so doctor/repair cover every ``hooks[]`` family. Tuples (not a
    target-keyed dict) because two families may share one target path —
    compact-session ``$.hooks.Stop`` and reinject-context
    ``$.hooks.SessionStart`` both patch ``.claude/settings.json``. Read
    from the portable-commands manifest in the clone — the same source
    ``install`` walks. Returns ``[]`` when the manifest predates schema v2
    (no ``hooks`` array) so doctor stays a noop on legacy overlays.
    """
    from workbay_bootstrap.install import _load_portable_manifest

    portable = _load_portable_manifest(clone)
    rows: list[tuple[str, str, Mapping[str, Any]]] = []
    for hook in portable.get("hooks") or []:
        if not isinstance(hook, Mapping):
            continue
        hook_id = str(hook.get("hook_id") or "")
        for adapter in hook.get("adapters") or []:
            if not isinstance(adapter, Mapping):
                continue
            tgt = adapter.get("target")
            if isinstance(tgt, str):
                rows.append((hook_id, tgt, adapter))
    return rows


def _managed_hook_adapters(
    clone: Path, manifest: Mapping[str, object]
) -> list[tuple[str, str, Mapping[str, Any]]]:
    """Subset of :func:`_manifest_hook_adapters` that bootstrap installed.

    Managed-ness is per opt-in flag, matched against the ``kind=hook_adapter``
    rows internal tags in the install manifest — NOT per path, because
    two families can share one target file and only one may have been opted
    into. Legacy manifests without tagged rows fall back to path-membership
    scoped to compact-session (the only family that existed pre-tagging), so
    their doctor behavior is unchanged.
    """
    configs = [e for e in manifest.get("configs") or [] if isinstance(e, dict)]
    tagged_flags = {
        str(e.get("opt_in_flag")) for e in configs if e.get("kind") == "hook_adapter"
    }
    legacy_paths: set[object] = (
        {e.get("path") for e in configs} if not tagged_flags else set()
    )
    managed: list[tuple[str, str, Mapping[str, Any]]] = []
    for hook_id, tgt, adapter in _manifest_hook_adapters(clone):
        if tagged_flags:
            if str(adapter.get("opt_in_flag")) in tagged_flags:
                managed.append((hook_id, tgt, adapter))
        elif hook_id == _COMPACT_SESSION_HOOK_ID and tgt in legacy_paths:
            managed.append((hook_id, tgt, adapter))
    return managed


def _normalize_managed_hook_entry(
    entry: Mapping[str, Any], *, target: Path
) -> dict[str, Any]:
    from workbay_bootstrap.claude_settings import normalize_hook_command_for_compare

    normalized: dict[str, Any] = dict(entry)
    command = normalized.get("command")
    if isinstance(command, str):
        normalized["command"] = normalize_hook_command_for_compare(
            command, target=target
        )
    hooks = normalized.get("hooks")
    if isinstance(hooks, list):
        new_hooks: list[Any] = []
        for hook in hooks:
            if not isinstance(hook, Mapping):
                new_hooks.append(hook)
                continue
            hook_copy = dict(hook)
            hook_cmd = hook_copy.get("command")
            if isinstance(hook_cmd, str):
                hook_copy["command"] = normalize_hook_command_for_compare(
                    hook_cmd, target=target
                )
            new_hooks.append(hook_copy)
        normalized["hooks"] = new_hooks
    return normalized


def _managed_stop_adapter_drifted(
    settings_path: Path, adapter: Mapping[str, Any], *, target: Path
) -> bool:
    """True when the installed adapter file no longer carries the
    manifest-declared managed Stop entry.

    Drift covers: the file is gone, is unparseable, the patch container
    (e.g. ``$.hooks.Stop``) is missing, the managed entry (matched by
    ``match_key``) is absent, or it is present but differs from the
    manifest entry after normalizing portable command forms. A present,
    equivalent entry is clean.
    """
    if not settings_path.is_file():
        return True
    try:
        doc = json.loads(settings_path.read_text())
    except (OSError, ValueError):
        return True

    patch = adapter.get("patch") or {}
    json_path = str(patch.get("json_path", ""))
    match_key = patch.get("match_key")
    entry_raw = patch.get("entry")
    if not json_path.startswith("$.") or match_key is None:
        return True
    if not isinstance(entry_raw, Mapping) or match_key not in entry_raw:
        return True
    expected = _normalize_managed_hook_entry(entry_raw, target=target)

    node: Any = doc
    for seg in json_path[2:].split("."):
        if not isinstance(node, Mapping) or seg not in node:
            return True
        node = node[seg]
    if not isinstance(node, list):
        return True

    match_value = expected[match_key]
    for item in node:
        if isinstance(item, Mapping) and item.get(match_key) == match_value:
            installed = _normalize_managed_hook_entry(item, target=target)
            return installed != expected
    return True


def _doctor_managed_stop_adapters(
    target: Path, clone: Path, manifest: Mapping[str, object]
) -> list[Finding]:
    """Flag drift for hook adapters that bootstrap installed (any family).

    internal generalized the internal compact-session check to
    every manifest ``hooks[]`` family via :func:`_managed_hook_adapters`.
    Never-installed adapters stay optional and are not reported here —
    lifecycle doctor owns optional-not-installed visibility. One finding is
    emitted per drifted path (repair re-applies every managed adapter on
    that path).
    """
    findings: list[Finding] = []
    flagged: set[str] = set()
    for _hook_id, tgt, adapter in _managed_hook_adapters(clone, manifest):
        if tgt in flagged:
            continue
        if _managed_stop_adapter_drifted(target / tgt, adapter, target=target):
            findings.append({"kind": "hook_adapter_drift", "path": tgt})
            flagged.add(tgt)
    return findings


def _doctor_generated_surfaces(
    target: Path,
    clone: Path,
    manifest: dict[str, object],
    override_root: Path | None,
) -> list[Finding]:
    """Detect drift in per-agent generated surfaces.

    Runs ``scripts/generate_agent_workflows.py --check --target <target>``
    against the target. Each ``drift detected: <path>`` line in the
    generator's stderr is mapped back to the manifest's ``generated``
    surface that owns it; one ``generated_drift`` finding per affected
    surface (deduplicated). Silent when no generated surfaces are recorded
    (legacy manifests) or the generator script is missing from the clone
    (older overlay refs that pre-date implementation note).
    """

    surfaces = manifest.get("surfaces") or []
    generated_surfaces = [
        str(entry.get("path", ""))
        for entry in surfaces
        if entry.get("source") == "generated" and entry.get("path")
    ]
    if not generated_surfaces:
        return []

    plugin_root = Path(*PLUGIN_GENERATED_ROOT).as_posix()
    plugin_surfaces = [
        surface
        for surface in generated_surfaces
        if surface.startswith(plugin_root + "/")
    ]
    cursor_surfaces = {
        CURSOR_COMMANDS_DEST.as_posix(),
        CURSOR_SKILLS_DEST.as_posix(),
        CURSOR_HOOKS_PATH.as_posix(),
    }
    legacy_surfaces = [
        surface
        for surface in generated_surfaces
        if surface not in plugin_surfaces and surface not in cursor_surfaces
    ]

    # Package installs never persist ``remote_sha`` (their base anchor is a
    # synthetic ``sha1("workbay-system@<version>")``), but the override-aware
    # generator requires ``--plugin-base-remote-sha`` whenever overrides are in
    # play. Install writes that anchor into the override lock, so recover it
    # there when the manifest lacks one — otherwise doctor would --check
    # WITHOUT overrides while install emitted WITH them and report spurious
    # generated_drift on every package install carrying plugin overrides.
    base_remote_sha = _resolve_plugin_base_sha(manifest, override_root)

    findings: list[Finding] = []
    if legacy_surfaces:
        findings.extend(
            _doctor_legacy_generated_surfaces(
                target,
                clone,
                legacy_surfaces,
                override_root,
                base_remote_sha,
            )
        )
    if plugin_surfaces:
        findings.extend(
            _doctor_plugin_generated_surfaces(
                target,
                clone,
                plugin_surfaces,
                override_root,
                base_remote_sha,
            )
        )
    return findings


def _resolve_plugin_base_sha(
    manifest: Mapping[str, object], override_root: Path | None
) -> str | None:
    """Resolve the base remote SHA the override-aware generator needs.

    Prefers the manifest ``remote_sha`` (git-overlay installs). Falls back to
    the ``base_remote_sha`` recorded in the plugin override lock, which install
    writes from ``source.base_anchor`` for every install — including package
    installs that never persist ``remote_sha`` to the manifest.
    """
    remote_sha = manifest.get("remote_sha")
    if isinstance(remote_sha, str):
        return remote_sha
    if override_root is None:
        return None
    lock_path = override_root / PLUGIN_OVERRIDE_LOCK
    try:
        lock = json.loads(lock_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    base = lock.get("base_remote_sha") if isinstance(lock, dict) else None
    return base if isinstance(base, str) else None


def _doctor_legacy_generated_surfaces(
    target: Path,
    clone: Path,
    generated_surfaces: list[str],
    override_root: Path | None = None,
    remote_sha: str | None = None,
) -> list[Finding]:
    import sys

    from workbay_bootstrap.install import _resolve_in_clone

    generator_script = _resolve_in_clone(clone, GENERATOR_SCRIPT)
    manifest_path = _resolve_in_clone(clone, GENERATOR_MANIFEST)
    skills_source = _resolve_in_clone(clone, GENERATOR_SKILLS_SOURCE)
    if not generator_script.is_file() or not manifest_path.is_file():
        return []

    from workbay_bootstrap.external import ExternalCallTimeout, run_external

    cmd = [
        sys.executable,
        str(generator_script),
        "--manifest",
        str(manifest_path),
        "--skills-source-root",
        str(skills_source),
        "--target",
        str(target),
        "--check",
    ]
    if override_root is not None and isinstance(remote_sha, str):
        cmd.extend(
            [
                "--plugin-overrides",
                str(override_root),
                "--plugin-base-remote-sha",
                remote_sha,
            ]
        )

    try:
        proc = run_external(
            cmd,
            call_class="generator",
            cwd=str(clone),
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, ExternalCallTimeout):
        # Generator unavailable / hung — surface as a single coarse finding
        # rather than crashing doctor.
        return [
            {"kind": "generated_drift", "path": surface}
            for surface in generated_surfaces
        ]

    if proc.returncode == 0:
        return []

    # Parse "drift detected: <abs path>" lines and map each back to the
    # manifest surface whose target-relative path it sits under.
    drifted: set[str] = set()
    for line in (proc.stderr or "").splitlines():
        line = line.strip()
        if not line.startswith("drift detected:"):
            continue
        drifted_path = line.split(":", 1)[1].strip()
        try:
            rel = Path(drifted_path).resolve().relative_to(target)
        except (ValueError, OSError):
            continue
        rel_str = str(rel)
        for surface in generated_surfaces:
            if rel_str == surface or rel_str.startswith(surface.rstrip("/") + "/"):
                drifted.add(surface)
                break

    if not drifted:
        # Generator reported failure but we couldn't map any path. Flag
        # all generated surfaces coarsely so the operator knows there's
        # work to do.
        drifted.update(generated_surfaces)

    return [{"kind": "generated_drift", "path": surface} for surface in sorted(drifted)]


def _doctor_plugin_generated_surfaces(
    target: Path,
    clone: Path,
    generated_surfaces: list[str],
    override_root: Path | None,
    remote_sha: str | None = None,
) -> list[Finding]:
    import sys

    from workbay_bootstrap.install import _resolve_in_clone

    generator_script = _resolve_in_clone(clone, GENERATOR_SCRIPT)
    manifest_path = _resolve_in_clone(clone, GENERATOR_MANIFEST)
    skills_source = _resolve_in_clone(clone, GENERATOR_SKILLS_SOURCE)
    if not generator_script.is_file() or not manifest_path.is_file():
        return []

    findings: list[Finding] = []
    base_surface = Path(*PLUGIN_GENERATED_ROOT, "base").as_posix()
    effective_surface = Path(*PLUGIN_GENERATED_ROOT, "effective").as_posix()

    for surface in generated_surfaces:
        cmd = [
            sys.executable,
            str(generator_script),
            "--mode=plugin",
            "--manifest",
            str(manifest_path),
            "--skills-source-root",
            str(skills_source),
            "--plugin-out",
            str(target / surface),
            "--check",
        ]

        if surface == effective_surface:
            if not isinstance(remote_sha, str):
                findings.append({"kind": "generated_drift", "path": surface})
                continue
            if override_root is None:
                cmd.append("--plugin-passthrough-lock")
            else:
                cmd.extend(["--plugin-overrides", str(override_root)])
            cmd.extend(["--plugin-base-remote-sha", remote_sha])
        elif surface != base_surface:
            findings.append({"kind": "generated_drift", "path": surface})
            continue

        from workbay_bootstrap.external import ExternalCallTimeout, run_external

        try:
            proc = run_external(
                cmd,
                call_class="generator",
                cwd=str(clone),
                capture_output=True,
                text=True,
                check=False,
            )
        except (OSError, ExternalCallTimeout):
            findings.append({"kind": "generated_drift", "path": surface})
            continue

        if proc.returncode != 0 and not _plugin_check_reports_only_stale_override(
            target / surface, proc.stderr
        ):
            findings.append({"kind": "generated_drift", "path": surface})

    return findings


def _plugin_check_reports_only_stale_override(plugin_root: Path, stderr: str) -> bool:
    lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
    drift_markers = (
        "Plugin tree is out of sync",
        "missing plugin tree file:",
        "plugin tree drift:",
    )
    if any(any(marker in line for marker in drift_markers) for line in lines):
        return False

    lock_path = plugin_root / "plugin-lock.json"
    if not lock_path.is_file():
        return False

    try:
        payload = json.loads(lock_path.read_text())
    except json.JSONDecodeError:
        return False

    components = payload.get("components", [])
    # internal: merge_conflict (skill patch mode) is, like stale, an override
    # condition — not generated-tree drift. The dedicated doctor finding kinds
    # report it; generated_drift must not double-report.
    return any(
        isinstance(entry, dict) and entry.get("status") in {"stale", "merge_conflict"}
        for entry in components
    )


def _doctor_plugin_pin_targets(
    target: Path, override_root: Path | None
) -> list[Finding]:
    findings: list[Finding] = []
    # internal always-effective: the expected pin target is the effective tree
    # in every state; legacy base/ pins surface as repairable pin_target_drift.
    plugin_tree_kind = "effective"

    claude_path = target / CLAUDE_MARKETPLACE_PATH
    if claude_path.is_file():
        try:
            claude_payload = json.loads(claude_path.read_text())
        except json.JSONDecodeError:
            claude_payload = {}
        plugins = claude_payload.get("plugins")
        expected = _relative_plugin_tree_path(plugin_tree_kind, "claude")
        actual = None
        if isinstance(plugins, list):
            for plugin in plugins:
                if isinstance(plugin, dict) and plugin.get("name") == PLUGIN_NAME:
                    actual = plugin.get("source")
                    break
        if actual != expected:
            findings.append(
                {"kind": "pin_target_drift", "path": CLAUDE_MARKETPLACE_PATH.as_posix()}
            )

    codex_path = target / CODEX_MARKETPLACE_PATH
    if codex_path.is_file():
        try:
            codex_payload = json.loads(codex_path.read_text())
        except json.JSONDecodeError:
            codex_payload = {}
        plugins = codex_payload.get("plugins")
        expected = {
            "source": "local",
            "path": _relative_plugin_tree_path(plugin_tree_kind, "codex"),
        }
        actual = None
        if isinstance(plugins, list):
            for plugin in plugins:
                if isinstance(plugin, dict) and plugin.get("name") == PLUGIN_NAME:
                    actual = plugin.get("source")
                    break
        if actual != expected:
            findings.append(
                {"kind": "pin_target_drift", "path": CODEX_MARKETPLACE_PATH.as_posix()}
            )

    return findings


def _doctor_hook_coherence(target: Path) -> list[Finding]:
    """Hook-surface coherence facet (internal).

    Thin doctor projection over ``coherence.assess_hook_coherence`` — runs
    for every source kind (package, git_overlay, AND ``source=local``
    surfaces). Severity rides along so ``error`` findings (a config naming a
    script the harness cannot resolve; mixed-snapshot hook mounts) are
    distinguishable from advisory ``warning`` rows.
    """
    from .coherence import assess_hook_coherence

    findings: list[Finding] = []
    for finding in assess_hook_coherence(target):
        row = finding.as_doctor_finding()
        row["message"] = row.pop("detail")
        findings.append(row)
    return findings


def _git_common_dir_available(target: Path) -> bool:
    """True when ``git rev-parse --git-common-dir`` succeeds at ``target``.

    Used to guard surface-leak severity: git path probes fail-closed to
    False, so an absent repo would otherwise false-read every surface as a
    leak ([OBS-04]/[OBS-08]).
    """
    import subprocess

    from workbay_bootstrap.external import ExternalCallTimeout, run_external

    try:
        run_external(
            ["git", "-C", str(target), "rev-parse", "--git-common-dir"],
            call_class="git",
            check=True,
            capture_output=True,
            text=True,
        )
    except (
        FileNotFoundError,
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        ExternalCallTimeout,
    ):
        return False
    return True


def _doctor_materialized_surface_leaks(target: Path) -> list[Finding]:
    """internal: provenance surface present but neither tracked nor ignored.

    When git is available → ``error`` (actionable; non-zero doctor exit).
    When git is unavailable → ``warning`` only (probes fail-closed to False;
    do not alarm-fatigue absent-repo consumers).
    """
    from workbay_bootstrap.install import (
        GENERATED_SURFACES,
        SHARED_SURFACES,
        _git_path_is_ignored,
        _git_path_is_tracked,
        _harness_materialized_surfaces,
    )
    from workbay_bootstrap.surfaces import surfaces_for_kind

    git_ok = _git_common_dir_available(target)
    severity = "error" if git_ok else "warning"
    # Materialized provenance set (fence-aligned, not generator_input ledger paths).
    surfaces: list[str] = []
    seen: set[str] = set()
    for surface in (
        *SHARED_SURFACES,
        *GENERATED_SURFACES,
        *surfaces_for_kind("lifecycle"),
        *_harness_materialized_surfaces(),
    ):
        if surface in seen:
            continue
        seen.add(surface)
        surfaces.append(surface)

    findings: list[Finding] = []
    for surface in surfaces:
        path = target / surface
        if not path.exists() and not path.is_symlink():
            continue
        if _git_path_is_tracked(target, surface) or _git_path_is_ignored(
            target, surface
        ):
            continue
        findings.append(
            {
                "kind": "surface_leak",
                "severity": severity,
                "path": surface,
                "message": (
                    f"materialized surface {surface!r} is present but neither "
                    f"git-tracked nor git-ignored; it will leak into "
                    f"git status. Ensure the consumer ignore fence covers it "
                    f"(workbay install/update) or track it intentionally."
                    + (
                        ""
                        if git_ok
                        else " (git unavailable — severity downgraded to warning)"
                    )
                ),
            }
        )
    return findings


def _doctor_codex_activation_config(target: Path) -> list[Finding]:
    if not (target / CODEX_MARKETPLACE_PATH).is_file():
        return []

    path = target / CODEX_CONFIG_PATH
    if not path.is_file():
        return [
            {"kind": "codex_activation_drift", "path": CODEX_CONFIG_PATH.as_posix()}
        ]

    problems: list[str] = []
    try:
        payload = tomllib.loads(path.read_text())
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        return [
            {
                "kind": "codex_activation_drift",
                "path": CODEX_CONFIG_PATH.as_posix(),
                "message": f"invalid TOML: {exc}",
            }
        ]

    marketplaces = payload.get("marketplaces")
    if not isinstance(marketplaces, dict):
        problems.append("marketplaces must be a table")
        marketplace = None
    else:
        marketplace = marketplaces.get(PLUGIN_MARKETPLACE_NAME)
    if not isinstance(marketplace, dict):
        problems.append(f"missing marketplaces.{PLUGIN_MARKETPLACE_NAME}")
    else:
        if marketplace.get("source_type") != "local":
            problems.append(
                f"marketplaces.{PLUGIN_MARKETPLACE_NAME}.source_type must be local"
            )
        if marketplace.get("source") != ".":
            problems.append(f"marketplaces.{PLUGIN_MARKETPLACE_NAME}.source must be .")

    selector = f"{PLUGIN_NAME}@{PLUGIN_MARKETPLACE_NAME}"
    plugins = payload.get("plugins")
    if not isinstance(plugins, dict):
        problems.append("plugins must be a table")
        plugin = None
    else:
        plugin = plugins.get(selector)
    if not isinstance(plugin, dict) or not isinstance(plugin.get("enabled"), bool):
        problems.append(f'plugins."{selector}".enabled must be a boolean')

    if not problems:
        return []
    return [
        {
            "kind": "codex_activation_drift",
            "path": CODEX_CONFIG_PATH.as_posix(),
            "message": "; ".join(problems),
        }
    ]


def _resolve_plugin_source_path(
    target: Path,
    raw_path: object,
    *,
    field_name: str,
) -> tuple[Path | None, list[str]]:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None, [f"{field_name} must be a non-empty string"]
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return None, [f"{field_name} must be relative"]
    if ".." in candidate.parts:
        return None, [f"{field_name} must not traverse outside the repo"]
    resolved = (target / raw_path.removeprefix("./")).resolve(strict=False)
    try:
        resolved.relative_to(target.resolve())
    except ValueError:
        return None, [f"{field_name} resolves outside the repo"]
    return resolved, []


def _plugin_tree_integrity_problems(root: Path, harness: str) -> list[str]:
    manifest_dir = {
        "claude": ".claude-plugin",
        "codex": ".codex-plugin",
        "grok": ".grok-plugin",
    }.get(harness, f".{harness}-plugin")
    problems: list[str] = []
    if not root.is_dir():
        return [f"source path does not exist: {root}"]
    manifest_path = root / manifest_dir / "plugin.json"
    if not manifest_path.is_file():
        problems.append(f"missing {manifest_dir}/plugin.json")
    if harness == "grok" and not (root / "hooks" / "hooks.json").is_file():
        problems.append("missing hooks/hooks.json")
    # implementation note: the tree is self-describing — plugin.json declares
    # `mcpServers` only when the plugin owns MCP registration for this
    # harness. Require the sibling .mcp.json exactly then; a stray
    # .mcp.json without the manifest key is the dual-registration bug.
    if manifest_path.is_file():
        try:
            manifest_payload = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError):
            manifest_payload = {}
        declares_mcp = "mcpServers" in manifest_payload
        has_mcp_json = (root / ".mcp.json").is_file()
        if declares_mcp and not has_mcp_json:
            problems.append(
                "plugin.json declares mcpServers but sibling .mcp.json is missing"
            )
        elif not declares_mcp and has_mcp_json:
            problems.append(
                "stray .mcp.json (plugin.json does not declare mcpServers; "
                "dual MCP registration)"
            )
    skills_root = root / "skills"
    if not skills_root.is_dir():
        problems.append("missing skills/")
    elif not any(path.is_file() for path in skills_root.glob("*/SKILL.md")):
        problems.append("skills/ contains no SKILL.md entries")
    return problems


def _doctor_effective_tree_missing(target: Path) -> list[Finding]:
    """internal fresh-clone diagnosis: pins always target the effective tree,
    so an entirely absent effective root means install/update has not
    materialized the generated target yet. Subsumes per-pin source-drift
    noise for that state."""
    effective_root = target.joinpath(*PLUGIN_GENERATED_ROOT, "effective")
    if effective_root.is_dir():
        return []
    if (
        not (target / CLAUDE_MARKETPLACE_PATH).is_file()
        and not (target / CODEX_MARKETPLACE_PATH).is_file()
    ):
        return []
    try:
        display = effective_root.relative_to(target).as_posix()
    except ValueError:
        display = str(effective_root)
    return [
        {
            "kind": "effective_tree_missing",
            "path": display,
            "message": (
                "effective plugin tree is missing; run workbay-bootstrap "
                "install or update (or repair) to materialize it"
            ),
        }
    ]


def _doctor_plugin_source_integrity(target: Path) -> list[Finding]:
    findings: list[Finding] = []

    claude_path = target / CLAUDE_MARKETPLACE_PATH
    if claude_path.is_file():
        problems: list[str] = []
        try:
            payload = json.loads(claude_path.read_text())
        except json.JSONDecodeError as exc:
            payload = {}
            problems.append(f"invalid JSON: {exc}")
        plugins = payload.get("plugins") if isinstance(payload, dict) else None
        if not isinstance(plugins, list):
            problems.append("plugins must be a list")
        else:
            for plugin in plugins:
                if not isinstance(plugin, dict) or plugin.get("name") != PLUGIN_NAME:
                    continue
                root, path_problems = _resolve_plugin_source_path(
                    target,
                    plugin.get("source"),
                    field_name="plugins[].source",
                )
                problems.extend(path_problems)
                if root is not None:
                    problems.extend(_plugin_tree_integrity_problems(root, "claude"))
                break
            else:
                problems.append(f"missing {PLUGIN_NAME} plugin entry")
        if problems:
            findings.append(
                {
                    "kind": "plugin_source_drift",
                    "path": CLAUDE_MARKETPLACE_PATH.as_posix(),
                    "message": "; ".join(problems),
                }
            )

    codex_path = target / CODEX_MARKETPLACE_PATH
    if codex_path.is_file():
        problems = []
        try:
            payload = json.loads(codex_path.read_text())
        except json.JSONDecodeError as exc:
            payload = {}
            problems.append(f"invalid JSON: {exc}")
        plugins = payload.get("plugins") if isinstance(payload, dict) else None
        if not isinstance(plugins, list):
            problems.append("plugins must be a list")
        else:
            for plugin in plugins:
                if not isinstance(plugin, dict) or plugin.get("name") != PLUGIN_NAME:
                    continue
                source = plugin.get("source")
                if not isinstance(source, dict) or source.get("source") != "local":
                    problems.append("plugins[].source.source must be local")
                    break
                root, path_problems = _resolve_plugin_source_path(
                    target,
                    source.get("path"),
                    field_name="plugins[].source.path",
                )
                problems.extend(path_problems)
                if root is not None:
                    problems.extend(_plugin_tree_integrity_problems(root, "codex"))
                break
            else:
                problems.append(f"missing {PLUGIN_NAME} plugin entry")
        if problems:
            findings.append(
                {
                    "kind": "plugin_source_drift",
                    "path": CODEX_MARKETPLACE_PATH.as_posix(),
                    "message": "; ".join(problems),
                }
            )

    findings.extend(_doctor_grok_plugin_materialization(target))
    findings.extend(_doctor_cursor_native_surfaces(target))
    return findings


def _doctor_cursor_native_surfaces(target: Path) -> list[Finding]:
    problems = cursor_native_surface_problems(target)
    if not problems:
        return []
    return [
        {
            "kind": "cursor_native_surface_drift",
            "path": ".cursor",
            "message": "; ".join(problems),
        }
    ]


def _doctor_grok_plugin_materialization(target: Path) -> list[Finding]:
    """Validate materialized grok plugin tree, activation, and stale selectors."""
    from workbay_bootstrap.activation import (
        GROK_BARE_SELECTOR,
        detect_stale_grok_discovery_selectors,
        grok_bare_selector_enabled,
        grok_plugin_surface_problems,
        grok_surface_is_foreign_local,
    )

    findings: list[Finding] = []
    effective_root = _plugin_tree_out(target, "effective") / "grok"
    if not effective_root.is_dir():
        return []

    if grok_surface_is_foreign_local(target):
        # Operator-owned local content is preserved (skipped_foreign_content),
        # never integrity-checked, and never repair-eligible: surface it as a
        # non-fatal advisory instead of perpetual unfixable drift.
        findings.append(
            {
                "kind": "grok_plugin_local_precedence",
                "path": GROK_PLUGIN_DEST.as_posix(),
                "severity": "warning",
                "message": (
                    "operator-owned content is kept under local precedence; "
                    "remove it and re-run repair/adopt to restore the managed "
                    "grok plugin symlink"
                ),
            }
        )

    surface_problems = grok_plugin_surface_problems(target)
    if surface_problems:
        findings.append(
            {
                "kind": "grok_plugin_drift",
                "path": GROK_PLUGIN_DEST.as_posix(),
                "message": "; ".join(surface_problems),
            }
        )
        return findings

    dest_root = target / GROK_PLUGIN_DEST
    if dest_root.exists():
        resolved = dest_root.resolve() if dest_root.is_symlink() else dest_root
        manifest_path = resolved / ".grok-plugin" / "plugin.json"
        if manifest_path.is_file():
            try:
                payload = json.loads(manifest_path.read_text())
            except json.JSONDecodeError as exc:
                findings.append(
                    {
                        "kind": "grok_plugin_drift",
                        "path": GROK_PLUGIN_DEST.as_posix(),
                        "message": f"invalid plugin.json: {exc}",
                    }
                )
            else:
                if isinstance(payload, dict) and payload.get("mcpServers"):
                    findings.append(
                        {
                            "kind": "grok_plugin_drift",
                            "path": GROK_PLUGIN_DEST.as_posix(),
                            "message": (
                                "plugin.json must not declare mcpServers "
                                "(compat .mcp.json is authoritative)"
                            ),
                        }
                    )
        for rel_path in (
            Path("hooks") / "hooks.json",
            Path(".grok-plugin") / "plugin.json",
        ):
            expected_path = effective_root / rel_path
            actual_path = resolved / rel_path
            if expected_path.is_file() and actual_path.is_file():
                if expected_path.read_bytes() != actual_path.read_bytes():
                    findings.append(
                        {
                            "kind": "grok_plugin_drift",
                            "path": GROK_PLUGIN_DEST.as_posix(),
                            "message": (
                                f"{rel_path.as_posix()} differs from effective "
                                "grok plugin tree"
                            ),
                        }
                    )

    enabled = grok_bare_selector_enabled()
    if enabled is False:
        findings.append(
            {
                "kind": "grok_activation_drift",
                "path": GROK_PLUGIN_DEST.as_posix(),
                "message": (
                    f"Grok plugin {GROK_BARE_SELECTOR!r} is not enabled in "
                    "~/.grok/config.toml; run "
                    f"'grok plugin install {GROK_PLUGIN_DEST.as_posix()} --trust' "
                    f"&& grok plugin enable {GROK_BARE_SELECTOR}"
                ),
            }
        )

    stale_selectors = detect_stale_grok_discovery_selectors()
    for selector in stale_selectors:
        findings.append(
            {
                "kind": "grok_stale_selector_warning",
                "path": "~/.grok/config.toml",
                "severity": "warning",
                "message": (
                    f"stale discovery selector {selector!r} in ~/.grok/config.toml; "
                    f"prefer bare-name enablement via "
                    f"'grok plugin enable {GROK_BARE_SELECTOR}' "
                    "(bootstrap never writes hash selectors)"
                ),
            }
        )

    return findings


def _doctor_hidden_override_collisions(
    target: Path, clone: Path, override_root: Path | None
) -> list[Finding]:
    if override_root is None:
        return []

    override_manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    if not override_manifest_path.is_file():
        return []

    try:
        override_manifest = yaml.safe_load(override_manifest_path.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return []

    declared_paths: set[str] = set()
    components = override_manifest.get("components")
    if isinstance(components, dict):
        skills = components.get("skills")
        if isinstance(skills, dict):
            for spec in skills.values():
                if not isinstance(spec, dict):
                    continue
                path = spec.get("path")
                if isinstance(path, str) and path:
                    declared_paths.add(path)

    override_skills_root = override_root / "skills"
    if not override_skills_root.is_dir():
        return []

    skills_source_root = _resolve_in_clone(clone, GENERATOR_SKILLS_SOURCE)
    if not skills_source_root.is_dir():
        return []

    findings: list[Finding] = []
    for candidate in sorted(override_skills_root.glob("*/SKILL.md")):
        relative_path = candidate.relative_to(override_root).as_posix()
        if relative_path in declared_paths:
            continue
        if not (skills_source_root / candidate.parent.name).is_dir():
            continue
        findings.append(
            {
                "kind": "hidden_override_collision",
                "path": _plugin_override_display_path(
                    target, override_root, relative_path
                ),
            }
        )

    return findings


def _plugin_override_display_path(
    target: Path, override_root: Path | None, relative_path: str
) -> str:
    if override_root is None:
        return Path(*PLUGIN_OVERRIDE_ROOT, relative_path).as_posix()

    candidate = override_root / relative_path
    try:
        return candidate.relative_to(target).as_posix()
    except ValueError:
        return candidate.as_posix()


def _doctor_plugin_override_state(
    target: Path, override_root: Path | None
) -> list[Finding]:
    lock_path = target.joinpath(*PLUGIN_GENERATED_ROOT, "effective", "plugin-lock.json")

    findings: list[Finding] = []

    if lock_path.is_file():
        try:
            payload = json.loads(lock_path.read_text())
        except json.JSONDecodeError:
            payload = {}

        # internal: merge_conflict (skill patch mode) gets its own finding kind
        # alongside the existing stale_override reporting.
        status_kinds = {
            "stale": "stale_override",
            "merge_conflict": "override_merge_conflict",
        }
        for entry in payload.get("components", []):
            if not isinstance(entry, dict):
                continue
            kind = status_kinds.get(entry.get("status"))
            if kind is None:
                continue
            override_path = entry.get("override_path")
            if not isinstance(override_path, str) or not override_path:
                continue
            findings.append(
                {
                    "kind": kind,
                    "path": _plugin_override_display_path(
                        target, override_root, override_path
                    ),
                }
            )

    override_lock_path = (
        None if override_root is None else override_root / "overrides.lock.json"
    )
    if not override_lock_path or not override_lock_path.is_file():
        return findings

    try:
        override_payload = json.loads(override_lock_path.read_text())
    except json.JSONDecodeError:
        return findings

    unsafe_op_names = {
        "replace_command",
        "replace_args",
        "append_args",
        "upsert_env",
        "remove_env",
    }
    seen_paths: set[str] = set()
    for entry in override_payload.get("components", []):
        if not isinstance(entry, dict) or entry.get("component_kind") != "mcp_server":
            continue
        patch_path = entry.get("patch_path")
        if not isinstance(patch_path, str) or not patch_path:
            continue

        patch_file = override_root / patch_path
        display_path = _plugin_override_display_path(target, override_root, patch_path)
        try:
            patch_payload = yaml.safe_load(patch_file.read_text()) or {}
        except (OSError, yaml.YAMLError):
            if display_path not in seen_paths:
                seen_paths.add(display_path)
                findings.append(
                    {"kind": "invalid_override_schema", "path": display_path}
                )
            continue

        ops = patch_payload.get("ops")
        if not isinstance(ops, list):
            if display_path not in seen_paths:
                seen_paths.add(display_path)
                findings.append(
                    {"kind": "invalid_override_schema", "path": display_path}
                )
            continue
        if not any(
            isinstance(op, dict) and op.get("op") in unsafe_op_names for op in ops
        ):
            continue

        if display_path in seen_paths:
            continue
        seen_paths.add(display_path)
        findings.append({"kind": "unsafe_tool_patch", "path": display_path})

    return findings


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


def _adopt_redundant_surfaces(target: Path) -> list[str]:
    """Adopt every ``local_redundant`` surface after an install/update.

    implementation note: a local surface byte-identical to the current payload only
    differs in materialization mode, so adoption (backup + re-materialize +
    receipt ``source=shared``) is provably safe to run unattended. Delegates
    to :func:`repair`'s implementation note adoption path with exactly the redundant
    set; ``local_stale`` remains opt-in and ``local_override`` untouched.
    """
    manifest = _load_manifest(target)
    clone = target.joinpath(*CLONE_SUBDIR)
    is_package_source = str(manifest.get("source_kind") or "git_overlay") == "package"
    findings = _doctor_local_surfaces(
        target, clone, manifest, is_package_source=is_package_source
    )
    redundant = sorted({f["path"] for f in findings if f["kind"] == "local_redundant"})
    if not redundant:
        return []
    report = repair(target=target, adopt_stale_local=redundant)
    return sorted(
        f["path"] for f in report["repaired"] if f["kind"] == "local_redundant"
    )


def apply_hooks(
    *,
    target: Path,
    install_claude_stop_hook_local: bool = False,
    install_codex_stop_hook: bool = False,
    install_vscode_stop_hook: bool = False,
    install_grok_stop_hook: bool = False,
    install_claude_reinject_hook_local: bool = False,
    install_codex_ensure_agent_surfaces_hook: bool = False,
    install_vscode_ensure_agent_surfaces_hook: bool = False,
    install_grok_ensure_agent_surfaces_hook: bool = False,
) -> dict[str, object]:
    """Re-apply managed hook adapters without re-resolving the overlay source."""
    from workbay_bootstrap.install import (
        PROFILE_ALL,
        _finalize_install_manifest,
        _load_portable_manifest,
        _walk_hook_adapters,
    )

    target = Path(target).resolve()
    manifest = _load_manifest(target)
    overlay_root = _resolve_installed_overlay_root(target, manifest)
    active_flags: set[str] = set()
    flag_values = {
        "--install-claude-stop-hook-local": install_claude_stop_hook_local,
        "--install-codex-stop-hook": install_codex_stop_hook,
        "--install-vscode-stop-hook": install_vscode_stop_hook,
        "--install-grok-stop-hook": install_grok_stop_hook,
        "--install-claude-reinject-hook-local": install_claude_reinject_hook_local,
        "--install-codex-ensure-agent-surfaces-hook": install_codex_ensure_agent_surfaces_hook,
        "--install-vscode-ensure-agent-surfaces-hook": install_vscode_ensure_agent_surfaces_hook,
        "--install-grok-ensure-agent-surfaces-hook": install_grok_ensure_agent_surfaces_hook,
    }
    for flag, enabled in flag_values.items():
        if enabled:
            active_flags.add(flag)
    if not active_flags:
        raise ValueError(
            "apply-hooks requires at least one --install-*-hook opt-in flag."
        )
    profile = str(manifest.get("profile") or PROFILE_ALL)
    hook_configs = _walk_hook_adapters(
        manifest=_load_portable_manifest(overlay_root),
        clone=overlay_root,
        target=target,
        profile=profile,
        active_flags=active_flags,
    )
    updated = dict(manifest)
    # Merge, do not replace: keep non-hook configs AND previously-managed hook
    # adapters for flags we are NOT re-applying now, so an incremental
    # `apply-hooks --install-X` does not silently de-manage earlier opt-ins
    # (a later `update` re-derives managed hooks from these manifest rows).
    preserved: list[object] = []
    for entry in manifest.get("configs") or []:
        if not isinstance(entry, dict) or entry.get("kind") != "hook_adapter":
            preserved.append(entry)
            continue
        if entry.get("opt_in_flag") in active_flags:
            # Re-applied this run; the fresh hook_configs row supersedes it.
            continue
        preserved.append(entry)
    updated["configs"] = [*preserved, *hook_configs]
    return _finalize_install_manifest(target, updated)


def update(
    *,
    target: Path,
    remote_ref: str | None = None,
    remote_url: str | None = None,
    package_root: Path | None = None,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None = None,
    plugin_overrides: Path | None = None,
    reset_overrides: bool = False,
    backup_overrides: bool = False,
    enforce_required_surfaces: bool = True,
    adopt_redundant: bool = True,
    allow_member_skew: bool = False,
) -> dict[str, object]:
    """Re-run ``install`` against ``target`` from its recorded source.

    The manifest's ``source_kind`` selects the refresh path (implementation note):

    * ``git_overlay`` — requires ``remote_ref`` (the new tag/branch/sha);
      ``remote_url`` defaults to whatever the manifest already records.
    * ``package`` — re-installs from the **currently installed**
      workbay-system distribution (upgrade the wheel first, e.g. via
      ``pip install --upgrade workbay``); ``remote_ref`` is invalid
      here. ``package_root`` is a test/pinned-install override.

    When ``mcp_servers`` is omitted, managed installs preserve their existing
    ``.mcp.json`` registration so config surfaces and init-state still
    refresh. ``enforce_required_surfaces`` defaults to ``True`` to match the
    install CLI contract.

    ``adopt_redundant`` (implementation note, default True) adopts every
    ``local_redundant`` surface after the refresh and reports them in the
    returned manifest's ``adopted_redundant`` list.
    """
    from workbay_bootstrap.install import _discover_plugin_override_root, install

    target = Path(target).resolve()
    manifest = _load_manifest(target)
    source_kind = str(manifest.get("source_kind") or "git_overlay")
    if source_kind == "package":
        if remote_ref is not None:
            raise ValueError(
                "--remote-ref is invalid for a package-source install "
                "(source_kind=package refreshes from the installed "
                "workbay-system distribution, not a git ref). Upgrade the "
                "wheel (pip install --upgrade workbay) and re-run "
                "`workbay-bootstrap update --target <dir>` without "
                "--remote-ref."
            )
    elif source_kind == "worktree":
        if remote_ref is not None:
            raise ValueError(
                "--remote-ref is invalid for a worktree-source install "
                "(source_kind=worktree refreshes from the local HEAD)."
            )
    elif remote_ref is None:
        raise ValueError(
            "update for a git_overlay install requires --remote-ref "
            "(the new tag/branch/sha to refresh the overlay to)."
        )
    if mcp_servers is None:
        mcp_servers = _preserved_mcp_servers(target, manifest)
    override_root = _discover_plugin_override_root(
        target,
        manifest=manifest,
        plugin_overrides=plugin_overrides,
    )
    hook_kwargs = _preserved_hook_opt_ins(manifest)

    if source_kind == "package":
        result = install(
            target=target,
            source="package",
            package_root=package_root,
            mcp_servers=mcp_servers,
            plugin_overrides=override_root,
            reset_overrides=reset_overrides,
            backup_overrides=backup_overrides,
            enforce_required_surfaces=enforce_required_surfaces,
            allow_member_skew=allow_member_skew,
            **hook_kwargs,
        )
    elif source_kind == "worktree":
        result = install(
            target=target,
            source="worktree",
            mcp_servers=mcp_servers,
            plugin_overrides=override_root,
            reset_overrides=reset_overrides,
            backup_overrides=backup_overrides,
            enforce_required_surfaces=enforce_required_surfaces,
            allow_member_skew=allow_member_skew,
            **hook_kwargs,
        )
    else:
        if remote_url is None:
            remote_url = str(manifest["remote_url"])
        result = install(
            target=target,
            remote_url=remote_url,
            remote_ref=remote_ref,
            mcp_servers=mcp_servers,
            plugin_overrides=override_root,
            reset_overrides=reset_overrides,
            backup_overrides=backup_overrides,
            enforce_required_surfaces=enforce_required_surfaces,
            allow_member_skew=allow_member_skew,
            **hook_kwargs,
        )

    adopted = _adopt_redundant_surfaces(target) if adopt_redundant else []
    if adopted:
        # repair() rewrote the on-disk manifest (source flips); return the
        # post-adoption shape so callers and the CLI see the final state.
        # install()'s backup paths are transient (never persisted to disk),
        # so carry them over or the CLI's "override backup:" line vanishes.
        post_adoption = dict(_load_manifest(target))
        for transient_key in ("override_backup_path", "state_backup_path"):
            if transient_key in result:
                post_adoption[transient_key] = result[transient_key]
        result = post_adoption
    result["adopted_redundant"] = adopted
    return result


# ---------------------------------------------------------------------------
# repair
# ---------------------------------------------------------------------------


def _backup_local_surface(target: Path, surface: str) -> Path:
    """Copy a local surface to ``.workbay/backup/<utc-ts>/<surface>``."""
    import shutil
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = target / RUNTIME_ROOT_DIRNAME / "backup" / ts / surface
    dest.parent.mkdir(parents=True, exist_ok=True)
    src = target / surface
    if src.is_dir():
        shutil.copytree(src, dest)
    else:
        shutil.copy2(src, dest)
    return dest


def _restore_local_surface(backup: Path, local: Path) -> None:
    """Best-effort rollback: put the backed-up local copy back at ``local``.

    Used when adoption fails between deleting the local surface and finishing
    re-materialization, so a failed adoption never leaves the surface absent.
    """
    import shutil

    if local.is_symlink() or local.is_file():
        local.unlink()
    elif local.is_dir():
        shutil.rmtree(local)
    local.parent.mkdir(parents=True, exist_ok=True)
    if backup.is_dir():
        shutil.copytree(backup, local)
    else:
        shutil.copy2(backup, local)


def _rematerialize_carved_parent(
    target: Path, surface: str, payload: Path, *, is_package_source: bool
) -> list[dict[str, str]]:
    """Re-materialize an adopted carved surface in install's carved form.

    A whole-directory symlink/copy would re-expose the carve-excluded
    children, so the parent becomes a real directory whose non-excluded
    children are symlinked (git overlay) or copied (package source), exactly
    like :func:`workbay_bootstrap.install._materialize_surfaces` /
    ``_materialize_surfaces_copy``. Lifecycle-hoist children are copied as
    real files (the lifecycle pass owns them at install time and does not
    re-run under repair). Returns the per-child manifest entries that replace
    the adopted parent's ``source=local`` receipt entry.
    """
    import shutil

    from workbay_bootstrap.install import (
        _carved_surface_children,
        _lifecycle_hoist_children,
    )

    local = target / surface
    local.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, str]] = []
    for child in _carved_surface_children(surface, payload):
        dest = local / child.name
        if is_package_source:
            if child.is_dir():
                shutil.copytree(child, dest)
            else:
                shutil.copy2(child, dest)
        else:
            rel = os.path.relpath(child, dest.parent)
            dest.symlink_to(rel, target_is_directory=child.is_dir())
        entries.append({"path": f"{surface}/{child.name}", "source": "shared"})
    for child_name in sorted(_lifecycle_hoist_children(surface)):
        src = payload / child_name
        if not src.exists():
            continue
        dest = local / child_name
        if src.is_dir():
            shutil.copytree(src, dest)
        else:
            shutil.copy2(src, dest)
        entries.append({"path": f"{surface}/{child_name}", "source": "lifecycle"})
    return entries



def _finish_repair(
    target: Path,
    *,
    no_embeddings: bool,
    repaired: list[Finding],
    skipped: list[Finding],
) -> dict[str, list[Finding]]:
    from workbay_bootstrap.embedding_provision import maybe_provision_embeddings
    import sys

    for line in maybe_provision_embeddings(target, no_embeddings=no_embeddings):
        print(line, file=sys.stderr)
    return {"repaired": repaired, "skipped": skipped}

def repair(
    *,
    target: Path,
    force_dirty: bool = False,
    no_embeddings: bool = False,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None = None,
    plugin_overrides: Path | None = None,
    adopt_stale_local: list[str] | None = None,
    materialize_managed: bool = False,
) -> dict[str, list[Finding]]:
    """Restore drifted overlay surfaces flagged by :func:`doctor`.

    For each surface flagged as ``surface_drift``:

    - If the path no longer exists or is a broken/foreign symlink, the
      canonical symlink into the clone is recreated.
    - If the path has been replaced by a real directory or file, the surface
      is **skipped** unless ``force_dirty=True`` (per regression guard rg-017
      "never silently force-remove dirty content"). With ``force_dirty=True``
      the dirty content is removed and the symlink reinstated.

    Config drift (``.mcp.json`` / ``.vscode/mcp.json``) is repaired by
    re-running the install-time writers when ``mcp_servers`` is supplied.

    ``adopt_stale_local`` (internal) opts specific surfaces into
    adoption: a surface doctor classified ``local_stale`` or
    ``local_redundant`` is backed up to ``.workbay/backup/<ts>/<surface>``,
    re-materialized as the managed surface (symlink for the git overlay
    source, copy for the package source), and its receipt entry flips back to
    ``source=shared``. Never automatic; ``local_override`` surfaces are
    excluded even when requested.

    ``materialize_managed`` opts package-mode consumers into replacing dangling
    managed symlinks with payload copies. Local-precedence surfaces are never
    eligible.

    Returns a report dict ``{"repaired": [...], "skipped": [...]}`` whose
    entries reuse the :func:`doctor` finding shape.
    """
    from workbay_bootstrap.install import (
        _run_generator,
        _set_git_hooks_path,
        _write_plugin_pins,
        _discover_plugin_override_root,
    )
    from workbay_bootstrap.mcp_sync import sync_mcp_configs
    import shutil

    target = Path(target).resolve()

    # Worktree short-circuit (M4): an unadopted linked worktree is repaired by
    # adopting the overlay from its primary, not by the surface-drift path below
    # (which would target the worktree's own absent clone). Runs before
    # _load_manifest, which would fail when the marker is gitignored.
    unadopted_primary = _unadopted_worktree_primary(target)
    if unadopted_primary is not None:
        from workbay_bootstrap.adopt import adopt_worktree

        # force_dirty / mcp_servers / plugin_overrides are intentionally NOT
        # threaded here: adoption never force-removes local content (it honors
        # foreign-precedence), so the surface-drift force-overwrite path has no
        # analog for a bare unadopted worktree.
        adopt_worktree(target=target, primary=unadopted_primary)
        return _finish_repair(
            target,
            no_embeddings=no_embeddings,
            repaired=[{"kind": "unadopted_worktree", "path": str(unadopted_primary)}],
            skipped=[],
        )

    # implementation note: a self-host worktree (ships the payload, no clone) is healed by
    # emitting the surfaces locally via bootstrap-surfaces, not by the
    # surface-drift / clone path below (which would target the absent clone).
    # Short-circuit UNCONDITIONALLY (mirrors doctor): a healed self-host worktree
    # is a no-op rather than a fall-through to the clone repair path.
    if _is_selfhost_worktree(target):
        missing = _selfhost_missing_surface_paths(target)
        if not missing:
            return _finish_repair(target, no_embeddings=no_embeddings, repaired=[], skipped=[])
        from workbay_bootstrap.bootstrap_surfaces import bootstrap_surfaces

        finding: Finding = {
            "kind": "selfhost_worktree_missing_surfaces",
            "path": ", ".join(missing),
        }
        receipt = bootstrap_surfaces(target=target)
        if receipt.get("ok"):
            return _finish_repair(
                target,
                no_embeddings=no_embeddings,
                repaired=[finding],
                skipped=[],
            )
        return _finish_repair(
            target,
            no_embeddings=no_embeddings,
            repaired=[],
            skipped=[finding],
        )

    manifest = _load_manifest(target)
    if mcp_servers is None:
        mcp_servers = _preserved_mcp_servers(target, manifest)
    if mcp_servers is None:
        mcp_servers = _mcp_servers_from_manifest(target, manifest)
    presync_repairs, presync_skips = _repair_failed_presync_install(
        target, manifest, mcp_servers
    )
    if presync_repairs:
        # The convergence re-run wrote a fresh manifest; everything below
        # (deferred-step retry, doctor, drift repair) must read it, not the
        # superseded abort snapshot.
        manifest = _load_manifest(target)
    receipt_repairs = list(presync_repairs)
    receipt_repairs += _repair_deferred_install_steps(target, manifest, mcp_servers)
    override_root = _discover_plugin_override_root(
        target,
        manifest=manifest,
        plugin_overrides=plugin_overrides,
    )
    findings = doctor(
        target=target,
        mcp_servers=mcp_servers,
        plugin_overrides=override_root,
    )
    repaired: list[Finding] = list(receipt_repairs)
    skipped: list[Finding] = list(presync_skips)

    if not findings:
        return _finish_repair(target, no_embeddings=no_embeddings, repaired=repaired, skipped=skipped)

    # implementation note: repair must resolve payload from the installed overlay root,
    # which is the worktree itself for source_kind=worktree (no managed clone)
    # and the package source root for source_kind=package.
    is_package_source = str(manifest.get("source_kind") or "git_overlay") == "package"
    is_worktree_source = str(manifest.get("source_kind") or "git_overlay") == "worktree"
    # For worktree the managed clone IS the worktree (mirrors doctor's clone
    # short-circuit), so the generator + managed-hook-adapter repair branches
    # below resolve from it instead of the nonexistent .workbay/remote.
    clone = target if is_worktree_source else target.joinpath(*CLONE_SUBDIR)
    overlay_source_root = _resolve_installed_overlay_root(target, manifest)

    config_drift_paths = {f["path"] for f in findings if f["kind"] == "config_drift"}
    if config_drift_paths and mcp_servers:
        drifted_surfaces = [
            _MANAGED_SURFACE_BY_CONFIG_PATH[p]
            for p in config_drift_paths
            if p in _MANAGED_SURFACE_BY_CONFIG_PATH
        ]
        if drifted_surfaces:
            sync_mcp_configs(
                target, mcp_servers, surfaces=drifted_surfaces, check_only=False
            )

    for finding in findings:
        kind = finding["kind"]
        path = finding["path"]

        if finding.get("severity") == "info":
            # Informational notes (local_override) are never repair-eligible:
            # consumer-authored content in a managed path is respected.
            continue

        if kind in {"local_stale", "local_redundant"}:
            if not adopt_stale_local or path not in adopt_stale_local:
                # Adoption is strictly opt-in (internal).
                skipped.append(finding)
                continue
            from workbay_bootstrap.install import _resolve_in_clone

            source_root = overlay_source_root
            payload = _resolve_in_clone(source_root, path)
            if not payload.exists():
                skipped.append(finding)
                continue
            local = target / path
            try:
                backup = _backup_local_surface(target, path)
            except OSError:
                skipped.append(finding)
                continue
            # Per-child entries for an adopted carved parent, else None
            # (plain surface: flip the existing entry in place).
            new_entries: list[dict[str, str]] | None = None
            try:
                if local.is_symlink() or local.is_file():
                    local.unlink()
                elif local.is_dir():
                    shutil.rmtree(local)
                local.parent.mkdir(parents=True, exist_ok=True)
                if path in SURFACE_CHILD_EXCLUSIONS:
                    # Carved surface: a whole-directory symlink/copy would
                    # re-expose the carve-excluded children; re-materialize
                    # in install's carved per-child form instead.
                    new_entries = _rematerialize_carved_parent(
                        target, path, payload, is_package_source=is_package_source
                    )
                elif is_package_source:
                    if payload.is_dir():
                        shutil.copytree(payload, local)
                    else:
                        shutil.copy2(payload, local)
                else:
                    rel = os.path.relpath(payload, local.parent)
                    local.symlink_to(rel, target_is_directory=payload.is_dir())
            except OSError:
                # Mid-re-materialization failure must not strand the surface
                # deleted: restore the backup we just took and report the
                # adoption as skipped instead of half-done.
                try:
                    _restore_local_surface(backup, local)
                except OSError:
                    pass  # backup itself remains under .workbay/backup/
                skipped.append(finding)
                continue
            surfaces_list = manifest.get("surfaces")
            if isinstance(surfaces_list, list):
                if new_entries is None:
                    for entry in surfaces_list:
                        if isinstance(entry, dict) and entry.get("path") == path:
                            entry["source"] = "shared"
                else:
                    # Carved parent adopted: replace its parent-level local
                    # entry with the per-child entries install would record.
                    surfaces_list[:] = [
                        e
                        for e in surfaces_list
                        if not (isinstance(e, dict) and e.get("path") == path)
                    ]
                    existing = {
                        e.get("path"): e for e in surfaces_list if isinstance(e, dict)
                    }
                    for new_entry in new_entries:
                        current = existing.get(new_entry["path"])
                        if current is not None:
                            current["source"] = new_entry["source"]
                        else:
                            surfaces_list.append(new_entry)
                (target / BOOTSTRAP_MANIFEST_NAME).write_text(
                    json.dumps(manifest, indent=2) + "\n"
                )
            repaired.append(finding)
            continue

        if kind == "managed_link_dangling":
            if not materialize_managed:
                skipped.append(finding)
                continue
            source = overlay_source_root / path
            if not source.exists():
                skipped.append(finding)
                continue
            local = target / path
            try:
                if local.is_symlink() or local.is_file():
                    local.unlink()
                elif local.is_dir():
                    shutil.rmtree(local)
                local.parent.mkdir(parents=True, exist_ok=True)
                if source.is_dir():
                    shutil.copytree(source, local)
                else:
                    shutil.copy2(source, local)
            except OSError:
                skipped.append(finding)
                continue
            repaired.append(finding)
            continue

        if kind == "surface_drift":
            from workbay_bootstrap.install import _resolve_in_clone

            if is_package_source:
                # Package source copies surfaces; the symlink-recreate below
                # does not apply. Preserve the pre-worktree skip behavior.
                skipped.append(finding)
                continue
            link = target / path
            remote_path = _resolve_in_clone(overlay_source_root, path)
            if not remote_path.exists():
                skipped.append(finding)
                continue

            if link.is_symlink() or not link.exists():
                # Broken/foreign symlink or missing entirely: safe to replace.
                if link.is_symlink():
                    link.unlink()
            elif link.is_dir() and any(link.iterdir()):
                if not force_dirty:
                    skipped.append(finding)
                    continue
                shutil.rmtree(link)
            elif link.is_dir():
                link.rmdir()
            else:
                if not force_dirty:
                    skipped.append(finding)
                    continue
                link.unlink()

            link.parent.mkdir(parents=True, exist_ok=True)
            rel = os.path.relpath(remote_path, link.parent)
            link.symlink_to(rel, target_is_directory=True)
            repaired.append(finding)
            continue

        if kind in {"generated_drift", "plugin_source_drift"}:
            # Re-run the generator once for the whole batch — it rewrites
            # every per-agent surface from the canonical source. Collapse
            # subsequent generated/source findings into the same repair op.
            if any(
                f["kind"] in {"generated_drift", "plugin_source_drift"}
                for f in repaired
            ):
                repaired.append(finding)
                continue
            try:
                remote_sha = manifest.get("remote_sha")
                if not isinstance(remote_sha, str):
                    raise ValueError("install manifest missing remote_sha")
                _run_generator(target, clone, remote_sha, override_root)
                _write_plugin_pins(target, override_root, clone=clone)
            except Exception:
                skipped.append(finding)
                continue
            repaired.append(finding)
            continue

        if kind in {"pin_target_drift", "codex_activation_drift"}:
            if any(
                f["kind"] in {"pin_target_drift", "codex_activation_drift"}
                for f in repaired
            ):
                repaired.append(finding)
                continue
            try:
                _write_plugin_pins(target, override_root, clone=clone)
            except Exception:
                skipped.append(finding)
                continue
            repaired.append(finding)
            continue

        if kind == "cursor_native_surface_drift":
            if any(f["kind"] == "cursor_native_surface_drift" for f in repaired):
                repaired.append(finding)
                continue
            from workbay_bootstrap.install import _materialize_cursor_plugin
            from workbay_bootstrap.activation import write_plugin_activation

            try:
                cursor_surfaces, _cursor_config = _materialize_cursor_plugin(target)
                if any(entry.get("source") == "local" for entry in cursor_surfaces):
                    skipped.append(finding)
                    continue
                write_plugin_activation("cursor", target, clone=clone)
            except Exception:
                skipped.append(finding)
                continue
            repaired.append(finding)
            continue

        if kind in {"grok_activation_drift", "grok_plugin_drift"}:
            if any(
                f["kind"] in {"grok_activation_drift", "grok_plugin_drift"}
                for f in repaired
            ):
                repaired.append(finding)
                continue
            from workbay_bootstrap.activation import (
                activate_grok_plugin,
                grok_dest_is_unmanaged_dir,
                materialize_grok_plugin_symlink,
            )
            from workbay_bootstrap.install import _materialize_grok_plugin
            from workbay_bootstrap.worktree import is_linked_worktree

            try:
                dest = target / GROK_PLUGIN_DEST
                if kind == "grok_plugin_drift" or not dest.exists():
                    if is_linked_worktree(target):
                        _, grok_config = materialize_grok_plugin_symlink(target)
                        if grok_config.get("action") == "skipped_foreign_content":
                            # Local precedence honored: nothing changed, so
                            # claiming "repaired" would be a false success and
                            # doctor would re-report the same drift.
                            skipped.append(finding)
                            continue
                    elif grok_dest_is_unmanaged_dir(target):
                        # Primary-repo parity with the worktree path above:
                        # operator-owned content (real dir without the
                        # generated manifest) must not be rmtree'd by
                        # _materialize_grok_plugin. Honest skip.
                        skipped.append(finding)
                        continue
                    else:
                        _materialize_grok_plugin(target)
                activate_grok_plugin(target)
            except Exception:
                skipped.append(finding)
                continue
            repaired.append(finding)
            continue

        if kind == "config_drift":
            if mcp_servers and path in _MANAGED_SURFACE_BY_CONFIG_PATH:
                repaired.append(finding)
            else:
                skipped.append(finding)
            continue

        if kind == "hook_adapter_drift":
            # Re-apply the manifest-declared managed entries for this path —
            # every managed family that patches it (e.g. compact-session Stop
            # AND reinject-context SessionStart can share .claude/settings.json).
            # The walker's merge is idempotent and preserves unrelated user
            # entries, so restoring drifted managed adapters is safe without
            # force_dirty. Never-opted-in adapters on the same path are NOT
            # applied (repair must not install an uninvited family).
            from workbay_bootstrap.install import _apply_merge_array_entry

            path_adapters = [
                adapter
                for _hook_id, tgt, adapter in _managed_hook_adapters(clone, manifest)
                if tgt == path
            ]
            if not path_adapters:
                skipped.append(finding)
                continue
            try:
                for adapter in path_adapters:
                    _apply_merge_array_entry(adapter, target=target)
            except Exception:
                skipped.append(finding)
                continue
            repaired.append(finding)
            continue

        # missing_clone / missing_manifest are out of scope here — caller
        # should run install/update instead. Surface as skipped.
        skipped.append(finding)

    # Refresh git hooks path defensively when we touched anything.
    if repaired and (target / ".git").exists():
        try:
            _set_git_hooks_path(target)
        except Exception:
            pass

    return _finish_repair(target, no_embeddings=no_embeddings, repaired=repaired, skipped=skipped)
