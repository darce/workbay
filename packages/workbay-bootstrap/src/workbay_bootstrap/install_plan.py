"""Split Phase planning and execution for ``install()`` (implementation note S2).

Phase 1 — :func:`build_install_plan` is pure given an injected
:class:`SourceResolver` (no subprocesses, no filesystem writes).

Phase 2 — :func:`execute_install_plan` performs all mutations and returns the
install manifest dict.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from workbay_bootstrap.install_receipt import InstallReceipt

SURFACE_MODE_SYMLINK = "symlink"
SURFACE_MODE_COPY = "copy"


@dataclass(frozen=True)
class SourceResolver:
    """Resolved overlay source root and provenance anchor."""

    root: Path
    kind: str  # ``git_overlay`` | ``package`` | ``worktree``
    base_anchor: str  # 40-char SHA (git or synthetic for package)
    surface_mode: str  # :data:`SURFACE_MODE_SYMLINK` | :data:`SURFACE_MODE_COPY`
    package_version: str | None = None
    remote_url: str | None = None
    remote_ref: str | None = None
    remote_sha: str | None = None


@dataclass(frozen=True)
class InstallRequest:
    """Parameter object for a single ``install()`` invocation."""

    target: Path
    source: str
    remote_url: str | None
    remote_ref: str | None
    package_root: Path | None
    mcp_servers: Mapping[str, Mapping[str, Any]] | None
    plugin_overrides: Path | None
    reset_overrides: bool
    backup_overrides: bool
    enforce_required_surfaces: bool
    profile: str
    install_claude_stop_hook_local: bool
    install_codex_stop_hook: bool
    install_vscode_stop_hook: bool
    install_grok_stop_hook: bool
    install_claude_reinject_hook_local: bool
    allow_member_skew: bool
    # Defaulted (opt-in, off) + placed last so direct InstallRequest(...)
    # constructions that predate these fields keep working.
    install_codex_ensure_agent_surfaces_hook: bool = False
    install_vscode_ensure_agent_surfaces_hook: bool = False
    install_grok_ensure_agent_surfaces_hook: bool = False

    @classmethod
    def from_install_kwargs(
        cls,
        *,
        target: Path,
        remote_url: str | None,
        remote_ref: str | None,
        source: str,
        package_root: Path | None,
        mcp_servers: Mapping[str, Mapping[str, Any]] | None,
        plugin_overrides: Path | None,
        reset_overrides: bool,
        backup_overrides: bool,
        enforce_required_surfaces: bool,
        profile: str,
        install_claude_stop_hook_local: bool,
        install_codex_stop_hook: bool,
        install_vscode_stop_hook: bool,
        install_grok_stop_hook: bool,
        install_claude_reinject_hook_local: bool,
        install_codex_ensure_agent_surfaces_hook: bool,
        install_vscode_ensure_agent_surfaces_hook: bool,
        install_grok_ensure_agent_surfaces_hook: bool,
        allow_member_skew: bool,
    ) -> InstallRequest:
        return cls(
            target=target,
            source=source,
            remote_url=remote_url,
            remote_ref=remote_ref,
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
            install_codex_ensure_agent_surfaces_hook=install_codex_ensure_agent_surfaces_hook,
            install_vscode_ensure_agent_surfaces_hook=install_vscode_ensure_agent_surfaces_hook,
            install_grok_ensure_agent_surfaces_hook=install_grok_ensure_agent_surfaces_hook,
            allow_member_skew=allow_member_skew,
        )


@dataclass
class InstallPlan:
    """Computed install plan consumed by :func:`execute_install_plan`."""

    request: InstallRequest
    source: SourceResolver
    mcp_servers: Mapping[str, Mapping[str, Any]] | None
    init_state_expected_remote_url: str | None = None
    state_backup_path: str | None = None
    active_flags: frozenset[str] = field(default_factory=frozenset)
    run_presync_prewarm: bool = False
    run_profile_all: bool = False
    run_lifecycle: bool = False


def resolve_git_overlay_source(
    target: Path,
    remote_url: str,
    remote_ref: str,
    *,
    receipt: InstallReceipt | None = None,
) -> SourceResolver:
    """Clone or fast-forward the overlay remote; return a :class:`SourceResolver`."""
    from workbay_bootstrap.git_write import _git_write
    from workbay_bootstrap.install import (
        CLONE_SUBDIR,
        RemoteUrlMismatchError,
        _git,
        _load_existing_manifest_remote_url,
        _managed_clone_can_switch_remote,
        _replace_managed_clone_for_remote_switch,
        _resolve_ref_to_sha,
    )

    clone = target.joinpath(*CLONE_SUBDIR)
    existing_manifest_remote_url = _load_existing_manifest_remote_url(target)

    if (clone / ".git").exists():
        existing_origin = _git("remote", "get-url", "origin", cwd=clone)
        if existing_origin != remote_url:
            if _managed_clone_can_switch_remote(
                existing_origin=existing_origin,
                existing_manifest_remote_url=existing_manifest_remote_url,
            ):
                _replace_managed_clone_for_remote_switch(
                    clone,
                    existing_origin=existing_origin,
                    remote_url=remote_url,
                )
                _git_write(
                    target,
                    "clone",
                    "--branch",
                    remote_ref,
                    remote_url,
                    str(clone),
                    receipt=receipt,
                )
            else:
                raise RemoteUrlMismatchError(
                    f"{clone} already tracks origin {existing_origin!r}, "
                    f"but install was called with remote_url={remote_url!r}. "
                    "Move or remove .workbay/remote (or pass the original URL) to "
                    "switch overlays."
                )
        else:
            _git_write(
                target,
                "fetch",
                "--tags",
                "--prune",
                "--force",
                "origin",
                cwd=clone,
                receipt=receipt,
            )
    else:
        clone.parent.mkdir(parents=True, exist_ok=True)
        if clone.exists():
            raise FileExistsError(
                f"{clone} exists but is not a git clone. "
                "Move or remove it before re-running install."
            )
        _git_write(
            target,
            "clone",
            "--branch",
            remote_ref,
            remote_url,
            str(clone),
            receipt=receipt,
        )

    sha = _resolve_ref_to_sha(clone, remote_ref)
    if len(sha) != 40:
        raise RuntimeError(f"unexpected sha shape from git rev-parse: {sha!r}")

    _git_write(
        target,
        "checkout",
        "--detach",
        sha,
        cwd=clone,
        receipt=receipt,
    )
    return SourceResolver(
        root=clone,
        kind="git_overlay",
        base_anchor=sha,
        surface_mode=SURFACE_MODE_SYMLINK,
        remote_url=remote_url,
        remote_ref=remote_ref,
        remote_sha=sha,
    )


def resolve_worktree_source(worktree_root: Path) -> SourceResolver:
    """Resolve the overlay from a local git worktree (no clone, no remote)."""
    from workbay_bootstrap.install import _git

    root = Path(worktree_root).resolve()
    if not (root / ".git").exists():
        raise FileNotFoundError(
            f"source='worktree' requires a git repository at {root}"
        )
    sha = _git("rev-parse", "HEAD", cwd=root)
    if len(sha) != 40:
        raise RuntimeError(f"unexpected sha shape from git rev-parse HEAD: {sha!r}")
    return SourceResolver(
        root=root,
        kind="worktree",
        base_anchor=sha,
        surface_mode=SURFACE_MODE_SYMLINK,
        remote_sha=sha,
    )


def resolve_package_source(package_root: Path | None) -> SourceResolver:
    """Resolve the package overlay source (no subprocesses)."""
    from workbay_bootstrap.install import _package_source_root, _package_version

    root = _package_source_root(package_root)
    package_version = _package_version(root)
    base_anchor = hashlib.sha1(
        f"workbay-system@{package_version}".encode("utf-8")
    ).hexdigest()
    return SourceResolver(
        root=root,
        kind="package",
        base_anchor=base_anchor,
        surface_mode=SURFACE_MODE_COPY,
        package_version=package_version,
    )


def build_install_plan(
    request: InstallRequest,
    source: SourceResolver,
    *,
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
    init_state_expected_remote_url: str | None = None,
) -> InstallPlan:
    """Pure planning: derive step flags from request + resolved source."""
    from workbay_bootstrap.install import PROFILE_ALL, PROFILE_LIFECYCLE

    active_flags: set[str] = set()
    if request.install_claude_stop_hook_local:
        active_flags.add("--install-claude-stop-hook-local")
    if request.install_codex_stop_hook:
        active_flags.add("--install-codex-stop-hook")
    if request.install_vscode_stop_hook:
        active_flags.add("--install-vscode-stop-hook")
    if request.install_grok_stop_hook:
        active_flags.add("--install-grok-stop-hook")
    if request.install_claude_reinject_hook_local:
        active_flags.add("--install-claude-reinject-hook-local")
    if request.install_codex_ensure_agent_surfaces_hook:
        active_flags.add("--install-codex-ensure-agent-surfaces-hook")
    if request.install_vscode_ensure_agent_surfaces_hook:
        active_flags.add("--install-vscode-ensure-agent-surfaces-hook")
    if request.install_grok_ensure_agent_surfaces_hook:
        active_flags.add("--install-grok-ensure-agent-surfaces-hook")

    run_profile_all = request.profile == PROFILE_ALL
    return InstallPlan(
        request=request,
        source=source,
        mcp_servers=mcp_servers,
        init_state_expected_remote_url=init_state_expected_remote_url,
        active_flags=frozenset(active_flags),
        run_presync_prewarm=(
            source.kind in ("git_overlay", "worktree")
            and run_profile_all
            and bool(mcp_servers)
        ),
        run_profile_all=run_profile_all,
        run_lifecycle=request.profile in (PROFILE_ALL, PROFILE_LIFECYCLE),
    )


def _manifest_for_resolved_source(
    *,
    source: SourceResolver,
    request: InstallRequest,
    target: Path,
    override_root: Path,
    surfaces: list[dict[str, str]],
    configs: list[dict[str, str]],
    mcp_servers: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, object]:
    from workbay_bootstrap.install import (
        _build_install_manifest,
        _plugin_override_root_manifest_path,
        _stack_provenance,
    )

    plugin_overrides_path = _plugin_override_root_manifest_path(target, override_root)
    if source.kind == "package":
        stack_distribution, stack_version, stack_members = _stack_provenance()
        return _build_install_manifest(
            source_kind="package",
            package_version=source.package_version,
            stack_distribution=stack_distribution,
            stack_version=stack_version,
            stack_members=stack_members,
            profile=request.profile,
            surfaces=surfaces,
            configs=configs,
            mcp_servers=mcp_servers,
            plugin_overrides_path=plugin_overrides_path,
        )
    if source.kind == "worktree":
        return _build_install_manifest(
            source_kind="worktree",
            remote_sha=source.remote_sha,
            profile=request.profile,
            surfaces=surfaces,
            configs=configs,
            mcp_servers=mcp_servers,
            plugin_overrides_path=plugin_overrides_path,
        )
    return _build_install_manifest(
        remote_url=source.remote_url,
        remote_ref=source.remote_ref,
        remote_sha=source.remote_sha,
        profile=request.profile,
        surfaces=surfaces,
        configs=configs,
        mcp_servers=mcp_servers,
        plugin_overrides_path=plugin_overrides_path,
    )


def execute_install_plan(
    plan: InstallPlan, *, receipt: InstallReceipt | None = None
) -> dict[str, object]:
    """Execute a computed plan and return the install manifest dict."""
    from workbay_bootstrap.install_receipt import InstallReceipt
    from workbay_bootstrap.install import (
        _annotate_surface_provenance,
        _append_config_entry,
        _apply_doc_surface_overrides,
        _discover_plugin_override_root,
        _ensure_consumer_gitignore_block,
        _ensure_consumer_makefile_include,
        converge_partial_overlay_state,
        _contract_orphaned_clone_homes,
        _enforce_hook_coherence_gate,
        _existing_surface_sources,
        _finalize_install_manifest,
        _install_lifecycle_profile,
        _load_portable_manifest,
        _materialize_cursor_plugin,
        _materialize_grok_plugin,
        _materialize_surfaces,
        _materialize_surfaces_copy,
        _prepare_generated_surfaces,
        _prepare_generator_ledger_surfaces,
        _prepare_plugin_generated_surfaces,
        _prepare_state_for_remote_switch,
        _presync_local_mcp_envs,
        _prewarm_uvx_mcp_envs,
        _install_gitonly_mcp_tools,
        _mcp_specs_use_launch_shim,
        _resolve_gitonly_member_specs,
        _prime_worktree_manifest_for_init_state,
        _reset_plugin_overrides,
        _run_generator,
        _run_init_state,
        _set_git_hooks_path,
        _walk_hook_adapters,
        _write_configs,
        _write_plugin_override_lock,
        _write_plugin_pins,
        BootstrapManifestValidationError,
    )
    from workbay_bootstrap.activation import write_plugin_activation

    if receipt is None:
        receipt = InstallReceipt()

    request = plan.request
    source = plan.source
    target = request.target
    clone = source.root
    converge_partial_overlay_state(target)

    override_root = _discover_plugin_override_root(
        target, plugin_overrides=request.plugin_overrides
    )
    override_root, override_backup_path = _reset_plugin_overrides(
        target,
        override_root,
        reset_overrides=request.reset_overrides,
        backup_overrides=request.backup_overrides,
    )

    surfaces: list[dict[str, str]] = []
    configs: list[dict[str, str]] = []
    state_backup_path = plan.state_backup_path
    init_state_expected_remote_url = plan.init_state_expected_remote_url

    if plan.run_profile_all:
        if plan.mcp_servers:
            import subprocess

            from workbay_bootstrap.external import (
                DeferredExternalCall,
                ExternalCallTimeout,
                offline_latch_active,
            )

            if plan.run_presync_prewarm:
                try:
                    receipt.presync_projects = [
                        str(path)
                        for path in _presync_local_mcp_envs(target, plan.mcp_servers)
                    ]
                    receipt.ok("presync_local_mcp")
                except (subprocess.CalledProcessError, ExternalCallTimeout, OSError) as exc:
                    reason = str(exc)
                    receipt.failed(
                        "presync_local_mcp",
                        reason=reason,
                        failure_class="system",
                        criticality="abort",
                    )
                    receipt.write_abort_snapshot(
                        target,
                        profile=plan.request.profile,
                        source_kind=source.kind,
                        remote_url=source.remote_url,
                        remote_ref=source.remote_ref,
                        remote_sha=source.remote_sha,
                        package_version=source.package_version,
                        mcp_servers=plan.mcp_servers,
                    )
                    from workbay_bootstrap.install_receipt import InstallExecutionError

                    raise InstallExecutionError(
                        f"presync_local_mcp failed: {reason}",
                        failure_class="system",
                    ) from exc
                try:
                    receipt.prewarm_refs = _prewarm_uvx_mcp_envs(target, plan.mcp_servers)
                    if offline_latch_active() and not receipt.prewarm_refs:
                        receipt.deferred("prewarm_uvx_mcp", reason="offline")
                    else:
                        receipt.ok("prewarm_uvx_mcp")
                except DeferredExternalCall as exc:
                    receipt.deferred("prewarm_uvx_mcp", reason=exc.reason)
                except (subprocess.CalledProcessError, ExternalCallTimeout, OSError) as exc:
                    # Best-effort contract: a FIRST-call timeout/OSError trips the
                    # offline latch inside run_external but still raises (only
                    # subsequent calls see the latch as DeferredExternalCall).
                    # Defer like a latched skip — presync above is the
                    # abort-worthy step; prewarm must never abort the install.
                    receipt.deferred("prewarm_uvx_mcp", reason=str(exc) or "offline")
            if _mcp_specs_use_launch_shim(plan.mcp_servers):
                try:
                    member_specs = _resolve_gitonly_member_specs(
                        target,
                        source_kind=source.kind,
                        remote_url=source.remote_url or request.remote_url,
                        remote_ref=source.remote_ref or request.remote_ref,
                    )
                    if member_specs:
                        receipt.gitonly_mcp_tools = _install_gitonly_mcp_tools(
                            target, member_specs=member_specs
                        )
                        receipt.ok("gitonly_mcp_tools")
                    else:
                        receipt.deferred(
                            "gitonly_mcp_tools", reason="no_resolvable_member_specs"
                        )
                except DeferredExternalCall as exc:
                    # Offline latch already tripped (e.g. by prewarm): the
                    # git-only tool install is best-effort like prewarm and must
                    # defer rather than abort when the host is offline. Genuine
                    # online failures still surface as CalledProcessError/timeout
                    # below and keep abort criticality.
                    receipt.deferred("gitonly_mcp_tools", reason=exc.reason)
                except (subprocess.CalledProcessError, ExternalCallTimeout, OSError) as exc:
                    reason = str(exc)
                    receipt.failed(
                        "gitonly_mcp_tools",
                        reason=reason,
                        failure_class="system",
                        criticality="abort",
                    )
                    receipt.write_abort_snapshot(
                        target,
                        profile=plan.request.profile,
                        source_kind=source.kind,
                        remote_url=source.remote_url,
                        remote_ref=source.remote_ref,
                        remote_sha=source.remote_sha,
                        package_version=source.package_version,
                        mcp_servers=plan.mcp_servers,
                    )
                    from workbay_bootstrap.install_receipt import InstallExecutionError

                    raise InstallExecutionError(
                        f"gitonly_mcp_tools failed: {reason}",
                        failure_class="system",
                    ) from exc
            receipt.offline_latch = offline_latch_active()
            # Remote-switch state prep is a git_overlay concern (it reconciles
            # handoff state across a changed remote_url). A worktree install has
            # no remote, so it must NOT reassign init_state_expected_remote_url
            # (which stays None per the plan, mirroring package); only presync
            # + prewarm above are shared. implementation note.
            if source.kind == "git_overlay":
                init_state_expected_remote_url, state_backup_path = (
                    _prepare_state_for_remote_switch(target, source.remote_url or "")
                )

        use_copy = source.surface_mode == SURFACE_MODE_COPY
        previous_sources = _existing_surface_sources(target) if use_copy else None
        if use_copy:
            surfaces.extend(
                _materialize_surfaces_copy(
                    target, clone, previous_sources=previous_sources or {}
                )
            )
        else:
            surfaces.extend(_materialize_surfaces(target, clone))

        surfaces.extend(_prepare_generated_surfaces(target, clone))
        surfaces.extend(_prepare_generator_ledger_surfaces(clone))
        plugin_surfaces = _prepare_plugin_generated_surfaces(
            target, clone, override_root
        )
        surfaces.extend(plugin_surfaces)

        if override_root is not None:
            _apply_doc_surface_overrides(target, override_root, clone)

        materialized_paths = {
            entry["path"] for entry in surfaces if isinstance(entry, dict)
        }
        if (
            request.enforce_required_surfaces
            and "scripts/hooks" not in materialized_paths
        ):
            if source.kind == "package":
                raise BootstrapManifestValidationError(
                    "refusing to declare install successful: required surface "
                    "'scripts/hooks' was not materialized from the "
                    "workbay-system package."
                )
            raise BootstrapManifestValidationError(
                "refusing to declare install successful: required surface "
                "'scripts/hooks' was not materialized. Bootstrap-installed hooks "
                "are part of the harness contract; without them, target-side "
                "guardrails do not run. Set enforce_required_surfaces=False to "
                "bypass for non-standard remotes."
            )

        _run_generator(target, clone, source.base_anchor, override_root)
        if plugin_surfaces:
            _write_plugin_override_lock(override_root, source.base_anchor)
            configs.extend(
                _write_plugin_pins(
                    target,
                    override_root,
                    include_codex_activation=False,
                    clone=clone,
                )
            )
            cursor_surfaces, cursor_config = _materialize_cursor_plugin(target)
            surfaces.extend(cursor_surfaces)
            _append_config_entry(configs, cursor_config)
            grok_surface, grok_config = _materialize_grok_plugin(target)
            surfaces.append(grok_surface)
            _append_config_entry(configs, grok_config)
        configs.extend(
            _write_configs(
                target, plan.mcp_servers, include_hooks=False, receipt=receipt
            )
        )
        if plugin_surfaces:
            _append_config_entry(
                configs, write_plugin_activation("codex", target, clone=clone)
            )
            _append_config_entry(
                configs, write_plugin_activation("grok", target, clone=clone)
            )
            _append_config_entry(
                configs, write_plugin_activation("cursor", target, clone=clone)
            )
            _append_config_entry(
                configs,
                write_plugin_activation("claude-code", target, clone=clone),
            )
        _prime_worktree_manifest_for_init_state(target, source)
        _run_init_state(
            target,
            plan.mcp_servers,
            expected_remote_url=init_state_expected_remote_url,
        )

    if plan.run_lifecycle:
        surfaces.extend(_install_lifecycle_profile(target, clone))
        include_entry = _ensure_consumer_makefile_include(target)
        if include_entry is not None:
            configs.append(include_entry)

    hooks_entry = _set_git_hooks_path(target, receipt=receipt)
    if hooks_entry is not None:
        configs.append(hooks_entry)

    if source.kind == "package":
        _contract_orphaned_clone_homes(target)

    if (target / ".git").exists():
        configs.append(_ensure_consumer_gitignore_block(target))

    configs.extend(
        _walk_hook_adapters(
            manifest=_load_portable_manifest(clone),
            clone=clone,
            target=target,
            profile=request.profile,
            active_flags=set(plan.active_flags),
        )
    )

    manifest = _manifest_for_resolved_source(
        source=source,
        request=request,
        target=target,
        override_root=override_root,
        surfaces=surfaces,
        configs=configs,
        mcp_servers=plan.mcp_servers,
    )

    _annotate_surface_provenance(target, manifest, package_root=clone)
    _enforce_hook_coherence_gate(target, manifest, package_root=clone)
    receipt.ok("finalize_manifest")
    receipt.attach_to_manifest(manifest)

    return _finalize_install_manifest(
        target,
        manifest,
        override_backup_path=override_backup_path,
        state_backup_path=state_backup_path,
        allow_member_skew=request.allow_member_skew,
    )


def run_install(request: InstallRequest) -> dict[str, object]:
    """Thin orchestrator: validate → preflight → plan → execute."""
    from workbay_bootstrap.install import (
        DEFAULT_MCP_SERVERS,
        SUPPORTED_PROFILES,
        LEGACY_AGENTIC_OVERLAY_REMEDIATION,
        _detect_legacy_agentic_overlay,
        _migrate_legacy_manifest,
        _resolve_install_mcp_servers,
        _resolve_worktree_install_mcp_servers,
    )
    from workbay_bootstrap.external import reset_offline_latch
    from workbay_bootstrap.git_write import GitWriteEscapeError
    from workbay_bootstrap.install_receipt import (
        InstallExecutionError,
        InstallPreflightError,
        InstallReceipt,
        run_install_preflight,
    )

    if request.profile not in SUPPORTED_PROFILES:
        raise ValueError(
            f"profile={request.profile!r} is not a recognized install profile; "
            f"expected one of {sorted(SUPPORTED_PROFILES)!r}."
        )

    target = Path(request.target).resolve()
    if not target.is_dir():
        raise FileNotFoundError(f"target directory does not exist: {target}")

    request = replace(request, target=target)

    _migrate_legacy_manifest(target)
    from workbay_bootstrap.install import converge_partial_overlay_state

    converge_partial_overlay_state(target)
    legacy_reason = _detect_legacy_agentic_overlay(target)
    if legacy_reason:
        raise InstallPreflightError(
            LEGACY_AGENTIC_OVERLAY_REMEDIATION.format(
                target=target, reason=legacy_reason
            ),
            failure_class="application",
        )
    reset_offline_latch()

    mcp_servers = request.mcp_servers
    if isinstance(mcp_servers, str):
        if mcp_servers != "default":
            raise ValueError(
                f"mcp_servers={mcp_servers!r} is not a recognized sentinel; "
                "pass a mapping, the literal 'default', or None."
            )
        mcp_servers = DEFAULT_MCP_SERVERS

    if request.source == "package":
        source = resolve_package_source(request.package_root)
        run_install_preflight(
            target=target,
            source_root=source.root,
            profile=request.profile,
            source_kind="package",
        )
        plan = build_install_plan(
            request,
            source,
            mcp_servers=mcp_servers,
            init_state_expected_remote_url=None,
        )
        return execute_install_plan(plan, receipt=InstallReceipt())

    if request.source == "worktree":
        source = resolve_worktree_source(target)
        run_install_preflight(
            target=target,
            source_root=source.root,
            profile=request.profile,
            source_kind="worktree",
        )
        resolved_mcp = _resolve_worktree_install_mcp_servers(target, mcp_servers)
        plan = build_install_plan(
            request,
            source,
            mcp_servers=resolved_mcp,
            init_state_expected_remote_url=None,
        )
        return execute_install_plan(plan, receipt=InstallReceipt())

    if request.source != "git_overlay":
        raise ValueError(
            f"source={request.source!r} is not recognized; "
            "expected 'git_overlay', 'package', or 'worktree'."
        )
    from workbay_bootstrap.install import _resolve_git_overlay_remote_url

    resolved_remote_url = _resolve_git_overlay_remote_url(target, request.remote_url)
    if not resolved_remote_url or not request.remote_ref:
        raise ValueError(
            "source='git_overlay' requires remote_ref (remote_url defaults to "
            "the existing clone origin, the adjacent manifest, then the "
            "built-in default)."
        )

    receipt = InstallReceipt()
    try:
        source = resolve_git_overlay_source(
            target,
            resolved_remote_url,
            request.remote_ref,
            receipt=receipt,
        )
    except GitWriteEscapeError as exc:
        receipt.write_abort_snapshot(
            target,
            profile=request.profile,
            source_kind="git_overlay",
            remote_url=resolved_remote_url,
            remote_ref=request.remote_ref,
        )
        raise InstallExecutionError(
            f"git write containment blocked install: {exc}",
            failure_class="application",
        ) from exc
    run_install_preflight(
        target=target,
        source_root=source.root,
        profile=request.profile,
        source_kind="git_overlay",
    )
    resolved_mcp = _resolve_install_mcp_servers(target, request.remote_ref, mcp_servers)
    plan = build_install_plan(
        request,
        source,
        mcp_servers=resolved_mcp,
        init_state_expected_remote_url=resolved_remote_url,
    )
    return execute_install_plan(plan, receipt=receipt)
