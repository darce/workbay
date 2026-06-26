"""Console entrypoint for ``workbay-bootstrap``."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

from workbay_protocol import REPO_URL

from workbay_bootstrap.install import (
    DEFAULT_MCP_SERVERS,
    PROFILE_ALL,
    PROFILE_LIFECYCLE,
    PROFILE_MINIMAL,
    _build_local_default_mcp_servers,
    install,
)
from workbay_bootstrap.mcp_sync import (
    DEFAULT_SURFACES,
    SUPPORTED_SURFACES,
    SyncReport,
    sync_mcp_configs,
)
from workbay_bootstrap.subcommands import apply_hooks, doctor, repair, status, update

# implementation note: CLI default flips to ``minimal``. The library
# ``install()`` API keeps ``profile="all"`` for back-compat with
# pre-Plan-0009 callers.
INSTALL_PROFILE_CHOICES: tuple[str, ...] = (
    PROFILE_MINIMAL,
    PROFILE_LIFECYCLE,
    PROFILE_ALL,
)
# internal: flipped from PROFILE_MINIMAL back to PROFILE_ALL so
# a no-argument ``workbay-bootstrap install`` materializes the full
# surface set out of the box. ``--profile minimal`` and
# ``--profile lifecycle`` remain opt-in for lean installs.
INSTALL_PROFILE_DEFAULT: str = PROFILE_ALL


def _resolve_mcp_servers(
    raw: str | None,
    *,
    no_servers: bool = False,
    default_when_unset: bool = False,
) -> Mapping[str, Mapping[str, Any]] | None:
    """Resolve the ``--mcp-servers`` argument into a server map (or None).

    - ``no_servers=True``: explicit opt-out → ``None`` (no config files written).
    - ``raw is None``: behavior depends on ``default_when_unset``. ``install``
      passes ``True`` so an unset flag falls back to :data:`DEFAULT_MCP_SERVERS`
      (implementation note step 2a — single-command, no-hand-edits install). ``doctor`` /
      ``update`` / ``repair`` pass ``False`` so an unset flag means
      "don't check / refresh configs at all".
    - ``raw == "default"``: use :data:`DEFAULT_MCP_SERVERS`.
    - ``raw`` is a path: load JSON. Accepts ``{"mcpServers": {...}}`` or a
      flat mapping.
    """
    if no_servers:
        return None
    if raw is None:
        return DEFAULT_MCP_SERVERS if default_when_unset else None
    if raw == "default":
        return DEFAULT_MCP_SERVERS
    doc = json.loads(Path(raw).read_text())
    if isinstance(doc, dict) and "mcpServers" in doc:
        return doc["mcpServers"]
    return doc


def _resolve_managed_servers(
    target: Path, raw: str | None
) -> Mapping[str, Mapping[str, Any]] | None:
    """Resolve ``--mcp-servers``, making ``default`` profile-aware.

    On a ``git_overlay`` install with local MCP packages, ``default`` resolves
    the LOCAL ``uv run --no-sync --project ...`` launchers (via
    :func:`_build_local_default_mcp_servers`) instead of the uvx package map, so
    ``mcp-sync`` and ``repair`` converge on the same launchers as
    ``install``/``update`` and never silently downgrade a local install. Falls
    back to the uvx map when no local packages are present. implementation note A1.

    implementation note: a ``worktree`` install ships the packages in-tree (no clone), so
    the probe base is the worktree itself; all other installs probe the managed
    clone (default base). This keeps ``mcp-sync``/``repair`` from re-downgrading
    a worktree dogfood's local launchers back to the published uvx map.
    """
    from workbay_bootstrap.install import _manifest_source_kind

    servers = _resolve_mcp_servers(raw)
    if raw == "default":
        base = target if _manifest_source_kind(target) == "worktree" else None
        local = _build_local_default_mcp_servers(target, base=base)
        if local is not None:
            return local
    return servers


DEFAULT_REMOTE_URL = REPO_URL
DEFAULT_REMOTE_REF = "main"
INSTALL_SOURCE_CHOICES: tuple[str, ...] = ("git_overlay", "package", "worktree")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay-bootstrap",
        description=(
            "Hoist the shared workbay-system surface into a consumer repo. "
            "Future subcommands: doctor, repair, update."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_install = sub.add_parser(
        "install",
        help="Materialize the shared workbay-system overlay and write the manifest.",
        # Disable prefix abbreviation so removed flags (e.g.
        # ``--install-claude-stop-hook``) error as unknown args instead of
        # silently abbreviating to a surviving ``-local`` variant (implementation note).
        allow_abbrev=False,
    )
    p_install.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root. Must already exist.",
    )
    p_install.add_argument(
        "--remote-url",
        default=None,
        help=(
            "Git URL for the shared workbay-system remote. When omitted, "
            "install derives the URL from the existing managed clone's origin, "
            f"then the adjacent manifest, then {DEFAULT_REMOTE_URL}."
        ),
    )
    p_install.add_argument(
        "--remote-ref",
        default=DEFAULT_REMOTE_REF,
        help=f"Tag or branch to check out (default: {DEFAULT_REMOTE_REF}).",
    )
    p_install.add_argument(
        "--source",
        choices=INSTALL_SOURCE_CHOICES,
        default="git_overlay",
        help=(
            "Overlay source. 'git_overlay' keeps the existing clone-backed "
            "default; 'package' resolves payload data from the installed "
            "workbay-system distribution; 'worktree' materializes from the "
            "target repo's own working tree / HEAD (no clone, no release)."
        ),
    )
    p_install.add_argument(
        "--mcp-servers",
        default=None,
        help=(
            "Either the literal 'default' (or omit the flag) to register the "
            "monorepo's two managed MCP servers (mcp-workbay-handoff, "
            "mcp-workbay-orchestrator) via uvx, or a path to a JSON file "
            'carrying a custom mapping. Accepts {"mcpServers": {...}} or a '
            "flat mapping. Writes .mcp.json / .vscode/mcp.json / "
            ".codex/config.toml. Use --no-mcp-servers to opt out entirely."
        ),
    )
    p_install.add_argument(
        "--no-mcp-servers",
        action="store_true",
        help=(
            "Opt out of writing .mcp.json / .vscode/mcp.json / "
            ".codex/config.toml. Use when bootstrapping a target that manages "
            "MCP servers separately."
        ),
    )
    p_install.add_argument(
        "--no-enforce-required-surfaces",
        action="store_true",
        help=(
            "Skip the required-surfaces refusal (currently scripts/hooks). "
            "Use only when bootstrapping from a non-standard remote that "
            "intentionally does not ship the harness hooks; the default is "
            "to refuse install in that case so target-side guardrails cannot "
            "silently no-op."
        ),
    )
    p_install.add_argument(
        "--plugin-overrides",
        type=Path,
        default=None,
        help=(
            "Optional explicit plugin override root. Defaults to auto-discovery "
            "at workbay-overrides/workbay-system/ when omitted."
        ),
    )
    p_install.add_argument(
        "--reset-overrides",
        action="store_true",
        help=(
            "Remove the resolved plugin override root before regenerating the "
            "plugin trees. Refuses on a dirty git worktree unless --backup is set."
        ),
    )
    p_install.add_argument(
        "--backup",
        action="store_true",
        help=(
            "Archive plugin overrides under .workbay/override-backups/<timestamp>/ "
            "before a reset-overrides removal."
        ),
    )
    p_install.add_argument(
        "--profile",
        choices=INSTALL_PROFILE_CHOICES,
        default=INSTALL_PROFILE_DEFAULT,
        help=(
            "Install profile. 'all' (default, internal) materializes "
            "per-agent surfaces, runs the workflow generator, writes MCP "
            "config surfaces, and performs the lifecycle hoist. 'minimal' "
            "clones the remote, writes the manifest, and sets core.hooksPath "
            "only — no per-agent surfaces, no generator, no consumer-tool "
            "config writers. 'lifecycle' adds the hoisted Make fragment + "
            "runner package and injects '-include Makefile.d/*.mk' into the "
            "consumer Makefile."
        ),
    )
    p_install.add_argument(
        "--install-claude-stop-hook-local",
        action="store_true",
        help=(
            "Opt in to writing the bootstrap-managed Stop hook into the "
            "user-owned <target>/.claude/settings.local.json (gitignored). "
            "Reversible by deleting the file. Off by default."
        ),
    )
    p_install.add_argument(
        "--install-codex-stop-hook",
        action="store_true",
        help=(
            "Opt in to writing the bootstrap-managed Stop hook into "
            "<target>/.codex/hooks/stop.json (Codex CLI harness). The "
            "adapter rendered is the codex adapter declared under hook "
            "'compact-session' in config/agent-workflows/portable_commands.json. "
            "Off by default."
        ),
    )
    p_install.add_argument(
        "--install-vscode-stop-hook",
        action="store_true",
        help=(
            "Opt in to writing the bootstrap-managed Stop hook into "
            "<target>/.vscode/workbay-stop-hooks.json (VS Code harness). "
            "The adapter rendered is the vscode adapter declared under "
            "hook 'compact-session' in config/agent-workflows/portable_commands.json. "
            "Off by default."
        ),
    )
    p_install.add_argument(
        "--install-grok-stop-hook",
        action="store_true",
        help=(
            "Opt in to writing the bootstrap-managed Stop hook into "
            "<target>/.grok/hooks/stop.json (Grok harness). The adapter "
            "rendered is the grok adapter declared under hook "
            "'compact-session' in config/agent-workflows/portable_commands.json. "
            "Off by default."
        ),
    )
    p_install.add_argument(
        "--install-claude-reinject-hook-local",
        action="store_true",
        help=(
            "Opt in to writing the bootstrap-managed SessionStart re-injection "
            "hook into the user-owned <target>/.claude/settings.local.json "
            "(gitignored). Reversible by deleting the file. Off by default."
        ),
    )
    p_status = sub.add_parser(
        "status",
        help="Print a summary of the installed overlay manifest.",
    )
    p_status.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )

    p_doctor = sub.add_parser(
        "doctor",
        help="Check the installed overlay for drift. Exit 1 when findings exist.",
    )
    p_doctor.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    p_doctor.add_argument(
        "--mcp-servers",
        default=None,
        help=(
            "Either 'default' for the monorepo's managed-server map, or a "
            "path to a JSON file. When set, config drift is checked."
        ),
    )
    p_doctor.add_argument(
        "--plugin-overrides",
        type=Path,
        default=None,
        help=(
            "Optional explicit plugin override root. Defaults to the path "
            "recorded in the bootstrap manifest or auto-discovery at "
            "workbay-overrides/workbay-system/."
        ),
    )
    p_doctor.add_argument(
        "--check-pypi",
        action="store_true",
        help=(
            "Also query PyPI for a newer workbay-stack release "
            "(informational note only; never affects the exit code). "
            "Doctor stays offline without this flag."
        ),
    )

    p_update = sub.add_parser(
        "update",
        help=(
            "Refresh an existing overlay from its recorded source: a new "
            "remote ref (git_overlay) or the installed workbay-system "
            "distribution (package)."
        ),
    )
    p_update.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    p_update.add_argument(
        "--remote-ref",
        default=None,
        help=(
            "New git ref (tag/branch/sha) to update the overlay to. Required "
            "for git_overlay manifests; invalid for package-source manifests "
            "(upgrade the wheel instead, then run update with no ref)."
        ),
    )
    p_update.add_argument(
        "--remote-url",
        default=None,
        help="Override remote URL. Defaults to the value in the existing manifest.",
    )
    p_update.add_argument(
        "--mcp-servers",
        default=None,
        help=(
            "Either 'default' for the monorepo's managed-server map, or a "
            "path to a JSON file. When set, configs are refreshed."
        ),
    )
    p_update.add_argument(
        "--plugin-overrides",
        type=Path,
        default=None,
        help=(
            "Optional explicit plugin override root. Defaults to the path "
            "recorded in the bootstrap manifest or auto-discovery at "
            "workbay-overrides/workbay-system/."
        ),
    )
    p_update.add_argument(
        "--reset-overrides",
        action="store_true",
        help=(
            "Remove the resolved plugin override root before regenerating the "
            "updated plugin trees. Refuses on a dirty git worktree unless "
            "--backup is set."
        ),
    )
    p_update.add_argument(
        "--backup",
        action="store_true",
        help=(
            "Archive plugin overrides under .workbay/override-backups/<timestamp>/ "
            "before a reset-overrides removal."
        ),
    )
    p_update.add_argument(
        "--no-enforce-required-surfaces",
        action="store_true",
        help=(
            "Skip the required-surfaces refusal during update. Use only for "
            "non-standard remotes or narrow tests that intentionally omit "
            "scripts/hooks."
        ),
    )
    p_update.add_argument(
        "--no-adopt-redundant",
        action="store_true",
        help=(
            "Keep local_redundant surfaces under local precedence instead of "
            "the default post-update adoption (backup + re-materialize as "
            "managed). Identical-content surfaces only; local_stale and "
            "local_override are never auto-adopted."
        ),
    )

    p_apply_hooks = sub.add_parser(
        "apply-hooks",
        help=(
            "Re-apply managed hook adapters from the installed overlay "
            "manifest without re-resolving source or running init-state."
        ),
        # See ``install`` above: ``allow_abbrev=False`` keeps removed shared
        # Claude hook flags from abbreviating onto surviving ``-local`` flags.
        allow_abbrev=False,
    )
    p_apply_hooks.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    for flag, help_text in (
        (
            "--install-claude-stop-hook-local",
            "Apply the bootstrap-managed Claude Stop hook adapter locally.",
        ),
        (
            "--install-codex-stop-hook",
            "Apply the bootstrap-managed Codex Stop hook adapter.",
        ),
        (
            "--install-vscode-stop-hook",
            "Apply the bootstrap-managed VS Code Stop hook adapter.",
        ),
        (
            "--install-grok-stop-hook",
            "Apply the bootstrap-managed Grok Stop hook adapter.",
        ),
        (
            "--install-claude-reinject-hook-local",
            "Apply the bootstrap-managed Claude SessionStart reinject adapter locally.",
        ),
    ):
        p_apply_hooks.add_argument(flag, action="store_true", help=help_text)

    p_mcp_sync = sub.add_parser(
        "mcp-sync",
        help=(
            "Reconcile .mcp.json / .vscode/mcp.json / .codex/config.toml "
            "and the ledger's mcp_servers block from a managed-server map. "
            "Does NOT fetch the remote, regenerate skills, or run init-state."
        ),
    )
    p_mcp_sync.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    p_mcp_sync.add_argument(
        "--mcp-servers",
        required=True,
        help=(
            "Either 'default' for the monorepo's managed-server map, or a "
            'path to a JSON file. Accepts {"mcpServers": {...}} or a flat '
            "mapping."
        ),
    )
    mode = p_mcp_sync.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help=(
            "Report drift without writing. Exit 0 if clean, 1 if any surface drifts."
        ),
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Rewrite drifted surfaces and the ledger mcp_servers block. "
            "Default action when neither --check nor --apply is given."
        ),
    )
    p_mcp_sync.add_argument(
        "--prune-removed-managed",
        action="store_true",
        help=(
            "Drop launchers from the surfaces whose names appear in the "
            "ledger's previously-managed list but NOT in the resolved map. "
            "Third-party launchers (absent from the ledger) are never "
            "pruned. On legacy targets without the ledger block this is a "
            "no-op for the first run; the block is seeded on this pass so "
            "the next run has provenance."
        ),
    )
    p_mcp_sync.add_argument(
        "--surfaces",
        nargs="+",
        choices=sorted(SUPPORTED_SURFACES),
        default=list(DEFAULT_SURFACES),
        metavar="SURFACE",
        help=("Subset of surfaces to reconcile. Default: claude vscode codex cursor."),
    )
    p_mcp_sync.add_argument(
        "--json",
        dest="emit_json",
        action="store_true",
        help=(
            "Emit the SyncReport as JSON on stdout. Schema: "
            "{surfaces: [{name, path, drift, action}], "
            "preserved_third_party: [...], pruned_managed: [...], "
            "ledger_mcp_servers: [...], exit_code: int}."
        ),
    )

    p_repair = sub.add_parser(
        "repair",
        help="Restore drifted overlay surfaces flagged by `doctor`.",
    )
    p_repair.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    p_repair.add_argument(
        "--force-dirty",
        action="store_true",
        help=(
            "Replace surfaces that contain real local content. "
            "Without this flag, dirty surfaces are skipped (rg-017)."
        ),
    )
    p_repair.add_argument(
        "--mcp-servers",
        default=None,
        help=(
            "Either 'default' for the monorepo's managed-server map, or a "
            "path to a JSON file. When set, config drift is also repaired."
        ),
    )
    p_repair.add_argument(
        "--plugin-overrides",
        type=Path,
        default=None,
        help=(
            "Optional explicit plugin override root. Defaults to the path "
            "recorded in the bootstrap manifest or auto-discovery at "
            "workbay-overrides/workbay-system/."
        ),
    )
    p_repair.add_argument(
        "--adopt-stale-local",
        action="append",
        default=None,
        metavar="SURFACE",
        help=(
            "Adopt a surface doctor classified local_stale/local_redundant: "
            "back the local copy up under .workbay/backup/<ts>/ and "
            "re-materialize the managed surface. Repeatable; local_override "
            "surfaces are never adopted."
        ),
    )

    p_adopt = sub.add_parser(
        "adopt-worktree",
        help=(
            "Adopt the bootstrap overlay into a linked git worktree by "
            "redirecting its surfaces at the primary's clone."
        ),
    )
    p_adopt.add_argument(
        "--target",
        type=Path,
        default=None,
        help="Linked worktree to adopt. Defaults to the current directory.",
    )
    p_adopt.add_argument(
        "--primary",
        type=Path,
        default=None,
        help=(
            "Primary overlay root to adopt from. Defaults to resolving it by "
            "the .workbay-bootstrap.json marker."
        ),
    )
    p_adopt.add_argument(
        "--check",
        action="store_true",
        help="Report drift without writing. Exit 1 when the worktree has drift.",
    )
    p_adopt.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit the adoption receipt as JSON.",
    )

    p_bootstrap_surfaces = sub.add_parser(
        "bootstrap-surfaces",
        help=(
            "Emit generated agent surfaces (base + effective plugin trees, "
            "root .github/prompts, Cursor/Grok native wiring) locally inside a "
            "self-host linked worktree that has no .workbay/remote clone."
        ),
    )
    p_bootstrap_surfaces.add_argument(
        "--target",
        type=Path,
        default=None,
        help="Self-host worktree to bootstrap. Defaults to the current directory.",
    )
    p_bootstrap_surfaces.add_argument(
        "--primary",
        type=Path,
        default=None,
        help=(
            "Primary overlay root supplying .cursor/mcp.json. Defaults to "
            "resolving it by the .workbay-bootstrap.json marker."
        ),
    )
    p_bootstrap_surfaces.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit the bootstrap receipt as JSON.",
    )

    p_overrides = sub.add_parser(
        "overrides",
        help="Inspect and maintain consumer plugin overrides (internal).",
    )
    overrides_sub = p_overrides.add_subparsers(dest="overrides_command", required=True)
    p_ov_status = overrides_sub.add_parser(
        "status",
        help="Report each declared override with its composition status.",
    )
    p_ov_status.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    p_ov_status.add_argument(
        "--plugin-overrides",
        type=Path,
        default=None,
        help="Optional explicit plugin override root.",
    )
    p_ov_status.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit the status report as JSON.",
    )
    p_ov_accept = overrides_sub.add_parser(
        "accept-upstream",
        help=(
            "Re-record one skill override's upstream digest against the "
            "current base tree (patch mode also refreshes the stored fork "
            "copy), writing accept provenance into overrides.lock.json."
        ),
    )
    p_ov_accept.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Consumer repository root that was previously installed.",
    )
    p_ov_accept.add_argument(
        "skill",
        help="Skill slug whose override should accept the current upstream.",
    )
    p_ov_accept.add_argument(
        "--plugin-overrides",
        type=Path,
        default=None,
        help="Optional explicit plugin override root.",
    )
    p_ov_accept.add_argument(
        "--force",
        action="store_true",
        help="Proceed even when the override root has uncommitted changes.",
    )
    return parser


def _print_local_precedence_skips(manifest: Mapping[str, object]) -> None:
    """Name every surface install/update left under local precedence.

    internal: a receipt that bumps ``remote_ref`` while ``source=local``
    surfaces keep older content must never report silently. One line per skip
    plus an aggregate count.
    """
    surfaces = manifest.get("surfaces")
    if not isinstance(surfaces, list):
        return
    skipped = [
        entry["path"]
        for entry in surfaces
        if isinstance(entry, dict) and entry.get("source") == "local"
    ]
    if not skipped:
        return
    for path in skipped:
        print(
            f"skipped (local precedence): {path} — run doctor for drift detail",
            file=sys.stdout,
        )
    print(
        f"{len(skipped)} surface(s) kept under local precedence.",
        file=sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "install":
        from workbay_bootstrap.install_receipt import (
            InstallExecutionError,
            InstallPreflightError,
        )

        try:
            manifest = install(
                target=args.target.resolve(),
                remote_url=args.remote_url,
                remote_ref=args.remote_ref,
                source=args.source,
                mcp_servers=_resolve_mcp_servers(
                    args.mcp_servers,
                    no_servers=args.no_mcp_servers,
                    default_when_unset=True,
                ),
                plugin_overrides=args.plugin_overrides,
                reset_overrides=args.reset_overrides,
                backup_overrides=args.backup,
                enforce_required_surfaces=not args.no_enforce_required_surfaces,
                profile=args.profile,
                install_claude_stop_hook_local=args.install_claude_stop_hook_local,
                install_codex_stop_hook=args.install_codex_stop_hook,
                install_vscode_stop_hook=args.install_vscode_stop_hook,
                install_grok_stop_hook=args.install_grok_stop_hook,
                install_claude_reinject_hook_local=args.install_claude_reinject_hook_local,
            )
        except (InstallExecutionError, InstallPreflightError) as exc:
            print(str(exc), file=sys.stderr)
            return 1 if exc.failure_class == "system" else 2
        source_kind = manifest.get("source_kind") or "git_overlay"
        if source_kind == "package":
            print(
                f"installed workbay-system overlay: "
                f"workbay-system=={manifest['package_version']} -> {args.target}",
                file=sys.stdout,
            )
        elif source_kind == "worktree":
            print(
                f"installed workbay-system overlay: "
                f"worktree@{manifest['remote_sha']} -> {args.target}",
                file=sys.stdout,
            )
        else:
            remote_url = args.remote_url or manifest.get(
                "remote_url", DEFAULT_REMOTE_URL
            )
            print(
                f"installed workbay-system overlay: "
                f"{remote_url}@{manifest['remote_sha']} -> {args.target}",
                file=sys.stdout,
            )
        if isinstance(manifest.get("override_backup_path"), str):
            print(
                f"override backup: {manifest['override_backup_path']}", file=sys.stdout
            )
        if isinstance(manifest.get("state_backup_path"), str):
            print(f"state backup: {manifest['state_backup_path']}", file=sys.stdout)
        _print_local_precedence_skips(manifest)
        return 0

    if args.command == "status":
        try:
            summary = status(target=args.target)
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        sys.stdout.write(summary)
        return 0

    if args.command == "doctor":
        try:
            findings = doctor(
                target=args.target,
                mcp_servers=_resolve_mcp_servers(args.mcp_servers),
                plugin_overrides=args.plugin_overrides,
                check_pypi=args.check_pypi,
            )
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        # Informational notes (severity=info, e.g. local_override) and
        # advisory warnings (severity=warning, e.g. grok_stale_selector_warning)
        # are listed but never affect the exit code (internal; internal
        # requires stale-selector detection to be reported, never a hard fail).
        notes = [f for f in findings if f.get("severity") in {"info", "warning"}]
        actionable = [
            f for f in findings if f.get("severity") not in {"info", "warning"}
        ]

        def _line(f: dict[str, str]) -> str:
            # Older facets carry remediation text under 'detail'; newer ones
            # (mcp config, hook coherence, grok) under 'message'.
            detail = f.get("detail") or f.get("message")
            return f"{f['kind']}: {f['path']}" + (f" — {detail}" if detail else "")

        for f in actionable:
            print(_line(f), file=sys.stdout)
        for f in notes:
            prefix = "warning" if f.get("severity") == "warning" else "note"
            print(f"{prefix} {_line(f)}", file=sys.stdout)
        if not actionable:
            print("doctor: no drift detected.", file=sys.stdout)
            return 0
        return 1

    if args.command == "update":
        try:
            manifest = update(
                target=args.target,
                remote_ref=args.remote_ref,
                remote_url=args.remote_url,
                mcp_servers=_resolve_mcp_servers(args.mcp_servers),
                plugin_overrides=args.plugin_overrides,
                reset_overrides=args.reset_overrides,
                backup_overrides=args.backup,
                enforce_required_surfaces=not args.no_enforce_required_surfaces,
                adopt_redundant=not args.no_adopt_redundant,
            )
        except (FileNotFoundError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 1
        update_kind = manifest.get("source_kind") or "git_overlay"
        if update_kind == "package":
            print(
                "update: refreshed overlay from "
                f"workbay-system=={manifest['package_version']}.",
                file=sys.stdout,
            )
        elif update_kind == "worktree":
            print(
                "update: refreshed worktree overlay at "
                f"HEAD (sha={str(manifest['remote_sha'])[:12]}).",
                file=sys.stdout,
            )
        else:
            print(
                f"update: refreshed overlay at {manifest['remote_ref']} "
                f"(sha={manifest['remote_sha'][:12]}).",
                file=sys.stdout,
            )
        if isinstance(manifest.get("override_backup_path"), str):
            print(
                f"override backup: {manifest['override_backup_path']}", file=sys.stdout
            )
        for path in manifest.get("adopted_redundant") or []:
            print(
                f"adopted redundant local surface: {path} "
                "(identical to payload; backup under .workbay/backup/)",
                file=sys.stdout,
            )
        _print_local_precedence_skips(manifest)
        return 0

    if args.command == "apply-hooks":
        try:
            manifest = apply_hooks(
                target=args.target,
                install_claude_stop_hook_local=args.install_claude_stop_hook_local,
                install_codex_stop_hook=args.install_codex_stop_hook,
                install_vscode_stop_hook=args.install_vscode_stop_hook,
                install_grok_stop_hook=args.install_grok_stop_hook,
                install_claude_reinject_hook_local=args.install_claude_reinject_hook_local,
            )
        except (FileNotFoundError, ValueError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            return 1
        applied = [
            entry.get("opt_in_flag")
            for entry in (manifest.get("configs") or [])
            if isinstance(entry, dict) and entry.get("kind") == "hook_adapter"
        ]
        print(
            f"apply-hooks: recorded {len(applied)} managed adapter(s) in manifest.",
            file=sys.stdout,
        )
        return 0

    if args.command == "repair":
        try:
            report = repair(
                target=args.target,
                force_dirty=args.force_dirty,
                mcp_servers=_resolve_managed_servers(args.target, args.mcp_servers),
                plugin_overrides=args.plugin_overrides,
                adopt_stale_local=args.adopt_stale_local,
            )
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        for f in report["repaired"]:
            print(f"repaired {f['kind']}: {f['path']}", file=sys.stdout)
        for f in report["skipped"]:
            if f.get("kind") == "install_step_repair_skipped":
                detail = f.get("detail", "")
                suffix = f" ({detail})" if detail else ""
                print(
                    f"skipped {f['kind']}: {f['path']}{suffix}",
                    file=sys.stdout,
                )
            else:
                print(
                    f"skipped {f['kind']}: {f['path']} "
                    "(re-run with --force-dirty to overwrite)",
                    file=sys.stdout,
                )
        if not report["repaired"] and not report["skipped"]:
            print("repair: no drift detected.", file=sys.stdout)
        return 0

    if args.command == "overrides":
        from workbay_bootstrap.overrides import (
            OverridesError,
            accept_upstream,
            overrides_status,
        )

        if args.overrides_command == "status":
            try:
                report = overrides_status(
                    target=args.target, plugin_overrides=args.plugin_overrides
                )
            except OverridesError as exc:
                print(f"overrides status: {exc}", file=sys.stderr)
                return 1
            if args.as_json:
                print(json.dumps(report, indent=2), file=sys.stdout)
            else:
                print(f"override root: {report['override_root']}", file=sys.stdout)
                for row in report["components"]:
                    print(
                        f"  {row['component_kind']} {row['name']}: "
                        f"mode={row['mode']} status={row['status']}",
                        file=sys.stdout,
                    )
            return 0

        if args.overrides_command == "accept-upstream":
            try:
                receipt = accept_upstream(
                    target=args.target,
                    skill=args.skill,
                    plugin_overrides=args.plugin_overrides,
                    force=args.force,
                )
            except OverridesError as exc:
                print(f"overrides accept-upstream: {exc}", file=sys.stderr)
                return 1
            print(
                f"accepted upstream for {receipt['skill']} "
                f"({receipt['previous_upstream_digest']} -> "
                f"{receipt['new_upstream_digest']}); "
                f"next: {receipt['next_command']}",
                file=sys.stdout,
            )
            return 0

    if args.command == "adopt-worktree":
        from workbay_bootstrap.adopt import adopt_worktree
        from workbay_bootstrap.worktree import WorktreeError

        target = args.target if args.target is not None else Path.cwd()
        try:
            receipt: dict[str, Any] = adopt_worktree(
                target=target, primary=args.primary, check=args.check
            )
        except WorktreeError as exc:
            print(f"adopt-worktree: {exc}", file=sys.stderr)
            return 1

        if args.as_json:
            print(json.dumps(receipt, indent=2), file=sys.stdout)
        elif args.check:
            if receipt["ok"]:
                print("adopt-worktree: no drift detected.", file=sys.stdout)
            else:
                for entry in receipt["drift"]:
                    print(f"drift: {entry}", file=sys.stdout)
        elif receipt["adopted"]:
            print(
                f"adopt-worktree: adopted overlay from {receipt['primary']}.",
                file=sys.stdout,
            )
        else:
            print(f"adopt-worktree: no-op ({receipt['reason']}).", file=sys.stdout)

        if args.check and not receipt["ok"]:
            return 1
        return 0

    if args.command == "bootstrap-surfaces":
        from workbay_bootstrap.bootstrap_surfaces import bootstrap_surfaces

        target = args.target if args.target is not None else Path.cwd()
        receipt = bootstrap_surfaces(target=target, primary=args.primary)

        if args.as_json:
            print(json.dumps(receipt, indent=2), file=sys.stdout)
        elif receipt["ok"]:
            print(
                f"bootstrap-surfaces: emitted surfaces into {receipt['target']}.",
                file=sys.stdout,
            )
        else:
            print(
                f"bootstrap-surfaces: skipped ({receipt['skipped']}).",
                file=sys.stderr,
            )
        return 0 if receipt["ok"] else 1

    if args.command == "mcp-sync":
        try:
            servers = _resolve_managed_servers(args.target, args.mcp_servers)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            print(f"mcp-sync: --mcp-servers: {exc}", file=sys.stderr)
            return 2
        if servers is None:
            print(
                "mcp-sync: --mcp-servers must resolve to a server mapping.",
                file=sys.stderr,
            )
            return 2
        try:
            report = sync_mcp_configs(
                args.target,
                servers,
                surfaces=tuple(args.surfaces),
                check_only=args.check,
                prune_removed_managed=args.prune_removed_managed,
            )
        except FileNotFoundError as exc:
            print(f"mcp-sync: {exc}", file=sys.stderr)
            return 2
        if args.emit_json:
            print(json.dumps(_sync_report_to_dict(report), indent=2))
        else:
            _print_sync_report(report, check_only=args.check)
        return report.exit_code

    # argparse with required=True prevents this branch from being reachable.
    parser.error(f"unknown command: {args.command!r}")
    return 2  # pragma: no cover


def _sync_report_to_dict(report: SyncReport) -> dict[str, Any]:
    return {
        "surfaces": [
            {
                "name": s.name,
                "path": s.path,
                "drift": s.drift,
                "action": s.action,
            }
            for s in report.surfaces
        ],
        "preserved_third_party": list(report.preserved_third_party),
        "pruned_managed": list(report.pruned_managed),
        "ledger_mcp_servers": list(report.ledger_mcp_servers),
        "exit_code": report.exit_code,
    }


def _print_sync_report(report: SyncReport, *, check_only: bool) -> None:
    mode = "check" if check_only else "apply"
    print(f"mcp-sync ({mode}):")
    for s in report.surfaces:
        marker = "*" if s.drift else " "
        print(f"  {marker} {s.name:<7} {s.path:<22} {s.action}")
    if report.preserved_third_party:
        print(f"  preserved third-party: {', '.join(report.preserved_third_party)}")
    if report.pruned_managed:
        print(f"  pruned removed-managed: {', '.join(report.pruned_managed)}")
    if not check_only and report.ledger_mcp_servers:
        print(f"  ledger mcp_servers: {', '.join(report.ledger_mcp_servers)}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
