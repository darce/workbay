"""Config-only MCP-server reconciliation for ``workbay-bootstrap``.

Public entry point ``sync_mcp_configs(target, mcp_servers, *, surfaces,
check_only)`` rewrites (or, in check mode, only inspects) the three
client config surfaces and the bootstrap ledger's ``mcp_servers``
provenance block — without fetching the remote, regenerating skill
surfaces, or running ``init-state``. ``install`` and ``mcp-sync`` share
the same render seam in ``install.py`` so byte output is identical.

This module is parameter-only by design (no implicit file discovery
past what is passed in) so non-CLI callers — Make targets, the
``bootstrap doctor`` drift check, future release helpers — drive the
same code path as the CLI subcommand. implementation note covers the basic check
+ apply paths plus the surfaces filter; ``--prune-removed-managed``
lands in a follow-up slice.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from workbay_bootstrap.install import (
    BOOTSTRAP_MANIFEST_NAME,
    _render_codex_config,
    _render_cursor_mcp_json,
    _render_mcp_json,
    _render_vscode_mcp_json,
    _write_codex_config,
    _write_cursor_mcp_json,
    _write_mcp_json,
    _write_vscode_mcp_json,
)


SurfaceName = Literal["claude", "vscode", "codex", "cursor"]
SurfaceAction = Literal["created", "merged", "unchanged", "would_write"]


SUPPORTED_SURFACES: frozenset[str] = frozenset({"claude", "vscode", "codex", "cursor"})
"""Stable set of surface names this sync API knows how to render."""


def _root_owned_surfaces() -> tuple[str, ...]:
    """Surfaces whose harness declares ``root`` MCP registration ownership.

    implementation note: the generated ``MCP_REGISTRATION`` table (rendered from
    mcp_servers.yaml) decides which harnesses get bootstrap-written root
    surfaces. A harness flipped to ``plugin`` ownership stops receiving a
    root surface so the plugin tree is its only registration carrier —
    dual registration is never valid. (Grok has no surface of its own: it
    reads the ``claude`` root ``.mcp.json`` compat surface.)
    """
    from workbay_bootstrap._mcp_pins import MCP_REGISTRATION

    ordered = ("claude", "vscode", "codex", "cursor")
    return tuple(
        surface for surface in ordered if MCP_REGISTRATION.get(surface) == "root"
    )


DEFAULT_SURFACES: tuple[str, ...] = _root_owned_surfaces()
"""Surfaces touched when the caller does not pass an explicit subset."""


_SURFACE_PATHS: dict[str, str] = {
    "claude": ".mcp.json",
    "vscode": ".vscode/mcp.json",
    "codex": ".codex/config.toml",
    "cursor": ".cursor/mcp.json",
}


_RENDERERS: dict[str, Any] = {
    "claude": _render_mcp_json,
    "vscode": _render_vscode_mcp_json,
    "codex": _render_codex_config,
    "cursor": _render_cursor_mcp_json,
}


_WRITERS: dict[str, Any] = {
    "claude": _write_mcp_json,
    "vscode": _write_vscode_mcp_json,
    "codex": _write_codex_config,
    "cursor": _write_cursor_mcp_json,
}


@dataclass(frozen=True)
class SurfaceReport:
    """Per-surface outcome of a sync pass.

    ``drift`` is True when the rendered bytes differ from the on-disk
    file (or the file is absent). ``action`` is the operator-facing
    label for what happened: ``created`` (file did not exist and apply
    wrote it), ``merged`` (file existed and apply rewrote it),
    ``unchanged`` (no drift), or ``would_write`` (check mode saw drift
    but did not touch disk).
    """

    name: str
    path: str
    drift: bool
    action: SurfaceAction
    preserved_third_party: tuple[str, ...] = ()


@dataclass(frozen=True)
class SyncReport:
    """Aggregate result of a ``sync_mcp_configs`` call.

    ``exit_code`` matches the CLI contract (``0`` clean reconcile,
    ``1`` drift detected with ``check_only=True``, ``>=2`` reserved for
    resolution failures the CLI raises before reaching this code path).
    ``ledger_mcp_servers`` reflects the names persisted to the ledger
    after this pass — the empty list when the ledger was not rewritten
    (e.g. ``check_only=True``).
    """

    surfaces: tuple[SurfaceReport, ...]
    preserved_third_party: tuple[str, ...] = ()
    pruned_managed: tuple[str, ...] = ()
    ledger_mcp_servers: tuple[str, ...] = ()
    exit_code: int = 0


def sync_mcp_configs(
    target: Path,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    surfaces: Sequence[str] = DEFAULT_SURFACES,
    check_only: bool = False,
    prune_removed_managed: bool = False,
) -> SyncReport:
    """Reconcile the client config surfaces against ``mcp_servers``.

    ``check_only=True`` returns drift information without touching disk
    (the render seam guarantees no surface file is created or modified).
    ``check_only=False`` writes only the surfaces that drift and rewrites
    the ledger's ``mcp_servers`` block to ``sorted(mcp_servers.keys())``
    so the next run sees the new baseline.

    ``prune_removed_managed=True`` reads the ledger's ``mcp_servers``
    provenance block — the authoritative record of names this tool
    previously managed — computes
    ``prune_set = previously_managed - resolved_map.keys()``, and drops
    those keys from the rendered surfaces. Third-party launchers (names
    NOT in the ledger) are never pruned. On legacy targets where the
    ledger lacks the block (or has ``[]``), the first run is a prune
    no-op; the block is seeded from the resolved map at write time so
    the next run has provenance.

    Raises:
        ValueError: ``surfaces`` contains a name not in
            :data:`SUPPORTED_SURFACES`.
    """
    target = Path(target)
    requested = tuple(surfaces)
    unknown = [name for name in requested if name not in SUPPORTED_SURFACES]
    if unknown:
        raise ValueError(
            f"surfaces={requested!r} contains unknown name(s) {unknown!r}; "
            f"expected a subset of {sorted(SUPPORTED_SURFACES)!r}."
        )

    prune_names: tuple[str, ...] = ()
    if prune_removed_managed:
        previously_managed = _read_ledger_mcp_servers(target)
        resolved = set(mcp_servers)
        prune_names = tuple(sorted(set(previously_managed) - resolved))

    surface_reports: list[SurfaceReport] = []
    any_drift = False
    for name in requested:
        report = _evaluate_surface(
            target,
            name,
            mcp_servers,
            check_only=check_only,
            prune_names=prune_names,
        )
        surface_reports.append(report)
        if report.drift:
            any_drift = True

    ledger_names: tuple[str, ...] = ()
    if not check_only:
        ledger_names = _rewrite_ledger_mcp_servers(target, sorted(mcp_servers))

    preserved = tuple(
        sorted({name for s in surface_reports for name in s.preserved_third_party})
    )

    exit_code = 1 if (check_only and any_drift) else 0
    return SyncReport(
        surfaces=tuple(surface_reports),
        preserved_third_party=preserved,
        pruned_managed=prune_names,
        ledger_mcp_servers=ledger_names,
        exit_code=exit_code,
    )


def _evaluate_surface(
    target: Path,
    name: str,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    check_only: bool,
    prune_names: tuple[str, ...] = (),
) -> SurfaceReport:
    surface_path = _SURFACE_PATHS[name]
    on_disk_path = target / surface_path
    rendered = _RENDERERS[name](target, mcp_servers, prune_names=prune_names)
    existed = on_disk_path.exists()
    on_disk = on_disk_path.read_bytes() if existed else b""
    drift = rendered != on_disk

    if not drift:
        action: SurfaceAction = "unchanged"
    elif check_only:
        action = "would_write"
    else:
        _WRITERS[name](target, mcp_servers, prune_names=prune_names)
        action = "merged" if existed else "created"

    preserved = _preserved_third_party_names(name, on_disk, mcp_servers, prune_names)

    return SurfaceReport(
        name=name,
        path=surface_path,
        drift=drift,
        action=action,
        preserved_third_party=preserved,
    )


def _preserved_third_party_names(
    name: str,
    on_disk: bytes,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    prune_names: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """Return server names present on disk that are NOT in the managed map.

    These are launchers the consumer added themselves; they survive the
    render path because the writers only merge managed names. Reporting
    the set lets the operator see at a glance that their custom entries
    are not being silently rewritten.
    """
    if not on_disk:
        return ()
    managed = set(mcp_servers)
    if name == "claude":
        try:
            doc = json.loads(on_disk.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ()
        servers = doc.get("mcpServers", {})
    elif name == "vscode":
        try:
            doc = json.loads(on_disk.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ()
        servers = doc.get("servers", {})
    else:
        # Codex TOML: no managed/third-party split is required for slice
        # 1d's report shape — preservation is structural in the writer
        # (managed tables are replaced; everything else stays). Treat
        # third-party as empty here; the byte-parity test in
        # test_render_seam.py already pins the preservation property.
        return ()
    if not isinstance(servers, dict):
        return ()
    return tuple(sorted(set(servers) - managed - set(prune_names)))


def _read_ledger_mcp_servers(target: Path) -> tuple[str, ...]:
    """Return the ledger's previously-managed names, or ``()`` when the
    ledger is missing or the block is absent / empty.

    The empty result drives the legacy-fallback path in
    ``sync_mcp_configs(prune_removed_managed=True)``: the first run is a
    prune no-op for that target; the rewrite step seeds the block from
    the resolved map so the next run has provenance.
    """
    ledger_path = target / BOOTSTRAP_MANIFEST_NAME
    if not ledger_path.exists():
        return ()
    try:
        payload = json.loads(ledger_path.read_text())
    except json.JSONDecodeError:
        return ()
    block = payload.get("mcp_servers")
    if not isinstance(block, list):
        return ()
    return tuple(name for name in block if isinstance(name, str))


def _rewrite_ledger_mcp_servers(target: Path, names: list[str]) -> tuple[str, ...]:
    """Rewrite the ledger's ``mcp_servers`` block to ``names``.

    No-op when the ledger does not exist — ``sync_mcp_configs`` is a
    config-only refresh path and does not synthesize ledgers for
    targets that were never bootstrapped.
    """
    ledger_path = target / BOOTSTRAP_MANIFEST_NAME
    if not ledger_path.exists():
        return tuple(names)
    payload = json.loads(ledger_path.read_text())
    payload["mcp_servers"] = list(names)
    ledger_path.write_text(json.dumps(payload, indent=2) + "\n")
    return tuple(names)
