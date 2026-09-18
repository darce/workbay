"""Config-only MCP-server reconciliation for ``workbay-bootstrap``.

Public entry point ``sync_mcp_configs(target, mcp_servers, *, surfaces,
check_only)`` rewrites (or, in check mode, only inspects) the four
client config surfaces and the bootstrap ledger's ``mcp_servers``
provenance block — without fetching the remote, regenerating skill
surfaces, or running ``init-state``. ``install`` and ``mcp-sync`` share
the same render seam in ``install.py`` so byte output is identical.

Apply is two-phase: every requested surface is validated (check mode)
before the first write. An invalid surface refuses the whole apply with
nothing written (CARD-02). The consumer root is pinned once at the
sync boundary (HARM3FIX05RV-001): opened ``O_DIRECTORY|O_NOFOLLOW``
and carried through every surface read, stage, replace, and ledger
rewrite so a swapped ``--target`` cannot be re-resolved into an
outside write (WEB-13, CON-11). The ledger is the owner record
(DATA-14): each surface persist is an atomic same-dir temp file +
``os.replace`` so a writer that raises cannot leave new bytes without
a returned action. Publication is marked immediately after a
successful replace so a later dir-fsync failure still owns the
published names (HARM3FIX05RV-002, DATA-16). After replace, parent
identity (``st_dev``/``st_ino``, no-follow against the pinned root)
and the destination entry are re-checked; a mismatch is
``invalid_surface_path``, never ``created`` (HARM3FIX05RV-003,
CARD-07). The pinned-root identity is the sole authority through
the final replace, reporting, and every post-sync action
(HARM3FIX06R2RV-001): a caller-visible root that no longer names
the pinned inode is ``invalid_surface_path``, never ``created``.
The staged file identity must survive replace (HARM3FIX06R2RV-002).
The ledger parent directory is fsynced after the ledger rename
(HARM3FIX06R2RV-003, DATA-16). The ledger is preflighted through
the pinned root before any surface publication
(HARM3FIX06R2RV-004, DATA-14). The replace is relative to a parent
directory descriptor opened ``O_DIRECTORY|O_NOFOLLOW`` so a
symlinked or swapped ``.vscode`` / ``.codex`` / ``.cursor`` cannot
carry bytes outside the consumer (HARM3FIX04RV-001). The staged
inode is re-checked immediately before ``published=True`` and
again before the ledger names it (HARM3FIX09R3RV-002). The ledger
inode pinned at preflight is re-stat'd no-follow immediately
before rewrite (HARM3FIX09R3RV-003). Post-sync advice walks and
launches the shim through the pinned directory fd
(HARM3FIX09R3RV-001). Writes that
do land persist ``mcp_servers`` in the same pass rather than
rolling files back, including when a later writer raises. A
refusal names the invalid surface and path and exits nonzero
(CARD-07).

This module is parameter-only by design (no implicit file discovery
past what is passed in) so non-CLI callers — Make targets, the
``bootstrap doctor`` drift check, future release helpers — drive the
same code path as the CLI subcommand.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from workbay_bootstrap.install import (
    BOOTSTRAP_MANIFEST_NAME,
    InvalidCodexConfigError,
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
SurfaceAction = Literal[
    "created",
    "merged",
    "unchanged",
    "would_write",
    "invalid_codex_config",
    "invalid_surface_path",
]
_REFUSAL_ACTIONS: frozenset[str] = frozenset({"invalid_codex_config", "invalid_surface_path"})


class SurfacePathRefusalError(ValueError):
    """Destination escaped the target, or a parent/dest path is a symlink."""


@dataclass
class _PinnedRoot:
    """Consumer root opened once at the sync boundary (HARM3FIX05RV-001)."""

    fd: int
    path: Path
    identity: tuple[int, int]
    published: bool = False
    ledger_identity: tuple[int, int] | None = None
    published_identities: dict[str, tuple[int, int]] = field(default_factory=dict)


def _is_refusal(action: str) -> bool:
    return action in _REFUSAL_ACTIONS


def _format_refusal_detail(surfaces: Sequence[SurfaceReport]) -> str:
    """Shared refusal classifier text: ``action: path`` for every refusal."""
    refused = [s for s in surfaces if _is_refusal(s.action)]
    return ", ".join(f"{s.action}: {s.path}" for s in refused) or "mcp-sync refused"


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
    return tuple(surface for surface in ordered if MCP_REGISTRATION.get(surface) == "root")


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
    ``unchanged`` (no drift), ``would_write`` (check mode saw drift
    but did not touch disk), ``invalid_codex_config`` (the Codex
    surface could not be parsed; the whole apply is refused), or
    ``invalid_surface_path`` (a destination parent escaped the target
    or was a symlink; the whole apply is refused).
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
    after_sync: Callable[[_PinnedRoot], None] | None = None,
) -> SyncReport:
    """Reconcile the client config surfaces against ``mcp_servers``.

    ``check_only=True`` returns drift information without touching disk
    (the render seam guarantees no surface file is created or modified).
    ``check_only=False`` is two-phase: every requested surface is
    validated with no writes, then (only if every surface is valid)
    drifted surfaces are written and the ledger's ``mcp_servers`` block
    is rewritten to ``sorted(mcp_servers.keys())``.

    Ownership policy (DATA-14): the ledger is the owner record. An
    invalid surface refuses the whole apply *before* the first write, so
    there is nothing to own. Each surface write is atomic (temp file
    created with the pinned parent ``dir_fd`` +
    ``os.replace(..., src_dir_fd=fd, dst_dir_fd=fd)``) so a raise
    during a writer cannot publish new bytes without a returned action,
    and a symlink parent cannot redirect the replace. If a
    write still lands (a later writer raises after an earlier surface
    was persisted), this pass records the desired names in the ledger
    rather than rolling files back, so a later ``prune_removed_managed``
    run can remove them. The no-write refusal path leaves the ledger
    unchanged.

    ``prune_removed_managed=True`` reads the ledger's ``mcp_servers``
    provenance block — the authoritative record of names this tool
    previously managed — computes
    ``prune_set = previously_managed - resolved_map.keys()``, and drops
    those keys from the rendered surfaces. Third-party launchers (names
    NOT in the ledger) are never pruned. On legacy targets where the
    ledger lacks the block (or has ``[]``), the first run is a prune
    no-op; the block is seeded from the resolved map at write time so
    the next run has provenance.

    ``after_sync`` runs while the consumer-root descriptor is still
    held so post-sync actions (CLI availability advice) use the pinned
    inode rather than re-resolving the caller path
    (HARM3FIX06R2RV-001). Failures in the hook are swallowed: an
    advisory must not rewrite a completed sync outcome.

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

    try:
        with _pin_consumer_root(target) as pinned:
            report = _sync_mcp_configs_pinned(
                pinned,
                mcp_servers,
                requested=requested,
                check_only=check_only,
                prune_removed_managed=prune_removed_managed,
            )
            if after_sync is not None:
                try:
                    after_sync(pinned)
                except Exception:
                    pass
            return report
    except SurfacePathRefusalError:
        return _sync_report(
            [
                SurfaceReport(
                    name=name,
                    path=_SURFACE_PATHS[name],
                    drift=True,
                    action="invalid_surface_path",
                )
                for name in requested
            ],
            prune_names=(),
            ledger_names=(),
            exit_code=1,
        )


def _sync_mcp_configs_pinned(
    pinned: _PinnedRoot,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    requested: tuple[str, ...],
    check_only: bool,
    prune_removed_managed: bool,
) -> SyncReport:
    prune_names: tuple[str, ...] = ()
    if prune_removed_managed:
        previously_managed = _read_ledger_mcp_servers(pinned)
        resolved = set(mcp_servers)
        prune_names = tuple(sorted(set(previously_managed) - resolved))

    validation_reports = [
        _evaluate_surface(
            pinned,
            name,
            mcp_servers,
            check_only=True,
            prune_names=prune_names,
        )
        for name in requested
    ]
    refused = any(_is_refusal(s.action) for s in validation_reports)
    if refused:
        return _sync_report(
            validation_reports,
            prune_names=prune_names,
            ledger_names=(),
            exit_code=1,
        )
    if check_only:
        any_drift = any(s.drift for s in validation_reports)
        return _sync_report(
            validation_reports,
            prune_names=prune_names,
            ledger_names=(),
            exit_code=1 if any_drift else 0,
        )

    _preflight_ledger(pinned)

    surface_reports: list[SurfaceReport] = []
    written = False
    ledger_names: tuple[str, ...] = ()
    try:
        for name in requested:
            report = _evaluate_surface(
                pinned,
                name,
                mcp_servers,
                check_only=False,
                prune_names=prune_names,
            )
            surface_reports.append(report)
            if report.action in ("created", "merged"):
                written = True
            if _is_refusal(report.action):
                # Race: the surface became invalid after phase-1 validation.
                # Stop further writers; ownership for what already landed is
                # persisted below.
                break
        write_refused = any(_is_refusal(s.action) for s in surface_reports)
        if written or not write_refused:
            _before_ledger_rewrite(pinned)
            try:
                _assert_published_destination_identities(pinned)
                ledger_names = _rewrite_ledger_mcp_servers(pinned, sorted(mcp_servers))
            except SurfacePathRefusalError:
                surface_reports.append(
                    SurfaceReport(
                        name="ledger",
                        path=BOOTSTRAP_MANIFEST_NAME,
                        drift=True,
                        action="invalid_surface_path",
                    )
                )
                ledger_names = ()
    except Exception:
        if written or pinned.published:
            try:
                _rewrite_ledger_mcp_servers(pinned, sorted(mcp_servers))
            except Exception:
                pass
        raise

    write_refused = any(_is_refusal(s.action) for s in surface_reports)
    return _sync_report(
        surface_reports,
        prune_names=prune_names,
        ledger_names=ledger_names,
        exit_code=1 if write_refused else 0,
    )


def _sync_report(
    surface_reports: list[SurfaceReport],
    *,
    prune_names: tuple[str, ...],
    ledger_names: tuple[str, ...],
    exit_code: int,
) -> SyncReport:
    preserved = tuple(sorted({name for s in surface_reports for name in s.preserved_third_party}))
    return SyncReport(
        surfaces=tuple(surface_reports),
        preserved_third_party=preserved,
        pruned_managed=prune_names,
        ledger_mcp_servers=ledger_names,
        exit_code=exit_code,
    )


def _evaluate_surface(
    pinned: _PinnedRoot,
    name: str,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    check_only: bool,
    prune_names: tuple[str, ...] = (),
) -> SurfaceReport:
    surface_path = _SURFACE_PATHS[name]
    on_disk_path = pinned.path / surface_path
    try:
        _assert_pinned_root(pinned)
        _assert_contained_surface_path(pinned.path, surface_path)
    except SurfacePathRefusalError:
        return SurfaceReport(name=name, path=surface_path, drift=True, action="invalid_surface_path")
    try:
        rendered = _RENDERERS[name](pinned.path, mcp_servers, prune_names=prune_names)
    except InvalidCodexConfigError:
        return SurfaceReport(name=name, path=surface_path, drift=True, action="invalid_codex_config")
    try:
        _assert_pinned_root(pinned)
    except SurfacePathRefusalError:
        return SurfaceReport(name=name, path=surface_path, drift=True, action="invalid_surface_path")
    existed = on_disk_path.exists()
    on_disk = on_disk_path.read_bytes() if existed else b""
    drift = rendered != on_disk

    if not drift:
        action: SurfaceAction = "unchanged"
    elif check_only:
        action = "would_write"
    else:
        try:
            _atomic_write_surface(pinned, name, mcp_servers, prune_names=prune_names)
        except InvalidCodexConfigError:
            return SurfaceReport(name=name, path=surface_path, drift=True, action="invalid_codex_config")
        except SurfacePathRefusalError:
            return SurfaceReport(name=name, path=surface_path, drift=True, action="invalid_surface_path")
        action = "merged" if existed else "created"

    preserved = _preserved_third_party_names(name, on_disk, mcp_servers, prune_names)

    return SurfaceReport(
        name=name,
        path=surface_path,
        drift=drift,
        action=action,
        preserved_third_party=preserved,
    )


def _before_atomic_stage(dest: Path) -> None:
    """Test seam (CON-11): swap dest.parent after pin, before staging."""


def _before_publish_mark(dest: Path) -> None:
    """Test seam (CON-11): swap dest after identity check, before published=True."""


def _before_ledger_rewrite(pinned: _PinnedRoot) -> None:
    """Test seam (CON-11): swap the ledger after publication, before rewrite."""


def _surface_parent_components(target: Path, dest_rel: str) -> tuple[Path, tuple[str, ...], str]:
    """Split a surface path into pinned root, parent parts, and file name.

    Does not re-resolve ``target``: the consumer root is pinned once at the
    sync boundary (HARM3FIX05RV-001, WEB-13, CON-11).
    """
    rel = Path(dest_rel)
    if rel.is_absolute() or any(part == ".." for part in rel.parts):
        raise SurfacePathRefusalError(f"refusing non-relative surface path: {dest_rel}")
    if rel.name in {"", ".", ".."}:
        raise SurfacePathRefusalError(f"refusing surface path without a file name: {dest_rel}")
    parent_parts = tuple(part for part in rel.parent.parts if part not in {"", "."})
    return Path(target), parent_parts, rel.name


def _assert_beneath(root: Path, path: Path) -> None:
    """WEB-13: ``path`` must stay beneath the already-resolved ``root``."""
    root_s = os.path.normpath(os.fspath(root))
    path_s = os.path.normpath(os.fspath(path))
    if path_s != root_s and not path_s.startswith(root_s + os.sep):
        raise SurfacePathRefusalError(f"surface path {path} escapes target {root}")


def _assert_contained_surface_path(target: Path, dest_rel: str) -> None:
    """Refuse symlink parents/destinations without creating missing dirs."""
    resolved, parts, name = _surface_parent_components(target, dest_rel)
    current = resolved
    if current.is_symlink():
        raise SurfacePathRefusalError(f"refusing symlink root: {current}")
    _assert_beneath(resolved, current)
    for part in parts:
        current = current / part
        _assert_beneath(resolved, current)
        if current.is_symlink():
            raise SurfacePathRefusalError(f"refusing symlink parent: {current}")
        if not current.exists():
            return
        if not current.is_dir():
            raise SurfacePathRefusalError(f"refusing non-directory parent: {current}")
    dest = current / name
    if dest.is_symlink():
        raise SurfacePathRefusalError(f"refusing symlink destination: {dest}")


@contextmanager
def _pin_consumer_root(target: Path) -> Iterator[_PinnedRoot]:
    """Open the consumer root once with ``O_DIRECTORY|O_NOFOLLOW``.

    A symlinked ``--target`` is refused. The descriptor is the only handle
    later I/O uses; the caller path is never re-resolved (HARM3FIX05RV-001).
    """
    raw = os.path.abspath(os.fspath(target))
    path = Path(raw)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        fd = os.open(raw, flags)
    except OSError as exc:
        raise SurfacePathRefusalError(f"refusing symlinked or non-directory consumer root: {path}") from exc
    try:
        info = os.fstat(fd)
        yield _PinnedRoot(fd=fd, path=path, identity=(info.st_dev, info.st_ino))
    finally:
        os.close(fd)


def _assert_pinned_root(pinned: _PinnedRoot) -> None:
    """Refuse if the caller path no longer names the pinned inode."""
    try:
        st = os.lstat(pinned.path)
    except OSError as exc:
        raise SurfacePathRefusalError(f"refusing missing consumer root: {pinned.path}") from exc
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise SurfacePathRefusalError(f"refusing swapped or symlinked consumer root: {pinned.path}")
    if (st.st_dev, st.st_ino) != pinned.identity:
        raise SurfacePathRefusalError(f"refusing swapped consumer root: {pinned.path}")


def _assert_final_pinned_root(pinned: _PinnedRoot) -> None:
    """HARM3FIX06R2RV-001: pinned fd vs caller-visible root at replace."""
    _assert_pinned_root(pinned)


def _descriptor_root_path(dir_fd: int) -> Path:
    """Current path of the directory opened at ``dir_fd``.

    Uses the descriptor, not the caller-visible name, so a later path
    steal cannot redirect post-sync actions (HARM3FIX06R2RV-001). Linux
    uses ``/proc/self/fd/<n>``; macOS uses ``F_GETPATH`` with a
    MAXPATHLEN (1024) buffer. Either result is a name and must still
    name the same inode as ``dir_fd`` (CON-11, CARD-07).
    """
    derived: Path | None = None
    proc = f"/proc/self/fd/{dir_fd}"
    try:
        derived = Path(os.readlink(proc))
    except OSError:
        derived = None
    if derived is None:
        try:
            import fcntl
        except ImportError as exc:
            raise SurfacePathRefusalError("cannot derive pinned consumer root from descriptor") from exc
        if not hasattr(fcntl, "F_GETPATH"):
            raise SurfacePathRefusalError("cannot derive pinned consumer root from descriptor")
        try:
            raw = fcntl.fcntl(dir_fd, fcntl.F_GETPATH, bytes(1024))
        except (OSError, TypeError, ValueError) as exc:
            raise SurfacePathRefusalError("cannot derive pinned consumer root from descriptor") from exc
        if isinstance(raw, bytes):
            raw = os.fsdecode(raw.split(b"\x00", 1)[0])
        derived = Path(raw)
    try:
        path_info = os.stat(derived)
        fd_info = os.fstat(dir_fd)
    except OSError as exc:
        raise SurfacePathRefusalError("cannot derive pinned consumer root from descriptor") from exc
    if not stat.S_ISDIR(path_info.st_mode) or (path_info.st_dev, path_info.st_ino) != (
        fd_info.st_dev,
        fd_info.st_ino,
    ):
        raise SurfacePathRefusalError("cannot derive pinned consumer root from descriptor")
    return derived


def _fsync_directory(dir_fd: int) -> None:
    """Fsync a directory after a same-dir replace (DATA-16, REF-26)."""
    os.fsync(dir_fd)


def _assert_replaced_destination_identity(
    parent_fd: int,
    dest_name: str,
    staged_identity: tuple[int, int],
    dest: Path,
) -> None:
    """HARM3FIX06R2RV-002: dest must still be the staged inode after replace."""
    try:
        info = os.stat(dest_name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise SurfacePathRefusalError(f"refusing missing destination after replace: {dest}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SurfacePathRefusalError(f"refusing destination identity mismatch: {dest}")
    if (info.st_dev, info.st_ino) != staged_identity:
        raise SurfacePathRefusalError(f"refusing destination identity mismatch: {dest}")


def _open_relative_regular_file(dir_fd: int, rel: str) -> int:
    """Open ``rel`` no-follow, component by component, from ``dir_fd``.

    Directories use ``O_DIRECTORY|O_NOFOLLOW``. The final fd must name a
    regular file. The caller owns the returned descriptor.
    """
    parts = tuple(part for part in Path(rel).parts if part not in {"", "."})
    if not parts or Path(rel).is_absolute() or any(part == ".." for part in parts):
        raise SurfacePathRefusalError(f"refusing non-relative path: {rel}")
    opened: list[int] = []
    current = dir_fd
    try:
        for index, part in enumerate(parts):
            last = index == len(parts) - 1
            flags = os.O_RDONLY | os.O_NOFOLLOW
            if not last:
                flags |= os.O_DIRECTORY
            try:
                nxt = os.open(part, flags, dir_fd=current)
            except FileNotFoundError:
                raise
            except OSError as exc:
                raise SurfacePathRefusalError(
                    f"refusing non-directory or symlink component {part!r} of {rel}"
                ) from exc
            opened.append(nxt)
            current = nxt
        info = os.fstat(current)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise SurfacePathRefusalError(f"refusing non-regular path: {rel}")
        return os.dup(current)
    finally:
        for fd in opened:
            os.close(fd)


def _assert_ledger_identity(pinned: _PinnedRoot) -> None:
    """HARM3FIX09R3RV-003: ledger inode must still be the preflighted object."""
    try:
        info = os.stat(BOOTSTRAP_MANIFEST_NAME, dir_fd=pinned.fd, follow_symlinks=False)
    except FileNotFoundError as exc:
        if pinned.ledger_identity is not None:
            raise SurfacePathRefusalError(
                f"refusing missing ledger {BOOTSTRAP_MANIFEST_NAME}"
            ) from exc
        return
    except OSError as exc:
        raise SurfacePathRefusalError(
            f"refusing unusable ledger {BOOTSTRAP_MANIFEST_NAME}"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SurfacePathRefusalError(f"refusing non-regular ledger {BOOTSTRAP_MANIFEST_NAME}")
    if pinned.ledger_identity is not None and (info.st_dev, info.st_ino) != pinned.ledger_identity:
        raise SurfacePathRefusalError(f"refusing swapped ledger {BOOTSTRAP_MANIFEST_NAME}")


def _assert_published_destination_identities(pinned: _PinnedRoot) -> None:
    """Re-prove each published dest inode before the ledger names it (CON-11)."""
    for dest_rel, identity in pinned.published_identities.items():
        with _pin_contained_parent(pinned, dest_rel, create=False) as contained:
            parent_fd, dest_name, dest, _validate = contained
            _assert_replaced_destination_identity(parent_fd, dest_name, identity, dest)


def _preflight_ledger(pinned: _PinnedRoot) -> None:
    """Refuse apply when the ledger cannot be updated (HARM3FIX06R2RV-004).

    Open the ledger no-follow through the pinned root. It must be a
    regular parseable object (a JSON object) or absent and creatable in
    the pinned directory. Publish nothing when this fails.
    """
    try:
        infd = os.open(
            BOOTSTRAP_MANIFEST_NAME,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=pinned.fd,
        )
    except FileNotFoundError:
        try:
            os.stat(BOOTSTRAP_MANIFEST_NAME, dir_fd=pinned.fd, follow_symlinks=False)
        except FileNotFoundError:
            pinned.ledger_identity = None
            return
        raise SurfacePathRefusalError(
            f"refusing unusable ledger path {BOOTSTRAP_MANIFEST_NAME}"
        ) from None
    except OSError as exc:
        raise SurfacePathRefusalError(
            f"refusing symlinked or unusable ledger {BOOTSTRAP_MANIFEST_NAME}"
        ) from exc
    try:
        info = os.fstat(infd)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise SurfacePathRefusalError(f"refusing non-regular ledger {BOOTSTRAP_MANIFEST_NAME}")
        pinned.ledger_identity = (info.st_dev, info.st_ino)
        with os.fdopen(infd, "r") as inf:
            infd = -1
            try:
                payload = json.loads(inf.read())
            except json.JSONDecodeError as exc:
                raise SurfacePathRefusalError(
                    f"refusing malformed ledger {BOOTSTRAP_MANIFEST_NAME}"
                ) from exc
        if not isinstance(payload, dict):
            raise SurfacePathRefusalError(f"refusing malformed ledger {BOOTSTRAP_MANIFEST_NAME}")
    finally:
        if infd >= 0:
            os.close(infd)


@contextmanager
def _pin_contained_parent(
    pinned: _PinnedRoot, dest_rel: str, *, create: bool
) -> Iterator[tuple[int, str, Path, Callable[..., None]]]:
    """Open dest.parent component-by-component with ``O_DIRECTORY|O_NOFOLLOW``.

    One helper used by every surface writer and the ledger rewrite (REF-26).
    Starts from the already-pinned root descriptor rather than re-resolving
    the caller path. Missing parents are created as real directories when
    ``create=True``. Yields ``(parent_fd, dest_name, dest, validate)``.
    """
    resolved, parts, name = _surface_parent_components(pinned.path, dest_rel)
    dest = resolved.joinpath(*parts, name)
    _assert_beneath(resolved, dest)
    fds: list[int] = []
    try:
        fds.append(os.dup(pinned.fd))
        for part in parts:
            if create:
                try:
                    os.mkdir(part, dir_fd=fds[-1])
                except FileExistsError:
                    pass
            try:
                fds.append(
                    os.open(
                        part,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=fds[-1],
                    )
                )
            except OSError as exc:
                raise SurfacePathRefusalError(f"refusing non-directory or symlink parent for {dest_rel}") from exc

        parent_fd = fds[-1]

        def validate(*, require_dest: bool = False) -> None:
            opened_root = os.fstat(fds[0])
            if (opened_root.st_dev, opened_root.st_ino) != pinned.identity:
                raise SurfacePathRefusalError(f"refusing swapped consumer root for {dest_rel}")
            for index, part in enumerate(parts):
                try:
                    current = os.stat(part, dir_fd=fds[index], follow_symlinks=False)
                except OSError as exc:
                    raise SurfacePathRefusalError(f"refusing changed parent for {dest_rel}") from exc
                opened = os.fstat(fds[index + 1])
                if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                    raise SurfacePathRefusalError(f"refusing swapped parent for {dest_rel}")
                if stat.S_ISLNK(current.st_mode) or not stat.S_ISDIR(current.st_mode):
                    raise SurfacePathRefusalError(f"refusing non-directory or symlink parent for {dest_rel}")
            try:
                info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError as exc:
                if require_dest:
                    raise SurfacePathRefusalError(f"refusing missing destination after replace: {dest}") from exc
                return
            if stat.S_ISLNK(info.st_mode):
                raise SurfacePathRefusalError(f"refusing symlink destination: {dest}")
            if not stat.S_ISREG(info.st_mode):
                raise SurfacePathRefusalError(f"refusing non-regular destination: {dest}")

        validate()
        yield parent_fd, name, dest, validate
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _copy_regular_file_from_dirfd(parent_fd: int, name: str, dest: Path) -> None:
    """Copy dest into isolated staging without following a dest symlink."""
    try:
        infd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise SurfacePathRefusalError(f"refusing destination {name}") from exc
    try:
        info = os.fstat(infd)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise SurfacePathRefusalError(f"refusing destination {name}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(infd, "rb") as inf:
            infd = -1
            dest.write_bytes(inf.read())
    finally:
        if infd >= 0:
            os.close(infd)


def _commit_pinned_destination_bytes(
    pinned: _PinnedRoot,
    parent_fd: int,
    dest_name: str,
    dest: Path,
    dest_rel: str,
    validate: Callable[..., None],
    payload: bytes,
    *,
    mark_published: bool,
) -> None:
    """Stage ``payload`` in the pinned parent and ``os.replace`` it no-follow.

    Shared by MCP surface publication and Codex activation writes (REF-26):
    one parent opened ``O_DIRECTORY|O_NOFOLLOW``, dest must be a regular
    non-symlink file (or absent), atomic same-dir replace, identity
    re-check. MCP callers pass ``mark_published=True`` so a later ledger
    rewrite owns the inode; activation callers leave publication unmarked.
    """
    tmp_name = f".{dest_name}.{uuid.uuid4().hex}.tmp"
    tmp_created = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        out_fd = os.open(tmp_name, flags, 0o600, dir_fd=parent_fd)
        tmp_created = True
        out = os.fdopen(out_fd, "wb")
        try:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())
            staged = os.fstat(out.fileno())
            staged_identity = (staged.st_dev, staged.st_ino)
        finally:
            out.close()
        validate()
        _assert_final_pinned_root(pinned)
        os.replace(tmp_name, dest_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        tmp_created = False
        _assert_replaced_destination_identity(parent_fd, dest_name, staged_identity, dest)
        validate(require_dest=True)
        _assert_final_pinned_root(pinned)
        if mark_published:
            _before_publish_mark(dest)
            _assert_replaced_destination_identity(parent_fd, dest_name, staged_identity, dest)
            pinned.published_identities[dest_rel] = staged_identity
            pinned.published = True
        _fsync_directory(parent_fd)
    finally:
        if tmp_created:
            try:
                os.unlink(tmp_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass


def _replace_contained_regular_file(target: Path, dest_rel: str, payload: bytes) -> bool:
    """Atomically replace ``dest_rel`` under ``target`` without following links.

    Pins the consumer root ``O_DIRECTORY|O_NOFOLLOW``, opens every parent
    component no-follow, and commits through :func:`_commit_pinned_destination_bytes`.
    A dest/parent/root symlink is ``SurfacePathRefusalError`` (WEB-13).
    Returns whether a regular destination existed before the replace.
    """
    with _pin_consumer_root(target) as pinned:
        _assert_contained_surface_path(pinned.path, dest_rel)
        _assert_pinned_root(pinned)
        with _pin_contained_parent(pinned, dest_rel, create=True) as contained:
            parent_fd, dest_name, dest, validate = contained
            try:
                os.stat(dest_name, dir_fd=parent_fd, follow_symlinks=False)
                existed = True
            except FileNotFoundError:
                existed = False
            _assert_final_pinned_root(pinned)
            validate()
            _commit_pinned_destination_bytes(
                pinned,
                parent_fd,
                dest_name,
                dest,
                dest_rel,
                validate,
                payload,
                mark_published=False,
            )
            return existed


def _atomic_write_surface(
    pinned: _PinnedRoot,
    name: str,
    mcp_servers: Mapping[str, Mapping[str, Any]],
    *,
    prune_names: tuple[str, ...] = (),
) -> None:
    """Persist one surface via a pinned parent fd + ``os.replace``.

    HARM3FIX02RV-001: install writers write the destination in place. If
    they persist bytes and then raise, ``_evaluate_surface`` never
    returns an action, ``written`` stays false, and the exception path
    skips ``_rewrite_ledger_mcp_servers`` — file-new, ledger-old
    (DATA-14). Atomic replace was chosen over persist-ledger-first
    because a raise then leaves the previous bytes, so the ledger stays
    the owner record without a dual-write order.

    HARM3FIX04RV-001: staging and replace are relative to a parent
    opened ``O_DIRECTORY|O_NOFOLLOW`` component-by-component so a
    symlinked or swapped ``.vscode`` / ``.codex`` / ``.cursor`` cannot
    carry bytes outside the consumer (WEB-13, CON-11).

    HARM3FIX05RV-002: publication is marked immediately after replace
    succeeds and the post-replace identity check passes, so a later
    dir-fsync failure still rewrites the ledger. HARM3FIX05RV-003: a
    parent or destination swap inside ``os.replace`` is
    ``invalid_surface_path``, never ``created``. HARM3FIX06R2RV-001:
    the caller-visible root must still name the pinned inode at the
    replace boundary. HARM3FIX06R2RV-002: the destination inode after
    replace must be the staged file.
    """
    rel = _SURFACE_PATHS[name]
    _assert_pinned_root(pinned)
    with _pin_contained_parent(pinned, rel, create=True) as contained:
        parent_fd, dest_name, dest, validate = contained
        staging_dir = tempfile.mkdtemp(prefix=f".{dest_name}.")
        staging_root = Path(staging_dir)
        staging_dest = staging_root.joinpath(*Path(rel).parts)
        try:
            staging_dest.parent.mkdir(parents=True, exist_ok=True)
            _copy_regular_file_from_dirfd(parent_fd, dest_name, staging_dest)
            _WRITERS[name](staging_root, mcp_servers, prune_names=prune_names)
            rendered = staging_dest.read_bytes()
            _before_atomic_stage(dest)
            _assert_final_pinned_root(pinned)
            validate()
            _commit_pinned_destination_bytes(
                pinned,
                parent_fd,
                dest_name,
                dest,
                rel,
                validate,
                rendered,
                mark_published=True,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)


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


def _read_ledger_mcp_servers(pinned: _PinnedRoot) -> tuple[str, ...]:
    """Return the ledger's previously-managed names, or ``()`` when the
    ledger is missing or the block is absent / empty.

    The empty result drives the legacy-fallback path in
    ``sync_mcp_configs(prune_removed_managed=True)``: the first run is a
    prune no-op for that target; the rewrite step seeds the block from
    the resolved map so the next run has provenance. Reads through the
    pinned root descriptor so a swapped ``--target`` cannot redirect
    provenance (HARM3FIX05RV-001).
    """
    try:
        infd = os.open(
            BOOTSTRAP_MANIFEST_NAME,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=pinned.fd,
        )
    except FileNotFoundError:
        return ()
    except OSError:
        return ()
    try:
        with os.fdopen(infd, "r") as inf:
            infd = -1
            try:
                payload = json.loads(inf.read())
            except json.JSONDecodeError:
                return ()
    finally:
        if infd >= 0:
            os.close(infd)
    if not isinstance(payload, dict):
        return ()
    block = payload.get("mcp_servers")
    if not isinstance(block, list):
        return ()
    return tuple(name for name in block if isinstance(name, str))


def _rewrite_ledger_mcp_servers(pinned: _PinnedRoot, names: list[str]) -> tuple[str, ...]:
    """Rewrite the ledger's ``mcp_servers`` block to ``names``.

    No-op when the ledger does not exist — ``sync_mcp_configs`` is a
    config-only refresh path and does not synthesize ledgers for
    targets that were never bootstrapped. Writes through the pinned
    root descriptor so a swapped ``--target`` cannot rewrite an
    outside ledger (HARM3FIX05RV-001).
    """
    _assert_ledger_identity(pinned)
    with _pin_contained_parent(pinned, BOOTSTRAP_MANIFEST_NAME, create=False) as contained:
        parent_fd, dest_name, _dest, _validate = contained
        try:
            infd = os.open(dest_name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
        except FileNotFoundError:
            return tuple(names)
        except OSError as exc:
            raise SurfacePathRefusalError(f"refusing ledger destination {dest_name}") from exc
        try:
            with os.fdopen(infd, "r") as inf:
                infd = -1
                payload = json.loads(inf.read())
        finally:
            if infd >= 0:
                os.close(infd)
        payload["mcp_servers"] = list(names)
        text = json.dumps(payload, indent=2) + "\n"
        tmp_name = f".{dest_name}.{uuid.uuid4().hex}.tmp"
        tmp_created = False
        try:
            out_fd = os.open(
                tmp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=parent_fd,
            )
            tmp_created = True
            out = os.fdopen(out_fd, "w")
            try:
                out.write(text)
                out.flush()
                os.fsync(out.fileno())
            finally:
                out.close()
            os.replace(tmp_name, dest_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            tmp_created = False
            _fsync_directory(parent_fd)
        finally:
            if tmp_created:
                try:
                    os.unlink(tmp_name, dir_fd=parent_fd)
                except FileNotFoundError:
                    pass
        return tuple(names)
