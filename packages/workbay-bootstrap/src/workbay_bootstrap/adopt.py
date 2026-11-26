"""Adopt the bootstrap overlay into a linked git worktree (implementation note, Slice S1).

A linked worktree shares the primary's ``.git`` but not gitignored files, so the
bootstrap overlay is absent until it is adopted. ``adopt_worktree`` re-runs the
*existing* install materialization passes against the worktree, but with
the primary's clone or the installed ``workbay-system`` package payload as the
source — reusing the battle-tested carve logic,
foreign-file precedence, relative links to the source, and the lifecycle
real-file hoist.

Isolation is safe-by-construction rather than via a runtime allow-list (decision
``adopt-drop-runtime-allowlist-for-materializer-scope-invariant``):

* The materializer only ever touches the recorded shared/generated/lifecycle
  surfaces + the clone — the per-worktree mutable set (``.task-state``,
  ``DASHBOARD.txt``, ``CURRENT_TASK.json``, ``state-backups`` …) is never a
  surface, so it is never adopted.
* ``.workbay/`` is kept a real *local* directory; only its ``remote`` and
  ``generated`` children are symlinked to the primary, so sibling per-worktree
  state under ``.workbay/`` stays local.
* ``_run_init_state`` / MCP presync are intentionally NOT run — the worktree uses
  the primary-rooted MCP server; adopt never fabricates a second ``.task-state``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

# ``workbay_bootstrap.__init__`` re-exports the install *function*, which
# shadows the submodule only under *attribute* access (``import ... as`` and
# ``getattr(pkg, "install")`` both yield the function). A direct
# ``from workbay_bootstrap.install import ...`` submodule import is
# unaffected — no importlib indirection needed. adopt reuses the install
# materializer's surface enumeration + apply passes (shared so apply and
# ``--check`` cannot desync); the private names below are install internals it
# orchestrates.
from workbay_bootstrap.activation import (
    AGY_PLUGINS_POINTER_PATH,
    agy_plugin_surface_problems,
    grok_plugin_surface_problems,
    materialize_grok_plugin_symlink,
    write_plugin_activation,
)
from workbay_bootstrap.harnesses import (
    CURSOR_HOOKS_PATH,
    PLUGIN_NAME,
    cursor_native_surface_problems,
    cursor_root_surface_dests,
)
from workbay_bootstrap.install import (
    GROK_PLUGIN_DEST,
    HOOKS_PATH_VALUE,
    LEGACY_LIFECYCLE_INCLUDE_SENTINEL_BEGIN,
    LIFECYCLE_HOISTS,
    LIFECYCLE_INCLUDE_SENTINEL_BEGIN,
    RUNTIME_ROOT_DIRNAME,
    InstallPreflightError,
    _ensure_consumer_gitignore_block,
    _ensure_consumer_makefile_include,
    _git,
    _install_lifecycle_profile,
    _is_repointable_bootstrap_symlink,
    _leaking_overlay_entries,
    _materialize_cursor_plugin,
    _materialize_surfaces,
    _manifest_source_kind,
    _package_source_root,
    _plugin_tree_out,
    _preflight_package_source,
    _prepare_generated_surfaces,
    _raw_symlink_target_path,
    _resolve_in_clone,
    _set_git_hooks_path,
    iter_expected_surface_targets,
)
from workbay_bootstrap.worktree import (
    WorktreeError,
    is_linked_worktree,
    overlay_is_materialized,
    primary_overlay_root,
)

_RUNTIME = RUNTIME_ROOT_DIRNAME  # ".workbay"
_WORKBAY_CHILDREN = ("remote", "generated")


class OverlayNotMaterializedError(WorktreeError):
    """The resolved primary has no materialized overlay to adopt from."""


class PackageSourceError(WorktreeError):
    """The installed workbay-system package cannot safely supply the overlay."""


def _materialize_workbay_child(target: Path, primary: Path, child: str) -> bool:
    """Symlink ``target/.workbay/<child>`` -> ``primary/.workbay/<child>``.

    ``.workbay/`` itself stays a real local directory (created if absent) so
    sibling per-worktree state does not leak across worktrees. The child link is
    relative for relocation safety. Foreign/real local content at the child path
    is left untouched (overlay precedence). Returns True when the source exists.
    """
    src = primary / _RUNTIME / child
    if not src.exists():
        return False
    ws = target / _RUNTIME
    if ws.is_symlink():
        # A pre-existing .workbay SYMLINK would route the remote/generated
        # child links THROUGH it (into the primary or a foreign dir) — exactly
        # the cross-worktree contamination the isolation invariant forbids.
        # Replace it with a real local directory before materializing children.
        ws.unlink()
    ws.mkdir(parents=True, exist_ok=True)
    dest = ws / child
    rel = os.path.relpath(src, ws)
    if dest.is_symlink():
        if dest.resolve(strict=False) == src.resolve():
            return True
        dest.unlink()
    elif dest.exists():
        return True  # foreign/real local content wins
    dest.symlink_to(rel, target_is_directory=src.is_dir())
    return True


def _primary_has_effective_agy(primary: Path) -> bool:
    return (_plugin_tree_out(primary, "effective") / "agy").is_dir()


def _ensure_relative_symlink(dest: Path, src: Path) -> None:
    if dest.is_symlink():
        if dest.resolve(strict=False) == src.resolve():
            return
        dest.unlink()
    elif dest.exists():
        return  # foreign/real local content wins
    rel = os.path.relpath(src, dest.parent)
    dest.symlink_to(rel, target_is_directory=src.is_dir())


def _tree_snapshot(root: Path, *, allow_symlinks: bool) -> dict[str, bytes] | None:
    """Return relative-path -> bytes map for ``root``, or None if invalid/symlink."""
    if not root.is_dir() or (not allow_symlinks and root.is_symlink()):
        return None
    files: dict[str, bytes] = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=allow_symlinks):
        cur = Path(dirpath)
        if not allow_symlinks:
            for dname in dirnames:
                if (cur / dname).is_symlink():
                    return None
        for fname in filenames:
            fpath = cur / fname
            if not allow_symlinks and fpath.is_symlink():
                return None
            rel = fpath.relative_to(root).as_posix()
            try:
                files[rel] = fpath.read_bytes()
            except OSError:
                return None
    return files


def _agy_trees_match(src: Path, dest: Path) -> bool:
    src_snap = _tree_snapshot(src, allow_symlinks=True)
    dest_snap = _tree_snapshot(dest, allow_symlinks=False)
    return src_snap is not None and dest_snap is not None and src_snap == dest_snap


def _materialize_generated_with_local_agy(target: Path, primary: Path) -> bool:
    """Materialize ``.workbay/generated`` with a real in-worktree ``effective/agy`` copy.

    Live ``agy -p`` refuses plugin trees reached through symlinks escaping the
    worktree root (S1b). Keep ``base/`` and sibling ``effective/{claude,codex,
    cursor,grok}`` symlinked to primary while copying ``effective/agy`` into a
    real directory contained inside ``target``.
    """
    primary_gen = primary / _RUNTIME / "generated"
    if not primary_gen.exists():
        return False
    ws = target / _RUNTIME
    if ws.is_symlink():
        ws.unlink()
    ws.mkdir(parents=True, exist_ok=True)

    spine: tuple[tuple[Path, Path, str], ...] = (
        (ws / "generated", primary_gen, "plugins"),
        (ws / "generated" / "plugins", primary_gen / "plugins", PLUGIN_NAME),
        (
            ws / "generated" / "plugins" / PLUGIN_NAME,
            primary_gen / "plugins" / PLUGIN_NAME,
            "effective",
        ),
        (
            ws / "generated" / "plugins" / PLUGIN_NAME / "effective",
            primary_gen / "plugins" / PLUGIN_NAME / "effective",
            "agy",
        ),
    )
    for t_dir, p_dir, keep_local in spine:
        if t_dir.is_symlink():
            t_dir.unlink()
        t_dir.mkdir(parents=True, exist_ok=True)
        if p_dir.is_dir():
            for child in p_dir.iterdir():
                if child.name == keep_local:
                    continue
                _ensure_relative_symlink(t_dir / child.name, child)

    src_agy = _plugin_tree_out(primary, "effective") / "agy"
    dest_agy = _plugin_tree_out(target, "effective") / "agy"
    if dest_agy.is_symlink():
        dest_agy.unlink()
    if dest_agy.is_dir() and _agy_trees_match(src_agy, dest_agy):
        return True
    if dest_agy.exists():
        if dest_agy.is_dir():
            shutil.rmtree(dest_agy)
        else:
            dest_agy.unlink()
    shutil.copytree(src_agy, dest_agy, symlinks=False)
    return True


def _generated_agy_tree_drifts(target: Path, primary: Path) -> bool:
    """Return True iff the local-copy ``effective/agy`` layout or siblings drifted."""
    ws = target / _RUNTIME
    if ws.is_symlink() or not ws.is_dir():
        return True
    primary_gen = primary / _RUNTIME / "generated"
    spine: tuple[tuple[Path, Path, str], ...] = (
        (ws / "generated", primary_gen, "plugins"),
        (ws / "generated" / "plugins", primary_gen / "plugins", PLUGIN_NAME),
        (
            ws / "generated" / "plugins" / PLUGIN_NAME,
            primary_gen / "plugins" / PLUGIN_NAME,
            "effective",
        ),
        (
            ws / "generated" / "plugins" / PLUGIN_NAME / "effective",
            primary_gen / "plugins" / PLUGIN_NAME / "effective",
            "agy",
        ),
    )
    for t_dir, p_dir, keep_local in spine:
        if t_dir.is_symlink() or not t_dir.is_dir():
            return True
        if p_dir.is_dir():
            for child in p_dir.iterdir():
                if child.name == keep_local:
                    continue
                t_child = t_dir / child.name
                if t_child.is_symlink():
                    if t_child.resolve(strict=False) != child.resolve():
                        return True
                elif not t_child.exists():
                    return True

    src_agy = _plugin_tree_out(primary, "effective") / "agy"
    dest_agy = _plugin_tree_out(target, "effective") / "agy"
    if dest_agy.is_symlink() or not dest_agy.is_dir():
        return True
    try:
        dest_agy.resolve().relative_to(target.resolve())
    except (OSError, ValueError):
        return True
    return not _agy_trees_match(src_agy, dest_agy)


def _hooks_path_value(target: Path) -> str:
    try:
        return _git("config", "core.hooksPath", cwd=target)
    except subprocess.CalledProcessError:
        return ""


def _materialize_cursor_mcp_config(target: Path, primary: Path) -> dict[str, str] | None:
    source = primary / ".cursor" / "mcp.json"
    if not source.is_file():
        return None
    dest = target / ".cursor" / "mcp.json"
    existed = dest.exists() or dest.is_symlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_symlink() or dest.is_file():
        dest.unlink()
    elif dest.exists():
        return {
            "path": ".cursor/mcp.json",
            "action": "skipped_foreign_content",
            "kind": "cursor_mcp_config",
        }
    dest.write_bytes(source.read_bytes())
    return {
        "path": ".cursor/mcp.json",
        "action": "updated" if existed else "created",
        "kind": "cursor_mcp_config",
    }


def _link_drifts(
    target_path: Path,
    expected_src: Path,
    clone_resolved: Path,
    remote_subtree_prefix: str,
) -> bool:
    """True iff a bootstrap-owned surface link is missing or stale.

    Shares the repoint rule with apply via
    ``install._is_repointable_bootstrap_symlink`` so ``--check`` and apply cannot
    disagree: a correct symlink (resolves to ``expected_src``) is clean; a
    repointable bootstrap-owned link (stale in-tree pointer, or a dangling link
    naming a relocated ``.workbay/remote`` clone) is drift; a foreign symlink
    (resolving, or dangling but not naming our clone) and real local content keep
    local precedence; an absent target is drift.
    """
    if target_path.is_symlink():
        if target_path.resolve(strict=False) == expected_src.resolve():
            return False
        raw = _raw_symlink_target_path(target_path)
        return _is_repointable_bootstrap_symlink(
            target_path, raw, clone_resolved, remote_subtree_prefix
        )
    if target_path.exists():
        return False  # real local content — overlay precedence
    return True  # absent


def _compute_drift(target: Path, primary: Path, clone: Path) -> list[str]:
    """Report what an adopt would materialize, without writing anything.

    Shares the materializer's exclusion sets and foreign-precedence rule so the
    guard cannot desync from apply: carved-surface children are checked
    individually, and foreign/local content is never reported as drift.
    """
    drift: list[str] = []
    clone_resolved = clone.resolve()
    remote_subtree_prefix = str(clone_resolved) + os.sep

    # Clone + generated child redirects.
    for child in _WORKBAY_CHILDREN:
        src = primary / _RUNTIME / child
        if not src.exists():
            continue
        if child == "generated" and _primary_has_effective_agy(primary):
            if _generated_agy_tree_drifts(target, primary):
                drift.append(f"{_RUNTIME}/generated/plugins/{PLUGIN_NAME}/effective/agy")
            continue
        dest = target / _RUNTIME / child
        if not (dest.is_symlink() and dest.resolve(strict=False) == src.resolve()):
            drift.append(f"{_RUNTIME}/{child}")

    # Shared surfaces: enumerated by the SAME helper the materializer uses
    # (install.iter_expected_surface_targets), so the guard cannot desync from
    # apply. Each expected target — a whole-dir plain surface or a carved
    # per-child link — drifts iff it is missing or a stale bootstrap-owned link
    # (foreign/local content is never drift; see _link_drifts).
    for expected in iter_expected_surface_targets(target, clone):
        if _link_drifts(
            expected.target_path,
            expected.remote_path,
            clone_resolved,
            remote_subtree_prefix,
        ):
            drift.append(expected.rel)

    # Lifecycle hoists: expect a real file/dir at the destination.
    for src_rel, dest_rel in LIFECYCLE_HOISTS:
        if not _resolve_in_clone(clone, src_rel).exists():
            continue
        if not (target / dest_rel).exists():
            drift.append(dest_rel)

    # Consumer Makefile include sentinel.
    makefile = target / "Makefile"
    text = makefile.read_text() if makefile.exists() else ""
    if not (
        LIFECYCLE_INCLUDE_SENTINEL_BEGIN in text
        or LEGACY_LIFECYCLE_INCLUDE_SENTINEL_BEGIN in text
    ):
        drift.append("Makefile")

    # Managed overlay-ignore block (keeps git status clean). apply writes and
    # reconciles it, so --check must verify coverage too, else a missing OR
    # stale block silently regresses. Any leaking managed entry is drift —
    # whether the sentinel block is absent entirely or present but predating a
    # newer managed surface (apply repairs both: appended / updated). A
    # self-hosting source repo manages the surfaces itself (tracked or already
    # ignored): nothing leaks there, so no drift is reported.
    if _leaking_overlay_entries(target):
        drift.append(".gitignore")

    # core.hooksPath (resolved against this worktree's config).
    if _hooks_path_value(target) != HOOKS_PATH_VALUE:
        drift.append("core.hooksPath")

    effective_grok = _plugin_tree_out(target, "effective") / "grok"
    if effective_grok.is_dir() and grok_plugin_surface_problems(target):
        drift.append(GROK_PLUGIN_DEST.as_posix())

    effective_cursor = _plugin_tree_out(target, "effective") / "cursor"
    if effective_cursor.is_dir() and cursor_native_surface_problems(target):
        drift.extend(
            path.as_posix()
            for path in (*cursor_root_surface_dests(target), CURSOR_HOOKS_PATH)
            if not (target / path).exists()
        )
        if not (target / ".cursor" / "mcp.json").is_file():
            drift.append(".cursor/mcp.json")

    effective_agy = _plugin_tree_out(target, "effective") / "agy"
    if (
        (target / AGY_PLUGINS_POINTER_PATH).is_file()
        and (effective_agy.exists() or effective_agy.is_symlink())
        and agy_plugin_surface_problems(target)
    ):
        agy_rel = f"{_RUNTIME}/generated/plugins/{PLUGIN_NAME}/effective/agy"
        if agy_rel not in drift:
            drift.append(agy_rel)

    return drift


def adopt_worktree(
    *, target: Path, primary: Path | None = None, check: bool = False
) -> dict[str, object]:
    """Adopt (or check) the bootstrap overlay into a linked worktree.

    Args:
        target: The linked worktree to adopt. Adoption is a no-op when ``target``
            is the primary worktree (or otherwise not a linked worktree).
        primary: The primary overlay root. Resolved from ``target`` by marker when
            omitted (:func:`primary_overlay_root`).
        check: When True, report drift (``ok``/``drift``) without writing.

    Returns:
        A receipt dict with ``adopted``, ``check``, ``ok``, ``drift``, ``reason``,
        ``target``, ``primary``, and (on apply) ``surfaces``.

    Raises:
        OverlayNotMaterializedError: the resolved primary has no overlay to adopt.
        PackageSourceError: the primary uses an unavailable or unsafe package source.
        OverlayMarkerNotFoundError / NotAGitRepositoryError: from resolution.
    """
    target = Path(target).resolve()

    if not is_linked_worktree(target):
        return {
            "adopted": False,
            "check": check,
            "ok": True,
            "drift": [],
            "reason": "not_a_linked_worktree",
            "target": str(target),
            "primary": None,
            "surfaces": [],
        }

    primary = (
        primary_overlay_root(target) if primary is None else Path(primary).resolve()
    )
    if _manifest_source_kind(primary) == "package":
        try:
            _preflight_package_source(primary, "package", None)
            clone = _package_source_root(None)
            # Materializers skip absent surfaces for older clone refs. A missing
            # or empty package must instead refuse before any target writes.
            if not clone.is_dir() or not iter_expected_surface_targets(primary, clone):
                raise PackageSourceError(
                    f"cannot adopt from the workbay-system package at {clone}: "
                    "no shared overlay surfaces found; reinstall workbay-system"
                )
        except (InstallPreflightError, OSError) as exc:
            raise PackageSourceError(
                f"cannot adopt from the workbay-system package: {exc}"
            ) from exc
    else:
        if not overlay_is_materialized(primary):
            raise OverlayNotMaterializedError(
                f"primary overlay is not materialized at {primary}; "
                f"run `workbay-bootstrap install` (or repair) there first"
            )
        clone = primary / _RUNTIME / "remote"

    if check:
        drift = _compute_drift(target, primary, clone)
        return {
            "adopted": False,
            "check": True,
            "ok": not drift,
            "drift": drift,
            "reason": None,
            "target": str(target),
            "primary": str(primary),
            "surfaces": [],
        }

    # Apply — order mirrors install() under profile=all + lifecycle.
    for child in _WORKBAY_CHILDREN:
        if child == "generated" and _primary_has_effective_agy(primary):
            _materialize_generated_with_local_agy(target, primary)
        else:
            _materialize_workbay_child(target, primary, child)

    surfaces: list[dict[str, str]] = []
    surfaces.extend(_materialize_surfaces(target, clone))
    surfaces.extend(_prepare_generated_surfaces(target, clone))
    surfaces.extend(_install_lifecycle_profile(target, clone))
    configs: list[dict[str, str]] = []
    effective_grok = _plugin_tree_out(target, "effective") / "grok"
    if effective_grok.is_dir():
        grok_surface, grok_config = materialize_grok_plugin_symlink(target)
        surfaces.append(grok_surface)
        configs.append(grok_config)
        configs.append(write_plugin_activation("grok", target, clone=clone))

    effective_cursor = _plugin_tree_out(target, "effective") / "cursor"
    if effective_cursor.is_dir():
        cursor_surfaces, cursor_config = _materialize_cursor_plugin(target)
        surfaces.extend(cursor_surfaces)
        configs.append(cursor_config)
        cursor_mcp = _materialize_cursor_mcp_config(target, primary)
        if cursor_mcp is not None:
            configs.append(cursor_mcp)
        configs.append(write_plugin_activation("cursor", target, clone=clone))
    makefile_include = _ensure_consumer_makefile_include(target)
    # _set_git_hooks_path is worktree-aware: for a linked worktree (.git is a
    # file) it writes core.hooksPath with --worktree, never touching the primary.
    hooks = _set_git_hooks_path(target)
    # implementation note S4: ensure the overlay-ignore block exists so the adopted
    # worktree's `git status` is clean even when the primary's tracked
    # .gitignore predates the managed block (idempotent / already_present).
    gitignore = _ensure_consumer_gitignore_block(target)

    return {
        "adopted": True,
        "check": False,
        "ok": True,
        "drift": [],
        "reason": None,
        "target": str(target),
        "primary": str(primary),
        "clone": str(clone),
        "surfaces": surfaces,
        "configs": configs,
        "makefile_include": makefile_include,
        "hooks": hooks,
        "gitignore": gitignore,
    }
