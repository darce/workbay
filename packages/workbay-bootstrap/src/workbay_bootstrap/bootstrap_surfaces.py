"""implementation note — self-host linked-worktree agent-surface bootstrap.

Emits the gitignored generated agent surfaces — base + effective plugin trees,
root ``.github/prompts``, and Cursor/Grok native wiring — **and** the gitignored
``install.SHARED_SURFACES`` symlink set (``docs/workbay/rules`` cited by the
branch-review skill, ``.github/hooks``, ``Makefile.d``, ``scripts/workbay``,
``.codex/hooks.json``) — directly inside a self-host linked worktree. This heals
the ``no_overlay_clone`` starvation: the workbay monorepo ships a tracked in-tree
payload instead of a ``.workbay/remote`` clone, so consumer ``adopt-worktree`` is
skipped and a linked worktree inherits only tracked files (no effective plugin
tree, no prompts, no ``.cursor``, and none of the shared doc/hook symlinks).

The emission reuses the install + adopt orchestration anchors rather than
re-deriving the generator argv or shelling out to ``make``:

* :func:`install._run_generator` already runs the legacy ``--target`` prompts
  pass **then** both plugin passes (base, then effective with overrides or a
  passthrough lock) — the exact argv ``Makefile.d/plugins.mk`` replicates.
* The Cursor/Grok block mirrors :func:`adopt.adopt_worktree`, but against the
  locally-emitted effective tree (``clone`` is the in-tree payload root, used by
  ``write_plugin_activation`` for the harness-protocol contract lookup).

Best-effort and idempotent: re-running it on an already-bootstrapped worktree
re-emits the same surfaces without error.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from workbay_bootstrap.activation import write_plugin_activation
from workbay_bootstrap.adopt import _materialize_cursor_mcp_config
from workbay_bootstrap.harnesses import (
    materialize_cursor_plugin,
    materialize_grok_plugin_symlink,
    plugin_tree_out,
)
from workbay_bootstrap.install import (
    CLONE_SUBDIR,
    PLUGIN_OVERRIDE_MANIFEST,
    PLUGIN_OVERRIDE_ROOT,
    _materialize_surfaces,
    _run_generator,
    self_host_payload_root,
)
from workbay_bootstrap.worktree import WorktreeError, primary_overlay_root


# Syntactically-valid 40-char placeholder base SHA. The generator's effective
# plugin-lock render rejects any value that is not exactly 40 lowercase hex
# chars, so a HEAD-less or git-unavailable worktree must substitute this rather
# than forward an empty string (which would crash the generator subprocess).
_ZERO_BASE_SHA = "0" * 40


def _git_head_sha(target: Path) -> str:
    """``git rev-parse HEAD`` for ``target`` — the self-host equivalent of
    ``PLUGINS_BASE_SHA`` (``plugins.mk:44``). Empty when the target has no commit
    or git is unavailable; callers must substitute :data:`_ZERO_BASE_SHA` for an
    empty result before passing it to the generator (which requires a 40-char
    hex base SHA)."""
    try:
        return subprocess.run(
            ["git", "-C", str(target), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return ""


def _override_root(target: Path) -> Path | None:
    """The consumer plugin override root when it carries an ``overrides.yaml``,
    else ``None`` — mirrors ``PLUGINS_EFFECTIVE_ARGS`` (``plugins.mk:45``): with
    overrides the effective pass composes base + overrides, without one it emits
    base unchanged plus a passthrough ``plugin-lock.json``."""
    candidate = target.joinpath(*PLUGIN_OVERRIDE_ROOT)
    if (candidate / PLUGIN_OVERRIDE_MANIFEST).is_file():
        return candidate
    return None


def bootstrap_surfaces(
    *, target: Path, primary: Path | None = None
) -> dict[str, object]:
    """Emit generated agent surfaces locally inside a self-host worktree.

    Args:
        target: The worktree to bootstrap. Must ship the in-tree payload
            (``self-host``); otherwise the command refuses with
            ``skipped='not_self_host'`` rather than silently emitting nothing.
        primary: Source of the materialized ``.cursor/mcp.json`` to copy in.
            Resolved by the ``.workbay-bootstrap.json`` marker when omitted,
            falling back to ``target`` itself.

    Returns:
        A receipt dict with ``ok``, ``ran``, ``skipped``, ``target``,
        ``primary``, ``payload_root``, ``steps``, ``surfaces`` and ``configs``.
    """
    target = Path(target).resolve()
    payload_root = self_host_payload_root(target)
    if payload_root is None:
        return {
            "ok": False,
            "ran": False,
            "skipped": "not_self_host",
            "target": str(target),
            "primary": None,
            "payload_root": None,
            "steps": [],
            "surfaces": [],
            "configs": [],
        }

    if primary is None:
        try:
            primary = primary_overlay_root(target)
        except WorktreeError:
            primary = target
    else:
        primary = Path(primary).resolve()

    steps: list[dict[str, object]] = []
    surfaces: list[dict[str, str]] = []
    configs: list[dict[str, str]] = []

    # Best-effort, never fatal: a generator non-zero exit, a timeout, or a
    # missing interpreter must degrade to an ok=False receipt (the worktree
    # stays healable via doctor --apply / a later task-start) rather than
    # tracebacking out into the CLI or the task-start runner.
    try:
        # Scope addendum (internal) — shared doc/hook surfaces.
        # The generated passes below emit plugins/prompts/cursor only; the
        # gitignored install.SHARED_SURFACES symlink set — docs/workbay/rules
        # (cited by the branch-review skill), .github/hooks, Makefile.d,
        # scripts/workbay, .codex/hooks.json — is materialized by install/adopt
        # on the PRIMARY but never in a self-host worktree, so skill policy-doc
        # links dangle there. Reuse the install SSOT
        # (iter_expected_surface_targets, which mounts each surface from the
        # in-tree payload on self-host) so apply and the drift guard cannot
        # desync. The clone arg mirrors adopt.adopt_worktree's
        # target/.workbay/remote — absent on self-host; the live-payload mount
        # overrides it, and the foreign-precedence rules leave tracked surfaces
        # (docs/workbay/contracts, scripts/hooks) untouched. Done first so the
        # skill-critical docs land even if a later generated pass degrades.
        surfaces.extend(
            _materialize_surfaces(target, target.joinpath(*CLONE_SUBDIR))
        )
        steps.append({"step": "shared_surfaces", "ok": True})

        # Prong 1 steps 1+2 — prompt gen + base + effective plugin emission.
        override_root = _override_root(target)
        remote_sha = _git_head_sha(target) or _ZERO_BASE_SHA
        _run_generator(target, payload_root, remote_sha, override_root)
        steps.append({"step": "generate", "ok": True})

        # Prong 1 step 3 — Cursor + Grok native materialization. Gated on the
        # locally-emitted effective tree existing, mirroring adopt_worktree.
        effective_grok = plugin_tree_out(target, "effective") / "grok"
        if effective_grok.is_dir():
            grok_surface, grok_config = materialize_grok_plugin_symlink(target)
            surfaces.append(grok_surface)
            configs.append(grok_config)
            configs.append(write_plugin_activation("grok", target, clone=payload_root))
            steps.append({"step": "grok", "ok": True})

        effective_cursor = plugin_tree_out(target, "effective") / "cursor"
        if effective_cursor.is_dir():
            cursor_surfaces, cursor_config = materialize_cursor_plugin(target)
            surfaces.extend(cursor_surfaces)
            configs.append(cursor_config)
            # _materialize_cursor_mcp_config copies primary's mcp.json; when
            # primary IS target (self-host primary, not a worktree) source ==
            # dest and the in-place unlink would corrupt it, so only copy across
            # distinct roots.
            if primary != target:
                cursor_mcp = _materialize_cursor_mcp_config(target, primary)
                if cursor_mcp is not None:
                    configs.append(cursor_mcp)
            configs.append(
                write_plugin_activation("cursor", target, clone=payload_root)
            )
            steps.append({"step": "cursor", "ok": True})
    except (subprocess.SubprocessError, OSError) as exc:
        steps.append(
            {"step": "emit", "ok": False, "error": f"{type(exc).__name__}: {exc}"}
        )
        return {
            "ok": False,
            "ran": True,
            "skipped": "emit_failed",
            "target": str(target),
            "primary": str(primary),
            "payload_root": str(payload_root),
            "steps": steps,
            "surfaces": surfaces,
            "configs": configs,
        }

    return {
        "ok": True,
        "ran": True,
        "skipped": None,
        "target": str(target),
        "primary": str(primary),
        "payload_root": str(payload_root),
        "steps": steps,
        "surfaces": surfaces,
        "configs": configs,
    }
