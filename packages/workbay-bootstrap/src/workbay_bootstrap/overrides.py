"""internal operator surface for consumer plugin overrides.

``overrides_status`` reports each declared override with its composition
status (clean / stale / merge_conflict) from the effective ``plugin-lock.json``
receipt. ``accept_upstream`` re-records one skill override's
``upstream_digest`` against the current base tree (and, for ``patch`` mode,
refreshes the stored ``base_path`` fork copy), writing accept provenance into
the tracked ``overrides.lock.json``.

Neither function recomposes the effective tree; ``workbay-bootstrap repair``
(or ``update``) owns recomposition. ``accept_upstream`` reports that as its
``next_command``.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from workbay_bootstrap.install import (
    PLUGIN_GENERATED_ROOT,
    PLUGIN_OVERRIDE_LOCK,
    PLUGIN_OVERRIDE_MANIFEST,
    _discover_plugin_override_root,
    _render_plugin_override_lock,
)


class OverridesError(RuntimeError):
    """Base class for overrides operator-surface failures."""


class OverrideRootMissingError(OverridesError):
    """No override root with an overrides.yaml manifest was found."""


class UnknownOverrideSkillError(OverridesError):
    """The named skill is not declared in overrides.yaml."""


class BaseTreeMissingError(OverridesError):
    """The generated base plugin tree is absent; install/update must run first."""


class DirtyOverrideRootError(OverridesError):
    """The override root has uncommitted changes and --force was not given."""


def _sha256_digest(content: str) -> str:
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def _require_override_root(target: Path, plugin_overrides: Path | None) -> Path:
    override_root = _discover_plugin_override_root(
        target, plugin_overrides=plugin_overrides
    )
    if override_root is None:
        raise OverrideRootMissingError(
            f"no plugin override root found under {target}; expected "
            f"workbay-overrides/workbay-system/{PLUGIN_OVERRIDE_MANIFEST}"
        )
    return override_root


def _load_override_manifest(override_root: Path) -> dict[str, Any]:
    from pydantic import ValidationError
    from workbay_protocol.bootstrap import PluginOverrideManifest

    manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    payload = yaml.safe_load(manifest_path.read_text()) or {}
    if not isinstance(payload, dict):
        raise OverridesError(f"{manifest_path} must contain a YAML mapping")
    try:
        PluginOverrideManifest.model_validate(payload)
    except ValidationError as exc:
        raise OverridesError(f"{manifest_path}: {exc}") from exc
    return payload


def _effective_lock_entries(target: Path) -> dict[tuple[str, str], dict[str, Any]]:
    lock_path = target.joinpath(*PLUGIN_GENERATED_ROOT, "effective", "plugin-lock.json")
    if not lock_path.is_file():
        return {}
    try:
        payload = json.loads(lock_path.read_text())
    except json.JSONDecodeError:
        return {}
    entries: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in payload.get("components", []):
        if not isinstance(entry, dict):
            continue
        kind = entry.get("component_kind")
        name = entry.get("name")
        if isinstance(kind, str) and isinstance(name, str):
            entries[(kind, name)] = entry
    return entries


def overrides_status(
    *, target: Path, plugin_overrides: Path | None = None
) -> dict[str, Any]:
    """Report every declared override with its current composition status."""
    target = Path(target).resolve()
    override_root = _require_override_root(target, plugin_overrides)
    manifest = _load_override_manifest(override_root)
    lock_entries = _effective_lock_entries(target)

    components: list[dict[str, Any]] = []
    declared = manifest.get("components", {})
    for name, override in sorted((declared.get("skills") or {}).items()):
        lock_entry = lock_entries.get(("skill", name), {})
        components.append(
            {
                "component_kind": "skill",
                "name": name,
                "mode": override.get("mode"),
                "recorded_upstream_digest": override.get("upstream_digest"),
                "current_base_digest": lock_entry.get("current_base_digest"),
                "status": lock_entry.get("status") or "clean",
            }
        )
    for name, override in sorted((declared.get("mcp_servers") or {}).items()):
        lock_entry = lock_entries.get(("mcp_server", name), {})
        components.append(
            {
                "component_kind": "mcp_server",
                "name": name,
                "mode": override.get("mode"),
                "status": lock_entry.get("status") or "clean",
            }
        )
    for name, override in sorted((declared.get("portable_commands") or {}).items()):
        lock_entry = lock_entries.get(("portable_command", name), {})
        components.append(
            {
                "component_kind": "portable_command",
                "name": name,
                "mode": override.get("mode"),
                "status": lock_entry.get("status") or "clean",
            }
        )

    for name, override in sorted((declared.get("rules") or {}).items()):
        lock_entry = lock_entries.get(("rules", name), {})
        components.append(
            {
                "component_kind": "rules",
                "name": name,
                "mode": override.get("mode"),
                "status": lock_entry.get("status") or "clean",
            }
        )
    for name, override in sorted((declared.get("guides") or {}).items()):
        lock_entry = lock_entries.get(("guides", name), {})
        components.append(
            {
                "component_kind": "guides",
                "name": name,
                "mode": override.get("mode"),
                "status": lock_entry.get("status") or "clean",
            }
        )

    try:
        override_root_display = override_root.relative_to(target).as_posix()
    except ValueError:
        override_root_display = str(override_root)
    return {"override_root": override_root_display, "components": components}


def _override_root_is_dirty(target: Path, override_root: Path) -> bool:
    try:
        rel = override_root.relative_to(target).as_posix()
    except ValueError:
        rel = str(override_root)
    from workbay_bootstrap.external import run_external

    proc = run_external(
        ["git", "-C", str(target), "status", "--porcelain", "--", rel],
        call_class="git",
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        # Not a git repo (or git unavailable): treat as clean rather than
        # blocking the operator on a guard we cannot evaluate.
        return False
    return bool(proc.stdout.strip())


def _resolve_base_remote_sha(target: Path, override_root: Path) -> str:
    lock_path = override_root / PLUGIN_OVERRIDE_LOCK
    if lock_path.is_file():
        try:
            payload = json.loads(lock_path.read_text())
        except json.JSONDecodeError:
            payload = {}
        sha = payload.get("base_remote_sha")
        if isinstance(sha, str) and len(sha) == 40:
            return sha
    manifest_path = target / ".workbay-bootstrap.json"
    if manifest_path.is_file():
        try:
            payload = json.loads(manifest_path.read_text())
        except json.JSONDecodeError:
            payload = {}
        sha = payload.get("remote_sha")
        if isinstance(sha, str) and len(sha) == 40:
            return sha
    raise OverridesError(
        "cannot resolve the installed base remote SHA; run "
        "workbay-bootstrap install or update first"
    )


def accept_upstream(
    *,
    target: Path,
    skill: str,
    plugin_overrides: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Re-pin one skill override's recorded upstream digest to the current
    base tree; refresh the ``patch``-mode fork copy; record provenance."""
    target = Path(target).resolve()
    override_root = _require_override_root(target, plugin_overrides)
    manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    manifest = _load_override_manifest(override_root)

    skills = (manifest.get("components") or {}).get("skills") or {}
    if skill not in skills:
        raise UnknownOverrideSkillError(
            f"{manifest_path}: no components.skills.{skill} override declared"
        )
    override = skills[skill]
    mode = override.get("mode")
    if mode not in {"replace", "patch"}:
        raise OverridesError(
            f"accept-upstream only applies to replace/patch overrides; "
            f"components.skills.{skill}.mode is {mode!r}"
        )

    base_skill_path = target.joinpath(
        *PLUGIN_GENERATED_ROOT, "base", "claude", "skills", skill, "SKILL.md"
    )
    if not base_skill_path.is_file():
        raise BaseTreeMissingError(
            f"base plugin tree is missing {base_skill_path}; run "
            "workbay-bootstrap install or update first"
        )

    if not force and _override_root_is_dirty(target, override_root):
        raise DirtyOverrideRootError(
            f"{override_root} has uncommitted changes; commit them first or "
            "pass --force to accept upstream anyway"
        )

    current_upstream = base_skill_path.read_text()
    new_digest = _sha256_digest(current_upstream)
    previous_digest = override.get("upstream_digest")
    if not isinstance(previous_digest, str) or not previous_digest:
        raise OverridesError(
            f"{manifest_path}: components.skills.{skill}.upstream_digest is "
            "missing; there is no recorded fork digest to accept away from"
        )
    accepted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    override["upstream_digest"] = new_digest
    if mode == "patch":
        base_rel = override.get("base_path")
        if not isinstance(base_rel, str) or not base_rel:
            raise OverridesError(
                f"{manifest_path}: components.skills.{skill}.base_path is "
                "required for patch mode"
            )
        (override_root / base_rel).write_text(current_upstream)

    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False))

    remote_sha = _resolve_base_remote_sha(target, override_root)
    lock_path = override_root / PLUGIN_OVERRIDE_LOCK
    lock_path.write_text(
        _render_plugin_override_lock(
            override_root,
            remote_sha,
            accept_provenance={
                skill: {
                    "previous_upstream_digest": previous_digest,
                    "new_upstream_digest": new_digest,
                    "accepted_at": accepted_at,
                }
            },
        )
    )

    return {
        "skill": skill,
        "mode": mode,
        "previous_upstream_digest": previous_digest,
        "new_upstream_digest": new_digest,
        "accepted_at": accepted_at,
        "forced": force,
        "next_command": "workbay-bootstrap repair --target .",
    }

def relock(
    *,
    target: Path,
    plugin_overrides: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Recompute recorded upstream digests from on-disk override/base content."""
    target = Path(target).resolve()
    override_root = _require_override_root(target, plugin_overrides)
    manifest_path = override_root / PLUGIN_OVERRIDE_MANIFEST
    manifest = _load_override_manifest(override_root)

    if not force and _override_root_is_dirty(target, override_root):
        raise DirtyOverrideRootError(
            f"{override_root} has uncommitted changes; commit them first or "
            "pass --force to relock anyway"
        )

    relocked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    updated: list[dict[str, Any]] = []
    skills = (manifest.get("components") or {}).get("skills") or {}
    for name, override in sorted(skills.items()):
        mode = override.get("mode")
        previous_digest = override.get("upstream_digest")
        if not isinstance(previous_digest, str) or not previous_digest:
            continue
        if mode == "patch":
            base_rel = override.get("base_path")
            if not isinstance(base_rel, str) or not base_rel:
                raise OverridesError(
                    f"{manifest_path}: components.skills.{name}.base_path is "
                    "required for patch mode"
                )
            base_path = override_root / base_rel
            if not base_path.is_file():
                raise OverridesError(
                    f"{manifest_path}: patch base_path missing: {base_rel}"
                )
            new_digest = _sha256_digest(base_path.read_text())
        elif mode == "replace":
            base_skill_path = target.joinpath(
                *PLUGIN_GENERATED_ROOT,
                "base",
                "claude",
                "skills",
                name,
                "SKILL.md",
            )
            if not base_skill_path.is_file():
                raise BaseTreeMissingError(
                    f"base plugin tree is missing {base_skill_path}; run "
                    "workbay-bootstrap install or update first"
                )
            new_digest = _sha256_digest(base_skill_path.read_text())
        else:
            continue
        if new_digest == previous_digest:
            continue
        override["upstream_digest"] = new_digest
        updated.append(
            {
                "skill": name,
                "mode": mode,
                "previous_upstream_digest": previous_digest,
                "new_upstream_digest": new_digest,
            }
        )

    if updated:
        manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False))
        remote_sha = _resolve_base_remote_sha(target, override_root)
        lock_path = override_root / PLUGIN_OVERRIDE_LOCK
        lock_path.write_text(
            _render_plugin_override_lock(override_root, remote_sha)
        )

    return {
        "updated": updated,
        "relocked_at": relocked_at,
        "forced": force,
        "next_command": "workbay-bootstrap repair --target .",
    }



