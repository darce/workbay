"""worktreeConfig hygiene when bootstrap enables per-worktree git config."""

from __future__ import annotations

import configparser
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from workbay_bootstrap.install_receipt import InstallReceipt

# ``core.worktree`` must never remain in common config once worktreeConfig is on.
# It is DROPPED (unset from common), never relocated: a ``core.worktree`` in the
# SHARED config is pathological (git ignores it and warns), the main worktree's
# location is already fixed by its gitdir, and relocating a possibly-invalid path
# into ``config.worktree`` would ACTIVATE it and wedge the next config op.
# ``core.bare`` is relocated only when ``true`` — ``false`` stays shared (git default).
_DROP_KEYS = ("core.worktree",)


class WorktreeConfigMigrationError(RuntimeError):
    """Raised when worktreeConfig cannot be enabled safely."""


def _git_read(target: Path, *args: str, cwd: Path | None = None) -> str:
    from workbay_bootstrap.install import _git

    return _git(*args, cwd=cwd or target)


def _common_config_path(target: Path) -> Path:
    common = _git_read(target, "rev-parse", "--git-common-dir")
    path = Path(common)
    if not path.is_absolute():
        path = (target / path).resolve()
    return path / "config"


def _config_file_get(config_path: Path, key: str) -> str | None:
    from workbay_bootstrap.external import run_external

    if not config_path.is_file():
        return None
    result = run_external(
        ["git", "config", "--file", str(config_path), "--get", key],
        call_class="git",
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _main_worktree_is_bare_owner(main_worktree: Path) -> bool:
    """Return whether the main worktree path is a bare git directory.

    Do not trust ``git worktree list``'s ``bare`` line while ``core.bare=true``
    still lives in the shared common config — that pollution makes git report the
    main worktree as bare even for a normal checkout.
    """
    return not (main_worktree / ".git").exists()


def _parse_main_worktree_entry(target: Path) -> tuple[Path, bool]:
    raw = _git_read(target, "worktree", "list", "--porcelain")
    for line in raw.splitlines():
        if line.startswith("worktree "):
            path = Path(line.split(" ", 1)[1])
            return path, _main_worktree_is_bare_owner(path)
    raise WorktreeConfigMigrationError(
        "cannot resolve the main worktree for worktreeConfig migration; "
        "run `git worktree list` and repair the repository before retrying"
    )


def _main_worktree_path(target: Path) -> Path:
    return _parse_main_worktree_entry(target)[0]


def _keys_to_migrate(
    common_config: Path,
    *,
    main_is_bare: bool,
) -> list[tuple[str, str, str]]:
    """Return ``(key, value, action)`` tuples; action is ``relocate`` or ``drop``."""
    pending: list[tuple[str, str, str]] = []
    for key in _DROP_KEYS:
        value = _config_file_get(common_config, key)
        if value is not None:
            pending.append((key, value, "drop"))
    bare_value = _config_file_get(common_config, "core.bare")
    if bare_value == "true":
        # Polluted common bare=true on a working-tree main must be corrected.
        pending.append(("core.bare", "true" if main_is_bare else "false", "relocate"))
    return pending


def _migrate_common_keys_to_main_worktree(
    target_root: Path,
    *,
    main_worktree: Path,
    pending: list[tuple[str, str, str]],
    receipt: InstallReceipt | None,
) -> None:
    from workbay_bootstrap.git_write import _git_write

    for key, value, action in pending:
        # ``relocate`` moves the key into the main worktree's config.worktree first;
        # ``drop`` only removes it from the shared common config (never activated).
        if action == "relocate":
            _git_write(
                target_root,
                "config",
                "--worktree",
                key,
                value,
                cwd=main_worktree,
                receipt=receipt,
            )
        # ``--unset-all`` (not ``--unset``): a multivar key makes ``--unset`` exit 5
        # and crash the install. ``--unset-all`` removes every value and tolerates a
        # single or absent value, so the drop/relocate cleanup is idempotent.
        try:
            _git_write(
                target_root,
                "config",
                "--unset-all",
                key,
                cwd=main_worktree,
                receipt=receipt,
            )
        except subprocess.CalledProcessError as exc:
            raise WorktreeConfigMigrationError(
                f"failed to remove {key} from the shared git config "
                f"(main worktree {main_worktree}); inspect "
                f"{_common_config_path(target_root)} and "
                f"{main_worktree}/.git/config.worktree before retrying "
                "bootstrap install"
            ) from exc


def enable_worktree_config_with_migration(
    target_root: Path,
    *,
    receipt: InstallReceipt | None = None,
) -> None:
    """Enable ``extensions.worktreeConfig`` and migrate stray common keys.

    When ``target_root`` is a linked worktree, git-worktree(1) documents that
    ``core.bare`` / ``core.worktree`` must live in the main worktree's
    ``config.worktree`` once ``extensions.worktreeConfig`` is on. Leaving them
    in the shared common config makes every worktree inherit ``core.bare=true``.
    """
    from workbay_bootstrap.git_write import _git_write

    common_config = _common_config_path(target_root)
    main_worktree, main_is_bare = _parse_main_worktree_entry(target_root)

    already_enabled = _config_file_get(common_config, "extensions.worktreeConfig")
    if already_enabled != "true":
        _git_write(
            target_root,
            "config",
            "extensions.worktreeConfig",
            "true",
            cwd=target_root,
            receipt=receipt,
        )

    pending = _keys_to_migrate(common_config, main_is_bare=main_is_bare)
    if pending:
        _migrate_common_keys_to_main_worktree(
            target_root,
            main_worktree=main_worktree,
            pending=pending,
            receipt=receipt,
        )
        remaining = _keys_to_migrate(common_config, main_is_bare=main_is_bare)
        if remaining:
            keys = ", ".join(k for k, *_ in remaining)
            raise WorktreeConfigMigrationError(
                f"failed to relocate {keys} from the shared git config into "
                f"the main worktree ({main_worktree}); inspect "
                f"{common_config} and {main_worktree}/.git/config.worktree "
                "before retrying bootstrap install"
            )


def parse_git_config(text: str) -> dict[str, dict[str, str]]:
    """Parse a git config ini file into section -> key -> value."""
    parser = configparser.ConfigParser()
    parser.optionxform = str  # type: ignore[method-assign]
    parser.read_string(text)
    return {section: dict(parser.items(section)) for section in parser.sections()}


def common_config_delta(
    before: str,
    after: str,
) -> dict[str, Any]:
    """Return added/removed/changed keys between two common config snapshots."""
    b = parse_git_config(before or "")
    a = parse_git_config(after or "")
    sections = sorted(set(b) | set(a))
    delta: dict[str, Any] = {"added": {}, "removed": {}, "changed": {}}
    for section in sections:
        b_keys = b.get(section, {})
        a_keys = a.get(section, {})
        for key in sorted(set(b_keys) | set(a_keys)):
            bv, av = b_keys.get(key), a_keys.get(key)
            if bv is None and av is not None:
                delta["added"].setdefault(section, {})[key] = av
            elif bv is not None and av is None:
                delta["removed"].setdefault(section, {})[key] = bv
            elif bv != av:
                delta["changed"].setdefault(section, {})[key] = {
                    "before": bv,
                    "after": av,
                }
    return delta
