"""Validator for the committed ``.claude/settings.json`` pin contract.

Run as a script:
    python validate_claude_settings_pin.py <path/to/settings.json>

Exit code is 0 when the file matches the internal pin contract, 1 when
any rule fails. Errors are written one-per-line to stderr.

The validator is also importable: ``validate_settings(parsed_dict)``
returns a list of human-readable error strings (empty on success).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Sequence


class SettingsPinError(Exception):
    """Raised by ``main`` when the pin file fails validation."""


REQUIRED_SOURCE_DISCRIMINATOR = "directory"
EFFECTIVE_PLUGIN_PREFIX = "./.workbay/generated/plugins/workbay-system/effective/"


def _validate_marketplace_pin(settings_path: Path) -> list[str]:
    repo_root = settings_path.resolve().parents[1]
    marketplace_path = repo_root / ".claude-plugin" / "marketplace.json"
    if not marketplace_path.is_file():
        return []

    try:
        parsed_marketplace = json.loads(marketplace_path.read_text())
    except json.JSONDecodeError as exc:
        return [f"invalid JSON in {marketplace_path}: {exc}"]

    plugins = parsed_marketplace.get("plugins")
    if not isinstance(plugins, list) or not plugins:
        return [f"{marketplace_path} must declare a non-empty plugins list"]

    errors: list[str] = []
    for index, plugin in enumerate(plugins):
        if not isinstance(plugin, dict):
            errors.append(f"{marketplace_path}.plugins[{index}] must be an object")
            continue
        source = plugin.get("source")
        if not isinstance(source, str) or not source:
            errors.append(
                f"{marketplace_path}.plugins[{index}].source must be a non-empty string"
            )
            continue
        source_path = Path(source)
        if source_path.is_absolute():
            errors.append(
                f"{marketplace_path}.plugins[{index}].source must be a relative path"
            )
            continue
        if ".." in source_path.parts:
            errors.append(
                f"{marketplace_path}.plugins[{index}].source must not traverse outside the repo"
            )
            continue

        plugin_path = repo_root / source.removeprefix("./")
        if source.startswith(EFFECTIVE_PLUGIN_PREFIX) and not plugin_path.exists():
            errors.append(
                f"{marketplace_path}.plugins[{index}].source points at missing path {source!r}; "
                "run install or update to materialize the Claude plugin tree"
            )
    return errors


def validate_settings(
    parsed: object, *, settings_path: Path | None = None
) -> list[str]:
    errors: list[str] = []

    if not isinstance(parsed, dict):
        return [f"settings root must be a JSON object, got {type(parsed).__name__}"]

    extra = parsed.get("extraKnownMarketplaces", {})
    if not isinstance(extra, dict):
        errors.append("extraKnownMarketplaces must be an object")
        extra = {}

    for name, entry in extra.items():
        if not isinstance(entry, dict):
            errors.append(f"extraKnownMarketplaces.{name} must be an object")
            continue
        source = entry.get("source")
        if not isinstance(source, dict):
            errors.append(
                f"extraKnownMarketplaces.{name}.source must be an object "
                f"with 'source' and 'path' keys"
            )
            continue
        discriminator = source.get("source")
        if discriminator != REQUIRED_SOURCE_DISCRIMINATOR:
            errors.append(
                f"extraKnownMarketplaces.{name}.source.source must be "
                f"'directory' (got {discriminator!r}); Codex's 'local' "
                f"discriminator is not valid for Claude's "
                f"extraKnownMarketplaces schema"
            )
        path = source.get("path")
        if not isinstance(path, str) or not path:
            errors.append(
                f"extraKnownMarketplaces.{name}.source.path must be a "
                f"non-empty string"
            )
        elif Path(path).is_absolute():
            errors.append(
                f"extraKnownMarketplaces.{name}.source.path must be a "
                f"relative path (got absolute path {path!r}); the "
                f"`claude plugin marketplace add` CLI rewrites this to "
                f"absolute — restore '.' before committing"
            )

    enabled = parsed.get("enabledPlugins", {})
    if not isinstance(enabled, dict):
        errors.append("enabledPlugins must be an object")
        enabled = {}

    declared_marketplaces = set(extra.keys()) if isinstance(extra, dict) else set()
    for key in enabled.keys():
        if "@" not in key:
            errors.append(
                f"enabledPlugins key {key!r} must use <plugin>@<marketplace> form"
            )
            continue
        _, _, marketplace = key.partition("@")
        if marketplace not in declared_marketplaces:
            errors.append(
                f"enabledPlugins.{key} references marketplace "
                f"{marketplace!r}, which is not declared in "
                f"extraKnownMarketplaces"
            )

    if settings_path is not None:
        errors.extend(_validate_marketplace_pin(Path(settings_path)))

    return errors


def main(argv: Sequence[str]) -> int:
    if len(argv) != 1:
        print(
            "usage: validate_claude_settings_pin.py <path/to/settings.json>",
            file=sys.stderr,
        )
        return 2
    settings_path = Path(argv[0])
    if not settings_path.is_file():
        print(f"error: not a file: {settings_path}", file=sys.stderr)
        return 1
    try:
        parsed = json.loads(settings_path.read_text())
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON in {settings_path}: {exc}", file=sys.stderr)
        return 1

    errors = validate_settings(parsed, settings_path=settings_path)
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
