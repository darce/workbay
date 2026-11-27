#!/usr/bin/env python3
"""SessionStart hook: warn when installed hook surfaces are incoherent.

internal. For already-installed Claude workspaces the fail-open
``_run_guard.py`` wrapper only engages once configs are re-rendered with the
wrapper prefix; this self-check surfaces dangling hook references at session
start instead of letting them fail-close mid-session.

Stdlib-only, offline, and always exits 0: findings are reported through the
Claude SessionStart ``additionalContext`` channel, never as a block. This is
a lightweight projection of the resolve-every-script gate in
``workbay_bootstrap.coherence`` (which consumers may not have installed);
the full assessment still runs via ``make check-harness-coherence`` /
``make doctor``.
"""

from __future__ import annotations

import glob
import json
import os
import re
import shlex
import sys

_PLUGIN_HOOK_CONFIG_GLOB = ".workbay/generated/plugins/**/hooks*.json"
_HOOK_CONFIG_CANDIDATE_GLOBS = (
    ".github/hooks/*.json",
    ".codex/hooks.json",
    _PLUGIN_HOOK_CONFIG_GLOB,
)
_INTERPRETERS = frozenset({"python", "python3", "bash", "sh", "uv", "uvx"})
_ENV_ANCHOR_RE = re.compile(r"^\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/")
_PLUGIN_RUNNER_PATHS = {"./hooks/run.py", "hooks/run.py"}


def _workspace_root() -> str:
    from resolve_handoff_src import workspace_env_anchor

    # Per-harness anchors via the shared helper; VS Code / Codex spawn hooks
    # with cwd = workspace root.
    return workspace_env_anchor() or os.getcwd()


def _iter_command_strings(node: object) -> list[str]:
    commands: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "command" and isinstance(value, str):
                commands.append(value)
            else:
                commands.extend(_iter_command_strings(value))
    elif isinstance(node, list):
        for item in node:
            commands.extend(_iter_command_strings(item))
    return commands


def _command_path_tokens(command: str, *, plugin: bool) -> list[tuple[str, bool, bool]]:
    """Return paths with flags for plugin-relative paths and runner handlers."""
    try:
        words = shlex.split(command)
    except ValueError:
        return []
    if len(words) >= 3 and words[0] in {"sh", "bash"} and words[1] == "-c":
        # Substitutions and cd only establish execution context; resolve paths
        # in the remaining commands, including wrapper and handler arguments.
        script = re.sub(r"\$\([^)]*\)", "", words[2])
        tokens: list[tuple[str, bool, bool]] = []
        for segment in re.split(r"&&|;|\|", script):
            try:
                inner = shlex.split(segment)
            except ValueError:
                continue
            if not inner or inner[0] == "cd":
                continue
            if inner[0] == "exec":
                inner = inner[1:]
            tokens.extend(_command_path_tokens(shlex.join(inner), plugin=plugin))
        return tokens
    tokens = []
    for index, raw_word in enumerate(words):
        word = _ENV_ANCHOR_RE.sub("", raw_word)
        if word in _INTERPRETERS or word.startswith("-"):
            continue
        runner_handler = (
            plugin
            and index > 0
            and words[index - 1] in _PLUGIN_RUNNER_PATHS
            and raw_word not in {"", ".", ".."}
            and "/" not in raw_word
            and "\\" not in raw_word
        )
        if runner_handler or "/" in word or word.endswith((".py", ".sh")):
            plugin_relative = plugin and (raw_word.startswith("./") or raw_word in _PLUGIN_RUNNER_PATHS)
            tokens.append((word, plugin_relative, runner_handler))
    return tokens


def _runner_handler_exists(root: str, config_dir: str, handler: str) -> bool:
    if os.path.isfile(os.path.join(config_dir, "hooks", handler)):
        return True
    if os.path.basename(config_dir) != "agy":
        return False
    # The generated Agy adapter dispatches installed workspace hooks.
    installed = os.path.join(root, "scripts", "hooks", handler)
    return os.path.isfile(installed) or (not handler.endswith((".py", ".sh")) and os.path.isfile(installed + ".py"))


def _dangling_references(root: str) -> list[str]:
    dangling: list[str] = []
    for pattern in _HOOK_CONFIG_CANDIDATE_GLOBS:
        for config_path in glob.glob(os.path.join(root, pattern), recursive=True):
            try:
                payload = json.loads(open(config_path, encoding="utf-8").read())
            except (OSError, ValueError):
                continue
            rel_config = os.path.relpath(config_path, root)
            plugin = pattern == _PLUGIN_HOOK_CONFIG_GLOB
            config_dir = os.path.dirname(config_path)
            for command in _iter_command_strings(payload):
                for token, plugin_relative, runner_handler in _command_path_tokens(command, plugin=plugin):
                    if runner_handler:
                        exists = _runner_handler_exists(root, config_dir, token)
                    else:
                        exists = os.path.exists(os.path.join(config_dir if plugin_relative else root, token))
                    if not exists:
                        dangling.append(f"{rel_config}: `{command}` -> {token}")
    return dangling


def main() -> int:
    try:
        root = _workspace_root()
        dangling = _dangling_references(root)
        if not dangling:
            return 0
        lines = "\n".join(f"- {entry}" for entry in sorted(set(dangling)))
        context = (
            "Installed hook-surface coherence warning: rendered hook configs "
            "reference scripts that do not resolve from their expected locations. "
            "A missing handler is fail-open at runtime (_run_guard.py) but "
            "should be repaired: run `make check-harness-coherence` (or "
            "`make doctor`) and re-render/reinstall the hook surfaces.\n"
            f"{lines}"
        )
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "SessionStart",
                        "additionalContext": context,
                    }
                }
            )
        )
        return 0
    except Exception:
        # Self-check must never block a session.
        return 0


if __name__ == "__main__":
    sys.exit(main())
