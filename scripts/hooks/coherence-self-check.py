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

_HOOK_CONFIG_CANDIDATE_GLOBS = (
    ".github/hooks/*.json",
    ".codex/hooks.json",
    ".workbay/generated/plugins/**/hooks*.json",
)
_INTERPRETERS = frozenset({"python", "python3", "bash", "sh", "uv", "uvx"})
_ENV_ANCHOR_RE = re.compile(r"^\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/")


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


def _command_path_tokens(command: str) -> list[str]:
    try:
        words = shlex.split(command)
    except ValueError:
        return []
    tokens: list[str] = []
    for word in words:
        word = _ENV_ANCHOR_RE.sub("", word)
        if word in _INTERPRETERS or word.startswith("-"):
            continue
        if "/" in word or word.endswith((".py", ".sh")):
            tokens.append(word)
    return tokens


def _dangling_references(root: str) -> list[str]:
    dangling: list[str] = []
    for pattern in _HOOK_CONFIG_CANDIDATE_GLOBS:
        for config_path in glob.glob(os.path.join(root, pattern), recursive=True):
            try:
                payload = json.loads(open(config_path, encoding="utf-8").read())
            except (OSError, ValueError):
                continue
            rel_config = os.path.relpath(config_path, root)
            for command in _iter_command_strings(payload):
                for token in _command_path_tokens(command):
                    resolved = (
                        token if os.path.isabs(token) else os.path.join(root, token)
                    )
                    if not os.path.exists(resolved):
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
            "reference scripts that do not resolve from the workspace root. "
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
