"""Fail-open wrapper command transform (internal).

Single source for the wrapper-prefix form shared by the renderer
(``generate_agent_workflows.py``) and the contract-content checker
(``check_harness_sync.py``). Stdlib-only by design: the checker runs in
environments without the renderer's pydantic dependency.
"""

from __future__ import annotations

import re
import shlex

_GUARD_WRAPPER_RELPATH = "scripts/hooks/_run_guard.py"
# Kept aligned with coherence._INTERPRETERS (REV-A-002): the resolve gate and
# the wrap transform must classify the same words as interpreter prefixes.
_WRAP_INTERPRETER_WORDS = frozenset({"python", "python3", "bash", "sh", "uv", "uvx"})
# Workspace-root env anchors ($CLAUDE_PROJECT_DIR/, ${GROK_WORKSPACE_ROOT}/, …)
_WRAP_ANCHOR_RE = re.compile(r"^\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/")


def _looks_like_handler_path(token: str) -> bool:
    stripped = _WRAP_ANCHOR_RE.sub("", token)
    return "/" in stripped or stripped.endswith((".py", ".sh"))


def _quote_command_token(token: str) -> str:
    # Re-add the double quotes the contract uses around env-anchored paths so
    # the harness shell still expands the anchor variable.
    if "$" in token or " " in token:
        return f'"{token}"'
    return token


def wrap_guard_command(command: str, *, fail_mode: object = None) -> str:
    """Prefix one rendered hook command with the fail-open wrapper.

    The wrapper path is emitted with the SAME per-harness anchor the command
    already uses (``$CLAUDE_PROJECT_DIR`` for Claude, ``${GROK_WORKSPACE_ROOT}``
    for Grok, relative for VS Code/Codex whose runners spawn hooks with
    cwd=workspace root), so wrapper self-resolution rides the anchor the
    contract already verified per harness. The original interpreter word is
    dropped: the wrapper re-derives bash vs python3 from the handler
    extension. Idempotent — an already-wrapped command is returned unchanged.
    ``fail_mode == "closed"`` renders the ``--fail-mode=closed`` opt-out flag
    (the wrapper then blocks on a missing handler instead of failing open).
    """
    try:
        words = shlex.split(command)
    except ValueError:
        return command
    if not words:
        return command
    rest = words[1:] if words[0] in _WRAP_INTERPRETER_WORDS else list(words)
    if not rest:
        return command
    script = rest[0]
    if script.endswith("_run_guard.py"):
        return command
    if not _looks_like_handler_path(script):
        # The leading token is not a recognizable handler path (e.g. an
        # interpreter subcommand like `uv run <script>`): refuse to wrap
        # rather than mis-wrap a non-path token as the handler (REV-A-002).
        return command
    anchor_match = _WRAP_ANCHOR_RE.match(script)
    anchor = anchor_match.group(0) if anchor_match else ""
    tokens = ["python3", f"{anchor}{_GUARD_WRAPPER_RELPATH}"]
    if fail_mode == "closed":
        tokens.append("--fail-mode=closed")
    tokens.extend(rest)
    return " ".join(_quote_command_token(token) for token in tokens)
