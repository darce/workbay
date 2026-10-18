"""Whole-token-safe remote env_file path validation (implementation note S1, DATA-14).

Leaf module: imports nothing from ``backend_spec`` or ``backend_registry`` so
both can validate ``env_file`` values against ONE regex. ``backend_spec``
re-exports ``_ENV_FILE_RE`` / ``_validate_env_file`` for existing callers.
"""

from __future__ import annotations

import re

# Whole-token-safe env_file paths for host-side single-quote embedding.
# Optional leading ~, then / and path chars only. No braces (not placeholder-
# substituted), no empty string, no shell metacharacters.
_ENV_FILE_RE = re.compile(r"^~?/[A-Za-z0-9._+/-]+$")


def _validate_env_file(env_file: str | None) -> None:
    """Refuse env_file values that are not whole-token safe for bash emit."""
    if env_file is None:
        return
    if not isinstance(env_file, str) or not env_file:
        raise ValueError(f"env_file must be None or a non-empty path, got {env_file!r}")
    if "\0" in env_file or not _ENV_FILE_RE.fullmatch(env_file):
        raise ValueError(f"env_file not whole-token safe (need ^~?/[A-Za-z0-9._+/-]+$): {env_file!r}")
