"""Stdlib-only loader for harness-neutral ``.workbay/embedding.env`` (C1 / S3).

Parses ``KEY=VALUE`` lines and applies them to ``os.environ`` with set-if-unset
semantics for non-empty process values. Unset and empty values are filled from
the file, matching the handoff embedding-env loader. No workbay imports.
"""

from __future__ import annotations

import os
from pathlib import Path

_EMBEDDING_ENV_REL = Path(".workbay/embedding.env")

# Vars provisioned by workbay-bootstrap; non-empty operator values win.
_EMBEDDING_ENV_KEYS = frozenset(
    {
        "WORKBAY_HANDOFF_EMBEDDING_MODEL",
        "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER",
        "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256",
        "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256",
        "WORKBAY_REINJECT_SEMANTIC",
        "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED",
    }
)


def embedding_env_path(repo_root: str | Path) -> Path:
    """Absolute path to the per-worktree embedding env file."""
    return Path(repo_root) / _EMBEDDING_ENV_REL


def _parse_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if "=" not in stripped:
        return None
    key, _, value = stripped.partition("=")
    key = key.strip()
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        value = value[1:-1]
    if not key:
        return None
    return key, value


def load_embedding_env(repo_root: str | Path) -> bool:
    """Load ``.workbay/embedding.env`` into ``os.environ``.

    Returns ``True`` when the env file existed and was read; ``False`` when
    absent (callers keep today's degrade path unchanged). A non-empty process
    value wins; an unset or empty process value receives the file pin.
    """
    path = embedding_env_path(repo_root)
    if not path.is_file():
        return False
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_line(line)
        if parsed is None:
            continue
        key, value = parsed
        if key not in _EMBEDDING_ENV_KEYS:
            continue
        process_value = os.environ.get(key)
        if process_value is None or not process_value.strip():
            os.environ[key] = value
    return True
