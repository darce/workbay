"""Stdlib-only loader for ``.workbay/embedding.env``.

Shared by the backfill CLI and bootstrap doctor so file-only vs process-only
cannot drift. Known keys only, no workbay imports.

Overlay rule — nonempty-process-overlay: a process env entry overlays the
file pin only when the key is present *and* its value is non-empty after
strip. Unset and ``""`` are not overrides; the file pin wins.

This is deliberately different from
``scripts/hooks/_envfile.load_embedding_env``, which is presence-wins
(set-if-unset): an empty-string process value is already present, so the
hook never fills it from the file. The hook must keep that rule. This
module must not claim parity with it.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

_EMBEDDING_ENV_REL = Path(".workbay/embedding.env")

_EMBEDDING_ENV_KEYS: frozenset[str] = frozenset(
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


def parse_embedding_env_file(repo_root: str | Path) -> dict[str, str]:
    """Read known keys from ``.workbay/embedding.env``; absent file is empty."""
    path = embedding_env_path(repo_root)
    if not path.is_file():
        return {}
    parsed: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        item = _parse_line(line)
        if item is None:
            continue
        key, value = item
        if key in _EMBEDDING_ENV_KEYS:
            parsed[key] = value
    return parsed


def _nonempty_process_overlay(source: Mapping[str, str], key: str) -> str | None:
    """Return a process value only when it is a deliberate non-empty override.

    Rule — nonempty-process-overlay: a process env entry overlays the file
    pin only when the key is present *and* its value is non-empty after
    strip. Unset and ``""`` are not overrides. Presence-not-value (``key in
    source``) must not clobber a file-level model pin or disable flag.
    """
    if key not in source:
        return None
    raw = source[key]
    if raw is None or not str(raw).strip():
        return None
    return str(raw)


def merge_embedding_env(
    repo_root: str | Path,
    process_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """File values with nonempty-process-overlay (empty process values lose).

    Same contract as :func:`apply_embedding_env` followed by reading
    ``os.environ``: a non-empty process value is not replaced; unset and
    empty-string process values do not outrank the file pin.
    """
    source = os.environ if process_env is None else process_env
    merged = parse_embedding_env_file(repo_root)
    for key in _EMBEDDING_ENV_KEYS:
        overlay = _nonempty_process_overlay(source, key)
        if overlay is not None:
            merged[key] = overlay
    return merged


def apply_embedding_env(repo_root: str | Path) -> bool:
    """Load ``.workbay/embedding.env`` into ``os.environ``.

    Returns ``True`` when the env file existed and was read; ``False`` when
    absent. Never clobbers an explicit non-empty operator/process value.
    Empty-string process values are not explicit overrides (nonempty-
    process-overlay) and receive the file pin. This is not the hook's
    presence-wins set-if-unset rule.
    """
    path = embedding_env_path(repo_root)
    if not path.is_file():
        return False
    for key, value in parse_embedding_env_file(repo_root).items():
        if _nonempty_process_overlay(os.environ, key) is None:
            os.environ[key] = value
    return True
