"""Canonical WorkBay env-var resolution helpers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import overload

__all__ = ["resolve_env_alias"]


def _non_empty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


@overload
def resolve_env_alias(
    canonical: str,
    *,
    env: Mapping[str, str] | None = ...,
    default: str,
) -> str: ...


@overload
def resolve_env_alias(
    canonical: str,
    *,
    env: Mapping[str, str] | None = ...,
    default: None = ...,
) -> str | None: ...


def resolve_env_alias(
    canonical: str,
    *,
    env: Mapping[str, str] | None = None,
    default: str | None = None,
) -> str | None:
    """Resolve a canonical WorkBay env var.

    Blank/whitespace-only values are treated as unset, matching the existing
    ``_first_non_empty_env`` behaviour in the handoff package.
    """

    source = os.environ if env is None else env

    canonical_value = _non_empty(source.get(canonical))
    if canonical_value is not None:
        return canonical_value
    return default
