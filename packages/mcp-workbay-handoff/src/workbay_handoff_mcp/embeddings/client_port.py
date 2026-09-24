"""Stdlib embedding client records and protocol (implementation note S0).

Request/result/error construction is frozen and numpy-free. Transport, sidecar
activation, and inference belong to later stages.

Reason fields are machine-readable ``^[a-z][a-z0-9_]*$`` tokens of at most
``MAX_REASON_CHARS`` characters.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Protocol, get_args

_FLOAT32_BYTES = 4
PROTOCOL_VERSION = 1
MAX_REASON_CHARS = 256
_REASON_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

ResultStatus = Literal[
    "ok",
    "unavailable",
    "deferred",
    "refused",
    "deadline",
    "revision_mismatch",
]
ErrorStatus = Literal[
    "unavailable",
    "deferred",
    "refused",
    "deadline",
    "revision_mismatch",
]
_RESULT_STATUSES = frozenset(get_args(ResultStatus))
_ERROR_STATUSES = frozenset(get_args(ErrorStatus))


def _owned_bytes(item: object) -> bytes:
    if isinstance(item, (bytes, bytearray, memoryview)):
        return bytes(item)
    raise TypeError("vectors must be bytes-like")


def _require_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _require_str(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _require_protocol_version(value: object) -> int:
    version = _require_int("protocol_version", value)
    if version != PROTOCOL_VERSION:
        raise ValueError(f"protocol_version must be {PROTOCOL_VERSION}")
    return version


def _require_status(name: str, value: object, allowed: frozenset[str]) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if value not in allowed:
        raise ValueError(f"unsupported {name}: {value!r}")
    return value


def _require_reason(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("reason must be a string")
    if len(value) > MAX_REASON_CHARS:
        raise ValueError("reason exceeds documented bound")
    if _REASON_PATTERN.fullmatch(value) is None:
        raise ValueError("reason must be a machine-readable token")
    return value


def _owned_texts(value: object) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise TypeError("texts must be a sequence of strings")
    try:
        items = tuple(value)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("texts must be a sequence of strings") from exc
    owned: list[str] = []
    for item in items:
        if not isinstance(item, str):
            raise TypeError("texts must be a sequence of strings")
        owned.append(item)
    return tuple(owned)


def _owned_truncation(value: object) -> tuple[bool, ...]:
    try:
        items = tuple(value)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("truncated flags must be a sequence of booleans") from exc
    if any(not isinstance(flag, bool) for flag in items):
        raise TypeError("truncated flags must be booleans")
    return items


@dataclass(frozen=True)
class EmbeddingRequest:
    """Immutable embed request: remaining timeout, not a cross-process clock."""

    protocol_version: int
    request_id: str
    model_revision: str
    texts: tuple[str, ...]
    timeout_ms: int

    def __post_init__(self) -> None:
        _require_protocol_version(self.protocol_version)
        _require_str("request_id", self.request_id)
        _require_str("model_revision", self.model_revision)
        _require_int("timeout_ms", self.timeout_ms)
        object.__setattr__(self, "texts", _owned_texts(self.texts))


@dataclass(frozen=True)
class EmbeddingResult:
    """Immutable embed result with owned float32 little-endian vector payloads."""

    protocol_version: int
    request_id: str
    model_revision: str
    dim: int
    status: ResultStatus
    vectors: tuple[bytes, ...]
    truncated: tuple[bool, ...]
    reason: str

    def __post_init__(self) -> None:
        _require_protocol_version(self.protocol_version)
        _require_str("request_id", self.request_id)
        _require_str("model_revision", self.model_revision)
        dim = _require_int("dim", self.dim)
        if dim <= 0:
            raise TypeError("dim must be a positive integer")
        _require_status("status", self.status, _RESULT_STATUSES)
        _require_reason(self.reason)
        vectors = tuple(_owned_bytes(item) for item in self.vectors)
        truncated = _owned_truncation(self.truncated)
        object.__setattr__(self, "vectors", vectors)
        object.__setattr__(self, "truncated", truncated)
        if len(vectors) != len(truncated):
            raise ValueError("vector count must match truncated flags")
        expected = dim * _FLOAT32_BYTES
        if any(len(item) != expected for item in vectors):
            raise ValueError("vector payload length must match float32 dimension")


@dataclass(frozen=True)
class EmbeddingClientError:
    """Immutable client-side failure record; construction does not imply inference."""

    protocol_version: int
    request_id: str
    model_revision: str
    status: ErrorStatus
    reason: str

    def __post_init__(self) -> None:
        _require_protocol_version(self.protocol_version)
        _require_str("request_id", self.request_id)
        _require_str("model_revision", self.model_revision)
        _require_status("status", self.status, _ERROR_STATUSES)
        _require_reason(self.reason)


class EmbeddingClient(Protocol):
    """Typed embed surface. Implementations live outside S0."""

    def embed(self, request: EmbeddingRequest) -> EmbeddingResult: ...
