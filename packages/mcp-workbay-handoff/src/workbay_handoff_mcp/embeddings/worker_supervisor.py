"""Stdlib worker registration and control receipts (implementation note S0).

Receipts are frozen typed outcomes. Construction does not open a database,
launch a child, acquire a model permit, or import provider/model code.

Reason fields are machine-readable ``^[a-z][a-z0-9_]*$`` tokens of at most
``MAX_REASON_CHARS`` characters.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Protocol, get_args

MAX_REASON_CHARS = 256
_REASON_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

ReceiptStatus = Literal[
    "unavailable",
    "deferred",
    "refused",
    "deadline",
    "revision_mismatch",
]
_RECEIPT_STATUSES = frozenset(get_args(ReceiptStatus))


def _require_str(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _require_status(value: object) -> ReceiptStatus:
    if isinstance(value, bool) or not isinstance(value, str):
        raise TypeError("status must be a string")
    if value not in _RECEIPT_STATUSES:
        raise ValueError(f"unsupported status: {value!r}")
    return value  # type: ignore[return-value]


def _require_reason(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("reason must be a string")
    if len(value) > MAX_REASON_CHARS:
        raise ValueError("reason exceeds documented bound")
    if _REASON_PATTERN.fullmatch(value) is None:
        raise ValueError("reason must be a machine-readable token")
    return value


@dataclass(frozen=True)
class RegistrationRequest:
    """Canonical database identity for a later supervisor; S0 does not resolve it."""

    canonical_db_identity: str

    def __post_init__(self) -> None:
        _require_str("canonical_db_identity", self.canonical_db_identity)


@dataclass(frozen=True)
class RegistrationReceipt:
    """Identity plus a typed outcome. Status is never an untyped success boolean."""

    identity: str
    status: ReceiptStatus

    def __post_init__(self) -> None:
        _require_str("identity", self.identity)
        _require_status(self.status)


@dataclass(frozen=True)
class ControlReceipt:
    """Typed control outcome. Construction does not imply inference occurred."""

    status: ReceiptStatus
    reason: str

    def __post_init__(self) -> None:
        _require_status(self.status)
        _require_reason(self.reason)


class WorkerRegistration(Protocol):
    """Registration port. S2 owns the implementation that talks to a live worker."""

    def register(self, request: RegistrationRequest) -> RegistrationReceipt: ...


class WorkerControl(Protocol):
    """Control port. S2 owns start/stop/health behavior."""

    def control(self, identity: str) -> ControlReceipt: ...
