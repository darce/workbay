"""Config parse/resolve errors for design-sources.json (WBC-03)."""

from __future__ import annotations

from typing import Literal

InventoryConfigCode = Literal[
    "unknown_version",
    "unknown_field",
    "invalid",
    "escape",
    "oversized",
    "duplicate_id",
]


class InventoryConfigError(ValueError):
    """design-sources.json rejected at a JSON Pointer (RFC 6901)."""

    def __init__(
        self,
        *,
        pointer: str,
        code: InventoryConfigCode,
        message: str,
    ) -> None:
        super().__init__(f"{code} at {pointer or '(root)'}: {message}")
        self.pointer = pointer
        self.code = code
        self.message = message
