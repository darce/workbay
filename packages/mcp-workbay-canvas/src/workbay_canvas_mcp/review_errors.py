"""Typed errors for ReviewService (WBC-05 S6)."""

from __future__ import annotations


class ReviewServiceError(Exception):
    """Domain error with a stable code, workspace-relative message, and details."""

    def __init__(self, *, code: str, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details: dict = {} if details is None else details
