"""Fail-closed checks for WorkBay response envelopes.

WorkBay write boundaries use two carriers: MCP tools return mappings with an
explicit boolean ``ok`` field, while a small number of queue APIs return a
bare boolean.  This module gives both carriers one read-side contract.  It
only classifies responses; retry and degradation policy remain with callers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, TypeVar

ResponseStatus = Literal["success", "refused", "invalid"]

_REASON_KEYS = ("error", "reason", "message", "detail")


@dataclass(frozen=True, slots=True)
class ResponseClassification:
    """Non-raising result of checking a response boundary.

    ``refused`` means the boundary explicitly rejected the operation.  It is
    deliberately separate from ``invalid`` (missing/malformed acknowledgement)
    so callers may apply different policy without reinterpreting the carrier.
    The original response is retained for logging and durable diagnostics.
    """

    status: ResponseStatus
    reason: str | None
    response: object

    @property
    def ok(self) -> bool:
        """Whether the response explicitly acknowledged success."""
        return self.status == "success"


class ResponseEnvelopeError(RuntimeError):
    """Raised when a response did not explicitly acknowledge success."""

    def __init__(
        self,
        classification: ResponseClassification,
        *,
        source: str = "response",
    ) -> None:
        if classification.ok:
            raise ValueError("ResponseEnvelopeError requires a non-success classification")
        self.classification = classification
        self.status = classification.status
        self.reason = classification.reason
        self.response = classification.response
        self.source = source
        super().__init__(f"{source} {classification.status}: {classification.reason}")


def _reason_text(response: Mapping[object, object]) -> str | None:
    """Extract the producer's cause without assuming only the v2 shape."""
    nested = response.get("data")
    data = nested if isinstance(nested, Mapping) else None

    # Prefer errors over softer reason/message fields, wherever they appear.
    # This covers the canonical v2 ``data.error`` and legacy/root responses.
    for key in _REASON_KEYS:
        for carrier in (response, data):
            if carrier is None or key not in carrier:
                continue
            value = carrier[key]
            if isinstance(value, str):
                if value.strip():
                    return value
                continue
            if value is not None:
                return str(value)
    return None


def classify_response(response: object) -> ResponseClassification:
    """Classify a WorkBay mapping/boolean response, failing closed.

    Success requires exactly ``True``: either a bare queue acknowledgement or
    the value of a mapping's ``ok`` field.  Explicit ``False`` is a refusal.
    Missing, non-boolean, ``None``, and other response shapes are invalid.
    """
    if isinstance(response, bool):
        if response:
            return ResponseClassification(status="success", reason=None, response=response)
        return ResponseClassification(
            status="refused",
            reason="response reported False",
            response=response,
        )

    if not isinstance(response, Mapping):
        if response is None:
            reason = "response is None"
        elif isinstance(response, str) and response.strip():
            reason = response
        elif isinstance(response, BaseException) and str(response):
            reason = str(response)
        else:
            reason = f"response has unsupported type {type(response).__name__}"
        return ResponseClassification(status="invalid", reason=reason, response=response)

    producer_reason = _reason_text(response)
    if "ok" not in response:
        return ResponseClassification(
            status="invalid",
            reason=producer_reason or "response mapping is missing required 'ok' field",
            response=response,
        )

    acknowledged = response["ok"]
    if acknowledged is True:
        return ResponseClassification(status="success", reason=None, response=response)
    if acknowledged is False:
        return ResponseClassification(
            status="refused",
            reason=producer_reason or "response reported ok=false",
            response=response,
        )
    return ResponseClassification(
        status="invalid",
        reason=(
            producer_reason
            or f"response 'ok' must be a boolean, got {type(acknowledged).__name__}"
        ),
        response=response,
    )


_ResponseT = TypeVar("_ResponseT")


def require_success(response: _ResponseT, *, source: str = "response") -> _ResponseT:
    """Return ``response`` unchanged, or raise when success is not explicit.

    Returning the same object lets callers validate and consume a write result
    in one expression.  The exception retains both the classification and the
    original response so the producer's cause remains observable.
    """
    classification = classify_response(response)
    if not classification.ok:
        raise ResponseEnvelopeError(classification, source=source)
    return response


__all__ = [
    "ResponseClassification",
    "ResponseEnvelopeError",
    "ResponseStatus",
    "classify_response",
    "require_success",
]
