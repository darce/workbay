"""The handoff store's embedding protocol over the shared resident sidecar.

Only public provider error types are imported; no model/runtime is loaded here.
"""

from __future__ import annotations

import time

import numpy as np

from .provider import EmbeddingArtifactError
from .sidecar_client import EmbeddingClient, SidecarUnavailable

# A warmth check is advisory and must never launch or wait for a model load.
SIDECAR_PING_TIMEOUT_SECONDS = 0.25
# store.py records up to 51.46s for bounded-text ONNX inference. Allow that
# request to finish, with margin, while bounding abandoned best-effort workers
# independently of their callers' shorter budgets (Canon v0.26.2 RES-02,
# Release It!, ch. 5). Startup has a separate child-owned load deadline.
SIDECAR_EMBED_TIMEOUT_SECONDS = 60.0


class SidecarEmbeddingError(EmbeddingArtifactError):
    """Sidecar transport/identity/vector failure on the existing degrade path."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"embedding sidecar unavailable: {reason}", kind=f"sidecar_{reason}")


class SidecarEmbeddingAdapter:
    """Satisfy SupportsEmbed and its existing bounded-loader/warmth protocol."""

    def __init__(self, client: EmbeddingClient) -> None:
        self._client = client
        self.dim = client.dim
        self.model_id = client.model_id

    @property
    def is_loaded(self) -> bool:
        return self._client.ping(timeout_seconds=SIDECAR_PING_TIMEOUT_SECONDS) is True

    def _ensure_loaded_with_timeout(self, *, timeout_seconds: float, timeout_message: str) -> None:
        result = self._client.ensure_started_detached(timeout_seconds=timeout_seconds)
        if isinstance(result, SidecarUnavailable):
            if result.reason == "deadline":
                # Same type and caller-supplied message as EmbeddingProvider.
                raise TimeoutError(timeout_message)
            raise SidecarEmbeddingError(result.reason)

    def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, self.dim), dtype="<f4")
        # Direct query/anchor callers do not use the loader protocol themselves.
        # Share one budget across startup and the single inference request.
        deadline = time.monotonic() + SIDECAR_EMBED_TIMEOUT_SECONDS
        try:
            if not self.is_loaded:
                self._ensure_loaded_with_timeout(
                    timeout_seconds=max(0.0, deadline - time.monotonic()),
                    timeout_message="embedding sidecar request timed out",
                )
            vectors = self._client.embed(
                texts, deadline_seconds=max(0.0, deadline - time.monotonic()), start_if_missing=False
            )
        except SidecarEmbeddingError:
            raise
        except TimeoutError as exc:
            # related_prior_work catches artifact errors, not TimeoutError.
            raise SidecarEmbeddingError("deadline") from exc
        except Exception as exc:
            raise SidecarEmbeddingError("unavailable") from exc
        if isinstance(vectors, SidecarUnavailable):
            raise SidecarEmbeddingError(vectors.reason)
        try:
            array = np.asarray(vectors, dtype="<f4")
        except (TypeError, ValueError, OverflowError) as exc:
            raise SidecarEmbeddingError("invalid_response") from exc
        if array.shape != (len(texts), self.dim) or not np.isfinite(array).all():
            raise SidecarEmbeddingError("invalid_response")
        return array
