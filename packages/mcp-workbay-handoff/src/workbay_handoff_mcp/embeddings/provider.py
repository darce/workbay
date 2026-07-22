"""Local, offline gte-base-en-v1.5 embedding provider (implementation note, implementation note).

Design constraints (from the plan / scope):

- **Offline & deterministic.** The provider never reaches the network on any code
  path. The ~147MB int8 ONNX model is *not* bundled; it is resolved from an
  explicit, hash-pinned artifact location (e.g. env-configured local path). An
  absent artifact or missing optional libs is a clean degrade, not a fetch.
- **No prefixes.** gte-base-en-v1.5 requires no query/passage instruction prefix;
  text is embedded symmetrically.
- **CLS pooling + L2 norm.** gte-* pools the first ([CLS]) token, then we
  L2-normalize so cosine == inner product.
- **Memory-bounded inference.** Inputs are character-capped and token-capped
  before inference; ``embed`` runs fixed-size sub-batches; a pre-flight
  attention-cost estimate refuses requests that would blow a configurable
  ceiling. The ONNX ``InferenceSession`` is cached on the provider instance
  (not recreated per call); CPU arena is left enabled but the session is
  constructed with explicit ``SessionOptions``.

The pure helpers (`l2_normalize`, `cls_pool`, `sha256_file`, `verify_artifact`)
depend only on numpy and are unit-tested without the model. onnxruntime and
tokenizers are imported lazily inside `_ensure_loaded` so importing this module
needs only the (optional) numpy dependency.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np

from workbay_handoff_mcp.embeddings.model_pin import EMBEDDING_DIM, MODEL_ID

# Bound measured corpus comfortably: concept + compaction texts peak ~1.4k
# tokens; 2048 is lossless for every observed row with headroom.
MAX_SEQUENCE_TOKENS = 2048
# ~4 chars/token rough pre-tokenizer proxy. Applied to raw strings before
# tokenization so callers / stub providers never see unbounded text.
MAX_EMBED_CHARS = MAX_SEQUENCE_TOKENS * 4  # 8192
# Sub-batch size for embed(): keeps tensors off the "one huge list" path.
EMBED_SUB_BATCH_SIZE = 16
# gte-base-en-v1.5 attention head count (base BERT-family geometry).
ATTENTION_HEADS = 12
# Env override for the pre-flight ceiling (integer bytes).
ENV_MAX_ATTENTION_BYTES = "WORKBAY_HANDOFF_EMBEDDING_MAX_ATTENTION_BYTES"

_HASH_CHUNK = 1 << 20

# Opt-in artifact configuration (offline; operator points these at a locally
# installed model — never fetched by the provider).
ENV_MODEL = "WORKBAY_HANDOFF_EMBEDDING_MODEL"
ENV_TOKENIZER = "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER"
ENV_MODEL_SHA256 = "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256"
ENV_TOKENIZER_SHA256 = "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256"
ENV_EMBEDDINGS_DISABLED = "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED"

_TRUE_VALUES = frozenset({"1", "true", "yes"})


def _embeddings_disabled(env: Mapping[str, str]) -> bool:
    raw = env.get(ENV_EMBEDDINGS_DISABLED)
    if raw is None:
        return False
    return raw.strip().lower() in _TRUE_VALUES


def embedding_unavailable_reason(env: Mapping[str, str] | None = None) -> str | None:
    """Classify *why* ``EmbeddingProvider.from_env`` would return ``None``.

    Returns ``"disabled"`` when embeddings are explicitly turned off, ``"unconfigured"``
    when the artifact env vars / SHA pins are missing, or ``None`` when a provider is
    constructible (env-on and fully configured). Mirrors ``from_env``'s two ``None``
    branches so callers can report a typed skip reason instead of the generic
    ``"provider_unavailable"``. Never loads the artifact or imports the runtime.
    """
    source = os.environ if env is None else env
    if _embeddings_disabled(source):
        return "disabled"
    model = source.get(ENV_MODEL)
    tokenizer = source.get(ENV_TOKENIZER)
    model_sha = source.get(ENV_MODEL_SHA256)
    tokenizer_sha = source.get(ENV_TOKENIZER_SHA256)
    if not (model and tokenizer and model_sha and tokenizer_sha):
        return "unconfigured"
    return None


class EmbeddingArtifactError(RuntimeError):
    """Raised when the embedding artifact is missing, corrupt, hash-mismatched, or its runtime libs are absent."""


class EmbeddingBudgetExceeded(RuntimeError):
    """Raised when estimated attention cost exceeds the configured ceiling.

    Caller-handleable: a refused embed is recoverable; an OOM-killed host is not.
    """

    def __init__(
        self,
        *,
        estimated_bytes: int,
        ceiling_bytes: int,
        batch: int,
        seq: int,
    ) -> None:
        self.estimated_bytes = estimated_bytes
        self.ceiling_bytes = ceiling_bytes
        self.batch = batch
        self.seq = seq
        super().__init__(
            f"embedding attention budget exceeded: estimated {estimated_bytes} bytes "
            f"(batch={batch}, seq={seq}, heads={ATTENTION_HEADS}) > ceiling {ceiling_bytes} bytes"
        )


def truncate_embed_text(text: str, max_chars: int = MAX_EMBED_CHARS) -> str:
    """Cap raw text length before it reaches tokenization / ``embed``.

    Character-level pre-tokenizer bound. ``MAX_EMBED_CHARS`` (~4 chars/token ×
    ``MAX_SEQUENCE_TOKENS``) is lossless for the measured concept/compaction
    corpus. Empty input is returned unchanged.
    """
    if max_chars < 0:
        raise ValueError("max_chars must be non-negative")
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def estimate_attention_bytes(batch: int, seq: int, *, heads: int = ATTENTION_HEADS) -> int:
    """Estimate attention-matrix bytes: ``batch × seq² × heads × 4``.

    Matches the measured hazard model (float32 attention scores). Used only as
    a pre-flight admission control — not a precise ORT allocator forecast.
    """
    if batch < 0 or seq < 0 or heads < 0:
        raise ValueError("batch, seq, and heads must be non-negative")
    return int(batch) * int(seq) * int(seq) * int(heads) * 4


# Default ceiling, sized against the MEASURED corpus rather than the theoretical
# worst case. Deriving it from EMBED_SUB_BATCH_SIZE × MAX_SEQUENCE_TOKENS admits
# ~3.0 GiB — over half the 6 GiB remote-VM cap and most of an 8 GiB laptop — so a
# guard at that value permits the very spike it exists to prevent.
#
# Real inputs are far smaller: the concept corpus (16,837 rows) averages ~500
# chars (~125 tokens) and peaks at 3,548 chars (~890 tokens); the largest
# compaction anchor observed is 5,800 chars (~1,450 tokens). A 512 MiB ceiling
# admits a full 16-item sub-batch at ~815 tokens, comfortably above every row
# ever embedded, while keeping one pass an order of magnitude below the host.
#
# Over-ceiling requests are REFUSED (typed EmbeddingBudgetExceeded), not silently
# downscaled: a refused embed is recoverable and observable, an OOM-killed host is
# neither. Operators may override via WORKBAY_HANDOFF_EMBEDDING_MAX_ATTENTION_BYTES.
DEFAULT_MAX_ATTENTION_BYTES = 512 * 1024 * 1024


def resolve_max_attention_bytes(env: Mapping[str, str] | None = None) -> int:
    """Return the pre-flight attention-byte ceiling (env override or default)."""
    source = os.environ if env is None else env
    raw = source.get(ENV_MAX_ATTENTION_BYTES)
    if raw is None or not str(raw).strip():
        return DEFAULT_MAX_ATTENTION_BYTES
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise ValueError(
            f"{ENV_MAX_ATTENTION_BYTES} must be an integer byte count, got {raw!r}"
        ) from exc
    if value < 0:
        raise ValueError(f"{ENV_MAX_ATTENTION_BYTES} must be non-negative, got {value}")
    return value


def configure_tokenizer_bounds(tokenizer: object, *, max_length: int = MAX_SEQUENCE_TOKENS) -> None:
    """Apply truncation so batches can never pad past ``max_length``.

    Truncation is what bounds the batch: BatchLongest pads to the widest item and
    the widest item is capped, so the padded width is ``min(longest, max_length)``.

    Deliberately NOT ``enable_padding(length=max_length)``. Fixed-length padding
    inflates every short text to the full cap — a 20-token finding priced like a
    2048-token document — making the common case ~100x more expensive while still
    appearing bounded. The original defect was the cap being 8192, not the padding
    strategy; lowering the cap fixes it without that cost.
    """
    tokenizer.enable_truncation(max_length=max_length)  # type: ignore[attr-defined]
    tokenizer.enable_padding()  # type: ignore[attr-defined]  # BatchLongest, bounded by truncation


def l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """Row-wise L2 normalization. Zero rows stay zero (no divide-by-zero)."""
    arr = np.asarray(matrix, dtype=np.float32)
    if arr.ndim != 2:
        raise ValueError(f"expected a 2D (batch, dim) matrix, got shape {arr.shape}")
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms = np.where(norms == 0.0, 1.0, norms)
    return cast(np.ndarray, (arr / norms).astype(np.float32))


def cls_pool(last_hidden_state: np.ndarray) -> np.ndarray:
    """gte-* pooling: the [CLS] (first) token of each sequence.

    ``(batch, seq, hidden) -> (batch, hidden)``.
    """
    arr = np.asarray(last_hidden_state, dtype=np.float32)
    if arr.ndim != 3:
        raise ValueError(f"expected (batch, seq, hidden), got shape {arr.shape}")
    return arr[:, 0, :].astype(np.float32)


def sha256_file(path: Path) -> str:
    """Streaming SHA-256 of a file (handles large binary artifacts)."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_artifact(path: Path, expected_sha256: str) -> None:
    """Fail closed: raise if the file is absent or its hash does not match."""
    if not path.is_file():
        raise EmbeddingArtifactError(f"embedding artifact not found: {path}")
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise EmbeddingArtifactError(
            f"embedding artifact hash mismatch for {path}: expected {expected_sha256}, got {actual}"
        )


@dataclass(frozen=True)
class ArtifactSpec:
    """Pinned location + checksums of the model and tokenizer artifacts."""

    model_path: Path
    tokenizer_path: Path
    model_sha256: str
    tokenizer_sha256: str
    dim: int = EMBEDDING_DIM


class EmbeddingProvider:
    """CLS-pooled, L2-normalized gte-base-en-v1.5 embeddings via onnxruntime (CPU).

    Lifetime notes (instrumentation deliverable for the 6.9 GB investigation):

    - ``InferenceSession`` is created **once** in ``_ensure_loaded`` and cached
      on ``self._session``. Subsequent ``embed`` calls reuse it; it is **not**
      recreated per call.
    - The process-level provider cache in ``embeddings.store`` further ensures
      a single provider (and thus a single session) per MCP server process.
    - ``SessionOptions.enable_cpu_mem_arena`` remains **True** (ORT default):
      ORT does not expose a portable max-arena-bytes knob on the CPU EP that we
      can set without version-private config keys. Disabling the arena trades
      peak RSS for alloc churn; we keep the default and rely on input bounds +
      pre-flight refusal as the primary host-protection layer.
    """

    def __init__(
        self,
        spec: ArtifactSpec,
        *,
        max_attention_bytes: int | None = None,
    ) -> None:
        self._spec = spec
        self._session: object | None = None
        self._tokenizer: object | None = None
        self._max_attention_bytes = (
            DEFAULT_MAX_ATTENTION_BYTES if max_attention_bytes is None else max_attention_bytes
        )

    @property
    def dim(self) -> int:
        return self._spec.dim

    @property
    def model_id(self) -> str:
        """Stable model identity stored with vectors; reporting it never loads the artifact."""
        return MODEL_ID

    @property
    def max_attention_bytes(self) -> int:
        return self._max_attention_bytes

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> EmbeddingProvider | None:
        """Build from the opt-in env configuration, or ``None`` (clean degrade) if unset.

        Returns ``None`` when the artifact is not fully configured so callers fall
        back to non-semantic reinjection. Never fetches anything.
        """
        source = os.environ if env is None else env
        if _embeddings_disabled(source):
            return None
        model = source.get(ENV_MODEL)
        tokenizer = source.get(ENV_TOKENIZER)
        model_sha = source.get(ENV_MODEL_SHA256)
        tokenizer_sha = source.get(ENV_TOKENIZER_SHA256)
        if not (model and tokenizer and model_sha and tokenizer_sha):
            return None
        return cls(
            ArtifactSpec(Path(model), Path(tokenizer), model_sha, tokenizer_sha),
            max_attention_bytes=resolve_max_attention_bytes(source),
        )

    def verify_artifacts(self) -> None:
        """Fail closed if either pinned artifact is absent or hash-mismatched.

        SHA-256 only: never imports onnxruntime/tokenizers and never loads the
        ONNX session, so callers like ``doctor`` can validate the pinned model +
        tokenizer cheaply without paying the inference-runtime import cost.
        Raises :class:`EmbeddingArtifactError` on the first failure.
        """
        verify_artifact(self._spec.model_path, self._spec.model_sha256)
        verify_artifact(self._spec.tokenizer_path, self._spec.tokenizer_sha256)

    def _ensure_loaded(self) -> None:
        if self._session is not None:
            return
        # Fail closed on missing/corrupt artifacts before importing heavy libs.
        verify_artifact(self._spec.model_path, self._spec.model_sha256)
        verify_artifact(self._spec.tokenizer_path, self._spec.tokenizer_sha256)
        try:
            import onnxruntime as ort
            from tokenizers import Tokenizer
        except ImportError as exc:  # pragma: no cover - only without the embeddings extra
            raise EmbeddingArtifactError(
                "embeddings runtime missing; install the optional extra: pip install 'mcp-workbay-handoff[embeddings]'"
            ) from exc
        # Explicit SessionOptions so arena/settings are intentional, not accidental defaults.
        # Session is cached on self._session after both session + tokenizer succeed (below).
        session_options = ort.SessionOptions()
        # Keep CPU mem arena on (ORT default). No portable max-arena-bytes API on the
        # CPU EP; input caps + pre-flight budget are the host-protection controls.
        session_options.enable_cpu_mem_arena = True
        # CPU only; self-contained ONNX graph => no network, no trust_remote_code.
        session = ort.InferenceSession(
            str(self._spec.model_path),
            sess_options=session_options,
            providers=["CPUExecutionProvider"],
        )
        tokenizer = Tokenizer.from_file(str(self._spec.tokenizer_path))
        configure_tokenizer_bounds(tokenizer, max_length=MAX_SEQUENCE_TOKENS)
        # Commit both only after both succeed — never leave a half-loaded provider
        # (a tokenizer failure must re-raise cleanly on the next embed(), not assert).
        self._session = session
        self._tokenizer = tokenizer

    def _preflight_or_raise(self, batch: int, seq: int) -> None:
        estimated = estimate_attention_bytes(batch, seq)
        if estimated > self._max_attention_bytes:
            raise EmbeddingBudgetExceeded(
                estimated_bytes=estimated,
                ceiling_bytes=self._max_attention_bytes,
                batch=batch,
                seq=seq,
            )

    def _embed_sub_batch(self, texts: list[str]) -> np.ndarray:
        """Run inference for one sub-batch (already character-capped)."""
        self._ensure_loaded()
        session = self._session
        tokenizer = self._tokenizer
        assert session is not None and tokenizer is not None  # _ensure_loaded set these

        encodings = tokenizer.encode_batch(texts)  # type: ignore[attr-defined]
        input_ids = np.array([enc.ids for enc in encodings], dtype=np.int64)
        attention_mask = np.array([enc.attention_mask for enc in encodings], dtype=np.int64)
        # Defense in depth: refuse if tokenizer somehow exceeded the cap.
        if input_ids.shape[1] > MAX_SEQUENCE_TOKENS:
            raise EmbeddingBudgetExceeded(
                estimated_bytes=estimate_attention_bytes(input_ids.shape[0], input_ids.shape[1]),
                ceiling_bytes=self._max_attention_bytes,
                batch=int(input_ids.shape[0]),
                seq=int(input_ids.shape[1]),
            )
        # Budget check on the REAL padded shape, immediately before the allocating
        # call. Pricing it at MAX_SEQUENCE_TOKENS pre-tokenization would charge every
        # batch its worst case and refuse ordinary work (~3.0 GiB vs ~12 MB true cost).
        self._preflight_or_raise(int(input_ids.shape[0]), int(input_ids.shape[1]))
        # Supply only the inputs the exported graph actually declares (gte v1.5
        # may or may not take token_type_ids depending on the export).
        candidates = {
            "input_ids": input_ids,
            "attention_mask": attention_mask,
            "token_type_ids": np.zeros_like(input_ids),
        }
        declared = {spec.name for spec in session.get_inputs()}  # type: ignore[attr-defined]
        feed = {name: value for name, value in candidates.items() if name in declared}
        outputs = session.run(None, feed)  # type: ignore[attr-defined]
        pooled = cls_pool(np.asarray(outputs[0], dtype=np.float32))
        # The hash pin guarantees bytes, not hidden size — fail loud on a model
        # whose width != the contracted dim rather than emitting mis-width vectors.
        if pooled.shape[1] != self._spec.dim:
            raise EmbeddingArtifactError(f"model hidden size {pooled.shape[1]} != expected dim {self._spec.dim}")
        return l2_normalize(pooled)

    def embed(self, texts: list[str]) -> np.ndarray:
        """Return ``(len(texts), dim)`` float32, L2-normalized embeddings.

        Texts are character-capped, then processed in fixed-size sub-batches.
        Raises ``EmbeddingArtifactError`` if the artifact/runtime is unavailable,
        or ``EmbeddingBudgetExceeded`` if a sub-batch would exceed the attention
        ceiling.
        """
        if not texts:
            return np.zeros((0, self._spec.dim), dtype=np.float32)
        capped = [truncate_embed_text(t) for t in texts]
        parts: list[np.ndarray] = []
        for start in range(0, len(capped), EMBED_SUB_BATCH_SIZE):
            chunk = capped[start : start + EMBED_SUB_BATCH_SIZE]
            parts.append(self._embed_sub_batch(chunk))
        return np.vstack(parts)
