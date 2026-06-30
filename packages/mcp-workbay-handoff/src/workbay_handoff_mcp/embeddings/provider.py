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

The pure helpers (`l2_normalize`, `cls_pool`, `sha256_file`, `verify_artifact`)
depend only on numpy and are unit-tested without the model. onnxruntime and
tokenizers are imported lazily inside `_ensure_loaded` so importing this module
needs only the (optional) numpy dependency.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np

from workbay_handoff_mcp.embeddings.model_pin import EMBEDDING_DIM, MODEL_ID

MAX_SEQUENCE_TOKENS = 8192
_HASH_CHUNK = 1 << 20

# Opt-in artifact configuration (offline; operator points these at a locally
# installed model — never fetched by the provider).
ENV_MODEL = "WORKBAY_HANDOFF_EMBEDDING_MODEL"
ENV_TOKENIZER = "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER"
ENV_MODEL_SHA256 = "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256"
ENV_TOKENIZER_SHA256 = "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256"
ENV_EMBEDDINGS_DISABLED = "WORKBAY_HANDOFF_EMBEDDINGS_DISABLED"

_TRUE_VALUES = frozenset({"1", "true", "yes"})


def _embeddings_disabled(env: dict[str, str]) -> bool:
    raw = env.get(ENV_EMBEDDINGS_DISABLED)
    if raw is None:
        return False
    return raw.strip().lower() in _TRUE_VALUES


class EmbeddingArtifactError(RuntimeError):
    """Raised when the embedding artifact is missing, corrupt, hash-mismatched, or its runtime libs are absent."""


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
    """CLS-pooled, L2-normalized gte-base-en-v1.5 embeddings via onnxruntime (CPU)."""

    def __init__(self, spec: ArtifactSpec) -> None:
        self._spec = spec
        self._session: object | None = None
        self._tokenizer: object | None = None

    @property
    def dim(self) -> int:
        return self._spec.dim

    @property
    def model_id(self) -> str:
        """Stable model identity stored with vectors; reporting it never loads the artifact."""
        return MODEL_ID

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
        return cls(ArtifactSpec(Path(model), Path(tokenizer), model_sha, tokenizer_sha))

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
        # CPU only; self-contained ONNX graph => no network, no trust_remote_code.
        session = ort.InferenceSession(str(self._spec.model_path), providers=["CPUExecutionProvider"])
        tokenizer = Tokenizer.from_file(str(self._spec.tokenizer_path))
        tokenizer.enable_truncation(max_length=MAX_SEQUENCE_TOKENS)
        tokenizer.enable_padding()
        # Commit both only after both succeed — never leave a half-loaded provider
        # (a tokenizer failure must re-raise cleanly on the next embed(), not assert).
        self._session = session
        self._tokenizer = tokenizer

    def embed(self, texts: list[str]) -> np.ndarray:
        """Return ``(len(texts), dim)`` float32, L2-normalized embeddings.

        Raises ``EmbeddingArtifactError`` if the artifact/runtime is unavailable.
        """
        if not texts:
            return np.zeros((0, self._spec.dim), dtype=np.float32)
        self._ensure_loaded()
        session = self._session
        tokenizer = self._tokenizer
        assert session is not None and tokenizer is not None  # _ensure_loaded set these

        encodings = tokenizer.encode_batch(texts)  # type: ignore[attr-defined]
        input_ids = np.array([enc.ids for enc in encodings], dtype=np.int64)
        attention_mask = np.array([enc.attention_mask for enc in encodings], dtype=np.int64)
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
