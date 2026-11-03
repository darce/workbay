"""Stdlib hashing/coverage plus import-lazy numerical codec helpers (implementation note S0).

``text_hash`` and ``classify_coverage_row`` stay numpy-free. Serialize/deserialize
import numpy only when a numerical codec call runs, never at module import.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Literal

from workbay_handoff_mcp.embeddings.model_pin import EMBEDDING_DIM

if TYPE_CHECKING:
    import numpy as np

_INT8_SCALE = 127.0

CoverageOutcome = Literal["empty", "skipped", "pending"]


def text_hash(text: str) -> str:
    """Stable SHA-256 hex of the concept text — the re-embed idempotency key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def classify_coverage_row(
    text: object,
    existing: tuple[str, str] | None,
    model_id: str | None,
) -> tuple[CoverageOutcome, str | None]:
    """Pure skip-gate shared by write-path classify and the doctor facet.

    Blank text is ``empty``. When ``model_id`` is set, a row is ``skipped``
    only on a matching ``(text_hash, model_id)`` pair — the production resume
    gate. When ``model_id`` is ``None`` (preview with no resolved provider),
    skip is hash-only so operators can still count remaining work without
    constructing a provider.
    """
    if text is None or not str(text).strip():
        return "empty", None
    new_hash = text_hash(str(text))
    if model_id is None:
        if existing is not None and existing[0] == new_hash:
            return "skipped", new_hash
        return "pending", new_hash
    if existing == (new_hash, model_id):
        return "skipped", new_hash
    return "pending", new_hash


def serialize_vector(vector: np.ndarray) -> bytes:
    """Canonical little-endian int8 bytes (``dim``), decoded as ``q / 127``.

    Quantization is ``q = clip(round(x * 127 / peak), -127, 127)`` where
    ``peak = max(|x|)`` (or 1 when the vector is empty). Dequant ``q / 127``
    recovers the *direction*; :func:`deserialize_vector` L2-renormalizes so
    ranking dots stay comparable across rows. A naive ``x * 127`` on a 768-d
    random unit vector only uses ~16 of 127 levels and drops cosine below
    the 0.999 rewrite bar; stretching to the int8 range keeps the on-disk
    dtype as dim signed int8 with no appended per-row scale float.
    """
    import numpy as np

    vec = np.asarray(vector, dtype=np.float64).reshape(-1)
    peak = float(np.max(np.abs(vec))) if vec.size else 0.0
    if peak <= 0.0:
        return np.zeros(vec.shape[0], dtype="<i1").tobytes()
    quantized = np.clip(np.rint(vec * (_INT8_SCALE / peak)), -_INT8_SCALE, _INT8_SCALE)
    return np.asarray(quantized, dtype="<i1").tobytes()


def deserialize_vector(blob: bytes, dim: int | None = None) -> np.ndarray:
    """Decode an int8 or legacy float32 payload.

    ``dim is None`` is the legacy-compatible default: a payload of
    ``EMBEDDING_DIM`` bytes decodes as signed int8 (``q / 127`` then L2
    renormalize); any other length decodes as little-endian float32 and
    never raises.

    When ``dim`` is given, format is discriminated strictly by payload
    length relative to that dimension: ``dim`` bytes is signed-int8 and
    ``dim * 4`` bytes is little-endian float32. Int8 dequant is ``q / 127``
    then L2-renormalized so stored vectors stay unit for ranking. Returns
    an owned, writable copy and rejects payloads of any other length.
    """
    import numpy as np

    data = bytes(blob)
    n = len(data)
    if dim is None:
        if n == EMBEDDING_DIM:
            return _l2_from_int8(data)
        return np.frombuffer(data, dtype="<f4").copy()
    if n == dim:
        return _l2_from_int8(data)
    if n == dim * 4:
        return np.frombuffer(data, dtype="<f4").copy()
    raise ValueError(
        f"embedding payload length {n} does not match int8 ({dim}) or float32 ({dim * 4}) encoding for dimension {dim}"
    )


def _l2_from_int8(data: bytes) -> np.ndarray:
    import numpy as np

    quantized = np.frombuffer(data, dtype="<i1")
    vec = quantized.astype(np.float64) / _INT8_SCALE
    norm = float(np.linalg.norm(vec))
    if norm > 0.0:
        vec = vec / norm
    return np.asarray(vec, dtype=np.float32)
