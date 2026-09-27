"""Local, offline embedding provider for semantic compaction reinjection (implementation note).

This subpackage is imported only when the semantic-reinjection feature is enabled.
It depends on the optional ``embeddings`` extra (numpy/onnxruntime/tokenizers); the
core server never imports it on the default path.

``MODEL_PIN`` and ``ModelPin`` resolve through the stdlib-only pin module. Legacy
provider and numerical names load only on explicit attribute access.
"""

from __future__ import annotations

import importlib
from typing import Any

from workbay_handoff_mcp.embeddings.model_pin import MODEL_PIN, ModelPin

__all__ = [
    "EMBEDDING_DIM",
    "EMBED_SUB_BATCH_SIZE",
    "MAX_EMBED_CHARS",
    "MAX_SEQUENCE_TOKENS",
    "MODEL_PIN",
    "ArtifactSpec",
    "EmbeddingArtifactError",
    "EmbeddingBudgetExceeded",
    "EmbeddingProvider",
    "ModelPin",
    "cls_pool",
    "configure_tokenizer_bounds",
    "estimate_attention_bytes",
    "l2_normalize",
    "sha256_file",
    "truncate_embed_text",
    "verify_artifact",
]

_LAZY_LEGACY_EXPORTS = frozenset(
    {
        "EMBEDDING_DIM",
        "EMBED_SUB_BATCH_SIZE",
        "MAX_EMBED_CHARS",
        "MAX_SEQUENCE_TOKENS",
        "ArtifactSpec",
        "EmbeddingArtifactError",
        "EmbeddingBudgetExceeded",
        "EmbeddingProvider",
        "cls_pool",
        "configure_tokenizer_bounds",
        "estimate_attention_bytes",
        "l2_normalize",
        "sha256_file",
        "truncate_embed_text",
        "verify_artifact",
    }
)


def __getattr__(name: str) -> Any:
    if name not in _LAZY_LEGACY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    provider = importlib.import_module("workbay_handoff_mcp.embeddings.provider")
    value = getattr(provider, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
