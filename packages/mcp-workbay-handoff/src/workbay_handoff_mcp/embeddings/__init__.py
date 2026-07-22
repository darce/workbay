"""Local, offline embedding provider for semantic compaction reinjection (implementation note).

This subpackage is imported only when the semantic-reinjection feature is enabled.
It depends on the optional ``embeddings`` extra (numpy/onnxruntime/tokenizers); the
core server never imports it on the default path.
"""

from workbay_handoff_mcp.embeddings.model_pin import MODEL_PIN, ModelPin
from workbay_handoff_mcp.embeddings.provider import (
    EMBEDDING_DIM,
    EMBED_SUB_BATCH_SIZE,
    MAX_EMBED_CHARS,
    MAX_SEQUENCE_TOKENS,
    ArtifactSpec,
    EmbeddingArtifactError,
    EmbeddingBudgetExceeded,
    EmbeddingProvider,
    cls_pool,
    configure_tokenizer_bounds,
    estimate_attention_bytes,
    l2_normalize,
    sha256_file,
    truncate_embed_text,
    verify_artifact,
)

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
