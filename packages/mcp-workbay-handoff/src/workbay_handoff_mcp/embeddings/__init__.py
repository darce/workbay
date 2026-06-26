"""Local, offline embedding provider for semantic compaction reinjection (implementation note).

This subpackage is imported only when the semantic-reinjection feature is enabled.
It depends on the optional ``embeddings`` extra (numpy/onnxruntime/tokenizers); the
core server never imports it on the default path.
"""

from workbay_handoff_mcp.embeddings.provider import (
    EMBEDDING_DIM,
    ArtifactSpec,
    EmbeddingArtifactError,
    EmbeddingProvider,
    cls_pool,
    l2_normalize,
    sha256_file,
    verify_artifact,
)

__all__ = [
    "EMBEDDING_DIM",
    "ArtifactSpec",
    "EmbeddingArtifactError",
    "EmbeddingProvider",
    "cls_pool",
    "l2_normalize",
    "sha256_file",
    "verify_artifact",
]
