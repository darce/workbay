"""Digest-pin SSOT for the gte-base-en-v1.5 int8 embedding artifacts (C1 / S1).

Both the embedding provider and the bootstrap provisioner read these constants.
Digest values were computed offline at pin time from HuggingFace revision
``a829fd0e060bb84554da0dfd354d0de0f7712b7f``; no network I/O at import.
"""

from __future__ import annotations

from dataclasses import dataclass

MODEL_ID = "gte-base-en-v1.5"
EMBEDDING_DIM = 768

SOURCE_REPO = "Alibaba-NLP/gte-base-en-v1.5"
SOURCE_REVISION = "a829fd0e060bb84554da0dfd354d0de0f7712b7f"

MODEL_FILENAME = "onnx/model_int8.onnx"
TOKENIZER_FILENAME = "tokenizer.json"

MODEL_SHA256 = "e7f6af7a9457d4fdd3af220c68e9a37325aad7c2d306bbc855fe0d019c326509"
TOKENIZER_SHA256 = "cb374d6bc042c22455946f4e09a89d29882a199fdaf8fb25be00dc8b8857a448"


@dataclass(frozen=True)
class ModelPin:
    """Pinned model identity, artifact paths, digests, and HF source revision."""

    model_id: str
    dim: int
    model_filename: str
    tokenizer_filename: str
    model_sha256: str
    tokenizer_sha256: str
    source_repo: str
    source_revision: str


MODEL_PIN = ModelPin(
    model_id=MODEL_ID,
    dim=EMBEDDING_DIM,
    model_filename=MODEL_FILENAME,
    tokenizer_filename=TOKENIZER_FILENAME,
    model_sha256=MODEL_SHA256,
    tokenizer_sha256=TOKENIZER_SHA256,
    source_repo=SOURCE_REPO,
    source_revision=SOURCE_REVISION,
)
