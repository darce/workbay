"""Digest-pin SSOT for the gte-base-en-v1.5 int8 embedding artifacts (C1 / S1).

Both the embedding provider and the bootstrap provisioner read these constants.
Digest values were computed offline at pin time from HuggingFace revision
``a829fd0e060bb84554da0dfd354d0de0f7712b7f``; no network I/O at import.

This module also owns the only canonical embedding-revision digest (implementation note).
"""

from __future__ import annotations

import hashlib
import json
import re
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

REVISION_DOMAIN = "workbay.embedding.revision.v1"
SUPPORTED_RECIPE_VERSION = "workbay-cls-l2-f32-v1"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_FIELDS = (
    "schema_version",
    "model_sha256",
    "tokenizer_sha256",
    "source_repo",
    "source_revision",
    "dim",
    "recipe_version",
    "max_sequence_tokens",
    "max_embed_chars",
)


def _require_hex64(name: str, value: object) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise ValueError(f"{name} must be 64 lowercase hex characters")
    return value


def _require_nonempty_str(name: str, value: object) -> str:
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _require_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _require_positive_int(name: str, value: object) -> int:
    number = _require_int(name, value)
    if number <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return number


def canonical_revision(**kwargs: object) -> str:
    """SHA-256 of domain prefix + NUL + sorted compact ASCII JSON.

    Display names and local paths are ignored. Malformed hashes, empty source
    identity, non-integer limits, or an unsupported recipe raise.
    """
    missing = [name for name in _CANONICAL_FIELDS if name not in kwargs]
    if missing:
        raise TypeError(f"canonical_revision missing fields: {', '.join(missing)}")
    payload = {
        "schema_version": _require_int("schema_version", kwargs["schema_version"]),
        "model_sha256": _require_hex64("model_sha256", kwargs["model_sha256"]),
        "tokenizer_sha256": _require_hex64("tokenizer_sha256", kwargs["tokenizer_sha256"]),
        "source_repo": _require_nonempty_str("source_repo", kwargs["source_repo"]),
        "source_revision": _require_nonempty_str("source_revision", kwargs["source_revision"]),
        "dim": _require_positive_int("dim", kwargs["dim"]),
        "recipe_version": _require_nonempty_str("recipe_version", kwargs["recipe_version"]),
        "max_sequence_tokens": _require_positive_int("max_sequence_tokens", kwargs["max_sequence_tokens"]),
        "max_embed_chars": _require_positive_int("max_embed_chars", kwargs["max_embed_chars"]),
    }
    if payload["recipe_version"] != SUPPORTED_RECIPE_VERSION:
        raise ValueError(f"unsupported recipe_version: {payload['recipe_version']!r}")
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    preimage = REVISION_DOMAIN.encode("ascii") + b"\x00" + canonical_json.encode("utf-8")
    return hashlib.sha256(preimage).hexdigest()
