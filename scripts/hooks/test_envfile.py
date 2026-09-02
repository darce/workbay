"""S3 tests for ``.workbay/embedding.env`` loader (internal)."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).parent
ENVFILE = HOOKS_DIR / "_envfile.py"
REINJECT = HOOKS_DIR / "reinject-context.py"


def _load_envfile():
    spec = importlib.util.spec_from_file_location("_envfile_under_test", str(ENVFILE))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_reinject():
    spec = importlib.util.spec_from_file_location("reinject_context_under_test", str(REINJECT))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_load_embedding_env_set_if_unset(tmp_path, monkeypatch):
    envfile = _load_envfile()
    for key in (
        "WORKBAY_HANDOFF_EMBEDDING_MODEL",
        "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER",
        "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256",
        "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256",
        "WORKBAY_REINJECT_SEMANTIC",
    ):
        monkeypatch.delenv(key, raising=False)

    workbay = tmp_path / ".workbay"
    workbay.mkdir()
    (workbay / "embedding.env").write_text(
        "\n".join(
            [
                "WORKBAY_HANDOFF_EMBEDDING_MODEL=/cache/model.onnx",
                "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER=/cache/tokenizer.json",
                "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256=" + "a" * 64,
                "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256=" + "b" * 64,
                "WORKBAY_REINJECT_SEMANTIC=1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert envfile.load_embedding_env(tmp_path) is True
    assert os.environ["WORKBAY_HANDOFF_EMBEDDING_MODEL"] == "/cache/model.onnx"
    assert os.environ["WORKBAY_REINJECT_SEMANTIC"] == "1"


def test_load_embedding_env_never_clobbers_operator_env(tmp_path, monkeypatch):
    envfile = _load_envfile()
    monkeypatch.setenv("WORKBAY_REINJECT_SEMANTIC", "0")
    workbay = tmp_path / ".workbay"
    workbay.mkdir()
    (workbay / "embedding.env").write_text("WORKBAY_REINJECT_SEMANTIC=1\n", encoding="utf-8")

    envfile.load_embedding_env(tmp_path)
    assert os.environ["WORKBAY_REINJECT_SEMANTIC"] == "0"


def test_missing_env_file_is_noop(tmp_path, monkeypatch):
    envfile = _load_envfile()
    monkeypatch.delenv("WORKBAY_REINJECT_SEMANTIC", raising=False)
    assert envfile.load_embedding_env(tmp_path) is False
    assert "WORKBAY_REINJECT_SEMANTIC" not in os.environ


@pytest.mark.skipif(
    not pytest.importorskip("workbay_handoff_mcp", reason="handoff optional in hook tests"),
    reason="needs handoff embeddings",
)
def test_provisioned_env_enables_semantic_and_provider(tmp_path, monkeypatch):
    pytest.importorskip("numpy")
    envfile = _load_envfile()
    reinject = _load_reinject()
    from workbay_handoff_mcp.embeddings import EmbeddingProvider

    for key in (
        "WORKBAY_HANDOFF_EMBEDDING_MODEL",
        "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER",
        "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256",
        "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256",
        "WORKBAY_REINJECT_SEMANTIC",
    ):
        monkeypatch.delenv(key, raising=False)

    workbay = tmp_path / ".workbay"
    workbay.mkdir()
    (workbay / "embedding.env").write_text(
        "\n".join(
            [
                "WORKBAY_HANDOFF_EMBEDDING_MODEL=/x/model.onnx",
                "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER=/x/tokenizer.json",
                "WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256=" + "a" * 64,
                "WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256=" + "b" * 64,
                "WORKBAY_REINJECT_SEMANTIC=1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    envfile.load_embedding_env(tmp_path)
    assert reinject._semantic_enabled() is True
    assert EmbeddingProvider.from_env() is not None
