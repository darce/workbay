"""internal: hook source resolution — module-aware overlay probe (internal)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_module():
    path = Path(__file__).with_name("resolve_handoff_src.py")
    spec = importlib.util.spec_from_file_location("resolve_handoff_src", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _no_installed_dist(monkeypatch: pytest.MonkeyPatch) -> None:
    from importlib import metadata as importlib_metadata

    def fake_distribution(_name: str):
        raise importlib_metadata.PackageNotFoundError(_name)

    monkeypatch.setattr(importlib_metadata, "distribution", fake_distribution)


def _overlay_src(root: Path, runtime: str, package: str) -> Path:
    return root / runtime / "remote" / "packages" / package / "src"


def _with_module(src: Path, module: str = "workbay_handoff_mcp") -> Path:
    (src / module).mkdir(parents=True)
    return src


def test_source_repo_prefers_in_tree_over_overlay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mod = _load_module()
    in_tree = tmp_path / "packages" / "mcp-workbay-handoff" / "src"
    in_tree.mkdir(parents=True)
    _with_module(_overlay_src(tmp_path, ".workbay", "mcp-workbay-handoff"))
    _no_installed_dist(monkeypatch)

    assert mod.resolve_agent_handoff_src(str(tmp_path)) == str(in_tree)


def test_consumer_prefers_canonical_overlay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Consumer repo (no in-tree): the canonical ``.workbay/remote`` overlay
    that actually carries ``workbay_handoff_mcp`` resolves."""
    mod = _load_module()
    overlay = _with_module(_overlay_src(tmp_path, ".workbay", "mcp-workbay-handoff"))
    _no_installed_dist(monkeypatch)

    assert mod.resolve_agent_handoff_src(str(tmp_path)) == str(overlay)


def test_empty_canonical_overlay_does_not_shadow(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An existing-but-module-less canonical ``src`` must NOT resolve — returning
    it would leave the per-call hook ``import workbay_handoff_mcp`` failing.
    Fall through to in-tree instead of a path that cannot satisfy the import."""
    mod = _load_module()
    _overlay_src(tmp_path, ".workbay", "mcp-workbay-handoff").mkdir(parents=True)
    in_tree = tmp_path / "packages" / "mcp-workbay-handoff" / "src"  # not created (consumer)
    _no_installed_dist(monkeypatch)

    assert mod.resolve_agent_handoff_src(str(tmp_path)) == str(in_tree)
