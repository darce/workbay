"""Unit coverage for the shared active-task resolver.

These cases pin the resolver behavior that both `_worktree_drift.py`
(PreToolUse blocker) and `advise-worktree-cd.py` (advisory hook) depend
on. Coverage focuses on identity-row parsing, fallback paths when MCP
exports are unavailable, and canonicalization of worktree paths.
"""

from __future__ import annotations

import importlib
import inspect
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _active_task_context as ctx  # noqa: E402
from _worktree_drift import evaluate_payload  # noqa: E402

# In-tree source pins so the real resolver (implementation note ``strict=`` param) is used
# rather than a stale editable-install copy. ``parents[5]`` is the repo
# ``packages/`` dir from this shipped hook-test location.
PACKAGES_DIR = Path(__file__).resolve().parents[5]
HANDOFF_SRC = PACKAGES_DIR / "mcp-workbay-handoff" / "src"
PROTOCOL_SRC = PACKAGES_DIR / "workbay-protocol" / "src"
_WB_PREFIXES = ("workbay_protocol", "workbay_handoff_mcp")


def _is_wb_module(name: str) -> bool:
    return any(name == prefix or name.startswith(f"{prefix}.") for prefix in _WB_PREFIXES)


@pytest.fixture
def pinned_handoff() -> Any:
    """Pin the in-tree handoff + protocol sources ahead of any stale editable
    install, drop cached workbay modules so the worktree source wins on the next
    import, then restore at teardown. Skips when the in-tree strict-capable hook
    resolver is unavailable (shipped/consumer layout has no sibling ``src``)."""
    saved_path = list(sys.path)
    saved_modules = {name: mod for name, mod in sys.modules.items() if _is_wb_module(name)}
    for src in (PROTOCOL_SRC, HANDOFF_SRC):
        if src.is_dir() and str(src) not in sys.path:
            sys.path.insert(0, str(src))
    for name in list(sys.modules):
        if _is_wb_module(name):
            del sys.modules[name]
    try:
        shared_primitives = importlib.import_module("workbay_handoff_mcp.shared_primitives")
        if "strict" not in inspect.signature(
            shared_primitives.resolve_active_task_ref_for_hook
        ).parameters:
            pytest.skip("in-tree strict-capable hook resolver not resolvable on sys.path")
        yield
    finally:
        sys.path[:] = saved_path
        for name in list(sys.modules):
            if _is_wb_module(name):
                del sys.modules[name]
        sys.modules.update(saved_modules)


def _seed_ambiguous_feature_rows(repo: Path) -> None:
    """Configure the in-tree runtime to ``repo`` and insert two live
    feature-branch handoff rows so the workspace→task resolver is ambiguous
    (the shape that produced 2026-07-02 UnresolvedTaskContextError blocks)."""
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.shared_schema import _open_db_connection

    configure_runtime(RuntimeConfig.for_repo(repo))
    conn = _open_db_connection()
    try:
        for task_ref, branch, worktree, updated in (
            ("internal", "feature/a", repo / "wt-a", "2026-07-02 01:00:00"),
            ("internal", "feature/b", repo / "wt-b", "2026-07-02 02:00:00"),
        ):
            conn.execute(
                """
                INSERT INTO handoff_state (
                    task_ref, objective, focus, status, target_branch,
                    target_worktree_path, revision, updated_at, updated_by,
                    updated_branch, updated_commit_sha
                ) VALUES (?, ?, ?, 'in_progress', ?, ?, 0, ?, 'tester', 'main', 'abc123')
                """,
                (task_ref, f"obj-{task_ref}", f"focus-{task_ref}", branch, str(worktree), updated),
            )
        conn.commit()
    finally:
        conn.close()


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    return repo


def _git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, timeout=5, check=True
    )
    return proc.stdout.strip()


def _make_repo_with_feature_worktree(tmp_path: Path) -> tuple[Path, Path]:
    primary = tmp_path / "primary"
    primary.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(primary)], check=True)
    _git("config", "user.email", "test@example.com", cwd=primary)
    _git("config", "user.name", "Test", cwd=primary)
    (primary / "README.md").write_text("seed\n", encoding="utf-8")
    _git("add", "README.md", cwd=primary)
    _git("commit", "-q", "-m", "init", cwd=primary)
    feature = tmp_path / "primary-feature"
    _git("worktree", "add", "-b", "feature/x", str(feature), cwd=primary)
    return primary, feature


def test_canonical_target_worktree_returns_none_for_empty() -> None:
    assert ctx._canonical_target_worktree(None) is None
    assert ctx._canonical_target_worktree("") is None


def test_canonical_target_worktree_resolves_relative_segments(tmp_path: Path) -> None:
    target = tmp_path / "a" / ".." / "a" / "wt"
    expected = str((tmp_path / "a" / "wt").resolve(strict=False))
    assert ctx._canonical_target_worktree(str(target)) == expected


def test_canonical_target_worktree_expands_user(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    assert ctx._canonical_target_worktree("~/foo").endswith("/foo")


def test_primary_workspace_root_returns_primary_for_linked_worktree(tmp_path: Path) -> None:
    primary, feature = _make_repo_with_feature_worktree(tmp_path)
    assert ctx._primary_workspace_root(feature) == str(primary.resolve(strict=False))
    assert ctx._primary_workspace_root(primary) == str(primary.resolve(strict=False))


def test_primary_workspace_root_falls_back_to_resolved_root_outside_git(tmp_path: Path) -> None:
    outside = tmp_path / "not-a-repo"
    outside.mkdir()
    assert ctx._primary_workspace_root(outside) == str(outside.resolve(strict=False))


def test_workspace_root_returns_git_toplevel_or_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    primary, _ = _make_repo_with_feature_worktree(tmp_path)
    monkeypatch.chdir(primary)
    assert ctx._workspace_root() == primary

    bare = tmp_path / "bare"
    bare.mkdir()
    monkeypatch.chdir(bare)
    # Outside any git repo, falls through to cwd.
    result = ctx._workspace_root()
    assert result == bare or result == Path.cwd()


def test_load_active_task_falls_back_when_handoff_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: None)
    result = ctx._load_active_task(tmp_path)
    assert result.task_ref is None
    assert result.target_worktree is None
    assert result.target_branch is None
    assert result.primary_worktree == str(tmp_path.resolve(strict=False))


def _stub_exports(get_state_returns: Any, *, raises: BaseException | None = None) -> tuple[Any, Any, Any, type[BaseException]]:
    class _Runtime:
        def __init__(self, workspace_root: Path) -> None:
            self.workspace_root = str(workspace_root.resolve(strict=False))

        @classmethod
        def for_repo(cls, workspace_root: Path) -> "_Runtime":
            return cls(workspace_root)

    def _configure(_runtime: Any) -> None:
        return None

    class _Unresolved(ValueError):
        pass

    def _get_state(*, sections: str = "identity") -> Any:
        if raises is not None:
            raise raises
        return get_state_returns

    return (_Runtime, _configure, _get_state, _Unresolved)


def test_load_active_task_parses_identity_row(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "wt-feature"
    payload = {
        "ok": True,
        "data": {
            "active": {
                "task_ref": "internal",
                "target_worktree_path": str(target),
                "target_branch": "feature/internal-35",
            }
        },
    }
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: _stub_exports(json.dumps(payload)))

    result = ctx._load_active_task(tmp_path)
    assert result.task_ref == "internal"
    assert result.target_worktree == str(target)
    assert result.target_branch == "feature/internal-35"
    assert result.primary_worktree == str(tmp_path.resolve(strict=False))


def test_load_active_task_accepts_dict_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    payload = {"ok": True, "data": {"active": {"task_ref": "internal"}}}
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: _stub_exports(payload))
    result = ctx._load_active_task(tmp_path)
    assert result.task_ref == "internal"
    assert result.target_worktree is None


def test_load_active_task_returns_empty_for_invalid_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: _stub_exports("not-json"))
    result = ctx._load_active_task(tmp_path)
    assert result.task_ref is None
    assert result.target_worktree is None
    assert result.primary_worktree == str(tmp_path.resolve(strict=False))


def _scrub_resolver_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKBAY_HANDOFF_ACTIVE_TASK", raising=False)
    monkeypatch.delenv("WORKBAY_LANE_ID", raising=False)
    monkeypatch.setenv("WORKBAY_HANDOFF_EMBEDDINGS_DISABLED", "1")


def test_load_active_task_falls_back_on_ambiguous_task(
    pinned_handoff: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """implementation note current behavior (real resolver, not a stub): with the fallback
    kill-switch UNSET, an ambiguous multi-row DB resolves to the most-recently
    updated active row and returns a ``resolution_note`` — it does NOT raise.

    Replaces the prior false-coverage test that stubbed ``_load_handoff_exports``
    so the real resolver never ran yet asserted ambiguity RAISED (the opposite of
    the shipped fallback)."""
    _scrub_resolver_env(monkeypatch)
    monkeypatch.delenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", raising=False)
    repo = _init_repo(tmp_path)
    _seed_ambiguous_feature_rows(repo)

    result = ctx._load_active_task(repo)

    assert result.task_ref == "internal", "fallback picks the most-recently-updated row"
    assert result.resolution_note is not None
    assert "ambiguous active task" in result.resolution_note.lower()


def test_load_active_task_raises_on_ambiguous_task_with_killswitch(
    pinned_handoff: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The ``WORKBAY_GUARD_AMBIGUITY_FALLBACK=0`` kill-switch restores strict
    behavior: the same ambiguous DB raises loudly (real resolver)."""
    _scrub_resolver_env(monkeypatch)
    monkeypatch.setenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", "0")
    repo = _init_repo(tmp_path)
    _seed_ambiguous_feature_rows(repo)

    with pytest.raises(ValueError, match="Ambiguous active task"):
        ctx._load_active_task(repo)


def test_worktree_drift_guard_blocks_on_ambiguity_with_killswitch(
    pinned_handoff: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End-to-end guard behavior with the kill-switch set: the drift guard
    resolves through the real resolver, which raises, and the guard BLOCKS
    (outcome='block') rather than allowing a fallback."""
    _scrub_resolver_env(monkeypatch)
    monkeypatch.setenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", "0")
    monkeypatch.delenv("ALT_ALLOW_WORKTREE_DRIFT", raising=False)
    repo = _init_repo(tmp_path)
    _seed_ambiguous_feature_rows(repo)

    payload = {"toolName": "Edit", "toolInput": {"file_path": str(repo / "docs" / "x.md")}}
    decision = evaluate_payload(payload, workspace_root=repo, active_task=None)

    assert decision is not None
    assert decision.outcome == "block"
    assert "UnresolvedTaskContextError" in (decision.reason or "")


def test_worktree_drift_guard_falls_back_on_ambiguity_default(
    pinned_handoff: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The kill-switch's counterpart: with the fallback default (unset), the
    drift guard resolves the ambiguity through the real resolver and ALLOWS with
    outcome='fallback'."""
    _scrub_resolver_env(monkeypatch)
    monkeypatch.delenv("WORKBAY_GUARD_AMBIGUITY_FALLBACK", raising=False)
    monkeypatch.delenv("ALT_ALLOW_WORKTREE_DRIFT", raising=False)
    repo = _init_repo(tmp_path)
    _seed_ambiguous_feature_rows(repo)

    payload = {"toolName": "Edit", "toolInput": {"file_path": str(repo / "docs" / "x.md")}}
    decision = evaluate_payload(payload, workspace_root=repo, active_task=None)

    assert decision is not None
    assert decision.outcome == "fallback"


def test_load_active_task_raises_on_no_active_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    payload = {"ok": False, "data": {"error": "No active task in handoff_state for workspace."}}
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: _stub_exports(json.dumps(payload)))
    with pytest.raises(ValueError, match="No active task"):
        ctx._load_active_task(tmp_path)


def test_load_active_task_swallows_runtime_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        ctx,
        "_load_handoff_exports",
        lambda: _stub_exports(None, raises=RuntimeError("connection broken")),
    )
    result = ctx._load_active_task(tmp_path)
    # Generic exceptions fall back to an empty context (advisory hook stays silent).
    assert result.task_ref is None
    assert result.target_worktree is None


def test_load_active_task_propagates_unresolved_task_context_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    exports = _stub_exports(None)
    Runtime, configure, get_state, Unresolved = exports

    def _raise(*, sections: str = "identity") -> Any:
        raise Unresolved("ambiguous")

    monkeypatch.setattr(
        ctx,
        "_load_handoff_exports",
        lambda: (Runtime, configure, _raise, Unresolved),
    )
    with pytest.raises(Unresolved):
        ctx._load_active_task(tmp_path)


def test_load_handoff_exports_returns_none_when_module_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    def _raise(*_args: Any, **_kwargs: Any) -> Any:
        raise ImportError("workbay_handoff_mcp not installed")

    monkeypatch.setattr(importlib, "import_module", _raise)
    assert ctx._load_handoff_exports() is None


def test_load_handoff_exports_returns_none_when_attributes_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    stub = SimpleNamespace()  # no RuntimeConfig / configure_runtime / get_handoff_state
    monkeypatch.setattr(importlib, "import_module", lambda _name: stub)
    assert ctx._load_handoff_exports() is None
