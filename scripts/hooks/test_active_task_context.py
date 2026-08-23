"""Unit coverage for the shared active-task resolver.

These cases pin the resolver behavior that both `_worktree_drift.py`
(PreToolUse blocker) and `advise-worktree-cd.py` (advisory hook) depend
on. Coverage focuses on identity-row parsing, fallback paths when MCP
exports are unavailable, and canonicalization of worktree paths.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _active_task_context as ctx  # noqa: E402


def _git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, timeout=5, check=True
    )
    return proc.stdout.strip()


def _dir_with_no_git_ancestor() -> Path:
    """Temp dir whose ancestors have no ``.git`` (true walk-to-root None).

    pytest's default root is often under ``/tmp``, and some hosts keep a real
    ``/tmp/.git``; walk-up correctly treats those paths as inside that repo.
    """
    for base in (Path("/var/tmp"), Path.home(), Path("/tmp")):
        if not base.is_dir():
            continue
        cursor = base.resolve(strict=False)
        blocked = False
        for _ in range(128):
        # Over-approximation is deliberate: bare .git existence is the SAFE
        # direction for this OPPOSITE contract (find a temp base with no repo
        # above it). Rejecting doubtful .git entries is more conservative about
        # isolation; do not harden this probe to the production ascent predicate.
            if (cursor / ".git").exists():
                blocked = True
                break
            parent = cursor.parent
            if parent == cursor:
                break
            cursor = parent
        if blocked:
            continue
        return Path(tempfile.mkdtemp(prefix="atc-none-", dir=str(base)))
    raise RuntimeError("no base directory free of .git ancestors for negative walk-up")


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


def test_primary_workspace_root_is_none_outside_git() -> None:
    isolated = _dir_with_no_git_ancestor()
    try:
        outside = isolated / "not-a-repo"
        outside.mkdir()
        assert ctx._primary_workspace_root(outside) is None
    finally:
        shutil.rmtree(isolated, ignore_errors=True)


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
    # Explicit call-site fallback when primary is unresolvable; otherwise the
    # real primary root from the identity helper (never a silent fabrication).
    expected_primary = ctx._primary_workspace_root(tmp_path) or str(
        tmp_path.resolve(strict=False)
    )
    assert result.primary_worktree == expected_primary


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


def test_load_active_task_returns_ambiguous_probe_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """ok:false ambiguity is a returned token, not a raise that hard-blocks edits."""
    payload = {"ok": False, "error": "Ambiguous active task for workspace path."}
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: _stub_exports(json.dumps(payload)))
    result = ctx._load_active_task(tmp_path)
    assert result.probe_error == "handoff_probe_ambiguous"
    assert result.task_ref is None
    assert result.resolution_note is not None
    assert "Ambiguous" in result.resolution_note


def test_load_active_task_returns_ambiguous_probe_error_for_no_active(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """ok:false 'No active task' shares the ambiguity token (recoverable state)."""
    payload = {"ok": False, "data": {"error": "No active task in handoff_state for workspace."}}
    monkeypatch.setattr(ctx, "_load_handoff_exports", lambda: _stub_exports(json.dumps(payload)))
    result = ctx._load_active_task(tmp_path)
    assert result.probe_error == "handoff_probe_ambiguous"
    assert result.task_ref is None
    assert result.resolution_note is not None
    assert "No active task" in result.resolution_note


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


def test_load_active_task_via_handoff_maps_locked_sqlite_to_probe_locked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """RELAND-REV-C-01: locked/busy OperationalError → handoff_probe_locked.

    Pins the production split in ``_load_active_task_via_handoff`` (not merely
    allowlist membership of a hand-built context).
    """
    monkeypatch.setattr(
        ctx,
        "_load_handoff_exports",
        lambda: _stub_exports(None, raises=sqlite3.OperationalError("database is locked")),
    )
    result = ctx._load_active_task_via_handoff(tmp_path)
    assert result.probe_error == "handoff_probe_locked"
    assert result.task_ref is None
    assert result.target_worktree is None


def test_load_active_task_via_handoff_maps_other_sqlite_to_probe_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """RELAND-REV-C-01 companion: non-lock OperationalError stays failed."""
    monkeypatch.setattr(
        ctx,
        "_load_handoff_exports",
        lambda: _stub_exports(
            None, raises=sqlite3.OperationalError("no such table: handoff_state")
        ),
    )
    result = ctx._load_active_task_via_handoff(tmp_path)
    assert result.probe_error == "handoff_probe_failed"
    assert result.task_ref is None


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


def test_ensure_handoff_import_paths_includes_workbay_protocol() -> None:
    """FIXB-FALLTHROUGH-LANDS-IN-ALLOWLISTED-FAILOPEN-01.

    Bare-interpreter hooks must put workbay-protocol/src on sys.path alongside
    mcp-workbay-handoff/src. Without protocol, ``import workbay_handoff_mcp``
    fails at ``from workbay_protocol.version import version_of`` and every probe
    collapses to allowlisted ``handoff_unavailable``.

    Mutation: stop inserting protocol src → pin RED when monorepo layout exists.
    """
    protocol = REPO_ROOT / "packages" / "workbay-protocol" / "src"
    handoff = REPO_ROOT / "packages" / "mcp-workbay-handoff" / "src"
    assert protocol.is_dir(), f"expected monorepo protocol tree at {protocol}"
    assert handoff.is_dir(), f"expected monorepo handoff tree at {handoff}"

    resolved_protocol = ctx._resolve_protocol_src()
    assert resolved_protocol is not None
    assert resolved_protocol.resolve() == protocol.resolve()

    # Re-run ensure against a clean view of membership (idempotent insert).
    before = list(sys.path)
    try:
        # Remove both so ensure must re-insert.
        for entry in (str(protocol.resolve()), str(handoff.resolve()), str(protocol), str(handoff)):
            while entry in sys.path:
                sys.path.remove(entry)
        ctx._ensure_handoff_import_paths()
        assert str(ctx._resolve_protocol_src()) in sys.path
        assert str(ctx._resolve_package_src()) in sys.path
    finally:
        sys.path[:] = before
        ctx._ensure_handoff_import_paths()


def test_ensure_handoff_import_paths_degrades_when_protocol_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Installed-consumer layout: no monorepo protocol tree → resolve None, no crash.

    Forces ``__file__`` into a temp tree with no packages/workbay-protocol/src at
    any candidate depth so the degrade path is pinned by name rather than hidden
    behind a conditional assertion [CARD-11][CARD-12].
    """
    fake_hook = tmp_path / "consumer" / "hooks" / "_active_task_context.py"
    fake_hook.parent.mkdir(parents=True)
    fake_hook.write_text("# consumer layout\n", encoding="utf-8")
    monkeypatch.setattr(ctx, "__file__", str(fake_hook))

    assert ctx._resolve_protocol_src() is None

    before = list(sys.path)
    try:
        ctx._ensure_handoff_import_paths()
        # Protocol leg must stay off sys.path; package leg may still insert its
        # fallback candidate string (even if that path does not exist).
        for entry in sys.path:
            if entry in before:
                continue
            assert "workbay-protocol" not in entry.replace("\\", "/"), (
                f"degrade must not invent a protocol sys.path entry; got {entry!r}"
            )
    finally:
        sys.path[:] = before
        # Restore real module __file__ side effects for later tests in-session.
        monkeypatch.undo()
        ctx._ensure_handoff_import_paths()


def test_resolve_protocol_src_no_indexerror_on_shallow_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RELAND-R2-PROTOCOL-SRC-PARENTS-INDEXERROR-ON-SHALLOW-CHECKOUT-01.

    Eager construction of parents[5]/[6] raised IndexError when the hook path
    had fewer than 7 parents (e.g. /repo/scripts/hooks → 4 parents). That
    crashed PreToolUse import and wedged every Edit/Write. Bounded candidate
    build must skip missing parent indices and return None without raising.
    """
    # Exactly 4 parents: hooks, scripts, repo, / — mirrors measured /repo layout.
    shallow = Path("/repo/scripts/hooks/_active_task_context.py")
    assert len(shallow.parents) <= 4, (
        f"pin requires at most 4 parents; got {len(shallow.parents)} for {shallow}"
    )
    monkeypatch.setattr(ctx, "__file__", str(shallow))
    assert ctx._resolve_protocol_src() is None
