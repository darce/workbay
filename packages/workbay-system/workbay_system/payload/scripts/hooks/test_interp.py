"""Tests for the shared hook interpreter helper (``_interp.py``).

Covers the two entry points used by the stack-touching hooks:
``resolve_deps_python`` (subprocess hooks, no re-exec) and
``ensure_deps_interpreter`` (in-process hooks, re-exec under the venv), plus
venv resolution (git toplevel, primary-checkout fallback, Windows layout).
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).parent
HELPER = HOOKS_DIR / "_interp.py"


def _load():
    spec = importlib.util.spec_from_file_location("_interp_under_test", str(HELPER))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def interp(monkeypatch: pytest.MonkeyPatch):
    mod = _load()
    # Default isolation: ignore the real primary checkout's .venv unless a
    # test opts in, so resolution is deterministic off the test's tmp dirs.
    monkeypatch.setattr(mod, "_primary_checkout_root", lambda: "")
    return mod


def _make_venv(root: Path, *, layout: tuple[str, str] = ("bin", "python")) -> Path:
    venv_py = root / ".venv" / layout[0] / layout[1]
    venv_py.parent.mkdir(parents=True)
    venv_py.write_text("")
    return venv_py


# The probe spans the full stack the hooks import, not just pydantic.
_REQUIRED_DEPS = ("pydantic", "fastmcp", "workbay_protocol")


def _deps_present(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake(name, *a, **k):
        assert name in _REQUIRED_DEPS  # pin the probed dependency set
        return object()

    monkeypatch.setattr(importlib.util, "find_spec", fake)


def _deps_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake(name, *a, **k):
        assert name in _REQUIRED_DEPS  # short-circuits on pydantic
        return None

    monkeypatch.setattr(importlib.util, "find_spec", fake)


def _deps_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    """pydantic present but a deeper stack dep (fastmcp) absent.

    REV-A-HOOK-DEPS-01: the old pydantic-only probe wrongly treated this host
    as deps-bearing; the full-stack probe must still heal to the venv.
    """

    def fake(name, *a, **k):
        assert name in _REQUIRED_DEPS
        return object() if name == "pydantic" else None

    monkeypatch.setattr(importlib.util, "find_spec", fake)


def _ban_execv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        os, "execv", lambda *a: pytest.fail("must not re-exec on this path")
    )


# -- _venv_python ----------------------------------------------------------


def test_venv_python_absent_returns_empty(interp, tmp_path: Path) -> None:
    assert interp._venv_python(str(tmp_path)) == ""


def test_venv_python_present_returns_path(interp, tmp_path: Path) -> None:
    venv_py = _make_venv(tmp_path)
    assert interp._venv_python(str(tmp_path)) == str(venv_py)


def test_venv_python_no_repo_root_returns_empty(interp) -> None:
    assert interp._venv_python("") == ""


def test_venv_python_finds_windows_layout(interp, tmp_path: Path) -> None:
    win_py = _make_venv(tmp_path, layout=("Scripts", "python.exe"))
    assert interp._venv_python(str(tmp_path)) == str(win_py)


def test_venv_python_falls_back_to_primary_checkout(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # git toplevel (a linked worktree) has NO .venv; the primary checkout does.
    worktree = tmp_path / "wt"
    worktree.mkdir()
    primary = tmp_path / "primary"
    primary_py = _make_venv(primary)
    monkeypatch.setattr(interp, "_git_repo_root", lambda: str(worktree))
    monkeypatch.setattr(interp, "_primary_checkout_root", lambda: str(primary))
    assert interp._venv_python() == str(primary_py)


# -- resolve_deps_python (subprocess hooks; MUST NOT re-exec) --------------


def test_resolve_deps_python_returns_current_when_deps_present(
    interp, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deps_present(monkeypatch)
    _ban_execv(monkeypatch)
    assert interp.resolve_deps_python() == sys.executable


def test_resolve_deps_python_prefers_venv_when_deps_missing(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _deps_missing(monkeypatch)
    _ban_execv(monkeypatch)  # per-event hooks must never re-exec
    venv_py = _make_venv(tmp_path)
    monkeypatch.setattr(interp, "_git_repo_root", lambda: str(tmp_path))
    assert interp.resolve_deps_python() == str(venv_py)


def test_resolve_deps_python_falls_back_when_no_venv(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _deps_missing(monkeypatch)
    _ban_execv(monkeypatch)
    monkeypatch.setattr(interp, "_git_repo_root", lambda: str(tmp_path))
    assert interp.resolve_deps_python() == sys.executable


def test_resolve_deps_python_heals_when_stack_dep_missing(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # REV-A-HOOK-DEPS-01: pydantic present but fastmcp absent must still heal
    # to the venv -- the old pydantic-only probe wrongly stayed on this host.
    _deps_partial(monkeypatch)
    _ban_execv(monkeypatch)
    venv_py = _make_venv(tmp_path)
    monkeypatch.setattr(interp, "_git_repo_root", lambda: str(tmp_path))
    assert interp.resolve_deps_python() == str(venv_py)


# -- ensure_deps_interpreter (in-process hooks; re-exec) -------------------


def test_ensure_noop_when_deps_present(
    interp, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deps_present(monkeypatch)
    calls: list = []
    monkeypatch.setattr(os, "execv", lambda *a: calls.append(a))
    interp.ensure_deps_interpreter()
    assert calls == []


def test_ensure_noop_when_sentinel_set(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _deps_missing(monkeypatch)
    monkeypatch.setenv("WORKBAY_HOOK_REEXEC", "1")
    monkeypatch.setattr(interp, "_venv_python", lambda *a, **k: str(tmp_path / "py"))
    calls: list = []
    monkeypatch.setattr(os, "execv", lambda *a: calls.append(a))
    interp.ensure_deps_interpreter()
    assert calls == []


def test_ensure_reexecs_to_venv_when_deps_missing(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _deps_missing(monkeypatch)
    # The code-under-test sets WORKBAY_HOOK_REEXEC before os.execv. Patch
    # os.environ to a throwaway copy so that mutation cannot leak into the
    # pytest process (monkeypatch.delenv would NOT undo a code-set value).
    fake_env = dict(os.environ)
    fake_env.pop("WORKBAY_HOOK_REEXEC", None)
    monkeypatch.setattr(os, "environ", fake_env)
    venv_py = _make_venv(tmp_path)
    monkeypatch.setattr(interp, "_venv_python", lambda *a, **k: str(venv_py))
    captured: dict = {}
    monkeypatch.setattr(
        os, "execv", lambda path, argv: captured.update(path=path, argv=argv)
    )
    interp.ensure_deps_interpreter()
    assert captured["path"] == str(venv_py)
    assert captured["argv"][0] == str(venv_py)
    assert captured["argv"][1:] == sys.argv
    # sentinel set (on the throwaway env) before re-exec so the child can't loop
    assert fake_env.get("WORKBAY_HOOK_REEXEC") == "1"


def test_ensure_reexecs_when_stack_dep_missing(
    interp, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # REV-A-HOOK-DEPS-01: in-process hooks must also heal when pydantic is
    # present but a deeper stack dep (fastmcp) is not.
    _deps_partial(monkeypatch)
    fake_env = dict(os.environ)
    fake_env.pop("WORKBAY_HOOK_REEXEC", None)
    monkeypatch.setattr(os, "environ", fake_env)
    venv_py = _make_venv(tmp_path)
    monkeypatch.setattr(interp, "_venv_python", lambda *a, **k: str(venv_py))
    captured: dict = {}
    monkeypatch.setattr(
        os, "execv", lambda path, argv: captured.update(path=path, argv=argv)
    )
    interp.ensure_deps_interpreter()
    assert captured["path"] == str(venv_py)
    assert fake_env.get("WORKBAY_HOOK_REEXEC") == "1"


def test_ensure_no_reexec_when_venv_is_current_interpreter(
    interp, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deps_missing(monkeypatch)
    monkeypatch.setattr(interp, "_venv_python", lambda *a, **k: sys.executable)
    calls: list = []
    monkeypatch.setattr(os, "execv", lambda *a: calls.append(a))
    interp.ensure_deps_interpreter()
    assert calls == []  # abspath(candidate) == abspath(sys.executable) -> skip


def test_ensure_no_reexec_when_no_venv(
    interp, monkeypatch: pytest.MonkeyPatch
) -> None:
    _deps_missing(monkeypatch)
    monkeypatch.setattr(interp, "_venv_python", lambda *a, **k: "")
    calls: list = []
    monkeypatch.setattr(os, "execv", lambda *a: calls.append(a))
    interp.ensure_deps_interpreter()
    assert calls == []


# -- consumer import surface (the silent-no-op risk if _interp is renamed) --


def test_consumer_import_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    # Hooks do `from _interp import ...` relying on sys.path[0] = hooks dir.
    monkeypatch.syspath_prepend(str(HOOKS_DIR))
    sys.modules.pop("_interp", None)
    from _interp import ensure_deps_interpreter, resolve_deps_python

    assert callable(ensure_deps_interpreter)
    assert callable(resolve_deps_python)


# -- resolve_env_alias (canonical-only after implementation note B1) -----------------
#
# The per-tool-call payload hooks read operator-set WORKBAY_* overrides. The
# stdlib-fallback path is exercised directly (sys.modules patch) so the
# assertions do not depend on whether the installed workbay_protocol copy
# already carries the canonical resolver.


def _force_stdlib_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``from workbay_protocol import ...`` raise so the shim's stdlib
    fallback (the bare-python3 / pre-re-exec path) is taken."""
    monkeypatch.setitem(sys.modules, "workbay_protocol", None)


def test_resolve_env_alias_reads_canonical(interp, monkeypatch: pytest.MonkeyPatch) -> None:
    _force_stdlib_fallback(monkeypatch)
    monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", "/canonical")
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR") == "/canonical"


def test_resolve_env_alias_default_when_unset(interp, monkeypatch: pytest.MonkeyPatch) -> None:
    _force_stdlib_fallback(monkeypatch)
    monkeypatch.delenv("WORKBAY_HANDOFF_STATE_DIR", raising=False)
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR") is None
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR", "fallback") == "fallback"


def test_resolve_env_alias_blank_is_unset(interp, monkeypatch: pytest.MonkeyPatch) -> None:
    _force_stdlib_fallback(monkeypatch)
    monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", "   ")
    # blank canonical is treated as unset, so the default is returned
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR") is None
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR", "d") == "d"


def test_resolve_env_alias_delegates_to_workbay_protocol(
    interp, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the stack is importable (re-exec hooks), the shim delegates to the
    canonical resolver rather than the stdlib fallback."""
    import types

    sentinel = types.ModuleType("workbay_protocol")
    captured: dict[str, object] = {}

    def _canonical(canonical, *, env=None, default=None):
        captured["canonical"] = canonical
        captured["default"] = default
        return "delegated"

    sentinel.resolve_env_alias = _canonical  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "workbay_protocol", sentinel)
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_HARNESS", "d") == "delegated"
    assert captured == {"canonical": "WORKBAY_HANDOFF_HARNESS", "default": "d"}


def test_resolve_env_alias_strips_returned_value(
    interp, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CAN-3f8c1a5d-8: the returned value is whitespace-stripped on the
    canonical read, matching the canonical resolver's _non_empty parity the
    docstring claims. A regression dropping .strip() would otherwise pass the
    rest of the suite (all other cases use whitespace-free values)."""
    _force_stdlib_fallback(monkeypatch)
    monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", "  /canonical  ")
    assert interp.resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR") == "/canonical"
