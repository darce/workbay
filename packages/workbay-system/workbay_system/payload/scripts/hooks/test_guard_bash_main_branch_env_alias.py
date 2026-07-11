"""In-tree unit tests for the bash-main-write guard's env-bypass.

The env-side bypass reads only the canonical ``WORKBAY_ALLOW_BASH_MAIN_WRITE``;
the ancient ``ALT_ALLOW_BASH_MAIN_WRITE`` name remains a separate raw bypass.
These exercise ``_env_bypass_set`` / ``_bypass_request`` directly (the
subprocess-level test runs only in the materialized payload layout).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).parent
GUARD = HOOKS_DIR / "guard-bash-main-branch.py"


def _load_guard():
    spec = importlib.util.spec_from_file_location("guard_bash_main_branch_under_test", str(GUARD))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def guard(monkeypatch: pytest.MonkeyPatch):
    # Ensure the sibling _interp helper resolves the same way it does in
    # production (the hook inserts scripts/hooks on sys.path before the bypass
    # check runs).
    monkeypatch.syspath_prepend(str(HOOKS_DIR))
    return _load_guard()


def test_env_bypass_set_canonical(guard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKBAY_ALLOW_BASH_MAIN_WRITE", "1")
    assert guard._env_bypass_set("WORKBAY_ALLOW_BASH_MAIN_WRITE") is True


def test_env_bypass_set_unset_is_false(guard, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKBAY_ALLOW_BASH_MAIN_WRITE", raising=False)
    assert guard._env_bypass_set("WORKBAY_ALLOW_BASH_MAIN_WRITE") is False


def test_env_bypass_set_alt_legacy_is_raw(guard, monkeypatch: pytest.MonkeyPatch) -> None:
    """The ancient ALT_* name stays a raw (non-aliased) read."""
    monkeypatch.setenv("ALT_ALLOW_BASH_MAIN_WRITE", "1")
    assert guard._env_bypass_set("ALT_ALLOW_BASH_MAIN_WRITE") is True


def test_bypass_request_reports_canonical_env(
    guard, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A canonical env bypass is reported under the canonical var name."""
    monkeypatch.setenv("WORKBAY_ALLOW_BASH_MAIN_WRITE", "1")
    assert guard._bypass_request("sed -i s/x/y/ packages/foo/bar.py") == (
        "env",
        "WORKBAY_ALLOW_BASH_MAIN_WRITE",
    )
