"""SessionStart subprocess caps for reinject-context.py."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType

import pytest

HOOKS_DIR = Path(__file__).resolve().parent
REPO_ROOT = next(parent for parent in HOOKS_DIR.parents if (parent / "scripts" / "hooks" / "_interp.py").is_file())


def _load_interp():
    spec = importlib.util.spec_from_file_location("test_interp_origin_guard_module", HOOKS_DIR / "_interp.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# -- one subprocess deadline per hook invocation (finding 28155, RES-02/RES-03) --

# reinject-context.py registers no SessionStart timeout, so the harness default
# applies. The reserve covers interpreter starts, the workbay_handoff_mcp import,
# the handoff.db reads and the emit; reinject-context.py states the same reserve.
_SESSION_START_DEFAULT_TIMEOUT_SECONDS = 60
_REINJECT_OWN_WORK_RESERVE_SECONDS = 15
_HOOK_DEADLINE_ENV = "WORKBAY_HOOK_SUBPROCESS_DEADLINE"


class _Reexec(Exception):
    """Raised by the fake ``os.execv``: the process image would be replaced here."""


def _isolated_env(monkeypatch, **values: str) -> dict[str, str]:
    dropped = {"WORKBAY_HOOK_REEXEC", _HOOK_DEADLINE_ENV, "CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT"}
    env = {key: value for key, value in os.environ.items() if key not in dropped}
    env.update(values)
    monkeypatch.setattr(os, "environ", env)
    return env


def _fake_clock(monkeypatch, start: float) -> dict[str, float]:
    clock = {"now": start}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    return clock


def _wait_kind(cmd) -> str:
    if cmd[0] == "git":
        return "git"
    if "names = json.load(sys.stdin)" in str(cmd[2]):
        return "origin_probe"
    if "importlib.metadata as m" in str(cmd[2]):
        return "version_skew"
    raise AssertionError(f"unexpected subprocess on the SessionStart path: {cmd!r}")


def _subprocess_answer(cmd, kwargs, checkout: Path) -> str:
    kind = _wait_kind(cmd)
    if kind == "git":
        return str(checkout / ".git") if "--git-common-dir" in cmd else str(checkout)
    if kind == "version_skew":
        return "1.0" if cmd[0] == sys.executable else "2.0"
    names = json.loads(kwargs["input"])
    packages = {
        name: {"origin": str(checkout / "src" / name / "__init__.py"), "locations": [], "direct_url": None}
        for name in names
    }
    return json.dumps({"prefix": str(checkout / ".venv"), "site_packages": [], "packages": packages})


def _load_interp_from(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _drive_reinject_session_start(tmp_path: Path, monkeypatch, slow) -> dict[str, object]:
    """Run reinject-context.py ``main()`` through both passes of the venv re-exec.

    ``slow(kind, pass_no)`` picks the waits that run to the full timeout they were
    granted and then answer; the rest answer at once. Answering at the bound is the
    worst case: a wait that times out instead returns "" and cuts the path short.
    Each pass loads a fresh ``_interp`` (a new process image after ``os.execv``).
    """
    checkout = tmp_path / "checkout"
    venv_python = checkout / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text("", encoding="utf-8")
    clock = _fake_clock(monkeypatch, 1000.0)
    start = clock["now"]
    state = {"pass": 1}
    waits: list[tuple[int, str, float]] = []
    hook_git_entered: list[float] = []

    def run(cmd, **kwargs):
        timeout = kwargs.get("timeout")
        assert isinstance(timeout, (int, float)) and not isinstance(timeout, bool) and timeout > 0, (cmd, timeout)
        kind = _wait_kind(cmd)
        waits.append((state["pass"], kind, timeout))
        if slow(kind, state["pass"]):
            clock["now"] += float(timeout)
        return subprocess.CompletedProcess(cmd, 0, stdout=_subprocess_answer(cmd, kwargs, checkout), stderr="")

    def execv(path, argv):
        raise _Reexec(path)

    _isolated_env(monkeypatch)
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(os, "execv", execv)
    monkeypatch.setattr(sys, "argv", [str(HOOKS_DIR / "reinject-context.py")])
    monkeypatch.setattr(sys, "meta_path", list(sys.meta_path))
    monkeypatch.setattr(sys, "path", [str(HOOKS_DIR), *sys.path])
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    envfile = ModuleType("_envfile")
    envfile.load_embedding_env = lambda *_args, **_kwargs: None
    monkeypatch.setitem(sys.modules, "_envfile", envfile)

    outcomes: list[object] = []
    hook = None
    for pass_no in (1, 2):
        state["pass"] = pass_no
        if pass_no == 2:
            monkeypatch.setattr(sys, "executable", str(venv_python))
        interp = _load_interp_from(HOOKS_DIR / "_interp.py", f"_interp_session_start_pass_{pass_no}")
        # In-process origin lookups are not subprocess waits; place the stack in the checkout.
        monkeypatch.setattr(interp, "_inprocess_origin", lambda name: str(checkout / "src" / name / "__init__.py"))
        monkeypatch.setattr(interp, "_read_direct_url", lambda _name: None)
        monkeypatch.setitem(sys.modules, "_interp", interp)
        spec = importlib.util.spec_from_file_location(
            f"reinject_context_session_start_pass_{pass_no}", HOOKS_DIR / "reinject-context.py"
        )
        assert spec is not None and spec.loader is not None
        hook = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hook)
        original_git_repo_root = hook._git_repo_root

        def tracked_git_repo_root(original=original_git_repo_root):
            hook_git_entered.append(clock["now"])
            return original()

        monkeypatch.setattr(hook, "_git_repo_root", tracked_git_repo_root)
        monkeypatch.setattr(hook, "_ensure_in_repo_sources_on_path", lambda _repo_root: None)
        try:
            outcomes.append(hook.main())
        except _Reexec:
            outcomes.append("reexec")
            continue
        except ImportError as exc:
            outcomes.append(str(exc).split(":", 1)[0])
        break
    total = clock["now"] - start
    ensure_wait = (hook_git_entered[0] - start) if hook_git_entered else total
    return {"hook": hook, "waits": waits, "outcomes": outcomes, "total": total, "ensure_wait": ensure_wait}


_SESSION_START_SLOW_WAITS = {
    "every_wait": lambda kind, pass_no: True,
    "origin_probe_then_second_pass": lambda kind, pass_no: kind == "origin_probe" or pass_no == 2,
    "version_skew_then_second_pass": lambda kind, pass_no: kind == "version_skew" or pass_no == 2,
}


def test_reinject_context_passes_its_origin_probe_cap(monkeypatch) -> None:
    hook_path = HOOKS_DIR / "reinject-context.py"
    spec = importlib.util.spec_from_file_location("reinject_context_origin_cap", hook_path)
    assert spec is not None and spec.loader is not None
    hook = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(hook)
    observed: dict[str, float] = {}
    interp_stub = ModuleType("_interp")
    interp_stub.ensure_deps_interpreter = lambda **kwargs: observed.update(kwargs)
    monkeypatch.setitem(sys.modules, "_interp", interp_stub)
    monkeypatch.setattr(hook, "_git_repo_root", lambda: "")
    monkeypatch.setattr(hook, "_emit", lambda _message: None)
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))

    assert hook.main() == 0

    assert observed == {
        "origin_probe_timeout_seconds": hook._ORIGIN_PROBE_TIMEOUT_CAP_SECONDS,
        "subprocess_budget_seconds": hook._INTERP_SUBPROCESS_BUDGET_SECONDS,
    }
    assert hook._ORIGIN_PROBE_TIMEOUT_CAP_SECONDS == 30
    assert hook._INTERP_SUBPROCESS_BUDGET_SECONDS == 40
    # The budget, the later 5s _git_repo_root wait and the stated reserve fit 60s.
    assert (
        hook._INTERP_SUBPROCESS_BUDGET_SECONDS + 5 + _REINJECT_OWN_WORK_RESERVE_SECONDS
        <= _SESSION_START_DEFAULT_TIMEOUT_SECONDS
    )


@pytest.mark.parametrize("scenario", sorted(_SESSION_START_SLOW_WAITS))
def test_session_start_worst_case_subprocess_wait_fits_the_hook_budget(scenario, tmp_path: Path, monkeypatch) -> None:
    """Every slow wait runs to exactly its grant; the SessionStart path still fits 60s.

    Fake clock and fake ``subprocess.run``, as test_hook_inner_timeout_budget.py
    does for the bash guard. The second pass of the re-exec is included.
    """
    result = _drive_reinject_session_start(tmp_path, monkeypatch, _SESSION_START_SLOW_WAITS[scenario])

    allowed = _SESSION_START_DEFAULT_TIMEOUT_SECONDS - _REINJECT_OWN_WORK_RESERVE_SECONDS
    assert result["total"] <= allowed, (
        f"SessionStart waited {result['total']}s on subprocesses; the harness kills the hook at "
        f"{_SESSION_START_DEFAULT_TIMEOUT_SECONDS}s and {_REINJECT_OWN_WORK_RESERVE_SECONDS}s is the "
        f"hook's own work. Outcomes: {result['outcomes']}. Waits: {result['waits']}"
    )
    hook = result["hook"]
    assert result["ensure_wait"] <= hook._INTERP_SUBPROCESS_BUDGET_SECONDS, result["waits"]
    if scenario != "every_wait":
        assert [pass_no for pass_no, _kind, _timeout in result["waits"]][-1] == 2, result["waits"]
    # Out of budget before the trust anchors resolved: refuse, never trust unchecked (SECD-05).
    assert result["outcomes"][-1] == "hook_origin_anchor_unavailable", result["outcomes"]


def test_session_start_healthy_host_gets_every_cap_under_the_budget(tmp_path: Path, monkeypatch) -> None:
    """No starvation: prompt subprocesses receive their full caps and the hook completes."""
    result = _drive_reinject_session_start(tmp_path, monkeypatch, lambda kind, pass_no: False)

    hook = result["hook"]
    assert result["outcomes"] == ["reexec", 0]
    assert result["total"] == 0
    caps = {"git": 5, "origin_probe": hook._ORIGIN_PROBE_TIMEOUT_CAP_SECONDS, "version_skew": 15}
    assert [(kind, timeout) for _pass_no, kind, timeout in result["waits"]] == [
        (kind, caps[kind]) for _pass_no, kind, _timeout in result["waits"]
    ]
    assert {(pass_no, kind) for pass_no, kind, _timeout in result["waits"]} == {
        (1, "git"),
        (1, "origin_probe"),
        (1, "version_skew"),
        (2, "git"),
    }
