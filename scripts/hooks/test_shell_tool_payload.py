"""Shared shell-tool payload adapter (``_shell_tool_payload.py``, decision 37578).

Pure calls into the adapter pin the payload shapes it reads and the per-guard
switches. One parity check per wired guard then shows an ``exec_command``
payload gets the same outcome as the equivalent Bash payload (SECD-05). The
main-branch guard's exec coverage lives in
``test_codex_exec_command_transport.py``.

Modules are loaded by path, because root and payload twins share basenames.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve()
HOOKS_DIR = HERE.parent
PAYLOAD_HOOKS_DIR = Path("packages/workbay-system/workbay_system/payload/scripts/hooks")
# parents[2] is the checkout root for scripts/hooks and the payload root for the
# twin; both carry the harness contract at the same relative path.
CONTRACT = HERE.parents[2] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
UNION_NAMES = ("Bash", "Shell", "run_in_terminal", "exec_command", "functions.exec_command")
EXEC_NAMES = ("exec_command", "functions.exec_command")


def _load(filename: str):
    path = HOOKS_DIR / filename
    name = f"{path.stem.replace('-', '_')}_under_test_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


adapter = _load("_shell_tool_payload.py")
extract = adapter.extract_shell_call


def _payload(name: str, key: str = "command", value: object = "ls -la", **extra: object) -> dict:
    return {"tool_name": name, "tool_input": {key: value}, **extra}


# --- names, keys and transport ------------------------------------------------


def test_shell_tool_names_are_the_union_of_guard_names_plus_exec() -> None:
    assert adapter.SHELL_TOOL_NAMES == UNION_NAMES
    assert adapter.EXEC_COMMAND_TOOL_NAMES == EXEC_NAMES


@pytest.mark.parametrize("key", ["command", "cmd"])
@pytest.mark.parametrize("name", [*UNION_NAMES, *(n.lower() for n in UNION_NAMES)])
def test_every_shell_name_any_case_with_command_or_cmd(name: str, key: str) -> None:
    call = extract(_payload(name, key))
    assert call is not None
    assert (call.tool_name, call.command, call.workdir) == (name, "ls -la", None)
    assert call.transport == ("exec_command" if name.lower() in EXEC_NAMES else "bash")


def test_command_wins_over_cmd() -> None:
    call = extract({"tool_name": "exec_command", "tool_input": {"command": "a", "cmd": "b"}})
    assert call is not None and call.command == "a"


@pytest.mark.parametrize("name_key", ["tool_name", "toolName", "tool"])
def test_each_name_key(name_key: str) -> None:
    call = extract({name_key: "exec_command", "tool_input": {"cmd": "ls"}})
    assert call is not None and call.tool_name == "exec_command"


def test_first_non_empty_name_key_decides() -> None:
    assert extract({"tool_name": "Edit", "toolName": "Bash", "tool_input": {"command": "ls"}}) is None


@pytest.mark.parametrize("input_key", ["tool_input", "toolInput", "input"])
def test_each_tool_input_key(input_key: str) -> None:
    call = extract({"tool_name": "Bash", input_key: {"command": "ls"}})
    assert call is not None and call.command == "ls"


@pytest.mark.parametrize("args", ['{"cmd": "ls"}', {"cmd": "ls"}], ids=["json-string", "dict"])
def test_tool_args(args: object) -> None:
    call = extract({"toolName": "exec_command", "toolArgs": args})
    assert call is not None and call.command == "ls"


@pytest.mark.parametrize("args", ["{not json", "[" * 20_000, '"ls"', "[]"])
def test_unusable_tool_args_is_not_a_call(args: str) -> None:
    assert extract({"toolName": "Bash", "toolArgs": args}) is None


# --- working directory ----------------------------------------------------------


def test_absolute_workdir_wins_over_cwd(tmp_path: Path) -> None:
    payload = {"tool_name": "exec_command", "tool_input": {"cmd": "ls", "workdir": str(tmp_path)}, "cwd": "/elsewhere"}
    call = extract(payload)
    assert call is not None and call.workdir == tmp_path


def test_relative_workdir_resolves_against_cwd() -> None:
    payload = {"tool_name": "exec_command", "tool_input": {"cmd": "ls", "workdir": "sub/dir"}, "cwd": "/repo"}
    call = extract(payload)
    assert call is not None and call.workdir == Path("/repo/sub/dir")


def test_cwd_only() -> None:
    call = extract(_payload("Bash", cwd="/repo"))
    assert call is not None and call.workdir == Path("/repo")


def test_neither_workdir_nor_cwd_keeps_process_cwd() -> None:
    call = extract(_payload("Bash"))
    assert call is not None and call.workdir is None


def test_relative_workdir_without_cwd_stays_relative() -> None:
    call = extract({"tool_name": "exec_command", "tool_input": {"cmd": "ls", "workdir": "sub"}})
    assert call is not None and call.workdir == Path("sub")


@pytest.mark.parametrize("bad", ["", 7, None, "/repo\x00x"])
def test_unusable_directory_values_are_ignored(bad: object) -> None:
    call = extract({"tool_name": "Bash", "tool_input": {"command": "ls", "workdir": bad}, "cwd": bad})
    assert call is not None and call.workdir is None


# --- not a shell call -------------------------------------------------------------


@pytest.mark.parametrize("value", ["", None, 42, ["ls"], {"x": 1}])
@pytest.mark.parametrize("key", ["command", "cmd"])
def test_empty_or_non_string_command_is_not_a_call(key: str, value: object) -> None:
    assert extract(_payload("exec_command", key, value)) is None


def test_missing_command_is_not_a_call() -> None:
    assert extract({"tool_name": "Bash", "tool_input": {"description": "x"}}) is None
    assert extract({"tool_name": "Bash"}) is None


def test_nested_functions_exec_is_not_unwrapped() -> None:
    nested = {
        "tool_name": "functions.exec",
        "tool_input": {"calls": [{"name": "tools.exec_command", "arguments": {"cmd": "rm -rf x"}}]},
    }
    assert extract(nested) is None
    assert extract(_payload("functions.exec", "cmd")) is None


def test_non_shell_tool_is_not_a_call() -> None:
    assert extract({"tool_name": "Edit", "tool_input": {"file_path": "a.py", "command": "ls"}}) is None


@pytest.mark.parametrize(
    "payload",
    [None, [], "Bash", 3, {"tool_name": ["Bash"]}, {"tool_name": "Bash", "tool_input": "ls"}, {"tool_input": None}],
)
def test_malformed_payload_is_not_a_call_and_never_raises(payload: object) -> None:
    assert extract(payload) is None


# --- per-guard switches -----------------------------------------------------------


def test_untyped_payload_needs_allow_untyped() -> None:
    untyped = {"tool_input": {"command": "pytest x.py"}}
    assert extract(untyped) is None
    call = extract(untyped, allow_untyped=True)
    assert call is not None and (call.tool_name, call.command, call.transport) == ("", "pytest x.py", "bash")


def test_top_level_command_needs_the_switch() -> None:
    assert extract({"command": "pytest x.py"}, allow_untyped=True) is None
    for key in ("command", "cmd"):
        call = extract({key: "pytest x.py"}, allow_untyped=True, top_level_command=True)
        assert call is not None and call.command == "pytest x.py"
    inner = {"tool_name": "Bash", "tool_input": {"command": "inner"}, "command": "outer"}
    assert extract(inner, top_level_command=True).command == "inner"


def test_allow_empty_command_keeps_an_empty_string_only() -> None:
    call = extract(_payload("Bash", value=""), allow_empty_command=True)
    assert call is not None and call.command == ""
    assert extract(_payload("Bash", value=None), allow_empty_command=True) is None
    assert extract({"command": ""}, allow_untyped=True, top_level_command=True, allow_empty_command=True).command == ""


def test_coerce_command_stringifies_like_the_codemap_classifier() -> None:
    assert extract(_payload("Bash", value=42), coerce_command=True).command == "42"
    assert extract(_payload("Bash", value=None), coerce_command=True).command == ""
    assert extract({"tool_name": "Bash"}, coerce_command=True).command == ""
    assert extract({"tool_input": {"command": None}}, allow_untyped=True, coerce_command=True).command == ""
    assert extract({"tool_input": {"x": 1}}, allow_untyped=True, coerce_command=True) is None


def test_name_keys_limit_which_keys_name_the_tool() -> None:
    camel = {"toolName": "Grep", "tool_input": {"command": "rg foo src"}}
    assert extract(camel, allow_untyped=True) is None
    call = extract(camel, name_keys=("tool_name", "tool"), allow_untyped=True)
    assert call is not None and call.tool_name == ""


def test_empty_name_keys_treat_every_payload_as_untyped() -> None:
    edit = {"tool_name": "Edit", "tool_input": {"command": "pytest -q"}}
    call = extract(edit, name_keys=(), allow_untyped=True)
    assert call is not None and (call.tool_name, call.command) == ("", "pytest -q")


def test_shell_tool_name() -> None:
    assert adapter.shell_tool_name({"toolName": "RUN_IN_TERMINAL"}) == "RUN_IN_TERMINAL"
    assert adapter.shell_tool_name({"tool_name": "Edit"}) is None
    assert adapter.shell_tool_name({"command": "ls"}) is None
    assert adapter.shell_tool_name(["Bash"]) is None


# --- twin -------------------------------------------------------------------------


def _repo_toplevel() -> Path:
    proc = subprocess.run(
        ["git", "-C", str(HOOKS_DIR), "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(proc.stdout.strip())


def test_adapter_twin_copy_is_byte_identical() -> None:
    top = _repo_toplevel()
    module = HOOKS_DIR / "_shell_tool_payload.py"
    if "payload" in HERE.parts:
        twin = top / "scripts" / "hooks" / module.name
    else:
        twin = top / PAYLOAD_HOOKS_DIR / module.name
    assert twin.is_file(), f"twin missing: {twin}"
    assert module.read_bytes() == twin.read_bytes(), f"twin differs: {module} vs {twin}"


# --- exec_command parity through each wired guard ---------------------------------


def _bash_and_exec(command: str, **extra: object) -> tuple[dict, dict]:
    bash = {"tool_name": "Bash", "tool_input": {"command": command}, **extra}
    exec_call = {"tool_name": "exec_command", "tool_input": {"cmd": command}, **extra}
    return bash, exec_call


def _run_hook(filename: str, payload: dict, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(HOOKS_DIR / filename)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env={**os.environ, **env},
        timeout=60,
        check=False,
    )


def test_codemap_classifies_exec_like_bash() -> None:
    codemap = _load("guard-codemap-first.py")
    bash, exec_call = _bash_and_exec("rg foo src/a.py")
    expected = codemap.classify_source_query(bash)
    assert expected.kind == "source_query"
    assert codemap.classify_source_query(exec_call) == expected


def test_lifecycle_refuses_exec_like_bash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKBAY_ALLOW_RAW_GIT_LIFECYCLE", raising=False)
    env = {"CLAUDE_PROJECT_DIR": str(tmp_path)}
    bash, exec_call = _bash_and_exec('git commit -m "m"')
    expected = _run_hook("guard-bash-lifecycle-primitives.py", bash, env)
    assert expected.returncode == 2, expected.stderr
    got = _run_hook("guard-bash-lifecycle-primitives.py", exec_call, env)
    assert (got.returncode, got.stderr) == (expected.returncode, expected.stderr)


def test_remote_gate_refuses_exec_like_bash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKBAY_ALLOW_LOCAL_GATE_UNDER_REMOTE_ONLY", raising=False)
    (tmp_path / ".workbay-bootstrap.json").write_text(json.dumps({"execution_mode": "remote_only"}), encoding="utf-8")
    env = {"CLAUDE_PROJECT_DIR": str(tmp_path)}
    bash, exec_call = _bash_and_exec("pytest x.py")
    expected = _run_hook("guard-bash-remote-gate-routing.py", bash, env)
    assert expected.returncode == 2, expected.stderr
    got = _run_hook("guard-bash-remote-gate-routing.py", exec_call, env)
    assert (got.returncode, got.stderr) == (expected.returncode, expected.stderr)


def test_filter_summarises_exec_like_bash() -> None:
    stdout = "tests/test_x.py ....\n\n============================== 4 passed in 0.12s ==============================\n"
    response = {"tool_response": {"stdout": stdout, "stderr": "", "exitCode": 0}}
    bash, exec_call = _bash_and_exec("pytest tests/", hook_event_name="PostToolUse", **response)
    expected = _run_hook("filter-test-output.py", bash, {})
    assert "hookSpecificOutput" in json.loads(expected.stdout), expected.stdout
    got = _run_hook("filter-test-output.py", exec_call, {})
    assert got.stdout == expected.stdout


def test_capture_records_exec_like_bash(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.syspath_prepend(str(HOOKS_DIR))
    capture = _load("capture-agent-errors.py")
    captured: list[list[str]] = []

    class _Done:
        returncode = 0

    monkeypatch.setattr(capture, "_resolve_errors_record_argv", lambda _root=None: ["errors-record"])
    monkeypatch.setattr(capture, "_resolve_agent_handoff_src", lambda _root: "")
    monkeypatch.setattr(capture.subprocess, "run", lambda argv, **_kw: captured.append(list(argv)) or _Done())
    response = {
        "tool_response": {
            "stdout": "",
            "stderr": "ModuleNotFoundError: No module named 'workbay_handoff_mcp'",
            "exitCode": 1,
        }
    }
    bash, exec_call = _bash_and_exec("python -c 'import workbay_handoff_mcp'", **response)
    assert capture.process_event(bash) == 0
    assert capture.process_event(exec_call) == 0
    assert len(captured) == 2, captured
    assert "install_drift" in captured[0]
    assert captured[1] == captured[0]


def test_drift_evaluates_exec_like_bash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.syspath_prepend(str(HOOKS_DIR))
    drift = _load("_worktree_drift.py")
    workspace = tmp_path / "workspace"
    contract = workspace / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_bytes(CONTRACT.read_bytes())
    # The write lands in another git worktree, so the drift check has a
    # candidate worktree to compare against the task's target.
    other = tmp_path / "other"
    subprocess.run(["git", "init", "-q", str(other)], check=True, capture_output=True)
    outside = other / "mod.py"
    outside.write_text("a = 1\n", encoding="utf-8")
    bash, exec_call = _bash_and_exec(f"sed -i 's/a/b/' {outside}")
    active = ("TASK-1", str(tmp_path / "target"))
    expected = drift.evaluate_payload(bash, workspace_root=workspace, active_task=active)
    assert expected is not None
    assert drift.evaluate_payload(exec_call, workspace_root=workspace, active_task=active) == expected
