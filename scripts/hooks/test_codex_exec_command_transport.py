"""Codex ``exec_command`` transport vs the main-branch write guard (RED).

Codex runs shell commands through a tool whose input key is ``cmd`` (optionally
with ``workdir``), reached as ``tool_name`` ``exec_command`` or
``functions.exec_command``, or nested inside ``functions.exec``. Before the shared
adapter (``_shell_tool_payload.py``, ur-hk-13) the main-branch guard
(``guard-bash-main-branch.py``) matched only ``Bash`` and read only
``tool_input.command``, so a Codex write to a protected path on ``main`` was
never inspected.

Remaining RED cases are strict xfails, and a case that turns green must have its
marker removed. Hook-config wiring is ur-hk-14's and is tested there. Every case
drives the hook as a subprocess and asserts at the process boundary only (exit
code, ``BLOCKED`` on stderr, the captured telemetry argv). Helpers are copied from
``test_guard_bash_main_branch_root_switch.py`` and
``test_guard_bash_main_branch_telemetry.py`` rather than imported, because twin
modules share basenames.

``README.md`` is not a protected path (the policy protects code roots such as
``packages/`` with code extensions, plus the root ``Makefile``), so writes target
``packages/demo/mod.py``. A Bash write to ``README.md`` exits 0 at base.
"""

from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve()
HOOK_SCRIPT = HERE.parent / "guard-bash-main-branch.py"
CONTRACT_SOURCE = HERE.parents[2] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
PAYLOAD_HOOKS_DIR = Path("packages/workbay-system/workbay_system/payload/scripts/hooks")
PROTECTED_REL = "packages/demo/mod.py"
EXEC_TOOL_NAMES = ("exec_command", "functions.exec_command")
# Bypass tokens are scrubbed so ambient env cannot turn a RED case into a pass.
_BYPASS_ENV = (
    "WORKBAY_ALLOW_BASH_MAIN_WRITE",
    "ALT_ALLOW_BASH_MAIN_WRITE",
    "WORKBAY_ALLOW_ROOT_BRANCH_SWITCH",
    "ALT_ALLOW_ROOT_BRANCH_SWITCH",
    "WORKBAY_HOOK_PROTOCOL_STRICT",
)

# ASSUMED, NOT OBSERVED: the real nested ``functions.exec`` payload is being
# measured by sibling lane ur-hk-07. Replace this single constant with that probe
# verdict. Keys: outer tool name, the list key holding nested calls, the nested
# tool's name, and the keys for that call's name and arguments.
NESTED_EXEC_PAYLOAD_SHAPE_ASSUMED = {
    "outer_tool_name": "functions.exec",
    "calls_key": "calls",
    "inner_tool_name": "tools.exec_command",
    "inner_name_key": "name",
    "inner_args_key": "arguments",
}


def _red(reason: str):
    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=f"RED ur-hk-14: {reason}")


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        [
            "git",
            "-C",
            str(cwd),
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _repo_toplevel() -> Path:
    proc = subprocess.run(
        ["git", "-C", str(HERE.parent), "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(proc.stdout.strip())


def _init_primary(path: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True, capture_output=True)
    # Contract must be committed so the linked worktree sees the same policy.
    contract = path / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    contract.parent.mkdir(parents=True)
    shutil.copy2(CONTRACT_SOURCE, contract)
    (path / "README.md").write_text("hello\n", encoding="utf-8")
    protected = path / PROTECTED_REL
    protected.parent.mkdir(parents=True)
    protected.write_text("x = 1\na = 2\n", encoding="utf-8")
    _git("add", "-A", cwd=path)
    _git("commit", "-q", "-m", "init", cwd=path)


@pytest.fixture
def primary_with_linked(tmp_path: Path) -> tuple[Path, Path]:
    """Primary checkout on main plus one linked feature worktree."""
    primary = tmp_path / "primary"
    primary.mkdir()
    _init_primary(primary)
    linked = tmp_path / "primary-linked"
    _git("worktree", "add", "-q", str(linked), "-b", "feature/seed", cwd=primary)
    return primary, linked


@pytest.fixture(autouse=True)
def telemetry_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Route block-path telemetry to a stub that logs its full argv, one JSON line per call.

    ``WORKBAY_TERMINAL_GUARD_RECORD`` is the hook's own CLI seam. Without it a
    blocked run would reach the real ``terminal-guard-record`` CLI and write to the
    live handoff store.
    """
    log = tmp_path / "terminal-guard-record.jsonl"
    stub = tmp_path / "terminal_guard_record_stub.py"
    stub.write_text(
        "import json, sys\n"
        f"with open({str(log)!r}, 'a', encoding='utf-8') as fh:\n"
        "    fh.write(json.dumps(sys.argv) + '\\n')\n",
        encoding="utf-8",
    )
    override = f"{shlex.quote(sys.executable)} {shlex.quote(str(stub))} terminal-guard-record"
    monkeypatch.setenv("WORKBAY_TERMINAL_GUARD_RECORD", override)
    for var in _BYPASS_ENV:
        monkeypatch.delenv(var, raising=False)
    return log


def _invoke(cwd: Path, payload: dict) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        cwd=cwd,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )


def _bash_payload(command: str) -> dict:
    return {"tool_name": "Bash", "tool_input": {"command": command}}


def _exec_payload(tool_name: str, cmd: str, workdir: Path | None = None) -> dict:
    tool_input: dict = {"cmd": cmd}
    if workdir is not None:
        tool_input["workdir"] = str(workdir)
    return {"tool_name": tool_name, "tool_input": tool_input}


def _nested_exec_payload(cmd: str) -> dict:
    shape = NESTED_EXEC_PAYLOAD_SHAPE_ASSUMED
    return {
        "tool_name": shape["outer_tool_name"],
        "tool_input": {
            shape["calls_key"]: [
                {
                    shape["inner_name_key"]: shape["inner_tool_name"],
                    shape["inner_args_key"]: {"cmd": cmd},
                }
            ]
        },
    }


def _await_telemetry(log: Path, timeout: float = 10.0) -> list[list[str]]:
    """Wait for the detached telemetry Popen to land its argv in the stub log."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not (log.is_file() and log.stat().st_size > 0):
        time.sleep(0.05)
    if not log.is_file():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


# --- CONTROLS: Bash shape and allowed exec_command paths pass today -----------


@pytest.mark.parametrize(
    "command",
    [
        pytest.param(f"echo x >> {PROTECTED_REL}", id="append"),
        pytest.param(f"sed -i 's/a/b/' {PROTECTED_REL}", id="sed-i"),
        # Finding 21745 shape. Already blocked for Bash at base, so only the
        # exec_command variant below is RED.
        pytest.param(f"echo hi\necho x >> {PROTECTED_REL}", id="multiline-second-line"),
    ],
)
def test_control_bash_write_on_main_blocked(primary_with_linked, command: str) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _bash_payload(command))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
def test_control_exec_write_in_feature_worktree_allowed(primary_with_linked, tool_name: str) -> None:
    _, linked = primary_with_linked
    proc = _invoke(linked, _exec_payload(tool_name, f"echo x >> {PROTECTED_REL}"))
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
def test_case5_converse_workdir_feature_from_primary_allowed(primary_with_linked, tool_name: str) -> None:
    primary, linked = primary_with_linked
    proc = _invoke(primary, _exec_payload(tool_name, f"echo x >> {PROTECTED_REL}", workdir=linked))
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
@pytest.mark.parametrize("cmd", ["git status", f"cat {PROTECTED_REL}"])
def test_case7_read_only_exec_on_main_allowed(primary_with_linked, tool_name: str, cmd: str) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _exec_payload(tool_name, cmd))
    assert proc.returncode == 0, proc.stderr


def test_control_bash_block_fires_one_telemetry_call(primary_with_linked, telemetry_log: Path) -> None:
    # Seam sanity: proves the stub capture works, so a RED telemetry case below
    # fails on the missing block, not on a broken fixture.
    primary, _ = primary_with_linked
    proc = _invoke(primary, _bash_payload(f"echo x >> {PROTECTED_REL}"))
    assert proc.returncode == 2, proc.stderr
    calls = _await_telemetry(telemetry_log)
    assert len(calls) == 1, calls
    argv = calls[0]
    assert "terminal-guard-record" in argv
    assert argv[argv.index("--decision") + 1] == "block"


def _sibling_twin() -> Path:
    top = _repo_toplevel()
    if "payload" in HERE.parts:
        return top / "scripts" / "hooks" / HERE.name
    return top / PAYLOAD_HOOKS_DIR / HERE.name


def test_case13_twin_copy_is_byte_identical() -> None:
    twin = _sibling_twin()
    assert twin.is_file(), f"twin missing: {twin}"
    assert HERE.read_bytes() == twin.read_bytes(), f"twin differs: {HERE} vs {twin}"


# --- RED: exec_command / cmd / workdir / nested transport is not inspected ----


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
@pytest.mark.parametrize(
    "cmd",
    [
        pytest.param(f"echo x >> {PROTECTED_REL}", id="append"),
        pytest.param(f"sed -i 's/a/b/' {PROTECTED_REL}", id="sed-i"),
    ],
)
def test_case1_exec_write_on_main_blocked(primary_with_linked, tool_name: str, cmd: str) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _exec_payload(tool_name, cmd))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
def test_exec_multiline_write_on_second_line_blocked(primary_with_linked, tool_name: str) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _exec_payload(tool_name, f"echo hi\necho x >> {PROTECTED_REL}"))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
def test_case4_workdir_primary_on_main_blocked(primary_with_linked, tool_name: str) -> None:
    primary, linked = primary_with_linked
    proc = _invoke(linked, _exec_payload(tool_name, f"echo x >> {PROTECTED_REL}", workdir=primary))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
@_red(
    "cross-worktree absolute target is not scanned: with no workdir or cwd the branch is keyed off the feature "
    "process cwd, and the same Bash write also exits 0 at base, so this needs a policy change outside decision 37578"
)
def test_case6_absolute_primary_target_from_feature_cwd_blocked(primary_with_linked, tool_name: str) -> None:
    primary, linked = primary_with_linked
    proc = _invoke(linked, _exec_payload(tool_name, f"echo x >> {primary / PROTECTED_REL}"))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
@pytest.mark.parametrize(
    "cmd",
    [
        pytest.param(f"mv {PROTECTED_REL} packages/demo/renamed.py", id="mv"),
        pytest.param(f"rm {PROTECTED_REL}", id="rm"),
    ],
)
def test_case8_exec_rename_or_remove_on_main_blocked(primary_with_linked, tool_name: str, cmd: str) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _exec_payload(tool_name, cmd))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.parametrize("tool_name", EXEC_TOOL_NAMES)
def test_case9_exec_block_fires_one_telemetry_call(primary_with_linked, telemetry_log: Path, tool_name: str) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _exec_payload(tool_name, f"echo x >> {PROTECTED_REL}"))
    assert proc.returncode == 2, proc.stderr
    calls = _await_telemetry(telemetry_log)
    assert len(calls) == 1, calls
    argv = calls[0]
    assert "terminal-guard-record" in argv
    assert argv[argv.index("--decision") + 1] == "block"


@_red(
    "the nested payload shape is unobserved (ur-hk-07 verdict inconclusive); the adapter does not unwrap functions.exec"
)
def test_case11_nested_functions_exec_write_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, _nested_exec_payload(f"echo x >> {PROTECTED_REL}"))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


@pytest.mark.skip(reason="interface decided by ur-hk-14")
def test_case12_unmatched_tool_telemetry() -> None:
    """Intended assertion, not implemented: a PreToolUse event whose tool name no
    guard claims is recorded once through the telemetry seam and the hook exits 0.
    The seam, record path, and record shape are for ur-hk-14 to decide, so nothing
    is asserted here.
    """


# --- A selected directory never drops the hook cwd's checkout -----------------
# Gate finding URHK13-27748ed3f3e4b2e4-H-2941aee49c98c5c516d16a16: with the
# hook process in the primary on main and the payload selecting the linked
# feature worktree, the selected branch let a write into the primary through.
# The selected directory decides where relative paths land; it must not remove
# the primary.


def _select(selector: str, command: str, directory: Path) -> dict:
    """``command`` run in ``directory``, named by ``tool_input.workdir`` or top-level ``cwd``."""
    if selector == "workdir":
        return _exec_payload("exec_command", command, workdir=directory)
    return {**_bash_payload(command), "cwd": str(directory)}


@pytest.mark.parametrize("selector", ["workdir", "cwd"])
@pytest.mark.parametrize("shape", ["absolute", "cd-primary", "parent-relative", "sed-i-absolute"])
def test_feature_dir_selected_from_primary_still_blocks_primary_write(
    primary_with_linked, selector: str, shape: str
) -> None:
    primary, linked = primary_with_linked
    command = {
        "absolute": f"echo x >> {primary / PROTECTED_REL}",
        "cd-primary": f"cd {primary} && echo x >> {PROTECTED_REL}",
        "parent-relative": f"echo x >> ../{primary.name}/{PROTECTED_REL}",
        "sed-i-absolute": f"sed -i 's/a/b/' {primary / PROTECTED_REL}",
    }[shape]
    proc = _invoke(primary, _select(selector, command, linked))
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr
    assert str(primary) in proc.stderr


@pytest.mark.parametrize("selector", ["workdir", "cwd"])
@pytest.mark.parametrize("shape", ["relative", "absolute-feature", "formatter"])
def test_feature_dir_selected_from_primary_allows_feature_writes(
    primary_with_linked, selector: str, shape: str
) -> None:
    primary, linked = primary_with_linked
    command = {
        "relative": f"echo x >> {PROTECTED_REL}",
        "absolute-feature": f"echo x >> {linked / PROTECTED_REL}",
        "formatter": "ruff format .",
    }[shape]
    proc = _invoke(primary, _select(selector, command, linked))
    assert proc.returncode == 0, proc.stderr
