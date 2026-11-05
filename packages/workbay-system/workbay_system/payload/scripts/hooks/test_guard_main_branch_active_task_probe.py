"""The main-branch guard must not resolve the active task by exec'ing a CLI.

internal, shell layer. ``guard-main-branch.sh`` answers "is a handoff
task active?" by spawning the *installed* ``mcp-workbay-handoff`` console script
and piping its JSON into a second ``python3``. Two costs, both measured on this
host:

* **Latency.** That probe is 2118ms of the hook's 5s budget, on every edit made
  from the primary worktree on ``main``. ``PreToolUse:Edit`` runs at a median of
  1618ms with 82 timeouts across 2227 runs.
* **Correctness, which is worse.** The installed console script is a separately
  versioned artifact. Here it exits 1 against a newer handoff schema. Its stderr
  is discarded by ``2>/dev/null`` and its non-zero exit by ``|| true``, so a
  crashed probe and a clean "no task" answer produce the identical empty
  ``ACTIVE_TASK`` -- and the hook then states, as fact, something it never
  established. It printed ``WARNING: Editing on main without an active handoff
  task`` on every main-branch edit for the whole period a task *was* active.

Silence from a dead probe must not read as an answer [OBS-08], and the swallowed
failure has to leave a trace instead of dissolving into a default [AGT-10].

The fix is convergence, not invention: ``scripts/hooks/_active_task_context.py``
already resolves this in-process from the in-tree package source, and
``guard-worktree-drift.sh`` uses it at 464ms with no dependence on what happens
to be installed. This hook must go the same way -- which is also why
``test_active_task_context_probe_error.py`` has to land with it: the shared
resolver currently conflates failure with a negative in exactly the same way, so
switching to it without that discriminator would move the bug rather than fix it.

The tests below drive the real script. ``git`` is shimmed so the hook believes
it is on ``main`` with a clean tree regardless of the checkout these tests run
from, and a marker-writing ``mcp-workbay-handoff`` is put on ``PATH``. The
assertions are on the marker file and on the emitted text, never on elapsed
time -- a latency threshold would be a flake on shared CI.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent
HOOK_SCRIPT = HOOKS_DIR / "guard-main-branch.sh"
REPO_ROOT = HOOKS_DIR.parent.parent

_WARNING_TEXT = "without an active handoff task"
_PROBE_FAIL_PREFIX = "active-task probe failed:"

_GIT_SHIM = """#!/bin/sh
# Report main, and a clean tree, whatever the real checkout looks like. Anything
# else is delegated to the real git so repo-root resolution stays honest.
for a in "$@"; do
  case "$a" in
    --show-current) echo main; exit 0;;
    status) exit 0;;
  esac
done
exec {real_git} "$@"
"""

_CLI_SHIM = """#!/bin/sh
# Records that it was called at all, then fails the way the installed console
# script currently fails here: non-zero, with the cause on stderr.
touch "$MARKER"
echo 'handoff_schema_version_mismatch: db user_version=33, package supports 32' >&2
exit 1
"""

# The hook resolves the active task by feeding a short program to `python3 -`.
# This shim prepends a preamble to that program and leaves every other python3
# invocation -- notably _guard_main_branch_inline.py -- untouched.
_PY_SHIM = """#!/bin/sh
if [ "$1" = "-" ] && [ -n "${{HOOK_TEST_PREAMBLE:-}}" ]; then
  shift
  {{ cat "$HOOK_TEST_PREAMBLE"; cat; }} | {real_python} - "$@"
  exit $?
fi
exec {real_python} "$@"
"""

# Each preamble pre-registers _active_task_context in sys.modules, so the hook's
# `from _active_task_context import _load_active_task` binds to a stub whose
# answer we choose. Injecting at the resolver rather than at the console script
# is the whole point: after the fix there is no console script left to fail, so a
# probe failure can only be staged where the probe actually lives.
#
# The preamble also writes HOOK_TEST_INJECT_MARKER so every inject-dependent arm
# can assert the shim actually ran [TEST-04]. Without that check, disabling the
# shim leaves property arms green and only reddens the control arm.
_PREAMBLE = """
import os
import sys
import types
from pathlib import Path

_marker = os.environ.get("HOOK_TEST_INJECT_MARKER")
if _marker:
    Path(_marker).write_text("injected\\n", encoding="utf-8")

_m = types.ModuleType("_active_task_context")


class _Ctx:
    task_ref = {task_ref!r}
    target_worktree = None
    target_branch = None
    primary_worktree = ""
    task_plan_path = None
    probe_error = {probe_error!r}


_m.ActiveTaskContext = _Ctx
_m._load_active_task = lambda *a, **k: _Ctx()
sys.modules["_active_task_context"] = _m
"""

# The resolver does not turn every failure into a probe_error: it deliberately
# re-raises UnresolvedTaskContextError, and an import-time failure in the
# handoff package escapes as whatever it is. Those exits are non-zero rather
# than PROBE_ERROR-on-stdout, which is a different shell path entirely.
_PREAMBLE_RAISE = """
import os
import sys
import types
from pathlib import Path

_marker = os.environ.get("HOOK_TEST_INJECT_MARKER")
if _marker:
    Path(_marker).write_text("injected\\n", encoding="utf-8")

_m = types.ModuleType("_active_task_context")


class UnresolvedTaskContextError(RuntimeError):
    pass


def _load_active_task(*a, **k):
    raise UnresolvedTaskContextError("Ambiguous active task: 3 candidate rows")


_m.UnresolvedTaskContextError = UnresolvedTaskContextError
_m._load_active_task = _load_active_task
sys.modules["_active_task_context"] = _m
"""


def _shim_dir(tmp_path: Path) -> Path:
    real_git = shutil.which("git")
    if not real_git:
        pytest.skip("git is not on PATH")
    real_python = shutil.which("python3")
    if not real_python:
        pytest.skip("python3 is not on PATH")

    shims = tmp_path / "shims"
    shims.mkdir()
    (shims / "git").write_text(_GIT_SHIM.format(real_git=real_git), encoding="utf-8")
    (shims / "mcp-workbay-handoff").write_text(_CLI_SHIM, encoding="utf-8")
    (shims / "python3").write_text(_PY_SHIM.format(real_python=real_python), encoding="utf-8")
    for name in ("git", "mcp-workbay-handoff", "python3"):
        (shims / name).chmod(0o755)
    return shims


def _run_hook(
    tmp_path: Path,
    *,
    task_ref: str | None = None,
    probe_error: str | None = None,
    inject: bool = False,
    raises: bool = False,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    """Invoke the hook for a permitted main-branch edit.

    ``README.md`` is a permitted operator surface, so the protected-path gate
    lets the payload through and execution reaches the active-task block. If a
    future policy change makes it protected these tests go red on the reachability
    assertion below rather than passing vacuously.

    With ``inject=True`` the resolver's answer is staged via the ``python3``
    shim; without it the hook resolves for real, which is what the marker arm
    wants.

    Returns ``(proc, cli_marker, inject_marker)``. Every inject-dependent arm
    must assert ``inject_marker`` exists so a broken harness cannot look green.
    """
    if not HOOK_SCRIPT.is_file():
        pytest.skip("guard-main-branch.sh is not present in this tree")

    shims = _shim_dir(tmp_path)
    marker = tmp_path / "cli-was-called"
    inject_marker = tmp_path / "inject-took-effect"
    env = dict(os.environ)
    env["PATH"] = f"{shims}{os.pathsep}{env.get('PATH', '')}"
    env["MARKER"] = str(marker)
    env["HOOK_TEST_INJECT_MARKER"] = str(inject_marker)
    env.pop("WORKBAY_SKIP_ACTIVE_TASK_PROBE", None)
    env.pop("HOOK_TEST_PREAMBLE", None)
    if inject or raises:
        preamble = tmp_path / "preamble.py"
        preamble.write_text(
            _PREAMBLE_RAISE
            if raises
            else _PREAMBLE.format(task_ref=task_ref, probe_error=probe_error),
            encoding="utf-8",
        )
        env["HOOK_TEST_PREAMBLE"] = str(preamble)

    payload = (
        '{"session_id":"t","transcript_path":"/dev/null",'
        f'"cwd":"{REPO_ROOT}","hook_event_name":"PreToolUse","tool_name":"Edit",'
        f'"tool_input":{{"file_path":"{REPO_ROOT / "README.md"}",'
        '"old_string":"a","new_string":"b"}}'
    )
    proc = subprocess.run(
        ["bash", str(HOOK_SCRIPT)],
        input=payload,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env=env,
        timeout=60,
    )
    return proc, marker, inject_marker


def _assert_injection_took_effect(inject_marker: Path) -> None:
    """Fail loudly when an inject arm ran without the shim applying [TEST-04]."""
    assert inject_marker.is_file(), (
        "injection-dependent arm did not observe HOOK_TEST_INJECT_MARKER. The "
        "python3 PATH shim / HOOK_TEST_PREAMBLE path never executed, so this arm "
        "is asserting production resolution rather than the staged answer. A "
        "broken harness must not read as a passing property."
    )


def test_the_hook_reaches_the_active_task_block_at_all(tmp_path: Path) -> None:
    """Guard against the other three tests passing for the wrong reason.

    Every assertion below is about what happens *after* the protected-path gate
    allows the edit. If the payload were blocked earlier -- a policy change, a
    shim that stops convincing the script it is on main -- the hook would exit
    before the probe and the marker would be absent for reasons that have nothing
    to do with the defect. Pin reachability explicitly.
    """
    proc, _, _ = _run_hook(tmp_path)

    assert proc.returncode == 0, (
        f"the hook blocked a permitted main-branch edit (rc={proc.returncode}); "
        f"the remaining tests in this file would then prove nothing.\n"
        f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    assert "BLOCKED" not in proc.stderr, (
        f"the edit was blocked before the active-task probe: {proc.stderr}"
    )


def test_hook_does_not_shell_out_to_the_installed_console_script(tmp_path: Path) -> None:
    """The active-task answer must come from the in-tree source, not from PATH.

    Deterministic in place of a latency assertion: a hook that never spawns the
    console script cannot pay its ~2.1s, and it also cannot be broken by whatever
    version of that script happens to be installed.
    """
    _, marker, _ = _run_hook(tmp_path)

    assert not marker.exists(), (
        "guard-main-branch.sh executed the installed mcp-workbay-handoff console "
        "script. That is a separately versioned artifact resolved from PATH: it "
        "costs ~2.1s of a 5s hook budget on every main-branch edit, and when it "
        "is out of step with the handoff schema it fails and the hook silently "
        "treats the failure as 'no active task'. Resolve in-process via "
        "_active_task_context, as guard-worktree-drift.sh already does."
    )


def test_a_failed_probe_is_not_reported_as_no_active_task(tmp_path: Path) -> None:
    """The live repro: a crashing probe must not be phrased as a fact.

    The resolver is staged to report ``probe_error`` -- exactly what it returns
    when the handoff package cannot be imported or ``get_handoff_state`` raises,
    the state the installed console script has been failing in. Whether the hook
    stays silent or says it could not determine the answer is the implementer's
    call; what it may not do is assert the negative.
    """
    reason = "schema_version_mismatch: db user_version=33"
    proc, _, inject_marker = _run_hook(
        tmp_path, probe_error=reason, inject=True
    )
    _assert_injection_took_effect(inject_marker)

    assert proc.returncode == 0, f"the hook must stay advisory; rc={proc.returncode}"
    assert _WARNING_TEXT not in proc.stderr, (
        "the active-task probe failed, and the hook reported that as 'editing "
        "without an active handoff task' -- a claim it never established. A "
        "could-not-determine outcome must be distinguishable from a negative "
        f"one.\nstderr: {proc.stderr}"
    )


def test_a_raising_probe_is_not_reported_as_no_active_task(tmp_path: Path) -> None:
    """The other half of the same defect, one layer up in the shell.

    ``probe_error`` only covers failures the resolver *catches* and converts.
    Two failures escape as exceptions instead: the deliberately preserved
    ``UnresolvedTaskContextError`` escalation, and anything that blows up while
    importing the handoff package. Those exit the heredoc non-zero, and
    ``2>/dev/null || true`` on the command substitution swallows the exit
    exactly as it swallowed the console script's -- so ``PROBE_OUT`` is empty,
    which the hook reads as a clean negative and phrases as fact.

    That is the original defect verbatim, reintroduced by the very construct
    that was meant to remove it [OBS-08]/[AGT-10]. A non-zero probe is a
    could-not-determine outcome and must be treated as one.
    """
    proc, _, inject_marker = _run_hook(tmp_path, raises=True)
    _assert_injection_took_effect(inject_marker)

    assert proc.returncode == 0, (
        f"the hook must stay advisory even when the probe raises; "
        f"rc={proc.returncode}\nstderr: {proc.stderr}"
    )
    assert _WARNING_TEXT not in proc.stderr, (
        "the active-task probe raised, the non-zero exit was discarded by "
        "`|| true`, and the empty result was reported as 'editing without an "
        "active handoff task' -- a claim the hook never established. Every "
        "could-not-determine outcome must suppress the warning, not just the "
        f"ones the resolver catches.\nstderr: {proc.stderr}"
    )


def test_a_genuine_clean_negative_still_warns(tmp_path: Path) -> None:
    """Control arm: silencing the warning must not be how the arm above passes.

    Same injection point, opposite answer -- the resolver ran, answered, and
    found no active task. That is a real absence and must still be reported.
    Without this arm, deleting the warning block entirely satisfies the
    failed-probe test while destroying the guard's whole purpose.
    """
    proc, _, inject_marker = _run_hook(
        tmp_path, task_ref=None, probe_error=None, inject=True
    )
    _assert_injection_took_effect(inject_marker)

    assert proc.returncode == 0, f"the hook must stay advisory; rc={proc.returncode}"
    assert _WARNING_TEXT in proc.stderr, (
        "the probe answered cleanly that no handoff task is active, and the hook "
        "said nothing. Only a *failed* probe may be silent; a genuine absence "
        f"must still warn.\nstderr: {proc.stderr}"
    )


def test_a_resolved_task_is_not_warned_about(tmp_path: Path) -> None:
    """Second control arm: the happy path must stay quiet.

    Guards the other direction -- a hook that always warns would pass the
    clean-negative arm while nagging on every edit made under an active task,
    which is the behaviour this whole slice exists to stop.
    """
    proc, _, inject_marker = _run_hook(
        tmp_path, task_ref="internal", probe_error=None, inject=True
    )
    _assert_injection_took_effect(inject_marker)

    assert proc.returncode == 0, f"the hook must stay advisory; rc={proc.returncode}"
    assert _WARNING_TEXT not in proc.stderr, (
        "a handoff task was active and the hook warned anyway -- the resolved "
        f"task_ref was ignored.\nstderr: {proc.stderr}"
    )


def test_failed_probe_emits_reason_on_stderr(tmp_path: Path) -> None:
    """D3: a caught probe_error must surface a bounded reason [OBS-01][RES-03].

    ``2>/dev/null`` plus a bare PROBE_ERROR token used to discard *why* the
    probe failed. An error swallowed to keep the session advisory must still
    land somewhere the operator can find -- one line on stderr, no stack.
    """
    reason = "schema_version_mismatch: db user_version=33"
    proc, _, inject_marker = _run_hook(
        tmp_path, probe_error=reason, inject=True
    )
    _assert_injection_took_effect(inject_marker)

    assert proc.returncode == 0, f"the hook must stay advisory; rc={proc.returncode}"
    assert _PROBE_FAIL_PREFIX in proc.stderr, (
        "probe_error was set but stderr has no failure reason. Operators cannot "
        f"tell why the active-task probe failed.\nstderr: {proc.stderr}"
    )
    assert reason in proc.stderr, (
        f"expected the staged probe_error reason on stderr.\nstderr: {proc.stderr}"
    )
    assert "Traceback" not in proc.stderr, (
        f"failure reason must stay one line; no stack traces on the hot path.\n"
        f"stderr: {proc.stderr}"
    )


def test_raising_probe_emits_reason_on_stderr(tmp_path: Path) -> None:
    """D3 (raise path): non-zero probe exit must still leave a one-line reason."""
    proc, _, inject_marker = _run_hook(tmp_path, raises=True)
    _assert_injection_took_effect(inject_marker)

    assert proc.returncode == 0, f"the hook must stay advisory; rc={proc.returncode}"
    assert _PROBE_FAIL_PREFIX in proc.stderr, (
        f"raising probe left no failure reason on stderr.\nstderr: {proc.stderr}"
    )
    assert "Ambiguous active task" in proc.stderr, (
        f"expected the raised exception text on stderr.\nstderr: {proc.stderr}"
    )
    assert "Traceback" not in proc.stderr, (
        f"failure reason must stay one line; no stack traces on the hot path.\n"
        f"stderr: {proc.stderr}"
    )


def test_injection_marker_is_required_by_inject_arms(tmp_path: Path) -> None:
    """Structural pin: every inject-dependent arm must assert the marker [TEST-04].

    Without this, a future edit can drop the marker check from a property arm
    and the suite again green-washes a dead shim.
    """
    source = Path(__file__).read_text(encoding="utf-8")
    # Arms that stage answers via inject=True / raises=True.
    inject_call_sites = (
        "probe_error=reason, inject=True",
        "raises=True",
        "task_ref=None, probe_error=None, inject=True",
        'task_ref="internal", probe_error=None, inject=True',
    )
    for site in inject_call_sites:
        assert site in source, f"expected inject call site missing: {site}"
    assert source.count("_assert_injection_took_effect") >= 6, (
        "injection-dependent arms must call _assert_injection_took_effect; "
        f"found {source.count('_assert_injection_took_effect')} references"
    )


def test_the_absence_warning_survives_the_fix(tmp_path: Path) -> None:
    """Structural backstop for the warning text itself.

    The behavioural arm above proves the warning fires; this one proves the
    operator-facing text was not hollowed out to a bare newline to satisfy it.
    """
    source = HOOK_SCRIPT.read_text(encoding="utf-8")

    assert _WARNING_TEXT in source, (
        "the 'no active handoff task' warning was removed from "
        "guard-main-branch.sh. Not warning at all is not the fix -- a genuine "
        "absence must still be reported; only a *failed* probe must stop being "
        "reported as an absence."
    )


def test_the_console_script_is_gone_from_the_source(tmp_path: Path) -> None:
    """The call site itself must go, not merely stop being reached.

    Distinct from the marker arm: that one proves the script was not executed on
    *this* payload, which a conditional guard could also achieve while leaving
    the slow, version-fragile path one branch away. The dependency has to leave
    the file.
    """
    source = HOOK_SCRIPT.read_text(encoding="utf-8")

    assert "mcp-workbay-handoff" not in source, (
        "guard-main-branch.sh still names the installed console script. Even "
        "behind a guard, resolving the active task through a PATH-installed "
        "artifact reintroduces both the latency and the version-skew failure."
    )


def test_probe_does_not_use_or_true_on_command_substitution(tmp_path: Path) -> None:
    """Structural pin for D1: probe exit must not be swallowed as success [OBS-08]."""
    source = HOOK_SCRIPT.read_text(encoding="utf-8")
    # The defect was `... || true` on the probe substitution, which forced a
    # zero status and empty stdout. Require status capture instead.
    assert "PROBE_STATUS" in source, (
        "guard-main-branch.sh must capture the probe exit status separately "
        "from its stdout so a non-zero exit is could-not-determine."
    )
    assert "|| PROBE_STATUS=$?" in source, (
        "probe command substitution must record non-zero exit into PROBE_STATUS, "
        "not discard it."
    )
    # Reject the specific swallow pattern on executable lines.
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if stripped.endswith("|| true"):
            raise AssertionError(
                "executable line still ends with `|| true`, which can convert a "
                "raising probe into empty stdout and then report a clean negative: "
                f"{stripped}"
            )
