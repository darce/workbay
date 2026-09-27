"""A hook's own subprocess waits must fit inside its configured budget.

internal. ``guard-bash-main-branch.py`` bounded each of its git probes
with a static ``timeout=`` literal, and the literals summed past the ``timeout: 5``
the harness grants the hook in ``.claude/settings.json``. When git is slow the
hook is *killed* at 5s rather than reaching either of its own degrade paths --
and a killed PreToolUse hook denies the tool call. Measured over 50 recent
transcripts, ``PreToolUse:Bash`` timed out 28 times in 70 runs.

This is the failure mode ``test_branch_isolation_guard_timeout.py`` already
guards for git scans: a slow probe must be could-not-determine, never fatal
[RES-03]/[AGT-10]. The handlers here are correct and already written; they are
simply unreachable, because the budget was never made structural [ARCH-13].

**The invariant, restated.** An earlier revision of this file pinned the wrong
property: *the sum of the granted timeouts must be under the budget*. That is a
sufficient condition only for static per-call literals, and once a shared
deadline exists it is actively harmful -- the only way to satisfy a sum bound is
to hand later probes less and less, until they get zero and the guard fails open
on a host where git was never slow at all. Measured against the first
implementation of the shared budget: a ``git -C <dir> checkout -b`` with three
switch intents exhausted the pool after the *first* intent, in 0.7s of real time
with instant git, so the intent that actually targeted the primary worktree was
never checked and the misroute went unblocked.

What must actually hold is a **deadline** property, which the arms below pin:

1. **Bounded worst case.** If every probe hangs to its full grant, the hook's
   cumulative subprocess wait still lands inside the budget.
2. **No starvation on a healthy host.** When git returns promptly, every probe
   on the path still receives a positive bound and the guard still reaches its
   verdict. Unused patience must be refunded, not charged.

Both are needed. (1) alone is met by granting zero to everything; (2) alone is
met by granting unbounded patience to everything.
"""

from __future__ import annotations

import importlib.util
import io
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parent
HOOK_SCRIPT = HOOKS_DIR / "guard-bash-main-branch.py"
REPO_ROOT = HOOKS_DIR.parents[1]
CONTRACT_SOURCE = (
    REPO_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
)

# Interpreter start plus the hook's own non-subprocess work. Measured cold start
# for this hook is ~500ms on a warm git; a 1.5s reserve keeps the bound honest
# without pinning it to one machine's speed.
_NON_SUBPROCESS_RESERVE_SECONDS = 1.5


def _seed_contract(repo: Path) -> None:
    target = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(CONTRACT_SOURCE, target)


def _configured_hook_timeout(script_name: str) -> float | None:
    """Read the hook's registered timeout from the harness settings.

    Returns ``None`` when the settings file is not reachable -- these test files
    are twinned into the packaged payload tree, where ``.claude/settings.json``
    does not exist.
    """
    for parent in HOOKS_DIR.parents:
        settings = parent / ".claude" / "settings.json"
        if not settings.is_file():
            continue
        try:
            config = json.loads(settings.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        for entries in (config.get("hooks") or {}).values():
            for entry in entries or []:
                for hook in entry.get("hooks") or []:
                    if script_name in str(hook.get("command", "")):
                        timeout = hook.get("timeout")
                        if isinstance(timeout, (int, float)):
                            return float(timeout)
        return None
    return None


def _load_hook_module():
    spec = importlib.util.spec_from_file_location("_guard_bash_main_branch", HOOK_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_primary_with_linked_worktree(tmp_path: Path) -> Path:
    """A checkout that ``has_linked_worktrees`` accepts.

    The root-branch-switch guard returns early unless linked worktrees exist,
    and that early return is what keeps the expensive path out of a cheap
    ``echo hi`` measurement. ``has_linked_worktrees`` reads the filesystem
    rather than shelling out, so it cannot be stubbed alongside the git probes
    -- it needs a real ``.git/worktrees/<name>/gitdir`` whose target exists.
    """
    linked = tmp_path / "linked-worktree"
    linked.mkdir()
    entry = tmp_path / ".git" / "worktrees" / "wt1"
    entry.mkdir(parents=True)
    (entry / "gitdir").write_text(f"{linked}/.git\n", encoding="utf-8")
    (linked / ".git").write_text("gitdir: whatever\n", encoding="utf-8")
    return tmp_path


def _drive(module, root: Path, command: str) -> tuple[int, str]:
    """Feed the hook one PreToolUse Bash payload; return (exit_code, stderr)."""
    payload = json.dumps(
        {
            "session_id": "t",
            "transcript_path": "/dev/null",
            "cwd": str(root),
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": command},
        }
    )
    stdin = io.StringIO(payload)
    stderr = io.StringIO()
    original_in, original_err = sys.stdin, sys.stderr
    sys.stdin = stdin
    sys.stderr = stderr
    try:
        code = module.main()
    except SystemExit as exc:  # pragma: no cover - defensive
        code = int(exc.code or 0)
    finally:
        sys.stdin = original_in
        sys.stderr = original_err
    return int(code), stderr.getvalue()


def test_worst_case_subprocess_wait_fits_the_configured_budget(monkeypatch, tmp_path):
    """Every probe hangs to its full grant; cumulative wait stays in budget.

    This is the safety half of the invariant, and it is asserted against a fake
    clock rather than by sleeping: each stubbed probe advances time by exactly
    the timeout it was granted, which is what a hung git would do. A hook that
    outlives its budget is killed before any of its degrade paths run, and a
    killed PreToolUse hook denies the tool call.

    **Why five switch intents and not one.** ``_worktree_toplevel`` is called
    once per intent inside the loop, so the number of probes on this path is
    driven by the command, not by the code. With a single intent the static caps
    happen to sum to exactly the budget, and an implementation that ignored the
    deadline entirely and always granted the full cap would still pass -- the
    arm would be pinning a coincidence. Measured: that mutation survived the
    single-intent form. Five intents drive the cap-sum to 9.0s against a 3.5s
    budget, so the arm passes only if the shared deadline is actually clamping.

    The stub resolves every intent to a *linked* worktree so none of them
    matches primary by path; under a full deadline the mid-loop zero grant must
    fail closed (exit 2 + BLOCKED), not silently allow [TEST-04][D2][D4]. A
    hook that stops guarding (exit 0) fails this arm even if wall time fits.
    """
    budget = _configured_hook_timeout("guard-bash-main-branch.py")
    if budget is None:
        pytest.skip("harness settings not reachable from this tree")

    root = _fake_primary_with_linked_worktree(tmp_path)
    module = _load_hook_module()

    clock = {"now": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: clock["now"])

    granted: list[float] = []

    def _hang_to_the_bound(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        cmd_text = " ".join(str(part) for part in cmd)
        timeout = kwargs.get("timeout")
        if isinstance(timeout, (int, float)):
            granted.append(float(timeout))
            clock["now"] += float(timeout)  # a probe that hangs to its bound
        if "--show-current" in cmd_text:
            resolved = "main"
        elif "--show-toplevel" in cmd_text and "-C" in cmd_text:
            resolved = str(root / "linked-worktree")  # never the primary
        else:
            resolved = str(root)
        return subprocess.CompletedProcess(cmd, 0, stdout=resolved, stderr="")

    monkeypatch.setattr(module.subprocess, "run", _hang_to_the_bound)

    exit_code, stderr = _drive(
        module,
        root,
        " && ".join(f"git -C {root}/w{i} checkout -b feature/some-work-{i}" for i in range(5)),
    )

    assert granted, (
        "no subprocess timeout was observed -- the recording stub did not "
        "intercept the hook's git probes, so this arm proves nothing"
    )
    allowed = budget - _NON_SUBPROCESS_RESERVE_SECONDS
    assert clock["now"] <= allowed, (
        f"with every probe hanging to its bound the hook waited {clock['now']}s, "
        f"but the harness kills it at {budget}s (reserving "
        f"{_NON_SUBPROCESS_RESERVE_SECONDS}s for interpreter start and the "
        f"hook's own work, leaving {allowed}s). Grants: {granted}."
    )
    # Exit code + message pin: a hook that stops guarding still satisfies the
    # wall-clock bound, so without these asserts the arm is vacuous [TEST-04].
    assert exit_code == 2, (
        "worst-case deadline exhaustion must fail closed (exit 2), not silently "
        f"allow; got exit {exit_code}. stderr={stderr!r}. Grants: {granted}."
    )
    assert "BLOCKED" in stderr, (
        "fail-closed path must emit a BLOCKED message an operator can see "
        f"[OBS-08]; stderr={stderr!r}"
    )


def test_a_fast_git_does_not_starve_the_later_probes(monkeypatch, tmp_path):
    """The anti-starvation half: unused patience must be refunded.

    A budget that charges each probe its granted *cap* rather than the time it
    actually consumed drains to zero on a host where git answered instantly.
    The probes that follow then get a zero grant and take their
    could-not-determine paths -- so the guard silently stops guarding, on a
    healthy machine, for no reason an operator can observe [OBS-08].

    This is asserted where it bites hardest: three switch intents where only the
    *third* targets the primary worktree. The verdict depends on the guard still
    having budget left by the time it reaches that intent. Measured against the
    first shared-budget implementation, the pool was empty after the first
    intent and this misroute was allowed through.
    """
    budget = _configured_hook_timeout("guard-bash-main-branch.py")
    if budget is None:
        pytest.skip("harness settings not reachable from this tree")

    root = _fake_primary_with_linked_worktree(tmp_path)
    module = _load_hook_module()

    clock = {"now": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: clock["now"])

    granted: list[tuple[str, float]] = []

    def _fast_git(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        cmd_text = " ".join(str(part) for part in cmd)
        timeout = kwargs.get("timeout")
        if isinstance(timeout, (int, float)):
            granted.append((cmd_text, float(timeout)))
        clock["now"] += 0.01  # a healthy git: ~10ms per probe
        if "--show-toplevel" in cmd_text and "-C" in cmd_text:
            # Only the third intent's target resolves to the primary worktree.
            resolved = str(root) if "/third " in f"{cmd_text} " else str(root / "linked-worktree")
        elif "--show-current" in cmd_text:
            resolved = "main"
        else:
            resolved = str(root)
        return subprocess.CompletedProcess(cmd, 0, stdout=resolved, stderr="")

    monkeypatch.setattr(module.subprocess, "run", _fast_git)

    exit_code, stderr = _drive(
        module,
        root,
        f"git -C {root}/first checkout -b f1 "
        f"&& git -C {root}/second checkout -b f2 "
        f"&& git -C {root}/third checkout -b f3",
    )

    starved = [cmd for cmd, timeout in granted if timeout <= 0]
    assert not starved, (
        f"these probes were granted a zero timeout after only {clock['now']:.2f}s "
        f"of real work against a {budget}s budget, so they never ran and the "
        f"guard fell back to could-not-determine on a healthy host: {starved}. "
        f"All grants: {granted}"
    )
    assert exit_code == 2, (
        "the third switch intent targets the PRIMARY worktree and must be "
        "blocked, but the guard returned "
        f"{exit_code}. Probes granted: {granted}. stderr={stderr!r}. A budget "
        "that charges the granted cap rather than the elapsed time runs out "
        "before reaching the intent that mattered, and every unchecked intent "
        "fails open."
    )


def test_every_git_probe_declares_a_timeout(monkeypatch, tmp_path):
    """Control arm: the budget must be met by bounding, not by dropping bounds.

    Deleting ``timeout=`` from the probes drives the measured sum to zero and
    satisfies any budget assertion above while making the hook wait forever --
    strictly worse than the defect. Every subprocess the hook starts and waits
    on must still carry a bound.
    """
    module = _load_hook_module()
    seen: list[tuple[list[str], object]] = []

    def _record(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        seen.append((cmd, kwargs.get("timeout")))
        return subprocess.CompletedProcess(cmd, 0, stdout=str(tmp_path), stderr="")

    monkeypatch.setattr(module.subprocess, "run", _record)
    _drive(module, tmp_path, "echo hi")

    assert seen, "the hook started no subprocess; this arm proves nothing"
    unbounded = [cmd for cmd, timeout in seen if not isinstance(timeout, (int, float))]
    assert not unbounded, (
        f"these subprocess calls carry no timeout and can hang until the "
        f"harness kills the hook: {unbounded}"
    )


def test_undetermined_branch_still_runs_protected_write_scan(monkeypatch, tmp_path):
    """Could-not-determine branch (``None``) still runs the write scan [RES-03].

    Strengthening note: previously this pin mocked ``_current_branch -> ""``,
    which encoded the bug that collapsed detached HEAD into undetermined.
    ``None`` is now the could-not-determine sentinel; empty string is detached
    (see ``test_detached_head_still_runs_protected_write_scan``).
    """
    if not CONTRACT_SOURCE.is_file():
        pytest.skip("harness contract not reachable from this tree")

    root = tmp_path / "repo"
    root.mkdir()
    _seed_contract(root)
    # Ensure sibling imports resolve when main inserts repo_root/scripts/hooks.
    hooks_link = root / "scripts" / "hooks"
    hooks_link.mkdir(parents=True)
    for name in (
        "_bash_isolation_guard.py",
        "_harness_protocol.py",
        "_worktree_identity.py",
        "_interp.py",
    ):
        src = HOOKS_DIR / name
        if src.is_file():
            (hooks_link / name).symlink_to(src)

    module = _load_hook_module()
    monkeypatch.setattr(module, "_current_branch", lambda _repo: None)
    monkeypatch.setattr(module, "_repo_root", lambda: root)
    monkeypatch.setattr(module, "_detect_root_branch_switch", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "_record_terminal_guard_block", lambda **_k: None)

    exit_code, stderr = _drive(module, root, "echo x > Makefile")
    assert exit_code == 2, (
        "could-not-determine branch must still block protected writes; "
        f"got exit {exit_code}. stderr={stderr!r}"
    )
    assert "BLOCKED" in stderr
    assert "Makefile" in stderr
    assert "could not determine current branch" in stderr
    assert "Branch: (unknown)" in stderr


def test_detached_head_still_runs_protected_write_scan(monkeypatch, tmp_path):
    """D3 (9541): rc0 + empty branch is detached HEAD, not undetermined.

    ``git branch --show-current`` returns empty stdout with exit 0 on detached
    HEAD. Write scan must still run; the diagnostic and BLOCKED line must name
    detached HEAD rather than could-not-determine.
    """
    if not CONTRACT_SOURCE.is_file():
        pytest.skip("harness contract not reachable from this tree")

    root = tmp_path / "repo"
    root.mkdir()
    _seed_contract(root)
    hooks_link = root / "scripts" / "hooks"
    hooks_link.mkdir(parents=True)
    for name in (
        "_bash_isolation_guard.py",
        "_harness_protocol.py",
        "_worktree_identity.py",
        "_interp.py",
    ):
        src = HOOKS_DIR / name
        if src.is_file():
            (hooks_link / name).symlink_to(src)

    module = _load_hook_module()
    monkeypatch.setattr(module, "_current_branch", lambda _repo: "")
    monkeypatch.setattr(module, "_repo_root", lambda: root)
    monkeypatch.setattr(module, "_detect_root_branch_switch", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "_record_terminal_guard_block", lambda **_k: None)

    exit_code, stderr = _drive(module, root, "echo x > Makefile")
    assert exit_code == 2, (
        "detached HEAD must still block protected writes; "
        f"got exit {exit_code}. stderr={stderr!r}"
    )
    assert "BLOCKED" in stderr
    assert "Makefile" in stderr
    assert "detached HEAD" in stderr
    assert "Branch: (detached HEAD)" in stderr
    assert "could not determine current branch" not in stderr


def test_deleted_cwd_does_not_crash_repo_root(monkeypatch):
    """D2 (9935): Path.cwd() FileNotFoundError must fail closed, not crash.

    A deleted agent cwd (e.g. after make task-finish removes the worktree)
    makes Path.cwd() raise. Exit 1 from PreToolUse is non-blocking, so a crash
    silently disables the write scan. Must return the could-not-determine
    sentinel instead.
    """
    module = _load_hook_module()

    def _gone():
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(module.Path, "cwd", staticmethod(_gone))
    monkeypatch.setattr(module, "_probe_timeout", lambda _cap: 0.0)

    result = module._repo_root()
    assert result is module._COULD_NOT_DETERMINE, (
        f"deleted cwd with zero budget must be could-not-determine, got {result!r}"
    )


def test_deleted_cwd_main_fails_closed(monkeypatch, tmp_path):
    """D2 (9935): main() must exit 2 on deleted cwd, never raise SystemExit(1)."""
    module = _load_hook_module()

    def _gone():
        raise FileNotFoundError(2, "No such file or directory")

    def _timeout_or_gone(*_a, **_k):
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(module.Path, "cwd", staticmethod(_gone))
    monkeypatch.setattr(module.subprocess, "run", _timeout_or_gone)

    exit_code, stderr = _drive(module, tmp_path, "echo hi")
    assert exit_code == 2, (
        f"deleted cwd must fail closed with exit 2, got {exit_code}; stderr={stderr!r}"
    )
    assert "could not determine repository root" in stderr
    assert "BLOCKED" in stderr


def test_repo_root_timeout_is_could_not_determine(monkeypatch):
    """D6 (9982): TimeoutExpired from rev-parse must not fall back to cwd."""
    module = _load_hook_module()

    def _timeout(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="git", timeout=1.0)

    monkeypatch.setattr(module.subprocess, "run", _timeout)
    # Ensure a live cwd exists so a buggy fallback would succeed silently.
    monkeypatch.setattr(module, "_probe_timeout", lambda _cap: 1.0)

    result = module._repo_root()
    assert result is module._COULD_NOT_DETERMINE, (
        f"TimeoutExpired must be could-not-determine, not cwd fallback; got {result!r}"
    )


def test_repo_root_timeout_main_fails_closed(monkeypatch, tmp_path):
    """D6 (9982): hung rev-parse must fail closed at main, not key off cwd."""
    module = _load_hook_module()

    def _timeout(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="git", timeout=1.0)

    monkeypatch.setattr(module.subprocess, "run", _timeout)

    exit_code, stderr = _drive(module, tmp_path, "echo hi")
    assert exit_code == 2, (
        f"rev-parse timeout must fail closed with exit 2, got {exit_code}; "
        f"stderr={stderr!r}"
    )
    assert "could not determine repository root" in stderr


def test_mid_loop_zero_grant_fails_closed_not_silent_skip(monkeypatch, tmp_path):
    """D2: deadline exhaustion mid-loop is whole-scan could-not-determine.

    When the third intent targets the primary, a zero grant must not skip it
    and report clean — fail closed with an observable BLOCKED message [OBS-08].
    """
    budget = _configured_hook_timeout("guard-bash-main-branch.py")
    if budget is None:
        pytest.skip("harness settings not reachable from this tree")

    root = _fake_primary_with_linked_worktree(tmp_path)
    module = _load_hook_module()

    clock = {"now": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: clock["now"])

    probes: list[str] = []

    def _slow_then_zero(cmd, **kwargs):  # noqa: ANN001, ANN003
        cmd_text = " ".join(str(part) for part in cmd)
        timeout = kwargs.get("timeout")
        if isinstance(timeout, (int, float)):
            # Consume the full grant so later probes see a zero budget.
            clock["now"] += float(timeout)
        probes.append(cmd_text)
        if "--show-current" in cmd_text:
            resolved = "main"
        elif "--show-toplevel" in cmd_text and "-C" in cmd_text:
            # First two intents: linked; third would be primary — but budget
            # should be gone before we resolve it under a hanging earlier probe.
            resolved = (
                str(root) if "/third " in f"{cmd_text} " else str(root / "linked-worktree")
            )
        else:
            resolved = str(root)
        return subprocess.CompletedProcess(cmd, 0, stdout=resolved, stderr="")

    monkeypatch.setattr(module.subprocess, "run", _slow_then_zero)

    exit_code, stderr = _drive(
        module,
        root,
        f"git -C {root}/first checkout -b f1 "
        f"&& git -C {root}/second checkout -b f2 "
        f"&& git -C {root}/third checkout -b f3",
    )
    assert exit_code == 2, (
        "mid-loop zero grant must fail closed, not exit 0 as if checked clean; "
        f"got {exit_code}. probes={probes!r} stderr={stderr!r}"
    )
    assert "BLOCKED" in stderr
    # Either we blocked the primary switch or we failed closed on budget —
    # both are safe; silent allow is not.
    assert (
        "PRIMARY" in stderr
        or "deadline exhausted" in stderr
        or "could not complete" in stderr
    )


def test_signature_skew_does_not_silently_disable_switch_guard(monkeypatch, tmp_path):
    """D3: TypeError from primary_workspace_root must not become silent None.

    Partial upgrades where ``timeout=`` is unsupported used to disable the
    entire switch guard via bare ``except Exception: return None`` [CON-11].
    """
    root = _fake_primary_with_linked_worktree(tmp_path)
    module = _load_hook_module()

    def _always_type_error(*_a, **_k):  # noqa: ANN001, ANN003
        # Raise regardless of kwargs: the production call site no longer
        # passes timeout=, but a signature-skew TypeError must still fail
        # closed rather than becoming a silent None [CON-11][D3].
        raise TypeError("unexpected keyword argument 'timeout'")

    # Inject a skew-shaped primary_workspace_root via a fake sibling module.
    import types

    fake = types.ModuleType("_worktree_identity")
    fake.has_linked_worktrees = lambda _p: True  # type: ignore[attr-defined]
    fake.primary_workspace_root = _always_type_error  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "_worktree_identity", fake)

    def _fast(cmd, **kwargs):  # noqa: ANN001, ANN003
        cmd_text = " ".join(str(part) for part in cmd)
        if "--show-current" in cmd_text:
            out = "main"
        else:
            out = str(root)
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    monkeypatch.setattr(module.subprocess, "run", _fast)

    exit_code, stderr = _drive(
        module,
        root,
        f"git -C {root} checkout -b feature/skew-test",
    )
    assert exit_code == 2, (
        "signature skew must fail closed with an explicit signal, not silent "
        f"allow; got exit {exit_code}. stderr={stderr!r}"
    )
    assert "BLOCKED" in stderr
    assert "signature skew" in stderr or "could not complete" in stderr


def test_pre_loop_deadline_exhaustion_fails_closed(monkeypatch, tmp_path):
    """W4: deadline gone before primary identity probe fails closed [AGT-10].

    Pins the pre-loop ``primary_timeout <= 0`` return. A ``pass`` mutation that
    falls through into the scan must not keep this arm green [TEST-04].
    """
    root = _fake_primary_with_linked_worktree(tmp_path)
    module = _load_hook_module()

    monkeypatch.setattr(module, "_repo_root", lambda: root)
    monkeypatch.setattr(module, "_current_branch", lambda _r: "main")
    monkeypatch.setattr(module, "_record_terminal_guard_block", lambda **_k: None)

    real_probe = module._probe_timeout

    def _probe(cap: float) -> float:
        if float(cap) == float(module._PRIMARY_ROOT_TIMEOUT_CAP):
            return 0.0
        return real_probe(cap)

    monkeypatch.setattr(module, "_probe_timeout", _probe)

    def _fast(cmd, **kwargs):  # noqa: ANN001, ANN003
        cmd_text = " ".join(str(part) for part in cmd)
        if "--show-current" in cmd_text:
            out = "main"
        else:
            out = str(root)
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    monkeypatch.setattr(module.subprocess, "run", _fast)

    exit_code, stderr = _drive(
        module,
        root,
        f"git -C {root} checkout -b feature/pre-loop-deadline",
    )
    assert exit_code == 2, (
        "pre-loop deadline exhaustion must block (exit 2), not silently allow; "
        f"got exit {exit_code}. stderr={stderr!r}"
    )
    assert "BLOCKED" in stderr
    assert "deadline exhausted before primary-worktree identity probe" in stderr, (
        f"must name the pre-loop primary-probe exhaustion path; stderr={stderr!r}"
    )


def test_deps_python_never_uses_agent_cwd(monkeypatch, tmp_path):
    """D5: interpreter must not be chosen from the agent's cwd.

    An unrelated project ``.venv`` under cwd must lose to the script-tree /
    primary-checkout probe (or fall through to ``sys.executable``).
    """
    module = _load_hook_module()

    foreign = tmp_path / "foreign-project"
    foreign_venv = foreign / ".venv" / "bin" / "python"
    foreign_venv.parent.mkdir(parents=True)
    foreign_venv.write_text("#!/bin/sh\n", encoding="utf-8")
    foreign_venv.chmod(0o755)

    # No script-tree or primary .venv: only cwd would have found foreign.
    monkeypatch.setattr(module, "_primary_checkout_root", lambda: None)
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: foreign))
    # Point __file__ at a tmp tree with no .venv so parents[2] misses.
    decoy_hooks = tmp_path / "decoy" / "scripts" / "hooks"
    decoy_hooks.mkdir(parents=True)
    decoy_script = decoy_hooks / "guard-bash-main-branch.py"
    decoy_script.write_text("# decoy\n", encoding="utf-8")
    monkeypatch.setattr(module, "__file__", str(decoy_script))

    chosen = module._deps_python()
    assert chosen != str(foreign_venv), (
        f"_deps_python selected the agent's cwd venv: {chosen}"
    )
    assert chosen == sys.executable, (
        f"expected fallback to sys.executable, got {chosen}"
    )


def test_deps_python_prefers_primary_checkout_venv(tmp_path, monkeypatch):
    """D5: restore primary-checkout probe when script-tree root has no venv."""
    module = _load_hook_module()

    primary = tmp_path / "primary"
    primary_venv = primary / ".venv" / "bin" / "python"
    primary_venv.parent.mkdir(parents=True)
    primary_venv.write_text("#!/bin/sh\n", encoding="utf-8")
    primary_venv.chmod(0o755)

    decoy_hooks = tmp_path / "payload" / "scripts" / "hooks"
    decoy_hooks.mkdir(parents=True)
    decoy_script = decoy_hooks / "guard-bash-main-branch.py"
    decoy_script.write_text("# decoy\n", encoding="utf-8")
    monkeypatch.setattr(module, "__file__", str(decoy_script))
    monkeypatch.setattr(module, "_primary_checkout_root", lambda: primary)
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: tmp_path / "elsewhere"))

    chosen = module._deps_python()
    assert chosen == str(primary_venv)
