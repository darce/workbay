"""Run one mutant: sandbox -> mutate -> select tests -> run -> classify."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from mutation_harness.coverage_map import (
    CoverageMap,
    CoverageMapError,
    is_import_or_module_scope,
    load_or_build_coverage_map,
    mutation_lines,
)
from mutation_harness.models import FallbackReason, Mutant, MutantResult, SelectionMode
from mutation_harness.mutate import MutationError, apply_mutation, locate_mutation_spans
from mutation_harness.sandbox import (
    SandboxError,
    create_sandbox,
    destroy_sandbox,
    resolve_sandbox_target,
)


class RunnerError(RuntimeError):
    """Internal runner failure before/around the test subprocess."""


# Sandbox environment policy: deny ambient pytest controls and runner
# topology metadata so a parent test run cannot alter child startup,
# collection, or filesystem paths. Core sandbox settings below still win.
_SANDBOX_ENV_DENYLIST = frozenset(
    {
        "CI",
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "PYTEST_PLUGINS",
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "TOX_ENV_DIR",
    }
)
# Pytest exit 1 means test failures; exit 5 means no tests were collected.
# Both make a selection unusable, while other nonzero exits are runner errors.
_PYTEST_SELECTION_FAILURE_CODES = frozenset({1, 5})


@dataclass(frozen=True, slots=True)
class TestSelection:
    tests: tuple[str, ...]
    mode: SelectionMode
    fallback_reason: FallbackReason | None = None


@dataclass(frozen=True, slots=True)
class _ControlOutcome:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool


_ControlCache = dict[tuple[str, ...], _ControlOutcome]


def _select_test_plan(
    mutant: Mutant,
    *,
    full_suite: bool = False,
    coverage_map: CoverageMap | None = None,
    source_root: Path | None = None,
) -> TestSelection:
    if full_suite:
        return TestSelection((), "full_suite")
    if mutant.tests:
        return TestSelection(tuple(mutant.tests), "manifest")
    if coverage_map is None or source_root is None:
        return TestSelection(
            (), "full_suite_fallback", fallback_reason="coverage_map_unavailable"
        )

    coverage_map.assert_fresh(source_root)
    lines = mutation_lines(mutant, source_root)
    if is_import_or_module_scope(mutant, source_root, lines):
        return TestSelection(
            (), "full_suite_fallback", fallback_reason="import_or_module_scope"
        )
    if coverage_map.has_unattributed_execution(mutant.target, lines):
        return TestSelection(
            (), "full_suite_fallback", fallback_reason="unattributed_execution"
        )
    selected = tuple(coverage_map.tests_for(mutant.target, lines))
    if not selected:
        # Direct coverage cannot prove that no test reaches this line: child
        # processes and other coverage contexts can be invisible. Run the full
        # suite and label the reason instead of asserting test absence.
        return TestSelection(
            (),
            "full_suite_fallback",
            fallback_reason="coverage_blind_execution",
        )
    return TestSelection(selected, "coverage")


def select_tests(
    mutant: Mutant,
    *,
    full_suite: bool = False,
    coverage_map: CoverageMap | None = None,
    source_root: Path | None = None,
) -> TestSelection:
    """Return a typed pytest selection plan for this mutant.

    Explicit manifest lists win; ``full_suite`` forces empty selection (full
    suite). Otherwise, an empty manifest list is resolved from direct dynamic
    coverage. The return type keeps an empty full-suite argument list distinct
    from coverage-blind fallback, so callers cannot mistake absence for benign
    full-suite selection.
    """
    return _select_test_plan(
        mutant,
        full_suite=full_suite,
        coverage_map=coverage_map,
        source_root=source_root,
    )


def classify_pytest_outcome(
    returncode: int,
    *,
    timed_out: bool,
    stdout: str = "",
    stderr: str = "",
    selected_tests: Sequence[str] | None = None,
    unmutated_control_passed: bool = False,
) -> tuple[str, list[str], str | None]:
    """Map a pytest-like exit to (status, killing_tests, error_message).

    ``unmutated_control_passed`` must explicitly attest that the identical
    selection was green before mutation. Convention (pytest):
    - 0: all passed -> mutant survived
    - 1: tests failed -> mutant killed (failures are the killing tests)
    - other: collection/internal error -> error
    """
    if timed_out:
        return "hung", [], "per-mutant timeout exceeded"
    if returncode in (0, 1) and not unmutated_control_passed:
        selection = (
            ", ".join(selected_tests) if selected_tests else "<full pytest suite>"
        )
        return (
            "unusable_test_selection",
            [],
            f"cannot classify without a green unmutated control ({selection})",
        )
    if returncode == 0:
        return "survived", [], None
    if returncode == 1:
        killers = _extract_failed_nodeids(stdout, stderr, selected_tests)
        return "killed", killers, None
    msg = (stderr or stdout or f"pytest exit {returncode}").strip()
    if len(msg) > 500:
        msg = msg[:500] + "…"
    return "error", [], msg or f"pytest exit {returncode}"


def _unusable_selection_message(
    selected_tests: Sequence[str], outcome: _ControlOutcome
) -> str:
    selection = ", ".join(selected_tests) if selected_tests else "<full pytest suite>"
    if outcome.timed_out:
        detail = "timed out"
    else:
        output = (outcome.stderr or outcome.stdout).strip()
        if len(output) > 300:
            output = output[-300:]
        detail = f"pytest exit {outcome.returncode}"
        if output:
            detail += f": {output}"
    return f"unmutated test selection is unusable ({selection}): {detail}"


def _classify_control_failure(
    selected_tests: Sequence[str], outcome: _ControlOutcome
) -> tuple[str, str]:
    """Classify a failed unmutated control without blaming selection blindly."""
    if outcome.timed_out or outcome.returncode in _PYTEST_SELECTION_FAILURE_CODES:
        return "unusable_test_selection", _unusable_selection_message(
            selected_tests, outcome
        )
    _, _, error_message = classify_pytest_outcome(
        outcome.returncode,
        timed_out=False,
        stdout=outcome.stdout,
        stderr=outcome.stderr,
        selected_tests=selected_tests,
    )
    return "error", error_message or f"pytest exit {outcome.returncode}"


def _extract_failed_nodeids(
    stdout: str,
    stderr: str,
    selected_tests: Sequence[str] | None,
) -> list[str]:
    """Best-effort extract of failed node ids from pytest output."""
    text = stdout + "\n" + stderr
    found: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        # pytest short summary: FAILED path::test_name
        if s.startswith("FAILED "):
            node = s[len("FAILED ") :].split()[0]
            if node and node not in found:
                found.append(node)
    if found:
        return found
    # Fallback: if we only ran explicit tests, report them as killers on fail.
    if selected_tests:
        return list(selected_tests)
    return []


def build_sandbox_env(
    sandbox_root: Path,
    *,
    source_root: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> dict[str, str]:
    """Environment for the pytest subprocess inside a mutant sandbox.

    - ``PYTHONPATH`` puts the sandbox first so mutated code is imported.
    - ``TMPDIR`` / ``TEMP`` / ``TMP`` / ``HOME`` redirect into the sandbox so
      casual temp writes do not land on the host tree.
    - ``PYTHONNOUSERSITE`` avoids user-site surprises.
    - Ambient pytest controls and runner topology metadata are removed by the
      named sandbox environment denylist above.
    - ``MUTATION_HARNESS_SOURCE_ROOT`` is consumed by the path-fix launcher to
      drop host editable-install / ``.pth`` redirects that would otherwise
      import unmutated code (silent no-op mutants).

    ``extra_env`` may extend ``PYTHONPATH`` but cannot displace the sandbox
    prefix — caller paths are appended after the sandbox root. Denylisted
    names are removed from both inherited and explicit environment values.
    """
    sandbox_s = str(Path(sandbox_root).resolve())
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in _SANDBOX_ENV_DENYLIST
    }
    extra = dict(extra_env or {})
    extra_pp = extra.pop("PYTHONPATH", None)
    path_parts: list[str] = [sandbox_s]
    if extra_pp and extra_pp.strip():
        path_parts.extend(p for p in extra_pp.split(os.pathsep) if p)
    existing = env.get("PYTHONPATH", "").strip()
    if existing:
        path_parts.extend(p for p in existing.split(os.pathsep) if p)
    # Dedupe while keeping order (sandbox stays first).
    seen: set[str] = set()
    ordered: list[str] = []
    for p in path_parts:
        key = os.path.realpath(p) if p else p
        if key in seen:
            continue
        seen.add(key)
        ordered.append(p)
    env["PYTHONPATH"] = os.pathsep.join(ordered)
    tmp = Path(sandbox_root) / ".mutharness-tmp"
    home = Path(sandbox_root) / ".mutharness-home"
    tmp.mkdir(parents=True, exist_ok=True)
    home.mkdir(parents=True, exist_ok=True)
    env["TMPDIR"] = str(tmp)
    env["TEMP"] = str(tmp)
    env["TMP"] = str(tmp)
    env["HOME"] = str(home)
    env["PYTHONNOUSERSITE"] = "1"
    if source_root is not None:
        env["MUTATION_HARNESS_SOURCE_ROOT"] = str(Path(source_root).resolve())
    env["MUTATION_HARNESS_SANDBOX"] = sandbox_s
    # Apply remaining extra_env after core isolation keys so callers can
    # override TEMP/HOME if needed, but PYTHONPATH was already merged above.
    if extra:
        env.update(extra)
    # Keep the denylist authoritative even when callers provide extra_env;
    # explicit sandbox settings cannot reintroduce ambient pytest controls.
    for key in _SANDBOX_ENV_DENYLIST:
        env.pop(key, None)
    # Re-assert sandbox-first PYTHONPATH if a caller tried to set it indirectly.
    env["PYTHONPATH"] = os.pathsep.join(ordered)
    env["MUTATION_HARNESS_SANDBOX"] = sandbox_s
    if source_root is not None:
        env["MUTATION_HARNESS_SOURCE_ROOT"] = str(Path(source_root).resolve())
    return env


def prefer_sandbox_sys_path(
    sandbox: str,
    source_root: str | None = None,
    path_entries: list[str] | None = None,
) -> list[str]:
    """Return a sys.path with ``sandbox`` first and ``source_root`` removed.

    Editable installs drop a ``.pth`` that runs ``sys.path.insert(0, <root>)``
    during site init *after* PYTHONPATH is applied. Without this reorder, the
    pytest child imports unmutated host code and every mutant "survives".
    """

    def _real(p: str) -> str:
        try:
            return os.path.realpath(p)
        except OSError:
            return p

    sb_r = _real(sandbox)
    src_r = _real(source_root) if source_root else None
    entries = list(sys.path if path_entries is None else path_entries)
    cleaned: list[str] = []
    for p in entries:
        rp = _real(p) if p else p
        if src_r and rp == src_r:
            continue
        if rp == sb_r:
            continue
        cleaned.append(p)
    return [sandbox] + cleaned


def _pytest_launcher_code(test_args: Sequence[str]) -> str:
    """Python -c payload: re-order sys.path then invoke pytest.

    Editable installs drop a ``.pth`` that ``sys.path.insert(0, <root>)``.
    That runs during site init *after* PYTHONPATH is applied, so a bare
    ``python -m pytest`` with cwd=sandbox still imports unmutated host code.
    This launcher re-prioritizes the sandbox and drops the recorded source
    root before pytest (and the package under test) is imported.
    """
    args_list = ["-q", "--tb=no", *list(test_args)]
    # Inline the same algorithm as prefer_sandbox_sys_path (child has no package).
    return (
        "import os, sys\n"
        "sb = os.environ.get('MUTATION_HARNESS_SANDBOX')\n"
        "src = os.environ.get('MUTATION_HARNESS_SOURCE_ROOT')\n"
        "def _real(p):\n"
        "    try:\n"
        "        return os.path.realpath(p)\n"
        "    except OSError:\n"
        "        return p\n"
        "if sb:\n"
        "    sb_r = _real(sb)\n"
        "    src_r = _real(src) if src else None\n"
        "    cleaned = []\n"
        "    for p in sys.path:\n"
        "        rp = _real(p)\n"
        "        if src_r and rp == src_r:\n"
        "            continue\n"
        "        if rp == sb_r:\n"
        "            continue\n"
        "        cleaned.append(p)\n"
        "    sys.path[:] = [sb] + cleaned\n"
        "import pytest\n"
        f"raise SystemExit(pytest.main({args_list!r}))\n"
    )


# Loud opt-out only. Default is refuse when no process isolation mechanism
# is available — never silently run mutants with cwd-only scoping.
_ALLOW_UNISOLATED_FS_ENV = "MUTATION_HARNESS_ALLOW_UNISOLATED_FS"


def _sandbox_exec_profile(sandbox_resolved: str) -> str:
    """Seatbelt profile: deny all writes, re-allow only the sandbox tree.

    ``sandbox_resolved`` must already be realpath'd. On macOS, ``/tmp`` and
    ``/var`` are symlinks into ``/private/...``; subpath rules match the
    resolved form only.
    """
    # Escape backslash and double-quote for SBPL string literals.
    path = sandbox_resolved.replace("\\", "\\\\").replace('"', '\\"')
    return (
        "(version 1)\n"
        "(allow default)\n"
        "(deny file-write*)\n"
        f'(allow file-write* (subpath "{path}"))\n'
        # Python/pytest often open /dev/null; keep that after the global deny.
        '(allow file-write* (literal "/dev/null"))\n'
    )


def wrap_with_filesystem_isolation(
    cmd: list[str],
    sandbox_root: Path,
) -> list[str]:
    """Wrap ``cmd`` so the child cannot write outside the sandbox.

    Mechanism selection probes the host (not ``sys.platform`` alone):

    1. **bubblewrap** (``bwrap``) — Linux: host root read-only, sandbox
       bind-mounted writable, private tmpfs on ``/tmp``.
    2. **sandbox-exec** — macOS Seatbelt: deny ``file-write*`` globally,
       re-allow only the resolved sandbox subpath (and ``/dev/null``).

    Path-join containment alone is not process isolation; this is the
    process boundary. When **neither** mechanism is on ``PATH``, this
    **refuses** (raises :class:`RunnerError`) instead of returning ``cmd``
    unwrapped. Explicit opt-out: set
    ``MUTATION_HARNESS_ALLOW_UNISOLATED_FS=1`` (loud, non-default).
    """
    sandbox_s = str(Path(sandbox_root).resolve())
    bwrap = shutil.which("bwrap")
    if bwrap:
        return [
            bwrap,
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--bind",
            sandbox_s,
            sandbox_s,
            "--chdir",
            sandbox_s,
            *cmd,
        ]
    sandbox_exec = shutil.which("sandbox-exec")
    if sandbox_exec:
        profile = _sandbox_exec_profile(sandbox_s)
        return [sandbox_exec, "-p", profile, *cmd]

    allow = (os.environ.get(_ALLOW_UNISOLATED_FS_ENV) or "").strip().lower()
    if allow in ("1", "true", "yes"):
        return list(cmd)

    raise RunnerError(
        "filesystem isolation unavailable: neither bwrap nor sandbox-exec "
        "found on PATH; refusing to run mutant unwrapped "
        f"(set {_ALLOW_UNISOLATED_FS_ENV}=1 to override)"
    )


def run_pytest_in_sandbox(
    sandbox_root: Path,
    test_args: Sequence[str],
    *,
    timeout: float,
    python: str | None = None,
    extra_env: dict[str, str] | None = None,
    cwd: Path | None = None,
    source_root: Path | None = None,
    isolate_filesystem: bool = True,
) -> tuple[int, str, str, bool]:
    """Invoke pytest as a subprocess inside the sandbox.

    Returns ``(returncode, stdout, stderr, timed_out)``.

    Isolation layers (all on by default):
    1. Env: PYTHONPATH/TMPDIR/HOME + source-root drop for editable ``.pth``
    2. Launcher: reorders ``sys.path`` before pytest imports the package
    3. Filesystem: bwrap or sandbox-exec deny host writes (fail closed if neither)
    """
    exe = python or os.environ.get("MUTATION_HARNESS_PYTHON") or sys.executable
    env = build_sandbox_env(
        sandbox_root,
        source_root=source_root,
        extra_env=extra_env,
    )
    # Prefer the path-fix launcher over bare ``-m pytest`` so .pth redirects
    # cannot silently test unmutated host code.
    cmd = [exe, "-c", _pytest_launcher_code(test_args)]
    if isolate_filesystem:
        cmd = wrap_with_filesystem_isolation(cmd, sandbox_root)
    work = str(cwd or sandbox_root)
    try:
        proc = subprocess.run(
            cmd,
            cwd=work,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout or "", proc.stderr or "", False
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        err = (exc.stderr or "") if isinstance(exc.stderr, str) else ""
        return -1, out, err, True


def run_mutant(
    mutant: Mutant,
    *,
    source_root: Path,
    default_timeout: float = 60.0,
    full_suite: bool = False,
    python: str | None = None,
    progress: Callable[[dict], None] | None = None,
    extra_env: dict[str, str] | None = None,
    coverage_map: CoverageMap | None = None,
    _control_cache: _ControlCache | None = None,
    _control_lock: threading.Lock | None = None,
) -> MutantResult:
    """Full single-mutant pipeline with isolated sandbox."""
    timeout = mutant.timeout if mutant.timeout is not None else default_timeout
    t0 = time.monotonic()
    error_selection_mode: SelectionMode = (
        "full_suite" if full_suite else ("manifest" if mutant.tests else "coverage")
    )
    if progress:
        progress({"event": "mutant_start", "mutant_id": mutant.id, "timeout": timeout})

    sandbox: Path | None = None
    try:
        sandbox = create_sandbox(source_root, mutant_id=mutant.id)
        # Join + realpath containment (absolute/.. /symlink escape all refused).
        target = resolve_sandbox_target(sandbox, mutant.target, mutant_id=mutant.id)
        # Validate the mutation grammar and match set before paying for the
        # unmutated control run. This does not change the sandbox on disk.
        locate_mutation_spans(target.read_text(encoding="utf-8"), mutant.mutation)
        plan = _select_test_plan(
            mutant,
            full_suite=full_suite,
            coverage_map=coverage_map,
            source_root=source_root,
        )
        selected = list(plan.tests)
        control_cache = _control_cache if _control_cache is not None else {}
        control_lock = _control_lock if _control_lock is not None else threading.Lock()
        cache_key = tuple(selected)
        # Serialize cache misses so a parallel sweep pays for each exact
        # selection only once. Both green and unusable controls are cached.
        with control_lock:
            control = control_cache.get(cache_key)
            if control is None:
                control = _ControlOutcome(
                    *run_pytest_in_sandbox(
                        sandbox,
                        selected,
                        timeout=timeout,
                        python=python,
                        extra_env=extra_env,
                        source_root=source_root,
                    )
                )
                control_cache[cache_key] = control
        if control.returncode != 0 or control.timed_out:
            status, error_message = _classify_control_failure(selected, control)
            result = MutantResult(
                mutant_id=mutant.id,
                status=status,  # type: ignore[arg-type]
                duration=time.monotonic() - t0,
                error_message=error_message,
                selection_mode=plan.mode,
                fallback_reason=plan.fallback_reason,
            )
        else:
            apply_mutation(target, mutant.mutation, mutant_id=mutant.id)
            rc, stdout, stderr, timed_out = run_pytest_in_sandbox(
                sandbox,
                selected,
                timeout=timeout,
                python=python,
                extra_env=extra_env,
                source_root=source_root,
            )
            status, killers, err_msg = classify_pytest_outcome(
                rc,
                timed_out=timed_out,
                stdout=stdout,
                stderr=stderr,
                selected_tests=selected,
                unmutated_control_passed=True,
            )
            duration = time.monotonic() - t0
            result = MutantResult(
                mutant_id=mutant.id,
                status=status,  # type: ignore[arg-type]
                killing_tests=killers,
                duration=duration,
                error_message=err_msg,
                selection_mode=plan.mode,
                fallback_reason=plan.fallback_reason,
            )
    except (CoverageMapError, MutationError, SandboxError, RunnerError, OSError) as exc:
        duration = time.monotonic() - t0
        result = MutantResult(
            mutant_id=mutant.id,
            status="error",
            killing_tests=[],
            duration=duration,
            error_message=str(exc),
            selection_mode=error_selection_mode,
        )
    finally:
        if sandbox is not None:
            destroy_sandbox(sandbox)

    if progress:
        progress(
            {
                "event": "mutant_done",
                "mutant_id": result.mutant_id,
                "status": result.status,
                "duration": result.duration,
            }
        )
    return result


def make_default_runner(
    source_root: Path,
    *,
    default_timeout: float = 60.0,
    full_suite: bool = False,
    python: str | None = None,
    progress: Callable[[dict], None] | None = None,
    extra_env: dict[str, str] | None = None,
    coverage_map: CoverageMap | None = None,
) -> Callable[[Mutant], MutantResult]:
    """Build a runner callable suitable for :func:`scheduler.run_sweep`."""

    resolved_map = coverage_map
    coverage_error: str | None = None
    coverage_lock = threading.Lock()
    control_cache: _ControlCache = {}
    control_lock = threading.Lock()

    def _run(mutant: Mutant) -> MutantResult:
        nonlocal resolved_map, coverage_error
        if not full_suite and not mutant.tests and resolved_map is None:
            with coverage_lock:
                if resolved_map is None and coverage_error is None:
                    try:
                        resolved_map = load_or_build_coverage_map(
                            source_root, python=python
                        )
                    except CoverageMapError as exc:
                        coverage_error = str(exc)
            if coverage_error is not None:
                return MutantResult(
                    mutant_id=mutant.id,
                    status="error",
                    error_message=coverage_error,
                    selection_mode="coverage",
                )
        return run_mutant(
            mutant,
            source_root=source_root,
            default_timeout=default_timeout,
            full_suite=full_suite,
            python=python,
            progress=progress,
            extra_env=extra_env,
            coverage_map=resolved_map,
            _control_cache=control_cache,
            _control_lock=control_lock,
        )

    return _run
