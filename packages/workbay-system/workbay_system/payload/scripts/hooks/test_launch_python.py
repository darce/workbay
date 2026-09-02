"""Focused tests for scripts/hooks/_launch_python.sh."""

from __future__ import annotations

import errno
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import time

import pytest

LAUNCHER = Path(__file__).resolve().parent / "_launch_python.sh"
RUN_GUARD = Path(__file__).resolve().parent / "_run_guard.py"


def _chmod_exec(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def test_launch_python_skips_exit_126_and_execs_run_guard(tmp_path: Path) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    workspace = tmp_path / "workspace"
    hooks = workspace / "scripts" / "hooks"
    hooks.mkdir(parents=True)
    shutil.copy2(LAUNCHER, hooks / "_launch_python.sh")
    shutil.copy2(RUN_GUARD, hooks / "_run_guard.py")
    _chmod_exec(hooks / "_launch_python.sh")

    marker = tmp_path / "handler.marker"
    handler = hooks / "probe.py"
    handler.write_text(
        "import sys\nfrom pathlib import Path\n"
        "Path(sys.argv[1]).write_text(sys.stdin.read())\n",
        encoding="utf-8",
    )

    hostile = tmp_path / "hostile-bin"
    hostile.mkdir()
    shim = hostile / "python3"
    shim.write_text("#!/bin/sh\nexit 126\n", encoding="utf-8")
    _chmod_exec(shim)

    env = os.environ.copy()
    env["PATH"] = f"{hostile}{os.pathsep}{env.get('PATH', '')}"
    env.pop("PYENV_VERSION", None)
    env.pop("VIRTUAL_ENV", None)

    proc = subprocess.run(
        [
            str(hooks / "_launch_python.sh"),
            str(hooks / "_run_guard.py"),
            str(handler),
            str(marker),
        ],
        cwd=workspace,
        env=env,
        input="payload-in\n",
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert marker.read_text(encoding="utf-8") == "payload-in\n"


def test_launch_python_ignores_hostile_support_utilities(tmp_path: Path) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    workspace = tmp_path / "workspace"
    hooks = workspace / "scripts" / "hooks"
    hooks.mkdir(parents=True)
    shutil.copy2(LAUNCHER, hooks / "_launch_python.sh")
    shutil.copy2(RUN_GUARD, hooks / "_run_guard.py")
    _chmod_exec(hooks / "_launch_python.sh")

    payload_marker = tmp_path / "handler.marker"
    handler = hooks / "probe.py"
    handler.write_text(
        "import sys\nfrom pathlib import Path\n"
        "Path(sys.argv[1]).write_text(sys.stdin.read())\n",
        encoding="utf-8",
    )

    hostile_marker = tmp_path / "hostile.marker"
    hostile = tmp_path / "hostile-bin"
    hostile.mkdir()
    for name in ("mktemp", "sleep", "cat", "rm", "rmdir", "kill"):
        shim = hostile / name
        shim.write_text(
            f'#!/bin/sh\nprintf "%s\\n" "{name}" >> "{hostile_marker}"\nexit 1\n',
            encoding="utf-8",
        )
        _chmod_exec(shim)

    env = os.environ.copy()
    env["PATH"] = f"{hostile}{os.pathsep}{env.get('PATH', '')}"
    env.pop("PYENV_VERSION", None)
    env.pop("VIRTUAL_ENV", None)
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)

    proc = subprocess.run(
        [
            str(hooks / "_launch_python.sh"),
            str(hooks / "_run_guard.py"),
            str(handler),
            str(payload_marker),
        ],
        cwd=workspace,
        env=env,
        input="payload-in\n",
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert payload_marker.read_text(encoding="utf-8") == "payload-in\n"
    assert not hostile_marker.exists()


def test_launch_python_rejects_hanging_candidate_without_full_hang(
    tmp_path: Path,
) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    launcher = tmp_path / "_launch_python.sh"
    shutil.copy2(LAUNCHER, launcher)
    _chmod_exec(launcher)

    hang_bin = tmp_path / "hang-bin"
    hang_bin.mkdir()
    hang = hang_bin / "python"
    pidfile = tmp_path / "hang.pid"
    live_marker = tmp_path / "hang.live"
    hang.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$$" > "{pidfile}"\n'
        f': > "{live_marker}"\n'
        "while :; do command -p sleep 30; done\n",
        encoding="utf-8",
    )
    _chmod_exec(hang)

    venv_py = workspace / ".venv" / "bin" / "python"
    venv_py.parent.mkdir(parents=True)
    shutil.copy2(hang, venv_py)
    _chmod_exec(venv_py)

    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)

    started = time.monotonic()
    proc = subprocess.run(
        [str(launcher), "-c", "import sys; sys.stdout.write('ok')"],
        cwd=workspace,
        env=env,
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )
    elapsed = time.monotonic() - started
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "ok"
    assert elapsed < 5, f"watchdog waited too long: {elapsed:.2f}s"

    if pidfile.is_file():
        hang_pid = int(pidfile.read_text(encoding="utf-8").strip())
        still_alive = True
        try:
            os.kill(hang_pid, 0)
        except OSError:
            still_alive = False
        assert not still_alive, f"hanging candidate pid {hang_pid} still alive"
    if live_marker.is_file():
        live_marker.unlink()


def test_launch_python_fail_open_and_closed_without_viable_python(
    tmp_path: Path,
) -> None:
    shim = tmp_path / "dead126"
    shim.write_text("#!/bin/sh\nexit 126\n", encoding="utf-8")
    _chmod_exec(shim)
    shim_s = str(shim)

    src = LAUNCHER.read_text(encoding="utf-8")
    rewritten: list[str] = []
    for line in src.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith("_path_py="):
            nl = "\n" if line.endswith("\n") else ""
            rewritten.append(f'_path_py="{shim_s}"{nl}')
        elif stripped.startswith("_consider "):
            indent = line[: len(line) - len(stripped)]
            nl = "\n" if line.endswith("\n") else ""
            rewritten.append(f'{indent}_consider "{shim_s}"{nl}')
        else:
            rewritten.append(line)

    launcher = tmp_path / "_launch_python.sh"
    launcher.write_text("".join(rewritten), encoding="utf-8")
    _chmod_exec(launcher)

    workspace = tmp_path / "ws"
    workspace.mkdir()

    env = os.environ.copy()
    env["CLAUDE_PROJECT_DIR"] = str(workspace)
    env["PATH"] = str(tmp_path / "empty-bin")
    (tmp_path / "empty-bin").mkdir()

    open_proc = subprocess.run(
        [str(launcher), "missing.py"],
        cwd=workspace,
        env=env,
        capture_output=True,
        timeout=8,
        check=False,
    )
    closed_proc = subprocess.run(
        [str(launcher), "--fail-mode=closed", "missing.py"],
        cwd=workspace,
        env=env,
        capture_output=True,
        timeout=8,
        check=False,
    )
    assert open_proc.returncode == 0, open_proc.stderr
    assert closed_proc.returncode == 2, closed_proc.stderr


def _reap_session(proc: subprocess.Popen[str]) -> None:
    # Always SIGKILL the fresh test session even if the launcher parent
    # already exited, so leftover descendants cannot outlive the test.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    if proc.poll() is not None:
        return
    try:
        proc.wait(timeout=2)
    except Exception:
        pass


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _force_reap_pid(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        return
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and _pid_alive(pid):
        time.sleep(0.05)


def test_launch_python_bounds_term_immune_candidate_and_reaps_descendant(
    tmp_path: Path,
) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    pidfile = tmp_path / "immune.pids"
    shim = workspace / ".venv" / "bin" / "python"
    shim.parent.mkdir(parents=True)
    shim.write_text(
        "#!/bin/sh\n"
        "trap '' TERM\n"
        f'printf "%s\\n" "$$" > "{pidfile}"\n'
        "command -p sleep 30 &\n"
        "child=$!\n"
        f'printf "%s\\n" "$child" >> "{pidfile}"\n'
        "while :; do command -p sleep 30; done\n",
        encoding="utf-8",
    )
    _chmod_exec(shim)

    launcher = tmp_path / "_launch_python.sh"
    shutil.copy2(LAUNCHER, launcher)
    _chmod_exec(launcher)

    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)
    env.pop("PYENV_VERSION", None)
    env.pop("VIRTUAL_ENV", None)

    proc = subprocess.Popen(
        [str(launcher), "-c", "import sys; sys.stdout.write('ok')"],
        cwd=workspace,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    stdout = ""
    stderr = ""
    try:
        try:
            stdout, stderr = proc.communicate(timeout=3.5)
        except subprocess.TimeoutExpired:
            pytest.fail(
                "launcher hung waiting on TERM-immune candidate "
                "(TERM-only probe then blocking wait)"
            )
        assert proc.returncode == 0, stderr
        assert stdout == "ok"
        parent_pid = None
        child_pid = None
        if pidfile.is_file():
            lines = [
                line.strip()
                for line in pidfile.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            if lines:
                parent_pid = int(lines[0])
            if len(lines) > 1:
                child_pid = int(lines[1])
        if parent_pid is not None:
            assert not _pid_alive(parent_pid), f"immune parent {parent_pid} still alive"
        if child_pid is not None:
            assert not _pid_alive(child_pid), f"immune child {child_pid} still alive"
        work_dir = Path(f"/tmp/wb-lp-{proc.pid}-0")
        assert not work_dir.exists(), f"launcher work dir remained: {work_dir}"
    finally:
        parent_pid = None
        child_pid = None
        if pidfile.is_file():
            lines = [
                line.strip()
                for line in pidfile.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            if lines:
                parent_pid = int(lines[0])
            if len(lines) > 1:
                child_pid = int(lines[1])
        _reap_session(proc)
        if parent_pid is not None:
            _force_reap_pid(parent_pid)
        if child_pid is not None:
            _force_reap_pid(child_pid)
        work_dir = Path(f"/tmp/wb-lp-{proc.pid}-0")
        if work_dir.is_dir():
            shutil.rmtree(work_dir, ignore_errors=True)


def test_launch_python_ignores_pythonpath_sitecustomize_before_guard(
    tmp_path: Path,
) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    workspace = tmp_path / "workspace"
    hooks = workspace / "scripts" / "hooks"
    hooks.mkdir(parents=True)
    shutil.copy2(LAUNCHER, hooks / "_launch_python.sh")
    shutil.copy2(RUN_GUARD, hooks / "_run_guard.py")
    _chmod_exec(hooks / "_launch_python.sh")

    marker = tmp_path / "handler.marker"
    handler = hooks / "probe.sh"
    handler.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$1" > "{marker}"\ncat >> "{marker}"\n',
        encoding="utf-8",
    )
    _chmod_exec(handler)

    site_dir = tmp_path / "hostile-site"
    site_dir.mkdir()
    site_marker = tmp_path / "sitecustomize.marker"
    (site_dir / "sitecustomize.py").write_text(
        f'from pathlib import Path\nPath("{site_marker}").write_text("imported\\n")\n',
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(site_dir)
    env.pop("PYENV_VERSION", None)
    env.pop("VIRTUAL_ENV", None)

    proc = subprocess.run(
        [
            str(hooks / "_launch_python.sh"),
            str(hooks / "_run_guard.py"),
            str(handler),
            "argv-proof",
        ],
        cwd=workspace,
        env=env,
        input="payload-in\n",
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert marker.read_text(encoding="utf-8") == "argv-proof\npayload-in\n"
    assert not site_marker.exists(), (
        "sitecustomize ran during probe or before _run_guard.py"
    )


def test_launch_python_fast_success_reaps_watchdog_timer(tmp_path: Path) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    ps_bin = Path("/bin/ps")
    if not (ps_bin.is_file() and os.access(ps_bin, os.X_OK)):
        ps_bin = Path("/usr/bin/ps")
    if not (ps_bin.is_file() and os.access(ps_bin, os.X_OK)):
        pytest.skip("trusted ps unavailable")

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    launcher = tmp_path / "_launch_python.sh"
    shutil.copy2(LAUNCHER, launcher)
    _chmod_exec(launcher)

    env = os.environ.copy()
    env.pop("PYENV_VERSION", None)
    env.pop("VIRTUAL_ENV", None)
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)

    proc = subprocess.Popen(
        [str(launcher), "-c", "import sys; sys.stdout.write('ok')"],
        cwd=workspace,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    pgid = proc.pid
    try:
        rc = proc.wait(timeout=2)
        assert rc == 0, f"launcher returncode={rc}"
        time.sleep(0.05)

        listing = subprocess.run(
            [str(ps_bin), "-e", "-o", "pid=", "-o", "ppid=", "-o", "pgid="],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        assert listing.returncode == 0, f"ps returncode={listing.returncode}"
        observed: list[tuple[int, int, int]] = []
        for line in listing.stdout.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            try:
                pid, ppid, row_pgid = (int(parts[0]), int(parts[1]), int(parts[2]))
            except ValueError:
                continue
            if row_pgid == pgid:
                observed.append((pid, ppid, row_pgid))
        assert observed == [], f"orphaned process group still live: {observed}"
    finally:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                raise
        try:
            os.waitpid(-pgid, os.WNOHANG)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                pass
        try:
            proc.wait(timeout=1)
        except OSError:
            pass


_ARM_SPIN = (
    "        _arm_gap_i=0\n"
    "        while [ \"$_arm_gap_i\" -lt 200000 ]; do\n"
    "            _arm_gap_i=$((_arm_gap_i + 1))\n"
    "        done\n"
)
_TIMER_ARM = '        command -p sleep "$PROBE_SECS" &\n        _timer=$!\n'
_TIMER_ARM_DELAYED = (
    '        command -p sleep "$PROBE_SECS" &\n' + _ARM_SPIN + "        _timer=$!\n"
)


def test_launch_python_reaps_timer_when_watchdog_arm_is_delayed(tmp_path: Path) -> None:
    platform_py = Path("/usr/bin/python3")
    if not (platform_py.is_file() and os.access(platform_py, os.X_OK)):
        pytest.skip("verified explicit platform interpreter unavailable")

    ps_bin = Path("/bin/ps")
    if not (ps_bin.is_file() and os.access(ps_bin, os.X_OK)):
        ps_bin = Path("/usr/bin/ps")
    if not (ps_bin.is_file() and os.access(ps_bin, os.X_OK)):
        pytest.skip("trusted ps unavailable")

    src = LAUNCHER.read_text(encoding="utf-8")
    assert src.count(_TIMER_ARM) == 1, "expected exactly one watchdog timer arm"
    rewritten = src.replace(_TIMER_ARM, _TIMER_ARM_DELAYED, 1)
    assert rewritten.count(_ARM_SPIN) == 1
    assert rewritten.count(_TIMER_ARM_DELAYED) == 1

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    launcher = tmp_path / "_launch_python.sh"
    launcher.write_text(rewritten, encoding="utf-8")
    _chmod_exec(launcher)

    env = os.environ.copy()
    env.pop("PYENV_VERSION", None)
    env.pop("VIRTUAL_ENV", None)
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)

    proc = subprocess.Popen(
        [str(launcher), "-c", "import sys; sys.stdout.write('ok')"],
        cwd=workspace,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    pgid = proc.pid
    try:
        # Synthetic scheduling-fault bound, not a production latency assertion.
        rc = proc.wait(timeout=10)
        assert rc == 0, f"launcher returncode={rc}"

        listing = subprocess.run(
            [str(ps_bin), "-e", "-o", "pid=", "-o", "ppid=", "-o", "pgid="],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        assert listing.returncode == 0, f"ps returncode={listing.returncode}"
        observed: list[tuple[int, int, int]] = []
        for line in listing.stdout.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            try:
                pid, ppid, row_pgid = (int(parts[0]), int(parts[1]), int(parts[2]))
            except ValueError:
                continue
            if row_pgid == pgid:
                observed.append((pid, ppid, row_pgid))
        assert observed == [], f"orphaned process group still live: {observed}"
    finally:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                raise
        try:
            os.waitpid(-pgid, os.WNOHANG)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                pass
        try:
            proc.wait(timeout=1)
        except OSError:
            pass
