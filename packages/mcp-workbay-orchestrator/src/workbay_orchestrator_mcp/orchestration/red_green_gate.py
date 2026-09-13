"""Deterministic red/green proof for junior-produced lane patches."""

from __future__ import annotations

import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from fnmatch import fnmatch
from functools import partial
from pathlib import Path
from stat import S_ISREG
from typing import Any

__all__ = ["RedGreenResult", "verify_red_green"]

_OUTPUT_TAIL_CHARS = 4_000
_MAX_COMMAND_OUTPUT_BYTES = 4 * 1024 * 1024
_PROCESS_POLL_SECONDS = 0.01
_PROCESS_GROUP_TERMINATE_GRACE_SECONDS = 0.5
_PROCESS_GROUP_KILL_GRACE_SECONDS = 1.0

_HARNESS_FAILURE_TYPES = {
    2: "interrupted_or_usage_error",
    3: "pytest_internal_error",
    4: "pytest_usage_error",
    5: "pytest_no_tests_collected",
    70: "harness_software_error",
    75: "harness_temporarily_unavailable",
    126: "command_not_executable",
    127: "command_not_found",
}


def _lane_check_timeout_seconds() -> int:
    # Lazy import keeps this small gate usable without loading the daemon until
    # execution reaches the bounded test phase.
    from .worker_daemon import _lane_check_timeout_seconds as configured_timeout  # noqa: PLC0415

    return configured_timeout()


class RedGreenResult(dict[str, Any]):
    """Observable gate result that is truthy only for a valid red/green proof."""

    def __bool__(self) -> bool:
        return self.get("status") == "pass"


def _result(reason: str, *, passed: bool = False, **evidence: Any) -> RedGreenResult:
    status = "pass" if passed else "fail"
    return RedGreenResult(
        status=status,
        red_green=status,
        passed=passed,
        reason=reason,
        **evidence,
    )


def _run_git(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _orchestrator_repository(worktree: Path) -> Path:
    common = _run_git(worktree, "rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip()
    common_dir = Path(common)
    if not common_dir.is_absolute():
        common_dir = (worktree / common_dir).resolve()
    if common_dir.name != ".git":
        raise RuntimeError(f"git common directory is not a worktree repository: {common_dir}")
    return common_dir.parent


def _changed_test_files(repository: Path, base_sha: str, tip_sha: str) -> list[str]:
    changed = _run_git(
        repository,
        "diff",
        "--diff-filter=ACMR",
        "--name-only",
        base_sha,
        tip_sha,
        "--",
    ).stdout.splitlines()
    return sorted(path for path in changed if _is_test_path(path))


def _apply_tip_test_patch(
    repository: Path,
    base_worktree: Path,
    base_sha: str,
    tip_sha: str,
    changed_test_files: list[str],
) -> None:
    """Overlay only tip-side test changes onto base production code."""
    patch = _run_git(repository, "diff", "--binary", base_sha, tip_sha, "--", *changed_test_files).stdout
    applied = subprocess.run(
        ["git", "-C", str(base_worktree), "apply", "--whitespace=nowarn", "-"],
        input=patch,
        capture_output=True,
        text=True,
        check=False,
    )
    if applied.returncode != 0:
        raise RuntimeError((applied.stderr or applied.stdout or "test patch apply failed").strip())


def _is_test_path(path: str) -> bool:
    parts = Path(path).parts
    name = Path(path).name.lower()
    return "tests" in parts or name.startswith("test_") or ".test." in name or name.endswith("_test.py")


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _site_roots_without_candidate_hooks(python: Path, candidate_worktree: Path) -> list[Path]:
    """Expose installed dependencies without executing candidate venv ``.pth`` hooks."""
    environment_root = python.parent.parent
    site_roots = sorted(
        {
            path.absolute()
            for pattern in ("lib/python*/site-packages", "lib64/python*/site-packages", "Lib/site-packages")
            for path in environment_root.glob(pattern)
            if path.is_dir()
        }
    )
    candidate = candidate_worktree.resolve()
    dependency_roots: list[Path] = list(site_roots)
    for site_root in site_roots:
        for pth_file in sorted(site_root.glob("*.pth")):
            try:
                lines = pth_file.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError):
                continue
            for line in lines:
                entry = line.strip()
                # Path-only dependency entries are safe to reproduce. Executable
                # import hooks are candidate-controlled code and stay disabled.
                if not entry or entry.startswith("#") or entry.startswith(("import ", "import\t")):
                    continue
                path = Path(entry)
                if not path.is_absolute():
                    path = site_root / path
                try:
                    resolved = path.resolve()
                except OSError:
                    continue
                if resolved.exists() and not _is_within(resolved, candidate):
                    dependency_roots.append(resolved)
    return list(dict.fromkeys(dependency_roots))


def _resolve_command_path(token: str, *, cwd: Path) -> Path | None:
    path = Path(token)
    if path.is_absolute() or path.parent != Path("."):
        return path if path.is_absolute() else cwd / path
    found = shutil.which(token)
    return Path(found) if found else None


def _isolated_base_command(command: str, *, cwd: Path) -> tuple[str, Path] | None:
    """Rewrite the pytest runner so Python starts with ``-S`` for the base arm.

    The gate deliberately supports a bounded pytest command grammar. Shell
    composition is rejected rather than evaluated under a partially isolated
    interpreter.
    """
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if not tokens or any(token in {";", "&&", "||", "|", "&"} for token in tokens):
        return None
    # The sanitized environment owns Python's import roots. A command-local
    # override would otherwise restore candidate paths after we disabled site.
    blocked_assignments = ("PYTHONPATH=", "PYTHONHOME=", "PYTHONUSERBASE=")
    tokens = [token for token in tokens if not token.startswith(blocked_assignments)]

    def _environment_assignments_only(prefix: list[str]) -> bool:
        for token in prefix:
            name, separator, _value = token.partition("=")
            if not separator or not name or not name.replace("_", "a").isalnum() or name[0].isdigit():
                return False
        return True

    for index, token in enumerate(tokens):
        if Path(token).name == "pytest":
            if not _environment_assignments_only(tokens[:index]):
                return None
            pytest_script = _resolve_command_path(token, cwd=cwd)
            if pytest_script is None:
                return None
            runner_python = pytest_script.parent / "python"
            if not runner_python.exists():
                runner_python = Path(shutil.which("python3") or "")
            if not runner_python.exists():
                return None
            tokens[index : index + 1] = [str(runner_python), "-S", str(pytest_script)]
            return shlex.join(tokens), runner_python
        if (Path(token).name.startswith("python") or Path(token).name.endswith("-python")) and tokens[
            index + 1 : index + 3
        ] == ["-m", "pytest"]:
            if not _environment_assignments_only(tokens[:index]):
                return None
            resolved_python = _resolve_command_path(token, cwd=cwd)
            if resolved_python is None or not resolved_python.exists():
                return None
            tokens.insert(index + 1, "-S")
            return shlex.join(tokens), resolved_python
    return None


def _revision_test_environment(
    worktree: Path,
    *,
    isolated_python: Path | None = None,
    candidate_worktree: Path | None = None,
) -> dict[str, str]:
    """Prefer production imports from the revision under test.

    The command may reuse the lane virtualenv for third-party dependencies,
    but a src-layout editable in that environment must not resolve production
    from the tip while the detached base is under test. Revision-local source
    roots precede site-packages for both runs [AGT-10][PROV-01].
    """
    roots = [worktree / "src"]
    packages = worktree / "packages"
    if packages.is_dir():
        for member in sorted(packages.iterdir()):
            if member.is_dir():
                roots.extend((member / "src", member))
    roots.append(worktree)
    env = dict(os.environ)
    import_roots = [path.resolve() for path in roots if path.is_dir()]
    if isolated_python is not None and candidate_worktree is not None:
        import_roots.extend(_site_roots_without_candidate_hooks(isolated_python, candidate_worktree))
    env["PYTHONPATH"] = os.pathsep.join(str(path) for path in dict.fromkeys(import_roots))
    env["PYTHONNOUSERSITE"] = "1"
    if isolated_python is not None:
        env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return env


def _run_test_command(
    command: str,
    *,
    cwd: Path,
    timeout: float,
    candidate_worktree: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    isolated_python: Path | None = None
    if candidate_worktree is not None:
        isolated = _isolated_base_command(command, cwd=cwd)
        if isolated is None:
            return subprocess.CompletedProcess(
                command,
                127,
                "",
                "red-green base pytest command is unavailable or cannot be isolated",
            )
        command, isolated_python = isolated
    popen_kwargs: dict[str, Any]
    if os.name == "nt":
        popen_kwargs = {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)}
    else:
        popen_kwargs = {"start_new_session": True}
    linux_cgroup = _create_linux_cgroup()
    if linux_cgroup is not None:
        popen_kwargs["preexec_fn"] = partial(_join_linux_cgroup, str(linux_cgroup))
    descendants: dict[int, str] = {}
    timed_out = False
    output_limit_exceeded = False
    output = _BoundedOutput(_MAX_COMMAND_OUTPUT_BYTES)
    try:
        proc = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=_revision_test_environment(
                cwd,
                isolated_python=isolated_python,
                candidate_worktree=candidate_worktree,
            ),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **popen_kwargs,
        )
    except subprocess.SubprocessError:
        # A nominally delegated cgroup can still reject migration. Preserve a
        # process-group + descendant-identity fallback on such hosts.
        _remove_linux_cgroup(linux_cgroup)
        linux_cgroup = None
        popen_kwargs.pop("preexec_fn", None)
        proc = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=_revision_test_environment(
                cwd,
                isolated_python=isolated_python,
                candidate_worktree=candidate_worktree,
            ),
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **popen_kwargs,
        )
    windows_job = _create_windows_job(proc)
    assert proc.stdout is not None and proc.stderr is not None
    readers = [
        threading.Thread(target=_drain_output, args=(proc.stdout, output, "stdout"), daemon=True),
        threading.Thread(target=_drain_output, args=(proc.stderr, output, "stderr"), daemon=True),
    ]
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout
    try:
        while True:
            _refresh_descendants(proc.pid, descendants)
            if output.exceeded.is_set():
                output_limit_exceeded = True
                break
            if proc.poll() is not None:
                break
            if time.monotonic() >= deadline:
                timed_out = True
                break
            time.sleep(_PROCESS_POLL_SECONDS)
    finally:
        # Test processes are disposable containment units. Clean the whole
        # observed tree for success, failure, overflow, and timeout alike.
        _cleanup_test_process_tree(proc, descendants, linux_cgroup, windows_job)
    try:
        proc.wait(timeout=_PROCESS_GROUP_KILL_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    _remove_linux_cgroup(linux_cgroup)
    _close_windows_job(windows_job)
    for reader in readers:
        reader.join(timeout=_PROCESS_GROUP_KILL_GRACE_SECONDS)
    output_limit_exceeded = output_limit_exceeded or output.exceeded.is_set()
    stdout = output.text("stdout")
    stderr = output.text("stderr")

    if timed_out:
        raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
    if output_limit_exceeded:
        stderr = f"{stderr}\nred-green harness: output_limit_exceeded ({_MAX_COMMAND_OUTPUT_BYTES} bytes)\n"
        return subprocess.CompletedProcess(command, 70, stdout, stderr)
    return subprocess.CompletedProcess(command, proc.returncode, stdout, stderr)


class _BoundedOutput:
    """Thread-safe byte ring buffers with one aggregate output ceiling."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.total = 0
        self.buffers = {"stdout": bytearray(), "stderr": bytearray()}
        self.exceeded = threading.Event()
        self._lock = threading.Lock()

    def append(self, stream_name: str, chunk: bytes) -> None:
        with self._lock:
            self.total += len(chunk)
            buffer = self.buffers[stream_name]
            buffer.extend(chunk)
            if len(buffer) > self.limit:
                del buffer[: len(buffer) - self.limit]
            if self.total > self.limit:
                self.exceeded.set()

    def text(self, stream_name: str) -> str:
        with self._lock:
            return bytes(self.buffers[stream_name]).decode("utf-8", errors="replace")


def _drain_output(stream: Any, output: _BoundedOutput, stream_name: str) -> None:
    try:
        while chunk := stream.read1(64 * 1024):
            output.append(stream_name, chunk)
    except (OSError, ValueError):
        pass


def _create_linux_cgroup() -> Path | None:
    """Create a cgroup-v2 containment unit when this process has delegation."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        membership = Path("/proc/self/cgroup").read_text().splitlines()
        relative = next(line.partition("::")[2] for line in membership if "::" in line)
        parent = Path("/sys/fs/cgroup", relative.lstrip("/"))
        if not parent.joinpath("cgroup.controllers").exists():
            return None
        cgroup = parent / f"workbay-rg-{os.getpid()}-{time.monotonic_ns()}"
        cgroup.mkdir()
        return cgroup
    except (OSError, StopIteration):
        return None


def _join_linux_cgroup(cgroup: str) -> None:
    """Run after fork and before exec; use only async-signal-safe os calls."""
    descriptor = os.open(os.path.join(cgroup, "cgroup.procs"), os.O_WRONLY)
    try:
        os.write(descriptor, b"0")
    finally:
        os.close(descriptor)


def _signal_linux_cgroup(cgroup: Path | None, signum: int) -> None:
    if cgroup is None:
        return
    try:
        pids = cgroup.joinpath("cgroup.procs").read_text().splitlines()
    except OSError:
        return
    for raw_pid in pids:
        try:
            os.kill(int(raw_pid), signum)
        except (OSError, ValueError):
            pass


def _kill_linux_cgroup(cgroup: Path | None) -> None:
    if cgroup is None:
        return
    try:
        cgroup.joinpath("cgroup.kill").write_text("1")
    except OSError:
        _signal_linux_cgroup(cgroup, signal.SIGKILL)


def _linux_cgroup_has_processes(cgroup: Path | None) -> bool:
    if cgroup is None:
        return False
    try:
        return bool(cgroup.joinpath("cgroup.procs").read_text().strip())
    except OSError:
        return False


def _remove_linux_cgroup(cgroup: Path | None) -> None:
    if cgroup is None:
        return
    try:
        cgroup.rmdir()
    except OSError:
        pass


def _create_windows_job(proc: subprocess.Popen[Any]) -> int | None:
    """Assign the command to a Windows Job Object when available."""
    if os.name != "nt":
        return None
    try:
        import ctypes  # noqa: PLC0415

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = ctypes.c_void_p
        kernel32.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None
        if not kernel32.AssignProcessToJobObject(job, int(proc._handle)):  # type: ignore[attr-defined]
            kernel32.CloseHandle(job)
            return None
        return int(job)
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def _terminate_windows_job(job: int | None) -> None:
    if job is None:
        return
    try:
        import ctypes  # noqa: PLC0415

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = [ctypes.c_void_p, ctypes.c_uint]
        kernel32.TerminateJobObject(job, 1)
    except (AttributeError, OSError):
        pass


def _close_windows_job(job: int | None) -> None:
    if job is None:
        return
    try:
        import ctypes  # noqa: PLC0415

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle(job)
    except (AttributeError, OSError):
        pass


def _process_snapshot() -> dict[int, tuple[int, str, str]]:
    """Return pid -> (parent pid, start identity, state) for fallback tracking."""
    if os.name != "posix":
        return {}
    if not sys.platform.startswith("linux"):
        try:
            listing = subprocess.run(
                ["ps", "-axo", "pid=,ppid=,lstart=,state="],
                check=True,
                capture_output=True,
                text=True,
                timeout=1,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            return {}
        snapshot: dict[int, tuple[int, str, str]] = {}
        for line in listing.splitlines():
            fields = line.split()
            if len(fields) < 8:
                continue
            try:
                snapshot[int(fields[0])] = (int(fields[1]), " ".join(fields[2:7]), fields[7][0])
            except (IndexError, ValueError):
                continue
        return snapshot
    snapshot: dict[int, tuple[int, str, str]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = entry.joinpath("stat").read_text()
            fields = stat[stat.rfind(")") + 2 :].split()
            snapshot[int(entry.name)] = (int(fields[1]), fields[19], fields[0])
        except (FileNotFoundError, IndexError, OSError, ValueError):
            continue
    return snapshot


def _refresh_descendants(root_pid: int, descendants: dict[int, str]) -> None:
    """Remember descendants before reparenting lets them escape a process group."""
    snapshot = _process_snapshot()
    if not snapshot:
        return
    known = {
        root_pid,
        *(pid for pid, started in descendants.items() if snapshot.get(pid, (0, "", ""))[1] == started),
    }
    while True:
        discovered = {
            pid for pid, (parent, _started, _state) in snapshot.items() if parent in known and pid not in known
        }
        if not discovered:
            break
        known.update(discovered)
    for pid in known - {root_pid}:
        current = snapshot.get(pid)
        if current is not None:
            descendants[pid] = current[1]


def _signal_descendants(descendants: dict[int, str], signum: int) -> None:
    snapshot = _process_snapshot()
    for pid, started in descendants.items():
        current = snapshot.get(pid, (0, "", ""))
        if current[1] != started or current[2] == "Z":
            continue
        try:
            os.kill(pid, signum)
        except OSError:
            pass


def _live_descendants(descendants: dict[int, str]) -> bool:
    snapshot = _process_snapshot()
    return any(
        current[1] == started and current[2] != "Z"
        for pid, started in descendants.items()
        if (current := snapshot.get(pid)) is not None
    )


def _cleanup_test_process_tree(
    proc: subprocess.Popen[Any],
    descendants: dict[int, str],
    linux_cgroup: Path | None,
    windows_job: int | None,
) -> None:
    _refresh_descendants(proc.pid, descendants)
    _terminate_test_process_group(proc)
    _signal_descendants(descendants, signal.SIGTERM)
    _signal_linux_cgroup(linux_cgroup, signal.SIGTERM)
    deadline = time.monotonic() + _PROCESS_GROUP_TERMINATE_GRACE_SECONDS
    while (_live_descendants(descendants) or _linux_cgroup_has_processes(linux_cgroup)) and time.monotonic() < deadline:
        _refresh_descendants(proc.pid, descendants)
        time.sleep(_PROCESS_POLL_SECONDS)
    # Escalate even if the shell already exited: its process group or a tracked
    # setsid descendant can remain alive after the command outcome is known.
    _kill_test_process_group(proc)
    _signal_descendants(descendants, signal.SIGKILL)
    _kill_linux_cgroup(linux_cgroup)
    _terminate_windows_job(windows_job)


def _terminate_test_process_group(proc: subprocess.Popen[str]) -> None:
    """Best-effort graceful termination of the isolated test process tree."""
    try:
        if os.name == "nt":
            proc.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
        else:
            os.killpg(proc.pid, signal.SIGTERM)
    except (OSError, ValueError):
        if proc.poll() is None:
            try:
                proc.terminate()
            except OSError:
                pass


def _kill_test_process_group(proc: subprocess.Popen[str]) -> None:
    """Best-effort force kill after the graceful cleanup deadline expires."""
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=_PROCESS_GROUP_KILL_GRACE_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
        if proc.poll() is None:
            try:
                proc.kill()
            except OSError:
                pass
        return
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except OSError:
        if proc.poll() is None:
            try:
                proc.kill()
            except OSError:
                pass


def _command_targets_changed_tests(command: str, changed_test_files: list[str]) -> bool:
    """Require a pytest invocation that names at least one changed test target."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        return False
    targets = {path.removeprefix("./") for path in changed_test_files}
    runner_indexes: list[int] = []
    for index, token in enumerate(tokens):
        if Path(token).name == "pytest":
            runner_indexes.append(index)
        elif (Path(token).name.startswith("python") or Path(token).name.endswith("-python")) and tokens[
            index + 1 : index + 3
        ] == ["-m", "pytest"]:
            runner_indexes.append(index + 2)
    if not runner_indexes:
        return False
    arguments = tokens[min(runner_indexes) + 1 :]
    ignored: list[str] = []
    deselected: set[str] = set()
    positional: list[str] = []
    options_with_values = {
        "-c",
        "-k",
        "-m",
        "-o",
        "--basetemp",
        "--capture",
        "--confcutdir",
        "--deselect",
        "--ignore",
        "--ignore-glob",
        "--import-mode",
        "--junit-prefix",
        "--junit-xml",
        "--maxfail",
        "--override-ini",
        "--rootdir",
        "--tb",
    }
    index = 0
    while index < len(arguments):
        token = arguments[index]
        if token == "--":
            positional.extend(arguments[index + 1 :])
            break
        option, separator, inline_value = token.partition("=")
        if option in options_with_values:
            if separator:
                value = inline_value
            elif index + 1 < len(arguments):
                index += 1
                value = arguments[index]
            else:
                return False
            if option in {"--ignore", "--ignore-glob"}:
                ignored.append(value.removeprefix("./"))
            elif option == "--deselect":
                deselected.add(value.removeprefix("./"))
        elif token.startswith("-"):
            pass
        else:
            positional.append(token)
        index += 1

    for token in positional:
        candidate = token.removeprefix("./").split("::", 1)[0]
        if candidate not in targets:
            continue
        if any(candidate == pattern or fnmatch(candidate, pattern) for pattern in ignored):
            continue
        node_id = token.removeprefix("./")
        if node_id in deselected or candidate in deselected:
            continue
        return True
    return False


def _command_evidence(prefix: str, result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    output = f"{result.stdout or ''}{result.stderr or ''}"
    return {
        f"{prefix}_exit_code": result.returncode,
        f"{prefix}_output_tail": output[-_OUTPUT_TAIL_CHARS:],
    }


def _harness_failure_type(result: subprocess.CompletedProcess[str]) -> str | None:
    """Return a typed harness failure, or ``None`` for an assertion-style RED.

    Test runners conventionally reserve exit 1 for an executed, failing suite.
    Pytest's other documented nonzero exits and shell execution failures mean
    that the harness did not establish RED. Signal deaths can be reported by
    ``subprocess`` as ``-N`` or by an intervening shell as ``128 + N``.
    """
    returncode = result.returncode
    if returncode < 0 or returncode > 128:
        return "signal"
    if returncode == 1:
        return None
    return _HARNESS_FAILURE_TYPES.get(returncode, "unexpected_exit")


def verify_red_green(
    worktree: Path | str,
    base_sha: str,
    tip_sha: str,
    test_cmd: str,
    turn_patch: Path | str,
) -> RedGreenResult:
    """Prove that changed tests fail at *base_sha* and pass at *tip_sha*.

    The tip command runs in the existing lane worktree.  The base command runs
    in a detached temporary worktree created by the orchestrator repository,
    and that worktree is removed on every exit path.  Producer result text is
    deliberately not an input to this gate.
    """
    patch_path = Path(turn_patch)
    try:
        patch_stat = patch_path.stat()
    except OSError as exc:
        return _result("patch_unreadable", error=str(exc))
    if not S_ISREG(patch_stat.st_mode):
        return _result("patch_not_file")
    if patch_stat.st_size == 0:
        return _result("empty_patch")
    if not isinstance(test_cmd, str) or not test_cmd.strip():
        return _result("empty_test_cmd")

    lane = Path(worktree).resolve()
    try:
        repository = _orchestrator_repository(lane)
        observed_tip = _run_git(lane, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip()
        expected_tip = _run_git(repository, "rev-parse", "--verify", f"{tip_sha}^{{commit}}").stdout.strip()
        expected_base = _run_git(repository, "rev-parse", "--verify", f"{base_sha}^{{commit}}").stdout.strip()
        if observed_tip != expected_tip:
            return _result("tip_mismatch", expected_tip=expected_tip, observed_tip=observed_tip)
        changed_test_files = _changed_test_files(repository, expected_base, expected_tip)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        return _result("git_error", error=str(exc))

    evidence: dict[str, Any] = {
        "changed_test_files": changed_test_files,
        "timeout_seconds": _lane_check_timeout_seconds(),
        "tip_exit_code": None,
        "tip_outcome": None,
        "base_exit_code": None,
        "base_outcome": None,
    }
    if not changed_test_files:
        return _result("no_test_changes", **evidence)
    if not _command_targets_changed_tests(test_cmd, changed_test_files):
        return _result("test_cmd_not_bound_to_changed_tests", **evidence)

    timeout = evidence["timeout_seconds"]
    try:
        tip_result = _run_test_command(test_cmd, cwd=lane, timeout=timeout)
    except subprocess.TimeoutExpired:
        return _result("tip_timeout", **evidence)
    except OSError as exc:
        return _result("tip_execution_error", error=str(exc), **evidence)
    evidence.update(_command_evidence("tip", tip_result))
    if tip_result.returncode != 0:
        tip_failure_type = _harness_failure_type(tip_result)
        evidence["tip_outcome"] = "failed" if tip_failure_type is None else "harness_error"
        if tip_failure_type is not None:
            evidence["tip_failure_type"] = tip_failure_type
        return _result("tip_failed", **evidence)
    evidence["tip_outcome"] = "passed"

    with tempfile.TemporaryDirectory(prefix="workbay-red-green-") as scratch:
        base_worktree = Path(scratch) / "base"
        added = False
        base_result: subprocess.CompletedProcess[str] | None = None
        base_timeout = False
        base_error: str | None = None
        cleanup_error: str | None = None
        try:
            _run_git(repository, "worktree", "add", "--detach", str(base_worktree), expected_base)
            added = True
            _apply_tip_test_patch(repository, base_worktree, expected_base, expected_tip, changed_test_files)
            try:
                # Root virtualenvs are intentionally untracked (and commonly a
                # managed symlink), so `git worktree add` cannot reproduce the
                # interpreter path used by the tip command. Reuse its installed
                # dependencies; _run_test_command separately pins import roots
                # to the revision under test so tip editables cannot supply base
                # production code.
                lane_venv = lane / ".venv"
                base_venv = base_worktree / ".venv"
                if lane_venv.exists() and not base_venv.exists() and not base_venv.is_symlink():
                    base_venv.symlink_to(lane_venv.resolve(), target_is_directory=True)
                # The proof is candidate tests against base production code.
                # Materialize only the changed test paths from the tip; leaving
                # the detached worktree untouched made new tests disappear and
                # strengthened assertions silently revert to their old form.
                _run_git(base_worktree, "checkout", expected_tip, "--", *changed_test_files)
                base_result = _run_test_command(
                    test_cmd,
                    cwd=base_worktree,
                    timeout=timeout,
                    candidate_worktree=lane,
                )
            except subprocess.TimeoutExpired:
                base_timeout = True
            except OSError as exc:
                base_error = str(exc)
        except (OSError, subprocess.CalledProcessError) as exc:
            base_error = str(exc)
        finally:
            if added:
                try:
                    cleanup = _run_git(repository, "worktree", "remove", "--force", str(base_worktree), check=False)
                    if cleanup.returncode != 0:
                        cleanup_error = (cleanup.stderr or cleanup.stdout).strip()
                except OSError as exc:
                    cleanup_error = str(exc)

    if cleanup_error is not None:
        return _result("base_worktree_cleanup_failed", error=cleanup_error, **evidence)

    if base_timeout:
        return _result("base_timeout", **evidence)
    if base_error is not None:
        return _result("base_execution_error", error=base_error, **evidence)
    if base_result is None:
        return _result("base_execution_error", error="base command produced no result", **evidence)
    evidence.update(_command_evidence("base", base_result))
    if base_result.returncode == 0:
        evidence["base_outcome"] = "passed"
        return _result("base_passed", **evidence)
    failure_type = _harness_failure_type(base_result)
    if failure_type is not None:
        evidence["base_outcome"] = "harness_error"
        evidence["base_failure_type"] = failure_type
        return _result("base_harness_failure", **evidence)
    evidence["base_outcome"] = "failed"
    return _result("red_green", passed=True, **evidence)
