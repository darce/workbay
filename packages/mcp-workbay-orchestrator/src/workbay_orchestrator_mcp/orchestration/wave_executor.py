"""Durable, detached local executor for wave-supervisor lane jobs.

The executor keeps job state below the repository's Git directory and runs
trusted, explicitly configured argv in a detached local clone.  It never
integrates lane branches; the wave supervisor remains the sole integrator.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

_SCHEMA_VERSION = 1
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_HEX_RE = re.compile(r"^[0-9a-f]+$")
_STAGES = ("implement", "review", "fix", "test")
_MAX_SPEC_BYTES = 256 * 1024
_MAX_PLAN_BYTES = 128 * 1024
_MAX_RESULT_BYTES = 16 * 1024
_MAX_ARG_COUNT = 256
_MAX_ARG_BYTES = 4096
_MAX_TIMEOUT_S = 24 * 60 * 60
_REASONS = {
    "budget_exhausted",
    "command_failed",
    "clone_failed",
    "implement_failed",
    "review_failed",
    "review_mutated_tree",
    "fix_failed",
    "test_failed",
    "test_mutated_tree",
    "invalid_verdict",
    "untracked_files",
    "dirty_worktree",
    "invalid_job_state",
    "worker_exception",
}


class ExecutorError(ValueError):
    """Typed fail-closed executor error; ``status_code`` mirrors HTTP 429/4xx."""

    def __init__(self, reason: str, *, status_code: int = 400) -> None:
        self.reason = reason
        self.status_code = status_code
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class LanePlan:
    backend: str
    model: str
    effort: str
    speed: str
    commands: Mapping[str, tuple[str, ...]]
    timeout_s: float
    digest: str
    canonical: str


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ExecutorError("execution_plan_not_strict_json") from exc


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _safe_identifier(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not _IDENTIFIER_RE.fullmatch(value)
        or ".." in value
        or value.endswith(".")
        or value.lower().endswith(".lock")
    ):
        raise ExecutorError(f"invalid_{field}")
    return value


def _normalise_plan(raw: object, lane_id: str) -> LanePlan:
    if not isinstance(raw, Mapping):
        raise ExecutorError("execution_lane_missing")
    expected = {"backend", "model", "effort", "speed", "commands", "timeout_s"}
    if set(raw) != expected:
        raise ExecutorError("invalid_execution_lane_fields")
    values: dict[str, str] = {}
    for key in ("backend", "model", "effort", "speed"):
        value = raw.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > 256:
            raise ExecutorError(f"invalid_execution_{key}")
        values[key] = value.strip()

    raw_commands = raw.get("commands")
    if not isinstance(raw_commands, Mapping) or set(raw_commands) != set(_STAGES):
        raise ExecutorError("invalid_execution_commands")
    commands: dict[str, tuple[str, ...]] = {}
    for stage in _STAGES:
        argv = raw_commands.get(stage)
        if not isinstance(argv, (list, tuple)) or not argv or len(argv) > _MAX_ARG_COUNT:
            raise ExecutorError(f"invalid_{stage}_argv")
        if any(not isinstance(arg, str) or not arg or len(arg.encode("utf-8")) > _MAX_ARG_BYTES for arg in argv):
            raise ExecutorError(f"invalid_{stage}_argv")
        commands[stage] = tuple(argv)

    timeout = raw.get("timeout_s")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ExecutorError("invalid_execution_timeout")
    timeout_s = float(timeout)
    if not math.isfinite(timeout_s) or timeout_s <= 0 or timeout_s > _MAX_TIMEOUT_S:
        raise ExecutorError("invalid_execution_timeout")

    normalized = {
        "backend": values["backend"],
        "model": values["model"],
        "effort": values["effort"],
        "speed": values["speed"],
        "commands": {stage: list(commands[stage]) for stage in _STAGES},
        "timeout_s": timeout_s,
    }
    canonical = _canonical(normalized)
    if len(canonical.encode("utf-8")) > _MAX_PLAN_BYTES:
        raise ExecutorError("execution_plan_too_large")
    return LanePlan(
        backend=values["backend"],
        model=values["model"],
        effort=values["effort"],
        speed=values["speed"],
        commands=MappingProxyType(commands),
        timeout_s=timeout_s,
        digest=_sha256(canonical),
        canonical=canonical,
    )


def _remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise ExecutorError("budget_exhausted", status_code=429)
    return value


def _clean_env() -> dict[str, str]:
    """Do not let inherited Git routing/configuration escape the selected repo."""
    return {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}


def _git(repo: Path, *args: str, timeout: float = 15.0, deadline: float | None = None) -> str:
    if deadline is not None:
        timeout = min(timeout, _remaining(deadline))
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            env={**_clean_env(), "GIT_OPTIONAL_LOCKS": "0"},
        )
    except subprocess.TimeoutExpired as exc:
        if deadline is not None and time.monotonic() >= deadline:
            raise ExecutorError("budget_exhausted", status_code=429) from exc
        raise ExecutorError("git_command_failed") from exc
    except OSError as exc:
        raise ExecutorError("git_command_failed") from exc
    if result.returncode != 0:
        raise ExecutorError("git_command_failed")
    return result.stdout.decode("utf-8", errors="strict").strip()


def _gitdir(repo: Path) -> Path:
    root = repo.resolve(strict=True)
    if not root.is_dir():
        raise ExecutorError("invalid_repo")
    try:
        raw = _git(root, "rev-parse", "--absolute-git-dir")
    except ExecutorError as exc:
        raise ExecutorError("invalid_repo") from exc
    git_dir = Path(raw).resolve(strict=True)
    if not git_dir.is_dir():
        raise ExecutorError("invalid_repo")
    return git_dir


def _object_format(repo: Path) -> int:
    value = _git(repo, "rev-parse", "--show-object-format")
    if value == "sha1":
        return 40
    if value == "sha256":
        return 64
    raise ExecutorError("unsupported_git_object_format")


def _full_oid(value: object, length: int) -> bool:
    return isinstance(value, str) and len(value) == length and _HEX_RE.fullmatch(value) is not None


def _head(repo: Path, *, deadline: float | None = None) -> str:
    value = _git(repo, "rev-parse", "--verify", "HEAD^{commit}", deadline=deadline)
    if not _full_oid(value, _object_format(repo)):
        raise ExecutorError("invalid_base_commit")
    return value


def _base_for_wave(repo: Path, wave: str, object_length: int, *, deadline: float | None = None) -> str:
    for ref in (f"refs/heads/wave/{wave}", f"refs/heads/wave-base/{wave}"):
        timeout = 15.0 if deadline is None else _remaining(deadline)
        try:
            result = subprocess.run(
                ["git", "-C", str(repo), "rev-parse", "--verify", f"{ref}^{{commit}}"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
                check=False,
                env={**_clean_env(), "GIT_OPTIONAL_LOCKS": "0"},
            )
        except subprocess.TimeoutExpired as exc:
            if deadline is not None and time.monotonic() >= deadline:
                raise ExecutorError("budget_exhausted", status_code=429) from exc
            raise ExecutorError("git_command_failed") from exc
        if result.returncode == 0:
            value = result.stdout.decode("ascii", errors="strict").strip()
            if _full_oid(value, object_length):
                return value
        if deadline is not None and time.monotonic() >= deadline:
            raise ExecutorError("budget_exhausted", status_code=429)
    raise ExecutorError("wave_base_missing")


def _mkdir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    data = _canonical(value).encode("utf-8")
    if len(data) > _MAX_SPEC_BYTES:
        raise ExecutorError("job_state_too_large")
    _mkdir(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        _fsync_dir(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path, *, max_bytes: int = _MAX_SPEC_BYTES) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ExecutorError("job_state_missing") from exc
    if len(raw) > max_bytes:
        raise ExecutorError("job_state_too_large")

    def no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise ValueError("duplicate key")
            output[key] = value
        return output

    try:
        value = json.loads(
            raw, object_pairs_hook=no_duplicate_keys, parse_constant=lambda _: (_ for _ in ()).throw(ValueError())
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        raise ExecutorError("invalid_job_state") from exc
    if not isinstance(value, dict):
        raise ExecutorError("invalid_job_state")
    return value


class _FileLock:
    def __init__(self, path: Path, *, deadline: float | None = None, wait: bool = True) -> None:
        _mkdir(path.parent)
        self._file = path.open("a+b")
        self._locked = False
        while True:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._locked = True
                break
            except BlockingIOError:
                if not wait:
                    self._file.close()
                    raise ExecutorError("coordinator_busy", status_code=429)
                if deadline is not None and time.monotonic() >= deadline:
                    self._file.close()
                    raise ExecutorError("budget_exhausted", status_code=429)
                time.sleep(0.01)

    def close(self) -> None:
        if self._locked:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._locked = False
        self._file.close()

    def __enter__(self) -> _FileLock:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


def _proc_start_identity(pid: int) -> str | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        close = stat.rfind(")")
        fields = stat[close + 2 :].split()
        return fields[19] if close >= 0 and len(fields) > 19 else None
    except (OSError, UnicodeError):
        return None


def _boot_id() -> str:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        return value[:64]
    except OSError:
        return "unknown"


def _proc_cmdline(pid: int) -> list[str] | None:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        return [item.decode("utf-8", errors="replace") for item in raw.split(b"\0") if item]
    except OSError:
        return None


def _worker_process_matches(pid: int, start: object, job_id: str, digest: str) -> bool:
    if type(pid) is not int or _proc_start_identity(pid) != start:
        return False
    argv = _proc_cmdline(pid)
    if not argv:
        return False
    return "--wave-worker" in argv and job_id in argv and digest in argv


def _find_worker(job_id: str, digest: str) -> tuple[int, str] | None:
    proc_root = Path("/proc")
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return None
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        argv = _proc_cmdline(pid)
        if argv and "--wave-worker" in argv and job_id in argv and digest in argv:
            start = _proc_start_identity(pid)
            if start is not None:
                return pid, start
    return None


class LaneExecutor:
    """Durable local job adapter implementing the supervisor LaneExecutor API."""

    def __init__(self, repo: Path, spec: Mapping[str, Any], *, local: bool = False) -> None:
        self.local = local
        if sys.platform != "linux":
            raise ExecutorError("linux_executor_required")
        self.repo = repo.resolve(strict=True)
        self.git_dir = _gitdir(self.repo)
        self.object_length = _object_format(self.repo)
        wave = spec.get("wave")
        self.wave = _safe_identifier(wave, "wave")
        raw_execution = spec.get("execution")
        if not isinstance(raw_execution, Mapping) or set(raw_execution) != {"schema_version", "lanes"}:
            raise ExecutorError("execution_plan_required")
        if type(raw_execution.get("schema_version")) is not int or raw_execution["schema_version"] != _SCHEMA_VERSION:
            raise ExecutorError("unsupported_execution_schema")
        raw_lanes = raw_execution.get("lanes")
        if not isinstance(raw_lanes, Mapping) or not raw_lanes or len(raw_lanes) > 256:
            raise ExecutorError("execution_lanes_required")
        plans: dict[str, LanePlan] = {}
        for raw_lane_id, raw_plan in raw_lanes.items():
            lane_id = _safe_identifier(raw_lane_id, "lane_id")
            plans[lane_id] = _normalise_plan(raw_plan, lane_id)
        self.plans = MappingProxyType(plans)
        self.state_root = self.git_dir / "workbay-waveexecutor" / "waves" / self.wave
        _mkdir(self.state_root)
        self.jobs_root = self.state_root / "jobs"
        self.identities_root = self.state_root / "identities"
        self.locks_root = self.state_root / "locks"
        _mkdir(self.jobs_root)
        _mkdir(self.identities_root)
        _mkdir(self.locks_root)
        try:
            self._coordinator_lock = _FileLock(self.state_root / "coordinator.lock", wait=False)
        except BlockingIOError as exc:  # pragma: no cover - flock uses nonblocking retries
            raise ExecutorError("coordinator_busy", status_code=429) from exc
        except Exception as exc:
            if isinstance(exc, ExecutorError):
                raise
            raise ExecutorError("coordinator_busy", status_code=429) from exc

    def close(self) -> None:
        lock = getattr(self, "_coordinator_lock", None)
        if lock is not None:
            lock.close()
            self._coordinator_lock = None

    def __enter__(self) -> LaneExecutor:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()

    def _plan(self, lane_id: object) -> tuple[str, LanePlan]:
        safe_lane_id = _safe_identifier(lane_id, "lane_id")
        plan = self.plans.get(safe_lane_id)
        if plan is None:
            raise ExecutorError("execution_lane_missing")
        return safe_lane_id, plan

    def _identity(self, lane_id: str, attempt: int) -> str:
        return _sha256(_canonical({"wave": self.wave, "lane_id": lane_id, "attempt": attempt}))[:40]

    def _job_id(self, lane_id: str, attempt: int, digest: str) -> str:
        return _sha256(_canonical({"wave": self.wave, "lane_id": lane_id, "attempt": attempt, "plan_digest": digest}))[
            :40
        ]

    def _job_dir(self, job_id: str) -> Path:
        if not re.fullmatch(r"[0-9a-f]{40}", job_id):
            raise ExecutorError("invalid_job_id")
        return self.jobs_root / job_id

    def _load_job(self, job: object) -> tuple[dict[str, Any], Path]:
        if not isinstance(job, Mapping):
            raise ExecutorError("invalid_job_record")
        if set(job) != {"job_id", "wave", "lane_id", "attempt", "plan_digest"}:
            raise ExecutorError("invalid_job_record")
        job_id = job.get("job_id")
        if not isinstance(job_id, str) or not re.fullmatch(r"[0-9a-f]{40}", job_id):
            raise ExecutorError("invalid_job_record")
        job_dir = self._job_dir(job_id)
        state = _read_json(job_dir / "state.json")
        required = {
            "schema_version",
            "job_id",
            "wave",
            "lane_id",
            "attempt",
            "plan_digest",
            "plan",
            "base_sha",
            "branch",
            "checkout",
            "deadline_monotonic",
            "boot_id",
            "created_at",
            "status",
            "stages",
            "terminal_reason",
        }
        if (
            not required.issubset(state)
            or type(state.get("schema_version")) is not int
            or state.get("schema_version") != _SCHEMA_VERSION
            or state.get("job_id") != job_id
        ):
            raise ExecutorError("invalid_job_state")
        if state.get("wave") != self.wave:
            raise ExecutorError("invalid_job_state")
        if any(job.get(key) != state.get(key) for key in ("job_id", "wave", "lane_id", "attempt", "plan_digest")):
            raise ExecutorError("job_record_mismatch")
        if not isinstance(state.get("lane_id"), str) or state["lane_id"] not in self.plans:
            raise ExecutorError("invalid_job_state")
        if state.get("plan_digest") != self.plans[state["lane_id"]].digest:
            raise ExecutorError("job_plan_mismatch")
        if type(state.get("attempt")) is not int or state["attempt"] < 1:
            raise ExecutorError("invalid_job_state")
        plan = self.plans[state["lane_id"]]
        if state.get("plan") != json.loads(plan.canonical):
            raise ExecutorError("job_plan_mismatch")
        if state.get("job_id") != self._job_id(state["lane_id"], state["attempt"], state["plan_digest"]):
            raise ExecutorError("invalid_job_state")
        if not _full_oid(state.get("base_sha"), self.object_length):
            raise ExecutorError("invalid_job_state")
        if state.get("checkout") != str(job_dir / "checkout"):
            raise ExecutorError("invalid_job_state")
        deadline = state.get("deadline_monotonic")
        status_value = state.get("status")
        try:
            valid_deadline = (
                not isinstance(deadline, bool)
                and isinstance(deadline, (int, float))
                and math.isfinite(float(deadline))
                and float(deadline) > 0
            )
        except OverflowError:
            valid_deadline = False
        if (
            not valid_deadline
            or not isinstance(status_value, str)
            or status_value not in {"queued", "starting", "running", "done", "lost", "unknown"}
        ):
            raise ExecutorError("invalid_job_state")
        if state.get("branch") != f"lane/{state['lane_id']}" or not isinstance(state.get("stages"), dict):
            raise ExecutorError("invalid_job_state")
        return state, job_dir

    def submit(self, wave: str, lane_id: str, attempt: int) -> dict[str, Any]:
        safe_wave = _safe_identifier(wave, "wave")
        if safe_wave != self.wave:
            raise ExecutorError("wave_mismatch")
        safe_lane_id, plan = self._plan(lane_id)
        if type(attempt) is not int or attempt < 1 or attempt > 1_000_000:
            raise ExecutorError("invalid_attempt")
        admission_deadline = time.monotonic() + plan.timeout_s
        identity = self._identity(safe_lane_id, attempt)
        identity_path = self.identities_root / f"{identity}.json"
        lock_path = self.locks_root / f"identity-{identity}.lock"
        with _FileLock(lock_path, deadline=admission_deadline):
            existing_identity: dict[str, Any] | None = None
            if identity_path.exists():
                existing_identity = _read_json(identity_path)
                if existing_identity.get("plan_digest") != plan.digest:
                    raise ExecutorError("plan_mismatch")
                job_id = existing_identity.get("job_id")
                if not isinstance(job_id, str):
                    raise ExecutorError("invalid_job_state")
                state, _job_dir = self._load_job(
                    {
                        "job_id": job_id,
                        "wave": self.wave,
                        "lane_id": safe_lane_id,
                        "attempt": attempt,
                        "plan_digest": plan.digest,
                    }
                )
                return self._public_job(state)

            job_id = self._job_id(safe_lane_id, attempt, plan.digest)
            job_dir = self._job_dir(job_id)
            if job_dir.exists():
                raise ExecutorError("orphaned_job_intent")
            base_sha = _base_for_wave(self.repo, self.wave, self.object_length, deadline=admission_deadline)
            branch = f"lane/{safe_lane_id}"
            checkout = job_dir / "checkout"
            state: dict[str, Any] = {
                "schema_version": _SCHEMA_VERSION,
                "job_id": job_id,
                "wave": self.wave,
                "lane_id": safe_lane_id,
                "attempt": attempt,
                "plan_digest": plan.digest,
                "plan": json.loads(plan.canonical),
                "base_sha": base_sha,
                "branch": branch,
                "checkout": str(checkout),
                "deadline_monotonic": admission_deadline,
                "boot_id": _boot_id(),
                "created_at": time.time(),
                "status": "starting",
                "stages": {},
                "terminal_reason": None,
                "candidate_tip": None,
                "tree": None,
                "pipeline_ok": False,
                "current_stage": None,
                "worker_pid": None,
                "worker_start": None,
                "worker_boot_id": None,
                "collected": None,
            }
            _mkdir(job_dir)
            # Identity and durable intent precede Popen: ambiguity never permits
            # a second effect for this wave/lane/attempt.
            _atomic_json(
                identity_path, {"schema_version": _SCHEMA_VERSION, "plan_digest": plan.digest, "job_id": job_id}
            )
            _atomic_json(job_dir / "state.json", state)
            _fsync_dir(job_dir)
            if admission_deadline <= time.monotonic():
                state.update(status="done", terminal_reason="budget_exhausted", finished_at=time.time())
                _atomic_json(job_dir / "state.json", state)
                raise ExecutorError("budget_exhausted", status_code=429)
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--wave-worker",
                "--repo",
                str(self.repo),
                "--wave",
                self.wave,
                "--job-id",
                job_id,
                "--plan-digest",
                plan.digest,
            ]
            env = _clean_env()
            src_root = str(Path(__file__).resolve().parents[2])
            env["PYTHONPATH"] = src_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
            if not self.local:
                unit = "workbay-job-" + _sha256(str(self.git_dir) + ":" + job_id) + ".service"
                _update_state(job_dir / "state.json", job_unit=unit)
                # env -i also removes Git routing inherited from the user manager.
                command = [
                    "systemd-run",
                    "--user",
                    "--quiet",
                    "--collect",
                    "--unit=" + unit,
                    "--service-type=exec",
                    "--expand-environment=no",
                    "--property=Restart=no",
                    "--property=WorkingDirectory=" + str(self.repo),
                    "--",
                    "/usr/bin/env",
                    "-i",
                    *[f"{key}={value}" for key, value in env.items()],
                    *command,
                ]
                try:
                    launched = subprocess.run(
                        command,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        env=env,
                        timeout=_remaining(admission_deadline),
                        check=False,
                    )
                    if launched.returncode:
                        raise ExecutorError("systemd_job_launch_failed")
                except (OSError, subprocess.TimeoutExpired, ExecutorError) as exc:
                    # A timeout has unknown outcome: retain identity and never relaunch.
                    _update_state(job_dir / "state.json", status="unknown")
                    raise ExecutorError("systemd_job_launch_failed") from exc
            else:
                try:
                    process = subprocess.Popen(
                        command,
                        cwd=self.repo,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        env=env,
                        close_fds=True,
                        start_new_session=True,
                    )
                except OSError as exc:
                    state.update(status="lost", terminal_reason="spawn_failed", finished_at=time.time())
                    _atomic_json(job_dir / "state.json", state)
                    raise ExecutorError("spawn_failed") from exc
                _update_state(
                    job_dir / "state.json",
                    worker_pid=process.pid,
                    worker_start=_proc_start_identity(process.pid),
                    worker_boot_id=_boot_id(),
                )
            return self._public_job(state)

    def _public_job(self, state: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "job_id": state["job_id"],
            "wave": state["wave"],
            "lane_id": state["lane_id"],
            "attempt": state["attempt"],
            "plan_digest": state["plan_digest"],
        }

    def status(self, job: dict[str, Any]) -> str:
        state, job_dir = self._load_job(job)
        if state.get("status") == "done":
            return "done"
        if state.get("status") == "queued":
            return "queued"
        if state.get("status") not in {"starting", "running", "unknown"}:
            return "lost"
        if state.get("boot_id") != _boot_id():
            return self._mark_lost(job_dir, state, "host_restarted")
        pid = state.get("worker_pid")
        start = state.get("worker_start")
        if _worker_process_matches(pid, start, state["job_id"], state["plan_digest"]):
            if state.get("status") != "running":
                return _update_state(job_dir / "state.json", status="running")
            return "running"
        recovered = _find_worker(state["job_id"], state["plan_digest"])
        if recovered is not None:
            return _update_state(
                job_dir / "state.json",
                status="running",
                worker_pid=recovered[0],
                worker_start=recovered[1],
                worker_boot_id=_boot_id(),
            )
        # A worker can durably finish between the first read and process
        # identity check. Re-read before recording loss so completion wins.
        latest = _read_json(job_dir / "state.json")
        if latest.get("status") == "done":
            return "done"
        if latest.get("status") == "lost":
            return "lost"
        if latest.get("status") == "queued":
            return "queued"
        recovered = _find_worker(latest.get("job_id", ""), latest.get("plan_digest", ""))
        if recovered is not None:
            return _update_state(
                job_dir / "state.json",
                status="running",
                worker_pid=recovered[0],
                worker_start=recovered[1],
                worker_boot_id=_boot_id(),
            )
        if latest.get("job_unit"):
            # Type=exec acknowledges env(1); Python may not have published its
            # process identity yet. The manager owns that startup interval.
            try:
                active = subprocess.run(
                    ["systemctl", "--user", "show", latest["job_unit"], "--property=ActiveState", "--value"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    env=_clean_env(),
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise ExecutorError("systemd_job_status_unavailable") from exc
            if active.returncode:
                raise ExecutorError("systemd_job_status_unavailable")
            if active.stdout.strip() in {"active", "activating", "reloading"}:
                return "running"
        return self._mark_lost(job_dir, latest, "worker_identity_missing")

    def _mark_lost(self, job_dir: Path, state: dict[str, Any], reason: str) -> str:
        return _update_state(job_dir / "state.json", status="lost", terminal_reason=reason, finished_at=time.time())

    def collect(self, job: dict[str, Any]) -> dict[str, Any]:
        state, job_dir = self._load_job(job)
        status = self.status(self._public_job(state))
        if status != "done":
            return {
                "ok": False,
                "branch": state["branch"],
                "tip": state.get("candidate_tip") or state["base_sha"],
                "detail": state.get("terminal_reason") or status,
            }
        # status() may update process evidence; reload the durable terminal row.
        state = _read_json(job_dir / "state.json")
        tip = state.get("candidate_tip")
        checkout = job_dir / "checkout"
        if isinstance(tip, str) and _full_oid(tip, self.object_length) and checkout.is_dir():
            branch_ref = f"refs/heads/{state['branch']}"
            checkout_branch = _git(checkout, "rev-parse", "--verify", f"{branch_ref}^{{commit}}")
            if checkout_branch != tip:
                return {"ok": False, "branch": state["branch"], "tip": tip, "detail": "candidate_ref_mismatch"}
            fetch = subprocess.run(
                ["git", "-C", str(self.repo), "fetch", "--no-tags", str(checkout), tip],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
                env={**_clean_env(), "GIT_OPTIONAL_LOCKS": "0"},
            )
            if fetch.returncode != 0:
                return {"ok": False, "branch": state["branch"], "tip": tip, "detail": "local_fetch_failed"}
            fetched = _git(self.repo, "rev-parse", "--verify", f"{tip}^{{commit}}")
            if fetched != tip:
                return {"ok": False, "branch": state["branch"], "tip": tip, "detail": "fetched_tip_mismatch"}
            ancestry = subprocess.run(
                ["git", "-C", str(self.repo), "merge-base", "--is-ancestor", state["base_sha"], tip],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
                check=False,
                env={**_clean_env(), "GIT_OPTIONAL_LOCKS": "0"},
            )
            if ancestry.returncode != 0:
                return {"ok": False, "branch": state["branch"], "tip": tip, "detail": "candidate_not_based_on_wave"}
            local_ref = f"refs/workbay-lanes/{state['wave']}/{state['lane_id']}/{state['job_id']}"
            _git(self.repo, "update-ref", local_ref, tip)
            collected = {
                "tip": tip,
                "branch": state["branch"],
                "local_ref": local_ref,
                "pipeline_ok": state.get("pipeline_ok") is True,
                "tree": state.get("tree"),
                "plan_digest": state["plan_digest"],
                "collected_at": time.time(),
            }
            _update_state(job_dir / "state.json", collected=collected)
            detail = (
                "ok" if state.get("pipeline_ok") is True else str(state.get("terminal_reason") or "pipeline_failed")
            )
            return {"ok": state.get("pipeline_ok") is True, "branch": state["branch"], "tip": tip, "detail": detail}
        detail = str(state.get("terminal_reason") or "candidate_missing")
        tip = tip if isinstance(tip, str) and _full_oid(tip, self.object_length) else state["base_sha"]
        return {"ok": False, "branch": state["branch"], "tip": tip, "detail": detail}

    def gate(self, repo: str | os.PathLike[str], lane_id: str, tip: str) -> dict[str, Any]:
        try:
            safe_lane_id, _plan = self._plan(lane_id)
            gate_repo = Path(repo).resolve(strict=True)
            if _gitdir(gate_repo) != self.git_dir:
                return {"ok": False, "reason": "repository_mismatch"}
            if not _full_oid(tip, self.object_length):
                return {"ok": False, "reason": "invalid_tip"}
            for state_path in sorted(self.jobs_root.glob("*/state.json")):
                try:
                    raw_state = _read_json(state_path)
                    state, job_dir = self._load_job(
                        {
                            "job_id": raw_state.get("job_id"),
                            "wave": raw_state.get("wave"),
                            "lane_id": raw_state.get("lane_id"),
                            "attempt": raw_state.get("attempt"),
                            "plan_digest": raw_state.get("plan_digest"),
                        }
                    )
                except ExecutorError:
                    continue
                if state.get("lane_id") != safe_lane_id or state.get("candidate_tip") != tip:
                    continue
                if state.get("status") != "done" or state.get("pipeline_ok") is not True:
                    continue
                if state.get("plan_digest") != self.plans[safe_lane_id].digest or state.get("wave") != self.wave:
                    continue
                collected = state.get("collected")
                if (
                    not isinstance(collected, dict)
                    or collected.get("tip") != tip
                    or collected.get("pipeline_ok") is not True
                ):
                    continue
                if collected.get("plan_digest") != state["plan_digest"] or collected.get("tree") != state.get("tree"):
                    continue
                expected_ref = f"refs/workbay-lanes/{state['wave']}/{state['lane_id']}/{state['job_id']}"
                if collected.get("branch") != state["branch"] or collected.get("local_ref") != expected_ref:
                    continue
                stages = state.get("stages")
                if not isinstance(stages, dict):
                    continue
                implement = stages.get("implement")
                reviews = stages.get("reviews")
                fix = stages.get("fix")
                test = stages.get("test")
                if not self._valid_verdict_receipt(implement, state, "implement", True):
                    continue
                if not isinstance(reviews, list) or not reviews or len(reviews) > 2:
                    continue
                if reviews[0].get("ok") is False:
                    if (
                        len(reviews) != 2
                        or not self._valid_verdict_receipt(reviews[0], state, "review", False)
                        or not self._valid_verdict_receipt(fix, state, "fix", True)
                        or not self._valid_verdict_receipt(reviews[1], state, "review", True)
                    ):
                        continue
                elif (
                    len(reviews) != 1
                    or not self._valid_verdict_receipt(reviews[0], state, "review", True)
                    or fix is not None
                ):
                    continue
                if not isinstance(test, dict) or test.get("tip") != tip or test.get("tree") != state.get("tree"):
                    continue
                if test.get("exit_code") != 0 or test.get("plan_digest") != state["plan_digest"]:
                    continue
                actual_tip = _git(gate_repo, "rev-parse", "--verify", f"{tip}^{{commit}}")
                actual_tree = _git(gate_repo, "rev-parse", "--verify", f"{tip}^{{tree}}")
                actual_ref = _git(gate_repo, "rev-parse", "--verify", f"{expected_ref}^{{commit}}")
                if actual_tip != tip or actual_ref != tip or actual_tree != state.get("tree"):
                    continue
                ancestry = subprocess.run(
                    ["git", "-C", str(gate_repo), "merge-base", "--is-ancestor", state["base_sha"], tip],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=15,
                    check=False,
                    env=_clean_env(),
                )
                if ancestry.returncode == 0:
                    return {"ok": True, "reason": "exact_tip_receipt"}
            return {"ok": False, "reason": "missing_exact_tip_receipt"}
        except (ExecutorError, OSError, subprocess.SubprocessError):
            return {"ok": False, "reason": "invalid_gate_evidence"}

    @staticmethod
    def _valid_verdict_receipt(receipt: object, state: Mapping[str, Any], stage: str, ok: bool) -> bool:
        if not isinstance(receipt, dict):
            return False
        return (
            receipt.get("schema_version") == _SCHEMA_VERSION
            and type(receipt.get("schema_version")) is int
            and receipt.get("job_id") == state.get("job_id")
            and receipt.get("plan_digest") == state.get("plan_digest")
            and receipt.get("stage") == stage
            and type(receipt.get("ok")) is bool
            and receipt.get("ok") is ok
            and receipt.get("exit_code") == 0
        )


def create(*, repo: Path, spec: dict[str, Any]) -> tuple[LaneExecutor, Any]:
    """Create an executor and its receipt gate for a raw supervisor spec."""
    executor = LaneExecutor(Path(repo), spec)
    return executor, executor.gate


def _read_verdict(path: Path, state: Mapping[str, Any], stage: str) -> dict[str, Any]:
    try:
        receipt = _read_json(path, max_bytes=_MAX_RESULT_BYTES)
    except ExecutorError as exc:
        raise ExecutorError("invalid_verdict") from exc
    if (
        set(receipt) != {"schema_version", "job_id", "plan_digest", "stage", "ok"}
        or type(receipt.get("schema_version")) is not int
        or receipt.get("schema_version") != _SCHEMA_VERSION
        or receipt.get("job_id") != state["job_id"]
        or receipt.get("plan_digest") != state["plan_digest"]
        or receipt.get("stage") != stage
        or type(receipt.get("ok")) is not bool
    ):
        raise ExecutorError("invalid_verdict")
    return receipt


def _tree(repo: Path, *, deadline: float | None = None) -> str:
    return _git(repo, "rev-parse", "--verify", "HEAD^{tree}", deadline=deadline)


def _status(repo: Path, *, deadline: float | None = None) -> tuple[str, ...]:
    raw = _git(repo, "status", "--porcelain=v1", "--untracked-files=all", deadline=deadline)
    return tuple(line for line in raw.splitlines() if line)


def _commit_tracked_changes(repo: Path, state: dict[str, Any], *, deadline: float) -> str:
    rows = _status(repo, deadline=deadline)
    untracked = [row for row in rows if row.startswith("??")]
    if untracked:
        raise ExecutorError("untracked_files")
    if rows:
        _git(repo, "add", "-u", deadline=deadline)
        try:
            staged = subprocess.run(
                ["git", "-C", str(repo), "diff", "--cached", "--quiet"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=_remaining(deadline),
                check=False,
                env=_clean_env(),
            )
        except subprocess.TimeoutExpired as exc:
            raise ExecutorError("budget_exhausted", status_code=429) from exc
        if staged.returncode == 1:
            env = {
                **_clean_env(),
                "GIT_AUTHOR_NAME": "Workbay Wave Executor",
                "GIT_AUTHOR_EMAIL": "wave-executor@localhost",
                "GIT_COMMITTER_NAME": "Workbay Wave Executor",
                "GIT_COMMITTER_EMAIL": "wave-executor@localhost",
                "GIT_OPTIONAL_LOCKS": "0",
            }
            try:
                result = subprocess.run(
                    ["git", "-C", str(repo), "commit", "-m", f"wave {state['wave']} lane {state['lane_id']}"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=_remaining(deadline),
                    check=False,
                    env=env,
                )
            except subprocess.TimeoutExpired as exc:
                raise ExecutorError("budget_exhausted", status_code=429) from exc
            if result.returncode != 0:
                raise ExecutorError("commit_failed")
        elif staged.returncode != 0:
            raise ExecutorError("commit_failed")
    if _status(repo, deadline=deadline):
        raise ExecutorError("dirty_worktree")
    tip = _head(repo, deadline=deadline)
    state["candidate_tip"] = tip
    state["tree"] = _tree(repo, deadline=deadline)
    return tip


def _run_command(
    argv: tuple[str, ...],
    *,
    cwd: Path,
    env: Mapping[str, str],
    log_fd: int,
    deadline: float,
) -> tuple[int, bool]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return 124, True
    try:
        process = subprocess.Popen(
            list(argv),
            cwd=cwd,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=log_fd,
            close_fds=True,
            start_new_session=True,
        )
    except OSError:
        return 127, False
    try:
        return process.wait(timeout=remaining), False
    except subprocess.TimeoutExpired:
        start = _proc_start_identity(process.pid)
        if start is not None and _proc_start_identity(process.pid) == start:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            # Keep the exited group leader unreaped during the grace period so
            # its PID cannot be recycled before the second identity check.
            time.sleep(0.25)
            if _proc_start_identity(process.pid) == start:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
        return 124, True


def _update_state(path: Path, **changes: Any) -> str:
    # All coordinator updates are patches against the latest worker receipt.
    with _FileLock(path.with_suffix(".lock")):
        current = _read_json(path)
        if current.get("status") == "done":
            changes = {key: value for key, value in changes.items() if key == "collected"}
        current.update(changes)
        _atomic_json(path, current)
        return str(current["status"])


def _write_worker_state(path: Path, state: dict[str, Any]) -> None:
    with _FileLock(path.with_suffix(".lock")):
        current = _read_json(path)
        collected = current.get("collected")
        current.update(state)
        if collected is not None:
            current["collected"] = collected
        _atomic_json(path, current)


def _set_state(state_path: Path, state: dict[str, Any], **changes: Any) -> None:
    state.update(changes)
    _write_worker_state(state_path, state)


def _stage_env(state: Mapping[str, Any], result_path: Path, stage: str, plan: Mapping[str, Any]) -> dict[str, str]:
    env = _clean_env()
    env.update(
        {
            "WORKBAY_WAVE_RESULT_PATH": str(result_path),
            "WORKBAY_WAVE_JOB_ID": str(state["job_id"]),
            "WORKBAY_WAVE_PLAN_DIGEST": str(state["plan_digest"]),
            "WORKBAY_WAVE_STAGE": stage,
            "WORKBAY_WAVE_BACKEND": str(plan["backend"]),
            "WORKBAY_WAVE_MODEL": str(plan["model"]),
            "WORKBAY_WAVE_EFFORT": str(plan["effort"]),
            "WORKBAY_WAVE_SPEED": str(plan["speed"]),
        }
    )
    return env


def _run_worker(repo_arg: str, wave: str, job_id: str, plan_digest: str) -> int:
    repo = Path(repo_arg).resolve(strict=True)
    git_dir = _gitdir(repo)
    wave = _safe_identifier(wave, "wave")
    if not re.fullmatch(r"[0-9a-f]{40}", job_id) or not re.fullmatch(r"[0-9a-f]{64}", plan_digest):
        return 2
    job_dir = git_dir / "workbay-waveexecutor" / "waves" / wave / "jobs" / job_id
    state_path = job_dir / "state.json"
    lock = _FileLock(job_dir / "worker.lock")
    try:
        state = _read_json(state_path)
        if state.get("job_id") != job_id or state.get("wave") != wave or state.get("plan_digest") != plan_digest:
            return 2
        if state.get("status") == "done":
            return 0
        pid = os.getpid()
        start = _proc_start_identity(pid)
        state.update(status="running", worker_pid=pid, worker_start=start, worker_boot_id=_boot_id())
        _write_worker_state(state_path, state)
        plan = state.get("plan")
        if not isinstance(plan, dict) or state.get("boot_id") != _boot_id():
            raise ExecutorError("invalid_job_state")
        if _sha256(_canonical(plan)) != plan_digest:
            raise ExecutorError("invalid_job_state")
        deadline = float(state["deadline_monotonic"])
        if deadline <= time.monotonic():
            raise ExecutorError("budget_exhausted", status_code=429)
        checkout = job_dir / "checkout"
        try:
            clone = subprocess.run(
                ["git", "clone", "--no-hardlinks", "--no-checkout", str(repo), str(checkout)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=min(30.0, _remaining(deadline)),
                check=False,
                env=_clean_env(),
            )
        except subprocess.TimeoutExpired as exc:
            raise ExecutorError("budget_exhausted", status_code=429) from exc
        if clone.returncode != 0:
            raise ExecutorError("clone_failed")
        _git(checkout, "remote", "remove", "origin", deadline=deadline)
        if _git(checkout, "remote", deadline=deadline):
            raise ExecutorError("clone_remotes_present")
        _git(checkout, "checkout", "--detach", state["base_sha"], deadline=deadline)
        _git(checkout, "switch", "-c", state["branch"], deadline=deadline)
        _git(checkout, "config", "user.name", "Workbay Wave Executor", deadline=deadline)
        _git(checkout, "config", "user.email", "wave-executor@localhost", deadline=deadline)
        _git(checkout, "config", "core.hooksPath", os.devnull, deadline=deadline)
        # Output is discarded instead of persisted, making log retention
        # exactly zero bytes while keeping verdict files separately bounded.
        log_fd = os.open(os.devnull, os.O_WRONLY)
        stage_number = 0
        try:

            def run_verdict(stage: str) -> tuple[dict[str, Any], int, bool]:
                nonlocal stage_number
                stage_number += 1
                result_path = job_dir / f"result-{stage}.json"
                try:
                    result_path.unlink()
                except FileNotFoundError:
                    pass
                _remaining(deadline)
                started = time.time()
                stage_receipt = {"stage": stage, "started_at": started, "attempt": stage_number}
                state["current_stage"] = stage
                state.setdefault("stage_attempts", []).append(stage_receipt)
                _write_worker_state(state_path, state)
                rc, timed_out = _run_command(
                    tuple(plan["commands"][stage]),
                    cwd=checkout,
                    env=_stage_env(state, result_path, stage, plan),
                    log_fd=log_fd,
                    deadline=deadline,
                )
                finished = time.time()
                verdict: dict[str, Any] | None = None
                if not timed_out:
                    try:
                        verdict = _read_verdict(result_path, state, stage)
                    except ExecutorError:
                        pass
                evidence = {
                    "schema_version": _SCHEMA_VERSION,
                    "job_id": job_id,
                    "plan_digest": plan_digest,
                    "stage": stage,
                    "ok": verdict["ok"] if verdict else False,
                    "exit_code": rc,
                    "timed_out": timed_out,
                    "started_at": started,
                    "finished_at": finished,
                }
                if stage == "review":
                    state.setdefault("stages", {}).setdefault("reviews", []).append(evidence)
                else:
                    state.setdefault("stages", {})[stage] = evidence
                _write_worker_state(state_path, state)
                if timed_out:
                    raise ExecutorError("budget_exhausted", status_code=429)
                if rc != 0 or verdict is None:
                    raise ExecutorError("invalid_verdict" if verdict is None else "command_failed")
                return verdict, rc, False

            try:
                implement_verdict, _rc, _ = run_verdict("implement")
            except ExecutorError as exc:
                # Preserve committed tracked work even when the command or its
                # verdict fails; it remains a failed result and cannot gate.
                if exc.reason == "budget_exhausted":
                    raise
                try:
                    _commit_tracked_changes(checkout, state, deadline=deadline)
                except ExecutorError as content_exc:
                    if content_exc.reason == "untracked_files":
                        raise
                raise ExecutorError("implement_failed") from exc
            tip = _commit_tracked_changes(checkout, state, deadline=deadline)
            if implement_verdict["ok"] is not True:
                raise ExecutorError("implement_failed")
            initial_tree = _tree(checkout, deadline=deadline)
            initial_head = _head(checkout, deadline=deadline)
            if _status(checkout, deadline=deadline):
                raise ExecutorError("dirty_worktree")
            review_verdict, _rc, _ = run_verdict("review")
            if (
                _head(checkout, deadline=deadline) != initial_head
                or _tree(checkout, deadline=deadline) != initial_tree
                or _status(checkout, deadline=deadline)
            ):
                raise ExecutorError("review_mutated_tree")
            if review_verdict["ok"] is False:
                fix_verdict, _rc, _ = run_verdict("fix")
                _commit_tracked_changes(checkout, state, deadline=deadline)
                if fix_verdict["ok"] is not True:
                    raise ExecutorError("fix_failed")
                tip = _head(checkout, deadline=deadline)
                fixed_tree = _tree(checkout, deadline=deadline)
                if _status(checkout, deadline=deadline):
                    raise ExecutorError("dirty_worktree")
                review_verdict, _rc, _ = run_verdict("review")
                if (
                    _head(checkout, deadline=deadline) != tip
                    or _tree(checkout, deadline=deadline) != fixed_tree
                    or _status(checkout, deadline=deadline)
                ):
                    raise ExecutorError("review_mutated_tree")
            if review_verdict["ok"] is not True:
                raise ExecutorError("review_failed")
            tip = _head(checkout, deadline=deadline)
            tree = _tree(checkout, deadline=deadline)
            if _status(checkout, deadline=deadline):
                raise ExecutorError("dirty_worktree")
            result_path = job_dir / "result-test.json"
            try:
                result_path.unlink()
            except FileNotFoundError:
                pass
            test_started = time.time()
            state["current_stage"] = "test"
            state["stage_attempts"].append({"stage": "test", "started_at": test_started, "attempt": stage_number + 1})
            _write_worker_state(state_path, state)
            test_rc, test_timeout = _run_command(
                tuple(plan["commands"]["test"]),
                cwd=checkout,
                env=_stage_env(state, result_path, "test", plan),
                log_fd=log_fd,
                deadline=deadline,
            )
            test_finished = time.time()
            unchanged = (
                _head(checkout, deadline=deadline) == tip
                and _tree(checkout, deadline=deadline) == tree
                and not _status(checkout, deadline=deadline)
            )
            state["stages"]["test"] = {
                "tip": tip,
                "tree": tree,
                "plan_digest": plan_digest,
                "exit_code": test_rc,
                "timed_out": test_timeout,
                "started_at": test_started,
                "finished_at": test_finished,
            }
            state["candidate_tip"] = tip
            state["tree"] = tree
            _write_worker_state(state_path, state)
            if not unchanged:
                raise ExecutorError("test_mutated_tree")
            if test_timeout:
                raise ExecutorError("budget_exhausted", status_code=429)
            if test_rc != 0:
                raise ExecutorError("test_failed")
            state["pipeline_ok"] = True
            _set_state(
                state_path, state, status="done", terminal_reason=None, current_stage=None, finished_at=time.time()
            )
            return 0
        finally:
            os.close(log_fd)
    except ExecutorError as exc:
        try:
            state = _read_json(state_path)
            checkout = job_dir / "checkout"
            if checkout.is_dir():
                try:
                    state["candidate_tip"] = _head(checkout)
                    state["tree"] = _tree(checkout)
                except ExecutorError:
                    pass
            reason = exc.reason if exc.reason in _REASONS else "worker_exception"
            _set_state(
                state_path,
                state,
                status="done",
                pipeline_ok=False,
                terminal_reason=reason,
                current_stage=None,
                finished_at=time.time(),
            )
        except (ExecutorError, OSError):
            pass
        return 1
    except Exception:
        try:
            state = _read_json(state_path)
            _set_state(
                state_path,
                state,
                status="done",
                pipeline_ok=False,
                terminal_reason="worker_exception",
                current_stage=None,
                finished_at=time.time(),
            )
        except (ExecutorError, OSError):
            pass
        return 1
    finally:
        lock.close()


def _worker_main(argv: list[str]) -> int:
    parser = __import__("argparse").ArgumentParser(add_help=False)
    parser.add_argument("--wave-worker", action="store_true")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--wave", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--plan-digest", required=True)
    args = parser.parse_args(argv)
    return _run_worker(args.repo, args.wave, args.job_id, args.plan_digest)


if __name__ == "__main__" and "--wave-worker" in sys.argv:
    raise SystemExit(_worker_main(sys.argv[1:]))
