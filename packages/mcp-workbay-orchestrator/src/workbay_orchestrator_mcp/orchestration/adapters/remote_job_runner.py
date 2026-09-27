"""Submit, monitor, and collect detached remote-agent jobs."""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from time import sleep as _sleep
from typing import Any

from workbay_orchestrator_mcp.orchestration.lane_lifecycle_contracts import (
    ContractError,
    RemoteJobIdentity,
)

_HOST_ONLY_ENV_KEYS = (
    "WORKBAY_REMOTE_JOB_TASK_REF",
    "WORKBAY_REMOTE_JOB_LANE_ID",
    "WORKBAY_REMOTE_JOB_PASS_ID",
    "WORKBAY_REMOTE_JOB_ATTEMPT",
)
_POLL_INITIAL_SECONDS = 5.0
_POLL_MAX_SECONDS = 30.0
_CANCEL_TIMEOUT_SECONDS = 10.0


def _identity(env: dict[str, str] | None) -> RemoteJobIdentity | ContractError:
    try:
        return RemoteJobIdentity.from_env(env)
    except ContractError as exc:
        return exc


def _refusal(cmd: list[str], reason: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(cmd, 2, "", reason)


def _child_env(env: dict[str, str] | None) -> dict[str, str]:
    result = dict(os.environ if env is None else env)
    for key in _HOST_ONLY_ENV_KEYS:
        result.pop(key, None)
    return result


def _run_child(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            cwd=cwd,
            env=_child_env(env),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, ValueError) as exc:
        return subprocess.CompletedProcess(
            cmd,
            2,
            "",
            f"remote_job_subprocess_failed reason={type(exc).__name__}",
        )


def _build_command(cmd: list[str], identity: RemoteJobIdentity) -> list[str] | None:
    if len(cmd) < 2 or cmd[1] != "build":
        return None
    return [cmd[0], "submit", "--job-id", identity.job_id, *cmd[2:]]


def _operation_command(cmd: list[str], operation: str, identity: RemoteJobIdentity) -> list[str]:
    result = [cmd[0], operation, "--job-id", identity.job_id]
    if operation == "collect":
        flags = {
            "--out",
            "--result-out",
            "--debug-out",
            "--stream-out",
            "--selfverify-out",
            "--uncommitted-out",
            "--phases-out",
            "--provenance-out",
        }
        for index in range(2, len(cmd) - 1, 2):
            if cmd[index] in flags:
                result.extend(cmd[index : index + 2])
    return result


def _as_completed(
    original_cmd: list[str], completed: subprocess.CompletedProcess[str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        original_cmd,
        completed.returncode,
        completed.stdout or "",
        completed.stderr or "",
    )


def _identity_refusal(
    cmd: list[str], env: dict[str, str] | None
) -> tuple[RemoteJobIdentity | None, subprocess.CompletedProcess[str] | None]:
    identity = _identity(env)
    if isinstance(identity, ContractError):
        return None, _refusal(cmd, f"remote_job_identity_missing {identity.field}")
    return identity, None


def submit_only(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Submit ``build`` as an idempotent remote job without waiting for it."""
    identity, refusal = _identity_refusal(cmd, env)
    if refusal is not None:
        return refusal
    assert identity is not None
    submit_cmd = _build_command(cmd, identity)
    if submit_cmd is None:
        return _refusal(cmd, "remote_job_invalid_command expected=build")
    completed = _run_child(submit_cmd, cwd=cwd, env=env, timeout=timeout)
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip()
        suffix = f" {detail}" if detail else ""
        completed.stderr = f"remote_job_submit_failed rc={completed.returncode}{suffix}"
    return _as_completed(cmd, completed)


def collect_only(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Collect the completed job associated with a ``build`` command."""
    identity, refusal = _identity_refusal(cmd, env)
    if refusal is not None:
        return refusal
    assert identity is not None
    if _build_command(cmd, identity) is None:
        return _refusal(cmd, "remote_job_invalid_command expected=build")
    completed = _run_child(
        _operation_command(cmd, "collect", identity),
        cwd=cwd,
        env=env,
        timeout=timeout,
    )
    return _as_completed(cmd, completed)


@dataclass(frozen=True)
class _TimeoutContext:
    cmd: list[str]
    timeout: float
    identity: RemoteJobIdentity
    cwd: str
    env: dict[str, str] | None
    deadline: float


@dataclass(frozen=True)
class _JobStatus:
    completed: subprocess.CompletedProcess[str]
    payload: dict[str, Any]


def _status_payload(completed: subprocess.CompletedProcess[str], identity: RemoteJobIdentity) -> dict[str, Any] | None:
    try:
        payload: Any = json.loads(completed.stdout)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("job_id") != identity.job_id:
        return None
    if not isinstance(payload.get("state"), str):
        return None
    return payload


def _timed_out(
    context: _TimeoutContext,
    stderr_parts: list[str],
    *,
    stdout: str = "",
    cause: subprocess.TimeoutExpired | None = None,
    include_cause_output: bool = False,
) -> None:
    cancel_cmd = _operation_command(context.cmd, "cancel", context.identity)
    try:
        cancelled = _run_child(
            cancel_cmd,
            cwd=context.cwd,
            env=context.env,
            timeout=_CANCEL_TIMEOUT_SECONDS,
        )
        job_was_absent = cancelled.returncode == 2 and (cancelled.stderr or "").strip() == "remote_agent: job_not_found"
        if cancelled.returncode != 0 and not job_was_absent:
            stderr_parts.append(f"remote_job_cancel_failed rc={cancelled.returncode}")
    except subprocess.TimeoutExpired:
        stderr_parts.append("remote_job_cancel_failed reason=timeout")
    if cause is not None:
        if include_cause_output and cause.output:
            stdout = cause.output.decode(errors="replace") if isinstance(cause.output, bytes) else cause.output
        if cause.stderr:
            detail = cause.stderr.decode(errors="replace") if isinstance(cause.stderr, bytes) else cause.stderr
            stderr_parts.append(detail)
    stderr = "\n".join(part for part in stderr_parts if part)
    raise subprocess.TimeoutExpired(context.cmd, context.timeout, output=stdout, stderr=stderr) from None


def _remaining(context: _TimeoutContext) -> float:
    return context.deadline - time.monotonic()


def _submit_job(
    context: _TimeoutContext,
    submit_cmd: list[str],
    stderr_parts: list[str],
) -> subprocess.CompletedProcess[str] | None:
    try:
        submitted = _run_child(
            submit_cmd,
            cwd=context.cwd,
            env=context.env,
            timeout=max(0.0, _remaining(context)),
        )
    except subprocess.TimeoutExpired as exc:
        _timed_out(context, stderr_parts, cause=exc)
    if submitted.stderr:
        stderr_parts.append(submitted.stderr)
    if submitted.returncode == 0:
        return None
    detail = (submitted.stderr or "").strip()
    suffix = f" {detail}" if detail else ""
    return subprocess.CompletedProcess(
        context.cmd,
        submitted.returncode,
        submitted.stdout or "",
        f"remote_job_submit_failed rc={submitted.returncode}{suffix}",
    )


def _read_status(
    context: _TimeoutContext,
    status_cmd: list[str],
    stderr_parts: list[str],
) -> _JobStatus | subprocess.CompletedProcess[str]:
    try:
        status = _run_child(
            status_cmd,
            cwd=context.cwd,
            env=context.env,
            timeout=max(0.0, _remaining(context)),
        )
    except subprocess.TimeoutExpired as exc:
        _timed_out(context, stderr_parts, cause=exc)
    if status.stderr:
        stderr_parts.append(status.stderr)
    if status.returncode != 0:
        detail = (status.stderr or "").strip()
        suffix = f" {detail}" if detail else ""
        return subprocess.CompletedProcess(
            context.cmd,
            status.returncode,
            "",
            f"remote_job_status_failed rc={status.returncode}{suffix}",
        )
    payload = _status_payload(status, context.identity)
    if payload is None:
        return _refusal(context.cmd, f"remote_job_status_invalid job_id={context.identity.job_id}")
    return _JobStatus(status, payload)


def _poll_job(
    context: _TimeoutContext,
    stderr_parts: list[str],
) -> subprocess.CompletedProcess[str] | None:
    interval = _POLL_INITIAL_SECONDS
    status_cmd = _operation_command(context.cmd, "status", context.identity)
    while True:
        status = _read_status(context, status_cmd, stderr_parts)
        if isinstance(status, subprocess.CompletedProcess):
            return status
        payload = status.payload
        state = payload["state"]
        if state == "lost":
            return subprocess.CompletedProcess(context.cmd, 1, "", f"remote_job_lost job_id={context.identity.job_id}")
        if state == "done":
            return None
        if state == "unknown" and payload.get("observer_error") is None:
            return subprocess.CompletedProcess(
                context.cmd,
                1,
                status.completed.stdout or "",
                f"remote_job_unknown job_id={context.identity.job_id}",
            )
        remaining = _remaining(context)
        if remaining <= 0:
            _timed_out(context, stderr_parts)
        _sleep(min(interval, remaining))
        interval = min(interval * 2, _POLL_MAX_SECONDS)


def _collect_job(
    context: _TimeoutContext,
    stderr_parts: list[str],
) -> subprocess.CompletedProcess[str]:
    collect_cmd = _operation_command(context.cmd, "collect", context.identity)
    try:
        collected = _run_child(
            collect_cmd,
            cwd=context.cwd,
            env=context.env,
            timeout=max(0.0, _remaining(context)),
        )
    except subprocess.TimeoutExpired as exc:
        _timed_out(context, stderr_parts, cause=exc, include_cause_output=True)
    if collected.stderr:
        stderr_parts.append(collected.stderr)
    return subprocess.CompletedProcess(
        context.cmd,
        collected.returncode,
        collected.stdout or "",
        "".join(stderr_parts),
    )


def run_detached(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Run a remote build through submit, bounded status polling, and collect."""
    identity, refusal = _identity_refusal(cmd, env)
    if refusal is not None:
        return refusal
    assert identity is not None
    submit_cmd = _build_command(cmd, identity)
    if submit_cmd is None:
        return _refusal(cmd, "remote_job_invalid_command expected=build")
    context = _TimeoutContext(cmd, timeout, identity, cwd, env, time.monotonic() + timeout)
    stderr_parts: list[str] = []
    submit_error = _submit_job(context, submit_cmd, stderr_parts)
    if submit_error is not None:
        return submit_error
    poll_result = _poll_job(context, stderr_parts)
    if poll_result is not None:
        return poll_result
    return _collect_job(context, stderr_parts)
