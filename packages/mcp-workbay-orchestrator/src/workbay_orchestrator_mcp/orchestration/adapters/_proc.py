"""Shared bounded-subprocess execution for CLI adapters.

Modelled on ``grok_cli``'s private ``_run_bounded``/``_terminate_process_group``.
Both drive an autonomous CLI (``grok --always-approve``, ``cursor-agent
--force``) that spawns tool/shell grandchildren inside a lane worktree, so both
need the kill to reach the whole process GROUP rather than just the direct
child.

``grok_cli`` deliberately KEEPS its own copy rather than importing these. Its
``_run_bounded`` calls the module-level ``_terminate_process_group`` by name,
and ``test_grok_cli.py`` patches that attribute
(``mock.patch.object(grok_cli, "_terminate_process_group")``) to assert the
group kill fires on timeout. Re-pointing grok at this module would route the
call through ``_proc``'s own reference, leaving the patch installed but never
invoked — a green test that no longer proves anything. Converging the two is a
follow-up that must also move that patch seam (e.g. an injectable terminator),
not a drive-by import swap.
"""

from __future__ import annotations

import os
import signal
import subprocess


def terminate_process_group(proc: "subprocess.Popen[str]") -> None:
    """SIGKILL the child's whole process group (best effort)."""
    try:
        pgid = os.getpgid(proc.pid)
    except (ProcessLookupError, OSError):
        proc.kill()
        return
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, OSError):
        proc.kill()


def run_bounded(
    cmd: list[str],
    *,
    env: dict[str, str],
    cwd: str,
    timeout: int,
) -> "subprocess.CompletedProcess[str]":
    """Run ``cmd`` with a wall-clock bound that kills the whole process GROUP.

    ``subprocess.run(timeout=...)`` kills only the direct child on
    TimeoutExpired; an autonomous agent CLI spawns tool/shell grandchildren
    which would be re-parented and keep MUTATING the lane worktree after the
    adapter already raised (s3-a-005). Running in a new session
    (``start_new_session``) and ``os.killpg``-ing the group on timeout stops the
    whole tree.
    """
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=cwd,
        env=env,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        terminate_process_group(proc)
        try:
            stdout, stderr = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        raise subprocess.TimeoutExpired(cmd, timeout, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
