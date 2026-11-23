"""Detached delivery of requests already committed to the native pass store."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import threading
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from .offload_queue_intent import claimable, read_intent, runtime_scope

if TYPE_CHECKING:
    from workbay_handoff_mcp.config import RuntimeConfig

_MODULE = "workbay_orchestrator_mcp.orchestration.offload_queue_worker"
_LOG = logging.getLogger(__name__)
_PATH_FIELDS = (
    "workspace_root",
    "state_dir",
    "db_path",
    "current_task_path",
    "dashboard_path",
    "exports_dir",
    "artifact_db_path",
    "state_workspace_root",
    "git_workspace_root",
)


def wake(config: RuntimeConfig, pass_id: str | None = None) -> None:
    """Wake one known pass, or rediscover queued rows after MCP startup."""
    process = None
    try:
        # The pipe carries runtime configuration only. The worker loads its
        # request and claim permission from the native pass record.
        values = asdict(config)
        for key in _PATH_FIELDS:
            values[key] = str(values[key])
        wire = json.dumps(values).encode()
        process = subprocess.Popen(
            [sys.executable, "-P", "-m", _MODULE, *([pass_id] if pass_id is not None else [])],
            cwd=config.workspace_root,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        assert process.stdin is not None
        with process.stdin:
            process.stdin.write(wire)
    except (OSError, ValueError):
        _LOG.warning("offload queue wake failed for %s", pass_id or "startup scan", exc_info=True)
    finally:
        if process is not None:
            threading.Thread(target=process.wait, name="offload-queue-reap", daemon=True).start()


def consume(config: RuntimeConfig, pass_id: str) -> None:
    from workbay_orchestrator_mcp import api

    from . import offload_pass

    try:
        with api._offload_invocation_lease(config.state_dir, pass_id) as lease:
            if lease.borrowed:
                return
            state = offload_pass.read_pass_state(config.state_dir, pass_id, strict=True)
            if not state or not claimable(state):
                return
            intent = read_intent(state, pass_id)
            if intent["runtime"] != runtime_scope(config):
                return
            api._run_offload_pass_background(
                intent["request"],
                offload_pass,
                config.state_dir,
                state["task_ref"],
                state["invocation_token"],
                queue_state=state,
            )
    except api._OffloadInvocationBusy:
        return
    except (offload_pass.PassStateUnreadable, ValueError):
        _LOG.warning("offload queue evidence held for %s", pass_id, exc_info=True)


def recover(config: RuntimeConfig) -> None:
    from . import offload_pass

    for path in config.state_dir.glob("offload-pass-*.json"):
        pass_id = path.name.removeprefix("offload-pass-").removesuffix(".json")
        try:
            state = offload_pass.read_pass_state(config.state_dir, pass_id, strict=True)
            if state and claimable(state) and read_intent(state, pass_id)["runtime"] == runtime_scope(config):
                wake(config, pass_id)
        except (offload_pass.PassStateUnreadable, ValueError):
            _LOG.warning("offload queue evidence held for %s", pass_id, exc_info=True)


def main() -> None:
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.runtime import configure_runtime

    values = json.load(sys.stdin)
    for key in _PATH_FIELDS:
        values[key] = Path(values[key])
    for key in ("dashboard_attention_severities", "close_check_required_commands"):
        values[key] = tuple(values[key])
    config = RuntimeConfig(**values)
    configure_runtime(config)
    if len(sys.argv) == 2:
        consume(config, sys.argv[1])
    else:
        recover(config)


if __name__ == "__main__":
    main()
