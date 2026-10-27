"""Stateless transport to the VM's sole host-reservation authority."""

from __future__ import annotations

import json
import math
import shlex
import subprocess
from importlib import resources
from pathlib import Path

from . import backend_registry, remote_agent_root


def _invalid(field):
    return {"ok": False, "outcome": "invalid_request", "field": field}


def _tail(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return str(value or "")[-2048:]


def _arguments(operation, args):
    if operation not in ("reserve", "renew", "release", "status"):
        return _invalid("operation")
    if not isinstance(args, dict):
        return _invalid("args")
    fields = {
        "reserve": ("owner", "purpose", "cpu", "memory_bytes", "ttl_s", "max_drain_s"),
        "renew": ("reservation_id", "owner", "ttl_s"),
        "release": ("reservation_id", "owner"),
        "status": (),
    }[operation]
    argv = [operation]
    for field in (*fields, "exclusive", "objective", "start_at"):
        optional = field not in fields
        value = args.get(field)
        if optional and value is None:
            continue
        if field == "exclusive":
            valid = isinstance(value, bool)
        elif field in ("cpu", "ttl_s", "max_drain_s", "memory_bytes"):
            valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
            if field == "memory_bytes":
                valid = valid and type(value) is int
        else:
            valid = isinstance(value, str) and bool(value.strip()) and "\x00" not in value
            if field == "reservation_id":
                valid = valid and not value.startswith("-")
        if not valid:
            return _invalid(field)
        if optional and operation != "reserve":
            continue
        if field == "exclusive":
            if value:
                argv.append("--exclusive")
        elif field == "reservation_id":
            argv.append(value)
        else:
            argv.extend(("--" + field.replace("_", "-"), str(value)))
    return argv


def _module_bytes(workspace_root):
    if workspace_root is not None:
        source = Path(workspace_root) / "scripts/vm/host_reservation.py"
        if source.is_file():
            return source.read_bytes()
    return (resources.files("workbay_system") / "payload/scripts/vm/host_reservation.py").read_bytes()


def run(operation, *, args: dict, workspace_root=None, runner=subprocess.run, timeout_s=60) -> dict:
    """Validate transport fields, ship the canonical CLI, and return typed outcomes."""
    try:
        argv = _arguments(operation, args)
        if isinstance(argv, dict):
            return argv
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or timeout_s <= 0:
            return _invalid("timeout_s")
        host = backend_registry._resolve_remote_gate_host_for_probe(workspace_root=workspace_root)
        if not host:
            return {"ok": False, "outcome": "host_unconfigured"}
        try:
            root = remote_agent_root.resolve_remote_agent_root()
        except ValueError as exc:
            return {**_invalid("root"), "detail": _tail(exc)}
        try:
            module = _module_bytes(workspace_root)
        except (OSError, ImportError, TypeError):
            return {"ok": False, "outcome": "module_unavailable"}
        command = shlex.join(["timeout", str(timeout_s), "python3", "-", "--root", root, *argv])
        try:
            result = runner(
                ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "--", host, command],
                input=module,
                capture_output=True,
                timeout=timeout_s + 15,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return {"ok": False, "outcome": "transport_failure", "detail": _tail(exc.stderr)}
        except OSError as exc:
            return {"ok": False, "outcome": "transport_failure", "detail": _tail(exc)}
        code = result.returncode
        if code == 75:
            return {"ok": False, "outcome": "lock_timeout", "retryable": True}
        if code == 255:
            return {"ok": False, "outcome": "transport_failure", "detail": _tail(result.stderr)}
        invalid = {
            "ok": False,
            "outcome": "invalid_response",
            "exit_code": code,
            "detail": _tail(result.stderr or result.stdout),
        }
        try:
            obj = json.loads(result.stdout)
        except (ValueError, TypeError):
            return invalid
        if not isinstance(obj, dict):
            return invalid
        if code == 0:
            return {"ok": True, "outcome": "ok", "result": obj}
        if code == 3 and isinstance(obj.get("refusal"), str):
            return {"ok": False, "outcome": "refused", "refusal": obj["refusal"], "result": obj}
        return invalid
    except Exception as exc:
        # Boundary guarantee: configuration/resource/runner failures cannot escape MCP.
        return {"ok": False, "outcome": "transport_failure", "detail": _tail(exc)}
