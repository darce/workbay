"""Remote-gate SSH probe shared by installer and orchestrator (implementation note S1).

The pure reachability probe for the remote execution gate (an operator-provisioned
VM carrying the grok CLI). Extracted from the orchestrator's
``backend_registry._probe_grok_remote`` so ``workbay-bootstrap`` can verify
``install --with-remote`` without importing the orchestrator: this module is the
single owner of the SSH check; the orchestrator's probe is a thin wrapper that
adds its own capability mapping and TTL cache.

Stdlib-only, never raises. Callers branch on the typed ``ProbeResult.state``.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# Environment variable naming the remote gate host (``user@host`` or an ssh
# alias). Shared constant so installer and orchestrator cannot drift.
REMOTE_GATE_HOST_ENV = "WORKBAY_REMOTE_GATE_HOST"

# Command run on the VM to confirm the grok CLI is not just present but actually
# *runnable* the way the gate dispatches it. A bare ``test -x`` on the launcher
# file passes for a present-but-broken binary (wrong arch, partial download, or a
# dangling symlink target) that then fails the real turn with a grok-run error —
# a false "available". So mirror how ``scripts/remote_agent.sh`` resolves grok
# (it prepends ``$HOME/.grok/bin`` to PATH before invoking ``grok``) and exec a
# trivial, side-effect-free subcommand.
_REMOTE_GROK_CLI_TEST = 'export PATH="$HOME/.grok/bin:$PATH"; command -v grok >/dev/null 2>&1 && grok --version >/dev/null 2>&1'

_SSH_CONNECT_TIMEOUT_S = 10
_SSH_TOTAL_TIMEOUT_S = 15

ProbeState = Literal[
    "available",
    "not_configured",
    "malformed_host",
    "unreachable",
    "cli_absent",
]


@dataclass(frozen=True)
class ProbeResult:
    """Typed outcome of one remote-gate probe.

    ``ok`` is True only for ``available``. ``cacheable`` marks states that cost
    a network round-trip (a caller-side TTL cache may retain them); the
    configuration early-returns are never cacheable so a fixed env takes effect
    immediately.
    """

    ok: bool
    state: ProbeState
    detail: str
    cacheable: bool


def probe_remote_gate(host: str | None) -> ProbeResult:
    """Probe the remote gate at ``host`` for SSH reachability + VM grok CLI.

    ``host`` is the raw configured value (usually
    ``os.environ.get(REMOTE_GATE_HOST_ENV)``); ``None``/blank is a typed
    ``not_configured`` result, not an error.
    """
    value = (host or "").strip()
    if not value:
        return ProbeResult(
            ok=False,
            state="not_configured",
            detail=(
                f"{REMOTE_GATE_HOST_ENV} is not set; the remote gate requires it to "
                "point at a provisioned VM (grok CLI + auth present on the host). "
                "See docs/runbooks/remote-gate-provisioning.md."
            ),
            cacheable=False,
        )
    # Fail-closed on a malformed host before it reaches ssh argv (SEC / RES-13):
    # a value that begins with '-' is parsed by ssh as an option (e.g.
    # ``-oProxyCommand=<cmd>`` => arbitrary LOCAL command execution, CWE-88), and
    # embedded whitespace/newlines can smuggle further tokens. The ``--``
    # separator below is belt-and-suspenders for older ssh; this guard is the
    # primary defense.
    if value.startswith("-") or any(ch.isspace() for ch in value):
        return ProbeResult(
            ok=False,
            state="malformed_host",
            detail=(
                "Remote gate host is malformed and was refused before probing "
                f"(leading '-' or whitespace): {value!r}."
            ),
            cacheable=False,
        )
    try:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={_SSH_CONNECT_TIMEOUT_S}",
                "--",
                value,
                _REMOTE_GROK_CLI_TEST,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=_SSH_TOTAL_TIMEOUT_S,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ProbeResult(
            ok=False,
            state="unreachable",
            detail=f"Remote gate '{value}' unreachable: {exc}.",
            cacheable=True,
        )
    if result.returncode == 0:
        return ProbeResult(
            ok=True,
            state="available",
            detail=f"Remote gate '{value}' reachable; VM grok CLI present and runnable.",
            cacheable=True,
        )
    return ProbeResult(
        ok=False,
        state="cli_absent",
        detail=(
            f"Remote gate '{value}' reachable but the VM grok CLI is not runnable "
            "(absent from $HOME/.grok/bin, or present-but-broken: wrong arch, "
            "partial download, or dangling symlink)."
        ),
        cacheable=True,
    )


def resolve_remote_gate_host(repo_root: Path | str | None = None) -> str | None:
    """Resolve the remote-gate host: process env first, then the repo config file.

    Non-login-shell harnesses (GUI editors, spawned agents) often do not inherit
    an operator's shell env, which made a provisioned gate read as
    ``not_configured``. The gitignored ``.workbay/remote-gate.env``
    (``REMOTE_GATE_HOST=<user@host>``, same file ``remote_agent.sh`` already
    reads) is the durable per-repo fallback. Returns ``None`` when neither
    source names a host; never raises.
    """
    env_value = os.environ.get(REMOTE_GATE_HOST_ENV, "").strip()
    if env_value:
        return env_value
    if repo_root is None:
        return None
    config = Path(repo_root) / ".workbay" / "remote-gate.env"
    try:
        raw = config.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("REMOTE_GATE_HOST="):
            value = line.split("=", 1)[1].strip().strip("'\"")
            return value or None
    return None


__all__ = [
    "REMOTE_GATE_HOST_ENV",
    "ProbeResult",
    "ProbeState",
    "probe_remote_gate",
    "resolve_remote_gate_host",
]
