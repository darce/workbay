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

import math
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

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
_REMOTE_GROK_CLI_TEST = (
    'export PATH="$HOME/.grok/bin:$PATH"; command -v grok >/dev/null 2>&1 && grok --version >/dev/null 2>&1'
)

_SSH_CONNECT_TIMEOUT_S = 10
_SSH_TOTAL_TIMEOUT_S = 15

# Default capacity thresholds aligned with scripts/vm/lane_queue.py
# (DEFAULT_CPU_PSI_MAX = 60.0) and internal diagnostics (PERF-06 / OPS-32).
DEFAULT_CPU_PSI_CEILING = 60.0
DEFAULT_NON_CI_RSS_RATIO_MAX = 0.25

# Read-only remote diagnostic snippet collecting host capacity metrics.
# Never mutates state or kills processes on the target VM.
_REMOTE_CAPACITY_PROBE_CMD = (
    "printf 'NPROC=%s\\n' \"$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo '')\"; "
    "awk '/^(MemTotal|MemAvailable|SwapTotal):/ {gsub(/:/, \"\", $1); printf \"%s=%s\\n\", $1, $2}' "
    "/proc/meminfo 2>/dev/null; "
    "awk '/^some / {for (i=1; i<=NF; i++) if ($i ~ /^avg60=/) "
    "{sub(/^avg60=/, \"\", $i); printf \"CPU_SOME_AVG60=%s\\n\", $i; exit}}' "
    "/proc/pressure/cpu 2>/dev/null; "
    "ps -eo rss=,args= 2>/dev/null | "
    "awk 'BEGIN{s=0} $0 !~ /(pytest|workbay|grok|cargo|ruff|mypy|lane_queue|remote_agent)/ {s+=$1} "
    "END{printf \"NON_CI_RSS_KB=%d\\n\", s}'"
)

ProbeState = Literal[
    "available",
    "not_configured",
    "malformed_host",
    "unreachable",
    "cli_absent",
]

CapacityProbeState = Literal[
    "ok",
    "unconfigured",
    "malformed_host",
    "unreachable",
    "parse_error",
]


@dataclass(frozen=True)
class CapacityWarning:
    """Typed non-destructive capacity warning emitted from a VM snapshot."""

    code: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CapacityProbeResult:
    """Typed outcome of one read-only remote VM capacity diagnostic probe."""

    ok: bool
    state: str
    detail: str
    nproc: int | None = None
    mem_total_bytes: int | None = None
    mem_available_bytes: int | None = None
    swap_total_bytes: int | None = None
    cpu_some_avg60: float | None = None
    non_ci_rss_bytes: int | None = None
    warnings: tuple[CapacityWarning, ...] = ()


def _is_malformed_host(value: str) -> bool:
    """Return True when ``value`` is unsafe to pass to ``ssh`` (SEC-01 / RES-13)."""
    return value.startswith("-") or any(
        ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value
    )


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
    if _is_malformed_host(value):
        return ProbeResult(
            ok=False,
            state="malformed_host",
            detail=(
                f"Remote gate host is malformed and was refused before probing (leading '-' or whitespace): {value!r}."
            ),
            cacheable=False,
        )
    try:
        # stdin=DEVNULL + SSH -n: the caller may be the MCP server whose fd0 is
        # the JSON-RPC stdio pipe (never-EOF). A probe child that inherits it
        # consumes the next request and hangs the server (internal).
        result = subprocess.run(
            [
                "ssh",
                "-n",
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
            stdin=subprocess.DEVNULL,
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
        # Preserve CRs: the shell removes only one trailing CR from each line.
        with config.open(encoding="utf-8", newline="") as stream:
            raw = stream.read()
    except OSError:
        return None
    host = None
    for line in raw.split("\n"):
        line = line.removesuffix("\r").lstrip(" \t")
        if line.startswith(("export ", "export\t")):
            line = line[len("export") :].lstrip(" \t")
        if not line.startswith("REMOTE_GATE_HOST="):
            continue
        value = line.split("=", 1)[1]
        if len(value) >= 2 and value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
            if "'" in value:
                continue
        elif len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
            if any(char in value for char in ("$", "`", "\\")):
                continue
        elif any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@._:/-" for char in value):
            continue
        # Like printf -v in the shell reader, the last valid assignment wins.
        host = value or None
    return host


def evaluate_vm_capacity_warnings(
    *,
    nproc: int,
    mem_total_bytes: int,
    mem_available_bytes: int,
    swap_total_bytes: int,
    cpu_some_avg60: float,
    non_ci_rss_bytes: int = 0,
    cpu_psi_ceiling: float = DEFAULT_CPU_PSI_CEILING,
    non_ci_rss_ratio_max: float = DEFAULT_NON_CI_RSS_RATIO_MAX,
) -> list[CapacityWarning]:
    """Evaluate a measured VM capacity snapshot and emit read-only warnings.

    Detects zero swap, CPU PSI saturation, and heavy non-CI resident RSS share
    without taking any destructive action (PERF-06 / OPS-32).
    """
    warnings: list[CapacityWarning] = []

    if swap_total_bytes <= 0:
        warnings.append(
            CapacityWarning(
                code="no_swap",
                severity="warning",
                message=(
                    f"Remote VM reports no configured swap (swap_total_bytes={swap_total_bytes}); "
                    "memory spikes risk sudden OOM kills."
                ),
                details={
                    "swap_total_bytes": swap_total_bytes,
                    "mem_total_bytes": mem_total_bytes,
                    "mem_available_bytes": mem_available_bytes,
                },
            )
        )

    if cpu_some_avg60 >= cpu_psi_ceiling:
        warnings.append(
            CapacityWarning(
                code="cpu_psi_saturated",
                severity="warning",
                message=(
                    f"Remote VM CPU PSI some avg60={cpu_some_avg60:.2f}% reached or exceeded "
                    f"ceiling {cpu_psi_ceiling:.2f}% across {nproc} vCPU(s)."
                ),
                details={
                    "cpu_some_avg60": cpu_some_avg60,
                    "cpu_psi_ceiling": cpu_psi_ceiling,
                    "nproc": nproc,
                },
            )
        )

    if mem_total_bytes > 0:
        non_ci_ratio = non_ci_rss_bytes / mem_total_bytes
        if non_ci_ratio > non_ci_rss_ratio_max:
            warnings.append(
                CapacityWarning(
                    code="high_non_ci_rss",
                    severity="warning",
                    message=(
                        f"Non-CI resident memory is {non_ci_ratio * 100:.1f}% of RAM "
                        f"({non_ci_rss_bytes} / {mem_total_bytes} bytes), exceeding "
                        f"{non_ci_rss_ratio_max * 100:.1f}%."
                    ),
                    details={
                        "non_ci_rss_bytes": non_ci_rss_bytes,
                        "mem_total_bytes": mem_total_bytes,
                        "non_ci_rss_ratio": round(non_ci_ratio, 4),
                        "non_ci_rss_ratio_max": non_ci_rss_ratio_max,
                    },
                )
            )

    return warnings


def _parse_non_negative_int(raw: str, name: str) -> int:
    value = int(raw.strip())
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value


def _parse_capacity_output(stdout: str) -> tuple[int, int, int, int, float, int]:
    """Parse key=value capacity probe output into typed metrics.

    Fails closed by raising ``ValueError`` when any required metric is missing
    or malformed.
    """
    kv: dict[str, str] = {}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "=" in line:
            key, val = line.split("=", 1)
            kv[key.strip()] = val.strip()
        elif ":" in line:
            # Also tolerate raw /proc/meminfo lines like "MemTotal: 16384 kB".
            key, val = line.split(":", 1)
            key = key.strip()
            if key in ("MemTotal", "MemAvailable", "SwapTotal"):
                parts = val.strip().split()
                if parts:
                    kv[key] = parts[0]

    missing = [k for k in ("NPROC", "MemTotal", "MemAvailable", "SwapTotal", "CPU_SOME_AVG60") if not kv.get(k)]
    if missing:
        raise ValueError(f"missing required metric(s): {', '.join(missing)}")

    nproc = _parse_non_negative_int(kv["NPROC"], "NPROC")
    if nproc <= 0:
        raise ValueError(f"NPROC must be positive, got {nproc}")

    mem_total_bytes = _parse_non_negative_int(kv["MemTotal"], "MemTotal") * 1024
    if mem_total_bytes <= 0:
        raise ValueError(f"MemTotal must be positive, got {mem_total_bytes}")

    mem_available_bytes = _parse_non_negative_int(kv["MemAvailable"], "MemAvailable") * 1024
    swap_total_bytes = _parse_non_negative_int(kv["SwapTotal"], "SwapTotal") * 1024

    cpu_some_avg60 = float(kv["CPU_SOME_AVG60"])
    if not math.isfinite(cpu_some_avg60) or cpu_some_avg60 < 0.0:
        raise ValueError(f"CPU_SOME_AVG60 must be finite and non-negative, got {cpu_some_avg60!r}")

    if "NON_CI_RSS_BYTES" in kv and kv["NON_CI_RSS_BYTES"]:
        non_ci_rss_bytes = _parse_non_negative_int(kv["NON_CI_RSS_BYTES"], "NON_CI_RSS_BYTES")
    elif "NON_CI_RSS_KB" in kv and kv["NON_CI_RSS_KB"]:
        non_ci_rss_bytes = _parse_non_negative_int(kv["NON_CI_RSS_KB"], "NON_CI_RSS_KB") * 1024
    else:
        non_ci_rss_bytes = 0

    return (
        nproc,
        mem_total_bytes,
        mem_available_bytes,
        swap_total_bytes,
        cpu_some_avg60,
        non_ci_rss_bytes,
    )


def probe_remote_capacity(host: str | None, *, timeout_s: float = 10.0) -> CapacityProbeResult:
    """Run a read-only capacity diagnostic probe against ``host`` over SSH.

    Validates ``host`` with the SEC-01 guard before spawning ``ssh``, passes
    ``--`` before ``host``, isolates stdin (``-n`` + ``DEVNULL``), parses
    ``nproc``, ``/proc/meminfo``, ``/proc/pressure/cpu``, and non-CI RSS share,
    and attaches ``evaluate_vm_capacity_warnings(...)``. Never kills processes
    and never raises.
    """
    value = (host or "").strip()
    if not value:
        return CapacityProbeResult(
            ok=False,
            state="unconfigured",
            detail=f"{REMOTE_GATE_HOST_ENV} is not set; cannot probe remote VM capacity.",
        )
    if _is_malformed_host(value):
        return CapacityProbeResult(
            ok=False,
            state="malformed_host",
            detail=(
                "Remote capacity host is malformed and was refused before probing "
                f"(leading '-', whitespace, or control character): {value!r}."
            ),
        )

    effective_timeout = max(1.0, float(timeout_s)) if math.isfinite(timeout_s) else float(_SSH_CONNECT_TIMEOUT_S)
    connect_timeout = max(1, int(effective_timeout))

    try:
        result = subprocess.run(
            [
                "ssh",
                "-n",
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={connect_timeout}",
                "--",
                value,
                _REMOTE_CAPACITY_PROBE_CMD,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=effective_timeout,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return CapacityProbeResult(
            ok=False,
            state="unreachable",
            detail=f"Remote host '{value}' unreachable during capacity probe: {exc}.",
        )

    if result.returncode != 0:
        stderr_snippet = (result.stderr or "").strip().splitlines()
        reason = stderr_snippet[0] if stderr_snippet else "non-zero exit status"
        return CapacityProbeResult(
            ok=False,
            state="unreachable",
            detail=f"Remote capacity probe failed on '{value}' (exit {result.returncode}): {reason}.",
        )

    try:
        (
            nproc,
            mem_total_bytes,
            mem_available_bytes,
            swap_total_bytes,
            cpu_some_avg60,
            non_ci_rss_bytes,
        ) = _parse_capacity_output(result.stdout or "")
    except ValueError as exc:
        return CapacityProbeResult(
            ok=False,
            state="parse_error",
            detail=f"Malformed capacity probe output from '{value}': {exc}.",
        )

    warnings = tuple(
        evaluate_vm_capacity_warnings(
            nproc=nproc,
            mem_total_bytes=mem_total_bytes,
            mem_available_bytes=mem_available_bytes,
            swap_total_bytes=swap_total_bytes,
            cpu_some_avg60=cpu_some_avg60,
            non_ci_rss_bytes=non_ci_rss_bytes,
        )
    )
    warning_summary = (
        f"{len(warnings)} warning(s): {', '.join(w.code for w in warnings)}"
        if warnings
        else "healthy (no warnings)"
    )
    return CapacityProbeResult(
        ok=True,
        state="ok",
        detail=f"Remote VM '{value}' capacity probed: {warning_summary}.",
        nproc=nproc,
        mem_total_bytes=mem_total_bytes,
        mem_available_bytes=mem_available_bytes,
        swap_total_bytes=swap_total_bytes,
        cpu_some_avg60=cpu_some_avg60,
        non_ci_rss_bytes=non_ci_rss_bytes,
        warnings=warnings,
    )


__all__ = [
    "DEFAULT_CPU_PSI_CEILING",
    "DEFAULT_NON_CI_RSS_RATIO_MAX",
    "REMOTE_GATE_HOST_ENV",
    "CapacityProbeResult",
    "CapacityProbeState",
    "CapacityWarning",
    "ProbeResult",
    "ProbeState",
    "evaluate_vm_capacity_warnings",
    "probe_remote_capacity",
    "probe_remote_gate",
    "resolve_remote_gate_host",
]
