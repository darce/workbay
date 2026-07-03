"""Concurrent-boot probe harness for the workbay MCP servers (implementation note).

A harness, *not* a runtime dependency: it reproduces and measures the
boot-time tool-registration miss that implementation note fixes. The harness launches a
server's stdio command, drives it through the minimal MCP handshake
(``initialize`` -> ``notifications/initialized`` -> ``tools/list``) under a
fixed wall-clock deadline, and classifies the launch as *registered* (handshake
completed and a non-empty tool set returned in time) or *missed* (deadline
overrun, empty tool set, crash, or unspawnable). ``concurrent_boot_trials``
fans the probe out to ``parallelism`` simultaneous launches per trial — the
thundering herd that drives the real miss — and reports the pass rate plus the
per-launch latency distribution.

Stdlib only, on purpose: it must launch under a bare ``python3`` and stay
runnable in any checkout, mirroring the launcher shim it validates and keeping
it usable as the falsifiable baseline for the fix (criterion 2).

Used in implementation note to capture the pre-fix ``uv run`` failure rate and the
direct-venv-script latency; reused in implementation note to assert the shim registers
tools in 100% of runs.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

# Canonical (server_id, project_dir, console_script) for the two managed
# servers — mirrors mcp_servers.yaml / _build_local_default_mcp_servers.
HANDOFF = ("workbay-handoff-mcp", "packages/mcp-workbay-handoff", "mcp-workbay-handoff")
ORCHESTRATOR = (
    "workbay-orchestrator-mcp",
    "packages/mcp-workbay-orchestrator",
    "mcp-workbay-orchestrator",
)
_SERVERS = {"handoff": HANDOFF, "orchestrator": ORCHESTRATOR}


@dataclass(frozen=True)
class LaunchSpec:
    """One way to launch a server: the command, forwarded args, and cwd."""

    server_id: str
    command: str
    args: list[str]
    cwd: str


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of a single launch probe."""

    server_id: str
    registered: bool
    tool_count: int
    elapsed_s: float
    reason: str  # ok | timeout | no_tools | spawn_error | handshake_error
    detail: str = ""


@dataclass(frozen=True)
class TrialSummary:
    """Aggregate of ``parallelism`` x ``trials`` probes of one spec."""

    server_id: str
    parallelism: int
    trials: int
    deadline_s: float
    total_launches: int
    registered_count: int
    missed_count: int
    pass_rate: float
    latencies_s: list[float] = field(default_factory=list)

    def percentile(self, q: float) -> float:
        """Nearest-rank percentile of the latency distribution (q in [0,1])."""
        if not self.latencies_s:
            return 0.0
        ordered = sorted(self.latencies_s)
        idx = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
        return ordered[idx]

    @property
    def p50_s(self) -> float:
        return self.percentile(0.50)

    @property
    def p95_s(self) -> float:
        return self.percentile(0.95)

    @property
    def max_s(self) -> float:
        return max(self.latencies_s) if self.latencies_s else 0.0

    def as_dict(self) -> dict:
        return {
            "server_id": self.server_id,
            "parallelism": self.parallelism,
            "trials": self.trials,
            "deadline_s": self.deadline_s,
            "total_launches": self.total_launches,
            "registered_count": self.registered_count,
            "missed_count": self.missed_count,
            "pass_rate": round(self.pass_rate, 4),
            "p50_s": round(self.p50_s, 3),
            "p95_s": round(self.p95_s, 3),
            "max_s": round(self.max_s, 3),
        }


def _write(proc: subprocess.Popen, obj: dict) -> None:
    assert proc.stdin is not None
    proc.stdin.write(json.dumps(obj) + "\n")
    proc.stdin.flush()


def _read_response(proc: subprocess.Popen, expect_id: int) -> dict | None:
    """Read newline-delimited JSON-RPC until the reply for ``expect_id``.

    Returns ``None`` on EOF (the server exited before replying). Unrelated
    notifications / log lines are skipped.
    """
    assert proc.stdout is not None
    while True:
        line = proc.stdout.readline()
        if line == "":
            return None
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("id") == expect_id:
            return msg


def _do_handshake(proc: subprocess.Popen) -> list:
    """Run initialize + tools/list; return the tools list. Raises on protocol
    failure (caught by the caller's worker thread)."""
    _write(
        proc,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mcp-boot-probe", "version": "0"},
            },
        },
    )
    init = _read_response(proc, expect_id=1)
    if init is None:
        raise RuntimeError("no initialize response (server exited)")
    if "error" in init:
        raise RuntimeError(f"initialize returned JSON-RPC error: {init['error']}")
    _write(proc, {"jsonrpc": "2.0", "method": "notifications/initialized"})
    _write(proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    resp = _read_response(proc, expect_id=2)
    if resp is None:
        raise RuntimeError("no tools/list response (server exited)")
    if "error" in resp:
        raise RuntimeError(f"tools/list returned JSON-RPC error: {resp['error']}")
    tools = resp.get("result", {}).get("tools", [])
    # A non-list tools value is an unusable surface -> classified as no_tools.
    return tools if isinstance(tools, list) else []


def _signal(proc: subprocess.Popen, sig: int) -> None:
    """Signal the whole process group when we own one (POSIX), else the proc.

    The ``uv run`` launcher forks a grandchild (the real server); signalling
    only the ``uv`` parent orphans that grandchild. ``start_new_session=True``
    puts the launch in its own group so one ``killpg`` reaps the whole tree.
    """
    if os.name == "posix":
        try:
            os.killpg(os.getpgid(proc.pid), sig)
            return
        except (OSError, ProcessLookupError):
            pass
    try:
        proc.send_signal(sig)
    except (OSError, ValueError):
        pass


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        _signal(proc, signal.SIGTERM)
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            _signal(proc, signal.SIGKILL)
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass


def _close(proc: subprocess.Popen) -> None:
    for stream in (proc.stdin, proc.stdout):
        try:
            if stream is not None:
                stream.close()
        except OSError:
            pass


def _deadline_budget(start: float, deadline_s: float, now: float) -> float:
    """Handshake budget left, charging time already elapsed since launch.

    The deadline must cover process spawn too (the real MCP client times from
    process launch), so the worker-thread join budget is ``deadline_s`` minus the
    time spent spawning + starting the thread. Never negative.
    """
    return max(0.0, deadline_s - (now - start))


def probe_launch(spec: LaunchSpec, *, deadline_s: float) -> ProbeResult:
    """Launch ``spec`` and classify its boot handshake under ``deadline_s``."""
    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            [spec.command, *spec.args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,  # avoid a chatty server filling an unread pipe
            cwd=spec.cwd,
            text=True,
            # Own the launch's process group so _terminate reaps the uv-run
            # grandchild too, not just the uv parent (POSIX only).
            start_new_session=(os.name == "posix"),
        )
    except OSError as exc:
        return ProbeResult(
            spec.server_id, False, 0, time.monotonic() - t0, "spawn_error", str(exc)
        )

    holder: dict = {}

    def _run() -> None:
        try:
            holder["tools"] = _do_handshake(proc)
        except Exception as exc:  # noqa: BLE001 -- report, don't crash the probe
            holder["error"] = str(exc)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    # Charge spawn + thread-start against the deadline (A2): the budget is the
    # deadline minus the time already elapsed since t0, not a fresh deadline_s.
    worker.join(_deadline_budget(t0, deadline_s, time.monotonic()))
    elapsed = time.monotonic() - t0

    if worker.is_alive():
        # Deadline overrun. _terminate's SIGTERM unblocks the worker via EOF;
        # _close (closing our stdout fd) forces the blocked readline out even if
        # the kill failed, so the daemon worker + fds do not leak across a long
        # Slice-2 cold-herd run. Both happen before join.
        _terminate(proc)
        _close(proc)
        worker.join(2.0)
        detail = f"no handshake within {deadline_s}s"
        if worker.is_alive():
            detail += " (warning: reader thread still alive after teardown)"
        return ProbeResult(spec.server_id, False, 0, elapsed, "timeout", detail)

    _terminate(proc)
    _close(proc)

    if "error" in holder:
        return ProbeResult(
            spec.server_id, False, 0, elapsed, "handshake_error", holder["error"]
        )
    tools = holder.get("tools") or []
    tool_count = len(tools)
    if tool_count == 0:
        return ProbeResult(
            spec.server_id, False, 0, elapsed, "no_tools", "tools/list returned empty"
        )
    return ProbeResult(spec.server_id, True, tool_count, elapsed, "ok")


def concurrent_boot_trials(
    spec: LaunchSpec, *, parallelism: int, trials: int, deadline_s: float
) -> TrialSummary:
    """Run ``trials`` rounds of ``parallelism`` simultaneous probes of ``spec``.

    Each round launches ``parallelism`` servers at once — the boot thundering
    herd — so the aggregate pass rate reflects contention, not a single
    uncontended launch (which is why ``claude mcp list`` looks healthy).
    """
    if parallelism < 1 or trials < 1:
        raise ValueError("parallelism and trials must both be >= 1")
    results: list[ProbeResult] = []
    for _ in range(trials):
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = [
                pool.submit(probe_launch, spec, deadline_s=deadline_s)
                for _ in range(parallelism)
            ]
            for future in futures:
                try:
                    results.append(future.result())
                except Exception as exc:  # noqa: BLE001 -- one bad probe must not abort the round
                    results.append(
                        ProbeResult(
                            spec.server_id, False, 0, 0.0, "probe_error", str(exc)
                        )
                    )

    total = len(results)
    registered = sum(1 for r in results if r.registered)
    return TrialSummary(
        server_id=spec.server_id,
        parallelism=parallelism,
        trials=trials,
        deadline_s=deadline_s,
        total_launches=total,
        registered_count=registered,
        missed_count=total - registered,
        pass_rate=(registered / total) if total else 0.0,
        latencies_s=[r.elapsed_s for r in results],
    )


# --- real-launcher spec builders (used by the baseline CLI) ----------------


def uvrun_spec(repo_root: str, server: tuple[str, str, str]) -> LaunchSpec:
    """The current (slow) ``uv run --no-sync --project`` launcher."""
    server_id, project, console = server
    return LaunchSpec(
        server_id=server_id,
        command="uv",
        args=[
            "run",
            "--no-sync",
            "--project",
            project,
            console,
            "--workspace-root",
            ".",
            "serve-stdio",
        ],
        cwd=repo_root,
    )


def direct_spec(repo_root: str, server: tuple[str, str, str]) -> LaunchSpec:
    """The direct per-package venv console script — the shim's fast path.

    Resolves the POSIX ``bin/<console>`` or Windows ``Scripts/<console>.exe``
    layout: whichever exists, else the host-default, so the harness probes the
    real script on either platform (the shim it validates is cross-platform).
    """
    server_id, project, console = server
    venv = os.path.join(repo_root, project, ".venv")
    posix = os.path.join(venv, "bin", console)
    windows = os.path.join(venv, "Scripts", f"{console}.exe")
    if os.path.exists(posix):
        script = posix
    elif os.path.exists(windows):
        script = windows
    else:
        script = windows if os.name == "nt" else posix
    return LaunchSpec(
        server_id=server_id,
        command=script,
        args=["--workspace-root", ".", "serve-stdio"],
        cwd=repo_root,
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="implementation note MCP concurrent-boot probe")
    parser.add_argument("--repo", default=os.getcwd(), help="workspace root")
    parser.add_argument(
        "--server", choices=[*_SERVERS, "both"], default="both"
    )
    parser.add_argument("--mode", choices=["uvrun", "direct", "both"], default="both")
    parser.add_argument("--parallelism", type=int, default=8)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--deadline", type=float, default=30.0)
    args = parser.parse_args(argv)
    if args.parallelism < 1 or args.trials < 1:
        parser.error("--parallelism and --trials must both be >= 1")

    repo = os.path.abspath(args.repo)
    servers = list(_SERVERS.values()) if args.server == "both" else [_SERVERS[args.server]]
    modes = ["uvrun", "direct"] if args.mode == "both" else [args.mode]
    builders = {"uvrun": uvrun_spec, "direct": direct_spec}

    report = []
    for server in servers:
        for mode in modes:
            spec = builders[mode](repo, server)
            summary = concurrent_boot_trials(
                spec,
                parallelism=args.parallelism,
                trials=args.trials,
                deadline_s=args.deadline,
            )
            row = summary.as_dict()
            row["mode"] = mode
            report.append(row)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
