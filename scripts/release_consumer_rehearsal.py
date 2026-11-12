#!/usr/bin/env python3
"""Offline consumer-layout release gate. Invoke with python -s.

Reuse the landed boundary fixture's real Git/venvs, shell rendering, local SSH
seam and model substitute. Its runtime modules are UNIT substitutes: this
rehearses transport wiring, not native execution-policy or model evidence.
No dependencies are installed and no remote transport or model is contacted.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEADLINE_SECONDS = 50


def _load_fixture():
    path = ROOT / "scripts/tests/test_remote_agent_boundary_interpreter.py"
    spec = importlib.util.spec_from_file_location("boundary_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Refused(Exception):
    def __init__(self, refusal: str, output: str = ""):
        self.refusal = refusal
        self.output = output


def _report(step: str, refusal=None, error_type=None, missing_module=None) -> None:
    print(json.dumps({
        "step": step, "refusal": refusal,
        "error_type": error_type, "missing_module": missing_module,
    }))


def _diagnostic(output: str, key: str):
    match = re.search(rf"\b{key}=([^\s]+)", output)
    return match.group(1) if match else None


def _observe(completed) -> str:
    output = completed.stdout + completed.stderr
    print(output, file=sys.stderr, end="" if output.endswith("\n") else "\n")
    return output


def _reserve(boundary, consumer) -> None:
    """Run the live host resolver/bridge and reserve call in a consumer layout.

    Like the worker fixture, the imported module is a UNIT substitute. The
    interpreter selection and isolated version check are the real transport.
    """
    module = boundary.site_packages(consumer.tool) / "workbay_orchestrator_mcp/orchestration/review_cycle_dispatch.py"
    module.write_text(
        "import sys\nfrom ._unit_log import log\n"
        "def shell_entry():\n"
        "    assert sys.argv[1:] == ['reserve', 'unit-reserve', 'unit', 'analysis', 'unit-spec', '']\n"
        "    log('host_reserve')\n"
        "def shell_collect():\n    raise AssertionError('unexpected collect')\n"
    )
    source = consumer.script.read_text()
    # Include the stored pin, VM resolver, host ladder, and bridge unchanged.
    helpers = "_BOUNDARY_RUNTIME_VERSION=" + source.split("\n_BOUNDARY_RUNTIME_VERSION=", 1)[1].split(
        "\n# Read the consumer cadence", 1
    )[0]
    reserve = next(line for line in source.splitlines()
                   if line.strip().startswith('_review_cycle_bridge reserve "$JOB_ID"'))
    script = consumer.tmp / "scripts/unit-host-reserve.sh"
    script.parent.mkdir()
    script.write_text(
        "set -euo pipefail\n" + helpers + "\n"
        "JOB_ID=unit-reserve REVIEW_SUBJECT_BRANCH=unit REVIEW_KIND=analysis AGENT_SPEC=unit-spec CANON_PROPOSAL_PATH=\n"
        + reserve + "\n"
    )
    completed = subprocess.run(
        ["bash", str(script)], cwd=consumer.sbx, capture_output=True, text=True, timeout=30,
        env={"PATH": "/usr/bin:/bin", "HOME": str(consumer.home),
             "PYTHONNOUSERSITE": "1", "UNIT_SITE_LOG": str(consumer.site_log)},
    )
    output = _observe(completed)
    if completed.returncode != 0:
        raise Refused("host_reserve_failed", output)
    if ["host_reserve", str(consumer.tool / "bin/python"), 1] not in consumer.sites():
        raise Refused("host_reserve_not_observed", output)
    print(f"host reserve: ok interpreter={consumer.tool}/bin/python review_kind=analysis", file=sys.stderr)


def _rehearse(tmp: Path, state: dict) -> None:
    boundary = _load_fixture()
    consumer = boundary.Consumer(tmp / "consumer", ROOT / "scripts/remote_agent.sh")
    boundary.install_unit_runtime(consumer.tool)

    # The landed doctor helper runs the real command through a local SSH stub.
    # Share the turn's declared runtime, with the same consumer project metadata.
    doctor_root = tmp / "doctor"
    doctor_root.mkdir()
    run_doctor, doctor_tool = boundary.doctor.__wrapped__(doctor_root)
    (doctor_root / "install/pyproject.toml").write_bytes((consumer.sbx / "pyproject.toml").read_bytes())
    doctor_tool.parent.mkdir(parents=True, exist_ok=True)
    doctor_tool.symlink_to(consumer.tool, target_is_directory=True)
    state["step"] = "doctor"
    doctor = run_doctor()
    output = _observe(doctor)
    # doctor is informational and can exit zero after printing REFUSED.
    expected = f"boundary: ok interpreter={doctor_tool}/bin/python version={boundary.RELEASE}"
    if doctor.returncode != 0 or not doctor.stdout.splitlines() or doctor.stdout.splitlines()[0] != expected:
        raise Refused("doctor_boundary_failed", output)

    state["step"] = "host_reserve"
    _reserve(boundary, consumer)

    state["step"] = "turn"
    turn = consumer.turn()
    output = _observe(turn)
    if turn.returncode != 0:
        raise Refused("consumer_turn_failed", output)
    if consumer.receipt().get("model_process_started") is not True or not consumer.agent_marker.exists():
        raise Refused("model_process_not_started", output)
    if consumer.agent_marker.read_text() != "model-started\n":
        raise Refused("model_process_not_started", output)


def main() -> int:
    # Keep the VM user's .pth workaround and ambient command shims out of all
    # fixture setup children too. The fixture supplies clean per-turn envs.
    os.environ.update(PYTHONNOUSERSITE="1", PATH="/usr/bin:/bin", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
    state = {"step": "fixture"}

    def deadline(signum, frame):
        raise TimeoutError("consumer rehearsal exceeded its deadline")

    previous = signal.signal(signal.SIGALRM, deadline)
    signal.setitimer(signal.ITIMER_REAL, DEADLINE_SECONDS)
    try:
        with tempfile.TemporaryDirectory(prefix="workbay-consumer-rehearsal-") as tmp:
            _rehearse(Path(tmp), state)
    except Refused as exc:
        named = re.search(r"\b(boundary_runtime_missing|boundary_runtime_version_mismatch|runtime_handshake_failed)\b", exc.output)
        _report(state["step"], named.group(1) if named else exc.refusal,
                _diagnostic(exc.output, "error_type"), _diagnostic(exc.output, "module"))
        return 1
    except Exception as exc:
        _report(state["step"], "rehearsal_timeout" if isinstance(exc, TimeoutError) else "rehearsal_failed",
                type(exc).__name__, getattr(exc, "name", None))
        return 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    _report("model_process_started")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
