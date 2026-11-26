"""Opt-in PERF-06 latency baseline harness for workbay_handoff_mcp.

Measures end-to-end subprocess latency (p50/p95, default n=20) for:
  1. Importing ``workbay_handoff_mcp.api``
  2. ``mcp-workbay-handoff state <task_ref>``
  3. ``mcp-workbay-handoff find-related-prior-work --text ...`` with the
     embedding sidecar warm, absent, and disabled (``WORKBAY_EMBED_SIDECAR_DISABLED``)
  4. Top 15 ``python -X importtime`` cumulative offenders for ``workbay_handoff_mcp.api``

Security / hygiene ([SECD-01]):
  - Never emits any environment variable values or host/temp filesystem paths;
    only the *names* of environment toggles used for each scenario are reported.
  - Opt-in only: not invoked during the default pytest tier and no latency
    numbers are asserted in CI.

Usage::

    python -m workbay_handoff_mcp.embeddings.latency_bench --iterations 20
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from workbay_handoff_mcp.embeddings.model_pin import EMBEDDING_DIM, MODEL_ID
from workbay_handoff_mcp.embeddings.provider import (
    ENV_EMBEDDINGS_DISABLED,
    ENV_MODEL,
    ENV_MODEL_SHA256,
    ENV_TOKENIZER,
    ENV_TOKENIZER_SHA256,
)
from workbay_handoff_mcp.embeddings.sidecar import (
    ENV_LOAD_TIMEOUT_SECONDS,
    EmbeddingSidecar,
    resolve_socket_path,
)
from workbay_handoff_mcp.embeddings.store import ENV_EMBED_SIDECAR_DISABLED, serialize_vector, text_hash

DEFAULT_ITERATIONS = 20
DEFAULT_TOP_IMPORTS = 15
DEFAULT_TASK_REF = "PERF-06-BENCH"
DEFAULT_QUERY_TEXT = "prior work semantic latency baseline query"

_IMPORTTIME_LINE_RE = re.compile(r"^import time:\s+(\d+)\s+\|\s+(\d+)\s+\|\s+(.+?)\s*$")

_ENV_ARTIFACT_KEYS: tuple[str, ...] = (
    ENV_MODEL,
    ENV_TOKENIZER,
    ENV_MODEL_SHA256,
    ENV_TOKENIZER_SHA256,
)


class _DeterministicBenchProvider:
    """In-memory deterministic provider for the warm-sidecar benchmark.

    Avoids requiring a 147 MB ONNX artifact on developer/CI machines while
    exercising the real Unix-socket sidecar protocol end-to-end.
    """

    model_id = MODEL_ID
    dim = EMBEDDING_DIM

    def embed(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), self.dim), dtype="<f4")
        for idx, _text in enumerate(texts):
            out[idx, 0] = 1.0
        return out


def percentile(ordered: Sequence[float], pct: float) -> float:
    """Linear-interpolated percentile over a non-empty sorted sequence."""
    if not ordered:
        raise ValueError("cannot compute percentile of empty sequence")
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(ordered[low])
    weight = rank - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def summarize_samples(samples_ms: Sequence[float]) -> dict[str, Any]:
    """Summarize millisecond latency samples as n, min, p50, p95, max."""
    ordered = sorted(float(s) for s in samples_ms)
    return {
        "n": len(ordered),
        "min_ms": round(ordered[0], 2),
        "p50_ms": round(percentile(ordered, 50.0), 2),
        "p95_ms": round(percentile(ordered, 95.0), 2),
        "max_ms": round(ordered[-1], 2),
    }


def parse_importtime_output(stderr_text: str, *, top_n: int = DEFAULT_TOP_IMPORTS) -> list[dict[str, Any]]:
    """Parse ``python -X importtime`` stderr and return top ``top_n`` cumulative offenders."""
    rows: list[tuple[int, int, str]] = []
    for line in stderr_text.splitlines():
        match = _IMPORTTIME_LINE_RE.match(line)
        if match is None:
            continue
        self_us = int(match.group(1))
        cumulative_us = int(match.group(2))
        module_name = match.group(3).strip()
        rows.append((cumulative_us, self_us, module_name))
    rows.sort(key=lambda item: (-item[0], -item[1], item[2]))
    return [
        {
            "rank": idx,
            "module": module_name,
            "cumulative_ms": round(cumulative_us / 1000.0, 2),
            "self_ms": round(self_us / 1000.0, 2),
        }
        for idx, (cumulative_us, self_us, module_name) in enumerate(rows[: max(0, top_n)], start=1)
    ]


def _base_subprocess_env() -> dict[str, str]:
    """Minimal child environment preserving interpreter/module resolution without leaking secrets."""
    env: dict[str, str] = {}
    for key in ("PATH", "PYTHONPATH", "VIRTUAL_ENV", "HOME", "XDG_RUNTIME_DIR", "TMPDIR", "LANG", "LC_ALL"):
        val = os.environ.get(key)
        if val is not None:
            env[key] = val
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _run_timed(
    cmd: Sequence[str],
    *,
    env: Mapping[str, str],
    timeout_seconds: float = 30.0,
) -> tuple[float, int, str, str]:
    start = time.perf_counter()
    proc = subprocess.run(
        list(cmd),
        env=dict(env),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return elapsed_ms, proc.returncode, proc.stdout, proc.stderr


def _extract_prior_work_status(stdout_text: str) -> str | None:
    try:
        payload = json.loads(stdout_text)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        data_status = data.get("status")
        if isinstance(data_status, str):
            return data_status
    top_status = payload.get("status")
    if isinstance(top_status, str):
        return top_status
    return None


def _seed_bench_state(workspace_root: Path, state_dir: Path, task_ref: str) -> None:
    """Create a minimal handoff DB with one task and one concept embedding row."""
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.runtime import configure_runtime, reset_runtime_config
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    state_dir.mkdir(parents=True, exist_ok=True)
    runtime = RuntimeConfig.for_workspace(
        workspace_root,
        state_dir=state_dir,
        current_task_path=workspace_root / "CURRENT_TASK.json",
    )
    configure_runtime(runtime)
    try:
        with _get_db_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO handoff_state
                    (task_ref, status, objective, focus, updated_at)
                VALUES (?, 'in_progress', 'PERF-06 latency baseline', 'measure before optimizing', datetime('now'))
                """,
                (task_ref,),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO review_findings
                    (id, task_ref, finding_id, severity, file_path, description,
                     status, session, created_at, updated_at)
                VALUES (1, ?, 'FID-BENCH-1', 'low', 'bench.py',
                        'prior work semantic latency baseline finding',
                        'open', 'bench-session', datetime('now'), datetime('now'))
                """,
                (task_ref,),
            )
            vec = np.zeros(EMBEDDING_DIM, dtype="<f4")
            vec[0] = 1.0
            conn.execute(
                """
                INSERT OR REPLACE INTO concept_embeddings
                    (entity_kind, entity_id, task_ref, text_hash, dim, vector, model_id, created_at)
                VALUES ('finding.description', '1', ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    task_ref,
                    text_hash("prior work semantic latency baseline finding"),
                    EMBEDDING_DIM,
                    serialize_vector(vec),
                    MODEL_ID,
                ),
            )
            conn.commit()
    finally:
        reset_runtime_config()


def _create_dummy_artifacts(base_dir: Path) -> dict[str, str]:
    """Write small deterministic dummy artifact files and return their env mapping."""
    model_path = base_dir / "bench-model.onnx"
    tokenizer_path = base_dir / "bench-tokenizer.json"
    model_bytes = b"workbay-perf06-bench-dummy-onnx"
    tokenizer_bytes = b'{"workbay_bench": true}'
    model_path.write_bytes(model_bytes)
    tokenizer_path.write_bytes(tokenizer_bytes)
    return {
        ENV_MODEL: str(model_path),
        ENV_TOKENIZER: str(tokenizer_path),
        ENV_MODEL_SHA256: hashlib.sha256(model_bytes).hexdigest(),
        ENV_TOKENIZER_SHA256: hashlib.sha256(tokenizer_bytes).hexdigest(),
    }


def _start_warm_sidecar(state_dir: Path) -> tuple[EmbeddingSidecar, threading.Thread, list[BaseException]]:
    """Run a real Unix-socket sidecar with a deterministic in-memory provider."""
    server = EmbeddingSidecar(state_dir, provider=_DeterministicBenchProvider())
    errors: list[BaseException] = []

    def _run() -> None:
        try:
            server.serve_forever()
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(exc)

    thread = threading.Thread(target=_run, name="perf06-bench-sidecar", daemon=True)
    thread.start()
    socket_path = resolve_socket_path(state_dir / "embeddings")
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if errors:
            raise RuntimeError(f"bench sidecar failed to start: {type(errors[0]).__name__}: {errors[0]}")
        if socket_path.exists() and server.ready:
            return server, thread, errors
        time.sleep(0.01)
    server.stop()
    thread.join(timeout=2.0)
    raise RuntimeError("bench sidecar socket did not become ready in time")


def _stop_warm_sidecar(server: EmbeddingSidecar, thread: threading.Thread) -> None:
    server.stop()
    thread.join(timeout=2.0)


def measure_importtime(*, top_n: int = DEFAULT_TOP_IMPORTS) -> list[dict[str, Any]]:
    """Measure ``python -X importtime -c 'import workbay_handoff_mcp.api'`` top offenders."""
    env = _base_subprocess_env()
    _elapsed, rc, _stdout, stderr = _run_timed(
        [sys.executable, "-X", "importtime", "-c", "import workbay_handoff_mcp.api"],
        env=env,
    )
    if rc != 0:
        raise RuntimeError(f"importtime probe failed with exit code {rc}")
    return parse_importtime_output(stderr, top_n=top_n)


def run_baseline(
    *,
    iterations: int = DEFAULT_ITERATIONS,
    top_imports: int = DEFAULT_TOP_IMPORTS,
    task_ref: str = DEFAULT_TASK_REF,
    query_text: str = DEFAULT_QUERY_TEXT,
) -> dict[str, Any]:
    """Run the full PERF-06 before-baseline benchmark suite and return a JSON-serializable dict."""
    if iterations < 1:
        raise ValueError("iterations must be >= 1")
    if top_imports < 1:
        raise ValueError("top_imports must be >= 1")

    sock_base = "/tmp" if Path("/tmp").is_dir() else None
    with (
        tempfile.TemporaryDirectory(prefix="workbay-perf06-") as tmp_str,
        tempfile.TemporaryDirectory(prefix="workbay-perf06-sock-", dir=sock_base) as sock_str,
    ):
        tmp_root = Path(tmp_str)
        ws_root = tmp_root / "workspace"
        ws_root.mkdir(mode=0o700)
        socket_home = Path(sock_str) / "home"
        socket_home.mkdir(mode=0o700)
        artifact_env = _create_dummy_artifacts(tmp_root)

        state_main = tmp_root / "state-main"
        state_warm = tmp_root / "state-warm"
        state_absent = tmp_root / "state-absent"
        state_disabled = tmp_root / "state-disabled"
        for sdir in (state_main, state_warm, state_absent, state_disabled):
            _seed_bench_state(ws_root, sdir, task_ref)

        base_env = _base_subprocess_env()
        base_env["HOME"] = str(socket_home)
        base_env.pop("XDG_RUNTIME_DIR", None)
        for key in (*_ENV_ARTIFACT_KEYS, ENV_EMBEDDINGS_DISABLED, ENV_EMBED_SIDECAR_DISABLED):
            base_env.pop(key, None)

        # 1. Import workbay_handoff_mcp.api
        import_samples: list[float] = []
        for _ in range(iterations):
            ms, rc, _out, _err = _run_timed(
                [sys.executable, "-c", "import workbay_handoff_mcp.api"],
                env=base_env,
            )
            if rc != 0:
                raise RuntimeError(f"import workbay_handoff_mcp.api exited {rc}")
            import_samples.append(ms)

        # 2. mcp-workbay-handoff state <task_ref>
        state_samples: list[float] = []
        state_cmd = [
            sys.executable,
            "-m",
            "workbay_handoff_mcp.cli",
            "--workspace-root",
            str(ws_root),
            "--state-dir",
            str(state_main),
            "state",
            task_ref,
        ]
        for _ in range(iterations):
            ms, rc, _out, _err = _run_timed(state_cmd, env=base_env)
            if rc != 0:
                raise RuntimeError(f"cli state exited {rc}")
            state_samples.append(ms)

        # 3a. find-related-prior-work --text ... (sidecar warm)
        warm_env = {**base_env, **artifact_env}
        warm_cmd = [
            sys.executable,
            "-m",
            "workbay_handoff_mcp.cli",
            "--workspace-root",
            str(ws_root),
            "--state-dir",
            str(state_warm),
            "find-related-prior-work",
            "--text",
            query_text,
        ]
        warm_samples: list[float] = []
        warm_status: str | None = None
        # Point current process HOME to socket_home while binding the sidecar socket.
        prev_home = os.environ.get("HOME")
        prev_xdg = os.environ.get("XDG_RUNTIME_DIR")
        os.environ["HOME"] = str(socket_home)
        os.environ.pop("XDG_RUNTIME_DIR", None)
        try:
            server, thread, _errors = _start_warm_sidecar(state_warm)
            try:
                for _ in range(iterations):
                    ms, rc, out, _err = _run_timed(warm_cmd, env=warm_env)
                    if rc != 0:
                        raise RuntimeError(f"cli find-related-prior-work (warm) exited {rc}")
                    warm_samples.append(ms)
                    if warm_status is None:
                        warm_status = _extract_prior_work_status(out)
            finally:
                _stop_warm_sidecar(server, thread)
        finally:
            if prev_home is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = prev_home
            if prev_xdg is not None:
                os.environ["XDG_RUNTIME_DIR"] = prev_xdg

        # 3b. find-related-prior-work --text ... (sidecar absent)
        absent_env = {**base_env, **artifact_env, ENV_LOAD_TIMEOUT_SECONDS: "2"}
        absent_cmd = [
            sys.executable,
            "-m",
            "workbay_handoff_mcp.cli",
            "--workspace-root",
            str(ws_root),
            "--state-dir",
            str(state_absent),
            "find-related-prior-work",
            "--text",
            query_text,
        ]
        absent_samples: list[float] = []
        absent_status: str | None = None
        for _ in range(iterations):
            ms, rc, out, _err = _run_timed(absent_cmd, env=absent_env)
            if rc != 0:
                raise RuntimeError(f"cli find-related-prior-work (absent) exited {rc}")
            absent_samples.append(ms)
            if absent_status is None:
                absent_status = _extract_prior_work_status(out)

        # 3c. find-related-prior-work --text ... (sidecar disabled)
        disabled_env = {**base_env, **artifact_env, ENV_EMBED_SIDECAR_DISABLED: "1"}
        disabled_cmd = [
            sys.executable,
            "-m",
            "workbay_handoff_mcp.cli",
            "--workspace-root",
            str(ws_root),
            "--state-dir",
            str(state_disabled),
            "find-related-prior-work",
            "--text",
            query_text,
        ]
        disabled_samples: list[float] = []
        disabled_status: str | None = None
        for _ in range(iterations):
            ms, rc, out, _err = _run_timed(disabled_cmd, env=disabled_env)
            if rc != 0:
                raise RuntimeError(f"cli find-related-prior-work (disabled) exited {rc}")
            disabled_samples.append(ms)
            if disabled_status is None:
                disabled_status = _extract_prior_work_status(out)

        # 4. Top N -X importtime cumulative offenders
        importtime_offenders = measure_importtime(top_n=top_imports)

    return {
        "schema": "workbay/perf06-latency-baseline@1",
        "iterations": iterations,
        "benchmarks": {
            "import_api": {
                "description": "import workbay_handoff_mcp.api",
                "env_toggles": ["PYTHONDONTWRITEBYTECODE"],
                **summarize_samples(import_samples),
            },
            "cli_state": {
                "description": "mcp-workbay-handoff state <task_ref>",
                "env_toggles": ["PYTHONDONTWRITEBYTECODE"],
                **summarize_samples(state_samples),
            },
            "cli_find_related_prior_work_sidecar_warm": {
                "description": "mcp-workbay-handoff find-related-prior-work --text ... (sidecar warm)",
                "env_toggles": ["PYTHONDONTWRITEBYTECODE", *_ENV_ARTIFACT_KEYS],
                "observed_status": warm_status,
                **summarize_samples(warm_samples),
            },
            "cli_find_related_prior_work_sidecar_absent": {
                "description": "mcp-workbay-handoff find-related-prior-work --text ... (sidecar absent)",
                "env_toggles": [
                    "PYTHONDONTWRITEBYTECODE",
                    *_ENV_ARTIFACT_KEYS,
                    ENV_LOAD_TIMEOUT_SECONDS,
                ],
                "observed_status": absent_status,
                **summarize_samples(absent_samples),
            },
            "cli_find_related_prior_work_sidecar_disabled": {
                "description": "mcp-workbay-handoff find-related-prior-work --text ... (sidecar disabled)",
                "env_toggles": [
                    "PYTHONDONTWRITEBYTECODE",
                    ENV_EMBED_SIDECAR_DISABLED,
                    *_ENV_ARTIFACT_KEYS,
                ],
                "observed_status": disabled_status,
                **summarize_samples(disabled_samples),
            },
        },
        "importtime_top_offenders": importtime_offenders,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay_handoff_mcp.embeddings.latency_bench",
        description="Opt-in PERF-06 latency baseline harness (emits JSON to stdout).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help=f"Number of timed iterations per benchmark (default: {DEFAULT_ITERATIONS}).",
    )
    parser.add_argument(
        "--top-imports",
        type=int,
        default=DEFAULT_TOP_IMPORTS,
        help=f"Number of -X importtime cumulative offenders to report (default: {DEFAULT_TOP_IMPORTS}).",
    )
    parser.add_argument(
        "--task-ref",
        default=DEFAULT_TASK_REF,
        help="Task ref seeded for the state benchmark.",
    )
    parser.add_argument(
        "--query-text",
        default=DEFAULT_QUERY_TEXT,
        help="Free-text query used for the find-related-prior-work benchmarks.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_baseline(
        iterations=args.iterations,
        top_imports=args.top_imports,
        task_ref=args.task_ref,
        query_text=args.query_text,
    )
    sys.stdout.write(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
