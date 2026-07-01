"""Interpreter version skew probes (implementation note D4)."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

PROBE_PACKAGES = ("workbay-protocol", "mcp-workbay-handoff")


@dataclass(frozen=True)
class InterpreterProbe:
    label: str
    python: str
    versions: dict[str, str]
    error: str = ""


def _package_versions(python: str, packages: tuple[str, ...] = PROBE_PACKAGES) -> tuple[dict[str, str], str]:
    script = (
        "import importlib.metadata as m\n"
        "pkgs = " + repr(packages) + "\n"
        "out = {}\n"
        "for name in pkgs:\n"
        "    try:\n"
        "        out[name] = m.version(name)\n"
        "    except Exception as exc:\n"
        "        out[name] = f'<missing:{exc.__class__.__name__}>'\n"
        "import json; print(json.dumps(out))\n"
    )
    try:
        proc = subprocess.run(
            [python, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {}, f"{type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        return {}, (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
    try:
        import json

        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        return {}, f"json decode failed: {exc}"
    if not isinstance(data, dict):
        return {}, "probe returned non-object json"
    return {str(k): str(v) for k, v in data.items()}, ""


def _venv_python(repo_root: Path) -> str:
    for rel in (("bin", "python"), ("Scripts", "python.exe")):
        candidate = repo_root / ".venv" / rel[0] / rel[1]
        if candidate.is_file():
            return str(candidate)
    return ""


def collect_interpreter_probes(repo_root: Path) -> list[InterpreterProbe]:
    probes: list[InterpreterProbe] = []
    ambient = sys.executable
    versions, error = _package_versions(ambient)
    probes.append(
        InterpreterProbe(label="ambient", python=ambient, versions=versions, error=error)
    )
    venv_py = _venv_python(repo_root)
    if venv_py:
        versions, error = _package_versions(venv_py)
        probes.append(
            InterpreterProbe(label="workspace_venv", python=venv_py, versions=versions, error=error)
        )
    return probes


def find_skew(probes: list[InterpreterProbe]) -> list[str]:
    """Return human-readable skew lines for any package with >1 distinct version."""
    by_pkg: dict[str, dict[str, str]] = {}
    for probe in probes:
        if probe.error:
            continue
        for pkg, ver in probe.versions.items():
            if ver.startswith("<missing"):
                continue
            by_pkg.setdefault(pkg, {})[probe.label] = ver
    findings: list[str] = []
    for pkg, label_versions in sorted(by_pkg.items()):
        distinct = set(label_versions.values())
        if len(distinct) > 1:
            findings.append(
                f"{pkg}: " + ", ".join(f"{label}={ver}" for label, ver in sorted(label_versions.items()))
            )
    return findings


def warn_skew_if_needed(repo_root: Path, *, stream: object | None = None) -> list[str]:
    """Emit a one-line stderr warning when skew is detected (D4 visibility arm)."""
    probes = collect_interpreter_probes(repo_root)
    skew = find_skew(probes)
    if skew and stream is not None:
        stream.write(  # type: ignore[attr-defined]
            "workbay: interpreter version skew detected — "
            + "; ".join(skew)
            + "\n"
        )
    return skew


def ci_gate_enabled() -> bool:
    return os.environ.get("WORKBAY_CI_INTERPRETER_GATE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
