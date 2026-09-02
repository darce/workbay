#!/usr/bin/env python3
"""Replay the documented consumer install recipe against an export tree.

Given a public export tree (or mirror clone), creates a scratch venv and
installs every exported member as a local-path requirement WITHOUT
``--no-sources``. Post-S1 (member ``{ workspace = true }`` sources stripped
at export), this must resolve end-to-end. A bare single-package install is
NOT sufficient (S0 baseline): members are not on any index, so the gate
replays the FULL recipe (anchor + every exported member).

Exit 0 on success; non-zero with the resolver output on failure.
Runnable standalone for CI (``make check-consumer-install OUT=...``).
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Cap resolver output so --json excerpts stay bounded.
_RESOLVER_EXCERPT_CHARS = 4000
_UV_TIMEOUT_S = 600


def discover_member_paths(export_tree: Path) -> list[Path]:
    """Return sorted ``packages/<member>`` dirs, each of which MUST carry a
    pyproject.toml.

    Discovery is filesystem-based (not the private release manifest) so the
    gate works on a public export / mirror clone that has no monorepo root.
    A ``packages/<dir>`` without a pyproject.toml is a hard failure: silently
    skipping it would shrink the gate's coverage with no signal (an export
    that lost a member manifest must not pass). The discovered member list is
    always echoed to stderr so every run shows exactly what the gate covered.
    """
    packages_root = export_tree / "packages"
    if not packages_root.is_dir():
        return []
    members: list[Path] = []
    for child in sorted(packages_root.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "pyproject.toml").is_file():
            raise RuntimeError(
                f"packages/{child.name} has no pyproject.toml — the export "
                "tree is missing a member manifest (refusing to silently "
                "skip it)"
            )
        members.append(child.resolve())
    sys.stderr.write(
        f"[check-consumer-install] discovered members ({len(members)}): "
        + ", ".join(path.name for path in members)
        + "\n"
    )
    return members


def build_pip_install_argv(
    *,
    members: list[Path],
    python: Path,
) -> list[str]:
    """``uv pip install`` argv for the full local-path consumer recipe.

    Intentionally omits ``--no-sources``: post-S1 exports must parse without
    that flag. Every member is supplied as a local path so resolution does not
    consult an index for sibling dist names.
    """
    return [
        "uv",
        "pip",
        "install",
        "--python",
        str(python),
        *[str(path) for path in members],
    ]


# Markers that distinguish an infrastructure (network/index) failure in uv's
# output from a genuine resolution regression in the export tree itself.
_NETWORK_FAILURE_MARKERS = (
    "error sending request",
    "could not connect",
    "connection refused",
    "network",
    "timed out",
    "failed to fetch",
)


def classify_failure_output(output: str) -> str:
    """Prefix output that looks like a network/index failure, verbatim otherwise.

    A red gate caused by an unreachable index is an infrastructure problem,
    not evidence the export regressed; label it so operators (and the
    release-public report) do not misread it. The exit code stays non-zero
    either way — the gate still fails closed.
    """
    lowered = output.lower()
    if any(marker in lowered for marker in _NETWORK_FAILURE_MARKERS):
        return (
            "gate infrastructure failure (network/index), not necessarily an "
            "export regression\n" + output
        )
    return output


def _which_uv() -> str:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError(
            "uv not found on PATH; install uv to run the consumer-install gate"
        )
    return uv


def run_consumer_install(
    export_tree: Path,
    *,
    timeout_s: int = _UV_TIMEOUT_S,
) -> tuple[int, str]:
    """Create a scratch venv, install all members, return (rc, combined output).

    The tempdir (venv + any uv scratch) is always cleaned up. Deterministic
    for a given export tree content (no floating pins of its own).
    """
    export_tree = export_tree.resolve()
    if not export_tree.is_dir():
        return 2, f"export tree is not a directory: {export_tree}\n"

    members = discover_member_paths(export_tree)
    if not members:
        return (
            2,
            f"no packages/*/pyproject.toml found under export tree: {export_tree}\n",
        )

    uv = _which_uv()
    tmp = tempfile.mkdtemp(prefix="workbay-consumer-install-")
    try:
        venv_dir = Path(tmp) / "venv"
        venv_proc = subprocess.run(
            [uv, "venv", str(venv_dir)],
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env={**os.environ},
        )
        if venv_proc.returncode != 0:
            combined = (venv_proc.stdout or "") + (venv_proc.stderr or "")
            return venv_proc.returncode, classify_failure_output(combined)

        python = venv_dir / "bin" / "python"
        if not python.is_file():
            # Windows-style layout fallback (unlikely in this repo's CI).
            python = venv_dir / "Scripts" / "python.exe"
        argv = build_pip_install_argv(members=members, python=python)
        # Replace leading "uv" with the resolved binary.
        argv[0] = uv
        install_proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            cwd=str(export_tree),
            env={**os.environ},
        )
        combined = (install_proc.stdout or "") + (install_proc.stderr or "")
        if install_proc.returncode != 0:
            return install_proc.returncode, classify_failure_output(combined)
        return install_proc.returncode, combined
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        err = (exc.stderr or "") if isinstance(exc.stderr, str) else ""
        return 1, classify_failure_output(
            f"consumer-install gate timed out after {timeout_s}s\n{out}{err}"
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def excerpt(output: str, *, limit: int = _RESOLVER_EXCERPT_CHARS) -> str:
    """Bounded excerpt of resolver output for --json / report surfaces."""
    text = output.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 20].rstrip() + "\n... (truncated)\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "export_tree",
        type=Path,
        help="Path to a public export tree (or mirror clone) containing packages/*",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        rc, output = run_consumer_install(args.export_tree)
    except RuntimeError as exc:
        sys.stderr.write(f"[check-consumer-install] {exc}\n")
        return 2

    if rc != 0:
        sys.stderr.write(
            "[check-consumer-install] FAIL: consumer install recipe did not "
            "resolve against the export tree (no --no-sources).\n"
        )
        if output:
            sys.stderr.write(output)
            if not output.endswith("\n"):
                sys.stderr.write("\n")
        return rc if rc != 0 else 1

    sys.stderr.write(
        "[check-consumer-install] pass: full local-path recipe resolved "
        f"({args.export_tree})\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
