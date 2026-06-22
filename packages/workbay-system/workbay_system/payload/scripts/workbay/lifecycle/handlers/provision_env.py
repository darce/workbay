"""Mutating ``provision-env`` subcommand (internal).

A reusable entry point that provisions a worktree-root ``.venv`` for an
arbitrary worktree path. ``task-start`` provisions the root venv inline,
but orchestrator fresh-lane creation and the ``worktree-lane`` shell asset
spawn worktrees *outside* ``make task-start`` and still want the same
local-test environment contract. They invoke this subcommand so a bare
``pytest`` from the new worktree root resolves locally instead of via the
pyenv shim.

Emits a compact JSON receipt. Exit code is 0 on success (including the
no-package no-op) and 2 on a hard provisioning failure, so shell callers
can branch on the exit code while still treating an absent entry point as
recoverable on their side.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import uv_provisioning

from . import _common


def _emit_error(reason: str, *, worktree: str = "") -> int:
    receipt: dict[str, Any] = {
        "ok": False,
        "command": "provision-env",
        "worktree_path": worktree,
        "root_venv_path": None,
        "created": False,
        "installed": [],
        "skipped": [],
        "failure_reason": reason,
        "error": reason,
    }
    _common.emit(receipt)
    return 2


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle provision-env", add_help=True)
    parser.add_argument("--worktree", dest="worktree", default="")
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    raw = (args.worktree or "").strip()
    if not raw:
        return _emit_error("worktree_required")
    worktree = Path(raw).expanduser()
    if not worktree.is_dir():
        return _emit_error("worktree_not_found", worktree=str(worktree))

    preflight = uv_provisioning.uv_preflight()
    if not preflight.ok:
        return _emit_error(
            f"uv_preflight_failed: {preflight.error}", worktree=str(worktree)
        )

    # Manual recovery re-provisions in place: pass clear=True so an existing
    # (possibly partial) .venv is replaced rather than aborting `uv venv`.
    # This is the command the doctor venv facet points operators at when
    # root_venv_pytest_present is False, so it must succeed when .venv exists.
    result = uv_provisioning.provision_root_venv(
        worktree,
        override=uv_provisioning.sync_packages_override(),
        clear=True,
        stream=sys.stderr,
    )
    installed = [i.package for i in result.installs if i.installed]
    skipped = [i.package for i in result.installs if i.skipped]
    receipt = {
        "ok": result.ok,
        "command": "provision-env",
        "worktree_path": str(worktree),
        "root_venv_path": str(result.venv_dir) if result.created else None,
        "created": result.created,
        "python_path": str(result.python_path) if result.created else None,
        "installed": installed,
        "skipped": skipped,
        "failure_reason": result.failure_reason,
    }

    if not args.emit_json:
        sys.stderr.write(
            f"provision-env: worktree={worktree} created={result.created} "
            f"ok={result.ok} installed={len(installed)} skipped={len(skipped)}\n"
        )

    _common.emit(receipt)
    return 0 if result.ok else 2
