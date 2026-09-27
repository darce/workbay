"""Mutating ``slice-start`` subcommand (internal).

The RED-evidence measurement point: derives the active task ref from
the current git context, runs the supplied ``TEST_CMD``, and records
the test_result with the current HEAD so the handoff DB has a verifiable
RED-then-GREEN trail. This first sub-slice (4.1) covers the happy
synced path for both passing and failing test commands.

The measurement is gated on the install's ``execution_mode``
(GATE-REMOTE-ROUTING-01): under ``remote_only`` this handler refuses with a
typed ``remote_required`` outcome instead of measuring on the host. See
``_execution_mode`` below for why the refusal is unconditional there.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import resolver
import projection
import uv_provisioning

from . import _common


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _emit_error(
    reason: str,
    *,
    task_ref: str | None = None,
    branch: str = "",
    outcome: str | None = None,
    extra: dict[str, Any] | None = None,
) -> int:
    """Emit the typed-failure receipt and return the handler's failure code.

    ``outcome`` and ``extra`` are additive: existing callers pass neither and
    get the byte-identical receipt they always did. They exist so a policy
    refusal can name itself (``outcome="remote_required"``) and carry the
    evidence-shaped keys consumers read, without growing a second emit path
    that could drift from this one.
    """
    receipt: dict[str, Any] = {
        "ok": False,
        "command": "slice-start",
        "task_ref": task_ref,
        "branch": branch,
        "worktree_path": "",
        "head": "",
        "handoff_projection": "error",
        "events": [],
        "slice_slug": None,
        "test_command": "",
        "test_passed": False,
        "red_evidence_sha": "",
        "error": reason,
    }
    if outcome is not None:
        receipt["outcome"] = outcome
    if extra:
        receipt.update(extra)
    _common.emit(receipt)
    return 2


# GATE-REMOTE-ROUTING-01. Mirrors the orchestrator's refusal outcome
# (``offload_profiles.resolve_offload_backend_for_execution_mode``) so callers
# branch on one spelling across both layers.
_REMOTE_REQUIRED_OUTCOME = "remote_required"


def _execution_mode(repo: Path) -> str:
    """Read the install-time offload execution policy for ``repo``.

    Deferred import ([REF-19]): ``workbay_protocol.bootstrap.load_execution_mode``
    is the single sanctioned reader for ``execution_mode`` — it already
    normalizes a missing manifest, unreadable JSON, an absent field and any
    unknown value down to ``local_ok``, so this must not re-derive that rule.

    Every failure degrades to ``local_ok``, the pre-flag default: a consumer
    bootstrap without ``workbay_protocol`` on ``sys.path`` keeps running
    slice-start exactly as before. Degrading the other way would fail closed on
    an unreadable ledger and break every local install.
    """
    try:
        from workbay_protocol.bootstrap import load_execution_mode  # noqa: PLC0415
    except Exception:  # noqa: BLE001 - capability readers never raise
        return "local_ok"
    try:
        return str(load_execution_mode(repo))
    except Exception:  # noqa: BLE001 - never fail the host command on a read
        return "local_ok"


def _remote_required_refusal(repo: Path) -> str:
    """Explain why a ``remote_only`` install cannot measure the slice here.

    slice-start has no routing primitive at the lifecycle layer, and the
    refusal says so rather than implying a transient fault. The only path that
    reaches the VM is the orchestrator's agent-turn dispatcher
    (``RemoteExecAdapter.execute``), which needs an agent prompt, a JSON output
    schema, a clean worktree, an SSH turn and a patch-apply-plus-commit; it
    carries TEST_CMD only as a post-commit self-verify *inside* that turn, and
    never as a synchronous "run this command, return the exit code".
    ``workbay_protocol.remote_probe.probe_remote_gate`` is a fixed
    ``grok --version`` liveness check, not a command runner. So under
    ``remote_only`` the measurement is unconditionally unroutable from here.
    """
    return (
        f"{_REMOTE_REQUIRED_OUTCOME}: bootstrap ledger execution_mode="
        f"remote_only at {repo}/.workbay-bootstrap.json requires the slice "
        "TEST_CMD to be measured on the remote gate, but slice-start has no "
        "remote routing primitive at the lifecycle layer. Dispatch the slice "
        "through /wb-offload (run_offload_pass / dispatch_lane_work carry "
        "TEST_CMD as an off-box self-verify) to measure remotely, or as a last "
        "resort run `workbay repair --no-remote` to restore local_ok. Refusing "
        "rather than running TEST_CMD on this host and recording it as remote "
        "evidence"
    )


def _run_test_cmd(
    repo: Path, test_cmd: str, env: dict[str, str] | None = None
) -> tuple[bool, int]:
    """Run ``test_cmd`` from ``repo`` via the shell. Returns (passed, exit_code).

    ``env`` overrides the child environment when provided (internal: used to
    prepend the worktree-root ``.venv/bin`` so a bare ``pytest`` resolves
    locally instead of via the pyenv shim). ``None`` inherits the parent
    environment unchanged.
    """
    proc = subprocess.run(
        test_cmd,
        cwd=str(repo),
        shell=True,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    return proc.returncode == 0, proc.returncode


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle slice-start", add_help=True)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--test-cmd", dest="test_cmd", default="")
    parser.add_argument("--slug", dest="slug", default=None)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    test_cmd = args.test_cmd.strip()
    if not test_cmd:
        return _emit_error("test_cmd_required")

    repo = resolver.repo_root()
    if repo is None:
        return _emit_error("not_in_git_repo")

    branch = resolver.current_branch(repo) or ""
    head = resolver.head_sha(repo) or ""
    derived_task_ref = resolver.derive_task_ref(
        branch, known_task_refs=_common._live_task_refs(repo)
    )
    task_ref = (args.task or "").strip().upper() or derived_task_ref
    if not task_ref:
        return _emit_error(
            "task_ref_required", branch=branch
        )

    # GATE-REMOTE-ROUTING-01: honor the install's execution policy BEFORE any
    # provisioning or measurement. Running TEST_CMD here under ``remote_only``
    # would record a test_result attesting to a host run the operator believes
    # was remote — the evidence would not be what it claims to be. Refusing
    # returns before ``project_test_result``, so nothing reaches the handoff
    # DB: this is the same distinction ``test_setup_debt`` draws below —
    # "tooling could not run a measurement" is not "the test failed" — applied
    # to a policy refusal instead of a missing executable.
    execution_mode = _execution_mode(repo)
    if execution_mode == "remote_only":
        return _emit_error(
            _remote_required_refusal(repo),
            task_ref=task_ref,
            branch=branch,
            outcome=_REMOTE_REQUIRED_OUTCOME,
            extra={
                "execution_mode": execution_mode,
                "worktree_path": str(repo),
                "head": head,
                "test_command": test_cmd,
                "test_setup_debt": False,
                "handoff_projection": "skipped",
                # No measurement happened, so no phase and no evidence sha in
                # either direction. ``red_evidence_sha`` is already "" above.
                "green_evidence_sha": "",
                "evidence_phase": None,
            },
        )

    # internal: defensive ``uv sync --extra dev`` so a forgotten
    # mid-task dependency add does not cause a confusing test failure.
    # Cheap on no-op. Preflight failure is non-fatal here — the operator
    # has presumably already provisioned via ``task-start``; we still
    # surface the warning so they can fix it before close-check.
    # internal: refresh package sync AND root ``.venv`` provisioning so
    # TEST_CMD executes against the worktree-local environment. ``test_env``
    # prepends ``<worktree>/.venv/bin`` to PATH; it stays None (parent env
    # inherited) when there is no root venv to use, preserving prior behavior.
    test_env: dict[str, str] | None = None
    preflight = uv_provisioning.uv_preflight()
    if preflight.ok:
        root_venv = uv_provisioning.provision_worktree_env(
            repo,
            override=uv_provisioning.sync_packages_override(),
            stream=sys.stderr,
        )
        if not root_venv.ok:
            # Provisioning is defensive here — surface a test_setup_debt-style
            # warning rather than silently letting a bare pytest fall through
            # to the pyenv shim.
            sys.stderr.write(
                "slice-start: root venv unavailable — "
                f"{root_venv.failure_reason}; bare pytest in TEST_CMD may "
                "resolve via pyenv\n"
            )
        elif root_venv.created and (root_venv.venv_dir / "bin").is_dir():
            test_env = uv_provisioning.root_venv_env(repo)
    else:
        sys.stderr.write(f"slice-start: uv preflight skipped — {preflight.error}\n")

    passed, exit_code = _run_test_cmd(repo, test_cmd, env=test_env)
    # POSIX shells exit 127 when the requested executable cannot be
    # located. Surface this as test_setup_debt so consumers do not
    # mistake "tooling missing" for "test ran and produced RED
    # evidence" — the latter is a real measurement, the former just
    # means slice-start could not run a measurement at all.
    test_setup_debt = exit_code == 127

    session = (
        f"claude_slice_started_{task_ref.replace('-', '_').lower()}_{_utc_stamp()}"
    )
    status = projection.project_test_result(
        repo,
        session=session,
        command=test_cmd,
        passed=passed,
        exit_code=exit_code,
        task_ref=task_ref,
    )

    events = ["slice_started"]
    events.append("test_setup_debt" if test_setup_debt else "test_recorded")

    # RED-vs-GREEN classification: a passing TEST_CMD is GREEN evidence
    # at the current HEAD; a failing TEST_CMD is RED. test_setup_debt
    # means no real measurement happened, so the phase is None and
    # consumers must not classify the run either way.
    if test_setup_debt:
        evidence_phase: str | None = None
        green_evidence_sha = ""
    elif passed:
        evidence_phase = "green"
        green_evidence_sha = head
    else:
        evidence_phase = "red"
        green_evidence_sha = ""

    receipt = {
        "ok": True,
        "command": "slice-start",
        "task_ref": task_ref,
        "branch": branch,
        "worktree_path": str(repo),
        "head": head,
        "handoff_projection": status,
        "events": events,
        "slice_slug": args.slug,
        "test_command": test_cmd,
        "test_passed": passed,
        "test_setup_debt": test_setup_debt,
        "red_evidence_sha": head,
        "green_evidence_sha": green_evidence_sha,
        "evidence_phase": evidence_phase,
        # Where the measurement ran, so the receipt is self-describing about
        # the provenance of its own evidence (GATE-REMOTE-ROUTING-01). Always
        # ``local_ok`` on this path — ``remote_only`` refused above.
        "execution_mode": execution_mode,
    }

    if not args.emit_json:
        sys.stderr.write(
            f"slice-start: task_ref={task_ref} branch={branch} "
            f"head={head[:12]} test_cmd={shlex.quote(test_cmd)} "
            f"passed={passed} projection={status}\n"
        )

    _common.emit(receipt)
    return 0
