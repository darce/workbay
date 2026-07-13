"""``wb <verb>`` dispatcher — idempotent lifecycle one-shots ([RES-01]).

Collapses married multi-step segments into a single human/LLM Bash call
with JSON receipts and named-cause non-zero exits ([OBS-08]).

Delegates to the existing ``workbay_lifecycle`` runner; does not reimplement
handler bodies. ``wb ship`` encodes the merge-before-finish ordering trap
(branch-lifecycle skill step 9 before step 11).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------

_PKG_DIR = Path(__file__).resolve().parent
# .../scripts/workbay/wb → parents: wb, workbay, scripts
_PAYLOAD_SCRIPTS = _PKG_DIR.parent.parent
_DEFAULT_LIFECYCLE = _PAYLOAD_SCRIPTS / "workbay_lifecycle"

# Idempotency stamp root (per-repo). [RES-01] re-run of a completed verb → noop.
_STAMP_REL = Path(".task-state") / "wb-receipts"

VERBS: tuple[str, ...] = (
    "start",
    "status",
    "slice",
    "close",
    "gate",
    "ship",
    "stop",
    "accept",
    "doctor",
)

# Named-cause → suggested next verb (runbook "when things refuse" column).
CAUSE_NEXT_VERB: dict[str, str] = {
    "unknown_verb": "doctor",
    "missing_arg": "status",
    "not_in_git_repo": "doctor",
    "merge_failed": "gate",
    "finish_failed": "ship",
    "step_failed": "status",
    "lifecycle_failed": "doctor",
    "task_required": "start",
    "slice_n_required": "slice",
    "doc_required": "accept",
    "test_cmd_required": "slice",
}

# Exit codes: 0 ok/noop; 2 named-cause refusal; 130 interrupt (lifecycle parity).
_EXIT_OK = 0
_EXIT_REFUSE = 2


# ---------------------------------------------------------------------------
# Receipt helpers
# ---------------------------------------------------------------------------


def _emit(receipt: dict[str, Any]) -> None:
    json.dump(receipt, sys.stdout, sort_keys=False)
    sys.stdout.write("\n")


def _receipt(
    *,
    verb: str,
    ok: bool,
    status: str,
    cause: str | None = None,
    steps: list[dict[str, Any]] | None = None,
    next_verb: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "ok": ok,
        "command": "wb",
        "verb": verb,
        "status": status,
        "cause": cause,
        "steps": list(steps or []),
        "next_verb": next_verb
        if next_verb is not None
        else (CAUSE_NEXT_VERB.get(cause) if cause else None),
    }
    if extra:
        body.update(extra)
    return body


def _fail(
    verb: str,
    cause: str,
    *,
    steps: list[dict[str, Any]] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], int]:
    return (
        _receipt(
            verb=verb,
            ok=False,
            status="error",
            cause=cause,
            steps=steps,
            extra=extra,
        ),
        _EXIT_REFUSE,
    )


def _ok(
    verb: str,
    *,
    status: str = "ok",
    steps: list[dict[str, Any]] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], int]:
    return (
        _receipt(verb=verb, ok=True, status=status, steps=steps, extra=extra),
        _EXIT_OK,
    )


# ---------------------------------------------------------------------------
# Idempotency stamps ([RES-01])
# ---------------------------------------------------------------------------


def _stamp_path(repo: Path, verb: str, key: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in key) or "default"
    return repo / _STAMP_REL / f"{verb}__{safe}.json"


def _read_stamp(repo: Path, verb: str, key: str) -> dict[str, Any] | None:
    path = _stamp_path(repo, verb, key)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _write_stamp(repo: Path, verb: str, key: str, payload: Mapping[str, Any]) -> None:
    path = _stamp_path(repo, verb, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), sort_keys=True) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Lifecycle + git step runners (injectable for tests)
# ---------------------------------------------------------------------------


def lifecycle_runner() -> Path:
    override = os.environ.get("WORKBAY_LIFECYCLE_RUNNER", "").strip()
    if override:
        return Path(override)
    return _DEFAULT_LIFECYCLE


def _run_lifecycle(
    subcommand: str,
    args: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Invoke one lifecycle subcommand; returns the completed process."""
    runner = lifecycle_runner()
    # Directory package (workbay_lifecycle) or a shim script/binary.
    cmd = [sys.executable, str(runner), subcommand, *args]
    child_env = os.environ.copy()
    if env:
        child_env.update(env)
    # Optional test hook: append step name to a log file (ordering probes).
    step_log = child_env.get("WB_STEP_LOG", "").strip()
    if step_log:
        try:
            with open(step_log, "a", encoding="utf-8") as fh:
                fh.write(f"lifecycle:{subcommand}\n")
        except OSError:
            pass
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
        env=child_env,
    )


def _parse_lifecycle_stdout(stdout: str) -> dict[str, Any] | None:
    text = (stdout or "").strip()
    if not text:
        return None
    # Lifecycle emits one JSON receipt line (possibly with leading noise).
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            return data
    return None


def _redact_paths(text: str, repo: Path | None = None) -> str:
    """Redact absolute-path prefixes in receipt text ([OBS-08] hygiene).

    Maps the repo root to ``<repo>`` and ``$HOME`` to ``~`` so receipts do
    not leak operator filesystem layout into logs/transcripts.
    """
    if not text:
        return text
    if repo is not None:
        for candidate in {str(repo), str(repo.resolve())}:
            if candidate and candidate != "/":
                text = text.replace(candidate, "<repo>")
    home = str(Path.home())
    if home and home != "/":
        text = text.replace(home, "~")
    return text


def _step_from_proc(
    name: str,
    proc: subprocess.CompletedProcess[str],
    repo: Path | None = None,
) -> dict[str, Any]:
    receipt = _parse_lifecycle_stdout(proc.stdout)
    return {
        "name": name,
        "ok": proc.returncode == 0 and (receipt is None or receipt.get("ok", True) is not False),
        "exit_code": proc.returncode,
        "receipt": receipt,
        "stderr_tail": _redact_paths((proc.stderr or "")[-400:], repo),
    }


def _repo_root(cwd: Path | None = None) -> Path | None:
    start = cwd or Path.cwd()
    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=str(start),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return Path(proc.stdout.strip())


def _primary_root(repo: Path) -> Path | None:
    """Resolve the primary (non-linked) worktree root for ``repo``.

    ``git rev-parse --git-common-dir`` points at ``<primary>/.git`` from any
    linked worktree; from the primary itself it is the local ``.git``. Returns
    ``None`` when git errors (callers fall back to ``repo``).
    """
    proc = subprocess.run(
        ["git", "rev-parse", "--git-common-dir"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    common = Path((proc.stdout or "").strip())
    if not common.is_absolute():
        common = (repo / common).resolve()
    if common.name == ".git":
        return common.parent
    return None


def _current_branch(repo: Path) -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=str(repo),
        capture_output=True,
        text=True,
        check=False,
    )
    return (proc.stdout or "").strip() if proc.returncode == 0 else ""


def _run_merge_ff(
    repo: Path,
    *,
    branch: str,
    into: str = "main",
) -> dict[str, Any]:
    """Fast-forward ``into`` with ``branch``. Injectable via WB_MERGE_CMD.

    When ``WB_MERGE_CMD`` is set it is run via the shell with env
    ``WB_MERGE_BRANCH`` / ``WB_MERGE_INTO`` / ``WB_REPO``; exit non-zero
    means merge failed (finish must not run).
    """
    step_log = os.environ.get("WB_STEP_LOG", "").strip()
    if step_log:
        try:
            with open(step_log, "a", encoding="utf-8") as fh:
                fh.write("merge\n")
        except OSError:
            pass

    override = os.environ.get("WB_MERGE_CMD", "").strip()
    if override:
        env = os.environ.copy()
        env["WB_MERGE_BRANCH"] = branch
        env["WB_MERGE_INTO"] = into
        env["WB_REPO"] = str(repo)
        proc = subprocess.run(
            override,
            cwd=str(repo),
            shell=True,
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        return {
            "name": "merge",
            "ok": proc.returncode == 0,
            "exit_code": proc.returncode,
            "receipt": {
                "ok": proc.returncode == 0,
                "branch": branch,
                "into": into,
                "mode": "override",
            },
            "stderr_tail": _redact_paths((proc.stderr or "")[-400:], repo),
        }

    # Real path: resolve the PRIMARY checkout explicitly and operate on it
    # with ``git -C <primary-root>`` (S11-A-01). ``wb ship`` is routinely run
    # from a feature worktree; a bare ``git switch <into>`` there would either
    # fail (branch checked out elsewhere) or, worse, flip the feature worktree
    # itself. When the primary root cannot be resolved, refuse with a named
    # cause rather than guessing.
    primary = _primary_root(repo)
    if primary is None:
        error = "primary_worktree_unresolved: git rev-parse --git-common-dir failed"
        return {
            "name": "merge",
            "ok": False,
            "exit_code": 2,
            "receipt": {
                "ok": False,
                "branch": branch,
                "into": into,
                "error": error,
            },
            "stderr_tail": error,
        }
    steps_err: list[str] = []
    for argv in (
        ["git", "-C", str(primary), "switch", into],
        ["git", "-C", str(primary), "merge", "--ff-only", branch],
    ):
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            steps_err.append(
                _redact_paths(
                    f"{' '.join(argv)} -> {proc.returncode}: "
                    f"{(proc.stderr or proc.stdout or '')[:300]}",
                    repo,
                )
            )
            return {
                "name": "merge",
                "ok": False,
                "exit_code": proc.returncode,
                "receipt": {
                    "ok": False,
                    "branch": branch,
                    "into": into,
                    "error": steps_err[-1],
                },
                "stderr_tail": steps_err[-1],
            }
    return {
        "name": "merge",
        "ok": True,
        "exit_code": 0,
        "receipt": {
            "ok": True,
            "branch": branch,
            "into": into,
            "mode": "ff-only",
            "primary_root_resolved": True,
        },
        "stderr_tail": "",
    }


def _run_doc_restore(repo: Path) -> dict[str, Any]:
    """Best-effort post-ship doc restore hook (noop when nothing to restore).

    Injectable via ``WB_DOC_RESTORE_CMD``. Default is a no-op success so ship
    composition stays ordered without inventing destructive checkout logic.
    """
    step_log = os.environ.get("WB_STEP_LOG", "").strip()
    if step_log:
        try:
            with open(step_log, "a", encoding="utf-8") as fh:
                fh.write("doc_restore\n")
        except OSError:
            pass

    override = os.environ.get("WB_DOC_RESTORE_CMD", "").strip()
    if override:
        proc = subprocess.run(
            override,
            cwd=str(repo),
            shell=True,
            capture_output=True,
            text=True,
            check=False,
        )
        return {
            "name": "doc_restore",
            "ok": proc.returncode == 0,
            "exit_code": proc.returncode,
            "receipt": {"ok": proc.returncode == 0, "mode": "override"},
            "stderr_tail": _redact_paths((proc.stderr or "")[-400:], repo),
        }
    return {
        "name": "doc_restore",
        "ok": True,
        "exit_code": 0,
        "receipt": {"ok": True, "status": "noop", "mode": "default"},
        "stderr_tail": "",
    }


# ---------------------------------------------------------------------------
# Verb handlers
# ---------------------------------------------------------------------------


def _verb_start(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    parser = argparse.ArgumentParser(prog="wb start", add_help=True)
    parser.add_argument("task", nargs="?", default="")
    parser.add_argument("--task", dest="task_flag", default="")
    parser.add_argument("--objective", default=os.environ.get("OBJECTIVE", ""))
    parser.add_argument("--plan", default=os.environ.get("PLAN", ""))
    parser.add_argument("--mode", default=os.environ.get("MODE", ""))
    parser.add_argument("--slug", default=os.environ.get("SLUG", ""))
    args, rest = parser.parse_known_args(argv)
    task = (args.task_flag or args.task or os.environ.get("TASK", "")).strip()
    if not task:
        return _fail("start", "task_required")

    stamp_key = task.upper()
    prior = _read_stamp(repo, "start", stamp_key)
    if prior and prior.get("status") == "ok":
        return _ok(
            "start",
            status="noop",
            steps=[{"name": "task-start", "ok": True, "status": "noop"}],
            extra={"task_ref": stamp_key, "prior": prior},
        )

    lc_args = ["--task", task, "--json"]
    if args.objective:
        lc_args.extend(["--objective", args.objective])
    if args.plan:
        lc_args.extend(["--plan", args.plan])
    if args.mode:
        lc_args.extend(["--mode", args.mode])
    if args.slug:
        lc_args.extend(["--slug", args.slug])
    lc_args.extend(rest)

    proc = _run_lifecycle("task-start", lc_args, cwd=repo)
    step = _step_from_proc("task-start", proc, repo)
    if not step["ok"]:
        return _fail("start", "lifecycle_failed", steps=[step], extra={"task_ref": task})
    _write_stamp(
        repo,
        "start",
        stamp_key,
        {"status": "ok", "task_ref": stamp_key, "verb": "start"},
    )
    return _ok("start", steps=[step], extra={"task_ref": task})


def _verb_status(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    # Bounded read: context + status (both lifecycle handlers).
    steps: list[dict[str, Any]] = []
    for name in ("context", "status"):
        proc = _run_lifecycle(name, ["--json", *argv], cwd=repo)
        steps.append(_step_from_proc(name, proc, repo))
    # status is best-effort orientation; ok if either step succeeded.
    ok = any(s["ok"] for s in steps)
    if not ok:
        return _fail("status", "lifecycle_failed", steps=steps)
    return _ok("status", steps=steps)


def _verb_slice(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    parser = argparse.ArgumentParser(prog="wb slice", add_help=True)
    parser.add_argument("n", nargs="?", default="")
    parser.add_argument("--task", default=os.environ.get("TASK", ""))
    parser.add_argument(
        "--test-cmd",
        default=os.environ.get("TEST_CMD", ""),
        dest="test_cmd",
    )
    parser.add_argument("--slug", default=os.environ.get("SLUG", ""))
    args, rest = parser.parse_known_args(argv)
    n = (args.n or "").strip()
    if not n:
        return _fail("slice", "slice_n_required")
    test_cmd = (args.test_cmd or "").strip()
    if not test_cmd:
        return _fail("slice", "test_cmd_required", extra={"slice": n})

    stamp_key = f"{(args.task or 'active').upper()}__{n}"
    prior = _read_stamp(repo, "slice", stamp_key)
    if prior and prior.get("status") == "ok":
        return _ok(
            "slice",
            status="noop",
            steps=[{"name": "slice-start", "ok": True, "status": "noop"}],
            extra={"slice": n, "prior": prior},
        )

    lc_args = ["--test-cmd", test_cmd, "--json"]
    if args.task:
        lc_args.extend(["--task", args.task])
    if args.slug:
        lc_args.extend(["--slug", args.slug])
    else:
        lc_args.extend(["--slug", f"slice-{n}"])
    lc_args.extend(rest)

    proc = _run_lifecycle("slice-start", lc_args, cwd=repo)
    step = _step_from_proc("slice-start", proc, repo)
    # slice-start records RED evidence; a failing TEST_CMD is expected (ok
    # receipt with test_passed=false, possibly exit 1). Success therefore
    # requires a structured receipt that AFFIRMS ok — an absent or non-True
    # ``ok`` key is a failure, never a silent pass (S11-A-02).
    receipt = step.get("receipt")
    if proc.returncode not in (0, 1):
        return _fail("slice", "lifecycle_failed", steps=[step], extra={"slice": n})
    if isinstance(receipt, dict):
        if receipt.get("ok") is not True:
            # ok:false OR missing/unknown ok → failure.
            return _fail("slice", "lifecycle_failed", steps=[step], extra={"slice": n})
    elif proc.returncode != 0:
        # exit 1 without a structured receipt: cannot be verified as RED
        # evidence — treat as failure rather than assuming success.
        return _fail("slice", "lifecycle_failed", steps=[step], extra={"slice": n})
    _write_stamp(
        repo,
        "slice",
        stamp_key,
        {"status": "ok", "slice": n, "verb": "slice"},
    )
    return _ok("slice", steps=[step], extra={"slice": n})


def _verb_close(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    """close_slice mechanics + checklist tick.

    Server-side revision retry (slice-5) lives in handoff; here we compose:
    1. optional handoff close-slice when session/decision flags provided
    2. ``sync-task-plan-checklist --apply`` (checklist tick)
    """
    parser = argparse.ArgumentParser(prog="wb close", add_help=True)
    parser.add_argument("n", nargs="?", default="")
    parser.add_argument("--task", default=os.environ.get("TASK", ""))
    parser.add_argument("--session", default=os.environ.get("WB_CLOSE_SESSION", ""))
    parser.add_argument("--decision", default=os.environ.get("WB_CLOSE_DECISION", ""))
    args, rest = parser.parse_known_args(argv)
    n = (args.n or "").strip()
    if not n:
        return _fail("close", "slice_n_required")

    stamp_key = f"{(args.task or 'active').upper()}__{n}"
    prior = _read_stamp(repo, "close", stamp_key)
    if prior and prior.get("status") == "ok":
        return _ok(
            "close",
            status="noop",
            steps=[{"name": "close", "ok": True, "status": "noop"}],
            extra={"slice": n, "prior": prior},
        )

    steps: list[dict[str, Any]] = []
    # Checklist tick via lifecycle (close_slice itself is MCP; revision retry is server-side).
    lc_args = ["--apply", "--json", "--quiet"]
    if args.task:
        lc_args.extend(["--task", args.task])
    lc_args.extend(rest)
    proc = _run_lifecycle("sync-task-plan-checklist", lc_args, cwd=repo)
    steps.append(_step_from_proc("sync-task-plan-checklist", proc, repo))
    # sync may warn without failing; only hard process failure refuses.
    if proc.returncode not in (0, 1) and not steps[-1]["ok"]:
        return _fail("close", "step_failed", steps=steps, extra={"slice": n})

    _write_stamp(
        repo,
        "close",
        stamp_key,
        {"status": "ok", "slice": n, "verb": "close"},
    )
    return _ok(
        "close",
        steps=steps,
        extra={
            "slice": n,
            "note": "close_slice MCP write remains agent-owned; revision retry is server-side (T8)",
        },
    )


def _verb_gate(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    """Mechanics bracket only — prints steps for the LLM to run review content."""
    del argv, repo  # bracket is pure receipt; no side effects.
    bracket = [
        {
            "name": "review-parallel",
            "role": "llm",
            "command": "make review-run  # or /review-parallel for multi-reviewer",
            "note": "review content is model-owned; mechanics only here",
        },
        {
            "name": "auto-fix",
            "role": "llm",
            "command": "bounded auto-fix on open findings / failing tests",
            "note": "model-owned fix loop",
        },
        {
            "name": "close-check",
            "role": "mechanics",
            "command": "make handoff-close-check",
            "lifecycle": "handoff-close-check",
        },
    ]
    # Optionally run the mechanics close-check when WB_GATE_RUN_CLOSE_CHECK=1.
    steps: list[dict[str, Any]] = list(bracket)
    if os.environ.get("WB_GATE_RUN_CLOSE_CHECK", "").strip() in {"1", "true", "yes"}:
        proc = _run_lifecycle("handoff-close-check", ["--json"], cwd=Path.cwd())
        steps.append(_step_from_proc("handoff-close-check", proc, Path.cwd()))
    return _ok(
        "gate",
        steps=steps,
        extra={
            "bracket": ["review-parallel", "auto-fix", "close-check"],
            "model_needed": True,
        },
    )


def _verb_ship(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    """merge ff→main THEN task-finish (+ lane reap) THEN doc restore.

    Ordering is load-bearing: finish must not run when merge fails
    (branch-lifecycle rationalization trap).
    """
    parser = argparse.ArgumentParser(prog="wb ship", add_help=True)
    parser.add_argument("--task", default=os.environ.get("TASK", ""))
    parser.add_argument(
        "--branch",
        default=os.environ.get("WB_SHIP_BRANCH", ""),
        help="Feature branch to merge (default: current branch)",
    )
    parser.add_argument(
        "--into",
        default=os.environ.get("WB_SHIP_INTO", "main"),
        help="Integration branch (default: main)",
    )
    args, rest = parser.parse_known_args(argv)
    branch = (args.branch or _current_branch(repo) or "").strip()
    task = (args.task or "").strip()
    # S11-A-04: never stamp under a bare literal key — an unresolved
    # task+branch would make every future ship in this repo a false noop.
    # Without a discriminator, skip idempotency stamping entirely.
    stamp_key = (task or branch).upper() if (task or branch) else None

    prior = _read_stamp(repo, "ship", stamp_key) if stamp_key else None
    if prior and prior.get("status") == "ok":
        return _ok(
            "ship",
            status="noop",
            steps=[{"name": "ship", "ok": True, "status": "noop"}],
            extra={"prior": prior, "task_ref": task or None, "branch": branch},
        )

    steps: list[dict[str, Any]] = []

    # 1. MERGE FIRST (ordering trap)
    merge_step = _run_merge_ff(repo, branch=branch, into=args.into)
    steps.append(merge_step)
    if not merge_step["ok"]:
        # CRITICAL: do not invoke task-finish when merge fails.
        return _fail(
            "ship",
            "merge_failed",
            steps=steps,
            extra={
                "task_ref": task or None,
                "branch": branch,
                "finish_invoked": False,
            },
        )

    # 2. task-finish (includes T11 lane reap)
    lc_args = ["--json", *rest]
    if task:
        lc_args = ["--task", task, *lc_args]
    proc = _run_lifecycle("task-finish", lc_args, cwd=repo)
    finish_step = _step_from_proc("task-finish", proc, repo)
    steps.append(finish_step)
    if not finish_step["ok"]:
        return _fail(
            "ship",
            "finish_failed",
            steps=steps,
            extra={
                "task_ref": task or None,
                "branch": branch,
                "finish_invoked": True,
                "merge_ok": True,
            },
        )

    # 3. doc restore (best-effort; after finish). Policy: advisory — a
    # failure does not flip the ship receipt to error, but it must be
    # visible: the step stays ok:false and a named warning is surfaced
    # (S11-A-05), never silently ignored.
    restore_step = _run_doc_restore(repo)
    steps.append(restore_step)
    warnings: list[str] = []
    if not restore_step["ok"]:
        warnings.append(
            "doc_restore_failed: post-ship doc restore exited "
            f"{restore_step.get('exit_code')} (advisory step; merge and "
            "task-finish already completed)"
        )

    if stamp_key:
        _write_stamp(
            repo,
            "ship",
            stamp_key,
            {
                "status": "ok",
                "verb": "ship",
                "branch": branch,
                "task_ref": task or None,
            },
        )
    extra: dict[str, Any] = {
        "task_ref": task or None,
        "branch": branch,
        "finish_invoked": True,
        "merge_ok": True,
        "order": ["merge", "task-finish", "doc_restore"],
    }
    if warnings:
        extra["warnings"] = warnings
    if not stamp_key:
        extra["stamp_skipped"] = "no task/branch discriminator resolved"
    return _ok("ship", steps=steps, extra=extra)


def _verb_stop(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    """Pause/abandon + teardown, no merge."""
    parser = argparse.ArgumentParser(prog="wb stop", add_help=True)
    parser.add_argument("--task", default=os.environ.get("TASK", ""))
    parser.add_argument(
        "--status",
        default=os.environ.get("WB_STOP_STATUS", "paused"),
        choices=("paused", "abandoned"),
    )
    args, rest = parser.parse_known_args(argv)
    task = (args.task or "").strip()
    stamp_key = (task or "active").upper()
    prior = _read_stamp(repo, "stop", stamp_key)
    if prior and prior.get("status") == "ok":
        return _ok(
            "stop",
            status="noop",
            steps=[{"name": "stop", "ok": True, "status": "noop"}],
            extra={"prior": prior},
        )

    steps: list[dict[str, Any]] = []
    # Status flip routed through the shared lifecycle runner (S11-A-03) so
    # WORKBAY_LIFECYCLE_RUNNER injection / WB_STEP_LOG probes cover it like
    # every other step; the ``handoff-set`` subcommand shells to the handoff
    # CLI on the real path.
    if task:
        step_log = os.environ.get("WB_STEP_LOG", "").strip()
        if step_log:
            try:
                with open(step_log, "a", encoding="utf-8") as fh:
                    fh.write("set_status\n")
            except OSError:
                pass
        # Allow tests to skip real handoff via WB_STOP_SKIP_SET=1
        if os.environ.get("WB_STOP_SKIP_SET", "").strip() in {"1", "true", "yes"}:
            steps.append(
                {
                    "name": "set_status",
                    "ok": True,
                    "exit_code": 0,
                    "receipt": {"ok": True, "status": args.status, "skipped": True},
                }
            )
        else:
            proc = _run_lifecycle(
                "handoff-set",
                ["--task-ref", task, "--status", args.status, "--json"],
                cwd=repo,
            )
            set_step = _step_from_proc("set_status", proc, repo)
            set_step["receipt"] = set_step.get("receipt") or {
                "ok": proc.returncode == 0,
                "status": args.status,
            }
            steps.append(set_step)

    # Teardown helper: task-reap (no merge).
    lc_args = ["--json", *rest]
    if task:
        lc_args = ["--task", task, *lc_args]
    proc = _run_lifecycle("task-reap", lc_args, cwd=repo)
    steps.append(_step_from_proc("task-reap", proc, repo))

    _write_stamp(
        repo,
        "stop",
        stamp_key,
        {"status": "ok", "verb": "stop", "task_ref": task or None},
    )
    return _ok(
        "stop",
        steps=steps,
        extra={"task_ref": task or None, "stop_status": args.status, "merged": False},
    )


def _verb_accept(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    parser = argparse.ArgumentParser(prog="wb accept", add_help=True)
    parser.add_argument("doc", nargs="?", default="")
    parser.add_argument("--task", default=os.environ.get("TASK", ""))
    parser.add_argument("--plan", default=os.environ.get("PLAN", ""))
    parser.add_argument(
        "--local",
        action="store_true",
        default=os.environ.get("WB_ACCEPT_LOCAL", "").strip() in {"1", "true", "yes"},
    )
    args, rest = parser.parse_known_args(argv)
    doc = (args.doc or args.plan or "").strip()
    if not doc and not args.task:
        return _fail("accept", "doc_required")

    stamp_key = (doc or args.task or "accept").replace("/", "_")
    prior = _read_stamp(repo, "accept", stamp_key)
    if prior and prior.get("status") == "ok":
        return _ok(
            "accept",
            status="noop",
            steps=[{"name": "plan-accept", "ok": True, "status": "noop"}],
            extra={"prior": prior, "doc": doc or None},
        )

    lc_args = ["--json"]
    if args.task:
        lc_args.extend(["--task", args.task])
    if doc:
        lc_args.extend(["--plan", doc])
    if args.local:
        lc_args.append("--local")
    lc_args.extend(rest)

    proc = _run_lifecycle("plan-accept", lc_args, cwd=repo)
    step = _step_from_proc("plan-accept", proc, repo)
    if not step["ok"]:
        return _fail("accept", "lifecycle_failed", steps=[step], extra={"doc": doc or None})
    _write_stamp(
        repo,
        "accept",
        stamp_key,
        {"status": "ok", "verb": "accept", "doc": doc or None},
    )
    return _ok("accept", steps=[step], extra={"doc": doc or None})


def _verb_doctor(argv: list[str], repo: Path) -> tuple[dict[str, Any], int]:
    proc = _run_lifecycle("doctor", ["--json", *argv], cwd=repo)
    step = _step_from_proc("doctor", proc, repo)
    if not step["ok"]:
        return _fail("doctor", "lifecycle_failed", steps=[step])
    return _ok("doctor", steps=[step])


_HANDLERS: dict[str, Callable[[list[str], Path], tuple[dict[str, Any], int]]] = {
    "start": _verb_start,
    "status": _verb_status,
    "slice": _verb_slice,
    "close": _verb_close,
    "gate": _verb_gate,
    "ship": _verb_ship,
    "stop": _verb_stop,
    "accept": _verb_accept,
    "doctor": _verb_doctor,
}


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


def run(argv: Sequence[str] | None = None) -> int:
    """Dispatch one ``wb`` verb; print a single JSON receipt line."""
    raw = list(argv) if argv is not None else sys.argv[1:]
    if not raw:
        receipt, code = _fail(
            "",
            "missing_arg",
            extra={"usage": "wb <verb> [args]", "verbs": list(VERBS)},
        )
        # empty verb key for schema consistency
        receipt["verb"] = ""
        _emit(receipt)
        return code

    verb = raw[0].strip().lower()
    rest = raw[1:]
    if verb in {"-h", "--help", "help"}:
        _emit(
            _receipt(
                verb="help",
                ok=True,
                status="ok",
                extra={"verbs": list(VERBS), "usage": "wb <verb> [args]"},
            )
        )
        return _EXIT_OK

    if verb not in _HANDLERS:
        receipt, code = _fail(
            verb,
            "unknown_verb",
            extra={"verbs": list(VERBS)},
        )
        _emit(receipt)
        return code

    repo = _repo_root()
    if repo is None:
        receipt, code = _fail(verb, "not_in_git_repo")
        _emit(receipt)
        return code

    try:
        receipt, code = _HANDLERS[verb](rest, repo)
    except KeyboardInterrupt:
        sys.stderr.write(
            "\nwb: interrupted (exit 130); verify task/handoff state before retrying.\n"
        )
        return 130
    _emit(receipt)
    return code


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
