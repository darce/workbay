"""Synchronous offload pass engine (internal S2).

Carves the worker daemon's single-pass internals into a synchronous,
outcome-typed engine: one call validates the lane is actionable, runs the
bounded execute→review→fix loop, enforces a commit gate between execute and
review (review never sees a dirty tree), and returns a typed outcome enum —
never a bare ok/exit-0 the caller has to guess about.

Contract highlights (task plan `internal`):
- Mandatory positive ``token_budget`` and ``timeout_seconds``; the MCP layer
  refuses un-bounded calls before any spend and the engine re-asserts.
- Budget enforcement is fail-closed and three-point: pre-turn admission,
  backend turn bound where supported, post-turn reconciliation. A budgeted
  turn that reports no token usage is a typed ``error``, never
  warn-and-continue.
- ``timeout`` / ``error`` outcomes never re-execute inside the engine;
  recovery is a new explicit dispatch (idempotent on ``dispatch_id``).
- Dirty execute output is unconditionally checkpointed
  (``wip(offload): <lane_id> checkpoint <n>`` with an ``Offload-Backend``
  trailer); ``uncommitted_work`` is returned only when the checkpoint itself
  fails.
- Pass state persists in ``<state_dir>/offload-pass-<pass_id>.json`` so a
  disconnected client can recover the outcome via ``await_offload_pass``.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

PASS_OUTCOMES = frozenset(
    {
        "handoff_ready",
        "needs_guidance",
        "no_actionable_work",
        "uncommitted_work",
        "token_budget_exceeded",
        "timeout",
        "error",
        "still_running",
        "lane_not_found",
        "self_verify_failed",
        "composer_violation_quarantined",
        "checkpoint",
    }
)

_ENGINE_GIT_IDENTITY = ["-c", "user.name=workbay-offload-engine", "-c", "user.email=offload-engine@workbay.local"]


def _worker_daemon_module() -> Any:
    """Resolve the worker_daemon module with bare-name-first semantics.

    Mirrors ``api._import_orchestration_module`` so tests that patch the
    module the API layer resolves patch the same object this engine calls.
    """
    module = sys.modules.get("worker_daemon")
    if module is not None:
        return module
    from workbay_orchestrator_mcp.orchestration import worker_daemon  # noqa: PLC0415

    return worker_daemon


# ---------------------------------------------------------------------------
# Pass state persistence (disconnect recovery)
# ---------------------------------------------------------------------------


def _pass_state_path(state_dir: Path, pass_id: str) -> Path:
    return Path(state_dir) / f"offload-pass-{pass_id}.json"


def write_pass_state(state_dir: Path, pass_id: str, payload: dict[str, Any]) -> None:
    path = _pass_state_path(state_dir, pass_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic publish: a concurrent await_offload_pass reader long-polls this file
    # ~1s apart and must never observe a half-written document (JSONDecodeError ->
    # None -> spurious "unknown pass"). Write to a temp sibling then os.replace().
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(payload, sort_keys=False), encoding="utf-8")
    os.replace(tmp, path)


def read_pass_state(state_dir: Path, pass_id: str) -> dict[str, Any] | None:
    path = _pass_state_path(state_dir, pass_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


# ---------------------------------------------------------------------------
# Commit gate
# ---------------------------------------------------------------------------


def _worktree_dirty(worktree_path: Path) -> bool:
    result = subprocess.run(
        ["git", "-C", str(worktree_path), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    # Fail closed: a non-zero git status (corrupt repo, missing binary, permission
    # error) must NOT be read as "clean" — that would silently skip the commit gate
    # and let review run against an unknown tree state. Raise so the caller maps it
    # to a typed error / uncommitted_work rather than proceeding as if clean.
    if result.returncode != 0:
        raise RuntimeError(
            f"git status failed in {worktree_path} (rc={result.returncode}): {result.stderr.strip() or 'no stderr'}"
        )
    return bool(result.stdout.strip())


def _checkpoint_commit(
    worktree_path: Path,
    lane_id: str,
    checkpoint_number: int,
    backend: str,
    model: str | None,
) -> str | None:
    """Create the engine-identity checkpoint commit; return its sha or None."""
    add = subprocess.run(
        ["git", "-C", str(worktree_path), "add", "-A"],
        capture_output=True,
        text=True,
        check=False,
    )
    if add.returncode != 0:
        return None
    message = (
        f"wip(offload): {lane_id} checkpoint {checkpoint_number}\n\nOffload-Backend: {backend}/{model or 'default'}"
    )
    commit = subprocess.run(
        ["git", "-C", str(worktree_path), *_ENGINE_GIT_IDENTITY, "commit", "-m", message],
        capture_output=True,
        text=True,
        check=False,
    )
    if commit.returncode != 0:
        return None
    sha = subprocess.run(
        ["git", "-C", str(worktree_path), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return sha.stdout.strip() or None


def _checkpoint_if_dirty(
    worktree_path: Path,
    lane_id: str,
    checkpoints: list[str],
    backend: str,
    model: str | None,
) -> bool:
    """Checkpoint any dirty tree. Returns False only when the checkpoint failed."""
    if not _worktree_dirty(worktree_path):
        return True
    sha = _checkpoint_commit(worktree_path, lane_id, len(checkpoints) + 1, backend, model)
    if sha is None:
        return False
    checkpoints.append(sha)
    return True


# ---------------------------------------------------------------------------
# Worker end-state contract (PR-09/PR-10): evidence + engine-recorded closure
# ---------------------------------------------------------------------------


def _git_stdout(worktree_path: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(worktree_path), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def _handoff_db_path() -> Path:
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

    return Path(get_runtime_config().db_path)


def _open_dispatch_id(task_ref: str, lane_id: str) -> str | None:
    with sqlite3.connect(_handoff_db_path()) as conn:
        row = conn.execute(
            """
            SELECT dispatch_id FROM lane_messages
            WHERE task_ref = ? AND lane_id = ? AND direction = 'orchestrator_to_worker'
              AND status = 'open' AND dispatch_id IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id),
        ).fetchone()
    if row is None:
        return None
    dispatch_id = str(row[0] or "").strip()
    return dispatch_id or None


def _max_worker_report_id(task_ref: str, lane_id: str) -> int:
    with sqlite3.connect(_handoff_db_path()) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM worker_reports WHERE task_ref = ? AND lane_id = ?",
            (task_ref, lane_id),
        ).fetchone()
    return int(row[0])


def _fresh_worker_report(task_ref: str, lane_id: str, baseline_report_id: int) -> dict[str, Any] | None:
    with sqlite3.connect(_handoff_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM worker_reports
            WHERE task_ref = ? AND lane_id = ? AND id > ?
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id, baseline_report_id),
        ).fetchone()
    return dict(row) if row is not None else None


_UNPARSEABLE_SUMMARY = "grok produced no parseable JSON result"
_UNPARSEABLE_BLOCKER_PREFIX = "grok output unparseable"
_COMPOSER_VIOLATION_SUMMARY = "grok Composer-only guarantee not confirmed"
_COMPOSER_VIOLATION_BLOCKER_MARKERS = (
    "grok-build authored",
    "Composer-only guarantee violated",
    "Composer-only guarantee not confirmed",
)


def _max_verified_test_id(task_ref: str) -> int:
    with sqlite3.connect(_handoff_db_path()) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM verified_tests WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    return int(row[0])


def _parse_worker_report_blockers(report: dict[str, Any]) -> list[str]:
    try:
        blockers = json.loads(report.get("blockers_json") or "[]")
    except json.JSONDecodeError:
        blockers = []
    return [str(blocker) for blocker in blockers if isinstance(blocker, str)]


def _is_composer_violation_handoff_report(report: dict[str, Any]) -> bool:
    summary = str(report.get("summary") or "")
    if _COMPOSER_VIOLATION_SUMMARY in summary:
        return True
    for blocker in _parse_worker_report_blockers(report):
        if any(marker in blocker for marker in _COMPOSER_VIOLATION_BLOCKER_MARKERS):
            return True
    return False


def _is_unparseable_handoff_report(report: dict[str, Any]) -> bool:
    if _is_composer_violation_handoff_report(report):
        return False
    summary = str(report.get("summary") or "")
    if summary != _UNPARSEABLE_SUMMARY:
        return False
    return any(_UNPARSEABLE_BLOCKER_PREFIX in blocker for blocker in _parse_worker_report_blockers(report))


def _latest_worker_report(task_ref: str, lane_id: str) -> dict[str, Any] | None:
    from workbay_orchestrator_mcp.lanes import worker_reports  # noqa: PLC0415

    payload = worker_reports(
        operation="list",
        task_ref=task_ref,
        lane_id=lane_id,
        limit=1,
        fields="id,session,summary,blockers_json,created_at",
    )
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return None
    reports = payload.get("reports")
    if not isinstance(reports, list) or not reports:
        return None
    report = reports[0]
    return report if isinstance(report, dict) else None


def _commits_since_start(worktree_path: Path, start_head: str) -> list[str]:
    if not start_head:
        return []
    output = _git_stdout(worktree_path, "rev-list", f"{start_head}..HEAD")
    if not output:
        return []
    return [sha.strip() for sha in output.splitlines() if sha.strip()]


def _passing_test_since_baseline(task_ref: str, lane_id: str, baseline_test_id: int) -> dict[str, Any] | None:
    with sqlite3.connect(_handoff_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM verified_tests
            WHERE task_ref = ? AND (lane_id = ? OR lane_id IS NULL) AND id > ? AND passed = 1
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id, baseline_test_id),
        ).fetchone()
    return dict(row) if row is not None else None


def _tail_text(text: str, *, limit: int = 500) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[-limit:]


def _malformed_raw_output_tail(task_ref: str, lane_id: str) -> str:
    with sqlite3.connect(_handoff_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT message FROM lane_messages
            WHERE task_ref = ? AND lane_id = ? AND direction = 'worker_to_orchestrator'
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id),
        ).fetchone()
    if row is None:
        return ""
    return _tail_text(str(row["message"] or ""))


def _evaluate_malformed_handoff_salvage(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
    start_head: str,
    baseline_test_id: int,
    baseline_report_id: int,
) -> dict[str, Any] | None:
    report = _fresh_worker_report(task_ref, lane_id, baseline_report_id)
    if report is None or not _is_unparseable_handoff_report(report):
        return None
    commits = _commits_since_start(worktree_path, start_head)
    if not commits:
        return None
    passing_test = _passing_test_since_baseline(task_ref, lane_id, baseline_test_id)
    if passing_test is None:
        return None
    return {
        "commit_shas": commits,
        "passing_test": {
            "id": passing_test.get("id"),
            "command": passing_test.get("command"),
            "verified_at": passing_test.get("verified_at"),
        },
        "worker_report_id": report.get("id"),
        "raw_output_tail": _malformed_raw_output_tail(task_ref, lane_id),
    }


def _record_salvage_audit_decision(
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    evidence: dict[str, Any],
) -> None:
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    passing_test = evidence.get("passing_test") if isinstance(evidence.get("passing_test"), dict) else {}
    test_id = passing_test.get("id", "unknown")
    commits = evidence.get("commit_shas") if isinstance(evidence.get("commit_shas"), list) else []
    raw_tail = str(evidence.get("raw_output_tail") or "")
    decision_id = f"offload_salvage_candidate_{task_ref}_{lane_id}_{test_id}"
    rationale = (
        "## Salvage candidate (malformed handoff)\n"
        "Worker produced committed, test-green work but the final grok turn was unparseable.\n\n"
        "## Evidence\n"
        f"- Commits: {', '.join(str(commit) for commit in commits)}\n"
        f"- Passing test: #{passing_test.get('id')} `{passing_test.get('command')}` "
        f"at {passing_test.get('verified_at')}\n"
        f"- Worker report: #{evidence.get('worker_report_id')}\n\n"
        "## Malformed raw output tail\n"
        f"```\n{raw_tail}\n```\n"
    )
    record_decision(
        session=session,
        decision=decision_id,
        rationale=rationale,
        task_ref=task_ref,
    )


def _record_worker_closure(
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    backend: str,
    model: str | None,
    worktree_path: Path,
    start_head: str,
    baseline_report_id: int,
) -> "tuple[dict[str, Any] | None, str | None]":
    """Verify commit + fresh report + test evidence, then record test_result rows
    and the slice-complete decision with the backend's actor identity.

    Returns ``(closure_info, None)`` in three shapes:
    - ``recorded=True``: evidence + slice-complete decision were written.
    - ``recorded=False`` with a ``reason``: the worker did real work but did NOT
      mark it mergeable (merge_ready=false / blocked / has blockers), so the slice
      is deliberately left open for the review gate — NOT an error.
    Returns ``(None, error_reason)`` when the worker end-state contract is violated
    (no commit, no fresh report, no evidence, write failure) — no closure recorded.
    """
    if not start_head:
        return None, ("worker end-state violated: could not resolve the lane branch HEAD before the pass (fail-closed)")
    head = _git_stdout(worktree_path, "rev-parse", "HEAD")
    if not head or head == start_head:
        return None, ("worker end-state violated: no commit landed on the lane branch during this pass")
    # The landed HEAD must descend from the pre-pass HEAD; a rewound or unrelated
    # HEAD (force-reset, wrong worktree) is not evidence the worker advanced the lane.
    ancestry = subprocess.run(
        ["git", "-C", str(worktree_path), "merge-base", "--is-ancestor", start_head, head],
        capture_output=True,
        text=True,
        check=False,
    )
    if ancestry.returncode != 0:
        return None, ("worker end-state violated: lane HEAD does not descend from the pre-pass HEAD")
    report = _fresh_worker_report(task_ref, lane_id, baseline_report_id)
    if report is None:
        return None, (
            "worker end-state violated: missing/stale worker report — no report was "
            "recorded during this pass; no slice closure recorded"
        )
    try:
        test_commands = json.loads(report.get("test_commands_json") or "[]")
    except json.JSONDecodeError:
        test_commands = []
    if not test_commands:
        return None, ("worker end-state violated: worker report carries no test evidence; no slice closure recorded")

    # Merge-readiness gate: a slice-complete decision asserts the work is ready for
    # the review gate. A worker that finished but reported merge_ready=false (or a
    # blocked/failed outcome, or open blockers) must NOT auto-close the slice.
    merge_ready = bool(report.get("merge_ready"))
    report_outcome = str(report.get("outcome") or "").strip().lower()
    try:
        blockers = json.loads(report.get("blockers_json") or "[]")
    except json.JSONDecodeError:
        blockers = []
    if not merge_ready or report_outcome in {"failed", "exhausted", "stopped"} or blockers:
        return {
            "recorded": False,
            "reason": (
                f"worker report not mergeable (merge_ready={merge_ready}, "
                f"outcome={report_outcome or 'unset'}, blockers={len(blockers) if isinstance(blockers, list) else 0}); "
                "slice left open for the review gate"
            ),
            "merge_ready": merge_ready,
            "commit_sha": head,
            "worker_report_id": report.get("id"),
        }, None

    from workbay_handoff_mcp.core import close_slice as handoff_close_slice  # noqa: PLC0415
    from workbay_handoff_mcp.decisions import record_test_result  # noqa: PLC0415
    from workbay_handoff_mcp.shared_write_context import build_write_actor  # noqa: PLC0415

    author_tag = re.sub(r"[^a-z]", "", str(backend).split("-")[0].lower()) or "worker"
    branch = _git_stdout(worktree_path, "rev-parse", "--abbrev-ref", "HEAD") or None
    if branch == "HEAD":  # detached HEAD has no branch name
        branch = None
    backend_model = f"{backend}/{model or 'default'}"
    # Attribute the write to the offload backend engine (not the model identity):
    # build_write_actor would otherwise derive agent from model and drop the
    # backend marker. lane_id is carried for provenance; branch 'HEAD' -> None.
    actor = build_write_actor(
        agent=f"{author_tag}-offload-engine",
        branch=branch,
        commit_sha=head,
        lane_id=lane_id,
    )
    for command in test_commands:
        evidence = record_test_result(
            session=session,
            command=str(command),
            passed=merge_ready,
            result=f"Recorded by the offload engine ({backend_model}) from worker report #{report.get('id')} at {head}.",
            actor=actor,
            task_ref=task_ref,
        )
        if isinstance(evidence, dict) and evidence.get("ok") is False:
            evidence_err = evidence.get("error") or (evidence.get("data") or {}).get("error")
            return None, (
                f"worker evidence write failed for '{command}': {evidence_err or 'record_test_result rejected'}"
            )

    try:
        changed_files_raw = json.loads(report.get("changed_files_json") or "[]")
    except json.JSONDecodeError:
        changed_files_raw = []
    changed_files = [str(path) for path in changed_files_raw if isinstance(path, str)] or None

    with sqlite3.connect(_handoff_db_path()) as conn:
        revision_row = conn.execute(
            "SELECT revision FROM handoff_state WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    expected_revision = int(revision_row[0]) if revision_row is not None else None

    # Bind the decision id to the landed commit so a second pass on the same lane
    # (new commit) writes a distinct decision instead of hitting close_slice's
    # idempotent envelope and silently reporting a false success.
    slug = re.sub(r"\W", "_", f"offload_{lane_id}_{head[:12]}")
    decision_id = f"{author_tag}_slice_complete_{task_ref}_{slug}"
    summary = str(report.get("summary") or "Offloaded slice completed by the backend worker.")
    rationale = (
        f"## Changes\n{summary}\n\n"
        f"## Verification\nWorker test commands recorded as fresh test_result rows at {head} "
        f"by {backend_model}: {', '.join(str(command) for command in test_commands)}. merge_ready={merge_ready}.\n\n"
        "## Schema / Contract Changes\nNone recorded by the offload engine; see the lane diff at the review gate.\n\n"
        "## Open Threads\nLane handoff diff awaits the orchestrator review gate (no auto-merge)."
    )
    closure = handoff_close_slice(
        session=session,
        decision=decision_id,
        rationale=rationale,
        actor=actor,
        expected_revision=expected_revision,
        task_ref=task_ref,
        changed_files=changed_files,
    )
    closure_data = closure.get("data", {}) if isinstance(closure.get("data"), dict) else {}
    if not closure.get("ok"):
        return None, (
            "worker evidence verified but the engine slice closure write failed: "
            f"{closure_data.get('error') or closure_data.get('state_error') or 'unknown close_slice failure'}"
        )
    # close_slice's idempotent envelope returns ok=true but decision_recorded=false
    # when the same decision id already exists. Report that as NOT recorded rather
    # than a false success, so a repeated pass on one commit cannot masquerade as a
    # fresh closure.
    decision_recorded = closure.get("decision_recorded")
    if decision_recorded is None:
        decision_recorded = closure_data.get("decision_recorded")
    idempotent = bool(closure.get("idempotent") or closure_data.get("idempotent"))
    if idempotent or decision_recorded is False:
        return {
            "recorded": False,
            "reason": "idempotent close_slice: a slice-complete decision already exists for this commit; no new decision written",
            "decision": decision_id,
            "commit_sha": head,
            "worker_report_id": report.get("id"),
            "merge_ready": merge_ready,
        }, None
    return {
        "recorded": True,
        "decision": decision_id,
        "commit_sha": head,
        "worker_report_id": report.get("id"),
        "test_commands": [str(command) for command in test_commands],
        "merge_ready": merge_ready,
        "changed_files": changed_files or [],
    }, None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def run_offload_pass_engine(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    backend: str,
    model: str | None = None,
    reasoning_effort: str = "inherit",
    token_budget: int,
    timeout_seconds: float,
    max_review_cycles: int = 2,
    turn_timeout_seconds: float | None = None,
    grok_max_turns: int | None = None,
    session_mode: str = "fresh_turn",
    dry_run: bool = False,
    pass_id: str | None = None,
    state_dir: Path | None = None,
    test_cmd: str | None = None,
) -> dict[str, Any]:
    # bool is a subclass of int; token_budget=True would pass isinstance(_, int) and
    # run a pass with an effective budget of 1 token. Reject it explicitly.
    if isinstance(token_budget, bool) or not isinstance(token_budget, int) or token_budget <= 0:
        raise ValueError("token_budget must be a positive integer (mandatory, fail-closed).")
    if timeout_seconds is None or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive (mandatory bounded wait).")
    if turn_timeout_seconds is not None and turn_timeout_seconds > timeout_seconds:
        raise ValueError("turn_timeout_seconds must not exceed the pass timeout_seconds.")
    if isinstance(max_review_cycles, bool) or not isinstance(max_review_cycles, int) or max_review_cycles < 1:
        raise ValueError("max_review_cycles must be a positive integer (>=1).")

    wd = _worker_daemon_module()
    resolved_pass_id = pass_id or str(uuid.uuid4())
    # Writer (this engine) and reader (await_offload_pass) must share one state_dir;
    # deriving it from orchestrator_root here while the reader uses RuntimeConfig
    # .state_dir is a split-brain (recovery reads a directory nothing was written to).
    resolved_state_dir = Path(state_dir) if state_dir is not None else Path(orchestrator_root) / ".task-state"
    started = time.monotonic()
    deadline = started + float(timeout_seconds)
    checkpoints: list[str] = []
    run_ctx: Any = None

    # Token-governance mode, resolved once (internal).
    # A backend that emits token telemetry is governed by token_budget; one that
    # does not (grok-cli) is governed by the deadline + turn bounds, and the
    # downgrade is surfaced in every result payload so it is never silent.
    from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
        backend_supports_token_telemetry,
    )

    token_telemetry_supported = backend_supports_token_telemetry(backend)
    token_governance: dict[str, Any] = {
        "mode": "token_budget" if token_telemetry_supported else "degraded_turn_time",
        "enforced_by": "token_budget" if token_telemetry_supported else "turn_time_bounds",
        "token_telemetry": token_telemetry_supported,
    }

    def _payload(
        outcome: str,
        *,
        run_ctx: Any = None,
        error: str | None = None,
        slice_closure: dict[str, Any] | None = None,
        self_verify: dict[str, Any] | None = None,
        composer_violation: dict[str, Any] | None = None,
        continuation_dispatch_id: str | None = None,
    ) -> dict[str, Any]:
        cumulative = int(getattr(run_ctx, "cumulative_tokens", 0) or 0)
        effective_effort = getattr(run_ctx, "execution_effective_effort", None)
        result: dict[str, Any] = {
            "outcome": outcome,
            "pass_id": resolved_pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "backend": getattr(run_ctx, "backend", None) or backend,
            "model": getattr(run_ctx, "model", None) or model,
            "reasoning_effort": (
                effective_effort if effective_effort and effective_effort != "inherit" else reasoning_effort
            ),
            "tokens": {"cumulative_total": cumulative, "token_budget": token_budget},
            "token_governance": token_governance,
            "checkpoint_commits": list(checkpoints),
            "slice_closure": slice_closure if slice_closure is not None else {"recorded": False},
            "wall_seconds": round(time.monotonic() - started, 2),
            "retry_policy": "never_in_engine; recover via a new idempotent dispatch (dispatch_id)",
        }
        if error is not None:
            result["error"] = error
        if self_verify is not None:
            result["self_verify"] = self_verify
        if composer_violation is not None:
            result["composer_violation"] = composer_violation
        if continuation_dispatch_id is not None:
            result["continuation_dispatch_id"] = continuation_dispatch_id
        return result

    def _finish(result: dict[str, Any]) -> dict[str, Any]:
        write_pass_state(
            resolved_state_dir,
            resolved_pass_id,
            {"status": "done", "task_ref": task_ref, "lane_id": lane_id, "result": result},
        )
        return result

    def _execute_pass() -> dict[str, Any]:
        nonlocal run_ctx
        lane_state = wd.poll_lane_state(
            orchestrator_root=Path(orchestrator_root),
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=Path(worktree_path),
        )
        if lane_state != "actionable":
            return _payload("no_actionable_work", error=f"lane state: {lane_state}; record a brief first")

        # Worker end-state baselines: closure is recorded only from a commit and a
        # worker report produced DURING this pass (freshness gate, PR-10).
        start_head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
        baseline_report_id = _max_worker_report_id(task_ref, lane_id)
        baseline_test_id = _max_verified_test_id(task_ref)

        grok_timeout = int(turn_timeout_seconds) if turn_timeout_seconds and backend == "grok-cli" else None
        resolved_test_cmd = str(test_cmd or "").strip() or None
        config = wd.WorkerConfig(
            orchestrator_root=Path(orchestrator_root),
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            worktree_path=Path(worktree_path),
            max_review_cycles=max_review_cycles,
            single_pass=True,
            backend=backend,
            session_mode=session_mode,
            reasoning_effort=reasoning_effort,
            model=model,
            grok_timeout=grok_timeout,
            grok_max_turns=grok_max_turns,
            dry_run=dry_run,
            token_budget=token_budget,
            test_cmd=resolved_test_cmd,
        )
        config = wd._resolve_grok_cycle_bounds(config)
        run_ctx, _ = wd._setup_worker_run(config)

        outcome: str | None = None
        error_reason: str | None = None
        self_verify_result: dict[str, Any] | None = None
        composer_violation_result: dict[str, Any] | None = None
        continuation_dispatch_id: str | None = None
        for cycle in range(max_review_cycles):
            run_ctx.cycle = cycle
            # Pre-turn admission (fail-closed point 1 of 3).
            if run_ctx.cumulative_tokens >= token_budget:
                outcome = "token_budget_exceeded"
                break
            if time.monotonic() >= deadline:
                outcome = "timeout"
                break
            tokens_before = run_ctx.cumulative_tokens
            if not wd._execute_phase(run_ctx):
                if getattr(run_ctx, "execute_stop_reason", None) == "max_turns" and _worktree_dirty(
                    Path(worktree_path)
                ):
                    if config.test_cmd and not dry_run:
                        self_verify_result = wd._self_verify_phase(run_ctx)
                        if not self_verify_result.get("passed"):
                            wd._record_self_verify_blocker(
                                orchestrator_root=Path(orchestrator_root),
                                task_ref=task_ref,
                                lane_id=lane_id,
                                test_cmd=str(self_verify_result.get("command") or config.test_cmd),
                                output_tail=str(self_verify_result.get("output_tail") or ""),
                            )
                            outcome = "self_verify_failed"
                            error_reason = (
                                f"max-turns checkpoint blocked: self-verify failed on "
                                f"`{self_verify_result.get('command')}`"
                            )
                            break
                    if _checkpoint_if_dirty(Path(worktree_path), lane_id, checkpoints, run_ctx.backend, run_ctx.model):
                        outcome = "checkpoint"
                        continuation_dispatch_id = _open_dispatch_id(task_ref, lane_id)
                        error_reason = (
                            "execute stopped on max turns with a self-verified checkpoint preserved; "
                            "re-dispatch with the same dispatch_id to continue"
                        )
                        break
                outcome = "error"
                error_reason = "execute phase failed; see worker status/log for the failure stage"
                break
            # Post-turn reconciliation (point 3): a budgeted turn with no token
            # telemetry. This is a contract violation ONLY for a backend that
            # declares it emits token usage — for such a backend a zero delta
            # means the governor ran blind, so error out. A backend that cannot
            # self-meter (e.g. grok-cli) is not violating any contract; its
            # budget is enforced by the turn-count + deadline bounds in this same
            # loop, so a zero token delta is expected and must NOT abort a
            # working turn (internal / TB-001; unifies
            # this with worker_daemon._accumulate_run_ctx_tokens' soft-warn).
            if not dry_run and run_ctx.cumulative_tokens == tokens_before and token_telemetry_supported:
                outcome = "error"
                error_reason = "token telemetry missing on a budgeted turn (telemetry contract violation)"
                break
            # Worker self-verify gate (backend-neutral): TEST_CMD must pass before commit.
            if not dry_run and config.test_cmd:
                self_verify_result = wd._self_verify_phase(run_ctx)
                if not self_verify_result.get("passed"):
                    wd._record_self_verify_blocker(
                        orchestrator_root=Path(orchestrator_root),
                        task_ref=task_ref,
                        lane_id=lane_id,
                        test_cmd=str(self_verify_result.get("command") or config.test_cmd),
                        output_tail=str(self_verify_result.get("output_tail") or ""),
                    )
                    outcome = "self_verify_failed"
                    error_reason = (
                        f"worker self-verify failed on `{self_verify_result.get('command')}` "
                        f"(exit {self_verify_result.get('exit_code')})"
                    )
                    break
            # Commit gate: review never sees a dirty tree.
            if not _checkpoint_if_dirty(Path(worktree_path), lane_id, checkpoints, run_ctx.backend, run_ctx.model):
                outcome = "uncommitted_work"
                error_reason = "execute left the worktree dirty and the checkpoint commit failed"
                break
            if time.monotonic() >= deadline:
                outcome = "timeout"
                break
            if run_ctx.cumulative_tokens >= token_budget:
                outcome = "token_budget_exceeded"
                break
            try:
                review_output = wd._review_phase(run_ctx)
            except StopIteration as stop:
                # _review_phase raises only after submitting a BLOCKED handoff
                # (needs_guidance / scope_violation, outcome="failed"). A clean
                # exit code means the SUBMISSION succeeded, not that the work is
                # merge-ready — the lane is waiting on the orchestrator.
                blocked_kind = str(stop.args[0]) if stop.args else "needs_guidance"
                if run_ctx.handoff_exit == 0:
                    violation = wd._composer_violation_info(run_ctx)
                    if violation is not None and checkpoints:
                        composer_violation_result = violation
                        outcome = "composer_violation_quarantined"
                        error_reason = (
                            "Composer-only violation after a self-verified checkpoint; "
                            f"branch={violation.get('branch')}; commit preserved for orchestrator review"
                        )
                    else:
                        outcome = "needs_guidance"
                        error_reason = f"worker handed a blocked result back for guidance ({blocked_kind})"
                else:
                    outcome = "error"
                    error_reason = "review phase ended the pass without a clean handoff"
                break
            except (RuntimeError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
                outcome = "error"
                error_reason = f"review phase failed: {exc}"
                break
            if review_output.get("converged", False):
                check_ok = wd._verify_phase(run_ctx, review_output)
                wd._handoff_phase(run_ctx, check_ok)
                # handoff_exit==0 only means the submission landed. _handoff_phase
                # submits a needs_guidance handoff when verification failed, so a
                # clean exit with check_ok False is a blocked result, NOT ready.
                if run_ctx.handoff_exit != 0:
                    outcome = "error"
                    error_reason = "final handoff failed after a converged review"
                elif not check_ok:
                    outcome = "needs_guidance"
                    error_reason = "lane verification failed after review convergence"
                else:
                    outcome = "handoff_ready"
                break
        else:
            outcome = "error"
            error_reason = f"review did not converge after {max_review_cycles} cycles"

        # Salvage checkpoint: preserve partial work for timeout / budget / error so
        # it is referenced in the outcome rather than lost; a failed salvage downgrades
        # to uncommitted_work. Capture the budget trip BEFORE any downgrade so the
        # budget-exceeded handler still fires (the downgrade would flip the guard).
        budget_tripped = outcome == "token_budget_exceeded"
        if outcome in ("timeout", "token_budget_exceeded", "error"):
            try:
                salvaged = _checkpoint_if_dirty(
                    Path(worktree_path), lane_id, checkpoints, run_ctx.backend, run_ctx.model
                )
            except RuntimeError as exc:
                salvaged = False
                error_reason = f"{outcome}: checkpoint salvage failed: {exc}"
            if not salvaged:
                error_reason = error_reason or f"{outcome}: partial work could not be checkpointed"
                outcome = "uncommitted_work"
        if budget_tripped:
            wd._handle_token_budget_exceeded(run_ctx)

        slice_closure: dict[str, Any] | None = None
        if outcome == "handoff_ready":
            slice_closure, closure_error = _record_worker_closure(
                task_ref=task_ref,
                lane_id=lane_id,
                session=session,
                backend=run_ctx.backend,
                model=run_ctx.model,
                worktree_path=Path(worktree_path),
                start_head=start_head,
                baseline_report_id=baseline_report_id,
            )
            if closure_error is not None:
                outcome = "error"
                error_reason = closure_error

        salvage_candidate: dict[str, Any] | None = None
        if outcome == "needs_guidance":
            salvage_candidate = _evaluate_malformed_handoff_salvage(
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=Path(worktree_path),
                start_head=start_head,
                baseline_test_id=baseline_test_id,
                baseline_report_id=baseline_report_id,
            )
            if salvage_candidate is not None:
                _record_salvage_audit_decision(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    session=session,
                    evidence=salvage_candidate,
                )

        payload = _payload(
            outcome,
            run_ctx=run_ctx,
            error=error_reason,
            slice_closure=slice_closure,
            self_verify=self_verify_result,
            composer_violation=composer_violation_result,
            continuation_dispatch_id=continuation_dispatch_id,
        )
        if salvage_candidate is not None:
            payload["salvage_candidate"] = salvage_candidate
        return payload

    write_pass_state(
        resolved_state_dir,
        resolved_pass_id,
        {"status": "running", "task_ref": task_ref, "lane_id": lane_id},
    )
    # Per-lane exclusive lock: the engine drives _setup_worker_run/_execute_phase
    # directly, bypassing the daemon main()'s WorkerLock. Without this, a concurrent
    # daemon single-pass and an engine pass could act on the same lane at once.
    lock = wd.WorkerLock(lane_id, resolved_state_dir)
    if not lock.acquire():
        return _finish(
            _payload(
                "no_actionable_work",
                error=f"lane '{lane_id}' is locked by another worker/daemon; pass not started",
            )
        )
    try:
        result = _execute_pass()
    except Exception as exc:  # noqa: BLE001 - a crash must publish terminal state, never leave the pass 'running'
        result = _payload("error", run_ctx=run_ctx, error=f"offload pass crashed: {type(exc).__name__}: {exc}")
    finally:
        lock.release()
    return _finish(result)
