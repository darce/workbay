"""Remote-exec backend adapter (implementation note).

Ships each grok worker turn to the remote OCI VM (``WORKBAY_REMOTE_GATE_HOST``)
via ``scripts/remote_agent.sh`` and lands the resulting commit on the LOCAL lane.
Turn shaping + reasoning-effort resolution are delegated to :class:`GrokCliAdapter`
(the local grok port); only the transport is overridden.

Fetch-back is **patch-based, not ``git fetch``**: the VM sandbox is remote-severed
(0 remotes) by design, so ``remote_agent.sh`` returns a ``git format-patch`` on
``--out`` and grok's structured result JSON on ``--result-out``. This adapter
``git apply --index``-es the patch and commits it locally with the offload-engine
identity, so ``offload_pass._commits_since_start`` sees a real local commit and
``_worktree_dirty`` reports clean afterward (no spurious ``_checkpoint_if_dirty``).
No grok/LLM authorship lands in git history — the local commit is engine-authored.

MVP session mode is fresh_turn only; ``shared_lane`` continuity across remote turns
is a documented non-goal (harden-later).
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult
from ..grok_lane_config import DEFAULT_GROK_MODEL
from ..grok_lane_config import ENGINE_GIT_IDENTITY as _ENGINE_GIT_IDENTITY
from .grok_cli import (
    _GROK_BUILD_RE,
    GrokCliAdapter,
    _build_grok_prompt,
    _detect_grok_build_contamination,
    _extract_grok_payload,
    _loads_dict,
    _parse_envelope,
    _tail_text,
    _text_result_dicts,
    _worktree_branch,
)

#: Cap on remote-controlled --result-out file size before parse [RES-05].
_RESULT_FILE_MAX_BYTES = 5 * 1024 * 1024

#: remote_agent.sh --effort accepts these (NOT grok-cli's "max"); anything else is
#: dropped from argv so the script's own validation never fail-closes the turn.
_REMOTE_EFFORTS = frozenset({"low", "medium", "high", "xhigh"})

#: remote_agent.sh exit codes (see the script header).
_RC_PATCH = 0
_RC_HARD_FAIL = 1  # security posture tripwire or fatal in-sandbox setup (not transport)
_RC_USAGE = 2
_RC_GROK_FAILED = 3
_RC_NO_CHANGES = 4
_RC_ADMISSION_DEFERRED = 75  # VM admission (mem floor, lane cap, residual timeout, or same-branch lane lock); retryable (S3b/S5)
_RC_HOST_UNCONFIGURED = 78


def _default_remote_runner(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> "subprocess.CompletedProcess[str]":
    """Run ``remote_agent.sh`` bounded. Injectable seam: tests pass a fake runner
    (via the ``remote_runner`` execute kwarg) that writes canned --out/--result-out
    files and returns an exit code, so no SSH/VM is touched.

    ``stdin=DEVNULL`` is load-bearing (implementation note): the orchestrator MCP server's
    own stdin is the JSON-RPC stdio pipe — a non-tty, never-EOF fd. Without this,
    ``remote_agent.sh`` inherits it and its step-1 ``git push`` (git's default ssh)
    blocks reading it forever, consuming the whole timeout budget with no VM
    sandbox and 0 grok output (root cause, decision 4134). A ``/dev/null`` (EOF)
    stdin makes the identical dispatch complete normally.
    """
    return subprocess.run(  # noqa: S603
        cmd,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _resolve_script(worktree_path: Path) -> Path:
    script = Path(worktree_path) / "scripts" / "remote_agent.sh"
    if not script.is_file():
        raise RuntimeError(
            f"grok-remote transport missing: {script} not found (needs scripts/remote_agent.sh in the lane worktree)."
        )
    return script


def _local_dirty(worktree_path: Path) -> bool:
    """True when the lane worktree has uncommitted changes. Fail-closed: a non-zero
    git status raises (never read as clean)."""
    res = subprocess.run(
        ["git", "-C", str(worktree_path), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"git status failed in {worktree_path} (rc={res.returncode}): {res.stderr.strip()[-200:] or 'no stderr'}"
        )
    return bool(res.stdout.strip())


def _committed_files(worktree_path: Path) -> list[str]:
    """Paths touched by HEAD (the just-applied engine commit) — the authoritative
    changed-file list, preferred over grok's self-report."""
    res = subprocess.run(
        ["git", "-C", str(worktree_path), "show", "--name-only", "--format=", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        return []
    return [line.strip() for line in res.stdout.splitlines() if line.strip()]


def _apply_and_commit(worktree_path: Path, patch_file: Path, *, lane_id: str, summary: str) -> None:
    """``git apply --index`` the returned patch, then ONE engine-identity commit.

    Collapses grok's sandbox commits into a single local commit authored by the
    offload engine (no grok/LLM authorship in git history).

    Fail-closed (RES-13 crumple zone / RES-04 cleanup-on-throw): ``git apply`` is
    atomic — a failed apply applies nothing, leaving the lane untouched. But if the
    apply succeeds and the *commit* then fails (e.g. a repo pre-commit hook rejects
    it), the tree would be left staged/dirty, breaking the offload pass's
    clean-HEAD invariant (``_worktree_dirty`` / ``_commits_since_start``). So on
    commit failure we reverse the just-applied patch (``git apply -R --index`` — the
    exact inverse of a clean apply) before raising, restoring the pre-turn HEAD.
    Either way the caller sees a raise with no partial commit; recovery is a re-run.
    """
    apply = subprocess.run(
        ["git", "-C", str(worktree_path), "apply", "--index", str(patch_file)],
        capture_output=True,
        text=True,
        check=False,
    )
    if apply.returncode != 0:
        raise RuntimeError(
            f"git apply --index failed for the remote patch: {apply.stderr.strip()[-500:] or 'no stderr'}"
        )
    first_line = (summary.strip().splitlines()[0][:72]) if summary.strip() else "remote turn"
    message = f"offload: {first_line}\n\nPlan 0144 grok-remote turn for lane {lane_id}.\nOffload-Backend: grok-remote"
    commit = subprocess.run(
        ["git", "-C", str(worktree_path), *_ENGINE_GIT_IDENTITY, "-c", "commit.gpgsign=false", "commit", "-m", message],
        capture_output=True,
        text=True,
        check=False,
    )
    if commit.returncode != 0:
        rollback = subprocess.run(
            ["git", "-C", str(worktree_path), "apply", "-R", "--index", str(patch_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        rollback_note = (
            "reversed the applied patch (lane restored to pre-turn HEAD)"
            if rollback.returncode == 0
            else f"WARNING: rollback failed (rc={rollback.returncode}); lane may be dirty: {rollback.stderr.strip()[-200:] or 'no stderr'}"
        )
        raise RuntimeError(
            f"local commit of the remote patch failed: {commit.stderr.strip()[-500:] or 'no stderr'} — {rollback_note}"
        )


def _result_from_json(result_file: Path) -> BackendResult | None:
    """Parse grok's structured stdout JSON (fetched to --result-out) into a
    BackendResult, tolerating narration noise (same parse chain as GrokCliAdapter).
    Returns None when absent/empty/unparseable/oversized (caller fails closed).

    Size-capped before read [RES-05]: the remote controls --result-out content;
    an unbounded read of a multi-GB file would OOM the local adapter process.
    """
    if not result_file.is_file():
        return None
    try:
        size = result_file.stat().st_size
    except OSError:
        return None
    if size > _RESULT_FILE_MAX_BYTES:
        return None
    stdout = result_file.read_text(errors="replace")
    if not stdout.strip():
        return None
    envelope = _parse_envelope(stdout)
    payload = _extract_grok_payload(stdout, envelope)
    if payload is not None:
        return BackendResult.from_dict(payload)
    # Review turns emit REVIEW_OUTPUT_SCHEMA payloads (findings/summary,
    # additionalProperties:false) which by design carry NO handoff_action —
    # _extract_grok_payload's shape key — so they extract to None and were
    # hard-failed as "transport failures", destroying real findings
    # (r07163433 HIGH-1). Fall back to a review-shaped extraction and wrap the
    # payload; rc-code handling clamps handoff_action anyway.
    review = _extract_review_payload(stdout, envelope)
    if review is None:
        return None
    return BackendResult(
        handoff_action="needs_guidance",
        summary=str(review.get("summary") or "review output (no handoff envelope)")[:200],
        details="",
        raw_payload=review,
    )


def _off_box_self_verify_from_json(selfverify_file: Path) -> dict[str, Any] | None:
    """Parse the off-box self-verify JSON (fetched to --selfverify-out) into a
    normalized ``{command, exit_code, passed, output_tail}`` dict, or None when
    absent/empty/unparseable/oversized/malformed. Fail-open to None: a missing
    capture is handled by the worker's OBS-08 enforcement (a typed failure),
    never a silent local re-run.

    Size-capped before read [RES-05]: the remote controls the file content.
    """
    if not selfverify_file.is_file():
        return None
    try:
        if selfverify_file.stat().st_size > _RESULT_FILE_MAX_BYTES:
            return None
    except OSError:
        return None
    text = selfverify_file.read_text(errors="replace")
    if not text.strip():
        return None
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(payload, dict) or "exit_code" not in payload:
        return None
    try:
        exit_code = int(payload["exit_code"])
    except (TypeError, ValueError):
        return None
    return {
        "command": str(payload.get("command") or ""),
        "exit_code": exit_code,
        "passed": bool(payload.get("passed", exit_code == 0)),
        "output_tail": _tail_text(str(payload.get("output_tail") or "")),
    }


def _extract_review_payload(stdout: str, envelope: dict[str, Any] | None) -> dict[str, Any] | None:
    """Mirror ``_extract_grok_payload``'s tiers keyed on the review shape.

    A dict qualifies when it carries a list-valued ``findings`` key (the
    REVIEW_OUTPUT_SCHEMA discriminator). Same tier order: envelope root,
    narrated text channels (LAST shaped object wins), dict-valued envelope
    fields, then structuredOutput.
    """

    def _shaped(d: dict[str, Any]) -> bool:
        return isinstance(d.get("findings"), list)

    if envelope is not None and _shaped(envelope):
        return envelope
    texts: list[str] = []
    if envelope is not None:
        for key in ("text", "output_text", "content", "message", "result"):
            value = envelope.get(key)
            if isinstance(value, str):
                texts.append(value)
    else:
        texts.append(stdout)
    for text in texts:
        shaped = [d for d in _text_result_dicts(text) if _shaped(d)]
        if shaped:
            return shaped[-1]
    if envelope is not None:
        for key in ("result", "content", "output", "message"):
            value = envelope.get(key)
            if isinstance(value, dict) and _shaped(value):
                return value
        structured = envelope.get("structuredOutput")
        if isinstance(structured, dict) and _shaped(structured):
            return structured
        if isinstance(structured, str):
            candidate = _loads_dict(structured)
            if candidate is not None and _shaped(candidate):
                return candidate
    return None


def _remote_unavailable_result(
    *,
    summary: str,
    blocker: str,
    details: str,
    model: str,
    effort: str | None,
    raw_payload: dict | None = None,
) -> BackendResult:
    """Typed fail-closed result for a transport failure (implementation note S3).

    An ungated grok-remote dispatch whose VM is unconfigured, unreachable, or
    times out must degrade to a recorded ``needs_guidance`` blocker (announced +
    recorded by the pass) rather than crashing it with a ``RuntimeError`` — the
    RES-13 fail-closed crumple zone at the cross-host boundary. No commit landed,
    so ``merge_ready`` is always False.

    ``raw_payload`` defaults to empty; the exit-75 VM admission path (memory
    floor or lane cap) may attach an ``admission_deferred`` marker so the
    orchestrator can recover via a fresh re-dispatch (admission is re-checked)
    while keeping ``handoff_action="needs_guidance"`` as the fail-safe default.
    """
    return BackendResult(
        handoff_action="needs_guidance",
        summary=summary,
        details=details,
        merge_ready=False,
        blockers=[blocker],
        response_model=model,
        reasoning_effort=effort,
        raw_payload=raw_payload if raw_payload is not None else {},
    )


class RemoteExecAdapter(BackendAdapter):
    """Execute a grok turn on the remote VM; delegate shaping to GrokCliAdapter."""

    # Sandboxing is the VM's job (the remote sandbox is history-stripped +
    # remote-severed), not a local shallow clone — unlike GrokCliAdapter.
    supports_jail = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Reuse the local grok port for prompt shaping + effort resolution + bounds.
        self._grok = GrokCliAdapter(*args, **kwargs)

    def resolve_reasoning_effort(
        self,
        *,
        orchestrator_root: Path,
        task_ref: str,
        lane_id: str,
        requested: str,
        cycle: int,
        prompt_override: str | None,
        previous_run_exhausted: bool = False,
    ) -> tuple[str | None, list[str]]:
        return self._grok.resolve_reasoning_effort(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            requested=requested,
            cycle=cycle,
            prompt_override=prompt_override,
            previous_run_exhausted=previous_run_exhausted,
        )

    def execute(
        self,
        prompt: str,
        schema: dict[str, Any],
        worktree_path: Path,
        model: str | None = None,
        reasoning_effort: str | None = None,
        session_mode: str | None = None,
        env: dict[str, str] | None = None,
        progress_callback: Callable[..., None] | None = None,
        **kwargs: Any,
    ) -> BackendResult:
        """Ship one grok turn to the VM, land its commit locally, return a typed result."""
        from workbay_handoff_mcp.enums import (  # noqa: PLC0415
            WorkerEventName,
            normalize_model_identity,
            normalize_model_label,
        )

        del session_mode  # fresh_turn only (MVP); shared_lane continuity is a non-goal.
        worktree_path = Path(worktree_path)
        applied_effort = reasoning_effort if reasoning_effort in _REMOTE_EFFORTS else None

        # Pin guard: allow-list polarity matches GrokCliAdapter (decision #2799 /
        # implementation note S2) — only DEFAULT_GROK_MODEL is allowed; grok-build and any
        # other slug are refused. Preserve the build-specific message for the
        # grok-build family so existing tests keep matching on "grok-build".
        effective_model = model or DEFAULT_GROK_MODEL
        if _GROK_BUILD_RE.search(effective_model):
            raise RuntimeError(
                f"Refusing grok-remote dispatch with a build model '{effective_model}' (grok-build family refused)."
            )
        if effective_model != DEFAULT_GROK_MODEL:
            raise RuntimeError(
                f"Refusing grok-remote dispatch with model '{effective_model}': allowed is "
                f"the configured pin '{DEFAULT_GROK_MODEL}' (WORKBAY_GROK_MODEL)."
            )

        # Fail-closed on a dirty local tree: the offload pass checkpoints before each
        # turn, so HEAD is always the turn's committed input; a dirty tree is an
        # unexpected state, not something to ship to the VM.
        if _local_dirty(worktree_path):
            return BackendResult(
                handoff_action="needs_guidance",
                summary="grok-remote refused: local worktree is dirty before the turn",
                details="",
                merge_ready=False,
                blockers=["dirty local worktree before remote dispatch (expected a clean, committed HEAD)"],
                response_model=effective_model,
                reasoning_effort=applied_effort,
            )

        branch = _worktree_branch(worktree_path)
        script = _resolve_script(worktree_path)
        pinned_identity = normalize_model_identity(normalize_model_label(effective_model), None) or effective_model
        full_prompt = _build_grok_prompt(prompt, schema, pinned_identity)
        runner = kwargs.get("remote_runner") or _default_remote_runner
        lane_id = str(kwargs.get("lane_id") or branch)
        # Off-box self-verify command (item 26): when set, the VM runs it in the
        # sandbox venv after grok commits and reports the outcome, which the
        # worker consumes instead of re-running locally. Empty/None → not shipped
        # (review lanes and on-box paths are unaffected).
        test_cmd = str(kwargs.get("test_cmd") or "").strip() or None

        run_env = dict(env) if env else os.environ.copy()
        # Ensure the gate host reaches the script even under a restricted lane env
        # (the script also falls back to .workbay/remote-gate.env when unset).
        if "WORKBAY_REMOTE_GATE_HOST" not in run_env and os.environ.get("WORKBAY_REMOTE_GATE_HOST"):
            run_env["WORKBAY_REMOTE_GATE_HOST"] = os.environ["WORKBAY_REMOTE_GATE_HOST"]

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend="grok-remote")

        # Remote hard-timeout sits just under the local SSH bound (RES-02) so
        # grok self-terminates on the VM — and its result/debug logs still
        # fetch — before the local runner gives up. Subtract post-grok fetch
        # headroom (15s) from the local bound when the budget is large enough
        # that room remains; remote_agent.sh further subtracts actual
        # pre-dispatch elapsed (probe + push + scp) from this residual and
        # never floors it above the remaining budget (fail-fast when residual
        # is exhausted before grok starts). When local_timeout is at or below
        # the headroom threshold (~45s, e.g. unit tests), pass the local bound
        # through unchanged — there is no room to reserve post-grok fetch
        # headroom without collapsing the turn to zero.
        local_timeout = int(self._grok.timeout)
        remote_timeout = local_timeout - 15 if local_timeout > 45 else local_timeout

        with tempfile.TemporaryDirectory(prefix="remote-exec-") as tmpdir:
            tmp = Path(tmpdir)
            brief_file = tmp / "brief.md"
            brief_file.write_text(full_prompt)
            schema_file = tmp / "schema.json"
            schema_file.write_text(json.dumps(schema))
            patch_file = tmp / "turn.patch"
            result_file = tmp / "result.json"
            debug_file = tmp / "debug.log"
            selfverify_file = tmp / "selfverify.json"

            cmd = [
                str(script),
                "build",
                "--branch",
                branch,
                "--brief",
                str(brief_file),
                "--schema",
                str(schema_file),
                "--out",
                str(patch_file),
                "--result-out",
                str(result_file),
                "--debug-out",
                str(debug_file),
                "--timeout",
                str(remote_timeout),
                "--model",
                effective_model,
                "--max-turns",
                str(self._grok.max_turns),
            ]
            if applied_effort:
                cmd += ["--effort", applied_effort]
            if test_cmd:
                # Off-box self-verify (item 26): the VM runs this TEST_CMD in the
                # sandbox venv after grok commits, writing the outcome JSON to
                # --selfverify-out (fetched below into off_box_self_verify).
                cmd += ["--test-cmd", test_cmd, "--selfverify-out", str(selfverify_file)]

            try:
                completed = runner(cmd, cwd=str(worktree_path), env=run_env, timeout=self._grok.timeout)
            except subprocess.TimeoutExpired as exc:
                # Fail closed (S3): a slow/hung VM is announced+recorded, not a crash.
                return _remote_unavailable_result(
                    summary=f"grok-remote turn timed out after {self._grok.timeout}s",
                    blocker=(
                        f"remote turn exceeded the local transport bound ({self._grok.timeout}s) — "
                        "VM slow or unreachable; failing closed."
                    ),
                    details=_tail_text(exc.stderr),
                    model=effective_model,
                    effort=applied_effort,
                )

            rc = completed.returncode
            stderr_tail = _tail_text(completed.stderr)
            grok_result = _result_from_json(result_file)

            if rc == _RC_HOST_UNCONFIGURED:
                # Exit 78 is shared: genuine host-unconfigured OR flock missing on the
                # VM (same-branch collision guard). Discriminate from stderr so the
                # operator is not sent to set WORKBAY_REMOTE_GATE_HOST when the host
                # is configured and the VM image simply lacks util-linux.
                stderr_l_78 = (stderr_tail or "").lower()
                if "flock unavailable" in stderr_l_78:
                    return _remote_unavailable_result(
                        summary="grok-remote unavailable: flock missing on the VM",
                        blocker=(
                            "flock unavailable on the remote VM (remote_agent.sh exit 78): "
                            "the VM image lacks util-linux/flock required for the same-branch "
                            "collision guard — install util-linux on the VM image."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                    )
                # Opt-in graceful degradation (S3): unconfigured host is a typed skip,
                # never a hard error — normally gated by the availability probe.
                return _remote_unavailable_result(
                    summary="grok-remote unavailable: remote gate host not configured",
                    blocker=(
                        "remote gate host not configured (remote_agent.sh exit 78); set "
                        "WORKBAY_REMOTE_GATE_HOST. Normally gated by the availability probe."
                    ),
                    details=stderr_tail,
                    model=effective_model,
                    effort=applied_effort,
                )
            if rc == _RC_NO_CHANGES:
                # Exit 4: grok ran, made no commit. Keep raw_payload/summary/
                # details/tests_run intact so review_runner can still read
                # findings from --result-out ([REF-10] review path is
                # payload-only). Clamp handoff_action to needs_guidance and
                # force merge_ready=False: the execute chain keys exclusively
                # on handoff_action (0144 R3 / HIGH-1), so a commitless payload
                # that claims finished must never report merge-ready.
                no_commit_blocker = "remote turn made no commit (remote_agent.sh exit 4)"
                if grok_result is not None:
                    blockers = list(grok_result.blockers)
                    if no_commit_blocker not in blockers:
                        blockers.append(no_commit_blocker)
                    return BackendResult(
                        handoff_action="needs_guidance",
                        summary=grok_result.summary,
                        details=grok_result.details or stderr_tail,
                        tests_run=grok_result.tests_run,
                        blockers=blockers,
                        changed_files=list(grok_result.changed_files),
                        merge_ready=False,
                        token_usage=grok_result.token_usage,
                        response_model=grok_result.response_model or effective_model,
                        reasoning_effort=applied_effort or grok_result.reasoning_effort,
                        raw_payload=grok_result.raw_payload,
                    )
                return BackendResult(
                    handoff_action="needs_guidance",
                    summary="grok-remote produced no committed changes",
                    details=stderr_tail,
                    merge_ready=False,
                    blockers=[no_commit_blocker],
                    response_model=effective_model,
                    reasoning_effort=applied_effort,
                )
            if rc == _RC_GROK_FAILED:
                return BackendResult(
                    handoff_action="needs_guidance",
                    summary="grok run failed on the remote VM",
                    details=stderr_tail,
                    merge_ready=False,
                    blockers=["remote grok run failed (remote_agent.sh exit 3)"],
                    response_model=effective_model,
                    reasoning_effort=applied_effort,
                )
            if rc == _RC_ADMISSION_DEFERRED:
                # VM admission fired (remote_agent.sh exit 75): memory floor,
                # concurrent lane cap, residual timeout, or same-branch lane lock.
                # Retryable backpressure defer ([RES-14]), announced+recorded, not
                # a fault. Discriminate reason from the script's stderr so the pass
                # can surface the right defer text; carry admission_deferred on
                # raw_payload so the orchestrator can recover via a fresh re-dispatch
                # when the VM has headroom / the peer lane finishes.
                stderr_l = (stderr_tail or "").lower()
                if "lane cap" in stderr_l:
                    return _remote_unavailable_result(
                        summary="grok-remote deferred: VM lane cap reached",
                        blocker=(
                            "remote turn deferred by the VM lane cap (remote_agent.sh exit 75): "
                            "lane cap reached — deferring; retry when a concurrent lane frees."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "admission_deferred": True,
                            "defer_reason": "vm_lane_cap",
                        },
                    )
                if "residual timeout exhausted" in stderr_l:
                    # Two producers share the phrase with OPPOSITE causes:
                    # pre-dispatch transport vs in-sandbox setup/uv-sync.
                    # Discriminate on the unique producer token so a slow sync
                    # is not reported as "the VM itself is fine" [TEST-15].
                    if "in-sandbox setup" in stderr_l:
                        return _remote_unavailable_result(
                            summary=(
                                "grok-remote deferred: turn budget exhausted by "
                                "in-sandbox setup"
                            ),
                            blocker=(
                                "remote turn deferred (remote_agent.sh exit 75): residual "
                                "timeout exhausted after in-sandbox setup (archive/uv-sync) "
                                "— retry with more budget or a warm venv; not a transport "
                                "or free-RAM diagnosis."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "admission_deferred": True,
                                "defer_reason": "residual_timeout_in_sandbox_setup",
                            },
                        )
                    # pre-dispatch (or unknown residual) — transport ate budget.
                    return _remote_unavailable_result(
                        summary="grok-remote deferred: turn budget exhausted before grok started",
                        blocker=(
                            "remote turn deferred (remote_agent.sh exit 75): residual timeout "
                            "exhausted by pre-dispatch probe/push/scp — retry with more budget "
                            "or when transport is faster; the VM itself is fine."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "admission_deferred": True,
                            "defer_reason": "residual_timeout_pre_dispatch",
                        },
                    )
                if "same-branch lane already active" in stderr_l:
                    # Fourth exit-75 cause: another dispatch of this branch holds
                    # the same-branch lane lock. NOT a memory diagnosis — a false
                    # vm_memory_pressure here sends the operator hunting VM RAM
                    # when the remedy is to wait for the peer lane (serialization).
                    return _remote_unavailable_result(
                        summary="grok-remote deferred: same-branch lane already active",
                        blocker=(
                            "remote turn deferred by same-branch lane lock (remote_agent.sh "
                            "exit 75): another dispatch of this branch is already active — "
                            "deferring; retry when that lane finishes (branch serialization)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "admission_deferred": True,
                            "defer_reason": "same_branch_lane_active",
                        },
                    )
                if "occupying sandbox" in stderr_l:
                    # Fifth exit-75 cause: occupancy re-check after lock — a prior
                    # same-key dispatch lost its shell (and the lock) while its
                    # agent still occupies $SBX under the named scope. NOT memory
                    # pressure: the peer lane owns the sandbox; free RAM will not
                    # help. Becomes common once the scope-named occupancy probe
                    # actually fires.
                    return _remote_unavailable_result(
                        summary="grok-remote deferred: same-branch lane still occupying sandbox",
                        blocker=(
                            "remote turn deferred by sandbox occupancy (remote_agent.sh "
                            "exit 75): a same-branch lane still occupies the sandbox under "
                            "its systemd scope — deferring; retry when that occupant exits "
                            "(peer owns the sandbox; free VM headroom will not help)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "admission_deferred": True,
                            "defer_reason": "sandbox_occupied",
                        },
                    )
                return _remote_unavailable_result(
                    summary="grok-remote deferred: VM under memory pressure",
                    blocker=(
                        "remote turn deferred by the VM memory guard (remote_agent.sh exit 75): "
                        "VM MemAvailable below the reserved floor; retry when the VM has headroom "
                        "(dispatched lanes yield to all non-lane work)."
                    ),
                    details=stderr_tail,
                    model=effective_model,
                    effort=applied_effort,
                    raw_payload={
                        "admission_deferred": True,
                        "defer_reason": "vm_memory_pressure",
                    },
                )
            if rc == _RC_HARD_FAIL:
                # Exit 1 has two producers: the SANDBOX-NOT-REMOTE-SEVERED security
                # assertion, and a fatal uv-sync failure. Neither is transport —
                # labeling them "VM unreachable" invites a retry that re-runs the
                # same posture violation [RES-01][AGT-10].
                stderr_l_1 = (stderr_tail or "").lower()
                if "sandbox not remote-severed" in stderr_l_1 or "not remote-severed" in stderr_l_1:
                    return _remote_unavailable_result(
                        summary=(
                            "grok-remote security tripwire: sandbox not remote-severed"
                        ),
                        blocker=(
                            "remote_agent.sh failed (exit 1): SANDBOX NOT REMOTE-SEVERED — "
                            "security posture assertion; do not retry until the sandbox "
                            "init path is fixed (not a transport/VM-reachability flake)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "hard_fail_reason": "sandbox_not_remote_severed",
                        },
                    )
                if "uv sync failed" in stderr_l_1:
                    return _remote_unavailable_result(
                        summary="grok-remote failed: in-sandbox uv sync failed",
                        blocker=(
                            "remote_agent.sh failed (exit 1): uv sync failed after fresh "
                            "venv rebuild — dependency sync on the VM; not a transport "
                            "error (retry may help only after lock/index is healthy)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "hard_fail_reason": "uv_sync_failed",
                        },
                    )
                return _remote_unavailable_result(
                    summary="grok-remote hard failure (remote_agent.sh exit 1)",
                    blocker=(
                        "remote_agent.sh failed (exit 1): hard failure on the remote "
                        "path (not classified as transport); failing closed (announced "
                        "+ recorded)."
                    ),
                    details=stderr_tail,
                    model=effective_model,
                    effort=applied_effort,
                    raw_payload={
                        "hard_fail_reason": "unclassified_exit_1",
                    },
                )
            if rc == _RC_USAGE:
                return _remote_unavailable_result(
                    summary="grok-remote usage/validation error (remote_agent.sh exit 2)",
                    blocker=(
                        "remote_agent.sh failed (exit 2): usage or validation error — "
                        "fix the dispatch arguments/config (not a transport flake)."
                    ),
                    details=stderr_tail,
                    model=effective_model,
                    effort=applied_effort,
                )
            if rc != _RC_PATCH:
                # VM unreachable / SSH failure / unexpected transport error (S3):
                # fail closed with an announced+recorded reason, do not crash the pass.
                return _remote_unavailable_result(
                    summary=f"grok-remote transport failed (remote_agent.sh exit {rc})",
                    blocker=(
                        f"remote_agent.sh failed (exit {rc}) — VM unreachable or transport error; "
                        "failing closed (announced + recorded)."
                    ),
                    details=stderr_tail,
                    model=effective_model,
                    effort=applied_effort,
                )

            # rc == 0: apply the returned patch locally + commit with the engine identity.
            summary = grok_result.summary if grok_result is not None else ""
            _apply_and_commit(worktree_path, patch_file, lane_id=lane_id, summary=summary)
            # Off-box self-verify capture (item 26): parse inside the tempdir (the
            # file lives under tmp). None when no test_cmd shipped or the VM
            # emitted nothing — the worker's OBS-08 enforcement then blocks a
            # commit-landed lane with no captured verify rather than silently pass.
            off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
            # Post-turn grok-build contamination backstop (decision #2799): mirror
            # GrokCliAdapter and scan the fetched --debug-out log for grok-build-
            # authored AssistantItems while the tempdir still exists. Absent/empty
            # log => None (not contamination), so this degrades cleanly when the
            # remote produced no debug log (e.g. an older remote_agent.sh).
            contamination = _detect_grok_build_contamination(debug_file)

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_COMPLETE, backend="grok-remote")

        committed = _committed_files(worktree_path)
        if contamination is not None:
            # The engine-authored commit already landed; preserve it (never
            # silent-discard) but quarantine to needs_guidance so it is never
            # auto-merged — the grok-remote analogue of composer_violation_quarantined.
            blocker, evidence = contamination
            return BackendResult(
                handoff_action="needs_guidance",
                summary="grok-remote: grok-build contamination detected in the remote debug log",
                details=stderr_tail,
                merge_ready=False,
                blockers=[blocker],
                changed_files=committed,
                response_model=effective_model,
                reasoning_effort=applied_effort,
                raw_payload={
                    "composer_violation_evidence": evidence,
                    "attestation": {"status": "failed", "reason": "grok_build_contamination", "pin": effective_model},
                },
                off_box_self_verify=off_box_sv,
            )
        if grok_result is None:
            # Patch applied + committed, but grok's structured result was unparseable.
            # Fail closed to needs_guidance (never silent-empty); the commit stands.
            return BackendResult(
                handoff_action="needs_guidance",
                summary="grok-remote turn committed but its structured result was unparseable",
                details=stderr_tail,
                merge_ready=False,
                blockers=["remote result JSON unparseable (checked envelope + payload for a handoff_action result)"],
                changed_files=committed,
                response_model=effective_model,
                reasoning_effort=applied_effort,
                off_box_self_verify=off_box_sv,
            )

        # Trust the committed file list over grok's self-report; carry the rest through.
        return BackendResult(
            handoff_action=grok_result.handoff_action,
            summary=grok_result.summary,
            details=grok_result.details,
            tests_run=grok_result.tests_run,
            blockers=grok_result.blockers,
            changed_files=committed or grok_result.changed_files,
            merge_ready=grok_result.merge_ready,
            token_usage=grok_result.token_usage,
            response_model=grok_result.response_model or effective_model,
            reasoning_effort=applied_effort or grok_result.reasoning_effort,
            raw_payload=grok_result.raw_payload,
            off_box_self_verify=off_box_sv,
        )
