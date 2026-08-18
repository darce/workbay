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
  (``wip(offload): <lane_id> checkpoint <n>``); ``uncommitted_work`` is
  returned only when the checkpoint itself fails.
- Pass state persists in ``<state_dir>/offload-pass-<pass_id>.json`` so a
  disconnected client can recover the outcome via ``await_offload_pass``.
"""

from __future__ import annotations

import contextlib
import html
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

from workbay_handoff_mcp.shared_schema import connect_handoff_db
from workbay_orchestrator_mcp.orchestration.backend_registry import (
    backend_supports_adapter_timeout_bounds,
    backend_supports_token_budget_cycle_bounds,
)
from workbay_orchestrator_mcp.orchestration.grok_lane_config import (
    ENGINE_GIT_IDENTITY as _ENGINE_GIT_IDENTITY,
)
from workbay_orchestrator_mcp.orchestration.stop_reasons import (
    SALVAGE_STOP_REASONS as _SALVAGE_STOP_REASONS,
    STOP_REASONS_CHECKPOINT as _STOP_REASONS_CHECKPOINT,
)

PASS_OUTCOMES = frozenset(
    {
        "handoff_ready",
        # implementation note R3: a review lane (lane_kind='review') finished cleanly — clean
        # tree, unchanged HEAD, handoff submitted (exit 0), and a parseable findings
        # block harvested. A success, NOT a wedged needs_guidance transport failure.
        "review_complete",
        "needs_guidance",
        # implementation note S1 / OL-PF2: green self-verified committed work whose handoff
        # carried no genuine question. Distinct from needs_guidance (real blocker
        # or question) and from handoff_ready (verified merge-ready close).
        "completed_unreviewed",
        "no_actionable_work",
        "uncommitted_work",
        "token_budget_exceeded",
        "timeout",
        "error",
        "still_running",
        "lane_not_found",
        "self_verify_failed",
        # Zero tests executed (pytest usage error / no tests collected). Not red.
        "self_verify_inconclusive",
        # grok-build contamination quarantine only (Composer attestation retired, implementation note S2)
        "composer_violation_quarantined",
        "checkpoint",
        # implementation note R7: the engine's own on-disk source vanished since import (a
        # concurrent env flip deleted the installed package) — refuse loudly with
        # the restart remedy instead of crashing mid-pass.
        "server_stale_restart_required",
        # internal: host memory pressure rose mid-pass; the pass parked
        # (dirty work preserved as a checkpoint) rather than spawn another turn.
        "admission_deferred",
        # internal: the pass-start admission gate hard-refused the spawn (a
        # resource floor is breached — not retryable until the host recovers).
        "admission_refused",
        # implementation note S2 — remote_only ledger refused an explicit local backend;
        # policy outcome, distinct from transport exit codes 78/75.
        "remote_required",
        # Lane worktree missing and re-materialization impossible (branch gone /
        # primary missing / git worktree add failed). Pre-pass API refusal from
        # ensure_lane_worktree; callers branch on this instead of free-text.
        "worktree_unrecoverable",
        # internal / engoutcome: work landed (commit/checkpoint) but the
        # reporting/handoff ceremony failed. Distinct from error (work did not land
        # / unknown failure) and from success enums (handoff_ready / review_complete).
        # Keeps the two axes from overwriting each other [OBS-08][RLSE-05].
        "ceremony_failed",
    }
)

# Dual-axis terminal discriminators (internal / engoutcome).
# The outcome enum used to collapse "did work land?" and "did the handoff
# ceremony complete?" into one value; ceremony failure then overwrote a
# work-landed success into error/needs_guidance. Callers branch on these
# fields without parsing prose [OBS-08].
WORK_STATUS_LANDED = "landed"
WORK_STATUS_NOT_LANDED = "not_landed"
WORK_STATUSES = frozenset({WORK_STATUS_LANDED, WORK_STATUS_NOT_LANDED})

CEREMONY_STATUS_CLEAN = "clean"
CEREMONY_STATUS_FAILED = "failed"
CEREMONY_STATUS_NOT_ATTEMPTED = "not_attempted"
CEREMONY_STATUSES = frozenset(
    {CEREMONY_STATUS_CLEAN, CEREMONY_STATUS_FAILED, CEREMONY_STATUS_NOT_ATTEMPTED}
)

# Exit codes that mean the gate did not meaningfully execute tests.
# 4/5 = pytest usage / no tests collected; 70 = off-box not_run; 75 = suite-lock
# deferral; 126/127 = harness not-executable / not-found. Exit 2 (bash misuse /
# syntax) is handled via output_tail patterns so a real suite interrupted with
# exit 2 is not mislabeled unrun.
# Bare pytest after a red suite exits 1 (uses 0–5 only). Codes 70/75/126/127
# are reachable only via a harness wrapper and MUST be corroborated by the
# absence of red-suite evidence in the tail (see ``_self_verify_is_inconclusive``).
# These must not share the self_verify_failed bucket with a red suite.
_SELF_VERIFY_INCONCLUSIVE_EXIT_CODES = frozenset({4, 5, 70, 75, 126, 127})

# Red-suite evidence in output_tail. Presence vetoes promotion to unrun
# regardless of exit code, stamped outcome, or unrun-shaped substrings that
# merely appear inside assertion text (OFFP-A-R1 / MC1–MC7).
# Non-zero count required: "0 failed" is the wrong polarity for a suite-failed
# detector (OFFP2A-R1-01). Error-count / ERROR nodeid alternations cover
# collection/fixture/setup failures that carry no "failed" token (OFFP2A-R1-02).
_SELF_VERIFY_RED_TAIL_RE = re.compile(
    r"^\s*FAILED\s+\S+"
    r"|^\s*ERROR\s+\S+"
    r"|\b[1-9]\d*\s+failed\b"
    r"|\b[1-9]\d*\s+errors?\b"
    r"|^={3,}\s*FAILURES\s*={3,}",
    re.IGNORECASE | re.MULTILINE,
)

# Tail / stamped-outcome evidence that the TEST_CMD never reached a real suite.
# Syntax-error alternative is anchored to a shell diagnostic line so a red
# pytest run whose assertion text *quotes* the phrase is not typed unrun.
# Includes residual double-entity ``&&`` markers (``&amp;amp;``) that appear
# only when the suite never ran (red evidence above vetoes quote contamination).
_SELF_VERIFY_UNRUN_TAIL_RE = re.compile(
    r"(?m)^(?:ba)?sh:[^\n]*syntax error near unexpected token"
    r"|&amp;amp;"
    r"|self_verify_outcome=(?:harness_error|not_run)"
    r"|(?:^|[\s:`])(?:ba)?sh: .*(?:command not found|No such file or directory|Permission denied)",
    re.IGNORECASE | re.MULTILINE,
)

# Off-box / consumer-side stamped outcomes that mean the gate did not run.
_SELF_VERIFY_UNRUN_OUTCOMES = frozenset({"harness_error", "not_run"})

# Double-or-deeper HTML-entity encoding of ``&&`` (two amp-chains, each with
# at least two ``amp;`` units). Single-escaped ``&amp;&amp;`` and unrelated
# entities (``&amp;``, ``&lt;``, numeric ``&#38;``) are intentionally NOT
# matched — identity on those inputs (OFFP-B-R1 narrow repair).
# Case policy (OFFP2B-R1-01 / OFFP2B-R1-04): matcher is IGNORECASE; the decode
# step folds named-entity names to lowercase before html.unescape so mixed-case
# spans (``&Amp;``) are admitted *and* decodable. stdlib html.unescape handles
# ``&amp;``/``&AMP;`` but not mixed case — without the fold, IGNORECASE admits
# spans that never converge and raise ValueError.
_DOUBLE_ENTITY_AND_AND_RE = re.compile(r"&(?:amp;){2,}&(?:amp;){2,}", re.IGNORECASE)

# Named HTML entity token used to case-fold before stdlib unescape (decoder
# half of the case policy above).
_NAMED_ENTITY_TOKEN_RE = re.compile(r"&([A-Za-z][A-Za-z0-9]*);")

# Max html.unescape iterations when repairing one matched span. Exhaustion is
# a hard failure (never ship a partial residual entity string).
_NORMALIZE_AND_AND_MAX_UNESCAPE = 8


def _html_unescape_named_ci(text: str) -> str:
    """``html.unescape`` with case-insensitive named-entity recognition.

    Folds ``&Amp;`` / ``&aMp;`` → ``&amp;`` before decoding so the normalizer's
    IGNORECASE matcher and its decoder share one case policy (OFFP2B-R1-01).
    """
    folded = _NAMED_ENTITY_TOKEN_RE.sub(lambda m: f"&{m.group(1).lower()};", text)
    return html.unescape(folded)

# implementation note S3 / exit-8: checkpoint / salvage gates — derived from the single
# owner registry (stop_reasons.STOP_REASON_REGISTRY). Names kept for existing
# callers/tests; membership is never re-typed here.


def normalize_lane_test_cmd(test_cmd: str | None) -> str | None:
    """Return ``test_cmd`` with double-entity ``&&`` corruption repaired, or identity.

    Finding OFFLOAD-LANE-TESTCMD-AMPERSAND-DOUBLE-HTML-ESCAPED-AT-SHELL: a
    literal ``&&`` that reaches bash as ``&amp;amp;&amp;amp;`` is rejected with
    ``syntax error near unexpected token ;&`` and the pass reports a red-like
    failure even though no suite ran.

    Producer search (OFFP-B-R1 / this repair lane): zero production
    ``html.escape`` sites on the ``test_cmd`` path in this monorepo (control
    probe: ``def normalize_lane_test_cmd`` and ``def run_offload_pass_engine``
    are found; ``html.escape`` hits are this docstring and synthetic test
    fixtures only). Visibility ends at the sandbox boundary — an external
    MCP client / HTML round-trip / paste may still introduce the form. With
    no in-tree producer, this function is a narrow, logged peel of the
    specific double-or-deeper entity-encoded ``&&`` shape, and the engine
    converges the stored ``worktree_lanes.test_cmd`` row when it fires so
    operator-visible storage matches the executed value.

    Contract:
    - No double-entity ``&&`` match → byte-identical identity (other entities
      untouched).
    - Match → replace each span with literal ``&&``; log both forms.
    - Bounded unescape cannot reach ``&&`` → raise (never ship partial).
    """
    if test_cmd is None:
        return None
    cmd = str(test_cmd)
    if not cmd.strip():
        return None
    if not _DOUBLE_ENTITY_AND_AND_RE.search(cmd):
        return cmd

    def _repair_span(match: re.Match[str]) -> str:
        span = match.group(0)
        cur = span
        for _ in range(_NORMALIZE_AND_AND_MAX_UNESCAPE):
            # Case-insensitive named-entity decode (matches IGNORECASE matcher).
            nxt = _html_unescape_named_ci(cur)
            if nxt == cur:
                break
            cur = nxt
            if cur == "&&":
                return "&&"
        if cur != "&&":
            raise ValueError(
                "normalize_lane_test_cmd: could not fully repair entity-encoded "
                f"&& (span={span!r} residual={cur!r}); refusing partial result"
            )
        return "&&"

    fixed = _DOUBLE_ENTITY_AND_AND_RE.sub(_repair_span, cmd)
    if fixed != cmd:
        logger.warning(
            "normalize_lane_test_cmd repaired double-entity &&: before=%r after=%r",
            cmd,
            fixed,
        )
    return fixed


def _converge_stored_lane_test_cmd(
    *, task_ref: str, lane_id: str, test_cmd: str
) -> bool:
    """Write the repaired ``test_cmd`` back to ``worktree_lanes`` (source converge).

    Returns True when at least one row was updated. Best-effort: never raises
    into the pass outcome path — the runtime value is already repaired on the
    consumer; storage convergence is the durable fix for other readers.
    Returns False (not a false success) when the row is absent or the DB is
    unopenable (OFFP2B-R1-03). The UPDATE is scoped by task_ref AND lane_id so
    a peel cannot rewrite sibling lanes under the same task.

    Connection lifecycle (OFFP2B-R1-02): ``sqlite3.connect`` as a context
    manager manages the *transaction* only (commit/rollback); the connection
    stays open. Wrap with ``contextlib.closing`` so each peel closes its handle.
    """
    try:
        # closing() owns the connection; the inner with-conn still commits.
        with contextlib.closing(connect_handoff_db(_handoff_db_path())) as conn:
            cur = conn.execute(
                """
                UPDATE worktree_lanes
                SET test_cmd = ?, updated_at = datetime('now')
                WHERE task_ref = ? AND lane_id = ?
                """,
                (test_cmd, task_ref, lane_id),
            )
            conn.commit()
            updated = int(cur.rowcount or 0) > 0
    except Exception as exc:  # noqa: BLE001 — storage converge must not abort pass
        logger.warning(
            "normalize_lane_test_cmd: failed to converge stored test_cmd for "
            "task_ref=%s lane_id=%s: %s",
            task_ref,
            lane_id,
            exc,
        )
        return False
    if updated:
        logger.warning(
            "normalize_lane_test_cmd: converged worktree_lanes.test_cmd for "
            "task_ref=%s lane_id=%s to %r",
            task_ref,
            lane_id,
            test_cmd,
        )
    return updated


def _host_admission_should_park(orchestrator_root: Path, backend: str | None = None) -> str | None:
    """internal pre-turn re-check: reason to park a long pass, or None.

    Only a hard *refuse* dimension (rising memory pressure / swap floor / width
    0) parks mid-pass — slot capacity does not apply because the worker already
    holds its slot. ``WORKBAY_HOSTGOV_DISABLE`` and non-``enforce`` modes never
    park. Never raises (a probe failure returns None — the pass proceeds).

    The re-check runs under the pass's OWN cost class (resolved from ``backend``):
    a fully off-box COST_REMOTE lane is never gated by local memory, so it never
    parks on local pressure mid-pass — otherwise a multi-cycle grok-remote pass
    would defer on exactly the local condition the exemption ignores at dispatch
    (internal). Unknown/None backend => conservative heavy.
    """
    if os.environ.get("WORKBAY_HOSTGOV_DISABLE") == "1":
        return None
    try:
        from workbay_orchestrator_mcp.orchestration.backend_registry import (
            cost_class_for_backend,
        )
        from workbay_orchestrator_mcp.orchestration.host_resources import (
            evaluate_admission,
            load_host_memory_policy,
            probe_host,
            record_admission_telemetry,
        )

        policy = load_host_memory_policy(orchestrator_root)
        if policy.enforcement != "enforce":
            return None
        # Forward backend identity + live per-backend held ledger so the
        # per-backend local cap can evaluate (internal). Mid-pass only
        # parks on *refuse*; slots_full_outcome defaults to defer, so a saturated
        # peer does not abort a worker that already holds its own slot.
        held_slots_by_backend = None
        backend_key = str(backend) if backend else None
        if backend_key and policy.per_backend_local_cap > 0:
            try:
                from workbay_orchestrator_mcp.orchestration.host_resources import (  # noqa: PLC0415
                    count_held_backend_slots,
                    locks_root,
                )

                held_slots_by_backend = {
                    backend_key: count_held_backend_slots(
                        locks_root(orchestrator_root),
                        backend_key,
                        policy.per_backend_local_cap,
                    )
                }
            except Exception:  # noqa: BLE001 — ledger failure degrades to empty count
                held_slots_by_backend = {backend_key: 0}
        elif backend_key:
            held_slots_by_backend = {backend_key: 0}
        decision = evaluate_admission(
            probe_host(),
            cost_class_for_backend(backend),
            policy,
            held_slots=0,
            backend=backend_key,
            held_slots_by_backend=held_slots_by_backend,
        )
    except Exception:  # noqa: BLE001 — a probe failure must not abort a live pass
        return None
    if decision.decision != "refuse":
        return None
    record_admission_telemetry(orchestrator_root, decision, surface="pre_turn_recheck")
    return decision.reason


#: Stage markers carried on every outcome payload so the gate can branch without
#: git archaeology (internal). ``None`` means no failure stage (success
#: / pre-pass refusal / non-stage terminal like timeout with no phase fault).
FAILED_STAGES = frozenset(
    {
        "execute",
        "self_verify",
        "review",
        "handoff",
        "attestation",
    }
)


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


def build_checkpoint_commit_message(*, lane_id: str, checkpoint_number: int) -> str:
    """Pure builder for the checkpoint commit subject line.

    Neutral template only: lane id and checkpoint number. No vendor, backend,
    model, or trailer. ``lane_id`` is an *identifier*, not prose: sanitize for
    line-structure safety and length, but never run the commit-subject *prose*
    screen on it. That screen collapses credit-looking identifiers into the
    single fallback ``remote turn``, erasing lane identity from history
    (REVBYL-B-02). Identifier credit is handled by the lane-id sanitizer's
    discriminating digest path instead.
    """
    from workbay_orchestrator_mcp.orchestration.commit_subject import (  # noqa: PLC0415
        sanitize_lane_id_for_commit_message,
    )

    prefix = "wip(offload): "
    suffix = f" checkpoint {checkpoint_number}"
    # Git's hard wrap is on the whole subject, not on the lane_id component alone.
    max_lane_len = max(0, 72 - len(prefix) - len(suffix))
    safe_lane = sanitize_lane_id_for_commit_message(lane_id, max_len=max_lane_len)
    return f"{prefix}{safe_lane}{suffix}"


def _checkpoint_commit(
    worktree_path: Path,
    lane_id: str,
    checkpoint_number: int,
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
    message = build_checkpoint_commit_message(lane_id=lane_id, checkpoint_number=checkpoint_number)
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
    *,
    dry_run: bool = False,
) -> bool:
    """Checkpoint any dirty tree. Returns False only when the checkpoint failed.

    ``dry_run=True`` never mutates HEAD: dirty or clean, the probe reports success
    without writing so inspection-mode passes cannot land real commits
    (OFFLOAD-DRY-RUN-COMMITS-TO-THE-BRANCH-UNDER-REVIEW-01).
    """
    if dry_run:
        return True
    if not _worktree_dirty(worktree_path):
        return True
    sha = _checkpoint_commit(worktree_path, lane_id, len(checkpoints) + 1)
    if sha is None:
        return False
    checkpoints.append(sha)
    return True


def _remote_lane_key(branch: str) -> str:
    """Mirror ``remote_agent.sh`` LANE_KEY derivation (branch sanitise + 8-hex)."""
    import hashlib  # noqa: PLC0415
    import re  # noqa: PLC0415

    branch_hash = hashlib.sha256(branch.encode("utf-8", errors="surrogatepass")).hexdigest()[:8]
    key = re.sub(r"[^A-Za-z0-9-]", "-", branch)[:40]
    while key.startswith("-"):
        key = key[1:]
    key = key or "lane"
    return f"{key}-{branch_hash}"


def _co_located_remote_sandbox(worktree_path: Path) -> Path | None:
    """Best-effort path to a co-located remote_agent sandbox for this branch.

    When the orchestrator host is the same machine as ``WORKBAY_REMOTE_AGENT_ROOT``
    (gate layout), uncommitted remote work lives under
    ``$HOME/<agent_root>/<LANE_KEY>`` — not under the linked host worktree. Returns
    None when the sandbox is absent or unreadable.
    """
    branch = _git_stdout(worktree_path, "rev-parse", "--abbrev-ref", "HEAD")
    if not branch or branch == "HEAD":
        return None
    agent_root = (os.environ.get("WORKBAY_REMOTE_AGENT_ROOT") or "grok-sandbox").strip()
    if not agent_root or ".." in agent_root:
        return None
    root = Path(agent_root) if agent_root.startswith("/") else Path.home() / agent_root
    sbx = root / _remote_lane_key(branch)
    if not sbx.is_dir():
        return None
    if (sbx / ".git").exists() or (sbx / ".workbay-lane-sandbox").is_file():
        return sbx
    return None


def _backend_is_remote(backend: str | None) -> bool:
    """True for off-box remote backends whose agent writes outside the host worktree."""
    if not backend:
        return False
    try:
        from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
            backend_runs_self_verify_off_box,
        )

        if backend_runs_self_verify_off_box(backend):
            return True
    except Exception:  # noqa: BLE001 — registry must not break salvage
        pass
    return str(backend).endswith("-remote")


def _salvage_probe_paths(backend: str | None, worktree_path: Path) -> list[Path]:
    """Trees the end-of-pass salvage arm must probe.

    Remote backends: prefer the co-located remote sandbox (where the agent
    actually wrote), then the host worktree (exit-3 may have applied a patch
    there). Local backends: host worktree only.
    """
    host = Path(worktree_path)
    if not _backend_is_remote(backend):
        return [host]
    paths: list[Path] = []
    sbx = _co_located_remote_sandbox(host)
    if sbx is not None:
        paths.append(sbx)
    paths.append(host)
    # De-dupe while preserving order.
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path)
        try:
            key = str(path.resolve())
        except OSError:
            pass
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _salvage_checkpoint_if_dirty(
    backend: str | None,
    worktree_path: Path,
    lane_id: str,
    checkpoints: list[str],
    *,
    dry_run: bool = False,
) -> bool:
    """Checkpoint dirty trees across salvage probe paths. Fail only if a dirty tree could not commit."""
    if dry_run:
        return True
    any_failed = False
    for path in _salvage_probe_paths(backend, worktree_path):
        try:
            if not _checkpoint_if_dirty(path, lane_id, checkpoints, dry_run=False):
                any_failed = True
        except RuntimeError:
            any_failed = True
    return not any_failed


def _self_verify_is_inconclusive(self_verify_result: dict[str, Any] | None) -> bool:
    """True when the gate did not meaningfully run tests (unrun), not a red suite.

    Covers pytest zero-collection (exit 4/5), off-box ``not_run`` / harness
    errors, suite-lock deferral, and shell-level syntax rejection (including a
    double-HTML-escaped ``&&``). A genuine red suite must return False so the
    red-gate salvage arm stays separate from the unrun arm.

    Corroboration rule (OFFP-A-R1): ``unrun`` is a claim that no suite ran. Red
    evidence in the tail (pytest failure count / FAILED nodeid / FAILURES
    banner) **vetoes** every unrun signal — stamped outcome, tail regex, and
    inconclusive exit codes alike. Mixed evidence degrades to red (fail-safe),
    never to unrun.
    """
    if not isinstance(self_verify_result, dict):
        return False
    if self_verify_result.get("passed") or self_verify_result.get("skipped"):
        return False
    tail = str(self_verify_result.get("output_tail") or "")
    # Mixed evidence → red. Veto before any unrun promotion path.
    if _SELF_VERIFY_RED_TAIL_RE.search(tail):
        return False
    stamped = self_verify_result.get("self_verify_outcome")
    if stamped in _SELF_VERIFY_UNRUN_OUTCOMES:
        return True
    if _SELF_VERIFY_UNRUN_TAIL_RE.search(tail):
        return True
    try:
        exit_code = int(self_verify_result.get("exit_code"))
    except (TypeError, ValueError):
        return False
    return exit_code in _SELF_VERIFY_INCONCLUSIVE_EXIT_CODES


def _self_verify_gate_status(self_verify_result: dict[str, Any] | None) -> str | None:
    """Typed gate status for the pass surface: ``passed`` / ``skipped`` / ``unrun`` / ``red``.

    Promotes the unrun-vs-red distinction so orchestrators can branch without
    parsing ``output_tail`` prose (findings 3 and 4).
    """
    if not isinstance(self_verify_result, dict):
        return None
    if self_verify_result.get("skipped"):
        return "skipped"
    if self_verify_result.get("passed"):
        return "passed"
    if _self_verify_is_inconclusive(self_verify_result):
        return "unrun"
    return "red"


# Single source of truth for the max-turns execute contract string (E8FIX-R2-B-01 /
# REF-26). Both the early execute-stop arm and the review→execute reclassification
# arm must use this helper so the bare phrasing and the optional named-cause
# suffix cannot diverge.
_MAX_TURNS_EXECUTE_ERROR_BASE = "execute stopped: max turns reached"


def _max_turns_execute_error_reason(named_exec: str = "") -> str:
    """Contract string for a max_turns execute stop.

    ``named_exec`` is the stripped ``run_ctx.execute_error`` text when the
    adapter-exception path wrote it (worker_daemon run_lane_exec except). On the
    marker path ``execute_error`` stays unset and the bare base string is used.
    """
    named = (named_exec or "").strip()
    if named:
        return f"{_MAX_TURNS_EXECUTE_ERROR_BASE} ({named})"
    return _MAX_TURNS_EXECUTE_ERROR_BASE


def _handoff_carries_genuine_question(
    *,
    error_reason: str | None,
    run_ctx: Any,
    wd: Any,
) -> bool:
    """Fail-closed detector: only reclassify completed_unreviewed when no real ask.

    Non-empty structured blockers, or an error that is more specific than the
    generic blocked-handoff template, count as a genuine question.
    """
    blockers: list[str] = []
    try:
        result_path = getattr(run_ctx, "final_result_path", None)
        if result_path is not None:
            probe = wd._load_result(Path(result_path))
            raw_blockers = probe.get("blockers") if isinstance(probe, dict) else None
            if isinstance(raw_blockers, list):
                blockers = [str(b).strip() for b in raw_blockers if str(b).strip()]
    except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError, AttributeError):
        blockers = []
    if blockers:
        return True
    text = (error_reason or "").strip()
    if not text:
        return False
    lowered = text.casefold()
    # Real verification / review-product failures are genuine blockers.
    genuine_markers = (
        "lane verification failed",
        "no parseable findings",
        "record_failed",
        "scope_violation",
        "claim_contradicted_by_evidence",
    )
    if any(marker in lowered for marker in genuine_markers):
        return True
    # Generic blocked-handoff template with no structured blockers is NOT a question
    # (the 8/8 false-negative shape: green committed work, empty ask).
    if lowered.startswith("worker handed a blocked result back for guidance"):
        return False
    # Anything more specific is treated as a real ask / blocker (fail-closed).
    return True


def _ceremony_status_for(run_ctx: Any) -> str:
    """Classify handoff/reporting ceremony from run_ctx (always one of CEREMONY_STATUSES).

    ``handoff_exit`` defaults to 1 (fail-closed) even when no submit was attempted,
    so ``handoff_attempted`` is the required precondition for failed/clean.
    """
    if run_ctx is None or not getattr(run_ctx, "handoff_attempted", False):
        return CEREMONY_STATUS_NOT_ATTEMPTED
    exit_code = getattr(run_ctx, "handoff_exit", None)
    if exit_code in (0, None):
        return CEREMONY_STATUS_CLEAN
    return CEREMONY_STATUS_FAILED


def _work_status_for(commit_landed: bool) -> str:
    return WORK_STATUS_LANDED if commit_landed else WORK_STATUS_NOT_LANDED


def _self_verify_not_failed(self_verify_result: dict[str, Any] | None) -> bool:
    """True when self_verify is absent, skipped, or passed (not a red suite)."""
    if not isinstance(self_verify_result, dict):
        return True
    if self_verify_result.get("skipped"):
        return True
    return bool(self_verify_result.get("passed"))


def _final_result_merge_ready_clean(run_ctx: Any, wd: Any) -> bool:
    """True when the lane's final result is merge_ready with no structured blockers.

    Used to stop a green, merge-ready worker reply from being stuck as
    needs_guidance solely because a findings block was absent or the generic
    blocked-handoff template was applied (ENGINE-NEEDS-GUIDANCE-MISREPORTS…).
    """
    try:
        result_path = getattr(run_ctx, "final_result_path", None)
        if result_path is None:
            return False
        probe = wd._load_result(Path(result_path))
        if not isinstance(probe, dict):
            return False
        if str(probe.get("handoff_action") or "").strip() != "merge_ready":
            return False
        raw_blockers = probe.get("blockers")
        if isinstance(raw_blockers, list) and any(str(b).strip() for b in raw_blockers):
            return False
        return True
    except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError, AttributeError):
        return False


def _maybe_reclassify_completed_unreviewed(
    *,
    outcome: str,
    failed_stage: str | None,
    commit_landed: bool,
    self_verify_result: dict[str, Any] | None,
    error_reason: str | None,
    run_ctx: Any,
    wd: Any,
    task_ref: str | None = None,
    lane_id: str | None = None,
    baseline_report_id: int = 0,
) -> tuple[str, str | None, str | None]:
    """OL-PF2 fail-closed guard for ``completed_unreviewed``.

    May reclassify only when:
    - outcome is needs_guidance
    - self_verify passed (or was a no-op skip / absent)
    - commit_landed is true
    - failed_stage is NOT execute or self_verify
    - handoff carries no genuine question
      OR the final result is merge_ready with empty blockers (engoutcome finding 2)
    - latest worker report is not a composer-violation handoff

    Returns (outcome, failed_stage, error_reason).
    """
    if outcome != "needs_guidance":
        return outcome, failed_stage, error_reason
    if failed_stage in ("execute", "self_verify"):
        return outcome, failed_stage, error_reason
    if not commit_landed:
        return outcome, failed_stage, error_reason
    if not _self_verify_not_failed(self_verify_result):
        return outcome, failed_stage, error_reason
    # Fail closed without identity: report-backed guards cannot run safely
    # (SECD-05). Skipping them would reclassify without consulting durable evidence.
    if not task_ref or not lane_id:
        return outcome, failed_stage, error_reason
    # Load the fresh report once for composer + durable-blocker checks. Do not
    # depend on run_ctx.final_result_path here: a successful blocked handoff
    # deletes that file after persisting blockers into worker_reports (OBS-08).
    report = _fresh_worker_report(task_ref, lane_id, baseline_report_id)
    # Composer violation must never look like green completed_unreviewed (and
    # must stay needs_guidance so salvage stays gated off that report shape).
    if report is not None and _is_composer_violation_handoff_report(report):
        return outcome, failed_stage, error_reason
    # Real structured blockers survive only on the durable report after handoff
    # success cleans the result file — refuse reclass when any are present (AGT-10).
    if report is not None and _report_carries_blockers(report):
        return outcome, failed_stage, error_reason
    # Green merge_ready with empty blockers is never a genuine "blocked for
    # guidance" result — even when the engine error string mentions a missing
    # findings block or the generic blocked template (engoutcome finding 2).
    if _final_result_merge_ready_clean(run_ctx, wd):
        return "completed_unreviewed", None, None
    if _handoff_carries_genuine_question(error_reason=error_reason, run_ctx=run_ctx, wd=wd):
        return outcome, failed_stage, error_reason
    return "completed_unreviewed", None, None


def _maybe_reclassify_ceremony_failed(
    *,
    outcome: str,
    failed_stage: str | None,
    commit_landed: bool,
    self_verify_result: dict[str, Any] | None,
    run_ctx: Any,
) -> str:
    """Reclassify error → ceremony_failed when work landed but handoff submit failed.

    Fail-closed: never widens success enums. Only rewrites a pure ``error`` that
    would otherwise claim the lane produced nothing when commit_landed is true
    and the only terminal failure is the handoff/reporting ceremony
    (ENGINE-HANDOFF-SUBMIT-FAILURE-REPORTS-ERROR-FOR-COMPLETED-LANE).
    """
    if outcome != "error":
        return outcome
    if not commit_landed:
        return outcome
    if failed_stage in ("execute", "self_verify"):
        return outcome
    if not _self_verify_not_failed(self_verify_result):
        return outcome
    if run_ctx is None or not getattr(run_ctx, "handoff_attempted", False):
        return outcome
    if getattr(run_ctx, "handoff_exit", 0) in (0, None):
        return outcome
    return "ceremony_failed"


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


class _BlockerEvidence(NamedTuple):
    """Decoded ``blockers_json`` with unreadable as a first-class state.

    ``blockers`` is empty when the payload is unreadable or genuinely empty —
    callers that need "do blockers exist?" must consult ``readable`` (and
    ``present``) rather than treating empty as absence of evidence (OBS-08).

    ``blockers`` keeps only strings for marker scanning (composer / unparseable
    prefixes). ``any_non_empty_item`` is the fail-closed safety answer over
    *all* decoded list items with ``any(str(i).strip() for i in items)`` so a
    structured blocker such as ``[{"msg": "disk full"}]`` still carries.
    """

    blockers: tuple[str, ...]  # string blockers only — marker scanning
    readable: bool
    present: bool
    any_non_empty_item: bool  # any(str(i).strip() for i in <all items>)


def _decode_blockers_json(report: dict[str, Any]) -> _BlockerEvidence:
    """Total decode of durable ``blockers_json``; never raises.

    Handles null / 42 / true / corrupt text / missing key / pre-decoded lists.
    ``readable=False`` means the payload did not decode to a JSON list — that
    is distinct from an empty list of blockers.
    """
    if "blockers_json" not in report:
        # Key absent: not present. Readable empty so the missing-key fail-open
        # in _report_carries_blockers stays intentional (see that helper).
        return _BlockerEvidence(
            blockers=(), readable=True, present=False, any_non_empty_item=False
        )

    raw = report.get("blockers_json")
    if raw is None:
        # SQL NULL / Python None stored in the column: present, empty, readable.
        return _BlockerEvidence(
            blockers=(), readable=True, present=True, any_non_empty_item=False
        )

    if isinstance(raw, list):
        # String filter is correct for marker scanning; safety uses all items.
        blockers = tuple(str(item) for item in raw if isinstance(item, str))
        any_item = any(str(item).strip() for item in raw)
        return _BlockerEvidence(
            blockers=blockers,
            readable=True,
            present=True,
            any_non_empty_item=any_item,
        )

    if not isinstance(raw, str):
        # Non-string durable cell (int/bool/dict): unreadable, not a list.
        return _BlockerEvidence(
            blockers=(), readable=False, present=True, any_non_empty_item=False
        )

    text = raw.strip()
    if not text or text == "null":
        return _BlockerEvidence(
            blockers=(), readable=True, present=True, any_non_empty_item=False
        )

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return _BlockerEvidence(
            blockers=(), readable=False, present=True, any_non_empty_item=False
        )

    if parsed is None:
        return _BlockerEvidence(
            blockers=(), readable=True, present=True, any_non_empty_item=False
        )

    if not isinstance(parsed, list):
        # Valid JSON that is not a list (42, true, {}, ...): unreadable as blockers.
        return _BlockerEvidence(
            blockers=(), readable=False, present=True, any_non_empty_item=False
        )

    # String filter is correct for marker scanning; safety uses all items.
    blockers = tuple(str(item) for item in parsed if isinstance(item, str))
    any_item = any(str(item).strip() for item in parsed)
    return _BlockerEvidence(
        blockers=blockers,
        readable=True,
        present=True,
        any_non_empty_item=any_item,
    )


def _parse_worker_report_blockers(report: dict[str, Any]) -> list[str]:
    """Return decoded string blockers; total over any ``blockers_json`` shape.

    Never raises. Unreadable payloads yield ``[]`` — callers that need to
    distinguish unreadable from empty must use ``_decode_blockers_json``.
    """
    return list(_decode_blockers_json(report).blockers)


def _report_carries_blockers(report: dict[str, Any]) -> bool:
    """True when the durable worker report carries any non-empty blocker.

    Used by the completed_unreviewed reclassifier so genuine asks survive after
    a successful blocked handoff deletes the result file. Fail-closed on
    unparseable or non-list blocker payloads: malformed evidence is not evidence
    of absence (OBS-08). Empty / missing / whitespace-only / JSON null / ``[]``
    are treated as no blockers.

    List items are judged with the original total rule
    ``any(str(item).strip() for item in parsed)`` via
    ``any_non_empty_item`` — not the string-only ``blockers`` tuple used for
    marker scanning. Structured items such as ``[{"msg": "..."}]`` still carry.

    Missing-key fail-open: production ``_fresh_worker_report`` does
    ``SELECT *`` then ``dict(sqlite3.Row)``, so the ``blockers_json`` column is
    always present on real loader rows. The ``not present`` branch is only
    reachable from hand-built / mocked dicts; flipping it would churn tests
    for no production gain. The SELECT * invariant is pinned in tests.
    """
    evidence = _decode_blockers_json(report)
    if not evidence.present:
        return False  # missing-key fail-open, unchanged, still commented
    if not evidence.readable:
        return True  # OBS-08, unchanged
    return evidence.any_non_empty_item


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
    evidence = _decode_blockers_json(report)
    if not evidence.readable:
        # Unreadable blockers_json is not absence of the unparseable marker.
        # With a matching summary, keep the salvage route open rather than
        # fail-opening to an empty list and silently dropping the candidate.
        return True
    return any(_UNPARSEABLE_BLOCKER_PREFIX in blocker for blocker in evidence.blockers)


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

    passing_test_raw = evidence.get("passing_test")
    passing_test = passing_test_raw if isinstance(passing_test_raw, dict) else {}
    test_id = passing_test.get("id", "unknown")
    commits_raw = evidence.get("commit_shas")
    commits = commits_raw if isinstance(commits_raw, list) else []
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


def _collect_pass_findings(*, task_ref: str, limit: int = 50) -> list[dict[str, Any]]:
    """Return worker-recorded in-lane findings for the pass payload (T4 / [OBS-04]).

    Surfaces BR-* rows the worker wrote via MCP so the orchestrator does not need
    close-check archaeology after a degraded smoke review.
    """
    try:
        from workbay_handoff_mcp.review_findings_queries import list_review_findings  # noqa: PLC0415
    except Exception:  # pragma: no cover - optional import degrade
        return []
    try:
        envelope = list_review_findings(task_ref=task_ref, status="open", limit=limit, detail="summary")
    except Exception:  # pragma: no cover - I/O degrade; never fail the pass on listing
        return []
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else envelope
    if not isinstance(data, dict):
        return []
    raw = data.get("findings")
    if not isinstance(raw, list):
        return []
    findings: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        findings.append(
            {
                "finding_id": item.get("finding_id"),
                "severity": item.get("severity"),
                "file_path": item.get("file_path"),
                "description": item.get("description"),
                "status": item.get("status"),
            }
        )
    return findings


# ---------------------------------------------------------------------------
# implementation note R2: harvest GROK_REVIEW_FINDINGS_JSON from review-lane terminal results
# ---------------------------------------------------------------------------

_GROK_REVIEW_FINDINGS_MARKER = "GROK_REVIEW_FINDINGS_JSON"
_MARKER_LINE_RE = re.compile(rf"(?m)^{re.escape(_GROK_REVIEW_FINDINGS_MARKER)}\s*$")
# Opening fence only — do NOT match a closing fence with a non-greedy span:
# real finding descriptions can embed ``` (code-fence examples) which would
# truncate a ``.*?````` capture mid-JSON (R1 fixture, attribution-parsing item).
_OPEN_FENCE_RE = re.compile(r"^```(?:json)?[ \t]*\r?\n", re.IGNORECASE)
_VALID_HARVEST_SEVERITIES = frozenset({"high", "medium", "low"})


def _extract_findings_json(text: str) -> tuple[Any | None, str]:
    """Locate ``GROK_REVIEW_FINDINGS_JSON`` and parse the following JSON value.

    Returns ``(parsed, status)`` where status is one of:
    ``no_marker``, ``unparseable_block``, ``ok``.

    After the bare marker line, accept an optional opening ```` ```json ```` /
    ```` ``` ```` fence then ``json.JSONDecoder.raw_decode`` the payload. Using
    raw_decode (not closing-fence scan) tolerates nested ``` inside description
    strings (implementation note R1 fixture).
    """
    if not isinstance(text, str) or not text:
        return None, "no_marker"
    match = _MARKER_LINE_RE.search(text)
    if match is None:
        return None, "no_marker"
    rest = text[match.end() :]
    # Drop a single leading newline after the marker line.
    if rest.startswith("\r\n"):
        rest = rest[2:]
    elif rest.startswith("\n"):
        rest = rest[1:]
    if not rest.strip():
        return None, "unparseable_block"
    open_fence = _OPEN_FENCE_RE.match(rest)
    if open_fence is not None:
        rest = rest[open_fence.end() :]
    try:
        parsed, _end = json.JSONDecoder().raw_decode(rest.lstrip())
    except (json.JSONDecodeError, ValueError, TypeError):
        return None, "unparseable_block"
    return parsed, "ok"


def _tolerant_harvest_items(raw_items: Any) -> tuple[list[dict[str, Any]], int]:
    """Keep items with non-empty file_path + description; free-form categories.

    Intentionally does **not** use ``review_runner._validate_review_result``:
    real review-lane categories are free-form (``contract-drift``, …) and the
    self-review enum would drop every finding ([DATA-03] / implementation note R2).
    Severity is coerced to {high,medium,low} defaulting medium; category
    defaults to ``GAP`` when absent.
    """
    if not isinstance(raw_items, list):
        return [], 0
    kept: list[dict[str, Any]] = []
    dropped = 0
    for item in raw_items:
        if not isinstance(item, dict):
            dropped += 1
            continue
        file_path = item.get("file_path")
        description = item.get("description")
        if not isinstance(file_path, str) or not file_path.strip():
            dropped += 1
            continue
        if not isinstance(description, str) or not description.strip():
            dropped += 1
            continue
        severity_raw = item.get("severity")
        severity = (
            severity_raw if isinstance(severity_raw, str) and severity_raw in _VALID_HARVEST_SEVERITIES else "medium"
        )
        category_raw = item.get("category")
        category = category_raw.strip() if isinstance(category_raw, str) and category_raw.strip() else "GAP"
        normalized: dict[str, Any] = {
            "severity": severity,
            "category": category,
            "file_path": file_path.strip(),
            "description": description.strip(),
        }
        line_start = item.get("line_start")
        if isinstance(line_start, int):
            normalized["line_start"] = line_start
        line_end = item.get("line_end")
        if isinstance(line_end, int):
            normalized["line_end"] = line_end
        fix = item.get("fix")
        if isinstance(fix, str):
            normalized["fix"] = fix
        kept.append(normalized)
    return kept, dropped


def _rematerialize_details_artifact(source_id: int, orchestrator_root: Path) -> str | None:
    """Reassemble dematerialized details from the artifact sidecar. Fail-open → None."""
    try:
        from workbay_handoff_mcp import (
            RuntimeConfig,  # noqa: PLC0415
            artifact_index,  # noqa: PLC0415
        )

        artifact_db_path = RuntimeConfig.for_repo(orchestrator_root).artifact_db_path
        source = artifact_index.get_artifact_source(
            source_id=int(source_id),
            artifact_db_path=artifact_db_path,
        )
        if not isinstance(source, dict):
            return None
        chunks = source.get("chunks") or []
        if not chunks:
            return None
        parts: list[str] = []
        for chunk in chunks:
            if isinstance(chunk, dict) and isinstance(chunk.get("body"), str):
                parts.append(chunk["body"])
        if not parts:
            return None
        return "\n".join(parts)
    except Exception:  # noqa: BLE001 — artifact_unavailable: never raise from harvest
        return None


def _report_scan_text(report: dict[str, Any] | str | None) -> str | None:
    """Scannable text from a worker report carrier (expected miss for real reviews)."""
    if report is None:
        return None
    if isinstance(report, str):
        return report if report.strip() else None
    if not isinstance(report, dict):
        return None
    details = report.get("details")
    if isinstance(details, str) and details.strip():
        return details
    return None


def _parse_findings_from_text(text: str) -> tuple[str, list[dict[str, Any]] | None, int]:
    """Scan one carrier text.

    Returns ``(status, findings_or_none, dropped)`` where status is one of:
    ``no_marker``, ``unparseable_block``, ``no_valid_findings``, ``ok``.
    """
    parsed, extract_status = _extract_findings_json(text)
    if extract_status == "no_marker":
        return "no_marker", None, 0
    if extract_status == "unparseable_block" or parsed is None:
        return "unparseable_block", None, 0
    # A successfully-decoded NON-LIST root (object/string/number/bool) is not a
    # findings array — treat it as an invalid block, not a "reviewed clean" empty
    # array (implementation note review, grok/B2: else it false-types review_complete).
    if not isinstance(parsed, list):
        return "unparseable_block", None, 0
    kept, dropped = _tolerant_harvest_items(parsed)
    if not kept:
        return "no_valid_findings", None, dropped
    return "ok", kept, dropped


def _lane_kind(orchestrator_root: Path, task_ref: str, lane_id: str) -> tuple[str | None, bool]:
    """Return (kind, confirmed). confirmed only when MCP params carry an explicit kind.

    Fail-closed: unresolved lookup (exception, missing params, or invalid kind)
    returns ``(None, False)`` — never fabricates the domain default ``"implement"``
    (cs0166-r07-10 / DOM-06). Callers must refuse implement-only and review-only
    routing until kind is confirmed; treating a blipped review lane as implement
    would run TEST_CMD and misclassify clean-tree needs_guidance as transport failure.
    """
    try:
        from workbay_orchestrator_mcp.orchestration import worker_daemon as _wd  # noqa: PLC0415

        params = _wd._fetch_mcp_lane_params(Path(orchestrator_root), task_ref, lane_id)
        if not isinstance(params, dict):
            return None, False
        kind = params.get("lane_kind")
        if kind in ("implement", "review"):
            return str(kind), True
        return None, False
    except Exception:  # noqa: BLE001 — unresolved is typed unavailable, not implement
        return None, False


def _claim_contradicted_by_evidence(exec_probe: dict[str, Any], raw_payload: Any) -> bool:
    """True when structured payload claims finished work under a clean, unmoved tree.

    Structured signals only — never NLP-match the free-form summary alone (implementation note S4).
    """
    if bool(exec_probe.get("tests_run")):
        return True
    if isinstance(raw_payload, dict):
        if raw_payload.get("handoff_action") == "merge_ready":
            return True
        if raw_payload.get("merge_ready") is True:
            return True
    return False


# Machine-readable harvest verdicts threaded onto the pass payload (fail-closed
# review contract). Only ``recorded`` and ``reviewed_clean`` may produce a
# passing review outcome; ``harvest_failed`` must never look like either.
# ``not_attempted`` is emitted only for non-review terminals (no review product
# expected) and is intentionally excluded from _PASSING_HARVEST_VERDICTS so it
# can never promote a lane to review_complete.
HARVEST_VERDICT_RECORDED = "recorded"
HARVEST_VERDICT_REVIEWED_CLEAN = "reviewed_clean"
HARVEST_VERDICT_HARVEST_FAILED = "harvest_failed"
HARVEST_VERDICT_NOT_ATTEMPTED = "not_attempted"
_PASSING_HARVEST_VERDICTS = frozenset(
    {HARVEST_VERDICT_RECORDED, HARVEST_VERDICT_REVIEWED_CLEAN}
)


def _harvest_verdict(harvest: dict[str, Any] | None) -> str:
    """Stable typed harvest verdict from harvest *data* [AGT-21][OBS-08][SECD-05].

    - ``recorded``: ≥1 finding parsed and persisted (status == recorded).
    - ``reviewed_clean``: well-formed EMPTY findings array (reason
      ``no_valid_findings`` with ``dropped == 0``) — genuine reviewed-and-found-nothing.
    - ``harvest_failed``: every other case — missing marker, unparseable JSON,
      all-malformed array (``dropped > 0``), persist failure, or non-dict harvest.
      Never eligible for ``review_complete``; must not be collapsed into the
      empty-findings shape of ``reviewed_clean``.

    Callers that emit onto the pass payload must further gate with
    ``review_product_harvest``: non-review terminals emit
    ``HARVEST_VERDICT_NOT_ATTEMPTED`` instead of ``harvest_failed`` so
    implement lanes never look like broken review capture.
    """
    if not isinstance(harvest, dict):
        return HARVEST_VERDICT_HARVEST_FAILED
    if harvest.get("status") == "recorded":
        return HARVEST_VERDICT_RECORDED
    if harvest.get("reason") == "no_valid_findings" and int(harvest.get("dropped") or 0) == 0:
        return HARVEST_VERDICT_REVIEWED_CLEAN
    return HARVEST_VERDICT_HARVEST_FAILED


def _harvest_block_parsed(harvest: dict[str, Any] | None) -> bool:
    """True when harvest found a *clean well-formed* GROK_REVIEW_FINDINGS_JSON block (implementation note R3).

    ``recorded`` (>=1 finding) or an EMPTY array (``no_valid_findings`` with
    ``dropped == 0`` — "reviewed clean") both mean a reviewer emitted a well-formed
    block → review_complete. A missing marker (``no_findings_block``), invalid /
    non-list JSON (``unparseable_block``), an ALL-MALFORMED array
    (``no_valid_findings`` with ``dropped > 0`` — a broken reviewer), or a persist
    failure (``record_failed``) is NOT a clean block and must not type
    review_complete (grok/B2 false-success guard).
    """
    return _harvest_verdict(harvest) in _PASSING_HARVEST_VERDICTS


def _safe_final_result(run_ctx: Any) -> dict[str, Any]:
    """Best-effort load of the worker's terminal result JSON; ``{}`` on any failure."""
    try:
        from workbay_orchestrator_mcp.orchestration import worker_daemon as _wd  # noqa: PLC0415

        if getattr(run_ctx, "final_result_path", None):
            loaded = _wd._load_result(Path(run_ctx.final_result_path))
            return loaded if isinstance(loaded, dict) else {}
    except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError):
        pass
    return {}


def _safe_latest_worker_report(task_ref: str, lane_id: str) -> dict[str, Any] | str | None:
    """Best-effort latest worker report for harvest; ``None`` on any failure (implementation note)."""
    try:
        return _latest_worker_report(task_ref, lane_id)
    except Exception:  # noqa: BLE001 — report load is best-effort for harvest
        return None


def _harvest_review_findings(
    result: dict[str, Any],
    report: dict[str, Any] | str | None,
    *,
    task_ref: str,
    lane_id: str | None,
    session: str,
    orchestrator_root: Path,
) -> dict[str, Any]:
    """Scan terminal pass result for ``GROK_REVIEW_FINDINGS_JSON`` and record findings.

    Fail-open: never raises, never mutates pass outcome. Carrier order (first
    that yields ≥1 parseable finding wins): details → raw_payload.details →
    details_artifact_ref rematerialization → report body (implementation note R1/R2).
    """
    try:
        if not isinstance(result, dict):
            result = {}

        # implementation note D9: harvest skips any result whose result_parse is not "ok".
        from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
            review_result_is_harvestable,
        )

        if not review_result_is_harvestable(result):
            return {"status": "skipped", "reason": "result_parse_degraded"}

        carriers: list[tuple[str, str | None]] = []
        details = result.get("details")
        carriers.append(("details", details if isinstance(details, str) else None))

        raw_payload = result.get("raw_payload")
        if isinstance(raw_payload, dict):
            raw_details = raw_payload.get("details")
            carriers.append(("raw_payload", raw_details if isinstance(raw_details, str) else None))
        else:
            carriers.append(("raw_payload", None))

        artifact_text: str | None = None
        artifact_ref = result.get("details_artifact_ref")
        if artifact_ref is not None:
            try:
                artifact_text = _rematerialize_details_artifact(int(artifact_ref), Path(orchestrator_root))
            except (TypeError, ValueError):
                artifact_text = None
        carriers.append(("artifact", artifact_text))
        carriers.append(("report", _report_scan_text(report)))

        saw_unparseable = False
        saw_no_valid = False
        last_dropped = 0

        for carrier_name, text in carriers:
            if not text:
                continue
            status, findings, dropped = _parse_findings_from_text(text)
            if status == "no_marker":
                continue
            if status == "unparseable_block":
                # Marker present but unparseable here — remember it and keep trying
                # later, more-authoritative carriers. A truncated top-level
                # ``details`` can hold a partial marker while ``raw_payload.details``
                # / the rematerialized artifact hold the full block; short-circuiting
                # here would drop harvestable findings (reported post-loop only if
                # no carrier parses).
                saw_unparseable = True
                continue
            if status == "no_valid_findings":
                saw_no_valid = True
                last_dropped = dropped
                continue
            # status == "ok"
            assert findings is not None
            try:
                from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                    _record_findings,
                )

                finding_ids = _record_findings(
                    findings,
                    task_ref=task_ref,
                    session=session,
                    lane_id=lane_id,
                    orchestrator_root=Path(orchestrator_root),
                )
            except Exception:  # noqa: BLE001 — harvest must never raise
                return {"status": "skipped", "reason": "record_failed"}
            return {
                "status": "recorded",
                "count": len(finding_ids),
                "finding_ids": list(finding_ids),
                "carrier": carrier_name,
                "dropped": dropped,
            }

        if saw_unparseable:
            return {"status": "skipped", "reason": "unparseable_block"}
        if saw_no_valid:
            return {"status": "skipped", "reason": "no_valid_findings", "dropped": last_dropped}
        return {"status": "skipped", "reason": "no_findings_block"}
    except Exception:  # noqa: BLE001 — ultimate fail-open for the pass
        return {"status": "skipped", "reason": "record_failed"}


# implementation note R9: a grok pass can die pre-work on a backend-INTERNAL fault (e.g.
# ``max_tokens_truncation``, a provider rate-limit / overload / 5xx). The engine's
# no-auto-retry policy is correct, but a bare ``error`` outcome forces the
# coordinator into log forensics before it can re-dispatch. Classify these as a
# ``backend_transient`` discriminator on the error payload so re-dispatch is a
# mechanical decision (the outcome stays ``error`` — no new enum value).
_BACKEND_TRANSIENT_PATTERNS = re.compile(
    r"(?i)(max_tokens_truncation|max[-_ ]?tokens|truncat|rate[-_ ]?limit|\b429\b|overloaded"
    r"|temporarily unavailable|\b50[023]\b|internal server error|connection reset|connection error"
    r"|service unavailable|upstream (?:error|timeout))"
)


def _is_backend_transient_error(text: str | None) -> bool:
    """implementation note R9: True when an ``error`` outcome's text names a backend-internal
    transient fault safe to mechanically re-dispatch (not a code/worker fault)."""
    if not text:
        return False
    return _BACKEND_TRANSIENT_PATTERNS.search(str(text)) is not None


# implementation note R7: the engine spawns these scripts as subprocesses from the imported
# module's own dir (``SCRIPT_DIR = Path(__file__).parent`` in worker_daemon /
# lane_exec). If a concurrent env flip deletes that source after import, the spawn
# fails as a bare ``lane_prompt.py --check failed (exit 2)`` crash mid-pass.
_ENGINE_CRITICAL_SCRIPTS: tuple[str, ...] = ("offload_pass.py", "lane_prompt.py", "worker_daemon.py")


def _engine_source_integrity_note(engine_dir: Path | None = None) -> str | None:
    """implementation note R7: verify the engine's own on-disk source still exists.

    Returns a remedy string naming ``server_stale_restart_required`` when a
    critical engine script has vanished from the module dir since import (the
    concurrent-env-flip incident), else ``None``. ``engine_dir`` defaults to this
    module's own directory; it is a parameter only so the check is unit-testable.
    Existence is a distinct signal from version/commit skew (already surfaced at
    startup by handoff ``package_skew.emit_src_installed_skew_startup_log``), so
    this is not a forked fingerprint mechanism ([REF-19]).
    """
    resolved = engine_dir if engine_dir is not None else Path(__file__).resolve().parent
    missing = [name for name in _ENGINE_CRITICAL_SCRIPTS if not (resolved / name).exists()]
    if not missing:
        return None
    return (
        "server_stale_restart_required: the orchestrator engine's on-disk source no "
        f"longer exists ({', '.join(missing)} missing under {resolved}); a concurrent "
        "environment flip (e.g. a dev-redirect .pth removing the installed package) "
        "invalidated the running server. Restart the MCP orchestrator server before "
        "dispatching further offload passes."
    )


# implementation note R5: close-time package-smoke wall-clock cap (per touched package).
# A slice whose full-package suite fits under the cap is smoked at closure; a
# suite that overruns degrades to a typed skip (never an unbounded run).
_PACKAGE_SMOKE_WALL_CLOCK_CAP_SECONDS = 300


def _touched_packages(changed_files: list[str] | None, worktree_path: Path) -> dict[str, Path]:
    """Map a slice's ``changed_files`` to the ``packages/<name>/tests`` dirs it
    touched (only packages that actually ship a ``tests`` dir). Sorted by name
    for determinism. implementation note R5."""
    packages: dict[str, Path] = {}
    for entry in changed_files or []:
        parts = str(entry).split("/")
        if len(parts) >= 2 and parts[0] == "packages":
            name = parts[1]
            tests_dir = worktree_path / "packages" / name / "tests"
            if tests_dir.is_dir():
                packages[name] = tests_dir
    return dict(sorted(packages.items()))


def _package_smoke(
    worktree_path: Path,
    changed_files: list[str] | None,
    *,
    cap_seconds: int = _PACKAGE_SMOKE_WALL_CLOCK_CAP_SECONDS,
    python_bin: str | None = None,
) -> tuple[bool, str | None]:
    """Close-time package smoke (implementation note R5) — run each touched package's FULL
    test dir once at slice close so a slice that breaks its own package fails
    HERE, not at the merge gate (the 0108 SWEEP-01 shape: green scoped-suite,
    red package).

    Returns ``(ok, note)``:
    - ``(False, note)`` when a touched-package suite FAILS — BLOCKING; the caller
      records no closure and preserves the commit.
    - ``(True, note)`` when the wall-clock cap trips or the runner is unavailable
      — a typed non-blocking degrade (``smoke_skipped_too_slow`` / ``smoke_skipped``);
      the scoped self-verify already stands. [OBS-08] names the missing coverage.
    - ``(True, None)`` when every touched-package suite passes, or nothing to smoke.

    Bounded by construction: touched packages only, close-time only (not
    per-cycle), each run capped — never an unbounded sweep.
    """
    packages = _touched_packages(changed_files, Path(worktree_path))
    if not packages:
        return True, None
    py = python_bin or str(Path(worktree_path) / ".venv" / "bin" / "python")
    # internal D4: serialize the close-time smoke behind the global suite
    # lock — the worktree's git-common-dir resolves to the shared lock, so a
    # smoke run and a concurrent lane's self-verify never spike memory together.
    # A lock timeout degrades non-blocking (the scoped self-verify already
    # stands); the bulkhead is a no-op when disabled.
    from workbay_orchestrator_mcp.orchestration.host_resources import (
        SuiteLockTimeout,
        acquire_suite_bulkhead,
    )

    try:
        suite_fd = acquire_suite_bulkhead(Path(worktree_path))
    except SuiteLockTimeout as exc:
        return True, f"smoke_skipped_suite_lock_timeout: {exc}; the scoped self-verify stands"
    try:
        for name, tests_dir in packages.items():
            try:
                proc = subprocess.run(
                    [py, "-m", "pytest", str(tests_dir), "-q", "-p", "no:cacheprovider"],
                    cwd=str(worktree_path),
                    capture_output=True,
                    text=True,
                    timeout=cap_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                return True, (
                    f"smoke_skipped_too_slow: package {name!r} suite exceeded the "
                    f"{cap_seconds}s close-time cap; the scoped self-verify stands"
                )
            except OSError:
                return True, f"smoke_skipped: package {name!r} suite could not run ({py} unavailable)"
            if proc.returncode != 0:
                tail = "\n".join((proc.stdout or "").strip().splitlines()[-15:])
                return False, (
                    f"package_smoke_failed: package {name!r} full test suite failed at slice close "
                    f"(a slice must not break its own package; green scoped-suite is not enough)\n{tail}"
                )
        return True, None
    finally:
        if suite_fd is not None:
            os.close(suite_fd)


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

    # implementation note R5: close-time package smoke. A slice that broke its own package
    # (out-of-scope of the worker's scoped TEST_CMD) must fail at closure, not
    # slip through to the merge gate. Blocking on a real red (no closure, commit
    # preserved); the cap degrade is the only non-blocking path.
    smoke_ok, smoke_note = _package_smoke(worktree_path, changed_files)
    if not smoke_ok:
        return None, smoke_note

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
    closure_result: dict[str, Any] = {
        "recorded": True,
        "decision": decision_id,
        "commit_sha": head,
        "worker_report_id": report.get("id"),
        "test_commands": [str(command) for command in test_commands],
        "merge_ready": merge_ready,
        "changed_files": changed_files or [],
    }
    # Propagate close_slice partial/degrade so a degraded close is not reported
    # as clean success ([RES-06], [OBS-08]). partial/side_effect_failures live on
    # the envelope data block; warnings are top-level on the envelope.
    partial = bool(closure.get("partial") or closure_data.get("partial"))
    if partial:
        closure_result["partial"] = True
        failures = closure.get("side_effect_failures") or closure_data.get("side_effect_failures")
        if failures:
            closure_result["side_effect_failures"] = failures
        close_warnings = closure.get("warnings") or closure_data.get("warnings")
        if close_warnings:
            closure_result["warnings"] = close_warnings
    # implementation note R5: surface a non-blocking package-smoke degrade (cap tripped /
    # runner unavailable) so a skipped smoke reads as a named gap, not silence.
    if smoke_note is not None:
        closure_result["smoke_note"] = smoke_note
    return closure_result, None


# ---------------------------------------------------------------------------
# Lane lifecycle (implementation note S13 / T26)
# ---------------------------------------------------------------------------
# Sync /offload creates the worktree lane but historically never transitioned
# its status; only the daemon auto-closed post-intake. Own the lifecycle here:
# handoff_ready → status "review" (closeable state for the gate); gate then
# one-call closes status "merged" via close_offload_lane_merged / next_lane_action.
# task-finish deliberately does not force-close (WAI; slice-6 reap is the safety
# net). Heuristics: [OBS-08] terminal lifecycle must not be silent; [CON-04]
# avoid orphan open lanes after a completed pass.


def _lane_payload_dict(payload: Any) -> dict[str, Any]:
    """Normalize manage_worktree_lane / list responses to a plain dict."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("schema_version") == 2 and isinstance(payload.get("data"), dict):
        flat = dict(payload)
        flat.update(payload["data"])
        return flat
    return payload


def _lookup_worktree_lane(*, task_ref: str, lane_id: str) -> dict[str, Any] | None:
    """Return the worktree_lanes row for task_ref/lane_id, or None."""
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    listed = _lane_payload_dict(manage_worktree_lane(operation="list", task_ref=task_ref, status="all", limit=500))
    if listed.get("ok") is not True:
        return None
    lanes = listed.get("lanes")
    if not isinstance(lanes, list):
        return None
    for row in lanes:
        if isinstance(row, dict) and str(row.get("lane_id") or "") == lane_id:
            return row
    return None


def next_lane_close_action(*, task_ref: str, lane_id: str) -> dict[str, Any]:
    """One-call gate close contract exposed on handoff_ready pass results.

    The sync offload flow has no daemon post-intake hook; the review gate
    closes the lane when it merges the slice by invoking
    ``close_offload_lane_merged`` (or the equivalent ``manage_worktree_lane``
    call documented here).
    """
    return {
        "tool": "manage_worktree_lane",
        "helper": "close_offload_lane_merged",
        "operation": "close",
        "status": "merged",
        "lane_id": lane_id,
        "task_ref": task_ref,
        "notes": "Closed by offload review gate post-merge (implementation note S13 / T26).",
    }


def close_offload_lane_merged(
    *,
    task_ref: str,
    lane_id: str,
    notes: str | None = None,
    force: bool = False,
    orchestrator_root: Path | str | None = None,
) -> dict[str, Any]:
    """One-call gate close: terminal ``merged`` for a completed offload lane.

    Symmetric to the daemon post-intake close
    (``orchestrator_daemon`` → ``manage_worktree_lane(operation=close, status=merged)``).

    S13-A-01: refuses unless the lane is in status ``review`` (the state a green
    pass leaves it in) so a premature gate call cannot mark unreviewed work
    terminal-merged. ``force=True`` overrides for operator recovery.

    internal / review H1–H3: capture the landing SHA from the
    **orchestrator root** (never the lane worktree — this helper performs no
    merge; the lane worktree tip is the worker branch), require the lane branch
    to be fully contained in that SHA before recording, and withhold MERGED when
    a usable SHA is in hand but the ledger write fails. No usable SHA still
    allows MERGED so a pathologically git-less root does not wedge the lane.
    """
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        _is_full_commit_sha,
        _lane_branch_contained_in,
        _resolve_lane_branch,
        record_lane_landing,
    )

    row = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id)
    if not force:
        current_status = str((row or {}).get("status") or "").strip() or None
        if current_status != "review":
            return {
                "ok": False,
                "lane_id": lane_id,
                "task_ref": task_ref,
                "error": (
                    f"lane_not_in_review: lane '{lane_id}' status is "
                    f"{current_status!r} (expected 'review'); refuse terminal "
                    "merged close — pass force=True to override"
                ),
            }

    # Capture SHA from the orchestrator/task-branch checkout only (H1). Never
    # read the lane worktree tip — close_offload_lane_merged is a status close,
    # not a merge, so the worker branch tip is frequently not on the task branch.
    repo_for_sha: Path | None = None
    if orchestrator_root is not None:
        candidate = Path(orchestrator_root)
        if candidate.exists():
            repo_for_sha = candidate
    if repo_for_sha is None:
        try:
            from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

            cfg = get_runtime_config()
            root = getattr(cfg, "git_workspace_root", None) or getattr(cfg, "workspace_root", None)
            if root is not None:
                repo_for_sha = Path(root)
        except Exception:  # noqa: BLE001
            repo_for_sha = None

    landed_sha = ""
    task_branch = "main"
    if repo_for_sha is not None:
        # Prefer the shared capture helper when available for shape/detached-HEAD
        # rules; fall back to local _git_stdout so tests can patch this module.
        raw_sha = _git_stdout(repo_for_sha, "rev-parse", "HEAD") or ""
        raw_branch = _git_stdout(repo_for_sha, "rev-parse", "--abbrev-ref", "HEAD") or ""
        if _is_full_commit_sha(raw_sha):
            landed_sha = raw_sha.strip()
        if raw_branch and raw_branch != "HEAD":
            task_branch = raw_branch
        else:
            task_branch = "main"

    if landed_sha and repo_for_sha is not None:
        lane_branch = _resolve_lane_branch(
            repo_for_sha,
            task_ref,
            lane_id,
            branch_hint=str((row or {}).get("branch") or "") or None,
        )
        contained = _lane_branch_contained_in(repo_for_sha, landed_sha, lane_branch) if lane_branch else None
        if contained is not True:
            # H1/H2: do not stamp false evidence and do not write MERGED.
            return {
                "ok": False,
                "lane_id": lane_id,
                "task_ref": task_ref,
                "error": (
                    f"landing_not_contained: lane '{lane_id}' branch "
                    f"{lane_branch!r} is not fully contained in orchestrator tip "
                    f"{landed_sha[:12]}; refuse landing record and MERGED close"
                ),
                "landing_sha": landed_sha,
            }
        # Package import path already resolved above; tests patch record_lane_landing
        # on orchestrator_lanes.
        if not record_lane_landing(task_ref, lane_id, landed_sha, task_branch):
            # H3: SHA in hand but ledger write failed — do not write MERGED.
            return {
                "ok": False,
                "lane_id": lane_id,
                "task_ref": task_ref,
                "error": (
                    f"landing_record_failed: lane '{lane_id}' tip {landed_sha[:12]} "
                    "could not be recorded; withholding MERGED for retry"
                ),
                "landing_sha": landed_sha,
            }

    resolved_notes = notes if notes is not None else "Closed by offload review gate post-merge (implementation note S13 / T26)."
    payload = _lane_payload_dict(
        manage_worktree_lane(
            operation="close",
            lane_id=lane_id,
            status=LaneStatus.MERGED,
            notes=resolved_notes,
            task_ref=task_ref,
        )
    )
    if payload.get("ok") is True:
        lane_obj = payload.get("lane")
        lane = lane_obj if isinstance(lane_obj, dict) else {}
        result: dict[str, Any] = {
            "ok": True,
            "lane_id": lane_id,
            "task_ref": task_ref,
            "status": str(lane.get("status") or LaneStatus.MERGED),
            "lane": lane or None,
        }
        if landed_sha:
            result["landing_sha"] = landed_sha
        return result
    return {
        "ok": False,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "error": str(payload.get("error") or "close_offload_lane_merged failed"),
    }


def _mark_lane_review_on_handoff_ready(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
) -> dict[str, Any]:
    """Transition the offload lane to status ``review`` after a green pass.

    Mirrors ``orchestrator_guidance.apply_guidance_resolution`` upsert usage so
    the gate sees an unambiguous closeable state. Failure is surfaced (never
    silent) but does not downgrade handoff_ready — the worker work already
    landed ([OBS-08]).
    """
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    existing = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id)
    branch = ""
    title = None
    objective = None
    owner_agent = None
    model = None
    backend = None
    reasoning_effort = None
    test_cmd = None
    path = str(worktree_path)
    if existing is not None:
        branch = str(existing.get("branch") or "")
        title = existing.get("title") if isinstance(existing.get("title"), str) else None
        objective = existing.get("objective") if isinstance(existing.get("objective"), str) else None
        owner_agent = existing.get("owner_agent") if isinstance(existing.get("owner_agent"), str) else None
        model = existing.get("model") if isinstance(existing.get("model"), str) else None
        backend = existing.get("backend") if isinstance(existing.get("backend"), str) else None
        reasoning_effort = (
            existing.get("reasoning_effort") if isinstance(existing.get("reasoning_effort"), str) else None
        )
        test_cmd = existing.get("test_cmd") if isinstance(existing.get("test_cmd"), str) else None
        raw_path = existing.get("worktree_path")
        if isinstance(raw_path, str) and raw_path.strip():
            path = raw_path.strip()

    if not branch:
        return {
            "ok": False,
            "status": None,
            "error": f"lane '{lane_id}' missing branch; cannot upsert status=review",
        }

    payload = _lane_payload_dict(
        manage_worktree_lane(
            operation="upsert",
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=path,
            branch=branch,
            title=title,
            objective=objective,
            owner_agent=owner_agent,
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            test_cmd=test_cmd,
            status=LaneStatus.REVIEW,
            notes="Offload pass handoff_ready; lane awaiting review-gate close (implementation note S13 / T26).",
        )
    )
    if payload.get("ok") is True:
        lane_obj = payload.get("lane")
        lane = lane_obj if isinstance(lane_obj, dict) else {}
        return {
            "ok": True,
            "status": str(lane.get("status") or LaneStatus.REVIEW),
            "lane": lane or None,
        }
    return {
        "ok": False,
        "status": None,
        "error": str(payload.get("error") or "failed to set lane status=review"),
    }


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
    # HEAD at pass start; used to compute commit_landed without git archaeology
    # at the orchestrator gate (internal).
    start_head_ref: str | None = None

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

    def _build_tokens_payload(run_ctx: Any) -> dict[str, Any]:
        """Pass-end token block: main+subagent summary, labeled by usage_source.

        implementation note S3 / PR-0094-05/06: never advertise ``cumulative_total: 0`` as
        authoritative for telemetry-less backends; bucket by source and surface
        an explicit unavailable / pending-flush line instead.
        """
        cumulative = int(getattr(run_ctx, "cumulative_tokens", 0) or 0)
        # Best-effort main-agent read — never blocks pass completion (PR-0094-06).
        main_tokens: dict[str, Any] | None = None
        try:
            from workbay_orchestrator_mcp.orchestration.main_agent_tokens import (  # noqa: PLC0415
                read_main_agent_turn_tokens,
            )

            main_tokens = read_main_agent_turn_tokens()
        except Exception:  # noqa: BLE001 — degrade loudly via unavailable line
            main_tokens = None

        resolved_backend = str(getattr(run_ctx, "backend", None) or backend)
        # Re-resolve telemetry support against the backend the pass actually ran
        # on: a mid-pass MCP_BACKEND_OVERRIDE would otherwise recreate the unit
        # conflation this payload exists to prevent (grok deltas labeled observed).
        resolved_telemetry_supported = (
            token_telemetry_supported
            if resolved_backend == str(backend)
            else backend_supports_token_telemetry(resolved_backend)
        )
        subagents: list[dict[str, Any]] = []
        if resolved_telemetry_supported:
            if cumulative > 0:
                subagents.append(
                    {
                        "lane_id": lane_id,
                        "usage_source": "observed",
                        "total_tokens": cumulative,
                    }
                )
            else:
                subagents.append(
                    {
                        "lane_id": lane_id,
                        "usage_source": None,
                        "total_tokens": None,
                        "reason": "unavailable",
                    }
                )
            usage_source_label = "observed" if cumulative > 0 else "unavailable"
        elif resolved_backend == "grok-cli":
            # grok-cli self-meters approximately via session context-fill
            # deltas (a different unit): context-delta or pending flush.
            from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (  # noqa: PLC0415
                USAGE_SOURCE_GROK_CONTEXT_DELTA,
            )

            if cumulative > 0:
                subagents.append(
                    {
                        "lane_id": lane_id,
                        "usage_source": USAGE_SOURCE_GROK_CONTEXT_DELTA,
                        "total_tokens": cumulative,
                    }
                )
                usage_source_label = USAGE_SOURCE_GROK_CONTEXT_DELTA
            else:
                subagents.append(
                    {
                        "lane_id": lane_id,
                        "usage_source": USAGE_SOURCE_GROK_CONTEXT_DELTA,
                        "total_tokens": None,
                        "reason": "unavailable (pending flush)",
                    }
                )
                usage_source_label = "unavailable"
        else:
            # Any other telemetry-less backend: neutral unavailable — grok's
            # context-delta / pending-flush labels are grok-specific (REV-S3-05).
            subagents.append(
                {
                    "lane_id": lane_id,
                    "usage_source": None,
                    "total_tokens": None,
                    "reason": "unavailable",
                }
            )
            usage_source_label = "unavailable"

        try:
            from workbay_orchestrator_mcp.orchestration.turn_summary import (  # noqa: PLC0415
                render_turn_token_summary,
            )

            summary = render_turn_token_summary(main_tokens, subagents)
        except Exception:  # noqa: BLE001 — summary is additive; never fail the pass
            # Degrade per-lane (REV-S3-01): keep one explicit unavailable line
            # per lane instead of collapsing to a single generic summary.
            lane_ids = [str(entry.get("lane_id") or "unknown") for entry in subagents]
            fallback_lines = ["main-agent: unavailable"] + [f"subagent {lid}: unavailable" for lid in lane_ids]
            summary = {
                "text": "\n".join(fallback_lines),
                "lines": fallback_lines,
                "main_agent_available": False,
                "observed_total": 0,
                "grok_context_approx_total": 0,
                "total_tokens_by_usage_source": {},
                "unavailable_lanes": lane_ids,
            }

        tokens: dict[str, Any] = {
            "token_budget": token_budget,
            "token_telemetry": resolved_telemetry_supported,
            "usage_source": usage_source_label,
            "summary": summary,
            "summary_text": summary.get("text") if isinstance(summary, dict) else str(summary),
        }
        # Observed / telemetry-capable: cumulative_total remains the governor
        # figure. Telemetry-less: never publish under cumulative_total —
        # grok's context-fill delta is a different unit and goes under its own
        # key (context_delta_total, REV-S3-04); zero is never advertised as an
        # authoritative total.
        if resolved_telemetry_supported:
            tokens["cumulative_total"] = cumulative
        else:
            tokens["cumulative_total"] = None
            if resolved_backend == "grok-cli" and cumulative > 0:
                tokens["context_delta_total"] = cumulative
        return tokens

    def _compute_commit_landed() -> bool:
        """True when this pass advanced HEAD (worker commit or engine checkpoint)."""
        if checkpoints:
            return True
        if not start_head_ref:
            return False
        try:
            head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        return bool(head) and head != start_head_ref

    def _payload(
        outcome: str,
        *,
        run_ctx: Any = None,
        error: str | None = None,
        slice_closure: dict[str, Any] | None = None,
        self_verify: dict[str, Any] | None = None,
        composer_violation: dict[str, Any] | None = None,
        continuation_dispatch_id: str | None = None,
        failed_stage: str | None = None,
        reason: str | None = None,
        commit_landed: bool | None = None,
        review: str | None = None,
        findings: list[dict[str, Any]] | None = None,
        raw_tail: str | None = None,
    ) -> dict[str, Any]:
        if failed_stage is not None and failed_stage not in FAILED_STAGES:
            # Defensive: never ship an undeclared stage marker.
            failed_stage = None
        effective_effort = getattr(run_ctx, "execution_effective_effort", None)
        landed = _compute_commit_landed() if commit_landed is None else bool(commit_landed)
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
            "tokens": _build_tokens_payload(run_ctx),
            "token_governance": token_governance,
            "checkpoint_commits": list(checkpoints),
            "slice_closure": slice_closure if slice_closure is not None else {"recorded": False},
            "wall_seconds": round(time.monotonic() - started, 2),
            "retry_policy": "never_in_engine; recover via a new idempotent dispatch (dispatch_id)",
            # internal: always present so the gate branches without git archaeology.
            "commit_landed": landed,
            # Dual-axis discriminators (engoutcome): always populated so a caller
            # can tell work-landed+ceremony-failed from pure error without prose.
            "work_status": _work_status_for(landed),
            "ceremony_status": _ceremony_status_for(run_ctx),
            "failed_stage": failed_stage,
            # T4: always surface worker findings (empty list when none / listing failed).
            "findings": findings if findings is not None else [],
        }
        if error is not None:
            result["error"] = error
        # implementation note R9: mark a backend-internal transient error so the coordinator
        # re-dispatches mechanically instead of doing log forensics.
        if outcome == "error" and _is_backend_transient_error(error):
            result["backend_transient"] = True
        if reason is not None:
            result["reason"] = reason
        # E8FIX-A-03: surface the typed execute stop reason on every payload so
        # operators/gates can branch without re-parsing free-form error text.
        stop_reason_payload = getattr(run_ctx, "execute_stop_reason", None) if run_ctx is not None else None
        if stop_reason_payload:
            result["execute_stop_reason"] = str(stop_reason_payload)
        if self_verify is not None:
            result["self_verify"] = self_verify
            # Promote typed gate status + exit_code so unrun vs red is readable
            # from the pass surface alone (findings 3 and 4) without parsing
            # output_tail prose or opening selfverify.json.
            gate_status = _self_verify_gate_status(self_verify if isinstance(self_verify, dict) else None)
            if gate_status is not None:
                result["self_verify_gate"] = gate_status
            if isinstance(self_verify, dict) and "exit_code" in self_verify:
                result["self_verify_exit_code"] = self_verify.get("exit_code")
        if composer_violation is not None:
            result["composer_violation"] = composer_violation
        if continuation_dispatch_id is not None:
            result["continuation_dispatch_id"] = continuation_dispatch_id
        if review is not None:
            result["review"] = review
        if raw_tail is not None:
            result["raw_tail"] = raw_tail
        return result

    def _finish(result: dict[str, Any]) -> dict[str, Any]:
        write_pass_state(
            resolved_state_dir,
            resolved_pass_id,
            {"status": "done", "task_ref": task_ref, "lane_id": lane_id, "result": result},
        )
        return result

    def _execute_pass() -> dict[str, Any]:
        nonlocal run_ctx, start_head_ref
        # implementation note R7: per-pass engine self-integrity check. Refuse loudly with a
        # typed server_stale_restart_required outcome when the engine's own source
        # vanished since import, rather than crashing later on the lane_prompt.py
        # spawn (the 0108 concurrent-env-flip incident).
        integrity_note = _engine_source_integrity_note()
        if integrity_note is not None:
            return _payload("server_stale_restart_required", error=integrity_note)
        lane_state = wd.poll_lane_state(
            orchestrator_root=Path(orchestrator_root),
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=Path(worktree_path),
        )
        if lane_state != "actionable":
            return _payload("no_actionable_work", error=f"lane state: {lane_state}; record a brief first")

        # implementation note R3: an operator-declared review lane disambiguates the clean-tree
        # + needs_guidance shape (a completed review) from a wedged transport failure.
        # implementation note S4 / cs0166-r07-10: confirmed only when MCP params carry an
        # explicit kind. Unresolved → typed error (never fabricate implement).
        lane_kind, lane_kind_confirmed = _lane_kind(Path(orchestrator_root), task_ref, lane_id)
        if not lane_kind_confirmed or lane_kind not in ("implement", "review"):
            return _payload(
                "error",
                error=(
                    "lane_kind unavailable: MCP params did not resolve an explicit "
                    "implement|review kind (fail-closed; refusing implement default)"
                ),
                failed_stage="execute",
            )
        review_lane = lane_kind == "review"
        findings_harvest: dict[str, Any] | None = None
        # True when this pass used harvest as the *review product* classifier
        # (review_branch_applies). Only then is harvest_failed forbidden from
        # backfilling pre-existing open findings into the payload.
        review_product_harvest = False

        # implementation note S3 [OBS-08]/T3]: ensure lane manifest before execute/bootstrap
        # (auto-materialize when possible; named error mentions materialize_*).
        try:
            from workbay_orchestrator_mcp.orchestration.offload_preflight import (  # noqa: PLC0415
                ensure_lane_manifest_for_offload,
            )

            branch_name = _git_stdout(Path(worktree_path), "rev-parse", "--abbrev-ref", "HEAD") or ""
            manifest_ensure = ensure_lane_manifest_for_offload(
                orchestrator_root=Path(orchestrator_root),
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=Path(worktree_path),
                branch=branch_name if branch_name != "HEAD" else None,
                preferred_backend=backend,
                preferred_model=model,
                auto_materialize=True,
            )
        except Exception as exc:  # noqa: BLE001 — never crash the pass on preflight glue
            return _payload(
                "error",
                error=f"no manifest for {lane_id}; run materialize_offload_lane_manifest ({exc})",
                failed_stage="execute",
            )
        if not manifest_ensure.get("ok"):
            # S2R-4: a typed policy refusal (remote_required) from the ensure
            # path keeps its discriminator instead of collapsing to error.
            _ensure_outcome = str(manifest_ensure.get("outcome") or "error")
            return _payload(
                _ensure_outcome,
                error=str(
                    manifest_ensure.get("error") or f"no manifest for {lane_id}; run materialize_offload_lane_manifest"
                ),
                failed_stage="execute" if _ensure_outcome == "error" else None,
            )

        # Worker end-state baselines: closure is recorded only from a commit and a
        # worker report produced DURING this pass (freshness gate, PR-10).
        start_head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
        start_head_ref = start_head or None
        baseline_report_id = _max_worker_report_id(task_ref, lane_id)
        baseline_test_id = _max_verified_test_id(task_ref)

        # Union predicate: BOTH bounded families accept a per-cycle wall clock.
        # Gating on the grok-only predicate silently dropped the caller's
        # turn_timeout_seconds for the wall-clock-only family, so preflight's
        # "governed by turn/time bounds" note described a bound nothing applied.
        # ...but they carry it in DIFFERENT fields: the wall-clock-only family
        # uses adapter_timeout so its agent bound never becomes the local
        # TEST_CMD self-verify deadline (which reads grok_timeout).
        _cycle_timeout = int(turn_timeout_seconds) if turn_timeout_seconds else None
        grok_timeout = _cycle_timeout if backend_supports_token_budget_cycle_bounds(backend) else None
        adapter_timeout = _cycle_timeout if backend_supports_adapter_timeout_bounds(backend) else None
        # Normalize before any local bash -lc or remote --test-cmd ship so a
        # double-HTML-escaped && cannot reach the shell (finding 2). When the
        # peel fires, converge the stored worktree_lanes.test_cmd row so
        # operator-visible storage and the executed value agree (no in-tree
        # html.escape producer; consumption-only mask is not a fix).
        raw_test_cmd = str(test_cmd or "").strip() or None
        # Typed outcome for an un-repairable test_cmd (OFFP2B-R1-01): never let
        # normalize ValueError reach the outer ``except Exception`` that would
        # launder it into ``outcome="error", error="offload pass crashed: ..."``.
        try:
            resolved_test_cmd = normalize_lane_test_cmd(raw_test_cmd)
        except ValueError as exc:
            return _payload(
                "error",
                error=f"test_cmd unrepairable: {exc}",
                failed_stage="execute",
            )
        if (
            resolved_test_cmd is not None
            and raw_test_cmd is not None
            and resolved_test_cmd != raw_test_cmd
        ):
            _converge_stored_lane_test_cmd(
                task_ref=task_ref,
                lane_id=lane_id,
                test_cmd=resolved_test_cmd,
            )
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
            adapter_timeout=adapter_timeout,
            grok_max_turns=grok_max_turns,
            dry_run=dry_run,
            token_budget=token_budget,
            test_cmd=resolved_test_cmd,
        )
        config = wd._resolve_grok_cycle_bounds(config)
        run_ctx, _ = wd._setup_worker_run(config)

        outcome: str | None = None
        error_reason: str | None = None
        failed_stage: str | None = None
        # E8ROUTE-LOCAL-02: typed failure reason from raw_payload["failure_reason"],
        # carried on a distinct pass field so a gate *can* branch on equality
        # without a free-form parse. No in-repo gate branches on it today: the
        # key reaches operators via the MCP envelope (api.py `_offload_payload`
        # spreads additively and never allow-lists), and that is currently its
        # only consumer. Do not read this as evidence of a live in-repo gate.
        execute_failure_reason: str | None = None
        self_verify_result: dict[str, Any] | None = None
        composer_violation_result: dict[str, Any] | None = None
        continuation_dispatch_id: str | None = None
        review_discriminator: str | None = None
        review_raw_tail: str | None = None
        for cycle in range(max_review_cycles):
            run_ctx.cycle = cycle
            # Pre-turn admission (fail-closed point 1 of 3).
            if run_ctx.cumulative_tokens >= token_budget:
                outcome = "token_budget_exceeded"
                break
            if time.monotonic() >= deadline:
                outcome = "timeout"
                break
            # implementation note S1.3 / REV0192R12-B-2: per-cycle decision_ts (host wall
            # epoch seconds). Stamp once per cycle at the commit-to-dispatch
            # site, upstream of this cycle's admission_deferred park. Do NOT
            # stamp once before the loop — cycle N's pre_spawn must not absorb
            # cycle N-1's runtime.
            #
            # Admission park does not continue this loop: it checkpoints, sets
            # outcome=admission_deferred, and breaks. A later operator re-dispatch
            # builds a fresh WorkerRunContext and stamps here again; there is no
            # in-loop restamp path for the deferred cycle.
            run_ctx.decision_ts = int(time.time())
            # internal: host-memory re-check beside the budget/timeout gates.
            # If pressure rose since the last turn so a heavy spawn would now be
            # refused, park — preserve any dirty work as a checkpoint and report
            # admission_deferred (a deferred pass is recoverable via a fresh
            # dispatch; a paging panic is not).
            if not dry_run and cycle > 0:
                park_reason = _host_admission_should_park(Path(orchestrator_root), backend=run_ctx.backend)
                if park_reason is not None:
                    # A checkpoint COMMIT can itself fail (lock contention, disk
                    # full, failing pre-commit hook). If it does, dirty work is
                    # NOT preserved — report uncommitted_work rather than claim a
                    # clean admission_deferred with recoverable state (OBS-08:
                    # silence is not success). Mirrors the sibling checkpoint
                    # call sites in this function.
                    checkpointed = _checkpoint_if_dirty(
                        Path(worktree_path), lane_id, checkpoints, dry_run=dry_run
                    )
                    outcome = "admission_deferred" if checkpointed else "uncommitted_work"
                    break
            tokens_before = run_ctx.cumulative_tokens
            # Capture HEAD before execute so the blocked probe can distinguish a
            # transport/no-run failure (unchanged HEAD + clean tree) from real work
            # that later reported needs_guidance (dirty tree or landed commit).
            pre_exec_head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
            if not wd._execute_phase(run_ctx):
                stop_reason = getattr(run_ctx, "execute_stop_reason", None)
                # Remote agents write in the VM sandbox; host tree may be clean while
                # the co-located sandbox still holds uncommitted work. Probe both.
                dirty_for_checkpoint = False
                try:
                    for probe_path in _salvage_probe_paths(
                        getattr(run_ctx, "backend", None) or backend, Path(worktree_path)
                    ):
                        if _worktree_dirty(probe_path):
                            dirty_for_checkpoint = True
                            break
                except RuntimeError:
                    dirty_for_checkpoint = _worktree_dirty(Path(worktree_path))
                if stop_reason in _STOP_REASONS_CHECKPOINT and dirty_for_checkpoint:
                    # Unrun vs red must not share an arm (CHECKPOINT-BLOCKED-BY-
                    # INCONCLUSIVE-SELFVERIFY-DISCARDS-COMPLETE-LANES). A red gate
                    # still blocks the early checkpoint (post-loop may salvage when
                    # stop_reason is salvage_eligible). An unrun gate falls through
                    # to salvage so work in hand survives absence of evidence.
                    unrun_gate = False
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
                            if _self_verify_is_inconclusive(self_verify_result):
                                unrun_gate = True
                            else:
                                outcome = "self_verify_failed"
                                failed_stage = "self_verify"
                                error_reason = (
                                    f"resumable execute stop ({stop_reason}) checkpoint blocked: "
                                    f"self-verify failed on `{self_verify_result.get('command')}`"
                                )
                                break
                    salvaged = _salvage_checkpoint_if_dirty(
                        getattr(run_ctx, "backend", None) or backend,
                        Path(worktree_path),
                        lane_id,
                        checkpoints,
                        dry_run=dry_run,
                    )
                    if salvaged:
                        continuation_dispatch_id = _open_dispatch_id(task_ref, lane_id)
                        if unrun_gate:
                            outcome = "self_verify_inconclusive"
                            failed_stage = "self_verify"
                            error_reason = (
                                f"resumable execute stop ({stop_reason}) self-verify unrun "
                                f"(exit {self_verify_result.get('exit_code') if self_verify_result else None}; "
                                "zero tests executed) on "
                                f"`{(self_verify_result or {}).get('command')}`; "
                                "checkpoint preserved (unrun gate is not a red suite); "
                                "continue by re-dispatching with dispatch_lane_work(dispatch_id=<same>, "
                                "no brief) → continuation_armed, then run_offload_pass"
                            )
                        else:
                            outcome = "checkpoint"
                            error_reason = (
                                f"execute stopped with resumable work ({stop_reason}) and a "
                                "self-verified checkpoint preserved; "
                                "continue by re-dispatching with dispatch_lane_work(dispatch_id=<same>, "
                                "no brief) → continuation_armed, then run_offload_pass"
                            )
                        break
                    if unrun_gate:
                        # Work in hand but the checkpoint itself failed — still
                        # typed as uncommitted_work, never as a red suite.
                        outcome = "uncommitted_work"
                        failed_stage = "self_verify"
                        error_reason = (
                            f"resumable execute stop ({stop_reason}) self-verify unrun "
                            f"(exit {self_verify_result.get('exit_code') if self_verify_result else None}) "
                            "and checkpoint salvage failed"
                        )
                        break
                outcome = "error"
                failed_stage = "execute"
                # Prefer named execute cause (missing manifest → materialize_*) over
                # a generic status-log pointer (implementation note S3 / [OBS-08]).
                # E8FIX-A-01/A-03: every checkpoint-eligible stop reason is named in
                # the error string (not only max_turns). When the marker path never
                # set execute_error (adapter-exception-only assignment) and the tree
                # is clean, promote adapter blockers so the operator sees the real
                # cause instead of the generic execute-phase-failed string.
                named_exec = str(getattr(run_ctx, "execute_error", None) or "").strip()
                if stop_reason in _STOP_REASONS_CHECKPOINT:
                    blocker_text = ""
                    if not named_exec:
                        try:
                            if getattr(run_ctx, "final_result_path", None):
                                probe = wd._load_result(Path(run_ctx.final_result_path))
                                bl = probe.get("blockers") if isinstance(probe, dict) else None
                                if isinstance(bl, list):
                                    blocker_text = "; ".join(
                                        str(b).strip() for b in bl if str(b).strip()
                                    )
                        except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError):
                            blocker_text = ""
                    if stop_reason == "max_turns":
                        # Preserve the historical max-turns phrasing used by probes.
                        error_reason = _max_turns_execute_error_reason(named_exec)
                    elif named_exec:
                        error_reason = f"execute stopped: {stop_reason} ({named_exec})"
                    elif blocker_text:
                        error_reason = f"execute stopped: {stop_reason}: {blocker_text}"
                    else:
                        error_reason = f"execute stopped: {stop_reason}"
                else:
                    error_reason = (
                        named_exec
                        or "execute phase failed; see worker status/log for the failure stage"
                    )
                break
            # Post-turn reconciliation (point 3): a budgeted turn with no token
            # telemetry. This is a contract violation ONLY for a backend that
            # declares it emits token usage — for such a backend a zero delta
            # means the governor ran blind, so error out. A backend declared
            # telemetry-less (e.g. grok-cli, which self-meters only
            # approximately via session context-fill deltas — a different unit
            # not governed by token_budget) is not violating any contract; its
            # budget is enforced by the turn-count + deadline bounds in this same
            # loop, so a zero delta on a turn must NOT abort a
            # working turn (internal / TB-001; unifies
            # this with worker_daemon._accumulate_run_ctx_tokens' soft-warn).
            if not dry_run and run_ctx.cumulative_tokens == tokens_before and token_telemetry_supported:
                outcome = "error"
                failed_stage = "execute"
                error_reason = "token telemetry missing on a budgeted turn (telemetry contract violation)"
                break
            # Execute-result blocked probe (width2 dogfood): a turn whose result
            # signals needs_guidance / admission_deferred AND left a clean tree
            # with unchanged HEAD never ran real work (transport/no-run failure).
            # In that case TEST_CMD would fail for the WRONG reason and mask the
            # actual blocker as self_verify_failed — classify before self-verify.
            # Real work (dirty tree or landed commit) must keep the historical
            # self-verify-before-commit ordering even if handoff_action is
            # needs_guidance. _review_phase then routes the blocked handoff /
            # defer as usual when the probe does fire.
            execute_blocked_reason: str | None = None
            # implementation note R3 [PR-0155-02]: the execute-blocked probe's transport-failure
            # semantics apply to implement lanes only; a review lane's clean-tree +
            # needs_guidance is a completed review, typed at classification below.
            if not dry_run and not review_lane:
                exec_probe: dict[str, Any] = {}
                try:
                    if getattr(run_ctx, "final_result_path", None):
                        exec_probe = wd._load_result(Path(run_ctx.final_result_path))
                except (OSError, json.JSONDecodeError, RuntimeError):
                    exec_probe = {}
                raw_payload = exec_probe.get("raw_payload") or {}
                signals_blocked = exec_probe.get("handoff_action") == "needs_guidance" or (
                    isinstance(raw_payload, dict) and raw_payload.get("admission_deferred")
                )
                if (
                    signals_blocked
                    and not _worktree_dirty(Path(worktree_path))
                    and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") == pre_exec_head
                ):
                    # implementation note S4: claim-vs-evidence is a nested refinement of this
                    # clean-tree + unchanged-HEAD guard — never a sibling of it, so a
                    # dirty tree or advanced HEAD can never reach the fabrication token.
                    if (
                        lane_kind_confirmed
                        and lane_kind == "implement"
                        and not (isinstance(raw_payload, dict) and raw_payload.get("admission_deferred"))
                        and _claim_contradicted_by_evidence(exec_probe, raw_payload)
                    ):
                        execute_blocked_reason = "claim_contradicted_by_evidence"
                    else:
                        probe_blockers = exec_probe.get("blockers")
                        first_blocker = (
                            str(probe_blockers[0]) if isinstance(probe_blockers, list) and probe_blockers else ""
                        )
                        execute_blocked_reason = (
                            first_blocker or str(exec_probe.get("summary") or "") or "execute returned a blocked result"
                        )
                    # E8ROUTE-LOCAL-02 / E8RA-R1-01: structural consumer for the
                    # typed failure_reason channel already on raw_payload. Keep
                    # execute_blocked_reason as operator-facing prose (blockers[0]);
                    # do not recover the token by substring of that prose.
                    if isinstance(raw_payload, dict):
                        typed_fr = raw_payload.get("failure_reason")
                        if isinstance(typed_fr, str) and typed_fr.strip():
                            execute_failure_reason = typed_fr.strip()
            # Worker self-verify gate (backend-neutral): TEST_CMD must pass before commit.
            # Skipped for a blocked execute result — there is no verified work to gate.
            if execute_blocked_reason is None and not review_lane and not dry_run and config.test_cmd:
                self_verify_result = wd._self_verify_phase(run_ctx)
                if not self_verify_result.get("passed"):
                    wd._record_self_verify_blocker(
                        orchestrator_root=Path(orchestrator_root),
                        task_ref=task_ref,
                        lane_id=lane_id,
                        test_cmd=str(self_verify_result.get("command") or config.test_cmd),
                        output_tail=str(self_verify_result.get("output_tail") or ""),
                    )
                    if _self_verify_is_inconclusive(self_verify_result):
                        outcome = "self_verify_inconclusive"
                        failed_stage = "self_verify"
                        error_reason = (
                            f"worker self-verify inconclusive on `{self_verify_result.get('command')}` "
                            f"(exit {self_verify_result.get('exit_code')}; zero tests executed — "
                            "host re-verification required, not a red suite)"
                        )
                        # Unrun gate: preserve dirty work before breaking. A red
                        # suite still refuses the commit (fall through to break
                        # without checkpointing below).
                        if not _checkpoint_if_dirty(
                            Path(worktree_path), lane_id, checkpoints, dry_run=dry_run
                        ):
                            outcome = "uncommitted_work"
                            failed_stage = "execute"
                            error_reason = (
                                "self-verify unrun and the checkpoint commit failed; "
                                f"original: {error_reason}"
                            )
                        break
                    outcome = "self_verify_failed"
                    failed_stage = "self_verify"
                    error_reason = (
                        f"worker self-verify failed on `{self_verify_result.get('command')}` "
                        f"(exit {self_verify_result.get('exit_code')})"
                    )
                    break
            # Commit gate: review never sees a dirty tree. dry_run never writes.
            if not _checkpoint_if_dirty(Path(worktree_path), lane_id, checkpoints, dry_run=dry_run):
                outcome = "uncommitted_work"
                failed_stage = "execute"
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
                # S3b-2 also raises StopIteration("admission_deferred") for a
                # retryable VM memory-pressure defer (no handoff submitted).
                blocked_kind = str(stop.args[0]) if stop.args else "needs_guidance"
                if blocked_kind == "admission_deferred":
                    # Retryable VM memory-pressure defer from the remote adapter: no commit
                    # landed, recoverable via a fresh re-dispatch. Clean deferred outcome
                    # (mirrors the pre-turn admission_deferred); no salvage checkpoint needed.
                    outcome = "admission_deferred"
                    failed_stage = "execute"
                    error_reason = "grok-remote turn deferred by VM admission (memory floor / lane cap / residual timeout; retryable)"
                    break
                # Review product is decided by harvest, not by handoff-submit exit.
                # handoff_exit is the submit subprocess status only; a review lane may
                # already have recorded findings via _harvest_review_findings. Do not
                # gate the clean-tree review branch on exit 0 (surface submit failure
                # on the payload instead). composer_violation_quarantined stays under
                # the exit-0 guard: it is a self-verified checkpoint concern, not a
                # review product. `not checkpoints` is required: pre_exec_head is
                # re-baselined inside the cycle loop, so a multi-cycle lane that
                # already checkpointed can still satisfy HEAD == pre_exec_head.
                try:
                    review_branch_applies = (
                        review_lane
                        and not checkpoints
                        and not _worktree_dirty(Path(worktree_path))
                        and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") == pre_exec_head
                    )
                except RuntimeError as guard_exc:
                    outcome = "error"
                    failed_stage = "review"
                    error_reason = f"review-lane classification guard failed: {guard_exc}"
                    break
                if review_branch_applies:
                    # implementation note R3 [PR-0155-02/OBS-04]: a review lane that changed
                    # nothing is a COMPLETED review when the findings block is
                    # parseable (even an empty array = "reviewed clean"), regardless
                    # of whether the handoff-submit subprocess exited 0.
                    # Fail-closed [SECD-05]: only recorded / reviewed_clean may
                    # become review_complete; harvest_failed never can.
                    findings_harvest = _harvest_review_findings(
                        _safe_final_result(run_ctx),
                        _safe_latest_worker_report(task_ref, lane_id),
                        task_ref=task_ref,
                        lane_id=lane_id,
                        session=session,
                        orchestrator_root=Path(orchestrator_root),
                    )
                    review_product_harvest = True
                    if _harvest_block_parsed(findings_harvest):
                        outcome = "review_complete"
                        failed_stage = None
                        error_reason = None
                    elif run_ctx.handoff_exit == 0:
                        # A reviewer that reported nothing structured genuinely
                        # needs guidance ([CON-05]: needs_guidance meanings kept).
                        outcome = "needs_guidance"
                        failed_stage = "review"
                        if isinstance(findings_harvest, dict) and findings_harvest.get("reason") == "record_failed":
                            # Block WAS parseable; persistence failed (grok/F5) — do
                            # not claim "no parseable findings block".
                            error_reason = "review findings block parseable but recording failed (record_failed)"
                        else:
                            error_reason = "review lane submitted no parseable findings block"
                    else:
                        # No product + failed submit: still an error (rescue is scoped
                        # to a review that actually produced its findings block).
                        # Preserve record_failed specificity so a parseable block
                        # whose WRITE failed is not mislabeled as "no parseable block".
                        outcome = "error"
                        failed_stage = "review"
                        if isinstance(findings_harvest, dict) and findings_harvest.get("reason") == "record_failed":
                            error_reason = (
                                "review findings block parseable but recording failed (record_failed); "
                                "handoff submit also failed"
                            )
                        else:
                            error_reason = "review phase ended the pass without a clean handoff"
                elif run_ctx.handoff_exit == 0:
                    violation = wd._grok_build_contamination_info(run_ctx)
                    # implementation note S2: Composer attestation retired. Only real
                    # grok-build contamination quarantines a self-verified
                    # checkpoint ([OBS-08]); missing/format-drift attestation
                    # is no longer a pass outcome branch.
                    if violation is not None and checkpoints and str(violation.get("branch") or "") == "contamination":
                        composer_violation_result = violation
                        outcome = "composer_violation_quarantined"
                        failed_stage = "attestation"
                        error_reason = (
                            "grok-build contamination after a self-verified checkpoint; "
                            f"branch={violation.get('branch')}; commit preserved for orchestrator review"
                        )
                    else:
                        outcome = "needs_guidance"
                        # A pre-review blocked EXECUTE result (transport failure /
                        # no-commit) is an execute-stage failure with its real
                        # blocker text, not a generic review-stage handoff.
                        failed_stage = "execute" if execute_blocked_reason else "review"
                        error_reason = execute_blocked_reason or (
                            f"worker handed a blocked result back for guidance ({blocked_kind})"
                        )
                else:
                    outcome = "error"
                    failed_stage = "review"
                    error_reason = "review phase ended the pass without a clean handoff"
                break
            except (RuntimeError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
                outcome = "error"
                failed_stage = "review"
                error_reason = f"review phase failed: {exc}"
                break
            # Capture smoke-review degrade discriminator (T1 / [OBS-08]).
            if isinstance(review_output, dict):
                review_status = review_output.get("review")
                if isinstance(review_status, str) and review_status:
                    review_discriminator = review_status
                raw_tail_value = review_output.get("raw_tail")
                if isinstance(raw_tail_value, str) and raw_tail_value:
                    review_raw_tail = raw_tail_value
            if review_output.get("converged", False) or review_discriminator == "skipped_unparseable":
                # Unparseable smoke review after green self-verify is not a hard
                # failure: treat as converged-empty and continue to handoff + closure.
                if review_discriminator == "skipped_unparseable" and not review_output.get("converged", False):
                    review_output = dict(review_output)
                    review_output["converged"] = True
                    review_output.setdefault("findings", [])
                check_ok = wd._verify_phase(run_ctx, review_output)
                wd._handoff_phase(run_ctx, check_ok)
                # handoff_exit==0 only means the submission landed. _handoff_phase
                # submits a needs_guidance handoff when verification failed, so a
                # clean exit with check_ok False is a blocked result, NOT ready.
                if run_ctx.handoff_exit != 0:
                    outcome = "error"
                    failed_stage = "handoff"
                    error_reason = "final handoff failed after a converged review"
                elif not check_ok:
                    # BR-0108-S1-01: skipped_unparseable only softens smoke-review
                    # parse; it never overrides lane-check failure ([OBS-08]).
                    outcome = "needs_guidance"
                    failed_stage = "review"
                    error_reason = "lane verification failed after review convergence"
                else:
                    # T1: green + commit + unparseable smoke review → handoff_ready
                    # with review=skipped_unparseable (never bare error).
                    outcome = "handoff_ready"
                break
        else:
            outcome = "error"
            failed_stage = "review"
            error_reason = f"review did not converge after {max_review_cycles} cycles"

        # Surface max-turns when it collapsed into a generic review-stage error
        # (remote agent died with max turns; host tree stayed clean so the early
        # checkpoint arm never fired). Prefer the typed stop reason over a
        # handoff-shape mislabel (OFFLOAD-SALVAGE-PROBES-LOCAL-TREE…).
        stop_reason_final = getattr(run_ctx, "execute_stop_reason", None) if run_ctx is not None else None
        if (
            outcome == "error"
            and failed_stage == "review"
            and (
                stop_reason_final == "max_turns"
                or (
                    isinstance(error_reason, str)
                    and "max turns" in error_reason.casefold()
                )
            )
        ):
            failed_stage = "execute"
            # Bare default only: execute_error is written solely on the adapter-
            # exception path, which always returns False from _execute_phase and
            # is consumed by the early execute-stop arm (break before review).
            # Reaching this reclass arm requires failed_stage == "review", which
            # requires execute returned True — empty intersection with a set field.
            error_reason = _max_turns_execute_error_reason()
            if run_ctx is not None and not getattr(run_ctx, "execute_stop_reason", None):
                run_ctx.execute_stop_reason = "max_turns"

        # Land status BEFORE salvage: ceremony_failed is about worker-landed work
        # the handoff failed to report, not about engine salvage of a dirty tree
        # after the fact (dirty-tree control must stay bare error).
        landed_before_salvage = bool(checkpoints) or (
            bool(start_head)
            and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") not in ("", start_head)
        )

        # Salvage checkpoint: preserve partial work for timeout / budget / error so
        # it is referenced in the outcome rather than lost; a failed salvage downgrades
        # to uncommitted_work. Capture the budget trip BEFORE any downgrade so the
        # budget-exceeded handler still fires (the downgrade would flip the guard).
        budget_tripped = outcome == "token_budget_exceeded"
        # implementation note S3a / exit-8: also salvage when self-verify failed after
        # remote exit-3 (agent_exit_with_work). Deliberately excludes max_turns
        # and wall_clock_expiry so the F1 invariant (red time-bound stops stay
        # dirty/uncommitted) holds for both local and remote wall clocks.
        if outcome in ("timeout", "token_budget_exceeded", "error") or (
            outcome == "self_verify_failed" and getattr(run_ctx, "execute_stop_reason", None) in _SALVAGE_STOP_REASONS
        ):
            # A review-lane classification guard failure already means git status
            # was unreadable; re-probing dirty for salvage would raise again and
            # wrongly reclassify the typed review error as uncommitted_work.
            classification_guard_failed = (
                outcome == "error"
                and failed_stage == "review"
                and isinstance(error_reason, str)
                and error_reason.startswith("review-lane classification guard failed:")
            )
            if not classification_guard_failed:
                try:
                    # Remote backends: probe co-located sandbox first (host worktree
                    # is clean by construction for pre-commit deaths).
                    salvaged = _salvage_checkpoint_if_dirty(
                        getattr(run_ctx, "backend", None) if run_ctx is not None else backend,
                        Path(worktree_path),
                        lane_id,
                        checkpoints,
                        dry_run=dry_run,
                    )
                except RuntimeError as exc:
                    salvaged = False
                    error_reason = f"{outcome}: checkpoint salvage failed: {exc}"
                if not salvaged:
                    error_reason = error_reason or f"{outcome}: partial work could not be checkpointed"
                    outcome = "uncommitted_work"
                    failed_stage = failed_stage or "execute"
        if budget_tripped:
            wd._handle_token_budget_exceeded(run_ctx)

        slice_closure: dict[str, Any] | None = None
        # Closure from the verified commit even when smoke review degraded (T1).
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
                failed_stage = "handoff"
                error_reason = closure_error
            # implementation note S2 [REF-19]: handoff_ready_unattested collapsed → handoff_ready.

        # OL-PF2: green committed work with a no-question blocked handoff is
        # completed_unreviewed — never leave it as needs_guidance, and never
        # promote execute/self_verify failures (fail-closed).
        # Post-salvage land status for completed_unreviewed (salvage may land a
        # checkpoint the operator can still inspect).
        landed_for_reclass = bool(checkpoints) or (
            bool(start_head)
            and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") not in ("", start_head)
        )
        if outcome == "needs_guidance":
            outcome, failed_stage, error_reason = _maybe_reclassify_completed_unreviewed(
                outcome=outcome,
                failed_stage=failed_stage,
                commit_landed=landed_for_reclass,
                self_verify_result=self_verify_result,
                error_reason=error_reason,
                run_ctx=run_ctx,
                wd=wd,
                task_ref=task_ref,
                lane_id=lane_id,
                baseline_report_id=baseline_report_id,
            )

        # Engoutcome finding 1: work landed + handoff submit failed must not
        # collapse to bare error (orchestrators discard the committed product).
        # Use pre-salvage land only so a dirty-tree salvage checkpoint does not
        # launder into ceremony_failed. Success enums stay narrow.
        if outcome == "error":
            outcome = _maybe_reclassify_ceremony_failed(
                outcome=outcome,
                failed_stage=failed_stage,
                commit_landed=landed_before_salvage,
                self_verify_result=self_verify_result,
                run_ctx=run_ctx,
            )

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

        # T26 / implementation note S13: handoff_ready → lane status "review" + expose
        # one-call gate close. Error/needs_guidance leave status untouched
        # (no false merged). [OBS-08][CON-04]
        lane_status_transition: dict[str, Any] | None = None
        next_lane_action: dict[str, Any] | None = None
        if outcome == "handoff_ready":
            lane_status_transition = _mark_lane_review_on_handoff_ready(
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=Path(worktree_path),
            )
            next_lane_action = next_lane_close_action(task_ref=task_ref, lane_id=lane_id)

        # implementation note R2/R3: harvest review-lane GROK_REVIEW_FINDINGS_JSON before listing
        # so recorded rows appear in payload["findings"]. A review lane already
        # computed + reused this during outcome classification (do not re-run); every
        # other terminal computes it here. Fail-open — never changes outcome /
        # failed_stage.
        if findings_harvest is None:
            harvest_report: dict[str, Any] | str | None = None
            try:
                harvest_report = _latest_worker_report(task_ref, lane_id)
            except Exception:  # noqa: BLE001 — report load is best-effort for harvest
                harvest_report = None
            findings_harvest = _harvest_review_findings(
                _safe_final_result(run_ctx),
                harvest_report,
                task_ref=task_ref,
                lane_id=lane_id,
                session=session,
                orchestrator_root=Path(orchestrator_root),
            )

        # Typed harvest verdict [AGT-21][OBS-08]: distinguish reviewed-clean
        # (validated empty array) from harvest-broken so a lost review can never
        # look like a clean merge signal. Emit the real verdict for any review
        # lane (or any terminal that classified on a review product); only
        # non-review terminals emit not_attempted. Findings suppression stays
        # keyed on review_product_harvest alone — widening it would erase
        # pre-existing open rows on unrelated review-lane failure paths.
        if review_lane or review_product_harvest:
            harvest_verdict = _harvest_verdict(findings_harvest)
        else:
            harvest_verdict = HARVEST_VERDICT_NOT_ATTEMPTED
        # T4: surface worker-recorded findings on every terminal payload — EXCEPT
        # when a review-product harvest failed: backfilling pre-existing open rows
        # for the task_ref would make a broken review look productive. Implement
        # lanes and non-review terminals keep the usual collect-on-terminal path.
        findings_suppressed_reason: str | None = None
        findings_open_count_at_terminal: int | None = None
        if review_product_harvest and harvest_verdict == HARVEST_VERDICT_HARVEST_FAILED:
            # Count open rows for the discriminator only — do not resurrect them
            # into the payload findings list [OBS-08][AGT-21].
            findings_open_count_at_terminal = len(_collect_pass_findings(task_ref=task_ref))
            findings_suppressed_reason = "harvest_failed"
            pass_findings: list[dict[str, Any]] = []
        else:
            pass_findings = _collect_pass_findings(task_ref=task_ref)
        payload = _payload(
            outcome,
            run_ctx=run_ctx,
            error=error_reason,
            slice_closure=slice_closure,
            self_verify=self_verify_result,
            composer_violation=composer_violation_result,
            continuation_dispatch_id=continuation_dispatch_id,
            failed_stage=failed_stage,
            review=review_discriminator,
            findings=pass_findings,
            raw_tail=review_raw_tail,
        )
        payload["findings_harvest"] = findings_harvest
        payload["harvest_verdict"] = harvest_verdict
        # E8ROUTE-LOCAL-02: typed channel distinct from free-form error prose.
        if execute_failure_reason:
            payload["failure_reason"] = execute_failure_reason
        if findings_suppressed_reason is not None:
            payload["findings_suppressed_reason"] = findings_suppressed_reason
            payload["findings_open_count_at_terminal"] = findings_open_count_at_terminal
        # Degrade loudly [AGT-10]: handoff_submit_failed is a fact about the submit
        # subprocess, not a property of one outcome. Surface the discriminator on
        # every terminal payload whenever a submit was attempted and returned nonzero.
        # handoff_attempted is required because handoff_exit defaults to 1 (fail-closed).
        if (
            run_ctx is not None
            and getattr(run_ctx, "handoff_attempted", False)
            and getattr(run_ctx, "handoff_exit", 0) not in (0, None)
        ):
            payload["handoff_submit_failed"] = True
            payload["handoff_exit"] = int(run_ctx.handoff_exit)
        if salvage_candidate is not None:
            payload["salvage_candidate"] = salvage_candidate
        if lane_status_transition is not None:
            payload["lane_status"] = lane_status_transition.get("status")
            payload["lane_status_transition"] = lane_status_transition
        if next_lane_action is not None:
            payload["next_lane_action"] = next_lane_action
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
