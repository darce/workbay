"""Shared helpers for lifecycle handler modules."""

from __future__ import annotations

from dataclasses import dataclass
import importlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import resolver
from receipts import ReceiptWarning

# Override hook for tests and operators that want to point lifecycle
# at a different ``mcp-workbay-handoff`` invocation (e.g. uvx-launched
# isolated build, test stub).
MCP_BIN_ENV = "MCP_WORKBAY_HANDOFF_BIN"
DEFAULT_MCP_BIN = "mcp-workbay-handoff"
DEFAULT_HANDOFF_TIMEOUT = 6.0
DEFAULT_LIFECYCLE_AGENT = "workbay-lifecycle"
DEFAULT_AGENT_ENV = "WORKBAY_HANDOFF_DEFAULT_AGENT"

_FALLBACK_HARNESS_TRANSCRIPT_ENV_VARS: tuple[str, ...] = (
    "CLAUDE_SESSION_TRANSCRIPT_PATH",
    "CODEX_SESSION_TRANSCRIPT_PATH",
    "VSCODE_TARGET_SESSION_LOG",
    "GROK_SESSION_TRANSCRIPT_PATH",
)

# Mirrors the handoff knob so operators and tests can point the doctor probe at
# a stub orchestrator binary the same way they redirect ``mcp-workbay-handoff``.
MCP_ORCHESTRATOR_BIN_ENV = "MCP_WORKBAY_ORCHESTRATOR_BIN"
DEFAULT_MCP_ORCHESTRATOR_BIN = "mcp-workbay-orchestrator"
DEFAULT_ORCHESTRATOR_TIMEOUT = 6.0

FALLBACK_ACTIVE_STATUSES: tuple[str, ...] = ("in_progress", "review", "blocked")


def live_active_statuses() -> tuple[str, ...]:
    """Resolver/renderer-active task statuses.

    Prefers the canonical ``workbay_handoff_mcp.shared_primitives.LIVE_ACTIVE_STATUSES``
    symbol (internal). Falls back to a hardcoded tuple when the
    handoff package is unavailable, e.g. in workbay-bootstrap consumer-profile
    installs that don't ship handoff. Mirrors the contract documented at
    ``packages/workbay-system/docs/workbay/contracts/workbay-handoff-mcp.md``:
    ``status=done`` rows are archive-eligible only and never active-eligible.
    """
    try:
        shared_primitives = importlib.import_module("workbay_handoff_mcp.shared_primitives")
    except Exception:
        return FALLBACK_ACTIVE_STATUSES
    statuses = getattr(shared_primitives, "LIVE_ACTIVE_STATUSES", None)
    if not isinstance(statuses, tuple) or not all(isinstance(status, str) for status in statuses):
        return FALLBACK_ACTIVE_STATUSES
    return statuses


def snapshot_is_live(active: dict[str, Any]) -> bool:
    """Return True when a CURRENT_TASK ``active`` block represents live work.

    Older snapshots did not always carry ``status``; treat those as live so
    the ambiguity guard remains conservative. ``status=done`` (and any
    other non-live label) is a stale projection.
    """
    status = active.get("status")
    if not isinstance(status, str) or not status:
        return True
    return status in live_active_statuses()


def snapshot_is_live_for_task(task_ref: str, workspace_root: Path) -> bool:
    """Return True when the per-task projection for ``task_ref`` is live.

    Reads ``<workspace_root>/.task-state/current/<task_ref>.json`` and
    applies the same liveness vocabulary as :func:`snapshot_is_live`. Used
    by readers that operate under the v2 ``workspace_ambiguous`` shape,
    where ``CURRENT_TASK.json`` no longer carries a single ``active``
    block — the per-task file is the source of truth for "is this
    specific task still live work?".

    Missing file or unreadable/malformed JSON yields ``False`` (fail-safe
    non-live): the writer reaps the projection on ``archive``, so absence
    is the canonical "not live" signal, and a one-tick non-live read on
    transient corruption is preferable to raising into call sites that
    have no degrade-with-warning surface for this file.

    A projection without a ``status`` field is treated as live so the
    helper inherits :func:`snapshot_is_live`'s conservative-fallback
    semantic that the ambiguity guard relies on.

    ``task_ref`` is expected to match the canonical handoff grammar
    (``^[A-Z][A-Z0-9_-]+$``); a value containing path separators or
    leading dots returns ``False`` without touching the filesystem so a
    regression in upstream validation cannot turn this helper into a
    disk-traversal surface.
    """
    if not task_ref or task_ref.startswith(".") or any(
        sep in task_ref for sep in ("/", "\\", "\x00")
    ):
        return False
    target = workspace_root / ".task-state" / "current" / f"{task_ref}.json"
    try:
        text = target.read_text(encoding="utf-8")
    except OSError:
        return False
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    return snapshot_is_live(payload)


@dataclass(frozen=True)
class GitFacts:
    branch: str
    head: str
    derived_task_ref: str | None
    dirty_summary: dict[str, int]


def _venv_console_script(worktree_path: Path, name: str) -> Path | None:
    """Return a console script from a worktree venv when provisioned."""
    candidates = [
        worktree_path / ".venv" / "bin" / name,
        worktree_path / ".venv" / "Scripts" / f"{name}.exe",
        worktree_path / ".venv" / "Scripts" / name,
    ]
    for candidate in candidates:
        # Require the executable bit so a present-but-non-executable script
        # falls through rather than returning an unspawnable path.
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _handoff_cli_search_roots(start: Path | None = None) -> list[Path]:
    """Worktree-local venv first, then canonical workspace venv."""
    cwd = Path(start) if start is not None else Path.cwd()
    roots: list[Path] = []
    worktree = resolver.current_worktree(cwd)
    if worktree is not None:
        roots.append(worktree)
    canonical = resolver.canonical_workspace_root(cwd)
    if canonical is not None and canonical not in roots:
        roots.append(canonical)
    return roots


def mcp_handoff_bin(start: Path | None = None) -> str:
    """Resolve the handoff CLI, preferring the workspace ``.venv`` console script.

    ``start`` scopes the ``.venv`` search to a specific worktree (defaults to
    the process cwd). Callers that probe a *different* repo than the cwd — e.g.
    ``doctor``'s ``_probe_venv`` — must pass it so resolution stays hermetic to
    the probed repo instead of leaking the running worktree's environment.
    """
    override = os.environ.get(MCP_BIN_ENV)
    if override:
        return override
    for root in _handoff_cli_search_roots(start):
        script = _venv_console_script(root, DEFAULT_MCP_BIN)
        if script is not None:
            return str(script)
    return DEFAULT_MCP_BIN


_PROVISION_ENV_HINT = (
    "run `make provision-env` to provision the workspace .venv "
    "with mcp-workbay-handoff"
)


def handoff_cli_required_preflight_error() -> str | None:
    """Return an actionable error when a required gate cannot spawn the handoff CLI."""
    bin_path = mcp_handoff_bin()
    candidate = Path(bin_path)
    if candidate.is_file() and os.access(candidate, os.X_OK):
        return None
    if shutil.which(bin_path):
        return None
    if os.environ.get(MCP_BIN_ENV):
        return (
            f"handoff CLI override {bin_path!r} is missing or not executable; "
            f"{_PROVISION_ENV_HINT}"
        )
    return (
        f"handoff CLI is not available in the workspace .venv ({bin_path!r}); "
        f"{_PROVISION_ENV_HINT}"
    )


def handoff_command_argv(repo: Path, *argv: str) -> list[str]:
    workspace = resolver.canonical_workspace_root(repo) or repo
    return [mcp_handoff_bin(), "--workspace-root", str(workspace), *argv]


def _fallback_harness_transcript_env_vars() -> tuple[str, ...]:
    return _FALLBACK_HARNESS_TRANSCRIPT_ENV_VARS


def _harness_transcript_env_var_names(repo: Path | None) -> list[str]:
    """Single-source harness transcript env var names from the handoff contract."""
    workspace: Path | None = None
    if repo is not None:
        workspace = resolver.canonical_workspace_root(repo) or repo
    if workspace is None:
        workspace = Path.cwd()
    try:
        compaction_contract = importlib.import_module(
            "workbay_handoff_mcp.compaction_contract"
        )
        contract = compaction_contract.load_compaction_contract(workspace)
        return [
            rule.env_var for rule in contract.transcript_discovery.values()
        ]
    except Exception:
        return list(_FALLBACK_HARNESS_TRANSCRIPT_ENV_VARS)


def _harness_transcript_env_present(
    env: dict[str, str], repo: Path | None = None
) -> bool:
    for var in _harness_transcript_env_var_names(repo):
        if env.get(var, "").strip():
            return True
    return False


def handoff_subprocess_env(
    repo: Path | None = None,
    *,
    base: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build subprocess env for lifecycle handoff CLI invocations.

    Injects ``WORKBAY_HANDOFF_DEFAULT_AGENT=workbay-lifecycle`` only when no
    harness transcript env var is set and the operator has not already set the
    default-agent knob (Q1: never mask a resolvable harness identity).
    """
    merged = dict(base if base is not None else os.environ)
    if merged.get(DEFAULT_AGENT_ENV, "").strip():
        return merged
    if _harness_transcript_env_present(merged, repo):
        return merged
    merged[DEFAULT_AGENT_ENV] = DEFAULT_LIFECYCLE_AGENT
    return merged


def run_handoff_subprocess(
    repo: Path,
    argv: list[str],
    *,
    timeout: float | None = None,
    cwd: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a handoff-CLI argv with lifecycle attribution env injected."""
    return run_subprocess(
        argv,
        timeout=timeout,
        cwd=cwd,
        env=handoff_subprocess_env(repo),
    )


def worktree_write_context_argv(
    repo: Path, *, task_ref: str | None = None
) -> list[str]:
    """Thread authoritative per-call actor provenance for projecting writes."""
    argv: list[str] = []
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    branch = resolver.current_branch(repo)
    head = resolver.head_sha(repo)
    if branch:
        argv.extend(["--branch", branch])
    if head:
        argv.extend(["--commit-sha", head])
    return argv


def local_live_handoff_row_exists(repo: Path, task_ref: str) -> bool | None:
    """Return whether ``task_ref`` has a live row in the canonical handoff DB.

    ``False`` when the DB is absent or the row is missing/archived;
    ``None`` when sqlite is unreadable (caller may fall back to MCP).
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    db_path = workspace / ".task-state" / "handoff.db"
    if not db_path.is_file():
        return False
    statuses = live_active_statuses()
    try:
        with sqlite3.connect(str(db_path)) as conn:
            placeholders = ",".join(["?"] * len(statuses))
            row = conn.execute(
                f"SELECT 1 FROM handoff_state WHERE task_ref = ? "
                f"AND status IN ({placeholders})",
                (task_ref, *statuses),
            ).fetchone()
        return row is not None
    except sqlite3.Error:
        return None


def run_subprocess(
    argv: list[str],
    *,
    timeout: float | None = None,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run ``argv`` capturing stdout/stderr; never raises on non-zero."""
    try:
        return subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
            cwd=cwd,
            env=env,
        )
    except FileNotFoundError:
        return subprocess.CompletedProcess(
            args=argv, returncode=127, stdout="", stderr=f"command not found: {argv[0]}"
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args=argv, returncode=124, stdout="", stderr="timed out"
        )


def handoff_timeout() -> float:
    """Wall-clock budget for a single handoff-CLI subprocess on the projection
    write/replay paths.

    A *hung* (not crashed) handoff CLI is the worst failure mode for an
    integration point (Release It!): with no timeout it never returns, so it
    never becomes the synthetic ``124`` that ``run_subprocess`` raises on
    ``TimeoutExpired`` -> the projection circuit breaker (which keys on
    unreachable returncodes), its half-open probe, and the replay
    ``--max-seconds`` governor can never fire, and a detached auto-drain leaks a
    hung process. Bounding each call converts a hang into ``pending``, which the
    breaker and abort-on-pending already handle. Overridable via
    ``WORKBAY_PROJECTION_HANDOFF_TIMEOUT`` (seconds) for tests; defaults to
    ``DEFAULT_HANDOFF_TIMEOUT``.
    """
    raw = os.environ.get("WORKBAY_PROJECTION_HANDOFF_TIMEOUT")
    if raw:
        try:
            value = float(raw)
        except ValueError:
            return DEFAULT_HANDOFF_TIMEOUT
        if value > 0:
            return value
    return DEFAULT_HANDOFF_TIMEOUT


def run_checklist_sync(
    repo: Path, task_ref: str, *, apply: bool = True
) -> dict[str, Any]:
    """Best-effort post-step: invoke ``sync-task-plan-checklist`` for ``task_ref``.

    Spawns ``python <lifecycle-pkg> sync-task-plan-checklist --task <ref>
    [--apply] --quiet`` as a subprocess so the sync runs in its own
    interpreter (clean argv, isolated stdout for JSON capture). The
    returned dict is the slim shape suitable for embedding under the
    parent receipt's ``checklist_sync`` key. Never raises — every
    failure path collapses to ``{"ok": False, "warning": "<reason>"}``
    so a malformed plan never blocks a real ``slice-commit`` /
    ``task-finish`` close (internal: failure-as-warning).

    ``apply`` defaults to True (write the ticks) for the persisting callers
    (``slice-commit``, ``finalize-plan``). Pass ``apply=False`` for a
    read-only verify sweep: the returned ``ticked`` then counts boxes that
    *would* flip but ``applied`` stays False and the plan file is untouched.
    ``task-finish`` uses this post-merge so it never writes ticks into a
    worktree it is about to discard (which would silently lose them — the
    persisting sweep must run pre-merge via ``finalize-plan``).
    """
    lifecycle_pkg = Path(__file__).resolve().parent.parent
    argv = [
        sys.executable,
        str(lifecycle_pkg),
        "sync-task-plan-checklist",
        "--task", task_ref,
        *(["--apply"] if apply else []),
        "--quiet",
    ]
    try:
        proc = subprocess.run(
            argv,
            cwd=str(repo),
            capture_output=True,
            text=True,
            check=False,
            timeout=30.0,
            env=os.environ.copy(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "warning": f"sync_invoke_failed: {exc!s}"}
    if not proc.stdout.strip():
        return {
            "ok": False,
            "warning": (
                f"sync_no_output: rc={proc.returncode} "
                f"stderr={proc.stderr.strip()[:200]!r}"
            ),
        }
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError) as exc:
        return {"ok": False, "warning": f"sync_stdout_unparseable: {exc!s}"}
    error = payload.get("error")
    # ``plan_unresolved`` and ``plan_not_found`` are soft-skip states for
    # an auto-fire post-step: the task simply has no plan to sync (e.g.
    # MAINT-* rows that never set ``task_plan_path``, or rows whose
    # stored plan path was moved). Treat them as a no-op so the parent
    # receipt does not surface a spurious warning.
    if error in ("plan_unresolved", "plan_not_found"):
        return {
            "ok": True,
            "ticked": 0,
            "kept": 0,
            "unresolved": 0,
            "already_ticked": 0,
            "applied": False,
            "skipped": error,
            "plan_path": payload.get("plan_path"),
        }
    slim: dict[str, Any] = {
        "ok": bool(payload.get("ok")),
        "ticked": int(payload.get("ticked", 0) or 0),
        "kept": int(payload.get("kept", 0) or 0),
        "unresolved": int(payload.get("unresolved", 0) or 0),
        "already_ticked": int(payload.get("already_ticked", 0) or 0),
        "applied": bool(payload.get("applied", False)),
        "plan_path": payload.get("plan_path"),
        "plan_source": payload.get("plan_source"),
    }
    warning = payload.get("warning") or error
    if warning:
        slim["warning"] = warning
    return slim


def run_handoff_json(
    repo: Path,
    *,
    argv: list[str],
    timeout_seconds: float = DEFAULT_HANDOFF_TIMEOUT,
    field: str = "handoff",
) -> tuple[Any | None, ReceiptWarning | None]:
    proc = run_handoff_subprocess(
        repo, handoff_command_argv(repo, *argv), timeout=timeout_seconds
    )
    if proc.returncode == 124:
        return None, ReceiptWarning(
            field=field,
            reason="timeout",
            exception_type="TimeoutExpired",
        )
    if proc.returncode != 0:
        return None, ReceiptWarning(field=field, reason="unavailable")
    try:
        return json.loads(proc.stdout), None
    except (ValueError, json.JSONDecodeError):
        return None, ReceiptWarning(
            field=field,
            reason="malformed",
            exception_type="JSONDecodeError",
        )


def mcp_orchestrator_bin() -> str:
    return os.environ.get(MCP_ORCHESTRATOR_BIN_ENV) or DEFAULT_MCP_ORCHESTRATOR_BIN


def orchestrator_command_argv(repo: Path, *argv: str) -> list[str]:
    workspace = resolver.canonical_workspace_root(repo) or repo
    return [mcp_orchestrator_bin(), "--workspace-root", str(workspace), *argv]


def run_orchestrator_json(
    repo: Path,
    *,
    argv: list[str],
    timeout_seconds: float = DEFAULT_ORCHESTRATOR_TIMEOUT,
    field: str = "orchestrator",
) -> tuple[Any | None, ReceiptWarning | None]:
    """internal: bounded JSON probe for the orchestrator CLI.

    Mirrors :func:`run_handoff_json` exactly so the doctor probe can treat
    both endpoints the same way (timeout-as-warning, non-zero-as-warning,
    JSON-malformed-as-warning). Callers select the read-only orchestrator
    subcommand (e.g. ``["orchestrator-status"]``) — the helper does not
    pick one so a future cheaper surface stays trivially swappable.
    """
    proc = run_subprocess(
        orchestrator_command_argv(repo, *argv), timeout=timeout_seconds
    )
    if proc.returncode == 124:
        return None, ReceiptWarning(
            field=field,
            reason="timeout",
            exception_type="TimeoutExpired",
        )
    if proc.returncode != 0:
        return None, ReceiptWarning(field=field, reason="unavailable")
    try:
        return json.loads(proc.stdout), None
    except (ValueError, json.JSONDecodeError):
        return None, ReceiptWarning(
            field=field,
            reason="malformed",
            exception_type="JSONDecodeError",
        )


def repo_root() -> Path | None:
    """Return ``git rev-parse --show-toplevel`` (cwd-anchored) or None."""
    proc = run_subprocess(["git", "rev-parse", "--show-toplevel"])
    if proc.returncode != 0:
        return None
    return Path(proc.stdout.strip()) if proc.stdout.strip() else None


def find_hooks_dir(root: Path) -> Path | None:
    """Return the hooks directory shared by ``check_main_clean.py`` and
    the lifecycle dirty-main probes, supporting both layouts:

    - monorepo: ``<root>/packages/workbay-system/scripts/hooks``
    - bootstrapped consumer: ``<root>/scripts/hooks``

    The first existing directory wins; ``None`` when neither is present
    so callers can fail soft (degrade to "no dirty paths") rather than
    silently miss the consumer layout and let dirty-main slip past
    close-check / the doctor facet.
    """
    monorepo = root / "packages" / "workbay-system" / "scripts" / "hooks"
    if monorepo.is_dir():
        return monorepo
    consumer = root / "scripts" / "hooks"
    if consumer.is_dir():
        return consumer
    return None


def _live_handoff_rows(repo: Path) -> list[dict[str, Any]]:
    """Return live handoff rows from the canonical ``handoff-rows`` CLI.

    Returns an empty list on any failure (CLI missing, non-zero exit,
    malformed JSON, timeout) so callers can degrade without crashing
    lifecycle commands.
    """
    statuses = list(live_active_statuses())
    argv = handoff_command_argv(repo, "handoff-rows", "--status", *statuses)
    proc = run_handoff_subprocess(repo, argv, timeout=DEFAULT_HANDOFF_TIMEOUT)
    if proc.returncode != 0:
        return []
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _live_task_refs(repo: Path) -> set[str]:
    """Return the set of live task refs from the handoff registry.

    Shells out to the canonical ``mcp-workbay-handoff handoff-rows
    --status <LIVE_ACTIVE_STATUSES>`` CLI subcommand so the lookup runs
    in the handoff package's own configured runtime — the only context
    in which the SQLite DB path resolves. The previous in-process
    ``list_handoff_rows`` import path returned an empty set in every
    fresh lifecycle CLI process because ``configure_runtime`` had not
    been called (internal).

    ``status=done`` rows are excluded at the read boundary;
    ``task_archives`` rows are already excluded by table semantics.

    Returns an empty set on any failure (CLI missing, non-zero exit,
    malformed JSON, timeout) so the resolver can degrade to the
    no-context fallback (shortest-prefix) without crashing lifecycle
    commands.
    """
    refs: set[str] = set()
    for row in _live_handoff_rows(repo):
        ref = row.get("task_ref")
        if isinstance(ref, str) and ref:
            refs.add(ref)
    return refs


def gather_git_facts(repo: Path) -> GitFacts:
    """Collect branch/head/task-ref/dirty facts shared by read-only handlers.

    Threads the live task-ref registry into the resolver so branches like
    ``feature/<base>-<n>-fu-<slug>`` resolve to the registered follow-up
    ref instead of collapsing onto the base when both are live. See
    internal for the registered-ref selector contract.
    """
    branch = resolver.current_branch(repo) or ""
    known = _live_task_refs(repo)
    return GitFacts(
        branch=branch,
        head=resolver.head_sha(repo) or "",
        derived_task_ref=resolver.derive_task_ref(branch, known_task_refs=known),
        dirty_summary=resolver.dirty_summary(repo),
    )


def emit(receipt: dict[str, Any]) -> None:
    """Write ``receipt`` as a single JSON line to stdout."""
    json.dump(receipt, sys.stdout, sort_keys=False)
    sys.stdout.write("\n")


# ---------------------------------------------------------------------------
# Workspace-summary reader (internal)
#
# The live ``CURRENT_TASK.json`` writer emits ``schema_version: 2``
# unconditionally (derive-on-read with explicit ``single`` /
# ``workspace_ambiguous`` / ``none`` shapes). Every reader call goes
# through this layer instead of branching on schema_version itself.
# ---------------------------------------------------------------------------


class WorkspaceSummaryParseError(ValueError):
    """Raised when a CURRENT_TASK.json payload is malformed.

    A half-written or corrupt file is operator-actionable (e.g. crash
    mid-write); silently degrading to ``shape="none"`` would mask the
    corruption and let a reader pretend there is no active task when
    there is. Callers that genuinely want degrade-on-error semantics
    must catch this explicitly.
    """


@dataclass(frozen=True)
class WorkspaceSummaryView:
    """Normalized view of a v2 workspace-summary payload.

    Field semantics:

    - ``shape``: ``"single"``, ``"workspace_ambiguous"``, or ``"none"``.
      Shape names match the v2 derive-on-read builder
      (``_render_workspace_summary_from_per_task_files`` in
      ``current_task_rendering.py``).
    - ``task_ref``: populated only when ``shape == "single"``.
    - ``active``: the single-task payload when ``shape == "single"`` —
      the ``task_projection_schema_version=1`` per-task projection.
    - ``tasks``: populated only when ``shape == "workspace_ambiguous"``.
      Empty list otherwise (never ``None`` — callers iterate it).
    """

    shape: str
    task_ref: str | None
    active: dict[str, Any] | None
    tasks: list[dict[str, Any]]


_NONE_VIEW = WorkspaceSummaryView(
    shape="none",
    task_ref=None,
    active=None,
    tasks=[],
)


def _coerce_v2(payload: dict[str, Any]) -> WorkspaceSummaryView:
    shape = payload.get("shape")
    if shape == "none":
        return WorkspaceSummaryView(
            shape="none",
            task_ref=None,
            active=None,
            tasks=[],
        )
    if shape == "single":
        # internal: a v2 'single' shape requires a dict 'active'
        # block AND a non-empty top-level 'task_ref' that matches
        # active['task_ref']. Without this, readers (task_finish,
        # shell_out) silently trust the top-level task_ref and resolve
        # to a task with no active projection.
        active = payload.get("active")
        if not isinstance(active, dict):
            raise WorkspaceSummaryParseError(
                "v2 shape='single' requires a dict 'active' block"
            )
        task_ref = payload.get("task_ref")
        if not isinstance(task_ref, str) or not task_ref:
            raise WorkspaceSummaryParseError(
                "v2 shape='single' requires a non-empty top-level 'task_ref'"
            )
        active_task_ref = active.get("task_ref")
        if active_task_ref != task_ref:
            raise WorkspaceSummaryParseError(
                "v2 shape='single' requires top-level task_ref to match active.task_ref "
                f"(top-level={task_ref!r}, active={active_task_ref!r})"
            )
        return WorkspaceSummaryView(
            shape="single",
            task_ref=task_ref,
            active=active,
            tasks=[],
        )
    if shape == "workspace_ambiguous":
        # internal: 'tasks' is required and must be a list. A
        # missing/non-list 'tasks' silently filtered to [] would make a
        # malformed payload look like a clean ambiguity surface.
        raw_tasks = payload.get("tasks")
        if not isinstance(raw_tasks, list):
            raise WorkspaceSummaryParseError(
                "v2 shape='workspace_ambiguous' requires a list 'tasks' field"
            )
        tasks = [t for t in raw_tasks if isinstance(t, dict)]
        return WorkspaceSummaryView(
            shape="workspace_ambiguous",
            task_ref=None,
            active=None,
            tasks=tasks,
        )
    raise WorkspaceSummaryParseError(
        f"Unknown v2 workspace-summary shape: {shape!r}"
    )


def derive_workspace_summary_view(repo: Path) -> WorkspaceSummaryView:
    """Derive the workspace summary by shelling out to ``render-handoff``.

    internal: closes the internal derive-on-read contract for
    the four lifecycle handler readers (``task_start``, ``context``,
    ``task_finish``, ``shell_out``). Instead of reading the on-disk
    ``CURRENT_TASK.json``, each reader calls this helper, which invokes
    ``mcp-workbay-handoff render-handoff --kind=current_task --no-write``
    and parses the envelope's ``current_task_json`` field through
    :func:`load_workspace_summary`.

    The pure-read CLI path (locked by ``test_render_handoff_pure_read``)
    guarantees no DB mutation and no file mtime change, so this helper
    is safe to invoke from read-only and mutating handlers alike.

    Failure modes fail-open with the ``shape="none"`` sentinel, matching
    the historical degrade-on-error semantics of the file-based reader:

    - CLI unavailable / timeout / non-zero exit: ``shape="none"``.
    - Envelope JSON malformed: ``shape="none"``.
    - ``current_task_json`` payload malformed or unknown schema:
      ``shape="none"`` (the on-disk file reader silently swallowed
      ``WorkspaceSummaryParseError`` at every reader site; the derived
      path mirrors that contract).
    """
    proc = run_handoff_subprocess(
        repo,
        handoff_command_argv(repo, "render-handoff", "--kind=current_task", "--no-write"),
        timeout=DEFAULT_HANDOFF_TIMEOUT,
    )
    if proc.returncode != 0:
        return _NONE_VIEW
    try:
        envelope = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return _NONE_VIEW
    if not isinstance(envelope, dict):
        return _NONE_VIEW
    data = envelope.get("data")
    if not isinstance(data, dict):
        return _NONE_VIEW
    current_task_json = data.get("current_task_json")
    if not isinstance(current_task_json, str) or not current_task_json:
        return _NONE_VIEW
    try:
        payload = json.loads(current_task_json)
    except json.JSONDecodeError:
        return _NONE_VIEW
    if not isinstance(payload, dict):
        return _NONE_VIEW
    try:
        return load_workspace_summary(payload)
    except WorkspaceSummaryParseError:
        return _NONE_VIEW


def load_workspace_summary(
    source: Path | dict[str, Any],
) -> WorkspaceSummaryView:
    """Load a workspace summary from a v2 ``CURRENT_TASK.json``.

    Inputs:

    - ``Path``: read JSON from disk. A missing path returns the
      ``shape="none"`` view (no active task is the same semantic as no
      file). Corrupt JSON raises :class:`WorkspaceSummaryParseError`.
    - ``dict``: an already-parsed payload (e.g. from a subprocess that
      returned JSON over stdout).

    Output: a :class:`WorkspaceSummaryView` whose ``shape`` field is
    the canonical vocabulary. Anything other than
    ``schema_version: 2`` (including the retired v1 shape) raises
    :class:`WorkspaceSummaryParseError`.
    """
    if isinstance(source, Path):
        if not source.exists():
            return _NONE_VIEW
        try:
            text = source.read_text(encoding="utf-8")
        except OSError as exc:
            raise WorkspaceSummaryParseError(
                f"Failed to read {source}: {exc}"
            ) from exc
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise WorkspaceSummaryParseError(
                f"Malformed JSON in {source}: {exc}"
            ) from exc
    else:
        payload = source

    if not isinstance(payload, dict):
        raise WorkspaceSummaryParseError(
            f"Workspace summary payload must be a dict, got {type(payload).__name__}"
        )

    schema_version = payload.get("schema_version")
    if schema_version == 2:
        return _coerce_v2(payload)
    raise WorkspaceSummaryParseError(
        f"Unsupported workspace-summary schema_version: {schema_version!r}"
    )


_AUTO_DRAIN_MAX_ENTRIES = 50
_AUTO_REAP_STAMP_REL = Path(".task-state") / "auto-reap-stale-maint.stamp"
_AUTO_REAP_BACKOFF_SECONDS = 24 * 60 * 60


def maybe_auto_drain_projection_spool(command: str) -> None:
    """Spawn a detached, budgeted spool drain before most lifecycle commands."""
    if command == "project-events-replay":
        return
    if os.environ.get("WORKBAY_DISABLE_AUTO_DRAIN", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        return
    repo = resolver.repo_root()
    if repo is None:
        return
    from handlers import project_events_replay as replay_mod  # noqa: PLC0415

    if replay_mod.auto_drain_skip_reason(repo) is not None:
        return
    if not replay_mod.has_drainable_spool(repo):
        return
    lifecycle_pkg = Path(__file__).resolve().parent.parent
    try:
        subprocess.Popen(
            [
                sys.executable,
                str(lifecycle_pkg),
                "project-events-replay",
                "--max-entries",
                str(_AUTO_DRAIN_MAX_ENTRIES),
            ],
            cwd=str(repo),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        # Best-effort detached drain: a spawn failure (EMFILE, fork-limit,
        # missing interpreter) must never crash the foreground lifecycle
        # command. The spool stays put; the next invocation retries.
        return


_DEAD_LETTER_DRAIN_BACKOFF_SECONDS_DEFAULT = 300.0
_DEAD_LETTER_AUTO_DRAIN_MAX_ENTRIES = 50


def _auto_drain_disabled() -> bool:
    return os.environ.get("WORKBAY_DISABLE_AUTO_DRAIN", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _dead_letter_drain_backoff_seconds() -> float:
    raw = os.environ.get("WORKBAY_DEAD_LETTER_DRAIN_BACKOFF_SECONDS")
    if raw is None:
        return _DEAD_LETTER_DRAIN_BACKOFF_SECONDS_DEFAULT
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _DEAD_LETTER_DRAIN_BACKOFF_SECONDS_DEFAULT


def _task_is_archived(repo: Path, task_ref: str) -> bool:
    """Best-effort: True when ``task_ref`` has an archived snapshot.

    Shells out to the canonical ``archive --operation get`` CLI (the same runtime
    in which the SQLite DB path resolves). Any failure — CLI missing, non-zero
    exit, ``ok:false`` (not archived), malformed JSON — degrades to False so a
    dead-letter entry is replayed normally rather than silently discarded.
    """
    if not task_ref:
        return False
    argv = handoff_command_argv(
        repo, "archive", "--operation", "get", "--task-ref", task_ref
    )
    proc = run_handoff_subprocess(repo, argv, timeout=DEFAULT_HANDOFF_TIMEOUT)
    if proc.returncode != 0:
        return False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("ok") is True


def maybe_auto_drain_dead_letter(command: str) -> None:
    """Spawn a detached, backoff-gated dead-letter drain before lifecycle commands.

    Sibling of :func:`maybe_auto_drain_projection_spool` for the dead-letter sink
    (the spool's overflow accumulator, which previously had manual-only replay).
    When the live sink is non-empty OR an orphan drain snapshot exists and the
    backoff window has elapsed, a detached ``project-events-replay
    --drain-dead-letter`` child claims the sink and any orphan snapshots by rename
    (so a concurrent ``dead_letter`` append is never lost), tombstone-discards
    archived-task / unsupported / repeatedly-rejected rows, replays the rest, and
    re-appends any remainder to the LIVE sink so nothing is stranded. All the
    blocking work (archived-task probes, replay subprocesses) runs in the detached
    child; the foreground only checks counters and spawns. Every step is
    best-effort: a failure must never crash the foreground command.
    """
    if command == "project-events-replay":
        return
    if _auto_drain_disabled():
        return
    repo = resolver.repo_root()
    if repo is None:
        return
    import projection_queue  # noqa: PLC0415 -- lazy: mirror the spool sibling

    count, _capped = projection_queue.dead_letter_count(repo)
    orphan, _orphan_capped = projection_queue.dead_letter_orphan_count(repo)
    if count <= 0 and orphan <= 0:
        return
    now = time.time()
    last = projection_queue.read_dead_letter_drain_epoch(repo)
    if last is not None and (now - last) < _dead_letter_drain_backoff_seconds():
        return
    # Stamp the backoff/last-drain state before spawning so a concurrent lifecycle
    # command does not spawn a second overlapping drain before this child runs.
    projection_queue.record_dead_letter_drain(repo, epoch=now)
    lifecycle_pkg = Path(__file__).resolve().parent.parent
    try:
        subprocess.Popen(
            [
                sys.executable,
                str(lifecycle_pkg),
                "project-events-replay",
                "--drain-dead-letter",
                "--max-entries",
                str(_DEAD_LETTER_AUTO_DRAIN_MAX_ENTRIES),
            ],
            cwd=str(repo),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        # Best-effort detached drain: a spawn failure must never crash the
        # foreground command. The sink/orphans persist and drain on a later pass.
        return


def maybe_auto_reap_stale_rows(command: str) -> None:
    """Spawn a detached stale-row reap on a coarse maintenance cadence."""
    if command == "task-reap":
        return
    if os.environ.get("WORKBAY_DISABLE_AUTO_REAP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        return
    repo = resolver.repo_root()
    if repo is None:
        return
    stamp = repo / _AUTO_REAP_STAMP_REL
    now = time.time()
    try:
        if stamp.exists() and now - stamp.stat().st_mtime < _AUTO_REAP_BACKOFF_SECONDS:
            return
        stamp.parent.mkdir(parents=True, exist_ok=True)
        stamp.write_text(f"{int(now)}\n", encoding="utf-8")
    except OSError:
        return
    lifecycle_pkg = Path(__file__).resolve().parent.parent
    try:
        subprocess.Popen(
            [
                sys.executable,
                str(lifecycle_pkg),
                "task-reap",
                "--apply",
                "--json",
            ],
            cwd=str(repo),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        return
