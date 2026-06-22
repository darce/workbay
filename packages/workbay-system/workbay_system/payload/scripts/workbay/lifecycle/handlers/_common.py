"""Shared helpers for lifecycle handler modules."""

from __future__ import annotations

from dataclasses import dataclass
import importlib
import json
import os
import subprocess
import sys
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


def mcp_handoff_bin() -> str:
    return os.environ.get(MCP_BIN_ENV) or DEFAULT_MCP_BIN


def handoff_command_argv(repo: Path, *argv: str) -> list[str]:
    workspace = resolver.canonical_workspace_root(repo) or repo
    return [mcp_handoff_bin(), "--workspace-root", str(workspace), *argv]


def run_subprocess(
    argv: list[str],
    *,
    timeout: float | None = None,
    cwd: str | None = None,
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
        )
    except FileNotFoundError:
        return subprocess.CompletedProcess(
            args=argv, returncode=127, stdout="", stderr=f"command not found: {argv[0]}"
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args=argv, returncode=124, stdout="", stderr="timed out"
        )


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
    proc = run_subprocess(handoff_command_argv(repo, *argv), timeout=timeout_seconds)
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
    statuses = list(live_active_statuses())
    argv = handoff_command_argv(repo, "handoff-rows", "--status", *statuses)
    proc = run_subprocess(argv, timeout=DEFAULT_HANDOFF_TIMEOUT)
    if proc.returncode != 0:
        return set()
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, list):
        return set()
    refs: set[str] = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
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
# Workspace-summary compatibility reader (internal)
#
# The live ``CURRENT_TASK.json`` writer emits ``schema_version: 1`` today
# (legacy single-active shape with an ``active`` block); implementation note will flip
# it to ``schema_version: 2`` (derive-on-read with explicit
# ``single`` / ``workspace_ambiguous`` / ``none`` shapes). implementation note needs
# to migrate readers (lifecycle CLI, compact-session hook) one at a time
# without depending on the writer flip — every reader call goes through
# this compat layer instead of branching on schema_version itself.
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
    """Normalized view of either a v1 or v2 workspace-summary payload.

    Field semantics:

    - ``shape``: ``"single"``, ``"workspace_ambiguous"``, or ``"none"``.
      Shape names match the v2 derive-on-read builder
      (``_render_workspace_summary_from_per_task_files`` in
      ``current_task_rendering.py``); v1 payloads are mapped onto the
      same vocabulary.
    - ``task_ref``: populated only when ``shape == "single"``.
    - ``active``: the single-task payload when ``shape == "single"``.
      For v1 sources this is the legacy ``active`` block from
      ``CURRENT_TASK.json``; for v2 sources it is the
      ``task_projection_schema_version=1`` per-task projection. The
      two are field-equivalent for the keys lifecycle readers consume
      (``task_ref``, ``status``, ``objective``, ``focus``,
      ``target_branch``, ``target_worktree_path``, ``task_plan_path``,
      ``revision``, ``updated_at``).
    - ``tasks``: populated only when ``shape == "workspace_ambiguous"``.
      Empty list otherwise (never ``None`` — callers iterate it).
    - ``source_schema_version``: ``1`` or ``2`` depending on which
      payload we read. ``None`` only when the file did not exist.
      Migrated readers should NOT branch on this — it is exposed for
      diagnostic / DASHBOARD reporting only.
    """

    shape: str
    task_ref: str | None
    active: dict[str, Any] | None
    tasks: list[dict[str, Any]]
    source_schema_version: int | None


_NONE_VIEW = WorkspaceSummaryView(
    shape="none",
    task_ref=None,
    active=None,
    tasks=[],
    source_schema_version=None,
)


def _coerce_v1(payload: dict[str, Any]) -> WorkspaceSummaryView:
    active = payload.get("active")
    if not isinstance(active, dict) or not active:
        return WorkspaceSummaryView(
            shape="none",
            task_ref=None,
            active=None,
            tasks=[],
            source_schema_version=1,
        )
    task_ref = payload.get("task_ref")
    if not isinstance(task_ref, str) or not task_ref:
        # Fall back to active.task_ref if the top-level field is absent
        # — older v1 writers occasionally left it null.
        candidate = active.get("task_ref")
        task_ref = candidate if isinstance(candidate, str) and candidate else None
    return WorkspaceSummaryView(
        shape="single",
        task_ref=task_ref,
        active=active,
        tasks=[],
        source_schema_version=1,
    )


def _coerce_v2(payload: dict[str, Any]) -> WorkspaceSummaryView:
    shape = payload.get("shape")
    if shape == "none":
        return WorkspaceSummaryView(
            shape="none",
            task_ref=None,
            active=None,
            tasks=[],
            source_schema_version=2,
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
            source_schema_version=2,
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
            source_schema_version=2,
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
    :func:`load_workspace_summary_compat`.

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
    proc = run_subprocess(
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
        return load_workspace_summary_compat(payload)
    except WorkspaceSummaryParseError:
        return _NONE_VIEW


def load_workspace_summary_compat(
    source: Path | dict[str, Any],
) -> WorkspaceSummaryView:
    """Load a workspace summary, accepting v1 or v2 ``CURRENT_TASK.json``.

    Inputs:

    - ``Path``: read JSON from disk. A missing path returns the
      ``shape="none"`` view (no active task is the same semantic as no
      file). Corrupt JSON raises :class:`WorkspaceSummaryParseError`.
    - ``dict``: an already-parsed payload (e.g. from a subprocess that
      returned JSON over stdout).

    Output: a :class:`WorkspaceSummaryView` whose ``shape`` field is
    the canonical vocabulary regardless of which schema_version the
    source declared. Migrated readers branch on ``view.shape``, never
    on ``view.source_schema_version``.
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
    if schema_version == 1:
        return _coerce_v1(payload)
    if schema_version == 2:
        return _coerce_v2(payload)
    raise WorkspaceSummaryParseError(
        f"Unsupported workspace-summary schema_version: {schema_version!r}"
    )
