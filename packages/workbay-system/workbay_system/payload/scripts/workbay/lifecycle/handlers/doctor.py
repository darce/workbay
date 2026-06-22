"""Read-only ``doctor`` subcommand.

Single-call cold-start aggregator. Composes the env / mcp / branch /
lifecycle / dashboard / hooks facets into a ``DoctorReceipt`` so a
fresh agent's first turn has one structured payload to read instead
of three sequential surfaces (``make context`` + ``DASHBOARD.txt`` +
raw MCP). The facets deliberately reuse existing infrastructure:

- env: in-process call to ``workbay_bootstrap.subcommands.doctor``
  (skipped with a warning when the bootstrap package is unavailable).
- mcp: bounded handoff probe via
  ``_common.run_handoff_json``; never calls the deeper
  ``run_doctor`` MCP tools (their FTS5/stdio handshake exceeds the
  per-attempt probe budget; see ``_probe_timeout_seconds``).
- branch / lifecycle / dashboard / hooks: direct git, importlib, and
  filesystem reads.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import resolver
from receipts import (
    DoctorBranch,
    DoctorDashboard,
    DoctorDirtyMain,
    DoctorEnv,
    DoctorHooks,
    DoctorLifecycle,
    DoctorMcp,
    DoctorPlanBaseline,
    DoctorReceipt,
    DoctorVenv,
    NextCommand,
    ReceiptWarning,
)

from . import _common
from .plan_baseline import evaluate_plan_baseline


REMEDIATION_CLEAN = [
    "no protected paths dirty on the active branch — no action needed",
]
REMEDIATION_DIRTY = [
    "review each dirty file with `git diff` before acting",
    "move task-owned changes to a feature branch via `make task-start TASK=<ref>`",
    "preserve carryover with `git stash push -m 'carryover-<date>' -- <files>`",
    "see docs/workbay/rules/development-workflow.md#root-main-is-a-control-plane",
]


# Bounded retry/backoff for MCP reachability probes. Every attempt spawns a
# fresh CLI process and pays the full Python import cost — there is no daemon
# to warm up — so the per-attempt budget must cover observed CLI cold start
# (~1.6s via a worktree venv bin, ~2.7s via a pyenv shim). The previous 0.5s
# budget timed out every attempt on real hosts, reporting a permanent false
# ``mcp=unreachable`` with ``warming`` unreachable by construction
# (internal). Operators can tune the budget via
# ``WORKBAY_DOCTOR_PROBE_TIMEOUT_SECONDS`` (invalid or non-positive values
# fall back to the default). With the larger per-attempt budget the retry
# ladder shrinks to one retry: worst case per probe is 2 attempts * 5s + 1s
# sleep ≈ 11s before declaring unreachable. `_probe_mcp` runs two sequential
# probes (handoff then orchestrator), so `make doctor` compounds to ~2x
# (~22s) only when BOTH endpoints genuinely miss the budget; the common
# attempt-0-success path on each probe stays at one CLI invocation
# (~1.6-2.7s), and the missing-binary path still short-circuits.
_PROBE_TIMEOUT_ENV = "WORKBAY_DOCTOR_PROBE_TIMEOUT_SECONDS"
_PROBE_TIMEOUT_DEFAULT_SECONDS = 5.0
_PROBE_BACKOFF_SLEEPS = (1.0,)


def _probe_timeout_seconds() -> float:
    """Per-attempt probe budget: env override when valid, else the default."""
    raw = os.environ.get(_PROBE_TIMEOUT_ENV)
    if raw:
        try:
            value = float(raw)
        except ValueError:
            return _PROBE_TIMEOUT_DEFAULT_SECONDS
        if value > 0:
            return value
    return _PROBE_TIMEOUT_DEFAULT_SECONDS


_DASHBOARD_FRESH_SECONDS = 24 * 60 * 60


def _probe_env(repo: Path) -> tuple[DoctorEnv, list[ReceiptWarning]]:
    warnings: list[ReceiptWarning] = []
    try:
        bootstrap = importlib.import_module("workbay_bootstrap.subcommands")
    except Exception as exc:
        warnings.append(
            ReceiptWarning(
                field="env",
                reason="workbay_bootstrap.subcommands unavailable",
                exception_type=type(exc).__name__,
            )
        )
        return DoctorEnv(findings=[], available=False), warnings

    doctor_fn = getattr(bootstrap, "doctor", None)
    if not callable(doctor_fn):
        warnings.append(
            ReceiptWarning(
                field="env",
                reason="workbay_bootstrap.subcommands.doctor missing",
            )
        )
        return DoctorEnv(findings=[], available=False), warnings

    try:
        raw = doctor_fn(target=repo, mcp_servers=None)
    except TypeError:
        try:
            raw = doctor_fn(target=repo)
        except Exception as exc:
            warnings.append(
                ReceiptWarning(
                    field="env",
                    reason="bootstrap doctor raised",
                    exception_type=type(exc).__name__,
                )
            )
            return DoctorEnv(findings=[], available=False), warnings
    except Exception as exc:
        warnings.append(
            ReceiptWarning(
                field="env",
                reason="bootstrap doctor raised",
                exception_type=type(exc).__name__,
            )
        )
        return DoctorEnv(findings=[], available=False), warnings

    findings: list[dict[str, object]] = []
    for entry in raw or []:
        kind = getattr(entry, "kind", None) or (
            entry.get("kind") if isinstance(entry, dict) else None
        )
        path = getattr(entry, "path", None) or (
            entry.get("path") if isinstance(entry, dict) else None
        )
        message = getattr(entry, "message", None) or (
            entry.get("message") if isinstance(entry, dict) else None
        )
        findings.append(
            {
                "kind": kind if isinstance(kind, str) else None,
                "path": str(path) if path is not None else None,
                "message": message if isinstance(message, str) else None,
            }
        )
    return DoctorEnv(findings=findings, available=True), warnings


def _probe_with_retry(
    repo: Path,
    *,
    binary: str,
    runner,
    argv: list[str],
    field: str,
    latency_key: str,
    latencies: dict[str, float],
) -> tuple[str, ReceiptWarning | None]:
    """Shared bounded-retry probe shape.

    Used by both the handoff and orchestrator probes so the tri-state
    surface stays in one place. Returns ``(status, last_warning)`` where
    ``status`` is one of ``"reachable"`` / ``"warming"`` / ``"unreachable"``.
    Records the most recent attempt's wall-clock under
    ``latencies[latency_key]`` (back-compat with the original single-value
    latency shape; intentionally no full attempt list).

    Missing-binary short-circuit: when ``shutil.which`` cannot resolve
    ``binary`` we record exactly one attempt and skip the retry budget —
    retrying a non-existent path just burns wall-clock.
    """
    if shutil.which(binary) is None:
        started = time.monotonic()
        _, warning = runner(
            repo,
            argv=argv,
            timeout_seconds=_probe_timeout_seconds(),
            field=field,
        )
        latencies[latency_key] = round((time.monotonic() - started) * 1000.0, 1)
        return "unreachable", warning

    last_warning: ReceiptWarning | None = None
    for attempt_index in range(len(_PROBE_BACKOFF_SLEEPS) + 1):
        started = time.monotonic()
        payload, warning = runner(
            repo,
            argv=argv,
            timeout_seconds=_probe_timeout_seconds(),
            field=field,
        )
        latencies[latency_key] = round((time.monotonic() - started) * 1000.0, 1)
        if payload is not None and isinstance(payload, dict):
            return ("reachable" if attempt_index == 0 else "warming"), None
        last_warning = warning
        if attempt_index < len(_PROBE_BACKOFF_SLEEPS):
            time.sleep(_PROBE_BACKOFF_SLEEPS[attempt_index])

    return "unreachable", last_warning


def _probe_mcp(repo: Path) -> tuple[DoctorMcp, list[ReceiptWarning]]:
    """Bounded-retry probes with tri-state result.

    Runs two independent bounded-retry probes:

    - the workbay-handoff endpoint via ``_common.run_handoff_json`` →
      ``mcp_status``;
    - the workbay-orchestrator endpoint via ``_common.run_orchestrator_json``
      → ``orchestrator_status``.

    Both endpoints get their own per-attempt latency under
    ``latencies_ms[...]`` and surface their own derived back-compat
    boolean. A cold-start that responds on retry is recorded as
    ``"warming"`` so ``_suggest_next`` can distinguish slow
    startup from genuine outage on either endpoint independently.
    """
    warnings: list[ReceiptWarning] = []
    latencies: dict[str, float] = {}

    mcp_status, handoff_warning = _probe_with_retry(
        repo,
        binary=_common.mcp_handoff_bin(),
        runner=_common.run_handoff_json,
        argv=["state", "--sections", "identity", "--detail", "summary"],
        field="mcp.handoff",
        latency_key="handoff",
        latencies=latencies,
    )
    if handoff_warning is not None:
        warnings.append(handoff_warning)

    orchestrator_status, orchestrator_warning = _probe_with_retry(
        repo,
        binary=_common.mcp_orchestrator_bin(),
        runner=_common.run_orchestrator_json,
        argv=["orchestrator-status"],
        field="mcp.orchestrator",
        latency_key="orchestrator",
        latencies=latencies,
    )
    if orchestrator_warning is not None:
        warnings.append(orchestrator_warning)

    return (
        DoctorMcp(
            handoff_reachable=mcp_status in ("reachable", "warming"),
            orchestrator_reachable=orchestrator_status in ("reachable", "warming"),
            latencies_ms=latencies,
            mcp_status=mcp_status,
            orchestrator_status=orchestrator_status,
        ),
        warnings,
    )


def _probe_branch(repo: Path) -> DoctorBranch:
    facts = _common.gather_git_facts(repo)
    return DoctorBranch(
        name=facts.branch,
        head=facts.head,
        ahead_of_main=None,
        dirty=facts.dirty_summary.get("total", 0),
        protected_paths_dirty=[],
    )


def _probe_lifecycle() -> DoctorLifecycle:
    handlers_pkg = "handlers"
    status_ok = False
    tasks_ok = False
    try:
        importlib.import_module(f"{handlers_pkg}.status")
        status_ok = True
    except Exception:
        pass
    try:
        importlib.import_module(f"{handlers_pkg}.tasks")
        tasks_ok = True
    except Exception:
        pass
    return DoctorLifecycle(
        status_handler_ok=status_ok,
        tasks_handler_ok=tasks_ok,
        expected_stubs=[],
    )


def _probe_dashboard(repo: Path) -> DoctorDashboard:
    dashboard = repo / "DASHBOARD.txt"
    fragments_dir = repo / "DASHBOARD.d"
    last_regen_at: str | None = None
    fresh = False
    exists = dashboard.is_file()
    if exists:
        try:
            mtime = dashboard.stat().st_mtime
            last_regen_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(mtime))
            fresh = (time.time() - mtime) < _DASHBOARD_FRESH_SECONDS
        except OSError:
            pass
    return DoctorDashboard(
        exists=exists,
        fresh=fresh,
        fragments_present=fragments_dir.is_dir(),
        last_regen_at=last_regen_at,
    )


# doctor.py lives at
# ``<workbay-system>/scripts/workbay/lifecycle/handlers/doctor.py``. The
# compact-session manifest and the canonical hoisted git-hook scripts are
# package properties (they ship with this workbay-system version), so they are
# resolved relative to the module rather than the inspected consumer checkout.
_WORKBAY_SYSTEM_ROOT = Path(__file__).resolve().parents[4]
_HOOK_MANIFEST_PATH = (
    _WORKBAY_SYSTEM_ROOT / "config" / "agent-workflows" / "portable_commands.json"
)
_CANONICAL_GIT_HOOKS_DIR = _WORKBAY_SYSTEM_ROOT / "scripts" / "hooks" / "git"
_MANAGED_BY = "workbay-bootstrap"


def _load_hook_adapter_decls() -> list[tuple[str, dict[str, Any]]]:
    """Return ``(hook_id, adapter)`` declarations for every manifest hook.

    internal generalized the compact-session-only loader so the
    doctor facet reports every ``hooks[]`` family (compact-session Stop,
    reinject-context SessionStart, and any future family) without further
    code changes. Missing or malformed manifests degrade to an empty list so
    the doctor receipt stays renderable on a checkout without the manifest.
    """
    try:
        manifest = json.loads(_HOOK_MANIFEST_PATH.read_text())
    except (OSError, ValueError):
        return []
    decls: list[tuple[str, dict[str, Any]]] = []
    for hook in manifest.get("hooks", []) or []:
        hook_id = str(hook.get("hook_id") or "")
        for adapter in hook.get("adapters", []) or []:
            if isinstance(adapter, dict):
                decls.append((hook_id, adapter))
    return decls


def _claude_stop_entry_shape_ok(entry: dict[str, Any]) -> bool:
    """True when a managed entry uses Claude's nested ``hooks[]`` shape.

    The pre-fix flat ``{"_managed_by", "command"}`` shape is silently
    ignored by Claude Code, so it must read as *stale*, not installed.
    """
    if "command" in entry:
        return False
    nested = entry.get("hooks")
    if not isinstance(nested, list) or not nested:
        return False
    return all(
        isinstance(h, dict) and h.get("type") == "command" and h.get("command")
        for h in nested
    )


def _managed_adapter_state(
    repo: Path, target: str, harness: str, json_path: str
) -> str:
    """Classify the managed entry at ``repo/target`` under ``json_path``.

    ``json_path`` is the adapter patch's container (``$.hooks.Stop``,
    ``$.hooks.SessionStart``, ...) so every manifest hook family reuses one
    classifier. Returns ``"installed"``, ``"stale"`` (managed entry present
    but in a shape the harness ignores — e.g. the pre-fix flat Claude
    entry), or ``"absent"``.
    """
    path = repo / target
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return "absent"
    node: Any = data if isinstance(data, dict) else {}
    if not json_path.startswith("$."):
        return "absent"
    for seg in json_path[2:].split("."):
        node = node.get(seg) if isinstance(node, dict) else None
    entries = node if isinstance(node, list) else []
    managed = [
        entry
        for entry in entries
        if isinstance(entry, dict) and entry.get("_managed_by") == _MANAGED_BY
    ]
    if not managed:
        return "absent"
    if harness == "claude-code" and not all(
        _claude_stop_entry_shape_ok(entry) for entry in managed
    ):
        return "stale"
    return "installed"


def _expected_hoisted_git_hooks() -> list[str]:
    """Names of the canonical hoisted git-hook scripts shipped with the package."""
    try:
        return sorted(p.name for p in _CANONICAL_GIT_HOOKS_DIR.iterdir() if p.is_file())
    except OSError:
        return []


def _resolve_hooks_path(repo: Path) -> str | None:
    """Return the repo-local ``core.hooksPath`` value, or ``None`` when unset."""
    proc = resolver._run_git(repo, "config", "--get", "core.hooksPath")
    if proc is None or proc.returncode != 0:
        return None
    value = proc.stdout.strip()
    return value or None


def _hook_adapter_drift_paths(env_findings: list[dict[str, object]] | None) -> set[str]:
    if not env_findings:
        return set()
    return {
        str(path)
        for finding in env_findings
        if finding.get("kind") == "hook_adapter_drift"
        and isinstance(path := finding.get("path"), str)
    }


def _probe_hooks(
    repo: Path,
    *,
    env_findings: list[dict[str, object]] | None = None,
) -> DoctorHooks:
    """Report manifest hook-adapter and git-hook hoist state.

    Adapter *availability* comes from the package manifest — every
    ``hooks[]`` family since internal, reported per family in the
    additive ``hook_adapters`` field while the legacy ``stop_adapters_*``
    keys keep their compact-session-only meaning (internal contract).
    Bootstrap-reported managed drift takes precedence, then *installed*
    state is checked against the inspected ``repo``. Git-hook hoist
    readiness is read from the inspected checkout's ``core.hooksPath`` so a
    linked worktree can be diagnosed independently of the primary tree.
    Optional-not-installed adapters and an unset ``core.hooksPath`` are
    reported but never fail the doctor receipt.
    """
    remediation: list[str] = []
    drifted_paths = _hook_adapter_drift_paths(env_findings)

    hook_adapters: dict[str, dict[str, list[str]]] = {}
    for hook_id, adapter in _load_hook_adapter_decls():
        target = adapter.get("target")
        if not target:
            continue
        target_path = str(target)
        key = f"{adapter.get('harness', 'unknown')}:{target}"
        family = hook_adapters.setdefault(
            hook_id,
            {
                "available": [],
                "installed": [],
                "drifted": [],
                "optional_not_installed": [],
            },
        )
        family["available"].append(key)
        # compact-session predates the generalization; its remediation text
        # keeps the internal "stop adapter" wording other tooling greps for.
        noun = (
            "stop adapter"
            if hook_id == "compact-session"
            else f"{hook_id} hook adapter"
        )
        json_path = str((adapter.get("patch") or {}).get("json_path", "$.hooks.Stop"))
        state = _managed_adapter_state(
            repo, target_path, str(adapter.get("harness", "")), json_path
        )
        if target_path in drifted_paths:
            family["drifted"].append(key)
            remediation.append(
                f"repair the {key} {noun} via `workbay-bootstrap repair`"
            )
        elif state == "installed":
            family["installed"].append(key)
        elif state == "stale":
            family["drifted"].append(key)
            remediation.append(
                f"re-install the {key} {noun} (stale pre-fix flat shape "
                f"Claude Code ignores) via `workbay-bootstrap repair` or "
                f"`workbay-bootstrap install {adapter.get('opt_in_flag', '')}`"
            )
        else:
            family["optional_not_installed"].append(key)
            opt_in_flag = adapter.get("opt_in_flag")
            if opt_in_flag:
                remediation.append(
                    f"install the {key} {noun} via "
                    f"`workbay-bootstrap install {opt_in_flag}`"
                )

    compact_family = hook_adapters.get(
        "compact-session",
        {"available": [], "installed": [], "drifted": [], "optional_not_installed": []},
    )
    available = compact_family["available"]
    installed = compact_family["installed"]
    drifted = compact_family["drifted"]
    optional_not_installed = compact_family["optional_not_installed"]

    hooks_path = _resolve_hooks_path(repo)
    expected: list[str] = []
    actual: list[str] = []
    drift: list[str] = []
    git_hooks_hoisted = False
    if hooks_path is not None:
        expected = _expected_hoisted_git_hooks()
        hooks_dir = repo / hooks_path
        for name in expected:
            if (hooks_dir / name).is_file():
                actual.append(name)
            else:
                drift.append(f"missing hoisted git hook: {hooks_path}/{name}")
        git_hooks_hoisted = bool(expected) and not drift
        if drift:
            remediation.append(
                "re-run `workbay-bootstrap install` to hoist the missing git-hook scripts"
            )

    return DoctorHooks(
        expected=expected,
        actual=actual,
        drift=drift,
        stop_adapters_available=available,
        stop_adapters_installed=installed,
        stop_adapters_drifted=drifted,
        stop_adapters_optional_not_installed=optional_not_installed,
        git_hooks_path=hooks_path,
        git_hooks_hoisted=git_hooks_hoisted,
        remediation=remediation,
        hook_adapters=hook_adapters,
    )


def _probe_dirty_main(
    repo: Path, branch_name: str
) -> tuple[DoctorDirtyMain, list[ReceiptWarning]]:
    """Ownership-aware dirty-main facet for the doctor receipt.

    Reuses the same ``find_dirty_protected_paths`` helper that
    ``check_main_clean.py`` runs in the git hooks so the doctor and the
    publish-boundary hook agree on what counts as dirty. Off-protected
    branches the facet is still rendered with an empty path list and a
    ``warn`` recommendation so callers see a stable shape.
    """
    warnings: list[ReceiptWarning] = []
    if branch_name not in ("main", "master"):
        return (
            DoctorDirtyMain(
                branch=branch_name,
                protected_paths_dirty=[],
                mode_recommended="warn",
                remediation=[
                    "current branch is not main/master — no dirty-main check applies",
                ],
                ownership_hint=None,
            ),
            warnings,
        )

    hooks_dir = _common.find_hooks_dir(repo)
    if hooks_dir is None:
        # Fixtures or stripped checkouts may lack the hooks directory;
        # fall through to a clean shape rather than failing the receipt.
        return (
            DoctorDirtyMain(
                branch=branch_name,
                protected_paths_dirty=[],
                mode_recommended="warn",
                remediation=REMEDIATION_CLEAN,
                ownership_hint=None,
            ),
            warnings,
        )

    sys.path.insert(0, str(hooks_dir))
    try:
        from _branch_isolation_guard import find_dirty_protected_paths  # noqa: PLC0415
        from _harness_protocol import (  # noqa: PLC0415
            HarnessContractMissingError,
            load_branch_isolation_policy,
        )
    except ImportError as exc:
        warnings.append(
            ReceiptWarning(
                field="dirty_main",
                reason="hook helpers unavailable",
                exception_type=type(exc).__name__,
            )
        )
        return (
            DoctorDirtyMain(
                branch=branch_name,
                protected_paths_dirty=[],
                mode_recommended="warn",
                remediation=REMEDIATION_CLEAN,
                ownership_hint=None,
            ),
            warnings,
        )

    try:
        policy = load_branch_isolation_policy(repo)
    except HarnessContractMissingError as exc:
        warnings.append(
            ReceiptWarning(
                field="dirty_main",
                reason="harness contract missing",
                exception_type=type(exc).__name__,
            )
        )
        return (
            DoctorDirtyMain(
                branch=branch_name,
                protected_paths_dirty=[],
                mode_recommended="warn",
                remediation=REMEDIATION_CLEAN,
                ownership_hint=None,
            ),
            warnings,
        )
    except Exception as exc:
        warnings.append(
            ReceiptWarning(
                field="dirty_main",
                reason="policy load failed",
                exception_type=type(exc).__name__,
            )
        )
        return (
            DoctorDirtyMain(
                branch=branch_name,
                protected_paths_dirty=[],
                mode_recommended="warn",
                remediation=REMEDIATION_CLEAN,
                ownership_hint=None,
            ),
            warnings,
        )

    result = find_dirty_protected_paths(
        branch=branch_name,
        repo_root=str(repo),
        policy=policy,
        protected_branches={"main", "master"},
    )
    if result is None:
        return (
            DoctorDirtyMain(
                branch=branch_name,
                protected_paths_dirty=[],
                mode_recommended="warn",
                remediation=REMEDIATION_CLEAN,
                ownership_hint=None,
            ),
            warnings,
        )

    _resolved_branch, dirty_paths = result
    return (
        DoctorDirtyMain(
            branch=branch_name,
            protected_paths_dirty=list(dirty_paths),
            mode_recommended="doctor",
            remediation=REMEDIATION_DIRTY,
            ownership_hint=None,
        ),
        warnings,
    )


_LIVE_HANDOFF_STATUSES = ("in_progress", "review", "blocked")


def _query_live_handoff_rows(repo: Path) -> tuple[list[dict[str, Any]], bool]:
    """Return ``(rows, ok)`` for live handoff rows via the MCP CLI.

    This drives the cross-task plan-baseline drift probe. ``ok=False`` signals
    an MCP outage so the doctor surfaces
    ``available=False`` instead of an empty-clean state.
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root",
        str(workspace),
        "handoff-rows",
        "--status",
        *_LIVE_HANDOFF_STATUSES,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return [], False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return [], False
    if not isinstance(payload, list):
        return [], False
    return [row for row in payload if isinstance(row, dict)], True


def _probe_plan_baseline(
    repo: Path,
) -> tuple[DoctorPlanBaseline, list[ReceiptWarning]]:
    """Aggregate plan-baseline state across every live handoff row.

    Walks ``handoff-rows --status in_progress|review|blocked`` and runs
    :func:`evaluate_plan_baseline` per row. When MCP
    is unreachable the facet collapses to ``available=False`` so the
    caller renders ``baseline=unknown`` rather than silently passing.
    """
    warnings: list[ReceiptWarning] = []
    rows, ok = _query_live_handoff_rows(repo)
    if not ok:
        warnings.append(
            ReceiptWarning(
                field="plan_baseline",
                reason="handoff-rows query failed",
            )
        )
        return DoctorPlanBaseline(available=False, counts={}, baselines=[]), warnings

    counts: dict[str, int] = {"accepted": 0, "missing": 0, "unknown": 0}
    baselines: list[dict[str, object]] = []
    for row in rows:
        task_ref = str(row.get("task_ref") or "")
        if not task_ref:
            continue
        plan_path = row.get("task_plan_path")
        target_branch = row.get("target_branch")
        baseline = evaluate_plan_baseline(
            repo,
            task_ref=task_ref,
            task_plan_path=str(plan_path) if isinstance(plan_path, str) else None,
            target_branch=str(target_branch)
            if isinstance(target_branch, str)
            else None,
        )
        status = baseline.baseline_status
        counts[status] = counts.get(status, 0) + 1
        baselines.append(
            {
                "task_ref": task_ref,
                "status": status,
                "reason": baseline.reason,
                "task_plan_path": baseline.task_plan_path,
                "target_branch": (
                    str(target_branch) if isinstance(target_branch, str) else None
                ),
                "acceptance_ready": baseline.acceptance_ready,
                "next_command": baseline.next_command,
            }
        )
    return DoctorPlanBaseline(
        available=True, counts=counts, baselines=baselines
    ), warnings


def _suggest_next(branch: DoctorBranch, mcp: DoctorMcp) -> NextCommand:
    """Branch on the tri-state so cold-start (`warming`)
    and outage (`unreachable`) produce distinct operator remediations.

    Priority order:

    1. Handoff outage — lifecycle gates need it, so this dominates.
    2. Handoff cold-start — the probe succeeded on retry, so the right
       advice is "wait a moment, then re-run `make doctor`" rather than
       restarting a healthy server.
    3. Orchestrator outage — daemon lifecycle is degraded; flag it before
       the operator wastes a cycle.
    4. Orchestrator cold-start — informational; tell the operator it is
       warming so they can re-run if they need a green orchestrator gate.
    5. Branch/dirty cues.
    """
    if mcp.mcp_status == "unreachable":
        return NextCommand(
            command="check MCP_WORKBAY_HANDOFF_BIN; restart workbay-handoff-mcp",
            reason="handoff MCP unreachable; lifecycle gates need it",
        )
    if mcp.mcp_status == "warming":
        return NextCommand(
            command="wait a few seconds and re-run `make doctor`",
            reason="handoff MCP responded on retry (warming cold-start) — "
            "give it a moment before declaring outage",
        )
    if mcp.orchestrator_status == "unreachable":
        return NextCommand(
            command="check MCP_WORKBAY_ORCHESTRATOR_BIN; restart workbay-orchestrator-mcp",
            reason="orchestrator MCP unreachable; lane/worker controls need it",
        )
    if mcp.orchestrator_status == "warming":
        return NextCommand(
            command="wait a few seconds and re-run `make doctor`",
            reason="orchestrator MCP responded on retry (warming cold-start) — "
            "give it a moment before declaring outage",
        )
    if branch.name in ("", "main", "master"):
        return NextCommand(
            command="make tasks  # then `make task-start TASK=<ref>`",
            reason="no active feature branch — pick or start a task",
        )
    if branch.dirty > 0:
        return NextCommand(
            command="make slice-start  # working tree dirty",
            reason="uncommitted edits present on a feature branch",
        )
    return NextCommand(
        command="make context",
        reason="ready for the deeper status load",
    )


def _probe_venv(repo: Path) -> DoctorVenv:
    """Report root ``.venv`` / ambient-pytest resolution.

    Pure filesystem + ``PATH`` read. ``root_venv_pytest_present`` is the
    contract ``task-start`` provisioning is supposed to satisfy;
    ``ambient_pytest_outside_worktree`` is the pyenv-shim trap signal — a bare
    ``pytest`` resolving outside this worktree means the wrong environment
    would load. ``None`` for that field means no ambient ``pytest`` was found,
    which is unknown rather than a confirmed risk.
    """
    venv_dir = repo / ".venv"
    venv_pytest = venv_dir / "bin" / "pytest"
    root_venv_present = venv_dir.is_dir()
    root_venv_pytest_present = venv_pytest.is_file()

    ambient = shutil.which("pytest")
    outside: bool | None
    if ambient is None:
        outside = None
    else:
        try:
            outside = not Path(ambient).resolve().is_relative_to(repo.resolve())
        except (OSError, ValueError):
            outside = None

    remediation: list[str] = []
    if not root_venv_pytest_present:
        remediation.append(
            "provision the worktree-root venv: `make provision-env` "
            "(or `make slice-start`, which prepends .venv/bin)"
        )
    if outside:
        remediation.append(
            "ambient `pytest` resolves outside this worktree (pyenv-shim risk); "
            "`source .venv/bin/activate` or use a lifecycle command that "
            "prepends .venv/bin before running bare pytest"
        )

    return DoctorVenv(
        root_venv_present=root_venv_present,
        root_venv_pytest_present=root_venv_pytest_present,
        ambient_pytest_path=ambient,
        ambient_pytest_outside_worktree=outside,
        remediation=remediation,
    )


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle doctor", add_help=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = resolver.repo_root()
    if repo is None:
        receipt: dict[str, Any] = {
            "ok": False,
            "command": "doctor",
            "error": "not_in_git_repo",
        }
        _common.emit(receipt)
        return 2

    warnings: list[ReceiptWarning] = []
    env, env_warnings = _probe_env(repo)
    warnings.extend(env_warnings)
    mcp, mcp_warnings = _probe_mcp(repo)
    warnings.extend(mcp_warnings)
    branch = _probe_branch(repo)
    lifecycle = _probe_lifecycle()
    dashboard = _probe_dashboard(repo)
    hooks = _probe_hooks(repo, env_findings=env.findings)
    dirty_main, dirty_main_warnings = _probe_dirty_main(repo, branch.name)
    warnings.extend(dirty_main_warnings)
    plan_baseline, plan_baseline_warnings = _probe_plan_baseline(repo)
    warnings.extend(plan_baseline_warnings)
    venv = _probe_venv(repo)
    next_command = _suggest_next(branch, mcp)

    receipt_obj = DoctorReceipt(
        ok=True,
        command="doctor",
        env=env,
        mcp=mcp,
        branch=branch,
        lifecycle=lifecycle,
        dashboard=dashboard,
        hooks=hooks,
        next_command=next_command,
        warnings=warnings,
        dirty_main=dirty_main,
        plan_baseline=plan_baseline,
        venv=venv,
    )

    if not args.emit_json:
        if plan_baseline.available:
            counts = plan_baseline.counts
            baseline_line = (
                f"plan_baseline: accepted={counts.get('accepted', 0)} "
                f"missing={counts.get('missing', 0)} "
                f"unknown={counts.get('unknown', 0)}"
            )
        else:
            baseline_line = "plan_baseline: unavailable (MCP unreachable)"
        if venv.ambient_pytest_outside_worktree:
            ambient_state = "outside-worktree"
        elif venv.ambient_pytest_path is None:
            ambient_state = "none"
        else:
            ambient_state = "inside-worktree"
        venv_line = (
            f"venv: root_pytest={venv.root_venv_pytest_present} "
            f"ambient_pytest={ambient_state}"
        )
        # Surface both tri-states verbatim so operators
        # can read cold-start vs outage at a glance. `mcp.handoff` (the
        # back-compat derived boolean) stays on the line so existing
        # scrapers do not break.
        sys.stderr.write(
            f"doctor: branch={branch.name or '-'} head={(branch.head or '')[:12]} "
            f"dirty={branch.dirty} mcp={mcp.mcp_status} "
            f"orchestrator={mcp.orchestrator_status} "
            f"mcp.handoff={mcp.handoff_reachable} "
            f"dashboard.exists={dashboard.exists}\n"
            f"{baseline_line}\n"
            f"{venv_line}\n"
            f"next: {next_command.command}\n"
        )

    _common.emit(receipt_obj.to_dict())
    return 0
