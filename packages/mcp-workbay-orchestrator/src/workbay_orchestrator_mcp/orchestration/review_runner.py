#!/usr/bin/env python3
"""Self-review runner: build a review prompt, execute Codex, validate findings, optionally record to MCP."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, TypedDict

PACKAGE_SRC = Path(__file__).resolve().parents[2]
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from workbay_orchestrator_mcp.orchestration._env import WORKER_REASONING_EFFORT_CHOICES, apply_backend_runtime_hints
from workbay_orchestrator_mcp.orchestration.backend_registry import (
    backend_supports_token_budget_cycle_bounds,
    get_adapter,
    get_backend_choices,
    validate_backend,
)
from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config

if TYPE_CHECKING:
    from workbay_handoff_mcp.core import ReviewFindingDetails, WriteActor
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource
    from workbay_handoff_mcp.review_findings import BatchFindingItem

REPO_ROOT = PACKAGE_SRC.parents[2]
from workbay_orchestrator_mcp._assets import bundled_rules_dir  # noqa: E402

DEFAULT_RULES_DIR = bundled_rules_dir()
RULES_DIR = DEFAULT_RULES_DIR

BACKEND_CHOICES = get_backend_choices()

# Stack guide selection by file extension
STACK_GUIDES: dict[str, str] = {
    ".py": "branch-review-python.md",
    ".ts": "branch-review-typescript.md",
    ".tsx": "branch-review-typescript.md",
    ".js": "branch-review-typescript.md",
    ".jsx": "branch-review-typescript.md",
    ".php": "branch-review-php.md",
}

REVIEW_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["findings", "summary"],
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "severity",
                    "category",
                    "file_path",
                    "line_start",
                    "line_end",
                    "description",
                    "fix",
                ],
                "properties": {
                    "severity": {"type": "string", "enum": ["high", "medium", "low"]},
                    "category": {
                        "type": "string",
                        "enum": ["ANTIPATTERN", "DEAD_CODE", "COMPLEXITY", "GAP"],
                    },
                    "file_path": {"type": "string"},
                    "line_start": {"type": ["integer", "null"]},
                    "line_end": {"type": ["integer", "null"]},
                    "description": {"type": "string", "minLength": 1},
                    "fix": {"type": ["string", "null"]},
                },
            },
        },
        "summary": {"type": "string", "minLength": 1},
    },
}

# In-lane smoke review is non-authoritative for grok-cli; unparseable output is an
# expected backend-capability miss, not an engine error ([OBS-08] typed degradation).
REVIEW_SKIPPED_UNPARSEABLE = "skipped_unparseable"
_RAW_TAIL_CHARS = 800


class ReviewAdmissionDeferred(RuntimeError):
    """The review turn was deferred by remote VM admission (retryable).

    Raised when the review backend's result carries the ``admission_deferred``
    marker (remote_agent.sh exit 75): no review ran, so the payload must not
    tolerant-degrade to ``skipped_unparseable`` → converged. Callers translate
    this into the retryable ``admission_deferred`` outcome
    (``worker_daemon._review_phase`` → ``StopIteration("admission_deferred")``)
    instead of a terminal review error.
    """


def findings_converged(findings: list[dict[str, Any]]) -> bool:
    """Return True when findings meet convergence criteria: 0 HIGH, 0 MEDIUM, at most 1 LOW."""
    high = sum(1 for f in findings if f.get("severity") == "high")
    medium = sum(1 for f in findings if f.get("severity") == "medium")
    low = sum(1 for f in findings if f.get("severity") == "low")
    return high == 0 and medium == 0 and low <= 1


# ---------------------------------------------------------------------------
# Changed-file discovery
# ---------------------------------------------------------------------------


def _changed_files(worktree_path: Path) -> list[str]:
    """Return workspace-relative paths of changed files in the lane worktree."""
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
        check=False,
    )
    staged = subprocess.run(
        ["git", "diff", "--name-only", "--cached"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
        check=False,
    )
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
        check=False,
    )
    files: set[str] = set()
    for proc in (result, staged, untracked):
        if proc.returncode == 0:
            files.update(line.strip() for line in proc.stdout.splitlines() if line.strip())
    return sorted(files)


def _assert_recordable_review_scope(
    *,
    record_findings: bool,
    changed_files: list[str],
    scope_source: ReviewScopeSource,
) -> None:
    """Reject recorded branch-diff reviews that still point at dirty local changes."""
    if not record_findings:
        return
    if scope_source != "branch_diff":
        return
    if not changed_files:
        return
    raise RuntimeError(
        "review_runner.py refuses to record findings for branch_diff scope when the worktree has uncommitted "
        "changes. Commit or stash the dirty paths first, or rerun with --latest-slice so MCP findings map to "
        "a committed slice packet instead of the working tree."
    )


def _diff_stat(worktree_path: Path) -> str:
    """Return a compact diff stat for the lane worktree (unstaged + staged)."""
    parts: list[str] = []
    for cmd in (
        ["git", "diff", "--stat", "HEAD"],
        ["git", "diff", "--stat", "--cached"],
    ):
        result = subprocess.run(
            cmd,
            cwd=worktree_path,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            parts.append(result.stdout.strip())
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Stack guide auto-detection
# ---------------------------------------------------------------------------


def _detect_stack_guides(changed_files: list[str]) -> list[str]:
    """Return deduplicated list of stack guide filenames relevant to the changed files."""
    guides: dict[str, str] = {}
    for file_path in changed_files:
        ext = Path(file_path).suffix.lower()
        guide = STACK_GUIDES.get(ext)
        if guide and guide not in guides:
            guides[guide] = guide
    return list(guides.values())


def _resolve_rules_dir(*, orchestrator_root: str | Path | None = None, rules_dir: str | Path | None = None) -> Path:
    """Resolve the review-rules directory.

    Order: explicit ``rules_dir`` arg > module-level ``RULES_DIR`` override
    (test/CLI hook) > package-bundled defaults. The legacy
    ``<orchestrator_root>/docs/workbay/rules`` lookup is no longer
    consulted: review guides ship with the package itself.
    """
    if rules_dir is not None:
        return Path(rules_dir).expanduser().resolve()
    if RULES_DIR != DEFAULT_RULES_DIR:
        return RULES_DIR
    return RULES_DIR


def _read_guide(
    filename: str, *, orchestrator_root: str | Path | None = None, rules_dir: str | Path | None = None
) -> str:
    """Read a review guide file from the rules directory."""
    guide_path = _resolve_rules_dir(orchestrator_root=orchestrator_root, rules_dir=rules_dir) / filename
    if not guide_path.is_file():
        return ""
    return guide_path.read_text()


# ---------------------------------------------------------------------------
# Prompt rendering
# ---------------------------------------------------------------------------


class ReviewScope(TypedDict):
    changed_files: list[str]
    review_kind: ReviewKind
    scope_source: ReviewScopeSource
    scope_reason: str | None


def _guide_heading(filename: str) -> str:
    return Path(filename).stem.replace("-", " ").replace("_", " ").upper()


def _build_review_prompt(
    *,
    changed_files: list[str],
    diff_stat: str,
    stack_guides: list[str],
    main_guide_filename: str = "branch-review-guide.md",
    lane_id: str | None = None,
    orchestrator_root: str | Path | None = None,
    rules_dir: str | Path | None = None,
) -> str:
    """Assemble the full review prompt from guide content, stack guides, and diff context."""
    sections: list[str] = []

    sections.append("You are a code reviewer. Review the following lane changes using the checklist below.")
    sections.append(
        "Return ONLY a JSON object matching the required output schema. Do not include markdown fences or commentary."
    )

    if lane_id:
        sections.append(f"\nLane: {lane_id}")

    # Main review guide
    main_guide = _read_guide(main_guide_filename, orchestrator_root=orchestrator_root, rules_dir=rules_dir)
    if main_guide:
        sections.append(f"\n--- {_guide_heading(main_guide_filename)} ---\n")
        sections.append(main_guide)

    # Stack-specific guides
    for guide_name in stack_guides:
        guide_content = _read_guide(guide_name, orchestrator_root=orchestrator_root, rules_dir=rules_dir)
        if guide_content:
            sections.append(f"\n--- {_guide_heading(guide_name)} ---\n")
            sections.append(guide_content)

    # Changed files
    sections.append("\n--- CHANGED FILES ---\n")
    if changed_files:
        for f in changed_files:
            sections.append(f"- {f}")
    else:
        sections.append("(no changed files detected)")

    # Diff stat
    if diff_stat:
        sections.append("\n--- DIFF STAT ---\n")
        sections.append(diff_stat)

    sections.append("\n--- INSTRUCTIONS ---\n")
    sections.append(
        "Walk through each checklist item for the changed files above. "
        "For each issue found, add a finding to the findings array with severity, category, "
        "file_path, description, and optionally line_start, line_end, and fix. "
        "If no issues are found, return an empty findings array with a summary noting the review was clean."
    )

    return "\n".join(sections)


def _resolve_review_scope(
    *,
    worktree_path: Path,
    task_ref: str | None,
    orchestrator_root: Path | None,
    review_kind: ReviewKind | str | None,
    use_latest_slice: bool,
) -> ReviewScope:
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource  # noqa: PLC0415

    preferred_review_kind = ReviewKind(review_kind) if review_kind is not None else ReviewKind.BRANCH
    if use_latest_slice and task_ref and orchestrator_root is not None:
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime  # noqa: PLC0415

        from workbay_orchestrator_mcp.lanes import get_latest_slice_review_packet  # noqa: PLC0415

        runtime = RuntimeConfig.for_repo(orchestrator_root)
        configure_runtime(runtime)
        payload = _load_mcp_payload(
            get_latest_slice_review_packet(
                task_ref=task_ref,
                review_kind=preferred_review_kind.value,
            )
        )
        if payload.get("ok"):
            packet = payload["packet"]
            return {
                "changed_files": list(packet.get("changed_files") or []),
                "review_kind": ReviewKind(str(packet.get("review_kind") or ReviewKind.BRANCH.value)),
                "scope_source": ReviewScopeSource(
                    str(packet.get("scope_source") or ReviewScopeSource.SLICE_PACKET.value)
                ),
                "scope_reason": None,
            }
        return {
            "changed_files": _changed_files(worktree_path),
            "review_kind": preferred_review_kind,
            "scope_source": ReviewScopeSource.BRANCH_DIFF,
            "scope_reason": str(payload.get("error") or "latest slice packet lookup failed"),
        }

    return {
        "changed_files": _changed_files(worktree_path),
        "review_kind": preferred_review_kind,
        "scope_source": ReviewScopeSource.BRANCH_DIFF,
        "scope_reason": None,
    }


# ---------------------------------------------------------------------------
# Finding ID generation
# ---------------------------------------------------------------------------


def _generate_finding_id(lane_id: str | None, index: int, finding: dict[str, Any]) -> str:
    """Generate a stable, human-readable finding ID from lane + file + index."""
    prefix = (lane_id or "review").upper().replace("-", "")[:6]
    severity_char = {"high": "H", "medium": "M", "low": "L"}.get(finding.get("severity", ""), "X")
    return f"{prefix}-{severity_char}-{index + 1:02d}"


# ---------------------------------------------------------------------------
# Codex execution
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Result validation
# ---------------------------------------------------------------------------


def _load_mcp_payload(payload: dict[str, Any] | str | bytes | bytearray) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    loaded = json.loads(payload)
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Expected MCP payload object, got {type(loaded).__name__}.")
    return loaded


def _raw_tail(result: Any, *, max_chars: int = _RAW_TAIL_CHARS) -> str:
    """Bounded tail of a review payload for typed degrade diagnostics ([OBS-04])."""
    if isinstance(result, dict):
        try:
            text = json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            text = str(result)
    else:
        text = str(result)
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _unparseable_review_result(result: Any, *, reason: str) -> dict[str, Any]:
    """Typed degrade for unparseable smoke-review output ([OBS-08], [OBS-04])."""
    return {
        "review": REVIEW_SKIPPED_UNPARSEABLE,
        "findings": [],
        "summary": f"Smoke review unparseable ({reason}); non-authoritative degrade.",
        "raw_tail": _raw_tail(result),
    }


def _finding_field_error(finding: Any, index: int) -> str | None:
    """Return a per-finding validation error, or None when the item is well-formed."""
    valid_severities = {"high", "medium", "low"}
    valid_categories = {"ANTIPATTERN", "DEAD_CODE", "COMPLEXITY", "GAP"}
    if not isinstance(finding, dict):
        return f"Finding [{index}] is not an object."
    for required in ("severity", "category", "file_path", "description"):
        if required not in finding:
            return f"Finding [{index}] missing required field '{required}'."
    if finding["severity"] not in valid_severities:
        return f"Finding [{index}] has invalid severity '{finding['severity']}'. Valid: {sorted(valid_severities)}"
    if finding["category"] not in valid_categories:
        return f"Finding [{index}] has invalid category '{finding['category']}'. Valid: {sorted(valid_categories)}"
    if not isinstance(finding["description"], str) or not finding["description"].strip():
        return f"Finding [{index}] has empty description."
    return None


def _validate_review_result(result: Any, *, tolerant: bool = False) -> dict[str, Any]:
    """Validate the review result against the expected schema shape.

    When ``tolerant`` is True (grok-family smoke review):
    - Only a **non-empty** object that fails review-schema parsing degrades to
      typed ``skipped_unparseable`` ([OBS-08]). Empty/missing ``raw_payload`` is a
      transport failure (VM unreachable, exit-78/3, etc.) and raises exactly like
      the strict path so blockers/handoff_action are not masked as converged
      (0144 R3 / HIGH-2).
    - When the findings key is present as a list, salvage well-formed items and
      drop invalid ones with a note in ``raw_tail`` ([OBS-04]).
    Codex / non-tolerant paths stay strict (any field error raises).
    """
    # Empty/missing payload = transport failure: never tolerant-degrade.
    # Strict-path failure shape (mirror RuntimeError messages below).
    if result is None:
        raise RuntimeError(f"Review result must be an object, got {type(result).__name__}.")
    if isinstance(result, dict) and not result:
        raise RuntimeError("Review result missing required 'findings' key.")

    try:
        if not isinstance(result, dict):
            raise RuntimeError(f"Review result must be an object, got {type(result).__name__}.")
        if "findings" not in result:
            raise RuntimeError("Review result missing required 'findings' key.")
        if "summary" not in result:
            raise RuntimeError("Review result missing required 'summary' key.")
        if not isinstance(result["findings"], list):
            raise RuntimeError("Review result 'findings' must be an array.")
        if not isinstance(result["summary"], str) or not result["summary"].strip():
            raise RuntimeError("Review result 'summary' must be a non-empty string.")

        salvaged: list[Any] = []
        dropped: list[str] = []
        for i, finding in enumerate(result["findings"]):
            err = _finding_field_error(finding, i)
            if err is None:
                salvaged.append(finding)
                continue
            if not tolerant:
                raise RuntimeError(err)
            dropped.append(err)

        if not dropped:
            return result

        # Tolerant salvage: keep valid findings; note drops in raw_tail ([OBS-04]).
        # S1-A-01: the note is bounded via _raw_tail/_RAW_TAIL_CHARS — a large
        # malformed findings array would otherwise inflate raw_tail without limit
        # (50KB+ observed); the drop COUNT survives ahead of the capped detail.
        out = dict(result)
        out["findings"] = salvaged
        note = f"dropped {len(dropped)} invalid finding(s): {_raw_tail('; '.join(dropped))}"
        prior_tail = out.get("raw_tail")
        if isinstance(prior_tail, str) and prior_tail.strip():
            out["raw_tail"] = f"{_raw_tail(prior_tail)}\n{note}"
        else:
            out["raw_tail"] = note
        return out
    except RuntimeError as exc:
        if tolerant:
            return _unparseable_review_result(result, reason=str(exc))
        raise


# ---------------------------------------------------------------------------
# MCP recording
# ---------------------------------------------------------------------------


def _record_findings(
    findings: list[dict[str, Any]],
    *,
    task_ref: str,
    session: str,
    lane_id: str | None = None,
    orchestrator_root: Path,
) -> list[str]:
    """Record each finding into MCP atomically. Returns list of finding IDs that were recorded."""

    from workbay_handoff_mcp import RuntimeConfig, batch_record_review_findings, configure_runtime

    runtime = RuntimeConfig.for_repo(orchestrator_root)
    configure_runtime(runtime)

    actor: WriteActor = {}
    if lane_id:
        actor["lane_id"] = lane_id

    batch_items: list[BatchFindingItem] = []
    recorded_ids: list[str] = []
    for i, finding in enumerate(findings):
        finding_id = _generate_finding_id(lane_id, i, finding)
        recorded_ids.append(finding_id)

        details: ReviewFindingDetails = {}
        if "line_start" in finding and isinstance(finding["line_start"], int):
            details["line_start"] = finding["line_start"]
        if "line_end" in finding and isinstance(finding["line_end"], int):
            details["line_end"] = finding["line_end"]
        if "fix" in finding and isinstance(finding["fix"], str):
            details["fix"] = finding["fix"]

        batch_items.append(
            {
                "finding_id": finding_id,
                "severity": finding["severity"],
                "file_path": finding["file_path"],
                "description": f"[{finding['category']}] {finding['description']}",
                "details": details if details else None,
            }
        )

    batch_result = _load_mcp_payload(
        batch_record_review_findings(
            session=session,
            findings=batch_items,
            actor=actor if actor else None,
            task_ref=task_ref,
        )
    )
    if not batch_result.get("ok"):
        raise RuntimeError(f"batch_record_review_findings failed: {batch_result.get('error', 'unknown error')}")

    return recorded_ids


# ---------------------------------------------------------------------------
# Public API for daemon callers
# ---------------------------------------------------------------------------


def run_review(
    *,
    worktree_path: Path,
    lane_id: str | None = None,
    task_ref: str | None = None,
    session: str | None = None,
    orchestrator_root: Path | None = None,
    backend: str = "codex-cli",
    reasoning_effort: str | None = None,
    model: str | None = None,
    codex_bin: str | None = None,
    codex_args: list[str] | None = None,
    grok_bin: str | None = None,
    grok_args: list[str] | None = None,
    grok_max_turns: int | None = None,
    grok_timeout: int | None = None,
    review_kind: ReviewKind | str | None = None,
    use_latest_slice: bool = False,
    record_findings: bool = False,
    dry_run: bool = False,
    progress_callback: Callable[..., None] | None = None,
    rules_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run a full review cycle: discover changes, build prompt, execute Codex, validate, optionally record."""
    from workbay_handoff_mcp.enums import ReviewKind  # noqa: PLC0415

    # 1. Load manifest for overrides
    lane_cfg: dict[str, Any] = {}
    if task_ref and lane_id:
        lane_cfg = (
            get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root) if orchestrator_root else None)
            or {}
        )

    # Priority: CLI > Manifest > Default
    backend_name = backend
    if backend_name == "codex-cli" and lane_cfg.get("preferred_backend"):
        backend_name = str(lane_cfg["preferred_backend"])
    backend_name = validate_backend(backend_name)

    model_name = model or lane_cfg.get("preferred_model")

    env = None
    if orchestrator_root is not None:
        from workbay_orchestrator_mcp.orchestration._env import pythonpath_env

        env = pythonpath_env(orchestrator_root, task_ref=task_ref, lane_id=lane_id)
    elif reasoning_effort:
        env = {}
    if env is not None:
        apply_backend_runtime_hints(env, reasoning_effort=reasoning_effort)
    scope = _resolve_review_scope(
        worktree_path=worktree_path,
        task_ref=task_ref,
        orchestrator_root=orchestrator_root,
        review_kind=review_kind,
        use_latest_slice=use_latest_slice,
    )
    changed = scope["changed_files"]
    _assert_recordable_review_scope(
        record_findings=record_findings,
        changed_files=changed,
        scope_source=scope["scope_source"],
    )
    stat = _diff_stat(worktree_path)
    guides = _detect_stack_guides(changed)
    prompt = _build_review_prompt(
        changed_files=changed,
        diff_stat=stat,
        stack_guides=guides,
        main_guide_filename=(
            "planning-review-guide.md" if scope["review_kind"] == ReviewKind.PLANNING else "branch-review-guide.md"
        ),
        lane_id=lane_id,
        orchestrator_root=orchestrator_root,
        rules_dir=rules_dir,
    )

    if dry_run:
        return {
            "dry_run": True,
            "backend": backend_name,
            "prompt": prompt,
            "findings": [],
            "summary": "Dry-run mode: no review executed.",
            "converged": True,
            "changed_files": changed,
            "stack_guides": guides,
            "review_kind": scope["review_kind"],
            "scope_source": scope["scope_source"],
            "scope_reason": scope["scope_reason"],
        }

    # Get adapter and execute. Per-backend ctor kwargs (a non-codex CLI adapter
    # rejects codex_bin/codex_args at construction), mirroring lane_exec.
    # Grok-family cycle-bounds capability covers grok-cli and grok-remote so
    # derived max_turns/timeout reach RemoteExecAdapter (0144 S5a / [RES-02]).
    # Coupling note: supports_token_budget_cycle_bounds currently implies grok
    # CTOR kwargs (grok_bin/max_turns/timeout). No separate grok-family registry
    # predicate exists; a future cycle-bounds-capable non-grok backend would get
    # grok kwargs and TypeError unless routed by adapter family first.
    adapter_kwargs: dict[str, Any] = {}
    if backend_name == "codex-cli":
        adapter_kwargs = {"codex_bin": codex_bin, "codex_args": codex_args}
    elif backend_supports_token_budget_cycle_bounds(backend_name):
        adapter_kwargs = {"grok_bin": grok_bin, "grok_args": grok_args}
        if grok_max_turns is not None:
            adapter_kwargs["max_turns"] = grok_max_turns
        if grok_timeout is not None:
            adapter_kwargs["timeout"] = grok_timeout
        # Materialize the worktree-scoped Composer-only config for standalone
        # review turns that never ran the execute-phase bootstrap, so the
        # attribution config-env is present, not just the prompt-suffix belt
        # (s5-a-008). Idempotent (merge-don't-clobber). Best-effort: a review of a
        # worktree without lane context still runs on the prompt-suffix guarantee.
        # Local grok-cli only — remote turns do not consume a local Composer config.
        if backend_name == "grok-cli" and orchestrator_root is not None and task_ref and lane_id:
            try:
                from workbay_orchestrator_mcp.orchestration.bootstrap_lane import (
                    ensure_grok_lane_config,  # noqa: PLC0415
                )

                ensure_grok_lane_config(worktree_path, model_name)
            except Exception:  # pragma: no cover - defensive, never fail a review on config
                pass
    adapter = get_adapter(backend_name, **adapter_kwargs)
    result = adapter.execute(
        prompt=prompt,
        schema=REVIEW_OUTPUT_SCHEMA,
        worktree_path=worktree_path,
        model=model_name,
        reasoning_effort=reasoning_effort,
        env=env,
        progress_callback=progress_callback,
    )
    raw_result = result.raw_payload
    # Retryable VM admission defer (remote_agent.sh exit 75): the marker dict is
    # non-empty, so it would otherwise tolerant-degrade to skipped_unparseable →
    # converged, masking the defer. No review ran — surface the typed defer so
    # the caller re-dispatches instead of converging (r07163433 HIGH-2).
    if isinstance(raw_result, dict) and raw_result.get("admission_deferred"):
        defer_reason = str(raw_result.get("defer_reason") or "vm_admission")
        raise ReviewAdmissionDeferred(
            f"review turn deferred by VM admission ({defer_reason}); retryable, no review output"
        )
    # Grok --no-subagents smoke review often emits handoff-shaped JSON (or prose
    # fragments) without findings/summary — tolerate that as a typed degrade so a
    # green self-verify is never hard-failed ([OBS-08]). Codex stays strict.
    # Capability-gated (not a grok-cli name literal) so grok-remote gets the same
    # tolerant degrade the grok family needs (0144 R2). Empty raw_payload still
    # fails loud (0144 R3 / HIGH-2): transport failures must not become
    # skipped_unparseable → converged.
    tolerant = backend_supports_token_budget_cycle_bounds(backend_name)
    validated = _validate_review_result(raw_result, tolerant=tolerant)
    review_status = validated.get("review")
    findings = list(validated.get("findings") or [])
    summary = str(validated.get("summary") or "")

    output: dict[str, Any] = {
        "findings": findings,
        "summary": summary,
        "converged": findings_converged(findings),
        "changed_files": changed,
        "stack_guides": guides,
        "review_kind": scope["review_kind"],
        "scope_source": scope["scope_source"],
        "scope_reason": scope["scope_reason"],
    }
    if review_status == REVIEW_SKIPPED_UNPARSEABLE:
        # Explicit discriminator so the pass engine can branch without archaeology.
        output["review"] = REVIEW_SKIPPED_UNPARSEABLE
    # Always forward salvage/degrade notes ([OBS-04]): mixed valid/invalid findings
    # keep salvaged items while raw_tail records dropped items — not only on full
    # skipped_unparseable degrade (BR-0108-S1-05).
    if "raw_tail" in validated:
        output["raw_tail"] = validated["raw_tail"]

    if record_findings and review_status != REVIEW_SKIPPED_UNPARSEABLE:
        if not task_ref:
            raise RuntimeError("--task-ref is required when --record-findings is set.")
        if not session:
            raise RuntimeError("--session is required when --record-findings is set.")
        if not orchestrator_root:
            raise RuntimeError("--orchestrator-root is required when --record-findings is set.")
        recorded_ids = _record_findings(
            findings,
            task_ref=task_ref,
            session=session,
            lane_id=lane_id,
            orchestrator_root=orchestrator_root,
        )
        output["recorded_finding_ids"] = recorded_ids

    return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Self-review runner: execute structured code review via Codex.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("schema", help="Print the review output JSON schema.")

    run_parser = subparsers.add_parser("run", help="Execute a review against a lane worktree.")
    run_parser.add_argument("--worktree-path", required=True, help="Path to the lane worktree.")
    run_parser.add_argument("--lane-id", help="Lane identifier for finding ID generation.")
    run_parser.add_argument("--task-ref", help="Task reference (required with --record-findings).")
    run_parser.add_argument("--session", help="Session identifier (required with --record-findings).")
    run_parser.add_argument("--orchestrator-root", help="Orchestrator root path (required with --record-findings).")
    run_parser.add_argument(
        "--rules-dir",
        help="Optional review-rules directory override. Defaults to the package-bundled rules.",
    )
    run_parser.add_argument(
        "--backend",
        default="codex-cli",
        choices=BACKEND_CHOICES,
        help="Execution backend to use (default: codex-cli).",
    )
    run_parser.add_argument(
        "--reasoning-effort",
        choices=WORKER_REASONING_EFFORT_CHOICES,
        help="Optional reasoning effort hint for codex-subagent review turns.",
    )
    run_parser.add_argument("--model", help="Explicit model to use (e.g. gpt-5.4-mini).")
    run_parser.add_argument("--grok-bin", help="Explicit path to the grok binary.")
    run_parser.add_argument("--grok-args", help="Extra args for grok exec (space-separated).")
    run_parser.add_argument(
        "--grok-max-turns",
        type=int,
        default=None,
        help="Per-invocation grok --max-turns cap (default: adapter/backend default).",
    )
    run_parser.add_argument(
        "--grok-timeout",
        type=int,
        default=None,
        help="Per-invocation grok wall-clock timeout seconds (default: adapter/backend default).",
    )
    run_parser.add_argument(
        "--review-kind",
        choices=("branch", "planning"),
        help="Preferred review workflow. Packet-backed planning reviews only match docs-only slices.",
    )
    run_parser.add_argument(
        "--latest-slice",
        action="store_true",
        help="Resolve changed files from the latest completed slice packet when available, with branch-diff fallback.",
    )
    run_parser.add_argument(
        "--record-findings",
        action="store_true",
        help="Record findings into MCP before returning.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the assembled prompt and skip Codex/MCP side effects.",
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.command == "schema":
        print(json.dumps(REVIEW_OUTPUT_SCHEMA, indent=2))
        return 0

    worktree_path = Path(args.worktree_path).expanduser().resolve()
    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve() if args.orchestrator_root else None
    rules_dir = Path(args.rules_dir).expanduser().resolve() if args.rules_dir else None

    result = run_review(
        worktree_path=worktree_path,
        lane_id=args.lane_id,
        task_ref=args.task_ref,
        session=args.session,
        orchestrator_root=orchestrator_root,
        backend=args.backend,
        reasoning_effort=args.reasoning_effort,
        model=args.model,
        grok_bin=args.grok_bin,
        grok_args=args.grok_args.split() if args.grok_args else None,
        grok_max_turns=args.grok_max_turns,
        grok_timeout=args.grok_timeout,
        review_kind=args.review_kind,
        use_latest_slice=args.latest_slice,
        record_findings=args.record_findings,
        dry_run=args.dry_run,
        rules_dir=rules_dir,
    )

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
