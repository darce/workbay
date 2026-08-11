#!/usr/bin/env python3
"""SessionStart hook: re-inject handoff.db references into model context.

internal (implementation note). Claude Code adds SessionStart hook
**stdout** to the model's context — the documented injection point this
repo's compaction module was missing on the read side. The hook reads the
``SessionStart`` event from stdin, gates on its ``source`` (default:
``compact`` / ``resume``; ``WORKBAY_REINJECT_SOURCES`` overrides),
resolves the active task from the workspace, and emits ONE budgeted fenced
block of handoff.db references to stdout: task_ref, status, focus, latest
``compaction_id`` + turn range, open finding ids, the next-action hint, and
literal command hints for deeper agent-initiated recovery.

On gated reinjection attempts the hook best-effort writes one
``session_reinjections`` telemetry row; a failed write logs to stderr and
never blocks emission.

Failure-mode contract (implementation note, implementation note; mirrors compact-session.py):

- Emit the block on stdout and exit 0 on success. Diagnostics go to
  stderr only; stdout carries nothing except the injected block.
- On true full-skip (source not enabled, no active task, disabled surface,
  DB unreachable, invalid settings), log ``reinject skipped: <reason>`` to
  stderr, emit NOTHING on stdout, and exit 0.
- Semantic-only suppressions (non-compact source, missing compaction_id,
  sticky already-reinjected, budget drop) log
  ``reinject semantic skipped: <reason>`` while the generic block may still
  emit. Never blocks session start.

The single exception is strict-mode protocol drift
(``WORKBAY_HOOK_PROTOCOL_STRICT=1`` plus a malformed event payload):
``_protocol.validate_event`` raises ``SystemExit(2)`` and the hook
propagates it, matching every other wired hook.

Tunables (documented in ``harness-protocol.yaml`` ``reinjection:`` block;
env wins over the contract default):

- ``WORKBAY_REINJECT_SOURCES``       comma list for the **generic** block,
                                       default ``compact,resume``
- ``WORKBAY_REINJECT_BUDGET_CHARS``  total stdout budget, default ``1500``
- ``WORKBAY_REINJECT_SEMANTIC``      master semantic-arm switch (default off).
                                       When truthy, ``relevant:`` requires
                                       source=compact + compaction_id and is
                                       sticky for delivered selected emissions
                                       only; resume keeps generic-only
- ``WORKBAY_REINJECT_SEMANTIC_TOP_K`` top-K concept count, default ``8``
"""

from __future__ import annotations


import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from workbay_handoff_mcp import CompactionSettings

_DEFAULT_SOURCES = ("compact", "resume")
_DEFAULT_BUDGET_CHARS = 1500
_MAX_FINDING_IDS = 5
_FENCE_OPEN = "```workbay-reinject"
_FENCE_CLOSE = "```"
_RECOVER_HINT = (
    'recover: compaction(get_latest) | get_handoff_state(read_profile="hot_summary")'
)
_HARNESS_CHOICES = ("claude-code", "codex", "grok", "cursor", "manual")


def _env_alias(canonical: str, default: str | None = None) -> str | None:
    """Read a canonical ``WORKBAY_*`` override. The lazy ``_interp`` import keeps
    the bare-``python3`` module load (and the importlib-loaded test harness) free
    of a hooks-dir ``sys.path`` requirement; the shim resolves via
    ``workbay_protocol`` when importable, else a stdlib fallback."""
    from _interp import resolve_env_alias

    return resolve_env_alias(canonical, default=default)


def _resolve_harness() -> str:
    raw = (_env_alias("WORKBAY_HANDOFF_HARNESS") or "").strip()
    if not raw:
        # Grok fallback (REV-E-010), mirroring compact-session.py: grok
        # delivers SessionStart hooks via the compat-loaded
        # .claude/settings.json entry, which must not carry an inline
        # WORKBAY_HANDOFF_HARNESS export (it would mislabel Claude rows).
        # Grok exports GROK_WORKSPACE_ROOT for hook commands, so its
        # presence identifies a grok launcher when the explicit override is
        # absent; Claude Code never sets it. Without this, a grok session
        # would receive the Claude-only JSON envelope instead of the raw
        # fenced block, violating the harness-neutral injection contract
        # (implementation note R1; harness-protocol.yaml).
        if os.environ.get("GROK_WORKSPACE_ROOT", "").strip():
            return "grok"
        return "claude-code"
    if raw in _HARNESS_CHOICES:
        return raw
    return "manual"


def _emit(message: str) -> None:
    print(message, file=sys.stderr)


class _PlainSemanticSignal:
    """Duck-type for semantic telemetry when embeddings.reinjection is unimportable.

    ``SemanticReinjectionResult`` lives in ``embeddings.reinjection``, which
    imports numpy at module top — unusable on the deps-missing path [OBS-08].
    Carry plain-string status/skip_reason only so ``record_session_reinjection``
    still lands a typed ``semantic_detail_json`` row.
    """

    __slots__ = (
        "status",
        "skip_reason",
        "model_id",
        "selected",
        "chars_used",
        "chars_budget",
        "score_hi",
        "score_lo",
    )

    def __init__(
        self,
        *,
        status: str,
        skip_reason: str | None,
        chars_budget: int = 0,
    ) -> None:
        self.status = status
        self.skip_reason = skip_reason
        self.model_id: str | None = None
        self.selected: list = []
        self.chars_used = 0
        self.chars_budget = max(0, int(chars_budget))
        self.score_hi: float | None = None
        self.score_lo: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "skip_reason": self.skip_reason,
            "model_id": self.model_id,
            "selected": list(self.selected),
            "chars_used": self.chars_used,
            "chars_budget": self.chars_budget,
            "score_hi": self.score_hi,
            "score_lo": self.score_lo,
        }


def _payload_value(
    payload: dict, snake_key: str, camel_key: str, default: str = ""
) -> str:
    value = payload.get(snake_key)
    if value:
        return str(value)
    camel_value = payload.get(camel_key)
    if camel_value:
        return str(camel_value)
    return default


def _git_repo_root() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:  # noqa: BLE001 -- best-effort discovery
        pass
    from resolve_handoff_src import workspace_env_anchor

    return workspace_env_anchor()


def _resolve_agent_handoff_src(repo_root: str) -> str:
    from resolve_handoff_src import resolve_agent_handoff_src

    return resolve_agent_handoff_src(repo_root)


def _ensure_in_repo_sources_on_path(repo_root: str) -> None:
    """Make the in-repo handoff + protocol sources importable.

    Hooks run under whichever Python the harness happens to launch; pinning
    the local ``packages/.../src`` paths first guarantees the worktree's
    code handles this session start. Same contract as compact-session.py.
    """
    for relative in (
        ("packages", "workbay-protocol", "src"),
        ("packages", "mcp-workbay-handoff", "src"),
    ):
        candidate = os.path.join(repo_root, *relative)
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.insert(0, candidate)
    src_path = _resolve_agent_handoff_src(repo_root)
    if os.path.isdir(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)


def _enabled_sources() -> tuple[str, ...]:
    raw = _env_alias("WORKBAY_REINJECT_SOURCES") or ""
    parsed = tuple(part.strip().lower() for part in raw.split(",") if part.strip())
    return parsed or _DEFAULT_SOURCES


def _resolve_budget_chars() -> int:
    raw = (_env_alias("WORKBAY_REINJECT_BUDGET_CHARS") or "").strip()
    if not raw:
        return _DEFAULT_BUDGET_CHARS
    budget = int(raw)  # ValueError surfaces as `invalid budget` in main()
    if budget <= 0:
        raise ValueError(f"budget must be positive, got {budget}")
    return budget


def _sanitize_field(value: str) -> str:
    """Flatten agent-authored values so they cannot break the fenced block.

    Newlines collapse to single spaces (one block line per field) and
    backtick runs of three or more shrink to two, so no interpolated value
    can ever close the ``workbay-reinject`` fence early.
    """
    flattened = " ".join(value.split())
    return re.sub(r"`{3,}", "``", flattened)


_DEFAULT_SEMANTIC_TOP_K = 8


def _semantic_enabled() -> bool:
    """internal: semantic top-K reinjection is opt-in (default off)."""
    return (_env_alias("WORKBAY_REINJECT_SEMANTIC") or "").strip().lower() not in {
        "",
        "0",
        "false",
        "no",
        "off",
    }


_SEMANTIC_ADVISORY_EMITTED = False


def _maybe_emit_semantic_activation_advisory() -> None:
    """implementation note S4: surface opt-in path when a provider is configured."""
    global _SEMANTIC_ADVISORY_EMITTED
    if _SEMANTIC_ADVISORY_EMITTED or _semantic_enabled():
        return
    try:
        model = (_env_alias("WORKBAY_HANDOFF_EMBEDDING_MODEL") or "").strip()
    except Exception:
        return
    if not model:
        return
    _SEMANTIC_ADVISORY_EMITTED = True
    print(
        "[reinject] semantic mode available but inactive — set WORKBAY_REINJECT_SEMANTIC=1 to enable",
        file=sys.stderr,
    )


def _semantic_top_k() -> int:
    raw = (_env_alias("WORKBAY_REINJECT_SEMANTIC_TOP_K") or "").strip()
    if not raw:
        return _DEFAULT_SEMANTIC_TOP_K
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_SEMANTIC_TOP_K
    return value if value > 0 else _DEFAULT_SEMANTIC_TOP_K


def _selected_semantic_delivered(block: str, semantic_result: object) -> bool:
    """True when selected concept bodies appear under the ``relevant:`` section.

    Delivery proof is scoped to the semantic section only. Bare whole-block
    substring matches on numeric entity ids are rejected because
    ``latest_compaction: … (turns N-M)`` and other envelope lines often contain
    small integers that would false-positive sticky selected status.
    """
    if "relevant:" not in block:
        return False
    # Isolate content from the first relevant: header through end of block.
    section = block.split("relevant:", 1)[1]
    selected = getattr(semantic_result, "selected", None) or []
    for item in selected:
        item_id = str(getattr(item, "id", "") or "").strip()
        kind = str(getattr(item, "kind", "") or "").strip()
        if item_id and kind:
            marker = f"[{kind}:{item_id}]"
            if marker in section:
                return True
        # Multi-line bullets: "- label: snippet" under relevant:
        label = str(getattr(item, "label", "") or "").strip()
        if label:
            for line in section.splitlines():
                stripped = line.strip()
                if not stripped.startswith("- "):
                    continue
                body = stripped[2:].strip()
                if body == label or body.startswith(f"{label}:"):
                    return True
    # Inline form: "relevant: [kind:id] …" or non-empty payload on same line
    header_rest = section.split("\n", 1)[0].strip()
    if header_rest and "[" in header_rest and "]" in header_rest:
        return True
    # Multi-line form with at least one bullet after header (body delivered)
    for line in section.splitlines():
        stripped = line.strip()
        if stripped.startswith("- ") and len(stripped) > 2:
            return True
    return False


def _semantic_concept_line(
    *,
    task_ref: str,
    objective: str,
    focus: str,
    action_texts: list[str],
    latest_compaction_id: str | None,
    top_k: int,
) -> str | None:
    """Legacy opaque ``relevant: kind:id`` line (pre-readable fallback).

    internal. Retained for historical tests; the primary semantic path under
    ``WORKBAY_REINJECT_SEMANTIC`` uses :func:`_readable_semantic_lines`.
    """
    try:
        from workbay_handoff_mcp.embeddings.ranking import (  # type: ignore[import-not-found]
            compose_anchor,
            rank_concepts_by_anchor,
        )
        from workbay_handoff_mcp.embeddings.store import (  # type: ignore[import-not-found]
            CONCEPT_ENTITY_KINDS,
            _resolve_provider,
            deserialize_vector,
        )
        from workbay_handoff_mcp.shared_schema import (  # type: ignore[import-not-found]
            _get_db_connection,
        )
    except ImportError:
        return None
    try:
        provider = _resolve_provider()
        if provider is None:
            return None
        with _get_db_connection() as conn:
            persisted = None
            if latest_compaction_id:
                row = conn.execute(
                    "SELECT anchor_vector FROM session_compactions WHERE compaction_id = ?",
                    (latest_compaction_id,),
                ).fetchone()
                if row is not None and row[0] is not None:
                    persisted = deserialize_vector(row[0])
            anchor = compose_anchor(
                provider,
                persisted_anchor=persisted,
                texts=[objective, focus, *action_texts],
            )
            if anchor is None:
                return None
            # Exclude the always-rendered identity fields: objective/focus feed the
            # anchor and already appear on their own block lines, so ranking them
            # would just re-surface what the operator already sees. Surface other
            # concepts (decisions/findings/blockers/compaction residual) instead.
            rank_kinds = tuple(
                k for k in CONCEPT_ENTITY_KINDS if k not in ("handoff_state.objective", "handoff_state.focus")
            )
            ranked = rank_concepts_by_anchor(
                conn, anchor, task_ref, top_k=top_k, entity_kinds=rank_kinds, model_id=provider.model_id
            )
        if not ranked:
            return None
        refs = ", ".join(f"{r.entity_kind}:{r.entity_id}" for r in ranked)
        return _sanitize_field(f"relevant: {refs}")
    except Exception as exc:  # noqa: BLE001 - semantic ranking is best-effort
        _emit(f"reinject semantic ranking skipped: {exc}")
        return None


def _compute_semantic_content_budget(
    *,
    budget_chars: int,
    harness: str,
    settings: CompactionSettings,
    base_lines: list[str],
    recover_hint: str,
    notify_allowance_chars: int,
    provisional_notify: str,
) -> int:
    """Return character budget reserved for readable semantic snippet lines."""
    envelope_overhead = 0
    if harness == "claude-code" and settings.compaction_notify:
        from workbay_handoff_mcp.compaction import reinject_json_envelope_overhead_chars

        envelope_overhead = reinject_json_envelope_overhead_chars(
            block="",
            system_message=provisional_notify,
        )
    block_budget = max(1, budget_chars - envelope_overhead)
    fixed = _render_block([*base_lines, recover_hint], budget_chars=block_budget)
    if fixed is None:
        return 0
    return max(0, block_budget - (len(fixed) + 1))


def _readable_semantic_lines(
    *,
    task_ref: str,
    objective: str,
    focus: str,
    action_texts: list[str],
    latest_compaction_id: str | None,
    semantic_content_budget_chars: int,
):
    """Build readable ``relevant:`` snippet lines via the package service.

    Fail-open: any error degrades to no semantic lines and a typed skip result.
    """
    try:
        from workbay_handoff_mcp.embeddings.reinjection import (  # type: ignore[import-not-found]
            ReinjectionConfig,
            SemanticReinjectionResult,
            build_semantic_reinjection_packet,
            render_readable_relevant_lines,
        )
        from workbay_handoff_mcp.embeddings.store import (  # type: ignore[import-not-found]
            _resolve_provider,
            deserialize_vector,
        )
        from workbay_handoff_mcp.shared_schema import _get_db_connection  # type: ignore[import-not-found]
    except ImportError as exc:
        # [OBS-08] silence is not success — embeddings extra / model absent must
        # emit + land degraded telemetry. Do not construct SemanticReinjectionResult
        # here: that class is in the same unimportable module (numpy at top).
        _emit(
            "reinject semantic degraded: embeddings unavailable "
            f"({exc}); remedy: workbay-bootstrap provision-embeddings"
        )
        return [], _PlainSemanticSignal(
            status="degraded",
            skip_reason="deps_or_model_missing",
            chars_budget=semantic_content_budget_chars,
        )
    try:
        provider = _resolve_provider()
        config = ReinjectionConfig.from_env()
        with _get_db_connection() as conn:
            persisted = None
            if latest_compaction_id:
                row = conn.execute(
                    "SELECT anchor_vector FROM session_compactions WHERE compaction_id = ?",
                    (latest_compaction_id,),
                ).fetchone()
                if row is not None and row[0] is not None:
                    persisted = deserialize_vector(row[0])
            result = build_semantic_reinjection_packet(
                conn,
                task_ref=task_ref,
                provider=provider,
                persisted_anchor=persisted,
                visible_texts=[objective, focus, *action_texts],
                semantic_content_budget_chars=semantic_content_budget_chars,
                config=config,
            )
        if result.status != "selected" or not result.selected:
            return [], result
        lines = [_sanitize_field(line) for line in render_readable_relevant_lines(result.selected)]
        return lines, result
    except Exception as exc:  # noqa: BLE001 - semantic packet is best-effort
        _emit(f"reinject semantic packet skipped: {exc}")
        try:
            from workbay_handoff_mcp.embeddings.reinjection import SemanticReinjectionResult
        except ImportError:
            return [], None
        return [], SemanticReinjectionResult(
            status="degraded",
            skip_reason="error",
            model_id=None,
            chars_budget=max(0, semantic_content_budget_chars),
        )


def _render_block(lines: list[str], *, budget_chars: int) -> str | None:
    """Assemble the fenced block, greedily keeping lines that fit the budget.

    The first line (task_ref) is mandatory: when the budget cannot fit the
    fences plus that line, return ``None`` so the caller skips emission
    instead of injecting a contentless fence pair. Remaining content lines
    are included in priority order while the total rendered size (including
    the trailing newline ``print`` appends) stays within ``budget_chars``.
    """

    def _rendered_len(content: list[str]) -> int:
        return len("\n".join([_FENCE_OPEN, *content, _FENCE_CLOSE])) + 1

    if not lines or _rendered_len(lines[:1]) > budget_chars:
        return None
    kept: list[str] = [lines[0]]
    for line in lines[1:]:
        if _rendered_len([*kept, line]) <= budget_chars:
            kept.append(line)
    return "\n".join([_FENCE_OPEN, *kept, _FENCE_CLOSE])


def _reinject(
    *,
    repo_root: str,
    budget_chars: int,
    settings: CompactionSettings,
    session_id: str,
    source: str,
) -> int:
    """Resolve the active task and emit the budgeted block."""
    try:
        from workbay_handoff_mcp import (  # type: ignore[import-not-found]
            RuntimeConfig,
            configure_runtime,
            get_handoff_state,
            get_latest_compaction,
        )
        from workbay_handoff_mcp.compaction import (  # type: ignore[import-not-found]
            format_reinject_notify_message,
            format_reinject_session_start_stdout,
            record_session_reinjection,
            reinject_json_envelope_overhead_chars,
            resolve_compaction_disabled,
        )
        from workbay_handoff_mcp.shared_schema import (  # type: ignore[import-not-found]
            _get_db_connection,
        )
    except ImportError as exc:
        _emit(f"reinject skipped: workbay_handoff_mcp import: {exc}")
        return 0

    state_dir_override = _env_alias("WORKBAY_HANDOFF_STATE_DIR") or None
    try:
        configure_runtime(
            RuntimeConfig.for_repo(Path(repo_root), state_dir=state_dir_override)
        )
    except Exception as exc:  # noqa: BLE001
        _emit(f"reinject skipped: runtime configuration: {exc}")
        return 0

    try:
        with _get_db_connection() as conn:
            try:
                from workbay_handoff_mcp.shared_primitives import (  # type: ignore[import-not-found]
                    resolve_active_task_ref_for_hook,
                )

                resolution = resolve_active_task_ref_for_hook(conn, strict=False)
                task_ref = resolution.task_ref
                if resolution.tiebreak_note:
                    _emit(resolution.tiebreak_note)
            except Exception as exc:  # noqa: BLE001
                _emit(f"reinject skipped: active task unresolved: {exc}")
                return 0
            # internal: a disabled compaction surface silences re-injection
            # through the same unified resolver as the Stop hook + advisory.
            disabled, disabled_source = resolve_compaction_disabled(
                env=os.environ, conn=conn, task_ref=task_ref
            )
    except Exception as exc:  # noqa: BLE001 -- DB-open failure must not crash the hook
        _emit(f"reinject skipped: resolver unreachable: {exc}")
        return 0

    if disabled:
        _emit(f"reinject skipped: disabled (source={disabled_source})")
        return 0

    try:
        envelope = get_handoff_state(task_ref=task_ref, read_profile="hot_summary")
    except Exception as exc:  # noqa: BLE001
        _emit(f"reinject skipped: handoff state read: {exc}")
        return 0
    if not envelope.get("ok"):
        _emit(f"reinject skipped: handoff state read not ok: {envelope!r:.200}")
        return 0
    data = envelope.get("data") or {}
    active = data.get("active") or {}

    try:
        latest = get_latest_compaction(task_ref)
    except Exception as exc:  # noqa: BLE001
        _emit(f"reinject skipped: latest compaction lookup: {exc}")
        return 0

    # Stale-installed-package shim: pre-0.12.9 servers return the bare
    # StructuredSummary instead of a CompactionRecord wrapper. Fail open to
    # the old shape rather than crashing the SessionStart hook on the
    # documented installed-vs-payload version skew.
    latest_summary = getattr(latest, "summary", latest) if latest is not None else None

    lines = [f"task_ref: {_sanitize_field(str(task_ref))}"]
    status = _sanitize_field(str(active.get("status") or ""))
    if status:
        lines.append(f"status: {status}")
    focus = _sanitize_field(str(active.get("focus") or ""))
    if focus:
        lines.append(f"focus: {focus}")
    if latest_summary is not None:
        lines.append(
            f"latest_compaction: {latest_summary.compaction_id} "
            f"(turns {latest_summary.turn_range.start_turn}-{latest_summary.turn_range.end_turn})"
        )
    finding_ids = [
        _sanitize_field(str(row.get("finding_id") or ""))
        for row in (data.get("findings_open") or [])
        if row.get("finding_id")
    ][:_MAX_FINDING_IDS]
    if finding_ids:
        lines.append(f"open_findings: {', '.join(finding_ids)}")
    actions = data.get("actions_pending") or []
    if actions:
        next_action = _sanitize_field(str(actions[0].get("action") or ""))
        if next_action:
            lines.append(f"next_action: {next_action}")
    use_semantic = _semantic_enabled()
    sticky_dedupe_signal: _PlainSemanticSignal | None = None
    if not use_semantic:
        _maybe_emit_semantic_activation_advisory()
    if use_semantic and source != "compact":
        _emit(
            f"reinject semantic skipped: requires source=compact (got source={source})"
        )
        use_semantic = False
    elif use_semantic and source == "compact":
        compaction_id = (
            str(latest_summary.compaction_id)
            if latest_summary is not None and latest_summary.compaction_id
            else ""
        )
        if not compaction_id:
            _emit("reinject semantic skipped: requires compaction_id on compact source")
            use_semantic = False
        else:
            try:
                from workbay_handoff_mcp.compaction import (  # type: ignore[import-not-found]
                    session_reinjection_exists,
                )

                with _get_db_connection() as conn:
                    already_reinjected = session_reinjection_exists(
                        conn,
                        task_ref=str(task_ref),
                        compaction_id=compaction_id,
                    )
            except ImportError as exc:
                # [REF-20] helper genuinely absent (stale/skewed install): cannot
                # evaluate dedupe → suppress semantic to avoid duplicate reinjects.
                already_reinjected = False
                use_semantic = False
                sticky_dedupe_signal = _PlainSemanticSignal(
                    status="skipped",
                    skip_reason="dedupe_unavailable",
                )
                _emit(
                    "reinject semantic suppressed: sticky-check helper unavailable "
                    f"({exc}); skip=dedupe_unavailable"
                )
            except Exception as exc:  # noqa: BLE001 - transient DB hiccup fail-open
                already_reinjected = False
                _emit(f"reinject sticky-check failed (fail-open): {exc}")
            if already_reinjected:
                _emit(
                    "reinject semantic skipped: already reinjected for "
                    f"compaction_id={compaction_id}"
                )
                use_semantic = False
    semantic_result = sticky_dedupe_signal
    reinjection_config = None
    if use_semantic:
        try:
            from workbay_handoff_mcp.embeddings.reinjection import ReinjectionConfig  # type: ignore[import-not-found]

            reinjection_config = ReinjectionConfig.from_env()
        except ImportError:
            reinjection_config = None
        provisional_notify = format_reinject_notify_message(
            task_ref=str(task_ref),
            compaction_id=latest_summary.compaction_id if latest_summary is not None else None,
            start_turn=latest_summary.turn_range.start_turn if latest_summary is not None else None,
            end_turn=latest_summary.turn_range.end_turn if latest_summary is not None else None,
            source=source,
            semantic_status="selected",
            selected_count=99,
            selected_kinds=["decision", "finding", "compaction"],
            score_hi=1.0,
            score_lo=0.0,
            chars_used=9999,
            chars_budget=9999,
            max_chars=(reinjection_config.notify_allowance_chars if reinjection_config else 220),
        )
        semantic_budget = _compute_semantic_content_budget(
            budget_chars=budget_chars,
            harness=_resolve_harness(),
            settings=settings,
            base_lines=list(lines),
            recover_hint=_RECOVER_HINT,
            notify_allowance_chars=(reinjection_config.notify_allowance_chars if reinjection_config else 220),
            provisional_notify=provisional_notify,
        )
        try:
            sem_lines, semantic_result = _readable_semantic_lines(
                task_ref=str(task_ref),
                objective=str(active.get("objective") or ""),
                focus=str(active.get("focus") or ""),
                action_texts=[str(item.get("action") or "") for item in actions],
                latest_compaction_id=(latest_summary.compaction_id if latest_summary is not None else None),
                semantic_content_budget_chars=semantic_budget,
            )
        except Exception as exc:  # noqa: BLE001 - fail-open on hot path
            _emit(f"reinject semantic packet skipped: {exc}")
            try:
                from workbay_handoff_mcp.embeddings.reinjection import SemanticReinjectionResult  # type: ignore[import-not-found]

                sem_lines, semantic_result = [], SemanticReinjectionResult(
                    status="degraded",
                    skip_reason="error",
                    model_id=None,
                    chars_budget=max(0, semantic_budget),
                )
            except ImportError:
                sem_lines, semantic_result = [], None
        if sem_lines:
            lines.extend(sem_lines)
    lines.append(_RECOVER_HINT)

    harness = _resolve_harness()

    # Render the payload block first so delivery demotion can fix notify + telemetry
    # before anything is printed (mutable-derived-data-drift: notify must match emit).
    provisional_notify = format_reinject_notify_message(
        task_ref=str(task_ref),
        compaction_id=latest_summary.compaction_id if latest_summary is not None else None,
        start_turn=latest_summary.turn_range.start_turn if latest_summary is not None else None,
        end_turn=latest_summary.turn_range.end_turn if latest_summary is not None else None,
        source=source,
        semantic_status=(semantic_result.status if semantic_result is not None else None),
        semantic_skip_reason=(semantic_result.skip_reason if semantic_result is not None else None),
        selected_count=(len(semantic_result.selected) if semantic_result is not None else None),
        selected_kinds=(
            [item.kind for item in semantic_result.selected] if semantic_result is not None else None
        ),
        score_hi=(semantic_result.score_hi if semantic_result is not None else None),
        score_lo=(semantic_result.score_lo if semantic_result is not None else None),
        chars_used=(semantic_result.chars_used if semantic_result is not None else None),
        chars_budget=(semantic_result.chars_budget if semantic_result is not None else None),
        max_chars=(reinjection_config.notify_allowance_chars if reinjection_config else 220),
    )

    block_budget = budget_chars
    if harness == "claude-code" and settings.compaction_notify:
        shell_overhead = reinject_json_envelope_overhead_chars(
            block="",
            system_message=provisional_notify,
        )
        block_budget = max(1, budget_chars - shell_overhead)

    block = _render_block(lines, budget_chars=block_budget)
    if block is None:
        _emit(
            f"reinject skipped: budget {budget_chars} cannot fit the "
            "mandatory task_ref line"
        )
        return 0

    # Sticky only when selected concept *bodies* reached the emitted block.
    # Budget shrink can drop trailing bullets while keeping a bare ``relevant:``
    # header; recording status=selected would permanently suppress that id.
    semantic_detail_for_record: dict[str, object] | None = None
    notify_status = semantic_result.status if semantic_result is not None else None
    notify_skip = semantic_result.skip_reason if semantic_result is not None else None
    notify_selected_count = (
        len(semantic_result.selected) if semantic_result is not None else None
    )
    notify_selected_kinds = (
        [item.kind for item in semantic_result.selected] if semantic_result is not None else None
    )
    if semantic_result is not None:
        semantic_detail_for_record = semantic_result.to_dict()
        if semantic_result.status == "selected" and not _selected_semantic_delivered(
            block, semantic_result
        ):
            semantic_detail_for_record = {
                **semantic_detail_for_record,
                "status": "skipped",
                "skip_reason": "budget_exhausted",
            }
            notify_status = "skipped"
            notify_skip = "budget_exhausted"
            notify_selected_count = 0
            notify_selected_kinds = []
            _emit(
                "reinject semantic skipped: selected concepts dropped by block budget"
            )

    notify_message = format_reinject_notify_message(
        task_ref=str(task_ref),
        compaction_id=latest_summary.compaction_id if latest_summary is not None else None,
        start_turn=latest_summary.turn_range.start_turn if latest_summary is not None else None,
        end_turn=latest_summary.turn_range.end_turn if latest_summary is not None else None,
        source=source,
        semantic_status=notify_status,
        semantic_skip_reason=notify_skip,
        selected_count=notify_selected_count,
        selected_kinds=notify_selected_kinds,
        score_hi=(semantic_result.score_hi if semantic_result is not None else None),
        score_lo=(semantic_result.score_lo if semantic_result is not None else None),
        chars_used=(semantic_result.chars_used if semantic_result is not None else None),
        chars_budget=(semantic_result.chars_budget if semantic_result is not None else None),
        max_chars=(reinjection_config.notify_allowance_chars if reinjection_config else 220),
    )

    emitted_chars = 0
    if harness == "claude-code" and settings.compaction_notify:
        stdout_payload = format_reinject_session_start_stdout(
            block=block,
            system_message=notify_message,
        )
        if len(stdout_payload) + 1 > budget_chars:
            shrink = len(stdout_payload) - budget_chars
            block = _render_block(lines, budget_chars=max(1, block_budget - shrink))
            if block is None:
                _emit(
                    f"reinject skipped: budget {budget_chars} cannot fit the "
                    "mandatory task_ref line"
                )
                return 0
            # Re-check delivery after second-pass shrink.
            if (
                semantic_result is not None
                and semantic_result.status == "selected"
                and semantic_detail_for_record is not None
                and semantic_detail_for_record.get("status") == "selected"
                and not _selected_semantic_delivered(block, semantic_result)
            ):
                semantic_detail_for_record = {
                    **semantic_detail_for_record,
                    "status": "skipped",
                    "skip_reason": "budget_exhausted",
                }
                notify_message = format_reinject_notify_message(
                    task_ref=str(task_ref),
                    compaction_id=latest_summary.compaction_id if latest_summary is not None else None,
                    start_turn=latest_summary.turn_range.start_turn if latest_summary is not None else None,
                    end_turn=latest_summary.turn_range.end_turn if latest_summary is not None else None,
                    source=source,
                    semantic_status="skipped",
                    semantic_skip_reason="budget_exhausted",
                    selected_count=0,
                    selected_kinds=[],
                    score_hi=(semantic_result.score_hi if semantic_result is not None else None),
                    score_lo=(semantic_result.score_lo if semantic_result is not None else None),
                    chars_used=(semantic_result.chars_used if semantic_result is not None else None),
                    chars_budget=(semantic_result.chars_budget if semantic_result is not None else None),
                    max_chars=(reinjection_config.notify_allowance_chars if reinjection_config else 220),
                )
                _emit(
                    "reinject semantic skipped: selected concepts dropped by block budget"
                )
            stdout_payload = format_reinject_session_start_stdout(
                block=block,
                system_message=notify_message,
            )
        print(stdout_payload)
        emitted_chars = len(stdout_payload) + 1
        _emit(
            f"reinject emitted: task_ref={task_ref} chars={emitted_chars} "
            "shape=json_envelope"
        )
    else:
        print(block)
        emitted_chars = len(block) + 1
        _emit(f"reinject emitted: task_ref={task_ref} chars={emitted_chars}")

    try:
        with _get_db_connection() as conn:
            record_session_reinjection(
                conn,
                session_id=session_id,
                harness=harness,
                task_ref=str(task_ref),
                compaction_id=latest_summary.compaction_id if latest_summary is not None else None,
                source=source,
                emitted_chars=emitted_chars,
                semantic_detail=semantic_detail_for_record,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        _emit(f"reinject telemetry write failed: {exc}")

    return 0


def main() -> int:
    # Harness-agnostic interpreter self-heal (shared across all harnesses via
    # the single hook script). See scripts/hooks/_interp.py.
    from _interp import ensure_deps_interpreter

    ensure_deps_interpreter()
    repo_root = _git_repo_root()
    if not repo_root:
        _emit("reinject skipped: unable to resolve repo root")
        return 0
    from _envfile import load_embedding_env

    load_embedding_env(repo_root)
    _ensure_in_repo_sources_on_path(repo_root)

    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        _emit("reinject skipped: malformed stdin payload")
        return 0
    if not isinstance(data, dict):
        _emit("reinject skipped: stdin payload is not an object")
        return 0

    if not _payload_value(data, "session_id", "sessionId"):
        data["session_id"] = "unknown-session"

    # Cross-repo wire-shape contract: validate the SessionStart payload via
    # the shared helper. Strict mode escalates to SystemExit(2); lenient
    # mode logs and returns None. After a lenient validation failure we
    # exit 0 without injecting -- the payload cannot be trusted.
    try:
        from _protocol import validate_event  # type: ignore[import-not-found]
    except ImportError:
        validate_event = None  # type: ignore[assignment]

    if validate_event is not None:
        validated = validate_event(data, expected="SessionStart")
        if validated is None:
            _emit("reinject skipped: payload failed SessionStart schema validation")
            return 0

    # Source gate runs before any DB work so ordinary (non-enabled) session
    # starts stay cheap. Default excludes `startup` to avoid double-loading
    # next to load_session guidance.
    source = _payload_value(data, "source", "source").strip().lower()
    enabled = _enabled_sources()
    if source not in enabled:
        _emit(
            f"reinject skipped: source {source or '<unset>'!r} not enabled "
            f"(enabled: {','.join(enabled)})"
        )
        return 0

    try:
        budget_chars = _resolve_budget_chars()
    except ValueError as exc:
        _emit(f"reinject skipped: invalid budget: {exc}")
        return 0

    try:
        from workbay_handoff_mcp import CompactionSettings  # type: ignore[import-not-found]
    except ImportError as exc:
        _emit(f"reinject skipped: workbay_handoff_mcp import: {exc}")
        return 0

    try:
        settings = CompactionSettings.from_env()
    except Exception as exc:  # noqa: BLE001
        # Re-injection only consults compaction_notify; a malformed Stop-hook
        # tuning var (e.g. a typo'd MIN_NEW_TOKENS) must not silently disable
        # SessionStart context re-feeding. Fall back to defaults instead.
        _emit(f"reinject: invalid compaction settings, using defaults: {exc}")
        settings = CompactionSettings()

    session_id = _payload_value(data, "session_id", "sessionId") or "unknown-session"
    return _reinject(
        repo_root=repo_root,
        budget_chars=budget_chars,
        settings=settings,
        session_id=session_id,
        source=source,
    )


if __name__ == "__main__":
    raise SystemExit(main())
