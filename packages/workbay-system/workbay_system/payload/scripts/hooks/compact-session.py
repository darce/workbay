#!/usr/bin/env python3
"""Stop hook: persist a structured session_compactions row at turn-end.

internal (Option B). Reads the harness ``Stop`` event from
stdin, derives the active task from the workspace, and calls
``workbay_handoff_mcp.compact_session(...)`` against the transcript the
harness just closed. The new ``compaction_id`` is surfaced as the first
stderr line in a ``compaction_id=C-...`` envelope so the harness can keep
it in its retained summary; receipt value fields follow as stable
``key=value`` lines for operators that want the compression delta.

Failure-mode contract (must match implementation note of the internal task plan):

- Exit 0 on success after writing ``compaction_id=`` as the first stderr
    line, followed by receipt value ``key=value`` lines.
- Exit 0 also when there is nothing new to compact -- transcript head
  matches the latest stored compaction's ``turn_range``. Logs a
  ``compaction skipped: <reason>`` line and exits cleanly so the
  harness turn is not blocked.
- On any internal error (DB unreachable, transcript missing, no active
  task, validation failure), log a ``compaction failed: <reason>`` line
  and exit 0 *without* writing ``compaction_id=``. A failed compaction
  must never block the harness turn.

The single exception is strict-mode protocol drift
(``WORKBAY_HOOK_PROTOCOL_STRICT=1`` plus a malformed event payload):
``_protocol.validate_event`` raises ``SystemExit(2)`` and the hook
propagates it, matching every other wired hook.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from workbay_handoff_mcp import CompactionSettings

def _count_new_turn_tokens(text: str) -> int:
    """Encode ``text`` with ``cl100k_base`` and return token count.

    Falls back to whitespace word count when tiktoken is unavailable so
    the threshold gate is conservative (never silently skip a real
    compaction because the encoder import failed).
    """
    try:
        import tiktoken  # type: ignore[import-not-found]

        encoder = tiktoken.get_encoding("cl100k_base")
        return len(encoder.encode(text))
    except Exception:  # noqa: BLE001
        return len(text.split())


def _payload_value(payload: dict, snake_key: str, camel_key: str, default: str = "") -> str:
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
    return os.environ.get("CLAUDE_PROJECT_DIR", "")


def _prepend_handoff_sources(repo_root: str) -> None:
    """Pin under-test handoff sources ahead of any globally-installed copy.

    Order: explicit ``PYTHONPATH`` entries (test harness / operator override),
    then repo-relative ``packages/*/src``, then overlay/monorepo resolution via
    :func:`_resolve_agent_handoff_src`.
    """
    seen: set[str] = set()
    for entry in os.environ.get("PYTHONPATH", "").split(os.pathsep):
        if not entry or entry in seen:
            continue
        if os.path.isdir(os.path.join(entry, "workbay_handoff_mcp")):
            if entry not in sys.path:
                sys.path.insert(0, entry)
            seen.add(entry)
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


_COMPACTION_FAILED_SUMMARY_LIMIT = 256


def _resolve_agent_handoff_src(repo_root: str) -> str:
    from resolve_handoff_src import resolve_agent_handoff_src

    return resolve_agent_handoff_src(repo_root)


def _resolve_errors_record_argv() -> list[str]:
    console_script = shutil.which("mcp-workbay-handoff")
    if console_script:
        return [console_script, "errors-record"]
    return [sys.executable, "-m", "workbay_handoff_mcp", "errors-record"]


def _record_compaction_failed(failure_line: str) -> None:
    """Best-effort ``errors-record`` write for Stop-hook compaction failures."""
    summary = f"hook=Stop {failure_line}"[:_COMPACTION_FAILED_SUMMARY_LIMIT]
    argv = [
        *_resolve_errors_record_argv(),
        "--error-class",
        "compaction_failed",
        "--summary",
        summary,
        "--harness",
        _resolve_harness(),
    ]
    try:
        env = os.environ.copy()
        repo_root = _git_repo_root() or os.getcwd()
        src_path = _resolve_agent_handoff_src(repo_root)
        env["PYTHONPATH"] = src_path + (os.pathsep + env.get("PYTHONPATH", ""))
        subprocess.run(argv, capture_output=True, timeout=10, env=env, cwd=repo_root)
    except Exception:  # noqa: BLE001
        pass


def _emit(message: str) -> None:
    print(message, file=sys.stderr)
    if message.startswith("compaction failed:"):
        _record_compaction_failed(message)


_HARNESS_CHOICES = ("claude-code", "codex", "grok", "cursor", "manual")


def _env_alias(canonical: str, default: str | None = None) -> str | None:
    """Read a canonical ``WORKBAY_*`` override. The lazy ``_interp`` import keeps
    the bare-``python3`` module load (and the importlib-loaded test harness) free
    of a hooks-dir ``sys.path`` requirement; the shim resolves via
    ``workbay_protocol`` when importable, else a stdlib fallback."""
    from _interp import resolve_env_alias

    return resolve_env_alias(canonical, default=default)


def _resolve_harness() -> str:
    """Derive the harness label from WORKBAY_HANDOFF_HARNESS.

    The Stop hook is wired into multiple harnesses (Claude Code, Codex,
    Cursor, manual scripted runs); each launcher exports
    ``WORKBAY_HANDOFF_HARNESS`` so the compaction row reflects the actual
    caller. Unknown values are coerced to ``"manual"`` rather than
    rejected — a wrong-but-recognized label would silently mislabel
    rows. Defaults to ``"claude-code"`` only when the env is unset, to
    preserve the historical default for the harness this hook first
    shipped under. See internal.
    """
    raw = (_env_alias("WORKBAY_HANDOFF_HARNESS") or "").strip()
    if not raw:
        # Grok fallback (REV-E-010): grok delivers hooks via the
        # compat-loaded .claude/settings.json entry, which must not carry
        # an inline WORKBAY_HANDOFF_HARNESS export (internal -- it would
        # mislabel Claude rows). Grok exports GROK_WORKSPACE_ROOT for hook
        # commands (see _run_guard.py), so its presence identifies a grok
        # launcher when the explicit override is absent. Claude Code never
        # sets it.
        if os.environ.get("GROK_WORKSPACE_ROOT", "").strip():
            return "grok"
        return "claude-code"
    if raw in _HARNESS_CHOICES:
        return raw
    return "manual"


def _compact(
    *,
    transcript_path: str,
    session_id: str,
    repo_root: str,
    settings: CompactionSettings,
) -> int:
    """Drive ``compact_session`` against the resolved active task.

    Returns process exit code (always 0 by contract). ``settings`` is
    built once in ``main()``; this function never re-reads compaction env
    vars.
    """
    try:
        from workbay_handoff_mcp import (  # type: ignore[import-not-found]
            RuntimeConfig,
            configure_runtime,
            compact_session,
        )
        from workbay_handoff_mcp.compaction import (  # type: ignore[import-not-found]
            _derive_turn_range,
            _process_transcript_text,
            _read_raw_transcript,
            format_compaction_notify_message,
            format_compaction_record_receipt_lines,
            format_compaction_stop_notify_stdout,
            get_latest_compaction_for_session,
            slice_new_turn_transcript,
        )
        from workbay_protocol import TurnRange  # type: ignore[import-not-found]
        from workbay_handoff_mcp.shared_primitives import (  # type: ignore[import-not-found]
            resolve_active_task_ref_for_hook,
        )
        from workbay_handoff_mcp.shared_schema import (  # type: ignore[import-not-found]
            _get_db_connection,
        )
    except ImportError as exc:
        _emit(f"compaction failed: workbay_handoff_mcp import: {exc}")
        return 0

    # Honor WORKBAY_HANDOFF_STATE_DIR to mirror the MCP server CLI's
    # from_args resolution; production callers leave it unset and the
    # primary worktree's .task-state wins.
    state_dir_override = _env_alias("WORKBAY_HANDOFF_STATE_DIR") or None
    try:
        configure_runtime(
            RuntimeConfig.for_repo(Path(repo_root), state_dir=state_dir_override)
        )
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: runtime configuration: {exc}")
        return 0

    try:
        transcript = _read_raw_transcript(transcript_path, settings=settings)
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: transcript unreadable: {exc}")
        return 0

    try:
        with _get_db_connection() as conn:
            resolution = resolve_active_task_ref_for_hook(conn)
            task_ref = resolution.task_ref
            if resolution.tiebreak_note:
                _emit(resolution.tiebreak_note)
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: active task unresolved: {exc}")
        return 0

    try:
        latest = get_latest_compaction_for_session(task_ref, session_id)
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: latest compaction lookup: {exc}")
        return 0

    try:
        current_range = _derive_turn_range(transcript)
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: turn range derivation: {exc}")
        return 0

    # Stale-installed-package shim: pre-0.12.9 servers return the bare
    # StructuredSummary instead of a CompactionRecord wrapper. Fail open to
    # the old shape rather than crashing the Stop hook on the documented
    # installed-vs-payload version skew.
    latest_summary = getattr(latest, "summary", latest) if latest is not None else None

    if latest_summary is not None and latest_summary.session_id == session_id:
        # Only short-circuit when the *same* harness session is firing
        # the Stop hook again with no new turns. Different session_ids
        # (resumed sessions, new sessions, cross-harness handoffs) reset
        # transcript-local turn numbering, so a `current.end_turn <=
        # latest.end_turn` comparison would silently drop their
        # compaction. See internal.
        if current_range.end_turn <= latest_summary.turn_range.end_turn:
            _emit(
                "compaction skipped: no new turns since "
                f"{latest_summary.compaction_id}"
            )
            return 0
        prior_end_turn = latest_summary.turn_range.end_turn
        compaction_turn_range = TurnRange(
            start_turn=prior_end_turn + 1,
            end_turn=current_range.end_turn,
        )
    else:
        prior_end_turn = 0
        compaction_turn_range = current_range

    if settings.min_new_tokens == 0:
        _emit("compaction skipped: min_new_tokens gate disabled (override is 0)")
        return 0

    new_text = slice_new_turn_transcript(transcript, since_turn=prior_end_turn)
    if prior_end_turn > 0 and not new_text.strip():
        _emit("compaction skipped: no new turn text")
        return 0
    gate_text = _process_transcript_text(new_text, settings)
    new_token_count = _count_new_turn_tokens(gate_text)
    if new_token_count < settings.min_new_tokens:
        _emit(
            f"compaction skipped: only {new_token_count} new tokens; "
            f"threshold {settings.min_new_tokens}"
        )
        return 0

    try:
        receipt = compact_session(
            transcript_path=transcript_path,
            task_ref=task_ref,
            harness=_resolve_harness(),
            session_id=session_id,
            settings=settings,
            transcript_text=new_text,
            turn_range=compaction_turn_range,
        )
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: compact_session: {exc}")
        return 0

    for line in format_compaction_record_receipt_lines(receipt):
        _emit(line)

    if settings.compaction_notify:
        harness = _resolve_harness()
        notify_message = format_compaction_notify_message(receipt)
        if harness == "claude-code":
            print(format_compaction_stop_notify_stdout(notify_message), file=sys.stdout)

    return 0


def main() -> int:
    # Harness-agnostic interpreter self-heal (shared across all harnesses via
    # the single hook script). See scripts/hooks/_interp.py.
    from _interp import ensure_deps_interpreter

    ensure_deps_interpreter()
    repo_root = _git_repo_root()
    if not repo_root:
        _emit("compaction failed: unable to resolve repo root")
        return 0
    _prepend_handoff_sources(repo_root)

    # Single typed boundary for compaction env vars. Building settings
    # here (not inside ``_compact``) validates the WORKBAY_HANDOFF_COMPACTION_*
    # env vars once, so a bad value surfaces as one ``compaction failed`` line.
    try:
        from workbay_handoff_mcp import CompactionSettings  # type: ignore[import-not-found]
    except ImportError as exc:
        _emit(f"compaction failed: workbay_handoff_mcp import: {exc}")
        return 0

    try:
        settings = CompactionSettings.from_env(workspace_root=Path(repo_root))
    except Exception as exc:  # noqa: BLE001 -- pydantic ValidationError or env coercion
        _emit(f"compaction failed: invalid compaction settings: {exc}")
        return 0

    # internal: route both the Stop hook and the advisory through the
    # unified resolver so a single operator action turns off both surfaces.
    # The resolver consults env first, then a task-scoped
    # ``compaction_settings`` row, then the workspace-default row. Resolve
    # the active task before the check so per-task DB disables silence the
    # Stop hook as well as the advisory.
    try:
        from workbay_handoff_mcp import (  # type: ignore[import-not-found]
            RuntimeConfig,
            configure_runtime,
        )
        from workbay_handoff_mcp.compaction import (  # type: ignore[import-not-found]
            resolve_compaction_disabled,
        )
        from workbay_handoff_mcp.shared_primitives import (  # type: ignore[import-not-found]
            resolve_active_task_ref_for_hook,
        )
        from workbay_handoff_mcp.shared_schema import (  # type: ignore[import-not-found]
            _get_db_connection,
        )
    except ImportError as exc:
        _emit(f"compaction failed: workbay_handoff_mcp resolver import: {exc}")
        return 0

    state_dir_override = _env_alias("WORKBAY_HANDOFF_STATE_DIR") or None
    try:
        configure_runtime(
            RuntimeConfig.for_repo(Path(repo_root), state_dir=state_dir_override)
        )
    except Exception as exc:  # noqa: BLE001
        _emit(f"compaction failed: runtime configuration: {exc}")
        return 0

    try:
        with _get_db_connection() as conn:
            try:
                resolution = resolve_active_task_ref_for_hook(conn)
                resolved_task_ref = resolution.task_ref
            except Exception:  # noqa: BLE001 -- no active task falls through to workspace/env checks
                resolved_task_ref = None
            disabled, disabled_source = resolve_compaction_disabled(
                env=os.environ, conn=conn, task_ref=resolved_task_ref
            )
    except Exception as exc:  # noqa: BLE001 -- DB-open failure must not crash the hook
        _emit(f"compaction failed: resolver unreachable: {exc}")
        return 0

    if disabled:
        _emit(f"compaction skipped: disabled (source={disabled_source})")
        return 0

    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        _emit("compaction skipped: malformed stdin payload")
        return 0
    if not isinstance(data, dict):
        _emit("compaction skipped: stdin payload is not an object")
        return 0

    # Cross-repo wire-shape contract: validate the Stop payload via the
    # shared helper. Strict mode escalates to SystemExit(2); lenient
    # mode logs and returns None. After a lenient validation failure we
    # exit 0 without compacting -- the payload cannot be trusted.
    try:
        from _protocol import validate_event  # type: ignore[import-not-found]
    except ImportError:
        validate_event = None  # type: ignore[assignment]

    if validate_event is not None:
        validated = validate_event(data, expected="Stop")
        if validated is None:
            # Lenient mode swallowed a validation error; do not compact.
            # Strict mode would already have raised SystemExit(2).
            _emit("compaction skipped: payload failed Stop schema validation")
            return 0

    transcript_path = _payload_value(data, "transcript_path", "transcriptPath")
    session_id = _payload_value(data, "session_id", "sessionId")

    if not session_id:
        _emit("compaction skipped: missing session_id")
        return 0
    if not transcript_path:
        _emit("compaction skipped: missing transcript_path")
        return 0

    return _compact(
        transcript_path=transcript_path,
        session_id=session_id,
        repo_root=repo_root,
        settings=settings,
    )


if __name__ == "__main__":
    raise SystemExit(main())
