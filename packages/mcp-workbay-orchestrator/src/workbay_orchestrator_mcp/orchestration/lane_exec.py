#!/usr/bin/env python3
"""Non-reporting lane execution primitive: render prompt, run Codex, write structured result.

This module is the reusable inner step for both human ``make lane-run`` and the
autonomous worker daemon.  It does NOT record anything to MCP or trigger handoff
side-effects -- callers decide what to do with the result file.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _env import WORKER_REASONING_EFFORT_CHOICES, apply_backend_runtime_hints, pythonpath_env
from backend_registry import get_adapter, get_backend_choices, validate_backend
from bootstrap_lane import _bootstrap as bootstrap_lane
from lane_manifest import get_lane_config

BACKEND_CHOICES = get_backend_choices()


def _load_artifact_index_runtime() -> tuple[Any, Any] | None:
    if importlib.util.find_spec("workbay_handoff_mcp") is None:
        return None
    from workbay_handoff_mcp import artifact_index  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    return artifact_index, RuntimeConfig


# ---------------------------------------------------------------------------
# Prompt / schema helpers
# ---------------------------------------------------------------------------


def _render_prompt(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
) -> tuple[str, dict[str, Any]]:
    """Call ``lane_prompt.py`` and return ``(rendered_prompt, context_utilization_metrics)``."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "lane_prompt.py"),
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
        "--lane-id",
        lane_id,
        "--worktree-path",
        str(worktree_path),
    ]
    env = pythonpath_env(orchestrator_root, task_ref=task_ref, lane_id=lane_id)
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"lane_prompt.py failed (exit {completed.returncode}):\n{completed.stderr.strip()}")
    ctx_metrics: dict[str, Any] = {}
    for line in completed.stderr.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict) and "context_utilization" in parsed:
                ctx_metrics = parsed["context_utilization"]
                break
        except (json.JSONDecodeError, ValueError):
            pass
    return completed.stdout, ctx_metrics


_VALID_HANDOFF_ACTIONS = ("merge_ready", "needs_guidance")


def _validate_lane_result_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate a structured lane-worker payload against the lane-result contract.

    Mirrors the JSON Schema in ``lane_result._schema()``. Used by the
    workbay-codex-bridge contract test to assert that bridge-produced
    payloads still satisfy what the orchestrator's lane-result pipeline
    expects, without requiring jsonschema as a runtime dependency.
    """
    required = ("handoff_action", "summary", "details", "tests_run", "blockers")
    for key in required:
        if key not in payload:
            raise RuntimeError(f"Lane result missing required key '{key}'.")
    if payload["handoff_action"] not in _VALID_HANDOFF_ACTIONS:
        raise RuntimeError(
            f"Lane result 'handoff_action' must be one of {list(_VALID_HANDOFF_ACTIONS)}, "
            f"got {payload['handoff_action']!r}."
        )
    for text_key in ("summary", "details"):
        value = payload[text_key]
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Lane result '{text_key}' must be a non-empty string.")
    for list_key in ("tests_run", "blockers"):
        value = payload[list_key]
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise RuntimeError(f"Lane result '{list_key}' must be a list of strings.")
    return payload


def _render_schema(orchestrator_root: Path) -> str:
    """Call ``lane_result.py schema`` and return the JSON string."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "lane_result.py"),
        "schema",
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"lane_result.py schema failed (exit {completed.returncode}):\n{completed.stderr.strip()}")
    return completed.stdout


_HANDOFF_INSTRUCTIONS = """
When you finish, do not run `make lane-handoff` or `make lane-report` yourself.
Return a single JSON object that matches the provided output schema.

Set `handoff_action` to:
- `merge_ready` only if you produced lane-owned code changes that are ready for orchestrator review
- `needs_guidance` if you were blocked, verification was blocked, permissions/sandbox prevented progress, or the assigned issue already appears resolved and now needs orchestrator review instead of new lane code
"""


# ---------------------------------------------------------------------------
# Prompt augmentation for fix cycles
# ---------------------------------------------------------------------------


def build_fix_prompt(base_prompt: str, findings: list[dict[str, Any]]) -> str:
    """Augment a base lane prompt with review findings for a fix cycle."""
    if not findings:
        return base_prompt

    lines = [base_prompt.rstrip(), "", "--- REVIEW FINDINGS TO FIX ---", ""]
    for f in findings:
        severity = f.get("severity", "unknown").upper()
        category = f.get("category", "")
        file_path = f.get("file_path", "")
        desc = f.get("description", "")
        fix = f.get("fix", "")
        loc = file_path
        if isinstance(f.get("line_start"), int):
            loc = f"{file_path}:{f['line_start']}"
        lines.append(f"- [{severity}] [{category}] {loc}: {desc}")
        if fix:
            lines.append(f"  Fix: {fix}")

    lines.extend(
        [
            "",
            "Address each finding above. After fixing, return a JSON result per the output schema.",
        ]
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Subprocess execution with heartbeats
# ---------------------------------------------------------------------------


def _tail_text(text: str | bytes, *, limit: int = 240) -> str:
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    value = " ".join(line.strip() for line in text.splitlines() if line.strip())
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _run_lane_preflight(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
    env: dict[str, str],
) -> dict[str, Any]:
    try:
        lane_config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root)) or {}
    except FileNotFoundError:
        return {"ok": True, "commands": [], "capability_tags": []}
    commands = [str(item).strip() for item in lane_config.get("preflight_commands", []) if str(item).strip()]
    capability_tags = [str(item).strip() for item in lane_config.get("capability_tags", []) if str(item).strip()]
    if not commands:
        return {"ok": True, "commands": [], "capability_tags": capability_tags}

    failures: list[dict[str, Any]] = []
    for command in commands:
        completed = subprocess.run(
            ["/bin/bash", "-lc", command],
            cwd=worktree_path,
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        if completed.returncode == 0:
            continue
        failures.append(
            {
                "command": command,
                "exit_code": completed.returncode,
                "stderr_tail": _tail_text(completed.stderr or ""),
                "stdout_tail": _tail_text(completed.stdout or ""),
            }
        )

    return {
        "ok": not failures,
        "commands": commands,
        "capability_tags": capability_tags,
        "failures": failures,
        "failure_summary": str(lane_config.get("preflight_failure_summary") or "").strip(),
        "failure_details": str(lane_config.get("preflight_failure_details") or "").strip(),
    }


def _preflight_failure_payload(
    *,
    lane_id: str,
    preflight: dict[str, Any],
) -> dict[str, Any]:
    commands = [str(item) for item in preflight.get("commands", []) if str(item).strip()]
    capability_tags = [str(item) for item in preflight.get("capability_tags", []) if str(item).strip()]
    failures = [item for item in preflight.get("failures", []) if isinstance(item, dict)]
    summary = str(preflight.get("failure_summary") or "").strip()
    if not summary:
        summary = f"Lane preflight failed for {lane_id}; required local capabilities are unavailable."

    default_detail = (
        f"The lane requires local capabilities ({', '.join(capability_tags)}) before execution."
        if capability_tags
        else "The lane requires local prerequisites before execution."
    )
    detail_lines = [str(preflight.get("failure_details") or "").strip() or default_detail, "", "Preflight failures:"]
    blockers: list[str] = []
    for failure in failures:
        command = str(failure.get("command") or "").strip()
        stderr_tail = str(failure.get("stderr_tail") or "").strip()
        stdout_tail = str(failure.get("stdout_tail") or "").strip()
        exit_code = failure.get("exit_code")
        reason = stderr_tail or stdout_tail or f"exit {exit_code}"
        detail_lines.append(f"- `{command}` -> {reason}")
        blockers.append(f"`{command}` failed: {reason}")

    if not blockers:
        blockers.append("Lane preflight failed before execution.")

    return {
        "handoff_action": "needs_guidance",
        "summary": summary,
        "details": "\n".join(detail_lines).strip(),
        "tests_run": commands,
        "blockers": blockers,
    }


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Scope-violation helpers
# ---------------------------------------------------------------------------


def _matches_any_owned_path(file_path: str, owned_paths: list[str]) -> bool:
    """Return True if file_path falls under one of the owned glob patterns."""
    import fnmatch

    normalized = file_path.lstrip("/")
    for pattern in owned_paths:
        pat = pattern.rstrip("/")
        # Direct prefix match (handles "apps/foo/**" style)
        stripped = pat.rstrip("*").rstrip("/")
        prefix = stripped.lstrip("/")
        if prefix and (normalized.startswith(prefix + "/") or normalized == prefix):
            return True
        if fnmatch.fnmatch(normalized, pat):
            return True
    return False


def _check_scope_violations(
    worktree_path: Path,
    owned_paths: list[str],
) -> list[str]:
    """Return paths that were modified outside the lane's owned_paths.

    Uses ``git diff --name-only`` (staged + unstaged) and ``git ls-files --others``
    so newly created untracked files are also checked.
    """
    if not owned_paths:
        # No owned_paths defined; skip violation check.
        return []
    changed: list[str] = []
    for git_args in (
        ["git", "-C", str(worktree_path), "diff", "--name-only", "HEAD"],
        ["git", "-C", str(worktree_path), "ls-files", "--others", "--exclude-standard"],
    ):
        try:
            out = subprocess.run(git_args, capture_output=True, text=True, check=False, timeout=15)
            for line in (out.stdout or "").splitlines():
                f = line.strip()
                if f:
                    changed.append(f)
        except (subprocess.TimeoutExpired, OSError):
            pass
    violations = [f for f in sorted(set(changed)) if not _matches_any_owned_path(f, owned_paths)]
    return violations


def _get_effective_owned_paths(
    task_ref: str,
    lane_id: str,
    owned_paths: list[str],
    orchestrator_root: Path,
) -> list[str]:
    """Return owned_paths, optionally overridden by an MCP lane message artifact.

    Looks for the most recent ``orchestrator_to_worker`` lane message that carries
    an ``owned_paths_override`` artifact.  Falls back to the manifest list.
    """
    try:
        import subprocess as _sp

        cmd = [
            sys.executable,
            str(SCRIPT_DIR / "lane_activity.py"),
            "--orchestrator-root",
            str(orchestrator_root),
            "--task-ref",
            task_ref,
            "--lane-id",
            lane_id,
            "--format",
            "json",
        ]
        result = _sp.run(cmd, capture_output=True, text=True, check=False, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            activity = json.loads(result.stdout)
            messages = activity.get("messages") or []
            for msg in reversed(messages):
                if not isinstance(msg, dict):
                    continue
                if msg.get("direction") != "orchestrator_to_worker":
                    continue
                payload = msg.get("payload")
                if isinstance(payload, dict):
                    override = payload.get("owned_paths_override")
                    if isinstance(override, list) and override:
                        return [str(p) for p in override if str(p).strip()]
                artifacts = msg.get("artifacts")
                if isinstance(artifacts, list):
                    for art in artifacts:
                        # MCP normalizer preserves string items; parse JSON-encoded overrides
                        if isinstance(art, str):
                            try:
                                art = json.loads(art)
                            except (json.JSONDecodeError, ValueError):
                                continue
                        if isinstance(art, dict) and art.get("type") == "owned_paths_override":
                            paths = art.get("paths")
                            if isinstance(paths, list) and paths:
                                return [str(p) for p in paths if str(p).strip()]
    except Exception:  # noqa: BLE001
        pass
    return owned_paths


# Artifact helpers
# ---------------------------------------------------------------------------

_ARTIFACT_INLINE_CHARS = 500


def _compress_large_result_details(
    result_data: dict[str, Any],
    *,
    task_ref: str,
    lane_id: str,
    orchestrator_root: Path,
) -> dict[str, Any]:
    """Index large ``details`` fields and replace inline body with an artifact ref.

    When a result's ``details`` field exceeds the threshold bytes, the full text
    is indexed into the sidecar FTS5 cache and the inline body is replaced with a
    compact excerpt plus a ``details_artifact_ref`` field so callers can retrieve
    the full content on demand.  Non-fatal; returns the original dict on any error.
    """
    artifact_runtime = _load_artifact_index_runtime()
    if artifact_runtime is None:
        return result_data
    artifact_index, artifact_runtime_config = artifact_runtime
    details = result_data.get("details") or ""
    if not isinstance(details, str):
        return result_data
    try:
        art_config = artifact_runtime_config.for_repo(orchestrator_root)
        artifact_db_path = art_config.artifact_db_path
        min_bytes = art_config.artifact_index_min_bytes
        min_lines = art_config.artifact_index_min_lines
    except Exception:  # noqa: BLE001
        artifact_db_path = orchestrator_root / ".task-state" / "mcp-artifacts.db"
        min_bytes = 4096
        min_lines = 80
    if len(details.encode("utf-8")) < min_bytes:
        return result_data
    source_label = f"{lane_id}-exec-details"
    try:
        index_result = artifact_index.maybe_record_artifact(
            task_ref=task_ref,
            lane_id=lane_id,
            app_root=None,
            source_kind="execution-output",
            source_label=source_label,
            content_type="text/plain",
            summary=f"Execution details for lane {lane_id}",
            content=details,
            artifact_db_path=artifact_db_path,
            min_bytes=min_bytes,
            min_lines=min_lines,
        )
        if index_result is not None:
            source_id = index_result["source_id"]
            inline = details[:_ARTIFACT_INLINE_CHARS].rstrip()
            if len(details) > _ARTIFACT_INLINE_CHARS:
                inline += f"\n... [truncated — full output indexed as artifact:{source_id}]"
            return {**result_data, "details": inline, "details_artifact_ref": source_id}
    except Exception:  # noqa: BLE001
        pass
    return result_data


# Temp-file helpers
# ---------------------------------------------------------------------------


def _temp_output_path(*, lane_id: str) -> Path:
    fd, name = tempfile.mkstemp(suffix=".json", prefix=f"lane-exec-{lane_id}-")
    os.close(fd)
    return Path(name)


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------


def run_lane_exec(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    output_path: Path | None = None,
    backend: str = "codex-cli",
    session_mode: str = "fresh_turn",
    reasoning_effort: str | None = None,
    model: str | None = None,
    codex_bin: str | None = None,
    codex_args: list[str] | None = None,
    grok_bin: str | None = None,
    grok_args: list[str] | None = None,
    prompt_override: str | None = None,
    heartbeat_interval: int = 20,
    progress_callback: Callable[..., None] | None = None,
    dry_run: bool = False,
) -> Path:
    """Run Codex for a lane and write a structured result file.

    Returns the path to the result JSON file.  Does NOT trigger any MCP
    recording or handoff side-effects.
    """
    # 1. Load manifest for overrides
    lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root)) or {}

    # Priority: CLI > Manifest > Default
    # If caller provided a non-default backend, keep it.
    # Otherwise, check manifest.
    backend_name = backend
    if backend_name == "codex-cli" and lane_cfg.get("preferred_backend"):
        backend_name = str(lane_cfg["preferred_backend"])
    backend_name = validate_backend(backend_name)

    model_name = model or lane_cfg.get("preferred_model")

    env = pythonpath_env(orchestrator_root, task_ref=task_ref, lane_id=lane_id)
    apply_backend_runtime_hints(
        env,
        reasoning_effort=reasoning_effort,
        model=model_name,
        session_mode=session_mode,
        backend=backend_name,
    )

    if not dry_run:
        bootstrap_result = bootstrap_lane(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=worktree_path,
            backend=backend_name,
            # Thread the effective model so the grok config-env attribution
            # identity is derived from the ACTUAL model, not a hardcoded default
            # (s5-a-006 / s6-a-003).
            model=model_name,
        )
        if bootstrap_result != 0:
            raise RuntimeError(f"lane bootstrap failed for {lane_id} with exit code {bootstrap_result}")

        preflight = _run_lane_preflight(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=worktree_path,
            env=env,
        )
        if not preflight.get("ok", True):
            out = output_path or _temp_output_path(lane_id=lane_id)
            out.write_text(json.dumps(_preflight_failure_payload(lane_id=lane_id, preflight=preflight), indent=2))
            return out

    # Build prompt
    ctx_metrics: dict[str, Any] = {}
    if prompt_override:
        prompt_text = prompt_override
    else:
        prompt_text, ctx_metrics = _render_prompt(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=worktree_path,
        )
    prompt_text += _HANDOFF_INSTRUCTIONS

    # Build schema
    schema_text = _render_schema(orchestrator_root)

    if dry_run:
        result: dict[str, Any] = {
            "dry_run": True,
            "backend": backend_name,
            "model": model_name,
            "reasoning_effort": reasoning_effort,
            "handoff_action": "merge_ready",
            "summary": f"Dry-run lane execution for {lane_id}.",
            "details": "No codex exec was run; this is a simulated structured result.",
            "tests_run": [],
            "blockers": [],
            "prompt": prompt_text,
            "schema": json.loads(schema_text),
        }
        out = output_path or _temp_output_path(lane_id=lane_id)
        out.write_text(json.dumps(result, indent=2))
        return out

    # Get adapter and execute. Adapter ctor kwargs are per-backend: passing
    # codex_bin/codex_args to a non-codex CLI adapter (e.g. claude-code, grok-cli)
    # is a TypeError at construction (§0 latent bug), so route by backend.
    adapter_kwargs: dict[str, Any] = {}
    if backend_name == "codex-cli":
        adapter_kwargs = {"codex_bin": codex_bin, "codex_args": codex_args}
    elif backend_name == "grok-cli":
        adapter_kwargs = {"grok_bin": grok_bin, "grok_args": grok_args}
    adapter = get_adapter(backend_name, **adapter_kwargs)

    result = adapter.execute(
        prompt=prompt_text,
        schema=json.loads(schema_text),
        worktree_path=worktree_path,
        model=model_name,
        reasoning_effort=reasoning_effort,
        session_mode=session_mode,
        env=env,
        heartbeat_interval=heartbeat_interval,
        progress_callback=progress_callback,
    )

    out = output_path or _temp_output_path(lane_id=lane_id)
    result_payload = result.to_dict() if hasattr(result, "to_dict") else result
    out.write_text(json.dumps(result_payload, indent=2))

    # Patch result JSON with context_utilization and scope violations in one read-write pass
    result_data = json.loads(out.read_text())
    if ctx_metrics:
        result_data["context_utilization"] = ctx_metrics
    # Scope violation check: flag files modified outside owned_paths
    owned_paths_manifest = [str(item) for item in lane_cfg.get("owned_paths", []) if str(item).strip()]
    effective_paths = _get_effective_owned_paths(
        task_ref,
        lane_id,
        owned_paths=owned_paths_manifest,
        orchestrator_root=orchestrator_root,
    )
    violations = _check_scope_violations(worktree_path, owned_paths=effective_paths)
    if violations:
        result_data["scope_violation"] = True
        result_data["scope_violations"] = violations
    # Compress large details fields: index via FTS5 sidecar and replace inline body
    result_data = _compress_large_result_details(
        result_data,
        task_ref=task_ref,
        lane_id=lane_id,
        orchestrator_root=orchestrator_root,
    )
    if ctx_metrics or violations or "details_artifact_ref" in result_data:
        out.write_text(json.dumps(result_data, indent=2))

    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Non-reporting lane execution: render prompt, run Codex, write result."
    )
    parser.add_argument("--orchestrator-root", required=True)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--worktree-path", required=True)
    parser.add_argument("--output-path", help="Where to write the result JSON. Defaults to a temp file.")
    parser.add_argument(
        "--backend", default="codex-cli", choices=BACKEND_CHOICES, help="Execution backend to use (default: codex-cli)."
    )
    parser.add_argument(
        "--reasoning-effort",
        choices=WORKER_REASONING_EFFORT_CHOICES,
        help="Optional reasoning effort hint for codex-subagent turns.",
    )
    parser.add_argument("--model", help="Explicit model to use (e.g. gpt-5.4-mini).")
    parser.add_argument("--codex-bin", help="Explicit path to the codex binary.")
    parser.add_argument("--codex-args", help="Extra args for codex exec (space-separated).")
    parser.add_argument("--grok-bin", help="Explicit path to the grok binary.")
    parser.add_argument("--grok-args", help="Extra args for grok exec (space-separated).")
    parser.add_argument("--prompt-file", help="Override the lane prompt with contents of this file.")
    parser.add_argument("--dry-run", action="store_true", help="Print prompt/schema without running Codex.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    worktree_path = Path(args.worktree_path).expanduser().resolve()
    output_path = Path(args.output_path).expanduser().resolve() if args.output_path else None
    codex_args = args.codex_args.split() if args.codex_args else None
    grok_args = args.grok_args.split() if args.grok_args else None

    prompt_override = None
    if args.prompt_file:
        prompt_override = Path(args.prompt_file).expanduser().resolve().read_text()

    result_path = run_lane_exec(
        orchestrator_root=orchestrator_root,
        task_ref=args.task_ref,
        lane_id=args.lane_id,
        session=args.session,
        worktree_path=worktree_path,
        output_path=output_path,
        backend=args.backend,
        reasoning_effort=args.reasoning_effort,
        model=args.model,
        codex_bin=args.codex_bin,
        codex_args=codex_args,
        grok_bin=args.grok_bin,
        grok_args=grok_args,
        prompt_override=prompt_override,
        dry_run=args.dry_run,
    )

    print(json.dumps({"ok": True, "result_path": str(result_path)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
