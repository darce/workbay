#!/usr/bin/env python3
"""Stdlib-only agy plugin hook boundary adapter.

Bundled into ``effective/agy/hooks/run.py`` by ``generate_agent_workflows.py``.
``agy`` executes plugin hooks with OS ``cwd == <plugin_dir>`` as::

    python3 ./hooks/run.py <hook-script-name> [args...]

This adapter reads camelCase stdin JSON from ``agy``, resolves the workspace
root from ``cwd`` or ``workspacePaths[0]``, ``os.chdir``s into it, sets
``WORKBAY_HARNESS=agy-cli``, normalizes stdin into WorkBay's canonical snake_case
hook payload, invokes ``<workspace>/scripts/hooks/<hook>`` with the active
workspace Python (``.venv/bin/python`` when present else ``sys.executable``),
and translates WorkBay hook outputs into ``agy`` stdout JSON.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

_DEFAULT_TIMEOUT_SECONDS = 30
_TOOL_NAME_MAP: dict[str, str] = {
    "grep_search": "Grep",
    "find_by_name": "Glob",
}


def resolve_workspace_root(payload: dict[str, Any]) -> Path:
    """Resolve the workspace root from ``agy`` stdin payload."""
    cwd_value = payload.get("cwd")
    if isinstance(cwd_value, str) and cwd_value.strip():
        candidate = Path(cwd_value).expanduser()
        if candidate.is_dir():
            return candidate.resolve()

    workspace_paths = payload.get("workspacePaths")
    if isinstance(workspace_paths, list):
        for entry in workspace_paths:
            if isinstance(entry, str) and entry.strip():
                candidate = Path(entry).expanduser()
                if candidate.is_dir():
                    return candidate.resolve()

    if isinstance(cwd_value, str) and cwd_value.strip():
        return Path(cwd_value).expanduser().resolve()
    return Path.cwd().resolve()


def resolve_workspace_python(workspace_root: Path) -> str:
    """Prefer workspace ``.venv/bin/python`` when executable; else ``sys.executable``."""
    venv_python = workspace_root / ".venv" / "bin" / "python"
    if venv_python.is_file() and os.access(venv_python, os.X_OK):
        return str(venv_python)
    return sys.executable


def resolve_hook_script(workspace_root: Path, hook_name: str) -> Path:
    """Resolve ``<hook_name>`` strictly under ``<workspace>/scripts/hooks/``."""
    cleaned = hook_name.strip()
    if not cleaned or "/" in cleaned or "\\" in cleaned or cleaned in {".", ".."}:
        raise ValueError(f"invalid hook script name: {hook_name!r}")

    hooks_dir = (workspace_root / "scripts" / "hooks").resolve()
    candidates = [hooks_dir / cleaned]
    if not cleaned.endswith((".py", ".sh")):
        candidates.append(hooks_dir / f"{cleaned}.py")

    for candidate in candidates:
        resolved = candidate.resolve()
        try:
            resolved.relative_to(hooks_dir)
        except ValueError as exc:
            raise ValueError(f"hook script escapes scripts/hooks: {hook_name!r}") from exc
        if resolved.is_file():
            return resolved

    raise FileNotFoundError(f"hook script not found under {hooks_dir}: {hook_name!r}")


def _normalize_tool_input(
    raw_args: Any, tool_name: str | None = None
) -> dict[str, Any]:
    if not isinstance(raw_args, dict):
        return {}
    normalized = dict(raw_args)
    if "command" not in normalized and isinstance(raw_args.get("CommandLine"), str):
        normalized["command"] = raw_args["CommandLine"]
    if "file_path" not in normalized:
        for key in ("Path", "TargetFile", "AbsolutePath"):
            value = raw_args.get(key)
            if isinstance(value, str) and value.strip():
                normalized["file_path"] = value
                break
    if tool_name in {"Grep", "Glob", "grep_search", "find_by_name"}:
        if "pattern" not in normalized:
            for key in ("Pattern", "Query", "query"):
                value = raw_args.get(key)
                if isinstance(value, str) and value.strip():
                    normalized["pattern"] = value
                    break
            else:
                exts = raw_args.get("Extensions") or raw_args.get("extensions")
                if isinstance(exts, list):
                    clean_exts = [
                        e.strip().lstrip(".")
                        for e in exts
                        if isinstance(e, str) and e.strip().lstrip(".")
                    ]
                    if clean_exts:
                        normalized["pattern"] = f"*.{clean_exts[0]}"
        if "path" not in normalized:
            for key in (
                "SearchPath",
                "searchPath",
                "SearchDirectory",
                "searchDirectory",
                "Path",
                "AbsolutePath",
            ):
                value = raw_args.get(key)
                if isinstance(value, str) and value.strip():
                    normalized["path"] = value
                    break
        if "glob" not in normalized and "include" not in normalized:
            for key in ("Glob", "Include"):
                value = raw_args.get(key)
                if isinstance(value, str) and value.strip():
                    normalized["glob"] = value
                    break
            else:
                includes = raw_args.get("Includes") or raw_args.get("includes")
                if isinstance(includes, list):
                    clean_includes = [
                        item.strip()
                        for item in includes
                        if isinstance(item, str) and item.strip()
                    ]
                    if clean_includes:
                        normalized["glob"] = (
                            clean_includes[0]
                            if len(clean_includes) == 1
                            else ",".join(clean_includes)
                        )
    return normalized


def normalize_stdin_payload(
    payload: dict[str, Any], workspace_root: Path
) -> dict[str, Any]:
    """Translate ``agy`` camelCase stdin JSON into WorkBay's canonical hook payload."""
    canonical: dict[str, Any] = {
        "cwd": str(workspace_root),
        "harness": "agy-cli",
    }

    if isinstance(payload.get("conversationId"), str):
        canonical["session_id"] = payload["conversationId"]
    if isinstance(payload.get("transcriptPath"), str):
        canonical["transcript_path"] = payload["transcriptPath"]
    if isinstance(payload.get("artifactDirectoryPath"), str):
        canonical["artifact_directory_path"] = payload["artifactDirectoryPath"]
    if isinstance(payload.get("modelName"), str):
        canonical["model_name"] = payload["modelName"]
    if isinstance(payload.get("lastUserInput"), str):
        canonical["prompt"] = payload["lastUserInput"]
        canonical["last_user_input"] = payload["lastUserInput"]
    if isinstance(payload.get("hookEventName"), str):
        canonical["hook_event_name"] = payload["hookEventName"]
    if isinstance(payload.get("workspacePaths"), list):
        canonical["workspace_paths"] = list(payload["workspacePaths"])
    if isinstance(payload.get("continuationActive"), bool):
        canonical["stop_hook_active"] = payload["continuationActive"]
        canonical["continuation_active"] = payload["continuationActive"]
    if isinstance(payload.get("terminationReason"), str):
        canonical["termination_reason"] = payload["terminationReason"]

    tool_call = payload.get("toolCall")
    if isinstance(tool_call, dict):
        if isinstance(tool_call.get("id"), str):
            canonical["tool_use_id"] = tool_call["id"]
        raw_tool_name = tool_call.get("name")
        normalized_tool_name: str | None = None
        if isinstance(raw_tool_name, str):
            normalized_tool_name = _TOOL_NAME_MAP.get(raw_tool_name, raw_tool_name)
            canonical["tool_name"] = normalized_tool_name
        canonical["tool_input"] = _normalize_tool_input(
            tool_call.get("args"), tool_name=normalized_tool_name
        )

    return canonical


def _parse_json_output(stdout: str) -> dict[str, Any] | None:
    text = stdout.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_reason(
    parsed: dict[str, Any] | None, stdout: str, stderr: str, default: str
) -> str:
    if isinstance(parsed, dict):
        hso = parsed.get("hookSpecificOutput")
        if isinstance(hso, dict):
            for key in ("permissionDecisionReason", "reason", "additionalContext"):
                val = hso.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
        for key in ("reason", "permissionDecisionReason", "stopReason", "systemMessage", "errorMessage"):
            val = parsed.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    if stderr.strip():
        return stderr.strip()
    if stdout.strip():
        return stdout.strip()
    return default


def _is_explicit_deny(parsed: dict[str, Any] | None) -> bool:
    if not isinstance(parsed, dict):
        return False
    decision = str(parsed.get("decision") or "").lower()
    if decision in {"deny", "block"}:
        return True
    if parsed.get("continue") is False:
        return True
    perm = str(parsed.get("permissionDecision") or "").lower()
    if perm in {"deny", "block"}:
        return True
    hso = parsed.get("hookSpecificOutput")
    if isinstance(hso, dict):
        hso_perm = str(hso.get("permissionDecision") or "").lower()
        if hso_perm in {"deny", "block"}:
            return True
    return False


def _extract_injected_text(parsed: dict[str, Any] | None, stdout: str) -> str | None:
    if isinstance(parsed, dict):
        chunks: list[str] = []
        hso = parsed.get("hookSpecificOutput")
        if isinstance(hso, dict):
            add_ctx = hso.get("additionalContext")
            if isinstance(add_ctx, str) and add_ctx.strip():
                chunks.append(add_ctx.strip())
        for key in ("additionalContext", "systemMessage"):
            val = parsed.get(key)
            if isinstance(val, str) and val.strip() and val.strip() not in chunks:
                chunks.append(val.strip())
        return "\n\n".join(chunks) if chunks else None
    text = stdout.strip()
    return text if text else None


def translate_hook_result(
    event_name: str,
    returncode: int,
    stdout: str,
    stderr: str,
    *,
    continuation_active: bool = False,
) -> dict[str, Any]:
    """Translate WorkBay hook exit/stdout/stderr into ``agy`` hook output JSON."""
    parsed = _parse_json_output(stdout)

    if event_name == "PostToolUse":
        return {}

    if event_name == "Stop":
        if continuation_active:
            return {}
        if returncode == 2 or _is_explicit_deny(parsed):
            reason = _extract_reason(
                parsed, stdout, stderr, "WorkBay Stop hook requested continuation"
            )
            return {"decision": "continue", "reason": reason}
        return {}

    if event_name in {"SessionStart", "PreInvocation"}:
        if returncode == 2 or _is_explicit_deny(parsed):
            reason = _extract_reason(
                parsed, stdout, stderr, f"WorkBay {event_name} hook blocked execution"
            )
            return {
                "errorMessage": reason,
                "terminationBehavior": "terminate",
            }
        if returncode != 0:
            return {}
        injected = _extract_injected_text(parsed, stdout)
        if injected:
            return {"injectSteps": [{"systemMessage": {"systemMessage": injected}}]}
        return {}

    # Default / PreToolUse: fail closed on guard block or error (SECD-05).
    if returncode != 0 or _is_explicit_deny(parsed):
        reason = _extract_reason(
            parsed,
            stdout,
            stderr,
            f"WorkBay PreToolUse guard denied tool call (exit {returncode})",
        )
        return {"decision": "deny", "reason": reason}

    return {"decision": "allow"}


def run_adapter(argv: list[str], stdin_text: str) -> dict[str, Any]:
    """Execute the target WorkBay hook and return the ``agy`` JSON response."""
    if not argv:
        return {
            "decision": "deny",
            "reason": "agy hook adapter requires a hook script name argument",
        }

    hook_name = argv[0]
    extra_args = argv[1:]

    try:
        raw_payload = json.loads(stdin_text) if stdin_text.strip() else {}
    except json.JSONDecodeError as exc:
        return {
            "decision": "deny",
            "reason": f"agy hook adapter received invalid stdin JSON: {exc}",
        }
    if not isinstance(raw_payload, dict):
        raw_payload = {}

    event_name = str(raw_payload.get("hookEventName") or "PreToolUse")
    continuation_active = bool(raw_payload.get("continuationActive") is True)

    if event_name == "Stop" and continuation_active:
        return {}

    workspace_root = resolve_workspace_root(raw_payload)
    try:
        script_path = resolve_hook_script(workspace_root, hook_name)
    except (FileNotFoundError, ValueError) as exc:
        if event_name == "PreToolUse":
            return {"decision": "deny", "reason": str(exc)}
        return {}

    canonical_payload = normalize_stdin_payload(raw_payload, workspace_root)
    env = os.environ.copy()
    env["WORKBAY_HARNESS"] = "agy-cli"
    env.setdefault("WORKBAY_HANDOFF_HARNESS", "agy-cli")
    if isinstance(canonical_payload.get("transcript_path"), str):
        env.setdefault(
            "AGY_SESSION_TRANSCRIPT_PATH", canonical_payload["transcript_path"]
        )

    if script_path.suffix == ".sh":
        cmd = ["bash", str(script_path), *extra_args]
    else:
        cmd = [resolve_workspace_python(workspace_root), str(script_path), *extra_args]

    if workspace_root.is_dir():
        os.chdir(workspace_root)

    try:
        proc = subprocess.run(
            cmd,
            input=json.dumps(canonical_payload),
            text=True,
            capture_output=True,
            cwd=str(workspace_root),
            env=env,
            timeout=_DEFAULT_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        if event_name == "PreToolUse":
            return {
                "decision": "deny",
                "reason": f"agy hook adapter failed executing {hook_name}: {exc}",
            }
        return {}

    return translate_hook_result(
        event_name,
        proc.returncode,
        proc.stdout,
        proc.stderr,
        continuation_active=continuation_active,
    )


def main() -> int:
    response = run_adapter(sys.argv[1:], sys.stdin.read())
    sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
