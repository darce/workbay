#!/usr/bin/env python3
"""PostToolUse hook: capture workbay-related Bash failures into agent_errors.

internal (internal), Claude harness first. Reads the
PostToolUse JSON payload from stdin, pattern-matches workbay-related
failures in Bash tool results — ImportError/ModuleNotFoundError
tracebacks naming ``workbay_*`` modules, nonzero exits from workbay
make targets or CLIs, workbay MCP connection failures — classifies
them per the agent-error taxonomy, and writes through
``mcp-workbay-handoff errors-record`` so schema/DB ownership stays
with the package that defines the write contract.

Deliberately NOT captured here:
- non-workbay failures — the matcher errs toward silence on ambiguity
- MCP write rejections (ok:false envelopes) — the server self-captures
  those (implementation note); hook-side capture would double-count

Best-effort: exits 0 on any error so capture never blocks the user's
flow. ``errors-record`` itself spools when the local DB schema is
stale, so this hook never needs to reason about schema versions.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import zlib
from pathlib import Path

_SUMMARY_LIMIT = 256
_DETAIL_TAIL_CHARS = 4000

_IMPORT_ERROR_RE = re.compile(
    r"(?:ImportError|ModuleNotFoundError): "
    r"(?:cannot import name '[^']+' from '(?P<from_module>[A-Za-z0-9_.]+)'"
    r"|No module named '(?P<missing_module>[A-Za-z0-9_.]+)')"
)
_WORKBAY_COMMAND_RE = re.compile(
    r"\bmake\s+(?:(?:task|slice|handoff|review|plan|errors|context|release)[a-z-]*"
    r"|check-[a-z0-9-]+|dogfood|install)\b"
    r"|\bmcp-workbay-handoff\b"
    r"|\bworkbay-bootstrap\b"
    r"|\buv\s+run\s+workbay[a-z0-9_-]*\b"
    r"|python3?\s+-m\s+workbay_[a-z_]+"
)
_WORKBAY_TOKEN_RE = re.compile(r"workbay[_-][a-z0-9_-]+|mcp[_-]workbay[_-][a-z0-9_-]+", re.IGNORECASE)
_MCP_UNREACHABLE_RE = re.compile(
    r"MCP error -32\d\d\d|Connection (?:closed|refused|reset)|connect(?:ion)? timed? ?out",
    re.IGNORECASE,
)


def _first_matching_line(output: str, pattern: re.Pattern[str]) -> str:
    for line in output.splitlines():
        if pattern.search(line):
            return line.strip()
    return ""


def classify(*, command: str, output: str, exit_code: int) -> dict | None:
    """Classify a Bash tool result as a workbay agent error, or None.

    Silence-first: anything ambiguous or non-workbay returns None.
    Successful commands (exit 0) are never classified, even when their
    output happens to contain error-shaped text.
    """
    if exit_code == 0:
        return None
    command = command or ""
    output = output or ""

    # install_drift: ImportError/ModuleNotFoundError naming a workbay module.
    import_match = _IMPORT_ERROR_RE.search(output)
    if import_match:
        module = import_match.group("from_module") or import_match.group("missing_module") or ""
        root = module.split(".")[0]
        if root.startswith("workbay"):
            summary = _first_matching_line(output, _IMPORT_ERROR_RE) or import_match.group(0)
            return {
                "error_class": "install_drift",
                "summary": summary[:_SUMMARY_LIMIT],
                "package_name": root,
            }
        return None

    # mcp_unreachable: connection-level failure mentioning a workbay server.
    if _MCP_UNREACHABLE_RE.search(output) and (
        _WORKBAY_TOKEN_RE.search(output) or _WORKBAY_TOKEN_RE.search(command)
    ):
        summary = _first_matching_line(output, _MCP_UNREACHABLE_RE) or "workbay MCP server unreachable"
        return {
            "error_class": "mcp_unreachable",
            "summary": summary[:_SUMMARY_LIMIT],
        }

    # cli_failure: workbay make target / CLI exited nonzero.
    if _WORKBAY_COMMAND_RE.search(command):
        first_error_line = ""
        for line in output.splitlines():
            stripped = line.strip()
            if stripped:
                first_error_line = stripped
                break
        summary = f"{command.strip()[:120]} exited {exit_code}"
        if first_error_line:
            summary = f"{summary}: {first_error_line}"
        return {
            "error_class": "cli_failure",
            "summary": summary[:_SUMMARY_LIMIT],
        }

    return None


_SHELL_TOOL_PAYLOAD_PATH = Path(__file__).resolve().parent / "_shell_tool_payload.py"


def _shell_tool_payload():
    """Sibling shell-tool payload adapter, loaded by path.

    Root and payload twins share basenames, so the module is keyed by its file
    location instead of being imported through ``sys.path``.
    """
    name = f"_shell_tool_payload_{zlib.crc32(str(_SHELL_TOOL_PAYLOAD_PATH).encode()):08x}"
    module = sys.modules.get(name)
    if module is None:
        spec = importlib.util.spec_from_file_location(name, _SHELL_TOOL_PAYLOAD_PATH)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {_SHELL_TOOL_PAYLOAD_PATH}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(name, None)
            raise
    return module


_HARNESS_CHOICES = ("claude-code", "codex", "grok", "cursor", "manual")


def _env_alias(canonical: str, default: str | None = None) -> str | None:
    """Read a canonical ``WORKBAY_*`` override. The lazy ``_interp`` import keeps
    the bare-``python3`` module load (and the importlib-loaded test harness) free
    of a hooks-dir ``sys.path`` requirement; the shim resolves via
    ``workbay_protocol`` when importable, else a stdlib fallback."""
    from _interp import resolve_env_alias

    return resolve_env_alias(canonical, default=default)


def _resolve_harness() -> str:
    """Derive harness label from WORKBAY_HANDOFF_HARNESS (internal).

    Mirrors ``compact-session.py:_resolve_harness``: unknown values coerce to
    ``manual``; defaults to ``claude-code`` when unset.
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


def _resolve_errors_record_argv(repo_root: str | None = None) -> list[str]:
    """Resolve the errors-record invocation: console script, else module.

    The module fallback uses a deps-bearing interpreter (project venv) rather
    than a possibly deps-less launch ``python3``, so error telemetry survives a
    system Python upgrade even when the console script is off PATH.
    Harness-agnostic shared helper. See scripts/hooks/_interp.py.

    ``repo_root`` is forwarded to :func:`resolve_deps_python` on the module
    branch so worktree/primary-checkout venv discovery matches
    ``record-file-touch``.
    """
    console_script = shutil.which("mcp-workbay-handoff")
    if console_script:
        return [console_script, "errors-record"]
    from _interp import resolve_deps_python

    return [resolve_deps_python(repo_root), "-m", "workbay_handoff_mcp", "errors-record"]


def _resolve_agent_handoff_src(repo_root: str) -> str:
    from resolve_handoff_src import resolve_agent_handoff_src

    return resolve_agent_handoff_src(repo_root)


def process_event(data: dict) -> int:
    # A shell result with no usable command is still classified on its output,
    # as before the adapter, so the command is coerced rather than required.
    call = _shell_tool_payload().extract_shell_call(data, coerce_command=True)
    if call is None:
        return 0
    command = call.command
    workdir = str(call.workdir) if call.workdir is not None else None

    tool_response = data.get("tool_response") or data.get("toolResponse") or {}
    if not isinstance(tool_response, dict):
        return 0
    stdout = str(tool_response.get("stdout") or "")
    stderr = str(tool_response.get("stderr") or "")
    exit_code_raw = tool_response.get("exitCode", tool_response.get("exit_code", 0))
    try:
        exit_code = int(exit_code_raw)
    except (TypeError, ValueError):
        exit_code = 0
    output = (stdout + "\n" + stderr).strip()

    event = classify(command=command, output=output, exit_code=exit_code)
    if event is None:
        return 0

    # Argv construction is outside the write try so resolution failures are
    # recorded rather than vanishing inside a silent swallow (REV-B-06).
    try:
        from resolve_handoff_src import workspace_env_anchor

        repo_root = workspace_env_anchor() or workdir or os.getcwd()
    except Exception:
        repo_root = workdir or os.getcwd()

    try:
        argv = _resolve_errors_record_argv(repo_root)
        argv += ["--error-class", event["error_class"], "--summary", event["summary"]]
        if output:
            argv += ["--detail", output[-_DETAIL_TAIL_CHARS:]]
        if command:
            argv += ["--command-preview", command]
        if event.get("package_name"):
            argv += ["--package-name", event["package_name"]]
        argv += ["--tool-name", "Bash", "--harness", _resolve_harness()]
    except Exception as exc:
        try:
            from _hook_failure_sink import record_hook_failure

            record_hook_failure(
                source="capture-agent-errors",
                kind="exception",
                detail=f"argv: {type(exc).__name__}: {exc}",
                repo_root=repo_root,
            )
        except Exception:
            pass
        return 0

    try:
        # Sink is imported only on the failure path so a missing/broken
        # _hook_failure_sink can never prevent the primary errors-record write.
        from resolve_handoff_src import is_metadata_dist_src

        env = os.environ.copy()
        src_path = _resolve_agent_handoff_src(repo_root)
        # Per-argv-branch guard (one predicate cannot cover both shapes):
        # - console-script argv[0] is NOT an interpreter — suppress only a
        #   metadata-dist src_path; keep repo-relative injection.
        # - module branch argv[0] IS resolve_deps_python(repo_root) — same
        #   equality test as record-file-touch.
        is_module_branch = len(argv) >= 2 and argv[1] == "-m"
        if src_path:
            if is_module_branch:
                if not is_metadata_dist_src(src_path) or (
                    os.path.abspath(argv[0]) == os.path.abspath(sys.executable)
                ):
                    env["PYTHONPATH"] = src_path + (
                        os.pathsep + env.get("PYTHONPATH", "")
                    )
            elif not is_metadata_dist_src(src_path):
                env["PYTHONPATH"] = src_path + (
                    os.pathsep + env.get("PYTHONPATH", "")
                )
        proc = subprocess.run(argv, capture_output=True, timeout=10, env=env)
        # internal: surface non-zero writes via hook-failures.log.
        if proc.returncode != 0:
            err = proc.stderr or b""
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            from _hook_failure_sink import record_hook_failure

            record_hook_failure(
                source="capture-agent-errors",
                kind="returncode",
                detail=f"rc={proc.returncode} {err}",
                repo_root=repo_root,
            )
    except Exception as exc:
        # ImportError of is_metadata_dist_src lands as kind=exception (REV-A-03).
        try:
            from _hook_failure_sink import record_hook_failure

            record_hook_failure(
                source="capture-agent-errors",
                kind="exception",
                detail=f"{type(exc).__name__}: {exc}",
                repo_root=repo_root,
            )
        except Exception:
            pass
    return 0


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(data, dict):
        return 0

    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(data, expected="PostToolUse")
    except ImportError:
        pass

    try:
        return process_event(data)
    except Exception:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
