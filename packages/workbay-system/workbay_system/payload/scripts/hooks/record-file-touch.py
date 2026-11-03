#!/usr/bin/env python3
"""PostToolUse hook: record file touches after Edit/Write tool calls.

Reads the Claude Code PostToolUse JSON payload from stdin, extracts the
file path, determines the change kind (edit vs add), and calls
record_file_touch via the Python API.

Edit events always record change_kind='edit'. Write events check whether
the file was already tracked by git: tracked files record 'edit',
untracked files record 'add'. File deletion is out of scope for this
hook surface.

Best-effort: exits 0 on any error so file-touch recording never blocks
the user's editing flow.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap


def _payload_value(payload: dict, snake_key: str, camel_key: str, default: str = "") -> str:
    value = payload.get(snake_key)
    if value:
        return value
    camel_value = payload.get(camel_key)
    if camel_value:
        return camel_value
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
    except Exception:
        pass
    from resolve_handoff_src import workspace_env_anchor

    return workspace_env_anchor()


def _git_current_branch(repo_root: str) -> str:
    try:
        proc = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:
        pass
    return ""


def _to_monorepo_relative(abs_path: str, repo_root: str) -> str:
    if not repo_root or not abs_path:
        return ""
    try:
        return os.path.relpath(abs_path, repo_root)
    except ValueError:
        return ""


def _is_tracked_by_git(abs_path: str) -> bool:
    try:
        proc = subprocess.run(
            ["git", "ls-files", "--error-unmatch", abs_path],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return proc.returncode == 0
    except Exception:
        return True


def _resolve_agent_handoff_src(repo_root: str) -> str:
    from resolve_handoff_src import resolve_agent_handoff_src

    return resolve_agent_handoff_src(repo_root)


def _determine_change_kind(tool_name: str, abs_path: str) -> str:
    if tool_name == "Edit":
        return "edit"
    if _is_tracked_by_git(abs_path):
        return "edit"
    return "add"


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(data, dict):
        return 0

    # Cross-repo wire-shape contract: validate the PostToolUse payload
    # via the shared helper. Validation failures log to stderr (visible
    # in CI / hook output) rather than being swallowed, so contract
    # drift surfaces. The helper falls back to no-op when the protocol
    # package is not installed.
    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(data, expected="PostToolUse")
    except ImportError:
        pass

    tool_name = _payload_value(data, "tool_name", "toolName")
    if tool_name not in ("Edit", "Write"):
        return 0

    tool_input = data.get("tool_input") or data.get("toolInput") or {}
    if not isinstance(tool_input, dict):
        return 0

    file_path = _payload_value(tool_input, "file_path", "filePath")
    if not file_path:
        return 0

    repo_root = _git_repo_root()
    rel_path = _to_monorepo_relative(file_path, repo_root)
    if not rel_path or rel_path.startswith(".."):
        return 0

    change_kind = _determine_change_kind(tool_name, file_path)
    has_task_pin = any(os.environ.get(name, "").strip() for name in ("WORKBAY_LANE_ID", "WORKBAY_HANDOFF_ACTIVE_TASK"))
    branch = "" if has_task_pin else _git_current_branch(repo_root)

    try:
        # Per-PostToolUse hook: never re-exec (latency). Instead point the
        # stack subprocess at a deps-bearing interpreter (project venv) so a
        # deps-less launch `python3` doesn't silently drop touch records.
        # Harness-agnostic shared helper. See scripts/hooks/_interp.py.
        # Sink is imported only on the failure path so a missing/broken
        # _hook_failure_sink can never prevent the primary provenance write.
        from _interp import resolve_deps_python
        from resolve_handoff_src import is_metadata_dist_src

        env = os.environ.copy()
        src_path = _resolve_agent_handoff_src(repo_root)
        executor = resolve_deps_python(repo_root)
        # Only the metadata-dist branch is interpreter-bound. Repo-relative
        # results (in-tree / overlay) stay injected under any executor; a
        # metadata-dist path injects only when resolver == executor.
        if src_path and (
            not is_metadata_dist_src(src_path) or os.path.abspath(executor) == os.path.abspath(sys.executable)
        ):
            env["PYTHONPATH"] = src_path + (os.pathsep + env.get("PYTHONPATH", ""))
        child_code = textwrap.dedent(
            f"""\
            import os
            import sys
            from pathlib import Path

            from workbay_handoff_mcp import RuntimeConfig, configure_runtime, record_file_touch
            from workbay_handoff_mcp.lanes_recording import OPENABLE_LANE_STATUSES
            from workbay_handoff_mcp.shared_primitives import resolve_active_task_ref
            from workbay_handoff_mcp.shared_schema import _get_db_connection

            configure_runtime(RuntimeConfig.for_repo(Path({repo_root!r})))
            has_pin = any(os.environ.get(name, '').strip() for name in (
                'WORKBAY_LANE_ID', 'WORKBAY_HANDOFF_ACTIVE_TASK'
            ))
            task_ref = None
            if has_pin:
                try:
                    with _get_db_connection() as conn:
                        task_ref = resolve_active_task_ref(conn)
                except ValueError:
                    print('WORKBAY_RECORD_FILE_TOUCH_SKIP=unresolved_ambiguous')
                    raise SystemExit(75)
            else:
                branch = {branch!r}
                with _get_db_connection() as conn:
                    if branch:
                        # lanes_recording.py:17-24 keeps closed_stale openable
                        # for the reaper; the remaining openable statuses are live.
                        live_lane_statuses = tuple(sorted(OPENABLE_LANE_STATUSES - {{'closed_stale'}}))
                        status_placeholders = ", ".join("?" for _ in live_lane_statuses)
                        rows = conn.execute(
                            "SELECT DISTINCT wl.task_ref FROM worktree_lanes wl "
                            "JOIN handoff_state hs ON hs.task_ref = wl.task_ref "
                            "WHERE wl.branch = ? "
                            f"AND wl.status IN ({{status_placeholders}}) "
                            "AND hs.status IN ('in_progress', 'review', 'blocked')",
                            (branch, *live_lane_statuses),
                        ).fetchall()
                    else:
                        rows = []
                    task_refs = sorted({{str(row['task_ref']) for row in rows if row['task_ref']}})
                    if len(task_refs) == 1:
                        task_ref = task_refs[0]
            if task_ref is None:
                print('WORKBAY_RECORD_FILE_TOUCH_SKIP=unresolved_ambiguous')
                raise SystemExit(75)

            record_file_touch(
                file_path={rel_path!r},
                change_kind={change_kind!r},
                task_ref=task_ref,
            )
            """
        )
        proc = subprocess.run(
            [
                executor,
                "-c",
                child_code,
            ],
            capture_output=True,
            timeout=10,
            env=env,
        )
        stdout = proc.stdout or b""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if proc.returncode == 75 and "WORKBAY_RECORD_FILE_TOUCH_SKIP=unresolved_ambiguous" in stdout.splitlines():
            from _hook_failure_sink import record_hook_failure

            detail = (
                "environment task pin did not resolve to one active task"
                if has_task_pin
                else f"branch={branch or '(detached)'} has no unique active lane binding"
            )
            record_hook_failure(
                source="record-file-touch",
                kind="unresolved_ambiguous",
                detail=detail,
                repo_root=repo_root,
            )
        # internal: never present a failed write as success.
        # Exit stays 0 (do not block Edit/Write); surface via hook-failures.log.
        elif proc.returncode != 0:
            err = proc.stderr or b""
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            from _hook_failure_sink import record_hook_failure

            record_hook_failure(
                source="record-file-touch",
                kind="returncode",
                detail=f"rc={proc.returncode} {err}",
                repo_root=repo_root,
            )
    except Exception as exc:
        # ImportError of is_metadata_dist_src (stale sibling) and exec failures
        # land here as kind=exception — not silently dropped (REV-A-03).
        try:
            from _hook_failure_sink import record_hook_failure

            record_hook_failure(
                source="record-file-touch",
                kind="exception",
                detail=f"{type(exc).__name__}: {exc}",
                repo_root=repo_root,
            )
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
