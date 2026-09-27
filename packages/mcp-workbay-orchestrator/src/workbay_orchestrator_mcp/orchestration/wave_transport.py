"""Host-side Git transport and status helpers for VM-resident waves.

The host publishes one immutable input commit with the requested base in an
atomic push. Pull only updates the journal and wave remote-tracking refs, then
imports the journal into the host-local SQLite store.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

from . import wave_journal, wave_scheduler

_GIT_TIMEOUT_SECONDS = 15
_DEFAULT_UNIT_WIDTH = 4
_DEFAULT_UNIT_ATTEMPT_BUDGET = 2
_MAX_UNIT_WIDTH = 16
_MAX_UNIT_ATTEMPT_BUDGET = 5
_STATE_NAMES = frozenset({"pending", "running", "collected", "gated", "integrated", "failed", "parked"})
_KIND_TO_STATE = {
    "lane_pending": "pending",
    "lane_running": "running",
    "lane_collected": "collected",
    "lane_gated": "gated",
    "lane_integrated": "integrated",
    "lane_failed": "failed",
    "lane_parked": "parked",
    "lane.pending": "pending",
    "lane.running": "running",
    "lane.collected": "collected",
    "lane.gated": "gated",
    "lane.integrated": "integrated",
    "lane.failed": "failed",
    "lane.parked": "parked",
}
_INTEGRATION_SUBJECT = re.compile(r"^(?:integrate|merge)\s+(?:lane\s+)?([A-Za-z0-9._-]+)$", re.IGNORECASE)


class _GitError(RuntimeError):
    def __init__(self, reason: str, stderr: str = "") -> None:
        self.reason = reason
        self.stderr = stderr
        super().__init__(reason)


def _run_git(
    repo: str | os.PathLike[str],
    *args: str,
    input_data: bytes | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", "-C", os.fspath(repo), *args],
            input=input_data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise _GitError("git_timeout") from exc
    except OSError as exc:
        raise _GitError("git_unavailable") from exc


def _checked_git(
    repo: str | os.PathLike[str],
    *args: str,
    input_data: bytes | None = None,
    env: dict[str, str] | None = None,
) -> bytes:
    result = _run_git(repo, *args, input_data=input_data, env=env)
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise _GitError("git_command_failed", detail)
    return result.stdout


def _input_ref(wave: str) -> str:
    return f"refs/workbay/wave-inputs/{wave_journal.validate_wave_id(wave)}"


def _base_ref(wave: str) -> str:
    return f"refs/workbay/wave-bases/{wave_journal.validate_wave_id(wave)}"


def _validate_spec(wave: str, raw: bytes) -> tuple[dict[str, Any] | None, list[dict[str, object]]]:
    try:
        document = json.loads(raw.decode("utf-8"))
        if not isinstance(document, dict):
            return None, [{"type": "invalid_document"}]
        if document.get("wave") != wave:
            return None, [{"type": "wave_mismatch", "expected": wave, "actual": document.get("wave")}]
        problems = wave_scheduler.validate(document)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return None, [{"type": "invalid_document", "detail": str(exc)}]
    return document, problems


def _make_inputs_commit(repo: str | os.PathLike[str], wave: str, raw: bytes) -> str:
    blob = _checked_git(repo, "hash-object", "-w", "--stdin", input_data=raw).decode("ascii").strip()
    tree_input = f"100644 blob {blob}\twave.json\n".encode("ascii")
    tree = _checked_git(repo, "mktree", input_data=tree_input).decode("ascii").strip()
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "workbay-wave-inputs",
            "GIT_AUTHOR_EMAIL": "wave-inputs@workbay.invalid",
            "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
            "GIT_COMMITTER_NAME": "workbay-wave-inputs",
            "GIT_COMMITTER_EMAIL": "wave-inputs@workbay.invalid",
            "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
        }
    )
    commit = _checked_git(repo, "commit-tree", tree, "-m", f"workbay wave inputs {wave}", env=env)
    return commit.decode("ascii").strip()


def _remote_oid(repo: str | os.PathLike[str], remote: str, ref: str) -> str | None:
    try:
        result = _run_git(repo, "ls-remote", "--refs", "--", remote, ref)
    except _GitError as exc:
        raise _GitError("remote_unreachable", exc.stderr) from exc
    if result.returncode != 0:
        raise _GitError("remote_unreachable", result.stderr.decode("utf-8", errors="replace").strip())
    for line in result.stdout.decode("utf-8", errors="replace").splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1] == ref:
            return fields[0]
    return None


def _save_local_refs(repo: str | os.PathLike[str], wave: str, inputs_oid: str, base_oid: str) -> None:
    commands = (
        f"start\nupdate {_input_ref(wave)} {inputs_oid}\nupdate {_base_ref(wave)} {base_oid}\nprepare\ncommit\n"
    ).encode("ascii")
    _checked_git(repo, "update-ref", "--stdin", input_data=commands)


def push(
    repo: str | os.PathLike[str],
    wave: str,
    spec_path: str | os.PathLike[str],
    *,
    remote: str,
    base: str,
) -> dict[str, Any]:
    """Publish validated wave inputs and the requested base in one atomic push."""
    try:
        wave_journal.validate_wave_id(wave)
    except wave_journal.JournalError:
        return {"ok": False, "reason": "invalid_wave_id"}
    try:
        raw = Path(spec_path).read_bytes()
    except OSError:
        return {"ok": False, "reason": "spec_unreadable", "wave": wave}
    _document, problems = _validate_spec(wave, raw)
    if problems:
        return {"ok": False, "reason": "invalid_wave_spec", "wave": wave, "problems": problems}
    try:
        base_oid = (
            _checked_git(repo, "rev-parse", "--verify", "--end-of-options", f"{base}^{{commit}}")
            .decode("ascii")
            .strip()
        )
        inputs_oid = _make_inputs_commit(repo, wave, raw)
        inputs_ref = _input_ref(wave)
        remote_oid = _remote_oid(repo, remote, inputs_ref)
    except _GitError as exc:
        reason = "remote_unreachable" if exc.reason == "remote_unreachable" else exc.reason
        return {"ok": False, "reason": reason, "wave": wave}

    if remote_oid is not None and remote_oid != inputs_oid:
        return {"ok": False, "reason": "wave_inputs_conflict", "wave": wave}

    if remote_oid == inputs_oid:
        try:
            published_base = _remote_oid(repo, remote, f"refs/heads/wave-base/{wave}")
            if published_base is not None:
                _save_local_refs(repo, wave, inputs_oid, published_base)
        except _GitError as exc:
            reason = "remote_unreachable" if exc.reason == "remote_unreachable" else "local_ref_failed"
            return {"ok": False, "reason": reason, "wave": wave}
        return {"ok": True, "reason": "already_pushed", "wave": wave}

    base_destination = f"refs/heads/wave-base/{wave}"
    try:
        result = _run_git(
            repo,
            "push",
            "--atomic",
            "--porcelain",
            f"--force-with-lease={base_destination}:",
            "--",
            remote,
            f"{base_oid}:{base_destination}",
            f"{inputs_oid}:{inputs_ref}",
        )
    except _GitError:
        return {"ok": False, "reason": "remote_unreachable", "wave": wave}
    if result.returncode != 0:
        try:
            after = _remote_oid(repo, remote, inputs_ref)
        except _GitError:
            return {"ok": False, "reason": "remote_unreachable", "wave": wave}
        if after == inputs_oid:
            try:
                published_base = _remote_oid(repo, remote, base_destination)
                if published_base is not None:
                    _save_local_refs(repo, wave, inputs_oid, published_base)
            except _GitError:
                return {"ok": False, "reason": "local_ref_failed", "wave": wave, "published": True}
            return {"ok": True, "reason": "already_pushed", "wave": wave}
        if after is not None:
            return {"ok": False, "reason": "wave_inputs_conflict", "wave": wave}
        return {"ok": False, "reason": "push_failed", "wave": wave}

    try:
        _save_local_refs(repo, wave, inputs_oid, base_oid)
    except _GitError:
        return {"ok": False, "reason": "local_ref_failed", "wave": wave, "published": True}
    return {"ok": True, "reason": "pushed", "wave": wave, "inputs": inputs_oid, "base": base_oid}


def pull(
    repo: str | os.PathLike[str],
    wave: str,
    *,
    remote: str,
    store_path: str | os.PathLike[str],
) -> dict[str, Any]:
    """Atomically fetch only this wave's refs, then import its journal."""
    try:
        wave_journal.validate_wave_id(wave)
    except wave_journal.JournalError:
        return {"ok": False, "reason": "invalid_wave_id"}
    remote_branch = f"refs/remotes/{remote}/wave/{wave}"
    try:
        remote_check = _run_git(repo, "check-ref-format", remote_branch)
    except _GitError as exc:
        return {"ok": False, "reason": exc.reason}
    if remote_check.returncode != 0:
        return {"ok": False, "reason": "invalid_remote"}
    journal_spec = wave_journal.fetch_refspec(wave)
    branch_spec = f"+refs/heads/wave/{wave}:{remote_branch}"
    try:
        fetched = _run_git(
            repo,
            "fetch",
            "--atomic",
            "--no-tags",
            "--no-write-fetch-head",
            "--",
            remote,
            journal_spec,
            branch_spec,
        )
    except _GitError:
        return {"ok": False, "reason": "remote_unreachable"}
    if fetched.returncode != 0:
        return {"ok": False, "reason": "remote_unreachable"}
    try:
        return wave_journal.import_journal(repo, wave, store_path)
    except (wave_journal.JournalError, OSError, sqlite3.Error) as exc:
        reason = exc.reason if isinstance(exc, wave_journal.JournalError) else "journal_import_failed"
        return {"ok": False, "reason": reason}


def _fold_lane_states(events: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
    """Fold lane state events in journal order; ignore events for other purposes."""
    states: dict[str, str] = {}
    parked: dict[str, str] = {}
    for event in events:
        kind = event.get("kind")
        fields = event.get("fields")
        if not isinstance(fields, dict):
            continue
        lane_id = fields.get("lane_id", fields.get("lane"))
        if not isinstance(lane_id, str) or not lane_id:
            continue
        is_state_kind = isinstance(kind, str) and kind in {
            "lane_state",
            "lane.state",
            "lane_transition",
            "lane.transition",
        }
        state = fields.get("state") if is_state_kind else None
        if state is None and isinstance(kind, str):
            state = _KIND_TO_STATE.get(kind)
        if not isinstance(state, str) or state not in _STATE_NAMES:
            continue
        states[lane_id] = state
        if state == "parked":
            reason = fields.get("reason")
            parked[lane_id] = reason if isinstance(reason, str) and reason else "unspecified"
        else:
            parked.pop(lane_id, None)
    return states, parked


def _read_stored_events(store_path: str | os.PathLike[str], wave: str) -> list[dict[str, Any]]:
    path = Path(store_path)
    if not path.is_file():
        raise _GitError("journal_store_missing")
    try:
        uri = f"{path.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
            rows = conn.execute("SELECT payload FROM journal_events WHERE wave = ? ORDER BY rowid", (wave,)).fetchall()
    except (OSError, sqlite3.Error) as exc:
        raise _GitError("journal_store_read_failed") from exc
    events: list[dict[str, Any]] = []
    for (payload,) in rows:
        try:
            event = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _wave_branch(repo: str | os.PathLike[str], wave: str) -> str | None:
    refs = _checked_git(repo, "for-each-ref", "--format=%(refname)", "refs/remotes/").decode("utf-8", errors="replace")
    suffix = f"/wave/{wave}"
    matches = sorted(ref for ref in refs.splitlines() if ref.startswith("refs/remotes/") and ref.endswith(suffix))
    if matches:
        return matches[0]
    local = f"refs/heads/wave/{wave}"
    result = _run_git(repo, "rev-parse", "--verify", "--quiet", local)
    return local if result.returncode == 0 else None


def _integration_entries(repo: str | os.PathLike[str], wave: str) -> list[dict[str, str | None]]:
    branch = _wave_branch(repo, wave)
    if branch is None:
        return []
    base = _base_ref(wave)
    base_result = _run_git(repo, "rev-parse", "--verify", "--quiet", base)
    revision = f"{base}..{branch}" if base_result.returncode == 0 else branch
    commits = _checked_git(repo, "rev-list", "--first-parent", "--reverse", revision).decode().splitlines()
    entries: list[dict[str, str | None]] = []
    for commit in commits:
        subject = _checked_git(repo, "show", "-s", "--format=%s", commit).decode("utf-8", errors="replace").strip()
        match = _INTEGRATION_SUBJECT.fullmatch(subject)
        entries.append({"lane_id": match.group(1) if match else None, "commit": commit, "subject": subject})
    return entries


def status(repo: str | os.PathLike[str], wave: str, *, store_path: str | os.PathLike[str]) -> dict[str, Any]:
    """Report folded lane states, integration commits, and current parked reasons."""
    try:
        wave_journal.validate_wave_id(wave)
    except wave_journal.JournalError:
        return {"ok": False, "reason": "invalid_wave_id"}
    try:
        spec_raw = _checked_git(repo, "show", f"{_input_ref(wave)}:wave.json")
    except _GitError:
        return {"ok": False, "reason": "wave_inputs_missing", "wave": wave}
    _document, problems = _validate_spec(wave, spec_raw)
    if problems:
        return {"ok": False, "reason": "invalid_wave_spec", "wave": wave, "problems": problems}
    try:
        events = _read_stored_events(store_path, wave)
        integration_order = _integration_entries(repo, wave)
    except _GitError as exc:
        return {"ok": False, "reason": exc.reason, "wave": wave}
    states, parked = _fold_lane_states(events)
    lane_ids = _document.get("lanes", {}) if isinstance(_document, dict) else {}
    states = {lane_id: states.get(lane_id, "pending") for lane_id in sorted(lane_ids)}
    parked = {lane_id: reason for lane_id, reason in parked.items() if lane_id in lane_ids}
    return {
        "ok": True,
        "wave": wave,
        "states": states,
        "integration_order": integration_order,
        "parked": parked,
    }


def _systemd_arg(value: str) -> str:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError("unit arguments cannot contain line breaks or NUL")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%").replace("$", "$$")
    return f'"{escaped}"'


def render_unit(
    wave: str,
    *,
    repo: str,
    python: str,
    width: int = _DEFAULT_UNIT_WIDTH,
    attempt_budget: int = _DEFAULT_UNIT_ATTEMPT_BUDGET,
) -> str:
    """Render an operator-installable systemd user service for a wave."""
    wave_journal.validate_wave_id(wave)
    if type(width) is not int or not 1 <= width <= _MAX_UNIT_WIDTH:
        raise ValueError("width must be between 1 and 16")
    if type(attempt_budget) is not int or not 1 <= attempt_budget <= _MAX_UNIT_ATTEMPT_BUDGET:
        raise ValueError("attempt budget must be between 1 and 5")
    executable = _systemd_arg(python)
    repository = _systemd_arg(repo)
    wave_arg = _systemd_arg(wave)
    width_arg = _systemd_arg(str(width))
    attempt_budget_arg = _systemd_arg(str(attempt_budget))
    return (
        f"[Unit]\nDescription=Workbay wave supervisor {wave}\nAfter=network-online.target\n\n"
        "[Service]\nType=simple\n"
        f"ExecStart={executable} -m workbay_orchestrator_mcp.orchestration.wave_supervisor run "
        f"--wave {wave_arg} --repo {repository} --width {width_arg} --attempt-budget {attempt_budget_arg}\n"
        "Restart=on-failure\nRestartPreventExitStatus=2 3\nRestartSec=30\n"
        "KillSignal=SIGTERM\nTimeoutStopSec=120\n\n"
        "[Install]\nWantedBy=default.target\n"
    )


def _print_json(result: dict[str, Any]) -> None:
    print(json.dumps(result, sort_keys=True, ensure_ascii=False))


def _render_status_text(result: dict[str, Any]) -> str:
    lines = [f"wave: {result['wave']}", "states:"]
    lines.extend(f"  {lane}: {state}" for lane, state in result["states"].items())
    lines.append("integration order:")
    for index, entry in enumerate(result["integration_order"], start=1):
        label = entry["lane_id"] or entry["subject"]
        lines.append(f"  {index}. {label} ({entry['commit'][:12]})")
    lines.append("parked:")
    lines.extend(f"  {lane}: {reason}" for lane, reason in result["parked"].items())
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="wave_transport")
    commands = parser.add_subparsers(dest="command", required=True)

    push_parser = commands.add_parser("push", help="publish validated wave inputs")
    push_parser.add_argument("repo")
    push_parser.add_argument("wave")
    push_parser.add_argument("spec_path")
    push_parser.add_argument("--remote", required=True)
    push_parser.add_argument("--base", required=True)

    pull_parser = commands.add_parser("pull", help="fetch a wave and import its journal")
    pull_parser.add_argument("repo")
    pull_parser.add_argument("wave")
    pull_parser.add_argument("--remote", required=True)
    pull_parser.add_argument("--store-path", required=True)

    status_parser = commands.add_parser("status", help="print imported wave status")
    status_parser.add_argument("repo")
    status_parser.add_argument("wave")
    status_parser.add_argument("--store-path", required=True)
    status_parser.add_argument("--json", action="store_true")

    unit_parser = commands.add_parser("unit", help="render a systemd user service")
    unit_parser.add_argument("wave")
    unit_parser.add_argument("--repo", required=True)
    unit_parser.add_argument("--python", default=sys.executable)
    unit_parser.add_argument("--width", type=int, default=_DEFAULT_UNIT_WIDTH)
    unit_parser.add_argument("--attempt-budget", type=int, default=_DEFAULT_UNIT_ATTEMPT_BUDGET)

    args = parser.parse_args(argv)
    if args.command == "push":
        result = push(args.repo, args.wave, args.spec_path, remote=args.remote, base=args.base)
    elif args.command == "pull":
        result = pull(args.repo, args.wave, remote=args.remote, store_path=args.store_path)
    elif args.command == "status":
        result = status(args.repo, args.wave, store_path=args.store_path)
        if result.get("ok") and not args.json:
            print(_render_status_text(result))
            return 0
    else:
        try:
            print(
                render_unit(
                    args.wave,
                    repo=args.repo,
                    python=args.python,
                    width=args.width,
                    attempt_budget=args.attempt_budget,
                ),
                end="",
            )
            return 0
        except (ValueError, wave_journal.JournalError):
            return 2

    _print_json(result)
    if result.get("ok") is True:
        return 0
    return 4 if result.get("reason") == "remote_unreachable" else 2


if __name__ == "__main__":
    raise SystemExit(main())
