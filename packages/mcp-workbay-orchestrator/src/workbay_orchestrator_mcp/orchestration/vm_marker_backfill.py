"""Completion-keyed VM marker recovery, bounded to one batch per lane close.

The ledger is only a cache of remote confirmations. Losing it causes safe redo;
it is never evidence that an unconfirmed marker was delivered.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .lane_census import _default_sandbox_ssh_runner
from .remote_sandbox_reap import (
    SSH_TIMEOUT_S,
    TERMINAL_ROW_STATUSES,
    SshRunner,
    _ListedSandbox,
    _coerce_ssh_result,
    _derive_lane_key,
    _list_remote_sandboxes,
    _remote_lane_product_preserved,
    _resolve_sandbox_root,
    _sh_single_quote,
    _tip_is_ancestor,
)

MAX_MARKERS = 50
LEDGER_NAME = "vm-marker-backfill-confirmed.json"
_logger = logging.getLogger(__name__)


def _result() -> dict[str, Any]:
    return {
        "marked": 0,
        "already_marked": 0,
        "failed": 0,
        "retained": {
            "retained_not_on_main": 0,
            "retained_open_row": 0,
            "retained_no_row": 0,
            "retained_review_lane": 0,
            "retained_base_tip": 0,
            "retained_lookup_failed": 0,
            "retained_sandbox_live": 0,
            "retained_unproven_harvest": 0,
            "retained_unharvested": 0,
            "retained_sandbox_moved": 0,
        },
    }


def _lane_rows(state_dir: Path) -> list[Mapping[str, Any]]:
    with closing(sqlite3.connect((state_dir / "handoff.db").resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(row) for row in conn.execute(
                "SELECT branch, status, branch_tip_sha, landing_commit_sha, lane_kind FROM worktree_lanes"
            )
        ]


def _sibling_veto(
    key: str, *, state_dir: Path, rows: Sequence[Mapping[str, Any]] | None = None,
) -> str | None:
    """All tasks own the key together; an unreadable registry never grants release."""
    try:
        siblings = [
            row for row in (_lane_rows(state_dir) if rows is None else rows)
            if (branch := str(row.get("branch") or "").strip()) and _derive_lane_key(branch) == key
        ]
        if not siblings:
            return "retained_no_row"
        if any(str(row.get("status") or "").strip().lower() not in TERMINAL_ROW_STATUSES for row in siblings):
            return "retained_open_row"
    except Exception:  # noqa: BLE001 — a lookup miss cannot authorize evidence removal
        return "retained_lookup_failed"
    return None


def _main_history(repo: Path) -> tuple[str, set[str], set[str]]:
    env = {**os.environ, "GIT_NO_REPLACE_OBJECTS": "1"}

    def rev_list(*args: str) -> list[str]:
        return subprocess.run(
            ["git", "-C", str(repo), "rev-list", *args], text=True, capture_output=True,
            timeout=30, check=True, env=env,
        ).stdout.splitlines()

    history = rev_list("main")
    if not history or re.fullmatch(r"[0-9a-f]{40}", history[0]) is None:
        raise ValueError("main has no full commit SHA")
    # Pin both traversals to the same main generation.
    return history[0], set(history), set(rev_list("--first-parent", history[0]))


def _marker_eligibility(
    siblings: Sequence[Mapping[str, Any]], *, ancestors: set[str], first_parent: set[str],
    repo: Path, main_sha: str, sandbox: _ListedSandbox | None = None,
) -> tuple[tuple[Mapping[str, Any], str] | None, str | None]:
    """Shared host and VM proof for immediate delivery and bounded backfill."""
    base_tip = False
    proof = None
    landing = None
    for row in siblings:
        row_landing = str(row.get("landing_commit_sha") or "").strip().lower()
        tip = str(row.get("branch_tip_sha") or "").strip().lower()
        if re.fullmatch(r"[0-9a-f]{40}", row_landing) and row_landing in ancestors:
            proof, landing = (row, row_landing), row_landing
            break
        if re.fullmatch(r"[0-9a-f]{40}", tip) and tip in ancestors:
            if tip not in first_parent:
                proof = (row, tip)
                break
            base_tip = True
    if proof is None:
        reason = "retained_base_tip" if base_tip else "retained_not_on_main"
        if not base_tip and siblings and all(row.get("lane_kind") == "review" for row in siblings):
            reason = "retained_review_lane"
        return None, reason
    if sandbox is not None:
        if sandbox.occupant_live or sandbox.lock_held:
            return None, "retained_sandbox_live"
        if sandbox.tip_sha is None:
            return None, "retained_unproven_harvest"
        if not _tip_is_ancestor(repo, sandbox.tip_sha, main_sha):
            preserved, _ = _remote_lane_product_preserved(
                sandbox, repo, expected_tip=proof[1], landing_commit=landing or main_sha,
            )
            if not preserved:
                return None, "retained_unharvested"
    return proof, None


def _marker_ssh_runner(repo: Path) -> SshRunner:
    def runner(script: str, *, timeout: float | None = SSH_TIMEOUT_S):
        if os.environ.get("PYTEST_CURRENT_TEST"):
            raise RuntimeError("default marker backfill SSH is forbidden under pytest")
        return _default_sandbox_ssh_runner(script, timeout=timeout, workspace_root=repo)

    return runner


def _sandbox_inventory(ssh_runner: SshRunner, root: str, keys: Sequence[str]) -> dict[str, _ListedSandbox]:
    """Validate one targeted inventory completely before any marker can be written."""
    listed, error = _list_remote_sandboxes(ssh_runner, root, target_keys=keys)
    if error or getattr(listed, "invalid_keys", ()):
        raise ValueError(error or "invalid sandbox keys in inventory")
    inventory: dict[str, _ListedSandbox] = {}
    for item in listed:
        if not isinstance(item, _ListedSandbox) or item.key not in keys or item.key in inventory:
            raise ValueError("unexpected or duplicate sandbox in targeted inventory")
        inventory[item.key] = item
    return inventory


def _marker_script(
    root: str, markers: Sequence[tuple[str, dict[str, str]]], sandbox_tips: Mapping[str, str] | None = None,
) -> str:
    """Use the remote_sandbox_reap producer's branch/receipt_sha/landed_at shape.

    mv -n also protects concurrent backfill calls between the -s test and rename.
    The existing postmerge writer does not take this fence and remains unchanged.
    """
    lines = [
        "# WORKBAY_REMOTE_MERGED_MARKER op=backfill",
        f"ROOT={_sh_single_quote(root)}",
        'case "$ROOT" in /*) ;; *) ROOT="$HOME/$ROOT" ;; esac',
        "set -eu",
        "umask 077",
        "_tmp=",
        'trap \'if [ -n "$_tmp" ]; then rm -f -- "$_tmp"; fi\' EXIT HUP INT TERM',
        'mkdir -p -- "$ROOT/.merged"',
    ]
    for index, (key, body) in enumerate(markers):
        delimiter = f"WORKBAY_BACKFILL_JSON_{index}"
        if sandbox_tips and key in sandbox_tips:
            lines.extend([
                f"k={_sh_single_quote(key)}",
                f"_expected={_sh_single_quote(sandbox_tips[key])}",
                'if ! _head=$(git -C "$ROOT/$k" --work-tree="$ROOT/$k" --git-dir="$ROOT/$k/.git" rev-parse HEAD 2>/dev/null) || [ "$_head" != "$_expected" ]; then',
                '  printf "MOVED %s\\n" "$k"',
                "else",
            ])
        lines.extend(
            [
                f"k={_sh_single_quote(key)}",
                'if [ -s "$ROOT/.merged/$k.json" ]; then',
                '  printf "ALREADY %s\\n" "$k"',
                "else",
                '  _tmp=$(mktemp "$ROOT/.merged/$k.json.tmp.XXXXXX")',
                f"  cat > \"$_tmp\" <<'{delimiter}'",
                json.dumps(body, separators=(",", ":"), ensure_ascii=True),
                delimiter,
                '  mv -n -- "$_tmp" "$ROOT/.merged/$k.json"',
                '  if [ -e "$_tmp" ]; then',
                '    rm -f -- "$_tmp"',
                '    [ -s "$ROOT/.merged/$k.json" ] || exit 1',
                '    printf "ALREADY %s\\n" "$k"',
                "  else",
                '    printf "MARKED %s\\n" "$k"',
                "  fi",
                "  _tmp=",
                "fi",
            ]
        )
        if sandbox_tips and key in sandbox_tips:
            lines.append("fi")
    # Read-only inventory allows reporting keys with no registry row. No age
    # threshold, release operation, or VM-side ancestry decision is involved.
    lines.extend(
        [
            'for _f in "$ROOT"/*/.workbay-lane-sandbox; do',
            '  [ -f "$_f" ] || continue',
            "  _d=${_f%/.workbay-lane-sandbox}",
            '  printf "PRESENT %s\\n" "${_d##*/}"',
            "done",
            'for _d in "$ROOT"/attempt-evidence-*/; do',
            '  [ -d "$_d" ] || continue',
            "  _d=${_d%/}; _n=${_d##*/}",
            '  _k=$(printf "%s\\n" "$_n" | sed -nE "s/^attempt-evidence-(.+-[0-9a-f]{8})-[0-9]+-[0-9a-f]{16}$/\\1/p")',
            '  [ -z "$_k" ] || printf "PRESENT %s\\n" "$_k"',
            "done",
        ]
    )
    return "\n".join(lines)


def _save_ledger(path: Path, keys: set[str]) -> None:
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=path.name + ".", delete=False) as out:
            temporary = out.name
            json.dump(sorted(keys), out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def backfill_vm_merged_markers(
    *,
    repo: Path,
    state_dir: Path,
    rows: Sequence[Mapping[str, Any]] | None = None,
    runner: SshRunner | None = None,
    focus_branch: str | None = None,
) -> dict[str, Any]:
    """Inventory at most 50 keys, then deliver their markers in one SSH session.

    Every registry row participates in the open-sibling veto. The state-dir
    lock serializes only this backfill's ledger writers, not lane row writers.
    """
    result = _result()
    selected: list[tuple[str, dict[str, str]]] = []
    focus_key = _derive_lane_key(focus_branch) if focus_branch else None

    def retain(key: str, reason: str) -> None:
        result["retained"][reason] += 1
        if key == focus_key:
            result["focus_marker"] = {"ok": False, "reason": reason}

    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        with (state_dir / "vm-marker-backfill.lock").open("a") as lock:
            # Never wait behind another close's remote I/O; its batch will drain.
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {**result, "reason": "backfill_in_progress"}
            ledger = state_dir / LEDGER_NAME
            loaded = json.loads(ledger.read_text()) if ledger.exists() else []
            if not isinstance(loaded, list) or not all(isinstance(key, str) for key in loaded):
                raise ValueError("invalid VM marker confirmation ledger")
            confirmed = set(loaded)
            main_sha, ancestors, first_parent = _main_history(repo)
            if rows is None:
                try:
                    rows = _lane_rows(state_dir)
                except Exception:  # noqa: BLE001 — retain on failed ownership lookup
                    result["retained"]["retained_lookup_failed"] += 1
                    result["reason"] = "retained_lookup_failed"
                    raise
            by_key: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
            for row in rows:
                branch = str(row.get("branch") or "").strip()
                if branch:
                    by_key[_derive_lane_key(branch)].append(row)
            landed_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            candidates: dict[str, list[Mapping[str, Any]]] = {}
            for key, siblings in sorted(by_key.items()):
                veto = _sibling_veto(key, state_dir=state_dir, rows=siblings)
                if veto:
                    retain(key, veto)
                    continue
                proof, reason = _marker_eligibility(
                    siblings, ancestors=ancestors, first_parent=first_parent, repo=repo, main_sha=main_sha,
                )
                if proof is None:
                    retain(key, reason)
                elif key in confirmed:
                    result["already_marked"] += 1
                    if key == focus_key:
                        result["focus_marker"] = {"ok": True}
                elif len(candidates) < MAX_MARKERS:
                    candidates[key] = siblings
            root = _resolve_sandbox_root(None)
            if root is None:
                raise ValueError("sandbox_root is empty or unsafe")

            ssh_runner = runner if runner is not None else _marker_ssh_runner(repo)

            # The real producer rejects unexpected lines. Also reject malformed,
            # duplicate, or out-of-selection entries instead of treating them as
            # an evidence-only key. Nothing has been written or ledgered yet.
            try:
                inventory = _sandbox_inventory(ssh_runner, root, list(candidates))
            except Exception as exc:  # noqa: BLE001 — inventory is an all-or-nothing preflight
                _logger.warning("VM marker backfill inventory failed: %s: %s", type(exc).__name__, exc)
                return {**result, "ok": False, "reason": "inventory_failed", "failed": 1,
                        "error": f"{type(exc).__name__}: {exc}"}
            sandbox_tips: dict[str, str] = {}
            for key, siblings in candidates.items():
                proof, reason = _marker_eligibility(
                    siblings, ancestors=ancestors, first_parent=first_parent, repo=repo,
                    main_sha=main_sha, sandbox=inventory.get(key),
                )
                if proof is None:
                    retain(key, reason)
                    continue
                row, sha = proof
                selected.append((key, {"branch": str(row["branch"]).strip(), "receipt_sha": sha, "landed_at": landed_at}))
                if key in inventory:
                    sandbox_tips[key] = inventory[key].tip_sha
            raw = ssh_runner(_marker_script(root, selected, sandbox_tips), timeout=SSH_TIMEOUT_S)
            stdout, stderr, code = _coerce_ssh_result(raw)
            if code not in (0, None):
                raise RuntimeError(f"ssh exited {code}: {(stderr or stdout)[:300]}")
            expected = {key for key, _ in selected}
            receipts: dict[str, str] = {}
            moved: set[str] = set()
            present: set[str] = set()
            for line in stdout.splitlines():
                kind, _, key = line.partition(" ")
                if kind in {"MARKED", "ALREADY"} and key in expected:
                    receipts[key] = kind
                elif kind == "MOVED" and key in expected:
                    moved.add(key)
                elif kind == "PRESENT" and key:
                    present.add(key)
            result["retained"]["retained_no_row"] = len(present - by_key.keys())
            for key in moved:
                receipts.pop(key, None)
                retain(key, "retained_sandbox_moved")
            result["marked"] = sum(kind == "MARKED" for kind in receipts.values())
            result["already_marked"] += sum(kind == "ALREADY" for kind in receipts.values())
            result["failed"] = len(expected - receipts.keys() - moved)
            if focus_key in receipts:
                result["focus_marker"] = {"ok": True}
            if result["failed"]:
                result["error"] = "marker confirmation missing"
            if receipts:
                _save_ledger(ledger, confirmed | receipts.keys())
            if result["marked"] or result["already_marked"]:
                from .lane_census import request_remote_sandbox_reap  # noqa: PLC0415

                request_remote_sandbox_reap(state_dir)
    except Exception as exc:  # noqa: BLE001 — close has already committed
        result["failed"] = max(1, result["failed"], len(selected) - result["marked"])
        result["error"] = f"{type(exc).__name__}: {exc}"
    if result["failed"]:
        _logger.warning("VM marker backfill degraded: %s", result.get("error"))
    result["ok"] = not bool(result["failed"])
    return result
