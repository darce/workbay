"""The v1 vocabulary and read path for landing receipts on main's first-parent log."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

RECEIPT_VERSION = 1
KEY_LANE = "Workbay-Lane"
KEY_TIP = "Workbay-Lane-Tip"
KEY_GATE = "Workbay-Gate-Receipt"
KEY_RUN = "Workbay-Landing-Run"
KEY_VERSION = "Workbay-Receipt-Version"
KEY_TOMBSTONE = "Workbay-Tombstone"
# Reviewed operator provenance, published in the same commit as its tombstone.
KEY_SUPERSEDE_PLAN = "Workbay-Supersede-Plan"

_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX32 = re.compile(r"^[0-9a-f]{32}$")
_TRAILER_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9-]*): (.*)$")
_GATE_ID = re.compile(r"^[1-9][0-9]*$")
_TOMBSTONE_LINE = re.compile(
    r"^([^@\s]+)@([0-9a-f]{40}) (superseded|abandoned) "
    r"(accepted_plan|superseded_by_integration|content_relocated|bundle):(.+)$"
)
_KNOWN_RECEIPT_KEYS = frozenset((KEY_LANE, KEY_TIP, KEY_GATE, KEY_RUN, KEY_VERSION))
_SUPERSEDE_PLAN_VALUE = re.compile(r"^sha256:([0-9a-f]{64}) (refs/heads/[^\s]+)$")
_GIT_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class LandingReceipt:
    task_ref: str
    lane_id: str
    tip: str
    gate_id: int
    run_id: str | None
    version: int


@dataclass(frozen=True)
class Tombstone:
    branch: str
    tip: str
    disposition: str
    kind: str
    ref: str


@dataclass(frozen=True)
class ReceiptVerdict:
    commit: str
    receipt: LandingReceipt | None
    verified: bool
    failed_check: str | None
    depth: int | None
    detail: str | None


@dataclass(frozen=True)
class TombstoneVerdict:
    tombstone: Tombstone
    verified: bool
    failed_check: str | None
    detail: str | None


@dataclass(frozen=True)
class UnverifiedReceipt:
    commit: str
    task_ref: str | None
    lane_id: str | None
    tip: str | None
    reason: str


@dataclass(frozen=True)
class ScanResult:
    complete: bool
    receipts: tuple[tuple[str, LandingReceipt], ...]
    tombstones: tuple[tuple[str, Tombstone], ...]
    unverified: tuple[tuple[str, str], ...]
    scanned: int
    elapsed_s: float
    error: str | None
    unverified_receipts: tuple[UnverifiedReceipt, ...] = ()
    ordered_receipts: tuple[tuple[str, LandingReceipt | UnverifiedReceipt], ...] = ()


@dataclass(frozen=True)
class CarrierResult:
    commit: str
    main_ref: str
    main_tip: str | None
    contained: bool
    carrier: str | None
    lineage: str | None
    error: str | None


def format_landing_trailers(*, task_ref: str, lane_id: str, tip: str, gate_id: int, run_id: str) -> list[str]:
    """Format the required v1 landing trailer block after validating every field."""

    for label, value in (("task_ref", task_ref), ("lane_id", lane_id)):
        if not isinstance(value, str) or not value or "/" in value or any(ch.isspace() for ch in value):
            raise ValueError(f"{label} must be non-empty and contain no slash or whitespace")
    if not isinstance(tip, str) or _HEX40.fullmatch(tip) is None:
        raise ValueError("tip must be 40 lowercase hexadecimal characters")
    if isinstance(gate_id, bool) or not isinstance(gate_id, int) or gate_id <= 0:
        raise ValueError("gate_id must be a positive integer")
    if not isinstance(run_id, str) or _HEX32.fullmatch(run_id) is None:
        raise ValueError("run_id must be 32 lowercase hexadecimal characters")
    return [
        f"{KEY_LANE}: {task_ref}/{lane_id}",
        f"{KEY_TIP}: {tip}",
        f"{KEY_GATE}: test_result:{gate_id}",
        f"{KEY_RUN}: {run_id}",
    ]


def _valid_relpath(value: str) -> bool:
    if not value or "\x00" in value or value.startswith("/") or "\\" in value:
        return False
    parts = value.split("/")
    return all(part not in ("", ".", "..") for part in parts)


def format_tombstone(*, branch: str, tip: str, disposition: str, kind: str, ref: str) -> str:
    """Format one self-describing tombstone trailer, rejecting unsafe evidence."""

    if not isinstance(branch, str) or not branch or "@" in branch or any(ch.isspace() for ch in branch):
        raise ValueError("branch must be non-empty and contain no @ or whitespace")
    if not isinstance(tip, str) or _HEX40.fullmatch(tip) is None:
        raise ValueError("tip must be 40 lowercase hexadecimal characters")
    if disposition not in ("superseded", "abandoned"):
        raise ValueError("disposition must be superseded or abandoned")
    if kind not in ("accepted_plan", "superseded_by_integration", "content_relocated", "bundle"):
        raise ValueError("unsupported tombstone evidence kind")
    if disposition == "abandoned" and kind != "bundle":
        raise ValueError("abandoned tombstones require bundle evidence")
    if not isinstance(ref, str):
        raise ValueError("ref must be text")
    if kind in ("accepted_plan", "content_relocated"):
        path, separator, blob = ref.rpartition("@")
        if not separator or not _valid_relpath(path) or _HEX40.fullmatch(blob) is None:
            raise ValueError("plan and relocation evidence must be <path>@<blob40>")
    elif kind == "superseded_by_integration":
        if _HEX40.fullmatch(ref) is None:
            raise ValueError("integration evidence must be a 40-character commit")
    elif not _valid_relpath(ref):
        raise ValueError("bundle evidence must be a relative path")
    return f"{KEY_TOMBSTONE}: {branch}@{tip} {disposition} {kind}:{ref}"


def _final_paragraph(message: str) -> list[str] | None:
    if not isinstance(message, str):
        return None
    stripped = message.rstrip()
    if not stripped:
        return None
    paragraphs = [paragraph for paragraph in re.split(r"\n\s*\n", stripped) if paragraph.strip()]
    if len(paragraphs) < 2:
        return None
    return paragraphs[-1].splitlines()


def parse_landing_receipt(message: str) -> LandingReceipt | dict[str, str] | None:
    """Parse a receipt from only the final, pure trailer paragraph."""

    lines = _final_paragraph(message)
    if lines is None:
        return None
    fields: dict[str, str] = {}
    for line in lines:
        match = _TRAILER_LINE.fullmatch(line.rstrip())
        if match is None:
            return None
        key, value = match.group(1), match.group(2).strip()
        if key not in _KNOWN_RECEIPT_KEYS:
            continue
        if key in fields:
            return {"error": f"duplicate_key:{key}"}
        fields[key] = value
    if KEY_LANE not in fields:
        return None
    if fields.get(KEY_VERSION, "1") != "1":
        return {"error": "unknown_receipt_version"}
    for key in (KEY_TIP, KEY_GATE):
        if key not in fields:
            return {"error": f"missing_key:{key}"}
    lane = fields[KEY_LANE]
    if lane.count("/") != 1:
        return {"error": "malformed_lane"}
    task_ref, lane_id = lane.split("/", 1)
    if not task_ref or not lane_id or any(ch.isspace() for ch in lane):
        return {"error": "malformed_lane"}
    tip = fields[KEY_TIP]
    if _HEX40.fullmatch(tip) is None:
        return {"error": "malformed_tip"}
    gate = fields[KEY_GATE]
    gate_match = re.fullmatch(r"test_result:([0-9]+)", gate)
    if gate_match is None or not _GATE_ID.fullmatch(gate_match.group(1)):
        return {"error": "malformed_gate_receipt"}
    run_id = fields.get(KEY_RUN)
    if run_id is not None and _HEX32.fullmatch(run_id) is None:
        return {"error": "malformed_run_id"}
    return LandingReceipt(task_ref, lane_id, tip, int(gate_match.group(1)), run_id, RECEIPT_VERSION)


def _partial_receipt_identity(message: str) -> tuple[str | None, str | None, str | None]:
    """Return unambiguous lane and tip identity fields from malformed trailers."""

    lines = _final_paragraph(message)
    if lines is None:
        return None, None, None
    fields: dict[str, str | None] = {}
    for line in lines:
        match = _TRAILER_LINE.fullmatch(line.rstrip())
        if match is None:
            continue
        key, value = match.group(1), match.group(2).strip()
        if key not in (KEY_LANE, KEY_TIP):
            continue
        if key in fields:
            fields[key] = None
        else:
            fields[key] = value
    raw_lane = fields.get(KEY_LANE)
    task_ref: str | None = None
    lane_id: str | None = None
    if isinstance(raw_lane, str) and raw_lane.count("/") == 1:
        parsed_task, parsed_lane = raw_lane.split("/", 1)
        if parsed_task and parsed_lane and not any(ch.isspace() for ch in raw_lane):
            task_ref, lane_id = parsed_task, parsed_lane
    raw_tip = fields.get(KEY_TIP)
    tip = raw_tip if isinstance(raw_tip, str) and _HEX40.fullmatch(raw_tip) else None
    return task_ref, lane_id, tip


def _parse_tombstone_line(line: str) -> Tombstone | dict[str, str]:
    match = _TRAILER_LINE.fullmatch(line.rstrip())
    if match is None or match.group(1) != KEY_TOMBSTONE:
        return {"error": "malformed_tombstone"}
    parsed = _TOMBSTONE_LINE.fullmatch(match.group(2).strip())
    if parsed is None:
        return {"error": "malformed_tombstone"}
    branch, tip, disposition, kind, ref = parsed.groups()
    if disposition == "abandoned" and kind != "bundle":
        return {"error": "abandoned_requires_bundle"}
    if kind in ("accepted_plan", "content_relocated"):
        path, separator, blob = ref.rpartition("@")
        if not separator or not _valid_relpath(path) or _HEX40.fullmatch(blob) is None:
            return {"error": "malformed_tombstone_evidence"}
    elif kind == "superseded_by_integration":
        if _HEX40.fullmatch(ref) is None:
            return {"error": "malformed_tombstone_evidence"}
    elif not _valid_relpath(ref):
        return {"error": "malformed_tombstone_evidence"}
    return Tombstone(branch, tip, disposition, kind, ref)


def parse_tombstones(message: str) -> list[Tombstone | dict[str, str]]:
    """Parse every tombstone trailer in the final paragraph, retaining typed errors."""

    lines = _final_paragraph(message)
    if lines is None:
        return []
    tombstones: list[Tombstone | dict[str, str]] = []
    for line in lines:
        if line.startswith(KEY_TOMBSTONE):
            tombstones.append(_parse_tombstone_line(line))
    return tombstones


def _run_git(repo: str | Path, *args: str, timeout_s: float = _GIT_TIMEOUT_S) -> subprocess.CompletedProcess[str]:
    from workbay_handoff_mcp.shared_write_context import run_subprocess

    return run_subprocess(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_s,
    )


def _resolve_commit(repo: str | Path, revision: str, timeout_s: float) -> tuple[str | None, str | None, int | None]:
    proc = _run_git(
        repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{revision}^{{commit}}", timeout_s=timeout_s
    )
    if proc.returncode == 0:
        value = proc.stdout.strip().splitlines()
        return (value[0], None, 0) if value else (None, "git_failed:0", 0)
    if proc.returncode != 1:
        return None, f"git_failed:{proc.returncode}", proc.returncode
    return None, None, proc.returncode


def _call_timeout(timeout_s: float, deadline: float | None) -> float:
    """Recompute one blocking call's allowance from the earliest absolute ``deadline``."""
    if deadline is None:
        return timeout_s
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired("git", 0)
    return min(timeout_s, remaining)


def find_carrier(repo: str | Path, commit: str, *, main_ref: str = "main", timeout_s: float = 10.0) -> CarrierResult:
    """Find the earliest first-parent commit whose ancestry contains ``commit``."""
    return _find_carrier(repo, commit, main_ref=main_ref, timeout_s=timeout_s, deadline=None)


def _find_carrier(
    repo: str | Path, commit: str, *, main_ref: str, timeout_s: float, deadline: float | None
) -> CarrierResult:
    try:
        resolved_commit, commit_error, commit_rc = _resolve_commit(repo, commit, _call_timeout(timeout_s, deadline))
        if commit_error:
            return CarrierResult(commit, main_ref, None, False, None, None, commit_error)
        if resolved_commit is None:
            return CarrierResult(commit, main_ref, None, False, None, None, "commit_missing")
        resolved_main, main_error, main_rc = _resolve_commit(repo, main_ref, _call_timeout(timeout_s, deadline))
        if main_error:
            return CarrierResult(resolved_commit, main_ref, None, False, None, None, main_error)
        if resolved_main is None:
            return CarrierResult(resolved_commit, main_ref, None, False, None, None, "main_missing")
        if resolved_commit == resolved_main:
            return CarrierResult(resolved_commit, main_ref, resolved_main, True, resolved_commit, "first_parent", None)

        ancestry = _run_git(
            repo,
            "merge-base",
            "--is-ancestor",
            resolved_commit,
            resolved_main,
            timeout_s=_call_timeout(timeout_s, deadline),
        )
        if ancestry.returncode == 1:
            return CarrierResult(resolved_commit, main_ref, resolved_main, False, None, None, None)
        if ancestry.returncode != 0:
            return CarrierResult(
                resolved_commit, main_ref, resolved_main, False, None, None, f"git_failed:{ancestry.returncode}"
            )

        first_parent = _run_git(
            repo,
            "rev-list",
            "--first-parent",
            f"{resolved_commit}..{resolved_main}",
            timeout_s=_call_timeout(timeout_s, deadline),
        )
        if first_parent.returncode != 0:
            return CarrierResult(
                resolved_commit, main_ref, resolved_main, False, None, None, f"git_failed:{first_parent.returncode}"
            )
        # Keep this as a separate query. Combining --first-parent and
        # --ancestry-path drops valid nested lane ancestors on supported Git.
        ancestry_path = _run_git(
            repo,
            "rev-list",
            "--ancestry-path",
            f"{resolved_commit}..{resolved_main}",
            timeout_s=_call_timeout(timeout_s, deadline),
        )
        if ancestry_path.returncode != 0:
            return CarrierResult(
                resolved_commit, main_ref, resolved_main, False, None, None, f"git_failed:{ancestry_path.returncode}"
            )
        ancestry_commits = set(ancestry_path.stdout.split())
        candidates = [sha for sha in first_parent.stdout.split() if sha in ancestry_commits]
        if not candidates:
            return CarrierResult(resolved_commit, main_ref, resolved_main, False, None, None, "git_failed:no_carrier")
        oldest = candidates[-1]
        first_parent_of_oldest = _run_git(
            repo, "rev-parse", "--verify", "--quiet", f"{oldest}^1", timeout_s=_call_timeout(timeout_s, deadline)
        )
        if first_parent_of_oldest.returncode != 0:
            return CarrierResult(
                resolved_commit,
                main_ref,
                resolved_main,
                False,
                None,
                None,
                f"git_failed:{first_parent_of_oldest.returncode}",
            )
        if first_parent_of_oldest.stdout.strip() == resolved_commit:
            return CarrierResult(resolved_commit, main_ref, resolved_main, True, resolved_commit, "first_parent", None)
        return CarrierResult(resolved_commit, main_ref, resolved_main, True, oldest, "merge_carried", None)
    except subprocess.TimeoutExpired:
        return CarrierResult(commit, main_ref, None, False, None, None, "timeout")
    except OSError as exc:
        return CarrierResult(commit, main_ref, None, False, None, None, f"git_failed:{exc}")


def _read_commit_message(
    repo: str | Path, commit: str, timeout_s: float = _GIT_TIMEOUT_S
) -> tuple[str | None, str | None]:
    try:
        proc = _run_git(repo, "show", "-s", "--format=%B", commit, timeout_s=timeout_s)
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except OSError as exc:
        return None, f"git_failed:{exc}"
    if proc.returncode != 0:
        return None, f"git_failed:{proc.returncode}"
    return proc.stdout, None


def receipt_tip_for_landing(repo: str | Path, commit: str, *, task_ref: str, lane_id: str) -> str | None:
    """Return the lane tip from a matching, two-parent landing receipt commit."""
    message, read_error = _read_commit_message(repo, commit)
    if read_error:
        return None
    parsed = parse_landing_receipt(message or "")
    if not isinstance(parsed, LandingReceipt) or parsed.task_ref != task_ref or parsed.lane_id != lane_id:
        return None
    topology = _run_git(repo, "rev-list", "--parents", "-n", "1", commit, timeout_s=_GIT_TIMEOUT_S)
    fields = topology.stdout.split()
    if topology.returncode != 0 or len(fields) != 3 or fields[2] != parsed.tip:
        return None
    return parsed.tip


def _envelope_data(raw: object, key: str) -> tuple[dict[str, Any] | None, str | None]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            return None, str(exc)
    if not isinstance(raw, dict):
        return None, "lookup_returned_non_object"
    if raw.get("ok") is False:
        return None, str(raw.get("data", {}).get("error", "lookup_not_ok"))
    data = raw.get("data", raw)
    if not isinstance(data, dict):
        return None, "lookup_data_not_object"
    value = data.get(key)
    if value is None and key == "lane":
        value = data.get("row")
    if value is None and key == "test":
        value = data.get("verified_test")
    if value is None and any(field in data for field in ("branch_tip_sha", "branch", "passed", "commit_sha")):
        value = data
    if value is not None and not isinstance(value, dict):
        return None, "lookup_row_not_object"
    return value, None


def _pin_matches(repo: str | Path, lane_id: str, tip: str, timeout_s: float) -> tuple[bool, str | None]:
    pin = f"refs/reclaimed/{lane_id}/{tip}"
    proc = _run_git(
        repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{pin}^{{commit}}", timeout_s=timeout_s
    )
    if proc.returncode == 0:
        return proc.stdout.strip() == tip, None
    if proc.returncode == 1:
        return False, None
    return False, f"git_failed:{proc.returncode}"


def _lane_row_not_found(raw: object, task_ref: str, lane_id: str) -> bool:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return False
    if not isinstance(raw, dict) or raw.get("ok") is not False:
        return False
    data = raw.get("data")
    return isinstance(data, dict) and data.get("error") == f"Lane '{lane_id}' not found for task '{task_ref}'."


def _lane_identity_matches(
    repo: str | Path, row: dict[str, Any], tip: str, timeout_s: float
) -> tuple[Literal["match", "contradict", "defer_to_pin"], str | None]:
    registered_tip = row.get("branch_tip_sha")
    defer_stale_registration = False
    if registered_tip not in (None, ""):
        if registered_tip == tip:
            return "match", None
        defer_stale_registration = row.get("branch_tip_source") == "registration"
        if not defer_stale_registration and isinstance(registered_tip, str) and _HEX40.fullmatch(registered_tip):
            ancestry = _run_git(repo, "merge-base", "--is-ancestor", registered_tip, tip, timeout_s=timeout_s)
            if ancestry.returncode == 0:
                defer_stale_registration = True
        if not defer_stale_registration:
            return "contradict", None
    branch = row.get("branch")
    if not isinstance(branch, str) or not branch:
        return ("defer_to_pin" if defer_stale_registration else "contradict"), None
    ref = branch if branch.startswith("refs/") else f"refs/heads/{branch}"
    proc = _run_git(
        repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}", timeout_s=timeout_s
    )
    if proc.returncode == 0:
        live_tip = proc.stdout.strip()
        if live_tip == tip:
            return "match", None
        ancestry = _run_git(repo, "merge-base", "--is-ancestor", tip, live_tip, timeout_s=timeout_s)
        if ancestry.returncode == 0:
            return "defer_to_pin", None
        if ancestry.returncode == 1:
            return ("defer_to_pin" if defer_stale_registration else "contradict"), None
        return "contradict", f"git_failed:{ancestry.returncode}"
    if proc.returncode == 1:
        return ("defer_to_pin" if defer_stale_registration else "contradict"), None
    return "contradict", f"git_failed:{proc.returncode}"


def verify_receipt(
    repo: str | Path,
    commit: str,
    *,
    main_ref: str = "main",
    lane_lookup: Callable[[str, str], object] | None = None,
    gate_lookup: Callable[[int], object] | None = None,
) -> ReceiptVerdict:
    """Verify receipt position, merge topology, lane identity, and gate binding."""

    message, read_error = _read_commit_message(repo, commit)
    if read_error:
        return ReceiptVerdict(commit, None, False, "v1_probe_failed", None, read_error)
    parsed = parse_landing_receipt(message or "")
    if isinstance(parsed, dict):
        return ReceiptVerdict(commit, None, False, parsed["error"], None, None)
    if parsed is None:
        return ReceiptVerdict(commit, None, False, "missing_receipt", None, None)

    carrier = find_carrier(repo, commit, main_ref=main_ref)
    if carrier.error:
        return ReceiptVerdict(commit, parsed, False, "v1_probe_failed", None, carrier.error)
    if not carrier.contained:
        return ReceiptVerdict(commit, parsed, False, "not_on_main", None, None)

    depth = 0
    current_ref = main_ref
    position = carrier
    while position.lineage == "merge_carried":
        depth += 1
        if depth > 3:
            return ReceiptVerdict(commit, parsed, False, "nesting_depth", depth, None)
        if position.carrier is None:
            return ReceiptVerdict(commit, parsed, False, "v1_probe_failed", depth, "carrier_missing")
        try:
            second_parent = _run_git(
                repo, "rev-parse", "--verify", "--quiet", f"{position.carrier}^2", timeout_s=_GIT_TIMEOUT_S
            )
        except subprocess.TimeoutExpired:
            return ReceiptVerdict(commit, parsed, False, "v1_probe_failed", depth, "timeout")
        except OSError as exc:
            return ReceiptVerdict(commit, parsed, False, "v1_probe_failed", depth, str(exc))
        if second_parent.returncode != 0:
            return ReceiptVerdict(
                commit, parsed, False, "v1_probe_failed", depth, f"git_failed:{second_parent.returncode}"
            )
        current_ref = second_parent.stdout.strip()
        position = find_carrier(repo, commit, main_ref=current_ref)
        if position.error:
            return ReceiptVerdict(commit, parsed, False, "v1_probe_failed", depth, position.error)
        if not position.contained:
            return ReceiptVerdict(commit, parsed, False, "not_on_main", depth, None)
    if position.lineage != "first_parent":
        return ReceiptVerdict(commit, parsed, False, "v1_probe_failed", depth, "unknown_lineage")

    try:
        topology = _run_git(repo, "rev-list", "--parents", "-n", "1", commit, timeout_s=_GIT_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return ReceiptVerdict(commit, parsed, False, "v2_probe_failed", depth, "timeout")
    except OSError as exc:
        return ReceiptVerdict(commit, parsed, False, "v2_probe_failed", depth, str(exc))
    if topology.returncode != 0:
        return ReceiptVerdict(commit, parsed, False, "v2_probe_failed", depth, f"git_failed:{topology.returncode}")
    fields = topology.stdout.split()
    if len(fields) != 3 or fields[2] != parsed.tip:
        return ReceiptVerdict(commit, parsed, False, "v2_topology", depth, None)

    if lane_lookup is None:
        try:
            from workbay_handoff_mcp.lanes_recording import get_lane  # noqa: PLC0415
            from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415

            def default_lane_lookup(task: str, lane: str) -> object:
                return get_lane(task_ref=task, lane_id=lane)

            lane_lookup = default_lane_lookup
        except Exception as exc:  # noqa: BLE001 - an unavailable seam is an unknown result
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, str(exc))
    else:
        from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415

    try:
        raw_lane = lane_lookup(parsed.task_ref, parsed.lane_id)
        lane_row, lane_error = _envelope_data(raw_lane, "lane")
        if lane_error and _lane_row_not_found(raw_lane, parsed.task_ref, parsed.lane_id):
            lane_error = None
    except RuntimeNotConfiguredError as exc:
        return ReceiptVerdict(commit, parsed, False, "runtime_unconfigured", depth, str(exc))
    except Exception as exc:  # noqa: BLE001 - callers must report lookup failures as unknown
        return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, str(exc))
    if lane_error:
        return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, lane_error)
    lane_matches = False
    if lane_row is not None:
        try:
            lane_identity, branch_error = _lane_identity_matches(repo, lane_row, parsed.tip, _GIT_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, "timeout")
        except OSError as exc:
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, str(exc))
        if branch_error:
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, branch_error)
        if lane_identity == "contradict":
            return ReceiptVerdict(commit, parsed, False, "v3_identity", depth, None)
        lane_matches = lane_identity == "match"
    if not lane_matches:
        try:
            lane_matches, pin_error = _pin_matches(repo, parsed.lane_id, parsed.tip, _GIT_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, "timeout")
        except OSError as exc:
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, str(exc))
        if pin_error:
            return ReceiptVerdict(commit, parsed, False, "v3_lookup_failed", depth, pin_error)
    if not lane_matches:
        return ReceiptVerdict(commit, parsed, False, "v3_identity", depth, None)

    if gate_lookup is None:
        try:
            from workbay_handoff_mcp.verified_tests import get_verified_test_by_id  # noqa: PLC0415

            gate_lookup = get_verified_test_by_id
        except Exception as exc:  # noqa: BLE001 - an unavailable seam is an unknown result
            return ReceiptVerdict(commit, parsed, False, "v4_lookup_failed", depth, str(exc))
    try:
        gate_row, gate_error = _envelope_data(gate_lookup(parsed.gate_id), "test")
    except Exception as exc:  # noqa: BLE001 - callers must report lookup failures as unknown
        return ReceiptVerdict(commit, parsed, False, "v4_lookup_failed", depth, str(exc))
    if gate_error:
        return ReceiptVerdict(commit, parsed, False, "v4_lookup_failed", depth, gate_error)
    if not gate_row or gate_row.get("passed") not in (True, 1) or gate_row.get("commit_sha") != parsed.tip:
        return ReceiptVerdict(commit, parsed, False, "v4_gate", depth, None)
    return ReceiptVerdict(commit, parsed, True, None, depth, None)


def _blob_at_path(repo: str | Path, main_ref: str, path: str, blob: str, timeout_s: float) -> tuple[bool, str | None]:
    try:
        proc = _run_git(
            repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{main_ref}:{path}", timeout_s=timeout_s
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except OSError as exc:
        return False, str(exc)
    except ValueError as exc:
        return False, str(exc)
    if proc.returncode == 0:
        return proc.stdout.strip() == blob, None
    if proc.returncode == 1:
        return False, None
    return False, f"git_failed:{proc.returncode}"


_VERIFICATION_DEADLINE: ContextVar[float | None] = ContextVar("tombstone_verification_deadline", default=None)


@contextmanager
def tombstone_verification_deadline(deadline: float | None) -> Iterator[None]:
    """Bound every :func:`verify_tombstone` in this context by a monotonic ``deadline``.

    Nested scopes keep the earlier deadline, so a caller's remaining budget
    also bounds verifications made by modules it calls.
    """
    outer = _VERIFICATION_DEADLINE.get()
    effective = deadline if outer is None else outer if deadline is None else min(outer, deadline)
    token = _VERIFICATION_DEADLINE.set(effective)
    try:
        yield
    finally:
        _VERIFICATION_DEADLINE.reset(token)


def _carrier_successor(message: str) -> tuple[str, str] | None | str:
    """Return the carrier's landing identity, ``None`` for a non-landing commit, or a refusal."""
    parsed = parse_landing_receipt(message)
    if isinstance(parsed, LandingReceipt):
        return parsed.task_ref, parsed.lane_id
    task_ref, lane_id, _tip = _partial_receipt_identity(message)
    if task_ref and lane_id:
        return task_ref, lane_id
    if parsed is not None or any(
        line.startswith(f"{key}:") for line in message.splitlines() for key in _KNOWN_RECEIPT_KEYS
    ):
        return "declaration_untraceable"
    return None


def supersede_plan_digest(
    *,
    branch_ref: str,
    branch_tip: str,
    carrier_sha: str,
    integration_ref: str,
    integration_tip: str,
    tombstone_line: str,
) -> str:
    """Hash the canonical set of facts a reviewed supersede apply records."""
    plan = {
        "branch_ref": branch_ref,
        "branch_tip": branch_tip,
        "carrier_sha": carrier_sha,
        "integration_ref": integration_ref,
        "integration_tip": integration_tip,
        "tombstone_line": tombstone_line,
    }
    canonical = json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def format_supersede_plan(*, plan_digest: str, integration_ref: str) -> str:
    """Format the reviewed-operator trailer that accompanies a tombstone in its CAS commit."""
    line = f"{KEY_SUPERSEDE_PLAN}: sha256:{plan_digest} {integration_ref}"
    if _SUPERSEDE_PLAN_VALUE.fullmatch(line.removeprefix(f"{KEY_SUPERSEDE_PLAN}: ")) is None:
        raise ValueError("supersede plan requires a sha256 digest and a local integration branch ref")
    return line


def _reviewed_operator_plan(
    repo: str | Path, tombstone: Tombstone, *, main_ref: str, deadline: float
) -> tuple[str, str | None] | None:
    """Return ``None`` when a first-parent commit binds this tombstone to its reviewed plan.

    The binding is the plan trailer published by the tombstone's own CAS
    commit. Its digest is recomputed from the tombstone's branch, exact tip and
    carrier plus that commit's first parent, the integration tip the reviewed
    plan named. Otherwise returns ``(failed_check, detail)``: ``("absent",
    None)`` when no record carries a binding, a typed invalid or mismatched
    binding, or ``probe_failed``.
    """
    try:
        line = format_tombstone(
            branch=tombstone.branch,
            tip=tombstone.tip,
            disposition=tombstone.disposition,
            kind=tombstone.kind,
            ref=tombstone.ref,
        )
        proc = _run_git(
            repo,
            "log",
            "--first-parent",
            "--fixed-strings",
            f"--grep={line}",
            "--format=%H%x00%P%x00%B%x1e",
            f"{tombstone.ref}..{main_ref}",
            "--",
            timeout_s=_call_timeout(_GIT_TIMEOUT_S, deadline),
        )
    except ValueError:
        return "absent", None
    except subprocess.TimeoutExpired:
        return "probe_failed", "supersede_operator_timeout"
    except OSError as exc:
        return "probe_failed", f"supersede_operator_git_failed:{exc}"
    if proc.returncode != 0:
        return "probe_failed", f"supersede_operator_git_failed:{proc.returncode}"
    branch_ref = tombstone.branch if tombstone.branch.startswith("refs/heads/") else f"refs/heads/{tombstone.branch}"
    refusal: tuple[str, str | None] = ("absent", None)
    for record in proc.stdout.split("\x1e"):
        fields = record.lstrip("\n").split("\x00", 2)
        if len(fields) != 3:
            continue
        commit, parents, message = fields
        trailers = [entry.rstrip() for entry in _final_paragraph(message) or []]
        if line not in trailers:
            continue
        plans = [entry for entry in trailers if entry.startswith(f"{KEY_SUPERSEDE_PLAN}:")]
        if not plans:
            continue  # An unreviewed record of the same tombstone proves nothing.
        parsed = _SUPERSEDE_PLAN_VALUE.fullmatch(plans[0].removeprefix(f"{KEY_SUPERSEDE_PLAN}:").strip())
        if len(plans) != 1 or parsed is None:
            refusal = refusal if refusal[0] != "absent" else ("operator_provenance_invalid", commit.strip())
            continue
        digest, integration_ref = parsed.groups()
        parent = parents.split()[0] if parents.split() else ""
        expected = supersede_plan_digest(
            branch_ref=branch_ref,
            branch_tip=tombstone.tip,
            carrier_sha=tombstone.ref,
            integration_ref=integration_ref,
            integration_tip=parent,
            tombstone_line=line,
        )
        if expected == digest:
            return None
        refusal = refusal if refusal[0] != "absent" else ("operator_provenance_mismatch", commit.strip())
    return refusal


def _landing_declaration_refusal(
    repo: str | Path, tombstone: Tombstone, task_ref: str, lane_id: str, deadline: float
) -> TombstoneVerdict | None:
    """Return ``None`` only when the landing successor's declaration pins this tombstone."""
    from . import lane_supersede_auth

    detail = f"{task_ref}/{lane_id}"
    manifest = lane_supersede_auth.read_declaration_manifest(repo, task_ref, deadline=deadline)
    if isinstance(manifest, lane_supersede_auth.ManifestUnavailable):
        if manifest.reason == "manifest_missing":
            return TombstoneVerdict(tombstone, False, "declaration_manifest_missing", detail)
        reason = "manifest_invalid" if manifest.reason == "manifest_task_ref_mismatch" else manifest.reason
        return TombstoneVerdict(tombstone, False, "probe_failed", f"supersede_declaration_{reason}")
    refused = lane_supersede_auth.check_landing_tombstone(
        manifest, lane_id, branch=tombstone.branch.removeprefix("refs/heads/"), tip=tombstone.tip
    )
    if refused is not None:
        return TombstoneVerdict(tombstone, False, refused.reason, detail)
    return None


def _declared_supersede_authority(
    repo: str | Path, verdict: TombstoneVerdict, deadline: float, *, main_ref: str
) -> TombstoneVerdict:
    """Grant a carrier tombstone authority only from positive, durable evidence.

    Two records authorize retirement. ``lane_land`` names its receipt-bearing
    landing commit as the carrier, and the successor's declaration must pin
    this branch at this tip. The plan-digest-bound operator contract
    (``manage_lane`` supersede or the ``lane_tombstone`` CLI) publishes its
    reviewed plan binding in the tombstone's own CAS commit. A carrier without
    a landing receipt is not consent, and a missing manifest, entry,
    declaration, or binding is refused by name.
    """
    tombstone = verdict.tombstone
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return TombstoneVerdict(tombstone, False, "probe_failed", "supersede_declaration_deadline_exhausted")
    message, read_error = _read_commit_message(repo, tombstone.ref, min(_GIT_TIMEOUT_S, remaining))
    if read_error:
        return TombstoneVerdict(tombstone, False, "probe_failed", f"supersede_carrier_{read_error}")
    successor = _carrier_successor(message or "")
    if successor is None:
        refusal = TombstoneVerdict(tombstone, False, "operator_provenance_missing", tombstone.ref)
    elif isinstance(successor, str):
        refusal = TombstoneVerdict(tombstone, False, successor, tombstone.ref)
    else:
        declared = _landing_declaration_refusal(repo, tombstone, *successor, deadline)
        if declared is None:
            return verdict
        refusal = declared
    if refusal.failed_check == "probe_failed":
        return refusal
    operator = _reviewed_operator_plan(repo, tombstone, main_ref=main_ref, deadline=deadline)
    if operator is None:
        return verdict
    failed_check, detail = operator
    if failed_check == "absent":
        return refusal
    return TombstoneVerdict(tombstone, False, failed_check, detail)


def verify_tombstone(
    repo: str | Path,
    tombstone: Tombstone,
    *,
    main_ref: str = "main",
    current_tip: str,
) -> TombstoneVerdict:
    """Re-check a tombstone's evidence, current branch tip, and supersede authority on every use.

    Every consumer (retire disposition, tombstone writer, staging reaper, and
    landing replay) shares this one decision. One earliest deadline, from the
    active :func:`tombstone_verification_deadline` or else the Git probe
    timeout, bounds every Git probe and the declaration read.
    """

    started = time.monotonic()
    scoped = _VERIFICATION_DEADLINE.get()
    deadline = started + _GIT_TIMEOUT_S if scoped is None else min(scoped, started + _GIT_TIMEOUT_S)
    verdict = _verify_tombstone_evidence(repo, tombstone, main_ref=main_ref, current_tip=current_tip, deadline=deadline)
    if verdict.verified and tombstone.kind == "superseded_by_integration":
        return _declared_supersede_authority(repo, verdict, deadline, main_ref=main_ref)
    return verdict


def _verify_tombstone_evidence(
    repo: str | Path,
    tombstone: Tombstone,
    *,
    main_ref: str,
    current_tip: str,
    deadline: float,
) -> TombstoneVerdict:
    if tombstone.tip != current_tip:
        return TombstoneVerdict(tombstone, False, "tip_moved", None)
    try:
        if tombstone.kind in ("accepted_plan", "content_relocated"):
            path, separator, blob = tombstone.ref.rpartition("@")
            if not separator or _HEX40.fullmatch(blob) is None or not _valid_relpath(path):
                return TombstoneVerdict(tombstone, False, "evidence_missing", None)
            found, error = _blob_at_path(repo, main_ref, path, blob, _call_timeout(_GIT_TIMEOUT_S, deadline))
            if error:
                return TombstoneVerdict(tombstone, False, "probe_failed", error)
            return TombstoneVerdict(tombstone, found, None if found else "evidence_missing", None)
        if tombstone.kind == "superseded_by_integration":
            if _HEX40.fullmatch(tombstone.ref) is None:
                return TombstoneVerdict(tombstone, False, "evidence_missing", None)
            carrier = _find_carrier(repo, tombstone.ref, main_ref=main_ref, timeout_s=_GIT_TIMEOUT_S, deadline=deadline)
            if carrier.error == "commit_missing":
                return TombstoneVerdict(tombstone, False, "evidence_missing", None)
            if carrier.error:
                return TombstoneVerdict(tombstone, False, "probe_failed", carrier.error)
            found = carrier.contained and carrier.lineage == "first_parent" and carrier.carrier == carrier.commit
            return TombstoneVerdict(tombstone, bool(found), None if found else "evidence_missing", None)
        if tombstone.kind == "bundle":
            if not _valid_relpath(tombstone.ref):
                return TombstoneVerdict(tombstone, False, "evidence_missing", None)
            bundle = Path(repo) / tombstone.ref
            if not bundle.is_file():
                return TombstoneVerdict(tombstone, False, "evidence_missing", None)
            try:
                proc = _run_git(
                    repo, "bundle", "list-heads", str(bundle), timeout_s=_call_timeout(_GIT_TIMEOUT_S, deadline)
                )
            except subprocess.TimeoutExpired:
                return TombstoneVerdict(tombstone, False, "probe_failed", "timeout")
            except OSError as exc:
                return TombstoneVerdict(tombstone, False, "probe_failed", str(exc))
            if proc.returncode != 0:
                return TombstoneVerdict(tombstone, False, "probe_failed", f"git_failed:{proc.returncode}")
            found = any(line.split() and line.split()[0] == tombstone.tip for line in proc.stdout.splitlines())
            return TombstoneVerdict(tombstone, found, None if found else "evidence_missing", None)
    except subprocess.TimeoutExpired:
        return TombstoneVerdict(tombstone, False, "probe_failed", "timeout")
    except OSError as exc:
        return TombstoneVerdict(tombstone, False, "probe_failed", str(exc))
    return TombstoneVerdict(tombstone, False, "evidence_missing", None)


def scan_first_parent(
    repo: str | Path, *, main_ref: str = "main", since: str | None = None, timeout_s: float = 10.0
) -> ScanResult:
    """Read a bounded first-parent log scan; malformed evidence remains visible."""

    started = time.monotonic()
    revision = f"{since}..{main_ref}" if since else main_ref
    try:
        proc = _run_git(
            repo,
            "log",
            "--first-parent",
            "--format=%H%x00%P%x00%B%x1e",
            revision,
            timeout_s=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return ScanResult(False, (), (), (), 0, time.monotonic() - started, "scan_timeout")
    except OSError as exc:
        return ScanResult(False, (), (), (), 0, time.monotonic() - started, f"scan_failed:{exc}")
    if proc.returncode != 0:
        return ScanResult(False, (), (), (), 0, time.monotonic() - started, f"scan_failed:{proc.returncode}")

    receipts: list[tuple[str, LandingReceipt]] = []
    tombstones: list[tuple[str, Tombstone]] = []
    unverified: list[tuple[str, str]] = []
    unverified_receipts: list[UnverifiedReceipt] = []
    ordered_receipts: list[tuple[str, LandingReceipt | UnverifiedReceipt]] = []
    scanned = 0
    for record in proc.stdout.split("\x1e"):
        record = record.lstrip("\n")
        if not record.strip():
            continue
        fields = record.split("\x00", 2)
        if len(fields) != 3:
            return ScanResult(
                False,
                tuple(receipts),
                tuple(tombstones),
                tuple(unverified),
                scanned,
                time.monotonic() - started,
                "scan_malformed_output",
            )
        commit, _parents, message = fields
        commit = commit.strip()
        if not _HEX40.fullmatch(commit):
            return ScanResult(
                False,
                tuple(receipts),
                tuple(tombstones),
                tuple(unverified),
                scanned,
                time.monotonic() - started,
                "scan_malformed_output",
            )
        scanned += 1
        parsed = parse_landing_receipt(message)
        if isinstance(parsed, LandingReceipt):
            receipts.append((commit, parsed))
            ordered_receipts.append((commit, parsed))
        elif isinstance(parsed, dict):
            reason = parsed["error"]
            unverified.append((commit, reason))
            task_ref, lane_id, tip = _partial_receipt_identity(message)
            candidate = UnverifiedReceipt(commit, task_ref, lane_id, tip, reason)
            unverified_receipts.append(candidate)
            ordered_receipts.append((commit, candidate))
        for parsed_tombstone in parse_tombstones(message):
            if isinstance(parsed_tombstone, Tombstone):
                tombstones.append((commit, parsed_tombstone))
            else:
                unverified.append((commit, parsed_tombstone["error"]))
    return ScanResult(
        True,
        tuple(receipts),
        tuple(tombstones),
        tuple(unverified),
        scanned,
        time.monotonic() - started,
        None,
        tuple(unverified_receipts),
        tuple(ordered_receipts),
    )


def find_landing(
    repo: str | Path,
    *,
    task_ref: str,
    lane_id: str,
    tip: str,
    main_ref: str = "main",
    scan: ScanResult | None = None,
) -> ReceiptVerdict | None:
    """Find and verify the (lane, tip) natural-key receipt, newest first.

    A malformed receipt is reported only if it is newer than every parsed
    receipt matching this task, lane, and tip; older malformed entries are
    superseded by the newer receipt.
    """

    result = scan if scan is not None else scan_first_parent(repo, main_ref=main_ref)
    if not result.complete:
        return ReceiptVerdict("", None, False, "scan_timeout", None, result.error)
    ordered_receipts = result.ordered_receipts or tuple(
        [(candidate.commit, candidate) for candidate in result.unverified_receipts] + list(result.receipts)
    )
    for commit, candidate in ordered_receipts:
        if isinstance(candidate, UnverifiedReceipt):
            if candidate.task_ref != task_ref or candidate.lane_id != lane_id:
                continue
            if candidate.tip is not None and candidate.tip != tip:
                continue
            return ReceiptVerdict(commit, None, False, "unverified_receipt", None, candidate.reason)
        if candidate.task_ref == task_ref and candidate.lane_id == lane_id and candidate.tip == tip:
            break
    matches = [
        (commit, receipt)
        for commit, receipt in result.receipts
        if receipt.task_ref == task_ref and receipt.lane_id == lane_id and receipt.tip == tip
    ]
    unverified: ReceiptVerdict | None = None
    for commit, _receipt in matches:
        verdict = verify_receipt(repo, commit, main_ref=main_ref)
        if verdict.verified:
            return verdict
        if unverified is None:
            unverified = verdict
    return unverified


def _jsonable(value: object) -> object:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _records(cursor: Any) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _read_migration_state(gate_ids: set[int]) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]], str | None]:
    """Read lane and gate evidence without creating or mutating the configured database."""
    try:
        from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415
        from workbay_handoff_mcp.shared_schema import connect_handoff_db  # noqa: PLC0415

        db_path = get_runtime_config().db_path
        if not db_path.exists():
            return [], {}, None
        with connect_handoff_db(str(db_path), read_only=True) as conn:
            rows = _records(
                conn.execute(
                    "SELECT task_ref, lane_id, status, branch_tip_sha, branch_tip_source "
                    "FROM worktree_lanes ORDER BY task_ref, lane_id"
                )
            )
            gates: dict[int, dict[str, Any]] = {}
            gate_list = sorted(gate_ids)
            for offset in range(0, len(gate_list), 800):
                batch = gate_list[offset : offset + 800]
                if not batch:
                    continue
                placeholders = ",".join("?" for _ in batch)
                for row in _records(
                    conn.execute(
                        f"SELECT id, passed, commit_sha FROM verified_tests WHERE id IN ({placeholders})",
                        batch,
                    )
                ):
                    gates[int(row["id"])] = row
    except Exception as exc:  # noqa: BLE001 - an incomplete read never authorizes a migration
        return [], {}, f"state_read_failed:{type(exc).__name__}:{exc}"
    return rows, gates, None


def _migration_gate_lookup(gates: dict[int, dict[str, Any]]) -> Callable[[int], object]:
    def lookup(gate_id: int) -> object:
        row = gates.get(gate_id)
        if row is None:
            return {"ok": False, "data": {"error": "gate_not_found"}}
        return {"ok": True, "test": row}

    return lookup


def _receipt_v1_v2_v4_verified(
    repo: str | Path,
    commit: str,
    receipt: LandingReceipt,
    *,
    main_ref: str,
    gate_lookup: Callable[[int], object],
) -> bool:
    verdict = verify_receipt(
        repo,
        commit,
        main_ref=main_ref,
        lane_lookup=lambda _task, _lane: {"ok": True, "lane": {"branch_tip_sha": receipt.tip}},
        gate_lookup=gate_lookup,
    )
    return verdict.verified


@dataclass(frozen=True)
class ReceiptTipRepairResult:
    status: Literal["refused", "would_repair", "repaired", "already_repaired"]
    reason: str
    evidence: dict[str, Any] | None = None


def _repair_state(task_ref: str, lane_id: str, gate_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    from workbay_handoff_mcp.runtime import get_runtime_config
    from workbay_handoff_mcp.shared_schema import connect_handoff_db

    with connect_handoff_db(str(get_runtime_config().db_path), read_only=True) as conn:
        lane = conn.execute(
            "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?", (task_ref, lane_id)
        ).fetchone()
        gate = conn.execute("SELECT * FROM verified_tests WHERE id = ?", (gate_id,)).fetchone()
        dispatch = conn.execute(
            "SELECT 1 FROM lane_messages WHERE task_ref = ? AND lane_id = ? "
            "AND direction = 'orchestrator_to_worker' AND status = 'open' AND dispatch_id IS NOT NULL LIMIT 1",
            (task_ref, lane_id),
        ).fetchone()
        if dispatch is not None:
            raise ValueError("open_dispatch")
        if lane is None or gate is None:
            raise ValueError("missing_lane_or_gate")
        return dict(lane), dict(gate)


def _repair_ownership(repo: Path, row: dict[str, Any]) -> None:
    from workbay_orchestrator_mcp import lane_reaping as guards

    raw_path = row.get("worktree_path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError("owner_path_unknown")
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = repo / path
    # Do not turn an inaccessible or surviving tree into branch-only evidence.
    try:
        path.stat()
    except FileNotFoundError:
        guard_path = None
    else:
        guard_path = str(path)
    state, detail = guards._probe_worktree_process_owner(str(path))
    if state != "free":
        raise ValueError(f"owner_{state}:{detail}")
    blocked, detail = guards._session_heartbeat_blocks_reclaim(repo_root=repo, worktree_path=str(path))
    if blocked:
        raise ValueError(f"session_live_or_unknown:{detail}")
    blocked, detail = guards._shared_path_blocks_reclaim(
        worktree_path=str(path),
        task_ref=row["task_ref"],
        lane_id=row["lane_id"],
        repo_root=repo,
    )
    if blocked:
        raise ValueError(f"shared_path_owner:{detail}")
    refusal, detail = guards._under_claim_worktree_reclaim_guards(
        worktree_path=guard_path,
        branch=row.get("branch"),
        repo_root=repo,
        task_ref=row["task_ref"],
        lane_id=row["lane_id"],
    )
    if refusal:
        raise ValueError(f"{refusal}:{detail}")


def _repair_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def repair_receipt_backed_branch_tip(
    repo: str | Path,
    *,
    task_ref: str,
    lane_id: str,
    run_id: str,
    receipt_commit: str,
    main_ref: str = "main",
    apply: bool = False,
) -> ReceiptTipRepairResult:
    """Explicit operator repair of an absent branch's stale setup observation.

    Configure the handoff runtime for the target workspace first, then call
    ``repair_receipt_backed_branch_tip(repo, task_ref=..., lane_id=...,
    run_id=<original run>, receipt_commit=<original landing>)`` for a dry run;
    repeat with ``apply=True`` to apply. Never creates a new landing run.
    Both modes take bounded ownership claims; dry run changes no lane row.
    """
    from workbay_handoff_mcp.lanes_recording import LaneReceiptEvidence, repair_lane_receipt_identity

    from workbay_orchestrator_mcp import lane_reaping

    from .orchestrator_lanes import _landing_mutation_lock

    evidence = None
    try:
        if not _HEX40.fullmatch(receipt_commit) or not _HEX32.fullmatch(run_id):
            raise ValueError("invalid_original_identity")
        format_landing_trailers(task_ref=task_ref, lane_id=lane_id, tip=receipt_commit, gate_id=1, run_id=run_id)
        root = Path(repo).resolve()
        with _landing_mutation_lock(root) as acquired:
            if acquired is not True:
                raise ValueError("landing_lock_unavailable")
            handle, detail = lane_reaping._acquire_lane_worker_lock(lane_id)
            if handle is None:
                raise ValueError(detail or "worker_lock_unknown")
            try:
                initial_row = None
                # Re-read all mutable proof under ownership immediately before CAS.
                for _ in range(2):
                    message, error = _read_commit_message(root, receipt_commit)
                    receipt = parse_landing_receipt(message or "")
                    if error or not isinstance(receipt, LandingReceipt):
                        raise ValueError("invalid_receipt")
                    if (receipt.task_ref, receipt.lane_id, receipt.run_id) != (task_ref, lane_id, run_id):
                        raise ValueError("original_receipt_identity_mismatch")
                    row, gate = _repair_state(task_ref, lane_id, receipt.gate_id)
                    if initial_row is not None and row != initial_row:
                        raise ValueError("row_changed")
                    initial_row = row
                    if row.get("status") != "merged" or row.get("landing_commit_sha") != receipt_commit:
                        raise ValueError("terminal_landing_mismatch")
                    if gate.get("task_ref") != task_ref or gate.get("lane_id") not in (None, lane_id):
                        raise ValueError("gate_identity_mismatch")
                    if not _receipt_v1_v2_v4_verified(
                        root,
                        receipt_commit,
                        receipt,
                        main_ref=main_ref,
                        gate_lookup=lambda _id: {"ok": True, "test": gate},
                    ):
                        raise ValueError("receipt_proof_failed")
                    branch = row.get("branch")
                    if not isinstance(branch, str) or not branch:
                        raise ValueError("branch_unknown")
                    ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
                    if _run_git(root, "check-ref-format", ref).returncode != 0:
                        raise ValueError("branch_invalid")
                    # for-each-ref distinguishes missing refs from dangling/non-commit refs.
                    refs = _run_git(root, "for-each-ref", "--format=%(refname)", ref)
                    if refs.returncode != 0 or refs.stdout.strip():
                        raise ValueError("branch_present_or_unknown")
                    pin, pin_error = _pin_matches(root, lane_id, receipt.tip, _GIT_TIMEOUT_S)
                    if not pin or pin_error:
                        raise ValueError("reclaimed_pin_mismatch")
                    _repair_ownership(root, row)
                    event = _run_git(root, "show", "-s", "--format=%cI", receipt_commit)
                    product = _run_git(root, "show", "-s", "--format=%cI", receipt.tip)
                    if event.returncode or product.returncode:
                        raise ValueError("proof_time_unknown")
                    event_time = _repair_time(event.stdout.strip())
                    stamp = event_time.strftime("%Y-%m-%d %H:%M:%S")
                    evidence = LaneReceiptEvidence(
                        task_ref, lane_id, run_id, receipt_commit, receipt.tip, receipt.gate_id, stamp
                    )
                    repaired = (
                        row.get("branch_tip_sha") == receipt.tip
                        and row.get("branch_tip_source") == "backfill"
                        and row.get("branch_tip_observed_at") == stamp
                        and evidence.audit_line() in (row.get("notes") or "").splitlines()
                    )
                    if not repaired:
                        observed = row.get("branch_tip_observed_at")
                        old = row.get("branch_tip_sha")
                        if row.get("branch_tip_source") != "branch" or observed != row.get("created_at"):
                            raise ValueError("not_setup_observation")
                        if not isinstance(old, str) or not _HEX40.fullmatch(old) or old == receipt.tip:
                            raise ValueError("not_strict_ancestor")
                        if _run_git(root, "merge-base", "--is-ancestor", old, receipt.tip).returncode != 0:
                            raise ValueError("not_strict_ancestor")
                        if not all(
                            _repair_time(observed) < proof_time
                            for proof_time in (
                                event_time,
                                _repair_time(product.stdout.strip()),
                                _repair_time(gate["verified_at"]),
                            )
                        ):
                            raise ValueError("setup_observation_not_older")
                # Ownership probes can be slow: sample ref identity again after them.
                refs = _run_git(root, "for-each-ref", "--format=%(refname)", ref)
                pin, pin_error = _pin_matches(root, lane_id, receipt.tip, _GIT_TIMEOUT_S)
                if refs.returncode != 0 or refs.stdout.strip() or not pin or pin_error:
                    raise ValueError("git_identity_changed")
                if repaired:
                    return ReceiptTipRepairResult("already_repaired", "identical_evidence", asdict(evidence))
                if not apply:
                    return ReceiptTipRepairResult("would_repair", "verified", asdict(evidence))
                result = repair_lane_receipt_identity(expected_row=row, evidence=evidence)
                return ReceiptTipRepairResult(
                    "repaired" if result.applied else "refused", result.reason, asdict(evidence)
                )
            finally:
                lane_reaping._release_lane_worker_lock(handle)
    except Exception as exc:  # An incomplete probe never authorizes a historical correction.
        return ReceiptTipRepairResult("refused", f"{type(exc).__name__}:{exc}", asdict(evidence) if evidence else None)


def backfill_branch_tips(
    repo: str | Path,
    *,
    main_ref: str = "main",
    apply: bool = False,
) -> dict[str, Any]:
    """Plan or apply receipt-proven lane tip backfills; planning is read-only."""
    scan = scan_first_parent(repo, main_ref=main_ref)
    if not scan.complete:
        return {"command": "backfill", "mode": "apply" if apply else "dry_run", "complete": False, "error": scan.error}
    lane_rows, gates, state_error = _read_migration_state({receipt.gate_id for _, receipt in scan.receipts})
    if state_error:
        return {
            "command": "backfill",
            "mode": "apply" if apply else "dry_run",
            "complete": False,
            "error": state_error,
        }
    gate_lookup = _migration_gate_lookup(gates)
    planned: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    def is_ancestor(ancestor: str, descendant: str) -> bool | None:
        try:
            relation = _run_git(repo, "merge-base", "--is-ancestor", ancestor, descendant)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if relation.returncode == 0:
            return True
        if relation.returncode == 1:
            return False
        return None

    for row in lane_rows:
        task_ref = row.get("task_ref")
        lane_id = row.get("lane_id")
        if not isinstance(task_ref, str) or not isinstance(lane_id, str):
            skipped.append({"old_value": row, "reason": "invalid_lane_row"})
            continue
        candidate: tuple[str, LandingReceipt] | None = None
        for commit, receipt in scan.receipts:
            if receipt.task_ref != task_ref or receipt.lane_id != lane_id:
                continue
            if _receipt_v1_v2_v4_verified(repo, commit, receipt, main_ref=main_ref, gate_lookup=gate_lookup):
                candidate = (commit, receipt)
                break
        if candidate is None:
            skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "no_v1_v2_v4_receipt"})
            continue
        commit, receipt = candidate
        old_value = {"branch_tip_sha": row.get("branch_tip_sha"), "branch_tip_source": row.get("branch_tip_source")}
        new_value = {"branch_tip_sha": receipt.tip, "branch_tip_source": "backfill"}
        stored_tip = old_value["branch_tip_sha"]
        stored_source = old_value["branch_tip_source"]
        if stored_tip == receipt.tip:
            continue
        if stored_tip not in (None, ""):
            if not isinstance(stored_tip, str) or _HEX40.fullmatch(stored_tip) is None:
                skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "invalid_stored_tip"})
                continue
            stored_is_newer = is_ancestor(receipt.tip, stored_tip)
            if stored_is_newer:
                skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "stored_tip_newer"})
                continue
            # branch and manifest outrank backfill; registration is the
            # explicitly stale legacy source from the earlier registration path.
            source_rank = {None: 0, "registration": 0, "backfill": 1, "manifest": 2, "branch": 3}.get(stored_source, 99)
            if source_rank > 1:
                skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "stored_source_stronger"})
                continue
            if stored_is_newer is None:
                skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "stored_tip_relation_unknown"})
                continue
            stored_is_stale = is_ancestor(stored_tip, receipt.tip)
            if stored_is_stale is None:
                skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "stored_tip_relation_unknown"})
                continue
            if not stored_is_stale and stored_source != "registration":
                skipped.append({"task_ref": task_ref, "lane_id": lane_id, "reason": "stored_tip_not_stale"})
                continue
        planned.append(
            {
                "task_ref": task_ref,
                "lane_id": lane_id,
                "old_value": old_value,
                "new_value": new_value,
                "proof": {
                    "receipt_commit": commit,
                    "receipt_tip": receipt.tip,
                    "checks": ["v1_position", "v2_topology", "v4_gate"],
                    "verified": True,
                },
            }
        )

    result: dict[str, Any] = {
        "command": "backfill",
        "mode": "apply" if apply else "dry_run",
        "complete": True,
        "rows": planned,
        "skipped": skipped,
        "applied": [],
    }
    if not apply:
        return result
    from workbay_handoff_mcp.lanes_recording import update_lane  # noqa: PLC0415

    applied: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for item in planned:
        new_value = item["new_value"]
        raw = update_lane(
            lane_id=item["lane_id"],
            task_ref=item["task_ref"],
            branch_tip_sha=new_value["branch_tip_sha"],
            branch_tip_source="backfill",
            expected_branch_tip_sha=item["old_value"]["branch_tip_sha"],
            expected_branch_tip_source=item["old_value"]["branch_tip_source"],
        )
        lane_row, error = _envelope_data(raw, "lane")
        if error or lane_row is None or lane_row.get("branch_tip_sha") != new_value["branch_tip_sha"]:
            error_row = {
                "task_ref": item["task_ref"],
                "lane_id": item["lane_id"],
                "error": error or "tip_not_persisted",
            }
            error_data = raw
            if isinstance(error_data, str):
                try:
                    error_data = json.loads(error_data)
                except json.JSONDecodeError:
                    error_data = None
            if isinstance(error_data, dict):
                data = error_data.get("data")
                if isinstance(data, dict) and isinstance(data.get("error_kind"), str):
                    error_row["error_kind"] = data["error_kind"]
            errors.append(error_row)
        else:
            applied.append(item)
    result["applied"] = applied
    if errors:
        result["complete"] = False
        result["errors"] = errors
    return result


def _legacy_pin_alias_matches(name: str, receipt: LandingReceipt) -> bool:
    return name in {receipt.lane_id, f"lr01-{receipt.lane_id}", f"rev1-{receipt.task_ref}"}


def _flat_reclaimed_refs(repo: str | Path) -> tuple[list[tuple[str, str]], str | None]:
    proc = _run_git(repo, "for-each-ref", "--format=%(refname)%00%(objectname)", "refs/reclaimed")
    if proc.returncode != 0:
        return [], f"git_failed:{proc.returncode}"
    refs: list[tuple[str, str]] = []
    for line in proc.stdout.splitlines():
        ref, separator, object_id = line.partition("\x00")
        if not separator or not ref.startswith("refs/reclaimed/"):
            continue
        if "/" not in ref.removeprefix("refs/reclaimed/"):
            refs.append((ref, object_id.strip()))
    return refs, None


def _ref_commit(repo: str | Path, ref: str) -> tuple[str | None, str | None]:
    proc = _run_git(repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}")
    if proc.returncode == 0:
        return proc.stdout.strip(), None
    if proc.returncode == 1:
        return None, None
    return None, f"git_failed:{proc.returncode}"


def migrate_legacy_pins(
    repo: str | Path,
    *,
    main_ref: str = "main",
    apply: bool = False,
) -> dict[str, Any]:
    """Plan or idempotently migrate flat legacy pins to canonical lane/tip refs."""
    scan = scan_first_parent(repo, main_ref=main_ref)
    if not scan.complete:
        return {
            "command": "migrate-pins",
            "mode": "apply" if apply else "dry_run",
            "complete": False,
            "error": scan.error,
        }
    _rows, gates, state_error = _read_migration_state({receipt.gate_id for _, receipt in scan.receipts})
    if state_error:
        return {
            "command": "migrate-pins",
            "mode": "apply" if apply else "dry_run",
            "complete": False,
            "error": state_error,
        }
    gate_lookup = _migration_gate_lookup(gates)
    refs, ref_error = _flat_reclaimed_refs(repo)
    if ref_error:
        return {
            "command": "migrate-pins",
            "mode": "apply" if apply else "dry_run",
            "complete": False,
            "error": ref_error,
        }
    planned: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for old_ref, old_object in refs:
        tip, tip_error = _ref_commit(repo, old_ref)
        if tip_error:
            skipped.append({"old_ref": old_ref, "old_value": old_object, "reason": tip_error})
            continue
        if tip is None:
            skipped.append({"old_ref": old_ref, "old_value": old_object, "reason": "pin_not_commit"})
            continue
        name = old_ref.removeprefix("refs/reclaimed/")
        match: tuple[str, LandingReceipt] | None = None
        for commit, receipt in scan.receipts:
            if tip != receipt.tip or not _legacy_pin_alias_matches(name, receipt):
                continue
            if _receipt_v1_v2_v4_verified(repo, commit, receipt, main_ref=main_ref, gate_lookup=gate_lookup):
                match = (commit, receipt)
                break
        if match is None:
            skipped.append({"old_ref": old_ref, "old_value": old_object, "reason": "no_v1_v2_v4_receipt"})
            continue
        commit, receipt = match
        new_ref = f"refs/reclaimed/{receipt.lane_id}/{tip}"
        current_new, new_error = _ref_commit(repo, new_ref)
        if new_error:
            skipped.append({"old_ref": old_ref, "old_value": old_object, "reason": new_error})
            continue
        if current_new not in (None, tip):
            skipped.append({"old_ref": old_ref, "old_value": old_object, "reason": "canonical_pin_conflict"})
            continue
        planned.append(
            {
                "old_ref": old_ref,
                "old_value": old_object,
                "new_ref": new_ref,
                "new_value": tip,
                "proof": {
                    "receipt_commit": commit,
                    "receipt_tip": receipt.tip,
                    "task_ref": receipt.task_ref,
                    "lane_id": receipt.lane_id,
                    "checks": ["legacy_alias", "pin_object", "v1_position", "v2_topology", "v4_gate"],
                    "verified": True,
                },
            }
        )
    result: dict[str, Any] = {
        "command": "migrate-pins",
        "mode": "apply" if apply else "dry_run",
        "complete": True,
        "refs": planned,
        "skipped": skipped,
        "applied": [],
    }
    if not apply:
        return result

    applied: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    zero = "0" * 40
    for item in planned:
        current_old = _run_git(repo, "rev-parse", "--verify", "--quiet", "--end-of-options", item["old_ref"])
        if current_old.returncode == 0 and current_old.stdout.strip() != item["old_value"]:
            errors.append({"old_ref": item["old_ref"], "error": "legacy_pin_changed"})
            continue
        if current_old.returncode not in (0, 1):
            errors.append({"old_ref": item["old_ref"], "error": f"git_failed:{current_old.returncode}"})
            continue
        current_new, new_error = _ref_commit(repo, item["new_ref"])
        if new_error or current_new not in (None, item["new_value"]):
            errors.append({"old_ref": item["old_ref"], "error": new_error or "canonical_pin_conflict"})
            continue
        if current_new is None:
            create = _run_git(repo, "update-ref", item["new_ref"], item["new_value"], zero)
            if create.returncode != 0:
                current_new, new_error = _ref_commit(repo, item["new_ref"])
                if new_error or current_new != item["new_value"]:
                    errors.append(
                        {"old_ref": item["old_ref"], "error": f"canonical_pin_create_failed:{create.returncode}"}
                    )
                    continue
        if current_old.returncode == 0:
            remove = _run_git(repo, "update-ref", "-d", item["old_ref"], item["old_value"])
            if remove.returncode != 0:
                errors.append({"old_ref": item["old_ref"], "error": f"legacy_pin_remove_failed:{remove.returncode}"})
                continue
        applied.append(item)
    result["applied"] = applied
    if errors:
        result["complete"] = False
        result["errors"] = errors
    return result


class _UsageError(Exception):
    pass


class _ParserExit(Exception):
    def __init__(self, status: int, message: str = "") -> None:
        self.status = status
        self.message = message


class _JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise _UsageError(message)

    def exit(self, status: int = 0, message: str | None = None) -> None:
        raise _ParserExit(status, message or "")


def _parser() -> argparse.ArgumentParser:
    parser = _JsonArgumentParser(prog="landing_log")
    parser.add_argument("--workspace-root")
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_JsonArgumentParser)
    format_parser = commands.add_parser("format")
    format_parser.add_argument("--task", required=True)
    format_parser.add_argument("--lane", required=True)
    format_parser.add_argument("--tip", required=True)
    format_parser.add_argument("--gate", required=True)
    format_parser.add_argument("--run", required=True)
    parse_parser = commands.add_parser("parse")
    parse_parser.set_defaults(command="parse")
    find_parser = commands.add_parser("find")
    find_parser.add_argument("--task", required=True)
    find_parser.add_argument("--lane", required=True)
    find_parser.add_argument("--tip", required=True)
    find_parser.add_argument("--repo", default=".")
    find_parser.add_argument("--main-ref", default="main")
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--commit", required=True)
    verify_parser.add_argument("--repo", default=".")
    verify_parser.add_argument("--main-ref", default="main")
    scan_parser = commands.add_parser("scan")
    scan_parser.add_argument("--repo", default=".")
    scan_parser.add_argument("--main-ref", default="main")
    scan_parser.add_argument("--since")
    backfill_parser = commands.add_parser("backfill")
    backfill_parser.add_argument("--repo", default=".")
    backfill_parser.add_argument("--main-ref", default="main")
    backfill_parser.add_argument("--apply", action="store_true")
    migrate_parser = commands.add_parser("migrate-pins")
    migrate_parser.add_argument("--repo", default=".")
    migrate_parser.add_argument("--main-ref", default="main")
    migrate_parser.add_argument("--apply", action="store_true")
    return parser


def _emit(value: object) -> None:
    print(json.dumps(_jsonable(value), sort_keys=True, default=str))


def _workspace_root_for_args(args: argparse.Namespace) -> Path:
    if args.workspace_root:
        return Path(args.workspace_root).expanduser().resolve()
    repo = Path(getattr(args, "repo", ".")).expanduser().resolve()
    try:
        proc = _run_git(repo, "rev-parse", "--show-toplevel")
    except (OSError, subprocess.TimeoutExpired):
        return repo
    if proc.returncode == 0 and proc.stdout.strip():
        return Path(proc.stdout.strip()).expanduser().resolve()
    return repo


def main(argv: list[str] | None = None) -> int:
    """Run the JSON command-line interface for receipt tooling."""

    try:
        args = _parser().parse_args(argv)
    except _UsageError as exc:
        _emit({"error": str(exc)})
        return 2
    except _ParserExit as exc:
        _emit({"help": exc.message.strip()})
        return exc.status

    try:
        from workbay_handoff_mcp.api import configure_runtime  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        workspace_root = _workspace_root_for_args(args)
        configure_runtime(RuntimeConfig.for_workspace(workspace_root))
    except Exception as exc:  # noqa: BLE001 - CLI runtime absence has its own stable result
        _emit({"error": str(exc), "failed_check": "runtime_unconfigured"})
        return 4

    if args.command == "format":
        try:
            lines = format_landing_trailers(
                task_ref=args.task,
                lane_id=args.lane,
                tip=args.tip,
                gate_id=int(args.gate),
                run_id=args.run,
            )
        except (ValueError, TypeError) as exc:
            _emit({"error": str(exc)})
            return 2
        _emit({"trailers": lines})
        return 0
    if args.command == "parse":
        parsed = parse_landing_receipt(sys.stdin.read())
        _emit({"receipt": _jsonable(parsed)})
        if parsed is None:
            return 1
        return 3 if isinstance(parsed, dict) else 0
    if args.command == "find":
        scan = scan_first_parent(args.repo, main_ref=args.main_ref)
        if not scan.complete:
            _emit({"error": scan.error, "failed_check": "scan_timeout"})
            return 4
        verdict = find_landing(
            args.repo,
            task_ref=args.task,
            lane_id=args.lane,
            tip=args.tip,
            main_ref=args.main_ref,
            scan=scan,
        )
        if verdict is None:
            _emit({"status": "not_found"})
            return 1
        _emit(verdict)
        if verdict.failed_check == "scan_timeout":
            return 4
        return 0 if verdict.verified else 3
    if args.command == "verify":
        scan = scan_first_parent(args.repo, main_ref=args.main_ref)
        if not scan.complete:
            _emit({"error": scan.error, "failed_check": "scan_timeout"})
            return 4
        verdict = verify_receipt(args.repo, args.commit, main_ref=args.main_ref)
        _emit(verdict)
        return 0 if verdict.verified else 3
    if args.command == "scan":
        result = scan_first_parent(args.repo, main_ref=args.main_ref, since=args.since)
        _emit(result)
        return 0 if result.complete else 4
    if args.command == "backfill":
        result = backfill_branch_tips(args.repo, main_ref=args.main_ref, apply=args.apply)
        _emit(result)
        return 0 if result.get("complete") is True else 4
    if args.command == "migrate-pins":
        result = migrate_legacy_pins(args.repo, main_ref=args.main_ref, apply=args.apply)
        _emit(result)
        return 0 if result.get("complete") is True else 4
    _emit({"error": "unknown command"})
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
