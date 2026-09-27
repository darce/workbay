"""Persist lane-gate style debt and carry it into the next wave.

landing requires no NEW non-style failures versus the integration baseline;
style debt is recorded here and assigned to the next wave.  The JSONL ledger is
the durable observation, while handoff review findings provide the operator
visible tracking surface when that optional API is available.
"""

from __future__ import annotations

import datetime
import fcntl
import importlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_STYLE_DEBT_FILENAME = "style-debt.jsonl"
_STYLE_DEBT_DELIVERY_FILENAME = "style-debt-handoff.jsonl"
_STYLE_DEBT_FINDING_FILE = "Makefile.d/lane-gate.mk"
_RESOLVED_FINDING_STATUSES = frozenset({"fixed", "wontfix", "resolved_on_branch", "integrated", "superseded"})


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@dataclass(frozen=True)
class StyleDebtRecord:
    """One observed style-debt count for a lane tip."""

    lane_id: str
    task_ref: str
    branch: str
    tip_sha: str
    count: int
    advisories: list[str] = field(default_factory=list)
    gate_command: str | list[str] = ""
    observed_at: str = field(default_factory=_utc_now)

    def __post_init__(self) -> None:
        advisories = self.advisories
        if advisories is None:
            normalized: list[str] = []
        elif isinstance(advisories, str):
            normalized = [advisories] if advisories else []
        else:
            normalized = [str(item) for item in advisories]
        object.__setattr__(self, "advisories", normalized)

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSONL representation."""
        return {
            "lane_id": self.lane_id,
            "task_ref": self.task_ref,
            "branch": self.branch,
            "tip_sha": self.tip_sha,
            "count": self.count,
            "advisories": list(self.advisories),
            "gate_command": self.gate_command,
            "observed_at": self.observed_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StyleDebtRecord":
        """Decode a JSONL row, rejecting malformed required fields."""
        count = value["count"]
        if isinstance(count, bool):
            raise ValueError("style debt count must be an integer")
        count = int(count)
        if count < 0:
            raise ValueError("style debt count must be non-negative")
        return cls(
            lane_id=str(value["lane_id"]),
            task_ref=str(value["task_ref"]),
            branch=str(value.get("branch") or ""),
            tip_sha=str(value["tip_sha"]),
            count=count,
            advisories=value.get("advisories") or [],
            gate_command=value.get("gate_command") or "",
            observed_at=str(value.get("observed_at") or ""),
        )


def _ledger_path(state_dir: Path | str) -> Path:
    return Path(state_dir) / _STYLE_DEBT_FILENAME


def _read_records(path: Path) -> list[StyleDebtRecord]:
    if not path.exists():
        return []
    records: list[StyleDebtRecord] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
            if not isinstance(raw, dict):
                continue
            records.append(StyleDebtRecord.from_dict(raw))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            # A malformed historical row must not hide later valid observations.
            continue
    return records


def _append_if_new(record: StyleDebtRecord, *, state_dir: Path | str) -> tuple[bool, bool]:
    """Append under a file lock and return ``(written, already_present)``."""
    directory = Path(state_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = _ledger_path(directory)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            handle.seek(0)
            existing_keys: set[tuple[str, str, str]] = set()
            for line in handle:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    if isinstance(raw, dict):
                        existing_keys.add(
                            (
                                str(raw.get("task_ref") or ""),
                                str(raw.get("lane_id") or ""),
                                str(raw.get("tip_sha") or ""),
                            )
                        )
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
            key = (record.task_ref, record.lane_id, record.tip_sha)
            if key in existing_keys:
                return False, True
            handle.seek(0, 2)
            handle.write(json.dumps(record.to_dict(), sort_keys=True) + "\n")
            handle.flush()
            return True, False
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _handoff_api() -> Any | None:
    try:
        package = importlib.import_module("workbay_handoff_mcp")
    except Exception:  # noqa: BLE001 - optional handoff surface
        return None
    return getattr(package, "api", package)


def _finding_id(record: StyleDebtRecord) -> str:
    return f"STYLE-{record.lane_id.upper()}-{record.tip_sha[:7]}"


def _finding_description(record: StyleDebtRecord) -> str:
    advisories = ", ".join(record.advisories) if record.advisories else "none"
    return (
        f"[style_debt] Lane gate observed {record.count} style debt item(s) for "
        f"lane {record.lane_id} at tip {record.tip_sha[:7]} on branch {record.branch or 'unknown'}. "
        f"Advisories: {advisories}."
    )


def _defer_handoff_finding(
    api: Any | None,
    record: StyleDebtRecord,
    *,
    session: str | None = None,
) -> tuple[bool, str | None]:
    if api is None:
        return False, "handoff_unavailable"
    deferrer = getattr(api, "update_review_finding", None)
    if not callable(deferrer):
        return False, "handoff_defer_unavailable"
    resolved_session = session or f"style-debt-{record.lane_id}-{record.tip_sha[:7]}"
    try:
        deferred = deferrer(
            status="deferred",
            finding_id=_finding_id(record),
            task_ref=record.task_ref,
            session=resolved_session,
            resolution_notes=(
                "Lane-gate style debt is advisory and carried into the next wave; "
                "defer until the style-only issue is fixed."
            ),
        )
    except Exception as exc:  # noqa: BLE001 - observation must not block landing
        return False, f"{type(exc).__name__}: {exc}"
    if isinstance(deferred, dict) and deferred.get("ok") is False:
        data = deferred.get("data")
        detail = data.get("error") if isinstance(data, dict) else deferred.get("error")
        return False, str(detail or "handoff finding defer rejected")
    return True, None


def _record_handoff_finding(record: StyleDebtRecord) -> tuple[bool, str | None]:
    api = _handoff_api()
    if api is None:
        return False, "handoff_unavailable"
    recorder = getattr(api, "record_review_finding", None)
    if not callable(recorder):
        return False, "handoff_record_review_finding_unavailable"
    session = f"style-debt-{record.lane_id}-{record.tip_sha[:7]}"
    try:
        result = recorder(
            session=session,
            finding_id=_finding_id(record),
            severity="low",
            file_path=_STYLE_DEBT_FINDING_FILE,
            description=_finding_description(record),
            task_ref=record.task_ref,
            review_mode="planning",
        )
    except Exception as exc:  # noqa: BLE001 - observation must not block landing
        return False, f"{type(exc).__name__}: {exc}"
    if isinstance(result, dict) and result.get("ok") is False:
        data = result.get("data")
        detail = data.get("error") if isinstance(data, dict) else result.get("error")
        return False, str(detail or "handoff finding rejected")

    # Style debt is an operator-visible carry-forward item, not a merge
    # blocker. The handoff store represents that distinction with the
    # terminal ``deferred`` status; keep the row visible while ensuring open
    # finding gates cannot reject an otherwise green lane.
    return _defer_handoff_finding(api, record, session=session)


# A store that cannot be reached is not a store that rejected us. Retrying an
# unconfigured or absent handoff runtime can never succeed, so classifying it as
# "failed" turns a bounded best-effort delivery into an unbounded retry. Mirror
# the collapse ``_finding_delivery_state`` already performs and call it
# unavailable. Canon: feedback-bounded waiting; fail-closed on ambiguity.
_UNAVAILABLE_REASON_MARKERS = (
    "handoff_unavailable",
    "runtimenotconfigurederror",
    "_unavailable",
    "not configured",
)


def _delivery_state_for(*, recorded: bool, reason: str | None) -> str:
    if recorded:
        return "recorded"
    text = (reason or "").casefold()
    if any(marker in text for marker in _UNAVAILABLE_REASON_MARKERS):
        return "unavailable"
    return "failed"


def _delivery_path(state_dir: Path | str) -> Path:
    return Path(state_dir) / _STYLE_DEBT_DELIVERY_FILENAME


def _delivery_key(record: StyleDebtRecord) -> tuple[str, str, str]:
    return record.task_ref, record.lane_id, record.tip_sha


def _append_delivery_state(record: StyleDebtRecord, *, state_dir: Path | str, state: str) -> None:
    """Append the best-effort handoff delivery state for retryable duplicates."""
    directory = Path(state_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = _delivery_path(directory)
    with path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            handle.write(
                json.dumps(
                    {
                        "task_ref": record.task_ref,
                        "lane_id": record.lane_id,
                        "tip_sha": record.tip_sha,
                        "state": state,
                        "observed_at": _utc_now(),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            handle.flush()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _latest_delivery_state(record: StyleDebtRecord, *, state_dir: Path | str) -> str | None:
    path = _delivery_path(state_dir)
    if not path.exists():
        return None
    latest: str | None = None
    key = _delivery_key(record)
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(raw, dict):
                continue
            raw_key = (
                str(raw.get("task_ref") or ""),
                str(raw.get("lane_id") or ""),
                str(raw.get("tip_sha") or ""),
            )
            if raw_key == key:
                latest = str(raw.get("state") or "") or None
    except OSError:
        return None
    return latest


def _finding_delivery_state(record: StyleDebtRecord) -> str:
    """Return present/resolved/absent/unavailable without mutating a finding."""
    api = _handoff_api()
    if api is None:
        return "unavailable"
    reader = getattr(api, "list_review_findings", None)
    if not callable(reader):
        return "unavailable"
    try:
        response = reader(task_ref=record.task_ref, finding_id=_finding_id(record), detail="full")
    except TypeError:
        # Small optional adapters may expose the older two-argument reader;
        # preserve the retry contract when the detail projection is absent.
        try:
            response = reader(task_ref=record.task_ref, finding_id=_finding_id(record))
        except Exception:  # noqa: BLE001 - unavailable observation keeps debt visible
            return "unavailable"
    except Exception:  # noqa: BLE001 - unavailable observation keeps retry state
        return "unavailable"
    if not isinstance(response, dict):
        return "unavailable"
    data = response.get("data") if isinstance(response.get("data"), dict) else response
    findings = data.get("findings") if isinstance(data, dict) else None
    if isinstance(findings, list):
        for finding in findings:
            if not isinstance(finding, dict):
                continue
            if str(finding.get("finding_id") or "") == _finding_id(record):
                status = str(finding.get("status") or "").casefold()
                if status in _RESOLVED_FINDING_STATUSES:
                    return "resolved"
                return "open" if status == "open" else "present"
        return "absent"
    error = str(data.get("error") or "") if isinstance(data, dict) else ""
    if "not found" in error.casefold():
        return "absent"
    return "unavailable"


def persist_style_debt(record: StyleDebtRecord, *, state_dir: Path | str) -> dict[str, Any]:
    """Persist one observation before attempting optional handoff recording."""
    try:
        written, already_present = _append_if_new(record, state_dir=state_dir)
    except Exception as exc:  # noqa: BLE001 - typed persistence degradation
        return {"recorded": False, "reason": f"style_debt_jsonl_write_failed: {type(exc).__name__}: {exc}"}
    if already_present:
        finding_state = _finding_delivery_state(record)
        if finding_state in {"present", "resolved"}:
            return {
                "recorded": True,
                "idempotent": True,
                "finding_id": _finding_id(record),
                "handoff_state": finding_state,
            }
        # If the handoff reader is unavailable, do not blindly re-record: the
        # recorder reopens terminal findings. An open row only needs its
        # non-blocking disposition retried; an absent row needs record+defer.
        if finding_state == "open":
            finding_recorded, reason = _defer_handoff_finding(_handoff_api(), record)
            if not finding_recorded:
                return {
                    "recorded": False,
                    "reason": reason or "handoff_finding_not_deferred",
                    "jsonl_recorded": False,
                    "finding_id": _finding_id(record),
                    "handoff_retried": True,
                }
            return {
                "recorded": True,
                "idempotent": True,
                "handoff_retried": True,
                "finding_id": _finding_id(record),
            }
        if finding_state == "unavailable" and _latest_delivery_state(record, state_dir=state_dir) != "failed":
            return {"recorded": True, "idempotent": True, "finding_id": _finding_id(record)}
        if finding_state != "absent":
            finding_recorded, reason = _record_handoff_finding(record)
            try:
                _append_delivery_state(
                    record,
                    state_dir=state_dir,
                    state=_delivery_state_for(recorded=finding_recorded, reason=reason),
                )
            except Exception:
                pass
            if not finding_recorded:
                return {
                    "recorded": False,
                    "reason": reason or "handoff_finding_not_recorded",
                    "jsonl_recorded": False,
                    "finding_id": _finding_id(record),
                    "handoff_retried": True,
                }
            return {
                "recorded": True,
                "idempotent": True,
                "handoff_retried": True,
                "finding_id": _finding_id(record),
            }
        finding_recorded, reason = _record_handoff_finding(record)
        try:
            _append_delivery_state(
                record,
                state_dir=state_dir,
                state=_delivery_state_for(recorded=finding_recorded, reason=reason),
            )
        except Exception:
            pass
        if not finding_recorded:
            return {
                "recorded": False,
                "reason": reason or "handoff_finding_not_recorded",
                "jsonl_recorded": False,
                "finding_id": _finding_id(record),
                "handoff_retried": True,
            }
        return {
            "recorded": True,
            "idempotent": True,
            "handoff_retried": True,
            "finding_id": _finding_id(record),
        }

    finding_recorded, reason = _record_handoff_finding(record)
    try:
        _append_delivery_state(
            record,
            state_dir=state_dir,
            state=_delivery_state_for(recorded=finding_recorded, reason=reason),
        )
    except Exception:
        pass
    if not finding_recorded:
        return {
            "recorded": False,
            "reason": reason or "handoff_finding_not_recorded",
            "jsonl_recorded": written,
            "finding_id": _finding_id(record),
        }
    return {"recorded": True, "idempotent": False, "finding_id": _finding_id(record)}


def _finding_is_resolved(record: StyleDebtRecord) -> bool:
    return _finding_delivery_state(record) == "resolved"


def next_wave_style_debt(state_dir: Path | str, *, task_ref: str | None = None) -> list[StyleDebtRecord]:
    """Return the latest unresolved positive observation per task/lane."""
    records = _read_records(_ledger_path(Path(state_dir)))
    if task_ref is not None:
        records = [record for record in records if record.task_ref == task_ref]

    latest: dict[tuple[str, str], tuple[int, StyleDebtRecord]] = {}
    for index, record in enumerate(records):
        latest[(record.task_ref, record.lane_id)] = (index, record)

    carried: list[StyleDebtRecord] = []
    for index, record in sorted(latest.values(), key=lambda item: item[0]):
        if record.count <= 0:
            continue
        if any(
            later.task_ref == record.task_ref
            and later.lane_id == record.lane_id
            and later_index > index
            and later.count == 0
            for later_index, later in enumerate(records)
        ):
            continue
        if _finding_is_resolved(record):
            continue
        carried.append(record)
    return carried


def render_style_debt_section(records: list[StyleDebtRecord]) -> str:
    """Render a compact markdown block suitable for a next-wave brief."""
    if not records:
        return ""
    lines = ["## Style debt carried from previous wave", ""]
    for record in records:
        advisories = ", ".join(f"`{item}`" for item in record.advisories) or "`none`"
        lines.append(
            f"- lane: `{record.lane_id}`; count: `{record.count}`; advisories: {advisories}; "
            f"tip: `{record.tip_sha[:7]}`"
        )
    return "\n".join(lines) + "\n"
