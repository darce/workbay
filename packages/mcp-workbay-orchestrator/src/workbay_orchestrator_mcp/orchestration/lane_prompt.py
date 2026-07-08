#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from workbay_protocol import resolve_env_alias

from workbay_orchestrator_mcp.lanes import get_lane_activity


def get_handoff_state(*args: Any, **kwargs: Any) -> Any:
    from workbay_handoff_mcp import get_handoff_state as _get_handoff_state

    return _get_handoff_state(*args, **kwargs)


def _mcp_get_artifact(*args: Any, **kwargs: Any) -> Any:
    from workbay_handoff_mcp import get_artifact as _get_artifact

    return _get_artifact(*args, **kwargs)


def _mcp_search_artifacts(*args: Any, **kwargs: Any) -> Any:
    from workbay_handoff_mcp import search_artifacts as _search_artifacts

    return _search_artifacts(*args, **kwargs)


def _mcp_semantic_reinjection_packet(*args: Any, **kwargs: Any) -> Any:
    from workbay_handoff_mcp import semantic_reinjection_packet as _semantic_reinjection_packet

    return _semantic_reinjection_packet(*args, **kwargs)


def _mcp_latest_compaction_id(task_ref: str) -> str:
    from workbay_handoff_mcp.compaction import get_compaction_status

    receipt = get_compaction_status(task_ref=task_ref)
    latest = receipt.latest_compaction_id if receipt is not None else None
    return str(latest or "").strip()


# test-patch surface: these names are patched by test_lane_prompt_artifacts.py via lp._ARTIFACT_SEARCH_AVAILABLE etc.
_ARTIFACT_SEARCH_AVAILABLE = importlib.util.find_spec("workbay_handoff_mcp") is not None
ENGINEERING_HEURISTICS_REL = "docs/workbay/rules/engineering-heuristics.md"

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _env import extract_pyenv_version
from backend_registry import get_backend_spec
from orchestrator_helpers import _require_dict_payload

_handoff_read_shapes = importlib.import_module(
    f"{__package__}.handoff_read_shapes" if __package__ else "handoff_read_shapes"
)
from lane_manifest import get_lane_config

NO_WORK_MESSAGE = "No actionable lane inbox items."
WAITING_MESSAGE = "Open worker handoff already sent; waiting for orchestrator response."
NO_WORK_EXIT = 3
WAITING_EXIT = 4
MAX_ASSIGNMENT_ITEMS = 12
MAX_BRIEF_ITEMS = 6
MAX_DECISION_ITEMS = 4
MAX_TEST_ITEMS = 4
MAX_GLOBAL_ITEMS = 6
ANSI = {
    "reset": "\033[0m",
    "red": "\033[31m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "cyan": "\033[36m",
    "green": "\033[32m",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render an actionable worker prompt from lane MCP state.")
    parser.add_argument("--orchestrator-root", required=True)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--worktree-path", required=True)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 0 if actionable work exists, 3 if idle, 4 if waiting for orchestrator.",
    )
    parser.add_argument("--summary", action="store_true", help="Print a color-coded one-line-per-item summary.")
    parser.add_argument(
        "--include-lane-history",
        action="store_true",
        help="Include recent lane decisions and verification history for manual prompt inspection or escalated context reads.",
    )
    parser.add_argument(
        "--include-global-context",
        action="store_true",
        help="Include compact task-wide context that is intentionally omitted from the default lane-scoped prompt.",
    )
    return parser.parse_args()


def _as_dicts(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _line(text: str) -> str:
    return " ".join(text.split())


def _bullet_lines(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items]


def _bounded_items(items: list[str], *, limit: int) -> list[str]:
    if len(items) <= limit:
        return items
    hidden = len(items) - limit
    return [*items[:limit], f"... {hidden} additional item(s) omitted to keep the worker prompt focused."]


class PromptFormatter:
    """Formats individual activity records into compact prompt strings.

    All methods are pure static formatters that convert a record dict into a
    single descriptive line suitable for inclusion in a worker prompt.
    Grouping them into a class makes it easy to discover all formatters in one
    place and to subclass or override individual formatters in test harnesses.
    """

    @staticmethod
    def message(message: dict[str, Any]) -> str:
        subject = str(message.get("subject") or "lane message").strip()
        body = _line(str(message.get("message") or ""))
        return f"[#{message.get('id')}] {subject}: {body}"

    @staticmethod
    def brief_message(message: dict[str, Any]) -> str:
        payload = message.get("payload")
        if not isinstance(payload, dict):
            return PromptFormatter.message(message)
        source_lane = str(payload.get("source_lane") or "").strip()
        summary = _line(str(payload.get("summary") or message.get("message") or ""))
        reason = str(payload.get("reason") or "").strip()
        required_actions = [str(item).strip() for item in payload.get("required_actions", []) if str(item).strip()]
        artifacts = [str(item).strip() for item in payload.get("artifacts", []) if str(item).strip()]
        parts = [f"[#{message.get('id')}]"]
        if reason:
            parts.append(f"brief:{reason}")
        if source_lane:
            parts.append(f"from {source_lane}")
        text = " ".join(parts).strip()
        detail_parts = [summary]
        if required_actions:
            detail_parts.append("Actions: " + "; ".join(required_actions[:2]))
        if artifacts:
            detail_parts.append("Artifacts: " + ", ".join(artifacts[:2]))
        return f"{text}: {' | '.join(part for part in detail_parts if part)}"

    @staticmethod
    def action(action: dict[str, Any]) -> str:
        priority = action.get("priority")
        priority_label = f" [P{priority}]" if isinstance(priority, int) else ""
        return f"[#{action.get('id')}{priority_label}] {_line(str(action.get('action') or ''))}"

    @staticmethod
    def blocker(blocker: dict[str, Any]) -> str:
        return f"[#{blocker.get('id')}] {_line(str(blocker.get('description') or ''))}"

    @staticmethod
    def finding(finding: dict[str, Any]) -> str:
        location = str(finding.get("file_path") or "")
        line_start = finding.get("line_start")
        if isinstance(line_start, int):
            location = f"{location}:{line_start}"
        severity = str(finding.get("severity") or "unknown")
        description = _line(str(finding.get("description") or ""))
        return f"[{finding.get('finding_id')}] [{severity}] {location} - {description}"

    @staticmethod
    def decision(decision: dict[str, Any]) -> str:
        return f"[#{decision.get('id')}] {_line(str(decision.get('decision') or ''))}"

    @staticmethod
    def test(test: dict[str, Any]) -> str:
        passed = test.get("passed")
        status = "pass" if passed else "fail"
        command = _line(str(test.get("command") or test.get("test_command") or "verification command"))
        return f"[#{test.get('id')}] [{status}] {command}"

    @staticmethod
    def global_row(kind: str, row: dict[str, Any]) -> str:
        if kind == "action":
            return f"[action #{row.get('id')}] {_line(str(row.get('action') or ''))}"
        if kind == "blocker":
            return f"[blocker #{row.get('id')}] {_line(str(row.get('description') or ''))}"
        if kind == "finding":
            severity = str(row.get("severity") or "unknown")
            return f"[finding {severity}] {_line(str(row.get('description') or ''))}"
        if kind == "decision":
            return f"[decision #{row.get('id')}] {_line(str(row.get('decision') or ''))}"
        if kind == "test":
            return PromptFormatter.test(row)
        return _line(str(row))


def _format_message(message: dict[str, Any]) -> str:
    return PromptFormatter.message(message)


def _format_brief_message(message: dict[str, Any]) -> str:
    return PromptFormatter.brief_message(message)


def _format_action(action: dict[str, Any]) -> str:
    return PromptFormatter.action(action)


def _format_blocker(blocker: dict[str, Any]) -> str:
    return PromptFormatter.blocker(blocker)


def _format_finding(finding: dict[str, Any]) -> str:
    return PromptFormatter.finding(finding)


def _format_decision(decision: dict[str, Any]) -> str:
    return PromptFormatter.decision(decision)


def _format_test(test: dict[str, Any]) -> str:
    return PromptFormatter.test(test)


def _format_global_row(kind: str, row: dict[str, Any]) -> str:
    return PromptFormatter.global_row(kind, row)


def _summary_color(kind: str, *, severity: str = "", priority: int | None = None) -> str:
    if kind == "blocker":
        return ANSI["red"]
    if kind == "action":
        if priority == 1:
            return ANSI["red"]
        return ANSI["yellow"]
    if kind == "finding":
        if severity == "high":
            return ANSI["red"]
        if severity == "medium":
            return ANSI["yellow"]
        return ANSI["blue"]
    if kind == "message":
        return ANSI["cyan"]
    return ANSI["green"]


def _build_summary_lines(activity: dict[str, Any]) -> list[str]:
    state = _actionable_state(activity)
    if state["awaiting_orchestrator"]:
        return [f"{ANSI['yellow']}[WAITING]{ANSI['reset']} {WAITING_MESSAGE}"]
    if not state["actionable"]:
        return [f"{ANSI['green']}[IDLE]{ANSI['reset']} {NO_WORK_MESSAGE}"]

    messages = state["messages"]
    actions = state["actions"]
    blockers = state["blockers"]
    findings = state["findings"]

    lines: list[str] = []
    for blocker in blockers:
        color = _summary_color("blocker")
        lines.append(f"{color}[BLOCKER]{ANSI['reset']} {_line(str(blocker.get('description') or ''))}")
    for action in actions:
        priority = action.get("priority")
        color = _summary_color("action", priority=priority if isinstance(priority, int) else None)
        label = f"[ACTION P{priority}]" if isinstance(priority, int) else "[ACTION]"
        lines.append(f"{color}{label}{ANSI['reset']} {_line(str(action.get('action') or ''))}")
    for finding in findings:
        severity = str(finding.get("severity") or "unknown")
        color = _summary_color("finding", severity=severity)
        location = str(finding.get("file_path") or "").strip()
        if isinstance(finding.get("line_start"), int):
            location = f"{location}:{finding['line_start']}"
        suffix = f" ({location})" if location else ""
        lines.append(
            f"{color}[REVIEW {severity.upper()}]{ANSI['reset']} {_line(str(finding.get('description') or ''))}{suffix}"
        )
    for message in messages:
        color = _summary_color("message")
        subject = _line(str(message.get("subject") or "lane message"))
        body = _line(str(message.get("message") or ""))
        compact = f"{subject}: {body}" if body else subject
        lines.append(f"{color}[MESSAGE]{ANSI['reset']} {compact}")
    return lines


def _timestamp_text(row: dict[str, Any]) -> str:
    for key in ("updated_at", "created_at"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _latest_timestamp(rows: list[dict[str, Any]]) -> str:
    values = [_timestamp_text(row) for row in rows]
    values = [value for value in values if value]
    return max(values) if values else ""


def _actionable_state(activity: dict[str, Any]) -> dict[str, Any]:
    from workbay_handoff_mcp.enums import (  # noqa: PLC0415
        ActionStatus,
        BlockerStatus,
        FindingStatus,
        LaneMessageDirection,
        MessageStatus,
    )

    messages = [
        message
        for message in _as_dicts(activity.get("messages"))
        if message.get("direction") == LaneMessageDirection.ORCHESTRATOR_TO_WORKER
        and message.get("status") == MessageStatus.OPEN
    ]
    actions = sorted(
        [action for action in _as_dicts(activity.get("actions")) if action.get("status") == ActionStatus.PENDING],
        key=lambda action: action.get("priority", 99),
    )
    blockers = [
        blocker for blocker in _as_dicts(activity.get("blockers")) if blocker.get("status") == BlockerStatus.OPEN
    ]
    findings = [
        finding for finding in _as_dicts(activity.get("findings")) if finding.get("status") == FindingStatus.OPEN
    ]
    worker_messages = [
        message
        for message in _as_dicts(activity.get("messages"))
        if message.get("direction") == LaneMessageDirection.WORKER_TO_ORCHESTRATOR
        and message.get("status") == MessageStatus.OPEN
    ]

    actionable_rows: list[dict[str, Any]] = [*messages, *actions, *blockers, *findings]
    actionable = bool(actionable_rows)
    latest_worker_ts = _latest_timestamp(worker_messages)
    latest_action_ts = _latest_timestamp(actionable_rows)
    awaiting_orchestrator = bool(
        actionable and latest_worker_ts and latest_action_ts and latest_worker_ts >= latest_action_ts
    )
    return {
        "messages": messages,
        "actions": actions,
        "blockers": blockers,
        "findings": findings,
        "worker_messages": worker_messages,
        "actionable": actionable and not awaiting_orchestrator,
        "awaiting_orchestrator": awaiting_orchestrator,
    }


def _brief_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    briefs: list[dict[str, Any]] = []
    for message in messages:
        subject = str(message.get("subject") or "").strip().lower()
        if subject.startswith("brief:"):
            briefs.append(message)
    return briefs


def _non_brief_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    briefs = {message.get("id") for message in _brief_messages(messages)}
    return [message for message in messages if message.get("id") not in briefs]


def _runtime_guidance(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
) -> list[str]:
    try:
        lane_config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root)) or {}
    except FileNotFoundError:
        return []
    test_commands = [str(item).strip() for item in lane_config.get("test_commands", []) if str(item).strip()]
    app_root = str(lane_config.get("app_root") or "").strip()
    non_goals = [str(item).strip() for item in lane_config.get("non_goals", []) if str(item).strip()]
    owned_paths = [str(item).strip() for item in lane_config.get("owned_paths", []) if str(item).strip()]
    capability_tags = [str(item).strip() for item in lane_config.get("capability_tags", []) if str(item).strip()]
    preflight_commands = [str(item).strip() for item in lane_config.get("preflight_commands", []) if str(item).strip()]

    lines: list[str] = []

    if app_root:
        lines.extend(
            [
                "",
                "Working directory:",
                f"- Your primary application directory is `{app_root}/`. Run all test and build commands from there.",
                f"- `cd {app_root}` before running any lane verification commands.",
            ]
        )

    if owned_paths:
        lines.extend(["", "Owned paths (only edit files within these):"])
        lines.extend(_bullet_lines([f"`{path}`" for path in owned_paths]))

    if non_goals:
        lines.extend(["", "Constraints:"])
        lines.extend(_bullet_lines(non_goals))

    if test_commands:
        lines.extend(["", "Verification commands for this lane:"])
        lines.extend(_bullet_lines([f"`{command}`" for command in test_commands]))

    pyenv_version = extract_pyenv_version(test_commands)
    if pyenv_version:
        app_dir = app_root or "the application directory"
        lines.extend(
            [
                "",
                "Backend runtime notes:",
                f"- Your environment already has `PYENV_VERSION={pyenv_version}` exported with the virtualenv `bin/` directory on `PATH`.",
                f"- Use `python`, `pytest`, and `mypy` directly (they resolve to the `{pyenv_version}` virtualenv). Do NOT use bare `python3` or probe the system Python.",
                f"- Always `cd {app_dir}` first so imports resolve correctly.",
                "- A writable lane temp dir is provided under `.task-state/tmp/<lane>`; temp-file failures usually mean the command escaped the managed worker environment.",
                "- Local reset/bootstrap work depends on backend resources outside the worker sandbox. If PostgreSQL or other local services are unavailable, report `needs_guidance` instead of treating that as a code defect.",
            ]
        )
    if capability_tags or preflight_commands:
        labels = ", ".join(f"`{tag}`" for tag in capability_tags) if capability_tags else "configured lane requirements"
        lines.extend(
            [
                "",
                "Lane capability gate:",
                f"- `make lane-run` and the worker daemon run a preflight before any subagent turn for {labels}.",
                "- If the preflight fails, the lane auto-handoffs `needs_guidance` instead of spending tokens on a backend run that cannot succeed.",
            ]
        )
    return lines


def _render_section(title: str, items: list[str]) -> list[str]:
    if not items:
        return []
    return ["", f"{title}:", *_bullet_lines(items)]


def _worker_cycle_marker(orchestrator_root: Path, lane_id: str) -> int | None:
    state_dir = orchestrator_root / ".task-state"
    path = state_dir / f"worker-{lane_id}.status.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(errors="replace"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    cycle = payload.get("cycle")
    return cycle if isinstance(cycle, int) else None


def _semantic_reinjection_refresh_budget_chars() -> int:
    if not _ARTIFACT_SEARCH_AVAILABLE:
        return 0
    # The reinjection module top-imports numpy, which is only present with the
    # optional [embeddings] extra. On a core install (handoff package present but
    # extra absent) this import raises — never let it crash prompt rendering; the
    # packet call downstream degrades to the omission marker (REV-B-01).
    try:
        from workbay_handoff_mcp.embeddings.reinjection import ReinjectionConfig  # noqa: PLC0415
    except Exception:  # noqa: BLE001 - embeddings extra unavailable on core installs
        return 0

    return ReinjectionConfig.from_env().refresh_budget_chars


def _extract_changed_files(activity: dict[str, Any]) -> list[str]:
    for report in _as_dicts(activity.get("reports"))[:1]:
        raw = report.get("changed_files_json")
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError):
                parsed = []
        elif isinstance(raw, list):
            parsed = raw
        else:
            parsed = []
        return [str(path).strip() for path in parsed if isinstance(path, str) and str(path).strip()]
    return []


def _build_semantic_anchor_texts(activity: dict[str, Any], *, objective: str) -> list[str]:
    anchors: list[str] = []
    if objective.strip():
        anchors.append(objective.strip())
    for message in _brief_messages(_as_dicts(activity.get("messages", []))):
        payload = message.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (ValueError, TypeError):
                payload = {}
        if isinstance(payload, dict):
            summary = _line(str(payload.get("summary") or "")).strip()
            if summary:
                anchors.append(summary)
        body = _line(str(message.get("message") or "")).strip()
        if body:
            anchors.append(body)
    anchors.extend(_extract_changed_files(activity))
    seen: set[str] = set()
    ordered: list[str] = []
    for text in anchors:
        if text not in seen:
            seen.add(text)
            ordered.append(text)
    return ordered


def _engineering_guidance_lines(worktree_path: str) -> list[str]:
    guidance_path = Path(worktree_path).expanduser() / ENGINEERING_HEURISTICS_REL
    if not guidance_path.is_file():
        return []
    return [f"engineering guidance: {ENGINEERING_HEURISTICS_REL}"]


def _semantic_reinjection_cache_slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip()) or "unknown"


def _semantic_reinjection_cache_path(
    orchestrator_root: Path,
    *,
    task_ref: str,
    lane_id: str,
    cache_cycle: int,
) -> Path:
    task_slug = _semantic_reinjection_cache_slug(task_ref)
    lane_slug = _semantic_reinjection_cache_slug(lane_id)
    return (
        orchestrator_root / ".task-state" / "semantic-reinjection" / task_slug / f"{lane_slug}-cycle-{cache_cycle}.json"
    )


def _load_semantic_reinjection_cache(
    cache_path: Path,
    *,
    compaction_id: str,
) -> tuple[list[str], bool] | None:
    if not cache_path.is_file():
        return None
    try:
        payload = json.loads(cache_path.read_text(errors="replace"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("compaction_id") or "").strip() != compaction_id:
        return None
    lines = payload.get("lines")
    if not isinstance(lines, list):
        return None
    return [str(line) for line in lines if str(line).strip()], bool(payload.get("used"))


def _store_semantic_reinjection_cache(
    cache_path: Path,
    *,
    lines: list[str],
    compaction_id: str,
    used: bool,
) -> None:
    # Caching is best-effort: a read-only .task-state, a full disk, or a
    # permission error must never abort prompt rendering (REV-B-02).
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"lines": lines, "compaction_id": compaction_id, "used": used}
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        return
    # Only the current cycle's file is ever read; prune older per-lane cycle
    # files so the cache does not grow without bound (REV-B-05).
    prefix = cache_path.name.rsplit("-cycle-", 1)[0] + "-cycle-"
    try:
        for stale in cache_path.parent.glob(f"{prefix}*.json"):
            if stale != cache_path:
                stale.unlink()
    except OSError:
        return


def _relevant_concepts_section(
    *,
    activity: dict[str, Any],
    task_ref: str,
    lane_id: str,
    orchestrator_root: Path,
    objective: str,
) -> tuple[list[str], bool]:
    if not _ARTIFACT_SEARCH_AVAILABLE:
        return ["relevant concepts: (unavailable: handoff_mcp_unavailable)"], False

    cycle = _worker_cycle_marker(orchestrator_root, lane_id)
    # Workers start at cycle 0 (worker_daemon), so cycle 0 is a valid caching
    # cycle — only a missing/invalid marker (None) disables caching. Using > 0
    # here left the entire first cycle (and every --check poll before the status
    # file appears) uncached, re-running the packet on every poll (REV-B-03).
    cache_cycle = cycle if isinstance(cycle, int) and cycle >= 0 else -1

    compaction_id = ""
    try:
        compaction_id = _mcp_latest_compaction_id(task_ref)
    except Exception:  # noqa: BLE001
        compaction_id = ""

    if cache_cycle >= 0:
        cache_path = _semantic_reinjection_cache_path(
            orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            cache_cycle=cache_cycle,
        )
        cached = _load_semantic_reinjection_cache(cache_path, compaction_id=compaction_id)
        if cached is not None:
            return cached

    budget = _semantic_reinjection_refresh_budget_chars()
    anchor_texts = _build_semantic_anchor_texts(activity, objective=objective)
    try:
        payload = _require_dict_payload(
            _mcp_semantic_reinjection_packet(
                task_ref=task_ref,
                anchor_texts=anchor_texts,
                semantic_content_budget_chars=budget,
            ),
            source=f"semantic_reinjection_packet({lane_id})",
        )
    except Exception:  # noqa: BLE001
        lines = ["relevant concepts: (unavailable: error)"]
        return lines, False

    status = str(payload.get("status") or "").strip()
    skip_reason = str(payload.get("skip_reason") or "").strip()
    relevant_lines = payload.get("relevant_lines")
    if status == "selected" and isinstance(relevant_lines, list) and relevant_lines:
        # The packet renders its own readable block: a 'relevant:' header line
        # followed by '- <label>: <snippet>' bullets. The caller re-wraps these
        # in a "Relevant concepts:" section via _render_section (which bullets
        # each item), so pass through the concept bodies only — drop the packet's
        # header and any leading '- ' to avoid a spurious '- relevant:' line and
        # double-dashed '- - ...' bullets (REV-B-04).
        cleaned: list[str] = []
        for raw in relevant_lines:
            item = str(raw).strip()
            if not item or item.lower() == "relevant:":
                continue
            if item.startswith("- "):
                item = item[2:].strip()
            if item:
                cleaned.append(item)
        if cleaned:
            lines = cleaned
            used = True
        else:
            lines = ["relevant concepts: (unavailable: empty)"]
            used = False
    else:
        reason = skip_reason or status or "unavailable"
        lines = [f"relevant concepts: (unavailable: {reason})"]
        used = False

    if cache_cycle >= 0:
        _store_semantic_reinjection_cache(
            _semantic_reinjection_cache_path(
                orchestrator_root,
                task_ref=task_ref,
                lane_id=lane_id,
                cache_cycle=cache_cycle,
            ),
            lines=lines,
            compaction_id=compaction_id,
            used=used,
        )
    return lines, used


def _approx_chars(items: list[str]) -> int:
    return sum(len(item) for item in items)


def _discover_artifact_refs(activity: dict[str, Any]) -> list[int]:
    """Extract explicitly referenced artifact source_ids from activity data.

    Sources checked in priority order:
    1. Lane-message payloads (includes briefs, which are messages with ``subject``
       starting with ``"brief:"`` and may carry ``payload.artifacts``)
    2. Latest worker report payload (forward-compatible: currently a no-op since
       worker_reports do not yet have a payload column, but makes the scan explicit
       so future additions are automatically picked up)

    Returns a deduplicated list of integer source IDs in discovery order.
    """
    seen: set[int] = set()
    refs: list[int] = []

    def _collect(raw_payload: Any) -> None:
        payload = raw_payload
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (ValueError, TypeError):
                return
        if not isinstance(payload, dict):
            return
        for raw_id in payload.get("artifacts", []):
            try:
                sid = int(raw_id)
            except (ValueError, TypeError):
                continue
            if sid not in seen:
                seen.add(sid)
                refs.append(sid)

    # Phase 1: all lane messages (includes briefs stored as messages)
    for message in _as_dicts(activity.get("messages", [])):
        _collect(message.get("payload"))

    # Phase 2: latest worker report (forward-compatible scan; no-op with current schema)
    for report in _as_dicts(activity.get("reports", []))[:1]:
        _collect(report.get("payload") or report.get("payload_json"))

    return refs


def _build_artifact_queries(activity: dict[str, Any]) -> list[str]:
    """Extract search query terms from assignment messages, blockers, findings, brief summaries, and latest report.

    Priority: assignment message bodies > brief payload summaries/reasons >
    blocker descriptions > finding descriptions > latest report summary.
    Capped at 4 queries so FTS search stays focused.
    """
    queries: list[str] = []

    # Assignment message bodies (non-brief messages)
    for message in _as_dicts(activity.get("messages", [])):
        subject = str(message.get("subject") or "").strip().lower()
        if subject.startswith("brief:"):
            continue  # briefs handled separately below
        body = _line(str(message.get("message") or "")).strip()
        if body:
            queries.append(body[:120])

    # Brief payload summaries and reasons (highest-signal source for orchestrator briefs)
    for message in _as_dicts(activity.get("messages", [])):
        subject = str(message.get("subject") or "").strip().lower()
        if not subject.startswith("brief:"):
            continue
        payload = message.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (ValueError, TypeError):
                payload = {}
        if isinstance(payload, dict):
            brief_summary = _line(str(payload.get("summary") or "")).strip()
            brief_reason = _line(str(payload.get("reason") or "")).strip()
            if brief_summary:
                queries.append(brief_summary[:120])
            elif brief_reason:
                queries.append(brief_reason[:120])

    for blocker in _as_dicts(activity.get("blockers", [])):
        desc = _line(str(blocker.get("description") or "")).strip()
        if desc:
            queries.append(desc[:120])
    for finding in _as_dicts(activity.get("findings", [])):
        desc = _line(str(finding.get("description") or "")).strip()
        if desc:
            queries.append(desc[:120])

    # Latest worker report summary as a fallback when messages are sparse
    for report in _as_dicts(activity.get("reports", []))[:1]:
        report_summary = _line(str(report.get("summary") or "")).strip()
        if report_summary:
            queries.append(report_summary[:120])

    return queries[:4]


def _artifact_context_section(
    *,
    task_ref: str,
    lane_id: str,
    activity: dict[str, Any],
    budget_chars: int,
) -> list[str]:
    """Retrieve artifact snippets that fit within *budget_chars*.

    Returns compact rendered lines of the form ``[source_label] title: snippet``.
    Skips retrieval when the budget is exhausted or the search tool is unavailable.

    Strategy:
    1. Retrieve explicitly pinned artifacts (from ``payload.artifacts`` on lane messages)
       by source ID first — these are deterministic and highest-priority.
    2. Fall back to lexical FTS search with the remaining budget, skipping already-rendered
       source IDs so pinned refs are never duplicated.
    """
    if not _ARTIFACT_SEARCH_AVAILABLE or budget_chars <= 0:
        return []
    lines: list[str] = []
    used = 0
    rendered_ids: set[int] = set()

    # Phase 1: pinned artifact refs from message payloads
    pinned_ids = _discover_artifact_refs(activity)
    for sid in pinned_ids:
        if used >= budget_chars:
            break
        try:
            payload = _require_dict_payload(_mcp_get_artifact(source_id=sid), source=f"get_artifact({sid})")
            if not payload.get("ok"):
                continue
            data = payload.get("data")
            source = data.get("source") if isinstance(data, dict) else None
            if not source:
                continue
            label = str(source.get("source_label") or "artifact")
            summary = str(source.get("summary") or "").strip()
            if not summary:
                chunks = source.get("chunks") or []
                summary = str(chunks[0].get("body") or "") if chunks else ""
            snippet = summary[:200].rstrip()
            rendered = f"[{label}] {snippet}"
            if used + len(rendered) > budget_chars:
                break
            lines.append(rendered)
            used += len(rendered)
            rendered_ids.add(sid)
        except Exception:  # noqa: BLE001
            continue

    # Phase 2: lexical FTS search with remaining budget
    remaining = budget_chars - used
    if remaining <= 0:
        return lines
    queries = _build_artifact_queries(activity)
    if not queries:
        return lines
    try:
        payload = _require_dict_payload(
            _mcp_search_artifacts(queries=queries, task_ref=task_ref, lane_id=lane_id, limit=4),
            source=f"search_artifacts({lane_id})",
        )
        data = payload.get("data")
        hits = data.get("hits") if isinstance(data, dict) else []
        if not isinstance(hits, list) or not hits:
            return lines
    except Exception:  # noqa: BLE001
        return lines
    for hit in hits:
        sid = hit.get("source_id")
        if sid is not None and int(sid) in rendered_ids:
            continue
        label = str(hit.get("source_label") or "artifact")
        title = str(hit.get("title") or "")
        snippet = str(hit.get("snippet") or "")
        rendered = f"[{label}] {title}: {snippet}" if title else f"[{label}] {snippet}"
        if used + len(rendered) > budget_chars:
            break
        lines.append(rendered)
        used += len(rendered)
    return lines


_CHARS_PER_TOKEN_APPROX = 4
_TRUE_ENV_VALUES = {"1", "true", "yes", "on"}
_TIKTOKEN_OPENAI_EXACT_MODEL_RE = re.compile(
    r"^(gpt-(5|4\.1|4o)(?:[-\w.]*)?|o[134](?:[-\w.]*)?)$",
    re.IGNORECASE,
)
_TIKTOKEN_OPENAI_ESTIMATE_MODEL_RE = re.compile(
    r"^(gpt|o[134])[-\w.]*$",
    re.IGNORECASE,
)

try:
    import tiktoken
except ImportError:  # pragma: no cover - exercised via monkeypatch tests
    tiktoken = None  # type: ignore[assignment]


def _normalize_model_name(model: str | None) -> str:
    if not isinstance(model, str):
        return ""
    normalized = model.strip()
    if not normalized:
        return ""
    for separator in (":", "@"):
        if separator in normalized:
            normalized = normalized.split(separator, 1)[0].strip()
    return normalized


def _supports_exact_tiktoken_model(model_name: str) -> bool:
    """Return True only for model names with an explicit exact-tokenizer path."""
    return bool(_TIKTOKEN_OPENAI_EXACT_MODEL_RE.match(model_name))


def _prompt_token_count(
    rendered_prompt: str,
    *,
    backend: str | None,
    model: str | None,
) -> tuple[int, str]:
    prompt_chars = len(rendered_prompt)
    fallback_tokens = prompt_chars // _CHARS_PER_TOKEN_APPROX
    backend_name = (backend or "").strip()
    model_name = _normalize_model_name(model)
    if not backend_name or not model_name or tiktoken is None:
        return fallback_tokens, "char_estimate"

    try:
        tokenizer_family = get_backend_spec(backend_name).capabilities.preflight_tokenizer_family
    except RuntimeError:
        tokenizer_family = None
    if tokenizer_family != "tiktoken":
        return fallback_tokens, "char_estimate"

    if _supports_exact_tiktoken_model(model_name):
        try:
            encoding = tiktoken.encoding_for_model(model_name)
            return len(encoding.encode(rendered_prompt)), "observed"
        except Exception:  # noqa: BLE001
            return fallback_tokens, "char_estimate"

    if _TIKTOKEN_OPENAI_ESTIMATE_MODEL_RE.match(model_name):
        try:
            encoding = tiktoken.get_encoding("o200k_base")
            return len(encoding.encode(rendered_prompt)), "tokenizer_estimate"
        except Exception:  # noqa: BLE001
            return fallback_tokens, "char_estimate"

    return fallback_tokens, "char_estimate"


def _env_flag(name: str) -> bool:
    value = resolve_env_alias(name)
    return isinstance(value, str) and value.strip().lower() in _TRUE_ENV_VALUES


def _env_nonnegative_int(name: str) -> int:
    raw_value = resolve_env_alias(name)
    if not isinstance(raw_value, str) or not raw_value.strip():
        return 0
    try:
        return max(0, int(raw_value.strip()))
    except ValueError:
        return 0


def _runtime_attribution_from_env() -> dict[str, Any]:
    ctx7_query_count = _env_nonnegative_int("WORKBAY_HANDOFF_CTX7_QUERY_COUNT")
    used_ctx7 = _env_flag("WORKBAY_HANDOFF_USED_CTX7") or ctx7_query_count > 0
    return {
        "used_ace_guidance": _env_flag("WORKBAY_HANDOFF_ACE_GUIDANCE_USED"),
        "used_ctx7": used_ctx7,
        "ctx7_query_count": ctx7_query_count,
    }


def _measure_context_utilization(
    rendered_prompt: str,
    model_context_window: int,
    section_sizes: dict[str, int],
    *,
    attribution: dict[str, Any] | None = None,
    backend: str | None = None,
    model: str | None = None,
) -> dict:
    """Compute prompt context utilization metrics for a rendered prompt.

    Args:
        rendered_prompt: The fully rendered prompt text.
        model_context_window: Target model context window in tokens.
        section_sizes: Mapping of section name to character count.

    Returns:
        A dict with keys:
        - ``prompt_chars``: total character count of the rendered prompt
        - ``prompt_tokens``: current prompt token count (estimated here via chars / 4)
        - ``usage_source``: whether the prompt-token count is observed or estimated
        - ``utilization_ratio``: prompt_tokens / model_context_window
        - ``domain_signal_ratio``: fraction of chars that are domain-signal
          sections (assignment, runtime_guidance, dependency_briefs)
        - ``pressure``: "high", "medium", or "low" pressure indicator
    """
    prompt_chars = len(rendered_prompt)
    prompt_tokens, usage_source = _prompt_token_count(
        rendered_prompt,
        backend=backend,
        model=model,
    )
    utilization_ratio = prompt_tokens / model_context_window if model_context_window > 0 else 0.0

    domain_keys = {"assignment", "runtime_guidance", "dependency_briefs"}
    domain_chars = sum(v for k, v in section_sizes.items() if k in domain_keys)
    total_chars = sum(section_sizes.values()) if section_sizes else 0
    domain_signal_ratio = domain_chars / total_chars if total_chars > 0 else 0.0

    if utilization_ratio > 0.4 and domain_signal_ratio < 0.5:
        pressure = "high"
    elif utilization_ratio > 0.3:
        pressure = "elevated"
    else:
        pressure = "normal"

    return {
        "prompt_chars": prompt_chars,
        "prompt_tokens": prompt_tokens,
        "prompt_tokens_approx": prompt_tokens if usage_source != "observed" else None,
        "usage_source": usage_source,
        "utilization_ratio": round(utilization_ratio, 4),
        "domain_signal_ratio": round(domain_signal_ratio, 4),
        "pressure": pressure,
        "pressure_level": pressure,
        "section_sizes": dict(section_sizes),
        "attribution": dict(attribution or {}),
    }


def _prompt_budget_section(
    *,
    assignment_items: list[str],
    brief_items: list[str],
    runtime_guidance: list[str],
    recent_decisions: list[str],
    recent_tests: list[str],
    global_items: list[str],
    include_lane_history: bool,
    include_global_context: bool,
) -> list[str]:
    lines = [
        f"Assignment inbox contributes {len(assignment_items)} item(s), about {_approx_chars(assignment_items)} characters.",
        f"Dependency briefs contribute {len(brief_items)} item(s), about {_approx_chars(brief_items)} characters.",
        f"Runtime guidance contributes {len(runtime_guidance)} line(s), about {_approx_chars(runtime_guidance)} characters.",
    ]
    if include_lane_history:
        history_items = [*recent_decisions, *recent_tests]
        lines.append(
            f"Recent lane history contributes {len(history_items)} item(s), about {_approx_chars(history_items)} characters."
        )
    else:
        lines.append("Recent lane history is omitted from the default prompt budget.")
    if include_global_context:
        lines.append(
            f"Escalated task context contributes {len(global_items)} item(s), about {_approx_chars(global_items)} characters."
        )
    else:
        lines.append("Escalated task context is omitted from the default prompt budget.")
    return lines


def _task_global_context(task_ref: str) -> dict[str, list[dict[str, Any]]]:
    payload = _require_dict_payload(
        _handoff_read_shapes.read_handoff_state(
            **_handoff_read_shapes.global_context_kwargs(task_ref, limit=MAX_GLOBAL_ITEMS)
        ),
        source=f"get_handoff_state(global:{task_ref})",
    )
    return {
        "actions": [row for row in _as_dicts(payload.get("actions_pending")) if row.get("lane_id") in (None, "")],
        "blockers": [row for row in _as_dicts(payload.get("blockers_open")) if row.get("lane_id") in (None, "")],
        "findings": [row for row in _as_dicts(payload.get("findings_open")) if row.get("lane_id") in (None, "")],
        "decisions": [row for row in _as_dicts(payload.get("decisions_recent")) if row.get("lane_id") in (None, "")],
        "tests": [row for row in _as_dicts(payload.get("tests_recent")) if row.get("lane_id") in (None, "")],
    }


def _build_prompt_sections(
    activity: dict[str, Any],
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    *,
    orchestrator_root: Path,
    include_lane_history: bool = False,
    include_global_context: bool = False,
) -> dict[str, list[str]]:
    lane_value = activity.get("lane")
    lane: dict[str, Any] = lane_value if isinstance(lane_value, dict) else {}
    branch = str(lane.get("branch") or "")
    objective = str(lane.get("objective") or "").strip()
    state = _actionable_state(activity)
    messages = state["messages"]
    actions = state["actions"]
    blockers = state["blockers"]
    findings = state["findings"]
    brief_messages = _brief_messages(messages)
    assignment_messages = _non_brief_messages(messages)
    latest_report = _as_dicts(activity.get("reports"))[:1]

    header = [
        f"You are the worker agent for lane `{lane_id}` on task `{task_ref}`.",
        f"Worktree: `{worktree_path}`",
    ]
    if branch:
        header.append(f"Branch: `{branch}`")
    if objective:
        header.append(f"Objective: {objective}")
    header.extend(
        [
            "",
            "Operate only within this lane's owned files and do not edit sibling-lane paths.",
        ]
    )

    assignment_items = _bounded_items(
        [
            *[_format_message(message) for message in assignment_messages],
            *[_format_action(action) for action in actions],
            *[_format_finding(finding) for finding in findings],
            *[_format_blocker(blocker) for blocker in blockers],
        ],
        limit=MAX_ASSIGNMENT_ITEMS,
    )
    brief_items = _bounded_items(
        [_format_brief_message(message) for message in brief_messages],
        limit=MAX_BRIEF_ITEMS,
    )
    recent_decisions = _bounded_items(
        [_format_decision(decision) for decision in _as_dicts(activity.get("decisions"))],
        limit=MAX_DECISION_ITEMS,
    )
    recent_tests = _bounded_items(
        [_format_test(test) for test in _as_dicts(activity.get("tests"))],
        limit=MAX_TEST_ITEMS,
    )
    runtime_guidance = [
        line
        for line in _runtime_guidance(orchestrator_root=orchestrator_root, task_ref=task_ref, lane_id=lane_id)
        if line
    ]

    latest_report_lines: list[str] = []
    if latest_report:
        report = latest_report[0]
        latest_report_lines = [f"[{report.get('status')}] {_line(str(report.get('summary') or ''))}"]

    reporting_contract = [
        "Inspect the referenced files and implement the highest-priority open work in this lane.",
        "Run the lane-local tests before handoff.",
        "When merge-ready, run `make lane-handoff`.",
        "If you need clarification or are blocked, submit a blocked worker report so the orchestrator sees it in `make handoff-inbox`.",
    ]
    context_budget = [
        f"Assignment inbox is capped at {MAX_ASSIGNMENT_ITEMS} items.",
        f"Dependency briefs are capped at {MAX_BRIEF_ITEMS} items.",
        "Broader task/global context is intentionally excluded from worker prompts; ask the orchestrator for a compact brief instead of replaying full transcripts.",
    ]
    if include_lane_history:
        context_budget.append(
            "Escalated lane history is included below for this render because `--include-lane-history` was requested."
        )
    else:
        context_budget.append(
            "Recent lane decisions/tests are omitted by default to preserve tokens; rerun with `--include-lane-history` when manual inspection truly needs them."
        )
    if include_global_context:
        context_budget.append(
            "Compact task-wide context is included below because `--include-global-context` was requested."
        )
    else:
        context_budget.append(
            "Broader task/global context is excluded by default; rerun with `--include-global-context` only when lane-local state and briefs are insufficient."
        )

    global_items: list[str] = []
    if include_global_context:
        global_context = _task_global_context(task_ref)
        global_items = _bounded_items(
            [
                *[_format_global_row("action", row) for row in global_context["actions"]],
                *[_format_global_row("blocker", row) for row in global_context["blockers"]],
                *[_format_global_row("finding", row) for row in global_context["findings"]],
                *[_format_global_row("decision", row) for row in global_context["decisions"]],
                *[_format_global_row("test", row) for row in global_context["tests"]],
            ],
            limit=MAX_GLOBAL_ITEMS,
        )
    prompt_budget = _prompt_budget_section(
        assignment_items=assignment_items,
        brief_items=brief_items,
        runtime_guidance=runtime_guidance,
        recent_decisions=recent_decisions,
        recent_tests=recent_tests,
        global_items=global_items,
        include_lane_history=include_lane_history,
        include_global_context=include_global_context,
    )

    sections: dict[str, list[str]] = {
        "header": header,
        "context_budget": context_budget,
        "prompt_budget": prompt_budget,
        "assignment": assignment_items,
        "runtime_guidance": runtime_guidance,
        "dependency_briefs": brief_items,
        "latest_report": latest_report_lines,
        "reporting_contract": reporting_contract,
    }
    if include_lane_history:
        sections["recent_lane_history"] = [*recent_decisions, *recent_tests]
    if include_global_context:
        sections["global_context"] = global_items
    return sections


def _build_prompt(
    activity: dict[str, Any],
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    *,
    orchestrator_root: Path,
    include_lane_history: bool = False,
    include_global_context: bool = False,
    model_context_window: int = 128_000,
    backend: str | None = None,
    model: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Return ``(rendered_prompt, context_utilization_metrics)``."""
    state = _actionable_state(activity)
    if state["awaiting_orchestrator"]:
        return WAITING_MESSAGE, {}
    if not state["actionable"]:
        return NO_WORK_MESSAGE, {}
    sections = _build_prompt_sections(
        activity,
        task_ref=task_ref,
        lane_id=lane_id,
        worktree_path=worktree_path,
        orchestrator_root=orchestrator_root,
        include_lane_history=include_lane_history,
        include_global_context=include_global_context,
    )
    lane_value = activity.get("lane")
    lane_row: dict[str, Any] = lane_value if isinstance(lane_value, dict) else {}
    objective = str(lane_row.get("objective") or "").strip()
    relevant_concepts, used_semantic_reinjection = _relevant_concepts_section(
        activity=activity,
        task_ref=task_ref,
        lane_id=lane_id,
        orchestrator_root=orchestrator_root,
        objective=objective,
    )
    engineering_guidance = _engineering_guidance_lines(worktree_path)
    lines: list[str] = list(sections["header"])
    lines.extend(_render_section("Context Budget", list(sections["context_budget"])))
    lines.extend(_render_section("Prompt Budget", list(sections["prompt_budget"])))
    lines.extend(_render_section("Assignment Inbox", list(sections["assignment"])))
    lines.extend(_render_section("Runtime Guidance", list(sections["runtime_guidance"])))
    lines.extend(_render_section("Dependency Briefs", list(sections["dependency_briefs"])))
    if relevant_concepts and relevant_concepts[0].startswith("relevant concepts: (unavailable:"):
        lines.extend(["", *relevant_concepts])
    else:
        lines.extend(_render_section("Relevant concepts", relevant_concepts))
    if engineering_guidance:
        lines.extend(["", *engineering_guidance])
    lines.extend(_render_section("Recent Lane History", list(sections.get("recent_lane_history", []))))
    lines.extend(_render_section("Escalated Task Context", list(sections.get("global_context", []))))
    lines.extend(_render_section("Latest Worker Report", list(sections["latest_report"])))
    lines.extend(_render_section("Reporting Contract", list(sections["reporting_contract"])))
    rendered = "\n".join(lines)
    section_sizes = {k: sum(len(line) for line in v) for k, v in sections.items() if isinstance(v, list)}
    section_sizes["relevant_concepts"] = sum(len(line) for line in relevant_concepts)
    section_sizes["engineering_guidance"] = sum(len(line) for line in engineering_guidance)
    runtime_attribution = _runtime_attribution_from_env()
    attribution = {
        "used_ace_guidance": runtime_attribution["used_ace_guidance"],
        "used_artifact_context": False,
        "used_slice_packet": bool(sections["dependency_briefs"]),
        "used_recent_lane_history": include_lane_history,
        "used_global_context": include_global_context,
        "used_ctx7": runtime_attribution["used_ctx7"],
        "ctx7_query_count": runtime_attribution["ctx7_query_count"],
        "used_semantic_reinjection": used_semantic_reinjection,
    }
    ctx_metrics = _measure_context_utilization(
        rendered,
        model_context_window,
        section_sizes,
        attribution=attribution,
        backend=backend,
        model=model,
    )

    # Budget-aware artifact retrieval: skip when context is already elevated
    artifact_context: list[str] = []
    if ctx_metrics.get("pressure") not in ("elevated", "high"):
        # Remaining char budget = total window minus what the base prompt already uses
        budget_chars = max(0, model_context_window * _CHARS_PER_TOKEN_APPROX - len(rendered))
        artifact_context = _artifact_context_section(
            task_ref=task_ref,
            lane_id=lane_id,
            activity=activity,
            budget_chars=budget_chars,
        )
    if artifact_context:
        lines.extend(_render_section("Relevant Artifacts", artifact_context))
        rendered = "\n".join(lines)
        section_sizes["artifact_context"] = sum(len(line) for line in artifact_context)
        attribution["used_artifact_context"] = True
        ctx_metrics = _measure_context_utilization(
            rendered,
            model_context_window,
            section_sizes,
            attribution=attribution,
            backend=backend,
            model=model,
        )

    return rendered, ctx_metrics


def main() -> int:
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime  # noqa: PLC0415

    args = _parse_args()
    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    runtime = RuntimeConfig.for_repo(orchestrator_root)
    configure_runtime(runtime)

    history_limit = 20 if args.include_lane_history else 1
    activity = _require_dict_payload(
        get_lane_activity(
            lane_id=args.lane_id,
            task_ref=args.task_ref,
            limit_decisions=history_limit,
            limit_tests=history_limit,
            limit_findings=50,
            limit_actions=50,
            limit_blockers=50,
        ),
        source=f"get_lane_activity(prompt:{args.lane_id})",
    )
    if activity.get("ok") is not True:
        raise RuntimeError(f"Unable to load lane activity: {activity}")

    # Load model_context_window and preferred backend/model from manifest (fall back to defaults)
    model_context_window = 128_000
    preferred_backend: str | None = None
    preferred_model: str | None = None
    try:
        from lane_manifest import get_lane_config as _get_lane_cfg

        _lcfg = _get_lane_cfg(args.task_ref, args.lane_id)
        model_context_window = int(_lcfg.get("model_context_window") or model_context_window)
        preferred_backend = str(_lcfg.get("preferred_backend")).strip() if _lcfg.get("preferred_backend") else None
        preferred_model = str(_lcfg.get("preferred_model")).strip() if _lcfg.get("preferred_model") else None
    except Exception:  # noqa: BLE001
        pass

    state = _actionable_state(activity)
    prompt, ctx_metrics = _build_prompt(
        activity,
        task_ref=args.task_ref,
        lane_id=args.lane_id,
        worktree_path=args.worktree_path,
        orchestrator_root=orchestrator_root,
        include_lane_history=args.include_lane_history,
        include_global_context=args.include_global_context,
        model_context_window=model_context_window,
        backend=preferred_backend,
        model=preferred_model,
    )
    # Emit context utilization metrics to stderr so callers can capture without
    # polluting the prompt text that goes to stdout.
    if ctx_metrics:
        import json as _json

        print(_json.dumps({"context_utilization": ctx_metrics}), file=sys.stderr)
    if args.check:
        if state["actionable"]:
            return 0
        if state["awaiting_orchestrator"]:
            return WAITING_EXIT
        return NO_WORK_EXIT
    if args.summary:
        print("\n".join(_build_summary_lines(activity)))
        return 0
    print(prompt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
