"""ACE reflection/apply mechanism for strategy-bullet evolution.

Owns parser/validation, counter application, idempotent replay, advisory
curation report, and optional model-backed curation triggers.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import fcntl
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

BULLET_RE = re.compile(
    r"^\s*-\s+\[(?P<rule_id>(?:sr|rg)-\d{3})\]\s+"
    r"helpful=(?P<helpful>\d+)\s+"
    r"harmful=(?P<harmful>\d+)\s*::\s*(?P<text>.+)$"
)

RULE_REF_RE = re.compile(r"\[(?:sr|rg)-\d{3}\]")

_CONTRADICTION_KEYWORDS = frozenset(
    [
        "violat",
        "missing",
        "contradict",
        "breaks",
        "broke",
        "fail",
        "ignored",
        "bypass",
        "incorrect",
    ]
)

JOURNAL_NAME = "ace_apply_journal.jsonl"
LOCK_NAME = "ace_apply.lock"
OFFSET_SUFFIX = ".offset"
DEDUP_SUFFIX = ".ace_dedup.json"


class PlaybookValidationError(ValueError):
    """Raised when playbook declarations fail validation."""


@dataclass
class ApplySummary:
    total_processed: int = 0
    incremented: int = 0
    skipped: int = 0
    unapplied: list[dict[str, str]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_processed": self.total_processed,
            "incremented": self.incremented,
            "skipped": self.skipped,
            "unapplied": list(self.unapplied),
        }

    @property
    def has_unapplied(self) -> bool:
        return bool(self.unapplied)


def parse_strategy_bullets(filepath: Path) -> dict[str, dict]:
    """Parse ACE strategy bullets from a playbook file."""
    results: dict[str, dict] = {}
    if not filepath.exists():
        return results
    for line_number, line in enumerate(filepath.read_text(encoding="utf-8").splitlines(), start=1):
        match = BULLET_RE.match(line)
        if match:
            results[match.group("rule_id")] = {
                "helpful": int(match.group("helpful")),
                "harmful": int(match.group("harmful")),
                "text": match.group("text").strip(),
                "line_number": line_number,
            }
    return results


def validate_playbook_files(playbook_files: list[Path]) -> dict[str, Path]:
    """Validate playbook declarations and return rule_id -> owning file."""
    if not playbook_files:
        raise PlaybookValidationError(
            "at least one playbook file is required (--playbook-file or WORKBAY_ACE_PLAYBOOK_FILES)"
        )

    seen_paths: set[Path] = set()
    rule_owners: dict[str, Path] = {}
    total_rules = 0

    for path in playbook_files:
        resolved = path.resolve()
        if resolved in seen_paths:
            raise PlaybookValidationError(f"duplicate playbook path: {path}")
        seen_paths.add(resolved)
        if not path.exists():
            raise PlaybookValidationError(f"playbook file not found: {path}")
        bullets = parse_strategy_bullets(path)
        total_rules += len(bullets)
        for rule_id in bullets:
            if rule_id in rule_owners:
                raise PlaybookValidationError(f"duplicate rule id {rule_id} in {path} and {rule_owners[rule_id]}")
            rule_owners[rule_id] = path

    if total_rules == 0:
        raise PlaybookValidationError("playbook files contain no ACE strategy bullets")
    return rule_owners


def detect_rule_references(text: str) -> list[str]:
    """Return unique ACE rule ids found in text."""
    seen: dict[str, None] = {}
    for raw in RULE_REF_RE.findall(text):
        seen[raw[1:-1]] = None
    return list(seen)


def classify_rule_reference(text: str, rule_id: str) -> bool:
    """Return True when text indicates the rule was contradicted."""
    pattern = re.compile(re.escape(f"[{rule_id}]"))
    text_lower = text.lower()
    for match in pattern.finditer(text_lower):
        start = max(0, match.start() - 80)
        end = min(len(text_lower), match.end() + 80)
        neighbourhood = text_lower[start:end]
        if any(keyword in neighbourhood for keyword in _CONTRADICTION_KEYWORDS):
            return True
    return False


def _fsync_dir(directory: Path) -> None:
    if not directory.exists():
        return
    fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        _fsync_dir(path.parent)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _counter_values(filepath: Path, rule_id: str) -> tuple[int, int] | None:
    bullets = parse_strategy_bullets(filepath)
    entry = bullets.get(rule_id)
    if entry is None:
        return None
    return entry["helpful"], entry["harmful"]


def _replace_counter(
    filepath: Path,
    rule_id: str,
    counter: str,
    *,
    expected_old: int | None = None,
) -> tuple[bool, int, int]:
    """Atomically increment one counter; return (modified, old, new)."""
    text = filepath.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        match = BULLET_RE.match(line)
        if match and match.group("rule_id") == rule_id:
            old_val = int(match.group(counter))
            if expected_old is not None and old_val != expected_old:
                raise ValueError(f"counter conflict for {rule_id}.{counter}: expected {expected_old}, found {old_val}")
            new_val = old_val + 1
            # Rewrite only the matched counter digits by span, not a value-based
            # str.replace (which could collide with identical digits elsewhere
            # on the line, e.g. inside the bullet text).
            start, end = match.span(counter)
            lines[index] = line[:start] + str(new_val) + line[end:]
            _atomic_write_text(filepath, "".join(lines))
            return True, old_val, new_val
    return False, 0, 0


def identify_pruning_candidates(filepath: Path) -> list[dict]:
    bullets = parse_strategy_bullets(filepath)
    return [
        {"rule_id": rule_id, **info}
        for rule_id, info in bullets.items()
        if info["helpful"] == 0 and info["harmful"] >= 2
    ]


def _offset_path(reflect_log: Path) -> Path:
    return reflect_log.with_name(reflect_log.name + OFFSET_SUFFIX)


def _load_processed_offset(reflect_log: Path) -> int:
    offset_file = _offset_path(reflect_log)
    if not offset_file.exists():
        return 0
    try:
        return int(json.loads(offset_file.read_text(encoding="utf-8")).get("processed_line_count", 0))
    except (OSError, ValueError, json.JSONDecodeError):
        return 0


def _regenerate_offset_projection(reflect_log: Path, processed_line_count: int) -> None:
    offset_file = _offset_path(reflect_log)
    offset_file.write_text(json.dumps({"processed_line_count": processed_line_count}), encoding="utf-8")


def _regenerate_dedup_projections(playbook_files: list[Path], committed_keys: set[str]) -> None:
    keys_by_file: dict[Path, set[str]] = {path: set() for path in playbook_files}
    for key in committed_keys:
        # event_key is "<finding_id>:<rule_id>:<counter>"; finding_id may itself
        # contain ':' so split from the right to keep rule_id/counter intact.
        parts = key.rsplit(":", 2)
        if len(parts) != 3:
            continue
        rule_id = parts[1]
        for path in playbook_files:
            if rule_id in parse_strategy_bullets(path):
                keys_by_file[path].add(key)
                break
    for path, keys in keys_by_file.items():
        if not keys:
            continue
        sidecar = path.with_suffix(path.suffix + DEDUP_SUFFIX)
        sidecar.write_text(json.dumps(sorted(keys), indent=2), encoding="utf-8")


def _committed_event_keys(journal_rows: list[dict[str, Any]]) -> set[str]:
    prepared: dict[str, dict[str, Any]] = {}
    committed: set[str] = set()
    for row in journal_rows:
        phase = row.get("phase")
        event_key = row.get("event_key")
        if not isinstance(event_key, str):
            continue
        if phase == "prepared":
            prepared[event_key] = row
        elif phase == "committed":
            committed.add(event_key)
            prepared.pop(event_key, None)
    return committed


def _prepared_recovery_preview(
    journal_rows: list[dict[str, Any]],
    rule_owners: dict[str, Path],
) -> tuple[set[str], int, list[dict[str, str]]]:
    """Read-only preview of journal recovery for dry-run and committed-key seeding."""
    committed = _committed_event_keys(journal_rows)
    would_recover = 0
    errors: list[dict[str, str]] = []
    prepared: dict[str, dict[str, Any]] = {}
    for row in journal_rows:
        event_key = row.get("event_key")
        if not isinstance(event_key, str):
            continue
        if row.get("phase") == "prepared":
            prepared[event_key] = row
        elif row.get("phase") == "committed":
            prepared.pop(event_key, None)

    for event_key, row in prepared.items():
        if event_key in committed:
            continue
        rule_id = row.get("rule_id")
        counter = row.get("counter")
        old_value = row.get("old_value")
        new_value = row.get("new_value")
        if not isinstance(rule_id, str) or counter not in ("helpful", "harmful"):
            errors.append({"event_key": event_key, "reason": "invalid prepared journal row"})
            continue
        owner = rule_owners.get(rule_id)
        if owner is None:
            errors.append({"event_key": event_key, "reason": f"unknown rule {rule_id}"})
            continue
        current = _counter_values(owner, rule_id)
        if current is None:
            errors.append({"event_key": event_key, "reason": f"rule {rule_id} missing from playbook"})
            continue
        helpful, harmful = current
        actual = helpful if counter == "helpful" else harmful
        if isinstance(new_value, int) and actual == new_value:
            committed.add(event_key)
        elif isinstance(old_value, int) and actual == old_value:
            would_recover += 1
            committed.add(event_key)
        else:
            errors.append(
                {
                    "event_key": event_key,
                    "reason": f"journal/playbook conflict for {rule_id}.{counter}",
                }
            )
    return committed, would_recover, errors


def _recover_journal(
    state_dir: Path,
    playbook_files: list[Path],
    rule_owners: dict[str, Path],
) -> tuple[list[dict[str, str]], int]:
    journal = state_dir / JOURNAL_NAME
    rows = _read_jsonl(journal)
    errors: list[dict[str, str]] = []
    recovered = 0
    prepared: dict[str, dict[str, Any]] = {}
    for row in rows:
        event_key = row.get("event_key")
        if not isinstance(event_key, str):
            continue
        if row.get("phase") == "prepared":
            prepared[event_key] = row
        elif row.get("phase") == "committed":
            prepared.pop(event_key, None)

    for event_key, row in prepared.items():
        rule_id = row.get("rule_id")
        counter = row.get("counter")
        old_value = row.get("old_value")
        new_value = row.get("new_value")
        if not isinstance(rule_id, str) or counter not in ("helpful", "harmful"):
            errors.append({"event_key": event_key, "reason": "invalid prepared journal row"})
            continue
        owner = rule_owners.get(rule_id)
        if owner is None:
            errors.append({"event_key": event_key, "reason": f"unknown rule {rule_id}"})
            continue
        current = _counter_values(owner, rule_id)
        if current is None:
            errors.append({"event_key": event_key, "reason": f"rule {rule_id} missing from playbook"})
            continue
        helpful, harmful = current
        actual = helpful if counter == "helpful" else harmful
        if isinstance(new_value, int) and actual == new_value:
            _append_jsonl(
                journal,
                {
                    "phase": "committed",
                    "event_key": event_key,
                    "rule_id": rule_id,
                    "counter": counter,
                    "playbook": str(owner),
                    "old_value": old_value,
                    "new_value": new_value,
                    "recovered": True,
                },
            )
            continue
        if isinstance(old_value, int) and actual == old_value:
            try:
                modified, _, committed_new = _replace_counter(owner, rule_id, counter, expected_old=old_value)
            except ValueError as exc:
                errors.append({"event_key": event_key, "reason": str(exc)})
                continue
            if modified:
                _append_jsonl(
                    journal,
                    {
                        "phase": "committed",
                        "event_key": event_key,
                        "rule_id": rule_id,
                        "counter": counter,
                        "playbook": str(owner),
                        "old_value": old_value,
                        "new_value": committed_new,
                        "recovered": True,
                    },
                )
                recovered += 1
            continue
        errors.append(
            {
                "event_key": event_key,
                "reason": f"journal/playbook conflict for {rule_id}.{counter}",
            }
        )
    return errors, recovered


@contextlib.contextmanager
def _ace_apply_lock(state_dir: Path) -> Iterator[None]:
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / LOCK_NAME
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def ace_apply_counters(
    reflect_log: Path,
    playbook_files: list[Path],
    *,
    state_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Process reflect-log entries and increment playbook counters."""
    summary = ApplySummary()
    rule_owners = validate_playbook_files(playbook_files)
    resolved_state_dir = state_dir or reflect_log.parent
    journal = resolved_state_dir / JOURNAL_NAME

    with _ace_apply_lock(resolved_state_dir):
        journal_rows = _read_jsonl(journal)
        if dry_run:
            committed_keys, preview_recovered, preview_errors = _prepared_recovery_preview(journal_rows, rule_owners)
            summary.unapplied.extend(preview_errors)
            summary.incremented += preview_recovered
        else:
            recovery_errors, recovered = _recover_journal(resolved_state_dir, playbook_files, rule_owners)
            summary.unapplied.extend(recovery_errors)
            summary.incremented += recovered
            journal_rows = _read_jsonl(journal)
            committed_keys = _committed_event_keys(journal_rows)

        if not reflect_log.exists():
            if not dry_run:
                _regenerate_dedup_projections(playbook_files, committed_keys)
            return summary.as_dict()

        processed_offset = _load_processed_offset(reflect_log)
        lines = reflect_log.read_text(encoding="utf-8").splitlines()
        non_empty_lines = [line for line in lines if line.strip()]
        # The offset advances only over the contiguous *resolved* prefix. The
        # first unapplied line (malformed/unknown/conflict) blocks advancement
        # so it is re-surfaced on the next run; later valid records still apply,
        # but must never let the offset jump the gap — that would silently drop
        # an unknown-rule/malformed signal across re-runs.
        resolved_through = processed_offset
        prefix_blocked = False

        for line_number, line in enumerate(non_empty_lines):
            if line_number < processed_offset:
                continue
            line = line.strip()
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                summary.skipped += 1
                summary.unapplied.append({"line": str(line_number + 1), "reason": "malformed json"})
                prefix_blocked = True
                continue

            finding_id = record.get("finding_id", "unknown")
            rule_id = record.get("rule_id")
            contradicts = record.get("contradicts", False)
            if not isinstance(rule_id, str):
                summary.skipped += 1
                summary.unapplied.append({"line": str(line_number + 1), "reason": "missing rule_id"})
                prefix_blocked = True
                continue

            counter = "harmful" if contradicts else "helpful"
            event_key = f"{finding_id}:{rule_id}:{counter}"
            owner = rule_owners.get(rule_id)
            if owner is None:
                summary.skipped += 1
                summary.unapplied.append({"event_key": event_key, "reason": f"unknown rule {rule_id}"})
                prefix_blocked = True
                continue

            summary.total_processed += 1
            if event_key in committed_keys:
                summary.skipped += 1
                if not prefix_blocked:
                    resolved_through = line_number + 1
                continue

            current = _counter_values(owner, rule_id)
            if current is None:
                summary.unapplied.append({"event_key": event_key, "reason": f"rule {rule_id} missing"})
                prefix_blocked = True
                continue
            old_helpful, old_harmful = current
            old_value = old_harmful if counter == "harmful" else old_helpful
            new_value = old_value + 1

            if dry_run:
                summary.incremented += 1
                committed_keys.add(event_key)
                if not prefix_blocked:
                    resolved_through = line_number + 1
                continue

            _append_jsonl(
                journal,
                {
                    "phase": "prepared",
                    "event_key": event_key,
                    "rule_id": rule_id,
                    "counter": counter,
                    "playbook": str(owner),
                    "old_value": old_value,
                    "new_value": new_value,
                },
            )
            modified, _, committed_new = _replace_counter(owner, rule_id, counter, expected_old=old_value)
            if not modified:
                summary.unapplied.append({"event_key": event_key, "reason": f"failed to update {rule_id}"})
                prefix_blocked = True
                continue
            _append_jsonl(
                journal,
                {
                    "phase": "committed",
                    "event_key": event_key,
                    "rule_id": rule_id,
                    "counter": counter,
                    "playbook": str(owner),
                    "old_value": old_value,
                    "new_value": committed_new,
                },
            )
            committed_keys.add(event_key)
            summary.incremented += 1
            if not prefix_blocked:
                resolved_through = line_number + 1

        if not dry_run:
            _regenerate_offset_projection(reflect_log, resolved_through)
            _regenerate_dedup_projections(playbook_files, committed_keys)

    return summary.as_dict()


def ace_reflect_on_findings(
    findings: list[dict[str, Any]],
    playbook_files: list[Path],
    state_dir: Path | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    rule_owners = validate_playbook_files(playbook_files)
    known_rules = set(rule_owners)

    for finding in findings:
        finding_id = finding.get("id") or finding.get("finding_id", "unknown")
        description = finding.get("description") or finding.get("text") or ""
        for rule_id in detect_rule_references(description):
            if rule_id not in known_rules:
                continue
            records.append(
                {
                    "finding_id": finding_id,
                    "rule_id": rule_id,
                    "contradicts": classify_rule_reference(description, rule_id),
                }
            )

    if state_dir is not None and records:
        state_dir.mkdir(parents=True, exist_ok=True)
        reflect_log = state_dir / "ace_reflect_log.jsonl"
        timestamp = _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
        with reflect_log.open("a", encoding="utf-8") as handle:
            for record in records:
                entry = dict(record)
                entry["timestamp"] = timestamp
                handle.write(json.dumps(entry) + "\n")
        ace_apply_counters(reflect_log, playbook_files, state_dir=state_dir)

    return records


def _curation_report(playbook_files: list[Path]) -> str:
    lines = ["# ACE Curation Report"]
    for path in playbook_files:
        if not path.exists():
            lines.append(f"  {path}: not found")
            continue
        bullets = parse_strategy_bullets(path)
        candidates = identify_pruning_candidates(path)
        lines.append(f"\n## {path}")
        lines.append(f"  Total bullets: {len(bullets)}")
        total_helpful = sum(value["helpful"] for value in bullets.values())
        total_harmful = sum(value["harmful"] for value in bullets.values())
        lines.append(f"  helpful sum: {total_helpful}  harmful sum: {total_harmful}")
        if candidates:
            lines.append(f"  Pruning candidates ({len(candidates)}):")
            for candidate in candidates:
                lines.append(
                    f"    [{candidate['rule_id']}] helpful={candidate['helpful']} "
                    f"harmful={candidate['harmful']} :: {candidate['text'][:80]}"
                )
        else:
            lines.append("  No pruning candidates.")
    return "\n".join(lines) + "\n"


def _append_curation_log(state_dir: Path, entry: dict[str, Any]) -> None:
    log_path = state_dir / "ace_curation_log.jsonl"
    _append_jsonl(log_path, entry)


def _curation_token_total(state_dir: Path) -> int:
    total = 0
    for row in _read_jsonl(state_dir / "ace_curation_log.jsonl"):
        total += int(row.get("token_usage", {}).get("total", {}).get("total_tokens") or 0)
    return total


def _run_model_curation(
    *,
    state_dir: Path,
    playbook_files: list[Path],
    reflect_log: Path,
    backend: str | None,
    model: str | None,
    reasoning_effort: str | None,
    threshold: int,
    budget_tokens: int,
) -> dict[str, Any]:
    pending_entries = (
        sum(1 for line in reflect_log.read_text(encoding="utf-8").splitlines() if line.strip())
        if reflect_log.exists()
        else 0
    )
    pruning_candidates = sum(len(identify_pruning_candidates(path)) for path in playbook_files if path.exists())
    trigger_size = max(pending_entries, pruning_candidates)
    spent_tokens = _curation_token_total(state_dir)

    if not backend:
        return {"status": "disabled", "pending_entries": pending_entries, "pruning_candidates": pruning_candidates}
    if trigger_size < threshold:
        result: dict[str, Any] = {
            "status": "below_threshold",
            "pending_entries": pending_entries,
            "pruning_candidates": pruning_candidates,
            "threshold": threshold,
            "budget_tokens": budget_tokens,
        }
        _append_curation_log(state_dir, result)
        return result
    if spent_tokens >= budget_tokens:
        result = {
            "status": "budget_exhausted",
            "pending_entries": pending_entries,
            "pruning_candidates": pruning_candidates,
            "threshold": threshold,
            "budget_tokens": budget_tokens,
            "spent_tokens": spent_tokens,
        }
        _append_curation_log(state_dir, result)
        return result

    try:
        from workbay_orchestrator_mcp.orchestration.backend_registry import get_adapter  # noqa: PLC0415
    except ImportError:
        result = {
            "status": "backend_unavailable",
            "error": "Model-backed curation requires backend_registry.",
            "pending_entries": pending_entries,
            "pruning_candidates": pruning_candidates,
        }
        _append_curation_log(state_dir, result)
        return result

    bullet_summaries: list[str] = []
    for path in playbook_files:
        for candidate in identify_pruning_candidates(path):
            bullet_summaries.append(
                f"{candidate['rule_id']}: helpful={candidate['helpful']} "
                f"harmful={candidate['harmful']} text={candidate['text']}"
            )
    prompt = (
        "Review ACE rule evidence and propose curation actions.\n"
        f"Pending reflect entries: {pending_entries}\n"
        f"Pruning candidates: {pruning_candidates}\n"
        "Pruning candidate summaries:\n"
        + ("\n".join(f"- {item}" for item in bullet_summaries) if bullet_summaries else "- none")
    )
    schema = {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "recommendations": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["summary", "recommendations"],
        "additionalProperties": False,
    }
    adapter = get_adapter(backend)
    adapter_result = adapter.execute(
        prompt=prompt,
        schema=schema,
        worktree_path=Path.cwd(),
        model=model,
        reasoning_effort=reasoning_effort,
    )
    timestamp = _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
    entry = {
        "timestamp": timestamp,
        "status": "triggered",
        "backend": backend,
        "model": model or adapter_result.response_model,
        "reasoning_effort": reasoning_effort or adapter_result.reasoning_effort,
        "threshold": threshold,
        "budget_tokens": budget_tokens,
        "pending_entries": pending_entries,
        "pruning_candidates": pruning_candidates,
        "summary": adapter_result.summary,
        "token_usage": adapter_result.token_usage or {},
    }
    _append_curation_log(state_dir, entry)
    return entry


def run_curation_report(*, playbook_files: list[Path]) -> int:
    validate_playbook_files(playbook_files)
    print(_curation_report(playbook_files), end="")
    return 0


def run_ace_reflect(
    *,
    state_dir: Path,
    playbook_files: list[Path],
    dry_run: bool = False,
    model_curation_backend: str | None = None,
    model_curation_model: str | None = None,
    model_curation_reasoning_effort: str | None = None,
    model_curation_threshold: int = 5,
    model_curation_budget_tokens: int = 20000,
) -> int:
    validate_playbook_files(playbook_files)
    reflect_log = state_dir / "ace_reflect_log.jsonl"
    summary = ace_apply_counters(
        reflect_log,
        playbook_files,
        state_dir=state_dir,
        dry_run=dry_run,
    )
    print(
        f"ace-reflect: processed={summary['total_processed']}  "
        f"incremented={summary['incremented']}  skipped={summary['skipped']}"
    )
    if summary["total_processed"] == 0:
        print("  No pending entries in", reflect_log)
    if summary.get("unapplied"):
        for item in summary["unapplied"]:
            print(f"  unapplied: {item}")

    if not dry_run:
        curation = _run_model_curation(
            state_dir=state_dir,
            playbook_files=playbook_files,
            reflect_log=reflect_log,
            backend=model_curation_backend,
            model=model_curation_model,
            reasoning_effort=model_curation_reasoning_effort,
            threshold=max(1, model_curation_threshold),
            budget_tokens=max(1, model_curation_budget_tokens),
        )
        if curation.get("status") == "triggered":
            print(f"  model-curation: backend={curation.get('backend')} model={curation.get('model') or 'default'}")
        elif curation.get("status") == "backend_unavailable":
            print(f"  model-curation: {curation['error']}")

    return 1 if summary.get("unapplied") else 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply pending ACE counter updates or print a curation report.")
    parser.add_argument("--state-dir", default=".task-state")
    parser.add_argument("--playbook-file", action="append", required=True, dest="playbook_files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--curation-report-only", action="store_true")
    parser.add_argument("--model-curation-backend", default=None)
    parser.add_argument("--model-curation-model", default=None)
    parser.add_argument("--model-curation-reasoning-effort", default=None)
    parser.add_argument("--model-curation-threshold", type=int, default=5)
    parser.add_argument("--model-curation-budget-tokens", type=int, default=20000)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    playbook_files = [Path(path) for path in args.playbook_files]
    if args.curation_report_only:
        return run_curation_report(playbook_files=playbook_files)
    return run_ace_reflect(
        state_dir=Path(args.state_dir),
        playbook_files=playbook_files,
        dry_run=args.dry_run,
        model_curation_backend=args.model_curation_backend,
        model_curation_model=args.model_curation_model,
        model_curation_reasoning_effort=args.model_curation_reasoning_effort,
        model_curation_threshold=max(1, args.model_curation_threshold),
        model_curation_budget_tokens=max(1, args.model_curation_budget_tokens),
    )


if __name__ == "__main__":
    sys.exit(main())
