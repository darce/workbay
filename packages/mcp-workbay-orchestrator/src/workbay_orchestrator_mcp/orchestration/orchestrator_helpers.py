"""Shared helpers for orchestrator modules: logging, payload guards, text normalization."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any


def _log(log_dir: Path, level: str, event: str, **extra: Any) -> None:
    """Append one JSONL record to ``<log_dir>/orchestrator.jsonl``."""
    log_dir.mkdir(parents=True, exist_ok=True)
    entry: dict[str, Any] = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "level": level,
        "event": event,
        **extra,
    }
    path = log_dir / "orchestrator.jsonl"
    rotate_jsonl_if_needed(path, 1_000_000)
    with path.open("a") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
    print(f"[{level}] {event}", flush=True)


def rotate_jsonl_if_needed(path: Path, max_bytes: int) -> None:
    if not path.exists():
        return
    try:
        if path.stat().st_size >= max_bytes:
            rotated = path.with_suffix(path.suffix + ".1")
            if rotated.exists():
                rotated.unlink()
            path.replace(rotated)
    except OSError:
        pass


def _require_dict_payload(payload: Any, *, source: str) -> dict[str, Any]:
    """Assert that a helper/tool call already returned a native dict payload."""
    if isinstance(payload, dict):
        return payload
    raise TypeError(f"{source} returned {type(payload).__name__}; expected dict payload.")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _combined_text(*parts: Any) -> str:
    return " ".join(_normalize_text(part) for part in parts if _normalize_text(part)).lower()


def _json_list_text(raw_value: Any) -> str:
    if isinstance(raw_value, list):
        return " ".join(str(item) for item in raw_value)
    if not isinstance(raw_value, str) or not raw_value.strip():
        return ""
    try:
        data = json.loads(raw_value)
    except json.JSONDecodeError:
        return raw_value
    if isinstance(data, list):
        return " ".join(str(item) for item in data)
    return raw_value


def _message_timestamp(message: dict[str, Any]) -> str:
    return str(message.get("updated_at") or message.get("created_at") or "")


def _report_timestamp(report: dict[str, Any]) -> str:
    return str(report.get("created_at") or "")
