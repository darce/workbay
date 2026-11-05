"""Tests for the ACE PostToolUse hook."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


HOOK_SCRIPT = Path(__file__).parent / "ace-detect.py"


def _run_hook(payload: dict, cwd: str | None = None) -> tuple[int, dict | None]:
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=5,
        cwd=cwd,
    )
    stdout_json = None
    if proc.stdout.strip():
        stdout_json = json.loads(proc.stdout)
    return proc.returncode, stdout_json


def test_record_operation_writes_reflect_log_for_snake_case_payload(tmp_path: Path) -> None:
    payload = {
        "tool_name": "mcp__workbay-handoff-mcp__review_findings",
        "tool_input": {
            "review": {
                "operation": "record",
                "finding_id": "M-1",
                "description": "This violates [sr-001] because the hook is missing.",
            }
        },
    }

    exit_code, output = _run_hook(payload, cwd=str(tmp_path))

    assert exit_code == 0
    assert output == {"result": "continue"}
    reflect_log = tmp_path / ".task-state" / "ace_reflect_log.jsonl"
    assert reflect_log.exists()
    record = json.loads(reflect_log.read_text(encoding="utf-8").strip())
    assert record["finding_id"] == "M-1"
    assert record["rule_id"] == "sr-001"
    assert record["contradicts"] is True


def test_record_operation_writes_reflect_log_for_camel_case_payload(tmp_path: Path) -> None:
    payload = {
        "toolName": "mcp__workbay-handoff-mcp__review_findings",
        "toolInput": {
            "review": {
                "operation": "record",
                "finding_id": "M-1",
                "description": "This violates [rg-010] because the hook was bypassed.",
            }
        },
    }

    exit_code, output = _run_hook(payload, cwd=str(tmp_path))

    assert exit_code == 0
    assert output == {"result": "continue"}
    reflect_log = tmp_path / ".task-state" / "ace_reflect_log.jsonl"
    assert reflect_log.exists()
    record = json.loads(reflect_log.read_text(encoding="utf-8").strip())
    assert record["finding_id"] == "M-1"
    assert record["rule_id"] == "rg-010"
    assert record["contradicts"] is True


def test_batch_record_writes_one_row_per_rule_reference(tmp_path: Path) -> None:
    payload = {
        "tool_name": "mcp__workbay-handoff-mcp__review_findings",
        "tool_input": {
            "review": {
                "operation": "batch_record",
                "findings": [
                    {
                        "finding_id": "B-1",
                        "description": "Missing [sr-001] enforcement in handler.",
                    },
                    {
                        "finding_id": "B-2",
                        "description": "[rg-010] applied correctly.",
                    },
                ],
            }
        },
    }

    exit_code, output = _run_hook(payload, cwd=str(tmp_path))

    assert exit_code == 0
    assert output == {"result": "continue"}
    reflect_log = tmp_path / ".task-state" / "ace_reflect_log.jsonl"
    rows = [json.loads(line) for line in reflect_log.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 2
    assert {row["finding_id"] for row in rows} == {"B-1", "B-2"}


def test_duplicate_rule_reference_in_one_finding_is_deduped(tmp_path: Path) -> None:
    payload = {
        "tool_name": "mcp__workbay-handoff-mcp__review_findings",
        "tool_input": {
            "review": {
                "operation": "record",
                "finding_id": "D-1",
                "description": "[sr-001] cited twice [sr-001] in one finding.",
            }
        },
    }

    exit_code, _output = _run_hook(payload, cwd=str(tmp_path))

    assert exit_code == 0
    reflect_log = tmp_path / ".task-state" / "ace_reflect_log.jsonl"
    rows = [json.loads(line) for line in reflect_log.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["rule_id"] == "sr-001"


def test_record_shape_is_backward_compatible_without_optional_fields(tmp_path: Path) -> None:
    payload = {
        "tool_name": "mcp__workbay-handoff-mcp__review_findings",
        "tool_input": {
            "review": {
                "operation": "record",
                "finding_id": "C-1",
                "description": "Applied [sr-002] correctly.",
            }
        },
    }

    exit_code, _output = _run_hook(payload, cwd=str(tmp_path))

    assert exit_code == 0
    record = json.loads((tmp_path / ".task-state" / "ace_reflect_log.jsonl").read_text(encoding="utf-8").strip())
    assert set(record) >= {"finding_id", "rule_id", "contradicts", "timestamp"}
    assert record["finding_id"] == "C-1"
    assert record["rule_id"] == "sr-002"
    assert record["contradicts"] is False