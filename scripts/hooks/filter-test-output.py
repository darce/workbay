#!/usr/bin/env python3
"""PostToolUse hook: compact test-output summaries for context efficiency.

Fires after every Bash tool call. Detects pytest / vitest / phpunit output
and injects a compact summary via hookSpecificOutput.additionalContext.

The full output remains in the conversation (PostToolUse hooks cannot modify
tool output), but the summary directs the model's attention to what matters:
failures, errors, and key metrics.  For all-green runs the summary replaces
the cognitive overhead of scrolling past hundreds of dots and green lines.

Hook contract (Claude Code PostToolUse):
  stdin:  JSON with tool_input.command, tool_response.{stdout,stderr,exitCode}
  stdout: JSON with hookSpecificOutput.additionalContext (or {} to no-op)
  exit 0 always (observational hook, never blocks)
"""

from __future__ import annotations

import json
import re
import sys


# ---------------------------------------------------------------------------
# Command detection
# ---------------------------------------------------------------------------

_TEST_CMD_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bpytest\b"),
    re.compile(r"\bmake\s+test"),
    re.compile(r"\bvitest\b"),
    re.compile(r"\bphpunit\b"),
    re.compile(r"\bnpm\s+(?:run\s+)?test\b"),
    re.compile(r"\bnpx\s+vitest\b"),
]


def is_test_command(command: str) -> bool:
    """Return True if *command* looks like a test invocation."""
    return any(p.search(command) for p in _TEST_CMD_PATTERNS)


# ---------------------------------------------------------------------------
# pytest parser
# ---------------------------------------------------------------------------

_PYTEST_SUMMARY_RE = re.compile(r"={2,}\s+(.*?)\s+={2,}\s*$", re.MULTILINE)

_COUNT_PATTERNS: dict[str, re.Pattern[str]] = {
    "passed": re.compile(r"(\d+)\s+passed"),
    "failed": re.compile(r"(\d+)\s+failed"),
    "errors": re.compile(r"(\d+)\s+error"),
    "warnings": re.compile(r"(\d+)\s+warning"),
    "skipped": re.compile(r"(\d+)\s+skipped"),
    "deselected": re.compile(r"(\d+)\s+deselected"),
}

_DURATION_RE = re.compile(r"in\s+([\d.]+\s*s)")


_PYTEST_BLOCK_HEADER = re.compile(r"^_{5,}\s*(.*?)\s*_{5,}$", re.MULTILINE)


def _split_pytest_blocks(section: str) -> list[tuple[str, list[str]]]:
    """Return ``(name, body_lines)`` per block in a FAILURES/ERRORS section.

    pytest delimits blocks with a header line shaped ``____ test_name ____``.
    Splitting on the underscore runs alone also splits *inside* that header,
    producing one entry per run: the real name, then the body's first line
    masquerading as a second name. That doubled the entry count and, with a
    capped render, halved how many real names survived. Match the whole
    header line so a block stays a block [OBS-08].
    """
    blocks: list[tuple[str, list[str]]] = []
    headers = list(_PYTEST_BLOCK_HEADER.finditer(section))
    for index, header in enumerate(headers):
        name = header.group(1).strip()
        if not name:
            continue
        end = headers[index + 1].start() if index + 1 < len(headers) else len(section)
        body = [ln for ln in section[header.end():end].split("\n") if ln.strip()]
        blocks.append((name, body))
    return blocks


def _extract_pytest_failures(output: str) -> list[dict[str, str]]:
    section = re.search(
        r"={3,} FAILURES ={3,}\n(.*?)(?=\n={3,}|\Z)", output, re.DOTALL
    )
    if not section:
        return []
    return [
        {"name": name, "tail": "\n".join(body[-4:])}
        for name, body in _split_pytest_blocks(section.group(1))
    ]


def _extract_pytest_errors(output: str) -> list[dict[str, str]]:
    section = re.search(
        r"={3,} ERRORS ={3,}\n(.*?)(?=\n={3,}|\Z)", output, re.DOTALL
    )
    if not section:
        return []
    return [
        {"name": name, "tail": "\n".join(body[-4:])}
        for name, body in _split_pytest_blocks(section.group(1))
    ]


def parse_pytest(output: str) -> dict | None:
    """Parse pytest output into a structured result dict."""
    # Find the LAST ===...=== line that contains result keywords.
    # The first match is typically "test session starts", not the summary.
    matches = list(_PYTEST_SUMMARY_RE.finditer(output))
    if not matches:
        return None

    summary_line: str | None = None
    for m in reversed(matches):
        candidate = m.group(1)
        if any(k in candidate for k in ("passed", "failed", "error")):
            summary_line = candidate
            break

    if summary_line is None:
        return None

    counts: dict[str, int] = {}
    for key, pat in _COUNT_PATTERNS.items():
        m = pat.search(summary_line)
        counts[key] = int(m.group(1)) if m else 0

    dur_m = _DURATION_RE.search(summary_line)
    duration = dur_m.group(1) if dur_m else "?"

    return {
        "runner": "pytest",
        "counts": counts,
        "duration": duration,
        "failures": _extract_pytest_failures(output),
        "error_blocks": _extract_pytest_errors(output),
        "all_passed": counts["failed"] == 0 and counts["errors"] == 0,
    }


# ---------------------------------------------------------------------------
# vitest parser
# ---------------------------------------------------------------------------


def parse_vitest(output: str) -> dict | None:
    """Parse vitest output into a structured result dict."""
    # vitest summary: " Tests  2 failed | 47 passed (49)"
    summary_match = re.search(
        r"Tests?\s+(.*?)(?:\(\d+\))\s*$", output, re.MULTILINE
    )
    if not summary_match:
        return None

    summary_text = summary_match.group(1)
    passed = int(m.group(1)) if (m := re.search(r"(\d+)\s+passed", summary_text)) else 0
    failed = int(m.group(1)) if (m := re.search(r"(\d+)\s+failed", summary_text)) else 0

    dur_m = re.search(r"Duration\s+([\d.]+\s*[sm]?s?)", output)
    duration = dur_m.group(1) if dur_m else "?"

    fail_names = re.findall(r"FAIL\s+(.+?)(?:\s+\[|$)", output, re.MULTILINE)

    return {
        "runner": "vitest",
        "counts": {
            "passed": passed, "failed": failed,
            "errors": 0, "warnings": 0, "skipped": 0,
        },
        "duration": duration,
        # Keep every name: bounding belongs to the renderer, which announces
        # what it drops. Capping here destroys names before anything can
        # report them missing, so the render count would silently understate.
        "failures": [{"name": n.strip(), "tail": ""} for n in fail_names],
        "error_blocks": [],
        "all_passed": failed == 0,
    }


# ---------------------------------------------------------------------------
# phpunit parser
# ---------------------------------------------------------------------------


def parse_phpunit(output: str) -> dict | None:
    """Parse phpunit output into a structured result dict."""
    ok_match = re.search(
        r"OK\s+\((\d+)\s+tests?,\s*(\d+)\s+assertions?\)", output
    )
    if ok_match:
        tests = int(ok_match.group(1))
        return {
            "runner": "phpunit",
            "counts": {
                "passed": tests, "failed": 0,
                "errors": 0, "warnings": 0, "skipped": 0,
            },
            "duration": "?",
            "failures": [],
            "error_blocks": [],
            "all_passed": True,
        }

    fail_match = re.search(r"Tests:\s*(\d+).*?Failures:\s*(\d+)", output)
    if fail_match:
        total = int(fail_match.group(1))
        failures = int(fail_match.group(2))
        errors_m = re.search(r"Errors:\s*(\d+)", output)
        errors = int(errors_m.group(1)) if errors_m else 0
        return {
            "runner": "phpunit",
            "counts": {
                "passed": total - failures - errors, "failed": failures,
                "errors": errors, "warnings": 0, "skipped": 0,
            },
            "duration": "?",
            "failures": [],
            "error_blocks": [],
            "all_passed": False,
        }

    return None


# ---------------------------------------------------------------------------
# Summary formatting
# ---------------------------------------------------------------------------


# Detail (the failing assertion tail) is expensive per entry; a name is one
# line. So names survive far past the point where detail stops.
_FAILURE_DETAIL_CAP = 8
_FAILURE_NAME_CAP = 40
_ERROR_DETAIL_CAP = 4
_ERROR_NAME_CAP = 20


def _render_entries(
    lines: list[str],
    entries: list[dict],
    label: str,
    detail_cap: int,
    name_cap: int,
) -> None:
    """Append *entries* under *label*, stating outright whatever it drops.

    The first ``detail_cap`` entries render name + tail; the rest render the
    name alone, up to ``name_cap``. Anything still dropped is announced. A
    truncated list that does not admit its own truncation reads as complete
    coverage, which is exactly how a real regression gets filed as known
    noise -- the reader recognises the names shown and never learns there
    were others [OBS-08].
    """
    for index, entry in enumerate(entries[:name_cap]):
        lines.append(f"  {label}: {entry['name']}")
        tail = entry.get("tail") if index < detail_cap else None
        if tail:
            for tail_line in tail.split("\n"):
                lines.append(f"    {tail_line}")
            lines.append("")

    elided = len(entries) - min(len(entries), name_cap)
    if elided:
        lines.append(
            f"  ... {elided} more {label} not shown "
            f"({len(entries)} total; see the full output)"
        )


def format_summary(result: dict) -> str:
    """Format a structured test result into a compact summary string."""
    c = result["counts"]
    parts: list[str] = []
    if c.get("passed"):
        parts.append(f"{c['passed']} passed")
    if c.get("failed"):
        parts.append(f"{c['failed']} FAILED")
    if c.get("errors"):
        parts.append(f"{c['errors']} errors")
    if c.get("skipped"):
        parts.append(f"{c['skipped']} skipped")
    if c.get("warnings"):
        parts.append(f"{c['warnings']} warnings")

    header = f"[{result['runner']}] {', '.join(parts)} ({result['duration']})"

    if result["all_passed"]:
        return f"{header} -- all green, no action needed."

    lines = [header, ""]
    _render_entries(
        lines, result["failures"], "FAILED", _FAILURE_DETAIL_CAP, _FAILURE_NAME_CAP
    )
    _render_entries(
        lines, result["error_blocks"], "ERROR", _ERROR_DETAIL_CAP, _ERROR_NAME_CAP
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main hook entry
# ---------------------------------------------------------------------------


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, OSError):
        print("{}")
        return

    if isinstance(payload, dict):
        try:
            from _protocol import validate_event  # type: ignore[import-not-found]

            validate_event(payload, expected="PostToolUse")
        except ImportError:
            pass

    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        print("{}")
        return
    command = tool_input.get("command", "")
    if not isinstance(command, str) or not is_test_command(command):
        print("{}")
        return

    # tool_response is normally a dict ({stdout, stderr, exitCode}), but some
    # harnesses emit a bare string when the Bash tool itself failed (timeout,
    # process error, etc.). Defend against that — `or {}` is not enough because
    # a non-empty string is truthy and would fall through to .get().
    response = payload.get("tool_response")
    if not isinstance(response, dict):
        print("{}")
        return
    stdout = response.get("stdout") or ""
    if not isinstance(stdout, str) or len(stdout) < 30:
        print("{}")
        return

    # Try each parser in order of likelihood for this repo
    result = parse_pytest(stdout) or parse_vitest(stdout) or parse_phpunit(stdout)
    if not result:
        print("{}")
        return

    summary = format_summary(result)
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": summary,
        }
    }))


if __name__ == "__main__":
    main()
