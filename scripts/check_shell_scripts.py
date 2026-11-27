#!/usr/bin/env python3
"""Check tracked shell scripts for new ShellCheck warnings or errors."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

BASELINE = "config/lint/shellcheck_baseline.json"
SCHEMA = "workbay/shellcheck@1"
TIMEOUT_SECONDS = 60


def tracked_scripts(repo: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--deduplicate", "--", "scripts/"],  # noqa: S607 -- Git on PATH
        cwd=repo,
        capture_output=True,
        check=True,
        timeout=TIMEOUT_SECONDS,
    )
    return sorted(os.fsdecode(name) for name in result.stdout.split(b"\0") if name.endswith(b".sh"))


def new_findings(repo: Path, executable: str) -> list[dict]:
    baseline = json.loads((repo / BASELINE).read_text(encoding="utf-8"))
    allowed = baseline["allow"]
    if not isinstance(allowed, list) or any(not isinstance(key, str) for key in allowed):
        message = "ShellCheck baseline must contain a list of finding keys"
        raise ValueError(message)
    remaining = Counter(allowed)
    paths = tracked_scripts(repo)
    if not paths:
        return []
    result = subprocess.run(  # noqa: S603 -- resolved ShellCheck and tracked paths
        [executable, "--norc", "-S", "warning", "-f", "json", "--", *paths],
        cwd=repo,
        env={name: value for name, value in os.environ.items() if name != "SHELLCHECK_OPTS"},
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        timeout=TIMEOUT_SECONDS,
    )
    if result.returncode not in (0, 1):
        message = f"ShellCheck exited {result.returncode}: {result.stderr.strip()}"
        raise ValueError(message)
    diagnostics = json.loads(result.stdout)
    if not isinstance(diagnostics, list) or bool(diagnostics) != (result.returncode == 1):
        message = "ShellCheck returned inconsistent diagnostics"
        raise ValueError(message)

    sources = {}
    findings = []
    for diagnostic in sorted(diagnostics, key=lambda item: (item["file"], item["line"], item["column"], item["code"])):
        path = diagnostic["file"]
        if path not in paths:
            message = f"ShellCheck reported an unexpected path: {path}"
            raise ValueError(message)
        if path not in sources:
            sources[path] = (repo / path).read_bytes().splitlines()
        start, end = diagnostic["line"], diagnostic["endLine"]
        # Keep the finding tied to its source and column while permitting
        # unrelated insertions above it. Count repeated diagnostics separately.
        digest = hashlib.sha256(b"\n".join(sources[path][start - 1 : end])).hexdigest()
        key = f"{path}:SC{diagnostic['code']}:{diagnostic['column']}:{digest}"
        if remaining[key]:
            remaining[key] -= 1
        else:
            findings.append({**diagnostic, "key": key})
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--allow-missing-shellcheck", action="store_true")
    args = parser.parse_args()

    executable = shutil.which("shellcheck")
    if executable is None:
        print(
            json.dumps(
                {
                    "schema": SCHEMA,
                    "ok": args.allow_missing_shellcheck,
                    "degraded": args.allow_missing_shellcheck,
                    "kind": "shellcheck_missing",
                }
            )
        )
        return 0 if args.allow_missing_shellcheck else 2
    try:
        findings = new_findings(args.repo, str(Path(executable).absolute()))
    except (OSError, ValueError, LookupError, TypeError, subprocess.SubprocessError) as exc:
        print(
            json.dumps(
                {
                    "schema": SCHEMA,
                    "ok": False,
                    "degraded": False,
                    "kind": "shellcheck_failed",
                    "error": str(exc),
                }
            )
        )
        return 2
    print(json.dumps({"schema": SCHEMA, "ok": not findings, "degraded": False, "findings": findings}))
    return int(bool(findings))


if __name__ == "__main__":
    sys.exit(main())
