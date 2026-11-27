#!/usr/bin/env python3
"""Require a timeout on direct subprocess.run and communicate calls."""

import argparse
import ast
import hashlib
import json
import sys
import tokenize
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

BASELINE = "config/lint/subprocess_timeout_baseline.json"


def untimed_calls(tree: ast.AST) -> Iterator[tuple[ast.Call, str]]:
    """Recognize subprocess import spellings and method calls statically."""
    nodes = list(ast.walk(tree))
    modules = {"subprocess"}
    run_names = set()
    for node in nodes:
        if isinstance(node, ast.Import):
            modules.update(alias.asname for alias in node.names if alias.name == "subprocess" and alias.asname)
        elif isinstance(node, ast.ImportFrom) and node.module == "subprocess" and node.level == 0:
            for alias in node.names:
                if alias.name == "run":
                    run_names.add(alias.asname or alias.name)
                elif alias.name == "*":
                    run_names.add("run")

    for call in nodes:
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if isinstance(func, ast.Name) and func.id in run_names:
            name = "subprocess.run"
        elif isinstance(func, ast.Attribute) and func.attr == "communicate":
            name = "communicate"
        elif (
            isinstance(func, ast.Attribute)
            and func.attr == "run"
            and isinstance(func.value, ast.Name)
            and func.value.id in modules
        ):
            name = "subprocess.run"
        else:
            continue
        if not any(keyword.arg == "timeout" and not is_none(keyword.value) for keyword in call.keywords):
            yield call, name


def is_none(value: ast.expr) -> bool:
    return isinstance(value, ast.Constant) and value.value is None


def new_findings(repo: Path) -> list[dict]:
    baseline = json.loads((repo / BASELINE).read_text(encoding="utf-8"))
    allowed = baseline["allow"]
    if not isinstance(allowed, list) or any(not isinstance(key, str) for key in allowed):
        message = "Subprocess baseline must contain a list of call keys"
        raise ValueError(message)
    remaining = Counter(allowed)
    paths = (repo / "packages").glob("*/src/**/*.py")
    findings = []
    for path in sorted((*paths, *(repo / "scripts").rglob("*.py"))):
        filename = path.relative_to(repo).as_posix()
        with tokenize.open(path) as stream:
            tree = ast.parse(stream.read(), filename=filename)
        for call, name in sorted(untimed_calls(tree), key=lambda item: (item[0].lineno, item[0].col_offset)):
            # A moved or reformatted call keeps its allowance. An additional
            # copy consumes another allowance, even if the call AST is identical.
            digest = hashlib.sha256(ast.dump(call).encode()).hexdigest()
            key = f"{filename}:{digest}"
            if remaining[key]:
                remaining[key] -= 1
            else:
                findings.append({"file": filename, "line": call.lineno, "call": name, "key": key})
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        findings = new_findings(args.repo)
    except (OSError, ValueError, SyntaxError, LookupError, TypeError) as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(exc, file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"ok": not findings, "findings": findings}))
    else:
        for finding in findings:
            print(f"{finding['file']}:{finding['line']}: {finding['call']} needs an explicit timeout=")
    return int(bool(findings))


if __name__ == "__main__":
    sys.exit(main())
