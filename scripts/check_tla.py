#!/usr/bin/env python3
"""Run TLC on every cfg in a spec directory and compare against an expected-verdict table.

Promoted from the gdntla00 prototype (decision 48617); ``make tla-check`` runs it
on ``docs/specs/tla`` with ``config/tla/tla2tools.pin.json``. Stdlib only.

The jar is pinned: its path and sha256 come from a pin file (``~`` in ``jar`` is
expanded, a relative path resolves against the pin file's directory). The jar is
copied into the run's scratch directory while it is hashed, and TLC runs from
that copy, so the bytes checked are the bytes executed; a mismatch refuses
before any TLC process starts [SECD-05][SECD-07]. A run that cannot execute
reports a typed ``tlc_unavailable``, never PASS [CAL-02].

Exit codes (the outcome contract):
  0  pass             every cfg ran and matched its expected verdict within budget
  1  mismatch         any verdict/budget differs, a cfg is unlisted, a listed cfg
                      is missing, or no cfg ran at all
  2  tlc_unavailable  java or the pinned jar is absent
  3  refused          pin or table malformed/unset, or the jar sha256 differs

Verdicts: ``pass``, ``invariant_violated:<Name>``, ``property_violated``,
``deadlock``, ``timeout``, ``error``. A cfg whose expected verdict is a violation
(canaries, Fixed=FALSE arms, mutants) matches only when TLC reports exactly that
violation, so a vacuous spec surfaces as a mismatch.

TLC runs against a scratch copy of the spec directory (``--scratch``) so trace
files and the states directory never land in the source tree. The working copy
is removed after a PASS and kept (``workdir_retained``) otherwise, for triage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

SCHEMA = "gdntla00/tla-check-report@1"
TABLE_SCHEMA = "gdntla00/expected-verdicts@1"
EXIT = {"pass": 0, "mismatch": 1, "tlc_unavailable": 2, "refused": 3}
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_VERDICT = re.compile(r"^(pass|property_violated|deadlock|invariant_violated:[A-Za-z_][A-Za-z0-9_]*)$")
_INVARIANT = re.compile(r"Error: Invariant ([A-Za-z_][A-Za-z0-9_]*) is violated")
_ACTION_PROPERTY = re.compile(r"Error: Action property ([A-Za-z_][A-Za-z0-9_]*) is violated")
_DISTINCT = re.compile(r"([0-9][0-9,]*) distinct states found")
_DEFAULT_TIMEOUT = 300
_DEFAULT_BUDGET = 100_000
_TAIL = 2000


class Refused(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason, self.detail = reason, detail


class Unavailable(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason, self.detail = reason, detail


def _copy_hashing(source: Path, target: Path) -> str:
    """Copy ``source`` to ``target`` and return the sha256 of the bytes written."""
    digest = hashlib.sha256()
    with source.open("rb") as reader, target.open("xb") as writer:
        for block in iter(lambda: reader.read(1 << 20), b""):
            digest.update(block)
            writer.write(block)
    return digest.hexdigest()


def load_pin(pin_path: Path) -> dict[str, Any]:
    try:
        pin = json.loads(pin_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise Refused("pin_unreadable", str(exc)) from exc
    except ValueError as exc:
        raise Refused("pin_invalid", str(exc)) from exc
    if not isinstance(pin, dict) or not isinstance(pin.get("jar"), str) or not pin["jar"]:
        raise Refused("pin_invalid", "pin needs a nonempty string 'jar'")
    sha = pin.get("sha256")
    if not isinstance(sha, str) or not _HEX64.fullmatch(sha):
        raise Refused("pin_unset", "pin 'sha256' must be 64 lowercase hex characters")
    opts = pin.get("java_opts", [])
    if not isinstance(opts, list) or not all(isinstance(o, str) for o in opts):
        raise Refused("pin_invalid", "'java_opts' must be a list of strings")
    jar = Path(os.path.expanduser(pin["jar"]))
    if not jar.is_absolute():
        jar = pin_path.resolve().parent / jar
    return {"jar": jar, "sha256": sha, "java_opts": opts, "version": pin.get("tla2tools_version")}


def load_table(path: Path) -> dict[str, dict[str, Any]]:
    try:
        table = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise Refused("expected_table_invalid", str(exc)) from exc
    cfgs = table.get("cfgs") if isinstance(table, dict) else None
    if not isinstance(table, dict) or table.get("schema") != TABLE_SCHEMA or not isinstance(cfgs, dict):
        raise Refused("expected_table_invalid", f"schema must be {TABLE_SCHEMA} with a 'cfgs' object")
    defaults = table.get("defaults", {})
    out: dict[str, dict[str, Any]] = {}
    for name, entry in cfgs.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("verdict"), str):
            raise Refused("expected_table_invalid", f"{name}: entry needs a string 'verdict'")
        if not _VERDICT.fullmatch(entry["verdict"]):
            raise Refused("expected_table_invalid", f"{name}: unknown verdict {entry['verdict']!r}")
        budget = entry.get("max_distinct_states", defaults.get("max_distinct_states", _DEFAULT_BUDGET))
        timeout = entry.get("timeout_seconds", defaults.get("timeout_seconds", _DEFAULT_TIMEOUT))
        for key, value in (("max_distinct_states", budget), ("timeout_seconds", timeout)):
            if type(value) is not int or value <= 0:
                raise Refused("expected_table_invalid", f"{name}: {key} must be a positive integer")
        out[name] = {"verdict": entry["verdict"], "max_distinct_states": budget, "timeout_seconds": timeout}
    return out


def parse_tlc(output: str, exit_code: int | None) -> tuple[str, int | None]:
    """Map TLC output to a verdict; anything unrecognized is ``error``, never pass."""
    found = _DISTINCT.findall(output)
    distinct = int(found[-1].replace(",", "")) if found else None
    invariant = _INVARIANT.search(output)
    if invariant:
        return f"invariant_violated:{invariant.group(1)}", distinct
    if "Error: Temporal properties were violated" in output or _ACTION_PROPERTY.search(output):
        return "property_violated", distinct
    if "Error: Deadlock reached" in output:
        return "deadlock", distinct
    if "Model checking completed. No error has been found." in output and exit_code == 0 and "Error:" not in output:
        return "pass", distinct
    return "error", distinct


def _module_of(cfg: str) -> str:
    return cfg[: -len(".cfg")].split("__", 1)[0]


def run_cfg(java: str, pin: dict[str, Any], workdir: Path, cfg: str, expected: dict[str, Any]) -> dict[str, Any]:
    module = _module_of(cfg)
    metadir = workdir / ".tlc-meta" / cfg[: -len(".cfg")]
    command = [
        java,
        *pin["java_opts"],
        "-cp",
        str(pin["jar"]),
        "tlc2.TLC",
        "-workers",
        "1",
        "-metadir",
        str(metadir),
        "-config",
        cfg,
        f"{module}.tla",
    ]
    started = time.monotonic()
    try:
        proc = subprocess.run(
            command,
            cwd=workdir,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=expected["timeout_seconds"],
            check=False,
        )
        output, exit_code = proc.stdout + proc.stderr, proc.returncode
        verdict, distinct = parse_tlc(output, exit_code)
    except subprocess.TimeoutExpired as exc:
        raw = (exc.stdout or b"") + (exc.stderr or b"")
        output = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
        exit_code, verdict, distinct = None, "timeout", None
    except OSError as exc:
        output, exit_code, verdict, distinct = f"{type(exc).__name__}: {exc}", None, "error", None
    reason = None
    if verdict != expected["verdict"]:
        reason = "verdict_mismatch"
    elif distinct is None and verdict == "pass":
        reason = "distinct_states_unparsed"
    elif distinct is not None and distinct > expected["max_distinct_states"]:
        reason = "state_budget_exceeded"
    return {
        "cfg": cfg,
        "module": module,
        "expected": expected["verdict"],
        "observed": verdict,
        "distinct_states": distinct,
        "max_distinct_states": expected["max_distinct_states"],
        "exit_code": exit_code,
        "seconds": round(time.monotonic() - started, 3),
        "match": reason is None,
        "reason": reason,
        "command": command,
        "output_tail": output[-_TAIL:],
    }


def check(args: argparse.Namespace) -> dict[str, Any]:
    report: dict[str, Any] = {"schema": SCHEMA, "specs": str(args.specs), "expected_table": str(args.expected)}
    workdir: Path | None = None
    try:
        pin = load_pin(args.pin)
        report["pin"] = {"jar": str(pin["jar"]), "sha256": pin["sha256"], "version": pin["version"]}
        table = load_table(args.expected)
        if not pin["jar"].is_file():
            raise Unavailable("jar_missing", str(pin["jar"]))
        try:
            args.scratch.mkdir(parents=True, exist_ok=True)
            workdir = Path(tempfile.mkdtemp(prefix="tla-run-", dir=args.scratch))
            run_jar = workdir / "tla2tools.jar"
            observed_sha = _copy_hashing(pin["jar"], run_jar)
        except OSError as exc:
            raise Unavailable("jar_unreadable", f"{type(exc).__name__}: {exc}") from exc
        report["pin"]["observed_sha256"] = observed_sha
        if observed_sha != pin["sha256"]:
            raise Refused("jar_sha256_mismatch", f"expected {pin['sha256']} observed {observed_sha}")
        found = shutil.which(os.path.expanduser(args.java))
        if found is None:
            raise Unavailable("java_not_found", args.java)
        java = os.path.abspath(found)  # TLC runs with cwd=scratch copy; a relative path would not resolve
    except (Refused, Unavailable) as exc:
        if workdir is not None:
            shutil.rmtree(workdir, ignore_errors=True)
        status = "refused" if isinstance(exc, Refused) else "tlc_unavailable"
        return {**report, "status": status, "reason": exc.reason, "detail": exc.detail, "results": []}

    on_disk = sorted(p.name for p in args.specs.glob("*.cfg"))
    problems = [{"cfg": n, "reason": "unlisted_cfg"} for n in on_disk if n not in table]
    problems += [{"cfg": n, "reason": "missing_cfg"} for n in sorted(table) if n not in on_disk]
    selected = [n for n in on_disk if n in table]  # no subset mode: a partial run is never PASS
    for source in sorted(args.specs.iterdir()) if args.specs.is_dir() else []:
        if source.is_file() and source.suffix in {".tla", ".cfg"}:
            shutil.copy2(source, workdir / source.name)
    report["workdir"] = str(workdir)
    run_pin = {**pin, "jar": run_jar}
    results = [run_cfg(java, run_pin, workdir, name, table[name]) for name in selected]
    mismatches = [r for r in results if not r["match"]] + problems
    if not results:
        mismatches.append({"cfg": None, "reason": "no_cfgs_ran"})
    status = "mismatch" if mismatches else "pass"
    retained = status != "pass"
    if not retained:
        shutil.rmtree(workdir, ignore_errors=True)
    return {
        **report,
        "status": status,
        "results": results,
        "problems": problems,
        "mismatch_count": len(mismatches),
        "matched": sum(1 for r in results if r["match"]),
        "ran": len(results),
        "workdir_retained": retained,
    }


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pin", type=Path, required=True, help="tla2tools pin JSON {jar, sha256, java_opts}")
    parser.add_argument("--specs", type=Path, required=True, help="directory holding *.tla and *.cfg")
    parser.add_argument("--expected", type=Path, required=True, help="expected-verdict table JSON")
    parser.add_argument("--scratch", type=Path, required=True, help="scratch root for TLC working copies")
    parser.add_argument("--java", default="java", help="java executable (name on PATH or path)")
    parser.add_argument("--report", type=Path, default=None, help="also write the JSON report here")
    args = parser.parse_args(argv)
    report = check(args)
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.report is not None:
        try:
            _write_report(args.report, report)
        except OSError as exc:
            report["report_error"] = f"{type(exc).__name__}: {exc}"
            if report["status"] == "pass":  # an unrecorded PASS is not a PASS
                report["status"] = "mismatch"
                report["reason"] = "report_unavailable"
            text = json.dumps(report, indent=2, sort_keys=True)
    print(text)
    return EXIT[report["status"]]


if __name__ == "__main__":
    sys.exit(main())
