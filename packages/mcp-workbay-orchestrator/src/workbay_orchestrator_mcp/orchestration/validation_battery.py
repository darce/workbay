"""Harness-run validation battery for a lane candidate (decision 40377).

The battery replays, deterministically and without producer prose, the checks
a reviewer re-runs by hand today, and writes ``validation.json`` (schema
``workbay/validation-battery@1``):

1. ``red_on_base``: :func:`red_green_gate.verify_red_green`, mapped onto the
   battery statuses. Its red/green logic is reused, never reimplemented.
2. ``hunk_revert``: reverse-apply one production hunk at a time at the tip and
   run ``test_cmd``. A hunk whose revert leaves the tests green survived; only
   a behavioural test failure (rc 1) kills it. Harness errors are ``unknown``.
3. ``matched_suite``: ``suite_cmd`` on base, tip and base again; the second
   base run is an A/A null test that marks flaky ids [EXP-13].
4. ``acceptance``: :func:`acceptance_contract.run_contract` observes external
   frozen Python cases on both exact revisions, or a legacy tip command.

The explicit ``--profile full40377`` additionally runs actual qualified252r2
MODEL/property checks before candidate commands and selected regressions from
held regression-selection@1 JSON. All six categories are required: missing or
not-applicable evidence is UNKNOWN. Depth, states, deadline and seed must be
supplied. See ``docs/contracts/validation-battery-full40377-v1.md`` for the API,
selection/catalog format, external supervision and bounded qualification scope.

Readiness is the weakest applicable category, never an average [EVAL-23], and
``unknown`` is a first-class result when evidence is missing [CAL-02]. Nothing
automatically adopts these results into lifecycle state; the CLI exit status is
its outcome contract [AGT-21].

Public CLI (run from the independently owned harness installation)::

    python -m workbay_orchestrator_mcp.orchestration.validation_battery \
      --repo REPO --base BASE --tip TIP --test-cmd 'python -m pytest tests/test_fix.py' \
      --suite-cmd 'python -m pytest tests' --out NEW_VALIDATION.json \
      --acceptance-manifest EXTERNAL_MANIFEST.json \
      --acceptance-cases-root EXTERNAL_CASES --acceptance-expected-digest HELD_SHA256

The three acceptance binding arguments are all-or-none and mutually exclusive
with ``--acceptance-cmd``. Freeze using the acceptance_contract module *before*
dispatch; the coordinator must own the cases, manifest, expected digest and
source refs outside the measured lane. A digest establishes byte identity, not
authority or independence. Revision aliases are resolved before contract use.
Cases run under the actual harness Python with the contract's ``-S -P``
observer, pinned command, seed, timeout and revision-local imports. Assertion
RED on base and GREEN on tip can qualify a fix; both GREEN cannot. Import,
collection, timeout and checkout faults remain UNKNOWN. Invalid held pins are
usage errors (64), not evidence of a product failure.

``run_battery(..., acceptance_manifest=..., acceptance_cases_root=...,
acceptance_expected_digest=..., out_path=...)`` exposes the same API. Bound
runs retain contract receipt.json and logs in a new ``OUT.acceptance`` directory
beside the battery output; choose a fresh output name for each execution. The
execution evidence embeds the actual receipt, its byte digest and location,
source tuple, observer commands and runtime identity. Volatile receipt bytes
are covered by evidence_digest, outside the stable validation_digest.

``--evidence-role`` / ``evidence_role`` is ``positive_suite`` (default for a
bound invocation), ``negative_mutant`` or ``auxiliary``. Only a positive suite
with explicit full40377, fix_observed and all required categories passing has ``qualification.qualified_pass``
in execution_evidence. Passing rejection wrappers must be labelled auxiliary or
negative_mutant; their success never qualifies a candidate. Role labels are
caller assertions, not attestations, and must be held by the coordinator too.
Per-command observations distinguish positive controls from hunk mutations.

The default legacy profile is explicitly partial and unqualified, even with
frozen acceptance. Without the binding, invocation is ``legacy_invocation``
(default role auxiliary): the existing readiness/exit semantics and @1 schema
remain. ``--acceptance-cmd`` exit 0 does not claim
independent frozen acceptance. Exit codes: 0 readiness pass, 1 fail, 2 unknown,
64 invalid inputs, 70 internal I/O/software error. Consumers seeking qualified
PASS must check execution_evidence.qualification, not exit 0 alone.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from . import acceptance_contract
from .red_green_gate import (
    _harness_failure_type,
    _is_test_path,
    _isolated_base_command,
    _lane_check_timeout_seconds,
    _run_test_command,
    _site_roots_without_candidate_hooks,
    verify_red_green,
)

__all__ = ["BATTERY_VERSION", "SCHEMA", "main", "run_battery"]

SCHEMA = "workbay/validation-battery@1"
BATTERY_VERSION = "1"
MAX_HUNKS = 40
FULL40377_REQUIRED = (
    "acceptance",
    "red_on_base",
    "hunk_revert",
    "matched_suite",
    "model_properties",
    "regression_suite",
)
_PROFILES = ("legacy", "full40377")
_MODEL_DIRECTORY = "packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration"
_MODEL_FILES = ("validation_state_model.py", "statechart_conformance.py", "lifecycle_statechart.json")
_MAX_SELECTED = 256

# Harness-owned bootstrap, not a checker implementation. The actual revision's
# qualified252r2 API is invoked without its optional alternate-checker seam.
# The separate descriptor keeps ordinary candidate stdout out of the protocol.
_REVISION_OBSERVER = r"""
import hashlib, importlib, json, os, sys
from pathlib import Path
root = Path(sys.argv[1]).resolve()
mode = sys.argv[2]
request = json.loads(sys.argv[3])
fd = int(sys.argv[4])
roots = [root / 'src']
if (root / 'packages').is_dir():
    roots += [item for p in sorted((root / 'packages').iterdir()) for item in (p / 'src', p) if item.is_dir()]
roots.append(root)
local = [p.resolve() for p in roots if p.is_dir()]
sys.path[:0] = [str(p) for p in local] + request.get('dependency_roots', [])
names = {p.stem for r in local for p in r.glob('*.py')}
names |= {p.name for r in local for p in r.iterdir() if p.is_dir() and (p / '__init__.py').is_file()}

def audit():
    origins = {}
    for name, module in list(sys.modules.items()):
        origin = getattr(module, '__file__', None)
        if not origin:
            continue
        path = Path(origin).resolve()
        if name.split('.')[0] in names or name.startswith('workbay_'):
            if not path.is_relative_to(root):
                raise RuntimeError('revision import escaped checkout: ' + name + ': ' + str(path))
        if path.is_relative_to(root) and path.is_file():
            origins[name] = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    return origins

if mode == 'model':
    model = importlib.import_module('workbay_orchestrator_mcp.orchestration.validation_state_model')
    checker = importlib.import_module('workbay_orchestrator_mcp.orchestration.statechart_conformance')
    directory = root / request['directory']
    if Path(model.__file__).resolve() != directory / 'validation_state_model.py' or Path(checker.__file__).resolve() != directory / 'statechart_conformance.py':
        raise RuntimeError('MODEL/checker origin mismatch')
    before = audit()
    report = model.run_model(directory / 'lifecycle_statechart.json', **request['bounds'])
    result = {'report': report, 'origins_before': before, 'origins_after': audit()}
    code = {'pass': 0, 'fail': 1, 'unknown': 2}.get(report.get('status'), 2)
else:
    import pytest
    class Observer:
        def __init__(self):
            self.collected = []
        @pytest.hookimpl(trylast=True)
        def pytest_collection_finish(self, session):
            self.collected = [item.nodeid for item in session.items]
    observer = Observer()
    before = audit()
    code = int(pytest.main(request['args'], plugins=[observer]))
    result = {'collected': observer.collected, 'origins_before': before, 'origins_after': audit()}
raw = json.dumps(result, sort_keys=True, allow_nan=False).encode()
with os.fdopen(fd, 'wb') as stream:
    stream.write(raw)
sys.exit(code)
"""

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_UNKNOWN = 2
EXIT_USAGE = 64
EXIT_SOFTWARE = 70

_STATUS_RANK = {"fail": 0, "unknown": 1, "pass": 2}
_READINESS_EXIT = {"pass": EXIT_PASS, "fail": EXIT_FAIL, "unknown": EXIT_UNKNOWN}
_MAX_SEED = 4_294_967_295
_EVIDENCE_ROLES = ("positive_suite", "negative_mutant", "auxiliary")

# verify_red_green reasons that prove the candidate itself is wrong. Every other
# non-``red_green`` reason means the harness could not establish the proof.
_RED_ON_BASE_FAIL_REASONS = frozenset({"base_passed", "no_test_changes", "empty_patch"})
# Only stable fields enter the digest; output tails and error text carry
# durations and scratch paths, so they are kept in ``timing`` [TEST-08].
_RED_ON_BASE_EVIDENCE_KEYS = (
    "base_exit_code",
    "base_failure_type",
    "base_outcome",
    "changed_test_files",
    "timeout_seconds",
    "tip_exit_code",
    "tip_failure_type",
    "tip_outcome",
)
_RED_ON_BASE_VOLATILE_KEYS = ("base_output_tail", "error", "tip_output_tail")
_OUTPUT_TAIL_CHARS = 2_000

# Pin every git diff knob a user config could change, so hunk boundaries are a
# function of the two commits only.
_DIFF_OPTIONS = (
    "--no-color",
    "--no-ext-diff",
    "--no-textconv",
    "--no-renames",
    "--no-relative",
    "--binary",
    "--diff-algorithm=myers",
    "--indent-heuristic",
    "--inter-hunk-context=0",
    "--unified=3",
    "--src-prefix=a/",
    "--dst-prefix=b/",
)
_JUNIT_OUTCOME_RANK = {"passed": 0, "skipped": 1, "failed": 2, "error": 3}
_BROKEN_OUTCOMES = frozenset({"failed", "error"})
_LIMITATIONS = (
    "execution evidence requires an independently owned harness and frozen command/acceptance contract; "
    "candidate-controlled subprocess output and reports are observations, not trusted attestation [OBS-09]",
    "runtime identity identifies the harness Python and resolved pytest interpreter binary; arbitrary shell "
    "child interpreters, environment dependencies and executed code are not attested",
    "red_on_base execution observations reuse the gate's exit codes and bounded output tails; "
    "their hashes cover those tails, not full subprocess output; revision lineage is the gate contract "
    "and isolated command previews use tip cwd, not direct observation of the base subprocess",
    "hunk_revert treats every non-test file in base..tip as production; a hunk no test can observe "
    "(documentation, configuration) survives by construction",
    "hunk_revert and matched_suite read exit codes and junit XML written by candidate-controlled "
    "processes; the battery does not sandbox them from tampering [OBS-09]",
    "matched_suite runs one A/A repeat of base; a flake that does not flip in that pair is not detected",
    "matched_suite requires a pytest-shaped suite_cmd (red_green_gate's bounded command grammar); "
    "otherwise it is unknown",
    "commands run under POSIX sh with PYTHONHASHSEED and PYTHONDONTWRITEBYTECODE exported first; "
    "Windows shells are not supported",
)


@dataclass(frozen=True)
class _Hunk:
    path: str
    label: str
    patch: str

    @property
    def ident(self) -> str:
        return f"{self.path}:{self.label}"


@dataclass(frozen=True)
class _Run:
    """One command execution; ``returncode`` is ``None`` when it timed out."""

    returncode: int | None
    output: str
    seconds: float
    observation: dict[str, Any] = field(default_factory=dict)

    @property
    def harness_failure(self) -> str | None:
        if self.observation.get("identity_mismatch"):
            return str(self.observation["identity_mismatch"])
        if self.returncode is None:
            return "timeout"
        if self.returncode == 0:
            return None
        # Import failures from plain Python also exit 1. They do not establish
        # assertion RED. This can only downgrade evidence to unknown; process
        # output is not an independent attestation of what code executed.
        if self.returncode == 1 and re.search(r"(?m)^(?:ModuleNotFoundError|ImportError):", self.output):
            return "import_error"
        if self.returncode == 1 and re.search(r"(?m)^(?:SyntaxError|IndentationError|TabError):", self.output):
            return "collection_error"
        return _harness_failure_type(subprocess.CompletedProcess("", self.returncode))


class _Timing:
    """Sibling block for every wall-clock-dependent observation [TEST-08]."""

    def __init__(self) -> None:
        self.durations: dict[str, float] = {}
        self.output: dict[str, str] = {}
        self.observations: dict[str, dict[str, Any]] = {}
        self.external_contract: dict[str, Any] | None = None

    def record(self, label: str, seconds: float, output: str) -> None:
        self.durations[label] = round(seconds, 6)
        self.output[label] = output[-_OUTPUT_TAIL_CHARS:]

    def record_run(self, label: str, run: _Run, *, report: Path | None = None) -> None:
        self.record(label, run.seconds, run.output)
        observation = dict(run.observation)
        observation.update(
            exit_code=run.returncode,
            failure_type=run.harness_failure,
            output_sha256=_sha256(run.output.encode("utf-8", "surrogateescape")),
            output_scope="bounded_stdout_then_stderr",
        )
        if report is not None:
            observation["report_sha256"] = _file_sha256(report) if report.is_file() else None
        self.observations[label] = observation

    def as_dict(self) -> dict[str, Any]:
        return {
            "durations_seconds": dict(sorted(self.durations.items())),
            "output_tails": dict(sorted(self.output.items())),
        }


def _git(cwd: Path, *args: str, check: bool = True, input: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "--literal-pathspecs", "-C", str(cwd), *args],
        check=check,
        capture_output=True,
        input=input,
        text=True,
        encoding="utf-8",
        errors="surrogateescape",
    )


def _resolve_commit(repository: Path, rev: str) -> str:
    resolved = _git(
        repository, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{rev}^{{commit}}", check=False
    )
    if resolved.returncode != 0 or not resolved.stdout.strip():
        raise ValueError(f"not a commit in {repository}: {rev!r}")
    return resolved.stdout.strip()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path) -> str:
    return _sha256(path.read_bytes())


def _revision_identity(repository: Path, commit: str) -> dict[str, str]:
    return {"commit": commit, "tree": _git(repository, "rev-parse", f"{commit}^{{tree}}").stdout.strip()}


def _observed_source(worktree: Path) -> dict[str, str]:
    observed = _revision_identity(worktree, _resolve_commit(worktree, "HEAD"))
    # Includes staged changes too (RED overlays and reversed production hunks).
    observed["diff_sha256"] = _sha256(
        _git(worktree, "diff", *_DIFF_OPTIONS, "HEAD").stdout.encode("utf-8", "surrogateescape")
    )
    return observed


class _SourceIdentityMismatch(RuntimeError):
    pass


def _binary_identity(path: Path) -> dict[str, str]:
    return {
        "executable": str(path),
        "resolved_executable": str(path.resolve()),
        "binary_sha256": _file_sha256(path),
    }


def _check_interpreter(command: str, cwd: Path) -> dict[str, Any]:
    isolated = _isolated_base_command(command, cwd=cwd)
    if isolated is None:
        return {"status": "unavailable", "reason": "arbitrary_shell_command"}
    # A bare pytest script can have a shebang unrelated to its sibling Python.
    # The gate's isolation rewrite selects that sibling only for the base arm.
    tokens = shlex.split(command)
    if any(Path(token).name == "pytest" and tokens[index - 1 : index] != ["-m"] for index, token in enumerate(tokens)):
        return {"status": "unavailable", "reason": "pytest_script_runtime_unverified"}
    return {"status": "resolved_command", **_binary_identity(isolated[1])}


@contextmanager
def _scratch_worktree(repository: Path, commit: str, label: str) -> Iterator[Path]:
    """Detached worktree at *commit*, removed on every exit path.

    Mirrors verify_red_green's base-worktree lifecycle: remember whether
    ``worktree add`` succeeded and force-remove in ``finally``.
    """
    with tempfile.TemporaryDirectory(prefix=f"workbay-battery-{label}-") as scratch:
        worktree = Path(scratch) / label
        added = False
        try:
            _git(repository, "worktree", "add", "--detach", str(worktree), commit)
            added = True
            expected = _revision_identity(repository, commit)
            actual = _revision_identity(worktree, _resolve_commit(worktree, "HEAD"))
            if actual != expected:
                raise _SourceIdentityMismatch(f"expected {expected}, observed {actual}")
            # Same reason as verify_red_green: the lane virtualenv is untracked,
            # so relative interpreter paths need it materialized.
            lane_venv = repository / ".venv"
            scratch_venv = worktree / ".venv"
            if lane_venv.exists() and not scratch_venv.exists() and not scratch_venv.is_symlink():
                scratch_venv.symlink_to(lane_venv.resolve(), target_is_directory=True)
            yield worktree
        finally:
            if added:
                cleanup = _git(repository, "worktree", "remove", "--force", "--force", str(worktree), check=False)
                if cleanup.returncode != 0:
                    raise RuntimeError(f"scratch worktree cleanup failed: {(cleanup.stderr or cleanup.stdout).strip()}")


def _restore(worktree: Path) -> None:
    """Return a scratch worktree to the pristine checkout of its HEAD."""
    _git(worktree, "reset", "--hard", "--quiet")
    _git(worktree, "clean", "-fdxq", "-e", "/.venv")


def _run_command(command: str, *, cwd: Path, seed: int, timeout: float) -> _Run:
    # Export instead of a command prefix so compound commands are seeded too.
    seeded = f"export PYTHONHASHSEED={seed} PYTHONDONTWRITEBYTECODE=1; {command}"
    observation: dict[str, Any] = {
        "command": command.strip(),
        "executed_command": seeded,
        "seed": seed,
        "timeout_seconds": timeout,
        "source": _observed_source(cwd),
        "check_interpreter": _check_interpreter(command, cwd),
    }
    started = time.monotonic()
    try:
        result = _run_test_command(seeded, cwd=cwd, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        streams = (exc.stdout, exc.stderr)
        output = "".join(part.decode("utf-8", "replace") if isinstance(part, bytes) else part or "" for part in streams)
        return _Run(None, output, time.monotonic() - started, observation)
    observation["source_after"] = _observed_source(cwd)
    # A command that moves HEAD cannot attest execution against the requested
    # revision. Keep the process outcome separate from this harness fault.
    before = observation["source"]
    after = observation["source_after"]
    if any(before[key] != after[key] for key in ("commit", "tree")):
        observation["identity_mismatch"] = "source_identity_mismatch"
    interpreter_after = _check_interpreter(command, cwd)
    observation["check_interpreter_after"] = interpreter_after
    if interpreter_after != observation["check_interpreter"]:
        observation["identity_mismatch"] = "runtime_identity_mismatch"
    return _Run(
        result.returncode, f"{result.stdout or ''}{result.stderr or ''}", time.monotonic() - started, observation
    )


def _category(status: str, reason: str, **evidence: Any) -> dict[str, Any]:
    return {"status": status, "reason": reason, "evidence": evidence}


def _red_on_base_status(result: dict[str, Any]) -> str:
    reason = result.get("reason")
    if reason == "red_green":
        return "pass"
    if reason in _RED_ON_BASE_FAIL_REASONS:
        return "fail"
    # A changed test that fails on the tip by assertion is a candidate defect;
    # a tip harness error is not evidence either way.
    if reason == "tip_failed" and result.get("tip_outcome") == "failed":
        return "fail"
    return "unknown"


def _red_on_base(repository: Path, base: str, tip: str, test_cmd: str, *, seed: int, timing: _Timing) -> dict[str, Any]:
    command = f"PYTHONHASHSEED={seed} {test_cmd}"
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="workbay-battery-patch-") as scratch:
        turn_patch = Path(scratch) / "turn.patch"
        diff = _git(repository, "diff", *_DIFF_OPTIONS, base, tip).stdout
        turn_patch.write_text(diff, encoding="utf-8", errors="surrogateescape")
        with _scratch_worktree(repository, tip, "red-on-base") as lane:
            interpreter = _check_interpreter(command, lane)
            isolated = _isolated_base_command(command, cwd=lane)
            result = verify_red_green(lane, base, tip, command, turn_patch)
    for arm, commit in (("tip", tip), ("base", base)):
        # These fields are produced by verify_red_green's own subprocess path,
        # never by lane receipt files. Timeouts deliberately carry no exit code.
        if result.get(f"{arm}_exit_code") is None and result.get("reason") != f"{arm}_timeout":
            continue
        tail = result.get(f"{arm}_output_tail", "")
        timing.observations[f"red_on_base.{arm}"] = {
            "source": _revision_identity(repository, commit),
            "source_basis": "gate_contract_not_direct_observation",
            "source_scope": "candidate_tests_over_base" if arm == "base" else "tip_checkout",
            "command": command,
            "isolated_command_preview": isolated[0] if arm == "base" and isolated is not None else None,
            "command_basis": "gate_input_with_tip_cwd_isolation_preview",
            "check_interpreter": interpreter,
            "seed": seed,
            "timeout_seconds": result.get("timeout_seconds"),
            "exit_code": result.get(f"{arm}_exit_code"),
            "failure_type": "timeout"
            if result.get("reason") == f"{arm}_timeout"
            else result.get(f"{arm}_failure_type"),
            "output_sha256": _sha256(tail.encode("utf-8", "surrogateescape")),
            "output_scope": "gate_output_tail",
        }
    output = "".join(f"[{key}]\n{result[key]}\n" for key in _RED_ON_BASE_VOLATILE_KEYS if result.get(key))
    timing.record("red_on_base", time.monotonic() - started, output)
    evidence = {key: result[key] for key in _RED_ON_BASE_EVIDENCE_KEYS if key in result}
    return _category(_red_on_base_status(result), str(result.get("reason")), command=command, **evidence)


def _split_file_patch(path: str, file_patch: str) -> list[_Hunk]:
    lines = file_patch.splitlines(keepends=True)
    starts = [index for index, line in enumerate(lines) if line.startswith("@@ ")]
    if not starts:
        # Binary, mode-only and empty-file changes revert as one whole-file unit.
        label = "binary" if any(line.startswith("GIT binary patch") for line in lines) else "header-only"
        return [_Hunk(path, label, file_patch)]
    header = "".join(lines[: starts[0]])
    hunks = []
    for start, end in zip(starts, [*starts[1:], len(lines)]):
        hunk_header = lines[start]
        label = hunk_header[: hunk_header.index(" @@", 3) + 3]
        hunks.append(_Hunk(path, label, header + "".join(lines[start:end])))
    return hunks


def _production_hunks(repository: Path, base: str, tip: str) -> list[_Hunk]:
    names = _git(repository, "diff", "--no-renames", "--no-relative", "--name-only", "-z", base, tip).stdout
    hunks: list[_Hunk] = []
    for path in sorted(name for name in names.split("\0") if name and not _is_test_path(name)):
        file_patch = _git(repository, "diff", *_DIFF_OPTIONS, base, tip, "--", path).stdout
        if file_patch:
            hunks.extend(_split_file_patch(path, file_patch))
    return hunks


def _hunk_revert(
    repository: Path,
    base: str,
    tip: str,
    test_cmd: str,
    *,
    seed: int,
    timeout: float,
    timing: _Timing,
    max_hunks: int = MAX_HUNKS,
) -> dict[str, Any]:
    hunks = _production_hunks(repository, base, tip)
    if not hunks:
        return _category("not_applicable", "no_production_hunks", total_hunks=0, max_hunks=max_hunks, selected_hunks=[])
    selected = (
        [hunks[index] for index in sorted(random.Random(seed).sample(range(len(hunks)), max_hunks))]
        if len(hunks) > max_hunks
        else hunks
    )
    evidence: dict[str, Any] = {
        "command": test_cmd,
        "max_hunks": max_hunks,
        "selected_hunks": [hunk.ident for hunk in selected],
        "total_hunks": len(hunks),
        "truncated": len(hunks) > len(selected),
    }
    entries: list[dict[str, Any]] = []
    with _scratch_worktree(repository, tip, "hunk-revert") as worktree:
        baseline = _run_command(test_cmd, cwd=worktree, seed=seed, timeout=timeout)
        timing.record_run("hunk_revert.baseline", baseline)
        evidence["baseline_exit_code"] = baseline.returncode
        if baseline.returncode != 0 or baseline.harness_failure is not None:
            # Every revert would look killed if the unmutated tip is already red.
            return _category("unknown", "tip_baseline_not_green", **evidence)
        for index, hunk in enumerate(selected):
            _restore(worktree)
            entry: dict[str, Any] = {"file": hunk.path, "hunk": hunk.label}
            applied = _git(worktree, "apply", "-R", "--whitespace=nowarn", "-", input=hunk.patch, check=False)
            if applied.returncode != 0:
                entry.update(outcome="unknown", reason="reverse_apply_failed")
            else:
                run = _run_command(test_cmd, cwd=worktree, seed=seed, timeout=timeout)
                timing.record_run(f"hunk_revert.{index:03d}", run)
                entry["exit_code"] = run.returncode
                if run.returncode is None:
                    entry.update(outcome="unknown", reason="timeout")
                elif run.harness_failure is not None:
                    # rc != 0 alone is not a kill: harness errors establish no
                    # behavioural failure. Reuse the gate's typed classifier.
                    entry.update(outcome="unknown", reason="harness_failure", failure_type=run.harness_failure)
                else:
                    entry["outcome"] = "killed" if run.returncode == 1 else "survived"
            entries.append(entry)
    survivors = [f"{entry['file']}:{entry['hunk']}" for entry in entries if entry["outcome"] == "survived"]
    unknown = [f"{entry['file']}:{entry['hunk']}" for entry in entries if entry["outcome"] == "unknown"]
    evidence.update(
        hunks=entries,
        killed=sum(entry["outcome"] == "killed" for entry in entries),
        survivors=survivors,
        unknown=unknown,
    )
    if survivors:
        return _category("fail", "hunks_survived", **evidence)
    if unknown:
        return _category("unknown", "hunks_unknown", **evidence)
    if evidence["truncated"]:
        # Hunks past the cap were never reverted; their outcome is unknown.
        return _category("unknown", "truncated_without_survivors", **evidence)
    return _category("pass", "all_hunks_killed", **evidence)


def _junit_outcomes(report: Path) -> dict[str, str] | None:
    try:
        root = ET.parse(report).getroot()
    except (OSError, ET.ParseError):
        return None
    outcomes: dict[str, str] = {}
    for case in root.iter("testcase"):
        test_id = f"{case.get('classname', '')}::{case.get('name', '')}"
        tags = {child.tag for child in case}
        if "error" in tags:
            outcome = "error"
        elif "failure" in tags:
            outcome = "failed"
        elif "skipped" in tags:
            outcome = "skipped"
        else:
            outcome = "passed"
        previous = outcomes.get(test_id)
        if previous is None or _JUNIT_OUTCOME_RANK[outcome] > _JUNIT_OUTCOME_RANK[previous]:
            outcomes[test_id] = outcome
    return outcomes


def _matched_suite(
    repository: Path,
    base: str,
    tip: str,
    suite_cmd: str | None,
    *,
    seed: int,
    timeout: float,
    timing: _Timing,
    node_ids: list[str] | None = None,
    artifacts: Path | None = None,
    required_coverage: bool = False,
) -> dict[str, Any]:
    if suite_cmd is None and node_ids is None:
        return _category("not_applicable", "no_suite_cmd")
    selected = node_ids is not None
    name = "regression_suite" if selected else "matched_suite"
    evidence: dict[str, Any] = {"command": suite_cmd}
    if selected:
        evidence.update(node_ids=node_ids, execution="collect_then_execute_revision_local_pytest")
    outcomes: dict[str, dict[str, str]] = {}
    exit_codes: dict[str, int | None] = {}
    with (
        tempfile.TemporaryDirectory(prefix="workbay-battery-junit-") as reports,
        _scratch_worktree(repository, base, "suite-base") as base_worktree,
        _scratch_worktree(repository, tip, "suite-tip") as tip_worktree,
    ):
        # Reuse red_green_gate's bounded pytest grammar as the shape check, so
        # an appended --junitxml always reaches a pytest argument list.
        if not selected and _isolated_base_command(suite_cmd, cwd=tip_worktree) is None:
            return _category("unknown", "suite_cmd_not_pytest_shaped", **evidence)
        # base_b is the A/A arm: identical to base_a, run after the tip.
        for arm, worktree in (("base_a", base_worktree), ("tip", tip_worktree), ("base_b", base_worktree)):
            _restore(worktree)
            report = artifacts / f"{name}.{arm}.xml" if selected else Path(reports) / f"{arm}.xml"
            if selected:
                assert artifacts is not None
                for node in node_ids:
                    path = worktree / _node_path(node)
                    if not path.resolve().is_relative_to(worktree) or not path.is_file():
                        return _category("unknown", f"{arm}_node_unavailable", **evidence)
                expected = _revision_identity(repository, tip if arm == "tip" else base)
                collect_run, collected = _revision_pytest(
                    worktree,
                    expected,
                    node_ids,
                    collect=True,
                    seed=seed,
                    timeout=timeout,
                    artifacts=artifacts,
                    label=f"{name}.{arm}.collect",
                )
                timing.record_run(f"{name}.{arm}.collect", collect_run)
                if not collected or not isinstance(collected.get("collected"), list):
                    return _category("unknown", f"{arm}_collection_unavailable", **evidence)
                if not collected["collected"]:
                    return _category("unknown", f"{arm}_empty_collection", **evidence)
                if any(
                    not isinstance(item, str)
                    or not any(
                        item == node or item.startswith(node + "::") or item.startswith(node + "[") for node in node_ids
                    )
                    for item in collected["collected"]
                ):
                    return _category("unknown", f"{arm}_collection_outside_selection", **evidence)
                if collect_run.harness_failure is not None or collect_run.returncode != 0:
                    return _category("unknown", f"{arm}_collection_failure", **evidence)
                # Collection can execute conftest code. Restore the exact revision
                # before running the same held IDs, then compare actual item sets.
                _restore(worktree)
                run, executed = _revision_pytest(
                    worktree,
                    expected,
                    [*node_ids, f"--junitxml={report}"],
                    collect=False,
                    seed=seed,
                    timeout=timeout,
                    artifacts=artifacts,
                    label=f"{name}.{arm}",
                )
                if not executed or not executed.get("collected") or executed["collected"] != collected["collected"]:
                    timing.record_run(f"{name}.{arm}", run, report=report)
                    return _category("unknown", f"{arm}_collection_changed", **evidence)
            else:
                command = f"{suite_cmd} {shlex.quote(f'--junitxml={report}')}"
                run = _run_command(command, cwd=worktree, seed=seed, timeout=timeout)
            timing.record_run(f"{name}.{arm}", run, report=report)
            exit_codes[arm] = run.returncode
            evidence["exit_codes"] = dict(sorted(exit_codes.items()))
            if run.harness_failure is not None:
                return _category("unknown", f"{arm}_harness_failure", failure_type=run.harness_failure, **evidence)
            parsed = _junit_outcomes(report)
            if parsed is None:
                timing.observations[f"{name}.{arm}"]["failure_type"] = "junit_unreadable"
                return _category("unknown", f"{arm}_junit_unreadable", **evidence)
            if (selected or required_coverage) and not parsed:
                return _category("unknown", f"{arm}_empty_collection", **evidence)
            outcomes[arm] = parsed
    base_a, tip_run, base_b = outcomes["base_a"], outcomes["tip"], outcomes["base_b"]
    flaky = sorted(test_id for test_id in base_a.keys() | base_b.keys() if base_a.get(test_id) != base_b.get(test_id))
    broken_on_tip = {test_id for test_id, outcome in tip_run.items() if outcome in _BROKEN_OUTCOMES}
    new_failures = sorted(
        test_id
        for test_id in broken_on_tip
        if base_a.get(test_id) == "passed" and test_id not in flaky and (not selected or tip_run[test_id] == "failed")
    )
    evidence.update(
        base_failures=sorted(
            test_id
            for test_id in broken_on_tip
            if base_a.get(test_id) in _BROKEN_OUTCOMES and base_b.get(test_id) in _BROKEN_OUTCOMES
        ),
        flaky=flaky,
        new_failures=new_failures,
        test_counts={arm: len(results) for arm, results in sorted(outcomes.items())},
        tip_only_failures=sorted(
            test_id for test_id in broken_on_tip if test_id not in base_a and test_id not in base_b
        ),
    )
    if new_failures:
        return _category("fail", "new_failures", **evidence)
    if selected or required_coverage:
        # Collection/setup faults and stable skipped or missing cases do not
        # establish tested coverage. A/A flakes retain their separate label.
        missing = sorted(base_a.keys() - tip_run.keys())
        untested = sorted(
            test_id for test_id, outcome in tip_run.items() if outcome in {"skipped", "error"} and test_id not in flaky
        )
        evidence.update(missing_on_tip=missing, untested_on_tip=untested)
        if missing or untested or not any(value == "passed" for value in base_a.values()):
            return _category("unknown", "suite_coverage_incomplete", **evidence)
    return _category("pass", "no_new_failures", **evidence)


def _acceptance(
    repository: Path, tip: str, acceptance_cmd: str | None, *, seed: int, timeout: float, timing: _Timing
) -> dict[str, Any]:
    if acceptance_cmd is None:
        return _category("not_applicable", "no_acceptance_cmd")
    with (
        tempfile.TemporaryDirectory(prefix="workbay-battery-acceptance-") as reports,
        _scratch_worktree(repository, tip, "acceptance") as worktree,
    ):
        report = Path(reports) / "acceptance.xml"
        pytest_shaped = _isolated_base_command(acceptance_cmd, cwd=worktree) is not None
        command = f"{acceptance_cmd} {shlex.quote(f'--junitxml={report}')}" if pytest_shaped else acceptance_cmd
        run = _run_command(command, cwd=worktree, seed=seed, timeout=timeout)
        timing.record_run("acceptance", run, report=report if pytest_shaped else None)
        parsed = _junit_outcomes(report) if pytest_shaped else None
    evidence = {"command": acceptance_cmd, "exit_code": run.returncode}
    if run.returncode is None:
        return _category("unknown", "timeout", **evidence)
    assertion_failed = parsed is not None and "failed" in parsed.values()
    failure_type = run.harness_failure
    # A real pytest assertion failure remains a product failure even alongside
    # a separate import/setup error. Text-only hints cannot override that RED.
    if assertion_failed and run.returncode == 1 and failure_type in {"import_error", "collection_error"}:
        failure_type = None
        timing.observations["acceptance"]["failure_type"] = None
    if failure_type is not None:
        return _category("unknown", "harness_failure", failure_type=failure_type, **evidence)
    if pytest_shaped:
        if parsed is None:
            timing.observations["acceptance"]["failure_type"] = "junit_unreadable"
            return _category("unknown", "harness_failure", failure_type="junit_unreadable", **evidence)
        if "error" in parsed.values() and not assertion_failed:
            timing.observations["acceptance"]["failure_type"] = "pytest_test_error"
            return _category("unknown", "harness_failure", failure_type="pytest_test_error", **evidence)
    if run.returncode != 0:
        return _category("fail", "acceptance_failed", failure_type=failure_type, **evidence)
    return _category("pass", "acceptance_passed", **evidence)


def _contract_acceptance(
    repository: Path,
    base: str,
    tip: str,
    manifest: Path | str,
    cases_root: Path | str,
    expected_digest: str,
    out: Path,
    timing: _Timing,
) -> dict[str, Any]:
    """Execute held criteria, retaining the observer's actual receipt and logs."""
    receipt = acceptance_contract.run_contract(repository, manifest, cases_root, expected_digest, base, tip, out)
    path = out / "receipt.json"
    timing.external_contract = {
        "manifest_path": str(Path(manifest).resolve()),
        "cases_root": str(Path(cases_root).resolve()),
        "expected_digest": expected_digest,
        "receipt_path": str(path.resolve()),
        "receipt_sha256": _file_sha256(path),
        "receipt": receipt,
        "digest_scope": "byte_identity_not_authority",
    }
    verdict = receipt["verdict"]
    status = {
        "fix_observed": "pass",
        "fix_not_observed": "fail",
        "characterization_observed": "pass",
        "characterization_failed": "fail",
    }.get(verdict, "unknown")
    return _category(
        status,
        verdict,
        manifest_sha256=receipt["manifest_sha256"],
        claim=receipt["claim"],
        base=receipt["base"],
        tip=receipt["tip"],
        base_tree=receipt["base_tree"],
        tip_tree=receipt["tip_tree"],
        outcomes=[
            {"id": case["id"], "base": case["base"]["outcome"], "tip": case["tip"]["outcome"]}
            for case in receipt["cases"]
        ],
    )


def _guarded(name: str, timing: _Timing, build: Any) -> dict[str, Any]:
    """Turn a harness fault inside one category into ``unknown`` [CAL-02]."""
    try:
        return build()
    except _SourceIdentityMismatch as exc:
        timing.output[f"{name}.harness_error"] = str(exc)[-_OUTPUT_TAIL_CHARS:]
        return _category("unknown", "source_identity_mismatch", error_type=type(exc).__name__)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        detail = getattr(exc, "stderr", None) or str(exc)
        timing.output[f"{name}.harness_error"] = str(detail)[-_OUTPUT_TAIL_CHARS:]
        return _category("unknown", "harness_error", error_type=type(exc).__name__)


def readiness_of(categories: dict[str, dict[str, Any]], *, required: Any = ()) -> str:
    """Weakest applicable category: fail < unknown < pass [EVAL-23]."""
    statuses = [
        category.get("status", "unknown")
        for category in categories.values()
        if category.get("status") != "not_applicable"
    ]
    statuses = [status if status in _STATUS_RANK else "unknown" for status in statuses]
    statuses.extend("unknown" for name in required if categories.get(name, {}).get("status") not in _STATUS_RANK)
    if not statuses:
        return "unknown"
    return min(statuses, key=_STATUS_RANK.__getitem__)


def _preflight_acceptance(
    repository: Path, base: str, tip: str, manifest_path: Any, cases_root: Any, expected_digest: str, out: Path
) -> None:
    """Reuse freeze validation without dispatching its case scripts.

    run_contract revalidates immediately before use. This early check is needed
    because full40377 dispatches MODEL before those scripts.
    """
    root = acceptance_contract._external_root(cases_root, repository)
    if not isinstance(expected_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise ValueError("expected digest must be a caller-supplied sha256")
    raw = Path(manifest_path).read_bytes()
    if _sha256(raw) != expected_digest:
        raise ValueError("manifest digest mismatch")
    manifest = acceptance_contract._decode_json(raw)
    if acceptance_contract._canonical(manifest) != raw:
        raise ValueError("manifest bytes are not canonical")
    acceptance_contract._validate_spec(manifest, frozen=True)
    for arm, revision in (("base", base), ("tip", tip)):
        if manifest[arm] != revision or manifest[f"{arm}_tree"] != _revision_identity(repository, revision)["tree"]:
            raise ValueError("caller source IDs differ from pinned manifest")
    for case in manifest["cases"]:
        if _sha256(acceptance_contract._case_bytes(root, case["path"])) != case["sha256"]:
            raise ValueError(f"case bytes mismatch: {case['id']}")
    if out.exists():
        raise ValueError("output must be a new directory")


def _node_path(node: Any) -> str:
    if not isinstance(node, str) or len(node) > 2048:
        raise ValueError("regression node IDs must be bounded strings")
    parts = node.split("::")
    path = PurePosixPath(parts[0])
    if (
        not re.fullmatch(r"[A-Za-z0-9_./-]+\.py", parts[0])
        or path.is_absolute()
        or any(p in {"", ".", ".."} for p in parts[0].split("/"))
        or parts[0].startswith("-")
    ):
        raise ValueError("regression node path must be a contained relative Python file")
    if any(not re.fullmatch(r"[A-Za-z_]\w*(?:\[[^\x00-\x1f]*\])?", part) for part in parts[1:]):
        raise ValueError("invalid pytest node ID")
    return parts[0]


def _preflight_selection(
    repository: Path, base: str, tip: str, selection_path: Any, expected_digest: str, catalog_path: Any
) -> dict[str, Any]:
    """Held selection is membership, never evidence of test success."""
    if not isinstance(expected_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise ValueError("regression expected digest must be a caller-supplied sha256")
    raw = Path(selection_path).read_bytes()
    if _sha256(raw) != expected_digest:
        raise ValueError("regression selection digest mismatch")
    selection = acceptance_contract._decode_json(raw)
    # Reject non-JSON numeric extensions anywhere, including advisory metadata.
    try:
        json.dumps(selection, allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ValueError("regression selection must be finite JSON") from exc
    required = {
        "schema",
        "status",
        "source",
        "catalog_sha256",
        "semantic_packet_sha256",
        "query_model_id",
        "node_ids",
        "coverage",
        "limitations",
    }
    if not required <= selection.keys() or selection["schema"] != "workbay/regression-selection@1":
        raise ValueError("invalid regression-selection@1 schema")
    if not isinstance(selection["status"], str) or selection["status"] not in {"selected", "no_matches", "unavailable"}:
        raise ValueError("invalid regression selection status")
    source = {"base": _revision_identity(repository, base), "tip": _revision_identity(repository, tip)}
    if selection["source"] != source:
        raise ValueError("regression selection source mismatch")
    for key in ("catalog_sha256", "semantic_packet_sha256"):
        if not isinstance(selection[key], str) or not re.fullmatch(r"[0-9a-f]{64}", selection[key]):
            raise ValueError(f"invalid selection {key}")
    if (
        not isinstance(selection["query_model_id"], str)
        or not selection["query_model_id"].strip()
        or not isinstance(selection["coverage"], dict)
        or not isinstance(selection["limitations"], list)
        or any(not isinstance(s, str) for s in selection["limitations"])
    ):
        raise ValueError("invalid selection coverage/model/limitations")
    nodes = selection["node_ids"]
    if not isinstance(nodes, list) or len(nodes) > _MAX_SELECTED:
        raise ValueError(f"selection must contain at most {_MAX_SELECTED} node IDs")
    for node in nodes:
        _node_path(node)
    if len(set(nodes)) != len(nodes) or (selection["status"] != "selected" and nodes):
        raise ValueError("selection IDs must be deduplicated and consistent with status")
    binding = dict(selection=selection, selection_sha256=expected_digest, catalog_verified=False)
    # Validate selection pins even if catalog capability is missing.
    if catalog_path is None:
        return binding
    catalog_raw = Path(catalog_path).read_bytes()
    if _sha256(catalog_raw) != selection["catalog_sha256"]:
        raise ValueError("regression catalog digest mismatch")
    catalog = acceptance_contract._decode_json(catalog_raw)
    if catalog.get("schema") != "workbay/regression-catalog@1" or not isinstance(catalog.get("node_ids"), list):
        raise ValueError("invalid regression catalog schema")
    allowlist = catalog["node_ids"]
    for node in allowlist:
        _node_path(node)
    if not set(nodes) <= set(allowlist):
        raise ValueError("regression selection contains IDs outside held catalog")
    for revision in (base, tip):
        for path in sorted({_node_path(node) for node in nodes}):
            # ls-tree traverses only actual tree directories, never filesystem
            # symlinks. A missing file is capability UNKNOWN during collection.
            entry = _git(repository, "ls-tree", revision, "--", path).stdout
            if entry and not entry.startswith(("100644 blob ", "100755 blob ")):
                raise ValueError("regression node path is not a contained regular file")
            for parent in PurePosixPath(path).parents:
                if parent != PurePosixPath(".") and _git(
                    repository, "ls-tree", revision, "--", str(parent)
                ).stdout.startswith("120000 blob "):
                    raise ValueError("regression node path traverses a symlink")
    binding["catalog_verified"] = True
    return binding


def _terminate_group(process: subprocess.Popen[Any]) -> list[str]:
    """Bound TERM, KILL and reap; never replace the primary child outcome."""
    errors = []
    for sig, grace in ((signal.SIGTERM, 0.25), (signal.SIGKILL, 1.0)):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        except OSError as exc:
            errors.append(f"{sig.name}: {exc}")
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            if sig == signal.SIGKILL:
                errors.append("bounded wait exhausted")
        except OSError as exc:
            errors.append(f"wait: {exc}")
    return errors


def _supervised_revision(
    worktree: Path,
    expected: dict[str, str],
    *,
    mode: str,
    request: dict[str, Any],
    seed: int,
    timeout: float,
    artifacts: Path,
    label: str,
) -> tuple[_Run, dict[str, Any] | None]:
    before = _observed_source(worktree)
    if any(before.get(key) != value for key, value in expected.items()) or before["diff_sha256"] != _sha256(b""):
        raise _SourceIdentityMismatch("revision observer did not start at the exact clean source")
    argv = [
        sys.executable,
        "-S",
        "-P",
        "-c",
        _REVISION_OBSERVER,
        str(worktree),
        mode,
        json.dumps(request, allow_nan=False),
    ]
    paths = {kind: artifacts / f"{label}.{kind}" for kind in ("stdout", "stderr", "observer.json")}
    observation = dict(
        source=before,
        seed=seed,
        timeout_seconds=timeout,
        runtime=_binary_identity(Path(sys.executable)),
        observer_sha256=_sha256(_REVISION_OBSERVER.encode()),
    )
    started = time.monotonic()
    timed_out = False
    with (
        paths["stdout"].open("wb") as stdout,
        paths["stderr"].open("wb") as stderr,
        paths["observer.json"].open("wb") as channel,
    ):
        argv.append(str(channel.fileno()))
        observation["command"] = list(argv)
        env = {key: value for key, value in os.environ.items() if not key.startswith("PYTHON")}
        process = subprocess.Popen(
            argv,
            cwd=worktree,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            pass_fds=(channel.fileno(),),
            env={
                **env,
                "PYTHONHASHSEED": str(seed),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                "PYTEST_ADDOPTS": "",
            },
        )
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        finally:
            cleanup = _terminate_group(process)
    observation.update(
        timed_out=timed_out,
        cleanup_errors=cleanup,
        source_after=_observed_source(worktree),
        logs={
            kind: dict(path=str(path), sha256=_file_sha256(path), bytes=path.stat().st_size)
            for kind, path in paths.items()
        },
    )
    after = observation["source_after"]
    if before != after:
        observation["identity_mismatch"] = "source_identity_mismatch"
    payload = None
    try:
        if paths["observer.json"].stat().st_size <= 8_000_000:
            payload = acceptance_contract._decode_json(paths["observer.json"].read_bytes())
    except (ValueError, OSError):
        pass
    output = ""
    for kind in ("stdout", "stderr"):
        with paths[kind].open("rb") as stream:
            stream.seek(max(0, paths[kind].stat().st_size - _OUTPUT_TAIL_CHARS))
            output += stream.read().decode("utf-8", "replace")
    observation["observer_result"] = payload
    return _Run(None if timed_out else process.returncode, output, time.monotonic() - started, observation), payload


def _model_properties(
    repository: Path,
    tip: str,
    bounds: dict[str, Any],
    *,
    seed: int,
    artifacts: Path,
    timing: _Timing,
    qualified_model_sha256: str | None,
) -> dict[str, Any]:
    result = None
    try:
        with _scratch_worktree(repository, tip, "model") as worktree:
            result = _model_at_revision(
                repository,
                tip,
                worktree,
                bounds,
                seed=seed,
                artifacts=artifacts,
                timing=timing,
                qualified_model_sha256=qualified_model_sha256,
            )
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        if result is None or result["status"] != "fail":
            raise
        result["evidence"]["cleanup_error_type"] = type(exc).__name__
        timing.output["model_properties.cleanup_error"] = str(exc)[-_OUTPUT_TAIL_CHARS:]
    assert result is not None
    return result


def _model_at_revision(
    repository: Path,
    tip: str,
    worktree: Path,
    bounds: dict[str, Any],
    *,
    seed: int,
    artifacts: Path,
    timing: _Timing,
    qualified_model_sha256: str | None,
) -> dict[str, Any]:
    files = {name: worktree / _MODEL_DIRECTORY / name for name in _MODEL_FILES}
    if any(not path.is_file() for path in files.values()):
        return _category("unknown", "model_capability_missing", expected_files=list(_MODEL_FILES), bounds=bounds)
    if any(path.is_symlink() or not path.resolve().is_relative_to(worktree) for path in files.values()):
        raise _SourceIdentityMismatch("MODEL source escapes exact tip checkout")
    digests = {name: _file_sha256(path) for name, path in files.items()}
    if qualified_model_sha256 is None or digests["validation_state_model.py"] != qualified_model_sha256:
        return _category("unknown", "qualified_model_source_unavailable", source_sha256=digests, bounds=bounds)
    run, payload = _supervised_revision(
        worktree,
        _revision_identity(repository, tip),
        mode="model",
        request=dict(directory=_MODEL_DIRECTORY, bounds=bounds),
        seed=seed,
        timeout=bounds["deadline_seconds"],
        artifacts=artifacts,
        label="model_properties",
    )
    timing.record_run("model_properties", run)
    evidence = dict(bounds=bounds, source_sha256=digests)
    if payload is None or not isinstance(payload.get("report"), dict):
        return _category("unknown", run.harness_failure or "model_observer_unavailable", **evidence)
    report = payload["report"]
    # Keep the actual receipt in the digested execution observation. As for
    # existing categories, stable evidence excludes duration/scratch paths.
    stable_report = json.loads(json.dumps(report))
    stable_report.pop("timing", None)
    stable_report.get("identity", {}).pop("chart_path", None)
    evidence["report"] = stable_report
    identity = report.get("identity", {})
    expected_hashes = dict(
        model_sha256=digests["validation_state_model.py"],
        checker_sha256=digests["statechart_conformance.py"],
        chart_sha256=digests["lifecycle_statechart.json"],
    )
    if (
        run.observation.get("identity_mismatch")
        or any(_file_sha256(files[n]) != d for n, d in digests.items())
        or any(identity.get(k) != v for k, v in expected_hashes.items())
        or identity.get("checker_mode") != "actual"
        or identity.get("chart_path") != str(files["lifecycle_statechart.json"])
        or report.get("schema") != "validation-state-model@1"
        or report.get("bounds") != bounds
    ):
        return _category("unknown", "model_identity_mismatch", **evidence)
    # A discovered, bound violation remains primary even if the child later
    # exhausts its budget or encounters reporting/cleanup faults.
    if report.get("status") == "fail":
        return _category("fail", "model_violation", **evidence)
    if run.harness_failure is not None or run.returncode != 0 or run.observation["cleanup_errors"]:
        return _category("unknown", run.harness_failure or "model_process_unavailable", **evidence)
    if (
        report.get("status") == "pass"
        and report.get("coverage", {}).get("complete") is True
        and not report.get("counterexamples")
        and all(
            report.get("properties", {}).get(name, {}).get("status") == "pass"
            for name in ("order", "rename", "malformed", "illegal_edge")
        )
    ):
        return _category("pass", "bounded_actual_model_passed", **evidence)
    return _category("unknown", "model_incomplete", **evidence)


def _revision_pytest(
    worktree: Path,
    expected: dict[str, str],
    args: list[str],
    *,
    collect: bool,
    seed: int,
    timeout: float,
    artifacts: Path,
    label: str,
) -> tuple[_Run, dict[str, Any] | None]:
    roots = _site_roots_without_candidate_hooks(Path(sys.executable), worktree)
    return _supervised_revision(
        worktree,
        expected,
        mode="pytest",
        seed=seed,
        timeout=timeout,
        artifacts=artifacts,
        label=label,
        request=dict(
            args=["-q", "-p", "no:cacheprovider", "-o", "addopts=", *args, *(["--collect-only"] if collect else [])],
            dependency_roots=[str(path) for path in roots],
        ),
    )


def _canonical(document: dict[str, Any]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def validation_digest(document: dict[str, Any]) -> str:
    """Stable result digest; raw process/report observations have their own digest.

    JUnit reports and process output contain durations and scratch paths. Keep
    them out of this existing deterministic digest, as with ``timing``.
    """
    stable = {
        key: value
        for key, value in document.items()
        if key not in {"timing", "validation_digest", "execution_evidence"}
    }
    return hashlib.sha256(_canonical(stable)).hexdigest()


def _execution_identity(repository: Path, base: str, tip: str) -> dict[str, Any]:
    source = Path(__file__)
    return {
        "base": _revision_identity(repository, base),
        "tip": _revision_identity(repository, tip),
        "harness": {
            "version": BATTERY_VERSION,
            "sources": {
                **{
                    name: _file_sha256(source.with_name(name))
                    for name in ("validation_battery.py", "red_green_gate.py")
                },
                "acceptance_contract.py": _file_sha256(Path(acceptance_contract.__file__)),
            },
        },
        "runtime": {
            "python": {
                **_binary_identity(Path(sys.executable)),
                "version": sys.version,
                "implementation": platform.python_implementation(),
            },
            "shell": _binary_identity(Path("/bin/sh")),
        },
    }


def _execution_evidence(
    identity: dict[str, Any], document: dict[str, Any], timing: _Timing, *, role: str, acceptance_mode: str
) -> dict[str, Any]:
    inputs = document["inputs"]
    qualified = (
        role == "positive_suite"
        and document.get("profile") == "full40377"
        and acceptance_mode == "external_frozen_contract"
        and document["readiness"] == "pass"
        and all(document["categories"].get(name, {}).get("status") == "pass" for name in FULL40377_REQUIRED)
        and document["categories"]["acceptance"]["reason"] == "fix_observed"
    )
    observations = {}
    for label, observation in sorted(timing.observations.items()):
        mutant = label.startswith("hunk_revert.") and label != "hunk_revert.baseline"
        observations[label] = {**observation, "role": "negative_mutant" if mutant else role}
    evidence = {
        "schema": "workbay/validation-execution-evidence@1",
        "role": role,
        "qualification": {
            "qualified_pass": qualified,
            "basis": "full40377_positive_suite_and_frozen_fix_observed_and_all_required_pass",
            "acceptance_mode": acceptance_mode,
            "profile": document.get("profile", "legacy"),
            "scope": "bounded_battery_only; no lifecycle/review/release qualification",
        },
        "identity": identity,
        "contract": {
            "commands": {
                "test": inputs["test_cmd"].strip(),
                "suite": inputs["suite_cmd"].strip() if inputs["suite_cmd"] is not None else None,
                "acceptance": inputs["acceptance_cmd"].strip() if inputs["acceptance_cmd"] is not None else None,
            },
            "seed": inputs["seed"],
            "max_hunks": inputs["max_hunks"],
            "timeout_seconds": inputs["timeout_seconds"],
        },
        "observations": observations,
        "categories_sha256": _sha256(_canonical(document["categories"])),
        "validation_digest": document["validation_digest"],
    }
    if timing.external_contract is not None:
        evidence["external_contract"] = timing.external_contract
    if document.get("profile") == "full40377":
        evidence["contract"]["required_categories"] = document["required_categories"]
        evidence["contract"]["coverage"] = document["coverage"]
        evidence["contract"]["model_bounds"] = inputs["model_bounds"]
        evidence["contract"]["regression"] = inputs["regression"]
        evidence["dispatch_order"] = list(timing.observations)
    evidence["evidence_digest"] = _sha256(_canonical(evidence))
    return evidence


def run_battery(
    repo: Path | str,
    base_sha: str,
    tip_sha: str,
    test_cmd: str,
    *,
    suite_cmd: str | None = None,
    acceptance_cmd: str | None = None,
    acceptance_manifest: Path | str | None = None,
    acceptance_cases_root: Path | str | None = None,
    acceptance_expected_digest: str | None = None,
    evidence_role: str | None = None,
    out_path: Path | str,
    seed: int | None = None,
    max_hunks: int = MAX_HUNKS,
    profile: str = "legacy",
    model_max_depth: int | None = None,
    model_max_states: int | None = None,
    model_deadline_seconds: float | None = None,
    regression_selection: Path | str | None = None,
    regression_expected_digest: str | None = None,
    regression_catalog: Path | str | None = None,
) -> dict[str, Any]:
    """Run exact-source controls; see module docstring for binding and qualification.

    Invalid argument groups or held pins raise ValueError. Legacy callers keep
    readiness semantics, with explicit unqualified execution evidence.
    """
    if profile not in _PROFILES:
        raise ValueError(f"profile must be one of {_PROFILES}")
    full = profile == "full40377"
    if full:
        for name, value in (("model_max_depth", model_max_depth), ("model_max_states", model_max_states)):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a supplied positive finite integer for full40377")
        if (
            type(model_deadline_seconds) not in (int, float)
            or not math.isfinite(model_deadline_seconds)
            or model_deadline_seconds <= 0
        ):
            raise ValueError("model_deadline_seconds must be supplied, positive and finite for full40377")
        if seed is None:
            raise ValueError("seed must be supplied for full40377")
    elif any(
        value is not None
        for value in (
            model_max_depth,
            model_max_states,
            model_deadline_seconds,
            regression_selection,
            regression_expected_digest,
            regression_catalog,
        )
    ):
        raise ValueError("MODEL/regression controls require profile full40377")
    if seed is None:
        seed = 0
    selection_bound = regression_selection is not None or regression_expected_digest is not None
    if selection_bound and (regression_selection is None or regression_expected_digest is None):
        raise ValueError("regression_selection and regression_expected_digest must be supplied together")
    if regression_catalog is not None and not selection_bound:
        raise ValueError("regression_catalog requires a held selection")
    bindings = (acceptance_manifest, acceptance_cases_root, acceptance_expected_digest)
    bound = any(value is not None for value in bindings)
    if bound and not all(value is not None for value in bindings):
        raise ValueError(
            "acceptance_manifest, acceptance_cases_root and acceptance_expected_digest must be supplied together"
        )
    if bound and acceptance_cmd is not None:
        raise ValueError("frozen contract binding and legacy acceptance_cmd are mutually exclusive")
    if full and acceptance_cmd is not None:
        raise ValueError("full40377 requires frozen acceptance, not acceptance_cmd")
    if evidence_role is None:
        evidence_role = "positive_suite" if bound else "auxiliary"
    if evidence_role not in _EVIDENCE_ROLES:
        raise ValueError(f"evidence_role must be one of {_EVIDENCE_ROLES}")
    if not isinstance(test_cmd, str) or not test_cmd.strip():
        raise ValueError("test_cmd must be a non-empty command")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= _MAX_SEED:
        raise ValueError(f"seed must be an integer in [0, {_MAX_SEED}]")
    if isinstance(max_hunks, bool) or not isinstance(max_hunks, int) or not 1 <= max_hunks <= MAX_HUNKS:
        raise ValueError(f"max_hunks must be an integer in [1, {MAX_HUNKS}]")
    try:
        toplevel = _git(Path(repo), "rev-parse", "--show-toplevel").stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ValueError(f"not a git worktree: {repo}") from exc
    repository = Path(toplevel).resolve()
    base = _resolve_commit(repository, base_sha)
    tip = _resolve_commit(repository, tip_sha)
    timeout = _lane_check_timeout_seconds()
    timing = _Timing()
    # Freeze the driver/runtime identity before candidate-controlled children run.
    identity = _execution_identity(repository, base, tip)

    categories: dict[str, dict[str, Any]] = {}
    selection = None
    artifacts = None
    model_bounds = dict(
        max_depth=model_max_depth, max_states=model_max_states, deadline_seconds=model_deadline_seconds, seed=seed
    )
    acceptance_out = Path(out_path).resolve().with_name(Path(out_path).name + ".acceptance")
    if full:
        # All static flags and held pins are checked before even MODEL imports
        # revision code. Missing I/O capability is UNKNOWN; a bad pin is usage64.
        if bound:
            try:
                _preflight_acceptance(
                    repository,
                    base,
                    tip,
                    acceptance_manifest,
                    acceptance_cases_root,
                    acceptance_expected_digest,
                    acceptance_out,
                )
            except OSError as exc:
                categories["acceptance"] = _category(
                    "unknown", "acceptance_preflight_unavailable", error_type=type(exc).__name__
                )
        else:
            categories["acceptance"] = _category("unknown", "frozen_acceptance_required")
        if selection_bound:
            try:
                selection = _preflight_selection(
                    repository, base, tip, regression_selection, regression_expected_digest, regression_catalog
                )
            except OSError as exc:
                categories["regression_suite"] = _category(
                    "unknown", "selection_unavailable", error_type=type(exc).__name__
                )
        artifacts = Path(out_path).resolve().with_name(Path(out_path).name + ".runs")
        if artifacts.exists():
            raise ValueError("full-profile run artifacts must be a new directory")
        try:
            artifacts.mkdir(parents=True, exist_ok=False)
        except OSError as exc:
            for name in ("model_properties", "regression_suite"):
                categories[name] = _category("unknown", "observer_logs_unavailable", error_type=type(exc).__name__)
        try:
            qualified_model_sha256 = _file_sha256(Path(__file__).with_name("validation_state_model.py"))
        except OSError:
            qualified_model_sha256 = None
        identity["harness"]["sources"]["validation_state_model.py"] = qualified_model_sha256
        if "model_properties" not in categories:
            categories["model_properties"] = _guarded(
                "model_properties",
                timing,
                lambda: _model_properties(
                    repository,
                    tip,
                    model_bounds,
                    seed=seed,
                    artifacts=artifacts,
                    timing=timing,
                    qualified_model_sha256=qualified_model_sha256,
                ),
            )
    if bound and "acceptance" not in categories:
        # Validate held pins and execute BEFORE lane-controlled battery commands.
        # ValueError from run_contract is a usage error, never product RED.
        started = time.monotonic()
        categories["acceptance"] = _guarded(
            "acceptance",
            timing,
            lambda: _contract_acceptance(
                repository,
                base,
                tip,
                acceptance_manifest,
                acceptance_cases_root,
                acceptance_expected_digest,
                acceptance_out,
                timing,
            ),
        )
        timing.durations["category.acceptance"] = round(time.monotonic() - started, 6)
    builders = {
        "red_on_base": lambda: _red_on_base(repository, base, tip, test_cmd, seed=seed, timing=timing),
        "hunk_revert": lambda: _hunk_revert(
            repository, base, tip, test_cmd, seed=seed, timeout=timeout, timing=timing, max_hunks=max_hunks
        ),
        "matched_suite": lambda: _matched_suite(
            repository,
            base,
            tip,
            suite_cmd,
            seed=seed,
            timeout=timeout,
            timing=timing,
            required_coverage=full,
        ),
    }
    if not bound and not full:
        builders["acceptance"] = lambda: _acceptance(
            repository, tip, acceptance_cmd, seed=seed, timeout=timeout, timing=timing
        )
    for name, build in builders.items():
        started = time.monotonic()
        categories[name] = _guarded(name, timing, build)
        timing.durations[f"category.{name}"] = round(time.monotonic() - started, 6)

    if full:
        if "regression_suite" not in categories:
            if selection is None:
                categories["regression_suite"] = _category("unknown", "held_selection_required")
            else:
                selected = selection["selection"]
                if not selection["catalog_verified"]:
                    result = _category("unknown", "held_catalog_required")
                elif selected["status"] != "selected" or not selected["node_ids"]:
                    result = _category("unknown", f"selection_{selected['status']}")
                else:
                    # Recheck all held selection/catalog/source bindings before
                    # use; earlier candidate commands cannot silently repin them.
                    try:
                        rechecked = _preflight_selection(
                            repository, base, tip, regression_selection, regression_expected_digest, regression_catalog
                        )
                    except (ValueError, OSError):
                        result = _category("unknown", "selection_changed_after_preflight")
                    else:
                        result = _guarded(
                            "regression_suite",
                            timing,
                            lambda: _matched_suite(
                                repository,
                                base,
                                tip,
                                None,
                                seed=seed,
                                timeout=timeout,
                                timing=timing,
                                node_ids=rechecked["selection"]["node_ids"],
                                artifacts=artifacts,
                            ),
                        )
                result["evidence"]["selection_binding"] = selection
                categories["regression_suite"] = result
        for name in FULL40377_REQUIRED:
            category = categories.setdefault(name, _category("unknown", "required_category_missing"))
            if category["status"] == "not_applicable":
                category["status"] = "unknown"
                category["evidence"]["applicability"] = "required_but_not_applicable"

    limitations = list(_LIMITATIONS)
    limitations.append(
        "digest is byte identity, not authority; coordinator must own frozen criteria, expected digest, source and evidence role"
        if bound
        else "legacy_invocation: no independent frozen acceptance; readiness pass is not qualified PASS"
    )
    limitations.append(
        "legacy profile is partial and unqualified; use explicit full40377 for all six required categories"
        if not full
        else "full40377 is bounded battery coverage only; lifecycle consumer, strict review and 92-gate backtest remain outstanding"
    )
    hunk_evidence = categories["hunk_revert"]["evidence"]
    if hunk_evidence.get("truncated"):
        limitations.append(
            f"hunk_revert evaluated a seeded sample of {max_hunks} of {hunk_evidence['total_hunks']} hunks"
        )
    document: dict[str, Any] = {
        "schema": SCHEMA,
        "inputs": {
            "acceptance_cmd": acceptance_cmd,
            "base": base,
            "battery_version": BATTERY_VERSION,
            "max_hunks": max_hunks,
            "seed": seed,
            "suite_cmd": suite_cmd,
            "test_cmd": test_cmd,
            "timeout_seconds": timeout,
            "tip": tip,
        },
        "categories": categories,
        "readiness": readiness_of(categories, required=FULL40377_REQUIRED if full else ()),
        "limitations": sorted(limitations),
        "timing": timing.as_dict(),
    }
    document["profile"] = profile
    document["required_categories"] = list(FULL40377_REQUIRED) if full else []
    document["coverage"] = dict(
        partial=True,
        full_proposal_complete=False,
        scope="bounded_model_and_selected_regressions" if full else "legacy_categories_only",
        missing_required=[name for name in FULL40377_REQUIRED if categories.get(name, {}).get("status") != "pass"],
    )
    if full:
        document["inputs"].update(
            model_bounds=model_bounds,
            regression=dict(
                selection_path=str(Path(regression_selection).resolve()) if regression_selection is not None else None,
                expected_digest=regression_expected_digest,
                catalog_path=str(Path(regression_catalog).resolve()) if regression_catalog is not None else None,
            ),
        )
        for name in ("model_properties", "regression_suite"):
            categories[name]["evidence"]["binding"] = dict(
                source={"base": identity["base"], "tip": identity["tip"]},
                role=evidence_role,
                runtime=identity["runtime"]["python"],
                command="actual_revision.run_model"
                if name == "model_properties"
                else "revision_local_pytest_selected_ids",
                coverage="finite_edge_membership_only"
                if name == "model_properties"
                else "selected_catalog_members_only",
            )
    if bound:
        document["inputs"].update(
            acceptance_mode="external_frozen_contract",
            acceptance_manifest=str(Path(acceptance_manifest).resolve()),
            acceptance_cases_root=str(Path(acceptance_cases_root).resolve()),
            acceptance_expected_digest=acceptance_expected_digest,
            evidence_role=evidence_role,
        )
    document["validation_digest"] = validation_digest(document)
    document["execution_evidence"] = _execution_evidence(
        identity,
        document,
        timing,
        role=evidence_role,
        acceptance_mode="external_frozen_contract" if bound else "legacy_invocation",
    )

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    partial = out.with_name(f".{out.name}.{os.getpid()}.tmp")
    write_failed = False
    try:
        partial.write_text(json.dumps(document, sort_keys=True, indent=2, ensure_ascii=True) + "\n", encoding="ascii")
        os.replace(partial, out)
    except BaseException:
        write_failed = True
        raise
    finally:
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            # Cleanup must not mask the report write's original exception.
            if not write_failed:
                raise
    return document


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Any:
        # argparse's default 2 would collide with readiness "unknown".
        self.print_usage(sys.stderr)
        self.exit(EXIT_USAGE, f"{self.prog}: error: {message}\n")


def main(argv: list[str] | None = None) -> int:
    parser = _ArgumentParser(
        prog="python -m workbay_orchestrator_mcp.orchestration.validation_battery",
        description="Harness-run validation battery. Exit 0 pass, 1 fail, 2 unknown, 64 usage, 70 internal error.",
    )
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--base", required=True)
    parser.add_argument("--tip", required=True)
    parser.add_argument("--test-cmd", required=True)
    parser.add_argument("--suite-cmd")
    parser.add_argument("--acceptance-cmd")
    parser.add_argument("--acceptance-manifest", type=Path)
    parser.add_argument("--acceptance-cases-root", type=Path)
    parser.add_argument("--acceptance-expected-digest")
    parser.add_argument("--evidence-role", choices=_EVIDENCE_ROLES)
    parser.add_argument("--profile", choices=_PROFILES, default="legacy")
    parser.add_argument("--model-max-depth", type=int)
    parser.add_argument("--model-max-states", type=int)
    parser.add_argument("--model-deadline-seconds", type=float)
    parser.add_argument("--regression-selection", type=Path)
    parser.add_argument("--regression-expected-digest")
    parser.add_argument("--regression-catalog", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--max-hunks", type=int, default=MAX_HUNKS)
    args = parser.parse_args(argv)
    try:
        document = run_battery(
            args.repo,
            args.base,
            args.tip,
            args.test_cmd,
            suite_cmd=args.suite_cmd,
            acceptance_cmd=args.acceptance_cmd,
            acceptance_manifest=args.acceptance_manifest,
            acceptance_cases_root=args.acceptance_cases_root,
            acceptance_expected_digest=args.acceptance_expected_digest,
            evidence_role=args.evidence_role,
            out_path=args.out,
            seed=args.seed,
            max_hunks=args.max_hunks,
            profile=args.profile,
            model_max_depth=args.model_max_depth,
            model_max_states=args.model_max_states,
            model_deadline_seconds=args.model_deadline_seconds,
            regression_selection=args.regression_selection,
            regression_expected_digest=args.regression_expected_digest,
            regression_catalog=args.regression_catalog,
        )
    except ValueError as exc:
        print(f"validation-battery: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except Exception as exc:  # noqa: BLE001 - an uncaught traceback would exit 1, i.e. read as "fail"
        print(f"validation-battery: internal error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_SOFTWARE
    print(
        json.dumps(
            {
                "out": str(args.out),
                "readiness": document["readiness"],
                "validation_digest": document["validation_digest"],
            },
            sort_keys=True,
        )
    )
    return _READINESS_EXIT[document["readiness"]]


if __name__ == "__main__":
    raise SystemExit(main())
