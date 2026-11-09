"""Freeze and observe coordinator-owned Python acceptance scripts (stdlib only).

CLI::

    python -m workbay_orchestrator_mcp.orchestration.acceptance_contract freeze \
      --repo REPO --spec SPEC.json --cases-root EXTERNAL_DIR \
      --manifest MANIFEST.json --digest-out MANIFEST.sha256
    python -m workbay_orchestrator_mcp.orchestration.acceptance_contract run \
      --repo REPO --manifest MANIFEST.json --cases-root EXTERNAL_DIR \
      --expected-digest SHA256 --base FULL_SHA --tip FULL_SHA --out NEW_DIR

SPEC has schema ``workbay/acceptance-spec@1``, claim ``fix`` or
``characterization``, full base/tip commit IDs, and a nonempty ``cases`` list.
Each case has a unique ``id``, external relative ``path``, ``argv`` beginning
``["{python}", "{case}"]`` (remaining literal script arguments), positive
``timeout_seconds``, integer ``seed``, and nonempty ``source_modules``. Freeze
writes canonical manifest bytes exclusively plus a separate digest. The caller
must pin that digest outside author-controlled files and supply it to run.

The observer runs case scripts with ``python -S -P`` and revision-local source
roots. Only a caught Python AssertionError is RED; a completed script with
verified imports is GREEN. Other exceptions, early SystemExit, import/source
errors, and timeout are UNKNOWN. This script-only grammar deliberately excludes
shell commands, pytest plugins, and author-written JSON/JUnit receipts. Exit
codes are 0 observed, 1 disproven, 2 unknown, 64 invalid inputs, 70 harness I/O.
With ``-S``, site hooks and installed third-party dependencies are unavailable;
case scripts and declared source modules must run using stdlib and pinned source
roots, or their outcomes are UNKNOWN.

The digest is an integrity check, not a signature. A coordinator must own its
expected value and source refs. Processes with the same UID can interfere with
scratch worktrees, case files, process descriptors, or output, so this is not a
security boundary against a malicious same-UID producer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Any

SPEC_SCHEMA = "workbay/acceptance-spec@1"
MANIFEST_SCHEMA = "workbay/acceptance-manifest@1"
RECEIPT_SCHEMA = "workbay/acceptance-observation@1"
LOG_LIMIT = 65_536
_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")
_MODULE = re.compile(r"[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*\Z")
_OBSERVER = r'''
import importlib, json, os, runpy, sys, traceback
from pathlib import Path

root = Path(sys.argv[1]).resolve()
case = Path(sys.argv[2]).resolve()
modules = json.loads(sys.argv[3])
roots = [root, root / 'src']
package_dir = root / 'packages'
if package_dir.is_dir():
    roots += [item for p in sorted(package_dir.iterdir()) for item in (p / 'src', p) if item.is_dir()]
sys.path[:0] = [str(p) for p in roots]

def audit():
    origins = {}
    for name in modules:
        module = importlib.import_module(name)
        origin = getattr(module, '__file__', None)
        if origin is None or not Path(origin).resolve().is_relative_to(root):
            raise RuntimeError('source module outside selected worktree: ' + name + ': ' + str(origin))
        origins[name] = str(Path(origin).resolve())
    return origins

try:
    origins = audit()
except BaseException:
    traceback.print_exc()
    sys.exit(81)
done_fd = int(os.environ['WB_ACCEPT_DONE_FD'])
os.write(done_fd, ('SOURCES:' + json.dumps(origins, sort_keys=True) + '\n').encode('utf-8'))
try:
    sys.argv = [str(case), *sys.argv[4:]]
    runpy.run_path(str(case), run_name='__main__')
except AssertionError:
    traceback.print_exc()
    try:
        audit()
    except BaseException:
        traceback.print_exc()
        sys.exit(81)
    os.write(done_fd, b'RED')
    sys.exit(80)
except BaseException:
    traceback.print_exc()
    sys.exit(81)
try:
    origins = audit()
except BaseException:
    traceback.print_exc()
    sys.exit(81)
os.write(done_fd, b'GREEN')
'''


def _canonical(document: dict[str, Any]) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("ascii")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _decode_json(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw, object_pairs_hook=_no_duplicate_keys)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    return _decode_json(path.read_bytes())


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "--literal-pathspecs", "-C", str(repo), *args],
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise ValueError(f"git {args[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _repo(path: Path | str) -> Path:
    top = Path(_git(Path(path), "rev-parse", "--show-toplevel")).resolve()
    if not top.is_dir():
        raise ValueError("repository root is missing")
    return top


def _revision(repo: Path, value: Any, kind: str = "commit") -> str:
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise ValueError("source revision must be a full lowercase object ID")
    actual = _git(repo, "rev-parse", "--verify", "--end-of-options", f"{value}^{{{kind}}}")
    if actual != value and kind == "commit":
        raise ValueError("source commit did not resolve to exact supplied ID")
    return actual


def _external_root(path: Path | str, repo: Path) -> Path:
    raw = Path(path)
    if raw.is_symlink():
        raise ValueError("cases root may not be a symlink")
    root = raw.resolve(strict=True)
    if not root.is_dir() or root.is_relative_to(repo) or repo.is_relative_to(root):
        raise ValueError("cases root must be an external directory distinct from repository")
    return root


def _case_bytes(root: Path, name: Any) -> bytes:
    if not isinstance(name, str) or "\\" in name or "\x00" in name:
        raise ValueError("case path must be a safe relative POSIX path")
    relative = PurePosixPath(name)
    if not name.endswith(".py") or relative.is_absolute() or any(part in {"", ".", ".."} for part in name.split("/")):
        raise ValueError("case path escapes cases root")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("case path contains symlink")
    path = current.resolve(strict=True)
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError("case path escapes cases root or is not a file")
    return path.read_bytes()


def _validate_case(case: Any, *, frozen: bool) -> dict[str, Any]:
    if not isinstance(case, dict):
        raise ValueError("case must be an object")
    expected = {"id", "path", "argv", "timeout_seconds", "seed", "source_modules"}
    if frozen:
        expected.add("sha256")
    if set(case) != expected:
        raise ValueError("case fields differ from schema")
    if not isinstance(case["id"], str) or not _ID.fullmatch(case["id"]):
        raise ValueError("invalid case id")
    if not isinstance(case["argv"], list) or case["argv"][:2] != ["{python}", "{case}"]:
        raise ValueError("argv must begin with {python}, {case}")
    if any(not isinstance(arg, str) or "\x00" in arg or "{" in arg or "}" in arg for arg in case["argv"][2:]):
        raise ValueError("case arguments must be literal strings")
    timeout = case["timeout_seconds"]
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 < timeout <= 3600:
        raise ValueError("timeout_seconds must be finite and in (0, 3600]")
    seed = case["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= 4_294_967_295:
        raise ValueError("seed must be a uint32")
    modules = case["source_modules"]
    if not isinstance(modules, list) or not modules or len(modules) > 32 or len(set(map(str, modules))) != len(modules):
        raise ValueError("source_modules must be unique and nonempty")
    if any(not isinstance(name, str) or not _MODULE.fullmatch(name) for name in modules):
        raise ValueError("invalid source module name")
    if frozen and (not isinstance(case["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", case["sha256"])):
        raise ValueError("invalid case sha256")
    return case


def _validate_spec(spec: dict[str, Any], *, frozen: bool) -> None:
    fields = {"schema", "claim", "base", "tip", "cases"}
    if frozen:
        fields |= {"base_tree", "tip_tree"}
    if set(spec) != fields or spec["schema"] != (MANIFEST_SCHEMA if frozen else SPEC_SCHEMA):
        raise ValueError("acceptance document schema/fields mismatch")
    if not isinstance(spec["claim"], str) or spec["claim"] not in {"fix", "characterization"}:
        raise ValueError("claim must be fix or characterization")
    for key in ("base", "tip", "base_tree", "tip_tree") if frozen else ("base", "tip"):
        if not isinstance(spec[key], str) or not _SHA.fullmatch(spec[key]):
            raise ValueError(f"{key} must be a full lowercase object ID")
    cases = spec["cases"]
    if not isinstance(cases, list) or not cases:
        raise ValueError("cases must be nonempty")
    seen: set[str] = set()
    for case in cases:
        _validate_case(case, frozen=frozen)
        if case["id"] in seen:
            raise ValueError("duplicate case id")
        seen.add(case["id"])


def _exclusive_write(path: Path, data: bytes) -> None:
    created = False
    try:
        with path.open("xb") as stream:
            created = True
            stream.write(data)
        path.chmod(0o444)
    except OSError:
        if created:
            path.unlink(missing_ok=True)
        raise


def freeze_manifest(repo: Path | str, spec_path: Path | str, cases_root: Path | str,
                    manifest_path: Path | str, digest_path: Path | str) -> str:
    """Freeze canonical case hashes and exact source trees; return SHA256."""
    repository = _repo(repo)
    root = _external_root(cases_root, repository)
    spec = _read_json(Path(spec_path))
    _validate_spec(spec, frozen=False)
    base = _revision(repository, spec["base"])
    tip = _revision(repository, spec["tip"])
    manifest = {
        "schema": MANIFEST_SCHEMA, "claim": spec["claim"],
        "base": base, "tip": tip,
        "base_tree": _git(repository, "rev-parse", f"{base}^{{tree}}"),
        "tip_tree": _git(repository, "rev-parse", f"{tip}^{{tree}}"),
        "cases": [],
    }
    for case in spec["cases"]:
        data = _case_bytes(root, case["path"])
        manifest["cases"].append({**case, "sha256": _sha(data)})
    canonical = _canonical(manifest)
    digest = _sha(canonical)
    destination, digest_destination = Path(manifest_path), Path(digest_path)
    if destination == digest_destination or destination.exists() or digest_destination.exists():
        raise ValueError("freeze outputs must be distinct new files")
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest_destination.parent.mkdir(parents=True, exist_ok=True)
    destination_written = False
    digest_written = False
    try:
        _exclusive_write(destination, canonical)
        destination_written = True
        _exclusive_write(digest_destination, (digest + "\n").encode("ascii"))
        digest_written = True
    except OSError:
        if destination_written:
            destination.unlink(missing_ok=True)
        if digest_written:
            digest_destination.unlink(missing_ok=True)
        raise
    return digest


def _capture(command: list[str], cwd: Path, env: dict[str, str], timeout: float) -> dict[str, Any]:
    read_fd, write_fd = os.pipe()
    env = {**env, "WB_ACCEPT_DONE_FD": str(write_fd)}
    try:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, start_new_session=True, pass_fds=(write_fd,))
    except BaseException:
        os.close(read_fd)
        raise
    finally:
        os.close(write_fd)
    assert process.stdout is not None
    digest = hashlib.sha256()
    retained = bytearray()
    total = 0
    timed_out = False
    started = time.monotonic()
    try:
        drain_deadline = None
        while True:
            remaining = timeout - (time.monotonic() - started)
            if remaining <= 0 and not timed_out:
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                drain_deadline = time.monotonic() + 1
            if drain_deadline is not None and time.monotonic() >= drain_deadline:
                break
            ready, _, _ = select.select([process.stdout], [], [], min(max(remaining, 0.01), 0.1))
            if ready:
                chunk = os.read(process.stdout.fileno(), 8192)
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
                if len(retained) < LOG_LIMIT:
                    retained.extend(chunk[:LOG_LIMIT - len(retained)])
        process.wait(timeout=1)
        ready, _, _ = select.select([read_fd], [], [], 0)
        completion = os.read(read_fd, 65_536) if ready else b""
    finally:
        os.close(read_fd)
        process.stdout.close()
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if process.poll() is None:
            process.wait()
    code = process.returncode
    try:
        source_line, marker = completion.split(b"\n", 1)
        if not source_line.startswith(b"SOURCES:"):
            raise ValueError("missing source observation")
        source_origins = json.loads(source_line[len(b"SOURCES:"):])
    except (ValueError, json.JSONDecodeError):
        source_origins, marker = {}, b""
    outcome = "UNKNOWN" if timed_out or not source_origins or (code, marker) not in {(0, b"GREEN"), (80, b"RED")} else "RED" if code == 80 else "GREEN"
    return {
        "outcome": outcome, "exit_code": code, "timed_out": timed_out,
        "source_origins": source_origins,
        "seconds": round(time.monotonic() - started, 6),
        "log_sha256": digest.hexdigest(), "log_bytes": total,
        "log_retained_bytes": len(retained), "retained_log_sha256": _sha(bytes(retained)),
        "log_truncated": total > len(retained),
        "_log": bytes(retained),
    }


def _arm(repository: Path, revision: str, tree: str, case: dict[str, Any], data: bytes,
         scratch: Path, arm: str) -> dict[str, Any]:
    worktree = scratch / f"tree-{arm}"
    overlay = scratch / f"overlay-{arm}"
    case_path = overlay / "case.py"
    command = [sys.executable, "-S", "-P", "-c", _OBSERVER, str(worktree), str(case_path),
               json.dumps(case["source_modules"]), *case["argv"][2:]]
    result: dict[str, Any] | None = None
    checked_out_commit: str | None = None
    checked_out_tree: str | None = None
    try:
        _git(repository, "worktree", "add", "--detach", "--force", str(worktree), revision)
        checked_out_commit = _git(worktree, "rev-parse", "HEAD")
        checked_out_tree = _git(worktree, "rev-parse", "HEAD^{tree}")
        if checked_out_commit != revision or checked_out_tree != tree:
            raise ValueError("disposable worktree source differs from pinned commit/tree")
        overlay.mkdir()
        case_path.write_bytes(data)
        env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONHASHSEED": str(case["seed"]),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        result = _capture(command, worktree, env, case["timeout_seconds"])
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        result = {
            "outcome": "UNKNOWN", "reason": "harness_error", "error": str(exc)[:2000],
            "exit_code": None, "timed_out": False, "seconds": None,
            "source_origins": {}, "log_sha256": _sha(b""), "log_bytes": 0,
            "log_retained_bytes": 0, "retained_log_sha256": _sha(b""),
            "log_truncated": False, "_log": b"",
        }
    finally:
        if worktree.exists():
            try:
                _git(repository, "worktree", "remove", "--force", str(worktree))
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                if result is None:
                    result = {
                        "exit_code": None, "timed_out": False, "seconds": None,
                        "source_origins": {}, "log_sha256": _sha(b""), "log_bytes": 0,
                        "log_retained_bytes": 0, "retained_log_sha256": _sha(b""),
                        "log_truncated": False, "_log": b"",
                    }
                result.update(outcome="UNKNOWN", reason="cleanup_error", error=str(exc)[:2000])
    assert result is not None
    result.update(source_commit=revision, source_tree=tree,
                  checked_out_commit=checked_out_commit, checked_out_tree=checked_out_tree,
                  argv=case["argv"],
                  observer_argv=command, observer_sha256=_sha(_OBSERVER.encode("utf-8")))
    return result


def run_contract(repo: Path | str, manifest_path: Path | str, cases_root: Path | str,
                 expected_digest: str, base_sha: str, tip_sha: str, out_path: Path | str) -> dict[str, Any]:
    """Recheck pinned evidence and run cases in disposable exact-source worktrees."""
    repository = _repo(repo)
    root = _external_root(cases_root, repository)
    if not isinstance(expected_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise ValueError("expected digest must be a caller-supplied sha256")
    raw = Path(manifest_path).read_bytes()
    if _sha(raw) != expected_digest:
        raise ValueError("manifest digest mismatch")
    manifest = _decode_json(raw)
    if _canonical(manifest) != raw:
        raise ValueError("manifest bytes are not canonical")
    _validate_spec(manifest, frozen=True)
    if base_sha != manifest["base"] or tip_sha != manifest["tip"]:
        raise ValueError("caller source IDs differ from pinned manifest")
    base, tip = _revision(repository, base_sha), _revision(repository, tip_sha)
    for arm, revision in (("base", base), ("tip", tip)):
        if _git(repository, "rev-parse", f"{revision}^{{tree}}") != manifest[f"{arm}_tree"]:
            raise ValueError(f"{arm} tree mismatch")
    case_data = []
    for case in manifest["cases"]:
        data = _case_bytes(root, case["path"])
        if _sha(data) != case["sha256"]:
            raise ValueError(f"case bytes mismatch: {case['id']}")
        case_data.append(data)
    out = Path(out_path)
    if out.exists():
        raise ValueError("output must be a new directory")
    observations = []
    with tempfile.TemporaryDirectory(prefix="workbay-acceptance-") as directory:
        scratch = Path(directory)
        for case, data in zip(manifest["cases"], case_data, strict=True):
            # A changed external file between validation and overlay cannot be used.
            if _case_bytes(root, case["path"]) != data:
                raise ValueError(f"case bytes changed during run: {case['id']}")
            arms = {}
            for arm, revision in (("base", base), ("tip", tip)):
                arms[arm] = _arm(repository, revision, manifest[f"{arm}_tree"], case, data, scratch / case["id"], arm)
            observations.append({"id": case["id"], "case_sha256": case["sha256"], **arms})
    outcomes = [(entry["base"]["outcome"], entry["tip"]["outcome"]) for entry in observations]
    if any("UNKNOWN" in pair for pair in outcomes):
        verdict = "unknown"
    elif manifest["claim"] == "fix":
        verdict = "fix_observed" if all(pair == ("RED", "GREEN") for pair in outcomes) else "fix_not_observed"
    else:
        verdict = "characterization_observed" if all(tip == "GREEN" for _, tip in outcomes) else "characterization_failed"
    receipt = {
        "schema": RECEIPT_SCHEMA, "verdict": verdict, "claim": manifest["claim"],
        "manifest_sha256": expected_digest, "harness_revision": _sha(Path(__file__).read_bytes()),
        "base": base, "tip": tip, "base_tree": manifest["base_tree"], "tip_tree": manifest["tip_tree"],
        "cases": observations,
        "limitations": ["same-UID processes can interfere with cases, scratch worktrees and observer channels"],
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        out.mkdir()
        created = True
        for entry in observations:
            for arm in ("base", "tip"):
                result = entry[arm]
                log_name = f"{entry['id']}.{arm}.log"
                (out / log_name).write_bytes(result.pop("_log"))
                result["log_file"] = log_name
        (out / "receipt.json").write_bytes(_canonical(receipt))
    except OSError as primary:
        if created:
            try:
                shutil.rmtree(out)
            except OSError as cleanup:
                primary.add_note(f"output cleanup failed for {out}: {cleanup!r}")
        raise
    return receipt


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        print(f"acceptance-contract: {message}", file=sys.stderr)
        raise SystemExit(64)


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True, parser_class=_Parser)
    freeze = sub.add_parser("freeze")
    for key in ("repo", "spec", "cases-root", "manifest", "digest-out"):
        freeze.add_argument(f"--{key}", required=True)
    run = sub.add_parser("run")
    for key in ("repo", "manifest", "cases-root", "expected-digest", "base", "tip", "out"):
        run.add_argument(f"--{key}", required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "freeze":
            print(freeze_manifest(args.repo, args.spec, args.cases_root, args.manifest, args.digest_out))
            return 0
        receipt = run_contract(args.repo, args.manifest, args.cases_root, args.expected_digest,
                               args.base, args.tip, args.out)
        print(receipt["verdict"])
        return {"fix_observed": 0, "characterization_observed": 0, "unknown": 2}.get(receipt["verdict"], 1)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"acceptance-contract: {exc}", file=sys.stderr)
        return 64
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"acceptance-contract harness error: {exc}", file=sys.stderr)
        for note in getattr(exc, "__notes__", ()):
            print(note, file=sys.stderr)
        return 70


if __name__ == "__main__":
    raise SystemExit(main())
