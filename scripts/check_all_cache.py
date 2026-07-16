#!/usr/bin/env python3
"""Stamp-based skip wrapper for expensive ``make check-all`` suite lines.

Each invocation digests a named key's input footprint (tracked git state +
untracked files under ``--paths``). When the digest matches the last-green
stamp and ``CHECK_ALL_FRESH`` is unset, the wrapped command is skipped.

Stdlib-only. Fail-open: any git failure runs the command (never false-skip).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


STAMP_DIR_REL = Path(".workbay-cache") / "checkall-stamps"
FRESH_ENV = "CHECK_ALL_FRESH"


def _repo_root(cwd: Path | None = None) -> Path:
    """Resolve git toplevel from *cwd* (default: process cwd). Raises on failure."""
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git rev-parse failed")
    return Path(result.stdout.strip())


def _git(
    repo: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed ({result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result


def compute_digest(
    repo: Path, key: str, paths: list[str], fingerprint: str = ""
) -> str:
    """Sha256 over key + paths list + fingerprint + tracked index/diffs + untracked hashes.

    Order is stable and intentional: key, sorted ``--paths`` list (so the same
    key with a different footprint never reuses a stale stamp), the caller
    fingerprint (mode inputs like PYTEST_WORKERS that change suite behavior
    without touching files), ``ls-files -s``, unstaged diff, staged diff, then
    each untracked path (sorted) with its ``hash-object``.
    """
    h = hashlib.sha256()
    h.update(key.encode())
    h.update(b"\0")
    for p in sorted(paths):
        h.update(p.encode())
        h.update(b"\0")
    h.update(fingerprint.encode())
    h.update(b"\0")

    path_args = list(paths)

    tracked = _git(repo, "ls-files", "-s", "--", *path_args)
    h.update(tracked.stdout.encode())
    h.update(b"\0")

    unstaged = _git(repo, "diff", "--", *path_args)
    h.update(unstaged.stdout.encode())
    h.update(b"\0")

    staged = _git(repo, "diff", "--cached", "--", *path_args)
    h.update(staged.stdout.encode())
    h.update(b"\0")

    untracked = _git(repo, "ls-files", "-o", "--exclude-standard", "--", *path_args)
    untracked_paths = sorted(p for p in untracked.stdout.splitlines() if p)
    for rel in untracked_paths:
        abs_path = repo / rel
        # hash-object of the file content (same blob id git would assign)
        blob = _git(repo, "hash-object", "--", str(abs_path))
        h.update(rel.encode())
        h.update(b"\0")
        h.update(blob.stdout.strip().encode())
        h.update(b"\0")

    return h.hexdigest()


def stamp_path(repo: Path, key: str) -> Path:
    return repo / STAMP_DIR_REL / key


def read_stamp(path: Path) -> str | None:
    if not path.is_file():
        return None
    first = path.read_text(encoding="utf-8").splitlines()
    if not first:
        return None
    return first[0].strip() or None


def write_stamp(path: Path, digest: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(f"{digest}\n{ts}\n", encoding="utf-8")


def run_command(cmd: list[str], cwd: Path | None = None) -> int:
    # The fresh knob is consumed by this wrapper; child suites must not see it
    # (their own tests may assert skip behavior in scratch repos).
    env = {k: v for k, v in os.environ.items() if k != FRESH_ENV}
    proc = subprocess.run(cmd, cwd=cwd, env=env, check=False)
    return int(proc.returncode)


def cmd_run(
    key: str, paths: list[str], command: list[str], fingerprint: str = ""
) -> int:
    """Run or skip *command* based on footprint stamp for *key*."""
    if not command:
        print("check-all-cache: FAIL missing command after --", file=sys.stderr)
        return 2

    fresh = bool(os.environ.get(FRESH_ENV, "").strip())
    digest: str | None = None
    stamp: Path | None = None
    git_ok = True

    try:
        repo = _repo_root()
        digest = compute_digest(repo, key, paths, fingerprint)
        stamp = stamp_path(repo, key)
    except Exception as exc:  # noqa: BLE001 — fail-open on any git/path error
        git_ok = False
        print(
            f"check-all-cache: WARN git footprint unavailable ({exc}); running unconditionally",
            file=sys.stderr,
        )

    if not fresh and git_ok and digest is not None and stamp is not None:
        prev = read_stamp(stamp)
        if prev is not None and prev == digest:
            print(f"check-all-cache: SKIP {key} (unchanged since last green)")
            return 0

    print(f"check-all-cache: RUN {key}")
    code = run_command(command)

    if code == 0:
        if git_ok and digest is not None and stamp is not None:
            write_stamp(stamp, digest)
        print(f"check-all-cache: PASS {key}")
        return 0

    print(f"check-all-cache: FAIL {key} (exit {code})")
    return code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="check_all_cache.py",
        description="Stamp-skip wrapper for make check-all suite lines.",
    )
    sub = parser.add_subparsers(dest="action", required=True)

    run_p = sub.add_parser("run", help="Run command unless footprint stamp is fresh")
    run_p.add_argument(
        "--key", required=True, help="Stable suite key for the stamp file"
    )
    run_p.add_argument(
        "--paths",
        nargs="+",
        required=True,
        help="Paths whose git footprint feeds the digest",
    )
    run_p.add_argument(
        "--fingerprint",
        default="",
        help="Extra mode string folded into the digest (e.g. pytest-workers=4)",
    )
    run_p.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command after -- (e.g. -- make check-protocol)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.action == "run":
        command = list(args.command)
        # argparse.REMAINDER keeps a leading "--" when present
        if command and command[0] == "--":
            command = command[1:]
        return cmd_run(args.key, list(args.paths), command, args.fingerprint)

    parser.error(f"unknown action {args.action!r}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
