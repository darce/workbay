#!/usr/bin/env python3
"""Stamp-based skip wrapper for expensive ``make check-all`` suite lines.

Each invocation digests a named key's input footprint (tracked git state +
untracked files under ``--paths`` + the wrapped command). When the digest
matches the last-green stamp and ``CHECK_ALL_FRESH`` is unset, the wrapped
command is skipped.

Stdlib-only. Fail-open: any git/key/stamp error runs the command (never
false-skip, never turn a green suite red).

A stamp means the suite really ran and passed. Under ``make -n``/``-t``/``-q``
the ``$(MAKE)`` recipe line still executes this wrapper, but the sub-make only
prints or touches, so its exit 0 is not a pass: in that mode the footprint is
never computed, so no stamp is consulted or written.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

STAMP_DIR_REL = Path(".workbay-cache") / "checkall-stamps"
FRESH_ENV = "CHECK_ALL_FRESH"
# First input of every digest. Bump it whenever an older engine could have
# written a stamp that a run of this engine must not trust: v2 retires every
# stamp minted before make -n/-t/-q stopped stamping (CKDRY183-G1-F2).
STAMP_FORMAT_SALT = b"checkall-stamp-v2\0"
# GNU make single-letter flags under which recipes are not really executed:
# -n (--just-print/--dry-run), -t (--touch), -q (--question).
NON_EXECUTING_MAKE_FLAGS = frozenset("ntq")
# Single-letter options that take an argument (-C dir, -f file, -I dir, -j[N],
# -l[N], -o file, -O[type], -W file, -E string). In a "-<letters>" cluster any
# letter after one of these is that argument, never a flag.
ARG_TAKING_MAKE_FLAGS = frozenset("CfIjloOWE")
# MAKEFLAGS words; make escapes blanks inside an argument with a backslash.
_MAKEFLAGS_WORD = re.compile(r"(?:\\.|\S)+")
_MAKEFLAGS_CLUSTER = re.compile(r"-[A-Za-z]+")


def makeflags_non_executing(makeflags: str) -> bool:
    """True when a GNU make ``MAKEFLAGS`` value says recipes are not executed.

    GNU make (manual 5.7.3 "Communicating Options to a Sub-make" and 7.3
    "Conditionals that Test Flags") puts every single-letter option, without a
    leading dash, into the FIRST word of ``MAKEFLAGS`` (``n``, ``kn``,
    ``n -j4 --jobserver-auth=3,4``); that word is empty, so the value starts
    with a space or a ``-``, when no single-letter option is set. Long options
    and ``-j``/``--jobserver-auth`` follow, then ``VAR=value`` overrides after
    `` -- ``. An ``n`` inside a long option, a jobserver value or an override
    never counts.

    GNU make 3.81 (macOS ``/usr/bin/make``) builds the value differently: its
    ``define_makeflags`` writes a long option as its own `` --name`` word
    wherever it falls among the flags, so ``make -n --no-print-directory``
    exports `` --no-print-directory -n`` with an empty first word. So every
    word before `` -- `` that is a ``-<letters>`` cluster is scanned too, up
    to the first letter that takes an argument (``-Onone`` is ``-O none``).
    """
    if not makeflags:
        return False
    words = _MAKEFLAGS_WORD.findall(makeflags)
    if makeflags[0] not in " -" and words[0].isalpha():
        if not NON_EXECUTING_MAKE_FLAGS.isdisjoint(words[0]):
            return True
    for word in words:
        if word == "--":
            break
        if not _MAKEFLAGS_CLUSTER.fullmatch(word):
            continue
        for letter in word[1:]:
            if letter in ARG_TAKING_MAKE_FLAGS:
                break
            if letter in NON_EXECUTING_MAKE_FLAGS:
                return True
    return False


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
    repo: Path,
    key: str,
    paths: list[str],
    fingerprint: str = "",
    command: list[str] | None = None,
) -> str:
    """Sha256 over key + paths + fingerprint + command + tracked/diff/untracked state.

    Order is stable and intentional: key, sorted ``--paths`` list (so the same
    key with a different footprint never reuses a stale stamp), the caller
    fingerprint (mode inputs like PYTEST_WORKERS that change suite behavior
    without touching files), the wrapped ``command`` argv (so changing WHAT the
    suite runs — e.g. ``make test`` -> ``make test-strict`` — busts the stamp
    even when no ``--paths`` file changed), ``ls-files -s``, unstaged diff,
    staged diff, then each untracked path (sorted) with its ``hash-object``.

    :data:`STAMP_FORMAT_SALT` goes in first, so a stamp minted by an engine
    with an older (or no) salt never matches and that suite reruns once.
    """
    h = hashlib.sha256()
    h.update(STAMP_FORMAT_SALT)
    h.update(key.encode())
    h.update(b"\0")
    for p in sorted(paths):
        h.update(p.encode())
        h.update(b"\0")
    h.update(fingerprint.encode())
    h.update(b"\0")
    for arg in command or []:
        h.update(arg.encode())
        h.update(b"\0")
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


def _reject_unsafe_key(key: str, repo: Path) -> None:
    """Fail-closed guard: a ``--key`` must be a single, contained stamp-dir segment.

    Keys are Makefile-authored (trusted), but a mistaken or injected key such as
    ``../../.git/config`` must never let :func:`write_stamp` escape STAMP_DIR and
    clobber tracked files. Raising here is turned into fail-OPEN execution by
    :func:`cmd_run` (run the suite unconditionally, write no stamp).
    """
    if not key or key != key.strip():
        raise ValueError(f"empty or whitespace-padded --key {key!r}")
    if os.sep in key or (os.altsep and os.altsep in key) or ".." in Path(key).parts:
        raise ValueError(f"--key {key!r} must be a single segment (no '/', '\\', '..')")
    base = (repo / STAMP_DIR_REL).resolve()
    target = (repo / STAMP_DIR_REL / key).resolve()
    # Require a STRICT child: `target == base` (e.g. key '.') would let write_stamp
    # replace the stamp directory itself with a file.
    if target == base or base not in target.parents:
        raise ValueError(f"--key {key!r} must resolve to a file inside stamp dir {base}")


def read_stamp(path: Path) -> str | None:
    try:
        if not path.is_file():
            return None
        first = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        # Fail-open: an unreadable/oddly-permissioned stamp must run the suite
        # (return None), never abort the wrapper with a false red.
        return None
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
    dry_run = makeflags_non_executing(os.environ.get("MAKEFLAGS", ""))
    digest: str | None = None
    stamp: Path | None = None
    git_ok = True

    # Without a digest and stamp path neither the SKIP read nor the stamp
    # write below is reachable, so a non-executing make cannot mint a stamp.
    if not dry_run:
        try:
            repo = _repo_root()
            _reject_unsafe_key(key, repo)
            digest = compute_digest(repo, key, paths, fingerprint, command)
            stamp = stamp_path(repo, key)
        except Exception as exc:  # noqa: BLE001 — fail-open on any git/path/key error
            git_ok = False
            print(
                f"check-all-cache: WARN footprint/stamp unavailable ({exc}); running unconditionally",
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
            try:
                write_stamp(stamp, digest)
            except Exception as exc:  # noqa: BLE001 — fail-open: green must stay green
                # A stamp-write hiccup after a passing suite must never turn the
                # green suite red; skip caching this run and carry on.
                print(
                    f"check-all-cache: WARN stamp write failed ({exc}); "
                    f"suite passed, not cached",
                    file=sys.stderr,
                )
        if dry_run:
            print(f"check-all-cache: DRY-RUN {key} (no stamp written)")
        else:
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
