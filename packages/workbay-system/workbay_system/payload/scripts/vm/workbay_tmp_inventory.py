#!/usr/bin/env python3
"""Read-only, budgeted tmp inventory; observations confer no cleanup authority."""
from __future__ import annotations

import argparse
from contextlib import contextmanager, ExitStack
import json
import math
import os
import stat
import time


CURSOR_VERSION = 1
CURSOR_BYTES = 65536
CURSOR_DEPTH = 64


class _Deadline(Exception):
    pass


def _stamp(info):
    return [info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns]


@contextmanager
def _directory(name, parent_fd, stamp, check):
    """The allocating scope owns the fd, including failed identity checks."""
    check()
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                 dir_fd=parent_fd)
    try:
        check()
        if stamp is not None and _stamp(os.fstat(fd)) != stamp:
            raise ValueError("directory_generation_changed")
        check()
        yield fd
    finally:
        os.close(fd)


def _encoded_size(value):
    # Stop at the transport bound rather than constructing an unbounded token.
    size = 0
    for chunk in json.JSONEncoder(separators=(",", ":"), allow_nan=False).iterencode(value):
        size += len(chunk.encode("utf-8"))
        if size > CURSOR_BYTES:
            raise ValueError("cursor_limit")
    return size


def _cursor(value, root):
    """Admit only a bounded DFS position, never a restart on invalid input."""
    _encoded_size(value)
    if not isinstance(value, dict) or set(value) != {"version", "root", "stack", "tainted"}:
        raise ValueError("cursor_malformed")
    if type(value["version"]) is not int or value["version"] != CURSOR_VERSION:
        raise ValueError("cursor_version")
    if not isinstance(value["root"], str) or value["root"] != root:
        raise ValueError("cursor_wrong_root")
    frames = value["stack"]
    if not isinstance(frames, list) or not 1 <= len(frames) <= CURSOR_DEPTH:
        raise ValueError("cursor_state_limit")
    if type(value["tainted"]) is not bool:
        raise ValueError("cursor_malformed")
    for index, frame in enumerate(frames):
        if not isinstance(frame, dict) or set(frame) != {"name", "stamp", "offset"}:
            raise ValueError("cursor_malformed")
        name = frame["name"]
        if not isinstance(name, str) or (name != "" if index == 0 else
                not name or name in (".", "..") or "/" in name or "\0" in name):
            raise ValueError("cursor_path")
        if type(frame["offset"]) is not int or not 0 <= frame["offset"] <= 2**63 - 1:
            raise ValueError("cursor_position")
        if index < len(frames) - 1 and frame["offset"] == 0:
            raise ValueError("cursor_position")
        if not isinstance(frame["stamp"], list) or len(frame["stamp"]) != 4 or any(
                type(n) is not int or n < 0 for n in frame["stamp"]):
            raise ValueError("cursor_generation")
    # Own the state; no early return may mutate the caller's JSON object.
    return {"version": CURSOR_VERSION, "root": root, "tainted": value["tainted"],
            "stack": [{"name": f["name"], "stamp": f["stamp"][:], "offset": f["offset"]}
                      for f in frames]}


def scan(root, budget_seconds=2.0, sample_limit=16, cursor=None):
    """Observe per-call deltas, optionally continuing a portable DFS position.

    Counts concern root children; logical bytes include regular files and
    directories at all depths, per pathname, excluding root and symlink targets.
    FIFO/socket observations contribute zero bytes. Samples contain absolute
    paths and UID evidence, never cleanup authority. Resumed calls report lower
    bounds even on completion, rather than an exact cumulative headline.

    Unchanged finite trees within the state bounds eventually finish given
    slices sufficient for reopening, validation/replay and a new observation.
    One deadline covers that work and nested traversal. It is checked between
    OS calls and cannot interrupt a blocked syscall. Generation stamps detect
    observable active-directory changes; this live census is not a snapshot.
    """
    root = os.path.abspath(os.fspath(root))
    budget_seconds = float(budget_seconds)
    if not math.isfinite(budget_seconds) or budget_seconds < 0:
        raise ValueError("budget_seconds must be finite and nonnegative")
    if not isinstance(sample_limit, int) or sample_limit < 0:
        raise ValueError("sample_limit must be a nonnegative integer")
    deadline = time.monotonic() + budget_seconds
    result = {
        "quality": "complete", "truncated": False,
        "real_entries_count": 0, "files_count": 0, "directories_count": 0,
        "symlink_held_count": 0, "sample": [], "errors": [],
        "errors_omitted": 0, "coverage": "incomplete", "next_cursor": None,
    }
    measured = 0
    paused = False
    tainted = False
    state = None
    active = []

    def partial(reason, path, error=True):
        nonlocal tainted
        result["quality"] = "lower_bound"
        result["truncated"] = True
        tainted = tainted or error
        if len(result["errors"]) < max(1, sample_limit):
            result["errors"].append({"path": str(path), "reason": reason})
        else:
            result["errors_omitted"] += 1

    def check():
        if time.monotonic() >= deadline:
            raise _Deadline

    def finish():
        nonlocal paused
        # A slow final syscall cannot manufacture exact success.
        if time.monotonic() >= deadline and not paused:
            partial("deadline_exhausted", root, error=False)
            paused = True
        if state is not None:
            state["tainted"] = tainted
            if state["stack"]:
                try:
                    _encoded_size(state)
                except (ValueError, OverflowError):
                    partial("cursor_limit", root)
                else:
                    if paused:
                        result["coverage"] = "partial"
                        result["next_cursor"] = state
            elif not paused and not tainted:
                result["coverage"] = "complete"
        if cursor is not None:
            result["quality"] = "lower_bound"
            result["truncated"] = True
        result["count_quality"] = result["quality"]
        result["tmp_bytes_scanned_lower_bound" if result["truncated"] else "tmp_bytes"] = measured
        return result

    def acquire(frame, parent_fd, name):
        # A failed acquisition has its own scope; successful scopes are retained
        # only along the depth-bounded active path, then closed in reverse order.
        scope = ExitStack()
        try:
            fd = scope.enter_context(_directory(name, parent_fd, frame["stamp"], check))
        except BaseException:
            scope.close()
            raise
        authority = {"fd": fd, "scope": scope, "entries": None}
        active.append(authority)
        return authority

    def iterator(authority):
        check()
        authority["entries"] = authority["scope"].enter_context(os.scandir(authority["fd"]))
        check()
        return authority["entries"]

    if cursor is not None:
        try:
            state = _cursor(cursor, root)
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
            partial(str(exc) if isinstance(exc, ValueError) else "cursor_invalid_or_limit", root)
            return finish()
        tainted = state["tainted"]
        if tainted:
            partial("prior_observation_error", root)
    try:
        check()
        info = os.lstat(root)
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("root_not_directory")
        if state is None:
            state = {"version": CURSOR_VERSION, "root": root, "tainted": False,
                     "stack": [{"name": "", "stamp": _stamp(info), "offset": 0}]}
        elif state["stack"][0]["stamp"] != _stamp(info):
            raise ValueError("cursor_stale_generation")
        frames = state["stack"]
        # Ancestors are trusted configuration. Acquire root relative to its
        # parent; every subsequent component uses only its acquired parent fd.
        with _directory(os.path.join(os.path.dirname(root), "."), None, None, check) as parent_fd:
            acquire(frames[0], parent_fd, os.path.basename(root) or ".")
        # Validate the ENTIRE active chain before opening/replaying iterators.
        # A stale nested frame must refuse with zero new observations too.
        for frame in frames[1:]:
            acquire(frame, active[-1]["fd"], frame["name"])
        for index, (frame, authority) in enumerate(zip(frames, active)):
            entries = iterator(authority)
            last_name = None
            for _ in range(frame["offset"]):
                check()
                try:
                    entry = next(entries)
                except StopIteration as exc:
                    raise ValueError("cursor_position") from exc
                last_name = entry.name
                check()
            if index + 1 < len(frames) and last_name != frames[index + 1]["name"]:
                raise ValueError("cursor_position")

        while frames:
            check()
            frame, authority = frames[-1], active[-1]
            path = os.path.join(root, *(f["name"] for f in frames[1:]))
            try:
                entry = next(authority["entries"])
                check()
            except StopIteration:
                check()
                if _stamp(os.fstat(authority["fd"])) != frame["stamp"]:
                    partial("directory_generation_changed", path)
                check()
                active.pop()["scope"].close()
                frames.pop()
                continue
            except OSError as exc:
                partial(type(exc).__name__, path)
                active.pop()["scope"].close()
                frames.pop()
                continue
            child_path = os.path.join(path, entry.name)
            # An entry delivered at expiration is replayed next time. Once
            # admitted for metadata it is consumed, even on a metadata error.
            frame["offset"] += 1
            try:
                info = os.stat(entry.name, dir_fd=authority["fd"], follow_symlinks=False)
            except OSError as exc:
                partial(type(exc).__name__, child_path)
                continue
            mode = info.st_mode
            kind = ("symlink" if stat.S_ISLNK(mode) else "file" if stat.S_ISREG(mode)
                    else "directory" if stat.S_ISDIR(mode) else "fifo" if stat.S_ISFIFO(mode)
                    else "socket" if stat.S_ISSOCK(mode) else "other")
            if len(frames) == 1:
                if kind == "symlink":
                    result["symlink_held_count"] += 1
                else:
                    result["real_entries_count"] += 1
                    if kind == "file":
                        result["files_count"] += 1
                    elif kind == "directory":
                        result["directories_count"] += 1
                if len(result["sample"]) < sample_limit:
                    row = {"path": child_path, "kind": kind, "uid": info.st_uid}
                    if kind == "symlink":
                        row["state"] = "held"
                    result["sample"].append(row)
            if kind in ("file", "directory"):
                measured += info.st_size
            elif kind == "other":
                partial("unsupported_entry_kind", child_path)
            if kind == "directory":
                if len(frames) >= CURSOR_DEPTH:
                    raise ValueError("cursor_depth_limit")
                child_frame = {"name": entry.name, "stamp": _stamp(info), "offset": 0}
                frames.append(child_frame)
                _encoded_size(state)
                try:
                    child = acquire(child_frame, authority["fd"], entry.name)
                    iterator(child)
                except (OSError, ValueError) as exc:
                    # A live inaccessible child does not hide readable siblings.
                    partial(str(exc) if isinstance(exc, ValueError) else type(exc).__name__, child_path)
                    if len(active) == len(frames):
                        active.pop()["scope"].close()
                    frames.pop()
    except _Deadline:
        paused = True
        partial("deadline_exhausted", root, error=False)
    except (OSError, ValueError) as exc:
        partial(str(exc) if isinstance(exc, ValueError) else type(exc).__name__, root)
        state = None
    finally:
        for authority in reversed(active):
            authority["scope"].close()
    return finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["scan"])
    parser.add_argument("--root", required=True)
    parser.add_argument("--budget-seconds", type=float, default=2.0)
    parser.add_argument("--json", action="store_true", help="emit the JSON measurement (also the default)")
    args = parser.parse_args()
    if not math.isfinite(args.budget_seconds) or args.budget_seconds < 0:
        parser.error("--budget-seconds must be finite and nonnegative")
    print(json.dumps(scan(args.root, args.budget_seconds), sort_keys=True))


if __name__ == "__main__":
    main()
