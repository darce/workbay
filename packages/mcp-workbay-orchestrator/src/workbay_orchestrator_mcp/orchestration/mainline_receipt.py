"""Requests, receipts and the stop decision for the post-landing VM run of main.

Pure library: the caller supplies ``state_dir`` and an ``is_ancestor`` probe, and
no function here runs a subprocess, touches git or the network, or reads a clock
except the ``now`` default of ``request_run``.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_REQUEST = "workbay.mainline_run_request.v1"
SCHEMA_RECEIPT = "workbay.mainline_run.v1"
SCHEMA_STOP = "workbay.mainline_stop.v1"
GOOD_VERDICTS = frozenset({"green", "known_red_only"})
RED_VERDICTS = frozenset({"new_red", "new_red_unconfirmed"})
VERDICTS = GOOD_VERDICTS | RED_VERDICTS
ANCESTRY_PROBES_PER_RECEIPT = 16
ANCESTRY_PROBE_FLOOR = 64

_SHA = re.compile(r"[0-9a-f]{40}")
_RECEIPT_FILE = re.compile(r"[0-9a-f]{40}\.json")
_SLUG_SAFE = re.compile(r"[A-Za-z0-9.-]")
_RECEIPT_KEYS = (
    "schema",
    "main_sha",
    "ref",
    "verdict",
    "attempt",
    "targets",
    "failed",
    "new_failures",
    "known_red",
    "now_green",
    "since_sha",
    "started_at",
    "finished_at",
    "budget",
    "detail",
)
_RECEIPT_STRING_LISTS = ("failed", "new_failures", "known_red", "now_green")
_RECEIPT_STRINGS = ("ref", "started_at", "finished_at", "detail")
# Every character str.splitlines() breaks on, mapped to its repr escape.
_LINE_BREAK_ESCAPES = {ord(char): repr(char)[1:-1] for char in "\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029"}

IsAncestor = Callable[[str, str], bool]


class _ProbeBudgetExceeded(RuntimeError):
    """The decision would exceed its bound on distinct ancestry probes."""


def _bounded_ancestor(is_ancestor: IsAncestor, budget: int) -> IsAncestor:
    """Memoize successful probes and bound calls, including calls that raise."""
    cache: dict[tuple[str, str], bool] = {}
    probes = 0

    def probe(a: str, b: str) -> bool:
        nonlocal probes
        pair = (a, b)
        if pair in cache:
            return cache[pair]
        if probes >= budget:
            raise _ProbeBudgetExceeded
        probes += 1
        cache[pair] = is_ancestor(a, b)
        return cache[pair]

    return probe


class MainlineReceiptError(ValueError):
    """A refused or unreadable request or receipt; ``code`` holds the reason token."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


def _is_sha(value: Any) -> bool:
    return isinstance(value, str) and _SHA.fullmatch(value) is not None


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_sha(sha: Any) -> str:
    if not _is_sha(sha):
        raise MainlineReceiptError("mainline_sha_invalid", f"not a 40-character lowercase sha: {sha!r}")
    return sha


def _check_ref(ref: Any) -> str:
    if not isinstance(ref, str) or not ref:
        raise MainlineReceiptError("mainline_request_invalid", f"ref must be a non-empty string: {ref!r}")
    return ref


def _root(state_dir: str | os.PathLike[str]) -> Path:
    return Path(state_dir) / "mainline-runs"


def _ref_slug(ref: str) -> str:
    """Encode ``ref`` as a file-name stem; the encoding is injective, so each stem names one ref."""
    parts: list[str] = []
    for char in ref:
        if _SLUG_SAFE.fullmatch(char):
            parts.append(char)
        elif char == "/":
            parts.append("_")
        else:
            parts.extend(f"%{byte:02X}" for byte in char.encode("utf-8"))
    return "".join(parts)


def _request_path(state_dir: str | os.PathLike[str], ref: str) -> Path:
    return _root(state_dir) / "requests" / f"{_ref_slug(ref)}.json"


def _receipts_dir(state_dir: str | os.PathLike[str]) -> Path:
    return _root(state_dir) / "receipts"


def _receipt_path(state_dir: str | os.PathLike[str], sha: str) -> Path:
    return _receipts_dir(state_dir) / f"{sha}.json"


def _utc_stamp(now: datetime | None) -> str:
    moment = datetime.now(timezone.utc) if now is None else now
    if moment.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _encode(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, encoded: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(name)
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        if temporary is not None:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise


@contextlib.contextmanager
def _write_locked(state_dir: str | os.PathLike[str]) -> Iterator[None]:
    # One non-blocking advisory lock serialises every writer and compare-and-clear.
    lock_path = _root(state_dir) / "write.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise MainlineReceiptError("mainline_write_locked", f"another writer holds {lock_path.name}") from None
        yield


def _request_fault(record: Any, ref: str) -> str | None:
    if not isinstance(record, dict):
        return "not a JSON object"
    if record.get("schema") != SCHEMA_REQUEST:
        return f"schema is {record.get('schema')!r}, expected {SCHEMA_REQUEST!r}"
    if record.get("ref") != ref:
        return f"stored ref {record.get('ref')!r} differs from requested ref {ref!r}"
    if not _is_sha(record.get("sha")):
        return "sha is not 40 lowercase hex characters"
    if not isinstance(record.get("source"), str) or not record["source"]:
        return "source is not a non-empty string"
    if not isinstance(record.get("requested_at"), str):
        return "requested_at is not a string"
    return None


def _read_request_slot(path: Path) -> bytes | None:
    # The write lock may be held here, so refuse a link, FIFO or device instead of waiting on it.
    try:
        if not stat.S_ISREG(path.lstat().st_mode):
            raise MainlineReceiptError("mainline_request_unreadable", f"{path.name}: not a regular file")
        # lstat alone races with replacement: never follow a link or wait on a FIFO.
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise MainlineReceiptError("mainline_request_unreadable", f"{path.name}: not a regular file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            return stream.read()
    finally:
        os.close(fd)


def _read_request(state_dir: str | os.PathLike[str], ref: str) -> dict[str, Any] | None:
    path = _request_path(state_dir, ref)
    try:
        raw = _read_request_slot(path)
    except OSError as exc:
        raise MainlineReceiptError("mainline_request_unreadable", f"{path.name}: {type(exc).__name__}") from exc
    if raw is None:
        return None
    try:
        record = json.loads(raw)
    except ValueError as exc:
        raise MainlineReceiptError("mainline_request_unreadable", f"{path.name}: not valid JSON") from exc
    fault = _request_fault(record, ref)
    if fault is not None:
        raise MainlineReceiptError("mainline_request_unreadable", f"{path.name}: {fault}")
    return record


def request_run(
    state_dir: str | os.PathLike[str],
    sha: str,
    *,
    ref: str,
    source: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Make ``sha`` the pending run for ``ref``; the newest request wins."""
    _check_sha(sha)
    _check_ref(ref)
    if not isinstance(source, str) or not source:
        raise MainlineReceiptError("mainline_request_invalid", "source must be a non-empty string")
    stamp = _utc_stamp(now)
    with _write_locked(state_dir):
        previous = _read_request(state_dir, ref)
        if previous is not None and previous["sha"] == sha:
            superseded = None
            requested_at = previous["requested_at"]
        else:
            superseded = previous["sha"] if previous is not None else None
            requested_at = stamp
        record = {
            "schema": SCHEMA_REQUEST,
            "ref": ref,
            "sha": sha,
            "source": source,
            "requested_at": requested_at,
        }
        _atomic_write(_request_path(state_dir, ref), _encode(record))
    return {**record, "superseded": superseded}


def pending_request(state_dir: str | os.PathLike[str], *, ref: str) -> dict[str, Any] | None:
    """Return the pending request for ``ref``, or None when no slot file exists."""
    _check_ref(ref)
    return _read_request(state_dir, ref)


def clear_request(state_dir: str | os.PathLike[str], *, ref: str, sha: str) -> bool:
    """Remove the pending request only while it still names ``sha``."""
    _check_ref(ref)
    _check_sha(sha)
    with _write_locked(state_dir):
        current = _read_request(state_dir, ref)
        if current is None or current["sha"] != sha:
            return False
        _request_path(state_dir, ref).unlink()
        return True


def _invalid(key: str, why: str) -> MainlineReceiptError:
    return MainlineReceiptError("mainline_receipt_invalid", f"key '{key}' {why}")


def _receipt_unreadable(name: str, why: str) -> MainlineReceiptError:
    return MainlineReceiptError("mainline_receipt_unreadable", f"{name}: {why}")


def _check_evidence(out: dict[str, Any]) -> None:
    # The verdict is decided from the evidence fields, so they must agree with it.
    verdict = out["verdict"]
    new_failures = out["new_failures"]
    known_red = out["known_red"]
    if not out["targets"]:
        raise _invalid("targets", "must name at least one target")
    if verdict in GOOD_VERDICTS and new_failures:
        raise _invalid("new_failures", f"must be empty when 'verdict' is '{verdict}'")
    if verdict == "green" and any(entry["rc"] != 0 for entry in out["targets"]):
        raise _invalid("targets", "every 'rc' must be 0 when 'verdict' is 'green'")
    if verdict == "green" and out["failed"]:
        raise _invalid("failed", "must be empty when 'verdict' is 'green'")
    if verdict in RED_VERDICTS and not new_failures:
        raise _invalid("new_failures", f"must be non-empty when 'verdict' is '{verdict}'")
    if set(new_failures) & set(known_red):
        raise _invalid("known_red", "must not share entries with 'new_failures'")
    if out["failed"] != sorted(set(new_failures) | set(known_red)):
        raise _invalid("failed", "must equal the union of 'new_failures' and 'known_red'")


def _normalize_receipt(receipt: Any) -> dict[str, Any]:
    if not isinstance(receipt, Mapping):
        raise _invalid("receipt", "must be a mapping")
    out = dict(receipt)
    for key in _RECEIPT_KEYS:
        if key not in out:
            raise _invalid(key, "is required")
    if out["schema"] != SCHEMA_RECEIPT:
        raise _invalid("schema", f"must be {SCHEMA_RECEIPT!r}")
    if not _is_sha(out["main_sha"]):
        raise _invalid("main_sha", "must be a 40-character lowercase sha")
    for key in _RECEIPT_STRINGS:
        if not isinstance(out[key], str):
            raise _invalid(key, "must be a string")
    if not isinstance(out["verdict"], str) or out["verdict"] not in VERDICTS:
        raise _invalid("verdict", f"must be one of {sorted(VERDICTS)}")
    if not _is_int(out["attempt"]) or out["attempt"] < 1:
        raise _invalid("attempt", "must be an int >= 1")
    targets = out["targets"]
    if not isinstance(targets, list):
        raise _invalid("targets", "must be a list")
    for index, entry in enumerate(targets):
        if not isinstance(entry, Mapping) or not isinstance(entry.get("target"), str) or not _is_int(entry.get("rc")):
            raise _invalid("targets", f"entry {index} needs a str target and an int rc")
    out["targets"] = [dict(entry) for entry in targets]
    for key in _RECEIPT_STRING_LISTS:
        values = out[key]
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise _invalid(key, "must be a list of strings")
        out[key] = sorted(set(values))
    if out["since_sha"] is not None and not _is_sha(out["since_sha"]):
        raise _invalid("since_sha", "must be None or a 40-character lowercase sha")
    if not isinstance(out["budget"], Mapping):
        raise _invalid("budget", "must be a mapping")
    out["budget"] = dict(out["budget"])
    _check_evidence(out)
    return out


def _read_receipt_file(path: Path) -> dict[str, Any] | None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _receipt_unreadable(path.name, type(exc).__name__) from exc
    if not stat.S_ISREG(mode):
        raise _receipt_unreadable(path.name, "not a regular file")
    try:
        # lstat alone races with replacement: never follow a link or wait on a FIFO.
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise _receipt_unreadable(path.name, "not a regular file")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                raw = stream.read()
        finally:
            os.close(fd)
    except OSError as exc:
        raise _receipt_unreadable(path.name, type(exc).__name__) from exc
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise _receipt_unreadable(path.name, "not valid JSON") from exc
    try:
        receipt = _normalize_receipt(payload)
    except MainlineReceiptError as exc:
        raise _receipt_unreadable(path.name, str(exc)) from exc
    if path.name != f"{receipt['main_sha']}.json":
        raise _receipt_unreadable(path.name, f"stored main_sha {receipt['main_sha']} does not match the file name")
    return receipt


def write_receipt(state_dir: str | os.PathLike[str], receipt: Mapping[str, Any]) -> Path:
    """Store the receipt for its commit; a same-sha rewrite needs a higher attempt."""
    normalized = _normalize_receipt(receipt)
    sha = normalized["main_sha"]
    path = _receipt_path(state_dir, sha)
    try:
        encoded = _encode(normalized)
    except (TypeError, ValueError) as exc:
        raise _invalid("receipt", f"is not JSON-serialisable: {exc}") from exc
    with _write_locked(state_dir):
        existing = _read_receipt_file(path)
        if existing is not None and normalized["attempt"] <= existing["attempt"]:
            raise MainlineReceiptError(
                "mainline_receipt_exists",
                f"receipt for {sha} already holds attempt {existing['attempt']}",
            )
        if existing is not None and existing["verdict"] == "new_red" and normalized["verdict"] == "new_red_unconfirmed":
            raise MainlineReceiptError(
                "mainline_receipt_exists",
                f"receipt for {sha} is confirmed red and cannot become unconfirmed",
            )
        _atomic_write(path, encoded)
    return path


def read_receipt(state_dir: str | os.PathLike[str], sha: str) -> dict[str, Any] | None:
    """Return the receipt for ``sha``, or None when none was written."""
    _check_sha(sha)
    return _read_receipt_file(_receipt_path(state_dir, sha))


def list_receipts(state_dir: str | os.PathLike[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Return the valid receipts and the names of every entry that is not one."""
    directory = _receipts_dir(state_dir)
    if not directory.exists():
        return [], []
    valid: list[dict[str, Any]] = []
    unreadable: list[str] = []
    for path in sorted(directory.iterdir()):
        try:
            receipt = _read_receipt_file(path)
        except MainlineReceiptError:
            unreadable.append(path.name)
            continue
        if receipt is not None:
            valid.append(receipt)
        else:
            unreadable.append(path.name)
    return valid, unreadable


def _newest(receipts: list[dict[str, Any]], is_ancestor: IsAncestor) -> dict[str, Any] | None:
    # One pass keeps the latest candidate; the second pass proves it is the newest, so 2(n-1) probes.
    if not receipts:
        return None
    candidate = receipts[0]
    for other in receipts[1:]:
        if is_ancestor(candidate["main_sha"], other["main_sha"]):
            candidate = other
    if all(
        is_ancestor(other["main_sha"], candidate["main_sha"])
        for other in receipts
        if other["main_sha"] != candidate["main_sha"]
    ):
        return candidate
    return None


def _decision(
    state: str,
    stop: bool,
    tip: str,
    unreadable: list[str],
    detail: str,
    *,
    red_sha: str | None = None,
    since_sha: str | None = None,
    new_failures: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema": SCHEMA_STOP,
        "state": state,
        "stop": stop,
        "tip": tip,
        "red_sha": red_sha,
        "since_sha": since_sha,
        "new_failures": list(new_failures or []),
        "unreadable": list(unreadable),
        "detail": detail,
    }


def _frontier(
    receipts: list[dict[str, Any]], precedes: IsAncestor, *, width: int | None = None
) -> list[dict[str, Any]]:
    """Keep the minimal receipts under ``precedes``; reversing it gives the maximal ones.

    Once more than ``width`` receipts are minimal, building stops and every receipt is
    returned: any superset of the frontier gives the same reachability answers.
    """
    frontier: list[dict[str, Any]] = []
    for receipt in receipts:
        remaining = []
        for other in frontier:
            if precedes(other["main_sha"], receipt["main_sha"]):
                break
            if not precedes(receipt["main_sha"], other["main_sha"]):
                remaining.append(other)
        else:
            frontier = [*remaining, receipt]
            if width is not None and len(frontier) > width:
                return receipts
    return frontier


def _open_confirmed_reds(applicable: list[dict[str, Any]], is_ancestor: IsAncestor) -> list[dict[str, Any]]:
    """Every baselined confirmed red without a later good receipt, sorted by sha.

    Minimal goods prove baselines; maximal goods prove clearance by transitivity.
    On linear main, frontier construction costs at most 4*(|good|-1) probes and
    red checks cost at most 2*|red|: O(|good| + 2*|red|), rather than their product.
    Wider DAG frontiers retain the same reachability answers.
    With no confirmed red there is nothing to baseline or clear, so no frontier is built.
    """
    reds = [receipt for receipt in applicable if receipt["verdict"] == "new_red"]
    if not reds:
        return []
    good = [receipt for receipt in applicable if receipt["verdict"] in GOOD_VERDICTS]
    minimal = _frontier(good, is_ancestor)
    maximal = _frontier(good, lambda a, b: is_ancestor(b, a))
    open_reds = []
    for red in reds:
        red_sha = red["main_sha"]
        baselined = any(
            receipt["main_sha"] != red_sha and is_ancestor(receipt["main_sha"], red_sha) for receipt in minimal
        )
        cleared = any(is_ancestor(red_sha, receipt["main_sha"]) for receipt in maximal)
        if baselined and not cleared:
            open_reds.append(red)
    return sorted(open_reds, key=lambda receipt: receipt["main_sha"])


def _held_unreadable(good: list[dict[str, Any]], shas: list[str], is_ancestor: IsAncestor) -> list[str]:
    """Unreadable receipt shas, read as confirmed reds, that a good receipt precedes and none follows, sorted.

    As in _open_confirmed_reds, minimal goods prove baselines and maximal goods prove clearance,
    so on linear main this costs at most 4*(|good|-1) + 2*|shas| probes. Adding a good to a frontier
    of width w costs up to 2*w probes, while probing every (good, sha) pair costs |shas| per good, so
    a frontier is abandoned for those pairs once 2*w exceeds |shas|. Each check then costs at most
    2*|good|*|shas| probes even when the goods form an antichain, instead of |good|**2.
    Clearance is probed only for the baselined shas.
    """
    if not shas:
        return []
    minimal = _frontier(good, is_ancestor, width=len(shas) // 2)
    baselined = [
        sha
        for sha in shas
        if any(receipt["main_sha"] != sha and is_ancestor(receipt["main_sha"], sha) for receipt in minimal)
    ]
    if not baselined:
        return []
    maximal = _frontier(good, lambda a, b: is_ancestor(b, a), width=len(baselined) // 2)
    return sorted(sha for sha in baselined if not any(is_ancestor(sha, receipt["main_sha"]) for receipt in maximal))


def _open_red_stop(
    tip: str,
    unreadable: list[str],
    applicable: list[dict[str, Any]],
    is_ancestor: IsAncestor,
    context: str,
) -> dict[str, Any] | None:
    """The mainline_red stop for every open confirmed red, or None when there is none."""
    reds = _open_confirmed_reds(applicable, is_ancestor)
    if not reds:
        return None
    newest_red = _newest(reds, is_ancestor)
    red = newest_red if newest_red is not None else reds[0]
    good = [receipt for receipt in applicable if receipt["verdict"] in GOOD_VERDICTS]
    newest_good = _newest(good, is_ancestor)
    shas = ", ".join(receipt["main_sha"] for receipt in reds)
    return _decision(
        "mainline_red",
        True,
        tip,
        unreadable,
        f"confirmed red receipt(s) {shas} have no good receipt after them; {context}",
        red_sha=red["main_sha"],
        since_sha=newest_good["main_sha"] if newest_good is not None else None,
        new_failures=sorted({name for receipt in reds for name in receipt["new_failures"]}),
    )


def _ambiguous_decision(
    tip: str, unreadable: list[str], applicable: list[dict[str, Any]], is_ancestor: IsAncestor
) -> dict[str, Any]:
    shas = ", ".join(sorted(receipt["main_sha"] for receipt in applicable))
    detail = f"applicable receipts are not ordered by ancestry: {shas}"
    stop = _open_red_stop(tip, unreadable, applicable, is_ancestor, detail)
    if stop is not None:
        return stop
    return _decision("receipt_order_ambiguous", False, tip, unreadable, detail)


def _newer_unreadable(unreadable: list[str], newest_sha: str, tip: str, is_ancestor: IsAncestor) -> list[str]:
    """Names of unreadable receipt files that apply to ``tip`` and are not ancestors of ``newest_sha``."""
    return [
        name
        for name in unreadable
        if _RECEIPT_FILE.fullmatch(name)
        and is_ancestor(name.removesuffix(".json"), tip)
        and not is_ancestor(name.removesuffix(".json"), newest_sha)
    ]


def _verdict_decision(
    tip: str,
    unreadable: list[str],
    applicable: list[dict[str, Any]],
    newest: dict[str, Any],
    is_ancestor: IsAncestor,
) -> dict[str, Any]:
    good = [receipt for receipt in applicable if receipt["verdict"] in GOOD_VERDICTS]
    newest_good = _newest(good, is_ancestor) if good else None
    since_sha = newest_good["main_sha"] if newest_good is not None else None
    sha = newest["main_sha"]
    verdict = newest["verdict"]
    if verdict in GOOD_VERDICTS:
        return _decision(
            "mainline_ok",
            False,
            tip,
            unreadable,
            f"newest applicable receipt {sha} is {verdict}",
            since_sha=since_sha,
        )
    red = {"red_sha": sha, "since_sha": since_sha, "new_failures": newest["new_failures"]}
    if verdict == "new_red_unconfirmed":
        stop = _open_red_stop(tip, unreadable, applicable, is_ancestor, f"newer receipt {sha} is new_red_unconfirmed")
        if stop is not None:
            return stop
        detail = f"receipt {sha} is new_red_unconfirmed; not stopping until it is confirmed"
        return _decision("mainline_red_unconfirmed", False, tip, unreadable, detail, **red)
    if not good:
        detail = f"receipt {sha} is red and no earlier good receipt is recorded; not stopping"
        return _decision("mainline_red_unbaselined", False, tip, unreadable, detail, **red)
    goods = ", ".join(sorted(receipt["main_sha"] for receipt in good))
    detail = f"receipt {sha} is red; good receipts before it: {goods}"
    red["new_failures"] = sorted(
        {name for receipt in _open_confirmed_reds(applicable, is_ancestor) for name in receipt["new_failures"]}
    )
    return _decision("mainline_red", True, tip, unreadable, detail, **red)


def _stop_decision(
    valid: list[dict[str, Any]], unreadable: list[str], tip: str, is_ancestor: IsAncestor
) -> dict[str, Any]:
    if not valid and not unreadable:
        return _decision("no_receipt", False, tip, unreadable, "no receipt recorded")
    applicable = [receipt for receipt in valid if is_ancestor(receipt["main_sha"], tip)]
    if not applicable:
        if unreadable:
            # Entry names are raw, so escape line breaks to keep the detail on one line.
            shown = (name.translate(_LINE_BREAK_ESCAPES) for name in unreadable)
            detail = f"unreadable receipt file(s): {', '.join(shown)}"
            return _decision("receipt_unreadable", False, tip, unreadable, detail)
        return _decision("no_applicable_receipt", False, tip, unreadable, f"no recorded receipt applies to {tip}")
    newest = _newest(applicable, is_ancestor)
    if newest is None:
        decision = _ambiguous_decision(tip, unreadable, applicable, is_ancestor)
        newer: list[str] = []
    else:
        decision = _verdict_decision(tip, unreadable, applicable, newest, is_ancestor)
        newer = _newer_unreadable(unreadable, newest["main_sha"], tip, is_ancestor)
    named = f"newer unreadable receipt file(s): {', '.join(newer)}"
    if decision["stop"]:
        if newer:
            decision["detail"] = f"{decision['detail']}; {named}"
        return decision
    # An unreadable receipt may hold a confirmed red, so read it as one: it keeps landings
    # stopped while a good receipt precedes it and no good receipt follows it.
    good = [receipt for receipt in applicable if receipt["verdict"] in GOOD_VERDICTS]
    worst = [
        name.removesuffix(".json")
        for name in unreadable
        if _RECEIPT_FILE.fullmatch(name) and is_ancestor(name.removesuffix(".json"), tip)
    ]
    held = [f"{sha}.json" for sha in _held_unreadable(good, worst, is_ancestor)]
    if held:
        detail = (
            f"unreadable receipt file(s) {', '.join(held)} follow a good receipt and may hold a confirmed red; "
            "only a good receipt after them lifts the stop"
        )
        return _decision("receipt_unreadable", True, tip, unreadable, detail, since_sha=decision["since_sha"])
    if newer:
        return _decision("receipt_unreadable", False, tip, unreadable, named, since_sha=decision["since_sha"])
    return decision


def stop_signal(state_dir: str | os.PathLike[str], *, tip: str, is_ancestor: IsAncestor) -> dict[str, Any]:
    """Decide whether recorded mainline health stops further landings at ``tip``.

    Only a good receipt lifts a stop: a later unconfirmed red does not clear a confirmed one.
    Every helper shares a memoized probe budget linear in the recorded receipt count.
    Exhaustion stops landings with receipt_probe_budget_exceeded; probe errors propagate.
    """
    _check_sha(tip)
    valid, unreadable = list_receipts(state_dir)
    budget = ANCESTRY_PROBES_PER_RECEIPT * (len(valid) + len(unreadable)) + ANCESTRY_PROBE_FLOOR
    probe = _bounded_ancestor(is_ancestor, budget)
    try:
        return _stop_decision(valid, unreadable, tip, probe)
    except _ProbeBudgetExceeded:
        detail = f"ancestry probe budget {budget} exceeded; receipts valid={len(valid)}, unreadable={len(unreadable)}"
        return _decision("receipt_probe_budget_exceeded", True, tip, unreadable, detail)
