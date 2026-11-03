#!/usr/bin/env python3
"""VM-local leased host reservations (decision 37518).

Capacity dictionaries use ``cpu`` (cores) and ``memory_bytes``. Times in records
are UTC Unix seconds; start_at is RFC 3339 advance notice. Consumers
share admission_view; this module never discovers, stops, or preempts lanes.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

LIVE = {"pending", "granted"}
TERMINAL = {"released", "reservation_expired", "drain_timeout"}
UNREADABLE = {"refusal": "store_unreadable"}
CAPS = ("cpu", "memory_bytes")


def resolve_root(value=None):
    raw = value if value is not None else os.environ.get("WORKBAY_REMOTE_AGENT_ROOT", "workbay-sandbox")
    root = Path(raw).expanduser()
    if not root.is_absolute():
        root = Path.home() / root
    return root.resolve()


def _number(value, positive=True):
    try:
        return type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0)
    except OverflowError:
        return False


def _start_epoch(value):
    """Return UTC Unix seconds for timezone-qualified RFC 3339 notice."""
    if not isinstance(value, str) or "T" not in value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def _start(value):
    return value is None or _start_epoch(value) is not None


def expected_runtime_s(final_summaries, window=5):
    """Maximum measured phase sum in the newest window for one lane class."""
    if type(window) is not int or window <= 0 or not final_summaries:
        return None
    samples = []
    for summary in final_summaries[-window:]:
        phases = summary.get("phases") if isinstance(summary, dict) else None
        if not isinstance(phases, dict) or not phases:
            return None
        values = [phase.get("wall_seconds") if isinstance(phase, dict) else None for phase in phases.values()]
        if not all(_number(value, False) for value in values):
            return None
        try:
            sample = sum(values)
        except OverflowError:
            return None
        if not _number(sample, False):
            return None
        samples.append(sample)
    return max(samples)


def lane_window_until(reservations, deferrals, config):
    """Demand-backed readiness notice after the latest exclusive terminal."""
    duration = config.get("lane_window_s")
    if type(duration) is not int or duration <= 0:
        return None
    ended = [r for r in reservations if r["exclusive"] and r["state"] in TERMINAL]
    if not ended:
        return None
    last = max(ended, key=lambda r: (r.get("ended_at", r["renewed_at"]), r["seq"]))
    if deferrals.get(last["id"], 0) <= 0:
        return None
    return last.get("ended_at", last["renewed_at"]) + duration


def _valid_record(r):
    return (
        isinstance(r, dict)
        and isinstance(r.get("id"), str)
        and len(r["id"]) == 32
        and all(c in "0123456789abcdef" for c in r["id"])
        and all(isinstance(r.get(k), str) and bool(r[k].strip()) for k in ("owner", "purpose"))
        and _number(r.get("cpu"))
        and type(r.get("memory_bytes")) is int
        and r["memory_bytes"] > 0
        and type(r.get("exclusive")) is bool
        and all(_number(r.get(k)) for k in ("ttl_s", "max_drain_s", "max_hold_s"))
        and all(_number(r.get(k), False) for k in ("created_at", "renewed_at", "expires_at", "drain_deadline"))
        and type(r.get("seq")) is int
        and r["seq"] > 0
        and r.get("state") in LIVE | TERMINAL
        and (r.get("granted_at") is None or _number(r["granted_at"], False))
        and ("ended_at" not in r or _number(r["ended_at"], False))
        and "objective" in r
        and (r["objective"] is None or isinstance(r["objective"], str))
        and "start_at" in r
        and _start(r["start_at"])
    )


def _read(directory):
    path = directory / "state.json"
    if not path.exists():
        return {"schema_version": 1, "seq": 0, "reservations": {}, "deferrals": {}}
    try:
        state = json.loads(path.read_text())
        assert type(state["schema_version"]) is int and state["schema_version"] == 1
        assert type(state["seq"]) is int and state["seq"] >= 0
        assert isinstance(state["reservations"], dict)
        assert all(
            _valid_record(r) and k == r["id"] and r["seq"] <= state["seq"] for k, r in state["reservations"].items()
        )
        seqs = [r["seq"] for r in state["reservations"].values()]
        assert len(seqs) == len(set(seqs))
        state.setdefault("deferrals", {})
        assert isinstance(state["deferrals"], dict)
        assert all(k in state["reservations"] and type(v) is int and v >= 0 for k, v in state["deferrals"].items())
        return state
    except (OSError, ValueError, KeyError, TypeError, AssertionError):
        return dict(UNREADABLE)


def _atomic(path, value, once=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".reservation-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if once:
            try:
                os.link(name, path)
            except FileExistsError:
                pass
        else:
            os.replace(name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class _Refusal(Exception):
    def __init__(self, reason):
        self.reason = reason


class ReservationStore:
    """One root-scoped authority; every operation sweeps under the same lock."""

    def __init__(self, root=None, *, clock=time.time, lock_timeout=10.0):
        self.root = resolve_root(root)
        self.directory = self.root / ".host-reservations"
        self.clock = clock
        self.lock_timeout = lock_timeout

    @contextmanager
    def _locked(self):
        if not _number(self.lock_timeout, False):
            raise _Refusal("invalid_request:lock_timeout")
        self.directory.mkdir(parents=True, exist_ok=True)
        with (self.directory / "lock").open("a") as lock:
            deadline = time.monotonic() + self.lock_timeout
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise _Refusal("lock_timeout") from None
                    time.sleep(min(0.05, remaining))
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _window_until(self, state):
        path = self.directory / "config.json"
        config = json.loads(path.read_text()) if path.exists() else {}
        if not isinstance(config, dict):
            raise ValueError("invalid config")
        return lane_window_until(state["reservations"].values(), state["deferrals"], config)

    def _receipt(self, state, r):
        path = self.directory / "receipts" / (r["id"] + ".json")
        _atomic(path, r | {"deferrals": state["deferrals"].get(r["id"], 0)}, once=True)
        return json.loads(path.read_text())

    def _operation(self, action, now=None):
        try:
            with self._locked():
                state = _read(self.directory)
                if "refusal" in state:
                    return state
                now = self.clock() if now is None else now
                if not _number(now, False):
                    return {"refusal": "invalid_request:now"}
                for r in state["reservations"].values():
                    # A receipt committed before a crashed state write is authoritative.
                    receipt = self.directory / "receipts" / (r["id"] + ".json")
                    if receipt.exists():
                        saved = json.loads(receipt.read_text())
                        if not _valid_record(saved) or saved["id"] != r["id"] or saved["state"] not in TERMINAL:
                            return dict(UNREADABLE)
                        r.update(saved)
                    if r["state"] in LIVE:
                        if now >= r["expires_at"]:
                            r["state"] = "reservation_expired"
                        elif r["state"] == "pending" and now >= r["drain_deadline"]:
                            r["state"] = "drain_timeout"
                        if r["state"] in TERMINAL:
                            r["ended_at"] = now
                            self._receipt(state, r)
                result = action(state, now)
                _atomic(self.directory / "state.json", state)
                return result
        except _Refusal as exc:
            return {"refusal": exc.reason}
        except (OSError, ValueError, TypeError, KeyError):
            return dict(UNREADABLE)

    def reserve(
        self, *, owner, purpose, cpu, memory_bytes, exclusive, ttl_s, max_drain_s, objective=None, start_at=None
    ):
        def action(state, now):
            values = locals_request
            for field in ("owner", "purpose"):
                if not isinstance(values[field], str) or not values[field].strip():
                    return {"refusal": "invalid_request:" + field}
            for field in ("cpu", "ttl_s", "max_drain_s"):
                if not _number(values[field]):
                    return {"refusal": "invalid_request:" + field}
            for field, valid in (
                ("memory_bytes", type(memory_bytes) is int and memory_bytes > 0),
                ("exclusive", type(exclusive) is bool),
                ("objective", objective is None or isinstance(objective, str)),
                ("start_at", _start(start_at)),
            ):
                if not valid:
                    return {"refusal": "invalid_request:" + field}
            try:
                hold = json.loads((self.directory / "config.json").read_text())["max_hold_s"]
                if type(hold) is not int or hold <= 0:
                    raise ValueError
            except (OSError, ValueError, KeyError, TypeError):
                return {"refusal": "max_hold_unconfigured"}
            start_epoch = _start_epoch(start_at)
            if start_epoch is not None and start_epoch >= now + hold:
                return {"refusal": "invalid_request:start_at_beyond_hold"}
            for r in sorted(state["reservations"].values(), key=lambda r: r["seq"]):
                if exclusive and r["exclusive"] and r["state"] in LIVE:
                    return {"refusal": "reservation_conflict", "reservation_id": r["id"]}
            state["seq"] += 1
            r = {
                k: values[k]
                for k in (
                    "owner",
                    "purpose",
                    "cpu",
                    "memory_bytes",
                    "exclusive",
                    "ttl_s",
                    "max_drain_s",
                    "objective",
                    "start_at",
                )
            }
            r.update(
                id=uuid.uuid4().hex,
                created_at=now,
                renewed_at=now,
                expires_at=min(now + ttl_s, now + hold),
                drain_deadline=max(now, start_epoch) + max_drain_s if start_epoch is not None else now + max_drain_s,
                state="pending",
                seq=state["seq"],
                granted_at=None,
                max_hold_s=hold,
            )
            state["reservations"][r["id"]] = r
            return dict(r)

        locals_request = locals().copy()
        return self._operation(action)

    def _owned(self, state, reservation_id, owner):
        if not isinstance(reservation_id, str):
            return {"refusal": "invalid_request:id"}
        r = state["reservations"].get(reservation_id)
        if r is None:
            return {"refusal": "not_found"}
        if owner != r["owner"]:
            return {"refusal": "not_owner"}
        return r

    def renew(self, reservation_id, owner, ttl_s):
        def action(state, now):
            r = self._owned(state, reservation_id, owner)
            if "refusal" in r:
                return r
            if r["state"] not in LIVE:
                return {"refusal": "not_live:" + r["state"]}
            if not _number(ttl_s):
                return {"refusal": "invalid_request:ttl_s"}
            r.update(ttl_s=ttl_s, renewed_at=now, expires_at=min(now + ttl_s, r["created_at"] + r["max_hold_s"]))
            return dict(r)

        return self._operation(action)

    def release(self, reservation_id, owner):
        def action(state, now):
            r = self._owned(state, reservation_id, owner)
            if "refusal" in r:
                return r
            if r["state"] == "released":
                return self._receipt(state, r)
            if r["state"] not in LIVE:
                return {"refusal": "not_live:" + r["state"]}
            r.update(state="released", ended_at=now)
            return self._receipt(state, r)

        return self._operation(action)

    def grant(self, reservation_id, now=None, running=(), gauges=None):
        """running is a sequence of lane cap dictionaries; gauges are total caps."""

        def action(state, timestamp):
            if not isinstance(reservation_id, str):
                return {"refusal": "invalid_request:id"}
            r = state["reservations"].get(reservation_id)
            if r is None:
                return {"refusal": "not_found"}
            if r["state"] == "granted":
                return dict(r)
            if r["state"] != "pending":
                return {"refusal": "not_live:" + r["state"]}
            until = self._window_until(state) if r["exclusive"] else None
            if until is not None and timestamp < until:
                return {"refusal": "lane_window", "until": until}
            start_epoch = _start_epoch(r["start_at"])
            if start_epoch is not None and timestamp < start_epoch:
                return {"refusal": "not_started"}
            earlier = [x for x in state["reservations"].values() if x["state"] in LIVE and x["seq"] < r["seq"]]
            if any(x["exclusive"] for x in earlier) or (r["exclusive"] and earlier):
                return {"refusal": "draining"}
            if r["exclusive"]:
                if running:
                    return {"refusal": "draining"}
            else:
                reason = _capacity([*running, *earlier, r], gauges or {})
                if reason:
                    return {"refusal": reason}
            r.update(state="granted", granted_at=timestamp)
            return dict(r)

        return self._operation(action, now)

    def status(self):
        return self._operation(
            lambda state, now: {
                "reservations": sorted(
                    (dict(r) for r in state["reservations"].values() if r["state"] in LIVE), key=lambda r: r["seq"]
                ),
                "seq": state["seq"],
                "lane_window_until": self._window_until(state),
                "deferrals": dict(state["deferrals"]),
                "terminal_count": sum(r["state"] in TERMINAL for r in state["reservations"].values()),
            }
        )


class _AdmissionReservations(list):
    """Live records carrying the store's single computed readiness notice."""

    def __init__(self, records, until):
        super().__init__(records)
        self.lane_window_until = until


def load_view(root=None, *, clock=time.time, lock_timeout=10.0):
    """Sweep and return a live list, or a fail-closed unreadable sentinel."""
    result = ReservationStore(root, clock=clock, lock_timeout=lock_timeout).status()
    if "reservations" not in result:
        return dict(UNREADABLE)
    return _AdmissionReservations(result["reservations"], result["lane_window_until"])


def _capacity(items, gauges):
    for key in CAPS:
        if not _number(gauges.get(key), False):
            return "unknown_gauge:" + key
        if any(not isinstance(item, dict) or not _number(item.get(key), False) for item in items):
            return "unknown_gauge:" + key
    for key in CAPS:
        if sum(item[key] for item in items) > gauges[key]:
            return "capacity:" + key
    return None


def _defer(r, reason):
    return {
        "admit": False,
        "gauge": "host_reservation",
        "reservation_id": r.get("id", "unknown"),
        "owner": r.get("owner", "unknown"),
        "expires_at": r.get("expires_at", "unknown"),
        "reason": reason,
    }


def admission_view(reservations, now, candidate, running_caps, gauges):
    """Pure FIFO drain/capacity decision; no filesystem or clock access."""
    try:
        valid = isinstance(reservations, list) and all(_valid_record(r) for r in reservations)
    except (TypeError, ValueError):
        valid = False
    if not valid or not _number(now, False):
        return _defer({}, "store_unreadable")
    live = sorted(
        (
            r
            for r in reservations
            if r["state"] in LIVE and now < r["expires_at"] and (r["state"] != "pending" or now < r["drain_deadline"])
        ),
        key=lambda r: r["seq"],
    )
    advance_fits = set()
    for r in live:
        if r["exclusive"] and r["state"] == "granted":
            return _defer(r, "exclusive")
        if r["state"] == "pending":
            seq = candidate.get("enqueued_seq")
            queued = candidate.get("enqueued_at")
            earlier = (
                (_number(seq, False) and seq <= r["seq"])
                if seq is not None
                else (_number(queued, False) and queued <= r["created_at"])
            )
            if not earlier:
                start_epoch = _start_epoch(r["start_at"])
                until = getattr(reservations, "lane_window_until", None) if r["exclusive"] else None
                if until is not None:
                    start_epoch = max(start_epoch, until) if start_epoch is not None else until
                if start_epoch is not None and now < start_epoch:
                    runtime = candidate.get("expected_runtime_s")
                    if not _number(runtime):
                        return _defer(r, "runtime_unmeasured")
                    if now + runtime > start_epoch:
                        return _defer(r, "advance_notice")
                    advance_fits.add(r["id"])
                else:
                    return _defer(r, "draining")
    reserved = [r for r in live if not r["exclusive"] and r["id"] not in advance_fits]
    if reserved:
        reason = _capacity([running_caps, *reserved, candidate], gauges)
        if reason:
            return _defer(reserved[0], reason)
    return {"admit": True}


def format_refusal(view):
    return (
        "admission_deferred gauge=host_reservation "
        f"reservation_id={view['reservation_id']} owner={view['owner']} expires_at={view['expires_at']}"
    )


def record_deferral(root, reservation_id, *, clock=time.time, lock_timeout=10.0):
    store = ReservationStore(root, clock=clock, lock_timeout=lock_timeout)

    def action(state, now):
        if not isinstance(reservation_id, str):
            return {"refusal": "invalid_request:id"}
        r = state["reservations"].get(reservation_id)
        if r is None:
            return {"refusal": "not_found"}
        if r["state"] not in LIVE:
            return {"refusal": "not_live:" + r["state"]}
        count = state["deferrals"].get(reservation_id, 0) + 1
        state["deferrals"][reservation_id] = count
        return {"reservation_id": reservation_id, "deferrals": count}

    return store._operation(action)


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise _Refusal("invalid_request:arguments")


def _wait(store, reservation_id, owner, timeout_s, poll_s, sleep, clock):
    if not _number(timeout_s, False) or not _number(poll_s):
        return {"refusal": "invalid_request:wait"}, 3
    deadline = clock() + timeout_s
    while True:
        view = store.status()
        if "refusal" in view:
            return view, 75 if view["refusal"] == "lock_timeout" else 3

        def lookup(state, now):
            r = store._owned(state, reservation_id, owner)
            if "refusal" in r:
                return r
            return store._receipt(state, r) if r["state"] in TERMINAL else dict(r)

        result = store._operation(lookup)
        if "refusal" in result:
            return result, 3
        if result["state"] == "granted":
            return result, 0
        if result["state"] in TERMINAL:
            return result, 3
        remaining = deadline - clock()
        if remaining <= 0:
            return {"refusal": "wait_timeout", "reservation_id": reservation_id}, 75
        sleep(min(poll_s, remaining))


def main(argv=None, *, clock=None, sleep=time.sleep):
    try:
        parser = _Parser(description=__doc__)
        parser.add_argument("--root")
        parser.add_argument("--lock-timeout", type=float, default=10.0)
        parser.add_argument("--now", type=float, help=argparse.SUPPRESS)
        commands = parser.add_subparsers(dest="command", required=True, parser_class=_Parser)
        reserve = commands.add_parser("reserve")
        for name in ("owner", "purpose"):
            reserve.add_argument("--" + name, required=True)
        for name in ("cpu", "ttl-s", "max-drain-s"):
            reserve.add_argument("--" + name, type=float, required=True)
        reserve.add_argument("--memory-bytes", type=int, required=True)
        reserve.add_argument("--exclusive", action="store_true")
        reserve.add_argument("--objective")
        reserve.add_argument("--start-at")
        for command in ("renew", "release"):
            sub = commands.add_parser(command)
            sub.add_argument("id")
            sub.add_argument("--owner", required=True)
            if command == "renew":
                sub.add_argument("--ttl-s", type=float, required=True)
        wait = commands.add_parser("wait")
        wait.add_argument("id")
        wait.add_argument("--owner", required=True)
        wait.add_argument("--timeout-s", type=float, required=True)
        wait.add_argument("--poll-s", type=float, default=1.0)
        commands.add_parser("status")
        args = vars(parser.parse_args(argv))
        root, timeout, now, command = (args.pop(k) for k in ("root", "lock_timeout", "now", "command"))
        store = ReservationStore(root, clock=(clock or time.time) if now is None else lambda: now, lock_timeout=timeout)
        if "id" in args:
            args["reservation_id"] = args.pop("id")
        if command == "wait":
            result, code = _wait(store, sleep=sleep, clock=clock or time.monotonic, **args)
        else:
            result = getattr(store, command)(**args)
            code = 75 if result.get("refusal") == "lock_timeout" else 3 if "refusal" in result else 0
    except _Refusal as exc:
        result, code = {"refusal": exc.reason}, 3
    except Exception as exc:
        result, code = {"error": type(exc).__name__}, 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return code


if __name__ == "__main__":
    sys.exit(main())
