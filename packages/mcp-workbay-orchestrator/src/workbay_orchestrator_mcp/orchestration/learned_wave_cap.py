"""Host-wide learned concurrency cap for remote dispatch waves."""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Mapping, MutableMapping

logger = logging.getLogger(__name__)

DEFAULT_LEARNED_CAP_PATH = Path.home() / ".workbay" / "learned-wave-cap.json"
_SEED_WAVE_WIDTH = 12
_CAPACITY_DEFER_REASONS = frozenset({"vm_lane_cap", "vm_memory_pressure"})
CLEAN_WAVES_TO_CLIMB = 3
_reset_env_consumed = False
_reset_env_lock = threading.Lock()
FLOCK_DEADLINE_S = 2.0
FLOCK_RETRY_S = 0.01


class LockDeadlineExceeded(RuntimeError):
    """Raised when a durable state update cannot acquire its file lock."""

    def __init__(self, lock_path: Path | str) -> None:
        raw = str(lock_path)
        prefix = "timed out acquiring state lock: "
        self.lock_path = Path(raw.removeprefix(prefix))
        super().__init__(f"{prefix}{self.lock_path}")


@dataclass(frozen=True, slots=True)
class LearnedCapUpdate:
    previous: int
    current: int
    defer_reason: str
    changed: bool


def resolve_learned_cap_path(
    path: Path | str | None = None,
    *,
    environ: Mapping[str, str],
) -> Path:
    """Resolve an explicit, environment-provided, or host-wide cap path."""
    if path is not None:
        return Path(path)
    configured = str(environ.get("WORKBAY_LEARNED_WAVE_CAP_PATH") or "").strip()
    return Path(configured) if configured else DEFAULT_LEARNED_CAP_PATH


def _read_state(path: Path) -> tuple[int | None, int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        learned = payload.get("learned") if isinstance(payload, dict) else None
        if isinstance(learned, bool) or not isinstance(learned, int) or learned < 1:
            return None, 0
        clean_waves = payload.get("clean_waves", 0)
        if isinstance(clean_waves, bool) or not isinstance(clean_waves, int) or clean_waves < 0:
            clean_waves = 0
        return learned, clean_waves
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return None, 0


def _read_path(path: Path) -> int | None:
    return _read_state(path)[0]


def read_learned_cap(*, path: Path | str | None = None) -> int | None:
    """Read a valid learned cap; missing or torn state is treated as absent."""
    resolved = resolve_learned_cap_path(path, environ=os.environ)
    return _read_path(resolved)


@contextlib.contextmanager
def bounded_file_lock(path: Path, *, deadline_s: float = FLOCK_DEADLINE_S) -> Iterator[None]:
    """Take a stable sibling flock, refusing after a bounded deadline."""
    data_path = Path(path)
    lock_path = data_path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        deadline = time.monotonic() + deadline_s
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise LockDeadlineExceeded(lock_path) from exc
                time.sleep(FLOCK_RETRY_S)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def learned_cap_lock(path: Path) -> Iterator[None]:
    """Take the bounded stable sibling lock used to serialize cap updates."""
    with bounded_file_lock(path):
        yield


def _store(path: Path, learned: int, *, clean_waves: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "learned": learned,
        "clean_waves": clean_waves,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(body, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def record_capacity_deferral(
    defer_reason: str,
    *,
    path: Path | str | None = None,
    environ: Mapping[str, str] = os.environ,
) -> LearnedCapUpdate:
    """Ratchet the learned cap once for a capacity-related deferral."""
    reason = str(defer_reason or "")
    resolved = resolve_learned_cap_path(path, environ=environ)
    with learned_cap_lock(resolved):
        previous, clean_waves = _read_state(resolved)
        previous = previous or _SEED_WAVE_WIDTH
        current = max(1, previous - 1) if reason in _CAPACITY_DEFER_REASONS else previous
        changed = current != previous
        if reason in _CAPACITY_DEFER_REASONS:
            _store(resolved, current, clean_waves=0)
        if changed:
            logger.info("learned wave cap %s -> %s after %s", previous, current, reason)
    return LearnedCapUpdate(previous, current, reason, changed)


def record_clean_wave(
    *,
    admitted_width: int,
    effective_cap: int,
    maximum_cap: int,
    path: Path | str | None = None,
    environ: Mapping[str, str] = os.environ,
) -> int:
    """Climb by one after consecutive full-width waves without capacity deferrals."""
    resolved = resolve_learned_cap_path(path, environ=environ)
    with learned_cap_lock(resolved):
        learned, clean_waves = _read_state(resolved)
        learned = learned or _SEED_WAVE_WIDTH
        if admitted_width != effective_cap or effective_cap < 1 or learned >= maximum_cap:
            next_streak = 0
            current = learned
        else:
            next_streak = clean_waves + 1
            current = min(maximum_cap, learned + 1) if next_streak >= CLEAN_WAVES_TO_CLIMB else learned
            if current != learned:
                logger.info("learned wave cap %s -> %s after %s clean full-width waves", learned, current, next_streak)
                next_streak = 0
        _store(resolved, current, clean_waves=next_streak)
        return current


def reset_learned_cap(*, path: Path | str | None = None) -> int:
    """Atomically restore the fixed seed while preserving the lock inode."""
    resolved = resolve_learned_cap_path(path, environ=os.environ)
    with learned_cap_lock(resolved):
        previous = _read_path(resolved)
        _store(resolved, _SEED_WAVE_WIDTH, clean_waves=0)
        logger.info("learned wave cap reset %s -> %s", previous, _SEED_WAVE_WIDTH)
    return _SEED_WAVE_WIDTH


def consume_reset_env(
    environ: MutableMapping[str, str] | Mapping[str, str],
    *,
    path: Path | str | None = None,
) -> bool:
    """Consume the reset request at most once during this process lifetime."""
    global _reset_env_consumed
    if str(environ.get("WORKBAY_LEARNED_WAVE_CAP_RESET") or "").strip() != "1":
        return False
    with _reset_env_lock:
        if _reset_env_consumed:
            return False
        _reset_env_consumed = True
    resolved = resolve_learned_cap_path(path, environ=environ)
    reset_learned_cap(path=resolved)
    return True
