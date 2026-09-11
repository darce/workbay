"""Stdlib host-memory mini-probe for the doctor ``host_memory`` facet (internal S4).

``workbay-system`` must NOT import ``workbay_orchestrator_mcp``, so this
duplicates the orchestrator's D1 probe + admission constants in ~stdlib. The
duplication is deliberate and bounded, mirroring how the doctor's other facets
read git/filesystem directly instead of routing through the MCP. Everything
degrades gracefully: an unreadable host yields ``available=False`` and the
facet stays informational.
"""

from __future__ import annotations

import glob
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

_GIB = 1024**3
_MIB = 1024**2

# Enforce defaults mirrored from the orchestrator contract (D3/Contract block).
_OS_RESERVE_GIB = 3.0
_RSS_PER_HEAVY_GIB = 2.5
_MAX_WIDTH = 4
_SWAP_FREE_FLOOR_MB = 512
_SWAP_VOLUME_DISK_FLOOR_GIB = 8.0


@dataclass(frozen=True)
class HostMemorySnapshot:
    available: bool
    platform: str
    available_ram: int
    pressure: str
    swap_free: int
    swap_total: int
    swap_volume_free_bytes: int
    swapfile_count: int
    note: str = ""


def _run(cmd: list[str]) -> str:
    return subprocess.run(  # noqa: S603 -- fixed argv
        cmd, capture_output=True, text=True, timeout=10, check=True
    ).stdout


def _sysctl(name: str) -> str:
    return _run(["/usr/sbin/sysctl", "-n", name]).strip()


def _probe_darwin() -> HostMemorySnapshot:
    page_size = int(_sysctl("hw.pagesize"))
    wanted = {"Pages free": 0, "Pages inactive": 0, "Pages purgeable": 0}
    for line in _run(["/usr/bin/vm_stat"]).splitlines():
        key, _, value = line.partition(":")
        if key.strip() in wanted:
            digits = value.strip().rstrip(".")
            if digits.isdigit():
                wanted[key.strip()] = int(digits)
    available = sum(wanted.values()) * page_size
    try:
        level = _sysctl("kern.memorystatus_vm_pressure_level")
        pressure = {"1": "normal", "2": "warn", "4": "critical"}.get(level, "unknown")
    except (subprocess.SubprocessError, OSError):
        pressure = "unknown"
    swap_total = swap_free = 0
    try:
        swaptext = _sysctl("vm.swapusage")

        def _grab(token: str) -> int:
            parts = swaptext.split(f"{token} =")
            if len(parts) < 2:
                return 0
            raw = parts[1].strip().split()[0]
            unit = raw[-1] if raw and raw[-1] in "KMGT" else ""
            try:
                number = float(raw.rstrip("KMGT"))
            except ValueError:
                return 0
            return int(number * {"K": 1024, "M": _MIB, "G": _GIB, "T": 1024**4}.get(unit, 1))

        swap_total, swap_free = _grab("total"), _grab("free")
    except (subprocess.SubprocessError, OSError):
        pass
    try:
        prefix = _sysctl("vm.swapfileprefix")
    except (subprocess.SubprocessError, OSError):
        prefix = "/private/var/vm/swapfile"
    swap_dir = os.path.dirname(prefix) or "/private/var/vm"
    try:
        st = os.statvfs(swap_dir)
        volume_free = st.f_bavail * st.f_frsize
    except OSError:
        volume_free = 0
    swapfile_count = len(glob.glob(prefix + "*"))
    return HostMemorySnapshot(
        available=True,
        platform="darwin",
        available_ram=available,
        pressure=pressure,
        swap_free=swap_free,
        swap_total=swap_total,
        swap_volume_free_bytes=volume_free,
        swapfile_count=swapfile_count,
    )


def _probe_linux() -> HostMemorySnapshot:
    meminfo: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition(":")
        fields = value.split()
        if fields and fields[0].isdigit():
            meminfo[key.strip()] = int(fields[0]) * 1024
    pressure = "unknown"
    try:
        some = full = None
        for line in Path("/proc/pressure/memory").read_text(encoding="utf-8").splitlines():
            fields = line.split()
            if not fields:
                continue
            for tok in fields[1:]:
                if tok.startswith("avg10="):
                    try:
                        val = float(tok.split("=", 1)[1])
                    except ValueError:
                        continue
                    if fields[0] == "some":
                        some = val
                    elif fields[0] == "full":
                        full = val
        if some is not None or full is not None:
            if (full or 0.0) >= 10.0 or (some or 0.0) >= 60.0:
                pressure = "critical"
            elif (some or 0.0) >= 20.0:
                pressure = "warn"
            else:
                pressure = "normal"
    except OSError:
        pressure = "unknown"
    swap_total = meminfo.get("SwapTotal", 0)
    swap_free = meminfo.get("SwapFree", 0)
    swap_path = None
    swapfile_count = 0
    try:
        for line in Path("/proc/swaps").read_text(encoding="utf-8").splitlines()[1:]:
            fields = line.split()
            if len(fields) < 2:
                continue
            if swap_path is None:
                swap_path = fields[0]
            if fields[1] == "file":
                swapfile_count += 1
    except OSError:
        pass
    volume_free = 0
    if swap_path:
        try:
            st = os.statvfs(os.path.dirname(swap_path) or "/")
            volume_free = st.f_bavail * st.f_frsize
        except OSError:
            volume_free = 0
    return HostMemorySnapshot(
        available=True,
        platform="linux",
        available_ram=meminfo.get("MemAvailable", 0),
        pressure=pressure,
        swap_free=swap_free,
        swap_total=swap_total,
        swap_volume_free_bytes=volume_free,
        swapfile_count=swapfile_count,
    )


def probe_host_memory() -> HostMemorySnapshot:
    """Never raises; unknown platform / read failure yields ``available=False``."""
    try:
        if sys.platform == "darwin":
            return _probe_darwin()
        if sys.platform.startswith("linux"):
            return _probe_linux()
        return HostMemorySnapshot(
            available=False,
            platform=sys.platform,
            available_ram=0,
            pressure="unknown",
            swap_free=0,
            swap_total=0,
            swap_volume_free_bytes=0,
            swapfile_count=0,
            note="unsupported platform",
        )
    except Exception as exc:  # noqa: BLE001 -- the facet must never crash `make doctor`
        return HostMemorySnapshot(
            available=False,
            platform=sys.platform,
            available_ram=0,
            pressure="unknown",
            swap_free=0,
            swap_total=0,
            swap_volume_free_bytes=0,
            swapfile_count=0,
            note=f"probe error: {type(exc).__name__}",
        )


def derive_width(available_ram: int) -> int:
    """D3 formula with the enforce defaults."""
    usable = available_ram - int(_OS_RESERVE_GIB * _GIB)
    if usable <= 0:
        return 0
    return max(0, min(int(usable // (_RSS_PER_HEAVY_GIB * _GIB)), _MAX_WIDTH))


def count_held_heavy_slots(locks_root: Path, width: int) -> int:
    """Glob + non-blocking flock-test ``slot-N`` files under ``<locks_root>/admission``."""
    import fcntl

    held = 0
    slot_dir = locks_root / "admission"
    for n in range(max(0, width)):
        slot = slot_dir / f"slot-{n}"
        if not slot.exists():
            continue
        try:
            fd = os.open(slot, os.O_RDWR)
        except OSError:
            continue
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                held += 1
        finally:
            os.close(fd)
    return held


def would_refuse_heavy(snap: HostMemorySnapshot, width: int) -> bool:
    """Mirror the D2 refuse dimensions (pressure / swap floor / disk floor / width 0)."""
    if not snap.available:
        return False
    if snap.pressure == "critical":
        return True
    if snap.swap_total > 0 and snap.swap_free < _SWAP_FREE_FLOOR_MB * _MIB:
        return True
    if 0 < snap.swap_volume_free_bytes < _SWAP_VOLUME_DISK_FLOOR_GIB * _GIB:
        return True
    return width == 0
