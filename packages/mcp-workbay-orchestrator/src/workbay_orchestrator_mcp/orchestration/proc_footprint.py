"""Own-pid physical-memory footprint measurement (internal S2, D6).

D6: measure ``phys_footprint``, NOT RSS / ``ru_maxrss``. RSS under-reports macOS
compressed pages, so an RSS threshold can read healthy while the host OOMs (the
motivating evidence was 8712 MB phys_footprint, "mostly compressed"). This module reads
the *current* process's true footprint:

- **macOS**: libproc ``proc_pid_rusage(getpid(), RUSAGE_INFO_V2, &info)`` ->
  ``ri_phys_footprint`` (bytes) — the quantity Activity Monitor reports as "Memory".
- **Linux**: ``VmRSS`` + ``VmSwap`` from ``/proc/self/status`` (kB). **Platform margin:**
  this is resident+swapped, an *approximation* of macOS phys_footprint — it does not model
  compressor accounting, so absolute cross-platform comparisons carry that stated caveat.

**Unit:** all public values are **decimal MB (10^6 bytes)**, matching the macOS ``footprint``
tool / Activity Monitor "Memory" column that produced the plan's motivating "8712 MB"
evidence — so an operator's sanity-check against those tools reconciles (PROV-06). It is NOT
mebibytes; the ``_mb`` suffix is honest decimal MB.

Everything is fail-open: any binding / syscall / parse error yields ``None`` — measurement
must never raise into a prompt-render path ([RES-13]). This module deliberately imports
NOTHING from the admission gate (``host_resources.evaluate_admission``): the per-lane-prep
footprint signal is observability, never backpressure — ``COST_REMOTE`` stays ungated
(D4). [OBS-05][DIAG-08][PERF-02]
"""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

# Decimal MB (10^6 bytes), matching macOS `footprint` / Activity Monitor — see module
# docstring. /proc reports VmRSS/VmSwap in kibibytes (1024-byte units).
_BYTES_PER_MB = 1_000_000
_BYTES_PER_KIB = 1024

# ``RUSAGE_INFO_V2`` flavor for ``proc_pid_rusage`` (macOS <sys/resource.h>).
_RUSAGE_INFO_V2 = 2


class RusageInfoV2(ctypes.Structure):
    """``struct rusage_info_v2`` (macOS <sys/resource.h>).

    Field ORDER is load-bearing: ``ri_phys_footprint`` sits at byte offset 72 — the
    8th ``uint64`` after the 16-byte ``ri_uuid`` — verified empirically on Apple silicon
    against the adjacent ``ri_proc_start_abstime``. Selecting ``ri_resident_size`` (offset
    64) instead would return RSS, the value D6 rejects.
    """

    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
        ("ri_child_user_time", ctypes.c_uint64),
        ("ri_child_system_time", ctypes.c_uint64),
        ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_child_interrupt_wkups", ctypes.c_uint64),
        ("ri_child_pageins", ctypes.c_uint64),
        ("ri_child_elapsed_abstime", ctypes.c_uint64),
        ("ri_diskio_bytesread", ctypes.c_uint64),
        ("ri_diskio_byteswritten", ctypes.c_uint64),
    ]


def _phys_footprint_mb_from_rusage_v2(info: RusageInfoV2) -> float:
    """Pure field selection: phys_footprint bytes -> decimal MB. Isolated so the D6
    "not ru_maxrss/RSS" invariant is unit-testable without a live syscall."""
    return info.ri_phys_footprint / _BYTES_PER_MB


def _load_libproc() -> ctypes.CDLL:
    try:
        return ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
    except OSError:
        # libSystem is linked into every process; the null handle exposes its symbols.
        return ctypes.CDLL(None, use_errno=True)


def _darwin_phys_footprint_mb() -> float | None:
    libc = _load_libproc()
    proc_pid_rusage = libc.proc_pid_rusage
    proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    proc_pid_rusage.restype = ctypes.c_int
    info = RusageInfoV2()
    rc = proc_pid_rusage(os.getpid(), _RUSAGE_INFO_V2, ctypes.byref(info))
    if rc != 0:
        return None
    return _phys_footprint_mb_from_rusage_v2(info)


def _parse_proc_status_footprint_mb(status_text: str) -> float | None:
    """Pure parse of ``/proc/self/status`` text -> decimal MB (VmRSS+VmSwap), or ``None``
    when VmRSS is absent. Isolated from the file read so the kB(kiB)->MB conversion and the
    missing-VmRSS fail-open are hermetically testable on any platform (TEST-04)."""
    vmrss_kb: int | None = None
    vmswap_kb = 0
    for line in status_text.splitlines():
        if line.startswith("VmRSS:"):
            vmrss_kb = int(line.split()[1])
        elif line.startswith("VmSwap:"):
            vmswap_kb = int(line.split()[1])
    if vmrss_kb is None:
        return None
    return (vmrss_kb + vmswap_kb) * _BYTES_PER_KIB / _BYTES_PER_MB


def _linux_phys_footprint_mb() -> float | None:
    return _parse_proc_status_footprint_mb(Path("/proc/self/status").read_text())


def _platform_phys_footprint_mb() -> float | None:
    if sys.platform == "darwin":
        return _darwin_phys_footprint_mb()
    if sys.platform.startswith("linux"):
        return _linux_phys_footprint_mb()
    return None


def current_phys_footprint_mb() -> float | None:
    """Current-process phys_footprint in decimal MB (10^6 bytes), or ``None`` if unmeasurable.

    Fail-open: never raises. [RES-13]
    """
    try:
        return _platform_phys_footprint_mb()
    except Exception:  # noqa: BLE001 - measurement must never raise into the caller
        return None


def phys_footprint_source() -> str:
    """Auditable label for the measurement mechanism (PROV-06 / OBS). Never a maxrss
    source — a reviewer can confirm from the row alone that D6 was honored."""
    if sys.platform == "darwin":
        return "darwin_libproc_ri_phys_footprint"
    if sys.platform.startswith("linux"):
        return "linux_proc_status_vmrss_plus_vmswap"
    return "unsupported"
