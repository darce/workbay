"""Host resource probe + memory-admission policy (internal D1).

Call-time, cached (~5 s) view of host memory state feeding the admission
gate and elastic width derivation. Design constraints:

- ``probe_host`` never raises: any backend failure lands in
  ``HostResources.probe_error`` and admission treats the snapshot as
  pressure ``warn`` (degraded, width 1) — a broken probe can neither brick
  a healthy host nor silently disable the gate.
- Parsers are pure text -> value functions so the suite fakes their inputs
  (no real ``sysctl``/``/proc`` reads in tests).
- Policy is fail-closed: an absent or malformed ``orchestrator.host_memory``
  contract block yields the built-in *enforce* defaults; only an explicit
  ``enforcement: off`` disables the gate.
"""

from __future__ import annotations

import glob
import math
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Collection, Iterator, Mapping

from workbay_protocol import HARNESS_CONTRACT_RELPATH

from .worktree_stock import WorktreeStock

__all__ = [
    "AdmissionDecision",
    "HostMemoryPolicy",
    "HostResources",
    "StockSlotClaim",
    "StockSlotFailure",
    "SuiteLockTimeout",
    "acquire_backend_local_slot",
    "acquire_heavy_slot",
    "acquire_stock_slot",
    "acquire_suite_bulkhead",
    "acquire_suite_lock",
    "clear_crash_breaker",
    "count_held_backend_slots",
    "count_held_heavy_slots",
    "crash_breaker_width_cap",
    "derive_stock_lane_kind",
    "derive_width",
    "evaluate_admission",
    "evaluate_admission_claiming_stock",
    "format_admission_gate_error",
    "load_host_memory_policy",
    "locks_root",
    "probe_host",
    "record_admission_telemetry",
    "resolve_live_admission",
]

# Process-local heavy-slot ownership registry (implementation note S3 / rev5-b-01).
# flock re-probes cannot distinguish a self-held slot from a foreign one (a
# fresh LOCK_EX|NB open fails for both), so exclude_slots is honoured only when
# this process's registry confirms ownership of the idx.
_HEAVY_SLOT_REGISTRY_LOCK = threading.Lock()
_HEAVY_SLOT_REGISTRY: dict[int, int] = {}  # slot_idx → held fd
# Per-backend local slot held alongside a class-wide heavy slot (internal).
# Keyed by class slot_idx so _release_heavy_slot can free both. The backend fd
# stays open for the process lifetime — kernel drops the flock on death.
_BACKEND_SLOT_FOR_CLASS: dict[int, tuple[int, int]] = {}  # class_idx → (backend_idx, fd)
# Stock flocks owned by StockSlotClaim handles returned from live admission.
# The registry supports ownership-checked release and process-local teardown.
_STOCK_SLOT_REGISTRY_LOCK = threading.Lock()
_STOCK_SLOT_REGISTRY: dict[object, int] = {}  # namespace/slot key → held fd

_GIB = 1024**3
_MIB = 1024**2
_PROBE_CACHE_TTL_S = 5.0

# Cost classes (D2). ``light`` is never gated.
COST_HEAVY = "heavy"
COST_SUITE = "suite"
COST_LIGHT = "light"
# ``remote_api`` — a LOCAL CLI worker whose LLM inference is a remote API call but
# whose agent process AND test suite run ON THIS BOX (e.g. grok-cli). Its local
# RSS is smaller than a heavy worker, so it is sized on ``rss_per_remote_api_gib``,
# but it stays GATED (present in ``_GATED_COST_CLASSES``): local test execution and
# the swap/pressure floors genuinely bear on the local host (internal-
# COSTCLASS-01 D1 / PF-1). Deliberately NOT ``COST_LIGHT``.
COST_REMOTE_API = "remote_api"
# ``remote`` — a FULLY off-box worker: agent execution AND tests run on a remote VM,
# only the commit lands locally (e.g. grok-remote / RemoteExecAdapter). Its local
# footprint is ~0 (ssh + git), and the VM enforces its OWN admission, so the local
# host-memory guard must NOT gate it — gating a remote lane on local RAM is a false
# positive that blocks useful off-box work whenever the local box is merely busy
# (internal). Never gated (absent from ``_GATED_COST_CLASSES``) —
# like ``light``, but for the opposite reason: no LOCAL footprint at all, rather
# than "too small to matter". Do NOT lump grok-cli here: its tests run locally.
COST_REMOTE = "remote"
_GATED_COST_CLASSES = (COST_HEAVY, COST_SUITE, COST_REMOTE_API)


@dataclass(frozen=True, slots=True)
class HostResources:
    """One probe snapshot. Byte quantities unless suffixed otherwise."""

    platform: str
    total_ram: int = 0
    # ``None`` means the probe produced no measurement. Zero remains a valid,
    # genuinely measured value and must still trip the admission floor.
    available_ram: int | None = None
    swap_total: int = 0
    swap_used: int = 0
    swap_free: int = 0
    pressure: str = "unknown"  # normal | warn | critical | unknown
    swap_volume_free_bytes: int = 0
    swapfile_count: int = 0
    boot_time: float = 0.0
    probed_at: float = 0.0
    probe_error: str | None = None


@dataclass(frozen=True, slots=True)
class HostMemoryPolicy:
    """``orchestrator.host_memory`` contract block with enforce defaults."""

    enforcement: str = "enforce"  # enforce | warn_only | off
    os_reserve_gib: float = 3.0
    rss_per_heavy_gib: float = 2.5
    # Local RSS of a remote-API CLI driver worker (grok-cli): inference is off-box,
    # so the box-side footprint is small. Sizes COST_REMOTE_API width (D1/PF-1).
    rss_per_remote_api_gib: float = 0.5
    # Local OS headroom kept free before a remote-API worker is sized. Its
    # inference (and the suite it drives) runs off-box, so it does not consume the
    # full heavy OS reserve; gate it on a small local floor instead
    # (internal, extends D1/PF-1). Without this, a box with
    # available RAM < os_reserve_gib refuses remote-API lanes outright despite
    # their ~0 local footprint — the exact false-positive the remote cost class
    # exists to avoid.
    os_reserve_remote_api_gib: float = 0.5
    max_width: int = 4
    # Max concurrent local lanes for any single gated backend (internal S2b).
    # Orthogonal to class width: one claude-code AND one codex-cli is fine; two of
    # the same backend is not. COST_REMOTE is never gated so this does not apply
    # to fully off-box workers.
    per_backend_local_cap: int = 1
    # Legacy/aggregate ceiling, retained as the landable default and as the
    # fallback for old unsplit snapshots. STOCK is orthogonal to every memory
    # dimension and evaluated even for COST_REMOTE: an off-box lane still
    # creates a local worktree. 0 is a size-0 bulkhead (admits nothing); it
    # does not disarm the bound. Default 16 is 4× max_width for the landable
    # pool; a None record-only ceiling copies that number, so wiring both
    # bulkheads yields a 2× disk budget (16+16).
    worktree_stock_ceiling: int = 16
    # Split ceilings.  ``None`` keeps the historical ceiling authoritative for
    # each class independently so either class can fill without consuming the
    # other's capacity.
    worktree_landable_stock_ceiling: int | None = None
    worktree_record_only_stock_ceiling: int | None = None
    # Consecutive retryable stock defers before the gate refuses with a named
    # reclaim/merge demand.  Persisted under the locks root so CLI retries
    # share the streak.
    stock_defer_limit: int = 3
    # Local checkout and pytest scratch headroom for locally executing lanes.
    # Fully off-box lanes are exempt from this local staging constraint.
    local_staging_disk_floor_gib: float = 2.0
    # Arithmetic-independent last-ditch RAM floor. The width formula normally
    # refuses first, but this named dimension remains effective if width tuning
    # changes the reserve or per-worker RSS assumptions.
    available_ram_floor_gib: float = 1.0
    swap_free_floor_mb: int = 512
    swap_volume_disk_floor_gib: float = 8.0
    slots_full_outcome: str = "defer"  # defer | refuse
    suite_lock_timeout_s: int = 1800
    warnings: tuple[str, ...] = field(default=())


# ---------------------------------------------------------------------------
# pure parsers (hermetic-test surface)
# ---------------------------------------------------------------------------


_VM_STAT_REQUIRED_COUNTERS = ("Pages free", "Pages inactive", "Pages purgeable")


def _parse_vm_stat(text: str, page_size: int) -> int | None:
    """Available RAM per D1: (free + inactive + purgeable) x page size.

    ``None`` means the probe produced NO measurement: empty output, malformed
    values, or a ``vm_stat`` schema in which any of the three required counters
    is absent. Pre-seeding the counters to 0 (the previous behaviour) collapsed
    all three of those into a fabricated ``0``, which slipped past the
    ``available_ram is None`` arm of :func:`_classify_admission` and told the
    operator "available RAM 0.0GiB below floor" — blaming RAM for a reading
    never taken.

    A genuinely parsed all-zero reading still returns ``0``, never ``None``:
    zero is a real measurement and must still trip the admission floor. Absence
    is therefore keyed on counter *presence*, never on the summed value.
    """
    wanted: dict[str, int] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        key = key.strip()
        if key in _VM_STAT_REQUIRED_COUNTERS:
            digits = value.strip().rstrip(".")
            if digits.isdigit():
                wanted[key] = int(digits)
    if len(wanted) != len(_VM_STAT_REQUIRED_COUNTERS):
        return None
    return sum(wanted.values()) * page_size


def _parse_pressure_int(value: str) -> str:
    """Kernel memorystatus levels: 1 normal, 2 warn, 4 critical."""
    mapping = {"1": "normal", "2": "warn", "4": "critical"}
    return mapping.get(value.strip(), "unknown")


def _parse_memory_pressure_fallback(text: str) -> str:
    """``memory_pressure -Q`` free-percentage bands (fallback only)."""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("System-wide memory free percentage:"):
            digits = "".join(ch for ch in line.split(":", 1)[1] if ch.isdigit())
            if digits:
                free_pct = int(digits)
                if free_pct >= 50:
                    return "normal"
                if free_pct >= 20:
                    return "warn"
                return "critical"
    return "unknown"


def _parse_swapusage(text: str) -> tuple[int, int, int]:
    """``sysctl vm.swapusage`` -> (total, used, free) bytes."""

    def _grab(token: str) -> int:
        # e.g. "total = 2048.00M" — value directly follows "<token> ="
        parts = text.split(f"{token} =")
        if len(parts) < 2:
            return 0
        raw = parts[1].strip().split()[0]
        unit = raw[-1] if raw and raw[-1] in "KMGT" else ""
        try:
            number = float(raw.rstrip("KMGT"))
        except ValueError:
            return 0
        factor = {"K": 1024, "M": 1024**2, "G": _GIB, "T": 1024**4}.get(unit, 1)
        return int(number * factor)

    return _grab("total"), _grab("used"), _grab("free")


def _parse_meminfo(text: str) -> dict[str, int]:
    """/proc/meminfo -> bytes by key (values are kB)."""
    out: dict[str, int] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        fields = value.split()
        if fields and fields[0].isdigit():
            out[key.strip()] = int(fields[0]) * 1024
    return out


def _parse_psi_memory(text: str) -> str:
    """/proc/pressure/memory -> pressure enum.

    Bands: full avg10 >= 10 or some avg10 >= 60 -> critical;
    some avg10 >= 20 -> warn; parsed but below bands -> normal.
    """
    some = full = None
    for line in text.splitlines():
        fields = line.split()
        if not fields:
            continue
        for tok in fields[1:]:
            if tok.startswith("avg10="):
                try:
                    value = float(tok.split("=", 1)[1])
                except ValueError:
                    continue
                if fields[0] == "some":
                    some = value
                elif fields[0] == "full":
                    full = value
    if some is None and full is None:
        return "unknown"
    if (full or 0.0) >= 10.0 or (some or 0.0) >= 60.0:
        return "critical"
    if (some or 0.0) >= 20.0:
        return "warn"
    return "normal"


def _parse_proc_swaps(text: str) -> tuple[str | None, int]:
    """/proc/swaps -> (first swap path, file-type entry count)."""
    first: str | None = None
    file_count = 0
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 2:
            continue
        if first is None:
            first = fields[0]
        if fields[1] == "file":
            file_count += 1
    return first, file_count


def _parse_boottime_sysctl(value: str) -> float:
    """``kern.boottime`` -> epoch seconds ("{ sec = 1720000000, usec = 0 } ...")."""
    marker = "sec ="
    idx = value.find(marker)
    if idx == -1:
        return 0.0
    digits = ""
    for ch in value[idx + len(marker) :].lstrip():
        if ch.isdigit():
            digits += ch
        else:
            break
    return float(digits) if digits else 0.0


# ---------------------------------------------------------------------------
# probe backends
# ---------------------------------------------------------------------------


def _run(cmd: list[str]) -> str:
    return subprocess.run(  # noqa: S603 -- fixed argv, no user input
        cmd, capture_output=True, text=True, timeout=10, check=True
    ).stdout


def _sysctl(name: str) -> str:
    return _run(["/usr/sbin/sysctl", "-n", name]).strip()


def _probe_darwin(now: float) -> HostResources:
    total = int(_sysctl("hw.memsize"))
    page_size = int(_sysctl("hw.pagesize"))
    # ``None`` (unmeasurable vm_stat) is carried through verbatim — never
    # defaulted to 0 — so admission refuses with its probe-unavailable reason
    # instead of fabricating a RAM number. Mirrors the Linux arm's
    # ``meminfo.get("MemAvailable")`` with no default.
    available: int | None = _parse_vm_stat(_run(["/usr/bin/vm_stat"]), page_size)
    try:
        pressure = _parse_pressure_int(_sysctl("kern.memorystatus_vm_pressure_level"))
    except (subprocess.SubprocessError, OSError, ValueError):
        try:
            pressure = _parse_memory_pressure_fallback(_run(["/usr/bin/memory_pressure", "-Q"]))
        except (subprocess.SubprocessError, OSError):
            pressure = "unknown"
    swap_total, swap_used, swap_free = _parse_swapusage(_sysctl("vm.swapusage"))
    try:
        swap_prefix = _sysctl("vm.swapfileprefix")
    except (subprocess.SubprocessError, OSError):
        swap_prefix = "/private/var/vm/swapfile"
    swap_dir = os.path.dirname(swap_prefix) or "/private/var/vm"
    try:
        stats = os.statvfs(swap_dir)
        volume_free = stats.f_bavail * stats.f_frsize
    except OSError:
        volume_free = 0
    swapfile_count = len(glob.glob(swap_prefix + "*"))
    boot_time = _parse_boottime_sysctl(_sysctl("kern.boottime"))
    return HostResources(
        platform="darwin",
        total_ram=total,
        available_ram=available,
        swap_total=swap_total,
        swap_used=swap_used,
        swap_free=swap_free,
        pressure=pressure,
        swap_volume_free_bytes=volume_free,
        swapfile_count=swapfile_count,
        boot_time=boot_time,
        probed_at=now,
    )


def _probe_linux(now: float) -> HostResources:
    meminfo = _parse_meminfo(Path("/proc/meminfo").read_text(encoding="utf-8"))
    swap_total = meminfo.get("SwapTotal", 0)
    swap_free = meminfo.get("SwapFree", 0)
    try:
        pressure = _parse_psi_memory(Path("/proc/pressure/memory").read_text(encoding="utf-8"))
    except OSError:
        pressure = "unknown"
    uptime = float(Path("/proc/uptime").read_text(encoding="utf-8").split()[0])
    boot_time = now - uptime
    try:
        swap_path, swapfile_count = _parse_proc_swaps(Path("/proc/swaps").read_text(encoding="utf-8"))
    except OSError:
        swap_path, swapfile_count = None, 0
    volume_free = 0
    if swap_path:
        try:
            stats = os.statvfs(os.path.dirname(swap_path) or "/")
            volume_free = stats.f_bavail * stats.f_frsize
        except OSError:
            volume_free = 0
    return HostResources(
        platform="linux",
        total_ram=meminfo.get("MemTotal", 0),
        available_ram=meminfo.get("MemAvailable"),
        swap_total=swap_total,
        swap_used=swap_total - swap_free,
        swap_free=swap_free,
        pressure=pressure,
        swap_volume_free_bytes=volume_free,
        swapfile_count=swapfile_count,
        boot_time=boot_time,
        probed_at=now,
    )


_cache: HostResources | None = None
_cache_at: float = 0.0


def probe_host(*, force: bool = False) -> HostResources:
    """Cached (~5 s) host snapshot. Never raises."""
    global _cache, _cache_at
    mono = time.monotonic()
    if not force and _cache is not None and (mono - _cache_at) < _PROBE_CACHE_TTL_S:
        return _cache
    now = time.time()
    try:
        if sys.platform == "darwin":
            snapshot = _probe_darwin(now)
        elif sys.platform.startswith("linux"):
            snapshot = _probe_linux(now)
        else:
            snapshot = HostResources(platform=sys.platform, probed_at=now)
    except Exception as exc:  # noqa: BLE001 -- probe must never raise
        snapshot = HostResources(
            platform=sys.platform,
            pressure="warn",
            probed_at=now,
            probe_error=f"{type(exc).__name__}: {exc}",
        )
    _cache, _cache_at = snapshot, mono
    return snapshot


# ---------------------------------------------------------------------------
# locks root
# ---------------------------------------------------------------------------


class _NotAGitWorkTree(Exception):
    """workspace_root is not a git work tree; the stock lock dimension is inert."""


def locks_root(workspace_root: Path | None = None) -> Path:
    """``<git-common-root>/.workbay/locks`` — shared across worktrees/lanes.

    Three-state, matching :func:`_collect_live_stock`:

    * inside a work tree → the shared lock namespace
    * not a repository → raises :class:`_NotAGitWorkTree` so live admission
      can treat the stock dimension as inert (hermetic / non-git callers)
    * probe failed inside a repository → raises the git probe error so live
      admission can refuse with ``stock_lock_unavailable``
    """
    cwd_path = Path(workspace_root) if workspace_root is not None else Path.cwd()
    inside = _is_inside_git_work_tree(cwd_path)
    if inside is False:
        raise _NotAGitWorkTree(str(cwd_path))
    if inside is None:
        raise RuntimeError(f"git work-tree probe failed: {cwd_path}")
    common_dir = subprocess.run(  # noqa: S603
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
        cwd=str(cwd_path),
    ).stdout.strip()
    root = Path(common_dir).parent / ".workbay" / "locks"
    root.mkdir(parents=True, exist_ok=True)
    return root


# ---------------------------------------------------------------------------
# policy loader
# ---------------------------------------------------------------------------

_POLICY_TYPES: dict[str, type] = {
    "enforcement": str,
    "os_reserve_gib": float,
    "os_reserve_remote_api_gib": float,
    "rss_per_heavy_gib": float,
    "rss_per_remote_api_gib": float,
    "max_width": int,
    "per_backend_local_cap": int,
    "worktree_stock_ceiling": int,
    "worktree_landable_stock_ceiling": int,
    "worktree_record_only_stock_ceiling": int,
    "stock_defer_limit": int,
    "local_staging_disk_floor_gib": float,
    "available_ram_floor_gib": float,
    "swap_free_floor_mb": int,
    "swap_volume_disk_floor_gib": float,
    "slots_full_outcome": str,
    "suite_lock_timeout_s": int,
}
_ENUM_FIELDS = {
    "enforcement": ("enforce", "warn_only", "off"),
    "slots_full_outcome": ("defer", "refuse"),
}
_STOCK_CEILING_KEYS = frozenset(
    {
        "worktree_stock_ceiling",
        "worktree_landable_stock_ceiling",
        "worktree_record_only_stock_ceiling",
    }
)


def _strip_comment(line: str) -> str:
    idx = line.find("#")
    return line if idx == -1 else line[:idx]


def _parse_host_memory_block(text: str) -> dict[str, str]:
    """Extract scalar ``key: value`` pairs under ``orchestrator.host_memory``.

    Deliberately NOT shared with ``api._parse_daemons_enabled``: that helper's
    absent-block semantics are fail-open; this loader's are fail-closed.
    """
    values: dict[str, str] = {}
    in_orchestrator = False
    in_block = False
    for raw_line in text.splitlines():
        stripped = _strip_comment(raw_line)
        content = stripped.strip()
        if not content:
            continue
        indent = len(stripped) - len(stripped.lstrip(" "))
        if indent == 0:
            in_orchestrator = content == "orchestrator:"
            in_block = False
            continue
        if not in_orchestrator:
            continue
        if indent == 2:
            in_block = content == "host_memory:"
            continue
        if in_block and indent == 4 and ":" in content:
            key, _, value = content.partition(":")
            values[key.strip()] = value.strip()
    return values


def _top_level_host_memory_present(text: str) -> bool:
    """True when a ``host_memory:`` key sits at indent 0 (misplaced — it must be
    nested under ``orchestrator:``).

    Deliberately mirrors ``_parse_host_memory_block``'s fail-closed indent scan
    rather than a full YAML load (internal PF-3): the loader
    is a hand-rolled scanner by design, and a real YAML parse here would diverge
    from — and could disagree with — the scanner that actually extracts values.
    """
    for raw_line in text.splitlines():
        stripped = _strip_comment(raw_line)
        content = stripped.strip()
        if not content:
            continue
        indent = len(stripped) - len(stripped.lstrip(" "))
        if indent == 0 and content == "host_memory:":
            return True
    return False


_TOP_LEVEL_HOST_MEMORY_WARNING = "host_memory: block found at top level; must be nested under 'orchestrator:'"


def load_host_memory_policy(workspace_root: Path) -> HostMemoryPolicy:
    """Contract block -> policy; absent/malformed => enforce defaults."""
    defaults = HostMemoryPolicy()
    contract_path = Path(workspace_root) / HARNESS_CONTRACT_RELPATH
    try:
        text = contract_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        # Absent OR undecodable (non-UTF-8/binary) contract => enforce defaults.
        # UnicodeDecodeError is a ValueError, NOT an OSError; without this a
        # malformed contract byte would crash every caller — including the
        # offload_preflight policy echo, which reads the contract unconditionally
        # (even under WORKBAY_HOSTGOV_DISABLE=1). The dispatch surface must never
        # crash on a bad contract file.
        return defaults
    raw = _parse_host_memory_block(text)
    top_level_misplaced = _top_level_host_memory_present(text)
    if not raw:
        # A top-level `host_memory:` block silently yielded defaults before this
        # (internal D2): the operator saw admission_refused
        # with correct-looking on-disk config and zero signal. Surface it.
        if top_level_misplaced:
            return replace(
                defaults,
                warnings=(f"{_TOP_LEVEL_HOST_MEMORY_WARNING} — ignored, using defaults",),
            )
        return defaults

    policy = defaults
    warnings: list[str] = []
    if top_level_misplaced:
        # Nested block loads below; the stray top-level copy is inert — say so.
        warnings.append(
            f"{_TOP_LEVEL_HOST_MEMORY_WARNING} — ignored; the nested orchestrator.host_memory block is authoritative"
        )
    for key, value in raw.items():
        caster = _POLICY_TYPES.get(key)
        if caster is None:
            warnings.append(f"host_memory: unknown key {key!r} ignored")
            continue
        try:
            typed = caster(value)
        except ValueError:
            warnings.append(f"host_memory: malformed {key}={value!r}; default retained")
            continue
        # ``float()`` parses the IEEE-754 specials "inf"/"nan" without error;
        # they would crash ``derive_width`` (int(inf) OverflowError, int(nan)
        # ValueError) and slip past the ``rss <= 0`` guard. Reject them here so
        # the loader stays genuinely fail-closed. Also reject non-positive
        # numerics — an inverted ``os_reserve_gib: -3`` would *add* headroom.
        if isinstance(typed, float) and (not math.isfinite(typed) or typed < 0):
            warnings.append(f"host_memory: non-finite/negative {key}={value!r}; default retained")
            continue
        if isinstance(typed, int) and not isinstance(typed, bool) and typed < 0:
            warnings.append(f"host_memory: negative {key}={value!r}; default retained")
            continue
        allowed = _ENUM_FIELDS.get(key)
        if allowed and typed not in allowed:
            warnings.append(f"host_memory: {key}={typed!r} not in {allowed}; default retained")
            continue
        if (
            key in _STOCK_CEILING_KEYS
            and isinstance(typed, int)
            and not isinstance(typed, bool)
            and typed == 0
        ):
            warnings.append(
                f"host_memory: {key}=0 is a size-0 bulkhead (admits nothing); not unlimited"
            )
        policy = replace(policy, **{key: typed})
    if warnings:
        policy = replace(policy, warnings=tuple(warnings))
    return policy


def host_memory_policy_echo(workspace_root: Path) -> dict:
    """Typed, path-safe echo of the effective host_memory policy for tool surfaces
    (internal D2b): the resolved ``values`` + the RELATIVE
    contract ``source_path`` + any loader ``warnings`` (e.g. a misplaced top-level
    ``host_memory:`` block). Lets an operator verify from the tool surface that a
    contract edit took effect, instead of importing ``load_host_memory_policy``
    directly. Emits only the relative contract path — never an absolute host path
    (PF-4).
    """
    from dataclasses import asdict

    policy = load_host_memory_policy(workspace_root)
    data = asdict(policy)
    warnings = list(data.pop("warnings", ()) or ())
    # PMH-F6: the echo deliberately reports the on-disk contract as-configured even
    # under the WORKBAY_HOSTGOV_DISABLE kill-switch (so an operator can confirm a
    # contract edit landed). But without flagging the kill-switch the payload was
    # self-contradictory — enforcement='enforce' alongside a disabled admission
    # decision. Surface the override explicitly so 'as-configured' is not misread
    # as 'as-enforced'.
    disabled_by_env = os.environ.get("WORKBAY_HOSTGOV_DISABLE") == "1"
    if disabled_by_env:
        warnings.append(
            "WORKBAY_HOSTGOV_DISABLE=1 is active: host-memory admission is BYPASSED "
            "for this process regardless of the enforcement value below (contract "
            "values are echoed as-configured, not as-enforced)."
        )
    return {
        "values": data,
        "source_path": str(HARNESS_CONTRACT_RELPATH),
        "warnings": warnings,
        "disabled_by_env": disabled_by_env,
    }


# ---------------------------------------------------------------------------
# admission decision + elastic width (D2/D3)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class StockSlotClaim:
    """Owned stock reservation whose close is idempotent and exception-safe."""

    idx: int
    fd: int
    namespace: str = "landable"
    _registered: bool = False
    _released: bool = False

    def __iter__(self) -> Iterator[int]:
        yield self.idx
        yield self.fd

    def __getitem__(self, item: int) -> int:
        return (self.idx, self.fd)[item]

    def __enter__(self) -> StockSlotClaim:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()

    def _register(self) -> None:
        with _STOCK_SLOT_REGISTRY_LOCK:
            _STOCK_SLOT_REGISTRY[self._registry_key] = self.fd
        self._registered = True

    @property
    def _registry_key(self) -> object:
        return self.idx if self.namespace == "landable" else (self.namespace, self.idx)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        if self._registered:
            _release_stock_slot(self.idx, self.fd, namespace=self.namespace)
            return
        try:
            os.close(self.fd)
        except OSError:
            pass


@dataclass(frozen=True, slots=True)
class StockSlotFailure:
    """Typed acquisition failure that is distinct from ordinary contention."""

    reason_code: str
    detail: str


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    """Outcome of an admission evaluation.

    ``enforced`` is False when ``warn_only``/``off`` downgraded a would-be
    ``defer``/``refuse`` to ``allow`` — the caller admits but the reason names
    the decision that would have fired under ``enforce``.

    ``stock_claim`` is an owned context-manager handle retained by the live I/O
    path until the caller's dispatch/probe scope ends. It is omitted from
    :meth:`to_dict`; ``reason_code`` carries the serialisable taxonomy.
    """

    decision: str  # allow | defer | refuse
    reason: str
    cost_class: str
    derived_width: int
    held_slots: int
    enforced: bool
    snapshot: HostResources
    reason_code: str | None = None
    stock_claim: StockSlotClaim | None = None

    def release_stock_claim(self) -> AdmissionDecision:
        """Release an attached stock claim and return a claim-free decision."""
        if self.stock_claim is None:
            return self
        self.stock_claim.release()
        return replace(self, stock_claim=None)

    def __enter__(self) -> AdmissionDecision:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release_stock_claim()

    def to_dict(self) -> dict[str, object]:
        from dataclasses import asdict

        return {
            "decision": self.decision,
            "reason": self.reason,
            "cost_class": self.cost_class,
            "derived_width": self.derived_width,
            "held_slots": self.held_slots,
            "enforced": self.enforced,
            "reason_code": self.reason_code,
            "snapshot": asdict(self.snapshot),
        }

    def demand_label(self) -> str:
        """Name the dimension that fired: slots_full vs memory.

        Slot-cap, stock-ceiling, and stock-escalation outcomes are ``slots_full``.
        ``memory`` is reserved for actual host memory pressure / RAM floors.
        """
        if self.reason_code == "slots_full":
            return "slots_full"
        if self.reason_code in (
            "review_stock_reclaim_required",
            "implement_stock_merge_required",
            "gauge_unmeasurable",
            "namespace_unusable",
            "stock_lock_unavailable",
            "slot_count_unavailable",
        ):
            return "slots_full"
        if self.reason_code == "memory":
            return "memory"
        reason = (self.reason or "").lower()
        if "stock" in reason or ("slot" in reason and "busy" in reason):
            return "slots_full"
        if "backend" in reason and "cap" in reason:
            return "slots_full"
        return "memory"


def format_admission_gate_error(decision: AdmissionDecision) -> str:
    """Operator-visible spawn error that names slots_full vs memory."""
    return f"host {decision.demand_label()} admission {decision.decision}: {decision.reason}"


def _rss_per_gib_for(policy: HostMemoryPolicy, cost_class: str) -> float:
    """Per-class worker RSS (GiB): a remote-API CLI driver is sized on its small
    local footprint, everything else on the heavy RSS (D1/PF-1)."""
    if cost_class == COST_REMOTE_API:
        return policy.rss_per_remote_api_gib
    return policy.rss_per_heavy_gib


def _os_reserve_gib_for(policy: HostMemoryPolicy, cost_class: str) -> float:
    """Per-class OS reserve (GiB): a remote-API worker's inference and suite run
    off-box, so it does not draw on the local headroom the full heavy reserve
    protects — gate it on the small ``os_reserve_remote_api_gib`` floor instead
    (internal). Every other class keeps the full
    ``os_reserve_gib``."""
    if cost_class == COST_REMOTE_API:
        return policy.os_reserve_remote_api_gib
    return policy.os_reserve_gib


def derive_width(resources: HostResources, policy: HostMemoryPolicy, cost_class: str = COST_HEAVY) -> int:
    """Elastic slot width for ``cost_class`` (D3, single-source formula).

    ``width = clamp(floor((available_ram - os_reserve_<class>) / rss_per_<class>), 0, max_width)``

    Floor is **0**, not 1: width 0 means admission refuses. A floor of 1 would
    admit a spawn into the OS reserve, exactly the headroom the reserve exists to
    protect. ``cost_class`` selects BOTH the per-class RSS and the per-class OS
    reserve (heavy vs remote-API), so a remote-API lane is neither force-sized
    against the heavy footprint nor gated by the full heavy OS reserve — its
    inference/suite run off-box (D1/PF-1, internal).
    """
    if cost_class == COST_REMOTE:
        # Fully off-box (VM runs agent + tests): local available_ram does not
        # constrain it, so its width is the concurrency cap, never a function of
        # local RAM headroom (internal). It is also absent from
        # _GATED_COST_CLASSES, so evaluate_admission short-circuits to allow; this
        # branch only keeps the reported width honest (max_width, not 0).
        return policy.max_width
    if resources.available_ram is None:
        # Width cannot be derived from an absent measurement. Classification
        # gives this state its own probe-unavailable refusal reason below.
        return 0
    rss_per_gib = _rss_per_gib_for(policy, cost_class)
    os_reserve_gib = _os_reserve_gib_for(policy, cost_class)
    # Defense in depth: the loader rejects non-finite/negative policy numerics,
    # but derive_width must not crash on a hand-constructed policy either.
    if not math.isfinite(rss_per_gib) or not math.isfinite(os_reserve_gib):
        return 0
    rss = rss_per_gib * _GIB
    if rss <= 0:
        return 0
    usable = resources.available_ram - int(os_reserve_gib * _GIB)
    if usable <= 0:
        return 0
    raw = int(usable // rss)
    return max(0, min(raw, policy.max_width))


def _classify_admission(
    resources: HostResources,
    cost_class: str,
    policy: HostMemoryPolicy,
    width: int,
    held_slots: int,
) -> tuple[str, str, str | None]:
    """Would-be decision under ``enforce`` (refuse > defer > allow)."""
    # A partial probe failure (e.g. the OCI VM's ``/proc/pressure/memory``
    # unreadable / PSI absent, or the Darwin sysctl+fallback both failing) leaves
    # ``pressure="unknown"`` WITHOUT a ``probe_error`` — so it never routes
    # through the probe's degrade-to-``warn`` path. Treat ``unknown`` as the same
    # degraded ``warn`` here so a blind probe cannot silently disable the pressure
    # dimension of the gate (OBS-08: silence is not success).
    pressure = "warn" if resources.pressure == "unknown" else resources.pressure
    # --- refuse dimensions (each names the failing dimension) ---
    if pressure == "critical":
        return "refuse", "memory pressure critical", "memory"
    if resources.available_ram is None:
        # Fail closed when the guard cannot be evaluated, but do not invent a
        # zero reading or blame available RAM for a measurement never taken.
        return "refuse", "available RAM probe unavailable; admission fail-closed", "memory"
    available_ram_floor = policy.available_ram_floor_gib * _GIB
    if resources.available_ram < available_ram_floor:
        return (
            "refuse",
            f"available RAM {resources.available_ram / _GIB:.1f}GiB "
            f"below floor {policy.available_ram_floor_gib}GiB",
            "memory",
        )
    swap_floor = policy.swap_free_floor_mb * _MIB
    if resources.swap_total > 0 and resources.swap_free < swap_floor:
        return (
            "refuse",
            f"swap free {resources.swap_free / _MIB:.0f}MB below floor {policy.swap_free_floor_mb}MB",
            "memory",
        )
    disk_floor = policy.swap_volume_disk_floor_gib * _GIB
    # Skip when the volume reading is absent (0) — a narrow read failure must
    # not refuse every spawn; broad probe failure is handled via pressure=warn.
    if 0 < resources.swap_volume_free_bytes < disk_floor:
        return (
            "refuse",
            f"swap-volume free disk {resources.swap_volume_free_bytes / _GIB:.1f}GiB "
            f"below floor {policy.swap_volume_disk_floor_gib}GiB",
            "memory",
        )
    if width == 0:
        return (
            "refuse",
            "derived width 0 (available RAM minus OS reserve < per-class RSS)",
            "memory",
        )
    # --- defer dimensions (retryable) ---
    if pressure == "warn" and held_slots >= 1:
        detail = "warn" if resources.pressure == "warn" else "unknown(degraded)"
        return (
            "defer",
            f"memory pressure {detail} with {held_slots} heavy slot(s) held",
            "memory",
        )
    if held_slots >= width:
        outcome = policy.slots_full_outcome if policy.slots_full_outcome in ("defer", "refuse") else "defer"
        return outcome, f"all {width} derived heavy slot(s) busy", "slots_full"
    return "allow", f"width {width}, {held_slots} slot(s) held", None


def _stock_class_for_kind(lane_kind: str) -> str:
    return "record-only" if lane_kind == "review" else "landable"


def derive_stock_lane_kind(
    lane_kind: str | None = None,
    *,
    lane_row: Mapping[str, object] | None = None,
) -> str:
    """Resolve the stock class kind from an explicit value or the lane row.

    Callers that already know the kind pass it through. Spawn edges that only
    have a lane row derive it here. A present but empty kind (old-schema rows
    and fixtures that pin backend without stock identity) occupies the
    landable bulkhead — the same default the gauge uses. A non-empty unknown
    kind is returned unchanged so classification fail-closes instead of
    silently treating a corrupt value as implement.
    """
    explicit = str(lane_kind).strip() if lane_kind is not None else ""
    if explicit in ("implement", "review"):
        return explicit
    if isinstance(lane_row, Mapping):
        row_kind = str(lane_row.get("lane_kind") or "").strip()
        if row_kind in ("implement", "review"):
            return row_kind
        if row_kind:
            return row_kind
        return "implement"
    if explicit:
        return explicit
    return "implement"


def _split_ceiling(policy: HostMemoryPolicy, lane_kind: str) -> tuple[int, str]:
    if lane_kind == "review":
        ceiling = (
            policy.worktree_stock_ceiling
            if policy.worktree_record_only_stock_ceiling is None
            else policy.worktree_record_only_stock_ceiling
        )
        return ceiling, "record-only review"
    ceiling = (
        policy.worktree_stock_ceiling
        if policy.worktree_landable_stock_ceiling is None
        else policy.worktree_landable_stock_ceiling
    )
    return ceiling, "landable"


def _split_outstanding(stock: WorktreeStock, lane_kind: str) -> int | None:
    outstanding = stock.outstanding_record_only if lane_kind == "review" else stock.outstanding_landable
    if stock.stock_classes_measured:
        # Measured snapshots with an unknown class split or unknown sibling
        # occupancy must not fall back to the aggregate. False is only the
        # legacy never-measured constructor origin.
        if stock.unregistered_paths is None:
            return None
        return outstanding
    if outstanding is None:
        return stock.outstanding_unlanded
    return outstanding


def _stock_exhaustion_escalation(
    stock: WorktreeStock,
    *,
    lane_kind: str,
    outstanding: int,
    ceiling: int,
    bounded_limit: int,
    stock_label: str,
) -> tuple[str, str, str]:
    if lane_kind == "review":
        candidates = stock.reclaimable_record_only_worktrees
        named = ", ".join(candidates) if candidates else "none"
        return (
            "refuse",
            f"{stock_label} stock outstanding {outstanding} at/above ceiling {ceiling}; "
            f"bounded defer exhausted after {bounded_limit} attempt(s); "
            f"reclaimable record-only worktrees: {named}",
            "review_stock_reclaim_required",
        )
    candidates = stock.unmerged_landable_worktrees
    named = ", ".join(candidates) if candidates else "none"
    return (
        "refuse",
        f"{stock_label} stock outstanding {outstanding} at/above ceiling {ceiling}; "
        f"bounded defer exhausted after {bounded_limit} attempt(s); "
        f"unmerged landable worktrees (merge demand): {named}",
        "implement_stock_merge_required",
    )


def _classify_stock(
    policy: HostMemoryPolicy,
    stock: WorktreeStock | None,
    *,
    lane_kind: str = "implement",
    stock_defer_count: int = 0,
) -> tuple[str, str, str] | None:
    """Would-be stock-dimension decision, or ``None`` when the dimension is inert.

    A missing snapshot (``stock is None``) is the memory-only path used by
    existing callers — the dimension is not evaluated, so this slice does not
    turn the bound on for them. A supplied snapshot whose
    ``outstanding_unlanded`` is ``None`` is unknown: fail closed. Precedent
    in ``_classify_admission``: ``pressure="unknown"`` is mapped onto the
    degraded warn path so a blind probe cannot silently disable a dimension.
    Stock is stricter than that warn mapping — unknown outstanding must
    never allow, because treating it as zero is exactly the comfortable
    reading the gauge forbids.
    """
    if stock is None:
        return None
    if lane_kind not in ("implement", "review"):
        return (
            "refuse",
            f"stock lane kind {lane_kind!r} unknown; stock dimension fail-closed",
            "gauge_unmeasurable",
        )
    ceiling, stock_label = _split_ceiling(policy, lane_kind)
    if ceiling <= 0:
        return (
            "refuse",
            f"{stock_label} stock ceiling {ceiling} is non-positive; size-0 bulkhead admits nothing",
            "slots_full",
        )
    # Snapshots constructed by pre-bulkhead callers do not contain class
    # counters. Preserve their aggregate semantics. A newly measured snapshot
    # with an unknown split, however, must fail closed rather than guessing.
    outstanding = _split_outstanding(stock, lane_kind)
    if outstanding is None:
        return (
            "refuse",
            f"{stock_label} stock unmeasurable (outstanding unknown); stock dimension fail-closed",
            "gauge_unmeasurable",
        )
    if outstanding >= ceiling:
        outcome = policy.slots_full_outcome if policy.slots_full_outcome in ("defer", "refuse") else "defer"
        bounded_limit = max(1, policy.stock_defer_limit)
        if outcome == "defer" and max(0, stock_defer_count) + 1 >= bounded_limit:
            return _stock_exhaustion_escalation(
                stock,
                lane_kind=lane_kind,
                outstanding=outstanding,
                ceiling=ceiling,
                bounded_limit=bounded_limit,
                stock_label=stock_label,
            )
        return (
            outcome,
            f"{stock_label} stock outstanding {outstanding} at/above ceiling {ceiling}",
            "slots_full",
        )
    return None


def _classify_backend_cap(
    policy: HostMemoryPolicy,
    backend: str | None,
    held_slots_by_backend: Mapping[str, int] | None,
) -> tuple[str, str] | None:
    """Would-be per-backend local-cap decision, or ``None`` when inert."""
    if backend is None or policy.per_backend_local_cap <= 0:
        return None
    held_for_backend = (held_slots_by_backend or {}).get(backend, 0)
    if held_for_backend < policy.per_backend_local_cap:
        return None
    outcome = policy.slots_full_outcome if policy.slots_full_outcome in ("defer", "refuse") else "defer"
    return (
        outcome,
        (f"backend {backend} at local concurrency cap ({held_for_backend} held, cap {policy.per_backend_local_cap})"),
    )


def _apply_enforcement(
    decision: str,
    reason: str,
    cost_class: str,
    width: int,
    held_slots: int,
    resources: HostResources,
    policy: HostMemoryPolicy,
    reason_code: str | None = None,
) -> AdmissionDecision:
    if policy.enforcement in ("warn_only", "off") and decision != "allow":
        return AdmissionDecision(
            "allow",
            f"{policy.enforcement}: would {decision} ({reason})",
            cost_class,
            width,
            held_slots,
            False,
            resources,
            reason_code,
        )
    return AdmissionDecision(decision, reason, cost_class, width, held_slots, True, resources, reason_code)


def evaluate_admission(
    resources: HostResources,
    cost_class: str,
    policy: HostMemoryPolicy,
    held_slots: int = 0,
    *,
    backend: str | None = None,
    held_slots_by_backend: Mapping[str, int] | None = None,
    stock: WorktreeStock | None = None,
    lane_kind: str = "implement",
    stock_defer_count: int = 0,
) -> AdmissionDecision:
    """Call-time admission verdict for a spawn of ``cost_class`` (D2).

    Pure function: ``held_slots`` is injected by the caller (the slot registry
    read is I/O kept separate). ``light`` is never gated; ``enforcement=off``
    skips evaluation; ``warn_only`` downgrades a would-be defer/refuse to an
    unenforced allow.

    Optional ``backend`` / ``held_slots_by_backend`` apply a per-backend local
    concurrency cap (``policy.per_backend_local_cap``, default 1) on top of the
    class width gate for gated cost classes only (internal S2b). Callers
    without backend identity keep working: both kwargs default to None and the
    class-width path is unchanged.

    Optional ``stock`` is the orthogonal STOCK dimension: evaluated for every
    cost class, including the fully off-box one. Memory dimensions stay
    exempt for ungated classes; stock does not. Omit ``stock`` to keep the
    memory-only path (existing tests unchanged).
    """
    width = derive_width(resources, policy, cost_class)
    stock_hit = _classify_stock(
        policy,
        stock,
        lane_kind=lane_kind,
        stock_defer_count=stock_defer_count,
    )

    if cost_class not in _GATED_COST_CLASSES:
        if policy.enforcement != "off" and stock_hit is not None:
            return _apply_enforcement(
                stock_hit[0], stock_hit[1], cost_class, width, held_slots, resources, policy, stock_hit[2]
            )
        # Memory dimensions stay ungated; ``enforcement=off`` must not be
        # readable as an enforced allow by later claim sites.
        return AdmissionDecision(
            "allow",
            f"{cost_class} cost class is never gated",
            cost_class,
            width,
            held_slots,
            policy.enforcement != "off",
            resources,
        )
    if policy.enforcement == "off":
        return AdmissionDecision("allow", "enforcement=off", cost_class, width, held_slots, False, resources)

    decision, reason, reason_code = _classify_admission(resources, cost_class, policy, width, held_slots)
    if decision == "allow":
        backend_hit = _classify_backend_cap(policy, backend, held_slots_by_backend)
        if backend_hit is not None:
            decision, reason = backend_hit
            reason_code = "slots_full"
    if decision == "allow" and stock_hit is not None:
        decision, reason, reason_code = stock_hit
    return _apply_enforcement(decision, reason, cost_class, width, held_slots, resources, policy, reason_code)


def acquire_stock_slot(
    root: Path,
    ceiling: int,
    *,
    start: int = 0,
    stock_class: str = "landable",
) -> StockSlotClaim | StockSlotFailure | None:
    """Acquire an owned reservation, ``None`` on contention, or a typed failure.

    Tries ``admission/stock/slot-{start}`` .. ``slot-{ceiling-1}`` with a
    non-blocking flock. The namespace itself is always ``0 .. ceiling-1``;
    ``start`` is leftover occupancy that does not hold a flock, so new claims
    take the remaining high-index window instead of colliding with live
    low-index holders or overshooting leftover stock. The successful lock
    **is** the additional claim — there is no separate remaining-sized
    namespace that double-counts live holders. The caller MUST release the
    returned handle (preferably with ``with``); process death is the fallback.
    """
    import fcntl

    begin = max(0, start)
    if ceiling <= 0 or begin >= ceiling:
        return None
    slot_dir = root / "admission" / ("stock" if stock_class == "landable" else "stock-review")
    try:
        slot_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return StockSlotFailure("namespace_unusable", f"stock slot directory unusable: {exc}")
    for n in range(begin, ceiling):
        slot = slot_dir / f"slot-{n}"
        try:
            fd = os.open(slot, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError as exc:
            return StockSlotFailure("namespace_unusable", f"stock slot file unusable: {exc}")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            continue
        return StockSlotClaim(n, fd, namespace=stock_class)
    return None


def evaluate_admission_claiming_stock(
    resources: HostResources,
    cost_class: str,
    policy: HostMemoryPolicy,
    root: Path,
    *,
    stock: WorktreeStock,
    held_slots: int = 0,
    backend: str | None = None,
    held_slots_by_backend: Mapping[str, int] | None = None,
    lane_kind: str = "implement",
    stock_defer_count: int = 0,
) -> tuple[AdmissionDecision, StockSlotClaim | None]:
    """Evaluate admission and claim a stock slot as one operation.

    Classification (including fail-closed unknown stock) runs first; a
    would-be allow then claims via :func:`acquire_stock_slot`. The flock
    namespace is the stable ``0 .. ceiling-1`` range so remesure of live
    holders does not double-count. Leftover worktrees that no longer hold
    a flock occupy the low logical window: new claims start at
    ``outstanding``, so concurrent admits cannot take more than the
    remaining capacity. ``enforcement=off`` returns the classify decision
    without claiming.
    """
    decision = evaluate_admission(
        resources,
        cost_class,
        policy,
        held_slots,
        backend=backend,
        held_slots_by_backend=held_slots_by_backend,
        stock=stock,
        lane_kind=lane_kind,
        stock_defer_count=stock_defer_count,
    )
    if policy.enforcement == "off":
        return decision, None
    if decision.decision != "allow" or not decision.enforced:
        return decision, None
    if lane_kind not in ("implement", "review"):
        return decision, None
    ceiling, _label = _split_ceiling(policy, lane_kind)
    stock_class = _stock_class_for_kind(lane_kind)
    if ceiling <= 0:
        return (
            _apply_enforcement(
                "refuse",
                f"{stock_class} stock ceiling {ceiling} is non-positive; size-0 bulkhead admits nothing",
                cost_class,
                decision.derived_width,
                held_slots,
                resources,
                policy,
                "slots_full",
            ),
            None,
        )
    outstanding = _split_outstanding(stock, lane_kind)
    start = 0 if outstanding is None else outstanding
    claimed = acquire_stock_slot(root, ceiling, start=start, stock_class=stock_class)
    if isinstance(claimed, StockSlotFailure):
        return (
            _apply_enforcement(
                "refuse",
                claimed.detail,
                cost_class,
                decision.derived_width,
                held_slots,
                resources,
                policy,
                claimed.reason_code,
            ),
            None,
        )
    if claimed is None:
        outcome = policy.slots_full_outcome if policy.slots_full_outcome in ("defer", "refuse") else "defer"
        reason_code = "slots_full"
        if outcome == "defer" and max(0, stock_defer_count) + 1 >= max(1, policy.stock_defer_limit):
            _decision, reason, reason_code = _stock_exhaustion_escalation(
                stock,
                lane_kind=lane_kind,
                outstanding=0 if outstanding is None else outstanding,
                ceiling=ceiling,
                bounded_limit=max(1, policy.stock_defer_limit),
                stock_label="record-only review" if lane_kind == "review" else "landable",
            )
            outcome = _decision
        else:
            reason = (
                f"{stock_class} stock all {ceiling} slot(s) claimed "
                f"(outstanding {outstanding}, ceiling {ceiling})"
            )
        return (
            _apply_enforcement(
                outcome,
                reason,
                cost_class,
                decision.derived_width,
                held_slots,
                resources,
                policy,
                reason_code,
            ),
            None,
        )
    return decision, claimed


def _unmeasurable_stock() -> WorktreeStock:
    """Fail-closed snapshot used when a git checkout cannot be measured."""
    return WorktreeStock(
        registered_worktrees=None,
        primary_worktrees=None,
        linked_worktrees=None,
        outstanding_unlanded=None,
        unregistered_paths=None,
        volume_free_bytes=None,
        volume_total_bytes=None,
        probe_errors=("worktree_list",),
    )


def _is_inside_git_work_tree(root: Path) -> bool | None:
    """Work-tree membership for the stock gauge.

    ``True`` — git reported this path is inside a work tree.
    ``False`` — git reported it is not (stdout ``false``), or the path is
    not a git repository. The stock dimension is inert: there is no
    checkout whose unlanded stock could pile up.
    ``None`` — the probe failed (timeout, OSError, unexpected git error).
    Callers must fail closed; a stressed or locked git must not skip the
    ceiling.
    """
    try:
        proc = subprocess.run(  # noqa: S603
            ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    text = (proc.stdout or "").strip().lower()
    if proc.returncode == 0:
        if text == "true":
            return True
        if text == "false":
            return False
        return None
    err = (proc.stderr or "").lower()
    if "not a git repository" in err:
        return False
    return None


def _try_lane_rows_for_stock(workspace_root: Path) -> object:
    """Globally scoped lane rows, without resolving one active task."""
    from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415
    from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415
    from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

    # Cold CLI processes (workbay-hostgov under check-remote) have no
    # pre-configured handoff runtime. Configure for the repo before the
    # first DB call, matching the other hostgov bootstrap sites in this
    # module. Do not resolve one active task: the gauge is workspace-global.
    handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(workspace_root)))
    try:
        with _get_db_connection() as conn:
            rows = conn.execute("SELECT * FROM worktree_lanes ORDER BY updated_at DESC, id DESC").fetchall()
        return [dict(row) for row in rows]
    except RuntimeNotConfiguredError:
        raise
    except Exception:  # noqa: BLE001 -- a lane-row failure must fail closed, never zero
        return None


def _try_review_reports_for_stock(workspace_root: Path) -> object:
    """Newest completion reports used only to prove review reclaimability."""
    from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415
    from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError  # noqa: PLC0415
    from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

    handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(workspace_root)))
    try:
        with _get_db_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM worker_reports ORDER BY created_at DESC, id DESC"
            ).fetchall()
        return [dict(row) for row in rows]
    except RuntimeNotConfiguredError:
        raise
    except Exception:  # noqa: BLE001 -- doubt disables reclaim, never admission
        return None


def _worker_lock_in_use(lane_id: str) -> bool | None:
    """Probe the lane worker flock: True held, False idle, None ambiguous."""
    key = (lane_id or "").strip()
    if not key:
        return None
    try:
        from .lane_worktree import _runtime_is_configured, _worker_lock_path_for_lane

        path = _worker_lock_path_for_lane(key)
    except Exception:  # noqa: BLE001 -- unresolvable lock is ambiguous
        return None
    if path is None:
        try:
            return None if _runtime_is_configured() else False
        except Exception:  # noqa: BLE001
            return None
    if not path.exists():
        return False
    handle = None
    try:
        import fcntl

        handle = path.open("a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    except OSError:
        return None
    finally:
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass


def _admission_in_use_probe(row: Mapping[str, object]) -> bool | None:
    from .worktree_stock import _row_in_use_state

    occupied = _row_in_use_state(row)
    if occupied is not False:
        return occupied
    return _worker_lock_in_use(str(row.get("lane_id") or ""))


def _collect_live_stock(workspace_root: Path) -> WorktreeStock | None:
    """Gauge reading for the live gate.

    ``None`` means the workspace is not a git checkout and the dimension is
    inert (hermetic callers that are not a repository). A git checkout
    whose gauge cannot measure — including a stressed or locked
    ``rev-parse`` — returns a snapshot with unknown outstanding so
    admission fails closed. Only an explicit "not a work tree" reading is
    inert.
    """
    inside = _is_inside_git_work_tree(workspace_root)
    if inside is False:
        return None
    if inside is None:
        return _unmeasurable_stock()
    from .worktree_stock import collect_worktree_stock, reclaim_completed_review_worktrees

    try:
        def _load_reclaim_state():
            return (
                _try_lane_rows_for_stock(workspace_root),
                _try_review_reports_for_stock(workspace_root),
            )

        # Steady-state purge runs before each live reading. It is deliberately
        # fail-safe: the reclaimer re-reads all evidence at delete time and a
        # failed/unknown precondition merely leaves stock in the gauge.
        reclaim_completed_review_worktrees(
            workspace_root,
            load_state=_load_reclaim_state,
            in_use_probe=_admission_in_use_probe,
        )
        lane_rows, review_reports = _load_reclaim_state()
        return collect_worktree_stock(
            workspace_root,
            lane_rows=lane_rows,
            review_reports=review_reports,
        )
    except Exception:  # noqa: BLE001 -- a gauge exception is unknown stock, not zero
        return _unmeasurable_stock()


def resolve_live_admission(
    workspace_root: Path,
    cost_class: str = COST_HEAVY,
    *,
    exclude_slots: Collection[int] | None = None,
    lane_kind: str | None = None,
) -> AdmissionDecision:
    """Full call-time admission verdict: probe + policy + slots + stock.

    The single I/O entry point shared by the orchestrator dispatch surfaces and
    the ``workbay-hostgov`` CLI. Slot-count failure refuses with
    ``slot_count_unavailable`` rather than treating occupancy as zero.
    ``locks_root()`` is three-state: a git work tree claims the lock
    namespace; a path that is not a repository leaves the stock dimension
    inert (memory-only admission, same as main); a failed probe inside a
    repository refuses with ``stock_lock_unavailable`` instead of admitting
    without the stock reservation. A live lock namespace whose stock
    snapshot is missing refuses with ``gauge_unmeasurable`` rather than
    falling through to the non-claiming evaluator, which can allow with no
    reservation held. Stock is collected and claimed here so the dimension
    is live on every production surface that uses this function, including
    the fully off-box cost class.

    A successful stock claim is retained on ``AdmissionDecision.stock_claim``
    and in the process-local stock registry. The decision is a context manager;
    production callers release it after successful dispatch, later refusal,
    probe-only use, or exception.

    ``exclude_slots`` (keyword-only) is forwarded to
    :func:`count_held_heavy_slots` so a coordinator that already holds a slot
    does not self-count it. Only process-owned slots in the registry are
    excluded (implementation note S3).
    """
    resources = probe_host()
    policy = load_host_memory_policy(workspace_root)
    held = 0
    lock_root: Path | None = None
    try:
        lock_root = locks_root(workspace_root)
    except _NotAGitWorkTree:
        lock_root = None
    except Exception:  # noqa: BLE001 -- probe-failed inside a repo is a typed refusal
        width = derive_width(resources, policy, cost_class)
        return _apply_enforcement(
            "refuse",
            "stock lock namespace unavailable",
            cost_class,
            width,
            held,
            resources,
            policy,
            "stock_lock_unavailable",
        )
    width = derive_width(resources, policy, cost_class)
    if cost_class in _GATED_COST_CLASSES:
        try:
            if lock_root is not None:
                held = count_held_heavy_slots(lock_root, width, exclude_slots=exclude_slots)
        except Exception:  # noqa: BLE001 -- an untrusted occupancy must not read as zero
            return _apply_enforcement(
                "refuse",
                "heavy slot count unavailable; admission fail-closed",
                cost_class,
                width,
                held,
                resources,
                policy,
                "slot_count_unavailable",
            )
    stock = _collect_live_stock(workspace_root)
    kind = derive_stock_lane_kind(lane_kind)
    stock_class = _stock_class_for_kind(kind)
    stock_defer_count = (
        _read_stock_defer_count(lock_root, stock_class) if lock_root is not None else 0
    )
    if stock is not None and lock_root is not None:
        decision, claimed = evaluate_admission_claiming_stock(
            resources,
            cost_class,
            policy,
            lock_root,
            stock=stock,
            held_slots=held,
            lane_kind=kind,
            stock_defer_count=stock_defer_count,
        )
        _record_stock_defer_streak(lock_root, stock_class, decision, policy)
        if claimed is not None:
            claimed._register()
            return replace(decision, stock_claim=claimed)
        return decision
    if lock_root is not None:
        # Stock dimension is live (a lock namespace exists) but the gauge
        # returned nothing. The non-claiming evaluator can allow in that
        # shape; refusing keeps an allow from escaping without a reservation.
        decision = _apply_enforcement(
            "refuse",
            "stock snapshot unavailable; cannot claim a stock slot",
            cost_class,
            width,
            held,
            resources,
            policy,
            "gauge_unmeasurable",
        )
        _record_stock_defer_streak(lock_root, stock_class, decision, policy)
        return decision
    decision = evaluate_admission(
        resources,
        cost_class,
        policy,
        held,
        stock=stock,
        lane_kind=kind,
        stock_defer_count=stock_defer_count,
    )
    _record_stock_defer_streak(lock_root, stock_class, decision, policy)
    return decision


def _stock_defer_streak_path(lock_root: Path, stock_class: str) -> Path:
    name = "record-only" if stock_class == "record-only" else "landable"
    return Path(lock_root) / "admission" / "stock-defer" / name


def _read_stock_defer_count(lock_root: Path, stock_class: str) -> int:
    path = _stock_defer_streak_path(lock_root, stock_class)
    try:
        return max(0, int(path.read_text(encoding="utf-8").strip() or "0"))
    except (OSError, ValueError):
        return 0


def _record_stock_defer_streak(
    lock_root: Path | None,
    stock_class: str,
    decision: AdmissionDecision,
    policy: HostMemoryPolicy,
) -> None:
    """Persist the consecutive slots_full streak next to the stock flocks."""
    if lock_root is None:
        return
    import fcntl

    path = _stock_defer_streak_path(lock_root, stock_class)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.seek(0)
            raw = handle.read().strip()
            try:
                current = max(0, int(raw or "0"))
            except ValueError:
                current = 0
            if decision.reason_code == "slots_full" and decision.decision == "defer":
                current += 1
            elif decision.reason_code in (
                "review_stock_reclaim_required",
                "implement_stock_merge_required",
            ):
                current = max(current, max(1, policy.stock_defer_limit))
            else:
                current = 0
            handle.seek(0)
            handle.truncate()
            handle.write(str(current))
            handle.flush()
    except OSError:
        return


def _release_stock_slot(idx: int, fd: int, *, namespace: str = "landable") -> None:
    """Close a stock-slot fd and de-register it from the process-local registry.

    Pop the registry entry BEFORE closing the fd so a concurrent acquire that
    reuses the same fd number is not silently de-registered. Close only when
    this process currently owns ``(idx, fd)``.
    """
    owned = False
    key: object = idx if namespace == "landable" else (namespace, idx)
    with _STOCK_SLOT_REGISTRY_LOCK:
        if _STOCK_SLOT_REGISTRY.get(key) == fd:
            _STOCK_SLOT_REGISTRY.pop(key, None)
            owned = True
    if owned:
        try:
            os.close(fd)
        except OSError:
            pass


def _release_held_stock_slots() -> None:
    """Release every stock flock this process currently holds (test teardown)."""
    with _STOCK_SLOT_REGISTRY_LOCK:
        held = list(_STOCK_SLOT_REGISTRY.items())
        _STOCK_SLOT_REGISTRY.clear()
    for _idx, fd in held:
        try:
            os.close(fd)
        except OSError:
            pass


class SuiteLockTimeout(RuntimeError):
    """The global suite lock could not be acquired within the timeout (D4)."""


def acquire_suite_lock(root: Path, timeout_s: float) -> int | None:
    """Acquire the global ``suite.lock`` (blocking up to ``timeout_s`` seconds).

    Returns the held fd (the caller closes it to release) or ``None`` on timeout.
    Blocking-with-timeout is a poll loop because ``flock`` has no native timeout;
    the poll interval is coarse (suites run for minutes, so a fraction of a
    second of contention latency is irrelevant).
    """
    import fcntl

    root.mkdir(parents=True, exist_ok=True)
    lock = root / "suite.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o644)
    deadline = time.monotonic() + max(0.0, timeout_s)
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            if time.monotonic() >= deadline:
                os.close(fd)
                return None
            time.sleep(0.25)
            continue
        return fd


def acquire_suite_bulkhead(orchestrator_root: Path, *, timeout_s: float | None = None) -> int | None:
    """Acquire the suite bulkhead for a suite-cost subprocess (D4).

    Returns an fd the caller must close to release, or ``None`` when the
    bulkhead is disabled (``WORKBAY_HOSTGOV_DISABLE=1`` / ``enforcement: off`` /
    the locks root cannot be resolved). Raises :class:`SuiteLockTimeout` when the
    lock is held elsewhere past ``timeout_s`` (defaults to the policy's
    ``suite_lock_timeout_s``). Serializes suites globally so two lanes never run
    heavy suites concurrently.
    """
    if os.environ.get("WORKBAY_HOSTGOV_DISABLE") == "1":
        return None
    policy = load_host_memory_policy(orchestrator_root)
    if policy.enforcement == "off":
        return None
    try:
        root = locks_root(orchestrator_root)
    except Exception:  # noqa: BLE001 -- unresolved locks root degrades to unserialized, never crashes the suite
        return None
    resolved_timeout = policy.suite_lock_timeout_s if timeout_s is None else timeout_s
    fd = acquire_suite_lock(root, resolved_timeout)
    if fd is None:
        raise SuiteLockTimeout(f"suite lock not acquired within {resolved_timeout}s")
    return fd


def acquire_heavy_slot(root: Path, width: int) -> tuple[int, int] | None:
    """Try to acquire one heavy slot; return ``(slot_index, fd)`` or ``None``.

    Tries ``slot-0``..``slot-{width-1}`` in order, holding the first that a
    non-blocking ``flock`` grants. **The caller MUST keep the returned fd open
    for the worker's whole lifetime** — the kernel releases the lock when the fd
    closes or the process dies, which is exactly the no-reclaimer steady state.
    Returns ``None`` when width is 0 or every slot is already held.

    On success the ``(idx → fd)`` mapping is registered in the process-local
    ownership registry so :func:`count_held_heavy_slots` can exclude this
    process's own held slots. Call :func:`_release_heavy_slot` to close the fd
    and de-register (de-registration is not automatic on bare ``os.close``).
    """
    import fcntl

    slot_dir = root / "admission"
    slot_dir.mkdir(parents=True, exist_ok=True)
    for n in range(max(0, width)):
        slot = slot_dir / f"slot-{n}"
        fd = os.open(slot, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            continue
        with _HEAVY_SLOT_REGISTRY_LOCK:
            _HEAVY_SLOT_REGISTRY[n] = fd
        return (n, fd)
    return None


def _backend_slot_dirname(backend: str) -> str:
    """Filesystem-safe backend id for the per-backend slot namespace."""
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in backend) or "unknown"


def _backend_slot_dir(root: Path, backend: str) -> Path:
    """``<locks_root>/admission/by-backend/<backend>/`` — per-backend local slots."""
    return root / "admission" / "by-backend" / _backend_slot_dirname(backend)


def count_held_backend_slots(root: Path, backend: str, cap: int) -> int:
    """Count currently-held per-backend local slots by flock-probing slot files.

    Cross-process and self-releasing: a slot is held when non-blocking ``flock``
    fails; the kernel drops the lock when the holder dies. Same no-reclaimer
    model as :func:`count_held_heavy_slots` (internal).
    """
    import fcntl

    slot_dir = _backend_slot_dir(root, backend)
    held = 0
    for n in range(max(0, cap)):
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


def acquire_backend_local_slot(root: Path, backend: str, cap: int) -> tuple[int, int] | None:
    """Try to acquire one per-backend local slot; return ``(slot_index, fd)`` or ``None``.

    Namespace: ``admission/by-backend/<backend>/slot-{n}`` sized by ``cap``
    (``policy.per_backend_local_cap``). Same flock semantics as the class-wide
    heavy slots — cross-process, kernel-released on process death. Acquired
    *in addition* to a class-wide heavy slot at the worker gate; callers must
    keep the returned fd open for the worker lifetime and associate it with the
    class slot via :func:`bind_backend_slot_to_class` so
    :func:`_release_heavy_slot` frees both (reverse order).
    """
    import fcntl

    if cap <= 0:
        return None
    slot_dir = _backend_slot_dir(root, backend)
    slot_dir.mkdir(parents=True, exist_ok=True)
    for n in range(cap):
        slot = slot_dir / f"slot-{n}"
        fd = os.open(slot, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            continue
        return (n, fd)
    return None


class HeavySlotNotHeldError(RuntimeError):
    """A per-backend slot was bound to a class index this process does not hold."""


def bind_backend_slot_to_class(class_idx: int, backend_idx: int, backend_fd: int) -> None:
    """Associate a held per-backend slot with a class-wide heavy slot for joint release.

    Refuse when this process does not currently hold ``class_idx``. The joint
    release in ``_release_heavy_slot`` pops the backend entry only inside the
    class-ownership branch, so a mapping written against an index the registry
    does not own can never be reclaimed: a non-matching release is a no-op and a
    matching release is impossible while the index is absent. That is a
    per-backend capacity leak until process death.

    Failing closed here is what keeps the two maps unable to desynchronise
    (ARCH-13) instead of relying on every caller to bind only what it holds.
    The sole production caller binds immediately after a successful
    ``acquire_heavy_slot`` registered the same index, so this refuses no
    reachable bind today; it bounds the damage of a future one.
    """
    with _HEAVY_SLOT_REGISTRY_LOCK:
        if class_idx not in _HEAVY_SLOT_REGISTRY:
            raise HeavySlotNotHeldError(
                f"refusing to bind backend slot {backend_idx} to class slot {class_idx}: "
                "this process does not hold that class slot, so the binding could "
                "never be released"
            )
        _BACKEND_SLOT_FOR_CLASS[class_idx] = (backend_idx, backend_fd)


def _release_heavy_slot(idx: int, fd: int) -> None:
    """Close a heavy-slot fd and de-register it from the process-local registry.

    Ownership handoff helper for the wave coordinator (implementation note S3): call this
    when the coordinator claimed a slot but submission failed before a worker
    took ownership, or on every terminal worker path after the pass ends.
    De-registration is deliberate — bare ``os.close(fd)`` alone leaves the
    registry entry (worker_daemon historically owns the fd without the registry).

    Pop the registry entry BEFORE closing the fd: closing first, then de-registering
    by raw-fd equality, races a concurrent ``acquire_heavy_slot`` that re-opens the
    freed slot and receives the SAME fd number — which this de-register would then
    silently remove. Popping while the fd is still valid closes that window.

    Close the caller's fd only when the registry currently owns ``(idx, fd)``.
    A stale pair (double unwind, retry, journal redo of a dead process) must
    not close whatever descriptor now occupies that number. Close a bound
    backend fd only when that entry was actually popped for this index.

    When a per-backend local slot was bound to this class slot, it is released
    first (reverse of acquire order) so the live per-backend cap restores capacity
    (internal).
    """
    backend_held: tuple[int, int] | None = None
    owned = False
    with _HEAVY_SLOT_REGISTRY_LOCK:
        if _HEAVY_SLOT_REGISTRY.get(idx) == fd:
            _HEAVY_SLOT_REGISTRY.pop(idx, None)
            owned = True
            backend_held = _BACKEND_SLOT_FOR_CLASS.pop(idx, None)
    # Reverse acquire order: backend slot first, then class-wide slot.
    if backend_held is not None:
        _backend_idx, backend_fd = backend_held
        try:
            os.close(backend_fd)
        except OSError:
            pass
    if owned:
        try:
            os.close(fd)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# post-crash resume circuit breaker (D5)
# ---------------------------------------------------------------------------

_BREAKER_LOOKBACK_S = 6 * 3600
# Two boot_time readings for the same boot differ by clock jitter only.
_BREAKER_BOOT_TOLERANCE_S = 120.0


def _breaker_marker(root: Path, task_ref: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in task_ref)
    return root / "admission" / f"breaker-{safe}"


def _collect_created_at_epochs(payload: object) -> list[float]:
    """Recursively harvest parseable ``created_at`` timestamps (epoch seconds).

    Deliberately shape-agnostic: lane-activity payload sections vary by server
    version, and the breaker only needs "when was this lane last active".
    """
    from datetime import datetime, timezone  # noqa: PLC0415

    out: list[float] = []

    def _parse(value: object) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str) or not value.strip():
            return None
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    def _walk(node: object) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "created_at":
                    stamp = _parse(value)
                    if stamp is not None:
                        out.append(stamp)
                else:
                    _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(payload)
    return out


def _default_lane_activity_probe(workspace_root: Path, task_ref: str) -> list[float]:
    """Per-lane last-activity epochs for ``task_ref`` (one entry per lane)."""
    import json  # noqa: PLC0415

    from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    from workbay_orchestrator_mcp import lanes  # noqa: PLC0415

    handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(workspace_root)))

    def _load(payload: object) -> dict:
        return json.loads(payload) if isinstance(payload, str) else payload  # type: ignore[return-value]

    listing = _load(lanes.list_worktree_lanes(task_ref=task_ref))
    lane_rows = listing.get("lanes") or []
    last_active: list[float] = []
    for row in lane_rows:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        activity = _load(lanes.get_lane_activity(lane_id, task_ref=task_ref))
        stamps = _collect_created_at_epochs(activity)
        if stamps:
            last_active.append(max(stamps))
    return last_active


def crash_breaker_width_cap(
    workspace_root: Path,
    task_ref: str | None,
    boot_time: float,
    *,
    lane_activity_probe: object = None,
) -> tuple[int | None, str]:
    """Return ``(1, reason)`` when the post-crash breaker caps width, else ``(None, "")``.

    Trip condition (D5): >=2 of the task's lanes were active in the 6h before
    ``boot_time`` — the machine likely rebooted (panicked) out of a multi-lane
    run, so resume at width 1. The trip persists via a marker file holding the
    boot time; a marker from a previous boot self-clears. Every failure path is
    a no-cap (the breaker is a heuristic, never a brick).
    """
    if not task_ref or boot_time <= 0:
        return None, ""
    try:
        root = locks_root(Path(workspace_root))
    except Exception:  # noqa: BLE001 -- unresolved locks root => no breaker
        return None, ""
    marker = _breaker_marker(root, task_ref)
    try:
        if marker.exists():
            stamped = float(marker.read_text(encoding="utf-8").strip() or 0.0)
            if abs(stamped - boot_time) <= _BREAKER_BOOT_TOLERANCE_S:
                return 1, "post-crash breaker open (marker present for this boot)"
            marker.unlink()  # previous boot's marker — reboot resets the breaker
    except (OSError, ValueError):
        return None, ""
    probe = lane_activity_probe or _default_lane_activity_probe
    try:
        last_active = probe(Path(workspace_root), task_ref)  # type: ignore[operator]
    except Exception:  # noqa: BLE001 -- activity read failure => no trip
        return None, ""
    pre_reboot = [t for t in last_active if boot_time - _BREAKER_LOOKBACK_S <= t < boot_time]
    if len(pre_reboot) < 2:
        return None, ""
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(f"{boot_time}\n", encoding="utf-8")
    except OSError:
        pass
    reason = f"post-crash breaker tripped ({len(pre_reboot)} lanes active in the 6h before boot)"
    _record_breaker_blocker(Path(workspace_root), task_ref, reason)
    return 1, reason


def _record_breaker_blocker(workspace_root: Path, task_ref: str, reason: str) -> None:
    """Best-effort blocker event on breaker trip (D5). Never raises."""
    try:
        from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(workspace_root)))
        handoff_api.record_event(
            event={  # type: ignore[arg-type]  # pydantic validates raw dicts at runtime
                "event_kind": "blocker",
                "session": "hostgov-breaker",
                "operation": "add",
                "description": f"{reason}; width capped to 1 until admission_override resets the marker",
                "task_ref": task_ref,
            }
        )
    except Exception:  # noqa: BLE001, S110 -- best-effort by contract
        pass


def clear_crash_breaker(workspace_root: Path, task_ref: str | None) -> bool:
    """Operator reset (admission_override): remove the marker. True if removed."""
    if not task_ref:
        return False
    try:
        marker = _breaker_marker(locks_root(Path(workspace_root)), task_ref)
        if marker.exists():
            marker.unlink()
            return True
    except Exception:  # noqa: BLE001 -- reset is best-effort
        return False
    return False


def record_admission_telemetry(
    workspace_root: Path,
    decision: AdmissionDecision,
    *,
    surface: str,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> None:
    """Best-effort handoff decision event for a non-allow admission (D6).

    Never raises and never blocks the caller — telemetry failure must not turn
    a graceful defer into a crash. Allows are not recorded (too noisy; the
    doctor facet is the steady-state observability surface).
    """
    if decision.decision == "allow":
        return
    try:
        from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(workspace_root)))
        snap = decision.snapshot
        avail_detail = (
            f"{snap.available_ram / _GIB:.1f}GiB" if snap.available_ram is not None else "unavailable"
        )
        rationale = (
            f"host {decision.demand_label()} admission {decision.decision} at {surface}: {decision.reason}. "
            f"snapshot: platform={snap.platform} avail={avail_detail} "
            f"pressure={snap.pressure} width={decision.derived_width} held={decision.held_slots}"
            + (f" lane={lane_id}" if lane_id else "")
        )
        handoff_api.record_event(
            event={  # type: ignore[arg-type]  # pydantic validates raw dicts at runtime
                "event_kind": "decision",
                "session": f"hostgov-{surface}",
                "decision": f"hostgov_admission_{decision.decision}_{surface}",
                "rationale": rationale,
                **({"task_ref": task_ref} if task_ref else {}),
            }
        )
    except Exception:  # noqa: BLE001, S110 -- telemetry is best-effort by contract
        pass


def count_held_heavy_slots(
    root: Path,
    width: int,
    *,
    exclude_slots: Collection[int] | None = None,
) -> int:
    """Count currently-held heavy slots by flock-probing ``slot-N`` files.

    A slot is *held* when a non-blocking ``flock`` fails (another process owns
    it). Kernel releases the lock when the holder dies, so this is a live count
    with no reclaimer. Slots whose files do not yet exist are free.

    ``exclude_slots`` (keyword-only; default ``None`` reproduces the historical
    count exactly) skips slot ``n`` **only when** ``n`` is in the collection
    *and* the process-local registry confirms THIS process holds slot ``n``.
    A foreign idx passed in ``exclude_slots`` but not in the registry is still
    counted (implementation note S3 row 16 — ownership-verified exclusion).
    """
    import fcntl

    exclude: Collection[int] = exclude_slots if exclude_slots is not None else ()
    slot_dir = root / "admission"
    held = 0
    for n in range(max(0, width)):
        if n in exclude:
            with _HEAVY_SLOT_REGISTRY_LOCK:
                if n in _HEAVY_SLOT_REGISTRY:
                    continue
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
                # acquired => the slot was free; release immediately
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                held += 1
        finally:
            os.close(fd)
    return held
