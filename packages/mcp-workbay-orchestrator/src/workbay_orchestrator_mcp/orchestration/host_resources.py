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
import time
from dataclasses import dataclass, field, replace
from pathlib import Path

from workbay_protocol import HARNESS_CONTRACT_RELPATH

__all__ = [
    "AdmissionDecision",
    "HostMemoryPolicy",
    "HostResources",
    "SuiteLockTimeout",
    "acquire_heavy_slot",
    "acquire_suite_bulkhead",
    "acquire_suite_lock",
    "clear_crash_breaker",
    "count_held_heavy_slots",
    "crash_breaker_width_cap",
    "derive_width",
    "evaluate_admission",
    "load_host_memory_policy",
    "locks_root",
    "probe_host",
    "record_admission_telemetry",
    "resolve_live_admission",
]

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
    available_ram: int = 0
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
    swap_free_floor_mb: int = 512
    swap_volume_disk_floor_gib: float = 8.0
    slots_full_outcome: str = "defer"  # defer | refuse
    suite_lock_timeout_s: int = 1800
    warnings: tuple[str, ...] = field(default=())


# ---------------------------------------------------------------------------
# pure parsers (hermetic-test surface)
# ---------------------------------------------------------------------------


def _parse_vm_stat(text: str, page_size: int) -> int:
    """Available RAM per D1: (free + inactive + purgeable) x page size."""
    wanted = {"Pages free": 0, "Pages inactive": 0, "Pages purgeable": 0}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        key = key.strip()
        if key in wanted:
            digits = value.strip().rstrip(".")
            if digits.isdigit():
                wanted[key] = int(digits)
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
    available = _parse_vm_stat(_run(["/usr/bin/vm_stat"]), page_size)
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
        available_ram=meminfo.get("MemAvailable", 0),
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


def locks_root(workspace_root: Path | None = None) -> Path:
    """``<git-common-root>/.workbay/locks`` — shared across worktrees/lanes."""
    cwd = str(workspace_root) if workspace_root is not None else None
    common_dir = subprocess.run(  # noqa: S603
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
        cwd=cwd,
    ).stdout.strip()
    root = Path(common_dir).parent / ".workbay" / "locks"
    root.mkdir(parents=True, exist_ok=True)
    return root


# ---------------------------------------------------------------------------
# policy loader
# ---------------------------------------------------------------------------

_POLICY_FIELDS: dict[str, type] = {
    "enforcement": str,
    "os_reserve_gib": float,
    "os_reserve_remote_api_gib": float,
    "rss_per_heavy_gib": float,
    "rss_per_remote_api_gib": float,
    "max_width": int,
    "swap_free_floor_mb": int,
    "swap_volume_disk_floor_gib": float,
    "slots_full_outcome": str,
    "suite_lock_timeout_s": int,
}
_ENUM_FIELDS = {
    "enforcement": ("enforce", "warn_only", "off"),
    "slots_full_outcome": ("defer", "refuse"),
}


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
        caster = _POLICY_FIELDS.get(key)
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


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    """Outcome of an admission evaluation.

    ``enforced`` is False when ``warn_only``/``off`` downgraded a would-be
    ``defer``/``refuse`` to ``allow`` — the caller admits but the reason names
    the decision that would have fired under ``enforce``.
    """

    decision: str  # allow | defer | refuse
    reason: str
    cost_class: str
    derived_width: int
    held_slots: int
    enforced: bool
    snapshot: HostResources

    def to_dict(self) -> dict[str, object]:
        from dataclasses import asdict

        return {
            "decision": self.decision,
            "reason": self.reason,
            "cost_class": self.cost_class,
            "derived_width": self.derived_width,
            "held_slots": self.held_slots,
            "enforced": self.enforced,
            "snapshot": asdict(self.snapshot),
        }


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
) -> tuple[str, str]:
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
        return "refuse", "memory pressure critical"
    swap_floor = policy.swap_free_floor_mb * _MIB
    if resources.swap_total > 0 and resources.swap_free < swap_floor:
        return (
            "refuse",
            f"swap free {resources.swap_free / _MIB:.0f}MB below floor {policy.swap_free_floor_mb}MB",
        )
    disk_floor = policy.swap_volume_disk_floor_gib * _GIB
    # Skip when the volume reading is absent (0) — a narrow read failure must
    # not refuse every spawn; broad probe failure is handled via pressure=warn.
    if 0 < resources.swap_volume_free_bytes < disk_floor:
        return (
            "refuse",
            f"swap-volume free disk {resources.swap_volume_free_bytes / _GIB:.1f}GiB "
            f"below floor {policy.swap_volume_disk_floor_gib}GiB",
        )
    if width == 0:
        return (
            "refuse",
            "derived width 0 (available RAM minus OS reserve < per-class RSS)",
        )
    # --- defer dimensions (retryable) ---
    if pressure == "warn" and held_slots >= 1:
        detail = "warn" if resources.pressure == "warn" else "unknown(degraded)"
        return (
            "defer",
            f"memory pressure {detail} with {held_slots} heavy slot(s) held",
        )
    if held_slots >= width:
        outcome = policy.slots_full_outcome if policy.slots_full_outcome in ("defer", "refuse") else "defer"
        return outcome, f"all {width} derived heavy slot(s) busy"
    return "allow", f"width {width}, {held_slots} slot(s) held"


def evaluate_admission(
    resources: HostResources,
    cost_class: str,
    policy: HostMemoryPolicy,
    held_slots: int = 0,
) -> AdmissionDecision:
    """Call-time admission verdict for a spawn of ``cost_class`` (D2).

    Pure function: ``held_slots`` is injected by the caller (the slot registry
    read is I/O kept separate). ``light`` is never gated; ``enforcement=off``
    skips evaluation; ``warn_only`` downgrades a would-be defer/refuse to an
    unenforced allow.
    """
    width = derive_width(resources, policy, cost_class)

    if cost_class not in _GATED_COST_CLASSES:
        return AdmissionDecision(
            "allow", f"{cost_class} cost class is never gated", cost_class, width, held_slots, True, resources
        )
    if policy.enforcement == "off":
        return AdmissionDecision("allow", "enforcement=off", cost_class, width, held_slots, False, resources)

    decision, reason = _classify_admission(resources, cost_class, policy, width, held_slots)

    if policy.enforcement == "warn_only" and decision != "allow":
        return AdmissionDecision(
            "allow",
            f"warn_only: would {decision} ({reason})",
            cost_class,
            width,
            held_slots,
            False,
            resources,
        )
    return AdmissionDecision(decision, reason, cost_class, width, held_slots, True, resources)


def resolve_live_admission(
    workspace_root: Path,
    cost_class: str = COST_HEAVY,
) -> AdmissionDecision:
    """Full call-time admission verdict: probe + policy + slot count + evaluate.

    The single I/O entry point shared by the orchestrator dispatch surfaces and
    the ``workbay-hostgov`` CLI. Slot-count failure degrades to 0 held (the
    probe/pressure checks still gate); it never raises.
    """
    resources = probe_host()
    policy = load_host_memory_policy(workspace_root)
    held = 0
    if cost_class in _GATED_COST_CLASSES:
        width = derive_width(resources, policy, cost_class)
        try:
            held = count_held_heavy_slots(locks_root(workspace_root), width)
        except Exception:  # noqa: BLE001 -- a slot-count failure must not brick admission
            held = 0
    return evaluate_admission(resources, cost_class, policy, held)


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
        return (n, fd)
    return None


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
        rationale = (
            f"host memory admission {decision.decision} at {surface}: {decision.reason}. "
            f"snapshot: platform={snap.platform} avail={snap.available_ram / _GIB:.1f}GiB "
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


def count_held_heavy_slots(root: Path, width: int) -> int:
    """Count currently-held heavy slots by flock-probing ``slot-N`` files.

    A slot is *held* when a non-blocking ``flock`` fails (another process owns
    it). Kernel releases the lock when the holder dies, so this is a live count
    with no reclaimer. Slots whose files do not yet exist are free.
    """
    import fcntl

    slot_dir = root / "admission"
    held = 0
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
                # acquired => the slot was free; release immediately
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                held += 1
        finally:
            os.close(fd)
    return held
